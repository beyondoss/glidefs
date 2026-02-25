//! Garbage collection CLI command.
//!
//! Identifies and deletes orphaned packs and chunk metas in S3. Operates by
//! comparing known objects (listed from S3) against live objects (referenced
//! by volume manifests and their chunk metas). Objects referenced by no
//! manifest are dead and eligible for deletion after a grace period.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::{info, warn};
use uuid::Uuid;

use crate::block::chunk_meta::ChunkMeta;
use crate::block::content_store::{ChunkObjectKind, ContentStore};
use crate::block::volume_manifest::VolumeManifest;
use crate::config::Settings;
use crate::parse_object_store::parse_url_opts;

// ---------------------------------------------------------------------------
// GC State (persisted between runs for grace period tracking)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GcState {
    /// Pack ID (UUID string) -> first-seen-dead ISO 8601 timestamp.
    pub(crate) dead_packs: HashMap<String, String>,
    /// Chunk meta key ("{chunk_idx}/{hash}") -> first-seen-dead ISO 8601 timestamp.
    #[serde(default)]
    pub(crate) dead_metas: HashMap<String, String>,
}

impl GcState {
    fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(data) => Ok(serde_json::from_str(&data)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        let dir = path.parent().unwrap_or(Path::new("."));
        let tmp = tempfile::NamedTempFile::new_in(dir)
            .with_context(|| format!("failed to create temp file in {}", dir.display()))?;
        std::fs::write(tmp.path(), &json)?;
        tmp.persist(path).with_context(|| {
            format!("failed to atomically rename GC state to {}", path.display())
        })?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn mark_dead(&mut self, pack_id: &Uuid) {
        let key = pack_id.to_string();
        self.dead_packs
            .entry(key)
            .or_insert_with(|| Utc::now().to_rfc3339());
    }

    #[cfg(test)]
    fn mark_alive(&mut self, pack_id: &Uuid) {
        self.dead_packs.remove(&pack_id.to_string());
    }

    fn is_eligible(&self, pack_id: &Uuid, grace_period: Duration) -> bool {
        let key = pack_id.to_string();
        Self::is_key_eligible(&self.dead_packs, &key, grace_period)
    }

    // -- Meta tracking (same grace period pattern as packs) --

    fn meta_key(chunk_idx: u32, hash: &str) -> String {
        format!("{chunk_idx}/{hash}")
    }

    fn is_meta_eligible(&self, chunk_idx: u32, hash: &str, grace_period: Duration) -> bool {
        let key = Self::meta_key(chunk_idx, hash);
        Self::is_key_eligible(&self.dead_metas, &key, grace_period)
    }

    fn is_key_eligible(map: &HashMap<String, String>, key: &str, grace_period: Duration) -> bool {
        if let Some(ts_str) = map.get(key)
            && let Ok(ts) = ts_str.parse::<DateTime<Utc>>()
        {
            let age = Utc::now().signed_duration_since(ts);
            return age.to_std().unwrap_or(Duration::ZERO) >= grace_period;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// GC State Delta (collected per-prefix, merged after parallel execution)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct GcStateDelta {
    /// Packs newly seen as dead: (uuid_string, timestamp)
    newly_dead_packs: Vec<(String, String)>,
    /// Packs that became live again (were in dead state, now referenced)
    revived_packs: Vec<String>,
    /// Packs successfully deleted
    deleted_packs: Vec<String>,
    /// Metas newly seen as dead: (key, timestamp)
    newly_dead_metas: Vec<(String, String)>,
    /// Metas that became live again
    revived_metas: Vec<String>,
    /// Metas successfully deleted
    deleted_metas: Vec<String>,
}

impl GcState {
    fn apply_delta(&mut self, delta: GcStateDelta) {
        for (key, ts) in delta.newly_dead_packs {
            self.dead_packs.entry(key).or_insert(ts);
        }
        for key in delta.revived_packs {
            self.dead_packs.remove(&key);
        }
        for key in delta.deleted_packs {
            self.dead_packs.remove(&key);
        }
        for (key, ts) in delta.newly_dead_metas {
            self.dead_metas.entry(key).or_insert(ts);
        }
        for key in delta.revived_metas {
            self.dead_metas.remove(&key);
        }
        for key in delta.deleted_metas {
            self.dead_metas.remove(&key);
        }
    }
}

// ---------------------------------------------------------------------------
// GC Statistics
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct GcStats {
    prefixes_scanned: usize,
    manifests_scanned: usize,
    manifest_errors: usize,
    live_packs: usize,
    known_packs: usize,
    dead_found: usize,
    eligible_for_deletion: usize,
    packs_deleted: usize,
    // Meta cleanup stats
    known_metas: usize,
    dead_metas_found: usize,
    eligible_metas: usize,
    metas_deleted: usize,
    // Meta cache stats
    meta_cache_hits: usize,
    meta_cache_misses: usize,
    // Snapshot retention stats
    snapshots_scanned: usize,
    snapshots_deleted: usize,
}

impl GcStats {
    fn merge(&mut self, other: GcStats) {
        self.prefixes_scanned += other.prefixes_scanned;
        self.manifests_scanned += other.manifests_scanned;
        self.manifest_errors += other.manifest_errors;
        self.live_packs += other.live_packs;
        self.known_packs += other.known_packs;
        self.dead_found += other.dead_found;
        self.eligible_for_deletion += other.eligible_for_deletion;
        self.packs_deleted += other.packs_deleted;
        self.known_metas += other.known_metas;
        self.dead_metas_found += other.dead_metas_found;
        self.eligible_metas += other.eligible_metas;
        self.metas_deleted += other.metas_deleted;
        self.meta_cache_hits += other.meta_cache_hits;
        self.meta_cache_misses += other.meta_cache_misses;
        self.snapshots_scanned += other.snapshots_scanned;
        self.snapshots_deleted += other.snapshots_deleted;
    }
}

// ---------------------------------------------------------------------------
// On-disk meta cache (avoids redundant S3 ChunkMeta GETs across GC runs)
// ---------------------------------------------------------------------------

/// Caches chunk meta → pack_ids mapping on local disk so subsequent GC runs
/// skip S3 GETs for chunks whose content hash hasn't changed.
///
/// Directory layout mirrors the S3 prefix structure:
///   `{cache_dir}/{prefix}/{chunk_idx:04}_{chunk_hash}.bin`
///
/// Each file is a flat array of 16-byte UUIDs (the pack_ids). Correctness
/// invariant: entries not seen during a GC run are pruned afterwards — this
/// prevents stale entries from hiding dead packs after .meta GC deletes
/// an orphaned ChunkMeta from S3.
struct MetaCache {
    dir: PathBuf,
    /// Paths touched this run (for pruning stale entries).
    seen: Mutex<HashSet<PathBuf>>,
}

impl MetaCache {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// Cache file path for a given prefix + chunk.
    fn cache_path(&self, prefix: &str, chunk_idx: u32, chunk_hash: &str) -> PathBuf {
        self.dir
            .join(prefix)
            .join(format!("{chunk_idx:04}_{chunk_hash}.bin"))
    }

    /// Look up cached pack_ids. Returns None on cache miss.
    fn get(&self, prefix: &str, chunk_idx: u32, chunk_hash: &str) -> Option<HashSet<Uuid>> {
        let path = self.cache_path(prefix, chunk_idx, chunk_hash);
        self.seen.lock().unwrap().insert(path.clone());
        let data = std::fs::read(&path).ok()?;
        if data.len() % 16 != 0 {
            // Corrupt cache entry — treat as miss.
            let _ = std::fs::remove_file(&path);
            return None;
        }
        let uuids: HashSet<Uuid> = data
            .chunks_exact(16)
            .map(|c| Uuid::from_bytes(c.try_into().unwrap()))
            .collect();
        Some(uuids)
    }

    /// Write pack_ids to the cache.
    fn put(&self, prefix: &str, chunk_idx: u32, chunk_hash: &str, pack_ids: &HashSet<Uuid>) {
        let path = self.cache_path(prefix, chunk_idx, chunk_hash);
        self.seen.lock().unwrap().insert(path.clone());
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut data = Vec::with_capacity(pack_ids.len() * 16);
        for id in pack_ids {
            data.extend_from_slice(id.as_bytes());
        }
        if let Err(e) = std::fs::write(&path, &data) {
            warn!(path = %path.display(), error = %e, "failed to write meta cache entry");
        }
    }

    /// Delete cache entries not touched this run. Required for correctness:
    /// when .meta GC deletes an orphaned ChunkMeta from S3, the cache entry
    /// must also go, otherwise the cache would return pack_ids for a ChunkMeta
    /// that no longer exists.
    fn prune_unseen(&self) -> Result<usize> {
        let seen = self.seen.lock().unwrap().clone();
        let mut pruned = 0;

        if !self.dir.exists() {
            return Ok(0);
        }

        Self::prune_dir(&self.dir, &seen, &mut pruned);
        Ok(pruned)
    }

    /// Recursively walk a directory, removing unseen .bin files and empty dirs.
    fn prune_dir(dir: &Path, seen: &HashSet<PathBuf>, pruned: &mut usize) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                Self::prune_dir(&path, seen, pruned);
                let _ = std::fs::remove_dir(&path); // only succeeds if empty
            } else if path.extension().is_some_and(|ext| ext == "bin") && !seen.contains(&path) {
                if std::fs::remove_file(&path).is_ok() {
                    *pruned += 1;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run_gc(
    config_path: PathBuf,
    dry_run: bool,
    grace_period_str: String,
    max_deletes: usize,
    state_file: PathBuf,
    meta_cache_dir: PathBuf,
    snapshot_retention_str: Option<String>,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let grace_period = parse_duration(&grace_period_str)?;

    info!(
        dry_run,
        grace_period_secs = grace_period.as_secs(),
        max_deletes,
        state_file = %state_file.display(),
        "starting GC"
    );

    // Load config and create object store
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = path_from_url.to_string();

    // Load GC state and create meta cache
    let mut state = GcState::load(&state_file)?;
    let mut stats = GcStats::default();
    let meta_cache = Arc::new(MetaCache::new(meta_cache_dir));

    // Parse snapshot retention if provided.
    let snapshot_retention = snapshot_retention_str
        .as_deref()
        .map(parse_duration)
        .transpose()?;

    // Discover all S3 prefixes that contain manifests or chunks
    let prefixes = discover_s3_prefixes(&*object_store, &db_path).await?;
    info!(count = prefixes.len(), "discovered S3 prefixes");

    // Phase 0: Snapshot retention (runs BEFORE pack/meta reconciliation).
    // Fewer live snapshots = smaller live set = more dead packs eligible for cleanup.
    use futures::StreamExt;

    if let Some(retention) = snapshot_retention {
        let now = Utc::now();
        let mut snapshots_deleted: usize = 0;
        let mut snapshots_total: usize = 0;

        let retention_results: Vec<Result<(usize, usize)>> =
            futures::stream::iter(prefixes.iter())
                .map(|prefix| {
                    let store = Arc::clone(&object_store);
                    async move {
                        let content_store = ContentStore::new(store, prefix);
                        enforce_snapshot_retention(&content_store, retention, now, dry_run).await
                    }
                })
                .buffer_unordered(16)
                .collect()
                .await;

        for result in retention_results {
            let (total, deleted) = result?;
            snapshots_total += total;
            snapshots_deleted += deleted;
        }

        info!(snapshots_total, snapshots_deleted, "snapshot retention complete");
        stats.snapshots_scanned = snapshots_total;
        stats.snapshots_deleted = snapshots_deleted;
    }

    // Process prefixes in parallel. Each prefix is an independent S3 namespace.

    let per_prefix_budget = max_deletes / prefixes.len().max(1);
    let results: Vec<Result<(GcStateDelta, GcStats)>> =
        futures::stream::iter(prefixes.iter())
            .map(|prefix| {
                let store = Arc::clone(&object_store);
                let state_ref = &state;
                let cache = Arc::clone(&meta_cache);
                async move {
                    let content_store = ContentStore::new(store, prefix);
                    reconcile_prefix(
                        &content_store,
                        state_ref,
                        grace_period,
                        per_prefix_budget,
                        dry_run,
                        &cache,
                    )
                    .await
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;

    for result in results {
        let (delta, prefix_stats) = result?;
        state.apply_delta(delta);
        stats.merge(prefix_stats);
    }

    // Prune stale meta cache entries (correctness: evict entries for deleted .metas)
    match meta_cache.prune_unseen() {
        Ok(pruned) if pruned > 0 => info!(pruned, "pruned stale meta cache entries"),
        Ok(_) => {}
        Err(e) => warn!(error = %e, "failed to prune meta cache"),
    }

    // Save updated state
    state.save(&state_file)?;

    // Report
    println!("\n--- GC Report ---");
    if dry_run {
        println!("MODE: DRY RUN (no deletions performed)");
    }
    println!("Prefixes scanned:        {}", stats.prefixes_scanned);
    println!("Manifests scanned:       {}", stats.manifests_scanned);
    println!("Manifest parse errors:   {}", stats.manifest_errors);
    println!("Live packs:              {}", stats.live_packs);
    println!("Known packs:             {}", stats.known_packs);
    println!("Dead packs found:        {}", stats.dead_found);
    println!("Eligible for deletion:   {}", stats.eligible_for_deletion);
    println!("Packs deleted:           {}", stats.packs_deleted);
    println!("Known metas:             {}", stats.known_metas);
    println!("Dead metas found:        {}", stats.dead_metas_found);
    println!("Eligible metas:          {}", stats.eligible_metas);
    println!("Metas deleted:           {}", stats.metas_deleted);
    println!("Meta cache hits:         {}", stats.meta_cache_hits);
    println!("Meta cache misses:       {}", stats.meta_cache_misses);
    if stats.snapshots_scanned > 0 || stats.snapshots_deleted > 0 {
        println!("Snapshots scanned:       {}", stats.snapshots_scanned);
        println!("Snapshots deleted:       {}", stats.snapshots_deleted);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// S3 prefix discovery
// ---------------------------------------------------------------------------

/// Discover all unique S3 prefixes under `{db_path}/exports/`.
///
/// Uses `list_with_delimiter` to enumerate only the top-level export directory
/// names — O(exports) instead of O(total_objects). At 50M objects the old
/// flat-list approach required ~50K LIST pages; this uses 1.
async fn discover_s3_prefixes(
    object_store: &dyn object_store::ObjectStore,
    db_path: &str,
) -> Result<Vec<String>> {
    use object_store::path::Path as ObjectPath;

    let exports_prefix = ObjectPath::from(format!("{}/exports/", db_path.trim_end_matches('/')));
    let result = object_store
        .list_with_delimiter(Some(&exports_prefix))
        .await?;

    let mut prefixes: Vec<String> = result
        .common_prefixes
        .into_iter()
        .map(|p| p.to_string().trim_end_matches('/').to_string())
        .collect();
    prefixes.sort();
    Ok(prefixes)
}

// ---------------------------------------------------------------------------
// Snapshot retention
// ---------------------------------------------------------------------------

/// Delete snapshots older than `retention` for a single prefix.
/// Returns (total_snapshots, deleted_snapshots).
async fn enforce_snapshot_retention(
    content_store: &ContentStore,
    retention: Duration,
    now: DateTime<Utc>,
    dry_run: bool,
) -> Result<(usize, usize)> {
    let snapshots = content_store.list_all_snapshots_with_dates().await?;
    let total = snapshots.len();
    let mut deleted = 0;

    let cutoff = now - chrono::TimeDelta::from_std(retention)
        .unwrap_or(chrono::TimeDelta::MAX);

    for (path, last_modified) in &snapshots {
        if *last_modified < cutoff {
            if dry_run {
                info!(path = %path, age_days = (now - *last_modified).num_days(), "would delete expired snapshot (dry-run)");
            } else {
                match content_store.delete_snapshot_by_path(path).await {
                    Ok(()) => {
                        info!(path = %path, "deleted expired snapshot");
                    }
                    Err(e) => {
                        warn!(path = %path, error = %e, "failed to delete expired snapshot");
                        continue;
                    }
                }
            }
            deleted += 1;
        }
    }

    Ok((total, deleted))
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Reconcile a single S3 prefix: find and delete orphaned packs and metas.
///
/// Uses two-phase chunk dedup: first collects unique (chunk_idx, chunk_hash)
/// pairs across all manifests and snapshots, then fetches each ChunkMeta once.
/// For fork-heavy workloads this is dramatically cheaper — 2000 VMs forked
/// from the same base share most chunk hashes, reducing ChunkMeta fetches
/// from O(M×C) to O(unique_chunks).
///
/// Takes an immutable state snapshot for grace period checks and returns a
/// delta to apply after all prefixes complete. This enables per-prefix
/// parallelism with no shared mutable state.
async fn reconcile_prefix(
    content_store: &ContentStore,
    state: &GcState,
    grace_period: Duration,
    max_deletes: usize,
    dry_run: bool,
    meta_cache: &MetaCache,
) -> Result<(GcStateDelta, GcStats)> {
    let mut stats = GcStats::default();
    let mut delta = GcStateDelta::default();
    // Phase 1: Read all manifests + snapshots, collect unique (chunk_idx, chunk_hash) pairs.
    let mut unique_chunks: HashSet<(u32, String)> = HashSet::new();
    let mut total_chunk_refs: usize = 0;
    let manifest_names = content_store.list_all_manifests().await?;
    let mut manifest_failed = false;

    for name in &manifest_names {
        match content_store.get_volume_manifest(name).await {
            Ok(Some(data)) => match VolumeManifest::deserialize(&data) {
                Ok(vm) => {
                    for (&chunk_idx, chunk_hash) in &vm.chunks {
                        unique_chunks.insert((chunk_idx, chunk_hash.clone()));
                        total_chunk_refs += 1;
                    }
                    stats.manifests_scanned += 1;
                }
                Err(e) => {
                    warn!(manifest = %name, error = %e, "failed to parse volume manifest — treating all packs in prefix as live");
                    stats.manifest_errors += 1;
                    manifest_failed = true;
                }
            },
            Ok(None) => {
                warn!(manifest = %name, "manifest disappeared during GC");
            }
            Err(e) => {
                warn!(manifest = %name, error = %e, "failed to fetch manifest — treating all packs in prefix as live");
                stats.manifest_errors += 1;
                manifest_failed = true;
            }
        }
    }

    if manifest_failed {
        warn!("skipping GC for prefix due to manifest errors — no packs will be deleted");
        return Ok((delta, stats));
    }

    // Snapshot manifests — collect unique chunks from them too.
    match content_store.list_snapshot_manifests().await {
        Ok(snapshot_vms) => {
            for vm in &snapshot_vms {
                for (&chunk_idx, chunk_hash) in &vm.chunks {
                    unique_chunks.insert((chunk_idx, chunk_hash.clone()));
                    total_chunk_refs += 1;
                }
            }
            stats.manifests_scanned += snapshot_vms.len();
        }
        Err(e) => {
            warn!(error = %e, "failed to scan snapshot manifests — treating all packs as live");
            return Ok((delta, stats));
        }
    }

    info!(
        total_chunk_refs,
        unique_chunk_metas = unique_chunks.len(),
        "deduplicated chunk meta lookups"
    );

    // Phase 2: Fetch each unique ChunkMeta, collect live pack IDs.
    // Parallel fetch with bounded concurrency (same pattern as prefetch_chunk_metas).
    use futures::StreamExt;

    enum ChunkMetaResult {
        Packs(HashSet<Uuid>),
        NotFound,
        Failed,
    }

    let prefix = content_store.base_path();
    let mut cache_hits: usize = 0;
    let mut cache_misses: usize = 0;

    let raw_results: Vec<(ChunkMetaResult, bool)> = futures::stream::iter(unique_chunks.iter())
        .map(|(chunk_idx, chunk_hash_hex)| {
            let cs = &content_store;
            let mc = &meta_cache;
            let chunk_idx = *chunk_idx;
            let chunk_hash_hex = chunk_hash_hex.clone();
            async move {
                // Check on-disk cache first.
                if let Some(pack_ids) = mc.get(prefix, chunk_idx, &chunk_hash_hex) {
                    return (ChunkMetaResult::Packs(pack_ids), true);
                }

                // Cache miss — fetch from S3.
                let result = match cs.get_chunk_meta(chunk_idx, &chunk_hash_hex).await {
                    Ok(Some(meta_bytes)) => match ChunkMeta::deserialize(&meta_bytes) {
                        Ok(meta) => {
                            let pack_ids = meta.pack_ids();
                            mc.put(prefix, chunk_idx, &chunk_hash_hex, &pack_ids);
                            ChunkMetaResult::Packs(pack_ids)
                        }
                        Err(e) => {
                            warn!(
                                chunk_idx,
                                chunk_hash = %chunk_hash_hex,
                                error = %e,
                                "corrupt chunk meta — treating all packs in prefix as live"
                            );
                            ChunkMetaResult::Failed
                        }
                    },
                    Ok(None) => {
                        warn!(
                            chunk_idx,
                            chunk_hash = %chunk_hash_hex,
                            "chunk meta not found (may have been cleaned up)"
                        );
                        ChunkMetaResult::NotFound
                    }
                    Err(e) => {
                        warn!(
                            chunk_idx,
                            chunk_hash = %chunk_hash_hex,
                            error = %e,
                            "failed to fetch chunk meta — treating all packs in prefix as live"
                        );
                        ChunkMetaResult::Failed
                    }
                };
                (result, false)
            }
        })
        .buffer_unordered(32)
        .collect()
        .await;

    let mut results: Vec<ChunkMetaResult> = Vec::with_capacity(raw_results.len());
    for (result, hit) in raw_results {
        if hit {
            cache_hits += 1;
        } else {
            cache_misses += 1;
        }
        results.push(result);
    }

    stats.meta_cache_hits += cache_hits;
    stats.meta_cache_misses += cache_misses;

    let mut live_packs: HashSet<Uuid> = HashSet::new();
    for result in results {
        match result {
            ChunkMetaResult::Packs(packs) => live_packs.extend(packs),
            ChunkMetaResult::NotFound => {}
            ChunkMetaResult::Failed => manifest_failed = true,
        }
    }

    if manifest_failed {
        warn!("skipping GC for prefix due to chunk meta errors — no packs will be deleted");
        return Ok((delta, stats));
    }

    // 2. Single-pass listing: discover known packs AND known metas together.
    let all_chunk_objects = content_store.list_all_chunk_objects().await?;
    let mut known_packs: HashSet<Uuid> = HashSet::new();
    let mut pack_locations: HashMap<Uuid, u32> = HashMap::new();
    let mut known_metas: Vec<(u32, String)> = Vec::new();

    for obj in &all_chunk_objects {
        match &obj.kind {
            ChunkObjectKind::Pack(pack_id) => {
                known_packs.insert(*pack_id);
                pack_locations.insert(*pack_id, obj.chunk_idx);
            }
            ChunkObjectKind::Meta(hash) => {
                known_metas.push((obj.chunk_idx, hash.clone()));
            }
        }
    }

    stats.live_packs += live_packs.len();
    stats.known_packs += known_packs.len();
    stats.known_metas += known_metas.len();

    // 3. Compute dead packs = known - live
    let dead_packs: HashSet<Uuid> = known_packs.difference(&live_packs).copied().collect();
    stats.dead_found += dead_packs.len();

    // 4. Record state changes as delta: mark new dead packs, revive packs that became live
    let now_ts = Utc::now().to_rfc3339();
    for &pack_id in &dead_packs {
        let key = pack_id.to_string();
        if !state.dead_packs.contains_key(&key) {
            delta.newly_dead_packs.push((key, now_ts.clone()));
        }
    }
    let revived: Vec<Uuid> = known_packs
        .intersection(&live_packs)
        .copied()
        .filter(|id| state.dead_packs.contains_key(&id.to_string()))
        .collect();
    for pack_id in revived {
        delta.revived_packs.push(pack_id.to_string());
    }

    // 5. Filter by grace period.
    //    Packs already in state.dead_packs from a previous run are eligible if
    //    their age exceeds the grace period. Newly discovered dead packs (not yet
    //    in state) are eligible immediately when grace_period is zero.
    let eligible: Vec<Uuid> = dead_packs
        .iter()
        .filter(|id| {
            state.is_eligible(id, grace_period)
                || (grace_period.is_zero()
                    && !state.dead_packs.contains_key(&id.to_string()))
        })
        .copied()
        .collect();
    stats.eligible_for_deletion += eligible.len();

    // 6. Delete eligible packs (capped, parallel)
    let to_delete: Vec<Uuid> = eligible.into_iter().take(max_deletes).collect();

    if dry_run {
        for &pack_id in &to_delete {
            info!(pack_id = %pack_id, "would delete orphaned pack (dry-run)");
        }
        stats.packs_deleted += to_delete.len();
    } else {
        let delete_results: Vec<(Uuid, Result<(), _>)> =
            futures::stream::iter(to_delete.iter().copied())
                .map(|pack_id| {
                    let cs = &content_store;
                    let chunk_idx = pack_locations.get(&pack_id).copied().unwrap_or(u32::MAX);
                    async move {
                        let result = if chunk_idx == u32::MAX {
                            cs.delete_pack(pack_id).await
                        } else {
                            cs.delete_chunk_pack(chunk_idx, pack_id).await
                        };
                        (pack_id, result)
                    }
                })
                .buffer_unordered(32)
                .collect()
                .await;

        for (pack_id, result) in delete_results {
            match result {
                Ok(()) => {
                    delta.deleted_packs.push(pack_id.to_string());
                    stats.packs_deleted += 1;
                }
                Err(e) => {
                    warn!(pack_id = %pack_id, error = %e, "failed to delete pack");
                }
            }
        }
    }

    // 7. Identify and delete orphaned .meta files.
    //    Dead metas = known metas not referenced by any manifest or snapshot.
    let dead_metas: Vec<(u32, String)> = known_metas
        .into_iter()
        .filter(|(idx, hash)| !unique_chunks.contains(&(*idx, hash.clone())))
        .collect();
    stats.dead_metas_found += dead_metas.len();

    for (chunk_idx, hash) in &dead_metas {
        let key = GcState::meta_key(*chunk_idx, hash);
        if !state.dead_metas.contains_key(&key) {
            delta.newly_dead_metas.push((key, now_ts.clone()));
        }
    }
    // Revive metas that are now live
    for (chunk_idx, hash) in &unique_chunks {
        if state
            .dead_metas
            .contains_key(&GcState::meta_key(*chunk_idx, hash))
        {
            delta
                .revived_metas
                .push(GcState::meta_key(*chunk_idx, hash));
        }
    }

    let eligible_metas: Vec<(u32, String)> = dead_metas
        .iter()
        .filter(|(idx, hash)| {
            state.is_meta_eligible(*idx, hash, grace_period)
                || (grace_period.is_zero()
                    && !state
                        .dead_metas
                        .contains_key(&GcState::meta_key(*idx, hash)))
        })
        .cloned()
        .collect();
    stats.eligible_metas += eligible_metas.len();

    let remaining_budget = max_deletes.saturating_sub(stats.packs_deleted);
    let metas_to_delete: Vec<(u32, String)> =
        eligible_metas.into_iter().take(remaining_budget).collect();

    if dry_run {
        for (chunk_idx, hash) in &metas_to_delete {
            info!(chunk_idx, chunk_hash = %hash, "would delete orphaned meta (dry-run)");
        }
        stats.metas_deleted += metas_to_delete.len();
    } else {
        let delete_results: Vec<(u32, String, Result<(), _>)> =
            futures::stream::iter(metas_to_delete.iter().cloned())
                .map(|(chunk_idx, hash)| {
                    let cs = &content_store;
                    async move {
                        let result = cs.delete_chunk_meta(chunk_idx, &hash).await;
                        (chunk_idx, hash, result)
                    }
                })
                .buffer_unordered(32)
                .collect()
                .await;

        for (chunk_idx, hash, result) in delete_results {
            match result {
                Ok(()) => {
                    delta
                        .deleted_metas
                        .push(GcState::meta_key(chunk_idx, &hash));
                    stats.metas_deleted += 1;
                }
                Err(e) => {
                    warn!(chunk_idx, chunk_hash = %hash, error = %e, "failed to delete meta");
                }
            }
        }
    }

    stats.prefixes_scanned += 1;
    Ok((delta, stats))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a human-friendly duration string like "24h", "1h", "30m", "7d".
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if let Some(hours) = s.strip_suffix('h') {
        Ok(Duration::from_secs(hours.parse::<u64>()? * 3600))
    } else if let Some(minutes) = s.strip_suffix('m') {
        Ok(Duration::from_secs(minutes.parse::<u64>()? * 60))
    } else if let Some(days) = s.strip_suffix('d') {
        Ok(Duration::from_secs(days.parse::<u64>()? * 86400))
    } else if let Some(secs) = s.strip_suffix('s') {
        Ok(Duration::from_secs(secs.parse::<u64>()?))
    } else {
        anyhow::bail!("invalid duration '{}': use suffix h/m/s/d (e.g. '24h')", s)
    }
}

// ---------------------------------------------------------------------------
// Public API for testing
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code)]
/// Run GC reconciliation on a single content store prefix.
/// Exposed for integration testing.
pub async fn reconcile_prefix_for_test(
    content_store: &ContentStore,
    state: &mut GcState,
    grace_period: Duration,
    max_deletes: usize,
    dry_run: bool,
) -> Result<GcTestReport> {
    let cache_dir = tempfile::tempdir().expect("failed to create temp dir for meta cache");
    let cache = MetaCache::new(cache_dir.path().to_path_buf());
    reconcile_prefix_for_test_with_cache(content_store, state, grace_period, max_deletes, dry_run, &cache).await
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code)]
async fn reconcile_prefix_for_test_with_cache(
    content_store: &ContentStore,
    state: &mut GcState,
    grace_period: Duration,
    max_deletes: usize,
    dry_run: bool,
    cache: &MetaCache,
) -> Result<GcTestReport> {
    let (delta, stats) = reconcile_prefix(
        content_store,
        state,
        grace_period,
        max_deletes,
        dry_run,
        cache,
    )
    .await?;
    state.apply_delta(delta);
    Ok(GcTestReport { stats })
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code)]
/// Create a new empty GC state for testing.
pub fn new_gc_state_for_test() -> GcState {
    GcState::default()
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code)]
/// Inject a dead pack into GC state with a specific timestamp for testing.
pub fn inject_dead_pack_for_test(state: &mut GcState, pack_id: &Uuid, timestamp: DateTime<Utc>) {
    state
        .dead_packs
        .insert(pack_id.to_string(), timestamp.to_rfc3339());
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code)]
/// Inject a dead meta into GC state with a specific timestamp for testing.
pub fn inject_dead_meta_for_test(
    state: &mut GcState,
    chunk_idx: u32,
    hash: &str,
    timestamp: DateTime<Utc>,
) {
    let key = GcState::meta_key(chunk_idx, hash);
    state.dead_metas.insert(key, timestamp.to_rfc3339());
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code)]
/// Test report from reconciliation.
pub struct GcTestReport {
    stats: GcStats,
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code)]
impl GcTestReport {
    pub fn manifests_scanned(&self) -> usize {
        self.stats.manifests_scanned
    }
    pub fn manifest_errors(&self) -> usize {
        self.stats.manifest_errors
    }
    pub fn live_packs(&self) -> usize {
        self.stats.live_packs
    }
    pub fn known_packs(&self) -> usize {
        self.stats.known_packs
    }
    pub fn dead_found(&self) -> usize {
        self.stats.dead_found
    }
    pub fn eligible_for_deletion(&self) -> usize {
        self.stats.eligible_for_deletion
    }
    pub fn packs_deleted(&self) -> usize {
        self.stats.packs_deleted
    }
    pub fn known_metas(&self) -> usize {
        self.stats.known_metas
    }
    pub fn dead_metas_found(&self) -> usize {
        self.stats.dead_metas_found
    }
    pub fn eligible_metas(&self) -> usize {
        self.stats.eligible_metas
    }
    pub fn metas_deleted(&self) -> usize {
        self.stats.metas_deleted
    }
    pub fn deleted_count(&self) -> usize {
        self.stats.packs_deleted + self.stats.metas_deleted
    }
    pub fn meta_cache_hits(&self) -> usize {
        self.stats.meta_cache_hits
    }
    pub fn meta_cache_misses(&self) -> usize {
        self.stats.meta_cache_misses
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86400));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1800));
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604800));
    }

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("60s").unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("24").is_err());
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn test_gc_state_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gc-state.json");

        let mut state = GcState::default();
        let id = Uuid::new_v4();
        state.mark_dead(&id);
        state.save(&path).unwrap();

        let loaded = GcState::load(&path).unwrap();
        assert!(loaded.dead_packs.contains_key(&id.to_string()));
    }

    #[test]
    fn test_gc_state_load_missing() {
        let path = PathBuf::from("/tmp/nonexistent-gc-state-12345.json");
        let state = GcState::load(&path).unwrap();
        assert!(state.dead_packs.is_empty());
    }

    #[test]
    fn test_gc_state_eligibility() {
        let mut state = GcState::default();
        let id = Uuid::new_v4();

        // Not in state -> not eligible
        assert!(!state.is_eligible(&id, Duration::from_secs(3600)));

        // Mark dead with a timestamp in the past
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        state.dead_packs.insert(id.to_string(), old_ts.to_rfc3339());

        // Should be eligible (dead > 24h)
        assert!(state.is_eligible(&id, Duration::from_secs(86400)));

        // Should NOT be eligible with longer grace period
        assert!(!state.is_eligible(&id, Duration::from_secs(100 * 3600)));
    }

    #[tokio::test]
    async fn test_gc_reconciliation_deletes_orphaned_packs() {
        use crate::block::chunk_meta::{ChunkMeta, ChunkMetaEntry};
        use crate::block::content_store::ContentStore;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        // Create 3 packs: pack_a (live), pack_b (dead), pack_c (dead)
        let pack_a = Uuid::new_v4();
        let pack_b = Uuid::new_v4();
        let pack_c = Uuid::new_v4();

        let chunk_idx = 0u32;

        // Upload chunk packs to S3
        content_store
            .put_chunk_pack(chunk_idx, pack_a, vec![0u8; 100])
            .await
            .unwrap();
        content_store
            .put_chunk_pack(chunk_idx, pack_b, vec![0u8; 100])
            .await
            .unwrap();
        content_store
            .put_chunk_pack(chunk_idx, pack_c, vec![0u8; 100])
            .await
            .unwrap();

        // Create a chunk meta that only references pack_a
        let chunk_meta = ChunkMeta {
            chunk_idx,
            chunk_size: 10 * 1024 * 1024 * 1024,
            block_size: 131072,
            entries: vec![ChunkMetaEntry {
                offset: 0,
                hash: crate::block::block_map::Blake3Hash([1; 16]),
                pack_id: pack_a,
                pack_offset: 0,
                comp_length: 100,
            }],
        };
        let chunk_hash_hex = chunk_meta
            .content_hash()
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();

        content_store
            .put_chunk_meta(chunk_idx, &chunk_hash_hex, chunk_meta.serialize())
            .await
            .unwrap();

        // Create a VolumeManifest referencing this chunk
        let mut vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm.chunks.insert(chunk_idx, chunk_hash_hex);
        content_store
            .put_manifest("vm1", vm.serialize())
            .await
            .unwrap();

        // Run GC with 1h grace period
        let mut state = new_gc_state_for_test();
        // Pre-inject dead packs with old timestamp so they're eligible
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        inject_dead_pack_for_test(&mut state, &pack_b, old_ts);
        inject_dead_pack_for_test(&mut state, &pack_c, old_ts);

        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600), // 1h grace
            100,
            false, // not dry run
        )
        .await
        .unwrap();

        assert_eq!(report.manifests_scanned(), 1);
        assert_eq!(report.live_packs(), 1, "only pack_a is live");
        assert_eq!(report.known_packs(), 3, "all 3 chunk packs known");
        assert_eq!(report.dead_found(), 2, "pack_b and pack_c are dead");
        assert_eq!(report.eligible_for_deletion(), 2, "both past grace period");
        assert_eq!(report.packs_deleted(), 2, "both should be deleted");

        // Verify dead packs were removed from GC state
        assert!(!state.dead_packs.contains_key(&pack_b.to_string()));
        assert!(!state.dead_packs.contains_key(&pack_c.to_string()));
    }

    #[tokio::test]
    async fn test_gc_dry_run_doesnt_delete() {
        use crate::block::chunk_meta::ChunkMeta;
        use crate::block::content_store::ContentStore;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let dead_pack = Uuid::new_v4();
        let chunk_idx = 0u32;
        content_store
            .put_chunk_pack(chunk_idx, dead_pack, vec![0u8; 100])
            .await
            .unwrap();

        // Create an empty chunk meta (no entries -> no live packs from manifest)
        let chunk_meta = ChunkMeta::new(chunk_idx, 10 * 1024 * 1024 * 1024, 131072);
        let chunk_hash_hex = chunk_meta
            .content_hash()
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        content_store
            .put_chunk_meta(chunk_idx, &chunk_hash_hex, chunk_meta.serialize())
            .await
            .unwrap();

        // VolumeManifest referencing the empty chunk
        let mut vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm.chunks.insert(chunk_idx, chunk_hash_hex);
        content_store
            .put_manifest("vm1", vm.serialize())
            .await
            .unwrap();

        let mut state = new_gc_state_for_test();
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        inject_dead_pack_for_test(&mut state, &dead_pack, old_ts);

        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            100,
            true, // dry run
        )
        .await
        .unwrap();

        assert_eq!(
            report.packs_deleted(),
            1,
            "dry run should report as deleted"
        );

        // But the pack should still exist in S3
        // Check via list to verify
        let packs = content_store.list_chunk_packs(chunk_idx).await.unwrap();
        assert!(
            packs.iter().any(|p| p.contains(&dead_pack.to_string())),
            "pack should still exist after dry run"
        );
    }

    #[test]
    fn test_gc_state_mark_alive_removes() {
        let mut state = GcState::default();
        let id = Uuid::new_v4();
        state.mark_dead(&id);
        assert!(state.dead_packs.contains_key(&id.to_string()));

        state.mark_alive(&id);
        assert!(!state.dead_packs.contains_key(&id.to_string()));
    }

    #[tokio::test]
    async fn test_gc_cleans_orphaned_metas() {
        use crate::block::chunk_meta::{ChunkMeta, ChunkMetaEntry};
        use crate::block::content_store::ContentStore;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let pack_a = Uuid::new_v4();
        let chunk_idx = 0u32;

        // Upload a pack
        content_store
            .put_chunk_pack(chunk_idx, pack_a, vec![0u8; 100])
            .await
            .unwrap();

        // Create chunk meta referencing pack_a (the "current" version)
        let chunk_meta = ChunkMeta {
            chunk_idx,
            chunk_size: 10 * 1024 * 1024 * 1024,
            block_size: 131072,
            entries: vec![ChunkMetaEntry {
                offset: 0,
                hash: crate::block::block_map::Blake3Hash([1; 16]),
                pack_id: pack_a,
                pack_offset: 0,
                comp_length: 100,
            }],
        };
        let current_hash = chunk_meta
            .content_hash()
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();

        content_store
            .put_chunk_meta(chunk_idx, &current_hash, chunk_meta.serialize())
            .await
            .unwrap();

        // Also upload an OLD orphaned meta (different hash, simulating a previous flush)
        let orphaned_hash = "deadbeefdeadbeefdeadbeefdeadbeef";
        content_store
            .put_chunk_meta(chunk_idx, orphaned_hash, vec![0u8; 50])
            .await
            .unwrap();

        // Create VolumeManifest referencing only the current hash
        let mut vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm.chunks.insert(chunk_idx, current_hash.clone());
        content_store
            .put_manifest("vm1", vm.serialize())
            .await
            .unwrap();

        // Run GC — orphaned meta should be found but not yet eligible (grace period)
        let mut state = new_gc_state_for_test();
        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600), // 1h grace
            100,
            false,
        )
        .await
        .unwrap();

        assert_eq!(report.known_metas(), 2, "current + orphaned meta");
        assert_eq!(report.dead_metas_found(), 1, "orphaned meta is dead");
        assert_eq!(
            report.metas_deleted(),
            0,
            "not yet past grace period on first run"
        );

        // Inject dead meta with old timestamp so it's eligible
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        inject_dead_meta_for_test(&mut state, chunk_idx, orphaned_hash, old_ts);

        // Run GC again — orphaned meta should be deleted
        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            100,
            false,
        )
        .await
        .unwrap();

        assert_eq!(report.dead_metas_found(), 1);
        assert_eq!(report.eligible_metas(), 1);
        assert_eq!(report.metas_deleted(), 1, "orphaned meta should be deleted");

        // Verify: only the current meta remains
        let objects = content_store.list_all_chunk_objects().await.unwrap();
        let meta_count = objects
            .iter()
            .filter(|o| matches!(o.kind, crate::block::content_store::ChunkObjectKind::Meta(_)))
            .count();
        assert_eq!(meta_count, 1, "only current meta should remain");
    }

    #[tokio::test]
    async fn test_gc_discover_prefixes_delimiter() {
        use crate::block::content_store::ContentStore;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Create objects under two different exports
        let cs1 = ContentStore::new(Arc::clone(&s3), "db/exports/vm1");
        let cs2 = ContentStore::new(Arc::clone(&s3), "db/exports/vm2");

        cs1.put_manifest("vm1", b"{}".to_vec()).await.unwrap();
        cs2.put_manifest("vm2", b"{}".to_vec()).await.unwrap();

        // Also put a chunk pack under vm1 to ensure mixed content works
        cs1.put_chunk_pack(0, Uuid::new_v4(), vec![0u8; 100])
            .await
            .unwrap();

        let prefixes = discover_s3_prefixes(&*s3, "db").await.unwrap();
        assert_eq!(prefixes.len(), 2);
        assert!(prefixes.contains(&"db/exports/vm1".to_string()));
        assert!(prefixes.contains(&"db/exports/vm2".to_string()));
    }

    #[tokio::test]
    async fn test_gc_discover_prefixes_empty() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let prefixes = discover_s3_prefixes(&*s3, "db").await.unwrap();
        assert!(prefixes.is_empty());
    }

    #[tokio::test]
    async fn test_gc_meta_cache_hits_on_second_run() {
        use crate::block::chunk_meta::{ChunkMeta, ChunkMetaEntry};
        use crate::block::content_store::ContentStore;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let pack_a = Uuid::new_v4();
        let chunk_idx = 0u32;

        content_store
            .put_chunk_pack(chunk_idx, pack_a, vec![0u8; 100])
            .await
            .unwrap();

        let chunk_meta = ChunkMeta {
            chunk_idx,
            chunk_size: 10 * 1024 * 1024 * 1024,
            block_size: 131072,
            entries: vec![ChunkMetaEntry {
                offset: 0,
                hash: crate::block::block_map::Blake3Hash([1; 16]),
                pack_id: pack_a,
                pack_offset: 0,
                comp_length: 100,
            }],
        };
        let chunk_hash_hex = chunk_meta
            .content_hash()
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();

        content_store
            .put_chunk_meta(chunk_idx, &chunk_hash_hex, chunk_meta.serialize())
            .await
            .unwrap();

        let mut vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm.chunks.insert(chunk_idx, chunk_hash_hex);
        content_store
            .put_manifest("vm1", vm.serialize())
            .await
            .unwrap();

        // Shared cache across runs.
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = MetaCache::new(cache_dir.path().to_path_buf());

        // First run: all cache misses (cold cache).
        let mut state = new_gc_state_for_test();
        let report = reconcile_prefix_for_test_with_cache(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            100,
            false,
            &cache,
        )
        .await
        .unwrap();

        assert_eq!(report.meta_cache_hits(), 0, "first run: cold cache");
        assert_eq!(report.meta_cache_misses(), 1, "first run: 1 fetch");

        // Second run: should hit the cache.
        let report = reconcile_prefix_for_test_with_cache(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            100,
            false,
            &cache,
        )
        .await
        .unwrap();

        assert_eq!(report.meta_cache_hits(), 1, "second run: cache hit");
        assert_eq!(report.meta_cache_misses(), 0, "second run: no fetches");
    }

    #[test]
    fn test_meta_cache_prune_unseen() {
        let cache_dir = tempfile::tempdir().unwrap();

        let pack_ids: HashSet<Uuid> = [Uuid::new_v4()].into_iter().collect();

        // "First run": populate cache with two entries.
        {
            let cache = MetaCache::new(cache_dir.path().to_path_buf());
            cache.put("prefix/a", 0, "hash_a", &pack_ids);
            cache.put("prefix/b", 1, "hash_b", &pack_ids);
            // No prune — both entries survive.
        }

        // "Second run": only touch hash_a, then prune.
        {
            let cache = MetaCache::new(cache_dir.path().to_path_buf());
            assert!(cache.get("prefix/a", 0, "hash_a").is_some());
            // hash_b is NOT touched this run.

            let pruned = cache.prune_unseen().unwrap();
            assert_eq!(pruned, 1, "hash_b should be pruned");
        }

        // Verify: hash_a still exists, hash_b gone.
        let cache = MetaCache::new(cache_dir.path().to_path_buf());
        assert!(cache.get("prefix/a", 0, "hash_a").is_some());
        assert!(cache.get("prefix/b", 1, "hash_b").is_none());
    }

    #[tokio::test]
    async fn test_gc_snapshot_retention() {
        use crate::block::content_store::ContentStore;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        // Create snapshots: one "old" and one "recent".
        let vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        content_store
            .put_snapshot("vm1", 1, vm.serialize())
            .await
            .unwrap();
        content_store
            .put_snapshot("vm1", 2, vm.serialize())
            .await
            .unwrap();

        // Verify both exist.
        let snapshots = content_store.list_all_snapshots_with_dates().await.unwrap();
        assert_eq!(snapshots.len(), 2);

        // With InMemory, all objects have "now" timestamps.
        // Use a retention of 0s so everything is "expired".
        let now = Utc::now();
        let (total, deleted) = enforce_snapshot_retention(
            &content_store,
            Duration::from_secs(0), // everything expired
            now,
            false,
        )
        .await
        .unwrap();

        assert_eq!(total, 2);
        assert_eq!(deleted, 2);

        // Verify snapshots are gone.
        let snapshots = content_store.list_all_snapshots_with_dates().await.unwrap();
        assert_eq!(snapshots.len(), 0);
    }

    #[tokio::test]
    async fn test_gc_snapshot_retention_dry_run() {
        use crate::block::content_store::ContentStore;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        content_store
            .put_snapshot("vm1", 1, vm.serialize())
            .await
            .unwrap();

        let now = Utc::now();
        let (total, deleted) = enforce_snapshot_retention(
            &content_store,
            Duration::from_secs(0),
            now,
            true, // dry run
        )
        .await
        .unwrap();

        assert_eq!(total, 1);
        assert_eq!(deleted, 1, "dry run reports as deleted");

        // But snapshot should still exist.
        let snapshots = content_store.list_all_snapshots_with_dates().await.unwrap();
        assert_eq!(snapshots.len(), 1, "snapshot should survive dry run");
    }
}
