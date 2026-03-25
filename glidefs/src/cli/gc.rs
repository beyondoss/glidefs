//! Garbage collection CLI command (v4).
//!
//! Identifies and deletes orphaned packs in S3. Operates by comparing known
//! pack files (listed from S3) against live packs (referenced by binary GLVM
//! volume manifests). Packs referenced by no manifest or snapshot are dead and
//! eligible for deletion after a grace period.
//!
//! v4 simplification: pack IDs are read directly from the binary manifest via
//! `VolumeManifest::all_pack_ids()` — zero extra S3 fetches for chunk metas.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use tracing::{info, warn};

use crate::block::content_store::ContentStore;
use crate::block::pack::PackId;
use crate::block::volume_manifest::VolumeManifest;
use crate::config::Settings;
use crate::parse_object_store::parse_url_opts;

// ---------------------------------------------------------------------------
// GC State (persisted between runs for grace period tracking)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GcState {
    /// Pack key ("{chunk_idx:04}/{pack_id:016x}") -> first-seen-dead ISO 8601 timestamp.
    pub(crate) dead_packs: HashMap<String, String>,
}

/// Format a composite key for a (chunk_idx, pack_id) pair.
fn pack_key(chunk_idx: u32, pack_id: PackId) -> String {
    format!("{chunk_idx:04}/{pack_id:016x}")
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
            format!(
                "failed to atomically rename GC state to {}",
                path.display()
            )
        })?;
        Ok(())
    }

    #[cfg(test)]
    fn mark_dead_pack(&mut self, chunk_idx: u32, pack_id: PackId) {
        let key = pack_key(chunk_idx, pack_id);
        self.dead_packs
            .entry(key)
            .or_insert_with(|| Utc::now().to_rfc3339());
    }

    #[cfg(test)]
    fn mark_alive_pack(&mut self, chunk_idx: u32, pack_id: PackId) {
        self.dead_packs.remove(&pack_key(chunk_idx, pack_id));
    }

    fn is_pack_eligible(
        &self,
        chunk_idx: u32,
        pack_id: PackId,
        grace_period: Duration,
    ) -> bool {
        let key = pack_key(chunk_idx, pack_id);
        Self::is_key_eligible(&self.dead_packs, &key, grace_period)
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
    /// Packs newly seen as dead: (key, timestamp)
    newly_dead_packs: Vec<(String, String)>,
    /// Packs that became live again (were in dead state, now referenced)
    revived_packs: Vec<String>,
    /// Packs successfully deleted
    deleted_packs: Vec<String>,
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
    snapshots_checked: usize,
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
        self.snapshots_checked += other.snapshots_checked;
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
    let object_store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::from(object_store);
    let db_path = path_from_url.to_string();

    // Load GC state
    let mut state = GcState::load(&state_file)?;
    let mut stats = GcStats::default();

    // Discover all S3 prefixes that contain manifests or chunks
    let prefixes = discover_s3_prefixes(&*object_store, &db_path).await?;
    info!(count = prefixes.len(), "discovered S3 prefixes");

    // Process prefixes in parallel. Each prefix is an independent S3 namespace.
    let per_prefix_budget = max_deletes / prefixes.len().max(1);
    let results: Vec<Result<(GcStateDelta, GcStats)>> =
        futures::stream::iter(prefixes.iter())
            .map(|prefix| {
                let store = std::sync::Arc::clone(&object_store);
                let state_ref = &state;
                async move {
                    let content_store = ContentStore::new(store, prefix);
                    reconcile_prefix(
                        &content_store,
                        state_ref,
                        grace_period,
                        per_prefix_budget,
                        dry_run,
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
    println!("Snapshots checked:       {}", stats.snapshots_checked);
    println!("Known packs:             {}", stats.known_packs);
    println!("Dead packs found:        {}", stats.dead_found);
    println!("Eligible for deletion:   {}", stats.eligible_for_deletion);
    println!("Packs deleted:           {}", stats.packs_deleted);

    Ok(())
}

// ---------------------------------------------------------------------------
// S3 prefix discovery
// ---------------------------------------------------------------------------

/// Discover all unique S3 prefixes under `{db_path}/exports/`.
///
/// Uses `list_with_delimiter` to enumerate only the top-level export directory
/// names -- O(exports) instead of O(total_objects).
async fn discover_s3_prefixes(
    object_store: &dyn object_store::ObjectStore,
    db_path: &str,
) -> Result<Vec<String>> {
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
// S3 pack listing (test-only, reconcile_prefix streams inline)
// ---------------------------------------------------------------------------

/// List all v4 pack files across chunk directories.
///
/// Scans `{base_path}/chunks/` and parses filenames matching
/// `{idx:04}/{pack_id:016x}.pack`. Returns `(chunk_idx, pack_id)` pairs.
#[cfg(test)]
async fn list_all_packs(
    content_store: &ContentStore,
) -> Result<HashSet<(u32, PackId)>> {
    let base = content_store.base_path();
    let chunks_prefix_str = format!("{}/chunks/", base);
    let chunks_prefix = ObjectPath::from(chunks_prefix_str.clone());

    let mut packs = HashSet::new();
    let mut stream = content_store.object_store().list(Some(&chunks_prefix));

    while let Some(result) = stream.next().await {
        let meta = result?;
        let path_str = meta.location.to_string();
        let Some(rel) = path_str.strip_prefix(&chunks_prefix_str) else {
            continue;
        };
        let Some(slash_pos) = rel.find('/') else {
            continue;
        };
        let Ok(chunk_idx) = rel[..slash_pos].parse::<u32>() else {
            continue;
        };
        let filename = &rel[slash_pos + 1..];
        if let Some(hex_str) = filename.strip_suffix(".pack")
            && hex_str.len() == 16
            && let Ok(pack_id) = u64::from_str_radix(hex_str, 16)
        {
            packs.insert((chunk_idx, pack_id));
        }
    }

    Ok(packs)
}

// ---------------------------------------------------------------------------
// Reconciliation (two-pass, snapshot-count-independent memory)
// ---------------------------------------------------------------------------

/// Parse a pack path relative to the chunks prefix.
/// Returns `(chunk_idx, pack_id)` or None if the path doesn't match.
fn parse_pack_path(rel: &str) -> Option<(u32, PackId)> {
    let slash_pos = rel.find('/')?;
    let chunk_idx = rel[..slash_pos].parse::<u32>().ok()?;
    let filename = &rel[slash_pos + 1..];
    let hex_str = filename.strip_suffix(".pack")?;
    if hex_str.len() != 16 {
        return None;
    }
    let pack_id = u64::from_str_radix(hex_str, 16).ok()?;
    Some((chunk_idx, pack_id))
}

/// Reconcile a single S3 prefix: find and delete orphaned packs.
///
/// Two-pass algorithm with memory independent of snapshot count:
///
/// **Pass 1**: Build live set from live manifests only (bounded by volume size).
/// Stream S3 pack list, collect packs not in live set → `maybe_dead` HashSet.
///
/// **Pass 2**: Stream snapshot manifests one at a time. For each, remove any
/// referenced packs from `maybe_dead`. Whatever survives is truly dead.
///
/// Memory: O(live_manifest_packs + maybe_dead + max_deletes).
/// Snapshot manifests are never held simultaneously.
async fn reconcile_prefix(
    content_store: &ContentStore,
    state: &GcState,
    grace_period: Duration,
    max_deletes: usize,
    dry_run: bool,
) -> Result<(GcStateDelta, GcStats)> {
    let mut stats = GcStats::default();
    let mut delta = GcStateDelta::default();
    let mut manifest_failed = false;

    // Pass 1a: Stream live manifests 16-wide parallel, collect live (chunk_idx, pack_id) pairs.
    // This is bounded by the volume's current pack count, not snapshot count.
    let mut live_packs: HashSet<(u32, PackId)> = HashSet::new();

    let base = content_store.base_path();
    let manifests_prefix_str = format!("{}/manifests/", base);
    let manifests_prefix = ObjectPath::from(manifests_prefix_str.clone());

    let store = std::sync::Arc::clone(content_store.object_store());
    let prefix_for_map = manifests_prefix_str.clone();
    let mut manifest_results = content_store
        .object_store()
        .list(Some(&manifests_prefix))
        .map(move |result| {
            let store = std::sync::Arc::clone(&store);
            let prefix = prefix_for_map.clone();
            async move {
                let meta = result
                    .map_err(|e| anyhow::anyhow!("failed to list manifests: {}", e))?;
                let path_str = meta.location.to_string();
                let name = match path_str.strip_prefix(&prefix) {
                    Some(r) if !r.is_empty() && !r.ends_with(".hot-set") => r.to_string(),
                    _ => return Ok(None), // skip .hot-set and non-manifest entries
                };
                let response = match store.get(&meta.location).await {
                    Ok(r) => r,
                    Err(object_store::Error::NotFound { .. }) => {
                        warn!(manifest = %name, "manifest disappeared during GC");
                        return Ok(None);
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!(
                            "failed to fetch manifest {}: {}",
                            name,
                            e
                        ))
                    }
                };
                let data = response
                    .bytes()
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to read manifest {} bytes: {}", name, e))?;
                match VolumeManifest::deserialize(&data.to_vec()) {
                    Ok(vm) => Ok(Some((name, vm.all_pack_ids()))),
                    Err(e) => Err(anyhow::anyhow!(
                        "failed to parse volume manifest {}: {}",
                        name,
                        e
                    )),
                }
            }
        })
        .buffer_unordered(16);

    while let Some(result) = manifest_results.next().await {
        match result {
            Ok(Some((_name, pack_ids))) => {
                live_packs.extend(pack_ids);
                stats.manifests_scanned += 1;
            }
            Ok(None) => {} // manifest disappeared, already warned above
            Err(e) => {
                warn!("{} — treating all packs in prefix as live", e);
                stats.manifest_errors += 1;
                manifest_failed = true;
            }
        }
    }

    if manifest_failed {
        warn!("skipping GC for prefix due to manifest errors — no packs will be deleted");
        return Ok((delta, stats));
    }

    stats.live_packs += live_packs.len();

    // Pass 1b: Stream S3 pack list, classify against live manifests.
    // Packs in live set → revive if previously dead.
    // Packs NOT in live set → maybe_dead (needs snapshot check).
    let mut maybe_dead: HashSet<(u32, PackId)> = HashSet::new();

    let base = content_store.base_path();
    let chunks_prefix_str = format!("{}/chunks/", base);
    let chunks_prefix = ObjectPath::from(chunks_prefix_str.clone());
    let mut stream = content_store.object_store().list(Some(&chunks_prefix));

    while let Some(result) = stream.next().await {
        let meta = result?;
        let path_str = meta.location.to_string();
        let Some(rel) = path_str.strip_prefix(&chunks_prefix_str) else {
            continue;
        };
        let Some((chunk_idx, pack_id)) = parse_pack_path(rel) else {
            continue;
        };

        stats.known_packs += 1;

        if live_packs.contains(&(chunk_idx, pack_id)) {
            // Definitely live — revive if previously marked dead
            let key = pack_key(chunk_idx, pack_id);
            if state.dead_packs.contains_key(&key) {
                delta.revived_packs.push(key);
            }
        } else {
            maybe_dead.insert((chunk_idx, pack_id));
        }
    }

    // Drop the live set — no longer needed, free memory before pass 2.
    drop(live_packs);

    if maybe_dead.is_empty() {
        stats.prefixes_scanned += 1;
        return Ok((delta, stats));
    }

    info!(
        maybe_dead = maybe_dead.len(),
        "pass 1 complete, checking snapshot manifests"
    );

    // Pass 2: Check snapshot manifests 16-wide parallel.
    // Each task: fetch → deserialize → return pack_ids.
    // Consumer removes from maybe_dead, early-exits when empty.
    // Any unrecoverable error aborts the prefix (delete nothing).
    let snap_prefix_str = format!("{}/snapshots/", base);
    let snap_prefix = ObjectPath::from(snap_prefix_str);

    let store = std::sync::Arc::clone(content_store.object_store());
    let mut snap_results = content_store
        .object_store()
        .list(Some(&snap_prefix))
        .map(move |result| {
            let store = std::sync::Arc::clone(&store);
            async move {
                let path = result
                    .map_err(|e| anyhow::anyhow!("failed to list snapshots: {}", e))?
                    .location;
                let response = match store.get(&path).await {
                    Ok(r) => r,
                    Err(object_store::Error::NotFound { .. }) => return Ok(None),
                    Err(e) => {
                        return Err(anyhow::anyhow!(
                            "failed to fetch snapshot manifest {}: {}",
                            path,
                            e
                        ));
                    }
                };
                let data = response.bytes().await.map_err(|e| {
                    anyhow::anyhow!(
                        "failed to read snapshot manifest {} bytes: {}",
                        path,
                        e
                    )
                })?;
                let vm = VolumeManifest::deserialize(&data).map_err(|e| {
                    anyhow::anyhow!("corrupt snapshot manifest {}: {}", path, e)
                })?;
                Ok(Some(vm.all_pack_ids()))
            }
        })
        .buffer_unordered(16);

    while let Some(result) = snap_results.next().await {
        match result {
            Ok(Some(pack_ids)) => {
                for id in pack_ids {
                    maybe_dead.remove(&id);
                }
                stats.snapshots_checked += 1;
                if maybe_dead.is_empty() {
                    break; // All candidates accounted for, remaining futures dropped
                }
            }
            Ok(None) => {} // NotFound — snapshot deleted between list and get, skip
            Err(e) => {
                warn!("{} — treating remaining maybe_dead as live", e);
                stats.prefixes_scanned += 1;
                return Ok((delta, stats));
            }
        }
    }
    // Snapshot manifests are never held simultaneously — each task's
    // HashSet is consumed and dropped on the consumer side.

    // Whatever survives pass 2 is truly dead.
    let now_ts = Utc::now().to_rfc3339();
    stats.dead_found += maybe_dead.len();

    let mut eligible: Vec<(u32, PackId)> = Vec::new();
    for &(chunk_idx, pack_id) in &maybe_dead {
        let key = pack_key(chunk_idx, pack_id);
        let is_new = !state.dead_packs.contains_key(&key);
        if is_new {
            delta.newly_dead_packs.push((key, now_ts.clone()));
        }

        let is_eligible = state.is_pack_eligible(chunk_idx, pack_id, grace_period)
            || (grace_period.is_zero() && is_new);
        if is_eligible {
            stats.eligible_for_deletion += 1;
            if eligible.len() < max_deletes {
                eligible.push((chunk_idx, pack_id));
            }
        }
    }

    // Delete eligible packs (already capped at max_deletes, parallel)
    if dry_run {
        for &(chunk_idx, pack_id) in &eligible {
            info!(chunk_idx, pack_id, "would delete orphaned pack (dry-run)");
        }
        stats.packs_deleted += eligible.len();
    } else {
        let delete_results: Vec<((u32, PackId), Result<(), _>)> =
            futures::stream::iter(eligible.iter().copied())
                .map(|(chunk_idx, pack_id)| {
                    let cs = &content_store;
                    async move {
                        let result = cs.delete_chunk_pack(chunk_idx, pack_id).await;
                        ((chunk_idx, pack_id), result)
                    }
                })
                .buffer_unordered(32)
                .collect()
                .await;

        for ((chunk_idx, pack_id), result) in delete_results {
            match result {
                Ok(()) => {
                    delta.deleted_packs.push(pack_key(chunk_idx, pack_id));
                    stats.packs_deleted += 1;
                }
                Err(e) => {
                    warn!(chunk_idx, pack_id, error = %e, "failed to delete pack");
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
        anyhow::bail!(
            "invalid duration '{}': use suffix h/m/s/d (e.g. '24h')",
            s
        )
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
    let (delta, stats) = reconcile_prefix(
        content_store,
        state,
        grace_period,
        max_deletes,
        dry_run,
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
pub fn inject_dead_pack_for_test(
    state: &mut GcState,
    chunk_idx: u32,
    pack_id: PackId,
    timestamp: DateTime<Utc>,
) {
    let key = pack_key(chunk_idx, pack_id);
    state.dead_packs.insert(key, timestamp.to_rfc3339());
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
    pub fn deleted_count(&self) -> usize {
        self.stats.packs_deleted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use crate::block::content_store::ContentStore;
    use crate::block::pack::PackId;
    use crate::block::volume_manifest::VolumeManifest;

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
        state.mark_dead_pack(0, 0xDEADBEEF);
        state.save(&path).unwrap();

        let loaded = GcState::load(&path).unwrap();
        assert!(loaded.dead_packs.contains_key(&pack_key(0, 0xDEADBEEF)));
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
        let chunk_idx = 0u32;
        let pack_id: PackId = 0x1234567890ABCDEF;

        // Not in state -> not eligible
        assert!(!state.is_pack_eligible(chunk_idx, pack_id, Duration::from_secs(3600)));

        // Mark dead with a timestamp in the past
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        state
            .dead_packs
            .insert(pack_key(chunk_idx, pack_id), old_ts.to_rfc3339());

        // Should be eligible (dead > 24h)
        assert!(state.is_pack_eligible(chunk_idx, pack_id, Duration::from_secs(86400)));

        // Should NOT be eligible with longer grace period
        assert!(!state.is_pack_eligible(chunk_idx, pack_id, Duration::from_secs(100 * 3600)));
    }

    #[test]
    fn test_gc_state_mark_alive_removes() {
        let mut state = GcState::default();
        let chunk_idx = 0u32;
        let pack_id: PackId = 0xCAFEBABE;
        state.mark_dead_pack(chunk_idx, pack_id);
        assert!(
            state
                .dead_packs
                .contains_key(&pack_key(chunk_idx, pack_id))
        );

        state.mark_alive_pack(chunk_idx, pack_id);
        assert!(
            !state
                .dead_packs
                .contains_key(&pack_key(chunk_idx, pack_id))
        );
    }

    #[test]
    fn test_pack_key_format() {
        assert_eq!(pack_key(0, 0xDEADBEEF01234567), "0000/deadbeef01234567");
        assert_eq!(pack_key(42, 0x0000000000000001), "0042/0000000000000001");
    }

    #[tokio::test]
    async fn test_gc_reconciliation_deletes_orphaned_packs() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        // Create 3 packs: pack_a (live), pack_b (dead), pack_c (dead)
        let pack_a: PackId = 0xAAAAAAAAAAAAAAAA;
        let pack_b: PackId = 0xBBBBBBBBBBBBBBBB;
        let pack_c: PackId = 0xCCCCCCCCCCCCCCCC;

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

        // Create a VolumeManifest referencing only pack_a
        let mut vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm.append_pack(chunk_idx, pack_a);
        content_store
            .put_manifest("vm1", vm.serialize().unwrap(), None)
            .await
            .unwrap();

        // Run GC with 1h grace period
        let mut state = new_gc_state_for_test();
        // Pre-inject dead packs with old timestamp so they're eligible
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        inject_dead_pack_for_test(&mut state, chunk_idx, pack_b, old_ts);
        inject_dead_pack_for_test(&mut state, chunk_idx, pack_c, old_ts);

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
        assert!(!state
            .dead_packs
            .contains_key(&pack_key(chunk_idx, pack_b)));
        assert!(!state
            .dead_packs
            .contains_key(&pack_key(chunk_idx, pack_c)));
    }

    #[tokio::test]
    async fn test_gc_dry_run_doesnt_delete() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let dead_pack: PackId = 0xDEADDEADDEADDEAD;
        let chunk_idx = 0u32;
        content_store
            .put_chunk_pack(chunk_idx, dead_pack, vec![0u8; 100])
            .await
            .unwrap();

        // Create a VolumeManifest with no packs (empty manifest -> no live packs)
        let vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        content_store
            .put_manifest("vm1", vm.serialize().unwrap(), None)
            .await
            .unwrap();

        let mut state = new_gc_state_for_test();
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        inject_dead_pack_for_test(&mut state, chunk_idx, dead_pack, old_ts);

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
        let packs = list_all_packs(&content_store).await.unwrap();
        assert!(
            packs.contains(&(chunk_idx, dead_pack)),
            "pack should still exist after dry run"
        );
    }

    #[tokio::test]
    async fn test_gc_discover_prefixes_delimiter() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Create objects under two different exports
        let cs1 = ContentStore::new(Arc::clone(&s3), "db/exports/vm1");
        let cs2 = ContentStore::new(Arc::clone(&s3), "db/exports/vm2");

        cs1.put_manifest("vm1", b"test".to_vec(), None).await.unwrap();
        cs2.put_manifest("vm2", b"test".to_vec(), None).await.unwrap();

        // Also put a chunk pack under vm1 to ensure mixed content works
        cs1.put_chunk_pack(0, 0x1234567890ABCDEF, vec![0u8; 100])
            .await
            .unwrap();

        let prefixes = discover_s3_prefixes(&*s3, "db").await.unwrap();
        assert_eq!(prefixes.len(), 2);
        assert!(prefixes.contains(&"db/exports/vm1".to_string()));
        assert!(prefixes.contains(&"db/exports/vm2".to_string()));
    }

    #[tokio::test]
    async fn test_gc_discover_prefixes_empty() {
        let s3: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefixes = discover_s3_prefixes(&*s3, "db").await.unwrap();
        assert!(prefixes.is_empty());
    }

    #[tokio::test]
    async fn test_gc_grace_period_blocks_new_dead() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let dead_pack: PackId = 0xDEAD000000000001;
        let chunk_idx = 0u32;
        content_store
            .put_chunk_pack(chunk_idx, dead_pack, vec![0u8; 100])
            .await
            .unwrap();

        // Empty manifest -> dead_pack is unreferenced.
        let vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        content_store
            .put_manifest("vm1", vm.serialize().unwrap(), None)
            .await
            .unwrap();

        // First run: pack is newly dead, 1h grace period -> not eligible.
        let mut state = new_gc_state_for_test();
        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            100,
            false,
        )
        .await
        .unwrap();

        assert_eq!(report.dead_found(), 1);
        assert_eq!(
            report.eligible_for_deletion(),
            0,
            "newly dead, not yet past grace"
        );
        assert_eq!(report.packs_deleted(), 0);

        // The pack should be tracked as dead now.
        assert!(state
            .dead_packs
            .contains_key(&pack_key(chunk_idx, dead_pack)));
    }

    #[tokio::test]
    async fn test_gc_zero_grace_period_deletes_immediately() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let dead_pack: PackId = 0xDEAD000000000002;
        let chunk_idx = 0u32;
        content_store
            .put_chunk_pack(chunk_idx, dead_pack, vec![0u8; 100])
            .await
            .unwrap();

        let vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        content_store
            .put_manifest("vm1", vm.serialize().unwrap(), None)
            .await
            .unwrap();

        let mut state = new_gc_state_for_test();
        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(0), // zero grace
            100,
            false,
        )
        .await
        .unwrap();

        assert_eq!(report.dead_found(), 1);
        assert_eq!(
            report.eligible_for_deletion(),
            1,
            "zero grace: immediately eligible"
        );
        assert_eq!(report.packs_deleted(), 1);
    }

    #[tokio::test]
    async fn test_gc_revives_packs_that_become_live() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let pack_id: PackId = 0xAAAA000000000001;
        let chunk_idx = 0u32;
        content_store
            .put_chunk_pack(chunk_idx, pack_id, vec![0u8; 100])
            .await
            .unwrap();

        // First: manifest does NOT reference pack -> it's dead.
        let vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        content_store
            .put_manifest("vm1", vm.serialize().unwrap(), None)
            .await
            .unwrap();

        let mut state = new_gc_state_for_test();
        let _ = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            100,
            false,
        )
        .await
        .unwrap();

        assert!(state
            .dead_packs
            .contains_key(&pack_key(chunk_idx, pack_id)));

        // Now update manifest to reference the pack -> it's live again.
        let mut vm2 = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm2.append_pack(chunk_idx, pack_id);
        content_store
            .put_manifest("vm1", vm2.serialize().unwrap(), None)
            .await
            .unwrap();

        let _ = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            100,
            false,
        )
        .await
        .unwrap();

        // Should be revived (removed from dead_packs).
        assert!(!state
            .dead_packs
            .contains_key(&pack_key(chunk_idx, pack_id)));
    }

    #[tokio::test]
    async fn test_gc_multi_chunk_packs() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        // Packs in different chunks.
        let pack_0a: PackId = 0x0A0A0A0A0A0A0A0A;
        let pack_0b: PackId = 0x0B0B0B0B0B0B0B0B;
        let pack_5a: PackId = 0x5A5A5A5A5A5A5A5A;

        content_store
            .put_chunk_pack(0, pack_0a, vec![0u8; 100])
            .await
            .unwrap();
        content_store
            .put_chunk_pack(0, pack_0b, vec![0u8; 100])
            .await
            .unwrap();
        content_store
            .put_chunk_pack(5, pack_5a, vec![0u8; 100])
            .await
            .unwrap();

        // Manifest references pack_0a in chunk 0 and pack_5a in chunk 5.
        let mut vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm.append_pack(0, pack_0a);
        vm.append_pack(5, pack_5a);
        content_store
            .put_manifest("vm1", vm.serialize().unwrap(), None)
            .await
            .unwrap();

        let mut state = new_gc_state_for_test();
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        inject_dead_pack_for_test(&mut state, 0, pack_0b, old_ts);

        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            100,
            false,
        )
        .await
        .unwrap();

        assert_eq!(report.live_packs(), 2, "pack_0a and pack_5a are live");
        assert_eq!(report.known_packs(), 3);
        assert_eq!(report.dead_found(), 1, "only pack_0b is dead");
        assert_eq!(report.packs_deleted(), 1);
    }

    #[tokio::test]
    async fn test_gc_snapshot_packs_are_live() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let pack_main: PackId = 0x1111111111111111;
        let pack_snap: PackId = 0x2222222222222222;
        let chunk_idx = 0u32;

        content_store
            .put_chunk_pack(chunk_idx, pack_main, vec![0u8; 100])
            .await
            .unwrap();
        content_store
            .put_chunk_pack(chunk_idx, pack_snap, vec![0u8; 100])
            .await
            .unwrap();

        // Main manifest references only pack_main.
        let mut vm_main = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm_main.append_pack(chunk_idx, pack_main);
        content_store
            .put_manifest("vm1", vm_main.serialize().unwrap(), None)
            .await
            .unwrap();

        // Snapshot references pack_snap (keeping it alive).
        let mut vm_snap = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm_snap.append_pack(chunk_idx, pack_snap);
        content_store
            .put_snapshot("vm1", 1, vm_snap.serialize().unwrap())
            .await
            .unwrap();

        let mut state = new_gc_state_for_test();
        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(0),
            100,
            false,
        )
        .await
        .unwrap();

        // pack_main is live from manifest, pack_snap is saved by snapshot.
        assert_eq!(report.live_packs(), 1, "only pack_main from live manifest");
        assert_eq!(report.dead_found(), 0, "snapshot removed pack_snap from maybe_dead");
        assert_eq!(report.packs_deleted(), 0);
    }

    #[tokio::test]
    async fn test_gc_max_deletes_cap() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let chunk_idx = 0u32;

        // Create 5 dead packs.
        let dead_packs: Vec<PackId> = (1..=5u64).map(|i| i * 0x1000000000000000).collect();
        for &pack_id in &dead_packs {
            content_store
                .put_chunk_pack(chunk_idx, pack_id, vec![0u8; 100])
                .await
                .unwrap();
        }

        // Empty manifest -> all packs are dead.
        let vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        content_store
            .put_manifest("vm1", vm.serialize().unwrap(), None)
            .await
            .unwrap();

        let mut state = new_gc_state_for_test();
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        for &pack_id in &dead_packs {
            inject_dead_pack_for_test(&mut state, chunk_idx, pack_id, old_ts);
        }

        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(3600),
            2, // max_deletes = 2
            false,
        )
        .await
        .unwrap();

        assert_eq!(report.dead_found(), 5);
        assert_eq!(report.eligible_for_deletion(), 5);
        assert_eq!(report.packs_deleted(), 2, "capped at max_deletes");
    }

    #[tokio::test]
    async fn test_list_all_packs_parsing() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        // Upload packs in different chunks.
        content_store
            .put_chunk_pack(0, 0xDEADBEEF01234567, vec![0u8; 10])
            .await
            .unwrap();
        content_store
            .put_chunk_pack(42, 0x0000000000000001, vec![0u8; 10])
            .await
            .unwrap();
        content_store
            .put_chunk_pack(0, 0xCAFEBABE89ABCDEF, vec![0u8; 10])
            .await
            .unwrap();

        let packs = list_all_packs(&content_store).await.unwrap();
        assert_eq!(packs.len(), 3);
        assert!(packs.contains(&(0, 0xDEADBEEF01234567)));
        assert!(packs.contains(&(42, 0x0000000000000001)));
        assert!(packs.contains(&(0, 0xCAFEBABE89ABCDEF)));
    }

    // -----------------------------------------------------------------------
    // FaultyGetStore: ObjectStore wrapper that injects GET errors for specific
    // S3 keys. Used to verify GC aborts when snapshot reads fail.
    // -----------------------------------------------------------------------

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    };
    use std::sync::Mutex;

    #[derive(Debug)]
    struct FaultyGetStore {
        inner: InMemory,
        /// S3 keys where get() should return a non-NotFound error.
        fault_keys: Mutex<HashSet<String>>,
    }

    impl FaultyGetStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                fault_keys: Mutex::new(HashSet::new()),
            }
        }

        fn fail_on_get(&self, key: &str) {
            self.fault_keys.lock().unwrap().insert(key.to_string());
        }
    }

    impl std::fmt::Display for FaultyGetStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FaultyGetStore")
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for FaultyGetStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            if self
                .fault_keys
                .lock()
                .unwrap()
                .contains(&location.to_string())
            {
                return Err(object_store::Error::Generic {
                    store: "FaultyGetStore",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "simulated snapshot read failure",
                    )),
                });
            }
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &ObjectPath) -> ObjectStoreResult<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectStoreResult<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// Regression test: if a snapshot manifest can't be read, GC must NOT
    /// delete packs that the unreadable snapshot might reference.
    ///
    /// Before the fix, a bytes() read error on a snapshot caused `continue`
    /// (skip that snapshot), leaving its packs in maybe_dead → incorrect
    /// deletion. The fix aborts the entire prefix when any snapshot is
    /// unreadable.
    #[tokio::test]
    async fn test_gc_aborts_when_snapshot_read_fails() {
        let faulty = Arc::new(FaultyGetStore::new());
        let s3: Arc<dyn object_store::ObjectStore> = Arc::clone(&faulty) as _;
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        let pack_main: PackId = 0x1111111111111111;
        let pack_snap_only: PackId = 0x2222222222222222;
        let chunk_idx = 0u32;

        // Upload both packs to S3.
        content_store
            .put_chunk_pack(chunk_idx, pack_main, vec![0u8; 100])
            .await
            .unwrap();
        content_store
            .put_chunk_pack(chunk_idx, pack_snap_only, vec![0u8; 100])
            .await
            .unwrap();

        // Live manifest references only pack_main.
        let mut vm_main = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm_main.append_pack(chunk_idx, pack_main);
        content_store
            .put_manifest("vm1", vm_main.serialize().unwrap(), None)
            .await
            .unwrap();

        // Snapshot references pack_snap_only (the only thing keeping it alive).
        let mut vm_snap = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm_snap.append_pack(chunk_idx, pack_snap_only);
        content_store
            .put_snapshot("vm1", 1, vm_snap.serialize().unwrap())
            .await
            .unwrap();

        // Make the snapshot GET fail (simulates network error mid-read).
        faulty.fail_on_get("test/exports/vm1/snapshots/vm1/00000000000000000001");

        // Run GC with zero grace period — would delete immediately if it
        // incorrectly skips the failed snapshot.
        let mut state = new_gc_state_for_test();
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        inject_dead_pack_for_test(&mut state, chunk_idx, pack_snap_only, old_ts);

        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::from_secs(0),
            100,
            false,
        )
        .await
        .unwrap();

        // GC must abort the prefix — zero deletions.
        assert_eq!(
            report.packs_deleted(),
            0,
            "must not delete packs when a snapshot is unreadable"
        );

        // pack_snap_only must still exist in S3.
        let packs = list_all_packs(&content_store).await.unwrap();
        assert!(
            packs.contains(&(chunk_idx, pack_snap_only)),
            "snapshot-pinned pack must survive when snapshot read fails"
        );
    }

    /// Verify that GC on a shared prefix cannot delete packs referenced by
    /// the base manifest, even when fork manifests are added/removed.
    ///
    /// Reproduces the scenario:
    /// 1. Base manifest references packs A, B, C
    /// 2. Fork manifest references packs A, B, C + D (fork's own write)
    /// 3. Fork is removed WITHOUT purge (manifest stays — the purge bug)
    /// 4. GC runs — all packs should be live (base + orphaned fork manifests)
    /// 5. Fork manifest is manually deleted
    /// 6. GC runs again — pack D is dead, but A, B, C are still live via base
    /// 7. New fork created from base — should be able to read A, B, C
    #[tokio::test]
    async fn test_gc_shared_prefix_base_packs_survive_fork_cleanup() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/bases");

        let chunk_idx = 0u32;
        let pack_a: PackId = 0xAAAAAAAAAAAAAAAA;
        let pack_b: PackId = 0xBBBBBBBBBBBBBBBB;
        let pack_c: PackId = 0xCCCCCCCCCCCCCCCC;
        let pack_d: PackId = 0xDDDDDDDDDDDDDDDD; // fork-only pack

        // Upload all packs to S3
        for &pid in &[pack_a, pack_b, pack_c, pack_d] {
            content_store
                .put_chunk_pack(chunk_idx, pid, vec![0u8; 100])
                .await
                .unwrap();
        }

        // Base manifest references A, B, C
        let mut base_vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        base_vm.append_pack(chunk_idx, pack_a);
        base_vm.append_pack(chunk_idx, pack_b);
        base_vm.append_pack(chunk_idx, pack_c);
        content_store
            .put_manifest("bases/ubuntu", base_vm.serialize().unwrap(), None)
            .await
            .unwrap();

        // Fork manifest references A, B, C + D
        let mut fork_vm = base_vm.clone();
        fork_vm.append_pack(chunk_idx, pack_d);
        content_store
            .put_manifest("fork-1", fork_vm.serialize().unwrap(), None)
            .await
            .unwrap();

        // GC run 1: both manifests present, all packs live
        let mut state = new_gc_state_for_test();
        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::ZERO,
            100,
            false,
        )
        .await
        .unwrap();

        assert_eq!(report.manifests_scanned(), 2);
        assert_eq!(report.live_packs(), 4);
        assert_eq!(report.dead_found(), 0);
        assert_eq!(report.packs_deleted(), 0);

        // Simulate fork cleanup: delete fork manifest (as our purge fix does)
        content_store.delete_manifest("fork-1").await.unwrap();

        // GC run 2: only base manifest, pack_d should be dead
        let mut state2 = new_gc_state_for_test();
        let report2 = reconcile_prefix_for_test(
            &content_store,
            &mut state2,
            Duration::ZERO,
            100,
            false,
        )
        .await
        .unwrap();

        assert_eq!(report2.manifests_scanned(), 1, "only base manifest");
        assert_eq!(report2.live_packs(), 3, "A, B, C still live");
        assert_eq!(report2.dead_found(), 1, "only pack_d is dead");
        assert_eq!(report2.packs_deleted(), 1, "pack_d deleted");

        // Verify base packs survive
        let packs = list_all_packs(&content_store).await.unwrap();
        assert!(packs.contains(&(chunk_idx, pack_a)), "base pack A must survive");
        assert!(packs.contains(&(chunk_idx, pack_b)), "base pack B must survive");
        assert!(packs.contains(&(chunk_idx, pack_c)), "base pack C must survive");
        assert!(!packs.contains(&(chunk_idx, pack_d)), "fork pack D should be deleted");
    }

    /// Verify the live > known invariant: after GC, every pack referenced
    /// by a manifest must exist in S3. Tests that GC cannot create a state
    /// where live_packs > known_packs.
    #[tokio::test]
    async fn test_gc_never_deletes_live_packs() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/bases");

        let chunk_idx = 0u32;
        let base_pack: PackId = 0x1111111111111111;
        let fork_pack: PackId = 0x2222222222222222;
        let orphan_pack: PackId = 0x3333333333333333;

        // Upload packs
        for &pid in &[base_pack, fork_pack, orphan_pack] {
            content_store
                .put_chunk_pack(chunk_idx, pid, vec![0u8; 100])
                .await
                .unwrap();
        }

        // Base manifest references base_pack
        let mut base_vm = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        base_vm.append_pack(chunk_idx, base_pack);
        content_store
            .put_manifest("bases/ubuntu", base_vm.serialize().unwrap(), None)
            .await
            .unwrap();

        // Fork manifest references base_pack + fork_pack
        let mut fork_vm = base_vm.clone();
        fork_vm.append_pack(chunk_idx, fork_pack);
        content_store
            .put_manifest("fork-1", fork_vm.serialize().unwrap(), None)
            .await
            .unwrap();

        // Run GC with zero grace period — orphan_pack should be deleted
        let mut state = new_gc_state_for_test();
        let report = reconcile_prefix_for_test(
            &content_store,
            &mut state,
            Duration::ZERO,
            100,
            false,
        )
        .await
        .unwrap();

        assert_eq!(report.packs_deleted(), 1, "only orphan_pack deleted");

        // Verify invariant: every pack in every manifest must exist in S3
        let surviving_packs = list_all_packs(&content_store).await.unwrap();
        let manifest_names = content_store.list_all_manifests().await.unwrap();
        for name in &manifest_names {
            if let Ok(Some((data, _))) = content_store.get_manifest(name).await {
                let vm = VolumeManifest::deserialize(&data).unwrap();
                for (cidx, pid) in vm.all_pack_ids() {
                    assert!(
                        surviving_packs.contains(&(cidx, pid)),
                        "manifest '{}' references pack ({}, {:016x}) which was deleted by GC",
                        name, cidx, pid,
                    );
                }
            }
        }
    }
}
