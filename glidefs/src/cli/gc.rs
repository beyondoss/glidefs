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
        self.snapshots_scanned += other.snapshots_scanned;
        self.snapshots_deleted += other.snapshots_deleted;
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
    let object_store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::from(object_store);
    let db_path = path_from_url.to_string();

    // Load GC state
    let mut state = GcState::load(&state_file)?;
    let mut stats = GcStats::default();

    // Parse snapshot retention if provided.
    let snapshot_retention = snapshot_retention_str
        .as_deref()
        .map(parse_duration)
        .transpose()?;

    // Discover all S3 prefixes that contain manifests or chunks
    let prefixes = discover_s3_prefixes(&*object_store, &db_path).await?;
    info!(count = prefixes.len(), "discovered S3 prefixes");

    // Phase 0: Snapshot retention (runs BEFORE pack reconciliation).
    // Fewer live snapshots = smaller live set = more dead packs eligible for cleanup.
    if let Some(retention) = snapshot_retention {
        let now = Utc::now();
        let mut snapshots_deleted: usize = 0;
        let mut snapshots_total: usize = 0;

        let retention_results: Vec<Result<(usize, usize)>> =
            futures::stream::iter(prefixes.iter())
                .map(|prefix| {
                    let store = std::sync::Arc::clone(&object_store);
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
    println!("Known packs:             {}", stats.known_packs);
    println!("Dead packs found:        {}", stats.dead_found);
    println!("Eligible for deletion:   {}", stats.eligible_for_deletion);
    println!("Packs deleted:           {}", stats.packs_deleted);
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

    let cutoff = now
        - chrono::TimeDelta::from_std(retention).unwrap_or(chrono::TimeDelta::MAX);

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
// S3 pack listing
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
        // rel = "{idx:04}/{pack_id:016x}.pack"
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
// Snapshot manifest loading
// ---------------------------------------------------------------------------

/// Stream snapshot manifests for a prefix, inserting live pack IDs directly
/// into the provided set. Each manifest is deserialized and dropped before
/// fetching the next, avoiding O(snapshots) memory for manifests.
///
/// Returns the number of snapshot manifests successfully scanned.
async fn collect_snapshot_pack_ids(
    content_store: &ContentStore,
    live_packs: &mut HashSet<(u32, PackId)>,
) -> Result<usize> {
    let base = content_store.base_path();
    let prefix_str = format!("{}/snapshots/", base);
    let prefix = ObjectPath::from(prefix_str);

    let mut paths = Vec::new();
    let mut stream = content_store.object_store().list(Some(&prefix));
    while let Some(result) = stream.next().await {
        match result {
            Ok(meta) => paths.push(meta.location),
            Err(e) => return Err(e.into()),
        }
    }

    let mut scanned = 0usize;
    for path in paths {
        let data = match content_store.object_store().get(&path).await {
            Ok(response) => match response.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(key = %path, error = %e, "failed to read snapshot manifest bytes");
                    continue;
                }
            },
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => {
                warn!(key = %path, error = %e, "failed to fetch snapshot manifest");
                continue;
            }
        };
        match VolumeManifest::deserialize(&data) {
            Ok(vm) => {
                live_packs.extend(vm.all_pack_ids());
                scanned += 1;
            }
            Err(e) => {
                warn!(key = %path, error = %e, "corrupt snapshot manifest, skipping");
            }
        }
    }

    Ok(scanned)
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Reconcile a single S3 prefix: find and delete orphaned packs.
///
/// Reads all manifests + snapshots as binary VolumeManifest and extracts live
/// pack IDs directly via `all_pack_ids()`. Lists pack files from S3 to get
/// the known set. Dead = known - live. Grace period and deletion cap apply.
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

    // Phase 1: Read all manifests, collect live (chunk_idx, pack_id) pairs.
    let mut live_packs: HashSet<(u32, PackId)> = HashSet::new();

    let manifest_names = content_store.list_all_manifests().await?;

    for name in &manifest_names {
        match content_store.get_manifest(name).await {
            Ok(Some(data)) => match VolumeManifest::deserialize(&data) {
                Ok(vm) => {
                    live_packs.extend(vm.all_pack_ids());
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

    // Snapshot manifests — stream pack IDs directly into live_packs.
    // Each manifest is deserialized and dropped before the next, avoiding
    // O(snapshots) memory for manifest bodies.
    match collect_snapshot_pack_ids(content_store, &mut live_packs).await {
        Ok(count) => {
            stats.manifests_scanned += count;
        }
        Err(e) => {
            warn!(error = %e, "failed to scan snapshot manifests — treating all packs as live");
            return Ok((delta, stats));
        }
    }

    stats.live_packs += live_packs.len();

    // Phase 2+3: Stream S3 pack list, classify each pack inline.
    // No `known_packs` HashSet — each pack is checked against `live_packs`
    // as it arrives from the LIST stream, keeping memory at O(live + dead).
    let now_ts = Utc::now().to_rfc3339();
    let mut dead_packs: Vec<(u32, PackId)> = Vec::new();

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
        let Some(slash_pos) = rel.find('/') else {
            continue;
        };
        let Ok(chunk_idx) = rel[..slash_pos].parse::<u32>() else {
            continue;
        };
        let filename = &rel[slash_pos + 1..];
        let Some(hex_str) = filename.strip_suffix(".pack") else {
            continue;
        };
        if hex_str.len() != 16 {
            continue;
        }
        let Ok(pack_id) = u64::from_str_radix(hex_str, 16) else {
            continue;
        };

        stats.known_packs += 1;

        if live_packs.contains(&(chunk_idx, pack_id)) {
            // Live pack — check if it was previously marked dead (revive it)
            let key = pack_key(chunk_idx, pack_id);
            if state.dead_packs.contains_key(&key) {
                delta.revived_packs.push(key);
            }
        } else {
            // Dead pack — record in delta
            let key = pack_key(chunk_idx, pack_id);
            if !state.dead_packs.contains_key(&key) {
                delta.newly_dead_packs.push((key, now_ts.clone()));
            }
            dead_packs.push((chunk_idx, pack_id));
        }
    }

    stats.dead_found += dead_packs.len();

    // Phase 4: Filter by grace period.
    let eligible: Vec<(u32, PackId)> = dead_packs
        .iter()
        .filter(|(ci, pi)| {
            state.is_pack_eligible(*ci, *pi, grace_period)
                || (grace_period.is_zero()
                    && !state.dead_packs.contains_key(&pack_key(*ci, *pi)))
        })
        .copied()
        .collect();
    stats.eligible_for_deletion += eligible.len();

    // Phase 6: Delete eligible packs (capped, parallel)
    let to_delete: Vec<(u32, PackId)> = eligible.into_iter().take(max_deletes).collect();

    if dry_run {
        for &(chunk_idx, pack_id) in &to_delete {
            info!(chunk_idx, pack_id, "would delete orphaned pack (dry-run)");
        }
        stats.packs_deleted += to_delete.len();
    } else {
        let delete_results: Vec<((u32, PackId), Result<(), _>)> =
            futures::stream::iter(to_delete.iter().copied())
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
            .put_manifest("vm1", vm.serialize())
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
            .put_manifest("vm1", vm.serialize())
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

        cs1.put_manifest("vm1", b"test".to_vec()).await.unwrap();
        cs2.put_manifest("vm2", b"test".to_vec()).await.unwrap();

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
            .put_manifest("vm1", vm.serialize())
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
            .put_manifest("vm1", vm.serialize())
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
            .put_manifest("vm1", vm.serialize())
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
            .put_manifest("vm1", vm2.serialize())
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
            .put_manifest("vm1", vm.serialize())
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
    async fn test_gc_snapshot_retention() {
        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");

        // Create snapshots: both with "now" timestamps (InMemory behavior).
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
        let snapshots = content_store
            .list_all_snapshots_with_dates()
            .await
            .unwrap();
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
        let snapshots = content_store
            .list_all_snapshots_with_dates()
            .await
            .unwrap();
        assert_eq!(snapshots.len(), 0);
    }

    #[tokio::test]
    async fn test_gc_snapshot_retention_dry_run() {
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
        let snapshots = content_store
            .list_all_snapshots_with_dates()
            .await
            .unwrap();
        assert_eq!(snapshots.len(), 1, "snapshot should survive dry run");
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
            .put_manifest("vm1", vm_main.serialize())
            .await
            .unwrap();

        // Snapshot references pack_snap (keeping it alive).
        let mut vm_snap = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        vm_snap.append_pack(chunk_idx, pack_snap);
        content_store
            .put_snapshot("vm1", 1, vm_snap.serialize())
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

        // Both packs are live (one from manifest, one from snapshot).
        assert_eq!(report.live_packs(), 2);
        assert_eq!(report.dead_found(), 0);
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
            .put_manifest("vm1", vm.serialize())
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
}
