//! Garbage collection CLI command.
//!
//! Identifies and deletes orphaned packs in S3. Operates by comparing
//! known packs (listed from S3) against live packs (referenced by volume
//! manifests and their chunk metas). Packs referenced by no manifest are
//! dead and eligible for deletion after a grace period.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::{info, warn};
use uuid::Uuid;

use crate::block::chunk_meta::ChunkMeta;
use crate::block::content_store::ContentStore;
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

    fn mark_dead(&mut self, pack_id: &Uuid) {
        let key = pack_id.to_string();
        self.dead_packs
            .entry(key)
            .or_insert_with(|| Utc::now().to_rfc3339());
    }

    fn mark_alive(&mut self, pack_id: &Uuid) {
        self.dead_packs.remove(&pack_id.to_string());
    }

    fn mark_deleted(&mut self, pack_id: &Uuid) {
        self.dead_packs.remove(&pack_id.to_string());
    }

    fn is_eligible(&self, pack_id: &Uuid, grace_period: Duration) -> bool {
        let key = pack_id.to_string();
        if let Some(ts_str) = self.dead_packs.get(&key)
            && let Ok(ts) = ts_str.parse::<DateTime<Utc>>()
        {
            let age = Utc::now().signed_duration_since(ts);
            return age.to_std().unwrap_or(Duration::ZERO) >= grace_period;
        }
        false
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
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = path_from_url.to_string();

    // Load GC state
    let mut state = GcState::load(&state_file)?;
    let mut stats = GcStats::default();

    // Discover all S3 prefixes that contain manifests or chunks
    let prefixes = discover_s3_prefixes(&*object_store, &db_path).await?;
    info!(count = prefixes.len(), "discovered S3 prefixes");

    let mut total_deleted = 0usize;
    let remaining_budget = max_deletes;

    for prefix in &prefixes {
        if total_deleted >= max_deletes {
            info!("max deletes reached, stopping");
            break;
        }

        let content_store = ContentStore::new(Arc::clone(&object_store), prefix);
        let budget = remaining_budget.saturating_sub(total_deleted);
        let deleted = reconcile_prefix(
            &content_store,
            &mut state,
            &mut stats,
            grace_period,
            budget,
            dry_run,
        )
        .await?;
        total_deleted += deleted;
        stats.prefixes_scanned += 1;
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

    Ok(())
}

// ---------------------------------------------------------------------------
// S3 prefix discovery
// ---------------------------------------------------------------------------

/// Discover all unique S3 prefixes under `{db_path}/exports/` that contain
/// manifests, chunks, or snapshots.
async fn discover_s3_prefixes(
    object_store: &dyn object_store::ObjectStore,
    db_path: &str,
) -> Result<Vec<String>> {
    use futures::StreamExt;
    use object_store::path::Path as ObjectPath;

    let exports_prefix = ObjectPath::from(format!("{}/exports/", db_path.trim_end_matches('/')));
    let exports_prefix_str = exports_prefix.to_string();
    let mut prefixes = HashSet::new();

    let mut stream = object_store.list(Some(&exports_prefix));
    while let Some(result) = stream.next().await {
        let meta = result?;
        let path_str = meta.location.to_string();

        // Look for paths containing /manifests/, /chunks/, or /snapshots/
        for marker in &["/manifests/", "/chunks/", "/snapshots/"] {
            if let Some(pos) = path_str.find(marker) {
                let base = &path_str[..pos];
                if base.starts_with(&exports_prefix_str) || base.starts_with(db_path) {
                    prefixes.insert(base.to_string());
                }
            }
        }
    }

    let mut result: Vec<String> = prefixes.into_iter().collect();
    result.sort();
    Ok(result)
}

// ---------------------------------------------------------------------------
// Collect live packs from a VolumeManifest
// ---------------------------------------------------------------------------

/// Resolve all pack IDs referenced by a VolumeManifest by fetching and parsing
/// each chunk's ChunkMeta.
async fn collect_packs_from_volume_manifest(
    content_store: &ContentStore,
    vm: &VolumeManifest,
) -> Result<HashSet<Uuid>, anyhow::Error> {
    let mut packs = HashSet::new();
    for (&chunk_idx, chunk_hash_hex) in &vm.chunks {
        match content_store.get_chunk_meta(chunk_idx, chunk_hash_hex).await {
            Ok(Some(meta_bytes)) => match ChunkMeta::deserialize(&meta_bytes) {
                Ok(meta) => {
                    packs.extend(meta.pack_ids());
                }
                Err(e) => {
                    anyhow::bail!(
                        "corrupt chunk meta for chunk {} hash {}: {}",
                        chunk_idx,
                        chunk_hash_hex,
                        e
                    );
                }
            },
            Ok(None) => {
                warn!(
                    chunk_idx,
                    chunk_hash = %chunk_hash_hex,
                    "chunk meta not found (may have been cleaned up)"
                );
            }
            Err(e) => {
                anyhow::bail!(
                    "failed to fetch chunk meta for chunk {} hash {}: {}",
                    chunk_idx,
                    chunk_hash_hex,
                    e
                );
            }
        }
    }
    Ok(packs)
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Reconcile a single S3 prefix: find and delete orphaned packs.
///
/// Returns the number of packs deleted (or that would be deleted in dry-run).
async fn reconcile_prefix(
    content_store: &ContentStore,
    state: &mut GcState,
    stats: &mut GcStats,
    grace_period: Duration,
    max_deletes: usize,
    dry_run: bool,
) -> Result<usize> {
    // 1. Discover live packs from all manifests (VolumeManifest -> ChunkMeta -> pack_ids).
    let mut live_packs: HashSet<Uuid> = HashSet::new();
    let manifest_names = content_store.list_all_manifests().await?;
    let mut manifest_failed = false;

    for name in &manifest_names {
        match content_store.get_volume_manifest(name).await {
            Ok(Some(data)) => match VolumeManifest::deserialize(&data) {
                Ok(vm) => {
                    match collect_packs_from_volume_manifest(content_store, &vm).await {
                        Ok(packs) => {
                            live_packs.extend(packs);
                            stats.manifests_scanned += 1;
                        }
                        Err(e) => {
                            warn!(manifest = %name, error = %e, "failed to resolve chunk metas — treating all packs in prefix as live");
                            stats.manifest_errors += 1;
                            manifest_failed = true;
                        }
                    }
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

    // If any manifest failed to parse/resolve, we cannot determine liveness accurately.
    // Skip this prefix entirely to avoid deleting packs that might be live.
    if manifest_failed {
        warn!("skipping GC for prefix due to manifest errors — no packs will be deleted");
        return Ok(0);
    }

    // 1b. Extend live packs with references from versioned snapshot manifests.
    //     If snapshot scanning fails, bail out to prevent false-positive deletions.
    match content_store.collect_snapshot_live_packs().await {
        Ok(snap_packs) => {
            if !snap_packs.is_empty() {
                info!(
                    snapshot_packs = snap_packs.len(),
                    "added snapshot-referenced packs to live set"
                );
            }
            live_packs.extend(snap_packs);
        }
        Err(e) => {
            warn!(error = %e, "failed to scan snapshot manifests — treating all packs as live");
            return Ok(0);
        }
    }

    // 2. Discover known packs by listing all .pack files in S3 (chunk packs + legacy flat packs).
    //    This replaces the old registry-based approach.
    let all_known = content_store.list_all_known_packs().await?;
    let mut known_packs: HashSet<Uuid> = HashSet::new();
    // Track chunk_idx for each pack so we can delete from the right location.
    let mut pack_locations: HashMap<Uuid, u32> = HashMap::new();
    for (chunk_idx, pack_id) in &all_known {
        known_packs.insert(*pack_id);
        pack_locations.insert(*pack_id, *chunk_idx);
    }

    stats.live_packs += live_packs.len();
    stats.known_packs += known_packs.len();

    // 3. Compute dead packs = known - live
    let dead_packs: HashSet<Uuid> = known_packs.difference(&live_packs).copied().collect();
    stats.dead_found += dead_packs.len();

    // 4. Update state: mark new dead packs, revive packs that became live
    for &pack_id in &dead_packs {
        state.mark_dead(&pack_id);
    }
    // Remove packs that are now live from the dead state
    let revived: Vec<Uuid> = known_packs
        .intersection(&live_packs)
        .copied()
        .filter(|id| state.dead_packs.contains_key(&id.to_string()))
        .collect();
    for pack_id in revived {
        state.mark_alive(&pack_id);
    }

    // 5. Filter by grace period
    let eligible: Vec<Uuid> = dead_packs
        .iter()
        .filter(|id| state.is_eligible(id, grace_period))
        .copied()
        .collect();
    stats.eligible_for_deletion += eligible.len();

    // 6. Delete eligible packs (capped)
    let to_delete: Vec<Uuid> = eligible.into_iter().take(max_deletes).collect();

    for &pack_id in &to_delete {
        if dry_run {
            info!(pack_id = %pack_id, "would delete orphaned pack (dry-run)");
        } else {
            let chunk_idx = pack_locations.get(&pack_id).copied().unwrap_or(u32::MAX);
            let result = if chunk_idx == u32::MAX {
                // Legacy flat pack
                content_store.delete_pack(pack_id).await
            } else {
                // Chunk-scoped pack
                content_store.delete_chunk_pack(chunk_idx, pack_id).await
            };
            match result {
                Ok(()) => {
                    state.mark_deleted(&pack_id);
                    stats.packs_deleted += 1;
                }
                Err(e) => {
                    warn!(pack_id = %pack_id, error = %e, "failed to delete pack");
                }
            }
        }
    }

    if dry_run {
        stats.packs_deleted += to_delete.len();
    }

    Ok(to_delete.len())
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
    let mut stats = GcStats::default();
    let deleted = reconcile_prefix(
        content_store,
        state,
        &mut stats,
        grace_period,
        max_deletes,
        dry_run,
    )
    .await?;
    Ok(GcTestReport { stats, deleted })
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
/// Test report from reconciliation.
pub struct GcTestReport {
    stats: GcStats,
    deleted: usize,
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
        self.deleted
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
}
