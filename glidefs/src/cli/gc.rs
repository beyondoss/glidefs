//! Garbage collection CLI command.
//!
//! Identifies and deletes orphaned packs in S3. Operates by comparing
//! pack registries (what packs exist) against manifests (what packs are live).
//! Packs referenced by no manifest are dead and eligible for deletion after
//! a grace period.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::Settings;
use crate::nbd::content_store::ContentStore;
use crate::nbd::manifest::Manifest;
use crate::nbd::pack_registry::PackRegistry;
use crate::parse_object_store::parse_url_opts;

// ---------------------------------------------------------------------------
// GC State (persisted between runs for grace period tracking)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GcState {
    /// Pack ID (UUID string) → first-seen-dead ISO 8601 timestamp.
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
        tmp.persist(path)
            .with_context(|| format!("failed to atomically rename GC state to {}", path.display()))?;
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
    registries_scanned: usize,
    live_packs: usize,
    known_packs: usize,
    dead_found: usize,
    eligible_for_deletion: usize,
    packs_deleted: usize,
    registries_compacted: usize,
    registries_deleted: usize,
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

    // Discover all S3 prefixes that contain manifests or registries
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
    println!("Registries scanned:      {}", stats.registries_scanned);
    println!("Live packs:              {}", stats.live_packs);
    println!("Known packs (registry):  {}", stats.known_packs);
    println!("Dead packs found:        {}", stats.dead_found);
    println!("Eligible for deletion:   {}", stats.eligible_for_deletion);
    println!("Packs deleted:           {}", stats.packs_deleted);
    println!("Registries compacted:    {}", stats.registries_compacted);
    println!("Registries deleted:      {}", stats.registries_deleted);

    Ok(())
}

// ---------------------------------------------------------------------------
// S3 prefix discovery
// ---------------------------------------------------------------------------

/// Discover all unique S3 prefixes under `{db_path}/nbd/` that contain
/// manifests or pack-registries.
async fn discover_s3_prefixes(
    object_store: &dyn object_store::ObjectStore,
    db_path: &str,
) -> Result<Vec<String>> {
    use futures::StreamExt;
    use object_store::path::Path as ObjectPath;

    let nbd_prefix = ObjectPath::from(format!("{}/nbd/", db_path.trim_end_matches('/')));
    let nbd_prefix_str = nbd_prefix.to_string();
    let mut prefixes = HashSet::new();

    let mut stream = object_store.list(Some(&nbd_prefix));
    while let Some(result) = stream.next().await {
        let meta = result?;
        let path_str = meta.location.to_string();

        // Look for paths containing /manifests/ or /pack-registries/
        // Pattern: {db_path}/nbd/{s3_prefix}/manifests/{name}
        // Pattern: {db_path}/nbd/{s3_prefix}/pack-registries/{name}
        for marker in &["/manifests/", "/pack-registries/"] {
            if let Some(pos) = path_str.find(marker) {
                let base = &path_str[..pos];
                if base.starts_with(&nbd_prefix_str) || base.starts_with(db_path) {
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
    // 1. List all manifests, parse, collect live pack IDs
    let mut live_packs: HashSet<Uuid> = HashSet::new();
    let manifest_names = content_store.list_all_manifests().await?;

    for name in &manifest_names {
        match content_store.get_manifest(name).await {
            Ok(Some(data)) => match Manifest::deserialize(&data) {
                Ok(manifest) => {
                    for entry in &manifest.pack_index {
                        live_packs.insert(entry.pack_id);
                    }
                    stats.manifests_scanned += 1;
                }
                Err(e) => {
                    warn!(manifest = %name, error = %e, "skipping corrupt manifest");
                    stats.manifest_errors += 1;
                }
            },
            Ok(None) => {
                warn!(manifest = %name, "manifest disappeared during GC");
            }
            Err(e) => {
                warn!(manifest = %name, error = %e, "failed to fetch manifest");
                stats.manifest_errors += 1;
            }
        }
    }

    // 2. List all registries, parse, collect known pack IDs
    let mut known_packs: HashSet<Uuid> = HashSet::new();
    let mut registry_data: Vec<(String, PackRegistry)> = Vec::new();
    let registry_names = content_store.list_registries().await?;

    for name in &registry_names {
        match content_store.get_registry(name).await {
            Ok(Some(data)) => match PackRegistry::deserialize(&data) {
                Ok(reg) => {
                    known_packs.extend(&reg.pack_ids);
                    registry_data.push((name.clone(), reg));
                    stats.registries_scanned += 1;
                }
                Err(e) => {
                    warn!(registry = %name, error = %e, "skipping corrupt registry");
                }
            },
            Ok(None) => {
                warn!(registry = %name, "registry disappeared during GC");
            }
            Err(e) => {
                warn!(registry = %name, error = %e, "failed to fetch registry");
            }
        }
    }

    stats.live_packs += live_packs.len();
    stats.known_packs += known_packs.len();

    // 3. Compute dead packs
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
    let mut deleted_set: HashSet<Uuid> = HashSet::new();

    for &pack_id in &to_delete {
        if dry_run {
            info!(pack_id = %pack_id, "would delete orphaned pack (dry-run)");
        } else {
            match content_store.delete_pack(pack_id).await {
                Ok(()) => {
                    state.mark_deleted(&pack_id);
                    deleted_set.insert(pack_id);
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

    // 7. Compact registries: remove deleted pack IDs
    if !deleted_set.is_empty() || dry_run {
        let remove_set = if dry_run {
            to_delete.iter().copied().collect::<HashSet<_>>()
        } else {
            deleted_set.clone()
        };

        for (name, mut reg) in registry_data {
            let before = reg.pack_ids.len();
            reg.compact(&remove_set);
            let after = reg.pack_ids.len();

            if before != after {
                if !dry_run {
                    if reg.is_empty() {
                        // Check if the export still has a manifest
                        let has_manifest = manifest_names.contains(&name);
                        if !has_manifest {
                            // No manifest = deleted VM, delete empty registry
                            if let Err(e) = content_store.delete_registry(&name).await {
                                warn!(registry = %name, error = %e, "failed to delete empty registry");
                            } else {
                                stats.registries_deleted += 1;
                            }
                            continue;
                        }
                    }
                    if let Err(e) = content_store
                        .put_registry(&name, reg.serialize())
                        .await
                    {
                        warn!(registry = %name, error = %e, "failed to compact registry");
                    } else {
                        stats.registries_compacted += 1;
                    }
                } else {
                    stats.registries_compacted += 1;
                }
            }
        }
    }

    // 8. Delete registries for VMs that no longer have manifests and no packs left
    //    (even if no packs were deleted this run)
    let manifest_name_set: HashSet<&String> = manifest_names.iter().collect();
    for name in &registry_names {
        if !manifest_name_set.contains(name) {
            // Registry exists but no manifest — check if it was already handled above
            if !dry_run {
                // Only delete if the registry is empty (all packs cleaned)
                // We already checked this above during compaction.
                // For registries not compacted (no deleted packs this run), check separately.
                if deleted_set.is_empty() {
                    // No packs were deleted this run, so check if registry is already empty
                    if let Ok(Some(data)) = content_store.get_registry(name).await
                        && let Ok(reg) = PackRegistry::deserialize(&data)
                        && reg.is_empty()
                    {
                        if let Err(e) = content_store.delete_registry(name).await {
                            warn!(registry = %name, error = %e, "failed to delete empty orphan registry");
                        } else {
                            stats.registries_deleted += 1;
                        }
                    }
                }
            }
        }
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
    pub fn registries_scanned(&self) -> usize {
        self.stats.registries_scanned
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
    pub fn registries_compacted(&self) -> usize {
        self.stats.registries_compacted
    }
    pub fn registries_deleted(&self) -> usize {
        self.stats.registries_deleted
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

        // Not in state → not eligible
        assert!(!state.is_eligible(&id, Duration::from_secs(3600)));

        // Mark dead with a timestamp in the past
        let old_ts = Utc::now() - chrono::Duration::hours(25);
        state
            .dead_packs
            .insert(id.to_string(), old_ts.to_rfc3339());

        // Should be eligible (dead > 24h)
        assert!(state.is_eligible(&id, Duration::from_secs(86400)));

        // Should NOT be eligible with longer grace period
        assert!(!state.is_eligible(&id, Duration::from_secs(100 * 3600)));
    }

    #[tokio::test]
    async fn test_gc_reconciliation_deletes_orphaned_packs() {
        use crate::nbd::content_store::ContentStore;
        use crate::nbd::manifest::{Manifest, ManifestPackEntry};
        use crate::nbd::block_map::Blake3Hash;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/nbd/vm1");

        // Create 3 packs: pack_a (live), pack_b (dead), pack_c (dead)
        let pack_a = Uuid::new_v4();
        let pack_b = Uuid::new_v4();
        let pack_c = Uuid::new_v4();

        // Upload packs to S3
        content_store.put_pack(pack_a, vec![0u8; 100]).await.unwrap();
        content_store.put_pack(pack_b, vec![0u8; 100]).await.unwrap();
        content_store.put_pack(pack_c, vec![0u8; 100]).await.unwrap();

        // Create a manifest that only references pack_a
        let manifest = Manifest {
            name: "vm1".to_string(),
            sequence: 1,
            chunk_size: 131072,
            device_size: 1024 * 1024 * 1024,
            block_map: vec![],
            pack_index: vec![ManifestPackEntry {
                hash: Blake3Hash::from_bytes([1; 16]),
                pack_id: pack_a,
                offset: 0,
                comp_length: 100,
            }],
        };
        content_store.put_manifest("vm1", manifest.serialize()).await.unwrap();

        // Create a registry that references all 3 packs
        let registry = PackRegistry {
            pack_ids: vec![pack_a, pack_b, pack_c],
        };
        content_store.put_registry("vm1", registry.serialize()).await.unwrap();

        // Run GC with zero grace period
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
        assert_eq!(report.registries_scanned(), 1);
        assert_eq!(report.live_packs(), 1, "only pack_a is live");
        assert_eq!(report.known_packs(), 3, "all 3 in registry");
        assert_eq!(report.dead_found(), 2, "pack_b and pack_c are dead");
        assert_eq!(report.eligible_for_deletion(), 2, "both past grace period");
        assert_eq!(report.packs_deleted(), 2, "both should be deleted");

        // Verify dead packs were removed from GC state
        assert!(!state.dead_packs.contains_key(&pack_b.to_string()));
        assert!(!state.dead_packs.contains_key(&pack_c.to_string()));
    }

    #[tokio::test]
    async fn test_gc_dry_run_doesnt_delete() {
        use crate::nbd::content_store::ContentStore;
        use crate::nbd::manifest::Manifest;
        use object_store::memory::InMemory;

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = ContentStore::new(Arc::clone(&s3), "test/nbd/vm1");

        let dead_pack = Uuid::new_v4();
        content_store.put_pack(dead_pack, vec![0u8; 100]).await.unwrap();

        // Manifest with no pack references
        let manifest = Manifest {
            name: "vm1".to_string(),
            sequence: 1,
            chunk_size: 131072,
            device_size: 1024 * 1024 * 1024,
            block_map: vec![],
            pack_index: vec![],
        };
        content_store.put_manifest("vm1", manifest.serialize()).await.unwrap();

        let registry = PackRegistry {
            pack_ids: vec![dead_pack],
        };
        content_store.put_registry("vm1", registry.serialize()).await.unwrap();

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

        assert_eq!(report.packs_deleted(), 1, "dry run should report as deleted");

        // But the pack should still exist in S3
        let pack_data = content_store.get_pack(dead_pack).await;
        assert!(pack_data.is_ok(), "pack should still exist after dry run");
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
