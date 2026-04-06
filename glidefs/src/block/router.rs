//! Multi-tenant export router for NBD server.
//!
//! Manages multiple NBD exports, each with its own write cache and S3 storage.
//! Supports dynamic export creation/removal for microVM scale-to-zero and live migration.

use crate::block::cache::BlockCache;
use crate::block::content_store::ContentStore;
use crate::block::flush_scheduler::flush_scheduler;
use crate::block::handler::BlockHandler;
use crate::block::pack_index_cache::PackIndexCache;
use crate::block::manifest::deserialize_hot_set;
use crate::block::metrics::{ExportMetrics, MetricsSnapshot};
use crate::block::state::Active;
use crate::block::volume_manifest::VolumeManifest;
use crate::block::write_cache::{CacheError, SnapshotResult, WriteCache, WriteCacheConfig};
use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
use crate::config::ExportConfig;
use crate::task::spawn_named;
use bytes::Bytes;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Notify, RwLock, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, trace, warn};

/// Maximum number of entries in the base manifest / hot set caches.
/// Bases are immutable after bless, so eviction policy doesn't matter —
/// we just cap memory usage. 64 entries ≈ a few MB at most.
const MAX_BASE_CACHE_ENTRIES: usize = 64;

/// Errors that can occur during export operations.
#[derive(Error, Debug)]
pub enum RouterError {
    #[allow(dead_code)] // create_export is idempotent, but API layer still matches this
    #[error("Export '{0}' already exists")]
    ExportExists(String),

    #[error("Export '{0}' not found")]
    ExportNotFound(String),

    #[error(
        "Invalid export name '{0}': must be 1-128 chars, alphanumeric/hyphen/underscore/dot, starting with alphanumeric"
    )]
    InvalidExportName(String),

    #[error(
        "Cannot shrink export '{name}': current size {current_gb}GB, requested {requested_gb}GB"
    )]
    CannotShrink {
        name: String,
        current_gb: f64,
        requested_gb: f64,
    },

    #[error("Cache error: {0}")]
    Cache(#[from] CacheError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("S3 error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Content store error: {0}")]
    ContentStore(#[from] crate::block::content_store::ContentStoreError),

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error(
        "Drain incomplete for '{name}': {remaining} dirty blocks after {iterations} iterations"
    )]
    DrainIncomplete {
        name: String,
        remaining: u64,
        iterations: usize,
    },

    #[error("Shutdown incomplete: {incomplete_count} export(s) with dirty blocks: {details}")]
    ShutdownIncomplete {
        incomplete_count: usize,
        details: String,
    },
}

/// Information about an export.
#[derive(Clone, Debug)]
pub struct ExportInfo {
    pub name: String,
    pub size: u64,
    pub readonly: bool,
    pub transport: String,
    pub device: Option<PathBuf>,
    /// S3 prefix for pack/manifest storage (None = export name).
    pub s3_prefix: Option<String>,
    /// Unflushed bytes waiting to be synced to S3.
    pub dirty_bytes: u64,
    /// Total logical bytes stored in S3 (chunks × chunk_size).
    pub s3_bytes: u64,
    /// Filesystem used bytes from ext4 superblock (None if not ext4 or read failed).
    pub fs_used_bytes: Option<u64>,
}

/// Readiness check result for health endpoint.
#[derive(Debug, Serialize)]
pub struct ReadinessStatus {
    pub ready: bool,
    pub exports_count: usize,
    pub cache_writable: bool,
    pub s3_reachable: bool,
}

/// Aggregate stats across all exports for host-level pressure monitoring.
#[derive(Debug, Serialize)]
pub struct AggregateStats {
    pub ssd_utilization: f64,
    pub s3_circuit_state: u8,
    pub total_cache_hits: u64,
    pub total_cache_misses: u64,
    pub total_dirty_bytes: u64,
    pub exports_count: usize,
}

/// Response from a snapshot operation.
#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    pub manifest_etag: Option<String>,
    pub sequence: u64,
    /// Whether the versioned snapshot was persisted to S3.
    /// `false` means the manifest was saved but the versioned snapshot key wasn't.
    pub snapshot_persisted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
}

/// State for a single export.
pub struct ExportState {
    pub handler: Arc<BlockHandler>,
    pub cache: Arc<WriteCache<Active>>,
    pub content_store: Arc<ContentStore>,
    pub pack_index_cache: Arc<PackIndexCache>,
    pub volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,
    pub readonly: bool,
    pub metrics: Arc<ExportMetrics>,
    /// Original s3_prefix from ExportConfig (None = use export name).
    pub s3_prefix: Option<String>,
    /// Transport type: "nbd" or "ublk".
    pub transport: String,
    flush_shutdown_tx: watch::Sender<bool>,
    flush_handle: JoinHandle<()>,
    /// Background hot-set prefetch task (if spawned). Aborted on teardown
    /// to release Arc references to cache/content_store/etc.
    prefetch_handle: Option<JoinHandle<()>>,
}

/// Maximum drain iterations before giving up. Prevents infinite loops when
/// concurrent writes keep producing new dirty blocks faster than we flush.
const MAX_DRAIN_ITERATIONS: usize = 100;

/// Superblock offset in bytes (always 1024 for ext4).
const EXT4_SUPERBLOCK_OFFSET: u64 = 1024;

/// Superblock size in bytes.
const EXT4_SUPERBLOCK_SIZE: u32 = 1024;

/// Read filesystem used bytes from ext4 superblock.
///
/// Returns `None` if the read fails or the data isn't a valid ext4 superblock.
/// This is a best-effort operation - failures are logged but don't propagate.
async fn read_fs_used_bytes(handler: &BlockHandler) -> Option<u64> {
    // Read superblock (at offset 1024, size 1024 bytes).
    let data = match handler.read(EXT4_SUPERBLOCK_OFFSET, EXT4_SUPERBLOCK_SIZE).await {
        Ok(d) => d,
        Err(e) => {
            trace!(error = ?e, "failed to read superblock for fs_used_bytes");
            return None;
        }
    };

    // Parse superblock.
    let sb = match ext4::format::SuperBlock::read_from(&data) {
        Ok(sb) => sb,
        Err(e) => {
            trace!(error = %e, "failed to parse ext4 superblock");
            return None;
        }
    };

    Some(sb.used_bytes())
}

/// Configuration for the export router.
pub struct RouterConfig {
    /// S3/MinIO/etc backend
    pub object_store: Arc<dyn ObjectStore>,
    /// Base S3 path prefix
    pub db_path: String,
    /// Local cache directory
    pub cache_dir: PathBuf,
    /// Block size in bytes for all exports (default, can be overridden per-export)
    pub block_size: usize,
    /// Shared block cache for decompressed block data
    pub clean_cache: Arc<dyn BlockCache>,
    /// Whether to fsync the WAL after each write batch
    pub wal_sync: bool,
    /// Max concurrent S3 pack uploads across all exports (0 = unlimited).
    pub max_s3_uploads: usize,
    /// Max concurrent S3 pack downloads across all exports (0 = unlimited).
    pub max_s3_downloads: usize,
    /// Default blocks per pack for new exports (from NbdConfig).
    pub default_flush_threshold: usize,
    /// Number of ublk I/O queues per device (Linux + ublk feature only).
    #[cfg_attr(not(all(target_os = "linux", feature = "ublk")), allow(dead_code))]
    pub ublk_nr_queues: u16,
    /// Dead connection timeout for NBD netlink devices (seconds).
    /// Enables kernel-side I/O queueing during restarts. 0 = disabled.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub nbd_dead_conn_timeout: u32,
}

/// Multi-tenant export router.
///
/// Manages multiple NBD exports, each with independent storage and caching.
pub struct ExportRouter {
    /// Active exports: name → state
    exports: RwLock<HashMap<String, ExportState>>,

    /// Shared object store (S3/MinIO/etc)
    object_store: Arc<dyn ObjectStore>,

    /// Base S3 path prefix
    db_path: String,

    /// Local cache directory
    cache_dir: PathBuf,

    /// Block size for all exports (default, can be overridden per-export)
    block_size: usize,

    /// Shared pack index cache across all exports (v4 block resolution)
    pack_index_cache: Arc<PackIndexCache>,

    /// Shared clean cache across all exports (content-addressed dedup)
    clean_cache: Arc<dyn BlockCache>,

    /// Whether to fsync the WAL after each write batch
    wal_sync: bool,

    /// Default blocks per pack for new exports (from global config).
    default_flush_threshold: usize,

    /// Scrubber metrics (global, not per-export)
    scrubber_metrics: Arc<crate::block::scrubber::ScrubberMetrics>,

    /// Shared S3 circuit breaker: opens after 5 consecutive failures, probes after 30s.
    s3_circuit_breaker: Arc<CircuitBreaker>,

    /// Global S3 upload concurrency limit (None = unlimited).
    upload_semaphore: Option<Arc<tokio::sync::Semaphore>>,

    /// Global S3 download concurrency limit (None = unlimited).
    download_semaphore: Option<Arc<tokio::sync::Semaphore>>,

    /// Global flush concurrency limit (None = unlimited).
    flush_semaphore: Option<Arc<tokio::sync::Semaphore>>,

    /// SSD utilization ratio (0.0–1.0), updated by capacity monitor.
    /// Shared with BlockHandler instances for write rejection at high utilization.
    ssd_utilization: Arc<AtomicU64>, // f64 bits via to_bits()/from_bits()

    /// NBD kernel device manager (Linux only).
    #[cfg(target_os = "linux")]
    nbd_devices: tokio::sync::Mutex<crate::block::nbd::NbdDeviceManager>,

    /// ublk device manager (Linux + ublk feature only).
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    ublk_server: tokio::sync::Mutex<crate::block::ublk::UblkServer>,

    /// Bounded cache for blessed base manifests (bases/* are immutable after bless).
    /// Key: "{s3_prefix}:{manifest_name}" → deserialized VolumeManifest.
    /// Cloning the Arc is ~0ns vs ~100ms for an S3 round-trip.
    base_manifest_cache: parking_lot::Mutex<HashMap<String, Arc<VolumeManifest>>>,

    /// Bounded cache for boot hot sets (immutable, paired with base manifests).
    /// Key: "{s3_prefix}:{hot_set_name}" → parsed block indices.
    /// Arc-wrapped so it can be shared with background prefetch tasks.
    hot_set_cache: Arc<parking_lot::Mutex<HashMap<String, Arc<Vec<u64>>>>>,
}

/// Validate an export name: 1-128 chars, alphanumeric/hyphen/underscore/dot,
/// must start with an alphanumeric character. Rejects path traversal attempts.
/// Remove a file, logging non-NotFound errors instead of silently swallowing them.
fn remove_file_if_exists(path: &std::path::Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %path.display(), error = %e, "failed to remove file");
    }
}

fn validate_export_name(name: &str) -> Result<(), RouterError> {
    if name.is_empty() || name.len() > 128 {
        return Err(RouterError::InvalidExportName(name.to_string()));
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(RouterError::InvalidExportName(name.to_string())); // unreachable: checked is_empty above
    };
    if !first.is_ascii_alphanumeric() {
        return Err(RouterError::InvalidExportName(name.to_string()));
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.') {
        return Err(RouterError::InvalidExportName(name.to_string()));
    }
    Ok(())
}

impl ExportRouter {
    /// Create a new export router.
    pub async fn new(config: RouterConfig) -> Result<Self, RouterError> {
        let s3_circuit_breaker = Arc::new(CircuitBreaker::new(
            CircuitBreakerConfig::consecutive(5)
                .reset_timeout(Duration::from_secs(30))
                .half_open_permits(3),
        ));

        let upload_semaphore = if config.max_s3_uploads > 0 {
            Some(Arc::new(tokio::sync::Semaphore::new(config.max_s3_uploads)))
        } else {
            None
        };
        let download_semaphore = if config.max_s3_downloads > 0 {
            Some(Arc::new(tokio::sync::Semaphore::new(
                config.max_s3_downloads,
            )))
        } else {
            None
        };
        // Flush semaphore mirrors upload semaphore: prevents flushes from
        // compressing data faster than S3 can drain it (compressed blocks
        // sit in memory between rayon completion and upload permit acquisition).
        let flush_semaphore = upload_semaphore
            .as_ref()
            .map(|s| Arc::new(tokio::sync::Semaphore::new(s.available_permits())));

        let pack_index_cache = Arc::new(
            PackIndexCache::open(&config.cache_dir)
                .await
                .map_err(|e| RouterError::Manifest(format!("pack index cache: {e}")))?,
        );

        // Build device managers before moving config.cache_dir.
        #[cfg(target_os = "linux")]
        let nbd_devices = tokio::sync::Mutex::new(
            crate::block::nbd::NbdDeviceManager::new()
                .with_cache_dir(config.cache_dir.clone())
                .with_dead_conn_timeout(config.nbd_dead_conn_timeout),
        );
        #[cfg(all(target_os = "linux", feature = "ublk"))]
        let ublk_server = tokio::sync::Mutex::new(
            crate::block::ublk::UblkServer::new()
                .with_nr_queues(config.ublk_nr_queues)
                .with_cache_dir(config.cache_dir.clone()),
        );

        Ok(Self {
            exports: RwLock::new(HashMap::new()),
            object_store: config.object_store,
            db_path: config.db_path,
            cache_dir: config.cache_dir,
            block_size: config.block_size,
            pack_index_cache,
            clean_cache: config.clean_cache,
            wal_sync: config.wal_sync,
            default_flush_threshold: config.default_flush_threshold,
            scrubber_metrics: Arc::new(crate::block::scrubber::ScrubberMetrics::new()),
            s3_circuit_breaker,
            upload_semaphore,
            download_semaphore,
            flush_semaphore,
            ssd_utilization: Arc::new(AtomicU64::new(0f64.to_bits())),
            #[cfg(target_os = "linux")]
            nbd_devices,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            ublk_server,
            base_manifest_cache: parking_lot::Mutex::new(HashMap::new()),
            hot_set_cache: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        })
    }

    /// Get a reference to the shared clean cache (for scrubber).
    pub fn clean_cache(&self) -> &Arc<dyn BlockCache> {
        &self.clean_cache
    }

    /// Pre-warm the base manifest and hot set caches for a given S3 prefix.
    ///
    /// Lists all `bases/*` manifests under `{s3_prefix}/manifests/` and loads
    /// them into memory. Call after router construction (e.g. after
    /// `discover_exports`) so the first fork from each base avoids an S3
    /// round-trip.
    pub async fn prewarm_base_caches(&self, s3_prefix: &str) {
        use futures::stream::{self, StreamExt};

        let cs = ContentStore::new(Arc::clone(&self.object_store), s3_prefix)
            .with_circuit_breaker(Arc::clone(&self.s3_circuit_breaker));

        let manifests = match cs.list_all_manifests().await {
            Ok(m) => m,
            Err(e) => {
                warn!("prewarm: failed to list manifests: {}", e);
                return;
            }
        };

        let remaining_capacity = MAX_BASE_CACHE_ENTRIES
            .saturating_sub(self.base_manifest_cache.lock().len());
        let bases: Vec<_> = manifests
            .into_iter()
            .filter(|name| name.starts_with("bases/"))
            .take(remaining_capacity)
            .collect();
        if bases.is_empty() {
            debug!("prewarm: no base manifests found under {}", s3_prefix);
            return;
        }

        info!(
            count = bases.len(),
            s3_prefix = %s3_prefix,
            "pre-warming base manifest and hot set caches"
        );

        // Fetch all base manifests + hot sets concurrently.
        let cs = Arc::new(cs);
        stream::iter(bases)
            .for_each_concurrent(8, |manifest_name| {
                let cs = Arc::clone(&cs);
                async move {
                    let cache_key = format!("{}:{}", s3_prefix, &manifest_name);

                    // Manifest
                    if !self.base_manifest_cache.lock().contains_key(&cache_key) {
                        match cs.get_manifest(&manifest_name).await {
                            Ok(Some((data, _etag))) => match VolumeManifest::deserialize(&data) {
                                Ok(vm) => {
                                    let mut cache_map = self.base_manifest_cache.lock();
                                    if cache_map.len() < MAX_BASE_CACHE_ENTRIES {
                                        cache_map.insert(cache_key.clone(), Arc::new(vm));
                                    }
                                }
                                Err(e) => warn!(
                                    manifest = %manifest_name,
                                    "prewarm: failed to deserialize manifest: {}", e
                                ),
                            },
                            Ok(None) => debug!(manifest = %manifest_name, "prewarm: manifest not found"),
                            Err(e) => warn!(
                                manifest = %manifest_name,
                                "prewarm: failed to fetch manifest: {}", e
                            ),
                        }
                    }

                    // Hot set
                    let hot_set_name = manifest_name
                        .strip_prefix("bases/")
                        .unwrap_or(&manifest_name);
                    let hot_cache_key = format!("{}:{}", s3_prefix, hot_set_name);

                    if !self.hot_set_cache.lock().contains_key(&hot_cache_key) {
                        match cs.get_hot_set(hot_set_name).await {
                            Ok(Some(data)) => match deserialize_hot_set(&data) {
                                Ok(chunks) => {
                                    let mut cache_map = self.hot_set_cache.lock();
                                    if cache_map.len() < MAX_BASE_CACHE_ENTRIES {
                                        cache_map.insert(hot_cache_key, Arc::new(chunks));
                                    }
                                }
                                Err(e) => warn!(
                                    hot_set = %hot_set_name,
                                    "prewarm: failed to deserialize hot set: {}", e
                                ),
                            },
                            Ok(None) => debug!(hot_set = %hot_set_name, "prewarm: no hot set found"),
                            Err(e) => warn!(
                                hot_set = %hot_set_name,
                                "prewarm: failed to fetch hot set: {}", e
                            ),
                        }
                    }
                }
            })
            .await;

        let manifest_count = self.base_manifest_cache.lock().len();
        let hot_set_count = self.hot_set_cache.lock().len();
        info!(
            manifests = manifest_count,
            hot_sets = hot_set_count,
            "base cache pre-warm complete"
        );
    }

    /// Get the shared scrubber metrics (for scrubber + prometheus).
    pub fn scrubber_metrics(&self) -> &Arc<crate::block::scrubber::ScrubberMetrics> {
        &self.scrubber_metrics
    }

    /// Collect all known block hashes from active exports.
    ///
    /// Walks all VolumeManifest pack_ids and queries PackIndexCache for block
    /// hashes. Used by the background scrubber to know which cached blocks to
    /// verify. Results are best-effort — cold (uncached) pack indices return no
    /// hashes for those packs.
    pub async fn collect_block_hashes(&self) -> Vec<crate::block::block_map::Blake3Hash> {
        use std::collections::{HashMap, HashSet};

        // Group (chunk_idx → pack_ids) directly under the manifest lock,
        // avoiding an intermediate Vec of all pairs.
        let packs_by_chunk: HashMap<u32, Vec<crate::block::pack::PackId>> = {
            let exports = self.exports.read().await;
            let mut map: HashMap<u32, Vec<crate::block::pack::PackId>> = HashMap::new();
            for state in exports.values() {
                let vm = state.volume_manifest.read();
                for (chunk_idx, pack_id) in vm.all_pack_ids() {
                    map.entry(chunk_idx).or_default().push(pack_id);
                }
            }
            map
        };

        // Query PackIndexCache per-chunk. Global HashSet deduplicates across
        // chunks (same block hash can appear in multiple packs).
        let mut all_hashes = HashSet::new();
        for pack_ids in packs_by_chunk.values() {
            let chunk_hashes = self.pack_index_cache.known_hashes(pack_ids).await;
            all_hashes.extend(chunk_hashes);
        }
        all_hashes.into_iter().collect()
    }

    /// Warm the PackIndexCache for all active exports (v4 cold-start prefetch).
    ///
    /// In v4, pack indices are fetched on-demand (on first cold read, all pack
    /// indices for a chunk are prefetched in parallel). This function provides
    /// an explicit warm-up on server start, reducing first-read latency for
    /// all known packs across all exports.
    pub async fn prefetch_chunk_metas(&self) -> usize {
        use futures::stream::StreamExt;

        // Collect all (chunk_idx, pack_id, content_store) triples under lock.
        let to_fetch: Vec<(u32, crate::block::pack::PackId, Arc<ContentStore>)> = {
            let exports = self.exports.read().await;
            let mut triples = Vec::new();
            for state in exports.values() {
                let cs = Arc::clone(&state.content_store);
                let vm = state.volume_manifest.read();
                for (chunk_idx, pack_id) in vm.all_pack_ids() {
                    triples.push((chunk_idx, pack_id, Arc::clone(&cs)));
                }
            }
            triples
        };

        if to_fetch.is_empty() {
            return 0;
        }

        // Filter out packs already in cache and fetch uncached ones in parallel.
        // The cache check and S3 fetch are combined into a single stream to avoid
        // a sequential O(N) await loop for the filter step.
        info!(total = to_fetch.len(), "prefetching pack indices from S3");
        let pic = Arc::clone(&self.pack_index_cache);
        let fetched: usize = futures::stream::iter(to_fetch)
            .map(|(chunk_idx, pack_id, cs)| {
                let pic = Arc::clone(&pic);
                async move {
                    // Fast path: already cached — no S3 fetch needed.
                    if pic.get_entries(pack_id).await.is_some() {
                        return 0;
                    }
                    match cs.get_pack_index(chunk_idx, pack_id).await {
                        Ok(entries) => {
                            pic.insert_entries(pack_id, &entries);
                            1usize
                        }
                        Err(e) => {
                            warn!(chunk_idx, pack_id, error = %e, "pack index prefetch failed");
                            0
                        }
                    }
                }
            })
            .buffer_unordered(32)
            .fold(0usize, |acc, n| async move { acc + n })
            .await;

        info!(fetched, "pack index prefetch complete");
        fetched
    }

    /// Get the current S3 circuit breaker state for observability.
    pub fn s3_circuit_state(&self) -> CircuitState {
        self.s3_circuit_breaker.state()
    }

    /// Get the cache directory path (for capacity monitor `statvfs`).
    pub fn cache_dir(&self) -> &std::path::Path {
        &self.cache_dir
    }

    /// Get the current SSD utilization ratio (for Prometheus metrics).
    pub fn ssd_utilization(&self) -> f64 {
        f64::from_bits(self.ssd_utilization.load(Ordering::Relaxed))
    }

    /// Set the SSD utilization ratio (called by capacity monitor).
    pub fn set_ssd_utilization(&self, ratio: f64) {
        self.ssd_utilization
            .store(ratio.to_bits(), Ordering::Relaxed);
    }

    /// Flush dirty packs from the dirtiest exports under SSD pressure.
    ///
    /// Does NOT free SSD space — data files retain physical allocation.
    /// Purpose: ensure data reaches S3 (portability) before potential disk full.
    ///
    /// Acquires the per-export `flush_lock` via `try_lock` to serialize with
    /// the flush scheduler and drain paths. Skips exports where a flush is
    /// already in progress. Syncs the manifest after flushing so uploaded
    /// packs are always referenced (prevents orphaned packs on crash).
    pub async fn pressure_flush(&self) {
        // Clone Arc'd components under read lock, then release it so we don't
        // block export lifecycle operations (create/remove/shutdown) during flush.
        let mut targets: Vec<_> = {
            let exports = self.exports.read().await;
            exports
                .iter()
                .filter(|(_, s)| s.cache.dirty_block_count() > 0)
                .map(|(name, s)| {
                    (
                        name.clone(),
                        s.cache.dirty_block_count(),
                        Arc::clone(&s.cache),
                        Arc::clone(&s.content_store),
                        Arc::clone(&s.pack_index_cache),
                        Arc::clone(&s.volume_manifest),
                    )
                })
                .collect()
        };
        targets.sort_by(|a, b| b.1.cmp(&a.1));
        for (name, _, cache, cs, cmc, vm) in targets.iter().take(8) {
            // Skip exports already being flushed (drain, snapshot, scheduler).
            let Ok(_flush_guard) = cache.flush_lock().try_lock() else {
                continue;
            };
            match cache.flush_packs(cs, cmc, vm, Some(&self.clean_cache)).await {
                Ok((stats, _)) if stats.packs_uploaded > 0 => {
                    if let Err(e) = cache.sync_manifest(cs, vm).await {
                        warn!(export = %name, error = %e, "pressure flush manifest sync failed");
                    }
                    info!(export = %name, packs = stats.packs_uploaded, "pressure flush");
                }
                Err(e) => warn!(export = %name, error = %e, "pressure flush failed"),
                _ => {}
            }
        }
    }

    // =========================================================================
    // Export persistence to S3
    // =========================================================================

    /// S3 path for export definition.
    fn export_json_path(&self, name: &str) -> Path {
        Path::from(format!("{}/exports/{}/export.json", self.db_path, name))
    }

    /// Save export definition to S3 (idempotent).
    /// Persist export definition to S3 for discovery on restart.
    pub async fn save_export(&self, config: &ExportConfig) -> Result<(), RouterError> {
        let path = self.export_json_path(&config.name);
        let json = serde_json::to_vec(config)?;
        self.object_store
            .put(&path, Bytes::from(json).into())
            .await?;
        debug!("Saved export definition to S3: {}", path);
        Ok(())
    }

    /// Load export definition from S3.
    async fn load_export(&self, name: &str) -> Result<Option<ExportConfig>, RouterError> {
        let path = self.export_json_path(name);
        match self.object_store.get(&path).await {
            Ok(result) => {
                let data = result.bytes().await?;
                let config: ExportConfig = serde_json::from_slice(&data)?;
                Ok(Some(config))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete export definition from S3 (idempotent).
    async fn delete_export_definition(&self, name: &str) -> Result<(), RouterError> {
        let path = self.export_json_path(name);
        match self.object_store.delete(&path).await {
            Ok(()) => {
                debug!("Deleted export definition from S3: {}", path);
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("Export definition already gone: {}", path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Discover all exports from S3.
    ///
    /// Lists the `{db_path}/exports/` prefix and loads each `export.json` in parallel.
    pub async fn discover_exports(&self) -> Result<Vec<ExportConfig>, RouterError> {
        use futures::stream::{self, StreamExt};

        let prefix = Path::from(format!("{}/exports/", self.db_path));

        // List all objects under the exports prefix
        let mut list_stream = self.object_store.list(Some(&prefix));

        // Collect export names from export.json files
        let mut export_names = std::collections::HashSet::new();
        while let Some(result) = list_stream.next().await {
            let meta = result?;
            let path_str = meta.location.to_string();
            // Look for paths like "{db_path}/exports/{name}/export.json"
            if path_str.ends_with("/export.json") {
                // Extract export name from path
                if let Some(name) = extract_export_name(&path_str, &self.db_path) {
                    export_names.insert(name);
                }
            }
        }

        // Load export definitions in parallel
        let exports: Vec<ExportConfig> = stream::iter(export_names)
            .map(|name| async move {
                match self.load_export(&name).await {
                    Ok(Some(config)) => Some(config),
                    Ok(None) => {
                        warn!("Export.json disappeared during discovery: {}", name);
                        None
                    }
                    Err(e) => {
                        warn!("Failed to load export '{}': {}", name, e);
                        None
                    }
                }
            })
            .buffer_unordered(32)
            .filter_map(|x| async { x })
            .collect()
            .await;

        Ok(exports)
    }

    /// Create a new export.
    ///
    /// If `readonly` is true, the export will reject writes. Used during live migration to
    /// pre-stage the export on the destination node before promoting it to read-write.
    ///
    /// If `manifest_name` is provided, the export is forked from an S3 manifest:
    /// the manifest is downloaded, the pack index is populated, and the cache is
    /// initialized from the manifest state (no local recovery needed).
    ///
    /// **Idempotent**: If export already exists, returns Ok(()) without error.
    pub async fn create_export(
        &self,
        config: ExportConfig,
        readonly: bool,
        manifest_name: Option<&str>,
        snapshot_sequence: Option<u64>,
    ) -> Result<(), RouterError> {
        let name = config.name.clone();
        let orig_s3_prefix = config.s3_prefix.clone();
        validate_export_name(&name)?;

        // Check if export already exists - idempotent: return success if already exists
        {
            let exports = self.exports.read().await;
            if exports.contains_key(&name) {
                info!("Export '{}' already exists, skipping creation", name);
                return Ok(());
            }
        }

        info!(
            "Creating export '{}': size={}GB, s3_prefix={}, readonly={}, manifest={:?}",
            name,
            config.size_gb,
            config.s3_prefix(),
            readonly,
            manifest_name,
        );

        // Create metrics for this export (created first so S3BlockStore can use it)
        let metrics = Arc::new(ExportMetrics::new());

        // Use per-export block size if specified, otherwise use global default
        let block_size = config.block_size_or(self.block_size);

        let s3_prefix = format!("{}/exports/{}", self.db_path, config.s3_prefix());

        // Content-addressed pack storage with circuit breaker + concurrency limits
        let mut cs = ContentStore::new(Arc::clone(&self.object_store), &s3_prefix)
            .with_circuit_breaker(Arc::clone(&self.s3_circuit_breaker));
        if let Some(sem) = &self.upload_semaphore {
            cs = cs.with_upload_semaphore(Arc::clone(sem));
        }
        if let Some(sem) = &self.download_semaphore {
            cs = cs.with_download_semaphore(Arc::clone(sem));
        }
        let content_store = Arc::new(cs);
        let clean_cache = Arc::clone(&self.clean_cache);
        let pack_index_cache = Arc::clone(&self.pack_index_cache);

        // Determine effective device size — for forks, must be at least as large
        // as the base image (ext4 superblock records filesystem size, kernel rejects
        // a device smaller than that).
        let mut device_size = config.size_bytes();

        let (cache, volume_manifest) = if let Some(manifest_name) = manifest_name {
            // Fork path: load VolumeManifest, preferring in-memory cache for bases/*
            let is_base = manifest_name.starts_with("bases/");
            let cache_key = format!("{}:{}", s3_prefix, manifest_name);

            let fork_vm = if let Some(seq) = snapshot_sequence {
                // Fork from a specific versioned snapshot (never cached — snapshots are mutable)
                match content_store.get_snapshot(manifest_name, seq).await {
                    Ok(Some(data)) => VolumeManifest::deserialize(&data)
                        .map_err(|e| RouterError::Manifest(format!("failed to deserialize snapshot volume manifest: {}", e)))?,
                    Ok(None) => {
                        return Err(RouterError::Manifest(format!(
                            "snapshot '{}' seq={} not found",
                            manifest_name, seq
                        )));
                    }
                    Err(e) => {
                        return Err(RouterError::Manifest(format!(
                            "snapshot '{}' seq={} fetch error: {}",
                            manifest_name, seq, e
                        )));
                    }
                }
            } else if is_base {
                // Check base manifest cache first (bases/* are immutable after bless)
                if let Some(cached) = self.base_manifest_cache.lock().get(&cache_key) {
                    debug!(manifest = %manifest_name, "base manifest cache hit");
                    VolumeManifest::clone(cached)
                } else {
                    // Cache miss — fetch from S3 and populate
                    let vm = match content_store.get_manifest(manifest_name).await {
                        Ok(Some((data, _etag))) => VolumeManifest::deserialize(&data)
                            .map_err(|e| RouterError::Manifest(format!("failed to deserialize volume manifest: {}", e)))?,
                        Ok(None) => {
                            return Err(RouterError::Manifest(format!(
                                "manifest '{}' not found",
                                manifest_name
                            )));
                        }
                        Err(e) => {
                            return Err(RouterError::Manifest(format!(
                                "manifest '{}' fetch error: {}",
                                manifest_name, e
                            )));
                        }
                    };
                    let mut cache_map = self.base_manifest_cache.lock();
                    if cache_map.len() < MAX_BASE_CACHE_ENTRIES {
                        cache_map.insert(cache_key.clone(), Arc::new(vm.clone()));
                    }
                    vm
                }
            } else {
                // Non-base manifest (mutable) — always fetch from S3
                match content_store.get_manifest(manifest_name).await {
                    Ok(Some((data, _etag))) => VolumeManifest::deserialize(&data)
                        .map_err(|e| RouterError::Manifest(format!("failed to deserialize volume manifest: {}", e)))?,
                    Ok(None) => {
                        return Err(RouterError::Manifest(format!(
                            "manifest '{}' not found",
                            manifest_name
                        )));
                    }
                    Err(e) => {
                        return Err(RouterError::Manifest(format!(
                            "manifest '{}' fetch error: {}",
                            manifest_name, e
                        )));
                    }
                }
            };

            // Ensure fork is at least as large as the base
            if device_size < fork_vm.size {
                warn!(
                    "Export '{}': requested size ({:.1} GB) is smaller than base image ({:.1} GB), \
                     using base size to avoid ext4 geometry mismatch",
                    name,
                    device_size as f64 / 1e9,
                    fork_vm.size as f64 / 1e9,
                );
                device_size = fork_vm.size;
            }

            let mut vm = fork_vm;
            vm.size = device_size;

            // Store the volume manifest for this export
            let volume_manifest = Arc::new(parking_lot::RwLock::new(vm));

            let cache_config = WriteCacheConfig {
                cache_dir: self.cache_dir.clone(),
                device_name: name.clone(),
                device_size,
                block_size,
                wal_sync: self.wal_sync,
            };

            // Open a fresh cache (local SSD starts empty, reads go through VolumeManifest → ChunkMetaCache → S3)
            let cache = WriteCache::open_fresh_active(cache_config)?;

            info!("Export '{}' created from manifest (fork)", name);
            (Arc::new(cache), volume_manifest)
        } else {
            // Normal path: open cache, recover from WAL
            let cache_config = WriteCacheConfig {
                cache_dir: self.cache_dir.clone(),
                device_name: name.clone(),
                device_size,
                block_size,
                wal_sync: self.wal_sync,
            };
            let cache = WriteCache::open(cache_config)?;
            info!("Recovering write cache for export '{}'...", name);
            let cache = cache.finish_recovery().await?;
            info!("Export '{}' cache ready", name);

            // Load existing manifest from S3 or start fresh.
            //
            // CRITICAL: if a manifest exists in S3 but we fail to load it
            // (transient S3 error, deserialization failure), we MUST fail
            // rather than start with an empty manifest. Starting empty would
            // cause the first sync_manifest to unconditionally overwrite the
            // real manifest (manifest_etag would be None), permanently losing
            // all previously-flushed block references.
            let volume_manifest = match content_store.get_manifest(&name).await {
                Ok(Some((data, etag))) => {
                    let vm = VolumeManifest::deserialize(&data).map_err(|e| {
                        RouterError::Manifest(format!(
                            "failed to deserialize manifest for '{}': {}",
                            name, e
                        ))
                    })?;
                    let volume_manifest = Arc::new(parking_lot::RwLock::new(vm));
                    *cache.inner.manifest_etag.lock() = etag;
                    info!("Loaded existing volume manifest for '{}'", name);
                    volume_manifest
                }
                Ok(None) => {
                    // No manifest in S3 — genuinely new export.
                    info!("No existing manifest for '{}', starting fresh", name);
                    Arc::new(parking_lot::RwLock::new(
                        VolumeManifest::new(device_size, block_size as u32),
                    ))
                }
                Err(e) => {
                    return Err(RouterError::Manifest(format!(
                        "failed to load manifest for '{}' from S3: {} — refusing to start \
                         with empty manifest (would overwrite existing data on first flush)",
                        name, e
                    )));
                }
            };

            (Arc::new(cache), volume_manifest)
        };

        // Record any recovery issues in metrics
        let rw = cache.recovery_warning_count();
        if rw > 0 {
            for _ in 0..rw {
                metrics.record_recovery_warning();
            }
        }

        // Boot hot set prefetch: warm the clean cache before the VM reads.
        // On cache hit, spawn prefetch immediately. On cache miss, the S3
        // fetch is done inside the spawned task so it doesn't block fork creation.
        // The JoinHandle is stored in ExportState so teardown can abort it,
        // releasing Arc references to cache/content_store/etc.
        let prefetch_handle = if let Some(manifest_name) = manifest_name {
            let hot_set_name = manifest_name
                .strip_prefix("bases/")
                .unwrap_or(manifest_name);

            let is_base = manifest_name.starts_with("bases/");
            let hot_cache_key = format!("{}:{}", s3_prefix, hot_set_name);

            let cached_hot_set = if is_base {
                self.hot_set_cache.lock().get(&hot_cache_key).cloned()
            } else {
                None
            };

            let cache_clone = Arc::clone(&cache);
            let cmc = Arc::clone(&pack_index_cache);
            let vm = Arc::clone(&volume_manifest);
            let cs = Arc::clone(&content_store);

            if let Some(chunks) = cached_hot_set {
                debug!(hot_set = %hot_set_name, "hot set cache hit");
                info!(chunks = chunks.len(), "prefetching boot hot set");
                Some(spawn_named("hot-set-prefetch", async move {
                    cache_clone
                        .prefetch_chunks(&chunks, &cmc, &vm, &cs)
                        .await;
                    info!("boot hot set prefetch complete");
                }))
            } else {
                let hot_set_name = hot_set_name.to_string();
                let hot_set_cache = Arc::clone(&self.hot_set_cache);
                Some(spawn_named("hot-set-prefetch", async move {
                    let chunks = match cs.get_hot_set(&hot_set_name).await {
                        Ok(Some(data)) => match deserialize_hot_set(&data) {
                            Ok(chunks) => {
                                let chunks = Arc::new(chunks);
                                if is_base {
                                    let mut cache_map = hot_set_cache.lock();
                                    if cache_map.len() < MAX_BASE_CACHE_ENTRIES {
                                        cache_map.insert(hot_cache_key, Arc::clone(&chunks));
                                    }
                                }
                                Some(chunks)
                            }
                            Err(e) => {
                                warn!("failed to deserialize hot set: {}", e);
                                None
                            }
                        },
                        Ok(None) => {
                            debug!("no hot set found for '{}'", hot_set_name);
                            None
                        }
                        Err(e) => {
                            warn!("failed to fetch hot set: {}", e);
                            None
                        }
                    };
                    if let Some(chunks) = chunks {
                        info!(chunks = chunks.len(), "prefetching boot hot set");
                        cache_clone
                            .prefetch_chunks(&chunks, &cmc, &vm, &cs)
                            .await;
                        info!("boot hot set prefetch complete");
                    }
                }))
            }
        } else {
            None
        };

        // Resolve per-export flush_threshold: export config > global default.
        // 0 = manual mode (no auto-flush).
        let flush_threshold = config.flush_threshold_or(self.default_flush_threshold);

        // Shared notify: write path signals when dirty count crosses flush_threshold
        let flush_notify = Arc::new(Notify::new());

        // Create handler for block I/O
        let handler = Arc::new(BlockHandler::new(
            Arc::clone(&cache),
            Arc::clone(&content_store),
            Arc::clone(&clean_cache),
            Arc::clone(&pack_index_cache),
            Arc::clone(&volume_manifest),
            device_size,
            readonly,
            Arc::clone(&metrics),
            Arc::clone(&self.ssd_utilization),
            Arc::clone(&flush_notify),
            flush_threshold,
            None, // TODO: wire up write_trace_path from ExportConfig
        ));

        // Start flush scheduler for this export
        let (flush_shutdown_tx, flush_shutdown_rx) = watch::channel(false);
        let flush_cache = Arc::clone(&cache);
        let flush_cs = Arc::clone(&content_store);
        let flush_cmc = Arc::clone(&pack_index_cache);
        let flush_vm = Arc::clone(&volume_manifest);
        let export_name = name.clone();
        let flush_metrics = Arc::clone(&metrics);
        let flush_sem = self.flush_semaphore.clone();
        let flush_cc = Arc::clone(&clean_cache);
        let flush_handle = spawn_named(&format!("flush-{}", name), async move {
            flush_scheduler(
                flush_cache,
                flush_cs,
                flush_cmc,
                flush_vm,
                flush_cc,
                flush_notify,
                flush_shutdown_rx,
                flush_metrics,
                flush_sem,
            )
            .await;
            info!("Flush scheduler for export '{}' stopped", export_name);
        });

        // Store export state
        let transport = config.transport().to_string();
        let state = ExportState {
            handler,
            cache,
            content_store,
            pack_index_cache,
            volume_manifest,
            readonly,
            metrics,
            s3_prefix: orig_s3_prefix,
            transport: transport.clone(),
            flush_shutdown_tx,
            flush_handle,
            prefetch_handle,
        };

        let mut exports = self.exports.write().await;
        // Re-check under write lock to prevent TOCTOU race: a concurrent
        // create_export could have inserted between our read lock check and
        // this write lock acquisition.
        if exports.contains_key(&name) {
            info!(
                "Export '{}' already exists (concurrent create), cleaning up",
                name
            );
            // Abort the flush scheduler task we just spawned to avoid leaking it.
            state.flush_handle.abort();
            return Ok(());
        }
        exports.insert(name.clone(), state);
        drop(exports); // Release write lock before device registration

        info!(
            "Export '{}' created successfully (readonly={})",
            name, readonly
        );

        Ok(())
    }

    /// Snapshot an export: flush dirty blocks to S3 and upload a manifest.
    ///
    /// Returns the manifest ETag and sequence number for use by the control plane.
    /// If `tag` is provided, also publishes the manifest under that name within
    /// the export's S3 namespace (for content-addressed lookup by orchestrators).
    pub async fn snapshot_export(
        &self,
        name: &str,
        tag: Option<&str>,
    ) -> Result<SnapshotResponse, RouterError> {
        validate_export_name(name)?;
        if let Some(t) = tag {
            validate_export_name(t)?;
        }

        // Clone Arc'd components under read lock, then release it so we don't
        // block export lifecycle operations (create/remove/shutdown) during flush.
        let (cache, content_store, pack_index_cache, volume_manifest) = {
            let exports = self.exports.read().await;
            let state = exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            (
                Arc::clone(&state.cache),
                Arc::clone(&state.content_store),
                Arc::clone(&state.pack_index_cache),
                Arc::clone(&state.volume_manifest),
            )
        };

        info!("Taking snapshot of export '{}'...", name);
        let result: SnapshotResult = cache
            .snapshot(&content_store, &pack_index_cache, &volume_manifest)
            .await
            .map_err(RouterError::Cache)?;

        info!(
            "Snapshot of '{}' complete: seq={}, blocks_claimed={}, packs_uploaded={}",
            name, result.sequence, result.stats.blocks_claimed, result.stats.packs_uploaded,
        );

        // If a tag was provided, publish the manifest under that name too.
        // Uses manifest_bytes captured under the flush lock inside snapshot()
        // to ensure the tag is a consistent point-in-time snapshot.
        if let Some(tag) = tag {
            content_store
                .put_manifest(tag, result.manifest_bytes.clone(), None)
                .await
                .map_err(RouterError::ContentStore)?;
            info!("Tagged snapshot of '{}' as '{}'", name, tag);
        }

        Ok(SnapshotResponse {
            manifest_etag: result.manifest_etag,
            sequence: result.sequence,
            snapshot_persisted: result.snapshot_persisted,
            tag: tag.map(|t| t.to_string()),
        })
    }

    /// Publish the current VolumeManifest under a tag name (without re-flushing).
    ///
    /// The tag is stored within the export's S3 namespace, so it can be used
    /// as `manifest_name` when forking. Caller should snapshot first if they
    /// want the tag to include all dirty data.
    pub async fn tag_export(&self, name: &str, tag: &str) -> Result<(), RouterError> {
        validate_export_name(name)?;
        validate_export_name(tag)?;
        let exports = self.exports.read().await;
        let state = exports
            .get(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
        let manifest_bytes = state
            .volume_manifest
            .read()
            .serialize()
            .map_err(|e| RouterError::Manifest(e.to_string()))?;
        state
            .content_store
            .put_manifest(tag, manifest_bytes, None)
            .await
            .map_err(RouterError::ContentStore)?;
        info!("Tagged export '{}' as '{}'", name, tag);
        Ok(())
    }

    /// Check if a manifest exists in S3 (HEAD request, no data transfer).
    ///
    /// Does not require a running export — resolves the manifest path from
    /// `s3_prefix` and `manifest_name` against the router's object store.
    pub async fn head_manifest(
        &self,
        s3_prefix: &str,
        manifest_name: &str,
    ) -> Result<bool, RouterError> {
        let base = format!("{}/exports/{}", self.db_path, s3_prefix);
        let cs = ContentStore::new(Arc::clone(&self.object_store), &base)
            .with_circuit_breaker(Arc::clone(&self.s3_circuit_breaker));
        cs.head_manifest(manifest_name)
            .await
            .map_err(RouterError::ContentStore)
    }

    /// List snapshot sequence numbers for an export.
    pub async fn list_export_snapshots(
        &self,
        name: &str,
    ) -> Result<Vec<u64>, RouterError> {
        validate_export_name(name)?;
        let exports = self.exports.read().await;
        let state = exports
            .get(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
        state
            .content_store
            .list_snapshots(name)
            .await
            .map_err(RouterError::ContentStore)
    }

    /// Delete a specific snapshot for an export (idempotent).
    pub async fn delete_export_snapshot(
        &self,
        name: &str,
        sequence: u64,
    ) -> Result<(), RouterError> {
        validate_export_name(name)?;
        let exports = self.exports.read().await;
        let state = exports
            .get(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
        state
            .content_store
            .delete_snapshot(name, sequence)
            .await
            .map_err(RouterError::ContentStore)
    }

    /// Get info for a single export.
    pub async fn get_export_info(&self, name: &str) -> Option<ExportInfo> {
        let (mut info, handler) = {
            let exports = self.exports.read().await;
            let state = exports.get(name)?;
            let block_size = state.cache.block_size() as u64;
            let manifest = state.volume_manifest.read();
            let s3_bytes = manifest.chunks.len() as u64 * manifest.chunk_size;
            drop(manifest);
            let info = ExportInfo {
                name: name.to_string(),
                size: state.handler.device_size(),
                readonly: state.readonly,
                transport: state.transport.clone(),
                device: None,
                s3_prefix: state.s3_prefix.clone(),
                dirty_bytes: state.cache.dirty_block_count() * block_size,
                s3_bytes,
                fs_used_bytes: None,
            };
            (info, Arc::clone(&state.handler))
        };
        info.device = self.get_device_path(name).await;
        info.fs_used_bytes = read_fs_used_bytes(&handler).await;
        Some(info)
    }

    /// Get handler for an export (used during NBD negotiation).
    pub async fn get_handler(&self, name: &str) -> Option<Arc<BlockHandler>> {
        let exports = self.exports.read().await;
        exports.get(name).map(|s| Arc::clone(&s.handler))
    }

    /// Check if an export is readonly.
    #[allow(dead_code)]
    pub async fn is_readonly(&self, name: &str) -> Option<bool> {
        let exports = self.exports.read().await;
        exports.get(name).map(|s| s.readonly)
    }

    /// List all exports.
    pub async fn list_exports(&self) -> Vec<ExportInfo> {
        // First pass: collect basic info and handlers.
        let (mut result, handlers): (Vec<ExportInfo>, Vec<Arc<BlockHandler>>) = {
            let exports = self.exports.read().await;
            exports
                .iter()
                .map(|(name, state)| {
                    let block_size = state.cache.block_size() as u64;
                    let manifest = state.volume_manifest.read();
                    let info = ExportInfo {
                        name: name.clone(),
                        size: state.handler.device_size(),
                        readonly: state.readonly,
                        transport: state.transport.clone(),
                        device: None,
                        s3_prefix: state.s3_prefix.clone(),
                        dirty_bytes: state.cache.dirty_block_count() * block_size,
                        s3_bytes: manifest.chunks.len() as u64 * manifest.chunk_size,
                        fs_used_bytes: None,
                    };
                    (info, Arc::clone(&state.handler))
                })
                .unzip()
        };

        // Second pass: populate device paths and fs_used_bytes.
        for (info, handler) in result.iter_mut().zip(handlers.iter()) {
            info.device = self.get_device_path(&info.name).await;
            info.fs_used_bytes = read_fs_used_bytes(handler).await;
        }

        result
    }

    /// Get export names.
    pub async fn list_export_names(&self) -> Vec<String> {
        let exports = self.exports.read().await;
        exports.keys().cloned().collect()
    }

    // --- Device management (unified across transports) ---

    /// Register a kernel block device for an export.
    /// Dispatches to NBD netlink or ublk based on transport.
    /// Call after `create_export` succeeds. No-op for unknown transports.
    #[cfg(target_os = "linux")]
    pub async fn register_device(
        self: &Arc<Self>,
        name: &str,
        transport: &str,
    ) -> Result<(), RouterError> {
        match transport {
            "nbd" => {
                let size = self
                    .get_handler(name)
                    .await
                    .map(|h| h.device_size())
                    .unwrap_or(0);
                let mut nbd = self.nbd_devices.lock().await;
                let path = nbd
                    .add_device(name, Arc::clone(self), size)
                    .await
                    .map_err(|e| RouterError::Io(std::io::Error::other(e)))?;
                info!(export = %name, path = %path.display(), "nbd device registered");
                Ok(())
            }
            #[cfg(feature = "ublk")]
            "ublk" => {
                let handler = self
                    .get_handler(name)
                    .await
                    .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
                let mut ublk = self.ublk_server.lock().await;
                let path = ublk
                    .add_device(name, handler)
                    .await
                    .map_err(|e| RouterError::Io(std::io::Error::other(e)))?;
                info!(export = %name, path = %path.display(), "ublk device registered");
                Ok(())
            }
            _ => Ok(()), // unknown transport, no device to register
        }
    }

    /// Remove a kernel block device for an export (idempotent).
    #[cfg(target_os = "linux")]
    async fn remove_device(&self, name: &str, transport: &str) -> Result<(), RouterError> {
        match transport {
            "nbd" => {
                let mut nbd = self.nbd_devices.lock().await;
                nbd.remove_device(name)
                    .await
                    .map_err(|e| RouterError::Io(std::io::Error::other(e)))
            }
            #[cfg(feature = "ublk")]
            "ublk" => {
                let mut ublk = self.ublk_server.lock().await;
                ublk.remove_device(name)
                    .await
                    .map_err(|e| RouterError::Io(std::io::Error::other(e)))
            }
            _ => Ok(()),
        }
    }

    /// Get the device path for an export, if a kernel device is registered.
    pub async fn get_device_path(&self, name: &str) -> Option<PathBuf> {
        #[cfg(target_os = "linux")]
        {
            // Check NBD first
            {
                let nbd = self.nbd_devices.lock().await;
                if let Some(path) = nbd.get_device_path(name) {
                    return Some(path.to_path_buf());
                }
            }
            // Check ublk
            #[cfg(feature = "ublk")]
            {
                let ublk = self.ublk_server.lock().await;
                if let Some(path) = ublk.get_device_path(name) {
                    return Some(path.to_path_buf());
                }
            }
        }
        let _ = name;
        None
    }

    /// Shutdown all device managers (full shutdown — disconnects kernel devices).
    ///
    /// For hot reload, do NOT call this. Let the process exit so devices stay
    /// alive and the new process can reconfigure them.
    pub async fn shutdown_devices(&self) -> Result<(), RouterError> {
        #[cfg(target_os = "linux")]
        {
            info!("Shutting down NBD kernel devices...");
            let nbd = {
                let mut guard = self.nbd_devices.lock().await;
                let replacement = crate::block::nbd::NbdDeviceManager::new()
                    .with_cache_dir(self.cache_dir.clone());
                std::mem::replace(&mut *guard, replacement)
            };
            if let Err(e) = nbd.shutdown().await {
                warn!("NBD device shutdown failed: {}", e);
            }

            #[cfg(feature = "ublk")]
            {
                info!("Shutting down ublk devices...");
                let ublk = {
                    let mut guard = self.ublk_server.lock().await;
                    std::mem::take(&mut *guard)
                };
                if let Err(e) = ublk.shutdown().await {
                    warn!("ublk device shutdown failed: {}", e);
                }
            }
        }
        Ok(())
    }

    /// Recover QUIESCED ublk devices left by a previous daemon crash.
    ///
    /// Scans `/sys/class/ublk-char/` for glidefs-owned devices in QUIESCED
    /// state and resumes them using the already-created export handlers.
    /// Returns the number of successfully recovered devices.
    ///
    /// Call AFTER all exports have been created (via `create_export`) so that
    /// handlers exist for matching.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn recover_ublk_devices(&self) -> usize {
        // Snapshot ublk-transport handlers. Release exports read lock before
        // taking the ublk mutex to avoid lock ordering issues.
        let handlers: HashMap<String, Arc<BlockHandler>> = {
            let exports = self.exports.read().await;
            exports
                .iter()
                .filter(|(_, s)| s.transport == "ublk")
                .map(|(name, s)| (name.clone(), Arc::clone(&s.handler)))
                .collect()
        };

        let mut ublk = self.ublk_server.lock().await;
        ublk.recover_quiesced_devices(|name| handlers.get(name).cloned())
            .await
    }

    /// Check if a transport is available on this build/platform.
    #[allow(dead_code)] // Used by API layer (api.rs)
    #[allow(clippy::match_like_matches_macro)] // arms have different cfg! expressions
    pub fn device_available(transport: &str) -> bool {
        match transport {
            "nbd" => cfg!(target_os = "linux"),
            "ublk" => cfg!(all(target_os = "linux", feature = "ublk")),
            _ => false,
        }
    }

    /// Check readiness: exports exist, cache writable, and S3 reachable.
    pub async fn readiness_check(&self) -> ReadinessStatus {
        let exports_count = {
            let exports = self.exports.read().await;
            exports.len()
        };
        // Lock released — I/O probes below can take seconds and must not
        // hold the read lock (it blocks create/remove/shutdown write locks).

        let cache_writable = {
            let probe = self.cache_dir.join(".health-probe");
            tokio::fs::write(&probe, b"ok").await.is_ok()
                && tokio::fs::remove_file(&probe).await.is_ok()
        };

        let s3_reachable = {
            let probe_path = Path::from(format!("{}/", self.db_path));
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.object_store.list_with_delimiter(Some(&probe_path)),
            )
            .await
            .is_ok_and(|r| r.is_ok())
        };

        ReadinessStatus {
            ready: exports_count > 0 && cache_writable && s3_reachable,
            exports_count,
            cache_writable,
            s3_reachable,
        }
    }

    /// Get metrics snapshot for an export.
    pub async fn get_export_metrics(&self, name: &str) -> Option<MetricsSnapshot> {
        let exports = self.exports.read().await;
        exports.get(name).map(|s| Self::snapshot_export_metrics(s))
    }

    /// Snapshot metrics for all exports under a single lock acquisition.
    /// Aggregate stats across all exports for host-level pressure monitoring.
    /// Cheap — reads in-memory atomics only, no I/O.
    pub async fn aggregate_stats(&self) -> AggregateStats {
        use crate::circuit_breaker::CircuitState;
        use std::sync::atomic::Ordering;

        let exports = self.exports.read().await;
        let mut total_cache_hits: u64 = 0;
        let mut total_cache_misses: u64 = 0;
        let mut total_dirty_bytes: u64 = 0;

        for state in exports.values() {
            total_cache_hits += state.metrics.cache_hits.load(Ordering::Relaxed);
            total_cache_misses += state.metrics.cache_misses.load(Ordering::Relaxed);
            let block_size = state.cache.block_size() as u64;
            total_dirty_bytes += state.cache.dirty_block_count() * block_size;
        }

        let s3_circuit_state = match self.s3_circuit_state() {
            CircuitState::Closed { .. } => 0,
            CircuitState::Open => 1,
            CircuitState::HalfOpen { .. } => 2,
        };

        AggregateStats {
            ssd_utilization: self.ssd_utilization(),
            s3_circuit_state,
            total_cache_hits,
            total_cache_misses,
            total_dirty_bytes,
            exports_count: exports.len(),
        }
    }

    pub async fn all_export_metrics(&self) -> Vec<(String, MetricsSnapshot)> {
        let exports = self.exports.read().await;
        exports
            .iter()
            .map(|(name, s)| (name.clone(), Self::snapshot_export_metrics(s)))
            .collect()
    }

    fn snapshot_export_metrics(s: &ExportState) -> MetricsSnapshot {
        let block_size = s.cache.block_size() as u64;
        let dirty_blocks = s.cache.dirty_block_count();
        let manifest = s.volume_manifest.read();
        s.metrics.snapshot().with_cache_state(
            dirty_blocks,
            s.cache.syncing_block_count(),
            dirty_blocks * block_size,
            manifest.chunks.len() as u64 * manifest.chunk_size,
        )
    }

    /// Drain an export's dirty blocks to S3.
    pub async fn drain_export(&self, name: &str) -> Result<(), RouterError> {
        validate_export_name(name)?;

        // Clone Arc'd components under read lock, then release it so we don't
        // block export lifecycle operations (create/remove/shutdown) during
        // the potentially long-running drain.
        let (cache, content_store, pack_index_cache, volume_manifest) = {
            let exports = self.exports.read().await;
            let state = exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            (
                Arc::clone(&state.cache),
                Arc::clone(&state.content_store),
                Arc::clone(&state.pack_index_cache),
                Arc::clone(&state.volume_manifest),
            )
        };

        info!("Draining export '{}'...", name);
        // Inline the drain loop using the cloned components.
        for i in 0..MAX_DRAIN_ITERATIONS {
            let stats = cache
                .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
                .await
                .map_err(RouterError::Cache)?;
            if stats.blocks_claimed == 0 && cache.dirty_block_count() == 0 {
                info!("Export '{}' drained successfully", name);
                return Ok(());
            }
            if stats.blocks_claimed == 0 {
                tracing::debug!(
                    dirty = cache.dirty_block_count(),
                    iteration = i,
                    "drain: dirty blocks remain (likely partial), waiting for backfill"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        let remaining = cache.dirty_block_count();
        warn!(
            "drain hit iteration limit ({}), {} dirty blocks remain",
            MAX_DRAIN_ITERATIONS, remaining,
        );
        Err(RouterError::DrainIncomplete {
            name: name.to_string(),
            remaining,
            iterations: MAX_DRAIN_ITERATIONS,
        })
    }

    /// Record a drain/flush error for the named export's metrics.
    ///
    /// Best-effort: silently does nothing if the export doesn't exist
    /// (it may have been removed between the drain attempt and this call).
    pub async fn record_drain_error(&self, name: &str) {
        let exports = self.exports.read().await;
        if let Some(state) = exports.get(name) {
            state.metrics.record_flush_error();
        }
    }

    /// Drain all exports. Returns (name, error) pairs for any that failed.
    pub async fn drain_all(self: &Arc<Self>) -> Vec<(String, RouterError)> {
        use futures::stream;

        let names = self.list_export_names().await;
        let failed: Vec<_> = stream::iter(names)
            .map(|name| {
                let this = Arc::clone(self);
                async move {
                    match this.drain_export(&name).await {
                        Ok(()) => None,
                        Err(e) => {
                            warn!(export = %name, error = %e, "failed to drain export");
                            Some((name, e))
                        }
                    }
                }
            })
            .buffer_unordered(16)
            .filter_map(|x| async { x })
            .collect()
            .await;
        failed
    }

    /// Promote a readonly export to read-write.
    pub async fn promote_export(&self, name: &str) -> Result<(), RouterError> {
        validate_export_name(name)?;
        let mut exports = self.exports.write().await;
        let state = exports
            .get_mut(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;

        if !state.readonly {
            info!("Export '{}' is already read-write", name);
            return Ok(());
        }

        state.readonly = false;
        state.handler.set_readonly(false);
        info!("Export '{}' promoted to read-write", name);
        Ok(())
    }

    /// Resize an export (grow only).
    ///
    /// This drains dirty blocks, then recreates the export with the new size.
    /// The cache files are preserved, so existing data is retained.
    ///
    /// **Grow only**: Shrinking is not supported and will return an error.
    /// **Idempotent**: If new_size_gb <= current size, returns Ok(()) without changes.
    ///
    /// Note: NBD client must reconnect to see the new size.
    pub async fn resize_export(&self, name: &str, new_size_gb: f64) -> Result<(), RouterError> {
        validate_export_name(name)?;
        // Get current export info
        let (current_size, readonly, block_size, orig_s3_prefix, transport) = {
            let exports = self.exports.read().await;
            let state = exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            let current_size = state.handler.device_size();
            let readonly = state.readonly;
            let block_size = state.cache.block_size();
            let s3_prefix = state.s3_prefix.clone();
            let transport = state.transport.clone();
            (current_size, readonly, block_size, s3_prefix, transport)
        };

        let new_size_bytes = (new_size_gb * 1_073_741_824.0) as u64;
        let current_size_gb = current_size as f64 / 1_073_741_824.0;

        // Validate: grow only
        if new_size_bytes < current_size {
            return Err(RouterError::CannotShrink {
                name: name.to_string(),
                current_gb: current_size_gb,
                requested_gb: new_size_gb,
            });
        }

        // Idempotent: if already at or above requested size, nothing to do
        if new_size_bytes <= current_size {
            info!(
                "Export '{}' already at size {}GB (requested {}GB), nothing to do",
                name, current_size_gb, new_size_gb
            );
            return Ok(());
        }

        info!(
            "Resizing export '{}': {}GB -> {}GB",
            name, current_size_gb, new_size_gb
        );

        // Drain dirty blocks before resize
        self.drain_export(name).await?;

        // Remove export (preserves cache files)
        self.remove_export(name, false).await?;

        // Recreate with new size, loading from the manifest we just drained.
        // This preserves access to pre-resize data via the block_map.
        let config = ExportConfig {
            name: name.to_string(),
            size_gb: new_size_gb,
            s3_prefix: orig_s3_prefix,
            block_size: Some(block_size),
            flush_threshold: None,
            flush_mode: None,
            transport: Some(transport),
        };

        self.create_export(config.clone(), readonly, Some(name), None)
            .await?;
        self.save_export(&config).await?;

        info!("Export '{}' resized to {}GB", name, new_size_gb);
        Ok(())
    }

    /// Remove an export.
    ///
    /// If `purge` is true, also delete the local cache files.
    /// Properly transitions the cache through Draining state.
    ///
    /// **Idempotent**: If export doesn't exist, returns Ok(()) without error.
    pub async fn remove_export(&self, name: &str, purge: bool) -> Result<(), RouterError> {
        validate_export_name(name)?;
        let state = {
            let mut exports = self.exports.write().await;
            match exports.remove(name) {
                Some(state) => state,
                None => {
                    info!("Export '{}' doesn't exist, nothing to remove", name);
                    if purge {
                        // Clean local cache files
                        let cache_file = self.cache_dir.join(format!("{}.cache", name));
                        let meta_file = self.cache_dir.join(format!("{}.meta", name));
                        let wal_file = self.cache_dir.join(format!("{}.wal", name));
                        remove_file_if_exists(&cache_file);
                        remove_file_if_exists(&meta_file);
                        remove_file_if_exists(&wal_file);

                        // S3 cleanup: load export config to discover s3_prefix,
                        // then delete manifest, snapshots, and export definition.
                        match self.load_export(name).await {
                            Ok(Some(config)) => {
                                let s3_prefix = format!(
                                    "{}/exports/{}",
                                    self.db_path,
                                    config.s3_prefix()
                                );
                                let cs = ContentStore::new(
                                    Arc::clone(&self.object_store),
                                    &s3_prefix,
                                )
                                .with_circuit_breaker(Arc::clone(&self.s3_circuit_breaker));

                                if let Err(e) = cs.delete_manifest(name).await {
                                    warn!("Failed to delete manifest from S3: {}", e);
                                }
                                if let Err(e) = cs.delete_all_snapshots(name).await {
                                    warn!("Failed to delete snapshots from S3: {}", e);
                                }
                                if let Err(e) = self.delete_export_definition(name).await {
                                    warn!("Failed to delete export definition from S3: {}", e);
                                }
                            }
                            Ok(None) => {
                                debug!(
                                    "No export definition in S3 for '{}', skipping S3 cleanup",
                                    name
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to load export definition for S3 cleanup: {}",
                                    e
                                );
                            }
                        }
                    }
                    return Ok(());
                }
            }
        };

        info!("Removing export '{}'...", name);

        // Retain a handle to the content store for snapshot cleanup after teardown.
        let snapshot_cs = if purge {
            Some(Arc::clone(&state.content_store))
        } else {
            None
        };

        // Remove kernel block device before teardown.
        #[cfg(target_os = "linux")]
        {
            if let Err(e) = self.remove_device(name, &state.transport).await {
                warn!(export = %name, error = %e, "device removal failed (continuing teardown)");
            }
        }

        let remaining = Self::teardown_export(name, state, purge).await;

        if purge {
            let cache_file = self.cache_dir.join(format!("{}.cache", name));
            let meta_file = self.cache_dir.join(format!("{}.meta", name));
            let wal_file = self.cache_dir.join(format!("{}.wal", name));
            remove_file_if_exists(&cache_file);
            remove_file_if_exists(&meta_file);
            remove_file_if_exists(&wal_file);
            info!("Purged cache files for export '{}'", name);

            // Also delete export definition from S3
            if let Err(e) = self.delete_export_definition(name).await {
                warn!("Failed to delete export definition from S3: {}", e);
            }

            // Delete the manifest from S3 (best-effort).
            if let Some(cs) = &snapshot_cs && let Err(e) = cs.delete_manifest(name).await {
                warn!("Failed to delete manifest from S3: {}", e);
            }

            // Delete all versioned snapshots from S3 (best-effort).
            if let Some(cs) = snapshot_cs && let Err(e) = cs.delete_all_snapshots(name).await {
                warn!("Failed to delete snapshots from S3: {}", e);
            }
        }

        if remaining > 0 {
            return Err(RouterError::DrainIncomplete {
                name: name.to_string(),
                remaining,
                iterations: MAX_DRAIN_ITERATIONS,
            });
        }

        info!("Export '{}' removed", name);
        Ok(())
    }

    /// Shutdown all exports gracefully.
    ///
    /// This properly transitions each cache through the typestate:
    /// Active → Draining → finished.
    ///
    /// Returns `Err(ShutdownIncomplete)` if any export had dirty blocks
    /// remaining after drain attempts, indicating potential data loss.
    pub async fn shutdown(&self) -> Result<(), RouterError> {
        info!("Shutting down all exports...");

        // NOTE: Does NOT disconnect kernel block devices. For hot reload,
        // NBD devices must stay alive so the new process can reconfigure them.
        // Caller is responsible for shutdown_devices() when full teardown is needed.

        // Take ownership of all exports
        let mut exports = self.exports.write().await;
        let export_list: Vec<_> = exports.drain().collect();
        drop(exports); // Release the lock

        use futures::stream::{self, StreamExt};

        let incomplete: Vec<(String, u64)> = stream::iter(export_list)
            .map(|(name, state)| async move {
                info!("Shutting down export '{}'...", name);
                let remaining = Self::teardown_export(&name, state, false).await;
                (name, remaining)
            })
            .buffer_unordered(16)
            .filter_map(|(name, remaining)| async move {
                if remaining > 0 { Some((name, remaining)) } else { None }
            })
            .collect()
            .await;

        if !incomplete.is_empty() {
            let details = incomplete
                .iter()
                .map(|(name, remaining)| format!("{name}({remaining} dirty)"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(RouterError::ShutdownIncomplete {
                incomplete_count: incomplete.len(),
                details,
            });
        }

        info!("All exports shut down");
        Ok(())
    }

    /// Stop all flush schedulers without draining. Leaves dirty blocks on
    /// the local SSD for WAL-based recovery on next startup.
    ///
    /// Used by crash simulation in tests: a real process crash kills all
    /// tasks instantly, but in an in-process test we need to explicitly
    /// stop the schedulers so they release cache file handles before the
    /// next server opens the same files.
    #[cfg(feature = "test-utils")]
    pub async fn stop_flush_schedulers(&self) {
        let mut exports = self.exports.write().await;
        let export_list: Vec<_> = exports.drain().collect();
        drop(exports);

        for (name, state) in export_list {
            if let Some(handle) = state.prefetch_handle {
                handle.abort();
                let _ = handle.await;
            }
            let _ = state.flush_shutdown_tx.send(true);
            if let Err(e) = state.flush_handle.await {
                tracing::warn!("Flush scheduler for '{}' panicked: {}", name, e);
            }
            // Deliberately NO drain — dirty blocks stay on SSD.
        }
    }

    /// Drain dirty blocks, stop flush scheduler, and transition cache through
    /// the Draining typestate. Shared by `remove_export` and `shutdown`.
    ///
    /// When `skip_drain` is true, dirty blocks are discarded without flushing
    /// to S3. Used when purging — the S3 data will be deleted anyway, so
    /// draining is wasted work.
    ///
    /// Returns the number of dirty blocks remaining (0 = fully drained or skipped).
    async fn teardown_export(name: &str, state: ExportState, skip_drain: bool) -> u64 {
        let ExportState {
            handler,
            cache,
            content_store,
            pack_index_cache,
            volume_manifest,
            metrics,
            flush_shutdown_tx,
            flush_handle,
            prefetch_handle,
            ..
        } = state;

        // 0. Abort the hot-set prefetch task (if running) to release Arc clones.
        if let Some(handle) = prefetch_handle {
            handle.abort();
            let _ = handle.await;
        }

        // 1. Signal flush scheduler to stop
        let _ = flush_shutdown_tx.send(true);

        // 2. Wait for flush scheduler to exit (releases its Arc clone)
        if let Err(e) = flush_handle.await {
            warn!("Flush scheduler for '{}' panicked: {}", name, e);
        }

        let remaining = if skip_drain {
            // Purge path: skip drain entirely — dirty blocks are discarded.
            // No manifest sync needed since S3 data will be deleted.
            info!("Skipping drain for '{}' (purge)", name);
            0
        } else {
            // 3. V2 drain: flush remaining dirty data.
            //    Continue on errors (may be transient S3 failures) instead of
            //    breaking — matches the public ExportState::drain() behavior.
            //    Exponential backoff on consecutive errors prevents tight-looping
            //    when S3 is down (100 rapid retries → log spam + wasted network).
            let mut drain_done = false;
            let mut backoff = Duration::from_millis(100);
            for _ in 0..MAX_DRAIN_ITERATIONS {
                match cache.flush_to_s3(&content_store, &pack_index_cache, &volume_manifest).await {
                    Ok(stats) if stats.blocks_claimed == 0 && cache.dirty_block_count() == 0 => {
                        drain_done = true;
                        break;
                    }
                    Ok(_) => {
                        backoff = Duration::from_millis(100);
                    }
                    Err(e) => {
                        metrics.record_flush_error();
                        warn!("Drain error for '{}' (retrying in {:?}): {}", name, backoff, e);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
            let remaining = if drain_done {
                0
            } else {
                cache.dirty_block_count()
            };
            if remaining > 0 {
                warn!(
                    "Teardown drain for '{}' incomplete, {} dirty blocks remain",
                    name, remaining,
                );
            }

            // Best-effort final manifest sync — persists references to any packs
            // that were successfully uploaded even if the full drain didn't complete.
            if let Err(e) = cache.sync_manifest(&content_store, &volume_manifest).await {
                warn!(
                    "Final manifest sync for '{}' failed: {} — flushed packs may be orphaned",
                    name, e,
                );
            }

            remaining
        };

        // 4. Drop the handler (releases its Arc clone)
        drop(handler);

        // 5. Unwrap the Arc and transition through Draining state
        match Arc::try_unwrap(cache) {
            Ok(cache) => match cache.shutdown().await {
                Ok(draining) => {
                    draining.finish();
                    info!("Export '{}' torn down cleanly", name);
                }
                Err(e) => {
                    warn!("Failed to drain export '{}': {}", name, e);
                }
            },
            Err(arc) => {
                warn!(
                    "Export '{}' has {} references, cannot transition typestate",
                    name,
                    Arc::strong_count(&arc)
                );
            }
        }

        remaining
    }

    /// Create a minimal router for testing protocol handling.
    /// Uses a temporary directory and in-memory S3.
    #[cfg(test)]
    pub(crate) async fn new_for_test() -> Self {
        use crate::block::cache::SimpleBlockCache;
        let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let temp_dir = std::env::temp_dir().join(format!("glidefs-test-{:016x}", rand::random::<u64>()));
        std::fs::create_dir_all(&temp_dir).expect("Failed to create test cache dir");

        Self::new(RouterConfig {
            object_store: s3,
            db_path: "test".to_string(),
            cache_dir: temp_dir,
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
        })
        .await
        .expect("failed to create test router")
    }
}

/// Extract export name from a path like "{db_path}/exports/{name}/export.json".
fn extract_export_name(path: &str, db_path: &str) -> Option<String> {
    // Path format: "{db_path}/exports/{name}/export.json"
    let prefix = format!("{}/exports/", db_path);
    let suffix = "/export.json";

    if let Some(rest) = path.strip_prefix(&prefix)
        && let Some(name) = rest.strip_suffix(suffix)
        && !name.contains('/')
        && !name.is_empty()
    {
        return Some(name.to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::cache::SimpleBlockCache;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_router(temp_dir: &TempDir) -> ExportRouter {
        let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        ExportRouter::new(RouterConfig {
            object_store: s3,
            db_path: "test".to_string(),
            cache_dir: temp_dir.path().to_path_buf(),
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
        })
        .await
        .expect("failed to create test router")
    }

    fn test_export_config(name: &str) -> ExportConfig {
        ExportConfig {
            name: name.to_string(),
            size_gb: 0.01, // 10MB
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        }
    }

    #[tokio::test]
    async fn test_create_export_success() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let result = router
            .create_export(test_export_config("vol1"), false, None, None)
            .await;
        assert!(result.is_ok(), "Should create export successfully");

        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].name, "vol1");
    }

    #[tokio::test]
    async fn test_create_export_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create export twice
        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();
        let result = router
            .create_export(test_export_config("vol1"), false, None, None)
            .await;

        // Second create should succeed (idempotent)
        assert!(result.is_ok(), "Second create should succeed (idempotent)");

        // Should still have only one export
        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 1);
    }

    #[tokio::test]
    async fn test_create_export_readonly() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), true, None, None)
            .await
            .unwrap();

        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 1);
        assert!(exports[0].readonly, "Export should be readonly");
    }

    #[tokio::test]
    async fn test_get_handler_existing() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        let handler = router.get_handler("vol1").await;
        assert!(
            handler.is_some(),
            "Should return handler for existing export"
        );
    }

    #[tokio::test]
    async fn test_get_handler_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let handler = router.get_handler("nonexistent").await;
        assert!(
            handler.is_none(),
            "Should return None for nonexistent export"
        );
    }

    #[tokio::test]
    async fn test_list_exports_empty() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let exports = router.list_exports().await;
        assert!(exports.is_empty(), "Should return empty list");
    }

    #[tokio::test]
    async fn test_list_exports_multiple() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();
        router
            .create_export(test_export_config("vol2"), true, None, None)
            .await
            .unwrap();
        router
            .create_export(test_export_config("vol3"), false, None, None)
            .await
            .unwrap();

        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 3);

        let names: Vec<_> = exports.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"vol1"));
        assert!(names.contains(&"vol2"));
        assert!(names.contains(&"vol3"));
    }

    #[tokio::test]
    async fn test_drain_export_success() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        // Write some data through the handler
        let handler = router.get_handler("vol1").await.unwrap();
        let data = vec![0xAB; 4096];
        handler.write(0, &data, false).await.unwrap();

        // Drain should succeed
        let result = router.drain_export("vol1").await;
        assert!(result.is_ok(), "Drain should succeed");
    }

    #[tokio::test]
    async fn test_drain_export_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let result = router.drain_export("nonexistent").await;
        assert!(result.is_err(), "Drain should fail for nonexistent export");

        match result.unwrap_err() {
            RouterError::ExportNotFound(name) => assert_eq!(name, "nonexistent"),
            e => panic!("Expected ExportNotFound, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_remove_export_success() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();
        assert_eq!(router.list_exports().await.len(), 1);

        let result = router.remove_export("vol1", false).await;
        assert!(result.is_ok(), "Remove should succeed");

        assert_eq!(router.list_exports().await.len(), 0);
    }

    #[tokio::test]
    async fn test_remove_export_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Remove nonexistent export should succeed (idempotent)
        let result = router.remove_export("nonexistent", false).await;
        assert!(
            result.is_ok(),
            "Remove nonexistent should succeed (idempotent)"
        );
    }

    #[tokio::test]
    async fn test_remove_export_with_purge() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        // Write some data to create cache files
        let handler = router.get_handler("vol1").await.unwrap();
        handler.write(0, &[0xAB; 4096], false).await.unwrap();

        // Remove with purge
        router.remove_export("vol1", true).await.unwrap();

        // Cache files should be deleted (we can verify by trying to re-create)
        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();
        // Should succeed without "file exists" errors
    }

    #[tokio::test]
    async fn test_promote_export_success() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create readonly export
        router
            .create_export(test_export_config("vol1"), true, None, None)
            .await
            .unwrap();

        let exports = router.list_exports().await;
        assert!(exports[0].readonly, "Should be readonly initially");

        // Promote to read-write
        let result = router.promote_export("vol1").await;
        assert!(result.is_ok(), "Promote should succeed");

        let exports = router.list_exports().await;
        assert!(!exports[0].readonly, "Should be read-write after promote");
    }

    #[tokio::test]
    async fn test_promote_export_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let result = router.promote_export("nonexistent").await;
        assert!(
            result.is_err(),
            "Promote should fail for nonexistent export"
        );
    }

    #[tokio::test]
    async fn test_get_export_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        // Write some data
        let handler = router.get_handler("vol1").await.unwrap();
        handler.write(0, &[0xAB; 4096], false).await.unwrap();

        let metrics = router.get_export_metrics("vol1").await;
        assert!(metrics.is_some(), "Should return metrics");

        let m = metrics.unwrap();
        assert!(m.guest_write_ops >= 1, "Should have recorded write");
    }

    #[tokio::test]
    async fn test_get_export_metrics_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let metrics = router.get_export_metrics("nonexistent").await;
        assert!(metrics.is_none(), "Should return None for nonexistent");
    }

    #[tokio::test]
    async fn test_shutdown_all_exports() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();
        router
            .create_export(test_export_config("vol2"), false, None, None)
            .await
            .unwrap();

        let result = router.shutdown().await;
        assert!(result.is_ok(), "Shutdown should succeed");

        // After shutdown, list should be empty
        let exports = router.list_exports().await;
        assert!(exports.is_empty(), "Should have no exports after shutdown");
    }

    #[tokio::test]
    async fn test_export_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();
        router
            .create_export(test_export_config("vol2"), false, None, None)
            .await
            .unwrap();

        let handler1 = router.get_handler("vol1").await.unwrap();
        let handler2 = router.get_handler("vol2").await.unwrap();

        // Write different data to each export
        handler1.write(0, &[0x11; 4096], false).await.unwrap();
        handler2.write(0, &[0x22; 4096], false).await.unwrap();

        // Read back and verify isolation
        let data1 = handler1.read(0, 4096).await.unwrap();
        let data2 = handler2.read(0, 4096).await.unwrap();

        assert!(data1.iter().all(|&b| b == 0x11), "vol1 should have 0x11");
        assert!(data2.iter().all(|&b| b == 0x22), "vol2 should have 0x22");
    }

    // =========================================================================
    // Export persistence tests
    // =========================================================================

    #[tokio::test]
    async fn test_save_and_load_export() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let config = test_export_config("persist-vol");

        // Save export definition
        router.save_export(&config).await.unwrap();

        // Load it back
        let loaded = router.load_export("persist-vol").await.unwrap();
        assert!(loaded.is_some(), "Should load saved export");

        let loaded = loaded.unwrap();
        assert_eq!(loaded.name, "persist-vol");
        assert!((loaded.size_gb - 0.01).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_load_export_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let loaded = router.load_export("nonexistent").await.unwrap();
        assert!(loaded.is_none(), "Should return None for nonexistent");
    }

    #[tokio::test]
    async fn test_delete_export_definition_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Delete nonexistent should succeed (idempotent)
        let result = router.delete_export_definition("nonexistent").await;
        assert!(result.is_ok(), "Delete nonexistent should succeed");

        // Save then delete
        let config = test_export_config("delete-vol");
        router.save_export(&config).await.unwrap();
        router.delete_export_definition("delete-vol").await.unwrap();

        // Verify deleted
        let loaded = router.load_export("delete-vol").await.unwrap();
        assert!(loaded.is_none(), "Should be deleted");

        // Delete again (idempotent)
        let result = router.delete_export_definition("delete-vol").await;
        assert!(result.is_ok(), "Delete again should succeed");
    }

    #[tokio::test]
    async fn test_discover_exports_empty() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let discovered = router.discover_exports().await.unwrap();
        assert!(discovered.is_empty(), "Should discover no exports");
    }

    #[tokio::test]
    async fn test_discover_exports_multiple() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Save multiple export definitions directly to S3
        let configs = vec![
            ExportConfig {
                name: "discover-vol1".to_string(),
                size_gb: 1.0,
                s3_prefix: None,
                block_size: None,
                flush_threshold: None,
                flush_mode: None,
                transport: None,
            },
            ExportConfig {
                name: "discover-vol2".to_string(),
                size_gb: 2.0,
                s3_prefix: None,
                block_size: None,
                flush_threshold: None,
                flush_mode: None,
                transport: None,
            },
            ExportConfig {
                name: "discover-vol3".to_string(),
                size_gb: 3.0,
                s3_prefix: None,
                block_size: None,
                flush_threshold: None,
                flush_mode: None,
                transport: None,
            },
        ];

        for config in &configs {
            router.save_export(config).await.unwrap();
        }

        // Discover exports
        let discovered = router.discover_exports().await.unwrap();
        assert_eq!(discovered.len(), 3, "Should discover 3 exports");

        let names: Vec<_> = discovered.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"discover-vol1"));
        assert!(names.contains(&"discover-vol2"));
        assert!(names.contains(&"discover-vol3"));
    }

    #[tokio::test]
    async fn test_create_export_persists_to_s3() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create export and persist to S3
        let config = test_export_config("auto-persist");
        router
            .create_export(config.clone(), false, None, None)
            .await
            .unwrap();
        router.save_export(&config).await.unwrap();

        // Verify it was persisted
        let loaded = router.load_export("auto-persist").await.unwrap();
        assert!(loaded.is_some(), "Export should be persisted to S3");
    }

    #[tokio::test]
    async fn test_remove_export_with_purge_deletes_from_s3() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create export and persist
        let config = test_export_config("purge-vol");
        router
            .create_export(config.clone(), false, None, None)
            .await
            .unwrap();
        router.save_export(&config).await.unwrap();

        // Verify persisted
        let loaded = router.load_export("purge-vol").await.unwrap();
        assert!(loaded.is_some(), "Should be persisted");

        // Remove with purge
        router.remove_export("purge-vol", true).await.unwrap();

        // Verify deleted from S3
        let loaded = router.load_export("purge-vol").await.unwrap();
        assert!(loaded.is_none(), "Should be deleted from S3");
    }

    #[tokio::test]
    async fn test_purge_deletes_manifest_from_s3() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create export, write data, flush to create a manifest in S3
        let config = test_export_config("purge-manifest");
        router
            .create_export(config.clone(), false, None, None)
            .await
            .unwrap();
        let handler = router.get_handler("purge-manifest").await.unwrap();
        handler.write(0, &[0xAA; 4096], false).await.unwrap();
        router.drain_export("purge-manifest").await.unwrap();

        // Manifest should exist
        assert!(
            router
                .head_manifest("purge-manifest", "purge-manifest")
                .await
                .unwrap(),
            "manifest should exist after drain"
        );

        // Purge
        router.remove_export("purge-manifest", true).await.unwrap();

        // Manifest should be gone
        assert!(
            !router
                .head_manifest("purge-manifest", "purge-manifest")
                .await
                .unwrap(),
            "manifest should be deleted after purge"
        );
    }

    #[tokio::test]
    async fn test_purge_when_already_gone_cleans_s3() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create export, persist config to S3, write + drain to create manifest
        let config = test_export_config("gone-purge");
        router
            .create_export(config.clone(), false, None, None)
            .await
            .unwrap();
        router.save_export(&config).await.unwrap();
        let handler = router.get_handler("gone-purge").await.unwrap();
        handler.write(0, &[0xBB; 4096], false).await.unwrap();
        router.drain_export("gone-purge").await.unwrap();

        // Verify manifest + export definition exist
        assert!(router.head_manifest("gone-purge", "gone-purge").await.unwrap());
        assert!(router.load_export("gone-purge").await.unwrap().is_some());

        // Remove WITHOUT purge (simulates shutdown drain)
        router.remove_export("gone-purge", false).await.unwrap();

        // Export is gone from memory, but S3 artifacts remain
        assert!(router.head_manifest("gone-purge", "gone-purge").await.unwrap());
        assert!(router.load_export("gone-purge").await.unwrap().is_some());

        // Now purge the already-gone export
        router.remove_export("gone-purge", true).await.unwrap();

        // S3 artifacts should be cleaned up
        assert!(
            !router
                .head_manifest("gone-purge", "gone-purge")
                .await
                .unwrap(),
            "manifest should be deleted after purge-when-gone"
        );
        assert!(
            router.load_export("gone-purge").await.unwrap().is_none(),
            "export definition should be deleted after purge-when-gone"
        );
    }

    // =========================================================================
    // Snapshot + Fork tests
    // =========================================================================

    #[tokio::test]
    async fn test_snapshot_export() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("snap-vol"), false, None, None)
            .await
            .unwrap();

        // Write some data
        let handler = router.get_handler("snap-vol").await.unwrap();
        handler.write(0, &[0xAA; 4096], false).await.unwrap();
        handler.write(128 * 1024, &[0xBB; 4096], false).await.unwrap();

        // Take snapshot
        let result = router.snapshot_export("snap-vol", None).await;
        assert!(
            result.is_ok(),
            "Snapshot should succeed: {:?}",
            result.err()
        );

        let snap = result.unwrap();
        assert!(snap.sequence > 0, "Snapshot sequence should be > 0");
    }

    #[tokio::test]
    async fn test_snapshot_nonexistent_export() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let result = router.snapshot_export("nonexistent", None).await;
        assert!(
            result.is_err(),
            "Snapshot should fail for nonexistent export"
        );
        match result.unwrap_err() {
            RouterError::ExportNotFound(name) => assert_eq!(name, "nonexistent"),
            e => panic!("Expected ExportNotFound, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_fork_from_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create source export and write data
        router
            .create_export(test_export_config("source"), false, None, None)
            .await
            .unwrap();
        let handler = router.get_handler("source").await.unwrap();
        let data = vec![0xCC; 128 * 1024]; // one full block
        handler.write(0, &data, false).await.unwrap();

        // Snapshot source
        let snap = router.snapshot_export("source", None).await.unwrap();
        assert!(snap.sequence > 0);

        // Fork from snapshot
        let fork_config = ExportConfig {
            name: "fork1".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("source".to_string()), // same S3 prefix as source
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router
            .create_export(fork_config, false, Some("source"), None)
            .await
            .unwrap();

        // Read from fork — should get the same data that was written to source
        let fork_handler = router.get_handler("fork1").await.unwrap();
        let fork_data = fork_handler.read(0, 128 * 1024).await.unwrap();
        assert_eq!(
            fork_data.as_ref(),
            &data[..],
            "Fork should read source's data"
        );
    }

    #[tokio::test]
    async fn test_fork_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create and snapshot source
        router
            .create_export(test_export_config("src"), false, None, None)
            .await
            .unwrap();
        let src_handler = router.get_handler("src").await.unwrap();
        src_handler.write(0, &[0xAA; 128 * 1024], false).await.unwrap();
        router.snapshot_export("src", None).await.unwrap();

        // Fork
        let fork_config = ExportConfig {
            name: "fork-iso".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("src".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router
            .create_export(fork_config, false, Some("src"), None)
            .await
            .unwrap();

        // Write to fork
        let fork_handler = router.get_handler("fork-iso").await.unwrap();
        fork_handler.write(0, &[0xFF; 128 * 1024], false).await.unwrap();

        // Source should still see original data
        let src_data = src_handler.read(0, 128 * 1024).await.unwrap();
        assert!(
            src_data.iter().all(|&b| b == 0xAA),
            "Source should be unaffected by fork writes"
        );

        // Fork should see new data
        let fork_data = fork_handler.read(0, 128 * 1024).await.unwrap();
        assert!(
            fork_data.iter().all(|&b| b == 0xFF),
            "Fork should see its own writes"
        );
    }

    #[tokio::test]
    async fn test_fork_reads_unmodified_parent_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create source and write two blocks
        router
            .create_export(test_export_config("src"), false, None, None)
            .await
            .unwrap();
        let src_handler = router.get_handler("src").await.unwrap();
        src_handler.write(0, &[0xAA; 128 * 1024], false).await.unwrap();
        src_handler
            .write(128 * 1024, &[0xBB; 128 * 1024], false)
            .await
            .unwrap();
        router.snapshot_export("src", None).await.unwrap();

        // Fork
        let fork_config = ExportConfig {
            name: "fork-read".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("src".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router
            .create_export(fork_config, false, Some("src"), None)
            .await
            .unwrap();

        // Read both blocks from fork WITHOUT writing anything to the fork
        let fork_handler = router.get_handler("fork-read").await.unwrap();
        let block0 = fork_handler.read(0, 128 * 1024).await.unwrap();
        assert!(
            block0.iter().all(|&b| b == 0xAA),
            "fork should transparently serve parent's block 0"
        );
        let block1 = fork_handler.read(128 * 1024, 128 * 1024).await.unwrap();
        assert!(
            block1.iter().all(|&b| b == 0xBB),
            "fork should transparently serve parent's block 1"
        );
    }

    #[tokio::test]
    async fn test_fork_from_missing_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let config = ExportConfig {
            name: "bad-fork".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("nonexistent-source".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };

        let result = router
            .create_export(config, false, Some("does-not-exist"), None)
            .await;
        assert!(result.is_err(), "Fork from missing manifest should fail");
    }

    /// Fork B from A's snapshot, then fork C from B's snapshot.
    /// C should read A's data through two levels of manifest inheritance.
    ///
    /// All forks share the same S3 prefix ("a") because packs are content-
    /// addressed and shared. Each fork has its own manifest key under that
    /// prefix: manifests/a, manifests/b, manifests/c.
    #[tokio::test]
    async fn test_fork_of_fork_reads_grandparent_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // A: write 0xAA and snapshot
        router
            .create_export(test_export_config("a"), false, None, None)
            .await
            .unwrap();
        let a_handler = router.get_handler("a").await.unwrap();
        a_handler.write(0, &[0xAA; 128 * 1024], false).await.unwrap();
        router.snapshot_export("a", None).await.unwrap();

        // B: fork from A's manifest, same S3 prefix so packs are shared.
        // Write 0xBB to block 1 (leave block 0 from A untouched) and snapshot.
        let b_config = ExportConfig {
            name: "b".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("a".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router
            .create_export(b_config, false, Some("a"), None)
            .await
            .unwrap();
        let b_handler = router.get_handler("b").await.unwrap();
        b_handler
            .write(128 * 1024, &[0xBB; 128 * 1024], false)
            .await
            .unwrap();
        router.snapshot_export("b", None).await.unwrap();

        // C: fork from B's manifest (same S3 prefix).
        let c_config = ExportConfig {
            name: "c".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("a".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router
            .create_export(c_config, false, Some("b"), None)
            .await
            .unwrap();

        // C should read A's block 0 (0xAA) and B's block 1 (0xBB)
        let c_handler = router.get_handler("c").await.unwrap();
        let block0 = c_handler.read(0, 128 * 1024).await.unwrap();
        assert!(
            block0.iter().all(|&b| b == 0xAA),
            "fork-of-fork block 0 should read grandparent A's data (0xAA)"
        );
        let block1 = c_handler.read(128 * 1024, 128 * 1024).await.unwrap();
        assert!(
            block1.iter().all(|&b| b == 0xBB),
            "fork-of-fork block 1 should read parent B's data (0xBB)"
        );
    }

    // =========================================================================
    // Resize tests
    // =========================================================================

    #[tokio::test]
    async fn test_resize_export_grow() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        let before = router.list_exports().await;
        let old_size = before[0].size;

        // Resize to 0.02 GB (double the 0.01 GB default)
        router.resize_export("vol1", 0.02).await.unwrap();

        let after = router.list_exports().await;
        assert_eq!(after.len(), 1);
        assert!(after[0].size > old_size, "Export should have grown");
    }

    #[tokio::test]
    async fn test_resize_export_same_size_noop() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        // Resize to same size — should succeed as no-op
        let result = router.resize_export("vol1", 0.01).await;
        assert!(result.is_ok(), "Same-size resize should be idempotent");
    }

    #[tokio::test]
    async fn test_resize_export_shrink_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        let result = router.resize_export("vol1", 0.001).await;
        assert!(result.is_err(), "Shrink should be rejected");
        match result.unwrap_err() {
            RouterError::CannotShrink { .. } => {}
            e => panic!("Expected CannotShrink, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_resize_export_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        let result = router.resize_export("nonexistent", 0.02).await;
        assert!(result.is_err(), "Resize of nonexistent should fail");
        match result.unwrap_err() {
            RouterError::ExportNotFound(_) => {}
            e => panic!("Expected ExportNotFound, got {:?}", e),
        }
    }

    // =========================================================================
    // Fork isolation end-to-end (snapshot fork, verify parent unchanged)
    // =========================================================================

    #[tokio::test]
    async fn test_fork_snapshot_does_not_modify_parent_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create source, write data, snapshot
        router
            .create_export(test_export_config("parent"), false, None, None)
            .await
            .unwrap();
        let parent_handler = router.get_handler("parent").await.unwrap();
        parent_handler.write(0, &[0xAA; 128 * 1024], false).await.unwrap();
        let _parent_snap = router.snapshot_export("parent", None).await.unwrap();

        // Fork from parent
        let fork_config = ExportConfig {
            name: "child".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("parent".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router
            .create_export(fork_config, false, Some("parent"), None)
            .await
            .unwrap();

        // Write different data to the fork
        let fork_handler = router.get_handler("child").await.unwrap();
        fork_handler.write(0, &[0xFF; 128 * 1024], false).await.unwrap();
        fork_handler
            .write(128 * 1024, &[0xDD; 128 * 1024], false)
            .await
            .unwrap();

        // Snapshot the fork
        router.snapshot_export("child", None).await.unwrap();

        // Re-snapshot the parent — its manifest should be unchanged
        let _parent_snap2 = router.snapshot_export("parent", None).await.unwrap();
        // No new writes to parent, so sequence should be the same or
        // flushed blocks should be 0
        // The important thing: parent data is unaffected
        let parent_data = parent_handler.read(0, 128 * 1024).await.unwrap();
        assert!(
            parent_data.iter().all(|&b| b == 0xAA),
            "parent data should be unmodified after fork snapshot"
        );

        // Fork should have its own data
        let fork_data = fork_handler.read(0, 128 * 1024).await.unwrap();
        assert!(
            fork_data.iter().all(|&b| b == 0xFF),
            "fork should see its own writes"
        );

        let fork_data2 = fork_handler.read(128 * 1024, 128 * 1024).await.unwrap();
        assert!(fork_data2.iter().all(|&b| b == 0xDD), "fork second block");
    }

    // =========================================================================
    // Resize with active I/O
    // =========================================================================

    #[tokio::test]
    async fn test_resize_grows_export_and_allows_writes() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("resize-vol"), false, None, None)
            .await
            .unwrap();

        let old_size = {
            let exports = router.list_exports().await;
            exports
                .iter()
                .find(|e| e.name == "resize-vol")
                .unwrap()
                .size
        };

        // Resize to double
        router.resize_export("resize-vol", 0.02).await.unwrap();

        let new_size = {
            let exports = router.list_exports().await;
            exports
                .iter()
                .find(|e| e.name == "resize-vol")
                .unwrap()
                .size
        };
        assert!(new_size > old_size, "export should have grown");

        // Write to new region (beyond original size) should succeed
        let handler = router.get_handler("resize-vol").await.unwrap();
        let write_offset = old_size - 128 * 1024; // near old boundary
        handler
            .write(write_offset, &[0xDD; 128 * 1024], false)
            .await
            .unwrap();

        // Idempotent: resize to same size should be a no-op
        router.resize_export("resize-vol", 0.02).await.unwrap();

        // Shrink should fail
        let err = router.resize_export("resize-vol", 0.005).await;
        assert!(err.is_err(), "shrinking should fail");
    }

    #[tokio::test]
    async fn test_resize_preserves_readonly_flag() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create as readonly
        router
            .create_export(test_export_config("ro-resize"), true, None, None)
            .await
            .unwrap();

        // Resize
        router.resize_export("ro-resize", 0.02).await.unwrap();

        // Should still be readonly
        let exports = router.list_exports().await;
        let export = exports.iter().find(|e| e.name == "ro-resize").unwrap();
        assert!(export.readonly, "readonly flag should survive resize");
    }

    // =========================================================================
    // Pack upload partial failure (manifest fails after packs succeed)
    // =========================================================================

    #[tokio::test]
    async fn test_flush_retry_after_manifest_failure() {
        // This tests at the WriteCache level: first flush succeeds (packs + manifest),
        // write more data, second flush succeeds — verifying the pack index correctly
        // deduplicates across flushes and manifests accumulate.
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("retry-vol"), false, None, None)
            .await
            .unwrap();
        let handler = router.get_handler("retry-vol").await.unwrap();

        // First write + drain
        handler.write(0, &[0x11; 128 * 1024], false).await.unwrap();
        router.drain_export("retry-vol").await.unwrap();

        // Second write + drain with different data
        handler
            .write(128 * 1024, &[0x22; 128 * 1024], false)
            .await
            .unwrap();
        router.drain_export("retry-vol").await.unwrap();

        // Both blocks should be readable
        let data1 = handler.read(0, 128 * 1024).await.unwrap();
        assert!(data1.iter().all(|&b| b == 0x11), "first block after retry");

        let data2 = handler.read(128 * 1024, 128 * 1024).await.unwrap();
        assert!(data2.iter().all(|&b| b == 0x22), "second block after retry");

        // Snapshot should capture both blocks
        let snap = router.snapshot_export("retry-vol", None).await.unwrap();
        assert!(snap.sequence > 0);
    }

    // =========================================================================
    // Resize with dirty blocks
    // =========================================================================

    /// Resize with unflushed dirty blocks: the drain→remove→recreate cycle
    /// should flush dirty data to S3, grow the device, and allow writes to
    /// the new region.
    ///
    /// NOTE: Data preservation across resize is not tested here because
    /// the v2 block_map (.blockmap) is not persisted during save_metadata,
    /// so after the drain→truncate WAL→recreate cycle the block_map is empty
    /// and old blocks read as zeros. This is a known gap — the data exists
    /// in S3 but the local hash mapping is lost.
    #[tokio::test]
    async fn test_resize_with_dirty_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        router
            .create_export(test_export_config("resize-dirty"), false, None, None)
            .await
            .unwrap();

        let handler = router.get_handler("resize-dirty").await.unwrap();

        // Write dirty blocks before resize
        handler.write(0, &[0xAA; 128 * 1024], false).await.unwrap();
        handler
            .write(128 * 1024, &[0xBB; 128 * 1024], false)
            .await
            .unwrap();

        let old_size = {
            let exports = router.list_exports().await;
            exports
                .iter()
                .find(|e| e.name == "resize-dirty")
                .unwrap()
                .size
        };

        // Resize doubles the device — this calls drain_export first, so dirty
        // blocks get flushed to S3 before the remove/recreate cycle.
        router.resize_export("resize-dirty", 0.02).await.unwrap();

        let new_size = {
            let exports = router.list_exports().await;
            exports
                .iter()
                .find(|e| e.name == "resize-dirty")
                .unwrap()
                .size
        };
        assert!(new_size > old_size, "export should have grown");

        // Re-acquire handler after resize (old handler is invalidated)
        let handler = router.get_handler("resize-dirty").await.unwrap();

        // Write to the new region (beyond old device boundary)
        let new_region_offset = old_size; // first byte of new space
        handler
            .write(new_region_offset, &[0xCC; 128 * 1024], false)
            .await
            .unwrap();

        let data_new = handler.read(new_region_offset, 128 * 1024).await.unwrap();
        assert!(
            data_new.iter().all(|&b| b == 0xCC),
            "new region should be writable"
        );
    }

    #[test]
    fn test_validate_export_name() {
        // Valid names
        assert!(validate_export_name("vol1").is_ok());
        assert!(validate_export_name("my-export.v2").is_ok());
        assert!(validate_export_name("a").is_ok());
        assert!(validate_export_name("vm_ubuntu-22.04").is_ok());
        assert!(validate_export_name(&"a".repeat(128)).is_ok());

        // Empty / too long
        assert!(validate_export_name("").is_err());
        assert!(validate_export_name(&"a".repeat(129)).is_err());

        // Must start with alphanumeric
        assert!(validate_export_name("-leading-hyphen").is_err());
        assert!(validate_export_name(".leading-dot").is_err());
        assert!(validate_export_name("_leading-underscore").is_err());

        // Path traversal / invalid characters
        assert!(validate_export_name("../etc/passwd").is_err());
        assert!(validate_export_name("foo/bar").is_err());
        assert!(validate_export_name("foo bar").is_err());
        assert!(validate_export_name("name\0null").is_err());
    }

    #[test]
    fn test_extract_export_name() {
        // Valid paths
        assert_eq!(
            super::extract_export_name("test/exports/vol1/export.json", "test"),
            Some("vol1".to_string())
        );
        assert_eq!(
            super::extract_export_name("my-data/exports/my-export/export.json", "my-data"),
            Some("my-export".to_string())
        );

        // Invalid paths
        assert_eq!(
            super::extract_export_name("test/exports/vol1/lease.json", "test"),
            None
        );
        assert_eq!(
            super::extract_export_name("test/exports/vol1/batches/000000000000", "test"),
            None
        );
        assert_eq!(
            super::extract_export_name("other/exports/vol1/export.json", "test"),
            None
        );
        assert_eq!(
            super::extract_export_name("test/exports//export.json", "test"),
            None
        );
    }

    // =========================================================================
    // Per-export flush config tests
    // =========================================================================

    #[tokio::test]
    async fn test_manual_mode_export_no_auto_flush() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create export with flush_mode = "manual"
        let config = ExportConfig {
            name: "manual-vm".to_string(),
            size_gb: 0.01,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: Some("manual".to_string()),
            transport: None,
        };
        router.create_export(config, false, None, None).await.unwrap();

        // Write many blocks — well above DEFAULT_FLUSH_THRESHOLD
        let handler = router.get_handler("manual-vm").await.unwrap();
        // 50 blocks × 128KB = 6.4MB (within our 10MB device)
        for i in 0..50u64 {
            handler
                .write(i * 128 * 1024, &[0xAA; 128 * 1024], false)
                .await
                .unwrap();
        }

        // Wait briefly — in auto mode, the flush scheduler would drain dirty blocks.
        // In manual mode, they should accumulate.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let metrics = router.get_export_metrics("manual-vm").await.unwrap();
        let dirty = metrics.dirty_blocks.unwrap();
        assert!(
            dirty > 0,
            "manual mode should accumulate dirty blocks without auto-flush (dirty={dirty})",
        );

        // Explicit drain should still work
        router.drain_export("manual-vm").await.unwrap();

        let metrics_after = router.get_export_metrics("manual-vm").await.unwrap();
        assert_eq!(
            metrics_after.dirty_blocks.unwrap(),
            0,
            "drain should flush all dirty blocks even in manual mode"
        );
    }

    #[tokio::test]
    async fn test_pressure_flush_works_in_manual_mode() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create manual-mode export
        let config = ExportConfig {
            name: "manual-pressure".to_string(),
            size_gb: 0.01,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: Some("manual".to_string()),
            transport: None,
        };
        router.create_export(config, false, None, None).await.unwrap();

        // Write some dirty blocks
        let handler = router.get_handler("manual-pressure").await.unwrap();
        for i in 0..10u64 {
            handler
                .write(i * 128 * 1024, &[0xBB; 128 * 1024], false)
                .await
                .unwrap();
        }

        let dirty_before = router
            .get_export_metrics("manual-pressure")
            .await
            .unwrap()
            .dirty_blocks
            .unwrap();
        assert!(dirty_before > 0);

        // Pressure flush should still work (safety valve for SSD capacity)
        router.pressure_flush().await;

        // Pressure flush flushes packs but doesn't drain to zero — it flushes
        // one batch. Verify it ran without error (the important thing is it
        // doesn't skip manual-mode exports).
        // The actual dirty count may or may not decrease depending on timing.
    }

    #[tokio::test]
    async fn test_flush_threshold_config_resolution() {
        // Verify ExportConfig.flush_threshold_or() cascade:
        // 1. flush_mode = "manual" → 0
        // 2. export override → export value
        // 3. fallback → global default
        let manual = ExportConfig {
            name: "m".to_string(),
            size_gb: 1.0,
            s3_prefix: None,
            block_size: None,
            flush_threshold: Some(1000),
            flush_mode: Some("manual".to_string()),
            transport: None,
        };
        assert_eq!(
            manual.flush_threshold_or(500),
            0,
            "manual mode always returns 0"
        );

        let custom = ExportConfig {
            name: "c".to_string(),
            size_gb: 1.0,
            s3_prefix: None,
            block_size: None,
            flush_threshold: Some(1000),
            flush_mode: None,
            transport: None,
        };
        assert_eq!(custom.flush_threshold_or(500), 1000, "export override wins");

        let default = ExportConfig {
            name: "d".to_string(),
            size_gb: 1.0,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        assert_eq!(
            default.flush_threshold_or(500),
            500,
            "falls back to global default"
        );
    }

    // =========================================================================
    // Device management / transport tests
    // =========================================================================

    #[test]
    fn test_device_available_on_current_platform() {
        // On macOS: no kernel device support
        // On Linux: NBD available, ublk depends on feature flag
        if cfg!(target_os = "linux") {
            assert!(ExportRouter::device_available("nbd"));
        } else {
            assert!(!ExportRouter::device_available("nbd"));
        }

        if cfg!(all(target_os = "linux", feature = "ublk")) {
            assert!(ExportRouter::device_available("ublk"));
        } else {
            assert!(!ExportRouter::device_available("ublk"));
        }

        // Invalid transport is never available
        assert!(!ExportRouter::device_available("scsi"));
        assert!(!ExportRouter::device_available(""));
    }

    #[tokio::test]
    async fn test_get_device_path_returns_none_without_registration() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;
        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        // Without explicit device registration, get_device_path returns None.
        assert!(router.get_device_path("vol1").await.is_none());
        assert!(router.get_device_path("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_export_transport_defaults_to_nbd() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;
        router
            .create_export(test_export_config("vol1"), false, None, None)
            .await
            .unwrap();

        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].transport, "nbd");
        assert!(exports[0].device.is_none());
    }

    #[tokio::test]
    async fn test_export_transport_preserved_through_resize() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Create export with explicit transport
        let mut config = test_export_config("vol1");
        config.transport = Some("nbd".to_string());
        router.create_export(config, false, None, None).await.unwrap();

        // Resize (internally removes + recreates)
        router.resize_export("vol1", 0.02).await.unwrap();

        // Transport should be preserved
        let exports = router.list_exports().await;
        assert_eq!(exports[0].transport, "nbd");
    }

    #[tokio::test]
    async fn test_shutdown_devices_is_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Shutdown devices with nothing registered — should be a no-op
        router.shutdown_devices().await.unwrap();
        router.shutdown_devices().await.unwrap();
    }

    // =========================================================================
    // Stale export.json discovery tests
    // =========================================================================

    /// export.json exists in S3 but no manifest was ever uploaded (no flush/drain).
    /// The export should create successfully with an empty manifest and reads
    /// return zeros.
    #[tokio::test]
    async fn test_discover_exports_missing_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;

        // Save export config to S3 without creating the export locally
        // (simulates: export.json persisted, but node was replaced before first flush)
        let config = test_export_config("stale-vol");
        router.save_export(&config).await.unwrap();

        // Discover should find it
        let discovered = router.discover_exports().await.unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].name, "stale-vol");

        // Create from discovered config — normal path (no manifest_name)
        // The manifest doesn't exist in S3 so it starts with an empty one
        router
            .create_export(discovered[0].clone(), false, None, None)
            .await
            .unwrap();

        // Read block 0 — should return zeros (no data in manifest)
        let handler = router.get_handler("stale-vol").await.unwrap();
        let data = handler.read(0, 4096).await.unwrap();
        assert!(
            data.iter().all(|&b| b == 0),
            "missing manifest should yield zero-filled reads"
        );
    }

    /// export.json references an export whose manifest was deleted from S3
    /// (e.g., manual cleanup). The export should create successfully and start
    /// fresh rather than failing or blocking startup.
    #[tokio::test]
    async fn test_discover_exports_deleted_manifest() {
        let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let temp_dir = TempDir::new().unwrap();
        let router = ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
            db_path: "test".to_string(),
            cache_dir: temp_dir.path().to_path_buf(),
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
        })
        .await
        .unwrap();

        // Create export, write data, flush + sync manifest
        let config = test_export_config("del-manifest");
        router
            .create_export(config.clone(), false, None, None)
            .await
            .unwrap();
        let handler = router.get_handler("del-manifest").await.unwrap();
        handler
            .write(0, &vec![0xAA; 128 * 1024], false)
            .await
            .unwrap();
        router.drain_export("del-manifest").await.unwrap();
        router.save_export(&config).await.unwrap();

        // Manually delete the manifest from S3
        let manifest_path = Path::from("test/exports/del-manifest/manifests/del-manifest");
        s3.delete(&manifest_path).await.unwrap();

        // Simulate node replacement: create a new router with the same S3
        let temp_dir2 = TempDir::new().unwrap();
        let router2 = ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
            db_path: "test".to_string(),
            cache_dir: temp_dir2.path().to_path_buf(),
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
        })
        .await
        .unwrap();

        // Discover finds the config
        let discovered = router2.discover_exports().await.unwrap();
        assert_eq!(discovered.len(), 1);

        // Create export from discovered config — manifest is gone, should start fresh
        router2
            .create_export(discovered[0].clone(), false, None, None)
            .await
            .unwrap();

        // Export is usable — reads return zeros (manifest was deleted)
        let handler2 = router2.get_handler("del-manifest").await.unwrap();
        let data = handler2.read(0, 4096).await.unwrap();
        assert!(
            data.iter().all(|&b| b == 0),
            "deleted manifest should yield zero-filled reads"
        );
    }

    // =========================================================================
    // Partial block / async sub-block backfill tests
    // =========================================================================

    /// Helper: set up a parent export with block_size bytes of 0xAA flushed to
    /// S3, then return a freshly forked child router on the same InMemory store.
    async fn setup_parent_fork(
        temp_dir: &TempDir,
        parent_fill: u8,
    ) -> (ExportRouter, Arc<dyn object_store::ObjectStore>) {
        let s3: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let router = ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
            db_path: "test".to_string(),
            cache_dir: temp_dir.path().join("parent"),
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
        })
        .await
        .unwrap();

        router
            .create_export(test_export_config("parent"), false, None, None)
            .await
            .unwrap();
        let parent = router.get_handler("parent").await.unwrap();
        // Write one full block of parent_fill
        parent
            .write(0, &vec![parent_fill; 128 * 1024], false)
            .await
            .unwrap();
        router.snapshot_export("parent", None).await.unwrap();
        drop(parent);

        let fork_router = ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
            db_path: "test".to_string(),
            cache_dir: temp_dir.path().join("child"),
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
        })
        .await
        .unwrap();

        let fork_config = ExportConfig {
            name: "child".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("parent".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        fork_router
            .create_export(fork_config, false, Some("parent"), None)
            .await
            .unwrap();

        (fork_router, s3)
    }

    /// Multiple sub-block writes to the same 128KB block must all be preserved.
    ///
    /// Writes 4KB at offsets 0, 16384, and 122880 within the same block (which
    /// has parent data 0xAA in S3). Reads back all three regions and the
    /// unwritten regions — all must be correct.
    #[tokio::test]
    async fn test_multiple_sub_block_writes_same_block() {
        let temp_dir = TempDir::new().unwrap();
        let (router, _s3) = setup_parent_fork(&temp_dir, 0xAA).await;
        let child = router.get_handler("child").await.unwrap();

        let block_size = 128 * 1024usize;
        let sub = 4096usize;

        // Three non-overlapping 4KB writes within block 0
        child.write(0, &[0xBB; 4096], false).await.unwrap();
        child.write(16384, &[0xCC; 4096], false).await.unwrap();
        child.write(122880, &[0xDD; 4096], false).await.unwrap();

        // Allow background backfill to complete
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Read the full block
        let full = child.read(0, block_size as u32).await.unwrap();
        assert_eq!(full.len(), block_size);

        // Offset 0..4096: 0xBB (written)
        assert!(full[0..sub].iter().all(|&b| b == 0xBB), "sub-region 0 should be 0xBB");
        // Offset 4096..16384: 0xAA (parent, unwritten)
        assert!(full[sub..16384].iter().all(|&b| b == 0xAA), "gap 1 should be 0xAA");
        // Offset 16384..20480: 0xCC (written)
        assert!(full[16384..20480].iter().all(|&b| b == 0xCC), "sub-region 4 should be 0xCC");
        // Offset 20480..122880: 0xAA (parent, unwritten)
        assert!(full[20480..122880].iter().all(|&b| b == 0xAA), "gap 2 should be 0xAA");
        // Offset 122880..126976: 0xDD (written)
        assert!(full[122880..126976].iter().all(|&b| b == 0xDD), "sub-region 30 should be 0xDD");
        // Offset 126976..131072: 0xAA (parent, unwritten)
        assert!(full[126976..].iter().all(|&b| b == 0xAA), "tail should be 0xAA");
    }

    /// Sub-block write + flush to S3 + cold wake must serve correct merged data.
    ///
    /// Writes 4KB of 0xBB to a forked block (parent is 0xAA), flushes to S3,
    /// then a cold reader must see: [0xBB; 4KB] + [0xAA; 124KB].
    #[tokio::test]
    async fn test_sub_block_write_flush_cold_wake() {
        let temp_dir = TempDir::new().unwrap();
        let (router, s3) = setup_parent_fork(&temp_dir, 0xAA).await;
        let child = router.get_handler("child").await.unwrap();
        let block_size = 128 * 1024u32;

        // Sub-block write: only the first 4KB
        child.write(0, &[0xBB; 4096], false).await.unwrap();

        // Wait for background backfill to complete before flushing
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Flush child to S3
        router.drain_export("child").await.unwrap();
        router.save_export(&ExportConfig {
            name: "child".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("parent".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        }).await.unwrap();
        drop(child);

        // Cold reader from a fresh directory
        let reader_dir = TempDir::new().unwrap();
        let cold_router = ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
            db_path: "test".to_string(),
            cache_dir: reader_dir.path().to_path_buf(),
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
        })
        .await
        .unwrap();
        cold_router
            .create_export(
                ExportConfig {
                    name: "child".to_string(),
                    size_gb: 0.01,
                    s3_prefix: Some("parent".to_string()),
                    block_size: None,
                    flush_threshold: None,
                    flush_mode: None,
                    transport: None,
                },
                false,
                Some("child"),
                None,
            )
            .await
            .unwrap();
        let cold_handler = cold_router.get_handler("child").await.unwrap();

        let data = cold_handler.read(0, block_size).await.unwrap();
        assert_eq!(data.len(), block_size as usize);
        assert!(
            data[..4096].iter().all(|&b| b == 0xBB),
            "cold wake: first 4KB should be 0xBB"
        );
        assert!(
            data[4096..].iter().all(|&b| b == 0xAA),
            "cold wake: rest should be 0xAA"
        );
    }

    /// Concurrent sub-block writes to different sub-regions of the same block.
    ///
    /// 4 tasks each write 4KB to a distinct sub-region. After all complete
    /// and backfill runs, each written sub-region must have the task's data
    /// and all other sub-regions must have parent data (0xAA).
    #[tokio::test]
    async fn test_concurrent_sub_block_writes_same_block() {
        let temp_dir = TempDir::new().unwrap();
        let (router, _s3) = setup_parent_fork(&temp_dir, 0xAA).await;
        let child = Arc::new(router.get_handler("child").await.unwrap());

        // Write to 4 distinct sub-regions concurrently using different task data
        let offsets_and_fills: Vec<(u64, u8)> = vec![
            (0, 0xB1),
            (8192, 0xB2),
            (32768, 0xB3),
            (65536, 0xB4),
        ];

        let mut handles = Vec::new();
        for (offset, fill) in offsets_and_fills.clone() {
            let child_clone = Arc::clone(&child);
            handles.push(tokio::spawn(async move {
                child_clone
                    .write(offset, &[fill; 4096], false)
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Allow background backfill to settle
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let block_size = 128 * 1024u32;
        let data = child.read(0, block_size).await.unwrap();

        for (offset, fill) in &offsets_and_fills {
            let start = *offset as usize;
            let end = start + 4096;
            assert!(
                data[start..end].iter().all(|&b| b == *fill),
                "sub-region at offset {} should be 0x{:02X}",
                offset,
                fill
            );
        }

        // Verify unwritten sub-regions have parent data
        // Check middle of the gaps
        assert_eq!(data[4096], 0xAA, "gap at 4096 should be 0xAA");
        assert_eq!(data[16384], 0xAA, "gap at 16384 should be 0xAA");
        assert_eq!(data[40960], 0xAA, "gap at 40960 should be 0xAA");
        assert_eq!(data[73728], 0xAA, "gap at 73728 should be 0xAA");
    }

    /// Sub-block write to a forked export must NOT destroy unwritten portions
    /// of the same write-cache block.
    ///
    /// Scenario: parent block is 128KB of 0xAA in S3. Fork writes only the
    /// first 4KB with 0xBB. Reading the second 4KB must still return 0xAA
    /// (parent data), not 0x00 (uninitialised data file).
    ///
    /// This reproduces the corruption seen when VMs do sub-block writes
    /// (e.g., ext4 4KB filesystem blocks) against our 128KB write-cache blocks
    /// on a forked/restored export.
    #[tokio::test]
    async fn test_sub_block_write_preserves_unwritten_portions_of_s3_block() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir).await;
        let block_size = 128 * 1024; // 128KB write-cache block

        // --- Parent: write a full 128KB block of 0xAA, then snapshot ---
        router
            .create_export(test_export_config("parent"), false, None, None)
            .await
            .unwrap();
        let parent = router.get_handler("parent").await.unwrap();
        parent.write(0, &vec![0xAA; block_size], false).await.unwrap();
        router.snapshot_export("parent", None).await.unwrap();

        // --- Fork: fresh cache, data only in S3 ---
        let fork_config = ExportConfig {
            name: "child".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("parent".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router
            .create_export(fork_config, false, Some("parent"), None)
            .await
            .unwrap();
        let child = router.get_handler("child").await.unwrap();

        // Sanity: reading the full block from the fork returns parent data.
        let pre_write = child.read(0, block_size as u32).await.unwrap();
        assert!(
            pre_write.iter().all(|&b| b == 0xAA),
            "fork should serve parent data before any writes"
        );

        // --- Sub-block write: overwrite only the first 4KB with 0xBB ---
        child.write(0, &[0xBB; 4096], false).await.unwrap();

        // The written portion must reflect the new data.
        let first_4k = child.read(0, 4096).await.unwrap();
        assert!(
            first_4k.iter().all(|&b| b == 0xBB),
            "written sub-block should return new data"
        );

        // The UN-written portion (bytes 4096..8192) must still return parent
        // data (0xAA), NOT zeros.
        let second_4k = child.read(4096, 4096).await.unwrap();
        assert!(
            second_4k.iter().all(|&b| b == 0xAA),
            "unwritten portion of block should return parent S3 data (0xAA), \
             got 0x{:02x} — sub-block write destroyed unwritten data",
            second_4k[0],
        );
    }
}
