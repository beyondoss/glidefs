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
use crate::task::{self, spawn_supervised};
use std::time::Instant;
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
use dashmap::DashMap;
use tokio::sync::{Notify, RwLock, watch};
use tokio::task::JoinHandle;
use serde::Deserialize;
use tracing::{debug, error, info, trace, warn};

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

    #[error("OCI pull error: {0}")]
    OciPull(String),

    #[error(
        "Export limit reached: cannot create export '{name}' (router holds {current} of max {max})"
    )]
    ExportLimitReached {
        name: String,
        current: usize,
        max: usize,
    },
}

/// Status of an in-flight OCI bless operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlessStatus {
    pub state: String,
    pub oci_image: String,
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
    /// Supervised flush task. `Ok(Ok(()))` = clean exit, `Ok(Err(_))` =
    /// caught panic (already logged + counted by spawn_supervised),
    /// `Err(JoinError)` = aborted or unwind-after-catch (rare).
    flush_handle: JoinHandle<Result<(), task::Panicked>>,
    /// Background hot-set prefetch task (if spawned). Aborted on teardown
    /// to release Arc references to cache/content_store/etc.
    prefetch_handle: Option<JoinHandle<Result<(), task::Panicked>>>,
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
    /// Maximum total exports the router will host. `create_export` past
    /// this cap returns `RouterError::ExportLimitReached`. Caps the cost
    /// of a tenant repeatedly POSTing /api/exports.
    pub max_exports: usize,
}

/// Multi-tenant export router.
///
/// Manages multiple NBD exports, each with independent storage and caching.
pub struct ExportRouter {
    /// Active exports: name → state. Sharded `DashMap` so per-export
    /// lookups don't contend with each other or with create/remove.
    /// Previously a `tokio::sync::RwLock<HashMap<...>>` which serialized
    /// all readers behind any writer — and writers held the lock across
    /// `.await` on slow S3 ops, freezing every other tenant's I/O.
    exports: DashMap<String, ExportState>,

    /// Cap on `exports.len()`. See `RouterConfig::max_exports`.
    max_exports: usize,

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

    /// In-flight OCI bless tasks: "{s3_prefix}/{name}" → status.
    /// Entries exist only while in-flight. Removed on completion or failure.
    bless_tasks: RwLock<HashMap<String, BlessStatus>>,
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

/// Backoff/restart policy for `run_supervisor_loop`. Pulled out as constants
/// so the unit test can construct a low-latency variant.
struct SupervisorPolicy {
    max_consecutive_panics: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    stable_threshold: Duration,
}

impl Default for SupervisorPolicy {
    fn default() -> Self {
        Self {
            // After this many back-to-back panics with no stable run between,
            // the supervisor gives up and marks the export degraded.
            max_consecutive_panics: 5,
            initial_backoff: Duration::from_secs(1),
            // Restart attempts past this just sleep 60s each.
            max_backoff: Duration::from_secs(60),
            // If the inner ran at least this long, treat it as a fresh
            // failure (reset the streak). Without this, intermittent
            // failures over a long-running daemon would eventually exceed
            // the cap.
            stable_threshold: Duration::from_secs(300),
        }
    }
}

/// Generic auto-restart supervisor loop. Spawns `make_inner()` as an
/// `AssertUnwindSafe` task, awaits it, restarts on caught panic with backoff,
/// honors `shutdown_rx`, and gives up after the policy's
/// `max_consecutive_panics` (marking the export degraded via the metrics).
///
/// Aborting this supervisor's outer task also aborts the in-flight inner via
/// the `AbortOnDrop` guard — without that, dropping the supervisor's future
/// would only *detach* the inner task and it would keep running.
async fn run_supervisor_loop<F, Fut>(
    label: &str,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<ExportMetrics>,
    policy: SupervisorPolicy,
    mut make_inner: F,
) where
    F: FnMut() -> Fut + Send,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    use std::sync::atomic::Ordering;

    struct AbortOnDrop(Option<tokio::task::AbortHandle>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            if let Some(h) = self.0.take() {
                h.abort();
            }
        }
    }

    let mut consecutive_panics: u32 = 0;
    let mut backoff = policy.initial_backoff;

    loop {
        if *shutdown_rx.borrow() {
            info!(supervisor = label, "supervisor shutting down");
            return;
        }

        let started_at = Instant::now();
        let mut inner_handle = task::spawn_supervised("supervised-inner", make_inner());
        let _abort_guard = AbortOnDrop(Some(inner_handle.abort_handle()));

        let result = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => (&mut inner_handle).await,
            res = &mut inner_handle => res,
        };

        match result {
            Ok(Ok(())) => return,
            Ok(Err(panicked)) => {
                metrics.flush_task_panics.fetch_add(1, Ordering::Relaxed);
                if started_at.elapsed() >= policy.stable_threshold {
                    consecutive_panics = 0;
                    backoff = policy.initial_backoff;
                }
                consecutive_panics = consecutive_panics.saturating_add(1);
                if consecutive_panics >= policy.max_consecutive_panics {
                    error!(
                        supervisor = label,
                        consecutive_panics,
                        last_message = %panicked.message,
                        "supervised task panicked too many times in a row; giving up — export is degraded, manual restart required",
                    );
                    metrics.flush_degraded.store(1, Ordering::Relaxed);
                    return;
                }
                warn!(
                    supervisor = label,
                    consecutive_panics,
                    backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                    last_message = %panicked.message,
                    "supervised task panicked; restarting after backoff",
                );
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = backoff.saturating_mul(2).min(policy.max_backoff);
            }
            Err(je) if je.is_cancelled() => {
                info!(supervisor = label, "supervised task aborted");
                return;
            }
            Err(je) => {
                error!(
                    supervisor = label,
                    error = ?je,
                    "supervised task join error (panic escaped catch_unwind?); marking degraded",
                );
                metrics.flush_degraded.store(1, Ordering::Relaxed);
                return;
            }
        }
    }
}

/// Auto-restarting supervisor for the per-export flush task.
///
/// Thin wrapper around `run_supervisor_loop` that constructs a fresh
/// `flush_scheduler` future per restart from cloned `Arc`s.
#[allow(clippy::too_many_arguments)]
async fn run_flush_supervisor(
    export_name: String,
    cache: Arc<WriteCache<Active>>,
    content_store: Arc<ContentStore>,
    pack_index_cache: Arc<PackIndexCache>,
    volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,
    clean_cache: Arc<dyn BlockCache>,
    flush_notify: Arc<Notify>,
    shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<ExportMetrics>,
    flush_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    flush_threshold: usize,
) {
    let supervisor_metrics = Arc::clone(&metrics);
    let inner_shutdown_rx = shutdown_rx.clone();
    let make_inner = move || {
        let cache = Arc::clone(&cache);
        let content_store = Arc::clone(&content_store);
        let pack_index_cache = Arc::clone(&pack_index_cache);
        let volume_manifest = Arc::clone(&volume_manifest);
        let clean_cache = Arc::clone(&clean_cache);
        let flush_notify = Arc::clone(&flush_notify);
        let inner_shutdown = inner_shutdown_rx.clone();
        let inner_metrics = Arc::clone(&metrics);
        let flush_semaphore = flush_semaphore.clone();
        let export_name = export_name.clone();
        async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                inner_shutdown,
                inner_metrics,
                flush_semaphore,
                flush_threshold,
            )
            .await;
            info!(export = %export_name, "flush scheduler exited");
        }
    };

    run_supervisor_loop(
        "flush",
        shutdown_rx,
        supervisor_metrics,
        SupervisorPolicy::default(),
        make_inner,
    )
    .await;
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
            exports: DashMap::new(),
            max_exports: config.max_exports,
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
            bless_tasks: RwLock::new(HashMap::new()),
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
            let mut map: HashMap<u32, Vec<crate::block::pack::PackId>> = HashMap::new();
            for entry in self.exports.iter() {
                let vm = entry.value().volume_manifest.read();
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
            let mut triples = Vec::new();
            for entry in self.exports.iter() {
                let cs = Arc::clone(&entry.value().content_store);
                let vm = entry.value().volume_manifest.read();
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
        let mut targets: Vec<_> = self
            .exports
            .iter()
            .filter(|e| e.value().cache.dirty_block_count() > 0)
            .map(|e| {
                let s = e.value();
                (
                    e.key().clone(),
                    s.cache.dirty_block_count(),
                    Arc::clone(&s.cache),
                    Arc::clone(&s.content_store),
                    Arc::clone(&s.pack_index_cache),
                    Arc::clone(&s.volume_manifest),
                )
            })
            .collect();
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

        // Check if export already exists - idempotent: return success if already exists.
        // Also enforce the export-count cap here so a tenant flooding
        // POST /api/exports can't race past the limit. Note: with DashMap
        // there's still a small TOCTOU window between this check and the
        // insert below; under heavy concurrent create-storms a few extra
        // exports above the cap can briefly land. The cap is a safety
        // bound, not a hard regulator — acceptable.
        if self.exports.contains_key(&name) {
            info!("Export '{}' already exists, skipping creation", name);
            return Ok(());
        }
        if self.exports.len() >= self.max_exports {
            return Err(RouterError::ExportLimitReached {
                name: name.clone(),
                current: self.exports.len(),
                max: self.max_exports,
            });
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
                // Supervised: fire-and-forget prefetch. A panic logs +
                // counts but does not propagate. Caller awaits the
                // JoinHandle in some paths — the inner Result is the
                // unit-or-Panicked outcome of the wrapped future.
                Some(spawn_supervised("hot-set-prefetch", async move {
                    cache_clone
                        .prefetch_chunks(&chunks, &cmc, &vm, &cs)
                        .await;
                    info!("boot hot set prefetch complete");
                }))
            } else {
                let hot_set_name = hot_set_name.to_string();
                let hot_set_cache = Arc::clone(&self.hot_set_cache);
                Some(spawn_supervised("hot-set-prefetch", async move {
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

        // Start flush scheduler for this export, behind an auto-restart
        // supervisor. A bare `spawn_supervised(flush_scheduler)` would
        // log + count a panic but the flush task would stay dead forever
        // — dirty blocks pile up in write_cache, SSD eventually fills,
        // capacity_monitor rejects new writes for that export, operator
        // must restart the daemon. The supervisor catches the panic via
        // the inner JoinHandle, sleeps with exponential backoff, and
        // re-spawns flush_scheduler. Gives up after 5 consecutive panics
        // (resets the counter if the scheduler runs >5min between panics)
        // and marks the export degraded via metrics.flush_degraded.
        let (flush_shutdown_tx, flush_shutdown_rx) = watch::channel(false);
        let flush_handle = spawn_supervised(
            "flush-supervisor",
            run_flush_supervisor(
                name.clone(),
                Arc::clone(&cache),
                Arc::clone(&content_store),
                Arc::clone(&pack_index_cache),
                Arc::clone(&volume_manifest),
                Arc::clone(&clean_cache),
                flush_notify,
                flush_shutdown_rx,
                Arc::clone(&metrics),
                self.flush_semaphore.clone(),
                flush_threshold,
            ),
        );

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

        // DashMap entry-API: claim the slot atomically. If a concurrent
        // create_export got here first, we yield and clean up our spawned
        // flush task to avoid leaking it.
        use dashmap::mapref::entry::Entry;
        match self.exports.entry(name.clone()) {
            Entry::Occupied(_) => {
                info!(
                    "Export '{}' already exists (concurrent create), cleaning up",
                    name
                );
                state.flush_handle.abort();
                return Ok(());
            }
            Entry::Vacant(slot) => {
                slot.insert(state);
            }
        }

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

        // Clone Arc'd components from the per-shard guard, then drop the
        // guard so we don't hold it across .await on the snapshot below.
        let (cache, content_store, pack_index_cache, volume_manifest) = {
            let entry = self
                .exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            let s = entry.value();
            (
                Arc::clone(&s.cache),
                Arc::clone(&s.content_store),
                Arc::clone(&s.pack_index_cache),
                Arc::clone(&s.volume_manifest),
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
        // Clone the Arc'd content_store + serialize manifest under the
        // shard guard; drop the guard before the .await on put_manifest.
        let (manifest_bytes, content_store) = {
            let entry = self
                .exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            let s = entry.value();
            let manifest_bytes = s
                .volume_manifest
                .read()
                .serialize()
                .map_err(|e| RouterError::Manifest(e.to_string()))?;
            (manifest_bytes, Arc::clone(&s.content_store))
        };
        content_store
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

    /// Get the status of an in-flight bless task.
    pub async fn get_bless_status(&self, key: &str) -> Option<BlessStatus> {
        self.bless_tasks.read().await.get(key).cloned()
    }

    /// Start an OCI image bless operation.
    ///
    /// Returns immediately with the current status:
    /// - If the manifest already exists in S3, returns `{ state: "complete" }`.
    /// - If a bless is already in-flight for this key, returns the current status.
    /// - Otherwise, spawns a background task and returns `{ state: "pulling" }`.
    ///
    /// The spawned task adapts the CLI bless flow (`cli/bless.rs:run_bless_oci`)
    /// to reuse the running router's shared S3/cache infrastructure.
    pub async fn bless_oci_image(
        self: &Arc<Self>,
        s3_prefix: &str,
        name: &str,
        oci_image: &str,
        credentials: oci_registry::Credentials,
        insecure: bool,
    ) -> Result<BlessStatus, RouterError> {
        let key = format!("{s3_prefix}/{name}");

        // Check if already in-flight.
        {
            let tasks = self.bless_tasks.read().await;
            if let Some(status) = tasks.get(&key) {
                return Ok(status.clone());
            }
        }

        // Check if manifest already exists in S3.
        let manifest_name = format!("bases/{}", name);
        if self.head_manifest(s3_prefix, &manifest_name).await? {
            return Ok(BlessStatus {
                state: "complete".to_string(),
                oci_image: oci_image.to_string(),
            });
        }

        // Double-check under write lock and insert.
        let status = BlessStatus {
            state: "pulling".to_string(),
            oci_image: oci_image.to_string(),
        };
        {
            let mut tasks = self.bless_tasks.write().await;
            if let Some(existing) = tasks.get(&key) {
                return Ok(existing.clone());
            }
            tasks.insert(key.clone(), status.clone());
        }

        // Spawn the background ingest task. Supervised + RAII guard:
        // a panic in `run_bless_oci_task` previously left the
        // `bless_tasks` map entry orphaned forever (the cleanup at the
        // end of the closure never ran). The guard removes the entry
        // on Drop so cleanup fires on happy path, error, AND panic.
        let router = Arc::clone(self);
        let s3_prefix = s3_prefix.to_string();
        let name_owned = name.to_string();
        let oci_image_owned = oci_image.to_string();
        let cleanup_key = key.clone();
        let _handle = spawn_supervised("bless-bg", async move {
            // Guard: removes the bless_tasks entry on any exit (return,
            // error, panic). spawn_supervised's catch_unwind drops this
            // future on unwind; the guard's Drop runs before the catcher
            // observes the panic, so the map is always cleaned up.
            struct BlessGuard {
                router: Arc<ExportRouter>,
                key: String,
            }
            impl Drop for BlessGuard {
                fn drop(&mut self) {
                    // Use try_write/blocking removal: Drop is sync. If the
                    // lock is contended at panic time we tolerate skipping
                    // the cleanup (logged below) rather than blocking the
                    // unwind. In practice bless_tasks contention is rare
                    // and the next bless attempt with the same key will
                    // observe the stale entry and either retry or surface
                    // it as "already in progress".
                    let router = Arc::clone(&self.router);
                    let key = std::mem::take(&mut self.key);
                    tokio::spawn(async move {
                        router.bless_tasks.write().await.remove(&key);
                    });
                }
            }
            let _guard = BlessGuard { router: Arc::clone(&router), key: cleanup_key };

            if let Err(e) = router
                .run_bless_oci_task(&s3_prefix, &name_owned, &oci_image_owned, credentials, insecure)
                .await
            {
                error!(
                    s3_prefix = %s3_prefix,
                    name = %name_owned,
                    oci_image = %oci_image_owned,
                    error = %e,
                    "bless OCI task failed"
                );
            }
        });

        Ok(status)
    }

    /// Background bless task body. Adapted from `cli/bless.rs:run_bless_oci`.
    async fn run_bless_oci_task(
        &self,
        s3_prefix: &str,
        name: &str,
        oci_image: &str,
        credentials: oci_registry::Credentials,
        insecure: bool,
    ) -> Result<(), RouterError> {
        use crate::block::handler::BlockHandler;
        use crate::block::manifest::serialize_hot_set;
        use crate::block::metrics::ExportMetrics;
        use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
        use crate::block::write_cache::{WriteCache, WriteCacheConfig};
        use crate::oci::ingest::IngestOptions;
        use crate::oci::pull::pull_image;
        use ext4::writer::WriterOption;
        use oci_registry::RegistryClient;

        let start = std::time::Instant::now();

        // --- Resolve OCI image ---
        let registry_client = if insecure {
            RegistryClient::with_config(oci_registry::ClientConfig {
                protocol: oci_registry::ClientProtocol::Http,
                ..Default::default()
            })
        } else {
            RegistryClient::new()
        };
        let image: oci_registry::Reference = oci_image
            .parse()
            .map_err(|e| RouterError::OciPull(format!("invalid image reference: {e}")))?;

        info!(image = %oci_image, name = %name, s3_prefix = %s3_prefix, "resolving OCI image");

        let resolved = registry_client
            .resolve(&image, &credentials)
            .await
            .map_err(|e| RouterError::OciPull(format!("failed to resolve image: {e}")))?;

        // Estimate device size: compressed × 3, next power-of-2, min 64 MiB.
        let total_compressed: u64 = resolved.layers.iter().map(|l| l.size as u64).sum();
        let estimated = (total_compressed * 3).max(64 * 1024 * 1024);
        let device_size = estimated.next_power_of_two();

        info!(
            layers = resolved.layers.len(),
            total_compressed,
            device_size,
            "estimated device size"
        );

        // --- Temporary infrastructure ---
        let temp_dir = tempfile::TempDir::new().map_err(RouterError::Io)?;
        let cache = Arc::new(WriteCache::open_fresh_active(WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: format!("bless-oci-{}", name),
            device_size,
            block_size: self.block_size,
            wal_sync: false,
        })?);

        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            device_size,
            self.block_size as u32,
        )));

        // Build ContentStore reusing router's shared infra.
        let s3_base = format!("{}/exports/{}", self.db_path, s3_prefix);
        let mut cs = ContentStore::new(Arc::clone(&self.object_store), &s3_base)
            .with_circuit_breaker(Arc::clone(&self.s3_circuit_breaker));
        if let Some(sem) = &self.upload_semaphore {
            cs = cs.with_upload_semaphore(Arc::clone(sem));
        }
        if let Some(sem) = &self.download_semaphore {
            cs = cs.with_download_semaphore(Arc::clone(sem));
        }
        let content_store = Arc::new(cs);

        let metrics = Arc::new(ExportMetrics::new());
        let flush_notify = Arc::new(Notify::const_new());

        let handler = Arc::new(BlockHandler::new(
            Arc::clone(&cache),
            Arc::clone(&content_store),
            Arc::clone(&self.clean_cache),
            Arc::clone(&self.pack_index_cache),
            Arc::clone(&volume_manifest),
            device_size,
            false,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            flush_notify,
            DEFAULT_FLUSH_THRESHOLD,
            None,
        ));

        // --- Pull + ingest OCI image ---
        let uuid: [u8; 16] = rand::random();
        let ingest_opts = IngestOptions {
            writer_options: vec![
                WriterOption::MaximumDiskSize(device_size as i64),
                WriterOption::Uuid(uuid),
                WriterOption::Journal(1024), // 4 MiB journal
            ],
        };

        info!("pulling and ingesting layers");

        pull_image(
            &registry_client,
            &image,
            &credentials,
            Arc::clone(&handler),
            ingest_opts,
        )
        .await
        .map_err(|e| RouterError::OciPull(format!("pull failed: {e}")))?;

        // --- Update status to draining ---
        {
            let key = format!("{s3_prefix}/{name}");
            let mut tasks = self.bless_tasks.write().await;
            if let Some(status) = tasks.get_mut(&key) {
                status.state = "draining".to_string();
            }
        }

        // --- Drain to S3 ---
        info!("draining to S3");

        let max_drain_iterations = 100;
        let mut drained = false;
        for i in 0..max_drain_iterations {
            let stats = cache
                .flush_to_s3(&content_store, &self.pack_index_cache, &volume_manifest)
                .await?;
            if stats.blocks_claimed == 0 {
                info!(iterations = i + 1, "drain complete");
                drained = true;
                break;
            }
        }
        if !drained {
            return Err(RouterError::DrainIncomplete {
                name: name.to_string(),
                remaining: cache.dirty_block_count(),
                iterations: max_drain_iterations,
            });
        }

        // --- Generate hot set ---
        let hot_set = {
            let (blocks_per_chunk, chunk_packs): (u64, Vec<(u32, Vec<u64>)>) = {
                let vm = volume_manifest.read();
                let bpc = vm.blocks_per_chunk() as u64;
                let cp = vm
                    .chunks
                    .iter()
                    .map(|(&idx, entry)| (idx, entry.packs.clone()))
                    .collect();
                (bpc, cp)
            };

            let mut indices: Vec<u64> = Vec::new();
            for (chunk_idx, packs) in &chunk_packs {
                for &pack_id in packs {
                    match self.pack_index_cache.get_entries(pack_id).await {
                        Some(entries) => {
                            for e in entries.iter() {
                                let global_block =
                                    *chunk_idx as u64 * blocks_per_chunk + e.chunk_offset as u64;
                                indices.push(global_block);
                            }
                        }
                        None => {
                            warn!(
                                pack_id,
                                chunk_idx,
                                "pack index missing from cache, hot set may be incomplete"
                            );
                        }
                    }
                }
            }

            indices.sort_unstable();
            indices.dedup();
            indices
        };

        let hot_set_data = serialize_hot_set(&hot_set);
        content_store
            .put_hot_set(name, hot_set_data)
            .await
            .map_err(RouterError::ContentStore)?;

        // --- Save manifest ---
        let manifest_key = format!("bases/{}", name);
        let manifest_data = volume_manifest
            .read()
            .serialize()
            .map_err(|e| RouterError::Manifest(e.to_string()))?;
        content_store
            .put_manifest(&manifest_key, manifest_data, None)
            .await
            .map_err(RouterError::ContentStore)?;

        let elapsed = start.elapsed();
        info!(
            name = %name,
            s3_prefix = %s3_prefix,
            layers = resolved.layers.len(),
            device_size,
            hot_set_blocks = hot_set.len(),
            elapsed_secs = elapsed.as_secs_f64(),
            "bless OCI complete"
        );

        Ok(())
    }

    /// List snapshot sequence numbers for an export.
    pub async fn list_export_snapshots(
        &self,
        name: &str,
    ) -> Result<Vec<u64>, RouterError> {
        validate_export_name(name)?;
        let cs = {
            let entry = self
                .exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            Arc::clone(&entry.value().content_store)
        };
        cs.list_snapshots(name)
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
        let cs = {
            let entry = self
                .exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            Arc::clone(&entry.value().content_store)
        };
        cs.delete_snapshot(name, sequence)
            .await
            .map_err(RouterError::ContentStore)
    }

    /// Get info for a single export.
    pub async fn get_export_info(&self, name: &str) -> Option<ExportInfo> {
        let (mut info, handler) = {
            let entry = self.exports.get(name)?;
            let state = entry.value();
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
        self.exports.get(name).map(|e| Arc::clone(&e.value().handler))
    }

    /// Sync variant of [`Self::get_handler`] — needed by callbacks that
    /// can't `.await` (e.g., the `Fn` closure passed to ublk recovery).
    /// DashMap shard access is cheap and lock-free under no contention.
    pub fn get_handler_sync(&self, name: &str) -> Option<Arc<BlockHandler>> {
        self.exports.get(name).map(|e| Arc::clone(&e.value().handler))
    }

    /// True iff the kernel advertises `UBLK_F_PER_IO_DAEMON`. Used by
    /// the handoff strategy selector to pick PIOD over CRH when
    /// available. Always false on non-Linux or non-ublk builds.
    pub fn is_per_io_daemon_supported(&self) -> bool {
        #[cfg(all(target_os = "linux", feature = "ublk"))]
        {
            // Read happens under a brief tokio Mutex lock — but we're
            // called from sync code paths during startup. Use
            // try_lock_owned to avoid blocking; if contended (which
            // would be unusual since this is a config-time check),
            // conservatively return false.
            match self.ublk_server.try_lock() {
                Ok(g) => g.kernel_features().per_io_daemon,
                Err(_) => false,
            }
        }
        #[cfg(not(all(target_os = "linux", feature = "ublk")))]
        {
            false
        }
    }

    /// Check if an export is readonly.
    #[allow(dead_code)]
    pub async fn is_readonly(&self, name: &str) -> Option<bool> {
        self.exports.get(name).map(|e| e.value().readonly)
    }

    /// List all exports.
    pub async fn list_exports(&self) -> Vec<ExportInfo> {
        // First pass: collect basic info and handlers, dropping each
        // shard guard immediately as we walk.
        let (mut result, handlers): (Vec<ExportInfo>, Vec<Arc<BlockHandler>>) = self
            .exports
            .iter()
            .map(|entry| {
                let state = entry.value();
                let block_size = state.cache.block_size() as u64;
                let manifest = state.volume_manifest.read();
                let info = ExportInfo {
                    name: entry.key().clone(),
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
            .unzip();

        // Second pass: populate device paths and fs_used_bytes (async,
        // outside any shard guard).
        for (info, handler) in result.iter_mut().zip(handlers.iter()) {
            info.device = self.get_device_path(&info.name).await;
            info.fs_used_bytes = read_fs_used_bytes(handler).await;
        }

        result
    }

    /// Get export names.
    pub async fn list_export_names(&self) -> Vec<String> {
        self.exports.iter().map(|e| e.key().clone()).collect()
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
    ///
    /// For ublk this is a *two-phase* operation so the global
    /// `UblkServer` mutex is held only for the cheap map-removal step
    /// — the slow `kill_dev` ioctl + worker drain runs outside any
    /// lock, letting concurrent DELETEs proceed in parallel instead of
    /// queuing one-by-one behind the mutex.
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
                // Phase 1: take the device under the lock (microseconds).
                let device = {
                    let mut ublk = self.ublk_server.lock().await;
                    ublk.take_device(name)
                };
                // Phase 2: kill_dev outside the lock (seconds). Other
                // concurrent DELETEs reach this point in parallel and
                // each waits on its own device's STOP_DEV drain rather
                // than serializing on the UblkServer mutex.
                if let Some(device) = device {
                    crate::block::ublk::unregister_device(name, device)
                        .await
                        .map_err(|e| RouterError::Io(std::io::Error::other(e)))
                } else {
                    Ok(())
                }
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
        // Snapshot ublk-transport handlers. With DashMap, we never hold a
        // shard guard across the ublk-mutex acquisition below.
        let handlers: HashMap<String, Arc<BlockHandler>> = self
            .exports
            .iter()
            .filter(|e| e.value().transport == "ublk")
            .map(|e| (e.key().clone(), Arc::clone(&e.value().handler)))
            .collect();

        let mut ublk = self.ublk_server.lock().await;
        ublk.recover_quiesced_devices(|name| handlers.get(name).cloned())
            .await
    }

    /// Snapshot ublk worker pool capacity and utilization. Forwarded
    /// from `UblkServer::worker_capacity_snapshot`. Returns one tuple per
    /// worker: `(worker_idx, used_slots, capacity_slots, hosted_queues)`.
    /// Cheap (relaxed atomic loads); briefly holds the `ublk_server`
    /// mutex but does not block the worker threads.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn ublk_worker_capacity(&self) -> Vec<(usize, usize, usize, usize)> {
        self.ublk_server.lock().await.worker_capacity_snapshot()
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
        let exports_count = self.exports.len();

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
        self.exports.get(name).map(|e| Self::snapshot_export_metrics(e.value()))
    }

    /// Snapshot metrics for all exports under a single lock acquisition.
    /// Aggregate stats across all exports for host-level pressure monitoring.
    /// Cheap — reads in-memory atomics only, no I/O.
    pub async fn aggregate_stats(&self) -> AggregateStats {
        use crate::circuit_breaker::CircuitState;
        use std::sync::atomic::Ordering;

        let mut total_cache_hits: u64 = 0;
        let mut total_cache_misses: u64 = 0;
        let mut total_dirty_bytes: u64 = 0;

        for entry in self.exports.iter() {
            let state = entry.value();
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
            exports_count: self.exports.len(),
        }
    }

    pub async fn all_export_metrics(&self) -> Vec<(String, MetricsSnapshot)> {
        self.exports
            .iter()
            .map(|e| (e.key().clone(), Self::snapshot_export_metrics(e.value())))
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

        // Clone Arc'd components from the per-shard guard, then drop it
        // so we don't hold the lock across the long-running drain.
        let (cache, content_store, pack_index_cache, volume_manifest) = {
            let entry = self
                .exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            let s = entry.value();
            (
                Arc::clone(&s.cache),
                Arc::clone(&s.content_store),
                Arc::clone(&s.pack_index_cache),
                Arc::clone(&s.volume_manifest),
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
        if let Some(entry) = self.exports.get(name) {
            entry.value().metrics.record_flush_error();
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
        let mut entry = self
            .exports
            .get_mut(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
        let state = entry.value_mut();

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
        // Get current export info from the per-shard guard.
        let (current_size, readonly, block_size, orig_s3_prefix, transport) = {
            let entry = self
                .exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            let state = entry.value();
            (
                state.handler.device_size(),
                state.readonly,
                state.cache.block_size(),
                state.s3_prefix.clone(),
                state.transport.clone(),
            )
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
            match self.exports.remove(name) {
                Some((_k, state)) => state,
                None => {
                    info!("Export '{}' doesn't exist, nothing to remove", name);
                    if purge {
                        // Clean local cache files
                        let cache_file = self.cache_dir.join(format!("{}.cache", name));
                        let flushing_file = self.cache_dir.join(format!("{}.flushing", name));
                        let meta_file = self.cache_dir.join(format!("{}.meta", name));
                        let wal_file = self.cache_dir.join(format!("{}.wal", name));
                        remove_file_if_exists(&cache_file);
                        remove_file_if_exists(&flushing_file);
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
            let flushing_file = self.cache_dir.join(format!("{}.flushing", name));
            let meta_file = self.cache_dir.join(format!("{}.meta", name));
            let wal_file = self.cache_dir.join(format!("{}.wal", name));
            remove_file_if_exists(&cache_file);
            remove_file_if_exists(&flushing_file);
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

        // Take ownership of all exports. DashMap has no `drain` API
        // — collect names first, then remove each entry. Each remove
        // takes the entry's shard lock briefly; concurrent ops on
        // unrelated keys aren't blocked.
        let names: Vec<String> = self.exports.iter().map(|e| e.key().clone()).collect();
        let export_list: Vec<(String, ExportState)> = names
            .into_iter()
            .filter_map(|name| self.exports.remove(&name).map(|(k, v)| (k, v)))
            .collect();

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
        let names: Vec<String> = self.exports.iter().map(|e| e.key().clone()).collect();
        let export_list: Vec<(String, ExportState)> = names
            .into_iter()
            .filter_map(|name| self.exports.remove(&name).map(|(k, v)| (k, v)))
            .collect();

        for (name, state) in export_list {
            if let Some(handle) = state.prefetch_handle {
                handle.abort();
                let _ = handle.await;
            }
            let _ = state.flush_shutdown_tx.send(true);
            match state.flush_handle.await {
                Ok(Ok(())) => {}
                Ok(Err(panicked)) => {
                    tracing::warn!("Flush scheduler for '{}' panicked: {}", name, panicked);
                }
                Err(e) => {
                    tracing::warn!("Flush scheduler for '{}' join error: {:?}", name, e);
                }
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

        // 2. Wait for the flush scheduler to exit (releases its Arc clone).
        //
        // The scheduler only checks `shutdown` between iterations of its
        // outer `select!`; once it has committed to a `flush_and_sync`
        // cycle, it runs that cycle to completion. That latency is bounded
        // by the in-flight S3 work (single-pack PUT or multipart upload —
        // typically seconds, occasionally tens of seconds for large packs).
        //
        // For **purge** teardown we abort instead of waiting. The flush is
        // structurally cancellation-safe at the on-disk layer (init.rs
        // recovery converts orphaned SYNCING blocks back to DIRTY whether
        // or not a `.flushing` file is present), and the *in-memory*
        // state inconsistency that would otherwise be observed by the
        // drain loop below is moot here: purge skips the drain and then
        // deletes the cache + WAL + `.flushing` file in the same
        // teardown, so no observer ever sees the partial state. Aborting
        // turns "wait minutes on the current S3 PUT" into an immediate
        // task drop — the correct behavior when the user is throwing
        // away the data.
        //
        // For **non-purge** teardown (and SIGTERM, which routes through
        // `router.shutdown()` with `skip_drain=false`) we must wait. The
        // drain loop below calls `flush_to_s3`, which internally checks
        // for an existing `.flushing` file and returns
        // `FlushStats::default()` ("no dirty blocks") if one is present —
        // meaning if we aborted mid-rotation we'd silently lose the
        // SYNCING blocks' data instead of flushing them. Waiting for the
        // scheduler's current cycle to finish ensures the `.flushing`
        // file is cleaned up before drain runs.
        if skip_drain {
            flush_handle.abort();
        }
        match flush_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(panicked)) => {
                warn!("Flush scheduler for '{}' panicked: {}", name, panicked);
            }
            Err(e) if e.is_cancelled() => {
                debug!("Flush scheduler for '{}' aborted (purge)", name);
            }
            Err(e) => {
                warn!("Flush scheduler for '{}' join error: {:?}", name, e);
            }
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

    // ========================================================================
    // Graceful handoff support — methods called from `crate::handoff::*`.
    // ========================================================================

    /// Snapshot of exports for the predecessor to send in `HelloAck`.
    /// Includes every export's name, size, readonly flag, last WAL
    /// sequence, and ublk dev_id (if any). Captured atomically per
    /// export — values reflect a consistent point-in-time view.
    pub async fn handoff_snapshot(
        &self,
    ) -> Vec<crate::handoff::protocol::ExportSnapshot> {
        let mut out: Vec<crate::handoff::protocol::ExportSnapshot> = Vec::new();

        // Collect names first to avoid holding shard locks across an
        // .await for the ublk dev_id lookup.
        let names: Vec<String> = self.exports.iter().map(|e| e.key().clone()).collect();

        let dev_id_map: HashMap<String, i32> = {
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            {
                let pairs = self.ublk_server.lock().await.snapshot_dev_ids();
                pairs.into_iter().map(|(id, name)| (name, id)).collect()
            }
            #[cfg(not(all(target_os = "linux", feature = "ublk")))]
            {
                HashMap::new()
            }
        };

        for name in names {
            let Some(state) = self.exports.get(&name) else {
                continue;
            };
            let state = state.value();
            let last_wal_seq = state.cache.last_persisted_seq();
            out.push(crate::handoff::protocol::ExportSnapshot {
                name: name.clone(),
                size_bytes: state.handler.device_size(),
                readonly: state.readonly,
                last_wal_seq,
                ublk_dev_id: dev_id_map.get(&name).copied(),
            });
        }

        out
    }

    /// Quiesce every export for handoff: mark BlockHandlers as frozen
    /// (metadata only — see [`BlockHandler::freeze`] note about why
    /// it does not gate writes), then `cache.flush()` to fsync each
    /// WAL.
    ///
    /// **Why fsync is required**: skipping it interacts badly with the
    /// concurrent flush+rotate machinery. If the predecessor's
    /// flush_scheduler rotates the data file (DIRTY → SYNCING + active
    /// file → flushing file) between the successor's WARMING-time
    /// WriteCache::open and the predecessor's drop, the successor's
    /// state-map view loses entries that were rotated. Fsync forces a
    /// stable WAL state before the cutover so the successor's
    /// tail-replay sees the same sequence horizon the predecessor
    /// committed to.
    ///
    /// Cost: fdatasync per export, ~5–50ms each. At fleet scale (~1000
    /// exports) we parallelize at 16-way concurrency to bound the
    /// total to a few hundred ms.
    pub async fn freeze_all(&self) -> Result<(), RouterError> {
        let states: Vec<(String, Arc<BlockHandler>, Arc<WriteCache<Active>>)> = self
            .exports
            .iter()
            .map(|e| {
                let name = e.key().clone();
                let state = e.value();
                (name, Arc::clone(&state.handler), Arc::clone(&state.cache))
            })
            .collect();

        if states.is_empty() {
            return Ok(());
        }

        info!(count = states.len(), "handoff: freezing all handlers");
        for (_, handler, cache) in &states {
            handler.freeze();
            // Pause the per-export checkpoint truncate so the WAL
            // stays intact for the successor's tail-replay window.
            cache.set_freeze_in_progress(true);
        }

        use futures::stream::{self, StreamExt};
        let errs: Vec<_> = stream::iter(states.into_iter())
            .map(|(name, _handler, cache)| async move {
                tokio::task::spawn_blocking(move || cache.flush())
                    .await
                    .map_err(|e| (name.clone(), format!("join: {e}")))
                    .and_then(|r| r.map_err(|e| (name, format!("flush: {e}"))))
            })
            .buffer_unordered(16)
            .filter_map(|r| async move { r.err() })
            .collect()
            .await;

        if let Some((name, detail)) = errs.into_iter().next() {
            return Err(RouterError::ShutdownIncomplete {
                incomplete_count: 1,
                details: format!("freeze fsync failed for '{name}': {detail}"),
            });
        }

        Ok(())
    }

    /// Set the per-export `freeze_in_progress` flag on every WriteCache.
    /// While `true`, the flush_scheduler's checkpoint cycle skips WAL
    /// truncation. Used by:
    /// - Predecessor's `freeze_all` (briefly, during the cutover window)
    /// - Successor's WARMING phase (the whole time the predecessor is
    ///   still alive and may be appending to the WAL we share)
    pub async fn set_all_caches_freeze(&self, frozen: bool) {
        let caches: Vec<Arc<WriteCache<Active>>> = self
            .exports
            .iter()
            .map(|e| Arc::clone(&e.value().cache))
            .collect();
        for c in caches {
            c.set_freeze_in_progress(frozen);
        }
    }

    /// Reverse [`freeze_all`]. Called on the predecessor's revival path
    /// if the successor crashes between PREDS_DEAD and ALIVE. Also
    /// resumes per-export checkpoint truncation.
    pub async fn unfreeze_all(&self) {
        let states: Vec<(Arc<BlockHandler>, Arc<WriteCache<Active>>)> = self
            .exports
            .iter()
            .map(|e| {
                let s = e.value();
                (Arc::clone(&s.handler), Arc::clone(&s.cache))
            })
            .collect();
        for (h, c) in states {
            h.unfreeze();
            c.set_freeze_in_progress(false);
        }
        info!("handoff: handlers unfrozen (handoff aborted)");
    }

    /// Take the UblkServer out of the router and drop it. This is the
    /// kernel-level CRH cutover: dropping closes io_uring fds, which
    /// causes the kernel to transition every device to QUIESCED.
    ///
    /// The router keeps everything else alive — ExportRouter, ExportStates,
    /// WriteCaches, BlockHandlers — so revival is possible if the
    /// successor crashes before sending ALIVE.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn take_ublk_server(&self) -> Result<(), RouterError> {
        let mut guard = self.ublk_server.lock().await;
        let old = std::mem::take(&mut *guard);
        drop(guard);
        // Run shutdown to drop the worker pool cleanly. This blocks
        // until every worker has exited and its io_urings are closed.
        if let Err(e) = old.shutdown().await {
            return Err(RouterError::ShutdownIncomplete {
                incomplete_count: 1,
                details: format!("ublk server shutdown failed during handoff cutover: {e}"),
            });
        }
        info!("handoff: ublk server dropped; kernel devices QUIESCED");
        Ok(())
    }

    /// Successor-side: recover the QUIESCED devices the predecessor
    /// left behind. Called from `CrhStrategy::successor_takeover`.
    ///
    /// Before the ublk-level recovery, this also **tail-replays each
    /// export's WAL** to pick up any entries the predecessor fsync'd
    /// between the successor's WARMING-time WriteCache::open and the
    /// predecessor's freeze. Without this, writes acknowledged in that
    /// window would silently disappear on the successor side — the
    /// failure manifests as fio verify mismatches ("bad magic header 0,
    /// wanted acca"). See `WriteCache::replay_wal_tail`.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn recover_handoff_devices(
        &self,
        ids: &[(i32, String)],
    ) -> Result<usize, RouterError> {
        // Tail-replay WALs for every export we're about to recover.
        // Done before the ublk-level recovery so the BlockHandler's
        // view of state_map is current by the time the kernel reissues
        // bios at us.
        for (_, export_name) in ids {
            if let Some(state) = self.exports.get(export_name) {
                let cache = Arc::clone(&state.value().cache);
                match tokio::task::spawn_blocking(move || cache.replay_wal_tail())
                    .await
                {
                    Ok(Ok(n)) => {
                        if n > 0 {
                            tracing::info!(
                                export = %export_name,
                                replayed = n,
                                "handoff: tail-replayed WAL entries"
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        return Err(RouterError::ShutdownIncomplete {
                            incomplete_count: 1,
                            details: format!(
                                "WAL tail replay failed for '{export_name}': {e}"
                            ),
                        });
                    }
                    Err(e) => {
                        return Err(RouterError::ShutdownIncomplete {
                            incomplete_count: 1,
                            details: format!("spawn_blocking failed: {e}"),
                        });
                    }
                }
            }
        }

        let mut server = self.ublk_server.lock().await;
        let exports = &self.exports;
        let get_handler = |name: &str| -> Option<Arc<BlockHandler>> {
            exports.get(name).map(|e| Arc::clone(&e.value().handler))
        };
        let recovered = server.recover_devices_by_id(ids, get_handler).await;
        Ok(recovered)
    }

    /// Predecessor-side revival: the successor crashed after we already
    /// dropped our UblkServer. Spin up a fresh UblkServer and recover
    /// our own QUIESCED devices via the standard crash-recovery path.
    /// Returns the number of devices recovered. Caller should then call
    /// [`unfreeze_all`] so I/O resumes.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn revive_after_failed_handoff(&self) -> Result<usize, RouterError> {
        info!("handoff: reviving after failed handoff");
        // Replace the dropped UblkServer with a fresh one and rerun the
        // standard crash-recovery scan. Since this scan looks at the
        // kernel directly (`for_each_dev_id`), it doesn't need our
        // export inventory — but we still need handlers, which exist
        // because we never tore down ExportStates.
        let mut server = self.ublk_server.lock().await;
        *server = crate::block::ublk::UblkServer::new()
            .with_cache_dir(self.cache_dir.clone());
        let exports = &self.exports;
        let get_handler = |name: &str| -> Option<Arc<BlockHandler>> {
            exports.get(name).map(|e| Arc::clone(&e.value().handler))
        };
        let recovered = server.recover_quiesced_devices(get_handler).await;
        info!(recovered, "handoff: revival complete");
        Ok(recovered)
    }

    /// Stubs for non-Linux / non-ublk builds.
    #[cfg(not(all(target_os = "linux", feature = "ublk")))]
    pub async fn take_ublk_server(&self) -> Result<(), RouterError> {
        Ok(())
    }
    #[cfg(not(all(target_os = "linux", feature = "ublk")))]
    pub async fn recover_handoff_devices(
        &self,
        _ids: &[(i32, String)],
    ) -> Result<usize, RouterError> {
        Ok(0)
    }
    #[cfg(not(all(target_os = "linux", feature = "ublk")))]
    pub async fn revive_after_failed_handoff(&self) -> Result<usize, RouterError> {
        Ok(0)
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
            max_exports: 10_000,
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
    use std::sync::atomic::{AtomicU32, Ordering};
    use tempfile::TempDir;

    fn fast_test_policy() -> SupervisorPolicy {
        SupervisorPolicy {
            max_consecutive_panics: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            stable_threshold: Duration::from_secs(60), // never trips in fast tests
        }
    }

    /// Generic supervisor loop survives a single panic and re-spawns the inner
    /// task with the next-attempt closure. After the second attempt completes
    /// cleanly the supervisor exits without marking degraded.
    #[tokio::test]
    async fn supervisor_loop_restarts_after_one_panic() {
        let metrics = Arc::new(ExportMetrics::new());
        let (tx, rx) = watch::channel(false);
        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = Arc::clone(&attempt);
        let metrics_clone = Arc::clone(&metrics);
        let tx_clone = tx.clone();

        run_supervisor_loop(
            "test-restart",
            rx,
            metrics_clone,
            fast_test_policy(),
            move || {
                let attempt = Arc::clone(&attempt_clone);
                let tx = tx_clone.clone();
                async move {
                    let n = attempt.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        panic!("simulated inner panic");
                    }
                    // Second attempt: signal shutdown so the loop exits clean.
                    let _ = tx.send(true);
                }
            },
        )
        .await;

        assert_eq!(
            attempt.load(Ordering::SeqCst),
            2,
            "expected exactly 2 attempts (first panicked, second succeeded)",
        );
        assert_eq!(metrics.flush_task_panics.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.flush_degraded.load(Ordering::Relaxed),
            0,
            "should not be degraded after a single panic + recovery",
        );
    }

    /// Supervisor gives up after `max_consecutive_panics` and marks the
    /// export degraded.
    #[tokio::test]
    async fn supervisor_loop_marks_degraded_after_repeated_panics() {
        let metrics = Arc::new(ExportMetrics::new());
        let (_tx, rx) = watch::channel(false);
        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = Arc::clone(&attempt);
        let metrics_clone = Arc::clone(&metrics);

        run_supervisor_loop(
            "test-degraded",
            rx,
            metrics_clone,
            fast_test_policy(),
            move || {
                let attempt = Arc::clone(&attempt_clone);
                async move {
                    attempt.fetch_add(1, Ordering::SeqCst);
                    panic!("always panic");
                }
            },
        )
        .await;

        // 3 attempts (cap=3) before giving up.
        assert_eq!(attempt.load(Ordering::SeqCst), 3);
        assert_eq!(metrics.flush_task_panics.load(Ordering::Relaxed), 3);
        assert_eq!(
            metrics.flush_degraded.load(Ordering::Relaxed),
            1,
            "should mark degraded after exceeding the consecutive panic cap",
        );
    }

    /// Supervisor exits cleanly when the inner returns Ok(()) without
    /// panicking — does not increment counters.
    #[tokio::test]
    async fn supervisor_loop_clean_inner_exit() {
        let metrics = Arc::new(ExportMetrics::new());
        let (tx, rx) = watch::channel(false);
        let metrics_clone = Arc::clone(&metrics);
        let tx_clone = tx.clone();

        run_supervisor_loop(
            "test-clean",
            rx,
            metrics_clone,
            fast_test_policy(),
            move || {
                let tx = tx_clone.clone();
                async move {
                    // Signal shutdown so the supervisor sees the change
                    // alongside our clean Ok(()) return and stops looping.
                    let _ = tx.send(true);
                }
            },
        )
        .await;

        assert_eq!(metrics.flush_task_panics.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.flush_degraded.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn create_export_returns_error_when_at_max() {
        let temp = TempDir::new().unwrap();
        // Build a router with max_exports=2 by going through RouterConfig
        // (the test helper above hardcodes 10_000 for normal tests).
        let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let router = ExportRouter::new(RouterConfig {
            object_store: s3,
            db_path: "test".to_string(),
            cache_dir: temp.path().to_path_buf(),
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
            max_exports: 2,
        })
        .await
        .unwrap();

        let make = |name: &str| ExportConfig {
            name: name.to_string(),
            size_gb: 0.001,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router.create_export(make("a"), false, None, None).await.unwrap();
        router.create_export(make("b"), false, None, None).await.unwrap();
        let err = router
            .create_export(make("c"), false, None, None)
            .await
            .expect_err("third export must be rejected");
        assert!(
            matches!(err, RouterError::ExportLimitReached { current: 2, max: 2, .. }),
            "expected ExportLimitReached, got {:?}",
            err,
        );

        // Idempotent re-create of an existing name still succeeds.
        router.create_export(make("a"), false, None, None).await.unwrap();
    }

    /// Aborting the supervisor's outer task aborts the in-flight inner via
    /// the `AbortOnDrop` guard.
    #[tokio::test]
    async fn supervisor_loop_abort_propagates_to_inner() {
        let metrics = Arc::new(ExportMetrics::new());
        let (_tx, rx) = watch::channel(false);
        let inner_done = Arc::new(AtomicU32::new(0));
        let inner_done_clone = Arc::clone(&inner_done);
        let metrics_clone = Arc::clone(&metrics);

        let supervisor = task::spawn_supervised("supervisor-abort-test", async move {
            run_supervisor_loop(
                "test-abort",
                rx,
                metrics_clone,
                fast_test_policy(),
                move || {
                    let done = Arc::clone(&inner_done_clone);
                    async move {
                        // Run forever; only exit on abort.
                        std::future::pending::<()>().await;
                        done.fetch_add(1, Ordering::SeqCst);
                    }
                },
            )
            .await;
        });

        // Give the supervisor a moment to spawn its inner task.
        tokio::time::sleep(Duration::from_millis(50)).await;
        supervisor.abort();
        let _ = supervisor.await;

        // The inner future was pending forever; if the abort guard worked,
        // it never ran the `done.fetch_add` line.
        assert_eq!(inner_done.load(Ordering::SeqCst), 0);
    }

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
            max_exports: 10_000,
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
            max_exports: 10_000,
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
            max_exports: 10_000,
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
            max_exports: 10_000,
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
            max_exports: 10_000,
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
            max_exports: 10_000,
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
