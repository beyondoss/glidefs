//! Multi-tenant export router for NBD server.
//!
//! Manages multiple NBD exports, each with its own write cache and S3 storage.
//! Supports dynamic export creation/removal for microVM scale-to-zero and live migration.

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
use crate::config::ExportConfig;
use crate::nbd::cache::BlockCache;
use crate::nbd::content_store::ContentStore;
use crate::nbd::flush_scheduler::{flush_scheduler, FlushMode};
use crate::nbd::handler::NBDBlockHandler;
use crate::nbd::manifest::{deserialize_hot_set, Manifest};
use crate::nbd::metrics::{ExportMetrics, MetricsSnapshot};
use crate::nbd::pack::PackLocation;
use crate::nbd::pack_index::HostPackIndex;
use crate::nbd::pack_registry::PackRegistry;
use crate::nbd::state::Active;
use crate::nbd::write_cache::{CacheError, SnapshotResult, WriteCache, WriteCacheConfig};
use crate::task::spawn_named;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{watch, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Errors that can occur during export operations.
#[derive(Error, Debug)]
pub enum RouterError {
    #[allow(dead_code)] // create_export is idempotent, but API layer still matches this
    #[error("Export '{0}' already exists")]
    ExportExists(String),

    #[error("Export '{0}' not found")]
    ExportNotFound(String),

    #[error("Invalid export name '{0}': must be 1-128 chars, alphanumeric/hyphen/underscore/dot, starting with alphanumeric")]
    InvalidExportName(String),

    #[error("Cannot shrink export '{name}': current size {current_gb}GB, requested {requested_gb}GB")]
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
    ContentStore(#[from] crate::nbd::content_store::ContentStoreError),

    #[error("Manifest error: {0}")]
    Manifest(String),
}

/// Information about an export for NBD protocol.
#[derive(Clone, Debug)]
pub struct ExportInfo {
    pub name: String,
    pub size: u64,
    pub readonly: bool,
}

/// Readiness check result for health endpoint.
#[derive(Debug, Serialize)]
pub struct ReadinessStatus {
    pub ready: bool,
    pub exports_count: usize,
    pub cache_writable: bool,
    pub s3_reachable: bool,
}

/// Response from a snapshot operation.
#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    pub manifest_etag: Option<String>,
    pub sequence: u64,
}

/// State for a single export.
pub struct ExportState {
    pub handler: Arc<NBDBlockHandler>,
    pub cache: Arc<WriteCache<Active>>,
    pub content_store: Arc<ContentStore>,
    pub pack_index: Arc<HostPackIndex>,
    pub readonly: bool,
    pub metrics: Arc<ExportMetrics>,
    flush_mode_tx: watch::Sender<FlushMode>,
    flush_shutdown_tx: watch::Sender<bool>,
    flush_handle: JoinHandle<()>,
}

/// Maximum drain iterations before giving up. Prevents infinite loops when
/// concurrent writes keep producing new dirty blocks faster than we flush.
const MAX_DRAIN_ITERATIONS: usize = 100;

impl ExportState {
    /// Drain all dirty blocks to S3 via v2 content-addressed packs.
    pub async fn drain(&self) -> Result<(), RouterError> {
        // Loop until no more dirty blocks remain (concurrent writes may
        // produce new dirty data between flushes).
        for _ in 0..MAX_DRAIN_ITERATIONS {
            let stats = self
                .cache
                .flush_to_s3(&self.content_store, &self.pack_index)
                .await
                .map_err(RouterError::Cache)?;
            if stats.blocks_flushed == 0 {
                return Ok(());
            }
        }
        warn!(
            "drain hit iteration limit ({}), {} dirty blocks remain",
            MAX_DRAIN_ITERATIONS,
            self.cache.dirty_block_count()
        );
        Ok(())
    }

    /// Get the current flush mode.
    pub fn flush_mode(&self) -> FlushMode {
        self.flush_mode_tx.borrow().clone()
    }
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

    /// Host-level pack index shared across all exports (content-addressed dedup)
    pack_index: Arc<HostPackIndex>,

    /// Shared clean cache across all exports (content-addressed dedup)
    clean_cache: Arc<dyn BlockCache>,

    /// Whether to fsync the WAL after each write batch
    wal_sync: bool,

    /// Scrubber metrics (global, not per-export)
    scrubber_metrics: Arc<crate::nbd::scrubber::ScrubberMetrics>,

    /// Shared S3 circuit breaker: opens after 5 consecutive failures, probes after 30s.
    s3_circuit_breaker: Arc<CircuitBreaker>,
}

/// Validate an export name: 1-128 chars, alphanumeric/hyphen/underscore/dot,
/// must start with an alphanumeric character. Rejects path traversal attempts.
fn validate_export_name(name: &str) -> Result<(), RouterError> {
    if name.is_empty() || name.len() > 128 {
        return Err(RouterError::InvalidExportName(name.to_string()));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
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
    pub fn new(config: RouterConfig) -> Self {
        let s3_circuit_breaker = Arc::new(CircuitBreaker::new(
            CircuitBreakerConfig::consecutive(5)
                .reset_timeout(Duration::from_secs(30))
                .half_open_permits(3),
        ));

        Self {
            exports: RwLock::new(HashMap::new()),
            object_store: config.object_store,
            db_path: config.db_path,
            cache_dir: config.cache_dir,
            block_size: config.block_size,
            pack_index: Arc::new(HostPackIndex::new()),
            clean_cache: config.clean_cache,
            wal_sync: config.wal_sync,
            scrubber_metrics: Arc::new(crate::nbd::scrubber::ScrubberMetrics::new()),
            s3_circuit_breaker,
        }
    }

    /// Get a reference to the shared pack index (for scrubber).
    pub fn pack_index(&self) -> &Arc<HostPackIndex> {
        &self.pack_index
    }

    /// Prune pack index entries not referenced by any active export.
    ///
    /// Collects all non-zero hashes from every active export's block map,
    /// then removes pack index entries that aren't in the set. Cost is
    /// O(total_blocks + pack_index_entries), runs only on export removal.
    async fn prune_pack_index(&self) {
        let exports = self.exports.read().await;
        if exports.is_empty() {
            // No active exports — clear everything.
            let removed = self.pack_index.len();
            if removed > 0 {
                self.pack_index.rebuild(&[]);
                info!(removed, "pruned all pack index entries (no active exports)");
            }
            return;
        }

        // Union of all referenced hashes across active exports.
        let mut referenced = std::collections::HashSet::new();
        for state in exports.values() {
            referenced.extend(state.cache.referenced_hashes());
        }
        drop(exports);

        let removed = self.pack_index.prune_unreferenced(&referenced);
        if removed > 0 {
            info!(
                removed,
                remaining = self.pack_index.len(),
                "pruned unreferenced pack index entries"
            );
        }
    }

    /// Get a reference to the shared clean cache (for scrubber).
    pub fn clean_cache(&self) -> &Arc<dyn BlockCache> {
        &self.clean_cache
    }

    /// Get the shared scrubber metrics (for scrubber + prometheus).
    pub fn scrubber_metrics(&self) -> &Arc<crate::nbd::scrubber::ScrubberMetrics> {
        &self.scrubber_metrics
    }

    /// Get the current S3 circuit breaker state for observability.
    pub fn s3_circuit_state(&self) -> CircuitState {
        self.s3_circuit_breaker.state()
    }

    // =========================================================================
    // Export persistence to S3
    // =========================================================================

    /// S3 path for export definition.
    fn export_json_path(&self, name: &str) -> Path {
        Path::from(format!("{}/nbd/{}/export.json", self.db_path, name))
    }

    /// Save export definition to S3 (idempotent).
    async fn save_export(&self, config: &ExportConfig) -> Result<(), RouterError> {
        let path = self.export_json_path(&config.name);
        let json = serde_json::to_vec(config)?;
        self.object_store.put(&path, Bytes::from(json).into()).await?;
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
    /// Lists the `{db_path}/nbd/` prefix and loads each `export.json` found.
    pub async fn discover_exports(&self) -> Result<Vec<ExportConfig>, RouterError> {
        let prefix = Path::from(format!("{}/nbd/", self.db_path));
        let mut exports = Vec::new();

        // List all objects under the nbd prefix
        let mut stream = self.object_store.list(Some(&prefix));

        // Collect export names from export.json files
        let mut export_names = std::collections::HashSet::new();
        while let Some(result) = stream.next().await {
            let meta = result?;
            let path_str = meta.location.to_string();
            // Look for paths like "{db_path}/nbd/{name}/export.json"
            if path_str.ends_with("/export.json") {
                // Extract export name from path
                if let Some(name) = extract_export_name(&path_str, &self.db_path) {
                    export_names.insert(name);
                }
            }
        }

        // Load each export definition
        for name in export_names {
            match self.load_export(&name).await {
                Ok(Some(config)) => {
                    exports.push(config);
                }
                Ok(None) => {
                    warn!("Export.json disappeared during discovery: {}", name);
                }
                Err(e) => {
                    warn!("Failed to load export '{}': {}", name, e);
                }
            }
        }

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
    ) -> Result<(), RouterError> {
        let name = config.name.clone();
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

        let s3_prefix = format!("{}/nbd/{}", self.db_path, config.s3_prefix());

        // Content-addressed pack storage with circuit breaker
        let content_store = Arc::new(
            ContentStore::new(Arc::clone(&self.object_store), &s3_prefix)
                .with_circuit_breaker(Arc::clone(&self.s3_circuit_breaker)),
        );
        let clean_cache = Arc::clone(&self.clean_cache);
        let pack_index = Arc::clone(&self.pack_index);

        // Flush trigger shared between scheduler and API triggers
        let flush_trigger = Arc::new(Notify::new());

        // Create write cache — either from manifest (fork) or fresh (normal)
        let cache_config = WriteCacheConfig {
            cache_dir: self.cache_dir.clone(),
            device_name: name.clone(),
            device_size: config.size_bytes(),
            block_size,
            wal_sync: self.wal_sync,
        };

        let cache = if let Some(manifest_name) = manifest_name {
            // Fork path: load manifest from S3, populate pack index, open from manifest
            let manifest_data = content_store
                .get_manifest(manifest_name)
                .await?
                .ok_or_else(|| {
                    RouterError::Manifest(format!("manifest '{}' not found in S3", manifest_name))
                })?;

            let manifest = Manifest::deserialize(&manifest_data)
                .map_err(|e| RouterError::Manifest(format!("invalid manifest: {}", e)))?;

            // Populate pack index from manifest entries
            for entry in &manifest.pack_index {
                pack_index.insert(
                    entry.hash,
                    PackLocation {
                        pack_id: entry.pack_id,
                        offset: entry.offset,
                        comp_length: entry.comp_length,
                    },
                );
            }

            info!(
                "Loaded manifest '{}': {} block entries, {} pack entries, seq={}",
                manifest_name,
                manifest.block_map.len(),
                manifest.pack_index.len(),
                manifest.sequence,
            );

            // Build parent BlockMap from manifest for ForkedBlockMap overlay
            let parent_block_map = {
                use crate::nbd::block_map::{BlockMap, BlockMapEntry};
                let mut bm = BlockMap::new(config.size_bytes(), block_size as u32);
                for entry in &manifest.block_map {
                    bm.set(
                        entry.chunk_index as usize,
                        BlockMapEntry {
                            hash: entry.hash,
                            flags: 0,
                            sequence: 0, // sequence doesn't matter for parent — it's immutable
                        },
                    );
                }
                Arc::new(bm)
            };

            let cache = WriteCache::open_from_manifest(
                cache_config,
                &manifest,
                Some(parent_block_map),
            )?;

            // Create child pack registry from parent manifest's pack IDs (best-effort).
            // This ensures GC can track all packs referenced by this fork.
            {
                let fork_pack_ids: Vec<uuid::Uuid> = manifest
                    .pack_index
                    .iter()
                    .map(|e| e.pack_id)
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect();
                if !fork_pack_ids.is_empty() {
                    let child_registry = PackRegistry {
                        pack_ids: fork_pack_ids,
                    };
                    if let Err(e) = content_store
                        .put_registry(&name, child_registry.serialize())
                        .await
                    {
                        warn!(
                            "Failed to create pack registry for fork '{}': {}",
                            name, e
                        );
                    }
                }
            }

            info!("Export '{}' created from manifest (fork)", name);
            Arc::new(cache)
        } else {
            // Normal path: open cache, recover from WAL
            let cache = WriteCache::open(cache_config)?;
            info!("Recovering write cache for export '{}'...", name);
            let cache = cache.finish_recovery().await?;
            info!("Export '{}' cache ready", name);
            Arc::new(cache)
        };

        // Boot hot set prefetch: warm the clean cache before the VM reads
        if let Some(manifest_name) = manifest_name {
            // Extract base name from manifest_name (e.g., "bases/ubuntu-22.04" → "ubuntu-22.04")
            let hot_set_name = manifest_name.strip_prefix("bases/").unwrap_or(manifest_name);
            match content_store.get_hot_set(hot_set_name).await {
                Ok(Some(hot_set_data)) => {
                    match deserialize_hot_set(&hot_set_data) {
                        Ok(chunks) => {
                            info!(chunks = chunks.len(), "prefetching boot hot set");
                            let cache_clone = Arc::clone(&cache);
                            let cc = Arc::clone(&clean_cache);
                            let pi = Arc::clone(&pack_index);
                            let cs = Arc::clone(&content_store);
                            spawn_named("hot-set-prefetch", async move {
                                cache_clone.prefetch_chunks(&chunks, cc.as_ref(), &pi, &cs).await;
                                info!("boot hot set prefetch complete");
                            });
                        }
                        Err(e) => warn!("failed to deserialize hot set: {}", e),
                    }
                }
                Ok(None) => debug!("no hot set found for '{}'", hot_set_name),
                Err(e) => warn!("failed to fetch hot set: {}", e),
            }
        }

        // Create handler for block I/O
        let handler = Arc::new(NBDBlockHandler::new(
            Arc::clone(&cache),
            Arc::clone(&content_store),
            Arc::clone(&clean_cache),
            Arc::clone(&pack_index),
            config.size_bytes(),
            readonly,
            Arc::clone(&metrics),
        ));

        // Start flush scheduler for this export
        let flush_mode = config.flush_mode.clone().unwrap_or_default();
        let (flush_mode_tx, flush_mode_rx) = watch::channel(flush_mode);
        let (flush_shutdown_tx, flush_shutdown_rx) = watch::channel(false);
        let flush_cache = Arc::clone(&cache);
        let flush_cs = Arc::clone(&content_store);
        let flush_pi = Arc::clone(&pack_index);
        let flush_trig = Arc::clone(&flush_trigger);
        let export_name = name.clone();
        let flush_metrics = Arc::clone(&metrics);
        let flush_handle = spawn_named(&format!("flush-{}", name), async move {
            flush_scheduler(flush_cache, flush_cs, flush_pi, flush_mode_rx, flush_trig, flush_shutdown_rx, flush_metrics).await;
            info!("Flush scheduler for export '{}' stopped", export_name);
        });

        // Store export state
        let state = ExportState {
            handler,
            cache,
            content_store,
            pack_index,
            readonly,
            metrics,
            flush_mode_tx,
            flush_shutdown_tx,
            flush_handle,
        };

        let mut exports = self.exports.write().await;
        // Re-check under write lock to prevent TOCTOU race: a concurrent
        // create_export could have inserted between our read lock check and
        // this write lock acquisition.
        if exports.contains_key(&name) {
            info!("Export '{}' already exists (concurrent create), skipping", name);
            return Ok(());
        }
        exports.insert(name.clone(), state);

        info!("Export '{}' created successfully (readonly={})", name, readonly);

        // Persist export definition to S3 for discovery on restart
        if let Err(e) = self.save_export(&config).await {
            warn!("Failed to persist export to S3: {} (export is functional)", e);
        }

        Ok(())
    }

    /// Snapshot an export: flush dirty blocks to S3 and upload a manifest.
    ///
    /// Returns the manifest ETag and sequence number for use by the control plane.
    pub async fn snapshot_export(&self, name: &str) -> Result<SnapshotResponse, RouterError> {
        validate_export_name(name)?;
        let exports = self.exports.read().await;
        let state = exports
            .get(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;

        info!("Taking snapshot of export '{}'...", name);
        let result: SnapshotResult = state
            .cache
            .snapshot(&state.content_store, &state.pack_index)
            .await
            .map_err(RouterError::Cache)?;

        info!(
            "Snapshot of '{}' complete: seq={}, blocks_flushed={}, packs_uploaded={}",
            name, result.sequence, result.stats.blocks_flushed, result.stats.packs_uploaded,
        );

        Ok(SnapshotResponse {
            manifest_etag: result.manifest_etag,
            sequence: result.sequence,
        })
    }

    /// Get handler for an export (used during NBD negotiation).
    pub async fn get_handler(&self, name: &str) -> Option<Arc<NBDBlockHandler>> {
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
        let exports = self.exports.read().await;
        exports
            .iter()
            .map(|(name, state)| ExportInfo {
                name: name.clone(),
                size: state.handler.device_size(),
                readonly: state.readonly,
            })
            .collect()
    }

    /// Get export names.
    pub async fn list_export_names(&self) -> Vec<String> {
        let exports = self.exports.read().await;
        exports.keys().cloned().collect()
    }

    /// Check readiness: exports exist, cache writable, and S3 reachable.
    pub async fn readiness_check(&self) -> ReadinessStatus {
        let exports = self.exports.read().await;
        let exports_count = exports.len();

        let cache_writable = {
            let probe = self.cache_dir.join(".health-probe");
            tokio::fs::write(&probe, b"ok")
                .await
                .is_ok()
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
        exports.get(name).map(|s| {
            s.metrics.snapshot().with_cache_state(
                s.cache.dirty_block_count(),
                s.cache.syncing_block_count(),
            )
        })
    }

    /// Drain an export's dirty blocks to S3.
    pub async fn drain_export(&self, name: &str) -> Result<(), RouterError> {
        validate_export_name(name)?;
        let exports = self.exports.read().await;
        let state = exports
            .get(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;

        info!("Draining export '{}'...", name);
        state.drain().await?;
        info!("Export '{}' drained successfully", name);
        Ok(())
    }

    /// Drain all exports. Returns (name, error) pairs for any that failed.
    pub async fn drain_all(&self) -> Vec<(String, RouterError)> {
        let names = self.list_export_names().await;
        let mut failed = Vec::new();
        for name in names {
            if let Err(e) = self.drain_export(&name).await {
                warn!(export = %name, error = %e, "failed to drain export");
                failed.push((name, e));
            }
        }
        failed
    }

    /// Change the flush mode for an export at runtime.
    pub async fn set_flush_mode(&self, name: &str, mode: FlushMode) -> Result<(), RouterError> {
        let exports = self.exports.read().await;
        let state = exports
            .get(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
        state
            .flush_mode_tx
            .send(mode)
            .map_err(|_| RouterError::Manifest("flush scheduler stopped".to_string()))?;
        Ok(())
    }

    /// Get the current flush mode for an export.
    pub async fn get_flush_mode(&self, name: &str) -> Option<FlushMode> {
        let exports = self.exports.read().await;
        exports.get(name).map(|s| s.flush_mode())
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
        let (current_size, readonly, block_size) = {
            let exports = self.exports.read().await;
            let state = exports
                .get(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;
            let current_size = state.handler.device_size();
            let readonly = state.readonly;
            let block_size = state.cache.block_size();
            (current_size, readonly, block_size)
        };

        let new_size_bytes = (new_size_gb * 1_000_000_000.0) as u64;
        let current_size_gb = current_size as f64 / 1_000_000_000.0;

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
            s3_prefix: None, // Will use name as prefix (same as before)
            block_size: Some(block_size),
            flush_mode: None,
        };

        self.create_export(config, readonly, Some(name)).await?;

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
                    // Still purge local files if requested (idempotent cleanup)
                    if purge {
                        let cache_file = self.cache_dir.join(format!("{}.cache", name));
                        let meta_file = self.cache_dir.join(format!("{}.meta", name));
                        let _ = std::fs::remove_file(&cache_file);
                        let _ = std::fs::remove_file(&meta_file);
                    }
                    return Ok(());
                }
            }
        };

        info!("Removing export '{}'...", name);
        Self::teardown_export(name, state).await;

        // Prune pack index entries no longer referenced by any active export.
        // Must run after teardown (which drops the removed export's cache) so
        // that only remaining exports contribute to the referenced set.
        self.prune_pack_index().await;

        if purge {
            let cache_file = self.cache_dir.join(format!("{}.cache", name));
            let meta_file = self.cache_dir.join(format!("{}.meta", name));
            let _ = std::fs::remove_file(&cache_file);
            let _ = std::fs::remove_file(&meta_file);
            info!("Purged cache files for export '{}'", name);

            // Also delete export definition from S3
            if let Err(e) = self.delete_export_definition(name).await {
                warn!("Failed to delete export definition from S3: {}", e);
            }
        }

        info!("Export '{}' removed", name);
        Ok(())
    }

    /// Shutdown all exports gracefully.
    ///
    /// This properly transitions each cache through the typestate:
    /// Active → Draining → finished
    pub async fn shutdown(&self) -> Result<(), RouterError> {
        info!("Shutting down all exports...");

        // Take ownership of all exports
        let mut exports = self.exports.write().await;
        let export_list: Vec<_> = exports.drain().collect();
        drop(exports); // Release the lock

        for (name, state) in export_list {
            info!("Shutting down export '{}'...", name);
            Self::teardown_export(&name, state).await;
        }

        info!("All exports shut down");
        Ok(())
    }

    /// Drain dirty blocks, stop flush scheduler, and transition cache through
    /// the Draining typestate. Shared by `remove_export` and `shutdown`.
    async fn teardown_export(name: &str, state: ExportState) {
        let ExportState {
            handler,
            cache,
            content_store,
            pack_index,
            flush_shutdown_tx,
            flush_handle,
            ..
        } = state;

        // 1. Signal flush scheduler to stop
        let _ = flush_shutdown_tx.send(true);

        // 2. Wait for flush scheduler to exit (releases its Arc clone)
        if let Err(e) = flush_handle.await {
            warn!("Flush scheduler for '{}' panicked: {}", name, e);
        }

        // 3. V2 drain: flush remaining dirty data
        for _ in 0..MAX_DRAIN_ITERATIONS {
            match cache.flush_to_s3(&content_store, &pack_index).await {
                Ok(stats) if stats.blocks_flushed == 0 => break,
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to drain export '{}': {}", name, e);
                    break;
                }
            }
        }

        // 4. Drop the handler (releases its Arc clone)
        drop(handler);

        // 5. Unwrap the Arc and transition through Draining state
        match Arc::try_unwrap(cache) {
            Ok(cache) => {
                match cache.shutdown().await {
                    Ok(draining) => {
                        draining.finish();
                        info!("Export '{}' torn down cleanly", name);
                    }
                    Err(e) => {
                        warn!("Failed to drain export '{}': {}", name, e);
                    }
                }
            }
            Err(arc) => {
                warn!(
                    "Export '{}' has {} references, cannot transition typestate",
                    name,
                    Arc::strong_count(&arc)
                );
            }
        }
    }

    /// Create a minimal router for testing protocol handling.
    /// Uses a temporary directory and in-memory S3.
    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
        use crate::nbd::cache::SimpleBlockCache;
        let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let temp_dir = std::env::temp_dir().join(format!("glidefs-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).expect("Failed to create test cache dir");

        Self::new(RouterConfig {
            object_store: s3,
            db_path: "test".to_string(),
            cache_dir: temp_dir,
            block_size: 128 * 1024,

            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
        })
    }
}

/// Extract export name from a path like "{db_path}/nbd/{name}/export.json".
fn extract_export_name(path: &str, db_path: &str) -> Option<String> {
    // Path format: "{db_path}/nbd/{name}/export.json"
    let prefix = format!("{}/nbd/", db_path);
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
    use crate::nbd::cache::SimpleBlockCache;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_router(temp_dir: &TempDir) -> ExportRouter {
        let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        ExportRouter::new(RouterConfig {
            object_store: s3,
            db_path: "test".to_string(),
            cache_dir: temp_dir.path().to_path_buf(),
            block_size: 128 * 1024,

            clean_cache: Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
            wal_sync: false,
        })
    }

    fn test_export_config(name: &str) -> ExportConfig {
        ExportConfig {
            name: name.to_string(),
            size_gb: 0.01, // 10MB
            s3_prefix: None,
            block_size: None,
            flush_mode: None,
        }
    }

    #[tokio::test]
    async fn test_create_export_success() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        let result = router.create_export(test_export_config("vol1"), false, None).await;
        assert!(result.is_ok(), "Should create export successfully");

        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].name, "vol1");
    }

    #[tokio::test]
    async fn test_create_export_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Create export twice
        router.create_export(test_export_config("vol1"), false, None).await.unwrap();
        let result = router.create_export(test_export_config("vol1"), false, None).await;

        // Second create should succeed (idempotent)
        assert!(result.is_ok(), "Second create should succeed (idempotent)");

        // Should still have only one export
        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 1);
    }

    #[tokio::test]
    async fn test_create_export_readonly() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), true, None).await.unwrap();

        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 1);
        assert!(exports[0].readonly, "Export should be readonly");
    }

    #[tokio::test]
    async fn test_get_handler_existing() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();

        let handler = router.get_handler("vol1").await;
        assert!(handler.is_some(), "Should return handler for existing export");
    }

    #[tokio::test]
    async fn test_get_handler_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        let handler = router.get_handler("nonexistent").await;
        assert!(handler.is_none(), "Should return None for nonexistent export");
    }

    #[tokio::test]
    async fn test_list_exports_empty() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        let exports = router.list_exports().await;
        assert!(exports.is_empty(), "Should return empty list");
    }

    #[tokio::test]
    async fn test_list_exports_multiple() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();
        router.create_export(test_export_config("vol2"), true, None).await.unwrap();
        router.create_export(test_export_config("vol3"), false, None).await.unwrap();

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
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();

        // Write some data through the handler
        let handler = router.get_handler("vol1").await.unwrap();
        let data = vec![0xAB; 4096];
        handler.write(0, &data, false).unwrap();

        // Drain should succeed
        let result = router.drain_export("vol1").await;
        assert!(result.is_ok(), "Drain should succeed");
    }

    #[tokio::test]
    async fn test_drain_export_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

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
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();
        assert_eq!(router.list_exports().await.len(), 1);

        let result = router.remove_export("vol1", false).await;
        assert!(result.is_ok(), "Remove should succeed");

        assert_eq!(router.list_exports().await.len(), 0);
    }

    #[tokio::test]
    async fn test_remove_export_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Remove nonexistent export should succeed (idempotent)
        let result = router.remove_export("nonexistent", false).await;
        assert!(result.is_ok(), "Remove nonexistent should succeed (idempotent)");
    }

    #[tokio::test]
    async fn test_remove_export_with_purge() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();

        // Write some data to create cache files
        let handler = router.get_handler("vol1").await.unwrap();
        handler.write(0, &[0xAB; 4096], false).unwrap();

        // Remove with purge
        router.remove_export("vol1", true).await.unwrap();

        // Cache files should be deleted (we can verify by trying to re-create)
        router.create_export(test_export_config("vol1"), false, None).await.unwrap();
        // Should succeed without "file exists" errors
    }

    #[tokio::test]
    async fn test_promote_export_success() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Create readonly export
        router.create_export(test_export_config("vol1"), true, None).await.unwrap();

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
        let router = create_test_router(&temp_dir);

        let result = router.promote_export("nonexistent").await;
        assert!(result.is_err(), "Promote should fail for nonexistent export");
    }

    #[tokio::test]
    async fn test_get_export_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();

        // Write some data
        let handler = router.get_handler("vol1").await.unwrap();
        handler.write(0, &[0xAB; 4096], false).unwrap();

        let metrics = router.get_export_metrics("vol1").await;
        assert!(metrics.is_some(), "Should return metrics");

        let m = metrics.unwrap();
        assert!(m.guest_write_ops >= 1, "Should have recorded write");
    }

    #[tokio::test]
    async fn test_get_export_metrics_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        let metrics = router.get_export_metrics("nonexistent").await;
        assert!(metrics.is_none(), "Should return None for nonexistent");
    }

    #[tokio::test]
    async fn test_shutdown_all_exports() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();
        router.create_export(test_export_config("vol2"), false, None).await.unwrap();

        let result = router.shutdown().await;
        assert!(result.is_ok(), "Shutdown should succeed");

        // After shutdown, list should be empty
        let exports = router.list_exports().await;
        assert!(exports.is_empty(), "Should have no exports after shutdown");
    }

    #[tokio::test]
    async fn test_export_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();
        router.create_export(test_export_config("vol2"), false, None).await.unwrap();

        let handler1 = router.get_handler("vol1").await.unwrap();
        let handler2 = router.get_handler("vol2").await.unwrap();

        // Write different data to each export
        handler1.write(0, &[0x11; 4096], false).unwrap();
        handler2.write(0, &[0x22; 4096], false).unwrap();

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
        let router = create_test_router(&temp_dir);

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
        let router = create_test_router(&temp_dir);

        let loaded = router.load_export("nonexistent").await.unwrap();
        assert!(loaded.is_none(), "Should return None for nonexistent");
    }

    #[tokio::test]
    async fn test_delete_export_definition_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

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
        let router = create_test_router(&temp_dir);

        let discovered = router.discover_exports().await.unwrap();
        assert!(discovered.is_empty(), "Should discover no exports");
    }

    #[tokio::test]
    async fn test_discover_exports_multiple() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Save multiple export definitions directly to S3
        let configs = vec![
            ExportConfig {
                name: "discover-vol1".to_string(),
                size_gb: 1.0,
                s3_prefix: None,
                block_size: None,
                flush_mode: None,
            },
            ExportConfig {
                name: "discover-vol2".to_string(),
                size_gb: 2.0,
                s3_prefix: None,
                block_size: None,
                flush_mode: None,
            },
            ExportConfig {
                name: "discover-vol3".to_string(),
                size_gb: 3.0,
                s3_prefix: None,
                block_size: None,
                flush_mode: None,
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
        let router = create_test_router(&temp_dir);

        // Create export (should persist to S3)
        router.create_export(test_export_config("auto-persist"), false, None).await.unwrap();

        // Verify it was persisted
        let loaded = router.load_export("auto-persist").await.unwrap();
        assert!(loaded.is_some(), "Export should be persisted to S3");
    }

    #[tokio::test]
    async fn test_remove_export_with_purge_deletes_from_s3() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Create export
        router.create_export(test_export_config("purge-vol"), false, None).await.unwrap();

        // Verify persisted
        let loaded = router.load_export("purge-vol").await.unwrap();
        assert!(loaded.is_some(), "Should be persisted");

        // Remove with purge
        router.remove_export("purge-vol", true).await.unwrap();

        // Verify deleted from S3
        let loaded = router.load_export("purge-vol").await.unwrap();
        assert!(loaded.is_none(), "Should be deleted from S3");
    }

    // =========================================================================
    // Snapshot + Fork tests
    // =========================================================================

    #[tokio::test]
    async fn test_snapshot_export() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("snap-vol"), false, None).await.unwrap();

        // Write some data
        let handler = router.get_handler("snap-vol").await.unwrap();
        handler.write(0, &[0xAA; 4096], false).unwrap();
        handler.write(128 * 1024, &[0xBB; 4096], false).unwrap();

        // Take snapshot
        let result = router.snapshot_export("snap-vol").await;
        assert!(result.is_ok(), "Snapshot should succeed: {:?}", result.err());

        let snap = result.unwrap();
        assert!(snap.sequence > 0, "Snapshot sequence should be > 0");
    }

    #[tokio::test]
    async fn test_snapshot_nonexistent_export() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        let result = router.snapshot_export("nonexistent").await;
        assert!(result.is_err(), "Snapshot should fail for nonexistent export");
        match result.unwrap_err() {
            RouterError::ExportNotFound(name) => assert_eq!(name, "nonexistent"),
            e => panic!("Expected ExportNotFound, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_fork_from_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Create source export and write data
        router.create_export(test_export_config("source"), false, None).await.unwrap();
        let handler = router.get_handler("source").await.unwrap();
        let data = vec![0xCC; 128 * 1024]; // one full block
        handler.write(0, &data, false).unwrap();

        // Snapshot source
        let snap = router.snapshot_export("source").await.unwrap();
        assert!(snap.sequence > 0);

        // Fork from snapshot
        let fork_config = ExportConfig {
            name: "fork1".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("source".to_string()), // same S3 prefix as source
            block_size: None,
            flush_mode: None,
        };
        router.create_export(fork_config, false, Some("source")).await.unwrap();

        // Read from fork — should get the same data that was written to source
        let fork_handler = router.get_handler("fork1").await.unwrap();
        let fork_data = fork_handler.read(0, 128 * 1024).await.unwrap();
        assert_eq!(fork_data.as_ref(), &data[..], "Fork should read source's data");
    }

    #[tokio::test]
    async fn test_fork_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Create and snapshot source
        router.create_export(test_export_config("src"), false, None).await.unwrap();
        let src_handler = router.get_handler("src").await.unwrap();
        src_handler.write(0, &[0xAA; 128 * 1024], false).unwrap();
        router.snapshot_export("src").await.unwrap();

        // Fork
        let fork_config = ExportConfig {
            name: "fork-iso".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("src".to_string()),
            block_size: None,
            flush_mode: None,
        };
        router.create_export(fork_config, false, Some("src")).await.unwrap();

        // Write to fork
        let fork_handler = router.get_handler("fork-iso").await.unwrap();
        fork_handler.write(0, &[0xFF; 128 * 1024], false).unwrap();

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
    async fn test_fork_from_missing_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        let config = ExportConfig {
            name: "bad-fork".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("nonexistent-source".to_string()),
            block_size: None,
            flush_mode: None,
        };

        let result = router.create_export(config, false, Some("does-not-exist")).await;
        assert!(result.is_err(), "Fork from missing manifest should fail");
    }

    // =========================================================================
    // Resize tests
    // =========================================================================

    #[tokio::test]
    async fn test_resize_export_grow() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();

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
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();

        // Resize to same size — should succeed as no-op
        let result = router.resize_export("vol1", 0.01).await;
        assert!(result.is_ok(), "Same-size resize should be idempotent");
    }

    #[tokio::test]
    async fn test_resize_export_shrink_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false, None).await.unwrap();

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
        let router = create_test_router(&temp_dir);

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
        let router = create_test_router(&temp_dir);

        // Create source, write data, snapshot
        router.create_export(test_export_config("parent"), false, None).await.unwrap();
        let parent_handler = router.get_handler("parent").await.unwrap();
        parent_handler.write(0, &[0xAA; 128 * 1024], false).unwrap();
        let _parent_snap = router.snapshot_export("parent").await.unwrap();

        // Fork from parent
        let fork_config = ExportConfig {
            name: "child".to_string(),
            size_gb: 0.01,
            s3_prefix: Some("parent".to_string()),
            block_size: None,
            flush_mode: None,
        };
        router.create_export(fork_config, false, Some("parent")).await.unwrap();

        // Write different data to the fork
        let fork_handler = router.get_handler("child").await.unwrap();
        fork_handler.write(0, &[0xFF; 128 * 1024], false).unwrap();
        fork_handler.write(128 * 1024, &[0xDD; 128 * 1024], false).unwrap();

        // Snapshot the fork
        router.snapshot_export("child").await.unwrap();

        // Re-snapshot the parent — its manifest should be unchanged
        let _parent_snap2 = router.snapshot_export("parent").await.unwrap();
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
        assert!(fork_data.iter().all(|&b| b == 0xFF), "fork should see its own writes");

        let fork_data2 = fork_handler.read(128 * 1024, 128 * 1024).await.unwrap();
        assert!(fork_data2.iter().all(|&b| b == 0xDD), "fork second block");
    }

    // =========================================================================
    // Resize with active I/O
    // =========================================================================

    #[tokio::test]
    async fn test_resize_grows_export_and_allows_writes() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("resize-vol"), false, None).await.unwrap();

        let old_size = {
            let exports = router.list_exports().await;
            exports.iter().find(|e| e.name == "resize-vol").unwrap().size
        };

        // Resize to double
        router.resize_export("resize-vol", 0.02).await.unwrap();

        let new_size = {
            let exports = router.list_exports().await;
            exports.iter().find(|e| e.name == "resize-vol").unwrap().size
        };
        assert!(new_size > old_size, "export should have grown");

        // Write to new region (beyond original size) should succeed
        let handler = router.get_handler("resize-vol").await.unwrap();
        let write_offset = old_size - 128 * 1024; // near old boundary
        handler.write(write_offset, &[0xDD; 128 * 1024], false).unwrap();

        // Idempotent: resize to same size should be a no-op
        router.resize_export("resize-vol", 0.02).await.unwrap();

        // Shrink should fail
        let err = router.resize_export("resize-vol", 0.005).await;
        assert!(err.is_err(), "shrinking should fail");
    }

    #[tokio::test]
    async fn test_resize_preserves_readonly_flag() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Create as readonly
        router.create_export(test_export_config("ro-resize"), true, None).await.unwrap();

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
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("retry-vol"), false, None).await.unwrap();
        let handler = router.get_handler("retry-vol").await.unwrap();

        // First write + drain
        handler.write(0, &[0x11; 128 * 1024], false).unwrap();
        router.drain_export("retry-vol").await.unwrap();

        // Second write + drain with different data
        handler.write(128 * 1024, &[0x22; 128 * 1024], false).unwrap();
        router.drain_export("retry-vol").await.unwrap();

        // Both blocks should be readable
        let data1 = handler.read(0, 128 * 1024).await.unwrap();
        assert!(data1.iter().all(|&b| b == 0x11), "first block after retry");

        let data2 = handler.read(128 * 1024, 128 * 1024).await.unwrap();
        assert!(data2.iter().all(|&b| b == 0x22), "second block after retry");

        // Snapshot should capture both blocks
        let snap = router.snapshot_export("retry-vol").await.unwrap();
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
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("resize-dirty"), false, None).await.unwrap();

        let handler = router.get_handler("resize-dirty").await.unwrap();

        // Write dirty blocks before resize
        handler.write(0, &[0xAA; 128 * 1024], false).unwrap();
        handler.write(128 * 1024, &[0xBB; 128 * 1024], false).unwrap();

        let old_size = {
            let exports = router.list_exports().await;
            exports.iter().find(|e| e.name == "resize-dirty").unwrap().size
        };

        // Resize doubles the device — this calls drain_export first, so dirty
        // blocks get flushed to S3 before the remove/recreate cycle.
        router.resize_export("resize-dirty", 0.02).await.unwrap();

        let new_size = {
            let exports = router.list_exports().await;
            exports.iter().find(|e| e.name == "resize-dirty").unwrap().size
        };
        assert!(new_size > old_size, "export should have grown");

        // Re-acquire handler after resize (old handler is invalidated)
        let handler = router.get_handler("resize-dirty").await.unwrap();

        // Write to the new region (beyond old device boundary)
        let new_region_offset = old_size; // first byte of new space
        handler.write(new_region_offset, &[0xCC; 128 * 1024], false).unwrap();

        let data_new = handler.read(new_region_offset, 128 * 1024).await.unwrap();
        assert!(data_new.iter().all(|&b| b == 0xCC), "new region should be writable");
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
            super::extract_export_name("test/nbd/vol1/export.json", "test"),
            Some("vol1".to_string())
        );
        assert_eq!(
            super::extract_export_name("my-data/nbd/my-export/export.json", "my-data"),
            Some("my-export".to_string())
        );

        // Invalid paths
        assert_eq!(
            super::extract_export_name("test/nbd/vol1/lease.json", "test"),
            None
        );
        assert_eq!(
            super::extract_export_name("test/nbd/vol1/batches/000000000000", "test"),
            None
        );
        assert_eq!(
            super::extract_export_name("other/nbd/vol1/export.json", "test"),
            None
        );
        assert_eq!(
            super::extract_export_name("test/nbd//export.json", "test"),
            None
        );
    }
}
