//! Multi-tenant export router for NBD server.
//!
//! Manages multiple NBD exports, each with its own write cache and S3 storage.
//! Supports dynamic export creation/removal for microVM scale-to-zero and live migration.

use crate::config::ExportConfig;
use crate::nbd::block_store::S3BlockStore;
use crate::nbd::cache::BlockCache;
use crate::nbd::content_store::ContentStore;
use crate::nbd::flush_scheduler::{flush_scheduler, FlushMode};
use crate::nbd::handler::NBDBlockHandler;
use crate::nbd::manifest::{deserialize_hot_set, Manifest};
use crate::nbd::metrics::{ExportMetrics, MetricsSnapshot};
use crate::nbd::pack::PackLocation;
use crate::nbd::pack_index::HostPackIndex;
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
    pub s3_store: Arc<S3BlockStore>,
    pub content_store: Arc<ContentStore>,
    pub pack_index: Arc<HostPackIndex>,
    pub readonly: bool,
    pub metrics: Arc<ExportMetrics>,
    flush_mode_tx: watch::Sender<FlushMode>,
    flush_trigger: Arc<Notify>,
    flush_shutdown_tx: watch::Sender<bool>,
    flush_handle: JoinHandle<()>,
}

impl ExportState {
    /// Drain all dirty blocks to S3 via v2 content-addressed packs.
    pub async fn drain(&self) -> Result<(), RouterError> {
        // Loop until no more dirty blocks remain (concurrent writes may
        // produce new dirty data between flushes).
        loop {
            let stats = self
                .cache
                .flush_to_s3(&self.content_store, &self.pack_index)
                .await
                .map_err(RouterError::Cache)?;
            if stats.blocks_flushed == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Get the current flush mode.
    pub fn flush_mode(&self) -> FlushMode {
        self.flush_mode_tx.borrow().clone()
    }
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

    /// Number of blocks per S3 batch
    blocks_per_batch: u64,

    /// Sync delay in milliseconds (time to coalesce writes before S3 upload)
    sync_delay_ms: u64,

    /// Dirty budget in GB per export (triggers flush when exceeded)
    dirty_budget_gb: f64,

    /// Auto-create size for on-demand export creation (None = disabled)
    auto_create_size_gb: Option<f64>,

    /// Host-level pack index shared across all exports (content-addressed dedup)
    pack_index: Arc<HostPackIndex>,

    /// Shared clean cache across all exports (content-addressed dedup)
    clean_cache: Arc<dyn BlockCache>,
}

impl ExportRouter {
    /// Create a new export router.
    ///
    /// # Arguments
    /// * `object_store` - S3/MinIO/etc backend
    /// * `db_path` - Base path prefix in object store
    /// * `cache_dir` - Local directory for write cache
    /// * `block_size` - Block size in bytes
    /// * `blocks_per_batch` - Number of blocks per S3 batch object
    /// * `sync_delay_ms` - Delay before syncing writes to S3
    /// * `auto_create_size_gb` - Size for auto-created exports (None = disabled)
    /// * `clean_cache` - Shared block cache for decompressed block data
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        db_path: String,
        cache_dir: PathBuf,
        block_size: usize,
        blocks_per_batch: u64,
        sync_delay_ms: u64,
        dirty_budget_gb: f64,
        auto_create_size_gb: Option<f64>,
        clean_cache: Arc<dyn BlockCache>,
    ) -> Self {
        Self {
            exports: RwLock::new(HashMap::new()),
            object_store,
            db_path,
            cache_dir,
            block_size,
            blocks_per_batch,
            sync_delay_ms,
            dirty_budget_gb,
            auto_create_size_gb,
            pack_index: Arc::new(HostPackIndex::new()),
            clean_cache,
        }
    }

    /// Get the auto-create size (if enabled).
    pub fn auto_create_size_gb(&self) -> Option<f64> {
        self.auto_create_size_gb
    }

    /// Get a reference to the shared pack index (for scrubber).
    pub fn pack_index(&self) -> &Arc<HostPackIndex> {
        &self.pack_index
    }

    /// Get a reference to the shared clean cache (for scrubber).
    pub fn clean_cache(&self) -> &Arc<dyn BlockCache> {
        &self.clean_cache
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

        // Create S3 block store for this export
        let s3_prefix = format!("{}/nbd/{}", self.db_path, config.s3_prefix());
        let s3_store = Arc::new(
            S3BlockStore::new(Arc::clone(&self.object_store), s3_prefix.clone(), block_size)
                .with_blocks_per_batch(self.blocks_per_batch)
                .with_metrics(Arc::clone(&metrics)),
        );

        // v2 read path components (shared pack_index from router)
        let content_store = Arc::new(ContentStore::new(
            Arc::clone(&self.object_store),
            &s3_prefix,
        ));
        let clean_cache = Arc::clone(&self.clean_cache);
        let pack_index = Arc::clone(&self.pack_index);

        // Flush trigger shared between cache write path and scheduler
        let flush_trigger = Arc::new(Notify::new());
        let dirty_budget_bytes = (config.dirty_budget_gb
            .unwrap_or(self.dirty_budget_gb) * 1024.0 * 1024.0 * 1024.0) as u64;

        // Create write cache — either from manifest (fork) or fresh (normal)
        let cache_config = WriteCacheConfig {
            cache_dir: self.cache_dir.clone(),
            device_name: name.clone(),
            device_size: config.size_bytes(),
            block_size,
            dirty_budget_bytes,
            flush_trigger: Some(Arc::clone(&flush_trigger)),
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

            info!("Export '{}' created from manifest (fork)", name);
            Arc::new(cache)
        } else {
            // Normal path: open cache, recover from WAL
            let cache = WriteCache::open(cache_config)?;
            info!("Recovering write cache for export '{}'...", name);
            let cache = cache.finish_recovery(&s3_store).await?;
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
        let flush_handle = spawn_named(&format!("flush-{}", name), async move {
            flush_scheduler(flush_cache, flush_cs, flush_pi, flush_mode_rx, flush_trig, flush_shutdown_rx).await;
            info!("Flush scheduler for export '{}' stopped", export_name);
        });

        // Store export state
        let state = ExportState {
            handler,
            cache,
            s3_store,
            content_store,
            pack_index,
            readonly,
            metrics,
            flush_mode_tx,
            flush_trigger,
            flush_shutdown_tx,
            flush_handle,
        };

        let mut exports = self.exports.write().await;
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
        let exports = self.exports.read().await;
        let state = exports
            .get(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;

        info!("Draining export '{}'...", name);
        state.drain().await?;
        info!("Export '{}' drained successfully", name);
        Ok(())
    }

    /// Drain all exports.
    pub async fn drain_all(&self) -> Result<(), RouterError> {
        let names = self.list_export_names().await;
        for name in names {
            if let Err(e) = self.drain_export(&name).await {
                warn!("Failed to drain export '{}': {}", name, e);
            }
        }
        Ok(())
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

        // Recreate with new size
        let config = ExportConfig {
            name: name.to_string(),
            size_gb: new_size_gb,
            s3_prefix: None, // Will use name as prefix (same as before)
            block_size: Some(block_size),
            flush_mode: None,
            dirty_budget_gb: None,
        };

        self.create_export(config, readonly, None).await?;

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
            s3_store,
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
        loop {
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
                match cache.shutdown(&s3_store).await {
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

        Self::new(
            s3,
            "test".to_string(),
            temp_dir,
            128 * 1024, // 128KB blocks
            10,         // 10 blocks per batch
            100,        // 100ms sync delay
            5.0,        // 5GB dirty budget
            None,       // No auto-create in tests
            Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
        )
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
        ExportRouter::new(
            s3,
            "test".to_string(),
            temp_dir.path().to_path_buf(),
            128 * 1024, // 128KB blocks
            10,         // 10 blocks per batch
            100,        // 100ms sync delay
            5.0,        // 5GB dirty budget
            None,       // No auto-create in tests
            Arc::new(SimpleBlockCache::new(256 * 1024 * 1024)),
        )
    }

    fn test_export_config(name: &str) -> ExportConfig {
        ExportConfig {
            name: name.to_string(),
            size_gb: 0.01, // 10MB
            s3_prefix: None,
            block_size: None,
            flush_mode: None,
            dirty_budget_gb: None,
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
                dirty_budget_gb: None,
            },
            ExportConfig {
                name: "discover-vol2".to_string(),
                size_gb: 2.0,
                s3_prefix: None,
                block_size: None,
                flush_mode: None,
                dirty_budget_gb: None,
            },
            ExportConfig {
                name: "discover-vol3".to_string(),
                size_gb: 3.0,
                s3_prefix: None,
                block_size: None,
                flush_mode: None,
                dirty_budget_gb: None,
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
            dirty_budget_gb: None,
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
            dirty_budget_gb: None,
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
            dirty_budget_gb: None,
        };

        let result = router.create_export(config, false, Some("does-not-exist")).await;
        assert!(result.is_err(), "Fork from missing manifest should fail");
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
