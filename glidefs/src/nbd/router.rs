//! Multi-tenant export router for NBD server.
//!
//! Manages multiple NBD exports, each with its own write cache and S3 storage.
//! Supports dynamic export creation/removal for microVM scale-to-zero and live migration.

use crate::config::ExportConfig;
use crate::nbd::block_store::S3BlockStore;
use crate::nbd::handler::NBDBlockHandler;
use crate::nbd::metrics::{ExportMetrics, MetricsSnapshot};
use crate::nbd::state::Active;
use crate::nbd::write_cache::{sync_worker, CacheError, WriteCache, WriteCacheConfig};
use crate::task::spawn_named;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{watch, RwLock};
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
}

/// Information about an export for NBD protocol.
#[derive(Clone, Debug)]
pub struct ExportInfo {
    pub name: String,
    pub size: u64,
    pub readonly: bool,
}

/// State for a single export.
pub struct ExportState {
    pub handler: Arc<NBDBlockHandler>,
    pub cache: Arc<WriteCache<Active>>,
    pub s3_store: Arc<S3BlockStore>,
    pub readonly: bool,
    pub metrics: Arc<ExportMetrics>,
    sync_shutdown: watch::Sender<bool>,
    sync_handle: JoinHandle<()>,
}

impl ExportState {
    /// Drain all dirty blocks to S3.
    pub async fn drain(&self) -> Result<(), RouterError> {
        self.cache
            .drain_for_snapshot(&self.s3_store)
            .await
            .map_err(RouterError::Cache)
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

    /// Auto-create size for on-demand export creation (None = disabled)
    auto_create_size_gb: Option<f64>,
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
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        db_path: String,
        cache_dir: PathBuf,
        block_size: usize,
        blocks_per_batch: u64,
        sync_delay_ms: u64,
        auto_create_size_gb: Option<f64>,
    ) -> Self {
        Self {
            exports: RwLock::new(HashMap::new()),
            object_store,
            db_path,
            cache_dir,
            block_size,
            blocks_per_batch,
            sync_delay_ms,
            auto_create_size_gb,
        }
    }

    /// Get the auto-create size (if enabled).
    pub fn auto_create_size_gb(&self) -> Option<f64> {
        self.auto_create_size_gb
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
    /// **Idempotent**: If export already exists, returns Ok(()) without error.
    pub async fn create_export(
        &self,
        config: ExportConfig,
        readonly: bool,
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
            "Creating export '{}': size={}GB, s3_prefix={}, readonly={}",
            name,
            config.size_gb,
            config.s3_prefix(),
            readonly
        );

        // Create metrics for this export (created first so S3BlockStore can use it)
        let metrics = Arc::new(ExportMetrics::new());

        // Use per-export block size if specified, otherwise use global default
        let block_size = config.block_size_or(self.block_size);

        // Create S3 block store for this export
        // Note: S3BlockStore no longer uses client-side encryption - S3 SSE handles it
        let s3_prefix = format!("{}/nbd/{}", self.db_path, config.s3_prefix());
        let s3_store = Arc::new(
            S3BlockStore::new(Arc::clone(&self.object_store), s3_prefix, block_size)
                .with_blocks_per_batch(self.blocks_per_batch)
                .with_metrics(Arc::clone(&metrics)),
        );

        // Create write cache for this export
        let cache_config = WriteCacheConfig {
            cache_dir: self.cache_dir.clone(),
            device_name: name.clone(),
            device_size: config.size_bytes(),
            block_size,
        };

        let cache = WriteCache::open(cache_config)?;

        info!("Recovering write cache for export '{}'...", name);
        let cache = cache.finish_recovery(&s3_store).await?;
        let cache = Arc::new(cache);
        info!("Export '{}' cache ready", name);

        // Create handler with S3 store for read-through caching
        let handler = Arc::new(NBDBlockHandler::new(
            Arc::clone(&cache),
            Arc::clone(&s3_store),
            config.size_bytes(),
            readonly,
            Arc::clone(&metrics),
        ));

        // Start sync worker for this export
        let (sync_shutdown_tx, sync_shutdown_rx) = watch::channel(false);
        let sync_cache = Arc::clone(&cache);
        let sync_s3 = Arc::clone(&s3_store);
        let export_name = name.clone();
        let sync_config = super::write_cache::SyncWorkerConfig {
            hot_batch_cooldown: std::time::Duration::from_millis(self.sync_delay_ms),
            ..Default::default()
        };
        let sync_handle = spawn_named(&format!("sync-{}", name), async move {
            sync_worker(sync_cache, sync_s3, sync_config, sync_shutdown_rx).await;
            info!("Sync worker for export '{}' stopped", export_name);
        });

        // Store export state
        let state = ExportState {
            handler,
            cache,
            s3_store,
            readonly,
            metrics,
            sync_shutdown: sync_shutdown_tx,
            sync_handle,
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
        };

        self.create_export(config, readonly).await?;

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

        // 1. Signal sync worker to stop
        let _ = state.sync_shutdown.send(true);

        // 2. Wait for sync worker to exit (releases its Arc clone)
        if let Err(e) = state.sync_handle.await {
            warn!("Sync worker for '{}' panicked: {}", name, e);
        }

        // 3. Drop the handler (releases its Arc clone)
        drop(state.handler);

        // 6. Unwrap the Arc and transition through Draining state
        match Arc::try_unwrap(state.cache) {
            Ok(cache) => {
                match cache.shutdown(&state.s3_store).await {
                    Ok(draining) => {
                        draining.finish();
                        info!("Export '{}' drained and removed cleanly", name);
                    }
                    Err(e) => {
                        warn!("Failed to drain export '{}': {}", name, e);
                    }
                }
            }
            Err(arc) => {
                // Fallback if someone still holds a reference
                warn!(
                    "Export '{}' has {} references, using fallback drain",
                    name,
                    Arc::strong_count(&arc)
                );
                if let Err(e) = arc.drain_for_snapshot(&state.s3_store).await {
                    warn!("Failed to drain export '{}': {}", name, e);
                }
            }
        }

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

            // 1. Signal sync worker to stop
            let _ = state.sync_shutdown.send(true);

            // 2. Wait for sync worker to exit (releases its Arc clone)
            if let Err(e) = state.sync_handle.await {
                warn!("Sync worker for '{}' panicked: {}", name, e);
            }

            // 3. Drop the handler (releases its Arc clone)
            drop(state.handler);

            // 4. Now we should be the only holder - unwrap the Arc
            match Arc::try_unwrap(state.cache) {
                Ok(cache) => {
                    // 7. Transition Active → Draining via shutdown()
                    match cache.shutdown(&state.s3_store).await {
                        Ok(draining) => {
                            // 8. Finish draining
                            draining.finish();
                            info!("Export '{}' shut down cleanly", name);
                        }
                        Err(e) => {
                            warn!("Failed to drain export '{}': {}", name, e);
                        }
                    }
                }
                Err(arc) => {
                    // Someone still holds a reference - fall back to drain_for_snapshot
                    warn!(
                        "Export '{}' has {} references, using fallback drain",
                        name,
                        Arc::strong_count(&arc)
                    );
                    if let Err(e) = arc.drain_for_snapshot(&state.s3_store).await {
                        warn!("Failed to drain export '{}': {}", name, e);
                    }
                }
            }
        }

        info!("All exports shut down");
        Ok(())
    }

    /// Create a minimal router for testing protocol handling.
    /// Uses a temporary directory and in-memory S3.
    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
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
            None,       // No auto-create in tests
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
            None,       // No auto-create in tests
        )
    }

    fn test_export_config(name: &str) -> ExportConfig {
        ExportConfig {
            name: name.to_string(),
            size_gb: 0.01, // 10MB
            s3_prefix: None,
            block_size: None,
        }
    }

    #[tokio::test]
    async fn test_create_export_success() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        let result = router.create_export(test_export_config("vol1"), false).await;
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
        router.create_export(test_export_config("vol1"), false).await.unwrap();
        let result = router.create_export(test_export_config("vol1"), false).await;

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

        router.create_export(test_export_config("vol1"), true).await.unwrap();

        let exports = router.list_exports().await;
        assert_eq!(exports.len(), 1);
        assert!(exports[0].readonly, "Export should be readonly");
    }

    #[tokio::test]
    async fn test_get_handler_existing() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        router.create_export(test_export_config("vol1"), false).await.unwrap();

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

        router.create_export(test_export_config("vol1"), false).await.unwrap();
        router.create_export(test_export_config("vol2"), true).await.unwrap();
        router.create_export(test_export_config("vol3"), false).await.unwrap();

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

        router.create_export(test_export_config("vol1"), false).await.unwrap();

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

        router.create_export(test_export_config("vol1"), false).await.unwrap();
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

        router.create_export(test_export_config("vol1"), false).await.unwrap();

        // Write some data to create cache files
        let handler = router.get_handler("vol1").await.unwrap();
        handler.write(0, &[0xAB; 4096], false).unwrap();

        // Remove with purge
        router.remove_export("vol1", true).await.unwrap();

        // Cache files should be deleted (we can verify by trying to re-create)
        router.create_export(test_export_config("vol1"), false).await.unwrap();
        // Should succeed without "file exists" errors
    }

    #[tokio::test]
    async fn test_promote_export_success() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Create readonly export
        router.create_export(test_export_config("vol1"), true).await.unwrap();

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

        router.create_export(test_export_config("vol1"), false).await.unwrap();

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

        router.create_export(test_export_config("vol1"), false).await.unwrap();
        router.create_export(test_export_config("vol2"), false).await.unwrap();

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

        router.create_export(test_export_config("vol1"), false).await.unwrap();
        router.create_export(test_export_config("vol2"), false).await.unwrap();

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
            },
            ExportConfig {
                name: "discover-vol2".to_string(),
                size_gb: 2.0,
                s3_prefix: None,
                block_size: None,
            },
            ExportConfig {
                name: "discover-vol3".to_string(),
                size_gb: 3.0,
                s3_prefix: None,
                block_size: None,
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
        router.create_export(test_export_config("auto-persist"), false).await.unwrap();

        // Verify it was persisted
        let loaded = router.load_export("auto-persist").await.unwrap();
        assert!(loaded.is_some(), "Export should be persisted to S3");
    }

    #[tokio::test]
    async fn test_remove_export_with_purge_deletes_from_s3() {
        let temp_dir = TempDir::new().unwrap();
        let router = create_test_router(&temp_dir);

        // Create export
        router.create_export(test_export_config("purge-vol"), false).await.unwrap();

        // Verify persisted
        let loaded = router.load_export("purge-vol").await.unwrap();
        assert!(loaded.is_some(), "Should be persisted");

        // Remove with purge
        router.remove_export("purge-vol", true).await.unwrap();

        // Verify deleted from S3
        let loaded = router.load_export("purge-vol").await.unwrap();
        assert!(loaded.is_none(), "Should be deleted from S3");
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
