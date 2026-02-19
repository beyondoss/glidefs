//! NBD protocol handlers for block I/O operations.
//!
//! This module provides:
//! - `NBDBlockHandler`: Thin handler that uses WriteCache for all I/O
//! - `NBDDevice`: Device descriptor used during NBD transmission phase

use super::cache::BlockCache;
use super::content_store::ContentStore;
use super::error::{CommandError, CommandResult};
use super::metrics::ExportMetrics;
use super::pack::BLOCKS_PER_PACK;
use super::pack_index::HostPackIndex;
use super::readahead::SequentialDetector;
use super::state::Active;
use super::write_cache::WriteCache;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::Mutex;
use std::time::Instant;
use tokio::sync::Notify;

/// NBD device descriptor used during transmission phase.
#[derive(Clone)]
pub struct NBDDevice {
    pub name: Vec<u8>,
    pub size: u64,
}

/// Handler for NBD protocol operations using write-behind cache.
///
/// This is a thin layer that delegates all I/O to the WriteCache.
/// The key performance benefit: `flush()` only syncs to local SSD,
/// not to S3 (which happens asynchronously in the background).
///
/// Reads use read-through caching: if a block isn't present locally,
/// it's fetched from S3 on demand.
pub struct NBDBlockHandler {
    /// The write-behind cache (must be in Active state)
    cache: Arc<WriteCache<Active>>,

    /// Content-addressed S3 store for v2 pack reads
    content_store: Arc<ContentStore>,

    /// v2 clean block cache (decompressed blocks from S3 packs)
    clean_cache: Arc<dyn BlockCache>,

    /// Host-level pack index for hash→pack location lookup
    pack_index: Arc<HostPackIndex>,

    /// Device size in bytes (atomic for live resize)
    device_size: AtomicU64,

    /// Whether this export is readonly (rejects writes)
    /// Uses AtomicBool so promote_export can change it safely
    readonly: AtomicBool,

    /// I/O metrics for this export
    metrics: Arc<ExportMetrics>,

    /// Sequential read-ahead detector
    readahead: Mutex<SequentialDetector>,

    /// SSD utilization ratio shared from ExportRouter.
    /// Used to reject writes to new blocks when SSD > 95%.
    ssd_utilization: Arc<AtomicU64>,

    /// Notifies the flush scheduler when dirty blocks reach BLOCKS_PER_PACK.
    flush_notify: Arc<Notify>,
}

impl NBDBlockHandler {
    /// Create a new block handler.
    ///
    /// # Arguments
    /// * `cache` - Write-behind cache in Active state
    /// * `device_size` - Size of the block device in bytes
    /// * `readonly` - Whether this export rejects writes
    /// * `metrics` - Shared metrics for tracking I/O statistics
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cache: Arc<WriteCache<Active>>,
        content_store: Arc<ContentStore>,
        clean_cache: Arc<dyn BlockCache>,
        pack_index: Arc<HostPackIndex>,
        device_size: u64,
        readonly: bool,
        metrics: Arc<ExportMetrics>,
        ssd_utilization: Arc<AtomicU64>,
        flush_notify: Arc<Notify>,
    ) -> Self {
        Self {
            cache,
            content_store,
            clean_cache,
            pack_index,
            device_size: AtomicU64::new(device_size),
            readonly: AtomicBool::new(readonly),
            metrics,
            readahead: Mutex::new(SequentialDetector::new()),
            ssd_utilization,
            flush_notify,
        }
    }

    /// Check if this handler is readonly.
    pub fn is_readonly(&self) -> bool {
        self.readonly.load(Ordering::Relaxed)
    }

    /// Set the readonly flag.
    /// Used by promote_export to allow writes after migration.
    pub fn set_readonly(&self, readonly: bool) {
        self.readonly.store(readonly, Ordering::Relaxed);
    }

    /// Get the device size.
    #[inline]
    pub fn device_size(&self) -> u64 {
        self.device_size.load(Ordering::Relaxed)
    }

    /// Set the device size (for resize).
    /// Only safe to call after draining dirty blocks.
    #[allow(dead_code)]
    pub fn set_device_size(&self, new_size: u64) {
        self.device_size.store(new_size, Ordering::Relaxed);
    }

    /// Notify the flush scheduler if dirty blocks have reached pack size.
    #[inline]
    fn check_flush_threshold(&self) {
        if self.cache.dirty_block_count() >= BLOCKS_PER_PACK as u64 {
            self.flush_notify.notify_one();
        }
    }

    // ========================================================================
    // Block I/O Operations
    // ========================================================================

    /// Read data from the cache, fetching from S3 if not present locally.
    ///
    /// Uses read-through caching: blocks not present locally are fetched from S3.
    pub async fn read(&self, offset: u64, length: u32) -> CommandResult<Bytes> {
        let start = Instant::now();

        if offset + length as u64 > self.device_size() {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(Bytes::new());
        }

        self.metrics.record_guest_read(length as u64);

        let data = self
            .cache
            .read_v2(
                offset,
                length as usize,
                self.clean_cache.as_ref(),
                &self.pack_index,
                &self.content_store,
                &self.metrics,
            )
            .await?;

        // Sequential read-ahead: detect patterns and prefetch next pack
        {
            let chunk_size = self.cache.block_size() as u64;
            let chunk_idx = offset / chunk_size;
            if let Some(readahead_chunk) = self.readahead.lock().record(chunk_idx) {
                let cache = Arc::clone(&self.cache);
                let clean_cache = Arc::clone(&self.clean_cache);
                let pack_index = Arc::clone(&self.pack_index);
                let content_store = Arc::clone(&self.content_store);
                tokio::spawn(async move {
                    let _ = cache
                        .prefetch_chunk(
                            readahead_chunk as usize,
                            clean_cache.as_ref(),
                            &pack_index,
                            &content_store,
                        )
                        .await;
                });
            }
        }

        self.metrics.record_read_latency(start.elapsed());
        Ok(data)
    }

    /// Write data to the cache.
    ///
    /// Writes go to local SSD immediately. S3 sync happens in background.
    /// Returns error if the export is readonly or SSD is near-full and
    /// the write touches blocks not yet present on SSD.
    pub fn write(&self, offset: u64, data: &[u8], fua: bool) -> CommandResult<()> {
        let start = Instant::now();

        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset + data.len() as u64 > self.device_size() {
            return Err(CommandError::NoSpace);
        }

        if data.is_empty() {
            return Ok(());
        }

        // Reject writes to new blocks when SSD is near-full.
        // Overwrites to already-present blocks are allowed (no new SSD space).
        const WRITE_REJECT_THRESHOLD: f64 = 0.95;
        let util = f64::from_bits(self.ssd_utilization.load(Ordering::Relaxed));
        if util > WRITE_REJECT_THRESHOLD && self.cache.has_new_blocks(offset, data.len()) {
            return Err(CommandError::NoSpace);
        }

        self.metrics.record_guest_write(data.len() as u64);
        self.cache.write(offset, data, self.clean_cache.as_ref())?;
        self.check_flush_threshold();

        if fua {
            self.flush()?;
        }

        self.metrics.record_write_latency(start.elapsed());
        Ok(())
    }

    /// Trim (discard) a range of blocks.
    ///
    /// Writes zeros to the specified range using optimized platform-specific
    /// methods (fallocate on Linux, static buffer fallback elsewhere).
    /// Returns error if the export is readonly.
    pub fn trim(&self, offset: u64, length: u32, fua: bool) -> CommandResult<()> {
        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset + length as u64 > self.device_size() {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        self.cache.zero_range(offset, length as u64)?;
        self.check_flush_threshold();

        if fua {
            self.flush()?;
        }

        Ok(())
    }

    /// Write zeros to a range.
    ///
    /// Uses optimized platform-specific methods:
    /// - Linux: fallocate(FALLOC_FL_ZERO_RANGE) - no data written, kernel marks as zeros
    /// - Other: Static zero buffer to avoid per-call allocation
    ///
    /// Returns error if the export is readonly.
    pub fn write_zeroes(&self, offset: u64, length: u32, fua: bool) -> CommandResult<()> {
        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset + length as u64 > self.device_size() {
            return Err(CommandError::NoSpace);
        }

        if length == 0 {
            return Ok(());
        }

        self.cache.zero_range(offset, length as u64)?;
        self.check_flush_threshold();

        if fua {
            self.flush()?;
        }

        Ok(())
    }

    /// Cache hint - no-op for our implementation.
    pub fn cache(&self, offset: u64, length: u32) -> CommandResult<()> {
        if offset + length as u64 > self.device_size() {
            return Err(CommandError::InvalidArgument);
        }
        Ok(())
    }

    /// Flush data to durable storage.
    ///
    /// **CRITICAL**: This only flushes to local SSD, not to S3!
    /// This is the key performance optimization - ZFS snapshots/clones
    /// return in <100ms instead of 8-15 seconds.
    ///
    /// S3 sync happens asynchronously in the background via the sync worker.
    pub fn flush(&self) -> CommandResult<()> {
        self.cache.flush().map_err(|_| CommandError::IoError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nbd::cache::SimpleBlockCache;
    use crate::nbd::write_cache::WriteCacheConfig;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    fn test_handler() -> (NBDBlockHandler, TempDir) {
        test_handler_with_readonly(false)
    }

    fn test_handler_with_readonly(readonly: bool) -> (NBDBlockHandler, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "test".to_string(),
            device_size: 1024 * 1024, // 1MB
            block_size: 4096,
            wal_sync: false,
        };

        // Create in-memory S3 store for tests
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // v2 read path components
        let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
        let clean_cache: Arc<dyn BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index = Arc::new(HostPackIndex::open(temp_dir.path().join("pack_index.redb")).unwrap());

        // Create metrics for this handler
        let metrics = Arc::new(ExportMetrics::new());

        // open() returns WriteCache<Recovering>
        let cache = WriteCache::open(config).unwrap();
        // Skip recovery for test - go straight to active
        let cache = cache.skip_recovery_for_test();
        let handler = NBDBlockHandler::new(
            Arc::new(cache),
            content_store,
            clean_cache,
            pack_index,
            1024 * 1024,
            readonly,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::new(Notify::const_new()),
        );

        (handler, temp_dir)
    }

    #[tokio::test]
    async fn test_read_write() {
        let (handler, _temp) = test_handler();

        let data = vec![42u8; 4096];
        handler.write(0, &data, false).unwrap();

        let read_data = handler.read(0, 4096).await.unwrap();
        assert_eq!(read_data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_read_unwritten_returns_zeros() {
        let (handler, _temp) = test_handler();

        // Unwritten blocks should fetch from S3 (empty) and return zeros
        let data = handler.read(0, 1024).await.unwrap();
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_write_beyond_device_size() {
        let (handler, _temp) = test_handler();

        let data = vec![42u8; 4096];
        let result = handler.write(1024 * 1024, &data, false);
        assert!(matches!(result, Err(CommandError::NoSpace)));
    }

    #[test]
    fn test_readonly_rejects_write() {
        let (handler, _temp) = test_handler_with_readonly(true);

        let data = vec![42u8; 4096];
        let result = handler.write(0, &data, false);
        assert!(matches!(result, Err(CommandError::ReadOnly)));
    }

    #[test]
    fn test_readonly_rejects_trim() {
        let (handler, _temp) = test_handler_with_readonly(true);

        let result = handler.trim(0, 4096, false);
        assert!(matches!(result, Err(CommandError::ReadOnly)));
    }

    #[test]
    fn test_readonly_rejects_write_zeroes() {
        let (handler, _temp) = test_handler_with_readonly(true);

        let result = handler.write_zeroes(0, 4096, false);
        assert!(matches!(result, Err(CommandError::ReadOnly)));
    }

    #[tokio::test]
    async fn test_readonly_allows_read() {
        let (handler, _temp) = test_handler_with_readonly(true);

        // Reads should still work on readonly exports
        let result = handler.read(0, 1024).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_promote_readonly_to_readwrite() {
        let (handler, _temp) = test_handler_with_readonly(true);

        // Initially readonly - writes fail
        let data = vec![42u8; 4096];
        assert!(matches!(
            handler.write(0, &data, false),
            Err(CommandError::ReadOnly)
        ));

        // Promote to read-write
        handler.set_readonly(false);
        assert!(!handler.is_readonly());

        // Now writes work
        assert!(handler.write(0, &data, false).is_ok());
    }

    #[tokio::test]
    async fn test_read_beyond_device_size() {
        let (handler, _temp) = test_handler();

        let result = handler.read(1024 * 1024, 4096).await;
        assert!(matches!(result, Err(CommandError::InvalidArgument)));
    }

    #[tokio::test]
    async fn test_flush() {
        let (handler, _temp) = test_handler();

        let data = vec![42u8; 4096];
        handler.write(0, &data, false).unwrap();
        handler.flush().unwrap();

        // Verify data persists
        let read_data = handler.read(0, 4096).await.unwrap();
        assert_eq!(read_data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_write_with_fua() {
        let (handler, _temp) = test_handler();

        let data = vec![42u8; 4096];
        // FUA flag should trigger flush
        handler.write(0, &data, true).unwrap();

        let read_data = handler.read(0, 4096).await.unwrap();
        assert_eq!(read_data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_write_zeroes() {
        let (handler, _temp) = test_handler();

        // Write some data
        let data = vec![42u8; 4096];
        handler.write(0, &data, false).unwrap();

        // Write zeros over it
        handler.write_zeroes(0, 4096, false).unwrap();

        // Verify zeros
        let read_data = handler.read(0, 4096).await.unwrap();
        assert!(read_data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_trim() {
        let (handler, _temp) = test_handler();

        // Write some data
        let data = vec![42u8; 4096];
        handler.write(0, &data, false).unwrap();

        // Trim the region
        handler.trim(0, 4096, false).unwrap();

        // Verify zeros
        let read_data = handler.read(0, 4096).await.unwrap();
        assert!(read_data.iter().all(|&b| b == 0));
    }

}
