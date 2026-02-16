//! Batched block-to-S3 storage for NBD devices.
//!
//! Blocks are grouped into fixed-size batches to reduce S3 PUT costs.
//! Each batch contains `BLOCKS_PER_BATCH` consecutive blocks stored as a
//! single S3 object. Reads use range requests to fetch individual blocks.

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, instrument, warn};

use super::metrics::ExportMetrics;

/// Result of fetching a batch, including ETag for conditional PUT.
#[derive(Debug)]
pub struct BatchData {
    /// The batch contents (exactly `batch_size` bytes)
    pub data: Vec<u8>,
    /// ETag from S3 for conditional PUT (None if batch was newly created)
    pub etag: Option<String>,
}

/// Default block size: 128KB (matches ZFS default recordsize)
pub const DEFAULT_BLOCK_SIZE: usize = 128 * 1024;

/// Default number of blocks per batch.
/// 25 blocks × 128KB = 3.2MB per batch object.
/// Smaller batches reduce storage overhead for sparse data while
/// keeping API costs reasonable. See ARCHITECTURE.md for analysis.
pub const DEFAULT_BLOCKS_PER_BATCH: u64 = 25;

/// Errors that can occur during block store operations.
#[derive(Error, Debug)]
#[allow(dead_code)] // Public API for library consumers
pub enum BlockStoreError {
    #[error("S3 operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Block not found: {0}")]
    NotFound(u64),
}

/// Batched block storage backed by S3.
///
/// Blocks are stored in batches at `{prefix}/batches/{batch_num:012x}`.
/// Each batch is a fixed-size object containing `blocks_per_batch` consecutive blocks.
///
/// This reduces S3 PUT costs by ~10x compared to individual block storage.
///
/// **Encryption**: Uses S3 Server-Side Encryption (SSE) instead of client-side encryption.
/// This allows range requests to work correctly for individual block reads.
pub struct S3BlockStore {
    /// Underlying object store (S3, MinIO, etc.)
    object_store: Arc<dyn ObjectStore>,

    /// Path prefix for all batches (e.g., "glidefs/myvolume")
    prefix: String,

    /// Block size in bytes
    block_size: usize,

    /// Number of blocks per batch
    blocks_per_batch: u64,

    /// Optional metrics for tracking I/O statistics
    metrics: Option<Arc<ExportMetrics>>,
}

impl S3BlockStore {
    /// Create a new block store.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        block_size: usize,
    ) -> Self {
        Self {
            object_store,
            prefix: prefix.into(),
            block_size,
            blocks_per_batch: DEFAULT_BLOCKS_PER_BATCH,
            metrics: None,
        }
    }

    /// Set the metrics instance for this block store.
    pub fn with_metrics(mut self, metrics: Arc<ExportMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Set the number of blocks per batch.
    pub fn with_blocks_per_batch(mut self, blocks_per_batch: u64) -> Self {
        self.blocks_per_batch = blocks_per_batch;
        self
    }

    /// Create a new block store with default settings.
    #[allow(dead_code)]
    pub fn with_defaults(object_store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self::new(object_store, prefix, DEFAULT_BLOCK_SIZE)
    }

    /// Get the block size.
    #[inline]
    #[allow(dead_code)] // Public accessor for inspecting configuration
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Get the number of blocks per batch.
    #[inline]
    pub fn blocks_per_batch(&self) -> u64 {
        self.blocks_per_batch
    }

    /// Calculate the size of a batch in bytes.
    #[inline]
    pub fn batch_size(&self) -> usize {
        (self.blocks_per_batch as usize) * self.block_size
    }

    /// Generate the S3 key for a batch.
    ///
    /// Format: `{prefix}/batches/{batch_num:012x}`
    #[inline]
    pub fn batch_key(&self, batch_num: u64) -> Path {
        Path::from(format!("{}/batches/{:012x}", self.prefix, batch_num))
    }

    /// Calculate which batch a block belongs to.
    #[inline]
    pub fn batch_num(&self, block_num: u64) -> u64 {
        block_num / self.blocks_per_batch
    }

    /// Calculate the byte offset of a block within its batch.
    #[inline]
    pub fn offset_in_batch(&self, block_num: u64) -> u64 {
        (block_num % self.blocks_per_batch) * self.block_size as u64
    }

    /// Read a single block from S3 using a range request into its batch.
    ///
    /// Returns NotFound if the batch doesn't exist.
    /// S3 SSE handles decryption transparently - no client-side decryption needed.
    #[allow(dead_code)] // Public API for library consumers
    #[instrument(skip(self), fields(block = block_num))]
    pub async fn read_block(&self, block_num: u64) -> Result<Bytes, BlockStoreError> {
        let batch_num = self.batch_num(block_num);
        let offset = self.offset_in_batch(block_num);
        let key = self.batch_key(batch_num);

        let range = offset..offset + self.block_size as u64;
        let result = self.object_store.get_range(&key, range).await;

        match result {
            Ok(data) => {
                debug!(block = block_num, batch = batch_num, "read block from batch");
                Ok(data)
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(block = block_num, batch = batch_num, "batch not found, returning NotFound");
                Err(BlockStoreError::NotFound(block_num))
            }
            Err(e) => Err(BlockStoreError::ObjectStore(e)),
        }
    }

    /// Get an existing batch or create an empty one.
    ///
    /// Returns a mutable buffer of `batch_size()` bytes.
    /// For conditional PUT support, use `get_batch_with_etag` instead.
    #[allow(dead_code)] // Public API convenience method, used by tests
    #[instrument(skip(self), fields(batch = batch_num))]
    pub async fn get_batch_or_empty(&self, batch_num: u64) -> Result<Vec<u8>, BlockStoreError> {
        Ok(self.get_batch_with_etag(batch_num).await?.data)
    }

    /// Get an existing batch with its ETag for conditional PUT.
    ///
    /// Returns `BatchData` containing:
    /// - `data`: The batch contents (exactly `batch_size` bytes, zeros if new)
    /// - `etag`: The S3 ETag for conditional PUT (None if batch doesn't exist yet)
    #[instrument(skip(self), fields(batch = batch_num))]
    pub async fn get_batch_with_etag(&self, batch_num: u64) -> Result<BatchData, BlockStoreError> {
        let key = self.batch_key(batch_num);
        let batch_size = self.batch_size();

        match self.object_store.get(&key).await {
            Ok(response) => {
                let etag = response.meta.e_tag.clone();
                let data = response.bytes().await?;
                debug!(batch = batch_num, size = data.len(), etag = ?etag, "fetched existing batch");
                // Ensure we have exactly batch_size bytes
                let mut buf = data.to_vec();
                buf.resize(batch_size, 0);
                Ok(BatchData { data: buf, etag })
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(batch = batch_num, "batch not found, creating empty");
                Ok(BatchData {
                    data: vec![0u8; batch_size],
                    etag: None,
                })
            }
            Err(e) => Err(BlockStoreError::ObjectStore(e)),
        }
    }

    /// Write a batch to S3 (unconditional).
    ///
    /// The data must be exactly `batch_size()` bytes.
    /// For defense-in-depth against concurrent writers, use `put_batch_conditional`.
    #[allow(dead_code)] // Public API convenience method, used by tests
    #[instrument(skip(self, data), fields(batch = batch_num, size = data.len()))]
    pub async fn put_batch(&self, batch_num: u64, data: Vec<u8>) -> Result<(), BlockStoreError> {
        debug_assert_eq!(data.len(), self.batch_size());

        let key = self.batch_key(batch_num);
        let size = data.len() as u64;

        let payload = PutPayload::from(data);
        self.object_store.put(&key, payload).await?;

        if let Some(metrics) = &self.metrics {
            metrics.record_batch_write(size);
        }

        debug!(batch = batch_num, "wrote batch (unconditional)");
        Ok(())
    }

    /// Write a batch to S3 with conditional PUT.
    ///
    /// Provides defense-in-depth against concurrent writers:
    ///
    /// - If `etag` is `Some`: Uses If-Match, fails if ETag doesn't match (batch was modified)
    /// - If `etag` is `None`: Uses Create mode, fails if batch already exists (race condition)
    ///
    /// On failure, the sync worker marks blocks as dirty for retry. The retry will
    /// GET the batch again (getting the current ETag), merge changes, and PUT conditionally.
    ///
    /// Returns `Err(ObjectStore(Precondition))` on ETag mismatch.
    /// Returns `Err(ObjectStore(AlreadyExists))` if batch was created by another writer.
    #[instrument(skip(self, data), fields(batch = batch_num, size = data.len(), etag = ?etag))]
    pub async fn put_batch_conditional(
        &self,
        batch_num: u64,
        data: Vec<u8>,
        etag: Option<String>,
    ) -> Result<(), BlockStoreError> {
        debug_assert_eq!(data.len(), self.batch_size());

        let key = self.batch_key(batch_num);
        let size = data.len() as u64;

        let put_opts = match etag {
            Some(ref e) => {
                // Conditional PUT: only succeed if ETag matches
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: Some(e.clone()),
                        version: None,
                    }),
                    ..Default::default()
                }
            }
            None => {
                // New batch: only succeed if it doesn't exist yet
                // This prevents race where two nodes both try to create the same batch
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                }
            }
        };

        let payload = PutPayload::from(data);
        match self.object_store.put_opts(&key, payload, put_opts).await {
            Ok(_) => {
                // Record metrics if available
                if let Some(metrics) = &self.metrics {
                    metrics.record_batch_write(size);
                }
                debug!(batch = batch_num, "wrote batch");
                Ok(())
            }
            Err(object_store::Error::Precondition { .. }) => {
                warn!(
                    batch = batch_num,
                    "conditional PUT failed: batch was modified by another writer"
                );
                Err(BlockStoreError::ObjectStore(
                    object_store::Error::Precondition {
                        path: key.to_string(),
                        source: "ETag mismatch - concurrent modification detected".into(),
                    },
                ))
            }
            Err(object_store::Error::AlreadyExists { path, source }) => {
                warn!(
                    batch = batch_num,
                    "conditional PUT failed: batch was created by another writer"
                );
                Err(BlockStoreError::ObjectStore(
                    object_store::Error::AlreadyExists { path, source },
                ))
            }
            Err(e) => Err(BlockStoreError::ObjectStore(e)),
        }
    }

    /// Delete a batch from S3.
    #[allow(dead_code)]
    #[instrument(skip(self), fields(batch = batch_num))]
    pub async fn delete_batch(&self, batch_num: u64) -> Result<(), BlockStoreError> {
        let key = self.batch_key(batch_num);
        self.object_store.delete(&key).await?;
        debug!(batch = batch_num, "deleted batch");
        Ok(())
    }

    /// List all batch numbers in the store.
    #[allow(dead_code)]
    #[instrument(skip(self))]
    pub async fn list_batches(&self) -> Result<Vec<u64>, BlockStoreError> {
        use futures::StreamExt;

        let prefix = Path::from(format!("{}/batches/", self.prefix));
        let mut batch_nums = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));

        while let Some(result) = stream.next().await {
            let meta = result?;
            if let Some(filename) = meta.location.filename()
                && let Ok(batch_num) = u64::from_str_radix(filename, 16)
            {
                batch_nums.push(batch_num);
            }
        }

        batch_nums.sort_unstable();
        debug!(count = batch_nums.len(), "listed batches");
        Ok(batch_nums)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn test_store() -> S3BlockStore {
        let object_store = Arc::new(InMemory::new());
        S3BlockStore::with_defaults(object_store, "test").with_blocks_per_batch(10) // Small batches for testing
    }

    #[test]
    fn test_batch_calculations() {
        let store = test_store();

        // With 10 blocks per batch:
        // Block 0-9 -> Batch 0
        // Block 10-19 -> Batch 1
        assert_eq!(store.batch_num(0), 0);
        assert_eq!(store.batch_num(9), 0);
        assert_eq!(store.batch_num(10), 1);
        assert_eq!(store.batch_num(99), 9);

        // Offset within batch (with 128KB blocks)
        assert_eq!(store.offset_in_batch(0), 0);
        assert_eq!(store.offset_in_batch(1), 128 * 1024u64);
        assert_eq!(store.offset_in_batch(10), 0); // First block of batch 1
        assert_eq!(store.offset_in_batch(11), 128 * 1024u64);
    }

    #[test]
    fn test_batch_key_format() {
        let store = test_store();

        assert_eq!(store.batch_key(0).to_string(), "test/batches/000000000000");
        assert_eq!(store.batch_key(1).to_string(), "test/batches/000000000001");
        assert_eq!(
            store.batch_key(0xABCDEF).to_string(),
            "test/batches/000000abcdef"
        );
    }

    #[tokio::test]
    async fn test_read_nonexistent_batch() {
        let store = test_store();

        // Reading from non-existent batch returns NotFound
        let result = store.read_block(0).await;
        assert!(matches!(result, Err(BlockStoreError::NotFound(0))));
    }

    #[tokio::test]
    async fn test_get_batch_or_empty() {
        let store = test_store();

        // Non-existent batch returns zeros
        let batch = store.get_batch_or_empty(0).await.unwrap();
        assert_eq!(batch.len(), store.batch_size());
        assert!(batch.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_write_and_read_batch() {
        let store = test_store();
        let batch_size = store.batch_size();

        // Create a batch with some data
        let mut batch_data = vec![0u8; batch_size];
        // Write "hello" at block 0's position
        batch_data[..5].copy_from_slice(b"hello");
        // Write "world" at block 1's position
        let offset = store.block_size();
        batch_data[offset..offset + 5].copy_from_slice(b"world");

        // Write the batch
        store.put_batch(0, batch_data).await.unwrap();

        // Read block 0
        let block0 = store.read_block(0).await.unwrap();
        assert_eq!(&block0[..5], b"hello");

        // Read block 1
        let block1 = store.read_block(1).await.unwrap();
        assert_eq!(&block1[..5], b"world");

        // Read block 2 (should be zeros)
        let block2 = store.read_block(2).await.unwrap();
        assert!(block2.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_update_batch() {
        let store = test_store();

        // Get empty batch, modify, write
        let mut batch = store.get_batch_or_empty(0).await.unwrap();
        batch[0] = 42;
        store.put_batch(0, batch).await.unwrap();

        // Read it back
        let block = store.read_block(0).await.unwrap();
        assert_eq!(block[0], 42);

        // Update the batch (GET-modify-PUT)
        let mut batch = store.get_batch_or_empty(0).await.unwrap();
        assert_eq!(batch[0], 42); // Previous value preserved
        batch[1] = 99;
        store.put_batch(0, batch).await.unwrap();

        // Both values should be there
        let block = store.read_block(0).await.unwrap();
        assert_eq!(block[0], 42);
        assert_eq!(block[1], 99);
    }

    #[tokio::test]
    async fn test_list_batches() {
        let store = test_store();
        let batch_size = store.batch_size();

        // Write some batches
        store.put_batch(0, vec![1u8; batch_size]).await.unwrap();
        store.put_batch(5, vec![2u8; batch_size]).await.unwrap();
        store.put_batch(10, vec![3u8; batch_size]).await.unwrap();

        let batches = store.list_batches().await.unwrap();
        assert_eq!(batches, vec![0, 5, 10]);
    }

    #[tokio::test]
    async fn test_delete_batch() {
        let store = test_store();
        let batch_size = store.batch_size();

        store.put_batch(0, vec![1u8; batch_size]).await.unwrap();
        assert!(store.read_block(0).await.is_ok());

        store.delete_batch(0).await.unwrap();
        assert!(matches!(
            store.read_block(0).await,
            Err(BlockStoreError::NotFound(0))
        ));
    }
}
