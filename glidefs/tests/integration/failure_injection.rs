//! Failure injection tests for GlideFS.
//!
//! These tests verify correct behavior under various failure scenarios:
//! 1. S3 errors during flush (timeout, 503, connection refused)
//! 2. Partial operations and recovery
//! 3. Data integrity under concurrent writes and failures
//!
//! Run with: `cargo test --features test-utils --test integration`

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tempfile::TempDir;

use glidefs::nbd::cache::SimpleBlockCache;
use glidefs::nbd::content_store::ContentStore;
use glidefs::nbd::manifest::Manifest;
use glidefs::nbd::metrics::ExportMetrics;
use glidefs::nbd::pack_index::HostPackIndex;
use glidefs::nbd::state::Active;
use glidefs::nbd::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024;

/// A wrapper around InMemory that can inject failures.
#[derive(Debug)]
struct FailingObjectStore {
    inner: object_store::memory::InMemory,
    /// When true, PUT operations will fail with a simulated error.
    fail_puts: AtomicBool,
    /// When true, GET operations will fail.
    fail_gets: AtomicBool,
    /// Count of PUT operations (for conditional failure).
    put_count: AtomicU32,
    /// Fail after this many PUTs (0 = disabled).
    fail_after_puts: AtomicU32,
}

impl FailingObjectStore {
    fn new() -> Self {
        Self {
            inner: object_store::memory::InMemory::new(),
            fail_puts: AtomicBool::new(false),
            fail_gets: AtomicBool::new(false),
            put_count: AtomicU32::new(0),
            fail_after_puts: AtomicU32::new(0),
        }
    }

    fn set_fail_puts(&self, fail: bool) {
        self.fail_puts.store(fail, Ordering::SeqCst);
    }

    fn set_fail_gets(&self, fail: bool) {
        self.fail_gets.store(fail, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    fn set_fail_after_puts(&self, count: u32) {
        self.fail_after_puts.store(count, Ordering::SeqCst);
        self.put_count.store(0, Ordering::SeqCst);
    }

    fn should_fail_put(&self) -> bool {
        if self.fail_puts.load(Ordering::SeqCst) {
            return true;
        }
        let threshold = self.fail_after_puts.load(Ordering::SeqCst);
        if threshold > 0 {
            let count = self.put_count.fetch_add(1, Ordering::SeqCst) + 1;
            return count >= threshold;
        }
        false
    }
}

impl std::fmt::Display for FailingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingObjectStore")
    }
}

#[async_trait]
impl ObjectStore for FailingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.should_fail_put() {
            return Err(object_store::Error::Generic {
                store: "FailingObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Simulated S3 failure",
                )),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        if self.fail_gets.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "FailingObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Simulated S3 failure",
                )),
            });
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Helper to create a writer cache with the failing object store.
#[allow(clippy::type_complexity)]
fn create_test_cache(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<FailingObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<HostPackIndex>,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: 64 * 1024 * 1024, // 64MB
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let metrics = Arc::new(ExportMetrics::new());
    let content_store = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");
    let pack_index = Arc::new(HostPackIndex::open(temp_dir.path().join("pack_index.redb")).unwrap());
    let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::open(config).expect("Failed to open cache");
    let cache = cache.skip_recovery_for_test();

    (Arc::new(cache), content_store, pack_index, clean_cache, metrics)
}

/// Helper to create a cold reader cache from the manifest in S3.
///
/// After a writer flushes, the manifest in S3 contains the block_map and
/// pack_index entries. This helper downloads the manifest, opens a WriteCache
/// via `open_from_manifest` (which populates the block_map), and rebuilds
/// the HostPackIndex so read_v2 can resolve blocks through S3.
async fn create_reader_from_manifest(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<FailingObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<HostPackIndex>,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    let content_store = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");

    // Fetch manifest from S3
    let manifest_bytes = content_store
        .get_manifest(name)
        .await
        .expect("manifest fetch failed")
        .expect("manifest should exist in S3");
    let manifest =
        Manifest::deserialize(&manifest_bytes).expect("manifest deserialization failed");

    // Rebuild pack_index from manifest
    let pack_index = Arc::new(HostPackIndex::open(temp_dir.path().join("pack_index.redb")).unwrap());
    pack_index.rebuild(std::slice::from_ref(&manifest)).unwrap();

    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: manifest.device_size,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let metrics = Arc::new(ExportMetrics::new());
    let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    // open_from_manifest populates the block_map from the manifest so
    // read_v2 knows which hashes exist at each chunk index.
    let cache = WriteCache::open_from_manifest(config, &manifest, None)
        .expect("Failed to open cache from manifest");

    (Arc::new(cache), content_store, pack_index, clean_cache, metrics)
}

// =============================================================================
// FAILURE INJECTION TESTS
// =============================================================================

/// Test: S3 failure during flush keeps blocks dirty for retry.
///
/// This verifies that transient S3 failures don't cause data loss.
/// When flush_to_s3 fails (pack upload error), dirty flags are never
/// CAS-cleared, so dirty_block_count remains unchanged.
#[tokio::test]
async fn test_s3_failure_during_sync_marks_blocks_dirty() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write some blocks
    for i in 0..5 {
        let data = vec![i as u8; BLOCK_SIZE];
        cache.write(i as u64 * BLOCK_SIZE as u64, &data, clean_cache.as_ref()).unwrap();
    }

    assert_eq!(cache.dirty_block_count(), 5, "Should have 5 dirty blocks");

    // Enable S3 failures
    s3.set_fail_puts(true);

    // Attempt to flush - will fail because pack upload fails.
    // flush_dirty_inner returns Err before CAS-clearing dirty flags.
    let result = cache.flush_to_s3(&content_store, &pack_index).await;
    assert!(result.is_err(), "Flush should fail when S3 is unavailable");

    // Blocks should still be dirty (error returned before CAS-clear step)
    assert_eq!(
        cache.dirty_block_count(),
        5,
        "Blocks should remain dirty after failed flush"
    );

    // Disable failures and retry
    s3.set_fail_puts(false);

    // Now flush should succeed
    let stats = cache
        .flush_to_s3(&content_store, &pack_index)
        .await
        .unwrap();
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "All blocks should be clean after successful flush"
    );
    assert!(
        stats.packs_uploaded > 0,
        "Should have uploaded at least one pack"
    );
}

/// Test: S3 failure during read returns error, not garbage.
///
/// When S3 is unavailable and the block isn't cached locally,
/// reads should fail cleanly rather than returning incorrect data.
#[tokio::test]
async fn test_s3_failure_during_read_returns_error() {
    let s3 = Arc::new(FailingObjectStore::new());

    // First, write data to S3 successfully
    let writer_dir = TempDir::new().unwrap();
    let (writer_cache, writer_content_store, writer_pack_index, writer_clean_cache, _) =
        create_test_cache(&writer_dir, "vol1", Arc::clone(&s3));

    let data = vec![0xAB; BLOCK_SIZE];
    writer_cache.write(0, &data, writer_clean_cache.as_ref()).unwrap();
    writer_cache
        .flush_to_s3(&writer_content_store, &writer_pack_index)
        .await
        .unwrap();
    drop(writer_cache);

    // Create a fresh reader from the manifest. This populates the block_map
    // so read_v2 knows that block 0 has a non-zero hash and will attempt S3.
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    // Enable S3 failures
    s3.set_fail_gets(true);

    // Read should fail (not cached locally, S3 unavailable)
    let result = reader_cache
        .read_v2(
            0,
            BLOCK_SIZE,
            reader_clean_cache.as_ref(),
            &reader_pack_index,
            &reader_content_store,
            &reader_metrics,
        )
        .await;

    assert!(result.is_err(), "Read should fail when S3 is unavailable");

    // Disable failures
    s3.set_fail_gets(false);

    // Now read should succeed
    let result = reader_cache
        .read_v2(
            0,
            BLOCK_SIZE,
            reader_clean_cache.as_ref(),
            &reader_pack_index,
            &reader_content_store,
            &reader_metrics,
        )
        .await;

    assert!(result.is_ok(), "Read should succeed when S3 is available");
    assert_eq!(result.unwrap().as_ref(), &data[..]);
}

/// Test: Write during flush doesn't lose data.
///
/// If a write comes in while we're flushing to S3, the new data should
/// be preserved. The flush CAS-clears only blocks whose hash hasn't changed.
#[tokio::test]
async fn test_write_during_sync_preserves_new_data() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write initial data and flush to S3
    let data_v1 = vec![0x11; BLOCK_SIZE];
    cache.write(0, &data_v1, clean_cache.as_ref()).unwrap();
    cache
        .flush_to_s3(&content_store, &pack_index)
        .await
        .unwrap();

    // Write new data to the same block
    let data_v2 = vec![0x22; BLOCK_SIZE];
    cache.write(0, &data_v2, clean_cache.as_ref()).unwrap();

    // Block should be dirty again with new data
    assert_eq!(cache.dirty_block_count(), 1);

    // Read should return the NEW data locally
    let read = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(read.as_ref(), &data_v2[..], "Should read the newer write");

    // Flush the new data
    cache
        .flush_to_s3(&content_store, &pack_index)
        .await
        .unwrap();

    // Verify S3 has the new data by reading from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    let s3_data = reader_cache
        .read_v2(
            0,
            BLOCK_SIZE,
            reader_clean_cache.as_ref(),
            &reader_pack_index,
            &reader_content_store,
            &reader_metrics,
        )
        .await
        .unwrap();

    assert_eq!(
        s3_data.as_ref(),
        &data_v2[..],
        "S3 should have the newer data"
    );
}

/// Test: Concurrent writes to same block don't cause torn reads.
///
/// Even under concurrent writes, reads should return complete blocks,
/// never a mix of two different writes.
#[tokio::test]
async fn test_concurrent_writes_no_torn_reads() {
    use std::sync::atomic::AtomicUsize;
    use tokio::task::JoinSet;

    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, _content_store, _pack_index, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    let cache = Arc::new(cache);
    let write_count = Arc::new(AtomicUsize::new(0));

    let mut tasks = JoinSet::new();

    // Spawn 10 concurrent writers, each writing a different pattern
    for writer_id in 0..10u8 {
        let cache = Arc::clone(&cache);
        let write_count = Arc::clone(&write_count);
        let clean_cache = Arc::clone(&clean_cache);

        tasks.spawn(async move {
            for _ in 0..100 {
                // Each writer writes its ID as the pattern
                let data = vec![writer_id; BLOCK_SIZE];
                cache.write(0, &data, clean_cache.as_ref()).unwrap();
                write_count.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    // Spawn readers that verify no torn reads
    for _ in 0..5 {
        let cache = Arc::clone(&cache);

        tasks.spawn(async move {
            for _ in 0..200 {
                if let Ok(data) = cache.read_local(0, BLOCK_SIZE) {
                    // All bytes should be the same (from one write)
                    let first = data[0];
                    assert!(
                        data.iter().all(|&b| b == first),
                        "Torn read detected: first byte is {} but found different bytes",
                        first
                    );
                }
                tokio::task::yield_now().await;
            }
        });
    }

    // Wait for all tasks
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    assert!(
        write_count.load(Ordering::Relaxed) >= 1000,
        "Should have completed many writes"
    );
}

/// Test: Zero blocks are not uploaded to S3 (optimization).
///
/// Writing all-zero blocks should not result in pack uploads,
/// as zero blocks are synthesized on read.
#[tokio::test]
async fn test_zero_blocks_not_synced_to_s3() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write zero blocks
    let zeros = vec![0u8; BLOCK_SIZE];
    for i in 0..10 {
        cache.write(i as u64 * BLOCK_SIZE as u64, &zeros, clean_cache.as_ref()).unwrap();
    }

    // Flush to S3
    let stats = cache
        .flush_to_s3(&content_store, &pack_index)
        .await
        .unwrap();

    // With zero-block optimization, no packs should be uploaded
    assert_eq!(
        stats.packs_uploaded, 0,
        "Zero blocks should not result in S3 uploads (optimization)"
    );
}

/// Test: Mixed zero and non-zero blocks in same flush.
///
/// A flush with some zero and some non-zero blocks should only
/// upload the non-zero blocks.
#[tokio::test]
async fn test_mixed_zero_nonzero_batch() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write alternating zero and non-zero blocks
    for i in 0..10 {
        let data = if i % 2 == 0 {
            vec![0u8; BLOCK_SIZE] // Zero block
        } else {
            vec![0xAB; BLOCK_SIZE] // Non-zero block
        };
        cache.write(i as u64 * BLOCK_SIZE as u64, &data, clean_cache.as_ref()).unwrap();
    }

    // Flush
    cache
        .flush_to_s3(&content_store, &pack_index)
        .await
        .unwrap();

    // Verify by reading from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    for i in 0..10 {
        let data = reader_cache
            .read_v2(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index,
                &reader_content_store,
                &reader_metrics,
            )
            .await
            .unwrap();

        let expected = if i % 2 == 0 { 0u8 } else { 0xAB };
        assert!(
            data.iter().all(|&b| b == expected),
            "Block {} should be all {}",
            i,
            expected
        );
    }
}

/// Test: Data integrity after transient failure and recovery.
///
/// Write data, fail during first flush attempt, succeed on retry,
/// verify all data is correct from a fresh node.
#[tokio::test]
async fn test_data_integrity_after_failure_recovery() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write known pattern
    let mut expected_data = Vec::new();
    for i in 0..20u8 {
        let data: Vec<u8> = (0..BLOCK_SIZE).map(|j| i.wrapping_add(j as u8)).collect();
        expected_data.push(data.clone());
        cache.write(i as u64 * BLOCK_SIZE as u64, &data, clean_cache.as_ref()).unwrap();
    }

    // Enable S3 failures and attempt flush
    s3.set_fail_puts(true);
    let result = cache.flush_to_s3(&content_store, &pack_index).await;
    assert!(result.is_err(), "First flush should fail");

    // Blocks should still be dirty
    assert!(
        cache.dirty_block_count() > 0,
        "Blocks should be dirty after failure"
    );

    // Disable failures and flush successfully
    s3.set_fail_puts(false);
    cache
        .flush_to_s3(&content_store, &pack_index)
        .await
        .unwrap();

    // Verify from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    for (i, expected) in expected_data.iter().enumerate() {
        let data = reader_cache
            .read_v2(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index,
                &reader_content_store,
                &reader_metrics,
            )
            .await
            .unwrap();

        assert_eq!(
            data.as_ref(),
            &expected[..],
            "Block {} data mismatch after failure recovery",
            i
        );
    }
}

/// Test: Multiple concurrent flushes don't corrupt data.
///
/// Even if flush_to_s3 is called multiple times concurrently (which shouldn't
/// happen in practice), it should not corrupt data.
#[tokio::test]
async fn test_concurrent_drain_safety() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write data
    for i in 0..10 {
        let data = vec![i as u8; BLOCK_SIZE];
        cache.write(i as u64 * BLOCK_SIZE as u64, &data, clean_cache.as_ref()).unwrap();
    }

    // Wrap in Arc for sharing across tasks (ContentStore doesn't implement
    // Clone, but flush_to_s3 takes &self references)
    let cache = Arc::new(cache);
    let content_store = Arc::new(content_store);

    let mut handles = vec![];
    for _ in 0..3 {
        let cache = Arc::clone(&cache);
        let content_store = Arc::clone(&content_store);
        let pack_index = Arc::clone(&pack_index);
        handles.push(tokio::spawn(async move {
            let _ = cache.flush_to_s3(&content_store, &pack_index).await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All data should be flushed
    assert_eq!(cache.dirty_block_count(), 0);

    // Verify data integrity from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    for i in 0..10 {
        let data = reader_cache
            .read_v2(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index,
                &reader_content_store,
                &reader_metrics,
            )
            .await
            .unwrap();

        let expected = vec![i as u8; BLOCK_SIZE];
        assert_eq!(data.as_ref(), &expected[..], "Block {} corrupted", i);
    }
}

// =============================================================================
// PARTIAL PACK UPLOAD FAILURE
// =============================================================================

/// Test: Partial pack upload failure preserves all dirty blocks for retry.
///
/// When a multi-pack flush fails partway through (e.g., pack 1 of 3 uploads,
/// pack 2 fails), all dirty flags must be preserved so the next flush retries
/// the entire batch. Uses `fail_after_puts` which is wired into FailingObjectStore
/// but was previously unused.
#[tokio::test]
async fn test_partial_pack_upload_preserves_dirty() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write enough blocks for multiple packs (100 blocks per pack, so 250 blocks = 2-3 packs)
    let num_blocks = 250u32;
    for i in 0..num_blocks {
        let data: Vec<u8> = (0..BLOCK_SIZE).map(|j| (i as u8).wrapping_add(j as u8)).collect();
        cache.write(i as u64 * BLOCK_SIZE as u64, &data, clean_cache.as_ref()).unwrap();
    }

    let dirty_before = cache.dirty_block_count();
    assert_eq!(dirty_before, num_blocks as u64, "should have all blocks dirty");

    // Fail after 2 PUTs (first pack upload succeeds, second fails)
    s3.set_fail_after_puts(2);

    // Flush should fail — some packs uploaded, but not all
    let result = cache.flush_to_s3(&content_store, &pack_index).await;
    assert!(result.is_err(), "flush should fail on partial pack upload");

    // All blocks should still be dirty (flush_dirty_inner returns Err before
    // the CAS-clear step, so no dirty flags are cleared)
    assert_eq!(
        cache.dirty_block_count(),
        dirty_before,
        "all blocks should remain dirty after partial failure"
    );

    // Reset failure state and retry — should succeed
    s3.set_fail_after_puts(0);
    s3.set_fail_puts(false);

    let stats = cache
        .flush_to_s3(&content_store, &pack_index)
        .await
        .unwrap();

    assert_eq!(
        cache.dirty_block_count(),
        0,
        "all blocks should be clean after successful retry"
    );
    assert!(stats.packs_uploaded > 0, "retry should upload packs");

    // Verify data integrity from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    for i in 0..num_blocks {
        let data = reader_cache
            .read_v2(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index,
                &reader_content_store,
                &reader_metrics,
            )
            .await
            .unwrap();

        let expected: Vec<u8> = (0..BLOCK_SIZE).map(|j| (i as u8).wrapping_add(j as u8)).collect();
        assert_eq!(
            data.as_ref(),
            &expected[..],
            "Block {} data mismatch after partial failure recovery",
            i
        );
    }
}
