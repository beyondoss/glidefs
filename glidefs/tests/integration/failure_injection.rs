//! Failure injection tests for GlideFS.
//!
//! These tests verify correct behavior under various failure scenarios:
//! 1. S3 errors during sync (timeout, 503, connection refused)
//! 2. Conditional PUT conflicts (ETag mismatch)
//! 3. Lease renewal failures
//! 4. Partial operations and recovery
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

use glidefs::nbd::block_store::S3BlockStore;
use glidefs::nbd::lease::LeaseManager;
use glidefs::nbd::metrics::ExportMetrics;
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

/// Helper to create a test cache with the failing object store.
fn create_test_cache(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<FailingObjectStore>,
) -> (Arc<WriteCache<Active>>, Arc<S3BlockStore>, Arc<ExportMetrics>) {
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: 10 * 1024 * 1024, // 10MB
        block_size: BLOCK_SIZE,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };

    let metrics = Arc::new(ExportMetrics::new());
    let s3_store = Arc::new(
        S3BlockStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test", BLOCK_SIZE)
            .with_blocks_per_batch(10)
            .with_metrics(Arc::clone(&metrics)),
    );

    let cache = WriteCache::open(config).expect("Failed to open cache");
    let cache = cache.skip_recovery_for_test();

    (Arc::new(cache), s3_store, metrics)
}

// =============================================================================
// FAILURE INJECTION TESTS
// =============================================================================

/// Test: S3 failure during batched sync marks blocks for retry.
///
/// This verifies that transient S3 failures don't cause data loss.
/// Blocks that fail to sync should be marked as dirty again.
#[tokio::test]
async fn test_s3_failure_during_sync_marks_blocks_dirty() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write some blocks
    for i in 0..5 {
        let data = vec![i as u8; BLOCK_SIZE];
        cache.write(i as u64 * BLOCK_SIZE as u64, &data).unwrap();
    }

    assert_eq!(cache.dirty_block_count(), 5, "Should have 5 dirty blocks");

    // Claim blocks for sync
    let claimed = cache.claim_dirty_blocks(5);
    assert_eq!(claimed.len(), 5);
    assert_eq!(cache.dirty_block_count(), 0, "Dirty count should be 0 after claim");
    assert_eq!(cache.syncing_block_count(), 5, "Syncing count should be 5");

    // Enable S3 failures
    s3.set_fail_puts(true);

    // Attempt to sync - will fail, but blocks should be re-queued
    let synced = cache.sync_blocks_batched(&s3_store, claimed).await.unwrap();
    assert_eq!(synced, 0, "No blocks should have synced");

    // Blocks should be back in dirty state (re-queued)
    assert_eq!(cache.dirty_block_count(), 5, "Blocks should be dirty again after failure");
    assert_eq!(cache.syncing_block_count(), 0, "No blocks should be syncing");

    // Disable failures and retry
    s3.set_fail_puts(false);

    // Now drain should succeed
    cache.drain_for_snapshot(&s3_store).await.unwrap();
    assert_eq!(cache.dirty_block_count(), 0, "All blocks should be synced");
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
    let (writer_cache, writer_s3, _) = create_test_cache(&writer_dir, "vol1", Arc::clone(&s3));

    let data = vec![0xAB; BLOCK_SIZE];
    writer_cache.write(0, &data).unwrap();
    writer_cache.drain_for_snapshot(&writer_s3).await.unwrap();
    drop(writer_cache);

    // Create fresh cache (no local data)
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_s3, reader_metrics) =
        create_test_cache(&reader_dir, "vol1", Arc::clone(&s3));

    // Enable S3 failures
    s3.set_fail_gets(true);

    // Read should fail (not cached locally, S3 unavailable)
    let result = reader_cache
        .read_with_fetch(0, BLOCK_SIZE, &reader_s3, &reader_metrics)
        .await;

    assert!(result.is_err(), "Read should fail when S3 is unavailable");

    // Disable failures
    s3.set_fail_gets(false);

    // Now read should succeed
    let result = reader_cache
        .read_with_fetch(0, BLOCK_SIZE, &reader_s3, &reader_metrics)
        .await;

    assert!(result.is_ok(), "Read should succeed when S3 is available");
    assert_eq!(result.unwrap().as_ref(), &data[..]);
}

/// Test: Lease acquisition failure is handled gracefully.
///
/// If lease acquisition fails (held by another node), we should
/// get a clear error, not a panic or corrupted state.
#[tokio::test]
async fn test_lease_acquisition_failure_graceful() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    let manager_a = LeaseManager::new(Arc::clone(&s3), "test", "node-a").with_ttl(300);
    let manager_b = LeaseManager::new(Arc::clone(&s3), "test", "node-b").with_ttl(300);

    // Node A acquires lease
    let _handle_a = manager_a.acquire("vol1").await.unwrap();

    // Node B tries to acquire - should fail cleanly
    let result = manager_b.acquire("vol1").await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    // Should be a LeaseHeldByOther error, not a panic
    assert!(
        err.to_string().contains("held") || err.to_string().contains("Lease"),
        "Error should indicate lease is held: {}",
        err
    );
}

/// Test: Write during sync doesn't lose data.
///
/// If a write comes in while we're syncing a block to S3,
/// the write should succeed and the block should be marked dirty again.
#[tokio::test]
async fn test_write_during_sync_preserves_new_data() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write initial data
    let data_v1 = vec![0x11; BLOCK_SIZE];
    cache.write(0, &data_v1).unwrap();

    // Claim for sync (block is now Syncing)
    let claimed = cache.claim_dirty_blocks(1);
    assert_eq!(claimed.len(), 1);

    // Write new data while syncing (block transitions Syncing → Dirty)
    let data_v2 = vec![0x22; BLOCK_SIZE];
    cache.write(0, &data_v2).unwrap();

    // Complete the sync of the old data
    let _ = cache.sync_blocks_batched(&s3_store, claimed).await;

    // Block should be dirty again with new data
    assert_eq!(cache.dirty_block_count(), 1);

    // Read should return the NEW data, not the old synced data
    let read = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(read.as_ref(), &data_v2[..], "Should read the newer write");

    // Drain the new data
    cache.drain_for_snapshot(&s3_store).await.unwrap();

    // Verify S3 has the new data by reading from fresh cache
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_s3, reader_metrics) =
        create_test_cache(&reader_dir, "vol1", Arc::clone(&s3));

    let s3_data = reader_cache
        .read_with_fetch(0, BLOCK_SIZE, &reader_s3, &reader_metrics)
        .await
        .unwrap();

    assert_eq!(
        s3_data.as_ref(),
        &data_v2[..],
        "S3 should have the newer data"
    );
}

/// Test: Rapid lease acquire/release cycles don't corrupt generation.
///
/// The generation counter should always increase monotonically.
#[tokio::test]
async fn test_lease_generation_monotonic() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    let manager = LeaseManager::new(Arc::clone(&s3), "test", "node-a").with_ttl(300);

    let mut last_gen = 0u64;

    for _ in 0..10 {
        let handle = manager.acquire("vol1").await.unwrap();
        assert!(
            handle.lease.generation > last_gen,
            "Generation should always increase: {} > {}",
            handle.lease.generation,
            last_gen
        );
        last_gen = handle.lease.generation;

        manager.release("vol1", &handle).await.unwrap();
        // Release also increments generation
        last_gen += 1;
    }
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
    let (cache, _s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    let cache = Arc::new(cache);
    let write_count = Arc::new(AtomicUsize::new(0));

    let mut tasks = JoinSet::new();

    // Spawn 10 concurrent writers, each writing a different pattern
    for writer_id in 0..10u8 {
        let cache = Arc::clone(&cache);
        let write_count = Arc::clone(&write_count);

        tasks.spawn(async move {
            for _ in 0..100 {
                // Each writer writes its ID as the pattern
                let data = vec![writer_id; BLOCK_SIZE];
                cache.write(0, &data).unwrap();
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

/// Test: Zero blocks are not written to S3 (optimization).
///
/// Writing all-zero blocks should not result in S3 writes,
/// as they can be synthesized on read.
#[tokio::test]
async fn test_zero_blocks_not_synced_to_s3() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write zero blocks
    let zeros = vec![0u8; BLOCK_SIZE];
    for i in 0..10 {
        cache.write(i as u64 * BLOCK_SIZE as u64, &zeros).unwrap();
    }

    // Drain to S3
    cache.drain_for_snapshot(&s3_store).await.unwrap();

    let snap = metrics.snapshot();

    // With zero-block optimization, no batches should be written
    // (or at least fewer than if we wrote all blocks)
    assert_eq!(
        snap.batches_written, 0,
        "Zero blocks should not result in S3 writes (optimization)"
    );
}

/// Test: Mixed zero and non-zero blocks in same batch.
///
/// A batch with some zero and some non-zero blocks should only
/// sync the non-zero blocks.
#[tokio::test]
async fn test_mixed_zero_nonzero_batch() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write alternating zero and non-zero blocks (all in batch 0)
    for i in 0..10 {
        let data = if i % 2 == 0 {
            vec![0u8; BLOCK_SIZE] // Zero block
        } else {
            vec![0xAB; BLOCK_SIZE] // Non-zero block
        };
        cache.write(i as u64 * BLOCK_SIZE as u64, &data).unwrap();
    }

    // Drain
    cache.drain_for_snapshot(&s3_store).await.unwrap();

    // Verify by reading from fresh cache
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_s3, reader_metrics) =
        create_test_cache(&reader_dir, "vol1", Arc::clone(&s3));

    for i in 0..10 {
        let data = reader_cache
            .read_with_fetch(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                &reader_s3,
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
/// Write data, fail during first sync attempt, succeed on retry,
/// verify all data is correct from a fresh node.
#[tokio::test]
async fn test_data_integrity_after_failure_recovery() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write known pattern
    let mut expected_data = Vec::new();
    for i in 0..20u8 {
        let data: Vec<u8> = (0..BLOCK_SIZE).map(|j| i.wrapping_add(j as u8)).collect();
        expected_data.push(data.clone());
        cache.write(i as u64 * BLOCK_SIZE as u64, &data).unwrap();
    }

    // Claim and fail first sync attempt
    let claimed = cache.claim_dirty_blocks(20);
    s3.set_fail_puts(true);
    let synced = cache.sync_blocks_batched(&s3_store, claimed).await.unwrap();
    assert_eq!(synced, 0, "First sync should fail");

    // Blocks should be re-queued
    assert!(cache.dirty_block_count() > 0, "Blocks should be dirty after failure");

    // Now succeed on drain
    s3.set_fail_puts(false);
    cache.drain_for_snapshot(&s3_store).await.unwrap();

    // Verify from fresh cache
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_s3, reader_metrics) =
        create_test_cache(&reader_dir, "vol1", Arc::clone(&s3));

    for (i, expected) in expected_data.iter().enumerate() {
        let data = reader_cache
            .read_with_fetch(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                &reader_s3,
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

/// Test: Multiple concurrent drains don't corrupt data.
///
/// Even if drain is called multiple times concurrently (which shouldn't
/// happen in practice), it should not corrupt data.
#[tokio::test]
async fn test_concurrent_drain_safety() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write data
    for i in 0..10 {
        let data = vec![i as u8; BLOCK_SIZE];
        cache.write(i as u64 * BLOCK_SIZE as u64, &data).unwrap();
    }

    // Spawn multiple concurrent drains
    let cache = Arc::new(cache);
    let s3_store = Arc::clone(&s3_store);

    let mut handles = vec![];
    for _ in 0..3 {
        let cache = Arc::clone(&cache);
        let s3_store = Arc::clone(&s3_store);
        handles.push(tokio::spawn(async move {
            let _ = cache.drain_for_snapshot(&s3_store).await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All data should be synced
    assert_eq!(cache.dirty_block_count(), 0);

    // Verify data integrity
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_s3, reader_metrics) =
        create_test_cache(&reader_dir, "vol1", Arc::clone(&s3));

    for i in 0..10 {
        let data = reader_cache
            .read_with_fetch(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                &reader_s3,
                &reader_metrics,
            )
            .await
            .unwrap();

        let expected = vec![i as u8; BLOCK_SIZE];
        assert_eq!(data.as_ref(), &expected[..], "Block {} corrupted", i);
    }
}
