//! Failure injection tests for GlideFS.
//!
//! These tests verify correct behavior under various failure scenarios:
//! 1. S3 errors during flush (timeout, 503, connection refused)
//! 2. Partial operations and recovery
//! 3. Data integrity under concurrent writes and failures
//!
//! Run with: `cargo test --features test-utils --test integration`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::state::Active;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024;

/// A wrapper around InMemory that can inject failures.
#[derive(Debug)]
struct FailingObjectStore {
    inner: object_store::memory::InMemory,
    /// When true, PUT operations will fail with a simulated error.
    fail_puts: AtomicBool,
    /// When true, GET operations will fail.
    fail_gets: AtomicBool,
    /// When true, DELETE operations will fail.
    fail_deletes: AtomicBool,
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
            fail_deletes: AtomicBool::new(false),
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

    fn set_fail_deletes(&self, fail: bool) {
        self.fail_deletes.store(fail, Ordering::SeqCst);
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
        if self.should_fail_put() {
            return Err(object_store::Error::Generic {
                store: "FailingObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Simulated S3 multipart failure",
                )),
            });
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
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
        if self.fail_deletes.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "FailingObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Simulated S3 delete failure",
                )),
            });
        }
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

const DEVICE_SIZE: u64 = 256 * 1024 * 1024; // 256MB (enough for multi-pack tests at 500 blocks/pack)

/// Helper to create a writer cache with the failing object store.
#[allow(clippy::type_complexity)]
async fn create_test_cache(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<FailingObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<PackIndexCache>,
    Arc<parking_lot::RwLock<VolumeManifest>>,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let metrics = Arc::new(ExportMetrics::new());
    let content_store = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");
    let pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));
    let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::open(config).expect("Failed to open cache");
    let cache = cache.skip_recovery_for_test();

    (
        Arc::new(cache),
        content_store,
        pack_index_cache,
        volume_manifest,
        clean_cache,
        metrics,
    )
}

/// Helper to create a cold reader cache from the volume manifest in S3.
///
/// After a writer flushes, the VolumeManifest in S3 maps chunk indices to
/// content-addressed chunk hashes. This helper downloads the VolumeManifest,
/// opens a fresh WriteCache via `open_fresh_active` (empty local block map),
/// and creates a PackIndexCache so read can resolve blocks through S3.
async fn create_reader_from_manifest(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<FailingObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<PackIndexCache>,
    Arc<parking_lot::RwLock<VolumeManifest>>,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    let content_store = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");

    // Fetch VolumeManifest from S3
    let (manifest_bytes, _etag) = content_store
        .get_manifest(name)
        .await
        .expect("volume manifest fetch failed")
        .expect("volume manifest should exist in S3");
    let volume_manifest = VolumeManifest::deserialize(&manifest_bytes)
        .expect("volume manifest deserialization failed");

    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: volume_manifest.size,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let metrics = Arc::new(ExportMetrics::new());
    let pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(volume_manifest));
    let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    // open_fresh_active creates a WriteCache with an empty local block map.
    // read resolves remote data via VolumeManifest + PackIndexCache.
    let cache = WriteCache::open_fresh_active(config)
        .expect("Failed to open fresh active cache");

    (
        Arc::new(cache),
        content_store,
        pack_index_cache,
        volume_manifest,
        clean_cache,
        metrics,
    )
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
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    // Write some blocks
    for i in 0..5 {
        let data = vec![i as u8; BLOCK_SIZE];
        cache
            .write(i as u64 * BLOCK_SIZE as u64, &data)
            .unwrap();
    }

    assert_eq!(cache.dirty_block_count(), 5, "Should have 5 dirty blocks");

    // Enable S3 failures
    s3.set_fail_puts(true);

    // Attempt to flush - will fail because pack upload fails.
    // flush_dirty_inner returns Err before CAS-clearing dirty flags.
    let result = cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await;
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
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
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
    let (writer_cache, writer_content_store, writer_pack_index_cache, writer_volume_manifest, _writer_clean_cache, _) =
        create_test_cache(&writer_dir, "vol1", Arc::clone(&s3)).await;

    let data = vec![0xAB; BLOCK_SIZE];
    writer_cache
        .write(0, &data)
        .unwrap();
    writer_cache
        .flush_to_s3(&writer_content_store, &writer_pack_index_cache, &writer_volume_manifest)
        .await
        .unwrap();
    drop(writer_cache);

    // Create a fresh reader from the volume manifest. read resolves
    // remote data via VolumeManifest + PackIndexCache.
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index_cache, reader_volume_manifest, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    // Enable S3 failures
    s3.set_fail_gets(true);

    // Read should fail (not cached locally, S3 unavailable)
    let result = reader_cache
        .read(
            0,
            BLOCK_SIZE,
            reader_clean_cache.as_ref(),
            &reader_pack_index_cache,
            &reader_volume_manifest,
            &reader_content_store,
            &reader_metrics,
        )
        .await;

    assert!(result.is_err(), "Read should fail when S3 is unavailable");

    // Disable failures
    s3.set_fail_gets(false);

    // Now read should succeed
    let result = reader_cache
        .read(
            0,
            BLOCK_SIZE,
            reader_clean_cache.as_ref(),
            &reader_pack_index_cache,
            &reader_volume_manifest,
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
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    // Write initial data and flush to S3
    let data_v1 = vec![0x11; BLOCK_SIZE];
    cache.write(0, &data_v1).unwrap();
    cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Write new data to the same block
    let data_v2 = vec![0x22; BLOCK_SIZE];
    cache.write(0, &data_v2).unwrap();

    // Block should be dirty again with new data
    assert_eq!(cache.dirty_block_count(), 1);

    // Read should return the NEW data locally
    let read = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(read.as_ref(), &data_v2[..], "Should read the newer write");

    // Flush the new data
    cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Verify S3 has the new data by reading from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index_cache, reader_volume_manifest, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    let s3_data = reader_cache
        .read(
            0,
            BLOCK_SIZE,
            reader_clean_cache.as_ref(),
            &reader_pack_index_cache,
            &reader_volume_manifest,
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
    let (cache, _content_store, _pack_index_cache, _volume_manifest, clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    let cache = Arc::new(cache);
    let write_count = Arc::new(AtomicUsize::new(0));

    let mut tasks = JoinSet::new();

    // Spawn 10 concurrent writers, each writing a different pattern
    for writer_id in 0..10u8 {
        let cache = Arc::clone(&cache);
        let write_count = Arc::clone(&write_count);
        let _clean_cache = Arc::clone(&clean_cache);

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

/// Test: Zero blocks produce tombstone entries in packs.
///
/// Writing all-zero blocks produces pack entries with comp_length = 0
/// (tombstones). This ensures "newest wins" semantics are preserved
/// across forks/migrations: a block overwritten with zeros must be
/// distinguishable from "never written" by the read path.
#[tokio::test]
async fn test_zero_blocks_produce_tombstones() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    // Write zero blocks
    let zeros = vec![0u8; BLOCK_SIZE];
    for i in 0..10 {
        cache
            .write(i as u64 * BLOCK_SIZE as u64, &zeros)
            .unwrap();
    }

    // Flush to S3
    let stats = cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Zero blocks produce tombstone pack entries (comp_length = 0) so
    // that forks see zeros instead of stale non-zero data from older packs.
    assert_eq!(
        stats.packs_uploaded, 1,
        "Zero blocks should produce one tombstone pack"
    );
    assert_eq!(
        stats.bytes_uploaded, 0,
        "Zero block tombstones have no compressed data"
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
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    // Write alternating zero and non-zero blocks
    for i in 0..10 {
        let data = if i % 2 == 0 {
            vec![0u8; BLOCK_SIZE] // Zero block
        } else {
            vec![0xAB; BLOCK_SIZE] // Non-zero block
        };
        cache
            .write(i as u64 * BLOCK_SIZE as u64, &data)
            .unwrap();
    }

    // Flush
    cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Verify by reading from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index_cache, reader_volume_manifest, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    for i in 0..10 {
        let data = reader_cache
            .read(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index_cache,
                &reader_volume_manifest,
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
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    // Write known pattern
    let mut expected_data = Vec::new();
    for i in 0..20u8 {
        let data: Vec<u8> = (0..BLOCK_SIZE).map(|j| i.wrapping_add(j as u8)).collect();
        expected_data.push(data.clone());
        cache
            .write(i as u64 * BLOCK_SIZE as u64, &data)
            .unwrap();
    }

    // Enable S3 failures and attempt flush
    s3.set_fail_puts(true);
    let result = cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await;
    assert!(result.is_err(), "First flush should fail");

    // Blocks should still be dirty
    assert!(
        cache.dirty_block_count() > 0,
        "Blocks should be dirty after failure"
    );

    // Disable failures and flush successfully
    s3.set_fail_puts(false);
    cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Verify from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index_cache, reader_volume_manifest, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    for (i, expected) in expected_data.iter().enumerate() {
        let data = reader_cache
            .read(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index_cache,
                &reader_volume_manifest,
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
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    // Write data
    for i in 0..10 {
        let data = vec![i as u8; BLOCK_SIZE];
        cache
            .write(i as u64 * BLOCK_SIZE as u64, &data)
            .unwrap();
    }

    // Wrap in Arc for sharing across tasks (ContentStore doesn't implement
    // Clone, but flush_to_s3 takes &self references)
    let cache = Arc::new(cache);
    let content_store = Arc::new(content_store);

    let mut handles = vec![];
    for _ in 0..3 {
        let cache = Arc::clone(&cache);
        let content_store = Arc::clone(&content_store);
        let pack_index_cache = Arc::clone(&pack_index_cache);
        let volume_manifest = Arc::clone(&volume_manifest);
        handles.push(tokio::spawn(async move {
            let _ = cache
                .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
                .await;
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
    let (reader_cache, reader_content_store, reader_pack_index_cache, reader_volume_manifest, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    for i in 0..10 {
        let data = reader_cache
            .read(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index_cache,
                &reader_volume_manifest,
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
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    // Write enough blocks for 3 packs (500 blocks per pack, so 1250 = 3 packs).
    // fail_after_puts(2) lets 2 pack uploads succeed, fails the 3rd.
    let num_blocks = 1250u32;
    for i in 0..num_blocks {
        // Embed block index as LE u16 to ensure unique content per block (avoids u8 wrapping dedup).
        let mut data = vec![0u8; BLOCK_SIZE];
        data[..2].copy_from_slice(&(i as u16).to_le_bytes());
        #[allow(clippy::needless_range_loop)]
        for j in 2..BLOCK_SIZE {
            data[j] = ((i as usize + j) % 256) as u8;
        }
        cache
            .write(i as u64 * BLOCK_SIZE as u64, &data)
            .unwrap();
    }

    let dirty_before = cache.dirty_block_count();
    assert_eq!(
        dirty_before, num_blocks as u64,
        "should have all blocks dirty"
    );

    // Fail after 2 PUTs (first pack upload succeeds, second fails)
    s3.set_fail_after_puts(2);

    // Flush should fail — some packs uploaded, but not all
    let result = cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await;
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
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
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
    let (reader_cache, reader_content_store, reader_pack_index_cache, reader_volume_manifest, reader_clean_cache, reader_metrics) =
        create_reader_from_manifest(&reader_dir, "vol1", Arc::clone(&s3)).await;

    for i in 0..num_blocks {
        let data = reader_cache
            .read(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index_cache,
                &reader_volume_manifest,
                &reader_content_store,
                &reader_metrics,
            )
            .await
            .unwrap();

        let mut expected = vec![0u8; BLOCK_SIZE];
        expected[..2].copy_from_slice(&(i as u16).to_le_bytes());
        #[allow(clippy::needless_range_loop)]
        for j in 2..BLOCK_SIZE {
            expected[j] = ((i as usize + j) % 256) as u8;
        }
        assert_eq!(
            data.as_ref(),
            &expected[..],
            "Block {} data mismatch after partial failure recovery",
            i
        );
    }
}

// =============================================================================
// DELETE_ALL_SNAPSHOTS PARTIAL FAILURE
// =============================================================================

/// Test: delete_all_snapshots returns Ok even when individual deletes fail.
///
/// This documents the best-effort contract: callers (e.g. purge_export)
/// cannot distinguish partial cleanup from full success. Orphaned snapshots
/// remain and their referenced packs won't be collected by GC.
#[tokio::test]
async fn test_delete_all_snapshots_returns_ok_on_partial_failure() {
    let s3 = Arc::new(FailingObjectStore::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3)).await;

    // Write data and create two snapshots
    let data = vec![0xAA; BLOCK_SIZE];
    cache.write(0, &data).unwrap();
    cache
        .snapshot(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    let data = vec![0xBB; BLOCK_SIZE];
    cache
        .write(BLOCK_SIZE as u64, &data)
        .unwrap();
    cache
        .snapshot(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    let snapshots = content_store.list_snapshots("vol1").await.unwrap();
    assert_eq!(snapshots.len(), 2, "should have 2 snapshots");

    // Enable delete failures — delete_all_snapshots should still return Ok
    s3.set_fail_deletes(true);
    let result = content_store.delete_all_snapshots("vol1").await;
    assert!(
        result.is_ok(),
        "delete_all_snapshots should return Ok even when deletes fail"
    );

    // Snapshots should still exist (deletes failed)
    s3.set_fail_deletes(false);
    let remaining = content_store.list_snapshots("vol1").await.unwrap();
    assert_eq!(
        remaining.len(),
        2,
        "snapshots should survive failed delete_all_snapshots"
    );

    // Retry without failures — should succeed
    content_store.delete_all_snapshots("vol1").await.unwrap();
    let remaining = content_store.list_snapshots("vol1").await.unwrap();
    assert_eq!(remaining.len(), 0, "all snapshots should be deleted on retry");
}
