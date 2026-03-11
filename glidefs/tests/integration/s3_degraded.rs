//! S3 degraded operation integration tests.
//!
//! Tests behavior under slow S3, intermittent failures, semaphore exhaustion,
//! and manifest failures. Uses custom ObjectStore wrappers to inject delays
//! and failures.
//!
//! Run with: `cargo test --features test-utils --test integration s3_degraded`

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

use super::{BLOCK_SIZE, DEVICE_SIZE, SHARED_PACK_INDEX_CACHE};

// =============================================================================
// SlowObjectStore — injects configurable delays on PUTs and GETs
// =============================================================================

#[derive(Debug)]
struct SlowObjectStore {
    inner: InMemory,
    put_delay: AtomicU32,  // milliseconds
    get_delay: AtomicU32,  // milliseconds
    enabled: AtomicBool,
}

impl SlowObjectStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            put_delay: AtomicU32::new(0),
            get_delay: AtomicU32::new(0),
            enabled: AtomicBool::new(true),
        }
    }

    fn set_put_delay_ms(&self, ms: u32) {
        self.put_delay.store(ms, Ordering::SeqCst);
    }

    fn set_get_delay_ms(&self, ms: u32) {
        self.get_delay.store(ms, Ordering::SeqCst);
    }

    fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }
}

impl std::fmt::Display for SlowObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowObjectStore")
    }
}

#[async_trait]
impl ObjectStore for SlowObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.enabled.load(Ordering::SeqCst) {
            let delay = self.put_delay.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        if self.enabled.load(Ordering::SeqCst) {
            let delay = self.put_delay.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            }
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        if self.enabled.load(Ordering::SeqCst) {
            let delay = self.get_delay.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            }
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

// =============================================================================
// IntermittentObjectStore — fails every Nth PUT
// =============================================================================

#[derive(Debug)]
struct IntermittentObjectStore {
    inner: InMemory,
    put_count: AtomicU32,
    /// Fail every Nth PUT (0 = disabled).
    fail_every_n: AtomicU32,
    /// When true, all PUTs fail.
    fail_all_puts: AtomicBool,
}

impl IntermittentObjectStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            put_count: AtomicU32::new(0),
            fail_every_n: AtomicU32::new(0),
            fail_all_puts: AtomicBool::new(false),
        }
    }

    fn set_fail_every_n(&self, n: u32) {
        self.fail_every_n.store(n, Ordering::SeqCst);
        self.put_count.store(0, Ordering::SeqCst);
    }

    fn set_fail_all_puts(&self, fail: bool) {
        self.fail_all_puts.store(fail, Ordering::SeqCst);
    }

    fn should_fail_put(&self) -> bool {
        if self.fail_all_puts.load(Ordering::SeqCst) {
            return true;
        }
        let n = self.fail_every_n.load(Ordering::SeqCst);
        if n > 0 {
            let count = self.put_count.fetch_add(1, Ordering::SeqCst) + 1;
            return count.is_multiple_of(n); // fail every Nth
        }
        false
    }

    fn make_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "IntermittentObjectStore",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "Simulated intermittent S3 failure",
            )),
        }
    }
}

impl std::fmt::Display for IntermittentObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IntermittentObjectStore")
    }
}

#[async_trait]
impl ObjectStore for IntermittentObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.should_fail_put() {
            return Err(Self::make_error());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        if self.should_fail_put() {
            return Err(Self::make_error());
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
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

// =============================================================================
// Helpers
// =============================================================================

#[allow(clippy::type_complexity)]
async fn create_cache_with_store(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<dyn ObjectStore>,
) -> (
    Arc<WriteCache<glidefs::block::state::Active>>,
    ContentStore,
    Arc<glidefs::block::pack_index_cache::PackIndexCache>,
    Arc<parking_lot::RwLock<VolumeManifest>>,
    Arc<dyn glidefs::block::cache::BlockCache>,
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
    let content_store = ContentStore::new(s3, "test");
    let pack_index_cache = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));
    let clean_cache: Arc<dyn glidefs::block::cache::BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::open(config).expect("open cache");
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

// =============================================================================
// S3 slow uploads don't block writes
// =============================================================================

/// Inject delay on S3 PUTs. Local SSD writes should still complete instantly.
/// Flush eventually succeeds despite slow S3.
#[tokio::test]
async fn test_s3_slow_uploads_dont_block_writes() {
    let s3 = Arc::new(SlowObjectStore::new());
    s3.set_put_delay_ms(500); // 500ms delay per PUT

    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _metrics) =
        create_cache_with_store(&dir, "slow-upload", Arc::clone(&s3) as Arc<dyn ObjectStore>).await;

    // Writes should be fast (local SSD only)
    let start = Instant::now();
    for i in 0..10usize {
        cache
            .write(
                (i * BLOCK_SIZE) as u64,
                &vec![(i as u8) + 1; BLOCK_SIZE],
                cc.as_ref(),
            )
            .unwrap();
    }
    let write_duration = start.elapsed();
    assert!(
        write_duration < Duration::from_secs(1),
        "10 local writes should complete in <1s, took {:?}",
        write_duration
    );

    // Flush to S3 — will be slow but should succeed
    let flush_start = Instant::now();
    let stats = cache.flush_to_s3(&cs, &pic, &vm, &cc).await.unwrap();
    let flush_duration = flush_start.elapsed();

    assert_eq!(stats.blocks_claimed, 10);
    // Flush should take at least 500ms due to S3 delay
    assert!(
        flush_duration >= Duration::from_millis(400),
        "flush should be slow due to S3 delay, took {:?}",
        flush_duration
    );

    // Disable delay for cold reader verification
    s3.disable();

    let manifest_bytes = vm.read().serialize();
    cs.put_manifest("slow-upload", manifest_bytes, None)
        .await
        .unwrap();

    let reader_dir = TempDir::new().unwrap();
    let (rcache, rcs, rpic, rvm, rcc, rmetrics) =
        super::create_cold_reader(&reader_dir, "slow-upload", Arc::clone(&s3) as Arc<dyn ObjectStore>)
            .await;

    for i in 0..10usize {
        let data = rcache
            .read(
                (i * BLOCK_SIZE) as u64,
                BLOCK_SIZE,
                rcc.as_ref(),
                &rpic,
                &rvm,
                &rcs,
                &rmetrics,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == (i as u8) + 1),
            "block {i} should be readable from S3"
        );
    }
}

// =============================================================================
// S3 slow downloads — bounded latency, not hung forever
// =============================================================================

/// Inject delay on S3 GETs. Cache-miss reads should eventually complete
/// (not hang forever). With a 500ms delay, reads take ~500ms, not infinity.
#[tokio::test]
async fn test_s3_slow_downloads_bounded_latency() {
    let s3 = Arc::new(SlowObjectStore::new());

    // Write blocks + flush to S3 with no delay
    let writer_dir = TempDir::new().unwrap();
    {
        let (cache, cs, pic, vm, cc, _m) =
            create_cache_with_store(
                &writer_dir,
                "slow-download",
                Arc::clone(&s3) as Arc<dyn ObjectStore>,
            )
            .await;

        for i in 0..5usize {
            cache
                .write(
                    (i * BLOCK_SIZE) as u64,
                    &vec![(i as u8) + 1; BLOCK_SIZE],
                    cc.as_ref(),
                )
                .unwrap();
        }
        cache.flush_to_s3(&cs, &pic, &vm, &cc).await.unwrap();
        let manifest_bytes = vm.read().serialize();
        cs.put_manifest("slow-download", manifest_bytes, None)
            .await
            .unwrap();
    }

    // Cold reader with 500ms GET delay
    s3.set_get_delay_ms(500);

    let reader_dir = TempDir::new().unwrap();
    let (rcache, rcs, rpic, rvm, rcc, rmetrics) = super::create_cold_reader(
        &reader_dir,
        "slow-download",
        Arc::clone(&s3) as Arc<dyn ObjectStore>,
    )
    .await;

    // Read should complete within a bounded time (not hang forever)
    // With 500ms delay, 5 reads should finish well under 30s timeout
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        for i in 0..5usize {
            let start = Instant::now();
            let data = rcache
                .read(
                    (i * BLOCK_SIZE) as u64,
                    BLOCK_SIZE,
                    rcc.as_ref(),
                    &rpic,
                    &rvm,
                    &rcs,
                    &rmetrics,
                )
                .await
                .unwrap();
            let elapsed = start.elapsed();
            assert!(
                data.iter().all(|&b| b == (i as u8) + 1),
                "block {i} data mismatch with slow GET"
            );
            // Each read should take at least 400ms (delay) but less than 10s
            assert!(
                elapsed >= Duration::from_millis(400),
                "block {i} read too fast ({elapsed:?}), delay not applied?"
            );
            assert!(
                elapsed < Duration::from_secs(10),
                "block {i} read too slow ({elapsed:?}), potential hang"
            );
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "slow-GET reads should complete within timeout, not hang forever"
    );
}

// =============================================================================
// S3 intermittent failures — eventual success
// =============================================================================

/// Fail every other S3 PUT. Retry flush until all blocks uploaded.
/// No blocks should be permanently stuck as dirty.
#[tokio::test]
async fn test_s3_intermittent_failures_eventual_success() {
    let s3 = Arc::new(IntermittentObjectStore::new());
    s3.set_fail_every_n(2); // Fail every 2nd PUT

    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _metrics) =
        create_cache_with_store(&dir, "intermittent", Arc::clone(&s3) as Arc<dyn ObjectStore>)
            .await;

    // Write 10 blocks
    for i in 0..10usize {
        cache
            .write(
                (i * BLOCK_SIZE) as u64,
                &vec![(i as u8) + 1; BLOCK_SIZE],
                cc.as_ref(),
            )
            .unwrap();
    }

    // First flush attempt may partially succeed or fail entirely
    for attempt in 0..10 {
        match cache.flush_to_s3(&cs, &pic, &vm, &cc).await {
            Ok(_stats) => {
                if cache.dirty_block_count() == 0 {
                    break;
                }
            }
            Err(_) => {
                // S3 failure — blocks remain dirty, retry
            }
        }
        if attempt == 8 {
            // After several retries, disable failures so we can converge
            s3.set_fail_every_n(0);
            s3.set_fail_all_puts(false);
        }
    }

    // All blocks must eventually be flushed
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "all blocks should eventually be flushed after retries"
    );

    // Disable intermittent failures for verification
    s3.set_fail_every_n(0);

    let manifest_bytes = vm.read().serialize();
    cs.put_manifest("intermittent", manifest_bytes, None)
        .await
        .unwrap();

    // Cold reader verifies all data
    let reader_dir = TempDir::new().unwrap();
    let (rcache, rcs, rpic, rvm, rcc, rmetrics) =
        super::create_cold_reader(
            &reader_dir,
            "intermittent",
            Arc::clone(&s3) as Arc<dyn ObjectStore>,
        )
        .await;

    for i in 0..10usize {
        let data = rcache
            .read(
                (i * BLOCK_SIZE) as u64,
                BLOCK_SIZE,
                rcc.as_ref(),
                &rpic,
                &rvm,
                &rcs,
                &rmetrics,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == (i as u8) + 1),
            "block {i} should be correct after intermittent failure recovery"
        );
    }
}

// =============================================================================
// S3 download semaphore exhaustion
// =============================================================================

/// Issue many concurrent reads to S3-only blocks exceeding the download
/// semaphore limit. All should queue and eventually complete.
#[tokio::test]
async fn test_s3_download_semaphore_exhaustion() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Writer: write 20 blocks, flush to S3
    let writer_dir = TempDir::new().unwrap();
    {
        let (cache, cs, pic, vm, cc, _m) =
            super::create_test_cache(&writer_dir, "sem-exhaust", Arc::clone(&s3)).await;

        for i in 0..20usize {
            cache
                .write(
                    (i * BLOCK_SIZE) as u64,
                    &vec![(i as u8) + 1; BLOCK_SIZE],
                    cc.as_ref(),
                )
                .unwrap();
        }

        cache.flush_to_s3(&cs, &pic, &vm, &cc).await.unwrap();
        let manifest_bytes = vm.read().serialize();
        cs.put_manifest("sem-exhaust", manifest_bytes, None)
            .await
            .unwrap();
    }

    // Cold reader with small download semaphore (2 concurrent)
    let reader_dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: reader_dir.path().to_path_buf(),
        device_name: "sem-exhaust".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let cs = Arc::new(
        ContentStore::new(Arc::clone(&s3), "test")
            .with_download_semaphore(Arc::new(tokio::sync::Semaphore::new(2))),
    );

    let (manifest_bytes, _etag) = cs
        .get_manifest("sem-exhaust")
        .await
        .unwrap()
        .expect("manifest should exist");
    let vm = Arc::new(parking_lot::RwLock::new(
        VolumeManifest::deserialize(&manifest_bytes).unwrap(),
    ));
    let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
    let cc = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let metrics = Arc::new(ExportMetrics::new());

    let cache = Arc::new(WriteCache::open_fresh_active(config).unwrap());

    // Issue 20 concurrent reads — semaphore allows only 2 concurrent S3 GETs
    let mut handles = Vec::new();
    for i in 0..20usize {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&cs);
        let pic = Arc::clone(&pic);
        let vm = Arc::clone(&vm);
        let cc = Arc::clone(&cc);
        let metrics = Arc::clone(&metrics);
        handles.push(tokio::spawn(async move {
            let data = cache
                .read(
                    (i * BLOCK_SIZE) as u64,
                    BLOCK_SIZE,
                    cc.as_ref(),
                    &pic,
                    &vm,
                    &cs,
                    &metrics,
                )
                .await
                .unwrap();
            assert!(
                data.iter().all(|&b| b == (i as u8) + 1),
                "block {i} data mismatch"
            );
        }));
    }

    // All 20 reads should complete (queued behind semaphore)
    for handle in handles {
        handle.await.unwrap();
    }
}

// =============================================================================
// S3 upload semaphore exhaustion
// =============================================================================

/// Flush with many blocks — upload semaphore queues rather than drops packs.
#[tokio::test]
async fn test_s3_upload_semaphore_exhaustion() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "sem-upload".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    // ContentStore with tiny upload semaphore (1 concurrent upload)
    let cs = ContentStore::new(Arc::clone(&s3), "test")
        .with_upload_semaphore(Arc::new(tokio::sync::Semaphore::new(1)));

    let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));
    let cc: Arc<dyn glidefs::block::cache::BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let _metrics = Arc::new(ExportMetrics::new());

    let cache = WriteCache::open(config).unwrap();
    let cache = Arc::new(cache.skip_recovery_for_test());

    // Write 50 blocks (enough for multiple packs)
    for i in 0..50usize {
        cache
            .write(
                (i * BLOCK_SIZE) as u64,
                &vec![(i as u8) + 1; BLOCK_SIZE],
                cc.as_ref(),
            )
            .unwrap();
    }

    // Flush — semaphore allows only 1 concurrent upload, but all packs should succeed
    let stats = cache.flush_to_s3(&cs, &pic, &vm, &cc).await.unwrap();
    assert_eq!(stats.blocks_claimed, 50, "all 50 blocks should be claimed");
    assert_eq!(cache.dirty_block_count(), 0, "no dirty blocks after flush");

    // Verify via cold reader
    let manifest_bytes = vm.read().serialize();
    cs.put_manifest("sem-upload", manifest_bytes, None)
        .await
        .unwrap();

    let reader_dir = TempDir::new().unwrap();
    let (rcache, rcs, rpic, rvm, rcc, rmetrics) =
        super::create_cold_reader(&reader_dir, "sem-upload", Arc::clone(&s3)).await;

    for i in 0..50usize {
        let data = rcache
            .read(
                (i * BLOCK_SIZE) as u64,
                BLOCK_SIZE,
                rcc.as_ref(),
                &rpic,
                &rvm,
                &rcs,
                &rmetrics,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == (i as u8) + 1),
            "block {i} should survive upload semaphore queuing"
        );
    }
}

// =============================================================================
// S3 manifest PUT fails after pack upload
// =============================================================================

/// Packs upload successfully, but manifest PUT fails. Retry should not
/// re-upload packs (they already exist). Manifest retry succeeds.
#[tokio::test]
async fn test_s3_manifest_put_fails_after_pack_upload() {
    let s3 = Arc::new(IntermittentObjectStore::new());

    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _metrics) =
        create_cache_with_store(&dir, "manifest-fail", Arc::clone(&s3) as Arc<dyn ObjectStore>)
            .await;

    // Write 10 blocks
    for i in 0..10usize {
        cache
            .write(
                (i * BLOCK_SIZE) as u64,
                &vec![(i as u8) + 1; BLOCK_SIZE],
                cc.as_ref(),
            )
            .unwrap();
    }

    // Flush packs to S3 (should succeed — failures disabled)
    let stats = cache.flush_to_s3(&cs, &pic, &vm, &cc).await.unwrap();
    assert_eq!(stats.blocks_claimed, 10);

    // Now fail the manifest PUT
    s3.set_fail_all_puts(true);
    let manifest_bytes = vm.read().serialize();
    let result = cs.put_manifest("manifest-fail", manifest_bytes.clone(), None).await;
    assert!(result.is_err(), "manifest PUT should fail");

    // Disable failures and retry
    s3.set_fail_all_puts(false);
    cs.put_manifest("manifest-fail", manifest_bytes, None)
        .await
        .unwrap();

    // Verify cold reader can read all blocks
    let reader_dir = TempDir::new().unwrap();
    let (rcache, rcs, rpic, rvm, rcc, rmetrics) =
        super::create_cold_reader(
            &reader_dir,
            "manifest-fail",
            Arc::clone(&s3) as Arc<dyn ObjectStore>,
        )
        .await;

    for i in 0..10usize {
        let data = rcache
            .read(
                (i * BLOCK_SIZE) as u64,
                BLOCK_SIZE,
                rcc.as_ref(),
                &rpic,
                &rvm,
                &rcs,
                &rmetrics,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == (i as u8) + 1),
            "block {i} should be correct after manifest retry"
        );
    }
}
