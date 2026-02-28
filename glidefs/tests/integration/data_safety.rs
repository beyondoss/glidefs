//! Data safety tests: critical failure modes that could cause data loss or corruption.
//!
//! These tests cover gaps identified in the test coverage audit:
//! 1. WAL durability gap (crash between pwrite and WAL sync)
//! 2. BLAKE3 hash verification catches S3 data corruption
//! 3. CRC32 detects SSD corruption between checkpoint and flush
//! 4. Pack index corruption from S3 returns clean error
//! 5. Concurrent compaction + flush preserves all data (CAS race)
//! 6. Corrupt block state entry in .meta file handled gracefully
//! 7. Multipart upload finalization failure preserves dirty blocks
//! 8. Compaction crash leaves orphan, GC cleans it up
//!
//! Run with: `cargo test --features test-utils --test integration data_safety`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::state::{Active, Initializing};
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024;
const DEVICE_SIZE: u64 = 256 * 1024 * 1024;

// =============================================================================
// TEST OBJECT STORES
// =============================================================================

/// Object store that corrupts GET responses (flips a byte in returned data).
///
/// Used to test that BLAKE3-128 hash verification catches S3-level data corruption.
#[derive(Debug)]
struct CorruptingObjectStore {
    inner: InMemory,
    /// When true, GET responses have a byte flipped in the payload.
    corrupt_gets: AtomicBool,
}

impl CorruptingObjectStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            corrupt_gets: AtomicBool::new(false),
        }
    }

    fn set_corrupt_gets(&self, corrupt: bool) {
        self.corrupt_gets.store(corrupt, Ordering::SeqCst);
    }
}

impl std::fmt::Display for CorruptingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CorruptingObjectStore")
    }
}

#[async_trait]
impl ObjectStore for CorruptingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let result = self.inner.get_opts(location, options).await?;

        if self.corrupt_gets.load(Ordering::SeqCst)
            && location.to_string().contains("/chunks/")
        {
            // Corrupt chunk pack data: read the full response, flip a byte, return new response.
            let meta = result.meta.clone();
            let attrs = result.attributes.clone();
            let range = result.range.clone();
            let data = result.bytes().await?;
            let mut corrupted = data.to_vec();
            if corrupted.len() > 20 {
                // Flip a byte in the middle of the data (past the GLPK header)
                corrupted[20] ^= 0xFF;
            }
            Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                    async move { Ok(Bytes::from(corrupted)) },
                ))),
                meta,
                range,
                attributes: attrs,
            })
        } else {
            Ok(result)
        }
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

/// Object store that fails `put_multipart_opts` with an upload whose `complete()` fails.
///
/// Unlike FailingObjectStore (which fails at initiation), this returns a valid
/// MultipartUpload that fails during finalization — the more realistic failure mode.
#[derive(Debug)]
struct FinishFailingObjectStore {
    inner: InMemory,
    /// When true, multipart upload complete() fails.
    fail_finish: AtomicBool,
}

impl FinishFailingObjectStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            fail_finish: AtomicBool::new(false),
        }
    }

    fn set_fail_finish(&self, fail: bool) {
        self.fail_finish.store(fail, Ordering::SeqCst);
    }
}

impl std::fmt::Display for FinishFailingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FinishFailingObjectStore")
    }
}

/// A MultipartUpload wrapper that silently discards written data and fails on complete().
#[derive(Debug)]
struct FailingMultipartUpload;

#[async_trait]
impl MultipartUpload for FailingMultipartUpload {
    fn put_part(&mut self, _data: PutPayload) -> object_store::UploadPart {
        Box::pin(async { Ok(()) })
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        Err(object_store::Error::Generic {
            store: "FinishFailingObjectStore",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "Simulated CompleteMultipartUpload failure",
            )),
        })
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for FinishFailingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        if self.fail_finish.load(Ordering::SeqCst) {
            // Return an upload object that accepts data but fails on complete()
            Ok(Box::new(FailingMultipartUpload))
        } else {
            self.inner.put_multipart_opts(location, opts).await
        }
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
// HELPERS
// =============================================================================

fn test_config(dir: &std::path::Path, name: &str) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    }
}

fn test_config_wal_sync(dir: &std::path::Path, name: &str) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: true,
    }
}

/// Helper to create a cache with a custom object store.
#[allow(clippy::type_complexity)]
async fn create_cache_with_store(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<dyn ObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<PackIndexCache>,
    Arc<parking_lot::RwLock<VolumeManifest>>,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    super::create_test_cache(temp_dir, name, s3).await
}

/// Create a cold reader that fetches data from S3.
async fn create_reader(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<dyn ObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<PackIndexCache>,
    Arc<parking_lot::RwLock<VolumeManifest>>,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    super::create_cold_reader(temp_dir, name, s3).await
}

// =============================================================================
// TEST 1: WAL DURABILITY GAP
// =============================================================================

/// Test: WAL with wal_sync:true makes writes recoverable after crash.
///
/// Write data → save metadata (checkpoints clean state) → write more data →
/// "crash" (drop without saving metadata). On recovery, the WAL entries from
/// the second write batch should mark those blocks dirty.
///
/// This tests the core WAL recovery invariant: data on SSD + WAL entry = recoverable.
#[tokio::test]
async fn test_wal_recovery_after_crash_without_metadata_save() {
    let dir = TempDir::new().unwrap();
    let config = test_config_wal_sync(dir.path(), "wal-test");

    let original_data = vec![0xAA; BLOCK_SIZE];
    let second_data = vec![0xBB; BLOCK_SIZE];

    // Session 1: write block 0, checkpoint, write block 1, crash without checkpoint
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();

        let clean = SimpleBlockCache::new(1024);

        // Write block 0 and checkpoint (saves metadata + truncates WAL)
        cache.write(0, &original_data, &clean).unwrap();
        cache.save_metadata().unwrap();

        // Write block 1 — WAL entry is fsynced (wal_sync: true) but metadata NOT saved
        cache.write(BLOCK_SIZE as u64, &second_data, &clean).unwrap();

        // "Crash" — drop without saving metadata.
        // Block 0 is saved in metadata as Dirty (from the first save_metadata).
        // Block 1 exists on SSD and in the WAL, but NOT in the metadata file.
        drop(cache);
    }

    // Session 2: recovery should find both blocks
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Block 0: was checkpointed as Dirty in metadata, should be recovered
        let block0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(
            block0.as_ref(),
            &original_data[..],
            "block 0 should survive recovery via metadata"
        );

        // Block 1: not in metadata, but WAL entry should mark it dirty
        let block1 = cache.read_local(BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
        assert_eq!(
            block1.as_ref(),
            &second_data[..],
            "block 1 should survive recovery via WAL replay"
        );

        // Both blocks should be dirty (ready for flush)
        assert_eq!(
            cache.dirty_block_count(),
            2,
            "both blocks should be dirty after recovery"
        );
    }
}

/// Test: Multiple crash cycles with WAL recovery.
///
/// Session 1: write A, checkpoint. Session 2: write B, crash.
/// Session 3: verify both A and B are present, write C, checkpoint.
/// Session 4: write D, crash. Session 5: verify A, B, C, D all present.
#[tokio::test]
async fn test_multi_crash_wal_recovery() {
    let dir = TempDir::new().unwrap();
    let config = test_config_wal_sync(dir.path(), "multi-crash");
    let clean = SimpleBlockCache::new(1024);

    let data_a = vec![0xAA; BLOCK_SIZE];
    let data_b = vec![0xBB; BLOCK_SIZE];
    let data_c = vec![0xCC; BLOCK_SIZE];
    let data_d = vec![0xDD; BLOCK_SIZE];

    // Session 1: write A at block 0, checkpoint
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        cache.write(0, &data_a, &clean).unwrap();
        cache.save_metadata().unwrap();
    }

    // Session 2: write B at block 1, crash without checkpoint
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache.write(BLOCK_SIZE as u64, &data_b, &clean).unwrap();
        // crash — no save_metadata()
    }

    // Session 3: recovery should have A and B. Write C, checkpoint.
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        assert_eq!(cache.read_local(0, BLOCK_SIZE).unwrap().as_ref(), &data_a[..]);
        assert_eq!(
            cache
                .read_local(BLOCK_SIZE as u64, BLOCK_SIZE)
                .unwrap()
                .as_ref(),
            &data_b[..]
        );

        cache
            .write(2 * BLOCK_SIZE as u64, &data_c, &clean)
            .unwrap();
        cache.save_metadata().unwrap();
    }

    // Session 4: write D, crash without checkpoint
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache
            .write(3 * BLOCK_SIZE as u64, &data_d, &clean)
            .unwrap();
        // crash
    }

    // Session 5: all 4 blocks should be present
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        assert_eq!(cache.read_local(0, BLOCK_SIZE).unwrap().as_ref(), &data_a[..]);
        assert_eq!(
            cache
                .read_local(BLOCK_SIZE as u64, BLOCK_SIZE)
                .unwrap()
                .as_ref(),
            &data_b[..]
        );
        assert_eq!(
            cache
                .read_local(2 * BLOCK_SIZE as u64, BLOCK_SIZE)
                .unwrap()
                .as_ref(),
            &data_c[..]
        );
        assert_eq!(
            cache
                .read_local(3 * BLOCK_SIZE as u64, BLOCK_SIZE)
                .unwrap()
                .as_ref(),
            &data_d[..]
        );

        assert!(
            cache.dirty_block_count() >= 1,
            "at least block D should be dirty after recovery"
        );
    }
}

// =============================================================================
// TEST 2: BLAKE3 HASH VERIFICATION
// =============================================================================

/// Test: BLAKE3-128 hash verification catches S3 data corruption on read.
///
/// Write data → flush to S3 → enable corruption → read from cold reader.
/// The corrupted compressed data should cause either:
/// - HashMismatch (if LZ4 decompression succeeds but hash differs)
/// - DecompressFailed (if corruption breaks LZ4 framing)
#[tokio::test]
async fn test_s3_data_corruption_detected_by_blake3() {
    let s3 = Arc::new(CorruptingObjectStore::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "corrupt-test", Arc::clone(&s3) as _).await;

    // Write distinct data
    let data = vec![0x42; BLOCK_SIZE];
    cache.write(0, &data, cc.as_ref()).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    drop(cache);

    // Enable corruption on GET responses for chunk pack data
    s3.set_corrupt_gets(true);

    // Read from a cold reader — should detect corruption
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        create_reader(&reader_dir, "corrupt-test", Arc::clone(&s3) as _).await;

    let result = reader
        .read(
            0,
            BLOCK_SIZE,
            reader_cc.as_ref(),
            &reader_pic,
            &reader_vm,
            &reader_cs,
            &reader_m,
        )
        .await;

    assert!(
        result.is_err(),
        "read should fail when S3 data is corrupted"
    );
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("hash mismatch") || err_str.contains("Hash mismatch")
            || err_str.contains("decompression") || err_str.contains("Decompression"),
        "error should be HashMismatch or DecompressFailed, got: {err_str}"
    );

    // Disable corruption — reads should succeed
    s3.set_corrupt_gets(false);

    // Need a fresh reader with an empty clean_cache (the corrupted attempt may have
    // cached nothing, but the pack index cache entry from the first reader is fine)
    let reader_dir2 = TempDir::new().unwrap();
    let (reader2, reader_cs2, reader_pic2, reader_vm2, reader_cc2, reader_m2) =
        create_reader(&reader_dir2, "corrupt-test", Arc::clone(&s3) as _).await;

    let result = reader2
        .read(
            0,
            BLOCK_SIZE,
            reader_cc2.as_ref(),
            &reader_pic2,
            &reader_vm2,
            &reader_cs2,
            &reader_m2,
        )
        .await;

    assert!(
        result.is_ok(),
        "read should succeed when S3 data is not corrupted: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().as_ref(), &data[..]);
}

// =============================================================================
// TEST 3: SSD CORRUPTION DETECTION VIA CRC32
// =============================================================================

/// Test: CRC32 mismatch during flush detects SSD corruption.
///
/// Write data → local_checkpoint (computes CRC32) → corrupt SSD data file →
/// flush. The corrupted block should be detected, skipped, and remain dirty.
/// Other blocks should flush successfully.
#[tokio::test]
async fn test_ssd_corruption_detected_during_flush() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path(), "ssd-corrupt");

    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.skip_recovery_for_test();

    let clean = Arc::new(SimpleBlockCache::new(1024));
    let cs = ContentStore::new(Arc::clone(&s3), "test");
    let pic = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));

    // Write 3 blocks with distinct data
    let data0 = vec![0x11; BLOCK_SIZE];
    let data1 = vec![0x22; BLOCK_SIZE];
    let data2 = vec![0x33; BLOCK_SIZE];
    cache.write(0, &data0, clean.as_ref()).unwrap();
    cache
        .write(BLOCK_SIZE as u64, &data1, clean.as_ref())
        .unwrap();
    cache
        .write(2 * BLOCK_SIZE as u64, &data2, clean.as_ref())
        .unwrap();
    assert_eq!(cache.dirty_block_count(), 3);

    // Run local_checkpoint to compute CRC32s for all dirty blocks
    cache.local_checkpoint().await.unwrap();

    // Corrupt block 1's data directly on the SSD cache file
    {
        use std::os::unix::fs::FileExt;
        let data_path = config.data_path();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .unwrap();
        let garbage = vec![0xFF; BLOCK_SIZE];
        file.write_all_at(&garbage, BLOCK_SIZE as u64).unwrap();
    }

    // Flush — block 1 should be detected as corrupted via CRC32 mismatch.
    // The flush should still succeed for blocks 0 and 2.
    let cache = Arc::new(cache);
    let stats = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    assert!(
        stats.blocks_corrupted > 0,
        "should detect SSD corruption: stats = {:?}",
        stats
    );

    // Block 1 should still be dirty (skipped due to corruption)
    assert!(
        cache.dirty_block_count() > 0,
        "corrupted block should remain dirty"
    );

    // Fix the corruption by writing the correct data back
    {
        use std::os::unix::fs::FileExt;
        let data_path = config.data_path();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .unwrap();
        file.write_all_at(&data1, BLOCK_SIZE as u64).unwrap();
    }

    // Need to recompute CRC32 for the fixed block. Run another local_checkpoint.
    cache.local_checkpoint().await.unwrap();

    // Retry flush — should now succeed for the previously-corrupted block
    let stats2 = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    assert_eq!(
        stats2.blocks_corrupted, 0,
        "no corruption on retry after fix"
    );
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "all blocks should be clean after successful retry"
    );
}

// =============================================================================
// TEST 4: PACK INDEX CORRUPTION
// =============================================================================

/// Test: Corrupt pack index data from S3 returns a clean error.
///
/// Write data → flush to S3 → corrupt the pack's GLIX trailer/index in S3 →
/// read from a cold reader (whose PackIndexCache is empty for this pack).
/// Should get a clean error, not a panic or garbage data.
#[tokio::test]
async fn test_pack_index_corruption_returns_error() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "idx-corrupt", Arc::clone(&s3) as Arc<dyn ObjectStore>)
            .await;

    let data = vec![0x42; BLOCK_SIZE];
    cache.write(0, &data, cc.as_ref()).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    // Find the pack file in S3 and corrupt the GLIX trailer
    let pack_ids: Vec<u64> = {
        let vm_guard = vm.read();
        vm_guard
            .chunk_pack_ids(0)
            .map(|ids| ids.to_vec())
            .unwrap_or_default()
    };
    assert!(!pack_ids.is_empty(), "should have at least one pack");

    // Corrupt the pack: overwrite with garbage that has bad trailer
    let pack_path = object_store::path::Path::from(format!(
        "test/chunks/0000/{}.pack",
        glidefs::block::pack::pack_id_to_string(pack_ids[0])
    ));
    let garbage = PutPayload::from(vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00]);
    s3.put(&pack_path, garbage).await.unwrap();

    // Read from a fresh cold reader with a separate PackIndexCache
    // so it must fetch the pack index from the (now corrupted) S3
    let reader_dir = TempDir::new().unwrap();

    // Create a fresh PackIndexCache that doesn't have the cached entries
    let fresh_pic = Arc::new({
        std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let dir = TempDir::new().unwrap();
            let dir = Box::leak(Box::new(dir));
            let cache = rt.block_on(PackIndexCache::open(dir.path())).unwrap();
            std::mem::forget(rt);
            cache
        })
        .join()
        .unwrap()
    });

    let reader_cs = ContentStore::new(Arc::clone(&s3) as _, "test");

    // Fetch the manifest to get the VolumeManifest
    let manifest_data = reader_cs
        .get_manifest("idx-corrupt")
        .await
        .unwrap()
        .unwrap();
    let reader_vm = Arc::new(parking_lot::RwLock::new(
        VolumeManifest::deserialize(&manifest_data).unwrap(),
    ));

    let reader_config = WriteCacheConfig {
        cache_dir: reader_dir.path().to_path_buf(),
        device_name: "idx-corrupt".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let reader_cache = WriteCache::open_fresh_active(reader_config).unwrap();
    let reader_cc = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let reader_m = Arc::new(ExportMetrics::new());

    let result = reader_cache
        .read(
            0,
            BLOCK_SIZE,
            reader_cc.as_ref(),
            &fresh_pic,
            &reader_vm,
            &reader_cs,
            &reader_m,
        )
        .await;

    assert!(
        result.is_err(),
        "read should fail when pack index is corrupted"
    );
}

// =============================================================================
// TEST 5: CONCURRENT COMPACTION + FLUSH
// =============================================================================

/// Test: Concurrent flush during compaction preserves all data via CAS.
///
/// Write enough packs to trigger compaction. While compaction would run,
/// concurrently write and flush new data. The CAS in replace_packs_cas
/// should correctly handle the concurrent append. All data (old and new)
/// should be readable from a cold reader.
#[tokio::test]
async fn test_concurrent_compaction_and_flush() {
    use glidefs::block::write_cache::compact::compact_if_needed;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "compact-race", Arc::clone(&s3)).await;

    // Write and flush enough times to accumulate >16 packs in chunk 0.
    // Each flush creates one pack per dirty chunk.
    // We write to block 0 each time with different data to create 17 packs.
    for i in 0..17u8 {
        let data = vec![i; BLOCK_SIZE];
        cache.write(0, &data, cc.as_ref()).unwrap();
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    }

    // Verify chunk 0 has >16 packs
    let pack_count = {
        let vm_guard = vm.read();
        vm_guard
            .chunk_pack_ids(0)
            .map(|ids| ids.len())
            .unwrap_or(0)
    };
    assert!(
        pack_count > 16,
        "chunk 0 should have >16 packs, has {pack_count}"
    );

    // Now write fresh data to the same chunk (this creates a new dirty block)
    let concurrent_data = vec![0xFE; BLOCK_SIZE];
    cache.write(0, &concurrent_data, cc.as_ref()).unwrap();

    // Run compaction and flush concurrently
    let cache_clone = Arc::clone(&cache);
    let cs_clone = ContentStore::new(Arc::clone(&s3), "test");
    let pic_clone = Arc::clone(&pic);
    let vm_clone = Arc::clone(&vm);
    let (compact_result, flush_result) = tokio::join!(
        compact_if_needed(16, &cs, &pic, &vm),
        async {
            // Small yield to let compaction start first
            tokio::task::yield_now().await;
            cache_clone.flush_to_s3(&cs_clone, &pic_clone, &vm_clone).await
        }
    );

    // Both should succeed (or compaction may abort due to CAS conflict, which is fine)
    if let Err(e) = &compact_result {
        // CAS abort is expected — compaction detected concurrent modification
        assert!(
            e.to_string().contains("concurrent")
                || e.to_string().contains("aborted")
                || e.to_string().contains("Io"),
            "unexpected compaction error: {e}"
        );
    }
    // Flush should always succeed
    assert!(
        flush_result.is_ok(),
        "flush should succeed: {:?}",
        flush_result.err()
    );

    // The latest data (concurrent_data) should be what we read
    // If flush won the race, it's flushed. If compaction won, the CAS
    // preserved the concurrent append. Either way, data integrity is maintained.
    assert_eq!(cache.dirty_block_count(), 0, "all blocks should be clean");

    // Verify from a cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        create_reader(&reader_dir, "compact-race", Arc::clone(&s3)).await;

    let result = reader
        .read(
            0,
            BLOCK_SIZE,
            reader_cc.as_ref(),
            &reader_pic,
            &reader_vm,
            &reader_cs,
            &reader_m,
        )
        .await
        .unwrap();

    assert_eq!(
        result.as_ref(),
        &concurrent_data[..],
        "cold reader should see the latest write after compaction+flush race"
    );
}

// =============================================================================
// TEST 6: METADATA CORRUPT BLOCK STATE
// =============================================================================

/// Test: CRC32 trailer on .meta file detects corruption.
///
/// Write data → save metadata (now includes CRC32 trailer) → flip a byte →
/// reopen cache. The CRC32 mismatch should be caught and return InvalidMetadata.
#[tokio::test]
async fn test_metadata_crc32_detects_corruption() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path(), "meta-corrupt");
    let clean = SimpleBlockCache::new(1024);

    // Session 1: write some blocks and save metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        cache.write(0, &vec![0xAA; BLOCK_SIZE], &clean).unwrap();
        cache
            .write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE], &clean)
            .unwrap();
        cache.save_metadata().unwrap();
    }

    // Corrupt a byte in the .meta file (the state byte of the first entry).
    // Header = 36 bytes, entry_count = 8 bytes, first entry index = 4 bytes,
    // first entry state byte is at offset 48.
    let meta_path = config.metadata_path();
    {
        use std::io::{Read, Seek, Write};
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&meta_path)
            .unwrap();

        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();
        assert!(contents.len() > 48);

        // Flip a byte — CRC32 will no longer match.
        file.seek(std::io::SeekFrom::Start(48)).unwrap();
        file.write_all(&[contents[48] ^ 0xFF]).unwrap();
        file.sync_all().unwrap();
    }

    // Session 2: open should fail with InvalidMetadata due to CRC mismatch.
    let result = WriteCache::<Initializing>::open(config.clone());
    match result {
        Ok(_) => panic!("should reject .meta file with CRC32 mismatch"),
        Err(e) => {
            let err = e.to_string();
            assert!(
                err.contains("invalid") || err.contains("Invalid") || err.contains("metadata"),
                "error should indicate metadata corruption, got: {err}"
            );
        }
    }
}

// =============================================================================
// TEST 7: MULTIPART FINISH FAILURE
// =============================================================================

/// Test: Multipart upload finalization failure preserves dirty blocks.
///
/// Unlike failing at put_multipart_opts (initiation), this tests failure during
/// writer.finish() (CompleteMultipartUpload). This is the more realistic failure mode —
/// S3 timeout or 500 during CompleteMultipartUpload.
#[tokio::test]
async fn test_multipart_finish_failure_preserves_dirty() {
    let s3 = Arc::new(FinishFailingObjectStore::new());
    let dir = TempDir::new().unwrap();

    let config = test_config(dir.path(), "finish-fail");
    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.skip_recovery_for_test();

    let clean = Arc::new(SimpleBlockCache::new(1024));
    let cs = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");
    let pic = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));

    // Write some blocks
    for i in 0..5 {
        let data = vec![i as u8; BLOCK_SIZE];
        cache
            .write(i as u64 * BLOCK_SIZE as u64, &data, clean.as_ref())
            .unwrap();
    }
    assert_eq!(cache.dirty_block_count(), 5);

    // Enable finish failure — multipart starts fine, but complete() fails
    s3.set_fail_finish(true);

    let cache = Arc::new(cache);

    // Flush should fail
    let result = cache.flush_to_s3(&cs, &pic, &vm).await;
    assert!(
        result.is_err(),
        "flush should fail when multipart finish fails"
    );

    // All blocks should still be dirty
    assert_eq!(
        cache.dirty_block_count(),
        5,
        "all blocks should remain dirty after finish failure"
    );

    // Disable failure and retry
    s3.set_fail_finish(false);

    let result = cache.flush_to_s3(&cs, &pic, &vm).await;
    assert!(
        result.is_ok(),
        "flush should succeed after failure resolved: {:?}",
        result.err()
    );
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "all blocks should be clean after successful retry"
    );

    // Verify data integrity from cold reader
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        create_reader(&reader_dir, "finish-fail", Arc::clone(&s3) as _).await;

    for i in 0..5 {
        let result = reader
            .read(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_cc.as_ref(),
                &reader_pic,
                &reader_vm,
                &reader_cs,
                &reader_m,
            )
            .await
            .unwrap();
        let expected = vec![i as u8; BLOCK_SIZE];
        assert_eq!(
            result.as_ref(),
            &expected[..],
            "block {} data mismatch after finish failure recovery",
            i
        );
    }
}

// =============================================================================
// TEST 8: COMPACTION ORPHAN + GC
// =============================================================================

/// Test: Racing compactions leave an orphaned pack that GC can identify.
///
/// Simulate two compactions starting with the same snapshot of pack_ids.
/// The first compaction succeeds (replaces [A,B,C,D] with [base_1]).
/// The second compaction's CAS fails because the prefix diverged — the manifest
/// now has [base_1], not [A,B,C,D]. The second compaction's uploaded pack is
/// orphaned in S3. GC should identify it as dead.
#[tokio::test]
async fn test_compaction_abort_leaves_orphan_gc_identifies() {
    use glidefs::block::write_cache::compact::compact_chunk;
    use glidefs::cli::gc::{new_gc_state_for_test, reconcile_prefix_for_test};
    use std::time::Duration;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "orphan-gc", Arc::clone(&s3)).await;

    // Write and flush multiple times to create packs in chunk 0
    for i in 0..4u8 {
        let data = vec![i; BLOCK_SIZE];
        cache.write(0, &data, cc.as_ref()).unwrap();
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    }

    // Snapshot pack list — both "compactions" start from this same stale view
    let pack_ids: Vec<u64> = {
        let vm_guard = vm.read();
        vm_guard
            .chunk_pack_ids(0)
            .map(|ids| ids.to_vec())
            .unwrap_or_default()
    };
    assert!(
        pack_ids.len() >= 4,
        "should have at least 4 packs in chunk 0"
    );

    let blocks_per_chunk = {
        let vm_guard = vm.read();
        vm_guard.blocks_per_chunk()
    };

    // First compaction succeeds — replaces [A,B,C,D] with [base_1]
    let result1 = compact_chunk(0, &pack_ids, blocks_per_chunk, &cs, &pic, &vm).await;
    assert!(
        result1.is_ok(),
        "first compaction should succeed: {:?}",
        result1.err()
    );

    // Upload manifest so GC can read it
    {
        let manifest_bytes = vm.read().serialize();
        cs.put_manifest("orphan-gc", manifest_bytes).await.unwrap();
    }

    // Second compaction with SAME stale pack_ids — CAS fails because
    // the manifest now has [base_1], not [A,B,C,D].
    // compact_chunk uploads a new pack to S3 BEFORE the CAS check,
    // so the uploaded pack becomes an orphan when CAS fails.
    let result2 = compact_chunk(0, &pack_ids, blocks_per_chunk, &cs, &pic, &vm).await;
    assert!(
        result2.is_err(),
        "second compaction should fail: pack list prefix diverged"
    );

    // Verify data is still accessible through the first compaction's base pack
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        create_reader(&reader_dir, "orphan-gc", Arc::clone(&s3)).await;

    let expected = vec![3u8; BLOCK_SIZE]; // last flush wrote seed=3
    let result = reader
        .read(
            0,
            BLOCK_SIZE,
            reader_cc.as_ref(),
            &reader_pic,
            &reader_vm,
            &reader_cs,
            &reader_m,
        )
        .await
        .unwrap();
    assert_eq!(
        result.as_ref(),
        &expected[..],
        "should read the latest data via first compaction's base pack"
    );

    // Run GC — should find dead packs (old packs replaced by compaction + orphan from failed compaction)
    let gc_cs = ContentStore::new(Arc::clone(&s3), "test");
    let mut gc_state = new_gc_state_for_test();
    let report =
        reconcile_prefix_for_test(&gc_cs, &mut gc_state, Duration::ZERO, 10000, false)
            .await
            .unwrap();

    // The old packs [A,B,C,D] + the orphaned base pack from the failed second
    // compaction should all be identified as dead (not referenced by any manifest).
    assert!(
        report.dead_found() > 0,
        "GC should find dead packs (old replaced packs + orphan from failed compaction)"
    );
}
