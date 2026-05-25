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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
        // Small packs (< 5 MB) go through put_opts instead of
        // put_multipart_opts. Fail chunk pack uploads when fail_finish
        // is set so failure injection works for both paths.
        if self.fail_finish.load(Ordering::SeqCst) && location.as_ref().contains("chunks/") {
            return Err(object_store::Error::Generic {
                store: "FinishFailingObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "Simulated single PUT pack upload failure",
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

        let _clean = SimpleBlockCache::new(1024);

        // Write block 0 and checkpoint (saves metadata + truncates WAL)
        cache.write(0, &original_data).unwrap();
        cache.save_metadata().unwrap();

        // Write block 1 — WAL entry is fsynced (wal_sync: true) but metadata NOT saved
        cache.write(BLOCK_SIZE as u64, &second_data).unwrap();

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
    let _clean = SimpleBlockCache::new(1024);

    let data_a = vec![0xAA; BLOCK_SIZE];
    let data_b = vec![0xBB; BLOCK_SIZE];
    let data_c = vec![0xCC; BLOCK_SIZE];
    let data_d = vec![0xDD; BLOCK_SIZE];

    // Session 1: write A at block 0, checkpoint
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        cache.write(0, &data_a).unwrap();
        cache.save_metadata().unwrap();
    }

    // Session 2: write B at block 1, crash without checkpoint
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache.write(BLOCK_SIZE as u64, &data_b).unwrap();
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
            .write(2 * BLOCK_SIZE as u64, &data_c)
            .unwrap();
        cache.save_metadata().unwrap();
    }

    // Session 4: write D, crash without checkpoint
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache
            .write(3 * BLOCK_SIZE as u64, &data_d)
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
    let (cache, cs, pic, vm, _cc, _m) =
        create_cache_with_store(&dir, "corrupt-test", Arc::clone(&s3) as _).await;

    // Write distinct data
    let data = vec![0x42; BLOCK_SIZE];
    cache.write(0, &data).unwrap();
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
    // Write-time CRCs are captured from the guest's write buffer at pwrite
    // time, providing end-to-end integrity: if the SSD silently corrupts
    // data between pwrite and flush, the CRC mismatch is detected.
    //
    // This test verifies that SSD corruption after write is detected and
    // the block retries on the next flush cycle.
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path(), "ssd-corrupt");

    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.skip_recovery_for_test();

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
    cache.write(0, &data0).unwrap();
    cache
        .write(BLOCK_SIZE as u64, &data1)
        .unwrap();
    cache
        .write(2 * BLOCK_SIZE as u64, &data2)
        .unwrap();
    assert_eq!(cache.dirty_block_count(), 3);

    // Flush all 3 blocks — should succeed.
    let cache = Arc::new(cache);
    let stats = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    assert_eq!(stats.blocks_crc_mismatched, 0);
    assert_eq!(stats.blocks_claimed, 3);
    assert_eq!(cache.dirty_block_count(), 0, "all blocks flushed");

    // Write new data, then overwrite on SSD (simulating corruption after write).
    // Write-time CRC was captured from the 0x11 buffer, but the SSD now has 0xFF.
    cache.write(0, &data0).unwrap();
    {
        use std::os::unix::fs::FileExt;
        let data_path = config.data_path();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .unwrap();
        let garbage = vec![0xFF; BLOCK_SIZE];
        file.write_all_at(&garbage, 0).unwrap();
    }

    let stats2 = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    // Write-time CRC detects the corruption: baseline (0x11) != flushing file (0xFF).
    assert_eq!(stats2.blocks_crc_mismatched, 1);
    assert_eq!(cache.dirty_block_count(), 1, "corrupted block remains dirty");

    // Fix: overwrite with correct data and flush again.
    cache.write(0, &data0).unwrap();
    let stats3 = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    assert_eq!(stats3.blocks_crc_mismatched, 0);
    assert_eq!(cache.dirty_block_count(), 0);
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
    let (cache, cs, pic, vm, _cc, _m) =
        create_cache_with_store(&dir, "idx-corrupt", Arc::clone(&s3) as Arc<dyn ObjectStore>)
            .await;

    let data = vec![0x42; BLOCK_SIZE];
    cache.write(0, &data).unwrap();
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
        "test/chunks/0000/{:016x}.pack",
        pack_ids[0]
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
    let (manifest_data, _etag) = reader_cs
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
    // Start from 1 to avoid all-zeros (zero blocks with no prior entry are
    // skipped by cross-flush dedup since reads return zeros by default).
    for i in 1..=17u8 {
        let data = vec![i; BLOCK_SIZE];
        cache.write(0, &data).unwrap();
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
    cache.write(0, &concurrent_data).unwrap();

    // Run compaction and flush concurrently
    let cache_clone = Arc::clone(&cache);
    let cs_clone = ContentStore::new(Arc::clone(&s3), "test");
    let pic_clone = Arc::clone(&pic);
    let vm_clone = Arc::clone(&vm);
    let compact_cc: Arc<dyn glidefs::block::cache::BlockCache> = cc.clone();
    let (compact_result, flush_result) = tokio::join!(
        compact_if_needed(16, 0.5, &cs, &pic, &vm, &compact_cc),
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
    let _clean = SimpleBlockCache::new(1024);

    // Session 1: write some blocks and save metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
        cache
            .write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE])
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

    let _clean = Arc::new(SimpleBlockCache::new(1024));
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
            .write(i as u64 * BLOCK_SIZE as u64, &data)
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

    // Write and flush multiple times to create packs in chunk 0.
    // Start from 1 to avoid all-zeros (zero blocks with no prior entry are
    // skipped by cross-flush dedup since reads return zeros by default).
    for i in 1..=4u8 {
        let data = vec![i; BLOCK_SIZE];
        cache.write(0, &data).unwrap();
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
    let compact_cc: Arc<dyn glidefs::block::cache::BlockCache> = cc.clone();
    let result1 = compact_chunk(0, &pack_ids, blocks_per_chunk, &cs, &pic, &vm, &compact_cc).await;
    assert!(
        result1.is_ok(),
        "first compaction should succeed: {:?}",
        result1.err()
    );

    // Upload manifest so GC can read it
    {
        let manifest_bytes = vm.read().serialize().unwrap();
        cs.put_manifest("orphan-gc", manifest_bytes, None).await.unwrap();
    }

    // Second compaction with SAME stale pack_ids — CAS fails because
    // the manifest now has [base_1], not [A,B,C,D].
    // compact_chunk uploads a new pack to S3 BEFORE the CAS check,
    // so the uploaded pack becomes an orphan when CAS fails.
    let result2 = compact_chunk(0, &pack_ids, blocks_per_chunk, &cs, &pic, &vm, &compact_cc).await;
    assert!(
        result2.is_err(),
        "second compaction should fail: pack list prefix diverged"
    );

    // Verify data is still accessible through the first compaction's base pack
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        create_reader(&reader_dir, "orphan-gc", Arc::clone(&s3)).await;

    let expected = vec![4u8; BLOCK_SIZE]; // last flush wrote seed=4
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

// =============================================================================
// TEST 9: MANIFEST SAVE FAILURE IN DRAIN PATH
// =============================================================================

/// Object store that allows pack uploads but fails manifest PUT.
///
/// `put_opts` fails for paths containing "manifests/" when `fail_manifest` is set.
/// `put_multipart_opts` always succeeds (pack uploads go through multipart).
#[derive(Debug)]
struct ManifestFailingObjectStore {
    inner: InMemory,
    fail_manifest: AtomicBool,
    manifest_put_attempts: AtomicU64,
}

impl ManifestFailingObjectStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            fail_manifest: AtomicBool::new(false),
            manifest_put_attempts: AtomicU64::new(0),
        }
    }

    fn set_fail_manifest(&self, fail: bool) {
        self.fail_manifest.store(fail, Ordering::SeqCst);
    }
}

impl std::fmt::Display for ManifestFailingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ManifestFailingObjectStore")
    }
}

#[async_trait]
impl ObjectStore for ManifestFailingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.fail_manifest.load(Ordering::SeqCst)
            && location.to_string().contains("manifests/")
        {
            self.manifest_put_attempts.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "ManifestFailingObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Simulated manifest upload failure",
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

/// Test: Manifest save failure in flush_to_s3 (drain path) keeps blocks
/// DIRTY in memory and preserves their data across a crash. With atomic
/// flush the ordering is upload → manifest sync → evict → checkpoint, so
/// a failed manifest sync returns Err BEFORE eviction; outer recovery
/// re-dirties via the flushing file. The cold reader sees the ORIGINAL
/// data after the recovery flush — no zero-data loss.
#[tokio::test]
async fn test_manifest_failure_in_drain_preserves_dirty_after_crash() {
    let s3 = Arc::new(ManifestFailingObjectStore::new());
    let dir = TempDir::new().unwrap();

    let config = test_config_wal_sync(dir.path(), "manifest-drain-fail");

    let _clean = Arc::new(SimpleBlockCache::new(1024));
    let cs = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");
    let pic = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));

    // Session 1: write blocks, flush_to_s3 with manifest failure, "crash".
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = Arc::new(cache.skip_recovery_for_test());

        let data0 = vec![0xAA; BLOCK_SIZE];
        let data1 = vec![0xBB; BLOCK_SIZE];
        let data2 = vec![0xCC; BLOCK_SIZE];
        cache.write(0, &data0).unwrap();
        cache.write(BLOCK_SIZE as u64, &data1).unwrap();
        cache.write(2 * BLOCK_SIZE as u64, &data2).unwrap();
        assert_eq!(cache.dirty_block_count(), 3);

        s3.set_fail_manifest(true);

        let result = cache.flush_to_s3(&cs, &pic, &vm).await;
        assert!(
            result.is_err(),
            "flush_to_s3 should fail when manifest PUT fails"
        );

        // Atomic flush: eviction never happened (manifest sync returned
        // Err first). Outer recovery re-dirtied via flushing file.
        assert!(
            cache.dirty_block_count() >= 3,
            "blocks must stay DIRTY after manifest failure (got {})",
            cache.dirty_block_count()
        );

        drop(cache);
    }

    // Session 2: recovery sees DIRTY blocks, retry flush succeeds.
    s3.set_fail_manifest(false);
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        assert!(
            cache.dirty_block_count() >= 3,
            "blocks should be dirty after crash recovery, got {}",
            cache.dirty_block_count()
        );

        let cache = Arc::new(cache);
        let vm2 = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            DEVICE_SIZE,
            BLOCK_SIZE as u32,
        )));
        let result = cache.flush_to_s3(&cs, &pic, &vm2).await;
        assert!(
            result.is_ok(),
            "flush_to_s3 should succeed on retry: {:?}",
            result.err()
        );
        assert_eq!(cache.dirty_block_count(), 0);
    }

    // Cold reader sees the ORIGINAL data — atomic flush preserved it.
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        create_reader(&reader_dir, "manifest-drain-fail", Arc::clone(&s3) as _).await;

    let expected = [vec![0xAA; BLOCK_SIZE], vec![0xBB; BLOCK_SIZE], vec![0xCC; BLOCK_SIZE]];
    for (i, expected) in expected.iter().enumerate() {
        let result = reader
            .read(
                (i as u64) * BLOCK_SIZE as u64,
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
            "block {} data lost across manifest-failure recovery (got 0x{:02X})",
            i,
            result[0],
        );
    }
}

// =============================================================================
// TEST 10: WAL DUPLICATE BLOCK REPLAY ORDERING
// =============================================================================

/// Test: WAL replay of two writes to the same block returns the last write.
///
/// Write block 0 with pattern A → write block 0 with pattern B → crash
/// (drop without metadata save). WAL replay should apply entries in order
/// and the final state should be pattern B, not pattern A.
#[tokio::test]
async fn test_wal_replay_same_block_last_write_wins() {
    let dir = TempDir::new().unwrap();
    let config = test_config_wal_sync(dir.path(), "wal-dup");
    let _clean = SimpleBlockCache::new(1024);

    let data_a = vec![0xAA; BLOCK_SIZE];
    let data_b = vec![0xBB; BLOCK_SIZE];

    // Session 1: Write same block twice, crash without saving metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();

        cache.write(0, &data_a).unwrap();
        cache.write(0, &data_b).unwrap();

        // Crash — drop without save_metadata()
    }

    // Session 2: WAL replay should give us data_b (last write wins)
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        let block0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(
            block0.as_ref(),
            &data_b[..],
            "WAL replay should return the LAST write to block 0, not the first"
        );

        assert_eq!(
            cache.dirty_block_count(),
            1,
            "block 0 should be dirty after WAL replay"
        );
    }

    // Session 3: Extend to 3 writes — also verify with a flush + cold read
    let dir2 = TempDir::new().unwrap();
    let config2 = test_config_wal_sync(dir2.path(), "wal-dup-3");
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let data_c = vec![0xCC; BLOCK_SIZE];

    {
        let cache = WriteCache::<Initializing>::open(config2.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();

        // Three writes to same block
        cache.write(0, &data_a).unwrap();
        cache.write(0, &data_b).unwrap();
        cache.write(0, &data_c).unwrap();

        // Crash
    }

    // Recover, flush, cold verify
    {
        let cache = WriteCache::<Initializing>::open(config2.clone()).unwrap();
        let cache = Arc::new(cache.finish_recovery().await.unwrap());

        let block0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(
            block0.as_ref(),
            &data_c[..],
            "3 writes to same block: WAL replay should give the last one"
        );

        let cs = ContentStore::new(Arc::clone(&s3), "test");
        let pic = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
        let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            DEVICE_SIZE,
            BLOCK_SIZE as u32,
        )));

        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
        drop(cache);

        // Cold reader should see data_c
        let reader_dir = TempDir::new().unwrap();
        let (reader, rcs, rpic, rvm, rcc, rm) =
            super::create_cold_reader(&reader_dir, "wal-dup-3", Arc::clone(&s3)).await;

        let result = reader
            .read(0, BLOCK_SIZE, rcc.as_ref(), &rpic, &rvm, &rcs, &rm)
            .await
            .unwrap();
        assert_eq!(
            result.as_ref(),
            &data_c[..],
            "cold reader should see the last write after WAL recovery + S3 roundtrip"
        );
    }
}

// =============================================================================
// TEST 11: CONCURRENT WRITES TO SAME BLOCK
// =============================================================================

/// Test: Concurrent writers to the same block produce no torn data.
///
/// Two tasks race to write block 0 with different patterns. After both complete,
/// the block should contain one of the two patterns entirely — never a mix.
/// Flush to S3 and verify from cold reader.
#[tokio::test]
async fn test_concurrent_same_block_no_torn_write() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "torn-write", Arc::clone(&s3)).await;

    let pattern_a = vec![0xAA; BLOCK_SIZE];
    let pattern_b = vec![0xBB; BLOCK_SIZE];

    // Run many rounds to exercise the race
    for _ in 0..50 {
        let cache_ref = &cache;
        let _cc_ref = cc.as_ref();
        let pa = &pattern_a;
        let pb = &pattern_b;

        let (r1, r2) = tokio::join!(
            async { cache_ref.write(0, pa) },
            async { cache_ref.write(0, pb) },
        );
        r1.unwrap();
        r2.unwrap();

        // Read back — should be entirely one pattern, never a mix
        let data = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert!(
            data.as_ref() == &pattern_a[..] || data.as_ref() == &pattern_b[..],
            "block 0 should be entirely pattern A or B, got mixed data: first={:#x} last={:#x}",
            data[0],
            data[BLOCK_SIZE - 1]
        );
    }

    // Flush and verify from cold reader
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    assert_eq!(cache.dirty_block_count(), 0);

    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, rcs, rpic, rvm, rcc, rm) =
        super::create_cold_reader(&reader_dir, "torn-write", Arc::clone(&s3)).await;

    let result = reader
        .read(0, BLOCK_SIZE, rcc.as_ref(), &rpic, &rvm, &rcs, &rm)
        .await
        .unwrap();
    assert!(
        result.as_ref() == &pattern_a[..] || result.as_ref() == &pattern_b[..],
        "cold reader should see entire pattern A or B from S3"
    );
}

// =============================================================================
// WITHIN-BATCH DEDUPLICATION INTEGRITY
// =============================================================================

/// Test: Within-batch dedup produces correct data at all deduplicated offsets.
///
/// When multiple blocks in the same flush batch contain identical data,
/// compute_flush_batch deduplicates them via `seen_hashes`: only one copy of
/// the compressed data is kept, but ALL blocks get pack index entries (each
/// with its own chunk_offset). After flush + cold restart, every original
/// block offset must independently return the correct data via S3.
///
/// This catches bugs where the dedup path:
/// - Drops a pack index entry for the second occurrence
/// - Maps multiple chunk_offsets to the wrong pack byte offset
/// - Breaks BLAKE3 verification due to shared compressed data
#[tokio::test]
async fn test_within_batch_dedup_all_offsets_readable() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "dedup-integrity", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

    let identical_data = vec![0xDD; BLOCK_SIZE];

    // Write the same data to 4 different block offsets in one batch.
    // All 4 will land in the same flush and trigger within-batch dedup.
    let offsets: Vec<u64> = (0..4).map(|i| i * BLOCK_SIZE as u64).collect();
    for &offset in &offsets {
        cache.write(offset, &identical_data).unwrap();
    }
    assert_eq!(cache.dirty_block_count(), 4);

    // Flush — 4 blocks claimed, 3 deduped (only the first unique hash uploads)
    let stats = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    assert_eq!(stats.blocks_claimed, 4, "should claim all 4 dirty blocks");
    assert_eq!(stats.blocks_deduped, 3, "3 of 4 identical blocks should be deduped");
    assert_eq!(stats.packs_uploaded, 1, "all 4 blocks should fit in 1 pack");
    assert_eq!(cache.dirty_block_count(), 0);

    // Cold restart: drop the writer and restore from S3 manifest
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, rcs, rpic, rvm, rcc, rm) =
        super::create_cold_reader(&reader_dir, "dedup-integrity", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

    // Every deduplicated offset must independently return correct data
    for &offset in &offsets {
        let data = reader
            .read(offset, BLOCK_SIZE, rcc.as_ref(), &rpic, &rvm, &rcs, &rm)
            .await
            .unwrap();
        assert_eq!(
            data.as_ref(),
            &identical_data[..],
            "block at offset {} should read correctly after dedup + cold restore",
            offset
        );
    }
}

/// Test: Mixed dedup + unique blocks in same flush batch.
///
/// A flush batch with both deduplicated and unique blocks must preserve
/// all data. This catches off-by-one errors in pack index construction
/// where unique block entries shift deduped block offsets.
#[tokio::test]
async fn test_within_batch_dedup_mixed_unique_and_duplicate() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "dedup-mixed", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

    // 3 blocks with identical data (will dedup to 1 upload)
    let dup_data = vec![0xAA; BLOCK_SIZE];
    for i in 0..3u64 {
        cache.write(i * BLOCK_SIZE as u64, &dup_data).unwrap();
    }

    // 2 blocks with unique data (each must upload separately)
    let unique_a: Vec<u8> = (0..BLOCK_SIZE).map(|i| (i % 251) as u8).collect();
    let unique_b: Vec<u8> = (0..BLOCK_SIZE).map(|i| ((i + 127) % 253) as u8).collect();
    cache.write(3 * BLOCK_SIZE as u64, &unique_a).unwrap();
    cache.write(4 * BLOCK_SIZE as u64, &unique_b).unwrap();

    assert_eq!(cache.dirty_block_count(), 5);

    let stats = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    assert_eq!(stats.blocks_claimed, 5);
    assert_eq!(stats.blocks_deduped, 2, "2 of 3 identical blocks deduped");

    // Cold restart
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, rcs, rpic, rvm, rcc, rm) =
        super::create_cold_reader(&reader_dir, "dedup-mixed", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

    // Verify all 5 blocks independently
    let expected: Vec<(&[u8], u64)> = vec![
        (&dup_data, 0),
        (&dup_data, 1),
        (&dup_data, 2),
        (unique_a.as_slice(), 3),
        (unique_b.as_slice(), 4),
    ];
    for (data, idx) in expected {
        let offset = idx * BLOCK_SIZE as u64;
        let result = reader
            .read(offset, BLOCK_SIZE, rcc.as_ref(), &rpic, &rvm, &rcs, &rm)
            .await
            .unwrap();
        assert_eq!(
            result.as_ref(), data,
            "block {} should read correctly after mixed dedup flush",
            idx
        );
    }
}

// =============================================================================
// TEST: COMPACTION DURING ACTIVE WRITES
// =============================================================================

/// Continuously write while compaction runs. No block data should be lost.
/// New writes should not be compacted mid-flight.
#[tokio::test]
async fn test_compaction_during_active_writes() {
    use glidefs::block::write_cache::compact::compact_if_needed;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "compact-writes", Arc::clone(&s3)).await;

    // Build up 17 packs to exceed compaction threshold of 16
    for i in 0..17u8 {
        let data = vec![i; BLOCK_SIZE];
        cache.write(0, &data).unwrap();
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    }

    // Spawn continuous writes to multiple blocks while compaction runs
    let cache2 = Arc::clone(&cache);
    let _cc2 = Arc::clone(&cc);
    let write_handle = tokio::spawn(async move {
        for round in 0..20u8 {
            // Write to blocks 0-4 with unique data per round
            for block in 0..5u64 {
                let data = vec![round.wrapping_add(0x80); BLOCK_SIZE];
                cache2
                    .write(block * BLOCK_SIZE as u64, &data)
                    .unwrap();
            }
            tokio::task::yield_now().await;
        }
    });

    // Run compaction concurrently
    let compact_cc: Arc<dyn glidefs::block::cache::BlockCache> = cc.clone();
    let compact_result = compact_if_needed(16, 0.5, &cs, &pic, &vm, &compact_cc).await;

    write_handle.await.unwrap();

    // Compaction may succeed or abort via CAS — both are correct
    if let Err(e) = &compact_result {
        assert!(
            e.to_string().contains("concurrent")
                || e.to_string().contains("aborted")
                || e.to_string().contains("Io"),
            "unexpected compaction error: {e}"
        );
    }

    // Flush remaining dirty blocks
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    assert_eq!(cache.dirty_block_count(), 0, "all blocks should be clean after flush");

    // Cold read: verify all blocks have the last written data (0xF0 + 19 = 0x03 wrapping)
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, rcs, rpic, rvm, rcc, rm) =
        create_reader(&reader_dir, "compact-writes", Arc::clone(&s3)).await;

    for block in 0..5u64 {
        let data = reader
            .read(
                block * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                rcc.as_ref(),
                &rpic,
                &rvm,
                &rcs,
                &rm,
            )
            .await
            .unwrap();
        // Last round wrote 19 + 0x80 = 0x93
        let expected = 19u8.wrapping_add(0x80);
        assert_eq!(
            data[0], expected,
            "block {} should have last written data (0x{:02x}), got 0x{:02x}",
            block, expected, data[0]
        );
    }
}

// =============================================================================
// TEST: COMPACTION CRASH MIDWAY
// =============================================================================

/// Start compaction, crash after new base pack is uploaded but before manifest
/// update. Restart. Either old or new packs should be valid — no orphaned refs.
#[tokio::test]
async fn test_compaction_crash_midway() {
    use glidefs::block::write_cache::compact::compact_chunk;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "compact-crash", Arc::clone(&s3)).await;

    // Build up 3 packs so we have something to compact
    for i in 0..3u8 {
        let data = vec![i + 1; BLOCK_SIZE];
        cache.write(0, &data).unwrap();
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    }
    cache.sync_manifest(&cs, &vm).await.unwrap();

    // Snapshot the pack list before compaction
    let packs_before: Vec<u64> = vm.read().chunk_pack_ids(0).unwrap().to_vec();
    assert_eq!(packs_before.len(), 3);

    // Run compaction — this uploads a new base pack and updates the manifest
    let blocks_per_chunk = vm.read().blocks_per_chunk();
    let compact_cc: Arc<dyn glidefs::block::cache::BlockCache> = cc.clone();
    let result = compact_chunk(0, &packs_before, blocks_per_chunk, &cs, &pic, &vm, &compact_cc)
        .await
        .unwrap();

    // Simulate crash: DON'T sync the manifest to S3.
    // The in-memory manifest has been updated (replace_packs_cas succeeded),
    // but S3 still has the old manifest.

    // "Restart": load manifest from S3 (old state, references old packs)
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        create_reader(&reader_dir, "compact-crash", Arc::clone(&s3)).await;

    // Old packs should still be on S3 — compaction doesn't delete them
    // (GC handles deletion). So the old manifest should read correctly.
    let data = reader
        .read(0, BLOCK_SIZE, reader_cc.as_ref(), &reader_pic, &reader_vm, &reader_cs, &reader_m)
        .await
        .unwrap();
    assert_eq!(
        data[0], 3,
        "cold reader with old manifest should see seed=3 (last write), got {}",
        data[0]
    );

    // Also verify: the NEW base pack uploaded by compaction is on S3
    let new_pack_id = result.new_pack_id;
    let chunk_packs = cs.list_chunk_packs(0).await.unwrap();
    assert!(
        chunk_packs.iter().any(|p| p.contains(&format!("{new_pack_id:016x}"))),
        "new base pack from compaction should exist on S3 (orphaned but not lost)"
    );

    // Now sync the updated manifest and verify it also reads correctly
    cache.sync_manifest(&cs, &vm).await.unwrap();
    let reader_dir2 = TempDir::new().unwrap();
    let (reader2, rcs2, rpic2, rvm2, rcc2, rm2) =
        create_reader(&reader_dir2, "compact-crash", Arc::clone(&s3)).await;

    let data2 = reader2
        .read(0, BLOCK_SIZE, rcc2.as_ref(), &rpic2, &rvm2, &rcs2, &rm2)
        .await
        .unwrap();
    assert_eq!(
        data2[0], 3,
        "cold reader with new manifest should also see seed=3, got {}",
        data2[0]
    );
}

// =============================================================================
// TEST: COMPACTION DEDUP CORRECTNESS
// =============================================================================

/// Write same data to 100 different blocks. Compact. Verify dedup occurs
/// (fewer packs, smaller size). Read all 100 blocks, verify data.
#[tokio::test]
async fn test_compaction_dedup_correctness() {
    use glidefs::block::write_cache::compact::compact_if_needed;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "compact-dedup", Arc::clone(&s3)).await;

    // Write the SAME data to 100 different blocks
    let dedup_data = vec![0xDD; BLOCK_SIZE];
    for i in 0..100u64 {
        cache
            .write(i * BLOCK_SIZE as u64, &dedup_data)
            .unwrap();
    }

    // Flush in two batches to create multiple packs
    // (flush_to_s3 creates packs based on flush_threshold)
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    let _packs_before_compact = vm.read().chunk_pack_ids(0).unwrap().len();

    // Write DIFFERENT data (to force a second pack with a different pack_id)
    // and flush. Using the same data would produce the same content-addressed
    // pack_id, causing per-export manifest dedup to skip the upload.
    let dedup_data2 = vec![0xEE; BLOCK_SIZE];
    for i in 0..100u64 {
        cache
            .write(i * BLOCK_SIZE as u64, &dedup_data2)
            .unwrap();
    }
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    let packs_after_second_flush = vm.read().chunk_pack_ids(0).unwrap().len();
    assert!(
        packs_after_second_flush >= 2,
        "should have at least 2 packs before compaction, got {}",
        packs_after_second_flush
    );

    // Compact
    let compact_cc: Arc<dyn glidefs::block::cache::BlockCache> = cc.clone();
    let results = compact_if_needed(1, 0.5, &cs, &pic, &vm, &compact_cc).await.unwrap();
    assert!(
        !results.is_empty(),
        "compaction should have run (threshold=1, have {} packs)",
        packs_after_second_flush
    );

    let packs_after_compact = vm.read().chunk_pack_ids(0).unwrap().len();
    assert!(
        packs_after_compact < packs_after_second_flush,
        "compaction should reduce pack count from {} to fewer, got {}",
        packs_after_second_flush,
        packs_after_compact
    );

    // Sync manifest and cold-read all 100 blocks
    cache.sync_manifest(&cs, &vm).await.unwrap();
    drop(cache);

    let reader_dir = TempDir::new().unwrap();
    let (reader, rcs, rpic, rvm, rcc, rm) =
        create_reader(&reader_dir, "compact-dedup", Arc::clone(&s3)).await;

    for i in 0..100u64 {
        let data = reader
            .read(
                i * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                rcc.as_ref(),
                &rpic,
                &rvm,
                &rcs,
                &rm,
            )
            .await
            .unwrap();
        assert_eq!(
            data.as_ref(),
            &dedup_data2[..],
            "block {} should have latest data (0xEE) after compaction",
            i
        );
    }
}

// =============================================================================
// TEST: CONCURRENT COMPACTION + FLUSH NO DUPLICATE BLOCK REFS
// =============================================================================

/// Concurrent compaction + flush must not produce duplicate block references.
///
/// After both operations complete, verify:
/// 1. No duplicate pack IDs in any chunk's pack list
/// 2. No duplicate chunk_offset entries within any single pack
/// 3. All written data is still readable with correct content
#[tokio::test]
async fn test_concurrent_compaction_flush_no_duplicate_block_refs() {
    use std::collections::HashSet;
    use glidefs::block::write_cache::compact::compact_if_needed;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        create_cache_with_store(&dir, "dedup-refs", Arc::clone(&s3)).await;

    // Write distinct data to 10 different blocks across 18 flush cycles
    // to accumulate >16 packs in chunk 0 (triggering compaction threshold).
    for flush_round in 0..18u8 {
        for block_idx in 0..10u64 {
            let data = vec![flush_round.wrapping_add(block_idx as u8); BLOCK_SIZE];
            cache
                .write(block_idx * BLOCK_SIZE as u64, &data)
                .unwrap();
        }
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    }

    // Verify we have >16 packs before starting
    let pack_count_before = {
        let guard = vm.read();
        guard.chunk_pack_ids(0).map(|ids| ids.len()).unwrap_or(0)
    };
    assert!(
        pack_count_before > 16,
        "should have >16 packs before test, got {pack_count_before}"
    );

    // Write fresh data that will be flushed concurrently with compaction
    for block_idx in 0..10u64 {
        let data = vec![0xFF; BLOCK_SIZE];
        cache
            .write(block_idx * BLOCK_SIZE as u64, &data)
            .unwrap();
    }

    // Run compaction and flush concurrently
    let cache_clone = Arc::clone(&cache);
    let cs_clone = ContentStore::new(Arc::clone(&s3), "test");
    let pic_clone = Arc::clone(&pic);
    let vm_clone = Arc::clone(&vm);
    let compact_cc: Arc<dyn glidefs::block::cache::BlockCache> = cc.clone();

    let (compact_result, flush_result) = tokio::join!(
        compact_if_needed(16, 0.5, &cs, &pic, &vm, &compact_cc),
        async {
            tokio::task::yield_now().await;
            cache_clone.flush_to_s3(&cs_clone, &pic_clone, &vm_clone).await
        }
    );

    // Compaction may CAS-abort; flush must succeed
    if let Err(e) = &compact_result {
        assert!(
            e.to_string().contains("concurrent")
                || e.to_string().contains("aborted")
                || e.to_string().contains("Io"),
            "unexpected compaction error: {e}"
        );
    }
    flush_result.expect("flush should succeed");

    // === Assertion 1: No duplicate pack IDs in any chunk's pack list ===
    {
        let guard = vm.read();
        for (&chunk_idx, entry) in &guard.chunks {
            let mut seen = HashSet::new();
            for &pack_id in &entry.packs {
                assert!(
                    seen.insert(pack_id),
                    "chunk {chunk_idx} has duplicate pack ID {pack_id}"
                );
            }
        }
    }

    // === Assertion 2: No duplicate chunk_offset entries within any pack ===
    {
        let chunks_snapshot: Vec<(u32, Vec<glidefs::block::pack::PackId>)> = {
            let guard = vm.read();
            guard.chunks.iter().map(|(&k, v)| (k, v.packs.clone())).collect()
        };
        for (chunk_idx, packs) in &chunks_snapshot {
            for &pack_id in packs {
                let entries: std::sync::Arc<[glidefs::block::pack::PackIndexEntry]> = match pic.get_entries(pack_id).await {
                    Some(e) => e,
                    None => {
                        // Fetch from S3 if not cached
                        let fetched = cs.get_pack_index(*chunk_idx, pack_id).await
                            .expect("pack index should be fetchable");
                        pic.insert_entries(pack_id, &fetched);
                        std::sync::Arc::from(fetched)
                    }
                };
                let mut offsets_seen = HashSet::new();
                for entry in entries.iter() {
                    assert!(
                        offsets_seen.insert(entry.chunk_offset),
                        "pack {pack_id} in chunk {chunk_idx} has duplicate chunk_offset {}",
                        entry.chunk_offset
                    );
                }
            }
        }
    }

    // === Assertion 3: All data readable with correct content ===
    drop(cache);
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        create_reader(&reader_dir, "dedup-refs", Arc::clone(&s3)).await;

    for block_idx in 0..10u64 {
        let data = reader
            .read(
                block_idx * BLOCK_SIZE as u64,
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
            data[0], 0xFF,
            "block {block_idx} should have latest data (0xFF) after concurrent compaction+flush"
        );
    }
}

// =============================================================================
// Full chunk cold wake — reproduces fio_verify_after_cold_wake corruption
// =============================================================================

/// Write every block in a full chunk (1024 blocks at 128KB = 128MB), drain to S3,
/// cold-restart from manifest, and verify all blocks read back correctly.
///
/// This test was added to reproduce a data corruption bug found by
/// `fio_verify_after_cold_wake`: blocks 960-1023 returned zeros after cold wake.
/// The test exercises the same path without ublk/fio to isolate whether the bug
/// is in the core write→flush→manifest→cold-read path.
#[tokio::test]
async fn test_full_chunk_cold_wake() {
    use super::BLOCK_SIZE;
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::content_store::ContentStore;
    use glidefs::block::metrics::ExportMetrics;
    use glidefs::block::volume_manifest::VolumeManifest;
    use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

    // 128MB device = exactly 1 chunk = 1024 blocks at 128KB
    const FULL_CHUNK_DEVICE_SIZE: u64 = 128 * 1024 * 1024;
    const NUM_BLOCKS: usize = (FULL_CHUNK_DEVICE_SIZE / BLOCK_SIZE as u64) as usize;
    assert_eq!(NUM_BLOCKS, 1024);

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // === Phase 1: Write all 1024 blocks ===
    let writer_dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: writer_dir.path().to_path_buf(),
        device_name: "full-chunk".to_string(),
        device_size: FULL_CHUNK_DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let content_store = ContentStore::new(Arc::clone(&s3), "test");
    let pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(
        VolumeManifest::new(FULL_CHUNK_DEVICE_SIZE, BLOCK_SIZE as u32),
    ));
    let _clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let _metrics = Arc::new(ExportMetrics::new());

    let cache = WriteCache::open(config).expect("open cache");
    let cache = Arc::new(cache.skip_recovery_for_test());

    // Write each block with a unique pattern using 4K sub-block writes (like fio).
    // Block N gets filled with (N+1) as u8, written in 32 × 4K chunks.
    const SUB_BLOCK: usize = 4096;
    const SUBS_PER_BLOCK: usize = BLOCK_SIZE / SUB_BLOCK; // 32
    for block in 0..NUM_BLOCKS {
        let fill = ((block + 1) % 256) as u8;
        let sub_data = vec![fill; SUB_BLOCK];
        for sub in 0..SUBS_PER_BLOCK {
            let offset = block as u64 * BLOCK_SIZE as u64 + sub as u64 * SUB_BLOCK as u64;
            cache
                .write(offset, &sub_data)
                .unwrap();
        }
    }

    // Drain: flush all dirty blocks to S3 + upload manifest
    loop {
        let stats = cache
            .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
            .await
            .unwrap();
        if stats.blocks_claimed == 0 {
            break;
        }
    }

    // Drop writer — only S3 has the data
    drop(cache);
    drop(writer_dir);

    // === Phase 2: Cold wake from S3 manifest ===
    let reader_dir = TempDir::new().unwrap();

    // Fetch manifest from S3
    let reader_cs = ContentStore::new(Arc::clone(&s3), "test");
    let (manifest_data, _etag) = reader_cs
        .get_manifest("full-chunk")
        .await
        .expect("get_manifest failed")
        .expect("manifest not found in S3");
    let reader_vm = Arc::new(parking_lot::RwLock::new(
        VolumeManifest::deserialize(&manifest_data).expect("deserialize manifest"),
    ));

    let reader_config = WriteCacheConfig {
        cache_dir: reader_dir.path().to_path_buf(),
        device_name: "full-chunk-reader".to_string(),
        device_size: FULL_CHUNK_DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let reader_cache = Arc::new(
        WriteCache::open_fresh_active(reader_config).expect("open fresh cache"),
    );

    // Fresh pack index cache for the reader (simulates cold start)
    let reader_pic = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let reader_cc = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let reader_m = Arc::new(ExportMetrics::new());

    // === Phase 3: Verify ALL 1024 blocks ===
    let mut failures = Vec::new();
    for block in 0..NUM_BLOCKS {
        let expected_fill = ((block + 1) % 256) as u8;
        let offset = block as u64 * BLOCK_SIZE as u64;
        let data = reader_cache
            .read(
                offset,
                BLOCK_SIZE,
                reader_cc.as_ref(),
                &reader_pic,
                &reader_vm,
                &reader_cs,
                &reader_m,
            )
            .await
            .unwrap();

        // Check first and last byte (fast) before doing full comparison
        if data[0] != expected_fill || data[BLOCK_SIZE - 1] != expected_fill {
            failures.push((block, expected_fill, data[0]));
        }
    }

    assert!(
        failures.is_empty(),
        "cold wake data corruption: {} blocks returned wrong data.\n\
         First 10 failures: {:?}",
        failures.len(),
        &failures[..std::cmp::min(10, failures.len())],
    );
}

/// Stress test: concurrent 4K sub-block writes, drain, cold wake, verify.
/// Runs 20 iterations to reproduce intermittent corruption.
///
/// Uses auto-flush (default mode) with concurrent sub-block writes.
/// Auto-flush now uses SYNCING→CLEAN (not NOT_PRESENT), so blocks stay
/// on local SSD during writes. Only drain evicts after all writes complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cold_wake_stress_concurrent_writes() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::config::ExportConfig;

    const DEVICE_SIZE_GB: f64 = 1.0;
    const NUM_BLOCKS: usize = 1024;
    const SUB_BLOCK: usize = 4096;
    const SUBS_PER_BLOCK: usize = BLOCK_SIZE / SUB_BLOCK;
    const ITERATIONS: usize = 20;

    for iteration in 0..ITERATIONS {
        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // === Write phase: concurrent 4K sub-block writes (manual flush) ===
        let cache_dir1 = TempDir::new().unwrap();
        let clean_cache: Arc<dyn glidefs::block::cache::BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let router1 = Arc::new(
            ExportRouter::new(RouterConfig {
                object_store: Arc::clone(&s3),
                db_path: "stress".to_string(),
                cache_dir: cache_dir1.path().to_path_buf(),
                block_size: BLOCK_SIZE,
                clean_cache,
                wal_sync: false,
                max_s3_uploads: 128,
                max_s3_downloads: 512,
                default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 4,
                nbd_dead_conn_timeout: 0,
                max_exports: 10_000,
                manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
            })
            .await
            .unwrap(),
        );

        let config = ExportConfig {
            name: "vol1".to_string(),
            size_gb: DEVICE_SIZE_GB,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router1.create_export(config, false, None, None).await.unwrap();

        let handler = router1.get_handler("vol1").await.unwrap();

        // Concurrent writers: 8 tasks each writing a stripe of blocks with 4K I/Os.
        // This mimics fio's iodepth=32 sequential write pattern.
        let blocks_per_writer = NUM_BLOCKS / 8;
        let mut write_handles = Vec::new();
        for writer_id in 0..8u64 {
            let h = Arc::clone(&handler);
            let start_block = writer_id as usize * blocks_per_writer;
            let end_block = start_block + blocks_per_writer;
            write_handles.push(tokio::spawn(async move {
                for block in start_block..end_block {
                    let fill = ((block + 1) % 256) as u8;
                    let sub_data = vec![fill; SUB_BLOCK];
                    for sub in 0..SUBS_PER_BLOCK {
                        let offset = block as u64 * BLOCK_SIZE as u64
                            + sub as u64 * SUB_BLOCK as u64;
                        h.write(offset, &sub_data, false).await.unwrap();
                    }
                }
            }));
        }
        for h in write_handles {
            h.await.unwrap();
        }

        // Drain to S3
        router1.drain_export("vol1").await.unwrap();
        router1.shutdown().await.unwrap();
        drop(cache_dir1);

        // === Cold wake: new router, fresh cache ===
        let cache_dir2 = TempDir::new().unwrap();
        let clean_cache2: Arc<dyn glidefs::block::cache::BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let router2 = Arc::new(
            ExportRouter::new(RouterConfig {
                object_store: Arc::clone(&s3),
                db_path: "stress".to_string(),
                cache_dir: cache_dir2.path().to_path_buf(),
                block_size: BLOCK_SIZE,
                clean_cache: clean_cache2,
                wal_sync: false,
                max_s3_uploads: 128,
                max_s3_downloads: 512,
                default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 4,
                nbd_dead_conn_timeout: 0,
                max_exports: 10_000,
                manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
            })
            .await
            .unwrap(),
        );

        let config2 = ExportConfig {
            name: "vol1".to_string(),
            size_gb: DEVICE_SIZE_GB,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router2.create_export(config2, false, Some("vol1"), None).await.unwrap();

        let handler2 = router2.get_handler("vol1").await.unwrap();

        // Verify all 1024 blocks
        let mut failures = Vec::new();
        for block in 0..NUM_BLOCKS {
            let expected_fill = ((block + 1) % 256) as u8;
            let offset = block as u64 * BLOCK_SIZE as u64;
            let data = handler2.read(offset, BLOCK_SIZE as u32).await.unwrap();

            if data[0] != expected_fill || data[BLOCK_SIZE - 1] != expected_fill {
                failures.push((block, expected_fill, data[0]));
            }
        }

        router2.shutdown().await.unwrap();

        assert!(
            failures.is_empty(),
            "iteration {iteration}: cold wake corruption — {} blocks wrong.\n\
             First 10: {:?}",
            failures.len(),
            &failures[..std::cmp::min(10, failures.len())],
        );
    }
}

// =============================================================================
// AUDIT: C1 — Flushing file deleted before manifest sync
// =============================================================================

/// Prove: when pack uploads succeed but manifest sync fails, crash recovery
/// Atomic flush eliminates the C1 window by construction: pack upload,
/// manifest sync, eviction, checkpoint, and flushing-file cleanup all
/// happen under one `flush_lock` inside `flush_dirty_body`. If the
/// manifest PUT fails, `flush_packs` returns `Err` BEFORE eviction;
/// outer recovery re-dirties the SYNCING blocks via the flushing file.
/// Crash + reopen then sees DIRTY in WAL + data file intact → data is
/// preserved.
#[tokio::test]
async fn test_c1_flushing_file_deleted_before_manifest_sync_causes_data_loss() {
    let s3 = Arc::new(ManifestFailingObjectStore::new());
    let dir = TempDir::new().unwrap();
    let cache_dir = dir.path().to_path_buf();

    let config = WriteCacheConfig {
        cache_dir: cache_dir.clone(),
        device_name: "c1-test".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let content_store = ContentStore::new(
        Arc::clone(&s3) as Arc<dyn ObjectStore>,
        "test",
    );
    let pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));

    let cache = WriteCache::open(config.clone()).expect("open cache");
    let cache = cache.skip_recovery_for_test();

    // Write known data pattern (0xAA) to 5 blocks.
    let original_data = vec![0xAA; BLOCK_SIZE];
    for i in 0..5u64 {
        cache.write(i * BLOCK_SIZE as u64, &original_data).unwrap();
    }
    assert_eq!(cache.dirty_block_count(), 5);

    // Persist metadata so recovery sees dirty blocks even if checkpoint
    // doesn't run after the failed flush.
    cache.checkpoint().await.unwrap();

    // Inject manifest-PUT failure BEFORE flushing. With atomic flush the
    // sequence is upload → manifest sync → evict; if sync fails, eviction
    // never happens and the outer recovery re-dirties via the flushing file.
    s3.set_fail_manifest(true);
    let result = cache
        .flush_packs(&content_store, &pack_index_cache, &volume_manifest, None)
        .await;
    assert!(
        result.is_err(),
        "flush_packs must return Err when manifest PUT fails"
    );

    // Blocks were re-dirtied by outer recovery — they were never evicted.
    assert!(
        cache.dirty_block_count() >= 5,
        "blocks must remain DIRTY after manifest failure (got dirty_count={})",
        cache.dirty_block_count(),
    );

    // Crash.
    drop(cache);

    // Reopen and read. Data is preserved because the active file still
    // has it (eviction never happened) and WAL replay marks blocks DIRTY.
    let recovered = WriteCache::<Initializing>::open(config).expect("reopen cache");
    let recovered = recovered.finish_recovery().await.expect("recovery");

    for i in 0..5u64 {
        let block = recovered.read_local(i * BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
        assert_eq!(
            block[0], 0xAA,
            "block {i} data lost after manifest failure + crash (got 0x{:02X}); \
             atomic flush should preserve data",
            block[0],
        );
    }
}

// =============================================================================
// AUDIT: C2 — Flush error recovery marks block dirty on flushing file read failure
// =============================================================================

/// Prove: when flush fails and error recovery can't read from the flushing file,
/// the block is incorrectly marked DIRTY with zeros in the active file.
///
/// Sequence:
/// 1. Write known data to blocks
/// 2. Start flush (rotation happens, blocks CAS'd to SYNCING)
/// 3. S3 upload fails (flush_dirty_body returns Err)
/// 4. Error recovery tries to copy from flushing→active
/// 5. Delete the flushing file before recovery reads it (simulating I/O error)
/// 6. Recovery marks block DIRTY despite active file having zeros
/// 7. Read returns zeros — data lost
///
/// We can't easily inject a read error on the flushing file, but we CAN test
/// the observable consequence: after a failed flush, the data must be intact
/// and readable regardless of what happened during error recovery.
#[tokio::test]
async fn test_c2_flush_error_recovery_preserves_data() {
    let s3 = Arc::new(FinishFailingObjectStore::new());
    let dir = TempDir::new().unwrap();

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "c2-test".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let content_store = ContentStore::new(
        Arc::clone(&s3) as Arc<dyn ObjectStore>,
        "test",
    );
    let pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));

    let cache = WriteCache::open(config).expect("open cache");
    let cache = cache.skip_recovery_for_test();

    // Write known data.
    let original_data = vec![0xBB; BLOCK_SIZE];
    for i in 0..5u64 {
        cache.write(i * BLOCK_SIZE as u64, &original_data).unwrap();
    }
    assert_eq!(cache.dirty_block_count(), 5);

    // Enable multipart completion failure — flush will fail after rotation
    // but during pack upload (complete() fails).
    s3.set_fail_finish(true);

    let result = cache
        .flush_packs(&content_store, &pack_index_cache, &volume_manifest, None)
        .await;
    assert!(result.is_err(), "flush should fail (multipart complete fails)");

    // After failed flush, blocks should still be dirty.
    assert_eq!(
        cache.dirty_block_count(),
        5,
        "blocks should be re-dirtied after flush failure"
    );

    // Critical check: data must still be readable and correct.
    for i in 0..5u64 {
        let block = cache.read_local(i * BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
        assert_eq!(
            block[0], 0xBB,
            "C2 BUG: block {i} data corrupted after flush failure recovery. \
             Expected 0xBB but got 0x{:02X}. Error recovery failed to preserve \
             block data when copying from flushing file to active file.",
            block[0],
        );
    }

    // Now disable failures and verify the full cycle works.
    s3.set_fail_finish(false);

    let (stats, _) = cache
        .flush_packs(&content_store, &pack_index_cache, &volume_manifest, None)
        .await
        .expect("retry flush should succeed");
    assert!(stats.packs_uploaded > 0);
}

// =============================================================================
// AUDIT: Concurrent sub-block writes to same NOT_PRESENT block
// =============================================================================

/// Prove: concurrent sub-block writes to the same NOT_PRESENT block (after
/// fork/cold-wake) must all be visible in the final block.
///
/// The backfill_and_write path uses a CAS claim to coordinate concurrent
/// writers to the same NOT_PRESENT block. The loser must wait for the winner
/// to complete before writing its sub-range. If the loser proceeds too early,
/// it writes into zeros and the winner's full-block write overwrites it.
///
/// This test:
/// 1. Writes known data (0xAA) to several blocks, drains to S3
/// 2. Cold-wakes a fork (all blocks NOT_PRESENT, S3 has prior data)
/// 3. Spawns N tasks that simultaneously write different 4K sub-ranges of
///    the same block with distinct fill patterns
/// 4. Reads back the full block and verifies every sub-range has the correct
///    fill, and non-written portions retain the original 0xAA
///
/// Runs multiple iterations to increase race probability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_sub_block_writes_same_not_present_block() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::config::ExportConfig;
    use tokio::task::JoinSet;

    const SUB_BLOCK: usize = 4096;
    const NUM_WRITERS: usize = 4;
    const ITERATIONS: usize = 10;
    const DEVICE_SIZE_GB: f64 = 1.0;

    for iteration in 0..ITERATIONS {
        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // === Phase 1: Write known data to S3 ===
        let cache_dir1 = TempDir::new().unwrap();
        let clean_cache1: Arc<dyn glidefs::block::cache::BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let router1 = Arc::new(
            ExportRouter::new(RouterConfig {
                object_store: Arc::clone(&s3),
                db_path: "sub-block-race".to_string(),
                cache_dir: cache_dir1.path().to_path_buf(),
                block_size: BLOCK_SIZE,
                clean_cache: clean_cache1,
                wal_sync: false,
                max_s3_uploads: 128,
                max_s3_downloads: 512,
                default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 4,
                nbd_dead_conn_timeout: 0,
                max_exports: 10_000,
                manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
            })
            .await
            .unwrap(),
        );

        let config1 = ExportConfig {
            name: "vol".to_string(),
            size_gb: DEVICE_SIZE_GB,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router1
            .create_export(config1, false, None, None)
            .await
            .unwrap();

        let handler1 = router1.get_handler("vol").await.unwrap();

        // Write 0xAA to the first 4 blocks (full blocks).
        let original_data = vec![0xAA; BLOCK_SIZE];
        for block in 0..4u64 {
            handler1
                .write(block * BLOCK_SIZE as u64, &original_data, false)
                .await
                .unwrap();
        }

        // Drain to S3.
        router1.drain_export("vol").await.unwrap();
        router1.shutdown().await.unwrap();
        drop(cache_dir1);

        // === Phase 2: Cold-wake fork (all blocks NOT_PRESENT) ===
        let cache_dir2 = TempDir::new().unwrap();
        let clean_cache2: Arc<dyn glidefs::block::cache::BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let router2 = Arc::new(
            ExportRouter::new(RouterConfig {
                object_store: Arc::clone(&s3),
                db_path: "sub-block-race".to_string(),
                cache_dir: cache_dir2.path().to_path_buf(),
                block_size: BLOCK_SIZE,
                clean_cache: clean_cache2,
                wal_sync: false,
                max_s3_uploads: 128,
                max_s3_downloads: 512,
                default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 4,
                nbd_dead_conn_timeout: 0,
                max_exports: 10_000,
                manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
            })
            .await
            .unwrap(),
        );

        let config2 = ExportConfig {
            name: "vol".to_string(),
            size_gb: DEVICE_SIZE_GB,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        };
        router2
            .create_export(config2, false, Some("vol"), None)
            .await
            .unwrap();

        let handler2 = router2.get_handler("vol").await.unwrap();

        // === Phase 3: Concurrent sub-block writes to the SAME block ===
        // Each writer writes a different 4K sub-range of block 0 with a
        // distinct fill byte. All blocks are NOT_PRESENT — this exercises
        // the backfill_and_write CAS claim path.
        for target_block in 0..4u64 {
            let mut tasks = JoinSet::new();
            // Use a barrier to maximize concurrent arrival at the CAS.
            let barrier = Arc::new(tokio::sync::Barrier::new(NUM_WRITERS));
            for writer_id in 0..NUM_WRITERS {
                let h = Arc::clone(&handler2);
                let b = Arc::clone(&barrier);
                let fill = (writer_id + 1) as u8; // 0x01, 0x02, 0x03, 0x04
                let offset = target_block * BLOCK_SIZE as u64
                    + writer_id as u64 * SUB_BLOCK as u64;
                let sub_data = vec![fill; SUB_BLOCK];
                tasks.spawn(async move {
                    b.wait().await;
                    h.write(offset, &sub_data, false).await.unwrap();
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        }

        // === Phase 4: Verify every sub-range ===
        let mut failures = Vec::new();
        for target_block in 0..4u64 {
            let offset = target_block * BLOCK_SIZE as u64;
            let data = handler2.read(offset, BLOCK_SIZE as u32).await.unwrap();

            for writer_id in 0..NUM_WRITERS {
                let expected_fill = (writer_id + 1) as u8;
                let sub_start = writer_id * SUB_BLOCK;
                let sub_end = sub_start + SUB_BLOCK;
                let actual = data[sub_start];
                if actual != expected_fill
                    || data[sub_end - 1] != expected_fill
                {
                    // Dump the first 64 bytes of each 4K sub-range for debugging
                    eprintln!(
                        "FAIL: block={target_block} writer={writer_id} expected=0x{expected_fill:02X} \
                         actual=0x{actual:02X} last=0x{:02X}",
                        data[sub_end - 1]
                    );
                    // Show which sub-ranges have which fill values
                    let subs = BLOCK_SIZE / SUB_BLOCK;
                    for s in 0..std::cmp::min(8, subs) {
                        let so = s * SUB_BLOCK;
                        eprintln!(
                            "  sub[{s}] at offset {so}: first=0x{:02X} last=0x{:02X}",
                            data[so], data[so + SUB_BLOCK - 1]
                        );
                    }
                    failures.push((
                        target_block,
                        writer_id,
                        expected_fill,
                        actual,
                    ));
                }
            }

            // Non-written portion (beyond NUM_WRITERS * SUB_BLOCK) should
            // retain the original 0xAA from S3.
            let untouched_start = NUM_WRITERS * SUB_BLOCK;
            if untouched_start < BLOCK_SIZE {
                let sample = data[untouched_start];
                if sample != 0xAA {
                    failures.push((target_block, NUM_WRITERS, 0xAA, sample));
                }
            }
        }

        router2.shutdown().await.unwrap();

        assert!(
            failures.is_empty(),
            "iteration {iteration}: concurrent sub-block write data loss — \
             {} failures.\nFirst 10: {:?}\n\
             This means a loser's sub-block write was overwritten by the \
             winner's full-block backfill write.",
            failures.len(),
            &failures[..std::cmp::min(10, failures.len())],
        );
    }
}
