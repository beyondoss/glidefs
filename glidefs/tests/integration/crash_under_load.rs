//! Crash-under-load tests for GlideFS.
//!
//! These tests verify that no acknowledged writes are lost when a crash
//! occurs while concurrent writers are actively producing dirty blocks
//! AND a flush to S3 is in flight.
//!
//! The scenario:
//! 1. N writer tasks hammer blocks concurrently
//! 2. A flush cycle starts (some blocks CAS DIRTY→SYNCING)
//! 3. The S3 upload fails mid-pack (simulating host death)
//! 4. The cache is dropped without clean shutdown
//! 5. A new cache is opened from the same SSD files (WAL + metadata recovery)
//! 6. Every block that was written is verified to be readable and correct
//!
//! Run with: `cargo test --features test-utils --test integration crash_under_load`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::task::JoinSet;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::state::Initializing;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

use super::{BLOCK_SIZE, DEVICE_SIZE};

/// Object store that succeeds for a while then fails permanently.
///
/// This simulates a host death scenario: some packs may have been uploaded
/// successfully, but the process dies before the flush completes.
#[derive(Debug)]
struct CrashingObjectStore {
    inner: object_store::memory::InMemory,
    /// Number of successful put_multipart_opts calls before crashing.
    puts_before_crash: AtomicU32,
    /// Current put count.
    put_count: AtomicU32,
    /// Once true, all PUTs fail.
    crashed: AtomicBool,
}

impl CrashingObjectStore {
    fn new(puts_before_crash: u32) -> Self {
        Self {
            inner: object_store::memory::InMemory::new(),
            puts_before_crash: AtomicU32::new(puts_before_crash),
            put_count: AtomicU32::new(0),
            crashed: AtomicBool::new(false),
        }
    }

    #[allow(dead_code)]
    fn has_crashed(&self) -> bool {
        self.crashed.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CrashingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CrashingObjectStore")
    }
}

#[async_trait]
impl ObjectStore for CrashingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.crashed.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "CrashingObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "host is dead",
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
        let count = self.put_count.fetch_add(1, Ordering::SeqCst);
        if count >= self.puts_before_crash.load(Ordering::SeqCst) {
            self.crashed.store(true, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "CrashingObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "host is dead",
                )),
            });
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

fn test_config(dir: &TempDir, name: &str) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: true, // production-grade: every write is durable on SSD
       
    }
}

/// Generate a unique, verifiable data pattern for a block.
///
/// The pattern encodes (writer_id, block_index, generation) so we can
/// verify exactly which write survived after recovery.
fn make_block_data(writer_id: u8, block_index: u32, generation: u32) -> Vec<u8> {
    let mut data = vec![0u8; BLOCK_SIZE];
    // Header: writer_id (1) + block_index (4) + generation (4) = 9 bytes
    data[0] = writer_id;
    data[1..5].copy_from_slice(&block_index.to_le_bytes());
    data[5..9].copy_from_slice(&generation.to_le_bytes());
    // Fill rest with a deterministic pattern based on header
    let seed = (writer_id as u32) ^ block_index ^ generation;
    for (i, byte) in data.iter_mut().enumerate().take(BLOCK_SIZE).skip(9) {
        *byte = ((seed.wrapping_mul(i as u32).wrapping_add(7)) & 0xFF) as u8;
    }
    data
}

/// Verify block data matches the expected pattern.
fn verify_block_data(data: &[u8], writer_id: u8, block_index: u32, generation: u32) -> bool {
    let expected = make_block_data(writer_id, block_index, generation);
    data == &expected[..]
}

/// Extract the header from a block to identify which write produced it.
fn parse_block_header(data: &[u8]) -> (u8, u32, u32) {
    let writer_id = data[0];
    let block_index = u32::from_le_bytes(data[1..5].try_into().unwrap());
    let generation = u32::from_le_bytes(data[5..9].try_into().unwrap());
    (writer_id, block_index, generation)
}

/// Test: Crash mid-flush under concurrent writer load — no acknowledged writes lost.
///
/// Multiple writers produce dirty blocks concurrently. A flush starts and
/// fails partway through (simulating host death). The cache is dropped
/// without clean shutdown. Recovery opens the same SSD files, replays WAL,
/// and verifies every block that was written is intact.
#[tokio::test]
async fn test_crash_mid_flush_concurrent_writers_no_data_loss() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "crash_load");

    // Track every successful write: block_index → (writer_id, generation, data)
    #[allow(clippy::type_complexity)]
    let write_log: Arc<Mutex<HashMap<u32, (u8, u32, Vec<u8>)>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Phase 1: Concurrent writers + crash mid-flush
    {
        let s3 = Arc::new(CrashingObjectStore::new(0));
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = Arc::new(cache.skip_recovery_for_test());
        let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let num_writers = 8u8;
        let blocks_per_writer = 50u32;
        let num_blocks = (DEVICE_SIZE / BLOCK_SIZE as u64) as u32;

        // Write an initial batch so there are dirty blocks when flush starts
        for writer_id in 0..num_writers {
            for iteration in 0..10u32 {
                let block_idx =
                    ((writer_id as u32) * blocks_per_writer + iteration) % num_blocks;
                let data = make_block_data(writer_id, block_idx, iteration);
                let offset = block_idx as u64 * BLOCK_SIZE as u64;
                cache.write(offset, &data).unwrap();
                write_log
                    .lock()
                    .insert(block_idx, (writer_id, iteration, data));
            }
        }

        assert!(cache.dirty_block_count() > 0, "should have dirty blocks before flush");

        // Start flush (will fail on first S3 put) AND more concurrent writers
        let content_store =
            ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");
        let pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            DEVICE_SIZE,
            BLOCK_SIZE as u32,
        )));

        let flush_cache = Arc::clone(&cache);
        let flush_handle = tokio::spawn(async move {
            let _ = flush_cache
                .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
                .await;
        });

        // Spawn concurrent writers that race with the flush
        let mut writers = JoinSet::new();
        for writer_id in 0..num_writers {
            let cache = Arc::clone(&cache);
            let _clean_cache = Arc::clone(&clean_cache);
            let write_log = Arc::clone(&write_log);

            writers.spawn(async move {
                for iteration in 10..blocks_per_writer {
                    let block_idx =
                        ((writer_id as u32) * blocks_per_writer + iteration) % num_blocks;
                    let data = make_block_data(writer_id, block_idx, iteration);
                    let offset = block_idx as u64 * BLOCK_SIZE as u64;

                    cache.write(offset, &data).unwrap();

                    write_log
                        .lock()
                        .insert(block_idx, (writer_id, iteration, data));

                    if iteration % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            });
        }

        // Wait for flush and writers to complete
        flush_handle.await.unwrap();
        while let Some(result) = writers.join_next().await {
            result.unwrap();
        }

        // Save metadata before "crash" — simulates the checkpoint that
        // would have run. In a real crash this wouldn't happen, but the
        // WAL covers us. We test the WAL-only path separately in
        // crash_recovery.rs. Here we test the combination of:
        // metadata + WAL + failed flush + concurrent writes.
        cache.save_metadata().unwrap();

        // Drop cache without drain — simulating host death
        drop(cache);
    }

    // Phase 2: Recovery — reopen from SSD files, verify all writes survived
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        let write_log = write_log.lock();
        assert!(
            !write_log.is_empty(),
            "should have recorded at least some writes"
        );

        let mut verified = 0;
        let mut mismatches = Vec::new();

        for (&block_idx, (writer_id, generation, expected_data)) in write_log.iter() {
            let offset = block_idx as u64 * BLOCK_SIZE as u64;
            let recovered = cache.read_local(offset, BLOCK_SIZE).unwrap();

            if recovered.as_ref() != &expected_data[..] {
                let (r_writer, r_block, r_gen) = parse_block_header(&recovered);
                mismatches.push(format!(
                    "block {}: expected writer={} gen={}, got writer={} block={} gen={}",
                    block_idx, writer_id, generation, r_writer, r_block, r_gen
                ));
            } else {
                verified += 1;
            }
        }

        assert!(
            mismatches.is_empty(),
            "Data loss detected after crash recovery! {} mismatches out of {} writes:\n{}",
            mismatches.len(),
            write_log.len(),
            mismatches.join("\n")
        );

        assert!(
            verified > 0,
            "should have verified at least some blocks (got {})",
            verified
        );

        // All recovered blocks should be dirty (flush failed, so nothing is CLEAN)
        assert!(
            cache.dirty_block_count() > 0,
            "recovered cache should have dirty blocks pending flush"
        );
    }
}

/// Test: Crash mid-flush, recover, flush to S3, verify from cold reader.
///
/// End-to-end: write under load → crash → recover → successful flush →
/// read from a completely fresh node using only S3 data.
#[tokio::test]
async fn test_crash_recover_then_flush_to_s3() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "crash_then_s3");

    let mut expected_blocks: HashMap<u32, Vec<u8>> = HashMap::new();

    // Phase 1: Write data, crash mid-flush
    {
        // CrashingObjectStore that fails on the first pack upload
        let crash_s3 = Arc::new(CrashingObjectStore::new(0));
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let _clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        // Write 20 blocks
        for i in 0..20u32 {
            let data = make_block_data(0, i, 0);
            cache
                .write(i as u64 * BLOCK_SIZE as u64, &data)
                .unwrap();
            expected_blocks.insert(i, data);
        }

        // Try to flush — will fail immediately
        let content_store =
            ContentStore::new(Arc::clone(&crash_s3) as Arc<dyn ObjectStore>, "test");
        let pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            DEVICE_SIZE,
            BLOCK_SIZE as u32,
        )));
        let result = cache
            .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
            .await;
        assert!(result.is_err(), "flush should fail");

        // Save metadata then crash
        cache.save_metadata().unwrap();
    }

    // Phase 2: Recover and flush to real S3
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        assert!(
            cache.dirty_block_count() >= 20,
            "should have at least 20 dirty blocks after recovery"
        );

        // Flush to the real (working) S3 backend
        let content_store = ContentStore::new(Arc::clone(&s3_backend), "test");
        let pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            DEVICE_SIZE,
            BLOCK_SIZE as u32,
        )));
        let stats = cache
            .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
            .await
            .unwrap();
        assert!(stats.packs_uploaded > 0, "should upload recovered blocks");
        assert_eq!(cache.dirty_block_count(), 0, "all blocks should be clean");
    }

    // Phase 3: Cold reader on a different "host" — verify from S3 only
    {
        let cold_dir = TempDir::new().unwrap();
        let (cold_cache, content_store, pack_index_cache, volume_manifest, clean_cache, metrics) =
            super::create_cold_reader(&cold_dir, "crash_then_s3", Arc::clone(&s3_backend)).await;

        for &block_idx in expected_blocks.keys() {
            let data = cold_cache
                .read(
                    block_idx as u64 * BLOCK_SIZE as u64,
                    BLOCK_SIZE,
                    clean_cache.as_ref(),
                    &pack_index_cache,
                    &volume_manifest,
                    &content_store,
                    &metrics,
                )
                .await
                .unwrap();

            assert!(
                verify_block_data(&data, 0, block_idx, 0),
                "block {} data mismatch from cold reader after crash recovery + S3 flush",
                block_idx
            );
        }
    }
}

/// Test: WAL-only recovery under load (no metadata save before crash).
///
/// Like test_crash_mid_flush_concurrent_writers_no_data_loss but without
/// save_metadata() — recovery must reconstruct entirely from WAL replay.
#[tokio::test]
async fn test_wal_only_recovery_under_concurrent_load() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "wal_only_load");

    let write_log: Arc<Mutex<HashMap<u32, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Phase 1: Concurrent writes, NO metadata save, NO flush — pure WAL recovery
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = Arc::new(cache.skip_recovery_for_test());
        let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let num_writers = 4u8;
        let writes_per_writer = 25u32;
        let num_blocks = (DEVICE_SIZE / BLOCK_SIZE as u64) as u32;

        let mut writers = JoinSet::new();
        for writer_id in 0..num_writers {
            let cache = Arc::clone(&cache);
            let _clean_cache = Arc::clone(&clean_cache);
            let write_log = Arc::clone(&write_log);

            writers.spawn(async move {
                for iteration in 0..writes_per_writer {
                    let block_idx =
                        ((writer_id as u32) * writes_per_writer + iteration) % num_blocks;
                    let data = make_block_data(writer_id, block_idx, iteration);
                    let offset = block_idx as u64 * BLOCK_SIZE as u64;

                    cache.write(offset, &data).unwrap();
                    write_log.lock().insert(block_idx, data);
                }
            });
        }

        while let Some(result) = writers.join_next().await {
            result.unwrap();
        }

        // Crash WITHOUT saving metadata — WAL is the only recovery source
        drop(cache);
    }

    // Phase 2: Recovery from WAL only
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        let write_log = write_log.lock();
        let mut verified = 0;

        for (&block_idx, expected_data) in write_log.iter() {
            let offset = block_idx as u64 * BLOCK_SIZE as u64;
            let recovered = cache.read_local(offset, BLOCK_SIZE).unwrap();

            assert_eq!(
                recovered.as_ref(),
                &expected_data[..],
                "block {} data mismatch after WAL-only recovery",
                block_idx
            );
            verified += 1;
        }

        assert!(verified > 0, "should have verified blocks");
        assert_eq!(
            cache.dirty_block_count(),
            write_log.len() as u64,
            "all written blocks should be dirty after WAL recovery"
        );
    }
}
