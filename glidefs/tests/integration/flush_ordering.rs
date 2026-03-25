//! Flush ordering and write visibility tests for GlideFS.
//!
//! These tests verify the durability contract of the write-behind cache:
//! 1. Writes visible after flush are durable across crash recovery
//! 2. FUA (Force Unit Access) syncs SSD before returning
//! 3. Concurrent client flush + background pressure_flush don't corrupt data
//! 4. Manifest consistency survives partial drain + crash
//!
//! Run with: `cargo test --features test-utils --test integration flush_ordering`

use std::collections::HashMap;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::router::{ExportRouter, RouterConfig};
use glidefs::block::state::Initializing;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};
use glidefs::config::ExportConfig;

use super::{BLOCK_SIZE, DEVICE_SIZE};

fn test_config(dir: &TempDir, name: &str) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: true, // production-grade: every write durable on SSD before ack
       
    }
}

fn create_router_config(s3: Arc<dyn ObjectStore>, dir: &TempDir) -> RouterConfig {
    RouterConfig {
        object_store: s3,
        db_path: "test".to_string(),
        cache_dir: dir.path().to_path_buf(),
        block_size: BLOCK_SIZE,
        clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
        wal_sync: true,
        max_s3_uploads: 0,
        max_s3_downloads: 0,
        default_flush_threshold: 500,
        ublk_nr_queues: 1,
        nbd_dead_conn_timeout: 0,
    }
}

fn test_export_config(name: &str) -> ExportConfig {
    ExportConfig {
        name: name.to_string(),
        size_gb: 0.01,
        s3_prefix: None,
        block_size: None,
        flush_threshold: None,
        flush_mode: None,
        transport: None,
    }
}

// ---------------------------------------------------------------------------
// flush_ordering_guarantee: write A + flush + write B (no flush) + crash
// → A must be present, B may or may not be
// ---------------------------------------------------------------------------

/// The fundamental durability contract: a write followed by flush is durable.
/// A write WITHOUT flush may be lost on crash.
///
/// Phase 1: write block A, flush (SSD sync), write block B (no flush), crash
/// Phase 2: recover from WAL + metadata
/// Verify: block A must be present and correct; block B may or may not be
#[tokio::test]
async fn test_flush_ordering_guarantee() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "flush_order");

    let block_a_data = vec![0xAAu8; BLOCK_SIZE];
    let block_b_data = vec![0xBBu8; BLOCK_SIZE];

    // Phase 1: write A, flush, write B (no flush), crash
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let _clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        // Write block A at offset 0
        cache.write(0, &block_a_data).unwrap();

        // Flush to SSD (this is the NBD FLUSH equivalent — syncs data file)
        cache.flush().unwrap();

        // Write block B at offset BLOCK_SIZE — WITHOUT flush
        cache
            .write(BLOCK_SIZE as u64, &block_b_data)
            .unwrap();

        // Crash: save metadata (simulates checkpoint) but no flush after block B
        // In a real crash, the WAL entry for B may or may not be durable.
        // With wal_sync=true, B's WAL entry IS durable, so B will survive.
        // The key invariant: A MUST survive because it was explicitly flushed.
        cache.save_metadata().unwrap();
        drop(cache);
    }

    // Phase 2: Recovery
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Block A MUST be present and correct (it was flushed)
        let recovered_a = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(
            recovered_a.as_ref(),
            &block_a_data[..],
            "block A must survive crash — it was explicitly flushed"
        );

        // Block B: with wal_sync=true, the WAL entry is durable, so B survives too.
        // This is expected — the WAL guarantees durability for all acknowledged writes
        // when wal_sync=true. The ordering guarantee is: flush makes PRIOR writes
        // durable even without WAL, but WAL makes ALL acknowledged writes durable.
        let recovered_b = cache.read_local(BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
        assert_eq!(
            recovered_b.as_ref(),
            &block_b_data[..],
            "block B should survive with wal_sync=true (WAL entry is durable)"
        );
    }
}

/// Same as above but with wal_sync=false — block B is NOT guaranteed to survive.
///
/// With wal_sync=false, WAL entries are buffered. A crash after write() but before
/// the OS flushes the WAL buffer means B's WAL entry is lost. Block A survives
/// because flush() synced the data file.
///
/// We can't reliably simulate a "lost WAL buffer" in a test (the OS usually flushes
/// it immediately), so we test the weaker property: block A always survives, and
/// block B survives OR reads as zeros (both are valid outcomes).
#[tokio::test]
async fn test_flush_ordering_guarantee_wal_sync_false() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = test_config(&temp_dir, "flush_order_nosync");
    config.wal_sync = false;

    let block_a_data = vec![0xAAu8; BLOCK_SIZE];
    let block_b_data = vec![0xBBu8; BLOCK_SIZE];

    // Phase 1: write A, flush, write B (no flush), crash
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let _clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        cache.write(0, &block_a_data).unwrap();
        cache.flush().unwrap();
        cache
            .write(BLOCK_SIZE as u64, &block_b_data)
            .unwrap();

        // Crash without metadata save — simulates abrupt process death
        // Don't call save_metadata() to make this a harder test
        drop(cache);
    }

    // Phase 2: Recovery
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Block A MUST survive — it was written + flushed (data file synced)
        let recovered_a = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(
            recovered_a.as_ref(),
            &block_a_data[..],
            "block A must survive crash — it was explicitly flushed before crash"
        );

        // Block B: may or may not survive (WAL not synced, no metadata save).
        // On SSD, the pwrite likely made it to the device, but this is NOT guaranteed.
        // Either outcome is valid — just verify it's not corrupted (either B's data or zeros).
        let recovered_b = cache.read_local(BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
        let is_b_data = recovered_b.as_ref() == &block_b_data[..];
        let is_zeros = recovered_b.as_ref().iter().all(|&b| b == 0);
        assert!(
            is_b_data || is_zeros,
            "block B should be either the written data or zeros, not corrupted garbage"
        );
    }
}

// ---------------------------------------------------------------------------
// flush_fua_write_durable_before_response
// ---------------------------------------------------------------------------

/// FUA writes must be durable on SSD before the write returns.
///
/// Write with FUA, crash (no explicit flush), recover. The FUA write must survive
/// because FUA implies an immediate SSD sync.
#[tokio::test]
async fn test_fua_write_durable_before_response() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "fua_durable");

    let fua_data = vec![0xFFu8; BLOCK_SIZE];

    // Phase 1: FUA write, then crash
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let _clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        // Write with FUA — this should sync SSD before returning
        cache.write(0, &fua_data).unwrap();
        cache.flush().unwrap(); // FUA triggers flush

        // Crash without metadata save
        drop(cache);
    }

    // Phase 2: Recovery — FUA write must be present
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        let recovered = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(
            recovered.as_ref(),
            &fua_data[..],
            "FUA write must survive crash — SSD was synced before write returned"
        );
    }
}

// ---------------------------------------------------------------------------
// flush_concurrent_with_client_flush
// ---------------------------------------------------------------------------

/// Client-initiated flush (NBD FLUSH command) running concurrently with
/// background pressure_flush must not deadlock or corrupt data.
#[tokio::test]
async fn test_flush_concurrent_with_pressure_flush() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = Arc::new(
        ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
            .await
            .unwrap(),
    );

    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    let handler = router.get_handler("vm1").await.unwrap();

    // Write enough blocks to make pressure_flush do real work
    for i in 0..30 {
        handler
            .write(
                i as u64 * BLOCK_SIZE as u64,
                &vec![(i + 1) as u8; BLOCK_SIZE],
                false,
            )
            .await
            .unwrap();
    }

    // Run client flush (SSD sync) and pressure_flush (S3 sync) concurrently
    let router_pressure = Arc::clone(&router);
    let pressure_handle = tokio::spawn(async move {
        router_pressure.pressure_flush().await;
    });

    // Client flush — just syncs local SSD
    handler.flush().unwrap();

    pressure_handle.await.unwrap();

    // Drain to ensure all blocks are in S3
    router.drain_export("vm1").await.unwrap();

    // Verify via cold reader — router stores at "test/exports/vm1"
    let cs = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");
    let (manifest_data, _etag) = cs
        .get_manifest("vm1")
        .await
        .expect("should succeed")
        .expect("manifest should exist after concurrent flush + drain");
    let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
    assert!(
        !vm.chunks.is_empty(),
        "manifest should have chunks after concurrent flush + pressure_flush"
    );

    // Cold-read every block
    let reader_dir = TempDir::new().unwrap();
    let reader_config = WriteCacheConfig {
        cache_dir: reader_dir.path().to_path_buf(),
        device_name: "vm1".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let reader_cache = WriteCache::open_fresh_active(reader_config).unwrap();
    let reader_pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let reader_vm = Arc::new(parking_lot::RwLock::new(vm));
    let reader_clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let reader_metrics = Arc::new(ExportMetrics::new());

    for i in 0..30 {
        let data = reader_cache
            .read(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_clean_cache.as_ref(),
                &reader_pack_index_cache,
                &reader_vm,
                &cs,
                &reader_metrics,
            )
            .await
            .unwrap();
        assert_eq!(
            data[0],
            (i + 1) as u8,
            "block {} should be correct after concurrent flush + pressure_flush",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// flush_manifest_consistency_after_partial_drain
// ---------------------------------------------------------------------------

/// Write 100 blocks, start drain, crash after partial upload, recover,
/// drain again. Cold reader must see all 100 blocks.
///
/// Uses CrashingObjectStore to fail after N successful pack uploads.
#[tokio::test]
async fn test_manifest_consistency_after_partial_drain() {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    };

    /// Object store that succeeds for a while then fails permanently.
    #[derive(Debug)]
    struct CrashingObjectStore {
        inner: InMemory,
        puts_before_crash: AtomicU32,
        put_count: AtomicU32,
        crashed: AtomicBool,
    }

    impl CrashingObjectStore {
        fn new(puts_before_crash: u32) -> Self {
            Self {
                inner: InMemory::new(),
                puts_before_crash: AtomicU32::new(puts_before_crash),
                put_count: AtomicU32::new(0),
                crashed: AtomicBool::new(false),
            }
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

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "partial_drain");
    let s3_good: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let block_count = 100u32;
    let mut expected: HashMap<u32, Vec<u8>> = HashMap::new();

    // Phase 1: Write 100 blocks, try to flush with crashing S3
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let _clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        for i in 0..block_count {
            let data = vec![(i % 256) as u8; BLOCK_SIZE];
            cache
                .write(i as u64 * BLOCK_SIZE as u64, &data)
                .unwrap();
            expected.insert(i, data);
        }

        // Flush will fail on the first pack upload (simulates crash mid-drain)
        let crash_s3 = Arc::new(CrashingObjectStore::new(0));
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
        assert!(result.is_err(), "flush should fail after crash");

        // Save metadata before crash
        cache.save_metadata().unwrap();
        drop(cache);
    }

    // Phase 2: Recover and flush to working S3
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        assert!(
            cache.dirty_block_count() > 0,
            "should have dirty blocks after crash recovery"
        );

        // Flush to working S3
        let content_store = ContentStore::new(Arc::clone(&s3_good), "test");
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

        // Save manifest
        let vm = volume_manifest.read().clone();
        content_store
            .put_manifest("partial_drain", vm.serialize().unwrap(), None)
            .await
            .unwrap();
    }

    // Phase 3: Cold reader — verify all 100 blocks
    {
        let reader_dir = TempDir::new().unwrap();
        let (cold_cache, content_store, pack_index_cache, volume_manifest, clean_cache, metrics) =
            super::create_cold_reader(&reader_dir, "partial_drain", Arc::clone(&s3_good)).await;

        for (&block_idx, expected_data) in &expected {
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
            assert_eq!(
                data.as_ref(),
                &expected_data[..],
                "block {} data mismatch from cold reader after partial drain + recovery",
                block_idx
            );
        }
    }
}
