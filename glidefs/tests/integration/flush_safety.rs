//! Flush safety tests for GlideFS.
//!
//! These tests verify:
//! 1. pressure_flush syncs the manifest so uploaded packs are always referenced
//! 2. pressure_flush + concurrent drain don't corrupt data
//! 3. snapshot_export does not hold the exports read lock during flush
//! 4. drain_all returns per-export errors when S3 is unavailable
//! 5. resize_export preserves data and rejects shrinks

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
use glidefs::block::router::{ExportRouter, RouterConfig};
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::config::ExportConfig;

/// InMemory wrapper that can toggle PUT failures for router-level tests.
#[derive(Debug)]
struct FailingObjectStore {
    inner: InMemory,
    fail_puts: AtomicBool,
}

impl FailingObjectStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            fail_puts: AtomicBool::new(false),
        }
    }

    fn set_fail_puts(&self, fail: bool) {
        self.fail_puts.store(fail, Ordering::SeqCst);
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
        if self.fail_puts.load(Ordering::SeqCst) {
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
        if self.fail_puts.load(Ordering::SeqCst) {
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

const BLOCK_SIZE: usize = 128 * 1024;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_router_config(s3: Arc<dyn ObjectStore>, dir: &TempDir) -> RouterConfig {
    RouterConfig {
        object_store: s3,
        db_path: "test".to_string(),
        cache_dir: dir.path().to_path_buf(),
        block_size: BLOCK_SIZE,
        clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
        wal_sync: false, bottomless: false,
        max_s3_uploads: 0,
        max_s3_downloads: 0,
        default_blocks_per_pack: 500,
        ublk_nr_queues: 1,
        nbd_dead_conn_timeout: 0,
    }
}

fn test_export_config(name: &str) -> ExportConfig {
    ExportConfig {
        name: name.to_string(),
        size_gb: 0.01, // 10MB
        s3_prefix: None,
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    }
}

// ---------------------------------------------------------------------------
// Fix 1: pressure_flush syncs manifest
// ---------------------------------------------------------------------------

/// pressure_flush uploads packs AND syncs the manifest, so uploaded packs
/// are always referenced by the manifest in S3.
///
/// Before the fix, pressure_flush only called flush_packs (no sync_manifest),
/// meaning a crash after pressure_flush would leave orphaned packs in S3 —
/// the blocks would still be on local SSD and re-flushed on recovery, but
/// the S3 uploads were wasted work.
#[tokio::test]
async fn test_pressure_flush_syncs_manifest() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
        .await
        .unwrap();

    // Create export and write data
    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    let handler = router.get_handler("vm1").await.unwrap();
    for i in 0..5 {
        handler
            .write(
                i as u64 * BLOCK_SIZE as u64,
                &vec![(i + 1) as u8; BLOCK_SIZE],
                false,
            )
            .await
            .unwrap();
    }

    // Run pressure_flush — should flush packs AND sync manifest
    router.pressure_flush().await;

    // Verify manifest exists in S3 with chunk references.
    // Router's ContentStore base path is "{db_path}/exports/{export_name}".
    let cs = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");
    let (manifest_data, _etag) = cs
        .get_manifest("vm1")
        .await
        .expect("should succeed")
        .expect("manifest should exist after pressure_flush");
    let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
    assert!(
        !vm.chunks.is_empty(),
        "manifest should have chunk entries after pressure_flush + sync_manifest"
    );
}

/// pressure_flush + drain_export running concurrently should not corrupt data.
/// The flush_lock serializes access: pressure_flush uses try_lock and skips
/// exports where drain already holds the lock.
#[tokio::test]
async fn test_pressure_flush_concurrent_with_drain() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = Arc::new(
        ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
            .await
            .unwrap(),
    );

    // Create export and write data
    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    let handler = router.get_handler("vm1").await.unwrap();
    for i in 0..20 {
        handler
            .write(
                i as u64 * BLOCK_SIZE as u64,
                &vec![(i + 1) as u8; BLOCK_SIZE],
                false,
            )
            .await
            .unwrap();
    }

    // Run pressure_flush and drain concurrently — both should succeed
    let router_pressure = Arc::clone(&router);
    let pressure_handle = tokio::spawn(async move {
        router_pressure.pressure_flush().await;
    });

    let router_drain = Arc::clone(&router);
    let drain_handle = tokio::spawn(async move {
        router_drain.drain_export("vm1").await.unwrap();
    });

    tokio::try_join!(pressure_handle, drain_handle).expect("tasks should not panic");

    // Verify data integrity: cold reader can read all blocks.
    // Router's ContentStore base path is "{db_path}/exports/{export_name}".
    let reader_dir = TempDir::new().unwrap();
    let cs = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");
    let (manifest_data, _etag) = cs
        .get_manifest("vm1")
        .await
        .expect("should succeed")
        .expect("manifest should exist");
    let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
    assert!(
        !vm.chunks.is_empty(),
        "manifest should reference chunks after drain"
    );

    // Cold-read every block via a fresh cache
    let reader_config = glidefs::block::write_cache::WriteCacheConfig {
        cache_dir: reader_dir.path().to_path_buf(),
        device_name: "vm1".to_string(),
        device_size: 256 * 1024 * 1024,
        block_size: BLOCK_SIZE,
        wal_sync: false, bottomless: false,
    };
    let reader_cache = glidefs::block::write_cache::WriteCache::open_fresh_active(reader_config)
        .unwrap();
    let reader_pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let reader_vm = Arc::new(parking_lot::RwLock::new(vm));
    let reader_clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let reader_metrics = Arc::new(glidefs::block::metrics::ExportMetrics::new());

    for i in 0..20 {
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
            "block {} should have correct seed byte after concurrent pressure_flush + drain",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// Fix 3: snapshot_export releases exports lock before flush
// ---------------------------------------------------------------------------

/// Snapshot on export A should not block creation of export B.
/// Before the fix, snapshot_export held the exports read lock for the entire
/// flush duration, which would block create_export (which needs the write lock).
#[tokio::test]
async fn test_snapshot_does_not_block_create() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = Arc::new(
        ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
            .await
            .unwrap(),
    );

    // Create export A
    router
        .create_export(test_export_config("export-a"), false, None, None)
        .await
        .unwrap();

    // Write data to export A so snapshot has work to do
    let handler = router.get_handler("export-a").await.unwrap();
    for i in 0..10 {
        handler
            .write(
                i as u64 * BLOCK_SIZE as u64,
                &vec![(i + 1) as u8; BLOCK_SIZE],
                false,
            )
            .await
            .unwrap();
    }

    // Run snapshot and create concurrently — both should succeed
    let router_snap = Arc::clone(&router);
    let snap_handle = tokio::spawn(async move {
        router_snap
            .snapshot_export("export-a", None)
            .await
            .unwrap()
    });

    let router_create = Arc::clone(&router);
    let create_handle = tokio::spawn(async move {
        router_create
            .create_export(test_export_config("export-b"), false, None, None)
            .await
            .unwrap()
    });

    // Both should complete without deadlock or timeout
    tokio::try_join!(snap_handle, create_handle).expect("tasks should not panic");

    // Verify both exports exist
    let exports = router.list_exports().await;
    let names: Vec<_> = exports.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"export-a"), "export-a should exist");
    assert!(names.contains(&"export-b"), "export-b should exist");
}

// ---------------------------------------------------------------------------
// drain_all returns per-export errors when S3 is unavailable
// ---------------------------------------------------------------------------

/// drain_all returns errors for each export that failed, without preventing
/// other exports from draining. Callers (e.g. shutdown) use the returned
/// errors to decide whether to retry or abort.
#[tokio::test]
async fn test_drain_all_returns_errors_on_s3_failure() {
    let s3 = Arc::new(FailingObjectStore::new());
    let dir = TempDir::new().unwrap();
    let router = Arc::new(
        ExportRouter::new(create_router_config(Arc::clone(&s3) as _, &dir))
            .await
            .unwrap(),
    );

    // Create two exports and write data to both
    for name in &["vm1", "vm2"] {
        router
            .create_export(test_export_config(name), false, None, None)
            .await
            .unwrap();

        let handler = router.get_handler(name).await.unwrap();
        for i in 0..5 {
            handler
                .write(
                    i as u64 * BLOCK_SIZE as u64,
                    &vec![(i + 1) as u8; BLOCK_SIZE],
                    false,
                )
                .await
                .unwrap();
        }
    }

    // Fail S3 — drain_all should return errors for both exports
    s3.set_fail_puts(true);
    let failed = router.drain_all().await;
    assert_eq!(
        failed.len(),
        2,
        "both exports should fail when S3 is unavailable"
    );
    let failed_names: Vec<_> = failed.iter().map(|(n, _)| n.as_str()).collect();
    assert!(failed_names.contains(&"vm1"), "vm1 should be in errors");
    assert!(failed_names.contains(&"vm2"), "vm2 should be in errors");

    // Fix S3 — drain_all should now succeed
    s3.set_fail_puts(false);
    let failed = router.drain_all().await;
    assert!(
        failed.is_empty(),
        "drain_all should succeed after S3 recovers, got: {:?}",
        failed.iter().map(|(n, e)| format!("{n}: {e}")).collect::<Vec<_>>()
    );

    // Verify data integrity from a cold reader for both exports
    for name in &["vm1", "vm2"] {
        let cs = ContentStore::new(Arc::clone(&s3) as _, &format!("test/exports/{name}"));
        let (manifest_data, _etag) = cs
            .get_manifest(name)
            .await
            .expect("should succeed")
            .expect("manifest should exist after drain");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
        assert!(
            !vm.chunks.is_empty(),
            "{name} manifest should have chunk entries"
        );
    }
}

// ---------------------------------------------------------------------------
// resize_export: happy path + shrink rejection
// ---------------------------------------------------------------------------

/// resize_export grows the device, preserving existing data.
#[tokio::test]
async fn test_resize_export_preserves_data() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
        .await
        .unwrap();

    // Create a 10MB export and write data
    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    let handler = router.get_handler("vm1").await.unwrap();
    for i in 0..5 {
        handler
            .write(
                i as u64 * BLOCK_SIZE as u64,
                &vec![(i + 1) as u8; BLOCK_SIZE],
                false,
            )
            .await
            .unwrap();
    }

    // Resize from 10MB (0.01 GB) to 20MB (0.02 GB)
    router.resize_export("vm1", 0.02).await.unwrap();

    // Export should have the new size
    let handler = router.get_handler("vm1").await.unwrap();
    let new_size = handler.device_size();
    let expected_size = (0.02 * 1_073_741_824.0) as u64;
    assert_eq!(new_size, expected_size, "device should be 20MB after resize");

    // Original data should still be readable
    for i in 0..5 {
        let data = handler
            .read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert_eq!(
            data[0],
            (i + 1) as u8,
            "block {} should have original data after resize",
            i
        );
    }
}

/// resize_export rejects shrink attempts.
#[tokio::test]
async fn test_resize_export_rejects_shrink() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
        .await
        .unwrap();

    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    // Try to shrink — should fail
    let result = router.resize_export("vm1", 0.005).await;
    assert!(result.is_err(), "shrinking should be rejected");
}

/// resize_export is idempotent when requested size <= current size.
#[tokio::test]
async fn test_resize_export_idempotent_same_size() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
        .await
        .unwrap();

    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    // Resize to same size — should be a no-op
    router.resize_export("vm1", 0.01).await.unwrap();

    // Export should still work
    let handler = router.get_handler("vm1").await.unwrap();
    handler
        .write(0, &vec![0xAA; BLOCK_SIZE], false)
        .await
        .unwrap();
    let data = handler.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert_eq!(data[0], 0xAA, "export should work after no-op resize");
}

// ---------------------------------------------------------------------------
// create_export idempotency
// ---------------------------------------------------------------------------

/// create_export with the same name twice should not error or corrupt state.
///
/// CLAUDE.md requires: "Check before create; don't error if it exists."
/// Data written before the second create_export call must survive.
#[tokio::test]
async fn test_create_export_idempotent_same_name() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
        .await
        .unwrap();

    // First create
    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    // Write data to the export
    let handler = router.get_handler("vm1").await.unwrap();
    handler
        .write(0, &vec![0xBB; BLOCK_SIZE], false)
        .await
        .unwrap();

    // Second create with same name — should succeed (idempotent)
    let result = router
        .create_export(test_export_config("vm1"), false, None, None)
        .await;
    assert!(
        result.is_ok(),
        "second create_export should not error: {:?}",
        result.err()
    );

    // Data written before the second create should still be readable
    let handler = router.get_handler("vm1").await.unwrap();
    let data = handler.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert_eq!(
        data[0], 0xBB,
        "data from before second create_export should survive"
    );

    // Export list should contain exactly one "vm1"
    let exports = router.list_exports().await;
    let vm1_count = exports.iter().filter(|e| e.name == "vm1").count();
    assert_eq!(vm1_count, 1, "should not duplicate the export");
}

// ---------------------------------------------------------------------------
// promote_export idempotency + error paths
// ---------------------------------------------------------------------------

/// promote_export on an already read-write export is a no-op.
#[tokio::test]
async fn test_promote_export_already_readwrite() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
        .await
        .unwrap();

    // Create a normal (read-write) export
    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    // Promote should succeed (idempotent no-op)
    let result = router.promote_export("vm1").await;
    assert!(
        result.is_ok(),
        "promote on already-writable export should succeed: {:?}",
        result.err()
    );

    // Export should still work
    let handler = router.get_handler("vm1").await.unwrap();
    handler
        .write(0, &vec![0xCC; BLOCK_SIZE], false)
        .await
        .unwrap();
    let data = handler.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert_eq!(data[0], 0xCC);
}

/// promote_export on a non-existent export returns ExportNotFound.
#[tokio::test]
async fn test_promote_export_not_found() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(create_router_config(Arc::clone(&s3), &dir))
        .await
        .unwrap();

    let result = router.promote_export("nonexistent").await;
    assert!(result.is_err(), "promote on missing export should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found") || err.contains("Not found"),
        "error should indicate export not found, got: {err}"
    );
}
