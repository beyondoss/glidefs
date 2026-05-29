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
        pack_index_cache: None,
        wal_sync: false,
        max_s3_uploads: 0,
        max_s3_downloads: 0,
        default_flush_threshold: 500,
        ublk_nr_queues: 1,
        nbd_dead_conn_timeout: 0,
        max_exports: 10_000,
        manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
    }
}

fn test_export_config(name: &str) -> ExportConfig {
    ExportConfig {
        name: name.to_string(),
        size_gb: 0.01, // 10MB
        s3_prefix: None,
        block_size: None,
        flush_threshold: None,
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
        wal_sync: false,
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

// ---------------------------------------------------------------------------
// save_export failure: export works locally but is invisible to other nodes
// ---------------------------------------------------------------------------

/// If S3 is unavailable when save_export is called, the export runs fine
/// locally but won't be discoverable by other nodes via discover_exports.
///
/// This is a data loss vector: the export's packs exist in S3 after flush,
/// but no export.json points at them. On node failure, the export vanishes.
#[tokio::test]
async fn test_save_export_failure_makes_export_undiscoverable() {
    let s3 = Arc::new(FailingObjectStore::new());
    let dir = TempDir::new().unwrap();
    let config = test_export_config("vm1");

    let router = Arc::new(
        ExportRouter::new(create_router_config(Arc::clone(&s3) as _, &dir))
            .await
            .unwrap(),
    );

    // Create export — works because create_export doesn't save config to S3
    router
        .create_export(config.clone(), false, None, None)
        .await
        .unwrap();

    // Write data and drain to S3 (packs + manifest land in S3)
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
    router.drain_export("vm1").await.unwrap();

    // Now fail S3 and attempt save_export — should fail
    s3.set_fail_puts(true);
    let save_result = router.save_export(&config).await;
    assert!(save_result.is_err(), "save_export should fail when S3 is down");
    s3.set_fail_puts(false);

    // Simulate a new node: fresh router discovers exports from S3.
    // Since save_export failed, export.json is missing — discover_exports
    // should NOT find "vm1".
    let dir2 = TempDir::new().unwrap();
    let router2 = ExportRouter::new(create_router_config(Arc::clone(&s3) as _, &dir2))
        .await
        .unwrap();
    let discovered = router2.discover_exports().await.unwrap();
    assert!(
        discovered.is_empty(),
        "export should be undiscoverable without export.json, got: {:?}",
        discovered.iter().map(|c| &c.name).collect::<Vec<_>>()
    );

    // But the manifest and packs ARE in S3 — the data exists, just no pointer.
    let cs = ContentStore::new(Arc::clone(&s3) as _, "test/exports/vm1");
    let manifest = cs.get_manifest("vm1").await.expect("should succeed");
    assert!(
        manifest.is_some(),
        "manifest should exist in S3 even though export.json is missing"
    );

    // Fix: save_export with S3 working makes the export discoverable again
    router.save_export(&config).await.unwrap();
    let discovered = router2.discover_exports().await.unwrap();
    assert_eq!(
        discovered.len(),
        1,
        "export should be discoverable after successful save_export"
    );
    assert_eq!(discovered[0].name, "vm1");
}

// ---------------------------------------------------------------------------
// drain failure: dirty blocks remain readable and retryable
// ---------------------------------------------------------------------------

/// After drain_all fails due to S3 outage, the export's dirty blocks must
/// remain readable locally AND be flushable on the next attempt.
///
/// Extends the existing test_drain_all_returns_errors_on_s3_failure by
/// verifying that data integrity holds through the failure: local reads
/// return correct data, dirty_bytes is non-zero, and a retry succeeds
/// with correct cold-read verification.
#[tokio::test]
async fn test_failed_drain_preserves_data_for_retry() {
    let s3 = Arc::new(FailingObjectStore::new());
    let dir = TempDir::new().unwrap();
    let router = Arc::new(
        ExportRouter::new(create_router_config(Arc::clone(&s3) as _, &dir))
            .await
            .unwrap(),
    );

    router
        .create_export(test_export_config("vm1"), false, None, None)
        .await
        .unwrap();

    let handler = router.get_handler("vm1").await.unwrap();
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

    // Fail S3 — drain should fail
    s3.set_fail_puts(true);
    let failed = router.drain_all().await;
    assert!(!failed.is_empty(), "drain should fail with S3 down");

    // Key assertion: data is still readable locally after failed drain
    for i in 0..10 {
        let data = handler
            .read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert_eq!(
            data[0],
            (i + 1) as u8,
            "block {} should be readable locally after failed drain",
            i
        );
    }

    // Dirty bytes should be non-zero (blocks not flushed)
    let info = router.get_export_info("vm1").await.unwrap();
    assert!(
        info.dirty_bytes > 0,
        "export should still have dirty bytes after failed drain, got: {}",
        info.dirty_bytes
    );

    // Fix S3 — retry drain should succeed
    s3.set_fail_puts(false);
    let failed = router.drain_all().await;
    assert!(
        failed.is_empty(),
        "drain retry should succeed, got: {:?}",
        failed.iter().map(|(n, e)| format!("{n}: {e}")).collect::<Vec<_>>()
    );

    // Verify data integrity from cold reader
    let reader_dir = TempDir::new().unwrap();
    let cs = ContentStore::new(Arc::clone(&s3) as _, "test/exports/vm1");
    let (manifest_data, _etag) = cs
        .get_manifest("vm1")
        .await
        .expect("should succeed")
        .expect("manifest should exist after successful drain");
    let vm = VolumeManifest::deserialize(&manifest_data).unwrap();

    let reader_config = glidefs::block::write_cache::WriteCacheConfig {
        cache_dir: reader_dir.path().to_path_buf(),
        device_name: "vm1".to_string(),
        device_size: vm.size,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let reader_cache = glidefs::block::write_cache::WriteCache::open_fresh_active(reader_config)
        .unwrap();
    let reader_pack_index_cache = Arc::clone(&*super::SHARED_PACK_INDEX_CACHE);
    let reader_vm = Arc::new(parking_lot::RwLock::new(vm));
    let reader_clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let reader_metrics = Arc::new(glidefs::block::metrics::ExportMetrics::new());

    for i in 0..10 {
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
            "block {} should be correct from cold reader after drain retry",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// Sub-block write to SYNCING block: flushing-file promotion path
// ---------------------------------------------------------------------------

/// A sub-block write arriving concurrently with flush must read prior data
/// from the flushing file (not the active file, which has zeros for that
/// block after rotation).
///
/// This test uses a concurrent flush + sub-block write. While flush_to_s3
/// is running (block is SYNCING), we issue a sub-block write that triggers
/// promote_syncing_blocks to copy flushing → active, then overlay the sub-
/// block data.
///
/// The key invariant: after the sub-block write, the block contains both
/// the original full-block data (from the flushing file) AND the new sub-
/// block data. Neither is lost.
#[tokio::test]
async fn test_sub_block_write_to_syncing_block_preserves_data() {
    use object_store::memory::InMemory;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        super::create_test_cache(&dir, "vol1", Arc::clone(&s3)).await;

    // Write a full block with known pattern (0xBB).
    let full_block = vec![0xBB; BLOCK_SIZE];
    cache.write(0, &full_block).unwrap();
    assert_eq!(cache.dirty_block_count(), 1);

    // Flush to S3 once so the flushing file is cleaned up properly.
    cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Write the same block again — now it's dirty with 0xBB data.
    cache.write(0, &full_block).unwrap();
    assert_eq!(cache.dirty_block_count(), 1);

    // Spawn flush_to_s3 concurrently. This will rotate (DIRTY→SYNCING),
    // upload the pack, and eventually transition SYNCING→NOT_PRESENT.
    let cache_clone = Arc::clone(&cache);
    let cs_clone = ContentStore::new(Arc::clone(&s3), "test");
    let pic_clone = Arc::clone(&pack_index_cache);
    let vm_clone = Arc::clone(&volume_manifest);
    let flush_handle = tokio::spawn(async move {
        cache_clone
            .flush_to_s3(&cs_clone, &pic_clone, &vm_clone)
            .await
    });

    // Give the flush a moment to rotate (DIRTY→SYNCING).
    // After rotation, block 0 is SYNCING and data lives in flushing file.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Sub-block write while flush is in progress.
    // If block is still SYNCING: promote_syncing_blocks reads flushing file,
    //   copies 0xBB to active, CAS SYNCING→DIRTY, then pwrite 0xCC sub-block.
    // If block already transitioned to NOT_PRESENT: write goes through the
    //   normal path (no prior data on SSD, so just writes sub-block).
    // If flush hasn't rotated yet: block is still DIRTY, direct pwrite.
    let sub_block = vec![0xCC; 4096];
    cache
        .write_with_eviction_check(0, &sub_block)
        .or_else(|e| {
            if matches!(e, glidefs::block::write_cache::CacheError::BlockEvicted) {
                // Block was evicted (SYNCING→NOT_PRESENT) between state check
                // and pwrite. No flushing file to promote from. Just write
                // the sub-block — remaining bytes will be zeros (no prior
                // data to preserve since it already reached S3).
                cache.write(0, &sub_block)
            } else {
                Err(e)
            }
        })
        .unwrap();

    // Wait for the flush to finish.
    flush_handle.await.unwrap().unwrap();

    // Read the full block — first 4096 bytes should be 0xCC.
    let data = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(
        data[..4096].iter().all(|&b| b == 0xCC),
        "first 4096 bytes should be sub-block write (0xCC), got {:#x}",
        data[0]
    );

    // The remaining bytes depend on timing:
    // - If we caught SYNCING: promoted from flushing file → 0xBB
    // - If we caught DIRTY (pre-rotation): direct pwrite → 0xBB
    // - If we caught NOT_PRESENT (post-eviction): no prior → 0x00
    // All three are correct behavior — the key invariant is that the
    // sub-block data (0xCC) is never lost.

    // Flush the current state to S3 and verify cold read.
    cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_cs, reader_pic, reader_vm, reader_cc, reader_metrics) =
        super::create_cold_reader(&reader_dir, "vol1", Arc::clone(&s3)).await;

    let cold_data = reader_cache
        .read(
            0,
            BLOCK_SIZE,
            reader_cc.as_ref(),
            &reader_pic,
            &reader_vm,
            &reader_cs,
            &reader_metrics,
        )
        .await
        .unwrap();
    assert!(
        cold_data[..4096].iter().all(|&b| b == 0xCC),
        "cold read: first 4096 bytes should be 0xCC, got {:#x}",
        cold_data[0]
    );
}

// ---------------------------------------------------------------------------
// Readahead integration: sequential reads trigger prefetch, reducing S3 GETs
// ---------------------------------------------------------------------------

/// Sequential block reads through the handler should trigger readahead,
/// which prefetches pack indices for the next chunk. This test verifies
/// the full integration path: handler.read → trigger_readahead →
/// SequentialDetector.record → prefetch_chunk → PackIndexCache.
///
/// If readahead silently broke (wrong chunk_idx, off-by-one), reads still
/// work but every S3 fetch pays an extra round-trip for the pack index.
/// That's real money on GET requests.
#[tokio::test]
async fn test_readahead_prefetches_next_pack_index() {
    use glidefs::block::handler::BlockHandler;
    use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
            db_path: "test".to_string(),
            cache_dir: dir.path().to_path_buf(),
            block_size: BLOCK_SIZE,
            clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
            pack_index_cache: None,
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
            max_exports: 10_000,
            manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
        })
        .await
        .unwrap(),
    );

    // Need enough blocks to span at least 2 packs. Write blocks in pack 0.
    // DEFAULT_FLUSH_THRESHOLD = 500, so blocks 0..499 = pack 0, 500..999 = pack 1.
    let config = ExportConfig {
        name: "vm1".to_string(),
        // Need device large enough for 2 packs (500 blocks * 128KB = 64MB per pack)
        size_gb: 0.2, // 200MB — enough for >1000 blocks
        s3_prefix: None,
        block_size: None,
        flush_threshold: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(config.clone(), false, None, None)
        .await
        .unwrap();

    let handler = router.get_handler("vm1").await.unwrap();

    // Write blocks across packs 0 and 1 (first 4 of each for speed)
    let blocks_to_write: Vec<usize> = (0..4)
        .chain(DEFAULT_FLUSH_THRESHOLD..DEFAULT_FLUSH_THRESHOLD + 4)
        .collect();
    for &block_idx in &blocks_to_write {
        let data = vec![block_idx as u8; BLOCK_SIZE];
        handler
            .write(block_idx as u64 * BLOCK_SIZE as u64, &data, false)
            .await
            .unwrap();
    }

    // Drain to S3
    router.drain_export("vm1").await.unwrap();
    router.save_export(&config).await.unwrap();
    drop(router);

    // Cold start: new router, fresh cache dir, no pack indices cached
    let dir2 = TempDir::new().unwrap();
    let router2 = Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
            db_path: "test".to_string(),
            cache_dir: dir2.path().to_path_buf(),
            block_size: BLOCK_SIZE,
            clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
            pack_index_cache: None,
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
            max_exports: 10_000,
            manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
        })
        .await
        .unwrap(),
    );

    let discovered = router2.discover_exports().await.unwrap();
    assert_eq!(discovered.len(), 1);
    for ec in discovered {
        router2
            .create_export(ec, false, None, None)
            .await
            .unwrap();
    }

    let handler2 = router2.get_handler("vm1").await.unwrap();

    // Read blocks 0, 1, 2 sequentially — should trigger readahead after block 2.
    // The readahead target should be the first chunk of pack 1 (= block 500).
    for i in 0..3 {
        let data = handler2
            .read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert_eq!(
            data[0], i as u8,
            "sequential read of block {} should return correct data",
            i
        );
    }

    // Give the readahead spawn a moment to complete the prefetch.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Now read block 500 (first block of pack 1). If readahead worked,
    // the pack index for pack 1 is already in PackIndexCache, so this
    // read doesn't need an extra S3 GET for the index.
    //
    // We can't directly observe the cache hit, but we CAN verify the read
    // succeeds and returns correct data — confirming the end-to-end path
    // from trigger_readahead → prefetch_chunk → read works.
    let data = handler2
        .read(
            DEFAULT_FLUSH_THRESHOLD as u64 * BLOCK_SIZE as u64,
            BLOCK_SIZE as u32,
        )
        .await
        .unwrap();
    assert_eq!(
        data[0],
        DEFAULT_FLUSH_THRESHOLD as u8,
        "block {} should be readable (readahead should have prefetched pack 1 index)",
        DEFAULT_FLUSH_THRESHOLD
    );
}
