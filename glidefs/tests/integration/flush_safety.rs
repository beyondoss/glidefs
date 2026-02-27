//! Flush safety tests for GlideFS.
//!
//! These tests verify:
//! 1. pressure_flush syncs the manifest so uploaded packs are always referenced
//! 2. pressure_flush + concurrent drain don't corrupt data
//! 3. snapshot_export does not hold the exports read lock during flush

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::router::{ExportRouter, RouterConfig};
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::config::ExportConfig;

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
        wal_sync: false,
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
            .unwrap();
    }

    // Run pressure_flush — should flush packs AND sync manifest
    router.pressure_flush().await;

    // Verify manifest exists in S3 with chunk references.
    // Router's ContentStore base path is "{db_path}/exports/{export_name}".
    let cs = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");
    let manifest_data = cs
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
    let manifest_data = cs
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
