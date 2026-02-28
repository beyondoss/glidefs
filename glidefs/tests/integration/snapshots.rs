//! Integration tests for explicit snapshot retention.
//!
//! These tests verify:
//! 1. `snapshot()` persists a versioned S3 key alongside the current manifest
//! 2. Multiple snapshots accumulate and are listed in order
//! 3. Background `sync_manifest` does NOT create snapshot keys
//! 4. Fork-from-snapshot restores state at a specific sequence
//! 5. GC respects packs referenced by snapshot manifests
//! 6. Deleting a snapshot + GC frees previously pinned packs
//! 7. Purging an export deletes all snapshots
//! 8. HTTP API list/delete round-trips work

use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use tempfile::TempDir;

use glidefs::block::content_store::ContentStore;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::cli::gc::{new_gc_state_for_test, reconcile_prefix_for_test};

use super::create_test_cache;

const BLOCK_SIZE: usize = 128 * 1024;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_blocks(
    cache: &glidefs::block::write_cache::WriteCache<glidefs::block::state::Active>,
    start: usize,
    count: usize,
    seed: u8,
    clean_cache: &dyn glidefs::block::cache::BlockCache,
) {
    for i in 0..count {
        let offset = (start + i) * BLOCK_SIZE;
        let mut data = vec![0u8; BLOCK_SIZE];
        data[0] = seed;
        let idx = (start + i) as u16;
        data[1..3].copy_from_slice(&idx.to_le_bytes());
        for (b, byte) in data.iter_mut().enumerate().take(BLOCK_SIZE).skip(3) {
            *byte = ((i + b) % 256) as u8;
        }
        cache.write(offset as u64, &data, clean_cache).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Calling snapshot() persists a versioned key at snapshots/{name}/{seq:020}.
#[tokio::test]
async fn test_snapshot_persists_versioned_key() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    write_blocks(&cache, 0, 3, 1, cc.as_ref());
    let result = cache.snapshot(&cs, &pack_index_cache, &volume_manifest).await.unwrap();

    // Snapshot sequence should be listed
    let snapshots = cs.list_snapshots("vm1").await.unwrap();
    assert_eq!(snapshots, vec![result.sequence]);

    // Snapshot bytes should deserialize to a valid volume manifest
    let data = cs
        .get_snapshot("vm1", result.sequence)
        .await
        .unwrap()
        .expect("snapshot should exist");
    let vm = VolumeManifest::deserialize(&data).unwrap();
    // 3 blocks written into a single chunk → 1 chunk entry
    assert!(!vm.chunks.is_empty(), "volume manifest should have chunk entries");
}

/// Multiple snapshots accumulate and list in ascending order.
#[tokio::test]
async fn test_multiple_snapshots_accumulate() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    write_blocks(&cache, 0, 2, 1, cc.as_ref());
    let r1 = cache.snapshot(&cs, &pack_index_cache, &volume_manifest).await.unwrap();

    write_blocks(&cache, 2, 2, 2, cc.as_ref());
    let r2 = cache.snapshot(&cs, &pack_index_cache, &volume_manifest).await.unwrap();

    write_blocks(&cache, 4, 2, 3, cc.as_ref());
    let r3 = cache.snapshot(&cs, &pack_index_cache, &volume_manifest).await.unwrap();

    let snapshots = cs.list_snapshots("vm1").await.unwrap();
    assert_eq!(snapshots, vec![r1.sequence, r2.sequence, r3.sequence]);
}

/// Background sync_manifest does NOT create snapshot keys.
#[tokio::test]
async fn test_sync_manifest_does_not_create_snapshots() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    write_blocks(&cache, 0, 3, 1, cc.as_ref());

    // flush_packs + sync_manifest (the background flush path)
    let (_stats, _seq) = cache.flush_packs(&cs, &pack_index_cache, &volume_manifest).await.unwrap();
    cache.sync_manifest(&cs, &volume_manifest).await.unwrap();

    // No snapshot should be created by sync_manifest
    let snapshots = cs.list_snapshots("vm1").await.unwrap();
    assert!(snapshots.is_empty(), "sync_manifest should not create snapshot keys");
}

/// Fork from a specific snapshot_sequence restores state at that point in time.
#[tokio::test]
async fn test_fork_from_snapshot() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::config::ExportConfig;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(RouterConfig {
        object_store: Arc::clone(&s3),
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
    })
    .await
    .unwrap();

    let config = ExportConfig {
        name: "vm1".to_string(),
        size_gb: 0.01,
        s3_prefix: None,
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(config, false, None, None)
        .await
        .unwrap();

    // Write block 0 with seed 0xAA and snapshot
    let handler = router.get_handler("vm1").await.unwrap();
    handler.write(0, &[0xAA; BLOCK_SIZE], false).unwrap();
    let snap1 = router.snapshot_export("vm1", None).await.unwrap();

    // Overwrite block 0 with seed 0xBB and snapshot again
    handler.write(0, &[0xBB; BLOCK_SIZE], false).unwrap();
    let _snap2 = router.snapshot_export("vm1", None).await.unwrap();

    // Fork from snap1 — should see 0xAA, not 0xBB
    let fork_config = ExportConfig {
        name: "fork1".to_string(),
        size_gb: 0.01,
        s3_prefix: Some("vm1".to_string()),
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(fork_config, false, Some("vm1"), Some(snap1.sequence))
        .await
        .unwrap();

    let fork_handler = router.get_handler("fork1").await.unwrap();
    let fork_data = fork_handler.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        fork_data.iter().all(|&b| b == 0xAA),
        "fork from snap1 should see snap1's data (0xAA), not snap2's (0xBB)"
    );
}

/// GC respects packs referenced by snapshot manifests.
#[tokio::test]
async fn test_gc_respects_snapshot_packs() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3)).await;

    // Write blocks and snapshot (creates snapshot manifest referencing pack_A)
    write_blocks(&cache, 0, 3, 1, cc.as_ref());
    let snap = cache.snapshot(&cs, &pack_index_cache, &volume_manifest).await.unwrap();

    // Overwrite same blocks with different data and flush (creates pack_B)
    write_blocks(&cache, 0, 3, 2, cc.as_ref());
    cache.snapshot(&cs, &pack_index_cache, &volume_manifest).await.unwrap();

    // pack_A is no longer referenced by manifests/{name} but IS referenced by the first snapshot.
    // GC should NOT delete pack_A.
    let mut gc_state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(
        &cs,
        &mut gc_state,
        Duration::from_secs(0), // no grace period
        1000,
        false, // not dry run
    )
    .await
    .unwrap();

    // Verify the snapshot's chunk packs are still live
    let snapshot_data = cs
        .get_snapshot("vm1", snap.sequence)
        .await
        .unwrap()
        .expect("snapshot should still exist");
    let vm = VolumeManifest::deserialize(&snapshot_data).unwrap();
    // Verify referenced packs still exist in S3 (v4: pack IDs are in the manifest directly)
    for (&chunk_idx, entry) in &vm.chunks {
        let packs = cs.list_chunk_packs(chunk_idx).await.unwrap();
        for &pack_id in &entry.packs {
            assert!(
                packs.iter().any(|p| p.contains(&glidefs::block::pack::pack_id_to_string(pack_id))),
                "pack referenced by snapshot should not be deleted"
            );
        }
    }

    // No packs should have been deleted (both snap1 and snap2 reference their packs)
    assert_eq!(report.packs_deleted(), 0, "GC should not delete packs referenced by snapshots");
}

/// Deleting a snapshot allows GC to free packs only referenced by that snapshot.
///
/// In v4, manifests accumulate pack_ids (append_pack), so overwriting blocks
/// doesn't immediately orphan old packs — compaction handles that. This test
/// creates orphan packs that are only kept alive by a snapshot manifest.
#[tokio::test]
async fn test_delete_snapshot_frees_packs_for_gc() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3)).await;

    // Write blocks and flush to create live packs
    write_blocks(&cache, 0, 3, 1, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Upload orphan packs to S3 (not in current manifest)
    let orphan_pack_id: u64 = 0xDEAD_5AAB_0000_0001;
    cs.put_chunk_pack(0, orphan_pack_id, b"snapshot-only pack data".to_vec())
        .await
        .unwrap();

    // Build a snapshot manifest that includes the orphan pack along with live packs
    let mut snap_vm = volume_manifest.read().clone();
    snap_vm.append_pack(0, orphan_pack_id);
    let snap_seq = 1u64;
    cs.put_snapshot("vm1", snap_seq, snap_vm.serialize())
        .await
        .unwrap();

    // GC should NOT delete the orphan pack (snapshot references it)
    let mut gc_state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(&cs, &mut gc_state, Duration::ZERO, 1000, false)
        .await
        .unwrap();
    assert_eq!(report.dead_found(), 0, "orphan pack should be live via snapshot");

    // Delete the snapshot
    cs.delete_snapshot("vm1", snap_seq).await.unwrap();

    // Delete again — must be idempotent
    cs.delete_snapshot("vm1", snap_seq).await.unwrap();

    // GC should now find the orphan pack as dead (no manifest references it)
    let report2 = reconcile_prefix_for_test(&cs, &mut gc_state, Duration::ZERO, 1000, false)
        .await
        .unwrap();

    assert!(
        report2.dead_found() >= 1,
        "GC should find orphaned pack after snapshot deletion, got {} dead",
        report2.dead_found()
    );
}

/// Purging an export deletes all its snapshots.
#[tokio::test]
async fn test_purge_export_deletes_snapshots() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::config::ExportConfig;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(RouterConfig {
        object_store: Arc::clone(&s3),
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
    })
    .await
    .unwrap();

    let config = ExportConfig {
        name: "vm1".to_string(),
        size_gb: 0.01,
        s3_prefix: None,
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(config, false, None, None)
        .await
        .unwrap();

    // Write and snapshot
    router.snapshot_export("vm1", None).await.unwrap();

    // Verify snapshot exists
    let cs = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");
    assert!(!cs.list_snapshots("vm1").await.unwrap().is_empty());

    // Remove with purge
    router.remove_export("vm1", true).await.unwrap();

    // Snapshots should be gone
    assert!(cs.list_snapshots("vm1").await.unwrap().is_empty());
}

/// HTTP API: list snapshots and delete a snapshot.
#[tokio::test]
async fn test_api_list_and_delete_snapshots() {
    use bytes::Bytes;
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use http_body_util::{BodyExt, Full};
    use hyper::{Method, Request, StatusCode};

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
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
        })
        .await
        .unwrap(),
    );

    // Create export via API
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/api/exports/vm1")
        .body(Full::new(Bytes::from(r#"{"size_gb": 0.01}"#)))
        .unwrap();
    let resp = glidefs::block::api::handle_request_for_test(Arc::clone(&router), req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Snapshot via API
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/exports/vm1/snapshot")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = glidefs::block::api::handle_request_for_test(Arc::clone(&router), req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let snap: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let seq = snap["sequence"].as_u64().unwrap();

    // List snapshots via API
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/exports/vm1/snapshots")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = glidefs::block::api::handle_request_for_test(Arc::clone(&router), req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let list: Vec<u64> = serde_json::from_slice(&body).unwrap();
    assert_eq!(list, vec![seq]);

    // Delete snapshot via API
    let req = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/api/exports/vm1/snapshots/{}", seq))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = glidefs::block::api::handle_request_for_test(Arc::clone(&router), req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // List should be empty now
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/exports/vm1/snapshots")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = glidefs::block::api::handle_request_for_test(Arc::clone(&router), req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let list: Vec<u64> = serde_json::from_slice(&body).unwrap();
    assert!(list.is_empty());
}

/// Snapshot on a fresh export with zero writes succeeds and is listed.
#[tokio::test]
async fn test_snapshot_empty_export() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, _cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    // Snapshot immediately — no writes. Sequence may be 0 (no flushes yet).
    let result = cache.snapshot(&cs, &pack_index_cache, &volume_manifest).await.unwrap();

    let snapshots = cs.list_snapshots("vm1").await.unwrap();
    assert_eq!(snapshots, vec![result.sequence]);

    // Snapshot manifest should be valid (empty volume manifest)
    let data = cs
        .get_snapshot("vm1", result.sequence)
        .await
        .unwrap()
        .expect("snapshot should exist");
    let vm = VolumeManifest::deserialize(&data).unwrap();
    assert!(vm.chunks.is_empty(), "empty export has no chunk entries");
}

/// remove_export(name, false) does NOT delete snapshots from S3.
#[tokio::test]
async fn test_remove_without_purge_preserves_snapshots() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::config::ExportConfig;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(RouterConfig {
        object_store: Arc::clone(&s3),
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
    })
    .await
    .unwrap();

    let config = ExportConfig {
        name: "vm1".to_string(),
        size_gb: 0.01,
        s3_prefix: None,
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(config, false, None, None)
        .await
        .unwrap();

    router.snapshot_export("vm1", None).await.unwrap();

    let cs = ContentStore::new(Arc::clone(&s3), "test/exports/vm1");
    assert_eq!(cs.list_snapshots("vm1").await.unwrap().len(), 1);

    // Remove WITHOUT purge
    router.remove_export("vm1", false).await.unwrap();

    // Snapshots should still be in S3
    assert_eq!(
        cs.list_snapshots("vm1").await.unwrap().len(),
        1,
        "non-purge remove should preserve snapshots in S3"
    );
}

/// Snapshot with tag publishes manifest under the tag name, fork from tag works.
#[tokio::test]
async fn test_snapshot_tag_and_fork() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::config::ExportConfig;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(RouterConfig {
        object_store: Arc::clone(&s3),
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
    })
    .await
    .unwrap();

    // Create export, write known data, snapshot with tag
    let config = ExportConfig {
        name: "vm1".to_string(),
        size_gb: 0.01,
        s3_prefix: None,
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router.create_export(config, false, None, None).await.unwrap();

    let handler = router.get_handler("vm1").await.unwrap();
    handler.write(0, &[0xCC; BLOCK_SIZE], false).unwrap();

    let snap = router.snapshot_export("vm1", Some("setup-abc123")).await.unwrap();
    assert_eq!(snap.tag.as_deref(), Some("setup-abc123"));

    // Head check: tagged manifest exists, nonexistent doesn't
    assert!(router.head_manifest("vm1", "setup-abc123").await.unwrap());
    assert!(!router.head_manifest("vm1", "nonexistent").await.unwrap());

    // Fork from tag — should see 0xCC
    let fork_config = ExportConfig {
        name: "fork1".to_string(),
        size_gb: 0.01,
        s3_prefix: Some("vm1".to_string()),
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(fork_config, false, Some("setup-abc123"), None)
        .await
        .unwrap();

    let fork_handler = router.get_handler("fork1").await.unwrap();
    let data = fork_handler.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert_eq!(data[0], 0xCC, "fork from tag should see tagged data");
}

/// Standalone tag_export publishes manifest without re-flushing.
#[tokio::test]
async fn test_standalone_tag() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::config::ExportConfig;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(RouterConfig {
        object_store: Arc::clone(&s3),
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
    })
    .await
    .unwrap();

    let config = ExportConfig {
        name: "vm1".to_string(),
        size_gb: 0.01,
        s3_prefix: None,
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router.create_export(config, false, None, None).await.unwrap();

    let handler = router.get_handler("vm1").await.unwrap();
    handler.write(0, &[0xDD; BLOCK_SIZE], false).unwrap();

    // Snapshot first (to flush data), then tag separately
    router.snapshot_export("vm1", None).await.unwrap();
    router.tag_export("vm1", "my-tag").await.unwrap();

    // Tag should exist
    assert!(router.head_manifest("vm1", "my-tag").await.unwrap());

    // Fork from standalone tag
    let fork_config = ExportConfig {
        name: "fork2".to_string(),
        size_gb: 0.01,
        s3_prefix: Some("vm1".to_string()),
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(fork_config, false, Some("my-tag"), None)
        .await
        .unwrap();

    let fork_handler = router.get_handler("fork2").await.unwrap();
    let data = fork_handler.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert_eq!(data[0], 0xDD, "fork from standalone tag should see data");
}

/// After compaction, GC deletes old packs not referenced by snapshots
/// but preserves old packs that a snapshot still references.
///
/// Scenario:
/// 1. Write blocks, flush → pack A
/// 2. Overwrite blocks, flush → pack B  (chunk now has [A, B])
/// 3. Take snapshot (snapshot manifest references [A, B])
/// 4. Compact chunk → new base pack C replaces [A, B] in live manifest
/// 5. Upload new manifest (so GC sees [C], snapshot sees [A, B])
/// 6. GC should NOT delete A or B (snapshot references them)
/// 7. Delete the snapshot → GC should find A and B as dead
#[tokio::test]
async fn test_compaction_old_packs_gc_respects_snapshots() {
    use glidefs::block::pack::PackId;
    use glidefs::block::write_cache::compact;
    use glidefs::cli::gc::{new_gc_state_for_test, reconcile_prefix_for_test};

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3)).await;

    // Flush 1: write blocks → pack A
    write_blocks(&cache, 0, 3, 1, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    let packs_after_flush1: Vec<PackId> = volume_manifest
        .read()
        .chunk_pack_ids(0)
        .unwrap()
        .to_vec();
    assert_eq!(packs_after_flush1.len(), 1);

    // Flush 2: overwrite same blocks → pack B
    write_blocks(&cache, 0, 3, 2, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    let packs_after_flush2: Vec<PackId> = volume_manifest
        .read()
        .chunk_pack_ids(0)
        .unwrap()
        .to_vec();
    assert_eq!(packs_after_flush2.len(), 2);

    // Take snapshot — snapshot manifest references both pack A and pack B
    let snap = cache
        .snapshot(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Compact: merge [A, B] → C, live manifest now has [C]
    let old_packs = packs_after_flush2.clone();
    let blocks_per_chunk = volume_manifest.read().blocks_per_chunk();
    compact::compact_chunk(
        0,
        &old_packs,
        blocks_per_chunk,
        &cs,
        &pack_index_cache,
        &volume_manifest,
    )
    .await
    .unwrap();

    // Upload the compacted manifest so GC sees [C] not [A, B]
    cache.sync_manifest(&cs, &volume_manifest).await.unwrap();

    // GC: packs A and B are on S3 but NOT in the live manifest.
    // However, the snapshot manifest references them → they should be live.
    let mut gc_state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(&cs, &mut gc_state, Duration::ZERO, 1000, false)
        .await
        .unwrap();
    assert_eq!(
        report.dead_found(), 0,
        "old packs should be live via snapshot manifest"
    );

    // Delete the snapshot
    cs.delete_snapshot("vm1", snap.sequence).await.unwrap();

    // GC again: now A and B are truly dead (no manifest references them)
    let report2 = reconcile_prefix_for_test(&cs, &mut gc_state, Duration::ZERO, 1000, false)
        .await
        .unwrap();
    assert!(
        report2.dead_found() >= 2,
        "old packs should be dead after snapshot deletion, got {} dead",
        report2.dead_found()
    );
}

/// Calling snapshot() twice with no writes in between is idempotent.
///
/// The sequence number only advances on writes, so a second snapshot with no
/// intervening writes returns the same sequence and overwrites the same S3 key
/// with identical content. The list shows a single entry, not a duplicate.
#[tokio::test]
async fn test_snapshot_idempotent_when_no_writes() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    // Write some data and take the first snapshot.
    write_blocks(&cache, 0, 3, 1, cc.as_ref());
    let r1 = cache
        .snapshot(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Take a second snapshot with no writes in between.
    let r2 = cache
        .snapshot(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Same sequence — sequence only advances on writes.
    assert_eq!(
        r1.sequence, r2.sequence,
        "snapshot with no writes should return same sequence"
    );

    // Only one entry in the list (same S3 key, overwritten idempotently).
    let snapshots = cs.list_snapshots("vm1").await.unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0], r1.sequence);

    // Manifest is still valid and has the same chunk content.
    let data = cs
        .get_snapshot("vm1", r1.sequence)
        .await
        .unwrap()
        .expect("snapshot should exist");
    let vm = VolumeManifest::deserialize(&data).unwrap();
    assert!(!vm.chunks.is_empty(), "snapshot should have chunk entries");
}

/// Concurrent compaction and fork from the same manifest.
///
/// Compaction replaces [A, B] → [C] via replace_packs_cas. A concurrent fork
/// loads the manifest from S3 and gets either [A, B] (pre-compaction) or [C]
/// (post-compaction). Either way, reads through the fork manifest must return
/// correct data.
#[tokio::test]
async fn test_fork_during_compaction_sees_consistent_data() {
    use glidefs::block::pack::PackId;
    use glidefs::block::write_cache::compact;
    use glidefs::block::metrics::ExportMetrics;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3)).await;

    // Flush 1: write blocks → pack A
    write_blocks(&cache, 0, 3, 1, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Flush 2: overwrite same blocks with seed=2 → pack B (manifest: [A, B])
    write_blocks(&cache, 0, 3, 2, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    let packs_before: Vec<PackId> = volume_manifest
        .read()
        .chunk_pack_ids(0)
        .unwrap()
        .to_vec();
    assert_eq!(packs_before.len(), 2, "should have 2 packs before compaction");

    // Sync manifest to S3 so fork can load it
    cache.sync_manifest(&cs, &volume_manifest).await.unwrap();

    // Run compaction and "fork" (manifest load from S3) concurrently
    let blocks_per_chunk = volume_manifest.read().blocks_per_chunk();
    let cs2 = ContentStore::new(Arc::clone(&s3), "test");

    let compact_handle = {
        let packs = packs_before.clone();
        let pic = Arc::clone(&pack_index_cache);
        let vm = Arc::clone(&volume_manifest);
        let cs_clone = ContentStore::new(Arc::clone(&s3), "test");
        tokio::spawn(async move {
            compact::compact_chunk(0, &packs, blocks_per_chunk, &cs_clone, &pic, &vm)
                .await
                .unwrap();
            // Sync the compacted manifest to S3
            cache.sync_manifest(&cs_clone, &vm).await.unwrap();
        })
    };

    let fork_handle = tokio::spawn(async move {
        // Simulate fork: load manifest from S3 (may be pre- or post-compaction)
        let data = cs2
            .get_manifest("vm1")
            .await
            .unwrap()
            .expect("manifest should exist");
        VolumeManifest::deserialize(&data).unwrap()
    });

    let (compact_result, fork_result) = tokio::join!(compact_handle, fork_handle);
    compact_result.unwrap();
    let fork_manifest = fork_result.unwrap();

    // Regardless of timing, the fork manifest should produce correct reads.
    // Create a cold reader using the fork manifest.
    let fork_dir = TempDir::new().unwrap();
    let fork_cs = ContentStore::new(Arc::clone(&s3), "test");
    let fork_vm = Arc::new(parking_lot::RwLock::new(fork_manifest));
    let fork_cc = Arc::new(glidefs::block::cache::SimpleBlockCache::new(64 * 1024 * 1024));
    let fork_config = glidefs::block::write_cache::WriteCacheConfig {
        cache_dir: fork_dir.path().to_path_buf(),
        device_name: "fork-reader".to_string(),
        device_size: super::DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let fork_cache = glidefs::block::write_cache::WriteCache::open_fresh_active(fork_config)
        .unwrap();
    let fork_metrics = Arc::new(ExportMetrics::new());

    // Read blocks through the fork manifest — all should contain seed=2 data
    // (seed=2 was the last write, so it wins regardless of pack layout)
    for i in 0..3usize {
        let offset = (i * BLOCK_SIZE) as u64;
        let data = fork_cache
            .read(
                offset,
                BLOCK_SIZE,
                fork_cc.as_ref(),
                &pack_index_cache,
                &fork_vm,
                &fork_cs,
                &fork_metrics,
            )
            .await
            .unwrap();
        assert_eq!(
            data[0], 2,
            "block {} should have seed=2 (last write wins), got seed={}",
            i, data[0]
        );
    }
}

/// Snapshot and flush_to_s3 (which triggers compaction) running concurrently.
///
/// Both acquire the flush lock — they serialize, not race. The test verifies
/// that after both complete, the manifest is valid and all data is readable.
#[tokio::test]
async fn test_snapshot_concurrent_with_flush_and_compaction() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3)).await;

    // Build up pack count: 17 flushes to chunk 0 (exceeds DEFAULT_COMPACTION_THRESHOLD of 16)
    for round in 0u8..17 {
        write_blocks(&cache, 0, 3, round + 1, cc.as_ref());
        cache
            .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
            .await
            .unwrap();
    }

    let pack_count = volume_manifest
        .read()
        .chunk_pack_ids(0)
        .unwrap()
        .len();
    assert!(
        pack_count >= 17,
        "should have 17+ packs to trigger compaction, got {}",
        pack_count
    );

    // Write fresh data so both tasks have something to flush
    write_blocks(&cache, 0, 3, 0xAA, cc.as_ref());

    // Spawn concurrent flush_to_s3 (triggers compact_if_needed) + snapshot
    let cache2 = Arc::clone(&cache);
    let cs2 = ContentStore::new(Arc::clone(&s3), "test");
    let pic2 = Arc::clone(&pack_index_cache);
    let vm2 = Arc::clone(&volume_manifest);

    let flush_handle = tokio::spawn(async move {
        cache2
            .flush_to_s3(&cs2, &pic2, &vm2)
            .await
    });

    let snapshot_handle = {
        let cache3 = Arc::clone(&cache);
        let cs3 = ContentStore::new(Arc::clone(&s3), "test");
        let pic3 = Arc::clone(&pack_index_cache);
        let vm3 = Arc::clone(&volume_manifest);
        tokio::spawn(async move {
            cache3.snapshot(&cs3, &pic3, &vm3).await
        })
    };

    let (flush_result, snap_result) = tokio::join!(flush_handle, snapshot_handle);
    flush_result.unwrap().unwrap();
    snap_result.unwrap().unwrap();

    // Verify: live manifest is valid — cold read all blocks
    cache.sync_manifest(&cs, &volume_manifest).await.unwrap();

    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        super::create_cold_reader(&reader_dir, "vm1", Arc::clone(&s3)).await;

    for i in 0..3usize {
        let offset = (i * BLOCK_SIZE) as u64;
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
        // Last write was seed=0xAA
        assert_eq!(
            data[0], 0xAA,
            "block {} should have seed=0xAA after concurrent flush+snapshot, got 0x{:02x}",
            i, data[0]
        );
    }
}

/// Cold reader resolves zero-block tombstones correctly.
///
/// Write non-zero data, flush → pack A. Overwrite with zeros, flush → pack B
/// (zero tombstones with comp_length = 0). Cold reader with manifest [A, B]
/// must return zeros — the tombstone in B overrides the non-zero entry in A
/// via "newest wins" in the read path (read.rs:575-580).
#[tokio::test]
async fn test_zero_overwrite_cold_reader_sees_zeros() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3)).await;

    // Write non-zero data to blocks 0-2
    write_blocks(&cache, 0, 3, 0xAA, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Overwrite blocks 0-2 with all zeros
    for i in 0..3 {
        let offset = i * BLOCK_SIZE;
        cache
            .write(offset as u64, &vec![0u8; BLOCK_SIZE], cc.as_ref())
            .unwrap();
    }
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Manifest should have 2 packs: [A (non-zero), B (zero tombstones)]
    let pack_count = volume_manifest.read().chunk_pack_ids(0).unwrap().len();
    assert_eq!(pack_count, 2, "should have 2 packs before cold read");

    // Cold reader: loads manifest from S3, empty local SSD
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        super::create_cold_reader(&reader_dir, "vm1", Arc::clone(&s3)).await;

    // Read blocks 0-2 through cold reader — must be all zeros
    for i in 0..3 {
        let offset = i * BLOCK_SIZE;
        let data = reader_cache
            .read(
                offset as u64,
                BLOCK_SIZE,
                reader_cc.as_ref(),
                &reader_pic,
                &reader_vm,
                &reader_cs,
                &reader_m,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == 0),
            "block {} should be all zeros (tombstone overrides non-zero pack), got first byte 0x{:02x}",
            i,
            data[0]
        );
    }
}

/// Fork from snapshot sees original non-zero data; current manifest sees zeros.
///
/// Write non-zero → snapshot → overwrite with zeros → flush.
/// Fork from snapshot gets [A] only → reads non-zero data.
/// Cold reader from current manifest gets [A, B] → tombstone wins → zeros.
#[tokio::test]
async fn test_fork_from_snapshot_zero_overwrite_sees_original() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::config::ExportConfig;

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(RouterConfig {
        object_store: Arc::clone(&s3),
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
    })
    .await
    .unwrap();

    let config = ExportConfig {
        name: "vm1".to_string(),
        size_gb: 0.01,
        s3_prefix: None,
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(config, false, None, None)
        .await
        .unwrap();

    // Write non-zero data and snapshot
    let handler = router.get_handler("vm1").await.unwrap();
    for i in 0..3 {
        handler
            .write(i as u64 * BLOCK_SIZE as u64, &[0xAA; BLOCK_SIZE], false)
            .unwrap();
    }
    let snap1 = router.snapshot_export("vm1", None).await.unwrap();

    // Overwrite with zeros and snapshot again (creates tombstones)
    for i in 0..3 {
        handler
            .write(
                i as u64 * BLOCK_SIZE as u64,
                &vec![0u8; BLOCK_SIZE],
                false,
            )
            .unwrap();
    }
    let _snap2 = router.snapshot_export("vm1", None).await.unwrap();

    // Fork from snap1 — should see 0xAA (pre-zero snapshot)
    let fork_config = ExportConfig {
        name: "fork1".to_string(),
        size_gb: 0.01,
        s3_prefix: Some("vm1".to_string()),
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(fork_config, false, Some("vm1"), Some(snap1.sequence))
        .await
        .unwrap();

    let fork_handler = router.get_handler("fork1").await.unwrap();
    for i in 0..3 {
        let data = fork_handler
            .read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == 0xAA),
            "fork from snap1 block {} should be 0xAA, got 0x{:02x}",
            i,
            data[0]
        );
    }

    // Fork from snap2 (current, with tombstones) — should see zeros
    let fork2_config = ExportConfig {
        name: "fork2".to_string(),
        size_gb: 0.01,
        s3_prefix: Some("vm1".to_string()),
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(fork2_config, false, Some("vm1"), Some(_snap2.sequence))
        .await
        .unwrap();

    let fork2_handler = router.get_handler("fork2").await.unwrap();
    for i in 0..3 {
        let data = fork2_handler
            .read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == 0),
            "fork from snap2 block {} should be all zeros (tombstone), got 0x{:02x}",
            i,
            data[0]
        );
    }
}

/// Compaction CAS failure leaves an orphaned pack; GC cleans it up.
///
/// Run compaction #1 with [A, B] → succeeds, manifest becomes [C].
/// Run compaction #2 with stale [A, B] → CAS fails (manifest has [C]),
/// but the new base pack D is already on S3 (orphaned).
/// GC finds A, B, D as dead and deletes them. Data stays intact via pack C.
#[tokio::test]
async fn test_compaction_cas_failure_orphan_cleaned_by_gc() {
    use glidefs::block::pack::PackId;
    use glidefs::block::write_cache::compact;
    use glidefs::cli::gc::{new_gc_state_for_test, reconcile_prefix_for_test};

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3)).await;

    // Flush 1: write blocks → pack A
    write_blocks(&cache, 0, 3, 1, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Flush 2: overwrite same blocks → pack B (manifest: [A, B])
    write_blocks(&cache, 0, 3, 2, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    let stale_packs: Vec<PackId> = volume_manifest
        .read()
        .chunk_pack_ids(0)
        .unwrap()
        .to_vec();
    assert_eq!(stale_packs.len(), 2);

    // Compaction #1: [A, B] → [C] (succeeds)
    let blocks_per_chunk = volume_manifest.read().blocks_per_chunk();
    compact::compact_chunk(
        0,
        &stale_packs,
        blocks_per_chunk,
        &cs,
        &pack_index_cache,
        &volume_manifest,
    )
    .await
    .unwrap();

    // Manifest now has [C]
    let packs_after_compact: Vec<PackId> = volume_manifest
        .read()
        .chunk_pack_ids(0)
        .unwrap()
        .to_vec();
    assert_eq!(packs_after_compact.len(), 1, "compaction should produce 1 pack");

    // Sync manifest to S3 so GC sees [C]
    cache.sync_manifest(&cs, &volume_manifest).await.unwrap();

    // Compaction #2: same stale [A, B] — CAS must fail because manifest has [C]
    let result = compact::compact_chunk(
        0,
        &stale_packs,
        blocks_per_chunk,
        &cs,
        &pack_index_cache,
        &volume_manifest,
    )
    .await;
    assert!(
        result.is_err(),
        "compaction with stale pack list should fail (CAS mismatch)"
    );

    // Orphaned base pack from failed compaction #2 is now on S3.
    // GC should find it along with A and B (no longer in live manifest).
    let mut gc_state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(&cs, &mut gc_state, Duration::ZERO, 1000, false)
        .await
        .unwrap();

    // Dead packs: A, B (pre-compaction), D (orphan from failed compaction #2)
    assert!(
        report.dead_found() >= 3,
        "should find at least 3 dead packs (A, B, and orphan D), got {}",
        report.dead_found()
    );
    assert!(
        report.packs_deleted() >= 3,
        "should delete at least 3 dead packs with grace_period=0, got {}",
        report.packs_deleted()
    );

    // Data integrity: cold reader reads through pack C
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_cs, reader_pic, reader_vm, reader_cc, reader_m) =
        super::create_cold_reader(&reader_dir, "vm1", Arc::clone(&s3)).await;
    for i in 0..3 {
        let offset = i * BLOCK_SIZE;
        let data = reader_cache
            .read(
                offset as u64,
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
            data[0], 2,
            "block {} should have seed=2 (last write) via compacted pack, got {}",
            i, data[0]
        );
    }
}

/// head_manifest returns false for nonexistent manifest in nonexistent prefix.
#[tokio::test]
async fn test_head_manifest_not_found() {
    use glidefs::block::cache::SimpleBlockCache;
    use glidefs::block::router::{ExportRouter, RouterConfig};

    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = ExportRouter::new(RouterConfig {
        object_store: Arc::clone(&s3),
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
    })
    .await
    .unwrap();

    // No exports, no manifests — should return false, not error
    assert!(!router.head_manifest("nonexistent-prefix", "nonexistent-tag").await.unwrap());
}
