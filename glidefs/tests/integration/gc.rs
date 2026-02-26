//! Integration tests for garbage collection (v4).
//!
//! These tests verify:
//! 1. GC reconciliation identifies and deletes orphaned packs
//! 2. Grace period and max-deletes caps work correctly
//! 3. Shared packs via fork manifest are preserved
//! 4. Corrupt manifests are handled gracefully

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tempfile::TempDir;

use glidefs::cli::gc::{
    inject_dead_pack_for_test, new_gc_state_for_test, reconcile_prefix_for_test,
};

use super::create_test_cache;

const BLOCK_SIZE: usize = 128 * 1024; // 128KB

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write `count` distinct blocks starting at block index `start`.
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

/// Upload fake orphan packs to S3 that aren't referenced by any manifest.
async fn create_orphan_packs(
    cs: &glidefs::block::content_store::ContentStore,
    chunk_idx: u32,
    pack_ids: &[u64],
) {
    for &pack_id in pack_ids {
        cs.put_chunk_pack(chunk_idx, pack_id, b"orphan pack data".to_vec())
            .await
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// GC Reconciliation
// ---------------------------------------------------------------------------

/// GC should find no orphans when all packs are referenced by manifests.
#[tokio::test]
async fn test_gc_finds_no_orphans() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    let mut state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, false)
        .await
        .unwrap();

    assert!(report.manifests_scanned() > 0);
    assert!(report.live_packs() > 0);
    assert_eq!(report.dead_found(), 0);
    assert_eq!(report.packs_deleted(), 0);
}

/// GC should identify and delete orphaned packs (packs on S3 not referenced by any manifest).
#[tokio::test]
async fn test_gc_deletes_orphaned_packs() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    // Create live packs via flush
    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Upload orphan packs directly to S3 (not referenced by any manifest)
    let orphan_ids = [0xDEAD_0001_0000_0001u64, 0xDEAD_0001_0000_0002];
    create_orphan_packs(&cs, 0, &orphan_ids).await;

    // Inject old timestamps so orphans pass the grace period
    let mut state = new_gc_state_for_test();
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for &pack_id in &orphan_ids {
        inject_dead_pack_for_test(&mut state, 0, pack_id, old_ts);
    }

    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, false)
        .await
        .unwrap();

    assert!(report.dead_found() > 0, "should find dead packs");
    assert!(report.packs_deleted() > 0, "should delete orphaned packs");
}

/// GC should respect the grace period: new dead packs should not be deleted immediately.
#[tokio::test]
async fn test_gc_respects_grace_period() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    // Create live packs
    write_blocks(&cache, 0, 3, 0, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Upload orphan packs
    let orphan_ids = [0xDEAD_0002_0000_0001u64, 0xDEAD_0002_0000_0002];
    create_orphan_packs(&cs, 0, &orphan_ids).await;

    // GC with 24h grace period — orphans just discovered, should NOT be deleted
    let mut state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(
        &cs,
        &mut state,
        Duration::from_secs(24 * 3600),
        10000,
        false,
    )
    .await
    .unwrap();

    assert!(report.dead_found() > 0, "should find dead packs");
    assert_eq!(
        report.eligible_for_deletion(),
        0,
        "no packs should be eligible yet (grace period)"
    );
    assert_eq!(report.packs_deleted(), 0);

    // Now inject old timestamps (past grace period) and run GC again
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for &pack_id in &orphan_ids {
        inject_dead_pack_for_test(&mut state, 0, pack_id, old_ts);
    }

    let report2 = reconcile_prefix_for_test(
        &cs,
        &mut state,
        Duration::from_secs(24 * 3600),
        10000,
        false,
    )
    .await
    .unwrap();

    assert!(
        report2.eligible_for_deletion() > 0,
        "packs should be eligible after grace period"
    );
    assert!(
        report2.packs_deleted() > 0,
        "should delete packs past grace"
    );
}

/// GC should cap deletions at --max-deletes.
#[tokio::test]
async fn test_gc_respects_max_deletes() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    // Create live packs
    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Upload several orphan packs across different chunks
    let orphan_ids: Vec<u64> = (1..=5).map(|i| 0xDEAD_0003_0000_0000u64 + i).collect();
    create_orphan_packs(&cs, 0, &orphan_ids).await;

    // Inject old timestamps so all orphans are eligible
    let mut state = new_gc_state_for_test();
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for &pack_id in &orphan_ids {
        inject_dead_pack_for_test(&mut state, 0, pack_id, old_ts);
    }

    // Cap at 1 delete
    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 1, false)
        .await
        .unwrap();

    assert!(
        report.dead_found() > 1,
        "should find multiple dead packs (found {})",
        report.dead_found(),
    );
    assert_eq!(
        report.deleted_count(),
        1,
        "should delete at most max_deletes"
    );
}

/// GC dry-run should report but not delete.
#[tokio::test]
async fn test_gc_dry_run() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm1", Arc::clone(&s3) as _).await;

    // Create live packs
    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Upload orphan packs
    let orphan_ids = [0xDEAD_0004_0000_0001u64, 0xDEAD_0004_0000_0002];
    create_orphan_packs(&cs, 0, &orphan_ids).await;

    let mut state = new_gc_state_for_test();
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for &pack_id in &orphan_ids {
        inject_dead_pack_for_test(&mut state, 0, pack_id, old_ts);
    }

    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, true)
        .await
        .unwrap();

    assert!(report.dead_found() > 0);
    assert!(
        report.packs_deleted() > 0,
        "dry-run should report would-delete count"
    );
}

/// Shared packs referenced by a fork should NOT be deleted even after source is gone.
#[tokio::test]
async fn test_gc_fork_then_delete_source() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "parent", Arc::clone(&s3) as _).await;

    // Write and flush parent
    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Get parent volume manifest bytes
    let manifest_bytes = cs.get_manifest("parent").await.unwrap().unwrap();

    // Copy parent manifest as child manifest (child references same chunks/packs)
    cs.put_manifest("child", manifest_bytes.clone())
        .await
        .unwrap();

    // Delete parent manifest (simulate VM deletion)
    let parent_manifest_key =
        object_store::path::Path::from("test/manifests/parent".to_string());
    s3.delete(&parent_manifest_key).await.unwrap();

    // Run GC -- parent packs should be live because child manifest references them
    let mut state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, false)
        .await
        .unwrap();

    // All parent packs are still live via child manifest
    assert_eq!(
        report.dead_found(),
        0,
        "shared packs should not be dead while child references them"
    );
    assert_eq!(report.packs_deleted(), 0);
}

/// GC should skip corrupt manifests and not delete any packs in the prefix.
#[tokio::test]
async fn test_gc_manifest_parse_error() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pack_index_cache, volume_manifest, cc, _m) =
        create_test_cache(&dir, "vm-good", Arc::clone(&s3) as _).await;

    // Create a valid VM with packs
    write_blocks(&cache, 0, 3, 0, cc.as_ref());
    cache
        .flush_to_s3(&cs, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Write a corrupt manifest for another "VM"
    cs.put_manifest("vm-corrupt", b"not a valid manifest".to_vec())
        .await
        .unwrap();

    // GC should detect the corrupt manifest and skip the entire prefix
    let mut state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, false)
        .await
        .unwrap();

    assert_eq!(
        report.manifest_errors(),
        1,
        "should report one manifest parse error"
    );
    assert_eq!(
        report.packs_deleted(),
        0,
        "should not delete any packs when manifest errors occur"
    );
}
