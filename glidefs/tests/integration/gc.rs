//! Integration tests for garbage collection.
//!
//! These tests verify:
//! 1. Pack registries are created and updated during flush
//! 2. Fork creates a child registry from parent manifest's packs
//! 3. GC reconciliation identifies and deletes orphaned packs
//! 4. Grace period and max-deletes caps work correctly

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use tempfile::TempDir;
use uuid::Uuid;

use glidefs::cli::gc::{
    inject_dead_pack_for_test, new_gc_state_for_test, reconcile_prefix_for_test,
};
use glidefs::nbd::content_store::ContentStore;
use glidefs::nbd::manifest::Manifest;
use glidefs::nbd::pack_registry::PackRegistry;
use glidefs::nbd::write_cache::WriteCacheConfig;

use super::create_v2_test_cache;

const BLOCK_SIZE: usize = 128 * 1024; // 128KB

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write `count` distinct blocks starting at block index `start`.
/// The `seed` parameter ensures different data for the same block indices across calls.
/// Data is generated so that different seeds produce non-overlapping hashes
/// even across different block indices.
fn write_blocks(
    cache: &glidefs::nbd::write_cache::WriteCache<glidefs::nbd::state::Active>,
    start: usize,
    count: usize,
    seed: u8,
    clean_cache: &dyn glidefs::nbd::cache::BlockCache,
) {
    for i in 0..count {
        let offset = (start + i) * BLOCK_SIZE;
        // Embed seed + block index as LE u16 to ensure unique content per block,
        // even for block indices > 255 (u8 wrapping caused dedup at 500 blocks/pack).
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

/// Get and parse the pack registry for an export.
async fn get_registry(content_store: &ContentStore, name: &str) -> Option<PackRegistry> {
    let data = content_store.get_registry(name).await.unwrap()?;
    Some(PackRegistry::deserialize(&data).unwrap())
}

// ---------------------------------------------------------------------------
// Lifecycle: flush + registry
// ---------------------------------------------------------------------------

/// Flushing dirty blocks to S3 should create/update the pack registry.
#[tokio::test]
async fn test_flush_updates_registry() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm1", Arc::clone(&s3) as _);

    // No registry before any writes
    assert!(get_registry(&cs, "vm1").await.is_none());

    // Write 3 blocks and flush
    write_blocks(&cache, 0, 3, 0, cc.as_ref());
    let stats = cache.flush_to_s3(&cs, &pi).await.unwrap();
    assert!(stats.packs_uploaded > 0, "should have uploaded packs");

    // Registry should exist and contain the pack IDs from the flush
    let reg = get_registry(&cs, "vm1").await.expect("registry should exist");
    assert_eq!(
        reg.pack_ids.len(),
        stats.new_pack_ids.len(),
        "registry should have same number of pack IDs as flush produced"
    );
    for id in &stats.new_pack_ids {
        assert!(
            reg.pack_ids.contains(id),
            "registry should contain all flushed pack IDs"
        );
    }
}

/// Multiple flushes should accumulate pack IDs in the registry.
#[tokio::test]
async fn test_multiple_flushes_accumulate() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm1", Arc::clone(&s3) as _);

    let mut all_pack_ids: Vec<Uuid> = Vec::new();

    // 3 successive flushes with different blocks
    for round in 0..3 {
        write_blocks(&cache, round * 5, 3, round as u8, cc.as_ref());
        let stats = cache.flush_to_s3(&cs, &pi).await.unwrap();
        all_pack_ids.extend_from_slice(&stats.new_pack_ids);
    }

    let reg = get_registry(&cs, "vm1").await.expect("registry should exist");
    assert_eq!(
        reg.pack_ids.len(),
        all_pack_ids.len(),
        "registry should have all pack IDs from all flushes"
    );
    for id in &all_pack_ids {
        assert!(reg.pack_ids.contains(id));
    }
}

// ---------------------------------------------------------------------------
// Lifecycle: fork + registry
// ---------------------------------------------------------------------------

/// Forking a VM should create a child registry containing the parent's pack IDs.
#[tokio::test]
async fn test_fork_creates_child_registry() {
    let s3 = Arc::new(InMemory::new());
    let parent_dir = TempDir::new().unwrap();
    let (parent_cache, parent_cs, parent_pi, cc, _m) =
        create_v2_test_cache(&parent_dir, "parent", Arc::clone(&s3) as _);

    // Write and flush parent
    write_blocks(&parent_cache, 0, 10, 0, cc.as_ref());
    let parent_stats = parent_cache.flush_to_s3(&parent_cs, &parent_pi).await.unwrap();
    assert!(!parent_stats.new_pack_ids.is_empty());

    // Simulate fork: read parent manifest, create child cache from it,
    // and create child registry from manifest's pack IDs.
    let manifest_bytes = parent_cs
        .get_manifest("parent")
        .await
        .unwrap()
        .expect("parent manifest should exist");
    let manifest = Manifest::deserialize(&manifest_bytes).unwrap();

    // Collect unique pack IDs from manifest (what the router does on fork)
    let fork_pack_ids: Vec<Uuid> = manifest
        .pack_index
        .iter()
        .map(|e| e.pack_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let child_registry = PackRegistry { pack_ids: fork_pack_ids.clone() };
    parent_cs
        .put_registry("child", child_registry.serialize())
        .await
        .unwrap();

    // Verify child registry
    let child_reg = get_registry(&parent_cs, "child")
        .await
        .expect("child registry should exist");

    // Child should reference the same packs that the parent manifest uses
    let manifest_pack_ids: std::collections::HashSet<Uuid> =
        manifest.pack_index.iter().map(|e| e.pack_id).collect();
    let child_pack_set = child_reg.pack_id_set();

    assert_eq!(child_pack_set, manifest_pack_ids);
}

/// After forking, child writes create independent pack IDs.
#[tokio::test]
async fn test_fork_registries_independent() {
    let s3 = Arc::new(InMemory::new());
    let parent_dir = TempDir::new().unwrap();
    let (parent_cache, parent_cs, parent_pi, cc, _m) =
        create_v2_test_cache(&parent_dir, "parent", Arc::clone(&s3) as _);

    // Write and flush parent
    write_blocks(&parent_cache, 0, 5, 0, cc.as_ref());
    parent_cache.flush_to_s3(&parent_cs, &parent_pi).await.unwrap();

    // Read parent manifest for fork
    let manifest_bytes = parent_cs.get_manifest("parent").await.unwrap().unwrap();
    let manifest = Manifest::deserialize(&manifest_bytes).unwrap();

    // Create child registry from parent manifest packs
    let fork_pack_ids: Vec<Uuid> = manifest
        .pack_index
        .iter()
        .map(|e| e.pack_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let child_registry = PackRegistry { pack_ids: fork_pack_ids };
    parent_cs
        .put_registry("child", child_registry.serialize())
        .await
        .unwrap();

    // Create child cache from manifest and write new blocks
    let child_dir = TempDir::new().unwrap();
    let child_config = WriteCacheConfig {
        cache_dir: child_dir.path().to_path_buf(),
        device_name: "child".to_string(),
        device_size: super::DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let child_cs = ContentStore::new(Arc::clone(&s3) as _, "test");
    let child_pi = Arc::new(glidefs::nbd::pack_index::HostPackIndex::open(child_dir.path().join("pack_index.redb")).unwrap());
    child_pi.rebuild(std::slice::from_ref(&manifest)).unwrap();
    let child_cc = glidefs::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    let child_cache = glidefs::nbd::write_cache::WriteCache::open_from_manifest(
        child_config,
        &manifest,
        None,
    )
    .unwrap();

    // Write new blocks to child and flush
    write_blocks(&child_cache, 20, 5, 99, &child_cc);
    let child_stats = child_cache.flush_to_s3(&child_cs, &child_pi).await.unwrap();

    // Child registry should now contain parent packs + child's new packs
    let child_reg = get_registry(&child_cs, "child").await.unwrap();
    for id in &child_stats.new_pack_ids {
        assert!(
            child_reg.pack_ids.contains(id),
            "child registry should contain child's new pack IDs"
        );
    }

    // Parent registry should NOT contain child's new packs
    let parent_reg = get_registry(&parent_cs, "parent").await.unwrap();
    for id in &child_stats.new_pack_ids {
        assert!(
            !parent_reg.pack_ids.contains(id),
            "parent registry should not contain child's pack IDs"
        );
    }
}

// ---------------------------------------------------------------------------
// GC Reconciliation
// ---------------------------------------------------------------------------

/// GC should find no orphans when all registry packs are referenced by manifests.
#[tokio::test]
async fn test_gc_finds_no_orphans() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm1", Arc::clone(&s3) as _);

    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    let mut state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, false)
        .await
        .unwrap();

    assert!(report.manifests_scanned() > 0);
    assert!(report.live_packs() > 0);
    assert_eq!(report.dead_found(), 0);
    assert_eq!(report.packs_deleted(), 0);
}

/// GC should identify and delete orphaned packs after blocks are overwritten.
#[tokio::test]
async fn test_gc_deletes_orphaned_packs() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm1", Arc::clone(&s3) as _);

    // First flush: creates packs
    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    let stats1 = cache.flush_to_s3(&cs, &pi).await.unwrap();
    let first_packs = stats1.new_pack_ids.clone();
    assert!(!first_packs.is_empty());

    // Overwrite the same blocks with different data, flush again
    // This creates new packs; the old packs are now orphaned
    write_blocks(&cache, 0, 5, 42, cc.as_ref());
    let stats2 = cache.flush_to_s3(&cs, &pi).await.unwrap();
    assert!(!stats2.new_pack_ids.is_empty());

    // Run GC with zero grace period — inject past timestamps for dead packs
    let mut state = new_gc_state_for_test();
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for id in &first_packs {
        inject_dead_pack_for_test(&mut state, id, old_ts);
    }

    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, false)
        .await
        .unwrap();

    assert!(report.dead_found() > 0, "should find dead packs");
    assert!(report.packs_deleted() > 0, "should delete orphaned packs");

    // Verify the old packs are actually gone from S3
    for id in &first_packs {
        let result = cs.get_pack(*id).await;
        assert!(
            result.is_err(),
            "deleted pack should not be retrievable from S3"
        );
    }
}

/// GC should respect the grace period: new dead packs should not be deleted immediately.
#[tokio::test]
async fn test_gc_respects_grace_period() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm1", Arc::clone(&s3) as _);

    // Create packs, overwrite with different data, flush to create orphans
    write_blocks(&cache, 0, 3, 0, cc.as_ref());
    let stats1 = cache.flush_to_s3(&cs, &pi).await.unwrap();
    write_blocks(&cache, 0, 3, 42, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    // GC with 24h grace period — dead packs just discovered, should NOT be deleted
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

    // Verify old packs still exist
    for id in &stats1.new_pack_ids {
        let result = cs.get_pack(*id).await;
        assert!(result.is_ok(), "pack should still exist during grace period");
    }

    // Now inject old timestamps (past grace period) and run GC again
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for id in &stats1.new_pack_ids {
        inject_dead_pack_for_test(&mut state, id, old_ts);
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
    assert!(report2.packs_deleted() > 0, "should delete packs past grace");
}

/// GC should cap deletions at --max-deletes.
#[tokio::test]
async fn test_gc_respects_max_deletes() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm1", Arc::clone(&s3) as _);

    // Create orphans: write blocks, flush, overwrite with different data, flush.
    // Need >500 blocks per write to create multiple packs (500 blocks/pack).
    write_blocks(&cache, 0, 750, 0, cc.as_ref());
    let stats1 = cache.flush_to_s3(&cs, &pi).await.unwrap();
    let _orphan_count = stats1.new_pack_ids.len();

    write_blocks(&cache, 0, 750, 42, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    // Inject old timestamps so all orphans are eligible
    let mut state = new_gc_state_for_test();
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for id in &stats1.new_pack_ids {
        inject_dead_pack_for_test(&mut state, id, old_ts);
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
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm1", Arc::clone(&s3) as _);

    // Create orphans
    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    let stats1 = cache.flush_to_s3(&cs, &pi).await.unwrap();
    write_blocks(&cache, 0, 5, 42, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    let mut state = new_gc_state_for_test();
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for id in &stats1.new_pack_ids {
        inject_dead_pack_for_test(&mut state, id, old_ts);
    }

    let report =
        reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, true).await.unwrap();

    assert!(report.dead_found() > 0);
    assert!(report.packs_deleted() > 0, "dry-run should report would-delete count");

    // But packs should still exist in S3
    for id in &stats1.new_pack_ids {
        let result = cs.get_pack(*id).await;
        assert!(result.is_ok(), "dry-run should not actually delete packs");
    }
}

/// Shared packs referenced by a fork should NOT be deleted even after source is gone.
#[tokio::test]
async fn test_gc_fork_then_delete_source() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "parent", Arc::clone(&s3) as _);

    // Write and flush parent
    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    // Get parent manifest's pack IDs
    let manifest_bytes = cs.get_manifest("parent").await.unwrap().unwrap();
    let manifest = Manifest::deserialize(&manifest_bytes).unwrap();
    let parent_pack_ids: std::collections::HashSet<Uuid> =
        manifest.pack_index.iter().map(|e| e.pack_id).collect();

    // Create child registry + manifest (simulate fork)
    let fork_packs: Vec<Uuid> = parent_pack_ids.iter().copied().collect();
    let child_registry = PackRegistry { pack_ids: fork_packs };
    cs.put_registry("child", child_registry.serialize()).await.unwrap();
    // Copy parent manifest as child manifest (child references same packs)
    cs.put_manifest("child", manifest_bytes.clone()).await.unwrap();

    // Delete parent manifest (simulate VM deletion)
    // We need to delete via object_store directly
    let parent_manifest_key =
        object_store::path::Path::from("test/manifests/parent".to_string());
    s3.delete(&parent_manifest_key).await.unwrap();

    // Also delete parent registry
    cs.delete_registry("parent").await.unwrap();

    // Run GC — parent packs should be live because child manifest references them
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

    // Verify packs are still accessible
    for id in &parent_pack_ids {
        let result = cs.get_pack(*id).await;
        assert!(result.is_ok(), "shared pack should still exist");
    }
}

/// GC should compact registries by removing deleted pack IDs.
#[tokio::test]
async fn test_gc_compacts_registries() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm1", Arc::clone(&s3) as _);

    // Create orphans
    write_blocks(&cache, 0, 5, 0, cc.as_ref());
    let stats1 = cache.flush_to_s3(&cs, &pi).await.unwrap();
    let old_packs = stats1.new_pack_ids.clone();

    write_blocks(&cache, 0, 5, 42, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    let reg_before = get_registry(&cs, "vm1").await.unwrap();
    let before_count = reg_before.pack_ids.len();

    // Inject old timestamps and run GC
    let mut state = new_gc_state_for_test();
    let old_ts = Utc::now() - chrono::Duration::hours(25);
    for id in &old_packs {
        inject_dead_pack_for_test(&mut state, id, old_ts);
    }

    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, false)
        .await
        .unwrap();

    assert!(report.packs_deleted() > 0);
    assert!(
        report.registries_compacted() > 0,
        "should compact at least one registry"
    );

    // Registry should have fewer pack IDs after compaction
    let reg_after = get_registry(&cs, "vm1").await.unwrap();
    assert!(
        reg_after.pack_ids.len() < before_count,
        "registry should be smaller after compaction"
    );

    // Deleted pack IDs should not be in the compacted registry
    for id in &old_packs {
        assert!(
            !reg_after.pack_ids.contains(id),
            "compacted registry should not contain deleted pack IDs"
        );
    }
}

/// GC should skip corrupt manifests and continue processing others.
#[tokio::test]
async fn test_gc_manifest_parse_error() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _m) =
        create_v2_test_cache(&dir, "vm-good", Arc::clone(&s3) as _);

    // Create a valid VM with packs
    write_blocks(&cache, 0, 3, 0, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    // Write a corrupt manifest for another "VM"
    cs.put_manifest("vm-corrupt", b"not a valid manifest".to_vec())
        .await
        .unwrap();

    // GC should process the good manifest and skip the corrupt one
    let mut state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(&cs, &mut state, Duration::ZERO, 10000, false)
        .await
        .unwrap();

    assert_eq!(
        report.manifest_errors(),
        1,
        "should report one manifest parse error"
    );
    assert!(
        report.manifests_scanned() >= 1,
        "should still scan the good manifest"
    );
    assert_eq!(report.packs_deleted(), 0, "should not delete any packs");
}
