//! Integration tests for delta manifests.
//!
//! These tests verify:
//! 1. Delta sync cycle: write → flush_packs → sync(full) → write more → flush_packs → sync(delta)
//! 2. Cold reader can restore from effective manifest (base + delta)
//! 3. Compaction triggers: sync count limit, size ratio, explicit snapshot
//! 4. Stale delta ignored on restore
//! 5. GC sees conservative pack union across base + delta

use std::sync::Arc;
use tempfile::TempDir;

use glidefs::nbd::content_store::ContentStore;
use glidefs::nbd::manifest::{delta_manifest_s3_key, manifest_s3_key};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

use super::{create_v2_cold_reader, create_v2_test_cache, BLOCK_SIZE};

/// Write `count` blocks starting at `start_idx` with data seeded by `seed`.
fn write_blocks(
    cache: &glidefs::nbd::write_cache::WriteCache<glidefs::nbd::state::Active>,
    start: u32,
    count: u32,
    seed: u8,
    clean_cache: &glidefs::nbd::cache::SimpleBlockCache,
) {
    for i in start..start + count {
        let data: Vec<u8> = (0..BLOCK_SIZE)
            .map(|j| seed.wrapping_add(i as u8).wrapping_add(j as u8))
            .collect();
        cache
            .write(i as u64 * BLOCK_SIZE as u64, &data, clean_cache)
            .unwrap();
    }
}

/// Check if a key exists in the object store.
async fn s3_key_exists(s3: &dyn ObjectStore, base_path: &str, relative_key: &str) -> bool {
    let key = format!("{}/{}", base_path, relative_key);
    let path = ObjectPath::from(key);
    s3.head(&path).await.is_ok()
}

/// Delta sync cycle: first sync is full, second sync is delta,
/// cold reader can read all data via effective manifest.
#[tokio::test]
async fn test_delta_sync_cycle() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _metrics) =
        create_v2_test_cache(&dir, "delta-test", Arc::clone(&s3));

    // Phase 1: Write 10 blocks, flush packs, sync (should be full — no base yet).
    write_blocks(&cache, 0, 10, 0xAA, cc.as_ref());
    let (_stats, seq1) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq1).await.unwrap();

    // Base manifest should exist, no delta.
    assert!(
        s3_key_exists(&*s3, "test", &manifest_s3_key("delta-test")).await,
        "base manifest should exist after first sync"
    );
    assert!(
        !s3_key_exists(&*s3, "test", &delta_manifest_s3_key("delta-test")).await,
        "delta should NOT exist after first sync (compaction)"
    );

    // Phase 2: Write 5 more blocks, flush packs, sync (should be delta).
    write_blocks(&cache, 10, 5, 0xBB, cc.as_ref());
    let (_stats, seq2) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq2).await.unwrap();

    // Now delta should exist.
    assert!(
        s3_key_exists(&*s3, "test", &delta_manifest_s3_key("delta-test")).await,
        "delta manifest should exist after second sync"
    );

    // Phase 3: Cold reader restores from effective manifest and reads all data.
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pi, reader_cc, reader_metrics) =
        create_v2_cold_reader(&reader_dir, "delta-test", Arc::clone(&s3)).await;

    // Verify all 15 blocks are readable.
    for i in 0..15u32 {
        let seed: u8 = if i < 10 { 0xAA } else { 0xBB };
        let expected: Vec<u8> = (0..BLOCK_SIZE)
            .map(|j| seed.wrapping_add(i as u8).wrapping_add(j as u8))
            .collect();
        let data = reader
            .read_v2(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_cc.as_ref(),
                &reader_pi,
                &reader_cs,
                &reader_metrics,
            )
            .await
            .unwrap();
        assert_eq!(
            data.as_ref(),
            &expected[..],
            "block {i} data mismatch after delta restore"
        );
    }
}

/// Sync count compaction trigger: after 10 delta syncs, full manifest is uploaded.
#[tokio::test]
async fn test_compaction_trigger_sync_count() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _) = create_v2_test_cache(&dir, "compact-test", Arc::clone(&s3));

    // First sync: full (establishes base) with enough blocks that
    // 10 single-block deltas won't trigger the 50% size compaction.
    write_blocks(&cache, 0, 100, 0x01, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    // 11 more syncs: each writes 1 new block. The sync count check fires
    // when syncs_since_base >= 10. Counter starts at 0, increments after
    // each delta upload, so the 11th sync (i=10) sees count=10 and compacts.
    for i in 0..11u32 {
        write_blocks(&cache, 200 + i, 1, i as u8, cc.as_ref());
        let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
        cache.sync_manifest(&cs, &pi, seq).await.unwrap();

        let has_delta =
            s3_key_exists(&*s3, "test", &delta_manifest_s3_key("compact-test")).await;

        if i < 10 {
            assert!(
                has_delta,
                "sync {i}: delta should exist (not yet at compaction threshold)"
            );
        } else {
            // 11th delta sync triggers compaction → full manifest, delta deleted.
            assert!(
                !has_delta,
                "sync {i}: delta should NOT exist (compaction triggered)"
            );
        }
    }
}

/// Size-based compaction trigger: when delta block changes exceed 50% of full,
/// sync_manifest compacts to a full manifest instead of uploading a delta.
#[tokio::test]
async fn test_compaction_trigger_size_ratio() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _) =
        create_v2_test_cache(&dir, "size-compact", Arc::clone(&s3));

    // Establish base with 10 blocks.
    write_blocks(&cache, 0, 10, 0x01, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    // Write 2 new blocks (20% of 10) — should stay as delta.
    write_blocks(&cache, 10, 2, 0x02, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    assert!(
        s3_key_exists(&*s3, "test", &delta_manifest_s3_key("size-compact")).await,
        "small change should produce delta, not full manifest"
    );

    // Overwrite 6 of the original 10 blocks (6 upserts out of 12 total = 50%).
    // delta_block_size = 6 * 25 = 150, full_block_size = 12 * 25 = 300
    // 150 * 2 = 300 which is NOT > 300, so still delta.
    // Overwrite 7 blocks instead: delta_block_size = 7 * 25 = 175, 175 * 2 = 350 > 300.
    write_blocks(&cache, 0, 7, 0x03, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    assert!(
        !s3_key_exists(&*s3, "test", &delta_manifest_s3_key("size-compact")).await,
        "large change (>50% of blocks) should trigger compaction — no delta"
    );

    // Verify all data is readable via cold reader.
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pi, reader_cc, reader_metrics) =
        create_v2_cold_reader(&reader_dir, "size-compact", Arc::clone(&s3)).await;

    for i in 0..12u32 {
        let seed: u8 = if i < 7 { 0x03 } else if i < 10 { 0x01 } else { 0x02 };
        let expected: Vec<u8> = (0..BLOCK_SIZE)
            .map(|j| seed.wrapping_add(i as u8).wrapping_add(j as u8))
            .collect();
        let data = reader
            .read_v2(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_cc.as_ref(),
                &reader_pi,
                &reader_cs,
                &reader_metrics,
            )
            .await
            .unwrap();
        assert_eq!(
            data.as_ref(),
            &expected[..],
            "block {i} data mismatch after size-triggered compaction"
        );
    }
}

/// Snapshot always compacts: write, sync(delta), snapshot → no delta remains.
#[tokio::test]
async fn test_snapshot_always_compacts() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _) = create_v2_test_cache(&dir, "snap-test", Arc::clone(&s3));

    // Establish base.
    write_blocks(&cache, 0, 5, 0x01, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    // Write more, sync delta.
    write_blocks(&cache, 5, 3, 0x02, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    assert!(
        s3_key_exists(&*s3, "test", &delta_manifest_s3_key("snap-test")).await,
        "delta should exist before snapshot"
    );

    // Snapshot always uploads full and deletes delta.
    cache.snapshot(&cs, &pi).await.unwrap();

    assert!(
        !s3_key_exists(&*s3, "test", &delta_manifest_s3_key("snap-test")).await,
        "delta should NOT exist after snapshot (compaction)"
    );
}

/// flush_to_s3 always compacts (same as snapshot).
#[tokio::test]
async fn test_flush_to_s3_always_compacts() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _) = create_v2_test_cache(&dir, "flush-test", Arc::clone(&s3));

    // Establish base.
    write_blocks(&cache, 0, 5, 0x01, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    // Write more, sync delta.
    write_blocks(&cache, 5, 3, 0x02, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    assert!(
        s3_key_exists(&*s3, "test", &delta_manifest_s3_key("flush-test")).await,
        "delta should exist before flush_to_s3"
    );

    // flush_to_s3 compacts.
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    assert!(
        !s3_key_exists(&*s3, "test", &delta_manifest_s3_key("flush-test")).await,
        "delta should NOT exist after flush_to_s3"
    );
}

/// Stale delta (wrong base_sequence) is ignored on restore — base manifest used as-is.
#[tokio::test]
async fn test_stale_delta_ignored() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _) = create_v2_test_cache(&dir, "stale-test", Arc::clone(&s3));

    // Write and flush to create a full manifest.
    write_blocks(&cache, 0, 5, 0xAA, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    // Manually upload a stale delta with wrong base_sequence.
    let stale_delta = glidefs::nbd::manifest::ManifestDelta {
        name: "stale-test".to_string(),
        base_sequence: 999999, // won't match base
        sequence: 1000000,
        chunk_size: BLOCK_SIZE as u32,
        device_size: 64 * 1024 * 1024,
        upserted: vec![glidefs::nbd::manifest::ManifestBlockEntry {
            chunk_index: 0,
            hash: glidefs::nbd::block_map::Blake3Hash::from_bytes([0xFF; 16]),
            flags: 0,
        }],
        deleted_chunks: vec![1, 2, 3, 4],
        new_pack_entries: vec![],
    };
    cs.put_delta_manifest("stale-test", stale_delta.serialize())
        .await
        .unwrap();

    // Cold reader should use base only (stale delta ignored).
    let reader_dir = TempDir::new().unwrap();
    let (reader, reader_cs, reader_pi, reader_cc, reader_metrics) =
        create_v2_cold_reader(&reader_dir, "stale-test", Arc::clone(&s3)).await;

    // All 5 original blocks should be readable (delta's deletes were ignored).
    for i in 0..5u32 {
        let expected: Vec<u8> = (0..BLOCK_SIZE)
            .map(|j| 0xAAu8.wrapping_add(i as u8).wrapping_add(j as u8))
            .collect();
        let data = reader
            .read_v2(
                i as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE,
                reader_cc.as_ref(),
                &reader_pi,
                &reader_cs,
                &reader_metrics,
            )
            .await
            .unwrap();
        assert_eq!(
            data.as_ref(),
            &expected[..],
            "block {i} should match base (stale delta ignored)"
        );
    }
}

/// Delta manifest is smaller than full manifest for small changes.
#[tokio::test]
async fn test_delta_is_smaller_than_full() {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _) = create_v2_test_cache(&dir, "size-test", Arc::clone(&s3));

    // Write 200 blocks, flush full.
    write_blocks(&cache, 0, 200, 0x01, cc.as_ref());
    cache.flush_to_s3(&cs, &pi).await.unwrap();

    // Get full manifest size.
    let base_key = format!("test/{}", manifest_s3_key("size-test"));
    let base_size = s3
        .head(&ObjectPath::from(base_key))
        .await
        .unwrap()
        .size;

    // Write 5 more blocks, sync delta.
    write_blocks(&cache, 200, 5, 0x02, cc.as_ref());
    let (_, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();

    // Get delta manifest size.
    let delta_key = format!("test/{}", delta_manifest_s3_key("size-test"));
    let delta_size = s3
        .head(&ObjectPath::from(delta_key))
        .await
        .unwrap()
        .size;

    assert!(
        delta_size < base_size,
        "delta ({delta_size} bytes) should be smaller than full ({base_size} bytes)"
    );
}

/// GC sees packs from both base and delta manifests as live.
#[tokio::test]
async fn test_gc_with_delta_manifests() {
    use glidefs::cli::gc::{new_gc_state_for_test, reconcile_prefix_for_test};
    use std::time::Duration;

    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pi, cc, _) = create_v2_test_cache(&dir, "gc-delta", Arc::clone(&s3));

    // Phase 1: Write blocks, flush full → creates packs referenced by base manifest.
    write_blocks(&cache, 0, 110, 0x01, cc.as_ref());
    let stats1 = cache.flush_to_s3(&cs, &pi).await.unwrap();
    let base_pack_ids = stats1.new_pack_ids.clone();
    assert!(!base_pack_ids.is_empty(), "should have created packs in base flush");

    // Phase 2: Write more blocks, flush packs, sync delta → new packs referenced by delta.
    write_blocks(&cache, 200, 110, 0x02, cc.as_ref());
    let (stats2, seq) = cache.flush_packs(&cs, &pi).await.unwrap();
    cache.sync_manifest(&cs, &pi, seq).await.unwrap();
    let delta_pack_ids = stats2.new_pack_ids.clone();
    assert!(!delta_pack_ids.is_empty(), "should have created packs in delta flush");

    // Upload a registry with all pack IDs.
    let all_pack_ids: Vec<_> = base_pack_ids.iter().chain(delta_pack_ids.iter()).copied().collect();
    let registry = glidefs::nbd::pack_registry::PackRegistry {
        pack_ids: all_pack_ids.clone(),
    };
    cs.put_registry("gc-delta", registry.serialize()).await.unwrap();

    // Run GC — all packs should be live (referenced by base+delta).
    let gc_cs = ContentStore::new(Arc::clone(&s3), "test");
    let mut gc_state = new_gc_state_for_test();
    let report = reconcile_prefix_for_test(
        &gc_cs,
        &mut gc_state,
        Duration::from_secs(0),
        1000,
        true, // dry run
    )
    .await
    .unwrap();

    assert_eq!(report.dead_found(), 0, "all packs should be live (base + delta)");
    assert_eq!(
        report.live_packs(),
        all_pack_ids.len(),
        "live pack count should match total"
    );
}
