//! TRIM semantics integration tests.
//!
//! Exercises TRIM (zero_range) through the full stack: WriteCache → flush → S3 → cold reader.
//! Covers partial-range trim, S3 tombstones, fork semantics, crash recovery, and large ranges.
//!
//! Run with: `cargo test --features test-utils --test integration trim`

use std::sync::Arc;

use object_store::memory::InMemory;
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::state::Initializing;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

use super::{BLOCK_SIZE, DEVICE_SIZE, SHARED_PACK_INDEX_CACHE};

// =============================================================================
// TRIM partial range
// =============================================================================

/// TRIM 512 bytes in the middle of a block. Trimmed region must be zeros,
/// rest must be original data.
#[tokio::test]
async fn test_trim_partial_range() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, metrics) =
        super::create_test_cache(&dir, "trim-partial", Arc::clone(&s3)).await;

    // Write full block of 0xAA
    let full_data = vec![0xAA; BLOCK_SIZE];
    cache.write(0, &full_data, &[]).unwrap();

    // TRIM 512 bytes starting at offset 2048 within the block
    let trim_offset = 2048u64;
    let trim_len = 512u64;
    cache.zero_range(trim_offset, trim_len, &[]).unwrap();

    // Read full block — trimmed region must be zeros, rest must be 0xAA
    let data = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &metrics)
        .await
        .unwrap();

    for i in 0..BLOCK_SIZE {
        let expected = if i >= trim_offset as usize && i < (trim_offset + trim_len) as usize {
            0x00
        } else {
            0xAA
        };
        assert_eq!(
            data[i], expected,
            "byte {i}: expected 0x{expected:02X}, got 0x{:02X}",
            data[i]
        );
    }
}

// =============================================================================
// TRIM then flush to S3 — tombstone in manifest
// =============================================================================

/// Write block, flush to S3, TRIM same block, flush to S3 again.
/// Cold reader must see zeros (tombstone must override previous pack entry).
#[tokio::test]
async fn test_trim_then_flush_to_s3() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

    // Writer: write block 0, flush to S3
    let writer_dir = TempDir::new().unwrap();
    {
        let (cache, cs, pic, vm, cc, _m) =
            super::create_test_cache(&writer_dir, "trim-flush", Arc::clone(&s3)).await;

        cache.write(0, &vec![0xBB; BLOCK_SIZE], &[]).unwrap();
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

        // TRIM block 0
        cache.zero_range(0, BLOCK_SIZE as u64, &[]).unwrap();

        // Flush again — this should create a tombstone (zero block with comp_length=0)
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

        // Save manifest
        let manifest_bytes = vm.read().serialize();
        cs.put_manifest("trim-flush", manifest_bytes, None).await.unwrap();
    }

    // Cold reader: must see zeros (tombstone overrides the earlier 0xBB pack)
    let reader_dir = TempDir::new().unwrap();
    let (cache, cs, pic2, vm, cc, metrics) =
        super::create_cold_reader(&reader_dir, "trim-flush", Arc::clone(&s3)).await;

    let data = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic2, &vm, &cs, &metrics)
        .await
        .unwrap();

    assert!(
        data.iter().all(|&b| b == 0),
        "cold reader must see zeros after TRIM + flush (tombstone)"
    );
}

// =============================================================================
// TRIM fork semantics — child TRIM doesn't affect parent
// =============================================================================

/// Parent has block in S3. Fork child, TRIM that block on child.
/// Child reads zeros. Parent reads original data.
#[tokio::test]
async fn test_trim_fork_semantics() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);

    // Parent: write block 0 = 0xCC, flush to S3
    let parent_dir = TempDir::new().unwrap();
    let parent_manifest = {
        let (cache, cs, pic, vm, cc, _m) =
            super::create_test_cache(&parent_dir, "trim-fork-parent", Arc::clone(&s3)).await;

        cache
            .write(0, &vec![0xCC; BLOCK_SIZE], &[])
            .unwrap();
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

        let manifest_bytes = vm.read().serialize();
        cs.put_manifest("trim-fork-parent", manifest_bytes, None)
            .await
            .unwrap();
        vm.read().clone()
    };

    // Fork child: inherits parent manifest, TRIM block 0
    let child_dir = TempDir::new().unwrap();
    {
        let config = WriteCacheConfig {
            cache_dir: child_dir.path().to_path_buf(),
            device_name: "trim-fork-child".to_string(),
            device_size: DEVICE_SIZE,
            block_size: BLOCK_SIZE,
            wal_sync: false,
        };
        let cache = WriteCache::open_fresh_active(config).unwrap();
        let cs = ContentStore::new(Arc::clone(&s3), "test");
        let vm = Arc::new(parking_lot::RwLock::new(parent_manifest));
        let cc = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let metrics = Arc::new(ExportMetrics::new());

        // TRIM block 0 on child
        cache.zero_range(0, BLOCK_SIZE as u64, &[]).unwrap();

        // Child must read zeros
        let child_data = cache
            .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &metrics)
            .await
            .unwrap();
        assert!(
            child_data.iter().all(|&b| b == 0),
            "child must see zeros after TRIM"
        );

        // Flush child to S3 with tombstone
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
        let manifest_bytes = vm.read().serialize();
        cs.put_manifest("trim-fork-child", manifest_bytes, None)
            .await
            .unwrap();
    }

    // Verify parent data is unchanged via cold reader
    let parent_reader_dir = TempDir::new().unwrap();
    let (pcache, pcs, ppic, pvm, pcc, pmetrics) =
        super::create_cold_reader(&parent_reader_dir, "trim-fork-parent", Arc::clone(&s3)).await;

    let parent_data = pcache
        .read(0, BLOCK_SIZE, pcc.as_ref(), &ppic, &pvm, &pcs, &pmetrics)
        .await
        .unwrap();
    assert!(
        parent_data.iter().all(|&b| b == 0xCC),
        "parent data must be unchanged after child TRIM"
    );

    // Verify child cold reader sees zeros (tombstone in manifest)
    let child_reader_dir = TempDir::new().unwrap();
    let (ccache, ccs, cpic, cvm, ccc, cmetrics) =
        super::create_cold_reader(&child_reader_dir, "trim-fork-child", Arc::clone(&s3)).await;

    let child_cold_data = ccache
        .read(0, BLOCK_SIZE, ccc.as_ref(), &cpic, &cvm, &ccs, &cmetrics)
        .await
        .unwrap();
    assert!(
        child_cold_data.iter().all(|&b| b == 0),
        "child cold reader must see zeros (tombstone)"
    );
}

// =============================================================================
// TRIM crash recovery — WAL replay preserves TRIM
// =============================================================================

/// TRIM block, crash before metadata checkpoint. Recover via WAL replay.
/// The trimmed block must be zeros (WAL entry records the TRIM).
#[tokio::test]
async fn test_trim_crash_recovery() {
    let dir = TempDir::new().unwrap();

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "trim-crash".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: true,
    };

    // Session 1: write block, then TRIM, then crash (drop without save)
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let cc = SimpleBlockCache::new(1024);

        // Write full block of 0xDD
        cache.write(0, &vec![0xDD; BLOCK_SIZE], &[]).unwrap();

        // TRIM same block
        cache.zero_range(0, BLOCK_SIZE as u64, &[]).unwrap();

        // Drop without metadata save — simulates crash
    }

    // Session 2: WAL replay should recover the TRIM
    {
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Block should be dirty (TRIM was WAL'd)
        assert!(cache.dirty_block_count() >= 1, "trimmed block should be dirty");

        // Read local data — should be zeros
        let data = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert!(
            data.iter().all(|&b| b == 0),
            "trimmed block must be zeros after crash recovery"
        );
    }
}

// =============================================================================
// TRIM large range — batch efficiency
// =============================================================================

/// TRIM a range spanning many blocks (100MB). All must return zeros.
#[tokio::test]
async fn test_trim_large_range() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, metrics) =
        super::create_test_cache(&dir, "trim-large", Arc::clone(&s3)).await;

    let num_blocks = 800usize; // 800 * 128KB = 100MB
    let range_size = num_blocks * BLOCK_SIZE;

    // Write all blocks with non-zero data
    for i in 0..num_blocks {
        let fill = (i % 251) as u8 + 1;
        cache
            .write(
                (i * BLOCK_SIZE) as u64,
                &vec![fill; BLOCK_SIZE],
                &[],
            )
            .unwrap();
    }

    // TRIM the entire 100MB range in one call
    cache.zero_range(0, range_size as u64, &[]).unwrap();

    // Verify all blocks are zeros
    for i in 0..num_blocks {
        let data = cache
            .read(
                (i * BLOCK_SIZE) as u64,
                BLOCK_SIZE,
                cc.as_ref(),
                &pic,
                &vm,
                &cs,
                &metrics,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == 0),
            "block {i} should be zeros after large TRIM"
        );
    }

    // Flush to S3 — all should create tombstones (zero blocks)
    let stats = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    // All 800 blocks should be claimed as dirty
    assert_eq!(
        stats.blocks_claimed, num_blocks,
        "all {num_blocks} blocks should be claimed"
    );
    // Zero blocks are deduped (tombstone): no compressed data uploaded, just index entries
    assert_eq!(
        stats.blocks_deduped, num_blocks,
        "all {num_blocks} zero blocks should be deduped (tombstones)"
    );
    // No compressed block data should be uploaded (tombstones have comp_length=0)
    assert_eq!(stats.bytes_uploaded, 0, "no block data should be uploaded for zero blocks");
}
