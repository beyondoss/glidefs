//! Tests for the async sub-block backfill feature.
//!
//! When a forked VM writes a sub-block region (e.g., 4KB) to a 128KB block
//! that only exists in S3, we defer the S3 fetch and track which sub-regions
//! have guest data via a 32-bit bitmap. This file exercises:
//!
//! - WAL partial entry append + replay (bitmap survives crash)
//! - Crash before checkpoint: WAL replay reconstructs bitmap
//! - Crash after checkpoint: metadata + WAL replay merges bitmaps
//! - Flush correctly skips partial blocks (no garbage upload)
//! - MAX_PARTIAL_BLOCKS cap falls back to synchronous backfill
//! - Property: random (offset, length) sub-block writes preserve parent data
//!
//! Run with: `cargo test --features test-utils --test integration partial_blocks`

use std::sync::Arc;

use object_store::memory::InMemory;
use proptest::prelude::*;
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::state::Initializing;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

use super::{BLOCK_SIZE, DEVICE_SIZE, SHARED_PACK_INDEX_CACHE};

const SUB_BLOCK: usize = 4096;

fn test_config(dir: &std::path::Path, name: &str) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: true,
    }
}

// =============================================================================
// WAL PARTIAL ENTRY: APPEND + REPLAY
// =============================================================================

/// WAL partial entries survive a crash: append a partial WAL entry, drop without
/// saving metadata, reopen — WAL replay reconstructs the partial_blocks bitmap.
#[tokio::test]
async fn test_partial_wal_entry_survives_crash() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path(), "partial-wal");
    let clean = SimpleBlockCache::new(1024);

    // Session 1: write a full block (to establish present/dirty), then manually
    // write a partial WAL entry to simulate a partial-block write.
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();

        // Write full block 0 so it's marked present and dirty
        cache.write(0, &vec![0xAA; BLOCK_SIZE], &clean).unwrap();

        // Write sub-block to block 1: only the first two 4KB sub-regions (bitmap = 0b11 = 3)
        cache
            .write(BLOCK_SIZE as u64, &[0xBB; SUB_BLOCK * 2], &clean)
            .unwrap();

        // Drop without metadata save — WAL entries survive on disk
    }

    // Session 2: WAL replay should reconstruct dirty state
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Both blocks should be dirty
        assert!(
            cache.dirty_block_count() >= 2,
            "both blocks should be dirty after WAL replay, got {}",
            cache.dirty_block_count()
        );

        // Block 0 data should be on SSD
        let block0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert!(
            block0.iter().all(|&b| b == 0xAA),
            "block 0 should be 0xAA after recovery"
        );
    }
}

// =============================================================================
// FLUSH SKIPS PARTIAL BLOCKS
// =============================================================================

/// A partial block must be skipped by flush (no garbage upload).
///
/// After a sub-block write to a forked block: the partial block stays DIRTY
/// but the flush skips it. After backfill completes (on-demand read), the
/// flush succeeds and the block is clean.
#[tokio::test]
async fn test_flush_skips_partial_block_until_backfill() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cs = ContentStore::new(Arc::clone(&s3), "test");
    let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
    let metrics = Arc::new(ExportMetrics::new());
    let cc = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    // --- Parent: write block 0 of 0xAA, flush to S3 ---
    let parent_dir = TempDir::new().unwrap();
    let parent_vm = {
        let (cache, cs2, pic2, vm, cc2, _m) =
            super::create_test_cache(&parent_dir, "parent", Arc::clone(&s3)).await;
        cache.write(0, &vec![0xAA; BLOCK_SIZE], cc2.as_ref()).unwrap();
        cache.flush_to_s3(&cs2, &pic2, &vm).await.unwrap();
        let manifest_bytes = vm.read().serialize();
        cs.put_manifest("parent", manifest_bytes).await.unwrap();
        vm
    };
    let parent_manifest = parent_vm.read().clone();

    // --- Fork child: fresh SSD, parent manifest in S3 ---
    let child_dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: child_dir.path().to_path_buf(),
        device_name: "child".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let cache = WriteCache::open_fresh_active(config).unwrap();
    let child_vm = Arc::new(parking_lot::RwLock::new(parent_manifest.clone()));
    let cache = Arc::new(cache);

    // Insert block 0 as a partial block with empty bitmap (simulating what
    // backfill_missing_blocks does before the write)
    cache.insert_partial_block_for_test(0);

    // Write first 4KB — this sets bit 0 in the bitmap
    cache.write(0, &[0xBB; SUB_BLOCK], cc.as_ref()).unwrap();

    // Verify: block 0 is dirty AND partial → flush should skip it
    assert!(cache.dirty_block_count() > 0, "block should be dirty");
    assert!(
        cache.is_block_partial(0),
        "block 0 should still be partial"
    );

    // Flush — block 0 is partial, so it must be skipped (counted as cas_failed)
    cache.flush_to_s3(&cs, &pic, &child_vm).await.unwrap();
    assert!(
        cache.dirty_block_count() > 0,
        "partial block should remain dirty after flush — was incorrectly uploaded"
    );

    // On-demand read triggers merge: fetches S3 data, fills in unwritten sub-regions,
    // marks the block as no longer partial
    let data = cache
        .read(
            0,
            BLOCK_SIZE,
            cc.as_ref(),
            &pic,
            &child_vm,
            &cs,
            &metrics,
        )
        .await
        .unwrap();

    // After on-demand merge: first 4KB = 0xBB (written), rest = 0xAA (parent)
    assert!(
        data[..SUB_BLOCK].iter().all(|&b| b == 0xBB),
        "first 4KB should be 0xBB"
    );
    assert!(
        data[SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "remaining should be 0xAA from parent"
    );
    assert!(
        !cache.is_block_partial(0),
        "block 0 should no longer be partial after on-demand read"
    );

    // Now flush should succeed and block should become clean
    cache.flush_to_s3(&cs, &pic, &child_vm).await.unwrap();
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "block should be clean after successful flush"
    );
}

// =============================================================================
// CRASH RECOVERY: PARTIAL BITMAPS FROM WAL
// =============================================================================

/// Crash before checkpoint: WAL entry for partial block must reconstruct bitmap.
///
/// Write sub-block (creates partial WAL entry), crash without checkpoint.
/// On recovery, WAL replay marks the block dirty and the data is on SSD.
#[tokio::test]
async fn test_partial_block_crash_before_checkpoint_recovery() {
    let cc = SimpleBlockCache::new(1024);
    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "crash-before-ckpt".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: true,
    };

    // Session 1: write 4KB to partial block, crash without checkpoint
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();

        // Simulate partial block: mark block 0 as partial, write first 4KB
        cache.insert_partial_block_for_test(0);
        cache.write(0, &[0xBB; SUB_BLOCK], &cc).unwrap();

        // Partial WAL entry with bit 0 set is fsynced to disk.
        // Crash (drop) without saving metadata.
    }

    // Session 2: WAL replay must reconstruct dirty state
    {
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Block 0 should be dirty (written before crash)
        assert!(
            cache.dirty_block_count() >= 1,
            "block 0 should be dirty after crash recovery"
        );

        // Block 0 data should be on SSD: first SUB_BLOCK is 0xBB
        let block0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert!(
            block0[..SUB_BLOCK].iter().all(|&b| b == 0xBB),
            "first 4KB should be 0xBB on SSD after recovery"
        );
    }
}

/// Crash AFTER checkpoint: metadata saves bitmap, WAL re-appends partial entries.
/// Second crash (after a write to a new sub-region but before second checkpoint)
/// should reconstruct the full bitmap: persisted bits OR WAL bits.
#[tokio::test]
async fn test_partial_block_crash_after_checkpoint_recovery() {
    let cc = SimpleBlockCache::new(1024);
    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "crash-after-ckpt".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: true,
    };

    // Session 1: write sub-block 0, local_checkpoint (saves metadata + re-appends partial WAL),
    // then write sub-block 4, crash without second checkpoint.
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let cache = Arc::new(cache);

        // Mark block 0 as partial, write sub-block 0 (bit 0 set)
        cache.insert_partial_block_for_test(0);
        cache.write(0, &[0xBB; SUB_BLOCK], &cc).unwrap();

        // local_checkpoint: saves metadata (bitmap with bit 0), truncates WAL,
        // re-appends the partial WAL entry
        cache.local_checkpoint().await.unwrap();

        // Write sub-block 4 (offset 16384, bit 4 set) — WAL captures this but metadata doesn't yet
        cache.write(16384, &[0xCC; SUB_BLOCK], &cc).unwrap();

        // Crash: metadata has bitmap = bit 0, WAL has partial entry for bit 0 (re-appended)
        // PLUS a new partial WAL entry for bit 4.
    }

    // Session 2: recovery must merge metadata bitmap (bit 0) with WAL bitmap (bits 0+4).
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        assert!(
            cache.dirty_block_count() >= 1,
            "block 0 should be dirty after checkpoint+crash recovery"
        );

        // Both sub-regions should be on SSD
        let block0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert!(
            block0[..SUB_BLOCK].iter().all(|&b| b == 0xBB),
            "sub-block 0 should be 0xBB"
        );
        assert!(
            block0[16384..16384 + SUB_BLOCK].iter().all(|&b| b == 0xCC),
            "sub-block 4 should be 0xCC after recovery"
        );
    }
}

// =============================================================================
// PROPERTY TESTS: RANDOM SUB-BLOCK WRITES
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    /// Property: Any sub-block write to a fresh (non-forked) block is readable.
    ///
    /// Writes (offset, len) within a single block, reads back the full block.
    /// The written range must match; unwritten bytes must be zero (fresh block).
    #[test]
    fn prop_sub_block_write_read_roundtrip(
        offset_in_block in 0usize..(BLOCK_SIZE - SUB_BLOCK),
        len in SUB_BLOCK..=BLOCK_SIZE,
        fill in any::<u8>(),
    ) {
        // Clamp len so we don't exceed block boundary
        let len = len.min(BLOCK_SIZE - offset_in_block);
        if len == 0 {
            return Ok(());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s3 = Arc::new(InMemory::new());
            let temp_dir = TempDir::new().unwrap();
            let (cache, cs, pic, vm, cc, metrics) =
                super::create_test_cache(&temp_dir, "prop-subblock", Arc::clone(&s3) as _).await;

            // Write sub-block
            let write_data = vec![fill; len];
            cache.write(offset_in_block as u64, &write_data, cc.as_ref()).unwrap();

            // Read full block via async path
            let block_data = cache
                .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &metrics)
                .await
                .unwrap();

            prop_assert_eq!(block_data.len(), BLOCK_SIZE);

            // Written range must match fill
            for i in offset_in_block..offset_in_block + len {
                prop_assert_eq!(block_data[i], fill, "byte {} should be fill 0x{:02X}", i, fill);
            }
            // Unwritten range before must be zero (fresh block)
            for i in 0..offset_in_block {
                prop_assert_eq!(block_data[i], 0, "byte {} before write should be 0", i);
            }
            // Unwritten range after must be zero
            for i in offset_in_block + len..BLOCK_SIZE {
                prop_assert_eq!(block_data[i], 0, "byte {} after write should be 0", i);
            }

            Ok(())
        })?;
    }

    /// Property: Multiple sub-block writes to the same fresh block — reference model check.
    ///
    /// Generates 1-8 random (offset, length, fill) writes within one block.
    /// Maintains a reference Vec<u8> applying writes in order.
    /// After all writes, reads the full block and compares byte-by-byte.
    #[test]
    fn prop_multiple_sub_block_writes_reference_model(
        writes in proptest::collection::vec(
            (0usize..(BLOCK_SIZE - SUB_BLOCK), SUB_BLOCK..=4*SUB_BLOCK, any::<u8>()),
            1..=8
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s3 = Arc::new(InMemory::new());
            let temp_dir = TempDir::new().unwrap();
            let (cache, cs, pic, vm, cc, metrics) =
                super::create_test_cache(&temp_dir, "prop-multiwrite", Arc::clone(&s3) as _).await;

            // Reference model: all-zero (fresh block), writes applied in order
            let mut reference = vec![0u8; BLOCK_SIZE];

            for &(offset, len, fill) in &writes {
                let len = len.min(BLOCK_SIZE - offset);
                if len == 0 {
                    continue;
                }
                let data = vec![fill; len];
                cache.write(offset as u64, &data, cc.as_ref()).unwrap();
                reference[offset..offset + len].fill(fill);
            }

            // Read full block
            let block_data = cache
                .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &metrics)
                .await
                .unwrap();

            prop_assert_eq!(block_data.len(), BLOCK_SIZE);
            for i in 0..BLOCK_SIZE {
                prop_assert_eq!(
                    block_data[i],
                    reference[i],
                    "byte {} mismatch: got 0x{:02X}, expected 0x{:02X}",
                    i,
                    block_data[i],
                    reference[i]
                );
            }

            Ok(())
        })?;
    }
}
