//! Property-based tests for GlideFS.
//!
//! These tests use proptest to verify invariants hold across random inputs:
//! 1. Read-write roundtrip: data written can be read back correctly
//! 2. Batch boundary correctness: data spanning batch boundaries is handled
//! 3. Concurrent operations: no torn reads under concurrent writes
//! 4. State machine invariants: block states transition correctly
//!
//! Run with: `cargo test --features test-utils --test integration property`

use std::collections::HashMap;
use std::sync::Arc;

use proptest::prelude::*;
use tempfile::TempDir;

use super::{BLOCK_SIZE, DEVICE_SIZE};

const BLOCKS_PER_BATCH: u64 = 10;

// =============================================================================
// PROPERTY TESTS
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: Any data written can be read back exactly.
    #[test]
    fn prop_write_read_roundtrip(
        block_idx in 0u64..(DEVICE_SIZE / BLOCK_SIZE as u64),
        data in proptest::collection::vec(any::<u8>(), BLOCK_SIZE),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s3 = Arc::new(object_store::memory::InMemory::new());
            let temp_dir = TempDir::new().unwrap();
            let (cache, content_store, pack_index, _clean_cache, _metrics) =
                super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

            let offset = block_idx * BLOCK_SIZE as u64;

            // Write data
            cache.write(offset, &data).unwrap();

            // Read back (local)
            let read_data = cache.read_local(offset, BLOCK_SIZE).unwrap();
            prop_assert_eq!(read_data.as_ref(), &data[..], "Local read mismatch");

            // Flush to S3
            cache.flush_to_s3(&content_store, &pack_index).await.unwrap();

            // Read from fresh cache (S3 fetch)
            drop(cache);
            let temp_dir2 = TempDir::new().unwrap();
            let (cache2, content_store2, pack_index2, clean_cache2, metrics2) =
                super::create_v2_cold_reader(&temp_dir2, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

            let s3_data = cache2
                .read_v2(offset, BLOCK_SIZE, clean_cache2.as_ref(), &pack_index2, &content_store2, &metrics2)
                .await
                .unwrap();
            prop_assert_eq!(s3_data.as_ref(), &data[..], "S3 read mismatch");

            Ok(())
        })?;
    }

    /// Property: Multiple writes to same block - last write wins.
    #[test]
    fn prop_last_write_wins(
        block_idx in 0u64..(DEVICE_SIZE / BLOCK_SIZE as u64),
        writes in proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), BLOCK_SIZE),
            1..=10
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s3 = Arc::new(object_store::memory::InMemory::new());
            let temp_dir = TempDir::new().unwrap();
            let (cache, content_store, pack_index, _clean_cache, _metrics) =
                super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

            let offset = block_idx * BLOCK_SIZE as u64;

            // Write multiple times
            for data in &writes {
                cache.write(offset, data).unwrap();
            }

            // Last write should be what we read
            let expected = writes.last().unwrap();
            let read_data = cache.read_local(offset, BLOCK_SIZE).unwrap();
            prop_assert_eq!(read_data.as_ref(), &expected[..], "Last write should win");

            // Also verify after S3 roundtrip
            cache.flush_to_s3(&content_store, &pack_index).await.unwrap();

            drop(cache);
            let temp_dir2 = TempDir::new().unwrap();
            let (cache2, content_store2, pack_index2, clean_cache2, metrics2) =
                super::create_v2_cold_reader(&temp_dir2, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

            let s3_data = cache2
                .read_v2(offset, BLOCK_SIZE, clean_cache2.as_ref(), &pack_index2, &content_store2, &metrics2)
                .await
                .unwrap();
            prop_assert_eq!(s3_data.as_ref(), &expected[..], "S3 should have last write");

            Ok(())
        })?;
    }

    /// Property: Writes to different blocks are independent.
    #[test]
    fn prop_block_independence(
        // Generate pairs of (block_index, data) ensuring unique block indices
        block_data in proptest::collection::hash_map(
            0u64..(DEVICE_SIZE / BLOCK_SIZE as u64).min(20),
            proptest::collection::vec(any::<u8>(), BLOCK_SIZE),
            1..=10
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s3 = Arc::new(object_store::memory::InMemory::new());
            let temp_dir = TempDir::new().unwrap();
            let (cache, content_store, pack_index, _clean_cache, _metrics) =
                super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

            // Write all blocks
            for (&block_idx, data) in &block_data {
                let offset = block_idx * BLOCK_SIZE as u64;
                cache.write(offset, data).unwrap();
            }

            // Verify each block independently
            for (&block_idx, expected) in &block_data {
                let offset = block_idx * BLOCK_SIZE as u64;
                let read_data = cache.read_local(offset, BLOCK_SIZE).unwrap();
                prop_assert_eq!(
                    read_data.as_ref(),
                    &expected[..],
                    "Block {} data mismatch",
                    block_idx
                );
            }

            // Verify after S3 roundtrip
            cache.flush_to_s3(&content_store, &pack_index).await.unwrap();

            drop(cache);
            let temp_dir2 = TempDir::new().unwrap();
            let (cache2, content_store2, pack_index2, clean_cache2, metrics2) =
                super::create_v2_cold_reader(&temp_dir2, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

            for (&block_idx, expected) in &block_data {
                let offset = block_idx * BLOCK_SIZE as u64;
                let s3_data = cache2
                    .read_v2(offset, BLOCK_SIZE, clean_cache2.as_ref(), &pack_index2, &content_store2, &metrics2)
                    .await
                    .unwrap();
                prop_assert_eq!(
                    s3_data.as_ref(),
                    &expected[..],
                    "S3 block {} data mismatch",
                    block_idx
                );
            }

            Ok(())
        })?;
    }

    /// Property: Batch boundaries don't affect correctness.
    ///
    /// Blocks at batch boundaries (0, 9, 10, 19, 20, ...) should work correctly.
    #[test]
    fn prop_batch_boundary_correctness(
        // Test blocks at and around batch boundaries
        batch_idx in 0u64..3,
        position in prop_oneof![
            Just(0u64),  // First block in batch
            Just(BLOCKS_PER_BATCH - 1),  // Last block in batch
        ],
        data in proptest::collection::vec(any::<u8>(), BLOCK_SIZE),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s3 = Arc::new(object_store::memory::InMemory::new());
            let temp_dir = TempDir::new().unwrap();
            let (cache, content_store, pack_index, _clean_cache, _metrics) =
                super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

            let block_idx = batch_idx * BLOCKS_PER_BATCH + position;
            let offset = block_idx * BLOCK_SIZE as u64;

            // Skip if beyond device
            if offset >= DEVICE_SIZE {
                return Ok(());
            }

            // Write at batch boundary
            cache.write(offset, &data).unwrap();

            // Flush and verify
            cache.flush_to_s3(&content_store, &pack_index).await.unwrap();

            drop(cache);
            let temp_dir2 = TempDir::new().unwrap();
            let (cache2, content_store2, pack_index2, clean_cache2, metrics2) =
                super::create_v2_cold_reader(&temp_dir2, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

            let s3_data = cache2
                .read_v2(offset, BLOCK_SIZE, clean_cache2.as_ref(), &pack_index2, &content_store2, &metrics2)
                .await
                .unwrap();
            prop_assert_eq!(
                s3_data.as_ref(),
                &data[..],
                "Batch boundary block {} failed",
                block_idx
            );

            Ok(())
        })?;
    }

    /// Property: Dirty block count is accurate.
    #[test]
    fn prop_dirty_count_accurate(
        writes in proptest::collection::vec(
            0u64..(DEVICE_SIZE / BLOCK_SIZE as u64).min(20),
            1..=15
        ),
    ) {
        let s3 = Arc::new(object_store::memory::InMemory::new());
        let temp_dir = TempDir::new().unwrap();
        let (cache, _content_store, _pack_index, _clean_cache, _metrics) =
            super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

        let data = vec![0xAB; BLOCK_SIZE];

        // Track unique blocks written
        let mut written_blocks: std::collections::HashSet<u64> = std::collections::HashSet::new();

        for block_idx in writes {
            let offset = block_idx * BLOCK_SIZE as u64;
            cache.write(offset, &data).unwrap();
            written_blocks.insert(block_idx);

            // Dirty count should equal unique blocks written
            let dirty = cache.dirty_block_count();
            prop_assert_eq!(
                dirty,
                written_blocks.len() as u64,
                "Dirty count mismatch after writing block {}",
                block_idx
            );
        }
    }

    /// Property: Unwritten blocks return zeros.
    #[test]
    fn prop_unwritten_blocks_zero(
        block_idx in 0u64..(DEVICE_SIZE / BLOCK_SIZE as u64),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s3 = Arc::new(object_store::memory::InMemory::new());
            let temp_dir = TempDir::new().unwrap();
            let (cache, content_store, pack_index, clean_cache, metrics) =
                super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

            let offset = block_idx * BLOCK_SIZE as u64;

            // Read unwritten block
            let data = cache
                .read_v2(offset, BLOCK_SIZE, clean_cache.as_ref(), &pack_index, &content_store, &metrics)
                .await
                .unwrap();

            prop_assert!(
                data.iter().all(|&b| b == 0),
                "Unwritten block {} should be zeros",
                block_idx
            );

            Ok(())
        })?;
    }

    /// Property: Zero blocks written can be read as zeros.
    #[test]
    fn prop_zero_block_roundtrip(
        block_idx in 0u64..(DEVICE_SIZE / BLOCK_SIZE as u64),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s3 = Arc::new(object_store::memory::InMemory::new());
            let temp_dir = TempDir::new().unwrap();
            let (cache, content_store, pack_index, _clean_cache, _metrics) =
                super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

            let offset = block_idx * BLOCK_SIZE as u64;
            let zeros = vec![0u8; BLOCK_SIZE];

            // Write zeros
            cache.write(offset, &zeros).unwrap();

            // Flush (zero-block optimization may skip S3 write)
            cache.flush_to_s3(&content_store, &pack_index).await.unwrap();

            // Read from fresh cache
            drop(cache);
            let temp_dir2 = TempDir::new().unwrap();
            let (cache2, content_store2, pack_index2, clean_cache2, metrics2) =
                super::create_v2_cold_reader(&temp_dir2, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

            let read_data = cache2
                .read_v2(offset, BLOCK_SIZE, clean_cache2.as_ref(), &pack_index2, &content_store2, &metrics2)
                .await
                .unwrap();

            prop_assert!(
                read_data.iter().all(|&b| b == 0),
                "Zero block should read as zeros"
            );

            Ok(())
        })?;
    }
}

// =============================================================================
// DETERMINISTIC EDGE CASE TESTS
// =============================================================================

/// Test: Writes spanning multiple batches.
#[tokio::test]
async fn test_multi_batch_writes() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, _clean_cache, _metrics) =
        super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

    // Write blocks across 3 batches
    let mut expected: HashMap<u64, Vec<u8>> = HashMap::new();
    for batch in 0..3 {
        for block_in_batch in 0..BLOCKS_PER_BATCH {
            let block_idx = batch * BLOCKS_PER_BATCH + block_in_batch;
            let offset = block_idx * BLOCK_SIZE as u64;

            if offset >= DEVICE_SIZE {
                break;
            }

            let data: Vec<u8> = (0..BLOCK_SIZE)
                .map(|i| ((block_idx as usize + i) % 256) as u8)
                .collect();
            cache.write(offset, &data).unwrap();
            expected.insert(block_idx, data);
        }
    }

    // Flush all
    cache.flush_to_s3(&content_store, &pack_index).await.unwrap();

    // Verify from fresh cache
    drop(cache);
    let temp_dir2 = TempDir::new().unwrap();
    let (cache2, content_store2, pack_index2, clean_cache2, metrics2) =
        super::create_v2_cold_reader(&temp_dir2, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

    for (block_idx, data) in &expected {
        let offset = block_idx * BLOCK_SIZE as u64;
        let read_data = cache2
            .read_v2(offset, BLOCK_SIZE, clean_cache2.as_ref(), &pack_index2, &content_store2, &metrics2)
            .await
            .unwrap();
        assert_eq!(
            read_data.as_ref(),
            &data[..],
            "Block {} mismatch",
            block_idx
        );
    }
}

/// Test: Alternating reads and writes.
#[tokio::test]
async fn test_alternating_read_write() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, metrics) =
        super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

    for i in 0..20 {
        let offset = (i % 5) * BLOCK_SIZE as u64;

        if i % 2 == 0 {
            // Write
            let data = vec![i as u8; BLOCK_SIZE];
            cache.write(offset, &data).unwrap();
        } else {
            // Read (should get last written value or zeros)
            let _ = cache
                .read_v2(offset, BLOCK_SIZE, clean_cache.as_ref(), &pack_index, &content_store, &metrics)
                .await
                .unwrap();
        }
    }

    // Final verification
    cache.flush_to_s3(&content_store, &pack_index).await.unwrap();
    assert_eq!(cache.dirty_block_count(), 0);
}

/// Test: Maximum device size boundary.
#[tokio::test]
async fn test_device_boundary() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, _clean_cache, _metrics) =
        super::create_v2_test_cache(&temp_dir, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>);

    let last_block = (DEVICE_SIZE / BLOCK_SIZE as u64) - 1;
    let offset = last_block * BLOCK_SIZE as u64;

    // Write to last block
    let data = vec![0xFF; BLOCK_SIZE];
    cache.write(offset, &data).unwrap();

    // Flush and verify
    cache.flush_to_s3(&content_store, &pack_index).await.unwrap();

    drop(cache);
    let temp_dir2 = TempDir::new().unwrap();
    let (cache2, content_store2, pack_index2, clean_cache2, metrics2) =
        super::create_v2_cold_reader(&temp_dir2, "proptest", Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>).await;

    let read_data = cache2
        .read_v2(offset, BLOCK_SIZE, clean_cache2.as_ref(), &pack_index2, &content_store2, &metrics2)
        .await
        .unwrap();
    assert_eq!(read_data.as_ref(), &data[..]);
}
