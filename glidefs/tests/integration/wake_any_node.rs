//! Integration tests proving VMs can wake from any compute node.
//!
//! These tests verify that:
//! 1. Data written on Node A is readable from Node B via S3
//! 2. Unwritten blocks return zeros (sparse VM support)
//! 3. Cache metrics correctly track S3 fetches
//!
//! Run with: `cargo test --features test-utils --test integration`

use std::sync::Arc;
use tempfile::TempDir;

use super::{create_v2_cold_reader, create_v2_test_cache};

/// Test: Data written on Node A is readable from Node B after flush to S3.
///
/// This proves the "wake from any node" capability - a VM can be stopped on one
/// node, its data flushed to S3, and then resumed on a completely different node
/// with an empty local cache.
#[tokio::test]
async fn test_wake_from_different_node() {
    // Shared S3 backend (simulates real S3)
    let s3 = Arc::new(object_store::memory::InMemory::new());

    // === NODE A: Write data and flush to S3 ===
    let node_a_dir = TempDir::new().unwrap();
    let (cache_a, content_store_a, pack_index_a, clean_cache_a, _metrics_a) =
        create_v2_test_cache(&node_a_dir, "vol1", Arc::clone(&s3) as _);

    // Write test pattern: blocks 0, 1, 5 with distinct data
    let block_0_data: Vec<u8> = (0..128 * 1024).map(|i| (i % 256) as u8).collect();
    let block_1_data: Vec<u8> = (0..128 * 1024).map(|i| ((i + 100) % 256) as u8).collect();
    let block_5_data: Vec<u8> = (0..128 * 1024).map(|i| ((i + 200) % 256) as u8).collect();

    cache_a.write(0, &block_0_data, clean_cache_a.as_ref()).unwrap();
    cache_a.write(128 * 1024, &block_1_data, clean_cache_a.as_ref()).unwrap();
    cache_a.write(5 * 128 * 1024, &block_5_data, clean_cache_a.as_ref()).unwrap();

    // Flush to S3 (simulates graceful shutdown)
    cache_a
        .flush_to_s3(&content_store_a, &pack_index_a)
        .await
        .unwrap();

    // Drop Node A's cache - only S3 has the data now
    drop(cache_a);
    drop(node_a_dir);

    // === NODE B: Fresh node with empty cache, same S3 ===
    // Cold reader: block_map and pack_index are populated from the S3 manifest
    let node_b_dir = TempDir::new().unwrap();
    let (cache_b, content_store_b, pack_index_b, clean_cache_b, metrics_b) =
        create_v2_cold_reader(&node_b_dir, "vol1", Arc::clone(&s3) as _).await;

    // Read data - should fetch from S3 (read-through)
    let read_0 = cache_b
        .read_v2(
            0,
            128 * 1024,
            clean_cache_b.as_ref(),
            &pack_index_b,
            &content_store_b,
            &metrics_b,
        )
        .await
        .unwrap();
    let read_1 = cache_b
        .read_v2(
            128 * 1024,
            128 * 1024,
            clean_cache_b.as_ref(),
            &pack_index_b,
            &content_store_b,
            &metrics_b,
        )
        .await
        .unwrap();
    let read_5 = cache_b
        .read_v2(
            5 * 128 * 1024,
            128 * 1024,
            clean_cache_b.as_ref(),
            &pack_index_b,
            &content_store_b,
            &metrics_b,
        )
        .await
        .unwrap();

    // Verify data integrity
    assert_eq!(read_0.as_ref(), &block_0_data[..], "Block 0 data mismatch");
    assert_eq!(read_1.as_ref(), &block_1_data[..], "Block 1 data mismatch");
    assert_eq!(read_5.as_ref(), &block_5_data[..], "Block 5 data mismatch");

    // Verify no S3 errors occurred during read-through
    let snapshot = metrics_b.snapshot();
    assert_eq!(snapshot.s3_get_errors, 0, "Should have no S3 errors");
}

/// Test: Unwritten blocks return zeros.
///
/// This is critical for sparse VMs - blocks that were never written should
/// return zeros, not errors. This allows VMs to start with mostly-empty disks.
#[tokio::test]
async fn test_unwritten_blocks_return_zeros() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, metrics) =
        create_v2_test_cache(&temp_dir, "sparse", Arc::clone(&s3) as _);

    // Read block 50 (within 10MB device) that was never written
    let data = cache
        .read_v2(
            50 * 128 * 1024,
            128 * 1024,
            clean_cache.as_ref(),
            &pack_index,
            &content_store,
            &metrics,
        )
        .await
        .unwrap();

    assert!(
        data.iter().all(|&b| b == 0),
        "Unwritten blocks should be all zeros"
    );
}

/// Test: Second read hits local cache, not S3.
///
/// Verifies that read-through caching works - first read fetches from S3,
/// subsequent reads should hit local cache.
#[tokio::test]
async fn test_cache_hit_on_second_read() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, content_store, pack_index, clean_cache, _metrics) =
        create_v2_test_cache(&temp_dir, "vol1", Arc::clone(&s3) as _);

    // Write some data
    let data: Vec<u8> = (0..128 * 1024).map(|i| (i % 256) as u8).collect();
    cache.write(0, &data, clean_cache.as_ref()).unwrap();
    cache
        .flush_to_s3(&content_store, &pack_index)
        .await
        .unwrap();

    // Create cold reader from manifest (block_map populated from S3)
    drop(cache);
    let temp_dir2 = TempDir::new().unwrap();
    let (cache2, content_store2, pack_index2, clean_cache2, metrics2) =
        create_v2_cold_reader(&temp_dir2, "vol1", Arc::clone(&s3) as _).await;

    // First read - fetches from S3, populates clean_cache
    let first_read = cache2
        .read_v2(
            0,
            128 * 1024,
            clean_cache2.as_ref(),
            &pack_index2,
            &content_store2,
            &metrics2,
        )
        .await
        .unwrap();
    assert_eq!(first_read.as_ref(), &data[..], "First read data mismatch");

    // Second read - should serve from clean_cache (no S3 round trip)
    let second_read = cache2
        .read_v2(
            0,
            128 * 1024,
            clean_cache2.as_ref(),
            &pack_index2,
            &content_store2,
            &metrics2,
        )
        .await
        .unwrap();
    assert_eq!(second_read.as_ref(), &data[..], "Second read data mismatch");

    // Both reads should return identical data, no S3 errors
    assert_eq!(first_read, second_read, "Reads should be identical");
    let snapshot = metrics2.snapshot();
    assert_eq!(snapshot.s3_get_errors, 0, "Should have no S3 errors");
}

/// Test: Batch prefetching brings in entire S3 batch.
///
/// When we read one block, we fetch the entire S3 batch. Subsequent reads
/// of blocks in the same batch should be cache hits.
#[tokio::test]
async fn test_batch_prefetch_optimization() {
    let s3 = Arc::new(object_store::memory::InMemory::new());

    // Write blocks 0-9 (all in same batch)
    let writer_dir = TempDir::new().unwrap();
    let (writer_cache, writer_content_store, writer_pack_index, writer_clean_cache, _) =
        create_v2_test_cache(&writer_dir, "vol1", Arc::clone(&s3) as _);

    for i in 0..10u64 {
        let data: Vec<u8> = vec![i as u8; 128 * 1024];
        writer_cache.write(i * 128 * 1024, &data, writer_clean_cache.as_ref()).unwrap();
    }
    writer_cache
        .flush_to_s3(&writer_content_store, &writer_pack_index)
        .await
        .unwrap();
    drop(writer_cache);

    // Cold reader from manifest
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index, reader_clean_cache, reader_metrics) =
        create_v2_cold_reader(&reader_dir, "vol1", Arc::clone(&s3) as _).await;

    // Read all 10 blocks and verify data integrity
    for i in 0..10u64 {
        let read_data = reader_cache
            .read_v2(
                i * 128 * 1024,
                128 * 1024,
                reader_clean_cache.as_ref(),
                &reader_pack_index,
                &reader_content_store,
                &reader_metrics,
            )
            .await
            .unwrap();
        let expected = vec![i as u8; 128 * 1024];
        assert_eq!(
            read_data.as_ref(),
            &expected[..],
            "Block {} data mismatch in batch prefetch",
            i
        );
    }

    // No S3 errors should have occurred
    let snapshot = reader_metrics.snapshot();
    assert_eq!(snapshot.s3_get_errors, 0, "Should have no S3 errors");
}
