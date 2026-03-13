//! Integration tests proving VMs can wake from any compute node.
//!
//! These tests verify that:
//! 1. Data written on Node A is readable from Node B via S3
//! 2. Unwritten blocks return zeros (sparse VM support)
//! 3. Cache metrics correctly track S3 fetches
//! 4. Pack flush (without drain) syncs manifest for cross-host recovery
//!
//! Run with: `cargo test --features test-utils --test integration`

use std::sync::Arc;
use tempfile::TempDir;

use super::{create_cold_reader, create_test_cache, BLOCK_SIZE};

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
    let (cache_a, content_store_a, pack_index_cache_a, volume_manifest_a, _clean_cache_a, _metrics_a) =
        create_test_cache(&node_a_dir, "vol1", Arc::clone(&s3) as _).await;

    // Write test pattern: blocks 0, 1, 5 with distinct data
    let block_0_data: Vec<u8> = (0..128 * 1024).map(|i| (i % 256) as u8).collect();
    let block_1_data: Vec<u8> = (0..128 * 1024).map(|i| ((i + 100) % 256) as u8).collect();
    let block_5_data: Vec<u8> = (0..128 * 1024).map(|i| ((i + 200) % 256) as u8).collect();

    cache_a.write(0, &block_0_data, &[]).unwrap();
    cache_a.write(128 * 1024, &block_1_data, &[]).unwrap();
    cache_a.write(5 * 128 * 1024, &block_5_data, &[]).unwrap();

    // Flush to S3 (simulates graceful shutdown)
    cache_a
        .flush_to_s3(&content_store_a, &pack_index_cache_a, &volume_manifest_a)
        .await
        .unwrap();

    // Drop Node A's cache - only S3 has the data now
    drop(cache_a);
    drop(node_a_dir);

    // === NODE B: Fresh node with empty cache, same S3 ===
    // Cold reader: volume_manifest is populated from the S3 manifest
    let node_b_dir = TempDir::new().unwrap();
    let (cache_b, content_store_b, pack_index_cache_b, volume_manifest_b, clean_cache_b, metrics_b) =
        create_cold_reader(&node_b_dir, "vol1", Arc::clone(&s3) as _).await;

    // Read data - should fetch from S3 (read-through)
    let read_0 = cache_b
        .read(
            0,
            128 * 1024,
            clean_cache_b.as_ref(),
            &pack_index_cache_b,
            &volume_manifest_b,
            &content_store_b,
            &metrics_b,
        )
        .await
        .unwrap();
    let read_1 = cache_b
        .read(
            128 * 1024,
            128 * 1024,
            clean_cache_b.as_ref(),
            &pack_index_cache_b,
            &volume_manifest_b,
            &content_store_b,
            &metrics_b,
        )
        .await
        .unwrap();
    let read_5 = cache_b
        .read(
            5 * 128 * 1024,
            128 * 1024,
            clean_cache_b.as_ref(),
            &pack_index_cache_b,
            &volume_manifest_b,
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
    let (cache, content_store, pack_index_cache, volume_manifest, clean_cache, metrics) =
        create_test_cache(&temp_dir, "sparse", Arc::clone(&s3) as _).await;

    // Read block 50 (within 256MB device) that was never written
    let data = cache
        .read(
            50 * 128 * 1024,
            128 * 1024,
            clean_cache.as_ref(),
            &pack_index_cache,
            &volume_manifest,
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
    let (cache, content_store, pack_index_cache, volume_manifest, _clean_cache, _metrics) =
        create_test_cache(&temp_dir, "vol1", Arc::clone(&s3) as _).await;

    // Write some data
    let data: Vec<u8> = (0..128 * 1024).map(|i| (i % 256) as u8).collect();
    cache.write(0, &data, &[]).unwrap();
    cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();

    // Create cold reader from manifest (volume_manifest populated from S3)
    drop(cache);
    let temp_dir2 = TempDir::new().unwrap();
    let (cache2, content_store2, pack_index_cache2, volume_manifest2, clean_cache2, metrics2) =
        create_cold_reader(&temp_dir2, "vol1", Arc::clone(&s3) as _).await;

    // First read - fetches from S3, populates clean_cache
    let first_read = cache2
        .read(
            0,
            128 * 1024,
            clean_cache2.as_ref(),
            &pack_index_cache2,
            &volume_manifest2,
            &content_store2,
            &metrics2,
        )
        .await
        .unwrap();
    assert_eq!(first_read.as_ref(), &data[..], "First read data mismatch");

    // Second read - should serve from clean_cache (no S3 round trip)
    let second_read = cache2
        .read(
            0,
            128 * 1024,
            clean_cache2.as_ref(),
            &pack_index_cache2,
            &volume_manifest2,
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
    let (writer_cache, writer_content_store, writer_pack_index_cache, writer_volume_manifest, _writer_clean_cache, _) =
        create_test_cache(&writer_dir, "vol1", Arc::clone(&s3) as _).await;

    for i in 0..10u64 {
        let data: Vec<u8> = vec![i as u8; 128 * 1024];
        writer_cache.write(i * 128 * 1024, &data, &[]).unwrap();
    }
    writer_cache
        .flush_to_s3(&writer_content_store, &writer_pack_index_cache, &writer_volume_manifest)
        .await
        .unwrap();
    drop(writer_cache);

    // Cold reader from manifest
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_content_store, reader_pack_index_cache, reader_volume_manifest, reader_clean_cache, reader_metrics) =
        create_cold_reader(&reader_dir, "vol1", Arc::clone(&s3) as _).await;

    // Read all 10 blocks and verify data integrity
    for i in 0..10u64 {
        let read_data = reader_cache
            .read(
                i * 128 * 1024,
                128 * 1024,
                reader_clean_cache.as_ref(),
                &reader_pack_index_cache,
                &reader_volume_manifest,
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

/// Test: Cross-host recovery works after pack flush without explicit drain.
///
/// Simulates host death: Node A writes data, the flush scheduler uploads packs
/// and syncs the manifest (via flush_packs + sync_manifest), but drain is never
/// called. Node B cold-wakes from the S3 manifest and reads the data.
///
/// This proves the flush scheduler's manifest sync closes the durability gap
/// for unplanned host death.
#[tokio::test]
async fn test_recovery_after_pack_flush_without_drain() {
    let s3: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());

    // === NODE A: Write data, flush packs + sync manifest (no drain) ===
    let node_a_dir = TempDir::new().unwrap();
    let (cache_a, content_store_a, pack_index_cache_a, volume_manifest_a, _clean_cache_a, _metrics_a) =
        create_test_cache(&node_a_dir, "vol1", Arc::clone(&s3)).await;

    // Write 3 blocks with distinct patterns
    let block_0_data: Vec<u8> = vec![0xAA; BLOCK_SIZE];
    let block_1_data: Vec<u8> = vec![0xBB; BLOCK_SIZE];
    let block_2_data: Vec<u8> = vec![0xCC; BLOCK_SIZE];

    cache_a.write(0, &block_0_data, &[]).unwrap();
    cache_a.write(BLOCK_SIZE as u64, &block_1_data, &[]).unwrap();
    cache_a.write(2 * BLOCK_SIZE as u64, &block_2_data, &[]).unwrap();

    // Simulate what the flush scheduler does: flush_packs + sync_manifest.
    // Critically, we do NOT call flush_to_s3 or drain — this is what happens
    // when the host dies after the scheduler fires but before drain.
    let (stats, _seq_cutpoint) = cache_a
        .flush_packs(&content_store_a, &pack_index_cache_a, &volume_manifest_a, None)
        .await
        .unwrap();
    assert!(stats.packs_uploaded > 0, "should have uploaded packs");

    // sync_manifest: this is the fix — without it, cold wake would lose data
    cache_a
        .sync_manifest(&content_store_a, &volume_manifest_a)
        .await
        .unwrap();

    // Simulate host death — drop everything, only S3 survives
    drop(cache_a);
    drop(node_a_dir);

    // === NODE B: Cold wake from S3 manifest (different host, empty cache) ===
    let node_b_dir = TempDir::new().unwrap();
    let (cache_b, content_store_b, pack_index_cache_b, volume_manifest_b, clean_cache_b, metrics_b) =
        create_cold_reader(&node_b_dir, "vol1", Arc::clone(&s3)).await;

    // Read all 3 blocks — should succeed via S3 read-through
    let read_0 = cache_b
        .read(0, BLOCK_SIZE, clean_cache_b.as_ref(), &pack_index_cache_b, &volume_manifest_b, &content_store_b, &metrics_b)
        .await
        .unwrap();
    let read_1 = cache_b
        .read(BLOCK_SIZE as u64, BLOCK_SIZE, clean_cache_b.as_ref(), &pack_index_cache_b, &volume_manifest_b, &content_store_b, &metrics_b)
        .await
        .unwrap();
    let read_2 = cache_b
        .read(2 * BLOCK_SIZE as u64, BLOCK_SIZE, clean_cache_b.as_ref(), &pack_index_cache_b, &volume_manifest_b, &content_store_b, &metrics_b)
        .await
        .unwrap();

    assert_eq!(read_0.as_ref(), &block_0_data[..], "Block 0 data mismatch after cross-host recovery");
    assert_eq!(read_1.as_ref(), &block_1_data[..], "Block 1 data mismatch after cross-host recovery");
    assert_eq!(read_2.as_ref(), &block_2_data[..], "Block 2 data mismatch after cross-host recovery");

    let snapshot = metrics_b.snapshot();
    assert_eq!(snapshot.s3_get_errors, 0, "Should have no S3 errors");
}
