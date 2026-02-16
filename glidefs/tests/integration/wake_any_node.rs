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

use glidefs::nbd::block_store::S3BlockStore;
use glidefs::nbd::metrics::ExportMetrics;
use glidefs::nbd::state::Active;
use glidefs::nbd::write_cache::{WriteCache, WriteCacheConfig};

/// Helper to create a test cache with in-memory S3.
fn create_test_cache(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<object_store::memory::InMemory>,
) -> (Arc<WriteCache<Active>>, Arc<S3BlockStore>, Arc<ExportMetrics>) {
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: 10 * 1024 * 1024, // 10MB
        block_size: 128 * 1024,        // 128KB
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };

    let _s3_store = Arc::new(
        S3BlockStore::new(Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>, "test", 128 * 1024)
            .with_blocks_per_batch(10),
    );

    let metrics = Arc::new(ExportMetrics::new());
    let s3_with_metrics = Arc::new(
        S3BlockStore::new(Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>, "test", 128 * 1024)
            .with_blocks_per_batch(10)
            .with_metrics(Arc::clone(&metrics)),
    );

    let cache = WriteCache::open(config).expect("Failed to open cache");
    let cache = cache.skip_recovery_for_test();

    (Arc::new(cache), s3_with_metrics, metrics)
}

/// Test: Data written on Node A is readable from Node B after drain.
///
/// This proves the "wake from any node" capability - a VM can be stopped on one
/// node, its data drained to S3, and then resumed on a completely different node
/// with an empty local cache.
#[tokio::test]
async fn test_wake_from_different_node() {
    // Shared S3 backend (simulates real S3)
    let s3 = Arc::new(object_store::memory::InMemory::new());

    // === NODE A: Write data and drain to S3 ===
    let node_a_dir = TempDir::new().unwrap();
    let (cache_a, s3_store_a, _metrics_a) = create_test_cache(&node_a_dir, "vol1", Arc::clone(&s3));

    // Write test pattern: blocks 0, 1, 5 with distinct data
    let block_0_data: Vec<u8> = (0..128 * 1024).map(|i| (i % 256) as u8).collect();
    let block_1_data: Vec<u8> = (0..128 * 1024).map(|i| ((i + 100) % 256) as u8).collect();
    let block_5_data: Vec<u8> = (0..128 * 1024).map(|i| ((i + 200) % 256) as u8).collect();

    cache_a.write(0, &block_0_data).unwrap();
    cache_a.write(128 * 1024, &block_1_data).unwrap();
    cache_a.write(5 * 128 * 1024, &block_5_data).unwrap();

    // Drain to S3 (simulates graceful shutdown)
    cache_a.drain_for_snapshot(&s3_store_a).await.unwrap();

    // Drop Node A's cache - only S3 has the data now
    drop(cache_a);
    drop(node_a_dir);

    // === NODE B: Fresh node with empty cache, same S3 ===
    let node_b_dir = TempDir::new().unwrap();
    let (cache_b, s3_store_b, metrics_b) = create_test_cache(&node_b_dir, "vol1", Arc::clone(&s3));

    // Read data - should fetch from S3 (read-through)
    let read_0 = cache_b
        .read_with_fetch(0, 128 * 1024, &s3_store_b, &metrics_b)
        .await
        .unwrap();
    let read_1 = cache_b
        .read_with_fetch(128 * 1024, 128 * 1024, &s3_store_b, &metrics_b)
        .await
        .unwrap();
    let read_5 = cache_b
        .read_with_fetch(5 * 128 * 1024, 128 * 1024, &s3_store_b, &metrics_b)
        .await
        .unwrap();

    // Verify data integrity
    assert_eq!(read_0.as_ref(), &block_0_data[..], "Block 0 data mismatch");
    assert_eq!(read_1.as_ref(), &block_1_data[..], "Block 1 data mismatch");
    assert_eq!(read_5.as_ref(), &block_5_data[..], "Block 5 data mismatch");

    // Verify metrics show S3 was read (blocks were fetched from S3)
    // Note: Due to batch prefetching, we may only see 1 S3 read op even for 3 blocks
    // (all 3 blocks are in batch 0 with blocks_per_batch=10)
    let snapshot = metrics_b.snapshot();
    assert!(
        snapshot.s3_read_ops >= 1,
        "Expected at least 1 S3 read, got {}",
        snapshot.s3_read_ops
    );
}

/// Test: Unwritten blocks return zeros.
///
/// This is critical for sparse VMs - blocks that were never written should
/// return zeros, not errors. This allows VMs to start with mostly-empty disks.
#[tokio::test]
async fn test_unwritten_blocks_return_zeros() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, metrics) = create_test_cache(&temp_dir, "sparse", Arc::clone(&s3));

    // Read block 50 (within 10MB device) that was never written
    let data = cache
        .read_with_fetch(50 * 128 * 1024, 128 * 1024, &s3_store, &metrics)
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
    let (cache, s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write some data
    let data: Vec<u8> = (0..128 * 1024).map(|i| (i % 256) as u8).collect();
    cache.write(0, &data).unwrap();
    cache.drain_for_snapshot(&s3_store).await.unwrap();

    // Create fresh cache pointing to same S3
    drop(cache);
    let temp_dir2 = TempDir::new().unwrap();
    let (cache2, s3_store2, metrics2) = create_test_cache(&temp_dir2, "vol1", Arc::clone(&s3));

    // First read - cache miss
    let _ = cache2
        .read_with_fetch(0, 128 * 1024, &s3_store2, &metrics2)
        .await
        .unwrap();

    let snapshot_after_first = metrics2.snapshot();
    let misses_after_first = snapshot_after_first.cache_misses;

    // Second read - should be cache hit
    let _ = cache2
        .read_with_fetch(0, 128 * 1024, &s3_store2, &metrics2)
        .await
        .unwrap();

    let snapshot_after_second = metrics2.snapshot();
    let misses_after_second = snapshot_after_second.cache_misses;

    // Cache misses should not increase on second read
    assert_eq!(
        misses_after_first, misses_after_second,
        "Second read should hit cache, not S3"
    );
    assert!(
        snapshot_after_second.cache_hits > 0,
        "Should have cache hits on second read"
    );
}

/// Test: Batch prefetching brings in entire S3 batch.
///
/// When we read one block, we fetch the entire S3 batch. Subsequent reads
/// of blocks in the same batch should be cache hits.
#[tokio::test]
async fn test_batch_prefetch_optimization() {
    let s3 = Arc::new(object_store::memory::InMemory::new());

    // Write blocks 0-9 (all in same batch with blocks_per_batch=10)
    let writer_dir = TempDir::new().unwrap();
    let (writer_cache, writer_s3, _) = create_test_cache(&writer_dir, "vol1", Arc::clone(&s3));

    for i in 0..10 {
        let data: Vec<u8> = vec![i as u8; 128 * 1024];
        writer_cache.write(i * 128 * 1024, &data).unwrap();
    }
    writer_cache.drain_for_snapshot(&writer_s3).await.unwrap();
    drop(writer_cache);

    // Fresh reader
    let reader_dir = TempDir::new().unwrap();
    let (reader_cache, reader_s3, reader_metrics) =
        create_test_cache(&reader_dir, "vol1", Arc::clone(&s3));

    // Read block 0 - should prefetch entire batch
    let _ = reader_cache
        .read_with_fetch(0, 128 * 1024, &reader_s3, &reader_metrics)
        .await
        .unwrap();

    let snapshot_after_block_0 = reader_metrics.snapshot();
    let s3_reads_after_block_0 = snapshot_after_block_0.s3_read_ops;

    // Read blocks 1-9 - should all be cache hits (prefetched)
    for i in 1..10 {
        let _ = reader_cache
            .read_with_fetch(i * 128 * 1024, 128 * 1024, &reader_s3, &reader_metrics)
            .await
            .unwrap();
    }

    let snapshot_after_all = reader_metrics.snapshot();

    // S3 read count should not have increased (all from prefetch)
    assert_eq!(
        s3_reads_after_block_0, snapshot_after_all.s3_read_ops,
        "Blocks 1-9 should have been prefetched with block 0"
    );
}
