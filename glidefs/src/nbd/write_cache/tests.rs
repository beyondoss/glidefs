use super::*;
use crate::nbd::block_map::blake3_128;
use crate::nbd::block_store::S3BlockStore;
use crate::nbd::state::{Active, Initializing};
use bytes::Bytes;
use object_store::memory::InMemory;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::TempDir;

fn test_config(dir: &Path) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.to_path_buf(),
        device_name: "test".to_string(),
        device_size: 1024 * 1024, // 1MB
        block_size: 4096,         // 4KB for testing
        dirty_budget_bytes: 0,
        flush_trigger: None,
    }
}

fn test_s3() -> S3BlockStore {
    let object_store = Arc::new(InMemory::new());
    S3BlockStore::with_defaults(object_store, "test")
}

#[tokio::test]
async fn test_open_fresh_cache() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let s3 = test_s3();

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    assert_eq!(cache.dirty_block_count(), 0);
}

#[tokio::test]
async fn test_write_read() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let s3 = test_s3();

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    // Write some data
    cache.write(0, b"hello world").unwrap();

    // Read it back
    let data = cache.read(0, 11).unwrap();
    assert_eq!(&data[..], b"hello world");

    // Should have dirty blocks now
    assert!(cache.dirty_block_count() > 0);
}

#[tokio::test]
async fn test_flush() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let s3 = test_s3();

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    cache.write(0, b"data").unwrap();
    cache.flush().unwrap();

    // Data should still be readable
    let data = cache.read(0, 4).unwrap();
    assert_eq!(&data[..], b"data");
}

#[tokio::test]
async fn test_metadata_persistence() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let s3 = test_s3();

    // Create cache and write data
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();
        cache.write(0, b"persistent").unwrap();
        cache.save_metadata().unwrap();
    }

    // Reopen and verify dirty blocks are preserved
    {
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        // Should have dirty blocks from previous session
        assert!(cache.inner.dirty_block_count.load(Ordering::Relaxed) > 0);

        let cache = cache.finish_recovery(&s3).await.unwrap();
        // Data should be readable
        let data = cache.read(0, 10).unwrap();
        assert_eq!(&data[..], b"persistent");
    }
}

#[tokio::test]
async fn test_read_through_from_s3() {
    // This test verifies the core read-through functionality:
    // When a block exists in S3 but not locally, read_with_fetch should fetch it.

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
        .with_blocks_per_batch(10); // 10 blocks per batch for testing
    let metrics = crate::nbd::metrics::ExportMetrics::new();

    // Pre-populate S3 with some data (simulating data from another node)
    // Block 0 is in batch 0, block 5 is also in batch 0 (with 10 blocks per batch)
    let mut batch0 = vec![0u8; s3.batch_size()];
    // Block 0: fill with 42
    batch0[..4096].copy_from_slice(&vec![42u8; 4096]);
    // Block 5: fill with 99
    let block5_offset = 5 * 4096;
    batch0[block5_offset..block5_offset + 4096].copy_from_slice(&vec![99u8; 4096]);
    s3.put_batch(0, batch0).await.unwrap();

    // Create a fresh cache on a "new node" (no local data)
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    // Read block 0 - should fetch from S3
    let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 42);
    assert!(data.iter().all(|&b| b == 42));

    // Read block 5 - should also fetch from S3
    let offset = 5 * 4096;
    let data = cache.read_with_fetch(offset, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 99);

    // Second read of block 0 should come from local cache now
    let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 42);

    // Read a block that doesn't exist in S3 - should return zeros
    let offset = 10 * 4096;
    let data = cache.read_with_fetch(offset, 4096, &s3, &metrics).await.unwrap();
    assert!(data.iter().all(|&b| b == 0));

    // Verify metrics were recorded
    // With batch prefetching:
    // - Read block 0: cache miss, fetches batch 0 (caches blocks 0-9), 1 S3 read
    // - Read block 5: cache HIT (was prefetched with block 0's batch)
    // - Read block 0 again: cache hit
    // - Read block 10: cache miss, fetches batch 1 (returns zeros), 1 S3 read
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.cache_misses, 2); // blocks 0 and 10 (block 5 was prefetched)
    assert_eq!(snapshot.cache_hits, 2); // block 5 (prefetched) + second read of block 0
    assert_eq!(snapshot.s3_read_ops, 2); // batch 0 + batch 1 (even though batch 1 is empty)
}

#[tokio::test]
async fn test_write_then_read_local() {
    // Verify that written blocks are marked as present and read locally
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let s3 = test_s3();
    let metrics = crate::nbd::metrics::ExportMetrics::new();

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    // Write data locally
    cache.write(0, b"local data!").unwrap();

    // Read should come from local cache, not S3
    let data = cache.read_with_fetch(0, 11, &s3, &metrics).await.unwrap();
    assert_eq!(&data[..], b"local data!");

    // S3 should NOT have this data yet (not synced)
    let s3_result = s3.read_block(0).await;
    assert!(matches!(
        s3_result,
        Err(crate::nbd::block_store::BlockStoreError::NotFound(_))
    ));

    // Verify cache hit (data was present locally)
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.cache_hits, 1);
    assert_eq!(snapshot.cache_misses, 0);
}

#[tokio::test]
async fn test_batch_prefetch_single_batch_efficiency() {
    // When reading multiple scattered blocks from the SAME S3 batch,
    // we should only make ONE S3 call (not one per block).
    //
    // This tests the core efficiency gain of batch prefetching.

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
        .with_blocks_per_batch(100); // 100 blocks per batch
    let metrics = crate::nbd::metrics::ExportMetrics::new();

    // Pre-populate S3 batch 0 with distinct data for blocks 0, 25, 50, 75
    let mut batch0 = vec![0u8; s3.batch_size()];
    batch0[0..4096].copy_from_slice(&vec![11u8; 4096]);           // block 0
    batch0[25 * 4096..26 * 4096].copy_from_slice(&vec![22u8; 4096]); // block 25
    batch0[50 * 4096..51 * 4096].copy_from_slice(&vec![33u8; 4096]); // block 50
    batch0[75 * 4096..76 * 4096].copy_from_slice(&vec![44u8; 4096]); // block 75
    s3.put_batch(0, batch0).await.unwrap();

    // Create fresh cache
    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "prefetch_test".to_string(),
        device_size: 100 * 4096, // 100 blocks
        block_size: 4096,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    // Read block 0 - triggers fetch of entire batch 0
    let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 11, "block 0 should have correct data");

    // Read block 25 - should be a CACHE HIT (prefetched with block 0)
    let data = cache.read_with_fetch(25 * 4096, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 22, "block 25 should have correct data");

    // Read block 50 - should be a CACHE HIT (prefetched with block 0)
    let data = cache.read_with_fetch(50 * 4096, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 33, "block 50 should have correct data");

    // Read block 75 - should be a CACHE HIT (prefetched with block 0)
    let data = cache.read_with_fetch(75 * 4096, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 44, "block 75 should have correct data");

    // Verify: only ONE S3 read operation for 4 block reads
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.s3_read_ops, 1, "should only fetch batch once");
    assert_eq!(snapshot.cache_misses, 1, "only first read should be a miss");
    assert_eq!(snapshot.cache_hits, 3, "subsequent reads should hit cache");
}

#[tokio::test]
async fn test_batch_prefetch_cross_batch_efficiency() {
    // When reading blocks from N different S3 batches,
    // we should make exactly N S3 calls.

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
        .with_blocks_per_batch(10); // 10 blocks per batch for easier testing
    let metrics = crate::nbd::metrics::ExportMetrics::new();

    // Pre-populate 3 batches
    let mut batch0 = vec![0u8; s3.batch_size()];
    batch0[0..4096].copy_from_slice(&vec![0xAAu8; 4096]); // block 0
    s3.put_batch(0, batch0).await.unwrap();

    let mut batch1 = vec![0u8; s3.batch_size()];
    batch1[5 * 4096..6 * 4096].copy_from_slice(&vec![0xBBu8; 4096]); // block 15 (5th in batch 1)
    s3.put_batch(1, batch1).await.unwrap();

    let mut batch2 = vec![0u8; s3.batch_size()];
    batch2[3 * 4096..4 * 4096].copy_from_slice(&vec![0xCCu8; 4096]); // block 23 (3rd in batch 2)
    s3.put_batch(2, batch2).await.unwrap();

    // Create fresh cache
    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "cross_batch_test".to_string(),
        device_size: 30 * 4096, // 30 blocks = 3 batches
        block_size: 4096,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    // Read from batch 0
    let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 0xAA);

    // Read from batch 1 (block 15)
    let data = cache.read_with_fetch(15 * 4096, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 0xBB);

    // Read from batch 2 (block 23)
    let data = cache.read_with_fetch(23 * 4096, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 0xCC);

    // Verify: exactly 3 S3 reads (one per batch)
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.s3_read_ops, 3, "should fetch each batch once");
    assert_eq!(snapshot.cache_misses, 3, "one miss per batch");
}

#[tokio::test]
async fn test_batch_prefetch_multi_block_read_span() {
    // When a single read spans multiple blocks in the same batch,
    // we should prefetch the entire batch and serve the read efficiently.

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
        .with_blocks_per_batch(10);
    let metrics = crate::nbd::metrics::ExportMetrics::new();

    // Pre-populate batch 0 with sequential pattern
    let mut batch0 = vec![0u8; s3.batch_size()];
    for i in 0..10 {
        let block_start = i * 4096;
        batch0[block_start..block_start + 4096].copy_from_slice(&vec![i as u8; 4096]);
    }
    s3.put_batch(0, batch0).await.unwrap();

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "span_test".to_string(),
        device_size: 10 * 4096,
        block_size: 4096,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    // Read 3 blocks at once (blocks 2, 3, 4) - should fetch batch once
    let data = cache.read_with_fetch(2 * 4096, 3 * 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 2, "block 2 should start with 2");
    assert_eq!(data[4096], 3, "block 3 should start with 3");
    assert_eq!(data[8192], 4, "block 4 should start with 4");

    // Now read block 7 - should be a cache hit (prefetched)
    let data = cache.read_with_fetch(7 * 4096, 4096, &s3, &metrics).await.unwrap();
    assert_eq!(data[0], 7, "block 7 should have been prefetched");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.s3_read_ops, 1, "only one S3 fetch");
    assert_eq!(snapshot.cache_misses, 3, "3 blocks were missing initially");
    assert_eq!(snapshot.cache_hits, 1, "block 7 was a cache hit");
}

#[tokio::test]
async fn test_batch_prefetch_with_local_dirty_blocks() {
    // Verify that batch prefetching correctly handles the case where
    // some blocks are dirty locally and others need to be fetched from S3.
    // The local dirty blocks should NOT be overwritten by S3 data.

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
        .with_blocks_per_batch(10);
    let metrics = crate::nbd::metrics::ExportMetrics::new();

    // Pre-populate S3 with old data
    let mut batch0 = vec![0u8; s3.batch_size()];
    batch0[0..4096].copy_from_slice(&vec![0xAAu8; 4096]); // block 0: old S3 data
    batch0[4096..8192].copy_from_slice(&vec![0xBBu8; 4096]); // block 1: old S3 data
    s3.put_batch(0, batch0).await.unwrap();

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "dirty_test".to_string(),
        device_size: 10 * 4096,
        block_size: 4096,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery(&s3).await.unwrap();

    // Write NEW data to block 0 locally (makes it dirty and present)
    cache.write(0, &[0xCCu8; 4096]).unwrap();

    // Now read blocks 0 and 1 together
    // Block 0 should come from local (dirty), block 1 should fetch from S3
    let data = cache.read_with_fetch(0, 8192, &s3, &metrics).await.unwrap();

    // Block 0 should have our local NEW data (not old S3 data)
    assert_eq!(data[0], 0xCC, "block 0 should have local data, not S3");

    // Block 1 should have S3 data (fetched)
    assert_eq!(data[4096], 0xBB, "block 1 should have S3 data");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.cache_hits, 1, "block 0 was local (hit)");
    assert_eq!(snapshot.cache_misses, 1, "block 1 was fetched (miss)");
}

#[test]
fn test_is_zero_block() {
    // Test the is_zero_block helper function
    let zeros = vec![0u8; 4096];
    assert!(super::inner::is_zero_block(&zeros), "all zeros should return true");

    let mut non_zeros = vec![0u8; 4096];
    non_zeros[0] = 1;
    assert!(!super::inner::is_zero_block(&non_zeros), "first byte non-zero");

    non_zeros[0] = 0;
    non_zeros[4095] = 1;
    assert!(!super::inner::is_zero_block(&non_zeros), "last byte non-zero");

    non_zeros[4095] = 0;
    non_zeros[2048] = 1;
    assert!(!super::inner::is_zero_block(&non_zeros), "middle byte non-zero");

    // Empty slice
    assert!(super::inner::is_zero_block(&[]), "empty slice is 'all zeros'");
}

#[tokio::test]
async fn test_prefetch_write_race_data_integrity() {
    // Regression test for prefetch/write race condition.
    //
    // Without the fix, this sequence causes data loss:
    // 1. S3 has old data (0xAA)
    // 2. Write starts: pwrite(0xBB) completes
    // 3. Prefetch: sees is_present=false, fetches S3, pwrite(0xAA) OVERWRITES
    // 4. Write: set_present (too late)
    // 5. File now has 0xAA (stale), but marked dirty → syncs stale data
    //
    // With the fix (set_present before pwrite in write path):
    // - Write's set_present runs early, prefetch's CAS fails → prefetch skips
    // - OR prefetch CAS wins, but write's pwrite comes after → write wins

    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let s3 = Arc::new(
        S3BlockStore::new(Arc::clone(&object_store), "test", 4096).with_blocks_per_batch(10),
    );
    let metrics = crate::nbd::metrics::ExportMetrics::new();

    // Pre-populate S3 with OLD data (0xAA)
    let mut batch0 = vec![0u8; s3.batch_size()];
    batch0[0..4096].copy_from_slice(&[0xAAu8; 4096]);
    s3.put_batch(0, batch0).await.unwrap();

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "race_test".to_string(),
        device_size: 10 * 4096,
        block_size: 4096,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };
    let cache = Arc::new(
        WriteCache::<Initializing>::open(config)
            .unwrap()
            .skip_recovery_for_test(),
    );

    // Use barriers to force a specific interleaving
    let write_started = Arc::new(AtomicBool::new(false));
    let prefetch_can_continue = Arc::new(AtomicBool::new(false));

    // Spawn concurrent tasks
    let cache_write = Arc::clone(&cache);
    let write_started_clone = Arc::clone(&write_started);
    let prefetch_can_continue_clone = Arc::clone(&prefetch_can_continue);

    let write_handle = tokio::spawn(async move {
        // Write NEW data (0xBB) - should win over stale S3 data
        cache_write.write(0, &[0xBBu8; 4096]).unwrap();
        write_started_clone.store(true, AtomicOrdering::Release);
        // Signal prefetch can continue
        prefetch_can_continue_clone.store(true, AtomicOrdering::Release);
    });

    let cache_read = Arc::clone(&cache);
    let s3_read = Arc::clone(&s3);
    let write_started_read = Arc::clone(&write_started);
    let _prefetch_can_continue_read = Arc::clone(&prefetch_can_continue);

    let read_handle = tokio::spawn(async move {
        // Wait until write has started (to maximize race window)
        while !write_started_read.load(AtomicOrdering::Acquire) {
            tokio::task::yield_now().await;
        }
        // Small delay to let write progress
        tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;

        // Now do the read which triggers prefetch
        cache_read
            .read_with_fetch(0, 4096, &s3_read, &metrics)
            .await
            .unwrap()
    });

    // Wait for both
    write_handle.await.unwrap();
    let _read_data = read_handle.await.unwrap();

    // The critical assertion: we must see the WRITE's data (0xBB), not S3's stale data (0xAA)
    //
    // Either:
    // - Write completed first, read sees 0xBB (write won)
    // - Prefetch completed first with 0xAA, but write overwrote it, read sees 0xBB (write won)
    // - Prefetch won and write hasn't completed yet, read sees 0xAA (acceptable during race)
    //   BUT: block is marked dirty, so sync will upload the final file contents
    //
    // What we CANNOT accept: file has 0xAA, read returns 0xAA, block is dirty,
    // sync uploads 0xAA (stale) - this is data loss.

    // Verify final file contents (the authoritative state)
    let final_data = cache.read_local(0, 4096).unwrap();

    // The file MUST have 0xBB (write's data). If it has 0xAA, the race caused data loss.
    assert_eq!(
        final_data[0], 0xBB,
        "RACE CONDITION BUG: write's data was overwritten by stale S3 prefetch! \
         File has 0x{:02X}, expected 0xBB",
        final_data[0]
    );

    // Also verify block is dirty (will sync the correct data)
    assert!(
        cache.dirty_block_count() > 0 || cache.syncing_block_count() > 0,
        "Block should be dirty to ensure write's data syncs to S3"
    );
}

#[tokio::test]
async fn test_concurrent_write_and_prefetch_stress() {
    // Stress test: many concurrent writes and reads to maximize race probability.
    // Run multiple iterations to increase chance of hitting the race window.

    for iteration in 0..10 {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = Arc::new(
            S3BlockStore::new(Arc::clone(&object_store), "test", 4096).with_blocks_per_batch(10),
        );
        let metrics = Arc::new(crate::nbd::metrics::ExportMetrics::new());

        // Pre-populate S3 with old data for blocks 0-9
        let batch0 = vec![0xAAu8; s3.batch_size()];
        s3.put_batch(0, batch0).await.unwrap();

        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: format!("stress_{}", iteration),
            device_size: 10 * 4096,
            block_size: 4096,
            dirty_budget_bytes: 0,
            flush_trigger: None,
        };
        let cache = Arc::new(
            WriteCache::<Initializing>::open(config)
                .unwrap()
                .skip_recovery_for_test(),
        );

        // Spawn many concurrent operations
        let mut handles = vec![];

        for block in 0..5u64 {
            let cache_w = Arc::clone(&cache);
            let write_val = (block + 1) as u8; // 1, 2, 3, 4, 5
            handles.push(tokio::spawn(async move {
                cache_w.write(block * 4096, &[write_val; 4096]).unwrap();
            }));

            let cache_r = Arc::clone(&cache);
            let s3_r = Arc::clone(&s3);
            let metrics_r = Arc::clone(&metrics);
            handles.push(tokio::spawn(async move {
                let _ = cache_r
                    .read_with_fetch(block * 4096, 4096, &s3_r, &metrics_r)
                    .await;
            }));
        }

        // Wait for all
        for h in handles {
            let _ = h.await;
        }

        // Verify: each block should have its write value, not 0xAA
        for block in 0..5u64 {
            let data = cache.read_local(block * 4096, 4096).unwrap();
            let expected = (block + 1) as u8;
            assert_eq!(
                data[0], expected,
                "Iteration {}, block {}: expected 0x{:02X}, got 0x{:02X} (0xAA = stale S3 data)",
                iteration, block, expected, data[0]
            );
        }
    }
}

// ====================================================================
// v2 Test Harness + Flush Tests
// ====================================================================

/// Test harness for v2 content-addressed flush tests.
///
/// Bundles the full v2 stack (cache + content store + pack index) in one
/// struct so tests can focus on behavior, not setup boilerplate.
struct V2Harness {
    cache: WriteCache<Active>,
    content_store: crate::nbd::content_store::ContentStore,
    pack_index: crate::nbd::pack_index::HostPackIndex,
    clean_cache: crate::nbd::cache::SimpleBlockCache,
    #[allow(dead_code)]
    dir: TempDir,
}

impl V2Harness {
    /// Create a harness with default 1MB device / 4KB blocks.
    async fn new() -> Self {
        Self::with_config(1024 * 1024, 4096).await
    }

    /// Create a harness with custom device size and block size.
    async fn with_config(device_size: u64, block_size: usize) -> Self {
        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test".to_string(),
            device_size,
            block_size,
            dirty_budget_bytes: 0,
            flush_trigger: None,
        };
        let s3 = test_s3();
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = crate::nbd::content_store::ContentStore::new(object_store, "test-bucket");
        let pack_index = crate::nbd::pack_index::HostPackIndex::new();
        let clean_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();
        Self { cache, content_store, pack_index, clean_cache, dir }
    }

    /// Flush and return stats.
    async fn flush(&self) -> FlushStats {
        let result: Result<FlushStats, CacheError> = self.cache
            .flush_to_s3(&self.content_store, &self.pack_index)
            .await;
        result.unwrap()
    }

    /// v2 read through the full tiered resolution path.
    async fn read(&self, offset: u64, len: usize) -> Bytes {
        let metrics = crate::nbd::metrics::ExportMetrics::new();
        let result: Result<Bytes, CacheError> = self.cache
            .read_v2(
                offset,
                len,
                &self.clean_cache,
                &self.pack_index,
                &self.content_store,
                &metrics,
            )
            .await;
        result.unwrap()
    }

    /// Clear the dirty store (simulates blocks being evicted after flush).
    fn clear_dirty_store(&self) {
        self.cache.inner.dirty_store.lock().unwrap().clear();
    }

    /// Get the manifest from S3.
    async fn manifest(&self) -> crate::nbd::manifest::Manifest {
        let bytes = self.content_store
            .get_manifest("test")
            .await
            .unwrap()
            .expect("manifest should exist");
        crate::nbd::manifest::Manifest::deserialize(&bytes).unwrap()
    }
}

#[tokio::test]
async fn test_flush_end_to_end() {
    let h = V2Harness::new().await;

    // Write 10 distinct blocks (each 4KB = block_size)
    for i in 0u8..10 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }

    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 10);
    assert_eq!(stats.blocks_deduped, 0);
    assert!(stats.packs_uploaded > 0);
    assert!(stats.bytes_uploaded > 0);

    let manifest = h.manifest().await;
    assert_eq!(manifest.name, "test");
    assert_eq!(manifest.chunk_size, 4096);
    assert_eq!(manifest.block_map.len(), 10);
    assert!(!manifest.pack_index.is_empty());
    assert!(h.pack_index.len() >= 10);
}

#[tokio::test]
async fn test_flush_dedup_skips_existing() {
    let h = V2Harness::new().await;

    // Write 5 blocks
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }

    let stats1 = h.flush().await;
    assert_eq!(stats1.blocks_flushed, 5);
    assert_eq!(stats1.blocks_deduped, 0);
    assert!(stats1.packs_uploaded > 0);

    // Write the same data again to new offsets — same content, new positions
    for i in 0u8..5 {
        h.cache.write((i as u64 + 5) * 4096, &vec![i + 1; 4096]).unwrap();
    }

    let stats2 = h.flush().await;
    assert_eq!(stats2.blocks_flushed, 5);
    assert_eq!(stats2.blocks_deduped, 5, "all blocks should be deduped");
    assert_eq!(stats2.packs_uploaded, 0, "no new packs needed");
}

#[tokio::test]
async fn test_flush_partial_dedup() {
    let h = V2Harness::new().await;

    // Write 10 blocks with unique data
    for i in 0u8..10 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }

    let stats1 = h.flush().await;
    assert_eq!(stats1.blocks_flushed, 10);
    assert_eq!(stats1.blocks_deduped, 0);

    // Write 10 more blocks: 5 with SAME data as before (dedup), 5 with NEW data
    for i in 0u8..5 {
        h.cache.write((i as u64 + 10) * 4096, &vec![i + 1; 4096]).unwrap();
    }
    for i in 0u8..5 {
        h.cache.write((i as u64 + 15) * 4096, &vec![i + 100; 4096]).unwrap();
    }

    let stats2 = h.flush().await;
    assert_eq!(stats2.blocks_flushed, 10);
    assert_eq!(stats2.blocks_deduped, 5, "5 blocks should be deduped");
    assert_eq!(stats2.packs_uploaded, 1, "5 new blocks = 1 pack");
}

#[tokio::test]
async fn test_flush_zero_blocks_skipped() {
    // Use 128KB blocks so ZERO_BLOCK_HASH matches
    let h = V2Harness::with_config(128 * 1024 * 4, 128 * 1024).await;

    // Write one real block
    h.cache.write(0, &vec![42u8; 128 * 1024]).unwrap();
    // Write a block of zeros — this should get ZERO_BLOCK_HASH
    h.cache.write(128 * 1024, &vec![0u8; 128 * 1024]).unwrap();

    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 2);
    assert_eq!(stats.blocks_deduped, 1, "zero block should be deduped");
    assert_eq!(stats.packs_uploaded, 1, "only one pack for the real block");
}

#[tokio::test]
async fn test_flush_clears_dirty_state() {
    let h = V2Harness::new().await;

    for i in 0u8..3 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }

    assert!(h.cache.inner.dirty_store.lock().unwrap().len() >= 3);

    h.flush().await;

    assert_eq!(
        h.cache.inner.dirty_store.lock().unwrap().len(), 0,
        "dirty store should be empty after flush"
    );

    // A second flush should be a no-op
    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 0, "no dirty blocks after flush");
}

#[tokio::test]
async fn test_flush_concurrent_write_stays_dirty() {
    let h = V2Harness::new().await;

    h.cache.write(0, &vec![1u8; 4096]).unwrap();
    h.flush().await;

    // Overwrite block 0 with different data
    h.cache.write(0, &vec![2u8; 4096]).unwrap();

    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 1, "overwritten block should be flushed");
}

#[tokio::test]
async fn test_flush_manifest_self_contained() {
    let h = V2Harness::new().await;

    // Write 30 blocks (will produce 2 packs: 25 + 5)
    for i in 0u8..30 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }

    let stats = h.flush().await;
    assert_eq!(stats.packs_uploaded, 2, "should produce 2 packs (25+5)");

    let manifest = h.manifest().await;
    let pack_hashes: std::collections::HashSet<_> =
        manifest.pack_index.iter().map(|e| e.hash).collect();

    for bm_entry in &manifest.block_map {
        if !bm_entry.hash.is_zero() {
            assert!(
                pack_hashes.contains(&bm_entry.hash),
                "block map hash {:?} should have pack index entry",
                bm_entry.hash
            );
        }
    }
}

// ====================================================================
// v2 Read Path Tests
// ====================================================================

#[tokio::test]
async fn test_v2_read_from_dirty_store() {
    let h = V2Harness::new().await;
    let data = vec![0xAAu8; 4096];
    h.cache.write(0, &data).unwrap();

    let got = h.read(0, 4096).await;
    assert_eq!(got.as_ref(), &data[..]);
}

#[tokio::test]
async fn test_v2_read_zero_block() {
    let h = V2Harness::new().await;

    // Never written — block_map entry is Blake3Hash::ZERO → returns zeros.
    let got = h.read(0, 4096).await;
    assert!(got.iter().all(|&b| b == 0), "unwritten block should be all zeros");
}

#[tokio::test]
async fn test_v2_read_trimmed_block() {
    let h = V2Harness::new().await;

    // Write a block, then zero it out.
    h.cache.write(0, &vec![0xBBu8; 4096]).unwrap();
    h.cache.zero_range(0, 4096).unwrap();

    let got = h.read(0, 4096).await;
    assert!(got.iter().all(|&b| b == 0), "trimmed block should be all zeros");
}

#[tokio::test]
async fn test_v2_read_sub_chunk() {
    let h = V2Harness::new().await;
    let data = vec![0xCCu8; 4096];
    h.cache.write(0, &data).unwrap();

    // Read 100 bytes from offset 1000 within the chunk.
    let got = h.read(1000, 100).await;
    assert_eq!(got.len(), 100);
    assert!(got.iter().all(|&b| b == 0xCC));
}

#[tokio::test]
async fn test_v2_read_spans_chunks() {
    let h = V2Harness::new().await;

    // Write two distinct chunks.
    h.cache.write(0, &vec![0x11u8; 4096]).unwrap();
    h.cache.write(4096, &vec![0x22u8; 4096]).unwrap();

    // Read across the chunk boundary: last 100 bytes of chunk 0 + first 100 of chunk 1.
    let got = h.read(3996, 200).await;
    assert_eq!(got.len(), 200);
    assert!(got[..100].iter().all(|&b| b == 0x11), "first 100 bytes from chunk 0");
    assert!(got[100..].iter().all(|&b| b == 0x22), "last 100 bytes from chunk 1");
}

#[tokio::test]
async fn test_v2_read_from_s3_pack() {
    let h = V2Harness::new().await;

    // Write blocks, flush to S3, clear dirty store.
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }
    h.flush().await;
    h.clear_dirty_store();

    // Reads should now resolve through: block_map → (miss dirty) → (miss cache) → S3 pack.
    for i in 0u8..5 {
        let got = h.read(i as u64 * 4096, 4096).await;
        assert!(
            got.iter().all(|&b| b == i + 1),
            "block {} should contain 0x{:02x}",
            i,
            i + 1
        );
    }
}

#[tokio::test]
async fn test_v2_pack_prefetch_warms_siblings() {
    let h = V2Harness::new().await;

    // Write 25 blocks (1 full pack).
    for i in 0u8..25 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }
    h.flush().await;
    h.clear_dirty_store();

    // Read block 0 — triggers pack fetch, caches all 25 blocks.
    let got = h.read(0, 4096).await;
    assert!(got.iter().all(|&b| b == 1));

    // Blocks 1-24 should now be clean_cache hits (no additional S3 fetch).
    for i in 1u8..25 {
        let got = h.read(i as u64 * 4096, 4096).await;
        assert!(
            got.iter().all(|&b| b == i + 1),
            "sibling block {} should be cached from pack prefetch",
            i
        );
    }
}

#[tokio::test]
async fn test_v2_mixed_dirty_and_clean_reads() {
    let h = V2Harness::new().await;

    // Write 5 blocks and flush to S3 (they'll become "clean" once evicted from dirty).
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }
    h.flush().await;
    h.clear_dirty_store();

    // Write 5 more blocks (dirty, not flushed).
    for i in 5u8..10 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }

    // Read all 10 blocks. First 5 from S3, last 5 from dirty_store.
    for i in 0u8..10 {
        let got = h.read(i as u64 * 4096, 4096).await;
        assert!(
            got.iter().all(|&b| b == i + 1),
            "block {} (source: {}) should contain 0x{:02x}",
            i,
            if i < 5 { "S3" } else { "dirty" },
            i + 1
        );
    }
}

#[tokio::test]
async fn test_v2_clean_cache_eviction() {
    use crate::nbd::cache::{BlockCache, SimpleBlockCache};
    use crate::nbd::block_map::blake3_128;

    // Budget: 3 blocks of 4096 bytes.
    let cache = SimpleBlockCache::new(3 * 4096);

    for i in 0u8..5 {
        let hash = blake3_128(&vec![i; 4096]);
        cache.insert(hash, Bytes::from(vec![i; 4096]));
    }

    // Oldest 2 entries should have been evicted.
    let h0 = blake3_128(&vec![0u8; 4096]);
    let h1 = blake3_128(&vec![1u8; 4096]);
    let h4 = blake3_128(&vec![4u8; 4096]);

    assert!(cache.get(&h0).await.is_none(), "oldest should be evicted");
    assert!(cache.get(&h1).await.is_none(), "second oldest should be evicted");
    assert!(cache.get(&h4).await.is_some(), "newest should be present");
}

// ========================================================================
// Snapshot tests
// ========================================================================

#[tokio::test]
async fn test_snapshot_returns_sequence_and_stats() {
    let h = V2Harness::new().await;

    // Write 5 blocks
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }

    let result: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();

    assert!(result.sequence > 0, "sequence should be > 0 after writes");
    assert_eq!(result.stats.blocks_flushed, 5);
    assert!(result.stats.packs_uploaded > 0);

    // Manifest should exist in S3
    let manifest = h.manifest().await;
    assert_eq!(manifest.name, "test");
    assert_eq!(manifest.block_map.len(), 5);
}

#[tokio::test]
async fn test_snapshot_clears_dirty_state() {
    let h = V2Harness::new().await;

    // Write blocks and snapshot
    for i in 0u8..3 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
    }

    let result1: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(result1.stats.blocks_flushed, 3);

    // Second snapshot with no new writes should be a no-op
    let result2: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(result2.stats.blocks_flushed, 0, "no dirty blocks after snapshot");
}

#[tokio::test]
async fn test_snapshot_captures_concurrent_writes() {
    let h = V2Harness::new().await;

    // Write blocks at different times
    h.cache.write(0, &vec![0xAA; 4096]).unwrap();
    h.cache.write(4096, &vec![0xBB; 4096]).unwrap();

    let result: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(result.stats.blocks_flushed, 2);

    // Write more after snapshot
    h.cache.write(8192, &vec![0xCC; 4096]).unwrap();

    // Manifest from first snapshot should have 2 blocks, not 3
    let manifest = h.manifest().await;
    assert_eq!(manifest.block_map.len(), 2);

    // Second snapshot picks up the new write
    let result2: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(result2.stats.blocks_flushed, 1);
}

// ========================================================================
// open_from_manifest tests
// ========================================================================

fn make_test_manifest(num_entries: u64, device_size: u64, block_size: u32) -> crate::nbd::manifest::Manifest {
    use crate::nbd::manifest::ManifestBlockEntry;
    let block_map: Vec<ManifestBlockEntry> = (0..num_entries)
        .map(|i| ManifestBlockEntry {
            chunk_index: i,
            hash: blake3_128(format!("block-{i}").as_bytes()),
            flags: 0,
        })
        .collect();
    crate::nbd::manifest::Manifest {
        name: "test-fork".to_string(),
        sequence: 42,
        chunk_size: block_size,
        device_size,
        block_map,
        pack_index: vec![],
    }
}

#[test]
fn test_open_from_manifest_creates_clean_cache() {
    let dir = TempDir::new().unwrap();
    let device_size: u64 = 1024 * 1024; // 1MB
    let block_size: usize = 4096;
    let manifest = make_test_manifest(5, device_size, block_size as u32);

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test-manifest".to_string(),
        device_size,
        block_size,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };

    let cache = WriteCache::<Initializing>::open_from_manifest(
        config,
        &manifest,
        None,
    )
    .unwrap();

    // No dirty blocks -- everything is in S3
    assert_eq!(cache.dirty_block_count(), 0);
    assert_eq!(cache.syncing_block_count(), 0);

    // Sequence should match the manifest
    assert_eq!(cache.inner.sequence.current(), 42);

    // Block map should have the 5 entries from the manifest
    for i in 0u64..5 {
        let (hash, seq) = cache.inner.block_map_get(i as usize);
        let expected_hash = blake3_128(format!("block-{i}").as_bytes());
        assert_eq!(hash, expected_hash, "block map entry {i} hash mismatch");
        assert_eq!(seq, 42, "block map entry {i} sequence mismatch");
    }

    // Entries beyond the manifest should be empty (zero hash)
    let (hash, seq) = cache.inner.block_map_get(5);
    assert!(hash.is_zero(), "entry 5 should be empty");
    assert_eq!(seq, 0);

    // No blocks present locally
    assert_eq!(cache.inner.count_present(), 0);
}

#[test]
fn test_open_from_manifest_with_forked_overlay() {
    use crate::nbd::block_map::{BlockMap, BlockMapEntry, BlockMapKind};

    let dir = TempDir::new().unwrap();
    let device_size: u64 = 1024 * 1024;
    let block_size: usize = 4096;
    let manifest = make_test_manifest(5, device_size, block_size as u32);

    // Build a parent BlockMap from the manifest entries
    let mut parent_bm = BlockMap::new(device_size, block_size as u32);
    for entry in &manifest.block_map {
        parent_bm.set(
            entry.chunk_index as usize,
            BlockMapEntry {
                hash: entry.hash,
                flags: 0,
                sequence: manifest.sequence,
            },
        );
    }
    let parent = Arc::new(parent_bm);

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test-forked".to_string(),
        device_size,
        block_size,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };

    let cache = WriteCache::<Initializing>::open_from_manifest(
        config,
        &manifest,
        Some(Arc::clone(&parent)),
    )
    .unwrap();

    // Verify the block map is the Forked variant
    {
        let bm = cache.inner.block_map.read().unwrap();
        assert!(
            matches!(&*bm, BlockMapKind::Forked(_)),
            "block map should be Forked variant"
        );
    }

    // Reads should resolve from the parent
    for i in 0u64..5 {
        let (hash, seq) = cache.inner.block_map_get(i as usize);
        let expected_hash = blake3_128(format!("block-{i}").as_bytes());
        assert_eq!(hash, expected_hash, "forked entry {i} hash mismatch");
        assert_eq!(seq, 42, "forked entry {i} sequence mismatch");
    }

    // No dirty blocks
    assert_eq!(cache.dirty_block_count(), 0);
}

#[test]
fn test_open_from_manifest_fork_writes_to_overlay() {
    use crate::nbd::block_map::{BlockMap, BlockMapEntry, BlockMapKind};

    let dir = TempDir::new().unwrap();
    let device_size: u64 = 1024 * 1024;
    let block_size: usize = 4096;
    let manifest = make_test_manifest(5, device_size, block_size as u32);

    // Build parent from manifest
    let mut parent_bm = BlockMap::new(device_size, block_size as u32);
    for entry in &manifest.block_map {
        parent_bm.set(
            entry.chunk_index as usize,
            BlockMapEntry {
                hash: entry.hash,
                flags: 0,
                sequence: manifest.sequence,
            },
        );
    }
    let parent = Arc::new(parent_bm);

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test-fork-write".to_string(),
        device_size,
        block_size,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };

    let cache = WriteCache::<Initializing>::open_from_manifest(
        config,
        &manifest,
        Some(Arc::clone(&parent)),
    )
    .unwrap();

    // Write new data to chunk 0 -- should go to the overlay
    let new_data = vec![0xAB; block_size];
    cache.write(0, &new_data).unwrap();

    // The overlay should have grown
    {
        let bm = cache.inner.block_map.read().unwrap();
        if let BlockMapKind::Forked(f) = &*bm {
            assert_eq!(
                f.overlay_len(),
                1,
                "overlay should have 1 entry after write"
            );
        } else {
            panic!("block map should still be Forked variant");
        }
    }

    // The written chunk should have a new hash (not the parent's hash)
    let (hash, seq) = cache.inner.block_map_get(0);
    let parent_hash = blake3_128("block-0".as_bytes());
    assert_ne!(hash, parent_hash, "hash should differ from parent after write");
    assert!(seq > 42, "sequence should be beyond the manifest sequence");

    // Parent's entry at chunk 0 should be unchanged
    let parent_entry = parent.get(0);
    assert_eq!(parent_entry.hash, parent_hash, "parent must be unmodified");
    assert_eq!(parent_entry.sequence, 42, "parent sequence must be unmodified");

    // Chunk 1 should still read from parent (not in overlay)
    let (hash_1, seq_1) = cache.inner.block_map_get(1);
    let expected_hash_1 = blake3_128("block-1".as_bytes());
    assert_eq!(hash_1, expected_hash_1, "unwritten chunk should read from parent");
    assert_eq!(seq_1, 42);

    // Should have 1 dirty block
    assert_eq!(cache.dirty_block_count(), 1);
}
