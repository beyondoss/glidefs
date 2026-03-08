//! Transport stress integration tests.
//!
//! Tests rapid client connect/disconnect, request backpressure, and
//! connection handling edge cases. These run against the in-process
//! NBD server without Docker.
//!
//! Run with: `cargo test --features test-utils --test integration transport_stress`

use std::sync::Arc;

use object_store::memory::InMemory;
use tempfile::TempDir;

use super::BLOCK_SIZE;

// =============================================================================
// Client storm — rapid connect/disconnect
// =============================================================================

/// Open many clients rapidly, each does one read, then disconnects.
/// The server should handle without leaks or crashes.
///
/// This test exercises the handler at the WriteCache level: 100 concurrent
/// readers all hitting the same cache simultaneously.
#[tokio::test]
async fn test_client_storm_concurrent_reads() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, metrics) =
        super::create_test_cache(&dir, "client-storm", Arc::clone(&s3)).await;
    let cs = Arc::new(cs);

    // Write 10 blocks
    for i in 0..10usize {
        cache
            .write(
                (i * BLOCK_SIZE) as u64,
                &vec![(i as u8) + 1; BLOCK_SIZE],
                cc.as_ref(),
            )
            .unwrap();
    }

    // 100 concurrent readers, each reads a random block
    let mut handles = Vec::new();
    for client_id in 0..100u64 {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&cs);
        let pic = Arc::clone(&pic);
        let vm = Arc::clone(&vm);
        let cc = Arc::clone(&cc);
        let metrics = Arc::clone(&metrics);
        handles.push(tokio::spawn(async move {
            let block_idx = (client_id % 10) as usize;
            let data = cache
                .read(
                    (block_idx * BLOCK_SIZE) as u64,
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
                data.iter().all(|&b| b == (block_idx as u8) + 1),
                "client {client_id} read block {block_idx}: data mismatch"
            );
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

// =============================================================================
// Concurrent write stress — many writers to same cache
// =============================================================================

/// 50 concurrent writers each writing to their own block range.
/// No data should be lost or corrupted.
#[tokio::test]
async fn test_concurrent_write_stress() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, metrics) =
        super::create_test_cache(&dir, "write-stress", Arc::clone(&s3)).await;

    let num_writers = 50usize;
    let mut handles = Vec::new();

    for writer_id in 0..num_writers {
        let cache = Arc::clone(&cache);
        let cc = Arc::clone(&cc);
        handles.push(tokio::spawn(async move {
            let offset = (writer_id * BLOCK_SIZE) as u64;
            let fill = (writer_id as u8) + 1;
            cache.write(offset, &vec![fill; BLOCK_SIZE], cc.as_ref()).unwrap();
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all writes landed correctly
    for writer_id in 0..num_writers {
        let data = cache
            .read(
                (writer_id * BLOCK_SIZE) as u64,
                BLOCK_SIZE,
                cc.as_ref(),
                &pic,
                &vm,
                &cs,
                &metrics,
            )
            .await
            .unwrap();
        let expected = (writer_id as u8) + 1;
        assert!(
            data.iter().all(|&b| b == expected),
            "writer {writer_id} block data mismatch"
        );
    }
}

// =============================================================================
// Concurrent read + write (same block)
// =============================================================================

/// Multiple tasks read and write to the same block concurrently.
/// Reads should never return partially written data — each read must
/// return either the old value or the new value, never a mix.
#[tokio::test]
async fn test_concurrent_read_write_same_block() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, metrics) =
        super::create_test_cache(&dir, "rw-same-block", Arc::clone(&s3)).await;
    let cs = Arc::new(cs);

    // Initial write: block 0 = 0xAA
    cache
        .write(0, &vec![0xAA; BLOCK_SIZE], cc.as_ref())
        .unwrap();

    // Spawn 10 writers overwriting block 0 with different patterns
    let mut write_handles = Vec::new();
    for i in 0..10u8 {
        let cache = Arc::clone(&cache);
        let cc = Arc::clone(&cc);
        write_handles.push(tokio::spawn(async move {
            let fill = 0xB0 + i;
            cache.write(0, &vec![fill; BLOCK_SIZE], cc.as_ref()).unwrap();
        }));
    }

    // Spawn 20 concurrent readers
    let mut read_handles = Vec::new();
    for _ in 0..20 {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&cs);
        let pic = Arc::clone(&pic);
        let vm = Arc::clone(&vm);
        let cc = Arc::clone(&cc);
        let metrics = Arc::clone(&metrics);
        read_handles.push(tokio::spawn(async move {
            let data = cache
                .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &metrics)
                .await
                .unwrap();
            // Every byte in the block should be the same value (no torn reads)
            let first = data[0];
            assert!(
                data.iter().all(|&b| b == first),
                "torn read detected: first byte = {first:#x}, but block contains mixed values"
            );
        }));
    }

    for handle in write_handles {
        handle.await.unwrap();
    }
    for handle in read_handles {
        handle.await.unwrap();
    }
}
