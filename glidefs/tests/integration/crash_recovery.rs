//! Crash recovery tests for GlideFS.
//!
//! These tests verify that data survives process crashes during sync operations:
//! 1. Blocks in `Syncing` state are converted to `Dirty` on restart
//! 2. Data written before crash survives recovery
//! 3. Recovery correctly syncs data to S3
//!
//! Run with: `cargo test --features test-utils --test integration crash_recovery`

use std::sync::Arc;

use object_store::ObjectStore;
use tempfile::TempDir;

use glidefs::nbd::block_store::S3BlockStore;
use glidefs::nbd::metrics::ExportMetrics;
use glidefs::nbd::state::Initializing;
use glidefs::nbd::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB
const DEVICE_SIZE: u64 = 10 * 1024 * 1024; // 10MB

/// Helper to create a test cache config.
fn test_config(dir: &TempDir, name: &str) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    }
}

/// Helper to create an S3 block store with in-memory backend.
fn test_s3(store: Arc<dyn ObjectStore>) -> S3BlockStore {
    S3BlockStore::new(store, "test", BLOCK_SIZE).with_blocks_per_batch(10)
}

// =============================================================================
// CRASH RECOVERY TESTS
// =============================================================================

/// Test: Syncing → Dirty conversion in load_metadata().
///
/// This is the fundamental crash recovery mechanism. When the cache opens,
/// any blocks in Syncing state (from a previous crash mid-sync) must be
/// converted to Dirty so they can be re-synced.
///
/// This test isolates JUST the state conversion without any S3 interaction.
#[tokio::test]
async fn test_syncing_to_dirty_conversion() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "conversion_test");

    let test_data: Vec<u8> = vec![0xFF; BLOCK_SIZE];

    // Session 1: Write blocks and transition to Syncing state
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write 3 blocks
        cache.write(0, &test_data).unwrap();
        cache.write(BLOCK_SIZE as u64, &test_data).unwrap();
        cache.write(2 * BLOCK_SIZE as u64, &test_data).unwrap();

        // Verify initial state
        assert_eq!(cache.dirty_block_count(), 3);
        assert_eq!(cache.syncing_block_count(), 0);

        // Claim all blocks - transitions Dirty → Syncing
        let claimed = cache.claim_dirty_blocks(10);
        assert_eq!(claimed.len(), 3);
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 3);

        // Save metadata with blocks in Syncing state (simulating crash point)
        cache.save_metadata().unwrap();

        // Drop without syncing to S3 - simulates crash
    }

    // Session 2: Reopen and verify Syncing → Dirty conversion
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();

        // Use skip_recovery to see the raw state after load_metadata()
        let cache = cache.skip_recovery_for_test();

        // THE CRITICAL ASSERTION: Syncing blocks must become Dirty
        assert_eq!(
            cache.syncing_block_count(),
            0,
            "All Syncing blocks must be converted to Dirty"
        );
        assert_eq!(
            cache.dirty_block_count(),
            3,
            "All 3 blocks should now be Dirty (recovered from Syncing)"
        );

        // Data should still be intact
        for i in 0..3 {
            let data = cache.read_local(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
            assert_eq!(
                data.as_ref(),
                &test_data[..],
                "Block {} data should survive crash",
                i
            );
        }
    }
}

/// Test: Blocks in Syncing state are recovered as Dirty after crash.
///
/// This simulates a crash during sync_blocks_batched():
/// 1. Write data (blocks become Dirty)
/// 2. claim_dirty_blocks() transitions blocks to Syncing
/// 3. Simulate crash: save metadata but DON'T complete S3 sync
/// 4. Reopen cache and complete recovery
/// 5. Verify: data survives and is synced to S3
/// 6. Verify: data readable from cold cache (S3)
#[tokio::test]
async fn test_crash_during_sync_recovers_blocks() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "crash_test");

    // Test data - distinctive pattern so we can verify it
    let test_data: Vec<u8> = (0..BLOCK_SIZE).map(|i| (i % 256) as u8).collect();

    // Phase 1: Write data and claim blocks (simulating sync in progress)
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write to multiple blocks
        cache.write(0, &test_data).unwrap();
        cache.write(BLOCK_SIZE as u64, &test_data).unwrap();
        cache.write(2 * BLOCK_SIZE as u64, &test_data).unwrap();

        assert_eq!(cache.dirty_block_count(), 3, "Should have 3 dirty blocks");

        // Claim blocks for sync - this transitions them to Syncing state
        let claimed = cache.claim_dirty_blocks(10);
        assert_eq!(claimed.len(), 3, "Should claim 3 blocks");
        assert_eq!(cache.syncing_block_count(), 3, "Should have 3 syncing blocks");
        assert_eq!(cache.dirty_block_count(), 0, "Dirty count should be 0 after claim");

        // CRITICAL: Save metadata with blocks in Syncing state
        // This is what would happen if we crashed mid-sync
        cache.save_metadata().unwrap();

        // DON'T sync to S3 - simulate crash by just dropping the cache
        // The blocks are still in Syncing state in the metadata file
        drop(cache);
    }

    // Phase 2: Reopen and complete recovery
    // The Syncing → Dirty conversion happens internally in load_metadata()
    // We verify it worked by checking that recovery syncs the data to S3
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();

        // Complete recovery - this syncs the dirty blocks to S3
        // (The Syncing blocks were converted to Dirty by load_metadata)
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // After recovery, data should still be readable locally
        let read_data = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(
            read_data.as_ref(),
            &test_data[..],
            "Data should survive crash recovery"
        );

        // And dirty count should be 0 (synced to S3)
        assert_eq!(cache.dirty_block_count(), 0, "Blocks should be synced after recovery");
    }

    // Phase 3: Verify data survives from cold cache (different node simulation)
    {
        let cold_dir = TempDir::new().unwrap();
        let cold_config = test_config(&cold_dir, "cold_node");
        let metrics = Arc::new(ExportMetrics::new());

        let cold_cache = WriteCache::<Initializing>::open(cold_config).unwrap();
        let cold_cache = cold_cache.finish_recovery(&s3).await.unwrap();

        // Read from S3 (cold cache has no local data)
        let s3_data = cold_cache
            .read_with_fetch(0, BLOCK_SIZE, &s3, &metrics)
            .await
            .unwrap();

        assert_eq!(
            s3_data.as_ref(),
            &test_data[..],
            "Data should be readable from S3 after crash recovery"
        );
    }
}

/// Test: Multiple crashes during recovery are handled correctly.
///
/// Simulates: crash → partial recovery → crash → full recovery
/// The key invariant is that data is never lost, even with repeated crashes.
#[tokio::test]
async fn test_repeated_crash_during_recovery() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "repeated_crash");

    let test_data: Vec<u8> = vec![0xAB; BLOCK_SIZE];

    // First session: write and crash mid-sync
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        cache.write(0, &test_data).unwrap();
        let _ = cache.claim_dirty_blocks(10);
        cache.save_metadata().unwrap();
        // Crash without syncing - blocks are in Syncing state
    }

    // Second session: open, transition to active, claim again, crash again
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        // Skip recovery to test the claim/crash cycle without syncing to S3
        let cache = cache.skip_recovery_for_test();

        // CRITICAL: Verify the Syncing → Dirty conversion happened
        // This is the core crash recovery guarantee being tested
        assert_eq!(
            cache.syncing_block_count(),
            0,
            "Syncing blocks must be converted to Dirty on recovery"
        );
        assert_eq!(
            cache.dirty_block_count(),
            1,
            "Syncing block should now be Dirty"
        );

        // Claim and crash again
        let _ = cache.claim_dirty_blocks(10);
        cache.save_metadata().unwrap();
        // Crash again - blocks are in Syncing state again
    }

    // Third session: finally complete recovery
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Data should survive multiple crashes
        let data = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(data.as_ref(), &test_data[..], "Data should survive multiple crashes");

        // Blocks should be synced now
        assert_eq!(cache.dirty_block_count(), 0, "Should be synced after recovery");
    }
}

/// Test: Write during recovery is handled correctly.
///
/// New writes that come in after recovery should work correctly,
/// overwriting recovered data.
#[tokio::test]
async fn test_write_after_recovery_overwrites() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "write_during_recovery");

    let old_data: Vec<u8> = vec![0x11; BLOCK_SIZE];
    let new_data: Vec<u8> = vec![0x22; BLOCK_SIZE];

    // First session: write and crash mid-sync
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        cache.write(0, &old_data).unwrap();
        let _ = cache.claim_dirty_blocks(10);
        cache.save_metadata().unwrap();
        // Crash without completing sync
    }

    // Second session: complete recovery, then write NEW data
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();

        // Complete recovery (syncs old_data to S3)
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Now write new data to the same block (overwrites recovered data)
        cache.write(0, &new_data).unwrap();

        // Drain to S3
        cache.drain_for_snapshot(&s3).await.unwrap();
    }

    // Verify: new data should be in S3, not old data
    {
        let cold_dir = TempDir::new().unwrap();
        let cold_config = test_config(&cold_dir, "verify");
        let metrics = Arc::new(ExportMetrics::new());

        let cache = WriteCache::<Initializing>::open(cold_config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        let data = cache.read_with_fetch(0, BLOCK_SIZE, &s3, &metrics).await.unwrap();
        assert_eq!(
            data.as_ref(),
            &new_data[..],
            "New data should overwrite old data in S3"
        );
    }
}

/// Test: Crash with partially synced batch.
///
/// Simulates crash where some blocks in a batch synced but others didn't.
/// The unsynced blocks should be re-synced on recovery.
#[tokio::test]
async fn test_crash_with_partial_batch_sync() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "partial_sync");
    let metrics = Arc::new(ExportMetrics::new());

    // Write 5 blocks worth of distinctive data
    let mut block_data = Vec::new();
    for i in 0..5 {
        let data: Vec<u8> = vec![i as u8; BLOCK_SIZE];
        block_data.push(data);
    }

    // First session: write all blocks, sync some, crash before syncing others
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        for (i, data) in block_data.iter().enumerate() {
            cache.write(i as u64 * BLOCK_SIZE as u64, data).unwrap();
        }

        // Claim only the first 2 blocks
        let claimed = cache.claim_dirty_blocks(2);
        assert_eq!(claimed.len(), 2);

        // Sync only those 2 blocks to S3
        cache.sync_blocks_batched(&s3, claimed).await.unwrap();

        // Now claim the remaining 3 but DON'T sync them
        let remaining = cache.claim_dirty_blocks(10);
        assert_eq!(remaining.len(), 3);
        cache.save_metadata().unwrap();
        // Crash - 3 blocks in Syncing state never made it to S3
    }

    // Second session: recover and verify ALL data survives
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();

        // Recovery will convert Syncing→Dirty and sync to S3
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // All 5 blocks should be readable (2 were already synced, 3 recovered)
        for (i, expected) in block_data.iter().enumerate() {
            let data = cache
                .read_with_fetch(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE, &s3, &metrics)
                .await
                .unwrap();
            assert_eq!(
                data.as_ref(),
                &expected[..],
                "Block {} should have correct data after partial sync crash",
                i
            );
        }

        // All blocks should be synced now
        assert_eq!(cache.dirty_block_count(), 0, "All blocks should be synced after recovery");
    }
}

/// Test: Metadata file atomicity on crash.
///
/// Verifies that the atomic write pattern (write temp → fsync → rename)
/// protects against metadata corruption.
#[tokio::test]
async fn test_metadata_atomicity() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "atomicity");

    let test_data: Vec<u8> = vec![0xCC; BLOCK_SIZE];

    // Session 1: Write, save metadata multiple times
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        cache.write(0, &test_data).unwrap();
        cache.save_metadata().unwrap();

        cache.write(BLOCK_SIZE as u64, &test_data).unwrap();
        cache.save_metadata().unwrap();

        cache.write(2 * BLOCK_SIZE as u64, &test_data).unwrap();
        cache.save_metadata().unwrap();
    }

    // Session 2: Verify metadata loaded correctly
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();

        // Use skip_recovery_for_test to get to Active state and check dirty count
        let cache = cache.skip_recovery_for_test();

        // Should have 3 dirty blocks from previous session
        assert_eq!(
            cache.dirty_block_count(),
            3,
            "Metadata should track 3 dirty blocks"
        );

        // All data should be readable
        for i in 0..3 {
            let data = cache.read_local(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
            assert_eq!(data.as_ref(), &test_data[..], "Block {} data mismatch", i);
        }
    }
}

// =============================================================================
// POWER FAILURE SIMULATION TESTS
// =============================================================================
//
// These tests simulate filesystem-level failures that could occur during
// a power failure, going beyond the application-level crash recovery tests above.

/// Test: Leftover .meta.tmp file from previous crash is ignored.
///
/// Scenario: A previous crash left behind a .meta.tmp file, but the .meta file is valid.
/// The cache should use .meta and ignore the stale .meta.tmp.
#[tokio::test]
async fn test_leftover_temp_file_ignored() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "temp_file_test");

    let test_data: Vec<u8> = vec![0xAA; BLOCK_SIZE];

    // Session 1: Create valid metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();
        cache.write(0, &test_data).unwrap();
        cache.save_metadata().unwrap();
    }

    // Manually create a stale .meta.tmp file with garbage
    let tmp_path = config.metadata_path().with_extension("meta.tmp");
    std::fs::write(&tmp_path, b"GARBAGE_FROM_CRASHED_WRITE").unwrap();

    // Session 2: Should open successfully using .meta, ignoring .meta.tmp
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();

        // Data should be intact from the valid .meta file
        assert_eq!(cache.dirty_block_count(), 1);
        let data = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(data.as_ref(), &test_data[..], "Data should be intact");
    }

    // Verify temp file still exists (cache didn't clean it up on open)
    assert!(tmp_path.exists(), "Temp file should still exist");
}

/// Test: Torn write to temp file doesn't affect valid metadata.
///
/// Scenario: Power failure during file.write_all() leaves a truncated .meta.tmp.
/// The valid .meta file should be used.
#[tokio::test]
async fn test_torn_write_temp_file() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "torn_write_test");

    let test_data: Vec<u8> = vec![0xBB; BLOCK_SIZE];

    // Session 1: Create valid metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();
        cache.write(0, &test_data).unwrap();
        cache.save_metadata().unwrap();
    }

    // Create truncated .meta.tmp (simulating crash during write)
    // Only write header, missing block states
    let tmp_path = config.metadata_path().with_extension("meta.tmp");
    let mut truncated = Vec::new();
    truncated.extend_from_slice(b"ZFSCACHE"); // magic
    truncated.extend_from_slice(&3u32.to_le_bytes()); // version
    // Missing: device_size, block_size, num_blocks, states, bitmap
    std::fs::write(&tmp_path, &truncated).unwrap();

    // Session 2: Should succeed with valid .meta
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        assert_eq!(cache.dirty_block_count(), 1);
    }
}

/// Test: Truncated metadata file is detected and rejected.
///
/// Scenario: Storage corruption truncated the .meta file mid-read.
/// This should return an IO error from read_exact().
#[tokio::test]
async fn test_truncated_metadata_file() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "truncated_meta");

    // Create cache directory
    std::fs::create_dir_all(&config.cache_dir).unwrap();

    // Create data file (required for cache open)
    let data_path = config.data_path();
    std::fs::write(&data_path, vec![0u8; config.device_size as usize]).unwrap();

    // Create truncated .meta file (header only, no block states)
    let meta_path = config.metadata_path();
    let mut truncated = Vec::new();
    truncated.extend_from_slice(b"ZFSCACHE");
    truncated.extend_from_slice(&3u32.to_le_bytes()); // version
    truncated.extend_from_slice(&config.device_size.to_le_bytes());
    truncated.extend_from_slice(&(config.block_size as u64).to_le_bytes());
    truncated.extend_from_slice(&(config.num_blocks() as u64).to_le_bytes());
    // Missing: block states and presence bitmap
    std::fs::write(&meta_path, &truncated).unwrap();

    // Attempt to open: should fail with IO error (read_exact fails on short read)
    let result = WriteCache::<Initializing>::open(config);
    assert!(result.is_err(), "Should fail to open truncated metadata");
}

/// Test: Corrupted magic bytes are detected and rejected.
///
/// Scenario: Storage corruption (bit flip) in the magic header bytes.
/// This should return InvalidMetadata error.
#[tokio::test]
async fn test_corrupted_magic_bytes() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "corrupt_magic");

    // Session 1: Create valid metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();
        cache.write(0, &vec![0xCC; BLOCK_SIZE]).unwrap();
        cache.save_metadata().unwrap();
    }

    // Corrupt the magic bytes
    let meta_path = config.metadata_path();
    let mut data = std::fs::read(&meta_path).unwrap();
    data[0] = 0xFF; // Corrupt first byte of "ZFSCACHE"
    std::fs::write(&meta_path, &data).unwrap();

    // Attempt to open: should fail with InvalidMetadata
    let result = WriteCache::<Initializing>::open(config);
    assert!(result.is_err(), "Should reject corrupted magic bytes");
}

/// Test: Device size shrink between config and metadata is rejected.
///
/// Scenario: Metadata was saved with larger device size than current config.
/// Shrinking is rejected to prevent data loss (growing is allowed).
#[tokio::test]
async fn test_device_size_shrink_rejected() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "size_mismatch");

    // Session 1: Create valid metadata with original device size
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();
        cache.save_metadata().unwrap();
    }

    // Create new config with smaller device size (shrink is rejected)
    let new_config = WriteCacheConfig {
        cache_dir: config.cache_dir.clone(),
        device_name: config.device_name.clone(),
        device_size: config.device_size / 2, // Halve the size
        block_size: config.block_size,
        dirty_budget_bytes: 0,
        flush_trigger: None,
    };

    // Resize data file to match new config
    std::fs::write(
        new_config.data_path(),
        vec![0u8; new_config.device_size as usize],
    )
    .unwrap();

    // Attempt to open with smaller size: should fail
    let result = WriteCache::<Initializing>::open(new_config);
    assert!(result.is_err(), "Should reject device size shrink");
}

/// Test: Recovery from repeated crashes during metadata save operations.
///
/// Scenario: Multiple crash cycles with orphaned .meta.tmp files.
/// Each recovery should succeed using the valid .meta file.
#[tokio::test]
async fn test_repeated_metadata_crash_cycles() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = test_s3(Arc::clone(&s3_backend));
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "repeated_meta_crash");

    let test_data: Vec<u8> = vec![0xDD; BLOCK_SIZE];

    // Session 1: Write data and save
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();
        cache.write(0, &test_data).unwrap();
        cache.save_metadata().unwrap();
    }

    // Simulate 3 crash cycles with orphaned .meta.tmp files
    for i in 0..3 {
        // Create orphaned temp file simulating crash during metadata save
        let tmp_path = config.metadata_path().with_extension("meta.tmp");
        std::fs::write(&tmp_path, format!("CRASH_CYCLE_{}", i).as_bytes()).unwrap();

        // Reopen should succeed using valid .meta
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();

        // Data should survive every crash cycle
        let data = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(
            data.as_ref(),
            &test_data[..],
            "Data should survive crash cycle {}",
            i
        );

        // Save metadata (this overwrites the temp file via atomic rename pattern)
        cache.save_metadata().unwrap();
    }
}

/// Test: First-ever metadata save crash leaves recoverable state.
///
/// Scenario: Very first cache open, write, save_metadata() crashes after fsync
/// but before rename. Only .meta.tmp exists (no .meta file).
/// Expected: Cache opens fresh since no valid .meta exists.
#[tokio::test]
async fn test_crash_before_first_metadata_rename() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "first_rename_crash");

    // Create cache directory (normally done by cache open)
    std::fs::create_dir_all(&config.cache_dir).unwrap();

    // Create a valid .meta.tmp file (simulating crash after fsync but before rename)
    // This would have been renamed to .meta if the save had completed
    let tmp_path = config.metadata_path().with_extension("meta.tmp");
    let num_blocks = config.num_blocks();

    let mut valid_temp = Vec::new();
    valid_temp.extend_from_slice(b"ZFSCACHE");
    valid_temp.extend_from_slice(&3u32.to_le_bytes()); // version
    valid_temp.extend_from_slice(&config.device_size.to_le_bytes());
    valid_temp.extend_from_slice(&(config.block_size as u64).to_le_bytes());
    valid_temp.extend_from_slice(&(num_blocks as u64).to_le_bytes());
    valid_temp.extend(vec![1u8; num_blocks]); // All blocks dirty
    valid_temp.extend(vec![0xFFu8; num_blocks.div_ceil(8)]); // All present
    std::fs::write(&tmp_path, &valid_temp).unwrap();

    // Open cache: should start fresh because .meta doesn't exist
    // The .meta.tmp is orphaned from a failed first save and is ignored
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.skip_recovery_for_test();

    // Should have 0 dirty blocks (fresh start)
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "Should start fresh when only .meta.tmp exists"
    );
}
