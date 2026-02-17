//! Crash recovery tests for GlideFS.
//!
//! These tests verify that data survives process crashes:
//! 1. Metadata atomicity protects against corruption
//! 2. Power-failure edge cases (torn writes, orphaned temp files)
//! 3. Data written before crash survives recovery
//! 4. New writes after recovery overwrite correctly via v2 flush/read
//!
//! Run with: `cargo test --features test-utils --test integration crash_recovery`

use std::sync::Arc;

use object_store::ObjectStore;
use tempfile::TempDir;

use glidefs::nbd::cache::SimpleBlockCache;
use glidefs::nbd::state::Initializing;
use glidefs::nbd::write_cache::{WriteCache, WriteCacheConfig};

use super::{create_v2_cold_reader, BLOCK_SIZE, DEVICE_SIZE};

/// Helper to create a test cache config with `wal_sync: false`.
fn test_config(dir: &TempDir, name: &str) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    }
}

// =============================================================================
// METADATA & POWER FAILURE TESTS
// =============================================================================

/// Test: Metadata file atomicity on crash.
///
/// Verifies that the atomic write pattern (write temp -> fsync -> rename)
/// protects against metadata corruption.
#[tokio::test]
async fn test_metadata_atomicity() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "atomicity");

    let test_data: Vec<u8> = vec![0xCC; BLOCK_SIZE];

    // Session 1: Write, save metadata multiple times
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        cache.write(0, &test_data, &clean_cache).unwrap();
        cache.save_metadata().unwrap();

        cache.write(BLOCK_SIZE as u64, &test_data, &clean_cache).unwrap();
        cache.save_metadata().unwrap();

        cache.write(2 * BLOCK_SIZE as u64, &test_data, &clean_cache).unwrap();
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
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "temp_file_test");

    let test_data: Vec<u8> = vec![0xAA; BLOCK_SIZE];

    // Session 1: Create valid metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);
        cache.write(0, &test_data, &clean_cache).unwrap();
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
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "torn_write_test");

    let test_data: Vec<u8> = vec![0xBB; BLOCK_SIZE];

    // Session 1: Create valid metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);
        cache.write(0, &test_data, &clean_cache).unwrap();
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
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "corrupt_magic");

    // Session 1: Create valid metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);
        cache.write(0, &vec![0xCC; BLOCK_SIZE], &clean_cache).unwrap();
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
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "size_mismatch");

    // Session 1: Create valid metadata with original device size
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        cache.save_metadata().unwrap();
    }

    // Create new config with smaller device size (shrink is rejected)
    let new_config = WriteCacheConfig {
        cache_dir: config.cache_dir.clone(),
        device_name: config.device_name.clone(),
        device_size: config.device_size / 2, // Halve the size
        block_size: config.block_size,
        wal_sync: false,
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
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "repeated_meta_crash");

    let test_data: Vec<u8> = vec![0xDD; BLOCK_SIZE];

    // Session 1: Write data and save
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);
        cache.write(0, &test_data, &clean_cache).unwrap();
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

// =============================================================================
// V2 API RECOVERY TEST
// =============================================================================

/// Test: Write during recovery is handled correctly.
///
/// New writes that come in after recovery should work correctly,
/// overwriting recovered data. Uses the v2 flush/read path.
#[tokio::test]
async fn test_write_after_recovery_overwrites() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "write_during_recovery");

    let old_data: Vec<u8> = vec![0x11; BLOCK_SIZE];
    let new_data: Vec<u8> = vec![0x22; BLOCK_SIZE];

    // First session: write old data, save metadata, simulate crash
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        cache.write(0, &old_data, &clean_cache).unwrap();
        cache.save_metadata().unwrap();
        // Crash without flushing to S3
    }

    // Second session: complete recovery, write new data, flush to S3
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        // Now write new data to the same block (overwrites recovered data)
        cache.write(0, &new_data, &clean_cache).unwrap();

        // Flush to S3 via v2 pack path
        let content_store = glidefs::nbd::content_store::ContentStore::new(
            Arc::clone(&s3_backend),
            "test",
        );
        let pack_index = glidefs::nbd::pack_index::HostPackIndex::new();
        cache.flush_to_s3(&content_store, &pack_index).await.unwrap();
    }

    // Verify: new data should be in S3, not old data
    {
        let cold_dir = TempDir::new().unwrap();
        let (cold_cache, content_store, pack_index, clean_cache, metrics) =
            create_v2_cold_reader(&cold_dir, "write_during_recovery", Arc::clone(&s3_backend)).await;

        // Read from S3 (cold cache has no local data)
        let data = cold_cache
            .read_v2(0, BLOCK_SIZE, clean_cache.as_ref(), &pack_index, &content_store, &metrics)
            .await
            .unwrap();
        assert_eq!(
            data.as_ref(),
            &new_data[..],
            "New data should overwrite old data in S3"
        );
    }
}

// =============================================================================
// WAL RECOVERY INTEGRATION TEST
// =============================================================================

/// Test: WAL recovery reconstructs block map when metadata was never saved.
///
/// Scenario: Write blocks (WAL entries written), crash before save_metadata().
/// On reopen, finish_recovery() replays the WAL, re-reads blocks from SSD,
/// and reconstructs the v2 block map. Data should then flush to S3 correctly.
///
/// Note: dirty_block_count() tracks the v1 block_states metadata, which is
/// NOT updated by WAL replay. The v2 block_map (FLAG_DIRTY)
/// is populated by WAL replay, so flush_to_s3 picks up the blocks correctly.
#[tokio::test]
async fn test_wal_recovery_without_metadata_save() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "wal_recovery");

    let block_0_data: Vec<u8> = (0..BLOCK_SIZE).map(|i| (i % 251) as u8).collect();
    let block_1_data: Vec<u8> = (0..BLOCK_SIZE).map(|i| ((i + 100) % 251) as u8).collect();

    // Session 1: Write blocks but DO NOT save metadata — simulate crash
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        cache.write(0, &block_0_data, &clean_cache).unwrap();
        cache.write(BLOCK_SIZE as u64, &block_1_data, &clean_cache).unwrap();

        assert_eq!(cache.dirty_block_count(), 2);

        // Intentionally skip save_metadata() — WAL has entries, metadata does not.
        // Drop cache to simulate crash.
    }

    // Session 2: Reopen with full recovery (WAL replay + SSD re-read)
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Data should be readable from local SSD (pwrite'd before WAL append)
        let data_0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(data_0.as_ref(), &block_0_data[..], "block 0 data after recovery");

        let data_1 = cache.read_local(BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
        assert_eq!(data_1.as_ref(), &block_1_data[..], "block 1 data after recovery");

        // Flush to S3 — WAL-replayed blocks have FLAG_DIRTY in the v2 block map,
        // so flush_dirty_inner picks them up even though v1 block_states says Clean.
        let content_store = glidefs::nbd::content_store::ContentStore::new(
            Arc::clone(&s3_backend),
            "test",
        );
        let pack_index = glidefs::nbd::pack_index::HostPackIndex::new();
        let stats = cache.flush_to_s3(&content_store, &pack_index).await.unwrap();
        assert!(stats.packs_uploaded > 0, "should upload recovered blocks");
        assert_eq!(stats.blocks_flushed, 2, "should flush both WAL-recovered blocks");
    }

    // Session 3: Verify from a cold reader that the data made it to S3
    {
        let cold_dir = TempDir::new().unwrap();
        let (cold_cache, content_store, pack_index, clean_cache, metrics) =
            create_v2_cold_reader(&cold_dir, "wal_recovery", Arc::clone(&s3_backend)).await;

        let data_0 = cold_cache
            .read_v2(0, BLOCK_SIZE, clean_cache.as_ref(), &pack_index, &content_store, &metrics)
            .await
            .unwrap();
        assert_eq!(data_0.as_ref(), &block_0_data[..], "block 0 from S3 after WAL recovery");

        let data_1 = cold_cache
            .read_v2(BLOCK_SIZE as u64, BLOCK_SIZE, clean_cache.as_ref(), &pack_index, &content_store, &metrics)
            .await
            .unwrap();
        assert_eq!(data_1.as_ref(), &block_1_data[..], "block 1 from S3 after WAL recovery");
    }
}

/// Test: WAL recovery with multiple crash cycles preserves all data.
///
/// Scenario: Write block 0 in session 1 (save metadata, crash),
/// recover in session 2 and write block 1 (crash WITHOUT saving metadata),
/// recover in session 3 — both blocks should be intact and flushable.
///
/// This exercises the append-only WAL across multiple sessions: session 1's
/// entry persists through session 2's recovery (WAL is not truncated during
/// finish_recovery), and session 2's new entry appends. Session 3 replays all.
#[tokio::test]
async fn test_wal_recovery_multiple_crash_cycles() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "wal_multi_crash");

    let data_session_1: Vec<u8> = vec![0xAA; BLOCK_SIZE];
    let data_session_2: Vec<u8> = vec![0xBB; BLOCK_SIZE];

    // Session 1: Write block 0, save metadata, crash
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.skip_recovery_for_test();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);
        cache.write(0, &data_session_1, &clean_cache).unwrap();
        cache.save_metadata().unwrap();
        // Crash — .meta has block 0 dirty, WAL has block 0 entry
    }

    // Session 2: Recover, write block 1, crash WITHOUT saving metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        let clean_cache = SimpleBlockCache::new(64 * 1024 * 1024);

        // Block 0 should be recovered from metadata (.meta says dirty)
        assert!(cache.dirty_block_count() >= 1, "block 0 should be dirty from metadata");

        // Write block 1 (new data) — appends to WAL
        cache.write(BLOCK_SIZE as u64, &data_session_2, &clean_cache).unwrap();

        // Crash without saving metadata — .meta still has only block 0 dirty
    }

    // Session 3: Full recovery — both blocks should be present
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // SSD data should be intact for both blocks
        let read_0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert_eq!(read_0.as_ref(), &data_session_1[..], "block 0 from session 1");

        let read_1 = cache.read_local(BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
        assert_eq!(read_1.as_ref(), &data_session_2[..], "block 1 from session 2");

        // Both should flush to S3 — block 0 from metadata + WAL,
        // block 1 from WAL only (both have FLAG_DIRTY in v2 block map).
        let content_store = glidefs::nbd::content_store::ContentStore::new(
            Arc::clone(&s3_backend),
            "test",
        );
        let pack_index = glidefs::nbd::pack_index::HostPackIndex::new();
        let stats = cache.flush_to_s3(&content_store, &pack_index).await.unwrap();
        assert_eq!(stats.blocks_flushed, 2, "both blocks should flush to S3");

        // Verify from cold reader
        let cold_dir = TempDir::new().unwrap();
        let (cold_cache, cs, pi, cc, m) =
            create_v2_cold_reader(&cold_dir, "wal_multi_crash", Arc::clone(&s3_backend)).await;

        let s3_data_0 = cold_cache
            .read_v2(0, BLOCK_SIZE, cc.as_ref(), &pi, &cs, &m)
            .await
            .unwrap();
        assert_eq!(s3_data_0.as_ref(), &data_session_1[..], "block 0 in S3");

        let s3_data_1 = cold_cache
            .read_v2(BLOCK_SIZE as u64, BLOCK_SIZE, cc.as_ref(), &pi, &cs, &m)
            .await
            .unwrap();
        assert_eq!(s3_data_1.as_ref(), &data_session_2[..], "block 1 in S3");
    }
}
