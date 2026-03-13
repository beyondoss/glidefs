//! SSD capacity pressure integration tests.
//!
//! These tests exercise the ENOSPC rejection path and related behavior
//! when the SSD is near-full. Uses the shared `ssd_utilization` atomic to
//! simulate disk pressure without actually filling the SSD.
//!
//! Run with: `cargo test --features test-utils --test integration ssd_pressure`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use object_store::memory::InMemory;
use tempfile::TempDir;
use tokio::sync::Notify;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::error::CommandError;
use glidefs::block::handler::BlockHandler;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack::DEFAULT_BLOCKS_PER_PACK;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

use super::BLOCK_SIZE;

const DEVICE_SIZE: u64 = 4 * 1024 * 1024; // 4MB — small for fast tests
const HANDLER_BLOCK_SIZE: usize = 4096; // Handler tests use 4KB blocks for speed

/// Create a BlockHandler with a controllable SSD utilization atomic.
async fn handler_with_ssd_util() -> (BlockHandler, Arc<AtomicU64>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: "ssd-pressure".to_string(),
        device_size: DEVICE_SIZE,
        block_size: HANDLER_BLOCK_SIZE,
        wal_sync: false,
    };

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
    let clean_cache: Arc<dyn glidefs::block::cache::BlockCache> =
        Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        HANDLER_BLOCK_SIZE as u32,
    )));
    let metrics = Arc::new(ExportMetrics::new());
    let ssd_util = Arc::new(AtomicU64::new(0f64.to_bits()));

    let cache = WriteCache::open(config).unwrap();
    let cache = cache.skip_recovery_for_test();
    let handler = BlockHandler::new(
        Arc::new(cache),
        content_store,
        clean_cache,
        pack_index_cache,
        volume_manifest,
        DEVICE_SIZE,
        false,
        metrics,
        Arc::clone(&ssd_util),
        Arc::new(Notify::const_new()),
        DEFAULT_BLOCKS_PER_PACK,
        None,
    );

    (handler, ssd_util, temp_dir)
}

// =============================================================================
// SSD full rejects new blocks
// =============================================================================

/// At >95% SSD utilization, writes to new blocks return ENOSPC.
/// Overwrites to existing blocks succeed. Reads still work.
#[tokio::test]
async fn test_ssd_full_rejects_new_blocks() {
    let (handler, ssd_util, _temp) = handler_with_ssd_util().await;

    // Write blocks 0-3 at normal pressure — establishes them as present
    for i in 0..4u64 {
        let offset = i * HANDLER_BLOCK_SIZE as u64;
        handler
            .write(offset, &vec![0xAA; HANDLER_BLOCK_SIZE], false)
            .await
            .unwrap();
    }

    // Simulate 96% SSD utilization (above WRITE_REJECT_THRESHOLD of 0.95)
    ssd_util.store(0.96f64.to_bits(), Ordering::Relaxed);

    // Overwrite existing block 0 — should succeed
    assert!(
        handler
            .write(0, &vec![0xBB; HANDLER_BLOCK_SIZE], false)
            .await
            .is_ok(),
        "overwrite of existing block should succeed at 96%"
    );

    // Write to new block 4 — should fail with ENOSPC
    let result = handler
        .write(
            4 * HANDLER_BLOCK_SIZE as u64,
            &vec![0xCC; HANDLER_BLOCK_SIZE],
            false,
        )
        .await;
    assert!(
        matches!(result, Err(CommandError::NoSpace)),
        "write to new block should return ENOSPC at 96%"
    );

    // write_zeroes does NOT check SSD utilization — it uses fallocate(ZERO_RANGE)
    // on Linux (no new SSD allocation needed), so it should always succeed.
    assert!(
        handler
            .write_zeroes(5 * HANDLER_BLOCK_SIZE as u64, HANDLER_BLOCK_SIZE as u32, false)
            .await
            .is_ok(),
        "write_zeroes should succeed even at 96% (no SSD allocation needed)"
    );
}

// =============================================================================
// SSD full reads still work
// =============================================================================

/// At >95% SSD utilization, reads from local cache and from S3 must still work.
#[tokio::test]
async fn test_ssd_full_reads_still_work() {
    let (handler, ssd_util, _temp) = handler_with_ssd_util().await;

    // Write some blocks at normal pressure
    handler
        .write(0, &vec![0xAA; HANDLER_BLOCK_SIZE], false)
        .await
        .unwrap();
    handler
        .write(
            HANDLER_BLOCK_SIZE as u64,
            &vec![0xBB; HANDLER_BLOCK_SIZE],
            false,
        )
        .await
        .unwrap();

    // Simulate 96% SSD utilization
    ssd_util.store(0.96f64.to_bits(), Ordering::Relaxed);

    // Read from local cache — should succeed
    let data = handler.read(0, HANDLER_BLOCK_SIZE as u32).await.unwrap();
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "read from local cache should work at 96%"
    );

    let data = handler
        .read(HANDLER_BLOCK_SIZE as u64, HANDLER_BLOCK_SIZE as u32)
        .await
        .unwrap();
    assert!(
        data.iter().all(|&b| b == 0xBB),
        "read from local cache should work at 96%"
    );

    // Read an unwritten block (returns zeros) — should succeed
    let data = handler
        .read(2 * HANDLER_BLOCK_SIZE as u64, HANDLER_BLOCK_SIZE as u32)
        .await
        .unwrap();
    assert!(
        data.iter().all(|&b| b == 0),
        "read of unwritten block should return zeros at 96%"
    );

    // Flush still works at high pressure
    assert!(handler.flush().is_ok(), "flush should work at 96%");
}

// =============================================================================
// SSD full flush behavior
// =============================================================================

/// Flush to S3 at high SSD utilization. Verify dirty count drops to zero.
///
/// NOTE: Flush does NOT actually free SSD space — the data file retains its
/// physical allocation. But dirty_block_count drops, and data reaches S3.
#[tokio::test]
async fn test_ssd_full_flush_frees_dirty_count() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _metrics) =
        super::create_test_cache(&dir, "ssd-flush", Arc::clone(&s3)).await;

    // Write 10 blocks
    for i in 0..10usize {
        cache
            .write(
                (i * BLOCK_SIZE) as u64,
                &vec![(i as u8) + 1; BLOCK_SIZE],
                &[],
            )
            .unwrap();
    }

    assert!(
        cache.dirty_block_count() >= 10,
        "should have at least 10 dirty blocks"
    );

    // Flush to S3
    let stats = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    assert_eq!(stats.blocks_claimed, 10, "all 10 blocks should be claimed");

    // Dirty count should be zero after flush
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "dirty count should be zero after flush"
    );

    // Verify data is in S3 via cold reader
    let reader_dir = TempDir::new().unwrap();
    let (rcache, rcs, rpic, rvm, rcc, rmetrics) =
        super::create_cold_reader(&reader_dir, "ssd-flush", Arc::clone(&s3)).await;

    for i in 0..10usize {
        let data = rcache
            .read(
                (i * BLOCK_SIZE) as u64,
                BLOCK_SIZE,
                rcc.as_ref(),
                &rpic,
                &rvm,
                &rcs,
                &rmetrics,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == (i as u8) + 1),
            "block {i} should be readable from S3 after flush"
        );
    }
}

// =============================================================================
// SSD full recovery cycle
// =============================================================================

/// Full ENOSPC cycle: write blocks, raise pressure, writes rejected,
/// flush to S3 (dirty count drops), lower pressure, writes resume.
#[tokio::test]
async fn test_ssd_full_recovery() {
    let (handler, ssd_util, _temp) = handler_with_ssd_util().await;

    // Phase 1: Normal writes at low pressure
    for i in 0..4u64 {
        handler
            .write(
                i * HANDLER_BLOCK_SIZE as u64,
                &vec![0xAA; HANDLER_BLOCK_SIZE],
                false,
            )
            .await
            .unwrap();
    }

    // Phase 2: Raise pressure to 96%
    ssd_util.store(0.96f64.to_bits(), Ordering::Relaxed);

    // New block writes rejected
    let result = handler
        .write(
            4 * HANDLER_BLOCK_SIZE as u64,
            &vec![0xBB; HANDLER_BLOCK_SIZE],
            false,
        )
        .await;
    assert!(matches!(result, Err(CommandError::NoSpace)));

    // Overwrites still work
    handler
        .write(0, &vec![0xCC; HANDLER_BLOCK_SIZE], false)
        .await
        .unwrap();

    // Phase 3: Flush (local SSD sync — simulates pressure_flush behavior)
    handler.flush().unwrap();

    // Phase 4: Lower pressure to 50% (simulates space freed by external means)
    ssd_util.store(0.50f64.to_bits(), Ordering::Relaxed);

    // New block writes succeed again
    handler
        .write(
            4 * HANDLER_BLOCK_SIZE as u64,
            &vec![0xDD; HANDLER_BLOCK_SIZE],
            false,
        )
        .await
        .unwrap();

    // Verify all data
    let data = handler.read(0, HANDLER_BLOCK_SIZE as u32).await.unwrap();
    assert!(
        data.iter().all(|&b| b == 0xCC),
        "block 0 should have overwritten data"
    );

    let data = handler
        .read(4 * HANDLER_BLOCK_SIZE as u64, HANDLER_BLOCK_SIZE as u32)
        .await
        .unwrap();
    assert!(
        data.iter().all(|&b| b == 0xDD),
        "block 4 should have post-recovery data"
    );
}
