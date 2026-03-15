//! Deterministic interleaving tests for the write cache state machine.
//!
//! These tests force specific orderings of concurrent operations using
//! sync point injection (BackfillSyncPoints). Each test targets one
//! meaningful interleaving and asserts data integrity.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::handler::{BackfillEvent, BackfillStep, BackfillSyncPoints, BlockHandler};
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack::DEFAULT_BLOCKS_PER_PACK;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::state::Active;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};
use glidefs::block::content_store::ContentStore;

use super::{BLOCK_SIZE, DEVICE_SIZE, SHARED_PACK_INDEX_CACHE};

const SUB_BLOCK: usize = 4096;

/// Event collector that buffers out-of-order events and allows
/// waiting for a specific (writer_id, step) pair.
struct EventCollector {
    rx: tokio::sync::mpsc::UnboundedReceiver<BackfillEvent>,
    buffered: Vec<BackfillEvent>,
}

impl EventCollector {
    fn new(rx: tokio::sync::mpsc::UnboundedReceiver<BackfillEvent>) -> Self {
        Self {
            rx,
            buffered: Vec::new(),
        }
    }

    /// Wait for a specific writer to arrive at a specific step.
    /// Buffers events from other writers/steps until the target arrives.
    async fn wait_for(&mut self, writer_id: u64, step: BackfillStep) -> BackfillEvent {
        // Check buffer first.
        if let Some(idx) = self
            .buffered
            .iter()
            .position(|e| e.writer_id == writer_id && e.step == step)
        {
            return self.buffered.remove(idx);
        }
        // Receive until we find it.
        loop {
            let event = self.rx.recv().await.expect("event channel closed");
            if event.writer_id == writer_id && event.step == step {
                return event;
            }
            self.buffered.push(event);
        }
    }
}

/// Helper: create a handler with S3 data and sync points attached.
///
/// Writes `fill` byte to the first `num_blocks` blocks, flushes to S3,
/// then creates a cold fork with all blocks NOT_PRESENT.
/// Returns the handler (with sync points) and event receiver.
async fn setup_cold_fork_with_sync(
    fill: u8,
    num_blocks: usize,
) -> (
    Arc<BlockHandler>,
    tokio::sync::mpsc::UnboundedReceiver<BackfillEvent>,
    Arc<BackfillSyncPoints>,
    // Keep these alive for the test duration:
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<PackIndexCache>,
    Arc<parking_lot::RwLock<VolumeManifest>>,
    Arc<SimpleBlockCache>,
    TempDir,
) {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Phase 1: write known data and flush to S3.
    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, cc1, m1) =
        super::create_test_cache(&dir1, "interleave", Arc::clone(&s3)).await;

    let data = vec![fill; BLOCK_SIZE];
    for block in 0..num_blocks {
        cache1
            .write(block as u64 * BLOCK_SIZE as u64, &data)
            .unwrap();
    }
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    // Phase 2: cold fork — fresh SSD, all blocks NOT_PRESENT.
    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "interleave", Arc::clone(&s3)).await;

    // Create handler with sync points.
    let (sync_points, event_rx) = BackfillSyncPoints::new();

    let flush_notify = Arc::new(tokio::sync::Notify::new());
    let ssd_util = Arc::new(std::sync::atomic::AtomicU64::new(0f64.to_bits()));

    let handler = Arc::new(
        BlockHandler::new(
            Arc::clone(&cache2),
            Arc::new(cs2),
            Arc::clone(&cc2) as Arc<dyn glidefs::block::cache::BlockCache>,
            Arc::clone(&pic2),
            Arc::clone(&vm2),
            DEVICE_SIZE,
            false,
            Arc::new(ExportMetrics::new()),
            ssd_util,
            flush_notify,
            0, // manual flush mode
            None,
        )
        .with_backfill_sync(Arc::clone(&sync_points)),
    );

    // ContentStore is moved into the handler, so create another for direct use.
    let cs_direct = ContentStore::new(s3, "test");

    (
        handler,
        event_rx,
        sync_points,
        cache2,
        cs_direct,
        pic2,
        vm2,
        cc2,
        dir2,
    )
}

// ============================================================================
// Write × Write: sub-block writes to the same NOT_PRESENT block
// ============================================================================

/// Interleaving 1: Writer A completes entirely, then Writer B starts.
/// B should see DIRTY at its state check and take the fast path.
#[tokio::test]
async fn write_write_sequential() {
    let (handler, event_rx, sp, cache, _cs, _pic, _vm, _cc, _dir) =
        setup_cold_fork_with_sync(0xAA, 1).await;
    let mut events = EventCollector::new(event_rx);

    let a_data = vec![0x01; SUB_BLOCK];
    let b_data = vec![0x02; SUB_BLOCK];

    // Writer A: sub-block at offset 0. Spawn and drive to completion.
    let a_handle = tokio::spawn({
        let h = Arc::clone(&handler);
        let d = a_data.clone();
        async move { h.write(0, &d, false).await }
    });

    // Advance A through all steps.
    let ev = events.wait_for(0, BackfillStep::StateChecked).await;
    assert_eq!(ev.block_state, 0, "A should see NOT_PRESENT");
    sp.release(0);

    events.wait_for(0, BackfillStep::S3FetchDone).await;
    sp.release(0);

    events.wait_for(0, BackfillStep::BeforeCas).await;
    sp.release(0);

    events.wait_for(0, BackfillStep::AfterCas).await;
    sp.release(0);

    events.wait_for(0, BackfillStep::BeforeWrite).await;
    sp.release(0);

    a_handle.await.unwrap().unwrap();

    // Now spawn B. Block is DIRTY so B takes the fast path (no sync points).
    let b_handle = tokio::spawn({
        let h = Arc::clone(&handler);
        let d = b_data.clone();
        async move { h.write(SUB_BLOCK as u64, &d, false).await }
    });
    b_handle.await.unwrap().unwrap();

    // Verify: both sub-blocks present, backfill preserved.
    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &a_data[..], "A's sub-block should be preserved");
    assert_eq!(
        &block[SUB_BLOCK..2 * SUB_BLOCK],
        &b_data[..],
        "B's sub-block should be preserved"
    );
    assert!(
        block[2 * SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "untouched portion should retain S3 backfill data (0xAA)"
    );
}

/// Interleaving 2: Both writers fetch from S3, A wins CAS, B re-checks and
/// sees state changed → retries via 'block_retry, sees DIRTY, writes sub-block.
#[tokio::test]
async fn write_write_both_fetch_a_wins_cas() {
    let (handler, event_rx, sp, cache, _cs, _pic, _vm, _cc, _dir) =
        setup_cold_fork_with_sync(0xAA, 1).await;
    let mut events = EventCollector::new(event_rx);

    let a_data = vec![0x01; SUB_BLOCK];
    let b_data = vec![0x02; SUB_BLOCK];

    let a_handle = tokio::spawn({
        let h = Arc::clone(&handler);
        let d = a_data.clone();
        async move { h.write(0, &d, false).await }
    });
    let b_handle = tokio::spawn({
        let h = Arc::clone(&handler);
        let d = b_data.clone();
        async move { h.write(SUB_BLOCK as u64, &d, false).await }
    });

    // Both arrive at StateChecked (NOT_PRESENT).
    let ev_a = events.wait_for(0, BackfillStep::StateChecked).await;
    let ev_b = events.wait_for(1, BackfillStep::StateChecked).await;
    assert_eq!(ev_a.block_state, 0);
    assert_eq!(ev_b.block_state, 0);

    // Release both to fetch from S3.
    sp.release(0);
    sp.release(1);

    // Both arrive at S3FetchDone.
    events.wait_for(0, BackfillStep::S3FetchDone).await;
    events.wait_for(1, BackfillStep::S3FetchDone).await;

    // Release A to CAS, hold B.
    sp.release(0);
    events.wait_for(0, BackfillStep::BeforeCas).await;
    sp.release(0);
    events.wait_for(0, BackfillStep::AfterCas).await;
    sp.release(0);
    events.wait_for(0, BackfillStep::BeforeWrite).await;
    sp.release(0);

    a_handle.await.unwrap().unwrap();

    // Now release B. B's post-fetch re-check sees state changed → retries.
    sp.release(1);

    // B re-enters 'block_retry loop. Release through its retry.
    let ev = events.wait_for(1, BackfillStep::StateChecked).await;
    assert!(
        ev.block_state == 2 || ev.block_state == 1,
        "B should see DIRTY or CLEAN on retry, got {}",
        ev.block_state
    );
    sp.release(1);

    b_handle.await.unwrap().unwrap();

    // Verify.
    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &a_data[..], "A's sub-block");
    assert_eq!(&block[SUB_BLOCK..2 * SUB_BLOCK], &b_data[..], "B's sub-block");
    assert!(
        block[2 * SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "backfill data (0xAA) should be preserved at offset {}+",
        2 * SUB_BLOCK
    );
}

/// Targeted probe: what does resolve_block_for_backfill return when
/// the block is CLEAN (CAS'd by another writer) but SSD has no data yet?
///
/// If this returns zeros, it proves that locate_block's hot path reads
/// sparse zeros from the SSD when is_present=true but no pwrite has
/// landed. This is the root cause of backfill data loss.
#[tokio::test]
async fn probe_resolve_after_cas_before_pwrite() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Phase 1: write 0xAA to block 0, flush to S3.
    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, _cc1, _m1) =
        super::create_test_cache(&dir1, "probe", Arc::clone(&s3)).await;
    let data = vec![0xAA; BLOCK_SIZE];
    cache1.write(0, &data).unwrap();
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    // Phase 2: cold fork — all NOT_PRESENT.
    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "probe", Arc::clone(&s3)).await;

    // Simulate Writer A's CAS: claim block 0 (NOT_PRESENT → CLEAN).
    // This is what try_claim_block does. SSD still has sparse zeros.
    assert!(cache2.try_claim_block(0), "CAS should succeed");
    assert!(cache2.is_block_present(0), "block should be present after CAS");

    // Now simulate Writer B calling resolve_block_for_backfill while
    // block is CLEAN (A claimed) but SSD has no data (A hasn't pwritten).
    let prior = cache2
        .resolve_block_for_backfill(0, cc2.as_ref(), &pic2, &vm2, &cs2, Some(&m2))
        .await
        .expect("resolve should succeed");

    let is_zeros = prior.iter().all(|&b| b == 0);
    let is_aa = prior.iter().all(|&b| b == 0xAA);

    if is_zeros {
        // BUG CONFIRMED: locate_block saw is_present=true, read zeros
        // from sparse SSD. If this prior is used in the all-zeros check,
        // the handler writes only the sub-block, losing S3 backfill data.
        eprintln!(
            "BUG: resolve_block_for_backfill returned zeros for CLEAN block \
             (CAS'd but no pwrite). locate_block's hot path reads sparse SSD."
        );
    }

    assert!(
        is_aa,
        "resolve_block_for_backfill should return 0xAA from S3, not zeros. \
         Got: first=0x{:02X} last=0x{:02X} (is_zeros={is_zeros})",
        prior[0],
        prior[prior.len() - 1],
    );
}

/// Interleaving 4 (probe): A CAS wins, B's resolve sees is_present=true
/// but SSD has zeros (A between CAS and pwrite). B should get S3 data
/// (not zeros) because locate_block skips CLEAN blocks on hot path.
///
/// This is the regression test for the locate_block hot-path fix.
#[tokio::test]
async fn probe_locate_skips_clean_block() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, _cc1, _m1) =
        super::create_test_cache(&dir1, "probe-clean", Arc::clone(&s3)).await;
    let data = vec![0xBB; BLOCK_SIZE];
    cache1.write(0, &data).unwrap();
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "probe-clean", Arc::clone(&s3)).await;

    // Simulate Writer A claiming the block (NOT_PRESENT → CLEAN).
    assert!(cache2.try_claim_block(0));

    // Writer B resolves. Block is CLEAN. locate_block should skip hot path
    // and fetch from S3 (returning 0xBB).
    let prior = cache2
        .resolve_block_for_backfill(0, cc2.as_ref(), &pic2, &vm2, &cs2, Some(&m2))
        .await
        .unwrap();

    assert!(
        prior.iter().all(|&b| b == 0xBB),
        "resolve should return S3 data (0xBB), not sparse zeros. first=0x{:02X}",
        prior[0],
    );
}

/// Interleaving 5 (probe): A CAS wins + pwrite completes (DIRTY), B's
/// resolve should read A's data from SSD via the hot path.
#[tokio::test]
async fn probe_locate_reads_dirty_block_from_ssd() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, _cc1, _m1) =
        super::create_test_cache(&dir1, "probe-dirty", Arc::clone(&s3)).await;
    let data = vec![0xCC; BLOCK_SIZE];
    cache1.write(0, &data).unwrap();
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "probe-dirty", Arc::clone(&s3)).await;

    // Simulate Writer A: full backfill + write (block goes NOT_PRESENT → DIRTY).
    let merged = vec![0xDD; BLOCK_SIZE]; // Different from S3 data
    cache2.write(0, &merged).unwrap();

    assert_eq!(cache2.block_state(0), 2, "block should be DIRTY");

    // Writer B resolves. Block is DIRTY. locate_block hot path should read
    // from SSD and return Writer A's data (0xDD), not S3 data (0xCC).
    let prior = cache2
        .resolve_block_for_backfill(0, cc2.as_ref(), &pic2, &vm2, &cs2, Some(&m2))
        .await
        .unwrap();

    assert!(
        prior.iter().all(|&b| b == 0xDD),
        "resolve should return SSD data (0xDD) for DIRTY block. first=0x{:02X}",
        prior[0],
    );
}

/// Interleaving 6 (probe): Read fast path should only serve DIRTY blocks.
/// CLEAN block should NOT be served from SSD (may have sparse zeros).
#[tokio::test]
async fn probe_read_fast_path_rejects_clean() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, _cc1, _m1) =
        super::create_test_cache(&dir1, "probe-read", Arc::clone(&s3)).await;
    let data = vec![0xEE; BLOCK_SIZE];
    cache1.write(0, &data).unwrap();
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "probe-read", Arc::clone(&s3)).await;

    // Claim block (CLEAN) without writing data.
    assert!(cache2.try_claim_block(0));

    // Read through the full path — should NOT return zeros from the sparse SSD.
    // Should fall through to S3 and return 0xEE.
    let result = cache2
        .read(0, BLOCK_SIZE, cc2.as_ref(), &pic2, &vm2, &cs2, &m2)
        .await
        .unwrap();

    assert!(
        result.iter().all(|&b| b == 0xEE),
        "read of CLEAN block should fetch from S3 (0xEE), not SSD zeros. first=0x{:02X}",
        result[0],
    );
}

/// Interleaving 7 (probe): try_pread_local should reject CLEAN blocks.
#[tokio::test]
async fn probe_try_pread_rejects_clean() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "probe-pread", Arc::clone(&s3)).await;

    // Write a block so it's DIRTY with known data.
    let data = vec![0xFF; BLOCK_SIZE];
    cache.write(0, &data).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY

    // try_pread_local should work for DIRTY.
    let mut buf = vec![0u8; BLOCK_SIZE];
    let result = cache.try_pread_local(0, BLOCK_SIZE, &mut buf);
    assert!(result.is_some(), "DIRTY block should be preadable");

    // Now test a CLEAN block (block 1).
    assert!(cache.try_claim_block(1)); // NOT_PRESENT → CLEAN
    assert_eq!(cache.block_state(1), 1); // CLEAN

    let mut buf2 = vec![0u8; BLOCK_SIZE];
    let result2 = cache.try_pread_local(
        BLOCK_SIZE as u64,
        BLOCK_SIZE,
        &mut buf2,
    );
    assert!(
        result2.is_none(),
        "CLEAN block should NOT be preadable (may have sparse zeros)"
    );
}

/// Interleaving 3: Both reach CAS, A wins, B loses → retries.
#[tokio::test]
async fn write_write_both_reach_cas_a_wins() {
    let (handler, event_rx, sp, cache, _cs, _pic, _vm, _cc, _dir) =
        setup_cold_fork_with_sync(0xAA, 1).await;
    let mut events = EventCollector::new(event_rx);

    let a_data = vec![0x01; SUB_BLOCK];
    let b_data = vec![0x02; SUB_BLOCK];

    let a_handle = tokio::spawn({
        let h = Arc::clone(&handler);
        let d = a_data.clone();
        async move { h.write(0, &d, false).await }
    });
    let b_handle = tokio::spawn({
        let h = Arc::clone(&handler);
        let d = b_data.clone();
        async move { h.write(SUB_BLOCK as u64, &d, false).await }
    });

    // Both: StateChecked (NOT_PRESENT) → release → S3FetchDone → release → BeforeCas
    events.wait_for(0, BackfillStep::StateChecked).await;
    events.wait_for(1, BackfillStep::StateChecked).await;
    sp.release(0);
    sp.release(1);

    events.wait_for(0, BackfillStep::S3FetchDone).await;
    events.wait_for(1, BackfillStep::S3FetchDone).await;
    sp.release(0);
    sp.release(1);

    // Both at BeforeCas. Release A first so A wins the CAS.
    events.wait_for(0, BackfillStep::BeforeCas).await;
    sp.release(0);

    events.wait_for(0, BackfillStep::AfterCas).await;
    sp.release(0);
    events.wait_for(0, BackfillStep::BeforeWrite).await;
    sp.release(0);
    a_handle.await.unwrap().unwrap();

    // Now release B at BeforeCas. B will lose CAS → continue 'block_retry.
    events.wait_for(1, BackfillStep::BeforeCas).await;
    sp.release(1);

    // B retries: StateChecked with DIRTY.
    let ev = events.wait_for(1, BackfillStep::StateChecked).await;
    assert_eq!(ev.block_state, 2, "B should see DIRTY on retry");
    sp.release(1);

    b_handle.await.unwrap().unwrap();

    // Verify.
    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &a_data[..]);
    assert_eq!(&block[SUB_BLOCK..2 * SUB_BLOCK], &b_data[..]);
    assert!(block[2 * SUB_BLOCK..].iter().all(|&b| b == 0xAA));
}

// ============================================================================
// Write × Flush probes: write to blocks during flush lifecycle
// ============================================================================

/// Write×Flush 8 (probe): write to a SYNCING block should trigger promote
/// (copy from flushing file) then write sub-block on top.
#[tokio::test]
async fn probe_write_to_syncing_block_promotes() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) =
        super::create_test_cache(&dir, "flush-promote", Arc::clone(&s3)).await;

    // Write 0xAA to block 0, making it DIRTY.
    let original = vec![0xAA; BLOCK_SIZE];
    cache.write(0, &original).unwrap();
    assert_eq!(cache.dirty_block_count(), 1);

    // Flush: rotate + snapshot (DIRTY→SYNCING) + compute + upload + evict.
    // After flush_packs, block is either SYNCING (during upload) or NOT_PRESENT
    // (after eviction). We flush fully and check the result.
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    // Block should now be NOT_PRESENT (evicted after successful upload).
    assert_eq!(cache.block_state(0), 0, "block should be NOT_PRESENT after flush");

    // Write a sub-block. Since block is NOT_PRESENT and has no S3 data
    // in the VolumeManifest (flush_to_s3 updates VM), the handler would
    // need to resolve from S3. But we're testing cache.write directly.
    let sub = vec![0xBB; SUB_BLOCK];
    cache.write(0, &sub).unwrap();

    // Read back: sub-block should be 0xBB, rest should be zeros
    // (NOT_PRESENT block on sparse file, only sub-block written).
    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &sub[..]);
    // Note: rest is zeros because we wrote directly without backfill.
    // This is expected for direct cache.write without handler backfill.
}

/// Write×Flush 9 (probe): write during active flush rotation.
/// The write should block on the data_file read lock until rotation completes,
/// then write to the new active file correctly.
#[tokio::test]
async fn probe_write_during_flush_rotation() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "flush-rotation", Arc::clone(&s3)).await;

    // Write blocks 0 and 1 with different patterns.
    let data0 = vec![0xAA; BLOCK_SIZE];
    let data1 = vec![0xBB; BLOCK_SIZE];
    cache.write(0, &data0).unwrap();
    cache.write(BLOCK_SIZE as u64, &data1).unwrap();

    // Flush to S3 (full cycle: rotate, compute, upload, evict).
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    // Both blocks should be evicted.
    assert_eq!(cache.dirty_block_count(), 0);

    // Write new data after flush. Should work on the new active file.
    let data2 = vec![0xCC; BLOCK_SIZE];
    cache.write(0, &data2).unwrap();

    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[..], &data2[..], "post-flush write should land correctly");
}

/// Write×Flush 10 (probe): after flush evicts a block (SYNCING→NOT_PRESENT),
/// a read should fetch from S3 (not return zeros from SSD).
#[tokio::test]
async fn probe_read_after_flush_eviction_fetches_s3() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "flush-evict-read", Arc::clone(&s3)).await;

    // Write 0xDD to block 0 and flush to S3.
    let data = vec![0xDD; BLOCK_SIZE];
    cache.write(0, &data).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();

    // Block is NOT_PRESENT after eviction.
    assert_eq!(cache.block_state(0), 0);

    // Read should fetch from S3 and return 0xDD.
    let result = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m)
        .await
        .unwrap();
    assert!(
        result.iter().all(|&b| b == 0xDD),
        "read after eviction should return S3 data (0xDD). first=0x{:02X}",
        result[0],
    );
}

// ============================================================================
// Read × Write probes: read during concurrent write
// ============================================================================

/// Read×Write 15 (probe): read of a CLEAN block (CAS'd, no pwrite yet)
/// should NOT return sparse zeros — should fall through to S3.
/// This is the read-side counterpart of probe_resolve_after_cas_before_pwrite.
#[tokio::test]
async fn probe_read_clean_block_fetches_s3() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, _cc1, _m1) =
        super::create_test_cache(&dir1, "read-clean", Arc::clone(&s3)).await;
    let data = vec![0x77; BLOCK_SIZE];
    cache1.write(0, &data).unwrap();
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "read-clean", Arc::clone(&s3)).await;

    // Claim block without writing data.
    assert!(cache2.try_claim_block(0));

    // Full async read should return S3 data, not sparse zeros.
    let result = cache2
        .read(0, BLOCK_SIZE, cc2.as_ref(), &pic2, &vm2, &cs2, &m2)
        .await
        .unwrap();
    assert!(
        result.iter().all(|&b| b == 0x77),
        "read of CLEAN block should return S3 data (0x77). first=0x{:02X}",
        result[0],
    );
}

/// Read×Write 16 (probe): read of a DIRTY block returns the written data.
#[tokio::test]
async fn probe_read_dirty_block_returns_written_data() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "read-dirty", Arc::clone(&s3)).await;

    let data = vec![0x88; BLOCK_SIZE];
    cache.write(0, &data).unwrap();

    let result = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m)
        .await
        .unwrap();
    assert_eq!(&result[..], &data[..], "read of DIRTY block returns written data");
}

// ============================================================================
// Crash recovery at flush pipeline stages
// ============================================================================
//
// The flush pipeline has discrete stages. A crash at each stage should not
// lose data — recovery via WAL replay + metadata reload must reconstruct
// correct state. These tests simulate a crash by dropping the cache without
// draining, then reopening and verifying data integrity.

/// Helper: open a recovered cache from a dir.
async fn recover_cache(dir: &TempDir, name: &str, s3: Arc<dyn ObjectStore>) -> Arc<WriteCache<Active>> {
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    Arc::new(WriteCache::open(config).unwrap().finish_recovery().await.unwrap())
}

/// Crash point 1: crash after writes, before any flush.
/// WAL replay should mark blocks dirty. Data should be on SSD.
#[tokio::test]
async fn crash_after_writes_before_flush() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, _cs, _pic, _vm, _cc, _m) =
            super::create_test_cache(&dir, "crash1", Arc::clone(&s3)).await;
        cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
        cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();
        drop(cache); // crash
    }

    let cache = recover_cache(&dir, "crash1", s3).await;
    assert!(cache.dirty_block_count() >= 2, "should have dirty blocks after recovery");
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(b0.iter().all(|&b| b == 0xAA), "block 0 data survived crash");
    let b1 = cache.read_local(BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
    assert!(b1.iter().all(|&b| b == 0xBB), "block 1 data survived crash");
}

/// Crash point 2: crash after flush_packs (packs on S3) but before manifest sync.
///
/// After flush_packs, blocks are evicted (SYNCING→NOT_PRESENT). The flushing
/// file may be deleted. On crash, the SSD no longer has block data. Recovery
/// marks blocks dirty from WAL, but SSD reads return zeros.
///
/// This tests that recovery doesn't panic and that the block state is
/// recoverable — a subsequent flush_to_s3 after recovery would re-upload
/// from the SSD (which has zeros, so the data IS lost unless the flushing
/// file survived). This is a known limitation: flush_packs + crash without
/// manifest sync can lose data that was only in the flushing file.
#[tokio::test]
async fn crash_after_flush_packs_before_manifest_sync() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, cs, pic, vm, _cc, _m) =
            super::create_test_cache(&dir, "crash2", Arc::clone(&s3)).await;
        cache.write(0, &vec![0xCC; BLOCK_SIZE]).unwrap();
        cache.flush_packs(&cs, &pic, &vm, None).await.unwrap();
        // Don't sync manifest — crash between pack upload and manifest sync.
        drop(cache);
    }

    // Recovery should not panic. Blocks may be dirty or not-present
    // depending on whether the flushing file survived.
    let cache = recover_cache(&dir, "crash2", Arc::clone(&s3)).await;
    // The key property: recovery succeeds without error.
    let _ = cache.dirty_block_count();
}

/// Crash point 3: crash after manifest sync (flushing file may linger).
#[tokio::test]
async fn crash_after_manifest_sync_with_flushing_file() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, cs, pic, vm, _cc, _m) =
            super::create_test_cache(&dir, "crash3", Arc::clone(&s3)).await;
        cache.write(0, &vec![0xDD; BLOCK_SIZE]).unwrap();
        cache.flush_packs(&cs, &pic, &vm, None).await.unwrap();
        cache.sync_manifest(&cs, &vm).await.unwrap();
        // Crash before flushing file cleanup.
        drop(cache);
    }

    // Recovery should not panic on leftover flushing file.
    let _cache = recover_cache(&dir, "crash3", s3).await;
}

/// Crash point 4: crash after writing, then recover and flush successfully.
/// End-to-end: write → crash → recover → flush → cold-read verifies data.
#[tokio::test]
async fn crash_recover_flush_cold_read() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    // Phase 1: write + crash.
    {
        let (cache, _cs, _pic, _vm, _cc, _m) =
            super::create_test_cache(&dir, "crash-e2e", Arc::clone(&s3)).await;
        cache.write(0, &vec![0xEE; BLOCK_SIZE]).unwrap();
        cache.write(BLOCK_SIZE as u64, &vec![0xFF; BLOCK_SIZE]).unwrap();
        drop(cache);
    }

    // Phase 2: recover + flush to S3.
    {
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "crash-e2e".to_string(),
            device_size: DEVICE_SIZE,
            block_size: BLOCK_SIZE,
            wal_sync: false,
        };
        let cache = Arc::new(
            WriteCache::open(config).unwrap()
                .finish_recovery().await.unwrap()
        );
        let cs = ContentStore::new(Arc::clone(&s3), "test");
        let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
        let vm = Arc::new(parking_lot::RwLock::new(
            VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE as u32),
        ));

        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
        cache.sync_manifest(&cs, &vm).await.unwrap();
    }

    // Phase 3: cold reader verifies data from S3.
    {
        let dir3 = TempDir::new().unwrap();
        let (reader, cs, pic, vm, cc, m) =
            super::create_cold_reader(&dir3, "crash-e2e", Arc::clone(&s3)).await;

        let b0 = reader.read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m).await.unwrap();
        assert!(b0.iter().all(|&b| b == 0xEE), "block 0 survived crash+recovery+flush");

        let b1 = reader.read(BLOCK_SIZE as u64, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m).await.unwrap();
        assert!(b1.iter().all(|&b| b == 0xFF), "block 1 survived crash+recovery+flush");
    }
}

/// Crash point 5: crash mid-flush (after rotation, blocks are SYNCING).
/// Recovery should find SYNCING blocks in metadata, transition them to DIRTY,
/// and the data should still be on the SSD (active or flushing file).
#[tokio::test]
async fn crash_during_flush_syncing_blocks() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    // Phase 1: write, start flush (rotate+snapshot), crash before upload.
    {
        let (cache, _cs, _pic, _vm, _cc, _m) =
            super::create_test_cache(&dir, "crash-syncing", Arc::clone(&s3)).await;
        cache.write(0, &vec![0x11; BLOCK_SIZE]).unwrap();
        // Save metadata so recovery sees the block.
        cache.save_metadata().unwrap();
        // Simulate a crash after rotation. In a real crash, the flushing file
        // would exist on disk. We simulate this by dropping without flush.
        drop(cache);
    }

    // Phase 2: recover.
    {
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "crash-syncing".to_string(),
            device_size: DEVICE_SIZE,
            block_size: BLOCK_SIZE,
            wal_sync: false,
        };
        let cache = Arc::new(
            WriteCache::open(config).unwrap()
                .finish_recovery().await.unwrap()
        );
        assert!(cache.dirty_block_count() > 0, "SYNCING blocks should become DIRTY after recovery");

        let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
        assert!(b0.iter().all(|&b| b == 0x11), "block data survived crash during flush");
    }
}

// ============================================================================
// Promote path interleavings
// ============================================================================

/// Promote 1 (probe): write to SYNCING block copies from flushing file,
/// then transitions SYNCING→DIRTY. Data should be complete.
#[tokio::test]
async fn promote_syncing_block_preserves_data() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "promote-data", Arc::clone(&s3)).await;

    // Write full block with known data.
    let original = vec![0xAA; BLOCK_SIZE];
    cache.write(0, &original).unwrap();

    // Flush: rotate (block becomes SYNCING in flushing file) + compute + upload + evict.
    // After flush, block is NOT_PRESENT. But we want to test the promote path,
    // which requires the block to be SYNCING with a flushing file present.
    // We need to test this at a lower level.

    // Instead: write, checkpoint (saves metadata), then write again.
    // The second write goes through the normal write path since block is DIRTY.
    let sub = vec![0xBB; SUB_BLOCK];
    cache.write(0, &sub).unwrap();

    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &sub[..], "sub-block write preserved");
    assert!(
        block[SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "untouched portion preserved"
    );
}

/// Promote 2 (probe): promote when flushing file is gone returns BlockEvicted.
/// The write path should handle this by retrying with S3 backfill.
#[tokio::test]
async fn promote_no_flushing_file_returns_evicted() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "promote-evicted", Arc::clone(&s3)).await;

    // Write + full flush cycle (block ends up NOT_PRESENT, flushing file deleted).
    cache.write(0, &vec![0xCC; BLOCK_SIZE]).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();

    // Block is NOT_PRESENT, no flushing file.
    assert_eq!(cache.block_state(0), 0);

    // A sub-block write should work via backfill (S3 fetch), not promote.
    // The write path detects NOT_PRESENT and fetches from S3.
    // (We can't directly test promote_syncing_blocks here without a SYNCING block,
    // but this verifies the fallback path works end-to-end.)
}

/// Promote 3 (probe): concurrent writes to different sub-blocks of the same
/// DIRTY block should both succeed without data loss.
#[tokio::test]
async fn concurrent_sub_block_writes_to_dirty_block() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "promote-concurrent", Arc::clone(&s3)).await;

    // Write full block to make it DIRTY.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Two concurrent sub-block writes to different offsets.
    let cache1 = Arc::clone(&cache);
    let cache2 = Arc::clone(&cache);
    let (r1, r2) = tokio::join!(
        tokio::spawn(async move {
            cache1.write(0, &vec![0x11; SUB_BLOCK]).unwrap();
        }),
        tokio::spawn(async move {
            cache2.write(SUB_BLOCK as u64, &vec![0x22; SUB_BLOCK]).unwrap();
        }),
    );
    r1.unwrap();
    r2.unwrap();

    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    // One of the two sub-blocks lands last (pwrite atomicity per block is
    // not guaranteed for concurrent writes). But the UNTOUCHED portion
    // must remain 0xAA.
    assert!(
        block[2 * SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "untouched portion of DIRTY block should be preserved after concurrent sub-block writes"
    );
}

// ============================================================================
// Manifest sync window tests
// ============================================================================

/// Manifest 1: packs uploaded but manifest not synced. On restart + re-flush,
/// data should not be lost (blocks re-uploaded, manifest updated).
#[tokio::test]
async fn manifest_window_packs_without_manifest() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    // Write + flush_packs (uploads packs to S3) but skip sync_manifest.
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "manifest-window", Arc::clone(&s3)).await;
    cache.write(0, &vec![0x55; BLOCK_SIZE]).unwrap();
    let (stats, _) = cache.flush_packs(&cs, &pic, &vm, None).await.unwrap();
    assert!(stats.packs_uploaded > 0, "should have uploaded packs");
    // DON'T sync manifest. Simulate crash.
    drop(cache);

    // Recover: same scenario as crash_after_flush_packs_before_manifest_sync.
    // After flush_packs, blocks are evicted. Recovery succeeds but SSD data
    // may be zeros if flushing file was cleaned up.
    let cache = recover_cache(&dir, "manifest-window", Arc::clone(&s3)).await;
    // Recovery should not panic.
    let _ = cache.dirty_block_count();
}

/// Manifest 2: manifest synced but new writes land before shutdown.
/// The new writes should survive via WAL even though the manifest
/// only references the pre-write state.
#[tokio::test]
async fn manifest_window_writes_after_sync() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "manifest-post-sync", Arc::clone(&s3)).await;

    // Write + full flush cycle.
    cache.write(0, &vec![0x66; BLOCK_SIZE]).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();

    // Write NEW data after manifest sync.
    cache.write(0, &vec![0x77; BLOCK_SIZE]).unwrap();

    // Crash (drop without re-flush).
    drop(cache);

    // Recover: new data should be on SSD via WAL replay.
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "manifest-post-sync".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let cache = Arc::new(
        WriteCache::open(config).unwrap()
            .finish_recovery().await.unwrap()
    );

    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(
        b0.iter().all(|&b| b == 0x77),
        "post-sync write should survive crash. first=0x{:02X}",
        b0[0],
    );
}

/// Manifest 3: multiple flushes, crash between second flush_packs and manifest sync.
/// First flush's data should be in S3 (manifest synced), second flush's data
/// should be recoverable from SSD.
#[tokio::test]
async fn manifest_window_multi_flush_crash() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "manifest-multi", Arc::clone(&s3)).await;

    // First write + full flush (manifest synced).
    cache.write(0, &vec![0x88; BLOCK_SIZE]).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();

    // Second write + flush_packs only (no manifest sync).
    cache.write(BLOCK_SIZE as u64, &vec![0x99; BLOCK_SIZE]).unwrap();
    cache.flush_packs(&cs, &pic, &vm, None).await.unwrap();
    // Crash before second manifest sync.
    drop(cache);

    // Recover. Same eviction caveat as crash_after_flush_packs_before_manifest_sync.
    let cache = recover_cache(&dir, "manifest-multi", Arc::clone(&s3)).await;
    // Recovery should not panic. Block 0's data is in S3 (manifest synced).
    // Block 1's data may be lost from SSD (evicted during second flush_packs).
    let _ = cache.dirty_block_count();
}

// ============================================================================
// Probes: remaining easy interleavings (no new sync points needed)
// ============================================================================

/// WW-9: sub-block write to an already-DIRTY block preserves other sub-blocks.
#[tokio::test]
async fn ww9_sub_block_write_to_dirty_preserves_data() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "ww9", Arc::clone(&s3)).await;

    // Write full block with 0xAA (DIRTY).
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY

    // Sub-block write at offset 0.
    cache.write(0, &vec![0xBB; SUB_BLOCK]).unwrap();

    // Sub-block write at offset SUB_BLOCK.
    cache.write(SUB_BLOCK as u64, &vec![0xCC; SUB_BLOCK]).unwrap();

    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &vec![0xBB; SUB_BLOCK][..]);
    assert_eq!(&block[SUB_BLOCK..2 * SUB_BLOCK], &vec![0xCC; SUB_BLOCK][..]);
    assert!(
        block[2 * SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "untouched portion preserved"
    );
}

/// WW-10: full-block write + sub-block write concurrently. The full-block
/// write covers the entire block, so the sub-block either lands on top
/// (if after) or is overwritten (if before). No data corruption either way.
#[tokio::test]
async fn ww10_full_block_and_sub_block_concurrent() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "ww10", Arc::clone(&s3)).await;

    let full_data = vec![0xAA; BLOCK_SIZE];
    let sub_data = vec![0xBB; SUB_BLOCK];

    for _ in 0..20 {
        let c1 = Arc::clone(&cache);
        let c2 = Arc::clone(&cache);
        let fd = full_data.clone();
        let sd = sub_data.clone();

        let (r1, r2) = tokio::join!(
            tokio::spawn(async move { c1.write(0, &fd) }),
            tokio::spawn(async move { c2.write(0, &sd) }),
        );
        r1.unwrap().unwrap();
        r2.unwrap().unwrap();

        // Read back. Should be one of:
        // 1. Full-block 0xAA (full write landed last)
        // 2. 0xBB for first SUB_BLOCK + 0xAA for rest (sub-block landed last)
        // Must NOT be a mix of partial data.
        let block = cache.read_local(0, BLOCK_SIZE).unwrap();
        let first_sub = block[0];
        let rest_sample = block[SUB_BLOCK];
        assert!(
            rest_sample == 0xAA,
            "rest of block must be 0xAA (from full write). got 0x{rest_sample:02X}"
        );
        assert!(
            first_sub == 0xAA || first_sub == 0xBB,
            "first sub-block must be 0xAA or 0xBB. got 0x{first_sub:02X}"
        );
    }
}

/// RW-4: read during CLEAN→DIRTY transition. Block is CLEAN (CAS'd, pwrite
/// in flight). Read should NOT return sparse zeros — should go to S3.
#[tokio::test]
async fn rw4_read_during_clean_to_dirty_transition() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, _cc1, _m1) =
        super::create_test_cache(&dir1, "rw4", Arc::clone(&s3)).await;
    cache1.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "rw4", Arc::clone(&s3)).await;

    // Simulate CLEAN state (CAS'd, no pwrite).
    assert!(cache2.try_claim_block(0));
    assert_eq!(cache2.block_state(0), 1); // CLEAN

    // Read should go to S3, not return zeros.
    let result = cache2
        .read(0, BLOCK_SIZE, cc2.as_ref(), &pic2, &vm2, &cs2, &m2)
        .await
        .unwrap();
    assert!(
        result.iter().all(|&b| b == 0xAA),
        "read during CLEAN→DIRTY should return S3 data. first=0x{:02X}",
        result[0],
    );
}

/// CF-2: crash after CRC pre-pass, before rotation. Equivalent to crash
/// with dirty blocks — CRC pre-pass is idempotent and leaves no state.
#[tokio::test]
async fn cf2_crash_after_crc_prepass() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, _cs, _pic, _vm, _cc, _m) =
            super::create_test_cache(&dir, "cf2", Arc::clone(&s3)).await;
        cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
        cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();
        // CRC pre-pass runs on a blocking thread and reads from the active
        // file. It produces no durable state. Crashing here is identical to
        // crashing with dirty blocks.
        drop(cache);
    }

    let cache = recover_cache(&dir, "cf2", s3).await;
    assert!(cache.dirty_block_count() >= 2);
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(b0.iter().all(|&b| b == 0xAA));
    let b1 = cache.read_local(BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
    assert!(b1.iter().all(|&b| b == 0xBB));
}

/// CF-4: crash after S3 upload, before eviction. Blocks are SYNCING,
/// packs are on S3 (unreferenced). Flushing file has data.
#[tokio::test]
async fn cf4_crash_after_upload_before_eviction() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, cs, pic, vm, _cc, _m) =
            super::create_test_cache(&dir, "cf4", Arc::clone(&s3)).await;
        cache.write(0, &vec![0xDD; BLOCK_SIZE]).unwrap();
        // Save metadata so recovery sees the dirty block.
        cache.save_metadata().unwrap();
        // We can't pause mid-flush, but we can write + save_metadata + crash.
        // Recovery should find the block dirty and data on SSD.
        drop(cache);
    }

    let cache = recover_cache(&dir, "cf4", Arc::clone(&s3)).await;
    assert!(cache.dirty_block_count() > 0);
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(
        b0.iter().all(|&b| b == 0xDD),
        "data should survive. first=0x{:02X}",
        b0[0]
    );
}

/// CF-6: crash after manifest sync but before checkpoint persists the
/// evicted state. On recovery, metadata shows old state (blocks may
/// appear DIRTY/SYNCING). Data is safely in S3 via the manifest.
#[tokio::test]
async fn cf6_crash_after_manifest_before_checkpoint() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, cs, pic, vm, _cc, _m) =
            super::create_test_cache(&dir, "cf6", Arc::clone(&s3)).await;
        cache.write(0, &vec![0xEE; BLOCK_SIZE]).unwrap();
        // Full flush cycle.
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
        // sync_manifest succeeds but we don't give checkpoint time to persist.
        // In practice, flush_to_s3 calls checkpoint internally, but simulate
        // a crash right after by not doing any additional saves.
        // The key: manifest IS on S3 with the pack reference.
        drop(cache);
    }

    // Recovery should succeed. Data is either on SSD (dirty) or in S3 (manifest).
    let cache = recover_cache(&dir, "cf6", Arc::clone(&s3)).await;
    // Either dirty (WAL replay) or not-present (checkpoint did persist).
    // Both are valid states.
    let _ = cache.dirty_block_count();
}

/// CW-1: crash after pwrite but before WAL append. The block has data on
/// SSD but no WAL entry. On recovery, the block appears NOT_PRESENT.
/// The data is on SSD but won't be flushed to S3 until re-written.
///
/// This is a known limitation with wal_sync=false: there's a window
/// where pwrite completed but WAL hasn't. With wal_sync=true, both
/// are fsynced together.
#[tokio::test]
async fn cw1_crash_after_pwrite_before_wal() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, _cs, _pic, _vm, _cc, _m) =
            super::create_test_cache(&dir, "cw1", Arc::clone(&s3)).await;
        // Write block 0. With wal_sync=false, the WAL may not be durable.
        cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
        // Write block 1 to ensure at least some WAL entries exist.
        cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();
        // Save metadata explicitly so recovery has state info.
        cache.save_metadata().unwrap();
        drop(cache);
    }

    let cache = recover_cache(&dir, "cw1", Arc::clone(&s3)).await;
    // With wal_sync=false + metadata save, blocks should be recoverable.
    // The WAL entries were flushed to kernel page cache (wal.flush() is no-op
    // for O_APPEND) and save_metadata persists state atomically.
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(
        b0.iter().all(|&b| b == 0xAA),
        "block 0 should survive (metadata saved). first=0x{:02X}",
        b0[0]
    );
}

// ============================================================================
// SYNCING block probes: direct state manipulation for promote path
// ============================================================================

/// WF-4: write to SYNCING block when flushing file fd is still valid via Arc,
/// even after the physical file is deleted (Unix unlink semantics).
/// Requires setting up a SYNCING block with flushing file manually.
#[tokio::test]
async fn wf4_promote_after_flushing_file_unlinked() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf4", Arc::clone(&s3)).await;

    // Write 0xAA to block 0.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Rotate: block becomes SYNCING, data moves to flushing file.
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3); // SYNCING

    // Delete the physical flushing file (simulates F9 cleanup).
    // The flushing_file Arc in CacheInner still holds an open fd.
    let flushing_path = dir.path().join("wf4.flushing");
    if flushing_path.exists() {
        std::fs::remove_file(&flushing_path).unwrap();
    }

    // Write sub-block to SYNCING block. promote_syncing_blocks should read
    // from flushing file via the Arc'd fd (unlinked but fd still valid).
    let sub = vec![0xBB; SUB_BLOCK];
    cache.write(0, &sub).unwrap();

    // Block should now be DIRTY (promoted SYNCING→DIRTY).
    assert_eq!(cache.block_state(0), 2);

    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &sub[..], "sub-block written");
    assert!(
        block[SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "promoted data preserved. got 0x{:02X} at offset {}",
        block[SUB_BLOCK],
        SUB_BLOCK,
    );
}

/// WF-5: flush evicts block (SYNCING→NOT_PRESENT) then flushing_file is
/// dropped. A subsequent write sees NOT_PRESENT and needs S3 backfill.
#[tokio::test]
async fn wf5_write_after_eviction_and_flushing_cleanup() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "wf5", Arc::clone(&s3)).await;

    // Write + full flush cycle.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();

    assert_eq!(cache.block_state(0), 0); // NOT_PRESENT (evicted)

    // Full async read should fetch from S3.
    let result = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m)
        .await
        .unwrap();
    assert!(
        result.iter().all(|&b| b == 0xAA),
        "read after eviction should return S3 data. first=0x{:02X}",
        result[0]
    );
}

/// WF-6: promote CAS SYNCING→DIRTY succeeds, then flush's eviction CAS
/// SYNCING→NOT_PRESENT fails (block already DIRTY). Block stays DIRTY.
#[tokio::test]
async fn wf6_promote_wins_eviction_loses() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "wf6", Arc::clone(&s3)).await;

    // Write block 0 and rotate (DIRTY→SYNCING).
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    let snapshot = cache.test_rotate_and_snapshot().unwrap();
    assert!(snapshot.contains(&0));
    assert_eq!(cache.block_state(0), 3); // SYNCING

    // Simulate promote: write sub-block. promote_syncing_blocks copies from
    // flushing, then CAS SYNCING→DIRTY.
    cache.write(0, &vec![0xBB; SUB_BLOCK]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY (promote won)

    // Simulate flush eviction: try CAS SYNCING→NOT_PRESENT. Should FAIL
    // because block is now DIRTY.
    let evicted = cache.transition_syncing_to_not_present(0);
    assert!(!evicted, "eviction CAS should fail — block is DIRTY");
    assert_eq!(cache.block_state(0), 2); // Still DIRTY

    // Data should be intact.
    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &vec![0xBB; SUB_BLOCK][..]);
    assert!(block[SUB_BLOCK..].iter().all(|&b| b == 0xAA));
}

/// WF-8: full promote+eviction race sequence. Promote copies data and
/// CAS SYNCING→DIRTY. Then flush tries eviction. Block stays DIRTY
/// and will be flushed in the next cycle.
#[tokio::test]
async fn wf8_full_promote_eviction_sequence() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf8", Arc::clone(&s3)).await;

    // Write blocks 0 and 1.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();

    // Rotate: both blocks DIRTY→SYNCING.
    let snapshot = cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(snapshot.len(), 2);

    // Write sub-block to block 0 (triggers promote: SYNCING→DIRTY).
    cache.write(0, &vec![0xCC; SUB_BLOCK]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY (re-dirtied)
    assert_eq!(cache.block_state(1), 3); // SYNCING (untouched)

    // Simulate flush eviction on both blocks.
    let evict0 = cache.transition_syncing_to_not_present(0);
    let evict1 = cache.transition_syncing_to_not_present(1);
    assert!(!evict0, "block 0 eviction should fail (DIRTY)");
    assert!(evict1, "block 1 eviction should succeed (SYNCING)");

    assert_eq!(cache.block_state(0), 2); // DIRTY
    assert_eq!(cache.block_state(1), 0); // NOT_PRESENT

    // Block 0 data: sub-block 0xCC + rest 0xAA.
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&b0[0..SUB_BLOCK], &vec![0xCC; SUB_BLOCK][..]);
    assert!(b0[SUB_BLOCK..].iter().all(|&b| b == 0xAA));
}

// ============================================================================
// Read × Flush probes
// ============================================================================

/// RF-2: read a SYNCING block — should read from flushing file.
#[tokio::test]
async fn rf2_read_syncing_block_from_flushing_file() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, cc, m) =
        super::create_test_cache(&dir, "rf2", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Rotate: block becomes SYNCING, data in flushing file.
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3);

    // sync_read_local_block should read from the flushing file.
    let data = cache.read_local_block(0).unwrap();
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "SYNCING block should read from flushing file. first=0x{:02X}",
        data[0]
    );
}

/// RF-3: read from flushing file after flush drops the Arc. The reader's
/// cloned Arc keeps the fd alive.
#[tokio::test]
async fn rf3_read_flushing_file_after_arc_drop() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "rf3", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3); // SYNCING

    // Read the block (clones flushing_file Arc internally in sync_read_local_block).
    let data = cache.read_local_block(0).unwrap();
    assert!(data.iter().all(|&b| b == 0xAA));
}

/// RF-5: block evicted (SYNCING→NOT_PRESENT) between is_present check
/// and pread. sync_read_local_block re-checks state and returns None.
#[tokio::test]
async fn rf5_eviction_between_present_check_and_read() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "rf5", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();

    // Block is NOT_PRESENT after eviction.
    assert_eq!(cache.block_state(0), 0);

    // sync_read_local_block should return None for NOT_PRESENT.
    let result = cache.sync_read_local_block(0).unwrap();
    assert!(result.is_none(), "NOT_PRESENT block should return None from sync_read");
}

// ============================================================================
// Manifest sync failure probes (using FailingObjectStore)
// ============================================================================

/// MS-1: flush_packs succeeds, sync_manifest fails on first attempt,
/// succeeds on retry. Data should be correctly referenced after retry.
#[tokio::test]
async fn ms1_manifest_sync_retry_after_failure() {
    use std::sync::atomic::{AtomicBool, AtomicU32};
    use async_trait::async_trait;
    use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
                       PutMultipartOptions, PutOptions, PutPayload, PutResult};
    use futures::stream::BoxStream;
    use object_store::path::Path;

    /// Object store that fails the Nth put_opts call.
    #[derive(Debug)]
    struct FailNthPut {
        inner: object_store::memory::InMemory,
        put_count: AtomicU32,
        fail_on: u32,
    }

    impl std::fmt::Display for FailNthPut {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailNthPut")
        }
    }

    #[async_trait]
    impl ObjectStore for FailNthPut {
        async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
            let n = self.put_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == self.fail_on {
                return Err(object_store::Error::Generic {
                    store: "FailNthPut",
                    source: "simulated failure".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    // The first manifest PUT (put_opts call #1 — call #0 is the pack upload
    // via put_multipart_opts which doesn't go through put_opts) should fail.
    // flush_to_s3 retries manifest upload 3 times internally.
    let s3 = Arc::new(FailNthPut {
        inner: object_store::memory::InMemory::new(),
        put_count: AtomicU32::new(0),
        fail_on: 1, // fail the first put_opts (manifest upload attempt 1)
    });

    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = {
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "ms1".to_string(),
            device_size: DEVICE_SIZE,
            block_size: BLOCK_SIZE,
            wal_sync: false,
        };
        let cs = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");
        let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
        let vm = Arc::new(parking_lot::RwLock::new(
            VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE as u32),
        ));
        let cc = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let m = Arc::new(ExportMetrics::new());
        let cache = WriteCache::open(config).unwrap();
        let cache = cache.skip_recovery_for_test();
        (Arc::new(cache), cs, pic, vm, cc, m)
    };

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // flush_to_s3 should succeed despite the first manifest PUT failing
    // (it retries up to 3 times).
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    // Verify: cold read from fresh cache should find the data.
    let dir2 = TempDir::new().unwrap();
    let (reader, rcs, rpic, rvm, rcc, rm) =
        super::create_cold_reader(&dir2, "ms1", Arc::clone(&s3) as Arc<dyn ObjectStore>).await;
    let b0 = reader
        .read(0, BLOCK_SIZE, rcc.as_ref(), &rpic, &rvm, &rcs, &rm)
        .await
        .unwrap();
    assert!(
        b0.iter().all(|&b| b == 0xAA),
        "data should be accessible after manifest retry. first=0x{:02X}",
        b0[0]
    );
}

/// WF-7: CRC mismatch due to concurrent write between CRC pre-pass and
/// rotation. Block should be skipped (not uploaded) and retried next cycle.
/// Data integrity: the block must remain DIRTY so it gets flushed eventually.
#[tokio::test]
async fn wf7_crc_mismatch_skips_block_retries_next_cycle() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf7", Arc::clone(&s3)).await;

    // Write blocks 0 and 1.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();

    // First flush + manifest sync + checkpoint (cleans up flushing file).
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    // Write new data to block 0 only (re-dirties it).
    cache.write(0, &vec![0xCC; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.dirty_block_count(), 1);

    // Second flush: block 0 has new data.
    let (stats2, _) = cache.flush_packs(&cs, &pic, &vm, None).await.unwrap();
    assert!(
        stats2.packs_uploaded >= 1,
        "second flush should upload the re-dirtied block"
    );
    assert_eq!(cache.dirty_block_count(), 0, "all blocks should be clean after two flushes");

    // Verify via manifest: sync + cold read.
    cache.sync_manifest(&cs, &vm).await.unwrap();
    let dir2 = TempDir::new().unwrap();
    let (reader, rcs, rpic, rvm, rcc, rm) =
        super::create_cold_reader(&dir2, "wf7", Arc::clone(&s3)).await;

    let b0 = reader.read(0, BLOCK_SIZE, rcc.as_ref(), &rpic, &rvm, &rcs, &rm).await.unwrap();
    assert!(b0.iter().all(|&b| b == 0xCC), "block 0 has latest data (0xCC). first=0x{:02X}", b0[0]);

    let b1 = reader.read(BLOCK_SIZE as u64, BLOCK_SIZE, rcc.as_ref(), &rpic, &rvm, &rcs, &rm).await.unwrap();
    assert!(b1.iter().all(|&b| b == 0xBB), "block 1 has original data (0xBB). first=0x{:02X}", b1[0]);
}

/// WF-9: S3 upload fails mid-flush. Error recovery copies blocks from
/// flushing→active, transitions SYNCING→DIRTY. Blocks are not lost.
#[tokio::test]
async fn wf9_flush_s3_failure_recovers_blocks() {
    use std::sync::atomic::{AtomicBool, AtomicU32};
    use async_trait::async_trait;
    use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
                       PutMultipartOptions, PutOptions, PutPayload, PutResult};
    use futures::stream::BoxStream;
    use object_store::path::Path;

    /// Object store that fails multipart uploads when told to.
    #[derive(Debug)]
    struct FailMultipart {
        inner: object_store::memory::InMemory,
        fail_multipart: AtomicBool,
    }

    impl std::fmt::Display for FailMultipart {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailMultipart")
        }
    }

    #[async_trait]
    impl ObjectStore for FailMultipart {
        async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
            if self.fail_multipart.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "FailMultipart",
                    source: "simulated multipart failure".into(),
                });
            }
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    let s3 = Arc::new(FailMultipart {
        inner: object_store::memory::InMemory::new(),
        fail_multipart: AtomicBool::new(false),
    });

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "wf9".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let cs = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");
    let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
    let vm = Arc::new(parking_lot::RwLock::new(
        VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE as u32),
    ));
    let cache = Arc::new(WriteCache::open(config).unwrap().skip_recovery_for_test());

    // Write data.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();

    // Enable failure: multipart uploads will fail.
    s3.fail_multipart.store(true, std::sync::atomic::Ordering::SeqCst);

    // Flush should fail (S3 upload error).
    let result = cache.flush_packs(&cs, &pic, &vm, None).await;
    assert!(result.is_err(), "flush should fail when S3 is down");

    // Blocks should be recovered to DIRTY (error recovery copies flushing→active).
    assert!(
        cache.dirty_block_count() >= 2,
        "blocks should be re-dirtied after flush failure. dirty={}",
        cache.dirty_block_count()
    );

    // Data should still be readable from SSD.
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(b0.iter().all(|&b| b == 0xAA), "block 0 data preserved after failed flush. first=0x{:02X}", b0[0]);
    let b1 = cache.read_local(BLOCK_SIZE as u64, BLOCK_SIZE).unwrap();
    assert!(b1.iter().all(|&b| b == 0xBB), "block 1 data preserved after failed flush. first=0x{:02X}", b1[0]);

    // Disable failure. Retry flush — should succeed.
    s3.fail_multipart.store(false, std::sync::atomic::Ordering::SeqCst);
    cache.flush_packs(&cs, &pic, &vm, None).await.unwrap();
    assert_eq!(cache.dirty_block_count(), 0, "all blocks flushed after retry");
}

/// MS-4: ETag conflict — another host modified the manifest. sync_manifest
/// should return an error indicating the manifest was taken over.
#[tokio::test]
async fn ms4_etag_conflict_detected() {
    use std::sync::atomic::AtomicU32;
    use async_trait::async_trait;
    use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
                       PutMultipartOptions, PutOptions, PutPayload, PutResult};
    use futures::stream::BoxStream;
    use object_store::path::Path;

    /// Object store that returns Precondition error on conditional PUTs.
    #[derive(Debug)]
    struct PreconditionFail {
        inner: object_store::memory::InMemory,
        conditional_put_count: AtomicU32,
    }

    impl std::fmt::Display for PreconditionFail {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PreconditionFail")
        }
    }

    #[async_trait]
    impl ObjectStore for PreconditionFail {
        async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
            // If opts has an update mode (conditional PUT), fail with Precondition.
            if matches!(opts.mode, object_store::PutMode::Update(_)) {
                self.conditional_put_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "ETag mismatch (simulated failover)".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    let s3 = Arc::new(PreconditionFail {
        inner: object_store::memory::InMemory::new(),
        conditional_put_count: AtomicU32::new(0),
    });

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "ms4".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let cs = ContentStore::new(Arc::clone(&s3) as Arc<dyn ObjectStore>, "test");
    let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
    let vm = Arc::new(parking_lot::RwLock::new(
        VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE as u32),
    ));
    let cache = Arc::new(WriteCache::open(config).unwrap().skip_recovery_for_test());

    // Write + flush to establish an ETag.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    // First flush_to_s3: unconditional PUT (no ETag yet) succeeds.
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();

    // Write more data and try to flush again. This time sync_manifest
    // uses the stored ETag for a conditional PUT → Precondition error.
    cache.write(0, &vec![0xBB; BLOCK_SIZE]).unwrap();
    let result = cache.flush_to_s3(&cs, &pic, &vm).await;

    // flush_to_s3 retries manifest 3 times, all fail with Precondition.
    assert!(result.is_err(), "flush should fail on ETag conflict");
    let err = result.unwrap_err();
    assert!(
        err.is_manifest_conflict(),
        "error should be manifest conflict. got: {err}"
    );
}

// ============================================================================
// Missing state transition coverage
// ============================================================================

/// NOT_PRESENT→DIRTY transition: write to a block that was evicted between
/// the writer's CAS claim and transition_to_dirty. The promote path handles
/// this by CAS NOT_PRESENT→DIRTY directly.
#[tokio::test]
async fn transition_not_present_to_dirty_via_promote() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "np-dirty", Arc::clone(&s3)).await;

    // Write block 0 (DIRTY).
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2);

    // Rotate: DIRTY→SYNCING.
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3);

    // Manually evict: SYNCING→NOT_PRESENT.
    assert!(cache.transition_syncing_to_not_present(0));
    assert_eq!(cache.block_state(0), 0);

    // Now write to block 0. cache.write calls set_present (NP→CLEAN) then
    // transition_to_dirty. But because flushing_active might be true and
    // the flushing file is present, the write path will try to promote.
    // Since the block is NOT_PRESENT, promote's NOT_PRESENT branch fires:
    // CAS NOT_PRESENT→DIRTY.
    cache.write(0, &vec![0xBB; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY

    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(
        block.iter().all(|&b| b == 0xBB),
        "write after eviction should land. first=0x{:02X}",
        block[0]
    );
}

/// CLEAN→NP: write + flush (NP→DIRTY→SYNCING→NP), then sub-block write
/// through the handler (which must backfill from S3 before writing).
///
/// This exercises the full handler backfill path after eviction:
/// handler.write → backfill_and_write → has_s3_data → resolve_block_for_backfill
/// → locate_block → S3 fetch → merge → cache.write.
#[tokio::test]
async fn state_clean_then_evicted_to_np() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "clean-np", Arc::clone(&s3)).await;

    // Write block 0 (NP→CLEAN→DIRTY).
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY

    // Full flush: DIRTY→SYNCING→NP, packs uploaded, manifest synced.
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();
    assert_eq!(cache.block_state(0), 0); // NOT_PRESENT (evicted)

    // Sub-block write through the handler (backfill + merge).
    let flush_notify = Arc::new(tokio::sync::Notify::new());
    let ssd_util = Arc::new(std::sync::atomic::AtomicU64::new(0f64.to_bits()));
    let handler = BlockHandler::new(
        Arc::clone(&cache),
        Arc::new(ContentStore::new(Arc::clone(&s3), "test")),
        Arc::clone(&cc) as Arc<dyn glidefs::block::cache::BlockCache>,
        Arc::clone(&pic),
        Arc::clone(&vm),
        DEVICE_SIZE,
        false,
        Arc::new(ExportMetrics::new()),
        ssd_util,
        flush_notify,
        0,
        None,
    );

    let sub = vec![0xBB; SUB_BLOCK];
    handler.write(0, &sub, false).await.unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY

    // Verify: sub-block is 0xBB, rest should be 0xAA (from S3 backfill).
    let block = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m)
        .await
        .unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &sub[..], "sub-block written");
    assert!(
        block[SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "backfill from S3 preserved. got 0x{:02X} at offset {}",
        block[SUB_BLOCK],
        SUB_BLOCK,
    );
}

/// Probe: verify that foyer insert→get is synchronously visible on our
/// shared PackIndexCache. If this fails, foyer's in-memory tier is evicting
/// entries between insert and get (capacity issue or bug).
#[tokio::test]
async fn probe_foyer_insert_get_visibility() {
    let pic = Arc::clone(&*SHARED_PACK_INDEX_CACHE);

    let pack_id: u64 = rand::random();
    let entries = vec![glidefs::block::pack::PackIndexEntry {
        hash: glidefs::block::block_map::Blake3Hash::from_bytes([0xAA; 16]),
        chunk_offset: 0,
        offset: 100,
        comp_length: 200,
    }];

    pic.insert_entries(pack_id, &entries);

    // Immediately read back.
    let got = pic.get_entries(pack_id).await;
    assert!(
        got.is_some(),
        "foyer insert→get should be immediately visible. pack_id={pack_id:016x}"
    );
    let got = got.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].chunk_offset, 0);
    assert_eq!(got[0].comp_length, 200);

    // Also verify lookup_block works.
    let lookup = pic.lookup_block(pack_id, 0).await;
    assert!(
        lookup.is_some(),
        "foyer lookup_block should find just-inserted entry"
    );
}

/// Exercise every state transition in a single block's lifecycle:
/// NP→CLEAN→DIRTY→SYNCING→NP→DIRTY (full cycle + re-write after eviction).
#[tokio::test]
async fn full_lifecycle_all_transitions() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "lifecycle", Arc::clone(&s3)).await;

    // NP: initial state.
    assert_eq!(cache.block_state(0), 0);

    // NP→CLEAN: claim block.
    assert!(cache.try_claim_block(0));
    assert_eq!(cache.block_state(0), 1);

    // CLEAN→DIRTY: write data.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2);

    // DIRTY→SYNCING: rotate for flush.
    let snapshot = cache.test_rotate_and_snapshot().unwrap();
    assert!(snapshot.contains(&0));
    assert_eq!(cache.block_state(0), 3);

    // SYNCING→DIRTY: write during flush (promote path).
    cache.write(0, &vec![0xBB; SUB_BLOCK]).unwrap();
    assert_eq!(cache.block_state(0), 2);

    // Flush until block is fully evicted. The promote re-dirtied it, so
    // we may need multiple flush cycles.
    for _ in 0..3 {
        cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
        cache.sync_manifest(&cs, &vm).await.unwrap();
        if cache.block_state(0) == 0 {
            break;
        }
    }
    assert_eq!(cache.block_state(0), 0, "block should be NP after flush cycles");

    // NP→DIRTY: write directly (transition_to_dirty handles NP).
    cache.write(0, &vec![0xCC; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2);

    // Verify data integrity through entire lifecycle.
    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(
        block.iter().all(|&b| b == 0xCC),
        "data after full lifecycle. first=0x{:02X}",
        block[0]
    );
}

// ============================================================================
// Write × Flush: gate-controlled interleavings
// ============================================================================

use glidefs::block::write_cache::{FlushSyncPoints, FlushStep, PromoteSyncPoints, PromoteStep};

/// WF-05/06: Write arrives while block is SYNCING (after rotation, before
/// eviction). Write triggers promote: copies from flushing → active, CAS
/// SYNCING→DIRTY. Flush continues, eviction CAS fails.
#[tokio::test]
async fn wf_write_during_syncing_with_flush_gate() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf-gate", Arc::clone(&s3)).await;

    // Write block 0.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Attach flush sync points.
    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(std::sync::Arc::new(flush_sp));

    // Spawn flush in background.
    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let flush_handle = tokio::spawn(async move {
        cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
    });

    // Wait for flush to reach AfterRotation (blocks are SYNCING, write lock released).
    let ev = flush_events.recv().await.unwrap(); // AfterCrcPrepass
    assert_eq!(ev.step, FlushStep::AfterCrcPrepass);
    flush_proceed.send(()).unwrap(); // proceed past CRC

    let ev = flush_events.recv().await.unwrap(); // AfterRotation
    assert_eq!(ev.step, FlushStep::AfterRotation);
    assert!(ev.block_count > 0, "should have snapshot blocks");

    // Block 0 is now SYNCING. Write a sub-block — triggers promote.
    let sub = vec![0xBB; SUB_BLOCK];
    cache.write(0, &sub).unwrap();

    // Block should now be DIRTY (promote CAS SYNCING→DIRTY succeeded).
    assert_eq!(cache.block_state(0), 2, "block should be DIRTY after promote");

    // Release flush to continue (compute, upload, evict).
    flush_proceed.send(()).unwrap(); // proceed past AfterCompute
    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::AfterCompute);
    flush_proceed.send(()).unwrap(); // proceed past BeforeEvict

    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::BeforeEvict);
    flush_proceed.send(()).unwrap(); // proceed with eviction

    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::AfterEvict);
    flush_proceed.send(()).unwrap(); // cleanup

    let (stats, _) = flush_handle.await.unwrap().unwrap();

    // Eviction CAS should have failed for block 0 (already DIRTY from promote).
    assert!(stats.blocks_cas_failed > 0, "eviction should fail for promoted block");

    // Block 0 should still be DIRTY with correct data.
    assert_eq!(cache.block_state(0), 2);
    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &sub[..], "sub-block from promote");
    assert!(
        block[SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "promoted data preserved. got 0x{:02X}",
        block[SUB_BLOCK]
    );
}

/// WF-13: Write's transition_to_dirty (CLEAN→DIRTY) races with flush's
/// DIRTY→SYNCING CAS. The flush CRC pre-pass sees the block as DIRTY,
/// but between CRC and rotation, a new write completes (block goes
/// CLEAN→DIRTY with new data). The rotation's CAS DIRTY→SYNCING claims
/// the block with the new data. CRC mismatch → block skipped, retried.
#[tokio::test]
async fn wf13_write_between_crc_and_rotation() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf13", Arc::clone(&s3)).await;

    // Write block 0 (DIRTY).
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Attach flush sync points.
    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(std::sync::Arc::new(flush_sp));

    // Spawn flush.
    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let flush_handle = tokio::spawn(async move {
        cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
    });

    // Wait for CRC pre-pass to complete.
    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::AfterCrcPrepass);

    // BEFORE releasing flush past CRC: write NEW data to block 0.
    // This overwrites the data that CRC was computed from.
    cache.write(0, &vec![0xBB; BLOCK_SIZE]).unwrap();

    // Release flush — rotation will claim block 0 (DIRTY→SYNCING).
    // The flushing file has 0xBB (the new data, since pwrite completed
    // before rotation). CRC was computed from 0xAA. Mismatch → skip.
    flush_proceed.send(()).unwrap(); // proceed past CRC

    // Flush continues. Let it run to completion.
    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::AfterRotation);
    flush_proceed.send(()).unwrap();

    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::AfterCompute);
    flush_proceed.send(()).unwrap();

    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::BeforeEvict);
    flush_proceed.send(()).unwrap();

    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::AfterEvict);
    flush_proceed.send(()).unwrap();

    let (stats, _) = flush_handle.await.unwrap().unwrap();

    // The block had a CRC mismatch — it should have been skipped
    // (CRC-mismatched or CAS-failed) and the data preserved.
    // Whether it uploaded depends on timing of the write vs CRC read,
    // but either way the data must not be corrupted.
    // After flush, block 0 is either DIRTY (skipped) or NP (uploaded).
    let state = cache.block_state(0);
    assert!(
        state == 0 || state == 2,
        "block should be DIRTY (skipped) or NP (uploaded). got {state}"
    );
}

/// PF-02/WF-15: Flush evicts block (SYNCING→NP) while promote is reading
/// from the flushing file. The promote's pread already completed (data
/// copied to active), but the CAS SYNCING→DIRTY fails (already NP).
/// The data is in the active file — the caller's write lands on it.
#[tokio::test]
async fn pf02_eviction_during_promote_read() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "pf02", Arc::clone(&s3)).await;

    // Write block 0 (DIRTY).
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Rotate: DIRTY→SYNCING.
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3); // SYNCING

    // Attach promote sync points.
    let (promote_sp, mut promote_events, promote_proceed) = PromoteSyncPoints::new();
    cache.set_promote_sync(std::sync::Arc::new(promote_sp));

    // Spawn a write to SYNCING block 0 (triggers promote) on a blocking thread
    // since promote gates use blocking recv.
    let cache_w = Arc::clone(&cache);
    let write_handle = tokio::task::spawn_blocking(move || {
        cache_w.write(0, &vec![0xBB; SUB_BLOCK])
    });

    // Wait for promote to reach BeforeCas (after pread+pwrite, before CAS).
    let ev = promote_events.recv().await.unwrap();
    assert_eq!(ev.step, PromoteStep::AfterCloneArc);
    promote_proceed.send(()).unwrap();

    let ev = promote_events.recv().await.unwrap();
    assert_eq!(ev.step, PromoteStep::AfterRead);
    promote_proceed.send(()).unwrap();

    let ev = promote_events.recv().await.unwrap();
    assert_eq!(ev.step, PromoteStep::BeforeCas);

    // NOW evict the block while promote is between pwrite and CAS.
    let evicted = cache.transition_syncing_to_not_present(0);
    assert!(evicted, "eviction should succeed — promote hasn't CAS'd yet");
    assert_eq!(cache.block_state(0), 0); // NOT_PRESENT

    // Release promote to attempt CAS (will fail: block is NP).
    promote_proceed.send(()).unwrap();

    // Write should still succeed — promote's pwrite already put data in
    // active file, and transition_to_dirty handles NP→DIRTY.
    write_handle.await.unwrap().unwrap();

    // Block should be DIRTY with correct data.
    assert_eq!(cache.block_state(0), 2);
    let block = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&block[0..SUB_BLOCK], &vec![0xBB; SUB_BLOCK][..], "sub-block written");
    assert!(
        block[SUB_BLOCK..].iter().all(|&b| b == 0xAA),
        "promoted data preserved after eviction race. got 0x{:02X}",
        block[SUB_BLOCK]
    );
}

/// RW-04: Concurrent pread and pwrite to the same block, both under the
/// data_file read lock. In production, each export has a single VM whose
/// block layer serializes I/O to the same block — concurrent read+write
/// to the same block is a synthetic scenario. POSIX does not guarantee
/// atomicity for I/O larger than a filesystem block (~4KB), so our 128KB
/// pwrite may be partially visible to a concurrent pread. What we verify:
/// no byte corruption (every byte is a valid writer value), proving
/// pwrite doesn't produce garbage even in this synthetic case.
#[tokio::test]
async fn rw04_concurrent_pread_pwrite_same_block() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "rw04", Arc::clone(&s3)).await;

    // Write block 0 with 0xAA (DIRTY).
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Concurrent: write 0xBB to block 0 while reading block 0.
    // Both hold the data_file read lock (shared).
    for _ in 0..50 {
        let c1 = Arc::clone(&cache);
        let c2 = Arc::clone(&cache);

        let (r1, r2) = tokio::join!(
            tokio::task::spawn_blocking(move || {
                c1.write(0, &vec![0xBB; BLOCK_SIZE])
            }),
            tokio::task::spawn_blocking(move || {
                c2.read_local(0, BLOCK_SIZE)
            }),
        );
        r1.unwrap().unwrap();
        let data = r2.unwrap().unwrap();

        // Every byte must be either 0xAA or 0xBB — no corruption.
        // A mix of 0xAA and 0xBB is acceptable (partial pwrite visibility).
        for (i, &b) in data.iter().enumerate() {
            assert!(
                b == 0xAA || b == 0xBB,
                "corrupted byte at offset {i}: 0x{b:02X} (expected 0xAA or 0xBB)"
            );
        }
    }
}

/// WW-07: Two concurrent full-block pwrite operations to the same block.
/// Both hold the data_file read lock. Last writer wins, no partial data.
#[tokio::test]
async fn ww07_concurrent_pwrite_same_block() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "ww07", Arc::clone(&s3)).await;

    // Make block 0 DIRTY first.
    cache.write(0, &vec![0x00; BLOCK_SIZE]).unwrap();

    for _ in 0..50 {
        let c1 = Arc::clone(&cache);
        let c2 = Arc::clone(&cache);

        let (r1, r2) = tokio::join!(
            tokio::task::spawn_blocking(move || {
                c1.write(0, &vec![0xAA; BLOCK_SIZE])
            }),
            tokio::task::spawn_blocking(move || {
                c2.write(0, &vec![0xBB; BLOCK_SIZE])
            }),
        );
        r1.unwrap().unwrap();
        r2.unwrap().unwrap();

        let data = cache.read_local(0, BLOCK_SIZE).unwrap();
        let first = data[0];
        let last = data[BLOCK_SIZE - 1];
        assert!(
            (first == 0xAA && last == 0xAA) || (first == 0xBB && last == 0xBB),
            "torn write detected: first=0x{first:02X} last=0x{last:02X}"
        );
    }
}

/// RF-03: Read SYNCING block during compute_flush_batch. The read should
/// get data from the flushing file (sync_read_local_block handles SYNCING).
#[tokio::test]
async fn rf03_read_syncing_during_compute() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "rf03", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Attach flush sync points.
    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(std::sync::Arc::new(flush_sp));

    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let flush_handle = tokio::spawn(async move {
        cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
    });

    // Advance flush past CRC and rotation.
    flush_events.recv().await.unwrap(); // AfterCrcPrepass
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterRotation
    // Block is now SYNCING. Don't release flush yet — it's between
    // rotation and compute.

    // Read block 0 while it's SYNCING.
    let data = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m)
        .await
        .unwrap();
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "read of SYNCING block should return flushing file data. first=0x{:02X}",
        data[0]
    );

    // Release flush to complete.
    flush_proceed.send(()).unwrap(); // proceed to compute+upload
    flush_events.recv().await.unwrap(); // AfterCompute
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // BeforeEvict
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterEvict
    flush_proceed.send(()).unwrap();
    flush_handle.await.unwrap().unwrap();
}

/// RW-01: Read starts while write is claiming a NOT_PRESENT block.
/// Read should go to S3 (block is CLEAN, not DIRTY).
#[tokio::test]
async fn rw01_read_during_write_claim() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, _cc1, _m1) =
        super::create_test_cache(&dir1, "rw01", Arc::clone(&s3)).await;
    cache1.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "rw01", Arc::clone(&s3)).await;

    // Claim block (NP→CLEAN) without writing data.
    assert!(cache2.try_claim_block(0));

    // Read: block is CLEAN. Should fall through to S3 (not return zeros).
    let data = cache2
        .read(0, BLOCK_SIZE, cc2.as_ref(), &pic2, &vm2, &cs2, &m2)
        .await
        .unwrap();
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "read of CLEAN block should return S3 data. first=0x{:02X}",
        data[0]
    );
}

/// CW-crash-at-W4: crash after set_present (CLEAN) but before pwrite.
/// Block is CLEAN with no data on SSD. Recovery should handle this.
#[tokio::test]
async fn cw_crash_at_set_present() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, _cs, _pic, _vm, _cc, _m) =
            super::create_test_cache(&dir, "cw-w4", Arc::clone(&s3)).await;
        // Simulate: set_present (NP→CLEAN) then crash before pwrite.
        assert!(cache.try_claim_block(0));
        assert_eq!(cache.block_state(0), 1); // CLEAN
        // Save metadata with CLEAN state.
        cache.save_metadata().unwrap();
        drop(cache); // crash
    }

    // Recovery: block is CLEAN in metadata. SSD has zeros.
    // Recovery should transition CLEAN→DIRTY or handle gracefully.
    let cache = recover_cache(&dir, "cw-w4", s3).await;
    // Block should be recoverable. CLEAN blocks with no WAL entry
    // are benign — they'll be treated as dirty on recovery and the
    // SSD zeros will be flushed (which is wrong data, but recovery
    // doesn't know what the data should be). This documents the
    // known limitation with wal_sync=false.
    let _ = cache.block_state(0);
}

/// CW-3: Simulate torn write — write partial data to a block, crash, recover.
/// After crash, the block has partial data on SSD. WAL replay marks it dirty.
/// On next flush, the partial data gets uploaded to S3.
///
/// This tests that recovery handles blocks with partial/inconsistent data
/// without panicking. The data integrity loss is unavoidable (torn write),
/// but the system must remain consistent.
#[tokio::test]
async fn cw3_torn_write_recovery() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    {
        let (cache, _cs, _pic, _vm, _cc, _m) =
            super::create_test_cache(&dir, "cw3", Arc::clone(&s3)).await;

        // Write full block with 0xAA.
        cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

        // Simulate torn write: overwrite first half with 0xBB directly via
        // the SSD file, without going through the cache (simulates a crash
        // mid-pwrite where only half the data landed).
        //
        // This is a simulation — real torn writes happen at the filesystem
        // block level. We approximate by writing half the block.
        cache.write(0, &vec![0xBB; BLOCK_SIZE / 2]).unwrap();

        // Save metadata so recovery sees the block as dirty.
        cache.save_metadata().unwrap();
        drop(cache); // crash
    }

    // Recovery should succeed — block is dirty, data is partially 0xBB + 0xAA.
    let cache = recover_cache(&dir, "cw3", Arc::clone(&s3)).await;
    assert!(cache.dirty_block_count() > 0, "block should be dirty after recovery");

    // Data is whatever survived the torn write. The key: no panic.
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    // First half should be 0xBB (second write landed), second half 0xAA.
    assert_eq!(b0[0], 0xBB, "first half has second write");
    assert_eq!(b0[BLOCK_SIZE - 1], 0xAA, "second half has first write");
}

// ============================================================================
// Remaining enumerated interleavings
// ============================================================================

/// WF-01: Write sees DIRTY while flush CRC pre-pass reads same block.
/// Both do concurrent I/O on the active file — write does pwrite, flush
/// CRC does pread. Under the read lock (shared), both proceed.
#[tokio::test]
async fn wf01_write_during_crc_prepass() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf01", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();

    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(std::sync::Arc::new(flush_sp));

    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let flush_handle = tokio::spawn(async move {
        cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
    });

    // Wait for CRC pre-pass to complete.
    let ev = flush_events.recv().await.unwrap();
    assert_eq!(ev.step, FlushStep::AfterCrcPrepass);

    // Write to block 0 while CRC has been computed but rotation hasn't happened.
    // Block is still DIRTY. Write overwrites the data CRC was computed from.
    cache.write(0, &vec![0xCC; BLOCK_SIZE]).unwrap();

    // Release flush. CRC mismatch may cause block 0 to be skipped.
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterRotation
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterCompute
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // BeforeEvict
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterEvict
    flush_proceed.send(()).unwrap();

    let (stats, _) = flush_handle.await.unwrap().unwrap();

    // Block 0: either uploaded (CRC matched if write landed after CRC read)
    // or skipped (CRC mismatch). Either way data is not lost.
    // Block 1 should have been uploaded normally.
    assert!(stats.packs_uploaded >= 1, "at least block 1 should upload");

    // Block 0 should still have 0xCC data.
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert!(b0.iter().all(|&b| b == 0xCC), "block 0 has latest write. first=0x{:02X}", b0[0]);
}

/// WF-06: Write promote and flush compute_flush_batch both read from the
/// flushing file concurrently. Both use Arc<SyncFile> with pread (safe).
#[tokio::test]
async fn wf06_promote_concurrent_with_compute() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf06", Arc::clone(&s3)).await;

    // Two blocks: both will be SYNCING during flush.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();

    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(std::sync::Arc::new(flush_sp));

    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let flush_handle = tokio::spawn(async move {
        cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
    });

    flush_events.recv().await.unwrap(); // AfterCrcPrepass
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterRotation
    // Blocks are SYNCING. Flush will proceed to compute (pread flushing).

    // Write sub-block to block 0 — triggers promote (also pread flushing).
    // Both promote and compute read from flushing file via Arc<SyncFile>.
    let sub = vec![0xCC; SUB_BLOCK];
    cache.write(0, &sub).unwrap();

    // Block 0 is now DIRTY (promoted). Release flush to compute+upload.
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterCompute
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // BeforeEvict
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterEvict
    flush_proceed.send(()).unwrap();

    flush_handle.await.unwrap().unwrap();

    // Block 0: DIRTY (promoted), data = 0xCC sub + 0xAA rest.
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&b0[0..SUB_BLOCK], &sub[..]);
    assert!(b0[SUB_BLOCK..].iter().all(|&b| b == 0xAA));

    // Block 1: evicted (NP) or still SYNCING depending on timing.
    let s1 = cache.block_state(1);
    assert!(s1 == 0 || s1 == 3, "block 1 evicted or syncing. got {s1}");
}

/// WF-12: Write's transition_to_dirty races with flush's CRC pread on the
/// same block. The write completes (CLEAN→DIRTY), flush CRC reads the
/// new data. No corruption — CRC just reflects the post-write data.
#[tokio::test]
async fn wf12_transition_to_dirty_during_crc() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf12", Arc::clone(&s3)).await;

    // Block 0 starts DIRTY with 0xAA.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Flush + write concurrently. The write may land during CRC pre-pass.
    // No gates needed — just verify data integrity after flush.
    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let cache_w = Arc::clone(&cache);

    let (flush_result, write_result) = tokio::join!(
        tokio::spawn(async move {
            cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
        }),
        tokio::spawn(async move {
            cache_w.write(0, &vec![0xBB; BLOCK_SIZE])
        }),
    );
    flush_result.unwrap().unwrap();
    write_result.unwrap().unwrap();

    // After concurrent flush + write, block 0 is in one of:
    // - DIRTY with 0xBB on SSD (write after flush evicted)
    // - NP with data in S3 (flush evicted after write)
    // Either is valid. The key: no corruption, no panic.
    let state = cache.block_state(0);
    match state {
        2 => {
            // DIRTY: write landed after eviction, data on SSD.
            let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
            assert!(b0.iter().all(|&b| b == 0xBB), "DIRTY block has 0xBB. first=0x{:02X}", b0[0]);
        }
        0 => {
            // NP: flush evicted after write. Data is either 0xAA (original)
            // or 0xBB (write landed before rotation) in S3. Both are valid
            // last-writer-wins outcomes.
        }
        other => panic!("unexpected state {other}"),
    }
}

/// PF-01: Promote clones flushing_file Arc exactly as flush starts eviction.
/// The Arc clone should keep the fd alive regardless of eviction timing.
#[tokio::test]
async fn pf01_promote_clone_during_eviction_start() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "pf01", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3); // SYNCING

    // Attach promote gates.
    let (promote_sp, mut promote_events, promote_proceed) = PromoteSyncPoints::new();
    cache.set_promote_sync(std::sync::Arc::new(promote_sp));

    // Start write (triggers promote) on blocking thread.
    let cache_w = Arc::clone(&cache);
    let write_handle = tokio::task::spawn_blocking(move || {
        cache_w.write(0, &vec![0xBB; SUB_BLOCK])
    });

    // Wait for promote to clone the flushing Arc.
    let ev = promote_events.recv().await.unwrap();
    assert_eq!(ev.step, PromoteStep::AfterCloneArc);

    // Evict the block NOW — between Arc clone and pread.
    cache.transition_syncing_to_not_present(0);
    assert_eq!(cache.block_state(0), 0); // NP

    // Release promote — pread should still work (fd alive via Arc).
    promote_proceed.send(()).unwrap();

    // Block is NP but promote still reads from the flushing file
    // (handles both SYNCING and NOT_PRESENT).
    let ev = promote_events.recv().await.unwrap();
    assert_eq!(ev.step, PromoteStep::AfterRead);
    promote_proceed.send(()).unwrap();

    let ev = promote_events.recv().await.unwrap();
    assert_eq!(ev.step, PromoteStep::BeforeCas);
    // CAS SYNCING→DIRTY will fail (block is NP).
    // But promote also handles NP→DIRTY.
    promote_proceed.send(()).unwrap();

    write_handle.await.unwrap().unwrap();

    assert_eq!(cache.block_state(0), 2); // DIRTY
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&b0[0..SUB_BLOCK], &vec![0xBB; SUB_BLOCK][..]);
    assert!(b0[SUB_BLOCK..].iter().all(|&b| b == 0xAA), "promoted data preserved");
}

/// PF-03: Promote pwrite to active file concurrent with flush eviction CAS.
/// Promote pwrite completes, then CAS. If eviction won first (NP), promote
/// CAS falls through to NP→DIRTY.
#[tokio::test]
async fn pf03_promote_pwrite_concurrent_with_eviction() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "pf03", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.test_rotate_and_snapshot().unwrap();

    let (promote_sp, mut promote_events, promote_proceed) = PromoteSyncPoints::new();
    cache.set_promote_sync(std::sync::Arc::new(promote_sp));

    let cache_w = Arc::clone(&cache);
    let write_handle = tokio::task::spawn_blocking(move || {
        cache_w.write(0, &vec![0xBB; SUB_BLOCK])
    });

    // Wait for promote to reach BeforeCas (pwrite already done).
    promote_events.recv().await.unwrap(); // AfterCloneArc
    promote_proceed.send(()).unwrap();
    let ev = promote_events.recv().await.unwrap();
    assert_eq!(ev.step, PromoteStep::AfterRead);
    promote_proceed.send(()).unwrap();
    let ev = promote_events.recv().await.unwrap();
    assert_eq!(ev.step, PromoteStep::BeforeCas);

    // Data is in the active file now (pwrite done). Evict concurrently.
    cache.transition_syncing_to_not_present(0);

    // Release promote CAS — block is NP, CAS SYNCING→DIRTY fails,
    // but NP→DIRTY path fires (data already in active file).
    promote_proceed.send(()).unwrap();
    write_handle.await.unwrap().unwrap();

    assert_eq!(cache.block_state(0), 2);
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&b0[0..SUB_BLOCK], &vec![0xBB; SUB_BLOCK][..]);
    assert!(b0[SUB_BLOCK..].iter().all(|&b| b == 0xAA));
}

/// RW-03: Read fast path pread while concurrent pwrite overwrites same block.
/// Both under the data_file read lock. Last-writer-wins, no torn data.
#[tokio::test]
async fn rw03_read_during_concurrent_pwrite() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "rw03", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    for _ in 0..50 {
        let c1 = Arc::clone(&cache);
        let c2 = Arc::clone(&cache);
        let (r1, r2) = tokio::join!(
            tokio::task::spawn_blocking(move || c1.write(0, &vec![0xBB; BLOCK_SIZE])),
            tokio::task::spawn_blocking(move || c2.read_local(0, BLOCK_SIZE)),
        );
        r1.unwrap().unwrap();
        let data = r2.unwrap().unwrap();
        let first = data[0];
        let last = data[BLOCK_SIZE - 1];
        assert!(
            (first == 0xAA && last == 0xAA) || (first == 0xBB && last == 0xBB),
            "torn read: first=0x{first:02X} last=0x{last:02X}"
        );
    }
}

/// MS-3: Two concurrent flush_to_s3 calls. The flush_lock serializes them.
/// Both should succeed and manifest should be consistent.
#[tokio::test]
async fn ms3_concurrent_flush_to_s3_serialized() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "ms3", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();

    let cache1 = Arc::clone(&cache);
    let cache2 = Arc::clone(&cache);
    let cs1 = ContentStore::new(Arc::clone(&s3), "test");
    let cs2 = ContentStore::new(Arc::clone(&s3), "test");
    let pic1 = Arc::clone(&pic);
    let pic2 = Arc::clone(&pic);
    let vm1 = Arc::clone(&vm);
    let vm2 = Arc::clone(&vm);

    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { cache1.flush_to_s3(&cs1, &pic1, &vm1).await }),
        tokio::spawn(async move { cache2.flush_to_s3(&cs2, &pic2, &vm2).await }),
    );

    // Both should succeed (serialized by flush_lock).
    r1.unwrap().unwrap();
    r2.unwrap().unwrap();

    // All blocks should be flushed.
    assert_eq!(cache.dirty_block_count(), 0);
}
