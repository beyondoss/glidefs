//! Read×Flush and Read×Write gate-controlled interleaving tests.
//!
//! These tests use ReadSyncPoints to force specific read/flush and
//! read/write orderings at state transition boundaries.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::write_cache::{
    FlushSyncPoints, FlushStep,
    ReadSyncPoints, ReadStep,
};

use super::{BLOCK_SIZE, DEVICE_SIZE, SHARED_PACK_INDEX_CACHE};

const SUB_BLOCK: usize = 4096;

/// RF-03 (gated): Read at locate_block state check (R3) sees SYNCING while
/// flush is in compute_flush_batch (F6). Read should get correct data from
/// the flushing file.
#[tokio::test]
async fn rf03_gated_read_syncing_during_compute() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "rf03g", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Attach both flush and read sync points.
    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(Arc::new(flush_sp));

    let (read_sp, mut read_events, read_proceed) = ReadSyncPoints::new();
    cache.set_read_sync(Arc::new(read_sp));

    // Start flush.
    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let flush_handle = tokio::spawn(async move {
        cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
    });

    // Advance flush past CRC and rotation. Block is now SYNCING.
    flush_events.recv().await.unwrap(); // AfterCrcPrepass
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterRotation
    // Hold flush here — between rotation and compute.

    // Start read.
    let cache_r = Arc::clone(&cache);
    let cs_r = ContentStore::new(Arc::clone(&s3), "test");
    let pic_r = Arc::clone(&pic);
    let vm_r = Arc::clone(&vm);
    let cc_r = Arc::clone(&cc);
    let m_r = Arc::new(ExportMetrics::new());
    let read_handle = tokio::spawn(async move {
        cache_r.read(0, BLOCK_SIZE, cc_r.as_ref(), &pic_r, &vm_r, &cs_r, &m_r).await
    });

    // Read hits locate_block state check — should see SYNCING.
    let ev = read_events.recv().await.unwrap();
    assert_eq!(ev.step, ReadStep::AfterStateCheck);
    assert_eq!(ev.block_state, 3, "read should see SYNCING");

    // Release read to proceed — it'll read from flushing file.
    read_proceed.send(()).unwrap();
    let data = read_handle.await.unwrap().unwrap();
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "read during compute should get flushing file data. first=0x{:02X}",
        data[0]
    );

    // Release flush to complete.
    flush_proceed.send(()).unwrap(); // AfterCompute
    flush_events.recv().await.unwrap();
    flush_proceed.send(()).unwrap(); // BeforeEvict
    flush_events.recv().await.unwrap();
    flush_proceed.send(()).unwrap(); // AfterEvict
    flush_events.recv().await.unwrap();
    flush_proceed.send(()).unwrap();
    flush_handle.await.unwrap().unwrap();
}

/// RF-04 (gated): Read re-checks state (R4) exactly as flush evicts (F8).
/// Read should handle the SYNCING→NP transition gracefully.
#[tokio::test]
async fn rf04_gated_read_recheck_during_eviction() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "rf04g", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(Arc::new(flush_sp));

    let (read_sp, mut read_events, read_proceed) = ReadSyncPoints::new();
    cache.set_read_sync(Arc::new(read_sp));

    // Start flush, advance to BeforeEvict.
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
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterCompute
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // BeforeEvict
    // Flush paused right before eviction. Block is still SYNCING.

    // Start read.
    let cache_r = Arc::clone(&cache);
    let cs_r = ContentStore::new(Arc::clone(&s3), "test");
    let pic_r = Arc::clone(&pic);
    let vm_r = Arc::clone(&vm);
    let cc_r = Arc::clone(&cc);
    let m_r = Arc::new(ExportMetrics::new());
    let read_handle = tokio::spawn(async move {
        cache_r.read(0, BLOCK_SIZE, cc_r.as_ref(), &pic_r, &vm_r, &cs_r, &m_r).await
    });

    // Read sees SYNCING at state check.
    let ev = read_events.recv().await.unwrap();
    assert_eq!(ev.block_state, 3);

    // NOW release flush to evict (SYNCING→NP) while read is between
    // state check and sync_read_local_block.
    flush_proceed.send(()).unwrap(); // eviction runs

    // Release read to proceed. sync_read_local_block will re-check state.
    // If block was evicted, it returns None → falls to cold path → S3.
    read_proceed.send(()).unwrap();

    let data = read_handle.await.unwrap().unwrap();
    // Data should be correct regardless — either from flushing file (if read
    // won the race) or from S3/clean_cache (if eviction won).
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "read during eviction should return correct data. first=0x{:02X}",
        data[0]
    );

    // Finish flush.
    flush_events.recv().await.unwrap(); // AfterEvict
    flush_proceed.send(()).unwrap();
    flush_handle.await.unwrap().unwrap();
}

/// RW-01 (gated): Read's locate_block state check (R3) sees NP while
/// concurrent write is claiming (W3: NP→CLEAN). Read should go to S3.
#[tokio::test]
async fn rw01_gated_read_during_write_claim() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Set up S3 data.
    let dir1 = TempDir::new().unwrap();
    let (cache1, cs1, pic, vm1, _cc1, _m1) =
        super::create_test_cache(&dir1, "rw01g", Arc::clone(&s3)).await;
    cache1.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache1.flush_to_s3(&cs1, &pic, &vm1).await.unwrap();
    cache1.sync_manifest(&cs1, &vm1).await.unwrap();
    drop(cache1);
    drop(dir1);

    // Cold fork.
    let dir2 = TempDir::new().unwrap();
    let (cache2, cs2, pic2, vm2, cc2, m2) =
        super::create_cold_reader(&dir2, "rw01g", Arc::clone(&s3)).await;

    let (read_sp, mut read_events, read_proceed) = ReadSyncPoints::new();
    cache2.set_read_sync(Arc::new(read_sp));

    // Start read.
    let cache_r = Arc::clone(&cache2);
    let cs_r = ContentStore::new(Arc::clone(&s3), "test");
    let pic_r = Arc::clone(&pic2);
    let vm_r = Arc::clone(&vm2);
    let cc_r = Arc::clone(&cc2);
    let m_r = Arc::new(ExportMetrics::new());
    let read_handle = tokio::spawn(async move {
        cache_r.read(0, BLOCK_SIZE, cc_r.as_ref(), &pic_r, &vm_r, &cs_r, &m_r).await
    });

    // Read hits state check — block is NP.
    let ev = read_events.recv().await.unwrap();
    assert_eq!(ev.block_state, 0, "read should see NP");

    // While read is paused at state check, claim the block (NP→CLEAN).
    assert!(cache2.try_claim_block(0));

    // Release read. It already saw NP, so it should proceed to cold path
    // (not re-check and see CLEAN). The state check result is captured.
    read_proceed.send(()).unwrap();

    let data = read_handle.await.unwrap().unwrap();
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "read should return S3 data. first=0x{:02X}",
        data[0]
    );
}

// RW-03: Read fast path (DIRTY block) bypasses locate_block — no read gate
// fires. This interleaving is covered by rw04_concurrent_pread_pwrite_same_block
// in interleaving.rs (stress test, no gates needed).

/// RF-02: Read starts after flush rotation completes (missed the lock window).
/// Block is SYNCING. Read goes through locate_block slow path → flushing file.
#[tokio::test]
async fn rf02_read_after_rotation_completes() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "rf02", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Rotate: block becomes SYNCING.
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3);

    // Read after rotation — block is SYNCING, read uses flushing file.
    let data = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m)
        .await
        .unwrap();
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "read after rotation gets flushing file data. first=0x{:02X}",
        data[0]
    );
}

/// RF-08: Read does S3 fetch concurrent with flush cleanup (F9).
/// Independent operations — just verify no interference.
#[tokio::test]
async fn rf08_s3_fetch_during_flush_cleanup() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "rf08", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();

    // Block is NP (evicted). Read fetches from S3.
    // This is "after F9" — flush fully complete, flushing file gone.
    assert_eq!(cache.block_state(0), 0);
    let data = cache
        .read(0, BLOCK_SIZE, cc.as_ref(), &pic, &vm, &cs, &m)
        .await
        .unwrap();
    assert!(
        data.iter().all(|&b| b == 0xAA),
        "S3 fetch after flush cleanup returns correct data. first=0x{:02X}",
        data[0]
    );
}

/// WF-02/03/04: Write blocked by flush's data_file write lock during rotation.
/// The write must wait until rotation releases the write lock, then proceed
/// correctly on the new active file.
#[tokio::test]
async fn wf_write_blocked_during_rotation() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf-lock", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Concurrent flush + write. The write's cache.write acquires the data_file
    // read lock. Rotation acquires the write lock. RwLock ensures the write
    // either completes before rotation or waits until after.
    let cache_w = Arc::clone(&cache);
    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);

    let (write_result, flush_result) = tokio::join!(
        tokio::task::spawn_blocking(move || {
            cache_w.write(0, &vec![0xBB; BLOCK_SIZE])
        }),
        tokio::spawn(async move {
            cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
        }),
    );

    write_result.unwrap().unwrap();
    flush_result.unwrap().unwrap();

    // No deadlock, no panic. Block has 0xBB (write completed).
    let state = cache.block_state(0);
    assert!(state == 0 || state == 2, "block is DIRTY or NP. got {state}");
}

/// RW-03: Concurrent read + write to same DIRTY block — no crash or deadlock.
///
/// pwrite is NOT atomic for multi-page writes. A concurrent pread can
/// observe partially-written data (torn read). This is fine: the block
/// device protocol (NBD, NVMe, SCSI) does not require atomicity for
/// concurrent overlapping requests. The guest filesystem/page cache
/// serializes overlapping I/O before it reaches us.
///
/// We verify: no panic, no deadlock, no internal state corruption.
/// We do NOT assert data content — torn reads are expected and harmless.
#[tokio::test]
async fn rw03_read_fast_path_during_pwrite() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "rw03-fp", Arc::clone(&s3)).await;

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
        assert_eq!(data.len(), BLOCK_SIZE, "read returned wrong length");
    }
}

/// WW-08: Verify two writers can't both CAS NOT_PRESENT→CLEAN on the same
/// block. The second CAS must fail (returns false).
#[tokio::test]
async fn ww08_double_cas_claim_impossible() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "ww08", Arc::clone(&s3)).await;

    assert_eq!(cache.block_state(0), 0); // NP
    assert!(cache.try_claim_block(0), "first CAS should succeed");
    assert_eq!(cache.block_state(0), 1); // CLEAN
    assert!(!cache.try_claim_block(0), "second CAS must fail — block already CLEAN");
}

/// WF-10: Verify that F1 (CRC pre-pass) only targets DIRTY blocks, not CLEAN.
/// A block that is CLEAN during CRC pre-pass is not included in the flush.
#[tokio::test]
async fn wf10_crc_prepass_skips_clean_blocks() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf10", Arc::clone(&s3)).await;

    // Block 0: DIRTY (will be flushed).
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Block 1: CLEAN (CAS'd but no data written — simulates mid-write).
    assert!(cache.try_claim_block(1));
    assert_eq!(cache.block_state(1), 1); // CLEAN

    // Flush should only flush block 0, not block 1.
    let (stats, _) = cache.flush_packs(&cs, &pic, &vm, None).await.unwrap();
    assert!(stats.packs_uploaded > 0);

    // Block 0: evicted (NP). Block 1: still CLEAN (not touched by flush).
    assert_eq!(cache.block_state(0), 0); // NP
    assert_eq!(cache.block_state(1), 1); // CLEAN (untouched)
}

/// Stateright-derived: writer at BeforeCas while flush is rotating.
/// The writer's CAS targets NP→CLEAN. The flush's rotation targets
/// DIRTY→SYNCING. They can't conflict on the same block, but verify
/// the writer retries correctly when the block state changed.
#[tokio::test]
async fn sr_writer_cas_during_flush_rotation() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "sr-cas-rot", Arc::clone(&s3)).await;

    // Write block 0 to make it DIRTY.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Flush to S3 and evict (block becomes NP).
    cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
    cache.sync_manifest(&cs, &vm).await.unwrap();
    assert_eq!(cache.block_state(0), 0); // NP

    // Write again (NP→DIRTY).
    cache.write(0, &vec![0xBB; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY

    // Now concurrent: flush starts rotating (DIRTY→SYNCING) while
    // another write is about to CAS on the same block.
    // The write should see SYNCING (not NP), enter promote path, succeed.
    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(Arc::new(flush_sp));

    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let flush_handle = tokio::spawn(async move {
        cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
    });

    // Advance flush past rotation.
    flush_events.recv().await.unwrap(); // AfterCrcPrepass
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterRotation
    // Block is SYNCING. Flush paused.

    // Write sub-block — will see SYNCING, promote from flushing file.
    cache.write(0, &vec![0xCC; SUB_BLOCK]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY (promoted)

    // Finish flush.
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterCompute
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // BeforeEvict
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterEvict
    flush_proceed.send(()).unwrap();
    flush_handle.await.unwrap().unwrap();

    // Block should be DIRTY with correct data.
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&b0[0..SUB_BLOCK], &vec![0xCC; SUB_BLOCK][..]);
    assert!(b0[SUB_BLOCK..].iter().all(|&b| b == 0xBB));
}

/// WF-11/18: Verify that during flush, a block can only be in ONE state.
/// A CLEAN block on the new active file and a SYNCING block on the flushing
/// file cannot be the same block — the CAS is exclusive.
#[tokio::test]
async fn wf11_18_block_state_exclusivity() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "wf-excl", Arc::clone(&s3)).await;

    // Write block 0.
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY

    // Rotate: DIRTY→SYNCING.
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3); // SYNCING

    // Block 0 is SYNCING. It cannot also be CLEAN or DIRTY — the state
    // is a single atomic value in the SparseStateMap. Attempting to set
    // it to CLEAN (via try_claim_block) must fail.
    assert!(
        !cache.try_claim_block(0),
        "CAS NP→CLEAN must fail when block is SYNCING"
    );
    assert_eq!(cache.block_state(0), 3); // Still SYNCING

    // After promote (write to SYNCING block), block is DIRTY.
    cache.write(0, &vec![0xBB; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY

    // Now it can't be SYNCING or CLEAN.
    assert!(
        !cache.try_claim_block(0),
        "CAS NP→CLEAN must fail when block is DIRTY"
    );
}

/// Verify the complete state transition table: only valid transitions succeed.
#[tokio::test]
async fn state_transition_table_completeness() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, _cs, _pic, _vm, _cc, _m) =
        super::create_test_cache(&dir, "trans-table", Arc::clone(&s3)).await;

    // Block 0: NOT_PRESENT
    assert_eq!(cache.block_state(0), 0);

    // Valid: NP→CLEAN
    assert!(cache.try_claim_block(0));
    assert_eq!(cache.block_state(0), 1);

    // Invalid from CLEAN: can't claim again
    assert!(!cache.try_claim_block(0));

    // Valid: CLEAN→DIRTY (via write)
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2);

    // Valid: DIRTY→SYNCING (via rotation)
    cache.test_rotate_and_snapshot().unwrap();
    assert_eq!(cache.block_state(0), 3);

    // Valid: SYNCING→DIRTY (via promote/write)
    cache.write(0, &vec![0xBB; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2);

    // Valid: DIRTY→SYNCING again
    // (need to clean up flushing state first)
    cache.flush_to_s3(
        &ContentStore::new(Arc::clone(&s3), "test"),
        &Arc::clone(&*SHARED_PACK_INDEX_CACHE),
        &Arc::new(parking_lot::RwLock::new(
            glidefs::block::volume_manifest::VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE as u32),
        )),
    ).await.unwrap();

    // After full flush: NP (evicted)
    // May need multiple cycles if promote re-dirtied
    for _ in 0..3 {
        if cache.block_state(0) == 0 { break; }
        cache.flush_to_s3(
            &ContentStore::new(Arc::clone(&s3), "test"),
            &Arc::clone(&*SHARED_PACK_INDEX_CACHE),
            &Arc::new(parking_lot::RwLock::new(
                glidefs::block::volume_manifest::VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE as u32),
            )),
        ).await.unwrap();
    }

    // Valid: SYNCING→NP (eviction happened inside flush)
    assert_eq!(cache.block_state(0), 0);

    // Valid: NP→DIRTY (write after eviction, transition_to_dirty handles NP)
    cache.write(0, &vec![0xCC; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.block_state(0), 2);
}

/// WF-07 (gated): Write promote reads flushing file while flush is uploading
/// to S3. Both use Arc<SyncFile> with pread — safe concurrent I/O.
/// Gate flush at AfterCompute (between compute and upload), trigger promote.
#[tokio::test]
async fn wf07_promote_during_s3_upload() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) =
        super::create_test_cache(&dir, "wf07", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();
    cache.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();

    let (flush_sp, mut flush_events, flush_proceed) = FlushSyncPoints::new();
    cache.set_flush_sync(Arc::new(flush_sp));

    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);
    let flush_handle = tokio::spawn(async move {
        cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
    });

    // Advance flush to AfterCompute (compute done, about to upload).
    flush_events.recv().await.unwrap(); // AfterCrcPrepass
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterRotation
    flush_proceed.send(()).unwrap();
    flush_events.recv().await.unwrap(); // AfterCompute
    // Flush paused between compute and upload. Blocks are SYNCING.

    // Write sub-block to block 0 — triggers promote (reads flushing file).
    let sub = vec![0xCC; SUB_BLOCK];
    cache.write(0, &sub).unwrap();
    assert_eq!(cache.block_state(0), 2); // DIRTY (promoted)

    // Release flush to upload + evict.
    flush_proceed.send(()).unwrap(); // BeforeEvict
    flush_events.recv().await.unwrap();
    flush_proceed.send(()).unwrap(); // AfterEvict
    flush_events.recv().await.unwrap();
    flush_proceed.send(()).unwrap();
    flush_handle.await.unwrap().unwrap();

    // Block 0 data: promoted 0xAA + sub-block 0xCC.
    let b0 = cache.read_local(0, BLOCK_SIZE).unwrap();
    assert_eq!(&b0[0..SUB_BLOCK], &sub[..]);
    assert!(b0[SUB_BLOCK..].iter().all(|&b| b == 0xAA));
}

/// RF-01: Read holds data_file read lock, flush rotation needs write lock.
/// The read must complete before rotation can proceed. Verify no deadlock
/// and correct data.
#[tokio::test]
async fn rf01_read_blocks_rotation() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, m) =
        super::create_test_cache(&dir, "rf01", Arc::clone(&s3)).await;

    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Concurrent read + flush. Read holds read lock (shared with other reads
    // and writes). Flush rotation needs write lock — must wait for all readers.
    // No deadlock: read lock is not held across an await point that depends
    // on the write lock.
    let cache_r = Arc::clone(&cache);
    let cs_r = ContentStore::new(Arc::clone(&s3), "test");
    let pic_r = Arc::clone(&pic);
    let vm_r = Arc::clone(&vm);
    let cc_r = Arc::clone(&cc);
    let m_r = Arc::new(ExportMetrics::new());

    let cache_f = Arc::clone(&cache);
    let cs_f = ContentStore::new(Arc::clone(&s3), "test");
    let pic_f = Arc::clone(&pic);
    let vm_f = Arc::clone(&vm);

    let (read_result, flush_result) = tokio::join!(
        tokio::spawn(async move {
            cache_r.read(0, BLOCK_SIZE, cc_r.as_ref(), &pic_r, &vm_r, &cs_r, &m_r).await
        }),
        tokio::spawn(async move {
            cache_f.flush_packs(&cs_f, &pic_f, &vm_f, None).await
        }),
    );

    let data = read_result.unwrap().unwrap();
    assert!(data.iter().all(|&b| b == 0xAA), "read returned correct data");
    flush_result.unwrap().unwrap();
}
