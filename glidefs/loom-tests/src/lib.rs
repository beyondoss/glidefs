//! Loom concurrency tests for GlideFS write cache state machine.
//!
//! These tests exercise the REAL `SparseStateMap` from glidefs under loom's
//! exhaustive interleaving explorer. The `loom` feature in glidefs swaps
//! `std::sync::atomic` for `loom::sync::atomic` in `block_map.rs`, so every
//! CAS, load, and store is intercepted by loom's scheduler.
//!
//! Run with: cd loom-tests && cargo test --release

use glidefs::block::block_map::{SparseBlockState, SparseStateMap};
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

// Shorthand
const NP: u8 = SparseBlockState::NOT_PRESENT;
const CL: u8 = SparseBlockState::CLEAN;
const DI: u8 = SparseBlockState::DIRTY;
const SY: u8 = SparseBlockState::SYNCING;

/// Helper: mirrors CacheInner::transition_to_dirty on a SparseStateMap.
/// CAS loop handling all 4 source states.
fn transition_to_dirty(map: &SparseStateMap, idx: usize, dirty_count: &AtomicU64) -> bool {
    loop {
        let current = map.get(idx);

        if current == DI {
            return false;
        }

        if current == CL || current == NP {
            if map.cas(idx, current, DI).is_ok() {
                dirty_count.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        } else if current == SY {
            if map.cas(idx, SY, DI).is_ok() {
                dirty_count.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        } else {
            return false;
        }
    }
}

/// Helper: mirrors CacheInner::transition_dirty_to_syncing (single CAS, no loop).
fn transition_dirty_to_syncing(map: &SparseStateMap, idx: usize) -> bool {
    map.cas(idx, DI, SY).is_ok()
}

/// Helper: mirrors CacheInner::transition_syncing_to_not_present (single CAS).
fn transition_syncing_to_not_present(map: &SparseStateMap, idx: usize) -> bool {
    map.cas(idx, SY, NP).is_ok()
}

// ============================================================================
// Adjacent slot contention (the 2-bit packing invariant)
// ============================================================================

/// Two threads CAS adjacent slots (same AtomicU8) concurrently.
/// Both must succeed despite byte-level CAS contention.
#[test]
fn adjacent_slots_both_transition() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.set_present(1);

        let m1 = Arc::clone(&map);
        let m2 = Arc::clone(&map);

        // Slot 0: CLEAN → DIRTY
        let t1 = thread::spawn(move || m1.cas(0, CL, DI));
        // Slot 1: CLEAN → SYNCING
        let t2 = thread::spawn(move || m2.cas(1, CL, SY));

        assert!(t1.join().unwrap().is_ok(), "slot 0 CAS must succeed");
        assert!(t2.join().unwrap().is_ok(), "slot 1 CAS must succeed");
        assert_eq!(map.get(0), DI);
        assert_eq!(map.get(1), SY);
        // Untouched slots remain NOT_PRESENT
        assert_eq!(map.get(2), NP);
        assert_eq!(map.get(3), NP);
    });
}

// ============================================================================
// try_set_present races
// ============================================================================

/// Two threads race to claim the same NOT_PRESENT block.
/// Exactly one must win.
#[test]
fn try_set_present_exactly_one_wins() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        let m1 = Arc::clone(&map);
        let m2 = Arc::clone(&map);

        let t1 = thread::spawn(move || m1.try_set_present(0));
        let t2 = thread::spawn(move || m2.try_set_present(0));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert!(
            (r1 && !r2) || (!r1 && r2),
            "exactly one thread must win try_set_present"
        );
        assert_eq!(map.get(0), CL);
    });
}

/// try_set_present on adjacent slots — both must succeed.
#[test]
fn try_set_present_adjacent_both_succeed() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        let m1 = Arc::clone(&map);
        let m2 = Arc::clone(&map);

        let t1 = thread::spawn(move || m1.try_set_present(0));
        let t2 = thread::spawn(move || m2.try_set_present(1));

        assert!(t1.join().unwrap(), "slot 0 must succeed");
        assert!(t2.join().unwrap(), "slot 1 must succeed");
        assert_eq!(map.get(0), CL);
        assert_eq!(map.get(1), CL);
    });
}

// ============================================================================
// CacheInner transition logic on real SparseStateMap
// ============================================================================

/// Two concurrent writes to the same CLEAN block.
/// Exactly one should transition CLEAN → DIRTY.
#[test]
fn concurrent_writes_same_block() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        let dc = Arc::new(AtomicU64::new(0));
        map.set_present(0);

        let m1 = Arc::clone(&map);
        let d1 = Arc::clone(&dc);
        let m2 = Arc::clone(&map);
        let d2 = Arc::clone(&dc);

        let t1 = thread::spawn(move || transition_to_dirty(&m1, 0, &d1));
        let t2 = thread::spawn(move || transition_to_dirty(&m2, 0, &d2));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert!(
            (r1 && !r2) || (!r1 && r2),
            "exactly one write should transition CLEAN→DIRTY"
        );
        assert_eq!(map.get(0), DI);
        assert_eq!(dc.load(Ordering::Relaxed), 1);
    });
}

/// Two concurrent writes to adjacent slots — both must succeed.
#[test]
fn concurrent_writes_adjacent_slots() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        let dc = Arc::new(AtomicU64::new(0));
        map.set_present(0);
        map.set_present(1);

        let m1 = Arc::clone(&map);
        let d1 = Arc::clone(&dc);
        let m2 = Arc::clone(&map);
        let d2 = Arc::clone(&dc);

        let t1 = thread::spawn(move || transition_to_dirty(&m1, 0, &d1));
        let t2 = thread::spawn(move || transition_to_dirty(&m2, 1, &d2));

        assert!(t1.join().unwrap(), "slot 0 must transition");
        assert!(t2.join().unwrap(), "slot 1 must transition");
        assert_eq!(map.get(0), DI);
        assert_eq!(map.get(1), DI);
        assert_eq!(dc.load(Ordering::Relaxed), 2);
    });
}

/// Write during flush: transition_to_dirty vs transition_dirty_to_syncing.
#[test]
fn write_vs_flush_claim() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.cas(0, CL, DI).unwrap();

        let dc = Arc::new(AtomicU64::new(1));

        let m_flush = Arc::clone(&map);
        let m_write = Arc::clone(&map);
        let d_write = Arc::clone(&dc);

        let t_flush = thread::spawn(move || transition_dirty_to_syncing(&m_flush, 0));
        let t_write = thread::spawn(move || transition_to_dirty(&m_write, 0, &d_write));

        let flush_won = t_flush.join().unwrap();
        let write_changed = t_write.join().unwrap();

        let state = map.get(0);

        match (flush_won, write_changed) {
            (true, true) => {
                // Flush claimed DIRTY→SYNCING, then writer re-dirtied SYNCING→DIRTY
                assert_eq!(state, DI);
            }
            (true, false) => {
                // Flush claimed, writer saw DIRTY (no-op) before flush ran
                assert_eq!(state, SY);
            }
            (false, false) => {
                // Writer saw DIRTY (no-op), but then flush also failed?
                // Can't happen: if writer didn't change state, block is still DIRTY
                // and flush CAS should succeed (no adjacent contention here — but
                // actually there IS no retry loop in transition_dirty_to_syncing,
                // so a byte-level spurious failure from adjacent... wait, there's
                // no adjacent contention in this test. So this shouldn't happen.
                // But with loom we'll find out for sure.
                panic!("unexpected: neither flush nor write succeeded");
            }
            (false, true) => {
                // Flush CAS failed but write changed state — impossible from DIRTY
                // initial state without flush changing it first.
                panic!("unexpected: flush failed but write changed state");
            }
        }
    });
}

/// Eviction vs concurrent write: SYNCING→NP vs SYNCING→DIRTY.
/// The writer must always end up with the block DIRTY (data preservation).
#[test]
fn eviction_vs_write() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.cas(0, CL, SY).unwrap();
        let dc = Arc::new(AtomicU64::new(0));

        let m_evict = Arc::clone(&map);
        let m_write = Arc::clone(&map);
        let d_write = Arc::clone(&dc);

        let t_evict = thread::spawn(move || transition_syncing_to_not_present(&m_evict, 0));
        let t_write = thread::spawn(move || transition_to_dirty(&m_write, 0, &d_write));

        let _evict_won = t_evict.join().unwrap();
        let write_changed = t_write.join().unwrap();

        // Critical invariant: write must always succeed
        assert!(write_changed, "writer must always transition to DIRTY");
        assert_eq!(map.get(0), DI, "final state must be DIRTY");
        assert_eq!(dc.load(Ordering::Relaxed), 1);
    });
}

/// Full flush cycle: DIRTY→SYNCING→NP with a concurrent write.
/// If the write lands, data must not be lost (state must be DIRTY).
#[test]
fn full_flush_cycle_with_concurrent_write() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.cas(0, CL, DI).unwrap();
        let dc = Arc::new(AtomicU64::new(1));

        let m_flush = Arc::clone(&map);
        let m_write = Arc::clone(&map);
        let d_write = Arc::clone(&dc);

        let t_flush = thread::spawn(move || {
            let claimed = transition_dirty_to_syncing(&m_flush, 0);
            if claimed {
                let evicted = transition_syncing_to_not_present(&m_flush, 0);
                (true, evicted)
            } else {
                (false, false)
            }
        });

        let t_write = thread::spawn(move || transition_to_dirty(&m_write, 0, &d_write));

        let (_claimed, _evicted) = t_flush.join().unwrap();
        let write_changed = t_write.join().unwrap();

        let state = map.get(0);

        if write_changed {
            // Write landed — data must not be lost
            assert_eq!(state, DI, "write landed but state is not DIRTY — data loss!");
        }
    });
}

/// Two concurrent promotes of the same SYNCING block.
/// Exactly one wins SYNCING→DIRTY, the other sees DIRTY (no-op).
#[test]
fn concurrent_promote_same_block() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.cas(0, CL, SY).unwrap();
        let dc = Arc::new(AtomicU64::new(0));

        let m1 = Arc::clone(&map);
        let d1 = Arc::clone(&dc);
        let m2 = Arc::clone(&map);
        let d2 = Arc::clone(&dc);

        let t1 = thread::spawn(move || transition_to_dirty(&m1, 0, &d1));
        let t2 = thread::spawn(move || transition_to_dirty(&m2, 0, &d2));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert!(
            (r1 && !r2) || (!r1 && r2),
            "exactly one should transition SYNCING→DIRTY"
        );
        assert_eq!(map.get(0), DI);
        assert_eq!(dc.load(Ordering::Relaxed), 1);
    });
}

/// Write to NOT_PRESENT block (NP→DIRTY).
/// This path fires when promote's CAS fails because flush already evicted.
#[test]
fn write_to_not_present() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        let dc = Arc::new(AtomicU64::new(0));

        transition_to_dirty(&map, 0, &dc);

        assert_eq!(map.get(0), DI);
        assert_eq!(dc.load(Ordering::Relaxed), 1);
    });
}

/// Flush claims two adjacent dirty blocks concurrently.
/// transition_dirty_to_syncing has no retry loop — this verifies whether
/// byte-level contention can cause a missed claim.
#[test]
fn flush_claims_adjacent_blocks() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.set_present(1);
        map.cas(0, CL, DI).unwrap();
        map.cas(1, CL, DI).unwrap();

        let m1 = Arc::clone(&map);
        let m2 = Arc::clone(&map);

        let t1 = thread::spawn(move || transition_dirty_to_syncing(&m1, 0));
        let t2 = thread::spawn(move || transition_dirty_to_syncing(&m2, 1));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Counts must match states
        let s0 = map.get(0);
        let s1 = map.get(1);

        if !r1 {
            // Slot 0 claim failed due to byte-level contention.
            // This is a real finding if it happens — means the production
            // code's single-CAS approach can drop a claim under contention.
            // (Safe in practice because claiming is serialized under write lock.)
            assert_eq!(s0, DI, "failed claim should leave block DIRTY");
        } else {
            assert_eq!(s0, SY, "successful claim should leave block SYNCING");
        }

        if !r2 {
            assert_eq!(s1, DI, "failed claim should leave block DIRTY");
        } else {
            assert_eq!(s1, SY, "successful claim should leave block SYNCING");
        }
    });
}
