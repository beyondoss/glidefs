//! Loom concurrency tests for GlideFS write cache state machine.
//!
//! These tests exercise the REAL `SparseStateMap` from glidefs under loom's
//! exhaustive interleaving explorer. The `loom` feature in glidefs swaps
//! `std::sync::atomic` for `loom::sync::atomic` in `block_map.rs`, so every
//! CAS, load, and store is intercepted by loom's scheduler.
//!
//! Run with: cd loom-tests && cargo test --release

use glidefs::block::block_map::{DirtyTransition, SparseBlockState, SparseStateMap};
use loom::sync::Arc;
use loom::thread;

// Shorthand
const NP: u8 = SparseBlockState::NOT_PRESENT;
const CL: u8 = SparseBlockState::CLEAN;
const DI: u8 = SparseBlockState::DIRTY;
const SY: u8 = SparseBlockState::SYNCING;

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

        let t1 = thread::spawn(move || m1.cas(0, CL, DI));
        let t2 = thread::spawn(move || m2.cas(1, CL, SY));

        assert!(t1.join().unwrap().is_ok(), "slot 0 CAS must succeed");
        assert!(t2.join().unwrap().is_ok(), "slot 1 CAS must succeed");
        assert_eq!(map.get(0), DI);
        assert_eq!(map.get(1), SY);
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
// transition_to_dirty — the real method on SparseStateMap
// ============================================================================

/// Two concurrent writes to the same CLEAN block.
/// Exactly one should transition CLEAN → DIRTY.
#[test]
fn concurrent_writes_same_block() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);

        let m1 = Arc::clone(&map);
        let m2 = Arc::clone(&map);

        let t1 = thread::spawn(move || m1.transition_to_dirty(0));
        let t2 = thread::spawn(move || m2.transition_to_dirty(0));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Exactly one transitions, the other sees AlreadyDirty
        assert!(
            matches!(
                (r1, r2),
                (DirtyTransition::FromCleanOrNotPresent, DirtyTransition::AlreadyDirty)
                    | (DirtyTransition::AlreadyDirty, DirtyTransition::FromCleanOrNotPresent)
            ),
            "exactly one write should transition CLEAN→DIRTY, got ({:?}, {:?})",
            r1, r2
        );
        assert_eq!(map.get(0), DI);
    });
}

/// Two concurrent writes to adjacent slots — both must succeed.
#[test]
fn concurrent_writes_adjacent_slots() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.set_present(1);

        let m1 = Arc::clone(&map);
        let m2 = Arc::clone(&map);

        let t1 = thread::spawn(move || m1.transition_to_dirty(0));
        let t2 = thread::spawn(move || m2.transition_to_dirty(1));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert_eq!(r1, DirtyTransition::FromCleanOrNotPresent);
        assert_eq!(r2, DirtyTransition::FromCleanOrNotPresent);
        assert_eq!(map.get(0), DI);
        assert_eq!(map.get(1), DI);
    });
}

/// Write during flush: transition_to_dirty vs transition_dirty_to_syncing.
#[test]
fn write_vs_flush_claim() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.cas(0, CL, DI).unwrap();

        let m_flush = Arc::clone(&map);
        let m_write = Arc::clone(&map);

        let t_flush = thread::spawn(move || m_flush.transition_dirty_to_syncing(0));
        let t_write = thread::spawn(move || m_write.transition_to_dirty(0));

        let flush_won = t_flush.join().unwrap();
        let write_result = t_write.join().unwrap();

        let state = map.get(0);

        match (flush_won, write_result) {
            (true, DirtyTransition::FromSyncing) => {
                // Flush claimed DIRTY→SYNCING, then writer re-dirtied SYNCING→DIRTY
                assert_eq!(state, DI);
            }
            (true, DirtyTransition::AlreadyDirty) => {
                // Writer saw DIRTY (no-op) before flush CAS, flush won
                assert_eq!(state, SY);
            }
            (false, DirtyTransition::AlreadyDirty) => {
                // Writer saw DIRTY (no-op), flush CAS failed — shouldn't happen
                // with no adjacent contention (flush CAS should succeed if state
                // is still DIRTY). But loom will tell us.
                assert_eq!(state, DI);
            }
            other => {
                panic!("unexpected outcome: {:?}", other);
            }
        }
    });
}

/// Eviction vs concurrent write: SYNCING→NP vs transition_to_dirty.
/// The writer must always end up with the block DIRTY (data preservation).
#[test]
fn eviction_vs_write() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.cas(0, CL, SY).unwrap();

        let m_evict = Arc::clone(&map);
        let m_write = Arc::clone(&map);

        let t_evict = thread::spawn(move || m_evict.transition_syncing_to_not_present(0));
        let t_write = thread::spawn(move || m_write.transition_to_dirty(0));

        let _evict_won = t_evict.join().unwrap();
        let write_result = t_write.join().unwrap();

        // Writer must always succeed — either SYNCING→DIRTY or NP→DIRTY
        assert!(
            matches!(
                write_result,
                DirtyTransition::FromSyncing | DirtyTransition::FromCleanOrNotPresent
            ),
            "writer must transition to DIRTY, got {:?}",
            write_result
        );
        assert_eq!(map.get(0), DI, "final state must be DIRTY — data loss!");
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

        let m_flush = Arc::clone(&map);
        let m_write = Arc::clone(&map);

        let t_flush = thread::spawn(move || {
            if m_flush.transition_dirty_to_syncing(0) {
                m_flush.transition_syncing_to_not_present(0);
            }
        });

        let t_write = thread::spawn(move || m_write.transition_to_dirty(0));

        t_flush.join().unwrap();
        let write_result = t_write.join().unwrap();

        let state = map.get(0);

        if write_result != DirtyTransition::AlreadyDirty {
            // Write actually transitioned — block must be DIRTY
            assert_eq!(state, DI, "write landed but state is not DIRTY — data loss!");
        }
    });
}

/// Two concurrent promotes of the same SYNCING block.
/// Exactly one wins SYNCING→DIRTY, the other sees AlreadyDirty.
#[test]
fn concurrent_promote_same_block() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));
        map.set_present(0);
        map.cas(0, CL, SY).unwrap();

        let m1 = Arc::clone(&map);
        let m2 = Arc::clone(&map);

        let t1 = thread::spawn(move || m1.transition_to_dirty(0));
        let t2 = thread::spawn(move || m2.transition_to_dirty(0));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert!(
            matches!(
                (r1, r2),
                (DirtyTransition::FromSyncing, DirtyTransition::AlreadyDirty)
                    | (DirtyTransition::AlreadyDirty, DirtyTransition::FromSyncing)
            ),
            "exactly one should transition SYNCING→DIRTY, got ({:?}, {:?})",
            r1, r2
        );
        assert_eq!(map.get(0), DI);
    });
}

/// Write to NOT_PRESENT block (NP→DIRTY).
/// This path fires when promote's CAS fails because flush already evicted.
#[test]
fn write_to_not_present() {
    loom::model(|| {
        let map = Arc::new(SparseStateMap::new(4));

        let result = map.transition_to_dirty(0);

        assert_eq!(result, DirtyTransition::FromCleanOrNotPresent);
        assert_eq!(map.get(0), DI);
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

        let t1 = thread::spawn(move || m1.transition_dirty_to_syncing(0));
        let t2 = thread::spawn(move || m2.transition_dirty_to_syncing(1));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        let s0 = map.get(0);
        let s1 = map.get(1);

        // Verify state matches result
        if r1 {
            assert_eq!(s0, SY);
        } else {
            assert_eq!(s0, DI, "failed claim should leave block DIRTY");
        }

        if r2 {
            assert_eq!(s1, SY);
        } else {
            assert_eq!(s1, DI, "failed claim should leave block DIRTY");
        }
    });
}
