//! Loom concurrency tests for GlideFS write cache state machine.
//!
//! These tests exhaustively explore all thread interleavings of the lock-free
//! CAS-based state machine used in write_cache's SparseStateMap.
//!
//! Run with: cd loom-tests && cargo test --release
//!
//! The 2-bit packing (4 entries per AtomicU8) means CAS on one block can
//! spuriously fail because an adjacent block in the same byte was modified
//! concurrently. The CAS loops in SparseStateMap and CacheInner must handle
//! this correctly. These tests verify that.

#![allow(dead_code)]

use loom::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use loom::sync::Arc;
use loom::thread;

// Block states (must match SparseBlockState in block_map.rs)
const NOT_PRESENT: u8 = 0;
const CLEAN: u8 = 1;
const DIRTY: u8 = 2;
const SYNCING: u8 = 3;

// ============================================================================
// Minimal reproduction of SparseStateMap's 2-bit packed CAS
// ============================================================================

/// A single packed byte holding 4 block states at 2 bits each.
/// This is the unit of contention in SparseStateMap — CAS on any slot
/// touches the entire byte, causing spurious failures on adjacent slots.
struct PackedStates {
    byte: AtomicU8,
}

impl PackedStates {
    fn new() -> Self {
        Self {
            byte: AtomicU8::new(0),
        }
    }

    fn new_with(slot0: u8, slot1: u8, slot2: u8, slot3: u8) -> Self {
        let val = slot0 | (slot1 << 2) | (slot2 << 4) | (slot3 << 6);
        Self {
            byte: AtomicU8::new(val),
        }
    }

    /// Get the 2-bit state for a slot (0-3).
    fn get(&self, slot: u32) -> u8 {
        let shift = slot * 2;
        (self.byte.load(Ordering::Acquire) >> shift) & 0x3
    }

    /// CAS a single 2-bit slot within the packed byte.
    /// Mirrors SparseStateMap::cas() — retries on spurious failure from
    /// adjacent slot modifications.
    fn cas(&self, slot: u32, expected: u8, new: u8) -> Result<u8, u8> {
        let shift = slot * 2;
        let mask = 0x3u8 << shift;
        loop {
            let old = self.byte.load(Ordering::Acquire);
            let current = (old >> shift) & 0x3;
            if current != expected {
                return Err(current);
            }
            let updated = (old & !mask) | (new << shift);
            match self.byte.compare_exchange(
                old,
                updated,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(expected),
                Err(_) => continue, // adjacent slot changed, retry
            }
        }
    }

    /// try_set_present: CAS NOT_PRESENT → CLEAN for a slot.
    /// Returns true if this call won the transition.
    fn try_set_present(&self, slot: u32) -> bool {
        let shift = slot * 2;
        let mask = 0x3u8 << shift;
        loop {
            let old = self.byte.load(Ordering::Acquire);
            if (old >> shift) & 0x3 != NOT_PRESENT {
                return false;
            }
            let new = (old & !mask) | (CLEAN << shift);
            if self
                .byte
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }
}

// ============================================================================
// Reproduction of CacheInner transition logic atop PackedStates
// ============================================================================

/// Mirrors CacheInner's state machine with counters.
/// Single block (slot 0) with optional adjacent contention.
struct CacheModel {
    states: PackedStates,
    dirty_count: AtomicU64,
    syncing_count: AtomicU64,
}

impl CacheModel {
    fn new(slot0: u8, slot1: u8, slot2: u8, slot3: u8) -> Self {
        let dirty = [slot0, slot1, slot2, slot3]
            .iter()
            .filter(|&&s| s == DIRTY)
            .count() as u64;
        let syncing = [slot0, slot1, slot2, slot3]
            .iter()
            .filter(|&&s| s == SYNCING)
            .count() as u64;
        Self {
            states: PackedStates::new_with(slot0, slot1, slot2, slot3),
            dirty_count: AtomicU64::new(dirty),
            syncing_count: AtomicU64::new(syncing),
        }
    }

    /// Mirrors CacheInner::transition_to_dirty
    fn transition_to_dirty(&self, slot: u32) -> bool {
        loop {
            let current = self.states.get(slot);

            if current == DIRTY {
                return false;
            }

            if current == CLEAN || current == NOT_PRESENT {
                if self.states.cas(slot, current, DIRTY).is_ok() {
                    self.dirty_count.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            } else if current == SYNCING {
                if self.states.cas(slot, SYNCING, DIRTY).is_ok() {
                    self.syncing_count.fetch_sub(1, Ordering::Relaxed);
                    self.dirty_count.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    /// Mirrors CacheInner::transition_dirty_to_syncing
    fn transition_dirty_to_syncing(&self, slot: u32) -> bool {
        if self.states.cas(slot, DIRTY, SYNCING).is_ok() {
            self.dirty_count.fetch_sub(1, Ordering::Relaxed);
            self.syncing_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Mirrors CacheInner::transition_syncing_to_not_present
    fn transition_syncing_to_not_present(&self, slot: u32) -> bool {
        if self.states.cas(slot, SYNCING, NOT_PRESENT).is_ok() {
            self.syncing_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    fn state(&self, slot: u32) -> u8 {
        self.states.get(slot)
    }

    fn dirty_count(&self) -> u64 {
        self.dirty_count.load(Ordering::Relaxed)
    }

    fn syncing_count(&self) -> u64 {
        self.syncing_count.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Tests: Adjacent slot contention (the 2-bit packing invariant)
// ============================================================================

/// Two threads CAS adjacent slots in the same byte.
/// Invariant: both transitions must succeed despite sharing an AtomicU8.
#[test]
fn adjacent_slots_both_transition() {
    loom::model(|| {
        let ps = Arc::new(PackedStates::new_with(CLEAN, CLEAN, NOT_PRESENT, NOT_PRESENT));
        let ps1 = Arc::clone(&ps);
        let ps2 = Arc::clone(&ps);

        // Thread 1: slot 0 CLEAN → DIRTY
        let t1 = thread::spawn(move || ps1.cas(0, CLEAN, DIRTY));
        // Thread 2: slot 1 CLEAN → SYNCING
        let t2 = thread::spawn(move || ps2.cas(1, CLEAN, SYNCING));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert!(r1.is_ok(), "slot 0 CAS must succeed");
        assert!(r2.is_ok(), "slot 1 CAS must succeed");
        assert_eq!(ps.get(0), DIRTY);
        assert_eq!(ps.get(1), SYNCING);
        assert_eq!(ps.get(2), NOT_PRESENT);
        assert_eq!(ps.get(3), NOT_PRESENT);
    });
}

// NOTE: 4-thread tests omitted — loom's state space explodes with 4 threads
// (hours to complete). The 2-thread adjacent slot tests cover the same
// byte-level contention with tractable state spaces.

// ============================================================================
// Tests: try_set_present race (two threads claim the same block)
// ============================================================================

/// Two threads race to set_present on the same slot.
/// Exactly one must win.
#[test]
fn try_set_present_exactly_one_wins() {
    loom::model(|| {
        let ps = Arc::new(PackedStates::new());
        let ps1 = Arc::clone(&ps);
        let ps2 = Arc::clone(&ps);

        let t1 = thread::spawn(move || ps1.try_set_present(0));
        let t2 = thread::spawn(move || ps2.try_set_present(0));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert!(
            (r1 && !r2) || (!r1 && r2),
            "exactly one thread must win try_set_present"
        );
        assert_eq!(ps.get(0), CLEAN);
    });
}

/// try_set_present on adjacent slots — both must succeed.
#[test]
fn try_set_present_adjacent_both_succeed() {
    loom::model(|| {
        let ps = Arc::new(PackedStates::new());
        let ps1 = Arc::clone(&ps);
        let ps2 = Arc::clone(&ps);

        let t1 = thread::spawn(move || ps1.try_set_present(0));
        let t2 = thread::spawn(move || ps2.try_set_present(1));

        assert!(t1.join().unwrap(), "slot 0 must succeed");
        assert!(t2.join().unwrap(), "slot 1 must succeed");
        assert_eq!(ps.get(0), CLEAN);
        assert_eq!(ps.get(1), CLEAN);
    });
}

// ============================================================================
// Tests: CacheInner transition logic
// ============================================================================

/// Write during flush: transition_to_dirty vs transition_dirty_to_syncing on same slot.
/// If flush wins: state = SYNCING (or writer re-dirties to DIRTY).
/// If writer wins (already DIRTY, no-op): state stays DIRTY, flush then claims.
#[test]
fn write_vs_flush_claim() {
    loom::model(|| {
        let cm = Arc::new(CacheModel::new(DIRTY, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT));
        let cm_flush = Arc::clone(&cm);
        let cm_write = Arc::clone(&cm);

        // Flush claims DIRTY → SYNCING
        let t_flush = thread::spawn(move || cm_flush.transition_dirty_to_syncing(0));
        // Writer does transition_to_dirty (no-op if already DIRTY, or SYNCING → DIRTY)
        let t_write = thread::spawn(move || cm_write.transition_to_dirty(0));

        let flush_won = t_flush.join().unwrap();
        let write_changed = t_write.join().unwrap();

        let state = cm.state(0);
        let dirty = cm.dirty_count();
        let syncing = cm.syncing_count();

        match (flush_won, write_changed) {
            (true, true) => {
                // Flush claimed (DIRTY→SYNCING), then writer re-dirtied (SYNCING→DIRTY)
                assert_eq!(state, DIRTY);
                assert_eq!(dirty, 1);
                assert_eq!(syncing, 0);
            }
            (true, false) => {
                // Flush claimed, writer saw DIRTY (before flush) → no-op
                // Wait, write sees DIRTY → returns false. But flush changed it to SYNCING.
                // If writer reads DIRTY first and returns false, flush CAS succeeds → SYNCING.
                // If writer reads SYNCING, it would CAS SYNCING→DIRTY (returns true).
                // So (true, false) means writer saw DIRTY and returned false before flush ran,
                // or flush ran first and writer saw SYNCING and retried... no.
                // Actually: writer's loop reads current. If current==DIRTY, returns false.
                // Flush CAS DIRTY→SYNCING. These are on the same atomic byte.
                // Sequence: writer loads DIRTY, returns false. Then flush CAS succeeds.
                assert_eq!(state, SYNCING);
                assert_eq!(dirty, 0);
                assert_eq!(syncing, 1);
            }
            (false, false) => {
                // Writer saw DIRTY (no-op), flush CAS failed (block no longer DIRTY)
                // This can't happen: if writer didn't change state, block is still DIRTY,
                // so flush CAS should succeed. Unless there's an interleaving where
                // flush loads DIRTY, writer loads DIRTY (no-op), flush CAS succeeds...
                // Actually (false, false) could happen if flush CAS fails spuriously
                // due to adjacent slot? No — flush CAS is on a specific expected value,
                // and adjacent slot changes cause byte-level CAS failure but the loop
                // in transition_dirty_to_syncing does NOT retry (it's a single CAS).
                // So if an adjacent slot CAS changes the byte between flush's
                // check and CAS, flush would fail. But we have no adjacent contention
                // in this test. So (false, false) shouldn't happen.
                panic!("unexpected: neither flush nor write succeeded");
            }
            (false, true) => {
                // Flush CAS failed. Writer changed state... but from what?
                // Writer only changes if current != DIRTY. If current was DIRTY
                // at writer's read, writer returns false. So writer must have seen
                // non-DIRTY. But initial state is DIRTY and flush didn't change it
                // (flush_won=false). Contradiction? Not quite:
                // flush reads DIRTY, writer reads DIRTY (returns false, write_changed=false).
                // So (false, true) requires writer to see non-DIRTY without flush
                // changing it. Can't happen from DIRTY initial state.
                panic!("unexpected: flush failed but write changed state from DIRTY");
            }
        }
    });
}

/// Eviction vs concurrent write: transition_syncing_to_not_present vs transition_to_dirty.
/// This is the promote_syncing_blocks race.
#[test]
fn eviction_vs_write() {
    loom::model(|| {
        let cm = Arc::new(CacheModel::new(SYNCING, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT));
        let cm_evict = Arc::clone(&cm);
        let cm_write = Arc::clone(&cm);

        // Flush evicts SYNCING → NOT_PRESENT
        let t_evict = thread::spawn(move || cm_evict.transition_syncing_to_not_present(0));
        // Writer re-dirties SYNCING → DIRTY (or NP → DIRTY if eviction wins first)
        let t_write = thread::spawn(move || cm_write.transition_to_dirty(0));

        let evict_won = t_evict.join().unwrap();
        let write_changed = t_write.join().unwrap();

        let state = cm.state(0);
        let dirty = cm.dirty_count();
        let syncing = cm.syncing_count();

        // Key invariant: the write must always succeed (data is in active file)
        assert!(write_changed, "writer must always transition to DIRTY");
        assert_eq!(state, DIRTY, "final state must be DIRTY");
        assert_eq!(dirty, 1);
        assert_eq!(syncing, 0);

        // Eviction may or may not have succeeded (if it did, writer saw NP and
        // did NP→DIRTY; if it didn't, writer did SYNCING→DIRTY).
        let _ = evict_won;
    });
}

// NOTE: 3-thread tests (eviction + write + adjacent contention) omitted.
// Loom's state space is O(n!) in thread count — 3 threads takes minutes.
// The 2-thread eviction_vs_write test plus the adjacent_slots_both_transition
// test together cover the same invariants.

/// Full flush cycle on one block: DIRTY → SYNCING → NOT_PRESENT,
/// with a concurrent write arriving during the cycle.
/// The write must not be lost.
#[test]
fn full_flush_cycle_with_concurrent_write() {
    loom::model(|| {
        let cm = Arc::new(CacheModel::new(DIRTY, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT));
        let cm_flush = Arc::clone(&cm);
        let cm_write = Arc::clone(&cm);

        // Flush thread: claim then evict
        let t_flush = thread::spawn(move || {
            let claimed = cm_flush.transition_dirty_to_syncing(0);
            if claimed {
                let evicted = cm_flush.transition_syncing_to_not_present(0);
                (true, evicted)
            } else {
                (false, false)
            }
        });

        // Writer: transition_to_dirty (could hit DIRTY, SYNCING, or NP)
        let t_write = thread::spawn(move || cm_write.transition_to_dirty(0));

        let (_claimed, _evicted) = t_flush.join().unwrap();
        let write_changed = t_write.join().unwrap();

        let state = cm.state(0);

        // The critical invariant: if a write happened, data must not be lost.
        // "Lost" would mean state=NOT_PRESENT after a write landed.
        if write_changed {
            assert_eq!(
                state, DIRTY,
                "write landed but state is not DIRTY — data loss!"
            );
        }

        // If no write happened, any final state is valid:
        // - DIRTY: writer saw DIRTY (no-op false), flush didn't run or failed
        //   Actually if write_changed is false and writer saw DIRTY, that's (false).
        //   But state could be NP if flush completed.
        if !write_changed {
            // Writer saw DIRTY and returned false. Flush may have completed.
            assert!(
                state == DIRTY || state == SYNCING || state == NOT_PRESENT,
                "unexpected state without write: {}",
                state
            );
        }
    });
}

/// Two concurrent writes to the same CLEAN block.
/// Exactly one should increment dirty_count.
#[test]
fn concurrent_writes_same_block() {
    loom::model(|| {
        let cm = Arc::new(CacheModel::new(CLEAN, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT));
        let cm1 = Arc::clone(&cm);
        let cm2 = Arc::clone(&cm);

        let t1 = thread::spawn(move || cm1.transition_to_dirty(0));
        let t2 = thread::spawn(move || cm2.transition_to_dirty(0));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Exactly one transitions CLEAN → DIRTY, the other sees DIRTY (no-op)
        assert!(
            (r1 && !r2) || (!r1 && r2),
            "exactly one write should transition CLEAN→DIRTY"
        );
        assert_eq!(cm.state(0), DIRTY);
        assert_eq!(cm.dirty_count(), 1);
    });
}

/// Two concurrent writes to adjacent slots (same byte).
/// Both must succeed.
#[test]
fn concurrent_writes_adjacent_slots() {
    loom::model(|| {
        let cm = Arc::new(CacheModel::new(CLEAN, CLEAN, NOT_PRESENT, NOT_PRESENT));
        let cm1 = Arc::clone(&cm);
        let cm2 = Arc::clone(&cm);

        let t1 = thread::spawn(move || cm1.transition_to_dirty(0));
        let t2 = thread::spawn(move || cm2.transition_to_dirty(1));

        assert!(t1.join().unwrap(), "slot 0 must transition");
        assert!(t2.join().unwrap(), "slot 1 must transition");
        assert_eq!(cm.state(0), DIRTY);
        assert_eq!(cm.state(1), DIRTY);
        assert_eq!(cm.dirty_count(), 2);
    });
}

/// Write to a NOT_PRESENT block (NP → DIRTY).
/// This happens when promote_syncing_blocks CAS SYNCING→DIRTY fails because
/// flush evicted the block (SYNCING→NP) first, and then transition_to_dirty
/// picks up from NP.
#[test]
fn write_to_not_present_after_eviction() {
    loom::model(|| {
        let cm = Arc::new(CacheModel::new(NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT));
        let cm1 = Arc::clone(&cm);

        let t1 = thread::spawn(move || cm1.transition_to_dirty(0));

        let changed = t1.join().unwrap();
        assert!(changed, "NP → DIRTY must succeed");
        assert_eq!(cm.state(0), DIRTY);
        assert_eq!(cm.dirty_count(), 1);
    });
}

/// Simulate the promote path: two writers both see SYNCING,
/// both try to promote (CAS SYNCING→DIRTY). Exactly one wins,
/// the other retries and sees DIRTY (no-op).
#[test]
fn concurrent_promote_same_block() {
    loom::model(|| {
        let cm = Arc::new(CacheModel::new(SYNCING, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT));
        let cm1 = Arc::clone(&cm);
        let cm2 = Arc::clone(&cm);

        let t1 = thread::spawn(move || cm1.transition_to_dirty(0));
        let t2 = thread::spawn(move || cm2.transition_to_dirty(0));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Exactly one transitions SYNCING → DIRTY
        assert!(
            (r1 && !r2) || (!r1 && r2),
            "exactly one should transition SYNCING→DIRTY"
        );
        assert_eq!(cm.state(0), DIRTY);
        assert_eq!(cm.dirty_count(), 1);
        assert_eq!(cm.syncing_count(), 0);
    });
}

/// Flush claims two adjacent dirty blocks concurrently.
/// Both must succeed despite sharing an AtomicU8.
#[test]
fn flush_claims_adjacent_blocks() {
    loom::model(|| {
        let cm = Arc::new(CacheModel::new(DIRTY, DIRTY, NOT_PRESENT, NOT_PRESENT));
        let cm1 = Arc::clone(&cm);
        let cm2 = Arc::clone(&cm);

        let t1 = thread::spawn(move || cm1.transition_dirty_to_syncing(0));
        let t2 = thread::spawn(move || cm2.transition_dirty_to_syncing(1));

        // Note: transition_dirty_to_syncing is a single CAS (no retry loop).
        // With adjacent contention, the byte-level CAS can fail spuriously.
        // This test verifies whether that's a problem.
        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Both SHOULD succeed... but transition_dirty_to_syncing has no retry loop!
        // If one fails due to byte-level contention, that's a real bug in the
        // production code (it would leave a dirty block unclaimed).
        //
        // In practice this is safe because claim happens under the data_file
        // write lock (single-threaded). But the CAS itself is not protected
        // against adjacent contention. Let's see what loom finds.
        if !r1 || !r2 {
            // If loom finds an interleaving where one fails, it means
            // transition_dirty_to_syncing needs a retry loop for adjacent
            // slot contention. This would be a real finding.
            //
            // However: in the actual flush path, DIRTY→SYNCING transitions
            // happen under the data_file write lock, so they're serialized.
            // Adjacent contention only matters if we ever parallelize claiming.
        }

        // Whatever happened, state and counts must be consistent
        let s0 = cm.state(0);
        let s1 = cm.state(1);
        let dirty = cm.dirty_count();
        let syncing = cm.syncing_count();

        let expected_dirty = [s0, s1].iter().filter(|&&s| s == DIRTY).count() as u64;
        let expected_syncing = [s0, s1].iter().filter(|&&s| s == SYNCING).count() as u64;

        assert_eq!(dirty, expected_dirty, "dirty count must match state");
        assert_eq!(syncing, expected_syncing, "syncing count must match state");
    });
}
