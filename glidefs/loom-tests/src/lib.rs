//! Loom concurrency tests for GlideFS write cache state machine.
//!
//! These tests verify the lock-free CAS-based state machine used in write_cache.rs
//! by exploring all possible thread interleavings.
//!
//! Run with: cargo test --release
//!
//! The tests mirror the actual CAS logic from write_cache.rs:
//! - Clean → Dirty (write path, lines 821-836)
//! - Syncing → Dirty (write during sync, lines 838-855)
//! - Dirty → Syncing (claim_dirty_blocks, lines 1270-1282)
//! - Syncing → Clean (mark_synced, lines 1300-1310)
//! - Syncing → Dirty (mark_sync_failed, lines 1321-1334)
//! - Presence bitmap CAS (prefetch race, lines 1013-1029)

// Test infrastructure used by loom::model closures
#![allow(dead_code)]
#![allow(unused_imports)]

use loom::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use loom::sync::Mutex;
use loom::thread;
use std::collections::VecDeque;
use std::sync::Arc;

// Block states (must match BlockState enum values in write_cache.rs)
const CLEAN: u8 = 0;
const DIRTY: u8 = 1;
const SYNCING: u8 = 2;

/// Mock queue for loom testing (crossbeam SegQueue doesn't support loom).
#[derive(Debug)]
struct MockSegQueue<T> {
    inner: Mutex<VecDeque<T>>,
}

impl<T> MockSegQueue<T> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
        }
    }

    fn push(&self, value: T) {
        self.inner.lock().unwrap().push_back(value);
    }

    fn pop(&self) -> Option<T> {
        self.inner.lock().unwrap().pop_front()
    }

    fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

/// Minimal state machine for testing CAS transitions.
/// Reproduces the lock-free logic from CacheInner without file I/O.
struct TestStateMachine {
    block_state: AtomicU8,
    dirty_count: AtomicU64,
    syncing_count: AtomicU64,
    dirty_queue: MockSegQueue<u64>,
}

impl TestStateMachine {
    fn new(initial_state: u8) -> Self {
        let dirty_count = if initial_state == DIRTY { 1 } else { 0 };
        let syncing_count = if initial_state == SYNCING { 1 } else { 0 };
        Self {
            block_state: AtomicU8::new(initial_state),
            dirty_count: AtomicU64::new(dirty_count),
            syncing_count: AtomicU64::new(syncing_count),
            dirty_queue: MockSegQueue::new(),
        }
    }

    /// Write path: transition to Dirty (from Clean or Syncing)
    /// Mirrors write_cache.rs lines 812-860
    fn write(&self) -> bool {
        loop {
            let current = self.block_state.load(Ordering::Acquire);

            if current == DIRTY {
                // Already dirty, nothing to do
                return false;
            }

            if current == CLEAN {
                // Clean → Dirty
                if self
                    .block_state
                    .compare_exchange(current, DIRTY, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.dirty_count.fetch_add(1, Ordering::Relaxed);
                    self.dirty_queue.push(0);
                    return true;
                }
                // CAS failed, retry
            } else if current == SYNCING {
                // Syncing → Dirty (write during sync)
                if self
                    .block_state
                    .compare_exchange(current, DIRTY, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.syncing_count.fetch_sub(1, Ordering::Relaxed);
                    self.dirty_count.fetch_add(1, Ordering::Relaxed);
                    self.dirty_queue.push(0);
                    return true;
                }
                // CAS failed, retry
            } else {
                // Unknown state
                return false;
            }
        }
    }

    /// Claim dirty block for sync: Dirty → Syncing
    /// Mirrors write_cache.rs lines 1270-1282
    fn claim_dirty(&self) -> bool {
        if self.dirty_queue.pop().is_none() {
            return false;
        }

        if self
            .block_state
            .compare_exchange(DIRTY, SYNCING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.dirty_count.fetch_sub(1, Ordering::Relaxed);
            self.syncing_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            // Block is no longer dirty (write converted it back, etc.)
            false
        }
    }

    /// Mark block as synced: Syncing → Clean
    /// Mirrors write_cache.rs lines 1300-1310
    fn mark_synced(&self) -> bool {
        if self
            .block_state
            .compare_exchange(SYNCING, CLEAN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.syncing_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Mark sync as failed: Syncing → Dirty (re-queue for retry)
    /// Mirrors write_cache.rs lines 1321-1334
    fn mark_sync_failed(&self) -> bool {
        if self
            .block_state
            .compare_exchange(SYNCING, DIRTY, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.syncing_count.fetch_sub(1, Ordering::Relaxed);
            self.dirty_count.fetch_add(1, Ordering::Relaxed);
            self.dirty_queue.push(0);
            true
        } else {
            false
        }
    }

    fn state(&self) -> u8 {
        self.block_state.load(Ordering::Acquire)
    }

    fn dirty_count(&self) -> u64 {
        self.dirty_count.load(Ordering::Relaxed)
    }

    fn syncing_count(&self) -> u64 {
        self.syncing_count.load(Ordering::Relaxed)
    }
}

/// Test concurrent writes both trying to transition Clean → Dirty.
/// Invariant: Exactly one write should succeed in incrementing dirty_count.
#[test]
fn test_clean_to_dirty_concurrent() {
    loom::model(|| {
        let sm = Arc::new(TestStateMachine::new(CLEAN));
        let sm1 = Arc::clone(&sm);
        let sm2 = Arc::clone(&sm);

        let t1 = thread::spawn(move || sm1.write());
        let t2 = thread::spawn(move || sm2.write());

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Exactly one should have transitioned
        assert!(
            (r1 && !r2) || (!r1 && r2),
            "Exactly one write should transition Clean→Dirty"
        );
        assert_eq!(sm.state(), DIRTY);
        assert_eq!(sm.dirty_count(), 1);
        assert_eq!(sm.syncing_count(), 0);
    });
}

/// Test write during sync: one thread syncing, another writing.
/// Possible outcomes:
/// 1. Write wins CAS: Syncing → Dirty (sync fails)
/// 2. Sync wins CAS: Syncing → Clean, then write may transition Clean → Dirty
#[test]
fn test_write_during_sync() {
    loom::model(|| {
        // Start in Syncing state (simulates sync worker claimed the block)
        let sm = Arc::new(TestStateMachine::new(SYNCING));
        let sm_write = Arc::clone(&sm);
        let sm_sync = Arc::clone(&sm);

        // Writer tries to dirty the block
        let t_write = thread::spawn(move || sm_write.write());

        // Sync worker tries to mark as synced
        let t_sync = thread::spawn(move || sm_sync.mark_synced());

        let write_won = t_write.join().unwrap();
        let sync_won = t_sync.join().unwrap();

        let state = sm.state();
        let dirty = sm.dirty_count();
        let syncing = sm.syncing_count();

        // Valid outcomes:
        // 1. write_won && !sync_won: Write interrupted sync (Syncing → Dirty)
        // 2. !write_won && sync_won: Sync completed, write was no-op (saw Dirty? No, saw Syncing and lost)
        // 3. sync_won && write_won: Sync completed (Syncing → Clean), then write ran (Clean → Dirty)
        //
        // Case 3 is valid: sync finishes first, then write sees Clean and transitions to Dirty

        if write_won && !sync_won {
            // Write interrupted sync: Syncing → Dirty
            assert_eq!(state, DIRTY, "Write won: state should be Dirty");
            assert_eq!(dirty, 1);
            assert_eq!(syncing, 0);
        } else if !write_won && sync_won {
            // Sync completed, write saw Syncing and lost CAS
            assert_eq!(state, CLEAN, "Sync won, write lost: state should be Clean");
            assert_eq!(dirty, 0);
            assert_eq!(syncing, 0);
        } else if sync_won && write_won {
            // Sync completed first (Syncing → Clean), then write ran (Clean → Dirty)
            assert_eq!(state, DIRTY, "Both succeeded sequentially: state should be Dirty");
            assert_eq!(dirty, 1);
            assert_eq!(syncing, 0);
        } else {
            // Neither won - shouldn't happen with these two operations
            panic!("Unexpected: neither write nor sync succeeded");
        }
    });
}

/// Test claiming dirty blocks with concurrent writes.
/// The claim should either succeed or the block should end up dirty.
#[test]
fn test_claim_dirty_block() {
    loom::model(|| {
        let sm = Arc::new(TestStateMachine::new(DIRTY));
        // Pre-populate queue as write() does
        sm.dirty_queue.push(0);

        let sm_claim = Arc::clone(&sm);
        let sm_write = Arc::clone(&sm);

        let t_claim = thread::spawn(move || sm_claim.claim_dirty());
        let t_write = thread::spawn(move || sm_write.write());

        let claim_result = t_claim.join().unwrap();
        let write_result = t_write.join().unwrap();

        // Final state must be consistent
        let state = sm.state();
        let dirty = sm.dirty_count();
        let syncing = sm.syncing_count();

        match (claim_result, write_result) {
            (true, false) => {
                // Claim won, write saw Dirty (no-op) or Syncing
                assert!(state == SYNCING || state == DIRTY);
            }
            (true, true) => {
                // Claim won, then write converted Syncing → Dirty
                assert_eq!(state, DIRTY);
            }
            (false, _) => {
                // Claim failed (state wasn't Dirty when CAS ran)
                // Block could be Clean, Dirty, or Syncing depending on interleaving
            }
        }

        // Key invariant: counts must match state
        if state == DIRTY {
            assert!(dirty >= 1, "Dirty state must have dirty_count >= 1");
        }
        if state == SYNCING {
            assert_eq!(syncing, 1, "Syncing state must have syncing_count == 1");
        }
        if state == CLEAN {
            assert_eq!(dirty, 0);
            assert_eq!(syncing, 0);
        }
    });
}

/// Test mark_synced with concurrent write.
/// Invariant: Block should end up either Clean (sync won) or Dirty (write won).
#[test]
fn test_mark_synced() {
    loom::model(|| {
        let sm = Arc::new(TestStateMachine::new(SYNCING));
        let sm_sync = Arc::clone(&sm);
        let sm_write = Arc::clone(&sm);

        let t_sync = thread::spawn(move || sm_sync.mark_synced());
        let t_write = thread::spawn(move || sm_write.write());

        let sync_won = t_sync.join().unwrap();
        let write_won = t_write.join().unwrap();

        // At most one should win the CAS (both can fail if interleaved weirdly,
        // but in practice with 2 threads, one will win)
        let state = sm.state();

        if sync_won {
            // Sync completed first: Syncing → Clean
            // Write either did nothing (saw Clean) or saw Syncing and lost
            if !write_won {
                assert_eq!(state, CLEAN);
            } else {
                // Write won after sync - shouldn't happen since sync transitions to Clean
                // and write on Clean → Dirty
                assert_eq!(state, DIRTY);
            }
        } else if write_won {
            // Write won: Syncing → Dirty
            assert_eq!(state, DIRTY);
        }

        // Invariant: dirty + syncing counts match state
        let dirty = sm.dirty_count();
        let syncing = sm.syncing_count();
        match state {
            CLEAN => {
                assert_eq!(dirty, 0);
                assert_eq!(syncing, 0);
            }
            DIRTY => {
                assert!(dirty >= 1);
                assert_eq!(syncing, 0);
            }
            SYNCING => {
                assert_eq!(syncing, 1);
            }
            _ => panic!("Invalid state"),
        }
    });
}

/// Test sync failure with re-queue and concurrent write.
/// Invariant: After failure, block must be Dirty and re-queued.
#[test]
fn test_sync_failed_requeue() {
    loom::model(|| {
        let sm = Arc::new(TestStateMachine::new(SYNCING));
        let sm_fail = Arc::clone(&sm);
        let sm_write = Arc::clone(&sm);

        let t_fail = thread::spawn(move || sm_fail.mark_sync_failed());
        let t_write = thread::spawn(move || sm_write.write());

        let fail_won = t_fail.join().unwrap();
        let write_won = t_write.join().unwrap();

        // Both try Syncing → Dirty, exactly one should win
        assert!(
            (fail_won && !write_won) || (!fail_won && write_won),
            "Exactly one should transition Syncing→Dirty"
        );

        // Final state must be Dirty
        assert_eq!(sm.state(), DIRTY);
        assert_eq!(sm.dirty_count(), 1);
        assert_eq!(sm.syncing_count(), 0);

        // Block must be in queue for retry
        assert!(!sm.dirty_queue.is_empty(), "Block must be re-queued");
    });
}

/// Minimal presence bitmap CAS test.
/// Tests the race between write (setting present) and S3 fetch (setting present).
#[test]
fn test_presence_bitmap_race() {
    loom::model(|| {
        let present = Arc::new(AtomicU64::new(0));
        let p1 = Arc::clone(&present);
        let p2 = Arc::clone(&present);

        let bit_mask: u64 = 1 << 5; // Testing bit 5

        // Simulate two threads racing to set the same presence bit
        let t1 = thread::spawn(move || {
            loop {
                let old = p1.load(Ordering::Acquire);
                if (old & bit_mask) != 0 {
                    return false; // Someone else set it
                }
                if p1
                    .compare_exchange(old, old | bit_mask, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true; // We set it
                }
                // Retry
            }
        });

        let t2 = thread::spawn(move || {
            loop {
                let old = p2.load(Ordering::Acquire);
                if (old & bit_mask) != 0 {
                    return false;
                }
                if p2
                    .compare_exchange(old, old | bit_mask, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
            }
        });

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Exactly one should win
        assert!(
            (r1 && !r2) || (!r1 && r2),
            "Exactly one thread should set the presence bit"
        );

        // Bit must be set
        assert_ne!(present.load(Ordering::Relaxed) & bit_mask, 0);
    });
}

// ============================================================================
// SeqLock tests — AtomicBlockMap read/write protocol
// ============================================================================
//
// Mirrors the SeqLock protocol in block_map.rs AtomicBlockMap::get()/set().
// The SeqLock stores (hash_lo, hash_hi, sequence) behind a version counter.
// Writers increment version to odd, write data, increment to even.
// Readers spin-retry if version is odd or changed between read start/end.
//
// The critical invariant: a reader must never observe a torn read — it must
// see either the complete old value or the complete new value, never a mix.
// Getting the memory orderings wrong on ARM allows data loads to leak outside
// the version-check window, producing a "consistent" version but garbage data.

/// Minimal SeqLock entry matching the block_map.rs protocol.
struct SeqLockEntry {
    version: AtomicU32,
    hash_lo: AtomicU64,
    hash_hi: AtomicU64,
    sequence: AtomicU64,
}

impl SeqLockEntry {
    fn new(lo: u64, hi: u64, seq: u64) -> Self {
        SeqLockEntry {
            version: AtomicU32::new(0),
            hash_lo: AtomicU64::new(lo),
            hash_hi: AtomicU64::new(hi),
            sequence: AtomicU64::new(seq),
        }
    }

    /// Reader — mirrors AtomicBlockMap::get() exactly.
    fn read(&self) -> (u64, u64, u64) {
        loop {
            let v1 = self.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                loom::thread::yield_now();
                continue;
            }
            let lo = self.hash_lo.load(Ordering::Relaxed);
            let hi = self.hash_hi.load(Ordering::Relaxed);
            let seq = self.sequence.load(Ordering::Relaxed);
            loom::sync::atomic::fence(Ordering::Acquire);
            let v2 = self.version.load(Ordering::Relaxed);
            if v1 == v2 {
                return (lo, hi, seq);
            }
            loom::thread::yield_now();
        }
    }

    /// Writer — mirrors AtomicBlockMap::set() exactly.
    fn write(&self, lo: u64, hi: u64, seq: u64) {
        self.version.fetch_add(1, Ordering::AcqRel);
        self.hash_lo.store(lo, Ordering::Release);
        self.hash_hi.store(hi, Ordering::Release);
        self.sequence.store(seq, Ordering::Release);
        self.version.fetch_add(1, Ordering::Release);
    }
}

/// One writer, one reader. Reader must see either the old value or the new
/// value — never a mix of fields from different writes.
#[test]
fn test_seqlock_no_torn_read() {
    loom::model(|| {
        // Initial: (0xAAAA, 0xBBBB, 1)
        let entry = Arc::new(SeqLockEntry::new(0xAAAA, 0xBBBB, 1));

        let writer = Arc::clone(&entry);
        let reader = Arc::clone(&entry);

        // Writer overwrites with (0xCCCC, 0xDDDD, 2)
        let t_write = thread::spawn(move || {
            writer.write(0xCCCC, 0xDDDD, 2);
        });

        // Reader reads concurrently
        let t_read = thread::spawn(move || reader.read());

        t_write.join().unwrap();
        let (lo, hi, seq) = t_read.join().unwrap();

        // Must be one of the two complete values — no mixing.
        let is_old = lo == 0xAAAA && hi == 0xBBBB && seq == 1;
        let is_new = lo == 0xCCCC && hi == 0xDDDD && seq == 2;
        assert!(
            is_old || is_new,
            "Torn read detected! lo=0x{lo:X}, hi=0x{hi:X}, seq={seq}. \
             Expected (0xAAAA, 0xBBBB, 1) or (0xCCCC, 0xDDDD, 2)"
        );
    });
}

// NOTE: No concurrent multi-writer test — SeqLock is single-writer by design.
// Two concurrent writers both increment the version counter, breaking the
// "odd = write in progress" invariant (A→1, B→2 makes it even mid-write).
// In production, writes to the same chunk are serialized by the write-cache
// lock, so concurrent writers to the same SeqLock entry never happen.

/// Writer overwrites same entry twice, reader must see one of three complete
/// snapshots (initial, first write, second write). Tests that back-to-back
/// writes don't create a window where a partial first write is visible.
#[test]
fn test_seqlock_double_write() {
    loom::model(|| {
        let entry = Arc::new(SeqLockEntry::new(0x1111, 0x2222, 1));

        let writer = Arc::clone(&entry);
        let reader = Arc::clone(&entry);

        let t_write = thread::spawn(move || {
            writer.write(0xAAAA, 0xBBBB, 2);
            writer.write(0xCCCC, 0xDDDD, 3);
        });

        let t_read = thread::spawn(move || reader.read());

        t_write.join().unwrap();
        let (lo, hi, seq) = t_read.join().unwrap();

        let is_initial = lo == 0x1111 && hi == 0x2222 && seq == 1;
        let is_first = lo == 0xAAAA && hi == 0xBBBB && seq == 2;
        let is_second = lo == 0xCCCC && hi == 0xDDDD && seq == 3;
        assert!(
            is_initial || is_first || is_second,
            "Torn read detected! lo=0x{lo:X}, hi=0x{hi:X}, seq={seq}. \
             Expected one of: (0x1111,0x2222,1), (0xAAAA,0xBBBB,2), (0xCCCC,0xDDDD,3)"
        );
    });
}
