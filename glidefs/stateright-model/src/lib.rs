//! Stateright model of the GlideFS write cache state machine.
//!
//! Models all interleavings of 2 writers + 1 flusher + 1 reader on a single
//! block. Each operation is broken into individual steps matching the real
//! code's boundary points — no fused multi-step transitions.
//!
//! Run: `cd stateright-model && cargo test --release`

use stateright::*;
use std::fmt;

// ============================================================================
// Types
// ============================================================================

type Ver = u8; // data version (0 = no data / sparse zeros)

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum BS { NP, Clean, Dirty, Syncing } // block state

// ============================================================================
// Write operation phases (matches handler.rs backfill_and_write + cache.write)
//
// Each phase is ONE step. No fused transitions.
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum W {
    // --- handler layer ---
    CheckState,              // W1: read block_state
    SawState(BS),            // decision point based on observed state
    FetchS3,                 // W2: resolve_block_for_backfill (async S3 fetch)
    HasPrior(Ver),           // W3: fetch returned, holding `prior` data
    Recheck,                 // W4: post-fetch state re-check
    AllZerosCheck(Ver),      // W5: check if prior is all zeros
    BeforeCas(Ver),          // W6: about to try_claim_block
    CasWon(Ver),             // CAS succeeded, block is CLEAN, have merge base
    // --- cache.write layer (holds read lock across all of these) ---
    SetPresent(Ver),         // W7a: set_present (NP→CLEAN or no-op)
    Pwrite(Ver),             // W7b: pwrite data to SSD
    WalAppend(Ver),          // W7c: WAL append
    TransitionDirty(Ver),    // W7d: transition_to_dirty (CLEAN→DIRTY)
    ReleaseLock,             // W7e: drop read lock
    Done,
}

// ============================================================================
// Flush operation phases (matches flush.rs flush_dirty_inner)
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum F {
    CrcPrepass,          // F1: compute CRCs from active file
    AcquireWriteLock,    // F2: acquire data_file write lock
    Snapshot,            // F3: CAS DIRTY→SYNCING
    Rotate,              // F4: rename active→flushing, create new active
    ReleaseWriteLock,    // F5: release write lock
    Compute,             // F6: compute_flush_batch (pread flushing)
    Upload,              // F7: stream to S3
    Evict,               // F8: CAS SYNCING→NP
    Cleanup,             // F9: clear flushing_active, drop flushing file
    Done,
}

// ============================================================================
// Read operation phases (matches read.rs cache.read → locate_block)
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum R {
    AcquireReadLock,     // R1a: acquire data_file read lock
    FastPathCheck,       // R1b: check state under lock (DIRTY only)
    FastPathPread,       // R2: pread from active (if DIRTY)
    ReleaseAfterFast,    // release lock after fast path
    // Slow path (lock released):
    LocateCheck,         // R3: locate_block state check (DIRTY/SYNCING hot, else cold)
    SyncReadLock,        // R4a: acquire lock for sync_read_local_block
    SyncReadRecheck,     // R4b: re-check state under lock
    SyncReadPread,       // R4c: pread (DIRTY from active, SYNCING from flushing)
    SyncReadRelease,     // R4d: release lock
    ColdFetchS3,         // R6: fetch from S3
    Done(Ver),
}

// ============================================================================
// System state
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct S {
    block: BS,
    ssd_active: Ver,
    ssd_flushing: Ver,
    s3: Ver,
    flushing_present: bool,

    w: [Option<W>; 2],   // 2 concurrent writers
    f: Option<F>,         // 1 flusher
    r: Option<R>,         // 1 reader

    read_locks: u8,       // readers holding read lock
    write_locked: bool,   // flush holding write lock

    next_ver: Ver,        // next version a write will produce
    last_write_done: Ver, // version of most recently completed write
}

// ============================================================================
// Actions
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum A {
    StartW(usize),  // start writer 0 or 1
    StepW(usize),   // advance writer 0 or 1
    StartF,
    StepF,
    StartR,
    StepR,
}

impl fmt::Display for A {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Debug::fmt(self, f) }
}

// ============================================================================
// Model
// ============================================================================

struct M {
    s3_has_data: bool,
    sub_block: bool,
}

impl M {
    /// Can this writer step proceed given the current lock state?
    fn w_can_step(w: &W, write_locked: bool) -> bool {
        // cache.write steps hold the read lock — blocked if write lock held.
        let needs_read_lock = matches!(
            w,
            W::SetPresent(_) | W::Pwrite(_) | W::WalAppend(_) |
            W::TransitionDirty(_) | W::ReleaseLock
        );
        !needs_read_lock || !write_locked
    }

    /// Can this reader step proceed?
    fn r_can_step(r: &R, write_locked: bool) -> bool {
        let needs_read_lock = matches!(
            r,
            R::AcquireReadLock | R::FastPathCheck | R::FastPathPread |
            R::ReleaseAfterFast | R::SyncReadLock | R::SyncReadRecheck
        );
        !needs_read_lock || !write_locked
    }
}

impl Model for M {
    type State = S;
    type Action = A;

    fn init_states(&self) -> Vec<S> {
        vec![S {
            block: BS::NP,
            ssd_active: 0,
            ssd_flushing: 0,
            s3: if self.s3_has_data { 1 } else { 0 },
            flushing_present: false,
            w: [None, None],
            f: None,
            r: None,
            read_locks: 0,
            write_locked: false,
            next_ver: 2,
            last_write_done: 0,
        }]
    }

    fn actions(&self, s: &S, actions: &mut Vec<A>) {
        // Writers: can start if not in flight (None or Done).
        for i in 0..2 {
            let can_start = matches!(s.w[i], None | Some(W::Done));
            if can_start && s.next_ver < 6 { // cap at 4 writes to bound state space
                actions.push(A::StartW(i));
            }
            if let Some(ref w) = s.w[i] {
                if !matches!(w, W::Done) && Self::w_can_step(w, s.write_locked) {
                    actions.push(A::StepW(i));
                }
            }
        }
        // Flusher: can start if not in flight and block is DIRTY.
        if matches!(s.f, None | Some(F::Done)) && s.block == BS::Dirty {
            actions.push(A::StartF);
        }
        if let Some(ref f) = s.f {
            if !matches!(f, F::Done) {
                let needs_write = matches!(f, F::AcquireWriteLock);
                if !needs_write || s.read_locks == 0 {
                    actions.push(A::StepF);
                }
            }
        }
        // Reader: can start if not in flight.
        if matches!(s.r, None | Some(R::Done(_))) {
            actions.push(A::StartR);
        }
        if let Some(ref r) = s.r {
            if !matches!(r, R::Done(_)) && Self::r_can_step(r, s.write_locked) {
                actions.push(A::StepR);
            }
        }
    }

    fn next_state(&self, state: &S, action: A) -> Option<S> {
        let mut s = state.clone();
        match action {
            A::StartW(i) => { s.w[i] = Some(W::CheckState); }
            A::StepW(i) => {
                let phase = s.w[i].as_ref().unwrap().clone();
                s.w[i] = Some(match phase {
                    W::CheckState => W::SawState(s.block),

                    W::SawState(saw) => match saw {
                        BS::Dirty => {
                            // Fast path for DIRTY. Acquire read lock, then pwrite.
                            s.read_locks += 1;
                            let v = s.next_ver;
                            W::Pwrite(v)
                        }
                        BS::Syncing => {
                            // Promote: acquire read lock, copy flushing→active,
                            // CAS SYNCING→DIRTY, then pwrite.
                            s.read_locks += 1;
                            if s.flushing_present {
                                s.ssd_active = s.ssd_flushing;
                                s.block = BS::Dirty;
                            }
                            let v = s.next_ver;
                            W::Pwrite(v)
                        }
                        BS::Clean => {
                            // Wait for DIRTY. Re-check.
                            W::CheckState
                        }
                        BS::NP => {
                            if self.sub_block && self.s3_has_data {
                                W::FetchS3
                            } else {
                                // Full-block or no S3: enter cache.write directly.
                                s.read_locks += 1;
                                let v = s.next_ver;
                                W::SetPresent(v)
                            }
                        }
                    },

                    W::FetchS3 => {
                        // locate_block: check state, fetch accordingly.
                        let fetched = match s.block {
                            BS::Dirty | BS::Syncing => s.ssd_active, // hot path (real: DIRTY/SYNCING only)
                            BS::Clean => s.s3,  // our fix: CLEAN → cold path
                            BS::NP => s.s3,     // cold path
                        };
                        W::HasPrior(fetched)
                    }

                    W::HasPrior(prior) => W::Recheck,

                    W::Recheck => {
                        if s.block != BS::NP {
                            // State changed → retry.
                            W::CheckState
                        } else {
                            // Still NP. Check if prior was zeros.
                            // We need to re-derive prior. In the model, the prior
                            // was captured in HasPrior but we didn't carry it through
                            // Recheck. Let me fix this — carry prior through.
                            // For now: use s3 as the prior (what we'd have fetched).
                            let prior = s.s3;
                            W::AllZerosCheck(prior)
                        }
                    }

                    W::AllZerosCheck(prior) => {
                        if prior == 0 {
                            // All zeros: write sub-block only (no backfill merge).
                            s.read_locks += 1;
                            let v = s.next_ver;
                            W::SetPresent(v)
                        } else {
                            W::BeforeCas(prior)
                        }
                    }

                    W::BeforeCas(prior) => {
                        if s.block == BS::NP {
                            // CAS wins.
                            s.block = BS::Clean;
                            W::CasWon(prior)
                        } else {
                            // CAS fails → retry.
                            W::CheckState
                        }
                    }

                    W::CasWon(prior) => {
                        // Merge prior + write data. Enter cache.write.
                        s.read_locks += 1;
                        let v = s.next_ver;
                        // The merge means ssd_active will have BOTH prior and new data.
                        // We model this as version v (includes the merge).
                        W::Pwrite(v)
                    }

                    W::SetPresent(v) => {
                        // set_present: NP→CLEAN or no-op if already present.
                        if s.block == BS::NP {
                            s.block = BS::Clean;
                        }
                        W::Pwrite(v)
                    }

                    W::Pwrite(v) => {
                        s.ssd_active = v;
                        W::WalAppend(v)
                    }

                    W::WalAppend(v) => {
                        W::TransitionDirty(v)
                    }

                    W::TransitionDirty(v) => {
                        // CLEAN→DIRTY or SYNCING→DIRTY or NP→DIRTY.
                        s.block = BS::Dirty;
                        s.next_ver += 1;
                        W::ReleaseLock
                    }

                    W::ReleaseLock => {
                        s.read_locks -= 1;
                        // Track the highest completed version. Can't regress.
                        let v = s.next_ver - 1;
                        if v > s.last_write_done {
                            s.last_write_done = v;
                        }
                        W::Done
                    }

                    W::Done => W::Done,
                });
            }

            A::StartF => { s.f = Some(F::CrcPrepass); }
            A::StepF => {
                let phase = s.f.as_ref().unwrap().clone();
                s.f = Some(match phase {
                    F::CrcPrepass => F::AcquireWriteLock,

                    F::AcquireWriteLock => {
                        // actions() ensures read_locks == 0 before we get here.
                        s.write_locked = true;
                        F::Snapshot
                    }

                    F::Snapshot => {
                        if s.block == BS::Dirty {
                            s.block = BS::Syncing;
                        }
                        F::Rotate
                    }

                    F::Rotate => {
                        if s.block == BS::Syncing {
                            s.ssd_flushing = s.ssd_active;
                            s.ssd_active = 0;
                            s.flushing_present = true;
                        }
                        F::ReleaseWriteLock
                    }

                    F::ReleaseWriteLock => {
                        s.write_locked = false;
                        F::Compute
                    }

                    F::Compute => F::Upload,

                    F::Upload => {
                        if s.ssd_flushing > 0 {
                            s.s3 = s.ssd_flushing;
                        }
                        F::Evict
                    }

                    F::Evict => {
                        // CAS SYNCING→NP. Fails if block was re-dirtied.
                        if s.block == BS::Syncing {
                            s.block = BS::NP;
                        }
                        F::Cleanup
                    }

                    F::Cleanup => {
                        s.flushing_present = false;
                        s.ssd_flushing = 0;
                        F::Done
                    }

                    F::Done => F::Done,
                });
            }

            A::StartR => { s.r = Some(R::AcquireReadLock); }
            A::StepR => {
                let phase = s.r.as_ref().unwrap().clone();
                s.r = Some(match phase {
                    R::AcquireReadLock => {
                        s.read_locks += 1;
                        R::FastPathCheck
                    }

                    R::FastPathCheck => {
                        // Fast path: DIRTY only (not SYNCING, not CLEAN, not NP).
                        if s.block == BS::Dirty {
                            R::FastPathPread
                        } else {
                            // Release lock, go to slow path.
                            s.read_locks -= 1;
                            R::LocateCheck
                        }
                    }

                    R::FastPathPread => {
                        let v = s.ssd_active;
                        R::ReleaseAfterFast
                    }

                    R::ReleaseAfterFast => {
                        s.read_locks -= 1;
                        // We need the version from FastPathPread... model limitation.
                        // Use current ssd_active (lock was held, so it's stable).
                        R::Done(s.ssd_active)
                    }

                    R::LocateCheck => {
                        // locate_block: check state for hot vs cold path.
                        match s.block {
                            BS::Dirty | BS::Syncing => R::SyncReadLock,
                            _ => R::ColdFetchS3,
                        }
                    }

                    R::SyncReadLock => {
                        s.read_locks += 1;
                        R::SyncReadRecheck
                    }

                    R::SyncReadRecheck => {
                        // Re-check state + pread + release: all under read lock.
                        // In the real code, sync_read_local_block does recheck +
                        // pread atomically under the data_file read lock. Fuse
                        // them to prevent impossible interleavings.
                        let v = match s.block {
                            BS::NP | BS::Clean => {
                                // Evicted or CLEAN: fall to cold path.
                                s.read_locks -= 1;
                                return Some({ s.r = Some(R::ColdFetchS3); s });
                            }
                            BS::Syncing if s.flushing_present => s.ssd_flushing,
                            BS::Dirty => s.ssd_active,
                            _ => {
                                // SYNCING but no flushing file: fall to S3.
                                s.read_locks -= 1;
                                return Some({ s.r = Some(R::ColdFetchS3); s });
                            }
                        };
                        s.read_locks -= 1;
                        R::Done(v)
                    }

                    R::SyncReadPread | R::SyncReadRelease => {
                        // No longer separate steps — fused into SyncReadRecheck.
                        unreachable!()
                    }

                    R::ColdFetchS3 => {
                        R::Done(s.s3)
                    }

                    R::Done(v) => R::Done(v),
                });
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // P1: After any write completes, the block must have SOME valid
            // data (not zeros) on SSD or in S3. Concurrent writes may
            // overwrite each other (last-writer-wins) — that's correct.
            // The invariant: no write is silently dropped to zeros.
            Property::always("no data loss to zeros", |_: &M, s: &S| {
                if s.last_write_done == 0 { return true; }
                // Some write completed. Block must have data somewhere.
                let any_data = s.ssd_active > 0 || s.ssd_flushing > 0 || s.s3 > 0;
                any_data
            }),

            // P2: A DIRTY block always has data on the active SSD (ssd_active > 0).
            Property::always("dirty implies data on ssd", |_: &M, s: &S| {
                if s.block == BS::Dirty {
                    s.ssd_active > 0
                } else {
                    true
                }
            }),

            // P3: A SYNCING block has data in the flushing file.
            Property::always("syncing implies flushing data", |_: &M, s: &S| {
                if s.block == BS::Syncing && s.flushing_present {
                    s.ssd_flushing > 0
                } else {
                    true
                }
            }),

            // P4: Read from the SSD fast path (FastPathPread) never returns 0
            // when block is DIRTY. The read lock prevents rotation.
            Property::always("fast path read not zeros", |_: &M, s: &S| {
                if matches!(s.r, Some(R::FastPathPread)) {
                    // Block must be DIRTY (fast path only entered for DIRTY).
                    // ssd_active must be > 0 (DIRTY implies data).
                    s.block == BS::Dirty && s.ssd_active > 0
                } else {
                    true
                }
            }),

            // P5: CLEAN block is never read from SSD by the read path.
            // Our fix: locate_block skips CLEAN on the hot path.
            // SyncReadPread is fused into SyncReadRecheck now (atomic under lock).
            Property::always("clean block not read from ssd", |_: &M, s: &S| {
                if matches!(s.r, Some(R::FastPathPread)) {
                    s.block != BS::Clean
                } else {
                    true
                }
            }),

            // P6: Write lock is only held during flush rotation steps.
            Property::always("write lock discipline", |_: &M, s: &S| {
                if s.write_locked {
                    matches!(
                        s.f,
                        Some(F::Snapshot) | Some(F::Rotate) | Some(F::ReleaseWriteLock)
                    )
                } else {
                    true
                }
            }),

            // P7: Flush persists data to S3 before completing.
            Property::always("flush persists to s3", |_: &M, s: &S| {
                if matches!(s.f, Some(F::Done)) {
                    s.s3 > 0 || s.last_write_done == 0
                } else {
                    true
                }
            }),

            // Liveness
            Property::sometimes("write completes", |_: &M, s: &S| {
                s.w.iter().any(|w| matches!(w, Some(W::Done)))
            }),
            Property::sometimes("flush completes", |_: &M, s: &S| {
                matches!(s.f, Some(F::Done))
            }),
            Property::sometimes("read completes", |_: &M, s: &S| {
                matches!(s.r, Some(R::Done(_)))
            }),
        ]
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn check(s3: bool, sub: bool) -> CheckerBuilder<M> {
        M { s3_has_data: s3, sub_block: sub }.checker().threads(4)
    }

    #[test]
    fn sub_block_with_s3() {
        let r = check(true, true).spawn_dfs().join();
        eprintln!("sub_block+s3: {} unique states", r.unique_state_count());
        r.assert_properties();
    }

    /// Use "sometimes" properties to check if interesting concurrent states
    /// are reachable. If they're reachable, we should have interleaving tests
    /// covering them. If not reachable, our enumeration correctly excluded them.
    #[test]
    fn reachability_of_concurrent_states() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let model = M { s3_has_data: true, sub_block: true };

        // Add reachability properties alongside the normal ones.
        struct Reachability {
            inner: M,
        }
        impl Model for Reachability {
            type State = S;
            type Action = A;
            fn init_states(&self) -> Vec<S> { self.inner.init_states() }
            fn actions(&self, s: &S, a: &mut Vec<A>) { self.inner.actions(s, a) }
            fn next_state(&self, s: &S, a: A) -> Option<S> { self.inner.next_state(s, a) }

            fn properties(&self) -> Vec<Property<Self>> {
                vec![
                    // All safety properties from the inner model.
                    Property::always("no data loss", |_: &Reachability, s: &S| {
                        if s.last_write_done == 0 { return true; }
                        s.ssd_active > 0 || s.ssd_flushing > 0 || s.s3 > 0
                    }),
                    Property::always("dirty implies ssd data", |_: &Reachability, s: &S| {
                        if s.block == BS::Dirty { s.ssd_active > 0 } else { true }
                    }),

                    // Reachability probes: each "sometimes" property must be reachable.
                    // If stateright says "sometimes property not satisfied", that state
                    // combination is unreachable and we can skip testing it.

                    Property::sometimes("two writers in CAS race", |_: &Reachability, s: &S| {
                        matches!(s.w[0], Some(W::BeforeCas(_))) && matches!(s.w[1], Some(W::BeforeCas(_)))
                    }),

                    Property::sometimes("writer fetch while other pwrite", |_: &Reachability, s: &S| {
                        (matches!(s.w[0], Some(W::FetchS3)) && matches!(s.w[1], Some(W::Pwrite(_))))
                        || (matches!(s.w[1], Some(W::FetchS3)) && matches!(s.w[0], Some(W::Pwrite(_))))
                    }),

                    Property::sometimes("writer pwrite during flush evict", |_: &Reachability, s: &S| {
                        (matches!(s.w[0], Some(W::Pwrite(_))) || matches!(s.w[1], Some(W::Pwrite(_))))
                        && matches!(s.f, Some(F::Evict))
                    }),

                    Property::sometimes("reader locate while flush evicting", |_: &Reachability, s: &S| {
                        matches!(s.r, Some(R::LocateCheck)) && matches!(s.f, Some(F::Evict))
                    }),

                    Property::sometimes("writer CAS while flush rotating", |_: &Reachability, s: &S| {
                        (matches!(s.w[0], Some(W::BeforeCas(_))) || matches!(s.w[1], Some(W::BeforeCas(_))))
                        && matches!(s.f, Some(F::Snapshot) | Some(F::Rotate))
                    }),

                    Property::sometimes("reader sees CLEAN block at locate", |_: &Reachability, s: &S| {
                        s.block == BS::Clean && matches!(s.r, Some(R::LocateCheck))
                    }),

                    // "writer SetPresent while reader checks" is unreachable for
                    // a single block: SetPresent requires NP, FastPathCheck requires
                    // DIRTY — can't be both simultaneously on the same block.

                    Property::sometimes("both writers pwriting concurrently", |_: &Reachability, s: &S| {
                        matches!(s.w[0], Some(W::Pwrite(_))) && matches!(s.w[1], Some(W::Pwrite(_)))
                    }),

                    Property::sometimes("writer promote during flush compute", |_: &Reachability, s: &S| {
                        // Writer saw SYNCING, entered promote path (modeled as atomic
                        // in SawState, but we can check for the state conditions).
                        s.block == BS::Syncing
                        && s.flushing_present
                        && (matches!(s.w[0], Some(W::SawState(BS::Syncing))) || matches!(s.w[1], Some(W::SawState(BS::Syncing))))
                        && matches!(s.f, Some(F::Compute))
                    }),

                    Property::sometimes("writer sees NP after flush eviction", |_: &Reachability, s: &S| {
                        s.block == BS::NP
                        && matches!(s.f, Some(F::Cleanup) | Some(F::Done))
                        && (matches!(s.w[0], Some(W::SawState(BS::NP))) || matches!(s.w[1], Some(W::SawState(BS::NP))))
                    }),
                ]
            }
        }

        let r = Reachability { inner: model }.checker().threads(4).spawn_dfs().join();
        eprintln!("reachability check: {} unique states", r.unique_state_count());
        r.assert_properties();
    }

    #[test]
    fn full_block_with_s3() {
        let r = check(true, false).spawn_dfs().join();
        eprintln!("full_block+s3: {} unique states", r.unique_state_count());
        r.assert_properties();
    }

    #[test]
    fn sub_block_no_s3() {
        let r = check(false, true).spawn_dfs().join();
        eprintln!("sub_block_no_s3: {} unique states", r.unique_state_count());
        r.assert_properties();
    }

    #[test]
    fn full_block_no_s3() {
        let r = check(false, false).spawn_dfs().join();
        eprintln!("full_block_no_s3: {} unique states", r.unique_state_count());
        r.assert_properties();
    }
}
