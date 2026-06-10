//! Faithful model of the write-cache data path, built to EXPOSE the
//! stale-promote data-loss class the original model (lib.rs) cannot reach.
//!
//! Fidelity fixes vs lib.rs (see MODEL_AUDIT.md):
//!   D1: promote runs BEFORE set_present (write.rs:145 then :158).
//!   D2: blocks have 2 PAGES; a sub-block write sets one page and relies on
//!       promote/backfill to have preserved the other (the real corruption).
//!   D3: back-to-back flush generations — a 2nd writer keeps a block dirty so a
//!       new flush can rotate while the victim is NOT_PRESENT, giving gen N+1 a
//!       flushing file that holds ZEROS for a block evicted in gen N.
//!   D4: the flush's skipped-block recovery (flush.rs:1203) copy is modeled.
//!   D6: TWO writers (was 1) — required so one writer can be mid-write to the
//!       victim while the other drives the second flush generation.
//!   D7: the promote CLAIM (`PromoteClaimBitmap`, inner.rs) is modeled, with
//!       real holder/waiter semantics — including the waiter-bypass bug (see
//!       `waiter_fix` below). An earlier revision instead serialized writers
//!       per-block with an `own[]` bitmap when "fixed" — REAL CODE HAS NO SUCH
//!       SERIALIZATION, so that model verified a stronger system than shipped
//!       and its "fixed is clean" result was vacuous for the claim races.
//!
//! Oracle: `ev[b][p]` = last value written to page p of block b. Invariant
//! (read table — sync_read_local_block / locate_block): in any quiescent state
//! a read returns ev, i.e. NP→s3, DIRTY→ssd, SYNCING→flushing all equal ev.
//!
//! ## `zc`: the ublk zero-copy write path (vs USER_COPY)
//!
//! With `zc=false` the writer models USER_COPY's `backfill_and_write`: it
//! fetches the prior block and writes the MERGED full block (prior + new page)
//! ATOMICALLY under the rotation gate (one `Pwrite`).
//!
//! With `zc=true` the writer models the real ublk ZC path, which is NOT atomic:
//!   1. `Backfill` — `pre_write`/`backfill_blocks_in_range` fetches the prior
//!      block into the active file and marks it DIRTY, WITHOUT the rotation
//!      gate (it awaits S3). This is a separate write from the slice write.
//!   2. (gap) — a flush can rotate+evict the just-backfilled DIRTY block here,
//!      because `pre_write` returned before the gate was taken.
//!   3. `Lock` → promote — under the gate.
//!   4. `Pwrite` — the kernel `WRITE_FIXED` writes ONLY the slice (one page),
//!      NOT a merged full block. The remainder must already be in the active
//!      file from step 1.
//!
//! ## The two fixes (both shipped; both must hold)
//!
//! `promote_fix` (PR #77): `require_promotion=true` for sub-block writes, so a
//! claim HOLDER that finds the block NOT_PRESENT with stale zeros in the
//! flushing file returns BlockEvicted → the caller re-backfills — instead of
//! silently skipping and letting the slice `Pwrite` land on sparse zeros.
//!
//! `waiter_fix`: a claim WAITER must RE-CHECK the block state after the holder
//! releases, instead of skipping its own promote unconditionally. Without it,
//! a holder that bailed with BlockEvicted (promoted NOTHING) releases the
//! claim and the waiter proceeds as if the data were recovered — its slice
//! `Pwrite` lands on sparse zeros and `Dirty` commits the half-zero block.
//! This is the RESIDUAL `fio_verify_random_cold_wake` corruption that
//! survived PR #77 (observed: ~30 of 32 pages zero; the survivors were
//! exactly the waiters' slices). The claim code is shared by USER_COPY and
//! ZC, so the bypass is transport-independent — ZC just makes the evicted
//! state far more reachable via the Backfill→Lock gap.
//!
//! Model twin of the Rust regression tests
//! `write_cache::tests::zc_promote_rejects_subblock_write_to_evicted_block`
//! and `write_cache::tests::zc_promote_waiter_must_recheck_after_holder_bails`.

use stateright::*;
use std::fmt;

type Ver = u8;
const NBLK: usize = 2;
const PAGES: usize = 2;
const NW: usize = 2; // writers
const MAXV: Ver = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum BS { NP, Clean, Dirty, Syncing }

type Page = [Ver; PAGES];
fn zero() -> Page { [0; PAGES] }
fn is_zero(p: &Page) -> bool { p.iter().all(|&v| v == 0) }

// Writer phases — backfill_and_write + write_inner + promote (correct order).
//
// Promote is three phases mirroring `promote_syncing_blocks`'s per-block body:
//   Promote      — re-read state; nothing-to-do → SetPresent; else try_claim:
//                  free → PromoteDo (holder), held → PromoteWait (waiter).
//   PromoteWait  — parked on the claim condvar (still holding the rotation
//                  read gate `rl`). When the claim frees: waiter_fix → back
//                  to Promote (RE-CHECK); buggy → SetPresent (skip — assumes
//                  the holder promoted, which is FALSE if it bailed).
//   PromoteDo    — the holder's pread/stale-check/pwrite/CAS body; releases
//                  the claim on every exit (ClaimGuard).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum W {
    Check, SlowFetch, SlowRecheck, SlowClaim,
    // ZC only: pre_write backfill (no rotation gate) — evictable gap before
    // Lock. Two phases so the fetch-before-claim ABA is representable:
    // Backfill = the async S3 fetch (claim_first=false snapshots `prior`
    // here), BackfillWrite = claim + pwrite of the fetched block.
    Backfill, BackfillWrite,
    Lock, Promote, PromoteWait, PromoteDo,
    SetPresent, Pwrite, Dirty, Unlock, Done,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum F { Snapshot, Rotate, Compute, Recover, Upload, Manifest, Evict, Cleanup, Done }

// Per-writer op: (block, page-or-full, version, fetched-prior, require_promotion, phase)
type WOp = (usize, usize, Ver, Page, bool, W);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct S {
    bs: [BS; NBLK],
    ssd: [Page; NBLK], flu: [Page; NBLK], s3: [Page; NBLK], man: [Page; NBLK],
    ev: [Page; NBLK],
    flu_present: bool,
    snap: [bool; NBLK],
    w: [Option<WOp>; NW],
    f: Option<F>,
    rl: [bool; NW], wrl: bool,
    nv: Ver,
    /// Promote claim per block (`PromoteClaimBitmap`): which writer holds the
    /// pread+pwrite window. NOT a write-serialization lock — concurrent
    /// writers to the same block proceed through every other phase freely,
    /// exactly like the real code.
    claim: [Option<usize>; NBLK],
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum A { SW(usize, usize, usize), TW(usize), SF, TF }
impl fmt::Display for A {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Debug::fmt(self, f) }
}

pub struct M {
    /// require_promotion=true for sub-block writes (PR #77).
    pub promote_fix: bool,
    /// Claim waiters re-check state after release instead of skipping.
    pub waiter_fix: bool,
    /// Backfills claim (CAS NP→CLEAN) BEFORE the async S3 fetch, pinning the
    /// block so the fetched prior cannot go stale. Without it, a sibling's
    /// newer write can be committed + uploaded + evicted back to NP during
    /// the fetch (state ABA — the old `is_not_present()` re-check passes)
    /// and the merged write resurrects the STALE prior: silent rollback of
    /// an acknowledged write.
    pub claim_first: bool,
    /// Model the ublk zero-copy write path instead of USER_COPY.
    pub zc: bool,
}

impl M {
    // Advance writer `wi` by one real-code step.
    fn step_writer(&self, s: &mut S, wi: usize) {
        let (b, p, v, mut prior, mut req, ph) = s.w[wi].clone().unwrap();
        let is_full = p == PAGES;
        let next = match ph {
            // ZC path: pre_write backfills (separate, gated-free) then the
            // kernel WRITE_FIXED writes only the slice. require_promotion is
            // true for SUB-BLOCK writes ONLY with promote_fix; the buggy
            // pre-#77 ZC code passed false unconditionally.
            W::Check if self.zc => match s.bs[b] {
                // Present block (inline fast path): `try_zc_inflight_enter`
                // takes the rotation gate BEFORE the present-check, so no
                // flush rotation can interleave before promote — model that by
                // taking `rl` here and going straight to Promote (no gap). An
                // async eviction (SYNCING→NP) can still race a held gate;
                // require_promotion=true (promote_fix, sub-block) surfaces it
                // as BlockEvicted → the retry falls back to the deferred
                // re-backfill path.
                BS::Dirty | BS::Syncing => { req = self.promote_fix && !is_full; s.rl[wi] = true; W::Promote }
                BS::Clean => W::Check,
                // Full-block ZC write: WRITE_FIXED overwrites everything —
                // no backfill data needed, require_promotion=false (correct
                // even with fix). claim_first: the full writer must WIN the
                // block claim (CAS NP→CLEAN, here via pre_write_sync /
                // backfill_blocks_in_range full-cover) so a concurrent
                // sub-block backfill's stale full-block pwrite can't land
                // unordered against our data write; a CLEAN block routes
                // siblings into the Check spin (the CLEAN-wait).
                BS::NP if is_full => {
                    if self.claim_first {
                        // try_claim_block linearizes through the promote
                        // claim — a promoter in flight fails the claim and
                        // the writer retries (inline → deferred → spin).
                        if s.claim[b].is_some() { W::Check }
                        else { s.bs[b] = BS::Clean; req = false; W::Lock }
                    } else { req = false; W::Lock }
                }
                // Sub-block ZC write: deferred path — backfill (NO gate) then
                // write. The Backfill→Lock gap is the real eviction window.
                BS::NP => { req = self.promote_fix; W::Backfill }
            },
            W::Check => match s.bs[b] {
                // Present block: `backfill_and_write`'s loser path writes
                // ONLY the guest's sub-range via write_with_eviction_check
                // (handler.rs:656) — it does NOT merge a previously-fetched
                // prior (that's the claim winner's path). Reset `prior` so
                // Pwrite takes the sub-write branch; carrying a stale fetch
                // here would model a merged-full pwrite clobbering a sibling
                // writer's just-landed slice — a write the real code never
                // issues.
                BS::Dirty | BS::Syncing => { prior = zero(); req = true; W::Lock }
                BS::Clean => W::Check,
                BS::NP => {
                    // claim-first only when explicitly enabled (to test minimality)
                    if self.claim_first { req = false; W::SlowClaim }
                    else if is_full {
                        // claim_first: full-block USER_COPY writes claim too
                        // (backfill_and_write full-cover try_claim_block),
                        // linearized through the promote claim.
                        if self.claim_first && s.claim[b].is_some() { W::Check }
                        else {
                            if self.claim_first { s.bs[b] = BS::Clean; }
                            req = false; W::Lock
                        }
                    }
                    else { W::SlowFetch }
                }
            },
            // ZC pre_write: backfill_blocks_in_range fetches the prior block
            // into the active file + marks DIRTY when S3 has data, then
            // cache.pre_write set_presents the block. NO rotation gate is held
            // — a flush may rotate+evict between this step and W::Lock (the
            // gap that produces the corruption for sub-block writes). `prior`
            // stays zero() so the later Pwrite writes ONLY the slice (the
            // kernel WRITE_FIXED does not merge — unlike USER_COPY).
            // `backfill_blocks_in_range` SKIPS any present block (handler.rs
            // `is_block_present` — SYNCING/CLEAN/DIRTY all count as present).
            // Only a NOT_PRESENT block is fetched. Without that guard the
            // model would let a backfill clobber a SYNCING (mid-flush) block
            // with stale S3 bytes — an interleaving the presence check
            // forbids.
            //
            // claim_first=false (fetch-then-claim, the ABA bug): Backfill
            // snapshots the S3 prior into `prior`; BackfillWrite later claims
            // and writes that SNAPSHOT — which may be stale if a sibling's
            // newer write was uploaded + evicted (NP again) during the fetch.
            //
            // claim_first=true (the fix): the claim (CAS NP→CLEAN) happens
            // FIRST and pins the block — flush never touches CLEAN, siblings
            // CLEAN-wait — so fetch + write collapse into one atomic
            // BackfillWrite reading the LIVE s3 value (nothing can change it
            // while CLEAN).
            W::Backfill => {
                if s.bs[b] == BS::NP {
                    if self.claim_first {
                        // Materialization claim: state claim (CLEAN) + HELD
                        // promote claim across fetch + write, so a guest
                        // promote that observes CLEAN parks instead of
                        // slicing into unmaterialized bytes. actions() parks
                        // this step while another promoter holds the claim.
                        s.bs[b] = BS::Clean;
                        s.claim[b] = Some(wi);
                    } else {
                        prior = s.s3[b]; // async fetch: snapshot may go stale
                    }
                    W::BackfillWrite
                } else {
                    W::Lock
                }
            }
            W::BackfillWrite => {
                // The data write goes through `cache.write` → `write_inner`,
                // whose embedded `promote_syncing_blocks` CONTENDS ON THE
                // PROMOTE CLAIM — actions() gates this step on
                // `claim[b].is_none()` while the block is NP. The embedded
                // promote's flu-copy is invisible here because the full
                // backfill pwrite immediately overwrites it in the same
                // critical section, and its NP→DIRTY CAS is subsumed by the
                // backfill's own dirty-marking.
                if self.claim_first {
                    // write_materialized: under the data_file WRITE lock
                    // (actions() gates this step on no other writer holding
                    // the rotation gate — the drain) with a CLEAN re-check.
                    // A steal (straggler guest write committed CLEAN→DIRTY
                    // while we fetched) aborts the stale pre-image write.
                    // Zero prior → leave CLEAN (set_present covers it;
                    // WRITE_FIXED + commit dirty it). Release the promote
                    // claim AFTER the data write.
                    if s.bs[b] == BS::Clean && !is_zero(&s.s3[b]) {
                        s.ssd[b] = s.s3[b];
                        s.bs[b] = BS::Dirty;
                    }
                    s.claim[b] = None;
                } else if !is_zero(&prior) && s.bs[b] == BS::NP {
                    // Fetch-then-claim: zero-skip happens before the claim
                    // (leaves NP); otherwise try_claim_block + cache.write of
                    // the possibly-STALE snapshot (the ABA corruption).
                    s.ssd[b] = prior;
                    s.bs[b] = BS::Dirty;
                }
                prior = zero(); // ZC Pwrite is slice-only — never merges
                W::Lock
            }
            W::SlowFetch => {
                prior = s.s3[b];
                // FIX: we already hold the claim (CLEAN), so this S3 read is
                // stable — no flush writes a CLEAN block. Buggy path rechecks.
                if self.claim_first { W::Lock } else { W::SlowRecheck }
            }
            W::SlowRecheck => { // buggy path only
                if s.bs[b] != BS::NP { W::Check }
                else if is_zero(&s.s3[b]) { req = false; W::Lock }
                else { W::SlowClaim }
            }
            W::SlowClaim => {
                // The materialization claim linearizes through the promote
                // claim — held by another ⇒ retry from Check.
                if s.claim[b].is_some() { W::Check }
                else if s.bs[b] == BS::NP {
                    s.bs[b] = BS::Clean; req = false;
                    if self.claim_first {
                        // claim-first: HOLD the promote claim across the
                        // fetch + merged write (released at W::Dirty), so
                        // guest-write promotes observing CLEAN park instead
                        // of racing the merged pwrite. Full writes skip the
                        // fetch (they overwrite both pages).
                        s.claim[b] = Some(wi);
                        if is_full { W::Lock } else { W::SlowFetch }
                    } else { W::Lock }
                } else { W::Check }
            }
            W::Lock => { s.rl[wi] = true; W::Promote }
            // Claim acquisition — `promote_syncing_blocks` per-block entry.
            // State is RE-READ here on every entry (this is also where a
            // re-checking waiter loops back to).
            W::Promote => {
                if s.bs[b] == BS::Clean && req && s.claim[b].is_some() {
                    // CLEAN + held claim = an S3 materialization is mid-
                    // flight (fetch + write_materialized). A guest-write
                    // promote (require_promotion) bails with BlockEvicted —
                    // retrying WITHOUT the rotation gate (the backfill
                    // CLEAN-wait parks gate-free). It must NOT park here:
                    // the materializer's write_materialized acquires the
                    // data_file WRITE lock, which waits for our held gate —
                    // lock-order deadlock. CLEAN + free claim falls through
                    // to SetPresent — zero-prior or covered by the owner's
                    // own pending guest write.
                    s.rl[wi] = false;
                    s.w[wi] = Some((b, p, v, zero(), false, W::Check));
                    return;
                } else if !matches!(s.bs[b], BS::Syncing | BS::NP) {
                    // Present (CLEAN/DIRTY) — promoted by a prior holder or
                    // never needed promotion. Nothing to do.
                    W::SetPresent
                } else if s.claim[b].is_none() {
                    s.claim[b] = Some(wi);
                    W::PromoteDo
                } else {
                    W::PromoteWait
                }
            }
            // Parked on the claim condvar (rotation gate still held).
            W::PromoteWait => {
                if s.claim[b].is_some() {
                    W::PromoteWait // still held — keep parking
                } else if self.waiter_fix {
                    // FIX: re-check the state — the holder may have bailed
                    // (BlockEvicted) having promoted NOTHING.
                    W::Promote
                } else {
                    // BUG (shipped in #77): assume the holder promoted and
                    // skip our own pread/pwrite AND the stale-zero check.
                    W::SetPresent
                }
            }
            // The claim holder's body: pread flushing, stale-zero check,
            // pwrite active, CAS → DIRTY. Claim released on EVERY exit
            // (ClaimGuard semantics).
            W::PromoteDo => {
                if s.flu_present {
                    let stale_np_zero = s.bs[b] == BS::NP && is_zero(&s.flu[b]);
                    if stale_np_zero && self.promote_fix {
                        s.claim[b] = None; // ClaimGuard drop
                        if req {
                            s.rl[wi] = false;
                            s.w[wi] = Some((b, p, v, zero(), false, W::Check)); // BlockEvicted → retry
                            return;
                        }
                        // !req: skip (full/merged paths carry correct data)
                        W::SetPresent
                    } else {
                        s.ssd[b] = s.flu[b];   // copy flushing→active (BUG w/o promote_fix for stale NP zeros)
                        s.bs[b] = BS::Dirty;
                        s.claim[b] = None;
                        W::SetPresent
                    }
                } else {
                    s.claim[b] = None;
                    if req {
                        s.rl[wi] = false;
                        s.w[wi] = Some((b, p, v, zero(), false, W::Check)); // no flushing file → BlockEvicted
                        return;
                    }
                    W::SetPresent
                }
            }
            W::SetPresent => { if s.bs[b] == BS::NP { s.bs[b] = BS::Clean; } W::Pwrite }
            W::Pwrite => {
                if is_full {
                    for q in 0..PAGES { s.ssd[b][q] = v; s.ev[b][q] = v; }
                } else if !is_zero(&prior) {
                    s.ssd[b] = prior; s.ssd[b][p] = v; s.ev[b][p] = v; // merged full write
                } else {
                    s.ssd[b][p] = v; s.ev[b][p] = v;                   // sub-write; rest relies on promote
                }
                W::Dirty
            }
            W::Dirty => {
                s.bs[b] = BS::Dirty; s.nv = (s.nv + 1).min(MAXV + 1);
                // Release a still-held materialization claim (SlowClaim
                // claim-first path) — the data write has landed; parked
                // guest promotes re-check and see DIRTY.
                if s.claim[b] == Some(wi) { s.claim[b] = None; }
                W::Unlock
            }
            W::Unlock => { s.rl[wi] = false; W::Done }
            W::Done => W::Done,
        };
        s.w[wi] = Some((b, p, v, prior, req, next));
    }
}

impl Model for M {
    type State = S;
    type Action = A;

    fn init_states(&self) -> Vec<S> {
        vec![S {
            bs: [BS::NP; NBLK], ssd: [zero(); NBLK], flu: [zero(); NBLK],
            s3: [zero(); NBLK], man: [zero(); NBLK], ev: [zero(); NBLK],
            flu_present: false, snap: [false; NBLK],
            w: [None, None], f: None, rl: [false; NW], wrl: false, nv: 1,
            claim: [None; NBLK],
        }]
    }

    fn actions(&self, s: &S, a: &mut Vec<A>) {
        for wi in 0..NW {
            if matches!(s.w[wi], None | Some((_, _, _, _, _, W::Done))) && s.nv <= MAXV {
                // No per-block serialization: concurrent writers to the SAME
                // block are allowed, exactly like the real code (fio iodepth
                // 32 puts up to 32 sub-block writes of one 128 KiB block in
                // flight at once). The promote claim is the ONLY same-block
                // coordination.
                for b in 0..NBLK {
                    for p in 0..PAGES { a.push(A::SW(wi, b, p)); }
                    a.push(A::SW(wi, b, PAGES));
                }
            }
            if let Some((b, _, _, _, _, ref ph)) = s.w[wi] {
                if !matches!(ph, W::Done) {
                    // A step that touches the data_file is blocked while a
                    // rotation holds the data_file WRITE lock (`wrl`):
                    //  * `Backfill` = `cache.write`, takes the read lock.
                    //  * `Lock` acquires the rotation read-gate.
                    //  * `Promote*`..`Unlock` run under the held read-gate.
                    // `rotate_and_snapshot` does Snapshot+Rotate atomically
                    // under the write lock, so NONE of these can interleave
                    // it — modeling them as blocked-while-`wrl` keeps the
                    // snapshot/rotate pair atomic w.r.t. backfills and writes
                    // (otherwise a backfill could mark a block DIRTY between
                    // Snapshot and the all-ssd-zeroing Rotate, stranding it —
                    // an interleaving the real read/write lock forbids).
                    let needs_file = matches!(
                        ph,
                        W::BackfillWrite | W::Lock | W::Promote | W::PromoteWait | W::PromoteDo
                            | W::SetPresent | W::Pwrite | W::Dirty | W::Unlock
                    );
                    // A parked waiter only does something when the claim
                    // frees — don't emit self-loop actions while it's held.
                    // A BackfillWrite of a NOT_PRESENT block parks the same
                    // way: its `cache.write` runs `promote_syncing_blocks`,
                    // which waits on the claim before the data pwrite lands.
                    // write_materialized takes the data_file WRITE lock:
                    // it drains every other in-flight gated writer first.
                    let other_rl = (0..NW).any(|i| i != wi && s.rl[i]);
                    let blocked_on_write_lock =
                        self.claim_first && matches!(ph, W::BackfillWrite) && other_rl;
                    let held_by_other = s.claim[b].is_some_and(|c| c != wi);
                    let parked_on_claim = held_by_other
                        && (matches!(ph, W::PromoteWait)
                            || (matches!(ph, W::BackfillWrite) && s.bs[b] == BS::NP)
                            // claim-first: the materialization claim inside
                            // Backfill linearizes through the promote claim
                            // — park until it frees.
                            || (self.claim_first
                                && matches!(ph, W::Backfill)
                                && s.bs[b] == BS::NP));
                    if !(needs_file && s.wrl) && !parked_on_claim && !blocked_on_write_lock {
                        a.push(A::TW(wi));
                    }
                }
            }
        }
        if matches!(s.f, None | Some(F::Done)) && s.bs.iter().any(|&b| b == BS::Dirty) {
            a.push(A::SF);
        }
        if let Some(ref f) = s.f {
            if !matches!(f, F::Done) {
                // Snapshot AND Evict require the data_file WRITE lock —
                // both must drain every in-flight gate holder first
                // (rotation always did; eviction now does too, so a block
                // can never flip to NOT_PRESENT under a mid-flight writer).
                let needs_wl = matches!(f, F::Snapshot | F::Evict);
                let any_rl = s.rl.iter().any(|&x| x);
                if !(needs_wl && any_rl) { a.push(A::TF); }
            }
        }
    }

    fn next_state(&self, st: &S, action: A) -> Option<S> {
        let mut s = st.clone();
        match action {
            A::SW(wi, b, p) => {
                s.w[wi] = Some((b, p, s.nv, zero(), false, W::Check));
            }
            A::TW(wi) => { self.step_writer(&mut s, wi); }
            A::SF => { s.f = Some(F::Snapshot); }
            A::TF => {
                let ph = s.f.clone().unwrap();
                s.f = Some(match ph {
                    F::Snapshot => {
                        s.wrl = true;
                        for b in 0..NBLK { if s.bs[b] == BS::Dirty { s.snap[b] = true; s.bs[b] = BS::Syncing; } }
                        F::Rotate
                    }
                    F::Rotate => {
                        for b in 0..NBLK { s.flu[b] = s.ssd[b]; s.ssd[b] = zero(); }
                        s.flu_present = true; s.wrl = false;
                        F::Compute
                    }
                    F::Compute => F::Recover,
                    F::Recover => {
                        // skipped-block recovery (D4): a still-SYNCING snapshotted
                        // block whose flushing data is "torn" (flu != ev) is copied
                        // flushing→active and re-dirtied — can propagate stale bytes.
                        for b in 0..NBLK {
                            if s.snap[b] && s.bs[b] == BS::Syncing && s.flu[b] != s.ev[b] {
                                s.ssd[b] = s.flu[b]; s.bs[b] = BS::Dirty; s.snap[b] = false;
                            }
                        }
                        F::Upload
                    }
                    F::Upload => {
                        for b in 0..NBLK {
                            if s.snap[b] {
                                for q in 0..PAGES {
                                    if s.flu[b][q] > 0 { s.s3[b][q] = s.flu[b][q]; }
                                    else if s.man[b][q] > 0 { s.s3[b][q] = 0; }
                                }
                            }
                        }
                        F::Manifest
                    }
                    F::Manifest => { s.man = s.s3; F::Evict }
                    F::Evict => {
                        for b in 0..NBLK { if s.bs[b] == BS::Syncing && s.snap[b] { s.bs[b] = BS::NP; } }
                        F::Cleanup
                    }
                    F::Cleanup => {
                        s.flu_present = false; s.flu = [zero(); NBLK]; s.snap = [false; NBLK];
                        F::Done
                    }
                    F::Done => F::Done,
                });
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("read returns last write", |_: &M, s: &S| {
                let busy = s.w.iter().any(|w| w.as_ref().is_some_and(|(_,_,_,_,_,ph)| !matches!(ph, W::Done)))
                    || s.f.as_ref().is_some_and(|f| !matches!(f, F::Done));
                if busy { return true; }
                for b in 0..NBLK {
                    for q in 0..PAGES {
                        let want = s.ev[b][q];
                        let got = match s.bs[b] {
                            BS::Dirty => s.ssd[b][q],
                            BS::Syncing => if s.flu_present { s.flu[b][q] } else { s.ssd[b][q] },
                            BS::NP | BS::Clean => s.s3[b][q],
                        };
                        if got != want { return false; }
                    }
                }
                true
            }),
            Property::always("no data loss", |_: &M, s: &S| {
                let busy = s.w.iter().any(|w| w.as_ref().is_some_and(|(_,_,_,_,_,ph)| !matches!(ph, W::Done)))
                    || s.f.as_ref().is_some_and(|f| !matches!(f, F::Done));
                if busy { return true; }
                for b in 0..NBLK {
                    for q in 0..PAGES {
                        if s.ev[b][q] > 0 && s.ssd[b][q] != s.ev[b][q]
                            && s.flu[b][q] != s.ev[b][q] && s.s3[b][q] != s.ev[b][q] { return false; }
                    }
                }
                true
            }),
            Property::sometimes("a flush completes", |_: &M, s: &S| matches!(s.f, Some(F::Done))),
            Property::sometimes("a write completes", |_: &M, s: &S| {
                s.w.iter().any(|w| w.as_ref().is_some_and(|(_,_,_,_,_,ph)| matches!(ph, W::Done)))
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(promote_fix: bool, waiter_fix: bool, claim_first: bool, zc: bool) -> impl Checker<M> {
        M { promote_fix, waiter_fix, claim_first, zc }
            .checker().threads(4).spawn_dfs().join()
    }

    // ---- USER_COPY path (zc=false): merged full-block write under the gate ----
    #[test]
    fn without_fix_finds_corruption() {
        let r = run(false, false, false, false);
        eprintln!("[no-fix] {} states", r.unique_state_count());
        r.assert_any_discovery("read returns last write");
    }
    #[test]
    fn with_all_fixes_is_clean() {
        let r = run(true, true, true, false);
        eprintln!("[fix] {} states", r.unique_state_count());
        r.assert_properties();
    }
    /// BUG #3 (model-found): fetch-before-claim backfill ABA. With both
    /// promote fixes in place but claim_first missing, a sibling's newer
    /// write can be committed + uploaded + evicted back to NOT_PRESENT
    /// while a backfill's S3 fetch is in flight; the stale prior is then
    /// claimed + merged + written — silently rolling back the acknowledged
    /// newer write.
    #[test]
    fn fetch_before_claim_aba_still_corrupts() {
        let r = run(true, true, false, false);
        eprintln!("[aba] {} states", r.unique_state_count());
        r.assert_any_discovery("read returns last write");
    }

    // ---- ublk ZERO-COPY path (zc=true): backfill + slice-write with a gap ----
    #[test]
    fn zc_without_fix_finds_corruption() {
        let r = run(false, false, false, true);
        eprintln!("[zc no-fix] {} states", r.unique_state_count());
        r.assert_any_discovery("read returns last write");
    }
    /// THE RESIDUAL BUG — models exactly what PR #77 shipped: promote_fix on,
    /// waiter re-check missing. A claim waiter skips the stale-zero check
    /// after a bailed holder releases, lands its slice on sparse zeros, and
    /// commits the half-zero block DIRTY. This configuration MUST find the
    /// corruption; an earlier model revision hid it behind a per-block
    /// writer-serialization (`own[]`) that real code does not have.
    #[test]
    fn zc_promote_fix_alone_still_corrupts() {
        let r = run(true, false, false, true);
        eprintln!("[zc promote-fix-only] {} states", r.unique_state_count());
        r.assert_any_discovery("read returns last write");
    }
    /// The ZC backfill has the same fetch-before-claim ABA as USER_COPY.
    #[test]
    fn zc_fetch_before_claim_aba_still_corrupts() {
        let r = run(true, true, false, true);
        eprintln!("[zc aba] {} states", r.unique_state_count());
        r.assert_any_discovery("read returns last write");
    }
    #[test]
    fn zc_with_all_fixes_is_clean() {
        let r = run(true, true, true, true);
        eprintln!("[zc all-fixes] {} states", r.unique_state_count());
        r.assert_properties();
    }
}
