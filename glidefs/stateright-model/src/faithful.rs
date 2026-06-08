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
//!
//! Oracle: `ev[b][p]` = last value written to page p of block b. Invariant
//! (read table — sync_read_local_block / locate_block): in any quiescent state
//! a read returns ev, i.e. NP→s3, DIRTY→ssd, SYNCING→flushing all equal ev.

use stateright::*;
use std::fmt;

type Ver = u8;
const NBLK: usize = 2;
const PAGES: usize = 2;
const NW: usize = 2; // writers
const MAXV: Ver = 2;
const CLAIM_FIRST: bool = false; // test: is claim-first needed if we serialize?
const PROMOTE_FIX: bool = true; // test: is promote-fix needed if we serialize?

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum BS { NP, Clean, Dirty, Syncing }

type Page = [Ver; PAGES];
fn zero() -> Page { [0; PAGES] }
fn is_zero(p: &Page) -> bool { p.iter().all(|&v| v == 0) }

// Writer phases — backfill_and_write + write_inner + promote (correct order).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum W {
    Check, SlowFetch, SlowRecheck, SlowClaim,
    Lock, Promote, SetPresent, Pwrite, Dirty, Unlock, Done,
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
    own: [Option<usize>; NBLK], // FIX candidate: per-block write serialization
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum A { SW(usize, usize, usize), TW(usize), SF, TF }
impl fmt::Display for A {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Debug::fmt(self, f) }
}

pub struct M { pub fix: bool }

impl M {
    // Advance writer `wi` by one real-code step. Returns false if it produced a
    // full replacement of the op (early return, e.g. BlockEvicted retry).
    fn step_writer(&self, s: &mut S, wi: usize) {
        let (b, p, v, mut prior, mut req, ph) = s.w[wi].clone().unwrap();
        let is_full = p == PAGES;
        let next = match ph {
            W::Check => match s.bs[b] {
                BS::Dirty | BS::Syncing => { req = true; W::Lock }   // write_with_eviction_check
                BS::Clean => W::Check,
                BS::NP => {
                    // claim-first only when explicitly enabled (to test minimality)
                    if self.fix && CLAIM_FIRST { req = false; W::SlowClaim }
                    else if is_full { req = false; W::Lock }
                    else { W::SlowFetch }
                }
            },
            W::SlowFetch => {
                prior = s.s3[b];
                // FIX: we already hold the claim (CLEAN), so this S3 read is
                // stable — no flush writes a CLEAN block. Buggy path rechecks.
                if self.fix && CLAIM_FIRST { W::Lock } else { W::SlowRecheck }
            }
            W::SlowRecheck => { // buggy path only
                if s.bs[b] != BS::NP { W::Check }
                else if is_zero(&s.s3[b]) { req = false; W::Lock }
                else { W::SlowClaim }
            }
            W::SlowClaim => {
                if s.bs[b] == BS::NP {
                    s.bs[b] = BS::Clean; req = false;
                    // FIX: claimed (CLEAN) — now safe. Full writes skip the fetch
                    // (they overwrite both pages); sub writes fetch fresh prior.
                    if self.fix && CLAIM_FIRST { if is_full { W::Lock } else { W::SlowFetch } } else { W::Lock }
                } else { W::Check }
            }
            W::Lock => { s.rl[wi] = true; W::Promote }
            W::Promote => {
                if matches!(s.bs[b], BS::Syncing | BS::NP) {
                    if s.flu_present {
                        let stale_np_zero = s.bs[b] == BS::NP && is_zero(&s.flu[b]);
                        if stale_np_zero && self.fix && PROMOTE_FIX {
                            if req {
                                s.rl[wi] = false;
                                s.w[wi] = Some((b, p, v, zero(), false, W::Check)); // BlockEvicted → retry
                                return;
                            }
                            // !req: skip (full/merged paths carry correct data)
                        } else {
                            s.ssd[b] = s.flu[b];   // copy flushing→active (BUG w/o fix for stale NP zeros)
                            s.bs[b] = BS::Dirty;
                        }
                    } else if req {
                        s.rl[wi] = false;
                        s.w[wi] = Some((b, p, v, zero(), false, W::Check)); // no flushing file → BlockEvicted
                        return;
                    }
                }
                W::SetPresent
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
            W::Dirty => { s.bs[b] = BS::Dirty; s.nv = (s.nv + 1).min(MAXV + 1); W::Unlock }
            W::Unlock => { s.rl[wi] = false; if self.fix { s.own[b] = None; } W::Done }
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
            own: [None; NBLK],
        }]
    }

    fn actions(&self, s: &S, a: &mut Vec<A>) {
        for wi in 0..NW {
            if matches!(s.w[wi], None | Some((_, _, _, _, _, W::Done))) && s.nv <= MAXV {
                for b in 0..NBLK {
                    // FIX: serialize writers per block — only start if unowned.
                    if self.fix && s.own[b].is_some() { continue; }
                    for p in 0..PAGES { a.push(A::SW(wi, b, p)); }
                    a.push(A::SW(wi, b, PAGES));
                }
            }
            if let Some((_, _, _, _, _, ref ph)) = s.w[wi] {
                if !matches!(ph, W::Done) {
                    let holds_rl = matches!(ph, W::Promote | W::SetPresent | W::Pwrite | W::Dirty | W::Unlock);
                    if !(holds_rl && s.wrl) { a.push(A::TW(wi)); }
                }
            }
        }
        if matches!(s.f, None | Some(F::Done)) && s.bs.iter().any(|&b| b == BS::Dirty) {
            a.push(A::SF);
        }
        if let Some(ref f) = s.f {
            if !matches!(f, F::Done) {
                let needs_wl = matches!(f, F::Snapshot);
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
                if self.fix { s.own[b] = Some(wi); }
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
    fn run(fix: bool) -> impl Checker<M> {
        M { fix }.checker().threads(4).spawn_dfs().join()
    }
    #[test]
    fn without_fix_finds_corruption() {
        let r = run(false);
        eprintln!("[no-fix] {} states", r.unique_state_count());
        r.assert_any_discovery("read returns last write");
    }
    #[test]
    fn with_fix_is_clean() {
        let r = run(true);
        eprintln!("[fix] {} states", r.unique_state_count());
        r.assert_properties();
    }
}
