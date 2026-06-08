//! Stateright model of the GlideFS write cache state machine.
//!
//! 2 writers + 1 flusher + 1 reader on 2 blocks. Every real code boundary
//! is a separate model step. No fused transitions.
//!
//! Writers can perform normal writes (new version) or zero writes (trim/
//! write_zeroes). The flush path models the cross-flush dedup optimization:
//! zero data is not uploaded to S3 unless prior non-zero data exists at
//! that offset (tombstone needed to override).
//!
//! Run: `cd stateright-model && cargo test --release`

//! NOTE ON FIDELITY: this model's write phase orders `set_present` BEFORE
//! promote, whereas the real code (write.rs:145 then :158) promotes FIRST.
//! That simplification (audit item D1 in MODEL_AUDIT.md) masks the stale-promote
//! data-loss class, so the AUTHORITATIVE model for the write/flush data-loss
//! race is `src/faithful.rs` (correct order + sub-block writes + the verified
//! fix). This model remains useful for the cross-flush dedup and crash-recovery
//! properties it checks faithfully.

mod faithful;

use stateright::*;
use std::fmt;

type Ver = u8;
const NBLK: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum BS { NP, Clean, Dirty, Syncing }

// --- Write phases (handler.backfill_and_write + cache.write) ---
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum W {
    CheckState, Observed(BS),
    // Backfill:
    HasS3Check, LocateIsPresent(BS), LocateHotPread(BS), LocateColdFetch,
    FetchDone(Ver), RecheckState(Ver), ZerosCheck(Ver),
    BeforeCas(Ver), CasResult(Ver, bool),
    // cache.write (lock held):
    AcquireLock, SetPresent(Ver),
    PromoteCloneArc, PromoteHasArc(bool, Ver), PromotePread(Ver),
    PromotePwrite(Ver), PromoteCas, PromoteSkip,
    PwriteData(Ver), WalAppend(Ver), TransitionDirty, ReleaseLock,
    Done,
}

// --- Flush phases ---
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum F {
    CrcPrepass, AcquireWriteLock, Snapshot, Rotate, ReleaseWriteLock,
    Compute, Upload, ManifestSync, Evict, Checkpoint, Cleanup, Done,
}

// --- Read phases ---
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum R {
    AcquireReadLock, FastPathCheck, FastPathPread(Ver), ReleaseAfterFast(Ver),
    LocateStateCheck, SlowAcquireLock, SlowRecheck, ColdFetchS3, Done(Ver),
}

// --- State ---
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct S {
    blk: [BS; NBLK], ssd: [Ver; NBLK], flu: [Ver; NBLK],
    s3: [Ver; NBLK], man: [Ver; NBLK],
    flu_present: bool,
    w: [Option<(usize, W)>; 2], // (block_idx, phase)
    wz: [bool; 2],               // true if writer i is doing a zero-write
    f: Option<F>,
    r: Option<(usize, R)>,       // (block_idx, phase)
    wl: [bool; 2], rl: bool, wrl: bool, // per-op read locks, write lock
    nv: Ver, lwd: Ver,
    wal: [Ver; NBLK], pb: [BS; NBLK],
    snap: [bool; NBLK],          // blocks claimed by current flush
    ev: [Ver; NBLK],             // expected value: set in PwriteData
    zw_done: bool,                // limits state space to 1 zero write
    crashed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum A {
    SW(usize, usize), SZW(usize, usize), TW(usize),
    SF, TF,
    SR(usize), TR,
    Crash, Recover,
}
impl fmt::Display for A {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Debug::fmt(self, f) }
}

struct M { s3: bool, sub: bool, crash: bool, zero_writes: bool }

impl M {
    fn wl(w: &W) -> bool {
        matches!(w, W::AcquireLock | W::SetPresent(_) | W::PromoteCloneArc |
            W::PromoteHasArc(_,_) | W::PromotePread(_) | W::PromotePwrite(_) |
            W::PromoteCas | W::PromoteSkip | W::PwriteData(_) | W::WalAppend(_) |
            W::TransitionDirty | W::ReleaseLock)
    }
    fn rl(r: &R) -> bool {
        matches!(r, R::AcquireReadLock | R::FastPathCheck | R::FastPathPread(_) |
            R::ReleaseAfterFast(_) | R::SlowAcquireLock | R::SlowRecheck)
    }
}

impl Model for M {
    type State = S;
    type Action = A;

    fn init_states(&self) -> Vec<S> {
        let v = if self.s3 { 1 } else { 0 };
        vec![S {
            blk: [BS::NP; NBLK], ssd: [0; NBLK], flu: [0; NBLK],
            s3: [v; NBLK], man: [v; NBLK],
            flu_present: false,
            w: [None, None], wz: [false; 2], f: None, r: None,
            wl: [false; 2], rl: false, wrl: false,
            nv: 2, lwd: 0,
            wal: [0; NBLK], pb: [BS::NP; NBLK],
            snap: [false; NBLK],
            ev: [v; NBLK],
            zw_done: false,
            crashed: false,
        }]
    }

    fn actions(&self, s: &S, a: &mut Vec<A>) {
        if s.crashed { a.push(A::Recover); return; }

        for i in 0..2 {
            if matches!(s.w[i], None | Some((_, W::Done))) {
                if s.nv < 3 {
                    for b in 0..NBLK { a.push(A::SW(i, b)); }
                }
                if self.zero_writes && !s.zw_done {
                    for b in 0..NBLK { a.push(A::SZW(i, b)); }
                }
            }
            if let Some((_, ref w)) = s.w[i] {
                if !matches!(w, W::Done) && (!Self::wl(w) || !s.wrl) {
                    a.push(A::TW(i));
                }
            }
        }
        if matches!(s.f, None | Some(F::Done)) && s.blk.iter().any(|b| *b == BS::Dirty) {
            a.push(A::SF);
        }
        if let Some(ref f) = s.f {
            if !matches!(f, F::Done) {
                let nw = matches!(f, F::AcquireWriteLock);
                let ar = s.wl[0] || s.wl[1] || s.rl;
                if !nw || !ar { a.push(A::TF); }
            }
        }
        if matches!(s.r, None | Some((_, R::Done(_)))) {
            for b in 0..NBLK { a.push(A::SR(b)); }
        }
        if let Some((_, ref r)) = s.r {
            if !matches!(r, R::Done(_)) && (!Self::rl(r) || !s.wrl) {
                a.push(A::TR);
            }
        }
        if self.crash {
            let inf = s.w.iter().any(|w| w.as_ref().is_some_and(|(_, p)| !matches!(p, W::Done)))
                || s.f.as_ref().is_some_and(|f| !matches!(f, F::Done))
                || s.r.as_ref().is_some_and(|(_, r)| !matches!(r, R::Done(_)));
            if inf { a.push(A::Crash); }
        }
    }

    fn next_state(&self, st: &S, action: A) -> Option<S> {
        let mut s = st.clone();
        match action {
            A::Crash => {
                s.w = [None, None]; s.wz = [false; 2]; s.f = None; s.r = None;
                s.wl = [false; 2]; s.rl = false; s.wrl = false;
                s.blk = s.pb; s.lwd = 0; s.crashed = true;
                s.snap = [false; NBLK]; s.zw_done = false;
            }
            A::Recover => {
                for b in 0..NBLK {
                    if s.flu_present && s.flu[b] > 0 && s.ssd[b] == 0 { s.ssd[b] = s.flu[b]; }
                    if s.blk[b] == BS::Syncing { s.blk[b] = BS::Dirty; }
                    if s.blk[b] == BS::Clean { s.blk[b] = BS::NP; }
                    if s.wal[b] > 0 && s.blk[b] == BS::NP { s.blk[b] = BS::Dirty; }
                    s.pb[b] = s.blk[b];
                }
                if s.flu_present { s.flu_present = false; s.flu = [0; NBLK]; }
                s.crashed = false;
            }
            A::SW(i, b) => { s.w[i] = Some((b, W::CheckState)); s.wz[i] = false; }
            A::SZW(i, b) => { s.w[i] = Some((b, W::CheckState)); s.wz[i] = true; }
            A::TW(i) => {
                let (b, ph) = s.w[i].as_ref().unwrap().clone();
                let is_zero = s.wz[i];
                let write_ver = if is_zero { 0 } else { s.nv };
                s.w[i] = Some((b, match ph {
                    W::CheckState => W::Observed(s.blk[b]),
                    W::Observed(saw) => match saw {
                        BS::Dirty | BS::Syncing => W::AcquireLock,
                        BS::Clean => W::CheckState,
                        BS::NP => if self.sub && self.s3 { W::HasS3Check } else { W::AcquireLock },
                    },
                    W::AcquireLock => { s.wl[i] = true; W::SetPresent(write_ver) }
                    W::SetPresent(_) => { if s.blk[b] == BS::NP { s.blk[b] = BS::Clean; } W::PromoteCloneArc }
                    W::PromoteCloneArc => W::PromoteHasArc(s.flu_present, s.flu[b]),
                    W::PromoteHasArc(has, d) => {
                        if s.blk[b] != BS::Syncing && s.blk[b] != BS::NP {
                            W::PromoteSkip
                        } else if has {
                            W::PromotePread(d)
                        } else {
                            s.wl[i] = false; W::CheckState
                        }
                    }
                    W::PromotePread(d) => W::PromotePwrite(d),
                    W::PromotePwrite(d) => { s.ssd[b] = d; W::PromoteCas },
                    W::PromoteCas => {
                        if s.blk[b] == BS::Syncing || s.blk[b] == BS::NP { s.blk[b] = BS::Dirty; }
                        W::PwriteData(write_ver)
                    }
                    W::PromoteSkip => W::PwriteData(write_ver),
                    W::HasS3Check => W::LocateIsPresent(s.blk[b]),
                    W::LocateIsPresent(cs) => match cs {
                        BS::Dirty | BS::Syncing => W::LocateHotPread(cs),
                        _ => W::LocateColdFetch,
                    },
                    W::LocateHotPread(_) => {
                        match s.blk[b] {
                            BS::Dirty => W::FetchDone(s.ssd[b]),
                            BS::Syncing if s.flu_present => W::FetchDone(s.flu[b]),
                            _ => { return Some({ s.w[i] = Some((b, W::LocateColdFetch)); s }); }
                        }
                    }
                    W::LocateColdFetch => W::FetchDone(s.s3[b]),
                    W::FetchDone(p) => W::RecheckState(p),
                    W::RecheckState(p) => { if s.blk[b] != BS::NP { W::CheckState } else { W::ZerosCheck(p) } }
                    W::ZerosCheck(p) => { if p == 0 { W::AcquireLock } else { W::BeforeCas(p) } }
                    W::BeforeCas(p) => {
                        let w = s.blk[b] == BS::NP;
                        if w { s.blk[b] = BS::Clean; }
                        W::CasResult(p, w)
                    }
                    W::CasResult(_, w) => { if w { W::AcquireLock } else { W::CheckState } }
                    W::PwriteData(v) => { s.ssd[b] = v; s.ev[b] = v; W::TransitionDirty }
                    // Dirty before WAL: checkpoint persists state then
                    // truncates WAL. Old order (append→dirty) let checkpoint
                    // persist Clean and discard the entry. New order ensures
                    // checkpoint always sees Dirty.
                    W::TransitionDirty => {
                        s.blk[b] = BS::Dirty;
                        if is_zero { s.zw_done = true; } else { s.nv += 1; }
                        W::WalAppend(write_ver)
                    }
                    W::WalAppend(v) => { s.wal[b] = v; W::ReleaseLock }
                    W::ReleaseLock => {
                        s.wl[i] = false;
                        if !is_zero {
                            let v = s.nv - 1;
                            if v > s.lwd { s.lwd = v; }
                        }
                        W::Done
                    }
                    W::Done => W::Done,
                }));
            }
            A::SF => { s.f = Some(F::CrcPrepass); }
            A::TF => {
                let ph = s.f.as_ref().unwrap().clone();
                s.f = Some(match ph {
                    F::CrcPrepass => F::AcquireWriteLock,
                    F::AcquireWriteLock => { s.wrl = true; F::Snapshot }
                    F::Snapshot => {
                        for b in 0..NBLK {
                            s.snap[b] = s.blk[b] == BS::Dirty;
                            if s.blk[b] == BS::Dirty { s.blk[b] = BS::Syncing; }
                        }
                        F::Rotate
                    }
                    F::Rotate => {
                        for b in 0..NBLK {
                            if s.blk[b] == BS::Syncing {
                                s.flu[b] = s.ssd[b];
                                s.ssd[b] = 0;
                            }
                        }
                        s.flu_present = true;
                        F::ReleaseWriteLock
                    }
                    F::ReleaseWriteLock => { s.wrl = false; F::Compute }
                    F::Compute => F::Upload,
                    F::Upload => {
                        // Models the cross-flush dedup optimization:
                        // - Non-zero data (flu > 0): always upload.
                        // - Zero data + prior S3 data (man > 0): upload tombstone.
                        // - Zero data + no prior S3 data (man == 0): skip (dedup).
                        for b in 0..NBLK {
                            if s.snap[b] {
                                if s.flu[b] > 0 {
                                    s.s3[b] = s.flu[b];
                                } else if s.man[b] > 0 {
                                    s.s3[b] = 0;
                                }
                                // flu == 0 && man == 0: skip. s3 stays 0.
                            }
                        }
                        F::ManifestSync
                    }
                    F::ManifestSync => { s.man = s.s3; F::Evict }
                    F::Evict => {
                        for b in 0..NBLK {
                            if s.blk[b] == BS::Syncing { s.blk[b] = BS::NP; }
                        }
                        F::Checkpoint
                    }
                    F::Checkpoint => { s.pb = s.blk; s.wal = [0; NBLK]; F::Cleanup }
                    F::Cleanup => {
                        s.flu_present = false; s.flu = [0; NBLK];
                        s.snap = [false; NBLK];
                        F::Done
                    }
                    F::Done => F::Done,
                });
            }
            A::SR(b) => { s.r = Some((b, R::AcquireReadLock)); }
            A::TR => {
                let (b, ph) = s.r.as_ref().unwrap().clone();
                s.r = Some((b, match ph {
                    R::AcquireReadLock => { s.rl = true; R::FastPathCheck }
                    R::FastPathCheck => {
                        if s.blk[b] == BS::Dirty { R::FastPathPread(s.ssd[b]) }
                        else { s.rl = false; R::LocateStateCheck }
                    }
                    R::FastPathPread(v) => R::ReleaseAfterFast(v),
                    R::ReleaseAfterFast(v) => { s.rl = false; R::Done(v) }
                    R::LocateStateCheck => match s.blk[b] {
                        BS::Dirty | BS::Syncing => R::SlowAcquireLock,
                        _ => R::ColdFetchS3,
                    },
                    R::SlowAcquireLock => { s.rl = true; R::SlowRecheck }
                    R::SlowRecheck => {
                        let v = match s.blk[b] {
                            BS::NP | BS::Clean => { s.rl = false; return Some({ s.r = Some((b, R::ColdFetchS3)); s }); }
                            BS::Syncing if s.flu_present => s.flu[b],
                            BS::Dirty => s.ssd[b],
                            _ => { s.rl = false; return Some({ s.r = Some((b, R::ColdFetchS3)); s }); }
                        };
                        s.rl = false; R::Done(v)
                    }
                    R::ColdFetchS3 => R::Done(s.s3[b]),
                    R::Done(v) => R::Done(v),
                }));
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Non-zero data must be recoverable somewhere — unless every
            // block that was written non-zero has since been overwritten
            // by a zero write (or a write is in progress).
            Property::always("no data loss", |_: &M, s: &S| {
                if s.crashed || s.lwd == 0 { return true; }
                let any_writer_active = s.w.iter().any(|w|
                    w.as_ref().is_some_and(|(_, ph)| !matches!(ph, W::Done)));
                if any_writer_active { return true; }
                // Every block with a non-zero expected value must have
                // that value somewhere (ssd, flu, or s3).
                for b in 0..NBLK {
                    if s.ev[b] > 0 {
                        if s.ssd[b] != s.ev[b] && s.flu[b] != s.ev[b] && s.s3[b] != s.ev[b] {
                            return false;
                        }
                    }
                }
                true
            }),
            // DIRTY block with ssd==0 is valid only if: a zero-writer
            // is active for this block, OR a write is about to land, OR
            // data exists in flu/s3 for recovery.
            Property::always("dirty implies ssd", |m: &M, s: &S| {
                if s.crashed { return true; }
                for b in 0..NBLK {
                    if s.blk[b] == BS::Dirty && s.ssd[b] == 0 {
                        // ev[b] == 0 means last completed write was zero.
                        if s.ev[b] == 0 { continue; }
                        // Any writer active for this block: ssd may be
                        // in transit (between promote and PwriteData).
                        let writer_active = s.w.iter().any(|w|
                            matches!(w, Some((wb, ph)) if *wb == b && !matches!(ph, W::Done)));
                        if writer_active { continue; }
                        let de = s.s3[b] > 0 || s.flu[b] > 0;
                        if !de && !m.crash { return false; }
                    }
                }
                true
            }),
            // SYNCING block must have data in flushing file — unless
            // it was a zero write (flu[b] == 0 is valid for snapshotted
            // zero-written blocks).
            Property::always("syncing has flushing", |_: &M, s: &S| {
                if s.crashed { return true; }
                for b in 0..NBLK {
                    if s.blk[b] == BS::Syncing && s.flu_present && s.flu[b] == 0 {
                        // Zero-written blocks have flu[b] == 0. Valid if
                        // snapshotted in this flush cycle.
                        if s.snap[b] { continue; }
                        return false;
                    }
                }
                true
            }),
            Property::always("fast path dirty only", |_: &M, s: &S| {
                if let Some((b, R::FastPathPread(_))) = s.r { s.blk[b] == BS::Dirty }
                else { true }
            }),
            Property::always("write lock discipline", |_: &M, s: &S| {
                if s.wrl { matches!(s.f, Some(F::Snapshot) | Some(F::Rotate) | Some(F::ReleaseWriteLock)) }
                else { true }
            }),
            // The critical dedup invariant: after flush + evict, reading
            // a NP block from S3 must return its expected value. This
            // validates that the "skip zero upload when man==0" optimization
            // doesn't cause stale reads.
            //
            // Only checked in non-crash configurations: ev is set at
            // PwriteData but a crash before Upload loses the write.
            // That's a pre-existing crash recovery property, not a
            // dedup concern. Crash recovery is covered by the existing
            // "no data loss" property.
            Property::always("s3 matches expected for evicted blocks", |m: &M, s: &S| {
                if s.crashed || m.crash { return true; }
                // Only check when no flush or writer is in progress
                // (ev and s3 may be transiently inconsistent mid-operation).
                let flush_active = s.f.as_ref().is_some_and(|f| !matches!(f, F::Done));
                let any_writer_active = s.w.iter().any(|w|
                    w.as_ref().is_some_and(|(_, ph)| !matches!(ph, W::Done)));
                if flush_active || any_writer_active { return true; }
                for b in 0..NBLK {
                    if s.blk[b] == BS::NP && s.s3[b] != s.ev[b] {
                        return false;
                    }
                }
                true
            }),
            Property::sometimes("write done", |_: &M, s: &S| {
                s.w.iter().any(|w| matches!(w, Some((_, W::Done))))
            }),
            Property::sometimes("flush done", |_: &M, s: &S| { matches!(s.f, Some(F::Done)) }),
            Property::sometimes("read done", |_: &M, s: &S| { matches!(s.r, Some((_, R::Done(_)))) }),
            // Only check reachability of zero writes when enabled.
            Property::sometimes("zero write done", |m: &M, s: &S| {
                if !m.zero_writes { return true; }
                s.zw_done
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(s3: bool, sub: bool, crash: bool, zw: bool) -> impl Checker<M> {
        M { s3, sub, crash, zero_writes: zw }.checker().threads(4).spawn_dfs().join()
    }

    // Zero-write interleavings (no crash — exhaustive dedup verification).
    #[test] fn sub_s3_zw() { let r = run(true, true, false, true); eprintln!("{} states", r.unique_state_count()); r.assert_properties(); }
    #[test] fn full_s3_zw() { let r = run(true, false, false, true); eprintln!("{} states", r.unique_state_count()); r.assert_properties(); }
    #[test] fn sub_no_zw() { let r = run(false, true, false, true); eprintln!("{} states", r.unique_state_count()); r.assert_properties(); }
    #[test] fn full_no_zw() { let r = run(false, false, false, true); eprintln!("{} states", r.unique_state_count()); r.assert_properties(); }

    // Original tests (no zero writes — preserves baseline coverage).
    #[test] fn sub_s3() { let r = run(true, true, false, false); eprintln!("{} states", r.unique_state_count()); r.assert_properties(); }
    #[test] fn full_s3() { let r = run(true, false, false, false); eprintln!("{} states", r.unique_state_count()); r.assert_properties(); }
    #[test] fn sub_no() { let r = run(false, true, false, false); eprintln!("{} states", r.unique_state_count()); r.assert_properties(); }
    #[test] fn full_no() { let r = run(false, false, false, false); eprintln!("{} states", r.unique_state_count()); r.assert_properties(); }

    // Crash recovery (no zero writes — keeps state space tractable).
    #[test] fn crash() {
        let m = M { s3: true, sub: true, crash: true, zero_writes: false };
        let r = m.checker().threads(4).spawn_dfs().join();
        eprintln!("{} states", r.unique_state_count());
        r.assert_properties();
    }
}
