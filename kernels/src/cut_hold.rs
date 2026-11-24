//! Cuts of one export are serialised: the owner check of an owned cut, its flush, its marker and its manifest all run under one hold of the
//! export's `flush_lock`, so two cuts of an export never interleave. `WriteCache::snapshot_owned` (and `flush_to_s3`) take the lock and mint
//! the one [`CutHold`]; every step of a cut takes `&CutHold` (`owned_cut_view`, `flush_until_no_skips`, `put_cut_marker`,
//! `put_cut_manifest`), so a step cannot be called without a hold, and a hold cannot outlive the guard it borrows.
//!
//! * [`CutHold`]: the capability. A private field, no `Clone`, `Copy`, `Debug`, `Display`, `Serialize` or `Deref` (FA-rust-privacy): made
//!   only by [`CutHold::under`], which borrows the lock's guard. glidefs's `cut_hold_structure` pin sees every mint and every sink.
//! * The model below ([`Cuts`], [`cstep`]) is every cut of one export with the lock as `holder`: a cut's steps are enabled only for the
//!   holder, and the lock is taken only when free. [`inv`] is its inductive invariant.
//! * [`thm_cuts_of_an_export_never_interleave`]: in every reachable state at most one cut is inside its body (between its owner check
//!   and its last write), and a cut's body steps run from one acquisition to its release.
//!
//! Mutation (`scripts/verify-kernels.sh --features mut_cut_steps_skip_the_lock` must go red): a body step is enabled without the hold.

use std::marker::PhantomData;
use vstd::prelude::*;

/// Proof that the holder holds an export's `flush_lock`: made only from the lock's guard, and valid for as long as the guard is borrowed.
/// Every step of an owned cut takes `&CutHold`.
pub struct CutHold<'g> {
    held: PhantomData<&'g ()>,
}

impl<'g> CutHold<'g> {
    /// The one mint: `guard` is the guard `flush_lock.lock().await` returned. glidefs's `cut_hold_structure` test pins every call to a place
    /// that has just taken the lock.
    pub fn under<G>(_guard: &'g G) -> CutHold<'g> {
        CutHold { held: PhantomData }
    }
}

verus! {

/// Where a cut of the export is. `Idle` before it takes the lock and `Done` after it released it; between, it holds the lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Idle,
    /// Holds the lock, nothing done yet.
    Held,
    /// The owner check ran.
    Checked,
    /// The flush ran.
    Flushed,
    /// The owner marker was written.
    Marked,
    /// The manifest was written.
    Written,
    Done,
}

/// Every cut of one export (indexed by any integer) and who holds `flush_lock`.
pub struct Cuts {
    pub holder: Option<int>,
    pub phase: spec_fn(int) -> Phase,
}

pub enum CEvent {
    /// `flush_lock.lock().await` returned for the cut.
    Take(int),
    /// The owner check (`owned_cut_view`, `owned_cut_plan`).
    Check(int),
    /// The flush (`flush_until_no_skips`).
    Flush(int),
    /// The marker write (`put_cut_marker`).
    Mark(int),
    /// The manifest write (`put_cut_manifest`).
    Write(int),
    /// The guard dropped: the cut ended, answered, refused or finished.
    Release(int),
}

/// Inside its body: holds the lock.
pub open spec fn in_body(p: Phase) -> bool {
    p is Held || p is Checked || p is Flushed || p is Marked || p is Written
}

pub open spec fn holds(c: Cuts, i: int) -> bool {
    c.holder == Some(i)
}

#[cfg(not(feature = "mut_cut_steps_skip_the_lock"))]
pub open spec fn body_may_run(c: Cuts, i: int) -> bool {
    holds(c, i)
}

/// Mutation: a body step runs whether or not the cut holds the lock.
#[cfg(feature = "mut_cut_steps_skip_the_lock")]
pub open spec fn body_may_run(c: Cuts, i: int) -> bool {
    true
}

/// The cut a step belongs to.
pub open spec fn subject(e: CEvent) -> int {
    match e {
        CEvent::Take(i) | CEvent::Check(i) | CEvent::Flush(i) | CEvent::Mark(i) | CEvent::Write(i) | CEvent::Release(i) => i,
    }
}

pub open spec fn cenabled(c: Cuts, e: CEvent) -> bool {
    match e {
        CEvent::Take(i) => c.holder is None && (c.phase)(i) is Idle,
        CEvent::Check(i) => body_may_run(c, i) && ((c.phase)(i) is Held || (c.phase)(i) is Idle),
        CEvent::Flush(i) => body_may_run(c, i) && (c.phase)(i) is Checked,
        CEvent::Mark(i) => body_may_run(c, i) && (c.phase)(i) is Flushed,
        CEvent::Write(i) => body_may_run(c, i) && (c.phase)(i) is Marked,
        CEvent::Release(i) => holds(c, i) && in_body((c.phase)(i)),
    }
}

pub open spec fn cstep(c: Cuts, e: CEvent) -> Cuts {
    match e {
        CEvent::Take(i) => Cuts { holder: Some(i), phase: |j: int| if j == i { Phase::Held } else { (c.phase)(j) } },
        CEvent::Check(i) => Cuts { phase: |j: int| if j == i { Phase::Checked } else { (c.phase)(j) }, ..c },
        CEvent::Flush(i) => Cuts { phase: |j: int| if j == i { Phase::Flushed } else { (c.phase)(j) }, ..c },
        CEvent::Mark(i) => Cuts { phase: |j: int| if j == i { Phase::Marked } else { (c.phase)(j) }, ..c },
        CEvent::Write(i) => Cuts { phase: |j: int| if j == i { Phase::Written } else { (c.phase)(j) }, ..c },
        CEvent::Release(i) => Cuts { holder: None, phase: |j: int| if j == i { Phase::Done } else { (c.phase)(j) } },
    }
}

pub open spec fn start() -> Cuts {
    Cuts { holder: None, phase: |j: int| Phase::Idle }
}

/// Only the holder is inside a body, and a free lock means no cut is.
pub open spec fn inv(c: Cuts) -> bool {
    &&& forall|i: int| in_body(#[trigger] (c.phase)(i)) ==> holds(c, i)
    &&& c.holder is Some ==> in_body((c.phase)(c.holder->Some_0))
}

pub proof fn lemma_the_start_is_in_the_invariant()
    ensures
        inv(start()),
{
}

pub proof fn lemma_every_event_keeps_the_invariant(c: Cuts, e: CEvent)
    requires
        inv(c),
        cenabled(c, e),
    ensures
        inv(cstep(c, e)),
{
    let w = cstep(c, e);
    assert forall|k: int| in_body(#[trigger] (w.phase)(k)) implies holds(w, k) by {
        if k != subject(e) {
            // A step changes only its own cut's phase, so another cut in its body was in it before, and held the lock.
            assert((w.phase)(k) == (c.phase)(k));
        }
        match e {
            // The lock was free, so no cut was in its body.
            CEvent::Take(i) => {},
            // The releasing cut was the holder; every other cut in its body would also have held the lock.
            CEvent::Release(i) => {},
            _ => {},
        }
    }
}

/// P.snapshot.glidefs-cuts-of-an-export-are-serialised: in every reachable state at most one cut is inside its body, so the steps of two cuts of
/// an export never interleave; and the owner check, the flush, the marker and the manifest of a cut run in one acquisition (a step of a cut is
/// enabled only while it holds the lock, which `Release` is the only way to give up).
pub proof fn thm_cuts_of_an_export_never_interleave(c: Cuts, i: int, j: int)
    requires
        inv(c),
        in_body((c.phase)(i)),
        in_body((c.phase)(j)),
    ensures
        i == j,
{
    assert(holds(c, i));
    assert(holds(c, j));
}

pub proof fn thm_a_cut_runs_its_check_and_its_writes_in_one_hold(c: Cuts, i: int)
    requires
        inv(c),
        (c.phase)(i) is Marked || (c.phase)(i) is Written,
    ensures
        holds(c, i),
        // No other cut has taken a step of its body since this cut took the lock.
        forall|j: int| j != i ==> !in_body(#[trigger] (c.phase)(j)),
{
}

} // verus!
