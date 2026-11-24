//! When a cut may publish its manifest, as pure functions proven with Verus, called verbatim by `glidefs`'s
//! `WriteCache::flush_until_no_skips` (the flush that every cut, owned or not, runs before it serialises the manifest it publishes).
//!
//! A cut flushes the dirty blocks and then publishes the volume manifest as the snapshot. The flush runs in passes: a pass claims every
//! dirty block, uploads it and names it in the manifest, except a block it skips (a write landed between the CRC drain and the rotation, so
//! the drained baseline was stale); a skipped block goes back to DIRTY and the next pass takes it. A pass that ends with no skip has
//! flushed every block that was dirty at its rotation. A pass that never rotated (an orphaned flushing file the cache could not remove)
//! flushed nothing and reports no skip.
//!
//! * [`pass_verdict`]: `Publish` only when the pass skipped no block and did rotate; `Retry` for a skip a write race explains, while
//!   passes remain; `Fail` for an unexplained skip, a spent budget or a pass that never rotated.
//! * The model ([`Cut`], [`cstep`]) is one cut over the flush kernel's volume ([`crate::flush_kernel::Vol`]): the guest's writes and the
//!   passes, each pass built from `flush_kernel::step_spec`. [`cut_inv`] is its inductive invariant.
//! * [`thm_a_published_cut_holds_every_write_acked_before_it`]: a pass the verdict publishes leaves the store's manifest naming, for every
//!   block, a write at least as new as the last one acknowledged before the cut began.
//!
//! Mutations (`scripts/verify-kernels.sh --features mut_x` must go red): `mut_cut_publishes_with_skips` (a skip a write race explains
//! publishes), `mut_cut_ignores_blocked_flush` (a pass that never rotated publishes).

use crate::flush_kernel::Vol;
#[cfg(verus_only)]
use crate::flush_kernel::{inv as vol_inv, step_spec, Event, Phase, VEvent};
use vstd::prelude::*;

verus! {

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The pass flushed every block that was dirty when it rotated: serialise the manifest.
    Publish,
    /// A write raced the pass and a block was skipped: flush again.
    Retry,
    /// Publish nothing: the skip is unexplained, the passes are spent, or the pass never rotated.
    Fail,
}

pub open spec fn verdict_spec(skipped: int, raced: int, blocked: bool, pass: int, max_passes: int) -> Verdict {
    if blocked {
        Verdict::Fail
    } else if skipped == 0 {
        Verdict::Publish
    } else if raced < skipped || pass >= max_passes {
        Verdict::Fail
    } else {
        Verdict::Retry
    }
}

/// `skipped`: the blocks the pass skipped (`FlushStats::blocks_crc_mismatched`: every skipped block counts once); `raced`: the part of them a
/// concurrent write explains; `blocked`: the pass returned before it rotated; `pass`: 1-based pass number; `max_passes`: the budget.
#[cfg(not(any(feature = "mut_cut_publishes_with_skips", feature = "mut_cut_ignores_blocked_flush")))]
pub fn pass_verdict(skipped: usize, raced: usize, blocked: bool, pass: usize, max_passes: usize) -> (r: Verdict)
    ensures
        r == verdict_spec(skipped as int, raced as int, blocked, pass as int, max_passes as int),
        r is Publish ==> skipped == 0 && !blocked,
        blocked ==> r is Fail,
        // A retry stays inside the budget.
        r is Retry ==> pass < max_passes,
{
    if blocked {
        Verdict::Fail
    } else if skipped == 0 {
        Verdict::Publish
    } else if raced < skipped || pass >= max_passes {
        Verdict::Fail
    } else {
        Verdict::Retry
    }
}

/// Mutations: `mut_cut_publishes_with_skips` publishes when every skip is explained by a write race; `mut_cut_ignores_blocked_flush` treats a
/// pass that never rotated as a clean one.
#[cfg(any(feature = "mut_cut_publishes_with_skips", feature = "mut_cut_ignores_blocked_flush"))]
pub fn pass_verdict(skipped: usize, raced: usize, blocked: bool, pass: usize, max_passes: usize) -> (r: Verdict)
    ensures
        r == verdict_spec(skipped as int, raced as int, blocked, pass as int, max_passes as int),
        r is Publish ==> skipped == 0 && !blocked,
        blocked ==> r is Fail,
        r is Retry ==> pass < max_passes,
{
    if blocked && !cfg!(feature = "mut_cut_ignores_blocked_flush") {
        Verdict::Fail
    } else if skipped == 0 || (cfg!(feature = "mut_cut_publishes_with_skips") && raced == skipped) {
        Verdict::Publish
    } else if raced < skipped || pass >= max_passes {
        Verdict::Fail
    } else {
        Verdict::Retry
    }
}

// ---- The model the verdict is proven against: one cut of one volume ----

/// One cut: the volume, and each block's last acknowledged write (a version) at the moment the cut began.
pub struct Cut {
    pub v: Vol,
    pub start: spec_fn(int) -> int,
}

pub enum CutEvent {
    /// The guest's next write to block `b` is acknowledged (a version above the last).
    Write { b: int },
    /// A flush pass: `skipped` the blocks it skipped, `blocked` that it returned before it rotated.
    Pass { skipped: Set<int>, blocked: bool },
}

/// What a pass leaves: every dirty block it did not skip is claimed, uploaded and named by the manifest (then evicted); a skipped block is
/// claimed and given back. The phases are `flush_kernel::step_spec`.
pub open spec fn passed(v: Vol, skipped: Set<int>) -> Vol {
    Vol {
        phase: |b: int|
            if skipped.contains(b) {
                step_spec(step_spec((v.phase)(b), Event::Claim), Event::Requeue)
            } else {
                step_spec(step_spec((v.phase)(b), Event::Claim), Event::Evict { covered: true })
            },
        claimed: |b: int| if (v.phase)(b) == Phase::Dirty { (v.local)(b) } else { (v.claimed)(b) },
        store: |b: int| if (v.phase)(b) == Phase::Dirty && !skipped.contains(b) { (v.local)(b) } else { (v.store)(b) },
        ..v
    }
}

/// A pass skips only dirty blocks, and a pass that never rotated skipped none. A flush that finds a block stranded SYNCING fails before it
/// rotates (`CacheError::FlushStranded`), so a pass starts with no block SYNCING.
pub open spec fn enabled(c: Cut, e: CutEvent) -> bool {
    match e {
        CutEvent::Write { b } => true,
        CutEvent::Pass { skipped, blocked } => {
            &&& skipped.finite()
            &&& forall|b: int| skipped.contains(b) ==> #[trigger] (c.v.phase)(b) == Phase::Dirty
            &&& blocked ==> skipped == Set::<int>::empty()
        },
    }
}

pub open spec fn cstep(c: Cut, e: CutEvent) -> Cut {
    match e {
        CutEvent::Write { b } => Cut { v: crate::flush_kernel::vstep(c.v, VEvent::Write { b, v: (c.v.latest)(b) + 1 }), ..c },
        CutEvent::Pass { skipped, blocked } => if blocked {
            c
        } else {
            Cut { v: passed(c.v, skipped), ..c }
        },
    }
}

/// The cut's invariant: the volume is in the flush kernel's invariant, no block is SYNCING between passes, and no block's last write is older
/// than the one acknowledged when the cut began.
pub open spec fn cut_inv(c: Cut) -> bool {
    &&& vol_inv(c.v)
    &&& forall|b: int| #[trigger] (c.v.phase)(b) != Phase::Syncing
    &&& forall|b: int| #[trigger] (c.v.latest)(b) >= (c.start)(b)
}

/// A cut begins on a volume in the flush kernel's invariant with nothing stranded; its start is the volume's last acknowledged writes.
pub proof fn lemma_a_cut_starts_in_the_invariant(v: Vol)
    requires
        vol_inv(v),
        forall|b: int| #[trigger] (v.phase)(b) != Phase::Syncing,
    ensures
        cut_inv(Cut { v, start: v.latest }),
{
}

pub proof fn lemma_every_event_keeps_the_invariant(c: Cut, e: CutEvent)
    requires
        cut_inv(c),
        enabled(c, e),
    ensures
        cut_inv(cstep(c, e)),
{
    match e {
        CutEvent::Write { b } => {
            let w = cstep(c, e);
            crate::flush_kernel::lemma_every_step_keeps_the_invariant(c.v, VEvent::Write { b, v: (c.v.latest)(b) + 1 });
            assert forall|i: int| #[trigger] (w.v.phase)(i) != Phase::Syncing by {
                if i != b {
                    assert((w.v.phase)(i) == (c.v.phase)(i));
                }
            }
            assert forall|i: int| #[trigger] (w.v.latest)(i) >= (c.start)(i) by {
                if i != b {
                    assert((w.v.latest)(i) == (c.v.latest)(i));
                }
            }
        },
        CutEvent::Pass { skipped, blocked } => {
            if !blocked {
                let w = cstep(c, e);
                assert forall|i: int| {
                    &&& #[trigger] (w.v.phase)(i) == Phase::NotPresent ==> (w.v.store)(i) == (w.v.latest)(i)
                    &&& (w.v.phase)(i) == Phase::Clean ==> (w.v.store)(i) == (w.v.latest)(i)
                    &&& (w.v.phase)(i) == Phase::Syncing ==> (w.v.claimed)(i) == (w.v.latest)(i)
                    &&& (w.v.phase)(i) != Phase::NotPresent ==> (w.v.local)(i) == (w.v.latest)(i)
                } by {
                    assert((c.v.phase)(i) != Phase::Syncing);
                    assert((c.v.phase)(i) == Phase::NotPresent ==> (c.v.store)(i) == (c.v.latest)(i));
                    assert((c.v.phase)(i) != Phase::NotPresent ==> (c.v.local)(i) == (c.v.latest)(i));
                }
                assert forall|i: int| #[trigger] (w.v.phase)(i) != Phase::Syncing by {
                    assert((c.v.phase)(i) != Phase::Syncing);
                }
            }
        },
    }
}

/// P.snapshot.a-snapshot-of-a-light-asleep-sandbox-holds-a-file-written-before-the-sleep (GlideFS's part): a cut that publishes after a
/// pass the verdict approved holds, for every block, a write at least as new as the one acknowledged before the cut began; the guest's
/// writes between the passes, and the passes the verdict retried, are in the invariant.
pub proof fn thm_a_published_cut_holds_every_write_acked_before_it(
    c: Cut,
    skipped: Set<int>,
    raced: int,
    blocked: bool,
    pass: int,
    max_passes: int,
)
    requires
        cut_inv(c),
        enabled(c, CutEvent::Pass { skipped, blocked }),
        verdict_spec(skipped.len() as int, raced, blocked, pass, max_passes) is Publish,
    ensures
        forall|b: int| #[trigger] (cstep(c, CutEvent::Pass { skipped, blocked }).v.store)(b) >= (c.start)(b),
{
    assert(!blocked);
    assert(skipped.len() == 0);
    assert(skipped == Set::<int>::empty()) by {
        skipped.lemma_len0_is_empty();
    }
    let w = cstep(c, CutEvent::Pass { skipped, blocked });
    assert forall|b: int| #[trigger] (w.v.store)(b) >= (c.start)(b) by {
        assert((c.v.phase)(b) != Phase::Syncing);
        assert((c.v.latest)(b) >= (c.start)(b));
        assert((c.v.phase)(b) == Phase::NotPresent ==> (c.v.store)(b) == (c.v.latest)(b));
        assert((c.v.phase)(b) == Phase::Clean ==> (c.v.store)(b) == (c.v.latest)(b));
        assert((c.v.phase)(b) != Phase::NotPresent ==> (c.v.local)(b) == (c.v.latest)(b));
    }
}

} // verus!
