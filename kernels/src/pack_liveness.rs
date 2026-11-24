//! Which packs of a store a disk's deletion and the garbage collector may take, as pure functions proven with Verus, called verbatim by
//! `glidefs`'s `cli::gc::reconcile_prefix` (the verdict on every pack no live manifest names) and the collector is the only deleter of a pack: a disk's purge (`ExportRouter::purge_s3_state`)
//! deletes its manifest, snapshots, snapshot index and definition and no pack, which the model's `Purge` event states.
//!
//! A disk forked from a snapshot disk copies the snapshot disk's manifest and puts it in the store before it serves a read
//! (`ExportRouter::create_export`): the packs the child names are named by the child's own manifest, whatever becomes of the snapshot disk.
//! The only deleter of a pack is the collector, and [`pack_verdict`] reclaims only a pack no manifest and no snapshot names.
//!
//! * [`pack_verdict`]: `Keep` for a pack any live manifest or snapshot names; otherwise `Reclaim` once the grace period has elapsed, else `Wait`.
//! * [`thm_a_snapshot_child_disk_does_not_depend_on_its_snapshot_disk`]: the disk forked from a snapshot disk keeps every pack it names in the
//!   store after the snapshot disk is purged and the collector then runs, however often.
//!
//! Mutations (`scripts/verify-kernels.sh --features mut_x` must go red): `mut_gc_reclaims_named` (a named pack past its grace period is
//! reclaimed), `mut_gc_ignores_manifests` (only snapshots name a pack).

use vstd::prelude::*;

verus! {

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// A live manifest or a snapshot names the pack.
    Keep,
    /// Named by nothing, inside its grace period: marked dead, not yet deleted.
    Wait,
    /// Named by nothing and past its grace period: deleted.
    Reclaim,
}

pub open spec fn verdict_spec(in_manifest: bool, in_snapshot: bool, grace_elapsed: bool) -> Verdict {
    if in_manifest || in_snapshot {
        Verdict::Keep
    } else if grace_elapsed {
        Verdict::Reclaim
    } else {
        Verdict::Wait
    }
}

#[cfg(not(any(feature = "mut_gc_reclaims_named", feature = "mut_gc_ignores_manifests")))]
pub fn pack_verdict(in_manifest: bool, in_snapshot: bool, grace_elapsed: bool) -> (v: Verdict)
    ensures
        v == verdict_spec(in_manifest, in_snapshot, grace_elapsed),
{
    if in_manifest || in_snapshot {
        Verdict::Keep
    } else if grace_elapsed {
        Verdict::Reclaim
    } else {
        Verdict::Wait
    }
}

/// Mutations: `mut_gc_reclaims_named` reclaims a named pack once its grace period passed; `mut_gc_ignores_manifests` lets only a snapshot
/// name a pack.
#[cfg(any(feature = "mut_gc_reclaims_named", feature = "mut_gc_ignores_manifests"))]
pub fn pack_verdict(in_manifest: bool, in_snapshot: bool, grace_elapsed: bool) -> (v: Verdict)
    ensures
        v == verdict_spec(in_manifest, in_snapshot, grace_elapsed),
{
    if cfg!(feature = "mut_gc_reclaims_named") && grace_elapsed {
        Verdict::Reclaim
    } else if (in_manifest && !cfg!(feature = "mut_gc_ignores_manifests")) || in_snapshot {
        Verdict::Keep
    } else if grace_elapsed {
        Verdict::Reclaim
    } else {
        Verdict::Wait
    }
}

// ---- The model the kernels are proven against: one pool of packs and its disks ----

/// A pool: the packs in the store, and each live disk's manifest (the packs it names).
pub struct Pool {
    pub packs: ISet<int>,
    pub manifests: Map<int, ISet<int>>,
}

/// Whether any live manifest names the pack.
pub open spec fn named(pool: Pool, p: int) -> bool {
    exists|d: int| #[trigger] pool.manifests.dom().contains(d) && pool.manifests[d].contains(p)
}

pub enum PEvent {
    /// `child` is created from the disk `parent`'s manifest (a snapshot disk's), the manifest in the store before the child serves.
    Fork { parent: int, child: int },
    /// `disk` is purged: its manifest (and snapshots) leave; no pack does.
    Purge { disk: int },
    /// The collector ran: it reclaims the packs it judged `Reclaim`, `grace_elapsed` deciding which of the unnamed.
    Gc { grace_elapsed: spec_fn(int) -> bool },
}

pub open spec fn pstep(pool: Pool, e: PEvent) -> Pool {
    match e {
        PEvent::Fork { parent, child } => if pool.manifests.dom().contains(parent) {
            Pool { manifests: pool.manifests.insert(child, pool.manifests[parent]), ..pool }
        } else {
            pool
        },
        PEvent::Purge { disk } => Pool { manifests: pool.manifests.remove(disk), ..pool },
        PEvent::Gc { grace_elapsed } => Pool {
            packs: ISet::new(|p: int| pool.packs.contains(p) && verdict_spec(named(pool, p), false, grace_elapsed(p)) != Verdict::Reclaim),
            ..pool
        },
    }
}

/// Every pack a live disk names is in the store.
pub open spec fn inv(pool: Pool) -> bool {
    forall|d: int, p: int| pool.manifests.dom().contains(d) && #[trigger] pool.manifests[d].contains(p) ==> pool.packs.contains(p)
}

pub proof fn lemma_every_event_keeps_the_invariant(pool: Pool, e: PEvent)
    requires
        inv(pool),
    ensures
        inv(pstep(pool, e)),
{
    let w = pstep(pool, e);
    match e {
        PEvent::Fork { parent, child } => {
            assert forall|d: int, p: int| w.manifests.dom().contains(d) && #[trigger] w.manifests[d].contains(p) implies w.packs.contains(p) by {
                if d == child && pool.manifests.dom().contains(parent) {
                    assert(pool.manifests[parent].contains(p));
                }
            }
        },
        PEvent::Purge { disk } => {},
        PEvent::Gc { grace_elapsed } => {
            assert forall|d: int, p: int| w.manifests.dom().contains(d) && #[trigger] w.manifests[d].contains(p) implies w.packs.contains(p) by {
                assert(pool.manifests.dom().contains(d) && pool.manifests[d].contains(p));
                assert(named(pool, p));
            }
        },
    }
}

/// P.snapshot.a-snapshot-child-disk-does-not-depend-on-its-snapshot-disk: a disk forked from a snapshot disk keeps every chunk it names
/// readable after the snapshot disk is deleted: its packs stay in the store through the snapshot disk's purge and any collector run after it.
pub proof fn thm_a_snapshot_child_disk_does_not_depend_on_its_snapshot_disk(pool: Pool, parent: int, child: int, gc_before: spec_fn(int) -> bool, gc_after: spec_fn(int) -> bool)
    requires
        inv(pool),
        pool.manifests.dom().contains(parent),
        child != parent,
    ensures
        ({
            let forked = pstep(pool, PEvent::Fork { parent, child });
            let purged = pstep(forked, PEvent::Purge { disk: parent });
            let after = pstep(pstep(purged, PEvent::Gc { grace_elapsed: gc_before }), PEvent::Gc { grace_elapsed: gc_after });
            forall|p: int| pool.manifests[parent].contains(p) ==> after.manifests.dom().contains(child) && after.manifests[child].contains(p) && after.packs.contains(p)
        }),
{
    let forked = pstep(pool, PEvent::Fork { parent, child });
    lemma_every_event_keeps_the_invariant(pool, PEvent::Fork { parent, child });
    let purged = pstep(forked, PEvent::Purge { disk: parent });
    lemma_every_event_keeps_the_invariant(forked, PEvent::Purge { disk: parent });
    let g1 = pstep(purged, PEvent::Gc { grace_elapsed: gc_before });
    lemma_every_event_keeps_the_invariant(purged, PEvent::Gc { grace_elapsed: gc_before });
    lemma_every_event_keeps_the_invariant(g1, PEvent::Gc { grace_elapsed: gc_after });
}

} // verus!
