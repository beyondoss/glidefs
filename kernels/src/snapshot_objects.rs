//! The objects of a snapshot in the shared store, as pure functions proven with Verus, called verbatim by `glidefs`'s
//! `ExportRouter::snapshot_export_owned_inner` (the verdict on whether a cut is reported persisted).
//!
//! A cut of an export puts a manifest at `snapshots/{export}/{sequence}` (a copy of the export's volume manifest, naming the packs the
//! flush uploaded before it), and the router then saves the snapshot's index entry `index/snapshots/{export}@{sequence}`. A fork from the
//! snapshot on any node (`from: "snapshot:{id}"`: `ExportRouter::resolve_source`, then `create_export`) reads only those objects from the
//! store: the index entry, the snapshot manifest and the packs it names. It copies the snapshot manifest as the child's own manifest and
//! puts it in the store before the child serves a read. Deleting the source (a purge) deletes its manifest, snapshots, snapshot index
//! entries and definition and no pack; the collector reclaims a pack only when no manifest and no snapshot names it ([`pack_liveness`]).
//!
//! * [`snapshot_reported`]: a cut is reported persisted only when its manifest is stored and its index entry is saved.
//! * The model ([`Store`], [`step`]) is the shared store: packs, each disk's manifest, each snapshot's manifest, the index and the
//!   snapshots reported. A node holds nothing the store does not: a fork is enabled by what the store holds ([`fetchable`]), whichever node
//!   runs it. [`inv`] is its inductive invariant; [`step`] calls `pack_liveness::verdict_spec` for the collector.
//! * [`thm_a_reported_snapshot_is_fetchable_in_every_run`]: in every run from an empty store, a snapshot that was reported and not deleted is
//!   fetchable: its index entry, its manifest and every pack the manifest names are in the store (P.snapshot.a-snapshot-taken-on-one-node-is-obtainable-on-a-second-node).
//! * [`thm_a_clone_holds_the_snapshots_files_after_its_source_is_gone`]: a disk forked from a snapshot keeps, through the source's purge and
//!   any events after it that do not touch the child, the manifest the snapshot captured and every pack it names (P.snapshot.a-sandbox-created-from-a-filesystem-snapshot-has-the-files-the-snapshot-captured-after-the-source-is-gone).
//!
//! Mutations (`scripts/verify-kernels.sh --features mut_x` must go red): `mut_report_without_index` (a cut whose index entry was not saved
//! is reported persisted); `mut_gc_reclaims_named` (the collector step reclaims a named pack past its grace period: the clone theorem goes red;
//! it also turns `pack_liveness::pack_verdict`'s proof red). `mut_gc_ignores_manifests` turns `pack_verdict`'s proof red.

#[cfg(verus_only)]
use crate::pack_liveness::{verdict_spec, Verdict};
use vstd::prelude::*;

verus! {

pub open spec fn reported_spec(persisted: bool, indexed: bool) -> bool {
    persisted && indexed
}

/// `persisted`: the snapshot's manifest is in the store (`SnapshotResult::snapshot_persisted`); `indexed`: its index entry was saved.
#[cfg(not(feature = "mut_report_without_index"))]
pub fn snapshot_reported(persisted: bool, indexed: bool) -> (r: bool)
    ensures
        r == reported_spec(persisted, indexed),
        r ==> persisted && indexed,
{
    persisted && indexed
}

/// Mutation `mut_report_without_index`: the index entry is not asked.
#[cfg(feature = "mut_report_without_index")]
pub fn snapshot_reported(persisted: bool, indexed: bool) -> (r: bool)
    ensures
        r == reported_spec(persisted, indexed),
        r ==> persisted && indexed,
{
    persisted
}

// ---- The model: the shared store ----

/// A snapshot is named by its export and its sequence.
pub type Snap = (int, int);

pub struct Store {
    /// The packs in the store.
    pub packs: ISet<int>,
    /// Each live disk's manifest: the packs it names.
    pub manifests: Map<int, ISet<int>>,
    /// Each snapshot's manifest.
    pub snaps: Map<Snap, ISet<int>>,
    /// The snapshot index entries.
    pub index: ISet<Snap>,
    /// The snapshots a cut reported persisted.
    pub reported: ISet<Snap>,
}

pub enum Ev {
    /// A flush uploads a pack.
    Upload { p: int },
    /// A flush puts disk `d`'s manifest naming `names`, packs already uploaded.
    Sync { d: int, names: ISet<int> },
    /// A cut puts snapshot `(d, s)`'s manifest: the disk's, as the flush left it.
    Cut { d: int, s: int },
    /// The router saves the snapshot's index entry.
    Index { d: int, s: int },
    /// The router reports the cut persisted.
    Report { d: int, s: int },
    /// A fork of the snapshot on any node: `child`'s manifest is the snapshot's, put in the store.
    Fork { child: int, d: int, s: int },
    /// A purge of disk `d`: its manifest, snapshots, snapshot index entries and definition go; no pack does.
    Purge { d: int },
    /// The collector ran; `grace` says which unnamed packs are past their grace period.
    Gc { grace: spec_fn(int) -> bool },
}

/// Whether any live disk's manifest names the pack.
pub open spec fn named_by_a_manifest(st: Store, p: int) -> bool {
    exists|d: int| #[trigger] st.manifests.dom().contains(d) && st.manifests[d].contains(p)
}

/// Whether any snapshot's manifest names the pack.
pub open spec fn named_by_a_snapshot(st: Store, p: int) -> bool {
    exists|x: Snap| #[trigger] st.snaps.dom().contains(x) && st.snaps[x].contains(p)
}

/// What a fork needs from the store, on whichever node: the index entry, the snapshot's manifest, and every pack it names.
pub open spec fn fetchable(st: Store, x: Snap) -> bool {
    &&& st.index.contains(x)
    &&& st.snaps.dom().contains(x)
    &&& forall|p: int| #[trigger] st.snaps[x].contains(p) ==> st.packs.contains(p)
}

/// The collector's verdict on a pack in this model: `pack_liveness::verdict_spec`, the contract of the `pack_verdict` that `reconcile_prefix`
/// calls. Mutation `mut_gc_reclaims_named` reclaims a named pack once its grace period passed, which the clone theorem forbids.
#[cfg(not(feature = "mut_gc_reclaims_named"))]
pub open spec fn collector_verdict(in_manifest: bool, in_snapshot: bool, grace: bool) -> Verdict {
    verdict_spec(in_manifest, in_snapshot, grace)
}

#[cfg(feature = "mut_gc_reclaims_named")]
pub open spec fn collector_verdict(in_manifest: bool, in_snapshot: bool, grace: bool) -> Verdict {
    if grace { Verdict::Reclaim } else { verdict_spec(in_manifest, in_snapshot, grace) }
}

pub open spec fn enabled(st: Store, e: Ev) -> bool {
    match e {
        Ev::Upload { p } => true,
        // The manifest sync follows the upload of the packs it names (`flush_kernel`: Upload, ManifestSync, Evict).
        Ev::Sync { d, names } => forall|p: int| #[trigger] names.contains(p) ==> st.packs.contains(p),
        Ev::Cut { d, s } => st.manifests.dom().contains(d) && !st.snaps.dom().contains((d, s)),
        Ev::Index { d, s } => st.snaps.dom().contains((d, s)),
        Ev::Report { d, s } => reported_spec(st.snaps.dom().contains((d, s)), st.index.contains((d, s))),
        Ev::Fork { child, d, s } => fetchable(st, (d, s)) && !st.manifests.dom().contains(child),
        Ev::Purge { d } => true,
        Ev::Gc { grace } => true,
    }
}

pub open spec fn step(st: Store, e: Ev) -> Store {
    match e {
        Ev::Upload { p } => Store { packs: st.packs.insert(p), ..st },
        Ev::Sync { d, names } => Store { manifests: st.manifests.insert(d, names), ..st },
        Ev::Cut { d, s } => Store { snaps: st.snaps.insert((d, s), st.manifests[d]), ..st },
        Ev::Index { d, s } => Store { index: st.index.insert((d, s)), ..st },
        Ev::Report { d, s } => Store { reported: st.reported.insert((d, s)), ..st },
        Ev::Fork { child, d, s } => Store { manifests: st.manifests.insert(child, st.snaps[(d, s)]), ..st },
        Ev::Purge { d } => Store {
            manifests: st.manifests.remove(d),
            snaps: Map::new(st.snaps.dom().filter(|x: Snap| x.0 != d), |x: Snap| st.snaps[x]),
            index: ISet::new(|x: Snap| st.index.contains(x) && x.0 != d),
            reported: ISet::new(|x: Snap| st.reported.contains(x) && x.0 != d),
            ..st
        },
        Ev::Gc { grace } => Store {
            packs: ISet::new(
                |p: int|
                    st.packs.contains(p) && collector_verdict(named_by_a_manifest(st, p), named_by_a_snapshot(st, p), grace(p)) != Verdict::Reclaim,
            ),
            ..st
        },
    }
}

/// The invariant: every pack a disk's or a snapshot's manifest names is in the store, and a snapshot reported persisted is indexed and has
/// its manifest.
pub open spec fn inv(st: Store) -> bool {
    &&& forall|d: int, p: int| st.manifests.dom().contains(d) && #[trigger] st.manifests[d].contains(p) ==> st.packs.contains(p)
    &&& forall|x: Snap, p: int| st.snaps.dom().contains(x) && #[trigger] st.snaps[x].contains(p) ==> st.packs.contains(p)
    &&& forall|x: Snap| #[trigger] st.reported.contains(x) ==> st.snaps.dom().contains(x) && st.index.contains(x)
}

pub open spec fn empty_store() -> Store {
    Store { packs: ISet::empty(), manifests: Map::empty(), snaps: Map::empty(), index: ISet::empty(), reported: ISet::empty() }
}

pub proof fn lemma_the_empty_store_is_in_the_invariant()
    ensures
        inv(empty_store()),
{
}

pub proof fn lemma_every_event_keeps_the_invariant(st: Store, e: Ev)
    requires
        inv(st),
        enabled(st, e),
    ensures
        inv(step(st, e)),
{
    let w = step(st, e);
    match e {
        Ev::Upload { p } => {},
        Ev::Sync { d, names } => {
            assert forall|d2: int, p: int| w.manifests.dom().contains(d2) && #[trigger] w.manifests[d2].contains(p) implies w.packs.contains(p) by {
                if d2 != d {
                    assert(st.manifests.dom().contains(d2) && st.manifests[d2].contains(p));
                }
            }
        },
        Ev::Cut { d, s } => {
            assert forall|x: Snap, p: int| w.snaps.dom().contains(x) && #[trigger] w.snaps[x].contains(p) implies w.packs.contains(p) by {
                if x == (d, s) {
                    assert(st.manifests[d].contains(p));
                } else {
                    assert(st.snaps.dom().contains(x) && st.snaps[x].contains(p));
                }
            }
        },
        Ev::Index { d, s } => {},
        Ev::Report { d, s } => {},
        Ev::Fork { child, d, s } => {
            assert forall|d2: int, p: int| w.manifests.dom().contains(d2) && #[trigger] w.manifests[d2].contains(p) implies w.packs.contains(p) by {
                if d2 == child {
                    assert(st.snaps[(d, s)].contains(p));
                } else {
                    assert(st.manifests.dom().contains(d2) && st.manifests[d2].contains(p));
                }
            }
        },
        Ev::Purge { d } => {
            assert forall|d2: int, p: int| w.manifests.dom().contains(d2) && #[trigger] w.manifests[d2].contains(p) implies w.packs.contains(p) by {
                assert(st.manifests.dom().contains(d2) && st.manifests[d2].contains(p));
            }
            assert forall|x: Snap, p: int| w.snaps.dom().contains(x) && #[trigger] w.snaps[x].contains(p) implies w.packs.contains(p) by {
                assert(st.snaps.dom().contains(x) && st.snaps[x].contains(p));
            }
        },
        Ev::Gc { grace } => {
            assert forall|d: int, p: int| w.manifests.dom().contains(d) && #[trigger] w.manifests[d].contains(p) implies w.packs.contains(p) by {
                assert(st.manifests.dom().contains(d) && st.manifests[d].contains(p));
                assert(named_by_a_manifest(st, p));
            }
            assert forall|x: Snap, p: int| w.snaps.dom().contains(x) && #[trigger] w.snaps[x].contains(p) implies w.packs.contains(p) by {
                assert(st.snaps.dom().contains(x) && st.snaps[x].contains(p));
                assert(named_by_a_snapshot(st, p));
            }
        },
    }
}

/// A run: every event enabled where it happens.
pub open spec fn ok(st: Store, trace: Seq<Ev>) -> bool
    decreases trace.len(),
{
    if trace.len() == 0 {
        true
    } else {
        enabled(st, trace[0]) && ok(step(st, trace[0]), trace.drop_first())
    }
}

pub open spec fn run(st: Store, trace: Seq<Ev>) -> Store
    decreases trace.len(),
{
    if trace.len() == 0 {
        st
    } else {
        run(step(st, trace[0]), trace.drop_first())
    }
}

pub proof fn lemma_a_run_keeps_the_invariant(st: Store, trace: Seq<Ev>)
    requires
        inv(st),
        ok(st, trace),
    ensures
        inv(run(st, trace)),
    decreases trace.len(),
{
    if trace.len() > 0 {
        lemma_every_event_keeps_the_invariant(st, trace[0]);
        lemma_a_run_keeps_the_invariant(step(st, trace[0]), trace.drop_first());
    }
}

/// Whether the event deletes a snapshot of disk `d` (a purge of `d`).
pub open spec fn purges(e: Ev, d: int) -> bool {
    match e {
        Ev::Purge { d: d2 } => d2 == d,
        _ => false,
    }
}

/// No event of the trace purges disk `d`.
pub open spec fn never_purges(trace: Seq<Ev>, d: int) -> bool {
    forall|i: int| 0 <= i < trace.len() ==> !purges(#[trigger] trace[i], d)
}

/// A reported snapshot stays reported, until its export is purged.
pub proof fn lemma_a_run_keeps_a_report_until_a_purge(st: Store, trace: Seq<Ev>, x: Snap)
    requires
        ok(st, trace),
        st.reported.contains(x),
        never_purges(trace, x.0),
    ensures
        run(st, trace).reported.contains(x),
    decreases trace.len(),
{
    if trace.len() > 0 {
        let e = trace[0];
        assert(!purges(e, x.0));
        let w = step(st, e);
        assert(w.reported.contains(x));
        let rest = trace.drop_first();
        assert forall|i: int| 0 <= i < rest.len() implies !purges(#[trigger] rest[i], x.0) by {
            assert(rest[i] == trace[i + 1]);
        }
        lemma_a_run_keeps_a_report_until_a_purge(w, rest, x);
    }
}

/// P.snapshot.a-snapshot-taken-on-one-node-is-obtainable-on-a-second-node: from an empty store, in every run (whatever the cuts, uploads,
/// forks, purges of other exports and collector runs), a snapshot reported persisted and not purged is fetchable: its index entry, its manifest
/// and every pack the manifest names are in the store, which is all a fork on a second node reads.
pub proof fn thm_a_reported_snapshot_is_fetchable_in_every_run(trace: Seq<Ev>, x: Snap)
    requires
        ok(empty_store(), trace),
        run(empty_store(), trace).reported.contains(x),
    ensures
        fetchable(run(empty_store(), trace), x),
{
    lemma_the_empty_store_is_in_the_invariant();
    lemma_a_run_keeps_the_invariant(empty_store(), trace);
}

/// The same in any state of the invariant: a reported snapshot is fetchable.
pub proof fn thm_a_reported_snapshot_is_fetchable(st: Store, x: Snap)
    requires
        inv(st),
        st.reported.contains(x),
    ensures
        fetchable(st, x),
{
}

/// A fork's child is one `child` names: only a sync or a purge of the child, or another fork into its name, changes its manifest.
pub open spec fn touches(e: Ev, child: int) -> bool {
    match e {
        Ev::Sync { d, names } => d == child,
        Ev::Purge { d } => d == child,
        Ev::Fork { child: c2, d, s } => c2 == child,
        _ => false,
    }
}

pub open spec fn never_touches(trace: Seq<Ev>, child: int) -> bool {
    forall|i: int| 0 <= i < trace.len() ==> !touches(#[trigger] trace[i], child)
}

pub proof fn lemma_a_run_leaves_an_untouched_manifest(st: Store, trace: Seq<Ev>, child: int)
    requires
        ok(st, trace),
        never_touches(trace, child),
        st.manifests.dom().contains(child),
    ensures
        run(st, trace).manifests.dom().contains(child),
        run(st, trace).manifests[child] == st.manifests[child],
    decreases trace.len(),
{
    if trace.len() > 0 {
        let e = trace[0];
        assert(!touches(e, child));
        let w = step(st, e);
        let rest = trace.drop_first();
        assert forall|i: int| 0 <= i < rest.len() implies !touches(#[trigger] rest[i], child) by {
            assert(rest[i] == trace[i + 1]);
        }
        lemma_a_run_leaves_an_untouched_manifest(w, rest, child);
    }
}

/// P.snapshot.a-sandbox-created-from-a-filesystem-snapshot-has-the-files-the-snapshot-captured-after-the-source-is-gone: `child` is forked from
/// snapshot `(d, s)`, then the source `d` is purged (its manifest, snapshots, index entries and definition go), then any events that do not
/// touch the child run (collector runs among them, any number, any grace). The child still has the manifest the snapshot captured, and the
/// store holds every pack it names.
pub proof fn thm_a_clone_holds_the_snapshots_files_after_its_source_is_gone(st: Store, d: int, s: int, child: int, after: Seq<Ev>)
    requires
        inv(st),
        fetchable(st, (d, s)),
        !st.manifests.dom().contains(child),
        child != d,
        ok(step(step(st, Ev::Fork { child, d, s }), Ev::Purge { d }), after),
        never_touches(after, child),
    ensures
        ({
            let end = run(step(step(st, Ev::Fork { child, d, s }), Ev::Purge { d }), after);
            &&& end.manifests.dom().contains(child)
            &&& end.manifests[child] == st.snaps[(d, s)]
            &&& forall|p: int| #[trigger] st.snaps[(d, s)].contains(p) ==> end.packs.contains(p)
        }),
{
    let forked = step(st, Ev::Fork { child, d, s });
    lemma_every_event_keeps_the_invariant(st, Ev::Fork { child, d, s });
    let purged = step(forked, Ev::Purge { d });
    lemma_every_event_keeps_the_invariant(forked, Ev::Purge { d });
    assert(purged.manifests.dom().contains(child));
    assert(purged.manifests[child] == st.snaps[(d, s)]);
    lemma_a_run_keeps_the_invariant(purged, after);
    lemma_a_run_leaves_an_untouched_manifest(purged, after, child);
}

} // verus!
