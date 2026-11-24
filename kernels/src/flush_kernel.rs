//! A block's life between the guest's write and the store, as pure functions proven with Verus, called verbatim by `glidefs`'s
//! `SparseStateMap` (`transition_to_dirty`, `transition_dirty_to_syncing`, `transition_syncing_to_not_present`) and by the post-takeover
//! recovery in `WriteCache`. The rule the flush path keeps ("Upload -> ManifestSync -> Evict", `write_cache/flush.rs`): a block leaves
//! SYNCING for NOT_PRESENT only when the manifest in the store names the content the flush claimed, so every read after the eviction (which
//! reads the store) returns what the guest last wrote.
//!
//! * [`step`]: the phase of one block after an event. [`Event::Evict`] carries `covered`: the flush has put the manifest that names the packs.
//! * The model below ([`Vol`], [`vstep`]) composes the steps over every block of a volume, with the guest's writes, the flush's manifest
//!   sync, a failed flush and a drop of a clean block. [`inv`] is its inductive invariant and [`vstep`] calls [`step_spec`] for every phase.
//!
//! Theorems (the rows' whole text, no bound on the blocks):
//! * [`thm_a_volume_roundtrips_through_attach_and_detach`]: a drained volume (no dirty or syncing block: the detach's `drained_spec`) read after
//!   a reattach returns every block's last write, whatever the node's cache held before.
//! * [`thm_a_stop_and_start_keeps_the_volume`]: the same, and the reattached volume is again in the invariant, so every later stop and start
//!   keeps it too.
//! * [`thm_a_sleep_and_wake_reads_what_the_guest_wrote`]: in every reachable state (light sleep: the volume stays attached) a read returns the
//!   last write; after a deep sleep the wake on any node reads the same.
//!
//! Mutations (`scripts/verify-kernels.sh --features mut_x` must go red): `mut_evict_uncovered` (a SYNCING block is evicted whether or not the
//! manifest names it), `mut_write_keeps_syncing` (a write to a SYNCING block leaves it SYNCING, so the eviction drops the newer content),
//! `mut_stranded_marked_absent` (a claimed block whose flush and SSD copy both failed is marked NOT_PRESENT: the guest's write is dropped).

use vstd::prelude::*;

verus! {

/// A block's state in the write cache (`SparseBlockState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Never written to the local SSD, or evicted: reads go to the store.
    NotPresent,
    /// Present and equal to the store.
    Clean,
    /// Written by the guest and not yet claimed by a flush.
    Dirty,
    /// Claimed by a flush whose upload is in progress.
    Syncing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// The guest wrote the block.
    Write,
    /// A flush claims a dirty block.
    Claim,
    /// The flush evicts the block after its upload; `covered` is whether the manifest naming the uploaded packs is in the store.
    Evict { covered: bool },
    /// A failed flush gives its claimed block back.
    Requeue,
    /// A clean block's local copy is dropped.
    Drop,
    /// A failed flush could not copy a claimed block back to the SSD: its data is only in the flushing file, so it stays claimed.
    Stranded,
}

pub open spec fn step_spec(p: Phase, e: Event) -> Phase {
    match e {
        Event::Write => Phase::Dirty,
        Event::Claim => if p == Phase::Dirty { Phase::Syncing } else { p },
        Event::Evict { covered } => if p == Phase::Syncing && covered { Phase::NotPresent } else { p },
        Event::Requeue => if p == Phase::Syncing { Phase::Dirty } else { p },
        Event::Drop => if p == Phase::Clean { Phase::NotPresent } else { p },
        Event::Stranded => p,
    }
}

/// The phase of a block after `e`. The only way out of SYNCING toward the store is an `Evict` whose manifest covers the block.
#[cfg(not(any(feature = "mut_evict_uncovered", feature = "mut_write_keeps_syncing", feature = "mut_stranded_marked_absent")))]
pub fn step(p: Phase, e: Event) -> (r: Phase)
    ensures
        r == step_spec(p, e),
{
    match (e, p) {
        (Event::Write, _) => Phase::Dirty,
        (Event::Claim, Phase::Dirty) => Phase::Syncing,
        (Event::Evict { covered: true }, Phase::Syncing) => Phase::NotPresent,
        (Event::Requeue, Phase::Syncing) => Phase::Dirty,
        (Event::Drop, Phase::Clean) => Phase::NotPresent,
        _ => p,
    }
}

/// Mutations: `mut_evict_uncovered` evicts a SYNCING block whether or not the manifest names it; `mut_write_keeps_syncing` leaves a SYNCING
/// block SYNCING when the guest writes it (the eviction then drops the newer content).
#[cfg(any(feature = "mut_evict_uncovered", feature = "mut_write_keeps_syncing", feature = "mut_stranded_marked_absent"))]
pub fn step(p: Phase, e: Event) -> (r: Phase)
    ensures
        r == step_spec(p, e),
{
    match (e, p) {
        (Event::Stranded, Phase::Syncing) if cfg!(feature = "mut_stranded_marked_absent") => Phase::NotPresent,
        (Event::Write, Phase::Syncing) if cfg!(feature = "mut_write_keeps_syncing") => Phase::Syncing,
        (Event::Write, _) => Phase::Dirty,
        (Event::Claim, Phase::Dirty) => Phase::Syncing,
        (Event::Evict { covered: _ }, Phase::Syncing) if cfg!(feature = "mut_evict_uncovered") => Phase::NotPresent,
        (Event::Evict { covered: true }, Phase::Syncing) => Phase::NotPresent,
        (Event::Requeue, Phase::Syncing) => Phase::Dirty,
        (Event::Drop, Phase::Clean) => Phase::NotPresent,
        _ => p,
    }
}

// ---- The model the steps are proven against: every block of one volume ----

/// One volume. Blocks are indexed by any integer: a block never written is `NotPresent` with the volume's initial content in the store.
pub struct Vol {
    pub phase: spec_fn(int) -> Phase,
    /// What the guest last wrote (acknowledged) to the block.
    pub latest: spec_fn(int) -> int,
    /// The block's bytes on the local SSD; meaningful unless `NotPresent`.
    pub local: spec_fn(int) -> int,
    /// The block's content the store's manifest names.
    pub store: spec_fn(int) -> int,
    /// The content a flush claimed (read from the local SSD at the claim).
    pub claimed: spec_fn(int) -> int,
}

pub enum VEvent {
    /// The guest's write of `v` to block `b` is acknowledged.
    Write { b: int, v: int },
    Claim { b: int },
    /// The flush put the manifest that names the packs holding the claimed content of `b`.
    Sync { b: int },
    Evict { b: int, covered: bool },
    Requeue { b: int },
    /// A read of a block not on the SSD fills it from the store.
    Backfill { b: int },
    Drop { b: int },
    /// A failed flush could not copy the block back: it stays SYNCING, its data in the flushing file.
    Strand { b: int },
}

/// What a read of block `b` returns: the SSD copy if the block is present, else the store's.
pub open spec fn read(v: Vol, b: int) -> int {
    if (v.phase)(b) == Phase::NotPresent { (v.store)(b) } else { (v.local)(b) }
}

/// Whether the event can happen: a manifest sync names only blocks still dirty or syncing; an eviction claims `covered` only when the store
/// holds the claimed content.
pub open spec fn enabled(v: Vol, e: VEvent) -> bool {
    match e {
        VEvent::Sync { b } => (v.phase)(b) == Phase::Dirty || (v.phase)(b) == Phase::Syncing,
        VEvent::Evict { b, covered } => covered ==> (v.store)(b) == (v.claimed)(b),
        VEvent::Backfill { b } => (v.phase)(b) == Phase::NotPresent,
        _ => true,
    }
}

pub open spec fn with_phase(v: Vol, b: int, p: Phase) -> Vol {
    Vol { phase: |i: int| if i == b { p } else { (v.phase)(i) }, ..v }
}

pub open spec fn vstep(v: Vol, e: VEvent) -> Vol {
    match e {
        VEvent::Write { b, v: x } => Vol {
            phase: |i: int| if i == b { step_spec((v.phase)(i), Event::Write) } else { (v.phase)(i) },
            latest: |i: int| if i == b { x } else { (v.latest)(i) },
            local: |i: int| if i == b { x } else { (v.local)(i) },
            ..v
        },
        VEvent::Claim { b } => Vol {
            phase: |i: int| if i == b { step_spec((v.phase)(i), Event::Claim) } else { (v.phase)(i) },
            claimed: |i: int| if i == b && (v.phase)(i) == Phase::Dirty { (v.local)(i) } else { (v.claimed)(i) },
            ..v
        },
        VEvent::Sync { b } => Vol {
            store: |i: int| if i == b { (v.claimed)(i) } else { (v.store)(i) },
            ..v
        },
        VEvent::Evict { b, covered } => with_phase(v, b, step_spec((v.phase)(b), Event::Evict { covered })),
        VEvent::Requeue { b } => with_phase(v, b, step_spec((v.phase)(b), Event::Requeue)),
        VEvent::Backfill { b } => Vol {
            phase: |i: int| if i == b { Phase::Clean } else { (v.phase)(i) },
            local: |i: int| if i == b { (v.store)(i) } else { (v.local)(i) },
            ..v
        },
        VEvent::Drop { b } => with_phase(v, b, step_spec((v.phase)(b), Event::Drop)),
        VEvent::Strand { b } => with_phase(v, b, step_spec((v.phase)(b), Event::Stranded)),
    }
}

/// The invariant: a block that reads from the store has the store at its last write; a clean or syncing block's store or claim is at it;
/// a present block's SSD copy is its last write.
pub open spec fn inv(v: Vol) -> bool {
    forall|b: int| {
        &&& #[trigger] (v.phase)(b) == Phase::NotPresent ==> (v.store)(b) == (v.latest)(b)
        &&& (v.phase)(b) == Phase::Clean ==> (v.store)(b) == (v.latest)(b)
        &&& (v.phase)(b) == Phase::Syncing ==> (v.claimed)(b) == (v.latest)(b)
        &&& (v.phase)(b) != Phase::NotPresent ==> (v.local)(b) == (v.latest)(b)
    }
}

/// A volume nothing has been written to: every block reads the initial content from the store.
pub open spec fn fresh(v: Vol, init: spec_fn(int) -> int) -> bool {
    &&& forall|b: int| #[trigger] (v.phase)(b) == Phase::NotPresent
    &&& forall|b: int| #[trigger] (v.store)(b) == init(b) && (v.latest)(b) == init(b)
}

pub proof fn lemma_a_fresh_volume_is_in_the_invariant(v: Vol, init: spec_fn(int) -> int)
    requires
        fresh(v, init),
    ensures
        inv(v),
{
}

pub proof fn lemma_every_step_keeps_the_invariant(v: Vol, e: VEvent)
    requires
        inv(v),
        enabled(v, e),
    ensures
        inv(vstep(v, e)),
{
    let w = vstep(v, e);
    assert forall|b: int| {
        &&& #[trigger] (w.phase)(b) == Phase::NotPresent ==> (w.store)(b) == (w.latest)(b)
        &&& (w.phase)(b) == Phase::Clean ==> (w.store)(b) == (w.latest)(b)
        &&& (w.phase)(b) == Phase::Syncing ==> (w.claimed)(b) == (w.latest)(b)
        &&& (w.phase)(b) != Phase::NotPresent ==> (w.local)(b) == (w.latest)(b)
    } by {
        assert((v.phase)(b) == Phase::NotPresent ==> (v.store)(b) == (v.latest)(b));
    }
}

/// A volume is drained when no block is dirty or syncing (`detach_kernel::drained_spec`: no dirty, no syncing, nothing claimed).
pub open spec fn drained(v: Vol) -> bool {
    forall|b: int| #[trigger] (v.phase)(b) != Phase::Dirty && (v.phase)(b) != Phase::Syncing
}

/// A (re)attach, on this node or any other: the SSD cache holds nothing of the volume, every read goes to the store.
pub open spec fn reattach(v: Vol) -> Vol {
    Vol { phase: |i: int| Phase::NotPresent, ..v }
}

pub proof fn lemma_a_drained_volume_reads_the_store_at_its_last_write(v: Vol)
    requires
        inv(v),
        drained(v),
    ensures
        inv(reattach(v)),
        forall|b: int| read(reattach(v), b) == (v.latest)(b),
{
    let w = reattach(v);
    assert forall|b: int| #[trigger] (v.store)(b) == (v.latest)(b) by {
        assert((v.phase)(b) != Phase::Dirty && (v.phase)(b) != Phase::Syncing);
    }
}

/// P.volume.a-volume-roundtrips-through-attach-and-detach: data written to a volume attached to a VM is there after detach and reattach.
pub proof fn thm_a_volume_roundtrips_through_attach_and_detach(v: Vol, b: int, written: int)
    requires
        inv(v),
        (v.latest)(b) == written,
        drained(v),
    ensures
        read(reattach(v), b) == written,
{
    lemma_a_drained_volume_reads_the_store_at_its_last_write(v);
}

/// P.volume.stop-start-keeps-the-volume: a stop (the detach) and a start (the attach) keep the volume's sentinel, and the volume after the
/// start is in the invariant again, so the next stop and start keep it too.
pub proof fn thm_a_stop_and_start_keeps_the_volume(v: Vol, sentinel: int, written: int)
    requires
        inv(v),
        (v.latest)(sentinel) == written,
        drained(v),
    ensures
        read(reattach(v), sentinel) == written,
        inv(reattach(v)),
        (reattach(v).latest)(sentinel) == written,
{
    lemma_a_drained_volume_reads_the_store_at_its_last_write(v);
}

/// P.create.glidefs-keeps-what-the-guest-wrote-through-a-sleep-and-wake: every block the guest wrote before a light sleep (the volume stays
/// attached: any reachable state) or a deep sleep (the volume is detached, drained, and woken on any node) is read back unchanged.
pub proof fn thm_a_sleep_and_wake_reads_what_the_guest_wrote(v: Vol)
    requires
        inv(v),
    ensures
        forall|b: int| read(v, b) == (v.latest)(b),
        drained(v) ==> forall|b: int| read(reattach(v), b) == (v.latest)(b),
{
    assert forall|b: int| read(v, b) == (v.latest)(b) by {
        assert((v.phase)(b) == Phase::NotPresent ==> (v.store)(b) == (v.latest)(b));
    }
    if drained(v) {
        lemma_a_drained_volume_reads_the_store_at_its_last_write(v);
    }
}

} // verus!
