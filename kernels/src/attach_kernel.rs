//! What becomes of an attach that does not reach its end, as one pure function proven with Verus, called verbatim by `glidefs`'s
//! `create_or_attach_volume` (`block/api.rs`): after `create_export` put an export in the map, the attach persists its definition
//! (`save_export`) and registers its block device (`register_device`). The attach's client can go away at any await point in between (the
//! request is cancelled, the process is killed and the guard runs, the connection drops); a failed step ends it the same way. Whatever the
//! end, [`attach_fate`] says whether the export stays or is unwound (`cleanup_failed_create`).
//!
//! * [`attach_fate`]: `Keep` only when the definition is saved and, for a transport that exists only as its device (ublk), the device is
//!   registered; anything short of that is `Unwind`.
//! * [`thm_an_interrupted_attach_leaves_no_export_without_a_device`]: whatever step the client went away at, the export that remains has its
//!   block device, or the attach was unwound and no export remains.
//!
//! Mutation (`scripts/verify-kernels.sh --features mut_attach_keeps_deviceless` must go red): an export whose definition is saved is kept
//! whether or not its device is registered.

use vstd::prelude::*;

verus! {

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fate {
    /// The export stays: the attach is whole.
    Keep,
    /// The export is torn down (`cleanup_failed_create`): a retry creates it afresh.
    Unwind,
}

/// The attach's progress when it ended: what had been done to the export that `create_export` put in the map.
pub open spec fn fate_spec(needs_device: bool, registered: bool, saved: bool) -> Fate {
    if saved && (!needs_device || registered) { Fate::Keep } else { Fate::Unwind }
}

#[cfg(not(feature = "mut_attach_keeps_deviceless"))]
pub fn attach_fate(needs_device: bool, registered: bool, saved: bool) -> (f: Fate)
    ensures
        f == fate_spec(needs_device, registered, saved),
{
    if saved && (!needs_device || registered) { Fate::Keep } else { Fate::Unwind }
}

/// Mutation: the device is not asked for.
#[cfg(feature = "mut_attach_keeps_deviceless")]
pub fn attach_fate(needs_device: bool, registered: bool, saved: bool) -> (f: Fate)
    ensures
        f == fate_spec(needs_device, registered, saved),
{
    if saved { Fate::Keep } else { Fate::Unwind }
}

// ---- The model the kernel is proven against: the handler's real event sequence ----

/// The attach of one volume, from `create_export` on. `over` is set when the handler ended (`Finish`) or the client went away
/// (`ClientGone`); after it no step of the dropped future runs. `pending` is the unwind `AttachGuard::drop` spawned and has not run yet.
pub struct Attach {
    /// An entry for the volume is in the router's map.
    pub in_map: bool,
    /// Its block device is registered.
    pub registered: bool,
    /// Its definition (`export.json`) is saved.
    pub saved: bool,
    pub over: bool,
    pub pending: bool,
}

pub enum AEvent {
    /// `create_export` put the export in the map.
    Create,
    /// `save_export` completed (an await point of the `tokio::join!`).
    Save,
    /// `register_device` completed (the other arm of the join; either order).
    Register,
    /// The handler reached its end: it asks the kernel and, on `Unwind`, tears the export down before it answers.
    Finish,
    /// The client went away at this await point (the future is dropped): the guard asks the kernel and, on `Unwind`, spawns the teardown.
    ClientGone,
    /// The spawned teardown ran (`cleanup_failed_create`).
    Cleanup,
}

pub open spec fn enabled(a: Attach, e: AEvent) -> bool {
    match e {
        AEvent::Create => !a.in_map && !a.over,
        AEvent::Save | AEvent::Register | AEvent::Finish | AEvent::ClientGone => a.in_map && !a.over,
        AEvent::Cleanup => a.pending,
    }
}

pub open spec fn astep(needs_device: bool, a: Attach, e: AEvent) -> Attach {
    let fate = fate_spec(needs_device, a.registered, a.saved);
    match e {
        AEvent::Create => Attach { in_map: true, ..a },
        AEvent::Save => Attach { saved: true, ..a },
        AEvent::Register => Attach { registered: true, ..a },
        AEvent::Finish => match fate {
            Fate::Keep => Attach { over: true, ..a },
            Fate::Unwind => Attach { in_map: false, registered: false, over: true, ..a },
        },
        AEvent::ClientGone => match fate {
            Fate::Keep => Attach { over: true, ..a },
            Fate::Unwind => Attach { over: true, pending: true, ..a },
        },
        AEvent::Cleanup => Attach { in_map: false, registered: false, pending: false, ..a },
    }
}

pub open spec fn start() -> Attach {
    Attach { in_map: false, registered: false, saved: false, over: false, pending: false }
}

/// The invariant over every reachable state: a device is registered only on an export in the map; an unwind in flight belongs to an ended
/// attach; and once an attach ended with no unwind pending, an export that remains is saved and, if it needs its device, has it.
pub open spec fn inv(needs_device: bool, a: Attach) -> bool {
    &&& a.registered ==> a.in_map
    &&& a.pending ==> a.over
    &&& (a.over && !a.pending && a.in_map) ==> (a.saved && (!needs_device || a.registered))
}

pub proof fn lemma_the_start_is_in_the_invariant(needs_device: bool)
    ensures
        inv(needs_device, start()),
{
}

pub proof fn lemma_every_event_keeps_the_invariant(needs_device: bool, a: Attach, e: AEvent)
    requires
        inv(needs_device, a),
        enabled(a, e),
    ensures
        inv(needs_device, astep(needs_device, a, e)),
{
}

/// P.volume.an-interrupted-attach-leaves-no-export-without-a-device: a volume attach whose client goes away leaves no export without a block
/// device. Over every reachable state (the invariant holds from the start through every interleaving of `Save`, `Register`, `ClientGone` at
/// any await point, `Finish` and the spawned `Cleanup`): once the attach has ended and its unwind (if any) has run, an export that needs its
/// device and remains in the map has it registered. And an unwind in flight can always run (`Cleanup` is enabled), after which none remains.
pub proof fn thm_an_interrupted_attach_leaves_no_export_without_a_device(needs_device: bool, a: Attach)
    requires
        inv(needs_device, a),
        a.over,
        !a.pending,
    ensures
        a.in_map && needs_device ==> a.registered,
{
}

pub proof fn thm_a_pending_unwind_always_runs(needs_device: bool, a: Attach)
    requires
        inv(needs_device, a),
        a.pending,
    ensures
        enabled(a, AEvent::Cleanup),
        !astep(needs_device, a, AEvent::Cleanup).in_map,
        !astep(needs_device, a, AEvent::Cleanup).pending,
{
}

} // verus!
