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

/// The router's export map for the volume being attached.
pub struct Export {
    /// An entry for the volume is in the map.
    pub in_map: bool,
    /// Its block device is registered.
    pub has_device: bool,
}

/// The state the attach ends in when its client goes away with `registered` and `saved` done.
pub open spec fn after_interruption(needs_device: bool, registered: bool, saved: bool) -> Export {
    match fate_spec(needs_device, registered, saved) {
        Fate::Keep => Export { in_map: true, has_device: registered },
        Fate::Unwind => Export { in_map: false, has_device: false },
    }
}

/// P.volume.an-interrupted-attach-leaves-no-export-without-a-device: a volume attach whose client goes away, at any step, leaves no export
/// without a block device: an export that needs its device and remains in the map has it.
pub proof fn thm_an_interrupted_attach_leaves_no_export_without_a_device(needs_device: bool, registered: bool, saved: bool)
    ensures
        ({
            let e = after_interruption(needs_device, registered, saved);
            e.in_map && needs_device ==> e.has_device
        }),
{
}

} // verus!
