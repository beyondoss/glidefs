//! When GlideFS reports a volume's detach. A detach (`DELETE /api/exports/{name}`, `Router::remove_export`) tears the export down, and the
//! teardown drains the device: it flushes the cache to the store pass by pass until a pass SUCCEEDS (its manifest is published) and
//! claims nothing and leaves nothing dirty or syncing. The detach is reported (HTTP 200) only when the drain ended `Done` and the final
//! manifest sync held; a drain that ran out of passes, or whose manifest could not be published, is an error (`DrainIncomplete`, retried),
//! never a report. (Found by `test_a_detach_is_not_reported_while_the_manifest_cannot_be_published`: after a failed manifest PUT the retry pass
//! deduplicated the packs, evicted the blocks and left nothing dirty, and the old rule, "no dirty block remains", reported a detach whose
//! manifest was never in the store.)
//!
//! The decisions the router calls verbatim (`Router::teardown_export`, `Router::drain_export`, `Router::remove_export`):
//!
//! * [`drained`]: a successful drain pass is the last when it claimed no block and left none dirty or syncing.
//! * [`remaining_after_drain`]: what the detach still owes once the loop ended: nothing only after a `Done` drain with the final manifest sync
//!   held, or when a newer fence token owns the volume and nothing is dirty (the superseded writer's blocks were never committed).
//! * [`leaves_the_map`]: the export leaves the router's map only when nothing is owed, so a failed detach is retried against the same export.
//! * [`detach_reported`]: the detach is reported only with nothing owed.
//!
//! [`thm_a_reported_detach_has_flushed_every_dirty_block`] states them together. A purge (`?purge=true`) is a delete, not a detach: it throws
//! the data away and skips the drain.
//!
//! Mutations (`scripts/verify-kernels.sh --features <m>` must go red): `mut_detach_reports_before_flush` (an unfinished drain owes nothing),
//! `mut_drained_ignores_syncing` (a pass with blocks still syncing ends the drain), `mut_detach_ignores_the_final_sync` (a failed final manifest
//! sync owes nothing), `mut_export_leaves_before_the_final_sync` (the export leaves the map before the final sync held).

use vstd::prelude::*;

verus! {

/// How the drain loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainEnd {
    /// A successful pass claimed nothing and left nothing dirty or syncing.
    Done,
    /// A newer fence token owns the volume: this writer's dirty blocks were never committed and must not be written.
    Fenced,
    /// The loop ran out of passes (or every pass failed) without a clean one.
    Exhausted,
}

/// The decision of a successful drain pass: `true` when it claimed no block and left none dirty or syncing.
pub open spec fn drained_spec(claimed: u64, dirty: u64, syncing: u64) -> bool {
    claimed == 0 && dirty == 0 && syncing == 0
}

/// Whether a successful drain pass that claimed `claimed` blocks and left `dirty` dirty and `syncing` syncing ends the drain.
pub fn drained(claimed: u64, dirty: u64, syncing: u64) -> (done: bool)
    ensures
        done == drained_spec(claimed, dirty, syncing),
{
    // `mut_drained_ignores_syncing`: blocks claimed by a flush and not yet evicted do not hold the drain.
    if cfg!(feature = "mut_drained_ignores_syncing") {
        return claimed == 0 && dirty == 0;
    }
    claimed == 0 && dirty == 0 && syncing == 0
}

/// What the detach still owes once the loop ended. `final_sync_ok` is whether the final manifest sync held (read only for a non-fenced end).
pub open spec fn remaining_spec(end: DrainEnd, dirty_now: u64, syncing_now: u64, final_sync_ok: bool) -> u64 {
    match end {
        DrainEnd::Fenced => dirty_now,
        DrainEnd::Done => if final_sync_ok { 0 } else { 1 },
        DrainEnd::Exhausted => if dirty_now > 0 { dirty_now } else if syncing_now > 0 { syncing_now } else { 1 },
    }
}

pub fn remaining_after_drain(end: DrainEnd, dirty_now: u64, syncing_now: u64, final_sync_ok: bool) -> (remaining: u64)
    ensures
        remaining == remaining_spec(end, dirty_now, syncing_now, final_sync_ok),
{
    match end {
        DrainEnd::Fenced => dirty_now,
        DrainEnd::Done => {
            // `mut_detach_ignores_the_final_sync`: a failed final manifest sync owes nothing.
            if final_sync_ok || cfg!(feature = "mut_detach_ignores_the_final_sync") { 0 } else { 1 }
        },
        DrainEnd::Exhausted => {
            // `mut_detach_reports_before_flush`: the unfinished drain owes nothing.
            if cfg!(feature = "mut_detach_reports_before_flush") {
                0
            } else if dirty_now > 0 {
                dirty_now
            } else if syncing_now > 0 {
                syncing_now
            } else {
                1
            }
        },
    }
}

/// Whether the export may leave the router's map. It leaves only when nothing is owed, final manifest sync included: before that, a retry of
/// the detach (or a restart, which re-opens the export from its device map and WAL) finds it and resumes the owed flush and sync.
pub fn leaves_the_map(end: DrainEnd, dirty_now: u64, syncing_now: u64, final_sync_ok: bool) -> (leaves: bool)
    ensures
        leaves == (remaining_spec(end, dirty_now, syncing_now, final_sync_ok) == 0),
{
    // `mut_export_leaves_before_the_final_sync`: a clean drain frees the export without waiting for the final manifest sync.
    if cfg!(feature = "mut_export_leaves_before_the_final_sync") && end == DrainEnd::Done {
        return true;
    }
    remaining_after_drain(end, dirty_now, syncing_now, final_sync_ok) == 0
}

/// A detach is reported only with nothing owed.
pub fn detach_reported(remaining: u64) -> (reported: bool)
    ensures
        reported == (remaining == 0),
{
    remaining == 0
}

/// P.volume.a-glidefs-detach-flushes-the-device-to-the-store: a reported detach is one of two things. Either the drain ended `Done`, that is a
/// successful pass left no block dirty, syncing or claimed (so every block the VMM's fsync pushed into the device is in the store with its
/// manifest), and the final manifest sync held; or a newer token owns the volume and nothing here is dirty. A drain that ran out of passes is
/// never reported, whatever the dirty count reads afterwards.
pub proof fn thm_a_reported_detach_has_flushed_every_dirty_block(
    end: DrainEnd,
    claimed: u64,
    dirty_after_pass: u64,
    syncing_after_pass: u64,
    dirty_now: u64,
    syncing_now: u64,
    final_sync_ok: bool,
)
    ensures
        remaining_spec(end, dirty_now, syncing_now, final_sync_ok) == 0 ==> (end == DrainEnd::Done && final_sync_ok) || (end == DrainEnd::Fenced
            && dirty_now == 0),
        end == DrainEnd::Exhausted ==> remaining_spec(end, dirty_now, syncing_now, final_sync_ok) > 0,
        drained_spec(claimed, dirty_after_pass, syncing_after_pass) ==> (claimed == 0 && dirty_after_pass == 0 && syncing_after_pass == 0),
{
}

} // verus!
