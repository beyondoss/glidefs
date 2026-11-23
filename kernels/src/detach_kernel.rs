//! When GlideFS reports a volume's detach. A detach (`DELETE /api/exports/{name}`, `Router::remove_export`) tears the export down, and the
//! teardown drains the device: it flushes the cache's dirty blocks to the store pass by pass until a pass claims nothing and leaves nothing
//! dirty. The detach is reported (HTTP 200) only when nothing is left, so what the VMM's fsync pushed into the device is in the store when
//! the volume attaches to another VM; a drain that ends with blocks still dirty is an error (`DrainIncomplete`), never a report.
//!
//! The three decisions the router calls verbatim (`Router::teardown_export`, `Router::drain_export`, `Router::remove_export`):
//!
//! * [`drained`]: a drain pass is the last when it claimed no block and left none dirty.
//! * [`remaining_after_drain`]: the dirty blocks the detach still owes: none when a pass drained, else what is dirty now.
//! * [`detach_reported`]: the detach is reported only with none owed.
//!
//! [`thm_a_reported_detach_has_flushed_every_dirty_block`] states them together: a reported detach follows a pass that claimed and left
//! no dirty block, or a device with none dirty now. A purge (`?purge=true`) is a delete, not a detach: it throws the data away and skips the drain.
//!
//! Mutation (`scripts/verify-kernels.sh --features mut_detach_reports_before_flush` must go red): the detach is reported with the drain unfinished.

use vstd::prelude::*;

verus! {

/// The decision of a drain pass: `true` when it claimed no block and left none dirty.
pub open spec fn drained_spec(claimed: u64, dirty: u64) -> bool {
    claimed == 0 && dirty == 0
}

/// Whether a drain pass that claimed `claimed` blocks and left `dirty` dirty ends the drain.
pub fn drained(claimed: u64, dirty: u64) -> (done: bool)
    ensures
        done == drained_spec(claimed, dirty),
{
    claimed == 0 && dirty == 0
}

/// The dirty blocks still owed once the drain loop ended: none when a pass drained, else `dirty_now` (the count read after the loop).
pub open spec fn remaining_spec(done: bool, dirty_now: u64) -> u64 {
    if done { 0 } else { dirty_now }
}

pub fn remaining_after_drain(done: bool, dirty_now: u64) -> (remaining: u64)
    ensures
        remaining == remaining_spec(done, dirty_now),
{
    // `mut_detach_reports_before_flush`: the unfinished drain owes nothing.
    if done || cfg!(feature = "mut_detach_reports_before_flush") {
        0
    } else {
        dirty_now
    }
}

/// A detach is reported only with no dirty block owed.
pub fn detach_reported(remaining: u64) -> (reported: bool)
    ensures
        reported == (remaining == 0),
{
    remaining == 0
}

/// P.volume.a-glidefs-detach-flushes-the-device-to-the-store: when the detach is reported, the last drain pass claimed no block and left none
/// dirty, or no block is dirty now. A drain that ended with dirty blocks, whatever its pass counted, is not reported.
pub proof fn thm_a_reported_detach_has_flushed_every_dirty_block(claimed: u64, dirty_after_pass: u64, dirty_now: u64)
    ensures
        ({
            let done = drained_spec(claimed, dirty_after_pass);
            let remaining = remaining_spec(done, dirty_now);
            (remaining == 0) ==> ((claimed == 0 && dirty_after_pass == 0) || dirty_now == 0)
        }),
        ({
            let done = drained_spec(claimed, dirty_after_pass);
            (!done && dirty_now > 0) ==> remaining_spec(done, dirty_now) > 0
        }),
{
}

} // verus!
