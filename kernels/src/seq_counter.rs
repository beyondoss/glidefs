//! The write-sequence counter's transitions: pure over the counter's value, proven with Verus, called verbatim by `glidefs`'s
//! `SequenceNumber::next` and `advance_to`. A cut's sequence is a reading of the counter, so a later cut's is never below an earlier one's
//! (and so never below an owner marker's) while the process lives (`snapshot_owner::mono`).
//!
//! * [`next_value`]: `v + 1`, and `None` only at `u64::MAX` (the counter saturates, never wraps).
//! * [`advance_value`]: `Some(target)` only when it raises the counter; `None` otherwise.
//! * Safety `the_counter_never_decreases`: every transition leaves the value at least what it was.
//!
//! Not covered: a restart. The counter is re-seeded from the local state file and WAL (`write_cache/init.rs`: `persisted_max_seq.max(max_wal_seq)`),
//! so that a restart or a move keeps it above every cut's sequence is a property of what those hold, not of these transitions.
//!
//! Mutation (`scripts/verify-kernels.sh --features mut_counter_wraps` must go red, and glidefs's `sequence_number_never_wraps` fail):
//! `next_value` wraps at the top.

use vstd::prelude::*;

verus! {

/// The value after a transition that returned `r` from `v`.
pub open spec fn after(v: u64, r: Option<u64>) -> u64 {
    match r {
        Some(n) => n,
        None => v,
    }
}

#[cfg(not(feature = "mut_counter_wraps"))]
pub fn next_value(v: u64) -> (r: Option<u64>)
    ensures
        v < u64::MAX ==> r == Some((v + 1) as u64),
        v == u64::MAX ==> r is None,
{
    if v == u64::MAX { None } else { Some(v + 1) }
}

/// Mutation: wraps to zero at the top.
#[cfg(feature = "mut_counter_wraps")]
pub fn next_value(v: u64) -> (r: Option<u64>)
    ensures
        v < u64::MAX ==> r == Some((v + 1) as u64),
        v == u64::MAX ==> r is None,
{
    if v == u64::MAX { Some(0) } else { Some(v + 1) }
}

pub fn advance_value(v: u64, target: u64) -> (r: Option<u64>)
    ensures
        v < target ==> r == Some(target),
        v >= target ==> r is None,
{
    if v < target { Some(target) } else { None }
}

/// `the_counter_never_decreases`: whichever transition ran, the value after is at least the value before.
pub proof fn the_counter_never_decreases(v: u64, target: u64, rn: Option<u64>, ra: Option<u64>)
    requires
        v < u64::MAX ==> rn == Some((v + 1) as u64),
        v == u64::MAX ==> rn is None,
        v < target ==> ra == Some(target),
        v >= target ==> ra is None,
    ensures
        after(v, rn) >= v,
        after(v, ra) >= v,
{
}

} // verus!

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_counter_saturates_and_only_rises() {
        assert_eq!(next_value(4), Some(5));
        assert_eq!(next_value(u64::MAX), None);
        assert_eq!(advance_value(4, 9), Some(9));
        assert_eq!(advance_value(9, 4), None);
        assert_eq!(advance_value(9, 9), None);
    }
}
