//! The write-sequence counter's transitions: pure over the counter's value, proven with Verus, called verbatim by `glidefs`'s
//! `SequenceNumber::next` and `advance_to`. A cut's sequence is a reading of the counter, so a later cut's is never below an earlier one's
//! (and so never below an owner marker's) while the process lives (`snapshot_owner::mono`).
//!
//! * [`next_value`]: `v + 1`, and `None` only at `u64::MAX` (the counter saturates, never wraps).
//! * [`advance_value`]: `Some(target)` only when it raises the counter; `None` otherwise.
//! * Safety `the_counter_never_decreases`: every transition leaves the value at least what it was.
//!
//! [`seeded_value`]: a restart or a move seeds the counter from the local state file and WAL (`write_cache/init.rs`) and then from the store's
//! cuts (the router, after open): at least every sequence the export has cut.
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

/// The value a restarted or relocated export's counter holds once seeded: the local value (state file and WAL) raised to every sequence the
/// store shows the export cut (every snapshot manifest and every owner marker). `WriteCache` open seeds from the local files only, so a node
/// with none starts at 0; the router calls this after opening and `advance_to`s the result.
/// The row's whole text: the counter after the seed is at least every sequence the export has cut, and never below its local value.
#[cfg(not(feature = "mut_seed_ignores_store"))]
pub fn seeded_value(local: u64, cuts: &[u64]) -> (r: u64)
    ensures
        r >= local,
        forall|k: int| 0 <= k < cuts.len() ==> r >= cuts@[k],
        r == local || exists|k: int| 0 <= k < cuts.len() && r == cuts@[k],
{
    let mut r = local;
    let mut i = 0;
    while i < cuts.len()
        invariant
            0 <= i <= cuts.len(),
            r >= local,
            forall|k: int| 0 <= k < i ==> r >= cuts@[k],
            r == local || exists|k: int| 0 <= k < i && r == cuts@[k],
        decreases cuts.len() - i,
    {
        if cuts[i] > r {
            r = cuts[i];
        }
        i += 1;
    }
    r
}

/// Mutation: the store's sequences are ignored (a node with no local state starts at 0).
#[cfg(feature = "mut_seed_ignores_store")]
pub fn seeded_value(local: u64, cuts: &[u64]) -> (r: u64)
    ensures
        r >= local,
        forall|k: int| 0 <= k < cuts.len() ==> r >= cuts@[k],
        r == local || exists|k: int| 0 <= k < cuts.len() && r == cuts@[k],
{
    local
}

/// `advance_to(seeded_value(..))` leaves the counter at exactly the seed: from `local <= r`, `advance_value` returns `Some(r)` or, at equality, `None`.
pub proof fn advancing_to_the_seed_lands_on_it(local: u64, r: u64, ra: Option<u64>)
    requires
        local <= r,
        local < r ==> ra == Some(r),
        local >= r ==> ra is None,
    ensures
        after(local, ra) == r,
{
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
