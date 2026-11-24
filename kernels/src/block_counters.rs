//! The dirty and syncing block counters a recovered write cache starts with, as a pure function of the blocks' final phases, proven with
//! Verus and called verbatim by `WriteCache::open` (`write_cache/init.rs`). `P.volume.glidefs-block-counters-equal-the-block-phases`: an
//! export's counters are zero exactly when no block is DIRTY or SYNCING, and (stronger, so the counters can be maintained by deltas from
//! there) each equals the number of blocks in its phase.
//!
//! * [`recount`]: after recovery has settled every block (metadata loaded, WAL replayed, SYNCING resolved, CLEAN demoted), the counters are
//!   the number of DIRTY and of SYNCING blocks in the final state map. They are never carried through the recovery steps as deltas: a delta
//!   that is applied to a block the load never counted (a CLEAN block demoted, a WAL entry over a SYNCING block) leaves the counter
//!   below or above the phases, and a counter at 0 over DIRTY blocks makes the flush scheduler skip them and a detach report drained.
//!
//! Mutation (`scripts/verify-kernels.sh --features mut_count_by_deltas` must go red, and glidefs's
//! `a_restart_counts_the_dirty_blocks_the_metadata_holds` fail): the counters are the load's count (DIRTY and SYNCING blocks), one
//! subtracted for every CLEAN block.

use vstd::prelude::*;

verus! {

/// `SparseBlockState::CLEAN` (`block_map.rs`; a glidefs test pins the four values).
pub const CLEAN: u8 = 1;
/// `SparseBlockState::DIRTY`.
pub const DIRTY: u8 = 2;
/// `SparseBlockState::SYNCING`.
pub const SYNCING: u8 = 3;

/// The number of blocks in `phase`.
pub open spec fn count_of(s: Seq<u8>, phase: u8) -> nat
    decreases s.len(),
{
    if s.len() == 0 {
        0
    } else {
        count_of(s.drop_last(), phase) + (if s.last() == phase { 1nat } else { 0nat })
    }
}

pub struct Counts {
    pub dirty: u64,
    pub syncing: u64,
}

/// A counter is zero exactly when no block is in the phase.
proof fn lemma_count_zero(s: Seq<u8>, phase: u8)
    ensures
        count_of(s, phase) == 0 <==> forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i] != phase,
    decreases s.len(),
{
    if s.len() > 0 {
        lemma_count_zero(s.drop_last(), phase);
        let d = s.drop_last();
        assert(forall|i: int| 0 <= i < d.len() ==> #[trigger] d[i] == s[i]);
        assert(s[s.len() - 1] == s.last());
        if count_of(s, phase) == 0 {
            assert forall|i: int| 0 <= i < s.len() implies #[trigger] s[i] != phase by {
                if i < s.len() - 1 {
                    assert(d[i] == s[i]);
                }
            }
        }
        if forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i] != phase {
            assert forall|i: int| 0 <= i < d.len() implies #[trigger] d[i] != phase by {
                assert(d[i] == s[i]);
            }
        }
    }
}

proof fn lemma_count_bound(s: Seq<u8>, phase: u8)
    ensures
        count_of(s, phase) <= s.len(),
    decreases s.len(),
{
    if s.len() > 0 {
        lemma_count_bound(s.drop_last(), phase);
    }
}

#[cfg(not(feature = "mut_count_by_deltas"))]
pub fn recount(states: &[u8]) -> (r: Counts)
    ensures
        r.dirty == count_of(states@, DIRTY),
        r.syncing == count_of(states@, SYNCING),
        r.dirty == 0 <==> forall|i: int| 0 <= i < states@.len() ==> #[trigger] states@[i] != DIRTY,
        r.syncing == 0 <==> forall|i: int| 0 <= i < states@.len() ==> #[trigger] states@[i] != SYNCING,
{
    let mut dirty: usize = 0;
    let mut syncing: usize = 0;
    let mut i: usize = 0;
    while i < states.len()
        invariant
            0 <= i <= states.len(),
            dirty as nat == count_of(states@.subrange(0, i as int), DIRTY),
            syncing as nat == count_of(states@.subrange(0, i as int), SYNCING),
            dirty <= i,
            syncing <= i,
        decreases states.len() - i,
    {
        let s = states[i];
        proof {
            let p = states@.subrange(0, i as int + 1);
            assert(p.drop_last() =~= states@.subrange(0, i as int));
            assert(p.last() == states@[i as int]);
        }
        if s == DIRTY {
            dirty += 1;
        }
        if s == SYNCING {
            syncing += 1;
        }
        i += 1;
    }
    proof {
        assert(states@.subrange(0, states@.len() as int) =~= states@);
        lemma_count_zero(states@, DIRTY);
        lemma_count_zero(states@, SYNCING);
    }
    Counts { dirty: dirty as u64, syncing: syncing as u64 }
}

/// Mutation: the load's count (DIRTY and SYNCING blocks, as `load_metadata` counted) with one taken off for each CLEAN block that recovery
/// demotes, and the SYNCING counter left at 0 (as `open` initialised it).
#[cfg(feature = "mut_count_by_deltas")]
pub fn recount(states: &[u8]) -> (r: Counts)
    ensures
        r.dirty == count_of(states@, DIRTY),
        r.syncing == count_of(states@, SYNCING),
        r.dirty == 0 <==> forall|i: int| 0 <= i < states@.len() ==> #[trigger] states@[i] != DIRTY,
        r.syncing == 0 <==> forall|i: int| 0 <= i < states@.len() ==> #[trigger] states@[i] != SYNCING,
{
    let mut dirty: usize = 0;
    let mut i: usize = 0;
    while i < states.len()
        invariant
            0 <= i <= states.len(),
            dirty <= i,
        decreases states.len() - i,
    {
        let s = states[i];
        if s == DIRTY || s == SYNCING {
            dirty += 1;
        }
        if s == CLEAN && dirty > 0 {
            dirty -= 1;
        }
        i += 1;
    }
    Counts { dirty: dirty as u64, syncing: 0 }
}

} // verus!
