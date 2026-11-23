//! The attach fence's decision: the composite token `generation * 2^64 + lease_revision` and the admit rule over it, as pure functions proven
//! with Verus, called verbatim by `glidefs`'s `block::fence` (and so by `enforce_attach_fence` and the attach/commit routes). The twin on
//! instd's side is `recovery_kernel::{compose_token, attach_fence}`; the two are stated by the same lemmas.
//!
//! * [`thm_a_stale_placement_generation_is_refused`]: a token of an older placement generation is refused whatever either lease revision.
//! * [`thm_a_caught_up_token_is_admitted`]: a newer generation, or the same generation with a revision not lower, is admitted; the converse
//!   is refused.
//!
//! Mutations (`scripts/verify-kernels.sh --features mut_x` must go red, and the glidefs `block::fence` tests fail):
//! `mut_attach_admits_older` (every token is admitted), `mut_token_revision_dominates` (the revision takes the high bits).

use vstd::prelude::*;

verus! {

/// Outcome of [`attach_fence`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fence {
    /// `my_token >= stored_token`: admit.
    Grant,
    /// `my_token < stored_token`: a newer token owns the volume; reject.
    Reject,
}

/// The fence's answer as a spec: `>=` on the composite (a node re-attaching with its own token is admitted).
pub open spec fn attach_spec(stored: u128, mine: u128) -> Fence {
    if mine >= stored { Fence::Grant } else { Fence::Reject }
}

pub fn attach_fence(stored_token: u128, my_token: u128) -> (r: Fence)
    ensures
        r == attach_spec(stored_token, my_token),
{
    if my_token >= stored_token || cfg!(feature = "mut_attach_admits_older") {
        Fence::Grant
    } else {
        Fence::Reject
    }
}

pub open spec fn compose_spec(generation: u64, lease_revision: u64) -> int {
    (generation as int) * 0x1_0000_0000_0000_0000int + (lease_revision as int)
}

/// Pack `(generation, lease_revision)` into a `u128` so a plain `>=` is the lexicographic order.
pub fn compose_token(generation: u64, lease_revision: u64) -> (t: u128)
    ensures
        t as int == compose_spec(generation, lease_revision),
{
    // `mut_token_revision_dominates`: the revision takes the high bits, so a newer lease outranks a newer placement generation.
    if cfg!(feature = "mut_token_revision_dominates") {
        return (lease_revision as u128) * 0x1_0000_0000_0000_0000u128 + (generation as u128);
    }
    proof {
        assert((generation as int) * 0x1_0000_0000_0000_0000int <= 0xFFFF_FFFF_FFFF_FFFFint * 0x1_0000_0000_0000_0000int) by (nonlinear_arith)
            requires (generation as int) <= 0xFFFF_FFFF_FFFF_FFFFint;
    }
    (generation as u128) * 0x1_0000_0000_0000_0000u128 + (lease_revision as u128)
}

/// Call-site wrapper: the caller's `(generation, lease_revision)` against the stored one. The back-compat bypass is NOT here.
pub fn fence_attach(stored_generation: u64, stored_lease_revision: u64, my_generation: u64, my_lease_revision: u64) -> (r: Fence)
    ensures
        r == attach_spec(compose_spec(stored_generation, stored_lease_revision) as u128, compose_spec(my_generation, my_lease_revision) as u128),
{
    attach_fence(compose_token(stored_generation, stored_lease_revision), compose_token(my_generation, my_lease_revision))
}

pub proof fn lemma_token_order(g1: u64, r1: u64, g2: u64, r2: u64)
    ensures
        g1 < g2 ==> compose_spec(g1, r1) < compose_spec(g2, r2),
        (g1 == g2 && r1 <= r2) ==> compose_spec(g1, r1) <= compose_spec(g2, r2),
        compose_spec(g1, r1) <= 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFFint,
{
    assert(g1 < g2 ==> compose_spec(g1, r1) < compose_spec(g2, r2)) by (nonlinear_arith)
        requires
            0 <= r1 as int <= 0xFFFF_FFFF_FFFF_FFFFint,
            0 <= r2 as int <= 0xFFFF_FFFF_FFFF_FFFFint,
    ;
    assert(compose_spec(g1, r1) <= 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFFint) by (nonlinear_arith)
        requires
            0 <= g1 as int <= 0xFFFF_FFFF_FFFF_FFFFint,
            0 <= r1 as int <= 0xFFFF_FFFF_FFFF_FFFFint,
    ;
}

/// P.volume.a-stale-placement-generation-is-refused-at-the-attach: a token of an older placement generation is refused by the attach
/// fence over the composite token, whatever either lease revision is.
pub proof fn thm_a_stale_placement_generation_is_refused(stored_gen: u64, stored_rev: u64, my_gen: u64, my_rev: u64)
    requires
        my_gen < stored_gen,
    ensures
        attach_spec(compose_spec(stored_gen, stored_rev) as u128, compose_spec(my_gen, my_rev) as u128) == Fence::Reject,
{
    lemma_token_order(my_gen, my_rev, stored_gen, stored_rev);
    lemma_token_order(stored_gen, stored_rev, my_gen, my_rev);
}

/// P.volume.a-refused-attach-is-admitted-once-its-token-catches-up: a newer generation, or the same generation with a lease revision that
/// did not fall, is admitted; the same generation with a lower revision is refused.
pub proof fn thm_a_caught_up_token_is_admitted(stored_gen: u64, stored_rev: u64, my_gen: u64, my_rev: u64)
    ensures
        (my_gen > stored_gen || (my_gen == stored_gen && my_rev >= stored_rev))
            ==> attach_spec(compose_spec(stored_gen, stored_rev) as u128, compose_spec(my_gen, my_rev) as u128) == Fence::Grant,
        (my_gen < stored_gen || (my_gen == stored_gen && my_rev < stored_rev))
            ==> attach_spec(compose_spec(stored_gen, stored_rev) as u128, compose_spec(my_gen, my_rev) as u128) == Fence::Reject,
{
    lemma_token_order(stored_gen, stored_rev, my_gen, my_rev);
    lemma_token_order(my_gen, my_rev, stored_gen, stored_rev);
}

} // verus!
