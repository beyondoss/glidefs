//! The attach fence's decision: the composite token `generation * 2^64 + lease_revision` and the admit rule over it, as pure functions proven
//! with Verus, called verbatim by `glidefs`'s `block::fence` (and so by `enforce_attach_fence` and the attach/commit routes). The twin on
//! instd's side is `recovery_kernel::{compose_token, attach_fence}`; the two are stated by the same lemmas.
//!
//! * [`thm_a_stale_placement_generation_is_refused`]: a token of an older placement generation is refused whatever either lease revision.
//! * [`thm_a_caught_up_token_is_admitted`]: a newer generation, or the same generation with a revision not lower, is admitted; the converse
//!   is refused.
//!
//! * [`attach_seize`] and [`thm_of_two_racing_attaches_only_the_higher_is_the_holder`]: the router's whole attach decision (grant, the token the volume holds
//!   after, whether it stamps), and that of two racing attaches only the higher token is the holder after.
//!
//! Mutations (`scripts/verify-kernels.sh --features mut_x` must go red, and the glidefs `block::fence` tests fail):
//! `mut_attach_admits_older` (every token is admitted), `mut_token_revision_dominates` (the revision takes the high bits),
//! `mut_seize_stamps_the_lower` (a granted attach leaves the stored token as the holder; Verus only).

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

/// What the attach fence decides about one attach, and the token the volume holds after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attach {
    /// Grant or reject.
    pub fence: Fence,
    /// The `(generation, lease_revision)` the volume holds after this attach: the caller's on a grant, the stored one on a reject.
    pub holder_generation: u64,
    pub holder_lease_revision: u64,
    /// The caller's token is higher than the stored one, so the grant must stamp it into the manifest (the seize). Equal tokens (an idempotent
    /// re-attach, and the `0/0` caller against an unfenced volume) stamp nothing.
    pub seize: bool,
}

/// The step the attach takes on the stored token: `mine >= stored` grants and the holder becomes `mine`; otherwise the holder stays `stored`.
pub open spec fn attach_step_spec(stored: int, mine: int) -> (Fence, int) {
    if mine >= stored { (Fence::Grant, mine) } else { (Fence::Reject, stored) }
}

/// `enforce_attach_fence`'s decision (glidefs `block::router`): the fence's answer, the token the volume holds after, and whether the grant
/// stamps a new token. Called verbatim by the router, under the manifest's `If-Match` compare-and-swap for the stamp.
pub fn attach_seize(stored_generation: u64, stored_lease_revision: u64, my_generation: u64, my_lease_revision: u64) -> (r: Attach)
    ensures
        (r.fence, compose_spec(r.holder_generation, r.holder_lease_revision))
            == attach_step_spec(compose_spec(stored_generation, stored_lease_revision), compose_spec(my_generation, my_lease_revision)),
        r.fence == Fence::Grant ==> (r.holder_generation == my_generation && r.holder_lease_revision == my_lease_revision),
        r.fence == Fence::Reject ==> (r.holder_generation == stored_generation && r.holder_lease_revision == stored_lease_revision),
        r.seize == (r.fence == Fence::Grant && (my_generation != stored_generation || my_lease_revision != stored_lease_revision)),
{
    let stored = compose_token(stored_generation, stored_lease_revision);
    let mine = compose_token(my_generation, my_lease_revision);
    proof {
        // Two distinct pairs compose to two distinct tokens, so equal tokens are exactly equal pairs.
        lemma_token_injective(stored_generation, stored_lease_revision, my_generation, my_lease_revision);
    }
    match attach_fence(stored, mine) {
        Fence::Grant => {
            // `mut_seize_stamps_the_lower`: a granted attach leaves the stored (lower) token as the holder.
            if cfg!(feature = "mut_seize_stamps_the_lower") {
                return Attach {
                    fence: Fence::Grant,
                    holder_generation: stored_generation,
                    holder_lease_revision: stored_lease_revision,
                    seize: false,
                };
            }
            Attach {
                fence: Fence::Grant,
                holder_generation: my_generation,
                holder_lease_revision: my_lease_revision,
                seize: my_generation != stored_generation || my_lease_revision != stored_lease_revision,
            }
        },
        Fence::Reject => Attach {
            fence: Fence::Reject,
            holder_generation: stored_generation,
            holder_lease_revision: stored_lease_revision,
            seize: false,
        },
    }
}

/// Two pairs compose to one token only when they are the same pair.
pub proof fn lemma_token_injective(g1: u64, r1: u64, g2: u64, r2: u64)
    ensures
        compose_spec(g1, r1) == compose_spec(g2, r2) <==> (g1 == g2 && r1 == r2),
{
    lemma_token_order(g1, r1, g2, r2);
    lemma_token_order(g2, r2, g1, r1);
    if g1 == g2 {
        assert(compose_spec(g1, r1) == compose_spec(g2, r2) <==> r1 == r2);
    }
}

/// P.volume.glidefs-grants-an-attach-only-at-the-highest-token. The volume's stored token is the highest that has attached: every step leaves
/// it at the larger of itself and the caller's (`holder == max`), and a grant needs the caller's to be at least the stored one. So of two attaches
/// racing with different tokens `a < b` against a volume at `stored <= b`, in either order the holder after is `b`, and when `a` comes after `b`
/// it is refused. Nothing is assumed of the order: the manifest's `If-Match` makes each step read-compare-write one step.
pub proof fn thm_of_two_racing_attaches_only_the_higher_is_the_holder(stored: int, a: int, b: int)
    requires
        a < b,
        stored <= b,
    ensures
        // `a` then `b`: both are granted when `a >= stored`, and the holder is `b`.
        attach_step_spec(attach_step_spec(stored, a).1, b).1 == b,
        // `b` then `a`: `a` is refused, and the holder is `b`.
        attach_step_spec(attach_step_spec(stored, b).1, a).0 == Fence::Reject,
        attach_step_spec(attach_step_spec(stored, b).1, a).1 == b,
        // A grant is only ever at a token at least the highest attached so far, and a refusal never moves the holder.
        forall|s: int, m: int| (attach_step_spec(s, m).0 == Fence::Grant) == (m >= s),
        forall|s: int, m: int| attach_step_spec(s, m).1 == (if m >= s { m } else { s }),
{
}

} // verus!
