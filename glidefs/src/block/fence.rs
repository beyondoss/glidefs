//! Single-attach generation fence — the storage-layer half of the recovery
//! split-brain fix.
//!
//! Under a network partition, instd's node-death recovery can let two nodes
//! attach and write the same volume at once (a partitioned-but-alive
//! predecessor's lease lapses, a replacement re-forks and re-attaches). A TTL
//! lease provably cannot prevent this; only the storage layer can authoritatively
//! stop two writers. GlideFS does it with a monotonic *fencing token* checked at
//! the resource: any operation carrying a stale token is rejected.
//!
//! The token is **composite** and `u128`-wide, packed lexicographically by
//! [`compose_token`]:
//!
//! - High 64 bits = `generation` — the orchestrator's per-instance placement
//!   generation. It dominates and strictly increases on a cross-node
//!   re-placement (evacuation / deploy), so it fences the cross-node split-brain.
//! - Low 64 bits = `lease_revision` — the node-lease claim revision. It orders
//!   same-`node_id` incarnations: a node-death re-fork onto the *same* node does
//!   NOT advance the orchestrator generation, so without the lease revision two
//!   incarnations would carry equal tokens and neither would fence the other.
//!
//! The composite is the only value monotonic across BOTH transitions. This is
//! the GlideFS copy of the rule model-checked and unit-tested on the instd side —
//! `recovery_kernel.rs::attach_fence` (K4) and its `compose_token`. There is no
//! shared crate; the formal spec (`12-recovery-formal-spec.md` §2.5/§6.1) pins
//! the copies together. Keep this byte-for-byte equivalent to those.

/// Outcome of [`attach_fence`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fence {
    /// `my_token >= stored_token` — admit; this token becomes the accepted
    /// writer, fencing any strictly-older holder out.
    Grant,
    /// `my_token < stored_token` — a newer token owns the volume; reject. A
    /// partitioned / superseded predecessor's writes must not land.
    Reject,
}

/// Pack the composite fencing token: `(generation, lease_revision)`
/// lexicographically into a `u128`, so a plain `>=` realises the lexicographic
/// order. The orchestrator generation dominates (high bits); within a fixed
/// placement the lease revision orders same-node incarnations (low bits).
///
/// Mirror of `recovery_kernel.rs::compose_token`. Keep identical.
#[must_use]
pub fn compose_token(generation: u64, lease_revision: u64) -> u128 {
    ((generation as u128) << 64) | (lease_revision as u128)
}

/// Decide whether an attach/commit carrying composite token `my_token` is
/// admitted, given `stored_token` (the highest token that has owned the volume).
///
/// `>=` (NOT `>`) is deliberate, exactly as instd's K4: a node
/// re-reading/re-attaching its OWN token must still be admitted; only a strictly
/// NEWER token fences an older one out.
///
/// This is the pure rule. The back-compat bypass (a caller that does not
/// participate in fencing) lives at the call sites, NOT here — see
/// [`fence_attach`] — so this function stays a faithful mirror of instd.
#[must_use]
pub fn attach_fence(stored_token: u128, my_token: u128) -> Fence {
    if my_token >= stored_token {
        Fence::Grant
    } else {
        Fence::Reject
    }
}

/// Call-site wrapper applying the back-compat bypass around [`attach_fence`],
/// composing the caller's `(generation, lease_revision)` against the stored
/// `(stored_generation, stored_lease_revision)`.
///
/// Fencing engages only when the caller opts in with a non-zero token. A caller
/// presenting `generation == 0 && lease_revision == 0` (a legacy instd that
/// sends neither, or any non-participating client) is always granted and never
/// bumps the stored token. Note the bypass requires BOTH halves to be zero: a
/// caller that sends only a `lease_revision` (generation 0, lease > 0) DOES
/// participate and is fenced on the composite. Roll-out is therefore one-way per
/// volume: once a volume has been stamped with any non-zero token, only callers
/// presenting a high-enough composite are admitted.
#[must_use]
pub fn fence_attach(
    stored_generation: u64,
    stored_lease_revision: u64,
    my_generation: u64,
    my_lease_revision: u64,
) -> Fence {
    if my_generation == 0 && my_lease_revision == 0 {
        return Fence::Grant;
    }
    attach_fence(
        compose_token(stored_generation, stored_lease_revision),
        compose_token(my_generation, my_lease_revision),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_generation_grants() {
        assert_eq!(
            attach_fence(compose_token(5, 0), compose_token(6, 0)),
            Fence::Grant
        );
    }

    #[test]
    fn equal_token_grants_reattach() {
        // A node re-attaching its OWN token must be admitted (idempotent
        // re-attach / retry). This is why the rule is `>=`, not `>`.
        assert_eq!(
            attach_fence(compose_token(5, 0), compose_token(5, 0)),
            Fence::Grant
        );
    }

    #[test]
    fn older_generation_rejects() {
        assert_eq!(
            attach_fence(compose_token(5, 0), compose_token(4, 0)),
            Fence::Reject
        );
    }

    #[test]
    fn first_owner_grants_against_zero() {
        assert_eq!(
            attach_fence(compose_token(0, 0), compose_token(1, 0)),
            Fence::Grant
        );
    }

    #[test]
    fn generation_dominates_lease_revision() {
        // The orchestrator generation is the high half: it dominates the lease
        // revision regardless of how large the latter is.
        assert!(compose_token(2, 0) > compose_token(1, u64::MAX));
        // A cross-node move (higher generation, fresh/low lease) still wins.
        assert_eq!(
            attach_fence(compose_token(1, 9), compose_token(2, 1)),
            Fence::Grant
        );
    }

    #[test]
    fn same_generation_higher_lease_revision_grants_seizes() {
        // The same-node re-fork case the widening exists for: the orchestrator
        // generation does NOT advance, but the node-lease revision does — the
        // fresher incarnation seizes.
        assert_eq!(
            attach_fence(compose_token(7, 3), compose_token(7, 4)),
            Fence::Grant
        );
    }

    #[test]
    fn same_generation_lower_lease_revision_rejects() {
        // A stale same-node predecessor (lower lease revision under the same
        // generation) is fenced out.
        assert_eq!(
            attach_fence(compose_token(7, 4), compose_token(7, 3)),
            Fence::Reject
        );
    }

    #[test]
    fn zero_token_bypass_always_grants() {
        // A fully non-participating caller (generation == 0 AND
        // lease_revision == 0) is never fenced, even against an already-fenced
        // volume — this is the back-compat bypass.
        assert_eq!(fence_attach(5, 0, 0, 0), Fence::Grant);
        assert_eq!(fence_attach(0, 0, 0, 0), Fence::Grant);
        assert_eq!(fence_attach(9, 4, 0, 0), Fence::Grant);
    }

    #[test]
    fn lease_only_caller_participates() {
        // A caller sending only a lease_revision (generation 0, lease > 0) is NOT
        // bypassed — it participates and is ordered against the stored token.
        // (gen=0, lease=5) vs stored (gen=0, lease=3) → Grant (seizes).
        assert_eq!(fence_attach(0, 3, 0, 5), Fence::Grant);
        // (gen=0, lease=3) vs stored (gen=0, lease=5) → Reject (stale).
        assert_eq!(fence_attach(0, 5, 0, 3), Fence::Reject);
    }

    #[test]
    fn bypass_does_not_change_pure_rule() {
        // With a participating token the wrapper is identical to the pure rule
        // over the composites.
        assert_eq!(
            fence_attach(5, 0, 6, 0),
            attach_fence(compose_token(5, 0), compose_token(6, 0))
        );
        assert_eq!(
            fence_attach(5, 0, 4, 0),
            attach_fence(compose_token(5, 0), compose_token(4, 0))
        );
        assert_eq!(
            fence_attach(7, 3, 7, 4),
            attach_fence(compose_token(7, 3), compose_token(7, 4))
        );
    }
}
