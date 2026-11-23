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
//! The composite is the only value monotonic across BOTH transitions. The rule
//! ([`compose_token`], [`attach_fence`], [`fence_attach`]) is the pure kernel
//! `glidefs_kernels::fence_kernel`, proven with Verus: a stale placement
//! generation is refused whatever the revisions, and a token that caught up is
//! admitted. instd's `recovery_kernel.rs` states the same lemmas over its twin.

pub use glidefs_kernels::fence_kernel::{Attach, Fence, attach_fence, attach_seize, compose_token, fence_attach};

#[cfg(test)]
mod tests {
    use super::*;

    const POINTS: [u64; 6] = [0, 1, 2, 7, u64::MAX - 1, u64::MAX];

    /// P.volume.a-stale-placement-generation-is-refused-at-the-attach (Verus: `fence_kernel::thm_a_stale_placement_generation_is_refused`).
    #[test]
    fn a_stale_generation_is_refused_whatever_its_revision() {
        for sg in POINTS {
            for sr in POINTS {
                for g in POINTS.into_iter().filter(|g| *g < sg) {
                    for r in POINTS {
                        assert_eq!(fence_attach(sg, sr, g, r), Fence::Reject, "stored ({sg},{sr}) mine ({g},{r})");
                    }
                }
            }
        }
    }

    /// P.volume.a-refused-attach-is-admitted-once-its-token-catches-up (Verus: `fence_kernel::thm_a_caught_up_token_is_admitted`).
    #[test]
    fn a_caught_up_token_is_admitted() {
        for sg in POINTS {
            for sr in POINTS {
                for g in POINTS {
                    for r in POINTS {
                        let up = g > sg || (g == sg && r >= sr);
                        let want = if up { Fence::Grant } else { Fence::Reject };
                        assert_eq!(fence_attach(sg, sr, g, r), want, "stored ({sg},{sr}) mine ({g},{r})");
                    }
                }
            }
        }
    }

    /// P.volume.glidefs-grants-an-attach-only-at-the-highest-token (Verus: `fence_kernel::thm_of_two_racing_attaches_only_the_higher_is_the_holder`):
    /// of two attaches with different tokens against a volume at or below the higher, either order leaves the higher as the holder, and the lower
    /// one is refused when it comes second.
    #[test]
    fn of_two_racing_attaches_only_the_higher_holds() {
        let stored = (5u64, 1u64);
        let lo = (6u64, 2u64);
        let hi = (6u64, 9u64);
        let step = |s: (u64, u64), m: (u64, u64)| {
            let d = attach_seize(s.0, s.1, m.0, m.1);
            (d.fence, (d.holder_generation, d.holder_lease_revision))
        };
        let (f1, s1) = step(stored, lo);
        assert_eq!(f1, Fence::Grant);
        let (f2, s2) = step(s1, hi);
        assert_eq!((f2, s2), (Fence::Grant, hi), "lower first, then higher: the higher holds");
        let (g1, t1) = step(stored, hi);
        assert_eq!((g1, t1), (Fence::Grant, hi));
        let (g2, t2) = step(t1, lo);
        assert_eq!((g2, t2), (Fence::Reject, hi), "higher first: the lower is refused and the holder stays");
    }

    /// An equal-token re-attach (and a `0/0` caller against an unfenced volume) is granted and stamps nothing.
    #[test]
    fn an_equal_token_attach_stamps_nothing() {
        let d = attach_seize(5, 3, 5, 3);
        assert_eq!((d.fence, d.seize), (Fence::Grant, false));
        let d = attach_seize(0, 0, 0, 0);
        assert_eq!((d.fence, d.seize), (Fence::Grant, false));
        let d = attach_seize(5, 3, 5, 4);
        assert_eq!((d.fence, d.seize), (Fence::Grant, true));
    }

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
    fn zero_token_does_not_bypass_a_fenced_volume() {
        // First attach against an unfenced volume is still Grant.
        assert_eq!(fence_attach(0, 0, 0, 0), Fence::Grant);
        // Once a volume holds a real token, (0,0) must not sneak in —
        // that is the same machine as attach_fence(stored, compose(0,0)).
        assert_eq!(fence_attach(5, 0, 0, 0), Fence::Reject);
        assert_eq!(fence_attach(9, 4, 0, 0), Fence::Reject);
        assert_eq!(
            fence_attach(9, 4, 0, 0),
            attach_fence(compose_token(9, 4), compose_token(0, 0))
        );
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
