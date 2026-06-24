//! Single-attach generation fence — the storage-layer half of the recovery
//! split-brain fix.
//!
//! Under a network partition, instd's node-death recovery can let two nodes
//! attach and write the same volume at once (a partitioned-but-alive
//! predecessor's lease lapses, a replacement re-forks and re-attaches). A TTL
//! lease provably cannot prevent this; only the storage layer can authoritatively
//! stop two writers. GlideFS does it with a monotonic *generation* (fencing
//! token) issued by the orchestrator's per-instance placement generation: the
//! token is checked at the resource, and any operation carrying a stale token is
//! rejected.
//!
//! This is the GlideFS copy of the rule model-checked and unit-tested on the
//! instd side — `recovery_kernel.rs::attach_fence` (K4) and
//! `placement_kernel.rs::fence_check` (P3). There is no shared crate; the formal
//! spec (`12-recovery-formal-spec.md` §6.1) pins the copies together. Keep this
//! function byte-for-byte equivalent to those.

/// Outcome of [`attach_fence`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fence {
    /// `my_gen >= stored_gen` — admit; this generation becomes the accepted
    /// writer, fencing any strictly-older holder out.
    Grant,
    /// `my_gen < stored_gen` — a newer generation owns the volume; reject. A
    /// partitioned / superseded predecessor's writes must not land.
    Reject,
}

/// Decide whether an attach/commit carrying generation `my_gen` is admitted,
/// given `stored_gen` (the highest generation that has owned the volume).
///
/// `>=` (NOT `>`) is deliberate, exactly as instd's K4/P3: a node
/// re-reading/re-attaching its OWN generation must still be admitted; only a
/// strictly NEWER generation fences an older one out.
///
/// This is the pure rule. The gen-0 back-compat bypass (a caller that does not
/// participate in fencing) lives at the call sites, NOT here — see
/// [`fence_attach`] — so this function stays a faithful mirror of instd.
#[must_use]
pub fn attach_fence(stored_gen: u64, my_gen: u64) -> Fence {
    if my_gen >= stored_gen {
        Fence::Grant
    } else {
        Fence::Reject
    }
}

/// Call-site wrapper applying the back-compat bypass around [`attach_fence`].
///
/// Fencing engages only when the caller opts in with `my_gen > 0`. A
/// `my_gen == 0` caller (a legacy instd that does not send a generation, or any
/// non-participating client) is always granted and never bumps the stored
/// generation. Roll-out is therefore one-way per volume: once a volume has been
/// stamped with a generation `>= 1`, only callers presenting a high-enough
/// generation are admitted.
#[must_use]
pub fn fence_attach(stored_gen: u64, my_gen: u64) -> Fence {
    if my_gen == 0 {
        return Fence::Grant;
    }
    attach_fence(stored_gen, my_gen)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_generation_grants() {
        assert_eq!(attach_fence(5, 6), Fence::Grant);
    }

    #[test]
    fn equal_generation_grants_reattach() {
        // A node re-attaching its OWN generation must be admitted (idempotent
        // re-attach / retry). This is why the rule is `>=`, not `>`.
        assert_eq!(attach_fence(5, 5), Fence::Grant);
    }

    #[test]
    fn older_generation_rejects() {
        assert_eq!(attach_fence(5, 4), Fence::Reject);
    }

    #[test]
    fn first_owner_grants_against_zero() {
        assert_eq!(attach_fence(0, 1), Fence::Grant);
    }

    #[test]
    fn gen_zero_bypass_always_grants() {
        // A non-participating caller (my_gen == 0) is never fenced, even against
        // an already-fenced volume — this is the back-compat bypass.
        assert_eq!(fence_attach(5, 0), Fence::Grant);
        assert_eq!(fence_attach(0, 0), Fence::Grant);
    }

    #[test]
    fn gen_zero_bypass_does_not_change_pure_rule() {
        // With my_gen > 0 the wrapper is identical to the pure rule.
        assert_eq!(fence_attach(5, 6), attach_fence(5, 6));
        assert_eq!(fence_attach(5, 4), attach_fence(5, 4));
        assert_eq!(fence_attach(5, 5), attach_fence(5, 5));
    }
}
