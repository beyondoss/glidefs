//! Per-IO Daemon (PIOD) handoff strategy — kernel 6.16+.
//!
//! **NOT YET IMPLEMENTED.** This file is a placeholder/design note so the
//! extension point is visible in the source tree. See
//! `/home/jared/.claude/plans/ya-we-need-to-structured-pancake.md` for
//! the protocol design.
//!
//! ## What this strategy does (when implemented)
//!
//! `UBLK_F_PER_IO_DAEMON` lifts the per-queue single-daemon restriction.
//! Each tag tracks its own daemon (set on FETCH_REQ submission). The
//! predecessor and successor can hold FETCH_REQs for different tags of
//! the same queue concurrently. The handoff becomes per-tag:
//!
//! ```text
//! for each tag in each queue:
//!     P stops re-fetching (no new FETCH after this tag's COMMIT)
//!     S submits FETCH for this tag (kernel sets per-tag daemon = S)
//! P closes its io_urings; kernel sees S still driving every tag → stays LIVE
//! ```
//!
//! Result: no QUIESCED transition. Stall reduces from CRH's 5–50 ms to
//! sub-millisecond (just the FETCH_REQ submission latency).
//!
//! ## Adding it
//!
//! 1. Uncomment / implement `PiodStrategy` below.
//! 2. Add a per-tag handoff protocol payload to
//!    `crate::handoff::protocol::HandoffMessage::StrategyMsg` (or new
//!    explicit variants).
//! 3. Flip `crate::handoff::protocol::Capabilities::current().piod` to
//!    `true` (gate on a `cfg(feature = "piod")` if you want to keep it
//!    compile-time-optional).
//! 4. Update `crate::handoff::strategy::select` to return `PiodStrategy`
//!    when `per_io_daemon_kernel` is true.
//! 5. Add a second `#[test]` to `handoff_durability.rs` invoking
//!    `PiodStrategy` (the test body is already parameterized).
//!
//! Estimated work: ~1 week. The infrastructure (handoff socket protocol,
//! freeze typestate, listener fd inheritance, state machine, fallback
//! recovery, the test) is identical to CRH and doesn't change.

// TODO(piod): implement on kernel 6.16+. See module-level doc for protocol.
//
// pub struct PiodStrategy { /* ... */ }
//
// #[async_trait::async_trait]
// impl super::CutoverStrategy for PiodStrategy {
//     fn name(&self) -> &'static str { "piod" }
//     async fn predecessor_cutover(&self, ctx: &mut super::PredecessorCutoverCtx)
//         -> anyhow::Result<()> { todo!("per-tag handoff") }
//     async fn successor_takeover(&self, ctx: &mut super::SuccessorTakeoverCtx)
//         -> anyhow::Result<usize> { todo!("per-tag takeover") }
// }
