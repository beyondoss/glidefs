//! Cutover strategy abstraction.
//!
//! The handoff state machine is identical regardless of how the actual
//! kernel-level cutover happens. The differences live behind this trait.
//! Today only [`crh::CrhStrategy`] is implemented (kernel ≤ 6.15);
//! [`piod::PiodStrategy`] is reserved for kernel 6.16+ with
//! `UBLK_F_PER_IO_DAEMON`.
//!
//! ## Adding a new strategy
//!
//! 1. Create a new submodule, implementing [`CutoverStrategy`].
//! 2. Add a variant to [`select`] gated on the relevant feature flag.
//! 3. Add the strategy to [`crate::handoff::protocol::Capabilities`].
//! 4. Parameterize `handoff_durability.rs` over the new strategy.
//!
//! The handoff socket protocol, freeze typestate, fd inheritance, state
//! machine, fallback recovery, and the infallible test are all strategy-
//! agnostic — they don't change.

pub mod crh;
pub mod piod;

use crate::block::handler::BlockHandler;
use crate::handoff::protocol::ExportSnapshot;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

/// Context passed to the predecessor's cutover step.
///
/// Carries everything needed to drive the kernel-level handoff and the
/// fallback-revival metadata. Mutable because `predecessor_cutover` may
/// take ownership of the `UblkServer` (CRH drops it).
#[cfg(all(target_os = "linux", feature = "ublk"))]
pub struct PredecessorCutoverCtx {
    /// The router whose UblkServer this cutover will retire. Kept alive
    /// across the cutover so revival is possible if the successor crashes
    /// after PredsDead.
    pub router: Arc<crate::block::router::ExportRouter>,
}

#[cfg(not(all(target_os = "linux", feature = "ublk")))]
pub struct PredecessorCutoverCtx {
    /// Same as the Linux variant minus the ublk-specific fields.
    pub router: Arc<crate::block::router::ExportRouter>,
}

/// Context passed to the successor's takeover step.
pub struct SuccessorTakeoverCtx {
    /// The router that the successor has built during WARMING. Has
    /// BlockHandlers for every export in `exports`.
    pub router: Arc<crate::block::router::ExportRouter>,
    /// The export snapshot the predecessor sent in HelloAck. Successor
    /// uses dev_ids to call `recover_devices_by_id`.
    pub exports: Vec<ExportSnapshot>,
}

/// Trait abstracting the kernel-level cutover step.
///
/// The two methods sandwich the QUIESCED → LIVE transition (for CRH) or
/// the per-tag handoff (for future PIOD). Both run during the stall
/// window between `Cutover` and `Alive` on the wire.
///
/// Implementors must be `Send + Sync` because the strategy is held in a
/// `Box<dyn CutoverStrategy>` shared across async tasks.
#[async_trait]
pub trait CutoverStrategy: Send + Sync {
    /// Strategy name for tracing/metrics. Matches the `strategy` field in
    /// `HelloAck`.
    fn name(&self) -> &'static str;

    /// Predecessor-side cutover. Called after FREEZE, before sending the
    /// `PredsDead` wire message.
    ///
    /// For CRH: drops the UblkServer from the router → kernel transitions
    /// devices to QUIESCED. Returns once the UblkServer is fully dropped.
    ///
    /// For PIOD (future): per-tag handoff coordination.
    async fn predecessor_cutover(&self, ctx: &mut PredecessorCutoverCtx) -> Result<()>;

    /// Successor-side takeover. Called after receiving `PredsDead`, before
    /// sending `Alive`.
    ///
    /// For CRH: calls `recover_devices_by_id` on the router, which
    /// reattaches every QUIESCED device with `START_USER_RECOVERY` +
    /// FETCH_REQ + `END_USER_RECOVERY`.
    ///
    /// Returns the number of devices successfully taken over.
    async fn successor_takeover(&self, ctx: &mut SuccessorTakeoverCtx) -> Result<usize>;

    /// Lookup handler for an export by name — used by `successor_takeover`
    /// to satisfy the `get_handler` callback. Default implementation
    /// delegates to the router; strategies generally don't override this.
    fn get_handler(
        &self,
        ctx: &SuccessorTakeoverCtx,
        export_name: &str,
    ) -> Option<Arc<BlockHandler>> {
        // Async lookup happens via the router's `get_handler` method.
        // For the synchronous closure ublk's recovery API requires,
        // wrap with try_lock semantics — fine because handlers are
        // already constructed by the time we get here.
        ctx.router.get_handler_sync(export_name)
    }
}

/// Select the appropriate cutover strategy for the running kernel.
///
/// Today: always returns CRH. When PIOD ships, this becomes:
/// ```ignore
/// if per_io_daemon_kernel { Box::new(piod::PiodStrategy::new()) }
/// else { Box::new(crh::CrhStrategy::new()) }
/// ```
///
/// The `per_io_daemon_kernel` flag comes from
/// `glidefs::block::ublk::device::KernelFeatures::per_io_daemon` and is
/// detected at startup via the `UBLK_U_CMD_GET_FEATURES` ioctl.
pub fn select(per_io_daemon_kernel: bool) -> Box<dyn CutoverStrategy> {
    if per_io_daemon_kernel {
        // PIOD strategy not yet implemented; warn and fall through.
        tracing::warn!(
            "kernel advertises UBLK_F_PER_IO_DAEMON but PiodStrategy is not yet \
             implemented in this build; falling back to CrhStrategy (QUIESCED-based)"
        );
    }
    Box::new(crh::CrhStrategy::new())
}
