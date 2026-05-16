//! Cooperative Recovery Handoff strategy (CRH).
//!
//! Works on every kernel ≥ 6.0 that supports `UBLK_F_USER_RECOVERY`.
//! Achieves sub-50ms p99 stall by doing ALL slow startup work in the
//! successor's WARMING phase (foyer cache open, WAL replay, S3 prefetch,
//! ExportRouter construction) while the predecessor is still serving I/O.
//!
//! The actual stall window contains only:
//! 1. Predecessor drops `UblkServer` → kernel transitions devices to
//!    QUIESCED (microseconds).
//! 2. Successor calls `recover_devices_by_id` → for each device,
//!    `START_USER_RECOVERY` ioctl + FETCH_REQ per tag + `END_USER_RECOVERY`
//!    ioctl. Parallelized at `RECOVERY_CONCURRENCY = 64`.
//!
//! At 1000-device density: scan-free recovery hits the parallel ioctl
//! floor (~5–30 ms wall clock). Bios that queued in the kernel during the
//! QUIESCED window drain into the successor's FETCH CQEs immediately.

use super::{CutoverStrategy, PredecessorCutoverCtx, SuccessorTakeoverCtx};
use anyhow::{Context, Result};
use async_trait::async_trait;

pub const NAME: &str = "crh";

/// CRH strategy — the only kernel-version-agnostic cutover.
#[derive(Default)]
pub struct CrhStrategy {
    _private: (),
}

impl CrhStrategy {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[async_trait]
impl CutoverStrategy for CrhStrategy {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn predecessor_cutover(&self, ctx: &mut PredecessorCutoverCtx) -> Result<()> {
        tracing::info!("CRH: predecessor cutover — dropping UblkServer");

        // Take the UblkServer out of the router. The coordinator keeps its
        // ExportRouter / WriteCache mounts alive (revival fallback if S
        // crashes between PredsDead and Alive).
        #[cfg(all(target_os = "linux", feature = "ublk"))]
        {
            ctx.coord
                .take_ublk_server()
                .await
                .context("failed to take ublk server from router for handoff cutover")?;
        }
        #[cfg(not(all(target_os = "linux", feature = "ublk")))]
        {
            let _ = ctx;
        }

        tracing::info!(
            "CRH: predecessor cutover complete — kernel devices QUIESCED, awaiting successor"
        );
        Ok(())
    }

    async fn successor_takeover(&self, ctx: &mut SuccessorTakeoverCtx) -> Result<usize> {
        #[cfg(all(target_os = "linux", feature = "ublk"))]
        {
            // Collect (dev_id, export_name) pairs from the snapshot.
            let ids: Vec<(i32, String)> = ctx
                .exports
                .iter()
                .filter_map(|e| e.ublk_dev_id.map(|id| (id, e.name.clone())))
                .collect();

            if ids.is_empty() {
                tracing::info!("CRH: no ublk devices in handoff snapshot");
                return Ok(0);
            }

            tracing::info!(count = ids.len(), "CRH: successor takeover — recovering devices");

            // Strategy holds no per-export state; the coordinator owns
            // the UblkServer that does the actual recovery work.
            let recovered = ctx
                .coord
                .recover_handoff_devices(&ids)
                .await
                .context("CRH successor takeover failed during recover_devices_by_id")?;

            tracing::info!(recovered, total = ids.len(), "CRH: successor takeover complete");
            Ok(recovered)
        }
        #[cfg(not(all(target_os = "linux", feature = "ublk")))]
        {
            let _ = ctx;
            Ok(0)
        }
    }
}
