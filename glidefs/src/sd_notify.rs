#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! Best-effort sd_notify wrappers for systemd integration.
//!
//! All functions here are no-ops when `$NOTIFY_SOCKET` is unset (the
//! daemon isn't running under systemd, e.g. in tests or interactive
//! shells). Failures are logged and swallowed — sd_notify failures
//! should never crash an otherwise-healthy daemon.
//!
//! ## Why this exists
//!
//! Graceful daemon handoff under systemd requires careful coordination
//! with `Type=notify`:
//!
//! - **Cold start**: the daemon must send `READY=1` once listeners are
//!   bound, otherwise systemd's `TimeoutStartSec` fires.
//! - **Handoff cutover**: the predecessor (current `MainPID`) must
//!   tell systemd that `MainPID` has moved to the successor *before*
//!   the predecessor exits. Otherwise `KillMode=mixed` (or `control-group`)
//!   reaps the entire cgroup when the predecessor — still the recorded
//!   MainPID — exits, killing the successor we just handed control to.
//!
//! See `RUNBOOK.md` for the failure mode this prevents.

use sd_notify::NotifyState;
use tracing::warn;

/// Tell systemd the daemon is ready to serve. Called once during
/// cold-start after listeners are bound.
pub fn notify_ready() {
    if let Err(e) = sd_notify::notify(false, &[NotifyState::Ready]) {
        warn!(error = %e, "sd_notify(READY) failed");
    }
}

/// Hand off `MainPID` ownership to the successor process and mark it
/// as ready, in a single datagram. Called by the predecessor after it
/// receives `ALIVE` from the successor and before it exits — this
/// moves systemd's tracked MainPID off the about-to-exit predecessor
/// so the predecessor's clean exit isn't interpreted as service death.
///
/// Without this, `KillMode=mixed`/`control-group` SIGKILLs every
/// process in the cgroup (including the successor) when the
/// predecessor exits.
///
/// Allowed for the current MainPID under the default `NotifyAccess=main`.
pub fn notify_handoff_to(successor_pid: u32) {
    if let Err(e) = sd_notify::notify(
        false,
        &[NotifyState::MainPid(successor_pid), NotifyState::Ready],
    ) {
        warn!(
            error = %e,
            successor_pid,
            "sd_notify(MAINPID + READY) failed; systemd may kill the successor"
        );
    }
}

/// Tell systemd the daemon is starting a reload (graceful handoff).
/// Paired with `notify_handoff_to` ("reload complete, MainPID has moved").
/// Optional under `Type=notify`; required under `Type=notify-reload`.
pub fn notify_reloading() {
    let monotonic_usec: i128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i128)
        .unwrap_or(0);
    if let Err(e) = sd_notify::notify(
        false,
        &[
            NotifyState::Reloading,
            NotifyState::MonotonicUsec(monotonic_usec),
        ],
    ) {
        warn!(error = %e, "sd_notify(RELOADING) failed");
    }
}
