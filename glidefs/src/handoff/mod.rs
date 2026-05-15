//! Graceful zero-downtime daemon handoff.
//!
//! Replaces the previous "restart = serve EIO for seconds" failure mode
//! with a coordinated process-to-process handover. The currently-serving
//! daemon (predecessor) launches a successor, the successor does all
//! slow startup work in parallel, and the kernel-visible cutover is
//! just the ublk recovery ioctls — sub-50ms p99 stall.
//!
//! See `/home/jared/.claude/plans/ya-we-need-to-structured-pancake.md`
//! for the full design and rationale.
//!
//! ## Triggering a handoff
//!
//! From the predecessor, any of:
//! - SIGHUP (`kill -HUP $(pidof glidefs)`)
//! - `glidefs handoff` CLI (connects to the daemon's control socket)
//! - `systemctl reload glidefs` (if the systemd unit is configured)
//!
//! ## Entry points
//!
//! - [`predecessor::run_predecessor`] — drives the P state machine. Called
//!   by the SIGHUP handler in `cli/server.rs`.
//! - [`successor::run_successor`] — drives the S state machine. Called by
//!   `main.rs` when `--handoff-from <socket>` is present in argv.

pub mod fault;
pub mod fdpass;
pub mod predecessor;
pub mod protocol;
pub mod strategy;
pub mod successor;

pub use predecessor::{run_predecessor, HandoffOutcome};
pub use protocol::{DEFAULT_HANDOFF_SOCKET, HandoffTimeouts};
pub use successor::{run_successor, TakeoverResult};
