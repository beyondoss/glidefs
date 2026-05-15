//! Wire protocol for graceful daemon handoff over an AF_UNIX SOCK_SEQPACKET
//! socket.
//!
//! Messages are bincode-serialized framed datagrams. SEQPACKET preserves
//! message boundaries and supports SCM_RIGHTS (used in a future phase for
//! passing listener fds). Each call is a single sendmsg/recvmsg.
//!
//! ## Protocol versioning
//!
//! The first message is always `HELLO { protocol_version, capabilities, ... }`.
//! Both sides check that they understand the version. Unknown wire variants
//! that arrive on a version-compatible socket are forwarded to the active
//! [`crate::handoff::strategy::CutoverStrategy`] — this is the extension
//! point that lets the PIOD strategy add per-tag handoff messages without
//! breaking the CRH wire format.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Wire-format version. Bump when adding *non-extensible* changes.
/// Capability-bit additions (extensible) don't require a bump.
pub const PROTOCOL_VERSION: u32 = 1;

/// Default handoff socket path. Configurable via CLI / config in case
/// multiple daemons share a host.
pub const DEFAULT_HANDOFF_SOCKET: &str = "/run/glidefs/handoff.sock";

/// Wire protocol bytes for the handoff *control* socket
/// (`/run/glidefs/handoff.ctl.sock`). One byte each way:
/// - Request: REQUEST_HANDOFF or REQUEST_HANDOFF_DRY_RUN.
/// - Response: RESPONSE_ACCEPTED / BUSY / UNSUPPORTED.
///
/// Used by the `glidefs handoff` CLI, the `POST /admin/handoff` HTTP
/// API endpoint, and any orchestrator that wants to trigger a handoff
/// without sending SIGHUP.
pub mod ctl_wire {
    pub const REQUEST_HANDOFF: u8 = b'H';
    pub const REQUEST_HANDOFF_DRY_RUN: u8 = b'D';
    pub const RESPONSE_ACCEPTED: u8 = b'A';
    pub const RESPONSE_BUSY: u8 = b'B';
    pub const RESPONSE_UNSUPPORTED: u8 = b'U';
}

/// Capability bits negotiated in HELLO. Both sides advertise their
/// supported strategies; the cutover uses the highest-priority strategy
/// supported by both.
///
/// Today CRH is the only implemented strategy and is always set.
/// PIOD is reserved for kernel-6.16+ — when added, the negotiator picks
/// PIOD if both sides advertise it AND the kernel reports
/// `UBLK_F_PER_IO_DAEMON`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Cooperative Recovery Handoff (QUIESCED → recover). Always supported.
    pub crh: bool,
    /// Per-IO Daemon (kernel 6.16+ true zero-stall). Reserved.
    pub piod: bool,
}

impl Capabilities {
    /// Capabilities of the current build. The runtime kernel-feature
    /// check (`features.per_io_daemon`) further restricts what the
    /// strategy selector actually picks; this just advertises what code
    /// paths the binary contains.
    pub fn current() -> Self {
        Self {
            crh: true,
            // Set to true once `strategy::piod` is implemented and gated
            // off `cfg(feature = "piod")` or similar.
            piod: false,
        }
    }

    /// Intersect two capability sets. Used during HELLO negotiation.
    pub fn intersect(self, other: Self) -> Self {
        Self {
            crh: self.crh && other.crh,
            piod: self.piod && other.piod,
        }
    }
}

/// One entry in the predecessor's export snapshot. Carries everything the
/// successor needs to construct a matching [`crate::block::handler::BlockHandler`]
/// during its WARMING phase. Detail of the in-memory state (dirty blocks,
/// pending S3 ops, etc.) is not transferred — the successor rebuilds from
/// disk (foyer SSD + WAL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportSnapshot {
    /// Stable export name (used as key in the router).
    pub name: String,
    /// Device size in bytes.
    pub size_bytes: u64,
    /// Whether this export is currently readonly.
    pub readonly: bool,
    /// Last WAL sequence number the predecessor has fsynced. Successor
    /// uses this to verify WAL replay reached at least this point —
    /// detects "predecessor flushed past us" bugs early.
    pub last_wal_seq: u64,
    /// If this export has a ublk device, its kernel dev_id. None if
    /// only served via NBD/HTTP.
    pub ublk_dev_id: Option<i32>,
}

/// Reasons a handoff aborts. Recorded in tracing and the failure metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AbortReason {
    /// Protocol version mismatch.
    VersionMismatch { ours: u32, theirs: u32 },
    /// No common cutover strategy in negotiated capabilities.
    NoCommonStrategy,
    /// Successor's loaded router doesn't include some exports the
    /// predecessor lists. Indicates config drift between predecessor
    /// and successor binaries.
    ExportMismatch { missing: Vec<String> },
    /// Successor failed to warm up (foyer open, WAL replay, etc.).
    WarmingFailed { detail: String },
    /// Predecessor failed to freeze (in-flight writes blocked, WAL fsync
    /// failed, etc.).
    FreezeFailed { detail: String },
    /// Generic catch-all; populated with a human-readable string.
    Other(String),
}

/// Wire messages. SEQPACKET-framed. Each is a single bincode payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HandoffMessage {
    /// First message — sent by successor immediately after connecting.
    /// Predecessor responds with `HelloAck`.
    Hello {
        protocol_version: u32,
        capabilities: Capabilities,
        /// PID of the successor process. Predecessor logs it; not load-bearing.
        successor_pid: i32,
        /// Dry-run mode: the successor performs WARMING (proves it can
        /// open foyer cache, replay WAL, build router, prefetch S3)
        /// then signals abort. Predecessor never reaches CUTOVER, so
        /// nothing destructive happens. Operator gets a yes/no on
        /// "would this binary handoff cleanly". Default false.
        #[serde(default)]
        dry_run: bool,
    },

    /// Predecessor's response to Hello — carries the export inventory.
    /// Successor verifies its loaded router includes every export here.
    HelloAck {
        protocol_version: u32,
        capabilities: Capabilities,
        /// Negotiated strategy name (e.g., "crh"). Both sides must agree.
        strategy: String,
        /// Predecessor's export snapshot. Successor verifies it has a
        /// handler for every one of these.
        exports: Vec<ExportSnapshot>,
        /// PID of the predecessor process. Successor logs it.
        predecessor_pid: i32,
    },

    /// Successor signals it has finished WARMING — all slow startup work
    /// done, ready to take over. Predecessor responds with `Cutover`.
    Ready,

    /// Predecessor signals it has frozen its handlers and is about to
    /// drop its UblkServer. After sending this, the predecessor performs
    /// the strategy-specific cutover step.
    Cutover,

    /// Predecessor signals it has dropped its UblkServer (CRH: kernel
    /// devices are now QUIESCED). Successor performs strategy-specific
    /// takeover.
    PredsDead,

    /// Successor signals it has fully taken over — all devices LIVE,
    /// listeners accepting. Predecessor exits cleanly after receiving.
    Alive { recovered_count: usize },

    /// Either side abandoning the handoff. Recipient cleans up and
    /// returns to its prior state (predecessor stays SERVING; successor
    /// exits).
    Abort(AbortReason),

    /// Strategy-specific message. Reserved for future PIOD per-tag
    /// handoff messages; current CRH strategy ignores these.
    StrategyMsg {
        /// Tag indicating which strategy authored this message; the
        /// active `CutoverStrategy` dispatches on it.
        strategy: String,
        /// Opaque payload, strategy-deserialized.
        payload: Vec<u8>,
    },
}

/// Recommended timeouts for each protocol stage. Tuned generously so
/// transient pauses (S3 slow path, kernel ioctl jitter) don't trip
/// abort. Overridable via CLI for tests.
#[derive(Debug, Clone, Copy)]
pub struct HandoffTimeouts {
    /// Max time for successor's WARMING phase (foyer open, S3 prefetch,
    /// router build).
    pub warming: Duration,
    /// Max time predecessor waits for successor's Ready after HelloAck.
    pub ready_wait: Duration,
    /// Max time successor waits for Cutover after Ready.
    pub cutover_wait: Duration,
    /// Max time predecessor waits for Alive after PredsDead. If
    /// exceeded, predecessor revives via its own
    /// `recover_quiesced_devices()`.
    pub alive_wait: Duration,
}

impl Default for HandoffTimeouts {
    fn default() -> Self {
        Self {
            warming: Duration::from_secs(120),
            ready_wait: Duration::from_secs(180),
            cutover_wait: Duration::from_secs(30),
            alive_wait: Duration::from_secs(60),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_messages() {
        let messages = vec![
            HandoffMessage::Hello {
                protocol_version: 1,
                capabilities: Capabilities::current(),
                successor_pid: 12345,
            },
            HandoffMessage::HelloAck {
                protocol_version: 1,
                capabilities: Capabilities::current(),
                strategy: "crh".to_string(),
                exports: vec![ExportSnapshot {
                    name: "test".to_string(),
                    size_bytes: 1 << 30,
                    readonly: false,
                    last_wal_seq: 42,
                    ublk_dev_id: Some(5),
                }],
                predecessor_pid: 12344,
            },
            HandoffMessage::Ready,
            HandoffMessage::Cutover,
            HandoffMessage::PredsDead,
            HandoffMessage::Alive { recovered_count: 1 },
            HandoffMessage::Abort(AbortReason::NoCommonStrategy),
            HandoffMessage::StrategyMsg {
                strategy: "piod".to_string(),
                payload: vec![1, 2, 3, 4],
            },
        ];

        for msg in messages {
            let bytes = bincode::serialize(&msg).unwrap();
            let decoded: HandoffMessage = bincode::deserialize(&bytes).unwrap();
            // Round-trip via debug equality (HandoffMessage isn't PartialEq
            // because of the byte payload).
            assert_eq!(format!("{msg:?}"), format!("{decoded:?}"));
        }
    }

    #[test]
    fn capabilities_intersect() {
        let a = Capabilities { crh: true, piod: true };
        let b = Capabilities { crh: true, piod: false };
        let r = a.intersect(b);
        assert!(r.crh);
        assert!(!r.piod);
    }
}
