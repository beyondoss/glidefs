//! Successor-side state machine for graceful daemon handoff.
//!
//! The successor is launched by the predecessor via `fork+exec` with
//! `--handoff-from <socket>`. It:
//!
//! 1. Connects to the handoff socket.
//! 2. Sends HELLO with version + capabilities + its PID.
//! 3. Receives HELLO_ACK with the predecessor's export snapshot.
//! 4. Performs WARMING: loads config, opens foyer, replays WAL, builds
//!    ExportRouter, prefetches S3 chunk metadata. All slow startup work.
//! 5. Sends READY.
//! 6. Receives CUTOVER, then PREDS_DEAD.
//! 7. Runs strategy.successor_takeover (CRH: recover_devices_by_id).
//! 8. Sends ALIVE.
//! 9. Continues SERVING — same code path as a cold-start daemon from
//!    here on, just without the kernel device cold scan.

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixDatagram;

use crate::handoff::HandoffCoordinator;
use crate::handoff::protocol::{
    AbortReason, Capabilities, HandoffMessage, HandoffTimeouts, PROTOCOL_VERSION,
};
use crate::handoff::strategy::{self, SuccessorTakeoverCtx};

const RECV_BUF_BYTES: usize = 64 * 1024;

/// Outcome of the takeover. The successor continues SERVING after this
/// returns Ok — the calling context is responsible for entering the
/// normal serve loop with the returned coordinator (and its router).
pub struct TakeoverResult {
    pub coord: Arc<HandoffCoordinator>,
    pub recovered_count: usize,
    /// Listener fds inherited from the predecessor via SCM_RIGHTS in
    /// the HelloAck payload (NBD TCP/Unix, HTTP API). The successor's
    /// `serve_with_router_with_inherited_fds` consumes these instead
    /// of binding fresh sockets — preserving in-flight client TCP
    /// connections across the cutover.
    pub inherited_fds: crate::handoff::listener_registry::InheritedFds,
}

/// Run the successor state machine.
///
/// The caller (`cli::server::run_server_as_successor`) is expected to have
/// already done all the slow cold-start work: built `router`, opened
/// foyer caches, replayed WALs, discovered exports. This function just
/// drives the handoff protocol and triggers the kernel-level takeover.
///
/// After this returns successfully, the caller enters its normal
/// serving loop (bind listeners, start scrubber/capacity-monitor, signal
/// handler, etc.).
#[tracing::instrument(
    name = "handoff.successor",
    skip_all,
    fields(socket = %socket_path.display()),
)]
pub async fn run_successor(
    socket_path: &Path,
    coord: Arc<HandoffCoordinator>,
    timeouts: HandoffTimeouts,
) -> Result<TakeoverResult> {
    let sock = connect_seqpacket(socket_path)
        .context("connecting to handoff socket")?;

    // Send HELLO.
    let hello = HandoffMessage::Hello {
        protocol_version: PROTOCOL_VERSION,
        capabilities: Capabilities::current(),
        successor_pid: std::process::id() as i32,
        dry_run: dry_run_arg(),
    };
    send_one(&sock, &hello).await.context("send HELLO")?;

    // Receive HELLO_ACK with optional ancillary listener fds.
    let (ack_msg, ack_fds) = tokio::time::timeout(timeouts.ready_wait, recv_one_with_fds(&sock))
        .await
        .map_err(|_| anyhow!("timeout waiting for HELLO_ACK"))?
        .context("recv HELLO_ACK")?;

    let (negotiated_strategy_name, exports, predecessor_pid, listener_kinds) = match ack_msg {
        HandoffMessage::HelloAck { strategy, exports, predecessor_pid, capabilities, protocol_version, listener_kinds } => {
            if protocol_version != PROTOCOL_VERSION {
                return Err(anyhow!(
                    "protocol version mismatch: ours={PROTOCOL_VERSION} theirs={protocol_version}"
                ));
            }
            tracing::info!(?capabilities, "handoff: negotiated capabilities");
            (strategy, exports, predecessor_pid, listener_kinds)
        }
        HandoffMessage::Abort(r) => {
            return Err(anyhow!("predecessor aborted: {r:?}"));
        }
        other => return Err(anyhow!("expected HELLO_ACK, got {other:?}")),
    };

    let inherited_fds = if !listener_kinds.is_empty() {
        if listener_kinds.len() != ack_fds.len() {
            tracing::warn!(
                kinds = listener_kinds.len(),
                fds = ack_fds.len(),
                "handoff: HELLO_ACK kinds/fds count mismatch — dropping inherited fds, will rebind"
            );
            crate::handoff::listener_registry::InheritedFds::default()
        } else {
            tracing::info!(
                count = listener_kinds.len(),
                "handoff: inherited listener fds via SCM_RIGHTS"
            );
            crate::handoff::listener_registry::InheritedFds::from_kinds_and_fds(listener_kinds, ack_fds)
        }
    } else {
        crate::handoff::listener_registry::InheritedFds::default()
    };

    tracing::info!(
        predecessor_pid,
        strategy = %negotiated_strategy_name,
        export_count = exports.len(),
        "handoff: HELLO_ACK received"
    );

    // Verify our (already-warmed) router has every export the predecessor
    // listed. The caller is responsible for having done all slow startup
    // work before invoking us; we just verify the result.
    let _ = timeouts.warming; // currently unused — kept for future async warm path
    let missing = missing_exports(coord.router(), &exports).await;
    if !missing.is_empty() {
        send_one(
            &sock,
            &HandoffMessage::Abort(AbortReason::ExportMismatch { missing: missing.clone() }),
        )
        .await
        .ok();
        return Err(anyhow!("successor router missing exports: {missing:?}"));
    }

    #[cfg(feature = "test-fault-injection")]
    crate::handoff::fault::inject("s_crash_after_warming");

    // Send READY.
    send_one(&sock, &HandoffMessage::Ready).await.context("send READY")?;
    tracing::info!("handoff: READY sent, awaiting CUTOVER");

    #[cfg(feature = "test-fault-injection")]
    crate::handoff::fault::inject("s_crash_after_ready");

    // Wait for CUTOVER.
    let msg = tokio::time::timeout(timeouts.cutover_wait, recv_one(&sock))
        .await
        .map_err(|_| anyhow!("timeout waiting for CUTOVER"))?
        .context("recv CUTOVER")?;
    match msg {
        HandoffMessage::Cutover => {}
        HandoffMessage::Abort(r) => return Err(anyhow!("predecessor aborted: {r:?}")),
        other => return Err(anyhow!("expected CUTOVER, got {other:?}")),
    }

    // Wait for PREDS_DEAD.
    let msg = tokio::time::timeout(timeouts.cutover_wait, recv_one(&sock))
        .await
        .map_err(|_| anyhow!("timeout waiting for PREDS_DEAD"))?
        .context("recv PREDS_DEAD")?;
    match msg {
        HandoffMessage::PredsDead => {}
        HandoffMessage::Abort(r) => return Err(anyhow!("predecessor aborted: {r:?}")),
        other => return Err(anyhow!("expected PREDS_DEAD, got {other:?}")),
    }

    tracing::info!("handoff: PREDS_DEAD received, running takeover");

    #[cfg(feature = "test-fault-injection")]
    crate::handoff::fault::inject("s_crash_after_cutover");

    // Run strategy-specific takeover.
    let per_io_daemon = coord.is_per_io_daemon_supported();
    let strategy = strategy::select(per_io_daemon);
    if strategy.name() != negotiated_strategy_name {
        return Err(anyhow!(
            "negotiated strategy '{negotiated_strategy_name}' but local select() returned '{}'",
            strategy.name()
        ));
    }

    let mut takeover_ctx = SuccessorTakeoverCtx {
        coord: coord.clone(),
        exports,
    };
    let recovered_count = strategy
        .successor_takeover(&mut takeover_ctx)
        .await
        .context("successor_takeover")?;

    // Send ALIVE.
    send_one(&sock, &HandoffMessage::Alive { recovered_count })
        .await
        .context("send ALIVE")?;

    tracing::info!(
        recovered_count,
        "handoff: ALIVE sent — successor is now SERVING"
    );

    Ok(TakeoverResult {
        coord,
        recovered_count,
        inherited_fds,
    })
}

async fn missing_exports(
    router: &crate::block::router::ExportRouter,
    snapshot: &[crate::handoff::protocol::ExportSnapshot],
) -> Vec<String> {
    let mut missing = Vec::new();
    for snap in snapshot {
        if router.get_handler(&snap.name).await.is_none() {
            missing.push(snap.name.clone());
        }
    }
    missing
}

fn connect_seqpacket(path: &Path) -> std::io::Result<UnixDatagram> {
    use std::os::fd::FromRawFd;

    let fd = unsafe {
        libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_NONBLOCK, 0)
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }

    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as u16;
    let path_bytes = path.as_os_str().as_encoded_bytes();
    if path_bytes.len() >= addr.sun_path.len() {
        unsafe { libc::close(fd) };
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "socket path too long",
        ));
    }
    for (i, b) in path_bytes.iter().enumerate() {
        addr.sun_path[i] = *b as libc::c_char;
    }

    let connect_ret = unsafe {
        libc::connect(
            fd,
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_un>() as u32,
        )
    };
    if connect_ret < 0 {
        // EINPROGRESS is OK for nonblocking SEQPACKET; the kernel
        // completes the connect synchronously for AF_UNIX in practice.
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINPROGRESS) {
            unsafe { libc::close(fd) };
            return Err(err);
        }
    }

    let std_sock = unsafe { std::os::unix::net::UnixDatagram::from_raw_fd(fd) };
    UnixDatagram::from_std(std_sock)
}

/// Like `recv_one` but also collects any `SCM_RIGHTS` fds carried as
/// ancillary data. Used for `HelloAck` so the successor can pick up
/// the predecessor's listener fds.
async fn recv_one_with_fds(
    sock: &UnixDatagram,
) -> std::io::Result<(HandoffMessage, Vec<std::os::fd::OwnedFd>)> {
    sock.readable().await?;
    let mut buf = vec![0u8; RECV_BUF_BYTES];
    let (n, fds) = crate::handoff::fdpass::recvmsg_with_fds(sock, &mut buf)?;
    let msg = bincode::deserialize(&buf[..n])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok((msg, fds))
}

async fn recv_one(sock: &UnixDatagram) -> std::io::Result<HandoffMessage> {
    let mut buf = vec![0u8; RECV_BUF_BYTES];
    let n = sock.recv(&mut buf).await?;
    bincode::deserialize(&buf[..n])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

async fn send_one(sock: &UnixDatagram, msg: &HandoffMessage) -> std::io::Result<()> {
    let bytes = bincode::serialize(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    sock.send(&bytes).await?;
    Ok(())
}

/// Sniff `--handoff-from <path>` from argv without invoking clap. Returns
/// `None` for normal run modes; `Some(socket)` when the predecessor
/// spawned us as a successor.
#[allow(dead_code)]
pub fn handoff_from_arg() -> Option<PathBuf> {
    let mut args = std::env::args();
    while let Some(a) = args.next() {
        if a == "--handoff-from" {
            return args.next().map(PathBuf::from);
        }
    }
    None
}

/// Sniff `-c <path>` or `--config <path>` from argv. Used by the
/// successor entry to discover the config path the predecessor passed
/// along, without invoking clap (which would reject the unknown
/// `--handoff-from` flag).
#[allow(dead_code)]
pub fn config_arg() -> Option<PathBuf> {
    let mut args = std::env::args();
    while let Some(a) = args.next() {
        if a == "-c" || a == "--config" {
            return args.next().map(PathBuf::from);
        }
    }
    None
}

/// Sniff `--dry-run` from argv. The predecessor passes this when the
/// operator runs `glidefs handoff --dry-run` — successor performs
/// WARMING then aborts cleanly.
#[allow(dead_code)]
pub fn dry_run_arg() -> bool {
    std::env::args().any(|a| a == "--dry-run")
}
