//! Predecessor-side state machine for graceful daemon handoff.
//!
//! The predecessor is the *currently serving* daemon. When a handoff is
//! triggered (SIGHUP or `glidefs handoff` CLI), it:
//!
//! 1. fork+execs a successor process with `--handoff-from <socket>`.
//! 2. Listens on the handoff socket for the successor's HELLO.
//! 3. Responds with HELLO_ACK carrying its export snapshot.
//! 4. Waits for READY (successor finished WARMING).
//! 5. Calls `freeze_all()` on its router (gates new writes, fsyncs WALs).
//! 6. Sends CUTOVER, runs strategy.predecessor_cutover (CRH: drops UblkServer).
//! 7. Sends PREDS_DEAD.
//! 8. Waits for ALIVE (successor finished takeover).
//! 9. Drops everything and exits.
//!
//! **Revival invariant**: if the successor crashes between PREDS_DEAD and
//! ALIVE, the predecessor recovers its own QUIESCED devices via
//! `recover_quiesced_devices()` and resumes serving. The ExportRouter is
//! kept alive across the cutover specifically for this case.

use anyhow::{anyhow, Context, Result};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UnixDatagram;

use crate::block::router::ExportRouter;
use crate::handoff::protocol::{
    AbortReason, Capabilities, HandoffMessage, HandoffTimeouts, PROTOCOL_VERSION,
};
use crate::handoff::strategy::{self, CutoverStrategy, PredecessorCutoverCtx};

/// Buffer size for reading bincode-serialized HandoffMessages. Empirically
/// the largest message (HelloAck with N exports) is ~200 bytes per export
/// + ~50 bytes overhead. 64 KiB handles 1000 exports comfortably.
const RECV_BUF_BYTES: usize = 64 * 1024;

/// Outcome of a handoff attempt from the predecessor's perspective.
#[derive(Debug)]
pub enum HandoffOutcome {
    /// Successor took over; predecessor is exiting.
    Succeeded { recovered_count: usize, duration: std::time::Duration },
    /// Predecessor aborted before any destructive action; still SERVING.
    Aborted { reason: String, duration: std::time::Duration },
    /// Successor crashed after CUTOVER; predecessor revived. Still SERVING.
    RevivedFromFailedHandoff { duration: std::time::Duration },
}

/// Run the predecessor state machine.
///
/// `router` is shared with the rest of the daemon (NBD/HTTP/API listeners).
/// `config_path` is forwarded to the spawned successor via `--config` so
/// it can re-read the same configuration during WARMING. The handoff
/// socket is bound here and removed on exit.
#[tracing::instrument(
    name = "handoff.predecessor",
    skip_all,
    fields(
        socket = %socket_path.display(),
        binary = %successor_binary.display(),
        config = %config_path.display(),
    ),
)]
pub async fn run_predecessor(
    router: Arc<ExportRouter>,
    socket_path: &Path,
    successor_binary: &Path,
    config_path: &Path,
    timeouts: HandoffTimeouts,
    dry_run: bool,
) -> Result<HandoffOutcome> {
    let started = Instant::now();
    let _ = std::fs::remove_file(socket_path); // tolerate stale

    // Bind SEQPACKET listener. Tokio doesn't expose SEQPACKET directly
    // for UnixDatagram — we use the raw socket(2) and adopt it.
    let listener = bind_seqpacket(socket_path)
        .context("failed to bind handoff socket")?;

    tracing::info!(socket = %socket_path.display(), "handoff: socket bound, spawning successor");

    // **CRITICAL**: pause checkpoints + flushes on every cache from
    // the moment handoff starts, not just from `freeze_all` later.
    // The successor's WriteCache::open during WARMING reads the WAL
    // at that moment; if the predecessor's flush_scheduler fires a
    // checkpoint between then and PREDS_DEAD, the truncate drops
    // entries the successor's `replay_wal_tail` needs to pick up,
    // causing silent data loss observable as "verify: bad magic
    // header 0" in fio. Setting the flag this early covers the
    // entire WARMING + READY-wait + freeze + cutover window.
    router.set_all_caches_freeze(true).await;

    // Detect kernel features for strategy selection.
    let per_io_daemon = router.is_per_io_daemon_supported();
    let strategy = strategy::select(per_io_daemon);
    let strategy_name = strategy.name();
    tracing::info!(strategy = strategy_name, "handoff: selected cutover strategy");

    // Spawn the successor.
    let successor = spawn_successor(successor_binary, socket_path, config_path, dry_run)?;
    tracing::info!(pid = successor.id().unwrap_or(0), dry_run, "handoff: successor spawned");

    // Accept the successor's connection. SEQPACKET is connection-
    // oriented like SOCK_STREAM — recv on the listener fd directly
    // returns ENOTCONN. Accept gives us the actual connected socket.
    let connected = match tokio::time::timeout(
        timeouts.warming + timeouts.ready_wait,
        accept_seqpacket(&listener),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            let _ = std::fs::remove_file(socket_path);
            drop_successor(successor).await;
            return Err(anyhow!("handoff: accept failed: {e}"));
        }
        Err(_) => {
            let _ = std::fs::remove_file(socket_path);
            drop_successor(successor).await;
            return Ok(HandoffOutcome::Aborted {
                reason: "timeout waiting for successor to connect".into(),
                duration: started.elapsed(),
            });
        }
    };

    // Run the protocol; capture all errors so we can clean up.
    let result = run_protocol(
        &connected,
        router.clone(),
        &*strategy,
        timeouts,
        started,
    )
    .await;

    let _ = std::fs::remove_file(socket_path);

    // Reap the successor regardless of outcome — it's our child.
    drop_successor(successor).await;

    // If we're staying alive (Aborted, RevivedFromFailedHandoff), or
    // even if we're exiting on Succeeded, clear the freeze flag so
    // any cleanup paths in the existing flush_scheduler can run.
    // For Succeeded, the predecessor exits anyway so the flag is
    // moot. For Aborted/Revived, the predecessor resumes normal
    // serving and needs flush_scheduler back to truncate WAL.
    router.set_all_caches_freeze(false).await;

    result
}

async fn run_protocol(
    sock: &UnixDatagram,
    router: Arc<ExportRouter>,
    strategy: &dyn CutoverStrategy,
    timeouts: HandoffTimeouts,
    started: Instant,
) -> Result<HandoffOutcome> {
    // Wait for HELLO from successor over the accepted connection.
    let msg = match tokio::time::timeout(
        timeouts.warming + timeouts.ready_wait,
        recv_one(sock),
    )
    .await
    {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => return Err(anyhow!("handoff: failed to receive HELLO: {e}")),
        Err(_) => return Ok(HandoffOutcome::Aborted {
            reason: "timeout waiting for successor HELLO".into(),
            duration: started.elapsed(),
        }),
    };

    let HandoffMessage::Hello { protocol_version, capabilities, successor_pid, dry_run } = msg else {
        return Ok(HandoffOutcome::Aborted {
            reason: format!("expected HELLO, got {msg:?}"),
            duration: started.elapsed(),
        });
    };

    tracing::info!(
        successor_pid,
        protocol_version,
        ?capabilities,
        "handoff: received HELLO"
    );

    // Verify version + capabilities.
    if protocol_version != PROTOCOL_VERSION {
        let reason = AbortReason::VersionMismatch {
            ours: PROTOCOL_VERSION,
            theirs: protocol_version,
        };
        send_one(sock, &HandoffMessage::Abort(reason.clone())).await.ok();
        return Ok(HandoffOutcome::Aborted {
            reason: format!("{:?}", reason),
            duration: started.elapsed(),
        });
    }

    let negotiated = capabilities.intersect(Capabilities::current());
    let strategy_name = strategy.name();
    if !negotiated.crh && strategy_name == "crh" {
        send_one(sock, &HandoffMessage::Abort(AbortReason::NoCommonStrategy)).await.ok();
        return Ok(HandoffOutcome::Aborted {
            reason: "no common cutover strategy".into(),
            duration: started.elapsed(),
        });
    }

    // Build snapshot of our exports.
    let exports = router.handoff_snapshot().await;
    tracing::info!(count = exports.len(), "handoff: built export snapshot");

    let predecessor_pid = std::process::id() as i32;

    // Snapshot listener fds + dup so the originals stay live for our
    // running NBD/HTTP API tasks. The dup'd copies travel via
    // SCM_RIGHTS and are owned by the kernel until the successor
    // claims them via `recvmsg`.
    let listener_snapshot = router.listener_registry.snapshot();
    let mut dup_fds: Vec<std::os::fd::OwnedFd> = Vec::with_capacity(listener_snapshot.len());
    let mut listener_kinds = Vec::with_capacity(listener_snapshot.len());
    for (kind, fd) in &listener_snapshot {
        use std::os::fd::FromRawFd;
        // SAFETY: fd is a valid open listener fd registered by an
        // active NBD/HTTP server task; dup3 with O_CLOEXEC keeps the
        // duplicate from leaking into any future fork+exec on our
        // side, while still being valid to send via SCM_RIGHTS.
        let dup = unsafe {
            let dup = libc::fcntl(*fd, libc::F_DUPFD_CLOEXEC, 0);
            if dup < 0 {
                tracing::warn!(
                    error = %std::io::Error::last_os_error(),
                    ?kind,
                    "handoff: dup of listener fd failed; skipping inherit for this listener"
                );
                continue;
            }
            std::os::fd::OwnedFd::from_raw_fd(dup)
        };
        dup_fds.push(dup);
        listener_kinds.push(kind.clone());
    }
    if !listener_kinds.is_empty() {
        tracing::info!(
            count = listener_kinds.len(),
            "handoff: shipping listener fds via SCM_RIGHTS"
        );
    }

    let ack = HandoffMessage::HelloAck {
        protocol_version: PROTOCOL_VERSION,
        capabilities: negotiated,
        strategy: strategy_name.to_string(),
        exports,
        predecessor_pid,
        listener_kinds,
    };
    send_one_with_fds(sock, &ack, &dup_fds)
        .await
        .context("send HELLO_ACK")?;
    drop(dup_fds);

    #[cfg(feature = "test-fault-injection")]
    crate::handoff::fault::inject("p_crash_after_hello_ack");

    // Wait for READY.
    let msg = match tokio::time::timeout(timeouts.ready_wait, recv_one(sock)).await {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => return Err(anyhow!("recv READY: {e}")),
        Err(_) => return Ok(HandoffOutcome::Aborted {
            reason: "timeout waiting for READY".into(),
            duration: started.elapsed(),
        }),
    };
    match msg {
        HandoffMessage::Ready => {
            // Dry-run successful: successor proved WARMING works.
            // Send Abort(Other("dry-run-complete")), do NOT initiate
            // cutover — predecessor stays SERVING, successor exits.
            if dry_run {
                tracing::info!("handoff: dry-run mode — WARMING succeeded, aborting before cutover");
                send_one(
                    sock,
                    &HandoffMessage::Abort(AbortReason::Other(
                        "dry-run-complete".to_string(),
                    )),
                )
                .await
                .ok();
                return Ok(HandoffOutcome::Aborted {
                    reason: "dry-run completed successfully".into(),
                    duration: started.elapsed(),
                });
            }
        }
        HandoffMessage::Abort(r) => {
            return Ok(HandoffOutcome::Aborted {
                reason: format!("successor aborted: {r:?}"),
                duration: started.elapsed(),
            });
        }
        other => {
            return Ok(HandoffOutcome::Aborted {
                reason: format!("expected READY, got {other:?}"),
                duration: started.elapsed(),
            });
        }
    }

    tracing::info!("handoff: successor READY; freezing handlers");

    // FREEZE: block new writes, fsync WALs.
    if let Err(e) = router.freeze_all().await {
        // On freeze failure we are still SERVING; abort the handoff.
        let reason = AbortReason::FreezeFailed { detail: format!("{e:#}") };
        send_one(sock, &HandoffMessage::Abort(reason.clone())).await.ok();
        // Unfreeze so we can keep serving.
        router.unfreeze_all().await;
        return Ok(HandoffOutcome::Aborted {
            reason: format!("{:?}", reason),
            duration: started.elapsed(),
        });
    }

    #[cfg(feature = "test-fault-injection")]
    crate::handoff::fault::inject("p_crash_during_freeze");

    // CUTOVER: tell successor we're about to drop, then drop.
    send_one(sock, &HandoffMessage::Cutover).await.context("send CUTOVER")?;
    tracing::info!("handoff: CUTOVER sent; running predecessor cutover step");

    let mut cutover_ctx = PredecessorCutoverCtx { router: router.clone() };
    if let Err(e) = strategy.predecessor_cutover(&mut cutover_ctx).await {
        // The cutover failed. We may already have dropped the UblkServer
        // (kernel devices QUIESCED). Try to revive.
        tracing::error!(error = %e, "handoff: predecessor cutover failed; attempting revival");
        if let Err(rev) = router.revive_after_failed_handoff().await {
            return Err(anyhow!("cutover failed AND revival failed: cutover={e:#} revival={rev:#}"));
        }
        router.unfreeze_all().await;
        return Ok(HandoffOutcome::RevivedFromFailedHandoff {
            duration: started.elapsed(),
        });
    }

    #[cfg(feature = "test-fault-injection")]
    crate::handoff::fault::inject("p_crash_after_cutover");

    send_one(sock, &HandoffMessage::PredsDead).await.context("send PREDS_DEAD")?;
    tracing::info!("handoff: PREDS_DEAD sent; awaiting ALIVE");

    // Wait for ALIVE — successor finished takeover.
    let msg = match tokio::time::timeout(timeouts.alive_wait, recv_one(sock)).await {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => {
            // Socket EOF / error after PREDS_DEAD — successor likely
            // crashed. Revive.
            tracing::error!(error = %e, "handoff: socket error after PREDS_DEAD; reviving");
            router
                .revive_after_failed_handoff()
                .await
                .context("revival after successor socket-error")?;
            router.unfreeze_all().await;
            return Ok(HandoffOutcome::RevivedFromFailedHandoff {
                duration: started.elapsed(),
            });
        }
        Err(_) => {
            tracing::error!("handoff: timeout waiting for ALIVE; reviving");
            router
                .revive_after_failed_handoff()
                .await
                .context("revival after ALIVE timeout")?;
            router.unfreeze_all().await;
            return Ok(HandoffOutcome::RevivedFromFailedHandoff {
                duration: started.elapsed(),
            });
        }
    };

    let recovered_count = match msg {
        HandoffMessage::Alive { recovered_count } => recovered_count,
        HandoffMessage::Abort(r) => {
            tracing::error!(reason = ?r, "handoff: successor aborted after PREDS_DEAD; reviving");
            router.revive_after_failed_handoff().await?;
            router.unfreeze_all().await;
            return Ok(HandoffOutcome::RevivedFromFailedHandoff {
                duration: started.elapsed(),
            });
        }
        other => {
            tracing::error!(?other, "handoff: unexpected message after PREDS_DEAD; reviving");
            router.revive_after_failed_handoff().await?;
            router.unfreeze_all().await;
            return Ok(HandoffOutcome::RevivedFromFailedHandoff {
                duration: started.elapsed(),
            });
        }
    };

    let duration = started.elapsed();
    let duration_ms = duration.as_millis() as u64;
    tracing::info!(
        recovered_count,
        duration_ms,
        "handoff: SUCCESS — successor serving, predecessor exiting"
    );
    crate::handoff::metrics::record_outcome(
        crate::handoff::metrics::HandoffOutcomeKind::Succeeded,
    );
    crate::handoff::metrics::record_stall_ms(duration_ms);
    Ok(HandoffOutcome::Succeeded {
        recovered_count,
        duration,
    })
}

fn spawn_successor(
    binary: &Path,
    socket_path: &Path,
    config_path: &Path,
    dry_run: bool,
) -> Result<tokio::process::Child> {
    let mut cmd = tokio::process::Command::new(binary);
    cmd.arg("--handoff-from")
        .arg(socket_path)
        .arg("--config")
        .arg(config_path)
        .stdin(std::process::Stdio::null());
    if dry_run {
        cmd.arg("--dry-run");
    }
    // stdout/stderr inherit so successor logs flow to the same place.
    let child = cmd.spawn().context("spawning successor process")?;
    Ok(child)
}

async fn drop_successor(mut child: tokio::process::Child) {
    // We've already exchanged ALIVE (or are aborting). Either way, don't
    // hang on the successor — it has its own life now.
    if let Ok(Some(status)) = child.try_wait() {
        tracing::info!(?status, "successor process already exited");
        return;
    }
    // On Succeeded path, the successor doesn't exit — it keeps serving.
    // Detach (don't wait), but ensure we don't leak zombies via systemd
    // reaping (the parent's exit will reparent to init/systemd).
}

fn bind_seqpacket(path: &Path) -> std::io::Result<UnixDatagram> {
    use std::os::fd::FromRawFd;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_NONBLOCK, 0) };
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

    let bind_ret = unsafe {
        libc::bind(
            fd,
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_un>() as u32,
        )
    };
    if bind_ret < 0 {
        let err = std::io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(err);
    }

    let listen_ret = unsafe { libc::listen(fd, 1) };
    if listen_ret < 0 {
        let err = std::io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(err);
    }

    // Wrap as tokio UnixDatagram. Tokio's UnixDatagram treats this
    // appropriately for SEQPACKET in practice — the API is identical
    // (recv/send rather than recv_from for connected mode).
    let std_sock = unsafe { std::os::unix::net::UnixDatagram::from_raw_fd(fd) };
    UnixDatagram::from_std(std_sock)
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

/// Like `send_one` but attaches `SCM_RIGHTS` ancillary fds. Used for
/// `HelloAck` to ship the predecessor's listener fds to the
/// successor.
async fn send_one_with_fds(
    sock: &UnixDatagram,
    msg: &HandoffMessage,
    fds: &[std::os::fd::OwnedFd],
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let bytes = bincode::serialize(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    sock.writable().await?;
    let raw_fds: Vec<i32> = fds.iter().map(|f| f.as_raw_fd()).collect();
    crate::handoff::fdpass::sendmsg_with_fds(sock, &bytes, &raw_fds)?;
    Ok(())
}

/// accept(2) on a SEQPACKET listener and wrap the connected fd as a
/// tokio UnixDatagram (which on Linux happily drives SEQPACKET).
async fn accept_seqpacket(listener: &UnixDatagram) -> std::io::Result<UnixDatagram> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use tokio::io::Interest;

    // Wait for readable (incoming connection) then accept non-blocking.
    loop {
        listener.ready(Interest::READABLE).await?;
        let listener_fd = listener.as_raw_fd();
        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        let mut addrlen = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
        let connected_fd = unsafe {
            libc::accept4(
                listener_fd,
                &mut addr as *mut _ as *mut libc::sockaddr,
                &mut addrlen,
                libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            )
        };
        if connected_fd < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                // Spurious wakeup — loop and re-await.
                continue;
            }
            return Err(err);
        }
        let std_sock = unsafe {
            std::os::unix::net::UnixDatagram::from_raw_fd(connected_fd)
        };
        return UnixDatagram::from_std(std_sock);
    }
}
