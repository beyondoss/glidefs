//! `glidefs handoff` CLI — trigger a graceful handoff against a running daemon.
//!
//! Connects to the daemon's handoff *control* socket (NOT the handoff
//! data socket — control is a small Unix socket the daemon listens on
//! for trigger requests, distinct from the SEQPACKET handoff socket the
//! predecessor binds when actually performing a handoff). Sends a
//! one-byte request, waits for ack, then waits for the handoff's outcome and
//! exits 0 only if it (or the dry run) succeeded. An abort exits non-zero with
//! the reason on stderr.
//!
//! This is the most operator-friendly entry point — it's equivalent to
//! `kill -HUP $(pidof glidefs)` but doesn't require knowing the PID.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

// Wire protocol bytes are defined in `crate::handoff::protocol::ctl_wire`
// so they can be shared by the CLI, the HTTP API endpoint, and the
// daemon's control-socket listener.
use crate::handoff::protocol::ctl_wire::{
    REQUEST_HANDOFF as CONTROL_REQUEST_HANDOFF,
    REQUEST_HANDOFF_DRY_RUN as CONTROL_REQUEST_HANDOFF_DRY_RUN,
    RESPONSE_ACCEPTED as CONTROL_RESPONSE_ACCEPTED,
    RESPONSE_FAILED as CONTROL_RESPONSE_FAILED,
    RESPONSE_OK as CONTROL_RESPONSE_OK,
    RESPONSE_BUSY as CONTROL_RESPONSE_BUSY,
    RESPONSE_UNSUPPORTED as CONTROL_RESPONSE_UNSUPPORTED,
};

pub async fn run(socket: PathBuf, dry_run: bool, timeout: Duration) -> Result<()> {
    let mut stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("connecting to handoff control socket {}", socket.display()))?;

    let req = if dry_run {
        CONTROL_REQUEST_HANDOFF_DRY_RUN
    } else {
        CONTROL_REQUEST_HANDOFF
    };
    stream
        .write_all(&[req])
        .await
        .context("sending handoff request")?;
    stream.flush().await.context("flushing handoff request")?;

    let mut response = [0u8; 1];
    let n = stream
        .read(&mut response)
        .await
        .context("reading handoff response")?;
    if n == 0 {
        anyhow::bail!("handoff control socket closed without response");
    }

    match response[0] {
        CONTROL_RESPONSE_ACCEPTED => {
            println!("handoff accepted; waiting up to {}s for the outcome", timeout.as_secs());
            wait_for_outcome(&mut stream, dry_run, timeout).await
        }
        CONTROL_RESPONSE_BUSY => {
            anyhow::bail!("daemon reports a handoff is already in progress");
        }
        CONTROL_RESPONSE_UNSUPPORTED => {
            anyhow::bail!(
                "daemon does not support handoff (built without handoff feature, or older version)"
            );
        }
        other => {
            anyhow::bail!("unexpected response byte from daemon: 0x{:02x}", other);
        }
    }
}

/// Re-export of the canonical wire constants for the server-side
/// control-socket handler. New code should reference
/// `crate::handoff::protocol::ctl_wire` directly.
pub mod wire {
    pub use crate::handoff::protocol::ctl_wire::*;
}

/// Read the daemon's verdict. Every way this can fail to produce a clear "ok"
/// is an error: the caller (a deploy gate) must not proceed on a guess.
async fn wait_for_outcome(stream: &mut UnixStream, dry_run: bool, timeout: Duration) -> Result<()> {
    let what = if dry_run { "handoff dry run" } else { "handoff" };
    let mut verdict = Vec::new();
    match tokio::time::timeout(timeout, stream.read_to_end(&mut verdict)).await {
        Err(_) => anyhow::bail!(
            "{what}: no outcome within {}s; the daemon may still be working, check its journal",
            timeout.as_secs()
        ),
        Ok(Err(e)) => anyhow::bail!("{what}: reading the outcome failed: {e}"),
        Ok(Ok(_)) => {}
    }
    match verdict.split_first() {
        Some((&CONTROL_RESPONSE_OK, _)) => {
            println!("{what}: ok");
            Ok(())
        }
        Some((&CONTROL_RESPONSE_FAILED, reason)) => {
            anyhow::bail!("{what} aborted: {}", String::from_utf8_lossy(reason))
        }
        _ => anyhow::bail!(
            "{what}: the daemon closed the control socket without an outcome \
             (a daemon older than this binary does not report one; check its journal)"
        ),
    }
}
