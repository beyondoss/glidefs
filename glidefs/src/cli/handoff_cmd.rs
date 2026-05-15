//! `glidefs handoff` CLI — trigger a graceful handoff against a running daemon.
//!
//! Connects to the daemon's handoff *control* socket (NOT the handoff
//! data socket — control is a small Unix socket the daemon listens on
//! for trigger requests, distinct from the SEQPACKET handoff socket the
//! predecessor binds when actually performing a handoff). Sends a
//! one-byte request, waits for ack, exits.
//!
//! This is the most operator-friendly entry point — it's equivalent to
//! `kill -HUP $(pidof glidefs)` but doesn't require knowing the PID.

use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// Wire protocol for the control socket. One byte each way.
const CONTROL_REQUEST_HANDOFF: u8 = b'H';
const CONTROL_REQUEST_HANDOFF_DRY_RUN: u8 = b'D';
const CONTROL_RESPONSE_ACCEPTED: u8 = b'A';
const CONTROL_RESPONSE_BUSY: u8 = b'B';
const CONTROL_RESPONSE_UNSUPPORTED: u8 = b'U';

pub async fn run(socket: PathBuf, dry_run: bool) -> Result<()> {
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
    stream.flush().await.ok();

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
            println!("handoff accepted; watch daemon logs for progress");
            Ok(())
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

/// Constants re-exported for the server-side socket handler.
pub mod wire {
    pub const REQUEST_HANDOFF: u8 = super::CONTROL_REQUEST_HANDOFF;
    pub const REQUEST_HANDOFF_DRY_RUN: u8 = super::CONTROL_REQUEST_HANDOFF_DRY_RUN;
    pub const RESPONSE_ACCEPTED: u8 = super::CONTROL_RESPONSE_ACCEPTED;
    pub const RESPONSE_BUSY: u8 = super::CONTROL_RESPONSE_BUSY;
    pub const RESPONSE_UNSUPPORTED: u8 = super::CONTROL_RESPONSE_UNSUPPORTED;
}
