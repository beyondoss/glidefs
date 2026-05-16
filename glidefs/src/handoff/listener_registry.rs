//! Registry of currently-bound listener fds, used for SCM_RIGHTS
//! handoff so the successor can resume accepting on the predecessor's
//! sockets without dropping any in-flight TCP/Unix client connections.
//!
//! Each `start_*_server` task in `cli/server.rs` registers its listener
//! fd here right after `bind`; the predecessor's handoff code reads
//! the registry, `dup(2)`s every fd (so the originals stay alive for
//! the still-running listeners), and ships the duplicates as an
//! ancillary `SCM_RIGHTS` payload alongside the `HelloAck` message.
//!
//! On the receiving side (successor), `cli/server::run_server_as_successor`
//! pulls those fds out of the handoff socket's cmsg buffer, wraps them
//! into `std::net::TcpListener` / `std::os::unix::net::UnixListener`
//! via `FromRawFd`, and hands them to the new `NBDServer` / `ApiServer`
//! constructors instead of letting them call `bind` afresh. The kernel
//! socket structure is preserved, so existing clients see no
//! disconnection.

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// Kind of listener — used as the registry key. Each kind appears at
/// most once because each server type binds at most one socket per
/// address per daemon.
#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ListenerKind {
    /// NBD TCP listener, keyed by the bound address (`addr:port`).
    NbdTcp(String),
    /// NBD Unix socket listener.
    NbdUnix,
    /// HTTP API listener.
    HttpApi,
}

/// Shared registry of bound listener fds. Cloning is cheap (Arc).
#[derive(Debug, Clone, Default)]
pub struct ListenerRegistry {
    inner: Arc<Mutex<HashMap<ListenerKind, RawFd>>>,
}

impl ListenerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a listener fd. The fd must remain owned by the caller
    /// (the originating server task); the registry only keeps the
    /// integer for later `dup(2)`. Overwrites any prior entry under
    /// the same key.
    pub fn register(&self, kind: ListenerKind, fd: RawFd) {
        self.inner.lock().insert(kind, fd);
    }

    /// Snapshot the registry into a vec of `(kind, fd)` pairs. Used
    /// by the predecessor at handoff time to enumerate what to pass
    /// through to the successor.
    pub fn snapshot(&self) -> Vec<(ListenerKind, RawFd)> {
        self.inner
            .lock()
            .iter()
            .map(|(k, &v)| (k.clone(), v))
            .collect()
    }

    /// Number of registered listeners.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// True if no listeners are registered yet.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

/// Inherited listener fds, indexed by kind. Built on the successor
/// side from the SCM_RIGHTS payload.
///
/// Wraps `OwnedFd` so the kernel closes the fd if it goes unused;
/// that's the safe failure mode (server falls back to `bind`).
#[derive(Debug, Default)]
pub struct InheritedFds {
    map: HashMap<ListenerKind, std::os::fd::OwnedFd>,
}

impl InheritedFds {
    /// Build from a tag list + fd list (must be same length). Drops
    /// any extra fds on the floor so they're closed by `OwnedFd::Drop`.
    pub fn from_kinds_and_fds(
        kinds: Vec<ListenerKind>,
        fds: Vec<std::os::fd::OwnedFd>,
    ) -> Self {
        let mut map = HashMap::new();
        for (k, fd) in kinds.into_iter().zip(fds.into_iter()) {
            map.insert(k, fd);
        }
        Self { map }
    }

    /// Take the fd for a given kind (transferring ownership to the
    /// caller). Subsequent calls for the same kind return `None`.
    pub fn take(&mut self, kind: &ListenerKind) -> Option<std::os::fd::OwnedFd> {
        self.map.remove(kind)
    }

    /// Number of inherited fds still un-claimed.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
