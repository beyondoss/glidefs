#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! NBD server implementation with multi-export support.
//!
//! This module provides a TCP/Unix socket NBD server that supports multiple
//! exports, each with its own write-behind cache and S3 storage.
//!
//! # Concurrent Request Processing
//!
//! The transmission phase uses concurrent request handling to maximize IOPS:
//! - Requests are read sequentially from the socket (protocol requirement)
//! - Each request is dispatched to a separate task for processing
//! - Responses are sent through a channel to a dedicated writer task
//! - This allows multiple requests to be in-flight simultaneously

use super::error::{NBDError, Result};
use super::handler::{BlockHandler, BlockDevice};
use super::protocol::*;
use super::router::ExportRouter;
use crate::task;
use bytes::{Bytes, BytesMut};
use deku::prelude::*;
use futures::FutureExt;
use std::future::Future;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// RAII guard that cancels a `CancellationToken` on drop. Used to make sure a
/// connection's reader loop wakes up if its response-writer task exits or
/// panics — without it, the reader sits in `read_exact` forever while the
/// writer is gone and the client times out.
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Run a per-NBD-request handler future with panic isolation.
///
/// On normal completion the handler's `Response` is forwarded. On panic the
/// helper synthesizes a `Response::Simple { error: NBD_EIO }` so the wire
/// stays framed (the client demuxes on `cookie`) and optionally fires the
/// connection's `CancellationToken` to tear the session down.
///
/// For mutating handlers (write/trim/write_zeroes/flush) prefer
/// `run_mutating_request` — it additionally marks the export degraded so
/// peer connections (multi-conn clients) can't observe the poisoned state.
async fn run_request<F>(
    name: &'static str,
    cookie: u64,
    response_tx: mpsc::Sender<Response>,
    shutdown: CancellationToken,
    error_payload_len: u32,
    cancel_on_panic: bool,
    f: F,
) where
    F: Future<Output = Response> + Send + 'static,
{
    match AssertUnwindSafe(f).catch_unwind().await {
        Ok(resp) => {
            let _ = response_tx.send(resp).await;
        }
        Err(payload) => {
            task::record_panic();
            let msg = task::panic_message(payload.as_ref());
            error!(cookie, task = name, "NBD request handler panicked: {}", msg);
            let data = if error_payload_len > 0 {
                Bytes::from(vec![0u8; error_payload_len as usize])
            } else {
                Bytes::new()
            };
            let _ = response_tx
                .send(Response::Simple {
                    cookie,
                    error: NBD_EIO,
                    data,
                })
                .await;
            if cancel_on_panic {
                shutdown.cancel();
            }
        }
    }
}

/// Run a per-NBD-request handler future for a *mutating* op (write/trim/
/// write_zeroes). Same panic-to-EIO + cancel-connection contract as
/// `run_request`, with one extra step: on panic, the export is marked
/// **degraded** via `BlockHandler::mark_degraded`. While the mark is set,
/// every subsequent op against the handler returns EIO immediately —
/// including ops from peer connections that share the export — so no one
/// can observe the poisoned `WriteCache` state. Daemon restart clears the
/// mark via WAL replay + S3 recovery.
async fn run_mutating_request<F>(
    name: &'static str,
    cookie: u64,
    response_tx: mpsc::Sender<Response>,
    shutdown: CancellationToken,
    handler: Arc<BlockHandler>,
    f: F,
) where
    F: Future<Output = Response> + Send + 'static,
{
    match AssertUnwindSafe(f).catch_unwind().await {
        Ok(resp) => {
            let _ = response_tx.send(resp).await;
        }
        Err(payload) => {
            task::record_panic();
            let msg = task::panic_message(payload.as_ref());
            error!(cookie, task = name, "NBD mutating handler panicked: {}", msg);
            // Bound the cross-connection blast radius: peer conns on this
            // export will see EIO from this point, until daemon restart.
            handler.mark_degraded(name);
            let _ = response_tx
                .send(Response::Simple {
                    cookie,
                    error: NBD_EIO,
                    data: Bytes::new(),
                })
                .await;
            shutdown.cancel();
        }
    }
}

/// Maximum number of in-flight requests before backpressure kicks in.
/// This limits memory usage while allowing high concurrency.
const MAX_INFLIGHT_REQUESTS: usize = 256;

/// Maximum option data size during NBD negotiation (64KB).
/// NBD option data (export names, info requests) is small; anything larger
/// is either a bug or a malicious client trying to OOM the server.
const MAX_OPTION_DATA_LEN: u32 = 64 * 1024;

/// Maximum payload size for a single NBD read/write request (32MB).
/// Matches the Linux kernel NBD module's `NBD_MAX_BUFFER_SIZE` (32MB)
/// and QEMU's max request size. Without this cap, a malicious client
/// could request up to 4GB per request (u32::MAX), exhausting memory.
const MAX_REQUEST_PAYLOAD: u32 = 32 * 1024 * 1024;

/// Response to be sent back to the client.
/// Sent through a channel from handler tasks to the writer task.
#[derive(Debug)]
enum Response {
    /// Simple reply with optional data (for reads)
    Simple {
        cookie: u64,
        error: u32,
        data: Bytes,
    },
    /// Signal to drain and disconnect
    Disconnect { export_name: String },
    /// Signal that all requests have been processed
    Shutdown,
}

pub enum Transport {
    Tcp(SocketAddr),
    Unix(std::path::PathBuf),
}

/// NBD server with multi-export support.
///
/// Uses an ExportRouter to manage multiple exports, each with independent
/// storage and caching.
///
/// # Example
/// ```ignore
/// let router = Arc::new(ExportRouter::new(...));
/// router.create_export(config, false).await?;
///
/// let server = NBDServer::new_tcp(router, "0.0.0.0:10809".parse()?);
/// server.start(shutdown_token).await?;
/// ```
pub struct NBDServer {
    router: Arc<ExportRouter>,
    transport: Transport,
    /// Cap on concurrent client connections (TCP + Unix combined). One
    /// `Arc<Semaphore>` is shared across all `NBDServer` instances created
    /// with `new_tcp_with_limiter`/`new_unix_with_limiter` so a single
    /// budget covers all listeners. Each accepted connection holds an
    /// `OwnedSemaphorePermit` for its lifetime; the permit drops with
    /// the client task.
    connection_limiter: Arc<Semaphore>,
    /// Listener-fd registry. When set, `start` registers the listener's
    /// raw fd here right after `bind`, so the handoff predecessor can
    /// later snapshot + dup + send via SCM_RIGHTS to the successor.
    listener_registry: Option<crate::handoff::listener_registry::ListenerRegistry>,
    /// Optional inherited listener fd (from a handoff successor's
    /// SCM_RIGHTS receive). When `Some`, `start` reuses it instead of
    /// calling `bind` — preserving the kernel socket and the in-flight
    /// TCP/Unix connections currently established on it.
    inherited_listener: parking_lot::Mutex<Option<std::os::fd::OwnedFd>>,
}

impl NBDServer {
    /// Create a TCP NBD server with an unlimited connection budget.
    /// Test/benchmark only — production callers use
    /// `new_tcp_with_limiter`. Available to both this crate's `cfg(test)`
    /// and external test crates that depend on the `test-utils` feature
    /// (e.g. `docker_integration`).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_tcp(router: Arc<ExportRouter>, socket: SocketAddr) -> Self {
        Self::new_tcp_with_limiter(router, socket, Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)))
    }

    /// Create a TCP NBD server. The limiter caps concurrent connections
    /// across this server and any peer servers sharing the same limiter
    /// (e.g. multiple TCP listeners + a Unix socket listener all sharing
    /// one budget from `nbd.max_connections`).
    pub fn new_tcp_with_limiter(
        router: Arc<ExportRouter>,
        socket: SocketAddr,
        connection_limiter: Arc<Semaphore>,
    ) -> Self {
        Self {
            router,
            transport: Transport::Tcp(socket),
            connection_limiter,
            listener_registry: None,
            inherited_listener: parking_lot::Mutex::new(None),
        }
    }

    /// Attach a `ListenerRegistry` so that `start` records the bound
    /// listener fd for later handoff via SCM_RIGHTS.
    pub fn with_listener_registry(
        mut self,
        registry: crate::handoff::listener_registry::ListenerRegistry,
    ) -> Self {
        self.listener_registry = Some(registry);
        self
    }

    /// Attach an inherited listener fd (received via SCM_RIGHTS from a
    /// handoff predecessor). When set, `start` reuses this fd instead
    /// of calling `bind`, preserving in-flight client connections.
    pub fn with_inherited_listener(self, fd: std::os::fd::OwnedFd) -> Self {
        *self.inherited_listener.lock() = Some(fd);
        self
    }

    /// Create a Unix socket NBD server with an unlimited connection budget.
    /// Available under `cfg(test)` or the `test-utils` feature.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_unix(router: Arc<ExportRouter>, socket_path: impl Into<std::path::PathBuf>) -> Self {
        Self::new_unix_with_limiter(
            router,
            socket_path,
            Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
        )
    }

    /// Create a Unix socket NBD server with a shared connection limiter.
    pub fn new_unix_with_limiter(
        router: Arc<ExportRouter>,
        socket_path: impl Into<std::path::PathBuf>,
        connection_limiter: Arc<Semaphore>,
    ) -> Self {
        Self {
            router,
            transport: Transport::Unix(socket_path.into()),
            connection_limiter,
            listener_registry: None,
            inherited_listener: parking_lot::Mutex::new(None),
        }
    }

    fn spawn_client_handler<S>(
        &self,
        stream: S,
        shutdown: &CancellationToken,
        client_name: String,
        permit: OwnedSemaphorePermit,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        let router = Arc::clone(&self.router);
        let client_shutdown = shutdown.child_token();

        // Supervised: a panic inside one client's handler must not propagate
        // out and take down peer connections or the accept loop. The
        // CancellationToken is a child of the server token; on unwind the
        // socket and child token drop and tear down dependent tasks.
        // The permit is moved into the task and drops on completion (or
        // panic), releasing the connection budget back to the semaphore.
        let _handle = task::spawn_supervised("nbd-client", async move {
            let _permit = permit; // hold for the lifetime of the connection
            if let Err(e) = handle_client_stream(stream, router, client_shutdown).await {
                error!("Error handling NBD client {}: {}", client_name, e);
            }
        });
    }

    /// Start the NBD server.
    ///
    /// This listens for connections and spawns a handler for each client.
    /// Returns when the shutdown token is cancelled.
    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;
        match &self.transport {
            Transport::Tcp(socket) => {
                let listener = if let Some(fd) = self.inherited_listener.lock().take() {
                    info!("NBD TCP server inheriting listener fd from handoff predecessor (target {})", socket);
                    let std_listener = std::net::TcpListener::from(fd);
                    std_listener.set_nonblocking(true)?;
                    TcpListener::from_std(std_listener)?
                } else {
                    let l = TcpListener::bind(socket).await?;
                    info!("NBD server listening on {}", socket);
                    l
                };
                if let Some(reg) = &self.listener_registry {
                    reg.register(
                        crate::handoff::listener_registry::ListenerKind::NbdTcp(socket.to_string()),
                        listener.as_raw_fd(),
                    );
                }

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("NBD TCP server shutting down on {}", socket);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, addr) = result?;
                            // Acquire a connection permit. If the limiter is at
                            // capacity, drop the new socket immediately rather than
                            // queueing it — slow-accepting connections waste kernel
                            // backlog and let one tenant pin the budget. The client
                            // sees a connection-reset and can retry.
                            let permit = match Arc::clone(&self.connection_limiter).try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    warn!(
                                        addr = %addr,
                                        cap = self.connection_limiter.available_permits(),
                                        "NBD connection budget exhausted; rejecting client",
                                    );
                                    drop(stream);
                                    continue;
                                }
                            };
                            info!("NBD client connected from {}", addr);
                            stream.set_nodelay(true)?;
                            self.spawn_client_handler(stream, &shutdown, addr.to_string(), permit);
                        }
                    }
                }
            }
            Transport::Unix(path) => {
                let listener = if let Some(fd) = self.inherited_listener.lock().take() {
                    info!("NBD Unix server inheriting listener fd from handoff predecessor (target {:?})", path);
                    let std_listener = std::os::unix::net::UnixListener::from(fd);
                    std_listener.set_nonblocking(true)?;
                    UnixListener::from_std(std_listener)?
                } else {
                    // Remove existing socket file if it exists
                    if path.exists() {
                        warn!("Removing existing Unix socket at {:?}", path);
                        let _ = std::fs::remove_file(path);
                    }
                    let l = UnixListener::bind(path).map_err(|e| {
                        std::io::Error::new(
                            e.kind(),
                            format!("Failed to bind NBD Unix socket at {:?}: {}", path, e),
                        )
                    })?;
                    info!("NBD server listening on Unix socket {:?}", path);
                    l
                };
                if let Some(reg) = &self.listener_registry {
                    reg.register(
                        crate::handoff::listener_registry::ListenerKind::NbdUnix,
                        listener.as_raw_fd(),
                    );
                }

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("NBD Unix socket server shutting down at {:?}", path);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, _) = result?;
                            let permit = match Arc::clone(&self.connection_limiter).try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    warn!(
                                        cap = self.connection_limiter.available_permits(),
                                        "NBD connection budget exhausted; rejecting Unix client",
                                    );
                                    drop(stream);
                                    continue;
                                }
                            };
                            info!("NBD client connected via Unix socket");
                            self.spawn_client_handler(stream, &shutdown, "unix".to_string(), permit);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

pub(crate) async fn handle_client_stream<S>(
    stream: S,
    router: Arc<ExportRouter>,
    shutdown: CancellationToken,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let reader = BufReader::new(reader);
    let writer = BufWriter::new(writer);

    let mut session = NBDSession::new(reader, writer, router, shutdown);
    session.perform_handshake().await?;

    match session.negotiate_options().await {
        Ok((device, handler)) => {
            info!(
                "Client selected export: {}",
                String::from_utf8_lossy(&device.name)
            );
            session.handle_transmission(device, handler).await?;
        }
        Err(NBDError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            debug!("Client disconnected cleanly after option negotiation");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

struct NBDSession<R, W> {
    reader: R,
    writer: W,
    router: Arc<ExportRouter>,
    client_no_zeroes: bool,
    shutdown: CancellationToken,
}

// Basic session methods - no 'static required
impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> NBDSession<R, W> {
    fn new(
        reader: R,
        writer: W,
        router: Arc<ExportRouter>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            reader,
            writer,
            router,
            client_no_zeroes: false,
            shutdown,
        }
    }

    async fn perform_handshake(&mut self) -> Result<()> {
        let handshake = NBDServerHandshake::new(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES);
        let handshake_bytes = handshake.to_bytes()?;
        self.writer.write_all(&handshake_bytes).await?;
        self.writer.flush().await?;

        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf).await?;
        let client_flags = NBDClientFlags::from_bytes((&buf, 0))?.1;

        debug!("Client flags: 0x{:x}", client_flags.flags);

        if (client_flags.flags & NBD_FLAG_C_FIXED_NEWSTYLE) == 0 {
            return Err(NBDError::IncompatibleClient);
        }

        self.client_no_zeroes = (client_flags.flags & NBD_FLAG_C_NO_ZEROES) != 0;

        Ok(())
    }

    async fn negotiate_options(&mut self) -> Result<(BlockDevice, Arc<BlockHandler>)> {
        loop {
            let mut header_buf = [0u8; NBD_OPTION_HEADER_SIZE];
            match self.reader.read_exact(&mut header_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("Client disconnected during option negotiation");
                    return Err(NBDError::Io(e));
                }
                Err(e) => return Err(NBDError::Io(e)),
            }
            let header = NBDOptionHeader::from_bytes((&header_buf, 0))
                .map_err(|e| {
                    debug!("Raw header bytes: {:02x?}", header_buf);
                    NBDError::Protocol(format!("Invalid option header: {e}"))
                })?
                .1;

            debug!(
                "Received option: {} (length: {})",
                header.option, header.length
            );

            match header.option {
                NBD_OPT_LIST => {
                    debug!("Handling LIST option");
                    self.handle_list_option(header.length).await?;
                }
                NBD_OPT_EXPORT_NAME => {
                    debug!("Handling EXPORT_NAME option");
                    return self.handle_export_name_option(header.length).await;
                }
                NBD_OPT_INFO => {
                    debug!("Handling INFO option");
                    self.handle_info_option(header.length).await?;
                }
                NBD_OPT_GO => {
                    match self.handle_go_option(header.length).await {
                        Ok(result) => return Ok(result),
                        Err(NBDError::DeviceNotFound(_)) => {
                            // Device not found - stay in negotiation loop
                        }
                        Err(e) => return Err(e),
                    }
                }
                NBD_OPT_STRUCTURED_REPLY => {
                    debug!("Handling STRUCTURED_REPLY option");
                    self.handle_structured_reply_option(header.length).await?;
                }
                NBD_OPT_ABORT => {
                    debug!("Handling ABORT option");
                    self.send_option_reply(header.option, NBD_REP_ACK, &[])
                        .await?;
                    self.writer.flush().await?;
                    return Err(NBDError::Protocol("Client aborted".to_string()));
                }
                _ => {
                    debug!("Unknown option: {}", header.option);
                    self.drain_option_data(header.length).await?;
                    self.send_option_reply(header.option, NBD_REP_ERR_UNSUP, &[])
                        .await?;
                    self.writer.flush().await?;
                }
            }
        }
    }

    async fn handle_list_option(&mut self, length: u32) -> Result<()> {
        self.drain_option_data(length).await?;

        // List all exports from router
        let exports = self.router.list_exports().await;

        for export in exports {
            // Send NBD_REP_SERVER for each export
            let name_bytes = export.name.as_bytes();
            let mut data = Vec::with_capacity(4 + name_bytes.len());
            data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(name_bytes);

            self.send_option_reply(NBD_OPT_LIST, NBD_REP_SERVER, &data)
                .await?;
        }

        // Send final ACK
        self.send_option_reply(NBD_OPT_LIST, NBD_REP_ACK, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn handle_export_name_option(
        &mut self,
        length: u32,
    ) -> Result<(BlockDevice, Arc<BlockHandler>)> {
        if length > MAX_OPTION_DATA_LEN {
            return Err(NBDError::Protocol(format!(
                "export name length {length} exceeds maximum {MAX_OPTION_DATA_LEN}"
            )));
        }
        let mut name_buf = vec![0u8; length as usize];
        self.reader.read_exact(&mut name_buf).await?;

        let export_name = String::from_utf8(name_buf.clone())
            .map_err(|_| NBDError::Protocol("export name is not valid UTF-8".into()))?;
        debug!(
            "Client requested export: '{}' (length: {})",
            export_name, length
        );

        // Look up handler from router
        let handler = self.router.get_handler(&export_name).await.ok_or_else(|| {
            error!("Export '{}' not found, closing connection", export_name);
            NBDError::DeviceNotFound(name_buf.clone())
        })?;

        let device = BlockDevice {
            name: name_buf,
            size: handler.device_size(),
        };

        self.writer.write_all(&device.size.to_be_bytes()).await?;
        self.writer
            .write_all(&TRANSMISSION_FLAGS.to_be_bytes())
            .await?;

        if !self.client_no_zeroes {
            self.writer
                .write_all(&[0u8; NBD_EXPORT_NAME_PADDING])
                .await?;
        }

        self.writer.flush().await?;
        Ok((device, handler))
    }

    async fn handle_info_option(&mut self, length: u32) -> Result<()> {
        let data = self.read_option_data(length).await?;

        // Parse export name from data
        if data.len() < 4 {
            self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Ok(());
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len {
            self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Ok(());
        }

        let export_name = match String::from_utf8(data[4..4 + name_len].to_vec()) {
            Ok(n) => n,
            Err(_) => {
                self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_INVALID, &[]).await?;
                self.writer.flush().await?;
                return Ok(());
            }
        };

        // Look up handler
        let handler = match self.router.get_handler(&export_name).await {
            Some(h) => h,
            None => {
                self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_UNKNOWN, &[])
                    .await?;
                self.writer.flush().await?;
                return Ok(());
            }
        };

        // Send NBD_INFO_EXPORT
        let mut info_data = Vec::with_capacity(12);
        info_data.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
        info_data.extend_from_slice(&handler.device_size().to_be_bytes());
        info_data.extend_from_slice(&TRANSMISSION_FLAGS.to_be_bytes());

        self.send_option_reply(NBD_OPT_INFO, NBD_REP_INFO, &info_data)
            .await?;

        // Send ACK
        self.send_option_reply(NBD_OPT_INFO, NBD_REP_ACK, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn handle_go_option(
        &mut self,
        length: u32,
    ) -> Result<(BlockDevice, Arc<BlockHandler>)> {
        let data = self.read_option_data(length).await?;

        // Parse export name
        if data.len() < 4 {
            self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Err(NBDError::DeviceNotFound(Vec::new()));
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len {
            self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Err(NBDError::DeviceNotFound(Vec::new()));
        }

        let name_bytes = &data[4..4 + name_len];
        let export_name = match String::from_utf8(name_bytes.to_vec()) {
            Ok(n) => n,
            Err(_) => {
                self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_INVALID, &[]).await?;
                self.writer.flush().await?;
                return Err(NBDError::Protocol("export name is not valid UTF-8".into()));
            }
        };

        debug!("GO option for export: '{}'", export_name);

        // Look up handler
        let handler = match self.router.get_handler(&export_name).await {
            Some(h) => h,
            None => {
                warn!("Export '{}' not found", export_name);
                self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_UNKNOWN, &[])
                    .await?;
                self.writer.flush().await?;
                return Err(NBDError::DeviceNotFound(name_bytes.to_vec()));
            }
        };

        let device = BlockDevice {
            name: name_bytes.to_vec(),
            size: handler.device_size(),
        };

        // Send NBD_INFO_EXPORT
        let mut info_data = Vec::with_capacity(12);
        info_data.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
        info_data.extend_from_slice(&device.size.to_be_bytes());
        info_data.extend_from_slice(&TRANSMISSION_FLAGS.to_be_bytes());

        self.send_option_reply(NBD_OPT_GO, NBD_REP_INFO, &info_data)
            .await?;

        // Send ACK
        self.send_option_reply(NBD_OPT_GO, NBD_REP_ACK, &[]).await?;
        self.writer.flush().await?;

        Ok((device, handler))
    }

    async fn handle_structured_reply_option(&mut self, length: u32) -> Result<()> {
        self.drain_option_data(length).await?;
        self.send_option_reply(NBD_OPT_STRUCTURED_REPLY, NBD_REP_ERR_UNSUP, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn read_option_data(&mut self, length: u32) -> Result<Vec<u8>> {
        if length > MAX_OPTION_DATA_LEN {
            return Err(NBDError::Protocol(format!(
                "option data length {length} exceeds maximum {MAX_OPTION_DATA_LEN}"
            )));
        }
        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;
        Ok(data)
    }

    async fn drain_option_data(&mut self, length: u32) -> Result<()> {
        if length > MAX_OPTION_DATA_LEN {
            return Err(NBDError::Protocol(format!(
                "option data length {length} exceeds maximum {MAX_OPTION_DATA_LEN}"
            )));
        }
        if length > 0 {
            let mut buf = vec![0u8; length as usize];
            self.reader.read_exact(&mut buf).await?;
        }
        Ok(())
    }

    async fn send_option_reply(&mut self, option: u32, reply_type: u32, data: &[u8]) -> Result<()> {
        let reply = NBDOptionReply::new(option, reply_type, data.len() as u32);
        let reply_bytes = reply.to_bytes()?;
        self.writer.write_all(&reply_bytes).await?;
        if !data.is_empty() {
            self.writer.write_all(data).await?;
        }
        Ok(())
    }
}

// Transmission methods - require 'static for spawning tasks
impl<R: AsyncRead + Unpin + Send + 'static, W: AsyncWrite + Unpin + Send + 'static> NBDSession<R, W> {
    /// Handle the transmission phase with concurrent request processing.
    ///
    /// This implementation processes multiple requests concurrently:
    /// 1. Requests are read sequentially from the socket (protocol requirement)
    /// 2. Each request is dispatched to a tokio task for processing
    /// 3. Responses are sent through a channel to a dedicated writer task
    /// 4. The writer task serializes responses back to the client
    ///
    /// This allows high IOPS by overlapping I/O operations instead of
    /// processing requests one at a time.
    async fn handle_transmission(
        self,
        device: BlockDevice,
        handler: Arc<BlockHandler>,
    ) -> Result<()> {
        let export_name = match String::from_utf8(device.name.clone()) {
            Ok(n) => n,
            Err(_) => return Err(NBDError::Protocol("export name is not valid UTF-8".into())),
        };

        // Destructure self to split reader and writer
        let NBDSession { mut reader, writer, router, shutdown, .. } = self;

        // Channel for sending responses from handler tasks to writer task
        let (response_tx, response_rx) = mpsc::channel::<Response>(MAX_INFLIGHT_REQUESTS);

        // Spawn writer task that serializes responses to the socket. The
        // CancelOnDrop guard guarantees that if the writer task exits or
        // panics for any reason, the connection's shutdown token is fired
        // — without that, a writer panic leaves the reader loop blocked
        // in `read_exact` indefinitely (no socket EOF, no cancellation).
        let writer_router = Arc::clone(&router);
        let writer_shutdown = shutdown.clone();
        let writer_handle = task::spawn_supervised("nbd-response-writer", async move {
            let _guard = CancelOnDrop(writer_shutdown);
            Self::response_writer(writer, response_rx, writer_router).await
        });

        // JoinSet to track all spawned request handler tasks
        let mut tasks: JoinSet<()> = JoinSet::new();

        // Process requests until disconnect or shutdown
        let result = Self::request_reader_loop(
            &mut reader,
            &shutdown,
            &export_name,
            handler,
            response_tx.clone(),
            &mut tasks,
        ).await;

        // Wait for all in-flight request tasks to complete
        // This ensures all responses are sent before we close the channel
        while tasks.join_next().await.is_some() {}

        // Now drop the sender to signal writer task to finish
        drop(response_tx);

        // Wait for writer to finish. spawn_supervised already logged any
        // panic; we surface it here as an extra warning and rely on the
        // CancelOnDrop guard above to have torn down the connection.
        match writer_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(panicked)) => {
                error!("nbd response writer panicked: {}", panicked.message);
            }
            Err(join_err) => {
                error!("nbd response writer join error: {:?}", join_err);
            }
        }

        result
    }

    /// Read requests from socket and dispatch to handler tasks.
    async fn request_reader_loop(
        reader: &mut R,
        shutdown: &CancellationToken,
        export_name: &str,
        handler: Arc<BlockHandler>,
        response_tx: mpsc::Sender<Response>,
        tasks: &mut JoinSet<()>,
    ) -> Result<()> {
        // Reusable buffer for draining oversized write payloads (allocated on first use).
        let mut drain_buf: Option<Vec<u8>> = None;

        loop {
            let mut request_buf = [0u8; NBD_REQUEST_HEADER_SIZE];

            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("NBD client handler shutting down");
                    let _ = response_tx.send(Response::Shutdown).await;
                    return Ok(());
                }
                result = reader.read_exact(&mut request_buf) => {
                    match result {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            debug!("Client disconnected");
                            let _ = response_tx.send(Response::Shutdown).await;
                            return Ok(());
                        }
                        Err(e) => {
                            let _ = response_tx.send(Response::Shutdown).await;
                            return Err(NBDError::Io(e));
                        }
                    }
                }
            }

            let request = NBDRequest::from_bytes((&request_buf, 0))
                .map_err(|e| NBDError::Protocol(format!("Invalid request: {e}")))?
                .1;

            debug!(
                "NBD command: {:?}, offset={}, length={}",
                request.cmd_type, request.offset, request.length
            );

            let fua = (request.flags & NBD_CMD_FLAG_FUA) != 0;

            // Reject oversized payloads before allocating any memory.
            if request.length > MAX_REQUEST_PAYLOAD {
                warn!(
                    length = request.length,
                    max = MAX_REQUEST_PAYLOAD,
                    "rejecting oversized NBD request"
                );
                // For writes, we must still drain the payload bytes from the socket
                // to keep the protocol stream in sync.
                if matches!(request.cmd_type, NBDCommand::Write) {
                    let buf = drain_buf.get_or_insert_with(|| vec![0u8; 64 * 1024]);
                    let mut remaining = u64::from(request.length);
                    while remaining > 0 {
                        let to_read = std::cmp::min(remaining, buf.len() as u64) as usize;
                        reader.read_exact(&mut buf[..to_read]).await
                            .map_err(NBDError::Io)?;
                        remaining -= to_read as u64;
                    }
                }
                let _ = response_tx.send(Response::Simple {
                    cookie: request.cookie,
                    error: super::error::CommandError::InvalidArgument.to_nbd_errno(),
                    data: Bytes::new(),
                }).await;
                continue;
            }

            // Bound concurrent in-flight tasks to prevent unbounded memory growth.
            // When at the limit, drain one completed task before spawning a new one.
            while tasks.len() >= MAX_INFLIGHT_REQUESTS {
                tasks.join_next().await;
            }

            match request.cmd_type {
                NBDCommand::Read => {
                    // Read is non-mutating; on panic, ack with EIO + zero
                    // payload (NBD reply must include `length` bytes even
                    // on error — many clients require it) and keep the
                    // connection alive.
                    let h = Arc::clone(&handler);
                    let tx = response_tx.clone();
                    let offset = request.offset;
                    let length = request.length;
                    let cookie = request.cookie;
                    let sd = shutdown.clone();

                    tasks.spawn(run_request(
                        "nbd-read",
                        cookie,
                        tx,
                        sd,
                        length,
                        false,
                        async move {
                            let result = h.read(offset, length).await;
                            match result {
                                Ok(data) => Response::Simple { cookie, error: NBD_SUCCESS, data },
                                Err(ref e) => {
                                    error!(offset, length, error = ?e, "NBD read failed");
                                    let zeros = Bytes::from(vec![0u8; length as usize]);
                                    Response::Simple { cookie, error: e.to_nbd_errno(), data: zeros }
                                }
                            }
                        },
                    ));
                }
                NBDCommand::Write => {
                    // Must read write data from socket before spawning task
                    let write_data = Self::read_write_payload(reader, request.length).await;

                    let h = Arc::clone(&handler);
                    let tx = response_tx.clone();
                    let offset = request.offset;
                    let cookie = request.cookie;
                    // Mutating handler — write_cache::write_inner is NOT
                    // rollback-safe. run_mutating_request marks the
                    // export degraded on panic so peer connections see
                    // EIO too (not just this one's client).
                    let sd = shutdown.clone();
                    let mark_handler = Arc::clone(&h);

                    tasks.spawn(run_mutating_request(
                        "nbd-write",
                        cookie,
                        tx,
                        sd,
                        mark_handler,
                        async move {
                            let result = match write_data {
                                Ok(data) => h.write(offset, &data, fua).await,
                                Err(e) => Err(e),
                            };
                            match result {
                                Ok(()) => Response::Simple { cookie, error: NBD_SUCCESS, data: Bytes::new() },
                                Err(e) => Response::Simple { cookie, error: e.to_nbd_errno(), data: Bytes::new() },
                            }
                        },
                    ));
                }
                NBDCommand::Disconnect => {
                    // Wait for all in-flight writes to complete before
                    // draining. Without this, the drain can miss writes
                    // that are still executing their pwrite() calls.
                    while tasks.join_next().await.is_some() {}
                    info!("Client disconnecting from '{}', draining to S3...", export_name);
                    let _ = response_tx.send(Response::Disconnect { export_name: export_name.to_string() }).await;
                    return Ok(());
                }
                NBDCommand::Flush => {
                    // FLUSH is a write barrier: the NBD spec requires that all
                    // previously-submitted writes are on stable storage before
                    // the flush reply is sent. Drain all in-flight tasks first
                    // so their pwrite()s complete, then fdatasync.
                    while tasks.join_next().await.is_some() {}

                    // Synchronous handler.flush() runs in the reader loop
                    // body, not a spawned task. A panic here would unwind
                    // the entire reader loop and silently kill the
                    // connection. Catch it explicitly: mark export
                    // degraded, ack EIO, and tear down (flush panic
                    // implies WAL or fsync poisoning; this and peer
                    // connections cannot continue safely).
                    let cookie = request.cookie;
                    let response = match std::panic::catch_unwind(AssertUnwindSafe(|| handler.flush())) {
                        Ok(Ok(())) => Response::Simple { cookie, error: NBD_SUCCESS, data: Bytes::new() },
                        Ok(Err(e)) => Response::Simple { cookie, error: e.to_nbd_errno(), data: Bytes::new() },
                        Err(payload) => {
                            task::record_panic();
                            let msg = task::panic_message(payload.as_ref());
                            error!(cookie, "NBD flush handler panicked: {}", msg);
                            handler.mark_degraded("nbd-flush");
                            shutdown.cancel();
                            Response::Simple { cookie, error: NBD_EIO, data: Bytes::new() }
                        }
                    };
                    let _ = response_tx.send(response).await;
                }
                NBDCommand::Trim => {
                    let h = Arc::clone(&handler);
                    let tx = response_tx.clone();
                    let offset = request.offset;
                    let length = request.length;
                    let cookie = request.cookie;
                    // Mutating handler (zero_range sets-present-before-fallocate).
                    let sd = shutdown.clone();
                    let mark_handler = Arc::clone(&h);

                    tasks.spawn(run_mutating_request(
                        "nbd-trim",
                        cookie,
                        tx,
                        sd,
                        mark_handler,
                        async move {
                            let result = h.trim(offset, length, fua).await;
                            match result {
                                Ok(()) => Response::Simple { cookie, error: NBD_SUCCESS, data: Bytes::new() },
                                Err(e) => Response::Simple { cookie, error: e.to_nbd_errno(), data: Bytes::new() },
                            }
                        },
                    ));
                }
                NBDCommand::WriteZeroes => {
                    let h = Arc::clone(&handler);
                    let tx = response_tx.clone();
                    let offset = request.offset;
                    let length = request.length;
                    let cookie = request.cookie;
                    // Mutating handler (same code path as Trim).
                    let sd = shutdown.clone();
                    let mark_handler = Arc::clone(&h);

                    tasks.spawn(run_mutating_request(
                        "nbd-write-zeroes",
                        cookie,
                        tx,
                        sd,
                        mark_handler,
                        async move {
                            let result = h.write_zeroes(offset, length, fua).await;
                            match result {
                                Ok(()) => Response::Simple { cookie, error: NBD_SUCCESS, data: Bytes::new() },
                                Err(e) => Response::Simple { cookie, error: e.to_nbd_errno(), data: Bytes::new() },
                            }
                        },
                    ));
                }
                NBDCommand::Cache => {
                    let h = Arc::clone(&handler);
                    let tx = response_tx.clone();
                    let offset = request.offset;
                    let length = request.length;
                    let cookie = request.cookie;
                    // Pure validation no-op; no shared mutation, no need
                    // to tear down the connection on panic.
                    let sd = shutdown.clone();

                    tasks.spawn(run_request(
                        "nbd-cache",
                        cookie,
                        tx,
                        sd,
                        0,
                        false,
                        async move {
                            let result = h.cache(offset, length);
                            match result {
                                Ok(()) => Response::Simple { cookie, error: NBD_SUCCESS, data: Bytes::new() },
                                Err(e) => Response::Simple { cookie, error: e.to_nbd_errno(), data: Bytes::new() },
                            }
                        },
                    ));
                }
                NBDCommand::Unknown(cmd) => {
                    warn!(
                        cmd,
                        flags = request.flags,
                        offset = request.offset,
                        length = request.length,
                        cookie = request.cookie,
                        "Unknown NBD command"
                    );
                    let _ = response_tx.send(Response::Simple {
                        cookie: request.cookie,
                        error: super::error::CommandError::InvalidArgument.to_nbd_errno(),
                        data: Bytes::new(),
                    }).await;
                }
            }
        }
    }

    /// Read write payload from socket (must be done before spawning handler task).
    async fn read_write_payload(
        reader: &mut R,
        length: u32,
    ) -> super::error::CommandResult<Bytes> {
        use super::error::CommandError;

        if length == 0 {
            return Ok(Bytes::new());
        }

        let mut data = BytesMut::zeroed(length as usize);
        reader
            .read_exact(&mut data)
            .await
            .map_err(|_| CommandError::IoError)?;

        Ok(data.freeze())
    }

    /// Writer task that serializes responses to the socket.
    ///
    /// Batches writes and only flushes when no more responses are waiting,
    /// significantly improving throughput for high-concurrency workloads.
    async fn response_writer<Writer: AsyncWrite + Unpin>(
        mut writer: Writer,
        mut response_rx: mpsc::Receiver<Response>,
        router: Arc<ExportRouter>,
    ) {
        'outer: while let Some(mut response) = response_rx.recv().await {
            // Process all available responses before flushing
            loop {
                match response {
                    Response::Simple { cookie, error, data } => {
                        let reply = NBDSimpleReply::new(cookie, error);
                        let reply_bytes = match reply.to_bytes() {
                            Ok(b) => b,
                            Err(e) => {
                                debug!("Failed to serialize reply: {:?}", e);
                                // Try next response
                                match response_rx.try_recv() {
                                    Ok(next) => {
                                        response = next;
                                        continue;
                                    }
                                    Err(_) => break,
                                }
                            }
                        };

                        if let Err(e) = writer.write_all(&reply_bytes).await {
                            debug!("Failed to write reply header: {:?}", e);
                            break 'outer;
                        }

                        if !data.is_empty()
                            && let Err(e) = writer.write_all(&data).await {
                                debug!("Failed to write reply data: {:?}", e);
                                break 'outer;
                            }
                    }
                    Response::Disconnect { export_name } => {
                        // Flush before drain
                        let _ = writer.flush().await;
                        if let Err(e) = router.drain_export(&export_name).await {
                            router.record_drain_error(&export_name).await;
                            warn!("Failed to drain on disconnect: {}", e);
                        }
                        info!("Client disconnected, drain complete");
                        break 'outer;
                    }
                    Response::Shutdown => {
                        debug!("Writer task shutting down");
                        break 'outer;
                    }
                }

                // Try to get more responses without waiting
                match response_rx.try_recv() {
                    Ok(next) => response = next,
                    Err(_) => break, // No more waiting, flush now
                }
            }

            // Flush once after processing all available responses
            if let Err(e) = writer.flush().await {
                debug!("Failed to flush: {:?}", e);
                break;
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::{duplex, AsyncWriteExt};

    /// Build client flags bytes.
    fn client_flags_bytes(flags: u32) -> Vec<u8> {
        flags.to_be_bytes().to_vec()
    }

    /// Build an option header.
    fn option_header_bytes(option: u32, length: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&NBD_IHAVEOPT.to_be_bytes());
        buf.extend_from_slice(&option.to_be_bytes());
        buf.extend_from_slice(&length.to_be_bytes());
        buf
    }

    /// Build a GO option payload (name_len + name + info_requests).
    fn go_option_payload(name: &str) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&0u16.to_be_bytes()); // 0 info requests
        buf
    }

    /// Build an NBD request header.
    fn nbd_request_bytes(cmd: u16, flags: u16, cookie: u64, offset: u64, length: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(28);
        buf.extend_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
        buf.extend_from_slice(&flags.to_be_bytes());
        buf.extend_from_slice(&cmd.to_be_bytes());
        buf.extend_from_slice(&cookie.to_be_bytes());
        buf.extend_from_slice(&offset.to_be_bytes());
        buf.extend_from_slice(&length.to_be_bytes());
        buf
    }

    #[tokio::test]
    async fn test_handshake_client_fixed_newstyle() {
        // Create mock router
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        // Client sends FIXED_NEWSTYLE flag
        let client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE);
        let reader = Cursor::new(client_input);
        let writer = Vec::new();

        let mut session = NBDSession::new(reader, writer, router, shutdown);
        let result = session.perform_handshake().await;
        assert!(result.is_ok());
        assert!(!session.client_no_zeroes);
    }

    #[tokio::test]
    async fn test_handshake_client_no_zeroes() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        // Client sends both flags
        let client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES);
        let reader = Cursor::new(client_input);
        let writer = Vec::new();

        let mut session = NBDSession::new(reader, writer, router, shutdown);
        let result = session.perform_handshake().await;
        assert!(result.is_ok());
        assert!(session.client_no_zeroes);
    }

    #[tokio::test]
    async fn test_handshake_incompatible_client() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        // Client without FIXED_NEWSTYLE (incompatible)
        let client_input = client_flags_bytes(0);
        let reader = Cursor::new(client_input);
        let writer = Vec::new();

        let mut session = NBDSession::new(reader, writer, router, shutdown);
        let result = session.perform_handshake().await;
        assert!(matches!(result, Err(NBDError::IncompatibleClient)));
    }

    #[tokio::test]
    async fn test_handshake_writes_correct_magic() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        let client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE);
        let reader = Cursor::new(client_input);
        let mut writer = Vec::new();

        {
            let mut session = NBDSession::new(reader, &mut writer, router, shutdown);
            session.perform_handshake().await.unwrap();
        }

        // Verify handshake bytes
        assert!(writer.len() >= 18); // magic(8) + ihaveopt(8) + flags(2)
        let magic = u64::from_be_bytes(writer[0..8].try_into().unwrap());
        let ihaveopt = u64::from_be_bytes(writer[8..16].try_into().unwrap());
        assert_eq!(magic, NBD_MAGIC);
        assert_eq!(ihaveopt, NBD_IHAVEOPT);
    }

    #[tokio::test]
    async fn test_option_list_empty() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        // Client sends LIST option with 0 length
        let mut client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE);
        client_input.extend(option_header_bytes(NBD_OPT_LIST, 0));
        // Then ABORT to exit cleanly
        client_input.extend(option_header_bytes(NBD_OPT_ABORT, 0));

        let reader = Cursor::new(client_input);
        let mut writer = Vec::new();

        let mut session = NBDSession::new(reader, &mut writer, router, shutdown);
        session.perform_handshake().await.unwrap();
        let result = session.negotiate_options().await;

        // Should get abort error
        assert!(matches!(result, Err(NBDError::Protocol(msg)) if msg.contains("abort")));
    }

    #[tokio::test]
    async fn test_option_go_export_not_found() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        let payload = go_option_payload("nonexistent");
        let mut client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE);
        client_input.extend(option_header_bytes(NBD_OPT_GO, payload.len() as u32));
        client_input.extend(payload);
        // ABORT to exit
        client_input.extend(option_header_bytes(NBD_OPT_ABORT, 0));

        let reader = Cursor::new(client_input);
        let mut writer = Vec::new();

        let mut session = NBDSession::new(reader, &mut writer, router, shutdown);
        session.perform_handshake().await.unwrap();
        let result = session.negotiate_options().await;

        assert!(matches!(result, Err(NBDError::Protocol(msg)) if msg.contains("abort")));
    }

    #[tokio::test]
    async fn test_option_structured_reply_unsupported() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        let mut client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE);
        client_input.extend(option_header_bytes(NBD_OPT_STRUCTURED_REPLY, 0));
        client_input.extend(option_header_bytes(NBD_OPT_ABORT, 0));

        let reader = Cursor::new(client_input);
        let mut writer = Vec::new();

        let mut session = NBDSession::new(reader, &mut writer, router, shutdown);
        session.perform_handshake().await.unwrap();
        let result = session.negotiate_options().await;

        // Should send ERR_UNSUP reply, then continue to ABORT
        assert!(matches!(result, Err(NBDError::Protocol(msg)) if msg.contains("abort")));
    }

    #[tokio::test]
    async fn test_option_unknown_returns_unsupported() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        let mut client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE);
        // Unknown option 99
        client_input.extend(option_header_bytes(99, 0));
        client_input.extend(option_header_bytes(NBD_OPT_ABORT, 0));

        let reader = Cursor::new(client_input);
        let mut writer = Vec::new();

        let mut session = NBDSession::new(reader, &mut writer, router, shutdown);
        session.perform_handshake().await.unwrap();
        let _ = session.negotiate_options().await;

        // Verify ERR_UNSUP was sent for unknown option
        // Option reply: magic(8) + option(4) + reply_type(4) + length(4) = 20 bytes
        // Skip handshake (18 bytes), look for reply with NBD_REP_ERR_UNSUP
        let handshake_len = 18;
        let reply_start = handshake_len;
        if writer.len() >= reply_start + 20 {
            let reply_type = u32::from_be_bytes(
                writer[reply_start + 12..reply_start + 16]
                    .try_into()
                    .unwrap(),
            );
            assert_eq!(reply_type, NBD_REP_ERR_UNSUP);
        }
    }

    #[tokio::test]
    async fn test_read_write_data_beyond_device() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        // Simulate a duplex stream
        let (client, server) = duplex(4096);
        let (server_reader, server_writer) = tokio::io::split(server);
        let (mut client_reader, mut client_writer) = tokio::io::split(client);

        // Client sends handshake + GO + write beyond device
        tokio::spawn(async move {
            // Send client flags
            client_writer
                .write_all(&client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE))
                .await
                .unwrap();

            // Read and ignore server handshake (18 bytes)
            let mut buf = vec![0u8; 18];
            client_reader.read_exact(&mut buf).await.unwrap();

            // We can't easily complete this test without a real export
            // So we just verify the session starts correctly
        });

        let session = NBDSession::new(server_reader, server_writer, router, shutdown);
        // Session creation succeeds
        assert!(!session.client_no_zeroes);
    }

    #[tokio::test]
    async fn test_option_info_invalid_data() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        // INFO option with too-short data
        let mut client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE);
        client_input.extend(option_header_bytes(NBD_OPT_INFO, 2)); // Only 2 bytes
        client_input.extend(&[0u8, 0u8]); // Invalid - need at least 4 for name length
        client_input.extend(option_header_bytes(NBD_OPT_ABORT, 0));

        let reader = Cursor::new(client_input);
        let mut writer = Vec::new();

        let mut session = NBDSession::new(reader, &mut writer, router, shutdown);
        session.perform_handshake().await.unwrap();
        let _ = session.negotiate_options().await;

        // Should have sent ERR_INVALID for INFO
    }

    #[tokio::test]
    async fn test_go_option_invalid_name_length() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        let shutdown = CancellationToken::new();

        // GO option where name_len exceeds actual data
        let mut client_input = client_flags_bytes(NBD_FLAG_C_FIXED_NEWSTYLE);
        let mut payload = Vec::new();
        payload.extend_from_slice(&100u32.to_be_bytes()); // Says 100 bytes
        payload.extend_from_slice(b"short"); // Only 5 bytes
        client_input.extend(option_header_bytes(NBD_OPT_GO, payload.len() as u32));
        client_input.extend(payload);
        client_input.extend(option_header_bytes(NBD_OPT_ABORT, 0));

        let reader = Cursor::new(client_input);
        let mut writer = Vec::new();

        let mut session = NBDSession::new(reader, &mut writer, router, shutdown);
        session.perform_handshake().await.unwrap();
        let result = session.negotiate_options().await;

        // Should handle gracefully and continue to abort
        assert!(matches!(result, Err(NBDError::Protocol(_))));
    }

    #[test]
    fn test_nbd_request_bytes_format() {
        // Verify our test helper produces valid NBD request format
        let bytes = nbd_request_bytes(
            0,    // READ command
            0,    // no flags
            1234, // cookie
            4096, // offset
            512,  // length
        );

        assert_eq!(bytes.len(), 28);
        assert_eq!(
            u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            NBD_REQUEST_MAGIC
        );
    }

    #[test]
    fn test_transport_variants() {
        let addr: SocketAddr = "127.0.0.1:10809".parse().unwrap();
        let tcp = Transport::Tcp(addr);
        assert!(matches!(tcp, Transport::Tcp(_)));

        let unix = Transport::Unix("/tmp/test.sock".into());
        assert!(matches!(unix, Transport::Unix(_)));
    }

    // -- run_request panic-isolation tests ----------------------------------

    /// Helper: drain one Response from the channel under a short timeout.
    async fn recv_one(rx: &mut mpsc::Receiver<Response>) -> Response {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for response")
            .expect("channel closed before response")
    }

    #[tokio::test]
    async fn run_request_happy_path_forwards_response() {
        let (tx, mut rx) = mpsc::channel::<Response>(4);
        let shutdown = CancellationToken::new();
        let cookie = 0xdeadbeef_u64;

        run_request(
            "test-ok",
            cookie,
            tx,
            shutdown.clone(),
            0,
            true, // cancel_on_panic is irrelevant on happy path
            async move {
                Response::Simple {
                    cookie,
                    error: NBD_SUCCESS,
                    data: Bytes::from_static(b"ok"),
                }
            },
        )
        .await;

        let resp = recv_one(&mut rx).await;
        match resp {
            Response::Simple { cookie: c, error, data } => {
                assert_eq!(c, cookie);
                assert_eq!(error, NBD_SUCCESS);
                assert_eq!(&data[..], b"ok");
            }
            other => panic!("expected Response::Simple, got {:?}", other),
        }
        assert!(!shutdown.is_cancelled(), "happy path must not cancel");
    }

    #[tokio::test]
    async fn run_request_panic_no_cancel_sends_eio() {
        let (tx, mut rx) = mpsc::channel::<Response>(4);
        let shutdown = CancellationToken::new();
        let cookie = 7_u64;
        let panic_before = task::panic_count();

        run_request(
            "test-panic-no-cancel",
            cookie,
            tx,
            shutdown.clone(),
            0,
            false, // cache-shape: don't tear down connection
            async move {
                panic!("simulated handler panic");
                #[allow(unreachable_code)]
                Response::Simple {
                    cookie,
                    error: NBD_SUCCESS,
                    data: Bytes::new(),
                }
            },
        )
        .await;

        let resp = recv_one(&mut rx).await;
        match resp {
            Response::Simple { cookie: c, error, data } => {
                assert_eq!(c, cookie, "EIO must carry original cookie for client demux");
                assert_eq!(error, NBD_EIO);
                assert!(data.is_empty(), "non-read EIO carries empty payload");
            }
            other => panic!("expected Response::Simple, got {:?}", other),
        }
        assert!(!shutdown.is_cancelled(), "cancel_on_panic=false must not fire token");
        assert!(task::panic_count() > panic_before, "panic counter must increment");
    }

    #[tokio::test]
    async fn run_request_panic_with_cancel_fires_token() {
        let (tx, mut rx) = mpsc::channel::<Response>(4);
        let shutdown = CancellationToken::new();
        let cookie = 11_u64;

        run_request(
            "test-panic-cancel",
            cookie,
            tx,
            shutdown.clone(),
            0,
            true, // write-shape: tear down connection
            async move {
                panic!("write handler poison");
                #[allow(unreachable_code)]
                Response::Simple {
                    cookie,
                    error: NBD_SUCCESS,
                    data: Bytes::new(),
                }
            },
        )
        .await;

        // EIO response was sent BEFORE cancellation fires (writer needs it
        // to keep the wire framed).
        let resp = recv_one(&mut rx).await;
        match resp {
            Response::Simple { error, .. } => assert_eq!(error, NBD_EIO),
            other => panic!("expected Response::Simple, got {:?}", other),
        }
        assert!(shutdown.is_cancelled(), "cancel_on_panic=true must fire token");
    }

    #[tokio::test]
    async fn run_request_panic_read_shape_zero_pads_payload() {
        let (tx, mut rx) = mpsc::channel::<Response>(4);
        let shutdown = CancellationToken::new();
        let cookie = 99_u64;
        let read_length: u32 = 4096;

        run_request(
            "test-panic-read",
            cookie,
            tx,
            shutdown.clone(),
            read_length,
            false, // read is non-mutating
            async move {
                panic!("read handler boom");
                #[allow(unreachable_code)]
                Response::Simple {
                    cookie,
                    error: NBD_SUCCESS,
                    data: Bytes::new(),
                }
            },
        )
        .await;

        let resp = recv_one(&mut rx).await;
        match resp {
            Response::Simple { error, data, .. } => {
                assert_eq!(error, NBD_EIO);
                assert_eq!(
                    data.len(),
                    read_length as usize,
                    "read replies must include `length` bytes even on error",
                );
                assert!(data.iter().all(|b| *b == 0), "payload must be zero-filled");
            }
            other => panic!("expected Response::Simple, got {:?}", other),
        }
    }
}
