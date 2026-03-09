//! NBD kernel device management via generic netlink (`NBD_GENL`).
//!
//! Creates `/dev/nbdN` block devices backed by `BlockHandler` instances. Each
//! device is wired up internally: a Unix socketpair connects the kernel NBD
//! module to the existing NBD session handler, and netlink registers the device.
//!
//! # Architecture
//!
//! ```text
//! Kernel (/dev/nbdN)
//!   ↕ NBD commands over socketpair
//! NBD session (handle_client_stream)
//!   ↕ BlockHandler
//! WriteCache + ContentStore + S3
//! ```
//!
//! The orchestrator never touches the socket. It creates an export via the
//! HTTP API and gets back a device path.

pub(crate) mod netlink;

use crate::block::protocol::TRANSMISSION_FLAGS;
use crate::block::router::ExportRouter;
use crate::block::server::handle_client_stream;
use std::collections::HashMap;
use std::os::fd::{AsRawFd, IntoRawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Default dead connection timeout in seconds.
/// Enables kernel-side I/O queueing during restarts (zero-downtime upgrades).
const DEFAULT_DEAD_CONN_TIMEOUT: u32 = 30;

/// Persisted device mapping filename.
const DEVICE_MAP_FILE: &str = "nbd_devices.json";

/// Manages kernel NBD devices created via netlink.
///
/// Each export gets its own `/dev/nbdN` device. The device is backed by an
/// internal Unix socketpair: one end goes to the kernel, the other runs
/// the standard NBD session handler.
///
/// On hot reload, the manager detects existing devices via a persisted mapping
/// and uses `NBD_CMD_RECONFIGURE` to swap socket fds without removing the
/// device. The kernel queues I/O during the restart window.
pub struct NbdDeviceManager {
    devices: HashMap<String, NbdDevice>,
    dead_conn_timeout: u32,
    /// Directory for persisting device index mapping (enables hot reload).
    cache_dir: Option<PathBuf>,
}

struct NbdDevice {
    dev_index: i32,
    dev_path: PathBuf,
    session_handle: JoinHandle<()>,
    shutdown: CancellationToken,
}

impl Default for NbdDeviceManager {
    fn default() -> Self {
        Self::new()
    }
}

impl NbdDeviceManager {
    pub fn new() -> Self {
        Self {
            devices: HashMap::new(),
            dead_conn_timeout: DEFAULT_DEAD_CONN_TIMEOUT,
            cache_dir: None,
        }
    }

    /// Set the cache directory for persisting device mappings.
    /// Required for hot reload support (NBD_CMD_RECONFIGURE).
    pub fn with_cache_dir(mut self, dir: PathBuf) -> Self {
        self.cache_dir = Some(dir);
        self
    }

    /// Set the dead connection timeout (seconds). Controls how long the kernel
    /// queues I/O when the socket is disconnected (e.g., during binary upgrade).
    /// Set to 0 to disable.
    pub fn with_dead_conn_timeout(mut self, timeout: u32) -> Self {
        self.dead_conn_timeout = timeout;
        self
    }

    /// Load ALL persisted device indices from a previous run.
    ///
    /// Returns `{export_name → dev_index}` regardless of whether devices are
    /// still alive in the kernel. Callers check liveness separately to decide
    /// between reconfigure (alive) and fresh connect with preferred index (dead).
    fn load_persisted_indices(&self) -> HashMap<String, i32> {
        let Some(ref cache_dir) = self.cache_dir else {
            return HashMap::new();
        };
        let path = cache_dir.join(DEVICE_MAP_FILE);
        let Ok(data) = std::fs::read_to_string(&path) else {
            return HashMap::new();
        };
        let Ok(map): Result<HashMap<String, i32>, _> = serde_json::from_str(&data) else {
            warn!("corrupt {DEVICE_MAP_FILE}, ignoring");
            return HashMap::new();
        };
        for (name, index) in &map {
            debug!(export = %name, index, "loaded persisted nbd device index");
        }
        map
    }

    /// Persist current device mapping to disk.
    fn persist_devices(&self) {
        let Some(ref cache_dir) = self.cache_dir else {
            return;
        };
        let map: HashMap<&str, i32> = self
            .devices
            .iter()
            .map(|(name, dev)| (name.as_str(), dev.dev_index))
            .collect();
        let path = cache_dir.join(DEVICE_MAP_FILE);
        match serde_json::to_string(&map) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&path, json) {
                    warn!(path = %path.display(), error = %e, "failed to persist nbd device map");
                }
            }
            Err(e) => warn!(error = %e, "failed to serialize nbd device map"),
        }
    }

    /// Register an NBD kernel device for an export. Returns `/dev/nbdN`.
    ///
    /// If a device from a previous process is still alive (hot reload),
    /// uses `NBD_CMD_RECONFIGURE` to swap socket fds. The device keeps the
    /// same `/dev/nbdN` path and queued I/O resumes immediately.
    ///
    /// Otherwise creates a new device via `NBD_CMD_CONNECT`.
    pub async fn add_device(
        &mut self,
        export_name: &str,
        router: Arc<ExportRouter>,
        size_bytes: u64,
    ) -> anyhow::Result<PathBuf> {
        if self.devices.contains_key(export_name) {
            anyhow::bail!(
                "nbd device for export '{}' already registered",
                export_name
            );
        }

        // Check for a persisted device index from a previous run.
        let persisted = self.load_persisted_indices();
        let preferred_index = persisted.get(export_name).copied();

        // Create a Unix socketpair. One end for the server (NBD session),
        // one end for the kernel (via netlink).
        let (server_stream, client_stream) = UnixStream::pair()?;

        // Spawn the server-side NBD session handler.
        let shutdown = CancellationToken::new();
        let session_shutdown = shutdown.clone();
        let session_export = export_name.to_string();
        let session_handle = tokio::spawn(async move {
            if let Err(e) =
                handle_client_stream(server_stream, router, session_shutdown).await
            {
                // ConnectionReset / BrokenPipe are normal on disconnect.
                let is_normal = matches!(&e, crate::block::error::NBDError::Io(io_err)
                    if io_err.kind() == std::io::ErrorKind::ConnectionReset
                    || io_err.kind() == std::io::ErrorKind::BrokenPipe
                    || io_err.kind() == std::io::ErrorKind::UnexpectedEof);
                if !is_normal {
                    warn!(
                        export = %session_export,
                        error = %e,
                        "nbd kernel device session ended with error"
                    );
                }
            }
        });

        // Run the client-side NBD handshake on the other end.
        // This negotiates with the server session we just spawned.
        let handshake_result = client_handshake(&client_stream, export_name).await;
        if let Err(e) = handshake_result {
            shutdown.cancel();
            session_handle.abort();
            return Err(anyhow::anyhow!(
                "nbd client handshake failed for '{}': {}",
                export_name,
                e
            ));
        }

        // Convert to std stream now so into_raw_fd() can't fail after the
        // kernel takes ownership. If the netlink call fails below, std_stream
        // drops normally and closes the fd (correct — kernel rejected it).
        let std_stream = client_stream.into_std()?;
        // tokio sets sockets to non-blocking mode. The kernel NBD driver uses
        // kernel_recvmsg/kernel_sendmsg which return -EAGAIN on non-blocking
        // sockets when data isn't immediately available — treated as fatal EIO.
        // Set back to blocking so the kernel can wait for our replies.
        std_stream.set_nonblocking(false)?;
        let client_fd = std_stream.as_raw_fd();

        let dev_index = if let Some(index) = preferred_index {
            if is_nbd_device_alive(index) {
                // Hot reload: reconfigure existing device with new socket fd.
                // Queued I/O resumes immediately on the new socket.
                info!(
                    export = %export_name,
                    index,
                    "reconfiguring existing nbd device (hot reload)"
                );
                tokio::task::spawn_blocking(move || {
                    netlink::reconfigure(index, client_fd)
                })
                .await??;
                index
            } else {
                // Dead device from previous run — reclaim the same index so
                // the box manager's /dev/nbdN reference stays valid.
                debug!(
                    export = %export_name,
                    index,
                    "reclaiming dead nbd device index"
                );
                let size = size_bytes;
                let dead_conn_timeout = self.dead_conn_timeout;
                let server_flags = TRANSMISSION_FLAGS as u64;
                tokio::task::spawn_blocking(move || {
                    netlink::connect(client_fd, size, 512, server_flags, dead_conn_timeout, Some(index))
                })
                .await??
            }
        } else {
            // First time for this export — auto-assign.
            let size = size_bytes;
            let dead_conn_timeout = self.dead_conn_timeout;
            let server_flags = TRANSMISSION_FLAGS as u64;
            tokio::task::spawn_blocking(move || {
                netlink::connect(client_fd, size, 512, server_flags, dead_conn_timeout, None)
            })
            .await??
        };

        // The kernel now owns the client_fd. Release ownership so Drop
        // doesn't close it.
        let _ = std_stream.into_raw_fd();

        let dev_path = PathBuf::from(format!("/dev/nbd{dev_index}"));
        info!(
            export = %export_name,
            path = %dev_path.display(),
            index = dev_index,
            reclaimed = preferred_index.is_some(),
            "nbd kernel device registered"
        );

        self.devices.insert(
            export_name.to_string(),
            NbdDevice {
                dev_index,
                dev_path: dev_path.clone(),
                session_handle,
                shutdown,
            },
        );
        self.persist_devices();

        Ok(dev_path)
    }

    /// Get the device path for an export, if registered.
    pub fn get_device_path(&self, export_name: &str) -> Option<&Path> {
        self.devices.get(export_name).map(|d| d.dev_path.as_path())
    }

    /// Disconnect a kernel device without updating the persisted map.
    ///
    /// Simulates a crash: the device dies but `nbd_devices.json` still has the
    /// old index. Used in tests to prove device path stability across restarts.
    #[cfg(feature = "test-utils")]
    pub async fn crash_disconnect(&mut self, export_name: &str) -> anyhow::Result<()> {
        let Some(device) = self.devices.remove(export_name) else {
            return Ok(());
        };
        let index = device.dev_index;
        info!(export = %export_name, index, "crash_disconnect: killing kernel device (map preserved)");
        if let Err(e) = tokio::task::spawn_blocking(move || netlink::disconnect(index)).await? {
            warn!(export = %export_name, index, error = %e, "netlink disconnect failed");
        }
        device.shutdown.cancel();
        device.session_handle.abort();
        let _ = device.session_handle.await;
        // Intentionally NOT calling persist_devices() — the map still has the old index.
        Ok(())
    }

    /// Remove an NBD device for an export.
    ///
    /// Disconnects via netlink, then cancels the session handler.
    /// Idempotent: returns `Ok(())` if no device is registered.
    pub async fn remove_device(&mut self, export_name: &str) -> anyhow::Result<()> {
        let Some(device) = self.devices.remove(export_name) else {
            return Ok(());
        };

        info!(export = %export_name, index = device.dev_index, "removing nbd kernel device");

        // Disconnect via netlink (blocking).
        let index = device.dev_index;
        if let Err(e) = tokio::task::spawn_blocking(move || netlink::disconnect(index)).await? {
            warn!(
                export = %export_name,
                index,
                error = %e,
                "nbd netlink disconnect failed (device may already be gone)"
            );
        }

        // Cancel and join the session handler.
        device.shutdown.cancel();
        device.session_handle.abort();
        let _ = device.session_handle.await;

        self.persist_devices();
        Ok(())
    }

    /// Shutdown all NBD devices (full shutdown — disconnects kernel devices).
    ///
    /// Removes the persisted device mapping. For hot reload, do NOT call this —
    /// just drop the manager so devices stay alive for reconfigure.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let mut failed: Vec<String> = Vec::new();

        // Remove persisted mapping first — devices are being disconnected.
        if let Some(ref cache_dir) = self.cache_dir {
            let _ = std::fs::remove_file(cache_dir.join(DEVICE_MAP_FILE));
        }

        for (name, device) in self.devices {
            info!(export = %name, index = device.dev_index, "shutting down nbd kernel device");

            let index = device.dev_index;
            match tokio::task::spawn_blocking(move || netlink::disconnect(index)).await {
                Err(e) => {
                    failed.push(format!("{name}: task join failed: {e}"));
                    continue;
                }
                Ok(Err(e)) => {
                    warn!(
                        export = %name,
                        index,
                        error = %e,
                        "nbd netlink disconnect failed during shutdown (device may already be gone)"
                    );
                }
                Ok(Ok(())) => {}
            }

            device.shutdown.cancel();
            device.session_handle.abort();
            let _ = device.session_handle.await;
        }

        if !failed.is_empty() {
            anyhow::bail!(
                "nbd shutdown incomplete — {} device(s) failed: {}",
                failed.len(),
                failed.join(", ")
            );
        }

        Ok(())
    }
}

/// Check if an NBD device at the given index is still alive in the kernel.
/// Looks for `/sys/block/nbdN/pid` which exists when a device is connected.
fn is_nbd_device_alive(index: i32) -> bool {
    let pid_path = format!("/sys/block/nbd{index}/pid");
    std::path::Path::new(&pid_path).exists()
}

/// Client-side NBD handshake over a connected stream.
///
/// Performs the minimum handshake to get into transmission mode:
/// 1. Read server handshake (magic + flags)
/// 2. Send client flags (FIXED_NEWSTYLE | NO_ZEROES)
/// 3. Send NBD_OPT_GO with export name
/// 4. Read option replies until ACK
async fn client_handshake(
    stream: &UnixStream,
    export_name: &str,
) -> anyhow::Result<()> {
    // 1. Read server handshake: magic(8) + ihaveopt(8) + flags(2) = 18 bytes
    let mut handshake_buf = [0u8; 18];
    stream.readable().await?;
    // Use a loop since readable() may return before all data is available
    let mut offset = 0;
    while offset < 18 {
        stream.readable().await?;
        match stream.try_read(&mut handshake_buf[offset..]) {
            Ok(0) => return Err(anyhow::anyhow!("server closed during handshake")),
            Ok(n) => offset += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }

    // Verify magic
    let magic = u64::from_be_bytes(handshake_buf[0..8].try_into().unwrap());
    let ihaveopt = u64::from_be_bytes(handshake_buf[8..16].try_into().unwrap());
    if magic != crate::block::protocol::NBD_MAGIC {
        return Err(anyhow::anyhow!("bad server magic: {magic:#x}"));
    }
    if ihaveopt != crate::block::protocol::NBD_IHAVEOPT {
        return Err(anyhow::anyhow!("bad ihaveopt: {ihaveopt:#x}"));
    }

    // 2. Send client flags
    let client_flags: u32 = crate::block::protocol::NBD_FLAG_C_FIXED_NEWSTYLE
        | crate::block::protocol::NBD_FLAG_C_NO_ZEROES;
    stream.writable().await?;
    loop {
        match stream.try_write(&client_flags.to_be_bytes()) {
            Ok(_) => break,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                stream.writable().await?;
            }
            Err(e) => return Err(e.into()),
        }
    }

    // 3. Send NBD_OPT_GO with export name
    let name_bytes = export_name.as_bytes();
    // Option header: ihaveopt(8) + option(4) + length(4)
    // Option data: name_len(4) + name + info_requests_count(2)
    let data_len = 4 + name_bytes.len() + 2;
    let mut opt_msg = Vec::with_capacity(16 + data_len);
    opt_msg.extend_from_slice(&crate::block::protocol::NBD_IHAVEOPT.to_be_bytes());
    opt_msg.extend_from_slice(&crate::block::protocol::NBD_OPT_GO.to_be_bytes());
    opt_msg.extend_from_slice(&(data_len as u32).to_be_bytes());
    opt_msg.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
    opt_msg.extend_from_slice(name_bytes);
    opt_msg.extend_from_slice(&0u16.to_be_bytes()); // 0 info requests

    stream.writable().await?;
    let mut written = 0;
    while written < opt_msg.len() {
        match stream.try_write(&opt_msg[written..]) {
            Ok(n) => written += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                stream.writable().await?;
            }
            Err(e) => return Err(e.into()),
        }
    }

    // 4. Read option replies until ACK.
    // Reply header: magic(8) + option(4) + reply_type(4) + length(4) = 20 bytes
    loop {
        let mut reply_buf = [0u8; 20];
        let mut offset = 0;
        while offset < 20 {
            stream.readable().await?;
            match stream.try_read(&mut reply_buf[offset..]) {
                Ok(0) => return Err(anyhow::anyhow!("server closed during option reply")),
                Ok(n) => offset += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            }
        }

        let reply_type = u32::from_be_bytes(reply_buf[12..16].try_into().unwrap());
        let reply_len = u32::from_be_bytes(reply_buf[16..20].try_into().unwrap());

        // Drain reply data
        if reply_len > 0 {
            let mut drain = vec![0u8; reply_len as usize];
            let mut offset = 0;
            while offset < drain.len() {
                stream.readable().await?;
                match stream.try_read(&mut drain[offset..]) {
                    Ok(0) => {
                        return Err(anyhow::anyhow!("server closed during reply data"))
                    }
                    Ok(n) => offset += n,
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }

        // Check for errors
        if reply_type >= 0x80000000 {
            return Err(anyhow::anyhow!(
                "server rejected export '{}': reply type {reply_type:#x}",
                export_name
            ));
        }

        // ACK = handshake complete, we're in transmission mode
        if reply_type == crate::block::protocol::NBD_REP_ACK {
            return Ok(());
        }

        // NBD_REP_INFO = informational, keep reading
    }
}
