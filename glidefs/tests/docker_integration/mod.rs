//! Docker-based integration tests for GlideFS.
//!
//! Uses testcontainers to spin up MinIO as a real S3 backend and a lightweight
//! block client to exercise the full server stack. Tests are parameterized over
//! transport (NBD and ublk) via the `transport_test!` macro.
//!
//! Run: `cargo test --features docker-tests --test docker_integration`

mod nbd_client;
#[cfg(target_os = "linux")]
mod nbd_kernel_client;
#[cfg(all(target_os = "linux", feature = "ublk"))]
mod ublk_client;

use std::net::SocketAddr;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use tempfile::TempDir;
use testcontainers::ContainerAsync;
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
use tokio_util::sync::CancellationToken;

use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::router::{ExportRouter, RouterConfig, SnapshotResponse};
use glidefs::block::server::NBDServer;
use glidefs::config::ExportConfig;

// ---------------------------------------------------------------------------
// Transport selection
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub enum Transport {
    Nbd,
    #[cfg(target_os = "linux")]
    NbdKernel,
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    Ublk,
}

/// Check if kernel NBD is available (nbd module loaded).
#[cfg(target_os = "linux")]
pub fn nbd_kernel_available() -> bool {
    std::path::Path::new("/dev/nbd0").exists()
}

/// Check if ublk is available on this system (kernel module loaded + sufficient privileges).
#[cfg(all(target_os = "linux", feature = "ublk"))]
pub fn ublk_available() -> bool {
    std::path::Path::new("/dev/ublk-control").exists()
}

// ---------------------------------------------------------------------------
// transport_test! macro — generates NBD + ublk variants of each test
// ---------------------------------------------------------------------------

/// Generate a test function for each transport.
///
/// The macro binds `_transport: Transport` which tests pass to `TestServer::start()`.
/// The ublk variant is suffixed with `_ublk` and skips at runtime if ublk is unavailable.
///
/// Usage:
/// ```ignore
/// transport_test! {
///     async fn test_write_read_roundtrip() {
///         let server = TestServer::start(store, "db", _transport).await;
///         // ...
///     }
/// }
/// ```
macro_rules! transport_test {
    (async fn $name:ident($transport:ident) $body:block) => {
        #[tokio::test]
        async fn $name() {
            let $transport = $crate::Transport::Nbd;
            $body
        }

        #[cfg(target_os = "linux")]
        paste::paste! {
            #[tokio::test]
            async fn [< $name _nbd_kernel >]() {
                if !$crate::nbd_kernel_available() {
                    eprintln!("nbd-kernel: skipping (nbd module not loaded or insufficient privileges)");
                    return;
                }
                let $transport = $crate::Transport::NbdKernel;
                $body
            }
        }

        #[cfg(all(target_os = "linux", feature = "ublk"))]
        paste::paste! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn [< $name _ublk >]() {
                if !$crate::ublk_available() {
                    eprintln!("ublk: skipping (ublk_drv not available or insufficient privileges)");
                    return;
                }
                let $transport = $crate::Transport::Ublk;
                $body
            }
        }
    };
}

// ---------------------------------------------------------------------------
// TestClient — transport-agnostic block client
// ---------------------------------------------------------------------------

pub enum TestClient {
    Nbd(nbd_client::NbdClient),
    #[cfg(target_os = "linux")]
    NbdKernel(nbd_kernel_client::NbdKernelClient),
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    Ublk(ublk_client::UblkClient),
}

impl TestClient {
    pub async fn read(&mut self, offset: u64, length: u32) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Nbd(c) => c.read(offset, length).await,
            #[cfg(target_os = "linux")]
            Self::NbdKernel(c) => c.read(offset, length).await,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Self::Ublk(c) => c.read(offset, length).await,
        }
    }

    pub async fn write(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::Nbd(c) => c.write(offset, data).await,
            #[cfg(target_os = "linux")]
            Self::NbdKernel(c) => c.write(offset, data).await,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Self::Ublk(c) => c.write(offset, data).await,
        }
    }

    /// Write and return raw error code (0 = success).
    /// Use when you expect an error (e.g., write to readonly export).
    pub async fn write_raw(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<u32> {
        match self {
            Self::Nbd(c) => c.write_raw(offset, data).await,
            #[cfg(target_os = "linux")]
            Self::NbdKernel(c) => c.write_raw(offset, data).await,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Self::Ublk(c) => c.write_raw(offset, data).await,
        }
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Nbd(c) => c.flush().await,
            #[cfg(target_os = "linux")]
            Self::NbdKernel(c) => c.flush().await,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Self::Ublk(c) => c.flush().await,
        }
    }

    pub async fn disconnect(self) -> anyhow::Result<()> {
        match self {
            Self::Nbd(c) => c.disconnect().await,
            #[cfg(target_os = "linux")]
            Self::NbdKernel(c) => c.disconnect().await,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Self::Ublk(c) => c.disconnect().await,
        }
    }

    pub fn export_size(&self) -> u64 {
        match self {
            Self::Nbd(c) => c.export_size,
            #[cfg(target_os = "linux")]
            Self::NbdKernel(c) => c.export_size,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Self::Ublk(c) => c.export_size,
        }
    }
}

// ---------------------------------------------------------------------------
// ConnectInfo — clonable handle for creating clients in spawned tasks
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub enum ConnectInfo {
    Nbd {
        addr: SocketAddr,
        export_name: String,
    },
    #[cfg(target_os = "linux")]
    NbdKernel {
        dev_path: PathBuf,
        export_size: u64,
    },
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    Ublk {
        dev_path: PathBuf,
        export_size: u64,
    },
}

impl ConnectInfo {
    pub async fn connect(&self) -> anyhow::Result<TestClient> {
        match self {
            Self::Nbd { addr, export_name } => {
                let client = nbd_client::NbdClient::connect(*addr, export_name).await?;
                Ok(TestClient::Nbd(client))
            }
            #[cfg(target_os = "linux")]
            Self::NbdKernel {
                dev_path,
                export_size,
            } => {
                let client = nbd_kernel_client::NbdKernelClient::open(dev_path, *export_size)?;
                Ok(TestClient::NbdKernel(client))
            }
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Self::Ublk {
                dev_path,
                export_size,
            } => {
                let client = ublk_client::UblkClient::open(dev_path, *export_size)?;
                Ok(TestClient::Ublk(client))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TestContext — MinIO container + S3 object_store
// ---------------------------------------------------------------------------

pub struct TestContext {
    pub _minio: ContainerAsync<MinIO>,
    pub object_store: Arc<dyn ObjectStore>,
}

impl TestContext {
    /// Start a MinIO container and build an S3 object_store pointing at it.
    pub async fn new() -> Self {
        let minio = MinIO::default().start().await.unwrap();
        let host = minio.get_host().await.unwrap();

        // Retry port lookup — Docker can race between container start and
        // port mapping visibility, causing PortNotExposed on loaded machines.
        let port = {
            let mut last_err = None;
            let mut port = None;
            for _ in 0..10 {
                match minio.get_host_port_ipv4(9000).await {
                    Ok(p) => { port = Some(p); break; }
                    Err(e) => {
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                }
            }
            port.unwrap_or_else(|| panic!("MinIO port 9000 not available after retries: {:?}", last_err))
        };
        let endpoint = format!("http://{}:{}", host, port);

        // Create bucket using curl with AWS SigV4 auth inside the container
        // (reqwest basic_auth doesn't work for S3 API — MinIO requires SigV4)
        minio
            .exec(ExecCommand::new(vec![
                "curl",
                "-sf",
                "-X",
                "PUT",
                "--aws-sigv4",
                "aws:amz:us-east-1:s3",
                "-u",
                "minioadmin:minioadmin",
                "http://localhost:9000/test-bucket",
            ]))
            .await
            .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(&endpoint)
                .with_region("us-east-1")
                .with_bucket_name("test-bucket")
                .with_access_key_id("minioadmin")
                .with_secret_access_key("minioadmin")
                .with_allow_http(true)
                .build()
                .unwrap(),
        );

        Self {
            _minio: minio,
            object_store,
        }
    }
}

// ---------------------------------------------------------------------------
// Kernel NBD state (Linux only)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
struct NbdKernelState {
    manager: tokio::sync::Mutex<glidefs::block::nbd::NbdDeviceManager>,
}

// ---------------------------------------------------------------------------
// Ublk state (Linux + ublk feature only)
// ---------------------------------------------------------------------------

#[cfg(all(target_os = "linux", feature = "ublk"))]
struct UblkState {
    server: tokio::sync::Mutex<glidefs::block::ublk::UblkServer>,
    dev_paths: tokio::sync::Mutex<std::collections::HashMap<String, PathBuf>>,
}

// ---------------------------------------------------------------------------
// TestServer — in-process GlideFS server (NBD or ublk transport)
// ---------------------------------------------------------------------------

pub struct TestServer {
    pub router: Arc<ExportRouter>,
    addr: SocketAddr,
    transport: Transport,
    pub shutdown: CancellationToken,
    pub _cache_dir: TempDir,
    _server_handle: tokio::task::JoinHandle<()>,
    #[cfg(target_os = "linux")]
    nbd_kernel: Option<NbdKernelState>,
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    ublk: Option<UblkState>,
}

impl TestServer {
    /// Start a GlideFS server with the given transport.
    pub async fn start(
        object_store: Arc<dyn ObjectStore>,
        db_path: &str,
        transport: Transport,
    ) -> Self {
        let cache_dir = TempDir::new().unwrap();
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let shutdown = CancellationToken::new();

        let router = Arc::new(
            ExportRouter::new(RouterConfig {
                object_store,
                db_path: db_path.to_string(),
                cache_dir: cache_dir.path().to_path_buf(),
                block_size: 128 * 1024,
                clean_cache,
                wal_sync: false,
                max_s3_uploads: 128,
                max_s3_downloads: 512,
                default_blocks_per_pack: glidefs::block::pack::DEFAULT_BLOCKS_PER_PACK,
                ublk_nr_queues: 1,
                nbd_dead_conn_timeout: 0,
            })
            .await
            .expect("failed to create test router"),
        );

        // Always start NBD server (cheap, needed for NBD transport)
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let nbd_server = NBDServer::new_tcp(Arc::clone(&router), addr);
        let shutdown_clone = shutdown.clone();

        let server_handle = tokio::spawn(async move {
            if let Err(e) = nbd_server.start(shutdown_clone).await {
                eprintln!("NBD server error: {}", e);
            }
        });

        // Wait for NBD server to be ready
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Create kernel NBD device manager if requested
        #[cfg(target_os = "linux")]
        let nbd_kernel = if matches!(transport, Transport::NbdKernel) {
            Some(NbdKernelState {
                manager: tokio::sync::Mutex::new(
                    glidefs::block::nbd::NbdDeviceManager::new()
                        .with_cache_dir(cache_dir.path().to_path_buf())
                        .with_dead_conn_timeout(0),
                ),
            })
        } else {
            None
        };

        // Create ublk server if requested
        #[cfg(all(target_os = "linux", feature = "ublk"))]
        let ublk = if matches!(transport, Transport::Ublk) {
            Some(UblkState {
                server: tokio::sync::Mutex::new(glidefs::block::ublk::UblkServer::new()),
                dev_paths: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            })
        } else {
            None
        };

        Self {
            router,
            addr,
            transport,
            shutdown,
            _cache_dir: cache_dir,
            _server_handle: server_handle,
            #[cfg(target_os = "linux")]
            nbd_kernel,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            ublk,
        }
    }

    /// Register a ublk device for an export (no-op for NBD transport).
    #[allow(unused_variables)]
    pub async fn register_ublk_device(&self, name: &str) {
        #[cfg(all(target_os = "linux", feature = "ublk"))]
        if let Some(ref ublk) = self.ublk {
            let handler = self
                .router
                .get_handler(name)
                .await
                .unwrap_or_else(|| panic!("no handler for export '{name}'"));
            let path = ublk
                .server
                .lock()
                .await
                .add_device(name, handler)
                .await
                .unwrap_or_else(|e| panic!("failed to register ublk device for '{name}': {e}"));
            ublk.dev_paths
                .lock()
                .await
                .insert(name.to_string(), path);
        }
    }

    /// Unregister a ublk device for an export (no-op for NBD transport).
    #[allow(unused_variables)]
    async fn unregister_ublk_device(&self, name: &str) {
        #[cfg(all(target_os = "linux", feature = "ublk"))]
        if let Some(ref ublk) = self.ublk {
            ublk.server
                .lock()
                .await
                .remove_device(name)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("failed to unregister ublk device for '{name}': {e}")
                });
            ublk.dev_paths.lock().await.remove(name);
        }
    }

    /// Register a kernel NBD device for an export (no-op for other transports).
    #[allow(unused_variables)]
    async fn register_nbd_kernel_device(&self, name: &str) {
        #[cfg(target_os = "linux")]
        if let Some(ref nbd) = self.nbd_kernel {
            let handler = self
                .router
                .get_handler(name)
                .await
                .unwrap_or_else(|| panic!("no handler for export '{name}'"));
            let size = handler.device_size();
            nbd.manager
                .lock()
                .await
                .add_device(name, Arc::clone(&self.router), size)
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to register nbd kernel device for '{name}': {e}")
                });
        }
    }

    /// Unregister a kernel NBD device for an export (no-op for other transports).
    #[allow(unused_variables)]
    async fn unregister_nbd_kernel_device(&self, name: &str) {
        #[cfg(target_os = "linux")]
        if let Some(ref nbd) = self.nbd_kernel {
            nbd.manager
                .lock()
                .await
                .remove_device(name)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("failed to unregister nbd kernel device for '{name}': {e}")
                });
        }
    }

    /// Create a fresh export on this server (no existing data).
    pub async fn create_export(&self, name: &str, size_gb: f64) {
        let config = ExportConfig {
            name: name.to_string(),
            size_gb,
            s3_prefix: None,
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        self.router
            .create_export(config, false, None, None)
            .await
            .unwrap();
        self.register_nbd_kernel_device(name).await;
        self.register_ublk_device(name).await;
    }

    /// Persist an export definition to S3 (for discovery by other servers).
    pub async fn save_export(&self, name: &str, size_gb: f64) {
        let config = ExportConfig {
            name: name.to_string(),
            size_gb,
            s3_prefix: None,
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        self.router.save_export(&config).await.unwrap();
    }

    /// Restore an export from its S3 manifest (after drain/restart).
    pub async fn restore_export(&self, name: &str, size_gb: f64) {
        let config = ExportConfig {
            name: name.to_string(),
            size_gb,
            s3_prefix: None,
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        self.router
            .create_export(config, false, Some(name), None)
            .await
            .unwrap();
        self.register_nbd_kernel_device(name).await;
        self.register_ublk_device(name).await;
    }

    /// Restore a forked export from S3 manifest (using the source's S3 prefix).
    pub async fn restore_forked_export(&self, name: &str, source_prefix: &str, size_gb: f64) {
        let config = ExportConfig {
            name: name.to_string(),
            size_gb,
            s3_prefix: Some(source_prefix.to_string()),
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        self.router
            .create_export(config, false, Some(name), None)
            .await
            .unwrap();
        self.register_nbd_kernel_device(name).await;
        self.register_ublk_device(name).await;
    }

    /// Snapshot an export (flush dirty blocks + upload manifest).
    pub async fn snapshot_export(&self, name: &str) -> SnapshotResponse {
        self.router.snapshot_export(name, None).await.unwrap()
    }

    /// Fork an export from a source manifest (read-write).
    pub async fn fork_export(&self, name: &str, source_manifest: &str, size_gb: f64) {
        let config = ExportConfig {
            name: name.to_string(),
            size_gb,
            s3_prefix: Some(source_manifest.to_string()),
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        self.router
            .create_export(config, false, Some(source_manifest), None)
            .await
            .unwrap();
        self.register_nbd_kernel_device(name).await;
        self.register_ublk_device(name).await;
    }

    /// Fork an export from a source manifest (readonly).
    pub async fn fork_export_readonly(&self, name: &str, source_manifest: &str, size_gb: f64) {
        let config = ExportConfig {
            name: name.to_string(),
            size_gb,
            s3_prefix: Some(source_manifest.to_string()),
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        self.router
            .create_export(config, true, Some(source_manifest), None)
            .await
            .unwrap();
        self.register_nbd_kernel_device(name).await;
        self.register_ublk_device(name).await;
    }

    /// Promote a readonly export to read-write.
    pub async fn promote_export(&self, name: &str) {
        self.router.promote_export(name).await.unwrap();
    }

    /// Resize an export (grow only). Client must reconnect to see new size.
    pub async fn resize_export(&self, name: &str, new_size_gb: f64) {
        // Remove kernel devices before resize (router removes+recreates the export).
        self.unregister_nbd_kernel_device(name).await;
        self.unregister_ublk_device(name).await;
        self.router.resize_export(name, new_size_gb).await.unwrap();
        // Re-register with the new handler that has the updated size.
        self.register_nbd_kernel_device(name).await;
        self.register_ublk_device(name).await;
    }

    /// Drain all exports to S3, panicking if any fail.
    pub async fn drain_all(&self) {
        let failed = self.router.drain_all().await;
        assert!(
            failed.is_empty(),
            "drain_all failed: {}",
            failed
                .iter()
                .map(|(name, err)| format!("{name}: {err}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

    /// Connect a block client to a named export.
    pub async fn connect(&self, export_name: &str) -> TestClient {
        match self.transport {
            Transport::Nbd => {
                let client = nbd_client::NbdClient::connect(self.addr, export_name)
                    .await
                    .unwrap();
                TestClient::Nbd(client)
            }
            #[cfg(target_os = "linux")]
            Transport::NbdKernel => {
                let nbd = self.nbd_kernel.as_ref().expect("nbd kernel state missing");
                let dev_path = {
                    let manager = nbd.manager.lock().await;
                    manager
                        .get_device_path(export_name)
                        .unwrap_or_else(|| {
                            panic!("no nbd kernel device for export '{export_name}'")
                        })
                        .to_path_buf()
                };
                let handler = self
                    .router
                    .get_handler(export_name)
                    .await
                    .unwrap_or_else(|| panic!("no handler for export '{export_name}'"));
                let export_size = handler.device_size();
                TestClient::NbdKernel(
                    nbd_kernel_client::NbdKernelClient::open(&dev_path, export_size).unwrap(),
                )
            }
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Transport::Ublk => {
                let ublk = self.ublk.as_ref().expect("ublk state missing");
                let paths = ublk.dev_paths.lock().await;
                let dev_path = paths
                    .get(export_name)
                    .unwrap_or_else(|| panic!("no ublk device for export '{export_name}'"))
                    .clone();
                let handler = self
                    .router
                    .get_handler(export_name)
                    .await
                    .unwrap_or_else(|| panic!("no handler for export '{export_name}'"));
                let export_size = handler.device_size();
                TestClient::Ublk(ublk_client::UblkClient::open(&dev_path, export_size).unwrap())
            }
        }
    }

    /// Get connection info for creating clients in spawned tasks.
    pub async fn connect_info(&self, export_name: &str) -> ConnectInfo {
        match self.transport {
            Transport::Nbd => ConnectInfo::Nbd {
                addr: self.addr,
                export_name: export_name.to_string(),
            },
            #[cfg(target_os = "linux")]
            Transport::NbdKernel => {
                let nbd = self.nbd_kernel.as_ref().expect("nbd kernel state missing");
                let dev_path = {
                    let manager = nbd.manager.lock().await;
                    manager
                        .get_device_path(export_name)
                        .unwrap_or_else(|| {
                            panic!("no nbd kernel device for export '{export_name}'")
                        })
                        .to_path_buf()
                };
                let handler = self
                    .router
                    .get_handler(export_name)
                    .await
                    .unwrap_or_else(|| panic!("no handler for export '{export_name}'"));
                ConnectInfo::NbdKernel {
                    dev_path,
                    export_size: handler.device_size(),
                }
            }
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            Transport::Ublk => {
                let ublk = self.ublk.as_ref().expect("ublk state missing");
                let paths = ublk.dev_paths.lock().await;
                let dev_path = paths
                    .get(export_name)
                    .unwrap_or_else(|| panic!("no ublk device for export '{export_name}'"))
                    .clone();
                let handler = self
                    .router
                    .get_handler(export_name)
                    .await
                    .unwrap_or_else(|| panic!("no handler for export '{export_name}'"));
                ConnectInfo::Ublk {
                    dev_path,
                    export_size: handler.device_size(),
                }
            }
        }
    }

    /// Drain all exports and shut down gracefully.
    pub async fn shutdown(self) {
        // Shut down kernel devices first (they hold handler/router refs).
        #[cfg(target_os = "linux")]
        if let Some(nbd) = self.nbd_kernel {
            let manager = nbd.manager.into_inner();
            if let Err(e) = manager.shutdown().await {
                eprintln!("nbd kernel shutdown error: {e}");
            }
        }

        #[cfg(all(target_os = "linux", feature = "ublk"))]
        if let Some(ublk) = self.ublk {
            let server = ublk.server.into_inner();
            if let Err(e) = server.shutdown().await {
                eprintln!("ublk shutdown error: {e}");
            }
        }

        if let Err(e) = self.router.shutdown().await {
            eprintln!("Router shutdown error: {}", e);
        }
        self.shutdown.cancel();
    }
}

// ---------------------------------------------------------------------------
// Test modules (declared after macro so transport_test! is in scope)
// ---------------------------------------------------------------------------

mod cold_wake;
mod concurrent;
mod data_integrity;
mod export_discovery;
mod fork_roundtrip;
mod live_migration;
mod multi_export;
mod persistence;
mod range_reads;
mod resize;
mod write_read;
