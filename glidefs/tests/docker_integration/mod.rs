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
            #[tokio::test(flavor = "multi_thread")]
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
// TestContext — shared MinIO container, isolated per-test bucket
// ---------------------------------------------------------------------------
//
// Previously each TestContext spawned its own MinIO container. Under
// parallel test execution (cargo's default), N MinIOs competed for host
// resources on CI runners and produced ~40% flake rate: transient S3
// errors that surfaced as EIO on reads, empty discover_exports listings,
// and "ublk read failed". The errors were a real glidefs response to a
// real (test-induced) backend failure mode — not a glidefs bug — but
// the test infrastructure was unintentionally creating it.
//
// Fix: spin up ONE MinIO process-wide and give each TestContext its own
// bucket. Tests run in parallel without container-resource contention;
// each test's S3 namespace remains fully isolated via the unique bucket.

/// Process-wide shared MinIO. Initialized on first `TestContext::new()`
/// call; held alive for the entire test process.
///
/// **Why we can't rely on Drop:** Rust statics never run `Drop` at program
/// exit, so the `ContainerAsync` inside this `OnceCell` is never cleaned
/// up by the testcontainers crate's normal mechanism. testcontainers-rs
/// 0.26 also has no Ryuk-style sidecar reaper. Without intervention,
/// every test process leaves its MinIO container running forever.
///
/// We work around this by stashing the container ID in a static and
/// registering a libc::atexit handler that shells out to `docker rm -f`
/// at process shutdown. See `register_minio_atexit_cleanup`.
///
/// Limitation: atexit doesn't run on `SIGKILL` / abort. A SIGKILLed test
/// process can still leak its container.
static SHARED_MINIO: tokio::sync::OnceCell<SharedMinio> = tokio::sync::OnceCell::const_new();

/// Monotonic bucket counter so concurrent tests don't collide.
static BUCKET_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Container IDs registered for atexit cleanup. We use a `Mutex<Vec<_>>`
/// rather than `OnceLock<String>` for forward-compatibility with multiple
/// shared containers; today there's only ever one.
static ATEXIT_CONTAINER_IDS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
static ATEXIT_REGISTERED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

extern "C" fn atexit_cleanup_containers() {
    // atexit runs after main() returns. The tokio runtime is gone by now,
    // so we can't use the testcontainers async API — just shell out.
    let ids = match ATEXIT_CONTAINER_IDS.lock() {
        Ok(mut g) => std::mem::take(&mut *g),
        Err(_) => return,
    };
    for id in ids {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

fn register_minio_atexit_cleanup(container_id: String) {
    if let Ok(mut ids) = ATEXIT_CONTAINER_IDS.lock() {
        ids.push(container_id);
    }
    if !ATEXIT_REGISTERED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        // SAFETY: atexit_cleanup_containers is `extern "C"` and only touches
        // static state via Mutex/AtomicBool, which is safe in atexit context.
        unsafe { libc::atexit(atexit_cleanup_containers) };
    }
}

struct SharedMinio {
    _container: ContainerAsync<MinIO>,
    endpoint: String,
}

async fn shared_minio() -> &'static SharedMinio {
    SHARED_MINIO
        .get_or_init(|| async {
            let minio = MinIO::default().start().await.unwrap();
            register_minio_atexit_cleanup(minio.id().to_string());
            let host = minio.get_host().await.unwrap();

            // Retry port lookup — Docker can race between container start and
            // port mapping visibility, causing PortNotExposed on loaded machines.
            let port = {
                let mut last_err = None;
                let mut port = None;
                for _ in 0..10 {
                    match minio.get_host_port_ipv4(9000).await {
                        Ok(p) => {
                            port = Some(p);
                            break;
                        }
                        Err(e) => {
                            last_err = Some(e);
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        }
                    }
                }
                port.unwrap_or_else(|| {
                    panic!("MinIO port 9000 not available after retries: {:?}", last_err)
                })
            };

            // Wait for MinIO HTTP to actually answer — `start()` returns
            // before the server is fully accepting requests on heavily
            // loaded CI hosts.
            let endpoint = format!("http://{}:{}", host, port);
            for attempt in 0..20 {
                let probe = minio
                    .exec(ExecCommand::new(vec![
                        "curl",
                        "-sf",
                        "-o",
                        "/dev/null",
                        "http://localhost:9000/minio/health/ready",
                    ]))
                    .await;
                if let Ok(mut r) = probe {
                    let _ = r.stdout_to_vec().await;
                    if r.exit_code().await.unwrap_or(Some(1)) == Some(0) {
                        break;
                    }
                }
                if attempt == 19 {
                    panic!("MinIO never became ready after 20 attempts");
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }

            SharedMinio {
                _container: minio,
                endpoint,
            }
        })
        .await
}

pub struct TestContext {
    pub object_store: Arc<dyn ObjectStore>,
}

impl TestContext {
    /// Get a fresh, isolated S3 namespace inside the process-wide MinIO.
    /// Each call allocates a unique bucket so concurrent tests don't see
    /// each other's data.
    pub async fn new() -> Self {
        let minio = shared_minio().await;
        let id = BUCKET_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // MinIO bucket names: lowercase, 3-63 chars, alphanumeric + hyphen.
        let bucket = format!("test-bucket-{id:06}");

        // Create the bucket via the shared MinIO. Use the container's own
        // curl + SigV4 auth (reqwest basic_auth doesn't work against S3).
        for attempt in 0..10 {
            let result = minio
                ._container
                .exec(ExecCommand::new(vec![
                    "curl".to_string(),
                    "-sf".to_string(),
                    "-X".to_string(),
                    "PUT".to_string(),
                    "--aws-sigv4".to_string(),
                    "aws:amz:us-east-1:s3".to_string(),
                    "-u".to_string(),
                    "minioadmin:minioadmin".to_string(),
                    format!("http://localhost:9000/{bucket}"),
                ]))
                .await;
            match result {
                Ok(mut r) => {
                    let _ = r.stdout_to_vec().await;
                    if r.exit_code().await.unwrap_or(Some(1)) == Some(0) {
                        break;
                    }
                    if attempt >= 9 {
                        panic!(
                            "bucket creation failed for {bucket} after retries (exit {:?})",
                            r.exit_code().await,
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                Err(_) if attempt < 9 => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                Err(e) => panic!("bucket creation exec failed for {bucket}: {e}"),
            }
        }

        let object_store: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(&minio.endpoint)
                .with_region("us-east-1")
                .with_bucket_name(&bucket)
                .with_access_key_id("minioadmin")
                .with_secret_access_key("minioadmin")
                .with_allow_http(true)
                .build()
                .unwrap(),
        );

        Self { object_store }
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
    /// Owns the cache dir lifetime. None = externally managed (won't delete on drop).
    pub _cache_dir: Option<TempDir>,
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
                pack_index_cache: None,
                wal_sync: false,
                max_s3_uploads: 128,
                max_s3_downloads: 512,
                default_flush_threshold: glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 1,
                nbd_dead_conn_timeout: 0,
                max_exports: 10_000,
                manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
                profile: None,
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
            _cache_dir: Some(cache_dir),
            _server_handle: server_handle,
            #[cfg(target_os = "linux")]
            nbd_kernel,
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            ublk,
        }
    }

    /// Start a GlideFS server reusing an existing cache directory (for WAL
    /// recovery testing). The cache dir is NOT owned by this server — it won't
    /// be deleted on shutdown.
    pub async fn start_with_cache_dir(
        object_store: Arc<dyn ObjectStore>,
        db_path: &str,
        transport: Transport,
        cache_dir: std::path::PathBuf,
    ) -> Self {
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let shutdown = CancellationToken::new();

        let router = Arc::new(
            ExportRouter::new(RouterConfig {
                object_store,
                db_path: db_path.to_string(),
                cache_dir: cache_dir.clone(),
                block_size: 128 * 1024,
                clean_cache,
                pack_index_cache: None,
                wal_sync: false,
                max_s3_uploads: 128,
                max_s3_downloads: 512,
                default_flush_threshold: glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 1,
                nbd_dead_conn_timeout: 0,
                max_exports: 10_000,
                manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
                profile: None,
            })
            .await
            .expect("failed to create test router"),
        );

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

        for _ in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        #[cfg(target_os = "linux")]
        let nbd_kernel = if matches!(transport, Transport::NbdKernel) {
            Some(NbdKernelState {
                manager: tokio::sync::Mutex::new(
                    glidefs::block::nbd::NbdDeviceManager::new()
                        .with_cache_dir(cache_dir)
                        .with_dead_conn_timeout(0),
                ),
            })
        } else {
            None
        };

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
            _cache_dir: None, // externally managed
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
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
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
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
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
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
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
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
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
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
        };
        self.router
            .create_export(config, false, Some(source_manifest), None)
            .await
            .unwrap();
        self.register_nbd_kernel_device(name).await;
        self.register_ublk_device(name).await;
    }

    /// Fork an export from a specific snapshot sequence.
    pub async fn fork_export_from_snapshot(
        &self,
        name: &str,
        source_manifest: &str,
        size_gb: f64,
        snapshot_sequence: u64,
    ) {
        let config = ExportConfig {
            name: name.to_string(),
            size_gb,
            s3_prefix: Some(source_manifest.to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
        };
        self.router
            .create_export(config, false, Some(source_manifest), Some(snapshot_sequence))
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
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
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

    /// Simulate a process crash: abort the server task and drop the router
    /// without draining dirty blocks. Background flush schedulers exit once
    /// they detect the dropped shutdown channel. Data remains on the SSD
    /// for WAL recovery by the next server instance.
    pub async fn crash_shutdown(self) {
        // Stop flush schedulers so they release cache file handles.
        // Deliberately does NOT drain — dirty blocks stay on SSD for
        // WAL-based recovery, matching what happens in a real crash
        // (process dies, OS closes fds, nothing is flushed to S3).
        self.router.stop_flush_schedulers().await;
        self.shutdown.cancel();
        self._server_handle.abort();
        let _ = self._server_handle.await;
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

mod bless_api;
mod bless_integrity;
mod bottomless;
mod cold_wake;
mod concurrent;
mod data_integrity;
mod device_stability;
mod export_discovery;
mod ext4_verify;
mod fork_roundtrip;
mod fs_crash_recovery;
mod integrity_suite;
mod live_migration;
mod multi_export;
mod oci_distribution;
mod oci_layer_dedup;
mod oci_push;
mod persistence;
mod range_reads;
mod resize;
mod transport_stress;
mod write_read;
