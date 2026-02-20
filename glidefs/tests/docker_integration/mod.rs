//! Docker-based integration tests for GlideFS.
//!
//! Uses testcontainers to spin up MinIO as a real S3 backend and a lightweight
//! NBD TCP client to exercise the full server stack. Runs on any machine with
//! Docker — no kernel NBD or ZFS needed.
//!
//! Run: `cargo test --features docker-tests --test docker_integration`

mod nbd_client;

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

use std::net::SocketAddr;
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
        let port = minio.get_host_port_ipv4(9000).await.unwrap();
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
// TestServer — in-process GlideFS NBD server
// ---------------------------------------------------------------------------

pub struct TestServer {
    pub router: Arc<ExportRouter>,
    pub addr: SocketAddr,
    pub shutdown: CancellationToken,
    pub _cache_dir: TempDir,
    _server_handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// Start a GlideFS NBD server on a random port.
    pub async fn start(object_store: Arc<dyn ObjectStore>, db_path: &str) -> Self {
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
            })
            .expect("failed to create test router"),
        );

        // Pre-bind to get a random port, then release for the server
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

        // Wait for the server to be ready
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        Self {
            router,
            addr,
            shutdown,
            _cache_dir: cache_dir,
            _server_handle: server_handle,
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
            .create_export(config, false, None)
            .await
            .unwrap();
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
            .create_export(config, false, Some(name))
            .await
            .unwrap();
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
            .create_export(config, false, Some(name))
            .await
            .unwrap();
    }

    /// Snapshot an export (flush dirty blocks + upload manifest).
    pub async fn snapshot_export(&self, name: &str) -> SnapshotResponse {
        self.router.snapshot_export(name).await.unwrap()
    }

    /// Fork an export from a source manifest (read-write).
    ///
    /// The child export uses the source's S3 prefix so it can read the
    /// parent's packs and manifests (content-addressed storage is shared).
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
            .create_export(config, false, Some(source_manifest))
            .await
            .unwrap();
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
            .create_export(config, true, Some(source_manifest))
            .await
            .unwrap();
    }

    /// Promote a readonly export to read-write.
    pub async fn promote_export(&self, name: &str) {
        self.router.promote_export(name).await.unwrap();
    }

    /// Resize an export (grow only). Client must reconnect to see new size.
    pub async fn resize_export(&self, name: &str, new_size_gb: f64) {
        self.router.resize_export(name, new_size_gb).await.unwrap();
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
    pub async fn connect(&self, export_name: &str) -> nbd_client::NbdClient {
        nbd_client::NbdClient::connect(self.addr, export_name)
            .await
            .unwrap()
    }

    /// Drain all exports and shut down gracefully.
    pub async fn shutdown(self) {
        if let Err(e) = self.router.shutdown().await {
            eprintln!("Router shutdown error: {}", e);
        }
        self.shutdown.cancel();
    }
}
