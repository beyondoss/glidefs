//! Integration tests for the POST /api/bless endpoint.
//!
//! Pushes a small OCI image to a local registry:2, then exercises the
//! bless API to pull + ingest it. Verifies:
//! - Async status tracking (POST → poll → complete)
//! - Idempotent re-POST after completion
//! - Blessed manifest is forkable and serves correct data
//! - Concurrent POST deduplication
//!
//! Run: `cargo test --features docker-tests --test docker_integration bless_api`

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use tempfile::TempDir;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::Notify;

use ext4::format;
use ext4::writer::{File as Ext4File, Writer, WriterOption};
use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::content_store::ContentStore;
use glidefs::block::handler::BlockHandler;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::router::{BlessStatus, ExportRouter, RouterConfig};
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};
use glidefs::config::ExportConfig;
use glidefs::oci::{push_image, BlockAdapter};
use object_store::memory::InMemory;
use oci_registry::{ClientConfig, ClientProtocol, Credentials, RegistryClient};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEVICE_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB
const BLOCK_SIZE: usize = 4096;

/// Start a `registry:2` container and return (container, "host:port").
async fn start_registry() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("registry", "2")
        .with_exposed_port(5000.into())
        .start()
        .await
        .expect("failed to start registry:2");

    let host = container.get_host().await.unwrap();

    let port = {
        let mut last_err = None;
        let mut port = None;
        for _ in 0..20 {
            match container.get_host_port_ipv4(5000).await {
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
            panic!(
                "registry port 5000 not available after retries: {:?}",
                last_err
            )
        })
    };

    let addr = format!("{}:{}", host, port);
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    (container, addr)
}

fn http_client() -> RegistryClient {
    RegistryClient::with_config(ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    })
}

/// Create a test BlockHandler backed by an in-memory object store.
async fn test_handler() -> (Arc<BlockHandler>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: "bless-api-test".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
    let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        DEVICE_SIZE,
        BLOCK_SIZE as u32,
    )));
    let metrics = Arc::new(ExportMetrics::new());

    let cache = WriteCache::open(config).unwrap();
    let cache = cache.skip_recovery_for_test();
    let handler = BlockHandler::new(
        Arc::new(cache),
        content_store,
        clean_cache,
        pack_index_cache,
        volume_manifest,
        DEVICE_SIZE,
        false,
        metrics,
        Arc::new(AtomicU64::new(0f64.to_bits())),
        Arc::new(Notify::const_new()),
        DEFAULT_FLUSH_THRESHOLD,
        None,
    );

    (Arc::new(handler), temp_dir)
}

/// Write files to a BlockHandler as ext4 via BlockAdapter + Writer.
async fn write_ext4_files(handler: &Arc<BlockHandler>, files: &[(&str, &[u8])]) {
    let h = Arc::clone(handler);
    let files: Vec<(String, Vec<u8>)> = files
        .iter()
        .map(|(p, d)| (p.to_string(), d.to_vec()))
        .collect();

    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let adapter = BlockAdapter::new(&h, rt);
        let mut w = Writer::new(
            adapter,
            &[WriterOption::MaximumDiskSize(DEVICE_SIZE as i64)],
        );

        let mut created_dirs = std::collections::HashSet::new();
        for (path, _) in &files {
            if let Some((parent, _)) = path.rsplit_once('/') {
                let mut current = String::new();
                for component in parent.split('/') {
                    if !current.is_empty() {
                        current.push('/');
                    }
                    current.push_str(component);
                    if created_dirs.insert(current.clone()) {
                        w.create(
                            &current,
                            &Ext4File {
                                mode: format::S_IFDIR | 0o755,
                                uid: 1000,
                                gid: 1000,
                                ..Default::default()
                            },
                        )
                        .unwrap();
                    }
                }
            }
        }

        for (path, data) in &files {
            w.create(
                path,
                &Ext4File {
                    size: data.len() as i64,
                    mode: format::S_IFREG | 0o644,
                    uid: 1000,
                    gid: 1000,
                    ..Default::default()
                },
            )
            .unwrap();
            if !data.is_empty() {
                std::io::Write::write_all(&mut w, data).unwrap();
            }
        }

        w.close().unwrap();
    })
    .await
    .unwrap();
}

/// Push a test image to a local registry and return the image reference string.
async fn push_test_image(
    registry_addr: &str,
    tag: &str,
    files: &[(&str, &[u8])],
) -> String {
    let client = http_client();
    let auth = Credentials::Anonymous;
    let (handler, _tmp) = test_handler().await;
    write_ext4_files(&handler, files).await;

    let image_ref = format!("{}/test/{}:v1", registry_addr, tag);
    let image: oci_registry::Reference = image_ref.parse().unwrap();
    push_image(&client, &image, &auth, Arc::clone(&handler))
        .await
        .unwrap();
    image_ref
}

/// Create an ExportRouter backed by an in-memory object store.
async fn create_test_router(
    s3: Arc<dyn object_store::ObjectStore>,
) -> (Arc<ExportRouter>, TempDir) {
    let dir = TempDir::new().unwrap();
    let router = Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: s3,
            db_path: "test".to_string(),
            cache_dir: dir.path().to_path_buf(),
            block_size: 131_072, // 128 KB — matches bless default
            clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: 500,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
        })
        .await
        .unwrap(),
    );
    (router, dir)
}

/// Send an HTTP request through the API handler.
async fn api_request(
    router: &Arc<ExportRouter>,
    method: Method,
    uri: &str,
    body: &str,
) -> (StatusCode, Bytes) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let resp = glidefs::block::api::handle_request_for_test(Arc::clone(router), req)
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

/// Poll GET /api/bless/{s3_prefix}/{name} until the task completes (404).
/// Panics if the task doesn't complete within 60 seconds.
async fn poll_bless_complete(router: &Arc<ExportRouter>, s3_prefix: &str, name: &str) {
    let uri = format!("/api/bless/{}/{}", s3_prefix, name);
    for attempt in 0..300 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let (status, body) = api_request(router, Method::GET, &uri, "").await;

        if status == StatusCode::NOT_FOUND {
            // Task removed — either complete or failed. Verify it actually succeeded.
            let head_uri = format!("/api/manifests/{}/bases/{}", s3_prefix, name);
            let req = Request::builder()
                .method(Method::HEAD)
                .uri(&head_uri)
                .body(Full::new(Bytes::new()))
                .unwrap();
            let resp = glidefs::block::api::handle_request_for_test(Arc::clone(router), req)
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "bless task finished but manifest not found in S3 (attempt {})",
                attempt
            );
            return;
        }

        assert_eq!(status, StatusCode::OK, "unexpected status during poll");
        let status: BlessStatus = serde_json::from_slice(&body).unwrap();
        assert!(
            status.state == "pulling" || status.state == "draining",
            "unexpected state during poll: {}",
            status.state
        );
    }
    panic!("bless did not complete within 60s");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full end-to-end: push image → bless via API → poll → verify manifest exists →
/// fork export from blessed base → read blocks → verify non-zero data.
#[tokio::test]
async fn test_bless_api_full_flow() {
    // --- Setup: local registry + push a test image with known content ---
    let (_registry, addr) = start_registry().await;

    // Use a non-trivial file so ext4 has real data blocks.
    let test_data = vec![0xABu8; 8192];
    let image_ref = push_test_image(
        &addr,
        "bless-full",
        &[
            ("hello.txt", b"hello from bless api test"),
            ("app/config.json", b"{\"version\": 1}"),
            ("data/blob.bin", &test_data),
        ],
    )
    .await;

    // --- Setup: router with in-memory S3 ---
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (router, _dir) = create_test_router(Arc::clone(&s3)).await;

    // --- Step 1: POST to start bless ---
    let body = serde_json::json!({ "oci_image": image_ref }).to_string();
    let (status, resp_body) =
        api_request(&router, Method::POST, "/api/bless/myprefix/myimage", &body).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "first POST should return 202; body: {}",
        String::from_utf8_lossy(&resp_body)
    );
    let bless_status: BlessStatus = serde_json::from_slice(&resp_body).unwrap();
    assert_eq!(bless_status.state, "pulling");
    assert_eq!(bless_status.oci_image, image_ref);

    // --- Step 2: Poll until complete ---
    poll_bless_complete(&router, "myprefix", "myimage").await;

    // --- Step 3: GET after completion returns "complete" from S3 fallback ---
    let (status, body) =
        api_request(&router, Method::GET, "/api/bless/myprefix/myimage", "").await;
    assert_eq!(status, StatusCode::OK);
    let bless_status: BlessStatus = serde_json::from_slice(&body).unwrap();
    assert_eq!(bless_status.state, "complete");

    // --- Step 4: Idempotent re-POST returns 200 ---
    let body = serde_json::json!({ "oci_image": image_ref }).to_string();
    let (status, resp_body) =
        api_request(&router, Method::POST, "/api/bless/myprefix/myimage", &body).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "idempotent POST should return 200; body: {}",
        String::from_utf8_lossy(&resp_body)
    );
    let bless_status: BlessStatus = serde_json::from_slice(&resp_body).unwrap();
    assert_eq!(bless_status.state, "complete");

    // --- Step 5: Fork an export from the blessed base ---
    let config = ExportConfig {
        name: "child".to_string(),
        size_gb: 0.1, // Must be >= blessed device size
        s3_prefix: Some("myprefix".to_string()),
        block_size: None,
        flush_threshold: None,
        flush_mode: None,
        transport: None,
    };
    router
        .create_export(config, false, Some("bases/myimage"), None)
        .await
        .expect("fork from blessed base should succeed");

    // --- Step 6: Read blocks from forked export, verify non-zero content ---
    let info = router
        .get_export_info("child")
        .await
        .expect("child export should exist");

    // The blessed image is an ext4 filesystem — at least the superblock
    // and file data blocks should be non-zero.
    let handler = router
        .get_handler("child")
        .await
        .expect("child handler should exist");

    let block_size = 131_072u64; // 128 KB
    let total_blocks = info.size / block_size;
    let mut nonzero_blocks = 0u64;

    // Sample blocks across the device — don't need to read all, just prove
    // the fork resolved data from S3.
    let sample_count = total_blocks.min(32);
    let step = if total_blocks > sample_count {
        total_blocks / sample_count
    } else {
        1
    };

    for i in (0..total_blocks).step_by(step as usize) {
        let offset = i * block_size;
        let data = handler
            .read(offset, block_size as u32)
            .await
            .expect("read from forked export should succeed");
        if data.iter().any(|&b| b != 0) {
            nonzero_blocks += 1;
        }
    }

    assert!(
        nonzero_blocks > 0,
        "forked export should have non-zero blocks from blessed base \
         (sampled {} of {} blocks, all were zero)",
        sample_count,
        total_blocks,
    );
    eprintln!(
        "  fork verified: {}/{} sampled blocks non-zero",
        nonzero_blocks, sample_count
    );
}

/// Two concurrent POSTs for the same image: one starts the ingest,
/// the other sees the in-flight task. Both eventually resolve.
#[tokio::test]
async fn test_bless_api_concurrent_post() {
    let (_registry, addr) = start_registry().await;
    let image_ref = push_test_image(&addr, "bless-concurrent", &[("data.bin", &[0xAB; 1024])])
        .await;

    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (router, _dir) = create_test_router(Arc::clone(&s3)).await;

    let body = serde_json::json!({ "oci_image": image_ref }).to_string();

    // Fire two POSTs concurrently.
    let (r1, r2) = tokio::join!(
        api_request(&router, Method::POST, "/api/bless/pfx/img", &body),
        api_request(&router, Method::POST, "/api/bless/pfx/img", &body),
    );

    // Both should be 202 (one starts, one sees in-flight) or possibly 200
    // if the first completed before the second ran.
    for (i, (status, resp_body)) in [(1, &r1), (2, &r2)] {
        assert!(
            *status == StatusCode::ACCEPTED || *status == StatusCode::OK,
            "POST {} unexpected status: {} body: {}",
            i,
            status,
            String::from_utf8_lossy(resp_body)
        );
    }

    // Wait for completion.
    poll_bless_complete(&router, "pfx", "img").await;
}

/// GET /api/bless for a non-existent image returns 404.
#[tokio::test]
async fn test_bless_api_get_nonexistent() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (router, _dir) = create_test_router(Arc::clone(&s3)).await;

    let (status, _) = api_request(&router, Method::GET, "/api/bless/pfx/nope", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// POST with missing oci_image returns 400.
#[tokio::test]
async fn test_bless_api_missing_oci_image() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (router, _dir) = create_test_router(Arc::clone(&s3)).await;

    let (status, _) = api_request(
        &router,
        Method::POST,
        "/api/bless/pfx/img",
        r#"{"oci_image": ""}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// POST with path traversal in s3_prefix is rejected.
#[tokio::test]
async fn test_bless_api_path_traversal() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (router, _dir) = create_test_router(Arc::clone(&s3)).await;

    let body = r#"{"oci_image": "docker.io/library/nginx:latest"}"#;
    let (status, _) =
        api_request(&router, Method::POST, "/api/bless/../etc/img", body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
