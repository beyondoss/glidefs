//! Integration tests for the OCI distribution pipeline against MinIO.
//!
//! Creates an ext4 filesystem, publishes OCI artifacts to MinIO (real S3),
//! serves those artifacts via a minimal OCI HTTP server, and pulls back
//! using `oci_registry::RegistryClient` to verify the full roundtrip.
//!
//! Run: `cargo test --features docker-tests --test docker_integration oci_distribution`

use std::convert::Infallible;
use std::io::Read as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use futures::StreamExt;
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use ext4::format;
use ext4::writer::{File as Ext4File, Writer, WriterOption};
use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::content_store::ContentStore;
use glidefs::block::handler::BlockHandler;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};
use glidefs::oci::{BlockAdapter, export_full_layer, hex_sha256};
use object_store::memory::InMemory;
use oci_registry::{ClientConfig, ClientProtocol, Credentials, Reference, RegistryClient};

use super::TestContext;

const DEVICE_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB
const BLOCK_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a test `BlockHandler` backed by an in-memory object store.
async fn test_handler() -> (Arc<BlockHandler>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: "oci-dist-test".to_string(),
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

fn http_client() -> RegistryClient {
    RegistryClient::with_config(ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    })
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

// ---------------------------------------------------------------------------
// Publish OCI artifacts to S3
// ---------------------------------------------------------------------------

/// Export ext4 data as a compressed tar layer and upload OCI artifacts to S3.
///
/// Returns `(manifest_digest, layer_digest)`.
async fn publish_oci_artifacts(
    store: &Arc<dyn ObjectStore>,
    handler: Arc<BlockHandler>,
    oci_base: &str,
    tag: &str,
) -> (String, String) {
    // Export layer on a blocking thread.
    let layer = tokio::task::spawn_blocking(move || export_full_layer(handler))
        .await
        .unwrap()
        .unwrap();

    // Upload layer blob.
    let layer_data = tokio::fs::read(layer.temp_file.path()).await.unwrap();
    let blob_path = ObjectPath::from(format!("{}/blobs/sha256/{}", oci_base, layer.digest));
    store.put(&blob_path, layer_data.into()).await.unwrap();

    // Build + upload config blob.
    let config_json = serde_json::json!({
        "architecture": "amd64",
        "os": "linux",
        "rootfs": {
            "type": "layers",
            "diff_ids": [format!("sha256:{}", layer.diff_id)]
        },
        "config": {}
    });
    let config_bytes = serde_json::to_vec(&config_json).unwrap();
    let config_digest = hex_sha256(&config_bytes);
    let config_path = ObjectPath::from(format!("{}/blobs/sha256/{}", oci_base, config_digest));
    store
        .put(&config_path, config_bytes.clone().into())
        .await
        .unwrap();

    // Build + upload manifest (by tag and by digest).
    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{}", config_digest),
            "size": config_bytes.len()
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": format!("sha256:{}", layer.digest),
            "size": layer.size
        }]
    });
    let manifest_bytes = serde_json::to_vec(&manifest_json).unwrap();
    let manifest_digest = hex_sha256(&manifest_bytes);

    let tag_path = ObjectPath::from(format!("{}/manifests/{}", oci_base, tag));
    let digest_path =
        ObjectPath::from(format!("{}/manifests/sha256/{}", oci_base, manifest_digest));
    store
        .put(&tag_path, manifest_bytes.clone().into())
        .await
        .unwrap();
    store
        .put(&digest_path, manifest_bytes.into())
        .await
        .unwrap();

    (manifest_digest, layer.digest)
}

// ---------------------------------------------------------------------------
// Minimal OCI HTTP server (backed by object_store)
// ---------------------------------------------------------------------------

struct OciServerState {
    object_store: Arc<dyn ObjectStore>,
    oci_base: String,
    name: String,
}

/// Start a minimal OCI registry HTTP server backed by the given object store.
///
/// Returns `(listen_addr, join_handle)`. The server runs until the handle is
/// aborted.
async fn start_oci_server(
    store: Arc<dyn ObjectStore>,
    oci_base: String,
    name: String,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let state = Arc::new(OciServerState {
        object_store: store,
        oci_base,
        name,
    });

    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| {
                    let state = Arc::clone(&state);
                    async move { oci_handler(req, &state).await }
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });

    // Wait for server to accept connections.
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    (addr, handle)
}

async fn oci_handler<B>(
    req: Request<B>,
    state: &OciServerState,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let method = req.method().clone();

    let response = match (method, parts.as_slice()) {
        // GET /v2/ — API version check
        (Method::GET, ["v2"]) => Response::builder()
            .status(StatusCode::OK)
            .header("Docker-Distribution-API-Version", "registry/2.0")
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from("{}")))
            .unwrap(),

        // GET/HEAD /v2/{name}/manifests/{ref}
        (ref m, ["v2", name, "manifests", reference])
            if *name == state.name && (m == Method::GET || m == Method::HEAD) =>
        {
            let s3_path = if let Some(hex) = reference.strip_prefix("sha256:") {
                ObjectPath::from(format!("{}/manifests/sha256/{}", state.oci_base, hex))
            } else {
                ObjectPath::from(format!("{}/manifests/{}", state.oci_base, reference))
            };

            match state.object_store.get(&s3_path).await {
                Ok(result) => {
                    let data = result.bytes().await.unwrap();
                    let digest = format!("sha256:{}", sha256_hex(&data));
                    let builder = Response::builder()
                        .status(StatusCode::OK)
                        .header(
                            "Content-Type",
                            "application/vnd.oci.image.manifest.v1+json",
                        )
                        .header("Docker-Content-Digest", &digest)
                        .header("Content-Length", data.len().to_string());

                    if m == Method::HEAD {
                        builder.body(Full::new(Bytes::new())).unwrap()
                    } else {
                        builder.body(Full::new(data)).unwrap()
                    }
                }
                Err(_) => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(
                        r#"{"errors":[{"code":"MANIFEST_UNKNOWN","message":"not found"}]}"#,
                    )))
                    .unwrap(),
            }
        }

        // GET/HEAD /v2/{name}/blobs/sha256:{hex}
        (ref m, ["v2", name, "blobs", digest_ref])
            if *name == state.name && (m == Method::GET || m == Method::HEAD) =>
        {
            let hex = digest_ref.strip_prefix("sha256:").unwrap_or("");
            let s3_path =
                ObjectPath::from(format!("{}/blobs/sha256/{}", state.oci_base, hex));

            match state.object_store.get(&s3_path).await {
                Ok(result) => {
                    let data = result.bytes().await.unwrap();
                    let builder = Response::builder()
                        .status(StatusCode::OK)
                        .header("Docker-Content-Digest", *digest_ref)
                        .header("Content-Length", data.len().to_string());

                    if m == Method::HEAD {
                        builder.body(Full::new(Bytes::new())).unwrap()
                    } else {
                        builder.body(Full::new(data)).unwrap()
                    }
                }
                Err(_) => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(
                        r#"{"errors":[{"code":"BLOB_UNKNOWN","message":"not found"}]}"#,
                    )))
                    .unwrap(),
            }
        }

        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"errors":[{"code":"NAME_UNKNOWN","message":"not found"}]}"#,
            )))
            .unwrap(),
    };

    Ok(response)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full roundtrip: create ext4 → publish OCI artifacts to MinIO → serve via
/// HTTP → pull via RegistryClient → decompress → verify tar contents.
#[tokio::test]
async fn test_publish_serve_pull_roundtrip() {
    let ctx = TestContext::new().await;

    // Create handler and write ext4 files.
    let (handler, _tmp) = test_handler().await;
    write_ext4_files(
        &handler,
        &[
            ("hello.txt", b"distribution test content"),
            ("app/config.json", b"{\"env\": \"test\"}"),
        ],
    )
    .await;

    // Publish OCI artifacts to MinIO.
    let oci_base = "db/exports/myexport/oci";
    let (manifest_digest, layer_digest) =
        publish_oci_artifacts(&ctx.object_store, Arc::clone(&handler), oci_base, "v1").await;

    assert!(!manifest_digest.is_empty());
    assert!(!layer_digest.is_empty());

    // Start OCI HTTP server backed by MinIO.
    let (addr, server_handle) = start_oci_server(
        Arc::clone(&ctx.object_store),
        oci_base.to_string(),
        "myexport".to_string(),
    )
    .await;

    // Pull via RegistryClient (plain HTTP).
    let client = http_client();
    let image: Reference = format!("{}:{}/myexport:v1", addr.ip(), addr.port())
        .parse()
        .unwrap();

    let resolved = client
        .resolve(&image, &Credentials::Anonymous)
        .await
        .unwrap();
    assert_eq!(resolved.layers.len(), 1, "single-layer image");
    assert_eq!(
        resolved.layers[0].media_type,
        "application/vnd.oci.image.layer.v1.tar+gzip"
    );

    // Verify config has correct diff_id.
    let config: serde_json::Value = serde_json::from_slice(&resolved.config).unwrap();
    let diff_ids = config["rootfs"]["diff_ids"].as_array().unwrap();
    assert_eq!(diff_ids.len(), 1, "config should have 1 diff_id");

    // Pull layer, decompress, parse tar, verify file contents.
    let layer = &resolved.layers[0];
    let stream = client
        .pull_layer(&image, layer, &Credentials::Anonymous)
        .await
        .unwrap();
    let mut stream = std::pin::pin!(stream);
    let mut compressed = Vec::new();
    while let Some(chunk) = stream.next().await {
        compressed.extend_from_slice(&chunk.unwrap());
    }

    let decoder = flate2::read::GzDecoder::new(&compressed[..]);
    let mut archive = tar::Archive::new(decoder);
    let mut found_hello = false;
    let mut found_config = false;
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        if entry.header().entry_type() == tar::EntryType::XHeader {
            continue;
        }
        let path = entry.path().unwrap().to_string_lossy().to_string();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        if path == "hello.txt" {
            found_hello = true;
            assert_eq!(data, b"distribution test content");
        }
        if path == "app/config.json" {
            found_config = true;
            assert_eq!(data, b"{\"env\": \"test\"}");
        }
    }
    assert!(found_hello, "hello.txt not found in pulled tar");
    assert!(found_config, "app/config.json not found in pulled tar");

    server_handle.abort();
}
