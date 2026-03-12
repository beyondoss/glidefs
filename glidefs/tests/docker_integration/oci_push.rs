//! Integration tests for the OCI push pipeline against a real Docker registry.
//!
//! Spins up `registry:2` via testcontainers, creates in-memory BlockHandlers
//! with ext4 data, pushes to the local registry, pulls back and verifies tar
//! contents.
//!
//! Run: `cargo test --features docker-tests --test docker_integration oci_push`

use std::io::Read as _;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use futures::StreamExt;
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
use glidefs::block::pack::DEFAULT_BLOCKS_PER_PACK;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};
use glidefs::oci::{push_delta_image, push_image, BlockAdapter};
use object_store::memory::InMemory;
use oci_registry::{ClientConfig, ClientProtocol, Credentials, OciDescriptor, Reference, RegistryClient};

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

    // Retry port lookup — Docker can race between container start and
    // port mapping visibility.
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

    // Wait for the registry to accept TCP connections.
    let addr = format!("{}:{}", host, port);
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    (container, addr)
}

/// Create a `RegistryClient` configured for plain HTTP (local registry).
fn http_client() -> RegistryClient {
    RegistryClient::with_config(ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    })
}

/// Create a test `BlockHandler` backed by an in-memory object store.
async fn test_handler() -> (Arc<BlockHandler>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: "oci-push-test".to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false, bottomless: false,
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
        DEFAULT_BLOCKS_PER_PACK,
        None,
    );

    (Arc::new(handler), temp_dir)
}

/// Write files to a BlockHandler as ext4 via BlockAdapter + Writer.
///
/// `files` is a list of `(path, data)` tuples. Parent directories are
/// auto-created. Must be called from an async context (uses spawn_blocking).
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

        // Auto-create parent directories.
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

/// Pull a layer, decompress, and parse tar entries.
///
/// Returns `Vec<(path, data)>`, skipping PAX headers.
async fn pull_and_parse_layer(
    client: &RegistryClient,
    image: &Reference,
    layer: &OciDescriptor,
    auth: &Credentials,
) -> Vec<(String, Vec<u8>)> {
    let stream = client.pull_layer(image, layer, auth).await.unwrap();
    let mut stream = std::pin::pin!(stream);

    let mut compressed = Vec::new();
    while let Some(chunk) = stream.next().await {
        compressed.extend_from_slice(&chunk.unwrap());
    }

    let decoder = flate2::read::GzDecoder::new(&compressed[..]);
    let mut archive = tar::Archive::new(decoder);
    let mut entries = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        if entry.header().entry_type() == tar::EntryType::XHeader {
            continue;
        }
        let path = entry.path().unwrap().to_string_lossy().to_string();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        entries.push((path, data));
    }
    entries
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Push a single-layer image and verify contents via pull.
#[tokio::test]
async fn test_push_image_roundtrip() {
    let (_registry, addr) = start_registry().await;
    let client = http_client();
    let auth = Credentials::Anonymous;

    let (handler, _tmp) = test_handler().await;
    write_ext4_files(&handler, &[
        ("hello.txt", b"hello world"),
        ("app/config.json", b"{\"key\": \"value\"}"),
    ])
    .await;

    let image: Reference = format!("{}/test/push-roundtrip:v1", addr).parse().unwrap();
    let result = push_image(&client, &image, &auth, Arc::clone(&handler))
        .await
        .unwrap();

    assert!(!result.manifest_digest.is_empty(), "manifest digest must be set");
    assert!(!result.layer_digest.is_empty(), "layer digest must be set");
    assert!(result.layer_size > 0, "layer size must be positive");

    // Resolve the pushed image.
    let resolved = client.resolve(&image, &auth).await.unwrap();
    assert_eq!(resolved.layers.len(), 1, "single-layer image");
    assert_eq!(
        resolved.layers[0].media_type,
        "application/vnd.oci.image.layer.v1.tar+gzip"
    );

    // Config should have exactly 1 diff_id.
    let config: serde_json::Value = serde_json::from_slice(&resolved.config).unwrap();
    let diff_ids = config["rootfs"]["diff_ids"].as_array().unwrap();
    assert_eq!(diff_ids.len(), 1, "config should have 1 diff_id");

    // Pull layer and verify file content.
    let entries = pull_and_parse_layer(&client, &image, &resolved.layers[0], &auth).await;

    let hello = entries.iter().find(|(p, _)| p == "hello.txt");
    assert!(hello.is_some(), "hello.txt missing from tar; entries: {:?}", entries.iter().map(|(p, _)| p).collect::<Vec<_>>());
    assert_eq!(hello.unwrap().1, b"hello world");

    let config_entry = entries.iter().find(|(p, _)| p == "app/config.json");
    assert!(config_entry.is_some(), "app/config.json missing from tar; entries: {:?}", entries.iter().map(|(p, _)| p).collect::<Vec<_>>());
    assert_eq!(config_entry.unwrap().1, b"{\"key\": \"value\"}");
}

/// Push a two-layer delta image and verify both layers.
#[tokio::test]
async fn test_push_delta_image_roundtrip() {
    let (_registry, addr) = start_registry().await;
    let client = http_client();
    let auth = Credentials::Anonymous;

    // Base: a.txt + b.txt
    let (base_handler, _tmp1) = test_handler().await;
    write_ext4_files(&base_handler, &[
        ("a.txt", b"aaa"),
        ("b.txt", b"bbb"),
    ])
    .await;

    // Target: a.txt modified, b.txt deleted, c.txt added
    let (target_handler, _tmp2) = test_handler().await;
    write_ext4_files(&target_handler, &[
        ("a.txt", b"aaa-modified"),
        ("c.txt", b"ccc"),
    ])
    .await;

    let image: Reference = format!("{}/test/delta-roundtrip:v1", addr).parse().unwrap();
    let result = push_delta_image(
        &client,
        &image,
        &auth,
        Arc::clone(&base_handler),
        Arc::clone(&target_handler),
    )
    .await
    .unwrap();

    assert!(!result.manifest_digest.is_empty());

    // Resolve: should have 2 layers.
    let resolved = client.resolve(&image, &auth).await.unwrap();
    assert_eq!(resolved.layers.len(), 2, "delta image should have 2 layers");

    // Config should have 2 diff_ids.
    let config: serde_json::Value = serde_json::from_slice(&resolved.config).unwrap();
    let diff_ids = config["rootfs"]["diff_ids"].as_array().unwrap();
    assert_eq!(diff_ids.len(), 2, "config should have 2 diff_ids");

    // Layer 0 (base): should contain a.txt and b.txt with original content.
    let base_entries =
        pull_and_parse_layer(&client, &image, &resolved.layers[0], &auth).await;
    let a_base = base_entries.iter().find(|(p, _)| p == "a.txt");
    assert!(a_base.is_some(), "base layer missing a.txt");
    assert_eq!(a_base.unwrap().1, b"aaa");
    let b_base = base_entries.iter().find(|(p, _)| p == "b.txt");
    assert!(b_base.is_some(), "base layer missing b.txt");
    assert_eq!(b_base.unwrap().1, b"bbb");

    // Layer 1 (delta): should contain modified a.txt, added c.txt, whiteout for b.txt.
    let delta_entries =
        pull_and_parse_layer(&client, &image, &resolved.layers[1], &auth).await;
    let a_delta = delta_entries.iter().find(|(p, _)| p == "a.txt");
    assert!(a_delta.is_some(), "delta layer missing a.txt");
    assert_eq!(a_delta.unwrap().1, b"aaa-modified");
    let c_delta = delta_entries.iter().find(|(p, _)| p == "c.txt");
    assert!(c_delta.is_some(), "delta layer missing c.txt");
    assert_eq!(c_delta.unwrap().1, b"ccc");
    let whiteout = delta_entries.iter().find(|(p, _)| p == ".wh.b.txt");
    assert!(
        whiteout.is_some(),
        "delta layer missing .wh.b.txt whiteout; entries: {:?}",
        delta_entries.iter().map(|(p, _)| p).collect::<Vec<_>>()
    );
}

/// Push the same image twice — second push should succeed (idempotent blobs).
#[tokio::test]
async fn test_push_idempotent() {
    let (_registry, addr) = start_registry().await;
    let client = http_client();
    let auth = Credentials::Anonymous;

    let (handler, _tmp) = test_handler().await;
    write_ext4_files(&handler, &[("data.txt", b"some data")]).await;

    let image: Reference = format!("{}/test/idempotent:v1", addr).parse().unwrap();

    let result1 = push_image(&client, &image, &auth, Arc::clone(&handler))
        .await
        .unwrap();
    let result2 = push_image(&client, &image, &auth, Arc::clone(&handler))
        .await
        .unwrap();

    assert_eq!(result1.layer_digest, result2.layer_digest);
}
