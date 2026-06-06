//! End-to-end proof that **layered** OCI bless dedups shared layers.
//!
//! Builds three images that share an identical base layer, pushes them to a
//! local `registry:2`, then runs the layered bless (each layer → its own
//! content-addressed ext4 under `layers/{digest}`) and asserts:
//!
//!   1. the shared base layer is stored exactly once (idempotent across images),
//!   2. total layered storage is smaller than merged-bless storage of the same
//!      three images (which can't dedup the shared layer — proven empirically
//!      elsewhere to share 0 blocks),
//!   3. overlay-composing the per-layer ext4s reconstructs the same filesystem
//!      the merged path produces (including a whiteout deletion and an opaque
//!      directory),
//!   4. the image→layer reference invariant GC relies on holds (dropping one
//!      image keeps a layer still referenced by the others).
//!
//! Run: `cargo test --features docker-tests --test docker_integration oci_layer_dedup`

use std::collections::{BTreeMap, HashSet};
use std::io::{Cursor, Write};
use std::sync::Arc;

use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use ext4::format;
use ext4::reader::Reader;
use ext4::tar_convert::{convert_layer_to_ext4, convert_oci_layers_to_ext4, ConvertOptions};
use ext4::writer::WriterOption;

use glidefs::block::content_store::ContentStore;
use glidefs::oci::ext4_store::{deterministic_uuid, store_ext4_stream};
use glidefs::oci::hex_sha256;
use glidefs::oci::layer_store::{
    ensure_layer_stored, get_image_descriptor, layer_base_path, list_image_descriptors,
    put_image_descriptor, ImageDescriptor, StoredLayer,
};
use glidefs::oci::pull::pull_layer_to_tempfile;
use oci_registry::{
    ClientConfig, ClientProtocol, Credentials, OciDescriptor, OciImageManifest, Reference,
    RegistryClient,
};

// ---------------------------------------------------------------------------
// Registry + client (mirrors bless_api.rs / oci_push.rs)
// ---------------------------------------------------------------------------

async fn start_registry() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("registry", "2")
        .with_exposed_port(5000.into())
        .start()
        .await
        .expect("failed to start registry:2");
    let host = container.get_host().await.unwrap();
    let mut port = None;
    for _ in 0..20 {
        if let Ok(p) = container.get_host_port_ipv4(5000).await {
            port = Some(p);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let addr = format!("{}:{}", host, port.expect("registry port"));
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

// ---------------------------------------------------------------------------
// Building & pushing controlled multi-layer images
// ---------------------------------------------------------------------------

/// A tar entry to synthesize a layer.
enum E<'a> {
    File(&'a str, &'a [u8]),
    Dir(&'a str),
    /// OCI whiteout: deletes `path` from lower layers. Stored as `.wh.<name>`.
    Whiteout(&'a str),
    /// OCI opaque whiteout: hides all lower entries in `dir`.
    Opaque(&'a str),
}

/// Build a deterministic (mtime=0) tar from entries.
fn build_tar(entries: &[E]) -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    let mut push = |path: String, data: &[u8], dir: bool| {
        let mut h = tar::Header::new_gnu();
        h.set_path(&path).unwrap();
        h.set_size(data.len() as u64);
        h.set_mode(if dir { 0o755 } else { 0o644 });
        h.set_mtime(0);
        h.set_entry_type(if dir {
            tar::EntryType::Directory
        } else {
            tar::EntryType::Regular
        });
        h.set_cksum();
        b.append(&h, data).unwrap();
    };
    for e in entries {
        match e {
            E::File(p, d) => push((*p).to_string(), d, false),
            E::Dir(p) => push((*p).to_string(), &[], true),
            E::Whiteout(p) => {
                // `dir/name` → `dir/.wh.name`; bare `name` → `.wh.name`.
                let wh = match p.rsplit_once('/') {
                    Some((dir, name)) => format!("{dir}/.wh.{name}"),
                    None => format!(".wh.{p}"),
                };
                push(wh, &[], false);
            }
            E::Opaque(dir) => push(format!("{dir}/.wh..wh..opq"), &[], false),
        }
    }
    b.into_inner().unwrap()
}

/// Deterministic gzip so identical layer content → identical blob → identical
/// digest (the basis of cross-image layer dedup).
fn gzip(raw: &[u8]) -> Vec<u8> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(raw).unwrap();
    e.finish().unwrap()
}

/// Push a multi-layer image (gzip layers + minimal config + manifest) and return
/// its reference string.
async fn push_image(client: &RegistryClient, addr: &str, repo_tag: &str, layers_raw: &[Vec<u8>]) -> String {
    let auth = Credentials::Anonymous;
    let image_ref = format!("{addr}/test/{repo_tag}");
    let image: Reference = image_ref.parse().unwrap();

    let config_bytes =
        br#"{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}"#.to_vec();
    let config_digest = format!("sha256:{}", hex_sha256(&config_bytes));
    client
        .push_blob(
            &image,
            futures::stream::iter([Ok(Bytes::from(config_bytes.clone()))]),
            &config_digest,
            &auth,
        )
        .await
        .unwrap();

    let mut descriptors = Vec::new();
    for raw in layers_raw {
        let blob = gzip(raw);
        let digest = format!("sha256:{}", hex_sha256(&blob));
        client
            .push_blob(
                &image,
                futures::stream::iter([Ok(Bytes::from(blob.clone()))]),
                &digest,
                &auth,
            )
            .await
            .unwrap();
        descriptors.push(OciDescriptor {
            media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
            digest,
            size: blob.len() as i64,
            ..Default::default()
        });
    }

    let manifest = OciImageManifest {
        schema_version: 2,
        media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        config: OciDescriptor {
            media_type: "application/vnd.oci.image.config.v1+json".to_string(),
            digest: config_digest,
            size: config_bytes.len() as i64,
            ..Default::default()
        },
        layers: descriptors,
        ..Default::default()
    };
    client.push_manifest(&image, &manifest, &auth).await.unwrap();
    image_ref
}

// ---------------------------------------------------------------------------
// Layered bless (mirrors run_bless_oci_layered, but with the http client)
// ---------------------------------------------------------------------------

async fn bless_layered(
    client: &RegistryClient,
    image_ref: &str,
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    name: &str,
) -> Vec<StoredLayer> {
    let auth = Credentials::Anonymous;
    let image: Reference = image_ref.parse().unwrap();
    let resolved = client.resolve(&image, &auth).await.unwrap();

    let mut results = Vec::new();
    let mut digests = Vec::new();
    let mut sizes = Vec::new();
    for layer in &resolved.layers {
        let tmp = pull_layer_to_tempfile(client, &image, layer, &auth)
            .await
            .unwrap();
        let stored = ensure_layer_stored(object_store, db_path, &layer.digest, tmp)
            .await
            .unwrap();
        digests.push(layer.digest.clone());
        sizes.push(layer.size as u64);
        results.push(stored);
    }

    put_image_descriptor(
        object_store,
        db_path,
        name,
        &ImageDescriptor {
            image_ref: image_ref.to_string(),
            config_digest: resolved.manifest.config.digest.clone(),
            layers: digests,
            layer_sizes: sizes,
        },
    )
    .await
    .unwrap();
    results
}

// ---------------------------------------------------------------------------
// Merged-bless baseline (what flattening costs, no cross-image dedup)
// ---------------------------------------------------------------------------

async fn merged_stored_bytes(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    name: &str,
    layers_raw: &[Vec<u8>],
    uuid_seed: &str,
) -> u64 {
    let mut layers: Vec<Cursor<Vec<u8>>> = layers_raw.iter().cloned().map(Cursor::new).collect();
    let total: u64 = layers_raw.iter().map(|l| l.len() as u64).sum();
    let device_size = (total.saturating_mul(3).max(64 * 1024 * 1024)).next_power_of_two();
    let opts = ConvertOptions {
        convert_backslash: false,
        writer_options: vec![
            WriterOption::MaximumDiskSize(device_size as i64),
            WriterOption::Uuid(deterministic_uuid(uuid_seed)),
            WriterOption::Journal(1024),
        ],
    };
    let ext4 = convert_oci_layers_to_ext4(&mut layers, Cursor::new(Vec::new()), &opts)
        .unwrap()
        .into_inner();
    let cs = Arc::new(ContentStore::new(
        Arc::clone(object_store),
        &format!("{db_path}/exports/{name}"),
    ));
    let (_vm, _hot, stats) = store_ext4_stream(&cs, Cursor::new(ext4), device_size)
        .await
        .unwrap();
    stats.bytes_uploaded
}

// ---------------------------------------------------------------------------
// Composition: overlay-merge per-layer ext4s vs the merged ext4
// ---------------------------------------------------------------------------

fn read_regular(reader: &mut Reader<Cursor<&[u8]>>, inode_number: u32) -> Vec<u8> {
    let inode = reader.read_inode(inode_number).unwrap();
    reader.read_data(&inode).unwrap()
}

/// Visible regular files of the merged ext4 (the canonical answer).
fn merged_files(layers_raw: &[Vec<u8>]) -> BTreeMap<String, Vec<u8>> {
    let mut layers: Vec<Cursor<Vec<u8>>> = layers_raw.iter().cloned().map(Cursor::new).collect();
    let opts = ConvertOptions {
        convert_backslash: false,
        writer_options: vec![WriterOption::Uuid([0x33; 16]), WriterOption::Journal(1024)],
    };
    let image = convert_oci_layers_to_ext4(&mut layers, Cursor::new(Vec::new()), &opts)
        .unwrap()
        .into_inner();
    let mut reader = Reader::new(Cursor::new(image.as_slice())).unwrap();
    let mut out = BTreeMap::new();
    for e in reader.walk().unwrap() {
        if e.mode & format::TYPE_MASK == format::S_IFREG {
            let data = read_regular(&mut reader, e.inode_number);
            out.insert(e.path, data);
        }
    }
    out
}

/// Visible regular files obtained by overlay-stacking the per-layer ext4s,
/// applying overlayfs semantics (char-dev whiteout, opaque dirs).
fn overlay_files(layers_raw: &[Vec<u8>]) -> BTreeMap<String, Vec<u8>> {
    let mut visible: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for raw in layers_raw {
        let opts = ConvertOptions {
            convert_backslash: false,
            writer_options: vec![WriterOption::Uuid([0x44; 16]), WriterOption::Journal(1024)],
        };
        let image = convert_layer_to_ext4(Cursor::new(raw.clone()), Cursor::new(Vec::new()), &opts)
            .unwrap()
            .into_inner();
        let mut reader = Reader::new(Cursor::new(image.as_slice())).unwrap();
        let entries = reader.walk().unwrap();
        for e in &entries {
            let kind = e.mode & format::TYPE_MASK;
            if kind == format::S_IFCHR && e.devmajor == 0 && e.devminor == 0 {
                // Whiteout: delete this path (and any subtree) from lower layers.
                let prefix = format!("{}/", e.path);
                visible.remove(&e.path);
                visible.retain(|k, _| !k.starts_with(&prefix));
            } else if kind == format::S_IFDIR
                && e.xattrs.get("trusted.overlay.opaque").map(Vec::as_slice) == Some(b"y")
            {
                // Opaque dir: drop everything from lower layers under it.
                let prefix = format!("{}/", e.path);
                visible.retain(|k, _| !k.starts_with(&prefix));
            }
        }
        // Add this layer's regular files (after opaque purge above).
        for e in &entries {
            if e.mode & format::TYPE_MASK == format::S_IFREG {
                let data = read_regular(&mut reader, e.inode_number);
                visible.insert(e.path.clone(), data);
            }
        }
    }
    visible
}

fn digest_hex(d: &str) -> &str {
    d.strip_prefix("sha256:").unwrap_or(d)
}

/// Replicates GC's live-set collection from image descriptors (the invariant
/// GC ref-counting relies on).
async fn referenced_layers(store: &Arc<dyn ObjectStore>, db: &str) -> HashSet<String> {
    let mut live = HashSet::new();
    for name in list_image_descriptors(store, db).await.unwrap() {
        if let Some(d) = get_image_descriptor(store, db, &name).await.unwrap() {
            for l in &d.layers {
                live.insert(digest_hex(l).to_string());
            }
        }
    }
    live
}

async fn count_objects(store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
    use futures::StreamExt;
    let p = ObjectPath::from(prefix.to_string());
    store
        .list(Some(&p))
        .filter_map(|r| async move { r.ok() })
        .count()
        .await
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn layered_oci_bless_dedups_shared_layers_e2e() {
    let (_registry, addr) = start_registry().await;
    let client = http_client();

    // A base layer shared by all three images (a big shared file dominates size).
    let base = build_tar(&[
        E::Dir("etc"),
        E::File("etc/shared.bin", &vec![0x5A_u8; 300_000]),
        E::File("etc/keep", b"keep"),
        E::File("b", b"to-be-removed"),
        E::Dir("dir"),
        E::File("dir/x", b"x"),
        E::File("dir/y", b"y"),
    ]);
    let upper1 = build_tar(&[E::File("app1.bin", &vec![1u8; 50_000])]);
    // upper2 exercises a whiteout (delete /b) and an opaque dir (/dir).
    let upper2 = build_tar(&[
        E::File("app2.bin", &vec![2u8; 60_000]),
        E::Whiteout("b"),
        E::Opaque("dir"),
        E::File("dir/z", b"z"),
    ]);
    let upper3 = build_tar(&[E::File("app3.bin", &vec![3u8; 70_000])]);

    let img1_layers = vec![base.clone(), upper1];
    let img2_layers = vec![base.clone(), upper2];
    let img3_layers = vec![base.clone(), upper3];

    let ref1 = push_image(&client, &addr, "img1:v1", &img1_layers).await;
    let ref2 = push_image(&client, &addr, "img2:v1", &img2_layers).await;
    let ref3 = push_image(&client, &addr, "img3:v1", &img3_layers).await;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = "test";

    let r1 = bless_layered(&client, &ref1, &store, db, "img1").await;
    let r2 = bless_layered(&client, &ref2, &store, db, "img2").await;
    let r3 = bless_layered(&client, &ref3, &store, db, "img3").await;

    // (1) Shared base layer stored once; reused by images 2 and 3.
    assert!(!r1[0].already_present, "first image stores the base layer");
    assert!(r2[0].already_present, "second image reuses the base layer");
    assert!(r3[0].already_present, "third image reuses the base layer");
    assert!(!r1[1].already_present && !r2[1].already_present && !r3[1].already_present,
        "each upper layer is unique and stored fresh");
    assert_eq!(r1[0].digest, r2[0].digest, "base layer digest is identical across images");
    assert_eq!(r2[0].digest, r3[0].digest);
    let base_digest = r1[0].digest.clone();
    assert!(
        count_objects(&store, &layer_base_path(db, &base_digest)).await >= 2,
        "base layer artifact (manifest + ≥1 pack) exists once",
    );

    // (2) Layered storage < merged storage of the same three images.
    let layered_total: u64 = [&r1, &r2, &r3]
        .iter()
        .flat_map(|r| r.iter())
        .map(|s| s.stored_bytes)
        .sum();
    let mstore: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let merged_total = merged_stored_bytes(&mstore, db, "m1", &img1_layers, "seed-1").await
        + merged_stored_bytes(&mstore, db, "m2", &img2_layers, "seed-2").await
        + merged_stored_bytes(&mstore, db, "m3", &img3_layers, "seed-3").await;
    assert!(
        layered_total < merged_total,
        "layered storage {layered_total} must be smaller than merged {merged_total}",
    );

    // (3) Composition correctness: overlay-stacking the per-layer ext4s yields
    // the same filesystem the merged path produces — including the /b whiteout
    // deletion and the opaque /dir.
    let overlay = overlay_files(&img2_layers);
    let merged = merged_files(&img2_layers);
    assert_eq!(overlay, merged, "overlay composition must equal the merged filesystem");
    assert!(!merged.contains_key("b"), "whiteout deleted /b");
    assert!(!merged.contains_key("dir/x") && !merged.contains_key("dir/y"), "opaque dropped lower /dir entries");
    assert_eq!(merged.get("dir/z").map(Vec::as_slice), Some(b"z".as_slice()));
    assert_eq!(merged.get("app2.bin").map(|v| v.len()), Some(60_000));

    // (4) GC reference invariant: dropping one image keeps the base referenced.
    store
        .delete(&ObjectPath::from(format!("{db}/images/img1")))
        .await
        .unwrap();
    let live = referenced_layers(&store, db).await;
    assert!(
        live.contains(digest_hex(&base_digest)),
        "base layer still referenced by img2/img3 after dropping img1",
    );
}
