#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! Content-addressed OCI **layer** storage — the heart of layer-surviving dedup.
//!
//! Instead of flattening an image's layers into one ext4 (where a shared base
//! layer lands at different block offsets per image and never dedups), each OCI
//! layer is converted independently to a deterministic, overlay-preserving ext4
//! and stored once under a global, content-addressed namespace keyed by the
//! layer digest:
//!
//! ```text
//! {db_path}/layers/{sha256-hex}/manifests/layer   ← VolumeManifest
//! {db_path}/layers/{sha256-hex}/chunks/****/*.pack ← content-addressed packs
//! {db_path}/images/{name}                          ← ImageDescriptor (layer list)
//! ```
//!
//! Two images that share a layer write to the *same* `layers/{digest}` path, so
//! the packs are identical and stored exactly once. Storing is idempotent: a
//! layer already present (manifest HEAD hit) is skipped.

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use anyhow::{Context, Result};
use ext4::tar_convert::{convert_layer_to_ext4, ConvertOptions};
use ext4::writer::WriterOption;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};

use crate::block::content_store::ContentStore;
use crate::oci::ext4_store::{deterministic_uuid, store_ext4_stream, BLOCK_SIZE};

/// Manifest name for a stored layer (its sole VolumeManifest).
const LAYER_MANIFEST_NAME: &str = "layer";

/// Strip the `sha256:` algorithm prefix, leaving the hex digest used as the
/// content-addressed path segment.
fn digest_hex(digest: &str) -> &str {
    digest.strip_prefix("sha256:").unwrap_or(digest)
}

/// Global content-addressed base path for a layer: `{db_path}/layers/{hex}`.
///
/// Deliberately *not* under any per-image `exports/{prefix}` namespace — the
/// shared location is what lets the same layer dedup across different images.
pub fn layer_base_path(db_path: &str, digest: &str) -> String {
    format!("{}/layers/{}", db_path, digest_hex(digest))
}

/// S3 key for an image descriptor: `{db_path}/images/{name}`.
pub fn image_key(db_path: &str, name: &str) -> String {
    format!("{}/images/{}", db_path, name)
}

/// Outcome of storing one layer.
#[derive(Debug, Clone)]
pub struct StoredLayer {
    pub digest: String,
    /// True if the layer was already present (idempotent skip).
    pub already_present: bool,
    /// Bytes uploaded for this layer (0 when skipped).
    pub stored_bytes: u64,
}

/// An image as an ordered list of content-addressed layers. This is all that is
/// stored per image — the layer artifacts hold the actual blocks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageDescriptor {
    /// OCI image reference this was blessed from (informational).
    pub image_ref: String,
    /// OCI config digest (`sha256:...`).
    pub config_digest: String,
    /// Layer digests, bottom-to-top (`sha256:...`).
    pub layers: Vec<String>,
    /// Compressed size of each layer blob, parallel to `layers`.
    pub layer_sizes: Vec<u64>,
}

impl ImageDescriptor {
    pub fn to_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Deterministic device size for a layer's ext4 image.
///
/// Derived solely from the decompressed tar length so the same layer always
/// produces the same geometry (hence byte-identical ext4 → identical packs).
/// Zero blocks past the real content are skipped at store time, so oversizing
/// costs nothing in storage.
fn layer_device_size(tar_len: u64) -> u64 {
    // ×3 (not ×2): extra headroom for block-grid alignment padding, which
    // inflates the logical ext4. The padding is holes/zeros dropped by the
    // block store, so it costs address space, not stored bytes.
    (tar_len.saturating_mul(3).max(64 * 1024 * 1024)).next_power_of_two()
}

/// Ensure a single OCI layer is stored as a content-addressed ext4 artifact.
///
/// `decompressed_tar` is the layer's *decompressed* tar stream (seekable).
/// Idempotent: if the layer's manifest already exists, returns immediately
/// without re-converting or re-uploading.
pub async fn ensure_layer_stored<R: Read + Seek>(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    digest: &str,
    mut decompressed_tar: R,
) -> Result<StoredLayer> {
    let base = layer_base_path(db_path, digest);
    let content_store = Arc::new(ContentStore::new(Arc::clone(object_store), &base));

    // Idempotent skip: the layer is content-addressed, so an existing manifest
    // means the identical bytes are already stored.
    if content_store
        .head_manifest(LAYER_MANIFEST_NAME)
        .await
        .context("HEAD layer manifest")?
    {
        return Ok(StoredLayer {
            digest: digest.to_string(),
            already_present: true,
            stored_bytes: 0,
        });
    }

    // Size the device from the decompressed tar length (deterministic per layer).
    let tar_len = decompressed_tar.seek(SeekFrom::End(0))?;
    decompressed_tar.seek(SeekFrom::Start(0))?;
    let device_size = layer_device_size(tar_len);

    // Convert the layer to an overlay-preserving, deterministic ext4. The UUID
    // is derived from the layer digest so the output is byte-identical across
    // images — the property that makes the packs dedup.
    let opts = ConvertOptions {
        convert_backslash: false,
        writer_options: vec![
            WriterOption::MaximumDiskSize(device_size as i64),
            WriterOption::Uuid(deterministic_uuid(digest)),
            WriterOption::Journal(1024), // 4 MiB journal — same as bless
            // Align large file payloads to the dedup block grid (the volume's
            // 128 KiB block size) so the same file dedups across layers/images.
            WriterOption::AlignData { align: BLOCK_SIZE, min_size: BLOCK_SIZE },
        ],
    };
    let mut ext4_tmp = tempfile::tempfile().context("layer ext4 tempfile")?;
    convert_layer_to_ext4(&mut decompressed_tar, &mut ext4_tmp, &opts)
        .context("convert layer to ext4")?;
    ext4_tmp.seek(SeekFrom::Start(0))?;

    // Stream the ext4 into content-addressed packs + manifest under layers/{digest}.
    let (volume_manifest, _hot_set, stats) =
        store_ext4_stream(&content_store, ext4_tmp, device_size, crate::block::block_map::COMPRESSION_BLESS).await?;
    content_store
        .put_manifest(LAYER_MANIFEST_NAME, volume_manifest.serialize()?, None)
        .await
        .context("put layer manifest")?;

    Ok(StoredLayer {
        digest: digest.to_string(),
        already_present: false,
        stored_bytes: stats.bytes_uploaded,
    })
}

/// Persist an image descriptor at `images/{name}`.
pub async fn put_image_descriptor(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    name: &str,
    descriptor: &ImageDescriptor,
) -> Result<()> {
    let path = ObjectPath::from(image_key(db_path, name));
    object_store
        .put(&path, PutPayload::from(descriptor.to_json()?))
        .await
        .context("put image descriptor")?;
    Ok(())
}

/// Load an image descriptor, or None if it does not exist.
pub async fn get_image_descriptor(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    name: &str,
) -> Result<Option<ImageDescriptor>> {
    let path = ObjectPath::from(image_key(db_path, name));
    match object_store.get(&path).await {
        Ok(resp) => {
            let bytes = resp.bytes().await.context("read image descriptor")?;
            Ok(Some(ImageDescriptor::from_json(&bytes)?))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).context("get image descriptor"),
    }
}

/// List all image descriptor names under `{db_path}/images/`.
pub async fn list_image_descriptors(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> Result<Vec<String>> {
    use futures::StreamExt;
    let prefix_str = format!("{}/images/", db_path);
    let prefix = ObjectPath::from(prefix_str.clone());
    let mut names = Vec::new();
    let mut stream = object_store.list(Some(&prefix));
    while let Some(meta) = stream.next().await {
        let meta = meta.context("list images")?;
        let loc = meta.location.to_string();
        if let Some(rel) = loc.strip_prefix(&prefix_str)
            && !rel.is_empty()
        {
            names.push(rel.to_string());
        }
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::memory::InMemory;

    fn tar_with(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (path, data) in files {
            let mut h = tar::Header::new_gnu();
            h.set_path(path).unwrap();
            h.set_size(data.len() as u64);
            h.set_mode(0o644);
            h.set_entry_type(tar::EntryType::Regular);
            h.set_cksum();
            b.append(&h, *data).unwrap();
        }
        b.into_inner().unwrap()
    }

    async fn count_objects(store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
        let p = ObjectPath::from(prefix.to_string());
        store
            .list(Some(&p))
            .filter_map(|r| async move { r.ok() })
            .count()
            .await
    }

    #[tokio::test]
    async fn ensure_layer_stored_is_idempotent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = "db";
        let digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let tar = tar_with(&[("a.txt", b"hello"), ("b.txt", b"world")]);

        let r1 = ensure_layer_stored(&store, db, digest, std::io::Cursor::new(tar.clone()))
            .await
            .unwrap();
        assert!(!r1.already_present);
        assert!(r1.stored_bytes > 0);
        let n1 = count_objects(&store, &layer_base_path(db, digest)).await;
        assert!(n1 >= 2, "expected manifest + ≥1 pack, got {n1}");

        // Re-store the same digest → idempotent skip, no new objects.
        let r2 = ensure_layer_stored(&store, db, digest, std::io::Cursor::new(tar))
            .await
            .unwrap();
        assert!(r2.already_present);
        assert_eq!(r2.stored_bytes, 0);
        let n2 = count_objects(&store, &layer_base_path(db, digest)).await;
        assert_eq!(n1, n2, "no new objects on idempotent re-store");
    }

    #[tokio::test]
    async fn identical_layers_share_storage() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = "db";
        // Same content → same digest → same base path → stored once.
        let digest = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
        let tar = tar_with(&[("bin/sh", &[7u8; 200_000])]);

        ensure_layer_stored(&store, db, digest, std::io::Cursor::new(tar.clone()))
            .await
            .unwrap();
        let after_first = count_objects(&store, &format!("{db}/layers/")).await;

        // A second image references the same layer digest: skipped, no growth.
        let r = ensure_layer_stored(&store, db, digest, std::io::Cursor::new(tar))
            .await
            .unwrap();
        assert!(r.already_present);
        let after_second = count_objects(&store, &format!("{db}/layers/")).await;
        assert_eq!(after_first, after_second, "shared layer stored exactly once");
    }

    #[tokio::test]
    async fn image_descriptor_roundtrip() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = "db";
        let d = ImageDescriptor {
            image_ref: "img:latest".into(),
            config_digest: "sha256:cfg".into(),
            layers: vec!["sha256:a".into(), "sha256:b".into()],
            layer_sizes: vec![10, 20],
        };
        put_image_descriptor(&store, db, "img", &d).await.unwrap();
        let got = get_image_descriptor(&store, db, "img")
            .await
            .unwrap()
            .expect("descriptor present");
        assert_eq!(got, d);
        assert_eq!(list_image_descriptors(&store, db).await.unwrap(), vec!["img".to_string()]);
        assert!(get_image_descriptor(&store, db, "missing").await.unwrap().is_none());
    }
}
