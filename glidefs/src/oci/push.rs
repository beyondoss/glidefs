/// OCI push pipeline: GlideFS block storage → ext4 → tar → gzip → registry.
///
/// Exports an ext4 filesystem from GlideFS blocks, compresses it as a gzip tar
/// layer, and pushes it to an OCI registry. The compressed layer is spooled to
/// a temp file so the full blob never lives in memory. Upload uses chunked
/// streaming from disk.
use std::io::{self, BufWriter, Write};
use std::sync::Arc;

use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use oci_registry::{Credentials, OciDescriptor, OciImageManifest, Reference};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use tokio::fs::File as TokioFile;
use tokio_util::io::ReaderStream;
use tracing::debug;

use crate::block::handler::BlockHandler;

/// Result of a successful push.
pub struct PushResult {
    /// Digest of the pushed manifest (sha256:...).
    pub manifest_digest: String,
    /// Digest of the last compressed layer blob (sha256:...).
    pub layer_digest: String,
    /// Size of the last compressed layer in bytes.
    pub layer_size: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("registry: {0}")]
    Registry(#[from] oci_registry::Error),

    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// An exported, compressed layer ready for upload.
struct ExportedLayer {
    temp_file: NamedTempFile,
    /// sha256 of the uncompressed tar (OCI diff_id).
    diff_id: String,
    /// sha256 of the compressed blob.
    digest: String,
    /// Size of the compressed blob in bytes.
    size: u64,
}

/// Push GlideFS blocks as a single-layer OCI image.
pub async fn push_image(
    client: &oci_registry::RegistryClient,
    image: &Reference,
    auth: &Credentials,
    handler: Arc<BlockHandler>,
) -> Result<PushResult, PushError> {
    let layer = tokio::task::spawn_blocking(move || export_full_layer(handler))
        .await
        .map_err(join_err)??;

    push_layers(client, image, auth, vec![layer]).await
}

/// Push an incremental delta as a two-layer OCI image.
///
/// Layer 0 = full base snapshot (idempotent — skipped if blob already exists).
/// Layer 1 = delta between base and target (only added/modified/deleted entries).
pub async fn push_delta_image(
    client: &oci_registry::RegistryClient,
    image: &Reference,
    auth: &Credentials,
    base_handler: Arc<BlockHandler>,
    target_handler: Arc<BlockHandler>,
) -> Result<PushResult, PushError> {
    // Export base and delta in parallel.
    let base_handle = {
        let h = Arc::clone(&base_handler);
        tokio::task::spawn_blocking(move || export_full_layer(h))
    };
    let delta_handle = {
        let bh = Arc::clone(&base_handler);
        let th = Arc::clone(&target_handler);
        tokio::task::spawn_blocking(move || export_delta_layer(bh, th))
    };

    let base_layer = base_handle.await.map_err(join_err)??;
    let delta_layer = delta_handle.await.map_err(join_err)??;

    push_layers(client, image, auth, vec![base_layer, delta_layer]).await
}

/// Upload layers, build config + manifest, push to registry.
///
/// Layers are pushed in order. Each layer blob is stream-uploaded from its temp
/// file. `push_blob` is idempotent — skips if the digest already exists.
async fn push_layers(
    client: &oci_registry::RegistryClient,
    image: &Reference,
    auth: &Credentials,
    layers: Vec<ExportedLayer>,
) -> Result<PushResult, PushError> {
    // Upload layer blobs.
    for layer in &layers {
        let digest_str = format!("sha256:{}", layer.digest);
        debug!(digest = %digest_str, size = layer.size, "uploading layer");
        let file = TokioFile::open(layer.temp_file.path()).await?;
        let stream = ReaderStream::with_capacity(file, 4 * 1024 * 1024);
        client
            .push_blob(image, stream, &digest_str, auth)
            .await?;
    }

    // Build + push config blob.
    let diff_ids: Vec<String> = layers
        .iter()
        .map(|l| format!("sha256:{}", l.diff_id))
        .collect();
    let config_json = serde_json::json!({
        "architecture": "amd64",
        "os": "linux",
        "rootfs": {
            "type": "layers",
            "diff_ids": diff_ids
        },
        "config": {}
    });
    let config_bytes = serde_json::to_vec(&config_json)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let config_digest = format!("sha256:{}", hex_sha256(&config_bytes));
    client
        .push_blob(
            image,
            futures::stream::iter([Ok(Bytes::from(config_bytes.clone()))]),
            &config_digest,
            auth,
        )
        .await?;

    // Build + push manifest.
    let layer_descriptors: Vec<OciDescriptor> = layers
        .iter()
        .map(|l| OciDescriptor {
            media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
            digest: format!("sha256:{}", l.digest),
            size: l.size as i64,
            ..Default::default()
        })
        .collect();

    let manifest = OciImageManifest {
        schema_version: 2,
        media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
        config: OciDescriptor {
            media_type: "application/vnd.oci.image.config.v1+json".to_string(),
            digest: config_digest,
            size: config_bytes.len() as i64,
            ..Default::default()
        },
        layers: layer_descriptors,
        ..Default::default()
    };
    let manifest_digest = client.push_manifest(image, &manifest, auth).await?;

    debug!(manifest_digest, "image pushed");

    let last = layers.last().expect("at least one layer");
    Ok(PushResult {
        manifest_digest,
        layer_digest: format!("sha256:{}", last.digest),
        layer_size: last.size,
    })
}

// --- Layer export (blocking) ------------------------------------------------

/// Export a full snapshot as a compressed tar layer.
///
/// Must be called from a blocking context (`spawn_blocking`).
fn export_full_layer(handler: Arc<BlockHandler>) -> io::Result<ExportedLayer> {
    let (temp_file, writer_chain) = new_writer_chain()?;

    let rt = tokio::runtime::Handle::current();
    let adapter = crate::oci::BlockAdapter::new(&handler, rt);
    let mut reader = ext4::reader::Reader::new(adapter)?;
    let writer_chain = reader.to_tar(writer_chain)?;

    finish_layer(temp_file, writer_chain)
}

/// Export a delta layer between two snapshots.
///
/// Produces only the changes (adds, modifications, deletions with whiteouts).
/// Must be called from a blocking context (`spawn_blocking`).
fn export_delta_layer(
    base_handler: Arc<BlockHandler>,
    target_handler: Arc<BlockHandler>,
) -> io::Result<ExportedLayer> {
    let (temp_file, writer_chain) = new_writer_chain()?;

    let rt = tokio::runtime::Handle::current();
    let base_adapter = crate::oci::BlockAdapter::new(&base_handler, rt.clone());
    let target_adapter = crate::oci::BlockAdapter::new(&target_handler, rt);

    let mut base_reader = ext4::reader::Reader::new(base_adapter)?;
    let mut target_reader = ext4::reader::Reader::new(target_adapter)?;

    let writer_chain =
        ext4::diff_to_tar(&mut base_reader, &mut target_reader, writer_chain)?;

    finish_layer(temp_file, writer_chain)
}

// --- Writer chain helpers ---------------------------------------------------

/// The layered writer: tar → sha256 → gzip → sha256 → file.
type WriterChain = DigestWriter<GzEncoder<DigestWriter<BufWriter<std::fs::File>>>>;

fn new_writer_chain() -> io::Result<(NamedTempFile, WriterChain)> {
    let temp_file = NamedTempFile::new()?;
    let file_writer = BufWriter::new(temp_file.as_file().try_clone()?);
    let compressed_digest = DigestWriter::new(file_writer);
    let gz = GzEncoder::new(compressed_digest, Compression::default());
    let uncompressed_digest = DigestWriter::new(gz);
    Ok((temp_file, uncompressed_digest))
}

fn finish_layer(
    temp_file: NamedTempFile,
    writer_chain: WriterChain,
) -> io::Result<ExportedLayer> {
    let (gz, diff_id) = writer_chain.finish();
    let compressed_digest = gz.finish()?;
    let (mut file_writer, digest) = compressed_digest.finish();
    file_writer.flush()?;

    let size = temp_file.as_file().metadata()?.len();

    Ok(ExportedLayer {
        temp_file,
        diff_id,
        digest,
        size,
    })
}

// --- Utilities --------------------------------------------------------------

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

fn join_err(e: tokio::task::JoinError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("task panicked: {e}"))
}

/// A writer that passes all bytes through to an inner writer while computing
/// their sha256 digest.
struct DigestWriter<W> {
    inner: W,
    hasher: Sha256,
}

impl<W> DigestWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    /// Consume the writer, returning the inner writer and the hex digest.
    fn finish(self) -> (W, String) {
        let digest = format!("{:x}", self.hasher.finalize());
        (self.inner, digest)
    }
}

impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_digest_writer() {
        let mut w = DigestWriter::new(Vec::new());
        w.write_all(b"hello world").unwrap();
        let (data, digest) = w.finish();
        assert_eq!(data, b"hello world");
        // sha256("hello world")
        assert_eq!(
            digest,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_hex_sha256() {
        assert_eq!(
            hex_sha256(b"hello world"),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_digest_writer_chained() {
        // Verify the chained writer pattern works: inner → digest → gz → digest → vec
        let outer = DigestWriter::new(Vec::new());
        let gz = GzEncoder::new(outer, Compression::default());
        let mut inner = DigestWriter::new(gz);

        inner.write_all(b"test data").unwrap();

        let (gz, uncompressed_digest) = inner.finish();
        let outer = gz.finish().unwrap();
        let (compressed, compressed_digest) = outer.finish();

        // Uncompressed digest should be sha256("test data")
        assert_eq!(
            uncompressed_digest,
            "916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9"
        );
        // Compressed data should be valid gzip
        assert!(compressed.len() > 2 && compressed[0] == 0x1f && compressed[1] == 0x8b);
        // Compressed digest should be different from uncompressed
        assert_ne!(compressed_digest, uncompressed_digest);
    }
}
