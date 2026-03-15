/// OCI pull pipeline: registry → tar stream → ext4 → GlideFS block storage.
///
/// Pulls image layers from an OCI registry and merges them into a single ext4
/// filesystem on GlideFS blocks. Layers are downloaded to temp files, then
/// merged respecting OCI whiteout semantics (`.wh.*` and opaque whiteouts).
use std::io::{self, Seek, Write};
use std::sync::Arc;

use flate2::read::GzDecoder;
use oci_registry::{Credentials, OciDescriptor, Reference, ResolvedImage};
use tokio_util::io::StreamReader;
use tracing::debug;

use crate::block::handler::BlockHandler;
use crate::oci::ingest::IngestOptions;

/// Pull an OCI image from a registry and ingest its layers into GlideFS blocks.
///
/// All layers are downloaded and decompressed to temp files, then merged into
/// a single ext4 filesystem using OCI layer semantics (topmost layer wins,
/// whiteouts delete lower-layer entries).
pub async fn pull_image(
    client: &oci_registry::RegistryClient,
    image: &Reference,
    auth: &Credentials,
    handler: Arc<BlockHandler>,
    options: IngestOptions,
) -> Result<ResolvedImage, PullError> {
    let resolved = client.resolve(image, auth).await?;

    debug!(
        layers = resolved.layers.len(),
        digest = resolved.manifest_digest,
        "resolved image, pulling layers"
    );

    // Download all layers to seekable temp files (decompressed).
    let mut layer_files = Vec::with_capacity(resolved.layers.len());
    for (i, layer) in resolved.layers.iter().enumerate() {
        debug!(
            layer = i,
            digest = layer.digest,
            size = layer.size,
            "pulling layer"
        );
        let file = pull_layer_to_tempfile(client, image, layer, auth).await?;
        layer_files.push(file);
    }

    // Merge all layers into a single ext4 filesystem.
    let writer_options = options.writer_options.clone();
    let result = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let adapter = super::BlockAdapter::new(&handler, rt);
        let convert_opts = ext4::tar_convert::ConvertOptions {
            writer_options,
            ..Default::default()
        };
        let mut adapter =
            ext4::convert_oci_layers_to_ext4(&mut layer_files, adapter, &convert_opts)?;
        adapter.flush()?;
        Ok::<_, io::Error>(())
    })
    .await
    .map_err(|e| PullError::Io(io::Error::other(format!("task panicked: {e}"))))?;

    result?;
    Ok(resolved)
}

/// Download and decompress a single layer to a seekable temp file.
async fn pull_layer_to_tempfile(
    client: &oci_registry::RegistryClient,
    image: &Reference,
    layer: &OciDescriptor,
    auth: &Credentials,
) -> Result<std::fs::File, PullError> {
    let stream = client.pull_layer(image, layer, auth).await?;
    let async_reader = StreamReader::new(stream);

    let file = tokio::task::spawn_blocking(move || {
        let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);
        let mut decompressed = GzDecoder::new(sync_reader);
        let mut tmpfile = tempfile::tempfile()?;
        io::copy(&mut decompressed, &mut tmpfile)?;
        tmpfile.seek(io::SeekFrom::Start(0))?;
        Ok::<_, io::Error>(tmpfile)
    })
    .await
    .map_err(|e| PullError::Io(io::Error::other(format!("task panicked: {e}"))))?;

    Ok(file?)
}

#[derive(Debug, thiserror::Error)]
pub enum PullError {
    #[error("registry: {0}")]
    Registry(#[from] oci_registry::Error),

    #[error("io: {0}")]
    Io(#[from] io::Error),
}
