/// OCI pull pipeline: registry → tar stream → ext4 → GlideFS block storage.
///
/// Pulls image layers from an OCI registry and ingests them into GlideFS
/// blocks. Each layer is streamed, decompressed, and converted to ext4
/// without buffering the entire layer in memory.
use std::io::{self, Write};
use std::sync::Arc;

use flate2::read::GzDecoder;
use oci_registry::{Credentials, OciDescriptor, Reference, ResolvedImage};
use tokio_util::io::StreamReader;
use tracing::debug;

use crate::block::handler::BlockHandler;
use crate::oci::ingest::IngestOptions;

/// Pull an OCI image from a registry and ingest its layers into GlideFS blocks.
///
/// Layers are streamed directly from the registry, decompressed (gzip), and
/// fed into the ingest pipeline. The async-to-sync bridge uses the same
/// `spawn_blocking` + `SyncIoBridge` pattern as `BlockAdapter`.
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
        "resolved image, ingesting layers"
    );

    for (i, layer) in resolved.layers.iter().enumerate() {
        debug!(
            layer = i,
            digest = layer.digest,
            size = layer.size,
            "pulling layer"
        );
        pull_and_ingest_layer(client, image, layer, auth, Arc::clone(&handler), &options).await?;
    }

    Ok(resolved)
}

async fn pull_and_ingest_layer(
    client: &oci_registry::RegistryClient,
    image: &Reference,
    layer: &OciDescriptor,
    auth: &Credentials,
    handler: Arc<BlockHandler>,
    options: &IngestOptions,
) -> Result<(), PullError> {
    let stream = client.pull_layer(image, layer, auth).await?;

    // Convert async Stream<Bytes> → AsyncRead → sync Read (via SyncIoBridge
    // inside spawn_blocking) → GzDecoder → ingest_tar.
    let async_reader = StreamReader::new(stream);

    let writer_options = options.writer_options.clone();
    let result = tokio::task::spawn_blocking(move || {
        let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);
        let decompressed = GzDecoder::new(sync_reader);

        let rt = tokio::runtime::Handle::current();
        let adapter = super::BlockAdapter::new(&handler, rt);
        let convert_opts = ext4::tar_convert::ConvertOptions {
            writer_options,
            ..Default::default()
        };
        let mut adapter = ext4::convert_tar_to_ext4(decompressed, adapter, &convert_opts)?;
        adapter.flush()?;
        Ok::<_, io::Error>(())
    })
    .await
    .map_err(|e| PullError::Io(io::Error::other(format!("task panicked: {e}"))))?;

    Ok(result?)
}

#[derive(Debug, thiserror::Error)]
pub enum PullError {
    #[error("registry: {0}")]
    Registry(#[from] oci_registry::Error),

    #[error("io: {0}")]
    Io(#[from] io::Error),
}
