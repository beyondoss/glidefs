/// OCI pull pipeline: registry → tar stream → ext4 → GlideFS block storage.
///
/// Pulls image layers from an OCI registry and merges them into a single ext4
/// filesystem on GlideFS blocks. Layers are downloaded to temp files, then
/// merged respecting OCI whiteout semantics (`.wh.*` and opaque whiteouts).
use std::io::{self, Read, Seek, Write};
use std::sync::Arc;

use flate2::read::GzDecoder;
use oci_registry::{Credentials, OciDescriptor, Reference, ResolvedImage};
use tokio_util::io::StreamReader;
use tracing::{debug, warn};

use crate::block::handler::BlockHandler;
use crate::oci::ingest::IngestOptions;

/// Hard cap on decompressed bytes per OCI layer. A malicious image can pack
/// a multi-GB stream of zeros into a few-byte gzip blob; without a cap,
/// `io::copy(GzDecoder, tmpfile)` writes all of it to disk and either fills
/// the volume or OOMs the daemon. 16 GiB is generous enough for legitimate
/// container layers (real images rarely exceed 1–2 GB per layer) while
/// keeping a malicious blob from being able to wedge the host.
///
/// Note: this caps the *decompressed* size, not the network transfer. The
/// registry client also enforces its own connect/read timeouts.
pub const DEFAULT_MAX_LAYER_DECOMPRESSED_BYTES: u64 = 16 * 1024 * 1024 * 1024;

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
    emit_boot_set: bool,
) -> Result<(ResolvedImage, Vec<u64>), PullError> {
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

    // Merge all layers into a single ext4 filesystem. When asked, statically
    // derive the boot working set (ELF closure) so the writer reports where it
    // placed those files; ext4 can't reorder them contiguously (unlike EROFS),
    // so the server precisely warms their scattered device blocks at open.
    let writer_options = options.writer_options.clone();
    let config = resolved.config.clone();
    let boot_blocks = tokio::task::spawn_blocking(move || -> io::Result<Vec<u64>> {
        let rt = tokio::runtime::Handle::current();
        let adapter = super::BlockAdapter::new(&handler, rt);
        let mut writer_options = writer_options;
        if emit_boot_set {
            let boot_paths = crate::oci::boot_set::derive_boot_paths(&config, &mut layer_files);
            if !boot_paths.is_empty() {
                writer_options.push(ext4::writer::WriterOption::PriorityOrder(boot_paths));
            }
        }
        let convert_opts = ext4::tar_convert::ConvertOptions {
            writer_options,
            ..Default::default()
        };
        let (mut adapter, ranges) =
            ext4::convert_oci_layers_to_ext4_with_boot_blocks(&mut layer_files, adapter, &convert_opts)?;
        adapter.flush()?;
        Ok(boot_ranges_to_blocks(&ranges))
    })
    .await
    .map_err(|e| PullError::Io(io::Error::other(format!("task panicked: {e}"))))??;

    Ok((resolved, boot_blocks))
}

/// Convert the ext4 writer's boot-set byte ranges `(device_offset, len)` to the
/// sorted, deduplicated set of device block indices they touch (the block layer
/// is positional, so byte offset / block size = block index).
fn boot_ranges_to_blocks(ranges: &[(u64, u64)]) -> Vec<u64> {
    use crate::oci::ext4_store::BLOCK_SIZE;
    let bs = u64::from(BLOCK_SIZE);
    let mut set = std::collections::BTreeSet::new();
    for &(off, len) in ranges {
        if len == 0 {
            continue;
        }
        let first = off / bs;
        let last = (off + len - 1) / bs;
        for b in first..=last {
            set.insert(b);
        }
    }
    set.into_iter().collect()
}

/// Download and decompress a single layer to a seekable temp file.
pub async fn pull_layer_to_tempfile(
    client: &oci_registry::RegistryClient,
    image: &Reference,
    layer: &OciDescriptor,
    auth: &Credentials,
) -> Result<std::fs::File, PullError> {
    let stream = client.pull_layer(image, layer, auth).await?;
    let async_reader = StreamReader::new(stream);
    let layer_digest = layer.digest.clone();

    let file = tokio::task::spawn_blocking(move || {
        let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);
        let mut tmpfile = tempfile::tempfile()?;
        bounded_gz_copy(
            sync_reader,
            &mut tmpfile,
            DEFAULT_MAX_LAYER_DECOMPRESSED_BYTES,
            &layer_digest,
        )?;
        tmpfile.seek(io::SeekFrom::Start(0))?;
        Ok::<_, io::Error>(tmpfile)
    })
    .await
    .map_err(|e| PullError::Io(io::Error::other(format!("task panicked: {e}"))))?;

    Ok(file?)
}

/// Stream gzip data from `src` into `dst`, capped at `cap` decompressed bytes.
///
/// Uses `Read::take(cap + 1)` so we can distinguish "source ended at or under
/// the cap" from "source had more bytes but we stopped" — the latter is the
/// bomb signal. `layer_label` is included in the error / log for diagnostics.
fn bounded_gz_copy<R: Read, W: Write>(
    src: R,
    dst: &mut W,
    cap: u64,
    layer_label: &str,
) -> io::Result<u64> {
    let mut bounded = GzDecoder::new(src).take(cap.saturating_add(1));
    let written = io::copy(&mut bounded, dst)?;
    if written > cap {
        warn!(
            layer = %layer_label,
            cap_bytes = cap,
            "layer decompression hit the cap — refusing as possible decompression bomb",
        );
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "layer {layer_label}: decompressed size exceeds {cap}-byte cap (possible decompression bomb)",
            ),
        ));
    }
    Ok(written)
}

#[derive(Debug, thiserror::Error)]
pub enum PullError {
    #[error("registry: {0}")]
    Registry(#[from] oci_registry::Error),

    #[error("io: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;

    /// Build a gzip blob whose decompressed output is `decompressed_size`
    /// bytes of zeros — i.e. a minimal decompression bomb.
    fn make_zero_gz(decompressed_size: usize) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), Compression::best());
        let chunk = [0u8; 64 * 1024];
        let mut remaining = decompressed_size;
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            enc.write_all(&chunk[..n]).unwrap();
            remaining -= n;
        }
        enc.finish().unwrap()
    }

    #[test]
    fn bounded_gz_copy_accepts_layer_under_cap() {
        // 1 MB of zeros, cap at 4 MB — should pass.
        let gz = make_zero_gz(1024 * 1024);
        let mut dst = Vec::new();
        let written =
            bounded_gz_copy(&gz[..], &mut dst, 4 * 1024 * 1024, "test-ok").expect("under cap");
        assert_eq!(written, 1024 * 1024);
        assert_eq!(dst.len(), 1024 * 1024);
    }

    #[test]
    fn bounded_gz_copy_rejects_bomb_over_cap() {
        // 2 MB of zeros compresses to a few KB; cap at 1 MB → bomb.
        let gz = make_zero_gz(2 * 1024 * 1024);
        let mut dst = Vec::new();
        let err = bounded_gz_copy(&gz[..], &mut dst, 1024 * 1024, "test-bomb")
            .expect_err("bomb must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("possible decompression bomb"),
            "error message should flag bomb, got: {err}",
        );
        // dst received up to cap+1 bytes before the error fired.
        assert!(dst.len() >= 1024 * 1024);
        assert!(dst.len() <= 1024 * 1024 + 1);
    }

    #[test]
    fn bounded_gz_copy_propagates_io_error_for_corrupt_gzip() {
        // Random bytes that aren't valid gzip → decoder errors.
        let garbage = vec![0xDEu8; 1024];
        let mut dst = Vec::new();
        let err = bounded_gz_copy(&garbage[..], &mut dst, 1024 * 1024, "test-corrupt")
            .expect_err("corrupt gzip should error");
        // Not our bomb error — should be an underlying decode error.
        assert!(!err.to_string().contains("possible decompression bomb"));
    }
}
