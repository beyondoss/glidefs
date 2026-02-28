use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMultipartOptions, WriteMultipart};
use tokio::io::AsyncReadExt;
use tracing::info;

use glidefs::block::content_store::ContentStore;
use glidefs::config::Settings;
use glidefs::oci::{export_delta_layer, export_full_layer, hex_sha256};

use crate::config;

pub async fn run_publish(
    manifest_name: String,
    s3_prefix: String,
    tag: String,
    base_manifest: Option<String>,
    config_path: PathBuf,
) -> Result<()> {
    let start = Instant::now();

    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let (object_store, db_path) = config::setup_object_store(&settings)?;

    let base = format!("{}/exports/{}", db_path, s3_prefix);
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), &base));

    let layers = if let Some(ref base_name) = base_manifest {
        // Delta publish: base + delta layers in parallel.
        info!(
            target_manifest = %manifest_name,
            base_manifest = %base_name,
            "exporting delta layers"
        );

        let target_rh =
            config::load_readonly_handler(&content_store, &manifest_name, "target").await?;
        let base_rh =
            config::load_readonly_handler(&content_store, base_name, "base").await?;

        let base_handle = {
            let h = Arc::clone(&base_rh.handler);
            tokio::task::spawn_blocking(move || export_full_layer(h))
        };
        let delta_handle = {
            let bh = Arc::clone(&base_rh.handler);
            let th = Arc::clone(&target_rh.handler);
            tokio::task::spawn_blocking(move || export_delta_layer(bh, th))
        };

        let base_layer = base_handle
            .await
            .map_err(|e| anyhow::anyhow!("base export panicked: {e}"))??;
        let delta_layer = delta_handle
            .await
            .map_err(|e| anyhow::anyhow!("delta export panicked: {e}"))??;

        // Drop after both tasks complete so temp dirs stay alive during export.
        drop((base_rh, target_rh));

        vec![base_layer, delta_layer]
    } else {
        // Full publish: single layer.
        info!(manifest = %manifest_name, "exporting full layer");

        let rh =
            config::load_readonly_handler(&content_store, &manifest_name, "target").await?;
        let handler = Arc::clone(&rh.handler);

        let layer = tokio::task::spawn_blocking(move || export_full_layer(handler))
            .await
            .map_err(|e| anyhow::anyhow!("export panicked: {e}"))??;

        drop(rh);

        vec![layer]
    };

    // Upload layer blobs to S3 via streaming multipart upload.
    let oci_base = format!("{}/oci", base);
    for layer in &layers {
        let blob_path = ObjectPath::from(format!("{}/blobs/sha256/{}", oci_base, layer.digest));
        info!(digest = %layer.digest, size = layer.size, "uploading layer blob");
        upload_file(&*object_store, &blob_path, layer.temp_file.path()).await?;
    }

    // Build + upload config blob.
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
    let config_bytes = serde_json::to_vec(&config_json)?;
    let config_digest = hex_sha256(&config_bytes);
    let config_path = ObjectPath::from(format!("{}/blobs/sha256/{}", oci_base, config_digest));
    object_store
        .put(&config_path, config_bytes.clone().into())
        .await
        .context("failed to upload config blob")?;

    // Build manifest JSON.
    let layer_descriptors: Vec<serde_json::Value> = layers
        .iter()
        .map(|l| {
            serde_json::json!({
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": format!("sha256:{}", l.digest),
                "size": l.size
            })
        })
        .collect();

    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{}", config_digest),
            "size": config_bytes.len()
        },
        "layers": layer_descriptors
    });
    let manifest_bytes = serde_json::to_vec(&manifest_json)?;
    let manifest_digest = hex_sha256(&manifest_bytes);

    // Upload manifest by tag and by digest.
    let tag_path = ObjectPath::from(format!("{}/manifests/{}", oci_base, tag));
    let digest_path =
        ObjectPath::from(format!("{}/manifests/sha256/{}", oci_base, manifest_digest));

    object_store
        .put(&tag_path, manifest_bytes.clone().into())
        .await
        .context("failed to upload manifest by tag")?;
    object_store
        .put(&digest_path, manifest_bytes.into())
        .await
        .context("failed to upload manifest by digest")?;

    let elapsed = start.elapsed();
    let last = layers.last().expect("at least one layer");

    println!("Published '{}' as tag '{}' successfully:", manifest_name, tag);
    if base_manifest.is_some() {
        println!("  Mode:            incremental (delta)");
        println!("  Layers:          {}", layers.len());
    }
    println!("  Manifest digest: sha256:{}", manifest_digest);
    println!("  Layer digest:    sha256:{}", last.digest);
    println!(
        "  Layer size:      {:.1} MB",
        last.size as f64 / 1e6
    );
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());

    Ok(())
}

/// Stream a file to S3 via multipart upload, avoiding reading it all into memory.
async fn upload_file(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    file_path: &std::path::Path,
) -> Result<()> {
    let upload = store
        .put_multipart_opts(path, PutMultipartOptions::default())
        .await
        .context("failed to start multipart upload")?;
    let mut writer = WriteMultipart::new(upload);

    let mut file = tokio::fs::File::open(file_path).await?;
    let mut buf = vec![0u8; 8 * 1024 * 1024]; // 8 MB chunks
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        writer.put(bytes::Bytes::copy_from_slice(&buf[..n]));
    }

    writer.finish().await.context("failed to finish multipart upload")?;
    Ok(())
}
