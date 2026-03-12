use crate::block::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
use crate::block::content_store::ContentStore;
use crate::block::handler::BlockHandler;
use crate::block::metrics::ExportMetrics;
use crate::block::pack::DEFAULT_BLOCKS_PER_PACK;
use crate::block::pack_index_cache::PackIndexCache;
use crate::block::volume_manifest::VolumeManifest;
use crate::block::write_cache::{WriteCache, WriteCacheConfig};
use crate::config::Settings;
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use oci_registry::{Credentials, RegistryClient};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;
use tokio::sync::Notify;
use tracing::info;

/// Fixed block size for the chunked architecture: 128KB.
const BLOCK_SIZE: u32 = 131_072;

pub async fn run_push(
    manifest_name: String,
    image_ref: String,
    s3_prefix: String,
    config_path: PathBuf,
    base_manifest: Option<String>,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let start = Instant::now();

    // --- S3 setup ---
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = path_from_url.to_string();

    let base = format!("{}/exports/{}", db_path, s3_prefix);
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), &base));

    // --- Load target manifest from S3 ---
    info!(manifest = %manifest_name, "loading volume manifest from S3");

    let target_handler =
        load_readonly_handler(&content_store, &manifest_name, "target").await?;

    // --- Push to OCI registry ---
    let registry_client = RegistryClient::new();
    let image: oci_registry::Reference = image_ref
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid image reference: {e}"))?;

    let result = if let Some(ref base_name) = base_manifest {
        info!(
            image = %image_ref,
            target = %manifest_name,
            base = %base_name,
            "pushing incremental delta to registry"
        );

        let base_handler =
            load_readonly_handler(&content_store, base_name, "base").await?;

        crate::oci::push::push_delta_image(
            &registry_client,
            &image,
            &Credentials::Anonymous,
            base_handler,
            target_handler,
        )
        .await
        .map_err(|e| anyhow::anyhow!("delta push failed: {e}"))?
    } else {
        info!(image = %image_ref, manifest = %manifest_name, "pushing to registry");

        crate::oci::push::push_image(
            &registry_client,
            &image,
            &Credentials::Anonymous,
            target_handler,
        )
        .await
        .map_err(|e| anyhow::anyhow!("push failed: {e}"))?
    };

    let elapsed = start.elapsed();

    println!("Pushed '{}' to {} successfully:", manifest_name, image_ref);
    if base_manifest.is_some() {
        println!("  Mode:            incremental (delta)");
    }
    println!("  Manifest digest: {}", result.manifest_digest);
    println!("  Layer digest:    {}", result.layer_digest);
    println!(
        "  Layer size:      {:.1} MB",
        result.layer_size as f64 / 1e6
    );
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());

    Ok(())
}

/// Load a volume manifest from S3 and create a read-only BlockHandler.
///
/// Each call creates its own temp dir, cache, and handler — callers can create
/// multiple independent handlers for base/target comparisons.
async fn load_readonly_handler(
    content_store: &Arc<ContentStore>,
    manifest_name: &str,
    label: &str,
) -> Result<Arc<BlockHandler>> {
    let (manifest_data, _) = content_store
        .get_manifest(manifest_name)
        .await
        .context("failed to fetch manifest from S3")?
        .ok_or_else(|| anyhow::anyhow!("manifest '{}' not found in S3", manifest_name))?;

    let volume_manifest = VolumeManifest::deserialize(&manifest_data)
        .map_err(|e| anyhow::anyhow!("failed to deserialize volume manifest: {e}"))?;

    let device_size = volume_manifest.size;
    info!(device_size, label, "manifest loaded, setting up read-only handler");

    let temp_dir = tempfile::TempDir::new().context("failed to create temp dir")?;
    let cache_config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: format!("push-{}", manifest_name.replace('/', "-")),
        device_size,
        block_size: BLOCK_SIZE as usize,
        wal_sync: false,
    };

    let cache = Arc::new(WriteCache::open_fresh_active(cache_config)?);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(volume_manifest));

    let pack_index_cache = Arc::new(
        PackIndexCache::open(temp_dir.path())
            .await
            .context("failed to open pack index cache")?,
    );

    let foyer_dir = temp_dir.path().join("foyer");
    std::fs::create_dir_all(&foyer_dir)?;
    let clean_cache: Arc<dyn BlockCache> = Arc::new(
        FoyerBlockCache::open(FoyerCacheConfig {
            memory_bytes: 64 * 1024 * 1024,
            ssd_bytes: 256 * 1024 * 1024,
            ssd_dir: foyer_dir,
        })
        .await
        .context("failed to open block cache")?,
    );

    let metrics = Arc::new(ExportMetrics::new());
    let flush_notify = Arc::new(Notify::const_new());

    let handler = Arc::new(BlockHandler::new(
        Arc::clone(&cache),
        Arc::clone(content_store),
        Arc::clone(&clean_cache),
        Arc::clone(&pack_index_cache),
        Arc::clone(&volume_manifest),
        device_size,
        true, // readonly
        metrics,
        Arc::new(AtomicU64::new(0f64.to_bits())),
        flush_notify,
        DEFAULT_BLOCKS_PER_PACK,
        None,
    ));

    // Leak temp_dir so it lives as long as the handler.
    // The OS will clean up temp files on process exit.
    std::mem::forget(temp_dir);

    Ok(handler)
}
