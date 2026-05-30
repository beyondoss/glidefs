use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use tokio::sync::Notify;

use glidefs::block::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
use glidefs::block::content_store::ContentStore;
use glidefs::block::handler::BlockHandler;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};
use glidefs::config::Settings;
use glidefs::parse_object_store::parse_url_opts;

/// Fixed block size for the chunked architecture: 128KB.
const BLOCK_SIZE: u32 = 131_072;

/// Owns a read-only [`BlockHandler`] and the temporary directory backing its caches.
///
/// When dropped, the temp directory is cleaned up automatically.
pub struct ReadonlyHandler {
    pub handler: Arc<BlockHandler>,
    _temp_dir: tempfile::TempDir,
}

/// Parse the GlideFS config file and create an object store + base path.
pub fn setup_object_store(
    settings: &Settings,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    Ok((Arc::from(object_store), path_from_url.to_string()))
}

/// Load a volume manifest from S3 and create a read-only BlockHandler.
pub async fn load_readonly_handler(
    content_store: &Arc<ContentStore>,
    manifest_name: &str,
    label: &str,
) -> Result<ReadonlyHandler> {
    let manifest_data = content_store
        .get_manifest(manifest_name)
        .await
        .context("failed to fetch manifest from S3")?
        .ok_or_else(|| anyhow::anyhow!("manifest '{}' not found in S3", manifest_name))?;

    let (manifest_data, _etag) = manifest_data;
    let volume_manifest = VolumeManifest::deserialize(&manifest_data)
        .map_err(|e| anyhow::anyhow!("failed to deserialize volume manifest: {e}"))?;

    let device_size = volume_manifest.size;
    tracing::info!(device_size, label, "manifest loaded, setting up read-only handler");

    let temp_dir = tempfile::TempDir::new().context("failed to create temp dir")?;
    let cache_config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: format!("publish-{}", manifest_name.replace('/', "-")),
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
            direct: false, // ephemeral registry cache; buffered is fine
            io_uring: false, // ephemeral registry cache; psync avoids the idle-spin
        })
        .await
        .context("failed to open block cache")?,
    );

    let metrics = Arc::new(ExportMetrics::new());
    let flush_notify = Arc::new(Notify::const_new());

    let handler = Arc::new(BlockHandler::new(
        cache,
        Arc::clone(content_store),
        clean_cache,
        pack_index_cache,
        volume_manifest,
        device_size,
        true, // readonly
        metrics,
        Arc::new(AtomicU64::new(0f64.to_bits())),
        flush_notify,
        DEFAULT_FLUSH_THRESHOLD,
        None,
    ));

    Ok(ReadonlyHandler {
        handler,
        _temp_dir: temp_dir,
    })
}
