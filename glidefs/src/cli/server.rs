use crate::config::Settings;
use crate::nbd::api::ApiServer;
use crate::nbd::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
use crate::nbd::router::ExportRouter;
use crate::nbd::server::NBDServer;
use crate::parse_object_store::parse_url_opts;
use crate::task::spawn_named;
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn run_server(config_path: PathBuf) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));

    #[cfg(feature = "tokio-console")]
    {
        use tracing_subscriber::prelude::*;
        let console_layer = console_subscriber::spawn();
        tracing_subscriber::registry()
            .with(console_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .init();
    }

    #[cfg(not(feature = "tokio-console"))]
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();

    let (object_store, path_from_url) = parse_url_opts(&url.parse()?, env_vars.into_iter())?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);

    let db_path = path_from_url.to_string();

    info!("Starting GlideFS NBD server with {} backend", object_store);
    info!("Storage path: {}", db_path);

    let cache_dir = settings.cache.dir.clone();
    info!("Cache directory: {}", cache_dir.display());

    // Create cache directory if it doesn't exist
    std::fs::create_dir_all(&cache_dir)?;

    crate::storage_compatibility::check_if_match_support(&object_store, &db_path).await?;

    let shutdown = CancellationToken::new();

    let nbd_config = settings
        .servers
        .nbd
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("NBD server configuration is required"))?;

    // Create the export router
    let auto_create_size_gb = nbd_config.auto_create_size_gb();
    if let Some(size_gb) = auto_create_size_gb {
        info!(
            "Auto-create enabled: exports will be created on first NBD connect ({}GB default)",
            size_gb
        );
    }

    // Shared clean block cache — foyer HybridCache with memory + SSD tiers
    let memory_bytes =
        (settings.cache.memory_size_gb.unwrap_or(1.0) * 1024.0 * 1024.0 * 1024.0) as usize;
    let ssd_bytes =
        (settings.cache.ssd_cache_size_gb.unwrap_or(10.0) * 1024.0 * 1024.0 * 1024.0) as usize;
    let foyer_dir = cache_dir.join("foyer");
    info!(
        "Opening clean cache: {}MB memory, {}GB SSD at {}",
        memory_bytes / (1024 * 1024),
        ssd_bytes / (1024 * 1024 * 1024),
        foyer_dir.display(),
    );
    let clean_cache: Arc<dyn BlockCache> = Arc::new(
        FoyerBlockCache::open(FoyerCacheConfig {
            memory_bytes,
            ssd_bytes,
            ssd_dir: foyer_dir,
        })
        .await
        .context("Failed to open foyer clean cache")?,
    );

    let router = Arc::new(ExportRouter::new(
        Arc::clone(&object_store),
        db_path,
        cache_dir,
        nbd_config.block_size(),
        nbd_config.blocks_per_batch(),
        nbd_config.sync_delay_ms(),
        nbd_config.dirty_budget_gb(),
        auto_create_size_gb,
        clean_cache,
    ));

    // Discover exports from S3 (recovers exports created via API)
    info!("Discovering exports from S3...");
    let mut discovered_count = 0;
    match router.discover_exports().await {
        Ok(discovered) => {
            for config in discovered {
                info!(
                    "Discovered export '{}' ({}GB) from S3",
                    config.name, config.size_gb
                );
                if let Err(e) = router.create_export(config.clone(), false, None).await {
                    tracing::warn!("Failed to restore export '{}': {}", config.name, e);
                } else {
                    discovered_count += 1;
                }
            }
        }
        Err(e) => {
            tracing::warn!("Failed to discover exports from S3: {} (continuing with config)", e);
        }
    }
    if discovered_count > 0 {
        info!("Discovered {} export(s) from S3", discovered_count);
    }

    // Load static exports from config (can add new exports or override discovered ones)
    let exports = nbd_config.get_exports();
    if exports.is_empty() && auto_create_size_gb.is_none() && discovered_count == 0 {
        return Err(anyhow::anyhow!(
            "No exports configured or discovered. Add exports to your config file, enable auto_create_size_gb, or use the API to create them."
        ));
    }

    for export_config in exports {
        info!(
            "Loading static export '{}' ({}GB)",
            export_config.name, export_config.size_gb
        );
        router
            .create_export(export_config.clone(), false, None)
            .await
            .with_context(|| format!("Failed to create export '{}'", export_config.name))?;
    }

    let mut handles = Vec::new();

    // Start background scrubber (integrity verification)
    {
        use crate::nbd::scrubber::{scrubber, ScrubberConfig};
        let bps = nbd_config.scrubber_blocks_per_second();
        if bps > 0 {
            info!("Starting background scrubber ({} blocks/sec)", bps);
            let cc = Arc::clone(router.clean_cache());
            let pi = Arc::clone(router.pack_index());
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("scrubber", async move {
                scrubber(cc, pi, ScrubberConfig { blocks_per_second: bps }, shutdown_clone).await;
                Ok(())
            }));
        }
    }

    // Start NBD TCP servers
    if let Some(addresses) = &nbd_config.addresses {
        for addr in addresses {
            info!("Starting NBD server on {}", addr);
            let nbd_server = NBDServer::new_tcp(Arc::clone(&router), *addr);
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("nbd-tcp", async move {
                nbd_server.start(shutdown_clone).await
            }));
        }
    }

    // Start NBD Unix socket server
    if let Some(socket_path) = nbd_config.unix_socket.as_ref() {
        info!("Starting NBD server on Unix socket {}", socket_path.display());
        let nbd_server = NBDServer::new_unix(Arc::clone(&router), socket_path);
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("nbd-unix", async move {
            nbd_server.start(shutdown_clone).await
        }));
    }

    // Start HTTP API server
    if let Some(api_addr) = nbd_config.api_address {
        info!("Starting HTTP API server on {}", api_addr);
        let api_server = ApiServer::new(Arc::clone(&router), api_addr);
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("http-api", async move {
            api_server.start(shutdown_clone).await
        }));
    }

    if handles.is_empty() {
        return Err(anyhow::anyhow!(
            "No servers started. Configure at least one of: addresses, unix_socket, or api_address"
        ));
    }

    info!("GlideFS ready. Available exports:");
    for export in router.list_exports().await {
        info!(
            "  - {} ({}GB, {})",
            export.name,
            export.size / 1_000_000_000,
            if export.readonly { "readonly" } else { "read-write" }
        );
    }
    info!("Send SIGUSR1 to drain all exports to S3 (for node maintenance)");

    // Set up signal handlers
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigusr1 = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT, initiating graceful shutdown...");
                break;
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, initiating graceful shutdown...");
                break;
            }
            _ = sigusr1.recv() => {
                info!("Received SIGUSR1, draining all exports to S3...");
                if let Err(e) = router.drain_all().await {
                    tracing::error!("Drain failed: {}", e);
                } else {
                    info!("Drain complete - all exports synced to S3");
                }
            }
        }
    }

    info!("Cancelling all servers...");
    shutdown.cancel();

    info!("Waiting for servers to exit...");
    for handle in handles {
        let _ = handle.await;
    }

    // Graceful shutdown: drain all exports
    info!("Final drain before shutdown...");
    if let Err(e) = router.shutdown().await {
        tracing::error!("Shutdown drain failed: {}", e);
    }

    info!("Shutdown complete");
    Ok(())
}
