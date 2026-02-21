use crate::block::api::ApiServer;
use crate::block::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
use crate::block::router::ExportRouter;
use crate::block::server::NBDServer;
use crate::config::Settings;
use crate::parse_object_store::parse_url_opts;
use crate::task::spawn_named;
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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

    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
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

    let router = Arc::new(
        ExportRouter::new(crate::block::router::RouterConfig {
            object_store: Arc::clone(&object_store),
            db_path,
            cache_dir,
            block_size: nbd_config.block_size(),
            clean_cache,
            wal_sync: nbd_config.wal_sync(),
            max_s3_uploads: nbd_config.max_s3_uploads(),
            max_s3_downloads: nbd_config.max_s3_downloads(),
            default_blocks_per_pack: nbd_config.blocks_per_pack(),
            ublk_nr_queues: nbd_config.ublk_nr_queues(),
            nbd_dead_conn_timeout: nbd_config.nbd_dead_conn_timeout(),
        })
        .context("Failed to initialize export router")?,
    );

    // Discover exports from S3 (recovers exports created via API).
    // Device registration is deferred until after ublk recovery scanning.
    info!("Discovering exports from S3...");
    let discovered_count = match router.discover_exports().await {
        Ok(discovered) => {
            use futures::stream::{self, StreamExt};

            let total = discovered.len();
            info!("Found {} export(s) in S3, recovering in parallel...", total);
            let count: usize = stream::iter(discovered)
                .map(|config| {
                    let router = Arc::clone(&router);
                    async move {
                        let name = config.name.clone();
                        match router.create_export(config, false, None).await {
                            Ok(()) => {
                                info!("Restored export '{}'", name);
                                1
                            }
                            Err(e) => {
                                warn!("Failed to restore export '{}': {}", name, e);
                                0
                            }
                        }
                    }
                })
                .buffer_unordered(16)
                .fold(0usize, |acc, n| async move { acc + n })
                .await;
            info!("Discovered {}/{} export(s) from S3", count, total);
            count
        }
        Err(e) => {
            warn!(
                "Failed to discover exports from S3: {} (continuing with config)",
                e
            );
            0
        }
    };

    // Load static exports from config (can add new exports or override discovered ones).
    // Device registration is deferred until after ublk recovery scanning.
    let exports = nbd_config.get_exports();
    if exports.is_empty() && discovered_count == 0 {
        return Err(anyhow::anyhow!(
            "No exports configured or discovered. Add exports to your config file or use the API to create them."
        ));
    }

    {
        use futures::stream::{self, StreamExt};

        let errors: Vec<_> = stream::iter(exports)
            .map(|config| {
                let router = Arc::clone(&router);
                async move {
                    let name = config.name.clone();
                    info!("Loading static export '{}' ({}GB)", name, config.size_gb);
                    router
                        .create_export(config.clone(), false, None)
                        .await
                        .with_context(|| format!("Failed to create export '{}'", name))?;
                    if let Err(e) = router.save_export(&config).await {
                        warn!("Failed to persist export '{}' to S3: {}", name, e);
                    }
                    Ok(())
                }
            })
            .buffer_unordered(16)
            .filter_map(|r: Result<()>| async { r.err() })
            .collect()
            .await;

        if let Some(first) = errors.into_iter().next() {
            return Err(first);
        }
    }

    // Recover QUIESCED ublk devices from a previous daemon crash.
    // Must happen after all exports are created so handlers exist for matching.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    {
        let recovered = router.recover_ublk_devices().await;
        if recovered > 0 {
            info!("Recovered {} QUIESCED ublk device(s)", recovered);
        }
    }

    // Register kernel block devices for exports that don't already have one.
    // Recovered ublk devices are already registered, so they'll be skipped.
    #[cfg(target_os = "linux")]
    {
        for export in router.list_exports().await {
            if router.get_device_path(&export.name).await.is_some() {
                continue; // already has a device (recovered or previously registered)
            }
            if let Err(e) = router
                .register_device(&export.name, &export.transport)
                .await
            {
                warn!(
                    "Failed to register device for '{}': {}",
                    export.name, e
                );
            }
        }
    }

    let mut handles = Vec::new();

    // Start background scrubber (integrity verification)
    {
        use crate::block::scrubber::{ScrubberConfig, scrubber};
        let bps = nbd_config.scrubber_blocks_per_second();
        if bps > 0 {
            info!("Starting background scrubber ({} blocks/sec)", bps);
            let cc = Arc::clone(router.clean_cache());
            let pi = Arc::clone(router.pack_index());
            let sm = Arc::clone(router.scrubber_metrics());
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("scrubber", async move {
                scrubber(
                    cc,
                    pi,
                    ScrubberConfig {
                        blocks_per_second: bps,
                    },
                    sm,
                    shutdown_clone,
                )
                .await;
                Ok(())
            }));
        }
    }

    // Start SSD capacity monitor (backpressure)
    {
        use crate::block::capacity_monitor::{CapacityConfig, capacity_monitor};
        info!("Starting SSD capacity monitor");
        let router_clone = Arc::clone(&router);
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("capacity-monitor", async move {
            capacity_monitor(router_clone, CapacityConfig::default(), shutdown_clone).await;
            Ok(())
        }));
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
        info!(
            "Starting NBD server on Unix socket {}",
            socket_path.display()
        );
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
        let device_info = export
            .device
            .as_ref()
            .map(|p| format!(", device={}", p.display()))
            .unwrap_or_default();
        info!(
            "  - {} ({}GB, {}, transport={}{})",
            export.name,
            export.size / 1_000_000_000,
            if export.readonly {
                "readonly"
            } else {
                "read-write"
            },
            export.transport,
            device_info,
        );
    }
    info!("Send SIGUSR1 to drain all exports to S3 (for node maintenance)");

    // Set up signal handlers
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigusr1 =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;

    // Track whether SIGUSR1 was received before shutdown.
    // SIGUSR1 → SIGTERM = hot reload (keep NBD devices alive for NBD_CMD_RECONFIGURE).
    // SIGTERM alone = full shutdown (disconnect all devices).
    let mut hot_reload = false;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT, initiating graceful shutdown...");
                break;
            }
            _ = sigterm.recv() => {
                if hot_reload {
                    info!("Received SIGTERM after SIGUSR1, hot reload shutdown (NBD devices stay alive)...");
                } else {
                    info!("Received SIGTERM, initiating full shutdown...");
                }
                break;
            }
            _ = sigusr1.recv() => {
                info!("Received SIGUSR1, draining all exports to S3 (preparing for hot reload)...");
                hot_reload = true;
                let failed = router.drain_all().await;
                if failed.is_empty() {
                    info!("Drain complete - all exports synced to S3. Send SIGTERM to restart.");
                } else {
                    tracing::error!(failed = ?failed, "Drain incomplete - {} export(s) failed", failed.len());
                }
            }
        }
    }

    let shutdown_timeout = nbd_config.shutdown_timeout();
    info!("Cancelling all servers...");
    shutdown.cancel();

    let shutdown_result = match tokio::time::timeout(shutdown_timeout, async {
        info!("Waiting for servers to exit...");
        for handle in handles {
            let _ = handle.await;
        }

        // Full shutdown: disconnect kernel devices.
        // Hot reload: leave NBD devices alive for NBD_CMD_RECONFIGURE.
        if !hot_reload {
            info!("Disconnecting kernel devices...");
            if let Err(e) = router.shutdown_devices().await {
                warn!("device shutdown failed: {}", e);
            }
        }

        info!("Final drain before shutdown...");
        router.shutdown().await
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            tracing::error!("Shutdown drain failed: {}", e);
            Err(anyhow::anyhow!(e))
        }
        Err(_) => {
            let msg = format!(
                "Shutdown timed out after {}s, exiting with possible dirty blocks",
                shutdown_timeout.as_secs()
            );
            warn!("{}", msg);
            Err(anyhow::anyhow!(msg))
        }
    };

    info!("Shutdown complete");
    shutdown_result
}
