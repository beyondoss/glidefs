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

    let db_path_for_prewarm = db_path.clone();
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
            default_flush_threshold: nbd_config.flush_threshold(),
            ublk_nr_queues: settings.ublk_nr_queues(),
            nbd_dead_conn_timeout: nbd_config.nbd_dead_conn_timeout(),
        })
        .await
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

            // Collect unique S3 prefixes for base cache pre-warming.
            let s3_prefixes: std::collections::HashSet<String> = discovered
                .iter()
                .map(|c| format!("{}/exports/{}", &db_path_for_prewarm, c.s3_prefix()))
                .collect();

            let count: usize = stream::iter(discovered)
                .map(|config| {
                    let router = Arc::clone(&router);
                    async move {
                        let name = config.name.clone();
                        match router.create_export(config, false, None, None).await {
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

            // Pre-warm base manifest and hot set caches so the first fork
            // from each base avoids an S3 round-trip.
            for prefix in &s3_prefixes {
                router.prewarm_base_caches(prefix).await;
            }

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
                        .create_export(config.clone(), false, None, None)
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

    // Prefetch chunk metadata from S3 into ChunkMetaCache.
    // On a warm restart (same SSD), this is mostly a no-op (SSD cache already has the files).
    // On a cold start (new host), this hides S3 latency before VMs issue their first reads.
    {
        let prefetched = router.prefetch_chunk_metas().await;
        if prefetched > 0 {
            info!("Prefetched {} chunk meta(s) from S3", prefetched);
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

    // Background scrubber: verifies cached blocks by re-hashing.
    {
        use crate::block::scrubber::{RouterHashSource, ScrubberConfig, scrubber};

        let bps = nbd_config.scrubber_blocks_per_second();
        if bps > 0 {
            info!("Starting background scrubber ({} blocks/sec)", bps);
            let hash_source: Arc<dyn crate::block::scrubber::HashSource> =
                Arc::new(RouterHashSource::new(Arc::clone(&router)));
            let clean_cache = Arc::clone(router.clean_cache());
            let metrics = Arc::clone(router.scrubber_metrics());
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("scrubber", async move {
                scrubber(
                    clean_cache,
                    hash_source,
                    ScrubberConfig {
                        blocks_per_second: bps,
                    },
                    metrics,
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
            export.size / 1_073_741_824,
            if export.readonly {
                "readonly"
            } else {
                "read-write"
            },
            export.transport,
            device_info,
        );
    }
    info!("Send SIGUSR1 to drain all exports to S3 (without exiting)");
    info!("Send SIGTERM to drain + exit (kernel ublk devices stay QUIESCED for next daemon)");

    // Set up signal handlers.
    //
    // **M6.2 (post-incident fix)**: SIGTERM now always drains to S3
    // before exiting — the previous "SIGUSR1 required for clean
    // drain, SIGTERM alone = destructive teardown" pattern was the
    // root cause of the M0/M1 deploy deadlock (`systemctl restart`
    // delivers SIGTERM directly, the daemon's destructive path called
    // `kill_dev` while VMs were writing, kernel deadlocked).
    //
    // SIGUSR1 still exists as a "drain-without-exit" signal for
    // operator-driven flushes during node maintenance.
    //
    // Both signals are now safe-by-default: even if a destructive
    // path slipped through, the M6.1 ublk shutdown no longer calls
    // `kill_dev` on the hot path — the kernel auto-quiesces via
    // `UBLK_F_USER_RECOVERY` and the next daemon recovers.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigusr1 =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;

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
                info!("Received SIGUSR1, draining all exports to S3 (no exit)...");
                let failed = router.drain_all().await;
                if failed.is_empty() {
                    info!("Drain complete — all exports synced to S3.");
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

        // M6.1: do NOT call `router.shutdown_devices()` (kill_dev on
        // every device). The ublk path leaves devices QUIESCED for
        // the next daemon to recover; NBD's `dead_conn_timeout`
        // covers its hot-reload case. `router.shutdown` below drains
        // dirty write_caches to S3 cleanly.
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
