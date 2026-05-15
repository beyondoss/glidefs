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
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Outcome of [`build_router_only`] — the heavy-lifting startup phase
/// shared by `run_server` and `run_server_as_successor`.
pub struct BuiltRouter {
    pub router: Arc<ExportRouter>,
    pub settings: Settings,
    pub nbd_connection_limiter: Arc<Semaphore>,
    pub api_connection_limiter: Arc<Semaphore>,
}

/// Initialize the tracing subscriber. Idempotent — second call is a no-op.
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));

    #[cfg(feature = "tokio-console")]
    {
        use tracing_subscriber::prelude::*;
        let console_layer = console_subscriber::spawn();
        let _ = tracing_subscriber::registry()
            .with(console_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .try_init();
    }

    #[cfg(not(feature = "tokio-console"))]
    {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .try_init();
    }
}

/// The slow part of daemon startup: load config, open foyer cache, build
/// router, discover exports from S3, load static exports, prefetch chunk
/// metadata. Does NOT recover ublk devices, register kernel devices, or
/// bind listeners — caller does those after the router is built (and
/// after the handoff protocol, in the successor case).
///
/// Used by both:
/// - [`run_server`] — cold-start: build_router_only → recover_ublk →
///   register_devices → bind listeners → signal loop.
/// - [`run_server_as_successor`] — handoff: build_router_only (while
///   predecessor still serves) → handoff::run_successor (kernel takeover)
///   → register_devices for new ones → bind listeners → signal loop.
pub async fn build_router_only(config_path: PathBuf) -> Result<BuiltRouter> {
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

    std::fs::create_dir_all(&cache_dir)?;

    crate::storage_compatibility::check_if_match_support(&object_store, &db_path).await?;

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
            max_exports: nbd_config.max_exports(),
        })
        .await
        .context("Failed to initialize export router")?,
    );

    let nbd_connection_limiter = Arc::new(Semaphore::new(nbd_config.max_connections()));
    let api_connection_limiter = Arc::new(Semaphore::new(nbd_config.api_max_connections()));
    info!(
        "Connection budgets: NBD={}, API={}, exports max={}",
        nbd_config.max_connections(),
        nbd_config.api_max_connections(),
        nbd_config.max_exports(),
    );

    // Discover exports from S3.
    info!("Discovering exports from S3...");
    let discovered_count = match router.discover_exports().await {
        Ok(discovered) => {
            use futures::stream::{self, StreamExt};
            let total = discovered.len();
            info!("Found {} export(s) in S3, recovering in parallel...", total);

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

            for prefix in &s3_prefixes {
                router.prewarm_base_caches(prefix).await;
            }

            count
        }
        Err(e) => {
            warn!("Failed to discover exports from S3: {} (continuing with config)", e);
            0
        }
    };

    // Load static exports from config.
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

    // Prefetch chunk metadata from S3.
    {
        let prefetched = router.prefetch_chunk_metas().await;
        if prefetched > 0 {
            info!("Prefetched {} chunk meta(s) from S3", prefetched);
        }
    }

    Ok(BuiltRouter {
        router,
        settings,
        nbd_connection_limiter,
        api_connection_limiter,
    })
}

pub async fn run_server(config_path: PathBuf) -> Result<()> {
    init_tracing();

    let built = build_router_only(config_path.clone()).await?;

    // Recover QUIESCED ublk devices from a previous daemon crash.
    // Must happen after all exports are created so handlers exist for matching.
    // Successor-mode entry does its own handoff-based recovery and skips this.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    {
        let recovered = built.router.recover_ublk_devices().await;
        if recovered > 0 {
            info!("Recovered {} QUIESCED ublk device(s)", recovered);
        }
    }

    serve_with_router(built, config_path).await
}

/// Listener-binding + signal-loop portion of daemon lifecycle. Shared
/// between `run_server` (cold-start path) and `run_server_as_successor`
/// (handoff path).
///
/// On entry: `built.router` has all exports created and (for cold-start)
/// any QUIESCED ublk devices already recovered. This function registers
/// any remaining kernel devices, binds listeners, and enters the signal
/// loop until shutdown.
async fn serve_with_router(built: BuiltRouter, config_path: PathBuf) -> Result<()> {
    let BuiltRouter {
        router,
        settings,
        nbd_connection_limiter,
        api_connection_limiter,
    } = built;

    let nbd_config = settings
        .servers
        .nbd
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("NBD server configuration is required"))?;

    let shutdown = CancellationToken::new();

    // Register kernel block devices for exports that don't already have one.
    // Recovered ublk devices are already registered, so they'll be skipped.
    // Parallel so 1000-device cold-start doesn't serialize behind ublk control
    // ioctls — each registration is independent.
    #[cfg(target_os = "linux")]
    {
        use futures::stream::{FuturesUnordered, StreamExt};
        const REGISTER_CONCURRENCY: usize = 16;

        let mut to_register = Vec::new();
        for export in router.list_exports().await {
            if router.get_device_path(&export.name).await.is_some() {
                continue;
            }
            to_register.push((export.name, export.transport));
        }

        let router_clone = Arc::clone(&router);
        let register_one = move |name: String, transport: String| {
            let router = Arc::clone(&router_clone);
            Box::pin(async move {
                let res = router.register_device(&name, &transport).await;
                (name, res)
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = (String, Result<(), _>)> + Send>,
                >
        };

        let mut pending = FuturesUnordered::new();
        let mut iter = to_register.into_iter();
        for _ in 0..REGISTER_CONCURRENCY {
            if let Some((name, transport)) = iter.next() {
                pending.push(register_one(name, transport));
            }
        }
        while let Some((name, res)) = pending.next().await {
            if let Err(e) = res {
                warn!("Failed to register device for '{}': {}", name, e);
            }
            if let Some((next_name, next_transport)) = iter.next() {
                pending.push(register_one(next_name, next_transport));
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
            let nbd_server = NBDServer::new_tcp_with_limiter(
                Arc::clone(&router),
                *addr,
                Arc::clone(&nbd_connection_limiter),
            );
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
        let nbd_server = NBDServer::new_unix_with_limiter(
            Arc::clone(&router),
            socket_path,
            Arc::clone(&nbd_connection_limiter),
        );
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("nbd-unix", async move {
            nbd_server.start(shutdown_clone).await
        }));
    }

    // Start HTTP API server
    if let Some(api_addr) = nbd_config.api_address {
        info!("Starting HTTP API server on {}", api_addr);
        let api_server = ApiServer::new_with_limiter(
            Arc::clone(&router),
            api_addr,
            Arc::clone(&api_connection_limiter),
        );
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
    let mut sighup =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;

    // Set to true when a successful handoff completes; the signal loop
    // breaks and the daemon proceeds to shutdown.
    let mut handoff_succeeded = false;
    // Mutex to ensure only one handoff runs at a time.
    let handoff_in_progress = Arc::new(tokio::sync::Mutex::new(()));

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
            _ = sighup.recv() => {
                let Ok(_guard) = handoff_in_progress.clone().try_lock_owned() else {
                    warn!("SIGHUP received but a handoff is already in flight; ignoring");
                    continue;
                };
                info!("Received SIGHUP, initiating graceful handoff...");
                let router_clone = Arc::clone(&router);
                let config_clone = config_path.clone();

                // Run the handoff inline (in this signal-loop task). We
                // need its outcome before deciding whether to break the
                // loop, and the handoff itself only takes ~hundreds of
                // ms; blocking the signal loop for that long is fine.
                match run_predecessor_handoff(router_clone, config_clone).await {
                    Ok(outcome) => {
                        tracing::info!(?outcome, "handoff complete");
                        if matches!(outcome, crate::handoff::HandoffOutcome::Succeeded { .. }) {
                            info!("handoff succeeded — predecessor exiting to release listener fds");
                            handoff_succeeded = true;
                            break;
                        }
                        // Aborted or Revived — keep serving.
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "handoff failed; predecessor continues serving");
                    }
                }
                drop(_guard);
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

        if handoff_succeeded {
            // Handoff path: successor inherits every export's state
            // (including any dirty blocks the predecessor hadn't yet
            // flushed). Draining here is redundant — it would re-upload
            // bytes the successor already knows about — and on backends
            // that don't support all S3 operations (e.g., LocalFilesystem
            // for testing) can hang indefinitely. The predecessor's
            // `freeze_all()` already fsynced WALs before the cutover, so
            // disk state is durable.
            info!("Handoff-succeeded shutdown: skipping S3 drain (successor owns state)");
            Ok::<(), crate::block::router::RouterError>(())
        } else {
            // M6.1: do NOT call `router.shutdown_devices()` (kill_dev on
            // every device). The ublk path leaves devices QUIESCED for
            // the next daemon to recover; NBD's `dead_conn_timeout`
            // covers its hot-reload case. `router.shutdown` below drains
            // dirty write_caches to S3 cleanly.
            info!("Final drain before shutdown...");
            router.shutdown().await
        }
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

/// Drive the predecessor side of a graceful handoff.
///
/// This is the SIGHUP / `glidefs handoff` entry point. It blocks until
/// the handoff completes (success, abort, or revival) — the caller is
/// the signal-loop task in `run_server`.
async fn run_predecessor_handoff(
    router: Arc<ExportRouter>,
    config_path: PathBuf,
) -> Result<crate::handoff::HandoffOutcome> {
    let socket_path = std::path::PathBuf::from(crate::handoff::DEFAULT_HANDOFF_SOCKET);
    let successor_binary =
        std::env::current_exe().context("resolving current binary path for successor exec")?;
    let timeouts = crate::handoff::HandoffTimeouts::default();
    crate::handoff::run_predecessor(
        router,
        &socket_path,
        &successor_binary,
        &config_path,
        timeouts,
    )
    .await
}

/// Successor-mode entry: cold-start the router (WARMING — happens while
/// the predecessor still serves I/O), drive the handoff state machine to
/// take over from the predecessor, then enter the normal serving loop.
///
/// Called from `main.rs` when argv contains `--handoff-from <socket>`.
pub async fn run_server_as_successor(socket_path: PathBuf) -> Result<()> {
    init_tracing();

    info!(socket = %socket_path.display(), "starting in successor mode");

    // The predecessor passes `--config <path>` to us. Sniff it from argv
    // without invoking clap (clap doesn't know about `--handoff-from`).
    let config_path = crate::handoff::successor::config_arg().ok_or_else(|| {
        anyhow::anyhow!(
            "successor mode requires --config <path> in argv (predecessor should forward it)"
        )
    })?;

    info!(config = %config_path.display(), "successor: WARMING with config");

    // WARMING: build the router from disk + S3. The predecessor is still
    // serving I/O while we do this — the whole point of the design.
    let built = build_router_only(config_path.clone()).await?;

    // Drive the handoff protocol. After this returns, our router owns
    // every device the predecessor used to own.
    let timeouts = crate::handoff::HandoffTimeouts::default();
    let _takeover = crate::handoff::run_successor(
        &socket_path,
        Arc::clone(&built.router),
        timeouts,
    )
    .await
    .context("handoff successor takeover failed")?;

    info!("successor: takeover complete, entering serve loop");

    // Phase 1 MVP: wait briefly for the predecessor to release its
    // listener fds (NBD TCP/Unix, HTTP API). Predecessor's signal loop
    // breaks on successful handoff and drops listeners as part of
    // shutdown; this typically takes tens of ms. Phase 2 will fix this
    // properly via SCM_RIGHTS fd-passing.
    //
    // We poll-with-retry on bind rather than a fixed sleep: faster on
    // the common case (~30ms predecessor exit) and bounded by the
    // overall serve_with_router timeout.
    let api_port = built
        .settings
        .servers
        .nbd
        .as_ref()
        .and_then(|n| n.api_address.map(|a| a.port()));
    if let Some(port) = api_port {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut delay = std::time::Duration::from_millis(5);
        loop {
            // Try to bind the API port to see if it's free yet.
            match std::net::TcpListener::bind(("127.0.0.1", port)) {
                Ok(l) => {
                    drop(l);
                    break;
                }
                Err(_) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_millis(50));
                }
                Err(e) => {
                    warn!(error = %e, "successor: API port still in use after 5s; binding anyway");
                    break;
                }
            }
        }
    }

    serve_with_router(built, config_path).await
}

/// Background task: accept connections on the handoff control socket
/// and turn each one into a SIGHUP-equivalent trigger.
///
/// Wire protocol is one byte each way (see `crate::cli::handoff_cmd::wire`).
/// Reserved for Phase 2 — Phase 1 MVP relies on SIGHUP only.
#[allow(dead_code)]
async fn run_handoff_control_socket(
    socket_path: PathBuf,
    trigger: tokio::sync::mpsc::Sender<()>,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    let _ = std::fs::remove_file(&socket_path);
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("binding handoff control socket {}", socket_path.display()))?;
    info!(socket = %socket_path.display(), "handoff control socket listening");

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "handoff control socket accept failed");
                continue;
            }
        };

        let trigger = trigger.clone();
        tokio::spawn(async move {
            let mut req = [0u8; 1];
            if stream.read_exact(&mut req).await.is_err() {
                return;
            }
            let response_byte = match req[0] {
                crate::cli::handoff_cmd::wire::REQUEST_HANDOFF => {
                    match trigger.try_send(()) {
                        Ok(()) => crate::cli::handoff_cmd::wire::RESPONSE_ACCEPTED,
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            crate::cli::handoff_cmd::wire::RESPONSE_BUSY
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            crate::cli::handoff_cmd::wire::RESPONSE_UNSUPPORTED
                        }
                    }
                }
                _ => crate::cli::handoff_cmd::wire::RESPONSE_UNSUPPORTED,
            };
            let _ = stream.write_all(&[response_byte]).await;
        });
    }
}
