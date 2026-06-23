//! End-to-end test for glidefs's kernel zero-copy ublk integration.
//!
//! Brings up an in-process glidefs router + ublk export and exercises a
//! data round-trip through `/dev/ublkbN`. On a kernel that advertises
//! `UBLK_F_SUPPORT_ZERO_COPY + UBLK_F_AUTO_BUF_REG`, glidefs auto-selects
//! the ZC transport (see `block::ublk::device::register_inner`) and the
//! ZC integration's `io_task_zero_copy` worker handles the I/Os.
//!
//! On older kernels, glidefs falls back to USER_COPY transparently and
//! this test still passes — same code path, different transport. That's
//! the symmetry we need to verify both conditions in CI.
//!
//! Skips when:
//!   - /dev/ublk-control is absent
//!   - the test isn't running as root
//!
//! Uses `#[tokio::test(flavor = "multi_thread")]` because the daemon-side
//! ZC integration `spawn`s async dispatch work onto the tokio runtime from
//! a non-tokio thread (the per-queue ZC io_uring loop).

#![cfg(all(target_os = "linux", feature = "ublk"))]

// Match production: the `glidefs` binary uses jemalloc. Default glibc
// malloc retains per-thread arenas indefinitely under the soak's
// short-thread + heavy-allocation pattern, making RSS look like a leak
// when it's allocator fragmentation. The leak detector in
// `zc_glidefs_soak` must measure the allocator we actually ship.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
use glidefs::block::router::{ExportRouter, RouterConfig};
use glidefs::block::ublk::UblkServer;
use glidefs::config::ExportConfig;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use tempfile::TempDir;

const DEVICE_SIZE_GB: f64 = 0.5; // 512 MiB
const EXPORT_NAME: &str = "zc-test";

fn skip_reason() -> Option<&'static str> {
    if !Path::new("/dev/ublk-control").exists() {
        return Some("/dev/ublk-control absent");
    }
    if unsafe { libc::geteuid() } != 0 {
        return Some("not root");
    }
    None
}

async fn setup_router() -> (Arc<ExportRouter>, TempDir) {
    setup_router_with_flush_threshold(DEFAULT_FLUSH_THRESHOLD).await
}

async fn setup_router_with_flush_threshold(
    default_flush_threshold: usize,
) -> (Arc<ExportRouter>, TempDir) {
    let (router, cache_dir, _s3) =
        setup_router_full(default_flush_threshold, Arc::new(InMemory::new())).await;
    (router, cache_dir)
}

/// Like [`setup_router_with_flush_threshold`] but returns the typed
/// `Arc<InMemory>` so the caller can attach a periodic-GC task that
/// simulates production's out-of-band pack reaper. The default
/// `setup_router*` helpers can't return the typed handle because they
/// erase to `Arc<dyn ObjectStore>` at construction time.
async fn setup_router_full(
    default_flush_threshold: usize,
    s3: Arc<InMemory>,
) -> (Arc<ExportRouter>, TempDir, Arc<InMemory>) {
    let cache_dir = TempDir::new().unwrap();
    let s3_dyn: Arc<dyn ObjectStore> = s3.clone();
    let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let router = Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: s3_dyn,
            db_path: "zc-test".to_string(),
            cache_dir: cache_dir.path().to_path_buf(),
            block_size: 4096,
            clean_cache,
            pack_index_cache: None,
            wal_sync: false,
            max_s3_uploads: 8,
            max_s3_downloads: 16,
            default_flush_threshold,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
            max_exports: 10,
            manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
            profile: None,
        })
        .await
        .expect("router"),
    );
    let config = ExportConfig {
        name: EXPORT_NAME.to_string(),
        size_gb: DEVICE_SIZE_GB,
        s3_prefix: None,
        block_size: None,
        flush_threshold: None,
        flush_mode: None,
        transport: None,
        compaction_cooldown: None,
        source: None,
    };
    router
        .create_export(config, false, None, None)
        .await
        .expect("create_export");
    (router, cache_dir, s3)
}

/// Single-chunk 4 KiB write → 4 KiB read. Smoke test for the hot path
/// (post-write block is DIRTY → `ChunkSource::LocalSsd` → `READ_FIXED`).
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_roundtrip_4k() {
    run_scenario("zc-4k", |dev| {
        write_pattern(dev, 0, 4096)?;
        verify_pattern(dev, 0, 4096)?;
        Ok(())
    })
    .await;
}

/// 32-chunk read: write 128 KiB sequentially (across 32×4 KiB blocks),
/// then read 128 KiB starting at offset 0 in one I/O. The ZC dispatch
/// emits 32 `READ_FIXED` SQEs all targeting `buf_index=tag` at increasing
/// `buf_offset`. Exercises the multi-chunk SQE fan-out + per-tag CQE
/// aggregation in `run_zc_queue`.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_multi_chunk_128k() {
    run_scenario("zc-multi-chunk", |dev| {
        write_pattern(dev, 0, 128 * 1024)?;
        verify_pattern(dev, 0, 128 * 1024)?;
        Ok(())
    })
    .await;
}

/// Cold read: read a never-written block. The plan resolves to
/// `ChunkSource::Zero` (block is NOT_PRESENT and S3 is empty) — dispatch
/// uses `/dev/zero` as the `READ_FIXED` source. No userspace memset, no
/// memfd staging.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_cold_zero_read() {
    run_scenario("zc-cold-zero", |dev| {
        let mut readback = aligned_4k(&vec![0u8; 4096]);
        let mut file = open_direct(dev, false)?;
        use std::io::{Read, Seek, SeekFrom};
        file.seek(SeekFrom::Start(64 * 1024))?;
        file.read_exact(&mut readback)?;
        assert!(readback.iter().all(|&b| b == 0), "cold read non-zero");
        Ok(())
    })
    .await;
}

/// Mixed read: write block A, leave block B unwritten, read across both
/// in one I/O. ReadPlan has one `LocalSsd` entry + one `Zero` entry.
/// Exercises multi-chunk dispatch with heterogeneous chunk sources.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_mixed_dirty_and_zero() {
    run_scenario("zc-mixed", |dev| {
        // Write block 0 (offset 0).
        write_pattern(dev, 0, 4096)?;
        // Read [0, 8192) — block 0 DIRTY, block 1 still NOT_PRESENT.
        let mut readback = aligned_4k(&vec![0u8; 8192]);
        let mut file = open_direct(dev, false)?;
        use std::io::{Read, Seek, SeekFrom};
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut readback)?;
        let pattern: Vec<u8> = (0..4096usize).map(|i| (i & 0xff) as u8).collect();
        assert_eq!(&readback[..4096], pattern.as_slice(), "block 0 mismatch");
        assert!(readback[4096..].iter().all(|&b| b == 0), "block 1 not zero");
        Ok(())
    })
    .await;
}

/// Cross-block write: write 8 KiB straddling block boundary 0..1.
/// `pre_write` backfills + sets present for both blocks, then the kernel
/// `WRITE_FIXED` lands the bio into the cache at offset. Read it back.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_cross_block_write_8k() {
    run_scenario("zc-cross-block", |dev| {
        write_pattern(dev, 0, 8192)?;
        verify_pattern(dev, 0, 8192)?;
        Ok(())
    })
    .await;
}

/// Soak test: write-verify loop at sustained QD against many concurrent
/// rotations, for a wall-clock budget. Each cycle writes the entire
/// device with a generation-tagged pattern, then reads it back to
/// verify. After the budget elapses, snapshot RSS + open-FD count and
/// fail if either has grown unboundedly.
///
/// What this catches that the short rotation-race test doesn't:
/// - Memory leaks in the dispatch path (Vec<ZcChunk> allocations,
///   `Box<Keepalive>`s, mpsc backlog, tokio task accumulation).
/// - FD leaks (every queue dups `/dev/zero`; per-IO file handles).
/// - Rare-race accumulation — a 0.1%-per-cycle race that the 0.5s
///   rotation_race might miss surfaces within thousands of cycles.
///
/// Default duration 10s; override with `GLIDEFS_SOAK_DURATION_S=N`.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_soak() {
    if let Some(why) = skip_reason() {
        eprintln!("skip [zc-soak]: {why}");
        return;
    }
    if std::env::var("GLIDEFS_TEST_FORCE_USER_COPY").is_ok_and(|v| !v.is_empty()) {
        // The soak's flush thresholds and concurrency are tuned for the
        // ZC dispatch path. Under forced USER_COPY the per-IO syscall
        // bounce can't drain the device fast enough on a small-CPU
        // QEMU VM and the test wedges. USER_COPY's data-plane is
        // covered by the other tests in this file (cross-block,
        // mixed-dirty, multi-chunk, rotation-race).
        eprintln!("skip [zc-soak]: USER_COPY transport (soak is ZC-tuned)");
        return;
    }
    // Init a stderr tracing subscriber so glidefs internal diagnostics
    // (Zero-source returns, InMemory-of-zeros, promote anomalies) surface
    // when the soak fails — this is exactly the data we need to root-
    // cause a byte mismatch.
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("GLIDEFS_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,glidefs=info")),
        )
        .try_init();
    let duration = Duration::from_secs(
        std::env::var("GLIDEFS_SOAK_DURATION_S")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(10),
    );

    // Moderate flush_threshold (1024 blocks = 4 MiB at 4 KiB blocks):
    // every cycle triggers ~8 flushes so rotation IS exercised, but the
    // cascade-prone aggressive settings (256 or below) that wedge QEMU
    // under sustained load are avoided. We have a separate dedicated
    // rotation-race test (`zc_glidefs_rotation_race_under_load`) that
    // hammers rotation with flush_threshold=4 in a short burst.
    let (router, _cache_dir, s3) =
        setup_router_full(DEFAULT_FLUSH_THRESHOLD, Arc::new(InMemory::new())).await;
    let handler = router.get_handler(EXPORT_NAME).await.expect("handler");

    // Simulate production's out-of-band pack GC. The `InMemory`
    // object-store mock retains every uploaded pack forever; production
    // S3 does the same, but a separate GC process (the
    // `glidefs gc` CLI) reaps compaction-orphaned packs on a schedule.
    // Without a similar reaper here the soak's RSS would grow linearly
    // with cycle count — not because of a glidefs leak, but because the
    // test S3 mock accumulates the same compacted/deduplicated pack
    // bytes that real S3 would also keep around (and that the long-run
    // memory check should NOT attribute to the daemon under test).
    //
    // The reaper:
    //   * walks the mock every 250 ms,
    //   * sorts entries by `last_modified`,
    //   * deletes anything older than `STALE_MS` (the manifest-and-
    //     freshly-uploaded-pack window) once total bytes exceed the
    //     budget.
    //
    // It only deletes packs older than the freshness window, so an
    // in-flight flush's just-uploaded pack — referenced by the
    // about-to-be-PUT manifest — survives long enough to be linked.
    // The soak's reads are served from the dirty cache (the workload
    // never goes cold), so deleted-from-S3 packs aren't read back.
    const GC_BUDGET_BYTES: u64 = 128 * 1024 * 1024;
    const GC_STALE_MS: i64 = 5_000;
    let gc_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gc_stop_t = Arc::clone(&gc_stop);
    let s3_for_gc: Arc<InMemory> = Arc::clone(&s3);
    let gc_handle = tokio::spawn(async move {
        use futures::StreamExt;
        use object_store::ObjectMeta;
        while !gc_stop_t.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let mut entries: Vec<ObjectMeta> = s3_for_gc
                .list(None)
                .filter_map(|r| async move { r.ok() })
                .collect()
                .await;
            entries.sort_by_key(|e| e.last_modified);
            let total: u64 = entries.iter().map(|e| e.size).sum();
            if total <= GC_BUDGET_BYTES {
                continue;
            }
            let cutoff = chrono::Utc::now()
                - chrono::Duration::milliseconds(GC_STALE_MS);
            let mut to_free = total - GC_BUDGET_BYTES;
            for entry in &entries {
                if to_free == 0 {
                    break;
                }
                if entry.last_modified > cutoff {
                    // Too fresh — might be referenced by an unposted
                    // manifest. Stop; come back next tick.
                    break;
                }
                let _ = s3_for_gc.delete(&entry.location).await;
                to_free = to_free.saturating_sub(entry.size);
            }
        }
    });

    let mut ublk_server = UblkServer::new();
    let dev_path = ublk_server
        .add_device(EXPORT_NAME, handler)
        .await
        .expect("ublk register");
    eprintln!("[zc-soak] ublk device: {} (duration: {:?})", dev_path.display(), duration);

    let rss_start = read_rss_bytes();
    let fds_start = count_open_fds();

    let bdev = dev_path.clone();
    let io_result = tokio::task::spawn_blocking(move || soak_loop(&bdev, duration))
        .await
        .expect("spawn_blocking join");

    let rss_end = read_rss_bytes();
    let fds_end = count_open_fds();

    gc_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = gc_handle.await;

    if let Err(e) = ublk_server.shutdown().await {
        eprintln!("ublk shutdown error: {e}");
    }
    if let Err(e) = router.shutdown().await {
        eprintln!("router shutdown error: {e}");
    }

    let (cycles, total_bytes) = io_result.unwrap_or_else(|e| panic!("[zc-soak] failed: {e}"));
    let mb = total_bytes as f64 / (1024.0 * 1024.0);
    let mbps = mb / duration.as_secs_f64();
    let rss_growth = rss_end.saturating_sub(rss_start);
    eprintln!(
        "[zc-soak] OK: {cycles} cycles, {mb:.0} MB total ({mbps:.0} MB/s), \
         rss start={} MiB end={} MiB grew={} MiB, fds start={} end={}",
        rss_start / (1024 * 1024),
        rss_end / (1024 * 1024),
        rss_growth / (1024 * 1024),
        fds_start,
        fds_end,
    );

    // Leak guards. Generous because tikv-jemalloc doesn't always return
    // freed memory to the OS — what we want to catch is *unbounded*
    // growth, not steady-state allocator overhead.
    assert!(
        rss_growth < 500 * 1024 * 1024,
        "RSS grew by {} MiB during {:?} of soak — possible leak",
        rss_growth / (1024 * 1024),
        duration,
    );
    // FD growth: each device add takes ~30 FDs (cdev + data file + ublk
    // ring + ...). We only add one device so >2x growth means we're
    // leaking per-I/O FDs.
    assert!(
        fds_end <= fds_start.saturating_mul(2),
        "FD count grew from {} to {} during soak — possible leak",
        fds_start, fds_end,
    );
}

/// Multi-device density soak.
///
/// What the single-device `zc_glidefs_soak` can't reach:
///   * worker_pool's queue→worker scheduling under N-device contention
///     — each export's queues are mapped to a worker via QueueKey hash;
///     concurrent dispatch across N exports verifies that scheduling
///     stays fair and doesn't deadlock under load.
///   * cross-device rotation gate independence — each export has its
///     own `data_file` RwLock; a writer queued on one MUST NOT block
///     dispatch on another.
///   * the ZC inline fast path under genuine concurrent dispatch from
///     many queues (the single-device soak has 1 device × 4 queues).
///   * per-export memory accumulation at density — RSS / N tells us
///     the production-class memory budget per VM.
///
/// Defaults are tuned for CI runners (4 vCPU / 16 GB): N=4 devices,
/// 10 s. Override via:
///   * `GLIDEFS_MULTI_SOAK_DEVICES=N`  (production-scale N=32-64
///     in a host-class machine)
///   * `GLIDEFS_MULTI_SOAK_DURATION_S=N`  (manual / nightly long soak)
///
/// Acceptance is the same shape as the single-device soak: every
/// cycle's verify must succeed (byte-mismatch → panic), RSS bounded,
/// FDs bounded by a per-device budget.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_multi_device_soak() {
    if let Some(why) = skip_reason() {
        eprintln!("skip [zc-multi-soak]: {why}");
        return;
    }
    if std::env::var("GLIDEFS_TEST_FORCE_USER_COPY").is_ok_and(|v| !v.is_empty()) {
        // Same reason as the single-device soak: the cycle pacing is
        // tuned for ZC. USER_COPY can't drain fast enough on a small VM
        // and would wedge.
        eprintln!("skip [zc-multi-soak]: USER_COPY transport (soak is ZC-tuned)");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("GLIDEFS_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,glidefs=info")),
        )
        .try_init();

    let n_devices: usize = std::env::var("GLIDEFS_MULTI_SOAK_DEVICES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let duration = Duration::from_secs(
        std::env::var("GLIDEFS_MULTI_SOAK_DURATION_S")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(10),
    );

    eprintln!(
        "[zc-multi-soak] N={n_devices} devices, duration={duration:?}"
    );

    // Build a router with the same shape as single-soak (memory:// S3
    // mock + periodic GC reaper). Create N exports and register each as
    // a ublk device.
    let cache_dir = TempDir::new().unwrap();
    let s3: Arc<InMemory> = Arc::new(InMemory::new());
    let s3_dyn: Arc<dyn ObjectStore> = s3.clone();
    let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let router = Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: s3_dyn,
            db_path: "zc-multi-soak".to_string(),
            cache_dir: cache_dir.path().to_path_buf(),
            block_size: 4096,
            clean_cache,
            pack_index_cache: None,
            wal_sync: false,
            max_s3_uploads: 8,
            max_s3_downloads: 16,
            default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
            // Default `max_exports` (10) would cap us at N≤10; lift it
            // here for the density test. Production setting comes from
            // the router's own config; this only changes the test.
            max_exports: (n_devices + 4).max(64),
            manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
            profile: None,
        })
        .await
        .expect("router"),
    );

    let export_names: Vec<String> =
        (0..n_devices).map(|i| format!("multi-soak-{i:04}")).collect();
    for name in &export_names {
        let cfg = ExportConfig {
            name: name.clone(),
            size_gb: DEVICE_SIZE_GB,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
        };
        router
            .create_export(cfg, false, None, None)
            .await
            .expect("create_export");
    }

    // GC reaper — identical shape to the single-device soak's. Keeps
    // the InMemory S3 mock bounded under the N×rotation throughput we
    // generate, otherwise per-export pack accumulation in the mock
    // would dominate RSS (production GC happens out-of-band via the
    // `glidefs gc` CLI).
    const GC_BUDGET_BYTES: u64 = 128 * 1024 * 1024;
    const GC_STALE_MS: i64 = 5_000;
    let gc_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gc_stop_t = Arc::clone(&gc_stop);
    let s3_for_gc: Arc<InMemory> = Arc::clone(&s3);
    let gc_handle = tokio::spawn(async move {
        use futures::StreamExt;
        use object_store::ObjectMeta;
        while !gc_stop_t.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let mut entries: Vec<ObjectMeta> = s3_for_gc
                .list(None)
                .filter_map(|r| async move { r.ok() })
                .collect()
                .await;
            entries.sort_by_key(|e| e.last_modified);
            let total: u64 = entries.iter().map(|e| e.size).sum();
            if total <= GC_BUDGET_BYTES {
                continue;
            }
            let cutoff = chrono::Utc::now()
                - chrono::Duration::milliseconds(GC_STALE_MS);
            let mut to_free = total - GC_BUDGET_BYTES;
            for entry in &entries {
                if to_free == 0 {
                    break;
                }
                if entry.last_modified > cutoff {
                    break;
                }
                let _ = s3_for_gc.delete(&entry.location).await;
                to_free = to_free.saturating_sub(entry.size);
            }
        }
    });

    let mut ublk_server = UblkServer::new();
    let mut dev_paths = Vec::with_capacity(n_devices);
    for name in &export_names {
        let handler = router.get_handler(name).await.expect("handler");
        let dev = ublk_server
            .add_device(name, handler)
            .await
            .expect("ublk register");
        dev_paths.push(dev);
    }
    eprintln!(
        "[zc-multi-soak] registered {} devices: {} .. {}",
        dev_paths.len(),
        dev_paths.first().map(|p| p.display().to_string()).unwrap_or_default(),
        dev_paths.last().map(|p| p.display().to_string()).unwrap_or_default(),
    );

    let rss_start = read_rss_bytes();
    let fds_start = count_open_fds();

    // Run all soak_loops in parallel via spawn_blocking. Each driver
    // owns one device's worth of writers/verifier.
    let mut workers = Vec::with_capacity(n_devices);
    for dev in dev_paths {
        let dev = dev.clone();
        workers.push(tokio::task::spawn_blocking(move || soak_loop(&dev, duration)));
    }

    let mut total_cycles = 0u64;
    let mut total_bytes = 0u64;
    let mut errors = 0usize;
    for w in workers {
        match w.await {
            Ok(Ok((cycles, bytes))) => {
                total_cycles += cycles;
                total_bytes += bytes;
            }
            Ok(Err(e)) => {
                errors += 1;
                eprintln!("[zc-multi-soak] driver error: {e}");
            }
            Err(e) => {
                errors += 1;
                eprintln!("[zc-multi-soak] driver join failed: {e}");
            }
        }
    }

    let rss_end = read_rss_bytes();
    let fds_end = count_open_fds();

    gc_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = gc_handle.await;

    if let Err(e) = ublk_server.shutdown().await {
        eprintln!("ublk shutdown error: {e}");
    }
    if let Err(e) = router.shutdown().await {
        eprintln!("router shutdown error: {e}");
    }
    drop(cache_dir);

    let mb = total_bytes as f64 / (1024.0 * 1024.0);
    let mbps = mb / duration.as_secs_f64();
    let rss_growth = rss_end.saturating_sub(rss_start);
    eprintln!(
        "[zc-multi-soak] OK: {n_devices} devices, {total_cycles} cycle-runs, \
         {mb:.0} MB ({mbps:.0} MB/s aggregate), \
         rss start={} MiB end={} MiB grew={} MiB, fds start={fds_start} end={fds_end}",
        rss_start / (1024 * 1024),
        rss_end / (1024 * 1024),
        rss_growth / (1024 * 1024),
    );

    assert_eq!(errors, 0, "{errors} drivers failed — see logs above");

    // Per-device RSS bound. The 1h single-device soak grew 264 MiB
    // (allocator HWM + GC steady state); the per-device delta is
    // roughly constant. Allow 64 MiB per device + 256 MiB base —
    // catches unbounded growth without false-positiving on jemalloc
    // arenas at small N.
    let rss_budget_mib: u64 = 256 + 64 * n_devices as u64;
    assert!(
        rss_growth < rss_budget_mib * 1024 * 1024,
        "RSS grew by {} MiB across {n_devices} devices ({:?}) — budget {} MiB",
        rss_growth / (1024 * 1024),
        duration,
        rss_budget_mib,
    );

    // FD budget. Each ZC device opens ~30 FDs (cdev, ring, scratch
    // memfd, /dev/zero, eventfds). Allow start + 64×N — catches
    // per-IO FD leaks.
    let fd_budget = fds_start + 64 * n_devices;
    assert!(
        fds_end <= fd_budget,
        "FD count grew from {} to {} (budget {}) — possible per-IO leak",
        fds_start, fds_end, fd_budget,
    );
}

/// Soak inner loop. Returns (cycles_completed, total_bytes_io'd).
fn soak_loop(dev: &Path, duration: Duration) -> std::io::Result<(u64, u64)> {
    const CHUNK: usize = 64 * 1024;
    // Two parallel writers — enough to exercise the dispatch path's
    // concurrent gate acquisition, few enough that the QEMU VM (4
    // CPUs) doesn't drown in ublk+io_uring kernel worker scheduling
    // overhead. The bench config tested ≥10% IOPS uplift at QD=64; the
    // soak's value is in catching state-machine corruption, not in
    // maxing out throughput.
    const WRITERS: usize = 2;
    // Use a small slice of the device so cycles stay fast (the soak
    // value is in the *number* of cycles, not the bytes per cycle —
    // each cycle exercises the gate, flush, dispatch, and verify paths).
    const PER_CYCLE: usize = 32 * 1024 * 1024; // 32 MiB
    let chunk_count = PER_CYCLE / CHUNK;
    assert!(chunk_count % WRITERS == 0);

    let pattern = |generation: u64, off: u64| -> u8 {
        (off.wrapping_mul(2_654_435_761).wrapping_add(generation.wrapping_mul(1_000_003))) as u8
    };

    let start = Instant::now();
    let mut cycle = 0u64;
    let mut total_bytes = 0u64;
    while start.elapsed() < duration {
        // Parallel write phase.
        let chunks_per_writer = chunk_count / WRITERS;
        let dev_path = dev.to_path_buf();
        let mut handles = Vec::with_capacity(WRITERS);
        for w in 0..WRITERS {
            let dev_path = dev_path.clone();
            let cycle = cycle;
            handles.push(std::thread::spawn(move || -> std::io::Result<()> {
                use std::io::{Seek, SeekFrom, Write};
                let mut file = open_direct(&dev_path, true)?;
                let mut chunk_buf = aligned_4k(&vec![0u8; CHUNK]);
                for i in 0..chunks_per_writer {
                    let c = w + i * WRITERS;
                    let base = (c * CHUNK) as u64;
                    for j in 0..CHUNK {
                        chunk_buf.as_mut_slice()[j] = pattern(cycle, base + j as u64);
                    }
                    file.seek(SeekFrom::Start(base))?;
                    file.write_all(&chunk_buf)?;
                }
                file.sync_data()?;
                Ok(())
            }));
        }
        for h in handles {
            h.join().expect("writer panicked")?;
        }

        // Verify phase.
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = open_direct(dev, false)?;
            let mut readback = aligned_4k(&vec![0u8; CHUNK]);
            for c in 0..chunk_count {
                let base = (c * CHUNK) as u64;
                file.seek(SeekFrom::Start(base))?;
                file.read_exact(&mut readback)?;
                for j in 0..CHUNK {
                    let expected = pattern(cycle, base + j as u64);
                    if readback[j] != expected {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "soak cycle {cycle}: byte mismatch at offset {} (chunk {c}, in-chunk {j}): got {:#x}, want {:#x}",
                                base + j as u64,
                                readback[j],
                                expected,
                            ),
                        ));
                    }
                }
            }
        }

        total_bytes += 2 * PER_CYCLE as u64; // write + read
        cycle += 1;
    }
    Ok((cycle, total_bytes))
}

fn read_rss_bytes() -> u64 {
    let status = match std::fs::read_to_string("/proc/self/status") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // Format: "VmRSS:\t   12345 kB"
            for tok in rest.split_whitespace() {
                if let Ok(kb) = tok.parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    0
}

fn count_open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

/// Concurrent R+W on the same evicted block — the cold-read backfill race.
///
/// **ZC-specific:** the race is in the ZC cold-read path's pwrite to the
/// active cache file at the block's device offset (colliding with a
/// concurrent kernel `WRITE_FIXED` to the same offset). USER_COPY's
/// cold-read path returns the decompressed `Bytes` and the io_task
/// memcpies them into the kernel buffer via `pwrite(cdev_fd, ...)` —
/// no userspace pwrite to the active cache file, no race. We skip this
/// test under forced USER_COPY for two reasons: (1) the failure mode
/// it targets doesn't exist there, (2) the heavy R+W concurrency wedges
/// the per-IO syscall path on the 4-CPU CI VM.
///
/// Scenario:
/// 1. Pre-fill blocks with generation 0, flush + wait for eviction (state
///    → NOT_PRESENT, data only in S3).
/// 2. For each round, concurrently launch:
///    - A WRITER thread that writes generation `R` to a freshly-evicted
///      block.
///    - A READER thread that reads that same block.
/// 3. After both threads join, single-thread re-read and verify the
///    block holds generation `R`.
///
/// If the cold-read path's `pwrite_all_at(s3_data, block_start)` races
/// with the writer's `WRITE_FIXED` to the same offset:
/// - WRITER lands generation R via the kernel data plane, after_write
///   transitions state to DIRTY.
/// - READER (still in its async S3 fetch task) returns to the dispatch
///   path, pwrites the now-stale gen 0 bytes to the same active-file
///   offset, then submits READ_FIXED.
/// - State map says DIRTY (gen R, per writer's commit) but the disk has
///   gen 0 (clobbered by reader's backfill). Verify-re-read returns
///   gen 0. Silent write loss.
///
/// Each pre-filled block is used exactly once for a race round, so each
/// round starts from the "NOT_PRESENT + data in S3" precondition.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_concurrent_rw_race_on_evicted_block() {
    if let Some(why) = skip_reason() {
        eprintln!("skip [zc-rw-race]: {why}");
        return;
    }
    if std::env::var("GLIDEFS_TEST_FORCE_USER_COPY").is_ok_and(|v| !v.is_empty()) {
        eprintln!(
            "skip [zc-rw-race]: USER_COPY path doesn't have this race \
             (no userspace pwrite to active cache file on cold reads)"
        );
        return;
    }
    // Low flush threshold so the pre-fill writes evict quickly and
    // every block is NOT_PRESENT (data only in S3) by the time we
    // start the race rounds.
    let (router, _cache_dir) = setup_router_with_flush_threshold(8).await;
    let handler = router.get_handler(EXPORT_NAME).await.expect("handler");

    let mut ublk_server = UblkServer::new();
    let dev_path = ublk_server
        .add_device(EXPORT_NAME, handler)
        .await
        .expect("ublk register");
    eprintln!("[zc-rw-race] ublk device: {}", dev_path.display());

    let bdev = dev_path.clone();
    let io_result = tokio::task::spawn_blocking(move || rw_race_workload(&bdev))
        .await
        .expect("spawn_blocking join");

    if let Err(e) = ublk_server.shutdown().await {
        eprintln!("ublk shutdown error: {e}");
    }
    if let Err(e) = router.shutdown().await {
        eprintln!("router shutdown error: {e}");
    }

    io_result.unwrap_or_else(|e| panic!("[zc-rw-race] failed: {e}"));
    eprintln!("[zc-rw-race] OK");
}

fn rw_race_block_pattern(block_idx: u64, generation: u64) -> [u8; 4096] {
    let mut buf = [0u8; 4096];
    buf[..8].copy_from_slice(&generation.to_le_bytes());
    buf[8..16].copy_from_slice(&block_idx.to_le_bytes());
    let seed = block_idx
        .wrapping_mul(2_654_435_761)
        .wrapping_add(generation.wrapping_mul(1_000_003));
    for (i, b) in buf[16..].iter_mut().enumerate() {
        *b = seed.wrapping_add((i as u64).wrapping_mul(11)) as u8;
    }
    buf
}

fn rw_race_workload(dev: &Path) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};
    const ROUNDS: u64 = 256;

    // Phase 1: pre-fill blocks 0..ROUNDS with generation 0. Force flush
    // via sync_data; the low flush_threshold (8) makes nearly every
    // write trip a flush, so by the end of this phase every block is
    // SYNCING or NOT_PRESENT.
    {
        let mut file = open_direct(dev, true)?;
        for block_idx in 0..ROUNDS {
            let pat = rw_race_block_pattern(block_idx, 0);
            let aligned = aligned_4k(&pat);
            file.seek(SeekFrom::Start(block_idx * 4096))?;
            file.write_all(&aligned)?;
        }
        file.sync_data()?;
    }
    // Give the async flush worker time to finish evictions (state →
    // NOT_PRESENT, data only in S3).
    std::thread::sleep(Duration::from_millis(500));

    // Phase 2: race rounds. Each round uses a unique freshly-evicted
    // block. The writer writes generation `round`, the reader reads
    // the block concurrently. After both join, verify the block holds
    // generation `round`.
    for round in 1..ROUNDS {
        let block_idx = round;
        let off = block_idx * 4096;

        let dev_w = dev.to_path_buf();
        let writer = std::thread::spawn(move || -> std::io::Result<()> {
            let pat = rw_race_block_pattern(block_idx, round);
            let aligned = aligned_4k(&pat);
            let mut f = open_direct(&dev_w, true)?;
            f.seek(SeekFrom::Start(off))?;
            f.write_all(&aligned)?;
            f.sync_data()?;
            Ok(())
        });

        let dev_r = dev.to_path_buf();
        let reader = std::thread::spawn(move || -> std::io::Result<()> {
            let mut buf = aligned_4k(&[0u8; 4096]);
            let mut f = open_direct(&dev_r, false)?;
            f.seek(SeekFrom::Start(off))?;
            f.read_exact(&mut buf)?;
            Ok(())
        });

        writer.join().expect("writer panicked")?;
        reader.join().expect("reader panicked")?;

        // Verify: re-read and confirm the block holds `round`.
        let mut buf = aligned_4k(&[0u8; 4096]);
        let mut f = open_direct(dev, false)?;
        f.seek(SeekFrom::Start(off))?;
        f.read_exact(&mut buf)?;
        let observed_gen = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let observed_block = u64::from_le_bytes(buf[8..16].try_into().unwrap());

        if observed_block != block_idx {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "round {round}: block-idx mismatch — observed {observed_block}, want {block_idx} (torn read or block confusion)"
                ),
            ));
        }
        if observed_gen != round {
            let expected = rw_race_block_pattern(block_idx, round);
            let body_matches = expected[16..] == buf[16..];
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "round {round}: write lost — block {block_idx} reads back generation {observed_gen} (expected {round}). \
                     Body pattern matches expected: {body_matches}. \
                     This is the cold-read backfill clobber race."
                ),
            ));
        }
    }
    Ok(())
}

/// Rotation race: force frequent flushes during a heavy concurrent write
/// workload. If the inflight gate has any hole, the state map and the
/// actual data file diverge — blocks marked DIRTY-in-active but data
/// actually in flushing-file. We'd detect this as a read mismatch
/// because subsequent reads consult the state map to pick which file to
/// read from.
///
/// Workload: a single fio job writing a known byte-pattern across the
/// device at QD=64, runtime fixed. Flush threshold is set low enough
/// that many rotations happen during the test. After the writes
/// complete, we read everything back and verify byte-for-byte.
///
/// Probabilistic by nature — but with ~100s of rotations interleaved
/// with thousands of writes, any real race shows up in the comparison.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_rotation_race_under_load() {
    if let Some(why) = skip_reason() {
        eprintln!("skip [zc-rotation-race]: {why}");
        return;
    }

    // Flush threshold = 4 blocks: every ~16 KiB of dirty data triggers a
    // rotation. With 128 MiB device + heavy writes, we get hundreds of
    // rotations during the test.
    let (router, _cache_dir) = setup_router_with_flush_threshold(4).await;
    let handler = router.get_handler(EXPORT_NAME).await.expect("handler");

    let mut ublk_server = UblkServer::new();
    let dev_path = ublk_server
        .add_device(EXPORT_NAME, handler)
        .await
        .expect("ublk register");
    eprintln!("[zc-rotation-race] ublk device: {}", dev_path.display());

    let bdev = dev_path.clone();
    let io_result = tokio::task::spawn_blocking(move || {
        rotation_race_workload(&bdev, 64 * 1024 * 1024)
    })
    .await
    .expect("spawn_blocking join");

    if let Err(e) = ublk_server.shutdown().await {
        eprintln!("ublk shutdown error: {e}");
    }
    if let Err(e) = router.shutdown().await {
        eprintln!("router shutdown error: {e}");
    }

    io_result.unwrap_or_else(|e| panic!("[zc-rotation-race] failed: {e}"));
    eprintln!("[zc-rotation-race] OK");
}

/// Write `total` bytes of a deterministic pattern across the device with
/// multiple parallel writers (true concurrency through the dispatch
/// hot path), then read everything back and verify. Pattern: byte i =
/// `((offset+i).wrapping_mul(2654435761)) as u8` (Knuth multiplicative
/// hash — any block-misplaced copy shows up as a misaligned region).
fn rotation_race_workload(dev: &Path, total: usize) -> std::io::Result<()> {
    const CHUNK: usize = 64 * 1024; // 16 blocks at 4 KiB
    const WRITERS: usize = 8;
    assert!(total % CHUNK == 0);
    let chunk_count = total / CHUNK;
    assert!(chunk_count % WRITERS == 0);

    let device_pattern =
        |off: u64| -> u8 { (off.wrapping_mul(2_654_435_761)) as u8 };

    // Write phase — N parallel writers, each owning a disjoint chunk-
    // index modulus. This drives many concurrent dispatch tasks against
    // the ZC worker while the flush_threshold=4 rotation keeps firing.
    let dev_path = dev.to_path_buf();
    let chunks_per_writer = chunk_count / WRITERS;
    let mut handles = Vec::with_capacity(WRITERS);
    for w in 0..WRITERS {
        let dev_path = dev_path.clone();
        handles.push(std::thread::spawn(move || -> std::io::Result<()> {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = open_direct(&dev_path, true)?;
            let mut chunk_buf = aligned_4k(&vec![0u8; CHUNK]);
            for i in 0..chunks_per_writer {
                let c = w + i * WRITERS; // interleaved chunk indices
                let base = (c * CHUNK) as u64;
                for j in 0..CHUNK {
                    chunk_buf.as_mut_slice()[j] = device_pattern(base + j as u64);
                }
                file.seek(SeekFrom::Start(base))?;
                file.write_all(&chunk_buf)?;
            }
            file.sync_data()?;
            Ok(())
        }));
    }
    for h in handles {
        h.join().expect("writer panicked")?;
    }

    // Read phase — single thread, full sweep, byte-by-byte verify.
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = open_direct(dev, false)?;
        let mut readback = aligned_4k(&vec![0u8; CHUNK]);
        for c in 0..chunk_count {
            let base = (c * CHUNK) as u64;
            file.seek(SeekFrom::Start(base))?;
            file.read_exact(&mut readback)?;
            for i in 0..CHUNK {
                let expected = device_pattern(base + i as u64);
                if readback[i] != expected {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "rotation race: byte mismatch at offset {} (chunk {}, in-chunk {}): got {:#x}, want {:#x}",
                            base + i as u64,
                            c,
                            i,
                            readback[i],
                            expected
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Flush + rotation deadlock — found in the 10-min soak (April 2026).
///
/// Three-actor deadlock between:
///   1. ZC writes inflight, each holding `data_file.read_arc()` via the
///      keepalive in `run_zc_queue`'s `inflight[tag]` slot.
///   2. The flush scheduler queuing `data_file.write()` (task-fair) for
///      a rotation triggered by the dirty-block threshold.
///   3. A `UBLK_IO_OP_FLUSH` arriving while the writer is queued. The op
///      is dispatched INLINE on the io_uring loop thread, which calls
///      `handler.flush()` → `cache.flush()` → `data_file.read()` →
///      blocks behind the queued writer (task-fair).
///
/// The loop thread can no longer process `WRITE_FIXED` CQEs, so inflight
/// reads never drop, so the writer never proceeds, so the FLUSH stays
/// blocked. Deadlock.
///
/// Reproducer: tiny `flush_threshold` (rotations constant), many
/// concurrent writers interleaving `write_all` with `sync_data` (the
/// fsync that becomes a `UBLK_IO_OP_FLUSH`). 30 s watchdog: if the work
/// doesn't finish, we declare deadlock.
#[tokio::test(flavor = "multi_thread")]
async fn zc_glidefs_flush_rotation_deadlock() {
    if let Some(why) = skip_reason() {
        eprintln!("skip [zc-flush-deadlock]: {why}");
        return;
    }

    // flush_threshold=2: every couple of dirty blocks triggers a
    // rotation. Combined with continuous writes, the writer side of the
    // rotation gate is queued nearly all the time — which is the
    // necessary precondition for the FLUSH op to block in
    // `data_file.read()`.
    let (router, _cache_dir) = setup_router_with_flush_threshold(2).await;
    let handler = router.get_handler(EXPORT_NAME).await.expect("handler");

    let mut ublk_server = UblkServer::new();
    let dev_path = ublk_server
        .add_device(EXPORT_NAME, handler)
        .await
        .expect("ublk register");
    eprintln!("[zc-flush-deadlock] ublk device: {}", dev_path.display());

    let bdev = dev_path.clone();
    // Run the workload on an OS thread; cap the wait with a channel
    // recv_timeout so the deadlock surfaces as a clear timeout rather
    // than wedging the test runner forever.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("zc-flush-deadlock-driver".into())
        .spawn(move || {
            let _ = tx.send(flush_rotation_deadlock_workload(&bdev));
        })
        .expect("spawn driver");

    let outcome = rx.recv_timeout(Duration::from_secs(30));

    // On deadlock, do NOT attempt graceful shutdown — the shutdown path
    // (which also goes through the cache + flush scheduler) will block
    // on the same lock as the io_uring loop, and the test would wedge
    // forever waiting on its own cleanup. Panic immediately; process
    // exit will tear everything down.
    match outcome {
        Ok(Ok(())) => {
            if let Err(e) = ublk_server.shutdown().await {
                eprintln!("ublk shutdown error: {e}");
            }
            if let Err(e) = router.shutdown().await {
                eprintln!("router shutdown error: {e}");
            }
            eprintln!("[zc-flush-deadlock] OK");
        }
        Ok(Err(e)) => panic!("[zc-flush-deadlock] workload error: {e}"),
        Err(_) => panic!(
            "[zc-flush-deadlock] DEADLOCK: write+sync workload didn't \
             complete within 30s. Loop-thread FLUSH dispatch is \
             blocked behind a queued rotation writer."
        ),
    }
}

fn flush_rotation_deadlock_workload(dev: &Path) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    const WRITERS: usize = 8;
    const ITERS_PER_WRITER: usize = 64;
    // Spread writes across a small window so blocks get touched, mixed,
    // and re-dirtied — keeps the flush scheduler firing rotations.
    const HOT_WINDOW_BLOCKS: u64 = 256;

    let dev_path = dev.to_path_buf();
    let mut handles = Vec::with_capacity(WRITERS);
    for w in 0..WRITERS {
        let dev_path = dev_path.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("zc-flush-deadlock-w{w}"))
                .spawn(move || -> std::io::Result<()> {
                    let mut file = open_direct(&dev_path, true)?;
                    let buf = aligned_4k(&vec![(w as u8).wrapping_add(1); 4096]);
                    for i in 0..ITERS_PER_WRITER {
                        let block = ((w * ITERS_PER_WRITER + i) as u64) % HOT_WINDOW_BLOCKS;
                        let offset = block * 4096;
                        file.seek(SeekFrom::Start(offset))?;
                        file.write_all(&buf)?;
                        // The fsync — `UBLK_IO_OP_FLUSH` arrives at the
                        // userspace handler. The deadlock lives here:
                        // if a rotation writer is queued and the io_uring
                        // loop services FLUSH inline, this never returns.
                        file.sync_data()?;
                    }
                    Ok(())
                })
                .expect("writer spawn"),
        );
    }
    for h in handles {
        h.join().expect("writer panicked")?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

async fn run_scenario<F>(name: &str, work: F)
where
    F: FnOnce(&Path) -> std::io::Result<()> + Send + 'static,
{
    if let Some(why) = skip_reason() {
        eprintln!("skip [{name}]: {why}");
        return;
    }

    let (router, _cache_dir) = setup_router().await;
    let handler = router.get_handler(EXPORT_NAME).await.expect("handler");

    let mut ublk_server = UblkServer::new();
    // Test-only override: GLIDEFS_TEST_FORCE_USER_COPY=1 exercises the
    // USER_COPY fallback path on a kernel that would otherwise pick ZC.
    // Lets us verify the legacy transport still works after the ZC rewrite
    // without needing a second VM at a non-ZC kernel.
    if std::env::var("GLIDEFS_TEST_FORCE_USER_COPY").is_ok_and(|v| !v.is_empty()) {
        ublk_server.force_user_copy_transport();
        eprintln!("[{name}] forcing USER_COPY transport (test-only)");
    }
    let dev_path = ublk_server
        .add_device(EXPORT_NAME, handler)
        .await
        .expect("ublk register");
    eprintln!("[{name}] ublk device: {}", dev_path.display());

    let bdev = dev_path.clone();
    let io_result = tokio::task::spawn_blocking(move || work(&bdev))
        .await
        .expect("spawn_blocking join");

    if let Err(e) = ublk_server.shutdown().await {
        eprintln!("ublk shutdown error: {e}");
    }
    if let Err(e) = router.shutdown().await {
        eprintln!("router shutdown error: {e}");
    }

    io_result.unwrap_or_else(|e| panic!("[{name}] failed: {e}"));
    eprintln!("[{name}] OK");
}

fn open_direct(dev: &Path, write: bool) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).custom_flags(libc::O_DIRECT);
    if write {
        opts.write(true);
    }
    opts.open(dev)
}

/// Increasing-byte pattern: byte i = ((offset + i) & 0xff). Lets us verify
/// content + alignment even when writes straddle block boundaries.
fn write_pattern(dev: &Path, offset: u64, len: usize) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let pattern: Vec<u8> = (0..len)
        .map(|i| ((offset as usize + i) & 0xff) as u8)
        .collect();
    let aligned = aligned_4k(&pattern);
    let mut file = open_direct(dev, true)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&aligned)?;
    file.sync_data()?;
    Ok(())
}

fn verify_pattern(dev: &Path, offset: u64, len: usize) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let pattern: Vec<u8> = (0..len)
        .map(|i| ((offset as usize + i) & 0xff) as u8)
        .collect();
    let mut readback = aligned_4k(&vec![0u8; len]);
    let mut file = open_direct(dev, false)?;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut readback)?;
    if &readback[..len] != pattern.as_slice() {
        // Find first mismatch.
        let bad = readback[..len].iter().zip(pattern.iter()).position(|(a, b)| a != b).unwrap();
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "data mismatch at byte {bad}: got {:#x}, want {:#x} (offset {}, len {})",
                readback[bad], pattern[bad], offset, len
            ),
        ));
    }
    Ok(())
}

/// 4 KiB-aligned heap buffer for O_DIRECT.
struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
}
impl AlignedBuf {
    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, 4096).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        Self { ptr, len }
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}
impl std::ops::Deref for AlignedBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}
impl std::ops::DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len, 4096).unwrap();
        unsafe { std::alloc::dealloc(self.ptr, layout) };
    }
}

fn aligned_4k(src: &[u8]) -> AlignedBuf {
    let mut buf = AlignedBuf::new(src.len());
    buf.as_mut_slice()[..src.len()].copy_from_slice(src);
    buf
}
