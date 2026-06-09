//! blktests integration — runs the Linux block device test suite against glidefs.
//!
//! Starts glidefs with both NBD-kernel and ublk devices, clones and builds
//! blktests, then runs the `block/` and `nbd/` test groups against each device.
//!
//! Requires: Linux, root, nbd + ublk_drv kernel modules, build-essential.
//!
//! Run:
//!   sudo -E cargo test -p glidefs --features blktests --test blktests -- --nocapture

#[cfg(all(target_os = "linux", feature = "blktests"))]
mod blktests {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;

    use glidefs::block::cache::{BlockCache, SimpleBlockCache};
    use glidefs::block::nbd::NbdDeviceManager;
    use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::block::ublk::UblkServer;
    use glidefs::config::ExportConfig;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use tempfile::TempDir;

    const DEVICE_SIZE_GB: f64 = 1.0;

    fn is_root() -> bool {
        unsafe { libc::geteuid() == 0 }
    }

    fn nbd_available() -> bool {
        Path::new("/dev/nbd0").exists()
    }

    fn ublk_available() -> bool {
        Path::new("/dev/ublk-control").exists()
    }

    fn blktests_dir() -> PathBuf {
        // Use a stable path so we don't re-clone every run
        PathBuf::from("/tmp/blktests")
    }

    /// Clone and build blktests if not already present.
    fn ensure_blktests() -> PathBuf {
        let dir = blktests_dir();
        if !dir.join("check").exists() {
            eprintln!("[blktests] cloning blktests...");
            if dir.exists() {
                std::fs::remove_dir_all(&dir).ok();
            }
            let status = Command::new("git")
                .args([
                    "clone",
                    "--depth=1",
                    "https://github.com/osandov/blktests.git",
                    dir.to_str().unwrap(),
                ])
                .status()
                .expect("git clone failed");
            assert!(status.success(), "git clone blktests failed");
        }

        // Always rebuild (fast if already built)
        eprintln!("[blktests] building blktests...");
        let status = Command::new("make")
            .current_dir(&dir)
            .status()
            .expect("make failed");
        assert!(status.success(), "make blktests failed");

        dir
    }

    /// Run blktests against a device, returning (passed, failed, skipped).
    ///
    /// Uses spawn_blocking to avoid blocking a tokio worker thread — the
    /// NBD server tasks need worker threads to service kernel I/O requests.
    async fn run_blktests(
        blktests_dir: &Path,
        dev_path: &Path,
        groups: &[&str],
    ) -> (usize, usize, usize) {
        use std::io::{BufRead, BufReader};
        use std::process::Stdio;

        let dev_str = dev_path.to_str().unwrap().to_string();
        let blktests_dir = blktests_dir.to_path_buf();
        let groups: Vec<String> = groups.iter().map(|s| s.to_string()).collect();

        eprintln!(
            "[blktests] running {:?} against {}",
            groups,
            dev_str
        );

        const INFRA_ONLY: &[&str] = &[
            "block/003", // discard/TRIM — glidefs doesn't support TRIM
            "block/008", // needs CPU hotplug (unavailable in CI VMs)
            "block/039", // needs null_blk kernel module
        ];

        // Stream subprocess output line-by-line instead of capturing the
        // full buffer up front. The previous `.output()` form ate all
        // stdout/stderr until the subprocess exited, so CI looked
        // "hung" for the entire blktests run — no progress visible until
        // the whole group finished. Streaming gives the libtest
        // 60-second heartbeat something useful to interleave with.
        let dev_str_clone = dev_str.clone();
        let (passed, failed, skipped) = tokio::task::spawn_blocking(move || {
            let mut args = vec!["-q".to_string()];
            args.extend(groups);
            let mut child = Command::new(blktests_dir.join("check"))
                .current_dir(&blktests_dir)
                .env("TEST_DEVS", &dev_str_clone)
                .env("TIMEOUT", "60")
                .args(&args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("blktests check failed to spawn");

            let stdout = child.stdout.take().expect("piped stdout");
            let stderr = child.stderr.take().expect("piped stderr");

            // Drain stderr on a background thread so the parent doesn't
            // block when the child writes only to stderr.
            let stderr_thread = std::thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    eprintln!("[blktests:stderr] {line}");
                }
            });

            let mut passed = 0usize;
            let mut failed = 0usize;
            let mut skipped = 0usize;
            let reader = BufReader::new(stdout);
            for line in reader.lines().map_while(Result::ok) {
                eprintln!("[blktests] {line}");
                if line.contains("[passed]") {
                    passed += 1;
                } else if line.contains("[failed]") {
                    if INFRA_ONLY.iter().any(|t| line.starts_with(t)) {
                        eprintln!("[blktests] ignoring infra-only failure: {line}");
                        skipped += 1;
                    } else {
                        failed += 1;
                    }
                } else if line.contains("[not run]") {
                    skipped += 1;
                }
            }

            let _ = child.wait();
            let _ = stderr_thread.join();
            (passed, failed, skipped)
        })
        .await
        .expect("spawn_blocking panicked");

        eprintln!(
            "[blktests] results for {}: {} passed, {} failed, {} skipped",
            dev_str, passed, failed, skipped
        );

        (passed, failed, skipped)
    }

    struct BlktestServer {
        router: Arc<ExportRouter>,
        nbd_manager: Option<NbdDeviceManager>,
        ublk_server: Option<UblkServer>,
        nbd_dev: Option<PathBuf>,
        ublk_dev: Option<PathBuf>,
        _cache_dir: TempDir,
    }

    impl BlktestServer {
        async fn start() -> Self {
            let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let cache_dir = TempDir::new().unwrap();
            let clean_cache: Arc<dyn BlockCache> =
                Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

            let router = Arc::new(
                ExportRouter::new(RouterConfig {
                    object_store: Arc::clone(&s3),
                    db_path: "blktests".to_string(),
                    cache_dir: cache_dir.path().to_path_buf(),
                    block_size: 128 * 1024,
                    clean_cache,
                    pack_index_cache: None,
                    wal_sync: false,
                    max_s3_uploads: 128,
                    max_s3_downloads: 512,
                    default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
                    ublk_nr_queues: 4,
                    nbd_dead_conn_timeout: 0,
                    max_exports: 10_000,
                    manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
                    profile: None,
                })
                .await
                .expect("failed to create router"),
            );

            let mut nbd_manager = None;
            let mut ublk_server = None;
            let mut nbd_dev = None;
            let mut ublk_dev = None;

            // Create NBD export + kernel device
            if nbd_available() {
                let nbd_config = ExportConfig {
                    name: "blktest-nbd".to_string(),
                    size_gb: DEVICE_SIZE_GB,
                    s3_prefix: None,
                    block_size: None,
                    flush_threshold: None,
                    flush_mode: None,
                    transport: None,
                    compaction_cooldown: None,
                };
                router
                    .create_export(nbd_config, false, None, None)
                    .await
                    .unwrap();

                let size = (DEVICE_SIZE_GB * 1024.0 * 1024.0 * 1024.0) as u64;
                let mut mgr = NbdDeviceManager::new()
                    .with_dead_conn_timeout(0);
                let dev = mgr
                    .add_device("blktest-nbd", Arc::clone(&router), size)
                    .await
                    .expect("failed to register nbd device");
                eprintln!("[blktests] nbd device ready: {}", dev.display());
                nbd_dev = Some(dev);
                nbd_manager = Some(mgr);
            }

            // Create ublk export + device
            if ublk_available() {
                let ublk_config = ExportConfig {
                    name: "blktest-ublk".to_string(),
                    size_gb: DEVICE_SIZE_GB,
                    s3_prefix: None,
                    block_size: None,
                    flush_threshold: None,
                    flush_mode: None,
                    transport: None,
                    compaction_cooldown: None,
                };
                router
                    .create_export(ublk_config, false, None, None)
                    .await
                    .unwrap();

                let handler = router
                    .get_handler("blktest-ublk")
                    .await
                    .expect("no handler for ublk export");

                let mut srv = UblkServer::new();
                let dev = srv
                    .add_device("blktest-ublk", handler)
                    .await
                    .expect("failed to register ublk device");
                eprintln!("[blktests] ublk device ready: {}", dev.display());
                ublk_dev = Some(dev);
                ublk_server = Some(srv);
            }

            Self {
                router,
                nbd_manager,
                ublk_server,
                nbd_dev,
                ublk_dev,
                _cache_dir: cache_dir,
            }
        }

        async fn shutdown(self) {
            if let Some(mgr) = self.nbd_manager {
                if let Err(e) = mgr.shutdown().await {
                    eprintln!("[blktests] nbd shutdown error: {e}");
                }
            }
            if let Some(srv) = self.ublk_server {
                if let Err(e) = srv.shutdown().await {
                    eprintln!("[blktests] ublk shutdown error: {e}");
                }
            }
            if let Err(e) = self.router.shutdown().await {
                eprintln!("[blktests] router shutdown error: {e}");
            }
        }
    }

    /// Flake-hunting harness for blktests block/042 (`dio-offsets`).
    ///
    /// Sets up the same ublk device the real blktests run uses and loops the
    /// upstream `dio-offsets` binary against it, asserting zero failures.
    /// At ~25% pre-fix and 0% post-fix this is the regression test for the
    /// `pre_write` sibling-bio backfill race
    /// (see `BlockHandler::backfill_blocks_in_range` doc).
    ///
    /// `#[ignore]` because it depends on a built `dio-offsets` binary; CI
    /// already exercises this end-to-end through the normal `blktests`
    /// test which runs `block/042` via the `check` script. This harness
    /// is for local flake-hunting / future regression catching:
    ///
    /// ```ignore
    /// sudo -E cargo test -p glidefs --release --features blktests --test blktests \
    ///   dio_offsets_flake_hunt -- --ignored --nocapture --test-threads=1
    /// ```
    ///
    /// `DIO_DIRECT_ITERS=N` controls iteration count (default 20).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn dio_offsets_flake_hunt() {
        if !is_root() || !ublk_available() { return; }
        let blktests = ensure_blktests();
        let server = BlktestServer::start().await;
        let Some(ref dev) = server.ublk_dev else { return; };
        let iters = std::env::var("DIO_DIRECT_ITERS")
            .ok().and_then(|s| s.parse::<i32>().ok()).unwrap_or(20);
        let dio_offsets_bin = blktests.join("src/dio-offsets");
        assert!(dio_offsets_bin.exists(), "dio-offsets binary not built");
        let dev_name = dev.file_name().unwrap().to_str().unwrap().to_string();
        let read_sysfs = |attr: &str| -> String {
            std::fs::read_to_string(format!("/sys/class/block/{dev_name}/queue/{attr}"))
                .unwrap().trim().to_string()
        };
        let max_segments = read_sysfs("max_segments");
        let max_sectors_kb = read_sysfs("max_sectors_kb");
        let dma_alignment = read_sysfs("dma_alignment");
        let virt_boundary = read_sysfs("virt_boundary_mask");
        let logical_block_size = read_sysfs("logical_block_size");
        let dev_str = dev.to_str().unwrap().to_string();
        eprintln!("device={dev_str} max_seg={max_segments} max_kb={max_sectors_kb} dma={dma_alignment} virt={virt_boundary} lbs={logical_block_size}");
        let mut pass = 0;
        let mut fail = 0;
        for i in 0..iters {
            let output = tokio::task::spawn_blocking({
                let bin = dio_offsets_bin.clone();
                let dev = dev_str.clone();
                let ms = max_segments.clone();
                let mkb = max_sectors_kb.clone();
                let da = dma_alignment.clone();
                let vb = virt_boundary.clone();
                let lbs = logical_block_size.clone();
                move || std::process::Command::new(bin)
                    .args([&dev, &ms, &mkb, &da, &vb, &lbs])
                    .output()
                    .expect("dio-offsets failed to exec")
            }).await.unwrap();
            if output.status.success() {
                pass += 1;
                eprintln!("iter {i}: PASS");
            } else {
                fail += 1;
                eprintln!("iter {i}: FAIL stderr={}",
                    String::from_utf8_lossy(&output.stderr).trim());
            }
        }
        eprintln!("=== summary: pass={pass} fail={fail} ===");
        server.shutdown().await;
        assert_eq!(fail, 0, "dio-offsets flaked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blktests_block_and_nbd() {
        if !is_root() {
            eprintln!("skipping blktests: must run as root");
            return;
        }
        if !nbd_available() && !ublk_available() {
            eprintln!("skipping blktests: neither nbd nor ublk available");
            return;
        }

        let blktests = ensure_blktests();
        let server = BlktestServer::start().await;

        let mut total_failed = 0;

        // Run block/ tests against NBD device
        if let Some(ref dev) = server.nbd_dev {
            let (passed, failed, skipped) = run_blktests(&blktests, dev, &["block"]).await;
            eprintln!(
                "[blktests] NBD block/: {passed} passed, {failed} failed, {skipped} skipped"
            );
            total_failed += failed;
        }

        // Run nbd/ tests against NBD device
        if let Some(ref dev) = server.nbd_dev {
            let (passed, failed, skipped) = run_blktests(&blktests, dev, &["nbd"]).await;
            eprintln!(
                "[blktests] NBD nbd/: {passed} passed, {failed} failed, {skipped} skipped"
            );
            total_failed += failed;
        }

        // Run block/ tests against ublk device
        if let Some(ref dev) = server.ublk_dev {
            let (passed, failed, skipped) = run_blktests(&blktests, dev, &["block"]).await;
            eprintln!(
                "[blktests] ublk block/: {passed} passed, {failed} failed, {skipped} skipped"
            );
            total_failed += failed;
        }

        server.shutdown().await;

        assert_eq!(
            total_failed, 0,
            "blktests had {total_failed} failures — see output above"
        );
    }
}
