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
    use glidefs::block::pack::DEFAULT_BLOCKS_PER_PACK;
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
    fn run_blktests(
        blktests_dir: &Path,
        dev_path: &Path,
        groups: &[&str],
    ) -> (usize, usize, usize) {
        let dev_str = dev_path.to_str().unwrap();
        eprintln!(
            "[blktests] running {:?} against {}",
            groups,
            dev_str
        );

        let mut args = vec![];
        args.extend_from_slice(groups);

        let output = Command::new(blktests_dir.join("check"))
            .current_dir(blktests_dir)
            .env("TEST_DEVS", dev_str)
            .args(&args)
            .output()
            .expect("blktests check failed to execute");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        eprintln!("[blktests] stdout:\n{stdout}");
        if !stderr.is_empty() {
            eprintln!("[blktests] stderr:\n{stderr}");
        }

        // Count results from output lines like:
        //   block/001 (stress test) [passed]
        //   block/002 (something) [failed]
        //   block/003 (something) [not run]
        let mut passed = 0;
        let mut failed = 0;
        let mut skipped = 0;
        for line in stdout.lines() {
            if line.contains("[passed]") {
                passed += 1;
            } else if line.contains("[failed]") {
                failed += 1;
            } else if line.contains("[not run]") {
                skipped += 1;
            }
        }

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
                    wal_sync: false,
                    max_s3_uploads: 128,
                    max_s3_downloads: 512,
                    default_blocks_per_pack: DEFAULT_BLOCKS_PER_PACK,
                    ublk_nr_queues: 4,
                    nbd_dead_conn_timeout: 0,
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
                    blocks_per_pack: None,
                    flush_mode: None,
                    transport: None,
                };
                router
                    .create_export(nbd_config, false, None, None)
                    .await
                    .unwrap();

                let size = (DEVICE_SIZE_GB * 1024.0 * 1024.0 * 1024.0) as u64;
                let mut mgr = NbdDeviceManager::new();
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
                    blocks_per_pack: None,
                    flush_mode: None,
                    transport: None,
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
            let (passed, failed, skipped) = run_blktests(&blktests, dev, &["block"]);
            eprintln!(
                "[blktests] NBD block/: {passed} passed, {failed} failed, {skipped} skipped"
            );
            total_failed += failed;
        }

        // Run nbd/ tests against NBD device
        if let Some(ref dev) = server.nbd_dev {
            let (passed, failed, skipped) = run_blktests(&blktests, dev, &["nbd"]);
            eprintln!(
                "[blktests] NBD nbd/: {passed} passed, {failed} failed, {skipped} skipped"
            );
            total_failed += failed;
        }

        // Run block/ tests against ublk device
        if let Some(ref dev) = server.ublk_dev {
            let (passed, failed, skipped) = run_blktests(&blktests, dev, &["block"]);
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
