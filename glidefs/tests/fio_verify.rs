//! fio data integrity verification through kernel block devices.
//!
//! Unlike `fio_bench.rs` (throughput benchmarks), these tests use fio's
//! `--verify=crc32c` mode to detect data corruption. Each test writes
//! verifiable data, then reads it back and checks CRC32C checksums.
//!
//! Requires: Linux, kernel block device (ublk or nbd-kernel), fio installed.
//!
//! Run (ublk):
//!   sudo -E cargo test -p glidefs --features fio-bench --test fio_verify -- --nocapture
//!
//! These are also run in CI via the `fio-verify` job in `.github/workflows/rust.yml`.

#[cfg(all(target_os = "linux", feature = "fio-bench"))]
mod fio_verify {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;

    use glidefs::block::cache::{BlockCache, SimpleBlockCache};
    use glidefs::block::pack::DEFAULT_BLOCKS_PER_PACK;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::block::ublk::UblkServer;
    use glidefs::config::ExportConfig;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use tempfile::TempDir;

    const DEVICE_SIZE_GB: f64 = 1.0;

    fn ublk_available() -> bool {
        Path::new("/dev/ublk-control").exists()
    }

    fn fio_available() -> bool {
        Command::new("fio").arg("--version").output().is_ok()
    }

    fn skip_unless_ready() -> bool {
        if !ublk_available() {
            eprintln!("skipping fio verify: ublk_drv not available");
            return true;
        }
        if !fio_available() {
            eprintln!("skipping fio verify: fio not installed");
            return true;
        }
        false
    }

    struct VerifyServer {
        ublk_server: UblkServer,
        router: Arc<ExportRouter>,
        object_store: Arc<dyn ObjectStore>,
        dev_path: PathBuf,
        cache_dir: TempDir,
    }

    impl VerifyServer {
        async fn start(s3: Arc<dyn ObjectStore>) -> Self {
            let cache_dir = TempDir::new().unwrap();
            let clean_cache: Arc<dyn BlockCache> =
                Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

            let router = Arc::new(
                ExportRouter::new(RouterConfig {
                    object_store: Arc::clone(&s3),
                    db_path: "verify".to_string(),
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

            let config = ExportConfig {
                name: "verify".to_string(),
                size_gb: DEVICE_SIZE_GB,
                s3_prefix: None,
                block_size: None,
                blocks_per_pack: None,
                flush_mode: None,
                transport: None,
            };
            router
                .create_export(config, false, None, None)
                .await
                .unwrap();

            let handler = router
                .get_handler("verify")
                .await
                .expect("no handler for verify export");

            let mut ublk_server = UblkServer::new();
            let dev_path = ublk_server
                .add_device("verify", handler)
                .await
                .expect("failed to register ublk device");

            eprintln!("ublk device ready: {}", dev_path.display());

            Self {
                ublk_server,
                router,
                object_store: s3,
                dev_path,
                cache_dir,
            }
        }

        async fn shutdown(self) {
            if let Err(e) = self.ublk_server.shutdown().await {
                eprintln!("ublk shutdown error: {e}");
            }
            if let Err(e) = self.router.shutdown().await {
                eprintln!("router shutdown error: {e}");
            }
        }
    }

    /// Run fio write phase: stamps data with CRC32C headers but does not verify.
    fn fio_write(name: &str, rw: &str, dev: &Path, extra: &[&str]) {
        let dev_str = dev.to_str().expect("non-utf8 dev path");
        let mut args = vec![
            "--name", name,
            "--filename", dev_str,
            "--rw", rw,
            "--ioengine=io_uring",
            "--bs=4k",
            "--iodepth=32",
            "--numjobs=1",
            "--direct=1",
            "--verify=crc32c",
            "--do_verify=0",
            "--output-format=normal",
            "--group_reporting",
        ];
        args.extend_from_slice(extra);

        eprintln!("[fio-verify] write phase: {name} ({rw})...");
        let output = Command::new("fio").args(&args).output().expect("fio failed");
        assert!(
            output.status.success(),
            "fio write failed (exit {}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// Run fio verify phase: reads back and checks CRC32C checksums.
    fn fio_verify_read(name: &str, rw: &str, dev: &Path, extra: &[&str]) {
        let dev_str = dev.to_str().expect("non-utf8 dev path");
        let mut args = vec![
            "--name", name,
            "--filename", dev_str,
            "--rw", rw,
            "--ioengine=io_uring",
            "--bs=4k",
            "--iodepth=32",
            "--numjobs=1",
            "--direct=1",
            "--verify=crc32c",
            "--do_verify=1",
            "--verify_fatal=1",
            "--output-format=normal",
            "--group_reporting",
        ];
        args.extend_from_slice(extra);

        eprintln!("[fio-verify] verify phase: {name} ({rw})...");
        let output = Command::new("fio").args(&args).output().expect("fio failed");
        assert!(
            output.status.success(),
            "fio verify FAILED (exit {}): {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        );
        eprintln!("[fio-verify] {name}: CRC32C verification passed");
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// Sequential write then sequential read with CRC32C verification.
    /// Catches: block offset calculation errors, sequential I/O path bugs.
    #[tokio::test]
    async fn fio_verify_sequential() {
        if skip_unless_ready() { return; }

        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let server = VerifyServer::start(s3).await;
        let dev = &server.dev_path;

        // Write phase: sequential write across device
        fio_write("seq-write", "write", dev, &["--size=256m"]);

        // Verify phase: sequential read back
        fio_verify_read("seq-verify", "read", dev, &["--size=256m"]);

        server.shutdown().await;
    }

    /// Random write then random read with CRC32C verification.
    /// Catches: cache coherence bugs under random access patterns.
    #[tokio::test]
    async fn fio_verify_random() {
        if skip_unless_ready() { return; }

        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let server = VerifyServer::start(s3).await;
        let dev = &server.dev_path;

        // Write phase: random writes for 30s
        fio_write("rand-write", "randwrite", dev, &[
            "--runtime=30", "--time_based", "--size=512m",
        ]);

        // Verify phase: random read back
        fio_verify_read("rand-verify", "randread", dev, &[
            "--runtime=30", "--time_based", "--size=512m",
        ]);

        server.shutdown().await;
    }

    /// Mixed random read/write with inline CRC32C verification.
    /// Catches: read-write interleaving bugs, torn reads under concurrent writes.
    #[tokio::test]
    async fn fio_verify_mixed() {
        if skip_unless_ready() { return; }

        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let server = VerifyServer::start(s3).await;
        let dev = &server.dev_path;

        // Mixed workload with inline verify — fio writes with headers and
        // verifies on reads within the same job.
        let dev_str = dev.to_str().unwrap();
        let output = Command::new("fio")
            .args([
                "--name=mixed-verify",
                "--filename", dev_str,
                "--rw=randrw",
                "--rwmixwrite=50",
                "--ioengine=io_uring",
                "--bs=4k",
                "--iodepth=32",
                "--numjobs=1",
                "--direct=1",
                "--verify=crc32c",
                "--verify_fatal=1",
                "--runtime=30",
                "--time_based",
                "--size=512m",
                "--output-format=normal",
                "--group_reporting",
            ])
            .output()
            .expect("fio failed");

        assert!(
            output.status.success(),
            "fio mixed verify FAILED (exit {}): {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        );
        eprintln!("[fio-verify] mixed: CRC32C verification passed");

        server.shutdown().await;
    }

    /// Write with fio, drain to S3, cold restart from manifest, verify with fio.
    /// Catches: data loss through the full persistence path (write → SSD → pack → S3 → cold-wake → read).
    ///
    /// Must use multi_thread: the blocking `fio` command occupies a thread while
    /// ublk I/O handlers need the tokio runtime to serve cold reads from S3.
    /// With current_thread, fio blocks the only worker and ublk I/O deadlocks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fio_verify_after_cold_wake() {
        if skip_unless_ready() { return; }

        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Phase 1: write verifiable data
        let server = VerifyServer::start(Arc::clone(&s3)).await;
        let dev = &server.dev_path;

        fio_write("cold-write", "write", dev, &["--size=128m"]);

        // Drain to S3
        eprintln!("[fio-verify] draining to S3...");
        server.router.drain_export("verify").await.unwrap();

        // Diagnostic: verify manifest and handler reads before shutdown
        {
            let handler = server.router.get_handler("verify").await
                .expect("handler should exist after drain");
            // Spot-check blocks at boundaries: first, middle, last
            for block in [0u64, 512, 960, 1023] {
                let offset = block * 128 * 1024;
                let data = handler.read(offset, 4096).await
                    .unwrap_or_else(|e| panic!("[diag] handler read at block {block} (offset {offset}) failed: {e:?}"));
                let nonzero = data.iter().any(|&b| b != 0);
                eprintln!("[diag] block {block} (offset {offset}): len={}, nonzero={nonzero}, first4=[{:#04x},{:#04x},{:#04x},{:#04x}]",
                    data.len(), data[0], data[1], data[2], data[3]);
                assert!(nonzero, "[diag] block {block} is all zeros BEFORE shutdown — drain may have lost data");
            }
        }

        // Shutdown (drops local SSD cache)
        server.shutdown().await;

        // Phase 2: cold restart from S3 manifest
        eprintln!("[fio-verify] cold restart from S3...");
        let cache_dir2 = TempDir::new().unwrap();
        let clean_cache: Arc<dyn BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let router2 = Arc::new(
            ExportRouter::new(RouterConfig {
                object_store: Arc::clone(&s3),
                db_path: "verify".to_string(),
                cache_dir: cache_dir2.path().to_path_buf(),
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

        // Restore export from S3 manifest
        let config = ExportConfig {
            name: "verify".to_string(),
            size_gb: DEVICE_SIZE_GB,
            s3_prefix: None,
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        router2
            .create_export(config, false, Some("verify"), None)
            .await
            .unwrap();

        let handler = router2
            .get_handler("verify")
            .await
            .expect("no handler after cold wake");

        let mut ublk_server2 = UblkServer::new();
        let dev_path2 = ublk_server2
            .add_device("verify", handler)
            .await
            .expect("failed to register ublk device");

        eprintln!("[fio-verify] cold device ready: {}", dev_path2.display());

        // Diagnostic: verify handler reads BEFORE fio (bypasses ublk, tests cold read path)
        {
            let handler2 = router2.get_handler("verify").await
                .expect("handler should exist on cold router");
            let mut cold_failures = Vec::new();
            for block in [0u64, 512, 960, 1000, 1023] {
                let offset = block * 128 * 1024;
                match handler2.read(offset, 4096).await {
                    Ok(data) => {
                        let nonzero = data.iter().any(|&b| b != 0);
                        eprintln!("[diag-cold] block {block} (offset {offset}): nonzero={nonzero}, first4=[{:#04x},{:#04x},{:#04x},{:#04x}]",
                            data[0], data[1], data[2], data[3]);
                        if !nonzero {
                            cold_failures.push(block);
                        }
                    }
                    Err(e) => {
                        eprintln!("[diag-cold] block {block} read ERROR: {e:?}");
                        cold_failures.push(block);
                    }
                }
            }
            if !cold_failures.is_empty() {
                eprintln!("[diag-cold] CORRUPTION DETECTED via handler reads (no ublk): blocks {:?}", cold_failures);
                eprintln!("[diag-cold] This means the bug is in the write/drain/manifest path, NOT ublk reads");
            } else {
                eprintln!("[diag-cold] All sampled blocks non-zero via handler — cold read path OK");
            }
        }

        // Phase 3: verify reads from cold device
        fio_verify_read("cold-verify", "read", &dev_path2, &["--size=128m"]);

        if let Err(e) = ublk_server2.shutdown().await {
            eprintln!("ublk shutdown error: {e}");
        }
        if let Err(e) = router2.shutdown().await {
            eprintln!("router shutdown error: {e}");
        }
    }

    /// Stress variant: cold-wake cycle repeated 5 times with higher concurrency.
    ///
    /// Increases surface area for timing-dependent bugs by:
    /// - Running multiple write→drain→cold-wake→verify cycles
    /// - Using 4 fio jobs (4× concurrent ublk writers)
    /// - Using iodepth=64 (more in-flight operations per job)
    /// - Writing 256MB per cycle (2048 blocks, spans multiple packs)
    ///
    /// Catches: races between flush scheduler and ublk writes under load,
    /// manifest corruption across multiple drain cycles, pack index cache
    /// staleness after cold restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn fio_verify_cold_wake_stress() {
        if skip_unless_ready() { return; }

        const ITERATIONS: usize = 5;
        const WRITE_SIZE: &str = "256m";

        for iteration in 0..ITERATIONS {
            eprintln!("\n[stress] === iteration {}/{ITERATIONS} ===", iteration + 1);

            let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

            // Phase 1: write with high concurrency
            let server = VerifyServer::start(Arc::clone(&s3)).await;
            let dev = &server.dev_path;

            fio_write("stress-write", "write", dev, &[
                &format!("--size={WRITE_SIZE}"),
                "--numjobs=4",
                "--iodepth=64",
            ]);

            // Drain to S3
            eprintln!("[stress] draining to S3...");
            server.router.drain_export("verify").await.unwrap();
            server.shutdown().await;

            // Phase 2: cold restart + verify
            eprintln!("[stress] cold restart from S3...");
            let cache_dir2 = TempDir::new().unwrap();
            let clean_cache: Arc<dyn BlockCache> =
                Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

            let router2 = Arc::new(
                ExportRouter::new(RouterConfig {
                    object_store: Arc::clone(&s3),
                    db_path: "verify".to_string(),
                    cache_dir: cache_dir2.path().to_path_buf(),
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

            let config = ExportConfig {
                name: "verify".to_string(),
                size_gb: DEVICE_SIZE_GB,
                s3_prefix: None,
                block_size: None,
                blocks_per_pack: None,
                flush_mode: None,
                transport: None,
            };
            router2
                .create_export(config, false, Some("verify"), None)
                .await
                .unwrap();

            let handler = router2
                .get_handler("verify")
                .await
                .expect("no handler after cold wake");

            let mut ublk_server2 = UblkServer::new();
            let dev_path2 = ublk_server2
                .add_device("verify", handler)
                .await
                .expect("failed to register ublk device");

            fio_verify_read("stress-verify", "read", &dev_path2, &[
                &format!("--size={WRITE_SIZE}"),
                "--numjobs=4",
                "--iodepth=64",
            ]);

            eprintln!("[stress] iteration {} passed", iteration + 1);

            if let Err(e) = ublk_server2.shutdown().await {
                eprintln!("ublk shutdown error: {e}");
            }
            if let Err(e) = router2.shutdown().await {
                eprintln!("router shutdown error: {e}");
            }
        }

        eprintln!("[stress] all {ITERATIONS} iterations passed");
    }

    /// Write with concurrent flush scheduler activity, then cold-wake verify.
    ///
    /// Unlike `fio_verify_after_cold_wake` which writes sequentially then drains,
    /// this test uses random writes with a longer runtime so the flush scheduler
    /// fires multiple times during the write phase. This maximizes the window
    /// for flush scheduler / ublk write interactions.
    ///
    /// Catches: data loss when flush scheduler claims blocks while ublk writes
    /// are in-flight (between pre_write and post_write).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn fio_verify_random_cold_wake() {
        if skip_unless_ready() { return; }

        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Phase 1: random writes for 60s with high concurrency.
        // The flush scheduler should fire multiple times during this window
        // (threshold = DEFAULT_BLOCKS_PER_PACK = 500 dirty blocks).
        let server = VerifyServer::start(Arc::clone(&s3)).await;
        let dev = &server.dev_path;

        // Write phase: random writes across 512MB region
        fio_write("rand-cold-write", "randwrite", dev, &[
            "--size=512m",
            "--runtime=60",
            "--time_based",
            "--numjobs=4",
            "--iodepth=64",
        ]);

        // Now do a sequential write pass so we have known data for verification.
        // This overwrites everything with verifiable CRC32C headers.
        fio_write("rand-cold-seq", "write", dev, &["--size=512m"]);

        // Drain
        eprintln!("[rand-cold] draining to S3...");
        server.router.drain_export("verify").await.unwrap();
        server.shutdown().await;

        // Phase 2: cold restart + verify
        eprintln!("[rand-cold] cold restart from S3...");
        let cache_dir2 = TempDir::new().unwrap();
        let clean_cache: Arc<dyn BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let router2 = Arc::new(
            ExportRouter::new(RouterConfig {
                object_store: Arc::clone(&s3),
                db_path: "verify".to_string(),
                cache_dir: cache_dir2.path().to_path_buf(),
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

        let config = ExportConfig {
            name: "verify".to_string(),
            size_gb: DEVICE_SIZE_GB,
            s3_prefix: None,
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        router2
            .create_export(config, false, Some("verify"), None)
            .await
            .unwrap();

        let handler = router2
            .get_handler("verify")
            .await
            .expect("no handler after cold wake");

        let mut ublk_server2 = UblkServer::new();
        let dev_path2 = ublk_server2
            .add_device("verify", handler)
            .await
            .expect("failed to register ublk device");

        fio_verify_read("rand-cold-verify", "read", &dev_path2, &["--size=512m"]);

        if let Err(e) = ublk_server2.shutdown().await {
            eprintln!("ublk shutdown error: {e}");
        }
        if let Err(e) = router2.shutdown().await {
            eprintln!("router shutdown error: {e}");
        }
    }
}
