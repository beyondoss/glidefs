//! End-to-end fio benchmark through a ublk block device.
//!
//! Spins up an ExportRouter + UblkServer backed by an in-memory object store,
//! creates a 2GB ublk device, runs three fio workloads (write, read, mixed),
//! and prints a summary table.
//!
//! Run: `sudo -E cargo test --features fio-bench --test fio_bench -- --nocapture`

#[cfg(all(target_os = "linux", feature = "fio-bench"))]
mod fio_bench {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;

    use glidefs::block::cache::{BlockCache, SimpleBlockCache};
    use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use glidefs::block::router::{ExportRouter, RouterConfig};
    use glidefs::block::ublk::UblkServer;
    use glidefs::config::ExportConfig;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    const DEVICE_SIZE_GB: f64 = 0.25; // 256 MiB — fits in QEMU 4G VM with headroom

    fn ublk_available() -> bool {
        Path::new("/dev/ublk-control").exists()
    }

    fn fio_available() -> bool {
        Command::new("fio").arg("--version").output().is_ok()
    }

    struct BenchServer {
        ublk_server: UblkServer,
        router: Arc<ExportRouter>,
        dev_path: PathBuf,
        _cache_dir: TempDir,
    }

    impl BenchServer {
        async fn start() -> Self {
            Self::start_with_options(false).await
        }

        /// Same as `start` but forces the legacy `UBLK_F_USER_COPY` transport
        /// even on a ZC-capable kernel — used by the A/B benchmark to
        /// compare the two transports on the same kernel.
        async fn start_force_user_copy() -> Self {
            Self::start_with_options(true).await
        }

        async fn start_with_options(force_user_copy: bool) -> Self {
            let cache_dir = TempDir::new().unwrap();
            let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

            let router = Arc::new(
                ExportRouter::new(RouterConfig {
                    object_store,
                    db_path: "bench".to_string(),
                    cache_dir: cache_dir.path().to_path_buf(),
                    block_size: 128 * 1024,
                    clean_cache,
                    wal_sync: false,
                    max_s3_uploads: 128,
                    max_s3_downloads: 512,
                    // Disable auto-flush so the bench measures transport
                    // throughput against a hot cache, not S3 store traffic.
                    // Without this, the in-memory store accumulates blocks
                    // and OOMs the 4 GiB QEMU VM.
                    default_flush_threshold: 0,
                    ublk_nr_queues: 4,
                    nbd_dead_conn_timeout: 0,
                    max_exports: 10_000,
                    manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
                })
                .await
                .expect("failed to create router"),
            );

            let config = ExportConfig {
                name: "bench".to_string(),
                size_gb: DEVICE_SIZE_GB,
                s3_prefix: None,
                block_size: None,
                flush_threshold: None,
                flush_mode: None,
                transport: None,
            };
            router.create_export(config, false, None, None).await.unwrap();

            let handler = router
                .get_handler("bench")
                .await
                .expect("no handler for bench export");

            let mut ublk_server = UblkServer::new();
            if force_user_copy {
                ublk_server.force_user_copy_transport();
            }
            let dev_path = ublk_server
                .add_device("bench", handler)
                .await
                .expect("failed to register ublk device");

            eprintln!("ublk device ready: {}", dev_path.display());

            Self {
                ublk_server,
                router,
                dev_path,
                _cache_dir: cache_dir,
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

    // -----------------------------------------------------------------------
    // fio runner + JSON parsing
    // -----------------------------------------------------------------------

    struct FioResult {
        workload: String,
        read_iops: f64,
        write_iops: f64,
        read_bw_kib: f64,
        write_bw_kib: f64,
        read_lat_avg_ns: f64,
        write_lat_avg_ns: f64,
        read_lat_p99_ns: f64,
        write_lat_p99_ns: f64,
    }

    fn run_fio(workload: &str, rw: &str, dev_path: &Path, extra_args: &[&str]) -> FioResult {
        let dev = dev_path.to_str().expect("non-utf8 dev path");
        let mut args = vec![
            "--name=glidefs-bench",
            "--filename",
            dev,
            "--rw",
            rw,
            "--ioengine=io_uring",
            "--bs=4k",
            "--iodepth=64",
            "--numjobs=1",
            "--direct=1",
            "--runtime=15",
            "--time_based",
            "--output-format=json",
            "--group_reporting",
        ];
        args.extend_from_slice(extra_args);

        eprintln!("running fio: {workload} ({rw})...");
        let output = Command::new("fio")
            .args(&args)
            .output()
            .expect("failed to execute fio");

        assert!(
            output.status.success(),
            "fio failed (exit {}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );

        // fio may prefix stdout with a text banner before the JSON object.
        // Find the first '{' and parse from there.
        let stdout = &output.stdout;
        let json_start = stdout
            .iter()
            .position(|&b| b == b'{')
            .expect("fio output contains no JSON");
        let json: serde_json::Value =
            serde_json::from_slice(&stdout[json_start..]).expect("fio produced invalid JSON");

        let job = &json["jobs"][0];

        let read_iops = job["read"]["iops"].as_f64().unwrap_or(0.0);
        let write_iops = job["write"]["iops"].as_f64().unwrap_or(0.0);
        let read_bw_kib = job["read"]["bw"].as_f64().unwrap_or(0.0);
        let write_bw_kib = job["write"]["bw"].as_f64().unwrap_or(0.0);
        let read_lat_avg_ns = job["read"]["lat_ns"]["mean"].as_f64().unwrap_or(0.0);
        let write_lat_avg_ns = job["write"]["lat_ns"]["mean"].as_f64().unwrap_or(0.0);
        // fio 3.x reports percentiles under clat_ns (completion latency).
        let read_lat_p99_ns = job["read"]["clat_ns"]["percentile"]["99.000000"]
            .as_f64()
            .unwrap_or(0.0);
        let write_lat_p99_ns = job["write"]["clat_ns"]["percentile"]["99.000000"]
            .as_f64()
            .unwrap_or(0.0);

        FioResult {
            workload: workload.to_string(),
            read_iops,
            write_iops,
            read_bw_kib,
            write_bw_kib,
            read_lat_avg_ns,
            write_lat_avg_ns,
            read_lat_p99_ns,
            write_lat_p99_ns,
        }
    }

    fn print_results(results: &[FioResult]) {
        eprintln!();
        eprintln!("=== glidefs ublk fio benchmark ===");
        eprintln!();
        eprintln!(
            "{:<16} {:>10} {:>12} {:>14} {:>14}",
            "workload", "IOPS", "BW (MB/s)", "lat_avg (us)", "lat_p99 (us)"
        );
        eprintln!("{}", "-".repeat(70));

        for r in results {
            // For pure read/write workloads, show the relevant direction.
            // For mixed, show combined IOPS and read latency (dominant direction).
            let (iops, bw_kib, lat_avg_ns, lat_p99_ns) = if r.read_iops > 0.0 && r.write_iops > 0.0
            {
                // Mixed workload: combine IOPS/BW, show read latency
                (
                    r.read_iops + r.write_iops,
                    r.read_bw_kib + r.write_bw_kib,
                    r.read_lat_avg_ns,
                    r.read_lat_p99_ns,
                )
            } else if r.write_iops > 0.0 {
                (
                    r.write_iops,
                    r.write_bw_kib,
                    r.write_lat_avg_ns,
                    r.write_lat_p99_ns,
                )
            } else {
                (
                    r.read_iops,
                    r.read_bw_kib,
                    r.read_lat_avg_ns,
                    r.read_lat_p99_ns,
                )
            };

            let bw_mb = bw_kib / 1024.0;
            let lat_avg_us = lat_avg_ns / 1000.0;
            let lat_p99_us = lat_p99_ns / 1000.0;

            eprintln!(
                "{:<16} {:>10.0} {:>12.1} {:>14.1} {:>14.1}",
                r.workload, iops, bw_mb, lat_avg_us, lat_p99_us
            );
        }
        eprintln!();
    }

    // -----------------------------------------------------------------------
    // The actual test
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fio_benchmark() {
        if !ublk_available() {
            eprintln!("skipping fio benchmark: ublk_drv not available");
            return;
        }
        if !fio_available() {
            eprintln!("skipping fio benchmark: fio not installed");
            return;
        }

        let server = BenchServer::start().await;
        let dev = &server.dev_path;

        // 1. Random writes (pre-fills device for subsequent read benchmark)
        let write_result = run_fio("randwrite-4k", "randwrite", dev, &[]);

        // 2. Random reads (hits write cache populated by step 1)
        let read_result = run_fio("randread-4k", "randread", dev, &[]);

        // 3. Mixed 70/30 read/write
        let mixed_result = run_fio("mixed-4k", "randrw", dev, &["--rwmixread=70"]);

        print_results(&[write_result, read_result, mixed_result]);

        server.shutdown().await;
    }

    /// A/B benchmark: ZC vs USER_COPY on the same kernel.
    ///
    /// Runs four canonical workloads against a hot-cache device under each
    /// transport. Prints per-workload IOPS / bandwidth / latency for both,
    /// then a delta table. On a ZC-capable kernel (≥6.17) we expect ZC to
    /// win on every workload by skipping the kernel↔userspace memcpy on
    /// the data plane.
    ///
    /// Skips if the kernel doesn't advertise both ZC bits — the comparison
    /// is meaningless there (both runs would be USER_COPY).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fio_benchmark_zc_vs_usercopy() {
        if !ublk_available() {
            eprintln!("skipping fio A/B benchmark: ublk_drv not available");
            return;
        }
        if !fio_available() {
            eprintln!("skipping fio A/B benchmark: fio not installed");
            return;
        }

        // Probe: does the kernel actually support ZC? If not, both runs are
        // USER_COPY and the comparison is noise.
        let features = glidefs::block::ublk::device::detect_features();
        if !features.zero_copy {
            eprintln!(
                "skipping fio A/B benchmark: kernel does not advertise \
                 UBLK_F_SUPPORT_ZERO_COPY + UBLK_F_AUTO_BUF_REG"
            );
            return;
        }
        // CI runs this whole test file under both transports as a matrix
        // (Kernel Devices job). On the USER_COPY matrix row,
        // `GLIDEFS_FORCE_USER_COPY=1` masks the ZC bit at daemon startup —
        // both "passes" of this A/B test would actually be USER_COPY,
        // making the delta meaningless. Skip in that mode; the comparison
        // only belongs in the zero-copy row.
        if std::env::var("GLIDEFS_FORCE_USER_COPY").is_ok_and(|v| !v.is_empty()) {
            eprintln!(
                "skipping fio A/B benchmark: GLIDEFS_FORCE_USER_COPY set \
                 — the matrix's USER_COPY row already exercises that path \
                 via the rest of this file"
            );
            return;
        }

        let workloads: &[(&str, &str, &[&str])] = &[
            ("4k-randwrite", "randwrite", &["--bs=4k"]),
            ("4k-randread", "randread", &["--bs=4k"]),
            ("128k-seqwrite", "write", &["--bs=128k"]),
            ("128k-seqread", "read", &["--bs=128k"]),
        ];

        eprintln!("\n=== Pass 1: kernel ZC transport (UBLK_F_AUTO_BUF_REG) ===");
        let zc_results = {
            let server = BenchServer::start().await;
            // Pre-fill so reads hit the warm cache.
            let _prefill = run_fio("prefill", "write", &server.dev_path, &["--bs=128k"]);
            let mut out = Vec::new();
            for (name, rw, args) in workloads {
                let r = run_fio(name, rw, &server.dev_path, args);
                out.push(r);
            }
            server.shutdown().await;
            out
        };

        eprintln!("\n=== Pass 2: legacy USER_COPY transport (forced) ===");
        let uc_results = {
            let server = BenchServer::start_force_user_copy().await;
            let _prefill = run_fio("prefill", "write", &server.dev_path, &["--bs=128k"]);
            let mut out = Vec::new();
            for (name, rw, args) in workloads {
                let r = run_fio(name, rw, &server.dev_path, args);
                out.push(r);
            }
            server.shutdown().await;
            out
        };

        eprintln!("\n=== ZC results ===");
        print_results(&zc_results);
        eprintln!("=== USER_COPY results ===");
        print_results(&uc_results);

        eprintln!("\n=== ZC vs USER_COPY delta (positive = ZC wins) ===");
        eprintln!(
            "{:<16} {:>12} {:>12} {:>12}",
            "workload", "iops Δ%", "bw Δ%", "p99 lat Δ%"
        );
        eprintln!("{}", "-".repeat(54));
        let mut wins = 0usize;
        for (zc, uc) in zc_results.iter().zip(uc_results.iter()) {
            let zc_iops = zc.read_iops.max(zc.write_iops);
            let uc_iops = uc.read_iops.max(uc.write_iops);
            let zc_bw = zc.read_bw_kib.max(zc.write_bw_kib);
            let uc_bw = uc.read_bw_kib.max(uc.write_bw_kib);
            let zc_p99 = zc.read_lat_p99_ns.max(zc.write_lat_p99_ns);
            let uc_p99 = uc.read_lat_p99_ns.max(uc.write_lat_p99_ns);

            let iops_d = pct_delta(zc_iops, uc_iops);
            let bw_d = pct_delta(zc_bw, uc_bw);
            // Lower latency is better — invert sign so positive = ZC wins.
            let p99_d = -pct_delta(zc_p99, uc_p99);

            eprintln!(
                "{:<16} {:>+11.2}% {:>+11.2}% {:>+11.2}%",
                zc.workload, iops_d, bw_d, p99_d
            );
            if iops_d >= 10.0 {
                wins += 1;
            }
        }
        eprintln!();
        eprintln!(
            "ZC won (≥10% IOPS uplift) on {} / {} workloads",
            wins,
            zc_results.len()
        );
    }

    fn pct_delta(a: f64, b: f64) -> f64 {
        if b == 0.0 {
            0.0
        } else {
            (a - b) / b * 100.0
        }
    }
}
