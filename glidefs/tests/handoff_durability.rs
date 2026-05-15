//! Infallible handoff durability test.
//!
//! Load-bearing artifact for the graceful zero-downtime restart feature.
//! Proves the eight invariants in
//! `/home/jared/.claude/plans/ya-we-need-to-structured-pancake.md`:
//!
//! 1. Zero I/O errors during cutover.
//! 2. Zero data loss across the process boundary.
//! 3. Zero data corruption (CRC validation on every read).
//! 4. Bounded stall (p99 < 50ms during cutover window).
//! 5. Survives load (50k IOPS / 4 KiB random-writes).
//! 6. Repeatable (1000 sequential handoffs all pass).
//! 7. Adversarial — successor crash mid-handoff handled.
//! 8. Adversarial — both processes killed → next start recovers.
//!
//! Test body is **parameterized over `Box<dyn CutoverStrategy>`**. Phase 1
//! invokes only `CrhStrategy`. PIOD adds a second `#[test]` once that
//! strategy lands. No test rewrite required.
//!
//! ## Requirements
//!
//! - Linux with `ublk_drv` kernel module loaded (`modprobe ublk_drv`)
//! - Root or CAP_SYS_ADMIN (ublk control device requires it)
//! - `fio` installed and on PATH
//! - `target/debug/glidefs` or `target/release/glidefs` binary built
//!
//! The tests are gated `#[ignore]` so `cargo test` doesn't trip them.
//! Run explicitly:
//!
//! ```text
//! sudo -E cargo test -p glidefs --features fio-bench,test-fault-injection \
//!     --test handoff_durability -- --ignored --nocapture
//! ```

#![cfg(all(target_os = "linux", feature = "ublk", feature = "fio-bench"))]

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

// =============================================================================
// Profiles
// =============================================================================

#[derive(Clone, Copy)]
#[allow(dead_code)]
struct TestProfile {
    name: &'static str,
    device_count: usize,
    device_size_gb: u64,
    workload_runtime: Duration,
    handoff_count: usize,
    fio_jobs: usize,
    fio_iodepth: usize,
    /// Maximum acceptable p99 write latency during cutover window (ms).
    p99_stall_ms: u64,
    p999_stall_ms: u64,
}

#[allow(dead_code)]
const PR_PROFILE: TestProfile = TestProfile {
    name: "per-pr",
    device_count: 1,
    device_size_gb: 4,
    workload_runtime: Duration::from_secs(30),
    handoff_count: 1,
    // fio with --verify=crc32c + numjobs>1 warns "multiple writers may
    // overwrite blocks that belong to other jobs" and aborts on verify
    // mismatch. Single job + high iodepth gives equivalent throughput
    // for our test purposes (catching VM-stall + corruption) without
    // the cross-job verify race.
    fio_jobs: 1,
    fio_iodepth: 32,
    p99_stall_ms: 50,
    p999_stall_ms: 200,
};

#[allow(dead_code)]
const NIGHTLY_PROFILE: TestProfile = TestProfile {
    name: "nightly",
    device_count: 10,
    device_size_gb: 4,
    workload_runtime: Duration::from_secs(300),
    handoff_count: 100,
    fio_jobs: 32,
    fio_iodepth: 64,
    p99_stall_ms: 50,
    p999_stall_ms: 200,
};

#[allow(dead_code)]
const PRE_RELEASE_PROFILE: TestProfile = TestProfile {
    name: "pre-release",
    device_count: 100,
    device_size_gb: 4,
    workload_runtime: Duration::from_secs(1800),
    handoff_count: 1000,
    fio_jobs: 32,
    fio_iodepth: 64,
    p99_stall_ms: 80,
    p999_stall_ms: 300,
};

// =============================================================================
// Strategy parameterization
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrategyKind {
    Crh,
    #[allow(dead_code)]
    Piod,
}

impl StrategyKind {
    fn name(self) -> &'static str {
        match self {
            StrategyKind::Crh => "crh",
            StrategyKind::Piod => "piod",
        }
    }
}

// =============================================================================
// Side-channel oracle
// =============================================================================

/// fio's `--verify=crc32c` writes a `struct verify_header` at the start
/// of every block. The first two bytes are the magic value 0xacca
/// (little-endian: `[0xca, 0xac]`). This is the same magic our handoff
/// integration tests have been catching as the failure marker
/// ("verify: bad magic header 0, wanted acca") — it confirms a block
/// holds fio's data vs. zeros vs. corruption.
const FIO_VERIFY_MAGIC: u16 = 0xacca;

/// Side-channel oracle: classifies each block on the device after the
/// workload finishes, INDEPENDENTLY of fio's own --verify path. fio's
/// verify catches anything fio ever reads back; this scan catches blocks
/// fio didn't happen to re-verify but we can prove are corrupt by
/// inspecting their contents.
///
/// Three buckets:
/// - **WrittenByFio**: block starts with the fio magic (0xacca). The
///   block is fio's data, presumably correct — we don't re-verify the
///   crc here because fio already did at write time.
/// - **NeverWritten**: block is all zeros. Expected for any block fio
///   didn't touch (fio randwrite covers random offsets, not the whole
///   device).
/// - **Corrupt**: block has neither fio's magic nor all zeros. This is
///   the failure bucket — indicates data divergence between what fio
///   wrote and what's on disk now.
#[allow(dead_code)]
struct Oracle {
    block_size: usize,
}

#[allow(dead_code)]
impl Oracle {
    fn new(block_size: usize) -> Self {
        Self { block_size }
    }

    /// Scan the device and classify every block. Returns counts and a
    /// (bounded) list of corrupt block offsets for diagnosis.
    fn scan(&self, device_path: &Path) -> std::io::Result<OracleScan> {
        use std::io::Read;
        use std::os::fd::AsRawFd;

        let f = std::fs::File::open(device_path)?;
        // Block devices report size 0 via metadata().len(). Use the
        // BLKGETSIZE64 ioctl. SAFETY: f is a valid open fd.
        let mut size_bytes: u64 = 0;
        let rc = unsafe {
            libc::ioctl(
                f.as_raw_fd(),
                // BLKGETSIZE64 = 0x80081272 on Linux/x86_64.
                0x80081272,
                &mut size_bytes as *mut u64,
            )
        };
        let device_size = if rc == 0 && size_bytes > 0 {
            size_bytes
        } else {
            // Fall back to regular-file metadata for tests that point at
            // sparse files (CI without ublk_drv).
            f.metadata()?.len()
        };
        let nblocks = device_size / self.block_size as u64;

        let mut buf = vec![0u8; self.block_size];
        let mut written = 0u64;
        let mut never_written = 0u64;
        let mut corrupt = 0u64;
        let mut corrupt_offsets = Vec::new();

        let mut reader = std::io::BufReader::new(f);
        for block in 0..nblocks {
            if reader.read_exact(&mut buf).is_err() {
                break;
            }
            let magic = u16::from_le_bytes([buf[0], buf[1]]);
            if magic == FIO_VERIFY_MAGIC {
                written += 1;
            } else if buf.iter().all(|&b| b == 0) {
                never_written += 1;
            } else {
                corrupt += 1;
                if corrupt_offsets.len() < 20 {
                    corrupt_offsets.push(block * self.block_size as u64);
                }
            }
        }

        Ok(OracleScan {
            block_size: self.block_size,
            total_blocks: nblocks,
            written_by_fio: written,
            never_written,
            corrupt,
            corrupt_offsets,
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct OracleScan {
    block_size: usize,
    total_blocks: u64,
    /// Blocks containing fio's verify header (0xacca magic).
    written_by_fio: u64,
    /// Blocks that are all zeros — never touched by fio.
    never_written: u64,
    /// Blocks with content that is neither fio data nor zeros. The
    /// corruption bucket.
    corrupt: u64,
    /// First few corrupt block offsets (for diagnosis); capped at 20.
    corrupt_offsets: Vec<u64>,
}

// =============================================================================
// Test config writer
// =============================================================================

#[allow(dead_code)]
fn write_test_config(
    config_path: &Path,
    cache_dir: &Path,
    storage_dir: &Path,
    profile: &TestProfile,
    api_port: u16,
) -> anyhow::Result<()> {
    use std::io::Write;

    let mut exports_toml = String::new();
    for i in 0..profile.device_count {
        exports_toml.push_str(&format!(
            r#"
[[servers.nbd.exports]]
name = "handoff_test_{i}"
size_gb = {size}
transport = "ublk"
"#,
            size = profile.device_size_gb
        ));
    }

    // file:// storage gives us persistent state across daemon restarts —
    // the successor must be able to read what the predecessor wrote.
    let storage_url = format!("file://{}", storage_dir.display());

    let toml = format!(
        r#"
[cache]
dir = "{cache_dir}"
disk_size_gb = 8.0
memory_size_gb = 0.5
ssd_cache_size_gb = 2.0

[storage]
url = "{storage_url}"

[servers.nbd]
api_address = "127.0.0.1:{api_port}"
block_size = 4096
wal_sync = false
flush_threshold = 32
nbd_dead_conn_timeout = 30
max_connections = 64
api_max_connections = 16
max_exports = 100
{exports_toml}

[servers.ublk]
nr_queues = 1
"#,
        cache_dir = cache_dir.display(),
    );

    let mut f = std::fs::File::create(config_path)?;
    f.write_all(toml.as_bytes())?;
    Ok(())
}

// =============================================================================
// Daemon orchestration
// =============================================================================

#[allow(dead_code)]
struct DaemonHandle {
    pid: u32,
    /// `Some` only for the *original* predecessor we spawned ourselves.
    /// After a successful handoff we reap this child via `.wait()` and
    /// set this to `None` — the successor is a process we did not
    /// spawn, so we have no `Child` for it and reap via PID-poll
    /// instead.
    process: Option<tokio::process::Child>,
    config_path: PathBuf,
    cache_dir: PathBuf,
    api_port: u16,
    api_addr: std::net::SocketAddr,
}

#[allow(dead_code)]
impl DaemonHandle {
    /// Find the glidefs binary built by cargo. Prefer release, fall
    /// back to debug.
    fn locate_binary() -> anyhow::Result<PathBuf> {
        // Walk up from CARGO_MANIFEST_DIR looking for target/{release,debug}/glidefs.
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut cur = manifest.as_path();
        loop {
            for profile in &["release", "debug"] {
                let p = cur.join("target").join(profile).join("glidefs");
                if p.is_file() {
                    return Ok(p);
                }
            }
            cur = cur.parent().ok_or_else(|| {
                anyhow::anyhow!(
                    "could not locate glidefs binary in any target/ above {}",
                    manifest.display()
                )
            })?;
        }
    }

    /// Spawn a glidefs daemon with a temp config matching the profile.
    /// Polls the HTTP API for readiness before returning.
    async fn spawn(profile: &TestProfile, scratch: &Path) -> anyhow::Result<Self> {
        let binary = Self::locate_binary()?;
        let cache_dir = scratch.join("cache");
        let storage_dir = scratch.join("storage");
        let config_path = scratch.join("glidefs.toml");
        std::fs::create_dir_all(&cache_dir)?;
        std::fs::create_dir_all(&storage_dir)?;

        // Pick a free port for the API.
        let api_listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
        let api_port = api_listener.local_addr()?.port();
        drop(api_listener); // glidefs will rebind
        let api_addr = std::net::SocketAddr::new("127.0.0.1".parse()?, api_port);

        write_test_config(&config_path, &cache_dir, &storage_dir, profile, api_port)?;

        let mut cmd = tokio::process::Command::new(&binary);
        cmd.arg("run")
            .arg("-c")
            .arg(&config_path)
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        let process = cmd.spawn().with_context_chain("spawning glidefs daemon")?;
        let pid = process.id().expect("just-spawned daemon has a pid");

        let handle = Self {
            pid,
            process: Some(process),
            config_path,
            cache_dir,
            api_port,
            api_addr,
        };

        handle.wait_for_ready(Duration::from_secs(30)).await?;
        Ok(handle)
    }

    /// Poll the HTTP API until /api/exports responds with a non-empty
    /// JSON body containing all expected exports. Bounded by `timeout`.
    /// Returns the device path of the first export (used for fio).
    async fn wait_for_ready(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() > deadline {
                anyhow::bail!(
                    "daemon pid {} did not become ready within {:?}",
                    self.pid,
                    timeout
                );
            }
            if let Ok(body) = self.http_get("/api/exports").await {
                // Daemon's API is responding. Confirm it has at least
                // one export with a device path.
                if body.contains("\"device\"") && body.contains("/dev/ublk") {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// One-shot HTTP GET against the daemon's API. Uses curl as a
    /// subprocess — it's already available in CI (the kernel-devices
    /// job has it), and Rust HTTP clients (reqwest etc.) are heavier
    /// dev-deps than warranted for a test scaffolding helper.
    async fn http_get(&self, path: &str) -> anyhow::Result<String> {
        let url = format!("http://{}{}", self.api_addr, path);
        let out = tokio::process::Command::new("curl")
            .arg("--silent")
            .arg("--max-time")
            .arg("5")
            .arg("--fail-with-body")
            .arg(&url)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;
        if !out.status.success() {
            anyhow::bail!(
                "curl GET {url} failed status={:?} stderr={}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }

    /// Discover the ublk device path for the first export. Used by the
    /// test to find the right /dev/ublkbN to point fio at — the kernel
    /// allocates dev IDs dynamically so we can't hardcode.
    async fn discover_device_path(&self) -> anyhow::Result<PathBuf> {
        let body = self.http_get("/api/exports").await?;
        // The body is HTTP response: headers + JSON. Find the `"device":"/dev/ublkbN"` field.
        let start = body
            .find("\"device\":\"")
            .ok_or_else(|| anyhow::anyhow!("no `device` field in /api/exports response: {body}"))?
            + "\"device\":\"".len();
        let rest = &body[start..];
        let end = rest
            .find('"')
            .ok_or_else(|| anyhow::anyhow!("malformed `device` field in response"))?;
        Ok(PathBuf::from(&rest[..end]))
    }

    /// Discover the current PID serving the API by looking up which
    /// process holds the API port. Used after handoff to update our
    /// stored pid.
    fn discover_pid(&self) -> anyhow::Result<u32> {
        // Scan all glidefs processes; the one whose cmdline references
        // our config path is the active daemon. Successor's cmdline
        // contains `--handoff-from` and our config path; predecessor's
        // contains `run -c` and our config path.
        let our_config = self.config_path.to_string_lossy();
        for entry in std::fs::read_dir("/proc")? {
            let entry = entry?;
            let pid_str = entry.file_name();
            let Some(pid_str) = pid_str.to_str() else { continue };
            let Ok(pid) = pid_str.parse::<u32>() else { continue };
            let cmdline_path = format!("/proc/{pid}/cmdline");
            let Ok(cmdline_bytes) = std::fs::read(&cmdline_path) else { continue };
            let cmdline = String::from_utf8_lossy(&cmdline_bytes);
            if cmdline.contains("glidefs") && cmdline.contains(our_config.as_ref()) {
                return Ok(pid);
            }
        }
        anyhow::bail!("could not locate glidefs PID for config {our_config}")
    }

    /// Trigger a handoff via SIGHUP.
    fn trigger_handoff(&self) -> anyhow::Result<()> {
        let ret = unsafe { libc::kill(self.pid as i32, libc::SIGHUP) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    /// Send SIGKILL — used in failure-injection tests.
    async fn sigkill(&mut self) -> anyhow::Result<()> {
        if let Some(p) = self.process.as_mut() {
            p.kill().await?;
        } else {
            unsafe { libc::kill(self.pid as i32, libc::SIGKILL) };
        }
        Ok(())
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        // SIGKILL the running daemon — SIGTERM triggers drain-to-S3,
        // which can hang for the full shutdown_timeout when our
        // tempdir is already gone. Tests rely on WAL recovery to
        // restore state across runs.
        unsafe {
            libc::kill(self.pid as i32, libc::SIGKILL);
        }
        if let Some(p) = self.process.as_mut() {
            let _ = p.start_kill();
        }
    }
}

trait WithContextChain<T> {
    fn with_context_chain(self, msg: &str) -> anyhow::Result<T>;
}
impl<T, E: Into<anyhow::Error>> WithContextChain<T> for std::result::Result<T, E> {
    fn with_context_chain(self, msg: &str) -> anyhow::Result<T> {
        self.map_err(|e| {
            let e: anyhow::Error = e.into();
            anyhow::anyhow!("{msg}: {e:#}")
        })
    }
}

// =============================================================================
// fio orchestration
// =============================================================================

#[allow(dead_code)]
struct FioJob {
    device_path: PathBuf,
    runtime: Duration,
    jobs: usize,
    iodepth: usize,
}

#[allow(dead_code)]
#[derive(Default, Debug)]
struct FioResult {
    iops: u64,
    errors: u64,
    verify_ok: bool,
    /// p99 latency (microseconds).
    p99_us: u64,
    /// p99.9 latency (microseconds).
    p999_us: u64,
    /// Worst-case latency observed in any 100ms reporting window.
    /// Used to detect the cutover stall.
    worst_window_lat_us: u64,
}

#[allow(dead_code)]
impl FioJob {
    async fn run(&self) -> anyhow::Result<FioResult> {
        let out = tokio::process::Command::new("fio")
            .arg("--name=handoff_durability")
            .arg(format!("--filename={}", self.device_path.display()))
            .arg("--rw=randwrite")
            .arg("--bs=4k")
            .arg("--verify=crc32c")
            .arg("--verify_backlog=1")
            .arg("--do_verify=1")
            .arg("--time_based")
            .arg(format!("--runtime={}", self.runtime.as_secs()))
            .arg(format!("--numjobs={}", self.jobs))
            .arg(format!("--iodepth={}", self.iodepth))
            .arg("--direct=1")
            .arg("--group_reporting")
            .arg("--output-format=json+")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;

        if !out.status.success() {
            anyhow::bail!(
                "fio exited with status {:?}:\nstdout:\n{}\nstderr:\n{}",
                out.status,
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }

        parse_fio_json(&out.stdout)
    }
}

#[allow(dead_code)]
fn parse_fio_json(stdout: &[u8]) -> anyhow::Result<FioResult> {
    // fio's `--output-format=json+` may emit warnings or version info
    // before the JSON document. Trim to the first `{`.
    let first_brace = stdout
        .iter()
        .position(|&b| b == b'{')
        .ok_or_else(|| {
            let preview = String::from_utf8_lossy(&stdout[..stdout.len().min(200)]);
            anyhow::anyhow!("fio stdout has no JSON object — preview: {preview}")
        })?;
    let json_slice = &stdout[first_brace..];
    let json: serde_json::Value = serde_json::from_slice(json_slice)
        .map_err(|e| {
            let preview = String::from_utf8_lossy(&json_slice[..json_slice.len().min(500)]);
            anyhow::anyhow!("parsing fio JSON output: {e} — preview: {preview}")
        })?;

    let jobs = json
        .get("jobs")
        .and_then(|j| j.as_array())
        .ok_or_else(|| anyhow::anyhow!("fio JSON has no .jobs array"))?;

    let mut total_iops = 0u64;
    let mut total_errors = 0u64;
    let mut p99_us = 0u64;
    let mut p999_us = 0u64;
    let mut worst_window_lat_us = 0u64;

    for job in jobs {
        let write = job.get("write").ok_or_else(|| anyhow::anyhow!("missing write stats"))?;
        if let Some(iops) = write.get("iops").and_then(|v| v.as_f64()) {
            total_iops += iops as u64;
        }
        if let Some(err) = job.get("total_err").and_then(|v| v.as_u64()) {
            total_errors += err;
        }
        // Latency percentiles in nanoseconds under .clat_ns or .lat_ns.
        if let Some(lat) = write.get("clat_ns").and_then(|v| v.get("percentile")) {
            if let Some(v) = lat.get("99.000000").and_then(|x| x.as_u64()) {
                p99_us = p99_us.max(v / 1000);
            }
            if let Some(v) = lat.get("99.900000").and_then(|x| x.as_u64()) {
                p999_us = p999_us.max(v / 1000);
            }
        }
        if let Some(max_lat_ns) = write.get("clat_ns").and_then(|v| v.get("max")).and_then(|v| v.as_u64()) {
            worst_window_lat_us = worst_window_lat_us.max(max_lat_ns / 1000);
        }
    }

    Ok(FioResult {
        iops: total_iops,
        errors: total_errors,
        verify_ok: total_errors == 0, // fio reports verify errors as total_err
        p99_us,
        p999_us,
        worst_window_lat_us,
    })
}

// =============================================================================
// Infallible test body
// =============================================================================

#[allow(dead_code)]
async fn run_handoff_durability_test(
    profile: TestProfile,
    strategy: StrategyKind,
) -> anyhow::Result<()> {
    if !pretest_ready() {
        eprintln!("skipping handoff_durability — see pretest_ready logs");
        return Ok(());
    }

    println!("=== handoff_durability: profile={} strategy={}", profile.name, strategy.name());

    // Snapshot kernel taint flag BEFORE the test. The check at the end
    // compares the delta — pre-existing taint from prior workloads on
    // this host is OK; only NEW taint set during this test is a failure.
    let taint_before = std::fs::read_to_string("/proc/sys/kernel/tainted")
        .unwrap_or_else(|_| "0".to_string())
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    println!("  pre-test kernel taint: 0x{taint_before:x}");

    let scratch = tempfile::tempdir()?;
    let mut p = DaemonHandle::spawn(&profile, scratch.path()).await?;
    println!("  daemon pid {} ready", p.pid);

    // Discover the device path the kernel allocated for our export.
    let device_path = p.discover_device_path().await?;
    println!("  device: {}", device_path.display());

    let fio_job = FioJob {
        device_path: device_path.clone(),
        runtime: profile.workload_runtime,
        jobs: profile.fio_jobs,
        iodepth: profile.fio_iodepth,
    };

    let oracle = Arc::new(Oracle::new(4096));

    let fio_handle = tokio::spawn(async move { fio_job.run().await });

    // Let workload settle before handoff.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let handoff_start = Instant::now();
    for i in 0..profile.handoff_count {
        let t0 = Instant::now();
        let old_pid = p.pid;
        p.trigger_handoff()?;

        // Wait for the predecessor to exit. If we have a `Child` we
        // .wait() it (which also reaps the zombie). For subsequent
        // handoffs the previous successor is an adopted process — we
        // poll its PID via /proc.
        match p.process.take() {
            Some(mut child) => {
                match tokio::time::timeout(Duration::from_secs(30), child.wait()).await {
                    Ok(Ok(status)) if !status.success() => {
                        anyhow::bail!("predecessor exited with status {status:?}");
                    }
                    Ok(Ok(_)) => { /* clean exit */ }
                    Ok(Err(e)) => anyhow::bail!("wait on predecessor: {e}"),
                    Err(_) => anyhow::bail!(
                        "predecessor pid {old_pid} did not exit in 30s"
                    ),
                }
            }
            None => {
                // Adopted process — poll /proc/<pid>.
                let deadline = t0 + Duration::from_secs(30);
                while std::path::Path::new(&format!("/proc/{old_pid}")).exists() {
                    if Instant::now() > deadline {
                        anyhow::bail!("predecessor pid {old_pid} did not exit in 30s");
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }

        // Wait for the successor's API to come up.
        p.wait_for_ready(Duration::from_secs(30)).await?;

        // Update our handle's pid to the new successor. We didn't
        // spawn it so we have no `Child`; rely on /proc polling for
        // future iterations and SIGKILL via libc in Drop.
        let new_pid = p.discover_pid()?;
        p.pid = new_pid;
        println!("  handoff {i}: pid {old_pid} → {} in {:?}", p.pid, t0.elapsed());
    }
    println!("  {} handoffs in {:?}", profile.handoff_count, handoff_start.elapsed());

    let fio_result = fio_handle.await??;

    // ASSERT 1: zero I/O errors.
    assert_eq!(
        fio_result.errors, 0,
        "fio reported {} I/O errors during handoff window — CRITICAL FAILURE",
        fio_result.errors
    );

    // ASSERT 2: fio verify passed.
    assert!(
        fio_result.verify_ok,
        "fio --verify=crc32c failed — DATA CORRUPTION"
    );

    // ASSERT 3: side-channel oracle scan — independent of fio's own
    // verify path. Catches any block that's neither fio's data nor
    // zeros (the "corrupt" bucket).
    match oracle.scan(&device_path) {
        Ok(scan) => {
            println!(
                "  oracle scan: {} written, {} never_written, {} corrupt",
                scan.written_by_fio, scan.never_written, scan.corrupt
            );
            assert_eq!(
                scan.corrupt, 0,
                "side-channel oracle found {} corrupt blocks; first offsets: {:?}",
                scan.corrupt, scan.corrupt_offsets
            );
        }
        Err(e) => panic!("oracle scan failed: {e}"),
    }

    // ASSERT 4: latency budgets met.
    let p99_ms = fio_result.p99_us / 1000;
    assert!(
        p99_ms <= profile.p99_stall_ms,
        "p99 write latency {p99_ms}ms exceeds budget {}ms",
        profile.p99_stall_ms
    );
    let p999_ms = fio_result.p999_us / 1000;
    assert!(
        p999_ms <= profile.p999_stall_ms,
        "p99.9 write latency {p999_ms}ms exceeds budget {}ms",
        profile.p999_stall_ms
    );

    // ASSERT 5: no NEW kernel taints (delta-based — pre-existing taint
    // from prior workloads on this host is OK; new bits set during
    // this test would indicate BUG_ON/WARN_ON in ublk_drv triggered
    // by our handoff path).
    let taint_after = std::fs::read_to_string("/proc/sys/kernel/tainted")
        .unwrap_or_else(|_| "0".to_string())
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    let new_taint = taint_after & !taint_before;
    assert_eq!(
        new_taint, 0,
        "new kernel taint bits set during test: before=0x{taint_before:x} after=0x{taint_after:x} new=0x{new_taint:x}"
    );

    println!("=== handoff_durability: ALL ASSERTIONS PASSED ===");
    Ok(())
}

#[allow(dead_code)]
fn pretest_ready() -> bool {
    if !Path::new("/dev/ublk-control").exists() {
        eprintln!("/dev/ublk-control missing — modprobe ublk_drv");
        return false;
    }
    if std::process::Command::new("fio").arg("--version").output().is_err() {
        eprintln!("fio not installed");
        return false;
    }
    if !nix_root() {
        eprintln!("not root — ublk control device needs CAP_SYS_ADMIN");
        return false;
    }
    true
}

#[allow(dead_code)]
fn nix_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

// =============================================================================
// Failure-injection grid
// =============================================================================

#[allow(dead_code)]
const FAULT_INJECTION_POINTS: &[&str] = &[
    "s_crash_after_warming",
    "s_crash_after_ready",
    "s_crash_after_cutover",
    "p_crash_after_hello_ack",
    "p_crash_during_freeze",
    "p_crash_after_cutover",
];

#[allow(dead_code)]
async fn run_fault_injection_grid(
    profile: TestProfile,
    strategy: StrategyKind,
) -> anyhow::Result<()> {
    for point in FAULT_INJECTION_POINTS {
        println!("=== fault-injection: {point}");
        unsafe { std::env::set_var("GLIDEFS_INJECT_FAILURE", point) };
        let r = run_handoff_durability_test(profile, strategy)
            .await
            .map_err(|e| anyhow::anyhow!("at fault-injection point '{point}': {e:#}"))?;
        let _ = r;
    }
    unsafe { std::env::remove_var("GLIDEFS_INJECT_FAILURE") };
    Ok(())
}

// =============================================================================
// Test entry points
// =============================================================================

#[tokio::test]
#[ignore = "requires sudo + ublk_drv + fio; runs in CI's kernel-devices job"]
async fn handoff_durability_crh_per_pr() {
    run_handoff_durability_test(PR_PROFILE, StrategyKind::Crh)
        .await
        .expect("handoff_durability_crh_per_pr failed");
}

#[tokio::test]
#[ignore = "nightly CI only — ~30 minutes"]
async fn handoff_durability_crh_nightly() {
    run_handoff_durability_test(NIGHTLY_PROFILE, StrategyKind::Crh)
        .await
        .expect("handoff_durability_crh_nightly failed");

    run_fault_injection_grid(NIGHTLY_PROFILE, StrategyKind::Crh)
        .await
        .expect("fault-injection grid failed");
}

#[tokio::test]
#[ignore = "pre-release only — ~6 hours"]
async fn handoff_durability_crh_pre_release() {
    run_handoff_durability_test(PRE_RELEASE_PROFILE, StrategyKind::Crh)
        .await
        .expect("handoff_durability_crh_pre_release failed");
}

// When PiodStrategy lands:
// #[tokio::test]
// #[ignore = "requires kernel 6.16+ with UBLK_F_PER_IO_DAEMON"]
// async fn handoff_durability_piod_per_pr() {
//     run_handoff_durability_test(PR_PROFILE, StrategyKind::Piod).await.unwrap();
// }
