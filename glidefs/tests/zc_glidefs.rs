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
    let cache_dir = TempDir::new().unwrap();
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let router = Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: s3,
            db_path: "zc-test".to_string(),
            cache_dir: cache_dir.path().to_path_buf(),
            block_size: 4096,
            clean_cache,
            wal_sync: false,
            max_s3_uploads: 8,
            max_s3_downloads: 16,
            default_flush_threshold,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
            max_exports: 10,
            manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
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
    };
    router
        .create_export(config, false, None, None)
        .await
        .expect("create_export");
    (router, cache_dir)
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
    let (router, _cache_dir) =
        setup_router_with_flush_threshold(DEFAULT_FLUSH_THRESHOLD).await;
    let handler = router.get_handler(EXPORT_NAME).await.expect("handler");

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
    if std::env::var_os("GLIDEFS_TEST_FORCE_USER_COPY").is_some() {
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
