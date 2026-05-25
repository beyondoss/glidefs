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
            default_flush_threshold: DEFAULT_FLUSH_THRESHOLD,
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
