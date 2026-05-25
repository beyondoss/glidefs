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
//! Uses `#[tokio::test(flavor = "multi_thread")]` so glidefs's ZC
//! integration (which uses `Handle::block_on` from a blocking thread)
//! can make progress. On a single-thread runtime, glidefs detects the
//! flavor and falls back to USER_COPY automatically.

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

/// One I/O at the start of the device — a 4 KiB write followed by a
/// 4 KiB read with byte-level comparison. Runs whichever transport
/// glidefs auto-selects (ZC on ≥6.17, USER_COPY otherwise).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn zc_glidefs_roundtrip_4k() {
    if let Some(why) = skip_reason() {
        eprintln!("skip: {why}");
        return;
    }

    let (router, _cache_dir) = setup_router().await;
    let handler = router.get_handler(EXPORT_NAME).await.expect("handler");

    let mut ublk_server = UblkServer::new();
    let dev_path = ublk_server
        .add_device(EXPORT_NAME, handler)
        .await
        .expect("ublk register");
    eprintln!("ublk device: {} (flags: {:#x})", dev_path.display(), 0);

    let bdev = dev_path.clone();
    let io_result = tokio::task::spawn_blocking(move || run_io(&bdev))
        .await
        .expect("spawn_blocking join");

    // Shut down first so we don't leak the kernel device on assertion failure.
    if let Err(e) = ublk_server.shutdown().await {
        eprintln!("ublk shutdown error: {e}");
    }
    if let Err(e) = router.shutdown().await {
        eprintln!("router shutdown error: {e}");
    }

    io_result.expect("zc roundtrip");
}

fn run_io(dev: &Path) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::fs::OpenOptionsExt;

    let pattern: Vec<u8> = (0..4096usize).map(|i| (i & 0xff) as u8).collect();
    let aligned = aligned_4k(&pattern);

    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(dev)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&aligned)?;
        file.sync_data()?;
    }
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(dev)?;
        let mut readback = aligned_4k(&vec![0u8; 4096]);
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut readback)?;
        assert_eq!(
            &readback[..pattern.len()],
            pattern.as_slice(),
            "roundtrip data mismatch"
        );
        eprintln!("ROUND-TRIP MATCH");
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
