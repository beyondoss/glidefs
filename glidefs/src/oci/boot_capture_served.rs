//! Block-level boot-set capture by SERVING the blessed image (the `--profile`
//! producer for non-EROFS, GlideFS-built bases — currently ext4).
//!
//! The fanotify path-capture ([`crate::oci::boot_capture`]) records which *files*
//! a boot opens, then the ext4 writer maps those to file-DATA blocks. That misses
//! filesystem METADATA the kernel reads to *find* and *stat* the files (inode
//! tables, directory blocks) — measured ~21% of an ext4 boot, costing a large
//! readahead over-fetch tail. Those reads only exist at the BLOCK layer, so we
//! capture there: serve the blessed image over a throwaway ublk device with a
//! read-fault recorder, kernel-mount it, run the entrypoint once under a hard
//! timeout, and record the EXACT device blocks the kernel fetched (data AND
//! metadata). Zero over-fetch, ~100% coverage, and indifferent to the
//! filesystem — the same mechanism generalizes to raw/btrfs/f2fs (P3).
//!
//! Requires the `ublk` feature + root + an arch-compatible entrypoint. Best-
//! effort: any failure returns `None` and bless falls back to the fanotify+writer
//! (or static) boot set.

#[cfg(not(feature = "ublk"))]
pub async fn capture_boot_blocks_served(
    _content_store: std::sync::Arc<crate::block::content_store::ContentStore>,
    _volume_manifest: std::sync::Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    _device_size: u64,
    _block_size: u32,
    _fs_type: &str,
    _run_cmd: &[String],
    _env: &[String],
    _base_name: &str,
    _timeout: std::time::Duration,
    _max_blocks: usize,
) -> Option<Vec<u64>> {
    tracing::info!("boot profiling: built without the `ublk` feature — skipping block-level capture");
    None
}

/// Serve the blessed image over ublk, mount it, run the entrypoint under a read
/// tracer, and return the EXACT device blocks the boot read (first-touch order),
/// or `None` on any failure. See the module docs.
#[cfg(feature = "ublk")]
#[allow(clippy::too_many_arguments)] // a self-contained serving stack's deps
pub async fn capture_boot_blocks_served(
    content_store: std::sync::Arc<crate::block::content_store::ContentStore>,
    volume_manifest: std::sync::Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    device_size: u64,
    block_size: u32,
    fs_type: &str,
    run_cmd: &[String],
    env: &[String],
    base_name: &str,
    timeout: std::time::Duration,
    max_blocks: usize,
) -> Option<Vec<u64>> {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use crate::block::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
    use crate::block::handler::BlockHandler;
    use crate::block::metrics::ExportMetrics;
    use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use crate::block::pack_index_cache::PackIndexCache;
    use crate::block::ublk::UblkServer;
    use crate::block::write_cache::{WriteCache, WriteCacheConfig};
    use crate::block::write_trace::{boot_set_from_trace, WriteTracer};
    use tokio::sync::Notify;

    if run_cmd.is_empty() {
        return None;
    }
    if !std::path::Path::new("/dev/ublk-control").exists() {
        tracing::warn!("boot profiling: /dev/ublk-control absent — need root + ublk; skipping");
        return None;
    }

    let tmp = tempfile::TempDir::new().ok()?;
    let rtrace = tmp.path().join("boot.rtrace");
    let tracer = Arc::new(
        WriteTracer::new(&rtrace, block_size, device_size / u64::from(block_size), base_name).ok()?,
    );

    // A FRESH cold serving stack reading from the same store: cold reads fault to
    // S3 and the tracer records exactly which device blocks the boot touches.
    let cache = Arc::new(
        WriteCache::open_fresh_active(WriteCacheConfig {
            cache_dir: tmp.path().to_path_buf(),
            device_name: format!("profile-{base_name}"),
            device_size,
            block_size: block_size as usize,
            wal_sync: false,
        })
        .ok()?,
    );
    let foyer_dir = tmp.path().join("foyer");
    std::fs::create_dir_all(&foyer_dir).ok()?;
    let clean: Arc<dyn BlockCache> = Arc::new(
        FoyerBlockCache::open(FoyerCacheConfig {
            memory_bytes: 64 * 1024 * 1024,
            ssd_bytes: 256 * 1024 * 1024,
            ssd_dir: foyer_dir,
            direct: false,
            io_uring: false,
        })
        .await
        .ok()?,
    );
    let pack_index_cache = Arc::new(PackIndexCache::open(tmp.path()).await.ok()?);
    let handler = Arc::new(
        BlockHandler::new(
            cache,
            content_store,
            clean,
            pack_index_cache,
            volume_manifest,
            device_size,
            true, // read-only serve
            Arc::new(ExportMetrics::new()),
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::new(Notify::const_new()),
            DEFAULT_FLUSH_THRESHOLD,
            None,
        )
        .with_read_tracer(Some(Arc::clone(&tracer))),
    );

    let dev_name = format!("profile-{base_name}").replace('/', "-");
    let mut server = UblkServer::new();
    let dev = match server.add_device(&dev_name, Arc::clone(&handler)).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "boot profiling: ublk add_device failed; skipping");
            return None;
        }
    };
    tracing::info!(dev = %dev.display(), ?run_cmd, "boot profiling: serving image, mounting + running");

    // Mount + chroot-run the workload while ublk serves I/O. Long-running servers
    // are killed after their startup reads — that IS the boot working set.
    let fs_type = fs_type.to_string();
    let run_cmd = run_cmd.to_vec();
    let env = env.to_vec();
    let secs = timeout.as_secs().max(1).to_string();
    let mnt = tempfile::TempDir::new().ok()?;
    let mnt_path = mnt.path().to_path_buf();
    let ran = tokio::task::spawn_blocking(move || -> Result<(), String> {
        use std::process::Command;
        let m = Command::new("mount")
            .args(["-t", &fs_type, "-o", "ro"])
            .arg(&dev)
            .arg(&mnt_path)
            .status();
        if !matches!(m, Ok(s) if s.success()) {
            return Err(format!("mount -t {fs_type} {} failed: {m:?}", dev.display()));
        }
        for sub in ["proc", "dev", "sys"] {
            let _ = Command::new("mount").args(["--bind", &format!("/{sub}")]).arg(mnt_path.join(sub)).status();
        }
        let mut chroot = Command::new("unshare");
        chroot.arg("--net").arg("timeout").args(["--signal=KILL", &secs, "chroot"]).arg(&mnt_path);
        chroot.args(&run_cmd);
        chroot.env_clear();
        let mut have_path = false;
        for e in &env {
            if let Some((k, v)) = e.split_once('=') {
                if k == "PATH" {
                    have_path = true;
                }
                chroot.env(k, v);
            }
        }
        if !have_path {
            chroot.env("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");
        }
        chroot.env("HOME", "/root").env("LANG", "C");
        let st = chroot.status();
        tracing::info!("boot profiling: workload exited: {st:?}");
        for sub in ["sys", "dev", "proc"] {
            let _ = Command::new("umount").arg(mnt_path.join(sub)).status();
        }
        let _ = Command::new("umount").arg("-l").arg(&mnt_path).status();
        Ok(())
    })
    .await;

    // Always tear the device down.
    server.remove_device(&dev_name).await.ok();
    tracer.finish();

    match ran {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!("boot profiling: {e}");
            return None;
        }
        Err(e) => {
            tracing::warn!("boot profiling task panicked: {e}");
            return None;
        }
    }

    let bytes = std::fs::read(&rtrace).ok()?;
    let blocks = boot_set_from_trace(&bytes, max_blocks);
    if blocks.is_empty() {
        tracing::warn!("boot profiling: workload read nothing — no boot set captured");
        return None;
    }
    Some(blocks)
}
