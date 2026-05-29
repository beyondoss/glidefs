//! Foyer I/O engine selection.
//!
//! foyer's default psync engine dispatches every SSD-tier read to a tokio
//! blocking thread (`spawn_blocking` -> `pread`). The io_uring engine submits the
//! read inline instead, which measures ~25% lower per-read latency / ~37% higher
//! throughput on cached reads (see `benches/foyer_ssd_read.rs`). We prefer io_uring
//! on Linux and fall back to psync if io_uring setup fails (e.g. seccomp/gVisor
//! sandboxes) or on non-Linux platforms. There is no config knob — selection is
//! automatic.

use std::future::Future;
use std::path::Path;

use foyer::{HybridCache, IoEngineConfig, StorageKey, StorageValue};

/// Whether the filesystem backing `dir` supports `O_DIRECT`.
///
/// O_DIRECT lets the cache bypass the OS page cache, so it stops double-caching
/// data foyer already holds and stops evicting co-resident tenants' pages (see
/// `benches/` measurements). But it `EINVAL`s on filesystems that don't support
/// it (notably tmpfs, common for `/tmp` cache dirs in dev/CI), so we probe before
/// committing and let callers fall back to buffered I/O. The probe opens and
/// removes a tiny file with `O_DIRECT`; the result is used immediately, so TOCTOU
/// is not a concern.
pub fn o_direct_supported(dir: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let probe = dir.join(".glidefs_odirect_probe");
        let ok = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(&probe)
            .is_ok();
        let _ = std::fs::remove_file(&probe);
        ok
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = dir;
        false
    }
}

/// Build a foyer [`HybridCache`], preferring the io_uring I/O engine on Linux and
/// degrading gracefully to foyer's default psync engine.
///
/// `build` is invoked with the engine config to install: `Some(io_uring)` on the
/// Linux attempt, or `None` to let foyer use its psync default. It must
/// reconstruct the full builder on each call because foyer's device and storage
/// builders are consuming, so a failed io_uring attempt can't reuse them.
pub async fn build_preferring_uring<K, V, F, Fut>(
    name: &str,
    build: F,
) -> anyhow::Result<HybridCache<K, V>>
where
    K: StorageKey,
    V: StorageValue,
    F: Fn(Option<Box<dyn IoEngineConfig>>) -> Fut,
    Fut: Future<Output = foyer::Result<HybridCache<K, V>>>,
{
    #[cfg(target_os = "linux")]
    {
        match build(Some(Box::new(foyer::UringIoEngineConfig::new()))).await {
            Ok(cache) => {
                tracing::debug!(cache = name, "foyer cache using io_uring I/O engine");
                return Ok(cache);
            }
            Err(e) => tracing::warn!(
                cache = name,
                error = %e,
                "io_uring I/O engine init failed; falling back to psync"
            ),
        }
    }

    let cache = build(None).await?;
    tracing::debug!(cache = name, "foyer cache using psync I/O engine");
    Ok(cache)
}
