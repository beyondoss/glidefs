//! Block-level boot-set capture by SERVING the blessed image (the `--profile`
//! producer for non-EROFS, GlideFS-built bases — currently ext4).
//!
//! The fanotify path-capture ([`crate::oci::boot_capture`]) records which *files*
//! a boot opens, then the ext4 writer maps those to file-DATA blocks. That misses
//! filesystem METADATA the kernel reads to *find* and *stat* the files (inode
//! tables, directory blocks) — measured ~21% of an ext4 boot, costing a large
//! readahead over-fetch tail. Those reads only exist at the BLOCK layer, so we
//! capture there: serve the blessed image over a throwaway ublk device with a
//! read-fault recorder, run the entrypoint once **under an isolation sandbox**
//! ([`crate::oci::sandbox`]) under a hard timeout, and record the EXACT device
//! blocks the kernel fetched (data AND metadata). Zero over-fetch, ~100% coverage,
//! and indifferent to the filesystem — the same mechanism generalizes to
//! raw/btrfs/f2fs.
//!
//! The entrypoint is run through the pluggable [`Sandbox`] (hardened namespaces by
//! default — see [`crate::oci::sandbox`]) so a buggy or hostile entrypoint cannot
//! escape, OOM, or hang the build node. The run is repeated `runs` times and the
//! ordered block lists are **rank-merged** (boot nondeterminism), and the static
//! boot-set closure is read under the tracer so the captured set is the **union**
//! of the real boot and the static closure by construction.
//!
//! Requires the `ublk` feature + root + an arch-compatible entrypoint. Best-
//! effort: any failure returns `None` and bless falls back to the fanotify+writer
//! (or static) boot set.

use std::sync::Arc;
use std::time::Duration;

use crate::oci::sandbox::{ResourceLimits, Sandbox};

/// Knobs for a block-level capture: the isolation backend, accident-protection
/// limits, how many runs to rank-merge, the per-run timeout, the static-seed
/// closure to union in, and the capture cap.
pub struct BootProfileOptions {
    /// Isolation backend (built by the caller via `select_sandbox`, so the
    /// trusted-image gate surfaces there). Moved into `spawn_blocking`, hence `Arc`.
    pub sandbox: Arc<dyn Sandbox>,
    /// cgroup cpu/memory/pids caps for the run (accident protection).
    pub limits: ResourceLimits,
    /// Number of boot runs to rank-merge (1–3; 1 ≈ 97% stable per REAP).
    pub runs: u32,
    /// Hard per-run wall-clock timeout.
    pub timeout: Duration,
    /// Absolute in-image paths (the static ELF closure) to read under the tracer
    /// so the captured set unions the static boot-set by construction.
    pub static_seed: Vec<String>,
    /// Cap on captured blocks per run.
    pub max_blocks: usize,
    /// Parent directory for the per-run scratch (write cache + foyer clean
    /// cache). `None` falls back to `std::env::temp_dir()`.
    ///
    /// The long-lived daemon MUST set this to a real on-disk path (e.g. the
    /// configured `[cache].dir`): `std::env::temp_dir()` is `/tmp`, which is a
    /// tmpfs on many hosts, so the profiler's disk cache would otherwise run in
    /// RAM. Setting it here makes placement correct by construction, regardless
    /// of whether `$TMPDIR` is exported in the unit. Short-lived CLI callers can
    /// leave it `None` — their scratch is reclaimed when the process exits.
    pub scratch_dir: Option<std::path::PathBuf>,
}

/// Rank-merge ordered block lists from multiple boot runs into one ordered union.
/// A block ranks higher the more runs include it, then by its earliest (best)
/// first-touch position, then by index — deterministic and stable. With one run
/// this is just that run's first-touch order.
pub fn rank_merge(runs: &[Vec<u64>]) -> Vec<u64> {
    use std::collections::HashMap;
    let mut count: HashMap<u64, u32> = HashMap::new();
    let mut best_rank: HashMap<u64, usize> = HashMap::new();
    for run in runs {
        for (rank, &b) in run.iter().enumerate() {
            *count.entry(b).or_insert(0) += 1;
            let e = best_rank.entry(b).or_insert(usize::MAX);
            if rank < *e {
                *e = rank;
            }
        }
    }
    let mut blocks: Vec<u64> = count.keys().copied().collect();
    blocks.sort_by(|&a, &b| {
        count[&b]
            .cmp(&count[&a])
            .then(best_rank[&a].cmp(&best_rank[&b]))
            .then(a.cmp(&b))
    });
    blocks
}

#[cfg(not(feature = "ublk"))]
#[allow(clippy::too_many_arguments)]
pub async fn capture_boot_blocks_served(
    _content_store: std::sync::Arc<crate::block::content_store::ContentStore>,
    _volume_manifest: std::sync::Arc<
        parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
    >,
    _device_size: u64,
    _block_size: u32,
    _fs_type: &str,
    _argv: &[String],
    _env: &[String],
    _workdir: &str,
    _base_name: &str,
    _opts: &BootProfileOptions,
) -> Option<Vec<u64>> {
    tracing::info!(
        "boot profiling: built without the `ublk` feature — skipping block-level capture"
    );
    None
}

/// Serve the blessed image over ublk, run the entrypoint under the sandbox + read
/// tracer `runs` times, and return the rank-merged union of the exact boot blocks
/// (incl. the static closure), or `None` on failure. See the module docs.
#[cfg(feature = "ublk")]
#[allow(clippy::too_many_arguments)]
pub async fn capture_boot_blocks_served(
    content_store: std::sync::Arc<crate::block::content_store::ContentStore>,
    volume_manifest: std::sync::Arc<
        parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
    >,
    device_size: u64,
    block_size: u32,
    fs_type: &str,
    argv: &[String],
    env: &[String],
    workdir: &str,
    base_name: &str,
    opts: &BootProfileOptions,
) -> Option<Vec<u64>> {
    if argv.is_empty() {
        return None;
    }
    if !std::path::Path::new("/dev/ublk-control").exists() {
        tracing::warn!("boot profiling: /dev/ublk-control absent — need root + ublk; skipping");
        return None;
    }

    let runs = opts.runs.clamp(1, 3);
    let mut captured: Vec<Vec<u64>> = Vec::with_capacity(runs as usize);
    for run in 0..runs {
        match capture_once(
            &content_store,
            &volume_manifest,
            device_size,
            block_size,
            fs_type,
            argv,
            env,
            workdir,
            base_name,
            run,
            opts,
        )
        .await
        {
            Some(blocks) if !blocks.is_empty() => {
                tracing::info!(run, blocks = blocks.len(), "captured boot run");
                captured.push(blocks);
            }
            _ => tracing::warn!(run, "boot run captured nothing"),
        }
    }

    if captured.is_empty() {
        tracing::warn!("boot profiling: no run captured anything — no boot set");
        return None;
    }
    let merged = rank_merge(&captured);
    tracing::info!(
        runs = captured.len(),
        blocks = merged.len(),
        "rank-merged boot set"
    );
    Some(merged)
}

/// One serve+run+capture cycle. A fresh cold serving stack + ublk device per run,
/// so each run's first-touch order is independent (no host page-cache carryover).
#[cfg(feature = "ublk")]
#[allow(clippy::too_many_arguments)]
async fn capture_once(
    content_store: &std::sync::Arc<crate::block::content_store::ContentStore>,
    volume_manifest: &std::sync::Arc<
        parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
    >,
    device_size: u64,
    block_size: u32,
    fs_type: &str,
    argv: &[String],
    env: &[String],
    workdir: &str,
    base_name: &str,
    run: u32,
    opts: &BootProfileOptions,
) -> Option<Vec<u64>> {
    use std::sync::atomic::AtomicU64;

    use crate::block::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
    use crate::block::handler::BlockHandler;
    use crate::block::metrics::ExportMetrics;
    use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use crate::block::pack_index_cache::PackIndexCache;
    use crate::block::ublk::UblkServer;
    use crate::block::write_cache::{WriteCache, WriteCacheConfig};
    use crate::block::write_trace::{WriteTracer, boot_set_from_trace};
    use crate::oci::sandbox::SandboxSpec;
    use tokio::sync::Notify;

    // Scratch (write cache + foyer clean cache) goes under `opts.scratch_dir`
    // when set — keeping the profiler's disk cache off a tmpfs `/tmp`. Falls
    // back to `std::env::temp_dir()` only for callers that opt out (None).
    let tmp = match opts.scratch_dir.as_deref() {
        Some(dir) => {
            std::fs::create_dir_all(dir).ok()?;
            tempfile::TempDir::new_in(dir).ok()?
        }
        None => tempfile::TempDir::new().ok()?,
    };
    let rtrace = tmp.path().join("boot.rtrace");
    let tracer = Arc::new(
        WriteTracer::new(
            &rtrace,
            block_size,
            device_size / u64::from(block_size),
            base_name,
        )
        .ok()?,
    );

    // A FRESH cold serving stack reading from the same store: cold reads fault to
    // S3 and the tracer records exactly which device blocks the boot touches.
    let cache = Arc::new(
        WriteCache::open_fresh_active(WriteCacheConfig {
            cache_dir: tmp.path().to_path_buf(),
            device_name: format!("profile-{base_name}-{run}"),
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
    // Keep a handle to close the foyer clean cache before `tmp` (the TempDir)
    // is dropped. foyer's SSD-tier region files stay open until the storage
    // engine is closed; if the TempDir's `remove_dir_all` runs first, those
    // files become deleted-but-held fds that never free until the daemon
    // exits — so a daemon that profiles many images leaks them unbounded into
    // $TMPDIR (which is tmpfs on many hosts → node-wide ENOSPC).
    let clean_for_close = Arc::clone(&clean);

    let result = async {
    let pack_index_cache = Arc::new(PackIndexCache::open(tmp.path()).await.ok()?);
    let handler = Arc::new(
        BlockHandler::new(
            cache,
            Arc::clone(content_store),
            clean,
            pack_index_cache,
            Arc::clone(volume_manifest),
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

    let dev_name = format!("profile-{base_name}-{run}").replace('/', "-");
    let mut server = UblkServer::new();
    let dev = match server.add_device(&dev_name, Arc::clone(&handler)).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "boot profiling: ublk add_device failed; skipping");
            return None;
        }
    };
    tracing::info!(dev = %dev.display(), run, ?argv, "boot profiling: serving image, running entrypoint");

    // Run the entrypoint through the isolation sandbox (blocking) while ublk
    // serves I/O. The sandbox mounts the device, runs the entrypoint under a hard
    // timeout + cgroup limits, then reads the static-seed closure under the tracer.
    let spec = SandboxSpec {
        device: dev.clone(),
        fs_type: fs_type.to_string(),
        argv: argv.to_vec(),
        env: env.to_vec(),
        workdir: workdir.to_string(),
        timeout: opts.timeout,
        limits: opts.limits.clone(),
        static_seed: opts.static_seed.clone(),
    };
    let sandbox = Arc::clone(&opts.sandbox);
    let ran = tokio::task::spawn_blocking(move || sandbox.run(&spec)).await;

    // ALWAYS tear the device down (before processing the trace).
    server.remove_device(&dev_name).await.ok();
    tracer.finish();

    match ran {
        Ok(Ok(outcome)) => tracing::info!(?outcome, run, "boot profiling: run finished"),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, run, "boot profiling: sandbox run failed");
            return None;
        }
        Err(e) => {
            tracing::warn!("boot profiling task panicked: {e}");
            return None;
        }
    }

    let bytes = std::fs::read(&rtrace).ok()?;
    let blocks = boot_set_from_trace(&bytes, opts.max_blocks);
    if blocks.is_empty() {
        return None;
    }
    Some(blocks)
    }
    .await;

    // Close the foyer clean cache (flush + release device fds) BEFORE `tmp` is
    // dropped, so the TempDir cleanup actually frees the region files instead
    // of orphaning them. Runs on every exit path (including the early `None`s
    // above) because the work above is wrapped in the `result` async block.
    clean_for_close.close().await;

    result
}

#[cfg(test)]
mod tests {
    use super::rank_merge;

    #[test]
    fn rank_merge_single_run_preserves_order() {
        let runs = vec![vec![5, 1, 9, 3]];
        assert_eq!(rank_merge(&runs), vec![5, 1, 9, 3]);
    }

    #[test]
    fn rank_merge_prefers_frequency_then_earliest() {
        // 7 in both runs (count 2) → first. 1 in both → next. Singletons after,
        // ordered by best rank then index.
        let runs = vec![vec![7, 1, 2], vec![7, 1, 8]];
        let merged = rank_merge(&runs);
        assert_eq!(&merged[..2], &[7, 1]);
        assert!(merged.contains(&2) && merged.contains(&8));
        assert_eq!(merged.len(), 4);
    }

    #[test]
    fn rank_merge_unions_all_blocks() {
        let runs = vec![vec![1, 2], vec![3, 4], vec![2, 5]];
        let mut merged = rank_merge(&runs);
        merged.sort_unstable();
        assert_eq!(merged, vec![1, 2, 3, 4, 5]);
    }
}
