#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
use crate::block::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
use crate::block::content_store::ContentStore;
use crate::block::handler::BlockHandler;
use crate::block::metrics::ExportMetrics;
use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
use crate::block::pack_index_cache::PackIndexCache;
use crate::block::volume_manifest::VolumeManifest;
use crate::block::write_cache::{WriteCache, WriteCacheConfig};
use crate::config::Settings;
use crate::oci::ext4_store::{deterministic_uuid, store_ext4_stream, BLOCK_SIZE};
use crate::oci::ingest::IngestOptions;
use crate::oci::layer_store::{
    ensure_layer_stored, put_image_descriptor, ImageDescriptor,
};
use crate::oci::pull::{pull_image, pull_layer_to_tempfile};
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use ext4::writer::WriterOption;
use oci_registry::{Credentials, RegistryClient};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;
use tokio::sync::Notify;
use tracing::info;

/// A disk-backed scratch directory for bless. The WriteCache cache file, the
/// two-pass probe image, and the EROFS content spool can each be ~image-sized,
/// so they must NOT land on a tmpfs `/tmp` (RAM). Prefers `$GLIDEFS_BLESS_WORKDIR`,
/// then `/var/tmp` (FHS persistent temp, normally real disk) if writable, else
/// falls back to the system temp dir.
fn bless_scratch_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("GLIDEFS_BLESS_WORKDIR") {
        return PathBuf::from(d);
    }
    let var_tmp = std::path::Path::new("/var/tmp");
    if var_tmp.is_dir() && tempfile::tempfile_in(var_tmp).is_ok() {
        return var_tmp.to_path_buf();
    }
    std::env::temp_dir()
}

pub async fn run_bless(
    image_path: PathBuf,
    name: String,
    s3_prefix: String,
    config_path: PathBuf,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let start = Instant::now();

    // --- Setup ---
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = path_from_url.to_string();

    let base = format!("{}/exports/{}", db_path, s3_prefix);
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), &base));

    info!(image = %image_path.display(), name = %name, "starting bless");

    // --- Read image ---
    let file = std::fs::File::open(&image_path)
        .with_context(|| format!("Failed to open image {}", image_path.display()))?;
    let device_size = file.metadata()?.len();
    let total_blocks = device_size.div_ceil(u64::from(BLOCK_SIZE)) as usize;

    info!(device_size, total_blocks, "reading image");

    // --- Stream image: read blocks, upload each chunk as it completes ---
    let (volume_manifest, stats) =
        store_ext4_stream(&content_store, file, device_size, crate::block::block_map::COMPRESSION_BLESS).await?;

    // --- Upload manifest as a base ---
    let manifest_key = format!("bases/{}", name);
    content_store
        .put_manifest(&manifest_key, volume_manifest.serialize()?, None)
        .await
        .context("Failed to upload manifest")?;

    let elapsed = start.elapsed();

    info!(
        name = %name,
        total_blocks,
        zero_blocks = stats.zero_blocks,
        unique_blocks = stats.unique_blocks,
        packs_uploaded = stats.packs_uploaded,
        bytes_uploaded = stats.bytes_uploaded,
        chunks_written = stats.chunks_written,
        elapsed_secs = elapsed.as_secs_f64(),
        "bless complete"
    );

    println!("Blessed '{}' successfully:", name);
    println!("  Image size:      {:.1} GB", device_size as f64 / 1e9);
    println!("  Total blocks:    {}", total_blocks);
    println!("  Zero blocks:     {} (skipped)", stats.zero_blocks);
    println!("  Unique blocks:   {} (uploaded)", stats.unique_blocks);
    println!("  Packs uploaded:  {}", stats.packs_uploaded);
    println!(
        "  Bytes uploaded:  {:.1} MB",
        stats.bytes_uploaded as f64 / 1e6
    );
    println!("  Chunks written:  {}", stats.chunks_written);
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());
    println!("  Manifest:        manifests/{}", manifest_key);

    Ok(())
}

/// Convert a captured read trace into a base's boot SET and upload it.
///
/// Closes the trace→boot-set→upload loop: the read tracer
/// (`GLIDEFS_READ_TRACE_DIR`) records what a real boot touches; this turns that
/// bounded set into the `.boot-set` artifact the server data-prefetches on fork
/// open. Idempotent — re-running overwrites the boot set.
pub async fn run_make_boot_set(
    trace: PathBuf,
    name: String,
    s3_prefix: String,
    max_blocks: usize,
    config_path: PathBuf,
) -> Result<()> {
    use crate::block::manifest::serialize_block_list;
    use crate::block::write_trace::{boot_set_from_trace, read_header};

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let trace_bytes = std::fs::read(&trace)
        .with_context(|| format!("read trace {}", trace.display()))?;
    let header = read_header(&trace_bytes)
        .ok_or_else(|| anyhow::anyhow!("not a valid GLIDETRC trace: {}", trace.display()))?;
    // The trace's block size must match the volume's (128 KiB) so block indices
    // line up; refuse a mismatched trace rather than upload a wrong boot set.
    anyhow::ensure!(
        header.block_size == BLOCK_SIZE,
        "trace block_size {} != volume block_size {BLOCK_SIZE}; capture the trace on a {BLOCK_SIZE}-byte-block export",
        header.block_size,
    );

    let boot_set = boot_set_from_trace(&trace_bytes, max_blocks);
    anyhow::ensure!(
        !boot_set.is_empty(),
        "trace contains no read ops — nothing to prefetch (was the workload actually exercised?)"
    );

    // S3 setup (same shape as run_bless).
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &settings.storage.url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let base = format!("{}/exports/{}", path_from_url, s3_prefix);
    let content_store = ContentStore::new(Arc::clone(&object_store), &base);

    content_store
        .put_boot_set(&name, serialize_block_list(&boot_set))
        .await
        .map_err(|e| anyhow::anyhow!("failed to upload boot set: {e}"))?;

    println!("Uploaded boot set for '{}':", name);
    println!("  Trace:           {}", trace.display());
    println!("  Boot-set blocks: {} ({:.1} MiB)", boot_set.len(), boot_set.len() as f64 * BLOCK_SIZE as f64 / (1024.0 * 1024.0));
    println!("  Artifact:        manifests/bases/{}.boot-set", name);
    Ok(())
}

/// The container's runnable command (Entrypoint ++ Cmd) from its OCI config
/// blob, used to exercise the image during auto-profiling. None if the config
/// has neither (nothing meaningful to boot).
fn oci_run_command(config: &[u8]) -> Option<Vec<String>> {
    let v: serde_json::Value = serde_json::from_slice(config).ok()?;
    let cfg = v.get("config")?;
    let mut cmd = Vec::new();
    let strs = |k: &str| -> Vec<String> {
        cfg.get(k)
            .and_then(|x| x.as_array())
            .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
            .unwrap_or_default()
    };
    cmd.extend(strs("Entrypoint"));
    cmd.extend(strs("Cmd"));
    (!cmd.is_empty()).then_some(cmd)
}

/// The image's environment (`config.Env`, `KEY=val` strings) — notably `PATH`,
/// without which a relative entrypoint like `python3` won't exec in the chroot.
fn oci_env(config: &[u8]) -> Vec<String> {
    serde_json::from_slice::<serde_json::Value>(config)
        .ok()
        .and_then(|v| {
            v.get("config")?.get("Env")?.as_array().map(|a| {
                a.iter().filter_map(|s| s.as_str().map(String::from)).collect()
            })
        })
        .unwrap_or_default()
}

/// Profile by FILE PATH (for two-pass priority layout): loop-mount the built
/// image, run `run_cmd` once in a chroot under `strace -e openat`, and return the
/// in-image paths it opened, in first-touch order. These feed
/// `WriterOption::PriorityOrder` so pass 2 lays the boot working set contiguously
/// at the front. Best-effort: empty on any failure (no root, no strace, mount or
/// run fails) → pass 2 just falls back to natural order.
async fn profile_file_paths(
    image_path: &std::path::Path,
    fs_type: &str,
    run_cmd: &[String],
    env: &[String],
    timeout_secs: u32,
) -> Vec<String> {
    let image = image_path.to_path_buf();
    let fs_type = fs_type.to_string();
    let run_cmd = run_cmd.to_vec();
    let env = env.to_vec();
    tokio::task::spawn_blocking(move || -> Vec<String> {
        use std::process::Command;
        let Ok(mnt) = tempfile::TempDir::new() else { return vec![] };
        let mnt = mnt.path();
        let strace_out = mnt.parent().unwrap_or(mnt).join("glidefs-prof.strace");
        let _ = std::fs::remove_file(&strace_out);

        let mounted = Command::new("mount")
            .args(["-t", &fs_type, "-o", "ro,loop"])
            .arg(&image)
            .arg(mnt)
            .status();
        if !matches!(mounted, Ok(s) if s.success()) {
            return vec![]; // not root / loop unsupported / bad image
        }
        let _ = Command::new("mount").args(["--bind", "/proc"]).arg(mnt.join("proc")).status();
        let _ = Command::new("mount").args(["--bind", "/dev"]).arg(mnt.join("dev")).status();

        // strace -f -e openat -o OUT  timeout -sKILL N  chroot MNT  <cmd...>
        let mut c = Command::new("strace");
        c.args(["-f", "-e", "trace=openat", "-o"]).arg(&strace_out);
        c.arg("timeout").args(["--signal=KILL", &timeout_secs.to_string(), "chroot"]).arg(mnt);
        c.args(&run_cmd);
        c.env_clear();
        for kv in &env {
            if let Some((k, v)) = kv.split_once('=') {
                c.env(k, v);
            }
        }
        let _ = c.status(); // non-zero / killed is fine — we want the opens

        // Canonicalize the opened paths to real in-image paths WHILE mounted
        // (resolves `..` and symlinks so they match the writer's path table).
        let mut seen = std::collections::HashSet::new();
        let mut paths = Vec::new();
        if let Ok(trace) = std::fs::read_to_string(&strace_out) {
            for line in trace.lines() {
                let Some(p) = parse_openat_path(line) else { continue };
                if p.starts_with("/proc") || p.starts_with("/dev") || p.starts_with("/sys") {
                    continue;
                }
                let host = mnt.join(p.trim_start_matches('/'));
                let Ok(real) = std::fs::canonicalize(&host) else { continue };
                let Ok(rel) = real.strip_prefix(mnt) else { continue };
                if rel.as_os_str().is_empty() || !real.is_file() {
                    continue;
                }
                let rel = rel.to_string_lossy().into_owned();
                if seen.insert(rel.clone()) {
                    paths.push(rel);
                }
            }
        }
        let _ = Command::new("umount").arg(mnt.join("dev")).status();
        let _ = Command::new("umount").arg(mnt.join("proc")).status();
        let _ = Command::new("umount").arg(mnt).status();
        let _ = std::fs::remove_file(&strace_out);
        paths
    })
    .await
    .unwrap_or_default()
}

/// Extract the path from a successful `openat(..., "PATH", ...) = <fd>=0` strace
/// line. `None` for non-openat lines or failed opens (`= -1 ...`).
fn parse_openat_path(line: &str) -> Option<&str> {
    let rest = line.split_once("openat(")?.1;
    let q1 = rest.find('"')?;
    let after = &rest[q1 + 1..];
    let q2 = after.find('"')?;
    let path = &after[..q2];
    // success = the line ends with `= <non-negative number>` (fd), not `= -1`.
    let ret = line.rsplit_once('=')?.1.trim();
    let ok = ret.split_whitespace().next().and_then(|n| n.parse::<i64>().ok()).is_some_and(|n| n >= 0);
    ok.then_some(path)
}

/// Auto-profile a freshly-blessed base: serve it through GlideFS over a ublk
/// device, kernel-mount it (`fs_type`), run `run_cmd` once in a chroot while a
/// read tracer records the blocks the kernel actually fetches, and turn that
/// into a bounded boot set. Format-agnostic (ext4 or erofs — the kernel mounts
/// either; the tracer sees block reads regardless).
///
/// Best-effort: returns `None` (with a warning) if anything is missing (no root,
/// no ublk, mount/run fails). The base is fully valid without a boot set — this
/// only adds warm-on-open. Requires the `ublk` feature; without it, a no-op.
#[cfg(feature = "ublk")]
#[allow(clippy::too_many_arguments)]
async fn profile_boot_set(
    content_store: Arc<ContentStore>,
    volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,
    pack_index_cache: Arc<PackIndexCache>,
    device_size: u64,
    fs_type: &str,
    run_cmd: &[String],
    env: &[String],
    base_name: &str,
    max_blocks: usize,
) -> Option<Vec<u64>> {
    use crate::block::ublk::UblkServer;
    use crate::block::write_trace::{boot_set_from_trace, WriteTracer};

    if !std::path::Path::new("/dev/ublk-control").exists() {
        info!("auto-profile: /dev/ublk-control absent (need root + ublk) — skipping boot set");
        return None;
    }
    let tmp = tempfile::TempDir::new().ok()?;
    let rtrace_path = tmp.path().join("boot.rtrace");
    let tracer = Arc::new(
        WriteTracer::new(&rtrace_path, BLOCK_SIZE, device_size / u64::from(BLOCK_SIZE), base_name).ok()?,
    );

    // Fresh, cold, read-only serving handler over the just-drained store: reads
    // resolve through the manifest → packs exactly as a real fork would.
    let cache = Arc::new(
        WriteCache::open_fresh_active(WriteCacheConfig {
            cache_dir: tmp.path().to_path_buf(),
            device_name: format!("profile-{base_name}"),
            device_size,
            block_size: BLOCK_SIZE as usize,
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
    let handler = Arc::new(
        BlockHandler::new(
            cache,
            content_store,
            clean,
            pack_index_cache,
            volume_manifest,
            device_size,
            true,
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
            info!(error = %e, "auto-profile: ublk add_device failed — skipping boot set");
            return None;
        }
    };

    // Mount + run in a blocking section while ublk serves I/O in the background.
    let fs_type = fs_type.to_string();
    let run_cmd = run_cmd.to_vec();
    let env = env.to_vec();
    let mnt = tempfile::TempDir::new().ok()?;
    let mnt_path = mnt.path().to_path_buf();
    let ran = tokio::task::spawn_blocking(move || {
        use std::process::Command;
        let mnt = &mnt_path;
        let m = Command::new("mount")
            .args(["-t", &fs_type, "-o", "ro"])
            .arg(&dev)
            .arg(mnt)
            .status();
        if !matches!(m, Ok(s) if s.success()) {
            return Err(format!("mount -t {fs_type} {} failed: {m:?}", dev.display()));
        }
        // Best-effort pseudo-filesystems so the entrypoint can start.
        let _ = Command::new("mount").args(["--bind", "/proc"]).arg(mnt.join("proc")).status();
        let _ = Command::new("mount").args(["--bind", "/dev"]).arg(mnt.join("dev")).status();
        // Run the workload once under a hard timeout (long-running servers are
        // killed after their startup reads — that IS the boot working set). Use
        // the image's own environment (PATH etc.) so a relative entrypoint execs.
        let mut chroot = Command::new("timeout");
        chroot.args(["--signal=KILL", "12", "chroot"]).arg(mnt);
        chroot.args(&run_cmd);
        chroot.env_clear();
        for kv in &env {
            if let Some((k, v)) = kv.split_once('=') {
                chroot.env(k, v);
            }
        }
        let _ = chroot.status(); // failures/non-zero are fine; we want the reads
        let _ = Command::new("umount").arg(mnt.join("dev")).status();
        let _ = Command::new("umount").arg(mnt.join("proc")).status();
        let _ = Command::new("umount").arg(mnt).status();
        Ok(())
    })
    .await;

    // ALWAYS tear down the ublk device, even if the blocking task panicked —
    // otherwise the device (and any mount) leaks.
    server.remove_device(&dev_name).await.ok();
    tracer.finish();

    match ran {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            info!("auto-profile: {e} — skipping boot set");
            return None;
        }
        Err(e) => {
            info!("auto-profile: profiling task panicked ({e}) — skipping boot set");
            return None;
        }
    }
    let bytes = std::fs::read(&rtrace_path).ok()?;
    let boot_set = boot_set_from_trace(&bytes, max_blocks);
    if boot_set.is_empty() {
        info!("auto-profile: workload read nothing — skipping boot set");
        return None;
    }
    Some(boot_set)
}

#[cfg(not(feature = "ublk"))]
#[allow(clippy::too_many_arguments)]
async fn profile_boot_set(
    _content_store: Arc<ContentStore>,
    _volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,
    _pack_index_cache: Arc<PackIndexCache>,
    _device_size: u64,
    _fs_type: &str,
    _run_cmd: &[String],
    _env: &[String],
    _base_name: &str,
    _max_blocks: usize,
) -> Option<Vec<u64>> {
    info!("auto-profile: built without the `ublk` feature — skipping boot set");
    None
}

/// Bless an OCI image into a content-addressed base image.
///
/// Pulls layers from the registry, converts to ext4, writes through
/// BlockHandler → WriteCache, drains to S3, and saves the manifest as a base.
/// (Boot prefetch is driven by the manifest's packs + a runtime boot set, so no
/// sidecar is written here.)
pub async fn run_bless_oci(
    image_ref: String,
    name: String,
    s3_prefix: String,
    profile: bool,
    config_path: PathBuf,
) -> Result<()> {
    use crate::block::manifest::serialize_block_list;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let start = Instant::now();

    // --- S3 setup (same as run_bless) ---
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = path_from_url.to_string();

    let base = format!("{}/exports/{}", db_path, s3_prefix);
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), &base));

    // --- Resolve OCI image to get layer sizes for device size estimation ---
    let registry_client = RegistryClient::new();
    let image: oci_registry::Reference = image_ref
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid image reference: {e}"))?;

    info!(image = %image_ref, name = %name, "resolving OCI image");

    let resolved = registry_client
        .resolve(&image, &Credentials::Anonymous)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve image: {e}"))?;

    // Estimate device size: sum compressed layer sizes × 4 (decompression + ext4
    // overhead + block-grid alignment headroom). Round up to next power-of-2.
    // Minimum 64 MiB. The ×4 (vs ×3) covers the logical inflation from aligning
    // large files to the dedup block grid; that padding is holes/zeros which the
    // block store drops, so it costs address space, not stored bytes.
    let total_compressed: u64 = resolved.layers.iter().map(|l| l.size as u64).sum();
    let estimated = (total_compressed * 4).max(64 * 1024 * 1024);
    let device_size = estimated.next_power_of_two();

    info!(
        layers = resolved.layers.len(),
        total_compressed,
        device_size,
        "estimated device size"
    );

    // --- Create temporary export infrastructure (disk-backed scratch so the
    // WriteCache cache file doesn't balloon RAM via tmpfs /tmp). ---
    let temp_dir = tempfile::TempDir::new_in(bless_scratch_dir()).context("failed to create temp dir")?;
    let cache_config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: format!("bless-oci-{}", name),
        device_size,
        block_size: BLOCK_SIZE as usize,
        wal_sync: false,
    };

    let cache = Arc::new(WriteCache::open_fresh_active(cache_config)?);
    // Bless is offline + write-once/read-many: use the highest zstd level.
    cache.set_compression_level(crate::block::block_map::COMPRESSION_BLESS);

    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        device_size, BLOCK_SIZE,
    )));

    let pack_index_cache = Arc::new(
        PackIndexCache::open(temp_dir.path())
            .await
            .context("failed to open pack index cache")?,
    );

    // Minimal clean cache — OCI ingest is write-only, no reads from S3.
    let foyer_dir = temp_dir.path().join("foyer");
    std::fs::create_dir_all(&foyer_dir)?;
    let clean_cache: Arc<dyn BlockCache> = Arc::new(
        FoyerBlockCache::open(FoyerCacheConfig {
            memory_bytes: 4 * 1024 * 1024,
            ssd_bytes: 16 * 1024 * 1024,
            ssd_dir: foyer_dir,
            direct: false, // ephemeral CLI cache; buffered is fine
            io_uring: false, // ephemeral CLI cache; psync avoids the idle-spin
        })
        .await
        .context("failed to open block cache")?,
    );

    let metrics = Arc::new(ExportMetrics::new());
    let flush_notify = Arc::new(Notify::const_new());

    let handler = Arc::new(BlockHandler::new(
        Arc::clone(&cache),
        Arc::clone(&content_store),
        Arc::clone(&clean_cache),
        Arc::clone(&pack_index_cache),
        Arc::clone(&volume_manifest),
        device_size,
        false,
        metrics,
        Arc::new(AtomicU64::new(0f64.to_bits())),
        flush_notify,
        DEFAULT_FLUSH_THRESHOLD,
        None,
    ));

    // --- Pull + ingest OCI image ---
    // Derive the filesystem UUID deterministically from the resolved manifest
    // digest so that blessing the same image (same content-addressed manifest)
    // produces a byte-for-byte identical ext4 image every time. The UUID feeds
    // the superblock and the directory hash seed, so a random UUID would make
    // the whole pipeline non-reproducible.
    let uuid = deterministic_uuid(&resolved.manifest_digest);
    let ingest_opts = IngestOptions {
        writer_options: vec![
            WriterOption::MaximumDiskSize(device_size as i64),
            WriterOption::Uuid(uuid),
            WriterOption::Journal(1024), // 4 MiB journal
            // Align large file payloads to the dedup block grid (the volume's
            // 128 KiB block size) so the same file produces the same blocks
            // across images and the host's content-addressed cache + S3 packs
            // dedup it. Only files >= one full block are aligned, bounding the
            // padding. See dedup_probe / fsck_validity for the validation.
            WriterOption::AlignData { align: BLOCK_SIZE, min_size: BLOCK_SIZE },
        ],
    };

    info!("pulling and ingesting layers");

    pull_image(
        &registry_client,
        &image,
        &Credentials::Anonymous,
        Arc::clone(&handler),
        ingest_opts,
    )
    .await
    .map_err(|e| anyhow::anyhow!("pull failed: {e}"))?;

    // --- Drain to S3 ---
    info!("draining to S3");

    let max_drain_iterations = 100;
    let mut drained = false;
    for i in 0..max_drain_iterations {
        let stats = cache
            .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
            .await
            .map_err(|e| anyhow::anyhow!("flush failed: {e}"))?;
        if stats.blocks_claimed == 0 {
            info!(iterations = i + 1, "drain complete");
            drained = true;
            break;
        }
    }
    if !drained {
        anyhow::bail!(
            "drain did not converge after {max_drain_iterations} iterations — \
             cache still has dirty blocks, refusing to upload incomplete manifest"
        );
    }

    // --- Save manifest as base. Index warming on fork comes from the manifest's
    // pack list; the boot working set is captured at runtime (read trace → boot
    // set), so no `.hot-set` artifact is written. ---
    let manifest_key = format!("bases/{}", name);
    let manifest_data = volume_manifest.read().serialize()?;
    content_store
        .put_manifest(&manifest_key, manifest_data, None)
        .await
        .map_err(|e| anyhow::anyhow!("failed to upload manifest: {e}"))?;

    // --- Auto-profile: boot the ext4 rootfs once, capture its reads → boot set. ---
    let mut boot_set_blocks = 0usize;
    if profile {
        match oci_run_command(&resolved.config) {
            Some(cmd) => {
                info!(?cmd, "auto-profiling boot set");
                if let Some(bs) = profile_boot_set(
                    Arc::clone(&content_store),
                    Arc::clone(&volume_manifest),
                    Arc::clone(&pack_index_cache),
                    device_size,
                    "ext4",
                    &cmd,
                    &oci_env(&resolved.config),
                    &name,
                    4096,
                )
                .await
                {
                    boot_set_blocks = bs.len();
                    if let Err(e) =
                        content_store.put_boot_set(&name, serialize_block_list(&bs)).await
                    {
                        info!("auto-profile: boot set upload failed: {e}");
                        boot_set_blocks = 0;
                    }
                }
            }
            None => info!("auto-profile: image has no entrypoint/cmd — skipping boot set"),
        }
    }

    let elapsed = start.elapsed();

    println!("Blessed '{}' from OCI image successfully:", name);
    println!("  Image:           {}", image_ref);
    println!("  Layers:          {}", resolved.layers.len());
    println!("  Device size:     {:.1} MB", device_size as f64 / 1e6);
    println!("  Boot set:        {} blocks (auto-profiled; 0 = skipped/unavailable)", boot_set_blocks);
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());
    println!("  Manifest:        manifests/{}", manifest_key);

    Ok(())
}

/// Bless an OCI image into a **read-only EROFS** base — the correct format for
/// an immutable container/OCI rootfs served daemonless (kernel `erofs` over
/// ublk; the guest's writes go to an overlay upper, never into the image).
///
/// Same pipeline as [`run_bless_oci`] (pull → write into a volume → drain → hot
/// set + manifest), but it merges the layers into a deterministic, grid-aligned
/// EROFS image (via the hand-rolled writer) instead of ext4. The image is
/// content-addressed and dedups across images; the consumer mounts it read-only.
pub async fn run_bless_oci_erofs(
    image_ref: String,
    name: String,
    s3_prefix: String,
    profile: bool,
    config_path: PathBuf,
) -> Result<()> {
    use crate::block::manifest::serialize_block_list;
    use crate::oci::BlockAdapter;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let start = Instant::now();

    // --- S3 setup ---
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;
    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = path_from_url.to_string();
    let base = format!("{}/exports/{}", db_path, s3_prefix);
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), &base));

    // --- Resolve image + estimate device size (same headroom as ext4 path) ---
    let registry_client = RegistryClient::new();
    let image: oci_registry::Reference = image_ref
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid image reference: {e}"))?;
    info!(image = %image_ref, name = %name, "resolving OCI image (erofs)");
    let resolved = registry_client
        .resolve(&image, &Credentials::Anonymous)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve image: {e}"))?;

    let total_compressed: u64 = resolved.layers.iter().map(|l| l.size as u64).sum();
    let device_size = (total_compressed * 4).max(64 * 1024 * 1024).next_power_of_two();

    // --- Pull every layer to a decompressed temp file (the EROFS merge needs
    // all layers seekable up front, bottom-to-top). ---
    info!(layers = resolved.layers.len(), "pulling layers");
    let mut layer_files: Vec<std::fs::File> = Vec::with_capacity(resolved.layers.len());
    for (i, layer) in resolved.layers.iter().enumerate() {
        info!(layer = i, digest = %layer.digest, "pulling layer");
        layer_files.push(
            pull_layer_to_tempfile(&registry_client, &image, layer, &Credentials::Anonymous)
                .await
                .with_context(|| format!("pull layer {}", layer.digest))?,
        );
    }

    // --- Volume infrastructure (mirrors run_bless_oci). All bless scratch (the
    // WriteCache cache file, the probe image, the EROFS spool) goes on a
    // disk-backed dir so big images don't balloon RAM via tmpfs /tmp. ---
    let scratch = bless_scratch_dir();
    let temp_dir = tempfile::TempDir::new_in(&scratch).context("failed to create temp dir")?;
    let cache = Arc::new(WriteCache::open_fresh_active(WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: format!("bless-erofs-{}", name),
        device_size,
        block_size: BLOCK_SIZE as usize,
        wal_sync: false,
    })?);
    cache.set_compression_level(crate::block::block_map::COMPRESSION_BLESS);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        device_size, BLOCK_SIZE,
    )));
    let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await?);
    let foyer_dir = temp_dir.path().join("foyer");
    std::fs::create_dir_all(&foyer_dir)?;
    let clean_cache: Arc<dyn BlockCache> = Arc::new(
        FoyerBlockCache::open(FoyerCacheConfig {
            memory_bytes: 4 * 1024 * 1024,
            ssd_bytes: 16 * 1024 * 1024,
            ssd_dir: foyer_dir,
            direct: false,
            io_uring: false,
        })
        .await?,
    );
    let handler = Arc::new(BlockHandler::new(
        Arc::clone(&cache),
        Arc::clone(&content_store),
        Arc::clone(&clean_cache),
        Arc::clone(&pack_index_cache),
        Arc::clone(&volume_manifest),
        device_size,
        false,
        Arc::new(ExportMetrics::new()),
        Arc::new(AtomicU64::new(0f64.to_bits())),
        Arc::new(Notify::const_new()),
        DEFAULT_FLUSH_THRESHOLD,
        None,
    ));

    // --- TWO-PASS (when --profile): build a throwaway probe image, boot it to
    // learn which files the workload actually reads, then rebuild with those
    // ordered FIRST (`PriorityOrder`) so the boot working set is one contiguous,
    // coalesce-friendly run at the front. Otherwise a single pass. The
    // BlockAdapter sink uses block_on, so the final convert runs on a blocking
    // thread. `_ = serialize_block_list` (kept for make-boot-set parity). ---
    let _ = serialize_block_list;
    let uuid = deterministic_uuid(&resolved.manifest_digest);
    let run_cmd = if profile { oci_run_command(&resolved.config) } else { None };
    let env = oci_env(&resolved.config);

    let priority: Vec<String> = if let Some(cmd) = run_cmd {
        info!("two-pass: building probe image (pass 1)");
        let scratch1 = scratch.clone();
        let (probe, layers_back) = tokio::task::spawn_blocking(
            move || -> Result<(tempfile::NamedTempFile, Vec<std::fs::File>)> {
                let opts = ext4::tar_convert::ConvertOptions {
                    convert_backslash: false,
                    writer_options: vec![
                        WriterOption::Uuid(uuid),
                        WriterOption::AlignData { align: BLOCK_SIZE, min_size: BLOCK_SIZE },
                        WriterOption::SpoolDir(scratch1.clone()),
                    ],
                };
                let tmp = tempfile::NamedTempFile::new_in(&scratch1).context("probe tempfile")?;
                ext4::convert_oci_layers_to_erofs(&mut layer_files, tmp.reopen()?, &opts)
                    .map_err(|e| anyhow::anyhow!("probe convert failed: {e}"))?;
                Ok((tmp, layer_files))
            },
        )
        .await??;
        layer_files = layers_back;
        info!(?cmd, "two-pass: booting probe to capture hot files");
        let paths = profile_file_paths(probe.path(), "erofs", &cmd, &env, 12).await;
        info!(files = paths.len(), "two-pass: captured priority files");
        paths // probe NamedTempFile is dropped (deleted) here
    } else {
        Vec::new()
    };
    let priority_count = priority.len();

    let rt = tokio::runtime::Handle::current();
    let handler_for_write = Arc::clone(&handler);
    let scratch2 = scratch.clone();
    info!("merging layers into EROFS (final, pass 2)");
    let prefetch_len = tokio::task::spawn_blocking(move || -> Result<u64> {
        let mut writer_options = vec![
            WriterOption::Uuid(uuid),
            // Align large file payloads to the dedup block grid (cross-image
            // dedup); EROFS has no reserved blocks so this is always safe.
            WriterOption::AlignData { align: BLOCK_SIZE, min_size: BLOCK_SIZE },
            WriterOption::SpoolDir(scratch2.clone()),
        ];
        if !priority.is_empty() {
            writer_options.push(WriterOption::PriorityOrder(priority));
        }
        let opts = ext4::tar_convert::ConvertOptions { convert_backslash: false, writer_options };
        let (_sink, prefetch_len) = ext4::convert_oci_layers_to_erofs_with_prefetch(
            &mut layer_files,
            BlockAdapter::new(&handler_for_write, rt),
            &opts,
        )
        .map_err(|e| anyhow::anyhow!("erofs conversion failed: {e}"))?;
        Ok(prefetch_len)
    })
    .await??;
    // With a priority list, prefetch_len is the contiguous boot region the server
    // warms on open; 0 otherwise (no profile, or profiling found nothing).
    handler.set_prefetch_len(prefetch_len);

    // --- Drain to S3 ---
    info!("draining to S3");
    for i in 0..100 {
        let stats = cache
            .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
            .await
            .map_err(|e| anyhow::anyhow!("flush failed: {e}"))?;
        if stats.blocks_claimed == 0 {
            info!(iterations = i + 1, "drain complete");
            break;
        }
        if i == 99 {
            anyhow::bail!("drain did not converge");
        }
    }

    // --- Save manifest as base ---
    let manifest_key = format!("bases/{}", name);
    content_store
        .put_manifest(&manifest_key, volume_manifest.read().serialize()?, None)
        .await
        .map_err(|e| anyhow::anyhow!("failed to upload manifest: {e}"))?;

    println!("Blessed '{}' from OCI image as read-only EROFS:", name);
    println!(
        "  Priority files:  {priority_count} (two-pass; 0 = no --profile or profiling unavailable)"
    );
    println!("  Image:           {}", image_ref);
    println!("  Layers:          {}", resolved.layers.len());
    println!("  Device size:     {:.1} MB", device_size as f64 / 1e6);
    println!("  Prefetch extent: {:.1} MB (0 = no priority list at bless)", prefetch_len as f64 / 1e6);
    println!("  Elapsed:         {:.1}s", start.elapsed().as_secs_f64());
    println!("  Manifest:        manifests/{}  (mount read-only)", manifest_key);
    Ok(())
}

/// Bless an OCI image as **content-addressed layers** (layers survive).
///
/// Each layer is converted independently to a deterministic, overlay-preserving
/// ext4 and stored once under a global `layers/{digest}` namespace; the image is
/// recorded as an ordered list of layer digests under `images/{name}`. Two
/// images that share a layer share its storage — the dedup that flattening into
/// one merged ext4 cannot achieve.
pub async fn run_bless_oci_layered(
    image_ref: String,
    name: String,
    config_path: PathBuf,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let start = Instant::now();

    // --- S3 setup ---
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;
    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = path_from_url.to_string();

    // --- Resolve image ---
    let registry_client = RegistryClient::new();
    let image: oci_registry::Reference = image_ref
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid image reference: {e}"))?;

    info!(image = %image_ref, name = %name, "resolving OCI image (layered)");
    let resolved = registry_client
        .resolve(&image, &Credentials::Anonymous)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve image: {e}"))?;

    // --- Store each layer once (content-addressed) ---
    let mut layer_digests = Vec::with_capacity(resolved.layers.len());
    let mut layer_sizes = Vec::with_capacity(resolved.layers.len());
    let mut total_stored: u64 = 0;
    let mut reused = 0usize;

    for (i, layer) in resolved.layers.iter().enumerate() {
        info!(layer = i, digest = %layer.digest, size = layer.size, "ensuring layer");
        let decompressed =
            pull_layer_to_tempfile(&registry_client, &image, layer, &Credentials::Anonymous)
                .await
                .with_context(|| format!("pull layer {}", layer.digest))?;
        let stored = ensure_layer_stored(&object_store, &db_path, &layer.digest, decompressed)
            .await
            .with_context(|| format!("store layer {}", layer.digest))?;
        if stored.already_present {
            reused += 1;
        }
        total_stored += stored.stored_bytes;
        layer_digests.push(layer.digest.clone());
        layer_sizes.push(layer.size as u64);
    }

    // --- Record the image descriptor ---
    let descriptor = ImageDescriptor {
        image_ref: image_ref.clone(),
        config_digest: resolved.manifest.config.digest.clone(),
        layers: layer_digests,
        layer_sizes,
    };
    put_image_descriptor(&object_store, &db_path, &name, &descriptor)
        .await
        .context("write image descriptor")?;

    let elapsed = start.elapsed();
    println!("Blessed '{}' from OCI image (layered) successfully:", name);
    println!("  Image:           {}", image_ref);
    println!("  Layers:          {}", resolved.layers.len());
    println!("  Layers reused:   {} (already stored)", reused);
    println!("  Bytes uploaded:  {:.1} MB", total_stored as f64 / 1e6);
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());
    println!("  Descriptor:      images/{}", name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::block_map::{blake3_128, decompress_block};
    use crate::block::pack::{extract_block, lookup_block_in_index, parse_pack_index, PackId};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::ObjectStore;

    #[test]
    fn parse_openat_path_handles_success_and_failure() {
        assert_eq!(
            parse_openat_path(r#"openat(AT_FDCWD, "/lib/libc.so.6", O_RDONLY) = 3"#),
            Some("/lib/libc.so.6")
        );
        // failed open (ENOENT) → None
        assert_eq!(
            parse_openat_path(r#"openat(AT_FDCWD, "/nope", O_RDONLY) = -1 ENOENT (x)"#),
            None
        );
        // pid-prefixed, with flags after the path
        assert_eq!(
            parse_openat_path(r#"[pid 42] openat(AT_FDCWD, "/a/b", O_RDONLY|O_CLOEXEC) = 4"#),
            Some("/a/b")
        );
        assert_eq!(parse_openat_path("read(3, ...) = 10"), None);
    }

    /// Decompress a skopeo `dir:` image's layers to temp files (gzip/zstd/plain).
    fn load_oci_layers(dir: &std::path::Path) -> (Vec<std::fs::File>, Vec<u8>) {
        use std::io::{Read, Seek, SeekFrom};
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap();
        let layers = manifest["layers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| {
                let d = l["digest"].as_str().unwrap();
                let blob = dir.join(d.strip_prefix("sha256:").unwrap_or(d));
                let mut f = std::fs::File::open(&blob).unwrap();
                let mut magic = [0u8; 4];
                f.read_exact(&mut magic).unwrap();
                f.seek(SeekFrom::Start(0)).unwrap();
                let mut out = tempfile::tempfile().unwrap();
                if magic[0] == 0x1f && magic[1] == 0x8b {
                    std::io::copy(&mut flate2::read::GzDecoder::new(f), &mut out).unwrap();
                } else if magic == [0x28, 0xb5, 0x2f, 0xfd] {
                    std::io::copy(&mut zstd::Decoder::new(f).unwrap(), &mut out).unwrap();
                } else {
                    std::io::copy(&mut f, &mut out).unwrap();
                }
                out.seek(SeekFrom::Start(0)).unwrap();
                out
            })
            .collect();
        let config = std::fs::read(
            dir.join(manifest["config"]["digest"].as_str().unwrap().strip_prefix("sha256:").unwrap()),
        )
        .unwrap();
        (layers, config)
    }

    /// REAL smoke test of TWO-PASS profiling: build a python image as EROFS to a
    /// file, loop-mount it and run the real entrypoint under strace
    /// (`profile_file_paths`), then rebuild with `PriorityOrder(captured paths)`
    /// and confirm a non-trivial contiguous priority region results. Needs root +
    /// strace + /tmp/oci/py312.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_pass_profile_captures_paths_and_orders_them() {
        use std::io::{Seek, SeekFrom};
        // Effective UID == 0 via /proc (avoids an unsafe libc::geteuid call —
        // the crate denies unsafe). Uid line is: real effective saved fsuid.
        let is_root = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("Uid:"))
                    .and_then(|l| l.split_whitespace().nth(2).map(|u| u == "0"))
            })
            .unwrap_or(false);
        let img_dir = std::path::Path::new("/tmp/oci/py312");
        if !is_root || !img_dir.exists() {
            eprintln!("SKIP: two-pass profiling needs root + /tmp/oci/py312");
            return;
        }
        let (mut layers, config) = load_oci_layers(img_dir);
        let cmd = oci_run_command(&config).expect("entrypoint");
        let env = oci_env(&config);

        // Pass 1: build EROFS to a temp file.
        let probe = tokio::task::spawn_blocking(move || {
            let opts = ext4::tar_convert::ConvertOptions {
                convert_backslash: false,
                writer_options: vec![
                    WriterOption::Uuid([0u8; 16]),
                    WriterOption::AlignData { align: BLOCK_SIZE, min_size: BLOCK_SIZE },
                ],
            };
            let tmp = tempfile::NamedTempFile::new().unwrap();
            ext4::convert_oci_layers_to_erofs(&mut layers, tmp.reopen().unwrap(), &opts).unwrap();
            (tmp, layers)
        })
        .await
        .unwrap();
        let (probe_file, mut layers) = probe;

        // THE THING UNDER TEST: loop-mount + strace the real entrypoint.
        let paths = profile_file_paths(probe_file.path(), "erofs", &cmd, &env, 12).await;
        eprintln!("two-pass captured {} files; first few: {:?}", paths.len(), &paths[..paths.len().min(6)]);
        assert!(!paths.is_empty(), "profiling must capture opened files");
        assert!(
            paths.iter().any(|p| p.contains("python") || p.contains("libc") || p.contains("encodings")),
            "captured paths should include python/libc runtime files: {paths:?}"
        );

        // Pass 2: rebuild with those files prioritized; confirm a real priority
        // region (prefetch_len covers the captured hot files, not the whole image).
        for l in &mut layers {
            l.seek(SeekFrom::Start(0)).unwrap();
        }
        let prefetch_len = tokio::task::spawn_blocking(move || {
            let opts = ext4::tar_convert::ConvertOptions {
                convert_backslash: false,
                writer_options: vec![
                    WriterOption::Uuid([0u8; 16]),
                    WriterOption::AlignData { align: BLOCK_SIZE, min_size: BLOCK_SIZE },
                    WriterOption::PriorityOrder(paths),
                ],
            };
            let (cur, plen) = ext4::convert_oci_layers_to_erofs_with_prefetch(
                &mut layers,
                std::io::Cursor::new(Vec::new()),
                &opts,
            )
            .unwrap();
            (cur.into_inner().len() as u64, plen)
        })
        .await
        .unwrap();
        let (image_len, prefetch_len) = prefetch_len;
        eprintln!("two-pass: prefetch region {} MiB of {} MiB image", prefetch_len / 1048576, image_len / 1048576);
        assert!(prefetch_len > 0, "priority ordering must yield a non-zero prefetch region");
        assert!(prefetch_len < image_len, "prefetch region must be a prefix, not the whole image");
    }

    /// REAL smoke test of the auto-profiler: build a real python image as EROFS
    /// into a glidefs volume, then run `profile_boot_set` — which serves it over
    /// a ublk device, kernel-mounts it, chroots and runs python, and captures the
    /// blocks read — and assert it yields a sane, bounded boot set. This is the
    /// exact path `bless --oci --erofs` takes automatically. Needs root + ublk +
    /// a skopeo `dir:` python image at /tmp/oci/py312.
    #[cfg(feature = "ublk")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn auto_profile_produces_a_real_boot_set() {
        use crate::block::handler::BlockHandler;
        use crate::block::pack_index_cache::PackIndexCache;
        use crate::block::write_cache::{WriteCache, WriteCacheConfig};
        use crate::oci::BlockAdapter;
        use std::io::{Read, Seek, SeekFrom};
        use std::sync::atomic::AtomicU64;

        let img_dir = std::path::Path::new("/tmp/oci/py312");
        if !std::path::Path::new("/dev/ublk-control").exists() || !img_dir.exists() {
            eprintln!("SKIP: need /dev/ublk-control (root+ublk) and a skopeo dir at /tmp/oci/py312");
            return;
        }

        // Decompress the image's layers to temp files.
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(img_dir.join("manifest.json")).unwrap()).unwrap();
        let mut layers: Vec<std::fs::File> = manifest["layers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| {
                let d = l["digest"].as_str().unwrap();
                let blob = img_dir.join(d.strip_prefix("sha256:").unwrap_or(d));
                let mut f = std::fs::File::open(&blob).unwrap();
                let mut magic = [0u8; 4];
                f.read_exact(&mut magic).unwrap();
                f.seek(SeekFrom::Start(0)).unwrap();
                let mut out = tempfile::tempfile().unwrap();
                if magic[0] == 0x1f && magic[1] == 0x8b {
                    std::io::copy(&mut flate2::read::GzDecoder::new(f), &mut out).unwrap();
                } else if magic == [0x28, 0xb5, 0x2f, 0xfd] {
                    std::io::copy(&mut zstd::Decoder::new(f).unwrap(), &mut out).unwrap();
                } else {
                    std::io::copy(&mut f, &mut out).unwrap();
                }
                out.seek(SeekFrom::Start(0)).unwrap();
                out
            })
            .collect();

        // Build the EROFS into a glidefs volume (InMemory store), drain to S3.
        let store = Arc::new(InMemory::new());
        let cs = Arc::new(ContentStore::new(store.clone(), "test/exports/smoke"));
        let device_size = 512 * 1024 * 1024u64;
        let temp = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(
            WriteCache::open_fresh_active(WriteCacheConfig {
                cache_dir: temp.path().to_path_buf(),
                device_name: "smoke".into(),
                device_size,
                block_size: BLOCK_SIZE as usize,
                wal_sync: false,
            })
            .unwrap(),
        );
        let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(device_size, BLOCK_SIZE)));
        let pic = Arc::new(PackIndexCache::open(temp.path()).await.unwrap());
        let clean: Arc<dyn BlockCache> = Arc::new(
            FoyerBlockCache::open(FoyerCacheConfig {
                memory_bytes: 64 * 1024 * 1024,
                ssd_bytes: 256 * 1024 * 1024,
                ssd_dir: { let d = temp.path().join("f"); std::fs::create_dir_all(&d).unwrap(); d },
                direct: false,
                io_uring: false,
            })
            .await
            .unwrap(),
        );
        let handler = Arc::new(BlockHandler::new(
            Arc::clone(&cache), Arc::clone(&cs), Arc::clone(&clean), Arc::clone(&pic),
            Arc::clone(&vm), device_size, false, Arc::new(ExportMetrics::new()),
            Arc::new(AtomicU64::new(0f64.to_bits())), Arc::new(Notify::const_new()),
            DEFAULT_FLUSH_THRESHOLD, None,
        ));
        let rt = tokio::runtime::Handle::current();
        let hw = Arc::clone(&handler);
        tokio::task::spawn_blocking(move || {
            let opts = ext4::tar_convert::ConvertOptions {
                convert_backslash: false,
                writer_options: vec![
                    WriterOption::Uuid([0u8; 16]),
                    WriterOption::AlignData { align: BLOCK_SIZE, min_size: BLOCK_SIZE },
                ],
            };
            ext4::convert_oci_layers_to_erofs(&mut layers, BlockAdapter::new(&hw, rt), &opts).unwrap();
        })
        .await
        .unwrap();
        for _ in 0..100 {
            if cache.flush_to_s3(&cs, &pic, &vm).await.unwrap().blocks_claimed == 0 { break; }
        }

        // THE THING UNDER TEST: auto-profile using the image's REAL entrypoint
        // and env (exactly what `bless --oci` does), NOT a hardcoded command.
        let config = std::fs::read(img_dir.join(
            manifest["config"]["digest"].as_str().unwrap().strip_prefix("sha256:").unwrap(),
        ))
        .unwrap();
        let cmd = oci_run_command(&config).expect("image must expose an entrypoint/cmd");
        let env = oci_env(&config);
        eprintln!("real entrypoint: {cmd:?}  ({} env vars incl PATH)", env.len());
        let boot_set = profile_boot_set(
            Arc::clone(&cs), Arc::clone(&vm), Arc::clone(&pic),
            device_size, "erofs", &cmd, &env, "py312-smoke", 4096,
        )
        .await
        .expect("auto-profile should produce a boot set (root + ublk + real entrypoint)");

        eprintln!("auto-profiled boot set: {} blocks ({:.1} MiB)", boot_set.len(), boot_set.len() as f64 * BLOCK_SIZE as f64 / 1048576.0);
        assert!(!boot_set.is_empty(), "boot set must be non-empty");
        assert!(boot_set.len() < (device_size / u64::from(BLOCK_SIZE)) as usize / 2, "must be bounded, not the whole image");
        // The metadata region (low blocks) is always touched at mount/boot.
        assert!(boot_set.iter().any(|&b| b < 64), "boot set should include low/metadata blocks");
    }

    #[test]
    fn deterministic_uuid_is_stable_and_content_addressed() {
        let digest = "sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        // Same digest → same UUID, every time.
        assert_eq!(deterministic_uuid(digest), deterministic_uuid(digest));

        // Different digest → different UUID (no collision on a trivial change).
        let other = "sha256:00000000000000000000000000000000000000000000000000000000deadbeef";
        assert_ne!(deterministic_uuid(digest), deterministic_uuid(other));

        // Well-formed RFC 4122 v8 UUID: version nibble = 8, variant top bits = 10.
        let uuid = deterministic_uuid(digest);
        assert_eq!(uuid[6] & 0xf0, 0x80, "version must be 8");
        assert_eq!(uuid[8] & 0xc0, 0x80, "variant must be RFC 4122");

        // No randomness leaked in: the value is a pure function of the digest,
        // so it is reproducible across process runs (regression guard against
        // reintroducing rand::random()).
        assert_eq!(
            deterministic_uuid("sha256:abc"),
            deterministic_uuid("sha256:abc"),
        );
    }

    /// Helper: run the bless pipeline directly against an InMemory object store.
    async fn bless_bytes(
        content_store: &ContentStore,
        name: &str,
        image_data: &[u8],
    ) -> Result<crate::oci::ext4_store::StoreStats> {
        let device_size = image_data.len() as u64;
        let content_store = Arc::new(ContentStore::new(
            content_store.object_store().clone(),
            content_store.base_path(),
        ));

        let (volume_manifest, stats) =
            store_ext4_stream(&content_store, std::io::Cursor::new(image_data.to_vec()), device_size, crate::block::block_map::COMPRESSION_BLESS)
                .await?;

        content_store
            .put_manifest(&format!("bases/{}", name), volume_manifest.serialize()?, None)
            .await?;

        Ok(stats)
    }

    fn test_store() -> (Arc<InMemory>, ContentStore) {
        let store = Arc::new(InMemory::new());
        let cs = ContentStore::new(store.clone(), "test");
        (store, cs)
    }

    /// Fetch a full pack from the InMemory store and parse its index.
    async fn fetch_pack_index(
        store: &Arc<InMemory>,
        base_path: &str,
        chunk_idx: u32,
        pack_id: PackId,
    ) -> Vec<crate::block::pack::PackIndexEntry> {
        let key = format!("{}/chunks/{:04}/{:016x}.pack", base_path, chunk_idx, pack_id);
        let path = ObjectPath::from(key);
        let response = store.get(&path).await.unwrap();
        let bytes = response.bytes().await.unwrap();
        let index = parse_pack_index(&bytes).unwrap();
        index.entries
    }

    /// Fetch full pack bytes from the InMemory store.
    async fn fetch_pack_bytes(
        store: &Arc<InMemory>,
        base_path: &str,
        chunk_idx: u32,
        pack_id: PackId,
    ) -> Vec<u8> {
        let key = format!("{}/chunks/{:04}/{:016x}.pack", base_path, chunk_idx, pack_id);
        let path = ObjectPath::from(key);
        let response = store.get(&path).await.unwrap();
        response.bytes().await.unwrap().to_vec()
    }

    /// The EROFS-bless assembly (convert layers → write into the volume via
    /// BlockAdapter → drain to S3 → manifest) must produce a base that, read back
    /// cold from S3, is a valid EROFS image. Covers everything in
    /// `run_bless_oci_erofs` except the network pull.
    #[tokio::test]
    async fn test_bless_oci_erofs_assembly_reads_back() {
        use crate::block::handler::BlockHandler;
        use crate::block::pack_index_cache::PackIndexCache;
        use crate::block::write_cache::{WriteCache, WriteCacheConfig};
        use crate::oci::BlockAdapter;
        use std::io::Cursor;
        use std::sync::atomic::AtomicU64;

        let store = Arc::new(InMemory::new());
        let cs = Arc::new(ContentStore::new(store.clone(), "test"));
        let device_size = 16 * 1024 * 1024u64;
        let temp = tempfile::TempDir::new().unwrap();

        let cache = Arc::new(
            WriteCache::open_fresh_active(WriteCacheConfig {
                cache_dir: temp.path().to_path_buf(),
                device_name: "erofs-bless-test".into(),
                device_size,
                block_size: BLOCK_SIZE as usize,
                wal_sync: false,
            })
            .unwrap(),
        );
        let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(device_size, BLOCK_SIZE)));
        let pic = Arc::new(PackIndexCache::open(temp.path()).await.unwrap());
        let clean: Arc<dyn BlockCache> = Arc::new(crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024));
        let handler = Arc::new(BlockHandler::new(
            Arc::clone(&cache), Arc::clone(&cs), Arc::clone(&clean), Arc::clone(&pic),
            Arc::clone(&vm), device_size, false, Arc::new(ExportMetrics::new()),
            Arc::new(AtomicU64::new(0f64.to_bits())), Arc::new(Notify::const_new()),
            DEFAULT_FLUSH_THRESHOLD, None,
        ));

        // Two overlay layers → merged EROFS, written into the volume.
        let mk = |entries: &[(&str, &[u8])]| -> Vec<u8> {
            let mut b = tar::Builder::new(Vec::new());
            for (p, d) in entries {
                let mut h = tar::Header::new_gnu();
                h.set_path(p).unwrap();
                h.set_size(d.len() as u64);
                h.set_mode(0o644);
                h.set_entry_type(tar::EntryType::Regular);
                h.set_cksum();
                b.append(&h, *d).unwrap();
            }
            b.into_inner().unwrap()
        };
        let l0 = mk(&[("etc/os", b"base"), ("bin/sh", b"#!/bin/sh\n")]);
        let l1 = mk(&[("etc/os", b"top"), ("app/run", b"hi")]);
        let rt = tokio::runtime::Handle::current();
        let hw = Arc::clone(&handler);
        let prefetch_len = tokio::task::spawn_blocking(move || {
            let opts = ext4::tar_convert::ConvertOptions {
                convert_backslash: false,
                writer_options: vec![
                    WriterOption::Uuid([5u8; 16]),
                    WriterOption::AlignData { align: BLOCK_SIZE, min_size: BLOCK_SIZE },
                ],
            };
            let mut layers = vec![Cursor::new(l0), Cursor::new(l1)];
            let (_s, p) = ext4::convert_oci_layers_to_erofs_with_prefetch(
                &mut layers, BlockAdapter::new(&hw, rt), &opts,
            ).unwrap();
            p
        }).await.unwrap();
        handler.set_prefetch_len(prefetch_len);

        // Drain to S3 + persist manifest, then read back the EROFS superblock
        // through a fresh cold handler over the same store.
        for _ in 0..100 {
            let s = cache.flush_to_s3(&cs, &pic, &vm).await.unwrap();
            if s.blocks_claimed == 0 { break; }
        }
        cs.put_manifest("bases/erofs-test", vm.read().serialize().unwrap(), None).await.unwrap();

        let cold_temp = tempfile::TempDir::new().unwrap();
        let cold_cache = Arc::new(
            WriteCache::open_fresh_active(WriteCacheConfig {
                cache_dir: cold_temp.path().to_path_buf(),
                device_name: "erofs-cold".into(),
                device_size, block_size: BLOCK_SIZE as usize, wal_sync: false,
            }).unwrap(),
        );
        let cold_clean: Arc<dyn BlockCache> = Arc::new(crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024));
        let cold = Arc::new(BlockHandler::new(
            cold_cache, Arc::clone(&cs), cold_clean,
            Arc::new(PackIndexCache::open(cold_temp.path()).await.unwrap()),
            Arc::clone(&vm), device_size, true, Arc::new(ExportMetrics::new()),
            Arc::new(AtomicU64::new(0f64.to_bits())), Arc::new(Notify::const_new()),
            DEFAULT_FLUSH_THRESHOLD, None,
        ));
        // EROFS superblock magic lives at byte 1024 (block 0).
        let blk0 = cold.read(0, BLOCK_SIZE).await.unwrap();
        assert_eq!(&blk0[1024..1028], &[0xe2, 0xe1, 0xf5, 0xe0], "served EROFS magic");
    }

    #[tokio::test]
    async fn test_bless_and_read_back() {
        let (store, cs) = test_store();

        // Create a 1MB image with known pattern (8 x 128KB blocks)
        let mut image = vec![0u8; 8 * BLOCK_SIZE as usize];
        for i in 0..8 {
            let start = i * BLOCK_SIZE as usize;
            image[start..start + BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        let stats = bless_bytes(&cs, "test-image", &image).await.unwrap();

        assert_eq!(stats.zero_blocks, 0);
        assert_eq!(stats.unique_blocks, 8);

        // Load VolumeManifest and verify
        let (manifest_data, _) = cs
            .get_manifest("bases/test-image")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();

        assert_eq!(vm.size, image.len() as u64);
        assert_eq!(vm.block_size, BLOCK_SIZE);
        // All 8 blocks fit in chunk 0 (128 MiB chunks, 1MB image)
        assert_eq!(vm.chunks.len(), 1);
        assert!(vm.chunks.contains_key(&0));

        // Get the pack_id for chunk 0
        let pack_ids = vm.chunk_pack_ids(0).unwrap();
        assert_eq!(pack_ids.len(), 1);
        let pack_id = pack_ids[0];

        // Fetch full pack and parse index
        let pack_bytes = fetch_pack_bytes(&store, "test", 0, pack_id).await;
        let entries = parse_pack_index(&pack_bytes).unwrap().entries;

        assert_eq!(entries.len(), 8);

        // Verify every block can be fetched and matches original data
        for entry in &entries {
            let block_index = entry.chunk_offset as usize;
            let original_start = block_index * BLOCK_SIZE as usize;
            let original_block = &image[original_start..original_start + BLOCK_SIZE as usize];

            // Extract and decompress the block from pack
            let compressed =
                extract_block(&pack_bytes, entry.offset, entry.comp_length).unwrap();
            let decompressed = decompress_block(compressed).unwrap();

            assert_eq!(blake3_128(&decompressed), entry.hash);
            assert_eq!(&decompressed[..], original_block);
        }
    }

    #[tokio::test]
    async fn test_bless_idempotent() {
        let (_, cs) = test_store();

        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        for i in 0..4 {
            image[i * BLOCK_SIZE as usize..(i + 1) * BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        // First bless
        let stats1 = bless_bytes(&cs, "idempotent", &image).await.unwrap();
        assert_eq!(stats1.unique_blocks, 4);
        assert!(stats1.packs_uploaded > 0);

        // Second bless of the same image under a different name -- no cross-base
        // dedup in v4, so all blocks are uploaded again as a self-contained base.
        let stats2 = bless_bytes(&cs, "idempotent-2", &image).await.unwrap();
        assert_eq!(
            stats2.unique_blocks, 4,
            "v4 bless uploads all blocks (no cross-base dedup)"
        );
        assert!(
            stats2.packs_uploaded > 0,
            "v4 bless creates packs for every base"
        );
    }

    #[tokio::test]
    async fn test_bless_sparse_image() {
        let (store, cs) = test_store();

        // 4-block image: first block has data, rest are zeros
        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        image[..BLOCK_SIZE as usize].fill(0xAB);

        let stats = bless_bytes(&cs, "sparse", &image).await.unwrap();

        assert_eq!(stats.zero_blocks, 3, "3 zero blocks should be skipped");
        assert_eq!(stats.unique_blocks, 1, "1 data block should be uploaded");
        assert_eq!(stats.packs_uploaded, 1);

        // Verify VolumeManifest only has 1 chunk entry (chunk 0 with 1 pack)
        let (manifest_data, _) = cs
            .get_manifest("bases/sparse")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
        assert_eq!(vm.chunks.len(), 1);

        // Verify pack has 1 entry at chunk_offset 0
        let pack_ids = vm.chunk_pack_ids(0).unwrap();
        assert_eq!(pack_ids.len(), 1);

        let entries = fetch_pack_index(&store, "test", 0, pack_ids[0]).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].chunk_offset, 0);
    }

    #[tokio::test]
    async fn test_bless_within_batch_dedup() {
        let (store, cs) = test_store();

        // 4-block image where block 0 and block 2 have identical content.
        // Within-batch dedup should store only 3 unique blocks in the pack.
        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        image[0..BLOCK_SIZE as usize].fill(0xAA); // block 0
        image[BLOCK_SIZE as usize..2 * BLOCK_SIZE as usize].fill(0xBB); // block 1
        image[2 * BLOCK_SIZE as usize..3 * BLOCK_SIZE as usize].fill(0xAA); // block 2 = same as block 0
        image[3 * BLOCK_SIZE as usize..4 * BLOCK_SIZE as usize].fill(0xCC); // block 3

        let stats = bless_bytes(&cs, "dedup-test", &image).await.unwrap();

        // All 4 blocks are non-zero and counted as unique (within-batch dedup
        // only affects pack assembly, not the stats counter)
        assert_eq!(stats.unique_blocks, 4);
        assert_eq!(stats.packs_uploaded, 1);

        // The pack must contain 4 entries — one per block offset — even though
        // blocks 0 and 2 share a hash. Every block offset needs a pack index
        // entry so the read path can resolve it; missing entries return zeros.
        let (manifest_data, _) = cs
            .get_manifest("bases/dedup-test")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
        let pack_ids = vm.chunk_pack_ids(0).unwrap();
        assert_eq!(pack_ids.len(), 1);

        let entries = fetch_pack_index(&store, "test", 0, pack_ids[0]).await;
        assert_eq!(
            entries.len(),
            4,
            "every block offset must have a pack index entry, even with duplicate hashes"
        );

        // Verify all 4 offsets are present
        let offsets: Vec<u32> = entries.iter().map(|e| e.chunk_offset).collect();
        for expected in 0..4u32 {
            assert!(
                offsets.contains(&expected),
                "missing pack index entry for chunk_offset {}",
                expected,
            );
        }
    }

    #[tokio::test]
    async fn test_bless_self_contained_bases() {
        let (store, cs) = test_store();

        // Image A: 10 blocks of unique data
        let mut image_a = vec![0u8; 10 * BLOCK_SIZE as usize];
        for i in 0..10 {
            image_a[i * BLOCK_SIZE as usize..(i + 1) * BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        // Image B: first 8 blocks same as A, last 2 different
        let mut image_b = image_a.clone();
        image_b[8 * BLOCK_SIZE as usize..9 * BLOCK_SIZE as usize].fill(0xFE);
        image_b[9 * BLOCK_SIZE as usize..10 * BLOCK_SIZE as usize].fill(0xFF);

        // Bless A
        let stats_a = bless_bytes(&cs, "image-a", &image_a).await.unwrap();
        assert_eq!(stats_a.unique_blocks, 10);

        // Bless B -- v4 has no cross-base dedup, so all 10 blocks are uploaded
        let stats_b = bless_bytes(&cs, "image-b", &image_b).await.unwrap();
        assert_eq!(
            stats_b.unique_blocks, 10,
            "v4 bless uploads all blocks (self-contained)"
        );

        // Verify each base is independently readable
        for (name, image) in [("image-a", &image_a), ("image-b", &image_b)] {
            let (manifest_data, _) = cs
                .get_manifest(&format!("bases/{}", name))
                .await
                .unwrap()
                .expect("manifest should exist");
            let vm = VolumeManifest::deserialize(&manifest_data).unwrap();

            for (&chunk_idx, entry) in &vm.chunks {
                for &pack_id in &entry.packs {
                    let pack_bytes =
                        fetch_pack_bytes(&store, "test", chunk_idx, pack_id).await;
                    let index = parse_pack_index(&pack_bytes).unwrap();

                    for pie in &index.entries {
                        let block_index = chunk_idx as usize
                            * vm.blocks_per_chunk() as usize
                            + pie.chunk_offset as usize;
                        let original_start = block_index * BLOCK_SIZE as usize;
                        let original_block =
                            &image[original_start..original_start + BLOCK_SIZE as usize];

                        let compressed = extract_block(
                            &pack_bytes,
                            pie.offset,
                            pie.comp_length,
                        )
                        .unwrap();
                        let decompressed = decompress_block(compressed).unwrap();
                        assert_eq!(
                            &decompressed[..],
                            original_block,
                            "data mismatch at block {} in base {}",
                            block_index,
                            name,
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_bless_block_lookup_by_chunk_offset() {
        let (store, cs) = test_store();

        // Create a 4-block image with distinct patterns
        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        for i in 0..4 {
            image[i * BLOCK_SIZE as usize..(i + 1) * BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        bless_bytes(&cs, "lookup-test", &image).await.unwrap();

        let (manifest_data, _) = cs
            .get_manifest("bases/lookup-test")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
        let pack_ids = vm.chunk_pack_ids(0).unwrap();
        let pack_bytes = fetch_pack_bytes(&store, "test", 0, pack_ids[0]).await;
        let index = parse_pack_index(&pack_bytes).unwrap();

        // Look up each block by chunk_offset and verify data
        for offset in 0..4u32 {
            let (hash, pack_offset, comp_length) =
                lookup_block_in_index(&index.entries, offset)
                    .unwrap_or_else(|| panic!("block at chunk_offset {} not found", offset));

            let compressed =
                extract_block(&pack_bytes, pack_offset, comp_length).unwrap();
            let decompressed = decompress_block(compressed).unwrap();

            assert_eq!(blake3_128(&decompressed), hash);
            let expected = vec![(offset + 1) as u8; BLOCK_SIZE as usize];
            assert_eq!(decompressed, expected);
        }
    }
}
