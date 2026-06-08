//! Disposable boot-fault profiler (Phase 0).
//!
//! Captures the REAL boot working set of a blessed image, WITHOUT strace and
//! WITHOUT booting in production VMs: it serves the already-blessed image through
//! GlideFS over a throwaway ublk device with a read-fault recorder attached,
//! kernel-mounts it, runs the workload once in a chroot under a hard timeout, and
//! records the blocks the kernel actually fetches (first-touch order). It then
//! reports the boot set and — the headline number — how many distinct PACKS that
//! set spans in the image's manifest, which predicts whether the automatic 32 MiB
//! readahead already captures it (≈1 pack → no win) or not (many packs → the
//! reorder wins).
//!
//! This is the eStargz `optimize` / REAP record model. Requires root + ublk.
//! Run AFTER `glidefs bless --oci --erofs ...` against the same store/config:
//!
//!   sudo GLIDEFS_BOOT_SET_OUT=/tmp/busybox.bootset \
//!     target/debug/boot_profile busybox prof /tmp/prof-cfg.toml erofs \
//!     -- /bin/busybox sh -c 'ls / >/dev/null; uname -a >/dev/null'
//!
//! Writes the ordered boot block list (one u64 per line) to `GLIDEFS_BOOT_SET_OUT`
//! if set, for the cold-replay harness to consume as a real oracle trace.

#[cfg(not(feature = "ublk"))]
fn main() {
    eprintln!("boot_profile requires the `ublk` feature (build with --features ublk)");
    std::process::exit(2);
}

#[cfg(feature = "ublk")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use glidefs::block::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
    use glidefs::block::content_store::ContentStore;
    use glidefs::block::handler::BlockHandler;
    use glidefs::block::metrics::ExportMetrics;
    use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use glidefs::block::pack_index_cache::PackIndexCache;
    use glidefs::block::ublk::UblkServer;
    use glidefs::block::volume_manifest::VolumeManifest;
    use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};
    use glidefs::block::write_trace::{boot_set_from_trace, WriteTracer};
    use glidefs::config::Settings;
    use glidefs::parse_object_store::parse_url_opts;
    use tokio::sync::Notify;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    // ---- args: <name> <s3_prefix> <config> <fs_type> -- <run_cmd...> ----
    let args: Vec<String> = std::env::args().collect();
    let sep = args.iter().position(|a| a == "--");
    let (head, run_cmd): (&[String], Vec<String>) = match sep {
        Some(i) => (&args[1..i], args[i + 1..].to_vec()),
        None => (&args[1..], Vec::new()),
    };
    if head.len() < 4 || run_cmd.is_empty() {
        anyhow::bail!(
            "usage: boot_profile <name> <s3_prefix> <config> <fs_type> -- <run_cmd...>"
        );
    }
    let (name, s3_prefix, config, fs_type) =
        (&head[0], &head[1], std::path::PathBuf::from(&head[2]), &head[3]);
    let max_blocks: usize = std::env::var("GLIDEFS_BOOT_SET_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);

    // ---- store + content store (mirrors bless) ----
    let settings = Settings::from_file(&config)?;
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

    // ---- load the blessed manifest ----
    let manifest_key = format!("bases/{name}");
    let (data, _etag) = content_store
        .get_manifest(&manifest_key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("manifest 'bases/{name}' not found in store"))?;
    let manifest = VolumeManifest::deserialize(&data)?;
    let device_size = manifest.size;
    let block_size = manifest.block_size;
    let volume_manifest = Arc::new(parking_lot::RwLock::new(manifest));
    eprintln!(
        "[profile] {name}: device={:.1} MB block={} KiB",
        device_size as f64 / 1e6,
        block_size / 1024
    );

    if !std::path::Path::new("/dev/ublk-control").exists() {
        anyhow::bail!("/dev/ublk-control absent — need root + ublk");
    }

    // ---- disposable serving stack with the read-fault recorder ----
    let tmp = tempfile::TempDir::new()?;
    let rtrace_path = tmp.path().join("boot.rtrace");
    let tracer = Arc::new(WriteTracer::new(
        &rtrace_path,
        block_size,
        device_size / u64::from(block_size),
        name,
    )?);

    let cache = Arc::new(WriteCache::open_fresh_active(WriteCacheConfig {
        cache_dir: tmp.path().to_path_buf(),
        device_name: format!("profile-{name}"),
        device_size,
        block_size: block_size as usize,
        wal_sync: false,
    })?);
    let foyer_dir = tmp.path().join("foyer");
    std::fs::create_dir_all(&foyer_dir)?;
    let clean: Arc<dyn BlockCache> = Arc::new(
        FoyerBlockCache::open(FoyerCacheConfig {
            memory_bytes: 64 * 1024 * 1024,
            ssd_bytes: 256 * 1024 * 1024,
            ssd_dir: foyer_dir,
            direct: false,
            io_uring: false,
        })
        .await?,
    );
    let pack_index_cache = Arc::new(PackIndexCache::open(tmp.path()).await?);
    let handler = Arc::new(
        BlockHandler::new(
            cache,
            Arc::clone(&content_store),
            clean,
            pack_index_cache,
            Arc::clone(&volume_manifest),
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

    let dev_name = format!("profile-{name}").replace('/', "-");
    let mut server = UblkServer::new();
    let dev = server.add_device(&dev_name, Arc::clone(&handler)).await?;
    eprintln!("[profile] serving at {} — mounting + running {:?}", dev.display(), run_cmd);

    // ---- mount + chroot-run the workload while ublk serves I/O ----
    let fs_type_s = fs_type.to_string();
    let run_cmd_c = run_cmd.clone();
    let mnt = tempfile::TempDir::new()?;
    let mnt_path = mnt.path().to_path_buf();
    let ran = tokio::task::spawn_blocking(move || -> Result<(), String> {
        use std::process::Command;
        let mnt = &mnt_path;
        let m = Command::new("mount")
            .args(["-t", &fs_type_s, "-o", "ro"])
            .arg(&dev)
            .arg(mnt)
            .status();
        if !matches!(m, Ok(s) if s.success()) {
            return Err(format!("mount -t {fs_type_s} {} failed: {m:?}", dev.display()));
        }
        let _ = Command::new("mount").args(["--bind", "/proc"]).arg(mnt.join("proc")).status();
        let _ = Command::new("mount").args(["--bind", "/dev"]).arg(mnt.join("dev")).status();
        let _ = Command::new("mount").args(["--bind", "/sys"]).arg(mnt.join("sys")).status();
        // Run once under a hard timeout (long-running servers are killed after
        // their startup reads — that IS the boot working set). Cold reads through
        // ublk→S3 are slow, so large images need a longer window to finish their
        // startup before the kill; `GLIDEFS_PROFILE_TIMEOUT` (seconds) tunes it.
        let timeout_s =
            std::env::var("GLIDEFS_PROFILE_TIMEOUT").unwrap_or_else(|_| "15".into());
        let mut chroot = Command::new("timeout");
        chroot.args(["--signal=KILL", &timeout_s, "chroot"]).arg(mnt);
        chroot.args(&run_cmd_c);
        chroot.env_clear();
        chroot.env("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");
        chroot.env("HOME", "/root");
        chroot.env("LANG", "C");
        let st = chroot.status();
        eprintln!("[profile] workload exited: {st:?}");
        let _ = Command::new("umount").arg(mnt.join("sys")).status();
        let _ = Command::new("umount").arg(mnt.join("dev")).status();
        let _ = Command::new("umount").arg(mnt.join("proc")).status();
        let _ = Command::new("umount").arg(mnt).status();
        Ok(())
    })
    .await;

    // ALWAYS tear down the device.
    server.remove_device(&dev_name).await.ok();
    tracer.finish();

    match ran {
        Ok(Ok(())) => {}
        Ok(Err(e)) => anyhow::bail!("profiling: {e}"),
        Err(e) => anyhow::bail!("profiling task panicked: {e}"),
    }

    // ---- extract boot set + scatter characterization ----
    let bytes = std::fs::read(&rtrace_path)?;
    let boot_set = boot_set_from_trace(&bytes, max_blocks);
    if boot_set.is_empty() {
        anyhow::bail!("workload read nothing — no boot set captured");
    }

    let vm = volume_manifest.read();
    let mut chunks = std::collections::HashSet::new();
    let mut packs = std::collections::HashSet::new();
    for &b in &boot_set {
        let ci = vm.chunk_idx_for_block(b);
        chunks.insert(ci);
        if let Some(pids) = vm.chunk_pack_ids(ci) {
            for p in pids {
                packs.insert(*p);
            }
        }
    }
    let total_packs = vm.all_pack_ids().len();
    let boot_mib = boot_set.len() as f64 * block_size as f64 / 1e6;

    println!("\n==== BOOT SET: {name} ====");
    println!("  boot blocks:        {}", boot_set.len());
    println!("  boot set size:      {boot_mib:.1} MiB ({}-KiB blocks)", block_size / 1024);
    println!("  chunks spanned:     {}", chunks.len());
    println!("  PACKS spanned:      {}  (of {total_packs} total in image)", packs.len());
    println!(
        "  → readahead pays ~{} cold GET(s); a reorder collapses that to ~1.",
        packs.len()
    );

    if let Ok(out) = std::env::var("GLIDEFS_BOOT_SET_OUT") {
        use std::io::Write;
        let mut f = std::fs::File::create(&out)?;
        for b in &boot_set {
            writeln!(f, "{b}")?;
        }
        println!("  wrote boot block list → {out}");
    }
    Ok(())
}
