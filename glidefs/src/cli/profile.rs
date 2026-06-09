//! `glidefs profile` — decoupled, idempotent boot-set profiling.
//!
//! Bless writes the base fast (no image run); this command profiles it as a
//! separate, retryable step OFF the bless critical path. It loads the base
//! manifest, runs the entrypoint once inside an isolation sandbox over a throwaway
//! ublk device (capturing the exact boot blocks incl. fs metadata), and publishes
//! the result as the base's `.boot-set` (+ a `.boot-set.meta` fingerprint).
//!
//! Idempotent: keyed on the base manifest's ETag, a re-run on an unchanged base is
//! a no-op. Inherited: forks of the base warm the `.boot-set` by base name (router
//! tier-2 warm), so profiling once per base serves every fork.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use parking_lot::RwLock;
use tracing::info;

use crate::block::content_store::ContentStore;
use crate::block::manifest::serialize_block_list;
use crate::block::volume_manifest::VolumeManifest;
use crate::config::Settings;
use crate::oci::boot_capture_served::{BootProfileOptions, capture_boot_blocks_served};
use crate::oci::boot_meta::{BOOT_SET_META_VERSION, BootSetMeta, RunSpec};
use crate::oci::sandbox::{Sandbox, SandboxKind, select_sandbox};
use crate::parse_object_store::parse_url_opts;

pub struct ProfileArgs {
    pub name: String,
    pub s3_prefix: String,
    pub config: PathBuf,
    pub sandbox: Option<String>,
    pub cmd: Option<String>,
    pub timeout: u64,
    pub fs_type: Option<String>,
    pub runs: u32,
    pub force: bool,
    pub untrusted: bool,
    pub max_blocks: usize,
}

pub async fn run_profile(args: ProfileArgs) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    // --- Setup (mirrors bless) ---
    let settings = Settings::from_file(&args.config)
        .with_context(|| format!("load config from {}", args.config.display()))?;
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &settings.storage.url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let base = format!("{}/exports/{}", path_from_url, args.s3_prefix);
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), &base));

    // --- Load the base manifest + its fingerprint (ETag) ---
    let (data, etag) = content_store
        .get_manifest(&format!("bases/{}", args.name))
        .await
        .context("fetch base manifest")?
        .ok_or_else(|| anyhow::anyhow!("base 'bases/{}' not found — bless it first", args.name))?;
    let manifest = VolumeManifest::deserialize(&data).context("parse base manifest")?;
    let device_size = manifest.size;
    let block_size = manifest.block_size;
    let is_erofs_hint = manifest.prefetch_len.is_some();
    let volume_manifest = Arc::new(RwLock::new(manifest));
    let fingerprint = etag.unwrap_or_default();

    // --- Idempotency: skip when the base is unchanged since the last profile ---
    if !args.force && !fingerprint.is_empty() {
        if let Some(meta_bytes) = content_store.get_boot_set_meta(&args.name).await? {
            if let Ok(meta) = BootSetMeta::from_json(&meta_bytes) {
                let has_set = content_store.get_boot_set(&args.name).await?.is_some();
                if meta.fingerprint == fingerprint && has_set {
                    println!(
                        "Boot set for '{}' is up to date (fingerprint {}); skipping. Use --force to re-profile.",
                        args.name,
                        short(&fingerprint)
                    );
                    return Ok(());
                }
            }
        }
    }

    // --- Reconstruct the boot command: --cmd > recorded runspec ---
    let runspec = match content_store.get_runspec(&args.name).await? {
        Some(b) => RunSpec::from_json(&b).ok(),
        None => None,
    };
    let (argv, env, workdir, static_seed, config_digest) = if let Some(cmd) = &args.cmd {
        (
            vec!["/bin/sh".into(), "-c".into(), cmd.clone()],
            runspec.as_ref().map(|r| r.env.clone()).unwrap_or_default(),
            runspec
                .as_ref()
                .map(|r| r.workdir.clone())
                .unwrap_or_default(),
            runspec
                .as_ref()
                .map(|r| r.static_seed.clone())
                .unwrap_or_default(),
            runspec.as_ref().and_then(|r| r.config_digest.clone()),
        )
    } else if let Some(rs) = &runspec {
        if rs.argv.is_empty() {
            bail!("recorded runspec has no entrypoint; pass --cmd");
        }
        (
            rs.argv.clone(),
            rs.env.clone(),
            rs.workdir.clone(),
            rs.static_seed.clone(),
            rs.config_digest.clone(),
        )
    } else {
        bail!(
            "no --cmd given and no recorded runspec for '{}' — pass --cmd '<startup>'",
            args.name
        );
    };

    // fs_type: flag > runspec > infer (EROFS bases carry a prefetch_len hint).
    let fs_type = args
        .fs_type
        .clone()
        .or_else(|| runspec.as_ref().map(|r| r.fs_type.clone()))
        .unwrap_or_else(|| {
            if is_erofs_hint {
                "erofs".into()
            } else {
                "ext4".into()
            }
        });

    // --- Build the selected sandbox (the trusted gate surfaces here) ---
    let kind_override = match &args.sandbox {
        Some(s) => Some(s.parse::<SandboxKind>()?),
        None => None,
    };
    let pcfg = settings.profile.clone().unwrap_or_default();
    let sb_cfg = pcfg.sandbox_config(kind_override, !args.untrusted);
    let sandbox: Arc<dyn Sandbox> = Arc::from(select_sandbox(&sb_cfg)?);
    let limits = pcfg.resource_limits();

    info!(name = %args.name, %fs_type, runs = args.runs, "profiling base boot set");
    let opts = BootProfileOptions {
        sandbox,
        limits,
        runs: args.runs,
        timeout: Duration::from_secs(args.timeout.max(1)),
        static_seed,
        max_blocks: args.max_blocks,
    };
    let blocks = capture_boot_blocks_served(
        Arc::clone(&content_store),
        Arc::clone(&volume_manifest),
        device_size,
        block_size,
        &fs_type,
        &argv,
        &env,
        &workdir,
        &args.name,
        &opts,
    )
    .await;

    let Some(blocks) = blocks else {
        bail!("profiling captured no boot blocks (entrypoint failed to start, or no ublk/root?)");
    };

    // --- Atomic publish: data first, meta (the commit marker) LAST ---
    content_store
        .put_boot_set(&args.name, serialize_block_list(&blocks))
        .await
        .context("upload boot set")?;
    let meta = BootSetMeta {
        version: BOOT_SET_META_VERSION,
        fingerprint: fingerprint.clone(),
        fs_type: fs_type.clone(),
        block_count: blocks.len() as u64,
        profiled_at: chrono::Utc::now().to_rfc3339(),
        config_digest,
    };
    content_store
        .put_boot_set_meta(&args.name, meta.to_json())
        .await
        .context("upload boot set meta")?;

    let mib = blocks.len() as f64 * f64::from(block_size) / 1e6;
    println!("Profiled '{}' boot set:", args.name);
    println!("  fs type:       {fs_type}");
    println!("  boot blocks:   {} ({:.1} MiB)", blocks.len(), mib);
    println!("  runs merged:   {}", args.runs.clamp(1, 3));
    println!("  fingerprint:   {}", short(&fingerprint));
    println!(
        "  stored:        bases/{}.boot-set (+ .boot-set.meta)",
        args.name
    );
    Ok(())
}

/// Short fingerprint for display.
fn short(s: &str) -> String {
    let t = s.trim_matches('"');
    if t.len() > 16 {
        format!("{}…", &t[..16])
    } else {
        t.to_string()
    }
}
