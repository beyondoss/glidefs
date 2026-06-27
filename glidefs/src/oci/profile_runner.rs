//! Shared boot-set profiling core — the engine behind both `glidefs profile`
//! (CLI, `cli/profile.rs`) and the daemon's `POST /api/profile/{s3_prefix}/{name}`
//! (`block/api.rs` → `ExportRouter::start_profile`).
//!
//! Loads the base manifest, reconstructs the boot entrypoint (explicit cmd >
//! recorded runspec), runs it inside an isolation sandbox over a throwaway ublk
//! device, and publishes the captured boot set as the base's `.boot-set` +
//! `.boot-set.meta` sidecars. Idempotent keyed on the base manifest's ETag.
//!
//! Callers own the I/O setup (object store, ContentStore, ProfileConfig source)
//! and presentation (println vs status map); this module owns everything in
//! between.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use parking_lot::RwLock;
use tracing::info;

use crate::block::content_store::ContentStore;
use crate::block::manifest::serialize_block_list;
use crate::block::volume_manifest::VolumeManifest;
use crate::config::ProfileConfig;
use crate::oci::boot_capture_served::{BootProfileOptions, capture_boot_blocks_served};
use crate::oci::boot_meta::{BOOT_SET_META_VERSION, BootSetMeta, RunSpec};
use crate::oci::sandbox::{Sandbox, SandboxKind, select_sandbox};

/// What to profile and how. The `name` is the base's manifest key under
/// `bases/` (content store relative).
pub struct ProfileSpec {
    pub name: String,
    /// Entrypoint override (run as `/bin/sh -c <cmd>`). Falls back to the
    /// recorded runspec; an error if neither is present.
    pub cmd: Option<String>,
    /// Extra absolute in-image paths faulted under the tracer before the
    /// entrypoint runs — unioned with the runspec's static seed. Lets a caller
    /// that composed files into the image (and therefore knows their paths)
    /// guarantee those files land in the boot set without knowing the boot's
    /// dynamic working set.
    pub seed_paths: Vec<String>,
    /// fs_type override: flag > runspec > infer (EROFS bases carry a
    /// `prefetch_len` hint).
    pub fs_type: Option<String>,
    /// Sandbox override; `None` uses the `[profile]` config (default ns).
    pub sandbox: Option<SandboxKind>,
    /// Whether the image is first-party. The namespaces backend refuses
    /// untrusted images (host kernel mounts the fs).
    pub trusted: bool,
    /// Boot runs to rank-merge (clamped 1–3).
    pub runs: u32,
    /// Hard per-run wall-clock timeout.
    pub timeout: Duration,
    /// Re-profile even when the boot set matches the base fingerprint.
    pub force: bool,
    /// Cap on captured blocks per run.
    pub max_blocks: usize,
}

/// Result of a profile run.
pub enum ProfileOutcome {
    /// Boot set already current for the base's fingerprint; nothing done.
    UpToDate { fingerprint: String },
    /// Captured and published a fresh boot set.
    Profiled {
        block_count: u64,
        block_size: u32,
        fs_type: String,
        fingerprint: String,
    },
}

/// Profile `spec.name` and publish its boot-set sidecars via `content_store`.
///
/// Atomic publish: `.boot-set` data first, `.boot-set.meta` (the commit
/// marker) last — a crash in between is re-profiled on the next run.
pub async fn profile_base(
    content_store: Arc<ContentStore>,
    profile_cfg: &ProfileConfig,
    spec: ProfileSpec,
    // Parent dir for per-run profiler scratch. The daemon passes its
    // configured `[cache].dir` so the scratch lands on disk, not a tmpfs
    // `/tmp`; CLI callers pass `None` (process-lifetime scratch).
    scratch_dir: Option<std::path::PathBuf>,
) -> Result<ProfileOutcome> {
    // --- Load the base manifest + its fingerprint (ETag) ---
    let (data, etag) = content_store
        .get_manifest(&format!("bases/{}", spec.name))
        .await
        .context("fetch base manifest")?
        .ok_or_else(|| anyhow::anyhow!("base 'bases/{}' not found — bless it first", spec.name))?;
    let manifest = VolumeManifest::deserialize(&data).context("parse base manifest")?;
    let device_size = manifest.size;
    let block_size = manifest.block_size;
    let is_erofs_hint = manifest.prefetch_len.is_some();
    let volume_manifest = Arc::new(RwLock::new(manifest));
    let fingerprint = etag.unwrap_or_default();

    // --- Idempotency: skip when the base is unchanged since the last profile ---
    if !spec.force
        && !fingerprint.is_empty()
        && let Some(meta_bytes) = content_store.get_boot_set_meta(&spec.name).await?
        && let Ok(meta) = BootSetMeta::from_json(&meta_bytes)
        && meta.fingerprint == fingerprint
        && content_store.get_boot_set(&spec.name).await?.is_some()
    {
        return Ok(ProfileOutcome::UpToDate { fingerprint });
    }

    // --- Reconstruct the boot command: cmd > recorded runspec ---
    let runspec = match content_store.get_runspec(&spec.name).await? {
        Some(b) => RunSpec::from_json(&b).ok(),
        None => None,
    };
    let (argv, env, workdir, mut static_seed, config_digest) = if let Some(cmd) = &spec.cmd {
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
            bail!(
                "recorded runspec for '{}' has no entrypoint; pass a cmd",
                spec.name
            );
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
            "no cmd given and no recorded runspec for '{}' — pass a startup command",
            spec.name
        );
    };

    // Union caller-provided seed paths into the static seed (order-preserving).
    for p in &spec.seed_paths {
        if !static_seed.iter().any(|s| s == p) {
            static_seed.push(p.clone());
        }
    }

    // fs_type: override > runspec > infer (EROFS bases carry a prefetch_len hint).
    let fs_type = spec
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
    let sb_cfg = profile_cfg.sandbox_config(spec.sandbox, spec.trusted);
    let sandbox: Arc<dyn Sandbox> = Arc::from(select_sandbox(&sb_cfg)?);
    let limits = profile_cfg.resource_limits();

    info!(
        name = %spec.name,
        %fs_type,
        runs = spec.runs,
        seeds = static_seed.len(),
        "profiling base boot set"
    );
    let opts = BootProfileOptions {
        sandbox,
        limits,
        runs: spec.runs,
        timeout: spec.timeout.max(Duration::from_secs(1)),
        static_seed,
        max_blocks: spec.max_blocks,
        scratch_dir,
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
        &spec.name,
        &opts,
    )
    .await;

    let Some(blocks) = blocks else {
        bail!("profiling captured no boot blocks (entrypoint failed to start, or no ublk/root?)");
    };

    // --- Atomic publish: data first, meta (the commit marker) LAST ---
    content_store
        .put_boot_set(&spec.name, serialize_block_list(&blocks))
        .await
        .context("upload boot set")?;
    let block_count = blocks.len() as u64;
    let meta = BootSetMeta {
        version: BOOT_SET_META_VERSION,
        fingerprint: fingerprint.clone(),
        fs_type: fs_type.clone(),
        block_count,
        profiled_at: chrono::Utc::now().to_rfc3339(),
        config_digest,
    };
    content_store
        .put_boot_set_meta(&spec.name, meta.to_json())
        .await
        .context("upload boot set meta")?;

    Ok(ProfileOutcome::Profiled {
        block_count,
        block_size,
        fs_type,
        fingerprint,
    })
}
