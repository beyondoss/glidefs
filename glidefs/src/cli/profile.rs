//! `glidefs profile` — decoupled, idempotent boot-set profiling.
//!
//! Bless writes the base fast (no image run); this command profiles it as a
//! separate, retryable step OFF the bless critical path. The capture engine
//! lives in [`crate::oci::profile_runner`] (shared with the daemon's
//! `POST /api/profile/{s3_prefix}/{name}`); this wrapper owns the CLI
//! concerns: config/object-store setup, flag parsing, and console output.
//!
//! Idempotent: keyed on the base manifest's ETag, a re-run on an unchanged base is
//! a no-op. Inherited: forks of the base warm the `.boot-set` by base name (router
//! tier-2 warm), so profiling once per base serves every fork.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::block::content_store::ContentStore;
use crate::config::Settings;
use crate::oci::profile_runner::{ProfileOutcome, ProfileSpec, profile_base};
use crate::oci::sandbox::SandboxKind;
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

    let sandbox = match &args.sandbox {
        Some(s) => Some(s.parse::<SandboxKind>()?),
        None => None,
    };
    let pcfg = settings.profile.clone().unwrap_or_default();
    let spec = ProfileSpec {
        name: args.name.clone(),
        cmd: args.cmd,
        seed_paths: Vec::new(),
        fs_type: args.fs_type,
        sandbox,
        trusted: !args.untrusted,
        runs: args.runs,
        timeout: Duration::from_secs(args.timeout.max(1)),
        force: args.force,
        max_blocks: args.max_blocks,
    };

    match profile_base(content_store, &pcfg, spec, None).await? {
        ProfileOutcome::UpToDate { fingerprint } => {
            println!(
                "Boot set for '{}' is up to date (fingerprint {}); skipping. Use --force to re-profile.",
                args.name,
                short(&fingerprint)
            );
        }
        ProfileOutcome::Profiled {
            block_count,
            block_size,
            fs_type,
            fingerprint,
        } => {
            let mib = block_count as f64 * f64::from(block_size) / 1e6;
            println!("Profiled '{}' boot set:", args.name);
            println!("  fs type:       {fs_type}");
            println!("  boot blocks:   {block_count} ({mib:.1} MiB)");
            println!("  runs merged:   {}", args.runs.clamp(1, 3));
            println!("  fingerprint:   {}", short(&fingerprint));
            println!(
                "  stored:        bases/{}.boot-set (+ .boot-set.meta)",
                args.name
            );
        }
    }
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
