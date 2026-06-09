use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub mod bless;
pub mod gc;
pub mod handoff_cmd;
pub mod profile;
pub mod push;
pub mod server;

#[derive(Parser)]
#[command(name = "glidefs")]
#[command(author, version, about = "High-performance S3-backed block storage", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Generate a default configuration file
    Init {
        #[arg(default_value = "glidefs.toml")]
        path: PathBuf,
    },
    /// Run the NBD block device server
    Run {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Bless a disk image into a content-addressed base image
    Bless {
        /// Path to raw disk image file (mutually exclusive with --oci)
        #[arg(long, required_unless_present = "oci")]
        image: Option<PathBuf>,
        /// OCI image reference, e.g. "docker.io/library/ubuntu:24.04" (mutually exclusive with --image)
        #[arg(long, required_unless_present = "image", conflicts_with = "image")]
        oci: Option<String>,
        /// Store OCI layers as separate content-addressed artifacts (layers
        /// survive) so layers shared across images dedup in storage, instead of
        /// flattening into one merged ext4. Only valid with --oci.
        #[arg(long, requires = "oci")]
        layered: bool,
        /// Build the merged image as read-only EROFS instead of ext4 — the
        /// correct format for an immutable container/OCI rootfs served daemonless
        /// (kernel `erofs` over ublk; writes go to an overlay upper). Only valid
        /// with --oci; the consumer must mount it read-only. Default is writable
        /// ext4 (which VMs fork-and-write).
        #[arg(long, requires = "oci", conflicts_with = "layered")]
        erofs: bool,
        /// Capture the boot working set by RUNNING the image once at bless time
        /// (chroot + fanotify), so the *real* boot set — including dlopen'd
        /// libraries and config/data files static analysis can't see — drives the
        /// EROFS reorder (contiguous prefix) or the non-EROFS precise warm
        /// (scattered block list). Requires root + an arch-compatible entrypoint;
        /// falls back to static ELF-closure derivation on any failure. Only valid
        /// with --oci. Default off (static derivation).
        #[arg(long, requires = "oci")]
        profile: bool,
        /// Base image name (e.g., "ubuntu-22.04-node20-v3")
        #[arg(long)]
        name: String,
        /// S3 prefix (export namespace) to write the blessed image into.
        /// Ignored with --layered (layer artifacts use a global namespace).
        #[arg(long)]
        s3_prefix: String,
        /// Config file (for storage URL + credentials)
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Push a GlideFS volume to an OCI registry as a container image
    Push {
        /// S3 manifest to export (e.g., "bases/ubuntu-24.04" or "my-vm")
        #[arg(long)]
        manifest: String,
        /// Target OCI image reference (e.g., "registry.example.com/my-app:v1")
        #[arg(long)]
        image: String,
        /// S3 prefix (export namespace) where the manifest lives
        #[arg(long)]
        s3_prefix: String,
        /// Config file (for storage URL + credentials)
        #[arg(short, long)]
        config: PathBuf,
        /// Base manifest for incremental push. Only changes relative to this
        /// manifest are pushed as a delta layer.
        #[arg(long)]
        base_manifest: Option<String>,
    },
    /// Trigger a graceful zero-downtime handoff against a running daemon.
    ///
    /// Connects to the daemon's handoff control socket and signals it to
    /// spawn a successor. The current process exits immediately after
    /// sending the signal — the actual handoff runs in the daemon.
    /// To watch progress, tail the daemon logs.
    Handoff {
        /// Path to the daemon's handoff *control* socket. Defaults to
        /// `/run/glidefs/handoff.ctl.sock`. Note: this is distinct from
        /// the protocol socket at `/run/glidefs/handoff.sock` that the
        /// successor connects to during a handoff.
        #[arg(long, default_value = crate::handoff::DEFAULT_HANDOFF_CTL_SOCKET)]
        socket: PathBuf,
        /// Dry-run mode: spawn a successor that performs WARMING (proves
        /// it can open foyer cache, replay WAL, build router, prefetch
        /// S3) then aborts cleanly without ever touching kernel devices.
        /// Use as a canary check before fleet-wide upgrades.
        #[arg(long)]
        dry_run: bool,
    },
    /// Run garbage collection to clean up orphaned packs in S3
    Gc {
        /// Config file (for storage URL + credentials)
        #[arg(short, long)]
        config: PathBuf,
        /// Report what would be deleted without deleting
        #[arg(long)]
        dry_run: bool,
        /// Grace period before deleting dead packs (e.g., "24h", "1h", "7d")
        #[arg(long, default_value = "24h")]
        grace_period: String,
        /// Maximum number of packs to delete per run
        #[arg(long, default_value = "100000")]
        max_deletes: usize,
        /// Path to GC state file for grace period tracking
        #[arg(long, default_value = "gc-state.json")]
        state_file: PathBuf,
    },
    /// Profile a blessed base's boot working set (off the bless critical path).
    ///
    /// Runs the base's entrypoint once inside an isolation sandbox over a
    /// throwaway ublk device, records the exact boot blocks, and stores them as
    /// the base's `.boot-set` metadata (inherited by every fork of the base).
    /// Idempotent: skips when the base is unchanged since the last profile.
    Profile {
        /// Base name to profile (must already be blessed at `bases/{name}`).
        #[arg(long)]
        name: String,
        /// S3 prefix (export namespace) the base lives in. Mirrors `bless`.
        #[arg(long)]
        s3_prefix: String,
        /// Config file (storage URL + credentials; `[profile]` sandbox settings).
        #[arg(short, long)]
        config: PathBuf,
        /// Isolation backend override: `ns` (default) or `firecracker`.
        #[arg(long)]
        sandbox: Option<String>,
        /// Command to profile, run via `/bin/sh -c` inside the image. Overrides
        /// the base's recorded entrypoint (the `.runspec` from bless).
        #[arg(long)]
        cmd: Option<String>,
        /// Hard per-run timeout in seconds.
        #[arg(long, default_value = "30")]
        timeout: u64,
        /// Filesystem type of the base (`ext4` | `erofs`). Inferred if omitted.
        #[arg(long)]
        fs_type: Option<String>,
        /// Number of boot runs to rank-merge (1–3).
        #[arg(long, default_value = "1")]
        runs: u32,
        /// Re-profile even if the base is unchanged since the last profile.
        #[arg(long)]
        force: bool,
        /// Mark the image untrusted: refuses the namespaces backend (which shares
        /// the host kernel) and requires `--sandbox firecracker`.
        #[arg(long)]
        untrusted: bool,
        /// Cap on captured blocks.
        #[arg(long, default_value = "200000")]
        max_blocks: usize,
    },
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
