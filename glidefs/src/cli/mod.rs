use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub mod bless;
pub mod gc;
pub mod handoff_cmd;
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
    /// Build and upload a base's boot SET from a captured read trace.
    ///
    /// Profiling flow: run a fork of the base with `GLIDEFS_READ_TRACE_DIR=<dir>`
    /// set, boot the workload once (this records `<dir>/<export>.rtrace`), then
    /// run this command to convert that trace into the bounded boot working set
    /// and upload it. Future forks of the base then data-prefetch exactly those
    /// blocks on open (warm-on-open), instead of paying S3 round-trips lazily.
    MakeBootSet {
        /// Path to the captured read trace (`<export>.rtrace`).
        #[arg(long)]
        trace: PathBuf,
        /// Base image name to attach the boot set to (e.g. "ubuntu-22.04").
        #[arg(long)]
        name: String,
        /// S3 prefix (export namespace) the base lives in.
        #[arg(long)]
        s3_prefix: String,
        /// Cap on boot-set size in 128 KiB blocks (bounds the prefetch so it
        /// can never pull the whole image). Default 4096 (= 512 MiB).
        #[arg(long, default_value_t = 4096)]
        max_blocks: usize,
        /// Config file (for storage URL + credentials).
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
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
