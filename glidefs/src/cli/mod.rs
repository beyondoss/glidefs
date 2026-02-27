use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub mod bless;
pub mod gc;
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
    /// Bless a raw disk image into a content-addressed base image
    Bless {
        /// Path to raw disk image file
        #[arg(long)]
        image: PathBuf,
        /// Base image name (e.g., "ubuntu-22.04-node20-v3")
        #[arg(long)]
        name: String,
        /// S3 prefix (export namespace) to write the blessed image into
        #[arg(long)]
        s3_prefix: String,
        /// Config file (for storage URL + credentials)
        #[arg(short, long)]
        config: PathBuf,
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
