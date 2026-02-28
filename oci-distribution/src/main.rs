mod config;
mod publish;
mod serve;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "glidefs-registry")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Publish a GlideFS volume snapshot as an OCI image in S3
    Publish {
        /// Volume manifest name in S3
        #[arg(long)]
        manifest: String,
        /// S3 prefix where the export lives
        #[arg(long)]
        s3_prefix: String,
        /// OCI image tag (e.g., "v1", "latest")
        #[arg(long, default_value = "latest")]
        tag: String,
        /// Base manifest for delta publish (two-layer image)
        #[arg(long)]
        base_manifest: Option<String>,
        /// GlideFS config file (for S3 credentials)
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Start the OCI distribution server
    Serve {
        /// Listen address
        #[arg(long, default_value = "0.0.0.0:5000")]
        listen: SocketAddr,
        /// S3 prefix (the {name} in /v2/{name}/...)
        #[arg(long)]
        s3_prefix: String,
        /// GlideFS config file (for S3 credentials)
        #[arg(short, long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Publish {
            manifest,
            s3_prefix,
            tag,
            base_manifest,
            config,
        } => {
            publish::run_publish(manifest, s3_prefix, tag, base_manifest, config).await?;
        }
        Commands::Serve {
            listen,
            s3_prefix,
            config,
        } => {
            serve::run_serve(listen, s3_prefix, config).await?;
        }
    }

    Ok(())
}
