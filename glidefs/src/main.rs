use anyhow::Result;

mod circuit_breaker;
mod cli;
mod config;
mod block;
mod oci;
mod parse_object_store;
mod storage_compatibility;
mod task;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse_args();

    match cli.command {
        cli::Commands::Init { path } => {
            println!("Generating configuration file at: {}", path.display());
            config::Settings::write_default_config(&path)?;
            println!("Configuration file created successfully!");
            println!("Edit the file and run: glidefs run -c {}", path.display());
        }
        cli::Commands::Run { config } => {
            cli::server::run_server(config).await?;
        }
        cli::Commands::Bless {
            image,
            oci,
            name,
            s3_prefix,
            config,
        } => {
            if let Some(image_path) = image {
                cli::bless::run_bless(image_path, name, s3_prefix, config).await?;
            } else if let Some(image_ref) = oci {
                cli::bless::run_bless_oci(image_ref, name, s3_prefix, config).await?;
            }
        }
        cli::Commands::Push {
            manifest,
            image,
            s3_prefix,
            config,
            base_manifest,
        } => {
            cli::push::run_push(manifest, image, s3_prefix, config, base_manifest).await?;
        }
        cli::Commands::Gc {
            config,
            dry_run,
            grace_period,
            max_deletes,
            state_file,
        } => {
            cli::gc::run_gc(config, dry_run, grace_period, max_deletes, state_file)
                .await?;
        }
    }

    Ok(())
}
