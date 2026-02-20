use anyhow::Result;

mod circuit_breaker;
mod cli;
mod config;
mod block;
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
            name,
            config,
            chunk_size,
        } => {
            cli::bless::run_bless(image, name, config, chunk_size).await?;
        }
        cli::Commands::Gc {
            config,
            dry_run,
            grace_period,
            max_deletes,
            state_file,
        } => {
            cli::gc::run_gc(config, dry_run, grace_period, max_deletes, state_file).await?;
        }
    }

    Ok(())
}
