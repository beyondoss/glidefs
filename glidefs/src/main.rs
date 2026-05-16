use anyhow::Result;

mod circuit_breaker;
mod cli;
mod config;
mod block;
mod oci;
mod parse_object_store;
mod sd_notify;
mod storage_compatibility;
mod task;
#[allow(unsafe_code)] // libc socket/bind/connect/sendmsg/recvmsg for SCM_RIGHTS
mod handoff;

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Install a process-wide panic hook that records every panic before chaining
/// to the default Rust hook. The hook fires for *all* panics — including those
/// in `std::thread`-spawned ublk worker threads and in code reached before
/// `tracing-subscriber` is initialized — so we always increment the counter
/// and always emit a structured stderr line, even if `tracing::error!` is a
/// no-op (no subscriber yet).
fn install_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        task::record_panic();
        let message: String = if let Some(s) = info.payload().downcast_ref::<&'static str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let location: String = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("<unnamed>");
        // Tracing path — useful once a subscriber is installed.
        tracing::error!(
            panic.message = %message,
            panic.location = %location,
            thread = thread_name,
            "process-wide panic hook fired",
        );
        // Stderr fallback — covers panics that fire before tracing-subscriber
        // is initialized in `cli::server::run_server`.
        eprintln!(
            "PANIC: {} at {} (thread '{}')",
            message, location, thread_name
        );
        prev(info);
    }));
}

#[tokio::main]
async fn main() -> Result<()> {
    install_panic_hook();

    // Successor-mode entry: if the predecessor spawned us with
    // `--handoff-from <socket>`, take over and continue serving instead
    // of doing a cold-start. We sniff argv directly so the rest of the
    // CLI grammar (clap) stays unchanged.
    if let Some(socket) = handoff::successor::handoff_from_arg() {
        return cli::server::run_server_as_successor(socket).await;
    }

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
        cli::Commands::Handoff { socket, dry_run } => {
            cli::handoff_cmd::run(socket, dry_run).await?;
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
