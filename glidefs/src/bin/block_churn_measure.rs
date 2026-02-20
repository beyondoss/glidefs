/// Measure block-level churn between a base image and forked images.
///
/// Compares 128KB blocks by BLAKE3 hash to produce empirical data on:
/// - Unique block write volume per fork
/// - Spatial clustering of changed blocks (GC cohort quality)
/// - Pack simulation at various BLOCKS_PER_PACK values
/// - Write rate derivation given workload duration
///
/// Usage:
///   cargo run --release --bin block_churn_measure -- \
///     --base /tmp/glidefs-dedup-test/forked/base.raw \
///     /tmp/glidefs-dedup-test/forked/vm-*.raw
///
///   cargo run --release --bin block_churn_measure -- \
///     --base base.raw --duration-secs 60 --pack-sizes 100,200,500 fork*.raw
///
/// Generate test images first:
///   ./scripts/gen_dedup_images.sh /tmp/glidefs-dedup-test
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

use clap::Parser;

const BLOCK_SIZE: usize = 128 * 1024; // 128KB
const DEFAULT_PACK_SIZES: &[usize] = &[100, 200, 500, 1000];

#[derive(Parser)]
#[command(
    name = "block_churn_measure",
    about = "Measure block-level churn between base and forked disk images"
)]
struct Args {
    /// Base image to compare against.
    #[arg(long, required = true)]
    base: PathBuf,

    /// Forked images to analyze.
    #[arg(required = true)]
    forks: Vec<PathBuf>,

    /// Workload duration in seconds (for write rate derivation).
    #[arg(long)]
    duration_secs: Option<f64>,

    /// Comma-separated pack sizes to simulate.
    #[arg(long, value_delimiter = ',')]
    pack_sizes: Option<Vec<usize>>,
}

fn blake3_128(data: &[u8]) -> [u8; 16] {
    let full = blake3::hash(data);
    let mut out = [0u8; 16];
    out.copy_from_slice(&full.as_bytes()[..16]);
    out
}

fn is_zero(data: &[u8]) -> bool {
    let (prefix, chunks, suffix) = unsafe { data.align_to::<u64>() };
    prefix.iter().all(|&b| b == 0)
        && chunks.iter().all(|&w| w == 0)
        && suffix.iter().all(|&b| b == 0)
}

/// Hash every 128KB block in an image. Returns (hash, is_zero) per position.
fn hash_image(path: &PathBuf) -> Vec<([u8; 16], bool)> {
    let file = File::open(path).unwrap_or_else(|e| {
        eprintln!("error: {}: {e}", path.display());
        std::process::exit(1);
    });
    let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut buf = vec![0u8; BLOCK_SIZE];
    let mut hashes = Vec::new();

    loop {
        buf.fill(0);
        let mut filled = 0;
        while filled < BLOCK_SIZE {
            match reader.read(&mut buf[filled..BLOCK_SIZE]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => {
                    eprintln!("error: read {}: {e}", path.display());
                    std::process::exit(1);
                }
            }
        }
        if filled == 0 {
            break;
        }
        let zero = is_zero(&buf[..BLOCK_SIZE]);
        let hash = blake3_128(&buf[..BLOCK_SIZE]);
        hashes.push((hash, zero));
    }

    hashes
}

struct ForkChurn {
    name: String,
    total_blocks: usize,
    unchanged: usize,
    changed: usize,
    new_blocks: usize,
    deleted: usize,
    /// Indices of changed+new blocks (the "dirty" set)
    dirty_indices: Vec<usize>,
}

fn analyze_fork(
    base_hashes: &[([u8; 16], bool)],
    fork_hashes: &[([u8; 16], bool)],
    fork_name: &str,
) -> ForkChurn {
    let len = base_hashes.len().max(fork_hashes.len());
    let mut unchanged = 0usize;
    let mut changed = 0usize;
    let mut new_blocks = 0usize;
    let mut deleted = 0usize;
    let mut dirty_indices = Vec::new();

    for i in 0..len {
        let (b_hash, b_zero) = if i < base_hashes.len() {
            base_hashes[i]
        } else {
            ([0u8; 16], true) // beyond base = zero
        };
        let (f_hash, f_zero) = if i < fork_hashes.len() {
            fork_hashes[i]
        } else {
            ([0u8; 16], true) // beyond fork = zero
        };

        if b_hash == f_hash {
            unchanged += 1;
            continue;
        }

        if b_zero && !f_zero {
            new_blocks += 1;
            dirty_indices.push(i);
        } else if !b_zero && f_zero {
            deleted += 1;
        } else {
            changed += 1;
            dirty_indices.push(i);
        }
    }

    ForkChurn {
        name: fork_name.to_string(),
        total_blocks: len,
        unchanged,
        changed,
        new_blocks,
        deleted,
        dirty_indices,
    }
}

struct RunLengthBucket {
    label: &'static str,
    min: usize,
    max: usize,
    count: usize,
    blocks: usize,
}

fn spatial_clustering(dirty_indices: &[usize]) -> Vec<RunLengthBucket> {
    if dirty_indices.is_empty() {
        return Vec::new();
    }

    // Compute run lengths of consecutive dirty blocks
    let mut runs: Vec<usize> = Vec::new();
    let mut current_run = 1usize;

    for i in 1..dirty_indices.len() {
        if dirty_indices[i] == dirty_indices[i - 1] + 1 {
            current_run += 1;
        } else {
            runs.push(current_run);
            current_run = 1;
        }
    }
    runs.push(current_run);

    // Bucket the runs
    let mut buckets = vec![
        RunLengthBucket {
            label: "1 (isolated)",
            min: 1,
            max: 1,
            count: 0,
            blocks: 0,
        },
        RunLengthBucket {
            label: "2-4",
            min: 2,
            max: 4,
            count: 0,
            blocks: 0,
        },
        RunLengthBucket {
            label: "5-16",
            min: 5,
            max: 16,
            count: 0,
            blocks: 0,
        },
        RunLengthBucket {
            label: "17-64",
            min: 17,
            max: 64,
            count: 0,
            blocks: 0,
        },
        RunLengthBucket {
            label: "65+",
            min: 65,
            max: usize::MAX,
            count: 0,
            blocks: 0,
        },
    ];

    for &run in &runs {
        for bucket in &mut buckets {
            if run >= bucket.min && run <= bucket.max {
                bucket.count += 1;
                bucket.blocks += run;
                break;
            }
        }
    }

    buckets
}

fn human(bytes: usize) -> String {
    if bytes >= 1 << 30 {
        format!("{:.1}GB", bytes as f64 / (1 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.1}MB", bytes as f64 / (1 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.1}KB", bytes as f64 / (1 << 10) as f64)
    } else {
        format!("{bytes}B")
    }
}

fn main() {
    let args = Args::parse();
    let pack_sizes = args
        .pack_sizes
        .unwrap_or_else(|| DEFAULT_PACK_SIZES.to_vec());

    // Hash base image
    eprint!("Hashing base image {}...", args.base.display());
    let base_hashes = hash_image(&args.base);
    eprintln!(
        " {} blocks ({})",
        base_hashes.len(),
        human(base_hashes.len() * BLOCK_SIZE)
    );

    let base_nonzero = base_hashes.iter().filter(|(_, z)| !z).count();
    println!("Base: {} blocks, {} non-zero ({})",
        base_hashes.len(),
        base_nonzero,
        human(base_nonzero * BLOCK_SIZE),
    );
    println!();

    for fork_path in &args.forks {
        let fork_name = fork_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        eprint!("Hashing {}...", fork_name);
        let fork_hashes = hash_image(fork_path);
        eprintln!(" {} blocks", fork_hashes.len());

        let churn = analyze_fork(&base_hashes, &fork_hashes, &fork_name);
        let dirty_count = churn.changed + churn.new_blocks;
        let dirty_bytes = dirty_count * BLOCK_SIZE;

        // === Fork summary ===
        println!("Fork: {} vs base", churn.name);
        println!("  Total blocks:   {:>8}", churn.total_blocks);
        println!(
            "  Unchanged:      {:>8} ({:.1}%)",
            churn.unchanged,
            churn.unchanged as f64 / churn.total_blocks as f64 * 100.0
        );
        println!(
            "  Changed:        {:>8} ({:.1}%)",
            churn.changed,
            churn.changed as f64 / churn.total_blocks as f64 * 100.0
        );
        println!(
            "  New:            {:>8} ({:.1}%)",
            churn.new_blocks,
            churn.new_blocks as f64 / churn.total_blocks as f64 * 100.0
        );
        println!(
            "  Deleted:        {:>8} ({:.1}%)",
            churn.deleted,
            churn.deleted as f64 / churn.total_blocks as f64 * 100.0
        );
        println!(
            "  Unique writes:  {:>8} blocks ({})",
            dirty_count,
            human(dirty_bytes)
        );

        if let Some(duration) = args.duration_secs {
            let rate = dirty_count as f64 / duration;
            println!(
                "  Write rate:     {:>8.1} blocks/sec ({}/sec at --duration-secs {:.0})",
                rate,
                human((rate * BLOCK_SIZE as f64) as usize),
                duration,
            );
        }
        println!();

        // === Spatial clustering ===
        let buckets = spatial_clustering(&churn.dirty_indices);
        if !buckets.is_empty() {
            println!("  Spatial Clustering ({} dirty blocks):", dirty_count);
            println!(
                "  ┌─────────────────┬────────┬────────┬──────────┐"
            );
            println!(
                "  │ Run Length       │  Runs  │ Blocks │ % Blocks │"
            );
            println!(
                "  ├─────────────────┼────────┼────────┼──────────┤"
            );
            for b in &buckets {
                if b.count > 0 {
                    println!(
                        "  │ {:>15} │ {:>6} │ {:>6} │ {:>7.1}% │",
                        b.label,
                        b.count,
                        b.blocks,
                        b.blocks as f64 / dirty_count as f64 * 100.0,
                    );
                }
            }
            println!(
                "  └─────────────────┴────────┴────────┴──────────┘"
            );
            println!();
        }

        // === Pack simulation ===
        if dirty_count > 0 {
            println!("  Pack Simulation ({} dirty blocks):", dirty_count);
            println!("  ┌─────────────┬───────┬──────────┬─────────┐");
            println!("  │ Blocks/Pack │ Packs │ Avg Fill │ S3 PUTs │");
            println!("  ├─────────────┼───────┼──────────┼─────────┤");
            for &ps in &pack_sizes {
                let packs = dirty_count.div_ceil(ps);
                let avg_fill = dirty_count as f64 / packs as f64;
                println!(
                    "  │ {:>11} │ {:>5} │ {:>8.1} │ {:>7} │",
                    ps, packs, avg_fill, packs,
                );
            }
            println!("  └─────────────┴───────┴──────────┴─────────┘");
            println!();
        }
    }
}
