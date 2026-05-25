/// Measure block-level churn between a base image and forked images.
///
/// Compares 128KB blocks by BLAKE3 hash to produce empirical data on:
/// - Unique block write volume per fork
/// - Spatial clustering of changed blocks (GC cohort quality)
/// - Sub-block write density (bytes actually changed within each dirty block)
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
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

use clap::Parser;

const BLOCK_SIZE: usize = 128 * 1024; // 128KB
const PAGE_SIZE: usize = 4096; // 4KB sub-block granularity
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

/// Sub-block write density analysis for dirty blocks.
///
/// Reads the raw bytes of each dirty block from both images and compares
/// at byte and 4KB page granularity. This directly answers: "when a 128KB
/// block is dirty, how much of it actually changed?"
struct SubBlockStats {
    /// Total dirty blocks analyzed.
    total_blocks: usize,
    /// Per-block: number of bytes that differ.
    bytes_changed: Vec<usize>,
    /// Per-block: number of 4KB pages with at least one byte changed.
    pages_changed: Vec<usize>,
}

impl SubBlockStats {
    fn analyze(
        base_path: &PathBuf,
        fork_path: &PathBuf,
        dirty_indices: &[usize],
    ) -> Self {
        if dirty_indices.is_empty() {
            return Self {
                total_blocks: 0,
                bytes_changed: Vec::new(),
                pages_changed: Vec::new(),
            };
        }

        let mut base_file = BufReader::new(File::open(base_path).unwrap());
        let mut fork_file = BufReader::new(File::open(fork_path).unwrap());
        let mut base_buf = vec![0u8; BLOCK_SIZE];
        let mut fork_buf = vec![0u8; BLOCK_SIZE];

        let base_len = std::fs::metadata(base_path).unwrap().len() as usize;
        let fork_len = std::fs::metadata(fork_path).unwrap().len() as usize;

        let mut bytes_changed = Vec::with_capacity(dirty_indices.len());
        let mut pages_changed = Vec::with_capacity(dirty_indices.len());

        for &idx in dirty_indices {
            let offset = idx * BLOCK_SIZE;

            // Read base block (zeros if beyond file end)
            base_buf.fill(0);
            if offset < base_len {
                let readable = BLOCK_SIZE.min(base_len - offset);
                base_file.seek(SeekFrom::Start(offset as u64)).unwrap();
                base_file.read_exact(&mut base_buf[..readable]).unwrap();
            }

            // Read fork block (zeros if beyond file end)
            fork_buf.fill(0);
            if offset < fork_len {
                let readable = BLOCK_SIZE.min(fork_len - offset);
                fork_file.seek(SeekFrom::Start(offset as u64)).unwrap();
                fork_file.read_exact(&mut fork_buf[..readable]).unwrap();
            }

            // Count differing bytes
            let diff_bytes = base_buf
                .iter()
                .zip(fork_buf.iter())
                .filter(|(a, b)| a != b)
                .count();
            bytes_changed.push(diff_bytes);

            // Count 4KB pages with at least one byte changed
            let diff_pages = base_buf
                .chunks(PAGE_SIZE)
                .zip(fork_buf.chunks(PAGE_SIZE))
                .filter(|(a, b)| a != b)
                .count();
            pages_changed.push(diff_pages);
        }

        Self {
            total_blocks: dirty_indices.len(),
            bytes_changed,
            pages_changed,
        }
    }

    fn print(&self) {
        if self.total_blocks == 0 {
            return;
        }

        let pages_per_block = BLOCK_SIZE / PAGE_SIZE;

        // Compute summary stats
        let total_bytes_changed: usize = self.bytes_changed.iter().sum();
        let total_bytes_possible = self.total_blocks * BLOCK_SIZE;
        let avg_bytes = total_bytes_changed as f64 / self.total_blocks as f64;
        let avg_pct = avg_bytes / BLOCK_SIZE as f64 * 100.0;

        let total_pages_changed: usize = self.pages_changed.iter().sum();
        let total_pages_possible = self.total_blocks * pages_per_block;
        let avg_pages = total_pages_changed as f64 / self.total_blocks as f64;
        let avg_page_pct = avg_pages / pages_per_block as f64 * 100.0;

        // Histogram: what % of the block was changed?
        let mut density_buckets = [0usize; 6]; // 0-10%, 10-25%, 25-50%, 50-75%, 75-99%, 100%
        for &bc in &self.bytes_changed {
            let pct = bc as f64 / BLOCK_SIZE as f64 * 100.0;
            let bucket = if pct <= 0.0 {
                continue; // shouldn't happen for dirty blocks, but defensive
            } else if pct <= 10.0 {
                0
            } else if pct <= 25.0 {
                1
            } else if pct <= 50.0 {
                2
            } else if pct <= 75.0 {
                3
            } else if pct < 100.0 {
                4
            } else {
                5
            };
            density_buckets[bucket] += 1;
        }

        // Page-level histogram: how many 4KB pages changed per block?
        let mut page_buckets = [0usize; 5]; // 1, 2-4, 5-16, 17-31, 32 (all)
        for &pc in &self.pages_changed {
            let bucket = if pc == 0 {
                continue;
            } else if pc == 1 {
                0
            } else if pc <= 4 {
                1
            } else if pc <= 16 {
                2
            } else if pc < pages_per_block {
                3
            } else {
                4
            };
            page_buckets[bucket] += 1;
        }

        println!("  Sub-Block Write Density ({} dirty blocks):", self.total_blocks);
        println!();
        println!(
            "    Bytes changed:  {} of {} ({:.1}%)",
            human(total_bytes_changed),
            human(total_bytes_possible),
            total_bytes_changed as f64 / total_bytes_possible as f64 * 100.0,
        );
        println!(
            "    Avg per block:  {} of {} ({:.1}%)",
            human(avg_bytes as usize),
            human(BLOCK_SIZE),
            avg_pct,
        );
        println!(
            "    4KB pages:      {} of {} ({:.1}%)",
            total_pages_changed,
            total_pages_possible,
            total_pages_changed as f64 / total_pages_possible as f64 * 100.0,
        );
        println!(
            "    Avg pages/blk:  {:.1} of {} ({:.1}%)",
            avg_pages, pages_per_block, avg_page_pct,
        );
        println!();

        // Byte density histogram
        let labels = ["1-10%", "11-25%", "26-50%", "51-75%", "76-99%", "100%"];
        println!("    Byte density distribution:");
        println!("    ┌──────────┬────────┬──────────┐");
        println!("    │ Changed  │ Blocks │ % Blocks │");
        println!("    ├──────────┼────────┼──────────┤");
        for (i, &label) in labels.iter().enumerate() {
            if density_buckets[i] > 0 {
                println!(
                    "    │ {:>8} │ {:>6} │ {:>7.1}% │",
                    label,
                    density_buckets[i],
                    density_buckets[i] as f64 / self.total_blocks as f64 * 100.0,
                );
            }
        }
        println!("    └──────────┴────────┴──────────┘");
        println!();

        // Page density histogram
        let page_labels = [
            "1 page",
            "2-4 pages",
            "5-16 pages",
            "17-31 pages",
            "32 (all)",
        ];
        println!("    4KB page density distribution:");
        println!("    ┌─────────────┬────────┬──────────┐");
        println!("    │ Pages hit   │ Blocks │ % Blocks │");
        println!("    ├─────────────┼────────┼──────────┤");
        for (i, &label) in page_labels.iter().enumerate() {
            if page_buckets[i] > 0 {
                println!(
                    "    │ {:>11} │ {:>6} │ {:>7.1}% │",
                    label,
                    page_buckets[i],
                    page_buckets[i] as f64 / self.total_blocks as f64 * 100.0,
                );
            }
        }
        println!("    └─────────────┴────────┴──────────┘");
        println!();
    }
}

fn human(bytes: usize) -> String {
    if bytes >= 1 << 30 {
        format!("{:.1}GB", bytes as f64 / f64::from(1 << 30))
    } else if bytes >= 1 << 20 {
        format!("{:.1}MB", bytes as f64 / f64::from(1 << 20))
    } else if bytes >= 1 << 10 {
        format!("{:.1}KB", bytes as f64 / f64::from(1 << 10))
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

        // === Sub-block write density ===
        if dirty_count > 0 {
            eprint!("Analyzing sub-block density...");
            let sub_block = SubBlockStats::analyze(&args.base, fork_path, &churn.dirty_indices);
            eprintln!(" done");
            sub_block.print();
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
