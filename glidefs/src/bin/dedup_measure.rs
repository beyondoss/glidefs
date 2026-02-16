/// Measure content-addressed dedup effectiveness at various block sizes.
///
/// Point this at real disk images or filesystem exports and it tells you
/// how block size affects dedup ratio, metadata cost, and cross-image sharing.
///
/// Usage:
///   dedup_measure image1.raw image2.raw
///   dedup_measure --block-sizes 4096,65536,131072 *.raw
///
/// Generate real test images with the companion script:
///   ./scripts/gen_dedup_images.sh /tmp/dedup-test
///   dedup_measure /tmp/dedup-test/*.raw
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;

#[derive(Parser)]
#[command(name = "dedup_measure", about = "Measure dedup effectiveness at various block sizes")]
struct Args {
    /// Raw disk/filesystem images to analyze.
    #[arg(required = true)]
    images: Vec<PathBuf>,

    /// Comma-separated block sizes in bytes.
    /// Defaults to: 4096,16384,32768,65536,131072,262144
    #[arg(long, value_delimiter = ',')]
    block_sizes: Option<Vec<usize>>,

    /// Skip zero blocks from dedup accounting.
    #[arg(long, default_value_t = true)]
    skip_zeros: bool,
}

const DEFAULT_BLOCK_SIZES: &[usize] = &[
    4_096,   // 4KB
    16_384,  // 16KB
    32_768,  // 32KB
    65_536,  // 64KB
    131_072, // 128KB - current default
    262_144, // 256KB
];

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

// ---------------------------------------------------------------------------
// Analysis
// ---------------------------------------------------------------------------

struct Results {
    block_size: usize,
    per_image: Vec<ImageStats>,
    global_unique: usize,
    global_total_nonzero: usize,
    total_zero: usize,
}

struct ImageStats {
    name: String,
    total: usize,
    unique: usize,
    zero: usize,
    size_bytes: u64,
}

fn analyze(images: &[PathBuf], block_size: usize, skip_zeros: bool) -> Results {
    let mut global: HashMap<[u8; 16], u32> = HashMap::new();
    let mut per_image = Vec::new();
    let mut total_zero = 0usize;
    let mut buf = vec![0u8; block_size];

    for path in images {
        let file = File::open(path).unwrap_or_else(|e| {
            eprintln!("error: {}: {e}", path.display());
            std::process::exit(1);
        });
        let size_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);
        let mut img_hashes: HashMap<[u8; 16], u32> = HashMap::new();
        let mut img_total = 0usize;
        let mut img_zero = 0usize;

        loop {
            buf.fill(0);
            let mut filled = 0;
            while filled < block_size {
                match reader.read(&mut buf[filled..block_size]) {
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

            img_total += 1;
            let data = &buf[..block_size];

            if skip_zeros && is_zero(data) {
                img_zero += 1;
                total_zero += 1;
                continue;
            }

            let hash = blake3_128(data);
            *img_hashes.entry(hash).or_insert(0) += 1;
            *global.entry(hash).or_insert(0) += 1;
        }

        per_image.push(ImageStats {
            name: path.file_name().unwrap_or_default().to_string_lossy().to_string(),
            total: img_total,
            unique: img_hashes.len(),
            zero: img_zero,
            size_bytes,
        });
    }

    let global_total_nonzero: usize = per_image.iter().map(|s| s.total - s.zero).sum();

    Results {
        block_size,
        per_image,
        global_unique: global.len(),
        global_total_nonzero,
        total_zero,
    }
}

fn cross_image_sets(
    images: &[PathBuf],
    block_size: usize,
    skip_zeros: bool,
) -> Vec<(String, HashSet<[u8; 16]>)> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; block_size];

    for path in images {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);
        let mut hashes = HashSet::new();

        loop {
            buf.fill(0);
            let mut filled = 0;
            while filled < block_size {
                match reader.read(&mut buf[filled..block_size]) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(_) => break,
                }
            }
            if filled == 0 {
                break;
            }
            let data = &buf[..block_size];
            if !(skip_zeros && is_zero(data)) {
                hashes.insert(blake3_128(data));
            }
        }

        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        out.push((name, hashes));
    }
    out
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

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
    let block_sizes = args.block_sizes.unwrap_or_else(|| DEFAULT_BLOCK_SIZES.to_vec());

    for &bs in &block_sizes {
        if bs == 0 || bs % 512 != 0 {
            eprintln!("error: block size {bs} must be a positive multiple of 512");
            std::process::exit(1);
        }
    }

    // Print image info.
    println!("Images:");
    for p in &args.images {
        let size = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        println!(
            "  {} ({})",
            p.file_name().unwrap_or_default().to_string_lossy(),
            human(size as usize),
        );
    }
    println!();

    // Main analysis.
    let mut all_results = Vec::new();
    for &bs in &block_sizes {
        let t = Instant::now();
        eprint!("  analyzing at {} ...", human(bs));
        let r = analyze(&args.images, bs, args.skip_zeros);
        eprintln!(" {:.1}s", t.elapsed().as_secs_f64());
        all_results.push(r);
    }

    // Summary table.
    println!();
    println!("┌──────────┬──────────┬──────────┬──────────┬─────────┬───────────────┬──────────────────┐");
    println!("│ Block    │ Total    │ Unique   │ Dedup    │ Zero    │ Block Map     │ Effective        │");
    println!("│ Size     │ Blocks   │ Blocks   │ Ratio    │ Blocks  │ (per 10GB VM) │ Storage (unique) │");
    println!("├──────────┼──────────┼──────────┼──────────┼─────────┼───────────────┼──────────────────┤");

    for r in &all_results {
        let dedup = if r.global_unique > 0 {
            r.global_total_nonzero as f64 / r.global_unique as f64
        } else {
            0.0
        };
        let bmap = 10usize * 1024 * 1024 * 1024 / r.block_size * 17;
        let eff = r.global_unique * r.block_size;

        println!(
            "│ {:>8} │ {:>8} │ {:>8} │ {:>7.2}x │ {:>7} │ {:>13} │ {:>16} │",
            human(r.block_size), r.global_total_nonzero, r.global_unique,
            dedup, r.total_zero, human(bmap), human(eff),
        );
    }
    println!("└──────────┴──────────┴──────────┴──────────┴─────────┴───────────────┴──────────────────┘");

    // Per-image stats.
    if all_results.iter().any(|r| r.per_image.len() > 1) {
        println!();
        println!("Per-image unique (non-zero) blocks at each block size:");
        if let Some(r) = all_results.first() {
            print!("  {:>20}", "");
            for res in &all_results {
                print!(" │ {:>8}", human(res.block_size));
            }
            println!();

            for (i, img) in r.per_image.iter().enumerate() {
                print!("  {:>20}", img.name);
                for res in &all_results {
                    print!(" │ {:>8}", res.per_image[i].unique);
                }
                println!();
            }
        }
    }

    // Cross-image dedup.
    if args.images.len() > 1 {
        println!();
        println!("Cross-image sharing (vm-0 vs others):");

        for &bs in &block_sizes {
            let sets = cross_image_sets(&args.images, bs, args.skip_zeros);
            println!("  [{}]", human(bs));

            for j in 1..sets.len().min(6) {
                let shared = sets[0].1.intersection(&sets[j].1).count();
                let union = sets[0].1.union(&sets[j].1).count();
                let jaccard = if union > 0 {
                    100.0 * shared as f64 / union as f64
                } else {
                    0.0
                };
                println!(
                    "    <-> {}: {shared} shared ({jaccard:.1}% Jaccard), saves {}",
                    sets[j].0, human(shared * bs),
                );
            }
        }
    }
}
