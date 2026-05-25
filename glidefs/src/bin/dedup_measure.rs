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
    raw_bytes: usize,
    compressed_bytes: usize,
    deduped_raw_bytes: usize,
    deduped_compressed_bytes: usize,
}

#[allow(dead_code)]
struct ImageStats {
    name: String,
    total: usize,
    unique: usize,
    zero: usize,
    size_bytes: u64,
}

fn analyze(images: &[PathBuf], block_size: usize, skip_zeros: bool) -> Results {
    let mut global: HashMap<[u8; 16], u32> = HashMap::new();
    // Track compressed size for each unique block (first occurrence wins).
    let mut unique_compressed: HashMap<[u8; 16], usize> = HashMap::new();
    let mut per_image = Vec::new();
    let mut total_zero = 0usize;
    let mut raw_bytes = 0usize;
    let mut compressed_bytes = 0usize;
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

            let compressed = lz4_flex::compress_prepend_size(data);
            raw_bytes += block_size;
            compressed_bytes += compressed.len();

            let hash = blake3_128(data);
            unique_compressed.entry(hash).or_insert(compressed.len());
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
    let deduped_raw_bytes = global.len() * block_size;
    let deduped_compressed_bytes: usize = unique_compressed.values().sum();

    Results {
        block_size,
        per_image,
        global_unique: global.len(),
        global_total_nonzero,
        total_zero,
        raw_bytes,
        compressed_bytes,
        deduped_raw_bytes,
        deduped_compressed_bytes,
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
// Block map compression analysis
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct BlockMapStats {
    image_name: String,
    image_size: u64,
    block_size: usize,
    total_slots: usize,
    nonzero_slots: usize,
    zero_slots: usize,
    // Dense format: flat array of 17-byte entries (hash + flags) for every slot
    dense_raw: usize,
    dense_lz4: usize,
    // Sparse format: only non-zero entries, 25 bytes each (8-byte index + 16-byte hash + 1-byte flags)
    sparse_raw: usize,
    sparse_lz4: usize,
}

fn measure_block_maps(images: &[PathBuf], block_size: usize) -> Vec<BlockMapStats> {
    let zero_hash = blake3_128(&vec![0u8; block_size]);
    let mut results = Vec::new();
    let mut buf = vec![0u8; block_size];

    for path in images {
        let file = File::open(path).unwrap();
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);

        // Build ordered list of hashes (dense block map)
        let mut hashes: Vec<[u8; 16]> = Vec::new();
        let mut nonzero = 0usize;

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
            let hash = blake3_128(&buf[..block_size]);
            if hash != zero_hash {
                nonzero += 1;
            }
            hashes.push(hash);
        }

        let total_slots = hashes.len();
        let zero_slots = total_slots - nonzero;

        // Dense format: 17 bytes per slot (hash + 1 byte flags)
        let mut dense_bytes = Vec::with_capacity(total_slots * 17);
        for h in &hashes {
            dense_bytes.extend_from_slice(h);
            dense_bytes.push(0u8); // flags
        }
        let dense_lz4 = lz4_flex::compress_prepend_size(&dense_bytes);

        // Sparse format: only non-zero entries, 25 bytes each (u64 index + hash + flags)
        let mut sparse_bytes = Vec::new();
        for (i, h) in hashes.iter().enumerate() {
            if *h != zero_hash {
                sparse_bytes.extend_from_slice(&(i as u64).to_le_bytes());
                sparse_bytes.extend_from_slice(h);
                sparse_bytes.push(0u8);
            }
        }
        let sparse_lz4 = lz4_flex::compress_prepend_size(&sparse_bytes);

        results.push(BlockMapStats {
            image_name: path.file_name().unwrap_or_default().to_string_lossy().to_string(),
            image_size: size,
            block_size,
            total_slots,
            nonzero_slots: nonzero,
            zero_slots,
            dense_raw: dense_bytes.len(),
            dense_lz4: dense_lz4.len(),
            sparse_raw: sparse_bytes.len(),
            sparse_lz4: sparse_lz4.len(),
        });
    }

    results
}

// Measure post-fork overlay: blocks that differ between base and forked images
#[allow(dead_code)]
struct ForkOverlayStats {
    fork_name: String,
    block_size: usize,
    base_nonzero: usize,
    fork_nonzero: usize,
    changed_blocks: usize,   // blocks present in both but different hash
    new_blocks: usize,       // blocks non-zero in fork but zero in base
    deleted_blocks: usize,   // blocks zero in fork but non-zero in base
    overlay_entries: usize,  // total entries in overlay (changed + new)
    overlay_raw: usize,      // raw bytes for overlay (25 bytes per entry)
    overlay_lz4: usize,      // compressed overlay
}

fn measure_fork_overlays(
    images: &[PathBuf],
    block_size: usize,
) -> Vec<ForkOverlayStats> {
    let zero_hash = blake3_128(&vec![0u8; block_size]);

    // Read all images into hash arrays
    let mut all_hashes: Vec<(String, Vec<[u8; 16]>)> = Vec::new();
    let mut buf = vec![0u8; block_size];

    for path in images {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);
        let mut hashes = Vec::new();

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
            hashes.push(blake3_128(&buf[..block_size]));
        }

        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        all_hashes.push((name, hashes));
    }

    if all_hashes.len() < 2 {
        return Vec::new();
    }

    // Use first image as base, compare each subsequent image
    let base = &all_hashes[0];
    let mut results = Vec::new();

    for fork in &all_hashes[1..] {
        let len = base.1.len().min(fork.1.len());
        let mut changed = 0usize;
        let mut new_blocks = 0usize;
        let mut deleted = 0usize;
        let mut overlay_bytes = Vec::new();

        let base_nonzero = base.1.iter().filter(|h| **h != zero_hash).count();
        let fork_nonzero = fork.1.iter().filter(|h| **h != zero_hash).count();

        for i in 0..len {
            let b = base.1[i];
            let f = fork.1[i];
            if b == f {
                continue;
            }
            if b == zero_hash && f != zero_hash {
                new_blocks += 1;
            } else if b != zero_hash && f == zero_hash {
                deleted += 1;
            } else {
                changed += 1;
            }
            // Overlay entry for anything that differs (changed or new)
            if f != zero_hash {
                overlay_bytes.extend_from_slice(&(i as u64).to_le_bytes());
                overlay_bytes.extend_from_slice(&f);
                overlay_bytes.push(0u8);
            }
        }
        // Handle fork being longer than base
        for i in len..fork.1.len() {
            if fork.1[i] != zero_hash {
                new_blocks += 1;
                overlay_bytes.extend_from_slice(&(i as u64).to_le_bytes());
                overlay_bytes.extend_from_slice(&fork.1[i]);
                overlay_bytes.push(0u8);
            }
        }

        let overlay_entries = changed + new_blocks;
        let overlay_lz4 = lz4_flex::compress_prepend_size(&overlay_bytes);

        results.push(ForkOverlayStats {
            fork_name: fork.0.clone(),
            block_size,
            base_nonzero,
            fork_nonzero,
            changed_blocks: changed,
            new_blocks,
            deleted_blocks: deleted,
            overlay_entries,
            overlay_raw: overlay_bytes.len(),
            overlay_lz4: overlay_lz4.len(),
        });
    }

    results
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

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
    println!("┌──────────┬──────────┬──────────┬──────────┬─────────┬───────────────┬──────────────┬──────────────┐");
    println!("│ Block    │ Total    │ Unique   │ Dedup    │ Zero    │ Block Map     │ LZ4 Ratio    │ Dedup+LZ4    │");
    println!("│ Size     │ Blocks   │ Blocks   │ Ratio    │ Blocks  │ (per 10GB VM) │ (all blocks) │ (unique only)│");
    println!("├──────────┼──────────┼──────────┼──────────┼─────────┼───────────────┼──────────────┼──────────────┤");

    for r in &all_results {
        let dedup = if r.global_unique > 0 {
            r.global_total_nonzero as f64 / r.global_unique as f64
        } else {
            0.0
        };
        let bmap = (10u64 * 1024 * 1024 * 1024 / r.block_size as u64 * 17) as usize;
        let lz4_ratio = if r.compressed_bytes > 0 {
            r.raw_bytes as f64 / r.compressed_bytes as f64
        } else {
            0.0
        };
        let dedup_lz4_ratio = if r.deduped_compressed_bytes > 0 {
            r.raw_bytes as f64 / r.deduped_compressed_bytes as f64
        } else {
            0.0
        };

        println!(
            "│ {:>8} │ {:>8} │ {:>8} │ {:>7.2}x │ {:>7} │ {:>13} │ {:>11.2}x │ {:>11.2}x │",
            human(r.block_size), r.global_total_nonzero, r.global_unique,
            dedup, r.total_zero, human(bmap), lz4_ratio, dedup_lz4_ratio,
        );
    }
    println!("└──────────┴──────────┴──────────┴──────────┴─────────┴───────────────┴──────────────┴──────────────┘");

    // Compression detail.
    println!();
    println!("Storage breakdown:");
    for r in &all_results {
        println!(
            "  {:>6}:  raw {} → LZ4 {} (all) | deduped raw {} → deduped+LZ4 {}",
            human(r.block_size),
            human(r.raw_bytes),
            human(r.compressed_bytes),
            human(r.deduped_raw_bytes),
            human(r.deduped_compressed_bytes),
        );
    }


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

    // Block map compression analysis.
    println!();
    println!("Block map compression (per image):");
    let mut all_bm_stats: Vec<Vec<BlockMapStats>> = Vec::new();
    for &bs in &block_sizes {
        let t = Instant::now();
        eprint!("  block maps at {} ...", human(bs));
        let bm_stats = measure_block_maps(&args.images, bs);
        eprintln!(" {:.1}s", t.elapsed().as_secs_f64());

        println!("  [{}]", human(bs));
        println!("    {:>20} │ {:>8} │ {:>8} │ {:>10} │ {:>10} │ {:>10} │ {:>10} │ {:>10}",
            "image", "slots", "nonzero", "dense raw", "dense lz4", "sparse raw", "sparse lz4", "10GB extrap");

        for s in &bm_stats {
            // Extrapolate to 10GB VM: scale by (10GB / image_size)
            let scale = if s.image_size > 0 {
                (10u64 * 1024 * 1024 * 1024) as f64 / s.image_size as f64
            } else {
                1.0
            };
            let extrap_sparse_lz4 = (s.sparse_lz4 as f64 * scale) as usize;

            println!("    {:>20} │ {:>8} │ {:>8} │ {:>10} │ {:>10} │ {:>10} │ {:>10} │ {:>10}",
                s.image_name, s.total_slots, s.nonzero_slots,
                human(s.dense_raw), human(s.dense_lz4),
                human(s.sparse_raw), human(s.sparse_lz4),
                human(extrap_sparse_lz4));
        }
        all_bm_stats.push(bm_stats);
    }

    // Block map size comparison table (summary across block sizes, first image only).
    println!();
    println!("Block map size summary (first image, extrapolated to 10GB VM):");
    println!("  {:>8} │ {:>12} │ {:>12} │ {:>12} │ {:>12} │ {:>10}",
        "blk size", "sparse raw", "sparse lz4", "dense raw", "dense lz4", "lz4 ratio");
    for bm_stats in &all_bm_stats {
        if let Some(s) = bm_stats.first() {
            let scale = if s.image_size > 0 {
                (10u64 * 1024 * 1024 * 1024) as f64 / s.image_size as f64
            } else {
                1.0
            };
            let sr = (s.sparse_raw as f64 * scale) as usize;
            let sl = (s.sparse_lz4 as f64 * scale) as usize;
            let dr = (s.dense_raw as f64 * scale) as usize;
            let dl = (s.dense_lz4 as f64 * scale) as usize;
            let ratio = if sl > 0 { sr as f64 / sl as f64 } else { 0.0 };
            println!("  {:>8} │ {:>12} │ {:>12} │ {:>12} │ {:>12} │ {:>9.1}x",
                human(s.block_size), human(sr), human(sl), human(dr), human(dl), ratio);
        }
    }

    // Fork overlay analysis.
    if args.images.len() > 1 {
        println!();
        println!("Fork overlay analysis (base = first image):");
        for &bs in &block_sizes {
            let t = Instant::now();
            eprint!("  fork overlays at {} ...", human(bs));
            let overlays = measure_fork_overlays(&args.images, bs);
            eprintln!(" {:.1}s", t.elapsed().as_secs_f64());

            println!("  [{}]", human(bs));
            for o in &overlays {
                let scale = if o.base_nonzero > 0 {
                    (10u64 * 1024 * 1024 * 1024 / bs as u64) as f64 / o.base_nonzero as f64
                } else {
                    1.0
                };
                let extrap_overlay_lz4 = (o.overlay_lz4 as f64 * scale) as usize;
                println!("    base → {}:", o.fork_name);
                println!("      changed: {}, new: {}, deleted: {}, overlay entries: {}",
                    o.changed_blocks, o.new_blocks, o.deleted_blocks, o.overlay_entries);
                println!("      overlay raw: {}, overlay lz4: {}, 10GB extrap lz4: {}",
                    human(o.overlay_raw), human(o.overlay_lz4), human(extrap_overlay_lz4));
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
