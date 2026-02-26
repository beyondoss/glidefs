/// Empirical pack size analysis using real disk images.
///
/// Reads 128KB blocks from raw disk images (same images used by dedup_measure),
/// then sweeps BLOCKS_PER_PACK values and measures actual compression ratios,
/// assembly timings, and prefetch/extract costs with real data.
///
/// Usage:
///   cargo run --release --bin pack_size_measure -- /tmp/glidefs-dedup-test/forked/*.raw
///   cargo run --release --bin pack_size_measure -- --pack-sizes 25,100,500 /tmp/glidefs-dedup-test/forked/*.raw
///
/// Generate test images first:
///   ./scripts/gen_dedup_images.sh /tmp/glidefs-dedup-test
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;

use glidefs::block::block_map::{Blake3Hash, blake3_128, lz4_compress, lz4_decompress};
use glidefs::block::pack::{
    PACK_HEADER_SIZE, PACK_INDEX_ENTRY_SIZE, assemble_pack, extract_block, parse_pack_index,
};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB
const WARMUP_ITERS: usize = 2;
const MEASURE_ITERS: usize = 10;

// S3 transfer model (conservative same-region estimates)
const S3_FIXED_LATENCY_MS: f64 = 20.0;
const S3_BANDWIDTH_MBPS: f64 = 100.0; // MB/s per connection

const DEFAULT_PACK_SIZES: &[usize] = &[10, 25, 50, 100, 200, 500, 1000];

#[derive(Parser)]
#[command(
    name = "pack_size_measure",
    about = "Empirical pack size analysis using real disk images"
)]
struct Args {
    /// Raw disk/filesystem images to read blocks from.
    #[arg(required = true)]
    images: Vec<PathBuf>,

    /// Comma-separated pack sizes (blocks per pack) to test.
    #[arg(long, value_delimiter = ',')]
    pack_sizes: Option<Vec<usize>>,

    /// Number of measurement iterations (default 10).
    #[arg(long, default_value_t = MEASURE_ITERS)]
    iters: usize,

    /// S3 fixed latency in ms (default 20).
    #[arg(long, default_value_t = S3_FIXED_LATENCY_MS)]
    s3_latency_ms: f64,

    /// S3 bandwidth in MB/s (default 100).
    #[arg(long, default_value_t = S3_BANDWIDTH_MBPS)]
    s3_bandwidth_mbps: f64,

    /// Skip zero blocks (default true).
    #[arg(long, default_value_t = true)]
    skip_zeros: bool,

    /// Max blocks to read from all images combined (0 = no limit).
    #[arg(long, default_value_t = 0)]
    max_blocks: usize,
}

fn is_zero(data: &[u8]) -> bool {
    let (prefix, chunks, suffix) = unsafe { data.align_to::<u64>() };
    prefix.iter().all(|&b| b == 0)
        && chunks.iter().all(|&w| w == 0)
        && suffix.iter().all(|&b| b == 0)
}

/// Read all non-zero 128KB blocks from disk images.
fn read_blocks(images: &[PathBuf], skip_zeros: bool, max_blocks: usize) -> Vec<Vec<u8>> {
    let mut blocks = Vec::new();
    let mut buf = vec![0u8; BLOCK_SIZE];

    for path in images {
        let file = File::open(path).unwrap_or_else(|e| {
            eprintln!("error: {}: {e}", path.display());
            std::process::exit(1);
        });
        let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);

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

            if skip_zeros && is_zero(&buf[..BLOCK_SIZE]) {
                continue;
            }

            blocks.push(buf[..BLOCK_SIZE].to_vec());

            if max_blocks > 0 && blocks.len() >= max_blocks {
                return blocks;
            }
        }
    }

    blocks
}

struct PackMeasurement {
    blocks_per_pack: usize,
    num_packs: usize,
    // Sizes (averaged across all packs)
    avg_compressed_pack_bytes: usize,
    min_compressed_pack_bytes: usize,
    max_compressed_pack_bytes: usize,
    index_overhead_bytes: usize,
    avg_lz4_ratio: f64,
    // Timings (median of iters, in microseconds)
    assembly_us: f64,       // hash + compress + assemble_pack (per pack, median)
    prefetch_us: f64,       // parse_pack_index + extract + decompress all blocks (per pack, median)
    single_extract_us: f64, // extract + decompress 1 block (median)
    // Derived S3 estimates
    s3_get_ms: f64,
    amortized_per_block_ms: f64,
    // Memory
    peak_memory_mb: f64,
}

fn measure_pack_size(
    all_blocks: &[Vec<u8>],
    blocks_per_pack: usize,
    iters: usize,
    s3_latency_ms: f64,
    s3_bandwidth_mbps: f64,
) -> PackMeasurement {
    // Split blocks into packs (drop remainder)
    let num_packs = all_blocks.len() / blocks_per_pack;
    if num_packs == 0 {
        eprintln!(
            "warning: only {} blocks available, need at least {} for this pack size",
            all_blocks.len(),
            blocks_per_pack
        );
        return PackMeasurement {
            blocks_per_pack,
            num_packs: 0,
            avg_compressed_pack_bytes: 0,
            min_compressed_pack_bytes: 0,
            max_compressed_pack_bytes: 0,
            index_overhead_bytes: 0,
            avg_lz4_ratio: 0.0,
            assembly_us: 0.0,
            prefetch_us: 0.0,
            single_extract_us: 0.0,
            s3_get_ms: 0.0,
            amortized_per_block_ms: 0.0,
            peak_memory_mb: 0.0,
        };
    }

    let packs: Vec<&[Vec<u8>]> = (0..num_packs)
        .map(|i| &all_blocks[i * blocks_per_pack..(i + 1) * blocks_per_pack])
        .collect();

    // Pre-compute hashes + compressed data for each pack (with synthetic chunk_offsets)
    let prepared_packs: Vec<Vec<(Blake3Hash, u32, Vec<u8>)>> = packs
        .iter()
        .map(|pack_blocks| {
            pack_blocks
                .iter()
                .enumerate()
                .map(|(i, data)| {
                    let hash = blake3_128(data);
                    let compressed = lz4_compress(data);
                    (hash, i as u32, compressed)
                })
                .collect()
        })
        .collect();

    // Build reference assembled packs for size + prefetch measurements
    let assembled_packs: Vec<Vec<u8>> = prepared_packs
        .iter()
        .map(|prepared| {
            let (pack_bytes, _entries) =
                assemble_pack(prepared.clone(), blocks_per_pack as u32).unwrap();
            pack_bytes
        })
        .collect();

    // Size stats
    let pack_sizes: Vec<usize> = assembled_packs.iter().map(|p| p.len()).collect();
    let avg_compressed_pack_bytes = pack_sizes.iter().sum::<usize>() / pack_sizes.len();
    let min_compressed_pack_bytes = *pack_sizes.iter().min().unwrap();
    let max_compressed_pack_bytes = *pack_sizes.iter().max().unwrap();
    let index_overhead_bytes = PACK_HEADER_SIZE + blocks_per_pack * PACK_INDEX_ENTRY_SIZE;
    let raw_total = num_packs * blocks_per_pack * BLOCK_SIZE;
    let compressed_total: usize = pack_sizes.iter().sum();
    let avg_lz4_ratio = raw_total as f64 / compressed_total as f64;

    // --- Measure assembly (hash + compress + assemble) ---
    // Pick a subset of packs (up to 5) to keep timing reasonable for large sweeps
    let bench_packs: Vec<usize> = (0..num_packs.min(5)).collect();

    // Warmup
    for &pi in &bench_packs {
        for _ in 0..WARMUP_ITERS {
            let prepped: Vec<(Blake3Hash, u32, Vec<u8>)> = packs[pi]
                .iter()
                .enumerate()
                .map(|(i, data)| (blake3_128(data), i as u32, lz4_compress(data)))
                .collect();
            let (pack, _) = assemble_pack(prepped, blocks_per_pack as u32).unwrap();
            std::hint::black_box(&pack);
        }
    }

    let mut assembly_times = Vec::with_capacity(iters * bench_packs.len());
    for _ in 0..iters {
        for &pi in &bench_packs {
            let start = Instant::now();
            let prepped: Vec<(Blake3Hash, u32, Vec<u8>)> = packs[pi]
                .iter()
                .enumerate()
                .map(|(i, data)| (blake3_128(data), i as u32, lz4_compress(data)))
                .collect();
            let (_pack, _entries) = assemble_pack(prepped, blocks_per_pack as u32).unwrap();
            assembly_times.push(start.elapsed().as_micros() as f64);
        }
    }
    assembly_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // --- Measure full-pack prefetch (parse + extract + decompress all) ---
    // Warmup
    for &pi in &bench_packs {
        for _ in 0..WARMUP_ITERS {
            let pack_bytes = &assembled_packs[pi];
            let idx = parse_pack_index(pack_bytes).unwrap();
            for entry in &idx.entries {
                let compressed =
                    extract_block(pack_bytes, entry.offset, entry.comp_length).unwrap();
                let decompressed = lz4_decompress(compressed).unwrap();
                std::hint::black_box(&decompressed);
            }
        }
    }

    let mut prefetch_times = Vec::with_capacity(iters * bench_packs.len());
    for _ in 0..iters {
        for &pi in &bench_packs {
            let pack_bytes = &assembled_packs[pi];
            let start = Instant::now();
            let idx = parse_pack_index(pack_bytes).unwrap();
            for entry in &idx.entries {
                let compressed =
                    extract_block(pack_bytes, entry.offset, entry.comp_length).unwrap();
                let decompressed = lz4_decompress(compressed).unwrap();
                std::hint::black_box(&decompressed);
            }
            prefetch_times.push(start.elapsed().as_micros() as f64);
        }
    }
    prefetch_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // --- Measure single-block extract ---
    let pack_bytes = &assembled_packs[0];
    let idx = parse_pack_index(pack_bytes).unwrap();
    let mid = idx.entries.len() / 2;
    let target = &idx.entries[mid];

    // Warmup
    for _ in 0..WARMUP_ITERS {
        let compressed = extract_block(pack_bytes, target.offset, target.comp_length).unwrap();
        let decompressed = lz4_decompress(compressed).unwrap();
        std::hint::black_box(&decompressed);
    }

    let mut single_times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let compressed = extract_block(pack_bytes, target.offset, target.comp_length).unwrap();
        let decompressed = lz4_decompress(compressed).unwrap();
        std::hint::black_box(&decompressed);
        single_times.push(start.elapsed().as_micros() as f64);
    }
    single_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // S3 transfer estimate (based on average pack size)
    let s3_transfer_ms =
        avg_compressed_pack_bytes as f64 / (s3_bandwidth_mbps * 1024.0 * 1024.0) * 1000.0;
    let s3_get_ms = s3_latency_ms + s3_transfer_ms;
    let amortized_per_block_ms = s3_get_ms / blocks_per_pack as f64;

    // Peak memory per pack during assembly (owned: output buffer only, input freed as consumed)
    let peak_memory_mb = avg_compressed_pack_bytes as f64 / (1024.0 * 1024.0);

    PackMeasurement {
        blocks_per_pack,
        num_packs,
        avg_compressed_pack_bytes,
        min_compressed_pack_bytes,
        max_compressed_pack_bytes,
        index_overhead_bytes,
        avg_lz4_ratio,
        assembly_us: assembly_times[assembly_times.len() / 2],
        prefetch_us: prefetch_times[prefetch_times.len() / 2],
        single_extract_us: single_times[single_times.len() / 2],
        s3_get_ms,
        amortized_per_block_ms,
        peak_memory_mb,
    }
}

fn human(bytes: usize) -> String {
    if bytes >= 1 << 20 {
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
    let iters = args.iters;

    // Print image info
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

    // Read all non-zero blocks from images
    eprint!("  reading blocks from {} image(s)...", args.images.len());
    let t = Instant::now();
    let all_blocks = read_blocks(&args.images, args.skip_zeros, args.max_blocks);
    eprintln!(
        " {} non-zero blocks ({}) in {:.1}s",
        all_blocks.len(),
        human(all_blocks.len() * BLOCK_SIZE),
        t.elapsed().as_secs_f64()
    );

    if all_blocks.is_empty() {
        eprintln!("error: no non-zero blocks found in images");
        std::process::exit(1);
    }

    // Show compression ratio sample (first 100 blocks)
    let sample_count = all_blocks.len().min(100);
    let sample_raw: usize = sample_count * BLOCK_SIZE;
    let sample_compressed: usize = all_blocks[..sample_count]
        .iter()
        .map(|b| lz4_compress(b).len())
        .sum();
    println!(
        "LZ4 compression (sample of {} blocks): {:.2}x ({} -> {})",
        sample_count,
        sample_raw as f64 / sample_compressed as f64,
        human(sample_raw),
        human(sample_compressed),
    );

    let max_pack_size = *pack_sizes.iter().max().unwrap();
    println!(
        "Available: {} blocks, largest pack: {} blocks ({} packs)",
        all_blocks.len(),
        max_pack_size,
        all_blocks.len() / max_pack_size,
    );
    println!();

    println!("Pack Size Analysis");
    println!("==================");
    println!("Block size: {}KB", BLOCK_SIZE / 1024);
    println!("Iterations: {iters}");
    println!(
        "S3 model: {:.0}ms fixed + {:.0} MB/s bandwidth",
        args.s3_latency_ms, args.s3_bandwidth_mbps
    );
    println!("Data: real filesystem blocks from disk images");
    println!();

    let mut results = Vec::new();
    for &pack_size in &pack_sizes {
        eprint!("  measuring {pack_size} blocks/pack ...");
        let m = measure_pack_size(
            &all_blocks,
            pack_size,
            iters,
            args.s3_latency_ms,
            args.s3_bandwidth_mbps,
        );
        if m.num_packs > 0 {
            eprintln!(
                " {} packs, avg={}, ratio={:.2}x, assembly={:.0}us, prefetch={:.0}us",
                m.num_packs,
                human(m.avg_compressed_pack_bytes),
                m.avg_lz4_ratio,
                m.assembly_us,
                m.prefetch_us
            );
        } else {
            eprintln!(" skipped (not enough blocks)");
        }
        results.push(m);
    }

    // Filter out pack sizes with no data
    let results: Vec<&PackMeasurement> = results.iter().filter(|m| m.num_packs > 0).collect();

    // === Size Table ===
    println!();
    println!("Pack Size vs Compressed Size");
    println!(
        "┌────────────┬──────────┬──────────────┬──────────────┬──────────────┬───────────┬─────────────┬────────────┐"
    );
    println!(
        "│ Blocks/Pack│ # Packs  │ Raw Size     │ Avg Compress │ Min/Max      │ LZ4 Ratio │ Index OH    │ OH %       │"
    );
    println!(
        "├────────────┼──────────┼──────────────┼──────────────┼──────────────┼───────────┼─────────────┼────────────┤"
    );
    for m in &results {
        let raw = m.blocks_per_pack * BLOCK_SIZE;
        let oh_pct = m.index_overhead_bytes as f64 / m.avg_compressed_pack_bytes as f64 * 100.0;
        println!(
            "│ {:>10} │ {:>8} │ {:>12} │ {:>12} │ {:>5}/{:<5} │ {:>8.2}x │ {:>11} │ {:>9.2}% │",
            m.blocks_per_pack,
            m.num_packs,
            human(raw),
            human(m.avg_compressed_pack_bytes),
            human(m.min_compressed_pack_bytes),
            human(m.max_compressed_pack_bytes),
            m.avg_lz4_ratio,
            human(m.index_overhead_bytes),
            oh_pct,
        );
    }
    println!(
        "└────────────┴──────────┴──────────────┴──────────────┴──────────────┴───────────┴─────────────┴────────────┘"
    );

    // === Timing Table ===
    println!();
    println!("Assembly & Decompress Timings (median)");
    println!(
        "┌────────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐"
    );
    println!(
        "│ Blocks/Pack│ Assembly     │ Asm/Block    │ Prefetch All │ Per-Block    │ Single Block │"
    );
    println!(
        "│            │ (hash+lz4+  │              │ (parse+      │ (prefetch/N) │ (extract+    │"
    );
    println!(
        "│            │  assemble)   │              │  decomp all) │              │  decompress) │"
    );
    println!(
        "├────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────┤"
    );
    for m in &results {
        let per_block_asm = m.assembly_us / m.blocks_per_pack as f64;
        let per_block_prefetch = m.prefetch_us / m.blocks_per_pack as f64;
        println!(
            "│ {:>10} │ {:>10.0}us │ {:>10.1}us │ {:>10.0}us │ {:>10.1}us │ {:>10.1}us │",
            m.blocks_per_pack,
            m.assembly_us,
            per_block_asm,
            m.prefetch_us,
            per_block_prefetch,
            m.single_extract_us,
        );
    }
    println!(
        "└────────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘"
    );

    // === S3 Transfer Estimate ===
    println!();
    println!(
        "S3 Prefetch Estimate ({:.0}ms + size/{:.0}MB/s)",
        args.s3_latency_ms, args.s3_bandwidth_mbps
    );
    println!("┌────────────┬──────────────┬──────────────┬──────────────┬──────────────┐");
    println!("│ Blocks/Pack│ Transfer     │ S3 GET Total │ Amortized    │ Total w/     │");
    println!("│            │ (size/bw)    │ (fixed+xfer) │ per Block    │ Decompress   │");
    println!("├────────────┼──────────────┼──────────────┼──────────────┼──────────────┤");
    for m in &results {
        let xfer_ms = m.avg_compressed_pack_bytes as f64
            / (args.s3_bandwidth_mbps * 1024.0 * 1024.0)
            * 1000.0;
        let total_with_decomp = m.s3_get_ms + m.prefetch_us / 1000.0;
        println!(
            "│ {:>10} │ {:>9.1}ms │ {:>9.1}ms │ {:>9.2}ms │ {:>9.1}ms │",
            m.blocks_per_pack, xfer_ms, m.s3_get_ms, m.amortized_per_block_ms, total_with_decomp,
        );
    }
    println!("└────────────┴──────────────┴──────────────┴──────────────┴──────────────┘");

    // === Cost Table (at scale) ===
    // Model: event-driven flush (1 pack per flush), 1000 VMs,
    // moderate app server writing ~50 unique blocks/sec.
    println!();
    let unique_blocks_per_sec = 5.0_f64;
    let vms = 1000.0_f64;
    let unique_blocks_per_day = unique_blocks_per_sec * 86400.0;
    println!(
        "S3 PUT Cost ({:.0} VMs, {:.0} unique blocks/sec per VM)",
        vms, unique_blocks_per_sec
    );
    println!("┌────────────┬──────────────┬──────────────┬──────────────┐");
    println!("│ Blocks/Pack│ PUTs/VM/day  │ PUTs/Mo (1K) │ PUT Cost/Mo  │");
    println!("├────────────┼──────────────┼──────────────┼──────────────┤");
    for m in &results {
        let puts_per_vm_day = unique_blocks_per_day / m.blocks_per_pack as f64;
        let puts_per_month = puts_per_vm_day * 30.0 * vms;
        let cost = puts_per_month * 0.005 / 1000.0;
        println!(
            "│ {:>10} │ {:>12.0} │ {:>11.1}M │ ${:>10.0} │",
            m.blocks_per_pack,
            puts_per_vm_day,
            puts_per_month / 1e6,
            cost,
        );
    }
    println!("└────────────┴──────────────┴──────────────┴──────────────┘");

    // === Memory Table ===
    println!();
    println!("Memory During Pack Assembly");
    println!("┌────────────┬──────────────┐");
    println!("│ Blocks/Pack│ Peak Memory  │");
    println!("├────────────┼──────────────┤");
    for m in &results {
        println!(
            "│ {:>10} │ {:>10.1}MB │",
            m.blocks_per_pack, m.peak_memory_mb,
        );
    }
    println!("└────────────┴──────────────┘");
}
