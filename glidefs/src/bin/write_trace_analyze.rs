/// Analyze block-level write traces from GlideFS NBD handler.
///
/// Reads a binary trace file (produced by WriteTracer when enabled per-export)
/// and outputs:
///
/// 1. **Summary**: total writes, unique blocks, overwrite ratio, duration
/// 2. **Write rate timeline**: unique blocks/sec over configurable windows
/// 3. **Hot set analysis**: blocks by write frequency distribution
/// 4. **Overwrite ratio**: raw writes vs unique dirty blocks over time
/// 5. **Pack fill simulation**: at various blocks_per_pack, how many flushes?
/// 6. **Burst detection**: periods of high vs low write activity
///
/// Usage:
///   cargo run --release --bin write_trace_analyze -- trace.bin
///   cargo run --release --bin write_trace_analyze -- --window 10 --pack-sizes 100,500 trace.bin
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;

use clap::Parser;

use glidefs::block::write_trace::{TraceEntry, iter_entries, read_header};

#[derive(Parser)]
#[command(
    name = "write_trace_analyze",
    about = "Analyze block-level write traces from GlideFS"
)]
struct Args {
    /// Trace file to analyze.
    trace: PathBuf,

    /// Sliding window size in seconds for write rate timeline.
    #[arg(long, default_value = "10")]
    window: f64,

    /// Comma-separated pack sizes to simulate.
    #[arg(long, value_delimiter = ',')]
    pack_sizes: Option<Vec<usize>>,

    /// Show per-second timeline (can be verbose for long traces).
    #[arg(long)]
    timeline: bool,
}

fn main() {
    let args = Args::parse();
    let pack_sizes = args.pack_sizes.unwrap_or_else(|| vec![100, 200, 500, 1000]);

    let data = fs::read(&args.trace).unwrap_or_else(|e| {
        eprintln!("error: {}: {e}", args.trace.display());
        std::process::exit(1);
    });

    let header = read_header(&data).unwrap_or_else(|| {
        eprintln!("error: invalid trace file (bad magic or version)");
        std::process::exit(1);
    });

    let entries: Vec<TraceEntry> = iter_entries(&data).collect();

    if entries.is_empty() {
        println!(
            "Trace: {} (empty — no writes recorded)",
            args.trace.display()
        );
        return;
    }

    let duration_us = entries.last().unwrap().elapsed_us - entries.first().unwrap().elapsed_us;
    let duration_s = duration_us as f64 / 1_000_000.0;

    // =========================================================================
    // 1. Summary
    // =========================================================================

    let total_writes = entries.len();
    let total_blocks_touched: u64 = entries.iter().map(|e| e.span as u64).sum();

    // Unique blocks: each distinct block_index that was written
    let mut block_write_counts: HashMap<u32, u64> = HashMap::new();
    for e in &entries {
        for b in e.block_index..(e.block_index + e.span as u32) {
            *block_write_counts.entry(b).or_default() += 1;
        }
    }
    let unique_blocks = block_write_counts.len();
    let overwrite_ratio = total_blocks_touched as f64 / unique_blocks as f64;

    let writes_by_op = {
        let mut counts = [0u64; 3];
        for e in &entries {
            let op = (e.op as usize).min(2);
            counts[op] += 1;
        }
        counts
    };

    println!("Trace: {}", args.trace.display());
    println!("  Export:         {}", header.export_name);
    println!("  Block size:     {}KB", header.block_size / 1024);
    println!(
        "  Device:         {} blocks ({})",
        header.device_blocks,
        human(header.device_blocks as usize * header.block_size as usize)
    );
    println!("  Duration:       {:.1}s", duration_s);
    println!();
    println!("  Total ops:      {}", total_writes);
    println!("    Writes:       {}", writes_by_op[0]);
    println!("    Trims:        {}", writes_by_op[1]);
    println!("    WriteZeroes:  {}", writes_by_op[2]);
    println!(
        "  Blocks touched: {} (including multi-block ops)",
        total_blocks_touched
    );
    println!("  Unique blocks:  {}", unique_blocks);
    println!(
        "  Overwrite ratio: {:.2}x (total / unique)",
        overwrite_ratio
    );
    if duration_s > 0.0 {
        println!(
            "  Unique rate:    {:.1} blocks/sec",
            unique_blocks as f64 / duration_s
        );
        println!(
            "  Raw rate:       {:.1} blocks/sec",
            total_blocks_touched as f64 / duration_s
        );
    }
    println!();

    // =========================================================================
    // 2. Hot Set Analysis
    // =========================================================================

    let mut freq_buckets: BTreeMap<&str, (usize, u64)> = BTreeMap::new();
    let buckets = [
        ("1x (written once)", 1, 1),
        ("2-5x", 2, 5),
        ("6-20x", 6, 20),
        ("21-100x", 21, 100),
        ("101-1000x", 101, 1000),
        ("1001x+", 1001, u64::MAX),
    ];
    for (&_block, &count) in &block_write_counts {
        for &(label, lo, hi) in &buckets {
            if count >= lo && count <= hi {
                let entry = freq_buckets.entry(label).or_insert((0, 0));
                entry.0 += 1;
                entry.1 += count;
                break;
            }
        }
    }

    println!("Hot Set Analysis ({} unique blocks):", unique_blocks);
    println!("  ┌──────────────────┬────────┬──────────┬──────────┐");
    println!("  │ Write Frequency   │ Blocks │ % Blocks │  Writes  │");
    println!("  ├──────────────────┼────────┼──────────┼──────────┤");
    for &(label, _, _) in &buckets {
        if let Some(&(blocks, writes)) = freq_buckets.get(label) {
            println!(
                "  │ {:>16} │ {:>6} │ {:>7.1}% │ {:>8} │",
                label,
                blocks,
                blocks as f64 / unique_blocks as f64 * 100.0,
                writes,
            );
        }
    }
    println!("  └──────────────────┴────────┴──────────┴──────────┘");
    println!();

    // =========================================================================
    // 3. Write Rate Timeline (sliding window)
    // =========================================================================

    let window_us = (args.window * 1_000_000.0) as u64;
    let first_ts = entries.first().unwrap().elapsed_us;
    let last_ts = entries.last().unwrap().elapsed_us;

    if args.timeline && duration_us > 0 {
        println!("Write Rate Timeline ({:.0}s windows):", args.window);
        println!("  ┌──────────┬──────────┬──────────┬──────────┐");
        println!("  │ Time (s) │ Raw Ops  │ Unique   │ Blks/sec │");
        println!("  ├──────────┼──────────┼──────────┼──────────┤");

        let mut window_start = first_ts;
        let mut entry_idx = 0;
        while window_start < last_ts {
            let window_end = window_start + window_us;
            let mut ops_in_window = 0u64;
            let mut unique_in_window: std::collections::HashSet<u32> =
                std::collections::HashSet::new();

            while entry_idx < entries.len() && entries[entry_idx].elapsed_us < window_end {
                let e = &entries[entry_idx];
                ops_in_window += 1;
                for b in e.block_index..(e.block_index + e.span as u32) {
                    unique_in_window.insert(b);
                }
                entry_idx += 1;
            }

            let t = (window_start - first_ts) as f64 / 1_000_000.0;
            let rate = unique_in_window.len() as f64 / args.window;
            println!(
                "  │ {:>8.1} │ {:>8} │ {:>8} │ {:>8.1} │",
                t,
                ops_in_window,
                unique_in_window.len(),
                rate,
            );

            // Advance by window/2 for overlapping windows (smoother)
            window_start += window_us / 2;
            // Reset entry_idx to find entries in the new window
            entry_idx = entries.partition_point(|e| e.elapsed_us < window_start);
        }

        println!("  └──────────┴──────────┴──────────┴──────────┘");
        println!();
    }

    // =========================================================================
    // 4. Burst Detection
    // =========================================================================

    // Divide trace into 1-second buckets, find peaks and valleys
    let bucket_us = 1_000_000u64;
    let num_buckets = duration_us.div_ceil(bucket_us) as usize;
    if num_buckets > 0 {
        let mut bucket_counts = vec![0u64; num_buckets.max(1)];
        let mut bucket_unique: Vec<std::collections::HashSet<u32>> = (0..num_buckets.max(1))
            .map(|_| std::collections::HashSet::new())
            .collect();

        for e in &entries {
            let bucket = ((e.elapsed_us - first_ts) / bucket_us) as usize;
            let bucket = bucket.min(num_buckets - 1);
            bucket_counts[bucket] += e.span as u64;
            for b in e.block_index..(e.block_index + e.span as u32) {
                bucket_unique[bucket].insert(b);
            }
        }

        let unique_per_sec: Vec<usize> = bucket_unique.iter().map(|s| s.len()).collect();
        let max_rate = *unique_per_sec.iter().max().unwrap_or(&0);
        let min_rate = *unique_per_sec.iter().min().unwrap_or(&0);
        let avg_rate = if !unique_per_sec.is_empty() {
            unique_per_sec.iter().sum::<usize>() as f64 / unique_per_sec.len() as f64
        } else {
            0.0
        };
        let median_rate = {
            let mut sorted = unique_per_sec.clone();
            sorted.sort();
            if sorted.is_empty() {
                0
            } else {
                sorted[sorted.len() / 2]
            }
        };

        // Find burst periods (> 2x median)
        let burst_threshold = (median_rate as f64 * 2.0) as usize;
        let burst_seconds = unique_per_sec
            .iter()
            .filter(|&&r| r > burst_threshold)
            .count();
        let quiet_seconds = unique_per_sec.iter().filter(|&&r| r == 0).count();

        println!("Burst Analysis ({} seconds):", num_buckets);
        println!("  Peak:     {} unique blocks/sec", max_rate);
        println!("  Avg:      {:.1} unique blocks/sec", avg_rate);
        println!("  Median:   {} unique blocks/sec", median_rate);
        println!("  Min:      {} unique blocks/sec", min_rate);
        println!(
            "  Burst:    {}s above {}x median ({}s, {:.0}% of trace)",
            burst_seconds,
            2,
            burst_threshold,
            burst_seconds as f64 / num_buckets as f64 * 100.0,
        );
        println!(
            "  Quiet:    {}s with zero writes ({:.0}% of trace)",
            quiet_seconds,
            quiet_seconds as f64 / num_buckets as f64 * 100.0,
        );
        println!();
    }

    // =========================================================================
    // 5. Dirty Block Accumulation / Pack Fill Simulation
    // =========================================================================

    println!("Pack Fill Simulation ({} unique blocks):", unique_blocks);
    println!("  ┌─────────────┬─────────┬──────────┬──────────┬──────────────┐");
    println!("  │ Blocks/Pack │ Flushes │ Avg Fill │  S3 PUTs │ Time to Fill │");
    println!("  ├─────────────┼─────────┼──────────┼──────────┼──────────────┤");

    for &ps in &pack_sizes {
        // Simulate: replay trace, track dirty set, flush when |dirty| >= ps
        let mut dirty: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mut flushes = 0u64;
        let mut total_flushed = 0u64;
        let mut flush_times: Vec<u64> = Vec::new();
        let mut last_flush_ts = first_ts;

        for e in &entries {
            for b in e.block_index..(e.block_index + e.span as u32) {
                dirty.insert(b);
            }
            if dirty.len() >= ps {
                flushes += 1;
                total_flushed += dirty.len() as u64;
                flush_times.push(e.elapsed_us - last_flush_ts);
                last_flush_ts = e.elapsed_us;
                dirty.clear();
            }
        }
        // Remaining dirty blocks = partial pack (flushed on drain)
        if !dirty.is_empty() {
            flushes += 1;
            total_flushed += dirty.len() as u64;
            if last_flush_ts < last_ts {
                flush_times.push(last_ts - last_flush_ts);
            }
        }

        let avg_fill = if flushes > 0 {
            total_flushed as f64 / flushes as f64
        } else {
            0.0
        };
        let avg_fill_time = if !flush_times.is_empty() {
            let sum: u64 = flush_times.iter().sum();
            sum as f64 / flush_times.len() as f64 / 1_000_000.0
        } else {
            0.0
        };

        println!(
            "  │ {:>11} │ {:>7} │ {:>8.1} │ {:>8} │ {:>10.1}s │",
            ps, flushes, avg_fill, flushes, avg_fill_time,
        );
    }

    println!("  └─────────────┴─────────┴──────────┴──────────┴──────────────┘");
    println!();
    println!("  Note: \"Time to Fill\" is avg seconds between flush triggers.");
    println!("  Lower = more frequent S3 PUTs. Higher = more coalescing.");

    // =========================================================================
    // 6. Spatial Clustering (same as block_churn_measure)
    // =========================================================================

    let mut dirty_indices: Vec<u32> = block_write_counts.keys().copied().collect();
    dirty_indices.sort();

    if dirty_indices.len() > 1 {
        println!();
        println!("Spatial Clustering ({} dirty blocks):", dirty_indices.len());

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

        let run_buckets: &[(&str, usize, usize)] = &[
            ("1 (isolated)", 1, 1),
            ("2-4", 2, 4),
            ("5-16", 5, 16),
            ("17-64", 17, 64),
            ("65+", 65, usize::MAX),
        ];

        println!("  ┌─────────────────┬────────┬────────┬──────────┐");
        println!("  │ Run Length       │  Runs  │ Blocks │ % Blocks │");
        println!("  ├─────────────────┼────────┼────────┼──────────┤");
        for &(label, lo, hi) in run_buckets {
            let mut count = 0usize;
            let mut blocks = 0usize;
            for &run in &runs {
                if run >= lo && run <= hi {
                    count += 1;
                    blocks += run;
                }
            }
            if count > 0 {
                println!(
                    "  │ {:>15} │ {:>6} │ {:>6} │ {:>7.1}% │",
                    label,
                    count,
                    blocks,
                    blocks as f64 / dirty_indices.len() as f64 * 100.0,
                );
            }
        }
        println!("  └─────────────────┴────────┴────────┴──────────┘");
    }
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
