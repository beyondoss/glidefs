//! Empirical measurement of per-export WriteCache resource footprint.
//!
//! Creates N write caches of a given device size, writes varying amounts of
//! data, and reports RSS, disk usage, and timing.
//!
//! Run with: cargo run --example measure_footprint --features test-utils

use std::sync::Arc;
use std::time::Instant;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::state::Active;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB

fn rss_mb() -> f64 {
    // macOS: use mach API via ps
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps failed");
    let rss_kb: f64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or(0.0);
    rss_kb / 1024.0
}

fn disk_usage_mb(path: &std::path::Path) -> f64 {
    let output = std::process::Command::new("du")
        .args(["-sm", &path.to_string_lossy()])
        .output()
        .expect("du failed");
    let s = String::from_utf8_lossy(&output.stdout);
    s.split_whitespace()
        .next()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn create_cache(dir: &std::path::Path, name: &str, device_size: u64) -> Arc<WriteCache<Active>> {
    let config = WriteCacheConfig {
        cache_dir: dir.to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };
    let cache = WriteCache::open(config).expect("open failed");
    Arc::new(cache.skip_recovery_for_test())
}

fn main() {
    let device_size: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10); // GB
    let device_size_bytes = device_size * 1024 * 1024 * 1024;

    let num_exports: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let dirty_pct: f64 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5.0); // % of blocks to dirty

    let num_blocks = (device_size_bytes / BLOCK_SIZE as u64) as usize;
    let blocks_to_dirty = ((num_blocks as f64) * dirty_pct / 100.0) as usize;

    println!("=== WriteCache Footprint Measurement ===");
    println!(
        "Device size:     {} GB ({} blocks @ 128KB)",
        device_size, num_blocks
    );
    println!("Exports:         {}", num_exports);
    println!(
        "Dirty per export: {}% ({} blocks = {} MB)",
        dirty_pct,
        blocks_to_dirty,
        blocks_to_dirty * BLOCK_SIZE / (1024 * 1024)
    );
    println!();

    let tmp = tempfile::tempdir().expect("tempdir failed");
    let _clean_cache = Arc::new(SimpleBlockCache::new(1)); // minimal

    // Baseline
    let rss_before = rss_mb();
    println!("Baseline RSS:    {:.1} MB", rss_before);

    // Phase 1: Create exports (empty)
    let t0 = Instant::now();
    let mut caches: Vec<Arc<WriteCache<Active>>> = Vec::with_capacity(num_exports);
    for i in 0..num_exports {
        // Each export gets its own subdirectory to avoid file collisions
        let export_dir = tmp.path().join(format!("export-{}", i));
        std::fs::create_dir_all(&export_dir).unwrap();
        caches.push(create_cache(
            &export_dir,
            &format!("vm-{}", i),
            device_size_bytes,
        ));
    }
    let create_elapsed = t0.elapsed();
    let rss_after_create = rss_mb();
    let disk_after_create = disk_usage_mb(tmp.path());

    println!("\n--- After creating {} empty exports ---", num_exports);
    println!("Time:            {:.2}s", create_elapsed.as_secs_f64());
    println!(
        "RSS:             {:.1} MB (delta: {:.1} MB, per-export: {:.2} MB)",
        rss_after_create,
        rss_after_create - rss_before,
        (rss_after_create - rss_before) / num_exports as f64
    );
    println!(
        "Disk:            {:.1} MB (per-export: {:.2} MB)",
        disk_after_create,
        disk_after_create / num_exports as f64
    );

    if blocks_to_dirty == 0 {
        println!("\nNo dirty blocks requested, done.");
        return;
    }

    // Phase 2: Write dirty blocks to each export
    let write_buf = vec![0xABu8; BLOCK_SIZE];
    let t1 = Instant::now();
    for cache in &caches {
        // Write blocks spread across the device (every Nth block)
        let stride = if blocks_to_dirty > 0 {
            num_blocks / blocks_to_dirty
        } else {
            1
        };
        for j in 0..blocks_to_dirty {
            let block_idx = (j * stride) % num_blocks;
            let offset = block_idx as u64 * BLOCK_SIZE as u64;
            cache
                .write(offset, &write_buf, &[])
                .unwrap();
        }
    }
    let write_elapsed = t1.elapsed();
    let rss_after_write = rss_mb();
    let disk_after_write = disk_usage_mb(tmp.path());

    println!(
        "\n--- After writing {}% dirty blocks to each export ---",
        dirty_pct
    );
    println!(
        "Time:            {:.2}s ({:.0} us/write)",
        write_elapsed.as_secs_f64(),
        write_elapsed.as_micros() as f64 / (num_exports * blocks_to_dirty) as f64
    );
    println!(
        "RSS:             {:.1} MB (delta from empty: {:.1} MB, per-export: {:.2} MB)",
        rss_after_write,
        rss_after_write - rss_after_create,
        (rss_after_write - rss_after_create) / num_exports as f64
    );
    println!(
        "Disk:            {:.1} MB (per-export: {:.2} MB)",
        disk_after_write,
        disk_after_write / num_exports as f64
    );

    // Summary
    let total_per_export_mb = (rss_after_write - rss_before) / num_exports as f64;
    println!("\n=== Summary ===");
    println!("Total RSS per export:  {:.2} MB", total_per_export_mb);
    println!(
        "Projected 2000 exports: {:.1} GB RSS",
        total_per_export_mb * 2000.0 / 1024.0
    );
    println!(
        "Disk per export:       {:.2} MB (actual, not pre-allocated)",
        disk_after_write / num_exports as f64
    );
    println!(
        "Projected 2000 exports: {:.1} GB disk",
        disk_after_write / num_exports as f64 * 2000.0 / 1024.0
    );
}
