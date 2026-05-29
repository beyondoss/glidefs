//! Empirical probe for the buffered-vs-O_DIRECT RAM-ownership question.
//!
//! Builds a foyer `HybridCache<Blake3Hash, Bytes>` (mirroring the clean-cache
//! config) on a real directory, populates it, then reports the process RSS (which
//! includes foyer's explicit L1 memory tier). It then EXITS leaving the cache
//! files on disk so the page cache they hold survives -- inspect it with `fincore`
//! afterward. Buffered should show ~working-set bytes still in page cache (i.e.
//! double-cached on top of foyer's L1); O_DIRECT should show ~0.
//!
//! Usage:
//!   cargo run --release --example cache_ram -- <buffered|direct> <mem_mb> <num_blocks> <dir>
//!
//! Then:
//!   fincore --bytes --total <dir>/* ; rm -rf <dir>

use std::path::Path;
use std::time::Instant;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, EvictionConfig, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCachePolicy, IoEngineConfig, PsyncIoEngineConfig, RecoverMode,
    S3FifoConfig,
};

use glidefs::block::block_map::{Blake3Hash, blake3_128};
use rand::Rng;

const BLOCK_SIZE: usize = 128 * 1024;

fn vm_rss_kb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("buffered");
    let mem_mb: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(64);
    let num_blocks: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2048);
    let dir = args
        .get(4)
        .cloned()
        .unwrap_or_else(|| "/tmp/cache_ram_probe".to_string());
    // If > 0, after populating keep reading the L2 tier for this many seconds
    // (used as an "active cache" antagonist in the tenant-interference test).
    let soak_secs: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);

    let direct = match mode {
        "buffered" => false,
        "direct" => true,
        other => panic!("mode must be buffered|direct, got {other}"),
    };

    let mem_bytes = mem_mb * 1024 * 1024;
    let ssd_bytes = (num_blocks * BLOCK_SIZE) * 2; // headroom
    let working_set_mb = (num_blocks * BLOCK_SIZE) / (1024 * 1024);

    std::fs::create_dir_all(&dir).unwrap();
    let device = FsDeviceBuilder::new(Path::new(&dir))
        .with_capacity(ssd_bytes)
        .with_direct(direct)
        .build()
        .expect("build device (O_DIRECT not supported here?)");

    let cache: HybridCache<Blake3Hash, Bytes> = HybridCacheBuilder::new()
        .with_name("cache-ram-probe")
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(mem_bytes)
        .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
        .with_weighter(|_k: &Blake3Hash, v: &Bytes| v.len())
        .storage()
        .with_engine_config(BlockEngineConfig::new(device))
        .with_io_engine_config(Box::new(PsyncIoEngineConfig::new()) as Box<dyn IoEngineConfig>)
        .with_recover_mode(RecoverMode::Quiet)
        .build()
        .await
        .expect("build hybrid cache");

    let rss_before = vm_rss_kb();

    // Populate in batches, flushing so everything lands on the SSD tier.
    let mut hashes = Vec::with_capacity(num_blocks);
    let mut data = vec![0u8; BLOCK_SIZE];
    for i in 0..num_blocks {
        // Cheap deterministic fill; distinct per block so hashes differ.
        for (j, b) in data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_add(j as u8).wrapping_mul(31);
        }
        let hash = blake3_128(&data);
        hashes.push(hash);
        cache.insert(hash, Bytes::copy_from_slice(&data));
        if i % 32 == 31 {
            cache.storage().wait().await;
        }
    }
    cache.storage().wait().await;

    let rss_after = vm_rss_kb();

    println!("mode={mode} direct={direct} L1_mem={mem_mb}MiB working_set={working_set_mb}MiB");
    println!(
        "process RSS: before={} MiB  after={} MiB  (delta={} MiB)",
        rss_before / 1024,
        rss_after / 1024,
        (rss_after.saturating_sub(rss_before)) / 1024,
    );
    println!("cache dir: {dir}");
    println!("--> now run:  fincore --bytes --total {dir}/*");

    if soak_secs > 0 {
        // Continuously read the L2 tier so its footprint stays hot. In buffered
        // mode this keeps ~working-set bytes resident in the page cache (the
        // antagonist); in direct mode it touches the device, not the page cache.
        println!("SOAKING for {soak_secs}s"); // readiness marker for the harness
        let mut rng = rand::thread_rng();
        let deadline = Instant::now() + std::time::Duration::from_secs(soak_secs);
        while Instant::now() < deadline {
            for _ in 0..256 {
                let h = hashes[rng.gen_range(0..hashes.len())];
                let _ = cache.storage().load(&h).await;
            }
        }
    }
    // Exit without dropping caches; files persist for fincore inspection.
}
