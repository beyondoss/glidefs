//! Empirical probe for Claim 4: can a bigger explicit L1 + O_DIRECT recover the
//! warm-read latency that buffered I/O gets "for free" from the page cache?
//!
//! Exercises the full `HybridCache::get()` path (L1 memory tier -> L2 SSD tier)
//! under a hot-biased access pattern, so L1 sizing determines the hit source:
//!   - buffered + small L1  -> hot reads miss L1, served by page-cached L2 (~tens of µs)
//!   - direct   + small L1  -> hot reads miss L1, served by media           (~hundreds of µs)
//!   - direct   + large L1  -> hot reads hit L1                             (~sub-µs)
//!
//! Usage:
//!   cargo run --release --example cache_hot_read -- <buffered|direct> <l1_mb> <hot_mb> <total_mb> <dir>

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, EvictionConfig, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCachePolicy, IoEngineConfig, RecoverMode, S3FifoConfig,
};
use rand::Rng;

use glidefs::block::block_map::{Blake3Hash, blake3_128};

const BLOCK_SIZE: usize = 128 * 1024;

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let mode = a.get(1).map(String::as_str).unwrap_or("buffered");
    let l1_mb: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(2);
    let hot_mb: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(32);
    let total_mb: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(256);
    let dir = a.get(5).cloned().unwrap_or_else(|| "/tmp/cache_hot".into());
    let direct = mode == "direct";

    let num_total = total_mb * 1024 * 1024 / BLOCK_SIZE;
    let num_hot = (hot_mb * 1024 * 1024 / BLOCK_SIZE).min(num_total);

    std::fs::create_dir_all(&dir).unwrap();
    let device = FsDeviceBuilder::new(Path::new(&dir))
        .with_capacity(num_total * BLOCK_SIZE * 2)
        .with_direct(direct)
        .build()
        .expect("build device");
    let cache: HybridCache<Blake3Hash, Bytes> = HybridCacheBuilder::new()
        .with_name("cache-hot-read")
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(l1_mb * 1024 * 1024)
        .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
        .with_weighter(|_k: &Blake3Hash, v: &Bytes| v.len())
        .storage()
        .with_engine_config(BlockEngineConfig::new(device))
        .with_io_engine_config(Box::new(foyer::PsyncIoEngineConfig::new()) as Box<dyn IoEngineConfig>)
        .with_recover_mode(RecoverMode::Quiet)
        .build()
        .await
        .expect("build cache");

    // Populate, recording hashes (block index -> hash).
    let mut hashes = Vec::with_capacity(num_total);
    let mut buf = vec![0u8; BLOCK_SIZE];
    for i in 0..num_total {
        for (j, b) in buf.iter_mut().enumerate() {
            *b = (i as u32).wrapping_mul(2654435761).wrapping_add(j as u32) as u8;
        }
        let h = blake3_128(&buf);
        hashes.push(h);
        cache.insert(h, Bytes::copy_from_slice(&buf));
        if i % 32 == 31 {
            cache.storage().wait().await;
        }
    }
    cache.storage().wait().await;

    // 90% of accesses hit the hot set (first num_hot blocks), 10% uniform.
    let mut rng = rand::thread_rng();
    let pick = |rng: &mut rand::rngs::ThreadRng| -> usize {
        if rng.gen_bool(0.9) {
            rng.gen_range(0..num_hot)
        } else {
            rng.gen_range(0..num_total)
        }
    };

    // Warm up so L1 converges to the hot set under this pattern.
    for _ in 0..(num_total * 3) {
        let _ = cache.get(&hashes[pick(&mut rng)]).await.expect("get");
    }

    // Timed run.
    const ITERS: usize = 100_000;
    let mut samples = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let h = hashes[pick(&mut rng)];
        let t = Instant::now();
        let got = cache.get(&h).await.expect("get");
        samples.push(t.elapsed());
        assert!(got.is_some(), "miss");
    }
    samples.sort_unstable();
    let median = samples[ITERS / 2];
    let p99 = samples[ITERS * 99 / 100];
    let mean: Duration = samples.iter().sum::<Duration>() / ITERS as u32;

    println!(
        "mode={mode:<8} L1={l1_mb:>4}MiB hot={hot_mb}MiB total={total_mb}MiB | \
         median={:>8.2?} mean={:>8.2?} p99={:>8.2?}",
        median, mean, p99
    );
    std::fs::remove_dir_all(&dir).ok();
}
