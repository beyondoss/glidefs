//! Empirical probe: do io_uring's O_DIRECT-only knobs (iopoll / sqpoll) help the
//! foyer L2 (SSD) read path, and what do they cost in CPU?
//!
//! Builds an O_DIRECT foyer cache with the uring engine in {plain, iopoll,
//! sqpoll} mode, populates it, then times `storage().load()` (the pure media read
//! path) at a given concurrency -- reporting read latency AND the CPU time the
//! process burned during the timed window (polling modes spin, so watch stime).
//!
//! Usage:
//!   cargo run --release --example cache_l2_poll -- <none|iopoll|sqpoll> <conc> <dir>

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, EvictionConfig, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCachePolicy, IoEngineConfig, RecoverMode, S3FifoConfig,
    UringIoEngineConfig,
};
use futures::future::join_all;
use rand::Rng;

use glidefs::block::block_map::{Blake3Hash, blake3_128};

const BLOCK_SIZE: usize = 128 * 1024;
const NUM_BLOCKS: usize = 2048; // 256 MiB working set

// CPU time (user+sys) consumed by this process so far, in seconds.
fn cpu_secs() -> (f64, f64) {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // Fields 14 (utime) and 15 (stime) in clock ticks, after the ")" of comm.
    let after = stat.rsplit(')').next().unwrap_or("");
    let f: Vec<&str> = after.split_whitespace().collect();
    let hz = 100.0; // USER_HZ
    let utime = f.get(11).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
    let stime = f.get(12).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
    (utime / hz, stime / hz)
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let poll = a.get(1).map(String::as_str).unwrap_or("none");
    let conc: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(16);
    let dir = a.get(3).cloned().unwrap_or_else(|| "/tmp/cache_l2_poll".into());

    let mut uring = UringIoEngineConfig::new();
    match poll {
        "none" => {}
        "iopoll" => uring = uring.with_iopoll(true),
        "sqpoll" => uring = uring.with_sqpoll(true),
        other => panic!("poll must be none|iopoll|sqpoll, got {other}"),
    }

    std::fs::create_dir_all(&dir).unwrap();
    let device = FsDeviceBuilder::new(Path::new(&dir))
        .with_capacity(NUM_BLOCKS * BLOCK_SIZE * 2)
        .with_direct(true) // iopoll/sqpoll REQUIRE O_DIRECT
        .build()
        .expect("build O_DIRECT device");

    let cache: HybridCache<Blake3Hash, Bytes> = match HybridCacheBuilder::new()
        .with_name("cache-l2-poll")
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(2 * 1024 * 1024) // tiny L1 -> reads hit L2 media
        .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
        .with_weighter(|_k: &Blake3Hash, v: &Bytes| v.len())
        .storage()
        .with_engine_config(BlockEngineConfig::new(device))
        .with_io_engine_config(Box::new(uring) as Box<dyn IoEngineConfig>)
        .with_recover_mode(RecoverMode::Quiet)
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            println!("poll={poll:<7} UNSUPPORTED: {e}");
            std::fs::remove_dir_all(&dir).ok();
            return;
        }
    };

    // Populate; keep hashes.
    let mut hashes = Vec::with_capacity(NUM_BLOCKS);
    let mut buf = vec![0u8; BLOCK_SIZE];
    for i in 0..NUM_BLOCKS {
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

    // Timed: storage().load() (pure L2/media path), `conc` in flight per unit.
    const UNITS: usize = 4000;
    let mut rng = rand::thread_rng();
    let (u0, s0) = cpu_secs();
    let mut samples = Vec::with_capacity(UNITS);
    for _ in 0..UNITS {
        let keys: Vec<Blake3Hash> = (0..conc).map(|_| hashes[rng.gen_range(0..hashes.len())]).collect();
        let t = Instant::now();
        let loads = join_all(keys.iter().map(|k| cache.storage().load(k))).await;
        samples.push(t.elapsed());
        for l in loads {
            assert!(matches!(l.expect("load"), foyer::Load::Entry { .. }));
        }
    }
    let (u1, s1) = cpu_secs();
    samples.sort_unstable();
    let wall: Duration = samples.iter().sum();
    let per_read = wall / (UNITS * conc) as u32;
    let median_batch = samples[UNITS / 2];
    let reads = (UNITS * conc) as f64;
    let cpu_user = u1 - u0;
    let cpu_sys = s1 - s0;

    println!(
        "poll={poll:<7} conc={conc:<3} | per_read={per_read:>8.2?} batch_median={median_batch:>8.2?} \
         | CPU/read: user={:>5.2}us sys={:>5.2}us  (wall={:.1}s)",
        cpu_user / reads * 1e6,
        cpu_sys / reads * 1e6,
        wall.as_secs_f64(),
    );
    std::fs::remove_dir_all(&dir).ok();
}
