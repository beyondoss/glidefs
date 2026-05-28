//! Benchmark: foyer SSD-tier read latency, **psync vs io_uring** I/O engine.
//!
//! GlideFS's clean cache and pack-index cache are foyer `HybridCache`s. With no
//! I/O engine configured, foyer defaults to the **psync** engine, which dispatches
//! every SSD read to a tokio blocking thread (`spawn_blocking` -> `pread`). The
//! **io_uring** engine submits the read inline instead. This bench isolates the
//! SSD-tier `get` so we can measure the difference.
//!
//! It builds the cache directly (not via `FoyerBlockCache::open`) so it can pin the
//! engine explicitly and run both in one go -- production has no engine knob, it
//! auto-selects io_uring on Linux. The memory tier is sized far below the working
//! set so the vast majority of `get`s miss memory and hit the SSD tier (the path
//! under test).
//!
//! Run with: `cargo bench --features test-utils --bench foyer_ssd_read`

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use foyer::{
    BlockEngineConfig, DeviceBuilder, EvictionConfig, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCachePolicy, IoEngineConfig, PsyncIoEngineConfig, RecoverMode,
    S3FifoConfig,
};
use rand::Rng;
use tempfile::TempDir;

use glidefs::block::block_map::{Blake3Hash, blake3_128};

const BLOCK_SIZE: usize = 128 * 1024; // 128 KiB, production block size
const NUM_BLOCKS: usize = 512; // 64 MiB working set
const MEMORY_BYTES: usize = 2 * 1024 * 1024; // ~16 blocks -> forces SSD-tier reads
const SSD_BYTES: usize = 256 * 1024 * 1024; // holds the whole working set

/// Build a `HybridCache<Blake3Hash, Bytes>` mirroring the clean-cache config
/// (S3-FIFO eviction, `len` weighter, block engine over an fs device), with the
/// given I/O engine pinned.
async fn build_cache(dir: &Path, engine: Box<dyn IoEngineConfig>) -> HybridCache<Blake3Hash, Bytes> {
    let device = FsDeviceBuilder::new(dir)
        .with_capacity(SSD_BYTES)
        .build()
        .expect("build device");
    HybridCacheBuilder::new()
        .with_name("foyer-ssd-read-bench")
        // Persist on insert (not on eviction) so every block is guaranteed on the
        // SSD tier before we read -- with the tiny memory tier, eviction-time
        // writes could race the flusher and lose entries. The engine read path we
        // measure (psync vs io_uring) is identical regardless of write policy.
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(MEMORY_BYTES)
        .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
        .with_weighter(|_k: &Blake3Hash, v: &Bytes| v.len())
        .storage()
        .with_engine_config(BlockEngineConfig::new(device))
        .with_io_engine_config(engine)
        .with_recover_mode(RecoverMode::Quiet)
        .build()
        .await
        .expect("build hybrid cache")
}

/// Deterministic, content-addressed blocks of incompressible-ish data.
fn make_blocks() -> Vec<(Blake3Hash, Bytes)> {
    let mut rng = rand::thread_rng();
    (0..NUM_BLOCKS)
        .map(|_| {
            let mut data = vec![0u8; BLOCK_SIZE];
            rng.fill(&mut data[..]);
            let hash = blake3_128(&data);
            (hash, Bytes::from(data))
        })
        .collect()
}

/// Insert all blocks and flush them to the SSD tier, then return the subset that
/// actually persisted. foyer's insert-time enqueue is non-forced (`force=false`,
/// `hybrid/cache.rs:529`), so under a burst the flusher drops some pieces; we
/// insert in batches with a `wait()` after each to let it keep up, then probe the
/// disk tier directly and keep only the keys that landed.
async fn populate_and_filter(
    cache: &HybridCache<Blake3Hash, Bytes>,
    blocks: &[(Blake3Hash, Bytes)],
) -> Vec<Blake3Hash> {
    const BATCH: usize = 32;
    for chunk in blocks.chunks(BATCH) {
        for (hash, data) in chunk {
            cache.insert(*hash, data.clone());
        }
        cache.storage().wait().await;
    }

    let mut present = Vec::with_capacity(blocks.len());
    for (hash, _) in blocks {
        // storage().load() reads the disk tier directly, without promoting into
        // the memory tier -- so probing here doesn't pollute the bench.
        if matches!(
            cache.storage().load(hash).await.expect("probe load"),
            foyer::Load::Entry { .. }
        ) {
            present.push(*hash);
        }
    }
    present
}

fn bench_ssd_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let blocks = make_blocks();

    // (label, engine factory). io_uring is Linux-only.
    type EngineFactory = fn() -> Box<dyn IoEngineConfig>;
    let mut engines: Vec<(&str, EngineFactory)> = vec![("psync", || {
        Box::new(PsyncIoEngineConfig::new()) as Box<dyn IoEngineConfig>
    })];
    #[cfg(target_os = "linux")]
    engines.push(("io_uring", || {
        Box::new(foyer::UringIoEngineConfig::new()) as Box<dyn IoEngineConfig>
    }));

    let mut group = c.benchmark_group("foyer_ssd_read");
    group.throughput(Throughput::Bytes(BLOCK_SIZE as u64));

    for (name, make_engine) in engines {
        let dir = TempDir::new().unwrap();
        let cache = rt.block_on(build_cache(dir.path(), make_engine()));
        let present = rt.block_on(populate_and_filter(&cache, &blocks));
        assert!(
            present.len() >= NUM_BLOCKS / 2,
            "{name}: only {}/{NUM_BLOCKS} blocks persisted to SSD",
            present.len()
        );

        // Measure the pure disk-read path via storage().load(): no memory tier in
        // the way, so this is exactly the psync-vs-io_uring SSD read we care about.
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let cache = &cache;
                let present = &present;
                async move {
                    let mut total = Duration::ZERO;
                    let mut rng = rand::thread_rng();
                    for _ in 0..iters {
                        let key = present[rng.gen_range(0..present.len())];
                        let start = std::time::Instant::now();
                        let load = cache.storage().load(&key).await.expect("load");
                        total += start.elapsed();
                        assert!(matches!(load, foyer::Load::Entry { .. }), "disk miss");
                    }
                    total
                }
            });
        });

        // Keep dir alive across the (synchronous) bench above.
        drop(dir);
    }
    group.finish();
}

criterion_group!(benches, bench_ssd_read);
criterion_main!(benches);
