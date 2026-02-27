//! Comprehensive workload benchmarks with S3 metrics tracking.
//!
//! These benchmarks simulate real-world workloads and report:
//! - Throughput (MB/s)
//! - Latency (P50, P99)
//! - Write amplification
//! - S3 API calls
//! - Cache hit rates
//!
//! Run with: `cargo bench --features test-utils --bench workloads`

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Check if verbose benchmark output is enabled via BENCH_VERBOSE=1
fn is_verbose() -> bool {
    std::env::var("BENCH_VERBOSE")
        .map(|v| v == "1")
        .unwrap_or(false)
}
use object_store::ObjectStore;
use parking_lot::RwLock;
use rand::{Rng, thread_rng};
use tempfile::TempDir;

use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::state::Active;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB
const DEVICE_SIZE_MB: u64 = 256; // 256MB test device

/// Test harness with metrics tracking.
struct TestHarness {
    cache: WriteCache<Active>,
    content_store: ContentStore,
    pack_index_cache: Arc<PackIndexCache>,
    volume_manifest: Arc<RwLock<VolumeManifest>>,
    clean_cache: Arc<dyn BlockCache>,
    metrics: Arc<ExportMetrics>,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

/// Shared PackIndexCache with its backing TempDir.
struct SharedPackIndexCache {
    cache: Arc<PackIndexCache>,
    #[allow(dead_code)]
    dir: TempDir,
}

impl SharedPackIndexCache {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
        Self { cache, dir }
    }
}

impl TestHarness {
    fn new(pic: &Arc<PackIndexCache>) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let metrics = Arc::new(ExportMetrics::new());
        let device_size = DEVICE_SIZE_MB * 1024 * 1024;

        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "bench".to_string(),
            device_size,
            block_size: BLOCK_SIZE,
            wal_sync: false,
        };

        let content_store = ContentStore::new(Arc::clone(&s3), "bench");
        let volume_manifest = Arc::new(RwLock::new(
            VolumeManifest::new(device_size, BLOCK_SIZE as u32),
        ));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let cache = WriteCache::open(config).expect("Failed to open cache");
        let cache = cache.skip_recovery_for_test();

        Self {
            cache,
            content_store,
            pack_index_cache: Arc::clone(pic),
            volume_manifest,
            clean_cache,
            metrics,
            temp_dir,
        }
    }

    fn device_blocks(&self) -> u64 {
        (DEVICE_SIZE_MB * 1024 * 1024) / BLOCK_SIZE as u64
    }

    /// Print metrics summary (only when BENCH_VERBOSE=1).
    #[allow(dead_code)]
    fn print_metrics(&self, label: &str) {
        if !is_verbose() {
            return;
        }
        let snap = self.metrics.snapshot();
        eprintln!("\n=== {} Metrics ===", label);
        eprintln!(
            "  Guest: {} writes ({:.2} MB), {} reads ({:.2} MB)",
            snap.guest_write_ops,
            snap.guest_bytes_written as f64 / 1024.0 / 1024.0,
            snap.guest_read_ops,
            snap.guest_bytes_read as f64 / 1024.0 / 1024.0
        );
        eprintln!(
            "  S3: {} batches ({:.2} MB written), {} reads ({:.2} MB)",
            snap.batches_written,
            snap.s3_bytes_written as f64 / 1024.0 / 1024.0,
            snap.s3_read_ops,
            snap.s3_bytes_read as f64 / 1024.0 / 1024.0
        );
        eprintln!("  Write amplification: {:.2}x", snap.write_amplification);
        eprintln!("  Coalesce ratio: {:.1} writes/batch", snap.coalesce_ratio);
        eprintln!("  Cache hit rate: {:.1}%", snap.cache_hit_rate * 100.0);
    }
}

/// Benchmark: Random 4KB writes (simulates database random I/O).
fn bench_random_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("random_writes");
    let shared = rt.block_on(SharedPackIndexCache::new());

    for write_size in [4096usize, 16384, 65536] {
        group.throughput(Throughput::Bytes(write_size as u64));

        group.bench_with_input(
            BenchmarkId::new("random", format!("{}KB", write_size / 1024)),
            &write_size,
            |b, &size| {
                let harness = TestHarness::new(&shared.cache);
                let mut rng = rand::thread_rng();
                let data = vec![0xABu8; size];
                let max_offset = harness.device_blocks() * BLOCK_SIZE as u64 - size as u64;

                b.iter(|| {
                    let offset = rng.gen_range(0..max_offset);
                    // Align to 4KB
                    let aligned = (offset / 4096) * 4096;
                    harness
                        .cache
                        .write(aligned, &data, harness.clean_cache.as_ref())
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Sequential 128KB reads (simulates VM boot / large file reads).
fn bench_sequential_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("sequential_reads");
    group.throughput(Throughput::Bytes(BLOCK_SIZE as u64));
    let shared = rt.block_on(SharedPackIndexCache::new());

    // Cold cache (S3 fetch required)
    {
        let pic = Arc::clone(&shared.cache);
        group.bench_function("cold_cache_128kb", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let mut total = Duration::ZERO;

                    for _ in 0..iters {
                        let harness = TestHarness::new(&pic);
                        // Pre-populate S3 with data
                        for i in 0..10 {
                            let data = vec![i as u8; BLOCK_SIZE];
                            harness
                                .cache
                                .write(
                                    i as u64 * BLOCK_SIZE as u64,
                                    &data,
                                    harness.clean_cache.as_ref(),
                                )
                                .unwrap();
                        }
                        harness
                            .cache
                            .flush_to_s3(
                                &harness.content_store,
                                &harness.pack_index_cache,
                                &harness.volume_manifest,
                            )
                            .await
                            .unwrap();

                        // Create fresh cache (cold) — new clean_cache so nothing is cached
                        let cold_cache: Arc<dyn BlockCache> =
                            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

                        let start = Instant::now();
                        for i in 0..10 {
                            let _ = harness
                                .cache
                                .read(
                                    i as u64 * BLOCK_SIZE as u64,
                                    BLOCK_SIZE,
                                    cold_cache.as_ref(),
                                    &harness.pack_index_cache,
                                    &harness.volume_manifest,
                                    &harness.content_store,
                                    &harness.metrics,
                                )
                                .await
                                .unwrap();
                        }
                        total += start.elapsed();
                    }

                    total
                }
            });
        });
    }

    // Warm cache (local reads)
    group.bench_function("warm_cache_128kb", |b| {
        let harness = TestHarness::new(&shared.cache);
        // Pre-populate local cache
        for i in 0..100 {
            let data = vec![i as u8; BLOCK_SIZE];
            harness
                .cache
                .write(
                    i as u64 * BLOCK_SIZE as u64,
                    &data,
                    harness.clean_cache.as_ref(),
                )
                .unwrap();
        }

        let mut offset = 0u64;
        b.iter(|| {
            let _ = harness.cache.read_local(offset, BLOCK_SIZE);
            offset = (offset + BLOCK_SIZE as u64) % (100 * BLOCK_SIZE as u64);
        });
    });

    group.finish();
}

/// Benchmark: Mixed read/write IOPS without background flush (baseline).
fn bench_mixed_iops_baseline(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("mixed_iops_baseline");
    group.throughput(Throughput::Elements(1));
    let shared = rt.block_on(SharedPackIndexCache::new());

    // 70% read, 30% write (typical VM ratio)
    group.bench_function("70r_30w", |b| {
        let harness = TestHarness::new(&shared.cache);
        let mut rng = thread_rng();

        // Pre-populate some data
        for i in 0..50 {
            let data = vec![i as u8; BLOCK_SIZE];
            harness
                .cache
                .write(
                    i as u64 * BLOCK_SIZE as u64,
                    &data,
                    harness.clean_cache.as_ref(),
                )
                .unwrap();
        }

        let data = vec![0xCDu8; BLOCK_SIZE];
        let max_block = 50u64;

        b.iter(|| {
            let block: u64 = rng.gen_range(0..max_block);
            let offset = block * BLOCK_SIZE as u64;

            if rng.gen_ratio(7, 10) {
                let _ = harness.cache.read_local(offset, BLOCK_SIZE);
            } else {
                harness
                    .cache
                    .write(offset, &data, harness.clean_cache.as_ref())
                    .unwrap();
            }
        });
    });

    // 50% read, 50% write
    group.bench_function("50r_50w", |b| {
        let harness = TestHarness::new(&shared.cache);
        let mut rng = thread_rng();

        for i in 0..50 {
            let data = vec![i as u8; BLOCK_SIZE];
            harness
                .cache
                .write(
                    i as u64 * BLOCK_SIZE as u64,
                    &data,
                    harness.clean_cache.as_ref(),
                )
                .unwrap();
        }

        let data = vec![0xEFu8; BLOCK_SIZE];
        let max_block = 50u64;

        b.iter(|| {
            let block: u64 = rng.gen_range(0..max_block);
            let offset = block * BLOCK_SIZE as u64;

            if rng.gen_bool(0.5) {
                let _ = harness.cache.read_local(offset, BLOCK_SIZE);
            } else {
                harness
                    .cache
                    .write(offset, &data, harness.clean_cache.as_ref())
                    .unwrap();
            }
        });
    });

    group.finish();
}

/// Benchmark: Mixed IOPS with periodic background flush_to_s3.
///
/// Matches production continuous mode: flush every 5s. Runs I/O for 10s
/// so at least one flush fires mid-measurement. Reports sustained IOPS
/// including any stalls during flush.
fn bench_mixed_iops_during_flush(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("mixed_iops_during_flush");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    let shared = rt.block_on(SharedPackIndexCache::new());

    for (label, read_ratio) in [("70r_30w", 0.7f64), ("50r_50w", 0.5)] {
        let pic = Arc::clone(&shared.cache);
        group.bench_function(label, |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let read_ratio = read_ratio;
                    let harness = Arc::new(TestHarness::new(&pic));
                    let max_block = 200u64;

                    // Pre-populate data so reads hit local cache
                    for i in 0..max_block {
                        let data = vec![i as u8; BLOCK_SIZE];
                        harness
                            .cache
                            .write(i * BLOCK_SIZE as u64, &data, harness.clean_cache.as_ref())
                            .unwrap();
                    }

                    // Start background flush on 5s interval (matching production continuous mode)
                    let flush_harness = Arc::clone(&harness);
                    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                    let stop_clone = Arc::clone(&stop);
                    let flush_handle = tokio::spawn(async move {
                        let mut interval = tokio::time::interval(Duration::from_secs(5));
                        interval.tick().await; // consume immediate first tick
                        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                            interval.tick().await;
                            let _ = flush_harness
                                .cache
                                .flush_to_s3(
                                    &flush_harness.content_store,
                                    &flush_harness.pack_index_cache,
                                    &flush_harness.volume_manifest,
                                )
                                .await;
                        }
                    });

                    // Measure I/O ops while flush fires periodically
                    let data = vec![0xABu8; BLOCK_SIZE];
                    let start = Instant::now();

                    let io_harness = Arc::clone(&harness);
                    tokio::task::spawn_blocking(move || {
                        let mut rng = rand::thread_rng();
                        for _ in 0..iters {
                            let block: u64 = rng.gen_range(0..max_block);
                            let offset = block * BLOCK_SIZE as u64;

                            if rng.gen_bool(read_ratio) {
                                let _ = io_harness.cache.read_local(offset, BLOCK_SIZE);
                            } else {
                                io_harness
                                    .cache
                                    .write(offset, &data, io_harness.clean_cache.as_ref())
                                    .unwrap();
                            }
                        }
                    })
                    .await
                    .unwrap();

                    let elapsed = start.elapsed();

                    stop.store(true, std::sync::atomic::Ordering::Relaxed);
                    let _ = flush_handle.await;

                    elapsed
                }
            });
        });
    }

    group.finish();
}

/// Benchmark: Write coalescing effectiveness.
///
/// This measures how well the batching reduces S3 API calls.
fn bench_write_coalescing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("write_coalescing");
    let shared = rt.block_on(SharedPackIndexCache::new());

    // Overwrite same block repeatedly (best case - 100% coalesce)
    {
        let pic = Arc::clone(&shared.cache);
        group.bench_function("same_block_overwrite", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let harness = TestHarness::new(&pic);
                    let data = vec![0x11u8; BLOCK_SIZE];

                    let start = Instant::now();
                    // Write same block 1000 times
                    for _ in 0..1000 {
                        harness
                            .cache
                            .write(0, &data, harness.clean_cache.as_ref())
                            .unwrap();
                        harness.metrics.record_guest_write(BLOCK_SIZE as u64);
                    }
                    // Flush to S3
                    harness
                        .cache
                        .flush_to_s3(
                            &harness.content_store,
                            &harness.pack_index_cache,
                            &harness.volume_manifest,
                        )
                        .await
                        .unwrap();
                    let elapsed = start.elapsed();

                    harness.print_metrics("Same Block Overwrite");

                    elapsed * iters as u32
                }
            });
        });
    }

    // Sequential writes to different blocks (no coalesce opportunity)
    {
        let pic = Arc::clone(&shared.cache);
        group.bench_function("sequential_different_blocks", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let harness = TestHarness::new(&pic);

                    let start = Instant::now();
                    // Write 100 different blocks
                    for i in 0..100 {
                        let data = vec![i as u8; BLOCK_SIZE];
                        harness
                            .cache
                            .write(
                                i as u64 * BLOCK_SIZE as u64,
                                &data,
                                harness.clean_cache.as_ref(),
                            )
                            .unwrap();
                        harness.metrics.record_guest_write(BLOCK_SIZE as u64);
                    }
                    harness
                        .cache
                        .flush_to_s3(
                            &harness.content_store,
                            &harness.pack_index_cache,
                            &harness.volume_manifest,
                        )
                        .await
                        .unwrap();
                    let elapsed = start.elapsed();

                    harness.print_metrics("Sequential Different Blocks");

                    elapsed * iters as u32
                }
            });
        });
    }

    group.finish();
}

/// Benchmark: Real-world workload simulation.
fn bench_real_world_workloads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("real_world");
    group.sample_size(10); // Fewer samples for longer-running benchmarks
    let shared = rt.block_on(SharedPackIndexCache::new());

    // VM Boot simulation: Large sequential reads with occasional writes
    {
        let pic = Arc::clone(&shared.cache);
        group.bench_function("vm_boot_simulation", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let mut total = Duration::ZERO;

                    for _ in 0..iters {
                        let harness = TestHarness::new(&pic);
                        let mut rng = thread_rng();

                        // Pre-populate "disk image" - 64MB of data
                        for i in 0..512 {
                            let data: Vec<u8> =
                                (0..BLOCK_SIZE).map(|j| ((i + j) % 256) as u8).collect();
                            harness
                                .cache
                                .write(
                                    i as u64 * BLOCK_SIZE as u64,
                                    &data,
                                    harness.clean_cache.as_ref(),
                                )
                                .unwrap();
                        }
                        harness
                            .cache
                            .flush_to_s3(
                                &harness.content_store,
                                &harness.pack_index_cache,
                                &harness.volume_manifest,
                            )
                            .await
                            .unwrap();

                        let start = Instant::now();

                        // Boot: 90% sequential reads, 10% random writes (boot logs, tmp files)
                        for _ in 0..1000 {
                            if rng.gen_ratio(9, 10) {
                                // Sequential read (boot loading)
                                let block: u64 = rng.gen_range(0..512);
                                let _ = harness
                                    .cache
                                    .read_local(block * BLOCK_SIZE as u64, BLOCK_SIZE);
                                harness.metrics.record_guest_read(BLOCK_SIZE as u64);
                            } else {
                                // Random write (boot activity)
                                let block: u64 = rng.gen_range(0..512);
                                let data = vec![0xBBu8; 4096]; // 4KB write
                                harness
                                    .cache
                                    .write(
                                        block * BLOCK_SIZE as u64,
                                        &data,
                                        harness.clean_cache.as_ref(),
                                    )
                                    .unwrap();
                                harness.metrics.record_guest_write(4096);
                            }
                        }

                        // Flush to get S3 metrics
                        harness
                            .cache
                            .flush_to_s3(
                                &harness.content_store,
                                &harness.pack_index_cache,
                                &harness.volume_manifest,
                            )
                            .await
                            .unwrap();

                        total += start.elapsed();
                    }

                    total
                }
            });
        });
    }

    // Database workload: Small random writes with fsync
    {
        let pic = Arc::clone(&shared.cache);
        group.bench_function("database_random_io", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let mut total = Duration::ZERO;

                    for _ in 0..iters {
                        let harness = TestHarness::new(&pic);
                        let mut rng = thread_rng();
                        let data = vec![0xDBu8; 8192]; // 8KB database pages

                        let start = Instant::now();

                        // 1000 random 8KB writes with periodic flushes
                        for i in 0..1000 {
                            let block: u64 = rng.gen_range(0..100);
                            harness
                                .cache
                                .write(
                                    block * BLOCK_SIZE as u64,
                                    &data,
                                    harness.clean_cache.as_ref(),
                                )
                                .unwrap();
                            harness.metrics.record_guest_write(data.len() as u64);

                            // fsync every 10 writes (simulates transaction commits)
                            if i % 10 == 9 {
                                harness.cache.flush().unwrap();
                            }
                        }

                        // Flush to S3 to get write amplification metrics
                        harness
                            .cache
                            .flush_to_s3(
                                &harness.content_store,
                                &harness.pack_index_cache,
                                &harness.volume_manifest,
                            )
                            .await
                            .unwrap();

                        total += start.elapsed();
                    }

                    total
                }
            });
        });
    }

    // Compilation workload: Write many small files, read some back
    {
        let pic = Arc::clone(&shared.cache);
        group.bench_function("compilation_workload", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let mut total = Duration::ZERO;

                    for _ in 0..iters {
                        let harness = TestHarness::new(&pic);
                        let mut rng = thread_rng();

                        let start = Instant::now();

                        // Phase 1: Write many object files (small writes)
                        for i in 0..200 {
                            let size = rng.gen_range(1024..32768);
                            let data = vec![i as u8; size];
                            let offset = (i as u64 * 32768)
                                % (harness.device_blocks() * BLOCK_SIZE as u64);
                            harness
                                .cache
                                .write(offset, &data, harness.clean_cache.as_ref())
                                .unwrap();
                            harness.metrics.record_guest_write(size as u64);
                        }

                        // Phase 2: Read back for linking (sequential)
                        for i in 0..200 {
                            let offset = (i as u64 * 32768)
                                % (harness.device_blocks() * BLOCK_SIZE as u64);
                            let _ = harness.cache.read_local(offset, 32768);
                            harness.metrics.record_guest_read(32768);
                        }

                        // Phase 3: Write final binary
                        let binary = vec![0xEEu8; 512 * 1024]; // 512KB binary
                        harness
                            .cache
                            .write(0, &binary, harness.clean_cache.as_ref())
                            .unwrap();
                        harness.metrics.record_guest_write(binary.len() as u64);
                        harness.cache.flush().unwrap();

                        // Flush to get S3 metrics
                        harness
                            .cache
                            .flush_to_s3(
                                &harness.content_store,
                                &harness.pack_index_cache,
                                &harness.volume_manifest,
                            )
                            .await
                            .unwrap();

                        total += start.elapsed();
                    }

                    total
                }
            });
        });
    }

    group.finish();
}

/// Benchmark: Flush-to-S3 latency for different dirty block counts.
///
/// This shows the S3 sync cost that write-behind avoids on each snapshot.
fn bench_flush_to_s3_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("flush_to_s3_latency");
    group.sample_size(10);
    let shared = rt.block_on(SharedPackIndexCache::new());

    for dirty_mb in [1u64, 10, 50, 100] {
        let dirty_blocks = dirty_mb * 1024 * 1024 / BLOCK_SIZE as u64;
        group.throughput(Throughput::Bytes(dirty_mb * 1024 * 1024));

        let pic = Arc::clone(&shared.cache);
        group.bench_with_input(
            BenchmarkId::new("flush_to_s3", format!("{}MB", dirty_mb)),
            &dirty_blocks,
            |b, &blocks| {
                b.to_async(&rt).iter_custom(|iters| {
                    let pic = Arc::clone(&pic);
                    async move {
                        let mut total = Duration::ZERO;

                        for _ in 0..iters {
                            let harness = TestHarness::new(&pic);

                            // Write dirty blocks
                            for i in 0..blocks {
                                let data = vec![i as u8; BLOCK_SIZE];
                                harness
                                    .cache
                                    .write(
                                        i * BLOCK_SIZE as u64,
                                        &data,
                                        harness.clean_cache.as_ref(),
                                    )
                                    .unwrap();
                                harness.metrics.record_guest_write(BLOCK_SIZE as u64);
                            }

                            let start = Instant::now();
                            harness
                                .cache
                                .flush_to_s3(
                                    &harness.content_store,
                                    &harness.pack_index_cache,
                                    &harness.volume_manifest,
                                )
                                .await
                                .unwrap();
                            total += start.elapsed();

                            // Report metrics on last iteration (only when BENCH_VERBOSE=1)
                            if is_verbose() {
                                let snap = harness.metrics.snapshot();
                                eprintln!(
                                    "\n  {}MB flush: {} batches, {:.2}x write amp",
                                    blocks * BLOCK_SIZE as u64 / 1024 / 1024,
                                    snap.batches_written,
                                    snap.write_amplification
                                );
                            }
                        }

                        total
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Concurrent access from multiple threads.
///
/// This stress-tests the lock-free CAS-based state machine.
fn bench_concurrent_access(c: &mut Criterion) {
    use std::thread;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent");
    let shared = rt.block_on(SharedPackIndexCache::new());

    for num_threads in [2usize, 4, 8] {
        let pic = Arc::clone(&shared.cache);
        group.bench_with_input(
            BenchmarkId::new("writers", num_threads),
            &num_threads,
            |b, &threads| {
                b.iter_custom(|iters| {
                    let harness = Arc::new(TestHarness::new(&pic));
                    let iterations_per_thread = (iters as usize / threads).max(1);

                    let start = Instant::now();

                    let handles: Vec<_> = (0..threads)
                        .map(|t| {
                            let h = Arc::clone(&harness);
                            let thread_id = t;
                            thread::spawn(move || {
                                let mut rng = rand::thread_rng();
                                let data = vec![thread_id as u8; BLOCK_SIZE];

                                for _ in 0..iterations_per_thread {
                                    let block: u64 = rng.gen_range(0..100);
                                    h.cache
                                        .write(
                                            block * BLOCK_SIZE as u64,
                                            &data,
                                            h.clean_cache.as_ref(),
                                        )
                                        .unwrap();
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }

                    start.elapsed()
                });
            },
        );
    }

    // Mixed concurrent read/write
    group.bench_function("mixed_4_threads", |b| {
        b.iter_custom(|iters| {
            let harness = Arc::new(TestHarness::new(&shared.cache));

            // Pre-populate
            for i in 0..100 {
                let data = vec![i as u8; BLOCK_SIZE];
                harness
                    .cache
                    .write(
                        i as u64 * BLOCK_SIZE as u64,
                        &data,
                        harness.clean_cache.as_ref(),
                    )
                    .unwrap();
            }

            let iterations_per_thread = (iters as usize / 4).max(1);
            let start = Instant::now();

            let handles: Vec<_> = (0..4)
                .map(|t| {
                    let h = Arc::clone(&harness);
                    let is_writer = t % 2 == 0;
                    thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        let data = vec![t as u8; BLOCK_SIZE];

                        for _ in 0..iterations_per_thread {
                            let block: u64 = rng.gen_range(0..100);
                            let offset = block * BLOCK_SIZE as u64;

                            if is_writer {
                                h.cache
                                    .write(offset, &data, h.clean_cache.as_ref())
                                    .unwrap();
                            } else {
                                let _ = h.cache.read_local(offset, BLOCK_SIZE);
                            }
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            start.elapsed()
        });
    });

    group.finish();
}

/// Benchmark: Large sequential writes (like dd).
fn bench_sequential_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("sequential_writes");
    let shared = rt.block_on(SharedPackIndexCache::new());

    // 1MB sequential write
    group.throughput(Throughput::Bytes(1024 * 1024));
    group.bench_function("1mb_sequential", |b| {
        let harness = TestHarness::new(&shared.cache);
        let data = vec![0xAAu8; BLOCK_SIZE];
        let blocks_per_mb = 1024 * 1024 / BLOCK_SIZE;

        b.iter(|| {
            for i in 0..blocks_per_mb {
                harness
                    .cache
                    .write(
                        i as u64 * BLOCK_SIZE as u64,
                        &data,
                        harness.clean_cache.as_ref(),
                    )
                    .unwrap();
            }
        });
    });

    // 10MB sequential write
    group.throughput(Throughput::Bytes(10 * 1024 * 1024));
    group.bench_function("10mb_sequential", |b| {
        let harness = TestHarness::new(&shared.cache);
        let data = vec![0xBBu8; BLOCK_SIZE];
        let blocks_per_10mb = 10 * 1024 * 1024 / BLOCK_SIZE;

        b.iter(|| {
            for i in 0..blocks_per_10mb {
                harness
                    .cache
                    .write(
                        i as u64 * BLOCK_SIZE as u64,
                        &data,
                        harness.clean_cache.as_ref(),
                    )
                    .unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_random_writes,
    bench_sequential_reads,
    bench_sequential_writes,
    bench_mixed_iops_baseline,
    bench_mixed_iops_during_flush,
    bench_write_coalescing,
    bench_concurrent_access,
    bench_real_world_workloads,
    bench_flush_to_s3_latency,
);
criterion_main!(benches);
