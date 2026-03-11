//! Benchmark for v2 cold-read path (cache miss -> chunk meta -> S3 -> LZ4 decompress -> BLAKE3 verify).
//!
//! Measures read throughput under two conditions:
//! - **Cold read**: clean_cache is empty, every read goes through the full
//!   S3 fetch + LZ4 decompress + BLAKE3 verify pipeline.
//! - **Warm read**: clean_cache is populated, reads are pure in-memory hits.
//!
//! Run with: `cargo bench --features test-utils --bench cold_read`

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_store::ObjectStore;
use parking_lot::RwLock;
use rand::Rng;
use tempfile::TempDir;

use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::state::Active;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128 KB (production block size)

struct ReadBenchHarness {
    cache: WriteCache<Active>,
    content_store: ContentStore,
    pack_index_cache: Arc<PackIndexCache>,
    volume_manifest: Arc<RwLock<VolumeManifest>>,
    metrics: ExportMetrics,
    num_blocks: u64,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl ReadBenchHarness {
    /// Create a harness with `num_blocks` of random data already flushed to InMemory S3.
    async fn new(num_blocks: u64, pic: &Arc<PackIndexCache>) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        let device_size_bytes = (num_blocks as usize * BLOCK_SIZE) as u64;
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "bench".to_string(),
            device_size: device_size_bytes,
            block_size: BLOCK_SIZE,
            wal_sync: false,
        };

        let content_store = ContentStore::new(Arc::clone(&s3_backend), "bench");
        let volume_manifest = Arc::new(RwLock::new(
            VolumeManifest::new(device_size_bytes, BLOCK_SIZE as u32),
        ));
        let metrics = ExportMetrics::new();

        let cache = WriteCache::open(config).expect("Failed to open cache");
        let cache = cache.skip_recovery_for_test();

        // Temporary clean_cache used during setup writes.
        let setup_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        // Write random data -- realistic compression ratios.
        let mut rng = rand::thread_rng();
        for i in 0..num_blocks {
            let mut data = vec![0u8; BLOCK_SIZE];
            rng.fill(&mut data[..]);
            cache
                .write(i * BLOCK_SIZE as u64, &data, setup_cache.as_ref())
                .unwrap();
        }

        // Flush to S3 so the chunk meta cache and content store are populated.
        cache
            .snapshot(&content_store, pic, &volume_manifest, &setup_cache)
            .await
            .unwrap();

        Self {
            cache,
            content_store,
            pack_index_cache: Arc::clone(pic),
            volume_manifest,
            metrics,
            num_blocks,
            temp_dir,
        }
    }

    /// Read every block sequentially. Returns total data read in bytes.
    async fn read_all_blocks(&self, clean_cache: &dyn BlockCache) -> u64 {
        let mut bytes_read = 0u64;
        for i in 0..self.num_blocks {
            let offset = i * BLOCK_SIZE as u64;
            let buf = self
                .cache
                .read(
                    offset,
                    BLOCK_SIZE,
                    clean_cache,
                    &self.pack_index_cache,
                    &self.volume_manifest,
                    &self.content_store,
                    &self.metrics,
                )
                .await
                .unwrap();
            bytes_read += buf.len() as u64;
        }
        bytes_read
    }
}

/// Benchmark: cold read -- every read is a cache miss.
///
/// Measures the full pipeline per block:
///   block_map lookup -> chunk meta lookup -> S3 fetch -> LZ4 decompress -> BLAKE3 verify -> cache insert
fn bench_cold_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cold_read");
    group.sample_size(10);

    let pic_dir = TempDir::new().unwrap();
    let pic = Arc::new(rt.block_on(PackIndexCache::open(pic_dir.path())).unwrap());

    for num_blocks in [8u64, 64, 256] {
        let total_bytes = num_blocks * BLOCK_SIZE as u64;
        group.throughput(Throughput::Bytes(total_bytes));

        // Build the harness once (data is already in S3).
        let harness = rt.block_on(ReadBenchHarness::new(num_blocks, &pic));

        group.bench_with_input(
            BenchmarkId::new("sequential", format!("{num_blocks}_blocks")),
            &num_blocks,
            |b, &_blocks| {
                b.to_async(&rt).iter_custom(|iters| {
                    let harness = &harness;
                    async move {
                        let mut total = Duration::ZERO;

                        for _ in 0..iters {
                            // Fresh cache each iteration -- forces full S3 path.
                            let clean_cache: Arc<dyn BlockCache> =
                                Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

                            let start = std::time::Instant::now();
                            harness.read_all_blocks(clean_cache.as_ref()).await;
                            total += start.elapsed();
                        }

                        total
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: warm read -- every read is a cache hit.
///
/// Measures clean_cache lookup throughput (no S3, no decompression, no verification).
fn bench_warm_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("warm_read");
    group.sample_size(10);

    let pic_dir = TempDir::new().unwrap();
    let pic = Arc::new(rt.block_on(PackIndexCache::open(pic_dir.path())).unwrap());

    for num_blocks in [8u64, 64, 256] {
        let total_bytes = num_blocks * BLOCK_SIZE as u64;
        group.throughput(Throughput::Bytes(total_bytes));

        // Build the harness and pre-warm the cache.
        let harness = rt.block_on(ReadBenchHarness::new(num_blocks, &pic));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        // Prime the cache with one full read pass.
        rt.block_on(harness.read_all_blocks(clean_cache.as_ref()));

        group.bench_with_input(
            BenchmarkId::new("sequential", format!("{num_blocks}_blocks")),
            &num_blocks,
            |b, &_blocks| {
                let clean_cache = &clean_cache;
                b.to_async(&rt).iter_custom(|iters| {
                    let harness = &harness;
                    let clean_cache = clean_cache.as_ref();
                    async move {
                        let mut total = Duration::ZERO;

                        for _ in 0..iters {
                            let start = std::time::Instant::now();
                            harness.read_all_blocks(clean_cache).await;
                            total += start.elapsed();
                        }

                        total
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_cold_read, bench_warm_read);
criterion_main!(benches);
