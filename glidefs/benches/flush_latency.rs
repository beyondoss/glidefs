//! Benchmark proving flush() is local SSD only.
//!
//! This benchmark demonstrates that flush() latency is O(1) with respect to
//! dirty block count, proving that flush completes in ~10ms instead of the
//! 50-300ms an S3 roundtrip would cost.
//!
//! Run with: `cargo bench --features test-utils --bench flush_latency`

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_store::ObjectStore;
use parking_lot::RwLock;
use tempfile::TempDir;

use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::chunk_cache::ChunkMetaCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::state::Active;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB

struct BenchHarness {
    cache: WriteCache<Active>,
    content_store: ContentStore,
    chunk_meta_cache: Arc<ChunkMetaCache>,
    volume_manifest: Arc<RwLock<VolumeManifest>>,
    clean_cache: Arc<dyn BlockCache>,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl BenchHarness {
    async fn new(device_size_mb: u64) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let device_size = device_size_mb * 1024 * 1024;

        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "bench".to_string(),
            device_size,
            block_size: BLOCK_SIZE,
            wal_sync: false,
        };

        let content_store = ContentStore::new(Arc::clone(&s3), "bench");
        let chunk_meta_cache =
            Arc::new(ChunkMetaCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(RwLock::new(
            VolumeManifest::new(device_size, BLOCK_SIZE as u32),
        ));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let cache = WriteCache::open(config).expect("Failed to open cache");
        let cache = cache.skip_recovery_for_test();

        Self {
            cache,
            content_store,
            chunk_meta_cache,
            volume_manifest,
            clean_cache,
            temp_dir,
        }
    }
}

/// Benchmark: flush() latency is constant regardless of dirty block count.
///
/// This proves the key architectural claim: FLUSH syncs to local SSD only,
/// not to S3. The latency should be ~5-15ms regardless of how many dirty
/// blocks are pending S3 sync.
fn bench_flush_is_local_only(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("flush_latency");

    // Test with different dirty block counts
    for dirty_blocks in [0u64, 10, 100, 500] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("local_flush", dirty_blocks),
            &dirty_blocks,
            |b, &blocks| {
                let h = rt.block_on(BenchHarness::new(100)); // 100MB device

                // Write `blocks` worth of data (these become dirty)
                let data = vec![42u8; BLOCK_SIZE];
                for i in 0..blocks {
                    h.cache
                        .write(i * BLOCK_SIZE as u64, &data, h.clean_cache.as_ref())
                        .unwrap();
                }

                // Benchmark flush - should be ~constant time
                b.iter(|| {
                    h.cache.flush().unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Compare local flush vs flush to S3.
///
/// This shows the difference between:
/// - flush(): Local SSD sync (~10ms)
/// - flush_to_s3(): Full S3 sync (100ms+ depending on data volume)
fn bench_flush_vs_s3(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("flush_vs_s3");

    let dirty_blocks = 50u64;

    // Local flush benchmark
    group.bench_function("local_flush_50_blocks", |b| {
        let h = rt.block_on(BenchHarness::new(100));

        let data = vec![42u8; BLOCK_SIZE];
        for i in 0..dirty_blocks {
            h.cache
                .write(i * BLOCK_SIZE as u64, &data, h.clean_cache.as_ref())
                .unwrap();
        }

        b.iter(|| {
            h.cache.flush().unwrap();
        });
    });

    // S3 flush benchmark (what write-through would cost)
    group.bench_function("s3_flush_50_blocks", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let h = BenchHarness::new(100).await;

                let data = vec![42u8; BLOCK_SIZE];
                for i in 0..dirty_blocks {
                    h.cache
                        .write(i * BLOCK_SIZE as u64, &data, h.clean_cache.as_ref())
                        .unwrap();
                }

                let start = std::time::Instant::now();
                h.cache
                    .flush_to_s3(
                        &h.content_store,
                        &h.chunk_meta_cache,
                        &h.volume_manifest,
                    )
                    .await
                    .unwrap();
                total += start.elapsed();
            }

            total
        });
    });

    group.finish();
}

/// Benchmark: Write throughput (local cache).
///
/// Shows that write throughput is limited by local SSD speed, not S3.
fn bench_write_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("write_throughput");

    let block_size = BLOCK_SIZE;
    group.throughput(Throughput::Bytes(block_size as u64));

    group.bench_function("sequential_128kb_writes", |b| {
        let h = rt.block_on(BenchHarness::new(100));
        let data = vec![42u8; block_size];
        let mut offset = 0u64;

        b.iter(|| {
            h.cache
                .write(offset, &data, h.clean_cache.as_ref())
                .unwrap();
            offset = (offset + block_size as u64) % (100 * 1024 * 1024);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_flush_is_local_only,
    bench_flush_vs_s3,
    bench_write_throughput
);
criterion_main!(benches);
