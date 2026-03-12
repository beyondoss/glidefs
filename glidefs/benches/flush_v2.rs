//! Benchmark for v2 content-addressed flush path.
//!
//! Measures flush latency and throughput with content-addressing overhead
//! (BLAKE3 hashing, LZ4 compression, pack assembly, manifest creation).
//!
//! Run with: `cargo bench --features test-utils --bench flush_v2`

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_store::ObjectStore;
use parking_lot::RwLock;
use tempfile::TempDir;

use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::content_store::ContentStore;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::state::Active;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB (production block size)

struct V2BenchHarness {
    cache: WriteCache<Active>,
    content_store: ContentStore,
    volume_manifest: Arc<RwLock<VolumeManifest>>,
    clean_cache: Arc<dyn BlockCache>,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl V2BenchHarness {
    fn new(device_size_mb: u64) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let device_size = device_size_mb * 1024 * 1024;

        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "bench".to_string(),
            device_size,
            block_size: BLOCK_SIZE,
            wal_sync: false, bottomless: false,
        };

        let content_store = ContentStore::new(Arc::clone(&s3_backend), "bench");
        let volume_manifest = Arc::new(RwLock::new(
            VolumeManifest::new(device_size, BLOCK_SIZE as u32),
        ));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let cache = WriteCache::open(config).expect("Failed to open cache");
        let cache = cache.skip_recovery_for_test();

        Self {
            cache,
            content_store,
            volume_manifest,
            clean_cache,
            temp_dir,
        }
    }
}

/// Benchmark: v2 flush latency for varying dirty block counts.
///
/// Measures the end-to-end cost of:
/// snapshot → dedup check → LZ4 compress → pack assembly → S3 upload → manifest
fn bench_v2_flush_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("v2_flush_latency");
    group.sample_size(10);

    // Single PackIndexCache shared across all iterations (SSD tier holds FDs;
    // creating one per iteration exhausts the FD limit).
    let pic_dir = TempDir::new().unwrap();
    let pack_index_cache =
        Arc::new(rt.block_on(PackIndexCache::open(pic_dir.path())).unwrap());

    for dirty_mb in [1u64, 10, 50] {
        let dirty_blocks = dirty_mb * 1024 * 1024 / BLOCK_SIZE as u64;
        group.throughput(Throughput::Bytes(dirty_mb * 1024 * 1024));

        let pic = Arc::clone(&pack_index_cache);
        group.bench_with_input(
            BenchmarkId::new("flush", format!("{dirty_mb}MB")),
            &dirty_blocks,
            |b, &blocks| {
                b.to_async(&rt).iter_custom(|iters| {
                    let pic = Arc::clone(&pic);
                    async move {
                        let mut total = Duration::ZERO;

                        for _ in 0..iters {
                            let h = V2BenchHarness::new(256);

                            // Write dirty blocks with varied data
                            for i in 0..blocks {
                                let data: Vec<u8> = (0..BLOCK_SIZE)
                                    .map(|j| ((i as usize * 31 + j * 7) % 256) as u8)
                                    .collect();
                                h.cache
                                    .write(
                                        i * BLOCK_SIZE as u64,
                                        &data,
                                        h.clean_cache.as_ref(),
                                    )
                                    .unwrap();
                            }

                            let start = std::time::Instant::now();
                            let _stats = h
                                .cache
                                .flush_to_s3(
                                    &h.content_store,
                                    &pic,
                                    &h.volume_manifest,
                                )
                                .await
                                .unwrap();
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

/// Benchmark: Dedup effectiveness — second flush with 100% overlap.
///
/// Shows the speedup when all blocks are already in the chunk meta cache.
fn bench_v2_dedup_speedup(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("v2_dedup");
    group.sample_size(10);

    let dirty_blocks = 100u64;
    group.throughput(Throughput::Elements(dirty_blocks));

    let pic_dir = TempDir::new().unwrap();
    let pack_index_cache =
        Arc::new(rt.block_on(PackIndexCache::open(pic_dir.path())).unwrap());

    // Cold flush (nothing deduped)
    {
        let pic = Arc::clone(&pack_index_cache);
        group.bench_function("cold_100_blocks", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let mut total = Duration::ZERO;

                    for _ in 0..iters {
                        let h = V2BenchHarness::new(256);

                        for i in 0..dirty_blocks {
                            let data = vec![(i % 256) as u8; BLOCK_SIZE];
                            h.cache
                                .write(
                                    i * BLOCK_SIZE as u64,
                                    &data,
                                    h.clean_cache.as_ref(),
                                )
                                .unwrap();
                        }

                        let start = std::time::Instant::now();
                        h.cache
                            .flush_to_s3(&h.content_store, &pic, &h.volume_manifest)
                            .await
                            .unwrap();
                        total += start.elapsed();
                    }

                    total
                }
            });
        });
    }

    // Warm flush (100% deduped via chunk meta cache)
    {
        let pic = Arc::clone(&pack_index_cache);
        group.bench_function("warm_100_blocks_deduped", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let pic = Arc::clone(&pic);
                async move {
                    let mut total = Duration::ZERO;

                    for _ in 0..iters {
                        let h = V2BenchHarness::new(256);

                        // First: write and flush to populate chunk meta cache
                        for i in 0..dirty_blocks {
                            let data = vec![(i % 256) as u8; BLOCK_SIZE];
                            h.cache
                                .write(
                                    i * BLOCK_SIZE as u64,
                                    &data,
                                    h.clean_cache.as_ref(),
                                )
                                .unwrap();
                        }
                        h.cache
                            .flush_to_s3(&h.content_store, &pic, &h.volume_manifest)
                            .await
                            .unwrap();

                        // Second: write same content to different offsets
                        for i in 0..dirty_blocks {
                            let data = vec![(i % 256) as u8; BLOCK_SIZE];
                            h.cache
                                .write(
                                    (i + dirty_blocks) * BLOCK_SIZE as u64,
                                    &data,
                                    h.clean_cache.as_ref(),
                                )
                                .unwrap();
                        }

                        let start = std::time::Instant::now();
                        h.cache
                            .flush_to_s3(&h.content_store, &pic, &h.volume_manifest)
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

criterion_group!(benches, bench_v2_flush_latency, bench_v2_dedup_speedup,);
criterion_main!(benches);
