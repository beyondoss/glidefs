//! Targeted benchmark: old vs new flush compression allocation pattern.
//!
//! Isolates JUST the allocation difference:
//! - Old: 500 × lz4_flex::compress_prepend_size() → separate Vec per block
//! - New: BytesMut arena + compress_into + split → shared backing, ~8 allocs
//!
//! Run: cargo bench --bench alloc_pattern

use std::hint::black_box;

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const BLOCK_SIZE: usize = 128 * 1024; // 128 KB production block size

/// Old pattern: one Vec allocation per block via lz4_flex::compress_prepend_size
fn compress_old_pattern(blocks: &[Vec<u8>]) -> Vec<Bytes> {
    blocks
        .iter()
        .map(|block| Bytes::from(lz4_flex::compress_prepend_size(block)))
        .collect()
}

/// New pattern: per-thread BytesMut arena, compress_into + split
fn compress_new_pattern(blocks: &[Vec<u8>]) -> Vec<Bytes> {
    let max_compressed = lz4_flex::block::get_maximum_output_size(BLOCK_SIZE) + 4;
    let mut arena = BytesMut::with_capacity(max_compressed * blocks.len());

    blocks
        .iter()
        .map(|block| {
            let needed = max_compressed;
            if arena.capacity() - arena.len() < needed {
                arena = BytesMut::with_capacity(needed * 64);
            }
            let start = arena.len();
            arena.extend_from_slice(&(block.len() as u32).to_le_bytes());
            arena.resize(start + needed, 0);
            let n = lz4_flex::block::compress_into(block, &mut arena[start + 4..])
                .expect("arena sized for worst case");
            arena.truncate(start + 4 + n);
            arena.split().freeze()
        })
        .collect()
}

fn bench_flush_compress_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_compress_pattern");

    for num_blocks in [100usize, 500, 1024] {
        // Pre-generate realistic block data (random-ish, ~50% compressible)
        let blocks: Vec<Vec<u8>> = (0..num_blocks)
            .map(|i| {
                let mut data = vec![0u8; BLOCK_SIZE];
                // Fill with semi-compressible pattern
                for (j, byte) in data.iter_mut().enumerate() {
                    *byte = ((i * 31 + j * 7) % 256) as u8;
                }
                data
            })
            .collect();

        group.throughput(Throughput::Bytes((num_blocks * BLOCK_SIZE) as u64));

        group.bench_with_input(
            BenchmarkId::new("old_vec_per_block", num_blocks),
            &blocks,
            |b, blocks| {
                b.iter(|| {
                    let results = compress_old_pattern(blocks);
                    black_box(&results);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("new_arena_bytes", num_blocks),
            &blocks,
            |b, blocks| {
                b.iter(|| {
                    let results = compress_new_pattern(blocks);
                    black_box(&results);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark the dedup clone pattern: old (Vec::clone = memcpy) vs new (Bytes::clone = Arc bump)
fn bench_dedup_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("dedup_clone");

    // Simulate 500 blocks, 10% duplicate (50 clones of ~50KB compressed)
    let compressed_size = 50 * 1024;
    let num_clones = 50;

    // Old: Vec<u8> clone
    let vec_data = vec![0xABu8; compressed_size];
    group.bench_function("old_vec_clone_50x", |b| {
        b.iter(|| {
            let clones: Vec<Vec<u8>> = (0..num_clones).map(|_| vec_data.clone()).collect();
            black_box(&clones);
        });
    });

    // New: Bytes clone (Arc bump)
    let bytes_data = Bytes::from(vec![0xABu8; compressed_size]);
    group.bench_function("new_bytes_clone_50x", |b| {
        b.iter(|| {
            let clones: Vec<Bytes> = (0..num_clones).map(|_| bytes_data.clone()).collect();
            black_box(&clones);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_flush_compress_pattern, bench_dedup_clone);
criterion_main!(benches);
