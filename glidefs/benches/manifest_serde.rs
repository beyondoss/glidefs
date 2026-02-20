//! Benchmark for manifest serialization/deserialization throughput.
//!
//! Measures how fast we can serialize and deserialize manifests of varying
//! sizes, from 1K to 500K block_map entries. Reports throughput in entries/sec.
//!
//! Run with: `cargo bench --bench manifest_serde`

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use glidefs::block::block_map::Blake3Hash;
use glidefs::block::manifest::{Manifest, ManifestBlockEntry, ManifestPackEntry};

/// Build a manifest with `num_blocks` block_map entries and a proportional
/// number of pack_index entries (~1 pack per 25 blocks).
fn make_manifest(num_blocks: usize) -> Manifest {
    let num_packs = num_blocks / 25 + 1;
    Manifest {
        name: "bench-export".to_string(),
        sequence: 42,
        chunk_size: 128 * 1024,
        device_size: (num_blocks as u64) * 128 * 1024,
        block_map: (0..num_blocks)
            .map(|i| ManifestBlockEntry {
                chunk_index: i as u64,
                hash: {
                    let mut h = [0u8; 16];
                    h[..8].copy_from_slice(&(i as u64).to_le_bytes());
                    Blake3Hash::from_bytes(h)
                },
                flags: 0,
            })
            .collect(),
        pack_index: (0..num_packs)
            .map(|i| ManifestPackEntry {
                hash: {
                    let mut h = [0u8; 16];
                    h[..8].copy_from_slice(&(i as u64).to_le_bytes());
                    Blake3Hash::from_bytes(h)
                },
                pack_id: uuid::Uuid::from_u128(i as u128),
                offset: (i * 1024) as u32,
                comp_length: 1000,
            })
            .collect(),
    }
}

const SIZES: &[usize] = &[1_000, 10_000, 100_000, 500_000];

fn manifest_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("manifest_serialize");

    for &num_blocks in SIZES {
        group.throughput(Throughput::Elements(num_blocks as u64));

        let manifest = make_manifest(num_blocks);

        group.bench_with_input(
            BenchmarkId::new("blocks", num_blocks),
            &manifest,
            |b, manifest| {
                b.iter(|| manifest.serialize());
            },
        );
    }

    group.finish();
}

fn manifest_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("manifest_deserialize");

    for &num_blocks in SIZES {
        group.throughput(Throughput::Elements(num_blocks as u64));

        let data = make_manifest(num_blocks).serialize();

        group.bench_with_input(BenchmarkId::new("blocks", num_blocks), &data, |b, data| {
            b.iter(|| Manifest::deserialize(data).unwrap());
        });
    }

    group.finish();
}

criterion_group!(benches, manifest_serialize, manifest_deserialize);
criterion_main!(benches);
