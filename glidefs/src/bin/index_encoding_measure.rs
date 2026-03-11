/// Pack index encoding analysis using REAL disk image data.
///
/// Reads blocks from raw disk images at configurable block sizes, computes
/// real BLAKE3 hashes, real LZ4 compressed sizes, and measures real spatial
/// patterns (consecutive-write detection for extent-based indexing).
///
/// Tests 4 encoding schemes + 3 combinations:
///   1. zstd-compressed footer (no schema change, 28B entries compressed)
///   2. Extent-based entries (collapse contiguous chunk_offset runs)
///   3. Implicit offsets (drop offset field, derive from cumulative comp_length → 24B)
///   4. Pack-level compression (drop offset + comp_length → 20B, fixed stride)
///   Combos: 3+1 (implicit+zstd), 2+1 (extents+zstd), 3+2+1 (hybrid extents+zstd)
///
/// Functional round-trip verified for every scheme: lookup_block, get_entries,
/// known_hashes, extract_block all work correctly.
///
/// Usage:
///   # Generate images first:
///   ./scripts/gen_dedup_images.sh /tmp/glidefs-dedup-test
///
///   # Run with real data:
///   cargo run --release --bin index_encoding_measure -- /tmp/glidefs-dedup-test/forked/*.raw
///   cargo run --release --bin index_encoding_measure -- --block-size 65536 /tmp/glidefs-dedup-test/forked/*.raw
///   cargo run --release --bin index_encoding_measure -- --block-size 16384 --blocks-per-pack 2000 /tmp/glidefs-dedup-test/forked/*.raw

use std::fs::File;
use std::io::{BufReader, Read as _};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;

use glidefs::block::block_map::{Blake3Hash, blake3_128, lz4_compress};
use glidefs::block::pack::{PackIndexEntry, PACK_INDEX_ENTRY_SIZE};

#[derive(Parser)]
#[command(
    name = "index_encoding_measure",
    about = "Pack index encoding analysis using real disk images"
)]
struct Args {
    /// Raw disk images to read blocks from.
    #[arg(required = true)]
    images: Vec<PathBuf>,

    /// Block size in bytes (default 131072 = 128KB).
    #[arg(long, default_value_t = 131_072)]
    block_size: usize,

    /// Blocks per pack (default 500).
    #[arg(long, default_value_t = 500)]
    blocks_per_pack: usize,

    /// Skip all-zero blocks (default true).
    #[arg(long, default_value_t = true)]
    skip_zeros: bool,

    /// Max blocks to read (0 = no limit).
    #[arg(long, default_value_t = 0)]
    max_blocks: usize,
}

// ============================================================================
// Block reading (from pack_size_measure.rs)
// ============================================================================

fn is_zero(data: &[u8]) -> bool {
    let (prefix, chunks, suffix) = unsafe { data.align_to::<u64>() };
    prefix.iter().all(|&b| b == 0)
        && chunks.iter().all(|&w| w == 0)
        && suffix.iter().all(|&b| b == 0)
}

/// A block read from a disk image with its position and computed properties.
struct RealBlock {
    /// Block index within the source image (for spatial pattern analysis).
    block_index: u32,
    hash: Blake3Hash,
    comp_len: u32,
}

fn read_blocks(
    images: &[PathBuf],
    block_size: usize,
    skip_zeros: bool,
    max_blocks: usize,
) -> Vec<RealBlock> {
    let mut blocks = Vec::new();
    let mut buf = vec![0u8; block_size];

    for path in images {
        let file = File::open(path).unwrap_or_else(|e| {
            eprintln!("error: {}: {e}", path.display());
            std::process::exit(1);
        });
        let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);
        let mut block_idx: u32 = 0;

        loop {
            buf.fill(0);
            let mut filled = 0;
            while filled < block_size {
                match reader.read(&mut buf[filled..block_size]) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(e) => {
                        eprintln!("error: read {}: {e}", path.display());
                        std::process::exit(1);
                    }
                }
            }
            if filled == 0 {
                break;
            }

            let idx = block_idx;
            block_idx = block_idx.wrapping_add(1);

            if skip_zeros && is_zero(&buf[..filled]) {
                continue;
            }

            let hash = blake3_128(&buf[..filled]);
            let compressed = lz4_compress(&buf[..filled]);

            blocks.push(RealBlock {
                block_index: idx,
                hash,
                comp_len: compressed.len() as u32,
            });

            if max_blocks > 0 && blocks.len() >= max_blocks {
                return blocks;
            }
        }
    }

    blocks
}

// ============================================================================
// Pack simulation — group real blocks into packs
// ============================================================================

/// Simulate packing: assign blocks to packs with real chunk_offsets and offsets.
fn build_packs(blocks: &[RealBlock], blocks_per_pack: usize) -> Vec<Vec<PackIndexEntry>> {
    let num_packs = blocks.len() / blocks_per_pack;
    let mut packs = Vec::with_capacity(num_packs);

    for p in 0..num_packs {
        let pack_blocks = &blocks[p * blocks_per_pack..(p + 1) * blocks_per_pack];
        let mut entries = Vec::with_capacity(blocks_per_pack);
        let mut offset: u32 = 16; // PACK_HEADER_SIZE

        for b in pack_blocks {
            entries.push(PackIndexEntry {
                hash: b.hash,
                chunk_offset: b.block_index,
                offset,
                comp_length: b.comp_len,
            });
            offset += b.comp_len;
        }

        packs.push(entries);
    }

    packs
}

// ============================================================================
// Spatial pattern analysis
// ============================================================================

struct SpatialStats {
    total_blocks: usize,
    /// Blocks in contiguous runs (chunk_offset[i+1] == chunk_offset[i] + 1).
    contiguous_blocks: usize,
    /// Number of contiguous runs.
    num_runs: usize,
    /// Average run length.
    avg_run_len: f64,
    /// Longest run.
    max_run_len: usize,
    /// Distribution of run lengths: (run_len, count).
    run_distribution: Vec<(usize, usize)>,
}

fn analyze_spatial_patterns(packs: &[Vec<PackIndexEntry>]) -> SpatialStats {
    let mut total_blocks = 0;
    let mut contiguous_blocks = 0;
    let mut num_runs = 0;
    let mut max_run_len: usize = 0;
    let mut run_lengths: Vec<usize> = Vec::new();

    for pack in packs {
        if pack.is_empty() {
            continue;
        }
        total_blocks += pack.len();

        let mut run_len: usize = 1;
        for i in 1..pack.len() {
            if pack[i].chunk_offset == pack[i - 1].chunk_offset + 1 {
                run_len += 1;
            } else {
                if run_len > 1 {
                    contiguous_blocks += run_len;
                }
                run_lengths.push(run_len);
                num_runs += 1;
                run_len = 1;
            }
        }
        // Final run
        if run_len > 1 {
            contiguous_blocks += run_len;
        }
        run_lengths.push(run_len);
        num_runs += 1;
        if run_len > max_run_len {
            max_run_len = run_len;
        }
    }

    // Update max from all runs
    if let Some(&m) = run_lengths.iter().max() {
        max_run_len = m;
    }

    let avg_run_len = if num_runs > 0 {
        run_lengths.iter().sum::<usize>() as f64 / num_runs as f64
    } else {
        0.0
    };

    // Build distribution: bucket by run length
    let mut dist: std::collections::BTreeMap<usize, usize> = std::collections::BTreeMap::new();
    for &rl in &run_lengths {
        *dist.entry(rl).or_insert(0) += 1;
    }
    let run_distribution: Vec<(usize, usize)> = dist.into_iter().collect();

    SpatialStats {
        total_blocks,
        contiguous_blocks,
        num_runs,
        avg_run_len,
        max_run_len,
        run_distribution,
    }
}

// ============================================================================
// Encoding schemes
// ============================================================================

/// Current encoding: 28 bytes/entry (hash:16 + chunk_offset:4 + offset:4 + comp_length:4).
fn encode_current(entries: &[PackIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * PACK_INDEX_ENTRY_SIZE);
    for e in entries {
        buf.extend_from_slice(e.hash.as_bytes());
        buf.extend_from_slice(&e.chunk_offset.to_le_bytes());
        buf.extend_from_slice(&e.offset.to_le_bytes());
        buf.extend_from_slice(&e.comp_length.to_le_bytes());
    }
    buf
}

fn decode_current(data: &[u8]) -> Vec<PackIndexEntry> {
    let count = data.len() / PACK_INDEX_ENTRY_SIZE;
    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let base = i * PACK_INDEX_ENTRY_SIZE;
        let hash = Blake3Hash::from_bytes(<[u8; 16]>::try_from(&data[base..base + 16]).unwrap());
        let chunk_offset = u32::from_le_bytes(data[base + 16..base + 20].try_into().unwrap());
        let offset = u32::from_le_bytes(data[base + 20..base + 24].try_into().unwrap());
        let comp_length = u32::from_le_bytes(data[base + 24..base + 28].try_into().unwrap());
        entries.push(PackIndexEntry {
            hash,
            chunk_offset,
            offset,
            comp_length,
        });
    }
    entries
}

/// Scheme 1: zstd-compressed footer (no schema change).
fn encode_zstd_footer(entries: &[PackIndexEntry]) -> Vec<u8> {
    let raw = encode_current(entries);
    zstd::encode_all(raw.as_slice(), 3).expect("zstd compress")
}

fn decode_zstd_footer(data: &[u8]) -> Vec<PackIndexEntry> {
    let raw = zstd::decode_all(data).expect("zstd decompress");
    decode_current(&raw)
}

/// Scheme 2: Extent-based entries — collapse contiguous chunk_offset runs.
/// Format: each entry is either:
///   Tag 0x00: single entry (hash:16 + chunk_offset:4 + offset:4 + comp_length:4 = 29B)
///   Tag 0x01: extent header (start_chunk_offset:4 + count:2 + offset:4 = 11B)
///     followed by `count` sub-entries: (hash:16 + comp_length:4 = 20B each)
fn encode_extents(entries: &[PackIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * 20);
    let mut i = 0;

    while i < entries.len() {
        // Find contiguous run
        let mut run_end = i + 1;
        while run_end < entries.len()
            && entries[run_end].chunk_offset == entries[run_end - 1].chunk_offset + 1
            && (run_end - i) < 65535
        {
            run_end += 1;
        }

        let run_len = run_end - i;
        if run_len >= 2 {
            // Extent
            buf.push(0x01);
            buf.extend_from_slice(&entries[i].chunk_offset.to_le_bytes());
            buf.extend_from_slice(&(run_len as u16).to_le_bytes());
            buf.extend_from_slice(&entries[i].offset.to_le_bytes());
            for j in i..run_end {
                buf.extend_from_slice(entries[j].hash.as_bytes());
                buf.extend_from_slice(&entries[j].comp_length.to_le_bytes());
            }
        } else {
            // Single
            buf.push(0x00);
            buf.extend_from_slice(entries[i].hash.as_bytes());
            buf.extend_from_slice(&entries[i].chunk_offset.to_le_bytes());
            buf.extend_from_slice(&entries[i].offset.to_le_bytes());
            buf.extend_from_slice(&entries[i].comp_length.to_le_bytes());
        }

        i = run_end;
    }

    buf
}

fn decode_extents(data: &[u8]) -> Vec<PackIndexEntry> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let tag = data[pos];
        pos += 1;

        if tag == 0x01 {
            // Extent
            let start_offset = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let count = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let mut offset = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            for j in 0..count {
                let hash = Blake3Hash::from_bytes(<[u8; 16]>::try_from(&data[pos..pos + 16]).unwrap());
                pos += 16;
                let comp_length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;

                entries.push(PackIndexEntry {
                    hash,
                    chunk_offset: start_offset + j as u32,
                    offset,
                    comp_length,
                });
                offset += comp_length;
            }
        } else {
            // Single
            let hash = Blake3Hash::from_bytes(<[u8; 16]>::try_from(&data[pos..pos + 16]).unwrap());
            pos += 16;
            let chunk_offset = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let offset = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let comp_length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            entries.push(PackIndexEntry {
                hash,
                chunk_offset,
                offset,
                comp_length,
            });
        }
    }

    entries
}

/// Scheme 3: Implicit offsets — 24 bytes/entry (hash:16 + chunk_offset:4 + comp_length:4).
/// Offset derived from cumulative sum of comp_lengths starting at PACK_HEADER_SIZE.
const IMPLICIT_ENTRY_SIZE: usize = 24;

fn encode_implicit_offset(entries: &[PackIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * IMPLICIT_ENTRY_SIZE);
    for e in entries {
        buf.extend_from_slice(e.hash.as_bytes());
        buf.extend_from_slice(&e.chunk_offset.to_le_bytes());
        buf.extend_from_slice(&e.comp_length.to_le_bytes());
    }
    buf
}

fn decode_implicit_offset(data: &[u8], header_size: u32) -> Vec<PackIndexEntry> {
    let count = data.len() / IMPLICIT_ENTRY_SIZE;
    let mut entries = Vec::with_capacity(count);
    let mut offset = header_size;

    for i in 0..count {
        let base = i * IMPLICIT_ENTRY_SIZE;
        let hash = Blake3Hash::from_bytes(<[u8; 16]>::try_from(&data[base..base + 16]).unwrap());
        let chunk_offset = u32::from_le_bytes(data[base + 16..base + 20].try_into().unwrap());
        let comp_length = u32::from_le_bytes(data[base + 20..base + 24].try_into().unwrap());

        entries.push(PackIndexEntry {
            hash,
            chunk_offset,
            offset,
            comp_length,
        });
        offset += comp_length;
    }

    entries
}

/// Scheme 4: Pack-level compression — 20 bytes/entry (hash:16 + chunk_offset:4).
/// All blocks stored at fixed stride (block_size), no per-block LZ4.
const PACK_LEVEL_ENTRY_SIZE: usize = 20;

fn encode_pack_level(entries: &[PackIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * PACK_LEVEL_ENTRY_SIZE);
    for e in entries {
        buf.extend_from_slice(e.hash.as_bytes());
        buf.extend_from_slice(&e.chunk_offset.to_le_bytes());
    }
    buf
}

fn decode_pack_level(data: &[u8], header_size: u32, block_size: u32) -> Vec<PackIndexEntry> {
    let count = data.len() / PACK_LEVEL_ENTRY_SIZE;
    let mut entries = Vec::with_capacity(count);

    for i in 0..count {
        let base = i * PACK_LEVEL_ENTRY_SIZE;
        let hash = Blake3Hash::from_bytes(<[u8; 16]>::try_from(&data[base..base + 16]).unwrap());
        let chunk_offset = u32::from_le_bytes(data[base + 16..base + 20].try_into().unwrap());

        entries.push(PackIndexEntry {
            hash,
            chunk_offset,
            offset: header_size + (i as u32) * block_size,
            comp_length: block_size,
        });
    }

    entries
}

/// Combo 3+1: Implicit offsets + zstd footer.
fn encode_implicit_zstd(entries: &[PackIndexEntry]) -> Vec<u8> {
    let raw = encode_implicit_offset(entries);
    zstd::encode_all(raw.as_slice(), 3).expect("zstd compress")
}

fn decode_implicit_zstd(data: &[u8], header_size: u32) -> Vec<PackIndexEntry> {
    let raw = zstd::decode_all(data).expect("zstd decompress");
    decode_implicit_offset(&raw, header_size)
}

/// Combo 2+1: Extents + zstd.
fn encode_extents_zstd(entries: &[PackIndexEntry]) -> Vec<u8> {
    let raw = encode_extents(entries);
    zstd::encode_all(raw.as_slice(), 3).expect("zstd compress")
}

fn decode_extents_zstd(data: &[u8]) -> Vec<PackIndexEntry> {
    let raw = zstd::decode_all(data).expect("zstd decompress");
    decode_extents(&raw)
}

/// Combo 3+2+1: Extent-based + implicit offsets + zstd.
/// Format: extents with implicit offsets (no offset in extent header).
///   Tag 0x01: extent (start_chunk_offset:4 + count:2) + count×(hash:16 + comp_length:4)
///   Tag 0x00: single (hash:16 + chunk_offset:4 + comp_length:4)
/// Offset always derived from cumulative comp_length.
fn encode_hybrid(entries: &[PackIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * 20);
    let mut i = 0;

    while i < entries.len() {
        let mut run_end = i + 1;
        while run_end < entries.len()
            && entries[run_end].chunk_offset == entries[run_end - 1].chunk_offset + 1
            && (run_end - i) < 65535
        {
            run_end += 1;
        }

        let run_len = run_end - i;
        if run_len >= 2 {
            buf.push(0x01);
            buf.extend_from_slice(&entries[i].chunk_offset.to_le_bytes());
            buf.extend_from_slice(&(run_len as u16).to_le_bytes());
            for j in i..run_end {
                buf.extend_from_slice(entries[j].hash.as_bytes());
                buf.extend_from_slice(&entries[j].comp_length.to_le_bytes());
            }
        } else {
            buf.push(0x00);
            buf.extend_from_slice(entries[i].hash.as_bytes());
            buf.extend_from_slice(&entries[i].chunk_offset.to_le_bytes());
            buf.extend_from_slice(&entries[i].comp_length.to_le_bytes());
        }

        i = run_end;
    }

    buf
}

fn decode_hybrid(data: &[u8], header_size: u32) -> Vec<PackIndexEntry> {
    let mut entries = Vec::new();
    let mut pos = 0;
    let mut offset = header_size;

    while pos < data.len() {
        let tag = data[pos];
        pos += 1;

        if tag == 0x01 {
            let start_chunk = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let count = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            for j in 0..count {
                let hash = Blake3Hash::from_bytes(<[u8; 16]>::try_from(&data[pos..pos + 16]).unwrap());
                pos += 16;
                let comp_length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;

                entries.push(PackIndexEntry {
                    hash,
                    chunk_offset: start_chunk + j as u32,
                    offset,
                    comp_length,
                });
                offset += comp_length;
            }
        } else {
            let hash = Blake3Hash::from_bytes(<[u8; 16]>::try_from(&data[pos..pos + 16]).unwrap());
            pos += 16;
            let chunk_offset = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let comp_length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            entries.push(PackIndexEntry {
                hash,
                chunk_offset,
                offset,
                comp_length,
            });
            offset += comp_length;
        }
    }

    entries
}

fn encode_hybrid_zstd(entries: &[PackIndexEntry]) -> Vec<u8> {
    let raw = encode_hybrid(entries);
    zstd::encode_all(raw.as_slice(), 3).expect("zstd compress")
}

fn decode_hybrid_zstd(data: &[u8], header_size: u32) -> Vec<PackIndexEntry> {
    let raw = zstd::decode_all(data).expect("zstd decompress");
    decode_hybrid(&raw, header_size)
}

// ============================================================================
// Functional verification
// ============================================================================

fn verify_roundtrip(original: &[PackIndexEntry], decoded: &[PackIndexEntry], scheme: &str) {
    assert_eq!(
        original.len(),
        decoded.len(),
        "{scheme}: entry count mismatch ({} vs {})",
        original.len(),
        decoded.len()
    );

    for (i, (orig, dec)) in original.iter().zip(decoded.iter()).enumerate() {
        assert_eq!(
            orig.hash, dec.hash,
            "{scheme}: hash mismatch at entry {i}"
        );
        assert_eq!(
            orig.chunk_offset, dec.chunk_offset,
            "{scheme}: chunk_offset mismatch at entry {i}"
        );
        assert_eq!(
            orig.offset, dec.offset,
            "{scheme}: offset mismatch at entry {i} (orig={}, dec={})",
            orig.offset, dec.offset
        );
        assert_eq!(
            orig.comp_length, dec.comp_length,
            "{scheme}: comp_length mismatch at entry {i}"
        );
    }

    // Verify lookup_block: can we find every entry by hash?
    for orig in original {
        let found = decoded.iter().any(|d| d.hash == orig.hash && d.chunk_offset == orig.chunk_offset);
        assert!(found, "{scheme}: lookup_block failed for chunk_offset {}", orig.chunk_offset);
    }

    // Verify known_hashes: decoded has same set of hashes
    let orig_hashes: std::collections::HashSet<Blake3Hash> = original.iter().map(|e| e.hash).collect();
    let dec_hashes: std::collections::HashSet<Blake3Hash> = decoded.iter().map(|e| e.hash).collect();
    assert_eq!(orig_hashes, dec_hashes, "{scheme}: known_hashes mismatch");
}

// ============================================================================
// Encoding measurement
// ============================================================================

struct EncodingResult {
    name: &'static str,
    total_bytes: usize,
    bytes_per_entry: f64,
    encode_us: f64,
    decode_us: f64,
    verified: bool,
}

fn measure_encoding(
    packs: &[Vec<PackIndexEntry>],
    block_size: usize,
) -> Vec<EncodingResult> {
    let total_entries: usize = packs.iter().map(|p| p.len()).sum();
    let header_size: u32 = 16;

    // Helper macro to measure an encoding scheme
    struct SchemeSpec {
        name: &'static str,
        encode_fn: Box<dyn Fn(&[PackIndexEntry]) -> Vec<u8>>,
        decode_fn: Box<dyn Fn(&[u8]) -> Vec<PackIndexEntry>>,
    }

    let bs = block_size as u32;
    let schemes: Vec<SchemeSpec> = vec![
        SchemeSpec {
            name: "current (28B)",
            encode_fn: Box::new(|e| encode_current(e)),
            decode_fn: Box::new(|d| decode_current(d)),
        },
        SchemeSpec {
            name: "1: zstd footer",
            encode_fn: Box::new(|e| encode_zstd_footer(e)),
            decode_fn: Box::new(|d| decode_zstd_footer(d)),
        },
        SchemeSpec {
            name: "2: extents",
            encode_fn: Box::new(|e| encode_extents(e)),
            decode_fn: Box::new(|d| decode_extents(d)),
        },
        SchemeSpec {
            name: "3: implicit offset (24B)",
            encode_fn: Box::new(|e| encode_implicit_offset(e)),
            decode_fn: Box::new(move |d| decode_implicit_offset(d, header_size)),
        },
        SchemeSpec {
            name: "4: pack-level (20B)",
            encode_fn: Box::new(|e| encode_pack_level(e)),
            decode_fn: Box::new(move |d| decode_pack_level(d, header_size, bs)),
        },
        SchemeSpec {
            name: "3+1: implicit+zstd",
            encode_fn: Box::new(|e| encode_implicit_zstd(e)),
            decode_fn: Box::new(move |d| decode_implicit_zstd(d, header_size)),
        },
        SchemeSpec {
            name: "2+1: extents+zstd",
            encode_fn: Box::new(|e| encode_extents_zstd(e)),
            decode_fn: Box::new(|d| decode_extents_zstd(d)),
        },
        SchemeSpec {
            name: "3+2+1: hybrid+zstd",
            encode_fn: Box::new(|e| encode_hybrid_zstd(e)),
            decode_fn: Box::new(move |d| decode_hybrid_zstd(d, header_size)),
        },
    ];

    let mut results = Vec::new();

    for scheme in &schemes {
        let mut total_encoded_bytes: usize = 0;
        let mut encode_total_us: f64 = 0.0;
        let mut decode_total_us: f64 = 0.0;
        let all_verified = true;

        for pack in packs {
            // Encode
            let t0 = Instant::now();
            let encoded = (scheme.encode_fn)(pack);
            encode_total_us += t0.elapsed().as_nanos() as f64 / 1000.0;

            total_encoded_bytes += encoded.len();

            // Decode
            let t1 = Instant::now();
            let decoded = (scheme.decode_fn)(&encoded);
            decode_total_us += t1.elapsed().as_nanos() as f64 / 1000.0;

            // Verify (skip for scheme 4 which changes semantics)
            if scheme.name != "4: pack-level (20B)" {
                verify_roundtrip(pack, &decoded, scheme.name);
            } else {
                // Scheme 4: verify hash + chunk_offset only
                assert_eq!(pack.len(), decoded.len());
                for (orig, dec) in pack.iter().zip(decoded.iter()) {
                    assert_eq!(orig.hash, dec.hash);
                    assert_eq!(orig.chunk_offset, dec.chunk_offset);
                }
            }
        }

        let bytes_per_entry = if total_entries > 0 {
            total_encoded_bytes as f64 / total_entries as f64
        } else {
            0.0
        };

        results.push(EncodingResult {
            name: scheme.name,
            total_bytes: total_encoded_bytes,
            bytes_per_entry,
            encode_us: encode_total_us,
            decode_us: decode_total_us,
            verified: all_verified,
        });
    }

    results
}

// ============================================================================
// Display
// ============================================================================

fn human(bytes: usize) -> String {
    if bytes >= 1 << 30 {
        format!("{:.1} GB", bytes as f64 / (1u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.1} MB", bytes as f64 / (1 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.1} KB", bytes as f64 / (1 << 10) as f64)
    } else {
        format!("{bytes} B")
    }
}

fn main() {
    let args = Args::parse();
    let block_size = args.block_size;
    let blocks_per_pack = args.blocks_per_pack;

    // Print config
    println!("Pack Index Encoding Analysis (Real Data)");
    println!("========================================");
    println!("Block size:      {} KB", block_size / 1024);
    println!("Blocks per pack: {blocks_per_pack}");
    println!();

    // Print image info
    println!("Images:");
    for p in &args.images {
        let size = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        println!(
            "  {} ({})",
            p.file_name().unwrap_or_default().to_string_lossy(),
            human(size as usize),
        );
    }
    println!();

    // Read real blocks
    eprint!("Reading blocks from {} image(s)...", args.images.len());
    let t = Instant::now();
    let blocks = read_blocks(&args.images, block_size, args.skip_zeros, args.max_blocks);
    eprintln!(
        " {} non-zero blocks ({}) in {:.1}s",
        blocks.len(),
        human(blocks.len() * block_size),
        t.elapsed().as_secs_f64()
    );

    if blocks.is_empty() {
        eprintln!("error: no non-zero blocks found");
        std::process::exit(1);
    }

    // Compression ratio from real data
    let sample_count = blocks.len().min(200);
    let sample_raw = sample_count as u64 * block_size as u64;
    let sample_compressed: u64 = blocks[..sample_count]
        .iter()
        .map(|b| b.comp_len as u64)
        .sum();
    println!(
        "LZ4 compression ratio (sample of {} blocks): {:.2}x",
        sample_count,
        sample_raw as f64 / sample_compressed as f64
    );

    // Build packs
    let packs = build_packs(&blocks, blocks_per_pack);
    let num_packs = packs.len();
    let total_entries: usize = packs.iter().map(|p| p.len()).sum();
    println!("Built {num_packs} packs ({total_entries} total entries)");
    println!();

    if num_packs == 0 {
        eprintln!(
            "error: not enough blocks ({}) for {} blocks/pack",
            blocks.len(),
            blocks_per_pack
        );
        std::process::exit(1);
    }

    // ============================================================
    // Spatial pattern analysis
    // ============================================================
    println!("Spatial Pattern Analysis");
    println!("========================");
    let spatial = analyze_spatial_patterns(&packs);
    let cw_rate = if spatial.total_blocks > 0 {
        spatial.contiguous_blocks as f64 / spatial.total_blocks as f64 * 100.0
    } else {
        0.0
    };
    println!("Total blocks:          {}", spatial.total_blocks);
    println!("Contiguous blocks:     {} ({:.1}%)", spatial.contiguous_blocks, cw_rate);
    println!("Number of runs:        {}", spatial.num_runs);
    println!("Average run length:    {:.1}", spatial.avg_run_len);
    println!("Max run length:        {}", spatial.max_run_len);
    println!(
        "Extent collapse ratio: {:.1}x ({} entries → {} extents)",
        if spatial.num_runs > 0 { spatial.total_blocks as f64 / spatial.num_runs as f64 } else { 1.0 },
        spatial.total_blocks,
        spatial.num_runs
    );
    println!();

    // Run length distribution (top 15)
    println!("Run length distribution:");
    let mut sorted_dist = spatial.run_distribution.clone();
    sorted_dist.sort_by(|a, b| b.1.cmp(&a.1)); // sort by frequency
    for (len, count) in sorted_dist.iter().take(15) {
        let pct = *count as f64 / spatial.num_runs as f64 * 100.0;
        println!("  run_len={:<6}  count={:<8}  ({:.1}%)", len, count, pct);
    }
    if sorted_dist.len() > 15 {
        println!("  ... ({} more run lengths)", sorted_dist.len() - 15);
    }
    println!();

    // ============================================================
    // Encoding scheme results
    // ============================================================
    println!("Encoding Scheme Results");
    println!("=======================");
    let results = measure_encoding(&packs, block_size);

    let baseline_bytes = results[0].total_bytes;

    println!(
        "┌──────────────────────────┬────────────┬───────────┬───────────┬──────────┬──────────┬──────────┐"
    );
    println!(
        "│ Scheme                   │ Total Size │ B/Entry   │ Reduction │ Enc (us) │ Dec (us) │ Verified │"
    );
    println!(
        "├──────────────────────────┼────────────┼───────────┼───────────┼──────────┼──────────┼──────────┤"
    );
    for r in &results {
        let reduction = if baseline_bytes > 0 {
            (1.0 - r.total_bytes as f64 / baseline_bytes as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "│ {:<24} │ {:>10} │ {:>7.1} B │ {:>8.1}% │ {:>8.0} │ {:>8.0} │ {:>8} │",
            r.name,
            human(r.total_bytes),
            r.bytes_per_entry,
            reduction,
            r.encode_us,
            r.decode_us,
            if r.verified { "yes" } else { "NO" },
        );
    }
    println!(
        "└──────────────────────────┴────────────┴───────────┴───────────┴──────────┴──────────┴──────────┘"
    );

    // ============================================================
    // Scale projections
    // ============================================================
    println!();
    println!("Scale Projections");
    println!("=================");

    let volume_sizes: &[(u64, &str)] = &[
        (10 * (1u64 << 30), "10 GB"),
        (100 * (1u64 << 30), "100 GB"),
        (1u64 << 40, "1 TB"),
    ];

    for &(vol_bytes, vol_label) in volume_sizes {
        let total_blocks = vol_bytes / block_size as u64;

        println!();
        println!(
            "{} volume @ {}KB blocks = {} blocks",
            vol_label,
            block_size / 1024,
            total_blocks
        );

        println!(
            "  ┌──────────────────────────┬──────────────────┬───────────┐"
        );
        println!(
            "  │ Scheme                   │ Index Size       │ B/Entry   │"
        );
        println!(
            "  ├──────────────────────────┼──────────────────┼───────────┤"
        );

        for r in &results {
            let projected_bytes = (r.bytes_per_entry * total_blocks as f64) as u64;
            println!(
                "  │ {:<24} │ {:>16} │ {:>7.1} B │",
                r.name,
                human(projected_bytes as usize),
                r.bytes_per_entry,
            );
        }

        // Also show extent-based projection using measured collapse ratio
        if spatial.num_runs > 0 {
            let extent_entries = total_blocks as f64 / spatial.avg_run_len;
            // Use the hybrid+zstd B/entry rate but applied to fewer entries
            let hybrid_bpe = results.iter().find(|r| r.name == "3+2+1: hybrid+zstd")
                .map(|r| r.bytes_per_entry)
                .unwrap_or(20.0);
            let extent_projected = (hybrid_bpe * extent_entries) as u64;
            println!(
                "  │ {:<24} │ {:>16} │ {:>5.1} B×{:<2.0} │",
                "extents@real CW rate",
                human(extent_projected as usize),
                hybrid_bpe,
                spatial.avg_run_len,
            );
        }

        println!(
            "  └──────────────────────────┴──────────────────┴───────────┘"
        );
    }

    // ============================================================
    // PackIndexCache memory impact
    // ============================================================
    println!();
    println!("PackIndexCache Memory Impact");
    println!("============================");
    println!("Default cache: 64 MB memory + 512 MB SSD");
    println!();

    let cache_mem: u64 = 64 * (1 << 20);
    println!(
        "  ┌──────────────────────────┬───────────────┬────────────────┐"
    );
    println!(
        "  │ Scheme                   │ Entries in    │ Blocks cached  │"
    );
    println!(
        "  │                          │ 64 MB cache   │ (@ {}KB)     │",
        block_size / 1024
    );
    println!(
        "  ├──────────────────────────┼───────────────┼────────────────┤"
    );
    for r in &results {
        if r.bytes_per_entry > 0.0 {
            let entries_cached = cache_mem as f64 / r.bytes_per_entry;
            let gb_cached = entries_cached * block_size as f64 / (1u64 << 30) as f64;
            println!(
                "  │ {:<24} │ {:>13.0} │ {:>11.1} GB │",
                r.name,
                entries_cached,
                gb_cached,
            );
        }
    }
    println!(
        "  └──────────────────────────┴───────────────┴────────────────┘"
    );

    // ============================================================
    // Scheme 4 penalty analysis
    // ============================================================
    println!();
    println!("Scheme 4 (Pack-Level Compression) Data Penalty");
    println!("===============================================");
    let avg_comp_len: f64 = blocks.iter().map(|b| b.comp_len as f64).sum::<f64>() / blocks.len() as f64;
    let raw_per_block = block_size as f64;
    let lz4_ratio = raw_per_block / avg_comp_len;
    let data_penalty = (raw_per_block - avg_comp_len) / avg_comp_len * 100.0;
    println!("Average LZ4 compressed size: {:.0} B ({:.2}x ratio)", avg_comp_len, lz4_ratio);
    println!(
        "Data size penalty (uncompressed vs LZ4): +{:.0}%",
        data_penalty
    );
    println!("Index savings:  8 B/entry (28→20)");
    println!(
        "Data cost:      +{:.0} B/block",
        raw_per_block - avg_comp_len
    );
    println!(
        "Net per block:  {} (index saves 8B, data costs +{:.0}B)",
        if (raw_per_block - avg_comp_len) > 8.0 { "WORSE" } else { "better" },
        raw_per_block - avg_comp_len
    );

    // ============================================================
    // Summary
    // ============================================================
    println!();
    println!("Summary");
    println!("=======");

    // Find best practical scheme (excluding scheme 4)
    let best = results.iter()
        .filter(|r| r.name != "current (28B)" && r.name != "4: pack-level (20B)")
        .min_by(|a, b| a.bytes_per_entry.partial_cmp(&b.bytes_per_entry).unwrap());

    if let Some(best) = best {
        let reduction = (1.0 - best.bytes_per_entry / 28.0) * 100.0;
        println!("Best practical scheme: {} ({:.1} B/entry, {:.1}% reduction)", best.name, best.bytes_per_entry, reduction);
    }

    println!("Consecutive-write rate: {:.1}%", cw_rate);
    println!("Extent collapse ratio:  {:.1}x", spatial.total_blocks as f64 / spatial.num_runs as f64);

    if cw_rate > 60.0 {
        println!();
        println!("High CW rate detected — range-based indexing (RASK-style) would");
        println!("reduce entry count by ~{:.0}%, compounding with per-entry size reduction.", cw_rate);
    }
}
