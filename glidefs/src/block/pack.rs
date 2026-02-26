//! Content-addressed pack format (GLPK) for S3 block storage.
//!
//! Packs batch multiple compressed blocks into a single S3 object to reduce
//! per-object overhead and improve throughput. Each pack is self-describing:
//! a fixed header, a block index, and concatenated LZ4-compressed block data.
//!
//! ## Wire Format
//!
//! ```text
//! Pack Header (16 bytes, fixed):
//!   magic:        [u8; 4]  = b"GLPK"
//!   version:      u16 LE   = 2
//!   block_count:  u16 LE
//!   chunk_size:   u32 LE   (uncompressed block size, e.g. 131072 = 128KB)
//!   _reserved:    [u8; 4]  = [0; 4]
//!
//! Block Index (block_count * 28 bytes):
//!   hash:           [u8; 16]  (BLAKE3-128 of uncompressed data)
//!   chunk_offset:   u32 LE    (block offset within chunk, 0–1023)
//!   offset:         u32 LE    (byte offset from start of pack file)
//!   comp_length:    u32 LE    (compressed size in bytes)
//!
//! Block Data (immediately after block index):
//!   [LZ4-compressed blocks, concatenated]
//!   Offsets in the index point into this region.
//! ```

use std::io;

use super::block_map::Blake3Hash;

pub const PACK_MAGIC: &[u8; 4] = b"GLPK";
/// Current pack version (v2, chunk-indexed).
pub const PACK_VERSION: u16 = 2;
pub const DEFAULT_BLOCKS_PER_PACK: usize = 500;
pub const PACK_HEADER_SIZE: usize = 16;
/// Index entry size (28 bytes with chunk_offset).
pub const PACK_INDEX_ENTRY_SIZE: usize = 28;

/// 8-byte random pack identifier.
///
/// Collision-safe per chunk (birthday bound ~4.3 billion).
/// Hex representation distributes uniformly for S3 prefix sharding.
pub type PackId = u64;

/// Generate a random pack ID.
pub fn new_pack_id() -> PackId {
    rand::random::<u64>()
}

// ============================================================================
// Current format: GLPK v2 — chunk-indexed packs
// ============================================================================

/// One entry in a v2 pack's block index. Includes `chunk_offset` so packs are
/// self-describing for the chunk-based architecture (no external `.meta` needed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackIndexEntry {
    pub hash: Blake3Hash,
    /// Block offset within chunk (0–1023 for 128 MiB / 128 KB).
    pub chunk_offset: u32,
    /// Byte offset from start of pack file to the compressed block data.
    pub offset: u32,
    /// Compressed size in bytes.
    pub comp_length: u32,
}

/// Parsed v2 pack header + block index.
#[derive(Debug)]
#[allow(dead_code)]
pub struct PackIndex {
    pub block_count: u16,
    pub chunk_size: u32,
    pub entries: Vec<PackIndexEntry>,
}

/// Assemble a v2 pack from pre-compressed blocks.
///
/// `blocks` contains `(hash, chunk_offset, compressed_data)` triples.
/// Entries are sorted by `chunk_offset` before writing for read locality.
/// Returns the complete pack bytes and the sorted index entries.
pub fn assemble_pack(
    mut blocks: Vec<(Blake3Hash, u32, Vec<u8>)>,
    chunk_size: u32,
) -> io::Result<(Vec<u8>, Vec<PackIndexEntry>)> {
    // Sort by chunk_offset for sequential read locality.
    blocks.sort_by_key(|&(_, co, _)| co);

    let block_count: u16 = blocks.len().try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("block count {} exceeds u16::MAX", blocks.len()),
        )
    })?;

    let data_start = PACK_HEADER_SIZE + blocks.len() * PACK_INDEX_ENTRY_SIZE;

    // First pass: collect metadata + compute total size.
    let mut entries = Vec::with_capacity(blocks.len());
    let mut running_offset = data_start as u32;
    let mut total_compressed = 0usize;
    for (hash, chunk_offset, compressed) in &blocks {
        let comp_length = compressed.len() as u32;
        entries.push(PackIndexEntry {
            hash: *hash,
            chunk_offset: *chunk_offset,
            offset: running_offset,
            comp_length,
        });
        running_offset += comp_length;
        total_compressed += compressed.len();
    }

    let total_size = data_start + total_compressed;
    let mut buf = Vec::with_capacity(total_size);

    // -- Header (16 bytes) --
    buf.extend_from_slice(PACK_MAGIC);
    buf.extend_from_slice(&PACK_VERSION.to_le_bytes());
    buf.extend_from_slice(&block_count.to_le_bytes());
    buf.extend_from_slice(&chunk_size.to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]); // reserved

    // -- Block Index (28 bytes per entry) --
    for entry in &entries {
        buf.extend_from_slice(entry.hash.as_bytes());
        buf.extend_from_slice(&entry.chunk_offset.to_le_bytes());
        buf.extend_from_slice(&entry.offset.to_le_bytes());
        buf.extend_from_slice(&entry.comp_length.to_le_bytes());
    }

    // -- Block Data (consumes blocks — each Vec<u8> freed after copy) --
    for (_, _, compressed) in blocks {
        buf.extend_from_slice(&compressed);
    }

    debug_assert_eq!(buf.len(), total_size);
    Ok((buf, entries))
}

/// Parse a v2 pack's header and block index from raw bytes.
///
/// Validates magic and version (must be 2). Only reads metadata, no decompression.
pub fn parse_pack_index(data: &[u8]) -> io::Result<PackIndex> {
    if data.len() < PACK_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pack too small for header",
        ));
    }

    if &data[..4] != PACK_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid pack magic",
        ));
    }

    let version = u16::from_le_bytes([data[4], data[5]]);
    if version != PACK_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected pack version 2, got {version}"),
        ));
    }

    let block_count = u16::from_le_bytes([data[6], data[7]]);
    let chunk_size = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);

    let index_size = block_count as usize * PACK_INDEX_ENTRY_SIZE;
    let required = PACK_HEADER_SIZE + index_size;
    if data.len() < required {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack too small for index: need {required} bytes, got {}",
                data.len()
            ),
        ));
    }

    let mut entries = Vec::with_capacity(block_count as usize);
    for i in 0..block_count as usize {
        let base = PACK_HEADER_SIZE + i * PACK_INDEX_ENTRY_SIZE;
        let mut hash_bytes = [0u8; 16];
        hash_bytes.copy_from_slice(&data[base..base + 16]);
        let chunk_offset = u32::from_le_bytes([
            data[base + 16],
            data[base + 17],
            data[base + 18],
            data[base + 19],
        ]);
        let offset = u32::from_le_bytes([
            data[base + 20],
            data[base + 21],
            data[base + 22],
            data[base + 23],
        ]);
        let comp_length = u32::from_le_bytes([
            data[base + 24],
            data[base + 25],
            data[base + 26],
            data[base + 27],
        ]);
        entries.push(PackIndexEntry {
            hash: Blake3Hash::from_bytes(hash_bytes),
            chunk_offset,
            offset,
            comp_length,
        });
    }

    Ok(PackIndex {
        block_count,
        chunk_size,
        entries,
    })
}

/// Lookup a block in a pack index by its chunk offset.
///
/// Entries are sorted by chunk_offset (guaranteed by `assemble_pack`), so this
/// uses binary search. Returns `(hash, pack_offset, comp_length)` if found.
#[cfg(test)]
pub fn lookup_block_in_index(
    entries: &[PackIndexEntry],
    chunk_offset: u32,
) -> Option<(Blake3Hash, u32, u32)> {
    entries
        .binary_search_by_key(&chunk_offset, |e| e.chunk_offset)
        .ok()
        .map(|idx| {
            let e = &entries[idx];
            (e.hash, e.offset, e.comp_length)
        })
}

/// Extract a single block's compressed data from pack bytes.
///
/// Returns a slice into `pack_data` at `[offset..offset+comp_length]`,
/// or `None` if the range is out of bounds.
#[allow(dead_code)] // used by pack_size_measure binary
pub fn extract_block(pack_data: &[u8], offset: u32, comp_length: u32) -> Option<&[u8]> {
    let start = offset as usize;
    let end = start.checked_add(comp_length as usize)?;
    if end > pack_data.len() {
        return None;
    }
    Some(&pack_data[start..end])
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::block_map::{blake3_128, lz4_compress, lz4_decompress};

    /// Helper: generate deterministic test data for block `i`.
    fn test_block_data(i: usize, size: usize) -> Vec<u8> {
        (0..size).map(|j| ((i * 31 + j * 7) % 256) as u8).collect()
    }

    // ====================================================================
    // v2 (current format) tests
    // ====================================================================

    #[test]
    fn test_pack_round_trip() {
        let chunk_size: u32 = 131072;
        let block_count = DEFAULT_BLOCKS_PER_PACK;

        let mut blocks = Vec::with_capacity(block_count);
        let mut originals = Vec::with_capacity(block_count);
        for i in 0..block_count {
            let data = test_block_data(i, chunk_size as usize);
            let hash = blake3_128(&data);
            let compressed = lz4_compress(&data);
            originals.push((hash, data));
            blocks.push((hash, i as u32, compressed));
        }

        let (pack_bytes, entries) = assemble_pack(blocks, chunk_size).unwrap();

        let index = parse_pack_index(&pack_bytes).unwrap();
        assert_eq!(index.block_count as usize, block_count);
        assert_eq!(index.chunk_size, chunk_size);
        assert_eq!(index.entries.len(), block_count);

        // Entries sorted by chunk_offset (0, 1, 2, ...).
        for w in index.entries.windows(2) {
            assert!(w[0].chunk_offset < w[1].chunk_offset);
        }

        for entry in &index.entries {
            let i = entry.chunk_offset as usize;
            let compressed = extract_block(&pack_bytes, entry.offset, entry.comp_length).unwrap();
            let decompressed = lz4_decompress(compressed).unwrap();
            let hash = blake3_128(&decompressed);
            assert_eq!(hash, originals[i].0, "hash mismatch at block {i}");
            assert_eq!(decompressed, originals[i].1, "data mismatch at block {i}");
            assert_eq!(entry.offset, entries[entry.chunk_offset as usize].offset);
        }
    }

    #[test]
    fn test_pack_header_magic() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let blocks = vec![(hash, 0u32, compressed.clone()), (hash, 1u32, compressed)];

        let (pack_bytes, _) = assemble_pack(blocks, 4096).unwrap();

        assert_eq!(&pack_bytes[..4], b"GLPK");
        // version = 2
        assert_eq!(u16::from_le_bytes([pack_bytes[4], pack_bytes[5]]), 2);
        // block_count = 2
        assert_eq!(u16::from_le_bytes([pack_bytes[6], pack_bytes[7]]), 2);
    }

    #[test]
    fn test_pack_lookup_by_chunk_offset() {
        let chunk_size: u32 = 4096;
        let mut blocks = Vec::new();

        for co in [10u32, 20, 30, 40, 50] {
            let data = vec![co as u8; chunk_size as usize];
            let hash = blake3_128(&data);
            let compressed = lz4_compress(&data);
            blocks.push((hash, co, compressed));
        }

        let (pack_bytes, _) = assemble_pack(blocks, chunk_size).unwrap();
        let index = parse_pack_index(&pack_bytes).unwrap();

        let (hash, offset, comp_length) = lookup_block_in_index(&index.entries, 30).unwrap();
        let compressed = extract_block(&pack_bytes, offset, comp_length).unwrap();
        let decompressed = lz4_decompress(compressed).unwrap();
        assert_eq!(blake3_128(&decompressed), hash);
        assert_eq!(decompressed, vec![30u8; chunk_size as usize]);

        assert!(lookup_block_in_index(&index.entries, 25).is_none());
        assert!(lookup_block_in_index(&index.entries, 0).is_none());
        assert!(lookup_block_in_index(&index.entries, 999).is_none());
    }

    #[test]
    fn test_pack_empty() {
        let (pack_bytes, entries) = assemble_pack(vec![], 131072).unwrap();
        assert!(entries.is_empty());
        assert_eq!(pack_bytes.len(), PACK_HEADER_SIZE);

        let index = parse_pack_index(&pack_bytes).unwrap();
        assert_eq!(index.block_count, 0);
        assert!(index.entries.is_empty());
        assert_eq!(index.chunk_size, 131072);
    }

    #[test]
    fn test_parse_invalid_magic() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (mut pack_bytes, _) = assemble_pack(vec![(hash, 0, compressed)], 4096).unwrap();

        pack_bytes[0..4].copy_from_slice(b"NOPE");
        let err = parse_pack_index(&pack_bytes).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("magic"), "error: {err}");
    }

    #[test]
    fn test_parse_unsupported_version() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (mut pack_bytes, _) = assemble_pack(vec![(hash, 0, compressed)], 4096).unwrap();

        pack_bytes[4..6].copy_from_slice(&99u16.to_le_bytes());
        let err = parse_pack_index(&pack_bytes).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("version"), "error: {err}");
    }

    #[test]
    fn test_parse_truncated_header() {
        let err = parse_pack_index(&[0u8; 8]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("too small for header"),
            "error: {err}"
        );
    }

    #[test]
    fn test_parse_truncated_index() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (pack_bytes, _) = assemble_pack(vec![(hash, 0, compressed)], 4096).unwrap();

        let truncated = &pack_bytes[..PACK_HEADER_SIZE + 10];
        let err = parse_pack_index(truncated).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("too small for index"),
            "error: {err}"
        );
    }

    #[test]
    fn test_extract_block_out_of_bounds() {
        let data = vec![42u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (pack_bytes, entries) = assemble_pack(vec![(hash, 0, compressed)], 4096).unwrap();

        assert!(extract_block(&pack_bytes, entries[0].offset, entries[0].comp_length).is_some());
        assert!(extract_block(&pack_bytes, pack_bytes.len() as u32, 1).is_none());
        assert!(extract_block(&pack_bytes, entries[0].offset, pack_bytes.len() as u32).is_none());
        assert!(extract_block(&pack_bytes, u32::MAX, 1).is_none());
    }

    #[test]
    fn test_pack_sort_by_chunk_offset() {
        let chunk_size: u32 = 131072;

        // Build blocks with chunk_offsets out of order to verify sorting.
        let mut blocks = Vec::new();
        let mut originals = Vec::new();
        for i in [5u32, 2, 8, 0, 3] {
            let data = test_block_data(i as usize, chunk_size as usize);
            let hash = blake3_128(&data);
            let compressed = lz4_compress(&data);
            originals.push((i, hash, data));
            blocks.push((hash, i, compressed));
        }

        let (pack_bytes, entries) = assemble_pack(blocks, chunk_size).unwrap();

        // Version in header must be 2.
        assert_eq!(&pack_bytes[..4], b"GLPK");
        assert_eq!(u16::from_le_bytes([pack_bytes[4], pack_bytes[5]]), 2);

        // Entries sorted by chunk_offset.
        for w in entries.windows(2) {
            assert!(
                w[0].chunk_offset < w[1].chunk_offset,
                "entries not sorted: {} >= {}",
                w[0].chunk_offset,
                w[1].chunk_offset
            );
        }

        let index = parse_pack_index(&pack_bytes).unwrap();
        assert_eq!(index.block_count, 5);

        for entry in &index.entries {
            let compressed = extract_block(&pack_bytes, entry.offset, entry.comp_length).unwrap();
            let decompressed = lz4_decompress(compressed).unwrap();
            let hash = blake3_128(&decompressed);
            assert_eq!(hash, entry.hash);

            let orig = originals
                .iter()
                .find(|(co, _, _)| *co == entry.chunk_offset)
                .unwrap();
            assert_eq!(decompressed, orig.2);
        }
    }

    #[test]
    fn test_pack_id_distribution() {
        let mut top_bytes = std::collections::HashSet::new();
        for _ in 0..100 {
            let id = new_pack_id();
            top_bytes.insert((id >> 56) as u8);
        }
        assert!(
            top_bytes.len() > 20,
            "poor distribution: only {} distinct top bytes out of 100",
            top_bytes.len()
        );
    }

    #[test]
    fn test_decompress_invalid_lz4_data() {
        let garbage = vec![0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00];
        let result = lz4_decompress(&garbage);
        assert!(result.is_err(), "invalid LZ4 data should return Err");
    }
}
