//! Content-addressed pack format (GLPK v3) for S3 block storage.
//!
//! Packs batch multiple compressed blocks into a single S3 object to reduce
//! per-object overhead and improve throughput. The block index is a footer,
//! enabling streaming writes (blocks upload as they're produced via
//! `WriteMultipart`, with the index appended at the end).
//!
//! ## Wire Format
//!
//! ```text
//! Pack Header (16 bytes, fixed):
//!   magic:        [u8; 4]  = b"GLPK"
//!   version:      u16 LE   = 3
//!   block_count:  u16 LE
//!   chunk_size:   u32 LE   (uncompressed block size, e.g. 131072 = 128KB)
//!   _reserved:    [u8; 4]  = [0; 4]
//!
//! Block Data (immediately after header):
//!   [LZ4-compressed blocks, concatenated]
//!   Offsets in the index point into this region (absolute from pack start).
//!
//! Block Index (block_count * 28 bytes, footer):
//!   hash:           [u8; 16]  (BLAKE3-128 of uncompressed data)
//!   chunk_offset:   u32 LE    (block offset within chunk, 0–1023)
//!   offset:         u32 LE    (byte offset from start of pack file)
//!   comp_length:    u32 LE    (compressed size in bytes)
//!
//! Trailer (8 bytes):
//!   block_count:  u16 LE      (enables suffix-only reads without header)
//!   _reserved:    [u8; 2]     = [0; 2]
//!   magic:        [u8; 4]     = b"GLIX"
//! ```

use std::io;

use bytes::Bytes;
use object_store::WriteMultipart;

use super::block_map::Blake3Hash;

pub const PACK_MAGIC: &[u8; 4] = b"GLPK";
pub const PACK_VERSION: u16 = 3;
/// Default flush threshold: 1 chunk worth of dirty blocks triggers a flush.
/// 128 MiB chunk / 128 KiB block = 1024 blocks.
pub const DEFAULT_FLUSH_THRESHOLD: usize = 1024;
pub const PACK_HEADER_SIZE: usize = 16;
/// Index entry size (28 bytes with chunk_offset).
pub const PACK_INDEX_ENTRY_SIZE: usize = 28;
/// Trailer magic identifying footer-indexed packs.
pub const TRAILER_MAGIC: &[u8; 4] = b"GLIX";
/// Trailer size: block_count (2) + reserved (2) + magic (4).
pub const TRAILER_SIZE: usize = 8;

/// 8-byte pack identifier, derived from content hash.
///
/// Content-addressed: identical blocks at identical offsets produce the
/// same PackId, enabling cross-export dedup on shared S3 prefixes.
/// Collision-safe per chunk (birthday bound ~4.3 billion).
/// Hex representation distributes uniformly for S3 prefix sharding.
pub type PackId = u64;

/// Generate a random pack ID (used only by tests that don't care about content addressing).
pub fn new_pack_id() -> PackId {
    rand::random::<u64>()
}

/// Compute a content-addressed pack ID from sorted blocks.
///
/// BLAKE3 hash over `(chunk_offset, block_hash, comp_length, compressed_data)`
/// for each block, truncated to u64. Deterministic: same blocks in the same
/// order always produce the same ID.
///
/// Blocks must be sorted by chunk_offset before calling (the flush and
/// compaction paths already ensure this).
pub fn content_pack_id(blocks: &[(super::block_map::Blake3Hash, u32, bytes::Bytes)]) -> PackId {
    let mut hasher = blake3::Hasher::new();
    for (hash, chunk_offset, compressed) in blocks {
        hasher.update(&chunk_offset.to_le_bytes());
        hasher.update(hash.as_bytes());
        hasher.update(&(compressed.len() as u32).to_le_bytes());
        hasher.update(compressed);
    }
    let hash = hasher.finalize();
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
}

// ============================================================================
// GLPK v3 — footer-indexed packs with streaming writes
// ============================================================================

/// One entry in a pack's block index. Includes `chunk_offset` so packs are
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

/// Parsed pack header + block index.
#[derive(Debug)]
#[allow(dead_code)]
pub struct PackIndex {
    pub block_count: u16,
    pub chunk_size: u32,
    pub entries: Vec<PackIndexEntry>,
}

/// Assemble a v3 pack from pre-compressed blocks (in-memory).
///
/// `blocks` contains `(hash, chunk_offset, compressed_data)` triples.
/// Entries are sorted by `chunk_offset` before writing for read locality.
/// Returns the complete pack bytes and the sorted index entries.
///
/// For production uploads, prefer [`stream_pack_to_writer`] which streams
/// blocks to S3 via `WriteMultipart` without buffering the entire pack.
#[allow(dead_code)]
pub fn assemble_pack(
    mut blocks: Vec<(Blake3Hash, u32, Vec<u8>)>,
    chunk_size: u32,
) -> io::Result<(Vec<u8>, Vec<PackIndexEntry>)> {
    blocks.sort_by_key(|&(_, co, _)| co);

    let block_count: u16 = blocks.len().try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("block count {} exceeds u16::MAX", blocks.len()),
        )
    })?;

    // v3: data starts right after header (index is a footer).
    let mut entries = Vec::with_capacity(blocks.len());
    let mut running_offset = PACK_HEADER_SIZE as u32;
    let mut total_compressed = 0usize;
    for (hash, chunk_offset, compressed) in &blocks {
        let comp_length = compressed.len() as u32;
        entries.push(PackIndexEntry {
            hash: *hash,
            chunk_offset: *chunk_offset,
            offset: running_offset,
            comp_length,
        });
        running_offset = running_offset.checked_add(comp_length).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pack data exceeds u32::MAX bytes at block {}", entries.len()),
            )
        })?;
        total_compressed += compressed.len();
    }

    let index_size = blocks.len() * PACK_INDEX_ENTRY_SIZE;
    let total_size = PACK_HEADER_SIZE + total_compressed + index_size + TRAILER_SIZE;
    let mut buf = Vec::with_capacity(total_size);

    // -- Header (16 bytes) --
    buf.extend_from_slice(PACK_MAGIC);
    buf.extend_from_slice(&PACK_VERSION.to_le_bytes());
    buf.extend_from_slice(&block_count.to_le_bytes());
    buf.extend_from_slice(&chunk_size.to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]); // reserved

    // -- Block Data (consumes blocks — each Vec<u8> freed after copy) --
    for (_, _, compressed) in blocks {
        buf.extend_from_slice(&compressed);
    }

    // -- Block Index footer (28 bytes per entry) --
    for entry in &entries {
        buf.extend_from_slice(entry.hash.as_bytes());
        buf.extend_from_slice(&entry.chunk_offset.to_le_bytes());
        buf.extend_from_slice(&entry.offset.to_le_bytes());
        buf.extend_from_slice(&entry.comp_length.to_le_bytes());
    }

    // -- Trailer (8 bytes) --
    buf.extend_from_slice(&block_count.to_le_bytes());
    buf.extend_from_slice(&[0u8; 2]); // reserved
    buf.extend_from_slice(TRAILER_MAGIC);

    debug_assert_eq!(buf.len(), total_size);
    Ok((buf, entries))
}

/// Stream a v3 pack to a `WriteMultipart` writer.
///
/// Blocks are written to the multipart stream as they're consumed — each
/// compressed `Vec<u8>` is freed immediately after `writer.put()`. The index
/// and trailer are appended at the end.
///
/// Returns sorted index entries (for `PackIndexCache` insertion).
pub fn stream_pack_to_writer(
    mut blocks: Vec<(Blake3Hash, u32, Bytes)>,
    chunk_size: u32,
    writer: &mut WriteMultipart,
) -> io::Result<Vec<PackIndexEntry>> {
    blocks.sort_by_key(|&(_, co, _)| co);

    let block_count: u16 = blocks.len().try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("block count {} exceeds u16::MAX", blocks.len()),
        )
    })?;

    // -- Header (16 bytes) --
    let mut header = [0u8; PACK_HEADER_SIZE];
    header[0..4].copy_from_slice(PACK_MAGIC);
    header[4..6].copy_from_slice(&PACK_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&block_count.to_le_bytes());
    header[8..12].copy_from_slice(&chunk_size.to_le_bytes());
    writer.write(&header);

    // -- Block Data (zero-copy: each Vec<u8> converted to Bytes) --
    let mut entries = Vec::with_capacity(blocks.len());
    let mut running_offset = PACK_HEADER_SIZE as u32;
    for (hash, chunk_offset, compressed) in blocks {
        let comp_length = compressed.len() as u32;
        entries.push(PackIndexEntry {
            hash,
            chunk_offset,
            offset: running_offset,
            comp_length,
        });
        running_offset = running_offset.checked_add(comp_length).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pack data exceeds u32::MAX bytes at block {}", entries.len()),
            )
        })?;
        writer.put(compressed);
    }

    // -- Block Index footer (28 bytes per entry) --
    for entry in &entries {
        let mut entry_buf = [0u8; PACK_INDEX_ENTRY_SIZE];
        entry_buf[0..16].copy_from_slice(entry.hash.as_bytes());
        entry_buf[16..20].copy_from_slice(&entry.chunk_offset.to_le_bytes());
        entry_buf[20..24].copy_from_slice(&entry.offset.to_le_bytes());
        entry_buf[24..28].copy_from_slice(&entry.comp_length.to_le_bytes());
        writer.write(&entry_buf);
    }

    // -- Trailer (8 bytes) --
    let mut trailer = [0u8; TRAILER_SIZE];
    trailer[0..2].copy_from_slice(&block_count.to_le_bytes());
    // [2..4] reserved zeros
    trailer[4..8].copy_from_slice(TRAILER_MAGIC);
    writer.write(&trailer);

    Ok(entries)
}

/// Parse a v3 pack's block index from suffix data.
///
/// `data` is the last N bytes of the pack (suffix read). The trailer at the
/// end contains `block_count` and `GLIX` magic. The index entries immediately
/// precede the trailer.
///
/// For full-pack bytes (tests), this also works: the suffix is the entire pack,
/// and the index + trailer are at the end.
pub fn parse_pack_index(data: &[u8]) -> io::Result<PackIndex> {
    if data.len() < TRAILER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pack too small for trailer",
        ));
    }

    // Parse trailer (last 8 bytes).
    let trailer_start = data.len() - TRAILER_SIZE;
    if &data[trailer_start + 4..trailer_start + 8] != TRAILER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid trailer magic",
        ));
    }

    let block_count = u16::from_le_bytes([data[trailer_start], data[trailer_start + 1]]);

    let index_size = block_count as usize * PACK_INDEX_ENTRY_SIZE;
    let required = index_size + TRAILER_SIZE;
    if data.len() < required {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "suffix too small for index: need {required} bytes, got {}",
                data.len()
            ),
        ));
    }

    // Index entries are right before the trailer.
    let index_start = trailer_start - index_size;

    // Try to read chunk_size from header if the full pack is available.
    let chunk_size = if data.len() >= PACK_HEADER_SIZE
        && &data[..4] == PACK_MAGIC
    {
        u32::from_le_bytes(data[8..12].try_into().unwrap())
    } else {
        0 // suffix-only read; chunk_size not available (callers don't need it)
    };

    let mut entries = Vec::with_capacity(block_count as usize);
    for i in 0..block_count as usize {
        let base = index_start + i * PACK_INDEX_ENTRY_SIZE;
        let mut hash_bytes = [0u8; 16];
        hash_bytes.copy_from_slice(&data[base..base + 16]);
        let chunk_offset =
            u32::from_le_bytes(data[base + 16..base + 20].try_into().unwrap());
        let offset =
            u32::from_le_bytes(data[base + 20..base + 24].try_into().unwrap());
        let comp_length =
            u32::from_le_bytes(data[base + 24..base + 28].try_into().unwrap());
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

    #[test]
    fn test_pack_round_trip() {
        let chunk_size: u32 = 131072;
        let block_count = DEFAULT_FLUSH_THRESHOLD;

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
    fn test_pack_header_and_trailer() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let blocks = vec![(hash, 0u32, compressed.clone()), (hash, 1u32, compressed)];

        let (pack_bytes, _) = assemble_pack(blocks, 4096).unwrap();

        // Header
        assert_eq!(&pack_bytes[..4], b"GLPK");
        assert_eq!(u16::from_le_bytes([pack_bytes[4], pack_bytes[5]]), 3);
        assert_eq!(u16::from_le_bytes([pack_bytes[6], pack_bytes[7]]), 2);

        // Trailer (last 8 bytes)
        let t = pack_bytes.len() - TRAILER_SIZE;
        assert_eq!(u16::from_le_bytes([pack_bytes[t], pack_bytes[t + 1]]), 2);
        assert_eq!(&pack_bytes[t + 4..t + 8], b"GLIX");
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
        // v3: header (16) + no data + no index + trailer (8)
        assert_eq!(pack_bytes.len(), PACK_HEADER_SIZE + TRAILER_SIZE);

        let index = parse_pack_index(&pack_bytes).unwrap();
        assert_eq!(index.block_count, 0);
        assert!(index.entries.is_empty());
        assert_eq!(index.chunk_size, 131072);
    }

    #[test]
    fn test_parse_invalid_trailer_magic() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (mut pack_bytes, _) = assemble_pack(vec![(hash, 0, compressed)], 4096).unwrap();

        // Corrupt trailer magic
        let t = pack_bytes.len() - 4;
        pack_bytes[t..].copy_from_slice(b"NOPE");
        let err = parse_pack_index(&pack_bytes).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("trailer magic"), "error: {err}");
    }

    #[test]
    fn test_parse_truncated_trailer() {
        let err = parse_pack_index(&[0u8; 4]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("too small for trailer"),
            "error: {err}"
        );
    }

    #[test]
    fn test_parse_truncated_index() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (pack_bytes, _) = assemble_pack(vec![(hash, 0, compressed)], 4096).unwrap();

        // Give just the trailer (8 bytes) but claim 1 entry → need 28 + 8 = 36
        let suffix = &pack_bytes[pack_bytes.len() - TRAILER_SIZE..];
        let err = parse_pack_index(suffix).unwrap_err();
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

        // v3 header
        assert_eq!(&pack_bytes[..4], b"GLPK");
        assert_eq!(u16::from_le_bytes([pack_bytes[4], pack_bytes[5]]), 3);

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

    /// Suffix-only parse: pass just the index + trailer portion.
    #[test]
    fn test_parse_suffix_only() {
        let chunk_size: u32 = 4096;
        let mut blocks = Vec::new();
        for co in [0u32, 1, 2] {
            let data = vec![co as u8; chunk_size as usize];
            let hash = blake3_128(&data);
            blocks.push((hash, co, lz4_compress(&data)));
        }

        let (pack_bytes, _) = assemble_pack(blocks, chunk_size).unwrap();

        // Simulate a suffix read: just the index + trailer.
        let suffix_size = 3 * PACK_INDEX_ENTRY_SIZE + TRAILER_SIZE;
        let suffix = &pack_bytes[pack_bytes.len() - suffix_size..];

        let index = parse_pack_index(suffix).unwrap();
        assert_eq!(index.block_count, 3);
        assert_eq!(index.entries.len(), 3);
        // chunk_size not available from suffix-only
        assert_eq!(index.chunk_size, 0);
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

    #[test]
    fn test_content_pack_id_deterministic() {
        let block_data = test_block_data(0, 128 * 1024);
        let hash = blake3_128(&block_data);
        let compressed = bytes::Bytes::from(lz4_compress(&block_data));

        let blocks = vec![(hash, 0u32, compressed.clone())];

        let id1 = content_pack_id(&blocks);
        let id2 = content_pack_id(&blocks);
        assert_eq!(id1, id2, "same blocks must produce same ID");
    }

    #[test]
    fn test_content_pack_id_differs_on_content() {
        let data_a = test_block_data(0, 128 * 1024);
        let data_b = test_block_data(1, 128 * 1024);
        let blocks_a = vec![(blake3_128(&data_a), 0u32, bytes::Bytes::from(lz4_compress(&data_a)))];
        let blocks_b = vec![(blake3_128(&data_b), 0u32, bytes::Bytes::from(lz4_compress(&data_b)))];

        assert_ne!(
            content_pack_id(&blocks_a),
            content_pack_id(&blocks_b),
            "different content must produce different IDs"
        );
    }

    #[test]
    fn test_content_pack_id_differs_on_offset() {
        let data = test_block_data(0, 128 * 1024);
        let hash = blake3_128(&data);
        let compressed = bytes::Bytes::from(lz4_compress(&data));

        let blocks_at_0 = vec![(hash, 0u32, compressed.clone())];
        let blocks_at_1 = vec![(hash, 1u32, compressed)];

        assert_ne!(
            content_pack_id(&blocks_at_0),
            content_pack_id(&blocks_at_1),
            "same content at different offsets must produce different IDs"
        );
    }
}
