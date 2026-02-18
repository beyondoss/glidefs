//! Content-addressed pack format for S3 block storage.
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
//!   version:      u16 LE   = 1
//!   block_count:  u16 LE
//!   chunk_size:   u32 LE   (uncompressed block size, e.g. 131072 = 128KB)
//!   _reserved:    [u8; 4]  = [0; 4]
//!
//! Block Index (block_count * 24 bytes, immediately after header):
//!   hash:         [u8; 16]  (BLAKE3-128 of uncompressed data)
//!   offset:       u32 LE    (byte offset from start of pack file)
//!   comp_length:  u32 LE    (compressed size in bytes)
//!
//! Block Data (immediately after block index):
//!   [LZ4-compressed blocks, concatenated]
//!   Offsets in the index point into this region.
//! ```

use std::io;

use super::block_map::Blake3Hash;

pub const PACK_MAGIC: &[u8; 4] = b"GLPK";
pub const PACK_VERSION: u16 = 1;
pub const BLOCKS_PER_PACK: usize = 25;
pub const PACK_HEADER_SIZE: usize = 16;
pub const PACK_INDEX_ENTRY_SIZE: usize = 24;

/// Where a block lives inside a pack stored in S3.
#[derive(Debug, Clone, Copy)]
pub struct PackLocation {
    pub pack_id: uuid::Uuid,
    pub offset: u32,
    pub comp_length: u32,
}

/// A parsed pack header and block index (metadata only, no decompressed data).
#[derive(Debug)]
#[allow(dead_code)]
pub struct PackIndex {
    pub block_count: u16,
    pub chunk_size: u32,
    pub entries: Vec<PackIndexEntry>,
}

/// One entry in the pack's block index.
#[derive(Debug, Clone)]
pub struct PackIndexEntry {
    pub hash: Blake3Hash,
    pub offset: u32,
    pub comp_length: u32,
}

/// Assemble a pack from pre-compressed blocks.
///
/// `blocks` contains `(hash, compressed_data)` pairs where the compressed data
/// is already LZ4-encoded. `chunk_size` is the uncompressed block size (e.g. 131072).
///
/// Returns the complete pack bytes and the index entries with their final offsets.
///
/// Panics if `blocks.len()` exceeds `u16::MAX`.
pub fn assemble_pack(
    blocks: &[(Blake3Hash, Vec<u8>)],
    chunk_size: u32,
) -> (Vec<u8>, Vec<PackIndexEntry>) {
    let block_count: u16 = blocks
        .len()
        .try_into()
        .expect("block count must fit in u16");

    let data_start = PACK_HEADER_SIZE + blocks.len() * PACK_INDEX_ENTRY_SIZE;

    // Pre-calculate total size for a single allocation.
    let total_compressed: usize = blocks.iter().map(|(_, data)| data.len()).sum();
    let total_size = data_start + total_compressed;
    let mut buf = Vec::with_capacity(total_size);

    // -- Header (16 bytes) --
    buf.extend_from_slice(PACK_MAGIC);
    buf.extend_from_slice(&PACK_VERSION.to_le_bytes());
    buf.extend_from_slice(&block_count.to_le_bytes());
    buf.extend_from_slice(&chunk_size.to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]); // reserved

    // -- Block Index --
    let mut entries = Vec::with_capacity(blocks.len());
    let mut running_offset = data_start as u32;

    for (hash, compressed) in blocks {
        let comp_length = compressed.len() as u32;
        let entry = PackIndexEntry {
            hash: *hash,
            offset: running_offset,
            comp_length,
        };
        // Write index entry: hash (16) + offset (4) + comp_length (4)
        buf.extend_from_slice(hash.as_bytes());
        buf.extend_from_slice(&running_offset.to_le_bytes());
        buf.extend_from_slice(&comp_length.to_le_bytes());

        entries.push(entry);
        running_offset += comp_length;
    }

    // -- Block Data --
    for (_, compressed) in blocks {
        buf.extend_from_slice(compressed);
    }

    debug_assert_eq!(buf.len(), total_size);
    (buf, entries)
}

/// Parse a pack's header and block index from raw bytes.
///
/// Validates the magic bytes and version. Does NOT decompress any block data;
/// only reads the metadata needed to locate blocks within the pack.
pub fn parse_pack_index(data: &[u8]) -> io::Result<PackIndex> {
    if data.len() < PACK_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pack too small for header",
        ));
    }

    // Validate magic.
    if &data[..4] != PACK_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid pack magic",
        ));
    }

    // Validate version.
    let version = u16::from_le_bytes([data[4], data[5]]);
    if version != PACK_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported pack version: {version}"),
        ));
    }

    let block_count = u16::from_le_bytes([data[6], data[7]]);
    let chunk_size = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
    // bytes 12..16 are reserved

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
        let offset = u32::from_le_bytes([
            data[base + 16],
            data[base + 17],
            data[base + 18],
            data[base + 19],
        ]);
        let comp_length = u32::from_le_bytes([
            data[base + 20],
            data[base + 21],
            data[base + 22],
            data[base + 23],
        ]);
        entries.push(PackIndexEntry {
            hash: Blake3Hash::from_bytes(hash_bytes),
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

/// Extract a single block's compressed data from pack bytes.
///
/// Returns a slice into `pack_data` at `[offset..offset+comp_length]`,
/// or `None` if the range is out of bounds.
pub fn extract_block(pack_data: &[u8], offset: u32, comp_length: u32) -> Option<&[u8]> {
    let start = offset as usize;
    let end = start.checked_add(comp_length as usize)?;
    if end > pack_data.len() {
        return None;
    }
    Some(&pack_data[start..end])
}

/// Generate the S3 object key for a pack.
///
/// Format: `packs/{first-2-hex-of-uuid}/{uuid}`
///
/// The 2-character hex prefix provides 256-way prefix sharding for S3
/// performance (avoids hot-partition issues on high-throughput workloads).
pub fn pack_s3_key(pack_id: uuid::Uuid) -> String {
    let uuid_str = pack_id.to_string();
    let prefix = &uuid_str[..2];
    format!("packs/{prefix}/{uuid_str}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nbd::block_map::{blake3_128, lz4_compress, lz4_decompress};

    /// Helper: generate deterministic test data for block `i`.
    fn test_block_data(i: usize, size: usize) -> Vec<u8> {
        (0..size)
            .map(|j| ((i * 31 + j * 7) % 256) as u8)
            .collect()
    }

    #[test]
    fn test_pack_round_trip() {
        let chunk_size: u32 = 131072; // 128KB
        let block_count = BLOCKS_PER_PACK; // 25

        // Build blocks: hash the raw data, then compress it.
        let mut blocks = Vec::with_capacity(block_count);
        let mut originals = Vec::with_capacity(block_count);
        for i in 0..block_count {
            let data = test_block_data(i, chunk_size as usize);
            let hash = blake3_128(&data);
            let compressed = lz4_compress(&data);
            originals.push((hash, data));
            blocks.push((hash, compressed));
        }

        let (pack_bytes, entries) = assemble_pack(&blocks, chunk_size);

        // Parse the index back.
        let index = parse_pack_index(&pack_bytes).unwrap();
        assert_eq!(index.block_count as usize, block_count);
        assert_eq!(index.chunk_size, chunk_size);
        assert_eq!(index.entries.len(), block_count);

        // For each entry: extract, decompress, verify hash matches original.
        for (i, entry) in index.entries.iter().enumerate() {
            let compressed =
                extract_block(&pack_bytes, entry.offset, entry.comp_length).unwrap();
            let decompressed = lz4_decompress(compressed).unwrap();
            let hash = blake3_128(&decompressed);
            assert_eq!(hash, originals[i].0, "hash mismatch at block {i}");
            assert_eq!(decompressed, originals[i].1, "data mismatch at block {i}");
            // Verify the returned entries match the parsed index.
            assert_eq!(entry.offset, entries[i].offset);
            assert_eq!(entry.comp_length, entries[i].comp_length);
        }
    }

    #[test]
    fn test_pack_single_block() {
        let chunk_size: u32 = 131072;
        let data = vec![42u8; chunk_size as usize];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let blocks = vec![(hash, compressed)];

        let (pack_bytes, entries) = assemble_pack(&blocks, chunk_size);
        assert_eq!(entries.len(), 1);

        let index = parse_pack_index(&pack_bytes).unwrap();
        assert_eq!(index.block_count, 1);
        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.entries[0].hash, hash);

        let compressed =
            extract_block(&pack_bytes, index.entries[0].offset, index.entries[0].comp_length)
                .unwrap();
        let decompressed = lz4_decompress(compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_pack_header_magic() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let blocks = vec![(hash, compressed.clone()), (hash, compressed)];

        let (pack_bytes, _) = assemble_pack(&blocks, 4096);

        // First 4 bytes: magic.
        assert_eq!(&pack_bytes[..4], b"GLPK");
        // Bytes 4-5: version (u16 LE = 1).
        assert_eq!(u16::from_le_bytes([pack_bytes[4], pack_bytes[5]]), 1);
        // Bytes 6-7: block_count (u16 LE = 2).
        assert_eq!(u16::from_le_bytes([pack_bytes[6], pack_bytes[7]]), 2);
    }

    #[test]
    fn test_pack_block_lookup_by_hash() {
        let chunk_size: u32 = 4096;
        let mut blocks = Vec::new();
        let mut expected_data = Vec::new();

        for i in 0..5u8 {
            let data: Vec<u8> = vec![i; chunk_size as usize];
            let hash = blake3_128(&data);
            let compressed = lz4_compress(&data);
            expected_data.push((hash, data));
            blocks.push((hash, compressed));
        }

        let (pack_bytes, _) = assemble_pack(&blocks, chunk_size);
        let index = parse_pack_index(&pack_bytes).unwrap();

        // Look up block 3 by its hash.
        let target_hash = expected_data[3].0;
        let entry = index
            .entries
            .iter()
            .find(|e| e.hash == target_hash)
            .expect("should find block by hash");

        let compressed =
            extract_block(&pack_bytes, entry.offset, entry.comp_length).unwrap();
        let decompressed = lz4_decompress(compressed).unwrap();
        assert_eq!(decompressed, expected_data[3].1);
    }

    #[test]
    fn test_pack_incompressible_data() {
        let chunk_size: u32 = 4096;
        // Pseudo-random data that LZ4 cannot compress.
        let data: Vec<u8> = (0..chunk_size)
            .map(|i| {
                ((i.wrapping_mul(2654435761).wrapping_shr(16)) & 0xFF) as u8
            })
            .collect();
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let blocks = vec![(hash, compressed)];

        let (pack_bytes, _) = assemble_pack(&blocks, chunk_size);
        let index = parse_pack_index(&pack_bytes).unwrap();
        assert_eq!(index.entries.len(), 1);

        let compressed =
            extract_block(&pack_bytes, index.entries[0].offset, index.entries[0].comp_length)
                .unwrap();
        let decompressed = lz4_decompress(compressed).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(blake3_128(&decompressed), hash);
    }

    #[test]
    fn test_pack_s3_key_format() {
        let id = uuid::Uuid::parse_str("a7550e84-00e2-9b41-d4a7-16446655440a").unwrap();
        let key = pack_s3_key(id);

        assert!(key.starts_with("packs/"), "key must start with packs/");
        // Prefix is first 2 chars of the UUID string representation.
        assert!(key.starts_with("packs/a7/"), "key must have 2-char hex prefix");
        assert!(
            key.contains(&id.to_string()),
            "key must contain the full UUID"
        );
        assert_eq!(key, "packs/a7/a7550e84-00e2-9b41-d4a7-16446655440a");
    }

    #[test]
    fn test_parse_invalid_magic() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (mut pack_bytes, _) = assemble_pack(&[(hash, compressed)], 4096);

        // Corrupt magic bytes
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
        let (mut pack_bytes, _) = assemble_pack(&[(hash, compressed)], 4096);

        // Set version to 99
        pack_bytes[4..6].copy_from_slice(&99u16.to_le_bytes());
        let err = parse_pack_index(&pack_bytes).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("version"), "error: {err}");
    }

    #[test]
    fn test_parse_truncated_header() {
        // Less than PACK_HEADER_SIZE bytes
        let err = parse_pack_index(&[0u8; 8]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("too small for header"), "error: {err}");
    }

    #[test]
    fn test_parse_truncated_index() {
        let data = vec![0u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (pack_bytes, _) = assemble_pack(&[(hash, compressed)], 4096);

        // Truncate after header but before the full index entry
        let truncated = &pack_bytes[..PACK_HEADER_SIZE + 10];
        let err = parse_pack_index(truncated).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("too small for index"), "error: {err}");
    }

    #[test]
    fn test_extract_block_out_of_bounds() {
        let data = vec![42u8; 4096];
        let hash = blake3_128(&data);
        let compressed = lz4_compress(&data);
        let (pack_bytes, entries) = assemble_pack(&[(hash, compressed)], 4096);

        // Valid extraction works
        assert!(extract_block(&pack_bytes, entries[0].offset, entries[0].comp_length).is_some());

        // Offset past end of pack
        assert!(extract_block(&pack_bytes, pack_bytes.len() as u32, 1).is_none());

        // comp_length extends past end
        assert!(extract_block(&pack_bytes, entries[0].offset, pack_bytes.len() as u32).is_none());

        // u32 overflow in offset + comp_length
        assert!(extract_block(&pack_bytes, u32::MAX, 1).is_none());
    }

    #[test]
    fn test_decompress_invalid_lz4_data() {
        // Corrupt LZ4 data should return Err, not panic
        let garbage = vec![0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00];
        let result = lz4_decompress(&garbage);
        assert!(result.is_err(), "invalid LZ4 data should return Err");
    }

    #[test]
    fn test_pack_empty_rejected() {
        // Assembling with 0 blocks produces a header-only pack. This is allowed
        // for symmetry (a valid but useless pack), and parse_pack_index handles
        // it correctly.
        let (pack_bytes, entries) = assemble_pack(&[], 131072);
        assert!(entries.is_empty());
        assert_eq!(pack_bytes.len(), PACK_HEADER_SIZE);

        let index = parse_pack_index(&pack_bytes).unwrap();
        assert_eq!(index.block_count, 0);
        assert!(index.entries.is_empty());
        assert_eq!(index.chunk_size, 131072);
    }
}
