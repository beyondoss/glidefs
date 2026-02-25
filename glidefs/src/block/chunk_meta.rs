//! Chunk metadata (GLCM): self-contained binary index of block→pack mappings for one chunk.
//!
//! Each `.meta` file is a flat, sorted array of fixed-size entries that maps block offsets
//! within a chunk to their pack locations. The file is content-addressed: the chunk_hash
//! in the volume manifest is the BLAKE3-128 of the sorted `(offset, block_hash)` pairs.
//!
//! Binary format:
//! ```text
//! Header (32 bytes):
//!   magic: "GLCM"          (4 bytes)
//!   version: u16 LE         (2 bytes)
//!   chunk_idx: u32 LE       (4 bytes)
//!   block_count: u32 LE     (4 bytes)
//!   chunk_size: u64 LE      (8 bytes, bytes per chunk, e.g. 10GB)
//!   block_size: u32 LE      (4 bytes)
//!   reserved: [u8; 6]       (6 bytes)
//!
//! Entry array (44 bytes × block_count, sorted by offset):
//!   offset:       u32 LE     (block index within chunk, 0–81919)
//!   hash:         [u8; 16]   (BLAKE3-128 of uncompressed block)
//!   pack_id:      [u8; 16]   (UUID of pack)
//!   pack_offset:  u32 LE     (byte offset within pack)
//!   comp_length:  u32 LE     (compressed size)
//!
//! Trailing CRC32: 4 bytes
//! ```

use std::collections::HashSet;

use uuid::Uuid;

use super::block_map::Blake3Hash;
use super::pack::PackLocation;

/// GLCM magic bytes.
const GLCM_MAGIC: &[u8; 4] = b"GLCM";
/// Current GLCM version.
const GLCM_VERSION: u16 = 1;
/// Header size in bytes.
const HEADER_SIZE: usize = 32;
/// Entry size in bytes (offset:4 + hash:16 + pack_id:16 + pack_offset:4 + comp_length:4).
pub const ENTRY_SIZE: usize = 44;

/// A single block entry within a chunk .meta file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMetaEntry {
    /// Block index within the chunk (0 to blocks_per_chunk - 1).
    pub offset: u32,
    /// BLAKE3-128 hash of the uncompressed block.
    pub hash: Blake3Hash,
    /// UUID of the pack containing this block.
    pub pack_id: Uuid,
    /// Byte offset within the pack.
    pub pack_offset: u32,
    /// Compressed size in bytes.
    pub comp_length: u32,
}

/// Chunk metadata: block index → pack location for all written blocks in one chunk.
#[derive(Debug, Clone)]
pub struct ChunkMeta {
    /// Chunk index this metadata belongs to.
    pub chunk_idx: u32,
    /// Chunk size in bytes (u64 because 10GB > u32::MAX).
    pub chunk_size: u64,
    /// Block size in bytes.
    pub block_size: u32,
    /// Entries sorted by offset.
    pub entries: Vec<ChunkMetaEntry>,
}

impl ChunkMeta {
    /// Create an empty chunk meta for a given chunk.
    pub fn new(chunk_idx: u32, chunk_size: u64, block_size: u32) -> Self {
        Self {
            chunk_idx,
            chunk_size,
            block_size,
            entries: Vec::new(),
        }
    }

    /// Number of block entries.
    pub fn block_count(&self) -> u32 {
        self.entries.len() as u32
    }

    /// Compute the content hash: BLAKE3-128 of sorted (offset, block_hash) pairs.
    ///
    /// This is the chunk_hash stored in the VolumeManifest. Two chunk states
    /// with identical block content produce identical hashes.
    pub fn content_hash(&self) -> Blake3Hash {
        use super::block_map::blake3_128;

        let mut data = Vec::with_capacity(self.entries.len() * 20); // 4 + 16 per entry
        for entry in &self.entries {
            data.extend_from_slice(&entry.offset.to_le_bytes());
            data.extend_from_slice(&entry.hash.0);
        }
        blake3_128(&data)
    }

    /// Lookup a block by its offset within the chunk. Returns pack location if found.
    pub fn lookup(&self, block_offset: u32) -> Option<(Blake3Hash, PackLocation)> {
        self.entries
            .binary_search_by_key(&block_offset, |e| e.offset)
            .ok()
            .map(|idx| {
                let e = &self.entries[idx];
                (
                    e.hash,
                    PackLocation {
                        pack_id: e.pack_id,
                        offset: e.pack_offset,
                        comp_length: e.comp_length,
                    },
                )
            })
    }

    /// Collect all unique block hashes (for within-chunk dedup).
    pub fn block_hashes(&self) -> HashSet<Blake3Hash> {
        self.entries.iter().map(|e| e.hash).collect()
    }

    /// Collect all unique pack IDs (for GC).
    pub fn pack_ids(&self) -> HashSet<Uuid> {
        self.entries.iter().map(|e| e.pack_id).collect()
    }

    /// Merge old entries with new entries. New entries overwrite old entries at the same offset.
    ///
    /// Returns a new ChunkMeta with all entries merged and sorted by offset.
    pub fn merge(&self, new_entries: &[ChunkMetaEntry]) -> ChunkMeta {
        let updated_offsets: HashSet<u32> = new_entries.iter().map(|e| e.offset).collect();

        let mut merged: Vec<ChunkMetaEntry> = self
            .entries
            .iter()
            .filter(|e| !updated_offsets.contains(&e.offset))
            .cloned()
            .chain(new_entries.iter().cloned())
            .collect();

        merged.sort_by_key(|e| e.offset);

        ChunkMeta {
            chunk_idx: self.chunk_idx,
            chunk_size: self.chunk_size,
            block_size: self.block_size,
            entries: merged,
        }
    }

    /// Serialize to GLCM binary format.
    pub fn serialize(&self) -> Vec<u8> {
        let size = HEADER_SIZE + self.entries.len() * ENTRY_SIZE + 4;
        let mut buf = Vec::with_capacity(size);

        // Header (32 bytes)
        buf.extend_from_slice(GLCM_MAGIC);                          // 4
        buf.extend_from_slice(&GLCM_VERSION.to_le_bytes());         // 2
        buf.extend_from_slice(&self.chunk_idx.to_le_bytes());        // 4
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes()); // 4
        buf.extend_from_slice(&self.chunk_size.to_le_bytes());       // 8
        buf.extend_from_slice(&self.block_size.to_le_bytes());       // 4
        buf.extend_from_slice(&[0u8; 6]);                            // 6 reserved

        // Entry array
        for entry in &self.entries {
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.hash.0);
            buf.extend_from_slice(entry.pack_id.as_bytes());
            buf.extend_from_slice(&entry.pack_offset.to_le_bytes());
            buf.extend_from_slice(&entry.comp_length.to_le_bytes());
        }

        // CRC32 trailer
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize from GLCM binary format.
    pub fn deserialize(data: &[u8]) -> Result<Self, ChunkMetaError> {
        if data.len() < HEADER_SIZE + 4 {
            return Err(ChunkMetaError::TooShort);
        }

        // Verify CRC32 (covers everything except the trailing 4 bytes)
        let crc_offset = data.len() - 4;
        let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
        let computed_crc = crc32fast::hash(&data[..crc_offset]);
        if stored_crc != computed_crc {
            return Err(ChunkMetaError::CrcMismatch {
                stored: stored_crc,
                computed: computed_crc,
            });
        }

        // Parse header
        if &data[0..4] != GLCM_MAGIC {
            return Err(ChunkMetaError::BadMagic);
        }
        let version = u16::from_le_bytes(data[4..6].try_into().unwrap());
        if version != GLCM_VERSION {
            return Err(ChunkMetaError::UnsupportedVersion(version));
        }
        let chunk_idx = u32::from_le_bytes(data[6..10].try_into().unwrap());
        let block_count = u32::from_le_bytes(data[10..14].try_into().unwrap()) as usize;
        let chunk_size = u64::from_le_bytes(data[14..22].try_into().unwrap());
        let block_size = u32::from_le_bytes(data[22..26].try_into().unwrap());
        // reserved: data[26..32]

        let expected_size = HEADER_SIZE + block_count * ENTRY_SIZE + 4;
        if data.len() < expected_size {
            return Err(ChunkMetaError::TooShort);
        }

        // Parse entries
        let mut entries = Vec::with_capacity(block_count);
        for i in 0..block_count {
            let base = HEADER_SIZE + i * ENTRY_SIZE;
            let offset = u32::from_le_bytes(data[base..base + 4].try_into().unwrap());
            let mut hash_bytes = [0u8; 16];
            hash_bytes.copy_from_slice(&data[base + 4..base + 20]);
            let hash = Blake3Hash(hash_bytes);
            let pack_id = Uuid::from_bytes(data[base + 20..base + 36].try_into().unwrap());
            let pack_offset = u32::from_le_bytes(data[base + 36..base + 40].try_into().unwrap());
            let comp_length = u32::from_le_bytes(data[base + 40..base + 44].try_into().unwrap());

            entries.push(ChunkMetaEntry {
                offset,
                hash,
                pack_id,
                pack_offset,
                comp_length,
            });
        }

        Ok(ChunkMeta {
            chunk_idx,
            chunk_size,
            block_size,
            entries,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChunkMetaError {
    #[error("data too short for GLCM header")]
    TooShort,
    #[error("bad GLCM magic bytes")]
    BadMagic,
    #[error("unsupported GLCM version: {0}")]
    UnsupportedVersion(u16),
    #[error("CRC32 mismatch: stored={stored:#010x}, computed={computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHUNK_SIZE: u64 = 10 * 1024 * 1024 * 1024; // 10GB

    fn make_entry(offset: u32, hash_byte: u8, pack_byte: u8) -> ChunkMetaEntry {
        ChunkMetaEntry {
            offset,
            hash: Blake3Hash([hash_byte; 16]),
            pack_id: Uuid::from_bytes([pack_byte; 16]),
            pack_offset: offset * 1000,
            comp_length: 65536,
        }
    }

    /// Helper to fix CRC after mutating serialized bytes.
    fn fix_crc(bytes: &mut Vec<u8>) {
        let crc_offset = bytes.len() - 4;
        let crc = crc32fast::hash(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn test_empty_round_trip() {
        let meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        let bytes = meta.serialize();
        assert_eq!(bytes.len(), HEADER_SIZE + 4);
        let parsed = ChunkMeta::deserialize(&bytes).unwrap();
        assert_eq!(parsed.chunk_idx, 0);
        assert_eq!(parsed.chunk_size, CHUNK_SIZE);
        assert_eq!(parsed.block_count(), 0);
    }

    #[test]
    fn test_single_entry_round_trip() {
        let mut meta = ChunkMeta::new(5, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(42, 0xaa, 0xbb));

        let bytes = meta.serialize();
        assert_eq!(bytes.len(), HEADER_SIZE + ENTRY_SIZE + 4);

        let parsed = ChunkMeta::deserialize(&bytes).unwrap();
        assert_eq!(parsed.chunk_idx, 5);
        assert_eq!(parsed.block_count(), 1);
        assert_eq!(parsed.entries[0].offset, 42);
        assert_eq!(parsed.entries[0].hash, Blake3Hash([0xaa; 16]));
        assert_eq!(parsed.entries[0].pack_id, Uuid::from_bytes([0xbb; 16]));
        assert_eq!(parsed.entries[0].pack_offset, 42000);
        assert_eq!(parsed.entries[0].comp_length, 65536);
    }

    #[test]
    fn test_multi_entry_sorted() {
        let mut meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(10, 0x01, 0x10));
        meta.entries.push(make_entry(20, 0x02, 0x20));
        meta.entries.push(make_entry(30, 0x03, 0x30));

        let bytes = meta.serialize();
        let parsed = ChunkMeta::deserialize(&bytes).unwrap();
        assert_eq!(parsed.block_count(), 3);
        assert_eq!(parsed.entries[0].offset, 10);
        assert_eq!(parsed.entries[1].offset, 20);
        assert_eq!(parsed.entries[2].offset, 30);
    }

    #[test]
    fn test_lookup_found() {
        let mut meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(10, 0x01, 0x10));
        meta.entries.push(make_entry(20, 0x02, 0x20));
        meta.entries.push(make_entry(30, 0x03, 0x30));

        let (hash, loc) = meta.lookup(20).expect("should find entry");
        assert_eq!(hash, Blake3Hash([0x02; 16]));
        assert_eq!(loc.pack_id, Uuid::from_bytes([0x20; 16]));
        assert_eq!(loc.offset, 20000);
        assert_eq!(loc.comp_length, 65536);
    }

    #[test]
    fn test_lookup_not_found() {
        let mut meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(10, 0x01, 0x10));
        assert!(meta.lookup(99).is_none());
    }

    #[test]
    fn test_content_hash_deterministic() {
        let mut meta1 = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta1.entries.push(make_entry(10, 0x01, 0x10));
        meta1.entries.push(make_entry(20, 0x02, 0x20));

        // Same block content, different pack locations → same content hash
        let mut meta2 = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta2.entries.push(ChunkMetaEntry {
            offset: 10,
            hash: Blake3Hash([0x01; 16]),
            pack_id: Uuid::from_bytes([0xff; 16]),
            pack_offset: 99999,
            comp_length: 1,
        });
        meta2.entries.push(ChunkMetaEntry {
            offset: 20,
            hash: Blake3Hash([0x02; 16]),
            pack_id: Uuid::from_bytes([0xee; 16]),
            pack_offset: 88888,
            comp_length: 2,
        });

        assert_eq!(meta1.content_hash(), meta2.content_hash());
    }

    #[test]
    fn test_content_hash_differs_on_different_blocks() {
        let mut meta1 = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta1.entries.push(make_entry(10, 0x01, 0x10));

        let mut meta2 = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta2.entries.push(make_entry(10, 0x02, 0x10));

        assert_ne!(meta1.content_hash(), meta2.content_hash());
    }

    #[test]
    fn test_block_hashes() {
        let mut meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(10, 0x01, 0x10));
        meta.entries.push(make_entry(20, 0x02, 0x20));
        meta.entries.push(make_entry(30, 0x01, 0x30)); // same hash as first

        let hashes = meta.block_hashes();
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&Blake3Hash([0x01; 16])));
        assert!(hashes.contains(&Blake3Hash([0x02; 16])));
    }

    #[test]
    fn test_pack_ids() {
        let mut meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(10, 0x01, 0x10));
        meta.entries.push(make_entry(20, 0x02, 0x10)); // same pack
        meta.entries.push(make_entry(30, 0x03, 0x30));

        let packs = meta.pack_ids();
        assert_eq!(packs.len(), 2);
    }

    #[test]
    fn test_merge_overwrites() {
        let mut meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(10, 0x01, 0x10));
        meta.entries.push(make_entry(20, 0x02, 0x20));
        meta.entries.push(make_entry(30, 0x03, 0x30));

        let new_entries = vec![
            make_entry(20, 0xaa, 0xbb), // overwrite
            make_entry(40, 0x04, 0x40), // new
        ];

        let merged = meta.merge(&new_entries);
        assert_eq!(merged.block_count(), 4);
        assert_eq!(merged.entries[0].offset, 10);
        assert_eq!(merged.entries[1].offset, 20);
        assert_eq!(merged.entries[1].hash, Blake3Hash([0xaa; 16]));
        assert_eq!(merged.entries[2].offset, 30);
        assert_eq!(merged.entries[3].offset, 40);
    }

    #[test]
    fn test_merge_empty_old() {
        let meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        let new_entries = vec![make_entry(5, 0x01, 0x10)];
        let merged = meta.merge(&new_entries);
        assert_eq!(merged.block_count(), 1);
    }

    #[test]
    fn test_merge_empty_new() {
        let mut meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(10, 0x01, 0x10));
        let merged = meta.merge(&[]);
        assert_eq!(merged.block_count(), 1);
    }

    #[test]
    fn test_crc_corruption_detected() {
        let mut meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        meta.entries.push(make_entry(10, 0x01, 0x10));

        let mut bytes = meta.serialize();
        bytes[HEADER_SIZE + 5] ^= 0xff;
        let err = ChunkMeta::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, ChunkMetaError::CrcMismatch { .. }));
    }

    #[test]
    fn test_bad_magic() {
        let meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        let mut bytes = meta.serialize();
        bytes[0] = b'X';
        fix_crc(&mut bytes);
        let err = ChunkMeta::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, ChunkMetaError::BadMagic));
    }

    #[test]
    fn test_truncated_data() {
        let err = ChunkMeta::deserialize(&[0u8; 10]).unwrap_err();
        assert!(matches!(err, ChunkMetaError::TooShort));
    }

    #[test]
    fn test_unsupported_version() {
        let meta = ChunkMeta::new(0, CHUNK_SIZE, 131072);
        let mut bytes = meta.serialize();
        bytes[4..6].copy_from_slice(&99u16.to_le_bytes());
        fix_crc(&mut bytes);
        let err = ChunkMeta::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, ChunkMetaError::UnsupportedVersion(99)));
    }
}
