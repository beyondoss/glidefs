//! Volume manifest v6: binary GLVM format with pack ID lists + block bitmap per chunk.
//!
//! The volume manifest maps chunk indices to ordered lists of pack IDs.
//! Stored as binary GLVM at `manifests/{export_name}` in S3.
//! Sparse: only chunks with written data appear. Absent = all-zero / unwritten.
//!
//! v6 changes from v5: per-chunk 1024-bit bitmap tracking which block offsets
//! have been written to S3. Enables fast zero-return for unwritten offsets
//! without any pack index resolution.
//!
//! v5 changes from v4: pack_count widened from u8 to u16 to avoid silent
//! truncation when a chunk accumulates 256+ packs (possible if compaction
//! keeps failing).

use std::collections::BTreeMap;

use super::pack::PackId;

/// GLVM magic bytes.
pub const GLVM_MAGIC: &[u8; 4] = b"GLVM";

/// Volume manifest version 6 (v6: per-chunk block bitmap).
pub const VOLUME_MANIFEST_VERSION: u16 = 6;

/// Size of the per-chunk block bitmap in bytes (1024 bits = 128 bytes).
const CHUNK_BITMAP_SIZE: usize = 128;

/// Default chunk size for v4: 128 MiB (= 1 ext4 block group).
pub const DEFAULT_CHUNK_SIZE: u64 = 128 * 1024 * 1024;

/// GLVM header size in bytes.
const GLVM_HEADER_SIZE: usize = 32;

/// One chunk's pack list + block bitmap in the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkEntry {
    /// Ordered pack list: oldest first, newest last (last = highest priority).
    /// After compaction: single entry covering all written blocks.
    pub packs: Vec<PackId>,
    /// 1024-bit bitmap: bit N set = block offset N has been written to S3.
    /// Used to short-circuit reads to unwritten offsets (return zeros without
    /// any pack index resolution).
    pub bitmap: [u8; CHUNK_BITMAP_SIZE],
}

/// Volume manifest v4: binary format mapping chunk indices to pack ID lists.
///
/// Stored as binary GLVM at the same `manifests/{export_name}` S3 key.
/// Sparse: only chunks with written data appear. Absent = all-zero / unwritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeManifest {
    /// Device size in bytes.
    pub size: u64,
    /// Bytes per chunk (default 128 MiB).
    pub chunk_size: u64,
    /// Bytes per block (default 131072 = 128 KB).
    pub block_size: u32,
    /// Sparse map: chunk_idx → ordered pack list.
    /// Only written chunks appear. Absent = all-zero / unwritten.
    pub chunks: BTreeMap<u32, ChunkEntry>,
}

#[derive(Debug, thiserror::Error)]
pub enum VolumeManifestError {
    #[error("unsupported volume manifest version: {0}")]
    UnsupportedVersion(u32),
    #[error("invalid config: block_size and chunk_size must be non-zero")]
    InvalidConfig,
    #[error("binary manifest data too short")]
    TooShort,
    #[error("bad GLVM magic bytes")]
    BadMagic,
    #[error("CRC32 mismatch: stored={stored:#010x}, computed={computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },
}

impl VolumeManifest {
    /// Create an empty v4 manifest for a new device.
    pub fn new(size: u64, block_size: u32) -> Self {
        Self {
            size,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size,
            chunks: BTreeMap::new(),
        }
    }

    /// Number of blocks that fit in one chunk.
    pub fn blocks_per_chunk(&self) -> u32 {
        (self.chunk_size / self.block_size as u64) as u32
    }

    /// Total number of chunks needed to cover the device.
    #[allow(dead_code)]
    pub fn num_chunks(&self) -> u32 {
        self.size.div_ceil(self.chunk_size) as u32
    }

    /// Which chunk index contains the given block index.
    pub fn chunk_idx_for_block(&self, block_index: u64) -> u32 {
        let block_byte_offset = block_index * self.block_size as u64;
        (block_byte_offset / self.chunk_size) as u32
    }

    /// Block offset within its chunk (0-based).
    pub fn block_offset_in_chunk(&self, block_index: u64) -> u32 {
        let blocks_per_chunk = self.blocks_per_chunk() as u64;
        (block_index % blocks_per_chunk) as u32
    }

    /// Get pack IDs for a chunk. Returns None if unwritten.
    pub fn chunk_pack_ids(&self, chunk_idx: u32) -> Option<&[PackId]> {
        self.chunks.get(&chunk_idx).map(|e| e.packs.as_slice())
    }

    /// Check if a block offset has been written to S3 (bitmap check).
    /// Returns false for absent chunks or unset bits.
    pub fn has_block_in_s3(&self, chunk_idx: u32, block_offset: u32) -> bool {
        let Some(entry) = self.chunks.get(&chunk_idx) else {
            return false;
        };
        let byte_idx = (block_offset / 8) as usize;
        let bit_idx = block_offset % 8;
        byte_idx < CHUNK_BITMAP_SIZE && (entry.bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    /// Set bitmap bits for the given block offsets in a chunk.
    pub fn set_block_bits(&mut self, chunk_idx: u32, offsets: &[u32]) {
        let Some(entry) = self.chunks.get_mut(&chunk_idx) else {
            return;
        };
        for &offset in offsets {
            let byte_idx = (offset / 8) as usize;
            let bit_idx = offset % 8;
            if byte_idx < CHUNK_BITMAP_SIZE {
                entry.bitmap[byte_idx] |= 1 << bit_idx;
            }
        }
    }

    /// Rebuild bitmap from scratch: zero all bits, then set the given offsets.
    /// Used after compaction to reflect only live blocks.
    pub fn rebuild_bitmap(&mut self, chunk_idx: u32, offsets: &[u32]) {
        let Some(entry) = self.chunks.get_mut(&chunk_idx) else {
            return;
        };
        entry.bitmap = [0u8; CHUNK_BITMAP_SIZE];
        for &offset in offsets {
            let byte_idx = (offset / 8) as usize;
            let bit_idx = offset % 8;
            if byte_idx < CHUNK_BITMAP_SIZE {
                entry.bitmap[byte_idx] |= 1 << bit_idx;
            }
        }
    }

    /// Collect all (chunk_idx, pack_id) pairs across the manifest.
    ///
    /// Pairs are structurally unique: each pack_id appears exactly once per chunk.
    /// Returns a Vec (no hashing overhead) — callers that need set semantics
    /// can collect into a HashSet.
    pub fn all_pack_ids(&self) -> Vec<(u32, PackId)> {
        let mut result = Vec::new();
        for (&chunk_idx, entry) in &self.chunks {
            for &pack_id in &entry.packs {
                result.push((chunk_idx, pack_id));
            }
        }
        result
    }

    /// Append a new pack to a chunk's pack list (after flush).
    pub fn append_pack(&mut self, chunk_idx: u32, pack_id: PackId) {
        self.chunks
            .entry(chunk_idx)
            .or_insert_with(|| ChunkEntry {
                packs: Vec::new(),
                bitmap: [0u8; CHUNK_BITMAP_SIZE],
            })
            .packs
            .push(pack_id);
    }

    /// Replace a chunk's entire pack list (after compaction).
    ///
    /// **WARNING**: This is a blind overwrite. Prefer `replace_packs_cas` for
    /// compaction to avoid losing packs appended by concurrent drain/flush.
    #[cfg(test)]
    pub fn replace_packs(&mut self, chunk_idx: u32, packs: Vec<PackId>) {
        if packs.is_empty() {
            self.chunks.remove(&chunk_idx);
        } else {
            self.chunks.insert(chunk_idx, ChunkEntry {
                packs,
                bitmap: [0u8; CHUNK_BITMAP_SIZE],
            });
        }
    }

    /// Compare-and-swap replacement: replaces `old_packs` with `new_packs`,
    /// preserving any packs appended after `old_packs` was snapshotted.
    ///
    /// Returns `true` if the replacement was applied, `false` if the pack list
    /// diverged (e.g., a concurrent drain appended packs during compaction).
    /// On `false`, the caller should abort — the orphaned new base pack in S3
    /// will be cleaned up by GC.
    pub fn replace_packs_cas(
        &mut self,
        chunk_idx: u32,
        old_packs: &[PackId],
        new_packs: Vec<PackId>,
    ) -> bool {
        let Some(entry) = self.chunks.get_mut(&chunk_idx) else {
            // Chunk vanished (concurrent remove). Abort.
            return false;
        };

        // old_packs must be an exact prefix of the current pack list.
        // If not, a concurrent operation modified the chunk.
        if entry.packs.len() < old_packs.len()
            || entry.packs[..old_packs.len()] != *old_packs
        {
            return false;
        }

        // Preserve any packs appended after the snapshot.
        let tail: Vec<PackId> = entry.packs[old_packs.len()..].to_vec();
        let mut merged = new_packs;
        merged.extend(tail);

        if merged.is_empty() {
            self.chunks.remove(&chunk_idx);
        } else {
            entry.packs = merged;
        }

        true
    }

    /// Serialize to binary GLVM format.
    ///
    /// ```text
    /// Header (32 bytes):
    ///   magic:        [u8; 4]  = b"GLVM"
    ///   version:      u16 LE   = 5
    ///   chunk_count:  u32 LE
    ///   chunk_size:   u32 LE
    ///   block_size:   u32 LE
    ///   device_size:  u64 LE
    ///   reserved:     [u8; 6]
    ///
    /// Chunk entries (sorted by chunk_idx):
    ///   chunk_idx:    u32 LE
    ///   pack_count:   u16 LE
    ///   bitmap:       [u8; 128]          (v6+)
    ///   packs:        [u64 LE; pack_count]
    ///
    /// CRC32 trailer: 4 bytes
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        // Pre-compute size.
        let entries_size: usize = self
            .chunks
            .values()
            .map(|e| 4 + 2 + CHUNK_BITMAP_SIZE + e.packs.len() * 8)
            .sum();
        let total = GLVM_HEADER_SIZE + entries_size + 4; // +4 for CRC32
        let mut buf = Vec::with_capacity(total);

        // Header (32 bytes).
        buf.extend_from_slice(GLVM_MAGIC); // 4
        buf.extend_from_slice(&VOLUME_MANIFEST_VERSION.to_le_bytes()); // 2
        buf.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes()); // 4
        // chunk_size: truncate to u32 (128 MiB fits)
        buf.extend_from_slice(&(self.chunk_size as u32).to_le_bytes()); // 4
        buf.extend_from_slice(&self.block_size.to_le_bytes()); // 4
        buf.extend_from_slice(&self.size.to_le_bytes()); // 8
        buf.extend_from_slice(&[0u8; 6]); // 6 reserved

        // Chunk entries (sorted by chunk_idx via BTreeMap iteration order).
        for (&chunk_idx, entry) in &self.chunks {
            debug_assert!(
                entry.packs.len() <= u16::MAX as usize,
                "chunk {chunk_idx} has {} packs, exceeds u16::MAX",
                entry.packs.len()
            );
            buf.extend_from_slice(&chunk_idx.to_le_bytes());
            buf.extend_from_slice(&(entry.packs.len() as u16).to_le_bytes());
            buf.extend_from_slice(&entry.bitmap);
            for &pack_id in &entry.packs {
                buf.extend_from_slice(&pack_id.to_le_bytes());
            }
        }

        // CRC32 trailer.
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        debug_assert_eq!(buf.len(), total);
        buf
    }

    /// Deserialize from binary GLVM format.
    pub fn deserialize(data: &[u8]) -> Result<Self, VolumeManifestError> {
        if data.len() < GLVM_HEADER_SIZE + 4 {
            return Err(VolumeManifestError::TooShort);
        }

        // Verify CRC32.
        let crc_offset = data.len() - 4;
        let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
        let computed_crc = crc32fast::hash(&data[..crc_offset]);
        if stored_crc != computed_crc {
            return Err(VolumeManifestError::CrcMismatch {
                stored: stored_crc,
                computed: computed_crc,
            });
        }

        // Parse header.
        if &data[0..4] != GLVM_MAGIC {
            return Err(VolumeManifestError::BadMagic);
        }
        let version = u16::from_le_bytes(data[4..6].try_into().unwrap());
        if version != 5 && version != VOLUME_MANIFEST_VERSION {
            return Err(VolumeManifestError::UnsupportedVersion(version as u32));
        }
        let has_bitmap = version >= 6;
        let chunk_count = u32::from_le_bytes(data[6..10].try_into().unwrap()) as usize;
        let chunk_size = u32::from_le_bytes(data[10..14].try_into().unwrap()) as u64;
        let block_size = u32::from_le_bytes(data[14..18].try_into().unwrap());
        let device_size = u64::from_le_bytes(data[18..26].try_into().unwrap());
        // reserved: data[26..32]

        if block_size == 0 || chunk_size == 0 {
            return Err(VolumeManifestError::InvalidConfig);
        }

        // Parse chunk entries (pack_count is u16 LE).
        let mut chunks = BTreeMap::new();
        let mut pos = GLVM_HEADER_SIZE;
        for _ in 0..chunk_count {
            if pos + 6 > crc_offset {
                return Err(VolumeManifestError::TooShort);
            }
            let chunk_idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            let pack_count =
                u16::from_le_bytes(data[pos + 4..pos + 6].try_into().unwrap()) as usize;
            pos += 6;

            let bitmap = if has_bitmap {
                if pos + CHUNK_BITMAP_SIZE > crc_offset {
                    return Err(VolumeManifestError::TooShort);
                }
                let mut bm = [0u8; CHUNK_BITMAP_SIZE];
                bm.copy_from_slice(&data[pos..pos + CHUNK_BITMAP_SIZE]);
                pos += CHUNK_BITMAP_SIZE;
                bm
            } else {
                // v5 backward compat: assume all blocks written (no false negatives)
                [0xFF; CHUNK_BITMAP_SIZE]
            };

            if pos + pack_count * 8 > crc_offset {
                return Err(VolumeManifestError::TooShort);
            }
            let mut packs = Vec::with_capacity(pack_count);
            for _ in 0..pack_count {
                let pack_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                packs.push(pack_id);
                pos += 8;
            }

            chunks.insert(chunk_idx, ChunkEntry { packs, bitmap });
        }

        Ok(VolumeManifest {
            size: device_size,
            chunk_size,
            block_size,
            chunks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v4_new_manifest() {
        let m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072); // 1 TB
        assert_eq!(m.size, 1024 * 1024 * 1024 * 1024);
        assert_eq!(m.chunk_size, DEFAULT_CHUNK_SIZE);
        assert_eq!(m.block_size, 131072);
        assert!(m.chunks.is_empty());
        // 128 MiB / 128 KB = 1024 blocks per chunk
        assert_eq!(m.blocks_per_chunk(), 1024);
        // 1 TB / 128 MiB = 8192 chunks
        assert_eq!(m.num_chunks(), 8192);
    }

    #[test]
    fn test_v4_binary_round_trip() {
        let mut m = VolumeManifest::new(100 * 1024 * 1024 * 1024, 131072); // 100 GiB
        m.append_pack(0, 0xDEADBEEF01234567);
        m.append_pack(0, 0xCAFEBABE89ABCDEF);
        m.append_pack(5, 0x1111111111111111);
        m.append_pack(800, 0x2222222222222222);

        let bytes = m.serialize();
        let m2 = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(m, m2);

        // Verify chunk lookups.
        assert_eq!(
            m2.chunk_pack_ids(0),
            Some([0xDEADBEEF01234567, 0xCAFEBABE89ABCDEF].as_slice())
        );
        assert_eq!(
            m2.chunk_pack_ids(5),
            Some([0x1111111111111111].as_slice())
        );
        assert_eq!(
            m2.chunk_pack_ids(800),
            Some([0x2222222222222222].as_slice())
        );
        assert_eq!(m2.chunk_pack_ids(1), None);
    }

    #[test]
    fn test_v4_sparse_only_written_chunks() {
        let m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072); // 1 TB, empty
        let bytes = m.serialize();
        // Header (32) + CRC32 (4) = 36 bytes for empty manifest.
        assert_eq!(bytes.len(), 36);

        let m2 = VolumeManifest::deserialize(&bytes).unwrap();
        assert!(m2.chunks.is_empty());
        assert_eq!(m2.size, 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_v6_manifest_size_matches_predictions() {
        // 1 TB, 50% written, compacted (1 pack/chunk).
        let mut m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072);
        for i in 0..4096 {
            // 50% of 8192 chunks
            m.append_pack(i * 2, 0x1234567890ABCDEF);
        }
        let bytes = m.serialize();
        // 32 (header) + 4096 * (4 + 2 + 128 + 8) (entries) + 4 (crc)
        // = 32 + 4096 * 142 + 4 = 32 + 581632 + 4 = 581668
        assert_eq!(bytes.len(), 581668);

        // Verify round-trip.
        let m2 = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(m, m2);
    }

    #[test]
    fn test_v4_crc_corruption_detected() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 42);

        let mut bytes = m.serialize();
        // Corrupt a data byte.
        bytes[GLVM_HEADER_SIZE + 2] ^= 0xFF;
        let err = VolumeManifest::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, VolumeManifestError::CrcMismatch { .. }));
    }

    #[test]
    fn test_v4_bad_magic() {
        let m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        let mut bytes = m.serialize();
        bytes[0] = b'X';
        // Fix CRC so we test magic detection, not CRC.
        let crc_offset = bytes.len() - 4;
        let crc = crc32fast::hash(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());

        let err = VolumeManifest::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, VolumeManifestError::BadMagic));
    }

    #[test]
    fn test_v4_truncated_data() {
        let err = VolumeManifest::deserialize(&[0u8; 10]).unwrap_err();
        assert!(matches!(err, VolumeManifestError::TooShort));
    }

    #[test]
    fn test_v4_append_and_replace_packs() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100);
        m.append_pack(0, 200);
        m.append_pack(0, 300);
        assert_eq!(m.chunk_pack_ids(0), Some([100, 200, 300].as_slice()));

        // Compaction: replace 3 packs with 1.
        m.replace_packs(0, vec![400]);
        assert_eq!(m.chunk_pack_ids(0), Some([400].as_slice()));

        // Replace with empty removes the chunk.
        m.replace_packs(0, vec![]);
        assert_eq!(m.chunk_pack_ids(0), None);
        assert!(m.chunks.is_empty());
    }

    #[test]
    fn test_v4_replace_packs_cas_basic() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100);
        m.append_pack(0, 200);
        m.append_pack(0, 300);

        // CAS with correct old_packs succeeds.
        assert!(m.replace_packs_cas(0, &[100, 200, 300], vec![400]));
        assert_eq!(m.chunk_pack_ids(0), Some([400].as_slice()));
    }

    #[test]
    fn test_v4_replace_packs_cas_preserves_concurrent_appends() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100);
        m.append_pack(0, 200);

        // Simulate compaction snapshot: old_packs = [100, 200]
        let old_packs = vec![100u64, 200];

        // Simulate concurrent drain appending pack 300.
        m.append_pack(0, 300);
        // Now chunk 0 = [100, 200, 300]

        // CAS with old snapshot preserves the concurrent append.
        assert!(m.replace_packs_cas(0, &old_packs, vec![400]));
        assert_eq!(m.chunk_pack_ids(0), Some([400, 300].as_slice()));
    }

    #[test]
    fn test_v4_replace_packs_cas_fails_on_diverged_prefix() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100);
        m.append_pack(0, 200);

        // Another compaction already replaced the pack list.
        m.replace_packs(0, vec![999]);

        // CAS with stale snapshot fails.
        assert!(!m.replace_packs_cas(0, &[100, 200], vec![400]));
        // Manifest is unchanged.
        assert_eq!(m.chunk_pack_ids(0), Some([999].as_slice()));
    }

    #[test]
    fn test_v4_replace_packs_cas_fails_on_missing_chunk() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        // Chunk doesn't exist.
        assert!(!m.replace_packs_cas(0, &[100], vec![200]));
    }

    #[test]
    fn test_v4_all_pack_ids() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100);
        m.append_pack(0, 200);
        m.append_pack(5, 300);
        m.append_pack(5, 100); // same pack_id in different chunk is distinct

        let ids = m.all_pack_ids();
        assert_eq!(ids.len(), 4);
        assert!(ids.contains(&(0, 100)));
        assert!(ids.contains(&(0, 200)));
        assert!(ids.contains(&(5, 300)));
        assert!(ids.contains(&(5, 100)));
    }

    #[test]
    fn test_v4_chunk_idx_for_block() {
        let m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072); // 1 TB
        // 128 MiB / 128 KB = 1024 blocks per chunk
        assert_eq!(m.chunk_idx_for_block(0), 0);
        assert_eq!(m.chunk_idx_for_block(1023), 0);
        assert_eq!(m.chunk_idx_for_block(1024), 1);
        assert_eq!(m.chunk_idx_for_block(2048), 2);
    }

    #[test]
    fn test_v4_block_offset_in_chunk() {
        let m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072);
        assert_eq!(m.block_offset_in_chunk(0), 0);
        assert_eq!(m.block_offset_in_chunk(1023), 1023);
        assert_eq!(m.block_offset_in_chunk(1024), 0);
        assert_eq!(m.block_offset_in_chunk(1025), 1);
    }

    #[test]
    fn test_v4_deserialize_empty() {
        let err = VolumeManifest::deserialize(&[]).unwrap_err();
        assert!(matches!(err, VolumeManifestError::TooShort));
    }

    #[test]
    fn test_v4_deserialize_random_bytes() {
        // 64 bytes of garbage — long enough to pass the length check,
        // but CRC won't match.
        let garbage: Vec<u8> = (0u8..64).collect();
        let err = VolumeManifest::deserialize(&garbage).unwrap_err();
        assert!(matches!(err, VolumeManifestError::CrcMismatch { .. }));
    }

    #[test]
    fn test_v6_deserialize_truncated_entry() {
        // Serialize a valid manifest, then chop it so the chunk entry data
        // is incomplete. We re-compute the CRC over the truncated payload
        // so that the CRC check passes but entry parsing hits TooShort.
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 42);
        m.append_pack(0, 43);

        let full = m.serialize();
        // Keep header + partial entry (chunk_idx + pack_count + partial bitmap).
        // 6 bytes of chunk_idx+pack_count, but truncate in the bitmap.
        let body = &full[..GLVM_HEADER_SIZE + 6 + 64]; // only 64 of 128 bitmap bytes
        let mut truncated = body.to_vec();
        let crc = crc32fast::hash(&truncated);
        truncated.extend_from_slice(&crc.to_le_bytes());

        let err = VolumeManifest::deserialize(&truncated).unwrap_err();
        assert!(matches!(err, VolumeManifestError::TooShort));
    }

    #[test]
    fn test_v6_bitmap_set_and_check() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100);

        // Initially all bits are zero.
        assert!(!m.has_block_in_s3(0, 0));
        assert!(!m.has_block_in_s3(0, 500));
        assert!(!m.has_block_in_s3(0, 1023));

        // Absent chunk returns false.
        assert!(!m.has_block_in_s3(1, 0));

        // Set some bits.
        m.set_block_bits(0, &[0, 500, 1023]);
        assert!(m.has_block_in_s3(0, 0));
        assert!(m.has_block_in_s3(0, 500));
        assert!(m.has_block_in_s3(0, 1023));
        assert!(!m.has_block_in_s3(0, 1));
        assert!(!m.has_block_in_s3(0, 501));
    }

    #[test]
    fn test_v6_bitmap_rebuild() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100);
        m.set_block_bits(0, &[0, 1, 2, 3, 100, 200, 300]);

        // Rebuild with subset — old bits should be cleared.
        m.rebuild_bitmap(0, &[100, 200]);
        assert!(!m.has_block_in_s3(0, 0));
        assert!(!m.has_block_in_s3(0, 1));
        assert!(!m.has_block_in_s3(0, 300));
        assert!(m.has_block_in_s3(0, 100));
        assert!(m.has_block_in_s3(0, 200));
    }

    #[test]
    fn test_v6_bitmap_round_trip() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100);
        m.set_block_bits(0, &[0, 7, 8, 15, 16, 255, 512, 1023]);
        m.append_pack(5, 200);
        m.set_block_bits(5, &[42]);

        let bytes = m.serialize();
        let m2 = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(m, m2);

        // Verify bitmap survived round-trip.
        assert!(m2.has_block_in_s3(0, 0));
        assert!(m2.has_block_in_s3(0, 7));
        assert!(m2.has_block_in_s3(0, 1023));
        assert!(!m2.has_block_in_s3(0, 1));
        assert!(m2.has_block_in_s3(5, 42));
        assert!(!m2.has_block_in_s3(5, 43));
    }

    #[test]
    fn test_v5_backward_compat_all_ones_bitmap() {
        // Manually build a v5-format manifest (no bitmap).
        let mut buf = Vec::new();
        buf.extend_from_slice(GLVM_MAGIC);
        buf.extend_from_slice(&5u16.to_le_bytes()); // v5
        buf.extend_from_slice(&1u32.to_le_bytes()); // 1 chunk
        buf.extend_from_slice(&(DEFAULT_CHUNK_SIZE as u32).to_le_bytes());
        buf.extend_from_slice(&131072u32.to_le_bytes());
        buf.extend_from_slice(&(1024u64 * 1024 * 1024).to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]); // reserved
        // Chunk entry: chunk_idx=0, pack_count=1, pack_id=42
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&42u64.to_le_bytes());
        // CRC
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        let m = VolumeManifest::deserialize(&buf).unwrap();
        assert_eq!(m.chunk_pack_ids(0), Some([42].as_slice()));
        // v5 backward compat: all bits set (conservative).
        assert!(m.has_block_in_s3(0, 0));
        assert!(m.has_block_in_s3(0, 500));
        assert!(m.has_block_in_s3(0, 1023));
    }
}
