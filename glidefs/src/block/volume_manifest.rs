//! Volume manifest: lightweight JSON mapping of chunk indices to chunk content hashes.
//!
//! The volume manifest is the top-level metadata for a chunked block index volume.
//! It's ~1KB of JSON that maps chunk_idx → chunk_hash (BLAKE3-128 hex). Unwritten
//! chunk indices are absent from the map. This replaces the flat GLDE binary manifest
//! for the v3 chunked architecture.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::block_map::Blake3Hash;

/// Current volume manifest version.
pub const VOLUME_MANIFEST_VERSION: u32 = 3;

/// Default chunk size: 10GB.
pub const DEFAULT_CHUNK_SIZE: u64 = 10 * 1024 * 1024 * 1024;

/// Volume manifest: maps chunk indices to content-addressed chunk hashes.
///
/// Stored as JSON at the same `manifests/{export_name}` S3 key.
/// Typically ~1KB even for large volumes (sparse map of written chunks only).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeManifest {
    /// Device size in bytes.
    pub size: u64,
    /// Format version (= 3).
    pub version: u32,
    /// Bytes per chunk (default 10GB).
    pub chunk_size: u64,
    /// Bytes per block (default 131072 = 128KB).
    pub block_size: u32,
    /// Sparse map: chunk_idx → BLAKE3-128 content hash (hex string).
    /// Only written chunks appear. Absent = all-zero / unwritten.
    pub chunks: BTreeMap<u32, String>,
}

impl VolumeManifest {
    /// Create an empty volume manifest for a new device.
    pub fn new(size: u64, block_size: u32) -> Self {
        Self {
            size,
            version: VOLUME_MANIFEST_VERSION,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size,
            chunks: BTreeMap::new(),
        }
    }

    /// Create with a custom chunk size (for testing).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_chunk_size(size: u64, block_size: u32, chunk_size: u64) -> Self {
        Self {
            size,
            version: VOLUME_MANIFEST_VERSION,
            chunk_size,
            block_size,
            chunks: BTreeMap::new(),
        }
    }

    /// Number of blocks that fit in one chunk.
    pub fn blocks_per_chunk(&self) -> u32 {
        (self.chunk_size / self.block_size as u64) as u32
    }

    /// Total number of chunks needed to cover the device.
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

    /// Get the chunk hash for a given chunk index. Returns None if unwritten.
    pub fn get_chunk_hash(&self, chunk_idx: u32) -> Option<Blake3Hash> {
        self.chunks
            .get(&chunk_idx)
            .map(|hex| hex_to_blake3(hex))
    }

    /// Set the chunk hash for a given chunk index.
    pub fn set_chunk_hash(&mut self, chunk_idx: u32, hash: Blake3Hash) {
        self.chunks.insert(chunk_idx, blake3_to_hex(&hash));
    }

    /// Remove a chunk (mark as unwritten). Returns whether it was present.
    pub fn remove_chunk(&mut self, chunk_idx: u32) -> bool {
        self.chunks.remove(&chunk_idx).is_some()
    }

    /// Serialize to JSON bytes.
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("VolumeManifest serialization cannot fail")
    }

    /// Deserialize from JSON bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, VolumeManifestError> {
        let manifest: Self =
            serde_json::from_slice(data).map_err(VolumeManifestError::Json)?;
        if manifest.version != VOLUME_MANIFEST_VERSION {
            return Err(VolumeManifestError::UnsupportedVersion(manifest.version));
        }
        if manifest.block_size == 0 || manifest.chunk_size == 0 {
            return Err(VolumeManifestError::InvalidConfig);
        }
        Ok(manifest)
    }
}

/// Convert Blake3Hash to lowercase hex string.
fn blake3_to_hex(hash: &Blake3Hash) -> String {
    hash.0.iter().map(|b| format!("{b:02x}")).collect()
}

/// Convert hex string to Blake3Hash.
fn hex_to_blake3(hex: &str) -> Blake3Hash {
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let start = i * 2;
        if start + 2 <= hex.len() {
            *byte = u8::from_str_radix(&hex[start..start + 2], 16).unwrap_or(0);
        }
    }
    Blake3Hash(bytes)
}

#[derive(Debug, thiserror::Error)]
pub enum VolumeManifestError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported volume manifest version: {0}")]
    UnsupportedVersion(u32),
    #[error("invalid config: block_size and chunk_size must be non-zero")]
    InvalidConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_manifest() {
        let m = VolumeManifest::new(256 * 1024 * 1024 * 1024, 131072);
        assert_eq!(m.size, 256 * 1024 * 1024 * 1024);
        assert_eq!(m.version, 3);
        assert_eq!(m.chunk_size, DEFAULT_CHUNK_SIZE);
        assert_eq!(m.block_size, 131072);
        assert!(m.chunks.is_empty());
    }

    #[test]
    fn test_blocks_per_chunk() {
        let m = VolumeManifest::new(256 * 1024 * 1024 * 1024, 131072);
        // 10GB / 128KB = 81920
        assert_eq!(m.blocks_per_chunk(), 81920);
    }

    #[test]
    fn test_num_chunks() {
        // 256GB / 10GB = 25.6 → 26 chunks
        let m = VolumeManifest::new(256 * 1024 * 1024 * 1024, 131072);
        assert_eq!(m.num_chunks(), 26);

        // Exact multiple: 10GB / 10GB = 1
        let m2 = VolumeManifest::new(DEFAULT_CHUNK_SIZE, 131072);
        assert_eq!(m2.num_chunks(), 1);
    }

    #[test]
    fn test_chunk_idx_for_block() {
        let m = VolumeManifest::new(256 * 1024 * 1024 * 1024, 131072);
        // Block 0 → chunk 0
        assert_eq!(m.chunk_idx_for_block(0), 0);
        // Last block in chunk 0 (81919) → chunk 0
        assert_eq!(m.chunk_idx_for_block(81919), 0);
        // First block in chunk 1 (81920) → chunk 1
        assert_eq!(m.chunk_idx_for_block(81920), 1);
        // Block 200000 → chunk 2
        assert_eq!(m.chunk_idx_for_block(200000), 2);
    }

    #[test]
    fn test_block_offset_in_chunk() {
        let m = VolumeManifest::new(256 * 1024 * 1024 * 1024, 131072);
        assert_eq!(m.block_offset_in_chunk(0), 0);
        assert_eq!(m.block_offset_in_chunk(81919), 81919);
        assert_eq!(m.block_offset_in_chunk(81920), 0);
        assert_eq!(m.block_offset_in_chunk(81921), 1);
    }

    #[test]
    fn test_round_trip() {
        let mut m = VolumeManifest::new(256 * 1024 * 1024 * 1024, 131072);
        let hash = Blake3Hash([0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x01, 0x23,
                               0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x00, 0x11]);
        m.set_chunk_hash(0, hash);
        m.set_chunk_hash(5, Blake3Hash([0xff; 16]));

        let bytes = m.serialize();
        let m2 = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(m, m2);
        assert_eq!(m2.get_chunk_hash(0), Some(hash));
        assert_eq!(m2.get_chunk_hash(5), Some(Blake3Hash([0xff; 16])));
        assert_eq!(m2.get_chunk_hash(1), None);
    }

    #[test]
    fn test_remove_chunk() {
        let mut m = VolumeManifest::new(10 * 1024 * 1024 * 1024, 131072);
        let hash = Blake3Hash([0x42; 16]);
        m.set_chunk_hash(0, hash);
        assert!(m.get_chunk_hash(0).is_some());
        assert!(m.remove_chunk(0));
        assert!(m.get_chunk_hash(0).is_none());
        assert!(!m.remove_chunk(0)); // already removed
    }

    #[test]
    fn test_rejects_bad_version() {
        let mut m = VolumeManifest::new(10 * 1024 * 1024 * 1024, 131072);
        m.version = 99;
        let bytes = serde_json::to_vec(&m).unwrap();
        let err = VolumeManifest::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, VolumeManifestError::UnsupportedVersion(99)));
    }

    #[test]
    fn test_rejects_invalid_config() {
        let m = VolumeManifest {
            size: 1024,
            version: VOLUME_MANIFEST_VERSION,
            chunk_size: 0,
            block_size: 131072,
            chunks: BTreeMap::new(),
        };
        let bytes = serde_json::to_vec(&m).unwrap();
        let err = VolumeManifest::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, VolumeManifestError::InvalidConfig));
    }

    #[test]
    fn test_hex_round_trip() {
        let hash = Blake3Hash([0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x01, 0x23,
                               0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x00, 0x11]);
        let hex = blake3_to_hex(&hash);
        assert_eq!(hex, "a1b2c3d4e5f60123456789abcdef0011");
        let back = hex_to_blake3(&hex);
        assert_eq!(back, hash);
    }

    #[test]
    fn test_small_device_single_chunk() {
        // 1GB device → 1 chunk (10GB chunk size > device)
        let m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        assert_eq!(m.num_chunks(), 1);
        assert_eq!(m.blocks_per_chunk(), 81920);
        assert_eq!(m.chunk_idx_for_block(0), 0);
        assert_eq!(m.chunk_idx_for_block(8191), 0); // last block in 1GB
    }

    #[test]
    fn test_custom_chunk_size() {
        // 1GB chunks for testing
        let m = VolumeManifest::with_chunk_size(
            10 * 1024 * 1024 * 1024, // 10GB device
            131072,                    // 128KB blocks
            1024 * 1024 * 1024,       // 1GB chunks
        );
        assert_eq!(m.num_chunks(), 10);
        assert_eq!(m.blocks_per_chunk(), 8192); // 1GB / 128KB
        assert_eq!(m.chunk_idx_for_block(0), 0);
        assert_eq!(m.chunk_idx_for_block(8191), 0);
        assert_eq!(m.chunk_idx_for_block(8192), 1);
    }
}
