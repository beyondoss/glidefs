//! Binary manifest format for content-addressed S3 storage.
//!
//! A manifest captures the full state of an export at a point in time:
//! which chunks exist, their content hashes, and where the data lives
//! in pack files. This is the unit of snapshot/restore.
//!
//! Wire format is designed for direct serialization with no padding or
//! alignment constraints -- all integers are little-endian, all fields
//! are tightly packed.

use std::io;

use super::block_map::Blake3Hash;
use uuid::Uuid;

pub const MANIFEST_MAGIC: &[u8; 4] = b"GLDE";
pub const MANIFEST_VERSION: u16 = 1;

/// Fixed header size (before variable-length name):
/// 4 (magic) + 2 (version) + 2 (flags) + 2 (name_len) + 8 (sequence) +
/// 4 (chunk_size) + 8 (device_size) + 8 (block_map_count) + 8 (pack_index_count) = 46 bytes
pub const MANIFEST_HEADER_FIXED_SIZE: usize = 46;
pub const MANIFEST_BLOCK_ENTRY_SIZE: usize = 25;
pub const MANIFEST_PACK_ENTRY_SIZE: usize = 40;

#[derive(Debug, Clone)]
pub struct Manifest {
    pub name: String,
    pub sequence: u64,
    pub chunk_size: u32,
    pub device_size: u64,
    pub block_map: Vec<ManifestBlockEntry>,
    pub pack_index: Vec<ManifestPackEntry>,
}

#[derive(Debug, Clone)]
pub struct ManifestBlockEntry {
    pub chunk_index: u64,
    pub hash: Blake3Hash,
    pub flags: u8,
}

#[derive(Debug, Clone)]
pub struct ManifestPackEntry {
    pub hash: Blake3Hash,
    pub pack_id: Uuid,
    pub offset: u32,
    pub comp_length: u32,
}

impl Manifest {
    /// Serialize the manifest to binary format.
    pub fn serialize(&self) -> Vec<u8> {
        let name_bytes = self.name.as_bytes();
        let name_len = name_bytes.len();

        let total_size = MANIFEST_HEADER_FIXED_SIZE
            + name_len
            + self.block_map.len() * MANIFEST_BLOCK_ENTRY_SIZE
            + self.pack_index.len() * MANIFEST_PACK_ENTRY_SIZE
            + 4; // trailing CRC32

        let mut buf = Vec::with_capacity(total_size);

        // Header
        buf.extend_from_slice(MANIFEST_MAGIC);
        buf.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags (reserved)
        buf.extend_from_slice(&(name_len as u16).to_le_bytes());
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.extend_from_slice(&self.chunk_size.to_le_bytes());
        buf.extend_from_slice(&self.device_size.to_le_bytes());
        buf.extend_from_slice(&(self.block_map.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(self.pack_index.len() as u64).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        // Block map section
        for entry in &self.block_map {
            buf.extend_from_slice(&entry.chunk_index.to_le_bytes());
            buf.extend_from_slice(entry.hash.as_bytes());
            buf.push(entry.flags);
        }

        // Pack index section
        for entry in &self.pack_index {
            buf.extend_from_slice(entry.hash.as_bytes());
            buf.extend_from_slice(entry.pack_id.as_bytes());
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.comp_length.to_le_bytes());
        }

        // Trailing CRC32 over all preceding bytes
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize a manifest from binary data.
    pub fn deserialize(data: &[u8]) -> io::Result<Manifest> {
        if data.len() < MANIFEST_HEADER_FIXED_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest too short for header",
            ));
        }

        // Magic
        if &data[0..4] != MANIFEST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid manifest magic",
            ));
        }

        // Version
        let version = u16::from_le_bytes([data[4], data[5]]);
        if version != MANIFEST_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported manifest version: {version}"),
            ));
        }

        // flags at [6..8] -- reserved, ignored on read
        let name_len = u16::from_le_bytes([data[8], data[9]]) as usize;
        let sequence = u64::from_le_bytes(data[10..18].try_into().unwrap());
        let chunk_size = u32::from_le_bytes(data[18..22].try_into().unwrap());
        let device_size = u64::from_le_bytes(data[22..30].try_into().unwrap());
        let block_map_count = u64::from_le_bytes(data[30..38].try_into().unwrap()) as usize;
        let pack_index_count = u64::from_le_bytes(data[38..46].try_into().unwrap()) as usize;

        let header_total = MANIFEST_HEADER_FIXED_SIZE + name_len;
        let payload_len = header_total
            + block_map_count * MANIFEST_BLOCK_ENTRY_SIZE
            + pack_index_count * MANIFEST_PACK_ENTRY_SIZE;
        let expected_len = payload_len + 4; // + trailing CRC32

        if data.len() < expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "manifest data too short: have {} bytes, need {expected_len}",
                    data.len()
                ),
            ));
        }

        // Verify trailing CRC32
        let stored_crc = u32::from_le_bytes(
            data[payload_len..payload_len + 4].try_into().unwrap(),
        );
        let computed_crc = crc32fast::hash(&data[..payload_len]);
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest CRC32 mismatch",
            ));
        }

        let name = std::str::from_utf8(&data[46..46 + name_len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_owned();

        // Block map section
        let mut block_map = Vec::with_capacity(block_map_count);
        let mut pos = header_total;
        for _ in 0..block_map_count {
            let chunk_index = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let mut hash_bytes = [0u8; 16];
            hash_bytes.copy_from_slice(&data[pos + 8..pos + 24]);
            let flags = data[pos + 24];
            block_map.push(ManifestBlockEntry {
                chunk_index,
                hash: Blake3Hash::from_bytes(hash_bytes),
                flags,
            });
            pos += MANIFEST_BLOCK_ENTRY_SIZE;
        }

        // Pack index section
        let mut pack_index = Vec::with_capacity(pack_index_count);
        for _ in 0..pack_index_count {
            let mut hash_bytes = [0u8; 16];
            hash_bytes.copy_from_slice(&data[pos..pos + 16]);
            let mut uuid_bytes = [0u8; 16];
            uuid_bytes.copy_from_slice(&data[pos + 16..pos + 32]);
            let offset = u32::from_le_bytes(data[pos + 32..pos + 36].try_into().unwrap());
            let comp_length = u32::from_le_bytes(data[pos + 36..pos + 40].try_into().unwrap());
            pack_index.push(ManifestPackEntry {
                hash: Blake3Hash::from_bytes(hash_bytes),
                pack_id: Uuid::from_bytes(uuid_bytes),
                offset,
                comp_length,
            });
            pos += MANIFEST_PACK_ENTRY_SIZE;
        }

        Ok(Manifest {
            name,
            sequence,
            chunk_size,
            device_size,
            block_map,
            pack_index,
        })
    }
}

/// Generate S3 key for a manifest: "manifests/{name}"
pub fn manifest_s3_key(name: &str) -> String {
    format!("manifests/{name}")
}

// ============================================================================
// Boot Hot Set — list of chunk indices needed during boot
// ============================================================================

const HOT_SET_MAGIC: &[u8; 4] = b"GLHS";

/// Serialize a list of chunk indices into a hot set binary format.
/// Format: magic (4 bytes) + count (4 bytes LE) + chunk_indices (8 bytes LE each)
pub fn serialize_hot_set(chunks: &[u64]) -> Vec<u8> {
    let mut data = Vec::with_capacity(8 + chunks.len() * 8);
    data.extend_from_slice(HOT_SET_MAGIC);
    data.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for &chunk in chunks {
        data.extend_from_slice(&chunk.to_le_bytes());
    }
    data
}

/// Deserialize a hot set from binary format.
pub fn deserialize_hot_set(data: &[u8]) -> anyhow::Result<Vec<u64>> {
    if data.len() < 8 {
        anyhow::bail!("hot set too small");
    }
    if &data[..4] != HOT_SET_MAGIC {
        anyhow::bail!("invalid hot set magic");
    }
    let count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let expected_len = 8 + count * 8;
    if data.len() < expected_len {
        anyhow::bail!(
            "hot set truncated: expected {} bytes, got {}",
            expected_len,
            data.len()
        );
    }
    let mut chunks = Vec::with_capacity(count);
    for i in 0..count {
        let offset = 8 + i * 8;
        let chunk = u64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        chunks.push(chunk);
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn make_hash(seed: u8) -> Blake3Hash {
        let mut bytes = [0u8; 16];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        Blake3Hash::from_bytes(bytes)
    }

    #[test]
    fn test_manifest_round_trip() {
        let block_map: Vec<ManifestBlockEntry> = (0..1000)
            .map(|i| ManifestBlockEntry {
                chunk_index: i * 3,
                hash: make_hash((i % 256) as u8),
                flags: (i % 4) as u8,
            })
            .collect();

        let pack_index: Vec<ManifestPackEntry> = (0..40)
            .map(|i| {
                let mut uuid_bytes = [0u8; 16];
                uuid_bytes[0] = i as u8;
                uuid_bytes[15] = (i * 7) as u8;
                ManifestPackEntry {
                    hash: make_hash((i + 100) as u8),
                    pack_id: Uuid::from_bytes(uuid_bytes),
                    offset: i * 1024,
                    comp_length: 512 + i * 10,
                }
            })
            .collect();

        let manifest = Manifest {
            name: "test-export".to_string(),
            sequence: 42,
            chunk_size: 131072,
            device_size: 10 * 1024 * 1024 * 1024,
            block_map,
            pack_index,
        };

        let data = manifest.serialize();
        let restored = Manifest::deserialize(&data).unwrap();

        assert_eq!(restored.name, manifest.name);
        assert_eq!(restored.sequence, manifest.sequence);
        assert_eq!(restored.chunk_size, manifest.chunk_size);
        assert_eq!(restored.device_size, manifest.device_size);
        assert_eq!(restored.block_map.len(), manifest.block_map.len());
        assert_eq!(restored.pack_index.len(), manifest.pack_index.len());

        for (orig, got) in manifest.block_map.iter().zip(restored.block_map.iter()) {
            assert_eq!(orig.chunk_index, got.chunk_index);
            assert_eq!(orig.hash, got.hash);
            assert_eq!(orig.flags, got.flags);
        }

        for (orig, got) in manifest.pack_index.iter().zip(restored.pack_index.iter()) {
            assert_eq!(orig.hash, got.hash);
            assert_eq!(orig.pack_id, got.pack_id);
            assert_eq!(orig.offset, got.offset);
            assert_eq!(orig.comp_length, got.comp_length);
        }
    }

    #[test]
    fn test_manifest_header_fields() {
        let manifest = Manifest {
            name: "myvm".to_string(),
            sequence: 99,
            chunk_size: 131072,
            device_size: 1024 * 1024 * 1024,
            block_map: vec![ManifestBlockEntry {
                chunk_index: 7,
                hash: make_hash(0xAB),
                flags: 1,
            }],
            pack_index: vec![],
        };

        let data = manifest.serialize();

        // Magic
        assert_eq!(&data[0..4], b"GLDE");
        // Version
        assert_eq!(u16::from_le_bytes([data[4], data[5]]), 1);
        // Flags
        assert_eq!(u16::from_le_bytes([data[6], data[7]]), 0);
        // name_len
        assert_eq!(u16::from_le_bytes([data[8], data[9]]), 4);
        // sequence
        assert_eq!(u64::from_le_bytes(data[10..18].try_into().unwrap()), 99);
        // chunk_size
        assert_eq!(
            u32::from_le_bytes(data[18..22].try_into().unwrap()),
            131072
        );
        // device_size
        assert_eq!(
            u64::from_le_bytes(data[22..30].try_into().unwrap()),
            1024 * 1024 * 1024
        );
        // block_map_count
        assert_eq!(u64::from_le_bytes(data[30..38].try_into().unwrap()), 1);
        // pack_index_count
        assert_eq!(u64::from_le_bytes(data[38..46].try_into().unwrap()), 0);
        // name
        assert_eq!(&data[46..50], b"myvm");
    }

    #[test]
    fn test_manifest_sparse_encoding() {
        let manifest = Manifest {
            name: "sparse".to_string(),
            sequence: 1,
            chunk_size: 131072,
            device_size: 100 * 1024 * 1024 * 1024, // 100GB -- irrelevant to output size
            block_map: vec![
                ManifestBlockEntry {
                    chunk_index: 0,
                    hash: make_hash(1),
                    flags: 0,
                },
                ManifestBlockEntry {
                    chunk_index: 500,
                    hash: make_hash(2),
                    flags: 0,
                },
                ManifestBlockEntry {
                    chunk_index: 100_000,
                    hash: make_hash(3),
                    flags: 0,
                },
            ],
            pack_index: vec![],
        };

        let data = manifest.serialize();
        let name_len = "sparse".len();
        let expected = MANIFEST_HEADER_FIXED_SIZE + name_len + 3 * MANIFEST_BLOCK_ENTRY_SIZE + 4;
        assert_eq!(
            data.len(),
            expected,
            "sparse manifest should be {} bytes, got {}",
            expected,
            data.len()
        );
    }

    #[test]
    fn test_manifest_empty() {
        let manifest = Manifest {
            name: "empty".to_string(),
            sequence: 0,
            chunk_size: 131072,
            device_size: 1024 * 1024,
            block_map: vec![],
            pack_index: vec![],
        };

        let data = manifest.serialize();
        let restored = Manifest::deserialize(&data).unwrap();

        assert_eq!(restored.name, "empty");
        assert_eq!(restored.sequence, 0);
        assert_eq!(restored.chunk_size, 131072);
        assert_eq!(restored.device_size, 1024 * 1024);
        assert!(restored.block_map.is_empty());
        assert!(restored.pack_index.is_empty());
    }

    #[test]
    fn test_manifest_large() {
        let block_map: Vec<ManifestBlockEntry> = (0..800_000)
            .map(|i| ManifestBlockEntry {
                chunk_index: i,
                hash: make_hash((i % 256) as u8),
                flags: (i % 2) as u8,
            })
            .collect();

        let manifest = Manifest {
            name: "large".to_string(),
            sequence: 1_000_000,
            chunk_size: 131072,
            device_size: 100 * 1024 * 1024 * 1024,
            block_map,
            pack_index: vec![],
        };

        let start = Instant::now();
        let data = manifest.serialize();
        let restored = Manifest::deserialize(&data).unwrap();
        let elapsed = start.elapsed();

        assert_eq!(restored.block_map.len(), 800_000);
        for (orig, got) in manifest.block_map.iter().zip(restored.block_map.iter()) {
            assert_eq!(orig.chunk_index, got.chunk_index);
            assert_eq!(orig.hash, got.hash);
            assert_eq!(orig.flags, got.flags);
        }

        assert!(
            elapsed.as_millis() < 2_000,
            "round-trip took {}ms, expected < 2000ms",
            elapsed.as_millis()
        );
    }

    #[test]
    fn test_manifest_s3_key() {
        assert_eq!(manifest_s3_key("my-vm"), "manifests/my-vm");
    }

    #[test]
    fn test_manifest_name_utf8() {
        let manifest = Manifest {
            name: "vm-\u{00e9}\u{00e8}\u{00ea}-\u{1f600}".to_string(),
            sequence: 1,
            chunk_size: 131072,
            device_size: 1024 * 1024,
            block_map: vec![],
            pack_index: vec![],
        };

        let data = manifest.serialize();
        let restored = Manifest::deserialize(&data).unwrap();

        assert_eq!(restored.name, manifest.name);
    }
}
