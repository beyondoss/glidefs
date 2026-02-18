//! Per-VM pack registry for garbage collection.
//!
//! An append-only S3 object per VM that records every pack ID created by
//! or inherited by that VM. GC reads registries to enumerate packs, reads
//! manifests to determine liveness, deletes the difference.
//!
//! Wire format (no padding, little-endian):
//!   magic      [u8; 4]  = "GLPR"
//!   count      u32 LE   (number of pack IDs)
//!   pack_ids   [Uuid; count]  (16 bytes each, packed)

use std::collections::HashSet;
use std::io;

use uuid::Uuid;

pub const PACK_REGISTRY_MAGIC: &[u8; 4] = b"GLPR";
const HEADER_SIZE: usize = 8; // 4 magic + 4 count
const UUID_SIZE: usize = 16;

#[derive(Debug)]
pub struct PackRegistry {
    pub pack_ids: Vec<Uuid>,
}

impl Default for PackRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PackRegistry {
    pub fn new() -> Self {
        Self {
            pack_ids: Vec::new(),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let total = HEADER_SIZE + self.pack_ids.len() * UUID_SIZE;
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(PACK_REGISTRY_MAGIC);
        buf.extend_from_slice(&(self.pack_ids.len() as u32).to_le_bytes());
        for id in &self.pack_ids {
            buf.extend_from_slice(id.as_bytes());
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> io::Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pack registry too short for header",
            ));
        }

        if &data[0..4] != PACK_REGISTRY_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid pack registry magic",
            ));
        }

        let count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let expected = HEADER_SIZE + count * UUID_SIZE;
        if data.len() < expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "pack registry truncated: have {} bytes, need {expected}",
                    data.len()
                ),
            ));
        }

        let mut pack_ids = Vec::with_capacity(count);
        for i in 0..count {
            let offset = HEADER_SIZE + i * UUID_SIZE;
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&data[offset..offset + UUID_SIZE]);
            pack_ids.push(Uuid::from_bytes(bytes));
        }

        Ok(Self { pack_ids })
    }

    pub fn append(&mut self, new_ids: &[Uuid]) {
        self.pack_ids.extend_from_slice(new_ids);
    }

    pub fn compact(&mut self, to_remove: &HashSet<Uuid>) {
        self.pack_ids.retain(|id| !to_remove.contains(id));
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(dead_code)]
    pub fn pack_id_set(&self) -> HashSet<Uuid> {
        self.pack_ids.iter().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.pack_ids.is_empty()
    }
}

/// Generate S3 key for a pack registry: "pack-registries/{name}"
pub fn registry_s3_key(name: &str) -> String {
    format!("pack-registries/{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_registry_round_trip() {
        let ids: Vec<Uuid> = (0..100).map(|_| Uuid::new_v4()).collect();
        let reg = PackRegistry {
            pack_ids: ids.clone(),
        };
        let data = reg.serialize();
        let restored = PackRegistry::deserialize(&data).unwrap();
        assert_eq!(restored.pack_ids, ids);
    }

    #[test]
    fn test_registry_empty() {
        let reg = PackRegistry::new();
        let data = reg.serialize();
        assert_eq!(data.len(), HEADER_SIZE);

        let restored = PackRegistry::deserialize(&data).unwrap();
        assert!(restored.pack_ids.is_empty());
    }

    #[test]
    fn test_registry_large() {
        let ids: Vec<Uuid> = (0..10_000).map(|_| Uuid::new_v4()).collect();
        let reg = PackRegistry {
            pack_ids: ids.clone(),
        };

        let start = Instant::now();
        let data = reg.serialize();
        let restored = PackRegistry::deserialize(&data).unwrap();
        let elapsed = start.elapsed();

        assert_eq!(restored.pack_ids.len(), 10_000);
        assert_eq!(restored.pack_ids, ids);
        assert_eq!(data.len(), HEADER_SIZE + 10_000 * UUID_SIZE);
        assert!(
            elapsed.as_millis() < 10,
            "round-trip took {}ms, expected < 10ms",
            elapsed.as_millis()
        );
    }

    #[test]
    fn test_registry_append() {
        let mut reg = PackRegistry {
            pack_ids: (0..50).map(|_| Uuid::new_v4()).collect(),
        };
        let new_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        reg.append(&new_ids);

        assert_eq!(reg.pack_ids.len(), 55);
        assert_eq!(&reg.pack_ids[50..], &new_ids[..]);

        // Round-trip the appended registry
        let data = reg.serialize();
        let restored = PackRegistry::deserialize(&data).unwrap();
        assert_eq!(restored.pack_ids.len(), 55);
    }

    #[test]
    fn test_registry_invalid_magic() {
        let mut data = vec![0u8; HEADER_SIZE];
        data[0..4].copy_from_slice(b"NOPE");
        let result = PackRegistry::deserialize(&data);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid pack registry magic")
        );
    }

    #[test]
    fn test_registry_compact() {
        let ids: Vec<Uuid> = (0..100).map(|_| Uuid::new_v4()).collect();
        let to_remove: HashSet<Uuid> = ids[..30].iter().copied().collect();
        let expected_remaining: Vec<Uuid> = ids[30..].to_vec();

        let mut reg = PackRegistry {
            pack_ids: ids,
        };
        reg.compact(&to_remove);

        assert_eq!(reg.pack_ids.len(), 70);
        assert_eq!(reg.pack_ids, expected_remaining);
    }

    #[test]
    fn test_registry_pack_id_set() {
        let ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
        // Include a duplicate
        let mut with_dups = ids.clone();
        with_dups.push(ids[0]);

        let reg = PackRegistry {
            pack_ids: with_dups,
        };
        let set = reg.pack_id_set();
        assert_eq!(set.len(), 10);
        for id in &ids {
            assert!(set.contains(id));
        }
    }

    #[test]
    fn test_registry_truncated() {
        let reg = PackRegistry {
            pack_ids: vec![Uuid::new_v4(); 5],
        };
        let mut data = reg.serialize();
        data.truncate(HEADER_SIZE + 3 * UUID_SIZE); // only 3 of 5 UUIDs
        let result = PackRegistry::deserialize(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated"));
    }

    #[test]
    fn test_registry_s3_key() {
        assert_eq!(registry_s3_key("my-vm"), "pack-registries/my-vm");
    }
}
