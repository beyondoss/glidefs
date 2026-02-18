//! Host-level pack index for cross-VM block deduplication.
//!
//! Maps block content hashes to their S3 pack locations. Shared across all
//! exports on the host. When flushing, blocks whose hash already exists in
//! the index are skipped (already in S3).

use dashmap::DashMap;

use super::block_map::{Blake3Hash, BlockMap};
use super::manifest::{Manifest, ManifestPackEntry};
use super::pack::PackLocation;

pub struct HostPackIndex {
    index: DashMap<Blake3Hash, PackLocation>,
}

impl Default for HostPackIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl HostPackIndex {
    pub fn new() -> Self {
        Self {
            index: DashMap::new(),
        }
    }

    /// Insert a block's pack location.
    pub fn insert(&self, hash: Blake3Hash, location: PackLocation) {
        self.index.insert(hash, location);
    }

    /// Look up a block's pack location by hash.
    pub fn get(&self, hash: &Blake3Hash) -> Option<PackLocation> {
        self.index.get(hash).map(|r| *r)
    }

    /// Check if a block hash exists in the index.
    pub fn contains(&self, hash: &Blake3Hash) -> bool {
        self.index.contains_key(hash)
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Return all hashes in the index. Used by the background scrubber
    /// to iterate over known blocks for integrity verification.
    pub fn all_hashes(&self) -> Vec<Blake3Hash> {
        self.index.iter().map(|r| *r.key()).collect()
    }

    /// Rebuild the index from a set of manifests.
    /// Clears existing entries and inserts all pack index entries from the manifests.
    /// Called on VM arrive/depart to keep the index current.
    pub fn rebuild(&self, manifests: &[Manifest]) {
        self.index.clear();
        for manifest in manifests {
            for entry in &manifest.pack_index {
                self.index.insert(
                    entry.hash,
                    PackLocation {
                        pack_id: entry.pack_id,
                        offset: entry.offset,
                        comp_length: entry.comp_length,
                    },
                );
            }
        }
    }

    /// Derive pack index entries for a specific VM's block map.
    /// Returns ManifestPackEntry for each non-empty hash in the block map
    /// that exists in this host index.
    pub fn derive_for_block_map(&self, block_map: &BlockMap) -> Vec<ManifestPackEntry> {
        let mut result = Vec::new();
        for (_chunk_index, entry) in block_map.iter_non_empty() {
            if entry.hash.is_zero() {
                continue;
            }
            if let Some(location) = self.index.get(&entry.hash) {
                result.push(ManifestPackEntry {
                    hash: entry.hash,
                    pack_id: location.pack_id,
                    offset: location.offset,
                    comp_length: location.comp_length,
                });
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nbd::block_map::{blake3_128, BlockMapEntry};
    use uuid::Uuid;

    fn make_hash(data: &[u8]) -> Blake3Hash {
        blake3_128(data)
    }

    fn make_location(offset: u32) -> PackLocation {
        PackLocation {
            pack_id: Uuid::new_v4(),
            offset,
            comp_length: 4096,
        }
    }

    #[test]
    fn test_pack_index_insert_and_lookup() {
        let index = HostPackIndex::new();
        let hash = make_hash(b"block-content-1");
        let pack_id = Uuid::new_v4();
        let location = PackLocation {
            pack_id,
            offset: 100,
            comp_length: 2048,
        };

        index.insert(hash, location);

        let got = index.get(&hash).expect("should find inserted hash");
        assert_eq!(got.pack_id, pack_id);
        assert_eq!(got.offset, 100);
        assert_eq!(got.comp_length, 2048);
    }

    #[test]
    fn test_pack_index_dedup_check() {
        let index = HostPackIndex::new();
        let hash_a = make_hash(b"block-a");
        let hash_b = make_hash(b"block-b");

        index.insert(hash_a, make_location(0));

        assert!(index.contains(&hash_a));
        assert!(!index.contains(&hash_b));
    }

    #[tokio::test]
    async fn test_pack_index_concurrent_access() {
        use std::sync::Arc;

        let index = Arc::new(HostPackIndex::new());
        let mut handles = Vec::new();

        for task_id in 0..10u32 {
            let idx = Arc::clone(&index);
            handles.push(tokio::spawn(async move {
                for i in 0..1000u32 {
                    let data = format!("task-{task_id}-block-{i}");
                    let hash = make_hash(data.as_bytes());
                    let location = PackLocation {
                        pack_id: Uuid::new_v4(),
                        offset: i,
                        comp_length: 128,
                    };
                    idx.insert(hash, location);
                }
            }));
        }

        for handle in handles {
            handle.await.expect("task should not panic");
        }

        assert_eq!(index.len(), 10_000);
    }

    #[test]
    fn test_pack_index_rebuild() {
        let index = HostPackIndex::new();

        // Pre-populate with something that should be cleared.
        let stale_hash = make_hash(b"stale");
        index.insert(stale_hash, make_location(0));

        let shared_hash = make_hash(b"shared-block");
        let unique_hash_1 = make_hash(b"unique-1");
        let unique_hash_2 = make_hash(b"unique-2");
        let unique_hash_3 = make_hash(b"unique-3");

        let pack_id_1 = Uuid::new_v4();
        let pack_id_2 = Uuid::new_v4();
        let pack_id_3 = Uuid::new_v4();

        let manifests = vec![
            Manifest {
                name: "vm-1".to_string(),
                sequence: 1,
                chunk_size: 131072,
                device_size: 1024 * 1024,
                block_map: vec![],
                pack_index: vec![
                    ManifestPackEntry {
                        hash: shared_hash,
                        pack_id: pack_id_1,
                        offset: 0,
                        comp_length: 100,
                    },
                    ManifestPackEntry {
                        hash: unique_hash_1,
                        pack_id: pack_id_1,
                        offset: 100,
                        comp_length: 200,
                    },
                ],
            },
            Manifest {
                name: "vm-2".to_string(),
                sequence: 2,
                chunk_size: 131072,
                device_size: 1024 * 1024,
                block_map: vec![],
                pack_index: vec![
                    ManifestPackEntry {
                        hash: shared_hash,
                        pack_id: pack_id_2,
                        offset: 0,
                        comp_length: 150,
                    },
                    ManifestPackEntry {
                        hash: unique_hash_2,
                        pack_id: pack_id_2,
                        offset: 150,
                        comp_length: 250,
                    },
                ],
            },
            Manifest {
                name: "vm-3".to_string(),
                sequence: 3,
                chunk_size: 131072,
                device_size: 1024 * 1024,
                block_map: vec![],
                pack_index: vec![ManifestPackEntry {
                    hash: unique_hash_3,
                    pack_id: pack_id_3,
                    offset: 0,
                    comp_length: 300,
                }],
            },
        ];

        index.rebuild(&manifests);

        // Stale entry should be gone.
        assert!(!index.contains(&stale_hash));

        // 4 unique hashes: shared_hash, unique_hash_1, unique_hash_2, unique_hash_3.
        assert_eq!(index.len(), 4);

        // All unique hashes present.
        assert!(index.contains(&shared_hash));
        assert!(index.contains(&unique_hash_1));
        assert!(index.contains(&unique_hash_2));
        assert!(index.contains(&unique_hash_3));

        // shared_hash should have last-writer-wins (manifest 2's value).
        let shared_loc = index.get(&shared_hash).unwrap();
        assert_eq!(shared_loc.pack_id, pack_id_2);
        assert_eq!(shared_loc.comp_length, 150);
    }

    #[test]
    fn test_pack_index_derive_for_block_map() {
        let index = HostPackIndex::new();

        // Create 5 distinct hashes.
        let hashes: Vec<Blake3Hash> = (0..5)
            .map(|i| make_hash(format!("derive-block-{i}").as_bytes()))
            .collect();

        let pack_id = Uuid::new_v4();

        // Insert only 3 of the 5 hashes into the pack index.
        for (i, &hash) in hashes.iter().enumerate().take(3) {
            index.insert(
                hash,
                PackLocation {
                    pack_id,
                    offset: (i * 1000) as u32,
                    comp_length: 512,
                },
            );
        }

        // Build a block map with all 5 hashes.
        // 5 * 128KB = 640KB device, 128KB chunks.
        let mut block_map = BlockMap::new(5 * 131072, 131072);
        for (i, &hash) in hashes.iter().enumerate() {
            block_map.set(
                i,
                BlockMapEntry {
                    hash,
                    flags: 0,
                    sequence: i as u64 + 1,
                },
            );
        }

        let derived = index.derive_for_block_map(&block_map);

        assert_eq!(
            derived.len(),
            3,
            "should return exactly 3 entries for the 3 indexed hashes"
        );

        // Verify the derived entries reference the correct pack locations.
        for entry in &derived {
            assert_eq!(entry.pack_id, pack_id);
            assert_eq!(entry.comp_length, 512);
            assert!(index.contains(&entry.hash));
        }

        // Verify the 2 unindexed hashes are NOT in the result.
        let derived_hashes: Vec<Blake3Hash> = derived.iter().map(|e| e.hash).collect();
        assert!(!derived_hashes.contains(&hashes[3]));
        assert!(!derived_hashes.contains(&hashes[4]));
    }
}
