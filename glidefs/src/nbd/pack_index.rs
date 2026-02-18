//! Host-level pack index for cross-VM block deduplication.
//!
//! Maps block content hashes to their S3 pack locations. Shared across all
//! exports on the host. When flushing, blocks whose hash already exists in
//! the index are skipped (already in S3).
//!
//! Backed by redb — an embedded key-value store on local SSD. Since every
//! `get()` is followed by a 5-50ms S3 fetch, microsecond lookup latency is
//! fine. The index is derived data (rebuildable from manifests), so we use
//! `Durability::None` to skip fsync.

use std::collections::HashSet;
use std::path::Path;

use redb::{
    CommitError, Database, Durability, ReadableTable, ReadableTableMetadata, StorageError,
    TableDefinition, TableError, TransactionError,
};
use thiserror::Error;

use super::block_map::{Blake3Hash, BlockMap};
use super::manifest::{Manifest, ManifestPackEntry};
use super::pack::PackLocation;

/// Errors from the host pack index (redb-backed).
///
/// The pack index is derived data — rebuildable from manifests — so these
/// errors indicate local storage problems, not data loss.
#[derive(Error, Debug)]
pub enum PackIndexError {
    #[error("redb transaction: {0}")]
    Transaction(Box<TransactionError>),

    #[error("redb table: {0}")]
    Table(#[from] TableError),

    #[error("redb storage: {0}")]
    Storage(#[from] StorageError),

    #[error("redb commit: {0}")]
    Commit(#[from] CommitError),
}

impl From<TransactionError> for PackIndexError {
    fn from(e: TransactionError) -> Self {
        Self::Transaction(Box::new(e))
    }
}

const PACK_TABLE: TableDefinition<[u8; 16], [u8; 24]> = TableDefinition::new("packs");

pub struct HostPackIndex {
    db: Database,
}

fn pack_location_to_bytes(loc: &PackLocation) -> [u8; 24] {
    let mut buf = [0u8; 24];
    buf[..16].copy_from_slice(loc.pack_id.as_bytes());
    buf[16..20].copy_from_slice(&loc.offset.to_le_bytes());
    buf[20..24].copy_from_slice(&loc.comp_length.to_le_bytes());
    buf
}

fn pack_location_from_bytes(bytes: [u8; 24]) -> PackLocation {
    let pack_id = uuid::Uuid::from_bytes(bytes[..16].try_into().unwrap());
    let offset = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    let comp_length = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    PackLocation {
        pack_id,
        offset,
        comp_length,
    }
}

impl HostPackIndex {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Database::create(path)?;
        // Create table eagerly so reads don't need to handle "table not found"
        let mut txn = db.begin_write()?;
        {
            let _ = txn.open_table(PACK_TABLE)?;
        }
        txn.set_durability(Durability::None);
        txn.commit()?;
        Ok(Self { db })
    }

    /// Insert a block's pack location.
    #[cfg(test)]
    pub fn insert(&self, hash: Blake3Hash, location: PackLocation) {
        let val = pack_location_to_bytes(&location);
        let mut txn = self.db.begin_write().expect("redb write failed");
        {
            let mut table = txn.open_table(PACK_TABLE).expect("redb open table failed");
            table
                .insert(*hash.as_bytes(), val)
                .expect("redb insert failed");
        }
        txn.set_durability(Durability::None);
        txn.commit().expect("redb commit failed");
    }

    /// Insert a batch of entries in a single transaction.
    pub fn insert_batch(&self, entries: &[(Blake3Hash, PackLocation)]) -> Result<(), PackIndexError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PACK_TABLE)?;
            for (hash, location) in entries {
                let val = pack_location_to_bytes(location);
                table.insert(*hash.as_bytes(), val)?;
            }
        }
        txn.set_durability(Durability::None);
        txn.commit()?;
        Ok(())
    }

    /// Look up a block's pack location by hash.
    pub fn get(&self, hash: &Blake3Hash) -> Result<Option<PackLocation>, PackIndexError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PACK_TABLE)?;
        Ok(table
            .get(*hash.as_bytes())?
            .map(|v| pack_location_from_bytes(v.value())))
    }

    /// Check if a block hash exists in the index.
    pub fn contains(&self, hash: &Blake3Hash) -> Result<bool, PackIndexError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PACK_TABLE)?;
        Ok(table.get(*hash.as_bytes())?.is_some())
    }

    /// Number of entries in the index.
    pub fn len(&self) -> Result<usize, PackIndexError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PACK_TABLE)?;
        Ok(table.len()? as usize)
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> Result<bool, PackIndexError> {
        Ok(self.len()? == 0)
    }

    /// Return all hashes in the index. Used by the background scrubber
    /// to iterate over known blocks for integrity verification.
    pub fn all_hashes(&self) -> Result<Vec<Blake3Hash>, PackIndexError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PACK_TABLE)?;
        let mut hashes = Vec::new();
        for r in table.iter()? {
            let (k, _) = r?;
            hashes.push(Blake3Hash::from_bytes(k.value()));
        }
        Ok(hashes)
    }

    /// Rebuild the index from a set of manifests.
    /// Clears existing entries and inserts all pack index entries from the manifests.
    /// Called on VM arrive/depart to keep the index current.
    pub fn rebuild(&self, manifests: &[Manifest]) -> Result<(), PackIndexError> {
        let mut txn = self.db.begin_write()?;
        {
            // Delete and recreate the table to clear it
            txn.delete_table(PACK_TABLE).ok();
            let mut table = txn.open_table(PACK_TABLE)?;
            // Insert all manifest entries
            for manifest in manifests {
                for entry in &manifest.pack_index {
                    let loc = PackLocation {
                        pack_id: entry.pack_id,
                        offset: entry.offset,
                        comp_length: entry.comp_length,
                    };
                    let val = pack_location_to_bytes(&loc);
                    table.insert(*entry.hash.as_bytes(), val)?;
                }
            }
        }
        txn.set_durability(Durability::None);
        txn.commit()?;
        Ok(())
    }

    /// Remove entries not referenced by any active export.
    ///
    /// Retains only entries whose hash appears in `referenced`. Called after
    /// export removal to reclaim space from departed VMs.
    ///
    /// **Caller contract**: The `referenced` set must come from manifest-based
    /// hashes (via `rebuild_manifest_hashes` + `referenced_hashes`), not from
    /// a raw block_map scan. The block_map has ZERO placeholders for in-flight
    /// flushes, which would cause freshly-uploaded entries to be pruned.
    ///
    /// `prune_pack_index` in the router forces a manifest-hash rebuild on all
    /// remaining exports immediately before calling this, which captures
    /// everything flushed up to that moment.
    ///
    /// Returns the number of entries removed.
    pub fn prune_unreferenced(&self, referenced: &HashSet<Blake3Hash>) -> Result<usize, PackIndexError> {
        let mut txn = self.db.begin_write()?;
        let removed;
        {
            let mut table = txn.open_table(PACK_TABLE)?;
            let mut to_remove: Vec<[u8; 16]> = Vec::new();
            for r in table.iter()? {
                let (k, _) = r?;
                let hash = Blake3Hash::from_bytes(k.value());
                if !referenced.contains(&hash) {
                    to_remove.push(k.value());
                }
            }
            removed = to_remove.len();
            for key in &to_remove {
                table.remove(key)?;
            }
        }
        txn.set_durability(Durability::None);
        txn.commit()?;
        Ok(removed)
    }

    /// Derive pack index entries for a specific VM's block map.
    /// Returns ManifestPackEntry for each non-empty hash in the block map
    /// that exists in this host index.
    pub fn derive_for_block_map(&self, block_map: &BlockMap) -> Result<Vec<ManifestPackEntry>, PackIndexError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PACK_TABLE)?;
        let mut result = Vec::new();
        for (_chunk_index, entry) in block_map.iter_non_empty() {
            if entry.hash.is_zero() {
                continue;
            }
            if let Some(guard) = table.get(*entry.hash.as_bytes())? {
                let location = pack_location_from_bytes(guard.value());
                result.push(ManifestPackEntry {
                    hash: entry.hash,
                    pack_id: location.pack_id,
                    offset: location.offset,
                    comp_length: location.comp_length,
                });
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nbd::block_map::{blake3_128, BlockMapEntry};
    use tempfile::TempDir;
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

    fn open_test_index() -> (TempDir, HostPackIndex) {
        let dir = TempDir::new().unwrap();
        let index = HostPackIndex::open(dir.path().join("pack_index.redb")).unwrap();
        (dir, index)
    }

    #[test]
    fn test_pack_index_insert_and_lookup() {
        let (_dir, index) = open_test_index();
        let hash = make_hash(b"block-content-1");
        let pack_id = Uuid::new_v4();
        let location = PackLocation {
            pack_id,
            offset: 100,
            comp_length: 2048,
        };

        index.insert(hash, location);

        let got = index.get(&hash).unwrap().expect("should find inserted hash");
        assert_eq!(got.pack_id, pack_id);
        assert_eq!(got.offset, 100);
        assert_eq!(got.comp_length, 2048);
    }

    #[test]
    fn test_pack_index_dedup_check() {
        let (_dir, index) = open_test_index();
        let hash_a = make_hash(b"block-a");
        let hash_b = make_hash(b"block-b");

        index.insert(hash_a, make_location(0));

        assert!(index.contains(&hash_a).unwrap());
        assert!(!index.contains(&hash_b).unwrap());
    }

    #[tokio::test]
    async fn test_pack_index_concurrent_access() {
        use std::sync::Arc;

        let (_dir, index) = open_test_index();
        let index = Arc::new(index);
        let mut handles = Vec::new();

        for task_id in 0..10u32 {
            let idx = Arc::clone(&index);
            handles.push(tokio::spawn(async move {
                for i in 0..100u32 {
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

        assert_eq!(index.len().unwrap(), 1_000);
    }

    #[test]
    fn test_pack_index_insert_batch() {
        let (_dir, index) = open_test_index();
        let pack_id = Uuid::new_v4();

        let entries: Vec<_> = (0..100u32)
            .map(|i| {
                let hash = make_hash(format!("batch-{i}").as_bytes());
                let loc = PackLocation {
                    pack_id,
                    offset: i * 1000,
                    comp_length: 512,
                };
                (hash, loc)
            })
            .collect();

        index.insert_batch(&entries).unwrap();
        assert_eq!(index.len().unwrap(), 100);

        // Verify a sample entry
        let (hash, loc) = &entries[50];
        let got = index.get(hash).unwrap().unwrap();
        assert_eq!(got.pack_id, loc.pack_id);
        assert_eq!(got.offset, loc.offset);
    }

    #[test]
    fn test_pack_index_rebuild() {
        let (_dir, index) = open_test_index();

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

        index.rebuild(&manifests).unwrap();

        // Stale entry should be gone.
        assert!(!index.contains(&stale_hash).unwrap());

        // 4 unique hashes: shared_hash, unique_hash_1, unique_hash_2, unique_hash_3.
        assert_eq!(index.len().unwrap(), 4);

        // All unique hashes present.
        assert!(index.contains(&shared_hash).unwrap());
        assert!(index.contains(&unique_hash_1).unwrap());
        assert!(index.contains(&unique_hash_2).unwrap());
        assert!(index.contains(&unique_hash_3).unwrap());

        // shared_hash should have last-writer-wins (manifest 2's value).
        let shared_loc = index.get(&shared_hash).unwrap().unwrap();
        assert_eq!(shared_loc.pack_id, pack_id_2);
        assert_eq!(shared_loc.comp_length, 150);
    }

    #[test]
    fn test_pack_index_derive_for_block_map() {
        let (_dir, index) = open_test_index();

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

        let derived = index.derive_for_block_map(&block_map).unwrap();

        assert_eq!(
            derived.len(),
            3,
            "should return exactly 3 entries for the 3 indexed hashes"
        );

        // Verify the derived entries reference the correct pack locations.
        for entry in &derived {
            assert_eq!(entry.pack_id, pack_id);
            assert_eq!(entry.comp_length, 512);
            assert!(index.contains(&entry.hash).unwrap());
        }

        // Verify the 2 unindexed hashes are NOT in the result.
        let derived_hashes: Vec<Blake3Hash> = derived.iter().map(|e| e.hash).collect();
        assert!(!derived_hashes.contains(&hashes[3]));
        assert!(!derived_hashes.contains(&hashes[4]));
    }

    #[test]
    fn test_prune_unreferenced_removes_stale_entries() {
        let (_dir, index) = open_test_index();
        let active = make_hash(b"active");
        let stale = make_hash(b"stale");

        index.insert(active, make_location(0));
        index.insert(stale, make_location(100));
        assert_eq!(index.len().unwrap(), 2);

        let referenced: HashSet<Blake3Hash> = [active].into_iter().collect();
        let removed = index.prune_unreferenced(&referenced).unwrap();

        assert_eq!(removed, 1);
        assert!(index.contains(&active).unwrap());
        assert!(!index.contains(&stale).unwrap());
    }

    #[test]
    fn test_prune_with_empty_referenced_clears_all() {
        let (_dir, index) = open_test_index();
        index.insert(make_hash(b"a"), make_location(0));
        index.insert(make_hash(b"b"), make_location(1));

        let removed = index.prune_unreferenced(&HashSet::new()).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(index.len().unwrap(), 0);
    }

    #[test]
    fn test_prune_with_all_referenced_removes_nothing() {
        let (_dir, index) = open_test_index();
        let h1 = make_hash(b"one");
        let h2 = make_hash(b"two");
        index.insert(h1, make_location(0));
        index.insert(h2, make_location(1));

        let referenced: HashSet<Blake3Hash> = [h1, h2].into_iter().collect();
        let removed = index.prune_unreferenced(&referenced).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(index.len().unwrap(), 2);
    }
}
