//! Two-tier pack index cache backed by foyer HybridCache.
//!
//! V4 replacement for ChunkMetaCache (v3). Keyed by PackId (u64) instead of
//! chunk content hash. Each entry stores the serialized GLPK v3 index for one
//! pack, enabling block-level resolution without round-tripping to S3.
//!
//! Tiers:
//!   Memory (S3-FIFO eviction, byte-sized) → ~100ns lookup
//!   SSD (foyer disk engine, direct I/O) → ~100µs
//!   Miss → caller fetches pack header+index from S3

use std::collections::HashSet;
use std::path::Path;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, EvictionConfig, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, RecoverMode, S3FifoConfig,
};

use super::block_map::Blake3Hash;
use super::pack::{PackId, PackIndexEntry, PACK_INDEX_ENTRY_SIZE};

/// Default memory tier: 64MB (holds many pack indices — each ~1-28 KB).
const DEFAULT_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Default SSD tier: 512MB (holds thousands of pack indices).
const DEFAULT_SSD_BYTES: usize = 512 * 1024 * 1024;

/// Two-tier cache for v4 pack indices, backed by foyer HybridCache.
pub struct PackIndexCache {
    inner: HybridCache<PackId, Bytes>,
}

impl PackIndexCache {
    /// Open the pack index cache with default tier sizes (64MB memory, 512MB SSD).
    ///
    /// Creates `{cache_dir}/foyer_pack_index/` for the SSD tier.
    pub async fn open(cache_dir: &Path) -> anyhow::Result<Self> {
        Self::open_with_sizes(cache_dir, DEFAULT_MEMORY_BYTES, DEFAULT_SSD_BYTES).await
    }

    /// Open with custom tier sizes.
    pub async fn open_with_sizes(
        cache_dir: &Path,
        memory_bytes: usize,
        ssd_bytes: usize,
    ) -> anyhow::Result<Self> {
        let dir = cache_dir.join("foyer_pack_index");
        std::fs::create_dir_all(&dir)?;

        let inner: HybridCache<PackId, Bytes> = HybridCacheBuilder::new()
            .with_name("glidefs-pack-index-cache")
            .memory(memory_bytes)
            .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
            .with_weighter(|_key: &PackId, value: &Bytes| value.len())
            .storage()
            .with_engine_config(BlockEngineConfig::new(
                FsDeviceBuilder::new(&dir)
                    .with_capacity(ssd_bytes)
                    .build()?,
            ))
            .with_recover_mode(RecoverMode::Quiet)
            .build()
            .await?;

        Ok(Self { inner })
    }

    /// Get all index entries for a pack.
    ///
    /// Returns deserialized entries on hit, None on miss.
    pub async fn get_entries(&self, pack_id: PackId) -> Option<Vec<PackIndexEntry>> {
        match self.inner.get(&pack_id).await {
            Ok(Some(entry)) => deserialize_entries(entry.value()),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(pack_id, error = %e, "pack index cache error, treating as miss");
                None
            }
        }
    }

    /// Insert pack index entries into the cache (fire-and-forget).
    ///
    /// Serializes the entries to compact binary and inserts into foyer.
    /// Foyer handles memory → SSD eviction asynchronously.
    pub fn insert_entries(&self, pack_id: PackId, entries: &[PackIndexEntry]) {
        self.inner
            .insert(pack_id, Bytes::from(serialize_entries(entries)));
    }

    /// Look up a single block by chunk_offset within a cached pack index.
    ///
    /// Returns `(hash, pack_offset, comp_length)` on hit.
    pub async fn lookup_block(
        &self,
        pack_id: PackId,
        chunk_offset: u32,
    ) -> Option<(Blake3Hash, u32, u32)> {
        let entries = self.get_entries(pack_id).await?;
        // Entries are sorted by chunk_offset (assemble_pack_v2 sorts them).
        // Binary search for the target offset.
        entries
            .binary_search_by_key(&chunk_offset, |e| e.chunk_offset)
            .ok()
            .map(|idx| {
                let e = &entries[idx];
                (e.hash, e.offset, e.comp_length)
            })
    }

    /// Collect all block hashes across multiple packs (for flush dedup).
    ///
    /// Used by the v4 flush path to build the known_hashes set from
    /// existing pack indices in a chunk.
    pub async fn known_hashes(&self, pack_ids: &[PackId]) -> HashSet<Blake3Hash> {
        let mut hashes = HashSet::new();
        for &pack_id in pack_ids {
            if let Some(entries) = self.get_entries(pack_id).await {
                for entry in &entries {
                    hashes.insert(entry.hash);
                }
            }
        }
        hashes
    }
}

/// Serialize pack index entries to compact binary (28 bytes per entry).
fn serialize_entries(entries: &[PackIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * PACK_INDEX_ENTRY_SIZE);
    for entry in entries {
        buf.extend_from_slice(&entry.hash.0);
        buf.extend_from_slice(&entry.chunk_offset.to_le_bytes());
        buf.extend_from_slice(&entry.offset.to_le_bytes());
        buf.extend_from_slice(&entry.comp_length.to_le_bytes());
    }
    buf
}

/// Deserialize pack index entries from compact binary.
fn deserialize_entries(data: &[u8]) -> Option<Vec<PackIndexEntry>> {
    if !data.len().is_multiple_of(PACK_INDEX_ENTRY_SIZE) {
        return None;
    }
    let count = data.len() / PACK_INDEX_ENTRY_SIZE;
    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let base = i * PACK_INDEX_ENTRY_SIZE;
        let mut hash = [0u8; 16];
        hash.copy_from_slice(&data[base..base + 16]);
        let chunk_offset = u32::from_le_bytes(data[base + 16..base + 20].try_into().ok()?);
        let offset = u32::from_le_bytes(data[base + 20..base + 24].try_into().ok()?);
        let comp_length = u32::from_le_bytes(data[base + 24..base + 28].try_into().ok()?);
        entries.push(PackIndexEntry {
            hash: Blake3Hash(hash),
            chunk_offset,
            offset,
            comp_length,
        });
    }
    Some(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::pack::new_pack_id;
    use tempfile::TempDir;

    /// Small cache sizes for tests (foyer needs real directories).
    async fn open_test_cache(dir: &Path) -> PackIndexCache {
        PackIndexCache::open_with_sizes(dir, 16 * 1024 * 1024, 64 * 1024 * 1024)
            .await
            .expect("failed to open test cache")
    }

    fn make_entry(chunk_offset: u32) -> PackIndexEntry {
        PackIndexEntry {
            hash: Blake3Hash([chunk_offset as u8; 16]),
            chunk_offset,
            offset: chunk_offset * 1000,
            comp_length: 65536,
        }
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let tmp = TempDir::new().unwrap();
        let cache = open_test_cache(tmp.path()).await;

        let pack_id = new_pack_id();
        let entries = vec![make_entry(0), make_entry(5), make_entry(10)];
        cache.insert_entries(pack_id, &entries);

        let got = cache.get_entries(pack_id).await.expect("should find in cache");
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].chunk_offset, 0);
        assert_eq!(got[1].chunk_offset, 5);
        assert_eq!(got[2].chunk_offset, 10);
    }

    #[tokio::test]
    async fn test_miss_returns_none() {
        let tmp = TempDir::new().unwrap();
        let cache = open_test_cache(tmp.path()).await;
        assert!(cache.get_entries(0xDEADBEEF).await.is_none());
    }

    #[tokio::test]
    async fn test_lookup_block() {
        let tmp = TempDir::new().unwrap();
        let cache = open_test_cache(tmp.path()).await;

        let pack_id = new_pack_id();
        let entries = vec![make_entry(10), make_entry(20), make_entry(30)];
        cache.insert_entries(pack_id, &entries);

        let (hash, offset, comp_len) = cache
            .lookup_block(pack_id, 20)
            .await
            .expect("should find block");
        assert_eq!(hash, Blake3Hash([20; 16]));
        assert_eq!(offset, 20000);
        assert_eq!(comp_len, 65536);

        // Miss: block not in pack
        assert!(cache.lookup_block(pack_id, 99).await.is_none());
    }

    #[tokio::test]
    async fn test_known_hashes() {
        let tmp = TempDir::new().unwrap();
        let cache = open_test_cache(tmp.path()).await;

        let pack1 = new_pack_id();
        let pack2 = new_pack_id();
        cache.insert_entries(pack1, &[make_entry(0), make_entry(1)]);
        cache.insert_entries(pack2, &[make_entry(2), make_entry(3)]);

        let hashes = cache.known_hashes(&[pack1, pack2]).await;
        assert_eq!(hashes.len(), 4);
        assert!(hashes.contains(&Blake3Hash([0; 16])));
        assert!(hashes.contains(&Blake3Hash([1; 16])));
        assert!(hashes.contains(&Blake3Hash([2; 16])));
        assert!(hashes.contains(&Blake3Hash([3; 16])));
    }

    #[tokio::test]
    async fn test_known_hashes_missing_pack() {
        let tmp = TempDir::new().unwrap();
        let cache = open_test_cache(tmp.path()).await;

        let pack1 = new_pack_id();
        cache.insert_entries(pack1, &[make_entry(0)]);

        // One real pack + one missing pack — should not error.
        let hashes = cache.known_hashes(&[pack1, 0xDEADBEEF]).await;
        assert_eq!(hashes.len(), 1);
    }

    #[tokio::test]
    async fn test_ssd_persistence() {
        let tmp = TempDir::new().unwrap();

        let pack_id = new_pack_id();
        let entries = vec![make_entry(42)];

        // Insert into first cache instance, then drop it
        {
            let cache = open_test_cache(tmp.path()).await;
            cache.insert_entries(pack_id, &entries);
            cache.inner.close().await.unwrap();
        }

        // Open a new cache instance (memory is fresh, SSD should have the data)
        let cache2 = open_test_cache(tmp.path()).await;
        let got = cache2
            .get_entries(pack_id)
            .await
            .expect("should find on SSD");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].chunk_offset, 42);
    }

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let entries = vec![make_entry(0), make_entry(100), make_entry(1023)];
        let bytes = serialize_entries(&entries);
        assert_eq!(bytes.len(), 3 * PACK_INDEX_ENTRY_SIZE);

        let decoded = deserialize_entries(&bytes).expect("should deserialize");
        assert_eq!(decoded.len(), 3);
        for (a, b) in entries.iter().zip(decoded.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.chunk_offset, b.chunk_offset);
            assert_eq!(a.offset, b.offset);
            assert_eq!(a.comp_length, b.comp_length);
        }
    }

    #[test]
    fn test_deserialize_rejects_bad_length() {
        // Not a multiple of 28
        let bad = vec![0u8; 29];
        assert!(deserialize_entries(&bad).is_none());
    }
}
