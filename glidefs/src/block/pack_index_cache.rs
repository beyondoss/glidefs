//! Two-tier pack index cache backed by foyer HybridCache.
//!
//! Keyed by PackId (u64). Each entry stores the block index for one pack,
//! enabling block-level resolution without round-tripping to S3.
//!
//! Memory tier holds parsed `Vec<PackIndexEntry>` directly (~100ns lookup).
//! SSD tier stores extent-encoded + zstd-compressed entries (~100µs lookup).
//! Miss → caller fetches pack index from S3.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use foyer::{
    BlockEngineConfig, Code, DeviceBuilder, EvictionConfig, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, RecoverMode, S3FifoConfig,
};

use super::block_map::Blake3Hash;
use super::pack::{PackId, PackIndexEntry, PACK_HEADER_SIZE};

/// Default memory tier: 64MB.
const DEFAULT_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Default SSD tier: 2GB in production, 128MB in test builds.
/// 2GB holds ~117M entries = 7.5TB at 64KB blocks.
/// Tests use tiny volumes; 128MB avoids FD exhaustion in stress tests
/// that create/destroy many cache instances.
#[cfg(not(feature = "test-utils"))]
const DEFAULT_SSD_BYTES: usize = 2 * 1024 * 1024 * 1024;
#[cfg(feature = "test-utils")]
const DEFAULT_SSD_BYTES: usize = 128 * 1024 * 1024;

/// Cached pack index — wraps sorted entries for a single pack.
///
/// In the memory tier, this holds the parsed entries directly (zero-cost access).
/// On SSD, entries are extent-encoded + zstd-compressed via the `Code` trait.
#[derive(Debug, Clone)]
struct CachedPackIndex(Arc<[PackIndexEntry]>);

impl Code for CachedPackIndex {
    fn encode(&self, writer: &mut impl std::io::Write) -> foyer::Result<()> {
        let raw = extent_encode(&self.0);
        let compressed =
            zstd::bulk::compress(&raw, 1).map_err(foyer::Error::io_error)?;
        writer
            .write_all(&compressed)
            .map_err(foyer::Error::io_error)
    }

    fn decode(reader: &mut impl std::io::Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let mut compressed = Vec::new();
        reader
            .read_to_end(&mut compressed)
            .map_err(foyer::Error::io_error)?;
        let raw = zstd::bulk::decompress(&compressed, 1024 * 1024)
            .map_err(foyer::Error::io_error)?;
        extent_decode(&raw)
            .map(|v| CachedPackIndex(Arc::from(v)))
            .ok_or_else(|| foyer::Error::io_error(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid extent-encoded pack index"),
            ))
    }

    fn estimated_size(&self) -> usize {
        // Estimate compressed size on SSD: ~17 bytes per entry.
        self.0.len() * 17
    }
}

/// Two-tier cache for pack indices, backed by foyer HybridCache.
pub struct PackIndexCache {
    inner: HybridCache<PackId, CachedPackIndex>,
}

impl PackIndexCache {
    /// Open the pack index cache with default tier sizes (64MB memory, 2GB SSD).
    pub async fn open(cache_dir: &Path) -> anyhow::Result<Self> {
        Self::open_with_sizes(cache_dir, DEFAULT_MEMORY_BYTES, DEFAULT_SSD_BYTES).await
    }

    /// Open with custom tier sizes.
    pub async fn open_with_sizes(
        cache_dir: &Path,
        memory_bytes: usize,
        ssd_bytes: usize,
    ) -> anyhow::Result<Self> {
        let dir = cache_dir.join("foyer_pack_index_v2");
        std::fs::create_dir_all(&dir)?;

        let inner: HybridCache<PackId, CachedPackIndex> = HybridCacheBuilder::new()
            .with_name("glidefs-pack-index-cache")
            .memory(memory_bytes)
            .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
            .with_weighter(|_key: &PackId, value: &CachedPackIndex| {
                value.0.len() * std::mem::size_of::<PackIndexEntry>()
            })
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
    /// Returns an `Arc<[PackIndexEntry]>` — callers share the cached slice
    /// without cloning ~14 KB of entry data per call.
    pub async fn get_entries(&self, pack_id: PackId) -> Option<Arc<[PackIndexEntry]>> {
        match self.inner.get(&pack_id).await {
            Ok(Some(entry)) => Some(Arc::clone(&entry.value().0)),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(pack_id, error = %e, "pack index cache error, treating as miss");
                None
            }
        }
    }

    /// Insert pack index entries into the cache (fire-and-forget).
    pub fn insert_entries(&self, pack_id: PackId, entries: &[PackIndexEntry]) {
        self.inner
            .insert(pack_id, CachedPackIndex(Arc::from(entries)));
    }

    /// Look up a single block by chunk_offset within a cached pack index.
    ///
    /// Returns `(hash, pack_offset, comp_length)` on hit.
    pub async fn lookup_block(
        &self,
        pack_id: PackId,
        chunk_offset: u32,
    ) -> Option<(Blake3Hash, u32, u32)> {
        let entry = self.inner.get(&pack_id).await.ok()??;
        let entries = &entry.value().0;
        entries
            .binary_search_by_key(&chunk_offset, |e| e.chunk_offset)
            .ok()
            .map(|idx| {
                let e = &entries[idx];
                (e.hash, e.offset, e.comp_length)
            })
    }

    /// Close the cache, releasing file descriptors and flushing SSD state.
    #[allow(dead_code)]
    pub async fn close(&self) -> anyhow::Result<()> {
        self.inner.close().await.map_err(Into::into)
    }

    /// Collect all block hashes across multiple packs (for flush dedup).
    pub async fn known_hashes(&self, pack_ids: &[PackId]) -> HashSet<Blake3Hash> {
        let mut hashes = HashSet::new();
        for &pack_id in pack_ids {
            if let Some(entries) = self.get_entries(pack_id).await {
                for entry in entries.iter() {
                    hashes.insert(entry.hash);
                }
            }
        }
        hashes
    }
}

// ============================================================================
// Extent encoding: groups consecutive chunk_offsets into extents
// ============================================================================

/// Encode pack index entries as extents.
///
/// Format per extent:
///   start_chunk_offset: u32 LE
///   count:              u16 LE
///   per block: hash [u8; 16] + comp_length u32 LE
///
/// `offset` is implicit: PACK_HEADER_SIZE + cumulative comp_length.
fn extent_encode(entries: &[PackIndexEntry]) -> Vec<u8> {
    if entries.is_empty() {
        return Vec::new();
    }

    // ~20 bytes per entry + 6 bytes per extent header, few extents expected
    let mut buf = Vec::with_capacity(entries.len() * 20 + 64);
    let mut i = 0;

    while i < entries.len() {
        let start = i;
        let start_offset = entries[start].chunk_offset;

        // Find extent: consecutive chunk_offsets with diff == 1
        i += 1;
        while i < entries.len()
            && entries[i].chunk_offset == start_offset + (i - start) as u32
            && (i - start) < u16::MAX as usize
        {
            i += 1;
        }

        let count = (i - start) as u16;
        buf.extend_from_slice(&start_offset.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());

        for entry in &entries[start..i] {
            buf.extend_from_slice(entry.hash.as_bytes());
            buf.extend_from_slice(&entry.comp_length.to_le_bytes());
        }
    }

    buf
}

/// Decode extent-encoded data back to flat entries.
fn extent_decode(data: &[u8]) -> Option<Vec<PackIndexEntry>> {
    let mut entries = Vec::new();
    let mut pos = 0;
    let mut pack_offset = PACK_HEADER_SIZE as u32;

    while pos + 6 <= data.len() {
        let start_chunk_offset = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        let count = u16::from_le_bytes(data[pos + 4..pos + 6].try_into().ok()?) as usize;
        pos += 6;

        for j in 0..count {
            if pos + 20 > data.len() {
                return None;
            }
            let mut hash = [0u8; 16];
            hash.copy_from_slice(&data[pos..pos + 16]);
            let comp_length = u32::from_le_bytes(data[pos + 16..pos + 20].try_into().ok()?);
            pos += 20;

            entries.push(PackIndexEntry {
                hash: Blake3Hash::from_bytes(hash),
                chunk_offset: start_chunk_offset + j as u32,
                offset: pack_offset,
                comp_length,
            });

            pack_offset += comp_length;
        }
    }

    if pos != data.len() {
        return None;
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

    /// Create entries with realistic offsets (cumulative comp_length from PACK_HEADER_SIZE).
    fn make_consecutive_entries(start: u32, count: u32, comp_len: u32) -> Vec<PackIndexEntry> {
        let mut entries = Vec::new();
        let mut offset = PACK_HEADER_SIZE as u32;
        for i in 0..count {
            entries.push(PackIndexEntry {
                hash: Blake3Hash([(start + i) as u8; 16]),
                chunk_offset: start + i,
                offset,
                comp_length: comp_len,
            });
            offset += comp_len;
        }
        entries
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
        let entries = make_consecutive_entries(42, 1, 65536);

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
        assert_eq!(got[0].hash, entries[0].hash);
        assert_eq!(got[0].comp_length, 65536);
    }

    #[test]
    fn test_extent_encode_decode_round_trip() {
        // Consecutive entries (one extent)
        let entries = make_consecutive_entries(0, 500, 63000);
        let encoded = extent_encode(&entries);
        let decoded = extent_decode(&encoded).expect("should decode");

        assert_eq!(decoded.len(), entries.len());
        for (a, b) in entries.iter().zip(decoded.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.chunk_offset, b.chunk_offset);
            assert_eq!(a.offset, b.offset);
            assert_eq!(a.comp_length, b.comp_length);
        }
    }

    #[test]
    fn test_extent_encode_multiple_extents() {
        // Two extents: 0-2 and 10-12 (gap at 3-9)
        let mut entries = Vec::new();
        let mut offset = PACK_HEADER_SIZE as u32;
        for co in [0, 1, 2, 10, 11, 12] {
            entries.push(PackIndexEntry {
                hash: Blake3Hash([co as u8; 16]),
                chunk_offset: co,
                offset,
                comp_length: 1000,
            });
            offset += 1000;
        }

        let encoded = extent_encode(&entries);
        let decoded = extent_decode(&encoded).expect("should decode");

        assert_eq!(decoded.len(), 6);
        for (a, b) in entries.iter().zip(decoded.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.chunk_offset, b.chunk_offset);
            assert_eq!(a.offset, b.offset);
            assert_eq!(a.comp_length, b.comp_length);
        }
    }

    #[test]
    fn test_extent_encode_single_entry() {
        let entries = make_consecutive_entries(42, 1, 65536);
        let encoded = extent_encode(&entries);
        let decoded = extent_decode(&encoded).expect("should decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].chunk_offset, 42);
        assert_eq!(decoded[0].offset, entries[0].offset);
    }

    #[test]
    fn test_extent_encode_empty() {
        let encoded = extent_encode(&[]);
        assert!(encoded.is_empty());
        let decoded = extent_decode(&encoded).expect("should decode");
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_extent_encode_all_scattered() {
        // Every entry is its own extent (no consecutive offsets)
        let mut entries = Vec::new();
        let mut offset = PACK_HEADER_SIZE as u32;
        for co in [0, 5, 10, 100, 200] {
            entries.push(PackIndexEntry {
                hash: Blake3Hash([co as u8; 16]),
                chunk_offset: co,
                offset,
                comp_length: 2000,
            });
            offset += 2000;
        }

        let encoded = extent_encode(&entries);
        let decoded = extent_decode(&encoded).expect("should decode");
        assert_eq!(decoded.len(), 5);
        for (a, b) in entries.iter().zip(decoded.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.chunk_offset, b.chunk_offset);
            assert_eq!(a.offset, b.offset);
            assert_eq!(a.comp_length, b.comp_length);
        }
    }

    #[test]
    fn test_extent_decode_rejects_truncated() {
        let entries = make_consecutive_entries(0, 3, 1000);
        let mut encoded = extent_encode(&entries);
        encoded.truncate(encoded.len() - 1); // corrupt
        assert!(extent_decode(&encoded).is_none());
    }

    #[test]
    fn test_code_round_trip() {
        use std::io::Cursor;

        let entries = make_consecutive_entries(0, 100, 63000);
        let cached = CachedPackIndex(Arc::from(entries.clone()));

        let mut buf = Vec::new();
        cached.encode(&mut buf).expect("encode should succeed");

        let decoded = CachedPackIndex::decode(&mut Cursor::new(&buf)).expect("decode should succeed");
        assert_eq!(decoded.0.len(), entries.len());
        for (a, b) in entries.iter().zip(decoded.0.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.chunk_offset, b.chunk_offset);
            assert_eq!(a.offset, b.offset);
            assert_eq!(a.comp_length, b.comp_length);
        }

        // Verify compression: encoded should be smaller than raw 28B * 100
        assert!(buf.len() < 100 * 28, "compressed size {} should be < {}", buf.len(), 100 * 28);
    }
}
