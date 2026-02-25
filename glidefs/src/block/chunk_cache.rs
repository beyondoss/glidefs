//! Two-tier chunk metadata cache backed by foyer HybridCache.
//!
//! Replaces HostPackIndex (redb). Content-addressed by chunk hash, so identical
//! chunk states are shared across forks and volumes. Global per-host.
//!
//! Tiers:
//!   Memory (S3-FIFO eviction, byte-sized) → ~100ns lookup
//!   SSD (foyer disk engine, direct I/O) → ~100µs
//!   Miss → caller fetches from S3
//!
//! Uses the same foyer HybridCache as the clean block cache, storing serialized
//! GLCM bytes and deserializing on read. Deserialization is cheap (binary memcpy).

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, EvictionConfig, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, RecoverMode, S3FifoConfig,
};

use super::block_map::Blake3Hash;
use super::chunk_meta::ChunkMeta;
use super::pack::PackLocation;

/// Default memory tier: 64MB (holds many sparse chunk metas).
const DEFAULT_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Default SSD tier: 512MB (holds thousands of chunk metas).
const DEFAULT_SSD_BYTES: usize = 512 * 1024 * 1024;

/// Two-tier cache for chunk metadata, backed by foyer HybridCache.
pub struct ChunkMetaCache {
    inner: HybridCache<Blake3Hash, Bytes>,
}

impl ChunkMetaCache {
    /// Open the chunk meta cache with default tier sizes (64MB memory, 512MB SSD).
    ///
    /// Creates `{cache_dir}/foyer_chunk_meta/` for the SSD tier.
    pub async fn open(cache_dir: &Path) -> anyhow::Result<Self> {
        Self::open_with_sizes(cache_dir, DEFAULT_MEMORY_BYTES, DEFAULT_SSD_BYTES).await
    }

    /// Open with custom tier sizes.
    pub async fn open_with_sizes(
        cache_dir: &Path,
        memory_bytes: usize,
        ssd_bytes: usize,
    ) -> anyhow::Result<Self> {
        let dir = cache_dir.join("foyer_chunk_meta");
        std::fs::create_dir_all(&dir)?;

        let inner: HybridCache<Blake3Hash, Bytes> = HybridCacheBuilder::new()
            .with_name("glidefs-chunk-meta-cache")
            .memory(memory_bytes)
            .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
            .with_weighter(|_key: &Blake3Hash, value: &Bytes| value.len())
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

    /// Get a chunk meta by its content hash.
    ///
    /// Returns deserialized ChunkMeta on hit, None on miss.
    pub async fn get(&self, chunk_hash: &Blake3Hash) -> Option<Arc<ChunkMeta>> {
        match self.inner.get(chunk_hash).await {
            Ok(Some(entry)) => ChunkMeta::deserialize(entry.value()).ok().map(Arc::new),
            _ => None,
        }
    }

    /// Insert a chunk meta into the cache (fire-and-forget).
    ///
    /// Serializes the ChunkMeta to GLCM bytes and inserts into foyer.
    /// Foyer handles memory → SSD eviction asynchronously.
    pub fn insert(&self, chunk_hash: Blake3Hash, meta: Arc<ChunkMeta>) {
        self.inner
            .insert(chunk_hash, Bytes::from(meta.serialize()));
    }

    /// Resolve a block to its pack location via the chunk meta cache.
    ///
    /// Convenience method: get chunk meta by hash, then binary search for the block offset.
    pub async fn resolve_block(
        &self,
        chunk_hash: &Blake3Hash,
        block_offset: u32,
    ) -> Option<(Blake3Hash, PackLocation)> {
        let meta = self.get(chunk_hash).await?;
        meta.lookup(block_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::chunk_meta::ChunkMetaEntry;
    use tempfile::TempDir;
    use uuid::Uuid;

    const CHUNK_SIZE: u64 = 10 * 1024 * 1024 * 1024;

    /// Small cache sizes for tests (foyer needs real directories).
    async fn open_test_cache(dir: &Path) -> ChunkMetaCache {
        ChunkMetaCache::open_with_sizes(dir, 16 * 1024 * 1024, 64 * 1024 * 1024)
            .await
            .expect("failed to open test cache")
    }

    fn make_meta(chunk_idx: u32, entries: Vec<ChunkMetaEntry>) -> ChunkMeta {
        ChunkMeta {
            chunk_idx,
            chunk_size: CHUNK_SIZE,
            block_size: 131072,
            entries,
        }
    }

    fn make_entry(offset: u32) -> ChunkMetaEntry {
        ChunkMetaEntry {
            offset,
            hash: Blake3Hash([offset as u8; 16]),
            pack_id: Uuid::from_bytes([0xaa; 16]),
            pack_offset: offset * 1000,
            comp_length: 65536,
        }
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let tmp = TempDir::new().unwrap();
        let cache = open_test_cache(tmp.path()).await;

        let meta = make_meta(0, vec![make_entry(10), make_entry(20)]);
        let hash = meta.content_hash();
        cache.insert(hash, Arc::new(meta));

        let got = cache.get(&hash).await.expect("should find in cache");
        assert_eq!(got.block_count(), 2);
        assert_eq!(got.entries[0].offset, 10);
    }

    #[tokio::test]
    async fn test_miss_returns_none() {
        let tmp = TempDir::new().unwrap();
        let cache = open_test_cache(tmp.path()).await;
        assert!(cache.get(&Blake3Hash([0xff; 16])).await.is_none());
    }

    #[tokio::test]
    async fn test_ssd_persistence() {
        let tmp = TempDir::new().unwrap();

        let meta = make_meta(5, vec![make_entry(42)]);
        let hash = meta.content_hash();

        // Insert into first cache instance, then drop it
        {
            let cache = open_test_cache(tmp.path()).await;
            cache.insert(hash, Arc::new(meta));
            // Close gracefully so SSD data is flushed
            cache.inner.close().await.unwrap();
        }

        // Open a new cache instance (memory is fresh, SSD should have the data)
        let cache2 = open_test_cache(tmp.path()).await;
        let got = cache2.get(&hash).await.expect("should find on SSD");
        assert_eq!(got.chunk_idx, 5);
        assert_eq!(got.block_count(), 1);
    }

    #[tokio::test]
    async fn test_resolve_block() {
        let tmp = TempDir::new().unwrap();
        let cache = open_test_cache(tmp.path()).await;

        let meta = make_meta(0, vec![make_entry(10), make_entry(20), make_entry(30)]);
        let hash = meta.content_hash();
        cache.insert(hash, Arc::new(meta));

        let (block_hash, loc) = cache
            .resolve_block(&hash, 20)
            .await
            .expect("should resolve");
        assert_eq!(block_hash, Blake3Hash([20; 16]));
        assert_eq!(loc.offset, 20000);
        assert!(cache.resolve_block(&hash, 99).await.is_none());
    }
}
