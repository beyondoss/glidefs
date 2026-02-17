//! Block cache for decompressed block data.
//!
//! Maps content hashes to decompressed block bytes. Used as the "clean cache"
//! tier in the v2 read path: blocks fetched from S3 packs are decompressed,
//! verified, and inserted here for fast subsequent reads.
//!
//! Two implementations:
//! - `FoyerBlockCache`: Production cache with S3-FIFO eviction, memory + SSD tiers.
//! - `SimpleBlockCache`: Test-only Mutex<HashMap> with FIFO eviction.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use parking_lot::Mutex;

use super::block_map::Blake3Hash;

/// Trait for a content-addressed block cache.
///
/// `get` is async to support SSD-backed caches (foyer's HybridCache reads from
/// disk on memory miss). `insert` is sync — foyer writes to SSD asynchronously
/// on memory eviction.
#[async_trait]
pub trait BlockCache: Send + Sync {
    async fn get(&self, hash: &Blake3Hash) -> Option<Bytes>;
    fn insert(&self, hash: Blake3Hash, data: Bytes);
    /// Remove a block from the cache. Returns true if it was present.
    fn remove(&self, hash: &Blake3Hash) -> bool;
}

// ============================================================================
// FoyerBlockCache — production hybrid cache (memory + SSD)
// ============================================================================

use foyer::{
    BlockEngineConfig, DeviceBuilder, EvictionConfig, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, RecoverMode, S3FifoConfig,
};

/// Configuration for the foyer-backed block cache.
pub struct FoyerCacheConfig {
    /// Memory tier capacity in bytes.
    pub memory_bytes: usize,
    /// SSD tier capacity in bytes.
    pub ssd_bytes: usize,
    /// Directory for SSD cache files.
    pub ssd_dir: PathBuf,
}

/// Production block cache backed by foyer's HybridCache.
///
/// - Memory tier: S3-FIFO eviction, ~100ns reads.
/// - SSD tier: catches memory evictions, ~100us reads.
/// - Shared across all exports on the host (content-addressed dedup).
pub struct FoyerBlockCache {
    inner: HybridCache<Blake3Hash, Bytes>,
}

impl FoyerBlockCache {
    /// Open the hybrid cache. Creates the SSD directory if needed.
    pub async fn open(config: FoyerCacheConfig) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&config.ssd_dir)?;

        let inner: HybridCache<Blake3Hash, Bytes> = HybridCacheBuilder::new()
            .with_name("glidefs-clean-cache")
            .memory(config.memory_bytes)
            .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
            .with_weighter(|_key: &Blake3Hash, value: &Bytes| value.len())
            .storage()
            .with_engine_config(BlockEngineConfig::new(
                FsDeviceBuilder::new(&config.ssd_dir)
                    .with_capacity(config.ssd_bytes)
                    .build()?,
            ))
            .with_recover_mode(RecoverMode::Quiet)
            .build()
            .await?;

        Ok(Self { inner })
    }
}

#[async_trait]
impl BlockCache for FoyerBlockCache {
    async fn get(&self, hash: &Blake3Hash) -> Option<Bytes> {
        match self.inner.get(hash).await {
            Ok(Some(entry)) => Some(entry.value().clone()),
            _ => None,
        }
    }

    fn insert(&self, hash: Blake3Hash, data: Bytes) {
        self.inner.insert(hash, data);
    }

    fn remove(&self, hash: &Blake3Hash) -> bool {
        self.inner.remove(hash);
        // foyer's remove is fire-and-forget; assume it was present
        true
    }
}

// ============================================================================
// SimpleBlockCache — test-only in-memory cache
// ============================================================================

/// Bounded in-memory block cache backed by a HashMap.
///
/// FIFO eviction, Mutex-based. Used in tests that don't need foyer's
/// SSD tier or S3-FIFO behavior.
pub struct SimpleBlockCache {
    inner: Mutex<SimpleCacheInner>,
    max_bytes: usize,
}

struct SimpleCacheInner {
    map: HashMap<Blake3Hash, Bytes>,
    /// Insertion-order keys for FIFO eviction.
    order: VecDeque<Blake3Hash>,
    current_bytes: usize,
}

impl SimpleBlockCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(SimpleCacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                current_bytes: 0,
            }),
            max_bytes,
        }
    }
}

#[async_trait]
impl BlockCache for SimpleBlockCache {
    async fn get(&self, hash: &Blake3Hash) -> Option<Bytes> {
        let inner = self.inner.lock();
        inner.map.get(hash).cloned()
    }

    fn insert(&self, hash: Blake3Hash, data: Bytes) {
        let data_len = data.len();
        let mut inner = self.inner.lock();

        // Already present — skip.
        if inner.map.contains_key(&hash) {
            return;
        }

        // Evict oldest entries until we have room.
        while inner.current_bytes + data_len > self.max_bytes {
            let Some(oldest) = inner.order.pop_front() else {
                break;
            };
            if let Some(evicted) = inner.map.remove(&oldest) {
                inner.current_bytes -= evicted.len();
            }
        }

        inner.current_bytes += data_len;
        inner.order.push_back(hash);
        inner.map.insert(hash, data);
    }

    fn remove(&self, hash: &Blake3Hash) -> bool {
        let mut inner = self.inner.lock();
        if let Some(evicted) = inner.map.remove(hash) {
            inner.current_bytes -= evicted.len();
            inner.order.retain(|h| h != hash);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nbd::block_map::blake3_128;
    use std::sync::Arc;

    fn make_hash(i: u8) -> Blake3Hash {
        blake3_128(&[i; 32])
    }

    // ====================================================================
    // SimpleBlockCache tests
    // ====================================================================

    #[tokio::test]
    async fn test_insert_and_get() {
        let cache = SimpleBlockCache::new(1024 * 1024);
        let hash = make_hash(1);
        let data = Bytes::from(vec![0xAA; 4096]);

        cache.insert(hash, data.clone());
        let got = cache.get(&hash).await.expect("should find inserted block");
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn test_get_missing() {
        let cache = SimpleBlockCache::new(1024 * 1024);
        assert!(cache.get(&make_hash(99)).await.is_none());
    }

    #[tokio::test]
    async fn test_eviction_respects_budget() {
        // Budget: 3 blocks of 1000 bytes each.
        let cache = SimpleBlockCache::new(3000);

        for i in 0u8..5 {
            cache.insert(make_hash(i), Bytes::from(vec![i; 1000]));
        }

        // Oldest entries (0, 1) should have been evicted.
        assert!(cache.get(&make_hash(0)).await.is_none());
        assert!(cache.get(&make_hash(1)).await.is_none());
        // Newest entries (2, 3, 4) should be present.
        assert!(cache.get(&make_hash(2)).await.is_some());
        assert!(cache.get(&make_hash(3)).await.is_some());
        assert!(cache.get(&make_hash(4)).await.is_some());
    }

    #[tokio::test]
    async fn test_duplicate_insert_is_noop() {
        let cache = SimpleBlockCache::new(1024 * 1024);
        let hash = make_hash(1);
        let data = Bytes::from(vec![0xBB; 4096]);

        cache.insert(hash, data.clone());
        cache.insert(hash, Bytes::from(vec![0xCC; 4096]));

        let got = cache.get(&hash).await.unwrap();
        assert_eq!(got, data, "second insert should be a no-op");

        let inner = cache.inner.lock();
        assert_eq!(inner.current_bytes, 4096);
    }

    // ====================================================================
    // FoyerBlockCache tests
    // ====================================================================

    async fn open_test_foyer(memory_bytes: usize, ssd_bytes: usize) -> FoyerBlockCache {
        let dir = tempfile::tempdir().unwrap();
        FoyerBlockCache::open(FoyerCacheConfig {
            memory_bytes,
            ssd_bytes,
            ssd_dir: dir.path().to_path_buf(),
        })
        .await
        .expect("failed to open foyer cache")
    }

    #[tokio::test]
    async fn test_foyer_insert_and_get() {
        let cache = open_test_foyer(4 * 1024 * 1024, 16 * 1024 * 1024).await;
        let hash = make_hash(1);
        let data = Bytes::from(vec![0xAA; 4096]);

        cache.insert(hash, data.clone());
        let got = cache.get(&hash).await.expect("should find inserted block");
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn test_foyer_get_missing() {
        let cache = open_test_foyer(4 * 1024 * 1024, 16 * 1024 * 1024).await;
        assert!(cache.get(&make_hash(99)).await.is_none());
    }

    #[tokio::test]
    async fn test_foyer_trait_object() {
        // Verify FoyerBlockCache works through Arc<dyn BlockCache>.
        let cache: Arc<dyn BlockCache> =
            Arc::new(open_test_foyer(4 * 1024 * 1024, 16 * 1024 * 1024).await);

        let hash = make_hash(42);
        let data = Bytes::from(vec![0xFF; 4096]);
        cache.insert(hash, data.clone());
        let got: Bytes = cache.get(&hash).await.unwrap();
        assert_eq!(got, data);
    }
}
