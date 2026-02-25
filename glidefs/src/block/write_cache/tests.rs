use super::*;
use crate::block::chunk_cache::ChunkMetaCache;
use crate::block::state::{Active, Initializing};
use crate::block::volume_manifest::VolumeManifest;
use bytes::Bytes;
use object_store::memory::InMemory;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tempfile::TempDir;

fn test_config(dir: &Path) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.to_path_buf(),
        device_name: "test".to_string(),
        device_size: 1024 * 1024, // 1MB
        block_size: 4096,         // 4KB for testing
        wal_sync: false,
    }
}

#[tokio::test]
async fn test_open_fresh_cache() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery().await.unwrap();

    assert_eq!(cache.dirty_block_count(), 0);
}

#[tokio::test]
async fn test_write_read() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery().await.unwrap();
    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write some data
    cache.write(0, b"hello world", &clean_cache).unwrap();

    // Read it back
    let data = cache.read(0, 11).unwrap();
    assert_eq!(&data[..], b"hello world");

    // Should have dirty blocks now
    assert!(cache.dirty_block_count() > 0);
}

#[tokio::test]
async fn test_flush() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery().await.unwrap();
    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    cache.write(0, b"data", &clean_cache).unwrap();
    cache.flush().unwrap();

    // Data should still be readable
    let data = cache.read(0, 4).unwrap();
    assert_eq!(&data[..], b"data");
}

#[tokio::test]
async fn test_metadata_persistence() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Create cache and write data
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache.write(0, b"persistent", &clean_cache).unwrap();
        cache.save_metadata().unwrap();
    }

    // Reopen and verify dirty blocks are preserved
    {
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        // Should have dirty blocks from previous session
        assert!(cache.inner.dirty_block_count.load(Ordering::Relaxed) > 0);

        let cache = cache.finish_recovery().await.unwrap();
        // Data should be readable
        let data = cache.read(0, 10).unwrap();
        assert_eq!(&data[..], b"persistent");
    }
}

#[test]
fn test_is_zero_block() {
    // Test the is_zero_block helper function
    let zeros = vec![0u8; 4096];
    assert!(
        super::inner::is_zero_block(&zeros),
        "all zeros should return true"
    );

    let mut non_zeros = vec![0u8; 4096];
    non_zeros[0] = 1;
    assert!(
        !super::inner::is_zero_block(&non_zeros),
        "first byte non-zero"
    );

    non_zeros[0] = 0;
    non_zeros[4095] = 1;
    assert!(
        !super::inner::is_zero_block(&non_zeros),
        "last byte non-zero"
    );

    non_zeros[4095] = 0;
    non_zeros[2048] = 1;
    assert!(
        !super::inner::is_zero_block(&non_zeros),
        "middle byte non-zero"
    );

    // Empty slice
    assert!(
        super::inner::is_zero_block(&[]),
        "empty slice is 'all zeros'"
    );
}

// ====================================================================
// v2 Test Harness + Flush Tests
// ====================================================================

/// Test harness for v2 content-addressed flush tests.
///
/// Bundles the full v2 stack (cache + content store + pack index) in one
/// struct so tests can focus on behavior, not setup boilerplate.
struct V2Harness {
    cache: WriteCache<Active>,
    content_store: crate::block::content_store::ContentStore,
    chunk_meta_cache: Arc<ChunkMetaCache>,
    volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,
    clean_cache: crate::block::cache::SimpleBlockCache,
    #[allow(dead_code)]
    dir: TempDir,
}

impl V2Harness {
    /// Create a harness with default 1MB device / 4KB blocks.
    async fn new() -> Self {
        Self::with_config(1024 * 1024, 4096).await
    }

    /// Create a harness with custom device size and block size.
    async fn with_config(device_size: u64, block_size: usize) -> Self {
        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test".to_string(),
            device_size,
            block_size,
            wal_sync: false,
        };
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store =
            crate::block::content_store::ContentStore::new(object_store, "test-bucket");
        let chunk_meta_cache = Arc::new(ChunkMetaCache::open(dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            device_size,
            block_size as u32,
        )));
        let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        Self {
            cache,
            content_store,
            chunk_meta_cache,
            volume_manifest,
            clean_cache,
            dir,
        }
    }

    /// Flush and return stats.
    async fn flush(&self) -> FlushStats {
        let result: Result<FlushStats, CacheError> = self
            .cache
            .flush_to_s3(
                &self.content_store,
                &self.chunk_meta_cache,
                &self.volume_manifest,
            )
            .await;
        result.unwrap()
    }

    /// v2 read through the full tiered resolution path.
    async fn read(&self, offset: u64, len: usize) -> Bytes {
        let metrics = crate::block::metrics::ExportMetrics::new();
        let result: Result<Bytes, CacheError> = self
            .cache
            .read_v2(
                offset,
                len,
                &self.clean_cache,
                &self.chunk_meta_cache,
                &self.volume_manifest,
                &self.content_store,
                &metrics,
            )
            .await;
        result.unwrap()
    }

    /// Get the volume manifest from S3.
    async fn manifest(&self) -> VolumeManifest {
        let bytes = self
            .content_store
            .get_volume_manifest("test")
            .await
            .unwrap()
            .expect("manifest should exist");
        VolumeManifest::deserialize(&bytes).unwrap()
    }
}

#[tokio::test]
async fn test_flush_end_to_end() {
    let h = V2Harness::new().await;

    // Write 10 distinct blocks (each 4KB = block_size)
    for i in 0u8..10 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }

    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 10);
    assert_eq!(stats.blocks_deduped, 0);
    assert!(stats.packs_uploaded > 0);
    assert!(stats.bytes_uploaded > 0);

    let manifest = h.manifest().await;
    assert_eq!(manifest.block_size, 4096);
    // Volume manifest should reflect that data has been flushed
    assert_eq!(manifest.size, 1024 * 1024);
}

#[tokio::test]
async fn test_flush_dedup_skips_existing() {
    let h = V2Harness::new().await;

    // Write 5 blocks
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }

    let stats1 = h.flush().await;
    assert_eq!(stats1.blocks_flushed, 5);
    assert_eq!(stats1.blocks_deduped, 0);
    assert!(stats1.packs_uploaded > 0);

    // Write the same data again to new offsets — same content, new positions
    for i in 0u8..5 {
        h.cache
            .write((i as u64 + 5) * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }

    let stats2 = h.flush().await;
    assert_eq!(stats2.blocks_flushed, 5);
    assert_eq!(stats2.blocks_deduped, 5, "all blocks should be deduped");
    assert_eq!(stats2.packs_uploaded, 0, "no new packs needed");
}

#[tokio::test]
async fn test_flush_partial_dedup() {
    let h = V2Harness::new().await;

    // Write 10 blocks with unique data
    for i in 0u8..10 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }

    let stats1 = h.flush().await;
    assert_eq!(stats1.blocks_flushed, 10);
    assert_eq!(stats1.blocks_deduped, 0);

    // Write 10 more blocks: 5 with SAME data as before (dedup), 5 with NEW data
    for i in 0u8..5 {
        h.cache
            .write((i as u64 + 10) * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }
    for i in 0u8..5 {
        h.cache
            .write((i as u64 + 15) * 4096, &vec![i + 100; 4096], &h.clean_cache)
            .unwrap();
    }

    let stats2 = h.flush().await;
    assert_eq!(stats2.blocks_flushed, 10);
    assert_eq!(stats2.blocks_deduped, 5, "5 blocks should be deduped");
    assert_eq!(stats2.packs_uploaded, 1, "5 new blocks = 1 pack");
}

#[tokio::test]
async fn test_flush_zero_blocks_skipped() {
    // Use 128KB blocks so ZERO_BLOCK_HASH matches
    let h = V2Harness::with_config(128 * 1024 * 4, 128 * 1024).await;

    // Write one real block
    h.cache
        .write(0, &vec![42u8; 128 * 1024], &h.clean_cache)
        .unwrap();
    // Write a block of zeros — this should get ZERO_BLOCK_HASH
    h.cache
        .write(128 * 1024, &vec![0u8; 128 * 1024], &h.clean_cache)
        .unwrap();

    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 2);
    assert_eq!(stats.blocks_deduped, 1, "zero block should be deduped");
    assert_eq!(stats.packs_uploaded, 1, "only one pack for the real block");
}

#[tokio::test]
async fn test_flush_clears_dirty_state() {
    let h = V2Harness::new().await;

    for i in 0u8..3 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }

    h.flush().await;

    // A second flush should be a no-op
    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 0, "no dirty blocks after flush");
}

#[tokio::test]
async fn test_flush_concurrent_write_stays_dirty() {
    let h = V2Harness::new().await;

    h.cache.write(0, &vec![1u8; 4096], &h.clean_cache).unwrap();
    h.flush().await;

    // Overwrite block 0 with different data
    h.cache.write(0, &vec![2u8; 4096], &h.clean_cache).unwrap();

    let stats = h.flush().await;
    assert_eq!(
        stats.blocks_flushed, 1,
        "overwritten block should be flushed"
    );
}

#[tokio::test]
async fn test_flush_manifest_self_contained() {
    // 3MB device (750 blocks at 4KB) — enough for 600 writes
    let h = V2Harness::with_config(3 * 1024 * 1024, 4096).await;

    // Write 600 unique blocks.
    // Embed block index as LE bytes so every block has a unique hash.
    for i in 0u16..600 {
        let mut data = vec![0xAAu8; 4096];
        data[..2].copy_from_slice(&(i + 1).to_le_bytes());
        h.cache
            .write(i as u64 * 4096, &data, &h.clean_cache)
            .unwrap();
    }

    let stats = h.flush().await;
    assert!(stats.packs_uploaded > 0, "should produce at least 1 pack");

    // Volume manifest should exist and have correct device size
    let manifest = h.manifest().await;
    assert_eq!(manifest.size, 3 * 1024 * 1024);
    assert_eq!(manifest.block_size, 4096);
}

// ====================================================================
// v2 Read Path Tests
// ====================================================================

#[tokio::test]
async fn test_v2_read_recently_written_block() {
    let h = V2Harness::new().await;
    let data = vec![0xAAu8; 4096];
    h.cache.write(0, &data, &h.clean_cache).unwrap();

    let got = h.read(0, 4096).await;
    assert_eq!(got.as_ref(), &data[..]);
}

#[tokio::test]
async fn test_v2_read_zero_block() {
    let h = V2Harness::new().await;

    // Never written — block_map entry is Blake3Hash::ZERO → returns zeros.
    let got = h.read(0, 4096).await;
    assert!(
        got.iter().all(|&b| b == 0),
        "unwritten block should be all zeros"
    );
}

#[tokio::test]
async fn test_v2_read_trimmed_block() {
    let h = V2Harness::new().await;

    // Write a block, then zero it out.
    h.cache
        .write(0, &vec![0xBBu8; 4096], &h.clean_cache)
        .unwrap();
    h.cache.zero_range(0, 4096).unwrap();

    let got = h.read(0, 4096).await;
    assert!(
        got.iter().all(|&b| b == 0),
        "trimmed block should be all zeros"
    );
}

#[tokio::test]
async fn test_v2_read_sub_chunk() {
    let h = V2Harness::new().await;
    let data = vec![0xCCu8; 4096];
    h.cache.write(0, &data, &h.clean_cache).unwrap();

    // Read 100 bytes from offset 1000 within the chunk.
    let got = h.read(1000, 100).await;
    assert_eq!(got.len(), 100);
    assert!(got.iter().all(|&b| b == 0xCC));
}

#[tokio::test]
async fn test_v2_read_spans_chunks() {
    let h = V2Harness::new().await;

    // Write two distinct chunks.
    h.cache
        .write(0, &vec![0x11u8; 4096], &h.clean_cache)
        .unwrap();
    h.cache
        .write(4096, &vec![0x22u8; 4096], &h.clean_cache)
        .unwrap();

    // Read across the chunk boundary: last 100 bytes of chunk 0 + first 100 of chunk 1.
    let got = h.read(3996, 200).await;
    assert_eq!(got.len(), 200);
    assert!(
        got[..100].iter().all(|&b| b == 0x11),
        "first 100 bytes from chunk 0"
    );
    assert!(
        got[100..].iter().all(|&b| b == 0x22),
        "last 100 bytes from chunk 1"
    );
}

#[tokio::test]
async fn test_v2_read_from_s3_pack() {
    use crate::block::cache::BlockCache;

    let h = V2Harness::new().await;

    // Write blocks, flush to S3, clear clean_cache so reads go to S3.
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }
    h.flush().await;

    // Clear clean_cache so reads must resolve through S3 packs.
    for i in 0u8..5 {
        let hash = crate::block::block_map::blake3_128(&vec![i + 1; 4096]);
        h.clean_cache.remove(&hash);
    }

    // Reads should now resolve through: block_map → (miss cache) → S3 pack.
    for i in 0u8..5 {
        let got = h.read(i as u64 * 4096, 4096).await;
        assert!(
            got.iter().all(|&b| b == i + 1),
            "block {} should contain 0x{:02x}",
            i,
            i + 1
        );
    }
}

// NOTE: The old test_v2_pack_prefetch_warms_siblings test has been removed.
// The chunked architecture stores each block individually, so there is no
// "pack prefetch" that warms sibling blocks.

#[tokio::test]
async fn test_v2_mixed_dirty_and_clean_reads() {
    use crate::block::cache::BlockCache;

    let h = V2Harness::new().await;

    // Write 5 blocks and flush to S3 (they'll become "clean" once evicted from cache).
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }
    h.flush().await;

    // Clear clean_cache for flushed blocks so they must come from S3.
    for i in 0u8..5 {
        let hash = crate::block::block_map::blake3_128(&vec![i + 1; 4096]);
        h.clean_cache.remove(&hash);
    }

    // Write 5 more blocks (dirty, not flushed).
    for i in 5u8..10 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }

    // Read all 10 blocks. First 5 from S3, last 5 from clean_cache/SSD.
    for i in 0u8..10 {
        let got = h.read(i as u64 * 4096, 4096).await;
        assert!(
            got.iter().all(|&b| b == i + 1),
            "block {} (source: {}) should contain 0x{:02x}",
            i,
            if i < 5 { "S3" } else { "dirty" },
            i + 1
        );
    }
}

#[tokio::test]
async fn test_v2_clean_cache_eviction() {
    use crate::block::block_map::blake3_128;
    use crate::block::cache::{BlockCache, SimpleBlockCache};

    // Budget: 3 blocks of 4096 bytes.
    let cache = SimpleBlockCache::new(3 * 4096);

    for i in 0u8..5 {
        let hash = blake3_128(&vec![i; 4096]);
        cache.insert(hash, Bytes::from(vec![i; 4096]));
    }

    // Oldest 2 entries should have been evicted.
    let h0 = blake3_128(&vec![0u8; 4096]);
    let h1 = blake3_128(&vec![1u8; 4096]);
    let h4 = blake3_128(&vec![4u8; 4096]);

    assert!(cache.get(&h0).await.is_none(), "oldest should be evicted");
    assert!(
        cache.get(&h1).await.is_none(),
        "second oldest should be evicted"
    );
    assert!(cache.get(&h4).await.is_some(), "newest should be present");
}

// ========================================================================
// Snapshot tests
// ========================================================================

#[tokio::test]
async fn test_snapshot_returns_sequence_and_stats() {
    let h = V2Harness::new().await;

    // Write 5 blocks
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }

    let result: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.chunk_meta_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();

    assert!(result.sequence > 0, "sequence should be > 0 after writes");
    assert_eq!(result.stats.blocks_flushed, 5);
    assert!(result.stats.packs_uploaded > 0);

    // Manifest should exist in S3
    let manifest = h.manifest().await;
    assert_eq!(manifest.block_size, 4096);
}

#[tokio::test]
async fn test_snapshot_clears_dirty_state() {
    let h = V2Harness::new().await;

    // Write blocks and snapshot
    for i in 0u8..3 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache)
            .unwrap();
    }

    let result1: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.chunk_meta_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();
    assert_eq!(result1.stats.blocks_flushed, 3);

    // Second snapshot with no new writes should be a no-op
    let result2: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.chunk_meta_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();
    assert_eq!(
        result2.stats.blocks_flushed, 0,
        "no dirty blocks after snapshot"
    );
}

#[tokio::test]
async fn test_snapshot_captures_concurrent_writes() {
    let h = V2Harness::new().await;

    // Write blocks at different times
    h.cache.write(0, &vec![0xAA; 4096], &h.clean_cache).unwrap();
    h.cache
        .write(4096, &vec![0xBB; 4096], &h.clean_cache)
        .unwrap();

    let result: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.chunk_meta_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();
    assert_eq!(result.stats.blocks_flushed, 2);

    // Write more after snapshot
    h.cache
        .write(8192, &vec![0xCC; 4096], &h.clean_cache)
        .unwrap();

    // Second snapshot picks up the new write
    let result2: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.chunk_meta_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();
    assert_eq!(result2.stats.blocks_flushed, 1);
}

// ========================================================================
// Recovery hash drift
// ========================================================================

/// Recovery verifies dirty blocks are readable from SSD.
///
/// After a crash, dirty blocks must be readable so the flush path can
/// compute fresh blake3 hashes. This test writes data, persists metadata,
/// then reopens — recovery should succeed with 0 warnings.
#[tokio::test]
async fn test_recovery_verifies_dirty_blocks_readable() {
    let dir = TempDir::new().unwrap();
    let block_size = 4096;
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test-recovery".to_string(),
        device_size: 1024 * 1024,
        block_size,
        wal_sync: false,
    };

    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let original_data = vec![0xAAu8; block_size];

    // Session 1: write data and persist metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache.write(0, &original_data, &clean_cache).unwrap();
        cache.save_metadata().unwrap();
    }

    // Session 2: reopen — recovery should verify dirty blocks are readable
    {
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // No recovery warnings (SSD data is readable)
        assert_eq!(cache.inner.recovery_warnings.load(Ordering::Relaxed), 0);

        // Read should return the original data
        let data = cache.read(0, block_size).unwrap();
        assert_eq!(&data[..], &original_data[..]);
    }
}

// NOTE: The old pack registry integration tests (test_flush_updates_pack_registry
// and test_multiple_flushes_accumulate_registry) have been removed.
// The chunked architecture (ChunkMetaCache + VolumeManifest) does not maintain
// a separate pack registry; chunk metadata is managed via ChunkMeta files.

// ========================================================================
// Concurrency tests
// ========================================================================

/// Truly concurrent flush + write: writers hammer blocks while a flush is in flight.
///
/// Proves the CAS loop in `transition_to_dirty` and `flush_dirty_inner` cooperate:
/// blocks written during flush stay dirty for the next flush cycle.
#[tokio::test]
async fn test_concurrent_flush_and_writes() {
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "conc-flush".to_string(),
        device_size: 1024 * 1024,
        block_size: 4096,
        wal_sync: false,
    };
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let content_store = Arc::new(crate::block::content_store::ContentStore::new(
        Arc::clone(&object_store),
        "test-conc",
    ));
    let chunk_meta_cache = Arc::new(ChunkMetaCache::open(dir.path()).await.unwrap());
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        1024 * 1024,
        4096,
    )));
    let clean_cache = Arc::new(crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = Arc::new(cache.finish_recovery().await.unwrap());

    // Seed some initial dirty blocks
    for i in 0u8..10 {
        cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], clean_cache.as_ref())
            .unwrap();
    }

    let mut tasks = JoinSet::new();

    // Spawn a flusher that runs multiple flush cycles
    {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&content_store);
        let cmc = Arc::clone(&chunk_meta_cache);
        let vm = Arc::clone(&volume_manifest);
        tasks.spawn(async move {
            for _ in 0..5 {
                let _ = cache.flush_to_s3(&cs, &cmc, &vm).await;
                tokio::task::yield_now().await;
            }
        });
    }

    // Spawn concurrent writers that overwrite blocks during flush
    for writer_id in 0..5u8 {
        let cache = Arc::clone(&cache);
        let clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            for round in 0..20u8 {
                let block_idx = (writer_id as u64 * 2) % 10;
                let data = vec![writer_id.wrapping_add(round).wrapping_add(100); 4096];
                cache
                    .write(block_idx * 4096, &data, clean_cache.as_ref())
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    // Final flush to capture any remaining dirty blocks
    let _stats = cache
        .flush_to_s3(&content_store, &chunk_meta_cache, &volume_manifest)
        .await
        .unwrap();

    // After final flush, all blocks should be clean
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "all blocks should be clean after final flush"
    );

    // Every block should be readable and consistent (all bytes the same)
    for i in 0..10u64 {
        let data = cache.read_local(i * 4096, 4096).unwrap();
        let first = data[0];
        assert!(
            data.iter().all(|&b| b == first),
            "block {} has torn data: first byte is {}, but not all bytes match",
            i,
            first
        );
    }
}

// ========================================================================
// Draining state machine
// ========================================================================

/// Verify the Active → Draining → finish lifecycle.
///
/// After shutdown(), metadata is persisted and the cache transitions to Draining.
/// Draining.finish() completes the lifecycle.
#[tokio::test]
async fn test_draining_state_transition() {
    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "drain-test".to_string(),
        device_size: 1024 * 1024,
        block_size: 4096,
        wal_sync: false,
    };

    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.finish_recovery().await.unwrap();

    // Write some data before draining
    cache.write(0, &vec![0xAA; 4096], &clean_cache).unwrap();
    cache.write(4096, &vec![0xBB; 4096], &clean_cache).unwrap();
    assert_eq!(cache.dirty_block_count(), 2);

    // Save the dirty count before shutdown
    let dirty_before = cache.dirty_block_count();

    // Transition to Draining via shutdown
    let draining = cache.shutdown().await.unwrap();

    // Draining state — finish() completes the lifecycle
    draining.finish();

    // Reopen and verify metadata was persisted by shutdown
    let cache2 = WriteCache::<Initializing>::open(config).unwrap();
    let reopened_dirty = cache2.inner.dirty_block_count.load(Ordering::Relaxed);
    assert_eq!(
        reopened_dirty, dirty_before,
        "shutdown should persist metadata with dirty blocks"
    );

    // Data should be readable after reopen + recovery
    let cache2 = cache2.finish_recovery().await.unwrap();
    let data = cache2.read(0, 4096).unwrap();
    assert_eq!(&data[..4], &[0xAA; 4]);
}

// ========================================================================
// Targeted concurrency race tests
// ========================================================================

/// Concurrent flush + write: verify S3 content correctness after convergence.
///
/// The existing `test_concurrent_flush_and_writes` verifies no torn data on
/// local SSD, but doesn't check that S3 packs and the manifest converge to
/// the correct final state. This test exercises the SYNCING-based interleaving:
///
///   Flush claims blocks via CAS DIRTY→SYNCING. A concurrent write transitions
///   SYNCING→DIRTY (via transition_to_dirty). Flush's CAS SYNCING→CLEAN fails,
///   leaving the block dirty for the next cycle. After quiescence (no concurrent
///   writes), the final flush cycle converges.
///
/// After convergence (final quiesced flush), we verify:
/// 1. Reading every block through the S3 path returns the correct final data
#[tokio::test]
async fn test_concurrent_flush_write_s3_convergence() {
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "s3-converge".to_string(),
        device_size: 1024 * 1024,
        block_size: 4096,
        wal_sync: false,
    };
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let content_store = Arc::new(crate::block::content_store::ContentStore::new(
        Arc::clone(&object_store),
        "test-converge",
    ));
    let chunk_meta_cache = Arc::new(ChunkMetaCache::open(dir.path()).await.unwrap());
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        1024 * 1024,
        4096,
    )));
    let clean_cache = Arc::new(crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = Arc::new(cache.finish_recovery().await.unwrap());

    // Seed blocks with initial data
    for i in 0u8..10 {
        cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], clean_cache.as_ref())
            .unwrap();
    }

    let mut tasks = JoinSet::new();

    // Flusher: runs flush cycles concurrently with writers
    {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&content_store);
        let cmc = Arc::clone(&chunk_meta_cache);
        let vm = Arc::clone(&volume_manifest);
        tasks.spawn(async move {
            for _ in 0..5 {
                let _ = cache.flush_to_s3(&cs, &cmc, &vm).await;
                tokio::task::yield_now().await;
            }
        });
    }

    // Writers: each repeatedly overwrites a block with new data.
    // Final value is deterministic: writer_id * 10 + 9 (last round).
    for writer_id in 0..5u8 {
        let cache = Arc::clone(&cache);
        let clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            let block_idx = writer_id as u64 * 2; // blocks 0,2,4,6,8
            for round in 0..10u8 {
                let fill = writer_id * 10 + round;
                cache
                    .write(block_idx * 4096, &vec![fill; 4096], clean_cache.as_ref())
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    // Quiesced final flush — no concurrent writes
    let _stats = cache
        .flush_to_s3(&content_store, &chunk_meta_cache, &volume_manifest)
        .await
        .unwrap();
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "all blocks clean after final flush"
    );

    // Verify reads through S3 return the correct final data.
    // Use a fresh clean_cache to force resolution through S3 packs.
    let verify_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let metrics = crate::block::metrics::ExportMetrics::new();
    for block_idx in 0u64..10 {
        let final_ssd = cache.read_local(block_idx * 4096, 4096).unwrap();

        // Read through full S3 path
        let s3_data = cache
            .read_v2(
                block_idx * 4096,
                4096,
                &verify_cache,
                &chunk_meta_cache,
                &volume_manifest,
                &content_store,
                &metrics,
            )
            .await
            .unwrap();

        assert_eq!(
            &s3_data[..],
            &final_ssd[..],
            "block {} S3 read doesn't match SSD — flush may have committed \
             a hash for stale data",
            block_idx,
        );
    }
}

// NOTE: The old HostPackIndex prune tests (test_prune_stale_snapshot_loses_entries_regression
// and test_rebuild_manifest_hashes_prevents_prune_loss) have been removed.
// The chunked architecture (ChunkMetaCache + VolumeManifest) does not use a
// host-side pack_index, so pack_index pruning is no longer relevant here.

/// CRC32 mismatch at flush time: block is skipped, CRC consumed from crc_map,
/// next checkpoint recomputes, next flush succeeds.
#[tokio::test]
async fn test_crc32_mismatch_skips_block_then_heals() {
    let h = V2Harness::new().await;

    // Write a block.
    let original_data = vec![0xABu8; 4096];
    h.cache.write(0, &original_data, &h.clean_cache).unwrap();
    assert_eq!(h.cache.dirty_block_count(), 1);

    // Run local checkpoint — computes CRC32 for the dirty block.
    h.cache.local_checkpoint().unwrap();

    // Verify CRC32 was computed (present in crc_map).
    let inner = h.cache.inner();
    let crc_after_checkpoint = *inner.crc_map.get(&0).expect("checkpoint should compute CRC32");
    assert_ne!(crc_after_checkpoint, 0);

    // Corrupt the block on SSD (simulate bit rot after checkpoint).
    let corrupted_data = vec![0xFFu8; 4096];
    inner.data_file.write_all_at(&corrupted_data, 0).unwrap();

    // Flush should detect CRC32 mismatch and skip the block.
    let (stats, _seq) = h
        .cache
        .flush_packs(&h.content_store, &h.chunk_meta_cache, &h.volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 1, "should detect 1 corrupted block");
    assert_eq!(
        stats.packs_uploaded, 0,
        "corrupted block should not be uploaded"
    );
    assert_eq!(h.cache.dirty_block_count(), 1, "block should remain dirty");

    // CRC32 consumed by flush (crc_take removes it).
    assert!(inner.crc_map.get(&0).is_none(), "CRC32 consumed after flush");

    // Next checkpoint recomputes CRC32 from the (still corrupted) SSD data.
    h.cache.local_checkpoint().unwrap();
    let crc_recomputed = *inner.crc_map.get(&0).expect("checkpoint should recompute CRC32");
    assert_ne!(
        crc_recomputed, crc_after_checkpoint,
        "new CRC32 should differ (different data)"
    );

    // Next flush succeeds — CRC32 now matches the (corrupted) SSD data.
    // This is the inherent limitation of deferred checksumming: if corruption
    // is persistent, the next checkpoint captures the corrupted state.
    let (stats2, _seq) = h
        .cache
        .flush_packs(&h.content_store, &h.chunk_meta_cache, &h.volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0, "no mismatch on second flush");
    assert_eq!(stats2.blocks_flushed, 1);
}

/// CRC32 is invalidated on new writes, so stale checksums don't cause false positives.
#[tokio::test]
async fn test_crc32_cleared_on_write() {
    let h = V2Harness::new().await;

    // Write, checkpoint (compute CRC32).
    h.cache
        .write(0, &vec![0xAAu8; 4096], &h.clean_cache)
        .unwrap();
    h.cache.local_checkpoint().unwrap();

    let inner = h.cache.inner();
    assert!(inner.crc_map.get(&0).is_some(), "checkpoint should set CRC32");

    // Write new data to the same block — CRC32 should be invalidated.
    h.cache
        .write(0, &vec![0xBBu8; 4096], &h.clean_cache)
        .unwrap();
    assert!(inner.crc_map.get(&0).is_none(), "write should clear CRC32");

    // Flush without a checkpoint in between — no CRC32, verification skipped.
    let (stats, _) = h
        .cache
        .flush_packs(&h.content_store, &h.chunk_meta_cache, &h.volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 0);
    assert_eq!(stats.blocks_flushed, 1);
}

/// Partial corruption: multiple dirty blocks, only some corrupted.
/// Flush should upload the good blocks and skip the bad ones.
#[tokio::test]
async fn test_crc32_partial_corruption_flushes_good_blocks() {
    let h = V2Harness::new().await;

    // Write 5 distinct blocks.
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 10; 4096], &h.clean_cache)
            .unwrap();
    }
    assert_eq!(h.cache.dirty_block_count(), 5);

    // Checkpoint — computes CRC32 for all 5 dirty blocks.
    h.cache.local_checkpoint().unwrap();

    let inner = h.cache.inner();
    for i in 0usize..5 {
        assert!(
            inner.crc_map.get(&i).is_some(),
            "block {i} should have CRC32"
        );
    }

    // Corrupt blocks 1 and 3 on SSD (leave 0, 2, 4 intact).
    inner
        .data_file
        .write_all_at(&vec![0xFFu8; 4096], 4096)
        .unwrap();
    inner
        .data_file
        .write_all_at(&vec![0xFEu8; 4096], 3 * 4096)
        .unwrap();

    // Flush: should upload 3 good blocks, skip 2 corrupted.
    let (stats, _) = h
        .cache
        .flush_packs(&h.content_store, &h.chunk_meta_cache, &h.volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 2, "blocks 1 and 3 corrupted");
    assert_eq!(stats.blocks_flushed, 5, "all 5 scanned");
    assert_eq!(stats.packs_uploaded, 1, "3 good blocks → 1 pack");

    // Good blocks (0, 2, 4) should be clean now; corrupted (1, 3) still dirty.
    assert_eq!(
        h.cache.dirty_block_count(),
        2,
        "corrupted blocks remain dirty"
    );

    // CRC32 consumed by flush for all blocks.
    assert!(inner.crc_map.get(&1).is_none(), "CRC consumed by flush");
    assert!(inner.crc_map.get(&3).is_none(), "CRC consumed by flush");

    // Heal: checkpoint recomputes CRC32 from (still corrupted) SSD data.
    h.cache.local_checkpoint().unwrap();
    assert!(inner.crc_map.get(&1).is_some(), "checkpoint recomputed CRC for block 1");
    assert!(inner.crc_map.get(&3).is_some(), "checkpoint recomputed CRC for block 3");

    // Second flush succeeds for the remaining 2 blocks.
    let (stats2, _) = h
        .cache
        .flush_packs(&h.content_store, &h.chunk_meta_cache, &h.volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0);
    assert_eq!(stats2.blocks_flushed, 2);
    assert_eq!(
        h.cache.dirty_block_count(),
        0,
        "all blocks clean after heal"
    );
}

/// CRC32 verified correctly on the happy path: checkpoint computes CRC32,
/// flush verifies it matches, block is uploaded normally.
///
/// Also verifies the full CRC32 lifecycle across multiple cycles: write →
/// checkpoint → flush → write more → checkpoint → flush. CRC32 must be
/// properly invalidated on write and fresh CRC32s computed at checkpoint.
#[tokio::test]
async fn test_crc32_happy_path_multi_cycle() {
    let h = V2Harness::new().await;

    // Cycle 1: write 3 blocks, checkpoint (computes CRC32), flush.
    for i in 0u8..3 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 10; 4096], &h.clean_cache)
            .unwrap();
    }
    h.cache.local_checkpoint().unwrap();

    let inner = h.cache.inner();
    for i in 0usize..3 {
        assert!(
            inner.crc_map.get(&i).is_some(),
            "cycle 1: block {i} should have CRC32"
        );
    }

    let (stats1, _) = h
        .cache
        .flush_packs(&h.content_store, &h.chunk_meta_cache, &h.volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats1.blocks_corrupted, 0, "cycle 1: no corruption");
    assert_eq!(stats1.blocks_flushed, 3);
    assert_eq!(stats1.packs_uploaded, 1);
    assert_eq!(h.cache.dirty_block_count(), 0, "cycle 1: all clean");

    // Cycle 2: write 2 new blocks + overwrite 1 existing block.
    h.cache
        .write(3 * 4096, &vec![0xDD; 4096], &h.clean_cache)
        .unwrap();
    h.cache
        .write(4 * 4096, &vec![0xEE; 4096], &h.clean_cache)
        .unwrap();
    h.cache.write(0, &vec![0xFF; 4096], &h.clean_cache).unwrap(); // overwrite block 0

    assert_eq!(h.cache.dirty_block_count(), 3);

    // Block 0's CRC32 should be invalidated by the overwrite.
    assert!(
        inner.crc_map.get(&0).is_none(),
        "overwrite should clear CRC32"
    );

    h.cache.local_checkpoint().unwrap();

    for idx in [0usize, 3, 4] {
        assert!(
            inner.crc_map.get(&idx).is_some(),
            "cycle 2: block {idx} should have CRC32"
        );
    }

    let (stats2, _) = h
        .cache
        .flush_packs(&h.content_store, &h.chunk_meta_cache, &h.volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0, "cycle 2: no corruption");
    assert_eq!(stats2.blocks_flushed, 3);
    assert_eq!(h.cache.dirty_block_count(), 0, "cycle 2: all clean");
}

/// Concurrent writes during flush with CRC32 enabled must never produce
/// false corruption reports.
///
/// When a write lands during flush, it transitions SYNCING→DIRTY and
/// invalidates the stale CRC. The state discrimination in the CRC mismatch
/// branch detects that the block is no longer SYNCING → concurrent write,
/// not corruption. This test verifies that invariant under stress.
///
/// The assertion: across all flush cycles with concurrent writers,
/// `blocks_corrupted` is always 0. Any CRC32 mismatch from concurrent
/// writes is classified as `blocks_cas_failed`.
#[tokio::test]
async fn test_crc32_concurrent_writes_never_false_corruption() {
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "crc32-conc".to_string(),
        device_size: 1024 * 1024,
        block_size: 4096,
        wal_sync: false,
    };
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let content_store = Arc::new(crate::block::content_store::ContentStore::new(
        Arc::clone(&object_store),
        "test-crc32-conc",
    ));
    let chunk_meta_cache = Arc::new(ChunkMetaCache::open(dir.path()).await.unwrap());
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        1024 * 1024,
        4096,
    )));
    let clean_cache = Arc::new(crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = Arc::new(cache.finish_recovery().await.unwrap());

    // Seed blocks with initial data + checkpoint to arm CRC32.
    for i in 0u8..10 {
        cache
            .write(i as u64 * 4096, &vec![i + 1; 4096], clean_cache.as_ref())
            .unwrap();
    }
    cache.local_checkpoint().unwrap();

    let mut tasks = JoinSet::new();

    // Flusher: interleaves checkpoints and flush_packs to keep CRC32 active.
    let total_corrupted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&content_store);
        let cmc = Arc::clone(&chunk_meta_cache);
        let vm = Arc::clone(&volume_manifest);
        let corrupted = Arc::clone(&total_corrupted);
        tasks.spawn(async move {
            for _ in 0..10 {
                cache.local_checkpoint().unwrap();
                let (stats, _) = cache.flush_packs(&cs, &cmc, &vm).await.unwrap();
                corrupted.fetch_add(
                    stats.blocks_corrupted as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                tokio::task::yield_now().await;
            }
        });
    }

    // Concurrent writers hammering the same blocks during flush.
    for writer_id in 0..5u8 {
        let cache = Arc::clone(&cache);
        let clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            for round in 0..30u8 {
                let block_idx = (writer_id as u64 * 2) % 10;
                let fill = writer_id.wrapping_add(round).wrapping_add(100);
                cache
                    .write(block_idx * 4096, &vec![fill; 4096], clean_cache.as_ref())
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    assert_eq!(
        total_corrupted.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "concurrent writes must never be misclassified as SSD corruption"
    );

    // Final quiesced flush to verify convergence.
    cache.local_checkpoint().unwrap();
    let stats = cache
        .flush_to_s3(&content_store, &chunk_meta_cache, &volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 0);
    assert_eq!(cache.dirty_block_count(), 0);
}
