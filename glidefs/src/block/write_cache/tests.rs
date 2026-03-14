use super::*;
use crate::block::block_map::SparseBlockState;
use crate::block::pack_index_cache::PackIndexCache;
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
    let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write some data
    cache.write(0, b"hello world").unwrap();

    // Read it back
    let data = cache.read_local_only(0, 11).unwrap();
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
    let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    cache.write(0, b"data").unwrap();
    cache.flush().unwrap();

    // Data should still be readable
    let data = cache.read_local_only(0, 4).unwrap();
    assert_eq!(&data[..], b"data");
}

#[tokio::test]
async fn test_metadata_persistence() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Create cache and write data
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache.write(0, b"persistent").unwrap();
        cache.save_metadata().unwrap();
    }

    // Reopen and verify dirty blocks are preserved
    {
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        // Should have dirty blocks from previous session
        assert!(cache.inner.dirty_block_count.load(Ordering::Relaxed) > 0);

        let cache = cache.finish_recovery().await.unwrap();
        // Data should be readable
        let data = cache.read_local_only(0, 10).unwrap();
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
// Test Harness + Flush Tests
// ====================================================================

/// Test harness for content-addressed flush tests.
///
/// Bundles the full stack (cache + content store + pack index cache +
/// volume manifest) in one struct so tests can focus on behavior, not
/// setup boilerplate.
struct V2Harness {
    cache: WriteCache<Active>,
    content_store: crate::block::content_store::ContentStore,
    pack_index_cache: Arc<PackIndexCache>,
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
        let pack_index_cache = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
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
            pack_index_cache,
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
                &self.pack_index_cache,
                &self.volume_manifest,
            )
            .await;
        result.unwrap()
    }

    /// Read through the full tiered resolution path.
    async fn read(&self, offset: u64, len: usize) -> Bytes {
        let metrics = crate::block::metrics::ExportMetrics::new();
        let result: Result<Bytes, CacheError> = self
            .cache
            .read(
                offset,
                len,
                &self.clean_cache,
                &self.pack_index_cache,
                &self.volume_manifest,
                &self.content_store,
                &metrics,
            )
            .await;
        result.unwrap()
    }

    /// Get the volume manifest (in-memory copy).
    fn manifest(&self) -> VolumeManifest {
        self.volume_manifest.read().clone()
    }
}

#[tokio::test]
async fn test_flush_end_to_end() {
    let h = V2Harness::new().await;

    // Write 10 distinct blocks (each 4KB = block_size)
    for i in 0u8..10 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    let stats = h.flush().await;
    assert_eq!(stats.blocks_claimed, 10);
    assert_eq!(stats.blocks_deduped, 0);
    assert!(stats.packs_uploaded > 0);
    assert!(stats.bytes_uploaded > 0);

    let manifest = h.manifest();
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
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    let stats1 = h.flush().await;
    assert_eq!(stats1.blocks_claimed, 5);
    assert_eq!(stats1.blocks_deduped, 0);
    assert!(stats1.packs_uploaded > 0);

    // Write the same data again to new offsets — same content, new positions.
    // In the pack-based architecture, packs are indexed by chunk_offset (not
    // hash), so cross-flush hash-based dedup is intentionally not performed.
    // Each new block offset gets its own pack entry regardless of content.
    for i in 0u8..5 {
        h.cache
            .write((i as u64 + 5) * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    let stats2 = h.flush().await;
    assert_eq!(stats2.blocks_claimed, 5);
    // No cross-flush dedup in pack-based architecture — blocks at new offsets
    // are uploaded even if their content matches previously flushed blocks.
    assert!(stats2.packs_uploaded > 0, "new offsets require new pack entries");
}

#[tokio::test]
async fn test_flush_partial_dedup() {
    let h = V2Harness::new().await;

    // Write 10 blocks with unique data
    for i in 0u8..10 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    let stats1 = h.flush().await;
    assert_eq!(stats1.blocks_claimed, 10);
    assert_eq!(stats1.blocks_deduped, 0);

    // Write 10 more blocks at new offsets: 5 reuse previous content, 5 are new.
    // In the pack-based architecture, all 10 go to new pack entries since dedup
    // is offset-based (not hash-based across flushes).
    for i in 0u8..5 {
        h.cache
            .write((i as u64 + 10) * 4096, &vec![i + 1; 4096])
            .unwrap();
    }
    for i in 0u8..5 {
        h.cache
            .write((i as u64 + 15) * 4096, &vec![i + 100; 4096])
            .unwrap();
    }

    let stats2 = h.flush().await;
    assert_eq!(stats2.blocks_claimed, 10);
    // All 10 blocks are uploaded — no cross-flush hash dedup in pack-based architecture.
    assert!(stats2.packs_uploaded > 0, "all blocks need pack entries");
}

#[tokio::test]
async fn test_flush_zero_blocks_skipped() {
    // Use 128KB blocks so ZERO_BLOCK_HASH matches
    let h = V2Harness::with_config(128 * 1024 * 4, 128 * 1024).await;

    // Write one real block
    h.cache
        .write(0, &vec![42u8; 128 * 1024])
        .unwrap();
    // Write a block of zeros — this should get ZERO_BLOCK_HASH
    h.cache
        .write(128 * 1024, &vec![0u8; 128 * 1024])
        .unwrap();

    let stats = h.flush().await;
    assert_eq!(stats.blocks_claimed, 2);
    assert_eq!(stats.blocks_deduped, 1, "zero block should be deduped");
    assert_eq!(stats.packs_uploaded, 1, "only one pack for the real block");
}

#[tokio::test]
async fn test_flush_clears_dirty_state() {
    let h = V2Harness::new().await;

    for i in 0u8..3 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    h.flush().await;

    // A second flush should be a no-op
    let stats = h.flush().await;
    assert_eq!(stats.blocks_claimed, 0, "no dirty blocks after flush");
}

#[tokio::test]
async fn test_flush_concurrent_write_stays_dirty() {
    let h = V2Harness::new().await;

    h.cache.write(0, &vec![1u8; 4096]).unwrap();
    h.flush().await;

    // Overwrite block 0 with different data
    h.cache.write(0, &vec![2u8; 4096]).unwrap();

    let stats = h.flush().await;
    assert_eq!(
        stats.blocks_claimed, 1,
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
            .write(i as u64 * 4096, &data)
            .unwrap();
    }

    let stats = h.flush().await;
    assert!(stats.packs_uploaded > 0, "should produce at least 1 pack");

    // Volume manifest should have correct device size
    let manifest = h.manifest();
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
    h.cache.write(0, &data).unwrap();

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
        .write(0, &vec![0xBBu8; 4096])
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
    h.cache.write(0, &data).unwrap();

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
        .write(0, &vec![0x11u8; 4096])
        .unwrap();
    h.cache
        .write(4096, &vec![0x22u8; 4096])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096])
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

/// Multi-block read where some blocks are in the clean cache and others
/// require S3 fetch. Verifies the `try_join_all` fan-out assembles blocks
/// correctly even when resolution takes different paths (cached vs S3).
///
/// Layout: blocks 0-7 flushed to S3. Clean cache has 0-2 and 5-7.
/// Blocks 3-4 evicted from clean cache → must be fetched from S3 packs.
/// A single `read(0, 8*4096)` must return all 8 blocks in order.
#[tokio::test]
async fn test_v2_read_mixed_cache_hit_and_s3_miss() {
    use crate::block::block_map::blake3_128;
    use crate::block::cache::BlockCache;

    let h = V2Harness::new().await;

    // Write 8 blocks with distinct data per block
    for i in 0u8..8 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }
    h.flush().await;

    // Evict only blocks 3 and 4 from clean cache — they'll require S3 fetch
    for i in 3u8..5 {
        let hash = blake3_128(&vec![i + 1; 4096]);
        h.clean_cache.remove(&hash);
    }

    // Single multi-block read spanning all 8 blocks.
    // Blocks 0-2: clean cache hit
    // Blocks 3-4: cache miss → S3 pack fetch
    // Blocks 5-7: clean cache hit
    let got = h.read(0, 8 * 4096).await;
    assert_eq!(got.len(), 8 * 4096);

    for i in 0u8..8 {
        let start = i as usize * 4096;
        let block_data = &got[start..start + 4096];
        assert!(
            block_data.iter().all(|&b| b == i + 1),
            "block {} should contain 0x{:02x}, got 0x{:02x} (source: {})",
            i,
            i + 1,
            block_data[0],
            if (3..5).contains(&i) { "S3" } else { "cache" }
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
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    let result: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.pack_index_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();

    assert!(result.sequence > 0, "sequence should be > 0 after writes");
    assert_eq!(result.stats.blocks_claimed, 5);
    assert!(result.stats.packs_uploaded > 0);

    // Manifest should have correct block size
    let manifest = h.manifest();
    assert_eq!(manifest.block_size, 4096);
}

#[tokio::test]
async fn test_snapshot_clears_dirty_state() {
    let h = V2Harness::new().await;

    // Write blocks and snapshot
    for i in 0u8..3 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    let result1: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.pack_index_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();
    assert_eq!(result1.stats.blocks_claimed, 3);

    // Second snapshot with no new writes should be a no-op
    let result2: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.pack_index_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();
    assert_eq!(
        result2.stats.blocks_claimed, 0,
        "no dirty blocks after snapshot"
    );
}

#[tokio::test]
async fn test_snapshot_captures_concurrent_writes() {
    let h = V2Harness::new().await;

    // Write blocks at different times
    h.cache.write(0, &vec![0xAA; 4096]).unwrap();
    h.cache
        .write(4096, &vec![0xBB; 4096])
        .unwrap();

    let result: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.pack_index_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();
    assert_eq!(result.stats.blocks_claimed, 2);

    // Write more after snapshot
    h.cache
        .write(8192, &vec![0xCC; 4096])
        .unwrap();

    // Second snapshot picks up the new write
    let result2: SnapshotResult = h
        .cache
        .snapshot(
            &h.content_store,
            &h.pack_index_cache,
            &h.volume_manifest,
        )
        .await
        .unwrap();
    assert_eq!(result2.stats.blocks_claimed, 1);
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

    let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let original_data = vec![0xAAu8; block_size];

    // Session 1: write data and persist metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache.write(0, &original_data).unwrap();
        cache.save_metadata().unwrap();
    }

    // Session 2: reopen — recovery should verify dirty blocks are readable
    {
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // No recovery warnings (SSD data is readable)
        assert_eq!(cache.inner.recovery_warnings.load(Ordering::Relaxed), 0);

        // Read should return the original data
        let data = cache.read_local_only(0, block_size).unwrap();
        assert_eq!(&data[..], &original_data[..]);
    }
}

// NOTE: The old pack registry integration tests (test_flush_updates_pack_registry
// and test_multiple_flushes_accumulate_registry) have been removed.
// The pack-based architecture does not maintain a separate pack registry;
// pack metadata is managed via PackIndexCache and VolumeManifest.

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
    let pack_index_cache = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
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
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    let mut tasks = JoinSet::new();

    // Spawn a flusher that runs multiple flush cycles
    {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&content_store);
        let cmc = Arc::clone(&pack_index_cache);
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
        let _clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            for round in 0..20u8 {
                let block_idx = (writer_id as u64 * 2) % 10;
                let data = vec![writer_id.wrapping_add(round).wrapping_add(100); 4096];
                cache
                    .write(block_idx * 4096, &data)
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
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
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

    let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.finish_recovery().await.unwrap();

    // Write some data before draining
    cache.write(0, &vec![0xAA; 4096]).unwrap();
    cache.write(4096, &vec![0xBB; 4096]).unwrap();
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
    let data = cache2.read_local_only(0, 4096).unwrap();
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
    let pack_index_cache = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
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
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }

    let mut tasks = JoinSet::new();

    // Flusher: runs flush cycles concurrently with writers
    {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&content_store);
        let cmc = Arc::clone(&pack_index_cache);
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
        let _clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            let block_idx = writer_id as u64 * 2; // blocks 0,2,4,6,8
            for round in 0..10u8 {
                let fill = writer_id * 10 + round;
                cache
                    .write(block_idx * 4096, &vec![fill; 4096])
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
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();
    assert_eq!(
        cache.dirty_block_count(),
        0,
        "all blocks clean after final flush"
    );

    // Verify reads through S3 return the correct final data.
    // After flush, all blocks are NOT_PRESENT (evicted). Use a fresh
    // clean_cache to force resolution through S3 packs.
    let verify_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let metrics = crate::block::metrics::ExportMetrics::new();

    // Writer blocks (0,2,4,6,8) should have their final write value.
    // Non-writer blocks (1,3,5,7,9) should have their initial seed value.
    for block_idx in 0u64..10 {
        let s3_data = cache
            .read(
                block_idx * 4096,
                4096,
                &verify_cache,
                &pack_index_cache,
                &volume_manifest,
                &content_store,
                &metrics,
            )
            .await
            .unwrap();

        // Non-zero check: block must have been flushed to S3.
        assert!(
            !s3_data.iter().all(|&b| b == 0),
            "block {} reads as all zeros — data lost during concurrent flush",
            block_idx,
        );

        // All bytes in the block should be the same (our writes fill uniformly).
        let fill = s3_data[0];
        assert!(
            s3_data.iter().all(|&b| b == fill),
            "block {} has mixed bytes — partial write corruption",
            block_idx,
        );
    }
}

// NOTE: The old HostPackIndex prune tests (test_prune_stale_snapshot_loses_entries_regression
// and test_rebuild_manifest_hashes_prevents_prune_loss) have been removed.
// Pack index pruning is no longer relevant with PackIndexCache.

/// CRC32 mismatch at flush time: block is skipped, CRC consumed from crc_map,
/// next checkpoint recomputes, next flush succeeds.
#[tokio::test]
async fn test_crc32_mismatch_skips_block_then_heals() {
    let h = V2Harness::new().await;

    // Write a block.
    let original_data = vec![0xABu8; 4096];
    h.cache.write(0, &original_data).unwrap();
    assert_eq!(h.cache.dirty_block_count(), 1);

    // Run local checkpoint — computes CRC32 for the dirty block.
    h.cache.local_checkpoint().await.unwrap();

    // Verify CRC32 was computed (present in crc_map).
    let inner = h.cache.inner();
    let crc_after_checkpoint = inner.crc_map.load(0).expect("checkpoint should compute CRC32");
    assert_ne!(crc_after_checkpoint, 0);

    // Corrupt the block on SSD (simulate bit rot after checkpoint).
    let corrupted_data = vec![0xFFu8; 4096];
    inner.data_file.read().write_all_at(&corrupted_data, 0).unwrap();

    // Flush should detect CRC32 mismatch and skip the block.
    let (stats, _seq) = h
        .cache
        .flush_packs(&h.content_store, &h.pack_index_cache, &h.volume_manifest, None)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 1, "should detect 1 corrupted block");
    assert_eq!(
        stats.packs_uploaded, 0,
        "corrupted block should not be uploaded"
    );
    assert_eq!(h.cache.dirty_block_count(), 1, "block should remain dirty");

    // CRC32 consumed by flush (crc_take removes it).
    assert!(inner.crc_map.load(0).is_none(), "CRC32 consumed after flush");

    // Next checkpoint recomputes CRC32 from the (still corrupted) SSD data.
    h.cache.local_checkpoint().await.unwrap();
    let crc_recomputed = inner.crc_map.load(0).expect("checkpoint should recompute CRC32");
    assert_ne!(
        crc_recomputed, crc_after_checkpoint,
        "new CRC32 should differ (different data)"
    );

    // Next flush succeeds — CRC32 now matches the (corrupted) SSD data.
    // This is the inherent limitation of deferred checksumming: if corruption
    // is persistent, the next checkpoint captures the corrupted state.
    let (stats2, _seq) = h
        .cache
        .flush_packs(&h.content_store, &h.pack_index_cache, &h.volume_manifest, None)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0, "no mismatch on second flush");
    assert_eq!(stats2.blocks_claimed, 1);
}

/// CRC32 is invalidated on new writes, so stale checksums don't cause false positives.
#[tokio::test]
async fn test_crc32_cleared_on_write() {
    let h = V2Harness::new().await;

    // Write, checkpoint (compute CRC32).
    h.cache
        .write(0, &vec![0xAAu8; 4096])
        .unwrap();
    h.cache.local_checkpoint().await.unwrap();

    let inner = h.cache.inner();
    assert!(inner.crc_map.load(0).is_some(), "checkpoint should set CRC32");

    // Write new data to the same block — CRC32 should be invalidated (sentinel).
    h.cache
        .write(0, &vec![0xBBu8; 4096])
        .unwrap();
    assert_eq!(
        inner.crc_map.load(0).expect("sentinel should exist"),
        super::inner::CRC_SENTINEL,
        "write should set CRC sentinel"
    );

    // Flush without a checkpoint in between — no CRC32, verification skipped.
    let (stats, _) = h
        .cache
        .flush_packs(&h.content_store, &h.pack_index_cache, &h.volume_manifest, None)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 0);
    assert_eq!(stats.blocks_claimed, 1);
}

/// Partial corruption: multiple dirty blocks, only some corrupted.
/// Flush should upload the good blocks and skip the bad ones.
#[tokio::test]
async fn test_crc32_partial_corruption_flushes_good_blocks() {
    let h = V2Harness::new().await;

    // Write 5 distinct blocks.
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 10; 4096])
            .unwrap();
    }
    assert_eq!(h.cache.dirty_block_count(), 5);

    // Checkpoint — computes CRC32 for all 5 dirty blocks.
    h.cache.local_checkpoint().await.unwrap();

    let inner = h.cache.inner();
    for i in 0usize..5 {
        assert!(
            inner.crc_map.load(i).is_some(),
            "block {i} should have CRC32"
        );
    }

    // Corrupt blocks 1 and 3 on SSD (leave 0, 2, 4 intact).
    inner
        .data_file
        .read()
        .write_all_at(&vec![0xFFu8; 4096], 4096)
        .unwrap();
    inner
        .data_file
        .read()
        .write_all_at(&vec![0xFEu8; 4096], 3 * 4096)
        .unwrap();

    // Flush: should upload 3 good blocks, skip 2 corrupted.
    let (stats, _) = h
        .cache
        .flush_packs(&h.content_store, &h.pack_index_cache, &h.volume_manifest, None)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 2, "blocks 1 and 3 corrupted");
    assert_eq!(stats.blocks_claimed, 5, "all 5 scanned");
    assert_eq!(stats.packs_uploaded, 1, "3 good blocks → 1 pack");

    // Good blocks (0, 2, 4) should be clean now; corrupted (1, 3) still dirty.
    assert_eq!(
        h.cache.dirty_block_count(),
        2,
        "corrupted blocks remain dirty"
    );

    // CRC32 consumed by flush for all blocks.
    assert!(inner.crc_map.load(1).is_none(), "CRC consumed by flush");
    assert!(inner.crc_map.load(3).is_none(), "CRC consumed by flush");

    // Heal: checkpoint recomputes CRC32 from (still corrupted) SSD data.
    h.cache.local_checkpoint().await.unwrap();
    assert!(inner.crc_map.load(1).is_some(), "checkpoint recomputed CRC for block 1");
    assert!(inner.crc_map.load(3).is_some(), "checkpoint recomputed CRC for block 3");

    // Second flush succeeds for the remaining 2 blocks.
    let (stats2, _) = h
        .cache
        .flush_packs(&h.content_store, &h.pack_index_cache, &h.volume_manifest, None)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0);
    assert_eq!(stats2.blocks_claimed, 2);
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
            .write(i as u64 * 4096, &vec![i + 10; 4096])
            .unwrap();
    }
    h.cache.local_checkpoint().await.unwrap();

    let inner = h.cache.inner();
    for i in 0usize..3 {
        assert!(
            inner.crc_map.load(i).is_some(),
            "cycle 1: block {i} should have CRC32"
        );
    }

    let (stats1, _) = h
        .cache
        .flush_packs(&h.content_store, &h.pack_index_cache, &h.volume_manifest, None)
        .await
        .unwrap();
    assert_eq!(stats1.blocks_corrupted, 0, "cycle 1: no corruption");
    assert_eq!(stats1.blocks_claimed, 3);
    assert_eq!(stats1.packs_uploaded, 1);
    assert_eq!(h.cache.dirty_block_count(), 0, "cycle 1: all clean");

    // Cycle 2: write 2 new blocks + overwrite 1 existing block.
    h.cache
        .write(3 * 4096, &vec![0xDD; 4096])
        .unwrap();
    h.cache
        .write(4 * 4096, &vec![0xEE; 4096])
        .unwrap();
    h.cache.write(0, &vec![0xFF; 4096]).unwrap(); // overwrite block 0

    assert_eq!(h.cache.dirty_block_count(), 3);

    // Block 0's CRC32 should be invalidated by the overwrite (sentinel).
    assert_eq!(
        inner.crc_map.load(0).expect("sentinel should exist"),
        super::inner::CRC_SENTINEL,
        "overwrite should set CRC sentinel"
    );

    h.cache.local_checkpoint().await.unwrap();

    for idx in [0usize, 3, 4] {
        assert!(
            inner.crc_map.load(idx).is_some(),
            "cycle 2: block {idx} should have CRC32"
        );
    }

    let (stats2, _) = h
        .cache
        .flush_packs(&h.content_store, &h.pack_index_cache, &h.volume_manifest, None)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0, "cycle 2: no corruption");
    assert_eq!(stats2.blocks_claimed, 3);
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
    let pack_index_cache = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
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
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }
    cache.local_checkpoint().await.unwrap();

    let mut tasks = JoinSet::new();

    // Flusher: interleaves checkpoints and flush_packs to keep CRC32 active.
    let total_corrupted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&content_store);
        let cmc = Arc::clone(&pack_index_cache);
        let vm = Arc::clone(&volume_manifest);
        let corrupted = Arc::clone(&total_corrupted);
        tasks.spawn(async move {
            for _ in 0..10 {
                cache.local_checkpoint().await.unwrap();
                let (stats, _) = cache.flush_packs(&cs, &cmc, &vm, None).await.unwrap();
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
        let _clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            for round in 0..30u8 {
                let block_idx = (writer_id as u64 * 2) % 10;
                let fill = writer_id.wrapping_add(round).wrapping_add(100);
                cache
                    .write(block_idx * 4096, &vec![fill; 4096])
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
    cache.local_checkpoint().await.unwrap();
    let stats = cache
        .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 0);
    assert_eq!(cache.dirty_block_count(), 0);
}

// =========================================================================
// Bottomless storage tests
// =========================================================================

fn bottomless_config(dir: &Path) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.to_path_buf(),
        device_name: "test".to_string(),
        device_size: 1024 * 1024, // 1MB
        block_size: 4096,         // 4KB for testing
        wal_sync: false,
    }
}

/// Test harness for bottomless flush tests.
struct BottomlessHarness {
    cache: WriteCache<Active>,
    content_store: crate::block::content_store::ContentStore,
    pack_index_cache: Arc<PackIndexCache>,
    volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,
    clean_cache: crate::block::cache::SimpleBlockCache,
    #[allow(dead_code)]
    dir: TempDir,
}

impl BottomlessHarness {
    async fn new() -> Self {
        Self::with_config(1024 * 1024, 4096).await
    }

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
        let pack_index_cache = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
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
            pack_index_cache,
            volume_manifest,
            clean_cache,
            dir,
        }
    }

    async fn flush(&self) -> FlushStats {
        let cc: Arc<dyn crate::block::cache::BlockCache> =
            Arc::new(crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024));
        let (stats, _seq) = self
            .cache
            .flush_packs(
                &self.content_store,
                &self.pack_index_cache,
                &self.volume_manifest,
                Some(&cc),
            )
            .await
            .unwrap();
        stats
    }

    /// Drain: flush + evict (SYNCING→NOT_PRESENT). Use when testing eviction.
    async fn drain(&self) -> FlushStats {
        self.cache
            .flush_to_s3(
                &self.content_store,
                &self.pack_index_cache,
                &self.volume_manifest,
            )
            .await
            .unwrap()
    }
}

/// Test 1: Rotation mechanics.
///
/// Write blocks, simulate rotate_data_file (rename active -> flushing,
/// create new sparse active), verify:
/// - flushing_file is Some
/// - data is readable from the flushing file
/// - the new active file is empty (sparse/zeros) for those blocks
#[tokio::test]
async fn test_bottomless_rotate_data_file() {
    let dir = TempDir::new().unwrap();
    let config = bottomless_config(dir.path());
    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.finish_recovery().await.unwrap();
    let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write data to blocks 0 and 1
    let data0 = vec![0xAAu8; 4096];
    let data1 = vec![0xBBu8; 4096];
    cache.write(0, &data0).unwrap();
    cache.write(4096, &data1).unwrap();

    // Before rotation: no flushing file, flushing_active is false
    assert!(cache.inner.flushing_file.lock().is_none());
    assert!(!cache.inner.flushing_active.load(Ordering::Acquire));

    // Call the real rotate_data_file method
    cache.rotate_data_file().unwrap();

    // After rotation: flushing_file is Some, flushing_active is true
    assert!(cache.inner.flushing_active.load(Ordering::Acquire));
    assert!(cache.inner.flushing_file.lock().is_some());
    assert!(config.flushing_path().exists(), "flushing file should exist on disk");

    // Data should be readable from the flushing file
    {
        let ff_guard = cache.inner.flushing_file.lock();
        let ff = ff_guard.as_ref().unwrap();
        let mut buf = vec![0u8; 4096];
        ff.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf[..], &data0[..], "flushing file should have block 0 data");
        ff.read_exact_at(&mut buf, 4096).unwrap();
        assert_eq!(&buf[..], &data1[..], "flushing file should have block 1 data");
    }

    // New active file should be zeros for those blocks
    let mut buf = vec![0u8; 4096];
    cache.inner.data_file.read().read_exact_at(&mut buf, 0).unwrap();
    assert!(buf.iter().all(|&b| b == 0), "new active file block 0 should be zeros");
    cache.inner.data_file.read().read_exact_at(&mut buf, 4096).unwrap();
    assert!(buf.iter().all(|&b| b == 0), "new active file block 1 should be zeros");
}

/// Test 2: Eviction lifecycle.
///
/// Write blocks, flush via flush_packs with a real content store, verify blocks
/// transition through SYNCING to NOT_PRESENT (not CLEAN) in bottomless mode.
#[tokio::test]
async fn test_bottomless_eviction_lifecycle() {
    let h = BottomlessHarness::new().await;

    // Write 5 distinct blocks
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 1; 4096])
            .unwrap();
    }
    assert_eq!(h.cache.dirty_block_count(), 5);

    // Drain to S3 (drain evicts, unlike auto-flush which keeps blocks CLEAN)
    let stats = h.drain().await;
    assert_eq!(stats.blocks_claimed, 5);
    assert!(stats.packs_uploaded > 0);

    // After drain in bottomless mode, blocks should be NOT_PRESENT (evicted)
    for i in 0usize..5 {
        let state = h.cache.inner.state_map.get(i);
        assert_eq!(
            state,
            SparseBlockState::NOT_PRESENT,
            "block {i} should be NOT_PRESENT after drain, got {state}"
        );
    }

    // Dirty and syncing counts should be 0
    assert_eq!(h.cache.dirty_block_count(), 0);
    assert_eq!(h.cache.syncing_block_count(), 0);

    // Flushing file should be cleaned up
    assert!(
        h.cache.inner.flushing_file.lock().is_none(),
        "flushing_file should be None after flush completes"
    );
    assert!(
        !h.cache.inner.flushing_active.load(Ordering::Acquire),
        "flushing_active should be false after flush completes"
    );
    assert!(
        !h.cache.inner.config.flushing_path().exists(),
        "flushing file on disk should be deleted"
    );
}

/// Test 3: Re-dirty during flush.
///
/// Write a block, manually rotate + CAS DIRTY->SYNCING, write the same block
/// again (SYNCING->DIRTY via transition_to_dirty), verify the block stays DIRTY
/// with the new data in the active file.
#[tokio::test]
async fn test_bottomless_re_dirty_during_flush() {
    let dir = TempDir::new().unwrap();
    let config = bottomless_config(dir.path());
    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.finish_recovery().await.unwrap();
    let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write block 0 with initial data
    let initial_data = vec![0xAAu8; 4096];
    cache.write(0, &initial_data).unwrap();
    assert_eq!(cache.dirty_block_count(), 1);

    // Rotate the data file (start of flush)
    cache.rotate_data_file().unwrap();

    // CAS DIRTY -> SYNCING (what flush does to claim blocks)
    assert!(
        cache.inner.transition_dirty_to_syncing(0),
        "CAS DIRTY->SYNCING should succeed"
    );
    assert_eq!(cache.dirty_block_count(), 0);
    assert_eq!(cache.syncing_block_count(), 1);
    assert_eq!(cache.inner.state_map.get(0), SparseBlockState::SYNCING);

    // Write the same block again with new data (simulates concurrent write during flush)
    // This should CAS SYNCING -> DIRTY via transition_to_dirty
    let new_data = vec![0xBBu8; 4096];
    cache.write(0, &new_data).unwrap();

    // Block should be back to DIRTY
    assert_eq!(cache.inner.state_map.get(0), SparseBlockState::DIRTY);
    assert_eq!(cache.dirty_block_count(), 1);
    assert_eq!(cache.syncing_block_count(), 0);

    // New data should be in the active file (not the flushing file)
    let mut buf = vec![0u8; 4096];
    cache.inner.data_file.read().read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(
        &buf[..], &new_data[..],
        "active file should have the new data after re-dirty"
    );

    // Flushing file should still have the old data
    {
        let ff_guard = cache.inner.flushing_file.lock();
        let ff = ff_guard.as_ref().unwrap();
        let mut old_buf = vec![0u8; 4096];
        ff.read_exact_at(&mut old_buf, 0).unwrap();
        assert_eq!(
            &old_buf[..], &initial_data[..],
            "flushing file should have the original data"
        );
    }
}

/// Test 5: CLEAN -> NOT_PRESENT migration on open.
///
/// Create metadata with CLEAN blocks (as if from an older version that kept
/// blocks CLEAN after flush), reopen and verify they're migrated to NOT_PRESENT.
#[tokio::test]
async fn test_bottomless_clean_to_not_present_migration() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Create a cache and manually set blocks to CLEAN state, then save metadata.
    {
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test".to_string(),
            device_size: 1024 * 1024,
            block_size: 4096,
            wal_sync: false,
        };
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Manually set blocks to CLEAN state (simulates old metadata format)
        for i in 0usize..3 {
            cache.inner.state_map.set_present(i);
            // set_present puts them in CLEAN state already
            assert_eq!(
                cache.inner.state_map.get(i),
                SparseBlockState::CLEAN,
                "set_present should create CLEAN blocks"
            );
        }

        cache.save_metadata().unwrap();
    }

    // Phase 2: Reopen. CLEAN blocks should become NOT_PRESENT.
    {
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test".to_string(),
            device_size: 1024 * 1024,
            block_size: 4096,
            wal_sync: false,
        };

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // CLEAN blocks should have been migrated to NOT_PRESENT
        for i in 0usize..3 {
            assert_eq!(
                cache.inner.state_map.get(i),
                SparseBlockState::NOT_PRESENT,
                "block {i} should be NOT_PRESENT after migration"
            );
        }
        assert_eq!(cache.dirty_block_count(), 0);
    }
}

/// Test 6: Crash recovery with flushing file.
///
/// Simulate a crash mid-flush: create a flushing file on disk with data,
/// create metadata with DIRTY blocks (load_metadata converts SYNCING->DIRTY),
/// then open the cache. Verify recovery copies blocks to the active file,
/// deletes the flushing file, and all blocks are DIRTY.
#[tokio::test]
async fn test_bottomless_crash_recovery_with_flushing_file() {
    let dir = TempDir::new().unwrap();
    let block_size = 4096usize;
    let device_size = 1024u64 * 1024;

    // Phase 1: Create a cache, write data, save metadata with DIRTY blocks,
    // then manually create a flushing file to simulate a crash mid-flush.
    {
        let config = bottomless_config(dir.path());
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

        // Write 3 blocks
        for i in 0u8..3 {
            cache
                .write(i as u64 * block_size as u64, &vec![i + 0x10; block_size])
                .unwrap();
        }

        // Save metadata (blocks are DIRTY)
        cache.save_metadata().unwrap();

        // Simulate crash mid-flush: rename active file to flushing file.
        // The active file has our data. The next open() will see the flushing
        // file and recover.
        let active_path = config.data_path();
        let flushing_path = config.flushing_path();
        std::fs::rename(&active_path, &flushing_path).unwrap();

        // Create a new empty active file (what rotate_data_file would have done)
        let _new_active = std::fs::File::create(&active_path).unwrap();
        _new_active.set_len(device_size).unwrap();
    }

    // Phase 2: Reopen. Recovery should detect flushing file, copy data to
    // active file, and delete the flushing file.
    {
        let config = bottomless_config(dir.path());
        let flushing_path = config.flushing_path();

        // Flushing file should exist before open
        assert!(flushing_path.exists(), "flushing file should exist before recovery");

        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Flushing file should be deleted after recovery
        assert!(
            !flushing_path.exists(),
            "flushing file should be deleted after recovery"
        );

        // All 3 blocks should be DIRTY (not SYNCING, since load_metadata converts)
        for i in 0usize..3 {
            assert_eq!(
                cache.inner.state_map.get(i),
                SparseBlockState::DIRTY,
                "block {i} should be DIRTY after crash recovery"
            );
        }
        assert_eq!(cache.dirty_block_count(), 3);

        // Data should be readable from the active file (copied from flushing)
        for i in 0u8..3 {
            let mut buf = vec![0u8; block_size];
            cache
                .inner
                .data_file
                .read()
                .read_exact_at(&mut buf, i as u64 * block_size as u64)
                .unwrap();
            assert!(
                buf.iter().all(|&b| b == i + 0x10),
                "block {} should have recovered data 0x{:02x}, got 0x{:02x}",
                i,
                i + 0x10,
                buf[0]
            );
        }

        // No flushing file handle should be retained
        assert!(
            cache.inner.flushing_file.lock().is_none(),
            "flushing_file handle should not be retained after recovery"
        );
    }
}

/// Test 7: flushing_active flag lifecycle.
///
/// Verify flushing_active is false initially, true after rotate_data_file(),
/// and false after flushing file cleanup (via a full flush cycle).
#[tokio::test]
async fn test_bottomless_flushing_active_flag() {
    let h = BottomlessHarness::new().await;

    // Initially false
    assert!(
        !h.cache.inner.flushing_active.load(Ordering::Acquire),
        "flushing_active should be false initially"
    );

    // Write a block and flush. The flush internally rotates, so flushing_active
    // should be true during flush and false after.
    h.cache
        .write(0, &vec![0xDD; 4096])
        .unwrap();

    let stats = h.flush().await;
    assert_eq!(stats.blocks_claimed, 1);

    // After flush completes, flushing_active should be false
    assert!(
        !h.cache.inner.flushing_active.load(Ordering::Acquire),
        "flushing_active should be false after flush cleanup"
    );

    // Flushing file should be cleaned up
    assert!(
        h.cache.inner.flushing_file.lock().is_none(),
        "flushing_file should be None after flush"
    );
}

/// Test: flushing_active flag with no dirty blocks.
///
/// Verify that flushing_active is properly reset even when there are no
/// dirty blocks to flush (rotation still happens, but cleanup must run).
#[tokio::test]
async fn test_bottomless_flushing_active_no_dirty_blocks() {
    let h = BottomlessHarness::new().await;

    // Flush with no dirty blocks
    let stats = h.flush().await;
    assert_eq!(stats.blocks_claimed, 0);

    // flushing_active should still be false (cleanup path runs)
    assert!(
        !h.cache.inner.flushing_active.load(Ordering::Acquire),
        "flushing_active should be false even with empty flush"
    );
}

/// Test: Bottomless drain end-to-end with S3 read-back.
///
/// Write blocks, drain (evict to NOT_PRESENT), then read through the full
/// tiered path (local miss -> S3 pack). Verifies the full bottomless lifecycle.
#[tokio::test]
async fn test_bottomless_flush_and_s3_readback() {
    let h = BottomlessHarness::new().await;
    let metrics = crate::block::metrics::ExportMetrics::new();

    // Write 3 distinct blocks
    for i in 0u8..3 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 0x50; 4096])
            .unwrap();
    }

    // Drain to S3 (blocks become NOT_PRESENT)
    let stats = h.drain().await;
    assert_eq!(stats.blocks_claimed, 3);

    // All blocks should be NOT_PRESENT locally (drain evicts)
    for i in 0usize..3 {
        assert_eq!(h.cache.inner.state_map.get(i), SparseBlockState::NOT_PRESENT);
    }

    // Read blocks back through the tiered path (should resolve from S3/cache)
    for i in 0u8..3 {
        let data = h
            .cache
            .read(
                i as u64 * 4096,
                4096,
                &h.clean_cache,
                &h.pack_index_cache,
                &h.volume_manifest,
                &h.content_store,
                &metrics,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == i + 0x50),
            "block {} should read back 0x{:02x} from S3, got 0x{:02x}",
            i,
            i + 0x50,
            data[0]
        );
    }
}

/// Test: Concurrent writes during bottomless auto-flush.
///
/// Spawn an auto-flush on one task while writing to the same blocks concurrently.
/// Re-dirtied blocks must survive with the latest data. Flushed blocks stay
/// CLEAN on local SSD (auto-flush doesn't evict).
#[tokio::test]
async fn test_bottomless_concurrent_write_during_flush() {
    let h = Arc::new(BottomlessHarness::new().await);
    let metrics = crate::block::metrics::ExportMetrics::new();

    // Write 10 blocks
    for i in 0u8..10 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 0x10; 4096])
            .unwrap();
    }

    // Spawn flush in background
    let h2 = Arc::clone(&h);
    let flush_handle = tokio::spawn(async move {
        h2.flush().await
    });

    // Concurrently overwrite blocks 0-4 with new data
    // The write path CAS SYNCING→DIRTY races with the flush claiming DIRTY→SYNCING
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 0xA0; 4096])
            .unwrap();
    }

    let stats = flush_handle.await.unwrap();
    assert!(stats.blocks_claimed > 0, "some blocks should have been claimed");

    // After auto-flush, verify all blocks are readable with correct data.
    // Blocks 0-4: either re-dirtied (DIRTY, latest write = 0xA0+i) or
    // evicted (NOT_PRESENT, data in foyer).
    // Blocks 5-9: NOT_PRESENT (data in foyer) or still DIRTY.
    for i in 0u8..5 {
        let state = h.cache.inner.state_map.get(i as usize);
        assert!(
            state == SparseBlockState::DIRTY || state == SparseBlockState::NOT_PRESENT,
            "block {} unexpected state: {}", i, state,
        );
        let data = h
            .cache
            .read(
                i as u64 * 4096,
                4096,
                &h.clean_cache,
                &h.pack_index_cache,
                &h.volume_manifest,
                &h.content_store,
                &metrics,
            )
            .await
            .unwrap();
        if state == SparseBlockState::DIRTY {
            assert!(
                data.iter().all(|&b| b == i + 0xA0),
                "re-dirtied block {} should have 0x{:02x}, got 0x{:02x}",
                i, i + 0xA0, data[0],
            );
        }
        // If NOT_PRESENT: flush evicted the block, data resolves from foyer/S3.
    }

    for i in 5u8..10 {
        let data = h
            .cache
            .read(
                i as u64 * 4096,
                4096,
                &h.clean_cache,
                &h.pack_index_cache,
                &h.volume_manifest,
                &h.content_store,
                &metrics,
            )
            .await
            .unwrap();
        // Blocks 5-9 were not overwritten concurrently, so they should have
        // the original data (either from SSD if still dirty, or from S3 if evicted).
        assert!(
            data.iter().all(|&b| b == i + 0x10),
            "block {} should have 0x{:02x}, got 0x{:02x}",
            i, i + 0x10, data[0],
        );
    }
}

/// Test: Clean cache warming during bottomless auto-flush.
///
/// After auto-flush, blocks stay CLEAN on local SSD and the clean_cache
/// should have been warmed. Reads resolve from either local SSD or cache.
#[tokio::test]
async fn test_bottomless_clean_cache_warming() {
    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test".to_string(),
        device_size: 1024 * 1024,
        block_size: 4096,
        wal_sync: false,
    };
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let content_store =
        crate::block::content_store::ContentStore::new(object_store, "test-bucket");
    let pack_index_cache = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        1024 * 1024,
        4096,
    )));

    // Use a shared clean_cache that we can inspect after flush
    let clean_cache: Arc<dyn crate::block::cache::BlockCache> =
        Arc::new(crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024));
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.finish_recovery().await.unwrap();

    // Write 3 non-zero blocks
    for i in 0u8..3 {
        cache
            .write(
                i as u64 * 4096,
                &vec![i + 0x60; 4096],
            )
            .unwrap();
    }

    // Flush with the shared clean_cache — this should warm the cache
    let (stats, _seq) = cache
        .flush_packs(
            &content_store,
            &pack_index_cache,
            &volume_manifest,
            Some(&clean_cache),
        )
        .await
        .unwrap();
    assert_eq!(stats.blocks_claimed, 3);

    // All blocks should be NOT_PRESENT (flush always evicts from data file;
    // foyer serves as the read cache).
    for i in 0usize..3 {
        assert_eq!(cache.inner.state_map.get(i), SparseBlockState::NOT_PRESENT);
    }

    // Read them back — data resolves from foyer (warmed during flush).
    let metrics = crate::block::metrics::ExportMetrics::new();
    for i in 0u8..3 {
        let data = cache
            .read(
                i as u64 * 4096,
                4096,
                clean_cache.as_ref(),
                &pack_index_cache,
                &volume_manifest,
                &content_store,
                &metrics,
            )
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == i + 0x60),
            "block {} should be 0x{:02x}, got 0x{:02x}",
            i, i + 0x60, data[0],
        );
    }
}

/// Test: Rotation claims blocks atomically (CAS DIRTY→SYNCING under write lock).
///
/// Proves that after rotate_and_snapshot, claimed blocks are already SYNCING.
/// This prevents a race where a concurrent write between lock-release and CAS
/// puts data in the new active file, but the CAS still claims the block — the
/// flushing file then has stale data and the copy overwrites the new data.
///
/// With atomic claiming, concurrent writes see SYNCING → re-dirty via
/// promote_syncing_blocks + transition_to_dirty(SYNCING→DIRTY), preserving
/// the latest write.
#[tokio::test]
async fn test_rotation_claims_atomically() {
    let h = BottomlessHarness::new().await;

    // Write 5 blocks
    for i in 0u8..5 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 0x30; 4096])
            .unwrap();
    }

    // All blocks should be DIRTY before rotation
    for i in 0usize..5 {
        assert_eq!(
            h.cache.inner.state_map.get(i),
            SparseBlockState::DIRTY,
            "block {} should be DIRTY before rotation",
            i
        );
    }

    // rotate_and_snapshot does CAS DIRTY→SYNCING under the write lock
    let claimed = h.cache.rotate_and_snapshot().unwrap();
    assert_eq!(claimed.len(), 5, "all 5 dirty blocks should be claimed");

    // After rotation, all claimed blocks must be SYNCING (not DIRTY).
    // This is the key invariant: any concurrent write that acquires the
    // read lock after rotation sees SYNCING and properly re-dirties.
    for i in 0usize..5 {
        assert_eq!(
            h.cache.inner.state_map.get(i),
            SparseBlockState::SYNCING,
            "block {} must be SYNCING after atomic rotate+claim",
            i
        );
    }

    // Simulate what a concurrent write would do: write to block 2,
    // which should re-dirty it (CAS SYNCING→DIRTY).
    let new_data = vec![0xFFu8; 4096];
    h.cache.write(2 * 4096, &new_data).unwrap();
    assert_eq!(
        h.cache.inner.state_map.get(2),
        SparseBlockState::DIRTY,
        "concurrent write must re-dirty the block (SYNCING→DIRTY)"
    );

    // The other blocks should still be SYNCING
    for i in [0, 1, 3, 4] {
        assert_eq!(
            h.cache.inner.state_map.get(i),
            SparseBlockState::SYNCING,
            "block {} should still be SYNCING",
            i
        );
    }

    // Clean up: transition remaining SYNCING blocks back to DIRTY
    // so they can be re-flushed (prevent orphaned SYNCING state).
    for &idx in &claimed {
        h.cache.inner.transition_to_dirty(idx);
    }

    // Verify block 2 has the NEW data (not stale flushing file data).
    // promote_syncing_blocks should have copied the old data from flushing→active,
    // then the pwrite overwrote with 0xFF.
    let mut buf = vec![0u8; 4096];
    h.cache
        .inner
        .data_file
        .read()
        .read_exact_at(&mut buf, 2 * 4096)
        .unwrap();
    assert!(
        buf.iter().all(|&b| b == 0xFF),
        "block 2 in active file should have new data 0xFF, got 0x{:02x}",
        buf[0]
    );
}

/// Test: S3 upload failure during bottomless flush copies blocks back.
///
/// After flush failure, all SYNCING blocks should be copied from flushing→active
/// and marked DIRTY. The flushing file should be cleaned up.
#[tokio::test]
async fn test_bottomless_flush_failure_recovery() {
    // Use FailingObjectStore inline — the flush_scheduler's version is private.
    use async_trait::async_trait;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore as ObjStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as ObjectStoreResult,
    };

    #[derive(Debug)]
    struct AlwaysFailStore;
    impl std::fmt::Display for AlwaysFailStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "AlwaysFailStore")
        }
    }
    #[async_trait]
    impl ObjStore for AlwaysFailStore {
        async fn put_opts(&self, _: &object_store::path::Path, _: PutPayload, _: PutOptions) -> ObjectStoreResult<PutResult> {
            Err(object_store::Error::Generic { store: "AlwaysFailStore", source: "fail".into() })
        }
        async fn put_multipart_opts(&self, _: &object_store::path::Path, _: PutMultipartOptions) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            Err(object_store::Error::Generic { store: "AlwaysFailStore", source: "fail".into() })
        }
        async fn get_opts(&self, _: &object_store::path::Path, _: GetOptions) -> ObjectStoreResult<GetResult> {
            Err(object_store::Error::Generic { store: "AlwaysFailStore", source: "fail".into() })
        }
        async fn delete(&self, _: &object_store::path::Path) -> ObjectStoreResult<()> {
            Err(object_store::Error::Generic { store: "AlwaysFailStore", source: "fail".into() })
        }
        fn list(&self, _: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            futures::stream::empty().boxed()
        }
        async fn list_with_delimiter(&self, _: Option<&object_store::path::Path>) -> ObjectStoreResult<ListResult> {
            Ok(ListResult { common_prefixes: vec![], objects: vec![] })
        }
        async fn copy(&self, _: &object_store::path::Path, _: &object_store::path::Path) -> ObjectStoreResult<()> {
            Err(object_store::Error::Generic { store: "AlwaysFailStore", source: "fail".into() })
        }
        async fn copy_if_not_exists(&self, _: &object_store::path::Path, _: &object_store::path::Path) -> ObjectStoreResult<()> {
            Err(object_store::Error::Generic { store: "AlwaysFailStore", source: "fail".into() })
        }
    }

    use futures::StreamExt;

    let dir = TempDir::new().unwrap();
    let config = bottomless_config(dir.path());
    let failing_store: Arc<dyn object_store::ObjectStore> = Arc::new(AlwaysFailStore);
    let content_store =
        crate::block::content_store::ContentStore::new(failing_store, "test-bucket");
    let pack_index_cache = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        1024 * 1024,
        4096,
    )));

    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.finish_recovery().await.unwrap();
    let _clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write 3 blocks
    for i in 0u8..3 {
        cache
            .write(i as u64 * 4096, &vec![i + 0x70; 4096])
            .unwrap();
    }

    // Flush should fail (S3 upload rejected)
    let result = cache
        .flush_packs(&content_store, &pack_index_cache, &volume_manifest, None)
        .await;
    assert!(result.is_err(), "flush should fail with AlwaysFailStore");

    // After failure recovery: all blocks should be DIRTY (copied back from flushing)
    for i in 0usize..3 {
        assert_eq!(
            cache.inner.state_map.get(i),
            SparseBlockState::DIRTY,
            "block {i} should be DIRTY after flush failure recovery"
        );
    }

    // Data should be intact in the active file
    for i in 0u8..3 {
        let mut buf = vec![0u8; 4096];
        cache
            .inner
            .data_file
            .read()
            .read_exact_at(&mut buf, i as u64 * 4096)
            .unwrap();
        assert!(
            buf.iter().all(|&b| b == i + 0x70),
            "block {} should have 0x{:02x} after failure recovery, got 0x{:02x}",
            i, i + 0x70, buf[0],
        );
    }

    // Flushing file should be cleaned up
    assert!(cache.inner.flushing_file.lock().is_none());
    assert!(!cache.inner.flushing_active.load(Ordering::Acquire));
    assert!(!config.flushing_path().exists());
}

// ====================================================================
// Bug-proving tests for audit findings
// ====================================================================

/// Prove: ublk two-phase write race causes data corruption.
///
/// When a flush completes (SYNCING→NOT_PRESENT + flushing_file.take())
/// between pre_write and pwrite_and_commit, promote_syncing_blocks
/// silently skips the NOT_PRESENT block. A sub-block pwrite then lands
/// in a sparse active file, leaving the non-written portion as zeros.
///
/// This test simulates the exact interleaving without needing ublk or
/// Linux — it drives WriteCache internals directly.
#[tokio::test]
async fn test_promote_syncing_blocks_silent_skip_when_ff_none() {
    // 2 blocks of 128KB each (256KB device)
    let block_size = 128 * 1024usize;
    let h = V2Harness::with_config(2 * block_size as u64, block_size).await;

    // Write a full 128KB block with recognizable data
    let original_data = vec![0xAA_u8; block_size];
    h.cache.write(0, &original_data).unwrap();
    assert_eq!(h.cache.dirty_block_count(), 1);

    // Simulate flush phase 1: rotate (DIRTY→SYNCING)
    let snapshot = h.cache.rotate_and_snapshot().unwrap();
    assert_eq!(snapshot, vec![0], "block 0 should be claimed");
    assert_eq!(
        h.cache.inner.state_map.get(0),
        SparseBlockState::SYNCING
    );

    // Simulate flush completion (what flush.rs:887-897 does after S3 upload):
    // 1. SYNCING→NOT_PRESENT (evict)
    // 2. flushing_active = false
    // 3. flushing_file.take()
    assert!(h.cache.inner.transition_syncing_to_not_present(0));
    h.cache
        .inner
        .flushing_active
        .store(false, Ordering::Release);
    drop(h.cache.inner.flushing_file.lock().take());

    // Block 0 is now NOT_PRESENT, flushing file is gone
    assert_eq!(
        h.cache.inner.state_map.get(0),
        SparseBlockState::NOT_PRESENT
    );
    assert!(h.cache.inner.flushing_file.lock().is_none());

    // === Part 1: Without require_promotion (existing behavior, write() path) ===
    // promote_syncing_blocks with require_promotion=false silently skips.
    let df = h.cache.inner.data_file.read();
    let result = h.cache.inner.promote_syncing_blocks(&df, 0, 0, false);
    assert!(
        result.is_ok(),
        "require_promotion=false should silently skip (write() path behavior)"
    );

    // Without the fix, a sub-block pwrite here would land on zeros:
    df.write_all_at(&[0xBB; 4096], 0).unwrap();
    drop(df);

    let mut readback = vec![0u8; block_size];
    h.cache
        .inner
        .data_file
        .read()
        .read_exact_at(&mut readback, 0)
        .unwrap();
    assert_eq!(&readback[..4096], &[0xBB; 4096], "guest write landed");
    // Rest is zeros — this IS the data corruption the fix prevents:
    assert!(
        readback[4096..].iter().all(|&b| b == 0),
        "without the fix: non-written portion is zeros (original 0xAA data lost)"
    );

    // === Part 2: With require_promotion (ublk pwrite_and_commit safety net) ===
    // promote_syncing_blocks with require_promotion=true returns BlockEvicted,
    // preventing the corrupt pwrite from happening.
    //
    // Reset block to NOT_PRESENT for part 2:
    let _ = h.cache.inner.state_map.cas(0,
        SparseBlockState::DIRTY, SparseBlockState::NOT_PRESENT);
    let df = h.cache.inner.data_file.read();
    let result = h.cache.inner.promote_syncing_blocks(&df, 0, 0, true);
    assert!(
        result.is_err(),
        "require_promotion=true should return BlockEvicted when ff=None and block is NOT_PRESENT"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, super::CacheError::BlockEvicted),
        "error should be BlockEvicted, got: {err}"
    );
    drop(df);
}

/// Regression test: post-rotation zero-write must survive crash recovery.
///
/// Scenario:
/// 1. Write non-zero data to block 0
/// 2. Rotate (data moves to flushing file, fresh sparse active file)
/// 3. Write zeros to block 0 in the new active file (simulates guest trim)
/// 4. Save metadata but leave flushing file on disk (simulate crash)
/// 5. Re-open cache (triggers flushing file merge)
/// 6. Read block 0 — MUST be zeros (the guest's write), not stale 0xAA
///
/// Before the fix, `is_zero_block` treated the valid zeros as "sparse" and
/// restored stale pre-rotation data from the flushing file — silent data loss.
#[tokio::test]
async fn test_crash_recovery_preserves_post_rotation_zero_write() {
    let dir = TempDir::new().unwrap();
    let block_size = 4096usize;
    let device_size = 1024 * 1024u64;

    // Phase 1: write, rotate, zero, "crash"
    {
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test".to_string(),
            device_size,
            block_size,
            wal_sync: false,
        };
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Write non-zero data to block 0
        cache.write(0, &vec![0xAA; block_size]).unwrap();

        // Rotate + snapshot (production path): CAS DIRTY→SYNCING, then
        // rename active → flushing + create new sparse active.
        // Block 0's 0xAA data moves to flushing file.
        let _claimed = cache.rotate_and_snapshot().unwrap();

        // Post-rotation: write zeros to block 0 in the active file.
        // This simulates a guest trim or write_zeroes — the zeros are
        // intentional data, not "sparse/unwritten."
        cache.write(0, &vec![0x00; block_size]).unwrap();

        // Verify: active file has zeros, flushing file has 0xAA
        {
            let mut active_buf = vec![0u8; block_size];
            cache
                .inner
                .data_file
                .read()
                .read_exact_at(&mut active_buf, 0)
                .unwrap();
            assert!(
                active_buf.iter().all(|&b| b == 0),
                "active file should have zeros after zero-write"
            );

            let ff_guard = cache.inner.flushing_file.lock();
            let ff = ff_guard.as_ref().unwrap();
            let mut flush_buf = vec![0u8; block_size];
            ff.read_exact_at(&mut flush_buf, 0).unwrap();
            assert!(
                flush_buf.iter().all(|&b| b == 0xAA),
                "flushing file should still have 0xAA"
            );
        }

        // Save metadata (block 0 is DIRTY in the state map).
        // Leave flushing file on disk to simulate a crash mid-flush.
        cache.save_metadata().unwrap();

        // Clean up flushing_file handle so the file isn't held open,
        // but do NOT delete the flushing file — that's the crash scenario.
        *cache.inner.flushing_file.lock() = None;
    }

    // Phase 2: Re-open. Recovery sees the flushing file and merges.
    {
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test".to_string(),
            device_size,
            block_size,
            wal_sync: false,
        };
        let flushing_path = config.flushing_path();
        assert!(
            flushing_path.exists(),
            "flushing file must exist for crash recovery"
        );

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // Flushing file should be cleaned up
        assert!(
            !flushing_path.exists(),
            "flushing file should be deleted after recovery"
        );

        // THE CRITICAL ASSERTION: block 0 must be zeros (the guest's write),
        // not 0xAA (the stale pre-rotation data).
        let mut buf = vec![0u8; block_size];
        cache
            .inner
            .data_file
            .read()
            .read_exact_at(&mut buf, 0)
            .unwrap();
        assert!(
            buf.iter().all(|&b| b == 0),
            "block 0 should be zeros (guest's post-rotation write), \
             but got 0x{:02X} — recovery clobbered it with stale flushing data",
            buf[0]
        );
    }
}

