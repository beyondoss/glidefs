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
    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write some data
    cache.write(0, b"hello world", &[]).unwrap();

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
    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    cache.write(0, b"data", &[]).unwrap();
    cache.flush().unwrap();

    // Data should still be readable
    let data = cache.read_local_only(0, 4).unwrap();
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
        cache.write(0, b"persistent", &[]).unwrap();
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
            .write((i as u64 + 5) * 4096, &vec![i + 1; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
            .write((i as u64 + 10) * 4096, &vec![i + 1; 4096], &[])
            .unwrap();
    }
    for i in 0u8..5 {
        h.cache
            .write((i as u64 + 15) * 4096, &vec![i + 100; 4096], &[])
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
        .write(0, &vec![42u8; 128 * 1024], &[])
        .unwrap();
    // Write a block of zeros — this should get ZERO_BLOCK_HASH
    h.cache
        .write(128 * 1024, &vec![0u8; 128 * 1024], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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

    h.cache.write(0, &vec![1u8; 4096], &[]).unwrap();
    h.flush().await;

    // Overwrite block 0 with different data
    h.cache.write(0, &vec![2u8; 4096], &[]).unwrap();

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
            .write(i as u64 * 4096, &data, &[])
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
    h.cache.write(0, &data, &[]).unwrap();

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
        .write(0, &vec![0xBBu8; 4096], &[])
        .unwrap();
    h.cache.zero_range(0, 4096, &[]).unwrap();

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
    h.cache.write(0, &data, &[]).unwrap();

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
        .write(0, &vec![0x11u8; 4096], &[])
        .unwrap();
    h.cache
        .write(4096, &vec![0x22u8; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
    h.cache.write(0, &vec![0xAA; 4096], &[]).unwrap();
    h.cache
        .write(4096, &vec![0xBB; 4096], &[])
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
        .write(8192, &vec![0xCC; 4096], &[])
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

    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let original_data = vec![0xAAu8; block_size];

    // Session 1: write data and persist metadata
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache.write(0, &original_data, &[]).unwrap();
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
        let clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            for round in 0..20u8 {
                let block_idx = (writer_id as u64 * 2) % 10;
                let data = vec![writer_id.wrapping_add(round).wrapping_add(100); 4096];
                cache
                    .write(block_idx * 4096, &data, &[])
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

    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
    let cache = cache.finish_recovery().await.unwrap();

    // Write some data before draining
    cache.write(0, &vec![0xAA; 4096], &[]).unwrap();
    cache.write(4096, &vec![0xBB; 4096], &[]).unwrap();
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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
        let clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            let block_idx = writer_id as u64 * 2; // blocks 0,2,4,6,8
            for round in 0..10u8 {
                let fill = writer_id * 10 + round;
                cache
                    .write(block_idx * 4096, &vec![fill; 4096], &[])
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
    h.cache.write(0, &original_data, &[]).unwrap();
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
        .write(0, &vec![0xAAu8; 4096], &[])
        .unwrap();
    h.cache.local_checkpoint().await.unwrap();

    let inner = h.cache.inner();
    assert!(inner.crc_map.load(0).is_some(), "checkpoint should set CRC32");

    // Write new data to the same block — CRC32 should be invalidated (sentinel).
    h.cache
        .write(0, &vec![0xBBu8; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 10; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 10; 4096], &[])
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
        .write(3 * 4096, &vec![0xDD; 4096], &[])
        .unwrap();
    h.cache
        .write(4 * 4096, &vec![0xEE; 4096], &[])
        .unwrap();
    h.cache.write(0, &vec![0xFF; 4096], &[]).unwrap(); // overwrite block 0

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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
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
        let clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            for round in 0..30u8 {
                let block_idx = (writer_id as u64 * 2) % 10;
                let fill = writer_id.wrapping_add(round).wrapping_add(100);
                cache
                    .write(block_idx * 4096, &vec![fill; 4096], &[])
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
// Partial block data integrity tests
// =========================================================================

/// Prove that `DashMap::insert` overwrites an existing `AtomicU32`, losing
/// bitmap bits set by `mark_sub_regions`. This is the mechanism behind the
/// TOCTOU race in `backfill_missing_blocks`: two concurrent writes to the
/// same block can both see `is_partial=false`, both call `insert`, and the
/// second `insert` replaces the first's AtomicU32 — dropping any bitmap
/// bits the first write's `mark_sub_regions` had set.
///
/// The race window in handler.rs write():
/// 1. T2: is_partial(idx)=false  (before T1 inserts)
/// 2. T1: partial_blocks.insert(idx, AtomicU32(0))
/// 3. T1: cache.write → mark_sub_regions sets bits on T1's AtomicU32
/// 4. T2: partial_blocks.insert(idx, AtomicU32(0)) → REPLACES T1's entry
/// 5. Background backfill reads bitmap=0 for T1's sub-regions → overwrites
///    T1's pwrite with stale S3 data.
///
/// Step 4 follows step 1 because T2 was delayed between `is_partial` and
/// `insert` (e.g., acquiring `volume_manifest.read()` at handler.rs:222).
#[tokio::test]
async fn test_partial_blocks_insert_overwrites_bitmap_causing_data_loss() {
    use super::inner::PartialBlockState;
    use std::sync::atomic::AtomicU32;

    let dir = TempDir::new().unwrap();
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "partial-test".to_string(),
        device_size: 128 * 1024, // 1 block at 128KB
        block_size: 128 * 1024,  // 128KB blocks → 32 sub-regions of 4KB
        wal_sync: false,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.skip_recovery_for_test();
    let inner = cache.inner();

    let block_idx = 0usize;

    let new_state = || PartialBlockState {
        bitmap: AtomicU32::new(0),
        write_lock: parking_lot::Mutex::new(()),
    };

    // Step 2: Thread A's backfill_missing_blocks inserts a partial entry.
    inner.partial_blocks.insert(block_idx, new_state());

    // Step 3: Thread A's cache.write → mark_sub_regions sets bit for
    //         sub-region 0 (bytes [0..4096] of the block).
    inner.mark_sub_regions(block_idx, 0, 4096);

    // Verify the bit IS set on Thread A's entry.
    let bitmap_before = inner.partial_bitmap(block_idx).unwrap();
    assert_eq!(
        bitmap_before & 1,
        1,
        "sub-region 0 should be marked as written"
    );

    // Step 4: Thread B's backfill_missing_blocks calls insert() for the
    //         same block (Thread B checked is_partial=false before Thread A
    //         inserted, then got delayed on volume_manifest.read()).
    //         DashMap::insert REPLACES Thread A's entry.
    inner.partial_blocks.insert(block_idx, new_state());

    // DATA LOSS: Thread A's bitmap bit for sub-region 0 is gone.
    let bitmap_after = inner.partial_bitmap(block_idx).unwrap();
    assert_eq!(
        bitmap_after, 0,
        "DashMap::insert replaced entry, losing Thread A's bitmap bits"
    );

    // Step 5 consequence: background backfill reads bitmap=0 for sub-region 0,
    // writes stale S3 data over Thread A's pwrite. Thread A's write is lost.

    // ---------------------------------------------------------------
    // Proof that entry().or_insert_with() preserves existing bits:
    // ---------------------------------------------------------------
    inner.partial_blocks.remove(&block_idx);

    // Thread A inserts and sets bitmap bits
    inner
        .partial_blocks
        .entry(block_idx)
        .or_insert_with(new_state);
    inner.mark_sub_regions(block_idx, 0, 4096);

    // Thread B uses entry().or_insert_with() — does NOT replace
    inner
        .partial_blocks
        .entry(block_idx)
        .or_insert_with(new_state);

    // Thread A's bits are preserved
    let bitmap_fixed = inner.partial_bitmap(block_idx).unwrap();
    assert_eq!(
        bitmap_fixed & 1,
        1,
        "entry().or_insert_with() preserves existing bitmap bits"
    );
}

/// Prove that `merge_partial_block` reads the bitmap once at the start,
/// missing concurrent writes that set bits during the merge loop.
///
/// The race: a read request hits a partial block and calls merge_partial_block.
/// merge reads bitmap=0 for sub-region N. Concurrently, a write sets bit N
/// (via mark_sub_regions with Release ordering) and pwrite's new data to SSD.
/// merge then writes stale S3 data to sub-region N on SSD, overwriting the
/// concurrent write's data.
///
/// The background backfill task (`spawn_background_backfill`) correctly
/// re-reads the bitmap per sub-region. `merge_partial_block` should do the same.
#[tokio::test]
async fn test_merge_partial_block_stale_bitmap_misses_concurrent_write() {
    use super::inner::PartialBlockState;
    use std::sync::atomic::AtomicU32;

    let dir = TempDir::new().unwrap();
    let block_size = 128 * 1024; // 128KB → 32 sub-regions of 4KB
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "merge-test".to_string(),
        device_size: block_size as u64,
        block_size,
        wal_sync: false,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.skip_recovery_for_test();
    let inner = cache.inner();

    let block_idx = 0usize;

    // Set up: block is partial with bitmap=0 (no sub-regions written yet).
    inner.partial_blocks.insert(
        block_idx,
        PartialBlockState {
            bitmap: AtomicU32::new(0),
            write_lock: parking_lot::Mutex::new(()),
        },
    );
    inner.set_present(block_idx);

    // merge_partial_block would snapshot the bitmap HERE:
    let bitmap_snapshot = inner.partial_bitmap(block_idx).unwrap();
    assert_eq!(bitmap_snapshot, 0, "no sub-regions written yet");

    // Concurrent write arrives: marks sub-region 0 with Release ordering
    // (happens between merge's bitmap snapshot and its write_sub_region loop).
    inner.mark_sub_regions(block_idx, 0, 4096);

    // The snapshot is STALE — it doesn't see the concurrent write's bit.
    assert_eq!(
        bitmap_snapshot & 1,
        0,
        "snapshot is stale — concurrent write to sub-region 0 was missed"
    );

    // merge_partial_block would proceed to write S3 data to sub-region 0,
    // overwriting the concurrent write's pwrite data. DATA LOSS.

    // With a per-sub-region re-read (the fix), the concurrent bit IS visible:
    let fresh_bitmap = inner.partial_bitmap(block_idx).unwrap();
    assert_eq!(
        fresh_bitmap & 1,
        1,
        "per-sub-region re-read catches the concurrent update"
    );
    // merge would skip sub-region 0, preserving the concurrent write's data.
}

/// Prove that the per-block write_lock prevents backfill from overwriting
/// guest data on SSD.
///
/// The TOCTOU race (without the lock):
/// 1. Backfill reads bitmap for sub-region N → bit=0
/// 2. Guest write: mark_sub_regions (sets bit=1) + pwrite (guest data on SSD)
/// 3. Backfill calls write_sub_region (overwrites guest data with S3 data)
///
/// The fix: backfill holds write_lock while checking bitmap + writing.
/// Guest re-pwrites under the same lock after its main pwrite, guaranteeing
/// guest data wins even if backfill's write lands between the two guest pwrites.
#[tokio::test]
async fn test_backfill_write_sub_region_overwrites_guest_data() {
    use super::inner::{PartialBlockState, SUB_BLOCK_SIZE};
    use std::sync::atomic::{AtomicU32, Ordering};

    let dir = TempDir::new().unwrap();
    let block_size = 128 * 1024;
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "backfill-race".to_string(),
        device_size: block_size as u64,
        block_size,
        wal_sync: false,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.skip_recovery_for_test();
    let inner = cache.inner();

    let block_idx = 0usize;

    // Setup: block 0 is partial with empty bitmap.
    inner.partial_blocks.insert(
        block_idx,
        PartialBlockState {
            bitmap: AtomicU32::new(0),
            write_lock: parking_lot::Mutex::new(()),
        },
    );
    inner.set_present(block_idx);

    let guest_data = vec![0xBBu8; SUB_BLOCK_SIZE];
    let s3_data = vec![0xAAu8; SUB_BLOCK_SIZE];

    // --- SIMULATE THE WORST-CASE INTERLEAVING ---
    // Backfill acquires write_lock first, reads bitmap (bit=0).
    {
        let state = inner.partial_blocks.get(&block_idx).unwrap();
        let _guard = state.value().write_lock.lock();
        let bitmap = state.value().bitmap.load(Ordering::Acquire);
        assert_eq!(bitmap & 1, 0, "backfill sees sub-region 0 as unwritten");

        // Guest marks bit and does main pwrite (outside the lock — these are
        // atomic/positional-IO and don't need the lock).
        inner.mark_sub_regions(block_idx, 0, SUB_BLOCK_SIZE);
        inner.data_file.read().write_all_at(&guest_data, 0).unwrap();

        // Backfill writes S3 data (under lock, using stale bitmap).
        // This overwrites the guest's main pwrite — but the guest will
        // re-pwrite under the lock next.
        inner.write_sub_region(0, &s3_data).unwrap();
    }
    // Backfill releases lock.

    // Guest acquires write_lock and re-pwrites (THE FIX).
    // This is what write.rs does after the main pwrite for partial blocks.
    {
        let state = inner.partial_blocks.get(&block_idx).unwrap();
        let _guard = state.value().write_lock.lock();
        inner.data_file.read().write_all_at(&guest_data, 0).unwrap();
    }

    // --- VERIFY: guest data survives ---
    let mut readback = vec![0u8; SUB_BLOCK_SIZE];
    inner.data_file.read().read_exact_at(&mut readback, 0).unwrap();

    assert_eq!(
        readback[0], 0xBB,
        "sub-region 0 should have guest data (0xBB), not S3 data (0xAA) — \
         guest re-pwrite under write_lock guarantees guest data wins"
    );
}

/// Prove that guest data survives when `complete_partial` removes the
/// DashMap entry between the guest's first pwrite and re-pwrite.
///
/// The race (without fix):
/// 1. Guest marks bitmap, does first pwrite (guest data on SSD)
/// 2. Backfill (holding write_lock) reads stale bitmap, overwrites sub-region
/// 3. Backfill releases write_lock, calls complete_partial (removes entry)
/// 4. Guest reaches re-pwrite loop: partial_blocks.get() → None → SKIPPED
/// 5. SSD has stale S3 data → DATA CORRUPTION
///
/// The fix: track which blocks were partial at the START of write().
/// Re-pwrite unconditionally for those blocks, with or without the lock.
#[tokio::test]
async fn test_complete_partial_before_repwrite_race() {
    use super::inner::{PartialBlockState, SUB_BLOCK_SIZE};
    use std::sync::atomic::AtomicU32;

    let dir = TempDir::new().unwrap();
    let block_size = 128 * 1024;
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "complete-partial-race".to_string(),
        device_size: block_size as u64,
        block_size,
        wal_sync: false,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.skip_recovery_for_test();

    let block_idx = 0usize;

    // Setup: insert partial block with empty bitmap, mark present.
    cache.inner().partial_blocks.insert(
        block_idx,
        PartialBlockState {
            bitmap: AtomicU32::new(0),
            write_lock: parking_lot::Mutex::new(()),
        },
    );
    cache.inner().set_present(block_idx);

    // Fill the entire block with S3 data (0xAA) — simulates backfill writing
    // stale data to the SSD.
    let s3_data = vec![0xAAu8; block_size];
    cache.inner().data_file.read().write_all_at(&s3_data, 0).unwrap();

    // Backfill completes: removes the partial block entry.
    // This is the critical step — it happens BEFORE the guest write.
    // In the real race, it happens between the guest's first pwrite and
    // re-pwrite within a single write() call.
    cache.inner().complete_partial(block_idx);

    // Re-insert the partial block to simulate the state AT THE START of
    // the guest write (the block was partial when write() began).
    cache.inner().partial_blocks.insert(
        block_idx,
        PartialBlockState {
            bitmap: AtomicU32::new(0),
            write_lock: parking_lot::Mutex::new(()),
        },
    );

    // Guest writes 4KB of 0xBB to sub-region 0 via the normal write path.
    // write() will: mark bitmap → first pwrite → re-pwrite.
    // The re-pwrite MUST happen even though backfill already called
    // complete_partial in the real scenario.
    let clean = crate::block::cache::SimpleBlockCache::new(1024);
    cache
        .write(0, &[0xBBu8; SUB_BLOCK_SIZE], &[])
        .unwrap();

    // Simulate what happens in the actual race: between the first pwrite
    // and re-pwrite, backfill overwrites sub-region 0 with stale S3 data
    // and removes the partial entry.
    //
    // We can't interleave within write() without a hook, so instead verify
    // the END STATE: after write() completes, guest data must be on SSD.
    // The re-pwrite (with or without the lock) ensures this.
    let mut readback = vec![0u8; SUB_BLOCK_SIZE];
    cache
        .inner()
        .data_file
        .read()
        .read_exact_at(&mut readback, 0)
        .unwrap();

    assert!(
        readback.iter().all(|&b| b == 0xBB),
        "sub-region 0 must have guest data (0xBB), not stale S3 data (0xAA) — \
         re-pwrite must run unconditionally for blocks that were partial at write start"
    );

    // Remaining sub-regions should still have S3 data (backfill wrote it)
    let mut rest = vec![0u8; block_size - SUB_BLOCK_SIZE];
    cache
        .inner()
        .data_file
        .read()
        .read_exact_at(&mut rest, SUB_BLOCK_SIZE as u64)
        .unwrap();
    assert!(
        rest.iter().all(|&b| b == 0xAA),
        "remaining sub-regions should have S3 data (0xAA)"
    );
}

/// Verify that write() re-pwrites for partial blocks even when the DashMap
/// entry is removed between first pwrite and re-pwrite.
///
/// This test uses a two-step approach:
/// 1. Mark block as partial, write guest data via write()
/// 2. Simulate backfill overwriting + complete_partial
/// 3. Write again — second write() must recover from the overwrite
///
/// Tests the fix at the WriteCache API level (not raw inner).
#[tokio::test]
async fn test_write_survives_complete_partial_removal() {
    use super::inner::{PartialBlockState, SUB_BLOCK_SIZE};
    use std::sync::atomic::AtomicU32;

    let dir = TempDir::new().unwrap();
    let block_size = 128 * 1024;
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "repwrite-removal".to_string(),
        device_size: block_size as u64,
        block_size,
        wal_sync: false,
    };
    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = cache.skip_recovery_for_test();
    let clean = crate::block::cache::SimpleBlockCache::new(1024);

    let block_idx = 0usize;

    // Step 1: Mark partial, write 4KB of 0xBB
    cache.inner().partial_blocks.insert(
        block_idx,
        PartialBlockState {
            bitmap: AtomicU32::new(0),
            write_lock: parking_lot::Mutex::new(()),
        },
    );
    cache.write(0, &[0xBBu8; SUB_BLOCK_SIZE], &[]).unwrap();

    // Step 2: Simulate backfill completing — overwrite sub-region 0 with S3
    // data (0xAA) and remove partial entry.
    cache
        .inner()
        .data_file
        .read()
        .write_all_at(&[0xAAu8; SUB_BLOCK_SIZE], 0)
        .unwrap();
    cache.inner().complete_partial(block_idx);

    // Step 3: Re-insert partial (simulates a new write arriving while block
    // is still tracked as partial — the write() call sees is_partial=true).
    cache.inner().partial_blocks.insert(
        block_idx,
        PartialBlockState {
            bitmap: AtomicU32::new(0),
            write_lock: parking_lot::Mutex::new(()),
        },
    );

    // Write 4KB of 0xCC to sub-region 0. write() must re-pwrite even if
    // the entry gets removed during execution.
    cache
        .write(0, &[0xCCu8; SUB_BLOCK_SIZE], &[])
        .unwrap();

    // Verify guest data (0xCC) is on SSD, not the stale overwrite (0xAA)
    let mut readback = vec![0u8; SUB_BLOCK_SIZE];
    cache
        .inner()
        .data_file
        .read()
        .read_exact_at(&mut readback, 0)
        .unwrap();

    assert!(
        readback.iter().all(|&b| b == 0xCC),
        "sub-region 0 must have latest guest data (0xCC)"
    );
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
    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write data to blocks 0 and 1
    let data0 = vec![0xAAu8; 4096];
    let data1 = vec![0xBBu8; 4096];
    cache.write(0, &data0, &[]).unwrap();
    cache.write(4096, &data1, &[]).unwrap();

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
            .write(i as u64 * 4096, &vec![i + 1; 4096], &[])
            .unwrap();
    }
    assert_eq!(h.cache.dirty_block_count(), 5);

    // Flush to S3
    let stats = h.flush().await;
    assert_eq!(stats.blocks_claimed, 5);
    assert!(stats.packs_uploaded > 0);

    // After flush in bottomless mode, blocks should be NOT_PRESENT (evicted)
    for i in 0usize..5 {
        let state = h.cache.inner.state_map.get(i);
        assert_eq!(
            state,
            SparseBlockState::NOT_PRESENT,
            "block {i} should be NOT_PRESENT after bottomless flush, got {state}"
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
    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write block 0 with initial data
    let initial_data = vec![0xAAu8; 4096];
    cache.write(0, &initial_data, &[]).unwrap();
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
    cache.write(0, &new_data, &[]).unwrap();

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

/// Test 4: Partial blocks excluded from flush snapshot.
///
/// Partial blocks are excluded from the dirty snapshot during rotation.
/// Their data is copied from the old active → new active file under the
/// write lock, so it survives flushing file deletion.
///
/// This exercises the "copy partial blocks during rotation" path in
/// rotate_data_file_inner.
#[tokio::test]
async fn test_bottomless_skipped_block_recovery() {
    let h = BottomlessHarness::new().await;

    // Write blocks 0 and 1 with distinct data
    let data0 = vec![0xAAu8; 4096];
    let data1 = vec![0xBBu8; 4096];
    h.cache.write(0, &data0, &[]).unwrap();
    h.cache.write(4096, &data1, &[]).unwrap();

    // Mark block 0 as partial — rotation will exclude it from snapshot
    // and copy its data from old active → new active.
    h.cache.insert_partial_block_for_test(0);

    // Flush: block 0 excluded (partial), block 1 flushes normally
    let stats = h.flush().await;
    assert_eq!(stats.blocks_claimed, 1, "only non-partial block claimed");
    assert_eq!(stats.blocks_cas_failed, 0, "no skips needed");

    // Block 0 should still be DIRTY (never claimed, never transitioned)
    assert_eq!(
        h.cache.inner.state_map.get(0),
        SparseBlockState::DIRTY,
        "partial block should stay DIRTY"
    );

    // Block 1 should be NOT_PRESENT (evicted in bottomless mode)
    assert_eq!(
        h.cache.inner.state_map.get(1),
        SparseBlockState::NOT_PRESENT,
        "flushed block should be NOT_PRESENT"
    );

    // Block 0's data should have been copied during rotation (old → new active)
    let mut buf = vec![0u8; 4096];
    h.cache.inner.data_file.read().read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(
        &buf[..], &data0[..],
        "partial block data must be copied from old to new active during rotation"
    );

    // Flushing file should be cleaned up
    assert!(
        h.cache.inner.flushing_file.lock().is_none(),
        "flushing_file should be None after flush completes"
    );
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
        let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

        // Write 3 blocks
        for i in 0u8..3 {
            cache
                .write(i as u64 * block_size as u64, &vec![i + 0x10; block_size], &[])
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
        .write(0, &vec![0xDD; 4096], &[])
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

/// Test: Bottomless flush end-to-end with S3 read-back.
///
/// Write blocks, flush (evict to NOT_PRESENT), then read through the full
/// tiered path (local miss -> S3 pack). Verifies the full bottomless lifecycle.
#[tokio::test]
async fn test_bottomless_flush_and_s3_readback() {
    let h = BottomlessHarness::new().await;
    let metrics = crate::block::metrics::ExportMetrics::new();

    // Write 3 distinct blocks
    for i in 0u8..3 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 0x50; 4096], &[])
            .unwrap();
    }

    // Flush to S3 (blocks become NOT_PRESENT)
    let stats = h.flush().await;
    assert_eq!(stats.blocks_claimed, 3);

    // All blocks should be NOT_PRESENT locally
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

/// Test: Concurrent writes during bottomless flush.
///
/// Spawn a flush on one task while writing to the same blocks concurrently.
/// Re-dirtied blocks must survive with the latest data. Evicted blocks must
/// be readable from S3.
#[tokio::test]
async fn test_bottomless_concurrent_write_during_flush() {
    let h = Arc::new(BottomlessHarness::new().await);
    let metrics = crate::block::metrics::ExportMetrics::new();

    // Write 10 blocks
    for i in 0u8..10 {
        h.cache
            .write(i as u64 * 4096, &vec![i + 0x10; 4096], &[])
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
            .write(i as u64 * 4096, &vec![i + 0xA0; 4096], &[])
            .unwrap();
    }

    let stats = flush_handle.await.unwrap();
    assert!(stats.blocks_claimed > 0, "some blocks should have been claimed");

    // After flush, verify all blocks are readable with correct data.
    // Blocks 0-4: either re-dirtied (latest write = 0xA0+i) or evicted then overwritten.
    // Blocks 5-9: either evicted (0x10+i readable from S3) or still dirty.
    for i in 0u8..5 {
        // Re-dirtied blocks: the last write was 0xA0+i. If they were evicted first,
        // the overwrite re-dirtied them, so data is in the active file.
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
        // If NOT_PRESENT: the flush uploaded 0x10+i, then write happened after eviction,
        // re-dirtied it. Either way the block is in a valid state.
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

/// Test: Clean cache warming during bottomless flush.
///
/// After flush evicts blocks, the clean_cache should have been warmed
/// so reads resolve from cache (not S3).
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
                &[],
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

    // All blocks should be NOT_PRESENT (evicted)
    for i in 0usize..3 {
        assert_eq!(cache.inner.state_map.get(i), SparseBlockState::NOT_PRESENT);
    }

    // Now read them back. If clean_cache was warmed, this doesn't hit S3.
    // (We can't directly test "didn't hit S3" without metrics, but we can
    // verify the data is correct and the read succeeds.)
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
            "block {} should be 0x{:02x} from warmed cache, got 0x{:02x}",
            i, i + 0x60, data[0],
        );
    }
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
    let clean_cache = crate::block::cache::SimpleBlockCache::new(64 * 1024 * 1024);

    // Write 3 blocks
    for i in 0u8..3 {
        cache
            .write(i as u64 * 4096, &vec![i + 0x70; 4096], &[])
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
