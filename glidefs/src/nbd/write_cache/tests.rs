use super::*;
use crate::nbd::block_map::blake3_128;
use crate::nbd::state::{Active, Initializing};
use bytes::Bytes;
use object_store::memory::InMemory;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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
    let clean_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);

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
    let clean_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);

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
    let clean_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);

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
    assert!(super::inner::is_zero_block(&zeros), "all zeros should return true");

    let mut non_zeros = vec![0u8; 4096];
    non_zeros[0] = 1;
    assert!(!super::inner::is_zero_block(&non_zeros), "first byte non-zero");

    non_zeros[0] = 0;
    non_zeros[4095] = 1;
    assert!(!super::inner::is_zero_block(&non_zeros), "last byte non-zero");

    non_zeros[4095] = 0;
    non_zeros[2048] = 1;
    assert!(!super::inner::is_zero_block(&non_zeros), "middle byte non-zero");

    // Empty slice
    assert!(super::inner::is_zero_block(&[]), "empty slice is 'all zeros'");
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
    content_store: crate::nbd::content_store::ContentStore,
    pack_index: crate::nbd::pack_index::HostPackIndex,
    clean_cache: crate::nbd::cache::SimpleBlockCache,
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
        let content_store = crate::nbd::content_store::ContentStore::new(object_store, "test-bucket");
        let pack_index = crate::nbd::pack_index::HostPackIndex::open(dir.path().join("pack_index.redb")).unwrap();
        let clean_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        Self { cache, content_store, pack_index, clean_cache, dir }
    }

    /// Flush and return stats.
    async fn flush(&self) -> FlushStats {
        let result: Result<FlushStats, CacheError> = self.cache
            .flush_to_s3(&self.content_store, &self.pack_index)
            .await;
        result.unwrap()
    }

    /// v2 read through the full tiered resolution path.
    async fn read(&self, offset: u64, len: usize) -> Bytes {
        let metrics = crate::nbd::metrics::ExportMetrics::new();
        let result: Result<Bytes, CacheError> = self.cache
            .read_v2(
                offset,
                len,
                &self.clean_cache,
                &self.pack_index,
                &self.content_store,
                &metrics,
            )
            .await;
        result.unwrap()
    }

    /// Get the manifest from S3.
    async fn manifest(&self) -> crate::nbd::manifest::Manifest {
        let bytes = self.content_store
            .get_manifest("test")
            .await
            .unwrap()
            .expect("manifest should exist");
        crate::nbd::manifest::Manifest::deserialize(&bytes).unwrap()
    }
}

#[tokio::test]
async fn test_flush_end_to_end() {
    let h = V2Harness::new().await;

    // Write 10 distinct blocks (each 4KB = block_size)
    for i in 0u8..10 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 10);
    assert_eq!(stats.blocks_deduped, 0);
    assert!(stats.packs_uploaded > 0);
    assert!(stats.bytes_uploaded > 0);

    let manifest = h.manifest().await;
    assert_eq!(manifest.name, "test");
    assert_eq!(manifest.chunk_size, 4096);
    assert_eq!(manifest.block_map.len(), 10);
    assert!(!manifest.pack_index.is_empty());
    assert!(h.pack_index.len().unwrap() >= 10);
}

#[tokio::test]
async fn test_flush_dedup_skips_existing() {
    let h = V2Harness::new().await;

    // Write 5 blocks
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    let stats1 = h.flush().await;
    assert_eq!(stats1.blocks_flushed, 5);
    assert_eq!(stats1.blocks_deduped, 0);
    assert!(stats1.packs_uploaded > 0);

    // Write the same data again to new offsets — same content, new positions
    for i in 0u8..5 {
        h.cache.write((i as u64 + 5) * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
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
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    let stats1 = h.flush().await;
    assert_eq!(stats1.blocks_flushed, 10);
    assert_eq!(stats1.blocks_deduped, 0);

    // Write 10 more blocks: 5 with SAME data as before (dedup), 5 with NEW data
    for i in 0u8..5 {
        h.cache.write((i as u64 + 10) * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }
    for i in 0u8..5 {
        h.cache.write((i as u64 + 15) * 4096, &vec![i + 100; 4096], &h.clean_cache).unwrap();
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
    h.cache.write(0, &vec![42u8; 128 * 1024], &h.clean_cache).unwrap();
    // Write a block of zeros — this should get ZERO_BLOCK_HASH
    h.cache.write(128 * 1024, &vec![0u8; 128 * 1024], &h.clean_cache).unwrap();

    let stats = h.flush().await;
    assert_eq!(stats.blocks_flushed, 2);
    assert_eq!(stats.blocks_deduped, 1, "zero block should be deduped");
    assert_eq!(stats.packs_uploaded, 1, "only one pack for the real block");
}

#[tokio::test]
async fn test_flush_clears_dirty_state() {
    let h = V2Harness::new().await;

    for i in 0u8..3 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
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
    assert_eq!(stats.blocks_flushed, 1, "overwritten block should be flushed");
}

#[tokio::test]
async fn test_flush_manifest_self_contained() {
    let h = V2Harness::new().await;

    // Write 30 blocks (will produce 2 packs: 25 + 5)
    for i in 0u8..30 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    let stats = h.flush().await;
    assert_eq!(stats.packs_uploaded, 2, "should produce 2 packs (25+5)");

    let manifest = h.manifest().await;
    let pack_hashes: std::collections::HashSet<_> =
        manifest.pack_index.iter().map(|e| e.hash).collect();

    for bm_entry in &manifest.block_map {
        if !bm_entry.hash.is_zero() {
            assert!(
                pack_hashes.contains(&bm_entry.hash),
                "block map hash {:?} should have pack index entry",
                bm_entry.hash
            );
        }
    }
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
    assert!(got.iter().all(|&b| b == 0), "unwritten block should be all zeros");
}

#[tokio::test]
async fn test_v2_read_trimmed_block() {
    let h = V2Harness::new().await;

    // Write a block, then zero it out.
    h.cache.write(0, &vec![0xBBu8; 4096], &h.clean_cache).unwrap();
    h.cache.zero_range(0, 4096).unwrap();

    let got = h.read(0, 4096).await;
    assert!(got.iter().all(|&b| b == 0), "trimmed block should be all zeros");
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
    h.cache.write(0, &vec![0x11u8; 4096], &h.clean_cache).unwrap();
    h.cache.write(4096, &vec![0x22u8; 4096], &h.clean_cache).unwrap();

    // Read across the chunk boundary: last 100 bytes of chunk 0 + first 100 of chunk 1.
    let got = h.read(3996, 200).await;
    assert_eq!(got.len(), 200);
    assert!(got[..100].iter().all(|&b| b == 0x11), "first 100 bytes from chunk 0");
    assert!(got[100..].iter().all(|&b| b == 0x22), "last 100 bytes from chunk 1");
}

#[tokio::test]
async fn test_v2_read_from_s3_pack() {
    use crate::nbd::cache::BlockCache;

    let h = V2Harness::new().await;

    // Write blocks, flush to S3, clear clean_cache so reads go to S3.
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }
    h.flush().await;

    // Clear clean_cache so reads must resolve through S3 packs.
    for i in 0u8..5 {
        let hash = crate::nbd::block_map::blake3_128(&vec![i + 1; 4096]);
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

#[tokio::test]
async fn test_v2_pack_prefetch_warms_siblings() {
    use crate::nbd::cache::BlockCache;

    let h = V2Harness::new().await;

    // Write 25 blocks (1 full pack).
    for i in 0u8..25 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }
    h.flush().await;

    // Clear clean_cache so prefetch has to fetch from S3.
    for i in 0u8..25 {
        let hash = crate::nbd::block_map::blake3_128(&vec![i + 1; 4096]);
        h.clean_cache.remove(&hash);
    }

    // Prefetch block 0 — fetches entire pack, caches all 25 blocks.
    h.cache
        .prefetch_chunk(0, &h.clean_cache, &h.pack_index, &h.content_store)
        .await
        .unwrap();

    // All 25 blocks should now be in the clean cache.
    for i in 0u8..25 {
        let hash = {
            let (hash, _) = h.cache.inner.block_map_get(i as usize);
            hash
        };
        assert!(
            h.clean_cache.get(&hash).await.is_some(),
            "sibling block {} should be cached from pack prefetch",
            i
        );
    }

    // Reads should resolve from clean cache (no additional S3 fetch).
    for i in 0u8..25 {
        let got = h.read(i as u64 * 4096, 4096).await;
        assert!(
            got.iter().all(|&b| b == i + 1),
            "block {} should contain 0x{:02x}",
            i,
            i + 1
        );
    }
}

#[tokio::test]
async fn test_v2_mixed_dirty_and_clean_reads() {
    use crate::nbd::cache::BlockCache;

    let h = V2Harness::new().await;

    // Write 5 blocks and flush to S3 (they'll become "clean" once evicted from cache).
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }
    h.flush().await;

    // Clear clean_cache for flushed blocks so they must come from S3.
    for i in 0u8..5 {
        let hash = crate::nbd::block_map::blake3_128(&vec![i + 1; 4096]);
        h.clean_cache.remove(&hash);
    }

    // Write 5 more blocks (dirty, not flushed).
    for i in 5u8..10 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
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
    use crate::nbd::cache::{BlockCache, SimpleBlockCache};
    use crate::nbd::block_map::blake3_128;

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
    assert!(cache.get(&h1).await.is_none(), "second oldest should be evicted");
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
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    let result: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();

    assert!(result.sequence > 0, "sequence should be > 0 after writes");
    assert_eq!(result.stats.blocks_flushed, 5);
    assert!(result.stats.packs_uploaded > 0);

    // Manifest should exist in S3
    let manifest = h.manifest().await;
    assert_eq!(manifest.name, "test");
    assert_eq!(manifest.block_map.len(), 5);
}

#[tokio::test]
async fn test_snapshot_clears_dirty_state() {
    let h = V2Harness::new().await;

    // Write blocks and snapshot
    for i in 0u8..3 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    let result1: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(result1.stats.blocks_flushed, 3);

    // Second snapshot with no new writes should be a no-op
    let result2: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(result2.stats.blocks_flushed, 0, "no dirty blocks after snapshot");
}

#[tokio::test]
async fn test_snapshot_captures_concurrent_writes() {
    let h = V2Harness::new().await;

    // Write blocks at different times
    h.cache.write(0, &vec![0xAA; 4096], &h.clean_cache).unwrap();
    h.cache.write(4096, &vec![0xBB; 4096], &h.clean_cache).unwrap();

    let result: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(result.stats.blocks_flushed, 2);

    // Write more after snapshot
    h.cache.write(8192, &vec![0xCC; 4096], &h.clean_cache).unwrap();

    // Manifest from first snapshot should have 2 blocks, not 3
    let manifest = h.manifest().await;
    assert_eq!(manifest.block_map.len(), 2);

    // Second snapshot picks up the new write
    let result2: SnapshotResult = h.cache
        .snapshot(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(result2.stats.blocks_flushed, 1);
}

// ========================================================================
// open_from_manifest tests
// ========================================================================

fn make_test_manifest(num_entries: u64, device_size: u64, block_size: u32) -> crate::nbd::manifest::Manifest {
    use crate::nbd::manifest::ManifestBlockEntry;
    let block_map: Vec<ManifestBlockEntry> = (0..num_entries)
        .map(|i| ManifestBlockEntry {
            chunk_index: i,
            hash: blake3_128(format!("block-{i}").as_bytes()),
            flags: 0,
        })
        .collect();
    crate::nbd::manifest::Manifest {
        name: "test-fork".to_string(),
        sequence: 42,
        chunk_size: block_size,
        device_size,
        block_map,
        pack_index: vec![],
    }
}

#[test]
fn test_open_from_manifest_creates_clean_cache() {
    let dir = TempDir::new().unwrap();
    let device_size: u64 = 1024 * 1024; // 1MB
    let block_size: usize = 4096;
    let manifest = make_test_manifest(5, device_size, block_size as u32);

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test-manifest".to_string(),
        device_size,
        block_size,
        wal_sync: false,
    };

    let cache = WriteCache::<Initializing>::open_from_manifest(
        config,
        &manifest,
        None,
    )
    .unwrap();

    // No dirty blocks -- everything is in S3
    assert_eq!(cache.dirty_block_count(), 0);
    assert_eq!(cache.syncing_block_count(), 0);

    // Sequence should match the manifest
    assert_eq!(cache.inner.sequence.current(), 42);

    // Block map should have the 5 entries from the manifest
    for i in 0u64..5 {
        let (hash, seq) = cache.inner.block_map_get(i as usize);
        let expected_hash = blake3_128(format!("block-{i}").as_bytes());
        assert_eq!(hash, expected_hash, "block map entry {i} hash mismatch");
        assert_eq!(seq, 42, "block map entry {i} sequence mismatch");
    }

    // Entries beyond the manifest should be empty (zero hash)
    let (hash, seq) = cache.inner.block_map_get(5);
    assert!(hash.is_zero(), "entry 5 should be empty");
    assert_eq!(seq, 0);

    // No blocks present locally
    assert_eq!(cache.inner.count_present(), 0);
}

#[test]
fn test_open_from_manifest_with_forked_overlay() {
    use crate::nbd::block_map::{BlockMap, BlockMapEntry, BlockMapKind};

    let dir = TempDir::new().unwrap();
    let device_size: u64 = 1024 * 1024;
    let block_size: usize = 4096;
    let manifest = make_test_manifest(5, device_size, block_size as u32);

    // Build a parent BlockMap from the manifest entries
    let mut parent_bm = BlockMap::new(device_size, block_size as u32);
    for entry in &manifest.block_map {
        parent_bm.set(
            entry.chunk_index as usize,
            BlockMapEntry {
                hash: entry.hash,
                flags: 0,
                sequence: manifest.sequence,
            },
        );
    }
    let parent = Arc::new(parent_bm);

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test-forked".to_string(),
        device_size,
        block_size,
        wal_sync: false,
    };

    let cache = WriteCache::<Initializing>::open_from_manifest(
        config,
        &manifest,
        Some(Arc::clone(&parent)),
    )
    .unwrap();

    // Verify the block map is the Forked variant
    {
        let bm = cache.inner.block_map.read();
        assert!(
            matches!(&*bm, BlockMapKind::Forked(_)),
            "block map should be Forked variant"
        );
    }

    // Reads should resolve from the parent
    for i in 0u64..5 {
        let (hash, seq) = cache.inner.block_map_get(i as usize);
        let expected_hash = blake3_128(format!("block-{i}").as_bytes());
        assert_eq!(hash, expected_hash, "forked entry {i} hash mismatch");
        assert_eq!(seq, 42, "forked entry {i} sequence mismatch");
    }

    // No dirty blocks
    assert_eq!(cache.dirty_block_count(), 0);
}

#[test]
fn test_open_from_manifest_fork_writes_to_overlay() {
    use crate::nbd::block_map::{BlockMap, BlockMapEntry, BlockMapKind};

    let dir = TempDir::new().unwrap();
    let device_size: u64 = 1024 * 1024;
    let block_size: usize = 4096;
    let manifest = make_test_manifest(5, device_size, block_size as u32);

    // Build parent from manifest
    let mut parent_bm = BlockMap::new(device_size, block_size as u32);
    for entry in &manifest.block_map {
        parent_bm.set(
            entry.chunk_index as usize,
            BlockMapEntry {
                hash: entry.hash,
                flags: 0,
                sequence: manifest.sequence,
            },
        );
    }
    let parent = Arc::new(parent_bm);

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test-fork-write".to_string(),
        device_size,
        block_size,
        wal_sync: false,
    };

    let cache = WriteCache::<Initializing>::open_from_manifest(
        config,
        &manifest,
        Some(Arc::clone(&parent)),
    )
    .unwrap();

    // Write new data to chunk 0 -- should go to the overlay
    let new_data = vec![0xAB; block_size];
    let clean_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    cache.write(0, &new_data, &clean_cache).unwrap();

    // The overlay should have grown
    {
        let bm = cache.inner.block_map.read();
        if let BlockMapKind::Forked(f) = &*bm {
            assert_eq!(
                f.overlay_len(),
                1,
                "overlay should have 1 entry after write"
            );
        } else {
            panic!("block map should still be Forked variant");
        }
    }

    // With deferred hashing, the written chunk has ZERO placeholder hash.
    // The real hash is computed at flush time.
    let (hash, seq) = cache.inner.block_map_get(0);
    assert!(hash.is_zero(), "write() should set ZERO placeholder hash");
    assert!(seq > 42, "sequence should be beyond the manifest sequence");

    // Parent's entry at chunk 0 should be unchanged
    let parent_entry = parent.get(0);
    let expected_parent_hash = blake3_128("block-0".as_bytes());
    assert_eq!(parent_entry.hash, expected_parent_hash, "parent must be unmodified");
    assert_eq!(parent_entry.sequence, 42, "parent sequence must be unmodified");

    // Chunk 1 should still read from parent (not in overlay)
    let (hash_1, seq_1) = cache.inner.block_map_get(1);
    let expected_hash_1 = blake3_128("block-1".as_bytes());
    assert_eq!(hash_1, expected_hash_1, "unwritten chunk should read from parent");
    assert_eq!(seq_1, 42);

    // Should have 1 dirty block
    assert_eq!(cache.dirty_block_count(), 1);
}

// ========================================================================
// Recovery hash drift
// ========================================================================

#[tokio::test]
async fn test_recovery_detects_hash_drift() {
    let dir = TempDir::new().unwrap();
    let block_size = 4096;
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "test-drift".to_string(),
        device_size: 1024 * 1024,
        block_size,
        wal_sync: false,
    };

    let clean_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let original_data = vec![0xAAu8; block_size];
    let modified_data = vec![0xBBu8; block_size];
    let original_hash = blake3_128(&original_data);

    // Session 1: write data, persist block map + metadata, then corrupt SSD
    {
        let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
        let cache = cache.finish_recovery().await.unwrap();
        cache.write(0, &original_data, &clean_cache).unwrap();

        // With deferred hashing, write() sets ZERO placeholder in block_map.
        // The real hash is computed at flush-to-S3 time.
        let (hash, _) = cache.inner.block_map_get(0);
        assert!(hash.is_zero(), "write() should set ZERO placeholder hash");

        // Persist both v1 metadata (block states) and v2 block map file
        cache.save_metadata().unwrap();
        let bm_snapshot = cache.inner.block_map_snapshot();
        bm_snapshot.persist_to_file(&config.block_map_path()).unwrap();

        // Simulate "SSD data drifted" by overwriting the data file directly
        cache.inner.data_file.write_all_at(&modified_data, 0).unwrap();

        // Truncate WAL so recovery has to re-read from SSD (no WAL entries)
        cache.inner.wal.lock().truncate().unwrap();
    }

    // Session 2: reopen with recovery — should detect hash drift
    {
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery().await.unwrap();

        // The block_map should now have the hash of modified_data, not original_data
        let expected_hash = blake3_128(&modified_data);
        let (hash, _) = cache.inner.block_map_get(0);
        assert_eq!(
            hash, expected_hash,
            "block_map should reflect SSD contents after recovery drift detection"
        );
        assert_ne!(hash, original_hash, "hash should differ from pre-drift value");

        // Read should return the modified data
        let data = cache.read(0, block_size).unwrap();
        assert_eq!(&data[..], &modified_data[..]);
    }
}

// ========================================================================
// Pack registry integration (flush updates registry)
// ========================================================================

#[tokio::test]
async fn test_flush_updates_pack_registry() {
    let h = V2Harness::new().await;

    // Write enough blocks to produce at least 1 pack
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    let stats = h.flush().await;
    assert!(stats.packs_uploaded > 0);
    assert!(!stats.new_pack_ids.is_empty());

    // Update registry (the real flush_to_s3 does this, but V2Harness.flush() only
    // calls flush_to_s3 which includes update_registry)
    // Verify registry exists in S3
    let registry_data = h.content_store
        .get_registry("test")
        .await
        .unwrap();
    assert!(registry_data.is_some(), "registry should exist after flush");

    let reg = crate::nbd::pack_registry::PackRegistry::deserialize(
        &registry_data.unwrap()
    ).unwrap();
    assert!(!reg.pack_ids.is_empty(), "registry should contain pack IDs");

    // Verify the pack IDs from flush stats match the registry
    for pack_id in &stats.new_pack_ids {
        assert!(
            reg.pack_ids.contains(pack_id),
            "registry should contain flush pack ID {}",
            pack_id
        );
    }
}

#[tokio::test]
async fn test_multiple_flushes_accumulate_registry() {
    let h = V2Harness::new().await;

    // First flush
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }
    let stats1 = h.flush().await;

    // Second flush with different data
    for i in 5u8..10 {
        h.cache.write(i as u64 * 4096, &vec![i + 100; 4096], &h.clean_cache).unwrap();
    }
    let stats2 = h.flush().await;

    let all_expected_ids: Vec<uuid::Uuid> = stats1.new_pack_ids.iter()
        .chain(stats2.new_pack_ids.iter())
        .copied()
        .collect();

    let registry_data = h.content_store
        .get_registry("test")
        .await
        .unwrap()
        .expect("registry should exist");
    let reg = crate::nbd::pack_registry::PackRegistry::deserialize(&registry_data).unwrap();

    for pack_id in &all_expected_ids {
        assert!(
            reg.pack_ids.contains(pack_id),
            "registry should contain pack ID {} from both flushes",
            pack_id
        );
    }
}

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
    let content_store = Arc::new(crate::nbd::content_store::ContentStore::new(
        Arc::clone(&object_store),
        "test-conc",
    ));
    let pack_index = Arc::new(crate::nbd::pack_index::HostPackIndex::open(dir.path().join("pack_index.redb")).unwrap());
    let clean_cache = Arc::new(crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = Arc::new(cache.finish_recovery().await.unwrap());

    // Seed some initial dirty blocks
    for i in 0u8..10 {
        cache.write(i as u64 * 4096, &vec![i + 1; 4096], clean_cache.as_ref()).unwrap();
    }

    let mut tasks = JoinSet::new();

    // Spawn a flusher that runs multiple flush cycles
    {
        let cache = Arc::clone(&cache);
        let cs = Arc::clone(&content_store);
        let pi = Arc::clone(&pack_index);
        tasks.spawn(async move {
            for _ in 0..5 {
                let _ = cache.flush_to_s3(&cs, &pi).await;
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
                cache.write(block_idx * 4096, &data, clean_cache.as_ref()).unwrap();
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    // Final flush to capture any remaining dirty blocks
    let _stats = cache.flush_to_s3(&content_store, &pack_index).await.unwrap();

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

/// ForkedBlockMap: concurrent reads, writes, and flatten don't lose data.
///
/// Spawns tasks that read and write to the overlay while another task triggers
/// flatten via enough writes to cross the 50% threshold.
#[tokio::test]
async fn test_forked_block_map_concurrent_flatten() {
    use crate::nbd::block_map::{BlockMap, BlockMapEntry, BlockMapKind};
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let dir = TempDir::new().unwrap();
    let device_size: u64 = 256 * 4096; // 256 chunks
    let block_size: usize = 4096;
    let manifest = make_test_manifest(10, device_size, block_size as u32);

    // Build parent from manifest
    let mut parent_bm = BlockMap::new(device_size, block_size as u32);
    for entry in &manifest.block_map {
        parent_bm.set(
            entry.chunk_index as usize,
            BlockMapEntry {
                hash: entry.hash,
                flags: 0,
                sequence: manifest.sequence,
            },
        );
    }
    let parent = Arc::new(parent_bm);

    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: "conc-fork".to_string(),
        device_size,
        block_size,
        wal_sync: false,
    };

    let cache = WriteCache::<Initializing>::open_from_manifest(
        config,
        &manifest,
        Some(Arc::clone(&parent)),
    )
    .unwrap();
    let cache = Arc::new(cache);
    let clean_cache = Arc::new(crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024));

    // Verify it starts as Forked
    {
        let bm = cache.inner.block_map.read();
        assert!(matches!(&*bm, BlockMapKind::Forked(_)));
    }

    let mut tasks = JoinSet::new();

    // Spawn writers that write to enough chunks to trigger flatten (>50% = >128 chunks)
    for writer_id in 0..4u8 {
        let cache = Arc::clone(&cache);
        let clean_cache = Arc::clone(&clean_cache);
        tasks.spawn(async move {
            let start = writer_id as u64 * 40 + 10; // avoid manifest's 0..9
            for i in 0..40u64 {
                let offset = (start + i) * 4096;
                if offset + 4096 > device_size {
                    break;
                }
                let data = vec![writer_id.wrapping_add(i as u8); 4096];
                cache.write(offset, &data, clean_cache.as_ref()).unwrap();
                if i % 5 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });
    }

    // Spawn readers that read parent-range chunks concurrently
    for _ in 0..3 {
        let cache = Arc::clone(&cache);
        tasks.spawn(async move {
            for i in 0..10u64 {
                let (hash, seq) = cache.inner.block_map_get(i as usize);
                // Should always get a valid hash (parent or overlay)
                assert!(!hash.is_zero() || seq == 0, "unexpected zero hash at chunk {i}");
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    // After enough writes the forked map should have been flattened to Full
    {
        let bm = cache.inner.block_map.read();
        assert!(
            matches!(&*bm, BlockMapKind::Full(_)),
            "block map should have flattened after >128 overlay writes"
        );
    }

    // Parent entries not overwritten should still be readable from the flattened map
    for i in 0..10u64 {
        let (hash, _) = cache.inner.block_map_get(i as usize);
        // The writers started at chunk 10+, so chunks 0..9 should still have parent hashes
        let expected = blake3_128(format!("block-{i}").as_bytes());
        assert_eq!(
            hash, expected,
            "parent chunk {} should survive flatten",
            i
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

    let clean_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);
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
/// the correct final state. This test exercises the compound-check interleaving:
///
///   Flush snapshots (block_idx, old_seq), a concurrent write lands (pwrite +
///   block_map_set(ZERO, new_seq)), flush reads SSD (may get new data with old
///   seq), hash check in step 5 either catches the mismatch or the system
///   self-heals via re-flush.
///
/// After convergence (final quiesced flush), we verify:
/// 1. Every non-zero hash in the manifest has a matching pack_index entry
/// 2. Reading every block through the S3 path returns the correct final data
/// 3. The pack_index maps the correct hash for every block
#[tokio::test]
async fn test_concurrent_flush_write_s3_convergence() {
    use crate::nbd::block_map::blake3_128;
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
    let content_store = Arc::new(crate::nbd::content_store::ContentStore::new(
        Arc::clone(&object_store),
        "test-converge",
    ));
    let pack_index = Arc::new(crate::nbd::pack_index::HostPackIndex::open(dir.path().join("pack_index.redb")).unwrap());
    let clean_cache = Arc::new(crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024));

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
        let pi = Arc::clone(&pack_index);
        tasks.spawn(async move {
            for _ in 0..5 {
                let _ = cache.flush_to_s3(&cs, &pi).await;
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
    let _stats = cache.flush_to_s3(&content_store, &pack_index).await.unwrap();
    assert_eq!(cache.dirty_block_count(), 0, "all blocks clean after final flush");

    // Verify manifest is self-contained: every non-zero block_map hash has a
    // pack_index entry. A broken compound check would leave orphan hashes.
    let manifest_bytes = content_store
        .get_manifest("s3-converge")
        .await
        .unwrap()
        .expect("manifest should exist");
    let manifest = crate::nbd::manifest::Manifest::deserialize(&manifest_bytes).unwrap();
    let pack_hashes: std::collections::HashSet<_> =
        manifest.pack_index.iter().map(|e| e.hash).collect();

    for bm_entry in &manifest.block_map {
        if !bm_entry.hash.is_zero() {
            assert!(
                pack_hashes.contains(&bm_entry.hash),
                "block map hash {:?} at chunk {} has no pack_index entry — \
                 compound check may have committed a stale hash",
                bm_entry.hash,
                bm_entry.chunk_index,
            );
        }
    }

    // Verify reads through S3 return the correct final data.
    // Use a fresh clean_cache to force resolution through S3 packs.
    let verify_cache = crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024);
    let metrics = crate::nbd::metrics::ExportMetrics::new();
    for block_idx in 0u64..10 {
        let final_ssd = cache.read_local(block_idx * 4096, 4096).unwrap();
        let expected_hash = blake3_128(&final_ssd);

        // Verify pack_index has the right hash
        if !expected_hash.is_zero() {
            assert!(
                pack_index.get(&expected_hash).unwrap().is_some(),
                "block {} hash {:?} missing from pack_index after convergence",
                block_idx,
                expected_hash,
            );
        }

        // Read through full S3 path
        let s3_data = cache
            .read_v2(
                block_idx * 4096,
                4096,
                &verify_cache,
                &pack_index,
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

/// Regression guard: pruning with a stale block_map snapshot loses entries.
///
/// Reproduces the old race deterministically:
///   1. Write blocks 0-4, flush → Clean, hashes in pack_index
///   2. Write blocks 5-7 → Dirty, block_map has ZERO placeholder hash
///   3. Snapshot the referenced set (blocks 5-7 have ZERO → excluded)
///   4. Flush → uploads blocks 5-7, inserts hashes, CAS → Clean
///   5. Prune with stale snapshot → removes freshly-inserted entries
///
/// This is the bug that `rebuild_manifest_hashes` fixes. Keeping this test
/// as a regression guard: if the prune caller forgets the rebuild step,
/// entries are lost.
#[tokio::test]
async fn test_prune_stale_snapshot_loses_entries_regression() {
    use crate::nbd::block_map::blake3_128;
    use std::collections::HashSet;

    let h = V2Harness::new().await;

    // Write and flush 5 blocks
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }
    h.flush().await;

    // Write 3 more (dirty, ZERO hash in block_map)
    for i in 5u8..8 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    // Stale snapshot: blocks 5-7 contribute nothing (ZERO hash)
    let snapshot = h.cache.inner.block_map_snapshot();
    let stale_referenced: HashSet<_> = snapshot
        .iter_non_empty()
        .filter(|(_, e)| !e.hash.is_zero())
        .map(|(_, e)| e.hash)
        .collect();
    assert_eq!(stale_referenced.len(), 5);

    // Flush blocks 5-7 → pack_index now has their hashes
    h.flush().await;

    let new_hashes: Vec<_> = (5u8..8).map(|i| blake3_128(&vec![i + 1; 4096])).collect();
    for hash in &new_hashes {
        assert!(h.pack_index.get(hash).unwrap().is_some(), "hash in pack_index before prune");
    }

    // Prune with stale snapshot → loses the freshly-flushed entries
    let removed = h.pack_index.prune_unreferenced(&stale_referenced).unwrap();
    assert!(removed >= 3, "stale prune should remove at least 3 entries");

    for hash in &new_hashes {
        assert!(h.pack_index.get(hash).unwrap().is_none(), "stale prune loses entries");
    }

    // Reads still work via SSD fallback
    for i in 5u8..8 {
        let data = h.read(i as u64 * 4096, 4096).await;
        assert!(data.iter().all(|&b| b == i + 1), "block {} readable via SSD fallback", i);
    }
}

/// Verify that `rebuild_manifest_hashes` before prune prevents entry loss.
///
/// Same scenario as the regression test above, but uses the fixed path:
/// after flushing blocks 5-7, `rebuild_manifest_hashes` captures their
/// hashes from the current block_map + pack_index intersection. Prune
/// with this set retains all entries.
#[tokio::test]
async fn test_rebuild_manifest_hashes_prevents_prune_loss() {
    use crate::nbd::block_map::blake3_128;

    let h = V2Harness::new().await;

    // Write and flush 5 blocks
    for i in 0u8..5 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }
    h.flush().await;

    // Write 3 more (dirty, ZERO hash in block_map)
    for i in 5u8..8 {
        h.cache.write(i as u64 * 4096, &vec![i + 1; 4096], &h.clean_cache).unwrap();
    }

    // Flush blocks 5-7 → pack_index now has their hashes
    h.flush().await;
    assert_eq!(h.cache.dirty_block_count(), 0);

    let all_hashes: Vec<_> = (0u8..8).map(|i| blake3_128(&vec![i + 1; 4096])).collect();
    for hash in &all_hashes {
        assert!(h.pack_index.get(hash).unwrap().is_some(), "all hashes in pack_index before prune");
    }

    // The fix: rebuild manifest hashes before pruning (what prune_pack_index does)
    h.cache.rebuild_manifest_hashes(&h.pack_index).unwrap();
    let referenced = h.cache.referenced_hashes();

    // All 8 hashes should be in the referenced set
    assert!(referenced.len() >= 8, "rebuild should capture all 8 hashes, got {}", referenced.len());

    // Prune with manifest-based referenced set → nothing removed
    let removed = h.pack_index.prune_unreferenced(&referenced).unwrap();
    assert_eq!(removed, 0, "rebuild_manifest_hashes should prevent any pruning");

    // All entries still present
    for (i, hash) in all_hashes.iter().enumerate() {
        assert!(
            h.pack_index.get(hash).unwrap().is_some(),
            "block {} hash should survive prune with manifest-based referenced set",
            i,
        );
    }

    // Reads work through S3 path (not just SSD fallback)
    for i in 0u8..8 {
        let data = h.read(i as u64 * 4096, 4096).await;
        assert!(data.iter().all(|&b| b == i + 1), "block {} readable", i);
    }
}

/// CRC32 mismatch at flush time: block is skipped, CRC32 is cleared,
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

    // Verify CRC32 was computed (non-zero).
    let inner = h.cache.inner();
    let crc_after_checkpoint = inner.block_map_get_crc32(0);
    assert_ne!(crc_after_checkpoint, 0, "checkpoint should have computed CRC32");

    // Corrupt the block on SSD (simulate bit rot after checkpoint).
    let corrupted_data = vec![0xFFu8; 4096];
    inner.data_file.write_all_at(&corrupted_data, 0).unwrap();

    // Flush should detect CRC32 mismatch and skip the block.
    let (stats, _seq) = h.cache
        .flush_packs(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 1, "should detect 1 corrupted block");
    assert_eq!(stats.packs_uploaded, 0, "corrupted block should not be uploaded");
    assert_eq!(h.cache.dirty_block_count(), 1, "block should remain dirty");

    // CRC32 should have been cleared so next checkpoint can recompute.
    let crc_after_flush = inner.block_map_get_crc32(0);
    assert_eq!(crc_after_flush, 0, "CRC32 should be cleared after mismatch");

    // Next checkpoint recomputes CRC32 from the (still corrupted) SSD data.
    h.cache.local_checkpoint().unwrap();
    let crc_recomputed = inner.block_map_get_crc32(0);
    assert_ne!(crc_recomputed, 0, "checkpoint should recompute CRC32");
    assert_ne!(crc_recomputed, crc_after_checkpoint, "new CRC32 should differ (different data)");

    // Next flush succeeds — CRC32 now matches the (corrupted) SSD data.
    // This is the inherent limitation of deferred checksumming: if corruption
    // is persistent, the next checkpoint captures the corrupted state.
    let (stats2, _seq) = h.cache
        .flush_packs(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0, "no mismatch on second flush");
    assert_eq!(stats2.blocks_flushed, 1);
}

/// CRC32 is cleared on new writes, so stale checksums don't cause false positives.
#[tokio::test]
async fn test_crc32_cleared_on_write() {
    let h = V2Harness::new().await;

    // Write, checkpoint (compute CRC32).
    h.cache.write(0, &vec![0xAAu8; 4096], &h.clean_cache).unwrap();
    h.cache.local_checkpoint().unwrap();

    let inner = h.cache.inner();
    assert_ne!(inner.block_map_get_crc32(0), 0);

    // Write new data to the same block — CRC32 should be cleared.
    h.cache.write(0, &vec![0xBBu8; 4096], &h.clean_cache).unwrap();
    assert_eq!(inner.block_map_get_crc32(0), 0, "write should clear CRC32");

    // Flush without a checkpoint in between — CRC32 is 0, verification skipped.
    let (stats, _) = h.cache
        .flush_packs(&h.content_store, &h.pack_index)
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
        h.cache.write(i as u64 * 4096, &vec![i + 10; 4096], &h.clean_cache).unwrap();
    }
    assert_eq!(h.cache.dirty_block_count(), 5);

    // Checkpoint — computes CRC32 for all 5 dirty blocks.
    h.cache.local_checkpoint().unwrap();

    let inner = h.cache.inner();
    for i in 0..5 {
        assert_ne!(inner.block_map_get_crc32(i), 0, "block {i} should have CRC32");
    }

    // Corrupt blocks 1 and 3 on SSD (leave 0, 2, 4 intact).
    inner.data_file.write_all_at(&vec![0xFFu8; 4096], 4096).unwrap();
    inner.data_file.write_all_at(&vec![0xFEu8; 4096], 3 * 4096).unwrap();

    // Flush: should upload 3 good blocks, skip 2 corrupted.
    let (stats, _) = h.cache
        .flush_packs(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(stats.blocks_corrupted, 2, "blocks 1 and 3 corrupted");
    assert_eq!(stats.blocks_flushed, 5, "all 5 scanned");
    assert_eq!(stats.packs_uploaded, 1, "3 good blocks → 1 pack");

    // Good blocks (0, 2, 4) should be clean now; corrupted (1, 3) still dirty.
    assert_eq!(h.cache.dirty_block_count(), 2, "corrupted blocks remain dirty");

    // CRC32 cleared on corrupted blocks so next checkpoint recomputes.
    assert_eq!(inner.block_map_get_crc32(1), 0);
    assert_eq!(inner.block_map_get_crc32(3), 0);

    // Heal: checkpoint recomputes CRC32 from (still corrupted) SSD data.
    h.cache.local_checkpoint().unwrap();
    assert_ne!(inner.block_map_get_crc32(1), 0);
    assert_ne!(inner.block_map_get_crc32(3), 0);

    // Second flush succeeds for the remaining 2 blocks.
    let (stats2, _) = h.cache
        .flush_packs(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0);
    assert_eq!(stats2.blocks_flushed, 2);
    assert_eq!(h.cache.dirty_block_count(), 0, "all blocks clean after heal");
}

/// CRC32 verified correctly on the happy path: checkpoint computes CRC32,
/// flush verifies it matches, block is uploaded normally.
///
/// Also verifies the full CRC32 lifecycle across multiple cycles: write →
/// checkpoint → flush → write more → checkpoint → flush. CRC32 must be
/// properly cleared after flush (block transitions to CLEAN) and fresh
/// writes get new CRC32s.
#[tokio::test]
async fn test_crc32_happy_path_multi_cycle() {
    let h = V2Harness::new().await;

    // Cycle 1: write 3 blocks, checkpoint (computes CRC32), flush.
    for i in 0u8..3 {
        h.cache.write(i as u64 * 4096, &vec![i + 10; 4096], &h.clean_cache).unwrap();
    }
    h.cache.local_checkpoint().unwrap();

    let inner = h.cache.inner();
    for i in 0..3 {
        assert_ne!(inner.block_map_get_crc32(i), 0, "cycle 1: block {i} should have CRC32");
    }

    let (stats1, _) = h.cache
        .flush_packs(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(stats1.blocks_corrupted, 0, "cycle 1: no corruption");
    assert_eq!(stats1.blocks_flushed, 3);
    assert_eq!(stats1.packs_uploaded, 1);
    assert_eq!(h.cache.dirty_block_count(), 0, "cycle 1: all clean");

    // Cycle 2: write 2 new blocks + overwrite 1 existing block.
    h.cache.write(3 * 4096, &vec![0xDD; 4096], &h.clean_cache).unwrap();
    h.cache.write(4 * 4096, &vec![0xEE; 4096], &h.clean_cache).unwrap();
    h.cache.write(0, &vec![0xFF; 4096], &h.clean_cache).unwrap(); // overwrite block 0

    assert_eq!(h.cache.dirty_block_count(), 3);

    // Block 0's CRC32 should be cleared by the overwrite.
    assert_eq!(inner.block_map_get_crc32(0), 0, "overwrite should clear CRC32");

    h.cache.local_checkpoint().unwrap();

    for idx in [0, 3, 4] {
        assert_ne!(inner.block_map_get_crc32(idx), 0, "cycle 2: block {idx} should have CRC32");
    }

    let (stats2, _) = h.cache
        .flush_packs(&h.content_store, &h.pack_index)
        .await
        .unwrap();
    assert_eq!(stats2.blocks_corrupted, 0, "cycle 2: no corruption");
    assert_eq!(stats2.blocks_flushed, 3);
    assert_eq!(h.cache.dirty_block_count(), 0, "cycle 2: all clean");
}

/// Concurrent writes during flush with CRC32 enabled must never produce
/// false corruption reports.
///
/// When a write lands during flush, the CRC32 from a prior checkpoint
/// won't match the new SSD data. The seq-number check in the CRC32
/// mismatch branch disambiguates: if seq changed → CAS failure (retry),
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
    let content_store = Arc::new(crate::nbd::content_store::ContentStore::new(
        Arc::clone(&object_store),
        "test-crc32-conc",
    ));
    let pack_index = Arc::new(
        crate::nbd::pack_index::HostPackIndex::open(dir.path().join("pack_index.redb")).unwrap(),
    );
    let clean_cache = Arc::new(crate::nbd::cache::SimpleBlockCache::new(64 * 1024 * 1024));

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
        let pi = Arc::clone(&pack_index);
        let corrupted = Arc::clone(&total_corrupted);
        tasks.spawn(async move {
            for _ in 0..10 {
                cache.local_checkpoint().unwrap();
                let (stats, _) = cache.flush_packs(&cs, &pi).await.unwrap();
                corrupted.fetch_add(stats.blocks_corrupted as u64, std::sync::atomic::Ordering::Relaxed);
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
    let stats = cache.flush_to_s3(&content_store, &pack_index).await.unwrap();
    assert_eq!(stats.blocks_corrupted, 0);
    assert_eq!(cache.dirty_block_count(), 0);
}
