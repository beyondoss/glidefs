//! Integration tests for GlideFS NBD cache.
//!
//! These tests verify the key architectural claims:
//! 1. Wake from any node - read-through cache fetches from S3
//! 2. Instant snapshot - flush() is local SSD only
//! 3. Failure recovery - graceful handling of S3/network failures
//! 4. Crash recovery - dirty blocks survive process crashes

mod crash_recovery;
mod failure_injection;
mod gc;
mod property_tests;
mod wake_any_node;

use std::sync::Arc;

use object_store::ObjectStore;
use tempfile::TempDir;

use glidefs::nbd::cache::SimpleBlockCache;
use glidefs::nbd::content_store::ContentStore;
use glidefs::nbd::manifest::Manifest;
use glidefs::nbd::metrics::ExportMetrics;
use glidefs::nbd::pack_index::HostPackIndex;
use glidefs::nbd::state::Active;
use glidefs::nbd::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB
const DEVICE_SIZE: u64 = 10 * 1024 * 1024; // 10MB

/// Shared v2 test harness: creates a WriteCache with ContentStore + HostPackIndex + SimpleBlockCache.
///
/// Returns all components needed for the v2 flush/read API.
pub fn create_v2_test_cache(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<dyn ObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    HostPackIndex,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let metrics = Arc::new(ExportMetrics::new());
    let content_store = ContentStore::new(s3, "test");
    let pack_index = HostPackIndex::open(temp_dir.path().join("pack_index.redb")).unwrap();
    let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::open(config).expect("Failed to open cache");
    let cache = cache.skip_recovery_for_test();

    (Arc::new(cache), content_store, pack_index, clean_cache, metrics)
}

/// Cold reader: creates a WriteCache whose block_map is populated from the S3 manifest.
///
/// After a writer calls `flush_to_s3`, the manifest in S3 contains the block map and
/// pack entries. This helper downloads the manifest, opens a WriteCache via
/// `open_from_manifest` (populating the block_map), and rebuilds the HostPackIndex
/// so `read_v2` can resolve blocks through S3.
pub async fn create_v2_cold_reader(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<dyn ObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    HostPackIndex,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    let content_store = ContentStore::new(Arc::clone(&s3), "test");

    // Fetch manifest from S3
    let manifest_bytes = content_store
        .get_manifest(name)
        .await
        .expect("manifest fetch failed")
        .expect("manifest should exist in S3");
    let manifest =
        Manifest::deserialize(&manifest_bytes).expect("manifest deserialization failed");

    // Rebuild pack_index from manifest
    let pack_index = HostPackIndex::open(temp_dir.path().join("pack_index.redb")).unwrap();
    pack_index.rebuild(std::slice::from_ref(&manifest));

    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let metrics = Arc::new(ExportMetrics::new());
    let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::open_from_manifest(config, &manifest, None)
        .expect("Failed to open cache from manifest");

    (Arc::new(cache), content_store, pack_index, clean_cache, metrics)
}
