//! Integration tests for GlideFS NBD cache.
//!
//! These tests verify the key architectural claims:
//! 1. Wake from any node - read-through cache fetches from S3
//! 2. Instant snapshot - flush() is local SSD only
//! 3. Failure recovery - graceful handling of S3/network failures
//! 4. Crash recovery - dirty blocks survive process crashes

mod chunk_prefetch;
mod crash_recovery;
mod crash_under_load;
mod data_safety;
mod failure_injection;
mod flush_safety;
mod gc;
mod partial_blocks;
mod property_tests;
mod snapshots;
mod wake_any_node;

use std::sync::{Arc, LazyLock};

use object_store::ObjectStore;
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::state::Active;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB
const DEVICE_SIZE: u64 = 256 * 1024 * 1024; // 256MB (enough for multi-pack tests at 500 blocks/pack)

/// Process-global shared PackIndexCache.
///
/// Foyer's SSD tier doesn't release file descriptors on drop. Creating one per
/// test exhausts the FD limit and causes SIGSEGV on CI. A single shared cache
/// avoids this — entries are keyed by PackId so tests don't interfere.
static SHARED_PACK_INDEX_CACHE: LazyLock<Arc<PackIndexCache>> = LazyLock::new(|| {
    // LazyLock is initialized inside a #[tokio::test] runtime, so we can't call
    // Runtime::new() (panics with "cannot start a runtime from within a runtime").
    // Spawn a std thread to create and leak a dedicated runtime for foyer's background tasks.
    let cache = std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new()
            .expect("failed to create runtime for pack index cache");
        let dir = TempDir::new().expect("failed to create temp dir for shared pack index cache");
        let dir = Box::leak(Box::new(dir));
        let cache = rt.block_on(PackIndexCache::open(dir.path()))
            .expect("failed to open pack index cache");
        // Leak the runtime so foyer's background tasks keep running.
        std::mem::forget(rt);
        cache
    })
    .join()
    .expect("failed to initialize shared pack index cache");
    Arc::new(cache)
});

/// Shared test harness: creates a WriteCache with ContentStore + PackIndexCache + VolumeManifest + SimpleBlockCache.
///
/// Returns all components needed for the flush/read API.
#[allow(clippy::type_complexity)]
pub async fn create_test_cache(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<dyn ObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<PackIndexCache>,
    Arc<parking_lot::RwLock<VolumeManifest>>,
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
    let pack_index_cache = Arc::clone(&*SHARED_PACK_INDEX_CACHE);
    let volume_manifest = Arc::new(parking_lot::RwLock::new(
        VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE as u32),
    ));
    let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::open(config).expect("Failed to open cache");
    let cache = cache.skip_recovery_for_test();

    (
        Arc::new(cache),
        content_store,
        pack_index_cache,
        volume_manifest,
        clean_cache,
        metrics,
    )
}

/// Cold reader: creates a WriteCache whose reads go through VolumeManifest + PackIndexCache + S3.
///
/// After a writer calls `flush_to_s3`, the VolumeManifest in S3 contains pack IDs.
/// This helper downloads the VolumeManifest, opens a fresh WriteCache (local SSD starts
/// empty), and `read` resolves blocks through VolumeManifest -> PackIndexCache -> S3.
pub async fn create_cold_reader(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<dyn ObjectStore>,
) -> (
    Arc<WriteCache<Active>>,
    ContentStore,
    Arc<PackIndexCache>,
    Arc<parking_lot::RwLock<VolumeManifest>>,
    Arc<SimpleBlockCache>,
    Arc<ExportMetrics>,
) {
    let content_store = ContentStore::new(Arc::clone(&s3), "test");

    // Fetch VolumeManifest from S3
    let volume_manifest = match content_store.get_manifest(name).await {
        Ok(Some(data)) => Arc::new(parking_lot::RwLock::new(
            VolumeManifest::deserialize(&data).expect("failed to deserialize volume manifest"),
        )),
        Ok(None) => panic!("volume manifest should exist in S3 for cold reader"),
        Err(e) => panic!("failed to fetch volume manifest: {}", e),
    };

    let pack_index_cache = Arc::clone(&*SHARED_PACK_INDEX_CACHE);

    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
        wal_sync: false,
    };

    let metrics = Arc::new(ExportMetrics::new());
    let clean_cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

    let cache = WriteCache::open_fresh_active(config)
        .expect("Failed to open fresh cache for cold reader");

    (
        Arc::new(cache),
        content_store,
        pack_index_cache,
        volume_manifest,
        clean_cache,
        metrics,
    )
}
