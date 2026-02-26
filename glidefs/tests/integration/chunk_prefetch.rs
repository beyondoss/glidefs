//! Integration test for chunk metadata prefetch on cold start.
//!
//! Verifies that `ExportRouter::prefetch_chunk_metas()` warms the PackIndexCache
//! from S3 so that the first VM reads don't block on S3 .meta fetches.

use std::sync::Arc;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::router::{ExportRouter, RouterConfig};
use glidefs::config::ExportConfig;
use object_store::memory::InMemory;
use tempfile::TempDir;

use super::BLOCK_SIZE;

/// Write blocks → flush → create a cold router → prefetch → verify cache is warm.
#[tokio::test]
async fn test_prefetch_chunk_metas_cold_start() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dir1 = TempDir::new().unwrap();

    // --- Phase 1: create export, write blocks, drain to S3 ---
    let router1 = Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: Arc::clone(&s3),
            db_path: "test".to_string(),
            cache_dir: dir1.path().to_path_buf(),
            block_size: BLOCK_SIZE,
            clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_blocks_per_pack: 500,
            ublk_nr_queues: 1,
            nbd_dead_conn_timeout: 0,
            max_concurrent_flushes: 0,
        })
        .await
        .unwrap(),
    );

    let config = ExportConfig {
        name: "vm1".to_string(),
        size_gb: 0.01,
        s3_prefix: None,
        block_size: None,
        blocks_per_pack: None,
        flush_mode: None,
        transport: None,
    };
    router1
        .create_export(config.clone(), false, None, None)
        .await
        .unwrap();

    // Write 4 distinct blocks (write is sync, not async)
    let handler = router1.get_handler("vm1").await.unwrap();
    for i in 0u8..4 {
        let data = vec![0x10 + i; BLOCK_SIZE];
        handler
            .write(i as u64 * BLOCK_SIZE as u64, &data, false)
            .unwrap();
    }

    // Drain to S3 (creates chunks/ and manifests/)
    router1.drain_all().await;

    // Save export config so router2 can discover it
    router1.save_export(&config).await.unwrap();

    // Shut down router1
    drop(router1);

    // --- Phase 2: cold start on new cache dir (simulates new host) ---
    let dir2 = TempDir::new().unwrap();
    let router2 = ExportRouter::new(RouterConfig {
        object_store: Arc::clone(&s3),
        db_path: "test".to_string(),
        cache_dir: dir2.path().to_path_buf(),
        block_size: BLOCK_SIZE,
        clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
        wal_sync: false,
        max_s3_uploads: 0,
        max_s3_downloads: 0,
        default_blocks_per_pack: 500,
        ublk_nr_queues: 1,
        nbd_dead_conn_timeout: 0,
            max_concurrent_flushes: 0,
    })
    .await
    .unwrap();

    // Discover and recover exports from S3 (loads VolumeManifests)
    let discovered = router2.discover_exports().await.unwrap();
    assert_eq!(discovered.len(), 1, "should discover 1 export");
    for export_config in discovered {
        router2
            .create_export(export_config, false, None, None)
            .await
            .unwrap();
    }

    // Prefetch chunk metas — should fetch from S3 since cache dir is fresh
    let prefetched = router2.prefetch_chunk_metas().await;
    assert!(
        prefetched > 0,
        "should have prefetched at least 1 chunk meta from S3"
    );

    // Second call should be a no-op (all metas already cached)
    let prefetched_again = router2.prefetch_chunk_metas().await;
    assert_eq!(
        prefetched_again, 0,
        "second prefetch should be no-op (cache is warm)"
    );

    // Verify reads work (cache is warm from prefetch)
    let handler2 = router2.get_handler("vm1").await.unwrap();
    for i in 0u8..4 {
        let data = handler2
            .read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == 0x10 + i),
            "block {} should contain {:#x}, got {:#x}",
            i,
            0x10 + i,
            data[0],
        );
    }
}

/// Prefetch on an empty router returns 0 (no-op).
#[tokio::test]
async fn test_prefetch_empty_router() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();

    let router = ExportRouter::new(RouterConfig {
        object_store: s3,
        db_path: "test".to_string(),
        cache_dir: dir.path().to_path_buf(),
        block_size: BLOCK_SIZE,
        clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
        wal_sync: false,
        max_s3_uploads: 0,
        max_s3_downloads: 0,
        default_blocks_per_pack: 500,
        ublk_nr_queues: 1,
        nbd_dead_conn_timeout: 0,
            max_concurrent_flushes: 0,
    })
    .await
    .unwrap();

    let prefetched = router.prefetch_chunk_metas().await;
    assert_eq!(prefetched, 0);
}
