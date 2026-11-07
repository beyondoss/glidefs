//! A handoff successor builds its router while passive (`Warming`): no flush, no
//! WAL truncation, until takeover completes. Caches created AFTER takeover are
//! new exports and must flush like any other. v0.6.0 to v0.9.17 left the
//! passive flag set for the life of the process, so every export created after
//! a handoff upgrade never uploaded (the dirty-age timer fired into a skipped
//! flush forever).
//!
//! One test, one process: the flag is process-global, so the cases run in
//! sequence here and cannot race another test's cache construction.

#![cfg(feature = "test-utils")]

use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::content_store::ContentStore;
use glidefs::block::flush_scheduler::flush_scheduler;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack::DEFAULT_FLUSH_THRESHOLD;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::state::{Active, Initializing};
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{HandoffPhase, WriteCache, WriteCacheConfig, set_successor_passive};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::sync::{Notify, watch};

const BLOCK: usize = 128 * 1024;

struct Export {
    cache: Arc<WriteCache<Active>>,
    s3: Arc<InMemory>,
    notify: Arc<Notify>,
    _shutdown: watch::Sender<bool>,
    _dir: tempfile::TempDir,
}

async fn export(name: &str) -> Export {
    let dir = tempfile::TempDir::new().unwrap();
    let device_size = (BLOCK * (DEFAULT_FLUSH_THRESHOLD + 10)) as u64;
    let config = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: BLOCK as u32 as _,
        wal_sync: false,
    };
    let s3 = Arc::new(InMemory::new());
    let content_store = Arc::new(ContentStore::new(s3.clone() as _, "test"));
    let pic = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(device_size, BLOCK as _)));
    let clean: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    let cache = Arc::new(WriteCache::<Initializing>::open(config).unwrap().skip_recovery_for_test());
    let notify = Arc::new(Notify::new());
    let (tx, rx) = watch::channel(false);
    tokio::spawn(flush_scheduler(
        Arc::clone(&cache),
        content_store,
        pic,
        vm,
        clean,
        Arc::clone(&notify),
        rx,
        Arc::new(ExportMetrics::new()),
        None,
        DEFAULT_FLUSH_THRESHOLD,
        0,
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;
    Export { cache, s3, notify, _shutdown: tx, _dir: dir }
}

async fn objects(e: &Export) -> usize {
    e.s3.list(None).try_collect::<Vec<_>>().await.unwrap().len()
}

/// One block written, then 35 s: the dirty-age bound (30 s) has passed.
async fn age_flushes(e: &Export) -> bool {
    e.cache.write(0, &[0xAB; BLOCK]).unwrap();
    tokio::time::sleep(Duration::from_secs(35)).await;
    e.cache.dirty_block_count() == 0 && objects(e).await > 0
}

#[tokio::test(start_paused = true)]
async fn caches_created_after_takeover_flush() {
    // Successor starts: passive. A cache built now is Warming and must not flush.
    set_successor_passive(true);
    let existing = export("existing").await;
    assert_eq!(existing.cache.handoff_phase(), HandoffPhase::Warming);
    assert!(!age_flushes(&existing).await, "a Warming cache must not flush");
    assert_eq!(existing.cache.dirty_block_count(), 1);

    // Takeover completes: existing caches go Idle, the process-wide flag clears.
    existing.cache.set_handoff_phase(HandoffPhase::Idle).await;
    set_successor_passive(false);

    // A cache that existed at takeover flushes by dirty age, as before.
    tokio::time::sleep(Duration::from_secs(35)).await;
    assert_eq!(existing.cache.dirty_block_count(), 0, "existing cache flushes after takeover");

    // A cache created after takeover is Idle and flushes by dirty age...
    let by_age = export("new-age").await;
    assert_eq!(by_age.cache.handoff_phase(), HandoffPhase::Idle);
    assert!(age_flushes(&by_age).await, "a post-takeover cache must flush by dirty age");

    // ...and by the pack-size threshold, long before the age bound.
    let by_size = export("new-size").await;
    for i in 0..DEFAULT_FLUSH_THRESHOLD {
        by_size.cache.write((i * BLOCK) as u64, &[0xCD; BLOCK]).unwrap();
    }
    by_size.notify.notify_one();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(by_size.cache.dirty_block_count(), 0, "threshold flush of a post-takeover cache");
    assert!(objects(&by_size).await > 0);
}
