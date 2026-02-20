//! Per-export flush scheduler.
//!
//! Each export runs one scheduler that handles two concerns:
//! - **Pack-size flush** (event-driven): when dirty blocks reach the per-export
//!   `blocks_per_pack` threshold, the write path notifies the scheduler to flush
//!   packs + sync manifest to S3. Disabled in manual mode (blocks_per_pack = 0).
//! - **Local checkpoint** (periodic, 5s): persists block states and truncates the
//!   WAL. No S3 involvement.
//!
//! Manifest sync happens after every successful pack upload so that flushed packs
//! are immediately discoverable on cross-host recovery (host death without drain).

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::{watch, Notify};
use tracing::{info, warn};

use crate::nbd::content_store::ContentStore;
use crate::nbd::metrics::ExportMetrics;
use crate::nbd::pack_index::HostPackIndex;
use crate::nbd::state::Active;
use crate::nbd::write_cache::WriteCache;

/// Run the flush scheduler for a single export.
///
/// Loops until `shutdown` signals true. Two select branches:
/// 1. `flush_notify` — event-driven pack flush when dirty count crosses threshold
/// 2. Checkpoint ticker — periodic WAL truncation every 5s
pub async fn flush_scheduler(
    cache: Arc<WriteCache<Active>>,
    content_store: Arc<ContentStore>,
    pack_index: Arc<HostPackIndex>,
    flush_notify: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<ExportMetrics>,
) {
    info!("flush scheduler started");

    // Jitter the checkpoint start so 2K exports don't all checkpoint at the same instant.
    let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..5000));
    let checkpoint_interval = Duration::from_secs(5);
    let mut checkpoint_ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + jitter, checkpoint_interval);

    loop {
        tokio::select! {
            biased;

            // Shutdown takes priority.
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("flush scheduler: shutting down");
                    return;
                }
            }

            // Event-driven: write path notifies when dirty count crosses blocks_per_pack.
            () = flush_notify.notified() => {
                let start = Instant::now();
                match cache.flush_packs(&content_store, &pack_index).await {
                    Ok((stats, seq_cutpoint)) => {
                        metrics.record_s3_put_latency(start.elapsed());
                        metrics.record_flush_blocks_cas_failed(stats.blocks_cas_failed);
                        if stats.packs_uploaded > 0 {
                            info!(
                                packs = stats.packs_uploaded,
                                blocks = stats.blocks_flushed,
                                bytes = stats.bytes_uploaded,
                                "pack-size flush"
                            );
                            // Sync manifest so flushed packs are discoverable
                            // on cross-host recovery (host death without drain).
                            // sync_manifest includes checkpoint (persist + WAL truncate).
                            if let Err(e) = cache.sync_manifest(&content_store, &pack_index, seq_cutpoint).await {
                                warn!(error = %e, "manifest sync after flush failed");
                            }
                        } else {
                            // No packs uploaded — still checkpoint to persist
                            // clean block states and compute CRC32s.
                            if let Err(e) = cache.local_checkpoint() {
                                warn!(error = %e, "checkpoint after flush failed");
                            }
                        }
                    }
                    Err(e) => {
                        metrics.record_flush_error();
                        warn!(error = %e, "pack flush failed");
                    }
                }
            }

            // Periodic: truncate WAL every 5s.
            _ = checkpoint_ticker.tick() => {
                if cache.dirty_block_count() > 0
                    && let Err(e) = cache.local_checkpoint()
                {
                    warn!(error = %e, "local checkpoint failed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::nbd::cache::{BlockCache, SimpleBlockCache};
    use crate::nbd::content_store::ContentStore;
    use crate::nbd::pack::DEFAULT_BLOCKS_PER_PACK;
    use crate::nbd::pack_index::HostPackIndex;
    use crate::nbd::state::Initializing;
    use crate::nbd::write_cache::WriteCacheConfig;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    #[allow(clippy::type_complexity)]
    fn test_scheduler_components() -> (
        Arc<WriteCache<Active>>,
        Arc<ContentStore>,
        Arc<HostPackIndex>,
        Arc<Notify>,
        watch::Receiver<bool>,
        watch::Sender<bool>,
        Arc<ExportMetrics>,
        Arc<dyn BlockCache>,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "sched-test".to_string(),
            device_size: 128 * 1024 * (DEFAULT_BLOCKS_PER_PACK as u64 + 10), // enough for DEFAULT_BLOCKS_PER_PACK + headroom
            block_size: 128 * 1024,
            wal_sync: false,
        };

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = Arc::new(ContentStore::new(s3, "test"));
        let pack_index =
            Arc::new(HostPackIndex::open(temp_dir.path().join("pack_index.redb")).unwrap());
        let metrics = Arc::new(ExportMetrics::new());
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.skip_recovery_for_test());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let flush_notify = Arc::new(Notify::new());

        (
            cache,
            content_store,
            pack_index,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            temp_dir,
        )
    }

    #[tokio::test]
    async fn test_scheduler_shutdown() {
        let (cache, content_store, pack_index, flush_notify, shutdown_rx, shutdown_tx, metrics, ..) =
            test_scheduler_components();

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index,
                flush_notify,
                shutdown_rx,
                metrics,
            )
            .await;
        });

        // Signal shutdown
        shutdown_tx.send(true).unwrap();

        // Scheduler should exit promptly
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("scheduler should exit within 2s")
            .unwrap();
    }

    #[tokio::test]
    async fn test_pack_size_flush() {
        let (
            cache,
            content_store,
            pack_index,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            _temp,
        ) = test_scheduler_components();

        let cache_check = Arc::clone(&cache);
        let flush_notify_clone = Arc::clone(&flush_notify);

        // Write DEFAULT_BLOCKS_PER_PACK dirty blocks
        for i in 0..DEFAULT_BLOCKS_PER_PACK {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xAA; 128 * 1024], clean_cache.as_ref())
                .unwrap();
        }
        assert_eq!(cache_check.dirty_block_count(), DEFAULT_BLOCKS_PER_PACK as u64);

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index,
                flush_notify,
                shutdown_rx,
                metrics,
            )
            .await;
        });

        // Notify (simulating what the write path does)
        flush_notify_clone.notify_one();

        // Wait for flush to complete
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if cache_check.dirty_block_count() == 0 {
                break;
            }
        }
        assert_eq!(
            cache_check.dirty_block_count(),
            0,
            "pack-size flush should have flushed all dirty blocks"
        );

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }
}
