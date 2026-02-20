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

    // Backoff state: when flush fails (e.g., S3 down), wait before retrying
    // to avoid a tight spin of failed flush_packs calls.
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    let mut flush_backoff = Duration::ZERO;

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
                // If we're in backoff after a previous failure, wait before retrying.
                if flush_backoff > Duration::ZERO {
                    tokio::select! {
                        biased;
                        Ok(()) = shutdown.changed() => {
                            if *shutdown.borrow() {
                                info!("flush scheduler: shutting down during backoff");
                                return;
                            }
                        }
                        () = tokio::time::sleep(flush_backoff) => {}
                    }
                }

                let start = Instant::now();
                match cache.flush_packs(&content_store, &pack_index).await {
                    Ok((stats, seq_cutpoint)) => {
                        flush_backoff = Duration::ZERO;
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
                        // Exponential backoff: 1s → 2s → 4s → ... → 30s cap.
                        flush_backoff = if flush_backoff.is_zero() {
                            Duration::from_secs(1)
                        } else {
                            flush_backoff.saturating_mul(2).min(MAX_BACKOFF)
                        };
                        metrics.record_flush_error();
                        warn!(error = %e, backoff_secs = flush_backoff.as_secs(), "pack flush failed, backing off");
                        // Still checkpoint on flush error to prevent WAL growth
                        // when S3 is down and flush_notify fires continuously.
                        if let Err(e) = cache.local_checkpoint() {
                            warn!(error = %e, "checkpoint after flush error failed");
                        }
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
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::TempDir;

    /// Object store that can toggle PUT failures for testing backoff.
    #[derive(Debug)]
    struct FailingObjectStore {
        inner: InMemory,
        fail_puts: AtomicBool,
    }

    impl FailingObjectStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                fail_puts: AtomicBool::new(false),
            }
        }

        fn set_fail_puts(&self, fail: bool) {
            self.fail_puts.store(fail, Ordering::SeqCst);
        }
    }

    impl std::fmt::Display for FailingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingObjectStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FailingObjectStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            if self.fail_puts.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "FailingObjectStore",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "Simulated S3 failure",
                    )),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> ObjectStoreResult<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> ObjectStoreResult<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

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

    /// Like test_scheduler_components but with a custom object store.
    #[allow(clippy::type_complexity)]
    fn test_scheduler_components_with_store(
        s3: Arc<dyn object_store::ObjectStore>,
    ) -> (
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
            device_name: "sched-backoff".to_string(),
            device_size: 128 * 1024 * (DEFAULT_BLOCKS_PER_PACK as u64 + 10),
            block_size: 128 * 1024,
            wal_sync: false,
        };

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

    /// Test that the scheduler backs off exponentially when flush fails.
    #[tokio::test(start_paused = true)]
    async fn test_flush_backoff_on_failure() {
        let failing_s3 = Arc::new(FailingObjectStore::new());
        failing_s3.set_fail_puts(true);

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
        ) = test_scheduler_components_with_store(failing_s3.clone() as _);

        let metrics_check = Arc::clone(&metrics);
        let flush_notify_clone = Arc::clone(&flush_notify);

        // Write enough dirty blocks to trigger flush
        for i in 0..DEFAULT_BLOCKS_PER_PACK {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xBB; 128 * 1024], clean_cache.as_ref())
                .unwrap();
        }

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

        // First notify — should fail immediately (no backoff yet), backoff set to 1s
        flush_notify_clone.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(metrics_check.snapshot().flush_errors, 1);

        // Second notify — should wait 1s backoff before attempting flush
        flush_notify_clone.notify_one();
        // After 500ms the second flush hasn't happened yet (still in backoff)
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(metrics_check.snapshot().flush_errors, 1, "should still be backing off");
        // After another 600ms (total 1.1s), the backoff has elapsed and flush was attempted
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(metrics_check.snapshot().flush_errors, 2, "second flush should have been attempted");

        // Third notify — should wait 2s backoff
        flush_notify_clone.notify_one();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(metrics_check.snapshot().flush_errors, 2, "should still be in 2s backoff");
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(metrics_check.snapshot().flush_errors, 3, "third flush after 2s backoff");

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// Test that successful flush resets the backoff.
    #[tokio::test(start_paused = true)]
    async fn test_flush_backoff_resets_on_success() {
        let failing_s3 = Arc::new(FailingObjectStore::new());
        failing_s3.set_fail_puts(true);

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
        ) = test_scheduler_components_with_store(failing_s3.clone() as _);

        let metrics_check = Arc::clone(&metrics);
        let cache_check = Arc::clone(&cache);
        let flush_notify_clone = Arc::clone(&flush_notify);

        // Write dirty blocks
        for i in 0..DEFAULT_BLOCKS_PER_PACK {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xCC; 128 * 1024], clean_cache.as_ref())
                .unwrap();
        }

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

        // First notify — fails, backoff = 1s
        flush_notify_clone.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(metrics_check.snapshot().flush_errors, 1);
        assert!(cache_check.dirty_block_count() > 0, "blocks still dirty");

        // Enable S3 — next flush should succeed and reset backoff
        failing_s3.set_fail_puts(false);
        flush_notify_clone.notify_one();
        // Wait past the 1s backoff + processing time
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(cache_check.dirty_block_count(), 0, "flush should have succeeded");
        assert_eq!(metrics_check.snapshot().flush_errors, 1, "no new errors");

        // Write more blocks and fail again — should start from 1s, not 2s
        failing_s3.set_fail_puts(true);
        for i in 0..DEFAULT_BLOCKS_PER_PACK {
            let offset = i as u64 * 128 * 1024;
            cache_check
                .write(offset, &[0xDD; 128 * 1024], clean_cache.as_ref())
                .unwrap();
        }

        flush_notify_clone.notify_one();
        // Should fail immediately (backoff was reset to zero by success)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(metrics_check.snapshot().flush_errors, 2, "immediate retry after reset");

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// Test that shutdown is respected during backoff sleep.
    #[tokio::test(start_paused = true)]
    async fn test_shutdown_during_backoff() {
        let failing_s3 = Arc::new(FailingObjectStore::new());
        failing_s3.set_fail_puts(true);

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
        ) = test_scheduler_components_with_store(failing_s3 as _);

        let flush_notify_clone = Arc::clone(&flush_notify);

        // Write dirty blocks
        for i in 0..DEFAULT_BLOCKS_PER_PACK {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xEE; 128 * 1024], clean_cache.as_ref())
                .unwrap();
        }

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

        // Trigger first failure to enter backoff
        flush_notify_clone.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Trigger second notify — scheduler is now sleeping in 1s backoff
        flush_notify_clone.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Signal shutdown while scheduler is in backoff sleep
        shutdown_tx.send(true).unwrap();

        // Scheduler should exit promptly despite being in backoff
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("scheduler should exit within 2s during backoff")
            .unwrap();
    }
}
