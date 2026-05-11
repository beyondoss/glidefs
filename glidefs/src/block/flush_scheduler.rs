//! Per-export flush scheduler.
//!
//! Each export runs one scheduler that handles two concerns:
//! - **Pack-size flush** (event-driven): when dirty blocks reach the per-export
//!   `flush_threshold` threshold, the write path notifies the scheduler to flush
//!   packs + sync manifest to S3. Disabled in manual mode (flush_threshold = 0).
//! - **Local checkpoint** (demand-driven, 5s interval when active): persists block
//!   states and truncates the WAL. Only runs when dirty blocks or a pending manifest
//!   sync exist. Idle exports consume zero timer resources.
//!
//! Manifest sync happens after every successful pack upload so that flushed packs
//! are immediately discoverable on cross-host recovery (host death without drain).

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{Notify, watch};
use tracing::{info, warn};

use crate::block::cache::BlockCache;
use crate::block::content_store::ContentStore;
use crate::block::metrics::ExportMetrics;
use crate::block::pack_index_cache::PackIndexCache;
use crate::block::state::Active;
use crate::block::volume_manifest::VolumeManifest;
use crate::block::write_cache::WriteCache;

/// Maximum backoff between flush retries.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Duration used to "park" the checkpoint timer when idle. Must be large
/// enough to never fire during normal operation, but small enough that
/// `Instant::now() + FAR_FUTURE` does not overflow `Instant`'s internal
/// representation. 10 years is safe on all platforms.
const FAR_FUTURE: Duration = Duration::from_secs(86400 * 365 * 10);

/// Result of a flush-and-sync cycle.
struct FlushResult {
    /// Number of packs uploaded to S3 (0 = no dirty blocks or all deduped).
    packs_uploaded: usize,
    /// Whether the manifest was successfully synced to S3.
    manifest_synced: bool,
    /// Another host owns this export's manifest (ETag conflict).
    /// When true, the scheduler should stop flushing entirely.
    manifest_conflict: bool,
}

/// Execute flush_packs + sync_manifest (with retries).
///
/// Must be called while holding the per-export flush lock. On success: resets
/// backoff, records metrics, syncs manifest with 3 retries. On failure: extends
/// exponential backoff, records error metric.
///
/// Returns `Some(result)` on flush success, `None` on flush failure.
#[allow(clippy::too_many_arguments)]
async fn flush_and_sync(
    cache: &WriteCache<Active>,
    content_store: &ContentStore,
    pack_index_cache: &Arc<PackIndexCache>,
    volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
    clean_cache: &Arc<dyn BlockCache>,
    metrics: &ExportMetrics,
    flush_backoff: &mut Duration,
    last_flush_failure: &mut Option<tokio::time::Instant>,
) -> Option<FlushResult> {
    match cache
        .flush_packs(content_store, pack_index_cache, volume_manifest, Some(clean_cache))
        .await
    {
        Ok((stats, _seq_cutpoint)) => {
            *flush_backoff = Duration::ZERO;
            *last_flush_failure = None;
            metrics.record_flush_blocks_cas_failed(stats.blocks_cas_failed);
            metrics.record_flush_blocks_crc_mismatched(stats.blocks_crc_mismatched);

            let mut manifest_synced = false;
            let mut manifest_conflict = false;
            if stats.packs_uploaded > 0 || stats.packs_skipped > 0 {
                info!(
                    packs = stats.packs_uploaded,
                    packs_skipped = stats.packs_skipped,
                    blocks = stats.blocks_claimed,
                    blocks_cross_deduped = stats.blocks_cross_deduped,
                    bytes = stats.bytes_uploaded,
                    "flushed packs"
                );
                for attempt in 0..3u32 {
                    match cache
                        .sync_manifest(content_store, volume_manifest)
                        .await
                    {
                        Ok(()) => {
                            manifest_synced = true;
                            metrics.record_manifest_synced();
                            break;
                        }
                        Err(e) if e.is_manifest_conflict() => {
                            tracing::error!(
                                "manifest ETag conflict — another host owns this export, \
                                 stopping flush scheduler"
                            );
                            metrics.record_manifest_sync_error();
                            manifest_conflict = true;
                            break;
                        }
                        Err(e) => {
                            metrics.record_manifest_sync_error();
                            warn!(
                                error = %e, attempt = attempt + 1,
                                "manifest sync failed"
                            );
                            if attempt < 2 {
                                tokio::time::sleep(Duration::from_millis(
                                    100 * (1 << attempt),
                                ))
                                .await;
                            }
                        }
                    }
                }
            }

            Some(FlushResult {
                packs_uploaded: stats.packs_uploaded,
                manifest_synced,
                manifest_conflict,
            })
        }
        Err(e) => {
            *flush_backoff = if flush_backoff.is_zero() {
                Duration::from_secs(1)
            } else {
                flush_backoff.saturating_mul(2).min(MAX_BACKOFF)
            };
            *last_flush_failure = Some(tokio::time::Instant::now());
            metrics.record_flush_error();
            warn!(
                error = %e,
                backoff_secs = flush_backoff.as_secs(),
                "pack flush failed, backing off"
            );
            None
        }
    }
}

/// Run the flush scheduler for a single export.
///
/// Loops until `shutdown` signals true. Two select branches:
/// 1. `flush_notify` — event-driven pack flush when dirty count crosses threshold
/// 2. Checkpoint timer — demand-driven WAL truncation (5s interval, only when active)
///
/// The checkpoint timer is parked (`Duration::MAX`) when the export has no dirty
/// blocks and no pending manifest sync. This means idle exports consume zero timer
/// resources in tokio's timer wheel — critical for high-density deployments with
/// thousands of mostly-idle exports.
#[allow(clippy::too_many_arguments)]
pub async fn flush_scheduler(
    cache: Arc<WriteCache<Active>>,
    content_store: Arc<ContentStore>,
    pack_index_cache: Arc<PackIndexCache>,
    volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,
    clean_cache: Arc<dyn BlockCache>,
    flush_notify: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<ExportMetrics>,
    flush_semaphore: Option<Arc<tokio::sync::Semaphore>>,
) {
    info!("flush scheduler started");

    const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

    // Backoff state: when flush fails (e.g., S3 down), wait before retrying
    // to avoid a tight spin of failed flush_packs calls.
    let mut flush_backoff = Duration::ZERO;

    // Track when the last flush failure occurred so the checkpoint timer
    // can retry S3 flushes after the backoff has elapsed. Without this,
    // dirty blocks can sit unsynced indefinitely if S3 recovers but no
    // new writes trigger flush_notify.
    let mut last_flush_failure: Option<tokio::time::Instant> = None;

    // Pending manifest sync: if sync_manifest fails after a successful pack
    // flush, we retry on the next checkpoint fire to close the window where
    // packs are on S3 but not referenced by any manifest.
    let mut manifest_pending = false;

    // Demand-driven checkpoint timer. Parked at Duration::MAX when idle (no
    // dirty blocks, no pending manifest). Activated on first write or at
    // startup if WAL recovery left dirty blocks. reset() is O(1) — just
    // moves the entry in tokio's timer wheel.
    let checkpoint_timer = tokio::time::sleep(FAR_FUTURE);
    tokio::pin!(checkpoint_timer);
    let mut checkpoint_active = false;

    // Activate the checkpoint timer (idempotent). First activation includes
    // jitter to spread checkpoint storms across exports.
    macro_rules! activate_checkpoint {
        ($timer:expr, $active:expr) => {
            if !$active {
                let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..5000));
                $timer
                    .as_mut()
                    .reset(tokio::time::Instant::now() + CHECKPOINT_INTERVAL + jitter);
                $active = true;
            }
        };
    }

    // If WAL recovery left dirty blocks, activate the timer immediately so
    // they get checkpointed AND fire `flush_notify` so the event-driven
    // branch actually attempts an S3 flush.
    //
    // Without the `notify_one()`: the checkpoint-timer branch only retries
    // S3 when `flush_backoff > 0 && last_flush_failure.is_some()` (the
    // `should_retry` gate at the timer branch). Both are local state,
    // reset to zero/None at scheduler startup. So a freshly-spawned
    // scheduler with recovered-dirty blocks would do local checkpoints
    // every 5s forever without ever uploading to S3, until a *new*
    // pwrite happened to cross the flush_threshold. That stuck state
    // (observed 2026-05-11: 7 exports with 7 GB of dirty data accruing
    // for hours post-restart) is the bug this branch fixes.
    if cache.dirty_block_count() > 0 {
        activate_checkpoint!(checkpoint_timer, checkpoint_active);
        flush_notify.notify_one();
    }

    loop {
        tokio::select! {
            biased;

            // Shutdown takes priority.
            result = shutdown.changed() => {
                match result {
                    Ok(()) if *shutdown.borrow() => {
                        info!("flush scheduler: shutting down");
                        return;
                    }
                    Err(_) => {
                        // Sender dropped (e.g., ExportRouter dropped without clean
                        // shutdown). Exit immediately to avoid zombie schedulers
                        // that corrupt WAL/metadata files of a replacement server.
                        info!("flush scheduler: sender dropped, exiting");
                        return;
                    }
                    _ => {} // spurious wakeup with value still false
                }
            }

            // Event-driven: write path notifies when dirty count crosses flush_threshold.
            () = flush_notify.notified() => {
                // Writes have landed — ensure the checkpoint timer is running.
                activate_checkpoint!(checkpoint_timer, checkpoint_active);

                // If we're in backoff after a previous failure, wait before retrying.
                if flush_backoff > Duration::ZERO {
                    tokio::select! {
                        biased;
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                info!("flush scheduler: shutting down during backoff");
                                return;
                            }
                        }
                        () = tokio::time::sleep(flush_backoff) => {}
                    }
                }

                let start = std::time::Instant::now();
                let mut packs_uploaded = 0usize;

                // Acquire global flush semaphore to limit how many exports
                // prepare + upload pack data simultaneously (memory bound).
                // Wrapped in select! so shutdown can interrupt the wait.
                let _flush_permit = match &flush_semaphore {
                    Some(sem) => {
                        tokio::select! {
                            biased;
                            result = shutdown.changed() => {
                                if result.is_err() || *shutdown.borrow() {
                                    info!("flush scheduler: shutting down during semaphore wait");
                                    return;
                                }
                                continue; // spurious wakeup
                            }
                            permit = sem.acquire() => Some(permit),
                        }
                    }
                    None => None,
                };

                // If a previous flush's manifest hasn't been synced yet, retry
                // that sync before starting a new flush cycle. The flushing file
                // from the previous flush is kept on disk as a crash-safety net
                // until checkpoint (after manifest sync) deletes it. Starting a
                // new flush would rotate the data file, overwriting the flushing
                // file and destroying that safety net.
                if manifest_pending {
                    let _flush_guard = cache.flush_lock().lock().await;
                    match cache.sync_manifest(&content_store, &volume_manifest).await {
                        Ok(()) => {
                            info!("deferred manifest sync succeeded (flush_notify path)");
                            manifest_pending = false;
                            metrics.record_manifest_synced();
                            metrics.set_manifest_pending(false);
                            if let Err(e) = cache.checkpoint().await {
                                warn!(error = %e, "checkpoint after deferred manifest sync");
                            }
                        }
                        Err(e) if e.is_manifest_conflict() => {
                            tracing::error!(
                                "manifest ETag conflict — another host owns this export, \
                                 stopping flush scheduler"
                            );
                            metrics.record_manifest_sync_error();
                            return;
                        }
                        Err(e) => {
                            metrics.record_manifest_sync_error();
                            warn!(error = %e, "deferred manifest sync retry failed (flush_notify path)");
                        }
                    }
                    continue;
                }

                // Acquire per-export flush lock to serialize with concurrent
                // drain/snapshot operations. Prevents stale manifest uploads.
                {
                    let _flush_guard = cache.flush_lock().lock().await;
                    if let Some(result) = flush_and_sync(
                        &cache, &content_store, &pack_index_cache, &volume_manifest,
                        &clean_cache, &metrics, &mut flush_backoff, &mut last_flush_failure,
                    ).await {
                        metrics.record_s3_put_latency(start.elapsed());
                        packs_uploaded = result.packs_uploaded;
                        if result.manifest_conflict {
                            return;
                        }
                        if result.packs_uploaded > 0 {
                            if result.manifest_synced {
                                manifest_pending = false;
                                metrics.set_manifest_pending(false);
                            } else {
                                // Do NOT checkpoint here. checkpoint persists
                                // block states as CLEAN and truncates the WAL. If the
                                // host crashes before manifest sync succeeds, those
                                // blocks become irrecoverable (CLEAN on disk, but S3
                                // manifest doesn't reference the packs). The checkpoint
                                // timer retries manifest sync and will checkpoint after
                                // it succeeds.
                                manifest_pending = true;
                                metrics.set_manifest_pending(true);
                            }
                        } else {
                            // No packs uploaded — still checkpoint to persist
                            // clean block states.
                            if let Err(e) = cache.checkpoint().await {
                                warn!(error = %e, "checkpoint after flush");
                            }
                        }
                    } else {
                        // Flush failed — still checkpoint to prevent WAL growth
                        // when S3 is down and flush_notify fires continuously.
                        if let Err(e) = cache.checkpoint().await {
                            warn!(error = %e, "checkpoint after flush error");
                        }
                    }
                } // _flush_guard dropped — compaction runs without holding the flush lock.

                // Compaction runs outside the flush lock so it doesn't block
                // concurrent drain/snapshot/flush operations.
                if packs_uploaded > 0 {
                    match crate::block::write_cache::compact::compact_if_needed(
                        crate::block::write_cache::compact::DEFAULT_COMPACTION_THRESHOLD,
                        crate::block::write_cache::compact::DEFAULT_DEAD_RATIO_THRESHOLD,
                        &content_store,
                        &pack_index_cache,
                        &volume_manifest,
                        &clean_cache,
                    )
                    .await
                    {
                        Ok(compaction_results) => {
                            for r in &compaction_results {
                                info!(
                                    chunk_idx = r.chunk_idx,
                                    new_pack_id = r.new_pack_id,
                                    live_blocks = r.live_blocks,
                                    new_pack_bytes = r.new_pack_size,
                                    old_packs = r.old_pack_ids.len(),
                                    "compacted chunk — old packs left for GC"
                                );
                            }
                            // Compaction modified the manifest — schedule a sync
                            // so the compacted state is persisted to S3.
                            if !compaction_results.is_empty() {
                                manifest_pending = true;
                                metrics.set_manifest_pending(true);
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "compaction failed, will retry next cycle");
                        }
                    }
                }
            }

            // Demand-driven: checkpoint + retry manifest sync + retry S3 flush.
            // Only fires when dirty blocks or pending manifest exist.
            () = &mut checkpoint_timer => {
                // Retry manifest sync that failed after a previous pack flush.
                if manifest_pending {
                    let _flush_guard = cache.flush_lock().lock().await;
                    match cache.sync_manifest(&content_store, &volume_manifest).await {
                        Ok(()) => {
                            info!("deferred manifest sync succeeded");
                            manifest_pending = false;
                            metrics.record_manifest_synced();
                            metrics.set_manifest_pending(false);
                            // Checkpoint now: persist CLEAN block states and
                            // truncate the WAL. Without this, blocks that were
                            // flushed to S3 remain DIRTY in the .meta file and
                            // WAL entries are never truncated. On recovery,
                            // those blocks would be unnecessarily re-uploaded.
                            if let Err(e) = cache.checkpoint().await {
                                warn!(error = %e, "checkpoint after deferred manifest sync");
                            }
                        }
                        Err(e) if e.is_manifest_conflict() => {
                            tracing::error!(
                                "manifest ETag conflict — another host owns this export, \
                                 stopping flush scheduler"
                            );
                            metrics.record_manifest_sync_error();
                            return;
                        }
                        Err(e) => {
                            metrics.record_manifest_sync_error();
                            warn!(error = %e, "deferred manifest sync retry failed");
                        }
                    }
                } else if cache.dirty_block_count() > 0 {
                    // If a previous flush failed and enough backoff time has
                    // elapsed, retry the S3 flush. This ensures dirty blocks
                    // eventually reach S3 even if no new writes trigger
                    // flush_notify (liveness after S3 recovery).
                    let should_retry = flush_backoff > Duration::ZERO
                        && last_flush_failure
                            .is_some_and(|t| t.elapsed() >= flush_backoff);

                    if should_retry {
                        let _flush_guard = cache.flush_lock().lock().await;
                        if let Some(result) = flush_and_sync(
                            &cache, &content_store, &pack_index_cache, &volume_manifest,
                            &clean_cache, &metrics, &mut flush_backoff, &mut last_flush_failure,
                        ).await {
                            if result.manifest_conflict {
                                return;
                            }
                            if result.packs_uploaded > 0 {
                                manifest_pending = !result.manifest_synced;
                                metrics.set_manifest_pending(manifest_pending);
                            }
                        }
                    }

                    // Always checkpoint locally when dirty.
                    if let Err(e) = cache.checkpoint().await {
                        warn!(error = %e, "local checkpoint failed");
                    }
                }

                // Reschedule if still needed, otherwise park the timer.
                if cache.dirty_block_count() > 0 || manifest_pending {
                    checkpoint_timer
                        .as_mut()
                        .reset(tokio::time::Instant::now() + CHECKPOINT_INTERVAL);
                } else {
                    checkpoint_timer
                        .as_mut()
                        .reset(tokio::time::Instant::now() + FAR_FUTURE);
                    checkpoint_active = false;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::block::cache::{BlockCache, SimpleBlockCache};
    use crate::block::content_store::ContentStore;
    use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use crate::block::pack_index_cache::PackIndexCache;
    use crate::block::state::Initializing;
    use crate::block::volume_manifest::VolumeManifest;
    use crate::block::write_cache::WriteCacheConfig;
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
        /// When true, only single-object `put_opts` fails (manifest uploads).
        /// Multipart uploads (pack data) still succeed. This simulates
        /// "packs upload fine, manifest PUT fails."
        fail_single_puts: AtomicBool,
    }

    impl FailingObjectStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                fail_puts: AtomicBool::new(false),
                fail_single_puts: AtomicBool::new(false),
            }
        }

        fn set_fail_puts(&self, fail: bool) {
            self.fail_puts.store(fail, Ordering::SeqCst);
        }

        fn set_fail_single_puts(&self, fail: bool) {
            self.fail_single_puts.store(fail, Ordering::SeqCst);
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
            // fail_single_puts: only fail manifest paths, not pack uploads.
            // Pack uploads now use single PUT for small packs (content-addressed).
            if self.fail_single_puts.load(Ordering::SeqCst)
                && location.as_ref().contains("manifests/")
            {
                return Err(object_store::Error::Generic {
                    store: "FailingObjectStore",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "Simulated manifest PUT failure",
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
            if self.fail_puts.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "FailingObjectStore",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "Simulated S3 multipart failure",
                    )),
                });
            }
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

    fn device_size() -> u64 {
        128 * 1024 * (DEFAULT_FLUSH_THRESHOLD as u64 + 10)
    }

    #[allow(clippy::type_complexity)]
    async fn test_scheduler_components() -> (
        Arc<WriteCache<Active>>,
        Arc<ContentStore>,
        Arc<PackIndexCache>,
        Arc<parking_lot::RwLock<VolumeManifest>>,
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
            device_size: device_size(),
            block_size: 128 * 1024,
            wal_sync: false,
        };

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = Arc::new(ContentStore::new(s3, "test"));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            device_size(),
            128 * 1024,
        )));
        let metrics = Arc::new(ExportMetrics::new());
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.skip_recovery_for_test());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let flush_notify = Arc::new(Notify::new());

        (
            cache,
            content_store,
            pack_index_cache,
            volume_manifest,
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
    async fn test_scheduler_components_with_store(
        s3: Arc<dyn object_store::ObjectStore>,
    ) -> (
        Arc<WriteCache<Active>>,
        Arc<ContentStore>,
        Arc<PackIndexCache>,
        Arc<parking_lot::RwLock<VolumeManifest>>,
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
            device_size: device_size(),
            block_size: 128 * 1024,
            wal_sync: false,
        };

        let content_store = Arc::new(ContentStore::new(s3, "test"));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            device_size(),
            128 * 1024,
        )));
        let metrics = Arc::new(ExportMetrics::new());
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.skip_recovery_for_test());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let flush_notify = Arc::new(Notify::new());

        (
            cache,
            content_store,
            pack_index_cache,
            volume_manifest,
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
        let (cache, content_store, pack_index_cache, volume_manifest, flush_notify, shutdown_rx, shutdown_tx, metrics, clean_cache, _temp) =
            test_scheduler_components().await;

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                shutdown_rx,
                metrics,
                None,
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
            pack_index_cache,
            volume_manifest,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            _temp,
        ) = test_scheduler_components().await;

        let cache_check = Arc::clone(&cache);
        let flush_notify_clone = Arc::clone(&flush_notify);

        // Write DEFAULT_FLUSH_THRESHOLD dirty blocks
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xAA; 128 * 1024])
                .unwrap();
        }
        assert_eq!(
            cache_check.dirty_block_count(),
            DEFAULT_FLUSH_THRESHOLD as u64
        );

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                shutdown_rx,
                metrics,
                None,
            )
            .await;
        });

        // Notify (simulating what the write path does)
        flush_notify_clone.notify_one();

        // Wait for flush to complete (5s budget — flush_packs + sync_manifest +
        // checkpoint can be slow on loaded CI machines with a single-threaded runtime).
        for _ in 0..100 {
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

    /// Regression test: scheduler must flush blocks left dirty by WAL recovery,
    /// even when no `flush_notify` event comes from outside.
    ///
    /// Production scenario (2026-05-11 incident): daemon restarts with 7 exports
    /// holding ~7GB of dirty data recovered from disk. VMs were idle, so no
    /// pwrite ever crossed the flush_threshold to fire `flush_notify`. The
    /// checkpoint-timer branch's `should_retry` gate requires
    /// `flush_backoff > 0 && last_flush_failure.is_some()` — both reset to
    /// zero/None at scheduler startup, so the timer only did local checkpoints,
    /// never S3 uploads. Result: dirty blocks pinned to disk forever.
    ///
    /// Fix: scheduler fires `flush_notify.notify_one()` at startup if
    /// `dirty_block_count() > 0`.
    #[tokio::test]
    async fn test_recovery_dirty_blocks_flushed_without_external_notify() {
        let (
            cache,
            content_store,
            pack_index_cache,
            volume_manifest,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            _temp,
        ) = test_scheduler_components().await;

        let cache_check = Arc::clone(&cache);

        // Simulate WAL recovery: pre-populate the cache with dirty blocks
        // BEFORE spawning the scheduler. No external party will call
        // flush_notify.notify_one() — the scheduler must self-trigger.
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xCC; 128 * 1024])
                .unwrap();
        }
        assert_eq!(
            cache_check.dirty_block_count(),
            DEFAULT_FLUSH_THRESHOLD as u64,
            "precondition: cache should have dirty blocks from simulated recovery"
        );

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                shutdown_rx,
                metrics,
                None,
            )
            .await;
        });

        // Wait up to 5s for the scheduler to self-trigger and drain.
        // Pre-fix this loop times out; post-fix it drains in <1s.
        let drained = {
            let mut drained = false;
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if cache_check.dirty_block_count() == 0 {
                    drained = true;
                    break;
                }
            }
            drained
        };

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        assert!(
            drained,
            "scheduler must flush recovery-dirty blocks without an external \
             flush_notify; remaining dirty_block_count = {}",
            cache_check.dirty_block_count()
        );
    }

    /// Test that the scheduler backs off exponentially when flush fails.
    #[tokio::test(start_paused = true)]
    async fn test_flush_backoff_on_failure() {
        let failing_s3 = Arc::new(FailingObjectStore::new());
        failing_s3.set_fail_puts(true);

        let (
            cache,
            content_store,
            pack_index_cache,
            volume_manifest,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            _temp,
        ) = test_scheduler_components_with_store(failing_s3.clone() as _).await;

        let metrics_check = Arc::clone(&metrics);
        let flush_notify_clone = Arc::clone(&flush_notify);

        // Write enough dirty blocks to trigger flush
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xBB; 128 * 1024])
                .unwrap();
        }

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                shutdown_rx,
                metrics,
                None,
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
        assert_eq!(
            metrics_check.snapshot().flush_errors,
            1,
            "should still be backing off"
        );
        // After another 600ms (total 1.1s), the backoff has elapsed and flush was attempted
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(
            metrics_check.snapshot().flush_errors,
            2,
            "second flush should have been attempted"
        );

        // Third notify — should wait 2s backoff
        flush_notify_clone.notify_one();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(
            metrics_check.snapshot().flush_errors,
            2,
            "should still be in 2s backoff"
        );
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(
            metrics_check.snapshot().flush_errors,
            3,
            "third flush after 2s backoff"
        );

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
            pack_index_cache,
            volume_manifest,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            _temp,
        ) = test_scheduler_components_with_store(failing_s3.clone() as _).await;

        let metrics_check = Arc::clone(&metrics);
        let cache_check = Arc::clone(&cache);
        let flush_notify_clone = Arc::clone(&flush_notify);
        let _clean_cache_check = Arc::clone(&clean_cache);

        // Write dirty blocks
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xCC; 128 * 1024])
                .unwrap();
        }

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                shutdown_rx,
                metrics,
                None,
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
        assert_eq!(
            cache_check.dirty_block_count(),
            0,
            "flush should have succeeded"
        );
        assert_eq!(metrics_check.snapshot().flush_errors, 1, "no new errors");

        // Write more blocks and fail again — should start from 1s, not 2s
        failing_s3.set_fail_puts(true);
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache_check
                .write(offset, &[0xDD; 128 * 1024])
                .unwrap();
        }

        flush_notify_clone.notify_one();
        // Should fail immediately (backoff was reset to zero by success)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            metrics_check.snapshot().flush_errors,
            2,
            "immediate retry after reset"
        );

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
            pack_index_cache,
            volume_manifest,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            _temp,
        ) = test_scheduler_components_with_store(failing_s3 as _).await;

        let flush_notify_clone = Arc::clone(&flush_notify);

        // Write dirty blocks
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xEE; 128 * 1024])
                .unwrap();
        }

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                shutdown_rx,
                metrics,
                None,
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

    /// Verify that when pack uploads succeed but manifest sync fails, a
    /// simulated crash + recovery still sees the blocks as dirty. This
    /// prevents data loss: blocks are on S3 as unreferenced packs, and the
    /// manifest doesn't know about them. Recovery must re-flush them.
    #[tokio::test(start_paused = true)]
    async fn test_manifest_sync_failure_preserves_dirty_after_crash() {
        let failing_s3 = Arc::new(FailingObjectStore::new());
        // Only fail single-object puts (manifest) — multipart (packs) succeeds.
        failing_s3.set_fail_single_puts(true);

        let (
            cache,
            content_store,
            pack_index_cache,
            volume_manifest,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            temp,
        ) = test_scheduler_components_with_store(failing_s3 as _).await;

        let cache_check = Arc::clone(&cache);
        let flush_notify_clone = Arc::clone(&flush_notify);
        let cache_dir = temp.path().to_path_buf();

        // Write DEFAULT_FLUSH_THRESHOLD dirty blocks.
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xFF; 128 * 1024])
                .unwrap();
        }
        assert_eq!(
            cache_check.dirty_block_count(),
            DEFAULT_FLUSH_THRESHOLD as u64
        );

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                shutdown_rx,
                metrics,
                None,
            )
            .await;
        });

        // Trigger flush — packs upload (multipart OK), manifest fails (single put).
        flush_notify_clone.notify_one();

        // Wait for in-memory dirty count to reach 0 (blocks CAS'd CLEAN).
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if cache_check.dirty_block_count() == 0 {
                break;
            }
        }
        assert_eq!(
            cache_check.dirty_block_count(),
            0,
            "packs should have uploaded (blocks CAS'd CLEAN in memory)"
        );

        // Shut down scheduler.
        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        // Drop all references — simulates host crash.
        drop(cache_check);

        // Reopen cache from same directory — simulates host restart.
        let config = WriteCacheConfig {
            cache_dir,
            device_name: "sched-backoff".to_string(),
            device_size: device_size(),
            block_size: 128 * 1024,
            wal_sync: false,
        };
        let recovered = WriteCache::<Initializing>::open(config).unwrap();
        let recovered = recovered.skip_recovery_for_test();

        // After recovery, blocks MUST still be dirty. The manifest never
        // synced, so these blocks exist as unreferenced packs on S3. If
        // checkpoint() ran after the failed manifest sync, .meta
        // would show CLEAN + WAL truncated = data loss on crash.
        assert!(
            recovered.dirty_block_count() > 0,
            "blocks must be dirty after crash with unsynced manifest (got 0 — data loss bug)"
        );
    }

    /// Prove: shutdown is blocked when flush semaphore is fully held.
    ///
    /// The scheduler's flush_notify handler calls `sem.acquire().await`
    /// with no shutdown check. If all permits are held, shutdown can't
    /// be processed until a permit is released.
    #[tokio::test(start_paused = true)]
    async fn test_shutdown_blocked_by_held_semaphore() {
        let (
            cache,
            content_store,
            pack_index_cache,
            volume_manifest,
            flush_notify,
            shutdown_rx,
            shutdown_tx,
            metrics,
            clean_cache,
            _temp,
        ) = test_scheduler_components().await;

        // Create a semaphore with 1 permit and hold it
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let _held_permit = sem.clone().acquire_owned().await.unwrap();

        // Write dirty blocks so flush_notify triggers a flush attempt
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache.write(offset, &[0xFF; 128 * 1024]).unwrap();
        }

        let flush_notify_clone = Arc::clone(&flush_notify);
        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache,
                content_store,
                pack_index_cache,
                volume_manifest,
                clean_cache,
                flush_notify,
                shutdown_rx,
                metrics,
                Some(sem),
            )
            .await;
        });

        // Trigger flush — scheduler will block on semaphore acquire
        flush_notify_clone.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Signal shutdown while scheduler is blocked on semaphore
        shutdown_tx.send(true).unwrap();

        // BUG: scheduler can't exit because it's stuck in sem.acquire().await
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(
            result.is_ok(),
            "BUG: scheduler should exit within 5s but is stuck on semaphore"
        );
    }
}
