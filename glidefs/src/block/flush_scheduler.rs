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

use std::collections::{HashMap, HashSet};
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
    /// Another host owns this export's manifest (ETag conflict).
    /// When true, the scheduler should stop flushing entirely.
    manifest_conflict: bool,
    /// Chunk indices written this cycle. Feeds the per-chunk idle-age map used
    /// by cooldown compaction.
    touched_chunks: HashSet<u32>,
}

/// Execute the atomic flush+manifest+checkpoint cycle on the cache.
///
/// Must be called while holding the per-export flush lock. `flush_packs`
/// is now atomic — packs upload, manifest sync (with retries), eviction,
/// and checkpoint all happen inside before it returns. On success: resets
/// backoff, records metrics. On failure: extends exponential backoff,
/// records error metric.
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
    // Hold flush_lock for the atomic flush_packs cycle so any caller
    // that needs a serialization point (e.g. snapshot, drain) can wait
    // by acquiring the same lock.
    let _flush_guard = cache.flush_lock().lock().await;

    match cache
        .flush_packs(content_store, pack_index_cache, volume_manifest, Some(clean_cache))
        .await
    {
        Ok((stats, _seq_cutpoint)) => {
            *flush_backoff = Duration::ZERO;
            *last_flush_failure = None;
            metrics.record_flush_blocks_cas_failed(stats.blocks_cas_failed);
            metrics.record_flush_blocks_crc_mismatched(stats.blocks_crc_mismatched);

            if stats.packs_uploaded > 0 || stats.packs_skipped > 0 {
                info!(
                    packs = stats.packs_uploaded,
                    packs_skipped = stats.packs_skipped,
                    blocks = stats.blocks_claimed,
                    blocks_cross_deduped = stats.blocks_cross_deduped,
                    bytes = stats.bytes_uploaded,
                    "flushed packs"
                );
                if stats.packs_uploaded > 0 {
                    metrics.record_manifest_synced();
                }
            }

            Some(FlushResult {
                packs_uploaded: stats.packs_uploaded,
                manifest_conflict: false,
                touched_chunks: stats.touched_chunks,
            })
        }
        Err(e) if e.is_manifest_conflict() => {
            tracing::error!(
                "manifest ETag conflict — another host owns this export, \
                 stopping flush scheduler"
            );
            metrics.record_manifest_sync_error();
            Some(FlushResult {
                packs_uploaded: 0,
                manifest_conflict: true,
                touched_chunks: HashSet::new(),
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
    flush_threshold: usize,
    compaction_cooldown: u64,
) {
    info!("flush scheduler started");

    const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

    // Per-chunk idle age (flush cycles since last written) for cooldown
    // compaction. Ephemeral: empty on (re)start — every chunk then looks
    // freshly written (age 0) and dead-ratio compaction defers for up to
    // `compaction_cooldown` cycles, which is the safe direction. Unused when
    // `compaction_cooldown == 0`. Bounded to multi-pack chunks (pruned below).
    let mut chunk_idle_age: HashMap<u32, u64> = HashMap::new();

    // Backoff state: when flush fails (e.g., S3 down), wait before retrying
    // to avoid a tight spin of failed flush_packs calls.
    let mut flush_backoff = Duration::ZERO;

    // Track when the last flush failure occurred so the checkpoint timer
    // can retry S3 flushes after the backoff has elapsed. Without this,
    // dirty blocks can sit unsynced indefinitely if S3 recovers but no
    // new writes trigger flush_notify.
    let mut last_flush_failure: Option<tokio::time::Instant> = None;

    // Demand-driven checkpoint timer. Parked at Duration::MAX when idle (no
    // dirty blocks). Activated on first write or at startup if WAL recovery
    // left dirty blocks. reset() is O(1) — just moves the entry in tokio's
    // timer wheel.
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
        // Only fire flush_notify when this export auto-flushes. Manual mode
        // (`flush_threshold == 0`) means the operator drives drains explicitly
        // and would be surprised by a startup auto-flush — see the regression
        // test `test_manual_mode_export_no_auto_flush`.
        if flush_threshold > 0 {
            flush_notify.notify_one();
        }
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
                let mut touched_chunks: HashSet<u32> = HashSet::new();

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

                // Acquire per-export flush lock to serialize with concurrent
                // drain/snapshot operations. flush_and_sync is now atomic:
                // the cache itself uploads packs, syncs the manifest, evicts
                // SYNCING blocks, and checkpoints in one sequence under the
                // flush lock. The scheduler no longer needs a deferred
                // manifest-sync retry (the cache retries internally), and on
                // success the checkpoint is already done.
                {
                    if let Some(result) = flush_and_sync(
                        &cache, &content_store, &pack_index_cache, &volume_manifest,
                        &clean_cache, &metrics, &mut flush_backoff, &mut last_flush_failure,
                    ).await {
                        metrics.record_s3_put_latency(start.elapsed());
                        packs_uploaded = result.packs_uploaded;
                        touched_chunks = result.touched_chunks;
                        if result.manifest_conflict {
                            return;
                        }
                        if result.packs_uploaded == 0 {
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
                } // compaction runs without holding the flush lock.

                // Advance per-chunk idle ages for cooldown compaction: every
                // tracked chunk ages one cycle, chunks written this cycle reset
                // to 0, then prune chunks no longer multi-pack (compacted away or
                // never a candidate) to bound the map to the churning set. No-op
                // when cooldown is disabled.
                if compaction_cooldown > 0 && packs_uploaded > 0 {
                    for age in chunk_idle_age.values_mut() {
                        *age += 1;
                    }
                    for &c in &touched_chunks {
                        chunk_idle_age.insert(c, 0);
                    }
                    let vm = volume_manifest.read();
                    chunk_idle_age
                        .retain(|idx, _| vm.chunks.get(idx).is_some_and(|e| e.packs.len() >= 2));
                }

                // Compaction runs outside the flush lock so it doesn't block
                // concurrent drain/snapshot/flush operations.
                if packs_uploaded > 0 {
                    match crate::block::write_cache::compact::compact_if_needed(
                        crate::block::write_cache::compact::DEFAULT_COMPACTION_THRESHOLD,
                        crate::block::write_cache::compact::DEFAULT_DEAD_RATIO_THRESHOLD,
                        compaction_cooldown,
                        &chunk_idle_age,
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
                            // Compaction modified the in-memory manifest —
                            // sync it to S3 immediately. flush_packs's
                            // atomic sync only runs when there are dirty
                            // blocks, so compaction-only changes need
                            // their own sync.
                            if !compaction_results.is_empty() {
                                let _flush_guard = cache.flush_lock().lock().await;
                                let mut last_err: Option<crate::block::write_cache::CacheError> = None;
                                for attempt in 0..3u32 {
                                    match cache.sync_manifest(&content_store, &volume_manifest).await {
                                        Ok(()) => {
                                            metrics.record_manifest_synced();
                                            last_err = None;
                                            break;
                                        }
                                        Err(e) if e.is_manifest_conflict() => {
                                            tracing::error!(
                                                "manifest ETag conflict after compaction \
                                                 — another host owns this export, stopping \
                                                 flush scheduler"
                                            );
                                            metrics.record_manifest_sync_error();
                                            return;
                                        }
                                        Err(e) => {
                                            metrics.record_manifest_sync_error();
                                            warn!(
                                                error = %e, attempt = attempt + 1,
                                                "post-compaction manifest sync failed, retrying"
                                            );
                                            last_err = Some(e);
                                            if attempt < 2 {
                                                tokio::time::sleep(Duration::from_millis(
                                                    100 * (1 << attempt),
                                                ))
                                                .await;
                                            }
                                        }
                                    }
                                }
                                if let Some(e) = last_err {
                                    warn!(
                                        error = %e,
                                        "post-compaction manifest sync failed after 3 attempts; \
                                         will retry on next compaction or flush"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "compaction failed, will retry next cycle");
                        }
                    }
                }
            }

            // Demand-driven: retry S3 flush after backoff, otherwise checkpoint.
            // Only fires when dirty blocks exist.
            () = &mut checkpoint_timer => {
                if cache.dirty_block_count() > 0 {
                    // If a previous flush failed and enough backoff time has
                    // elapsed, retry the S3 flush. This ensures dirty blocks
                    // eventually reach S3 even if no new writes trigger
                    // flush_notify (liveness after S3 recovery).
                    let should_retry = flush_backoff > Duration::ZERO
                        && last_flush_failure
                            .is_some_and(|t| t.elapsed() >= flush_backoff);

                    if should_retry
                        && let Some(result) = flush_and_sync(
                            &cache, &content_store, &pack_index_cache, &volume_manifest,
                            &clean_cache, &metrics, &mut flush_backoff, &mut last_flush_failure,
                        ).await
                            && result.manifest_conflict
                        {
                            return;
                        }

                    // Always checkpoint locally when dirty.
                    if let Err(e) = cache.checkpoint().await {
                        warn!(error = %e, "local checkpoint failed");
                    }
                }

                // Reschedule if still needed, otherwise park the timer.
                if cache.dirty_block_count() > 0 {
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
                0, // flush_threshold (manual-mode-equivalent for tests that fire their own notify)
                0, // compaction_cooldown (disabled in scheduler tests)
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
                0, // flush_threshold (manual-mode-equivalent for tests that fire their own notify)
                0, // compaction_cooldown (disabled in scheduler tests)
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
                DEFAULT_FLUSH_THRESHOLD, // auto-flush mode — startup must self-trigger
                0, // compaction_cooldown (disabled in scheduler tests)
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

    /// Companion to the above: in manual mode (`flush_threshold == 0`), the
    /// scheduler must NOT auto-flush recovery-dirty blocks at startup. The
    /// operator drives drains explicitly via `drain_export`.
    #[tokio::test]
    async fn test_manual_mode_recovery_does_not_auto_flush() {
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

        // Pre-populate dirty blocks just like the auto-flush regression test.
        for i in 0..DEFAULT_FLUSH_THRESHOLD {
            let offset = i as u64 * 128 * 1024;
            cache
                .write(offset, &[0xDD; 128 * 1024])
                .unwrap();
        }
        let expected_dirty = DEFAULT_FLUSH_THRESHOLD as u64;
        assert_eq!(cache_check.dirty_block_count(), expected_dirty);

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
                0, // manual mode — scheduler must NOT auto-fire flush_notify
                0, // compaction_cooldown (disabled in scheduler tests)
            )
            .await;
        });

        // Generous wait: if the scheduler were going to leak an auto-flush, it
        // would have happened well before this.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let remaining = cache_check.dirty_block_count();
        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        assert_eq!(
            remaining, expected_dirty,
            "manual-mode scheduler must not flush recovery-dirty blocks; \
             expected {expected_dirty} to remain, found {remaining}"
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
                0, // flush_threshold (manual-mode-equivalent for tests that fire their own notify)
                0, // compaction_cooldown (disabled in scheduler tests)
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
                0, // flush_threshold (manual-mode-equivalent for tests that fire their own notify)
                0, // compaction_cooldown (disabled in scheduler tests)
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
                0, // flush_threshold (manual-mode-equivalent for tests that fire their own notify)
                0, // compaction_cooldown (disabled in scheduler tests)
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

    /// Verify that when pack uploads succeed but manifest sync fails,
    /// blocks remain dirty (in memory and across a simulated crash).
    /// With atomic flush, the manifest sync happens INSIDE
    /// flush_dirty_body BEFORE eviction; failure propagates as Err and
    /// outer recovery re-dirties the SYNCING blocks via the flushing
    /// file. So we never observe "dirty=0 but manifest stale" — that
    /// window no longer exists.
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
        let metrics_check = Arc::clone(&metrics);
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
                0, // flush_threshold (manual-mode-equivalent for tests that fire their own notify)
                0, // compaction_cooldown (disabled in scheduler tests)
            )
            .await;
        });

        // Trigger flush — packs upload (multipart OK), manifest fails
        // (single put). flush_dirty_body returns Err inside the atomic
        // sync step; outer recovery re-dirties the SYNCING blocks.
        flush_notify_clone.notify_one();

        // Wait for the scheduler to record the flush failure.
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if metrics_check.snapshot().flush_errors > 0 {
                break;
            }
        }
        assert!(
            metrics_check.snapshot().flush_errors > 0,
            "scheduler should have recorded a flush failure"
        );
        assert!(
            cache_check.dirty_block_count() > 0,
            "blocks must remain dirty after manifest-sync failure (atomic flush \
             returned Err before eviction; outer recovery re-dirtied)"
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
        // synced, so these blocks exist as unreferenced packs on S3.
        // The flushing file is preserved as a crash-safety net.
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
                0, // flush_threshold (manual-mode-equivalent for tests that fire their own notify)
                0, // compaction_cooldown (disabled in scheduler tests)
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
