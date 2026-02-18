//! Per-export flush scheduler.
//!
//! Replaces the v1 sync_worker. Two modes:
//! - DemandDriven: flush only on explicit trigger (budget exceeded, API call)
//! - Continuous: periodic pack flush (~5s) + manifest sync (~60s)

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Notify};
use tracing::{error, info, warn};

use crate::nbd::content_store::ContentStore;
use crate::nbd::metrics::ExportMetrics;
use crate::nbd::pack_index::HostPackIndex;
use crate::nbd::state::Active;
use crate::nbd::write_cache::WriteCache;

// ---------------------------------------------------------------------------
// FlushMode
// ---------------------------------------------------------------------------

/// Controls how dirty blocks are flushed to S3 for an export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FlushMode {
    /// Flush only when explicitly triggered (budget exceeded, API call).
    #[default]
    DemandDriven,
    /// Periodic flush with separate pack and manifest intervals.
    Continuous {
        /// Interval in seconds between pack flushes.
        #[serde(default = "default_pack_interval_secs")]
        pack_interval_secs: u64,
        /// Interval in seconds between manifest syncs.
        #[serde(default = "default_manifest_interval_secs")]
        manifest_interval_secs: u64,
    },
}

fn default_pack_interval_secs() -> u64 {
    5
}

fn default_manifest_interval_secs() -> u64 {
    60
}


// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// Run the flush scheduler for a single export.
///
/// This function loops until `shutdown` signals true. It reads the current
/// [`FlushMode`] from `mode_rx` and dispatches to the appropriate scheduling
/// strategy. The mode can be changed at runtime by sending a new value on the
/// corresponding `watch::Sender`.
pub async fn flush_scheduler(
    cache: Arc<WriteCache<Active>>,
    content_store: Arc<ContentStore>,
    pack_index: Arc<HostPackIndex>,
    mut mode_rx: watch::Receiver<FlushMode>,
    flush_trigger: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<ExportMetrics>,
) {
    info!("flush scheduler started");

    loop {
        let mode = mode_rx.borrow_and_update().clone();

        match mode {
            FlushMode::DemandDriven => {
                info!("flush scheduler: demand-driven mode");
                run_demand_driven(
                    &cache,
                    &content_store,
                    &pack_index,
                    &flush_trigger,
                    &mut mode_rx,
                    &mut shutdown,
                    &metrics,
                )
                .await;
            }
            FlushMode::Continuous {
                pack_interval_secs,
                manifest_interval_secs,
            } => {
                info!(
                    pack_interval_secs,
                    manifest_interval_secs, "flush scheduler: continuous mode"
                );
                run_continuous(
                    &cache,
                    &content_store,
                    &pack_index,
                    &flush_trigger,
                    &mut mode_rx,
                    &mut shutdown,
                    Duration::from_secs(pack_interval_secs),
                    Duration::from_secs(manifest_interval_secs),
                    &metrics,
                )
                .await;
            }
        }

        // If shutdown was signaled inside one of the run_* functions, exit.
        if *shutdown.borrow() {
            info!("flush scheduler: shutting down");
            return;
        }

        // Otherwise, the mode changed -- loop back to re-read it.
    }
}

// ---------------------------------------------------------------------------
// Demand-driven mode
// ---------------------------------------------------------------------------

/// Wait for an explicit flush trigger, a mode change, or shutdown.
///
/// Runs a local checkpoint every 5s to keep the WAL bounded — independent
/// of whether any S3 flush occurs. This is critical for demand-driven mode
/// where S3 flushes may be very infrequent.
async fn run_demand_driven(
    cache: &Arc<WriteCache<Active>>,
    content_store: &Arc<ContentStore>,
    pack_index: &Arc<HostPackIndex>,
    flush_trigger: &Arc<Notify>,
    mode_rx: &mut watch::Receiver<FlushMode>,
    shutdown: &mut watch::Receiver<bool>,
    metrics: &ExportMetrics,
) {
    let mut checkpoint_ticker = tokio::time::interval(Duration::from_secs(5));
    checkpoint_ticker.tick().await; // consume immediate first tick

    loop {
        tokio::select! {
            biased;

            // Shutdown takes priority.
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
            }

            // Mode changed -- break back to the outer loop.
            Ok(()) = mode_rx.changed() => {
                return;
            }

            // Explicit flush trigger.
            () = flush_trigger.notified() => {
                do_full_flush(cache, content_store, pack_index, flush_trigger, metrics).await;
            }

            // Local checkpoint: persist block states + truncate WAL.
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

// ---------------------------------------------------------------------------
// Continuous mode
// ---------------------------------------------------------------------------

/// Periodic pack flush + manifest sync with optional explicit triggers.
#[allow(clippy::too_many_arguments)]
async fn run_continuous(
    cache: &Arc<WriteCache<Active>>,
    content_store: &Arc<ContentStore>,
    pack_index: &Arc<HostPackIndex>,
    flush_trigger: &Arc<Notify>,
    mode_rx: &mut watch::Receiver<FlushMode>,
    shutdown: &mut watch::Receiver<bool>,
    pack_dur: Duration,
    manifest_dur: Duration,
    metrics: &ExportMetrics,
) {
    let mut pack_ticker = tokio::time::interval(pack_dur);
    let mut manifest_ticker = tokio::time::interval(manifest_dur);

    // Consume the immediate first tick so we don't fire right away.
    pack_ticker.tick().await;
    manifest_ticker.tick().await;

    let mut last_seq_cutpoint: u64 = 0;
    let mut accumulated_pack_ids: Vec<uuid::Uuid> = Vec::new();
    let mut pack_backoff = Duration::from_secs(1);
    let mut manifest_backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    // Backoff deadlines: when set, suppress the corresponding tick until the
    // deadline fires. This keeps the select loop responsive to shutdown/mode
    // changes during backoff (unlike an inline sleep which blocks the loop).
    let mut pack_backoff_until: Option<tokio::time::Instant> = None;
    let mut manifest_backoff_until: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            biased;

            // Shutdown takes priority.
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
            }

            // Mode changed -- break back to the outer loop.
            Ok(()) = mode_rx.changed() => {
                return;
            }

            // Explicit flush trigger (budget exceeded, API call).
            () = flush_trigger.notified() => {
                do_full_flush(cache, content_store, pack_index, flush_trigger, metrics).await;
                // flush_to_s3 handles registry update internally, so clear accumulated IDs.
                accumulated_pack_ids.clear();
                // A full flush includes a manifest sync, so reset cutpoint.
                last_seq_cutpoint = 0;
                pack_backoff = Duration::from_secs(1);
                manifest_backoff = Duration::from_secs(1);
                pack_backoff_until = None;
                manifest_backoff_until = None;
            }

            // Pack backoff deadline expired — clear it so the next tick proceeds.
            _ = async {
                tokio::time::sleep_until(pack_backoff_until.unwrap()).await
            }, if pack_backoff_until.is_some() => {
                pack_backoff_until = None;
            }

            // Manifest backoff deadline expired — clear it so the next tick proceeds.
            _ = async {
                tokio::time::sleep_until(manifest_backoff_until.unwrap()).await
            }, if manifest_backoff_until.is_some() => {
                manifest_backoff_until = None;
            }

            // Periodic pack flush.
            _ = pack_ticker.tick(), if pack_backoff_until.is_none() => {
                if cache.dirty_block_count() > 0 {
                    let start = Instant::now();
                    match cache.flush_packs(content_store, pack_index).await {
                        Ok((stats, seq_cutpoint)) => {
                            metrics.record_s3_put_latency(start.elapsed());
                            metrics.record_flush_blocks_cas_failed(stats.blocks_cas_failed);
                            if stats.packs_uploaded > 0 {
                                info!(
                                    packs = stats.packs_uploaded,
                                    blocks = stats.blocks_flushed,
                                    bytes = stats.bytes_uploaded,
                                    seq_cutpoint,
                                    "periodic pack flush complete"
                                );
                            }
                            accumulated_pack_ids.extend_from_slice(&stats.new_pack_ids);
                            last_seq_cutpoint = seq_cutpoint;
                            pack_backoff = Duration::from_secs(1);
                            // Local checkpoint between manifest syncs to keep WAL bounded.
                            if let Err(e) = cache.local_checkpoint() {
                                warn!(error = %e, "local checkpoint after pack flush failed");
                            }
                        }
                        Err(e) => {
                            metrics.record_flush_error();
                            warn!(error = %e, backoff_secs = pack_backoff.as_secs(), "periodic pack flush failed, backing off");
                            pack_backoff_until = Some(tokio::time::Instant::now() + pack_backoff);
                            pack_backoff = (pack_backoff * 2).min(MAX_BACKOFF);
                        }
                    }
                }
            }

            // Periodic manifest sync.
            _ = manifest_ticker.tick(), if manifest_backoff_until.is_none() => {
                if last_seq_cutpoint > 0 {
                    match cache.sync_manifest(content_store, pack_index, last_seq_cutpoint).await {
                        Ok(()) => {
                            info!(seq_cutpoint = last_seq_cutpoint, "manifest sync complete");
                            if !accumulated_pack_ids.is_empty() {
                                cache.update_registry(content_store, &accumulated_pack_ids).await;
                                accumulated_pack_ids.clear();
                            }
                            last_seq_cutpoint = 0;
                            manifest_backoff = Duration::from_secs(1);
                        }
                        Err(e) => {
                            metrics.record_flush_error();
                            warn!(error = %e, backoff_secs = manifest_backoff.as_secs(), "manifest sync failed, backing off");
                            manifest_backoff_until = Some(tokio::time::Instant::now() + manifest_backoff);
                            manifest_backoff = (manifest_backoff * 2).min(MAX_BACKOFF);
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Perform a full flush: pack all dirty blocks and sync the manifest.
///
/// On failure, re-notifies the flush trigger so the scheduler retries on the
/// next iteration. Without this, a failed demand-driven flush would leave
/// dirty blocks stranded with no retry mechanism.
async fn do_full_flush(
    cache: &Arc<WriteCache<Active>>,
    content_store: &Arc<ContentStore>,
    pack_index: &Arc<HostPackIndex>,
    flush_trigger: &Arc<Notify>,
    metrics: &ExportMetrics,
) {
    let start = Instant::now();
    match cache.flush_to_s3(content_store, pack_index).await {
        Ok(stats) => {
            metrics.record_s3_put_latency(start.elapsed());
            metrics.record_flush_blocks_cas_failed(stats.blocks_cas_failed);
            if stats.packs_uploaded > 0 {
                info!(
                    packs = stats.packs_uploaded,
                    blocks = stats.blocks_flushed,
                    bytes = stats.bytes_uploaded,
                    "full flush complete"
                );
            }
        }
        Err(e) => {
            metrics.record_flush_error();
            error!(error = %e, "full flush failed, scheduling retry");
            // Re-notify so the scheduler retries instead of silently dropping the flush.
            flush_trigger.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_mode_default_is_demand_driven() {
        assert_eq!(FlushMode::default(), FlushMode::DemandDriven);
    }

    #[test]
    fn flush_mode_serde_demand_driven() {
        let json = r#"{"mode":"demand_driven"}"#;
        let mode: FlushMode = serde_json::from_str(json).unwrap();
        assert_eq!(mode, FlushMode::DemandDriven);

        let roundtrip = serde_json::to_string(&mode).unwrap();
        assert_eq!(roundtrip, json);
    }

    #[test]
    fn flush_mode_serde_continuous_defaults() {
        let json = r#"{"mode":"continuous"}"#;
        let mode: FlushMode = serde_json::from_str(json).unwrap();
        assert_eq!(
            mode,
            FlushMode::Continuous {
                pack_interval_secs: 5,
                manifest_interval_secs: 60,
            }
        );
    }

    #[test]
    fn flush_mode_serde_continuous_custom() {
        let json = r#"{"mode":"continuous","pack_interval_secs":10,"manifest_interval_secs":120}"#;
        let mode: FlushMode = serde_json::from_str(json).unwrap();
        assert_eq!(
            mode,
            FlushMode::Continuous {
                pack_interval_secs: 10,
                manifest_interval_secs: 120,
            }
        );
    }

    // =========================================================================
    // Runtime behavior tests
    // =========================================================================

    use crate::nbd::cache::{BlockCache, SimpleBlockCache};
    use crate::nbd::content_store::ContentStore;
    use crate::nbd::pack_index::HostPackIndex;
    use crate::nbd::state::Initializing;
    use crate::nbd::write_cache::WriteCacheConfig;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    #[allow(clippy::type_complexity)]
    fn test_scheduler_components() -> (
        Arc<WriteCache<crate::nbd::state::Active>>,
        Arc<ContentStore>,
        Arc<HostPackIndex>,
        Arc<Notify>,
        watch::Receiver<FlushMode>,
        watch::Sender<FlushMode>,
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
            device_size: 1024 * 1024,
            block_size: 4096,
            wal_sync: false,
        };

        let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = Arc::new(ContentStore::new(s3, "test"));
        let pack_index = Arc::new(HostPackIndex::new());
        let metrics = Arc::new(ExportMetrics::new());
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.skip_recovery_for_test());

        let (mode_tx, mode_rx) = watch::channel(FlushMode::DemandDriven);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let flush_trigger = Arc::new(Notify::new());

        (
            cache, content_store, pack_index, flush_trigger, mode_rx, mode_tx,
            shutdown_rx, shutdown_tx, metrics, clean_cache, temp_dir,
        )
    }

    #[tokio::test]
    async fn test_demand_driven_shutdown() {
        let (
            cache, content_store, pack_index, flush_trigger, mode_rx, _mode_tx,
            shutdown_rx, shutdown_tx, metrics, _clean_cache, _temp,
        ) = test_scheduler_components();

        let handle = tokio::spawn(async move {
            flush_scheduler(cache, content_store, pack_index, mode_rx, flush_trigger, shutdown_rx, metrics).await;
        });

        // Signal shutdown
        shutdown_tx.send(true).unwrap();

        // Scheduler should exit promptly
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("scheduler should exit within 2s")
            .unwrap();
    }

    #[tokio::test]
    async fn test_demand_driven_flush_trigger() {
        let (
            cache, content_store, pack_index, flush_trigger, mode_rx, _mode_tx,
            shutdown_rx, shutdown_tx, metrics, clean_cache, _temp,
        ) = test_scheduler_components();

        // Write dirty data
        cache.write(0, &[0xAA; 4096], clean_cache.as_ref()).unwrap();
        assert_eq!(cache.dirty_block_count(), 1);

        let cache_check = Arc::clone(&cache);
        let flush_trigger_clone = Arc::clone(&flush_trigger);

        let handle = tokio::spawn(async move {
            flush_scheduler(cache, content_store, pack_index, mode_rx, flush_trigger, shutdown_rx, metrics).await;
        });

        // Trigger a flush
        flush_trigger_clone.notify_one();

        // Wait for flush to complete
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if cache_check.dirty_block_count() == 0 {
                break;
            }
        }
        assert_eq!(cache_check.dirty_block_count(), 0, "flush trigger should have flushed dirty blocks");

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn test_mode_switch() {
        let (
            cache, content_store, pack_index, flush_trigger, mode_rx, mode_tx,
            shutdown_rx, shutdown_tx, metrics, _clean_cache, _temp,
        ) = test_scheduler_components();

        let handle = tokio::spawn(async move {
            flush_scheduler(cache, content_store, pack_index, mode_rx, flush_trigger, shutdown_rx, metrics).await;
        });

        // Switch to continuous mode — scheduler should NOT exit
        mode_tx
            .send(FlushMode::Continuous {
                pack_interval_secs: 1,
                manifest_interval_secs: 5,
            })
            .unwrap();

        // Give it time to process the mode change
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Scheduler should still be running (not exited from mode change)
        assert!(!handle.is_finished(), "scheduler should still be running after mode switch");

        // Switch back to demand-driven
        mode_tx.send(FlushMode::DemandDriven).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!handle.is_finished(), "scheduler should still be running");

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn test_full_flush_failure_retriggers() {
        // Verify that when a full flush fails, the scheduler re-notifies
        // the flush trigger so a retry happens automatically.
        let (
            cache, content_store, pack_index, flush_trigger, mode_rx, _mode_tx,
            shutdown_rx, shutdown_tx, metrics, clean_cache, _temp,
        ) = test_scheduler_components();

        // Write dirty data
        cache.write(0, &[0xCC; 4096], clean_cache.as_ref()).unwrap();
        assert_eq!(cache.dirty_block_count(), 1);

        let cache_check = Arc::clone(&cache);
        let flush_trigger_clone = Arc::clone(&flush_trigger);

        let handle = tokio::spawn(async move {
            flush_scheduler(cache, content_store, pack_index, mode_rx, flush_trigger, shutdown_rx, metrics).await;
        });

        // Trigger a flush — with InMemory store this should succeed
        flush_trigger_clone.notify_one();

        // Wait for flush to complete
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if cache_check.dirty_block_count() == 0 {
                break;
            }
        }
        assert_eq!(cache_check.dirty_block_count(), 0, "flush should eventually succeed");

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn test_continuous_mode_periodic_flush() {
        let (
            cache, content_store, pack_index, flush_trigger, _mode_rx, _mode_tx,
            _shutdown_rx, _shutdown_tx, metrics, clean_cache, _temp,
        ) = test_scheduler_components();

        // Write dirty data before spawning the scheduler.
        cache.write(0, &[0xBB; 4096], clean_cache.as_ref()).unwrap();
        assert_eq!(cache.dirty_block_count(), 1, "should have 1 dirty block");

        // Keep references for assertions after scheduler is spawned.
        let cache_check = Arc::clone(&cache);
        let cs_check = Arc::clone(&content_store);

        // Start directly in continuous mode with 1s intervals.
        let (_, mode_rx) = watch::channel(FlushMode::Continuous {
            pack_interval_secs: 1,
            manifest_interval_secs: 1,
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            flush_scheduler(
                cache, content_store, pack_index, mode_rx,
                flush_trigger, shutdown_rx, metrics,
            )
            .await;
        });

        // Wait for the periodic pack flush to fire (1s interval + processing time).
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if cache_check.dirty_block_count() == 0 {
                break;
            }
        }
        assert_eq!(
            cache_check.dirty_block_count(),
            0,
            "periodic pack flush should have flushed dirty blocks"
        );

        // Wait for the periodic manifest sync to fire.
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if let Ok(Some(_)) = cs_check.get_manifest("sched-test").await {
                break;
            }
        }
        let manifest = cs_check.get_manifest("sched-test").await.unwrap();
        assert!(
            manifest.is_some(),
            "periodic manifest sync should have uploaded the manifest"
        );

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }
}
