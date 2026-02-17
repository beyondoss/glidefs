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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FlushMode {
    /// Flush only when explicitly triggered (budget exceeded, API call).
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

impl Default for FlushMode {
    fn default() -> Self {
        FlushMode::DemandDriven
    }
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
async fn run_demand_driven(
    cache: &Arc<WriteCache<Active>>,
    content_store: &Arc<ContentStore>,
    pack_index: &Arc<HostPackIndex>,
    flush_trigger: &Arc<Notify>,
    mode_rx: &mut watch::Receiver<FlushMode>,
    shutdown: &mut watch::Receiver<bool>,
    metrics: &ExportMetrics,
) {
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
                do_full_flush(cache, content_store, pack_index, metrics).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Continuous mode
// ---------------------------------------------------------------------------

/// Periodic pack flush + manifest sync with optional explicit triggers.
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
    let mut pack_backoff = Duration::from_secs(1);
    let mut manifest_backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

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
                do_full_flush(cache, content_store, pack_index, metrics).await;
                // A full flush includes a manifest sync, so reset cutpoint.
                last_seq_cutpoint = 0;
                pack_backoff = Duration::from_secs(1);
                manifest_backoff = Duration::from_secs(1);
            }

            // Periodic pack flush.
            _ = pack_ticker.tick() => {
                if cache.dirty_block_count() > 0 {
                    let start = Instant::now();
                    match cache.flush_packs(content_store, pack_index).await {
                        Ok((stats, seq_cutpoint)) => {
                            metrics.record_s3_put_latency(start.elapsed());
                            if stats.packs_uploaded > 0 {
                                info!(
                                    packs = stats.packs_uploaded,
                                    blocks = stats.blocks_flushed,
                                    bytes = stats.bytes_uploaded,
                                    seq_cutpoint,
                                    "periodic pack flush complete"
                                );
                            }
                            last_seq_cutpoint = seq_cutpoint;
                            pack_backoff = Duration::from_secs(1);
                        }
                        Err(e) => {
                            metrics.record_flush_error();
                            warn!(error = %e, backoff_secs = pack_backoff.as_secs(), "periodic pack flush failed, backing off");
                            tokio::time::sleep(pack_backoff).await;
                            pack_backoff = (pack_backoff * 2).min(MAX_BACKOFF);
                        }
                    }
                }
            }

            // Periodic manifest sync.
            _ = manifest_ticker.tick() => {
                if last_seq_cutpoint > 0 {
                    match cache.sync_manifest(content_store, pack_index, last_seq_cutpoint).await {
                        Ok(()) => {
                            info!(seq_cutpoint = last_seq_cutpoint, "manifest sync complete");
                            last_seq_cutpoint = 0;
                            manifest_backoff = Duration::from_secs(1);
                        }
                        Err(e) => {
                            metrics.record_flush_error();
                            warn!(error = %e, backoff_secs = manifest_backoff.as_secs(), "manifest sync failed, backing off");
                            tokio::time::sleep(manifest_backoff).await;
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
async fn do_full_flush(
    cache: &Arc<WriteCache<Active>>,
    content_store: &Arc<ContentStore>,
    pack_index: &Arc<HostPackIndex>,
    metrics: &ExportMetrics,
) {
    let start = Instant::now();
    match cache.flush_to_s3(content_store, pack_index).await {
        Ok(stats) => {
            metrics.record_s3_put_latency(start.elapsed());
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
            error!(error = %e, "full flush failed");
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
}
