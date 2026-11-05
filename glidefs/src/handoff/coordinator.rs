//! `HandoffCoordinator` — the single struct that owns the handoff side
//! of every per-export operation.
//!
//! These methods used to hang off `ExportRouter`. They were moved here
//! to keep `router.rs` focused on the per-IO dispatch path it actually
//! exists for. The coordinator borrows the router via `Arc` and reaches
//! per-export state through a small set of `pub(crate)` accessors —
//! see `ExportRouter::exports_map`, `cache_dir`, and `ublk_server_mutex`.
//!
//! Construction happens in `cli/server.rs` next to the router build,
//! and the coordinator is threaded through `run_predecessor` /
//! `run_successor` and the `CutoverStrategy` contexts.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::info;

use crate::block::handler::BlockHandler;
use crate::block::router::{ExportRouter, RouterError};
use crate::block::state::Active;
use crate::block::write_cache::{HandoffPhase, WriteCache};
use crate::handoff::protocol::ExportSnapshot;

/// All graceful-handoff entry points the predecessor/successor state
/// machines and `CutoverStrategy` impls need.
///
/// Construction is cheap (just an `Arc` clone of the router). Cloning
/// is encouraged when the coordinator needs to be shared across tasks.
pub struct HandoffCoordinator {
    router: Arc<ExportRouter>,
}

impl HandoffCoordinator {
    /// Wrap an existing router. The router stays the source of truth
    /// for per-export state; the coordinator just adds handoff-specific
    /// orchestration on top.
    pub fn new(router: Arc<ExportRouter>) -> Self {
        Self { router }
    }

    /// Borrow the underlying router. Used where existing code paths
    /// still take `&Arc<ExportRouter>` (NBD/HTTP API, listener fd
    /// registry access, etc.) and we want to thread the coordinator
    /// without rewiring the world.
    pub fn router(&self) -> &Arc<ExportRouter> {
        &self.router
    }

    /// Sync handler lookup — needed by the `Fn` closure passed to
    /// `recover_devices_by_id`, which can't `.await`.
    pub fn get_handler_sync(&self, name: &str) -> Option<Arc<BlockHandler>> {
        self.router
            .exports_map()
            .get(name)
            .map(|e| Arc::clone(&e.value().handler))
    }

    /// True iff the kernel advertises `UBLK_F_PER_IO_DAEMON`. Used by
    /// the handoff strategy selector. Always false on non-ublk builds.
    pub fn is_per_io_daemon_supported(&self) -> bool {
        #[cfg(all(target_os = "linux", feature = "ublk"))]
        {
            match self.router.ublk_server_mutex().try_lock() {
                Ok(g) => g.kernel_features().per_io_daemon,
                Err(_) => false,
            }
        }
        #[cfg(not(all(target_os = "linux", feature = "ublk")))]
        {
            false
        }
    }

    /// Snapshot of exports for the predecessor to send in `HelloAck`.
    pub async fn snapshot(&self) -> Vec<ExportSnapshot> {
        let mut out: Vec<ExportSnapshot> = Vec::new();

        let names: Vec<String> = self
            .router
            .exports_map()
            .iter()
            .map(|e| e.key().clone())
            .collect();

        let dev_id_map: HashMap<String, i32> = {
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            {
                let pairs = self.router.ublk_server_mutex().lock().await.snapshot_dev_ids();
                pairs.into_iter().map(|(id, name)| (name, id)).collect()
            }
            #[cfg(not(all(target_os = "linux", feature = "ublk")))]
            {
                HashMap::new()
            }
        };

        for name in names {
            let Some(state) = self.router.exports_map().get(&name) else {
                continue;
            };
            let state = state.value();
            let last_wal_seq = state.cache.last_persisted_seq();
            out.push(ExportSnapshot {
                name: name.clone(),
                size_bytes: state.handler.device_size(),
                readonly: state.readonly,
                last_wal_seq,
                ublk_dev_id: dev_id_map.get(&name).copied(),
            });
        }

        out
    }

    /// Successor-side: make the warmed router hold exactly the exports the
    /// predecessor lists, no more and no fewer. Returns the names that could
    /// not be adopted (empty on success).
    ///
    /// The router was built from the node's device maps some seconds before
    /// HELLO_ACK, while the predecessor kept creating and removing exports.
    /// The predecessor fences the export set from HELLO, so `snapshot` is
    /// final and this is the only reconcile there is:
    ///
    /// - listed, not warmed (created in the window): warm it now, from the
    ///   `export.json` the create persisted before releasing the fence.
    /// - warmed, not listed (removed in the window): forget it, writing
    ///   nothing. Left in, the serve loop would register a fresh device for it.
    /// - listed with a different size (resized in the window): forget and
    ///   warm again at the new size.
    ///
    /// Everything here is local to this process, so an abort at any point
    /// leaves the predecessor serving and nothing durable changed.
    pub async fn adopt_snapshot(&self, snapshot: &[ExportSnapshot]) -> Vec<String> {
        use futures::stream::{self, StreamExt};
        use std::collections::HashMap;

        let listed: HashMap<&str, &ExportSnapshot> =
            snapshot.iter().map(|e| (e.name.as_str(), e)).collect();

        let warmed: Vec<(String, u64)> = self
            .router
            .exports_map()
            .iter()
            .map(|e| (e.key().clone(), e.value().handler.device_size()))
            .collect();
        let mut stale = Vec::new();
        for (name, size) in warmed {
            match listed.get(name.as_str()) {
                None => {
                    info!(export = %name, "handoff: export removed on predecessor during warming");
                    // No export.json means a purge: the local files go too.
                    // "Can't tell" keeps them.
                    let purged = matches!(self.router.export_defined(&name).await, Ok(false));
                    self.router.forget_export(&name, purged).await;
                }
                Some(snap) if snap.size_bytes != size => {
                    info!(export = %name, warmed = size, listed = snap.size_bytes,
                        "handoff: export resized on predecessor during warming");
                    self.router.forget_export(&name, false).await;
                    stale.push(name);
                }
                Some(_) => {}
            }
        }

        let to_warm: Vec<&ExportSnapshot> = snapshot
            .iter()
            .filter(|e| self.router.exports_map().get(&e.name).is_none())
            .collect();
        if !to_warm.is_empty() {
            info!(count = to_warm.len(), resized = stale.len(),
                "handoff: warming exports the predecessor gained during warming");
        }

        let router = &self.router;
        stream::iter(to_warm)
            .map(|snap| async move {
                let attach = async {
                    let config = router.resolve_export(&snap.name).await.ok()??;
                    router.create_export(config, snap.readonly, None, None).await.ok()
                };
                attach.await.is_none().then(|| snap.name.clone())
            })
            .buffer_unordered(16)
            .filter_map(|failed| async move { failed })
            .collect()
            .await
    }

    /// Set the per-export handoff phase on every WriteCache.
    /// Replaces the older `set_all_caches_freeze(bool)` setter.
    pub async fn set_all_caches_phase(&self, phase: HandoffPhase) {
        let caches: Vec<Arc<WriteCache<Active>>> = self
            .router
            .exports_map()
            .iter()
            .map(|e| Arc::clone(&e.value().cache))
            .collect();
        for c in caches {
            c.set_handoff_phase(phase).await;
        }
    }

    /// Quiesce every export for handoff: mark BlockHandlers as frozen,
    /// wait for any in-flight atomic flush to complete, then
    /// `cache.flush()` to fsync each WAL. See `ARCHITECTURE.md`.
    ///
    /// Atomic `flush_packs` (Upload→ManifestSync→Evict→Checkpoint→Cleanup)
    /// makes S3 self-consistent at every point, BUT the predecessor must
    /// not exit while a flush cycle is mid-flight — the successor opens
    /// its `data_file` handle during WARMING (before this freeze) and
    /// expects the predecessor's flushing-file/checkpoint state to be
    /// quiesced by the time PREDS_DEAD fires. Acquiring `flush_lock`
    /// blocks until the cycle finishes; the bound is the atomic flush
    /// latency itself (seconds, not the old 8s fence floor).
    pub async fn freeze_all(&self) -> Result<(), RouterError> {
        let states: Vec<(String, Arc<BlockHandler>, Arc<WriteCache<Active>>)> = self
            .router
            .exports_map()
            .iter()
            .map(|e| {
                let name = e.key().clone();
                let state = e.value();
                (name, Arc::clone(&state.handler), Arc::clone(&state.cache))
            })
            .collect();

        if states.is_empty() {
            return Ok(());
        }

        info!(count = states.len(), "handoff: freezing all handlers");
        for (_, handler, cache) in &states {
            handler.freeze();
            // Pause the per-export checkpoint truncate so the WAL
            // stays intact for the successor's tail-replay window.
            cache.set_handoff_phase(HandoffPhase::Freezing).await;
        }

        use futures::stream::{self, StreamExt};

        // Wait for any in-flight atomic flush to complete. Phase=Freezing
        // is set before this acquire so any *new* flush_packs call sees
        // is_active() and short-circuits — only one cycle (the one
        // already inside the lock when WARMING began) can be in flight.
        // Generous 30s timeout: a stuck network upload shouldn't hang
        // handoff forever, but atomic flush typically finishes in ~1s.
        stream::iter(states.iter().map(|(name, _, cache)| (name.clone(), Arc::clone(cache))))
            .for_each_concurrent(16, |(name, cache)| async move {
                if (tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    cache.wait_for_inflight_flush(),
                )
                .await)
                    .is_err()
                {
                    tracing::warn!(
                        export = %name,
                        "handoff: in-flight atomic flush did not complete within 30s; \
                         proceeding with cutover (successor will recover via flushing file + S3)"
                    );
                }
            })
            .await;

        let errs: Vec<_> = stream::iter(states.into_iter())
            .map(|(name, _handler, cache)| async move {
                tokio::task::spawn_blocking(move || cache.flush())
                    .await
                    .map_err(|e| (name.clone(), format!("join: {e}")))
                    .and_then(|r| r.map_err(|e| (name, format!("flush: {e}"))))
            })
            .buffer_unordered(16)
            .filter_map(|r| async move { r.err() })
            .collect()
            .await;

        if let Some((name, detail)) = errs.into_iter().next() {
            return Err(RouterError::ShutdownIncomplete {
                incomplete_count: 1,
                details: format!("freeze fsync failed for '{name}': {detail}"),
            });
        }

        Ok(())
    }

    /// Reverse [`Self::freeze_all`]. Called on the predecessor's
    /// revival path if the successor crashes between PREDS_DEAD and
    /// ALIVE.
    pub async fn unfreeze_all(&self) {
        let states: Vec<(Arc<BlockHandler>, Arc<WriteCache<Active>>)> = self
            .router
            .exports_map()
            .iter()
            .map(|e| {
                let s = e.value();
                (Arc::clone(&s.handler), Arc::clone(&s.cache))
            })
            .collect();
        for (h, c) in states {
            h.unfreeze();
            c.set_handoff_phase(HandoffPhase::Idle).await;
        }
        info!("handoff: handlers unfrozen (handoff aborted)");
    }

    /// Take the UblkServer out of the router and drop it. This is the
    /// kernel-level CRH cutover: dropping closes io_uring fds, which
    /// causes the kernel to transition every device to QUIESCED.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn take_ublk_server(&self) -> Result<(), RouterError> {
        let mut guard = self.router.ublk_server_mutex().lock().await;
        let old = std::mem::take(&mut *guard);
        drop(guard);
        if let Err(e) = old.shutdown().await {
            return Err(RouterError::ShutdownIncomplete {
                incomplete_count: 1,
                details: format!("ublk server shutdown failed during handoff cutover: {e}"),
            });
        }
        info!("handoff: ublk server dropped; kernel devices QUIESCED");
        Ok(())
    }

    /// Successor-side: recover the QUIESCED devices the predecessor
    /// left behind. Called from `CrhStrategy::successor_takeover`.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn recover_handoff_devices(
        &self,
        ids: &[(i32, String)],
    ) -> Result<usize, RouterError> {
        // Tail-replay WALs for every export we're about to recover.
        // Done before the ublk-level recovery so the BlockHandler's
        // view of state_map is current by the time the kernel reissues
        // bios at us.
        //
        // Then resolve any flushing-file the predecessor was uploading
        // when handoff started (`recover_pending_flush_file`). We
        // deferred this in `WriteCache::open` (passive mode) because
        // touching the active data file or the flushing file while the
        // predecessor was still uploading would race writes and
        // corrupt blocks.
        for (_, export_name) in ids {
            if let Some(state) = self.router.exports_map().get(export_name) {
                let cache = Arc::clone(&state.value().cache);
                let cache_for_recovery = Arc::clone(&cache);
                match tokio::task::spawn_blocking(move || cache.replay_wal_tail()).await {
                    Ok(Ok(n)) => {
                        if n > 0 {
                            tracing::info!(
                                export = %export_name,
                                replayed = n,
                                "handoff: tail-replayed WAL entries"
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        return Err(RouterError::ShutdownIncomplete {
                            incomplete_count: 1,
                            details: format!(
                                "WAL tail replay failed for '{export_name}': {e}"
                            ),
                        });
                    }
                    Err(e) => {
                        return Err(RouterError::ShutdownIncomplete {
                            incomplete_count: 1,
                            details: format!("spawn_blocking failed: {e}"),
                        });
                    }
                }

                match tokio::task::spawn_blocking(move || {
                    cache_for_recovery.recover_pending_flush_file()
                })
                .await
                {
                    Ok(Ok(n)) => {
                        if n > 0 {
                            tracing::info!(
                                export = %export_name,
                                acted_on = n,
                                "handoff: post-takeover flush-file recovery"
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        return Err(RouterError::ShutdownIncomplete {
                            incomplete_count: 1,
                            details: format!(
                                "post-takeover flush-file recovery failed for '{export_name}': {e}"
                            ),
                        });
                    }
                    Err(e) => {
                        return Err(RouterError::ShutdownIncomplete {
                            incomplete_count: 1,
                            details: format!("spawn_blocking failed: {e}"),
                        });
                    }
                }

                // **Reload the volume manifest from S3.** The successor
                // loaded the manifest at WARMING-time, which is BEFORE
                // the predecessor's freeze fence. The predecessor may
                // have completed a `sync_manifest` call between then
                // and PREDS_DEAD, registering new packs. Without a
                // reload, the successor's in-memory manifest doesn't
                // know about those packs — reads of blocks resolved
                // through the manifest return zero (visible as fio
                // "verify: bad magic header 0"), and the successor's
                // own next manifest sync hits an ETag PreconditionFailed.
                let cs = Arc::clone(&state.value().content_store);
                let vm = Arc::clone(&state.value().volume_manifest);
                let cache_for_etag = Arc::clone(&state.value().cache);
                match cs.get_manifest(export_name).await {
                    Ok(Some((data, etag))) => {
                        match crate::block::volume_manifest::VolumeManifest::deserialize(&data) {
                            Ok(new_vm) => {
                                *vm.write() = new_vm;
                                cache_for_etag.set_manifest_etag(etag);
                                tracing::info!(
                                    export = %export_name,
                                    "handoff: reloaded volume manifest from S3"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    export = %export_name,
                                    error = %e,
                                    "handoff: failed to deserialize reloaded manifest \
                                     — keeping pre-handoff in-memory copy (some pack \
                                     references may be missing until next successful sync)"
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        // No manifest in S3 — predecessor never flushed.
                        // Our pre-handoff in-memory manifest is empty too. OK.
                    }
                    Err(e) => {
                        tracing::warn!(
                            export = %export_name,
                            error = %e,
                            "handoff: failed to fetch reloaded manifest from S3 \
                             — keeping pre-handoff in-memory copy"
                        );
                    }
                }
            }
        }

        let mut server = self.router.ublk_server_mutex().lock().await;
        let exports = self.router.exports_map();
        let get_handler = |name: &str| -> Option<Arc<BlockHandler>> {
            exports.get(name).map(|e| Arc::clone(&e.value().handler))
        };
        let recovered = server.recover_devices_by_id(ids, get_handler).await;
        Ok(recovered)
    }

    /// Predecessor-side revival: the successor crashed after we already
    /// dropped our UblkServer. Spin up a fresh UblkServer and recover
    /// our own QUIESCED devices via the standard crash-recovery path.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn revive_after_failed_handoff(&self) -> Result<usize, RouterError> {
        info!("handoff: reviving after failed handoff");
        let mut server = self.router.ublk_server_mutex().lock().await;
        *server = crate::block::ublk::UblkServer::new()
            .with_cache_dir(self.router.cache_dir_path().to_path_buf());
        let exports = self.router.exports_map();
        let get_handler = |name: &str| -> Option<Arc<BlockHandler>> {
            exports.get(name).map(|e| Arc::clone(&e.value().handler))
        };
        let recovered = server.recover_quiesced_devices(get_handler).await;
        info!(recovered, "handoff: revival complete");
        Ok(recovered)
    }

    // --- Stubs for non-ublk builds ---
    #[cfg(not(all(target_os = "linux", feature = "ublk")))]
    pub async fn take_ublk_server(&self) -> Result<(), RouterError> {
        Ok(())
    }
    #[cfg(not(all(target_os = "linux", feature = "ublk")))]
    pub async fn recover_handoff_devices(
        &self,
        _ids: &[(i32, String)],
    ) -> Result<usize, RouterError> {
        Ok(0)
    }
    #[cfg(not(all(target_os = "linux", feature = "ublk")))]
    pub async fn revive_after_failed_handoff(&self) -> Result<usize, RouterError> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExportConfig;

    fn config(name: &str, size_gb: f64) -> ExportConfig {
        ExportConfig {
            name: name.to_string(),
            size_gb,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
        }
    }

    fn listed(name: &str, size_gb: f64) -> ExportSnapshot {
        ExportSnapshot {
            name: name.to_string(),
            size_bytes: (size_gb * 1_073_741_824.0) as u64,
            readonly: false,
            last_wal_seq: 0,
            ublk_dev_id: None,
        }
    }

    /// The successor warmed {kept, gone, grown@0.01}; the predecessor now lists
    /// {kept, grown@0.02, born}. Adopting must leave exactly that set, at those
    /// sizes, and report an export it cannot warm instead of skipping it.
    #[tokio::test]
    async fn adopt_snapshot_matches_the_listed_set() {
        let router = Arc::new(ExportRouter::new_for_test().await);
        for (n, gb) in [("kept", 0.01), ("gone", 0.01), ("grown", 0.01)] {
            router.create_export(config(n, gb), false, None, None).await.unwrap();
            router.save_export(&config(n, gb)).await.unwrap();
        }
        // The predecessor created `born` and grew `grown` after we warmed.
        router.save_export(&config("born", 0.01)).await.unwrap();
        router.save_export(&config("grown", 0.02)).await.unwrap();
        let coord = HandoffCoordinator::new(Arc::clone(&router));

        let failed = coord
            .adopt_snapshot(&[
                listed("kept", 0.01),
                listed("grown", 0.02),
                listed("born", 0.01),
                listed("undefined", 0.01), // no export.json anywhere
            ])
            .await;

        assert_eq!(failed, vec!["undefined".to_string()]);
        let mut names = router.list_export_names().await;
        names.sort();
        assert_eq!(names, ["born", "grown", "kept"]);
        let grown = router.get_handler("grown").await.unwrap();
        assert_eq!(grown.device_size(), listed("grown", 0.02).size_bytes);
    }
}
