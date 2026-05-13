//! ublk transport layer — io_uring-based userspace block device (Linux 6.0+).
//!
//! Uses the `ublk_core` crate to register block devices in the kernel that route
//! I/O through io_uring to our [`BlockHandler`]. One ublk device per export.
//!
//! # Architecture
//!
//! - **UblkServer** manages per-export ublk devices
//! - **UblkDevice** handles a single block device: registration, per-queue
//!   I/O threads, and graceful teardown
//! - I/O dispatch reuses the same `BlockHandler` interface as NBD
//!
//! # Performance
//!
//! Compared to NBD:
//! - No socket overhead (shared memory via io_uring)
//! - No protocol serialization (fixed mmap'd descriptor array)
//! - Native multi-queue (per-CPU io_uring instances)
//! - Batched commit+fetch eliminates round-trips

pub mod device;
mod ffi_layout;
pub mod numa;
mod worker_pool;

use worker_pool::WorkerPool;

use crate::block::handler::BlockHandler;
use device::KernelFeatures;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Default number of I/O queues per device.
const DEFAULT_NR_QUEUES: u16 = 1;

/// Persisted device mapping filename.
const DEVICE_MAP_FILE: &str = "ublk_devices.json";

/// Max simultaneous `UblkDevice::recover` calls during startup scan.
/// Each call holds one blocking thread for the duration of its kernel
/// control-plane work; tokio's default blocking pool is 512 so 16 is
/// comfortably below the cap while overlapping enough to amortise
/// per-device ioctl latency.
/// Concurrency cap for the recovery sweep. Each in-flight recovery
/// does one `UblkCtrl::new_simple` + `START_USER_RECOVERY` (kernel
/// ioctls on a spawn_blocking thread) plus a few `AddQueue` round-trips
/// to the worker pool. Each in-flight unit holds ~1 blocking thread
/// plus nr_queues mpsc messages — both cheap, so we lean on
/// parallelism to hit the 5s-for-1000-devices target. 64 = 64 ×
/// nr_queues=4 = 256 AddQueue in flight, well below the workers'
/// inbox capacity.
const RECOVERY_CONCURRENCY: usize = 64;

/// Max simultaneous `kill_dev`+`del_dev` ioctls during the orphan
/// sweep. Each runs in its own `spawn_blocking` thread.
const ORPHAN_SWEEP_CONCURRENCY: usize = 16;

/// Result of probing one candidate kernel ublk device during the
/// recovery scan. Built on a single blocking thread (CTRL_URING is
/// thread_local) and consumed by the async parallel `recover()` step.
struct ProbedDev {
    dev_id: i32,
    name: String,
    export_name: Option<String>,
    nr_queues: u16,
}

/// Manages ublk devices for exports.
///
/// Each export gets its own `/dev/ublkbN` device. The caller provides
/// `BlockHandler` instances, and each device runs per-queue I/O threads
/// that dispatch to the handler.
pub struct UblkServer {
    nr_queues: u16,
    features: KernelFeatures,
    devices: HashMap<String, device::UblkDevice>,
    /// Directory for persisting device ID mapping (enables stable device paths).
    cache_dir: Option<PathBuf>,
    /// Cached NUMA node of `cache_dir`'s backing block device, resolved
    /// once on first `add_device` / `recover`. `Some(None)` = resolved
    /// and the device reports `numa_node = -1` (single-node host,
    /// virtualized storage, ZFS pool across nodes); `Some(Some(n))` =
    /// keep queues on node `n`; `None` = not yet resolved.
    cache_dir_numa_node: std::sync::OnceLock<Option<usize>>,
    /// The worker pool that hosts every queue's `io_task` futures.
    /// Sized at construction (default = `min(num_cpus, 32)`); each
    /// worker owns one io_uring + executor and may host queues from
    /// many devices.
    pool: WorkerPool,
}

impl Default for UblkServer {
    /// Equivalent to `Self::new()`. Used by `std::mem::take` in the
    /// router's shutdown path; spawns a fresh worker pool that's then
    /// immediately dropped — wasteful but correct.
    fn default() -> Self {
        Self::new()
    }
}

impl UblkServer {
    /// Create a new ublk server.
    ///
    /// Probes the running kernel for supported ublk features and spawns
    /// a worker pool sized at `min(num_cpus, 32)` workers. Must be
    /// called inside a tokio runtime context — workers re-enter the
    /// runtime to run handler futures (S3 fetches, etc.).
    pub fn new() -> Self {
        let features = device::detect_features();
        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(32);
        let topology: Arc<dyn numa::NodeTopology> =
            Arc::new(numa::SysfsTopology::detect());
        let pool = WorkerPool::new(num_workers, topology)
            .expect("failed to spawn ublk worker pool");
        Self {
            nr_queues: DEFAULT_NR_QUEUES,
            features,
            devices: HashMap::new(),
            cache_dir: None,
            cache_dir_numa_node: std::sync::OnceLock::new(),
            pool,
        }
    }

    /// Resolve `cache_dir`'s NUMA node lazily. Cached for the server's
    /// lifetime — the underlying block device doesn't migrate. Returns
    /// `None` if `cache_dir` is unset, the path doesn't resolve, or
    /// the device reports `numa_node = -1` (no preference).
    fn preferred_node(&self) -> Option<usize> {
        *self.cache_dir_numa_node.get_or_init(|| {
            let dir = self.cache_dir.as_deref()?;
            let node = numa::numa_node_for_path(dir);
            tracing::info!(
                cache_dir = %dir.display(),
                preferred_node = ?node,
                "resolved cache_dir NUMA preference"
            );
            node
        })
    }

    /// Set the number of I/O queues per device (default: 1).
    pub fn with_nr_queues(mut self, nr_queues: u16) -> Self {
        self.nr_queues = nr_queues;
        self
    }

    /// Set the cache directory for persisting device ID mappings.
    /// Required for stable device paths across restarts.
    pub fn with_cache_dir(mut self, dir: PathBuf) -> Self {
        self.cache_dir = Some(dir);
        self
    }

    /// Load ALL persisted device IDs from a previous run.
    ///
    /// Returns `{export_name → dev_id}`. Stale entries are harmless —
    /// the kernel rejects duplicate IDs and we fall back to auto-assign.
    fn load_persisted_indices(&self) -> HashMap<String, i32> {
        let Some(ref cache_dir) = self.cache_dir else {
            return HashMap::new();
        };
        let path = cache_dir.join(DEVICE_MAP_FILE);
        let Ok(data) = std::fs::read_to_string(&path) else {
            return HashMap::new();
        };
        let Ok(map): Result<HashMap<String, i32>, _> = serde_json::from_str(&data) else {
            tracing::warn!("corrupt {DEVICE_MAP_FILE}, ignoring");
            return HashMap::new();
        };
        for (name, dev_id) in &map {
            tracing::debug!(export = %name, dev_id, "loaded persisted ublk device id");
        }
        map
    }

    /// Persist current device mapping to disk.
    fn persist_devices(&self) {
        let Some(ref cache_dir) = self.cache_dir else {
            return;
        };
        let map: HashMap<&str, i32> = self
            .devices
            .iter()
            .map(|(name, dev)| (name.as_str(), dev.dev_id()))
            .collect();
        let path = cache_dir.join(DEVICE_MAP_FILE);
        match serde_json::to_string(&map) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&path, json) {
                    tracing::warn!(path = %path.display(), error = %e, "failed to persist ublk device map");
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to serialize ublk device map"),
        }
    }

    /// Register a ublk device for an export. Returns the `/dev/ublkbN` path.
    ///
    /// Returns an error if a device is already registered for this export.
    /// Call `remove_device` first to replace an existing device.
    ///
    /// **M6.1 crash-recovery semantics**: if the persisted device-id map
    /// names a kernel device that's still around in `UBLK_S_DEV_QUIESCED`
    /// (the state the kernel parks devices in when their userspace
    /// daemon vanishes, courtesy of `UBLK_F_USER_RECOVERY`), this call
    /// recovers that device in place instead of trying to add a new
    /// one. Without this, `UBLK_CMD_ADD_DEV` would fail with `-EEXIST`
    /// (the requested dev_id is taken), and our libublk ctrl-cmd retry
    /// would then re-issue the request with the legacy opcode, which
    /// returns `-EOPNOTSUPP` on kernels built without
    /// `CONFIG_BLKDEV_UBLK_LEGACY_OPCODES` (default off on Ubuntu 24.04
    /// CI runners and any 6.8+ distro kernel).
    ///
    /// Production flow already calls [`Self::recover_quiesced_devices`]
    /// at startup before `add_device`, so this branch is a defensive
    /// fallback for paths that bypass the orchestrated recovery scan
    /// (tests, single-export reattach, etc.).
    pub async fn add_device(
        &mut self,
        export_name: &str,
        handler: Arc<BlockHandler>,
    ) -> anyhow::Result<PathBuf> {
        if self.devices.contains_key(export_name) {
            anyhow::bail!(
                "ublk device for export '{}' already registered",
                export_name
            );
        }

        let preferred_id = self.load_persisted_indices()
            .get(export_name).copied();
        let preferred_node = self.preferred_node();

        // Probe the kernel for a QUIESCED glidefs-owned device at our
        // preferred dev_id. Cheap (single ctrl cmd) and only runs when
        // we actually have a persisted id.
        let quiesced_nr_queues = match preferred_id {
            Some(dev_id) => probe_quiesced_glidefs(dev_id).await,
            None => None,
        };

        let name = export_name.to_string();
        let device = if let (Some(dev_id), Some(nr_queues)) =
            (preferred_id, quiesced_nr_queues)
        {
            tracing::info!(
                export = %export_name, dev_id, nr_queues,
                "preferred dev_id is QUIESCED glidefs device — recovering in place"
            );
            device::UblkDevice::recover(
                dev_id,
                handler,
                nr_queues,
                name.clone(),
                &self.features,
                preferred_node,
                &self.pool,
            )
            .await?
        } else {
            device::UblkDevice::register(
                handler,
                self.nr_queues,
                name.clone(),
                &self.features,
                preferred_id,
                preferred_node,
                &self.pool,
            )
            .await?
        };
        let path = device.dev_path().to_path_buf();
        self.devices.insert(name, device);
        self.persist_devices();

        // Surface worker headroom on every add so hot-spotting shows up
        // in logs before it becomes an AddQueue failure. Logs the most
        // and least loaded workers — the spread reveals hash imbalance
        // long before any individual worker reaches AtCapacity.
        let snap = self.pool.capacity_snapshot();
        if let (Some(min), Some(max)) = (
            snap.iter().min_by_key(|(_, u, _, _)| *u),
            snap.iter().max_by_key(|(_, u, _, _)| *u),
        ) {
            tracing::info!(
                export = %export_name,
                devices = self.devices.len(),
                workers = snap.len(),
                hottest_worker = max.0,
                hottest_used = max.1,
                hottest_hosted_queues = max.3,
                coldest_worker = min.0,
                coldest_used = min.1,
                worker_capacity = max.2,
                "ublk device added — worker headroom snapshot"
            );
        }

        Ok(path)
    }

    /// Get the device path for an export, if registered.
    pub fn get_device_path(&self, export_name: &str) -> Option<&Path> {
        self.devices.get(export_name).map(|d| d.dev_path())
    }

    /// Unregister a kernel device without updating the persisted map.
    ///
    /// Simulates a crash: the device dies but `ublk_devices.json` still has the
    /// old ID. Used in tests to prove device path stability across restarts.
    #[cfg(feature = "test-utils")]
    pub async fn crash_remove(&mut self, export_name: &str) -> anyhow::Result<()> {
        if let Some(device) = self.devices.remove(export_name) {
            tracing::info!(export = %export_name, "crash_remove: killing ublk device (map preserved)");
            device.unregister().await?;
        }
        // Intentionally NOT calling persist_devices() — the map still has the old ID.
        Ok(())
    }

    /// Remove a ublk device for an export — *legacy combined API*.
    ///
    /// **Production callers should use the two-phase
    /// [`Self::take_device`] + [`Self::persist_now`] instead.** This
    /// method holds the caller-side mutex (the router's
    /// `tokio::Mutex<UblkServer>`) across the entire `kill_dev` ioctl,
    /// which serializes every concurrent DELETE behind one device's
    /// `STOP_DEV` drain. At fleet scale that turns a 1000-export
    /// teardown into a single-threaded multi-hour operation.
    ///
    /// The two-phase variant lets the router release the mutex between
    /// "remove from map" (microseconds) and "kill_dev" (seconds), so
    /// deletes parallelize across whatever concurrency the API
    /// receives. We keep this method only for `crash_remove`-style
    /// internal callers that already hold a tight scope on the lock.
    ///
    /// Idempotent: returns `Ok(())` if no device is registered for this export.
    pub async fn remove_device(&mut self, export_name: &str) -> anyhow::Result<()> {
        let Some(device) = self.devices.remove(export_name) else {
            tracing::debug!(export = %export_name, "no ublk device registered, nothing to remove");
            return Ok(());
        };
        // Persist while we still hold the lock. The slow `unregister`
        // call follows but doesn't need lock-protected state.
        self.persist_devices();
        unregister_device(export_name, device).await
    }

    /// Phase 1 of two-phase removal: take ownership of the named
    /// device's record without dropping it. Cheap — does a single
    /// HashMap remove + a `persist_devices` (JSON write to local disk).
    /// The returned `UblkDevice` can then have `unregister()` called on
    /// it *outside* the `UblkServer` mutex, freeing concurrent calls to
    /// proceed.
    ///
    /// Returns `None` if no device is registered for this export
    /// (idempotent removal).
    pub fn take_device(&mut self, export_name: &str) -> Option<device::UblkDevice> {
        let device = self.devices.remove(export_name)?;
        // Persist the now-shrunk device map while we still own the lock
        // so a concurrent restart sees this device as already-gone.
        self.persist_devices();
        Some(device)
    }
}

/// Probe a kernel ublk device by id. Returns `Some(nr_hw_queues)` iff
/// the device exists, is owned by us (name `"glidefs"` or `"none"` — the
/// latter being a mid-startup orphan whose json wasn't persisted before
/// the previous daemon died), and is currently in `UBLK_S_DEV_QUIESCED`
/// state. Returns `None` for anything else (missing, LIVE, STOPPED, or
/// owned by a different ublk consumer).
///
/// Used by `add_device` to detect "the dev_id we want is parked from a
/// previous run" and switch to the recover path instead of letting
/// `UBLK_CMD_ADD_DEV` fail with `-EEXIST`.
async fn probe_quiesced_glidefs(dev_id: i32) -> Option<u16> {
    tokio::task::spawn_blocking(move || -> Option<u16> {
        let ctrl = ublk_core::ctrl::UblkCtrl::new_simple(dev_id).ok()?;
        let name = ctrl.get_name();
        if name != "glidefs" && name != "none" {
            return None;
        }
        if (ctrl.dev_info().state as u32)
            != ublk_core::sys::UBLK_S_DEV_QUIESCED
        {
            return None;
        }
        Some(ctrl.dev_info().nr_hw_queues)
    })
    .await
    .ok()
    .flatten()
}

/// Phase 2 of two-phase removal. Drives the kernel-side teardown
/// (`STOP_DEV` + `DEL_DEV` + cdev-fd close) on an already-owned
/// `UblkDevice`. Does **not** require any `UblkServer` mutex. Idempotent.
/// Lifted out of `impl UblkServer` so the router can call it without any
/// lock held.
pub async fn unregister_device(
    export_name: &str,
    device: device::UblkDevice,
) -> anyhow::Result<()> {
    tracing::info!(export = %export_name, "removing ublk device");
    let dev_id = device.dev_id();
    if let Err(e) = device.unregister().await {
        // Unregister failed (e.g., worker thread stuck). Force-kill the kernel
        // device so add_device can reuse the dev_id without hitting EBUSY.
        tracing::warn!(
            export = %export_name,
            dev_id,
            error = %e,
            "ublk unregister failed — force-killing kernel device"
        );
        if let Err(kill_err) = tokio::task::spawn_blocking(move || {
            let ctrl = ublk_core::ctrl::UblkCtrl::new_simple(dev_id)?;
            ctrl.kill_dev()?;
            // Give kernel a moment to release the device
            std::thread::sleep(std::time::Duration::from_millis(100));
            Ok::<_, anyhow::Error>(())
        })
        .await?
        {
            tracing::warn!(
                dev_id,
                error = %kill_err,
                "force kill also failed — device may be leaked"
            );
        }
    }
    Ok(())
}

impl UblkServer {

    /// Scan for QUIESCED ublk devices left behind by a previous crash and
    /// recover them. After the scan, any **unrecoverable** glidefs-owned
    /// device (name=`glidefs` or `none` — orphan from a mid-startup crash
    /// where JSON wasn't persisted, no matching export handler, etc.)
    /// is killed. This prevents kernel-state-damage loops where a
    /// crash-restarting daemon accumulates orphans across iterations.
    ///
    /// `get_handler` resolves an export name to its `BlockHandler`.
    /// Devices whose export has no matching handler are killed (not
    /// just skipped) — if the daemon's config doesn't include the
    /// export, the kernel device is by definition garbage from this
    /// daemon's POV.
    ///
    /// Returns the number of successfully recovered devices.
    pub async fn recover_quiesced_devices(
        &mut self,
        get_handler: impl Fn(&str) -> Option<Arc<BlockHandler>>,
    ) -> usize {
        if !self.features.recovery {
            tracing::debug!("kernel does not support UBLK_F_USER_RECOVERY, skipping scan");
            return 0;
        }

        // Scan + probe every candidate in one spawn_blocking. CTRL_URING
        // is thread_local, so all `UblkCtrl::new_simple` handles built
        // here (and their Drop) stay pinned to a single blocking thread.
        // The async-side parallel `recover()` calls below each spawn
        // their own blocking thread internally.
        let probed: Vec<ProbedDev> = tokio::task::spawn_blocking(|| {
            // `for_each_dev_id` demands `Fn + Clone + 'static`; route
            // collection through an Arc<Mutex<...>> rather than a
            // capture-by-mut.
            let candidates = std::sync::Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
            let c = std::sync::Arc::clone(&candidates);
            ublk_core::ctrl::UblkCtrl::for_each_dev_id(move |dev_id| {
                c.lock().unwrap().push(dev_id as i32);
            });
            let candidates: Vec<i32> = std::sync::Arc::try_unwrap(candidates)
                .unwrap()
                .into_inner()
                .unwrap();

            let mut probed = Vec::with_capacity(candidates.len());
            for dev_id in candidates {
                let ctrl = match ublk_core::ctrl::UblkCtrl::new_simple(dev_id) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::debug!(dev_id, error = %e, "cannot open ublk ctrl, skipping");
                        continue;
                    }
                };

                // Filter by ownership. `name == "none"` = mid-startup
                // orphan (JSON not yet persisted when daemon died); also
                // ours by elimination. Skip devices owned by other ublk
                // consumers to avoid stomping on their state.
                let name = ctrl.get_name();
                if name != "glidefs" && name != "none" {
                    tracing::debug!(dev_id, %name, "skipping non-glidefs ublk device");
                    continue;
                }

                // Only recover QUIESCED devices. LIVE devices are still
                // serving I/O (somehow); leave them alone. STOPPED
                // devices are mid-teardown.
                let state = ctrl.dev_info().state as u32;
                if state != ublk_core::sys::UBLK_S_DEV_QUIESCED {
                    tracing::debug!(dev_id, %name, state, "skipping non-QUIESCED device");
                    continue;
                }

                let export_name = ctrl
                    .get_target_data_from_json()
                    .and_then(|v| v.get("export_name")?.as_str().map(String::from));
                let nr_queues = ctrl.dev_info().nr_hw_queues;
                probed.push(ProbedDev { dev_id, name, export_name, nr_queues });
            }
            probed
        })
        .await
        .unwrap_or_default();

        if probed.is_empty() {
            return 0;
        }

        tracing::info!(count = probed.len(), "scanning ublk devices for QUIESCED state");

        // Classify probed devices into (recover, orphan). Both
        // categorisation and `get_handler` are cheap and have to run on
        // the calling task anyway (`get_handler` isn't required to be
        // Send/Sync).
        let mut to_recover: Vec<(i32, String, Arc<BlockHandler>, u16)> = Vec::new();
        let mut orphans_to_kill: Vec<i32> = Vec::new();
        for p in probed {
            let Some(export_name) = p.export_name else {
                tracing::warn!(
                    dev_id = p.dev_id, name = %p.name,
                    "QUIESCED orphan has no export_name JSON; queuing for kill"
                );
                orphans_to_kill.push(p.dev_id);
                continue;
            };

            if self.devices.contains_key(&export_name) {
                tracing::debug!(dev_id = p.dev_id, export = %export_name, "already registered, skipping");
                continue;
            }

            let Some(handler) = get_handler(&export_name) else {
                tracing::warn!(
                    dev_id = p.dev_id, export = %export_name,
                    "QUIESCED device for unknown export; queuing for kill"
                );
                orphans_to_kill.push(p.dev_id);
                continue;
            };

            to_recover.push((p.dev_id, export_name, handler, p.nr_queues));
        }

        // Run `recover()` concurrently. Each call wraps its kernel ops
        // in its own `spawn_blocking` (separate CTRL_URING per thread),
        // so concurrency here translates directly to overlapped control-
        // plane ioctls. Worker pool absorbs concurrent `AddQueue` since
        // dispatch hashes by `(export, qid)` across K workers.
        let preferred_node = self.preferred_node();
        let recovered_devices: Vec<Result<(String, device::UblkDevice), i32>> = {
            use futures::stream::{self, StreamExt};
            stream::iter(to_recover)
                .map(|(dev_id, export_name, handler, nr_queues)| {
                    let features = self.features.clone();
                    let pool = &self.pool;
                    async move {
                        tracing::info!(
                            dev_id, export = %export_name, nr_queues,
                            "recovering QUIESCED ublk device"
                        );
                        match device::UblkDevice::recover(
                            dev_id,
                            handler,
                            nr_queues,
                            export_name.clone(),
                            &features,
                            preferred_node,
                            pool,
                        )
                        .await
                        {
                            Ok(dev) => {
                                tracing::info!(
                                    dev_id, export = %export_name,
                                    path = %dev.dev_path().display(),
                                    "ublk device recovered"
                                );
                                Ok((export_name, dev))
                            }
                            Err(e) => {
                                tracing::error!(
                                    dev_id, export = %export_name, error = %e,
                                    "recover failed; queuing for kill"
                                );
                                Err(dev_id)
                            }
                        }
                    }
                })
                .buffer_unordered(RECOVERY_CONCURRENCY)
                .collect()
                .await
        };

        let mut recovered = 0usize;
        for outcome in recovered_devices {
            match outcome {
                Ok((name, dev)) => {
                    self.devices.insert(name, dev);
                    recovered += 1;
                }
                Err(dev_id) => orphans_to_kill.push(dev_id),
            }
        }

        // Detach the orphan sweep into a background task so daemon
        // startup isn't blocked on it.
        //
        // Each `del_dev` ioctl waits inside the kernel for the per-device
        // tag set to fully idle (`ublk_wait_tagset_rqs_idle` and friends
        // at `ublk_ctrl_del_dev+0x116`). When the host already has many
        // active devices contending for the block layer, that wait can
        // run for minutes per orphan — measured up to 10 min/device at
        // 1000-device density. Awaiting the sweep inline turns a
        // 740-orphan recovery into a multi-hour boot; the daemon's
        // `/health/ready` endpoint would stay down the entire time,
        // systemd's `TimeoutStartSec` would expire, the restart loop
        // would kick in, and the host would be effectively offline.
        //
        // Correctness argument for backgrounding: orphans by definition
        // have no matching export in the daemon's config. They serve no
        // live I/O. While they sit unreaped, they consume kernel state
        // (a `ublk_device` struct + cdev fd) but nothing serves data
        // through them — recovery already classified them as orphan
        // precisely because no handler exists. The async sweep cleans
        // them up over the next few minutes/hours without blocking the
        // legit recovered devices from serving I/O.
        if !orphans_to_kill.is_empty() {
            let count = orphans_to_kill.len();
            tracing::warn!(
                count,
                "spawning background orphan sweep — daemon ready before completion"
            );
            tokio::task::spawn(async move {
                use futures::stream::{self, StreamExt};
                let deleted = stream::iter(orphans_to_kill)
                    .map(|dev_id| async move {
                        tokio::task::spawn_blocking(move || {
                            let Ok(ctrl) = ublk_core::ctrl::UblkCtrl::new_simple(dev_id)
                            else {
                                return 0usize;
                            };
                            // STOP first (no-op if already STOPPED), then DEL.
                            let _ = ctrl.kill_dev();
                            match ctrl.del_dev() {
                                Ok(_) => 1,
                                Err(e) => {
                                    tracing::error!(
                                        dev_id, error = ?e,
                                        "del_dev on orphan failed"
                                    );
                                    0
                                }
                            }
                        })
                        .await
                        .unwrap_or(0)
                    })
                    .buffer_unordered(ORPHAN_SWEEP_CONCURRENCY)
                    .fold(0usize, |acc, n| async move { acc + n })
                    .await;
                tracing::info!(deleted, "background orphan sweep complete");
            });
        }

        self.persist_devices();
        recovered
    }

    /// Shutdown the ublk transport without removing kernel devices.
    ///
    /// **M6.1**: this no longer calls `kill_dev`. Calling `STOP_DEV` on
    /// a device with VMs writing to it deadlocks the kernel: the
    /// `io_wq` workers wait for userspace COMMITs that never arrive
    /// (because the userspace queue threads / worker pool are exiting),
    /// leaving D-state threads behind that can only be cleared by a
    /// reboot. We hit this once during M0/M1 deployment.
    ///
    /// Instead: the worker pool drops its hosted `Rc<UblkQueue>`s and
    /// closes its io_uring fds, the daemon drops the persisted device
    /// map (so a clean restart starts fresh), and the kernel handles
    /// the userspace-vanishes case via `UBLK_F_USER_RECOVERY` —
    /// devices transition to QUIESCED, bios queue up, and the next
    /// daemon's `recover_quiesced_devices()` reattaches them.
    ///
    /// This means a clean shutdown is **indistinguishable from a crash**
    /// from the kernel's point of view. That's the design intent of
    /// `UBLK_F_USER_RECOVERY`: the device persists across daemon
    /// lifetimes. Operators that want a device truly gone must call
    /// `remove_device` (which still does call `kill_dev`, with the
    /// caller-owns-quiescing-the-VMs contract documented there).
    ///
    /// Snapshot per-worker capacity and utilization. Cheap (relaxed
    /// atomic loads, no lock or message roundtrip to the worker threads).
    /// Returned tuples are `(worker_idx, used_slots, capacity_slots,
    /// hosted_queues)` — used by the `/metrics` endpoint and a debug
    /// log line on every `add_device` so we see drift toward hot-spotting
    /// before it becomes an AddQueue failure.
    pub fn worker_capacity_snapshot(&self) -> Vec<(usize, usize, usize, usize)> {
        self.pool.capacity_snapshot()
    }

    /// Returns `Ok(())` always — there's no per-device failure mode in
    /// this path, just resource drops.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let device_count = self.devices.len();

        // Drop the worker pool — every worker receives `Shutdown`,
        // drops its hosted queues' Rc clones (the io_task futures are
        // cancelled by executor drop), and exits. The kernel sees each
        // worker's io_uring fd close and transitions the corresponding
        // ublk device(s) to QUIESCED via UBLK_F_USER_RECOVERY.
        if let Err(e) = self.pool.shutdown().await {
            tracing::warn!(error = %e, "ublk worker pool shutdown reported an error");
        }

        // Drop the per-device records (they hold Arc<UblkDev>; once
        // every UblkQueue's Arc clone is gone via the pool drop, this
        // is the last reference and the kernel record is freed).
        // Explicit drop for clarity.
        drop(self.devices);

        // Persisted dev_id map: keep it. On next start the daemon's
        // `recover_quiesced_devices` reads kernel state directly via
        // `for_each_dev_id`, so the map is just a hint for stable
        // /dev/ublkbN paths after a true clean restart.
        if let Some(ref cache_dir) = self.cache_dir {
            // Touch nothing — the map remains accurate as long as the
            // kernel devices it points to are still QUIESCED.
            let _ = cache_dir;
        }

        tracing::info!(
            count = device_count,
            "ublk transport shut down (kernel devices left QUIESCED for recovery)"
        );
        Ok(())
    }

    /// **Legacy / unused**: the previous shutdown path that called
    /// `kill_dev` on every device. Kept here to document the deadlock
    /// hazard described above — this is what NOT to do on the daemon
    /// hot path.
    #[doc(hidden)]
    #[allow(dead_code)]
    async fn shutdown_with_kill_dev_legacy(self) -> anyhow::Result<()> {
        // Remove persisted mapping — devices are being shut down.
        if let Some(ref cache_dir) = self.cache_dir {
            let _ = std::fs::remove_file(cache_dir.join(DEVICE_MAP_FILE));
        }

        let mut set = tokio::task::JoinSet::new();

        for (name, device) in self.devices {
            set.spawn(async move {
                tracing::info!(export = %name, "shutting down ublk device");
                (name, device.unregister().await)
            });
        }

        let mut failed: Vec<String> = Vec::new();
        while let Some(result) = set.join_next().await {
            match result {
                Ok((name, Err(e))) => {
                    tracing::error!(export = %name, error = %e, "failed to unregister ublk device");
                    failed.push(format!("{name}: {e}"));
                }
                Err(e) => {
                    failed.push(format!("shutdown task failed: {e}"));
                }
                Ok((_, Ok(()))) => {}
            }
        }

        if !failed.is_empty() {
            anyhow::bail!(
                "ublk shutdown incomplete — {} device(s) failed: {}",
                failed.len(),
                failed.join(", ")
            );
        }

        Ok(())
    }
}
