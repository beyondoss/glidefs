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

use crate::block::handler::BlockHandler;
use device::KernelFeatures;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Default number of I/O queues per device.
const DEFAULT_NR_QUEUES: u16 = 1;

/// Persisted device mapping filename.
const DEVICE_MAP_FILE: &str = "ublk_devices.json";

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
}

impl Default for UblkServer {
    fn default() -> Self {
        Self::new()
    }
}

impl UblkServer {
    /// Create a new ublk server.
    ///
    /// Probes the running kernel for supported ublk features (recovery,
    /// zero-copy). Falls back to conservative defaults on older kernels.
    pub fn new() -> Self {
        let features = device::detect_features();
        Self {
            nr_queues: DEFAULT_NR_QUEUES,
            features,
            devices: HashMap::new(),
            cache_dir: None,
        }
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

        let name = export_name.to_string();
        let device =
            device::UblkDevice::register(handler, self.nr_queues, name.clone(), &self.features, preferred_id)
                .await?;
        let path = device.dev_path().to_path_buf();
        self.devices.insert(name, device);
        self.persist_devices();
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

    /// Remove a ublk device for an export.
    ///
    /// Idempotent: returns `Ok(())` if no device is registered for this export.
    pub async fn remove_device(&mut self, export_name: &str) -> anyhow::Result<()> {
        if let Some(device) = self.devices.remove(export_name) {
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
            self.persist_devices();
        } else {
            tracing::debug!(export = %export_name, "no ublk device registered, nothing to remove");
        }
        Ok(())
    }

    /// Scan for QUIESCED ublk devices left behind by a previous crash and
    /// recover them.
    ///
    /// `get_handler` resolves an export name to its `BlockHandler`. Devices
    /// whose export has no matching handler are logged and skipped (they may
    /// belong to another glidefs instance).
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

        // Collect candidate device IDs.
        let candidates = std::sync::Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
        let c = std::sync::Arc::clone(&candidates);
        ublk_core::ctrl::UblkCtrl::for_each_dev_id(move |dev_id| {
            c.lock().unwrap().push(dev_id as i32);
        });
        let candidates = std::sync::Arc::try_unwrap(candidates)
            .unwrap()
            .into_inner()
            .unwrap();

        if candidates.is_empty() {
            return 0;
        }

        tracing::info!(count = candidates.len(), "scanning ublk devices for QUIESCED state");

        let mut recovered = 0usize;
        for dev_id in candidates {
            let ctrl = match ublk_core::ctrl::UblkCtrl::new_simple(dev_id) {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(dev_id, error = %e, "cannot open ublk ctrl, skipping");
                    continue;
                }
            };

            // Only recover devices we own.
            let name = ctrl.get_name();
            if name != "glidefs" {
                continue;
            }

            // Only recover QUIESCED devices.
            let state = ctrl.dev_info().state as u32;
            if state != ublk_core::sys::UBLK_S_DEV_QUIESCED {
                continue;
            }

            // Extract export name from target JSON.
            let export_name = match ctrl
                .get_target_data_from_json()
                .and_then(|v| v.get("export_name")?.as_str().map(String::from))
            {
                Some(n) => n,
                None => {
                    tracing::warn!(dev_id, "QUIESCED glidefs device has no export_name in target JSON, skipping");
                    continue;
                }
            };

            // Skip if we already have this export registered.
            if self.devices.contains_key(&export_name) {
                tracing::debug!(dev_id, export = %export_name, "already registered, skipping recovery");
                continue;
            }

            // Look up the handler for this export.
            let handler = match get_handler(&export_name) {
                Some(h) => h,
                None => {
                    tracing::warn!(dev_id, export = %export_name, "no handler for QUIESCED device, skipping");
                    continue;
                }
            };

            // Use the QUIESCED device's original queue count, not our current
            // config — the kernel expects recovery to match the original layout.
            let nr_queues = ctrl.dev_info().nr_hw_queues;

            tracing::info!(dev_id, export = %export_name, nr_queues, "recovering QUIESCED ublk device");
            match device::UblkDevice::recover(
                dev_id,
                handler,
                nr_queues,
                export_name.clone(),
                &self.features,
            )
            .await
            {
                Ok(dev) => {
                    tracing::info!(dev_id, export = %export_name, path = %dev.dev_path().display(), "ublk device recovered");
                    self.devices.insert(export_name, dev);
                    recovered += 1;
                }
                Err(e) => {
                    tracing::error!(dev_id, export = %export_name, error = %e, "failed to recover ublk device");
                }
            }
        }

        self.persist_devices();
        recovered
    }

    /// Shutdown all ublk devices concurrently.
    ///
    /// Issues `kill_dev` + thread join for every device in parallel, then
    /// returns an aggregated error describing which devices could not be
    /// unregistered.
    pub async fn shutdown(self) -> anyhow::Result<()> {
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
