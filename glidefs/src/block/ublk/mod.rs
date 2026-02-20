//! ublk transport layer — io_uring-based userspace block device (Linux 6.0+).
//!
//! Uses the `libublk` crate to register block devices in the kernel that route
//! I/O through io_uring to our [`BlockHandler`]. One ublk device per export.
//!
//! # Architecture
//!
//! - **UblkServer** manages per-export ublk devices via `ExportRouter`
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

mod device;

use crate::block::router::ExportRouter;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Default number of I/O queues per device.
const DEFAULT_NR_QUEUES: u16 = 1;

/// Manages ublk devices for exports.
///
/// Each export gets its own `/dev/ublkbN` device. The router provides
/// `BlockHandler` instances, and each device runs per-queue I/O threads
/// that dispatch to the handler.
pub struct UblkServer {
    router: Arc<ExportRouter>,
    nr_queues: u16,
    devices: HashMap<String, device::UblkDevice>,
}

impl UblkServer {
    /// Create a new ublk server backed by the given router.
    pub fn new(router: Arc<ExportRouter>) -> Self {
        Self {
            router,
            nr_queues: DEFAULT_NR_QUEUES,
            devices: HashMap::new(),
        }
    }

    /// Set the number of I/O queues per device (default: 1).
    pub fn with_nr_queues(mut self, nr_queues: u16) -> Self {
        self.nr_queues = nr_queues;
        self
    }

    /// Register a ublk device for an export. Returns the `/dev/ublkbN` path.
    ///
    /// Returns an error if a device is already registered for this export.
    /// Call `remove_device` first to replace an existing device.
    pub async fn add_device(&mut self, export_name: &str) -> anyhow::Result<PathBuf> {
        if self.devices.contains_key(export_name) {
            anyhow::bail!(
                "ublk device for export '{}' already registered",
                export_name
            );
        }

        let handler = self
            .router
            .get_handler(export_name)
            .await
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found", export_name))?;

        let device = device::UblkDevice::register(handler, self.nr_queues).await?;
        let path = device.dev_path().to_path_buf();
        self.devices.insert(export_name.to_string(), device);
        Ok(path)
    }

    /// Remove a ublk device for an export.
    ///
    /// Idempotent: returns `Ok(())` if no device is registered for this export.
    pub async fn remove_device(&mut self, export_name: &str) -> anyhow::Result<()> {
        if let Some(device) = self.devices.remove(export_name) {
            tracing::info!(export = %export_name, "removing ublk device");
            device.unregister().await?;
        } else {
            tracing::debug!(export = %export_name, "no ublk device registered, nothing to remove");
        }
        Ok(())
    }

    /// Shutdown all ublk devices.
    ///
    /// Attempts every device even if some fail, then returns an aggregated
    /// error describing which devices could not be unregistered.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let mut failed: Vec<String> = Vec::new();

        for (name, device) in self.devices {
            tracing::info!(export = %name, "shutting down ublk device");
            if let Err(e) = device.unregister().await {
                tracing::error!(export = %name, error = %e, "failed to unregister ublk device");
                failed.push(format!("{name}: {e}"));
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
