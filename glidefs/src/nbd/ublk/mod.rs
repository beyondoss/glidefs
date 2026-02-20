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

use crate::nbd::handler::BlockHandler;
use crate::nbd::router::ExportRouter;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Manages ublk devices for exports.
///
/// Each export gets its own `/dev/ublkbN` device. The router provides
/// `BlockHandler` instances, and each device runs per-queue I/O threads
/// that dispatch to the handler.
pub struct UblkServer {
    router: Arc<ExportRouter>,
    devices: HashMap<String, device::UblkDevice>,
}

impl UblkServer {
    /// Create a new ublk server backed by the given router.
    pub fn new(router: Arc<ExportRouter>) -> Self {
        Self {
            router,
            devices: HashMap::new(),
        }
    }

    /// Register a ublk device for an export. Returns the `/dev/ublkbN` path.
    pub async fn add_device(&mut self, export_name: &str) -> anyhow::Result<PathBuf> {
        let handler = self
            .router
            .get_handler(export_name)
            .await
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found", export_name))?;

        let device = device::UblkDevice::register(handler).await?;
        let path = device.dev_path().to_path_buf();
        self.devices.insert(export_name.to_string(), device);
        Ok(path)
    }

    /// Remove a ublk device for an export.
    pub async fn remove_device(&mut self, export_name: &str) -> anyhow::Result<()> {
        if let Some(device) = self.devices.remove(export_name) {
            device.unregister().await?;
        }
        Ok(())
    }

    /// Shutdown all ublk devices.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        for (name, device) in self.devices {
            if let Err(e) = device.unregister().await {
                tracing::error!(export = %name, error = %e, "failed to unregister ublk device");
            }
        }
        Ok(())
    }
}
