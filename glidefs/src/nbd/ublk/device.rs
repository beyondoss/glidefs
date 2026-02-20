//! Single ublk device: registration, per-queue I/O loop, teardown.
//!
//! Each `UblkDevice` corresponds to one `/dev/ublkbN` block device backed
//! by a `BlockHandler`. The device runs per-queue I/O threads that receive
//! commands via io_uring and dispatch to the handler.

use crate::nbd::handler::BlockHandler;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A registered ublk block device.
pub struct UblkDevice {
    dev_id: u32,
    dev_path: PathBuf,
    _handler: Arc<BlockHandler>,
    // TODO: worker thread handles, control fd, etc.
}

impl UblkDevice {
    /// Register a new ublk device backed by the given handler.
    ///
    /// This will:
    /// 1. Open `/dev/ublk-control`
    /// 2. Register a new device (`UBLK_CMD_ADD_DEV`)
    /// 3. Set device parameters (size, block_size)
    /// 4. Spawn per-queue I/O worker threads
    /// 5. Start the device (`UBLK_CMD_START_DEV`)
    pub async fn register(handler: Arc<BlockHandler>) -> anyhow::Result<Self> {
        // TODO: implement with libublk
        //
        // Key design points:
        // - libublk uses smol, not tokio. Per-queue I/O threads are CPU-pinned.
        // - handler.write/flush/trim/write_zeroes are sync — call directly
        // - handler.read is async — bridge via tokio::runtime::Handle::block_on()
        // - Each command maps 1:1:
        //   UBLK_IO_OP_READ       → handler.read(offset, length)
        //   UBLK_IO_OP_WRITE      → handler.write(offset, data, fua)
        //   UBLK_IO_OP_FLUSH      → handler.flush()
        //   UBLK_IO_OP_DISCARD    → handler.trim(offset, length, fua)
        //   UBLK_IO_OP_WRITE_ZEROES → handler.write_zeroes(offset, length, fua)
        // - Errors: handler errors → CommandError::to_linux_errno()

        let dev_id = 0; // placeholder
        let dev_path = PathBuf::from(format!("/dev/ublkb{}", dev_id));

        Ok(Self {
            dev_id,
            dev_path,
            _handler: handler,
        })
    }

    /// The block device path (e.g., `/dev/ublkb0`).
    pub fn dev_path(&self) -> &Path {
        &self.dev_path
    }

    /// The ublk device ID.
    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }

    /// Stop and unregister this ublk device.
    pub async fn unregister(self) -> anyhow::Result<()> {
        // TODO: UBLK_CMD_STOP_DEV + UBLK_CMD_DEL_DEV
        tracing::info!(dev_id = self.dev_id, "unregistering ublk device");
        Ok(())
    }
}
