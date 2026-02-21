//! Block device client for kernel NBD integration tests.
//!
//! Opens `/dev/nbdN` and performs I/O via standard POSIX file operations.
//! All blocking I/O is wrapped in `spawn_blocking` to avoid stalling the
//! tokio runtime.

use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub struct NbdKernelClient {
    file: File,
    pub export_size: u64,
    #[allow(dead_code)]
    dev_path: PathBuf,
}

impl NbdKernelClient {
    /// Open a kernel NBD block device for read-write I/O.
    pub fn open(dev_path: &Path, export_size: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dev_path)
            .with_context(|| format!("failed to open nbd device {}", dev_path.display()))?;
        Ok(Self {
            file,
            export_size,
            dev_path: dev_path.to_path_buf(),
        })
    }

    /// Open for read-only I/O (still allows write_raw for error testing).
    /// Readonly enforcement is in the block handler, not the kernel.
    pub fn open_readonly(dev_path: &Path, export_size: u64) -> Result<Self> {
        Self::open(dev_path, export_size)
    }

    /// Read `length` bytes from the device at `offset`.
    pub async fn read(&mut self, offset: u64, length: u32) -> Result<Vec<u8>> {
        let file = self.file.try_clone()?;
        let len = length as usize;
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; len];
            file.read_exact_at(&mut buf, offset)
                .context("nbd kernel read failed")?;
            Ok(buf)
        })
        .await?
    }

    /// Write `data` to the device at `offset`.
    pub async fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let file = self.file.try_clone()?;
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || {
            file.write_all_at(&data, offset)
                .context("nbd kernel write failed")?;
            Ok(())
        })
        .await?
    }

    /// Write and return raw error code (0 = success, errno on failure).
    pub async fn write_raw(&mut self, offset: u64, data: &[u8]) -> Result<u32> {
        let file = self.file.try_clone()?;
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || match file.write_all_at(&data, offset) {
            Ok(()) => Ok(0u32),
            Err(e) => Ok(e.raw_os_error().unwrap_or(libc::EIO) as u32),
        })
        .await?
    }

    /// Flush all pending writes to stable storage.
    pub async fn flush(&mut self) -> Result<()> {
        let file = self.file.try_clone()?;
        tokio::task::spawn_blocking(move || {
            file.sync_all().context("nbd kernel flush failed")?;
            Ok(())
        })
        .await?
    }

    /// Close the block device.
    pub async fn disconnect(self) -> Result<()> {
        // Drop closes the fd.
        Ok(())
    }
}
