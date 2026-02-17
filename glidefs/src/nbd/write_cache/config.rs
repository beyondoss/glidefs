use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Notify;

use super::CacheError;

/// The block size that ZERO_BLOCK_HASH and ZERO_BLOCK_BYTES are compiled for.
/// Changing this requires updating those statics.
pub const EXPECTED_BLOCK_SIZE: usize = 128 * 1024;

/// Configuration for the write cache.
#[derive(Clone, Debug)]
pub struct WriteCacheConfig {
    /// Path to the local cache directory
    pub cache_dir: PathBuf,

    /// Device name (used for cache file naming)
    pub device_name: String,

    /// Device size in bytes
    pub device_size: u64,

    /// Block size in bytes
    pub block_size: usize,

    /// Dirty byte budget (0 = no budget). When exceeded, the flush scheduler
    /// is notified to perform a flush cycle.
    pub dirty_budget_bytes: u64,

    /// Flush trigger notification. When dirty bytes exceed the budget,
    /// the write path calls `notify_one()` to wake the flush scheduler.
    pub flush_trigger: Option<Arc<Notify>>,

    /// Whether to fsync the WAL after each write batch (default: false).
    /// When true, calls sync() (fsync) instead of flush_buf() (OS buffer flush).
    /// Adds ~10ms latency per write but guarantees durability on SSDs without
    /// power-loss protection.
    pub wal_sync: bool,
}

impl WriteCacheConfig {
    /// Calculate the number of blocks for this device.
    pub fn num_blocks(&self) -> usize {
        self.device_size.div_ceil(self.block_size as u64) as usize
    }

    /// Path to the cache data file.
    pub fn data_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.cache", self.device_name))
    }

    /// Path to the cache metadata file.
    pub fn metadata_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.meta", self.device_name))
    }

    /// Path to the v2 block map persistence file.
    pub fn block_map_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.blockmap", self.device_name))
    }

    /// Path to the v2 WAL file.
    pub fn wal_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.wal", self.device_name))
    }

    /// Validate that block_size is compatible with the compiled-in ZERO_BLOCK_BYTES size.
    ///
    /// ZERO_BLOCK_BYTES is a static 128KB buffer used by resolve_chunk to serve
    /// zero reads. block_size must not exceed it or slice() will panic.
    /// ZERO_BLOCK_HASH is the hash of a 128KB zero block — zero-block dedup is
    /// only correct when block_size == EXPECTED_BLOCK_SIZE.
    pub fn validate(&self) -> Result<(), CacheError> {
        if self.block_size > EXPECTED_BLOCK_SIZE {
            return Err(CacheError::UnsupportedBlockSize(self.block_size, EXPECTED_BLOCK_SIZE));
        }
        Ok(())
    }
}
