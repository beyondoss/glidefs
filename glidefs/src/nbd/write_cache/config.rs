use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Notify;

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
}
