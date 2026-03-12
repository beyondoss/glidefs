use std::path::PathBuf;

use super::CacheError;

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

    /// Path to the v2 WAL file.
    pub fn wal_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.wal", self.device_name))
    }

    /// Path to the flushing data file (exists only during active flush in bottomless mode).
    pub fn flushing_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.flushing", self.device_name))
    }

    /// Validate configuration. Guards against zero block_size which would
    /// cause division-by-zero in num_blocks().
    pub fn validate(&self) -> Result<(), CacheError> {
        if self.block_size == 0 {
            return Err(CacheError::InvalidMetadata);
        }
        if self.device_size == 0 {
            return Err(CacheError::InvalidMetadata);
        }
        Ok(())
    }
}
