use thiserror::Error;

use crate::block::content_store::ContentStoreError;

/// Errors that can occur during cache operations.
#[derive(Error, Debug)]
pub enum CacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[allow(dead_code)]
    #[error("Cache not ready for I/O operations")]
    NotReady,

    #[allow(dead_code)]
    #[error("Cache is shutting down")]
    ShuttingDown,

    #[error("Invalid cache metadata")]
    InvalidMetadata,

    #[error("Offset {0} exceeds device size {1}")]
    OffsetOutOfBounds(u64, u64),

    #[error("Content store error: {0}")]
    ContentStore(#[from] ContentStoreError),

    #[error("Block hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("LZ4 decompression failed: {0}")]
    DecompressFailed(String),

    #[allow(dead_code)]
    #[error(
        "Unsupported block size {0}: must not exceed {1} (ZERO_BLOCK_BYTES is compiled for this size)"
    )]
    UnsupportedBlockSize(usize, usize),

    #[error("Block evicted during flush — retry with backfill")]
    BlockEvicted,
}

impl CacheError {
    /// Create an OffsetOutOfBounds error. Marked cold since bounds checks rarely fail.
    #[cold]
    #[inline(never)]
    pub fn offset_out_of_bounds(offset: u64, device_size: u64) -> Self {
        CacheError::OffsetOutOfBounds(offset, device_size)
    }

    /// Create an InvalidMetadata error. Marked cold since metadata is rarely corrupt.
    #[cold]
    #[inline(never)]
    pub fn invalid_metadata() -> Self {
        CacheError::InvalidMetadata
    }
}
