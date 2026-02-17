use thiserror::Error;

use crate::nbd::block_map::Blake3Hash;
use crate::nbd::block_store::BlockStoreError;
use crate::nbd::content_store::ContentStoreError;

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

    #[error("Block store error: {0}")]
    BlockStore(#[from] BlockStoreError),

    #[error("Invalid cache metadata")]
    InvalidMetadata,

    #[error("Offset {0} exceeds device size {1}")]
    OffsetOutOfBounds(u64, u64),

    #[error("Content store error: {0}")]
    ContentStore(#[from] ContentStoreError),

    #[error("Block hash mismatch: expected {expected}")]
    HashMismatch { expected: String },

    #[error("Block not found in any tier: {hash:?}")]
    BlockNotFound { hash: Blake3Hash },

    #[error("Pack format error: {0}")]
    PackFormat(String),

    #[error("LZ4 decompression failed: {0}")]
    DecompressFailed(String),
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
