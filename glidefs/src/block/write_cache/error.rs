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

    #[error("Volume manifest error: {0}")]
    VolumeManifest(#[from] crate::block::volume_manifest::VolumeManifestError),

    /// WAL is held exclusively by another process. During a graceful
    /// handoff, the predecessor must release its WAL before the successor
    /// opens — this error indicates the protocol failed and the daemons
    /// would otherwise both write to the same WAL. The successor must
    /// abort and let the predecessor revive.
    #[error("WAL file is locked by another process (pid in flock owner): {0}")]
    Locked(std::path::PathBuf),

    /// Single-attach fence tripped: a strictly newer generation has taken this
    /// volume, so this writer has been superseded and must not commit. Terminal
    /// — unlike a transient manifest race, retrying cannot succeed. The caller
    /// must stop the flush loop and tear the export down (the guest's writes
    /// since the last successful sync are dropped, which is correct: this node
    /// was partitioned and those writes were never durably committed).
    #[error("fenced: volume taken by generation {superseded_by} (this writer holds {held})")]
    Fenced { held: u64, superseded_by: u64 },
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

    /// True when the error means another writer owns this export and the flush
    /// scheduler must stop: either an S3 precondition failure (ETag mismatch) or
    /// a single-attach [`CacheError::Fenced`] (a strictly-newer generation took
    /// the volume).
    pub fn is_manifest_conflict(&self) -> bool {
        matches!(
            self,
            CacheError::ContentStore(ContentStoreError::PreconditionFailed(_))
                | CacheError::Fenced { .. }
        )
    }
}
