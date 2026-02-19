//! Write-behind cache for NBD devices.
//!
//! This module implements a local SSD cache that provides fast FLUSH semantics
//! while asynchronously syncing data to S3. The cache uses typestate to enforce
//! proper lifecycle management at compile time.
//!
//! # Lock-Free Design
//!
//! Block states and presence tracking use lock-free atomics to avoid contention
//! under high write concurrency. State transitions use compare-and-swap (CAS)
//! operations, and presence bits use atomic OR.
//!
//! The data file uses positional I/O (pread/pwrite) which is thread-safe per
//! POSIX semantics, eliminating the need for any locking on the hot path.

mod config;
mod error;
mod flush;
mod init;
pub(crate) mod inner;
mod read;
mod recovery;
mod write;

#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::sync::Arc;

use crate::nbd::state::Draining;

pub use config::WriteCacheConfig;
pub use error::CacheError;
use inner::CacheInner;

/// Statistics from a flush operation.
#[derive(Debug, Default)]
pub struct FlushStats {
    /// Total blocks included in the flush snapshot.
    pub blocks_flushed: usize,
    /// Blocks skipped (already in S3 or zero blocks).
    pub blocks_deduped: usize,
    /// Blocks left dirty because a concurrent write changed their sequence
    /// during the flush cycle (CAS failure on the sequence-number check).
    pub blocks_cas_failed: usize,
    /// Blocks skipped due to CRC32 mismatch (SSD corruption detected).
    pub blocks_corrupted: usize,
    /// Number of pack objects uploaded.
    pub packs_uploaded: usize,
    /// Total bytes uploaded to S3.
    pub bytes_uploaded: u64,
    /// Pack IDs created during this flush (for registry updates).
    pub new_pack_ids: Vec<uuid::Uuid>,
}

/// Result of a snapshot operation.
#[derive(Debug)]
pub struct SnapshotResult {
    /// S3 ETag of the uploaded manifest (if the backend provides one).
    pub manifest_etag: Option<String>,
    /// Sequence number at the snapshot cut point.
    pub sequence: u64,
    /// Flush statistics.
    pub stats: FlushStats,
}

/// Write-behind cache with typestate lifecycle management.
///
/// The cache progresses through states:
/// - `Initializing`: Loading local cache
/// - `Recovering`: Syncing dirty blocks from previous session
/// - `Active`: Serving I/O, only state where read/write/flush are allowed
/// - `Draining`: Syncing all blocks before shutdown
pub struct WriteCache<S> {
    pub(super) inner: Arc<CacheInner>,
    pub(super) _state: PhantomData<S>,
}

#[allow(dead_code)]
impl WriteCache<Draining> {
    /// Finish draining and drop the cache.
    pub fn finish(self) {
        tracing::info!("cache drained and closed");
    }
}

// Loom concurrency tests are in a separate crate: glidefs/loom-tests/
// This is necessary because loom disables tokio's networking which breaks
// dependencies like hyper-util. Run loom tests with:
//   cd loom-tests && cargo test --release
