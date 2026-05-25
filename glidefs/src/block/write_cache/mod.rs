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

#[cfg(any(test, feature = "test-utils"))]
pub mod compact;
#[cfg(not(any(test, feature = "test-utils")))]
pub(crate) mod compact;
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

use crate::block::state::Draining;

pub use config::WriteCacheConfig;
pub use error::CacheError;
#[cfg(all(target_os = "linux", feature = "ublk"))]
pub use read::{ChunkPlanEntry, ChunkSource, ReadPlan};
#[cfg(all(target_os = "linux", feature = "ublk"))]
pub use inner::SyncFile;
pub use inner::HandoffPhase;
use inner::CacheInner;

#[cfg(feature = "test-utils")]
pub use inner::{
    FlushSyncPoints, FlushStep, FlushGateEvent,
    PromoteSyncPoints, PromoteStep, PromoteGateEvent,
    ReadSyncPoints, ReadStep, ReadGateEvent,
};

/// Statistics from a flush operation.
#[derive(Debug, Default)]
pub struct FlushStats {
    /// Total blocks claimed from the dirty set (CAS DIRTY→SYNCING) for this
    /// flush cycle. Includes blocks that were later skipped (CRC mismatch,
    /// concurrent re-dirty). Zero means no dirty blocks existed.
    pub blocks_claimed: usize,
    /// Blocks skipped (already in S3 or zero blocks).
    pub blocks_deduped: usize,
    /// Blocks left dirty because a concurrent write re-dirtied them during
    /// the flush cycle. Detected either by CAS failure on SYNCING→NOT_PRESENT
    /// or by CRC mismatch when the block is no longer SYNCING.
    pub blocks_cas_failed: usize,
    /// Blocks skipped due to CRC32 mismatch. Includes both benign write races
    /// (a concurrent write landed between CRC pre-pass and rotation, producing
    /// a stale baseline CRC — these also increment `blocks_cas_failed`) and
    /// genuine SSD read instability (state still SYNCING, `blocks_cas_failed`
    /// NOT incremented). Occasional spikes under write-heavy workloads are
    /// expected; a persistent non-zero delta of
    /// `blocks_crc_mismatched - blocks_cas_failed` at low write rates warrants
    /// SSD health investigation.
    pub blocks_crc_mismatched: usize,
    /// CRC queue entries drained this flush cycle. Includes duplicates from
    /// overwrites to the same page between flushes.
    pub crc_entries_drained: usize,
    /// Unique blocks with CRC data after dedup (queue entries collapsed to
    /// one entry per page). Ratio of drained/unique = duplication factor.
    pub crc_unique_blocks: usize,
    /// Blocks skipped by cross-flush dedup: an existing pack for the same
    /// chunk already has an entry at the same chunk_offset with the same hash.
    pub blocks_cross_deduped: usize,
    /// Number of pack objects uploaded.
    pub packs_uploaded: usize,
    /// Number of pack uploads skipped (content-addressed pack already exists).
    pub packs_skipped: usize,
    /// Total bytes uploaded to S3.
    pub bytes_uploaded: u64,
}

/// Result of a snapshot operation.
#[derive(Debug)]
pub struct SnapshotResult {
    /// S3 ETag of the uploaded manifest (if the backend provides one).
    pub manifest_etag: Option<String>,
    /// Sequence number at the snapshot cut point.
    pub sequence: u64,
    /// Whether the versioned snapshot was persisted to S3.
    /// `false` means the manifest was saved but the versioned snapshot key wasn't.
    pub snapshot_persisted: bool,
    /// Flush statistics.
    pub stats: FlushStats,
    /// Serialized manifest bytes captured under the flush lock.
    /// Used for tag publishing to ensure point-in-time consistency.
    pub manifest_bytes: Vec<u8>,
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

// Loom concurrency tests: glidefs/loom-tests/
// Tests the REAL SparseStateMap under exhaustive interleaving exploration.
// The `loom` cargo feature swaps std atomics for loom atomics in block_map.rs.
// Run: cd loom-tests && cargo test --release
