// Truncating casts here are block-index → `usize`/`u32` (the u32 block-number is
// a deliberate on-disk format invariant, shared with `page_crcs`), `u64` lengths
// → `usize`, and CRC `finalize() → u32`, all lossless on the 64-bit targets
// GlideFS runs on. Scoped to this one lint so `cast_possible_wrap` /
// `cast_sign_loss` stay active and catch genuinely new sign/wrap mistakes.
#![allow(clippy::cast_possible_truncation)]
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicU8, Ordering};
use tracing::{debug, info, warn};

use crate::block::block_map::{Blake3Hash, SequenceNumber, SparseBlockState, SparseStateMap};

use crate::block::wal::Wal;

use super::config::WriteCacheConfig;
use super::error::CacheError;

use bytes::Bytes;

/// Magic bytes for cache metadata file
pub(super) const METADATA_MAGIC: &[u8; 8] = b"GLIDECCH";
/// Version 5: sparse state map + trailing max_sequence u64
pub(super) const METADATA_VERSION: u32 = 5;


/// Sealed module for `SyncFile`. The `File` field is private to this module,
/// preventing any code outside from accessing seek-based methods. This makes
/// the `unsafe impl Sync` locally verifiable: only the methods below touch the
/// inner file, and all of them use positional I/O (pread/pwrite).
mod sync_file {
    use std::fs::{File, OpenOptions};
    use std::path::Path;
    use tracing::info;

    /// A file handle safe for concurrent positional I/O.
    ///
    /// Only exposes positional I/O methods (`read_exact_at`, `write_all_at`)
    /// which use `pread`/`pwrite` system calls — no shared file position.
    /// Note: pwrite is NOT atomic for multi-page writes. A 128KB pwrite
    /// touches 32 pages; a concurrent pread can see partial updates.
    /// This is acceptable: the block device protocol does not require
    /// atomicity for concurrent overlapping requests.
    /// The inner `File` is module-private so no code outside this module
    /// can call seek-based methods.
    #[derive(Debug)]
    pub struct SyncFile {
        file: File,
    }

    // SAFETY: SyncFile only exposes positional I/O methods (pread/pwrite via
    // FileExt::{read_exact_at, write_all_at}) which use independent offsets
    // per call — no shared file position that could race.
    // The `file` field is private to this module — no external code can access
    // seek-based methods (read, write, seek) that would introduce data races.
    unsafe impl Sync for SyncFile {}

    impl SyncFile {
        /// Open a file for concurrent positional I/O.
        pub fn open(path: &Path, create: bool, device_size: u64) -> std::io::Result<Self> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(create)
                .truncate(false)
                .open(path)?;

            let file_size = file.metadata()?.len();
            if file_size < device_size {
                file.set_len(device_size)?;
            }

            info!(path = %path.display(), "opened cache file");
            Ok(SyncFile { file })
        }

        /// Read exact bytes at a specific offset (pread).
        #[inline]
        pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(buf, offset)
        }

        /// Write all bytes at a specific offset (pwrite).
        #[inline]
        pub fn write_all_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()> {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(buf, offset)
        }

        /// Sync all data and metadata to disk.
        #[inline]
        pub fn sync_all(&self) -> std::io::Result<()> {
            self.file.sync_all()
        }

        /// Get the raw file descriptor (for fallocate, etc).
        #[cfg(target_os = "linux")]
        pub fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
            use std::os::unix::io::AsRawFd;
            self.file.as_raw_fd()
        }

        /// Same as `as_raw_fd` — distinct name to make the ublk ZC call
        /// site greppable when auditing rotation-gate discipline. Always
        /// call this through a `data_file` read-guard deref, never via a
        /// fresh `data_file.read()` while another guard is held (would
        /// deadlock against a queued writer under parking_lot's task-fair
        /// policy).
        #[cfg(target_os = "linux")]
        pub fn as_raw_fd_for_ublk_zc(&self) -> std::os::unix::io::RawFd {
            self.as_raw_fd()
        }
    }
}

pub use sync_file::SyncFile;

/// Check if a block is all zeros.
///
/// Uses SIMD when available:
/// - AVX2 on x86_64 (32 bytes/iter → 4,096 iterations for 128KB)
/// - NEON on aarch64 (2×16 bytes/iter → 4,096 iterations for 128KB)
/// - u64 fallback on other arches (8 bytes/iter → 16,384 iterations for 128KB)
#[inline]
pub(super) fn is_zero_block(data: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We've verified AVX2 is available
            return unsafe { is_zero_block_avx2(data) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // NEON is always available on aarch64 (baseline ISA).
        return is_zero_block_neon(data);
    }
    #[allow(unreachable_code)]
    is_zero_block_u64(data)
}

/// AVX2 implementation - checks 32 bytes at a time.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn is_zero_block_avx2(data: &[u8]) -> bool {
    use std::arch::x86_64::*;

    // SAFETY: This function is only called when AVX2 is available (checked by caller).
    // All pointer operations stay within the bounds of `data`.
    unsafe {
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());

        // Process 32-byte chunks with AVX2
        // _mm256_loadu_si256 handles unaligned loads
        while ptr.add(32) <= end {
            let chunk = _mm256_loadu_si256(ptr as *const __m256i);
            // testz returns 1 if all bits are zero: (chunk AND chunk) == 0
            if _mm256_testz_si256(chunk, chunk) == 0 {
                return false;
            }
            ptr = ptr.add(32);
        }

        // Handle remainder (0-31 bytes) with scalar code
        while ptr < end {
            if *ptr != 0 {
                return false;
            }
            ptr = ptr.add(1);
        }

        true
    }
}

/// NEON implementation for aarch64 — checks 32 bytes at a time (2×128-bit loads + OR).
/// NEON is baseline on aarch64, so no runtime detection needed.
#[cfg(target_arch = "aarch64")]
#[inline]
fn is_zero_block_neon(data: &[u8]) -> bool {
    use std::arch::aarch64::*;

    // SAFETY: NEON is always available on aarch64. All pointer operations
    // stay within the bounds of `data`.
    unsafe {
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());

        // Process 32 bytes at a time: load 2×16B, OR together, check.
        // Same throughput as AVX2 (32 bytes/branch) using 128-bit registers.
        while ptr.add(32) <= end {
            let a = vld1q_u8(ptr);
            let b = vld1q_u8(ptr.add(16));
            let combined = vorrq_u8(a, b);
            if vmaxvq_u8(combined) != 0 {
                return false;
            }
            ptr = ptr.add(32);
        }

        // Handle 16-byte remainder
        if ptr.add(16) <= end {
            let chunk = vld1q_u8(ptr);
            if vmaxvq_u8(chunk) != 0 {
                return false;
            }
            ptr = ptr.add(16);
        }

        // Handle trailing bytes (0-15)
        while ptr < end {
            if *ptr != 0 {
                return false;
            }
            ptr = ptr.add(1);
        }

        true
    }
}

/// Fallback implementation using 64-bit words.
#[inline]
fn is_zero_block_u64(data: &[u8]) -> bool {
    // Process as u64 for 8x fewer comparisons than byte-by-byte
    // SAFETY: we're just reading, alignment doesn't matter for correctness
    let (prefix, middle, suffix) = unsafe { data.align_to::<u64>() };

    // Check unaligned prefix bytes
    if prefix.iter().any(|&b| b != 0) {
        return false;
    }

    // Check aligned u64 words (bulk of the work)
    if middle.iter().any(|&w| w != 0) {
        return false;
    }

    // Check unaligned suffix bytes
    suffix.iter().all(|&b| b == 0)
}

/// Per-export handoff phase. Reflects which window of the graceful
/// daemon-handoff state machine the cache is currently inside.
///
/// Stored as an `AtomicU8` on `CacheInner::handoff_phase`. Readers
/// check `is_active()` (any non-Idle) to gate flush rotation and WAL
/// truncation; writers (the handoff coordinator) transition the
/// phase as the predecessor moves through SIGHUP → freeze → cutover.
///
/// The variants intentionally encode the protocol's logical phases
/// even though all current readers collapse on `is_active`. This
/// leaves room for phase-specific gating (e.g. PIOD distinguishing
/// per-tag handoff windows from CRH's QUIESCED window) without
/// adding new flags.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HandoffPhase {
    /// No handoff in flight. Default state.
    Idle = 0,
    /// Successor has opened the cache; predecessor is still serving.
    /// On the predecessor side: set as soon as SIGHUP fires (covers
    /// the WARMING + READY-wait + freeze + cutover window).
    Warming = 1,
    /// Predecessor's `freeze_all` is in progress.
    Freezing = 2,
    /// Post-CUTOVER, before the successor sends ALIVE.
    Cutover = 3,
}

impl HandoffPhase {
    /// Decode from the raw u8 stored in the atomic. Unknown values
    /// fall back to `Idle` (defensive — no caller should ever store
    /// out-of-range values via `set_handoff_phase`).
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Warming,
            2 => Self::Freezing,
            3 => Self::Cutover,
            _ => Self::Idle,
        }
    }

    /// True when any handoff phase is in flight. Both current
    /// readers (`flush_packs`, `checkpoint`) gate on this.
    pub fn is_active(self) -> bool {
        !matches!(self, Self::Idle)
    }
}

/// Internal state shared across all cache states.
///
/// Uses lock-free atomics for block states and presence to avoid contention
/// under high write concurrency. The data file uses positional I/O which is
/// inherently thread-safe, eliminating all locking on the hot path.
/// Sparse per-block "promote pwrite in progress" claim, used by
/// `promote_syncing_blocks` to serialize the pread+pwrite window across
/// concurrent promoters of the same block without changing the state
/// machine (state stays SYNCING through the claim so eviction can still
/// race in; see `pf02_eviction_during_promote_read`).
///
/// **Sparse on purpose.** The claim is held only across the pread+pwrite
/// window (~30–100 µs per block), so at any instant the in-flight set is
/// bounded by `num_queues × queue_depth` across the device — typically
/// <256 entries. Storing this as a fixed-size bitmap would cost 1 byte
/// per block per export (8 MiB per 1 TiB export at 128 KiB blocks); at
/// the 20k-export fleet scale that's hundreds of GB of allocator-resident
/// state for an occasionally-held flag. Sparse storage costs O(in-flight),
/// independent of device size.
///
/// Empty cost: `Mutex<HashSet>` ≈ 64 B per export.
/// Per-claim cost: one short mutex critical section (insert/remove). The
/// mutex contends only during the promote handshake itself, never on the
/// data plane.
pub(crate) struct PromoteClaimBitmap {
    in_flight: parking_lot::Mutex<std::collections::HashSet<usize>>,
    released: parking_lot::Condvar,
}

impl PromoteClaimBitmap {
    pub(super) fn new() -> Self {
        Self {
            in_flight: parking_lot::Mutex::new(std::collections::HashSet::new()),
            released: parking_lot::Condvar::new(),
        }
    }

    /// Insert `idx` into the in-flight set. `true` iff this caller's
    /// insertion was new (i.e., they own the claim).
    pub(super) fn try_claim(&self, idx: usize) -> bool {
        self.in_flight.lock().insert(idx)
    }

    /// Park until another task `release`s `idx` (or `deadline` passes).
    /// Returns `true` if the claim was actually released; `false` on
    /// timeout. Real parking via `Condvar`, NOT a busy-spin / sleep
    /// loop — calling this from a tokio task still blocks the executor
    /// thread, but only for the actual wait, with no CPU burn.
    pub(super) fn wait_for_release(&self, idx: usize, deadline: std::time::Instant) -> bool {
        let mut g = self.in_flight.lock();
        while g.contains(&idx) {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let timeout = deadline.duration_since(now);
            // Spurious wake-ups are fine — the while-loop re-checks
            // `contains` and re-arms if needed.
            let _ = self.released.wait_for(&mut g, timeout);
        }
        true
    }

    pub(super) fn release(&self, idx: usize) {
        self.in_flight.lock().remove(&idx);
        self.released.notify_all();
    }

    /// Whether any task currently holds the claim for `idx`.
    pub(super) fn is_claimed(&self, idx: usize) -> bool {
        self.in_flight.lock().contains(&idx)
    }
}

/// Default pack-window readahead size: 32 MiB of pack bytes per cold-miss GET.
/// This is the production baseline the boot-set work is measured against.
pub(crate) const DEFAULT_READAHEAD_WINDOW_BYTES: u32 = 32 * 1024 * 1024;

pub(crate) struct CacheInner {
    /// Configuration
    pub(super) config: WriteCacheConfig,

    /// Block compression level used by the flush path. `COMPRESSION_LZ4`
    /// selects legacy LZ4; any other value selects zstd at that level. Defaults
    /// to zstd-1 (or `GLIDEFS_COMPRESSION_LEVEL`); bless overrides to zstd-19 via
    /// `WriteCache::set_compression_level`. The read path always auto-detects,
    /// so legacy LZ4 packs stay readable regardless of this.
    /// Atomic so it can be set post-construction; set once before any flush.
    pub(super) compression_level: AtomicI32,

    /// Pack-window readahead size in bytes for the cold-miss fetch path
    /// (`fetch_with_window`). On a cold miss we pull one contiguous range GET of
    /// this many pack bytes and cache every block in it. Defaults to
    /// [`DEFAULT_READAHEAD_WINDOW_BYTES`] (the production value). Atomic so the
    /// boot-set replay harness can sweep it — `0` disables the window (pure
    /// demand fetch, the cold floor arm); larger values raise coverage at the
    /// cost of accuracy (the accuracy↔coverage tradeoff). Not on the hot path:
    /// read once per cold-miss batch.
    pub(super) readahead_window_bytes: AtomicU32,

    /// Local cache file (data).
    /// Uses positional I/O (pread/pwrite) which is thread-safe. RwLock is
    /// read-locked for all I/O (~2ns overhead); write-locked only during
    /// file rotation (once per flush cycle).
    /// Active data file behind an `Arc<RwLock<_>>` so the ublk ZC path can
    /// acquire an owned [`parking_lot::lock_api::ArcRwLockReadGuard`] that
    /// outlives the async io_uring boundary. The guard is held from
    /// "SQE submission" through "after_write metadata commit"; rotation
    /// takes `.write()` and blocks until every inflight ZC I/O drops its
    /// guard. Without this, a flush rotating between WRITE_FIXED and the
    /// state-machine commit would land the data in the now-flushing file
    /// while the state map says DIRTY-in-active — silent corruption.
    pub(super) data_file: Arc<parking_lot::RwLock<SyncFile>>,

    /// Flushing file: the previous active file being uploaded to S3.
    /// Only set during an active flush. Immutable once set
    /// (no writes, only reads by compute_flush_batch).
    /// Arc-wrapped so rayon workers can share the reference without holding the Mutex.
    pub(super) flushing_file: parking_lot::Mutex<Option<Arc<SyncFile>>>,

    /// True while a flush rotation is in progress (between rotate_data_file()
    /// and flushing file deletion). Used by the read path to avoid reading
    /// SYNCING blocks from the active file (their data lives in the flushing
    /// file during flush).
    pub(super) flushing_active: AtomicBool,

    /// Sparse block state map - LOCK-FREE
    /// Combines block state and presence into a single sparse page table.
    /// State encoding: 0=NotPresent, 1=Clean (transient), 2=Dirty, 3=Syncing
    pub(super) state_map: SparseStateMap,

    /// Side-band per-block "promote pwrite in progress" claim. Held only
    /// across `promote_syncing_blocks`'s `ff.read_exact_at` →
    /// `df.write_all_at` window, so two concurrent promoters can't both
    /// pwrite OLD bytes that race a sibling bio's `WRITE_FIXED` NEW bytes
    /// (blktests block/042 corruption flake). Orthogonal to the state
    /// map: `state == SYNCING` is preserved across the pread+pwrite, so
    /// the flush thread's `transition_syncing_to_not_present` eviction
    /// can still race through (see `pf02_eviction_during_promote_read`).
    pub(super) promote_claim: PromoteClaimBitmap,

    /// Number of blocks (for bounds checking)
    pub(super) num_blocks: usize,

    /// Dirty/syncing block counters. These are not pure telemetry: the flush
    /// scheduler reads `dirty_block_count` cross-thread to decide whether to
    /// run a flush cycle (see `flush_scheduler.rs`, `handler.rs`). Publishing
    /// RMWs use `Release` and gating loads use `Acquire` so a thread that
    /// observes the bumped count also observes the block-state transition that
    /// preceded it — the standard message-passing pairing.
    pub(super) dirty_block_count: AtomicU64,
    pub(super) syncing_block_count: AtomicU64,

    /// Monotonic sequence counter for WAL replay ordering and snapshot versioning.
    /// Lock-free AtomicU64.
    pub(super) sequence: SequenceNumber,

    /// Write-ahead log for crash recovery.
    /// Uses O_APPEND for lock-free concurrent appends. The internal RwLock
    /// only contends during checkpoint truncation (~every 5s).
    pub(super) wal: Wal,

    /// Export name (used in WAL entries).
    pub(super) export_name: String,

    /// Pre-computed zero-block hash for this export's block_size.
    /// Used by flush, write, and read paths to identify trimmed/unwritten chunks.
    pub(super) zero_block_hash: Blake3Hash,

    /// Pre-computed zero-block bytes for this export's block_size.
    /// Avoids a heap allocation on every sparse read.
    pub(super) zero_block_bytes: Bytes,

    /// Number of recovery issues encountered during cache open (WAL replay
    /// failure, block map load failure). Exposed via metrics for monitoring.
    pub(super) recovery_warnings: AtomicU64,

    /// Current handoff phase. Set non-Idle during a graceful handoff
    /// to suppress WAL truncation in
    /// [`crate::block::write_cache::WriteCache::checkpoint`] and to
    /// skip new flushes in
    /// [`crate::block::write_cache::WriteCache::flush_packs`]. Without
    /// these gates, the predecessor's checkpoint/flush can truncate
    /// or rotate between the successor's WARMING-time replay and
    /// PREDS_DEAD, dropping entries or breaking inode sharing.
    /// `save_block_states` still runs (metadata file stays current);
    /// only the truncate/rotate is skipped. The WAL grows briefly
    /// (handoff window is bounded), then the successor's WAL fully
    /// replaces it on predecessor exit.
    ///
    /// Stored as an `AtomicU8` representation of [`HandoffPhase`].
    /// Use `WriteCache::handoff_phase()` / `set_handoff_phase()`
    /// rather than reading this directly.
    pub(super) handoff_phase: AtomicU8,

    /// Per-export flush serialization lock.
    ///
    /// Serializes flush + manifest upload operations to prevent concurrent
    /// callers (drain, flush_scheduler, snapshot) from uploading stale
    /// manifests. Without this, two concurrent `flush_to_s3` calls can each
    /// serialize the in-memory VolumeManifest at different points, and
    /// last-writer-wins on S3 can overwrite a correct manifest with a stale one.
    pub(crate) flush_lock: tokio::sync::Mutex<()>,

    /// Last known S3 ETag of the manifest for this export.
    ///
    /// Used for conditional PUT (`If-Match`) to prevent overwriting a manifest
    /// that was modified concurrently (e.g., by another host after failover).
    /// Seeded on manifest load, updated after every successful `put_manifest`.
    /// `None` means first upload (unconditional PUT).
    pub(crate) manifest_etag: Mutex<Option<String>>,

    /// Single-attach fencing generation this writer was granted at attach (the
    /// orchestrator's monotonic per-instance placement generation; see
    /// [`crate::block::fence`]). Seeded at attach. Stamped into the manifest on
    /// every sync and compared against S3 on a precondition failure: if a strictly
    /// newer generation has taken the volume, this writer is FENCED. `0` =
    /// un-fenced (legacy / non-participating caller); the fence is a no-op.
    pub(crate) current_generation: Mutex<u64>,

    /// Single-attach fencing token, low half: the node-lease claim revision this
    /// writer was granted at attach. Composed with [`Self::current_generation`]
    /// into the `u128` fence token (see [`crate::block::fence::compose_token`]),
    /// stamped into the manifest on every sync, and compared against S3 on a
    /// precondition failure. Orders same-`node_id` incarnations (a node-death
    /// re-fork does NOT advance the orchestrator generation). `0` = un-fenced on
    /// this half.
    pub(crate) current_lease_revision: Mutex<u64>,

    /// Set once this writer is FENCED — a strictly-newer generation took the
    /// volume (detected at manifest sync). Latches forever: once fenced, the
    /// write path rejects further guest writes so the guest stops receiving
    /// ACKs for writes that can never durably commit, and the flush scheduler
    /// stops. Self-contained so the cache fences itself without reaching for the
    /// handler/router.
    pub(crate) fenced: AtomicBool,

    /// Per-page CRC32C checksums captured at pwrite time from the guest's write
    /// buffer. One entry per block, value is per-page CRC array. Upsert via
    /// `insert()` — allocation happens outside the shard lock, lock hold time
    /// is just a pointer swap.
    pub(super) page_crcs: dashmap::DashMap<u32, Box<[u32]>>,

    /// Number of 4KB pages per block (block_size / 4096).
    pub(super) pages_per_block: usize,

    /// Sequence number at the last file rotation. 0 when no rotation is active.
    /// Persisted to metadata so crash recovery can distinguish pre- vs
    /// post-rotation WAL entries: entries with sequence > rotation_seq were
    /// written after rotation, so their data in the active file is
    /// authoritative (even if all zeros).
    pub(super) rotation_seq: AtomicU64,

    /// Test-only: sync points for flush path interleaving tests.
    #[cfg(feature = "test-utils")]
    pub(crate) flush_sync: parking_lot::Mutex<Option<std::sync::Arc<FlushSyncPoints>>>,

    /// Test-only: sync points for promote path interleaving tests.
    #[cfg(feature = "test-utils")]
    pub(crate) promote_sync: parking_lot::Mutex<Option<std::sync::Arc<PromoteSyncPoints>>>,

    /// Test-only: sync points for read path interleaving tests.
    #[cfg(feature = "test-utils")]
    pub(crate) read_sync: parking_lot::Mutex<Option<std::sync::Arc<ReadSyncPoints>>>,
}

// ============================================================================
// Test-only: sync point types for deterministic interleaving tests
// ============================================================================

/// Flush pipeline sync points.
#[cfg(feature = "test-utils")]
pub struct FlushSyncPoints {
    pub tx: tokio::sync::mpsc::UnboundedSender<FlushGateEvent>,
    rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>,
}

#[cfg(feature = "test-utils")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushStep {
    AfterCrcPrepass,  // F1→F2
    AfterRotation,    // F5
    AfterCompute,     // F6→F7
    BeforeEvict,      // F7→F8
    AfterEvict,       // F8→F9
}

#[cfg(feature = "test-utils")]
#[derive(Debug)]
pub struct FlushGateEvent {
    pub step: FlushStep,
    pub block_count: usize,
}

#[cfg(feature = "test-utils")]
impl FlushSyncPoints {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<FlushGateEvent>, tokio::sync::mpsc::UnboundedSender<()>) {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (proceed_tx, proceed_rx) = tokio::sync::mpsc::unbounded_channel();
        (Self { tx: event_tx, rx: tokio::sync::Mutex::new(proceed_rx) }, event_rx, proceed_tx)
    }

    pub(crate) async fn gate(&self, step: FlushStep, block_count: usize) {
        let _ = self.tx.send(FlushGateEvent { step, block_count });
        let _ = self.rx.lock().await.recv().await;
    }
}

/// Promote path sync points.
#[cfg(feature = "test-utils")]
pub struct PromoteSyncPoints {
    pub tx: tokio::sync::mpsc::UnboundedSender<PromoteGateEvent>,
    rx: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

#[cfg(feature = "test-utils")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromoteStep {
    AfterCloneArc,  // P1
    AfterRead,      // P3→P4 (between pread and pwrite)
    BeforeCas,      // P4→P5 (between pwrite and CAS)
}

#[cfg(feature = "test-utils")]
#[derive(Debug)]
pub struct PromoteGateEvent {
    pub step: PromoteStep,
    pub block_idx: usize,
}

/// Read path sync points.
#[cfg(feature = "test-utils")]
pub struct ReadSyncPoints {
    pub tx: tokio::sync::mpsc::UnboundedSender<ReadGateEvent>,
    rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>,
}

#[cfg(feature = "test-utils")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStep {
    AfterStateCheck,   // R3: after locate_block state check
    AfterRecheck,      // R4: after sync_read_local_block re-check
    BeforePread,       // R2/R5: before pread from active or flushing file
}

#[cfg(feature = "test-utils")]
#[derive(Debug)]
pub struct ReadGateEvent {
    pub step: ReadStep,
    pub block_idx: usize,
    pub block_state: u8,
}

#[cfg(feature = "test-utils")]
impl ReadSyncPoints {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<ReadGateEvent>, tokio::sync::mpsc::UnboundedSender<()>) {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (proceed_tx, proceed_rx) = tokio::sync::mpsc::unbounded_channel();
        (Self { tx: event_tx, rx: tokio::sync::Mutex::new(proceed_rx) }, event_rx, proceed_tx)
    }

    pub(crate) async fn gate(&self, step: ReadStep, block_idx: usize, block_state: u8) {
        let _ = self.tx.send(ReadGateEvent { step, block_idx, block_state });
        let _ = self.rx.lock().await.recv().await;
    }
}

#[cfg(feature = "test-utils")]
impl PromoteSyncPoints {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<PromoteGateEvent>, std::sync::mpsc::SyncSender<()>) {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (proceed_tx, proceed_rx) = std::sync::mpsc::sync_channel(1);
        (Self { tx: event_tx, rx: std::sync::Mutex::new(proceed_rx) }, event_rx, proceed_tx)
    }

    pub(crate) fn gate_sync(&self, step: PromoteStep, block_idx: usize) {
        let _ = self.tx.send(PromoteGateEvent { step, block_idx });
        // Blocking recv for sync (non-async) promote path.
        let _ = self.rx.lock().unwrap().recv();
    }
}


impl CacheInner {
    /// Get the raw file descriptor of the data file (for io_uring registration).
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    #[inline]
    pub(crate) fn data_file_fd(&self) -> std::os::unix::io::RawFd {
        self.data_file.read().as_raw_fd()
    }

    /// Acquire an owned read-lock guard that gates `data_file` rotation.
    ///
    /// The returned guard is `Send` and `'static`. Hold it across the entire
    /// ublk ZC I/O lifetime (SQE submission → kernel data plane →
    /// `after_*` metadata commit). While any inflight guard is held,
    /// `rotate_data_file` blocks on `data_file.write()` and cannot swap
    /// the file. This serializes ZC I/O with rotation so the state-map
    /// transition in `commit_after_zc_write` always operates on the same
    /// file the kernel wrote to.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub(crate) fn zc_inflight_enter(
        &self,
    ) -> parking_lot::lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, SyncFile> {
        self.data_file.read_arc()
    }

    /// Non-blocking variant: returns `None` if `data_file.write()` is
    /// held or queued (task-fair `parking_lot` would block a `read_arc`
    /// here). Used by the ZC dispatch inline fast path.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub(crate) fn try_zc_inflight_enter(
        &self,
    ) -> Option<parking_lot::lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, SyncFile>> {
        self.data_file.try_read_arc()
    }

    /// Write data to the data file via the RwLock'd handle (always correct fd).
    ///
    /// Used by the ublk write path — pwrite via the RwLock ensures writes
    /// always target the current active file, even after file rotation.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    #[inline]
    pub(crate) fn pwrite_data_file(&self, data: &[u8], offset: u64) -> std::io::Result<()> {
        self.data_file.read().write_all_at(data, offset)
    }

    /// Record per-page CRCs from the guest's write buffer.
    ///
    /// For each 4KB page fully covered by the write, computes CRC32C and
    /// upserts into the DashMap. Allocation of the per-block CRC array
    /// happens outside the shard lock — lock hold time is a pointer swap.
    #[inline]
    pub(super) fn capture_page_crcs(&self, offset: u64, data: &[u8]) {
        const PAGE_SIZE: u64 = 4096;
        let block_size = self.config.block_size as u64;
        let end = offset + data.len() as u64;
        let start_block = offset / block_size;
        let end_block = (end - 1) / block_size;
        let ppb = self.pages_per_block;

        // Fast path: single page-aligned write within one block (common case).
        if start_block == end_block
            && offset.is_multiple_of(PAGE_SIZE)
            && {
                #[allow(clippy::cast_possible_truncation)]
                let page_size = PAGE_SIZE as usize; // usize >= u64 on 64-bit
                data.len() == page_size
            }
        {
            let page = ((offset % block_size) / PAGE_SIZE) as usize;
            let crc = crc_fast::crc32_iscsi(data);
            // Try update existing entry first (common after first write).
            #[allow(clippy::cast_possible_truncation)]
            if let Some(mut entry) = self.page_crcs.get_mut(&(start_block as u32)) { // block numbers fit in u32
                entry[page] = crc;
            } else {
                // Allocate outside any lock, then insert.
                let mut crcs = vec![0u32; ppb].into_boxed_slice();
                crcs[page] = crc;
                #[allow(clippy::cast_possible_truncation)]
                let block_key = start_block as u32; // block numbers fit in u32
                self.page_crcs.insert(block_key, crcs);
            }
            return;
        }

        // General path: multi-page or unaligned writes.
        for block in start_block..=end_block {
            let block_start = block * block_size;

            #[allow(clippy::cast_possible_truncation)]
            let block_key = block as u32; // block numbers fit in u32
            if let Some(mut entry) = self.page_crcs.get_mut(&block_key) {
                for page in 0..ppb {
                    let page_start = block_start + (page as u64) * PAGE_SIZE;
                    let page_end = page_start + PAGE_SIZE;
                    if offset <= page_start && end >= page_end {
                        #[allow(clippy::cast_possible_truncation)]
                        let slice_start = (page_start - offset) as usize; // usize >= u64 on 64-bit
                        #[allow(clippy::cast_possible_truncation)]
                        let page_size = PAGE_SIZE as usize; // usize >= u64 on 64-bit
                        entry[page] = crc_fast::crc32_iscsi(
                            &data[slice_start..slice_start + page_size],
                        );
                    } else if end > page_start && offset < page_end {
                        entry[page] = 0;
                    }
                }
            } else {
                let mut crcs = vec![0u32; ppb].into_boxed_slice();
                for page in 0..ppb {
                    let page_start = block_start + (page as u64) * PAGE_SIZE;
                    let page_end = page_start + PAGE_SIZE;
                    if offset <= page_start && end >= page_end {
                        #[allow(clippy::cast_possible_truncation)]
                        let slice_start = (page_start - offset) as usize; // usize >= u64 on 64-bit
                        #[allow(clippy::cast_possible_truncation)]
                        let page_size = PAGE_SIZE as usize; // usize >= u64 on 64-bit
                        crcs[page] = crc_fast::crc32_iscsi(
                            &data[slice_start..slice_start + page_size],
                        );
                    } else if end > page_start && offset < page_end {
                        crcs[page] = 0;
                    }
                }
                self.page_crcs.insert(block_key, crcs);
            }
        }
    }

    /// Record per-page CRCs for a zero_range operation.
    pub(super) fn capture_zero_page_crcs(&self, offset: u64, len: u64) {
        const PAGE_SIZE: u64 = 4096;
        let zero_page_crc = crc_fast::crc32_iscsi(&[0u8; 4096]);
        let block_size = self.config.block_size as u64;
        let end = offset + len;
        let start_block = offset / block_size;
        let end_block = (end - 1) / block_size;
        let ppb = self.pages_per_block;

        for block in start_block..=end_block {
            let block_start = block * block_size;

            #[allow(clippy::cast_possible_truncation)]
            let block_key = block as u32; // block numbers fit in u32
            if let Some(mut entry) = self.page_crcs.get_mut(&block_key) {
                for page in 0..ppb {
                    let page_start = block_start + (page as u64) * PAGE_SIZE;
                    let page_end = page_start + PAGE_SIZE;
                    if offset <= page_start && end >= page_end {
                        entry[page] = zero_page_crc;
                    } else if end > page_start && offset < page_end {
                        entry[page] = 0;
                    }
                }
            } else {
                let mut crcs = vec![0u32; ppb].into_boxed_slice();
                for page in 0..ppb {
                    let page_start = block_start + (page as u64) * PAGE_SIZE;
                    let page_end = page_start + PAGE_SIZE;
                    if offset <= page_start && end >= page_end {
                        crcs[page] = zero_page_crc;
                    } else if end > page_start && offset < page_end {
                        crcs[page] = 0;
                    }
                }
                self.page_crcs.insert(block_key, crcs);
            }
        }
    }

    /// Check if block is present (lock-free read).
    #[inline]
    pub(super) fn is_present(&self, block_num: usize) -> bool {
        if block_num >= self.num_blocks {
            return false;
        }
        self.state_map.is_present(block_num)
    }

    /// Mark block as present (lock-free CAS NOT_PRESENT -> CLEAN).
    #[inline]
    pub(super) fn set_present(&self, block_num: usize) {
        if block_num >= self.num_blocks {
            return;
        }
        self.state_map.set_present(block_num);
    }

    /// Try to claim a NOT_PRESENT block (CAS NOT_PRESENT → CLEAN).
    /// Returns true if this call won the transition.
    #[inline]
    pub(super) fn try_set_present(&self, block_num: usize) -> bool {
        if block_num >= self.num_blocks {
            return false;
        }
        self.state_map.try_set_present(block_num)
    }

    /// CAS loop to transition a block to Dirty state (lock-free).
    ///
    /// Delegates the CAS logic to `SparseStateMap::transition_to_dirty()` and
    /// handles counter bookkeeping based on the result.
    ///
    /// Returns `true` if the state actually changed, `false` if already DIRTY.
    /// Used by the write path to skip redundant WAL entries.
    #[inline]
    pub(super) fn transition_to_dirty(&self, idx: usize) -> bool {
        use crate::block::block_map::DirtyTransition;
        match self.state_map.transition_to_dirty(idx) {
            DirtyTransition::AlreadyDirty => false,
            DirtyTransition::FromCleanOrNotPresent => {
                self.dirty_block_count.fetch_add(1, Ordering::Release);
                true
            }
            DirtyTransition::FromSyncing => {
                self.syncing_block_count.fetch_sub(1, Ordering::Release);
                self.dirty_block_count.fetch_add(1, Ordering::Release);
                true
            }
        }
    }

    /// Atomically claim a dirty block for flushing: CAS DIRTY→SYNCING.
    ///
    /// Returns true if the CAS succeeded (block claimed for this flush cycle).
    /// Returns false if the block is no longer DIRTY (already claimed or cleaned).
    #[inline]
    pub(super) fn transition_dirty_to_syncing(&self, idx: usize) -> bool {
        if self.state_map.transition_dirty_to_syncing(idx) {
            self.dirty_block_count.fetch_sub(1, Ordering::Release);
            self.syncing_block_count.fetch_add(1, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Atomically evict a flushed block: CAS SYNCING→NOT_PRESENT.
    ///
    /// After a successful S3 upload, the block data lives in S3 (and optionally
    /// in the clean cache). The local SSD copy is no longer needed.
    /// Returns true if the CAS succeeded (block evicted).
    /// Returns false if a concurrent write transitioned SYNCING→DIRTY.
    #[inline]
    pub(super) fn transition_syncing_to_not_present(&self, idx: usize) -> bool {
        if self.state_map.transition_syncing_to_not_present(idx) {
            self.syncing_block_count.fetch_sub(1, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Count present blocks (for metrics/logging).
    #[allow(dead_code)]
    pub(super) fn count_present(&self) -> usize {
        self.state_map.count_present()
    }

    /// True iff any block in the cache is currently SYNCING.
    ///
    /// Used by `flush_dirty_inner` to detect a stale rotation: if a
    /// flushing file is on disk and `flushing_active=true` but no block
    /// is SYNCING (e.g. every claimed block was promoted back to DIRTY
    /// or evicted), the flushing file is no longer load-bearing and the
    /// next flush cycle can safely clean it up before rotating again.
    pub(super) fn has_any_syncing(&self) -> bool {
        use crate::block::block_map::SparseBlockState;
        (0..self.num_blocks).any(|i| self.state_map.get(i) == SparseBlockState::SYNCING)
    }

    /// Promote SYNCING blocks from flushing → active file before a guest write.
    ///
    /// When a guest writes to a block being flushed (SYNCING), its pre-rotation
    /// data lives only in the flushing file. Without promotion, the guest's
    /// sub-block write lands in the active file but the rest of the block is
    /// zeros — the block is split across two files.
    ///
    /// Promote copies the full block from flushing → active BEFORE the guest
    /// pwrite, so the active file always has complete data for every DIRTY block.
    /// After the copy, CAS SYNCING→DIRTY so the flush thread's eviction CAS
    /// will fail — preventing a race where eviction clears the block between
    /// promote and the caller's transition_to_dirty.
    ///
    /// Idempotent: multiple concurrent promotions of the same block copy the
    /// same data. The flushing file is immutable after rotation.
    ///
    /// Called under the data_file read lock (passed as `df`).
    pub(super) fn promote_syncing_blocks(
        &self,
        df: &SyncFile,
        start_block: u64,
        end_block: u64,
        require_promotion: bool,
    ) -> Result<(), super::CacheError> {
        use crate::block::block_map::SparseBlockState;

        let block_size = self.config.block_size as u64;
        // Clone the flushing file Arc under the lock and release immediately.
        // The Arc keeps the SyncFile (and its fd) alive even after the flush
        // thread clears flushing_file and deletes the physical file — Unix
        // guarantees an unlinked file remains accessible via open fds.
        //
        // This avoids holding the Mutex across the entire IO loop (potentially
        // dozens of pread+pwrite calls), which would block rotate_data_file
        // and compute_flush_batch from accessing flushing_file.
        let ff = self.flushing_file.lock().clone();

        #[cfg(feature = "test-utils")]
        {
            let sp = self.promote_sync.lock().clone();
            if let Some(sp) = sp {
                sp.gate_sync(PromoteStep::AfterCloneArc, start_block as usize);
            }
        }

        if let Some(ref ff) = ff {
            // Allocate once, reuse across blocks (avoids per-block 128KB alloc churn).
            let mut buf = vec![0u8; block_size as usize];
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.num_blocks {
                    continue;
                }
                let offset = block * block_size;
                let valid = std::cmp::min(
                    block_size,
                    self.config.device_size.saturating_sub(offset),
                ) as usize;
                if valid == 0 {
                    continue;
                }
                // One overall deadline bounds the total time spent waiting
                // on other promoters of this block, so a panicking holder
                // can't park us indefinitely across re-check iterations.
                let deadline =
                    std::time::Instant::now() + std::time::Duration::from_secs(5);
                // Per-block promote claim + RE-CHECK loop. The state CAS
                // stays at the end (kept idempotent for the eviction-during-
                // promote contract covered by `pf02_eviction_during_promote_
                // read`), but we also need to prevent two concurrent
                // promoters from racing their pwrites: if A's pwrite, A's
                // WRITE_FIXED, and B's pwrite interleave in that order, B's
                // OLD bytes clobber A's just-landed NEW bytes — the blktests
                // block/042 corruption flake.
                //
                // The `promote_claim` bitmap is a side-band gate orthogonal
                // to the state machine: at most one task pwrites per block
                // at a time, but `state == SYNCING` is preserved across the
                // pread+pwrite window so the flush thread's
                // `transition_syncing_to_not_present` can still race
                // through to NOT_PRESENT.
                //
                // **Why a waiter must RE-CHECK instead of skipping:** the
                // claim holder may have promoted NOTHING — it bails with
                // `BlockEvicted` when the block is NOT_PRESENT with stale
                // zeros in the flushing file (`require_promotion`), or skips
                // for the same condition on the plain-write path. A waiter
                // that blindly proceeds after the release would return `Ok`
                // without anyone having recovered the block's bytes; its
                // caller's sub-block pwrite/WRITE_FIXED then lands on sparse
                // zeros and the commit marks the half-zero block DIRTY —
                // uploaded corrupt, read back as zeros after a cold restart.
                // That was the residual `fio_verify_random_cold_wake`
                // corruption (~30 of 32 pages zero, the 2 survivors being
                // exactly the waiters' slices). Re-reading the state closes
                // it: holder promoted → state is DIRTY → break; holder
                // bailed → state is still NOT_PRESENT/SYNCING → we claim and
                // run the same stale-zero check ourselves.
                loop {
                    let state = self.state_map.get(idx);
                    if state == SparseBlockState::CLEAN {
                        // CLEAN = claimed by a writer, data pending. Two
                        // sub-cases, distinguished by the promote claim:
                        //
                        // * claim HELD ⇒ an S3 MATERIALIZATION is in flight
                        //   (`claim_block_for_materialization` holds the
                        //   claim across fetch + `write_materialized`). For
                        //   a guest-write promote (`require_promotion`),
                        //   proceeding would let the caller's slice land on
                        //   unmaterialized bytes. Bail with `BlockEvicted` —
                        //   the caller retries WITHOUT the rotation gate
                        //   (its backfill CLEAN-wait parks gate-free). We
                        //   must NOT park here: our caller holds the
                        //   rotation read gate, and the materializer's
                        //   `write_materialized` acquires the data_file
                        //   WRITE lock — waiting in-gate on its claim is a
                        //   lock-order deadlock.
                        //   (The materializer's own embedded promote passes
                        //   require_promotion=false and breaks below — no
                        //   self-trip.)
                        //
                        // * claim FREE ⇒ the CLEAN owner is a zero-prior
                        //   claim (remainder legitimately sparse zeros) or a
                        //   full-block writer (its own guest write covers
                        //   every byte) — safe to proceed. No new
                        //   materializer can appear: materialization claims
                        //   only NOT_PRESENT blocks.
                        if require_promotion && self.promote_claim.is_claimed(idx) {
                            return Err(super::CacheError::BlockEvicted);
                        }
                        break;
                    }
                    if state != SparseBlockState::SYNCING
                        && state != SparseBlockState::NOT_PRESENT
                    {
                        // Present (DIRTY) — promoted by a prior holder or
                        // never needed promotion. Nothing to do.
                        break;
                    }
                    if !self.promote_claim.try_claim(idx) {
                        // Another promoter holds the claim. Park on the
                        // condvar until they release, then RE-CHECK the
                        // state (loop) — never assume the holder succeeded.
                        if !self.promote_claim.wait_for_release(idx, deadline) {
                            tracing::warn!(
                                block = idx,
                                "promote: timed out waiting for prior promote claim release"
                            );
                            if require_promotion {
                                // Refuse to proceed on an unverified block —
                                // the caller re-backfills from S3. Erring is
                                // recoverable; proceeding can corrupt.
                                return Err(super::CacheError::BlockEvicted);
                            }
                            // Plain-write path: best-effort proceed, same as
                            // the pre-deadline behavior.
                            break;
                        }
                        continue;
                    }
                    // We hold the claim. Wrap the rest in a RAII guard so a
                    // panic or early return — including the `?` propagation on
                    // pwrite errors — releases the claim and wakes waiters.
                    // Without this, a panicking holder would park every
                    // subsequent promoter for the full 5 s deadline.
                    struct ClaimGuard<'a> {
                        claim: &'a PromoteClaimBitmap,
                        idx: usize,
                    }
                    impl<'a> Drop for ClaimGuard<'a> {
                        fn drop(&mut self) {
                            self.claim.release(self.idx);
                        }
                    }
                    let _guard = ClaimGuard { claim: &self.promote_claim, idx };

                    // Propagate errors: silent skip on a flushing-read failure
                    // would leave the active file as zeros for un-covered
                    // bytes of the sibling's WRITE_FIXED.
                    ff.read_exact_at(&mut buf[..valid], offset)?;

                    // The flushing file is only an authoritative data source for
                    // SYNCING blocks — those were claimed (DIRTY→SYNCING) by the
                    // CURRENT flush's rotation, so their bytes are in it. A
                    // NOT_PRESENT block is different: it may have been evicted by an
                    // EARLIER flush, in which case its real data is in S3 and THIS
                    // flushing generation holds only sparse zeros at its offset.
                    // Promoting those zeros into the active file masks the real data,
                    // and the next flush packs the zeros — silent data loss (a
                    // cold-read-as-zero that fsck/CRC verify catches as corruption).
                    //
                    // So never promote a NOT_PRESENT block whose flushing-file data is
                    // all zeros: its bytes aren't reliably here. Route the caller to
                    // the S3 backfill path (BlockEvicted), which fetches the
                    // authoritative block — the evicting flush uploaded its pack and
                    // recorded it in the manifest before transitioning it to
                    // NOT_PRESENT, so S3 has the current data (zero or not). A
                    // genuinely-zero just-evicted block costs one redundant S3 fetch;
                    // a stale one is rescued from corruption.
                    if state == SparseBlockState::NOT_PRESENT
                        && buf[..valid].iter().all(|&b| b == 0)
                    {
                        if require_promotion {
                            return Err(super::CacheError::BlockEvicted);
                        }
                        // Best-effort (plain write): leave NOT_PRESENT so set_present
                        // + the caller's write reconstruct it; don't write zeros.
                        break;
                    }

                    #[cfg(feature = "test-utils")]
                    {
                        let sp = self.promote_sync.lock().clone();
                        if let Some(sp) = sp {
                            sp.gate_sync(PromoteStep::AfterRead, idx);
                        }
                    }

                    df.write_all_at(&buf[..valid], offset)?;

                    #[cfg(feature = "test-utils")]
                    {
                        let sp = self.promote_sync.lock().clone();
                        if let Some(sp) = sp {
                            sp.gate_sync(PromoteStep::BeforeCas, idx);
                        }
                    }

                    // Final CAS preserves the original contract: SYNCING→DIRTY
                    // or NOT_PRESENT→DIRTY at the end of promote. Eviction may
                    // have raced (state→NOT_PRESENT) — in that case CAS fails
                    // harmlessly; the pwrite we already landed is still
                    // correct in the active file.
                    if state == SparseBlockState::SYNCING {
                        if self
                            .state_map
                            .cas(idx, SparseBlockState::SYNCING, SparseBlockState::DIRTY)
                            .is_ok()
                        {
                            self.syncing_block_count.fetch_sub(1, Ordering::Release);
                            self.dirty_block_count.fetch_add(1, Ordering::Release);
                        }
                    } else if self
                        .state_map
                        .cas(idx, SparseBlockState::NOT_PRESENT, SparseBlockState::DIRTY)
                        .is_ok()
                    {
                        self.dirty_block_count.fetch_add(1, Ordering::Release);
                    }
                    // _guard drops here, releasing the claim + waking waiters.
                    break;
                }
            }
        } else if require_promotion {
            // No flushing file available. If any block in the range needs
            // promotion (SYNCING or NOT_PRESENT from a just-completed flush),
            // we can't recover its data — the flushing file has been taken.
            // Return BlockEvicted so the caller falls back to S3 backfill.
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.num_blocks {
                    continue;
                }
                let state = self.state_map.get(idx);
                if state == SparseBlockState::SYNCING
                    || state == SparseBlockState::NOT_PRESENT
                {
                    return Err(super::CacheError::BlockEvicted);
                }
                // Same as the flushing-file branch's CLEAN arm: a CLEAN
                // block whose promote claim is held is an in-flight S3
                // materialization — bail so the caller retries gate-free
                // (its backfill CLEAN-wait). A rotation needn't be active
                // for a materialization to be.
                if state == SparseBlockState::CLEAN
                    && self.promote_claim.is_claimed(idx)
                {
                    return Err(super::CacheError::BlockEvicted);
                }
            }
        }
        Ok(())
    }

    /// Persist block states to metadata file.
    ///
    /// v5 sparse format: only writes entries with non-zero state (present blocks),
    /// plus a trailing max_sequence u64. Uses atomic write pattern: temp file ->
    /// fsync -> rename.
    pub(super) fn save_block_states(&self) -> Result<(), CacheError> {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = self.config.metadata_path();
        // Use a unique temp file name to prevent a race between concurrent
        // callers (flush_to_s3's checkpoint vs flush_scheduler's checkpoint).
        // Without this, the first caller's rename moves .meta.tmp away and
        // the second caller's rename fails with ENOENT.
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp_path = path.with_extension(format!("meta.tmp.{id}"));

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Incremental CRC32 hasher — fed every byte written to the file.
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);

        // Helper: write to file and feed the hasher.
        macro_rules! write_hashed {
            ($data:expr) => {{
                let d = $data;
                file.write_all(d)?;
                hasher.update(d);
            }};
        }

        // Write header
        write_hashed!(METADATA_MAGIC);
        write_hashed!(&METADATA_VERSION.to_le_bytes());
        write_hashed!(&self.config.device_size.to_le_bytes());
        write_hashed!(&(self.config.block_size as u64).to_le_bytes());
        write_hashed!(&(self.num_blocks as u64).to_le_bytes());

        // v4/v5 sparse format: entry_count(u64) + entries of (index u32, state u8).
        // Collect into a Vec first so entry_count is consistent with the
        // entries written. Without this, a concurrent set_present() between
        // count_present() and iter_present() could yield more entries than
        // the header claims, failing CRC on reload.
        let entries: Vec<(usize, u8)> = self.state_map.iter_present().collect();
        let entry_count = entries.len() as u64;
        write_hashed!(&entry_count.to_le_bytes());

        for (idx, state) in &entries {
            write_hashed!(&(*idx as u32).to_le_bytes());
            write_hashed!(&[*state]);
        }

        // Trailing fields: max_sequence + rotation_seq (u64 LE each)
        write_hashed!(&self.sequence.current().to_le_bytes());
        write_hashed!(&self.rotation_seq.load(Ordering::Relaxed).to_le_bytes());

        // CRC32 trailer over all preceding bytes.
        let crc = hasher.finalize() as u32;
        file.write_all(&crc.to_le_bytes())?;

        // Fsync temp file to ensure data is on disk
        file.sync_all()?;
        drop(file);

        // Atomic rename (POSIX guarantees this is atomic)
        std::fs::rename(&tmp_path, &path)?;

        // Fsync the parent directory so the rename is durable across power loss.
        // Without this, the directory entry update can be lost on crash.
        if let Some(parent) = path.parent() {
            match File::open(parent) {
                Ok(dir) => {
                    if let Err(e) = dir.sync_all() {
                        warn!(
                            error = %e,
                            "dir fsync after metadata rename failed — durability weakened"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "failed to open parent dir for fsync — durability weakened"
                    );
                }
            }
        }

        let present_count = entry_count;
        debug!(
            path = %path.display(),
            blocks = self.num_blocks,
            present = present_count,
            "saved block states (atomic)"
        );
        Ok(())
    }

    /// Persist block states and presence.
    pub(super) fn save_metadata(&self) -> Result<(), CacheError> {
        self.save_block_states()
    }

    /// Load block states and presence from metadata file.
    ///
    /// Returns `(SparseStateMap, dirty_count, max_sequence, rotation_seq)`.
    pub(super) fn load_metadata(
        config: &WriteCacheConfig,
    ) -> Result<(SparseStateMap, usize, u64, u64), CacheError> {
        let path = config.metadata_path();
        let num_blocks = config.num_blocks();

        if !path.exists() {
            // No metadata file -- all blocks are NOT_PRESENT
            debug!(path = %path.display(), "no metadata file, starting fresh");
            return Ok((SparseStateMap::new(num_blocks), 0, 0, 0));
        }

        let mut file = File::open(&path)?;
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);

        // Helper: read from file and feed the hasher.
        macro_rules! read_hashed {
            ($buf:expr) => {{
                file.read_exact($buf)?;
                hasher.update($buf);
            }};
        }

        let mut header = [0u8; 8 + 4 + 8 + 8 + 8]; // magic + version + size + block_size + num_blocks
        read_hashed!(&mut header);

        // Validate header
        if &header[0..8] != METADATA_MAGIC {
            warn!("Invalid cache metadata magic bytes");
            return Err(CacheError::invalid_metadata());
        }

        let version = u32::from_le_bytes(header[8..12].try_into().unwrap());

        let device_size = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let block_size = u64::from_le_bytes(header[20..28].try_into().unwrap());
        let stored_num_blocks = u64::from_le_bytes(header[28..36].try_into().unwrap()) as usize;

        // Validate block size matches (must be identical)
        if block_size != config.block_size as u64 {
            warn!(
                stored_block = block_size,
                config_block = config.block_size,
                "Block size mismatch"
            );
            return Err(CacheError::invalid_metadata());
        }

        // Validate device size (allow grow, reject shrink)
        if config.device_size < device_size {
            warn!(
                stored_size = device_size,
                config_size = config.device_size,
                "Cannot shrink device"
            );
            return Err(CacheError::invalid_metadata());
        }

        let is_growing = config.device_size > device_size;
        if is_growing {
            info!(
                old_size = device_size,
                new_size = config.device_size,
                "Growing device"
            );
        }

        let state_map = SparseStateMap::new(num_blocks);
        let mut dirty_count = 0;

        // max_sequence persisted in v5+, defaults to 0 for older formats
        let mut persisted_max_seq: u64 = 0;

        if version < 4 {
            return Err(CacheError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported metadata version {version} (minimum 4)"),
            )));
        }

        // v4/v5: sparse format -- entry_count(u64) + entries of index(u32) + state(u8)
        let mut count_buf = [0u8; 8];
        read_hashed!(&mut count_buf);
        let entry_count = u64::from_le_bytes(count_buf) as usize;

        let mut entry_buf = [0u8; 5]; // u32 index + u8 state
        for _ in 0..entry_count {
            read_hashed!(&mut entry_buf);
            let idx = u32::from_le_bytes(entry_buf[0..4].try_into().unwrap()) as usize;
            let state = entry_buf[4];

            if idx >= num_blocks {
                continue; // skip out-of-bounds (shrink safety)
            }

            // Count SYNCING and DIRTY blocks (both need eventual flush).
            // SYNCING→DIRTY conversion is deferred to init.rs so crash
            // recovery can use the SYNCING state to identify pre-rotation
            // blocks whose data is in the flushing file.
            if state == SparseBlockState::DIRTY || state == SparseBlockState::SYNCING {
                dirty_count += 1;
            }

            // Populate state_map: first set_present (0->1), then CAS to target state
            // Ignore budget errors during load (no budget set yet).
            state_map.set_present(idx);
            if state != SparseBlockState::CLEAN {
                let _ = state_map.cas(idx, SparseBlockState::CLEAN, state);
            }
        }

        // Trailing fields: max_sequence + rotation_seq
        let mut seq_buf = [0u8; 8];
        read_hashed!(&mut seq_buf);
        persisted_max_seq = u64::from_le_bytes(seq_buf);

        let mut rot_buf = [0u8; 8];
        read_hashed!(&mut rot_buf);
        let rotation_seq = u64::from_le_bytes(rot_buf);

        // Verify CRC32 trailer (mandatory).
        let computed_crc = hasher.finalize() as u32;
        let mut crc_buf = [0u8; 4];
        file.read_exact(&mut crc_buf).map_err(|_| {
            warn!("metadata file missing CRC32 trailer");
            CacheError::invalid_metadata()
        })?;
        let stored_crc = u32::from_le_bytes(crc_buf);
        if stored_crc != computed_crc {
            warn!(
                stored_crc,
                computed_crc, "metadata CRC32 mismatch — file corrupted"
            );
            return Err(CacheError::invalid_metadata());
        }

        if is_growing {
            info!(
                old_blocks = stored_num_blocks,
                new_blocks = num_blocks,
                "Growing device (new blocks are NOT_PRESENT)"
            );
        }

        let present_count = state_map.count_present();
        info!(
            path = %path.display(),
            blocks = num_blocks,
            dirty = dirty_count,
            present = present_count,
            "loaded cache metadata"
        );

        Ok((state_map, dirty_count, persisted_max_seq, rotation_seq))
    }
}
