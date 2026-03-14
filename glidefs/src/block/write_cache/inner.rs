use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{debug, info, warn};

use crate::block::block_map::{
    Blake3Hash, SequenceNumber, SparseBlockState, SparseCrcMap, SparseStateMap,
};

use crate::block::wal::Wal;

use super::config::WriteCacheConfig;
use super::error::CacheError;

use bytes::Bytes;

/// Magic bytes for cache metadata file
pub(super) const METADATA_MAGIC: &[u8; 8] = b"ZFSCACHE";
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
    /// which use `pread`/`pwrite` system calls — atomic per POSIX, no shared
    /// file position. The inner `File` is module-private so no code outside
    /// this module can call seek-based methods.
    #[derive(Debug)]
    pub struct SyncFile {
        file: File,
    }

    // SAFETY: SyncFile only exposes positional I/O methods (pread/pwrite via
    // FileExt::{read_exact_at, write_all_at}) which are atomic per POSIX.
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
    }
}

pub use sync_file::SyncFile;

/// Sentinel value for CRC32 checksums invalidated by a concurrent write.
///
/// The write path stores this instead of removing the CRC entry, preventing
/// `compute_dirty_crc32s` from re-inserting a stale CRC via `or_insert`.
/// The flush path skips CRC verification when it encounters this sentinel.
///
/// CRC32 can legitimately produce u32::MAX (1-in-4-billion chance), in which
/// case we simply skip corruption detection for that one block — no correctness
/// impact, only a negligible loss of SSD corruption detection.
pub(super) const CRC_SENTINEL: u32 = u32::MAX;

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

/// Internal state shared across all cache states.
///
/// Uses lock-free atomics for block states and presence to avoid contention
/// under high write concurrency. The data file uses positional I/O which is
/// inherently thread-safe, eliminating all locking on the hot path.
pub(crate) struct CacheInner {
    /// Configuration
    pub(super) config: WriteCacheConfig,

    /// Local cache file (data).
    /// Uses positional I/O (pread/pwrite) which is thread-safe. RwLock is
    /// read-locked for all I/O (~2ns overhead); write-locked only during
    /// file rotation (once per flush cycle).
    pub(super) data_file: parking_lot::RwLock<SyncFile>,

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

    /// Number of blocks (for bounds checking)
    pub(super) num_blocks: usize,

    /// Statistics
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

    /// CRC32 checksums for dirty blocks, used to detect SSD corruption between
    /// checkpoint and flush. Concurrently accessed by: the write path (stores
    /// CRC_SENTINEL on every write), the checkpoint path (computes and stores
    /// CRCs), and the flush path (takes CRCs for verification). SparseCrcMap
    /// provides lock-free concurrent access via AtomicU32 leaves in a two-level
    /// page table, with 5x less memory than DashMap and zero shard contention.
    pub(super) crc_map: SparseCrcMap,

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

}


impl CacheInner {
    /// Get the raw file descriptor of the data file (for io_uring registration).
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    #[inline]
    pub(crate) fn data_file_fd(&self) -> std::os::unix::io::RawFd {
        self.data_file.read().as_raw_fd()
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
    /// Handles four source states (sparse encoding):
    /// - **Clean(1) -> Dirty(2)**: increments dirty_block_count.
    /// - **Syncing(3) -> Dirty(2)**: decrements syncing_block_count, increments dirty_block_count.
    /// - **Dirty(2) -> Dirty(2)**: no-op.
    /// - **NotPresent(0) -> Dirty(2)**: increments dirty_block_count. This
    ///   handles a race where promote_syncing_blocks copied data and the guest
    ///   wrote, but the flush thread evicted the block (SYNCING→NOT_PRESENT)
    ///   before this call. The data is already in the active file.
    ///
    /// Returns `true` if the state actually changed, `false` if already DIRTY.
    /// Used by the write path to skip redundant WAL entries.
    #[inline]
    pub(super) fn transition_to_dirty(&self, idx: usize) -> bool {
        loop {
            let current = self.state_map.get(idx);

            if current == SparseBlockState::DIRTY {
                return false;
            }

            if current == SparseBlockState::CLEAN
                || current == SparseBlockState::NOT_PRESENT
            {
                if self
                    .state_map
                    .cas(idx, current, SparseBlockState::DIRTY)
                    .is_ok()
                {
                    self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            } else if current == SparseBlockState::SYNCING {
                if self
                    .state_map
                    .cas(idx, SparseBlockState::SYNCING, SparseBlockState::DIRTY)
                    .is_ok()
                {
                    self.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                    self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    /// Atomically claim a dirty block for flushing: CAS DIRTY→SYNCING.
    ///
    /// Returns true if the CAS succeeded (block claimed for this flush cycle).
    /// Returns false if the block is no longer DIRTY (already claimed or cleaned).
    #[inline]
    pub(super) fn transition_dirty_to_syncing(&self, idx: usize) -> bool {
        if self
            .state_map
            .cas(idx, SparseBlockState::DIRTY, SparseBlockState::SYNCING)
            .is_ok()
        {
            self.dirty_block_count.fetch_sub(1, Ordering::Relaxed);
            self.syncing_block_count.fetch_add(1, Ordering::Relaxed);
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
        if self
            .state_map
            .cas(idx, SparseBlockState::SYNCING, SparseBlockState::NOT_PRESENT)
            .is_ok()
        {
            self.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
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
    ) -> std::io::Result<()> {
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
        if let Some(ref ff) = ff {
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.num_blocks {
                    continue;
                }
                let state = self.state_map.get(idx);
                if state != SparseBlockState::SYNCING
                    && state != SparseBlockState::NOT_PRESENT
                {
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
                let mut buf = vec![0u8; valid];
                // Propagate both read and write errors. If reading from
                // the flushing file fails (SSD error), we must not
                // silently skip promotion — the active file has zeros for
                // this block, so a sub-block guest write would leave the
                // non-written portion as zeros instead of the original
                // data. Failing the write to the guest is safer than
                // silent data corruption.
                ff.read_exact_at(&mut buf, offset)?;
                df.write_all_at(&buf, offset)?;
                if state == SparseBlockState::SYNCING {
                    // CAS SYNCING→DIRTY immediately after copying data.
                    // This prevents the flush thread from evicting the block
                    // (SYNCING→NOT_PRESENT) between here and the caller's
                    // transition_to_dirty. If the CAS fails, either another
                    // writer promoted it first (fine) or flush already evicted
                    // it (the data we just copied is still valid in active).
                    if self
                        .state_map
                        .cas(idx, SparseBlockState::SYNCING, SparseBlockState::DIRTY)
                        .is_ok()
                    {
                        self.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                        self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    // NOT_PRESENT: block was evicted between pre_write (which
                    // saw SYNCING and skipped backfill) and now. The flushing
                    // file still has the data. CAS NOT_PRESENT→DIRTY.
                    if self
                        .state_map
                        .cas(idx, SparseBlockState::NOT_PRESENT, SparseBlockState::DIRTY)
                        .is_ok()
                    {
                        self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        Ok(())
    }

    // -- CRC32 SparseCrcMap methods (for dirty-block corruption detection) -----

    /// Store a CRC32 checksum for a dirty block (checkpoint path).
    /// Only inserts if no entry exists — a concurrent write that re-dirtied
    /// the block should not overwrite a fresh CRC.
    #[inline]
    pub(super) fn crc_store(&self, idx: usize, crc: u32) {
        self.crc_map.try_insert(idx, crc);
    }

    /// Remove and return the CRC32 checksum for a block (flush path).
    #[inline]
    pub(super) fn crc_take(&self, idx: usize) -> Option<u32> {
        self.crc_map.take(idx)
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
        // callers (flush_to_s3's checkpoint vs flush_scheduler's local_checkpoint).
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
        let mut hasher = crc32fast::Hasher::new();

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

        // v5: append max_sequence as trailing u64 LE
        write_hashed!(&self.sequence.current().to_le_bytes());

        // CRC32 trailer over all preceding bytes.
        let crc = hasher.finalize();
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
    /// Returns `(SparseStateMap, dirty_count, max_sequence)`. Handles legacy
    /// v1/v2/v3/v4 formats by converting the old encoding (Clean=0, Dirty=1,
    /// Syncing=2) plus separate presence bitmap into the new sparse encoding
    /// (NotPresent=0, Clean=1, Dirty=2, Syncing=3). max_sequence is 0 for
    /// formats prior to v5.
    pub(super) fn load_metadata(
        config: &WriteCacheConfig,
    ) -> Result<(SparseStateMap, usize, u64), CacheError> {
        let path = config.metadata_path();
        let num_blocks = config.num_blocks();

        if !path.exists() {
            // No metadata file -- all blocks are NOT_PRESENT
            debug!(path = %path.display(), "no metadata file, starting fresh");
            return Ok((SparseStateMap::new(num_blocks), 0, 0));
        }

        let mut file = File::open(&path)?;
        let mut hasher = crc32fast::Hasher::new();

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
            let mut state = entry_buf[4];

            if idx >= num_blocks {
                continue; // skip out-of-bounds (shrink safety)
            }

            // Convert Syncing -> Dirty (conservative for crash recovery)
            if state == SparseBlockState::SYNCING {
                state = SparseBlockState::DIRTY;
            }
            if state == SparseBlockState::DIRTY {
                dirty_count += 1;
            }

            // Populate state_map: first set_present (0->1), then CAS to target state
            // Ignore budget errors during load (no budget set yet).
            state_map.set_present(idx);
            if state != SparseBlockState::CLEAN {
                let _ = state_map.cas(idx, SparseBlockState::CLEAN, state);
            }
        }

        // v5: read trailing max_sequence
        if version >= 5 {
            let mut seq_buf = [0u8; 8];
            read_hashed!(&mut seq_buf);
            persisted_max_seq = u64::from_le_bytes(seq_buf);
        }

        // Verify CRC32 trailer (mandatory).
        let computed_crc = hasher.finalize();
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

        Ok((state_map, dirty_count, persisted_max_seq))
    }
}
