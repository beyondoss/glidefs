use dashmap::DashMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

use crate::block::block_map::{Blake3Hash, SequenceNumber, SparseBlockState, SparseStateMap};

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

    /// Local cache file (data) - encrypted at rest
    /// Uses positional I/O (pread/pwrite) which is lock-free and thread-safe
    pub(super) data_file: SyncFile,

    /// Sparse block state map - LOCK-FREE
    /// Combines block state and presence into a single sparse page table.
    /// State encoding: 0=NotPresent, 1=Clean, 2=Dirty, 3=Syncing
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
    /// Mutex is contended under concurrent writes (multiple NBD I/O tasks
    /// per export), but each write batches all its WAL entries under a
    /// single lock acquisition to minimize hold time.
    pub(super) wal: Mutex<Wal>,

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
    /// checkpoint and flush. Sized proportional to currently dirty blocks, not
    /// device size. Concurrently accessed by: the write path (inserts
    /// CRC_SENTINEL on every write), the checkpoint path (computes and stores
    /// CRCs), and the flush path (takes CRCs for verification). DashMap
    /// provides the necessary concurrent access safety.
    pub(super) crc_map: DashMap<usize, u32>,

    /// Per-export flush serialization lock.
    ///
    /// Serializes flush + manifest upload operations to prevent concurrent
    /// callers (drain, flush_scheduler, snapshot) from uploading stale
    /// manifests. Without this, two concurrent `flush_to_s3` calls can each
    /// serialize the in-memory VolumeManifest at different points, and
    /// last-writer-wins on S3 can overwrite a correct manifest with a stale one.
    pub(crate) flush_lock: tokio::sync::Mutex<()>,
}

impl CacheInner {
    /// Get the raw file descriptor of the data file (for io_uring registration).
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    #[inline]
    pub(crate) fn data_file_fd(&self) -> std::os::unix::io::RawFd {
        self.data_file.as_raw_fd()
    }

    /// Check if block is present (lock-free read).
    #[allow(dead_code)]
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

    /// CAS loop to transition a block to Dirty state (lock-free).
    ///
    /// Handles three source states (sparse encoding):
    /// - **Clean(1) -> Dirty(2)**: increments dirty_block_count.
    /// - **Syncing(3) -> Dirty(2)**: decrements syncing_block_count, increments dirty_block_count.
    /// - **Dirty(2) -> Dirty(2)**: no-op.
    #[inline]
    pub(super) fn transition_to_dirty(&self, idx: usize) {
        loop {
            let current = self.state_map.get(idx);

            debug_assert_ne!(
                current,
                SparseBlockState::NOT_PRESENT,
                "transition_to_dirty called on NOT_PRESENT block {idx}"
            );

            if current == SparseBlockState::DIRTY {
                break;
            }

            if current == SparseBlockState::CLEAN {
                if self
                    .state_map
                    .cas(idx, SparseBlockState::CLEAN, SparseBlockState::DIRTY)
                    .is_ok()
                {
                    self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            } else if current == SparseBlockState::SYNCING {
                if self
                    .state_map
                    .cas(idx, SparseBlockState::SYNCING, SparseBlockState::DIRTY)
                    .is_ok()
                {
                    self.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                    self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            } else {
                break;
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

    /// Atomically finalize a flushed block: CAS SYNCING→CLEAN.
    ///
    /// Returns true if the CAS succeeded (block is now clean).
    /// Returns false if a concurrent write transitioned SYNCING→DIRTY,
    /// meaning the block must be re-flushed in the next cycle.
    #[inline]
    pub(super) fn transition_syncing_to_clean(&self, idx: usize) -> bool {
        if self
            .state_map
            .cas(idx, SparseBlockState::SYNCING, SparseBlockState::CLEAN)
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

    // -- CRC32 DashMap methods (for dirty-block corruption detection) ----------

    /// Store a CRC32 checksum for a dirty block (checkpoint path).
    /// Only inserts if no entry exists — a concurrent write that re-dirtied
    /// the block should not overwrite a fresh CRC.
    #[inline]
    pub(super) fn crc_store(&self, idx: usize, crc: u32) {
        self.crc_map.entry(idx).or_insert(crc);
    }

    /// Remove and return the CRC32 checksum for a block (flush path).
    #[inline]
    pub(super) fn crc_take(&self, idx: usize) -> Option<u32> {
        self.crc_map.remove(&idx).map(|(_, v)| v)
    }

    /// Persist block states to metadata file.
    ///
    /// v5 sparse format: only writes entries with non-zero state (present blocks),
    /// plus a trailing max_sequence u64. Uses atomic write pattern: temp file ->
    /// fsync -> rename.
    pub(super) fn save_block_states(&self) -> Result<(), CacheError> {
        let path = self.config.metadata_path();
        let tmp_path = path.with_extension("meta.tmp");

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
        let entry_count = self.state_map.count_present() as u64;
        write_hashed!(&entry_count.to_le_bytes());

        for (idx, state) in self.state_map.iter_present() {
            write_hashed!(&(idx as u32).to_le_bytes());
            write_hashed!(&[state]);
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
