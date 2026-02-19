use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::{Mutex, RwLock};
use tracing::{debug, info, warn};

use std::collections::{HashMap, HashSet};

use crate::nbd::block_map::{
    BlockMap, BlockMapKind, Blake3Hash, SequenceNumber,
    SparseBlockState, SparseStateMap,
};

/// Cached state from the most recent full (base) manifest upload.
///
/// Used to compute delta manifests: diff current block_map against
/// this cached snapshot to produce upserts and deletes.
pub(super) struct BaseManifestState {
    pub sequence: u64,
    pub block_map: HashMap<u64, Blake3Hash>,
    pub syncs_since_base: u32,
}
use crate::nbd::state::BlockState;
use crate::nbd::wal::Wal;

use super::config::WriteCacheConfig;
use super::error::CacheError;

use bytes::Bytes;

/// Magic bytes for cache metadata file
pub(super) const METADATA_MAGIC: &[u8; 8] = b"ZFSCACHE";
/// Version 4: sparse state map (only non-zero entries persisted)
pub(super) const METADATA_VERSION: u32 = 4;

/// A file handle safe for concurrent positional I/O.
///
/// This wrapper allows sharing a `File` across threads when using only
/// positional I/O methods (`read_at`, `write_at`, `read_exact_at`, `write_all_at`).
/// These methods use `pread`/`pwrite` system calls which are atomic and don't
/// use the internal file position, making them thread-safe per POSIX semantics.
///
/// # Safety
///
/// This type implements `Sync` because:
/// 1. We only expose positional I/O methods (pread/pwrite)
/// 2. POSIX guarantees pread/pwrite are atomic with respect to each other
/// 3. We never use seek-based read/write which would race on file position
/// 4. `sync_all()` is safe to call concurrently (just triggers fsync)
#[derive(Debug)]
pub struct SyncFile {
    file: File,
}

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

// Safety: SyncFile only exposes positional I/O methods which are thread-safe.
// pread/pwrite are atomic per POSIX and don't use the shared file position.
unsafe impl Sync for SyncFile {}
unsafe impl Send for SyncFile {}


/// Check if a block is all zeros.
///
/// Uses SIMD when available (AVX2 on x86_64), falling back to 64-bit word
/// comparison. For a 128KB block:
/// - Byte-by-byte: 131,072 comparisons
/// - u64 fallback: 16,384 comparisons
/// - AVX2 (256-bit): 4,096 comparisons
#[inline]
#[allow(dead_code)] // Used by tests
pub(super) fn is_zero_block(data: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We've verified AVX2 is available
            return unsafe { is_zero_block_avx2(data) };
        }
    }
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

    // === v2 content-addressed structures ===

    /// Content-addressed block map: chunk_index -> (Blake3Hash, sequence).
    ///
    /// Wrapped in RwLock<BlockMapKind> to support both full (AtomicBlockMap)
    /// and forked (ForkedBlockMap with DashMap overlay) variants.
    /// All normal reads/writes take a read lock (interior mutability handles
    /// the actual mutation). Write lock is only taken during rare flatten ops.
    pub(super) block_map: RwLock<BlockMapKind>,

    /// Monotonic sequence counter for snapshot consistency.
    /// Lock-free AtomicU64.
    pub(super) sequence: SequenceNumber,

    /// Write-ahead log for crash recovery.
    /// Mutex is effectively uncontended: single writer per export.
    pub(super) wal: Mutex<Wal>,

    /// Export name (used in WAL entries).
    pub(super) export_name: String,

    /// Pre-computed zero-block hash for this export's block_size.
    /// Used by flush, write, and read paths to identify trimmed/unwritten chunks.
    pub(super) zero_block_hash: Blake3Hash,

    /// Pre-computed zero-block bytes for this export's block_size.
    /// Avoids a heap allocation on every sparse read.
    pub(super) zero_block_bytes: Bytes,

    /// Cached set of hashes from the most recent manifest build.
    ///
    /// Updated on every `upload_full_manifest()` and on-demand via
    /// `rebuild_manifest_hashes()`. Used by pack index pruning to
    /// determine which entries are still needed — the manifest is the
    /// durable reference, not the live block_map (which has ZERO
    /// placeholders for in-flight flushes).
    pub(super) manifest_pack_hashes: Mutex<HashSet<Blake3Hash>>,

    /// Cached state from the last full (base) manifest upload.
    ///
    /// Used to compute delta manifests by diffing the current block_map
    /// against this snapshot. None until the first full manifest upload.
    pub(super) base_manifest_state: Mutex<Option<BaseManifestState>>,
}

impl CacheInner {
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

            if current == SparseBlockState::DIRTY {
                break;
            }

            if current == SparseBlockState::CLEAN {
                if self.state_map
                    .cas(idx, SparseBlockState::CLEAN, SparseBlockState::DIRTY)
                    .is_ok()
                {
                    self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            } else if current == SparseBlockState::SYNCING {
                if self.state_map
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

    /// Get a block map entry (takes read lock, effectively zero overhead).
    #[inline]
    pub(super) fn block_map_get(&self, chunk_index: usize) -> (Blake3Hash, u64) {
        self.block_map.read().get(chunk_index)
    }

    /// Set a block map entry (takes read lock — interior mutability handles the write).
    #[inline]
    pub(super) fn block_map_set(
        &self,
        chunk_index: usize,
        hash: Blake3Hash,
        seq: u64,
    ) {
        self.block_map.read().set(chunk_index, hash, seq);
    }

    /// Snapshot the block map (takes read lock).
    pub(super) fn block_map_snapshot(&self) -> BlockMap {
        self.block_map.read().snapshot(&self.state_map)
    }

    // -- CRC32 dirty-block integrity ------------------------------------------

    /// Load the CRC32 checksum for a chunk (takes read lock).
    #[inline]
    pub(super) fn block_map_get_crc32(&self, chunk_index: usize) -> u32 {
        self.block_map.read().get_crc32(chunk_index)
    }

    /// Clear the CRC32 checksum for a chunk (takes read lock).
    #[inline]
    pub(super) fn block_map_clear_crc32(&self, chunk_index: usize) {
        self.block_map.read().clear_crc32(chunk_index)
    }

    /// CAS the CRC32 checksum (takes read lock).
    #[inline]
    pub(super) fn block_map_cas_crc32(&self, chunk_index: usize, expected: u32, new: u32) -> Result<u32, u32> {
        self.block_map.read().cas_crc32(chunk_index, expected, new)
    }

    /// Count present blocks (for metrics/logging).
    #[allow(dead_code)]
    pub(super) fn count_present(&self) -> usize {
        self.state_map.count_present()
    }

    /// Persist block states to metadata file (fast path).
    ///
    /// v4 sparse format: only writes entries with non-zero state (present blocks).
    /// Does NOT persist the block map -- call `persist_block_map()` separately
    /// (outside the WAL lock) for that. Uses atomic write pattern: temp file ->
    /// fsync -> rename.
    pub(super) fn save_block_states(&self) -> Result<(), CacheError> {
        let path = self.config.metadata_path();
        let tmp_path = path.with_extension("meta.tmp");

        // Write to temp file first
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Write header
        file.write_all(METADATA_MAGIC)?;
        file.write_all(&METADATA_VERSION.to_le_bytes())?;
        file.write_all(&self.config.device_size.to_le_bytes())?;
        file.write_all(&(self.config.block_size as u64).to_le_bytes())?;
        file.write_all(&(self.num_blocks as u64).to_le_bytes())?;

        // v4 sparse format: collect (index, state) pairs for non-zero entries
        let mut sparse_entries: Vec<(u32, u8)> = Vec::new();
        for idx in 0..self.num_blocks {
            let state = self.state_map.get(idx);
            if state != SparseBlockState::NOT_PRESENT {
                sparse_entries.push((idx as u32, state));
            }
        }

        // Write entry count then entries: index(u32 LE) + state(u8) = 5 bytes each
        file.write_all(&(sparse_entries.len() as u64).to_le_bytes())?;
        for &(idx, state) in &sparse_entries {
            file.write_all(&idx.to_le_bytes())?;
            file.write_all(&[state])?;
        }

        // Fsync temp file to ensure data is on disk
        file.sync_all()?;
        drop(file);

        // Atomic rename (POSIX guarantees this is atomic)
        std::fs::rename(&tmp_path, &path)?;

        let present_count = sparse_entries.len();
        debug!(
            path = %path.display(),
            blocks = self.num_blocks,
            present = present_count,
            "saved block states (atomic)"
        );
        Ok(())
    }

    /// Persist the v2 block map (content hashes) to disk.
    ///
    /// Safe to call outside the WAL lock: the block map file only needs to be
    /// accurate for clean blocks (which don't change), and dirty blocks are
    /// re-hashed from SSD during crash recovery regardless.
    pub(super) fn persist_block_map(&self) -> Result<(), CacheError> {
        let block_map_path = self.config.block_map_path();
        let bm_snapshot = self.block_map_snapshot();
        if let Err(e) = bm_snapshot.persist_to_file(&block_map_path) {
            warn!(error = %e, "failed to persist block map");
            return Err(CacheError::Io(e));
        }
        Ok(())
    }

    /// Persist block states, presence, and block map.
    ///
    /// Convenience method that calls both `save_block_states` and
    /// `persist_block_map`. Used by callers that don't need to split
    /// these operations around a WAL lock.
    pub(super) fn save_metadata(&self) -> Result<(), CacheError> {
        self.save_block_states()?;
        self.persist_block_map()
    }

    /// Load block states and presence from metadata file.
    ///
    /// Returns `(SparseStateMap, dirty_count)`. Handles legacy v1/v2/v3 formats
    /// by converting the old encoding (Clean=0, Dirty=1, Syncing=2) plus
    /// separate presence bitmap into the new sparse encoding (NotPresent=0,
    /// Clean=1, Dirty=2, Syncing=3).
    pub(super) fn load_metadata(
        config: &WriteCacheConfig,
    ) -> Result<(SparseStateMap, usize), CacheError> {
        let path = config.metadata_path();
        let num_blocks = config.num_blocks();

        if !path.exists() {
            // No metadata file -- all blocks are NOT_PRESENT
            debug!(path = %path.display(), "no metadata file, starting fresh");
            return Ok((SparseStateMap::new(num_blocks), 0));
        }

        let mut file = File::open(&path)?;
        let mut header = [0u8; 8 + 4 + 8 + 8 + 8]; // magic + version + size + block_size + num_blocks
        file.read_exact(&mut header)?;

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

        if version >= 4 {
            // v4: sparse format -- entry_count(u64) + entries of index(u32) + state(u8)
            let mut count_buf = [0u8; 8];
            file.read_exact(&mut count_buf)?;
            let entry_count = u64::from_le_bytes(count_buf) as usize;

            let mut entry_buf = [0u8; 5]; // u32 index + u8 state
            for _ in 0..entry_count {
                file.read_exact(&mut entry_buf)?;
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
        } else {
            // Legacy v1/v2/v3: dense block_states + presence bitmap
            // Old encoding: Clean=0, Dirty=1, Syncing=2
            let mut old_state_bytes = vec![0u8; stored_num_blocks];
            file.read_exact(&mut old_state_bytes)?;

            // Convert Syncing(2) -> Dirty(1) in old encoding
            for state in &mut old_state_bytes {
                if *state == BlockState::Syncing as u8 {
                    *state = BlockState::Dirty as u8;
                }
                if *state == BlockState::Dirty as u8 {
                    dirty_count += 1;
                }
            }

            // Read presence bitmap (varies by version)
            let present: Vec<bool> = if version >= 3 {
                // Version 3: packed bits (1 bit per block)
                let num_bytes = stored_num_blocks.div_ceil(8);
                let mut present_bytes = vec![0u8; num_bytes];
                file.read_exact(&mut present_bytes)?;

                (0..stored_num_blocks)
                    .map(|i| {
                        let byte_idx = i / 8;
                        let bit_idx = i % 8;
                        present_bytes[byte_idx] & (1 << bit_idx) != 0
                    })
                    .collect()
            } else if version >= 2 {
                // Version 2: 1 byte per block
                let mut present_bytes = vec![0u8; stored_num_blocks];
                file.read_exact(&mut present_bytes)?;
                present_bytes.iter().map(|&b| b != 0).collect()
            } else {
                // Version 1: dirty blocks are present, clean blocks are NOT
                old_state_bytes
                    .iter()
                    .map(|&s| s == BlockState::Dirty as u8)
                    .collect()
            };

            // Convert old encoding to new sparse encoding and populate state_map
            for (idx, &old_state) in old_state_bytes.iter().enumerate() {
                let is_present = present.get(idx).copied().unwrap_or(false);
                if !is_present && old_state == BlockState::Clean as u8 {
                    // Not present + clean in old encoding -> NOT_PRESENT (0) in new
                    continue;
                }
                // Block is present (or dirty/syncing which implies present)
                let new_state = match old_state {
                    x if x == BlockState::Clean as u8 => SparseBlockState::CLEAN,
                    x if x == BlockState::Dirty as u8 => SparseBlockState::DIRTY,
                    _ => SparseBlockState::DIRTY, // conservative
                };
                state_map.set_present(idx);
                if new_state != SparseBlockState::CLEAN {
                    let _ = state_map.cas(idx, SparseBlockState::CLEAN, new_state);
                }
            }
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

        Ok((state_map, dirty_count))
    }
}
