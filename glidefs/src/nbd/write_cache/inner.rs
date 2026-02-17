use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::nbd::block_map::{BlockMap, BlockMapKind, Blake3Hash, SequenceNumber};
use crate::nbd::state::BlockState;
use crate::nbd::wal::Wal;

use super::config::WriteCacheConfig;
use super::error::CacheError;

use bytes::Bytes;

/// Magic bytes for cache metadata file
pub(super) const METADATA_MAGIC: &[u8; 8] = b"ZFSCACHE";
/// Version 3: present_blocks as packed bits (8x smaller than v2)
pub(super) const METADATA_VERSION: u32 = 3;

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

    /// Block states (indexed by block number) - LOCK-FREE
    /// Uses AtomicU8 with CAS for state transitions
    pub(super) block_states: Box<[AtomicU8]>,

    /// Presence bitmap as atomic u64 chunks - LOCK-FREE
    /// Each chunk covers 64 blocks. Uses atomic OR to set bits.
    /// Chunk index = block_num / 64, bit index = block_num % 64
    pub(super) present_chunks: Box<[AtomicU64]>,

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

    /// Dirty block data store: hash -> data.
    /// Pinned in memory until flushed to S3 (Phase 2).
    /// Mutex is effectively uncontended: single writer per export.
    pub(super) dirty_store: Mutex<HashMap<Blake3Hash, Bytes>>,

    /// Write-ahead log for crash recovery.
    /// Mutex is effectively uncontended: single writer per export.
    pub(super) wal: Mutex<Wal>,

    /// Total dirty bytes for budget enforcement.
    /// Incremented when a block transitions Clean→Dirty or Syncing→Dirty.
    /// Decremented in flush_dirty_inner when a block is successfully flushed.
    pub(super) dirty_bytes: AtomicU64,

    /// Flush trigger notification. When dirty bytes exceed the budget,
    /// the write path calls `notify_one()` to wake the flush scheduler.
    pub(super) flush_trigger: Option<Arc<Notify>>,

    /// Export name (used in WAL entries).
    pub(super) export_name: String,

    /// Pre-computed zero-block hash for this export's block_size.
    /// Used by flush, write, and read paths to identify trimmed/unwritten chunks.
    pub(super) zero_block_hash: Blake3Hash,

    /// Pre-computed zero-block bytes for this export's block_size.
    /// Avoids a heap allocation on every sparse read.
    pub(super) zero_block_bytes: Bytes,
}

impl CacheInner {
    /// Check if block is present (lock-free read).
    #[inline]
    pub(super) fn is_present(&self, block_num: usize) -> bool {
        if block_num >= self.num_blocks {
            return false;
        }
        let chunk_idx = block_num / 64;
        let bit_idx = block_num % 64;
        let chunk = self.present_chunks[chunk_idx].load(Ordering::Acquire);
        (chunk & (1u64 << bit_idx)) != 0
    }

    /// Mark block as present (lock-free atomic OR).
    #[inline]
    pub(super) fn set_present(&self, block_num: usize) {
        if block_num >= self.num_blocks {
            return;
        }
        let chunk_idx = block_num / 64;
        let bit_idx = block_num % 64;
        self.present_chunks[chunk_idx].fetch_or(1u64 << bit_idx, Ordering::Release);
    }

    /// CAS loop to transition a block to Dirty state (lock-free).
    ///
    /// Handles three source states:
    /// - **Clean → Dirty**: increments dirty_block_count and dirty_bytes.
    /// - **Syncing → Dirty**: decrements syncing_block_count, increments dirty_block_count and dirty_bytes.
    /// - **Dirty → Dirty**: no-op.
    #[inline]
    pub(super) fn transition_to_dirty(&self, idx: usize) {
        let block_size = self.config.block_size as u64;
        loop {
            let current = self.block_states[idx].load(Ordering::Acquire);

            if current == BlockState::Dirty as u8 {
                break;
            }

            if current == BlockState::Clean as u8 {
                if self.block_states[idx]
                    .compare_exchange(
                        current,
                        BlockState::Dirty as u8,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    self.dirty_bytes.fetch_add(block_size, Ordering::Relaxed);
                    break;
                }
            } else if current == BlockState::Syncing as u8 {
                if self.block_states[idx]
                    .compare_exchange(
                        current,
                        BlockState::Dirty as u8,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                    self.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    self.dirty_bytes.fetch_add(block_size, Ordering::Relaxed);
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
    pub(super) fn block_map_set(&self, chunk_index: usize, hash: Blake3Hash, seq: u64) {
        self.block_map.read().set(chunk_index, hash, seq)
    }

    /// Snapshot the block map (takes read lock).
    pub(super) fn block_map_snapshot(&self) -> BlockMap {
        self.block_map.read().snapshot(&self.block_states)
    }

    /// Count present blocks (for metrics/logging).
    pub(super) fn count_present(&self) -> usize {
        self.present_chunks
            .iter()
            .map(|chunk| chunk.load(Ordering::Relaxed).count_ones() as usize)
            .sum()
    }

    /// Persist block states and presence to metadata file.
    ///
    /// Uses atomic write pattern: write to temp file, fsync, then rename.
    /// This ensures metadata is never corrupted if we crash mid-write.
    pub(super) fn save_metadata(&self) -> Result<(), CacheError> {
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

        // Write block states (1 byte per block) - snapshot atomic values
        let state_bytes: Vec<u8> = self
            .block_states
            .iter()
            .map(|s| s.load(Ordering::Relaxed))
            .collect();
        file.write_all(&state_bytes)?;

        // Write presence bitmap as packed bits (1 bit per block)
        // Convert atomic u64 chunks back to packed bytes
        let mut present_bytes = vec![0u8; self.num_blocks.div_ceil(8)];
        for (chunk_idx, chunk) in self.present_chunks.iter().enumerate() {
            let chunk_val = chunk.load(Ordering::Relaxed);
            let base_byte = chunk_idx * 8;
            for byte_offset in 0..8 {
                let byte_idx = base_byte + byte_offset;
                if byte_idx < present_bytes.len() {
                    present_bytes[byte_idx] = ((chunk_val >> (byte_offset * 8)) & 0xFF) as u8;
                }
            }
        }
        file.write_all(&present_bytes)?;

        // Fsync temp file to ensure data is on disk
        file.sync_all()?;
        drop(file);

        // Atomic rename (POSIX guarantees this is atomic)
        std::fs::rename(&tmp_path, &path)?;

        let present_count = self.count_present();
        debug!(
            path = %path.display(),
            blocks = self.num_blocks,
            present = present_count,
            "saved cache metadata (atomic)"
        );
        Ok(())
    }

    /// Load block states and presence from metadata file.
    ///
    /// Returns (state_bytes, present_chunks, dirty_count) where:
    /// - state_bytes: Raw u8 values for block states (Syncing converted to Dirty)
    /// - present_chunks: Atomic u64 chunks for presence bitmap
    /// - dirty_count: Number of dirty blocks (for counter initialization)
    pub(super) fn load_metadata(
        config: &WriteCacheConfig,
    ) -> Result<(Vec<u8>, Vec<u64>, usize), CacheError> {
        let path = config.metadata_path();
        let num_blocks = config.num_blocks();
        let num_chunks = num_blocks.div_ceil(64);

        if !path.exists() {
            // No metadata file - all blocks are clean and NOT present
            debug!(path = %path.display(), "no metadata file, starting fresh");
            return Ok((
                vec![BlockState::Clean as u8; num_blocks],
                vec![0u64; num_chunks],
                0,
            ));
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

        // Read block states
        let mut state_bytes = vec![0u8; stored_num_blocks];
        file.read_exact(&mut state_bytes)?;

        // Convert Syncing to Dirty (conservative for crash recovery)
        let mut dirty_count = 0;
        for state in &mut state_bytes {
            let parsed = BlockState::from_u8(*state);
            // Syncing blocks had in-flight uploads that may have failed
            if parsed == BlockState::Syncing {
                *state = BlockState::Dirty as u8;
            }
            if *state == BlockState::Dirty as u8 {
                dirty_count += 1;
            }
        }

        // Read presence bitmap and convert to u64 chunks
        let present_chunks: Vec<u64> = if version >= 3 {
            // Version 3: packed bits (1 bit per block)
            let num_bytes = stored_num_blocks.div_ceil(8);
            let mut present_bytes = vec![0u8; num_bytes];
            file.read_exact(&mut present_bytes)?;

            // Convert packed bytes to u64 chunks
            let mut chunks = vec![0u64; num_chunks];
            for (chunk_idx, chunk) in chunks.iter_mut().enumerate() {
                let base_byte = chunk_idx * 8;
                for byte_offset in 0..8 {
                    let byte_idx = base_byte + byte_offset;
                    if byte_idx < present_bytes.len() {
                        *chunk |= (present_bytes[byte_idx] as u64) << (byte_offset * 8);
                    }
                }
            }
            chunks
        } else if version >= 2 {
            // Version 2: 1 byte per block (legacy)
            let mut present_bytes = vec![0u8; stored_num_blocks];
            file.read_exact(&mut present_bytes)?;

            // Convert to u64 chunks
            let mut chunks = vec![0u64; num_chunks];
            for (block_num, &present) in present_bytes.iter().enumerate() {
                if present != 0 {
                    let chunk_idx = block_num / 64;
                    let bit_idx = block_num % 64;
                    chunks[chunk_idx] |= 1u64 << bit_idx;
                }
            }
            chunks
        } else {
            // Version 1 compatibility: dirty blocks are present, clean blocks are NOT
            let mut chunks = vec![0u64; num_chunks];
            for (block_num, &state) in state_bytes.iter().enumerate() {
                if state == BlockState::Dirty as u8 {
                    let chunk_idx = block_num / 64;
                    let bit_idx = block_num % 64;
                    chunks[chunk_idx] |= 1u64 << bit_idx;
                }
            }
            chunks
        };

        // If growing, extend arrays with clean/not-present values
        let (state_bytes, present_chunks) = if is_growing {
            let mut extended_states = state_bytes;
            extended_states.resize(num_blocks, BlockState::Clean as u8);

            let mut extended_chunks = present_chunks;
            extended_chunks.resize(num_chunks, 0u64);

            info!(
                old_blocks = stored_num_blocks,
                new_blocks = num_blocks,
                "Extended arrays for resize"
            );
            (extended_states, extended_chunks)
        } else {
            (state_bytes, present_chunks)
        };

        let present_count: usize = present_chunks.iter().map(|c| c.count_ones() as usize).sum();
        info!(
            path = %path.display(),
            blocks = state_bytes.len(),
            dirty = dirty_count,
            present = present_count,
            "loaded cache metadata"
        );

        Ok((state_bytes, present_chunks, dirty_count))
    }
}
