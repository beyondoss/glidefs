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

use bytes::Bytes;
use crossbeam::queue::SegQueue;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use thiserror::Error;
use tokio::sync::Notify;
use tracing::{debug, error, info, instrument, warn};

use super::block_map::{AtomicBlockMap, BlockMap, BlockMapEntry, BlockMapKind, Blake3Hash, ForkedBlockMap, SequenceNumber, blake3_128, lz4_compress, lz4_decompress, ZERO_BLOCK_HASH};
use super::block_store::{BlockStoreError, S3BlockStore};
use super::cache::BlockCache;
use super::content_store::{ContentStore, ContentStoreError};
use super::manifest::{Manifest, ManifestBlockEntry};
use super::pack::{self, PackLocation, BLOCKS_PER_PACK};
use super::pack_index::HostPackIndex;
use super::state::{Active, BlockState, Draining, Initializing, Recovering};
use super::wal::{Wal, WalEntry};

/// Statistics from a flush operation.
#[derive(Debug, Default)]
pub struct FlushStats {
    /// Total blocks included in the flush snapshot.
    pub blocks_flushed: usize,
    /// Blocks skipped (already in S3 or zero blocks).
    pub blocks_deduped: usize,
    /// Number of pack objects uploaded.
    pub packs_uploaded: usize,
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
    /// Flush statistics.
    pub stats: FlushStats,
}

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
        self.file.read_exact_at(buf, offset)
    }

    /// Write all bytes at a specific offset (pwrite).
    #[inline]
    pub fn write_all_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()> {
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
fn is_zero_block(data: &[u8]) -> bool {
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

/// Magic bytes for cache metadata file
const METADATA_MAGIC: &[u8; 8] = b"ZFSCACHE";
/// Version 3: present_blocks as packed bits (8x smaller than v2)
const METADATA_VERSION: u32 = 3;

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

/// Configuration for the write cache.
#[derive(Clone, Debug)]
pub struct WriteCacheConfig {
    /// Path to the local cache directory
    pub cache_dir: PathBuf,

    /// Device name (used for cache file naming)
    pub device_name: String,

    /// Device size in bytes
    pub device_size: u64,

    /// Block size in bytes
    pub block_size: usize,
}

impl WriteCacheConfig {
    /// Calculate the number of blocks for this device.
    pub fn num_blocks(&self) -> usize {
        self.device_size.div_ceil(self.block_size as u64) as usize
    }

    /// Path to the cache data file.
    pub fn data_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.cache", self.device_name))
    }

    /// Path to the cache metadata file.
    pub fn metadata_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.meta", self.device_name))
    }

    /// Path to the v2 block map persistence file.
    pub fn block_map_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.blockmap", self.device_name))
    }

    /// Path to the v2 WAL file.
    pub fn wal_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.wal", self.device_name))
    }
}

/// Internal state shared across all cache states.
///
/// Uses lock-free atomics for block states and presence to avoid contention
/// under high write concurrency. The data file uses positional I/O which is
/// inherently thread-safe, eliminating all locking on the hot path.
pub(crate) struct CacheInner {
    /// Configuration
    config: WriteCacheConfig,

    /// Local cache file (data) - encrypted at rest
    /// Uses positional I/O (pread/pwrite) which is lock-free and thread-safe
    data_file: SyncFile,

    /// Block states (indexed by block number) - LOCK-FREE
    /// Uses AtomicU8 with CAS for state transitions
    block_states: Box<[AtomicU8]>,

    /// Presence bitmap as atomic u64 chunks - LOCK-FREE
    /// Each chunk covers 64 blocks. Uses atomic OR to set bits.
    /// Chunk index = block_num / 64, bit index = block_num % 64
    present_chunks: Box<[AtomicU64]>,

    /// Number of blocks (for bounds checking)
    num_blocks: usize,

    /// Statistics
    dirty_block_count: AtomicU64,
    syncing_block_count: AtomicU64,

    /// Queue of dirty block numbers for O(1) claiming (no scanning).
    /// Blocks are pushed here when transitioning to Dirty state.
    /// The sync worker pops from this queue instead of scanning all blocks.
    dirty_queue: SegQueue<u64>,

    /// Notification for waking the sync worker when dirty blocks are available.
    /// The sync worker waits on this instead of polling.
    dirty_notify: Notify,

    // === v2 content-addressed structures ===

    /// Content-addressed block map: chunk_index -> (Blake3Hash, sequence).
    ///
    /// Wrapped in RwLock<BlockMapKind> to support both full (AtomicBlockMap)
    /// and forked (ForkedBlockMap with DashMap overlay) variants.
    /// All normal reads/writes take a read lock (interior mutability handles
    /// the actual mutation). Write lock is only taken during rare flatten ops.
    block_map: RwLock<BlockMapKind>,

    /// Monotonic sequence counter for snapshot consistency.
    /// Lock-free AtomicU64.
    sequence: SequenceNumber,

    /// Dirty block data store: hash -> data.
    /// Pinned in memory until flushed to S3 (Phase 2).
    /// Mutex is effectively uncontended: single writer per export.
    dirty_store: Mutex<HashMap<Blake3Hash, Bytes>>,

    /// Write-ahead log for crash recovery.
    /// Mutex is effectively uncontended: single writer per export.
    wal: Mutex<Wal>,

    /// Total dirty bytes (for budget enforcement in Phase 6).
    dirty_bytes: AtomicU64,

    /// Export name (used in WAL entries).
    export_name: String,
}

impl CacheInner {
    /// Check if block is present (lock-free read).
    #[inline]
    fn is_present(&self, block_num: usize) -> bool {
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
    fn set_present(&self, block_num: usize) {
        if block_num >= self.num_blocks {
            return;
        }
        let chunk_idx = block_num / 64;
        let bit_idx = block_num % 64;
        self.present_chunks[chunk_idx].fetch_or(1u64 << bit_idx, Ordering::Release);
    }

    /// Get a block map entry (takes read lock, effectively zero overhead).
    #[inline]
    fn block_map_get(&self, chunk_index: usize) -> (Blake3Hash, u64) {
        self.block_map.read().unwrap().get(chunk_index)
    }

    /// Set a block map entry (takes read lock — interior mutability handles the write).
    #[inline]
    fn block_map_set(&self, chunk_index: usize, hash: Blake3Hash, seq: u64) {
        self.block_map.read().unwrap().set(chunk_index, hash, seq)
    }

    /// Snapshot the block map (takes read lock).
    fn block_map_snapshot(&self) -> BlockMap {
        self.block_map.read().unwrap().snapshot(&self.block_states)
    }

    /// Count present blocks (for metrics/logging).
    fn count_present(&self) -> usize {
        self.present_chunks
            .iter()
            .map(|chunk| chunk.load(Ordering::Relaxed).count_ones() as usize)
            .sum()
    }

    /// Persist block states and presence to metadata file.
    ///
    /// Uses atomic write pattern: write to temp file, fsync, then rename.
    /// This ensures metadata is never corrupted if we crash mid-write.
    fn save_metadata(&self) -> Result<(), CacheError> {
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
    fn load_metadata(
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

/// Write-behind cache with typestate lifecycle management.
///
/// The cache progresses through states:
/// - `Initializing`: Loading local cache
/// - `Recovering`: Syncing dirty blocks from previous session
/// - `Active`: Serving I/O, only state where read/write/flush are allowed
/// - `Draining`: Syncing all blocks before shutdown
pub struct WriteCache<S> {
    inner: Arc<CacheInner>,
    _state: PhantomData<S>,
}

impl WriteCache<Initializing> {
    /// Open or create a write cache.
    ///
    /// Returns a cache in `Recovering` state. Call `finish_recovery()` to
    /// transition to `Active` state before serving I/O.
    ///
    /// # Arguments
    /// * `config` - Cache configuration
    #[instrument(skip(config), fields(device = %config.device_name))]
    pub fn open(config: WriteCacheConfig) -> Result<WriteCache<Recovering>, CacheError> {
        // Ensure cache directory exists
        std::fs::create_dir_all(&config.cache_dir)?;

        // Open or create data file with O_DIRECT for sync worker reads
        // This prevents sync worker from polluting/contending with the page cache
        let data_path = config.data_path();
        let data_file = SyncFile::open(&data_path, true, config.device_size)?;

        // Load block states and presence (or create fresh)
        let num_blocks = config.num_blocks();
        let (state_bytes, present_chunk_vals, dirty_count) = CacheInner::load_metadata(&config)?;

        // Convert to atomic types
        let block_states: Box<[AtomicU8]> = state_bytes
            .into_iter()
            .map(AtomicU8::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let present_chunks: Box<[AtomicU64]> = present_chunk_vals
            .into_iter()
            .map(AtomicU64::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let present_count: usize = present_chunks
            .iter()
            .map(|c| c.load(Ordering::Relaxed).count_ones() as usize)
            .sum();

        // Pre-populate dirty queue with blocks that are dirty from recovery.
        // This avoids needing to scan on first sync cycle.
        let dirty_queue = SegQueue::new();
        for (idx, state) in block_states.iter().enumerate() {
            if state.load(Ordering::Relaxed) == BlockState::Dirty as u8 {
                dirty_queue.push(idx as u64);
            }
        }

        // === v2 initialization ===
        let block_map_path = config.block_map_path();
        let wal_path = config.wal_path();
        let export_name = config.device_name.clone();
        let chunk_size = config.block_size as u32;

        // Load persisted block map (or create empty)
        let mut persisted_bm = if block_map_path.exists() {
            match BlockMap::load_from_file(&block_map_path) {
                Ok(bm) => {
                    info!(path = %block_map_path.display(), entries = bm.non_empty_count(), "loaded v2 block map");
                    bm
                }
                Err(e) => {
                    warn!(error = %e, "failed to load v2 block map, starting fresh");
                    BlockMap::new(config.device_size, chunk_size)
                }
            }
        } else {
            BlockMap::new(config.device_size, chunk_size)
        };

        let persisted_max_seq = persisted_bm.max_sequence();

        // Replay WAL entries after persisted sequence
        let wal_entries = match Wal::replay(&wal_path, persisted_max_seq) {
            Ok(entries) => {
                if !entries.is_empty() {
                    info!(entries = entries.len(), min_seq = persisted_max_seq + 1, "replaying WAL entries");
                }
                entries
            }
            Err(e) => {
                warn!(error = %e, "WAL replay failed, continuing with persisted block map");
                vec![]
            }
        };

        // Apply WAL entries to block map and build dirty store
        let mut dirty_store_map: HashMap<Blake3Hash, Bytes> = HashMap::new();
        let mut max_wal_seq = persisted_max_seq;

        for entry in &wal_entries {
            if entry.name != export_name {
                continue; // Skip entries for other exports (shouldn't happen with per-export WAL)
            }
            let chunk_index = entry.chunk_index as usize;
            persisted_bm.set(chunk_index, BlockMapEntry {
                hash: entry.hash,
                flags: BlockMapEntry::FLAG_DIRTY,
                sequence: entry.sequence,
            });
            if !entry.data.is_empty() {
                dirty_store_map.insert(entry.hash, entry.data.clone());
            }
            max_wal_seq = max_wal_seq.max(entry.sequence);
        }

        // Build AtomicBlockMap from the merged block map
        let atomic_block_map = AtomicBlockMap::from_block_map(&persisted_bm);

        // Initialize sequence counter
        let initial_seq = persisted_max_seq.max(max_wal_seq);
        let sequence = SequenceNumber::new(initial_seq);

        // Open WAL for new appends
        let wal = match Wal::open(&wal_path) {
            Ok(w) => w,
            Err(e) => {
                warn!(error = %e, "failed to open WAL, creating new");
                Wal::open(&wal_path).map_err(CacheError::Io)?
            }
        };

        let dirty_bytes_count: u64 = dirty_store_map.len() as u64 * chunk_size as u64;

        let inner = Arc::new(CacheInner {
            config,
            data_file,
            block_states,
            present_chunks,
            num_blocks,
            dirty_block_count: AtomicU64::new(dirty_count as u64),
            syncing_block_count: AtomicU64::new(0),
            dirty_queue,
            dirty_notify: Notify::new(),
            // v2 fields
            block_map: RwLock::new(BlockMapKind::Full(atomic_block_map)),
            sequence,
            dirty_store: Mutex::new(dirty_store_map),
            wal: Mutex::new(wal),
            dirty_bytes: AtomicU64::new(dirty_bytes_count),
            export_name,
        });

        info!(
            dirty_blocks = dirty_count,
            present_blocks = present_count,
            "cache opened, transitioning to Recovering"
        );

        Ok(WriteCache {
            inner,
            _state: PhantomData,
        })
    }

    /// Create a write cache from a manifest (for forked VMs).
    ///
    /// Unlike `open()`, this skips WAL replay and local metadata loading.
    /// All block data is in S3, so the local cache starts empty.
    /// Goes directly to Active state (nothing to recover).
    pub fn open_from_manifest(
        config: WriteCacheConfig,
        manifest: &Manifest,
        parent_block_map: Option<Arc<BlockMap>>,
    ) -> Result<WriteCache<Active>, CacheError> {
        std::fs::create_dir_all(&config.cache_dir)?;

        let data_file = SyncFile::open(&config.data_path(), true, config.device_size)?;

        let num_blocks = config.num_blocks();
        let num_chunks = num_blocks.div_ceil(64);

        // Fresh block states: all Clean (data is in S3, not local)
        let block_states: Box<[AtomicU8]> = (0..num_blocks)
            .map(|_| AtomicU8::new(BlockState::Clean as u8))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        // Fresh presence bitmap: nothing present locally
        let present_chunks: Box<[AtomicU64]> = (0..num_chunks)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        // Empty dirty queue: no local dirty data
        let dirty_queue = SegQueue::new();

        // Build BlockMapKind from manifest or parent
        let block_map_kind = if let Some(parent) = parent_block_map {
            BlockMapKind::Forked(ForkedBlockMap::new(parent))
        } else {
            let mut bm = BlockMap::new(config.device_size, config.block_size as u32);
            for entry in &manifest.block_map {
                bm.set(
                    entry.chunk_index as usize,
                    BlockMapEntry {
                        hash: entry.hash,
                        flags: 0,
                        sequence: manifest.sequence,
                    },
                );
            }
            BlockMapKind::Full(AtomicBlockMap::from_block_map(&bm))
        };

        let sequence = SequenceNumber::new(manifest.sequence);
        let wal = Wal::open(&config.wal_path())?;
        let export_name = config.device_name.clone();

        let inner = Arc::new(CacheInner {
            config,
            data_file,
            block_states,
            present_chunks,
            num_blocks,
            dirty_block_count: AtomicU64::new(0),
            syncing_block_count: AtomicU64::new(0),
            dirty_queue,
            dirty_notify: Notify::new(),
            block_map: RwLock::new(block_map_kind),
            sequence,
            dirty_store: Mutex::new(HashMap::new()),
            wal: Mutex::new(wal),
            dirty_bytes: AtomicU64::new(0),
            export_name,
        });

        info!("cache opened from manifest, directly Active");

        Ok(WriteCache {
            inner,
            _state: PhantomData,
        })
    }
}

impl WriteCache<Recovering> {
    /// Skip recovery and transition directly to Active state.
    ///
    /// **TEST ONLY**: This bypasses recovery for unit tests that don't need S3.
    #[allow(dead_code)] // Used by integration tests and benchmarks
    #[cfg(any(test, feature = "test-utils"))]
    pub fn skip_recovery_for_test(self) -> WriteCache<Active> {
        WriteCache {
            inner: self.inner,
            _state: PhantomData,
        }
    }

    /// Sync all dirty blocks from previous session and transition to Active.
    ///
    /// This uploads any blocks that were dirty when the cache was last closed
    /// (or when the process crashed).
    #[instrument(skip(self, s3))]
    pub async fn finish_recovery(
        self,
        s3: &S3BlockStore,
    ) -> Result<WriteCache<Active>, CacheError> {
        let dirty_count = self.inner.dirty_block_count.load(Ordering::Relaxed);

        if dirty_count == 0 {
            info!("no dirty blocks, recovery complete");
        } else {
            info!(dirty_blocks = dirty_count, "starting recovery sync");

            // Collect and sync dirty blocks using batched writes
            let dirty_blocks = self.collect_dirty_blocks();
            match self.sync_blocks_batched(s3, dirty_blocks).await {
                Ok(synced) => {
                    info!(synced = synced, "recovery sync complete");
                }
                Err(e) => {
                    warn!(error = %e, "recovery sync failed, will retry on next startup");
                }
            }

            // Save metadata after recovery
            self.inner.save_metadata()?;
            info!("recovery complete");
        }

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }

    fn collect_dirty_blocks(&self) -> Vec<u64> {
        self.inner
            .block_states
            .iter()
            .enumerate()
            .filter(|(_, s)| s.load(Ordering::Relaxed) == BlockState::Dirty as u8)
            .map(|(i, _)| i as u64)
            .collect()
    }

    /// Sync dirty blocks to S3 using batch writes with conditional PUT.
    async fn sync_blocks_batched(&self, s3: &S3BlockStore, block_nums: Vec<u64>) -> Result<usize, CacheError> {
        use std::collections::HashMap;

        if block_nums.is_empty() {
            return Ok(0);
        }

        // Group blocks by batch number
        let mut batches: HashMap<u64, Vec<u64>> = HashMap::new();
        for block_num in block_nums {
            let batch_num = s3.batch_num(block_num);
            batches.entry(batch_num).or_default().push(block_num);
        }

        let mut synced_count = 0;

        // Process each batch
        for (batch_num, blocks_in_batch) in batches {
            // GET existing batch with ETag for conditional PUT
            let batch_result = s3.get_batch_with_etag(batch_num).await?;
            let mut batch_data = batch_result.data;
            let etag = batch_result.etag;

            // Update dirty block slots with local data
            for &block_num in &blocks_in_batch {
                let local_data = self.read_local_block(block_num)?;
                let offset = s3.offset_in_batch(block_num) as usize;
                batch_data[offset..offset + local_data.len()].copy_from_slice(&local_data);
            }

            // Conditional PUT: only succeed if no one else modified the batch
            s3.put_batch_conditional(batch_num, batch_data, etag).await?;

            // Mark all blocks in this batch as synced
            for block_num in blocks_in_batch {
                self.mark_synced(block_num);
                synced_count += 1;
            }
        }

        Ok(synced_count)
    }

    fn read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let offset = block_num * block_size as u64;

        // Calculate valid bytes for this block (handles partial last block)
        let valid_bytes = if offset >= device_size {
            return Ok(Bytes::from(vec![0u8; block_size]));
        } else {
            std::cmp::min(block_size as u64, device_size - offset) as usize
        };

        let mut buf = vec![0u8; block_size];
        if valid_bytes == block_size {
            // Use sync FD to avoid contention with NBD reads during recovery
            self.inner.data_file.read_exact_at(&mut buf, offset)?;
        } else {
            // Partial block (last block) - read only valid bytes, rest stays zero
            self.inner
                .data_file
                .read_exact_at(&mut buf[..valid_bytes], offset)?;
        }

        Ok(Bytes::from(buf))
    }

    fn mark_synced(&self, block_num: u64) {
        let idx = block_num as usize;
        if idx >= self.inner.num_blocks {
            return;
        }

        // CAS loop: Dirty|Syncing -> Clean
        loop {
            let current = self.inner.block_states[idx].load(Ordering::Acquire);
            if current != BlockState::Dirty as u8 && current != BlockState::Syncing as u8 {
                break;
            }

            if self.inner.block_states[idx]
                .compare_exchange(
                    current,
                    BlockState::Clean as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.inner.dirty_block_count.fetch_sub(1, Ordering::Relaxed);
                break;
            }
            // CAS failed, retry
        }
    }
}

impl WriteCache<Active> {
    /// Write data to the cache.
    ///
    /// Data is written to the local SSD and the affected blocks are marked dirty and present.
    /// The write returns immediately after local I/O completes.
    ///
    /// # Lock-Free State Updates
    ///
    /// Uses CAS operations for state transitions:
    /// - Clean → Dirty: increment dirty_count, push to queue, notify
    /// - Syncing → Dirty: decrement syncing_count, increment dirty_count, push to queue, notify
    /// - Dirty → Dirty: no-op
    #[instrument(skip(self, data), fields(offset = offset, len = data.len()))]
    pub fn write(&self, offset: u64, data: &[u8]) -> Result<(), CacheError> {
        if offset + data.len() as u64 > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + data.len() as u64,
                self.inner.config.device_size,
            ));
        }

        if data.is_empty() {
            return Ok(());
        }

        // Calculate affected blocks
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + data.len() as u64 - 1) / block_size;

        // CRITICAL: Mark blocks as present BEFORE writing to file.
        // This prevents a race with prefetch where:
        // 1. Prefetch sees is_present=false
        // 2. Write does pwrite(new_data)
        // 3. Prefetch does pwrite(s3_data) - OVERWRITES new_data
        // 4. Write does set_present (too late)
        //
        // By setting present first, prefetch's CAS will fail if we've claimed the block,
        // or if prefetch wins the CAS, our pwrite will overwrite their stale S3 data.
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.set_present(idx);
            }
        }

        // Now write to local file (after claiming blocks via set_present)
        self.inner.data_file.write_all_at(data, offset)?;

        // Mark affected blocks as dirty (lock-free)
        let mut newly_dirty = false;

        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }

            // Block is already present (set above)

            // CAS loop for state transition
            loop {
                let current = self.inner.block_states[idx].load(Ordering::Acquire);

                if current == BlockState::Dirty as u8 {
                    // Already dirty, nothing to do (already in queue)
                    break;
                }

                if current == BlockState::Clean as u8 {
                    // Clean → Dirty
                    if self.inner.block_states[idx]
                        .compare_exchange(
                            current,
                            BlockState::Dirty as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                        self.inner.dirty_queue.push(block);
                        newly_dirty = true;
                        break;
                    }
                    // CAS failed, retry
                } else if current == BlockState::Syncing as u8 {
                    // Syncing → Dirty (write during sync)
                    if self.inner.block_states[idx]
                        .compare_exchange(
                            current,
                            BlockState::Dirty as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                        self.inner.dirty_queue.push(block);
                        newly_dirty = true;
                        break;
                    }
                    // CAS failed, retry
                } else {
                    // Unknown state, just break
                    break;
                }
            }
        }

        // === v2: content-addressed updates ===
        // For each affected chunk, read the full chunk from SSD, hash it,
        // and update block map + WAL + dirty store.
        {
            let block_size = self.inner.config.block_size;
            let device_size = self.inner.config.device_size;

            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }

                // Read the full chunk from SSD (to get correct merged data for sub-chunk writes)
                let chunk_offset = block * block_size as u64;
                let valid_bytes = std::cmp::min(block_size as u64, device_size.saturating_sub(chunk_offset)) as usize;
                let mut chunk_buf = vec![0u8; block_size];
                if valid_bytes > 0 {
                    self.inner.data_file.read_exact_at(&mut chunk_buf[..valid_bytes], chunk_offset)?;
                }

                let hash = blake3_128(&chunk_buf);
                let seq = self.inner.sequence.next();

                // Update atomic block map (lock-free)
                self.inner.block_map_set(idx, hash, seq);

                // WAL append (Mutex, uncontended)
                let wal_entry = WalEntry {
                    name: self.inner.export_name.clone(),
                    chunk_index: block,
                    hash,
                    sequence: seq,
                    data: Bytes::copy_from_slice(&chunk_buf),
                };
                if let Err(e) = self.inner.wal.lock().unwrap().append(&wal_entry) {
                    error!(block = block, error = %e, "WAL append failed");
                    // Non-fatal: v1 SSD file still has the data
                }

                // Dirty store insert (Mutex, uncontended)
                self.inner.dirty_store.lock().unwrap().insert(hash, Bytes::from(chunk_buf));

                // Track dirty bytes
                self.inner.dirty_bytes.fetch_add(block_size as u64, Ordering::Relaxed);
            }

            // Flush WAL buffer
            if let Err(e) = self.inner.wal.lock().unwrap().flush_buf() {
                error!(error = %e, "WAL flush failed");
            }
        }

        // Wake sync worker if we added dirty blocks
        if newly_dirty {
            self.inner.dirty_notify.notify_one();
        }

        // If using a forked block map, check if overlay is large enough to flatten
        self.try_flatten_block_map();

        debug!(start_block = start_block, end_block = end_block, "marked blocks dirty and present");
        Ok(())
    }

    /// Read data from the cache, fetching from S3 if blocks are not present locally.
    ///
    /// This is the primary read path for NBD I/O. Blocks that haven't been written
    /// locally are fetched from S3 on demand (read-through caching).
    #[instrument(skip(self, s3, metrics), fields(offset = offset, len = len))]
    pub async fn read_with_fetch(
        &self,
        offset: u64,
        len: usize,
        s3: &S3BlockStore,
        metrics: &super::metrics::ExportMetrics,
    ) -> Result<Bytes, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len as u64 - 1) / block_size;

        // Check which blocks need to be fetched from S3 (lock-free)
        let blocks_to_fetch: Vec<u64> = (start_block..=end_block)
            .filter(|&block| !self.inner.is_present(block as usize))
            .collect();

        // Record cache hits/misses
        let total_blocks = (end_block - start_block + 1) as usize;
        let cache_misses = blocks_to_fetch.len();
        let cache_hits = total_blocks - cache_misses;
        for _ in 0..cache_hits {
            metrics.record_cache_hit();
        }
        for _ in 0..cache_misses {
            metrics.record_cache_miss();
        }

        // Fetch missing blocks from S3 using batch prefetching
        // Groups blocks by S3 batch to reduce round-trips
        if !blocks_to_fetch.is_empty() {
            let s3_start = Instant::now();
            self.fetch_blocks_batched(s3, blocks_to_fetch, metrics).await?;
            metrics.record_s3_fetch_latency(s3_start.elapsed());
        }

        // Now read from local cache
        let file_read_start = Instant::now();
        let result = self.read_local(offset, len);
        metrics.record_file_read_latency(file_read_start.elapsed());
        result
    }

    /// v2 read path: resolve blocks by content hash through tiered storage.
    ///
    /// Resolution order: block_map → dirty_store → clean_cache → S3 pack fetch.
    /// On S3 cache miss, the entire pack (~25 blocks) is fetched and all blocks
    /// are decompressed, verified, and inserted into the clean cache.
    #[instrument(skip(self, clean_cache, pack_index, content_store, _metrics), fields(offset = offset, len = len))]
    pub async fn read_v2(
        &self,
        offset: u64,
        len: usize,
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
        _metrics: &super::metrics::ExportMetrics,
    ) -> Result<Bytes, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        let chunk_size = self.inner.config.block_size as u64;
        let start_chunk = offset / chunk_size;
        let end_chunk = (offset + len as u64 - 1) / chunk_size;

        let mut result = Vec::with_capacity(len);

        for chunk_idx in start_chunk..=end_chunk {
            let chunk_data = self
                .resolve_chunk(chunk_idx as usize, clean_cache, pack_index, content_store)
                .await?;

            // Slice out the portion of this chunk that overlaps the requested range.
            let chunk_start_byte = chunk_idx * chunk_size;
            let slice_start = if chunk_idx == start_chunk {
                (offset - chunk_start_byte) as usize
            } else {
                0
            };
            let slice_end = if chunk_idx == end_chunk {
                let end_byte = offset + len as u64;
                let relative_end = (end_byte - chunk_start_byte) as usize;
                std::cmp::min(relative_end, chunk_data.len())
            } else {
                chunk_data.len()
            };

            result.extend_from_slice(&chunk_data[slice_start..slice_end]);
        }

        debug!(chunks = end_chunk - start_chunk + 1, "v2 read complete");
        Ok(Bytes::from(result))
    }

    /// Resolve a single chunk through the v2 tier hierarchy.
    ///
    /// 1. block_map lookup → if zero hash, return zeros
    /// 2. dirty_store → in-memory dirty block (~100ns)
    /// 3. clean_cache → previously fetched and decompressed block (~100ns)
    /// 4. S3 pack fetch → fetch entire pack, decompress all blocks, cache them
    async fn resolve_chunk(
        &self,
        chunk_index: usize,
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
    ) -> Result<Bytes, CacheError> {
        let chunk_size = self.inner.config.block_size;
        let (hash, _seq) = self.inner.block_map_get(chunk_index);

        // Never written or trimmed → zeros.
        if hash.is_zero() || hash == *ZERO_BLOCK_HASH {
            return Ok(Bytes::from(vec![0u8; chunk_size]));
        }

        // Tier 1: dirty_store (in-memory, not yet flushed to S3).
        if let Some(data) = self.inner.dirty_store.lock().unwrap().get(&hash) {
            return Ok(data.clone());
        }

        // Tier 2: clean_cache (previously fetched from S3).
        if let Some(data) = clean_cache.get(&hash).await {
            return Ok(data);
        }

        // Tier 3: S3 pack fetch — find the pack, fetch it, ingest all blocks.
        let pack_loc = pack_index.get(&hash).ok_or(CacheError::BlockNotFound { hash })?;

        let pack_data = content_store.get_pack(pack_loc.pack_id).await?;
        let pack_idx = pack::parse_pack_index(&pack_data)
            .map_err(|e| CacheError::PackFormat(e.to_string()))?;

        for entry in &pack_idx.entries {
            let compressed = pack::extract_block(&pack_data, entry.offset, entry.comp_length)
                .ok_or_else(|| {
                    CacheError::PackFormat(format!(
                        "block at offset {} length {} out of bounds in pack {}",
                        entry.offset, entry.comp_length, pack_loc.pack_id
                    ))
                })?;

            let decompressed = lz4_decompress(compressed)
                .map_err(|e| CacheError::DecompressFailed(e.to_string()))?;

            // Verify hash.
            let actual_hash = blake3_128(&decompressed);
            if actual_hash != entry.hash {
                return Err(CacheError::HashMismatch {
                    expected: format!("{:?}", entry.hash),
                });
            }

            clean_cache.insert(entry.hash, Bytes::from(decompressed));
        }

        // The requested block should now be in the cache.
        clean_cache.get(&hash).await.ok_or(CacheError::BlockNotFound { hash })
    }

    /// Fetch multiple blocks from S3, grouping by batch to reduce round-trips.
    ///
    /// When multiple blocks need fetching, this method groups them by S3 batch
    /// and fetches each batch once. For example, if blocks 0, 5, and 8 all belong
    /// to batch 0 (with 100 blocks per batch), we fetch the entire batch once
    /// and extract all three blocks.
    ///
    /// This is much faster than individual block fetches for sequential reads
    /// or reads that span multiple blocks in the same batch.
    #[instrument(skip(self, s3, metrics), fields(blocks = blocks.len()))]
    async fn fetch_blocks_batched(
        &self,
        s3: &S3BlockStore,
        blocks: Vec<u64>,
        metrics: &super::metrics::ExportMetrics,
    ) -> Result<(), CacheError> {
        use std::collections::HashMap;

        if blocks.is_empty() {
            return Ok(());
        }

        let block_size = self.inner.config.block_size;

        // Group blocks by S3 batch number
        let mut blocks_by_batch: HashMap<u64, Vec<u64>> = HashMap::new();
        for block_num in blocks {
            let batch_num = s3.batch_num(block_num);
            blocks_by_batch.entry(batch_num).or_default().push(block_num);
        }

        let num_batches = blocks_by_batch.len();
        debug!(
            batches = num_batches,
            "fetching blocks grouped by S3 batch"
        );

        // Fetch each batch and extract needed blocks
        for (batch_num, _block_nums) in blocks_by_batch {
            // Fetch the entire batch from S3
            // Note: get_batch_with_etag returns zeros (not error) if batch doesn't exist
            let batch_result = match s3.get_batch_with_etag(batch_num).await {
                Ok(r) => r,
                Err(e) => {
                    error!(batch = batch_num, error = %e, "S3 batch fetch failed");
                    return Err(e.into());
                }
            };
            metrics.record_s3_read(batch_result.data.len() as u64);
            let batch_data = batch_result.data;

            // Cache ALL blocks from the batch (not just requested ones)
            // This maximizes cache hits for sequential reads
            let blocks_per_batch = s3.blocks_per_batch();
            let first_block_in_batch = batch_num * blocks_per_batch;
            let mut blocks_cached = 0usize;

            for i in 0..blocks_per_batch {
                let block_num = first_block_in_batch + i;

                // Skip blocks past device end
                if block_num as usize >= self.inner.num_blocks {
                    break;
                }

                // Skip blocks already present
                if self.inner.is_present(block_num as usize) {
                    continue;
                }

                let offset_in_batch = (i as usize) * block_size;
                let cache_offset = block_num * block_size as u64;

                // Extract block data from batch (with bounds checking)
                let block_data = if offset_in_batch + block_size <= batch_data.len() {
                    &batch_data[offset_in_batch..offset_in_batch + block_size]
                } else {
                    // Partial block at end of batch - use zeros for remainder
                    break;
                };

                // Try to atomically claim this block for prefetching.
                // Use CAS on present bit: if someone else set it while we were
                // fetching from S3, they won the race and we skip this block.
                let chunk_idx = (block_num as usize) / 64;
                let bit_idx = (block_num as usize) % 64;
                let bit_mask = 1u64 << bit_idx;

                // CAS loop to set present bit atomically
                let was_present = loop {
                    let old = self.inner.present_chunks[chunk_idx].load(Ordering::Acquire);
                    if (old & bit_mask) != 0 {
                        // Someone else set it - they won the race
                        break true;
                    }
                    // Try to set the bit
                    match self.inner.present_chunks[chunk_idx].compare_exchange(
                        old,
                        old | bit_mask,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break false, // We set it
                        Err(_) => continue,   // Retry
                    }
                };

                if was_present {
                    // A concurrent write won the race - skip this block
                    // The write's data is authoritative
                    continue;
                }

                // We own this block now (we set present). Write S3 data to cache.
                if let Err(e) = self.inner.data_file.write_all_at(block_data, cache_offset) {
                    error!(block = block_num, offset = cache_offset, error = %e, "Failed to write S3 data to cache file");
                    return Err(e.into());
                }

                blocks_cached += 1;
            }

            debug!(
                batch = batch_num,
                blocks_cached = blocks_cached,
                "cached all blocks from S3 batch"
            );
        }

        Ok(())
    }

    /// Read data from local cache only (no S3 fetch).
    ///
    /// Used internally and by sync worker. Caller must ensure blocks are present.
    #[instrument(skip(self), fields(offset = offset, len = len))]
    pub fn read_local(&self, offset: u64, len: usize) -> Result<Bytes, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        let mut buf = vec![0u8; len];
        self.inner.data_file.read_exact_at(&mut buf, offset)?;

        Ok(Bytes::from(buf))
    }

    /// Legacy read method - reads from local cache only.
    ///
    /// **WARNING**: Returns zeros for blocks not present locally.
    /// Use `read_with_fetch` for NBD I/O to get proper S3 read-through.
    #[allow(dead_code)] // Used by tests
    #[instrument(skip(self), fields(offset = offset, len = len))]
    pub fn read(&self, offset: u64, len: usize) -> Result<Bytes, CacheError> {
        self.read_local(offset, len)
    }

    /// Flush the local cache file.
    ///
    /// This performs an fsync on the local SSD, which is fast (<10ms).
    /// It does NOT wait for S3 sync - that happens in the background.
    #[instrument(skip(self))]
    pub fn flush(&self) -> Result<(), CacheError> {
        self.inner.data_file.sync_all()?;
        debug!("local flush complete");
        Ok(())
    }

    /// Write zeros to a range efficiently.
    ///
    /// On Linux, uses `fallocate(FALLOC_FL_ZERO_RANGE)` to zero the range
    /// without actually writing data - the kernel marks the range as zeros.
    /// This is much faster for large ranges.
    ///
    /// On other platforms, falls back to writing a static zero buffer.
    pub fn zero_range(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        if len == 0 {
            return Ok(());
        }

        if offset + len > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len,
                self.inner.config.device_size,
            ));
        }

        // Zero the file range
        #[cfg(target_os = "linux")]
        {
            let fd = self.inner.data_file.as_raw_fd();

            // FALLOC_FL_ZERO_RANGE = 0x10
            // This zeros the range without deallocating - keeps the file contiguous
            const FALLOC_FL_ZERO_RANGE: libc::c_int = 0x10;

            let ret = unsafe {
                libc::fallocate(fd, FALLOC_FL_ZERO_RANGE, offset as libc::off_t, len as libc::off_t)
            };

            if ret != 0 {
                let err = std::io::Error::last_os_error();
                // If fallocate isn't supported, fall back to writing zeros
                if err.raw_os_error() == Some(libc::EOPNOTSUPP)
                    || err.raw_os_error() == Some(libc::ENOTSUP)
                {
                    return self.zero_range_fallback(offset, len);
                }
                return Err(CacheError::Io(err));
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.zero_range_fallback(offset, len)?;
        }

        // Mark affected blocks as dirty and present
        self.mark_range_dirty_and_present(offset, len);

        // === v2: content-addressed updates for zeroed chunks ===
        {
            let block_size = self.inner.config.block_size as u64;
            let start_block = offset / block_size;
            let end_block = (offset + len - 1) / block_size;
            let zero_hash = *ZERO_BLOCK_HASH;

            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }

                let seq = self.inner.sequence.next();

                // Update atomic block map with zero-block hash
                self.inner.block_map_set(idx, zero_hash, seq);

                // WAL entry with empty data (TRIM marker)
                let wal_entry = WalEntry {
                    name: self.inner.export_name.clone(),
                    chunk_index: block,
                    hash: zero_hash,
                    sequence: seq,
                    data: Bytes::new(),
                };
                if let Err(e) = self.inner.wal.lock().unwrap().append(&wal_entry) {
                    error!(block = block, error = %e, "WAL append failed for zero range");
                }
                // No dirty store insert for zero blocks -- read path returns zeros
            }

            // Flush WAL buffer
            if let Err(e) = self.inner.wal.lock().unwrap().flush_buf() {
                error!(error = %e, "WAL flush failed");
            }
        }

        // If using a forked block map, check if overlay is large enough to flatten
        self.try_flatten_block_map();

        Ok(())
    }

    /// Fallback zero writing using a static buffer.
    /// Used on non-Linux platforms or when fallocate isn't supported.
    fn zero_range_fallback(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        use std::sync::LazyLock;

        // Static zero buffer - allocated once, reused forever
        const ZERO_CHUNK_SIZE: usize = 128 * 1024; // 128KB
        static ZERO_CHUNK: LazyLock<Box<[u8]>> = LazyLock::new(|| {
            vec![0u8; ZERO_CHUNK_SIZE].into_boxed_slice()
        });

        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let chunk_size = (remaining as usize).min(ZERO_CHUNK_SIZE);
            self.inner.data_file.write_all_at(&ZERO_CHUNK[..chunk_size], current_offset)?;
            remaining -= chunk_size as u64;
            current_offset += chunk_size as u64;
        }

        Ok(())
    }

    /// Mark a range of blocks as dirty and present (lock-free).
    fn mark_range_dirty_and_present(&self, offset: u64, len: u64) {
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;

        let mut newly_dirty = false;

        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }

            // Mark as present (atomic OR)
            self.inner.set_present(idx);

            // CAS loop for state transition (same as write())
            loop {
                let current = self.inner.block_states[idx].load(Ordering::Acquire);

                if current == BlockState::Dirty as u8 {
                    break;
                }

                if current == BlockState::Clean as u8 {
                    if self.inner.block_states[idx]
                        .compare_exchange(
                            current,
                            BlockState::Dirty as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                        self.inner.dirty_queue.push(block);
                        newly_dirty = true;
                        break;
                    }
                } else if current == BlockState::Syncing as u8 {
                    if self.inner.block_states[idx]
                        .compare_exchange(
                            current,
                            BlockState::Dirty as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                        self.inner.dirty_queue.push(block);
                        newly_dirty = true;
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        // Wake sync worker if we added dirty blocks
        if newly_dirty {
            self.inner.dirty_notify.notify_one();
        }
    }

    /// Claim dirty blocks for syncing (lock-free, O(1) per block).
    ///
    /// Pops block numbers from the dirty queue and transitions them from Dirty
    /// to Syncing. The sync worker should call this, upload the blocks, then call
    /// `mark_synced` or `mark_sync_failed` for each block.
    ///
    /// Uses CAS to atomically transition Dirty → Syncing. If a block is no longer
    /// Dirty (e.g., already synced or transitioned by another thread), it is skipped.
    #[instrument(skip(self))]
    pub fn claim_dirty_blocks(&self, max_blocks: usize) -> Vec<u64> {
        let mut claimed = Vec::with_capacity(max_blocks);

        while claimed.len() < max_blocks {
            // Pop from dirty queue - O(1)
            let Some(block_num) = self.inner.dirty_queue.pop() else {
                break;
            };

            let idx = block_num as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }

            // Try CAS: Dirty → Syncing
            // The block might not be Dirty anymore if:
            // - It was already synced (duplicate in queue from recovery)
            // - Another thread claimed it (shouldn't happen with single sync worker)
            if self.inner.block_states[idx]
                .compare_exchange(
                    BlockState::Dirty as u8,
                    BlockState::Syncing as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                claimed.push(block_num);
                self.inner.dirty_block_count.fetch_sub(1, Ordering::Relaxed);
                self.inner.syncing_block_count.fetch_add(1, Ordering::Relaxed);
            }
            // If CAS fails, the block is no longer dirty - skip it
        }

        if !claimed.is_empty() {
            debug!(count = claimed.len(), "claimed dirty blocks for sync");
        }
        claimed
    }

    /// Mark a block as successfully synced (lock-free).
    pub fn mark_synced(&self, block_num: u64) {
        let idx = block_num as usize;
        if idx >= self.inner.num_blocks {
            return;
        }

        // CAS: Syncing → Clean
        if self.inner.block_states[idx]
            .compare_exchange(
                BlockState::Syncing as u8,
                BlockState::Clean as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Mark a block sync as failed (will retry) - lock-free.
    pub fn mark_sync_failed(&self, block_num: u64) {
        let idx = block_num as usize;
        if idx >= self.inner.num_blocks {
            return;
        }

        // CAS: Syncing → Dirty
        if self.inner.block_states[idx]
            .compare_exchange(
                BlockState::Syncing as u8,
                BlockState::Dirty as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
            self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
            // Re-queue for retry - without this, drain would miss failed blocks
            self.inner.dirty_queue.push(block_num);
        }
    }

    /// Read a single block from local cache.
    /// Handles partial last block correctly when device_size is not a multiple of block_size.
    #[allow(dead_code)] // Used by tests
    pub fn read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        self.sync_read_local_block(block_num)
    }

    /// Read a single block for sync worker.
    /// Reads from local disk cache (likely from page cache for recently written blocks).
    ///
    /// For the last block of a device, if device_size is not a multiple of block_size,
    /// this reads only the valid bytes and pads the rest with zeros. This is necessary
    /// because the cache file is sized to device_size, not num_blocks * block_size.
    pub fn sync_read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let offset = block_num * block_size as u64;

        // Calculate how many bytes are valid for this block
        // For most blocks this is block_size, but for the last partial block
        // it may be less (device_size - offset)
        let valid_bytes = if offset >= device_size {
            // Block is entirely beyond device - shouldn't happen but handle gracefully
            return Ok(Bytes::from(vec![0u8; block_size]));
        } else {
            std::cmp::min(block_size as u64, device_size - offset) as usize
        };

        let mut buf = vec![0u8; block_size];
        if valid_bytes == block_size {
            // Full block - read normally
            self.inner.data_file.read_exact_at(&mut buf, offset)?;
        } else {
            // Partial block (last block) - read only valid bytes, rest stays zero
            self.inner
                .data_file
                .read_exact_at(&mut buf[..valid_bytes], offset)?;
        }
        Ok(Bytes::from(buf))
    }

    /// Get the number of dirty blocks pending sync.
    pub fn dirty_block_count(&self) -> u64 {
        self.inner.dirty_block_count.load(Ordering::Relaxed)
    }

    /// Get the number of blocks currently being synced.
    pub fn syncing_block_count(&self) -> u64 {
        self.inner.syncing_block_count.load(Ordering::Relaxed)
    }

    /// Get the device size.
    #[allow(dead_code)]
    pub fn device_size(&self) -> u64 {
        self.inner.config.device_size
    }

    /// Get the block size.
    pub fn block_size(&self) -> usize {
        self.inner.config.block_size
    }

    /// Wait for dirty blocks to become available.
    ///
    /// Returns immediately if there are already dirty blocks in the queue.
    /// Otherwise, waits until a write operation adds dirty blocks and notifies.
    pub async fn wait_for_dirty(&self) {
        self.inner.dirty_notify.notified().await
    }

    /// Save metadata to disk.
    pub fn save_metadata(&self) -> Result<(), CacheError> {
        self.inner.save_metadata()
    }

    /// Sync dirty blocks to S3 using batch writes with conditional PUT.
    ///
    /// Groups blocks by batch number, performs GET-modify-PUT for each batch.
    /// Uses conditional PUT (If-Match) as defense-in-depth against concurrent writers.
    ///
    /// **Zero-block optimization**: Blocks that are all zeros are skipped if the
    /// corresponding position in S3 is already zeros (or the batch doesn't exist).
    /// Since missing blocks return zeros on read, this is safe and reduces S3 writes
    /// for fresh VMs, TRIM operations, and sparse filesystems.
    ///
    /// Returns the number of blocks successfully synced.
    pub async fn sync_blocks_batched(&self, s3: &S3BlockStore, block_nums: Vec<u64>) -> Result<usize, CacheError> {
        self.sync_blocks_batched_parallel(s3, block_nums, 1).await
    }

    /// Sync dirty blocks to S3 with parallel uploads.
    ///
    /// Like `sync_blocks_batched`, but processes up to `max_concurrent` batches
    /// in parallel. This significantly improves throughput when syncing many
    /// batches, as S3 latency (~100ms) is the bottleneck.
    ///
    /// Unlike the sequential version, this method does NOT fail-fast on errors.
    /// Each batch is processed independently - if one fails, others continue.
    /// Failed batches have their blocks marked for retry.
    ///
    /// Returns the number of blocks successfully synced.
    pub async fn sync_blocks_batched_parallel(
        &self,
        s3: &S3BlockStore,
        block_nums: Vec<u64>,
        max_concurrent: usize,
    ) -> Result<usize, CacheError> {
        use futures::stream::{self, StreamExt};
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;

        if block_nums.is_empty() {
            return Ok(0);
        }

        let max_concurrent = max_concurrent.max(1); // At least 1

        // Group blocks by batch number
        let mut batches: HashMap<u64, Vec<u64>> = HashMap::new();
        for block_num in block_nums {
            let batch_num = s3.batch_num(block_num);
            batches.entry(batch_num).or_default().push(block_num);
        }

        let block_size = self.inner.config.block_size;
        let synced_count = AtomicUsize::new(0);

        // Process batches in parallel with bounded concurrency
        let results: Vec<_> = stream::iter(batches)
            .map(|(batch_num, blocks_in_batch)| {
                let synced_count = &synced_count;
                async move {
                    // GET existing batch with ETag for conditional PUT
                    let batch_result = match s3.get_batch_with_etag(batch_num).await {
                        Ok(result) => result,
                        Err(e) => {
                            // Mark all blocks in this batch as failed
                            for block_num in &blocks_in_batch {
                                self.mark_sync_failed(*block_num);
                            }
                            warn!(batch = batch_num, error = %e, "failed to fetch batch for sync");
                            return;
                        }
                    };

                    let mut batch_data = batch_result.data;
                    let etag = batch_result.etag;
                    let mut batch_modified = false;

                    // Update dirty block slots with local data
                    // Use sync FD to avoid contention with NBD reads
                    for &block_num in &blocks_in_batch {
                        let local_data = match self.sync_read_local_block(block_num) {
                            Ok(data) => data,
                            Err(e) => {
                                warn!(block = block_num, error = %e, "failed to read local block");
                                self.mark_sync_failed(block_num);
                                continue;
                            }
                        };
                        let offset = s3.offset_in_batch(block_num) as usize;
                        let s3_slice = &batch_data[offset..offset + block_size];

                        // Zero-block optimization
                        let local_is_zero = is_zero_block(&local_data);
                        let s3_is_zero = is_zero_block(s3_slice);

                        if local_is_zero && s3_is_zero {
                            debug!(block = block_num, "skipping zero block (S3 already zeros)");
                        } else {
                            batch_data[offset..offset + local_data.len()].copy_from_slice(&local_data);
                            batch_modified = true;
                        }
                    }

                    // Only PUT if we actually modified the batch
                    if batch_modified
                        && let Err(e) = s3.put_batch_conditional(batch_num, batch_data, etag).await
                    {
                        // Mark all blocks in this batch as failed
                        for block_num in &blocks_in_batch {
                            self.mark_sync_failed(*block_num);
                        }
                        warn!(batch = batch_num, error = %e, "failed to upload batch");
                        return;
                    }

                    // Mark all blocks in this batch as synced
                    for block_num in blocks_in_batch {
                        self.mark_synced(block_num);
                        synced_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .buffer_unordered(max_concurrent)
            .collect()
            .await;

        // All futures completed (results is Vec<()>)
        drop(results);

        Ok(synced_count.load(Ordering::Relaxed))
    }

    /// Graceful shutdown: sync all blocks and transition to Draining.
    #[allow(dead_code)]
    #[instrument(skip(self, s3))]
    pub async fn shutdown(self, s3: &S3BlockStore) -> Result<WriteCache<Draining>, CacheError> {
        info!("starting graceful shutdown");

        // Claim and sync all remaining dirty blocks using batched writes
        loop {
            let dirty = self.claim_dirty_blocks(1000);
            if dirty.is_empty() {
                break;
            }

            if let Err(e) = self.sync_blocks_batched(s3, dirty).await {
                warn!(error = %e, "shutdown sync failed");
            }
        }

        // Wait for any in-flight syncs
        while self.syncing_block_count() > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Save final metadata
        self.inner.save_metadata()?;
        info!("shutdown complete, all blocks synced");

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }

    /// Drain all dirty blocks to S3 for a consistent snapshot.
    ///
    /// Call this before cross-host clone operations to ensure all data is
    /// visible to other hosts. Normal FLUSH operations use fast local fsync.
    ///
    /// Returns when all dirty blocks have been uploaded to S3.
    #[instrument(skip(self, s3))]
    pub async fn drain_for_snapshot(&self, s3: &S3BlockStore) -> Result<(), CacheError> {
        let initial_dirty = self.dirty_block_count();
        if initial_dirty == 0 && self.syncing_block_count() == 0 {
            debug!("no dirty blocks, drain complete");
            return Ok(());
        }

        info!(dirty_blocks = initial_dirty, "draining for snapshot");

        // Outer loop handles the case where blocks are re-queued during sync
        // (e.g., by background worker failures or our own sync failures)
        loop {
            // Inner loop: claim and sync all dirty blocks
            loop {
                let dirty = self.claim_dirty_blocks(100);
                if dirty.is_empty() {
                    break;
                }

                self.sync_blocks_batched(s3, dirty).await?;
            }

            // Wait for any in-flight syncs (from background worker or our parallel syncs)
            while self.syncing_block_count() > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }

            // Re-check: blocks may have been re-queued by failed syncs during the wait
            if self.dirty_block_count() == 0 {
                break;
            }

            debug!(
                dirty = self.dirty_block_count(),
                "blocks re-queued during drain, continuing"
            );
        }

        info!("drain complete, all blocks in S3");
        Ok(())
    }

    /// Shared inner logic for flush/snapshot: snapshot dirty entries, dedup,
    /// compress, pack upload, CAS-clear dirty flags.
    ///
    /// Returns (stats, seq_cutpoint) on success.
    async fn flush_dirty_inner(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
    ) -> Result<(FlushStats, u64), CacheError> {
        let mut stats = FlushStats::default();
        let chunk_size = self.inner.config.block_size as u32;

        // 1. Capture sequence cut point
        let seq_cutpoint = self.inner.sequence.current();

        // 2. Snapshot dirty entries: non-empty, dirty, sequence <= cutpoint
        let snapshot: Vec<(usize, Blake3Hash)> = {
            let block_map_snap = self.inner.block_map_snapshot();
            block_map_snap
                .iter_non_empty()
                .filter(|(_, entry)| {
                    entry.is_dirty() && entry.sequence <= seq_cutpoint
                })
                .map(|(idx, entry)| (idx, entry.hash))
                .collect()
        };

        if snapshot.is_empty() {
            debug!("flush: no dirty blocks to flush");
            return Ok((stats, seq_cutpoint));
        }

        info!(dirty_blocks = snapshot.len(), seq_cutpoint, "starting flush");

        // 3. Dedup check + compress new blocks
        let zero_hash = *ZERO_BLOCK_HASH;
        let mut to_upload: Vec<(Blake3Hash, Vec<u8>)> = Vec::new();

        for &(_chunk_index, hash) in &snapshot {
            stats.blocks_flushed += 1;

            // Skip zero blocks — read path returns zeros directly
            if hash == zero_hash {
                stats.blocks_deduped += 1;
                continue;
            }

            // Skip blocks already in the host pack index (cross-VM dedup)
            if host_pack_index.contains(&hash) {
                stats.blocks_deduped += 1;
                continue;
            }

            // Get data from dirty store
            let data = self
                .inner
                .dirty_store
                .lock()
                .unwrap()
                .get(&hash)
                .cloned();

            let Some(data) = data else {
                // Data missing from dirty store — could have been cleaned by a
                // concurrent flush. Skip; it's already in S3.
                stats.blocks_deduped += 1;
                continue;
            };

            let compressed = lz4_compress(&data);
            to_upload.push((hash, compressed));
        }

        // 4. Assemble packs and upload
        for chunk in to_upload.chunks(BLOCKS_PER_PACK) {
            let pack_id = uuid::Uuid::new_v4();
            let (pack_bytes, index_entries) = pack::assemble_pack(chunk, chunk_size);
            let pack_size = pack_bytes.len() as u64;

            content_store.put_pack(pack_id, pack_bytes).await?;
            stats.packs_uploaded += 1;
            stats.bytes_uploaded += pack_size;

            // Register in host pack index for future dedup
            for entry in &index_entries {
                host_pack_index.insert(
                    entry.hash,
                    PackLocation {
                        pack_id,
                        offset: entry.offset,
                        comp_length: entry.comp_length,
                    },
                );
            }
        }

        // 5. Clear dirty state for blocks whose hash hasn't changed
        for &(chunk_index, snapshot_hash) in &snapshot {
            let (current_hash, _seq) = self.inner.block_map_get(chunk_index);
            if current_hash == snapshot_hash {
                // Hash unchanged since snapshot — safe to clear dirty flag.
                // CAS Dirty → Clean on the block_states AtomicU8.
                let _ = self.inner.block_states[chunk_index].compare_exchange(
                    BlockMapEntry::FLAG_DIRTY,
                    0, // clean
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                // Remove from dirty store (keyed by hash, so safe)
                self.inner
                    .dirty_store
                    .lock()
                    .unwrap()
                    .remove(&snapshot_hash);
                self.inner
                    .dirty_bytes
                    .fetch_sub(chunk_size as u64, Ordering::Relaxed);
            }
            // else: concurrent write produced a new hash — leave dirty for next flush
        }

        info!(
            blocks_flushed = stats.blocks_flushed,
            blocks_deduped = stats.blocks_deduped,
            packs_uploaded = stats.packs_uploaded,
            bytes_uploaded = stats.bytes_uploaded,
            "flush dirty inner complete"
        );

        Ok((stats, seq_cutpoint))
    }

    /// Build and upload a manifest from current state.
    ///
    /// Returns the S3 ETag if the backend provides one.
    async fn upload_manifest(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
        seq_cutpoint: u64,
    ) -> Result<Option<String>, CacheError> {
        let chunk_size = self.inner.config.block_size as u32;
        let post_flush_snap = self.inner.block_map_snapshot();
        let block_map_entries: Vec<ManifestBlockEntry> = post_flush_snap
            .iter_non_empty()
            .map(|(idx, entry)| ManifestBlockEntry {
                chunk_index: idx as u64,
                hash: entry.hash,
                flags: entry.flags,
            })
            .collect();
        let pack_entries = host_pack_index.derive_for_block_map(&post_flush_snap);

        let manifest = Manifest {
            name: self.inner.export_name.clone(),
            sequence: seq_cutpoint,
            chunk_size,
            device_size: self.inner.config.device_size,
            block_map: block_map_entries,
            pack_index: pack_entries,
        };

        let etag = content_store
            .put_manifest(&self.inner.export_name, manifest.serialize())
            .await?;

        Ok(etag)
    }

    /// Flush dirty blocks to S3 as content-addressed packs + manifest.
    ///
    /// This is the v2 flush path:
    /// 1. Snapshot dirty block map entries up to the current sequence
    /// 2. Dedup against the host pack index (blocks already in S3)
    /// 3. LZ4-compress new blocks and assemble into 25-block packs
    /// 4. Upload packs to S3
    /// 5. Clear dirty flags (with concurrent-write safety)
    /// 6. Upload a self-contained manifest
    ///
    /// Manual trigger for now; Phase 6 adds scheduling.
    #[instrument(skip(self, content_store, host_pack_index))]
    pub async fn flush_to_s3(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
    ) -> Result<FlushStats, CacheError> {
        let (stats, seq_cutpoint) = self.flush_dirty_inner(content_store, host_pack_index).await?;
        self.upload_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        Ok(stats)
    }

    /// Take a point-in-time snapshot: flush dirty blocks + upload manifest.
    ///
    /// Returns the manifest ETag and sequence number for the control plane
    /// to use when creating a fork.
    #[instrument(skip(self, content_store, host_pack_index))]
    pub async fn snapshot(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
    ) -> Result<SnapshotResult, CacheError> {
        let (stats, seq_cutpoint) = self.flush_dirty_inner(content_store, host_pack_index).await?;
        let manifest_etag = self.upload_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        Ok(SnapshotResult {
            manifest_etag,
            sequence: seq_cutpoint,
            stats,
        })
    }

    /// Check if a forked block map should be flattened, and do so.
    ///
    /// Flatten replaces a ForkedBlockMap (sparse overlay) with a full AtomicBlockMap
    /// when the overlay exceeds 50% of total chunks. Takes a write lock briefly.
    fn try_flatten_block_map(&self) {
        let needs_flatten = {
            let bm = self.inner.block_map.read().unwrap();
            match &*bm {
                BlockMapKind::Forked(f) => f.should_flatten(),
                BlockMapKind::Full(_) => false,
            }
        };
        if needs_flatten {
            let mut bm = self.inner.block_map.write().unwrap();
            // Re-check under write lock (another thread may have flattened)
            if let BlockMapKind::Forked(f) = &*bm {
                if f.should_flatten() {
                    let full = f.flatten();
                    *bm = BlockMapKind::Full(full);
                    info!("flattened forked block map");
                }
            }
        }
    }

    /// Persist the v2 block map to disk and truncate the WAL.
    ///
    /// Called periodically (~5s) by the block map persistence worker.
    /// Uses atomic file write (tempfile + rename) for crash safety.
    pub fn persist_block_map(&self) -> Result<(), CacheError> {
        // Snapshot the atomic block map (lock-free)
        let snapshot = self.inner.block_map_snapshot();

        // Persist to file atomically
        let path = self.inner.config.block_map_path();
        snapshot.persist_to_file(&path).map_err(CacheError::Io)?;

        // Truncate WAL (entries are now in the persisted block map)
        if let Err(e) = self.inner.wal.lock().unwrap().truncate() {
            warn!(error = %e, "WAL truncate failed after block map persist");
        }

        debug!(entries = snapshot.non_empty_count(), "persisted v2 block map");
        Ok(())
    }

    /// Get a clone of the inner Arc for sharing with the sync worker.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> Arc<CacheInner> {
        Arc::clone(&self.inner)
    }
}

#[allow(dead_code)]
impl WriteCache<Draining> {
    /// Finish draining and drop the cache.
    pub fn finish(self) {
        info!("cache drained and closed");
    }
}

// ============================================================================
// Background Sync Worker
// ============================================================================

/// Configuration for the sync worker.
#[derive(Clone, Debug)]
pub struct SyncWorkerConfig {
    /// Maximum number of blocks to claim per sync cycle.
    /// Higher values reduce per-cycle overhead but increase memory usage during sync.
    pub batch_size: usize,

    /// Maximum concurrent S3 uploads per sync cycle.
    /// Higher values increase throughput but use more memory and connections.
    /// Set to 1 for sequential uploads (original behavior).
    pub max_concurrent_uploads: usize,

    /// Hot batch cooldown: don't re-sync an S3 batch within this duration.
    ///
    /// When a batch is synced, it enters a "cooldown" period. If more blocks
    /// in that batch become dirty during cooldown, they're deferred to the next
    /// cycle. This prevents repeated GET-modify-PUT of the same batch when
    /// writes are spread over time.
    ///
    /// Example: With 100ms cooldown and blocks 0,1,2 all in batch 0:
    /// - t=0ms:   Block 0 dirty → sync batch 0 (GET+PUT)
    /// - t=50ms:  Block 1 dirty → deferred (batch 0 in cooldown)
    /// - t=80ms:  Block 2 dirty → deferred (batch 0 in cooldown)
    /// - t=100ms: Cooldown expires → sync batch 0 with blocks 1,2 (single GET+PUT)
    ///
    /// Set to 0 to disable (every dirty block synced immediately).
    pub hot_batch_cooldown: std::time::Duration,

    /// Maximum times a block can be deferred due to hot batch cooldown.
    ///
    /// After a block has been deferred this many times, it will be force-synced
    /// even if its batch is still hot. This prevents starvation when a batch
    /// receives continuous writes.
    ///
    /// Set to 0 to disable (blocks can be deferred indefinitely).
    pub max_deferrals: u32,

    /// How often to save metadata (every N sync cycles).
    pub metadata_save_interval: usize,

    /// Back-pressure: warn when dirty queue exceeds this depth.
    /// With 128KB blocks, 1000 blocks = 128MB of uncommitted data.
    /// Set to 0 to disable warning.
    pub dirty_queue_warn_threshold: u64,

    /// Back-pressure: log errors when dirty queue exceeds this depth.
    /// Indicates S3 sync cannot keep up with write rate.
    /// Set to 0 to disable.
    pub dirty_queue_critical_threshold: u64,
}

impl Default for SyncWorkerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            // 1 concurrent S3 upload - minimizes I/O contention with reads
            max_concurrent_uploads: 1,
            // 8 second sync interval: testing PUT count reduction.
            // Longer interval = more coalescing = fewer S3 PUTs.
            // Recently written blocks are likely still in page cache when sync reads them.
            hot_batch_cooldown: std::time::Duration::from_secs(8),
            // After 10 deferrals, force sync even if batch is hot
            // At 8s cooldown, this means ~80 second max delay
            max_deferrals: 10,
            metadata_save_interval: 10,
            // 1000 blocks @ 128KB = 128MB uncommitted - time to investigate
            dirty_queue_warn_threshold: 1_000,
            // 10000 blocks @ 128KB = 1.28GB uncommitted - serious problem
            dirty_queue_critical_threshold: 10_000,
        }
    }
}

/// Sync worker that continuously drains dirty blocks to S3.
///
/// This function runs in a background task and uploads dirty blocks using
/// batched S3 writes. Blocks are grouped by batch number and uploaded
/// together to reduce S3 PUT costs by ~10x.
///
/// # Cancellation
/// The worker checks the shutdown signal between upload batches and exits
/// gracefully when signaled.
///
/// # Example
/// ```ignore
/// let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
/// let cache = Arc::new(cache);
/// let s3 = Arc::new(s3_store);
///
/// let worker_handle = tokio::spawn(sync_worker(
///     Arc::clone(&cache),
///     Arc::clone(&s3),
///     SyncWorkerConfig::default(),
///     shutdown_rx,
/// ));
///
/// // To stop:
/// let _ = shutdown_tx.send(true);
/// worker_handle.await;
/// ```
#[instrument(skip(cache, s3, config, shutdown))]
pub async fn sync_worker(
    cache: Arc<WriteCache<Active>>,
    s3: Arc<S3BlockStore>,
    config: SyncWorkerConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    use std::collections::HashMap;

    info!(
        batch_size = config.batch_size,
        hot_batch_cooldown_ms = config.hot_batch_cooldown.as_millis(),
        max_deferrals = config.max_deferrals,
        blocks_per_batch = s3.blocks_per_batch(),
        "sync worker started (event-driven)"
    );

    let mut batches_since_save = 0;
    let mut last_backpressure_warn = std::time::Instant::now() - std::time::Duration::from_secs(60);

    // Hot batch tracking: batch_num -> last sync time
    // Batches synced recently are "hot" and we defer syncing them again
    let mut hot_batches: HashMap<u64, std::time::Instant> = HashMap::new();

    // Deferral count tracking: block_num -> times deferred
    // When a block exceeds max_deferrals, force sync even if batch is hot
    let mut deferral_counts: HashMap<u64, u32> = HashMap::new();

    loop {
        // Check for shutdown
        if *shutdown.borrow() {
            info!("sync worker received shutdown signal");
            break;
        }

        // Claim a batch of dirty blocks from the queue (O(1) per block)
        let dirty = cache.claim_dirty_blocks(config.batch_size);

        if dirty.is_empty() {
            // No dirty blocks - wait for notification from write path.
            // Use a timeout as safety net in case notification was missed.
            tokio::select! {
                _ = cache.wait_for_dirty() => {}  // Woken by write path
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("sync worker received shutdown signal");
                        break;
                    }
                }
            }
            continue;
        }

        // Back-pressure monitoring: check queue depth and warn/error if too deep
        let current_dirty = cache.dirty_block_count();
        if config.dirty_queue_critical_threshold > 0
            && current_dirty > config.dirty_queue_critical_threshold
        {
            // Rate-limit critical logs to once per 10 seconds
            if last_backpressure_warn.elapsed() > std::time::Duration::from_secs(10) {
                tracing::error!(
                    dirty_blocks = current_dirty,
                    threshold = config.dirty_queue_critical_threshold,
                    block_size = cache.block_size(),
                    uncommitted_mb = (current_dirty as usize * cache.block_size()) / (1024 * 1024),
                    "CRITICAL: dirty queue depth exceeds critical threshold - S3 sync cannot keep up"
                );
                last_backpressure_warn = std::time::Instant::now();
            }
        } else if config.dirty_queue_warn_threshold > 0
            && current_dirty > config.dirty_queue_warn_threshold
        {
            // Rate-limit warnings to once per 30 seconds
            if last_backpressure_warn.elapsed() > std::time::Duration::from_secs(30) {
                warn!(
                    dirty_blocks = current_dirty,
                    threshold = config.dirty_queue_warn_threshold,
                    uncommitted_mb = (current_dirty as usize * cache.block_size()) / (1024 * 1024),
                    "dirty queue depth elevated - consider investigating S3 latency"
                );
                last_backpressure_warn = std::time::Instant::now();
            }
        }

        // === Hot Batch Filtering ===
        // Group blocks by batch and filter out batches that are still "hot" (recently synced).
        // This reduces write amplification by letting more blocks accumulate before re-syncing.
        let now = std::time::Instant::now();
        let cooldown_enabled = !config.hot_batch_cooldown.is_zero();

        // Clean up expired hot batch entries (older than 2x cooldown to avoid unbounded growth)
        if cooldown_enabled {
            let expiry = config.hot_batch_cooldown * 2;
            hot_batches.retain(|_, last_sync| now.duration_since(*last_sync) < expiry);
        }

        // Group claimed blocks by batch number
        let mut blocks_by_batch: HashMap<u64, Vec<u64>> = HashMap::new();
        for block_num in dirty {
            let batch_num = s3.batch_num(block_num);
            blocks_by_batch.entry(batch_num).or_default().push(block_num);
        }

        // Separate into ready-to-sync and deferred (hot batch)
        let mut blocks_to_sync = Vec::new();
        let mut deferred_count = 0u64;
        let mut force_synced_count = 0u64;
        let max_deferrals_enabled = config.max_deferrals > 0;

        for (batch_num, blocks) in blocks_by_batch {
            let is_hot = cooldown_enabled
                && hot_batches
                    .get(&batch_num)
                    .map(|last_sync| now.duration_since(*last_sync) < config.hot_batch_cooldown)
                    .unwrap_or(false);

            if is_hot {
                // Batch is hot - check each block's deferral count
                for block_num in blocks {
                    let count = deferral_counts.entry(block_num).or_insert(0);
                    *count += 1;

                    if max_deferrals_enabled && *count > config.max_deferrals {
                        // Block has been deferred too many times - force sync
                        blocks_to_sync.push(block_num);
                        force_synced_count += 1;
                    } else {
                        // Defer this block
                        cache.mark_sync_failed(block_num);
                        deferred_count += 1;
                    }
                }
            } else {
                // Batch is cool - include these blocks for sync
                blocks_to_sync.extend(blocks);
            }
        }

        if deferred_count > 0 || force_synced_count > 0 {
            debug!(
                deferred = deferred_count,
                force_synced = force_synced_count,
                syncing = blocks_to_sync.len(),
                "hot batch filtering complete"
            );
        }

        // If all blocks were deferred, continue to next cycle
        if blocks_to_sync.is_empty() {
            continue;
        }

        let block_count = blocks_to_sync.len();
        let start = std::time::Instant::now();

        // Sync blocks using batched writes (groups by S3 batch, GET-modify-PUT)
        // Track which batches and blocks we're about to sync
        let batches_syncing: std::collections::HashSet<u64> = blocks_to_sync
            .iter()
            .map(|&b| s3.batch_num(b))
            .collect();
        let blocks_syncing: Vec<u64> = blocks_to_sync.clone();

        match cache.sync_blocks_batched_parallel(&s3, blocks_to_sync, config.max_concurrent_uploads).await {
            Ok(synced) => {
                let elapsed = start.elapsed();

                // Mark synced batches as hot
                let sync_time = std::time::Instant::now();
                for batch_num in batches_syncing {
                    hot_batches.insert(batch_num, sync_time);
                }

                // Clear deferral counts for synced blocks
                for block_num in &blocks_syncing {
                    deferral_counts.remove(block_num);
                }

                debug!(
                    blocks = synced,
                    elapsed_ms = elapsed.as_millis(),
                    throughput_mb_s = (synced * cache.block_size()) as f64 / elapsed.as_secs_f64() / 1_000_000.0,
                    hot_batches = hot_batches.len(),
                    "sync batch completed"
                );
            }
            Err(e) => {
                warn!(
                    blocks = block_count,
                    error = %e,
                    "sync batch failed, backing off 1s"
                );
                // Simple backoff: sleep 1s after any failure to avoid retry spam
                // during S3 outages. Blocks are already re-queued by mark_sync_failed.
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }

        // Periodically save metadata to persist sync progress
        batches_since_save += 1;
        if batches_since_save >= config.metadata_save_interval {
            if let Err(e) = cache.save_metadata() {
                warn!(error = %e, "failed to save metadata during sync");
            }
            batches_since_save = 0;
        }
    }

    // Final metadata save on shutdown
    if let Err(e) = cache.save_metadata() {
        warn!(error = %e, "failed to save metadata on sync worker shutdown");
    }

    info!("sync worker stopped");
}

/// Background worker that periodically persists the v2 block map.
///
/// Runs every 5 seconds. Persists the block map snapshot to disk and
/// truncates the WAL. This bounds WAL replay time on recovery.
#[instrument(skip(cache, shutdown))]
pub async fn block_map_persist_worker(
    cache: Arc<WriteCache<Active>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    info!("block map persist worker started (5s interval)");

    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    // Final persist before shutdown
                    if let Err(e) = cache.persist_block_map() {
                        warn!(error = %e, "final block map persist failed");
                    }
                    info!("block map persist worker stopped");
                    return;
                }
            }
        }

        if let Err(e) = cache.persist_block_map() {
            warn!(error = %e, "periodic block map persist failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> WriteCacheConfig {
        WriteCacheConfig {
            cache_dir: dir.to_path_buf(),
            device_name: "test".to_string(),
            device_size: 1024 * 1024, // 1MB
            block_size: 4096,         // 4KB for testing
        }
    }

    fn test_s3() -> S3BlockStore {
        let object_store = Arc::new(InMemory::new());
        S3BlockStore::with_defaults(object_store, "test")
    }

    #[tokio::test]
    async fn test_open_fresh_cache() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        assert_eq!(cache.dirty_block_count(), 0);
    }

    #[tokio::test]
    async fn test_write_read() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write some data
        cache.write(0, b"hello world").unwrap();

        // Read it back
        let data = cache.read(0, 11).unwrap();
        assert_eq!(&data[..], b"hello world");

        // Should have dirty blocks now
        assert!(cache.dirty_block_count() > 0);
    }

    #[tokio::test]
    async fn test_flush() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        cache.write(0, b"data").unwrap();
        cache.flush().unwrap();

        // Data should still be readable
        let data = cache.read(0, 4).unwrap();
        assert_eq!(&data[..], b"data");
    }

    #[tokio::test]
    async fn test_claim_and_sync() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write to first block
        cache.write(0, b"block data").unwrap();
        assert_eq!(cache.dirty_block_count(), 1);

        // Claim dirty blocks
        let claimed = cache.claim_dirty_blocks(100);
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0], 0);
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 1);

        // Mark as synced
        cache.mark_synced(0);
        assert_eq!(cache.syncing_block_count(), 0);
    }

    #[tokio::test]
    async fn test_write_during_sync() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write and claim
        cache.write(0, b"first").unwrap();
        let _ = cache.claim_dirty_blocks(100);
        assert_eq!(cache.syncing_block_count(), 1);

        // Write again during sync - should go back to dirty
        cache.write(0, b"second").unwrap();
        assert_eq!(cache.dirty_block_count(), 1);
        assert_eq!(cache.syncing_block_count(), 0);
    }

    #[tokio::test]
    async fn test_metadata_persistence() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        // Create cache and write data
        {
            let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
            let cache = cache.finish_recovery(&s3).await.unwrap();
            cache.write(0, b"persistent").unwrap();
            cache.save_metadata().unwrap();
        }

        // Reopen and verify dirty blocks are preserved
        {
            let cache = WriteCache::<Initializing>::open(config).unwrap();
            // Should have dirty blocks from previous session
            assert!(cache.inner.dirty_block_count.load(Ordering::Relaxed) > 0);

            let cache = cache.finish_recovery(&s3).await.unwrap();
            // Data should be readable
            let data = cache.read(0, 10).unwrap();
            assert_eq!(&data[..], b"persistent");
        }
    }

    #[tokio::test]
    async fn test_drain_for_snapshot() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write some data
        cache.write(0, b"snapshot data").unwrap();
        assert!(cache.dirty_block_count() > 0);

        // Drain for snapshot
        cache.drain_for_snapshot(&s3).await.unwrap();

        // All blocks should be synced
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 0);

        // Data should be in S3
        let s3_data = s3.read_block(0).await.unwrap();
        assert_eq!(&s3_data[..13], b"snapshot data");
    }

    #[tokio::test]
    async fn test_sync_worker() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = Arc::new(test_s3());

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.finish_recovery(&s3).await.unwrap());

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Start sync worker
        let worker_cache = Arc::clone(&cache);
        let worker_s3 = Arc::clone(&s3);
        let worker_handle = tokio::spawn(super::sync_worker(
            worker_cache,
            worker_s3,
            super::SyncWorkerConfig::default(),
            shutdown_rx,
        ));

        // Write some data
        cache.write(0, b"worker test").unwrap();

        // Wait for sync to complete (with timeout)
        let start = std::time::Instant::now();
        while cache.dirty_block_count() > 0 && start.elapsed().as_secs() < 5 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        // Should be synced
        assert_eq!(cache.dirty_block_count(), 0);

        // Stop worker
        let _ = shutdown_tx.send(true);
        worker_handle.await.unwrap();

        // Data should be in S3
        let s3_data = s3.read_block(0).await.unwrap();
        assert_eq!(&s3_data[..11], b"worker test");
    }

    #[tokio::test]
    async fn test_read_through_from_s3() {
        // This test verifies the core read-through functionality:
        // When a block exists in S3 but not locally, read_with_fetch should fetch it.

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
            .with_blocks_per_batch(10); // 10 blocks per batch for testing
        let metrics = super::super::metrics::ExportMetrics::new();

        // Pre-populate S3 with some data (simulating data from another node)
        // Block 0 is in batch 0, block 5 is also in batch 0 (with 10 blocks per batch)
        let mut batch0 = vec![0u8; s3.batch_size()];
        // Block 0: fill with 42
        batch0[..4096].copy_from_slice(&vec![42u8; 4096]);
        // Block 5: fill with 99
        let block5_offset = 5 * 4096;
        batch0[block5_offset..block5_offset + 4096].copy_from_slice(&vec![99u8; 4096]);
        s3.put_batch(0, batch0).await.unwrap();

        // Create a fresh cache on a "new node" (no local data)
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Read block 0 - should fetch from S3
        let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 42);
        assert!(data.iter().all(|&b| b == 42));

        // Read block 5 - should also fetch from S3
        let offset = 5 * 4096;
        let data = cache.read_with_fetch(offset, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 99);

        // Second read of block 0 should come from local cache now
        let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 42);

        // Read a block that doesn't exist in S3 - should return zeros
        let offset = 10 * 4096;
        let data = cache.read_with_fetch(offset, 4096, &s3, &metrics).await.unwrap();
        assert!(data.iter().all(|&b| b == 0));

        // Verify metrics were recorded
        // With batch prefetching:
        // - Read block 0: cache miss, fetches batch 0 (caches blocks 0-9), 1 S3 read
        // - Read block 5: cache HIT (was prefetched with block 0's batch)
        // - Read block 0 again: cache hit
        // - Read block 10: cache miss, fetches batch 1 (returns zeros), 1 S3 read
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_misses, 2); // blocks 0 and 10 (block 5 was prefetched)
        assert_eq!(snapshot.cache_hits, 2); // block 5 (prefetched) + second read of block 0
        assert_eq!(snapshot.s3_read_ops, 2); // batch 0 + batch 1 (even though batch 1 is empty)
    }

    #[tokio::test]
    async fn test_write_then_read_local() {
        // Verify that written blocks are marked as present and read locally
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();
        let metrics = super::super::metrics::ExportMetrics::new();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write data locally
        cache.write(0, b"local data!").unwrap();

        // Read should come from local cache, not S3
        let data = cache.read_with_fetch(0, 11, &s3, &metrics).await.unwrap();
        assert_eq!(&data[..], b"local data!");

        // S3 should NOT have this data yet (not synced)
        let s3_result = s3.read_block(0).await;
        assert!(matches!(
            s3_result,
            Err(super::super::block_store::BlockStoreError::NotFound(_))
        ));

        // Verify cache hit (data was present locally)
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 1);
        assert_eq!(snapshot.cache_misses, 0);
    }

    #[tokio::test]
    async fn test_batch_prefetch_single_batch_efficiency() {
        // When reading multiple scattered blocks from the SAME S3 batch,
        // we should only make ONE S3 call (not one per block).
        //
        // This tests the core efficiency gain of batch prefetching.

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
            .with_blocks_per_batch(100); // 100 blocks per batch
        let metrics = super::super::metrics::ExportMetrics::new();

        // Pre-populate S3 batch 0 with distinct data for blocks 0, 25, 50, 75
        let mut batch0 = vec![0u8; s3.batch_size()];
        batch0[0..4096].copy_from_slice(&vec![11u8; 4096]);           // block 0
        batch0[25 * 4096..26 * 4096].copy_from_slice(&vec![22u8; 4096]); // block 25
        batch0[50 * 4096..51 * 4096].copy_from_slice(&vec![33u8; 4096]); // block 50
        batch0[75 * 4096..76 * 4096].copy_from_slice(&vec![44u8; 4096]); // block 75
        s3.put_batch(0, batch0).await.unwrap();

        // Create fresh cache
        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "prefetch_test".to_string(),
            device_size: 100 * 4096, // 100 blocks
            block_size: 4096,
        };
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Read block 0 - triggers fetch of entire batch 0
        let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 11, "block 0 should have correct data");

        // Read block 25 - should be a CACHE HIT (prefetched with block 0)
        let data = cache.read_with_fetch(25 * 4096, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 22, "block 25 should have correct data");

        // Read block 50 - should be a CACHE HIT (prefetched with block 0)
        let data = cache.read_with_fetch(50 * 4096, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 33, "block 50 should have correct data");

        // Read block 75 - should be a CACHE HIT (prefetched with block 0)
        let data = cache.read_with_fetch(75 * 4096, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 44, "block 75 should have correct data");

        // Verify: only ONE S3 read operation for 4 block reads
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.s3_read_ops, 1, "should only fetch batch once");
        assert_eq!(snapshot.cache_misses, 1, "only first read should be a miss");
        assert_eq!(snapshot.cache_hits, 3, "subsequent reads should hit cache");
    }

    #[tokio::test]
    async fn test_batch_prefetch_cross_batch_efficiency() {
        // When reading blocks from N different S3 batches,
        // we should make exactly N S3 calls.

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
            .with_blocks_per_batch(10); // 10 blocks per batch for easier testing
        let metrics = super::super::metrics::ExportMetrics::new();

        // Pre-populate 3 batches
        let mut batch0 = vec![0u8; s3.batch_size()];
        batch0[0..4096].copy_from_slice(&vec![0xAAu8; 4096]); // block 0
        s3.put_batch(0, batch0).await.unwrap();

        let mut batch1 = vec![0u8; s3.batch_size()];
        batch1[5 * 4096..6 * 4096].copy_from_slice(&vec![0xBBu8; 4096]); // block 15 (5th in batch 1)
        s3.put_batch(1, batch1).await.unwrap();

        let mut batch2 = vec![0u8; s3.batch_size()];
        batch2[3 * 4096..4 * 4096].copy_from_slice(&vec![0xCCu8; 4096]); // block 23 (3rd in batch 2)
        s3.put_batch(2, batch2).await.unwrap();

        // Create fresh cache
        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "cross_batch_test".to_string(),
            device_size: 30 * 4096, // 30 blocks = 3 batches
            block_size: 4096,
        };
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Read from batch 0
        let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 0xAA);

        // Read from batch 1 (block 15)
        let data = cache.read_with_fetch(15 * 4096, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 0xBB);

        // Read from batch 2 (block 23)
        let data = cache.read_with_fetch(23 * 4096, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 0xCC);

        // Verify: exactly 3 S3 reads (one per batch)
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.s3_read_ops, 3, "should fetch each batch once");
        assert_eq!(snapshot.cache_misses, 3, "one miss per batch");
    }

    #[tokio::test]
    async fn test_batch_prefetch_multi_block_read_span() {
        // When a single read spans multiple blocks in the same batch,
        // we should prefetch the entire batch and serve the read efficiently.

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
            .with_blocks_per_batch(10);
        let metrics = super::super::metrics::ExportMetrics::new();

        // Pre-populate batch 0 with sequential pattern
        let mut batch0 = vec![0u8; s3.batch_size()];
        for i in 0..10 {
            let block_start = i * 4096;
            batch0[block_start..block_start + 4096].copy_from_slice(&vec![i as u8; 4096]);
        }
        s3.put_batch(0, batch0).await.unwrap();

        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "span_test".to_string(),
            device_size: 10 * 4096,
            block_size: 4096,
        };
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Read 3 blocks at once (blocks 2, 3, 4) - should fetch batch once
        let data = cache.read_with_fetch(2 * 4096, 3 * 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 2, "block 2 should start with 2");
        assert_eq!(data[4096], 3, "block 3 should start with 3");
        assert_eq!(data[8192], 4, "block 4 should start with 4");

        // Now read block 7 - should be a cache hit (prefetched)
        let data = cache.read_with_fetch(7 * 4096, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 7, "block 7 should have been prefetched");

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.s3_read_ops, 1, "only one S3 fetch");
        assert_eq!(snapshot.cache_misses, 3, "3 blocks were missing initially");
        assert_eq!(snapshot.cache_hits, 1, "block 7 was a cache hit");
    }

    #[tokio::test]
    async fn test_max_deferrals_prevents_starvation() {
        // Verify that blocks aren't deferred forever when their batch
        // keeps getting updated (hot batch scenario).
        //
        // Strategy:
        // 1. Write and sync to establish the batch as "hot"
        // 2. Write again while batch is hot
        // 3. Verify that with max_deferrals, the block eventually syncs
        //    even though the batch remains "hot"

        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = Arc::new(test_s3());

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.finish_recovery(&s3).await.unwrap());

        // Write initial data and manually sync to establish batch 0 as "hot"
        cache.write(0, &[0x11u8; 100]).unwrap();
        let dirty = cache.claim_dirty_blocks(10);
        cache.sync_blocks_batched(&s3, dirty).await.unwrap();

        // Verify initial sync worked
        let s3_data = s3.read_block(0).await.unwrap();
        assert_eq!(s3_data[0], 0x11, "initial sync should succeed");

        // Now write again - batch 0 is "hot" from the sync we just did
        cache.write(0, &[0x22u8; 100]).unwrap();
        assert_eq!(cache.dirty_block_count(), 1);

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Start sync worker with very long cooldown (simulates perpetually hot batch)
        // but low max_deferrals to force sync quickly
        let worker_cache = Arc::clone(&cache);
        let worker_s3 = Arc::clone(&s3);
        let worker_handle = tokio::spawn(super::sync_worker(
            worker_cache,
            worker_s3,
            super::SyncWorkerConfig {
                batch_size: 100,
                max_concurrent_uploads: 1, // Sequential for predictable test behavior
                hot_batch_cooldown: std::time::Duration::from_secs(60), // Very long - batch stays hot
                max_deferrals: 3, // Force sync after 3 deferrals
                metadata_save_interval: 100,
                dirty_queue_warn_threshold: 0,
                dirty_queue_critical_threshold: 0,
            },
            shutdown_rx,
        ));

        // Wait for sync to complete - with max_deferrals=3, should happen within ~4 cycles
        // Each cycle is nearly instant since we're just deferring/syncing
        let start = std::time::Instant::now();
        while cache.dirty_block_count() > 0 && start.elapsed().as_millis() < 500 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Stop worker
        let _ = shutdown_tx.send(true);
        worker_handle.await.unwrap();

        // Block should have been synced despite the batch being "hot"
        assert_eq!(
            cache.dirty_block_count(),
            0,
            "max_deferrals should force sync even for hot batches"
        );

        // Verify updated data made it to S3
        let s3_data = s3.read_block(0).await.unwrap();
        assert_eq!(s3_data[0], 0x22, "updated data should be in S3");
    }

    #[tokio::test]
    async fn test_batch_prefetch_with_local_dirty_blocks() {
        // Verify that batch prefetching correctly handles the case where
        // some blocks are dirty locally and others need to be fetched from S3.
        // The local dirty blocks should NOT be overwritten by S3 data.

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
            .with_blocks_per_batch(10);
        let metrics = super::super::metrics::ExportMetrics::new();

        // Pre-populate S3 with old data
        let mut batch0 = vec![0u8; s3.batch_size()];
        batch0[0..4096].copy_from_slice(&vec![0xAAu8; 4096]); // block 0: old S3 data
        batch0[4096..8192].copy_from_slice(&vec![0xBBu8; 4096]); // block 1: old S3 data
        s3.put_batch(0, batch0).await.unwrap();

        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "dirty_test".to_string(),
            device_size: 10 * 4096,
            block_size: 4096,
        };
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write NEW data to block 0 locally (makes it dirty and present)
        cache.write(0, &[0xCCu8; 4096]).unwrap();

        // Now read blocks 0 and 1 together
        // Block 0 should come from local (dirty), block 1 should fetch from S3
        let data = cache.read_with_fetch(0, 8192, &s3, &metrics).await.unwrap();

        // Block 0 should have our local NEW data (not old S3 data)
        assert_eq!(data[0], 0xCC, "block 0 should have local data, not S3");

        // Block 1 should have S3 data (fetched)
        assert_eq!(data[4096], 0xBB, "block 1 should have S3 data");

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 1, "block 0 was local (hit)");
        assert_eq!(snapshot.cache_misses, 1, "block 1 was fetched (miss)");
    }

    #[tokio::test]
    async fn test_zero_block_skip_optimization() {
        // Verify that zero blocks are skipped during sync when S3 already has zeros.
        // This reduces S3 writes for fresh VMs and TRIM operations.

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = Arc::new(
            S3BlockStore::new(Arc::clone(&object_store), "test", 4096).with_blocks_per_batch(10),
        );
        let metrics = Arc::new(super::super::metrics::ExportMetrics::new());
        let s3_with_metrics = Arc::new(
            S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
                .with_blocks_per_batch(10)
                .with_metrics(Arc::clone(&metrics)),
        );

        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "zero_test".to_string(),
            device_size: 10 * 4096,
            block_size: 4096,
        };
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write zeros to block 0 (simulates TRIM or fresh VM)
        cache.write(0, &[0u8; 4096]).unwrap();
        assert_eq!(cache.dirty_block_count(), 1, "block should be dirty");

        // Sync - should skip the S3 write since block is zeros and S3 has no batch
        let dirty = cache.claim_dirty_blocks(10);
        assert_eq!(dirty.len(), 1);
        let synced = cache.sync_blocks_batched(&s3_with_metrics, dirty).await.unwrap();
        assert_eq!(synced, 1, "block should be marked synced");
        assert_eq!(cache.dirty_block_count(), 0, "no more dirty blocks");

        // Verify NO S3 write happened (zero block was skipped)
        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.batches_written, 0,
            "zero block should not trigger S3 write when S3 already zeros"
        );

        // Now write non-zero data and sync - this should write to S3
        cache.write(0, &[42u8; 4096]).unwrap();
        let dirty = cache.claim_dirty_blocks(10);
        cache.sync_blocks_batched(&s3_with_metrics, dirty).await.unwrap();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches_written, 1, "non-zero block should trigger S3 write");

        // Verify data in S3
        let batch = s3.get_batch_or_empty(0).await.unwrap();
        assert_eq!(batch[0], 42, "non-zero data should be in S3");

        // Now write zeros again - this time S3 has non-zero data
        // So zeros MUST be written to overwrite the old data
        cache.write(0, &[0u8; 4096]).unwrap();
        let dirty = cache.claim_dirty_blocks(10);
        cache.sync_blocks_batched(&s3_with_metrics, dirty).await.unwrap();

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.batches_written, 2,
            "zeros must be written to overwrite non-zero S3 data"
        );

        // Verify S3 now has zeros
        let batch = s3.get_batch_or_empty(0).await.unwrap();
        assert_eq!(batch[0], 0, "S3 should have zeros after overwrite");
    }

    #[test]
    fn test_is_zero_block() {
        // Test the is_zero_block helper function
        let zeros = vec![0u8; 4096];
        assert!(super::is_zero_block(&zeros), "all zeros should return true");

        let mut non_zeros = vec![0u8; 4096];
        non_zeros[0] = 1;
        assert!(!super::is_zero_block(&non_zeros), "first byte non-zero");

        non_zeros[0] = 0;
        non_zeros[4095] = 1;
        assert!(!super::is_zero_block(&non_zeros), "last byte non-zero");

        non_zeros[4095] = 0;
        non_zeros[2048] = 1;
        assert!(!super::is_zero_block(&non_zeros), "middle byte non-zero");

        // Empty slice
        assert!(super::is_zero_block(&[]), "empty slice is 'all zeros'");
    }

    #[tokio::test]
    async fn test_prefetch_write_race_data_integrity() {
        // Regression test for prefetch/write race condition.
        //
        // Without the fix, this sequence causes data loss:
        // 1. S3 has old data (0xAA)
        // 2. Write starts: pwrite(0xBB) completes
        // 3. Prefetch: sees is_present=false, fetches S3, pwrite(0xAA) OVERWRITES
        // 4. Write: set_present (too late)
        // 5. File now has 0xAA (stale), but marked dirty → syncs stale data
        //
        // With the fix (set_present before pwrite in write path):
        // - Write's set_present runs early, prefetch's CAS fails → prefetch skips
        // - OR prefetch CAS wins, but write's pwrite comes after → write wins

        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = Arc::new(
            S3BlockStore::new(Arc::clone(&object_store), "test", 4096).with_blocks_per_batch(10),
        );
        let metrics = super::super::metrics::ExportMetrics::new();

        // Pre-populate S3 with OLD data (0xAA)
        let mut batch0 = vec![0u8; s3.batch_size()];
        batch0[0..4096].copy_from_slice(&[0xAAu8; 4096]);
        s3.put_batch(0, batch0).await.unwrap();

        let dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "race_test".to_string(),
            device_size: 10 * 4096,
            block_size: 4096,
        };
        let cache = Arc::new(
            WriteCache::<Initializing>::open(config)
                .unwrap()
                .skip_recovery_for_test(),
        );

        // Use barriers to force a specific interleaving
        let write_started = Arc::new(AtomicBool::new(false));
        let prefetch_can_continue = Arc::new(AtomicBool::new(false));

        // Spawn concurrent tasks
        let cache_write = Arc::clone(&cache);
        let write_started_clone = Arc::clone(&write_started);
        let prefetch_can_continue_clone = Arc::clone(&prefetch_can_continue);

        let write_handle = tokio::spawn(async move {
            // Write NEW data (0xBB) - should win over stale S3 data
            cache_write.write(0, &[0xBBu8; 4096]).unwrap();
            write_started_clone.store(true, AtomicOrdering::Release);
            // Signal prefetch can continue
            prefetch_can_continue_clone.store(true, AtomicOrdering::Release);
        });

        let cache_read = Arc::clone(&cache);
        let s3_read = Arc::clone(&s3);
        let write_started_read = Arc::clone(&write_started);
        let _prefetch_can_continue_read = Arc::clone(&prefetch_can_continue);

        let read_handle = tokio::spawn(async move {
            // Wait until write has started (to maximize race window)
            while !write_started_read.load(AtomicOrdering::Acquire) {
                tokio::task::yield_now().await;
            }
            // Small delay to let write progress
            tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;

            // Now do the read which triggers prefetch
            cache_read
                .read_with_fetch(0, 4096, &s3_read, &metrics)
                .await
                .unwrap()
        });

        // Wait for both
        write_handle.await.unwrap();
        let _read_data = read_handle.await.unwrap();

        // The critical assertion: we must see the WRITE's data (0xBB), not S3's stale data (0xAA)
        //
        // Either:
        // - Write completed first, read sees 0xBB (write won)
        // - Prefetch completed first with 0xAA, but write overwrote it, read sees 0xBB (write won)
        // - Prefetch won and write hasn't completed yet, read sees 0xAA (acceptable during race)
        //   BUT: block is marked dirty, so sync will upload the final file contents
        //
        // What we CANNOT accept: file has 0xAA, read returns 0xAA, block is dirty,
        // sync uploads 0xAA (stale) - this is data loss.

        // Verify final file contents (the authoritative state)
        let final_data = cache.read_local(0, 4096).unwrap();

        // The file MUST have 0xBB (write's data). If it has 0xAA, the race caused data loss.
        assert_eq!(
            final_data[0], 0xBB,
            "RACE CONDITION BUG: write's data was overwritten by stale S3 prefetch! \
             File has 0x{:02X}, expected 0xBB",
            final_data[0]
        );

        // Also verify block is dirty (will sync the correct data)
        assert!(
            cache.dirty_block_count() > 0 || cache.syncing_block_count() > 0,
            "Block should be dirty to ensure write's data syncs to S3"
        );
    }

    #[tokio::test]
    async fn test_concurrent_write_and_prefetch_stress() {
        // Stress test: many concurrent writes and reads to maximize race probability.
        // Run multiple iterations to increase chance of hitting the race window.

        use std::sync::Arc;

        for iteration in 0..10 {
            let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let s3 = Arc::new(
                S3BlockStore::new(Arc::clone(&object_store), "test", 4096).with_blocks_per_batch(10),
            );
            let metrics = Arc::new(super::super::metrics::ExportMetrics::new());

            // Pre-populate S3 with old data for blocks 0-9
            let batch0 = vec![0xAAu8; s3.batch_size()];
            s3.put_batch(0, batch0).await.unwrap();

            let dir = TempDir::new().unwrap();
            let config = WriteCacheConfig {
                cache_dir: dir.path().to_path_buf(),
                device_name: format!("stress_{}", iteration),
                device_size: 10 * 4096,
                block_size: 4096,
            };
            let cache = Arc::new(
                WriteCache::<Initializing>::open(config)
                    .unwrap()
                    .skip_recovery_for_test(),
            );

            // Spawn many concurrent operations
            let mut handles = vec![];

            for block in 0..5u64 {
                let cache_w = Arc::clone(&cache);
                let write_val = (block + 1) as u8; // 1, 2, 3, 4, 5
                handles.push(tokio::spawn(async move {
                    cache_w.write(block * 4096, &[write_val; 4096]).unwrap();
                }));

                let cache_r = Arc::clone(&cache);
                let s3_r = Arc::clone(&s3);
                let metrics_r = Arc::clone(&metrics);
                handles.push(tokio::spawn(async move {
                    let _ = cache_r
                        .read_with_fetch(block * 4096, 4096, &s3_r, &metrics_r)
                        .await;
                }));
            }

            // Wait for all
            for h in handles {
                let _ = h.await;
            }

            // Verify: each block should have its write value, not 0xAA
            for block in 0..5u64 {
                let data = cache.read_local(block * 4096, 4096).unwrap();
                let expected = (block + 1) as u8;
                assert_eq!(
                    data[0], expected,
                    "Iteration {}, block {}: expected 0x{:02X}, got 0x{:02X} (0xAA = stale S3 data)",
                    iteration, block, expected, data[0]
                );
            }
        }
    }

    #[tokio::test]
    async fn test_mark_sync_failed_requeues_block() {
        // Regression test: mark_sync_failed must re-queue blocks for retry.
        //
        // Previously, mark_sync_failed would transition blocks from Syncing→Dirty
        // but NOT push them back onto the dirty_queue. This caused drain_for_snapshot
        // to complete prematurely, leaving data only in the local cache (not S3).
        //
        // This would manifest as "cannot import pool: one or more devices unavailable"
        // after NBD disconnect/reconnect because ZFS metadata never made it to S3.

        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write data to a block
        cache.write(0, b"important data").unwrap();
        assert_eq!(cache.dirty_block_count(), 1);

        // Claim the block (simulating sync worker starting)
        let claimed = cache.claim_dirty_blocks(100);
        assert_eq!(claimed.len(), 1);
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 1);

        // Simulate sync failure
        cache.mark_sync_failed(0);

        // Block should be back to dirty
        assert_eq!(cache.dirty_block_count(), 1);
        assert_eq!(cache.syncing_block_count(), 0);

        // CRITICAL: Block must be re-claimable (this was the bug)
        let reclaimed = cache.claim_dirty_blocks(100);
        assert_eq!(
            reclaimed.len(),
            1,
            "failed blocks must be re-queued for retry"
        );
        assert_eq!(reclaimed[0], 0);

        // Now mark as synced to complete the test
        cache.mark_synced(0);
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 0);
    }

    #[tokio::test]
    async fn test_drain_retries_after_sync_complete() {
        // Regression test: drain_for_snapshot must recheck dirty queue after
        // waiting for syncing blocks to complete.
        //
        // Previously, if blocks were re-queued (via mark_sync_failed) AFTER
        // drain's claim loop completed but BEFORE syncing_count reached 0,
        // those blocks would be missed. This could happen when:
        // 1. Background sync worker claims blocks
        // 2. drain_for_snapshot starts, finds queue empty (worker has them)
        // 3. drain waits for syncing_count == 0
        // 4. Worker's sync fails for some blocks → re-queued via mark_sync_failed
        // 5. Worker finishes, syncing_count = 0
        // 6. drain returns OK, but queue has dirty blocks!

        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write data
        cache.write(0, b"important data").unwrap();
        assert_eq!(cache.dirty_block_count(), 1);

        // Simulate background worker claiming the block
        let claimed = cache.claim_dirty_blocks(100);
        assert_eq!(claimed.len(), 1);
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 1);

        // Now simulate what happens when drain runs while worker is syncing:
        // drain would see empty queue and wait for syncing == 0

        // Simulate worker sync failure - block goes back to dirty
        cache.mark_sync_failed(0);
        assert_eq!(cache.dirty_block_count(), 1);
        assert_eq!(cache.syncing_block_count(), 0);

        // Now drain should pick up this block and sync it
        cache.drain_for_snapshot(&s3).await.unwrap();

        // After drain, no dirty or syncing blocks should remain
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 0);

        // Verify data made it to S3
        let s3_data = s3.read_block(0).await.unwrap();
        assert_eq!(&s3_data[..14], b"important data");
    }

    #[tokio::test]
    async fn test_partial_last_block_sync() {
        // Regression test: When device_size is not a multiple of block_size,
        // the last block is partial. sync_read_local_block must handle this
        // by reading only valid bytes and padding with zeros.
        //
        // This bug caused migration failures because the last partial block
        // (containing ZFS label L3) would fail to sync to S3, and the drain
        // would report success despite leaving data behind.
        //
        // Example: 2GB device (2,000,000,000 bytes) with 128KB blocks:
        // - num_blocks = 15,259
        // - last block starts at 1,999,998,208
        // - only 1,792 bytes are valid in last block

        let dir = TempDir::new().unwrap();

        // Use a device size that's NOT a multiple of block_size
        // 100 blocks + 1000 extra bytes = partial last block
        let block_size = 4096;
        let device_size = 100 * block_size as u64 + 1000; // 101 blocks, last is partial

        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "partial_test".to_string(),
            device_size,
            block_size,
        };

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", block_size)
            .with_blocks_per_batch(10);

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write data to the LAST (partial) block
        let last_block = 100u64; // block indices 0-100, so 101 blocks total
        let last_block_offset = last_block * block_size as u64;

        // Write 1000 bytes (all valid bytes in the partial block)
        let data = vec![0xABu8; 1000];
        cache.write(last_block_offset, &data).unwrap();

        assert_eq!(cache.dirty_block_count(), 1);

        // This is the critical test: sync_read_local_block must not fail
        // Previously it would try to read 4096 bytes starting at offset 409600
        // but the file is only 410600 bytes (100*4096 + 1000), so read_exact_at
        // would fail trying to read 4096 bytes when only 1000 are available.
        let block_data = cache.sync_read_local_block(last_block).unwrap();

        // Verify: first 1000 bytes should be our data, rest should be zeros
        assert_eq!(&block_data[..1000], &data[..]);
        assert!(block_data[1000..].iter().all(|&b| b == 0));

        // Now verify full drain works
        cache.drain_for_snapshot(&s3).await.unwrap();

        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 0);
    }

    // ====================================================================
    // v2 Test Harness + Flush Tests
    // ====================================================================

    /// Test harness for v2 content-addressed flush tests.
    ///
    /// Bundles the full v2 stack (cache + content store + pack index) in one
    /// struct so tests can focus on behavior, not setup boilerplate.
    struct V2Harness {
        cache: WriteCache<Active>,
        content_store: ContentStore,
        pack_index: HostPackIndex,
        clean_cache: super::super::cache::SimpleBlockCache,
        #[allow(dead_code)]
        dir: TempDir,
    }

    impl V2Harness {
        /// Create a harness with default 1MB device / 4KB blocks.
        async fn new() -> Self {
            Self::with_config(1024 * 1024, 4096).await
        }

        /// Create a harness with custom device size and block size.
        async fn with_config(device_size: u64, block_size: usize) -> Self {
            let dir = TempDir::new().unwrap();
            let config = WriteCacheConfig {
                cache_dir: dir.path().to_path_buf(),
                device_name: "test".to_string(),
                device_size,
                block_size,
            };
            let s3 = test_s3();
            let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let content_store = ContentStore::new(object_store, "test-bucket");
            let pack_index = HostPackIndex::new();
            let clean_cache = super::super::cache::SimpleBlockCache::new(64 * 1024 * 1024);
            let cache = WriteCache::<Initializing>::open(config).unwrap();
            let cache = cache.finish_recovery(&s3).await.unwrap();
            Self { cache, content_store, pack_index, clean_cache, dir }
        }

        /// Flush and return stats.
        async fn flush(&self) -> FlushStats {
            self.cache
                .flush_to_s3(&self.content_store, &self.pack_index)
                .await
                .unwrap()
        }

        /// v2 read through the full tiered resolution path.
        async fn read(&self, offset: u64, len: usize) -> Bytes {
            let metrics = super::super::metrics::ExportMetrics::new();
            self.cache
                .read_v2(
                    offset,
                    len,
                    &self.clean_cache,
                    &self.pack_index,
                    &self.content_store,
                    &metrics,
                )
                .await
                .unwrap()
        }

        /// Clear the dirty store (simulates blocks being evicted after flush).
        fn clear_dirty_store(&self) {
            self.cache.inner.dirty_store.lock().unwrap().clear();
        }

        /// Get the manifest from S3.
        async fn manifest(&self) -> Manifest {
            let bytes = self.content_store
                .get_manifest("test")
                .await
                .unwrap()
                .expect("manifest should exist");
            Manifest::deserialize(&bytes).unwrap()
        }
    }

    #[tokio::test]
    async fn test_flush_end_to_end() {
        let h = V2Harness::new().await;

        // Write 10 distinct blocks (each 4KB = block_size)
        for i in 0u8..10 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }

        let stats = h.flush().await;
        assert_eq!(stats.blocks_flushed, 10);
        assert_eq!(stats.blocks_deduped, 0);
        assert!(stats.packs_uploaded > 0);
        assert!(stats.bytes_uploaded > 0);

        let manifest = h.manifest().await;
        assert_eq!(manifest.name, "test");
        assert_eq!(manifest.chunk_size, 4096);
        assert_eq!(manifest.block_map.len(), 10);
        assert!(!manifest.pack_index.is_empty());
        assert!(h.pack_index.len() >= 10);
    }

    #[tokio::test]
    async fn test_flush_dedup_skips_existing() {
        let h = V2Harness::new().await;

        // Write 5 blocks
        for i in 0u8..5 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }

        let stats1 = h.flush().await;
        assert_eq!(stats1.blocks_flushed, 5);
        assert_eq!(stats1.blocks_deduped, 0);
        assert!(stats1.packs_uploaded > 0);

        // Write the same data again to new offsets — same content, new positions
        for i in 0u8..5 {
            h.cache.write((i as u64 + 5) * 4096, &vec![i + 1; 4096]).unwrap();
        }

        let stats2 = h.flush().await;
        assert_eq!(stats2.blocks_flushed, 5);
        assert_eq!(stats2.blocks_deduped, 5, "all blocks should be deduped");
        assert_eq!(stats2.packs_uploaded, 0, "no new packs needed");
    }

    #[tokio::test]
    async fn test_flush_partial_dedup() {
        let h = V2Harness::new().await;

        // Write 10 blocks with unique data
        for i in 0u8..10 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }

        let stats1 = h.flush().await;
        assert_eq!(stats1.blocks_flushed, 10);
        assert_eq!(stats1.blocks_deduped, 0);

        // Write 10 more blocks: 5 with SAME data as before (dedup), 5 with NEW data
        for i in 0u8..5 {
            h.cache.write((i as u64 + 10) * 4096, &vec![i + 1; 4096]).unwrap();
        }
        for i in 0u8..5 {
            h.cache.write((i as u64 + 15) * 4096, &vec![i + 100; 4096]).unwrap();
        }

        let stats2 = h.flush().await;
        assert_eq!(stats2.blocks_flushed, 10);
        assert_eq!(stats2.blocks_deduped, 5, "5 blocks should be deduped");
        assert_eq!(stats2.packs_uploaded, 1, "5 new blocks = 1 pack");
    }

    #[tokio::test]
    async fn test_flush_zero_blocks_skipped() {
        // Use 128KB blocks so ZERO_BLOCK_HASH matches
        let h = V2Harness::with_config(128 * 1024 * 4, 128 * 1024).await;

        // Write one real block
        h.cache.write(0, &vec![42u8; 128 * 1024]).unwrap();
        // Write a block of zeros — this should get ZERO_BLOCK_HASH
        h.cache.write(128 * 1024, &vec![0u8; 128 * 1024]).unwrap();

        let stats = h.flush().await;
        assert_eq!(stats.blocks_flushed, 2);
        assert_eq!(stats.blocks_deduped, 1, "zero block should be deduped");
        assert_eq!(stats.packs_uploaded, 1, "only one pack for the real block");
    }

    #[tokio::test]
    async fn test_flush_clears_dirty_state() {
        let h = V2Harness::new().await;

        for i in 0u8..3 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }

        assert!(h.cache.inner.dirty_store.lock().unwrap().len() >= 3);

        h.flush().await;

        assert_eq!(
            h.cache.inner.dirty_store.lock().unwrap().len(), 0,
            "dirty store should be empty after flush"
        );

        // A second flush should be a no-op
        let stats = h.flush().await;
        assert_eq!(stats.blocks_flushed, 0, "no dirty blocks after flush");
    }

    #[tokio::test]
    async fn test_flush_concurrent_write_stays_dirty() {
        let h = V2Harness::new().await;

        h.cache.write(0, &vec![1u8; 4096]).unwrap();
        h.flush().await;

        // Overwrite block 0 with different data
        h.cache.write(0, &vec![2u8; 4096]).unwrap();

        let stats = h.flush().await;
        assert_eq!(stats.blocks_flushed, 1, "overwritten block should be flushed");
    }

    #[tokio::test]
    async fn test_flush_manifest_self_contained() {
        let h = V2Harness::new().await;

        // Write 30 blocks (will produce 2 packs: 25 + 5)
        for i in 0u8..30 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }

        let stats = h.flush().await;
        assert_eq!(stats.packs_uploaded, 2, "should produce 2 packs (25+5)");

        let manifest = h.manifest().await;
        let pack_hashes: std::collections::HashSet<_> =
            manifest.pack_index.iter().map(|e| e.hash).collect();

        for bm_entry in &manifest.block_map {
            if !bm_entry.hash.is_zero() {
                assert!(
                    pack_hashes.contains(&bm_entry.hash),
                    "block map hash {:?} should have pack index entry",
                    bm_entry.hash
                );
            }
        }
    }

    // ====================================================================
    // v2 Read Path Tests
    // ====================================================================

    #[tokio::test]
    async fn test_v2_read_from_dirty_store() {
        let h = V2Harness::new().await;
        let data = vec![0xAAu8; 4096];
        h.cache.write(0, &data).unwrap();

        let got = h.read(0, 4096).await;
        assert_eq!(got.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_v2_read_zero_block() {
        let h = V2Harness::new().await;

        // Never written — block_map entry is Blake3Hash::ZERO → returns zeros.
        let got = h.read(0, 4096).await;
        assert!(got.iter().all(|&b| b == 0), "unwritten block should be all zeros");
    }

    #[tokio::test]
    async fn test_v2_read_trimmed_block() {
        let h = V2Harness::new().await;

        // Write a block, then zero it out.
        h.cache.write(0, &vec![0xBBu8; 4096]).unwrap();
        h.cache.zero_range(0, 4096).unwrap();

        let got = h.read(0, 4096).await;
        assert!(got.iter().all(|&b| b == 0), "trimmed block should be all zeros");
    }

    #[tokio::test]
    async fn test_v2_read_sub_chunk() {
        let h = V2Harness::new().await;
        let data = vec![0xCCu8; 4096];
        h.cache.write(0, &data).unwrap();

        // Read 100 bytes from offset 1000 within the chunk.
        let got = h.read(1000, 100).await;
        assert_eq!(got.len(), 100);
        assert!(got.iter().all(|&b| b == 0xCC));
    }

    #[tokio::test]
    async fn test_v2_read_spans_chunks() {
        let h = V2Harness::new().await;

        // Write two distinct chunks.
        h.cache.write(0, &vec![0x11u8; 4096]).unwrap();
        h.cache.write(4096, &vec![0x22u8; 4096]).unwrap();

        // Read across the chunk boundary: last 100 bytes of chunk 0 + first 100 of chunk 1.
        let got = h.read(3996, 200).await;
        assert_eq!(got.len(), 200);
        assert!(got[..100].iter().all(|&b| b == 0x11), "first 100 bytes from chunk 0");
        assert!(got[100..].iter().all(|&b| b == 0x22), "last 100 bytes from chunk 1");
    }

    #[tokio::test]
    async fn test_v2_read_from_s3_pack() {
        let h = V2Harness::new().await;

        // Write blocks, flush to S3, clear dirty store.
        for i in 0u8..5 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }
        h.flush().await;
        h.clear_dirty_store();

        // Reads should now resolve through: block_map → (miss dirty) → (miss cache) → S3 pack.
        for i in 0u8..5 {
            let got = h.read(i as u64 * 4096, 4096).await;
            assert!(
                got.iter().all(|&b| b == i + 1),
                "block {} should contain 0x{:02x}",
                i,
                i + 1
            );
        }
    }

    #[tokio::test]
    async fn test_v2_pack_prefetch_warms_siblings() {
        let h = V2Harness::new().await;

        // Write 25 blocks (1 full pack).
        for i in 0u8..25 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }
        h.flush().await;
        h.clear_dirty_store();

        // Read block 0 — triggers pack fetch, caches all 25 blocks.
        let got = h.read(0, 4096).await;
        assert!(got.iter().all(|&b| b == 1));

        // Blocks 1-24 should now be clean_cache hits (no additional S3 fetch).
        for i in 1u8..25 {
            let got = h.read(i as u64 * 4096, 4096).await;
            assert!(
                got.iter().all(|&b| b == i + 1),
                "sibling block {} should be cached from pack prefetch",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_v2_mixed_dirty_and_clean_reads() {
        let h = V2Harness::new().await;

        // Write 5 blocks and flush to S3 (they'll become "clean" once evicted from dirty).
        for i in 0u8..5 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }
        h.flush().await;
        h.clear_dirty_store();

        // Write 5 more blocks (dirty, not flushed).
        for i in 5u8..10 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }

        // Read all 10 blocks. First 5 from S3, last 5 from dirty_store.
        for i in 0u8..10 {
            let got = h.read(i as u64 * 4096, 4096).await;
            assert!(
                got.iter().all(|&b| b == i + 1),
                "block {} (source: {}) should contain 0x{:02x}",
                i,
                if i < 5 { "S3" } else { "dirty" },
                i + 1
            );
        }
    }

    #[tokio::test]
    async fn test_v2_clean_cache_eviction() {
        use super::super::cache::{BlockCache, SimpleBlockCache};
        use super::super::block_map::blake3_128;

        // Budget: 3 blocks of 4096 bytes.
        let cache = SimpleBlockCache::new(3 * 4096);

        for i in 0u8..5 {
            let hash = blake3_128(&vec![i; 4096]);
            cache.insert(hash, Bytes::from(vec![i; 4096]));
        }

        // Oldest 2 entries should have been evicted.
        let h0 = blake3_128(&vec![0u8; 4096]);
        let h1 = blake3_128(&vec![1u8; 4096]);
        let h4 = blake3_128(&vec![4u8; 4096]);

        assert!(cache.get(&h0).await.is_none(), "oldest should be evicted");
        assert!(cache.get(&h1).await.is_none(), "second oldest should be evicted");
        assert!(cache.get(&h4).await.is_some(), "newest should be present");
    }

    // ========================================================================
    // Snapshot tests
    // ========================================================================

    #[tokio::test]
    async fn test_snapshot_returns_sequence_and_stats() {
        let h = V2Harness::new().await;

        // Write 5 blocks
        for i in 0u8..5 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }

        let result = h.cache
            .snapshot(&h.content_store, &h.pack_index)
            .await
            .unwrap();

        assert!(result.sequence > 0, "sequence should be > 0 after writes");
        assert_eq!(result.stats.blocks_flushed, 5);
        assert!(result.stats.packs_uploaded > 0);

        // Manifest should exist in S3
        let manifest = h.manifest().await;
        assert_eq!(manifest.name, "test");
        assert_eq!(manifest.block_map.len(), 5);
    }

    #[tokio::test]
    async fn test_snapshot_clears_dirty_state() {
        let h = V2Harness::new().await;

        // Write blocks and snapshot
        for i in 0u8..3 {
            h.cache.write(i as u64 * 4096, &vec![i + 1; 4096]).unwrap();
        }

        let result1 = h.cache
            .snapshot(&h.content_store, &h.pack_index)
            .await
            .unwrap();
        assert_eq!(result1.stats.blocks_flushed, 3);

        // Second snapshot with no new writes should be a no-op
        let result2 = h.cache
            .snapshot(&h.content_store, &h.pack_index)
            .await
            .unwrap();
        assert_eq!(result2.stats.blocks_flushed, 0, "no dirty blocks after snapshot");
    }

    #[tokio::test]
    async fn test_snapshot_captures_concurrent_writes() {
        let h = V2Harness::new().await;

        // Write blocks at different times
        h.cache.write(0, &vec![0xAA; 4096]).unwrap();
        h.cache.write(4096, &vec![0xBB; 4096]).unwrap();

        let result = h.cache
            .snapshot(&h.content_store, &h.pack_index)
            .await
            .unwrap();
        assert_eq!(result.stats.blocks_flushed, 2);

        // Write more after snapshot
        h.cache.write(8192, &vec![0xCC; 4096]).unwrap();

        // Manifest from first snapshot should have 2 blocks, not 3
        let manifest = h.manifest().await;
        assert_eq!(manifest.block_map.len(), 2);

        // Second snapshot picks up the new write
        let result2 = h.cache
            .snapshot(&h.content_store, &h.pack_index)
            .await
            .unwrap();
        assert_eq!(result2.stats.blocks_flushed, 1);
    }

    // ========================================================================
    // open_from_manifest tests
    // ========================================================================

    fn make_test_manifest(num_entries: u64, device_size: u64, block_size: u32) -> Manifest {
        use super::super::manifest::ManifestBlockEntry;
        let block_map: Vec<ManifestBlockEntry> = (0..num_entries)
            .map(|i| ManifestBlockEntry {
                chunk_index: i,
                hash: blake3_128(format!("block-{i}").as_bytes()),
                flags: 0,
            })
            .collect();
        Manifest {
            name: "test-fork".to_string(),
            sequence: 42,
            chunk_size: block_size,
            device_size,
            block_map,
            pack_index: vec![],
        }
    }

    #[test]
    fn test_open_from_manifest_creates_clean_cache() {
        let dir = TempDir::new().unwrap();
        let device_size: u64 = 1024 * 1024; // 1MB
        let block_size: usize = 4096;
        let manifest = make_test_manifest(5, device_size, block_size as u32);

        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test-manifest".to_string(),
            device_size,
            block_size,
        };

        let cache = WriteCache::<Initializing>::open_from_manifest(
            config,
            &manifest,
            None,
        )
        .unwrap();

        // No dirty blocks -- everything is in S3
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 0);

        // Sequence should match the manifest
        assert_eq!(cache.inner.sequence.current(), 42);

        // Block map should have the 5 entries from the manifest
        for i in 0u64..5 {
            let (hash, seq) = cache.inner.block_map_get(i as usize);
            let expected_hash = blake3_128(format!("block-{i}").as_bytes());
            assert_eq!(hash, expected_hash, "block map entry {i} hash mismatch");
            assert_eq!(seq, 42, "block map entry {i} sequence mismatch");
        }

        // Entries beyond the manifest should be empty (zero hash)
        let (hash, seq) = cache.inner.block_map_get(5);
        assert!(hash.is_zero(), "entry 5 should be empty");
        assert_eq!(seq, 0);

        // No blocks present locally
        assert_eq!(cache.inner.count_present(), 0);
    }

    #[test]
    fn test_open_from_manifest_with_forked_overlay() {
        let dir = TempDir::new().unwrap();
        let device_size: u64 = 1024 * 1024;
        let block_size: usize = 4096;
        let manifest = make_test_manifest(5, device_size, block_size as u32);

        // Build a parent BlockMap from the manifest entries
        let mut parent_bm = BlockMap::new(device_size, block_size as u32);
        for entry in &manifest.block_map {
            parent_bm.set(
                entry.chunk_index as usize,
                BlockMapEntry {
                    hash: entry.hash,
                    flags: 0,
                    sequence: manifest.sequence,
                },
            );
        }
        let parent = Arc::new(parent_bm);

        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test-forked".to_string(),
            device_size,
            block_size,
        };

        let cache = WriteCache::<Initializing>::open_from_manifest(
            config,
            &manifest,
            Some(Arc::clone(&parent)),
        )
        .unwrap();

        // Verify the block map is the Forked variant
        {
            let bm = cache.inner.block_map.read().unwrap();
            assert!(
                matches!(&*bm, BlockMapKind::Forked(_)),
                "block map should be Forked variant"
            );
        }

        // Reads should resolve from the parent
        for i in 0u64..5 {
            let (hash, seq) = cache.inner.block_map_get(i as usize);
            let expected_hash = blake3_128(format!("block-{i}").as_bytes());
            assert_eq!(hash, expected_hash, "forked entry {i} hash mismatch");
            assert_eq!(seq, 42, "forked entry {i} sequence mismatch");
        }

        // No dirty blocks
        assert_eq!(cache.dirty_block_count(), 0);
    }

    #[test]
    fn test_open_from_manifest_fork_writes_to_overlay() {
        let dir = TempDir::new().unwrap();
        let device_size: u64 = 1024 * 1024;
        let block_size: usize = 4096;
        let manifest = make_test_manifest(5, device_size, block_size as u32);

        // Build parent from manifest
        let mut parent_bm = BlockMap::new(device_size, block_size as u32);
        for entry in &manifest.block_map {
            parent_bm.set(
                entry.chunk_index as usize,
                BlockMapEntry {
                    hash: entry.hash,
                    flags: 0,
                    sequence: manifest.sequence,
                },
            );
        }
        let parent = Arc::new(parent_bm);

        let config = WriteCacheConfig {
            cache_dir: dir.path().to_path_buf(),
            device_name: "test-fork-write".to_string(),
            device_size,
            block_size,
        };

        let cache = WriteCache::<Initializing>::open_from_manifest(
            config,
            &manifest,
            Some(Arc::clone(&parent)),
        )
        .unwrap();

        // Write new data to chunk 0 -- should go to the overlay
        let new_data = vec![0xAB; block_size];
        cache.write(0, &new_data).unwrap();

        // The overlay should have grown
        {
            let bm = cache.inner.block_map.read().unwrap();
            if let BlockMapKind::Forked(f) = &*bm {
                assert_eq!(
                    f.overlay_len(),
                    1,
                    "overlay should have 1 entry after write"
                );
            } else {
                panic!("block map should still be Forked variant");
            }
        }

        // The written chunk should have a new hash (not the parent's hash)
        let (hash, seq) = cache.inner.block_map_get(0);
        let parent_hash = blake3_128("block-0".as_bytes());
        assert_ne!(hash, parent_hash, "hash should differ from parent after write");
        assert!(seq > 42, "sequence should be beyond the manifest sequence");

        // Parent's entry at chunk 0 should be unchanged
        let parent_entry = parent.get(0);
        assert_eq!(parent_entry.hash, parent_hash, "parent must be unmodified");
        assert_eq!(parent_entry.sequence, 42, "parent sequence must be unmodified");

        // Chunk 1 should still read from parent (not in overlay)
        let (hash_1, seq_1) = cache.inner.block_map_get(1);
        let expected_hash_1 = blake3_128("block-1".as_bytes());
        assert_eq!(hash_1, expected_hash_1, "unwritten chunk should read from parent");
        assert_eq!(seq_1, 42);

        // Should have 1 dirty block
        assert_eq!(cache.dirty_block_count(), 1);
    }
}

// Loom concurrency tests are in a separate crate: glidefs/loom-tests/
// This is necessary because loom disables tokio's networking which breaks
// dependencies like hyper-util. Run loom tests with:
//   cd loom-tests && cargo test --release
