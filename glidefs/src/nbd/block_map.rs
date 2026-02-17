//! Content-addressed block map for the v2 write path.
//!
//! This module provides:
//! - `Blake3Hash`: 16-byte truncated BLAKE3 hash for content addressing
//! - `BlockMapEntry`: Per-chunk metadata (hash, flags, sequence)
//! - `BlockMap`: Dense array for serialization and persistence
//! - `AtomicBlockMap`: Lock-free runtime block map using parallel atomic arrays
//! - `SequenceNumber`: Monotonic counter for snapshot consistency
//! - LZ4 compress/decompress helpers

use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{self, Read, Write as IoWrite};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

// ============================================================================
// Blake3Hash -- 16-byte truncated content hash
// ============================================================================

/// A 16-byte truncated BLAKE3 hash used for content addressing.
///
/// We truncate to 128 bits because:
/// - 128-bit collision resistance is sufficient for content deduplication
/// - 16 bytes fits in two u64s for lock-free atomic storage
/// - Halves per-entry metadata cost vs full 256-bit hash
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Blake3Hash(pub(crate) [u8; 16]);

impl Blake3Hash {
    /// Sentinel value for entries that were never populated.
    pub const ZERO: Blake3Hash = Blake3Hash([0u8; 16]);

    /// Construct from raw bytes.
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Blake3Hash(bytes)
    }

    /// Borrow the underlying bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Returns true if this is the zero sentinel (entry never populated).
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 16]
    }
}

impl fmt::Debug for Blake3Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "blake3:{:02x}{:02x}{:02x}{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

/// Compute a 128-bit (16-byte) BLAKE3 hash of the given data.
#[inline]
pub fn blake3_128(data: &[u8]) -> Blake3Hash {
    let full = blake3::hash(data);
    let bytes = full.as_bytes();
    let mut truncated = [0u8; 16];
    truncated.copy_from_slice(&bytes[..16]);
    Blake3Hash(truncated)
}

/// Compute the well-known zero-block hash for a given block size.
///
/// This is the BLAKE3-128 hash of `block_size` zero bytes. Used by the write cache
/// to identify trimmed/unwritten chunks for dedup (zero blocks are never uploaded).
pub fn zero_block_hash(block_size: usize) -> Blake3Hash {
    blake3_128(&vec![0u8; block_size])
}


// ============================================================================
// BlockMapEntry -- per-chunk metadata
// ============================================================================

/// Per-chunk metadata stored in the block map.
///
/// This is a plain value type used for serialization and snapshot transfer.
/// The runtime hot path uses `AtomicBlockMap` instead.
#[derive(Clone, Copy)]
pub struct BlockMapEntry {
    pub hash: Blake3Hash,
    pub flags: u8,
    pub sequence: u64,
}

impl BlockMapEntry {
    /// Sentinel for an empty (never-written) entry.
    pub const EMPTY: BlockMapEntry = BlockMapEntry {
        hash: Blake3Hash::ZERO,
        flags: 0,
        sequence: 0,
    };

    /// Flag bit: block has local data not yet flushed to S3.
    pub const FLAG_DIRTY: u8 = 0x01;

    /// Returns true if this entry was never populated.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.hash.is_zero()
    }

    /// Returns true if this block has unflushed local data.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.flags & Self::FLAG_DIRTY != 0
    }

    /// Mark this block as dirty (unflushed).
    #[inline]
    pub fn set_dirty(&mut self) {
        self.flags |= Self::FLAG_DIRTY;
    }

    /// Clear the dirty flag (block has been flushed to S3).
    #[inline]
    pub fn clear_dirty(&mut self) {
        self.flags &= !Self::FLAG_DIRTY;
    }
}

impl Default for BlockMapEntry {
    #[inline]
    fn default() -> Self {
        Self::EMPTY
    }
}

impl fmt::Debug for BlockMapEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockMapEntry")
            .field("hash", &self.hash)
            .field("flags", &format_args!("0x{:02x}", self.flags))
            .field("sequence", &self.sequence)
            .finish()
    }
}

// ============================================================================
// AtomicBlockMap -- lock-free runtime block map
// ============================================================================

/// Lock-free runtime block map using atomic arrays with SeqLock protection.
///
/// Each chunk's 16-byte hash is stored as two `AtomicU64` values (lo and hi
/// halves), guarded by a per-entry `AtomicU32` version counter (SeqLock).
/// Writers increment the version to odd before updating, then to even after.
/// Readers spin-retry if the version is odd or changed between reads.
/// This guarantees readers always see a consistent (hash, sequence) triple
/// with no torn reads.
///
/// The flags byte is NOT stored here — it lives in v1's
/// `block_states: Box<[AtomicU8]>` in `CacheInner`.
pub struct AtomicBlockMap {
    versions: Box<[AtomicU32]>,
    hash_lo: Box<[AtomicU64]>,
    hash_hi: Box<[AtomicU64]>,
    sequences: Box<[AtomicU64]>,
    num_chunks: usize,
    chunk_size: u32,
    device_size: u64,
}

impl AtomicBlockMap {
    /// Allocate a new block map with all entries zeroed.
    pub fn new(device_size: u64, chunk_size: u32) -> Self {
        let num_chunks = device_size.div_ceil(chunk_size as u64) as usize;
        let versions = (0..num_chunks)
            .map(|_| AtomicU32::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let hash_lo = (0..num_chunks)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let hash_hi = (0..num_chunks)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let sequences = (0..num_chunks)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        AtomicBlockMap {
            versions,
            hash_lo,
            hash_hi,
            sequences,
            num_chunks,
            chunk_size,
            device_size,
        }
    }

    /// Number of chunk slots.
    #[inline]
    pub fn len(&self) -> usize {
        self.num_chunks
    }

    /// Returns true if there are no chunk slots.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_chunks == 0
    }

    /// Load the hash and sequence for a chunk (lock-free, SeqLock-protected).
    ///
    /// Spin-retries if the writer is mid-update (version is odd) or if the
    /// version changed between reading the hash halves. In practice, contention
    /// is near-zero because each chunk has its own version counter and writes
    /// are sub-microsecond.
    #[inline]
    pub fn get(&self, chunk_index: usize) -> (Blake3Hash, u64) {
        loop {
            let v1 = self.versions[chunk_index].load(Ordering::Acquire);
            if v1 & 1 != 0 {
                // Writer in progress — spin.
                std::hint::spin_loop();
                continue;
            }
            let lo = self.hash_lo[chunk_index].load(Ordering::Relaxed);
            let hi = self.hash_hi[chunk_index].load(Ordering::Relaxed);
            let seq = self.sequences[chunk_index].load(Ordering::Relaxed);
            // Acquire fence: ensures all data loads above complete before the
            // v2 check below. Without this, ARM could reorder data loads after
            // the v2 load, causing torn reads even when v1 == v2.
            std::sync::atomic::fence(Ordering::Acquire);
            let v2 = self.versions[chunk_index].load(Ordering::Relaxed);
            if v1 == v2 {
                let mut bytes = [0u8; 16];
                bytes[..8].copy_from_slice(&lo.to_le_bytes());
                bytes[8..].copy_from_slice(&hi.to_le_bytes());
                return (Blake3Hash(bytes), seq);
            }
            // Version changed — retry.
            std::hint::spin_loop();
        }
    }

    /// Store the hash and sequence for a chunk (lock-free, SeqLock-protected).
    #[inline]
    pub fn set(&self, chunk_index: usize, hash: Blake3Hash, sequence: u64) {
        let lo = u64::from_le_bytes(hash.0[..8].try_into().unwrap());
        let hi = u64::from_le_bytes(hash.0[8..].try_into().unwrap());
        // Increment version to odd (signals write in progress). AcqRel ensures
        // the subsequent data stores cannot be reordered before this increment
        // on weakly-ordered architectures (ARM/Graviton).
        self.versions[chunk_index].fetch_add(1, Ordering::AcqRel);
        self.hash_lo[chunk_index].store(lo, Ordering::Relaxed);
        self.hash_hi[chunk_index].store(hi, Ordering::Relaxed);
        self.sequences[chunk_index].store(sequence, Ordering::Relaxed);
        // Increment version to even (signals write complete). Release ensures
        // all data stores above are visible before the version goes even.
        self.versions[chunk_index].fetch_add(1, Ordering::Release);
    }

    /// Produce a `BlockMap` snapshot by combining atomic hash/sequence data
    /// with flags from the external `block_states` array.
    pub fn snapshot(&self, flags: &[AtomicU8]) -> BlockMap {
        let mut entries = Vec::with_capacity(self.num_chunks);
        for i in 0..self.num_chunks {
            let (hash, sequence) = self.get(i);
            let flag = flags[i].load(Ordering::Acquire);
            entries.push(BlockMapEntry {
                hash,
                flags: flag,
                sequence,
            });
        }
        BlockMap {
            entries,
            chunk_size: self.chunk_size,
            device_size: self.device_size,
        }
    }

    /// Construct from a deserialized `BlockMap`.
    pub fn from_block_map(bm: &BlockMap) -> Self {
        let num_chunks = bm.entries.len();
        let mut ver_vec = Vec::with_capacity(num_chunks);
        let mut lo_vec = Vec::with_capacity(num_chunks);
        let mut hi_vec = Vec::with_capacity(num_chunks);
        let mut seq_vec = Vec::with_capacity(num_chunks);

        for entry in &bm.entries {
            let lo = u64::from_le_bytes(entry.hash.0[..8].try_into().unwrap());
            let hi = u64::from_le_bytes(entry.hash.0[8..].try_into().unwrap());
            ver_vec.push(AtomicU32::new(0));
            lo_vec.push(AtomicU64::new(lo));
            hi_vec.push(AtomicU64::new(hi));
            seq_vec.push(AtomicU64::new(entry.sequence));
        }

        AtomicBlockMap {
            versions: ver_vec.into_boxed_slice(),
            hash_lo: lo_vec.into_boxed_slice(),
            hash_hi: hi_vec.into_boxed_slice(),
            sequences: seq_vec.into_boxed_slice(),
            num_chunks,
            chunk_size: bm.chunk_size,
            device_size: bm.device_size,
        }
    }
}

// ============================================================================
// BlockMap -- dense array for serialization/persistence
// ============================================================================

/// File header magic bytes.
const BLOCK_MAP_MAGIC: &[u8; 4] = b"GLBM";
/// File format version.
const BLOCK_MAP_VERSION: u16 = 1;

/// Dense block map used for serialization and persistence.
///
/// In memory this is a plain `Vec<BlockMapEntry>` indexed by chunk number.
/// On disk it uses a sparse representation: only non-empty entries are written,
/// keeping file size proportional to actual data rather than device size.
pub struct BlockMap {
    entries: Vec<BlockMapEntry>,
    chunk_size: u32,
    device_size: u64,
}

impl BlockMap {
    /// Allocate a new block map with all entries set to EMPTY.
    pub fn new(device_size: u64, chunk_size: u32) -> Self {
        let num_chunks = device_size.div_ceil(chunk_size as u64) as usize;
        BlockMap {
            entries: vec![BlockMapEntry::EMPTY; num_chunks],
            chunk_size,
            device_size,
        }
    }

    /// Number of chunk slots.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if there are no chunk slots.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Read the entry at a chunk index.
    #[inline]
    pub fn get(&self, chunk_index: usize) -> &BlockMapEntry {
        &self.entries[chunk_index]
    }

    /// Write an entry at a chunk index.
    #[inline]
    pub fn set(&mut self, chunk_index: usize, entry: BlockMapEntry) {
        self.entries[chunk_index] = entry;
    }

    /// Iterate over non-empty entries as (chunk_index, &entry).
    pub fn iter_non_empty(&self) -> impl Iterator<Item = (usize, &BlockMapEntry)> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, e)| !e.is_empty())
    }

    /// Count of non-empty entries.
    pub fn non_empty_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.is_empty()).count()
    }

    /// Highest sequence number across all entries.
    pub fn max_sequence(&self) -> u64 {
        self.entries.iter().map(|e| e.sequence).max().unwrap_or(0)
    }

    /// Chunk size in bytes.
    #[inline]
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Device size in bytes.
    #[inline]
    pub fn device_size(&self) -> u64 {
        self.device_size
    }

    /// Persist to disk atomically using a tempfile + rename.
    ///
    /// File format (sparse):
    ///   Header: "GLBM" (4) + version u16 LE + chunk_size u32 LE
    ///           + device_size u64 LE + entry_count u64 LE
    ///   Entries: for each non-empty: chunk_index u64 LE + hash [u8;16] + flags u8
    pub fn persist_to_file(&self, path: &Path) -> io::Result<()> {
        let dir = path.parent().unwrap_or(Path::new("."));
        let mut tmp = tempfile::NamedTempFile::new_in(dir)?;

        // Header
        tmp.write_all(BLOCK_MAP_MAGIC)?;
        tmp.write_all(&BLOCK_MAP_VERSION.to_le_bytes())?;
        tmp.write_all(&self.chunk_size.to_le_bytes())?;
        tmp.write_all(&self.device_size.to_le_bytes())?;

        let non_empty: Vec<(usize, &BlockMapEntry)> = self.iter_non_empty().collect();
        tmp.write_all(&(non_empty.len() as u64).to_le_bytes())?;

        // Sparse entries
        for (idx, entry) in &non_empty {
            tmp.write_all(&(*idx as u64).to_le_bytes())?;
            tmp.write_all(entry.hash.as_bytes())?;
            tmp.write_all(&[entry.flags])?;
        }

        tmp.flush()?;
        tmp.as_file().sync_all()?;
        tmp.persist(path).map_err(|e| e.error)?;
        Ok(())
    }

    /// Load from a previously persisted file.
    pub fn load_from_file(path: &Path) -> io::Result<Self> {
        let mut file = std::fs::File::open(path)?;

        // Read header
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != BLOCK_MAP_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid block map magic",
            ));
        }

        let mut version_buf = [0u8; 2];
        file.read_exact(&mut version_buf)?;
        let version = u16::from_le_bytes(version_buf);
        if version != BLOCK_MAP_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported block map version: {version}"),
            ));
        }

        let mut chunk_size_buf = [0u8; 4];
        file.read_exact(&mut chunk_size_buf)?;
        let chunk_size = u32::from_le_bytes(chunk_size_buf);

        let mut device_size_buf = [0u8; 8];
        file.read_exact(&mut device_size_buf)?;
        let device_size = u64::from_le_bytes(device_size_buf);

        let mut entry_count_buf = [0u8; 8];
        file.read_exact(&mut entry_count_buf)?;
        let entry_count = u64::from_le_bytes(entry_count_buf) as usize;

        // Reconstruct dense array
        let num_chunks = device_size.div_ceil(chunk_size as u64) as usize;
        let mut entries = vec![BlockMapEntry::EMPTY; num_chunks];

        // Per-entry buffer: chunk_index(8) + hash(16) + flags(1) = 25 bytes
        let mut entry_buf = [0u8; 25];
        for _ in 0..entry_count {
            file.read_exact(&mut entry_buf)?;
            let chunk_index =
                u64::from_le_bytes(entry_buf[..8].try_into().unwrap()) as usize;
            if chunk_index >= num_chunks {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "chunk index {chunk_index} out of bounds (max {num_chunks})"
                    ),
                ));
            }
            let mut hash_bytes = [0u8; 16];
            hash_bytes.copy_from_slice(&entry_buf[8..24]);
            let flags = entry_buf[24];
            entries[chunk_index] = BlockMapEntry {
                hash: Blake3Hash(hash_bytes),
                flags,
                sequence: 0, // Sequence not persisted in v1 sparse format
            };
        }

        Ok(BlockMap {
            entries,
            chunk_size,
            device_size,
        })
    }
}

// ============================================================================
// SequenceNumber -- monotonic counter
// ============================================================================

/// Monotonic sequence counter for write ordering and snapshot consistency.
///
/// Each write increments this counter and stores the resulting value in the
/// block map entry. A snapshot captures a sequence cut point; entries with
/// sequence <= cut point are included in the snapshot.
pub struct SequenceNumber(AtomicU64);

impl SequenceNumber {
    /// Create with an initial value.
    pub fn new(initial: u64) -> Self {
        SequenceNumber(AtomicU64::new(initial))
    }

    /// Atomically increment and return the new value.
    ///
    /// Uses Relaxed ordering because sequence numbers only need to be
    /// monotonically increasing, not synchronized with other memory.
    #[inline]
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Read the current value without incrementing.
    #[inline]
    pub fn current(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

// ============================================================================
// LZ4 helpers
// ============================================================================

/// Compress data with LZ4. Prepends the uncompressed size for self-describing frames.
#[inline]
pub fn lz4_compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}

/// Decompress LZ4 data (expects size-prepended format from `lz4_compress`).
#[inline]
pub fn lz4_decompress(
    compressed: &[u8],
) -> Result<Vec<u8>, lz4_flex::block::DecompressError> {
    lz4_flex::decompress_size_prepended(compressed)
}

// ============================================================================
// ForkedBlockMap -- overlay for forked VMs
// ============================================================================

/// Overlay block map for forked VMs.
///
/// Shares the parent's data via `Arc<BlockMap>` and stores only divergent
/// entries in a concurrent `DashMap`. Reads check the overlay first and fall
/// back to the parent. Writes always go to the overlay, never touching the
/// parent.
///
/// This allows many forks to share the same parent memory footprint, paying
/// only for the entries they actually modify.
pub struct ForkedBlockMap {
    parent: Arc<BlockMap>,
    overlay: DashMap<usize, (Blake3Hash, u64)>,
    num_chunks: usize,
    chunk_size: u32,
    device_size: u64,
}

impl ForkedBlockMap {
    /// Create from a parent block map. The overlay starts empty.
    pub fn new(parent: Arc<BlockMap>) -> Self {
        let num_chunks = parent.len();
        let chunk_size = parent.chunk_size();
        let device_size = parent.device_size();
        ForkedBlockMap {
            parent,
            overlay: DashMap::new(),
            num_chunks,
            chunk_size,
            device_size,
        }
    }

    /// Read: check overlay first, fall back to parent.
    #[inline]
    pub fn get(&self, chunk_index: usize) -> (Blake3Hash, u64) {
        if let Some(entry) = self.overlay.get(&chunk_index) {
            return *entry;
        }
        let entry = self.parent.get(chunk_index);
        (entry.hash, entry.sequence)
    }

    /// Write: inserts into overlay (never touches parent).
    #[inline]
    pub fn set(&self, chunk_index: usize, hash: Blake3Hash, sequence: u64) {
        self.overlay.insert(chunk_index, (hash, sequence));
    }

    /// Produce a `BlockMap` snapshot merging parent + overlay + external flags.
    ///
    /// For each chunk slot, the overlay entry takes precedence over the parent.
    /// Flags are loaded from the external `block_states` array with `Acquire`
    /// ordering.
    pub fn snapshot(&self, flags: &[AtomicU8]) -> BlockMap {
        let mut entries = Vec::with_capacity(self.num_chunks);
        for i in 0..self.num_chunks {
            let flag = flags[i].load(Ordering::Acquire);
            if let Some(ov) = self.overlay.get(&i) {
                let (hash, sequence) = *ov;
                entries.push(BlockMapEntry {
                    hash,
                    flags: flag,
                    sequence,
                });
            } else {
                let parent_entry = self.parent.get(i);
                entries.push(BlockMapEntry {
                    hash: parent_entry.hash,
                    flags: flag,
                    sequence: parent_entry.sequence,
                });
            }
        }
        BlockMap {
            entries,
            chunk_size: self.chunk_size,
            device_size: self.device_size,
        }
    }

    /// Number of entries in the overlay.
    #[inline]
    pub fn overlay_len(&self) -> usize {
        self.overlay.len()
    }

    /// True when the overlay exceeds 50% of parent entries -- should flatten.
    #[inline]
    pub fn should_flatten(&self) -> bool {
        self.overlay.len() > self.num_chunks / 2
    }

    /// Merge parent + overlay into a full `AtomicBlockMap`.
    ///
    /// The returned map has all parent entries with overlay entries applied on
    /// top. This is used when `should_flatten()` returns true to collapse the
    /// fork back into a flat runtime map.
    pub fn flatten(&self) -> AtomicBlockMap {
        let mut entries = Vec::with_capacity(self.num_chunks);
        for i in 0..self.num_chunks {
            if let Some(ov) = self.overlay.get(&i) {
                let (hash, sequence) = *ov;
                entries.push(BlockMapEntry {
                    hash,
                    flags: 0,
                    sequence,
                });
            } else {
                let parent_entry = self.parent.get(i);
                entries.push(BlockMapEntry {
                    hash: parent_entry.hash,
                    flags: 0,
                    sequence: parent_entry.sequence,
                });
            }
        }
        let merged = BlockMap {
            entries,
            chunk_size: self.chunk_size,
            device_size: self.device_size,
        };
        AtomicBlockMap::from_block_map(&merged)
    }

    /// Total number of chunk slots (same as parent).
    #[inline]
    pub fn len(&self) -> usize {
        self.num_chunks
    }

    /// Returns true if there are no chunk slots.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_chunks == 0
    }
}

// ============================================================================
// BlockMapKind -- unified dispatch over Full / Forked maps
// ============================================================================

/// Runtime block map that is either a full `AtomicBlockMap` or a forked
/// overlay. This avoids dynamic dispatch while keeping the call sites uniform.
pub enum BlockMapKind {
    Full(AtomicBlockMap),
    Forked(ForkedBlockMap),
}

impl BlockMapKind {
    /// Read hash and sequence for a chunk.
    #[inline]
    pub fn get(&self, chunk_index: usize) -> (Blake3Hash, u64) {
        match self {
            BlockMapKind::Full(m) => m.get(chunk_index),
            BlockMapKind::Forked(m) => m.get(chunk_index),
        }
    }

    /// Write hash and sequence for a chunk.
    #[inline]
    pub fn set(&self, chunk_index: usize, hash: Blake3Hash, sequence: u64) {
        match self {
            BlockMapKind::Full(m) => m.set(chunk_index, hash, sequence),
            BlockMapKind::Forked(m) => m.set(chunk_index, hash, sequence),
        }
    }

    /// Produce a `BlockMap` snapshot with external flags.
    pub fn snapshot(&self, flags: &[AtomicU8]) -> BlockMap {
        match self {
            BlockMapKind::Full(m) => m.snapshot(flags),
            BlockMapKind::Forked(m) => m.snapshot(flags),
        }
    }

    /// Number of chunk slots.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            BlockMapKind::Full(m) => m.len(),
            BlockMapKind::Forked(m) => m.len(),
        }
    }

    /// Returns true if there are no chunk slots.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;
    use std::time::Instant;
    use tempfile::TempDir;

    static ZERO_BLOCK_HASH_128K: LazyLock<Blake3Hash> =
        LazyLock::new(|| blake3_128(&[0u8; 131072]));

    #[test]
    fn test_blake3_deterministic() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let h1 = blake3_128(data);
        let h2 = blake3_128(data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_blake3_different_data() {
        let h1 = blake3_128(b"hello");
        let h2 = blake3_128(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_blake3_zero_block() {
        let zero_block = [0u8; 131072]; // 128KB
        let h = blake3_128(&zero_block);
        assert_eq!(h, *ZERO_BLOCK_HASH_128K);
        assert!(
            !h.is_zero(),
            "zero-block hash should not be the ZERO sentinel"
        );
    }

    #[test]
    fn test_blake3_performance() {
        let data = vec![0xABu8; 131072]; // 128KB non-zero
        // Warm up
        let _ = blake3_128(&data);

        let start = Instant::now();
        let iterations = 100;
        for _ in 0..iterations {
            std::hint::black_box(blake3_128(std::hint::black_box(&data)));
        }
        let elapsed = start.elapsed();
        let per_hash = elapsed / iterations;
        // Release builds: ~5us. Debug builds: much slower due to no inlining.
        // Use a generous threshold that catches catastrophic regressions.
        let max_us: u128 = if cfg!(debug_assertions) { 20_000 } else { 50 };
        assert!(
            per_hash.as_micros() < max_us,
            "blake3_128 took {}us per 128KB block, expected < {max_us}us",
            per_hash.as_micros()
        );
    }

    #[test]
    fn test_blake3_debug_format() {
        let h = Blake3Hash::from_bytes([
            0xa1, 0xb2, 0xc3, 0xd4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]);
        let dbg = format!("{:?}", h);
        assert_eq!(dbg, "blake3:a1b2c3d4");
    }

    #[test]
    fn test_blake3_zero_sentinel() {
        let z = Blake3Hash::ZERO;
        assert!(z.is_zero());
        assert_eq!(z.as_bytes(), &[0u8; 16]);
    }

    #[test]
    fn test_block_map_entry_default() {
        let entry = BlockMapEntry::default();
        assert!(entry.is_empty());
        assert!(!entry.is_dirty());
        assert_eq!(entry.sequence, 0);
        assert_eq!(entry.flags, 0);
    }

    #[test]
    fn test_block_map_entry_dirty_flag() {
        let mut entry = BlockMapEntry::EMPTY;
        assert!(!entry.is_dirty());

        entry.set_dirty();
        assert!(entry.is_dirty());
        assert_eq!(
            entry.flags & BlockMapEntry::FLAG_DIRTY,
            BlockMapEntry::FLAG_DIRTY
        );

        entry.clear_dirty();
        assert!(!entry.is_dirty());
        assert_eq!(entry.flags, 0);
    }

    #[test]
    fn test_block_map_entry_dirty_preserves_other_flags() {
        let mut entry = BlockMapEntry::EMPTY;
        entry.flags = 0xFE; // all bits except dirty
        assert!(!entry.is_dirty());

        entry.set_dirty();
        assert_eq!(entry.flags, 0xFF);

        entry.clear_dirty();
        assert_eq!(entry.flags, 0xFE);
    }

    #[test]
    fn test_block_map_insert_and_lookup() {
        let mut bm = BlockMap::new(1024 * 1024, 4096); // 1MB, 4KB chunks
        let hash = blake3_128(b"test data");
        let entry = BlockMapEntry {
            hash,
            flags: BlockMapEntry::FLAG_DIRTY,
            sequence: 7,
        };
        bm.set(42, entry);

        let got = bm.get(42);
        assert_eq!(got.hash, hash);
        assert!(got.is_dirty());
        assert_eq!(got.sequence, 7);
    }

    #[test]
    fn test_block_map_sparse_default() {
        let bm = BlockMap::new(1024 * 1024, 4096);
        let entry = bm.get(99);
        assert!(entry.is_empty());
        assert_eq!(entry.hash, Blake3Hash::ZERO);
    }

    #[test]
    fn test_block_map_overwrite() {
        let mut bm = BlockMap::new(1024 * 1024, 4096);
        let h1 = blake3_128(b"first");
        let h2 = blake3_128(b"second");

        bm.set(
            10,
            BlockMapEntry {
                hash: h1,
                flags: BlockMapEntry::FLAG_DIRTY,
                sequence: 1,
            },
        );
        bm.set(
            10,
            BlockMapEntry {
                hash: h2,
                flags: BlockMapEntry::FLAG_DIRTY,
                sequence: 2,
            },
        );

        let got = bm.get(10);
        assert_eq!(got.hash, h2);
        assert_eq!(got.sequence, 2);
    }

    #[test]
    fn test_block_map_persist_and_load() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.blkmap");

        let device_size: u64 = 10 * 1024 * 1024; // 10MB
        let chunk_size: u32 = 4096;
        let mut bm = BlockMap::new(device_size, chunk_size);

        // Write 1000 entries with distinct hashes
        for i in 0..1000 {
            let data = format!("block-{i}");
            let hash = blake3_128(data.as_bytes());
            bm.set(
                i,
                BlockMapEntry {
                    hash,
                    flags: if i % 3 == 0 {
                        BlockMapEntry::FLAG_DIRTY
                    } else {
                        0
                    },
                    sequence: 0, // sequence not persisted in v1 format
                },
            );
        }

        bm.persist_to_file(&path).unwrap();
        let loaded = BlockMap::load_from_file(&path).unwrap();

        assert_eq!(loaded.len(), bm.len());
        assert_eq!(loaded.chunk_size(), chunk_size);
        assert_eq!(loaded.device_size(), device_size);
        assert_eq!(loaded.non_empty_count(), 1000);

        for i in 0..1000 {
            let orig = bm.get(i);
            let got = loaded.get(i);
            assert_eq!(orig.hash, got.hash, "hash mismatch at index {i}");
            assert_eq!(orig.flags, got.flags, "flags mismatch at index {i}");
        }

        // Unwritten entries should still be empty
        assert!(loaded.get(1500).is_empty());
    }

    #[test]
    fn test_block_map_sparse_serialization() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sparse.blkmap");

        // Large device, only 3 entries written
        let device_size: u64 = 100 * 1024 * 1024; // 100MB
        let chunk_size: u32 = 4096;
        let mut bm = BlockMap::new(device_size, chunk_size);

        let h = blake3_128(b"sparse");
        let entry = BlockMapEntry {
            hash: h,
            flags: 0,
            sequence: 0,
        };
        bm.set(0, entry);
        bm.set(100, entry);
        bm.set(25000, entry);

        bm.persist_to_file(&path).unwrap();

        let file_size = std::fs::metadata(&path).unwrap().len();
        // Header: 4 + 2 + 4 + 8 + 8 = 26 bytes
        // Each entry: 8 + 16 + 1 = 25 bytes
        // Total: 26 + 3 * 25 = 101 bytes
        let expected = 26 + 3 * 25;
        assert_eq!(
            file_size, expected as u64,
            "file should be {expected} bytes for 3 sparse entries, got {file_size}"
        );

        // Verify round-trip
        let loaded = BlockMap::load_from_file(&path).unwrap();
        assert_eq!(loaded.non_empty_count(), 3);
        assert_eq!(loaded.get(0).hash, h);
        assert_eq!(loaded.get(100).hash, h);
        assert_eq!(loaded.get(25000).hash, h);
        assert!(loaded.get(1).is_empty());
    }

    #[test]
    fn test_block_map_sequence_tracking() {
        let mut bm = BlockMap::new(1024 * 1024, 4096);
        let h = blake3_128(b"data");

        bm.set(
            0,
            BlockMapEntry {
                hash: h,
                flags: 0,
                sequence: 10,
            },
        );
        bm.set(
            5,
            BlockMapEntry {
                hash: h,
                flags: 0,
                sequence: 42,
            },
        );
        bm.set(
            3,
            BlockMapEntry {
                hash: h,
                flags: 0,
                sequence: 25,
            },
        );

        assert_eq!(bm.max_sequence(), 42);
    }

    #[test]
    fn test_block_map_non_empty_count() {
        let mut bm = BlockMap::new(1024 * 1024, 4096);
        assert_eq!(bm.non_empty_count(), 0);

        let h = blake3_128(b"x");
        for i in [0, 10, 20, 30, 40] {
            bm.set(
                i,
                BlockMapEntry {
                    hash: h,
                    flags: 0,
                    sequence: i as u64,
                },
            );
        }
        assert_eq!(bm.non_empty_count(), 5);
    }

    #[test]
    fn test_block_map_iter_non_empty() {
        let mut bm = BlockMap::new(1024 * 1024, 4096);
        let h = blake3_128(b"iter");
        bm.set(
            5,
            BlockMapEntry {
                hash: h,
                flags: 0,
                sequence: 1,
            },
        );
        bm.set(
            50,
            BlockMapEntry {
                hash: h,
                flags: 0,
                sequence: 2,
            },
        );

        let non_empty: Vec<_> = bm.iter_non_empty().collect();
        assert_eq!(non_empty.len(), 2);
        assert_eq!(non_empty[0].0, 5);
        assert_eq!(non_empty[1].0, 50);
    }

    #[test]
    fn test_block_map_empty_persist_load() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.blkmap");

        let bm = BlockMap::new(1024 * 1024, 4096);
        bm.persist_to_file(&path).unwrap();

        let loaded = BlockMap::load_from_file(&path).unwrap();
        assert_eq!(loaded.non_empty_count(), 0);
        assert_eq!(loaded.len(), bm.len());
    }

    #[test]
    fn test_atomic_block_map_get_set() {
        let abm = AtomicBlockMap::new(1024 * 1024, 4096);
        let hash = blake3_128(b"atomic test");

        abm.set(42, hash, 7);
        let (got_hash, got_seq) = abm.get(42);
        assert_eq!(got_hash, hash);
        assert_eq!(got_seq, 7);
    }

    #[test]
    fn test_atomic_block_map_default_is_zero() {
        let abm = AtomicBlockMap::new(1024 * 1024, 4096);
        let (hash, seq) = abm.get(0);
        assert!(hash.is_zero());
        assert_eq!(seq, 0);
    }

    #[test]
    fn test_atomic_block_map_overwrite() {
        let abm = AtomicBlockMap::new(1024 * 1024, 4096);
        let h1 = blake3_128(b"first");
        let h2 = blake3_128(b"second");

        abm.set(10, h1, 1);
        abm.set(10, h2, 2);

        let (got_hash, got_seq) = abm.get(10);
        assert_eq!(got_hash, h2);
        assert_eq!(got_seq, 2);
    }

    #[test]
    fn test_atomic_block_map_snapshot() {
        let num_chunks = 256; // 1MB / 4KB
        let abm = AtomicBlockMap::new(1024 * 1024, 4096);

        // Set a few entries
        let h1 = blake3_128(b"chunk-0");
        let h2 = blake3_128(b"chunk-50");
        abm.set(0, h1, 1);
        abm.set(50, h2, 2);

        // Create flags array (simulating CacheInner's block_states)
        let flags: Vec<AtomicU8> =
            (0..num_chunks).map(|_| AtomicU8::new(0)).collect();
        flags[0].store(BlockMapEntry::FLAG_DIRTY, Ordering::Release);

        let snapshot = abm.snapshot(&flags);
        assert_eq!(snapshot.len(), num_chunks);

        let e0 = snapshot.get(0);
        assert_eq!(e0.hash, h1);
        assert_eq!(e0.sequence, 1);
        assert!(e0.is_dirty());

        let e50 = snapshot.get(50);
        assert_eq!(e50.hash, h2);
        assert_eq!(e50.sequence, 2);
        assert!(!e50.is_dirty());

        // Unset entry should be empty
        assert!(snapshot.get(100).is_empty());
    }

    #[test]
    fn test_atomic_block_map_from_block_map() {
        let mut bm = BlockMap::new(1024 * 1024, 4096);
        let h1 = blake3_128(b"entry-0");
        let h2 = blake3_128(b"entry-99");

        bm.set(
            0,
            BlockMapEntry {
                hash: h1,
                flags: BlockMapEntry::FLAG_DIRTY,
                sequence: 10,
            },
        );
        bm.set(
            99,
            BlockMapEntry {
                hash: h2,
                flags: 0,
                sequence: 20,
            },
        );

        let abm = AtomicBlockMap::from_block_map(&bm);
        assert_eq!(abm.len(), bm.len());

        let (got_h1, got_s1) = abm.get(0);
        assert_eq!(got_h1, h1);
        assert_eq!(got_s1, 10);

        let (got_h2, got_s2) = abm.get(99);
        assert_eq!(got_h2, h2);
        assert_eq!(got_s2, 20);

        // Round-trip: AtomicBlockMap -> snapshot -> compare
        let flags: Vec<AtomicU8> = (0..bm.len())
            .map(|i| AtomicU8::new(bm.get(i).flags))
            .collect();
        let snap = abm.snapshot(&flags);

        assert_eq!(snap.get(0).hash, h1);
        assert_eq!(snap.get(0).sequence, 10);
        assert!(snap.get(0).is_dirty());

        assert_eq!(snap.get(99).hash, h2);
        assert_eq!(snap.get(99).sequence, 20);
        assert!(!snap.get(99).is_dirty());
    }

    #[test]
    fn test_atomic_block_map_len() {
        let abm = AtomicBlockMap::new(1024 * 1024, 4096);
        // 1MB / 4KB = 256 chunks
        assert_eq!(abm.len(), 256);
        assert!(!abm.is_empty());
    }

    #[test]
    fn test_atomic_block_map_len_rounds_up() {
        // Device not evenly divisible by chunk size
        let abm = AtomicBlockMap::new(1024 * 1024 + 1, 4096);
        assert_eq!(abm.len(), 257);
    }

    #[test]
    fn test_sequence_monotonic() {
        let seq = SequenceNumber::new(0);
        assert_eq!(seq.current(), 0);

        let mut prev = 0;
        for _ in 0..1000 {
            let next = seq.next();
            assert!(next > prev, "sequence must be strictly increasing");
            prev = next;
        }
        assert_eq!(seq.current(), 1000);
    }

    #[test]
    fn test_sequence_starts_from_initial() {
        let seq = SequenceNumber::new(100);
        assert_eq!(seq.current(), 100);
        assert_eq!(seq.next(), 101);
        assert_eq!(seq.next(), 102);
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data =
            b"hello world, this is a test of lz4 compression roundtrip";
        let compressed = lz4_compress(data);
        let decompressed = lz4_decompress(&compressed).unwrap();
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn test_lz4_large_block() {
        // 128KB block with pattern data (compresses well)
        let mut data = vec![0u8; 131072];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        let compressed = lz4_compress(&data);
        assert!(
            compressed.len() < data.len(),
            "compressed should be smaller"
        );

        let decompressed = lz4_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_incompressible_data() {
        // Pseudo-random data that won't compress well
        let data: Vec<u8> = (0u32..4096)
            .map(|i| ((i.wrapping_mul(7).wrapping_add(13)) ^ (i >> 3)) as u8)
            .collect();
        let compressed = lz4_compress(&data);
        let decompressed = lz4_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    // ========================================================================
    // ForkedBlockMap tests
    // ========================================================================

    #[test]
    fn test_overlay_read_from_parent() {
        let mut parent = BlockMap::new(100 * 131072, 131072);
        for i in 0..100 {
            let hash = blake3_128(&[(i % 256) as u8; 128]);
            parent.set(
                i,
                BlockMapEntry {
                    hash,
                    flags: 0,
                    sequence: i as u64 + 1,
                },
            );
        }
        let parent = Arc::new(parent);
        let forked = ForkedBlockMap::new(Arc::clone(&parent));

        // All reads should come from parent
        for i in 0..100 {
            let (hash, seq) = forked.get(i);
            let parent_entry = parent.get(i);
            assert_eq!(hash, parent_entry.hash);
            assert_eq!(seq, parent_entry.sequence);
        }
        assert_eq!(forked.overlay_len(), 0);
    }

    #[test]
    fn test_overlay_write_diverges() {
        let mut parent = BlockMap::new(100 * 131072, 131072);
        let original_hash = blake3_128(b"original");
        parent.set(
            42,
            BlockMapEntry {
                hash: original_hash,
                flags: 0,
                sequence: 1,
            },
        );
        let parent = Arc::new(parent);

        let forked = ForkedBlockMap::new(Arc::clone(&parent));
        let new_hash = blake3_128(b"diverged");
        forked.set(42, new_hash, 2);

        // Fork reads new data
        let (hash, seq) = forked.get(42);
        assert_eq!(hash, new_hash);
        assert_eq!(seq, 2);

        // Parent unchanged
        let parent_entry = parent.get(42);
        assert_eq!(parent_entry.hash, original_hash);
        assert_eq!(parent_entry.sequence, 1);

        assert_eq!(forked.overlay_len(), 1);
    }

    #[test]
    fn test_overlay_snapshot_merges() {
        let mut parent = BlockMap::new(100 * 4096, 4096);
        for i in 0..50 {
            parent.set(
                i,
                BlockMapEntry {
                    hash: blake3_128(format!("parent-{i}").as_bytes()),
                    flags: 0,
                    sequence: i as u64 + 1,
                },
            );
        }
        let parent = Arc::new(parent);
        let forked = ForkedBlockMap::new(Arc::clone(&parent));

        // Write to indices 40..60 (overlap 40..50 with parent, new 50..60)
        for i in 40..60 {
            forked.set(
                i,
                blake3_128(format!("fork-{i}").as_bytes()),
                100 + i as u64,
            );
        }

        let flags: Vec<AtomicU8> =
            (0..100).map(|_| AtomicU8::new(0)).collect();
        let snap = forked.snapshot(&flags);

        // Indices 0..40: parent data
        for i in 0..40 {
            let entry = snap.get(i);
            assert_eq!(
                entry.hash,
                blake3_128(format!("parent-{i}").as_bytes())
            );
        }
        // Indices 40..60: fork overlay data
        for i in 40..60 {
            let entry = snap.get(i);
            assert_eq!(entry.hash, blake3_128(format!("fork-{i}").as_bytes()));
            assert_eq!(entry.sequence, 100 + i as u64);
        }
        // Indices 60..100: empty
        for i in 60..100 {
            assert!(snap.get(i).is_empty());
        }
    }

    #[test]
    fn test_overlay_flatten() {
        let mut parent = BlockMap::new(100 * 4096, 4096);
        for i in 0..100 {
            parent.set(
                i,
                BlockMapEntry {
                    hash: blake3_128(format!("p-{i}").as_bytes()),
                    flags: 0,
                    sequence: i as u64 + 1,
                },
            );
        }
        let parent = Arc::new(parent);
        let forked = ForkedBlockMap::new(Arc::clone(&parent));

        // Write >50 overlay entries (triggers should_flatten)
        for i in 0..60 {
            forked.set(
                i,
                blake3_128(format!("f-{i}").as_bytes()),
                200 + i as u64,
            );
        }
        assert!(forked.should_flatten());

        let flat = forked.flatten();

        // Verify flattened map has overlay data for 0..60
        for i in 0..60 {
            let (hash, seq) = flat.get(i);
            assert_eq!(hash, blake3_128(format!("f-{i}").as_bytes()));
            assert_eq!(seq, 200 + i as u64);
        }
        // And parent data for 60..100
        for i in 60..100 {
            let (hash, seq) = flat.get(i);
            assert_eq!(hash, blake3_128(format!("p-{i}").as_bytes()));
            assert_eq!(seq, i as u64 + 1);
        }
    }

    #[test]
    fn test_overlay_memory_sharing() {
        let parent = Arc::new(BlockMap::new(100 * 4096, 4096));
        let initial_count = Arc::strong_count(&parent);

        let forks: Vec<_> = (0..10)
            .map(|_| ForkedBlockMap::new(Arc::clone(&parent)))
            .collect();

        assert_eq!(Arc::strong_count(&parent), initial_count + 10);
        for fork in &forks {
            assert_eq!(fork.overlay_len(), 0);
        }
    }

    #[test]
    fn test_overlay_concurrent_access() {
        use std::sync::Arc as StdArc;

        let parent = Arc::new(BlockMap::new(1000 * 4096, 4096));
        let forked = StdArc::new(ForkedBlockMap::new(parent));

        let mut handles = vec![];
        for task_id in 0..10u32 {
            let f = StdArc::clone(&forked);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let idx = (task_id as usize * 100) + i;
                    let hash =
                        blake3_128(format!("t{task_id}-{i}").as_bytes());
                    f.set(idx, hash, (task_id as u64) * 1000 + i as u64);
                    let _ = f.get(idx);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(forked.overlay_len(), 1000);
    }
}
