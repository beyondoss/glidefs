//! Block state tracking and content-addressed hashing.
//!
//! This module provides:
//! - `Blake3Hash`: 16-byte truncated BLAKE3 hash for content addressing
//! - `SparseBlockState` / `SparseStateMap`: Lock-free sparse block state tracking
//! - `SequenceNumber`: Monotonic counter for WAL ordering
//! - LZ4 compress/decompress helpers

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicU8, Ordering};

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
// SparseStateMap -- lock-free sparse block state tracking
// ============================================================================

/// Block state constants for the sparse state map.
///
/// Zero-initialized default (unallocated pages) represents "not present",
/// so Clean/Dirty/Syncing are shifted by 1 from the legacy metadata encoding.
pub struct SparseBlockState;

impl SparseBlockState {
    /// Block has never been written to the local SSD.
    pub const NOT_PRESENT: u8 = 0;
    /// Block is present on SSD and synced to S3.
    pub const CLEAN: u8 = 1;
    /// Block is present on SSD and needs to be flushed to S3.
    pub const DIRTY: u8 = 2;
    /// Block is present on SSD and an upload is in progress.
    pub const SYNCING: u8 = 3;
}

const STATE_PAGE_BITS: usize = 12;
const STATE_PAGE_SIZE: usize = 1 << STATE_PAGE_BITS; // 4096 entries per page
const STATE_PAGE_MASK: usize = STATE_PAGE_SIZE - 1;

/// A page of 4096 block state entries (4096 bytes = one OS page).
#[repr(C, align(4096))]
struct StatePage {
    states: [AtomicU8; STATE_PAGE_SIZE],
}

impl StatePage {
    fn new_boxed() -> Box<Self> {
        // SAFETY: All-zeros is valid for StatePage because AtomicU8::new(0) is
        // represented as a zero byte with #[repr(C)] layout.
        unsafe { Box::new_zeroed().assume_init() }
    }
}

/// Sparse block state map using a two-level page table.
///
/// Only allocates 4 KB pages on first write to a block range. Unallocated
/// pages implicitly contain `NOT_PRESENT` (0) for all entries.
///
/// State encoding folds presence into the state byte:
/// - `0` = NotPresent (never written to SSD)
/// - `1` = Clean (present on SSD, synced to S3)
/// - `2` = Dirty (present on SSD, needs flush)
/// - `3` = Syncing (present on SSD, upload in progress)
///
/// This eliminates the need for a separate presence bitmap.
pub struct SparseStateMap {
    directory: Box<[AtomicPtr<StatePage>]>,
    num_pages: usize,
    num_entries: usize,
    allocated_pages: AtomicU64,
}

// SAFETY: Pages are heap-allocated, never freed during the map's lifetime
// (only in Drop), and directory slots transition null → valid exactly once (CAS).
unsafe impl Send for SparseStateMap {}
unsafe impl Sync for SparseStateMap {}

impl Drop for SparseStateMap {
    fn drop(&mut self) {
        for slot in self.directory.iter() {
            let ptr = slot.load(Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            }
        }
    }
}

impl SparseStateMap {
    /// Create a new sparse state map with no pages allocated.
    pub fn new(num_entries: usize) -> Self {
        let num_pages = num_entries.div_ceil(STATE_PAGE_SIZE);
        let directory = (0..num_pages)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SparseStateMap {
            directory,
            num_pages,
            num_entries,
            allocated_pages: AtomicU64::new(0),
        }
    }

    /// Number of entries (total block count).
    #[allow(dead_code)]
    #[inline]
    pub fn len(&self) -> usize {
        self.num_entries
    }

    /// Returns true if the state map has no entries.
    #[allow(dead_code)]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// Number of currently allocated pages.
    #[allow(dead_code)]
    pub fn allocated_pages(&self) -> u64 {
        self.allocated_pages.load(Ordering::Relaxed)
    }

    /// Estimated memory usage in bytes.
    #[allow(dead_code)]
    pub fn memory_usage(&self) -> usize {
        let dir_bytes = self.num_pages * size_of::<AtomicPtr<StatePage>>();
        let page_bytes =
            self.allocated_pages.load(Ordering::Relaxed) as usize * size_of::<StatePage>();
        dir_bytes + page_bytes
    }

    // -- Page access helpers --------------------------------------------------

    #[inline(always)]
    fn split_index(idx: usize) -> (usize, usize) {
        (idx >> STATE_PAGE_BITS, idx & STATE_PAGE_MASK)
    }

    #[inline]
    fn load_page(&self, page_idx: usize) -> Option<&StatePage> {
        let ptr = self.directory[page_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }

    #[inline]
    fn ensure_page(&self, page_idx: usize) -> &StatePage {
        let ptr = self.directory[page_idx].load(Ordering::Acquire);
        if !ptr.is_null() {
            return unsafe { &*ptr };
        }
        self.allocate_page(page_idx)
    }

    #[cold]
    fn allocate_page(&self, page_idx: usize) -> &StatePage {
        let new_page = StatePage::new_boxed();
        let new_ptr = Box::into_raw(new_page);

        match self.directory[page_idx].compare_exchange(
            ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.allocated_pages.fetch_add(1, Ordering::Relaxed);
                unsafe { &*new_ptr }
            }
            Err(existing) => {
                unsafe {
                    drop(Box::from_raw(new_ptr));
                }
                unsafe { &*existing }
            }
        }
    }

    // -- State operations -----------------------------------------------------

    /// Load the state for a block. Returns `NOT_PRESENT` (0) if the page is
    /// not allocated.
    #[inline]
    pub fn get(&self, idx: usize) -> u8 {
        let (page_idx, entry_idx) = Self::split_index(idx);
        match self.load_page(page_idx) {
            Some(page) => page.states[entry_idx].load(Ordering::Acquire),
            None => SparseBlockState::NOT_PRESENT,
        }
    }

    /// Check if a block is present on the local SSD (state != NOT_PRESENT).
    #[inline]
    pub fn is_present(&self, idx: usize) -> bool {
        self.get(idx) != SparseBlockState::NOT_PRESENT
    }

    /// Mark a block as present (CAS NOT_PRESENT → CLEAN).
    ///
    /// Idempotent: no-op if the block is already present.
    /// Allocates the page if needed.
    #[inline]
    pub fn set_present(&self, idx: usize) {
        let (page_idx, entry_idx) = Self::split_index(idx);
        let page = self.ensure_page(page_idx);
        // Only transition NOT_PRESENT → CLEAN. If already present, no-op.
        let _ = page.states[entry_idx].compare_exchange(
            SparseBlockState::NOT_PRESENT,
            SparseBlockState::CLEAN,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Compare-and-swap the state for a block.
    ///
    /// Returns `Ok(old)` on success, `Err(actual)` on failure.
    /// Allocates the page if needed (only when `new != NOT_PRESENT`).
    #[inline]
    pub fn cas(&self, idx: usize, expected: u8, new: u8) -> Result<u8, u8> {
        let (page_idx, entry_idx) = Self::split_index(idx);
        // For transitions to NOT_PRESENT, the page might not exist.
        let page = if new == SparseBlockState::NOT_PRESENT {
            match self.load_page(page_idx) {
                Some(p) => p,
                None => {
                    // Page doesn't exist, so current state is NOT_PRESENT.
                    return if expected == SparseBlockState::NOT_PRESENT {
                        Ok(SparseBlockState::NOT_PRESENT)
                    } else {
                        Err(SparseBlockState::NOT_PRESENT)
                    };
                }
            }
        } else {
            // Page allocation can't fail here because we only enforce budget
            // on set_present (the first write). Subsequent state transitions
            // hit already-allocated pages.
            match self.load_page(page_idx) {
                Some(p) => p,
                None => {
                    // Page doesn't exist → current is NOT_PRESENT.
                    return Err(SparseBlockState::NOT_PRESENT);
                }
            }
        };
        match page.states[entry_idx].compare_exchange(
            expected,
            new,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(expected),
            Err(actual) => Err(actual),
        }
    }

    /// Iterate over all allocated pages, yielding `(block_index, state)` for
    /// entries matching the given `target_state`.
    ///
    /// Only visits allocated pages — O(allocated_pages × PAGE_SIZE), not
    /// O(total_blocks). This is a major win for sparse exports.
    pub fn iter_with_state(&self, target_state: u8) -> impl Iterator<Item = usize> + '_ {
        (0..self.num_pages).flat_map(move |page_idx| {
            let page = self.load_page(page_idx);
            let page_start = page_idx << STATE_PAGE_BITS;
            let page_end = std::cmp::min(page_start + STATE_PAGE_SIZE, self.num_entries);
            let count = page_end - page_start;

            (0..count).filter_map(move |entry_idx| {
                let page = page?;
                let state = page.states[entry_idx].load(Ordering::Acquire);
                if state == target_state {
                    Some(page_start + entry_idx)
                } else {
                    None
                }
            })
        })
    }

    /// Iterate over all allocated pages, yielding `(block_index, state)` for
    /// entries with a non-zero state (present blocks).
    ///
    /// Only visits allocated pages — O(allocated_pages × PAGE_SIZE), not
    /// O(total_blocks). This is a major win for sparse exports.
    pub fn iter_present(&self) -> impl Iterator<Item = (usize, u8)> + '_ {
        (0..self.num_pages).flat_map(move |page_idx| {
            let page = self.load_page(page_idx);
            let page_start = page_idx << STATE_PAGE_BITS;
            let page_end = std::cmp::min(page_start + STATE_PAGE_SIZE, self.num_entries);
            let count = page_end - page_start;

            (0..count).filter_map(move |entry_idx| {
                let page = page?;
                let state = page.states[entry_idx].load(Ordering::Acquire);
                if state != SparseBlockState::NOT_PRESENT {
                    Some((page_start + entry_idx, state))
                } else {
                    None
                }
            })
        })
    }

    /// Count blocks with a non-zero state (present blocks).
    pub fn count_present(&self) -> usize {
        let mut count = 0;
        for page_idx in 0..self.num_pages {
            if let Some(page) = self.load_page(page_idx) {
                let page_end =
                    std::cmp::min((page_idx + 1) << STATE_PAGE_BITS, self.num_entries);
                let page_start = page_idx << STATE_PAGE_BITS;
                for entry_idx in 0..(page_end - page_start) {
                    if page.states[entry_idx].load(Ordering::Relaxed)
                        != SparseBlockState::NOT_PRESENT
                    {
                        count += 1;
                    }
                }
            }
        }
        count
    }

    /// Load state for a block, returning the `AtomicU8` reference if the page
    /// exists. Used by snapshot to read flags without allocating.
    #[allow(dead_code)]
    #[inline]
    pub fn load_atomic(&self, idx: usize) -> Option<&AtomicU8> {
        let (page_idx, entry_idx) = Self::split_index(idx);
        self.load_page(page_idx)
            .map(|page| &page.states[entry_idx])
    }
}

// ============================================================================
// SequenceNumber -- monotonic counter
// ============================================================================

/// Monotonic sequence counter for WAL ordering.
///
/// Each write increments this counter and stores the resulting value in the
/// WAL entry. The counter is persisted in block_states metadata for recovery.
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;
    use std::time::Instant;

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
}
