//! Block state tracking and content-addressed hashing.
//!
//! This module provides:
//! - `Blake3Hash`: 16-byte truncated BLAKE3 hash for content addressing
//! - `SparseBlockState` / `SparseStateMap`: Lock-free sparse block state tracking
//! - `SparseCrcMap`: Lock-free sparse CRC32 tracking (AtomicU32 page table)
//! - `SequenceNumber`: Monotonic counter for WAL ordering
//! - LZ4 compress/decompress helpers

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};

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
    #[allow(dead_code)]
    pub const ZERO: Blake3Hash = Blake3Hash([0u8; 16]);

    /// Construct from raw bytes.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

/// Number of 2-bit entries packed into each `AtomicU8`.
const ENTRIES_PER_BYTE: usize = 4;
/// Size of one state page in bytes (one OS page).
const STATE_PAGE_BYTES: usize = 4096;
/// Number of block state entries per page (4096 bytes × 4 entries/byte).
const STATE_PAGE_ENTRIES: usize = STATE_PAGE_BYTES * ENTRIES_PER_BYTE; // 16384
const STATE_PAGE_BITS: usize = 14; // log2(16384)
const STATE_PAGE_MASK: usize = STATE_PAGE_ENTRIES - 1;

/// A page of 16,384 block state entries packed into 4,096 bytes (one OS page).
///
/// Each `AtomicU8` holds 4 entries at 2 bits each:
/// - bits [1:0] = entry 0
/// - bits [3:2] = entry 1
/// - bits [5:4] = entry 2
/// - bits [7:6] = entry 3
#[repr(C, align(4096))]
struct StatePage {
    data: [AtomicU8; STATE_PAGE_BYTES],
}

impl StatePage {
    fn new_boxed() -> Box<Self> {
        // SAFETY: All-zeros is valid for StatePage because AtomicU8::new(0) is
        // represented as a zero byte with #[repr(C)] layout.
        unsafe { Box::new_zeroed().assume_init() }
    }
}

/// Sparse block state map using a two-level page table with 2-bit packing.
///
/// Only allocates 4 KB pages on first write to a block range. Unallocated
/// pages implicitly contain `NOT_PRESENT` (0) for all entries. Each page
/// holds 16,384 entries (4 entries per `AtomicU8` × 4,096 bytes).
///
/// State encoding (2 bits per entry):
/// - `0` = NotPresent (never written to SSD)
/// - `1` = Clean (present on SSD, synced to S3)
/// - `2` = Dirty (present on SSD, needs flush)
/// - `3` = Syncing (present on SSD, upload in progress)
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
        let num_pages = num_entries.div_ceil(STATE_PAGE_ENTRIES);
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

    // -- Index decomposition --------------------------------------------------

    /// Decompose a block index into (page_idx, byte_idx_within_page, bit_shift).
    #[inline(always)]
    fn split_index(idx: usize) -> (usize, usize, u32) {
        let page_idx = idx >> STATE_PAGE_BITS;
        let entry_in_page = idx & STATE_PAGE_MASK;
        let byte_idx = entry_in_page / ENTRIES_PER_BYTE;
        let shift = ((idx % ENTRIES_PER_BYTE) * 2) as u32;
        (page_idx, byte_idx, shift)
    }

    // -- Page access helpers --------------------------------------------------

    #[inline]
    fn load_page(&self, page_idx: usize) -> Option<&StatePage> {
        let ptr = self.directory[page_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            // SAFETY: Non-null pointers in the directory are valid Box<StatePage>
            // allocations that live for the lifetime of this SparseStateMap.
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
        let (page_idx, byte_idx, shift) = Self::split_index(idx);
        match self.load_page(page_idx) {
            Some(page) => (page.data[byte_idx].load(Ordering::Acquire) >> shift) & 0x3,
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
        let (page_idx, byte_idx, shift) = Self::split_index(idx);
        let page = self.ensure_page(page_idx);
        let mask = 0x3u8 << shift;
        loop {
            let old = page.data[byte_idx].load(Ordering::Acquire);
            if (old >> shift) & 0x3 != SparseBlockState::NOT_PRESENT {
                break; // already present
            }
            let new = (old & !mask) | (SparseBlockState::CLEAN << shift);
            if page.data[byte_idx]
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Compare-and-swap the state for a block.
    ///
    /// Returns `Ok(old)` on success, `Err(actual)` on failure.
    /// If the page doesn't exist, the current state is NOT_PRESENT.
    #[inline]
    pub fn cas(&self, idx: usize, expected: u8, new: u8) -> Result<u8, u8> {
        let (page_idx, byte_idx, shift) = Self::split_index(idx);
        let mask = 0x3u8 << shift;

        let page = match self.load_page(page_idx) {
            Some(p) => p,
            None => {
                // Page doesn't exist → current state is NOT_PRESENT.
                if expected != SparseBlockState::NOT_PRESENT {
                    return Err(SparseBlockState::NOT_PRESENT);
                }
                if new == SparseBlockState::NOT_PRESENT {
                    // No-op: NOT_PRESENT → NOT_PRESENT needs no allocation.
                    return Ok(SparseBlockState::NOT_PRESENT);
                }
                // Transitioning from NOT_PRESENT to a real state —
                // allocate the page so the CAS loop below can write it.
                self.ensure_page(page_idx)
            }
        };

        loop {
            let old = page.data[byte_idx].load(Ordering::Acquire);
            let current = (old >> shift) & 0x3;
            if current != expected {
                return Err(current);
            }
            let updated = (old & !mask) | (new << shift);
            match page.data[byte_idx].compare_exchange(
                old,
                updated,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(expected),
                Err(_) => continue, // another entry in same byte was modified, retry
            }
        }
    }

    /// Iterate over all allocated pages, yielding block indices for entries
    /// matching the given `target_state`.
    ///
    /// Only visits allocated pages — O(allocated_pages × PAGE_BYTES), not
    /// O(total_blocks). Bytes where all 4 entries are NOT_PRESENT (0x00) are
    /// skipped with a single comparison.
    pub fn iter_with_state(&self, target_state: u8) -> impl Iterator<Item = usize> + '_ {
        let num_entries = self.num_entries;
        (0..self.num_pages).flat_map(move |page_idx| {
            let page = self.load_page(page_idx);
            let page_start = page_idx * STATE_PAGE_ENTRIES;

            (0..STATE_PAGE_BYTES).flat_map(move |byte_idx| {
                let val = page
                    .map(|p| p.data[byte_idx].load(Ordering::Relaxed))
                    .unwrap_or(0);
                if val == 0 {
                    // All 4 entries NOT_PRESENT — skip entire byte.
                    return None;
                }
                let base = page_start + byte_idx * ENTRIES_PER_BYTE;
                Some((0..ENTRIES_PER_BYTE as u32).filter_map(move |slot| {
                    let idx = base + slot as usize;
                    if idx >= num_entries {
                        return None;
                    }
                    let state = (val >> (slot * 2)) & 0x3;
                    if state == target_state {
                        Some(idx)
                    } else {
                        None
                    }
                }))
            }).flatten()
        })
    }

    /// Iterate over all allocated pages, yielding `(block_index, state)` for
    /// entries with a non-zero state (present blocks).
    ///
    /// Only visits allocated pages. Bytes where all 4 entries are NOT_PRESENT
    /// (0x00) are skipped with a single comparison.
    pub fn iter_present(&self) -> impl Iterator<Item = (usize, u8)> + '_ {
        let num_entries = self.num_entries;
        (0..self.num_pages).flat_map(move |page_idx| {
            let page = self.load_page(page_idx);
            let page_start = page_idx * STATE_PAGE_ENTRIES;

            (0..STATE_PAGE_BYTES).flat_map(move |byte_idx| {
                let val = page
                    .map(|p| p.data[byte_idx].load(Ordering::Relaxed))
                    .unwrap_or(0);
                if val == 0 {
                    return None;
                }
                let base = page_start + byte_idx * ENTRIES_PER_BYTE;
                Some((0..ENTRIES_PER_BYTE as u32).filter_map(move |slot| {
                    let idx = base + slot as usize;
                    if idx >= num_entries {
                        return None;
                    }
                    let state = (val >> (slot * 2)) & 0x3;
                    if state != SparseBlockState::NOT_PRESENT {
                        Some((idx, state))
                    } else {
                        None
                    }
                }))
            }).flatten()
        })
    }

    /// Count blocks with a non-zero state (present blocks).
    pub fn count_present(&self) -> usize {
        let mut count = 0;
        for page_idx in 0..self.num_pages {
            if let Some(page) = self.load_page(page_idx) {
                let page_start = page_idx * STATE_PAGE_ENTRIES;
                for byte_idx in 0..STATE_PAGE_BYTES {
                    let val = page.data[byte_idx].load(Ordering::Relaxed);
                    if val == 0 {
                        continue;
                    }
                    for slot in 0..ENTRIES_PER_BYTE as u32 {
                        let idx = page_start + byte_idx * ENTRIES_PER_BYTE + slot as usize;
                        if idx >= self.num_entries {
                            break;
                        }
                        if (val >> (slot * 2)) & 0x3 != SparseBlockState::NOT_PRESENT {
                            count += 1;
                        }
                    }
                }
            }
        }
        count
    }
}

// ============================================================================
// SparseCrcMap -- lock-free sparse CRC32 tracking
// ============================================================================

/// Number of `AtomicU32` entries per CRC page (1024 × 4B = 4096B = one OS page).
const CRC_PAGE_ENTRIES: usize = 1024;
const CRC_PAGE_BITS: usize = 10; // log2(1024)
const CRC_PAGE_MASK: usize = CRC_PAGE_ENTRIES - 1;

/// A page of 1,024 CRC32 entries stored as `AtomicU32`, fitting in one 4KB OS page.
///
/// Value `0` means "empty slot" (no CRC stored). All other values are CRC32
/// checksums or sentinel values. A CRC32 that naturally computes to 0 is treated
/// as "no CRC" — verification is skipped for that block (1-in-4B chance, same
/// acceptable tradeoff as `CRC_SENTINEL`).
#[repr(C, align(4096))]
struct CrcPage {
    data: [AtomicU32; CRC_PAGE_ENTRIES],
}

impl CrcPage {
    fn new_boxed() -> Box<Self> {
        // SAFETY: All-zeros is valid for CrcPage because AtomicU32::new(0) is
        // represented as zero bytes with #[repr(C)] layout.
        unsafe { Box::new_zeroed().assume_init() }
    }
}

/// Lock-free sparse CRC32 map using a two-level page table with `AtomicU32` leaves.
///
/// Only allocates 4KB pages on first write to a block range. Unallocated pages
/// implicitly contain "empty" (0) for all entries. Each page holds 1,024 entries.
///
/// Value encoding:
/// - `0` = empty (no CRC stored)
/// - `u32::MAX` = CRC_SENTINEL (write invalidated this CRC)
/// - all others = real CRC32 checksum
///
/// Replaces `DashMap<usize, u32>` with 5x less memory and zero lock contention.
pub struct SparseCrcMap {
    directory: Box<[AtomicPtr<CrcPage>]>,
    #[allow(dead_code)]
    num_entries: usize,
    /// Approximate entry count for cap checks. May briefly be off by 1 under
    /// concurrent access — cap checks are already racy (same as DashMap::len).
    count: AtomicUsize,
}

// SAFETY: Pages are heap-allocated, never freed during the map's lifetime
// (only in Drop), and directory slots transition null → valid exactly once (CAS).
unsafe impl Send for SparseCrcMap {}
unsafe impl Sync for SparseCrcMap {}

impl Drop for SparseCrcMap {
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

impl SparseCrcMap {
    /// Create a new sparse CRC map with no pages allocated.
    pub fn new(num_entries: usize) -> Self {
        let num_pages = num_entries.div_ceil(CRC_PAGE_ENTRIES);
        let directory = (0..num_pages)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SparseCrcMap {
            directory,
            num_entries,
            count: AtomicUsize::new(0),
        }
    }

    // -- Index decomposition --------------------------------------------------

    #[inline(always)]
    fn split_index(idx: usize) -> (usize, usize) {
        let page_idx = idx >> CRC_PAGE_BITS;
        let entry_idx = idx & CRC_PAGE_MASK;
        (page_idx, entry_idx)
    }

    // -- Page access helpers --------------------------------------------------

    #[inline]
    fn load_page(&self, page_idx: usize) -> Option<&CrcPage> {
        let ptr = self.directory[page_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }

    #[inline]
    fn ensure_page(&self, page_idx: usize) -> &CrcPage {
        let ptr = self.directory[page_idx].load(Ordering::Acquire);
        if !ptr.is_null() {
            return unsafe { &*ptr };
        }
        self.allocate_page(page_idx)
    }

    #[cold]
    fn allocate_page(&self, page_idx: usize) -> &CrcPage {
        let new_page = CrcPage::new_boxed();
        let new_ptr = Box::into_raw(new_page);

        match self.directory[page_idx].compare_exchange(
            ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => unsafe { &*new_ptr },
            Err(existing) => {
                unsafe {
                    drop(Box::from_raw(new_ptr));
                }
                unsafe { &*existing }
            }
        }
    }

    // -- CRC operations -------------------------------------------------------

    /// Load the CRC for a block. Returns `None` if the page is not allocated
    /// or the slot is empty (value == 0).
    #[inline]
    pub fn load(&self, idx: usize) -> Option<u32> {
        let (page_idx, entry_idx) = Self::split_index(idx);
        match self.load_page(page_idx) {
            Some(page) => {
                let val = page.data[entry_idx].load(Ordering::Acquire);
                if val == 0 { None } else { Some(val) }
            }
            None => None,
        }
    }

    /// Unconditionally store a CRC value. Adjusts the approximate count.
    ///
    /// Used by the write path to insert `CRC_SENTINEL` on every write.
    #[inline]
    pub fn store(&self, idx: usize, val: u32) {
        let (page_idx, entry_idx) = Self::split_index(idx);
        let page = self.ensure_page(page_idx);
        let old = page.data[entry_idx].swap(val, Ordering::AcqRel);
        if old == 0 && val != 0 {
            self.count.fetch_add(1, Ordering::Relaxed);
        } else if old != 0 && val == 0 {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Insert a CRC only if the slot is empty (CAS 0 → val).
    ///
    /// Used by the checkpoint path (`crc_store`) to avoid overwriting a
    /// sentinel or existing CRC from a concurrent write.
    #[inline]
    pub fn try_insert(&self, idx: usize, val: u32) {
        if val == 0 {
            return; // storing 0 is a no-op (indistinguishable from empty)
        }
        let (page_idx, entry_idx) = Self::split_index(idx);
        let page = self.ensure_page(page_idx);
        if page.data[entry_idx]
            .compare_exchange(0, val, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Remove and return the CRC for a block (swap to 0).
    ///
    /// Used by the flush path to consume a CRC for verification.
    #[inline]
    pub fn take(&self, idx: usize) -> Option<u32> {
        let (page_idx, entry_idx) = Self::split_index(idx);
        match self.load_page(page_idx) {
            Some(page) => {
                let old = page.data[entry_idx].swap(0, Ordering::AcqRel);
                if old != 0 {
                    self.count.fetch_sub(1, Ordering::Relaxed);
                    Some(old)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Clear the CRC for a block (store 0). Decrements count if non-empty.
    ///
    /// Used by the checkpoint path to clear a sentinel before recomputing.
    #[inline]
    pub fn remove(&self, idx: usize) {
        let (page_idx, entry_idx) = Self::split_index(idx);
        if let Some(page) = self.load_page(page_idx) {
            let old = page.data[entry_idx].swap(0, Ordering::AcqRel);
            if old != 0 {
                self.count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Approximate number of non-empty entries. May briefly be off by 1
    /// under concurrent access — used for soft cap checks only.
    #[inline]
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
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
    /// Saturates at `u64::MAX` instead of wrapping, which would violate
    /// the monotonicity invariant that WAL replay depends on.
    #[inline]
    pub fn next(&self) -> u64 {
        match self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                if v == u64::MAX {
                    None
                } else {
                    Some(v + 1)
                }
            }) {
            Ok(prev) => prev + 1,
            Err(_) => {
                tracing::error!(
                    "SequenceNumber saturated at u64::MAX — \
                     this should be impossible in normal operation"
                );
                u64::MAX
            }
        }
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
///
/// Validates that the claimed uncompressed size does not exceed `MAX_DECOMPRESSED_SIZE`
/// (2 MB) before allocating — a corrupted or adversarial size prefix cannot OOM the process.
#[inline]
pub fn lz4_decompress(
    compressed: &[u8],
) -> Result<Vec<u8>, lz4_flex::block::DecompressError> {
    // The first 4 bytes are the little-endian uncompressed size.
    // Max legitimate block is 1 MB; allow 2× headroom.
    const MAX_DECOMPRESSED_SIZE: u32 = 2 * 1024 * 1024;

    if compressed.len() < 4 {
        return Err(lz4_flex::block::DecompressError::ExpectedAnotherByte);
    }
    let claimed_size =
        u32::from_le_bytes(compressed[..4].try_into().unwrap());
    if claimed_size > MAX_DECOMPRESSED_SIZE {
        return Err(lz4_flex::block::DecompressError::OutputTooSmall {
            expected: claimed_size as usize,
            actual: MAX_DECOMPRESSED_SIZE as usize,
        });
    }
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
        let max_us: u128 = if cfg!(debug_assertions) { 50_000 } else { 50 };
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

    // ========================================================================
    // SparseStateMap tests
    // ========================================================================

    #[test]
    fn test_sparse_state_map_initial_state() {
        let map = SparseStateMap::new(1000);
        assert_eq!(map.len(), 1000);
        assert!(!map.is_empty());
        assert_eq!(map.allocated_pages(), 0);
        // All entries start as NOT_PRESENT
        for i in [0, 1, 2, 3, 500, 999] {
            assert_eq!(map.get(i), SparseBlockState::NOT_PRESENT);
            assert!(!map.is_present(i));
        }
    }

    #[test]
    fn test_sparse_state_map_packing_all_slots() {
        // Verify each 2-bit slot position (0-3) within a byte works correctly
        let map = SparseStateMap::new(8);

        for slot in 0..4u8 {
            let idx = slot as usize;
            map.set_present(idx);
            assert_eq!(map.get(idx), SparseBlockState::CLEAN);
            assert!(map.is_present(idx));

            // Transition through all states
            assert!(map
                .cas(idx, SparseBlockState::CLEAN, SparseBlockState::DIRTY)
                .is_ok());
            assert_eq!(map.get(idx), SparseBlockState::DIRTY);

            assert!(map
                .cas(idx, SparseBlockState::DIRTY, SparseBlockState::SYNCING)
                .is_ok());
            assert_eq!(map.get(idx), SparseBlockState::SYNCING);

            assert!(map
                .cas(idx, SparseBlockState::SYNCING, SparseBlockState::CLEAN)
                .is_ok());
            assert_eq!(map.get(idx), SparseBlockState::CLEAN);
        }
    }

    #[test]
    fn test_sparse_state_map_slot_isolation() {
        // Modifying one slot must NOT affect adjacent slots in the same byte
        let map = SparseStateMap::new(8);

        // Set slot 0 to DIRTY, slot 1 to CLEAN, slot 2 to SYNCING, slot 3 stays NOT_PRESENT
        map.set_present(0);
        map.cas(0, SparseBlockState::CLEAN, SparseBlockState::DIRTY)
            .unwrap();
        map.set_present(1);
        map.set_present(2);
        map.cas(2, SparseBlockState::CLEAN, SparseBlockState::SYNCING)
            .unwrap();

        assert_eq!(map.get(0), SparseBlockState::DIRTY);
        assert_eq!(map.get(1), SparseBlockState::CLEAN);
        assert_eq!(map.get(2), SparseBlockState::SYNCING);
        assert_eq!(map.get(3), SparseBlockState::NOT_PRESENT);

        // Now modify slot 1 → should not touch 0, 2, 3
        map.cas(1, SparseBlockState::CLEAN, SparseBlockState::SYNCING)
            .unwrap();
        assert_eq!(map.get(0), SparseBlockState::DIRTY);
        assert_eq!(map.get(1), SparseBlockState::SYNCING);
        assert_eq!(map.get(2), SparseBlockState::SYNCING);
        assert_eq!(map.get(3), SparseBlockState::NOT_PRESENT);
    }

    #[test]
    fn test_sparse_state_map_cas_failure() {
        let map = SparseStateMap::new(4);
        map.set_present(0);
        // Expect DIRTY but actual is CLEAN → should fail
        let result = map.cas(0, SparseBlockState::DIRTY, SparseBlockState::SYNCING);
        assert_eq!(result, Err(SparseBlockState::CLEAN));
    }

    #[test]
    fn test_sparse_state_map_cas_not_present_page() {
        let map = SparseStateMap::new(STATE_PAGE_ENTRIES * 2);
        // CAS on unallocated page with expected=NOT_PRESENT → Ok
        let result = map.cas(
            STATE_PAGE_ENTRIES + 5,
            SparseBlockState::NOT_PRESENT,
            SparseBlockState::NOT_PRESENT,
        );
        assert_eq!(result, Ok(SparseBlockState::NOT_PRESENT));

        // CAS on unallocated page with expected=CLEAN → Err(NOT_PRESENT)
        let result = map.cas(
            STATE_PAGE_ENTRIES + 5,
            SparseBlockState::CLEAN,
            SparseBlockState::DIRTY,
        );
        assert_eq!(result, Err(SparseBlockState::NOT_PRESENT));
    }

    #[test]
    fn test_sparse_state_map_cas_allocates_on_transition_from_not_present() {
        let map = SparseStateMap::new(STATE_PAGE_ENTRIES * 2);
        let idx = STATE_PAGE_ENTRIES + 5; // on second (unallocated) page
        assert_eq!(map.allocated_pages(), 0);

        // CAS NOT_PRESENT → DIRTY on unallocated page should allocate and succeed.
        let result = map.cas(idx, SparseBlockState::NOT_PRESENT, SparseBlockState::DIRTY);
        assert_eq!(result, Ok(SparseBlockState::NOT_PRESENT));
        assert_eq!(map.get(idx), SparseBlockState::DIRTY);
        assert_eq!(map.allocated_pages(), 1);
    }

    #[test]
    fn test_sparse_state_map_set_present_idempotent() {
        let map = SparseStateMap::new(4);
        map.set_present(0);
        assert_eq!(map.get(0), SparseBlockState::CLEAN);

        // CAS to DIRTY
        map.cas(0, SparseBlockState::CLEAN, SparseBlockState::DIRTY)
            .unwrap();
        assert_eq!(map.get(0), SparseBlockState::DIRTY);

        // set_present again should be a no-op (state is not NOT_PRESENT)
        map.set_present(0);
        assert_eq!(map.get(0), SparseBlockState::DIRTY);
    }

    #[test]
    fn test_sparse_state_map_cross_page_boundary() {
        let n = STATE_PAGE_ENTRIES + 4;
        let map = SparseStateMap::new(n);

        // Set entries at page boundary
        let last_on_page0 = STATE_PAGE_ENTRIES - 1;
        let first_on_page1 = STATE_PAGE_ENTRIES;

        map.set_present(last_on_page0);
        map.set_present(first_on_page1);

        assert_eq!(map.get(last_on_page0), SparseBlockState::CLEAN);
        assert_eq!(map.get(first_on_page1), SparseBlockState::CLEAN);
        assert_eq!(map.allocated_pages(), 2);
    }

    #[test]
    fn test_sparse_state_map_iter_with_state() {
        let map = SparseStateMap::new(32);

        map.set_present(0);
        map.set_present(5);
        map.set_present(10);
        map.set_present(20);

        // Mark some dirty
        map.cas(5, SparseBlockState::CLEAN, SparseBlockState::DIRTY)
            .unwrap();
        map.cas(20, SparseBlockState::CLEAN, SparseBlockState::DIRTY)
            .unwrap();

        let clean: Vec<usize> = map.iter_with_state(SparseBlockState::CLEAN).collect();
        assert_eq!(clean, vec![0, 10]);

        let dirty: Vec<usize> = map.iter_with_state(SparseBlockState::DIRTY).collect();
        assert_eq!(dirty, vec![5, 20]);

        let syncing: Vec<usize> = map.iter_with_state(SparseBlockState::SYNCING).collect();
        assert!(syncing.is_empty());
    }

    #[test]
    fn test_sparse_state_map_iter_present() {
        let map = SparseStateMap::new(16);
        map.set_present(1);
        map.set_present(3);
        map.set_present(7);

        map.cas(3, SparseBlockState::CLEAN, SparseBlockState::DIRTY)
            .unwrap();
        map.cas(7, SparseBlockState::CLEAN, SparseBlockState::SYNCING)
            .unwrap();

        let present: Vec<(usize, u8)> = map.iter_present().collect();
        assert_eq!(
            present,
            vec![
                (1, SparseBlockState::CLEAN),
                (3, SparseBlockState::DIRTY),
                (7, SparseBlockState::SYNCING),
            ]
        );
    }

    #[test]
    fn test_sparse_state_map_count_present() {
        let map = SparseStateMap::new(100);
        assert_eq!(map.count_present(), 0);

        map.set_present(0);
        map.set_present(50);
        map.set_present(99);
        assert_eq!(map.count_present(), 3);
    }

    #[test]
    fn test_sparse_state_map_memory_usage_4x_smaller() {
        // Verify the directory is 4x smaller than a naive 1-byte-per-entry approach.
        // Use entry counts that divide evenly by both 4096 and 16384 for clean math.
        let n = STATE_PAGE_ENTRIES * 10; // 163,840 entries
        let map = SparseStateMap::new(n);

        let old_num_pages = n / 4096;  // 1 byte per entry, 4096 entries/page = 40
        let new_num_pages = n / 16384; // 2 bits per entry, 16384 entries/page = 10
        assert_eq!(map.num_pages, new_num_pages);
        assert_eq!(old_num_pages, new_num_pages * 4); // exactly 4x fewer pages

        // Also verify the constants are correct
        assert_eq!(ENTRIES_PER_BYTE, 4);
        assert_eq!(STATE_PAGE_ENTRIES, STATE_PAGE_BYTES * ENTRIES_PER_BYTE);
        assert_eq!(STATE_PAGE_ENTRIES, 1 << STATE_PAGE_BITS);
    }

    #[test]
    fn test_sparse_state_map_concurrent_cas_adjacent_slots() {
        use std::sync::Arc;
        use std::thread;

        let map = Arc::new(SparseStateMap::new(1024));

        // Pre-populate entries across multiple groups of 4
        for i in 0..128 {
            map.set_present(i);
        }

        // Spawn threads that CAS adjacent entries (same byte) concurrently
        let mut handles = Vec::new();
        for slot_offset in 0..4 {
            let map = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                // Each thread works on one slot position across many bytes
                for byte_group in 0..32 {
                    let idx = byte_group * 4 + slot_offset;
                    // CLEAN → DIRTY
                    loop {
                        match map.cas(idx, SparseBlockState::CLEAN, SparseBlockState::DIRTY) {
                            Ok(_) => break,
                            Err(SparseBlockState::CLEAN) => continue, // spurious CAS failure from adjacent slot
                            Err(other) => panic!("unexpected state {} at idx {}", other, idx),
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify all entries transitioned to DIRTY
        for i in 0..128 {
            assert_eq!(
                map.get(i),
                SparseBlockState::DIRTY,
                "entry {} should be DIRTY",
                i
            );
        }
    }

    #[test]
    fn test_sparse_state_map_iter_respects_num_entries() {
        // num_entries doesn't align to page boundary — iterator must not yield
        // phantom entries beyond num_entries
        let n = 10; // much less than STATE_PAGE_ENTRIES
        let map = SparseStateMap::new(n);
        for i in 0..n {
            map.set_present(i);
        }

        let present: Vec<(usize, u8)> = map.iter_present().collect();
        assert_eq!(present.len(), n);
        assert!(present.iter().all(|&(idx, _)| idx < n));
    }

    // ========================================================================
    // SparseCrcMap tests
    // ========================================================================

    #[test]
    fn test_sparse_crc_map_initial_state() {
        let map = SparseCrcMap::new(1000);
        assert_eq!(map.count(), 0);
        for i in [0, 1, 500, 999] {
            assert!(map.load(i).is_none());
        }
    }

    #[test]
    fn test_sparse_crc_map_store_and_load() {
        let map = SparseCrcMap::new(100);
        map.store(5, 0x12345678);
        assert_eq!(map.load(5), Some(0x12345678));
        assert_eq!(map.count(), 1);

        // Overwrite
        map.store(5, 0xABCDEF00);
        assert_eq!(map.load(5), Some(0xABCDEF00));
        assert_eq!(map.count(), 1); // count unchanged on overwrite
    }

    #[test]
    fn test_sparse_crc_map_store_zero_clears() {
        let map = SparseCrcMap::new(10);
        map.store(3, 42);
        assert_eq!(map.count(), 1);
        map.store(3, 0); // storing 0 = clearing
        assert!(map.load(3).is_none());
        assert_eq!(map.count(), 0);
    }

    #[test]
    fn test_sparse_crc_map_try_insert_empty() {
        let map = SparseCrcMap::new(10);
        map.try_insert(0, 100);
        assert_eq!(map.load(0), Some(100));
        assert_eq!(map.count(), 1);
    }

    #[test]
    fn test_sparse_crc_map_try_insert_occupied() {
        let map = SparseCrcMap::new(10);
        map.store(0, 100);
        map.try_insert(0, 200); // should not overwrite
        assert_eq!(map.load(0), Some(100));
        assert_eq!(map.count(), 1);
    }

    #[test]
    fn test_sparse_crc_map_try_insert_zero_is_noop() {
        let map = SparseCrcMap::new(10);
        map.try_insert(0, 0); // 0 is "empty", no-op
        assert!(map.load(0).is_none());
        assert_eq!(map.count(), 0);
    }

    #[test]
    fn test_sparse_crc_map_take() {
        let map = SparseCrcMap::new(10);
        map.store(5, 42);
        assert_eq!(map.take(5), Some(42));
        assert!(map.load(5).is_none());
        assert_eq!(map.count(), 0);

        // Take from empty slot
        assert_eq!(map.take(5), None);
    }

    #[test]
    fn test_sparse_crc_map_remove() {
        let map = SparseCrcMap::new(10);
        map.store(3, 99);
        assert_eq!(map.count(), 1);
        map.remove(3);
        assert!(map.load(3).is_none());
        assert_eq!(map.count(), 0);

        // Remove from empty slot — no-op
        map.remove(3);
        assert_eq!(map.count(), 0);
    }

    #[test]
    fn test_sparse_crc_map_cross_page() {
        // Entries on different pages work independently
        let n = CRC_PAGE_ENTRIES * 2 + 10;
        let map = SparseCrcMap::new(n);

        let idx_page0 = 0;
        let idx_page1 = CRC_PAGE_ENTRIES;
        let idx_page2 = CRC_PAGE_ENTRIES * 2 + 5;

        map.store(idx_page0, 1);
        map.store(idx_page1, 2);
        map.store(idx_page2, 3);

        assert_eq!(map.load(idx_page0), Some(1));
        assert_eq!(map.load(idx_page1), Some(2));
        assert_eq!(map.load(idx_page2), Some(3));
        assert_eq!(map.count(), 3);
    }

    #[test]
    fn test_sparse_crc_map_sentinel_lifecycle() {
        // Simulates the write → checkpoint → flush lifecycle
        let map = SparseCrcMap::new(10);

        // Write path: store sentinel
        map.store(0, u32::MAX);
        assert_eq!(map.load(0), Some(u32::MAX));
        assert_eq!(map.count(), 1);

        // Checkpoint: clear sentinel, then try_insert real CRC
        map.remove(0);
        assert_eq!(map.count(), 0);
        map.try_insert(0, 0xDEADBEEF);
        assert_eq!(map.load(0), Some(0xDEADBEEF));
        assert_eq!(map.count(), 1);

        // Flush: take CRC for verification
        let crc = map.take(0);
        assert_eq!(crc, Some(0xDEADBEEF));
        assert_eq!(map.count(), 0);
    }

    #[test]
    fn test_sparse_crc_map_concurrent_store_take() {
        use std::sync::Arc;
        use std::thread;

        let map = Arc::new(SparseCrcMap::new(1024));

        // Writer threads store sentinels
        let mut handles = Vec::new();
        for t in 0..4 {
            let map = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                for i in 0..256 {
                    let idx = t * 256 + i;
                    map.store(idx, u32::MAX);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(map.count(), 1024);

        // Taker threads consume all entries
        let mut handles = Vec::new();
        for t in 0..4 {
            let map = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                let mut taken = 0;
                for i in 0..256 {
                    let idx = t * 256 + i;
                    if map.take(idx).is_some() {
                        taken += 1;
                    }
                }
                taken
            }));
        }

        let total_taken: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total_taken, 1024);
        assert_eq!(map.count(), 0);
    }
}
