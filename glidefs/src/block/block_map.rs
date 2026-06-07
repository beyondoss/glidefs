// Truncating casts here are block-index/page arithmetic: `u64 → usize` (lossless
// on the 64-bit targets GlideFS runs on) and small-range values like
// `(idx % ENTRIES_PER_BYTE) * 2`. Scoped to this one lint so `cast_possible_wrap`
// and `cast_sign_loss` stay active and catch genuinely new sign/wrap mistakes.
#![allow(clippy::cast_possible_truncation)]
//! Block state tracking and content-addressed hashing.
//!
//! This module provides:
//! - `Blake3Hash`: 16-byte truncated BLAKE3 hash for content addressing
//! - `SparseBlockState` / `SparseStateMap`: Lock-free sparse block state tracking
//! - `SequenceNumber`: Monotonic counter for WAL ordering
//! - LZ4 compress/decompress helpers

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ptr;

#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicPtr, AtomicU64, AtomicU8, Ordering};
#[cfg(not(feature = "loom"))]
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
fn zero_block_hash(block_size: usize) -> Blake3Hash {
    blake3_128(&vec![0u8; block_size])
}

/// Returns zero-block bytes and hash for the given block size.
///
/// Memoized: computes once per unique block_size, returns cached result
/// thereafter. Typical deployments use 1-2 block sizes so the linear
/// scan is negligible.
pub fn shared_zero_block(block_size: usize) -> (Bytes, Blake3Hash) {
    use parking_lot::Mutex;
    use std::sync::LazyLock;

    static CACHE: LazyLock<Mutex<Vec<(usize, Bytes, Blake3Hash)>>> =
        LazyLock::new(|| Mutex::new(Vec::with_capacity(2)));

    let cache = CACHE.lock();
    if let Some((_, bytes, hash)) = cache.iter().find(|(bs, _, _)| *bs == block_size) {
        return (bytes.clone(), *hash);
    }
    drop(cache);

    let bytes = Bytes::from(vec![0u8; block_size]);
    let hash = zero_block_hash(block_size);

    let mut cache = CACHE.lock();
    // Double-check after re-acquiring (another thread may have inserted).
    if let Some((_, bytes, hash)) = cache.iter().find(|(bs, _, _)| *bs == block_size) {
        return (bytes.clone(), *hash);
    }
    cache.push((block_size, bytes.clone(), hash));
    (bytes, hash)
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
    /// Block is claimed but not yet dirty. Transient state used by the write
    /// path (set_present before pwrite) and as a CAS gate for backfill
    /// synchronization.
    pub const CLEAN: u8 = 1;
    /// Block is present on SSD and needs to be flushed to S3.
    pub const DIRTY: u8 = 2;
    /// Block is present on SSD and an upload is in progress.
    pub const SYNCING: u8 = 3;
}

/// Typed block state returned by `WriteCache::block_state()`.
///
/// Wraps the raw `u8` from `SparseStateMap` to prevent misuse at the public API
/// boundary. Internal CAS operations continue using raw `u8` for efficiency.
///
/// `PartialEq<u8>` is provided for test ergonomics (`assert_eq!(state, 2)`).
/// Production code should prefer the named methods (`is_dirty()`, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockState(u8);

impl PartialEq<u8> for BlockState {
    fn eq(&self, other: &u8) -> bool {
        self.0 == *other
    }
}

impl std::fmt::Display for BlockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            0 => write!(f, "NOT_PRESENT(0)"),
            1 => write!(f, "CLEAN(1)"),
            2 => write!(f, "DIRTY(2)"),
            3 => write!(f, "SYNCING(3)"),
            v => write!(f, "UNKNOWN({v})"),
        }
    }
}

impl BlockState {
    #[inline]
    pub fn is_not_present(self) -> bool {
        self.0 == SparseBlockState::NOT_PRESENT
    }

    #[inline]
    pub fn is_clean(self) -> bool {
        self.0 == SparseBlockState::CLEAN
    }

    #[inline]
    pub fn is_dirty(self) -> bool {
        self.0 == SparseBlockState::DIRTY
    }

    #[inline]
    pub fn is_syncing(self) -> bool {
        self.0 == SparseBlockState::SYNCING
    }

    /// True if block has data on local SSD (DIRTY or SYNCING).
    #[inline]
    pub fn has_local_data(self) -> bool {
        self.is_dirty() || self.is_syncing()
    }

    /// Raw value for test-only instrumentation (BackfillEvent).
    #[inline]
    pub fn raw(self) -> u8 {
        self.0
    }

    #[inline]
    pub(crate) fn from_raw(v: u8) -> Self {
        BlockState(v)
    }
}

/// Result of `SparseStateMap::transition_to_dirty()`.
///
/// Tells the caller which source state the block transitioned from,
/// so it can update the appropriate counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirtyTransition {
    /// Block was already DIRTY — no state change.
    AlreadyDirty,
    /// Transitioned from CLEAN or NOT_PRESENT → DIRTY.
    FromCleanOrNotPresent,
    /// Transitioned from SYNCING → DIRTY.
    FromSyncing,
}

/// Number of 2-bit entries packed into each `AtomicU8`.
const ENTRIES_PER_BYTE: usize = 4;

// Under loom, use tiny pages to avoid stack overflow (loom's AtomicU8 is large)
// and keep the state space tractable.
#[cfg(not(feature = "loom"))]
const STATE_PAGE_BYTES: usize = 4096;
#[cfg(feature = "loom")]
const STATE_PAGE_BYTES: usize = 4;

const STATE_PAGE_ENTRIES: usize = STATE_PAGE_BYTES * ENTRIES_PER_BYTE;
// log2(STATE_PAGE_ENTRIES)
#[cfg(not(feature = "loom"))]
const STATE_PAGE_BITS: usize = 14; // log2(16384)
#[cfg(feature = "loom")]
const STATE_PAGE_BITS: usize = 4; // log2(16)
const STATE_PAGE_MASK: usize = STATE_PAGE_ENTRIES - 1;

/// A page of 16,384 block state entries packed into 4,096 bytes (one OS page).
///
/// Each `AtomicU8` holds 4 entries at 2 bits each:
/// - bits [1:0] = entry 0
/// - bits [3:2] = entry 1
/// - bits [5:4] = entry 2
/// - bits [7:6] = entry 3
#[cfg_attr(not(feature = "loom"), repr(C, align(4096)))]
struct StatePage {
    data: [AtomicU8; STATE_PAGE_BYTES],
}

impl StatePage {
    fn new_boxed() -> Box<Self> {
        #[cfg(not(feature = "loom"))]
        {
            // SAFETY: All-zeros is valid for StatePage because AtomicU8::new(0) is
            // represented as a zero byte with #[repr(C)] layout.
            unsafe { Box::new_zeroed().assume_init() }
        }
        #[cfg(feature = "loom")]
        {
            // Loom's AtomicU8 has internal tracking state and cannot be
            // zero-initialized. Construct each element explicitly.
            Box::new(StatePage {
                data: std::array::from_fn(|_| AtomicU8::new(0)),
            })
        }
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
/// - `1` = Clean (claimed, not yet dirty — transient)
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
        self.try_set_present(idx);
    }

    /// Try to claim a NOT_PRESENT block (CAS NOT_PRESENT → CLEAN).
    ///
    /// Returns `true` if this call transitioned the block from NOT_PRESENT
    /// to CLEAN (caller "won" the claim). Returns `false` if the block was
    /// already present (CLEAN, DIRTY, or SYNCING).
    #[inline]
    pub fn try_set_present(&self, idx: usize) -> bool {
        let (page_idx, byte_idx, shift) = Self::split_index(idx);
        let page = self.ensure_page(page_idx);
        let mask = 0x3u8 << shift;
        loop {
            let old = page.data[byte_idx].load(Ordering::Acquire);
            if (old >> shift) & 0x3 != SparseBlockState::NOT_PRESENT {
                return false; // already present
            }
            let new = (old & !mask) | (SparseBlockState::CLEAN << shift);
            if page.data[byte_idx]
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
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

    // -- High-level transition methods ----------------------------------------
    //
    // These encode the CAS loop logic that CacheInner delegates to.
    // Placing them here means loom tests exercise the real code path.

    /// CAS loop to transition a block to Dirty.
    ///
    /// Handles all four source states:
    /// - **DIRTY → DIRTY**: no-op, returns `DirtyTransition::AlreadyDirty`.
    /// - **CLEAN or NOT_PRESENT → DIRTY**: returns `DirtyTransition::FromCleanOrNotPresent`.
    /// - **SYNCING → DIRTY**: returns `DirtyTransition::FromSyncing`.
    ///
    /// The caller is responsible for updating counters based on the result.
    pub fn transition_to_dirty(&self, idx: usize) -> DirtyTransition {
        loop {
            let current = self.get(idx);

            if current == SparseBlockState::DIRTY {
                return DirtyTransition::AlreadyDirty;
            }

            if current == SparseBlockState::CLEAN
                || current == SparseBlockState::NOT_PRESENT
            {
                if self.cas(idx, current, SparseBlockState::DIRTY).is_ok() {
                    return DirtyTransition::FromCleanOrNotPresent;
                }
            } else if current == SparseBlockState::SYNCING {
                if self
                    .cas(idx, SparseBlockState::SYNCING, SparseBlockState::DIRTY)
                    .is_ok()
                {
                    return DirtyTransition::FromSyncing;
                }
            } else {
                unreachable!("invalid 2-bit block state: {current}");
            }
        }
    }

    /// Single CAS: DIRTY → SYNCING.
    ///
    /// Returns `true` if the CAS succeeded (block claimed for flush).
    /// No retry loop — a single attempt. Caller is responsible for counters.
    #[inline]
    pub fn transition_dirty_to_syncing(&self, idx: usize) -> bool {
        self.cas(idx, SparseBlockState::DIRTY, SparseBlockState::SYNCING)
            .is_ok()
    }

    /// Single CAS: SYNCING → NOT_PRESENT.
    ///
    /// Returns `true` if the CAS succeeded (block evicted).
    /// Caller is responsible for counters.
    #[inline]
    pub fn transition_syncing_to_not_present(&self, idx: usize) -> bool {
        self.cas(idx, SparseBlockState::SYNCING, SparseBlockState::NOT_PRESENT)
            .is_ok()
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
    /// Uses `AcqRel` on the successful update (and `Acquire` on the read for
    /// the retry) so a value advanced by one thread is guaranteed visible to
    /// the next caller — including across the handoff `advance_to` boundary on
    /// weakly-ordered targets (aarch64). A purely `Relaxed` counter could let
    /// a post-handoff write reuse a sequence number `advance_to` already
    /// skipped past, breaking WAL replay ordering.
    /// Saturates at `u64::MAX` instead of wrapping, which would violate
    /// the monotonicity invariant that WAL replay depends on.
    #[inline]
    pub fn next(&self) -> u64 {
        match self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
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
        self.0.load(Ordering::Acquire)
    }

    /// Atomically advance the counter to at least `target`. Used by the
    /// handoff successor's tail-replay path to skip past WAL entries
    /// the predecessor wrote between WARMING and FREEZE, so future
    /// writes on this successor get monotonically-increasing sequence
    /// numbers strictly greater than anything in the inherited WAL.
    #[inline]
    pub fn advance_to(&self, target: u64) {
        let _ = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                if v < target { Some(target) } else { None }
            });
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
// Codec-agnostic block compression (LZ4 legacy + zstd)
// ============================================================================
//
// New packs are written with zstd; existing LZ4 packs stay readable forever.
// The codec is detected on read by sniffing the zstd frame magic — no on-disk
// format change, and per-block self-describing frames survive compaction (which
// reuses each block's original compressed bytes when merging packs).

/// First 4 bytes of every zstd frame (RFC 8878). An `lz4_compress` block can
/// never collide: it begins with a u32-LE uncompressed size ≤ 2 MiB, so its
/// byte[3] is always `0x00`, whereas zstd requires `0xFD`.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Cap on decompressed block size — guards against an adversarial/corrupt frame
/// claiming a huge size and OOMing the process (matches the LZ4 path's bound).
const MAX_DECOMPRESSED_SIZE: usize = 2 * 1024 * 1024;

/// Sentinel `compression_level` selecting the legacy LZ4 codec on write. Any
/// other value selects zstd at that level. The read path always auto-detects,
/// so this only governs what new packs are written as.
pub const COMPRESSION_LZ4: i32 = 0;

/// Default zstd level for runtime volume flushes. Level 1 compresses at roughly
/// LZ4 speed (near cost-neutral on guest-serving hosts) while still ~23% smaller.
pub const COMPRESSION_RUNTIME_DEFAULT: i32 = 1;

/// zstd level for bless (offline, write-once/read-many). Highest ratio (~37%
/// smaller than LZ4); slow to compress but bless is offline and decode speed is
/// ~level-independent, so the most-read data pays only at build time.
pub const COMPRESSION_BLESS: i32 = 19;

/// Default compression level applied to a freshly-opened cache. zstd-1 unless
/// overridden by the `GLIDEFS_COMPRESSION_LEVEL` env var (set it to `0` to pin
/// legacy LZ4, or e.g. `19` for max ratio). zstd is the system default so a
/// cache-open path that forgets to choose still gets compression rather than
/// silently falling back to LZ4. bless still overrides to `COMPRESSION_BLESS`.
pub fn env_default_compression_level() -> i32 {
    std::env::var("GLIDEFS_COMPRESSION_LEVEL")
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(COMPRESSION_RUNTIME_DEFAULT)
}

/// Error from `decompress_block`, preserving which codec failed.
#[derive(Debug)]
pub enum CompressError {
    Lz4(lz4_flex::block::DecompressError),
    Zstd(std::io::Error),
}

impl std::fmt::Display for CompressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressError::Lz4(e) => write!(f, "lz4 decompress: {e}"),
            CompressError::Zstd(e) => write!(f, "zstd decompress: {e}"),
        }
    }
}

impl std::error::Error for CompressError {}

/// Compress with zstd at `level`. zstd's bulk compressor only errors on OOM or
/// internal failure, neither of which is recoverable here.
#[inline]
pub fn zstd_compress(data: &[u8], level: i32) -> Vec<u8> {
    zstd::bulk::compress(data, level).expect("zstd bulk compress (fails only on OOM)")
}

/// Compress a block with the codec selected by `level`: `COMPRESSION_LZ4`
/// selects legacy LZ4, any other value selects zstd at that level. Used by all
/// production write paths (flush, ext4 ingest).
#[inline]
pub fn compress_block(data: &[u8], level: i32) -> Vec<u8> {
    if level == COMPRESSION_LZ4 {
        lz4_compress(data)
    } else {
        zstd_compress(data, level)
    }
}

/// Decompress a block, auto-detecting LZ4 vs zstd by frame magic. Used by all
/// production read paths so mixed-codec packs (during/after migration) read
/// transparently. Output is capped at `MAX_DECOMPRESSED_SIZE`.
#[inline]
pub fn decompress_block(compressed: &[u8]) -> Result<Vec<u8>, CompressError> {
    if compressed.len() >= 4 && compressed[..4] == ZSTD_MAGIC {
        zstd_decompress(compressed, MAX_DECOMPRESSED_SIZE).map_err(CompressError::Zstd)
    } else {
        lz4_decompress(compressed).map_err(CompressError::Lz4)
    }
}

/// Decompress a zstd frame into a **fallibly-allocated, right-sized** buffer,
/// bounded by `max_size`.
///
/// `zstd::bulk::decompress(_, cap)` allocates `Vec::with_capacity(cap)`
/// *infallibly* on every call: without the crate's `experimental` feature it
/// can't read the frame's content size, so it always reserves the full cap. On
/// any read path that is two bugs at once:
///
///   1. **Abort surface.** Under host memory pressure that `Vec::with_capacity`
///      routes failure through `handle_alloc_error` → `SIGABRT`, taking down the
///      daemon (and storage for every VM) instead of failing one read. With a
///      2 MiB cap this is the `memory allocation of 2097152 bytes failed` crash.
///   2. **Over-allocation.** Real payloads are far smaller than the cap, but
///      every decode reserves the whole cap regardless — churn on hot paths.
///
/// We read the frame's declared content size (one-shot `zstd::bulk::compress`
/// records it in the header), clamp it to `max_size`, and `try_reserve_exact`
/// so a genuine OOM returns `Err` — the caller fails that operation instead of
/// aborting. The cap is still enforced two ways: an absent/oversized declared
/// size clamps to `max_size`, and `decompress_to_buffer` errors rather than
/// growing past the buffer's capacity, so a corrupt or adversarial frame can
/// neither OOM nor overflow us.
pub(crate) fn zstd_decompress(
    compressed: &[u8],
    max_size: usize,
) -> Result<Vec<u8>, std::io::Error> {
    // Declared decompressed size from the frame header, if present and sane.
    let declared = zstd::zstd_safe::get_frame_content_size(compressed)
        .ok()
        .flatten()
        .map(|n| n as usize)
        .filter(|&n| n <= max_size);
    let capacity = declared.unwrap_or(max_size);

    let mut buf = Vec::new();
    buf.try_reserve_exact(capacity).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "zstd decompress buffer allocation failed (host OOM) — failing operation instead of aborting",
        )
    })?;

    let mut dec = zstd::bulk::Decompressor::new()?;
    dec.decompress_to_buffer(compressed, &mut buf)?;
    Ok(buf)
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

    #[test]
    fn test_zstd_roundtrip() {
        let mut data = vec![0u8; 131072];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        for level in [1, 3, 19] {
            let c = compress_block(&data, level);
            assert!(c.len() < data.len(), "zstd-{level} should shrink");
            assert_eq!(decompress_block(&c).unwrap(), data, "zstd-{level} roundtrip");
        }
    }

    #[test]
    fn test_codec_autodetect_reads_both() {
        // The read path must transparently handle LZ4 (legacy) and zstd (new),
        // detecting the codec from the frame — this is what keeps old packs
        // readable after the compressor swap.
        let data: Vec<u8> = (0u32..70000)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();

        let lz4 = compress_block(&data, COMPRESSION_LZ4);
        let zstd = compress_block(&data, 3);

        // zstd frames carry the magic; LZ4 size-prefix never does (byte[3]==0).
        assert_eq!(&zstd[..4], &ZSTD_MAGIC);
        assert_ne!(&lz4[..4], &ZSTD_MAGIC);
        assert_eq!(lz4[3], 0, "LZ4 size-prefix high byte is 0, never 0xFD");

        assert_eq!(decompress_block(&lz4).unwrap(), data, "auto-detect LZ4");
        assert_eq!(decompress_block(&zstd).unwrap(), data, "auto-detect zstd");
    }

    #[test]
    fn test_decompress_block_reads_legacy_lz4_helper() {
        // A block written by the pre-swap `lz4_compress` must still decode.
        let data = b"legacy pack written before the zstd swap";
        let legacy = lz4_compress(data);
        assert_eq!(decompress_block(&legacy).unwrap(), data);
    }

    #[test]
    fn test_legacy_lz4_decompress_rejects_zstd_frame() {
        // Documents the single-shot rollback floor: a pre-swap binary calling
        // the raw LZ4 decompressor on a new zstd pack must FAIL cleanly (the
        // zstd magic's high byte 0xFD reads as a >2MiB claimed size → guard
        // trips), never silently return wrong bytes.
        let zstd = compress_block(b"written after the swap", 3);
        assert!(lz4_decompress(&zstd).is_err(), "old LZ4 path must reject a zstd frame, not corrupt");
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

}
