#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
use tracing::{debug, instrument};

use crate::block::block_map::SparseBlockState;
use crate::block::state::Active;
use crate::block::wal::serialize_entry;

use super::{CacheError, WriteCache};

impl WriteCache<Active> {
    /// Build WAL entries for non-dirty blocks, append to WAL, sync if configured,
    /// then transition states to DIRTY.
    ///
    /// Must be called while holding a `data_file` lock guard (read or write).
    /// The `df` reference is needed for `sync_all()` when `wal_sync` is enabled.
    fn wal_append_and_mark_dirty(
        &self,
        df: &super::inner::SyncFile,
        start_block: u64,
        end_block: u64,
    ) -> Result<(), CacheError> {
        use smallvec::SmallVec;
        let mut batch = Vec::new();
        let mut to_dirty: SmallVec<[usize; 16]> = SmallVec::new();
        for block in start_block..=end_block {
            #[allow(clippy::cast_possible_truncation)]
            let idx = block as usize; // usize >= u64 on 64-bit systems
            if idx >= self.inner.num_blocks {
                continue;
            }
            let state = self.inner.state_map.get(idx);
            if state != SparseBlockState::DIRTY {
                let seq = self.inner.sequence.next();
                serialize_entry(&mut batch, block, seq);
                to_dirty.push(idx);
            }
        }

        // Transition to Dirty before WAL append. Checkpoint persists the
        // state map then truncates the WAL — if it ran between append and
        // dirty, it would persist Clean and discard the WAL entry, losing
        // the write on crash. Reordering ensures checkpoint always sees
        // Dirty. The WAL append after is the crash-recovery backstop for
        // "dirty in memory but not yet persisted to metadata".
        for idx in &to_dirty {
            self.inner.transition_to_dirty(*idx);
        }

        if !batch.is_empty() {
            self.inner.wal.append_batch(&batch)?;
            if self.inner.config.wal_sync {
                df.sync_all()?;
                self.inner.wal.sync()?;
            } else {
                self.inner.wal.flush()?;
            }
        }

        Ok(())
    }

    /// Write data to the cache.
    ///
    /// Data is written to the local SSD and the affected blocks are marked dirty and present.
    /// The write returns immediately after local I/O completes.
    ///
    /// # Lock-Free State Updates
    ///
    /// Uses CAS operations for state transitions:
    /// - Clean → Dirty: increment dirty_count (normal path after set_present)
    /// - Syncing → Dirty: decrement syncing_count, increment dirty_count
    /// - Dirty → Dirty: no-op (WAL entry skipped — already recorded)
    /// Hash computation is deferred to flush-to-S3 time. The write path only
    /// does: set_present → pwrite → WAL append → mark dirty.
    #[instrument(skip(self, data), fields(offset = offset, len = data.len()))]
    pub fn write(
        &self,
        offset: u64,
        data: &[u8],
    ) -> Result<(), CacheError> {
        self.write_inner(offset, data, false)
    }

    /// Write data with eviction detection for sub-block backfill safety.
    ///
    /// Like `write()`, but returns `CacheError::BlockEvicted` if any block
    /// in the range was evicted (NOT_PRESENT) and the flushing file is gone.
    /// Used by the handler's backfill path to detect flush eviction races
    /// and retry with a fresh S3 fetch.
    pub fn write_with_eviction_check(
        &self,
        offset: u64,
        data: &[u8],
    ) -> Result<(), CacheError> {
        self.write_inner(offset, data, true)
    }

    fn write_inner(
        &self,
        offset: u64,
        data: &[u8],
        require_promotion: bool,
    ) -> Result<(), CacheError> {
        let end = offset.checked_add(data.len() as u64).ok_or_else(|| {
            CacheError::offset_out_of_bounds(u64::MAX, self.inner.config.device_size)
        })?;
        if end > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                end,
                self.inner.config.device_size,
            ));
        }

        if data.is_empty() {
            return Ok(());
        }

        // Calculate affected blocks
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (end - 1) / block_size;

        // Hold the data_file read lock across promote + pwrite + dirty marking.
        //
        // rotate_and_snapshot() acquires the data_file WRITE lock to snapshot
        // dirty blocks and swap files atomically. If we release the read lock
        // between pwrite and transition_to_dirty, rotation can interleave:
        //   1. pwrite completes (data in old file, read lock released)
        //   2. rotate_and_snapshot takes write lock, snapshots DIRTY blocks,
        //      swaps files (block is CLEAN, not in snapshot)
        //   3. transition_to_dirty marks block DIRTY (but data is in the
        //      flushing file, not the new active file)
        // The flushing file is deleted after flush, permanently losing the data.
        //
        // Holding the read lock across all operations prevents rotation from
        // interleaving. The read lock is shared, so concurrent writers are
        // not blocked — only rotation waits until all writers release.
        let df = self.inner.data_file.read();

        // Promote SYNCING/NOT_PRESENT blocks from flushing → active BEFORE
        // set_present. promote_syncing_blocks needs to see the real block
        // state to recover data from the flushing file. If set_present runs
        // first, it masks NOT_PRESENT as CLEAN, and promote silently skips —
        // leaving zeros on the active file for sub-block writes.
        self.inner.promote_syncing_blocks(&df, start_block, end_block, require_promotion)?;

        // Mark blocks present AFTER promote but BEFORE pwrite.
        // This prevents a race with prefetch where:
        // 1. Prefetch sees is_present=false
        // 2. Write does pwrite(new_data)
        // 3. Prefetch does pwrite(s3_data) - OVERWRITES new_data
        //
        // By setting present first, prefetch's CAS will fail if we've
        // claimed the block, or our pwrite will overwrite their stale data.
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.set_present(idx);
            }
        }

        df.write_all_at(data, offset)?;

        self.inner.capture_page_crcs(offset, data);

        self.wal_append_and_mark_dirty(&df, start_block, end_block)?;

        drop(df);

        debug!(
            start_block = start_block,
            end_block = end_block,
            "marked blocks dirty and present"
        );
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

        let end = offset.checked_add(len).ok_or_else(|| {
            CacheError::offset_out_of_bounds(u64::MAX, self.inner.config.device_size)
        })?;
        if end > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                end,
                self.inner.config.device_size,
            ));
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (end - 1) / block_size;

        // Hold the data_file read lock across promote + set_present + zero +
        // dirty marking (same rotation race as write() — see comment there).
        let df = self.inner.data_file.read();

        // Promote SYNCING blocks from flushing → active before zeroing.
        // Also recovers NOT_PRESENT blocks if flushing file is still available.
        self.inner.promote_syncing_blocks(&df, start_block, end_block, false)?;

        // Mark blocks present AFTER promote but BEFORE zero write.
        // Same invariant as write() — prevents prefetch race where prefetch
        // could overwrite our zeros with stale S3 data.
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.set_present(idx);
            }
        }

        // Zero the file range (after claiming blocks via set_present)
        #[cfg(target_os = "linux")]
        {
            let fd = df.as_raw_fd();

            // FALLOC_FL_ZERO_RANGE = 0x10
            // This zeros the range without deallocating - keeps the file contiguous
            const FALLOC_FL_ZERO_RANGE: libc::c_int = 0x10;

            let ret = unsafe {
                libc::fallocate(
                    fd,
                    FALLOC_FL_ZERO_RANGE,
                    offset as libc::off_t,
                    len as libc::off_t,
                )
            };

            if ret != 0 {
                let err = std::io::Error::last_os_error();
                // If fallocate isn't supported, fall back to writing zeros
                if err.raw_os_error() == Some(libc::EOPNOTSUPP)
                    || err.raw_os_error() == Some(libc::ENOTSUP)
                {
                    self.zero_range_fallback_with(&df, offset, len)?;
                } else {
                    return Err(CacheError::Io(err));
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.zero_range_fallback_with(&df, offset, len)?;
        }

        self.inner.capture_zero_page_crcs(offset, len);

        self.wal_append_and_mark_dirty(&df, start_block, end_block)?;

        drop(df);

        Ok(())
    }

    /// Fallback zero writing using a static buffer and a pre-acquired file guard.
    fn zero_range_fallback_with(
        &self,
        df: &super::inner::SyncFile,
        offset: u64,
        len: u64,
    ) -> Result<(), CacheError> {
        use std::sync::LazyLock;

        // Static zero buffer - allocated once, reused forever
        const ZERO_CHUNK_SIZE: usize = 128 * 1024; // 128KB
        static ZERO_CHUNK: LazyLock<Box<[u8]>> =
            LazyLock::new(|| vec![0u8; ZERO_CHUNK_SIZE].into_boxed_slice());

        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let chunk_size = (remaining as usize).min(ZERO_CHUNK_SIZE);
            df.write_all_at(&ZERO_CHUNK[..chunk_size], current_offset)?;
            remaining -= chunk_size as u64;
            current_offset += chunk_size as u64;
        }

        Ok(())
    }

    /// Check if any block in the given range is not yet present on SSD.
    ///
    /// Used by the write rejection path: when SSD is near-full, writes to
    /// already-present blocks are allowed (overwrites don't grow the data file),
    /// but writes to new blocks are rejected with ENOSPC.
    pub fn has_new_blocks(&self, offset: u64, len: usize) -> bool {
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len as u64 - 1) / block_size;
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                return true;
            }
            if !self.inner.is_present(idx) {
                return true;
            }
        }
        false
    }

    /// Phase 1 of a two-phase write: prepare blocks before data lands on disk.
    ///
    /// Marks blocks as present. This MUST be called before the data write
    /// (pwrite) to prevent prefetch races. See `write()` for the full
    /// invariant explanation.
    ///
    /// After pre_write, call `pwrite_and_commit()` to write data and mark
    /// dirty under one lock. If the data write fails, the pre_write changes
    /// are harmless: blocks are marked present (not dirty) — recovery handles
    /// this.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn pre_write(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        let end = offset.checked_add(len).ok_or_else(|| {
            CacheError::offset_out_of_bounds(u64::MAX, self.inner.config.device_size)
        })?;
        if end > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                end,
                self.inner.config.device_size,
            ));
        }
        if len == 0 {
            return Ok(());
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (end - 1) / block_size;

        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.set_present(idx);
            }
        }

        Ok(())
    }

    /// Combined pwrite + dirty-marking under one data_file read lock.
    ///
    /// Prevents rotate_and_snapshot() from interleaving between the data
    /// write and the dirty marking. Without this, rotation can snapshot
    /// dirty blocks AFTER pwrite but BEFORE transition_to_dirty, causing
    /// the block's data to be stranded in the flushing file (deleted after
    /// flush) while the block stays DIRTY in the new active file (zeros).
    ///
    /// The read lock is shared — concurrent writers are not blocked. Only
    /// rotation (which takes the write lock) waits for all writers.
    ///
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn pwrite_and_commit(
        &self,
        offset: u64,
        data: &[u8],
    ) -> Result<(), CacheError> {
        let len = data.len() as u64;
        if len == 0 {
            return Ok(());
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;

        // Hold the data_file read lock across pwrite + dirty marking.
        // See cache.write() for the full explanation of the race.
        let df = self.inner.data_file.read();

        // Promote SYNCING blocks from flushing → active before pwrite.
        // Also recovers NOT_PRESENT blocks if flushing file is still available.
        // require_promotion=true: if the flushing file was already taken and
        // any block is NOT_PRESENT/SYNCING, return BlockEvicted so the ublk
        // handler can fall back to the full write path with S3 backfill.
        self.inner.promote_syncing_blocks(&df, start_block, end_block, true)?;

        df.write_all_at(data, offset)?;

        self.inner.capture_page_crcs(offset, data);

        self.wal_append_and_mark_dirty(&df, start_block, end_block)?;

        drop(df);

        tracing::debug!(start_block, end_block, "pwrite_and_commit: wrote + marked dirty");
        Ok(())
    }

}
