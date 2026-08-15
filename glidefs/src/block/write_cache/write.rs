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

    /// Write a MATERIALIZED full block (S3 pre-image, possibly merged with
    /// guest data) for a block claimed via
    /// [`claim_block_for_materialization`](Self::claim_block_for_materialization).
    ///
    /// Returns `Ok(true)` if the write landed, `Ok(false)` if the claim was
    /// STOLEN — a concurrent guest write committed the block (CLEAN→DIRTY via
    /// `wal_append_and_mark_dirty`, which dirties any non-DIRTY block) while
    /// our fetch was in flight. In that case the block holds NEWER guest
    /// data; landing our (stale) pre-image would roll it back, so we skip.
    /// The caller falls back to writing only its own guest sub-range (or
    /// nothing, for a pure backfill).
    ///
    /// **Why the data_file WRITE lock:** the stealer's data write may be a
    /// kernel `WRITE_FIXED` whose only userspace bracket is the rotation
    /// read-gate held from dispatch to commit. Acquiring the write lock
    /// drains every in-flight gated writer first, so after the CLEAN
    /// re-check under the lock there is no unordered data write left to
    /// race: still-CLEAN ⇒ nobody wrote (any future writer enters through
    /// the gate we now hold); DIRTY ⇒ a steal committed and we abort.
    /// (Found by the stateright faithful model — a straggler full-block
    /// writer whose pre_write predates our claim can land data + commit
    /// between our claim and our pwrite.)
    ///
    /// **Why the state check is not enough on its own:** `CLEAN` is not an
    /// identity. Across a slow fetch the block can be stolen, uploaded by a
    /// flush, evicted to `NOT_PRESENT` and then re-claimed by a LATER writer
    /// (the ublk ZC `pre_write` marks its blocks present before the kernel
    /// lands the bytes) — arriving back at `CLEAN` with someone else's write
    /// pending. Landing the pre-image there resurrects it over data that has
    /// already been acknowledged and uploaded. So the commit is gated on the
    /// claim's token as well, in the same critical section: `is_valid` ⇒ no
    /// bypassing `NOT_PRESENT → CLEAN` has happened since we claimed.
    pub fn write_materialized(
        &self,
        offset: u64,
        data: &[u8],
        claim: &super::flush::MaterializationClaim<'_>,
    ) -> Result<bool, CacheError> {
        use crate::block::block_map::SparseBlockState;
        debug_assert!(
            std::ptr::eq(claim.cache_inner, std::sync::Arc::as_ptr(&self.inner)),
            "materialization claim belongs to a different cache"
        );
        let block_idx = claim.block_idx();
        if data.is_empty() {
            return Ok(true);
        }
        let end = offset.checked_add(data.len() as u64).ok_or_else(|| {
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

        let df = self.inner.data_file.write();
        // Claim-identity + state check and the pwrite are one critical
        // section: `if_valid` holds the claim map, so a bypassing
        // NOT_PRESENT→CLEAN (which invalidates first, flips second) can
        // neither be missed here nor slip in before the pwrite lands.
        let landed = self
            .inner
            .promote_claim
            .if_valid(block_idx, claim.token, || -> Result<bool, CacheError> {
                if self.inner.state_map.get(block_idx) != SparseBlockState::CLEAN {
                    // Stolen: a guest write committed newer data for this block.
                    return Ok(false);
                }
                df.write_all_at(data, offset)?;
                self.inner.capture_page_crcs(offset, data);
                self.wal_append_and_mark_dirty(&df, start_block, end_block)?;
                Ok(true)
            })
            // Claim no longer ours: the block was re-claimed by a later
            // writer while we fetched. Our pre-image is stale — skip it.
            .unwrap_or(Ok(false))?;
        drop(df);
        Ok(landed)
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
        // Single-attach fence: once a strictly-newer generation has taken this
        // volume, reject all further guest writes. They could never durably
        // commit (the manifest sync is fenced), so ACKing them would be a lie.
        if self
            .inner
            .fenced
            .load(std::sync::atomic::Ordering::Acquire)
        {
            let held = *self.inner.current_generation.lock();
            return Err(CacheError::Fenced {
                held,
                superseded_by: 0,
            });
        }

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

    /// Step 1 of ublk zero-copy write: promote any SYNCING blocks in the
    /// range BEFORE the kernel `IORING_OP_WRITE_FIXED` writes new data.
    ///
    /// The stateright model (`stateright-model/src/lib.rs`) requires the
    /// write phase order `Promote* → PwriteData → WalAppend →
    /// TransitionDirty`. For USER_COPY, `pwrite_and_commit` runs all four
    /// under the data_file read lock in that order. For ZC the kernel
    /// does the data write (PwriteData) via WRITE_FIXED, and we have to
    /// arrange for promote to happen *before* that — otherwise promote's
    /// pwrite copies the *flushing*-file contents (the old version) on
    /// top of the just-written new data, silently rolling the write back.
    ///
    /// Call sequence the ZC dispatcher follows:
    /// 1. `pre_write` — state machine prep, no lock held
    /// 2. acquire the rotation gate (read_arc)
    /// 3. **this method** — promote any SYNCING blocks to DIRTY, copying
    ///    flushing→active under the held gate
    /// 4. submit WRITE_FIXED (kernel overwrites with new data)
    /// 5. `commit_after_zc_write_with` — WAL append + state transition
    ///    (no more promote)
    /// 6. drop gate
    ///
    /// **Lock requirement**: caller holds the rotation gate; pass the
    /// `&SyncFile` from the guard. Re-acquiring `data_file.read()` while
    /// the inflight guard is held deadlocks against a queued rotation
    /// writer under parking_lot's task-fair policy.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn zc_promote_for_write_with(
        &self,
        df: &super::inner::SyncFile,
        offset: u64,
        length: u64,
    ) -> Result<(), CacheError> {
        if length == 0 {
            return Ok(());
        }
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + length - 1) / block_size;

        // `require_promotion` hinges on whether the kernel WRITE_FIXED will
        // overwrite EVERY touched block in full.
        //
        // * Block-aligned, block-multiple write (`fully_covers`): WRITE_FIXED
        //   replaces all bytes of every block, so we don't need the prior
        //   data. `require_promotion=false` lets an evicted block stay
        //   NOT_PRESENT — the kernel write reconstructs it whole. (Preserves
        //   the large-IO fast path that motivated ZC.)
        //
        // * SUB-BLOCK write (`!fully_covers`): WRITE_FIXED touches only part
        //   of the block; the remainder must already be present in the active
        //   file. If a flush evicted the block in the unguarded gap between
        //   `pre_write` and gate acquisition (DIRTY→SYNCING→NOT_PRESENT,
        //   flushing file dropped), promoting a no-op here would leave the
        //   remainder as sparse zeros, the commit would still mark the block
        //   DIRTY, and the half-zero block would be uploaded — read back as
        //   zeros after a cold restart. That is the `fio_verify_random_cold_
        //   wake` corruption. `require_promotion=true` surfaces it as
        //   `BlockEvicted` so the caller (`BlockHandler::zc_prepare_write`)
        //   re-backfills from S3 and retries — exactly what USER_COPY's
        //   `pwrite_and_commit` does (require_promotion=true) and
        //   `backfill_and_write` retries on.
        let fully_covers = offset % block_size == 0 && length % block_size == 0;
        self.inner
            .promote_syncing_blocks(df, start_block, end_block, !fully_covers)?;
        Ok(())
    }

    /// Step 2 of ublk zero-copy write: WAL append + state transition,
    /// AFTER the kernel `WRITE_FIXED` has landed the data. See
    /// [`zc_promote_for_write_with`](Self::zc_promote_for_write_with)
    /// for the full write-phase ordering and why promote does NOT happen
    /// here (it must run before the kernel's data write to satisfy the
    /// stateright model's `Promote* → PwriteData → WalAppend →
    /// TransitionDirty` invariant).
    ///
    /// The CRC capture that `pwrite_and_commit` does is omitted — for ZC
    /// the data never enters userspace, so we have no source bytes to
    /// hash. Flush handles a missing CRC entry by skipping per-page
    /// verification (see `flush.rs` `stored_crc == 0` branch).
    ///
    /// See `BlockHandler::commit_after_zc_write_with` for the integrity
    /// contract: cache-file corruption in the WRITE_FIXED→flush window
    /// is the gap we accept, same as every Linux ZC block-storage stack;
    /// BLAKE3 still covers the S3 path end-to-end.
    ///
    /// **Lock requirement**: caller holds the rotation gate; pass the
    /// `&SyncFile` from the guard.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn commit_after_zc_write_with(
        &self,
        df: &super::inner::SyncFile,
        offset: u64,
        length: u64,
    ) -> Result<(), CacheError> {
        if length == 0 {
            return Ok(());
        }
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + length - 1) / block_size;
        // No promote here — already done by `zc_promote_for_write_with`
        // before WRITE_FIXED. Promoting again would re-copy from the
        // flushing file (now containing pre-WRITE_FIXED data) on top of
        // the kernel's just-landed new data — silent write rollback.
        self.wal_append_and_mark_dirty(df, start_block, end_block)?;
        Ok(())
    }
}
