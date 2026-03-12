use tracing::{debug, instrument};

use crate::block::cache::BlockCache;
use crate::block::state::Active;
use crate::block::wal::{serialize_entry, serialize_partial_entry};

use super::{CacheError, WriteCache};

impl WriteCache<Active> {
    /// Write data to the cache.
    ///
    /// Data is written to the local SSD and the affected blocks are marked dirty and present.
    /// The write returns immediately after local I/O completes.
    ///
    /// # Lock-Free State Updates
    ///
    /// Uses CAS operations for state transitions:
    /// - Clean → Dirty: increment dirty_count
    /// - Syncing → Dirty: decrement syncing_count, increment dirty_count
    /// - Dirty → Dirty: no-op (WAL entry skipped — already recorded)
    /// Hash computation is deferred to flush-to-S3 time. The write path only
    /// does: set_present → pwrite → mark dirty → invalidate CRC → WAL append.
    #[instrument(skip(self, data, _clean_cache), fields(offset = offset, len = data.len()))]
    pub fn write(
        &self,
        offset: u64,
        data: &[u8],
        _clean_cache: &dyn BlockCache,
    ) -> Result<(), CacheError> {
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
        //
        // For partial blocks: mark sub-regions BEFORE pwrite (same invariant).
        // This ensures background backfill sees the bit set and skips sub-regions
        // the guest is about to write.
        //
        // Snapshot which blocks are partial NOW — we must re-pwrite for these
        // even if complete_partial removes the entry before we reach re-pwrite.
        let mut partial_blocks: Vec<u64> = Vec::new();
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.set_present(idx);
                if self.inner.is_partial(idx) {
                    partial_blocks.push(block);
                    let block_start_byte = block * block_size;
                    let write_start = offset.max(block_start_byte);
                    let write_end = (offset + data.len() as u64).min(block_start_byte + block_size);
                    self.inner.mark_sub_regions(
                        idx,
                        (write_start - block_start_byte) as usize,
                        (write_end - write_start) as usize,
                    );
                }
            }
        }

        // Now write to local file (after claiming blocks via set_present)
        self.inner.data_file.read().write_all_at(data, offset)?;

        // Re-pwrite for blocks that were partial at the start of this write.
        //
        // This guarantees guest data wins over a concurrent backfill write
        // that may have overwritten our pwrite above using stale S3 data.
        //
        // If the DashMap entry still exists, acquire write_lock to serialize
        // with the backfill task's sub-region writes.
        //
        // If complete_partial already removed the entry (backfill finished),
        // re-pwrite unconditionally WITHOUT the lock — no concurrent backfill
        // can race, and the backfill may have overwritten our first pwrite
        // with stale S3 data before calling complete_partial.
        for &block in &partial_blocks {
            let idx = block as usize;
            let block_start_byte = block * block_size;
            let write_start = offset.max(block_start_byte);
            let write_end =
                (offset + data.len() as u64).min(block_start_byte + block_size);
            let data_offset = (write_start - offset) as usize;
            let write_len = (write_end - write_start) as usize;

            let entry = self.inner.partial_blocks.get(&idx);
            let _guard = entry.as_ref().map(|e| e.value().write_lock.lock());
            self.inner.data_file.read().write_all_at(
                &data[data_offset..data_offset + write_len],
                write_start,
            )?;
        }

        // Mark affected blocks as dirty, invalidate stale CRC32 checksums,
        // and batch WAL entries. Lock-free: O_APPEND WAL handles concurrency.
        // Skip WAL append for blocks already dirty (redundant — already recorded).
        let mut batch = Vec::new();
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }
            let state_changed = self.inner.transition_to_dirty(idx);
            self.inner.crc_map.store(idx, super::inner::CRC_SENTINEL);

            let partial_bitmap = self.inner.partial_bitmap(idx);
            if state_changed || partial_bitmap.is_some() {
                let seq = self.inner.sequence.next();
                if let Some(bitmap) = partial_bitmap {
                    serialize_partial_entry(&mut batch, block, seq, bitmap);
                } else {
                    serialize_entry(&mut batch, block, seq);
                }
            }
        }

        if !batch.is_empty() {
            self.inner.wal.append_batch(&batch)?;
            if self.inner.config.wal_sync {
                self.inner.wal.sync()?;
            } else {
                self.inner.wal.flush()?;
            }
        }

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

        if offset + len > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len,
                self.inner.config.device_size,
            ));
        }

        // CRITICAL: Mark blocks as present BEFORE writing zeros to file.
        // Same invariant as write() — prevents prefetch race where prefetch
        // could overwrite our zeros with stale S3 data.
        // For partial blocks: mark sub-regions before zeroing.
        // Snapshot partial blocks for unconditional re-zero (same race fix as write()).
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;
        let mut partial_blocks: Vec<u64> = Vec::new();
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.set_present(idx);
                if self.inner.is_partial(idx) {
                    partial_blocks.push(block);
                    let block_start_byte = block * block_size;
                    let write_start = offset.max(block_start_byte);
                    let write_end = (offset + len).min(block_start_byte + block_size);
                    self.inner.mark_sub_regions(
                        idx,
                        (write_start - block_start_byte) as usize,
                        (write_end - write_start) as usize,
                    );
                }
            }
        }

        // Zero the file range (after claiming blocks via set_present)
        #[cfg(target_os = "linux")]
        {
            let fd = self.inner.data_file.read().as_raw_fd();

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
                    self.zero_range_fallback(offset, len)?;
                } else {
                    return Err(CacheError::Io(err));
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.zero_range_fallback(offset, len)?;
        }

        // Re-zero for blocks that were partial at the start (same fix as write()).
        for &block in &partial_blocks {
            let idx = block as usize;
            let block_start_byte = block * block_size;
            let write_start = offset.max(block_start_byte);
            let write_end = (offset + len).min(block_start_byte + block_size);
            let zero_len = (write_end - write_start) as usize;

            let entry = self.inner.partial_blocks.get(&idx);
            let _guard = entry.as_ref().map(|e| e.value().write_lock.lock());
            self.inner
                .data_file
                .read()
                .write_all_at(&self.inner.zero_block_bytes[..zero_len], write_start)?;
        }

        // Mark affected blocks as dirty, invalidate stale CRCs, and batch
        // WAL entries. Skip redundant entries for already-dirty blocks.
        {
            let block_size = self.inner.config.block_size as u64;
            let start_block = offset / block_size;
            let end_block = (offset + len - 1) / block_size;

            let mut batch = Vec::new();
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }
                let state_changed = self.inner.transition_to_dirty(idx);
                self.inner.crc_map.store(idx, super::inner::CRC_SENTINEL);

                let partial_bitmap = self.inner.partial_bitmap(idx);
                if state_changed || partial_bitmap.is_some() {
                    let seq = self.inner.sequence.next();
                    if let Some(bitmap) = partial_bitmap {
                        serialize_partial_entry(&mut batch, block, seq, bitmap);
                    } else {
                        serialize_entry(&mut batch, block, seq);
                    }
                }
            }

            if !batch.is_empty() {
                self.inner.wal.append_batch(&batch)?;
                if self.inner.config.wal_sync {
                    self.inner.wal.sync()?;
                } else {
                    self.inner.wal.flush()?;
                }
            }
        }

        Ok(())
    }

    /// Fallback zero writing using a static buffer.
    /// Used on non-Linux platforms or when fallocate isn't supported.
    fn zero_range_fallback(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        use std::sync::LazyLock;

        // Static zero buffer - allocated once, reused forever
        const ZERO_CHUNK_SIZE: usize = 128 * 1024; // 128KB
        static ZERO_CHUNK: LazyLock<Box<[u8]>> =
            LazyLock::new(|| vec![0u8; ZERO_CHUNK_SIZE].into_boxed_slice());

        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let chunk_size = (remaining as usize).min(ZERO_CHUNK_SIZE);
            self.inner
                .data_file
                .read()
                .write_all_at(&ZERO_CHUNK[..chunk_size], current_offset)?;
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
    /// (io_uring or pwrite) to prevent prefetch races. See `write()` for the
    /// full invariant explanation.
    ///
    /// After the data write completes, call `post_write()` to finalize metadata.
    /// If the data write fails, the pre_write changes are harmless: blocks are
    /// marked present (not dirty) — recovery handles this.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn pre_write(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        if offset + len > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len,
                self.inner.config.device_size,
            ));
        }
        if len == 0 {
            return Ok(());
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;

        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.set_present(idx);
                if self.inner.is_partial(idx) {
                    let block_start_byte = block * block_size;
                    let write_start = offset.max(block_start_byte);
                    let write_end = (offset + len).min(block_start_byte + block_size);
                    self.inner.mark_sub_regions(
                        idx,
                        (write_start - block_start_byte) as usize,
                        (write_end - write_start) as usize,
                    );
                }
            }
        }

        Ok(())
    }

    /// Phase 2 of a two-phase write: record metadata after data is on disk.
    ///
    /// Marks blocks dirty, invalidates stale CRCs, and appends WAL entries.
    /// Call ONLY after the data write (io_uring or pwrite) has completed successfully.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn post_write(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        if len == 0 {
            return Ok(());
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;

        // Mark affected blocks as dirty, invalidate stale CRCs, and batch
        // WAL entries. Skip redundant entries for already-dirty blocks.
        let mut batch = Vec::new();
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }
            let state_changed = self.inner.transition_to_dirty(idx);
            self.inner.crc_map.store(idx, super::inner::CRC_SENTINEL);

            let partial_bitmap = self.inner.partial_bitmap(idx);
            if state_changed || partial_bitmap.is_some() {
                let seq = self.inner.sequence.next();
                if let Some(bitmap) = partial_bitmap {
                    serialize_partial_entry(&mut batch, block, seq, bitmap);
                } else {
                    serialize_entry(&mut batch, block, seq);
                }
            }
        }

        if !batch.is_empty() {
            self.inner.wal.append_batch(&batch)?;
            if self.inner.config.wal_sync {
                self.inner.wal.sync()?;
            } else {
                self.inner.wal.flush()?;
            }
        }

        tracing::debug!(start_block, end_block, "post_write: marked blocks dirty");
        Ok(())
    }

}
