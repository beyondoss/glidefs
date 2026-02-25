use tracing::{debug, instrument};

use crate::block::block_map::Blake3Hash;
use crate::block::cache::BlockCache;
use crate::block::state::Active;
use crate::block::wal::WalEntryRef;

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
    /// - Clean → Dirty: increment dirty_count, push to queue, notify
    /// - Syncing → Dirty: decrement syncing_count, increment dirty_count, push to queue, notify
    /// - Dirty → Dirty: no-op
    /// Hash computation is deferred to flush-to-S3 time. The write path only
    /// does: pwrite → mark dirty → WAL append. This keeps the hot path to
    /// ~10-15µs instead of ~90µs (no 128KB pread, no blake3, no cache insert).
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
        // CRITICAL: Clear CRC32 BEFORE pwrite. Between pwrite and block_map_set there
        // is a window where SSD data has changed but the sequence number is stale. A
        // concurrent flush during this window would see a CRC mismatch with matching
        // sequence — indistinguishable from real SSD corruption. Clearing the CRC first
        // ensures the flush sees CRC=0 and skips the check. The second clear after
        // block_map_set (in the WAL block below) handles the case where checkpoint
        // recomputes a CRC between our clear and pwrite.
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.set_present(idx);
                self.inner.block_map_clear_crc32(idx);
            }
        }

        // Now write to local file (after claiming blocks via set_present)
        self.inner.data_file.write_all_at(data, offset)?;

        // Mark affected blocks as dirty (lock-free)
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }
            self.inner.transition_to_dirty(idx);
        }

        // Record dirty blocks in WAL + block map with placeholder hash.
        // Real hash is computed at flush-to-S3 time from SSD data.
        // Batch all WAL entries under a single lock acquisition to reduce
        // lock/unlock overhead on multi-block writes.
        {
            let mut wal = self.inner.wal.lock();
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }
                let seq = self.inner.sequence.next();
                // Placeholder hash — flush reads SSD and computes the real hash.
                self.inner.block_map_set(idx, Blake3Hash::ZERO, seq);
                // Clear CRC32: data changed, old checksum is stale.
                // Next checkpoint will recompute from fresh SSD data.
                self.inner.block_map_clear_crc32(idx);

                let wal_entry = WalEntryRef {
                    name: &self.inner.export_name,
                    chunk_index: block,
                    sequence: seq,
                };
                wal.append(&wal_entry)?;
            }

            // Flush WAL buffer (or fsync if wal_sync is enabled)
            if self.inner.config.wal_sync {
                wal.sync()?;
            } else {
                wal.flush_buf()?;
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

        // Clear CRC32 before SSD write (same race as write() — see comment there).
        {
            let block_size = self.inner.config.block_size as u64;
            let start_block = offset / block_size;
            let end_block = (offset + len - 1) / block_size;
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx < self.inner.num_blocks {
                    self.inner.block_map_clear_crc32(idx);
                }
            }
        }

        // Zero the file range
        #[cfg(target_os = "linux")]
        {
            let fd = self.inner.data_file.as_raw_fd();

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

        // Record dirty blocks in WAL + block map.
        // zero_range uses the precomputed zero_block_hash since we know the content.
        // Batch all WAL entries under a single lock acquisition.
        {
            let block_size = self.inner.config.block_size as u64;
            let start_block = offset / block_size;
            let end_block = (offset + len - 1) / block_size;
            let zero_hash = self.inner.zero_block_hash;

            let mut wal = self.inner.wal.lock();
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }

                let seq = self.inner.sequence.next();
                self.inner.block_map_set(idx, zero_hash, seq);
                self.inner.block_map_clear_crc32(idx);

                let wal_entry = WalEntryRef {
                    name: &self.inner.export_name,
                    chunk_index: block,
                    sequence: seq,
                };
                wal.append(&wal_entry)?;
            }

            if self.inner.config.wal_sync {
                wal.sync()?;
            } else {
                wal.flush_buf()?;
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
    /// Marks blocks as present and clears CRC32 checksums. This MUST be called
    /// before the data write (io_uring or pwrite) to prevent prefetch races and
    /// CRC corruption windows. See `write()` for the full invariant explanation.
    ///
    /// After the data write completes, call `post_write()` to finalize metadata.
    /// If the data write fails, the pre_write changes are harmless: blocks are
    /// marked present (not dirty) with cleared CRCs — recovery handles this.
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
                self.inner.block_map_clear_crc32(idx);
            }
        }

        Ok(())
    }

    /// Phase 2 of a two-phase write: record metadata after data is on disk.
    ///
    /// Marks blocks dirty, appends WAL entries, and updates the block map.
    /// Call ONLY after the data write (io_uring or pwrite) has completed successfully.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn post_write(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        if len == 0 {
            return Ok(());
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;

        // Mark affected blocks as dirty (lock-free CAS)
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx < self.inner.num_blocks {
                self.inner.transition_to_dirty(idx);
            }
        }

        // Record dirty blocks in WAL + block map with placeholder hash.
        {
            let mut wal = self.inner.wal.lock();
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }
                let seq = self.inner.sequence.next();
                self.inner.block_map_set(idx, Blake3Hash::ZERO, seq);
                self.inner.block_map_clear_crc32(idx);

                let wal_entry = WalEntryRef {
                    name: &self.inner.export_name,
                    chunk_index: block,
                    sequence: seq,
                };
                wal.append(&wal_entry)?;
            }

            if self.inner.config.wal_sync {
                wal.sync()?;
            } else {
                wal.flush_buf()?;
            }
        }

        tracing::debug!(start_block, end_block, "post_write: marked blocks dirty");
        Ok(())
    }

    /// Mark a range of blocks as dirty and present (lock-free).
    fn mark_range_dirty_and_present(&self, offset: u64, len: u64) {
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;

        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }
            // Ignore budget errors for mark_range_dirty_and_present
            // (the block should already be present from the write path).
            self.inner.set_present(idx);
            self.inner.transition_to_dirty(idx);
        }
    }
}
