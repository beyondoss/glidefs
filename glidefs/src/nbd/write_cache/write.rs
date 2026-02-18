use tracing::{debug, instrument};

use crate::nbd::block_map::Blake3Hash;
use crate::nbd::cache::BlockCache;
use crate::nbd::state::Active;
use crate::nbd::wal::WalEntryRef;

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
    pub fn write(&self, offset: u64, data: &[u8], _clean_cache: &dyn BlockCache) -> Result<(), CacheError> {
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
                self.inner.set_present(idx)?;
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
        {
            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }
                let seq = self.inner.sequence.next();
                // Placeholder hash — flush reads SSD and computes the real hash.
                self.inner.block_map_set(idx, Blake3Hash::ZERO, seq)?;

                let wal_entry = WalEntryRef {
                    name: &self.inner.export_name,
                    chunk_index: block,
                    hash: Blake3Hash::ZERO,
                    sequence: seq,
                };
                self.inner.wal.lock().append(&wal_entry)?;
            }

            // Flush WAL buffer (or fsync if wal_sync is enabled)
            let mut wal = self.inner.wal.lock();
            if self.inner.config.wal_sync {
                wal.sync()?;
            } else {
                wal.flush_buf()?;
            }
        }

        // If using a forked block map, check if overlay is large enough to flatten
        self.try_flatten_block_map();

        debug!(start_block = start_block, end_block = end_block, "marked blocks dirty and present");
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

        // Record dirty blocks in WAL + block map.
        // zero_range uses the precomputed zero_block_hash since we know the content.
        {
            let block_size = self.inner.config.block_size as u64;
            let start_block = offset / block_size;
            let end_block = (offset + len - 1) / block_size;
            let zero_hash = self.inner.zero_block_hash;

            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }

                let seq = self.inner.sequence.next();
                self.inner.block_map_set(idx, zero_hash, seq)?;

                let wal_entry = WalEntryRef {
                    name: &self.inner.export_name,
                    chunk_index: block,
                    hash: zero_hash,
                    sequence: seq,
                };
                self.inner.wal.lock().append(&wal_entry)?;
            }

            let mut wal = self.inner.wal.lock();
            if self.inner.config.wal_sync {
                wal.sync()?;
            } else {
                wal.flush_buf()?;
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

        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }
            // Ignore budget errors for mark_range_dirty_and_present
            // (the block should already be present from the write path).
            let _ = self.inner.set_present(idx);
            self.inner.transition_to_dirty(idx);
        }
    }
}
