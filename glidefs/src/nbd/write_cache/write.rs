use bytes::Bytes;
use std::sync::atomic::Ordering;
use tracing::{debug, instrument};

use crate::nbd::block_map::{blake3_128, ZERO_BLOCK_HASH};
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
        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }
            self.inner.transition_to_dirty(idx);
        }

        // === v2: content-addressed updates ===
        // For each affected chunk, read the full chunk from SSD, hash it,
        // and update block map + WAL + dirty store.
        //
        // The chunk buffer is allocated once and reused across blocks to
        // avoid a 128KB allocation per block on the hot path.
        {
            let block_size = self.inner.config.block_size;
            let device_size = self.inner.config.device_size;
            let mut chunk_buf = vec![0u8; block_size];

            for block in start_block..=end_block {
                let idx = block as usize;
                if idx >= self.inner.num_blocks {
                    continue;
                }

                // Read the full chunk from SSD (to get correct merged data for sub-chunk writes)
                let chunk_offset = block * block_size as u64;
                let valid_bytes = std::cmp::min(block_size as u64, device_size.saturating_sub(chunk_offset)) as usize;
                if valid_bytes > 0 {
                    self.inner.data_file.read_exact_at(&mut chunk_buf[..valid_bytes], chunk_offset)?;
                }
                if valid_bytes < block_size {
                    chunk_buf[valid_bytes..].fill(0);
                }

                let hash = blake3_128(&chunk_buf);
                let seq = self.inner.sequence.next();

                // Update atomic block map (lock-free)
                self.inner.block_map_set(idx, hash, seq);

                // WAL append — metadata only (Mutex, uncontended).
                // Block data lives on SSD; recovery re-reads from there.
                let wal_entry = WalEntryRef {
                    name: &self.inner.export_name,
                    chunk_index: block,
                    hash,
                    sequence: seq,
                };
                self.inner.wal.lock().unwrap().append(&wal_entry)?;

                // Dirty store insert (Mutex, uncontended)
                self.inner.dirty_store.lock().unwrap().insert(hash, Bytes::copy_from_slice(&chunk_buf));
            }

            // Flush WAL buffer
            self.inner.wal.lock().unwrap().flush_buf()?;
        }

        // Budget enforcement — signal flush scheduler if over budget
        if let Some(ref trigger) = self.inner.flush_trigger {
            if self.inner.config.dirty_budget_bytes > 0 {
                let current = self.inner.dirty_bytes.load(Ordering::Relaxed);
                if current > self.inner.config.dirty_budget_bytes {
                    trigger.notify_one();
                }
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

                // WAL entry for zero range
                let wal_entry = WalEntryRef {
                    name: &self.inner.export_name,
                    chunk_index: block,
                    hash: zero_hash,
                    sequence: seq,
                };
                self.inner.wal.lock().unwrap().append(&wal_entry)?;
                // No dirty store insert for zero blocks -- read path returns zeros
            }

            // Flush WAL buffer
            self.inner.wal.lock().unwrap().flush_buf()?;
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
            self.inner.set_present(idx);
            self.inner.transition_to_dirty(idx);
        }
    }
}
