use bytes::Bytes;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use tracing::{info, instrument, warn};

use crate::nbd::block_map::blake3_128;
use crate::nbd::state::{Active, BlockState, Recovering};

use super::{CacheError, WriteCache};

impl WriteCache<Recovering> {
    /// Skip recovery and transition directly to Active state.
    ///
    /// **TEST ONLY**: This bypasses recovery for unit tests that don't need S3.
    #[allow(dead_code)] // Used by integration tests and benchmarks
    #[cfg(any(test, feature = "test-utils"))]
    pub fn skip_recovery_for_test(self) -> WriteCache<Active> {
        WriteCache {
            inner: self.inner,
            _state: PhantomData,
        }
    }

    /// Recover from a previous session and transition to Active.
    ///
    /// Ensures all dirty blocks have their data in the dirty_store (re-reading
    /// from SSD if necessary). The flush scheduler will upload dirty blocks
    /// via the v2 pack path once the cache is Active.
    ///
    /// This is purely local I/O — no network access required during recovery.
    #[instrument(skip(self))]
    pub async fn finish_recovery(self) -> Result<WriteCache<Active>, CacheError> {
        let dirty_count = self.inner.dirty_block_count.load(Ordering::Relaxed);

        if dirty_count == 0 {
            info!("no dirty blocks, recovery complete");
        } else {
            info!(dirty_blocks = dirty_count, "starting recovery");

            // Ensure all dirty blocks have data in the dirty_store.
            // WAL replay (in init.rs) populates dirty_store for entries in the WAL,
            // but blocks that were dirty when the block map was persisted — and whose
            // WAL entries were subsequently truncated — need to be re-read from SSD.
            let populated = self.populate_missing_dirty_blocks()?;
            if populated > 0 {
                info!(populated, "re-read dirty blocks from SSD into dirty_store");
            }

            // Save metadata after recovery
            self.inner.save_metadata()?;
            info!("recovery complete, dirty blocks will be flushed by scheduler");
        }

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }

    /// Ensure every dirty block has its data in the dirty_store.
    ///
    /// Returns the number of blocks that were re-read from SSD.
    fn populate_missing_dirty_blocks(&self) -> Result<usize, CacheError> {
        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let zero_hash = self.inner.zero_block_hash;
        let mut populated = 0;

        for idx in 0..self.inner.num_blocks {
            let state = self.inner.block_states[idx].load(Ordering::Relaxed);
            if state != BlockState::Dirty as u8 {
                continue;
            }

            let (hash, _seq) = self.inner.block_map_get(idx);

            // Zero-hash entries (empty or trimmed) don't need data in dirty_store
            if hash.is_zero() || hash == zero_hash {
                continue;
            }

            // Already in dirty_store (populated by WAL replay)
            if self.inner.dirty_store.lock().contains_key(&hash) {
                continue;
            }

            // Re-read block from SSD and populate dirty_store
            let offset = idx as u64 * block_size as u64;
            let valid_bytes =
                std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;

            let mut buf = vec![0u8; block_size];
            if valid_bytes > 0 {
                if let Err(e) =
                    self.inner.data_file.read_exact_at(&mut buf[..valid_bytes], offset)
                {
                    warn!(
                        chunk_index = idx,
                        error = %e,
                        "recovery: failed to re-read block from SSD, leaving dirty for retry"
                    );
                    continue;
                }
            }

            // Re-hash from SSD (authoritative) and update block map if hash drifted
            let actual_hash = blake3_128(&buf);
            if actual_hash != hash {
                let seq = self.inner.sequence.next();
                self.inner.block_map_set(idx, actual_hash, seq);
            }

            self.inner
                .dirty_store
                .lock()
                .insert(actual_hash, Bytes::from(buf));
            populated += 1;
        }

        Ok(populated)
    }
}
