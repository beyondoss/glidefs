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
    /// Verifies dirty block hashes against SSD data and corrects block_map
    /// entries where the SSD state has drifted (e.g. due to writes that made
    /// it to SSD but not to the WAL before crash). This is purely local I/O.
    #[instrument(skip(self))]
    pub async fn finish_recovery(self) -> Result<WriteCache<Active>, CacheError> {
        let dirty_count = self.inner.dirty_block_count.load(Ordering::Relaxed);

        if dirty_count == 0 {
            info!("no dirty blocks, recovery complete");
        } else {
            info!(dirty_blocks = dirty_count, "starting recovery");

            // Verify dirty block hashes against SSD data and correct any drift.
            let corrected = self.verify_dirty_block_hashes()?;
            if corrected > 0 {
                info!(corrected, "corrected dirty block hashes from SSD");
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

    /// Verify dirty block hashes against SSD data.
    ///
    /// Re-reads each dirty block from SSD and re-hashes. If the hash doesn't
    /// match the block_map entry (e.g. a write made it to SSD but the WAL
    /// entry was lost), update the block_map with the correct hash.
    ///
    /// Returns the number of blocks whose hashes were corrected.
    fn verify_dirty_block_hashes(&self) -> Result<usize, CacheError> {
        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let zero_hash = self.inner.zero_block_hash;
        let mut corrected = 0;

        for idx in 0..self.inner.num_blocks {
            let state = self.inner.block_states[idx].load(Ordering::Relaxed);
            if state != BlockState::Dirty as u8 {
                continue;
            }

            let (hash, _seq) = self.inner.block_map_get(idx);

            // Zero-hash entries don't need verification
            if hash.is_zero() || hash == zero_hash {
                continue;
            }

            // Re-read block from SSD and verify hash
            let offset = idx as u64 * block_size as u64;
            let valid_bytes =
                std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;

            let mut buf = vec![0u8; block_size];
            if valid_bytes > 0
                && let Err(e) =
                    self.inner.data_file.read_exact_at(&mut buf[..valid_bytes], offset)
            {
                warn!(
                    chunk_index = idx,
                    error = %e,
                    "recovery: failed to read block from SSD, leaving dirty for retry"
                );
                continue;
            }

            let actual_hash = blake3_128(&buf);
            if actual_hash != hash {
                // SSD state drifted — update block_map with correct hash
                let seq = self.inner.sequence.next();
                self.inner.block_map_set(idx, actual_hash, seq);
                corrected += 1;
            }
        }

        Ok(corrected)
    }
}
