use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use tracing::{info, instrument, warn};

use crate::block::block_map::SparseBlockState;
use crate::block::state::{Active, Recovering};

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
    /// Verifies dirty blocks are readable from SSD (confirms SSD integrity
    /// before entering Active state). The flush path always computes fresh
    /// blake3 hashes from SSD, so no stored hash needs to be verified.
    #[instrument(skip(self))]
    pub async fn finish_recovery(self) -> Result<WriteCache<Active>, CacheError> {
        let dirty_count = self.inner.dirty_block_count.load(Ordering::Relaxed);

        if dirty_count == 0 {
            info!("no dirty blocks, recovery complete");
        } else {
            info!(dirty_blocks = dirty_count, "starting recovery");

            // Verify dirty blocks are readable from SSD
            let warnings = self.verify_dirty_blocks_readable()?;
            if warnings > 0 {
                warn!(warnings, "some dirty blocks had SSD read errors");
                self.inner
                    .recovery_warnings
                    .fetch_add(warnings as u64, Ordering::Relaxed);
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

    /// Verify dirty blocks are readable from SSD.
    ///
    /// For each DIRTY block, pread from SSD to confirm the data is intact.
    /// The flush path computes fresh blake3 hashes from SSD, so no stored
    /// hash comparison is needed. This only checks SSD readability.
    ///
    /// Returns the number of blocks with read errors.
    fn verify_dirty_blocks_readable(&self) -> Result<usize, CacheError> {
        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let mut warnings = 0;

        for idx in self
            .inner
            .state_map
            .iter_with_state(SparseBlockState::DIRTY)
        {
            let offset = idx as u64 * block_size as u64;
            let valid_bytes =
                std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;

            if valid_bytes == 0 {
                continue;
            }

            let mut buf = vec![0u8; valid_bytes];
            if let Err(e) = self.inner.data_file.read_exact_at(&mut buf, offset) {
                warn!(
                    chunk_index = idx,
                    error = %e,
                    "recovery: failed to read dirty block from SSD"
                );
                warnings += 1;
            }
        }

        Ok(warnings)
    }
}
