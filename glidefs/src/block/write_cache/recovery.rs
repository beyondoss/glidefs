use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{info, instrument, warn};

use crate::block::block_map::SparseBlockState;
use crate::block::state::{Active, Recovering};

use super::inner::CacheInner;
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
    ///
    /// Runs the blocking SSD I/O (pread + CRC32) on a blocking thread to
    /// avoid starving the async runtime during recovery.
    #[instrument(skip(self))]
    pub async fn finish_recovery(self) -> Result<WriteCache<Active>, CacheError> {
        let dirty_count = self.inner.dirty_block_count.load(Ordering::Relaxed);

        if dirty_count == 0 {
            info!("no dirty blocks, recovery complete");
        } else {
            info!(dirty_blocks = dirty_count, "starting recovery");

            let inner = Arc::clone(&self.inner);
            let warnings = crate::task::spawn_blocking_named("recovery", move || {
                // Verify dirty blocks are readable from SSD
                let warnings = verify_dirty_blocks_readable(&inner)?;

                // Compute CRC32 baselines for dirty blocks before transitioning
                // to Active. Without this, a notify-triggered flush could process
                // recovered dirty blocks before the first checkpoint computes CRCs,
                // bypassing SSD corruption detection.
                super::flush::compute_dirty_crc32s(&inner);

                // Save metadata after recovery
                inner.save_metadata()?;

                Ok::<usize, CacheError>(warnings)
            })
            .await
            .map_err(|e| CacheError::Io(std::io::Error::other(e)))??;

            if warnings > 0 {
                warn!(warnings, "some dirty blocks had SSD read errors");
                self.inner
                    .recovery_warnings
                    .fetch_add(warnings as u64, Ordering::Relaxed);
            }

            info!("recovery complete, dirty blocks will be flushed by scheduler");
        }

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }
}

/// Verify dirty blocks are readable from SSD.
///
/// For each DIRTY block, pread from SSD to confirm the data is intact.
/// The flush path computes fresh blake3 hashes from SSD, so no stored
/// hash comparison is needed. This only checks SSD readability.
///
/// Returns the number of blocks with read errors.
fn verify_dirty_blocks_readable(inner: &CacheInner) -> Result<usize, CacheError> {
    let block_size = inner.config.block_size;
    let device_size = inner.config.device_size;
    let mut warnings = 0;
    let mut buf = vec![0u8; block_size];

    for idx in inner.state_map.iter_with_state(SparseBlockState::DIRTY) {
        let offset = idx as u64 * block_size as u64;
        let valid_bytes =
            std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;

        if valid_bytes == 0 {
            continue;
        }

        if let Err(e) = inner.data_file.read_exact_at(&mut buf[..valid_bytes], offset) {
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
