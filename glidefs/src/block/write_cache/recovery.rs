use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{info, instrument, warn};

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
    /// Computes CRC32 baselines for all dirty blocks (parallel via rayon),
    /// which also serves as an SSD readability check. Blocks that fail
    /// pread are logged as warnings but do not prevent recovery.
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
                // Compute CRC32 baselines for dirty blocks before transitioning
                // to Active. This also verifies SSD readability — blocks that
                // fail pread are counted as warnings (no separate pass needed).
                let warnings = super::flush::compute_dirty_crc32s(&inner);

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
