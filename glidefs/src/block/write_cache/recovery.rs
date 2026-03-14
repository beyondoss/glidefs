use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{info, instrument};

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
    /// Persists metadata so crash recovery state is durable before serving I/O.
    /// SSD readability issues surface at the first flush cycle (CRC pre-pass).
    #[instrument(skip(self))]
    pub async fn finish_recovery(self) -> Result<WriteCache<Active>, CacheError> {
        let dirty_count = self.inner.dirty_block_count.load(Ordering::Relaxed);

        if dirty_count == 0 {
            info!("no dirty blocks, recovery complete");
        } else {
            info!(dirty_blocks = dirty_count, "starting recovery");

            let inner = Arc::clone(&self.inner);
            crate::task::spawn_blocking_named("recovery", move || {
                inner.save_metadata()
            })
            .await
            .map_err(|e| CacheError::Io(std::io::Error::other(e)))??;

            info!("recovery complete, dirty blocks will be flushed by scheduler");
        }

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }
}
