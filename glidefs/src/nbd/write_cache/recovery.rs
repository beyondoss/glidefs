use bytes::Bytes;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use tracing::{info, instrument, warn};

use crate::nbd::block_store::S3BlockStore;
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

    /// Sync all dirty blocks from previous session and transition to Active.
    ///
    /// This uploads any blocks that were dirty when the cache was last closed
    /// (or when the process crashed).
    #[instrument(skip(self, s3))]
    pub async fn finish_recovery(
        self,
        s3: &S3BlockStore,
    ) -> Result<WriteCache<Active>, CacheError> {
        let dirty_count = self.inner.dirty_block_count.load(Ordering::Relaxed);

        if dirty_count == 0 {
            info!("no dirty blocks, recovery complete");
        } else {
            info!(dirty_blocks = dirty_count, "starting recovery sync");

            // Collect and sync dirty blocks using batched writes
            let dirty_blocks = self.collect_dirty_blocks();
            match self.sync_blocks_batched(s3, dirty_blocks).await {
                Ok(synced) => {
                    info!(synced = synced, "recovery sync complete");
                }
                Err(e) => {
                    warn!(error = %e, "recovery sync failed, will retry on next startup");
                }
            }

            // Save metadata after recovery
            self.inner.save_metadata()?;
            info!("recovery complete");
        }

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }

    fn collect_dirty_blocks(&self) -> Vec<u64> {
        self.inner
            .block_states
            .iter()
            .enumerate()
            .filter(|(_, s)| s.load(Ordering::Relaxed) == BlockState::Dirty as u8)
            .map(|(i, _)| i as u64)
            .collect()
    }

    /// Sync dirty blocks to S3 using batch writes with conditional PUT.
    async fn sync_blocks_batched(&self, s3: &S3BlockStore, block_nums: Vec<u64>) -> Result<usize, CacheError> {
        use std::collections::HashMap;

        if block_nums.is_empty() {
            return Ok(0);
        }

        // Group blocks by batch number
        let mut batches: HashMap<u64, Vec<u64>> = HashMap::new();
        for block_num in block_nums {
            let batch_num = s3.batch_num(block_num);
            batches.entry(batch_num).or_default().push(block_num);
        }

        let mut synced_count = 0;

        // Process each batch
        for (batch_num, blocks_in_batch) in batches {
            // GET existing batch with ETag for conditional PUT
            let batch_result = s3.get_batch_with_etag(batch_num).await?;
            let mut batch_data = batch_result.data;
            let etag = batch_result.etag;

            // Update dirty block slots with local data
            for &block_num in &blocks_in_batch {
                let local_data = self.read_local_block(block_num)?;
                let offset = s3.offset_in_batch(block_num) as usize;
                batch_data[offset..offset + local_data.len()].copy_from_slice(&local_data);
            }

            // Conditional PUT: only succeed if no one else modified the batch
            s3.put_batch_conditional(batch_num, batch_data, etag).await?;

            // Mark all blocks in this batch as synced
            for block_num in blocks_in_batch {
                self.mark_synced(block_num);
                synced_count += 1;
            }
        }

        Ok(synced_count)
    }

    fn read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let offset = block_num * block_size as u64;

        // Calculate valid bytes for this block (handles partial last block)
        let valid_bytes = if offset >= device_size {
            return Ok(Bytes::from(vec![0u8; block_size]));
        } else {
            std::cmp::min(block_size as u64, device_size - offset) as usize
        };

        let mut buf = vec![0u8; block_size];
        if valid_bytes == block_size {
            // Use sync FD to avoid contention with NBD reads during recovery
            self.inner.data_file.read_exact_at(&mut buf, offset)?;
        } else {
            // Partial block (last block) - read only valid bytes, rest stays zero
            self.inner
                .data_file
                .read_exact_at(&mut buf[..valid_bytes], offset)?;
        }

        Ok(Bytes::from(buf))
    }

    fn mark_synced(&self, block_num: u64) {
        let idx = block_num as usize;
        if idx >= self.inner.num_blocks {
            return;
        }

        // CAS loop: Dirty|Syncing -> Clean
        loop {
            let current = self.inner.block_states[idx].load(Ordering::Acquire);
            if current != BlockState::Dirty as u8 && current != BlockState::Syncing as u8 {
                break;
            }

            if self.inner.block_states[idx]
                .compare_exchange(
                    current,
                    BlockState::Clean as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.inner.dirty_block_count.fetch_sub(1, Ordering::Relaxed);
                break;
            }
            // CAS failed, retry
        }
    }
}
