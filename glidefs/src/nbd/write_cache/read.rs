use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{debug, error, instrument};

use std::sync::LazyLock;

use crate::nbd::block_map::{blake3_128, lz4_decompress, ZERO_BLOCK_HASH};
use crate::nbd::block_store::S3BlockStore;
use crate::nbd::cache::BlockCache;
use crate::nbd::content_store::ContentStore;
use crate::nbd::pack;
use crate::nbd::pack_index::HostPackIndex;
use crate::nbd::state::Active;

use super::{CacheError, WriteCache};

/// Static zero buffer (128KB) — allocated once, shared across all reads of unwritten chunks.
/// Avoids a 128KB heap allocation on every sparse read.
static ZERO_BLOCK_BYTES: LazyLock<Bytes> = LazyLock::new(|| Bytes::from(vec![0u8; 128 * 1024]));

impl WriteCache<Active> {
    /// Read data from the cache, fetching from S3 if blocks are not present locally.
    ///
    /// This is the primary read path for NBD I/O. Blocks that haven't been written
    /// locally are fetched from S3 on demand (read-through caching).
    #[instrument(skip(self, s3, metrics), fields(offset = offset, len = len))]
    pub async fn read_with_fetch(
        &self,
        offset: u64,
        len: usize,
        s3: &S3BlockStore,
        metrics: &super::super::metrics::ExportMetrics,
    ) -> Result<Bytes, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len as u64 - 1) / block_size;

        // Check which blocks need to be fetched from S3 (lock-free)
        let blocks_to_fetch: Vec<u64> = (start_block..=end_block)
            .filter(|&block| !self.inner.is_present(block as usize))
            .collect();

        // Record cache hits/misses
        let total_blocks = (end_block - start_block + 1) as usize;
        let cache_misses = blocks_to_fetch.len();
        let cache_hits = total_blocks - cache_misses;
        for _ in 0..cache_hits {
            metrics.record_cache_hit();
        }
        for _ in 0..cache_misses {
            metrics.record_cache_miss();
        }

        // Fetch missing blocks from S3 using batch prefetching
        // Groups blocks by S3 batch to reduce round-trips
        if !blocks_to_fetch.is_empty() {
            let s3_start = Instant::now();
            self.fetch_blocks_batched(s3, blocks_to_fetch, metrics).await?;
            metrics.record_s3_fetch_latency(s3_start.elapsed());
        }

        // Now read from local cache
        let file_read_start = Instant::now();
        let result = self.read_local(offset, len);
        metrics.record_file_read_latency(file_read_start.elapsed());
        result
    }

    /// v2 read path: resolve blocks by content hash through tiered storage.
    ///
    /// Resolution order: block_map → dirty_store → clean_cache → S3 pack fetch.
    /// On S3 cache miss, the entire pack (~25 blocks) is fetched and all blocks
    /// are decompressed, verified, and inserted into the clean cache.
    #[instrument(skip(self, clean_cache, pack_index, content_store, metrics), fields(offset = offset, len = len))]
    pub async fn read_v2(
        &self,
        offset: u64,
        len: usize,
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
        metrics: &super::super::metrics::ExportMetrics,
    ) -> Result<Bytes, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        let chunk_size = self.inner.config.block_size as u64;
        let start_chunk = offset / chunk_size;
        let end_chunk = (offset + len as u64 - 1) / chunk_size;

        let mut result = Vec::with_capacity(len);

        for chunk_idx in start_chunk..=end_chunk {
            let chunk_data = self
                .resolve_chunk(chunk_idx as usize, clean_cache, pack_index, content_store, Some(metrics))
                .await?;

            // Slice out the portion of this chunk that overlaps the requested range.
            let chunk_start_byte = chunk_idx * chunk_size;
            let slice_start = if chunk_idx == start_chunk {
                (offset - chunk_start_byte) as usize
            } else {
                0
            };
            let slice_end = if chunk_idx == end_chunk {
                let end_byte = offset + len as u64;
                let relative_end = (end_byte - chunk_start_byte) as usize;
                std::cmp::min(relative_end, chunk_data.len())
            } else {
                chunk_data.len()
            };

            result.extend_from_slice(&chunk_data[slice_start..slice_end]);
        }

        debug!(chunks = end_chunk - start_chunk + 1, "v2 read complete");
        Ok(Bytes::from(result))
    }

    /// Prefetch a single chunk into the clean cache.
    ///
    /// Triggers pack-level sibling prefetch: fetching one block from a pack
    /// automatically caches all blocks in that pack via `resolve_chunk`.
    pub async fn prefetch_chunk(
        &self,
        chunk_index: usize,
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
    ) -> Result<(), CacheError> {
        let (hash, _seq) = self.inner.block_map_get(chunk_index);
        if hash.is_zero() || hash == *ZERO_BLOCK_HASH {
            return Ok(());
        }
        if clean_cache.get(&hash).await.is_some() {
            return Ok(());
        }
        let _ = self
            .resolve_chunk(chunk_index, clean_cache, pack_index, content_store, None)
            .await;
        Ok(())
    }

    /// Prefetch multiple chunks with bounded concurrency.
    ///
    /// Used for boot hot set prefetch: given a list of chunk indices, resolves
    /// each one (triggering pack-level sibling prefetch). Pack-level dedup means
    /// the second chunk from the same pack hits the clean cache immediately.
    pub async fn prefetch_chunks(
        &self,
        chunk_indices: &[u64],
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
    ) {
        use futures::stream::{self, StreamExt};

        stream::iter(chunk_indices.iter().copied())
            .for_each_concurrent(8, |chunk_idx| async move {
                let _ = self
                    .prefetch_chunk(chunk_idx as usize, clean_cache, pack_index, content_store)
                    .await;
            })
            .await;
    }

    /// Resolve a single chunk through the v2 tier hierarchy.
    ///
    /// 1. block_map lookup → if zero hash, return zeros
    /// 2. dirty_store → in-memory dirty block (~100ns)
    /// 3. clean_cache → previously fetched and decompressed block (~100ns)
    /// 4. S3 pack fetch → fetch entire pack, decompress all blocks, cache them
    async fn resolve_chunk(
        &self,
        chunk_index: usize,
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
        metrics: Option<&super::super::metrics::ExportMetrics>,
    ) -> Result<Bytes, CacheError> {
        let chunk_size = self.inner.config.block_size;
        let (hash, _seq) = self.inner.block_map_get(chunk_index);

        // Never written or trimmed → zeros.
        if hash.is_zero() || hash == *ZERO_BLOCK_HASH {
            return Ok(ZERO_BLOCK_BYTES.slice(..chunk_size));
        }

        // Tier 1: dirty_store (in-memory, not yet flushed to S3).
        if let Some(data) = self.inner.dirty_store.lock().get(&hash) {
            return Ok(data.clone());
        }

        // Tier 2: clean_cache (previously fetched from S3).
        if let Some(data) = clean_cache.get(&hash).await {
            return Ok(data);
        }

        // Tier 3: S3 pack fetch — find the pack, fetch it, ingest all blocks.
        let pack_loc = pack_index.get(&hash).ok_or(CacheError::BlockNotFound { hash })?;

        let pack_data = match content_store.get_pack(pack_loc.pack_id).await {
            Ok(data) => data,
            Err(e) => {
                if let Some(m) = metrics {
                    m.record_s3_get_error();
                }
                return Err(e.into());
            }
        };
        let pack_idx = pack::parse_pack_index(&pack_data)
            .map_err(|e| CacheError::PackFormat(e.to_string()))?;

        for entry in &pack_idx.entries {
            let compressed = pack::extract_block(&pack_data, entry.offset, entry.comp_length)
                .ok_or_else(|| {
                    CacheError::PackFormat(format!(
                        "block at offset {} length {} out of bounds in pack {}",
                        entry.offset, entry.comp_length, pack_loc.pack_id
                    ))
                })?;

            let decompressed = lz4_decompress(compressed)
                .map_err(|e| CacheError::DecompressFailed(e.to_string()))?;

            // Verify hash.
            let actual_hash = blake3_128(&decompressed);
            if actual_hash != entry.hash {
                return Err(CacheError::HashMismatch {
                    expected: format!("{:?}", entry.hash),
                });
            }

            clean_cache.insert(entry.hash, Bytes::from(decompressed));
        }

        // The requested block should now be in the cache.
        clean_cache.get(&hash).await.ok_or(CacheError::BlockNotFound { hash })
    }

    /// Fetch multiple blocks from S3, grouping by batch to reduce round-trips.
    ///
    /// When multiple blocks need fetching, this method groups them by S3 batch
    /// and fetches each batch once. For example, if blocks 0, 5, and 8 all belong
    /// to batch 0 (with 100 blocks per batch), we fetch the entire batch once
    /// and extract all three blocks.
    ///
    /// This is much faster than individual block fetches for sequential reads
    /// or reads that span multiple blocks in the same batch.
    #[instrument(skip(self, s3, metrics), fields(blocks = blocks.len()))]
    async fn fetch_blocks_batched(
        &self,
        s3: &S3BlockStore,
        blocks: Vec<u64>,
        metrics: &super::super::metrics::ExportMetrics,
    ) -> Result<(), CacheError> {
        use std::collections::HashMap;

        if blocks.is_empty() {
            return Ok(());
        }

        let block_size = self.inner.config.block_size;

        // Group blocks by S3 batch number
        let mut blocks_by_batch: HashMap<u64, Vec<u64>> = HashMap::new();
        for block_num in blocks {
            let batch_num = s3.batch_num(block_num);
            blocks_by_batch.entry(batch_num).or_default().push(block_num);
        }

        let num_batches = blocks_by_batch.len();
        debug!(
            batches = num_batches,
            "fetching blocks grouped by S3 batch"
        );

        // Fetch each batch and extract needed blocks
        for (batch_num, _block_nums) in blocks_by_batch {
            // Fetch the entire batch from S3
            // Note: get_batch_with_etag returns zeros (not error) if batch doesn't exist
            let batch_result = match s3.get_batch_with_etag(batch_num).await {
                Ok(r) => r,
                Err(e) => {
                    error!(batch = batch_num, error = %e, "S3 batch fetch failed");
                    return Err(e.into());
                }
            };
            metrics.record_s3_read(batch_result.data.len() as u64);
            let batch_data = batch_result.data;

            // Cache ALL blocks from the batch (not just requested ones)
            // This maximizes cache hits for sequential reads
            let blocks_per_batch = s3.blocks_per_batch();
            let first_block_in_batch = batch_num * blocks_per_batch;
            let mut blocks_cached = 0usize;

            for i in 0..blocks_per_batch {
                let block_num = first_block_in_batch + i;

                // Skip blocks past device end
                if block_num as usize >= self.inner.num_blocks {
                    break;
                }

                // Skip blocks already present
                if self.inner.is_present(block_num as usize) {
                    continue;
                }

                let offset_in_batch = (i as usize) * block_size;
                let cache_offset = block_num * block_size as u64;

                // Extract block data from batch (with bounds checking)
                let block_data = if offset_in_batch + block_size <= batch_data.len() {
                    &batch_data[offset_in_batch..offset_in_batch + block_size]
                } else {
                    // Partial block at end of batch - use zeros for remainder
                    break;
                };

                // Try to atomically claim this block for prefetching.
                // Use CAS on present bit: if someone else set it while we were
                // fetching from S3, they won the race and we skip this block.
                let chunk_idx = (block_num as usize) / 64;
                let bit_idx = (block_num as usize) % 64;
                let bit_mask = 1u64 << bit_idx;

                // CAS loop to set present bit atomically
                let was_present = loop {
                    let old = self.inner.present_chunks[chunk_idx].load(Ordering::Acquire);
                    if (old & bit_mask) != 0 {
                        // Someone else set it - they won the race
                        break true;
                    }
                    // Try to set the bit
                    match self.inner.present_chunks[chunk_idx].compare_exchange(
                        old,
                        old | bit_mask,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break false, // We set it
                        Err(_) => continue,   // Retry
                    }
                };

                if was_present {
                    // A concurrent write won the race - skip this block
                    // The write's data is authoritative
                    continue;
                }

                // We own this block now (we set present). Write S3 data to cache.
                if let Err(e) = self.inner.data_file.write_all_at(block_data, cache_offset) {
                    error!(block = block_num, offset = cache_offset, error = %e, "Failed to write S3 data to cache file");
                    return Err(e.into());
                }

                blocks_cached += 1;
            }

            debug!(
                batch = batch_num,
                blocks_cached = blocks_cached,
                "cached all blocks from S3 batch"
            );
        }

        Ok(())
    }

    /// Read data from local cache only (no S3 fetch).
    ///
    /// Used internally and by sync worker. Caller must ensure blocks are present.
    #[instrument(skip(self), fields(offset = offset, len = len))]
    pub fn read_local(&self, offset: u64, len: usize) -> Result<Bytes, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        let mut buf = vec![0u8; len];
        self.inner.data_file.read_exact_at(&mut buf, offset)?;

        Ok(Bytes::from(buf))
    }

    /// Legacy read method - reads from local cache only.
    ///
    /// **WARNING**: Returns zeros for blocks not present locally.
    /// Use `read_with_fetch` for NBD I/O to get proper S3 read-through.
    #[allow(dead_code)] // Used by tests
    #[instrument(skip(self), fields(offset = offset, len = len))]
    pub fn read(&self, offset: u64, len: usize) -> Result<Bytes, CacheError> {
        self.read_local(offset, len)
    }

    /// Read a single block from local cache.
    /// Handles partial last block correctly when device_size is not a multiple of block_size.
    #[allow(dead_code)] // Used by tests
    pub fn read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        self.sync_read_local_block(block_num)
    }

    /// Read a single block for sync worker.
    /// Reads from local disk cache (likely from page cache for recently written blocks).
    ///
    /// For the last block of a device, if device_size is not a multiple of block_size,
    /// this reads only the valid bytes and pads the rest with zeros. This is necessary
    /// because the cache file is sized to device_size, not num_blocks * block_size.
    pub fn sync_read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let offset = block_num * block_size as u64;

        // Calculate how many bytes are valid for this block
        // For most blocks this is block_size, but for the last partial block
        // it may be less (device_size - offset)
        let valid_bytes = if offset >= device_size {
            // Block is entirely beyond device - shouldn't happen but handle gracefully
            return Ok(Bytes::from(vec![0u8; block_size]));
        } else {
            std::cmp::min(block_size as u64, device_size - offset) as usize
        };

        let mut buf = vec![0u8; block_size];
        if valid_bytes == block_size {
            // Full block - read normally
            self.inner.data_file.read_exact_at(&mut buf, offset)?;
        } else {
            // Partial block (last block) - read only valid bytes, rest stays zero
            self.inner
                .data_file
                .read_exact_at(&mut buf[..valid_bytes], offset)?;
        }
        Ok(Bytes::from(buf))
    }
}
