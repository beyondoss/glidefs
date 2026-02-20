use bytes::Bytes;
use tracing::{debug, instrument, warn};

use crate::block::block_map::{blake3_128, lz4_decompress};
use crate::block::cache::BlockCache;
use crate::block::content_store::ContentStore;
use crate::block::pack;
use crate::block::pack_index::HostPackIndex;
use crate::block::state::Active;

use super::{CacheError, WriteCache};

impl WriteCache<Active> {
    /// Read blocks by content hash through tiered storage.
    ///
    /// Resolution order per chunk: block_map → clean_cache → S3 → SSD pread fallback.
    /// Chunks are resolved concurrently — a multi-block read that spans multiple packs
    /// fetches them in parallel rather than serially.
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
        let num_chunks = (end_chunk - start_chunk + 1) as usize;

        // Single chunk fast path — no concurrency overhead.
        if num_chunks == 1 {
            let chunk_data = self
                .resolve_chunk(
                    start_chunk as usize,
                    clean_cache,
                    pack_index,
                    content_store,
                    Some(metrics),
                )
                .await?;
            let chunk_start_byte = start_chunk * chunk_size;
            let slice_start = (offset - chunk_start_byte) as usize;
            let slice_end = slice_start + len;
            return Ok(chunk_data.slice(slice_start..std::cmp::min(slice_end, chunk_data.len())));
        }

        // Multi-chunk: resolve all concurrently.
        let futures: Vec<_> = (start_chunk..=end_chunk)
            .map(|chunk_idx| {
                self.resolve_chunk(
                    chunk_idx as usize,
                    clean_cache,
                    pack_index,
                    content_store,
                    Some(metrics),
                )
            })
            .collect();
        let chunks = futures::future::try_join_all(futures).await?;

        let mut result = Vec::with_capacity(len);
        for (i, chunk_data) in chunks.into_iter().enumerate() {
            let chunk_idx = start_chunk + i as u64;
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

        debug!(chunks = num_chunks, "read complete");
        Ok(Bytes::from(result))
    }

    /// Prefetch a single chunk into the clean cache.
    ///
    /// Fetches the entire pack containing this chunk, decompressing and caching
    /// all sibling blocks for read-ahead benefit.
    pub async fn prefetch_chunk(
        &self,
        chunk_index: usize,
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
    ) -> Result<(), CacheError> {
        let (hash, _seq) = self.inner.block_map_get(chunk_index);
        if hash.is_zero() || hash == self.inner.zero_block_hash {
            return Ok(());
        }
        if clean_cache.get(&hash).await.is_some() {
            return Ok(());
        }
        if let Err(e) = self
            .resolve_pack(&hash, clean_cache, pack_index, content_store, None)
            .await
        {
            warn!(chunk_index, error = %e, "prefetch failed");
        }
        Ok(())
    }

    /// Prefetch multiple chunks with bounded concurrency.
    ///
    /// Used for boot hot set prefetch: given a list of chunk indices, resolves
    /// each one via full-pack fetch (caching all sibling blocks). Pack-level
    /// dedup means the second chunk from the same pack hits the clean cache.
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

    /// Resolve a single chunk through the tier hierarchy (hot path).
    ///
    /// 1. block_map lookup → if zero hash, return zeros
    /// 2. clean_cache → bounded in-memory/SSD cache (~100ns mem, ~100μs SSD)
    /// 3. S3 range request → fetch compressed bytes from pack
    /// 4. SSD pread fallback → dirty block not yet in S3 (OS page cache: ~μs)
    async fn resolve_chunk(
        &self,
        chunk_index: usize,
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
        metrics: Option<&super::super::metrics::ExportMetrics>,
    ) -> Result<Bytes, CacheError> {
        let (hash, _seq) = self.inner.block_map_get(chunk_index);

        // ZERO hash on a present block means the hash is deferred (not yet
        // computed). Read directly from SSD — the data is there.
        if hash.is_zero() {
            if self.inner.is_present(chunk_index) {
                return self.sync_read_local_block(chunk_index as u64);
            }
            return Ok(self.inner.zero_block_bytes.clone());
        }

        // Explicit zero-range → zeros.
        if hash == self.inner.zero_block_hash {
            return Ok(self.inner.zero_block_bytes.clone());
        }

        // Tier 1: clean_cache (recently written or previously fetched from S3).
        if let Some(data) = clean_cache.get(&hash).await {
            if let Some(m) = metrics {
                m.record_cache_hit();
            }
            return Ok(data);
        }

        // Tier 2: S3 range request (if block exists in a pack).
        if let Some(pack_loc) = pack_index.get(&hash)? {
            if let Some(m) = metrics {
                m.record_cache_miss();
            }
            let fetch_start = std::time::Instant::now();
            let compressed = match content_store
                .get_block(pack_loc.pack_id, pack_loc.offset, pack_loc.comp_length)
                .await
            {
                Ok(data) => data,
                Err(e) => {
                    if let Some(m) = metrics {
                        m.record_s3_get_error();
                    }
                    return Err(e.into());
                }
            };

            let decompressed = lz4_decompress(&compressed)
                .map_err(|e| CacheError::DecompressFailed(e.to_string()))?;

            let actual_hash = blake3_128(&decompressed);
            if actual_hash != hash {
                return Err(CacheError::HashMismatch {
                    expected: format!("{:?}", hash),
                    actual: format!("{:?}", actual_hash),
                });
            }

            if let Some(m) = metrics {
                m.record_s3_read(compressed.len() as u64);
                m.record_s3_fetch_latency(fetch_start.elapsed());
            }

            let data = Bytes::from(decompressed);
            clean_cache.insert(hash, data.clone());
            return Ok(data);
        }

        // Tier 3: SSD pread fallback — block is locally dirty, not yet in S3.
        // OS page cache makes this ~μs for recently written blocks.
        let data = self.sync_read_local_block(chunk_index as u64)?;
        Ok(data)
    }

    /// Fetch an entire pack from S3 and cache all sibling blocks (prefetch path).
    ///
    /// Streams the pack body: reads the header+index first, then decompresses
    /// each block incrementally as bytes arrive, inserting into clean_cache.
    /// Peak memory: full pack buffer + one decompressed block at a time.
    async fn resolve_pack(
        &self,
        target_hash: &crate::block::block_map::Blake3Hash,
        clean_cache: &dyn BlockCache,
        pack_index: &HostPackIndex,
        content_store: &ContentStore,
        metrics: Option<&super::super::metrics::ExportMetrics>,
    ) -> Result<Bytes, CacheError> {
        use futures::StreamExt;

        let pack_loc = pack_index
            .get(target_hash)?
            .ok_or(CacheError::BlockNotFound { hash: *target_hash })?;

        let mut stream = match content_store.get_pack_stream(pack_loc.pack_id).await {
            Ok(s) => s,
            Err(e) => {
                if let Some(m) = metrics {
                    m.record_s3_get_error();
                }
                return Err(e.into());
            }
        };

        // Accumulate bytes from the stream into a working buffer.
        // We process blocks as soon as we have enough data for each one.
        // Cap at 128MB to guard against malformed S3 responses.
        const MAX_PACK_BUF: usize = 128 * 1024 * 1024;
        let mut buf = Vec::new();
        let mut pack_idx: Option<pack::PackIndex> = None;
        let mut next_entry = 0usize;
        let mut target_data: Option<Bytes> = None;

        while let Some(chunk_result) = stream.next().await {
            let chunk =
                chunk_result.map_err(|e| CacheError::PackFormat(format!("stream error: {e}")))?;
            if buf.len() + chunk.len() > MAX_PACK_BUF {
                return Err(CacheError::PackFormat(format!(
                    "pack stream exceeded {}MB limit",
                    MAX_PACK_BUF / (1024 * 1024)
                )));
            }
            buf.extend_from_slice(&chunk);

            // Try to parse the header + index if we haven't yet.
            if pack_idx.is_none() {
                if buf.len() >= pack::PACK_HEADER_SIZE {
                    // Try parsing — need header + full index.
                    match pack::parse_pack_index(&buf) {
                        Ok(idx) => {
                            pack_idx = Some(idx);
                        }
                        Err(_) => {
                            continue;
                        } // Need more bytes for the index.
                    }
                } else {
                    continue;
                }
            }

            // Process blocks whose compressed data is fully available.
            let idx = pack_idx.as_ref().unwrap();
            while next_entry < idx.entries.len() {
                let entry = &idx.entries[next_entry];
                let end = entry.offset as usize + entry.comp_length as usize;
                if buf.len() < end {
                    break; // Need more bytes for this block.
                }

                let compressed = &buf[entry.offset as usize..end];
                let decompressed = lz4_decompress(compressed)
                    .map_err(|e| CacheError::DecompressFailed(e.to_string()))?;

                let actual_hash = blake3_128(&decompressed);
                if actual_hash != entry.hash {
                    return Err(CacheError::HashMismatch {
                        expected: format!("{:?}", entry.hash),
                        actual: format!("{:?}", actual_hash),
                    });
                }

                let data = Bytes::from(decompressed);
                if entry.hash == *target_hash {
                    target_data = Some(data.clone());
                }
                clean_cache.insert(entry.hash, data);
                next_entry += 1;
            }
        }

        // Process any remaining entries after stream is exhausted.
        if let Some(idx) = &pack_idx {
            while next_entry < idx.entries.len() {
                let entry = &idx.entries[next_entry];
                let compressed = pack::extract_block(&buf, entry.offset, entry.comp_length)
                    .ok_or_else(|| {
                        CacheError::PackFormat(format!(
                            "block at offset {} length {} out of bounds in pack {}",
                            entry.offset, entry.comp_length, pack_loc.pack_id
                        ))
                    })?;

                let decompressed = lz4_decompress(compressed)
                    .map_err(|e| CacheError::DecompressFailed(e.to_string()))?;

                let actual_hash = blake3_128(&decompressed);
                if actual_hash != entry.hash {
                    return Err(CacheError::HashMismatch {
                        expected: format!("{:?}", entry.hash),
                        actual: format!("{:?}", actual_hash),
                    });
                }

                let data = Bytes::from(decompressed);
                if entry.hash == *target_hash {
                    target_data = Some(data.clone());
                }
                clean_cache.insert(entry.hash, data);
                next_entry += 1;
            }
        }

        target_data.ok_or(CacheError::BlockNotFound { hash: *target_hash })
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
