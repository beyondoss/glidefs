use std::collections::HashMap;

use bytes::Bytes;
use tracing::{debug, instrument, warn};

use crate::block::cache::BlockCache;
use crate::block::content_store::ContentStore;
use crate::block::pack::PackId;
use crate::block::state::Active;

use super::{CacheError, WriteCache};

/// Where a block lives after resolution (before S3 fetch).
enum BlockLocation {
    /// Block is on local SSD — data already read.
    Local(Bytes),
    /// Block is in clean_cache — data already available.
    Cached(Bytes),
    /// Block is unwritten — all zeros.
    Zero,
    /// Block needs to be fetched from S3.
    NeedsFetch {
        pack_id: PackId,
        chunk_idx: u32,
        expected_hash: crate::block::block_map::Blake3Hash,
        pack_offset: u32,
        comp_length: u32,
    },
}

/// Source of data for a single chunk in a read plan.
///
/// Used by the ublk zero-copy path to determine how to fill each chunk
/// of a read request without going through an intermediate buffer.
#[cfg(all(target_os = "linux", feature = "ublk"))]
pub enum ChunkSource {
    /// Block is all zeros — memset the destination.
    Zero,
    /// Block is on local SSD at this file offset — io_uring Read from data file.
    LocalSsd { file_offset: u64 },
    /// Block data already in memory (clean cache or S3 fetch) — memcpy to destination.
    InMemory(Bytes),
}

/// One entry in a read plan: a chunk source plus the slice within that chunk
/// needed to satisfy the original read request.
#[cfg(all(target_os = "linux", feature = "ublk"))]
pub struct ChunkPlanEntry {
    pub source: ChunkSource,
    /// Byte offset within the chunk to start copying from.
    pub slice_start: usize,
    /// Number of bytes to copy from this chunk.
    pub slice_len: usize,
}

/// A fully-resolved read plan: a sequence of chunk entries that together
/// cover the requested byte range.
///
/// The caller iterates `entries` in order, writing `slice_len` bytes to the
/// destination buffer for each entry. For `LocalSsd`, submit an io_uring Read
/// at `file_offset + slice_start`. For `InMemory`, memcpy from
/// `data[slice_start..slice_start+slice_len]`. For `Zero`, memset.
#[cfg(all(target_os = "linux", feature = "ublk"))]
pub struct ReadPlan {
    pub entries: Vec<ChunkPlanEntry>,
}

impl WriteCache<Active> {
    /// Try to pread directly into a caller-provided buffer (zero-alloc fast path).
    ///
    /// Returns `Some(Ok(len))` if all blocks in range are present on local SSD
    /// and the pread succeeded. Returns `None` if any block is not present,
    /// signaling the caller to fall back to the full `read()` path.
    #[cfg_attr(not(all(target_os = "linux", feature = "ublk")), allow(dead_code))]
    #[inline]
    pub fn try_pread_local(&self, offset: u64, len: usize, buf: &mut [u8]) -> Option<Result<usize, CacheError>> {
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len as u64 - 1) / block_size;
        if !(start_block..=end_block).all(|i| {
            let idx = i as usize;
            self.inner.is_present(idx) && !self.inner.is_partial(idx)
        }) {
            return None;
        }
        Some(match self.inner.data_file.read_exact_at(&mut buf[..len], offset) {
            Ok(()) => Ok(len),
            Err(e) => Err(CacheError::from(e)),
        })
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

    /// Read from local cache only (sync, no S3 fetch).
    ///
    /// **WARNING**: Returns zeros for blocks not present locally.
    /// Use the async `read()` for NBD I/O to get proper S3 read-through.
    #[allow(dead_code)] // Used by tests
    #[instrument(skip(self), fields(offset = offset, len = len))]
    pub fn read_local_only(&self, offset: u64, len: usize) -> Result<Bytes, CacheError> {
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

    /// Build a read plan for the ublk zero-copy path.
    ///
    /// Same resolution order as `read`, but instead of copying data into a
    /// `Bytes` buffer, returns a plan describing where each chunk's data lives.
    /// The caller (io_task_zc) uses the plan to issue io_uring ops for `LocalSsd`
    /// chunks and memcpy for `InMemory` chunks, writing directly into the
    /// kernel-mapped bio pages.
    ///
    /// The cold path (S3 fetch) is boxed to keep the hot-path future small.
    /// This matters because 128 per-tag io_task_zc futures share L1 cache.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn resolve_read_plan(
        &self,
        offset: u64,
        len: usize,
        clean_cache: &dyn BlockCache,
        pack_index_cache: &crate::block::pack_index_cache::PackIndexCache,
        volume_manifest: &parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
        content_store: &ContentStore,
        metrics: &super::super::metrics::ExportMetrics,
    ) -> Result<ReadPlan, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::offset_out_of_bounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(ReadPlan {
                entries: Vec::new(),
            });
        }

        let chunk_size = self.inner.config.block_size as u64;
        let start_chunk = offset / chunk_size;
        let end_chunk = (offset + len as u64 - 1) / chunk_size;

        // Fast path: all chunks present on local SSD and not partial →
        // single LocalSsd entry. Collapses N per-chunk SQEs into one io_uring Read.
        if (start_chunk..=end_chunk).all(|i| {
            let idx = i as usize;
            self.inner.is_present(idx) && !self.inner.is_partial(idx)
        }) {
            return Ok(ReadPlan {
                entries: vec![ChunkPlanEntry {
                    source: ChunkSource::LocalSsd { file_offset: offset },
                    slice_start: 0,
                    slice_len: len,
                }],
            });
        }

        // Cold path: some blocks need S3 fetch. Box to keep the parent future
        // (io_task_zc) small — locate_block/fetch_coalesced have deep async
        // call trees that inflate the state machine.
        Box::pin(self.resolve_read_plan_cold(
            offset, len, start_chunk, end_chunk, clean_cache,
            pack_index_cache, volume_manifest, content_store, metrics,
        )).await
    }

    /// Cold path for resolve_read_plan: locate blocks and fetch from S3.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    #[allow(clippy::too_many_arguments)]
    async fn resolve_read_plan_cold(
        &self,
        offset: u64,
        len: usize,
        start_chunk: u64,
        end_chunk: u64,
        clean_cache: &dyn BlockCache,
        pack_index_cache: &crate::block::pack_index_cache::PackIndexCache,
        volume_manifest: &parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
        content_store: &ContentStore,
        metrics: &super::super::metrics::ExportMetrics,
    ) -> Result<ReadPlan, CacheError> {
        let chunk_size = self.inner.config.block_size as u64;
        let num_chunks = (end_chunk - start_chunk + 1) as usize;

        // Locate all blocks concurrently, then coalesce S3 fetches.
        let locate_futures: Vec<_> = (start_chunk..=end_chunk)
            .map(|block_idx| {
                let is_local = self.inner.is_present(block_idx as usize);
                let file_offset = block_idx * chunk_size;
                async move {
                    if is_local {
                        // Return LocalSsd directly — no data read needed.
                        return Ok((block_idx, None, Some(file_offset)));
                    }
                    let loc = self.locate_block(
                        block_idx as usize,
                        clean_cache,
                        pack_index_cache,
                        volume_manifest,
                        content_store,
                        Some(metrics),
                    ).await?;
                    Ok::<_, CacheError>((block_idx, Some(loc), None))
                }
            })
            .collect();
        let located = futures::future::try_join_all(locate_futures).await?;

        // Partition into sources and NeedsFetch entries
        let mut sources: HashMap<usize, ChunkSource> = HashMap::new();
        let mut fetch_entries = Vec::new();

        for (i, (_block_idx, location, local_offset)) in located.into_iter().enumerate() {
            if let Some(file_offset) = local_offset {
                sources.insert(i, ChunkSource::LocalSsd { file_offset });
                continue;
            }
            match location.unwrap() {
                BlockLocation::Local(data) => {
                    if data.iter().all(|&b| b == 0) {
                        sources.insert(i, ChunkSource::Zero);
                    } else {
                        sources.insert(i, ChunkSource::InMemory(data));
                    }
                }
                BlockLocation::Cached(data) => {
                    if data.iter().all(|&b| b == 0) {
                        sources.insert(i, ChunkSource::Zero);
                    } else {
                        sources.insert(i, ChunkSource::InMemory(data));
                    }
                }
                BlockLocation::Zero => {
                    sources.insert(i, ChunkSource::Zero);
                }
                BlockLocation::NeedsFetch {
                    pack_id,
                    chunk_idx,
                    expected_hash,
                    pack_offset,
                    comp_length,
                } => {
                    fetch_entries.push((i, pack_id, chunk_idx, expected_hash, pack_offset, comp_length));
                }
            }
        }

        // Coalesced S3 fetch for NeedsFetch blocks
        if !fetch_entries.is_empty() {
            let fetched = Self::fetch_coalesced(
                &fetch_entries,
                content_store,
                clean_cache,
                Some(metrics),
            )
            .await?;
            for (i, data) in fetched {
                if data.iter().all(|&b| b == 0) {
                    sources.insert(i, ChunkSource::Zero);
                } else {
                    sources.insert(i, ChunkSource::InMemory(data));
                }
            }
        }

        // Build plan entries in order
        let mut entries = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let source = sources.remove(&i).ok_or_else(|| {
                CacheError::DecompressFailed("missing block in coalesced read plan".to_string())
            })?;
            let block_idx = start_chunk + i as u64;
            let chunk_start_byte = block_idx * chunk_size;

            let slice_start = if block_idx == start_chunk {
                (offset - chunk_start_byte) as usize
            } else {
                0
            };
            let slice_end = if block_idx == end_chunk {
                let end_byte = offset + len as u64;
                let relative_end = (end_byte - chunk_start_byte) as usize;
                std::cmp::min(relative_end, self.inner.config.block_size)
            } else {
                self.inner.config.block_size
            };

            entries.push(ChunkPlanEntry {
                source,
                slice_start,
                slice_len: slice_end - slice_start,
            });
        }

        Ok(ReadPlan { entries })
    }

    /// Read via VolumeManifest → PackIndexCache → clean_cache → S3.
    ///
    /// Chunks are resolved concurrently — a multi-block read that spans multiple
    /// chunks fetches them in parallel.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, clean_cache, pack_index_cache, volume_manifest, content_store, metrics), fields(offset = offset, len = len))]
    pub async fn read(
        &self,
        offset: u64,
        len: usize,
        clean_cache: &dyn BlockCache,
        pack_index_cache: &crate::block::pack_index_cache::PackIndexCache,
        volume_manifest: &parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
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

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len as u64 - 1) / block_size;
        let num_blocks = (end_block - start_block + 1) as usize;

        // Fast path: all blocks present on local SSD and not partial →
        // single pread of exact bytes. Bypasses per-block resolution.
        if (start_block..=end_block).all(|i| {
            let idx = i as usize;
            self.inner.is_present(idx) && !self.inner.is_partial(idx)
        }) {
            let mut buf = vec![0u8; len];
            self.inner.data_file.read_exact_at(&mut buf, offset)?;
            return Ok(Bytes::from(buf));
        }

        // Single block fast path
        if num_blocks == 1 {
            let block_data = self
                .resolve_block(
                    start_block as usize,
                    clean_cache,
                    pack_index_cache,
                    volume_manifest,
                    content_store,
                    Some(metrics),
                )
                .await?;
            let block_start_byte = start_block * block_size;
            let slice_start = (offset - block_start_byte) as usize;
            let slice_end = slice_start + len;
            return Ok(block_data.slice(slice_start..std::cmp::min(slice_end, block_data.len())));
        }

        // Multi-block: check if any blocks are partial. If none are partial,
        // use the fast coalesced path. If any are partial, resolve per-block
        // (which handles merging SSD + S3 data for partial blocks).
        let has_partial = (start_block..=end_block)
            .any(|i| self.inner.is_partial(i as usize));

        let block_data_vec: Vec<Bytes> = if !has_partial {
            // Fast path: no partial blocks — locate + coalesce S3 fetches
            let locate_futures: Vec<_> = (start_block..=end_block)
                .map(|block_idx| {
                    self.locate_block(
                        block_idx as usize,
                        clean_cache,
                        pack_index_cache,
                        volume_manifest,
                        content_store,
                        Some(metrics),
                    )
                })
                .collect();
            let locations = futures::future::try_join_all(locate_futures).await?;

            let mut resolved_blocks: HashMap<usize, Bytes> = HashMap::new();
            let mut fetch_entries = Vec::new();

            for (i, location) in locations.into_iter().enumerate() {
                match location {
                    BlockLocation::Local(data) | BlockLocation::Cached(data) => {
                        resolved_blocks.insert(i, data);
                    }
                    BlockLocation::Zero => {
                        resolved_blocks.insert(i, self.inner.zero_block_bytes.clone());
                    }
                    BlockLocation::NeedsFetch {
                        pack_id,
                        chunk_idx,
                        expected_hash,
                        pack_offset,
                        comp_length,
                    } => {
                        fetch_entries.push((i, pack_id, chunk_idx, expected_hash, pack_offset, comp_length));
                    }
                }
            }

            if !fetch_entries.is_empty() {
                let fetched = Self::fetch_coalesced(
                    &fetch_entries,
                    content_store,
                    clean_cache,
                    Some(metrics),
                )
                .await?;
                resolved_blocks.extend(fetched);
            }

            let mut vec = Vec::with_capacity(num_blocks);
            for i in 0..num_blocks {
                let data = resolved_blocks.remove(&i).ok_or_else(|| {
                    CacheError::DecompressFailed("missing block in coalesced read".to_string())
                })?;
                vec.push(data);
            }
            vec
        } else {
            // Slow path: some blocks are partial — resolve per-block
            // to ensure proper SSD + S3 merge for partial blocks.
            let resolve_futures: Vec<_> = (start_block..=end_block)
                .map(|block_idx| {
                    self.resolve_block(
                        block_idx as usize,
                        clean_cache,
                        pack_index_cache,
                        volume_manifest,
                        content_store,
                        Some(metrics),
                    )
                })
                .collect();
            futures::future::try_join_all(resolve_futures).await?
        };

        // Assemble result in block order
        let mut result = Vec::with_capacity(len);
        for (i, block_data) in block_data_vec.iter().enumerate() {
            let block_idx = start_block + i as u64;
            let block_start_byte = block_idx * block_size;
            let slice_start = if block_idx == start_block {
                (offset - block_start_byte) as usize
            } else {
                0
            };
            let slice_end = if block_idx == end_block {
                let end_byte = offset + len as u64;
                let relative_end = (end_byte - block_start_byte) as usize;
                std::cmp::min(relative_end, block_data.len())
            } else {
                block_data.len()
            };
            result.extend_from_slice(&block_data[slice_start..slice_end]);
        }

        debug!(blocks = num_blocks, "read complete");
        Ok(Bytes::from(result))
    }

    /// Locate where a block lives without fetching from S3.
    ///
    /// Resolution order:
    /// 1. is_present AND not partial → Local (pread from SSD)
    /// 2. is_present AND partial → fetch from S3, merge with SSD, complete backfill
    /// 3. VolumeManifest → PackIndexCache → clean_cache → Cached
    /// 4. PackIndexCache miss → parallel prefetch ALL pack indices for chunk
    /// 5. Block found → NeedsFetch (S3 coordinates)
    /// 6. Block not in any pack → Zero
    async fn locate_block(
        &self,
        block_index: usize,
        clean_cache: &dyn BlockCache,
        pack_index_cache: &crate::block::pack_index_cache::PackIndexCache,
        volume_manifest: &parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
        content_store: &ContentStore,
        metrics: Option<&super::super::metrics::ExportMetrics>,
    ) -> Result<BlockLocation, CacheError> {
        // Hot path: block is present on local SSD and fully backfilled
        if self.inner.is_present(block_index) && !self.inner.is_partial(block_index) {
            let data = self.sync_read_local_block(block_index as u64)?;
            return Ok(BlockLocation::Local(data));
        }

        // Partial block: need S3 data for unwritten sub-regions.
        // Don't read SSD here — merge_partial_block will do the single
        // authoritative read under the write_lock. Fall through to the
        // S3 resolution path so resolve_block can fetch + merge.
        // (is_present but partial blocks are handled by the cold path below)

        // Cold path: resolve via VolumeManifest → PackIndexCache
        let (chunk_idx, block_offset, pack_ids) = {
            let vm = volume_manifest.read();
            let ci = vm.chunk_idx_for_block(block_index as u64);
            let bo = vm.block_offset_in_chunk(block_index as u64);
            let pids = vm
                .chunk_pack_ids(ci)
                .map(|ids| ids.to_vec())
                .unwrap_or_default();
            // Bitmap fast path: if this offset was never written to S3, return zeros
            // without any pack index resolution.
            if pids.is_empty() || !vm.has_block_in_s3(ci, bo) {
                return Ok(BlockLocation::Zero);
            }
            (ci, bo, pids)
        };

        // Iterate packs newest-first (last element = newest).
        // On cache miss, fetch that pack's index from S3 immediately and check it
        // before moving to older packs. This avoids fetching ALL pack indices when
        // the target block is typically in the newest pack ("newest wins").
        let mut resolved = None;
        let mut last_fetch_error = None;

        for &pack_id in pack_ids.iter().rev() {
            // Check cache: lookup_block does get_entries + binary search internally
            match pack_index_cache.lookup_block(pack_id, block_offset).await {
                Some((hash, pack_offset, comp_length)) => {
                    resolved = Some((pack_id, hash, pack_offset, comp_length));
                    break;
                }
                None => {
                    if pack_index_cache.get_entries(pack_id).await.is_some() {
                        // Pack is cached but doesn't contain this block — try older
                        continue;
                    }
                }
            }

            // Cache miss — fetch this single pack's index from S3
            match content_store.get_pack_index(chunk_idx, pack_id).await {
                Ok(entries) => {
                    pack_index_cache.insert_entries(pack_id, &entries);
                    if let Some(e) = entries
                        .iter()
                        .find(|e| e.chunk_offset == block_offset)
                    {
                        resolved =
                            Some((pack_id, e.hash, e.offset, e.comp_length));
                        break;
                    }
                    // Fetched but block not in this pack — continue to older
                }
                Err(e) => {
                    warn!(
                        chunk_idx, pack_id,
                        error = %e,
                        "failed to fetch pack index from S3"
                    );
                    last_fetch_error = Some(e);
                }
            }
        }

        if resolved.is_none() && let Some(e) = last_fetch_error {
            return Err(e.into());
        }

        let Some((pack_id, expected_hash, pack_offset, comp_length)) = resolved else {
            return Ok(BlockLocation::Zero);
        };

        // Zero block tombstone: comp_length == 0 means this block was explicitly
        // zeroed. The pack index entry exists to override older non-zero entries
        // via "newest wins", but there is no compressed data to fetch.
        if comp_length == 0 {
            return Ok(BlockLocation::Zero);
        }

        // Check clean_cache (block may have been fetched earlier)
        if let Some(data) = clean_cache.get(&expected_hash).await {
            if let Some(m) = metrics {
                m.record_cache_hit();
            }
            return Ok(BlockLocation::Cached(data));
        }

        if let Some(m) = metrics {
            m.record_cache_miss();
        }

        Ok(BlockLocation::NeedsFetch {
            pack_id,
            chunk_idx,
            expected_hash,
            pack_offset,
            comp_length,
        })
    }

    /// Resolve a single block through the full tier hierarchy (single-block path).
    ///
    /// Calls `locate_block`, then fetches from S3 if needed. For partial blocks,
    /// merges SSD data (guest-written sub-regions) with S3 data and completes
    /// the backfill on-demand.
    async fn resolve_block(
        &self,
        block_index: usize,
        clean_cache: &dyn BlockCache,
        pack_index_cache: &crate::block::pack_index_cache::PackIndexCache,
        volume_manifest: &parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
        content_store: &ContentStore,
        metrics: Option<&super::super::metrics::ExportMetrics>,
    ) -> Result<Bytes, CacheError> {
        let is_partial = self.inner.is_present(block_index) && self.inner.is_partial(block_index);

        let location = self
            .locate_block(
                block_index,
                clean_cache,
                pack_index_cache,
                volume_manifest,
                content_store,
                metrics,
            )
            .await?;

        let s3_data = match location {
            BlockLocation::Local(data) => return Ok(data),
            BlockLocation::Cached(data) => {
                if !is_partial {
                    return Ok(data);
                }
                data
            }
            BlockLocation::Zero => {
                if is_partial {
                    self.inner.zero_block_bytes.clone()
                } else {
                    return Ok(self.inner.zero_block_bytes.clone());
                }
            }
            BlockLocation::NeedsFetch {
                pack_id,
                chunk_idx,
                expected_hash,
                pack_offset,
                comp_length,
            } => {
                let data = Self::fetch_single_block(
                    content_store,
                    clean_cache,
                    metrics,
                    chunk_idx,
                    pack_id,
                    expected_hash,
                    pack_offset,
                    comp_length,
                )
                .await?;
                if !is_partial {
                    return Ok(data);
                }
                data
            }
        };

        // Merge: SSD data for written sub-regions, S3 data for the rest
        self.merge_partial_block(block_index, &s3_data)
    }

    /// Merge a partial block's SSD data with S3 data and complete backfill.
    ///
    /// For each sub-region: if the bitmap bit is set (guest wrote it), use SSD data.
    /// Otherwise, use S3 data and write it to SSD to complete the backfill.
    fn merge_partial_block(
        &self,
        block_index: usize,
        s3_data: &[u8],
    ) -> Result<Bytes, CacheError> {
        use super::inner::SUB_BLOCK_SIZE;

        let block_size = self.inner.config.block_size;

        // Read current SSD data (has guest writes in marked sub-regions)
        let local_data = self.sync_read_local_block(block_index as u64)?;
        let mut merged = local_data.to_vec();
        let block_start = block_index as u64 * block_size as u64;

        // Fill in unwritten sub-regions from S3 data.
        // Hold the per-block write_lock to prevent TOCTOU race (same as
        // spawn_background_backfill). Guest re-pwrites under the same lock.
        {
            let state = match self.inner.partial_blocks.get(&block_index) {
                Some(s) => s,
                None => {
                    // Block was completed by another path — return local data as-is
                    return Ok(Bytes::from(merged));
                }
            };
            let _guard = state.value().write_lock.lock();

            for sub in 0..(block_size / SUB_BLOCK_SIZE) {
                let bitmap = state.value().bitmap.load(std::sync::atomic::Ordering::Acquire);
                if bitmap & (1 << sub) != 0 {
                    continue; // guest wrote this sub-region, keep SSD data
                }
                let start = sub * SUB_BLOCK_SIZE;
                let end = start + SUB_BLOCK_SIZE;
                merged[start..end].copy_from_slice(&s3_data[start..end]);

                // Write S3 data to SSD to complete backfill for this sub-region
                let sub_offset = block_start + start as u64;
                self.inner.write_sub_region(sub_offset, &s3_data[start..end])?;
            }
        }

        // Backfill complete — remove from partial tracking
        self.inner.complete_partial(block_index);

        Ok(Bytes::from(merged))
    }

    /// Fetch, decompress, verify, and cache a single block from S3.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_single_block(
        content_store: &ContentStore,
        clean_cache: &dyn BlockCache,
        metrics: Option<&super::super::metrics::ExportMetrics>,
        chunk_idx: u32,
        pack_id: PackId,
        expected_hash: crate::block::block_map::Blake3Hash,
        pack_offset: u32,
        comp_length: u32,
    ) -> Result<Bytes, CacheError> {
        use crate::block::block_map::{blake3_128, lz4_decompress};

        let fetch_start = std::time::Instant::now();
        let compressed = match content_store
            .get_chunk_block(chunk_idx, pack_id, pack_offset, comp_length)
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
        if actual_hash != expected_hash {
            return Err(CacheError::HashMismatch {
                expected: format!("{:?}", expected_hash),
                actual: format!("{:?}", actual_hash),
            });
        }

        if let Some(m) = metrics {
            m.record_s3_read(compressed.len() as u64);
            m.record_s3_fetch_latency(fetch_start.elapsed());
        }

        let data = Bytes::from(decompressed);
        clean_cache.insert(expected_hash, data.clone());
        Ok(data)
    }

    /// Fetch multiple blocks from S3 with coalesced range requests.
    ///
    /// Groups blocks by (chunk_idx, pack_id), fetches each group as a single
    /// S3 range request covering all blocks in the group, then splits, decompresses,
    /// verifies, and caches each block individually.
    ///
    /// `entries`: Vec of (original_index, NeedsFetch fields)
    async fn fetch_coalesced(
        entries: &[(usize, PackId, u32, crate::block::block_map::Blake3Hash, u32, u32)],
        content_store: &ContentStore,
        clean_cache: &dyn BlockCache,
        metrics: Option<&super::super::metrics::ExportMetrics>,
    ) -> Result<HashMap<usize, Bytes>, CacheError> {
        use crate::block::block_map::{blake3_128, lz4_decompress};

        if entries.is_empty() {
            return Ok(HashMap::new());
        }

        // Group by (chunk_idx, pack_id)
        type FetchGroup = Vec<(usize, crate::block::block_map::Blake3Hash, u32, u32)>;
        let mut groups: HashMap<(u32, PackId), FetchGroup> = HashMap::new();
        for &(idx, pack_id, chunk_idx, hash, offset, comp_len) in entries {
            groups
                .entry((chunk_idx, pack_id))
                .or_default()
                .push((idx, hash, offset, comp_len));
        }

        // Fetch each group (potentially in parallel).
        //
        // Within each group, blocks are coalesced into a single range request
        // when they are densely packed. Sparse groups (gap > data) are fetched
        // individually to avoid wasting bandwidth.
        let mut group_futures = Vec::new();
        for ((chunk_idx, pack_id), mut blocks) in groups {
            let cs = content_store;
            let cc = clean_cache;
            group_futures.push(async move {
                // Sort by pack_offset for contiguous range
                blocks.sort_by_key(|&(_, _, offset, _)| offset);

                // Determine fetch strategy based on block density.
                let range_start = blocks.first().unwrap().2;
                let last = blocks.last().unwrap();
                let range_end = last.2 + last.3;
                let span = (range_end - range_start) as u64;
                let total_data: u64 = blocks.iter().map(|&(_, _, _, cl)| cl as u64).sum();
                let gap = span - total_data;

                // Sparse or single block — fetch each block individually.
                if blocks.len() == 1 || gap > total_data {
                    let mut results: Vec<(usize, Bytes)> =
                        Vec::with_capacity(blocks.len());
                    for (idx, expected_hash, offset, comp_len) in blocks {
                        let fetch_start = std::time::Instant::now();
                        let compressed = match cs
                            .get_chunk_block(chunk_idx, pack_id, offset, comp_len)
                            .await
                        {
                            Ok(data) => data,
                            Err(e) => {
                                if let Some(m) = metrics {
                                    m.record_s3_get_error();
                                }
                                return Err(CacheError::from(e));
                            }
                        };
                        if let Some(m) = metrics {
                            m.record_s3_read(compressed.len() as u64);
                            m.record_s3_fetch_latency(fetch_start.elapsed());
                        }
                        let decompressed = lz4_decompress(&compressed)
                            .map_err(|e| CacheError::DecompressFailed(e.to_string()))?;
                        let actual_hash = blake3_128(&decompressed);
                        if actual_hash != expected_hash {
                            return Err(CacheError::HashMismatch {
                                expected: format!("{:?}", expected_hash),
                                actual: format!("{:?}", actual_hash),
                            });
                        }
                        let data = Bytes::from(decompressed);
                        cc.insert(expected_hash, data.clone());
                        results.push((idx, data));
                    }
                    return Ok(results);
                }

                // Dense — coalesce into a single range request.
                let range_len = range_end - range_start;

                let fetch_start = std::time::Instant::now();
                let coalesced = match cs
                    .get_chunk_block(chunk_idx, pack_id, range_start, range_len)
                    .await
                {
                    Ok(data) => data,
                    Err(e) => {
                        if let Some(m) = metrics {
                            m.record_s3_get_error();
                        }
                        return Err(CacheError::from(e));
                    }
                };

                if let Some(m) = metrics {
                    m.record_s3_read(coalesced.len() as u64);
                    m.record_s3_fetch_latency(fetch_start.elapsed());
                }

                // Split coalesced response into individual blocks
                let mut results: Vec<(usize, Bytes)> = Vec::with_capacity(blocks.len());
                for (idx, expected_hash, offset, comp_len) in blocks {
                    let local_start = (offset - range_start) as usize;
                    let local_end = local_start + comp_len as usize;
                    let compressed = coalesced.slice(local_start..local_end);

                    let decompressed = lz4_decompress(&compressed)
                        .map_err(|e| CacheError::DecompressFailed(e.to_string()))?;

                    let actual_hash = blake3_128(&decompressed);
                    if actual_hash != expected_hash {
                        return Err(CacheError::HashMismatch {
                            expected: format!("{:?}", expected_hash),
                            actual: format!("{:?}", actual_hash),
                        });
                    }

                    let data = Bytes::from(decompressed);
                    cc.insert(expected_hash, data.clone());
                    results.push((idx, data));
                }
                Ok(results)
            });
        }

        let group_results = futures::future::try_join_all(group_futures).await?;
        let mut map = HashMap::new();
        for results in group_results {
            for (idx, data) in results {
                map.insert(idx, data);
            }
        }
        Ok(map)
    }

    /// Warm the PackIndexCache for a chunk (readahead/prefetch).
    ///
    /// Fetches all pack indices for the given chunk in parallel and inserts them
    /// into the PackIndexCache. This is a best-effort background operation —
    /// errors are logged and the result ignored by callers.
    pub async fn prefetch_chunk(
        &self,
        chunk_idx: u32,
        pack_index_cache: &crate::block::pack_index_cache::PackIndexCache,
        volume_manifest: &parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
        content_store: &ContentStore,
    ) -> Result<(), CacheError> {
        let pack_ids: Vec<crate::block::pack::PackId> = {
            let vm = volume_manifest.read();
            vm.chunk_pack_ids(chunk_idx)
                .map(|ids| ids.to_vec())
                .unwrap_or_default()
        };

        if pack_ids.is_empty() {
            return Ok(());
        }

        let futures: Vec<_> = pack_ids
            .into_iter()
            .map(|pack_id| async move {
                if pack_index_cache.get_entries(pack_id).await.is_some() {
                    return; // already in cache
                }
                match content_store.get_pack_index(chunk_idx, pack_id).await {
                    Ok(entries) => pack_index_cache.insert_entries(pack_id, &entries),
                    Err(e) => warn!(chunk_idx, pack_id, error = %e, "prefetch pack index failed"),
                }
            })
            .collect();

        futures::future::join_all(futures).await;
        Ok(())
    }

    /// Batch warm the PackIndexCache for multiple chunks (boot hot set prefetch).
    pub async fn prefetch_chunks(
        &self,
        chunk_indices: &[u64],
        pack_index_cache: &crate::block::pack_index_cache::PackIndexCache,
        volume_manifest: &parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>,
        content_store: &ContentStore,
    ) {
        use futures::stream::{self, StreamExt};

        stream::iter(chunk_indices.iter().copied())
            .for_each_concurrent(8, |chunk_idx| async move {
                let _ = self
                    .prefetch_chunk(
                        chunk_idx as u32,
                        pack_index_cache,
                        volume_manifest,
                        content_store,
                    )
                    .await;
            })
            .await;
    }
}

