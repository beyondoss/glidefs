use std::collections::{BTreeMap, HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, info, instrument, warn};

use crate::block::block_map::{Blake3Hash, SparseBlockState, blake3_128, lz4_compress};
use crate::block::content_store::ContentStore;
use crate::block::state::{Active, Draining};

use super::inner::CacheInner;
use super::{CacheError, FlushStats, SnapshotResult, WriteCache};

/// Result of CPU-heavy flush computation (pread + crc32 + blake3 + lz4).
///
/// Produced by `compute_flush_batch` on a blocking thread, consumed by
/// the async portion of `flush_dirty_inner` for S3 uploads.
struct FlushBatch {
    /// Compressed blocks ready for S3 upload: (hash, compressed_bytes).
    to_upload: Vec<(Blake3Hash, Vec<u8>)>,
    /// Computed hashes for post-upload CAS clearing: (chunk_index, hash).
    computed: Vec<(usize, Blake3Hash)>,
    /// Blocks skipped due to CRC mismatch or concurrent write.
    /// These need to be transitioned back SYNCING→DIRTY.
    skipped: Vec<usize>,
    /// Partial statistics from the computation phase.
    stats: FlushStats,
}

/// Per-block result from the parallel compute phase.
enum BlockResult {
    /// Successfully computed hash; may include compressed data for upload.
    Computed {
        chunk_index: usize,
        hash: Blake3Hash,
        /// `Some(compressed)` if block is new (not zero).
        /// `None` if deduped (zero block).
        compressed: Option<Vec<u8>>,
    },
    /// Block skipped due to CRC mismatch or concurrent write.
    Skipped {
        chunk_index: usize,
        cas_failed: bool,
        corrupted: bool,
    },
}

/// CPU-heavy flush computation: read blocks from SSD, verify CRC32, hash, compress, dedup.
///
/// Runs on a blocking thread via `spawn_blocking` to avoid starving the async runtime.
/// Phase 1 (rayon parallel): pread + crc32 + blake3 + lz4 per block.
/// Phase 2 (sequential): within-batch dedup via seen_hashes (cheap hash-set insertions).
///
/// All blocks in `snapshot` have already been claimed (CAS DIRTY→SYNCING).
/// CRC verification uses the crc_map (DashMap) and state discrimination:
/// - CRC mismatch + state == SYNCING → real SSD corruption
/// - CRC mismatch + state != SYNCING → concurrent write re-dirtied the block
fn compute_flush_batch(
    inner: &CacheInner,
    snapshot: &[usize],
    zero_hash: Blake3Hash,
) -> Result<FlushBatch, CacheError> {
    use rayon::prelude::*;

    let block_size = inner.config.block_size;
    let device_size = inner.config.device_size;

    // Phase 1: parallel per-block compute (pread + crc32 + blake3 + dedup + lz4).
    // Each rayon task allocates its own read buffer; peak memory = num_threads × block_size.
    let per_block: Vec<Result<BlockResult, CacheError>> = snapshot
        .par_iter()
        .map(|&chunk_index| {
            let mut chunk_buf = vec![0u8; block_size];

            let offset = chunk_index as u64 * block_size as u64;
            let valid_bytes =
                std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;
            if valid_bytes > 0 {
                inner
                    .data_file
                    .read_exact_at(&mut chunk_buf[..valid_bytes], offset)?;
            }
            if valid_bytes < block_size {
                chunk_buf[valid_bytes..].fill(0);
            }

            // Verify CRC32 if available (detects SSD corruption before BLAKE3).
            // CRC was stored at checkpoint time. Use state discrimination to
            // distinguish corruption from concurrent writes:
            // - Block still SYNCING → no concurrent write → real corruption
            // - Block now DIRTY → write re-dirtied it → stale CRC, not corruption
            if let Some(stored_crc) = inner.crc_take(chunk_index) {
                let computed_crc = crc32fast::hash(&chunk_buf);
                if computed_crc != stored_crc {
                    let current_state = inner.state_map.get(chunk_index);
                    if current_state != SparseBlockState::SYNCING {
                        // Block was re-dirtied by a concurrent write between
                        // checkpoint and flush. CRC is stale, not corruption.
                        return Ok(BlockResult::Skipped {
                            chunk_index,
                            cas_failed: true,
                            corrupted: false,
                        });
                    }
                    // Still SYNCING + CRC mismatch → real SSD corruption.
                    warn!(
                        chunk_index,
                        stored_crc,
                        computed_crc,
                        "CRC32 mismatch — possible SSD corruption, skipping block this cycle"
                    );
                    return Ok(BlockResult::Skipped {
                        chunk_index,
                        cas_failed: false,
                        corrupted: true,
                    });
                }
            }

            let hash = blake3_128(&chunk_buf);

            // Zero block → deduped, no upload needed.
            let compressed = if hash == zero_hash {
                None
            } else {
                Some(lz4_compress(&chunk_buf[..]))
            };

            Ok(BlockResult::Computed {
                chunk_index,
                hash,
                compressed,
            })
        })
        .collect();

    // Phase 2: sequential aggregation — within-batch dedup + stats.
    let mut stats = FlushStats::default();
    let mut to_upload: Vec<(Blake3Hash, Vec<u8>)> = Vec::new();
    let mut seen_hashes = HashSet::new();
    let mut computed: Vec<(usize, Blake3Hash)> = Vec::new();
    let mut skipped: Vec<usize> = Vec::new();

    for result in per_block {
        let result = result?;
        match result {
            BlockResult::Skipped {
                chunk_index,
                cas_failed,
                corrupted,
            } => {
                stats.blocks_flushed += 1;
                skipped.push(chunk_index);
                if cas_failed {
                    stats.blocks_cas_failed += 1;
                }
                if corrupted {
                    stats.blocks_corrupted += 1;
                }
            }
            BlockResult::Computed {
                chunk_index,
                hash,
                compressed,
            } => {
                stats.blocks_flushed += 1;
                computed.push((chunk_index, hash));

                match compressed {
                    None => {
                        // Zero block.
                        stats.blocks_deduped += 1;
                    }
                    Some(data) => {
                        // Within-batch dedup: skip if we've already seen this hash.
                        if !seen_hashes.insert(hash) {
                            stats.blocks_deduped += 1;
                        } else {
                            to_upload.push((hash, data));
                        }
                    }
                }
            }
        }
    }

    Ok(FlushBatch {
        to_upload,
        computed,
        skipped,
        stats,
    })
}

impl WriteCache<Active> {
    /// Flush the local cache file.
    ///
    /// This performs an fsync on the local SSD, which is fast (<10ms).
    /// It does NOT wait for S3 sync - that happens in the background.
    #[instrument(skip(self))]
    pub fn flush(&self) -> Result<(), CacheError> {
        self.inner.data_file.sync_all()?;
        debug!("local flush complete");
        Ok(())
    }

    /// Get the number of dirty blocks pending sync.
    pub fn dirty_block_count(&self) -> u64 {
        self.inner.dirty_block_count.load(Ordering::Relaxed)
    }

    /// Get the number of blocks currently being synced.
    pub fn syncing_block_count(&self) -> u64 {
        self.inner.syncing_block_count.load(Ordering::Relaxed)
    }

    /// Number of recovery issues encountered during cache open.
    pub fn recovery_warning_count(&self) -> u64 {
        self.inner.recovery_warnings.load(Ordering::Relaxed)
    }

    /// Get the device size.
    #[allow(dead_code)]
    pub fn device_size(&self) -> u64 {
        self.inner.config.device_size
    }

    /// Get the block size.
    pub fn block_size(&self) -> usize {
        self.inner.config.block_size
    }

    /// Save metadata to disk.
    #[allow(dead_code)]
    pub fn save_metadata(&self) -> Result<(), CacheError> {
        self.inner.save_metadata()
    }

    /// Graceful shutdown: save metadata and transition to Draining.
    ///
    /// The flush scheduler is responsible for ensuring dirty blocks are
    /// flushed to S3 before shutdown. This method only persists local metadata
    /// and transitions the cache state.
    #[instrument(skip(self))]
    pub async fn shutdown(self) -> Result<WriteCache<Draining>, CacheError> {
        info!("starting graceful shutdown");

        // Save final metadata
        self.inner.save_metadata()?;
        info!("shutdown complete");

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }

    /// Persist block states and truncate WAL.
    fn checkpoint(&self) -> Result<(), CacheError> {
        let mut wal = self.inner.wal.lock();
        self.inner.save_block_states()?;
        wal.truncate()?;
        Ok(())
    }

    /// Local checkpoint: compute CRC32s for dirty blocks, persist state, truncate WAL.
    ///
    /// Independent of S3 — keeps the WAL bounded in demand-driven mode
    /// where S3 flushes may be infrequent. Should run every ~5s when
    /// there are dirty blocks.
    pub fn local_checkpoint(&self) -> Result<(), CacheError> {
        self.compute_dirty_crc32s();
        self.checkpoint()?;
        debug!("local checkpoint complete");
        Ok(())
    }

    /// Compute CRC32 checksums for dirty blocks that don't have one yet.
    ///
    /// Stores CRCs in the crc_map (DashMap) for later verification by the
    /// flush path. Only runs on the flush scheduler thread. Writes invalidate
    /// stale CRCs via crc_map.remove in their hot path.
    ///
    /// Capped at MAX_CRC_ENTRIES to bound memory. Blocks beyond the cap skip
    /// CRC verification at flush time — the SYNCING state machine still
    /// guarantees correctness; we just lose SSD corruption detection for those
    /// blocks.
    fn compute_dirty_crc32s(&self) {
        /// Maximum crc_map entries per export. 10M entries × ~20 bytes
        /// (DashMap overhead: bucket metadata + alignment + load factor) ≈ 200MB.
        /// Covers a fully-dirty 1TB device (8M blocks of 128KB) with headroom.
        /// Prevents unbounded growth if device_size is ever misconfigured.
        const MAX_CRC_ENTRIES: usize = 10_000_000;

        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let mut buf = vec![0u8; block_size];
        let mut computed = 0u64;

        for idx in self
            .inner
            .state_map
            .iter_with_state(SparseBlockState::DIRTY)
        {
            // Bound memory: stop computing CRCs once we hit the cap.
            if self.inner.crc_map.len() >= MAX_CRC_ENTRIES {
                break;
            }

            // Skip blocks that already have a CRC
            if self.inner.crc_map.contains_key(&idx) {
                continue;
            }

            let offset = idx as u64 * block_size as u64;
            let valid_bytes =
                std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;
            if valid_bytes == 0 {
                continue;
            }

            if let Err(e) = self
                .inner
                .data_file
                .read_exact_at(&mut buf[..valid_bytes], offset)
            {
                warn!(
                    chunk_index = idx,
                    error = %e,
                    "failed to read block for CRC32 computation"
                );
                continue;
            }
            if valid_bytes < block_size {
                buf[valid_bytes..].fill(0);
            }

            let crc = crc32fast::hash(&buf);
            self.inner.crc_store(idx, crc);
            computed += 1;
        }

        if computed > 0 {
            debug!(computed, "computed CRC32 checksums for dirty blocks");
        }
    }

    /// Reference to the per-export flush serialization lock.
    ///
    /// The flush_scheduler must acquire this lock around the
    /// `flush_packs` + `sync_manifest` sequence to prevent concurrent
    /// manifest uploads from racing with `flush_to_s3` (drain path).
    pub(crate) fn flush_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.inner.flush_lock
    }

    /// Get a clone of the inner Arc for sharing with the sync worker.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> Arc<CacheInner> {
        Arc::clone(&self.inner)
    }

    /// Chunked flush: claim dirty blocks via CAS DIRTY→SYNCING, partition by
    /// chunk (128 MiB), per-chunk dedup/compress/upload as GLPK v2 packs,
    /// append pack_id to VolumeManifest, CAS SYNCING→CLEAN.
    ///
    /// Returns (stats, seq_cutpoint) on success.
    async fn flush_dirty_inner(
        &self,
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<(FlushStats, u64), CacheError> {
        let seq_cutpoint = self.inner.sequence.current();

        // Claim dirty blocks: CAS DIRTY→SYNCING.
        let snapshot: Vec<usize> = self
            .inner
            .state_map
            .iter_with_state(SparseBlockState::DIRTY)
            .filter(|&idx| self.inner.transition_dirty_to_syncing(idx))
            .collect();

        if snapshot.is_empty() {
            debug!("flush: no dirty blocks to flush");
            return Ok((FlushStats::default(), seq_cutpoint));
        }

        info!(dirty_blocks = snapshot.len(), seq_cutpoint, "starting flush");

        let result = self
            .flush_dirty_body(&snapshot, content_store, pack_index_cache, volume_manifest)
            .await;

        if result.is_err() {
            for &idx in &snapshot {
                self.inner.transition_to_dirty(idx);
            }
        }

        result.map(|stats| (stats, seq_cutpoint))
    }

    /// Inner body of flush_dirty_inner, factored out for error recovery.
    ///
    /// Per-chunk: warm PackIndexCache, compute_flush_batch (rayon: pread +
    /// CRC32 + BLAKE3 + LZ4), assemble GLPK v2 pack, upload to S3, update
    /// PackIndexCache. Manifest appends are staged and applied atomically
    /// after all chunk uploads succeed. No .meta upload.
    async fn flush_dirty_body(
        &self,
        snapshot: &[usize],
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<FlushStats, CacheError> {
        use crate::block::pack::{new_pack_id, assemble_pack};

        // Partition dirty blocks by chunk
        let mut per_chunk: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        {
            let vm = volume_manifest.read();
            for &block_index in snapshot {
                let chunk_idx = vm.chunk_idx_for_block(block_index as u64);
                per_chunk.entry(chunk_idx).or_default().push(block_index);
            }
        }

        // Warm PackIndexCache for all affected chunks (cold start path).
        // This ensures the read path can resolve blocks after flush without an
        // extra S3 round-trip per pack. Not used for write-path dedup (packs use
        // offset-based dedup, not hash-based — see comment below).
        for &chunk_idx in per_chunk.keys() {
            let pack_ids = {
                let vm = volume_manifest.read();
                vm.chunk_pack_ids(chunk_idx)
                    .map(|ids| ids.to_vec())
                    .unwrap_or_default()
            };
            for &pid in &pack_ids {
                if pack_index_cache.get_entries(pid).await.is_none() {
                    match content_store.get_pack_index(chunk_idx, pid).await {
                        Ok(entries) => {
                            pack_index_cache.insert_entries(pid, &entries);
                        }
                        Err(e) => {
                            warn!(
                                chunk_idx, pack_id = pid,
                                error = %e,
                                "failed to warm pack index cache from S3"
                            );
                        }
                    }
                }
            }
        }

        let mut total_stats = FlushStats::default();
        let mut all_computed: Vec<(usize, Blake3Hash)> = Vec::new();
        let mut staged_appends: Vec<(u32, crate::block::pack::PackId)> = Vec::new();

        // Per-chunk flush
        for (chunk_idx, chunk_blocks) in per_chunk {
            // Compute batch (pread + CRC32 + BLAKE3 + LZ4)
            let zero_hash = self.inner.zero_block_hash;
            let inner = Arc::clone(&self.inner);
            let mut batch = crate::task::spawn_blocking_named("flush-compute", move || {
                compute_flush_batch(&inner, &chunk_blocks, zero_hash)
            })
            .await
            .map_err(|e| CacheError::Io(std::io::Error::other(e)))??;

            total_stats.blocks_flushed += batch.stats.blocks_flushed;
            total_stats.blocks_deduped += batch.stats.blocks_deduped;
            total_stats.blocks_cas_failed += batch.stats.blocks_cas_failed;
            total_stats.blocks_corrupted += batch.stats.blocks_corrupted;

            // Transition skipped blocks back SYNCING→DIRTY
            for &idx in &batch.skipped {
                self.inner.transition_to_dirty(idx);
            }

            // Build hash → compressed data map from the unique-hash upload set.
            // Within-batch dedup (seen_hashes in compute_flush_batch) ensures one
            // copy of compressed data per unique hash.
            let hash_to_compressed: HashMap<Blake3Hash, Vec<u8>> =
                std::mem::take(&mut batch.to_upload).into_iter().collect();

            if hash_to_compressed.is_empty() {
                // All blocks are zero — nothing to upload, still record for CAS.
                all_computed.extend(batch.computed);
                continue;
            }

            // Build one pack entry per non-zero dirty block, each at its actual
            // chunk_offset. Two blocks with the same hash but different chunk_offsets
            // both get entries (clone of compressed bytes), ensuring both are
            // resolvable by the read path.
            let blocks_for_pack: Vec<(Blake3Hash, u32, Vec<u8>)> = {
                let vm = volume_manifest.read();
                batch
                    .computed
                    .iter()
                    .filter_map(|&(block_index, hash)| {
                        // hash_to_compressed excludes zero_hash (zero blocks → None in
                        // compute_flush_batch → not in to_upload).
                        let compressed = hash_to_compressed.get(&hash)?.clone();
                        let chunk_offset = vm.block_offset_in_chunk(block_index as u64);
                        Some((hash, chunk_offset, compressed))
                    })
                    .collect()
            };

            // Assemble GLPK v2 pack (sorted by chunk_offset for read locality)
            let blocks_per_chunk = {
                let vm = volume_manifest.read();
                vm.blocks_per_chunk()
            };
            let (pack_bytes, index_entries) =
                assemble_pack(blocks_for_pack, blocks_per_chunk)?;
            let pack_size = pack_bytes.len() as u64;
            let pack_id = new_pack_id();

            // Upload pack to S3
            content_store
                .put_chunk_pack(chunk_idx, pack_id, pack_bytes)
                .await?;

            total_stats.packs_uploaded += 1;
            total_stats.bytes_uploaded += pack_size;

            // Update PackIndexCache with new entries
            pack_index_cache.insert_entries(pack_id, &index_entries);

            // Stage manifest append (applied after all chunk uploads succeed).
            // This avoids orphaned manifest entries if a later chunk's S3 upload fails.
            staged_appends.push((chunk_idx, pack_id));

            all_computed.extend(batch.computed);
        }

        // Apply all staged manifest appends atomically.
        // Only reached if every chunk upload succeeded.
        if !staged_appends.is_empty() {
            let mut vm = volume_manifest.write();
            for (chunk_idx, pack_id) in staged_appends {
                vm.append_pack(chunk_idx, pack_id);
            }
        }

        // CAS SYNCING→CLEAN for successfully flushed blocks
        for &(chunk_index, _) in &all_computed {
            if !self.inner.transition_syncing_to_clean(chunk_index) {
                total_stats.blocks_cas_failed += 1;
            }
        }

        info!(
            blocks_flushed = total_stats.blocks_flushed,
            blocks_deduped = total_stats.blocks_deduped,
            blocks_cas_failed = total_stats.blocks_cas_failed,
            blocks_corrupted = total_stats.blocks_corrupted,
            packs_uploaded = total_stats.packs_uploaded,
            bytes_uploaded = total_stats.bytes_uploaded,
            "flush complete"
        );

        Ok(total_stats)
    }

    /// Flush dirty blocks to S3 as chunk-scoped GLPK v2 packs (no manifest upload).
    ///
    /// Returns (stats, seq_cutpoint). Compaction is the caller's responsibility
    /// and should run outside the flush lock to avoid blocking concurrent flushes.
    pub async fn flush_packs(
        &self,
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<(FlushStats, u64), CacheError> {
        self.flush_dirty_inner(content_store, pack_index_cache, volume_manifest)
            .await
    }

    /// Upload the VolumeManifest (binary GLVM) to S3.
    pub async fn sync_manifest(
        &self,
        content_store: &ContentStore,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<(), CacheError> {
        let manifest_bytes = volume_manifest.read().serialize();
        content_store
            .put_manifest(&self.inner.export_name, manifest_bytes)
            .await?;
        self.checkpoint()?;
        Ok(())
    }

    /// Flush dirty blocks to S3 + upload manifest (drain/snapshot path).
    ///
    /// Retries manifest upload up to 3 times before propagating the error,
    /// mirroring flush_scheduler's pattern. This prevents spurious drain
    /// failures when blocks are already clean but a transient S3 error
    /// prevents the manifest upload.
    #[instrument(skip(self, content_store, pack_index_cache, volume_manifest))]
    pub async fn flush_to_s3(
        &self,
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<FlushStats, CacheError> {
        let _flush_guard = self.inner.flush_lock.lock().await;
        let (stats, _seq_cutpoint) = self
            .flush_dirty_inner(content_store, pack_index_cache, volume_manifest)
            .await?;
        let manifest_bytes = volume_manifest.read().serialize();
        let mut last_err = None;
        for attempt in 0..3u32 {
            match content_store
                .put_manifest(&self.inner.export_name, manifest_bytes.clone())
                .await
            {
                Ok(_) => {
                    last_err = None;
                    break;
                }
                Err(e) => {
                    warn!(
                        error = %e, attempt = attempt + 1,
                        "manifest upload failed in flush_to_s3, retrying"
                    );
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            return Err(e.into());
        }
        self.checkpoint()?;
        Ok(stats)
    }

    /// Take a point-in-time snapshot.
    #[instrument(skip(self, content_store, pack_index_cache, volume_manifest))]
    pub async fn snapshot(
        &self,
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<SnapshotResult, CacheError> {
        let _flush_guard = self.inner.flush_lock.lock().await;
        let (stats, seq_cutpoint) = self
            .flush_dirty_inner(content_store, pack_index_cache, volume_manifest)
            .await?;

        let manifest_bytes = volume_manifest.read().serialize();
        let manifest_etag = content_store
            .put_manifest(&self.inner.export_name, manifest_bytes.clone())
            .await?;

        if let Err(e) = content_store
            .put_snapshot(&self.inner.export_name, seq_cutpoint, manifest_bytes)
            .await
        {
            warn!(
                error = %e, sequence = seq_cutpoint,
                "failed to persist versioned snapshot (continuing)"
            );
        }

        self.checkpoint()?;
        Ok(SnapshotResult {
            manifest_etag,
            sequence: seq_cutpoint,
            stats,
        })
    }
}
