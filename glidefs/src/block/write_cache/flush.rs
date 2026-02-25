use std::collections::{BTreeMap, HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, info, instrument, warn};

use crate::block::block_map::{Blake3Hash, SparseBlockState, blake3_128, format_hash, lz4_compress};
use crate::block::chunk_cache::ChunkMetaCache;
use crate::block::chunk_meta::{ChunkMeta, ChunkMetaEntry};
use crate::block::content_store::ContentStore;
use crate::block::pack::{self, DEFAULT_BLOCKS_PER_PACK};
use crate::block::state::{Active, Draining};
use crate::block::volume_manifest::VolumeManifest;

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
        /// `Some(compressed)` if block is new (not zero, not already known).
        /// `None` if deduped (zero block or already in chunk meta).
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
/// Phase 1 (rayon parallel): pread + crc32 + blake3 + known-hash check + lz4 per block.
/// Phase 2 (sequential): within-batch dedup via seen_hashes (cheap hash-set insertions).
///
/// All blocks in `snapshot` have already been claimed (CAS DIRTY→SYNCING).
/// CRC verification uses the crc_map (DashMap) and state discrimination:
/// - CRC mismatch + state == SYNCING → real SSD corruption
/// - CRC mismatch + state != SYNCING → concurrent write re-dirtied the block
fn compute_flush_batch(
    inner: &CacheInner,
    snapshot: &[usize],
    known_hashes: &HashSet<Blake3Hash>,
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

            // Zero block or already known in chunk meta → deduped, no upload needed.
            let compressed = if hash == zero_hash || known_hashes.contains(&hash) {
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
                        // Zero block or already in chunk meta.
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

    /// Chunked flush: claim dirty blocks via CAS DIRTY→SYNCING, partition by
    /// volume chunk, per-chunk dedup/compress/upload, CAS SYNCING→CLEAN.
    ///
    /// Returns (stats, seq_cutpoint) on success. seq_cutpoint is used for
    /// snapshot versioning only (not for per-block CAS).
    async fn flush_dirty_inner(
        &self,
        content_store: &ContentStore,
        chunk_meta_cache: &Arc<ChunkMetaCache>,
        volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
    ) -> Result<(FlushStats, u64), CacheError> {
        let block_size = self.inner.config.block_size as u32;

        // Capture sequence for snapshot versioning (not per-block CAS).
        let seq_cutpoint = self.inner.sequence.current();

        // Claim dirty blocks: CAS DIRTY→SYNCING.
        // Only blocks that successfully transition are included in the flush.
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

        info!(
            dirty_blocks = snapshot.len(),
            seq_cutpoint, "starting flush"
        );

        // Run the flush body. If any error occurs after claiming blocks,
        // revert all SYNCING blocks back to DIRTY so they're retried.
        let result = self
            .flush_dirty_body(&snapshot, block_size, content_store, chunk_meta_cache, volume_manifest)
            .await;

        if result.is_err() {
            for &idx in &snapshot {
                // transition_to_dirty handles SYNCING→DIRTY correctly
                // (decrements syncing_count, increments dirty_count).
                // No-op if already transitioned by a concurrent write.
                self.inner.transition_to_dirty(idx);
            }
        }

        result.map(|stats| (stats, seq_cutpoint))
    }

    /// Inner body of flush_dirty_inner, factored out for error recovery.
    async fn flush_dirty_body(
        &self,
        snapshot: &[usize],
        block_size: u32,
        content_store: &ContentStore,
        chunk_meta_cache: &Arc<ChunkMetaCache>,
        volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
    ) -> Result<FlushStats, CacheError> {
        // 3. Partition dirty blocks by volume chunk
        let mut per_chunk: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        {
            let vm = volume_manifest.read();
            for &block_index in snapshot {
                let chunk_idx = vm.chunk_idx_for_block(block_index as u64);
                per_chunk.entry(chunk_idx).or_default().push(block_index);
            }
        }

        // 4. Pre-fetch existing ChunkMeta for all affected chunks
        for &chunk_idx in per_chunk.keys() {
            let chunk_hash = volume_manifest.read().get_chunk_hash(chunk_idx);
            if let Some(chunk_hash) = chunk_hash && chunk_meta_cache.get(&chunk_hash).await.is_none() {
                let hash_hex = format_hash(&chunk_hash);
                match content_store.get_chunk_meta(chunk_idx, &hash_hex).await {
                    Ok(Some(data)) => match ChunkMeta::deserialize(&data) {
                        Ok(meta) => {
                            chunk_meta_cache.insert(chunk_hash, Arc::new(meta));
                        }
                        Err(e) => {
                            warn!(chunk_idx, error = %e, "failed to deserialize chunk meta from S3");
                        }
                    },
                    Ok(None) => {
                        warn!(chunk_idx, "chunk meta not found in S3");
                    }
                    Err(e) => {
                        warn!(chunk_idx, error = %e, "failed to fetch chunk meta from S3");
                    }
                }
            }
        }

        let mut total_stats = FlushStats::default();
        let mut all_computed: Vec<(usize, Blake3Hash)> = Vec::new();
        let mut chunk_updates: Vec<(u32, Blake3Hash)> = Vec::new();

        // 5. Per-chunk flush
        for (chunk_idx, chunk_blocks) in per_chunk {
            let existing_chunk_hash = volume_manifest.read().get_chunk_hash(chunk_idx);
            let existing_meta = match existing_chunk_hash {
                Some(h) => chunk_meta_cache.get(&h).await,
                None => None,
            };

            // Build dedup HashSet from existing entries
            let known_hashes: HashSet<Blake3Hash> = existing_meta
                .as_ref()
                .map(|m| m.block_hashes())
                .unwrap_or_default();

            // Build existing hash→pack_location map (for reusing pack locations of deduped blocks)
            let existing_hash_locs: HashMap<Blake3Hash, (uuid::Uuid, u32, u32)> = existing_meta
                .as_ref()
                .map(|m| {
                    m.entries
                        .iter()
                        .map(|e| (e.hash, (e.pack_id, e.pack_offset, e.comp_length)))
                        .collect()
                })
                .unwrap_or_default();

            // Compute batch for this chunk's blocks
            let zero_hash = self.inner.zero_block_hash;
            let inner = Arc::clone(&self.inner);
            let known = known_hashes;
            let mut batch = crate::task::spawn_blocking_named("flush-compute", move || {
                compute_flush_batch(&inner, &chunk_blocks, &known, zero_hash)
            })
            .await
            .map_err(|e| CacheError::Io(std::io::Error::other(e)))??;

            total_stats.blocks_flushed += batch.stats.blocks_flushed;
            total_stats.blocks_deduped += batch.stats.blocks_deduped;
            total_stats.blocks_cas_failed += batch.stats.blocks_cas_failed;
            total_stats.blocks_corrupted += batch.stats.blocks_corrupted;

            // Transition skipped blocks back SYNCING→DIRTY so they're
            // picked up by the next flush cycle.
            for &idx in &batch.skipped {
                self.inner.transition_to_dirty(idx);
            }

            // Assemble and upload packs (chunk-scoped)
            use futures::stream::{self, StreamExt};

            let to_upload = std::mem::take(&mut batch.to_upload);
            let mut owned_chunks: Vec<Vec<(Blake3Hash, Vec<u8>)>> = Vec::new();
            {
                let mut iter = to_upload.into_iter().peekable();
                while iter.peek().is_some() {
                    owned_chunks
                        .push(iter.by_ref().take(DEFAULT_BLOCKS_PER_PACK).collect());
                }
            }

            // Build hash→pack_location map from upload results
            let mut hash_to_pack_loc: HashMap<Blake3Hash, (uuid::Uuid, u32, u32)> = HashMap::new();

            if !owned_chunks.is_empty() {
                #[allow(clippy::type_complexity)]
                let pack_results: Vec<
                    Result<(uuid::Uuid, u64, Vec<pack::PackIndexEntry>), CacheError>,
                > = stream::iter(owned_chunks)
                    .map(|chunk| {
                        let cs = content_store;
                        let cidx = chunk_idx;
                        async move {
                            let pack_id = uuid::Uuid::new_v4();
                            let (pack_bytes, index_entries) =
                                pack::assemble_pack(chunk, block_size)?;
                            let pack_size = pack_bytes.len() as u64;
                            cs.put_chunk_pack(cidx, pack_id, pack_bytes).await?;
                            Ok((pack_id, pack_size, index_entries))
                        }
                    })
                    .buffer_unordered(4)
                    .collect()
                    .await;

                for result in pack_results {
                    let (pack_id, pack_size, index_entries) = result?;
                    total_stats.packs_uploaded += 1;
                    total_stats.bytes_uploaded += pack_size;
                    total_stats.new_pack_ids.push(pack_id);
                    for entry in &index_entries {
                        hash_to_pack_loc
                            .insert(entry.hash, (pack_id, entry.offset, entry.comp_length));
                    }
                }
            }

            // Build new ChunkMetaEntry list from computed results
            let mut new_entries: Vec<ChunkMetaEntry> = Vec::new();
            {
                let vm = volume_manifest.read();
                for &(block_index, hash) in &batch.computed {
                    if hash == zero_hash || hash == self.inner.zero_block_hash {
                        continue; // zero blocks don't need entries
                    }
                    let block_offset = vm.block_offset_in_chunk(block_index as u64);

                    // Try newly uploaded packs first, then existing entries
                    let pack_loc = hash_to_pack_loc
                        .get(&hash)
                        .or_else(|| existing_hash_locs.get(&hash));

                    if let Some(&(pack_id, pack_offset, comp_length)) = pack_loc {
                        new_entries.push(ChunkMetaEntry {
                            offset: block_offset,
                            hash,
                            pack_id,
                            pack_offset,
                            comp_length,
                        });
                    }
                }
            }

            // Read chunk geometry from VolumeManifest
            let (chunk_size_bytes, device_block_size) = {
                let vm = volume_manifest.read();
                (vm.chunk_size, vm.block_size)
            };

            // Merge with existing ChunkMeta to produce new version
            let new_meta = if let Some(ref old_meta) = existing_meta {
                ChunkMeta::merge(old_meta, &new_entries)
            } else {
                let mut entries = new_entries;
                entries.sort_by_key(|e| e.offset);
                ChunkMeta {
                    chunk_idx,
                    chunk_size: chunk_size_bytes,
                    block_size: device_block_size,
                    entries,
                }
            };

            // Compute new content hash and upload
            let new_chunk_hash = new_meta.content_hash();
            let meta_bytes = new_meta.serialize();
            let hash_hex = format_hash(&new_chunk_hash);
            content_store
                .put_chunk_meta(chunk_idx, &hash_hex, meta_bytes)
                .await?;

            // Update cache
            chunk_meta_cache.insert(new_chunk_hash, Arc::new(new_meta));
            chunk_updates.push((chunk_idx, new_chunk_hash));

            // Collect all computed entries for post-upload CAS
            all_computed.extend(batch.computed);
        }

        // 6. CAS SYNCING→CLEAN for successfully flushed blocks.
        // If a concurrent write transitioned SYNCING→DIRTY, the CAS fails
        // and the block stays DIRTY for the next flush cycle.
        for &(chunk_index, _actual_hash) in &all_computed {
            if !self.inner.transition_syncing_to_clean(chunk_index) {
                total_stats.blocks_cas_failed += 1;
            }
        }

        // 7. Update VolumeManifest with new chunk hashes
        {
            let mut vm = volume_manifest.write();
            for &(chunk_idx, ref chunk_hash) in &chunk_updates {
                vm.set_chunk_hash(chunk_idx, *chunk_hash);
            }
        }

        info!(
            blocks_flushed = total_stats.blocks_flushed,
            blocks_deduped = total_stats.blocks_deduped,
            blocks_cas_failed = total_stats.blocks_cas_failed,
            blocks_corrupted = total_stats.blocks_corrupted,
            packs_uploaded = total_stats.packs_uploaded,
            bytes_uploaded = total_stats.bytes_uploaded,
            chunks_updated = chunk_updates.len(),
            "flush dirty inner complete"
        );

        Ok(total_stats)
    }

    /// Flush dirty blocks to S3 as chunk-scoped packs (no manifest upload).
    ///
    /// Returns flush statistics and the sequence cutpoint for a subsequent
    /// `sync_manifest()` call. Used by the flush scheduler to separate
    /// pack flushes (~5s) from manifest syncs (~60s).
    pub async fn flush_packs(
        &self,
        content_store: &ContentStore,
        chunk_meta_cache: &Arc<ChunkMetaCache>,
        volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
    ) -> Result<(FlushStats, u64), CacheError> {
        self.flush_dirty_inner(content_store, chunk_meta_cache, volume_manifest)
            .await
    }

    /// Upload the VolumeManifest to S3.
    ///
    /// Call after `flush_packs()` with the returned `seq_cutpoint` to persist
    /// a recovery manifest.
    pub async fn sync_manifest(
        &self,
        content_store: &ContentStore,
        volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
    ) -> Result<(), CacheError> {
        let manifest_bytes = volume_manifest.read().serialize();
        content_store
            .put_volume_manifest(&self.inner.export_name, manifest_bytes)
            .await?;
        self.checkpoint()?;
        Ok(())
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

    /// Flush dirty blocks to S3 as chunk-scoped packs + volume manifest.
    ///
    /// This is the v3 chunked flush path:
    /// 1. Claim dirty blocks via CAS DIRTY→SYNCING
    /// 2. Partition by volume chunk, dedup against chunk meta
    /// 3. LZ4-compress new blocks and assemble into packs per chunk
    /// 4. Upload packs to S3 concurrently as chunks/{idx}/{uuid}.pack
    /// 5. Build new ChunkMeta, upload as chunks/{idx}/{hash}.meta
    /// 6. CAS SYNCING→CLEAN (fails if concurrent write re-dirtied)
    /// 7. Upload VolumeManifest
    /// 8. Checkpoint: persist block states + truncate WAL
    #[instrument(skip(self, content_store, chunk_meta_cache, volume_manifest))]
    pub async fn flush_to_s3(
        &self,
        content_store: &ContentStore,
        chunk_meta_cache: &Arc<ChunkMetaCache>,
        volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
    ) -> Result<FlushStats, CacheError> {
        let _flush_guard = self.inner.flush_lock.lock().await;
        let (stats, _seq_cutpoint) = self
            .flush_dirty_inner(content_store, chunk_meta_cache, volume_manifest)
            .await?;
        // Upload VolumeManifest
        let manifest_bytes = volume_manifest.read().serialize();
        content_store
            .put_volume_manifest(&self.inner.export_name, manifest_bytes)
            .await?;
        self.checkpoint()?;
        Ok(stats)
    }

    /// Take a point-in-time snapshot: flush dirty blocks + upload volume manifest.
    ///
    /// In addition to uploading the current volume manifest (overwriting
    /// `manifests/{name}`), this persists a versioned copy at
    /// `snapshots/{name}/{sequence:020}` that is never overwritten by
    /// background flushes.
    #[instrument(skip(self, content_store, chunk_meta_cache, volume_manifest))]
    pub async fn snapshot(
        &self,
        content_store: &ContentStore,
        chunk_meta_cache: &Arc<ChunkMetaCache>,
        volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
    ) -> Result<SnapshotResult, CacheError> {
        let _flush_guard = self.inner.flush_lock.lock().await;
        let (stats, seq_cutpoint) = self
            .flush_dirty_inner(content_store, chunk_meta_cache, volume_manifest)
            .await?;

        // Upload current volume manifest
        let manifest_bytes = volume_manifest.read().serialize();
        let manifest_etag = content_store
            .put_volume_manifest(&self.inner.export_name, manifest_bytes.clone())
            .await?;

        // Persist versioned snapshot (best-effort)
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
}
