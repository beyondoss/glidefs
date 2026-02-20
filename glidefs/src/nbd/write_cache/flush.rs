use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

use crate::nbd::block_map::{
    BlockMapKind, Blake3Hash, SparseBlockState, blake3_128, lz4_compress,
};
use crate::nbd::content_store::ContentStore;
use crate::nbd::manifest::{Manifest, ManifestBlockEntry, ManifestDelta};
use crate::nbd::pack::{self, PackLocation, DEFAULT_BLOCKS_PER_PACK};
use crate::nbd::pack_index::HostPackIndex;
use crate::nbd::pack_registry::PackRegistry;
use crate::nbd::state::{Active, Draining};

use super::inner::CacheInner;
use super::{CacheError, FlushStats, SnapshotResult, WriteCache};

/// Result of CPU-heavy flush computation (pread + crc32 + blake3 + lz4).
///
/// Produced by `compute_flush_batch` on a blocking thread, consumed by
/// the async portion of `flush_dirty_inner` for S3 uploads.
struct FlushBatch {
    /// Compressed blocks ready for S3 upload: (hash, compressed_bytes).
    to_upload: Vec<(Blake3Hash, Vec<u8>)>,
    /// Computed hashes for CAS-clearing dirty flags: (chunk_index, snapshot_seq, hash).
    computed: Vec<(usize, u64, Blake3Hash)>,
    /// Partial statistics from the computation phase.
    stats: FlushStats,
}

/// Per-block result from the parallel compute phase.
enum BlockResult {
    /// Successfully computed hash; may include compressed data for upload.
    Computed {
        chunk_index: usize,
        snapshot_seq: u64,
        hash: Blake3Hash,
        /// `Some(compressed)` if block is new (not zero, not in pack index).
        /// `None` if deduped (zero block or already in pack index).
        compressed: Option<Vec<u8>>,
    },
    /// Block skipped due to CRC mismatch or concurrent write.
    Skipped {
        cas_failed: bool,
        corrupted: bool,
    },
}

/// CPU-heavy flush computation: read blocks from SSD, verify CRC32, hash, compress, dedup.
///
/// Runs on a blocking thread via `spawn_blocking` to avoid starving the async runtime.
/// Phase 1 (rayon parallel): pread + crc32 + blake3 + pack-index check + lz4 per block.
/// Phase 2 (sequential): within-batch dedup via seen_hashes (cheap hash-set insertions).
fn compute_flush_batch(
    inner: &CacheInner,
    snapshot: &[(usize, u64)],
    host_pack_index: &HostPackIndex,
    zero_hash: Blake3Hash,
) -> Result<FlushBatch, CacheError> {
    use rayon::prelude::*;

    let block_size = inner.config.block_size;
    let device_size = inner.config.device_size;

    // Phase 1: parallel per-block compute (pread + crc32 + blake3 + pack-index + lz4).
    // Each rayon task allocates its own read buffer; peak memory = num_threads × block_size.
    let per_block: Vec<Result<BlockResult, CacheError>> = snapshot
        .par_iter()
        .map(|&(chunk_index, snapshot_seq)| {
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
            let stored_crc = inner.block_map_get_crc32(chunk_index);
            if stored_crc != 0 {
                let computed_crc = crc32fast::hash(&chunk_buf);
                if computed_crc != stored_crc {
                    let (_, current_seq) = inner.block_map_get(chunk_index);
                    if current_seq != snapshot_seq {
                        return Ok(BlockResult::Skipped {
                            cas_failed: true,
                            corrupted: false,
                        });
                    }
                    warn!(
                        chunk_index,
                        stored_crc,
                        computed_crc,
                        "CRC32 mismatch — possible SSD corruption, skipping block this cycle"
                    );
                    inner.block_map_clear_crc32(chunk_index);
                    return Ok(BlockResult::Skipped {
                        cas_failed: false,
                        corrupted: true,
                    });
                }
            }

            let hash = blake3_128(&chunk_buf);

            // Zero block or already in pack index → deduped, no upload needed.
            let compressed =
                if hash == zero_hash || host_pack_index.contains(&hash)? {
                    None
                } else {
                    Some(lz4_compress(&chunk_buf[..]))
                };

            Ok(BlockResult::Computed {
                chunk_index,
                snapshot_seq,
                hash,
                compressed,
            })
        })
        .collect();

    // Phase 2: sequential aggregation — within-batch dedup + stats.
    let mut stats = FlushStats::default();
    let mut to_upload: Vec<(Blake3Hash, Vec<u8>)> = Vec::new();
    let mut seen_hashes = std::collections::HashSet::new();
    let mut computed: Vec<(usize, u64, Blake3Hash)> = Vec::new();

    for result in per_block {
        let result = result?;
        match result {
            BlockResult::Skipped {
                cas_failed,
                corrupted,
            } => {
                stats.blocks_flushed += 1;
                if cas_failed {
                    stats.blocks_cas_failed += 1;
                }
                if corrupted {
                    stats.blocks_corrupted += 1;
                }
            }
            BlockResult::Computed {
                chunk_index,
                snapshot_seq,
                hash,
                compressed,
            } => {
                stats.blocks_flushed += 1;
                computed.push((chunk_index, snapshot_seq, hash));

                match compressed {
                    None => {
                        // Zero block or already in pack index.
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

    /// Return the cached manifest pack hashes.
    ///
    /// These are the hashes from the most recent manifest build (either from
    /// `upload_manifest` or `rebuild_manifest_hashes`). Used by pack index
    /// pruning — the manifest is the durable reference, not the live block_map
    /// which has ZERO placeholders for in-flight flushes.
    pub fn referenced_hashes(&self) -> std::collections::HashSet<Blake3Hash> {
        self.inner.manifest_pack_hashes.lock().clone()
    }

    /// Force-rebuild the cached manifest pack hashes from current state.
    ///
    /// Snapshots the block map and derives pack hashes via `derive_for_block_map`,
    /// capturing everything flushed up to this moment. No S3 round-trip.
    ///
    /// Called by `prune_pack_index` immediately before pruning to close the
    /// timing window between the last manifest upload and the prune call.
    pub fn rebuild_manifest_hashes(&self, host_pack_index: &HostPackIndex) -> Result<(), CacheError> {
        let snap = self.inner.block_map_snapshot();
        let hashes = host_pack_index
            .derive_for_block_map(&snap)?
            .into_iter()
            .map(|e| e.hash)
            .collect();
        *self.inner.manifest_pack_hashes.lock() = hashes;
        Ok(())
    }

    /// Save metadata to disk.
    #[allow(dead_code)]
    pub fn save_metadata(&self) -> Result<(), CacheError> {
        self.inner.save_metadata()
    }

    /// Graceful shutdown: save metadata and transition to Draining.
    ///
    /// The v2 flush scheduler is responsible for ensuring dirty blocks are
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

    /// Shared inner logic for flush/snapshot: collect dirty entries, dedup,
    /// compress, pack upload, CAS-clear dirty flags.
    ///
    /// Optimizations vs naive approach:
    /// - **Targeted dirty scan**: reads `block_states` (cheap byte loads) then
    ///   only reads block_map entries for dirty blocks. Cost is O(dirty_count)
    ///   SeqLock reads instead of O(device_size).
    /// - **Concurrent S3 uploads**: packs are uploaded in parallel via
    ///   `try_join_all` instead of sequentially.
    ///
    /// Returns (stats, seq_cutpoint) on success.
    async fn flush_dirty_inner(
        &self,
        content_store: &ContentStore,
        host_pack_index: &Arc<HostPackIndex>,
    ) -> Result<(FlushStats, u64), CacheError> {
        let chunk_size = self.inner.config.block_size as u32;

        // 1. Capture sequence cut point
        let seq_cutpoint = self.inner.sequence.current();

        // 2. Targeted dirty scan: collect dirty block indices with their
        //    snapshot sequence number (used for concurrent-write detection).
        //    Hashes are computed lazily from SSD in step 3.
        //    Uses SparseStateMap::iter_with_state for O(allocated_pages) scan.
        let snapshot: Vec<(usize, u64)> = {
            let mut dirty = Vec::new();
            for idx in self.inner.state_map.iter_with_state(SparseBlockState::DIRTY) {
                let (_hash, seq) = self.inner.block_map_get(idx);
                // Skip entries written after our cutpoint
                if seq > seq_cutpoint {
                    continue;
                }
                dirty.push((idx, seq));
            }
            dirty
        };

        if snapshot.is_empty() {
            debug!("flush: no dirty blocks to flush");
            return Ok((FlushStats::default(), seq_cutpoint));
        }

        info!(dirty_blocks = snapshot.len(), seq_cutpoint, "starting flush");

        // 3. Offload CPU-heavy work (pread + crc32 + blake3 + lz4) to blocking thread pool.
        //    This prevents flush computation from starving the async runtime under
        //    high tenant count (~240μs/block × thousands of blocks = seconds of CPU).
        let zero_hash = self.inner.zero_block_hash;
        let inner = Arc::clone(&self.inner);
        let pack_index_clone = Arc::clone(host_pack_index);
        let mut batch = crate::task::spawn_blocking_named("flush-compute", move || {
            compute_flush_batch(&inner, &snapshot, &pack_index_clone, zero_hash)
        })
        .await
        .map_err(|e| CacheError::Io(std::io::Error::other(e)))??;

        let mut stats = batch.stats;

        // 4. Pipeline: assemble packs + upload with bounded concurrency.
        //    Each pack is assembled (consuming its compressed blocks) then uploaded.
        //    buffer_unordered(4) keeps at most 4 packs in flight, freeing memory
        //    as uploads complete instead of holding all packs at once.
        use futures::stream::{self, StreamExt};

        let to_upload = std::mem::take(&mut batch.to_upload);
        let mut owned_chunks: Vec<Vec<(Blake3Hash, Vec<u8>)>> = Vec::new();
        {
            let mut iter = to_upload.into_iter().peekable();
            while iter.peek().is_some() {
                owned_chunks.push(iter.by_ref().take(DEFAULT_BLOCKS_PER_PACK).collect());
            }
        }

        #[allow(clippy::type_complexity)]
        let pack_results: Vec<Result<(uuid::Uuid, u64, Vec<pack::PackIndexEntry>), CacheError>> =
            stream::iter(owned_chunks)
                .map(|chunk| {
                    let cs = content_store;
                    async move {
                        let pack_id = uuid::Uuid::new_v4();
                        let (pack_bytes, index_entries) = pack::assemble_pack(chunk, chunk_size)?;
                        let pack_size = pack_bytes.len() as u64;
                        cs.put_pack(pack_id, pack_bytes).await?;
                        Ok((pack_id, pack_size, index_entries))
                    }
                })
                .buffer_unordered(4)
                .collect()
                .await;

        // Register in host pack index for future dedup
        let mut batch_entries = Vec::new();
        for result in pack_results {
            let (pack_id, pack_size, index_entries) = result?;
            stats.packs_uploaded += 1;
            stats.bytes_uploaded += pack_size;
            stats.new_pack_ids.push(pack_id);
            for entry in &index_entries {
                batch_entries.push((
                    entry.hash,
                    PackLocation {
                        pack_id,
                        offset: entry.offset,
                        comp_length: entry.comp_length,
                    },
                ));
            }
        }
        host_pack_index.insert_batch(&batch_entries)?;

        // 5. Set real hashes in block_map + clear dirty flags.
        //    Sequence number serves as version token: if a concurrent write
        //    changed the sequence, leave the block dirty for the next flush.
        for &(chunk_index, snapshot_seq, actual_hash) in &batch.computed {
            let (_hash, current_seq) = self.inner.block_map_get(chunk_index);
            if current_seq != snapshot_seq {
                // Concurrent write changed this block — leave dirty.
                stats.blocks_cas_failed += 1;
                continue;
            }
            // Replace ZERO placeholder with real content hash.
            self.inner.block_map_set(chunk_index, actual_hash, snapshot_seq);
            // CAS Dirty(2) -> Clean(1) on the state map.
            if self.inner.state_map
                .cas(chunk_index, SparseBlockState::DIRTY, SparseBlockState::CLEAN)
                .is_ok()
            {
                self.inner
                    .dirty_block_count
                    .fetch_sub(1, Ordering::Relaxed);
            }
        }

        info!(
            blocks_flushed = stats.blocks_flushed,
            blocks_deduped = stats.blocks_deduped,
            blocks_cas_failed = stats.blocks_cas_failed,
            blocks_corrupted = stats.blocks_corrupted,
            packs_uploaded = stats.packs_uploaded,
            bytes_uploaded = stats.bytes_uploaded,
            "flush dirty inner complete"
        );

        Ok((stats, seq_cutpoint))
    }

    /// Build and upload a full (base) manifest from current state.
    ///
    /// After upload, populates `base_manifest_state` for future delta computation
    /// and deletes any existing `.delta` file from S3.
    ///
    /// Returns the S3 ETag if the backend provides one.
    async fn upload_full_manifest(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
        seq_cutpoint: u64,
    ) -> Result<Option<String>, CacheError> {
        let chunk_size = self.inner.config.block_size as u32;
        let post_flush_snap = self.inner.block_map_snapshot();
        let block_map_entries: Vec<ManifestBlockEntry> = post_flush_snap
            .iter_non_empty()
            .map(|(idx, entry)| ManifestBlockEntry {
                chunk_index: idx as u64,
                hash: entry.hash,
                flags: entry.flags,
            })
            .collect();
        let pack_entries = host_pack_index.derive_for_block_map(&post_flush_snap)?;

        let manifest = Manifest {
            name: self.inner.export_name.clone(),
            sequence: seq_cutpoint,
            chunk_size,
            device_size: self.inner.config.device_size,
            block_map: block_map_entries,
            pack_index: pack_entries,
        };

        // Cache the manifest's pack hashes for pack index pruning.
        *self.inner.manifest_pack_hashes.lock() =
            manifest.pack_index.iter().map(|e| e.hash).collect();

        let etag = content_store
            .put_manifest(&self.inner.export_name, manifest.serialize())
            .await?;

        // Update base_manifest_state for future delta computation.
        {
            use super::inner::BaseManifestState;
            let base_block_map = manifest
                .block_map
                .iter()
                .map(|e| (e.chunk_index, e.hash))
                .collect();
            *self.inner.base_manifest_state.lock() = Some(BaseManifestState {
                sequence: seq_cutpoint,
                block_map: base_block_map,
                syncs_since_base: 0,
            });
        }

        // Delete any existing delta (we just compacted).
        if let Err(e) = content_store
            .delete_delta_manifest(&self.inner.export_name)
            .await
        {
            warn!(error = %e, "failed to delete delta manifest after compaction (harmless)");
        }

        info!(sequence = seq_cutpoint, "uploaded full manifest");
        Ok(etag)
    }

    /// Build and upload a delta manifest if possible, otherwise fall back to full.
    ///
    /// Compaction triggers (fall back to full):
    /// 1. No base_manifest_state (first sync)
    /// 2. Delta size > 50% of estimated full manifest size
    /// 3. syncs_since_base >= 10
    async fn upload_delta_or_full_manifest(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
        seq_cutpoint: u64,
    ) -> Result<Option<String>, CacheError> {
        use super::inner::BaseManifestState;

        // Check compaction triggers while borrowing (no take yet).
        // Extract decision before any .await to avoid holding MutexGuard across await.
        let needs_full = {
            let guard = self.inner.base_manifest_state.lock();
            match guard.as_ref() {
                None => Some("no base manifest state"),
                Some(s) if s.syncs_since_base >= 10 => Some("compaction trigger: sync count"),
                Some(_) => None, // proceed to delta path
            }
        };
        if let Some(reason) = needs_full {
            debug!("{reason}, uploading full manifest");
            return self
                .upload_full_manifest(content_store, host_pack_index, seq_cutpoint)
                .await;
        }

        // Take ownership for delta computation. From here, any error must
        // restore the state before propagating.
        let Some(base_state) = self.inner.base_manifest_state.lock().take() else {
            // Shouldn't happen (we just checked), but fall back to full upload.
            return self
                .upload_full_manifest(content_store, host_pack_index, seq_cutpoint)
                .await;
        };

        // Run the fallible delta logic, restoring base_state on error.
        let result = self
            .try_upload_delta(
                content_store,
                host_pack_index,
                seq_cutpoint,
                &base_state,
            )
            .await;

        match result {
            Ok(compacted) => {
                if compacted {
                    // Full manifest was uploaded (size trigger) — upload_full_manifest
                    // already set new base_manifest_state, nothing to restore.
                } else {
                    // Delta uploaded — put back with incremented sync count.
                    *self.inner.base_manifest_state.lock() = Some(BaseManifestState {
                        sequence: base_state.sequence,
                        block_map: base_state.block_map,
                        syncs_since_base: base_state.syncs_since_base + 1,
                    });
                }
                Ok(None)
            }
            Err(e) => {
                // Restore state so next sync can retry delta.
                *self.inner.base_manifest_state.lock() = Some(base_state);
                Err(e)
            }
        }
    }

    /// Inner delta upload logic. Returns Ok(true) if compaction was triggered
    /// (full manifest uploaded instead), Ok(false) if delta was uploaded.
    async fn try_upload_delta(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
        seq_cutpoint: u64,
        base_state: &super::inner::BaseManifestState,
    ) -> Result<bool, CacheError> {
        use crate::nbd::manifest::MANIFEST_BLOCK_ENTRY_SIZE;

        // Snapshot current block map and compute delta.
        let chunk_size = self.inner.config.block_size as u32;
        let post_flush_snap = self.inner.block_map_snapshot();
        let current_entries: Vec<ManifestBlockEntry> = post_flush_snap
            .iter_non_empty()
            .map(|(idx, entry)| ManifestBlockEntry {
                chunk_index: idx as u64,
                hash: entry.hash,
                flags: entry.flags,
            })
            .collect();

        // Compute upserts: entries that differ from base.
        let mut upserted = Vec::new();
        let mut current_map = std::collections::HashMap::new();
        for entry in &current_entries {
            current_map.insert(entry.chunk_index, entry);
            match base_state.block_map.get(&entry.chunk_index) {
                Some(&base_hash) if base_hash == entry.hash => {
                    // Unchanged — skip.
                }
                _ => {
                    // New or changed — upsert.
                    upserted.push(entry.clone());
                }
            }
        }

        // Compute deletes: entries in base but not in current.
        let deleted_chunks: Vec<u64> = base_state
            .block_map
            .keys()
            .filter(|k| !current_map.contains_key(k))
            .copied()
            .collect();

        // Derive pack entries once for the full current state. We need
        // them both for filtering (delta) and for caching (pruning).
        let all_pack_entries = host_pack_index.derive_for_block_map(&post_flush_snap)?;

        // Filter to only upserted hashes for the delta manifest.
        let upserted_hashes: std::collections::HashSet<Blake3Hash> =
            upserted.iter().map(|e| e.hash).collect();
        let new_pack_entries: Vec<_> = all_pack_entries
            .iter()
            .filter(|e| upserted_hashes.contains(&e.hash))
            .cloned()
            .collect();

        // Size-based compaction trigger: delta block changes > 50% of full block map.
        // Compare block-level sizes only (pack entries scale proportionally).
        let delta_block_size = upserted.len() * MANIFEST_BLOCK_ENTRY_SIZE
            + deleted_chunks.len() * 8;
        let full_block_size = current_entries.len() * MANIFEST_BLOCK_ENTRY_SIZE;
        if full_block_size > 0 && delta_block_size * 2 > full_block_size {
            debug!(
                delta_entries = upserted.len(),
                deleted = deleted_chunks.len(),
                full_entries = current_entries.len(),
                "compaction trigger: delta > 50% of full, uploading full manifest"
            );
            self.upload_full_manifest(content_store, host_pack_index, seq_cutpoint)
                .await?;
            return Ok(true); // compacted
        }

        // Build and upload delta.
        let delta = ManifestDelta {
            name: self.inner.export_name.clone(),
            base_sequence: base_state.sequence,
            sequence: seq_cutpoint,
            chunk_size,
            device_size: self.inner.config.device_size,
            upserted,
            deleted_chunks,
            new_pack_entries,
        };

        // Cache the full manifest's pack hashes for pack index pruning.
        *self.inner.manifest_pack_hashes.lock() =
            all_pack_entries.iter().map(|e| e.hash).collect();

        content_store
            .put_delta_manifest(&self.inner.export_name, delta.serialize())
            .await?;

        info!(
            sequence = seq_cutpoint,
            base_sequence = base_state.sequence,
            upserts = delta.upserted.len(),
            deletes = delta.deleted_chunks.len(),
            new_packs = delta.new_pack_entries.len(),
            "uploaded delta manifest"
        );
        Ok(false) // delta, not compacted
    }

    /// Flush dirty blocks to S3 as packs (no manifest upload).
    ///
    /// Returns flush statistics and the sequence cutpoint for a subsequent
    /// `sync_manifest()` call. Used by the flush scheduler to separate
    /// pack flushes (~5s) from manifest syncs (~60s).
    pub async fn flush_packs(
        &self,
        content_store: &ContentStore,
        host_pack_index: &Arc<HostPackIndex>,
    ) -> Result<(FlushStats, u64), CacheError> {
        self.flush_dirty_inner(content_store, host_pack_index).await
    }

    /// Upload a manifest reflecting the current block map state.
    ///
    /// Call after `flush_packs()` with the returned `seq_cutpoint` to persist
    /// a recovery manifest. Uses delta manifests when possible to reduce S3
    /// bandwidth, falling back to full on compaction triggers.
    pub async fn sync_manifest(
        &self,
        content_store: &ContentStore,
        host_pack_index: &Arc<HostPackIndex>,
        seq_cutpoint: u64,
    ) -> Result<(), CacheError> {
        self.upload_delta_or_full_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        self.checkpoint()?;
        Ok(())
    }

    /// Persist block map + block states and truncate WAL.
    ///
    /// Block map is persisted outside the WAL lock (safe: clean blocks
    /// don't change), then the WAL lock is held only for the fast
    /// block-states save + truncate.
    fn checkpoint(&self) -> Result<(), CacheError> {
        self.inner.persist_block_map()?;
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
        // Compute CRC32 for dirty blocks that don't have one yet.
        self.compute_dirty_crc32s();
        self.checkpoint()?;
        debug!("local checkpoint complete");
        Ok(())
    }

    /// Compute CRC32 checksums for dirty blocks that don't have one yet.
    ///
    /// Reads each block from SSD and computes CRC32. Uses a sequence-number
    /// guard to avoid storing stale checksums when a concurrent write changes
    /// the block data during the read.
    fn compute_dirty_crc32s(&self) {
        let block_size = self.inner.config.block_size;
        let device_size = self.inner.config.device_size;
        let mut buf = vec![0u8; block_size];
        let mut computed = 0u64;

        for idx in self.inner.state_map.iter_with_state(SparseBlockState::DIRTY) {
            // Only compute for blocks without a CRC32.
            if self.inner.block_map_get_crc32(idx) != 0 {
                continue;
            }

            // Seq guard: capture sequence before and after reading data.
            let (_, seq_before) = self.inner.block_map_get(idx);

            let offset = idx as u64 * block_size as u64;
            let valid_bytes =
                std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;
            if valid_bytes == 0 {
                continue;
            }

            if let Err(e) = self.inner.data_file.read_exact_at(&mut buf[..valid_bytes], offset) {
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

            let (_, seq_after) = self.inner.block_map_get(idx);
            if seq_before != seq_after {
                // Concurrent write changed this block — skip.
                continue;
            }

            let crc = crc32fast::hash(&buf);
            // CAS: only store if still 0 (write may have cleared it).
            let _ = self.inner.block_map_cas_crc32(idx, 0, crc);
            computed += 1;
        }

        if computed > 0 {
            debug!(computed, "computed CRC32 checksums for dirty blocks");
        }
    }

    /// Update the pack registry after a flush. Best-effort: warn on failure.
    ///
    /// This is off the critical I/O path — the manifest is already uploaded.
    /// If the registry PUT fails, the packs become untracked orphans (harmless).
    pub(crate) async fn update_registry(
        &self,
        content_store: &ContentStore,
        new_pack_ids: &[uuid::Uuid],
    ) {
        if new_pack_ids.is_empty() {
            return;
        }
        let name = &self.inner.export_name;
        let result: Result<(), crate::nbd::content_store::ContentStoreError> = async {
            let mut registry = match content_store.get_registry(name).await? {
                Some(data) => PackRegistry::deserialize(&data).map_err(|e| {
                    object_store::Error::Generic {
                        store: "registry",
                        source: Box::new(e),
                    }
                })?,
                None => PackRegistry::new(),
            };
            registry.append(new_pack_ids);
            content_store
                .put_registry(name, registry.serialize())
                .await?;
            Ok(())
        }
        .await;

        if let Err(e) = result {
            warn!(
                error = %e,
                export = %name,
                packs = new_pack_ids.len(),
                "failed to update pack registry (packs are harmless orphans)"
            );
        }
    }

    /// Flush dirty blocks to S3 as content-addressed packs + manifest.
    ///
    /// This is the v2 flush path:
    /// 1. Scan block_states for dirty blocks (targeted, not full-device snapshot)
    /// 2. Dedup against the host pack index (blocks already in S3)
    /// 3. LZ4-compress new blocks and assemble into 25-block packs
    /// 4. Upload packs to S3 concurrently
    /// 5. Clear dirty flags (with concurrent-write safety)
    /// 6. Upload a self-contained manifest
    /// 7. Update pack registry (best-effort)
    /// 8. Checkpoint: persist block map (outside lock) + block states (under lock) + truncate WAL
    #[instrument(skip(self, content_store, host_pack_index))]
    pub async fn flush_to_s3(
        &self,
        content_store: &ContentStore,
        host_pack_index: &Arc<HostPackIndex>,
    ) -> Result<FlushStats, CacheError> {
        let (stats, seq_cutpoint) = self.flush_dirty_inner(content_store, host_pack_index).await?;
        self.upload_full_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        self.update_registry(content_store, &stats.new_pack_ids).await;
        self.checkpoint()?;
        Ok(stats)
    }

    /// Take a point-in-time snapshot: flush dirty blocks + upload manifest.
    ///
    /// Returns the manifest ETag and sequence number for the control plane
    /// to use when creating a fork.
    #[instrument(skip(self, content_store, host_pack_index))]
    pub async fn snapshot(
        &self,
        content_store: &ContentStore,
        host_pack_index: &Arc<HostPackIndex>,
    ) -> Result<SnapshotResult, CacheError> {
        let (stats, seq_cutpoint) = self.flush_dirty_inner(content_store, host_pack_index).await?;
        let manifest_etag = self.upload_full_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        self.update_registry(content_store, &stats.new_pack_ids).await;
        self.checkpoint()?;
        Ok(SnapshotResult {
            manifest_etag,
            sequence: seq_cutpoint,
            stats,
        })
    }

    /// Check if a forked block map should be flattened, and do so.
    ///
    /// Flatten replaces a ForkedBlockMap (sparse overlay) with a full AtomicBlockMap
    /// when the overlay exceeds 50% of total chunks. Takes a write lock briefly.
    pub(super) fn try_flatten_block_map(&self) {
        let needs_flatten = {
            let bm = self.inner.block_map.read();
            match &*bm {
                BlockMapKind::Forked(f) => f.should_flatten(),
                BlockMapKind::Full(_) => false,
            }
        };
        if needs_flatten {
            let mut bm = self.inner.block_map.write();
            // Re-check under write lock (another thread may have flattened)
            if let BlockMapKind::Forked(f) = &*bm
                && f.should_flatten()
            {
                let full = f.flatten();
                *bm = BlockMapKind::Full(full);
                info!("flattened forked block map");
            }
        }
    }

    /// Get a clone of the inner Arc for sharing with the sync worker.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> Arc<CacheInner> {
        Arc::clone(&self.inner)
    }
}
