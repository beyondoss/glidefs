use std::collections::{BTreeMap, HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tracing::{debug, error, info, instrument, warn};

use crate::block::block_map::{Blake3Hash, SparseBlockState, blake3_128, lz4_compress};
use crate::block::cache::BlockCache;
use crate::block::content_store::{ContentStore, ContentStoreError};
use crate::block::state::{Active, Draining};

use super::inner::{CacheInner, is_zero_block};
use super::{CacheError, FlushStats, SnapshotResult, WriteCache};

/// Result of CPU-heavy flush computation (pread + crc32 + blake3 + lz4).
///
/// Produced by `compute_flush_batch` on a blocking thread, consumed by
/// the async portion of `flush_dirty_inner` for S3 uploads.
struct FlushBatch {
    /// Compressed blocks ready for S3 upload: (hash, compressed_bytes).
    to_upload: Vec<(Blake3Hash, Bytes)>,
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
        compressed: Option<Bytes>,
    },
    /// Block skipped due to CRC mismatch or concurrent write.
    Skipped {
        chunk_index: usize,
        cas_failed: bool,
        crc_mismatched: bool,
    },
}

/// CPU-heavy flush computation: read blocks from SSD, verify CRC32, hash, compress, dedup.
///
/// Runs on a blocking thread via `spawn_blocking` to avoid starving the async runtime.
/// Phase 1 (rayon parallel): pread + crc32 + blake3 + lz4 per block.
/// Phase 2 (sequential): within-batch dedup via seen_hashes (cheap hash-set insertions).
///
/// All blocks in `snapshot` have already been claimed (CAS DIRTY→SYNCING).
/// CRC verification uses the ephemeral `crcs` HashMap (computed pre-rotation)
/// and state discrimination:
/// - CRC mismatch + state == SYNCING → SSD corruption or stale CRC from write
///   between pre-pass and rotation (block retries next cycle)
/// - CRC mismatch + state != SYNCING → concurrent write re-dirtied the block
fn compute_flush_batch(
    inner: &CacheInner,
    snapshot: &[usize],
    zero_hash: Blake3Hash,
    clean_cache: Option<Arc<dyn BlockCache>>,
    crcs: &HashMap<usize, Box<[u32]>>,
) -> Result<FlushBatch, CacheError> {
    use rayon::prelude::*;

    let block_size = inner.config.block_size;
    let device_size = inner.config.device_size;

    // Snap the flushing file Arc before entering rayon so workers share it
    // lock-free. Without this, every rayon thread serializes on the Mutex
    // for each pread, defeating parallelism.
    //
    // flush_dirty_inner always rotates before calling us, so flushing_file
    // is always Some here. Unwrap is safe.
    let flushing_file: Arc<super::inner::SyncFile> = inner
        .flushing_file
        .lock()
        .as_ref()
        .ok_or_else(|| CacheError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "compute_flush_batch: no flushing file (rotation not performed)",
        )))?
        .clone();

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
                    flushing_file.read_exact_at(&mut chunk_buf[..valid_bytes], offset)?;
                }
                if valid_bytes < block_size {
                    chunk_buf[valid_bytes..].fill(0);
                }

                // Verify per-page CRC32C against write-time baselines.
                if let Some(page_crcs) = crcs.get(&chunk_index) {
                    const PAGE_SIZE: usize = 4096;
                    let mut any_mismatch = false;
                    for (page, &stored_crc) in page_crcs.iter().enumerate() {
                        if stored_crc == 0 {
                            continue;
                        }
                        let page_start = page * PAGE_SIZE;
                        let page_end = (page_start + PAGE_SIZE).min(chunk_buf.len());
                        let computed_crc = crc_fast::crc32_iscsi(&chunk_buf[page_start..page_end]);
                        if computed_crc != stored_crc {
                            any_mismatch = true;
                            break;
                        }
                    }
                    if any_mismatch {
                        let current_state = inner.state_map.get(chunk_index);
                        if current_state != SparseBlockState::SYNCING {
                            return Ok(BlockResult::Skipped {
                                chunk_index,
                                cas_failed: true,
                                crc_mismatched: true,
                            });
                        }
                        debug!(
                            chunk_index,
                            "page CRC mismatch — block will retry next flush cycle"
                        );
                        return Ok(BlockResult::Skipped {
                            chunk_index,
                            cas_failed: false,
                            crc_mismatched: true,
                        });
                    }
                }

                // Fast zero-block detection (AVX2 on x86_64) before BLAKE3.
                if is_zero_block(&chunk_buf) {
                    return Ok(BlockResult::Computed {
                        chunk_index,
                        hash: zero_hash,
                        compressed: None,
                    });
                }

                let hash = blake3_128(&chunk_buf);

                let compressed = Some(Bytes::from(lz4_compress(&chunk_buf[..])));

                // Warm clean_cache
                if let Some(ref cache) = clean_cache {
                    cache.insert(hash, Bytes::from(chunk_buf));
                }

                Ok(BlockResult::Computed {
                    chunk_index,
                    hash,
                    compressed,
                })
        })
        .collect();

    // Phase 2: sequential aggregation — within-batch dedup + stats.
    let mut stats = FlushStats::default();
    let mut to_upload: Vec<(Blake3Hash, Bytes)> = Vec::new();
    let mut seen_hashes = HashSet::new();
    let mut computed: Vec<(usize, Blake3Hash)> = Vec::new();
    let mut skipped: Vec<usize> = Vec::new();

    for result in per_block {
        let result = result?;
        match result {
            BlockResult::Skipped {
                chunk_index,
                cas_failed,
                crc_mismatched,
            } => {
                stats.blocks_claimed += 1;
                skipped.push(chunk_index);
                if cas_failed {
                    stats.blocks_cas_failed += 1;
                }
                if crc_mismatched {
                    stats.blocks_crc_mismatched += 1;
                }
            }
            BlockResult::Computed {
                chunk_index,
                hash,
                compressed,
            } => {
                stats.blocks_claimed += 1;
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

/// Drain per-page CRC32C baselines captured at pwrite time.
///
/// Drain per-page CRC32C baselines captured at pwrite time.
///
/// Removes all entries from the DashMap, returning them for verification.
/// The DashMap shrinks back to empty after each flush cycle.
fn drain_page_crcs(inner: &CacheInner) -> HashMap<usize, Box<[u32]>> {
    let mut map = HashMap::with_capacity(inner.page_crcs.len());
    // retain() with always-false predicate: removes every entry, giving
    // us ownership of the values. Locks each shard once.
    inner.page_crcs.retain(|key, value| {
        map.insert(*key as usize, value.clone());
        false // remove
    });
    map
}

impl WriteCache<Active> {
    /// Flush the local cache file and WAL to durable storage.
    ///
    /// Syncs both the data file and the WAL so that all dirty block metadata
    /// survives a crash. Without the WAL sync, blocks written since the last
    /// checkpoint would have data on SSD but no state entry — recovery would
    /// see them as NOT_PRESENT and lose data.
    ///
    /// This is fast (<10ms). It does NOT wait for S3 sync — that happens in
    /// the background.
    #[instrument(skip(self))]
    pub fn flush(&self) -> Result<(), CacheError> {
        self.inner.data_file.read().sync_all()?;
        self.inner.wal.sync()?;
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

    /// Number of CRC entries pending drain. Indicates queue memory pressure —
    /// grows with write throughput between flush cycles, drains to zero on flush.
    pub fn pending_crc_count(&self) -> usize {
        self.inner.page_crcs.len()
    }

    /// Number of recovery issues encountered during cache open.
    pub fn recovery_warning_count(&self) -> u64 {
        self.inner.recovery_warnings.load(Ordering::Relaxed)
    }

    /// Current WAL sequence number. Read by the handoff predecessor to
    /// populate the `last_wal_seq` field of `ExportSnapshot`; the
    /// successor uses it to sanity-check that its WAL replay reached at
    /// least this point.
    pub fn last_persisted_seq(&self) -> u64 {
        self.inner.sequence.current()
    }

    /// Toggle the "freeze in progress" flag. While set,
    /// [`Self::checkpoint`] skips the WAL truncate step (the metadata
    /// save still runs). Used by the handoff predecessor to keep the
    /// WAL intact across the successor's WARMING → CUTOVER window.
    pub fn set_freeze_in_progress(&self, frozen: bool) {
        self.inner
            .freeze_in_progress
            .store(frozen, std::sync::atomic::Ordering::Release);
    }

    /// Replay any WAL entries newer than the current sequence number and
    /// apply them to the in-memory state map.
    ///
    /// **Critical for graceful handoff correctness.** The successor's
    /// `WriteCache::open` runs during WARMING, before the predecessor's
    /// `freeze_all()`. Writes that arrive at the predecessor between
    /// WARMING and FREEZE get fsync'd to the WAL on disk but the
    /// successor's already-open state map doesn't know about them. After
    /// the predecessor's PREDS_DEAD message, the successor must call
    /// this to pick up the tail of new WAL entries.
    ///
    /// Returns the number of entries replayed.
    pub fn replay_wal_tail(&self) -> Result<usize, super::CacheError> {
        use crate::block::block_map::SparseBlockState;
        use crate::block::wal::Wal;

        let min_seq = self.inner.sequence.current();
        let wal_path = self.inner.config.wal_path();
        let entries = Wal::replay(&wal_path, min_seq).map_err(|e| {
            super::CacheError::Io(std::io::Error::other(format!(
                "WAL tail replay failed: {e}"
            )))
        })?;

        if entries.is_empty() {
            return Ok(0);
        }

        let num_blocks = self.inner.num_blocks;
        let mut max_seq = min_seq;
        for entry in &entries {
            let idx = entry.block_index as usize;
            max_seq = max_seq.max(entry.sequence);
            if idx >= num_blocks {
                continue;
            }
            let old = self.inner.state_map.get(idx);
            if old != SparseBlockState::DIRTY {
                self.inner.state_map.set_present(idx);
                let current = self.inner.state_map.get(idx);
                if current != SparseBlockState::DIRTY {
                    if self
                        .inner
                        .state_map
                        .cas(idx, current, SparseBlockState::DIRTY)
                        .is_ok()
                    {
                        self.inner
                            .dirty_block_count
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        // Bump the sequence counter past the new max so future writes
        // get monotonically-increasing sequence numbers.
        if max_seq > self.inner.sequence.current() {
            self.inner.sequence.advance_to(max_seq);
        }

        tracing::info!(
            replayed = entries.len(),
            min_seq,
            max_seq,
            "WriteCache: tail-replayed WAL entries after handoff"
        );

        Ok(entries.len())
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

    /// Check if a block is present on local SSD.
    #[inline]
    pub fn is_block_present(&self, block_idx: usize) -> bool {
        self.inner.is_present(block_idx)
    }

    /// Try to claim a NOT_PRESENT block (CAS NOT_PRESENT → CLEAN).
    /// Returns true if this call won the transition, false if already present.
    #[inline]
    pub fn try_claim_block(&self, block_idx: usize) -> bool {
        self.inner.try_set_present(block_idx)
    }

    /// Get the block state as a typed value.
    #[inline]
    pub fn block_state(&self, block_idx: usize) -> crate::block::block_map::BlockState {
        crate::block::block_map::BlockState::from_raw(self.inner.state_map.get(block_idx))
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
    ///
    /// Runs on a blocking thread via `spawn_blocking` to avoid blocking
    /// the Tokio runtime with `sync_all()` + `rename()` syscalls.
    ///
    /// Independent of S3 — keeps the WAL bounded in demand-driven mode
    /// where S3 flushes may be infrequent. Should run every ~5s when
    /// there are dirty blocks.
    pub async fn checkpoint(&self) -> Result<(), CacheError> {
        let inner = Arc::clone(&self.inner);
        crate::task::spawn_blocking_named("checkpoint", move || {
            // Always save the block-state metadata file — it's the
            // canonical record of what's PRESENT/DIRTY/SYNCING.
            inner.save_block_states()?;

            // **Skip WAL truncate during the handoff freeze window.**
            // The successor's `replay_wal_tail` reads the same WAL the
            // predecessor was appending into. If we truncate here while
            // the successor is in WARMING/CUTOVER, any entry the
            // predecessor has acked since the successor's WARMING-time
            // open vanishes from the WAL — the metadata file captures
            // the state, but the successor's already-loaded state_map
            // doesn't get re-loaded. Without the truncate, those
            // entries remain in the WAL and the successor's tail-replay
            // picks them up.
            //
            // The freeze window is bounded (~hundreds of ms). The WAL
            // grows briefly. After the predecessor exits, the successor
            // starts fresh and resumes normal checkpoint behavior on
            // its own WriteCache.
            if inner
                .freeze_in_progress
                .load(std::sync::atomic::Ordering::Acquire)
            {
                debug!("checkpoint: skipping WAL truncate (freeze in progress)");
                return Ok(());
            }

            inner.wal.truncate()?;
            debug!("checkpoint complete");
            Ok(())
        })
        .await
        .map_err(|e| CacheError::Io(std::io::Error::other(e)))?
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

    /// Test-only: fire a flush gate if sync points are attached.
    #[cfg(feature = "test-utils")]
    async fn flush_gate(&self, step: super::inner::FlushStep, count: usize) {
        let sp = self.inner.flush_sync.lock().clone();
        if let Some(sp) = sp {
            sp.gate(step, count).await;
        }
    }

    /// Test-only: rotate without flushing. Leaves a flushing file on disk,
    /// which blocks flush_dirty_inner until checkpoint() deletes it.
    #[cfg(feature = "test-utils")]
    pub fn test_rotate_and_snapshot(&self) -> Result<Vec<usize>, CacheError> {
        self.rotate_and_snapshot()
    }

    /// Test-only: expose inner for direct state manipulation in tests.
    #[cfg(feature = "test-utils")]
    pub fn test_inner(&self) -> Arc<CacheInner> {
        self.inner()
    }

    /// Test-only: try CAS SYNCING→NOT_PRESENT for a specific block.
    #[cfg(feature = "test-utils")]
    pub fn transition_syncing_to_not_present(&self, block_idx: usize) -> bool {
        self.inner.transition_syncing_to_not_present(block_idx)
    }

    /// Test-only: attach flush sync points.
    #[cfg(feature = "test-utils")]
    pub fn set_flush_sync(&self, sp: std::sync::Arc<super::inner::FlushSyncPoints>) {
        *self.inner.flush_sync.lock() = Some(sp);
    }

    /// Test-only: attach promote sync points.
    #[cfg(feature = "test-utils")]
    pub fn set_promote_sync(&self, sp: std::sync::Arc<super::inner::PromoteSyncPoints>) {
        *self.inner.promote_sync.lock() = Some(sp);
    }

    /// Test-only: attach read sync points.
    #[cfg(feature = "test-utils")]
    pub fn set_read_sync(&self, sp: std::sync::Arc<super::inner::ReadSyncPoints>) {
        *self.inner.read_sync.lock() = Some(sp);
    }

    /// Rotate the data file for flush.
    ///
    /// 1. Rename active file -> flushing file
    /// 2. Create new sparse active file
    /// 3. Swap data_file RwLock (write-locked briefly)
    /// 4. Store old handle in flushing_file Mutex
    #[cfg_attr(feature = "test-utils", allow(dead_code))]
    pub(crate) fn rotate_data_file(&self) -> Result<(), CacheError> {
        let (_, ()) = self.rotate_data_file_inner(false)?;
        Ok(())
    }

    /// Snapshot dirty block indices AND rotate the data file atomically.
    ///
    /// Holds the data_file write lock across both operations so no concurrent
    /// writes can land between the snapshot and the file swap. This prevents
    /// a race where a block becomes dirty (with data in the new active file)
    /// after rotation but is picked up by the CAS scan — compute_flush_batch
    /// would read zeros from the flushing file for that block.
    #[cfg_attr(feature = "test-utils", allow(dead_code))]
    pub(crate) fn rotate_and_snapshot(&self) -> Result<Vec<usize>, CacheError> {
        let (claimed, ()) = self.rotate_data_file_inner(true)?;
        Ok(claimed)
    }

    /// Core rotation logic. If `snapshot` is true, captures dirty block
    /// indices under the write lock before swapping files.
    ///
    /// The critical section under the write lock covers: rename, new file
    /// creation, CAS DIRTY→SYNCING, handle swap, and flushing_file
    /// assignment. Dir fsync runs after the lock is released — it only
    /// makes the rename durable across power loss and does not affect
    /// correctness of concurrent I/O. If a crash occurs before dir fsync,
    /// the rename may be lost, but crash recovery handles the resulting
    /// state (no flushing file → SYNCING blocks converted to DIRTY).
    ///
    /// All DIRTY blocks are claimed (CAS DIRTY→SYNCING).
    fn rotate_data_file_inner(&self, snapshot: bool) -> Result<(Vec<usize>, ()), CacheError> {
        let active_path = self.inner.config.data_path();
        let flushing_path = self.inner.config.flushing_path();

        // === Single write lock: everything below is atomic w.r.t. I/O ===
        let mut data_file_guard = self.inner.data_file.write();

        // Capture rotation_seq under the write lock: all writes with
        // sequence <= this value have completed their pwrite to the current
        // active file (which is about to become the flushing file). Used by
        // crash recovery to distinguish pre- vs post-rotation WAL entries.
        let rotation_seq = self.inner.sequence.current();
        self.inner
            .rotation_seq
            .store(rotation_seq, Ordering::Release);

        // Signal flush rotation in progress. Under the lock so no reader
        // can observe flushing_active=true with stale file state.
        self.inner.flushing_active.store(true, Ordering::Release);

        // Rename active → flushing. The file handle in data_file_guard
        // still refers to the same inode (now at flushing_path).
        std::fs::rename(&active_path, &flushing_path)?;

        // Create new sparse active file. If this fails (e.g. inode
        // exhaustion), undo the rename and reset flags so future flush
        // cycles aren't permanently blocked.
        let new_file = match super::inner::SyncFile::open(
            &active_path,
            true,
            self.inner.config.device_size,
        ) {
            Ok(f) => f,
            Err(e) => {
                // Undo: rename flushing back to active. The data_file_guard
                // still holds the old fd (same inode), so I/O continues
                // against whatever path the inode lives at. Restoring the
                // original path keeps the on-disk state consistent.
                if let Err(undo_err) = std::fs::rename(&flushing_path, &active_path) {
                    // Both the new-active creation and the rollback rename failed.
                    // active_path no longer exists on disk; the export is in an
                    // unrecoverable state until it is removed and re-added.
                    error!(
                        original_error = %e,
                        undo_error = %undo_err,
                        "FATAL: rotation rollback rename failed — active cache file is gone, export must be removed and re-added"
                    );
                    self.inner.flushing_active.store(false, Ordering::Release);
                    self.inner.rotation_seq.store(0, Ordering::Release);
                    return Err(CacheError::Io(undo_err));
                }
                self.inner.flushing_active.store(false, Ordering::Release);
                self.inner.rotation_seq.store(0, Ordering::Release);
                return Err(CacheError::Io(e));
            }
        };

        // Snapshot dirty blocks: CAS DIRTY→SYNCING under the lock.
        let claimed = if snapshot {
            let mut claimed = Vec::new();
            for idx in self.inner.state_map.iter_with_state(SparseBlockState::DIRTY) {
                if self.inner.transition_dirty_to_syncing(idx) {
                    claimed.push(idx);
                }
            }
            claimed
        } else {
            Vec::new()
        };

        // Swap file handle: new active file goes into the RwLock.
        let old_file = std::mem::replace(&mut *data_file_guard, new_file);

        // No block data copy needed. The flushing file preserves block data.

        // Set flushing_file before releasing the write lock.
        // Writers acquiring the read lock after rotation will see
        // flushing_active=true AND flushing_file=Some, so
        // promote_syncing_blocks works correctly.
        *self.inner.flushing_file.lock() = Some(Arc::new(old_file));

        drop(data_file_guard);
        // === Write lock released — concurrent I/O resumes ===

        // Fsync parent directory so the rename is durable across power loss.
        // Runs outside the write lock: dir fsync can take 1-5ms on SATA SSDs
        // and we don't need to block concurrent reads/writes for it. If we
        // crash before this completes, the rename may be lost — recovery
        // finds no flushing file and converts SYNCING→DIRTY (safe).
        if let Some(parent) = active_path.parent() {
            match std::fs::File::open(parent) {
                Ok(dir) => {
                    if let Err(e) = dir.sync_all() {
                        warn!(
                            error = %e,
                            "dir fsync after rotation failed — durability weakened"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "failed to open parent dir for fsync after rotation — durability weakened"
                    );
                }
            }
        }

        info!("rotated data file for flush");
        Ok((claimed, ()))
    }

    /// Chunked flush: claim dirty blocks via CAS DIRTY→SYNCING, partition by
    /// chunk (128 MiB), per-chunk dedup/compress/upload as GLPK v3 packs,
    /// append pack_id to VolumeManifest, CAS SYNCING→NOT_PRESENT.
    ///
    /// Returns (stats, seq_cutpoint) on success.
    async fn flush_dirty_inner(
        &self,
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
        clean_cache: Option<&Arc<dyn BlockCache>>,
    ) -> Result<(FlushStats, u64), CacheError> {
        let seq_cutpoint = self.inner.sequence.current();

        // Guard: if a flushing file exists on disk from a previous flush whose
        // manifest hasn't been synced + checkpointed yet, skip this flush cycle.
        // Rotation would rename active → flushing, overwriting the old flushing
        // file and destroying the crash-safety net for the previous flush's
        // blocks. The scheduler retries manifest sync on the checkpoint timer;
        // once that succeeds and checkpoint deletes the flushing file, the next
        // flush cycle can proceed.
        //
        // Callers receive FlushStats::default() (packs_uploaded=0) — this is
        // indistinguishable from "no dirty blocks existed". The flush scheduler
        // handles this via retry; direct callers of flush_to_s3 must not assume
        // packs_uploaded>0 implies all dirty blocks were flushed.
        if self.inner.config.flushing_path().exists() {
            // The flushing file might be orphaned from a previous cleanup
            // failure (e.g., empty flush or error recovery where remove_file
            // failed). If no rotation is in progress (flushing_active=false,
            // flushing_file=None), the file is stale — try to delete it so
            // this flush cycle can proceed.
            if !self.inner.flushing_active.load(Ordering::Acquire)
                && self.inner.flushing_file.lock().is_none()
            {
                let flushing_path = self.inner.config.flushing_path();
                match std::fs::remove_file(&flushing_path) {
                    Ok(()) => {
                        info!("removed orphaned flushing file");
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "orphaned flushing file exists but cannot be removed — flush blocked"
                        );
                        return Ok((FlushStats::default(), seq_cutpoint));
                    }
                }
            } else {
                debug!("flush: flushing file still exists (pending manifest sync), skipping");
                return Ok((FlushStats::default(), seq_cutpoint));
            }
        }

        // Drain write-time CRC baselines BEFORE rotation.
        // These were captured from guest write buffers at pwrite time.
        let crcs = drain_page_crcs(&self.inner);

        #[cfg(feature = "test-utils")]
        self.flush_gate(super::inner::FlushStep::AfterCrcPrepass, crcs.len()).await;

        // Snapshot dirty blocks, claim (CAS DIRTY→SYNCING), AND rotate —
        // all under the data_file write lock.
        //
        // The write lock blocks concurrent pwrite/pread, giving us a clean
        // cut: every block in `snapshot` has its data in the current active
        // file (now the flushing file) AND is already SYNCING.
        //
        // The CAS DIRTY→SYNCING must happen under the write lock to prevent
        // a race where a concurrent write between lock-release and CAS lands
        // data in the new active file while the block is still DIRTY. Without
        // this, the CAS would claim the block, but the flushing file would
        // have stale data (missing the concurrent write). By claiming under
        // the lock, concurrent writes after lock-release see SYNCING and
        // CAS SYNCING→DIRTY in transition_to_dirty, properly re-dirtying
        // the block. promote_syncing_blocks then copies data from
        // flushing→active so the new active file has complete block data.
        let snapshot = self.rotate_and_snapshot()?;

        #[cfg(feature = "test-utils")]
        self.flush_gate(super::inner::FlushStep::AfterRotation, snapshot.len()).await;

        if snapshot.is_empty() {
            debug!("flush: no dirty blocks to flush");
            // Clean up the empty flushing file (rotated but no dirty blocks)
            self.inner.flushing_active.store(false, Ordering::Release);
            self.inner.rotation_seq.store(0, Ordering::Release);
            drop(self.inner.flushing_file.lock().take());
            let flushing_path = self.inner.config.flushing_path();
            match std::fs::remove_file(&flushing_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => warn!(error = %e, "failed to remove empty flushing file"),
            }
            return Ok((FlushStats::default(), seq_cutpoint));
        }

        info!(dirty_blocks = snapshot.len(), seq_cutpoint, "starting flush");

        let result = self
            .flush_dirty_body(&snapshot, content_store, pack_index_cache, volume_manifest, clean_cache, crcs)
            .await;

        if result.is_err() {
            // Copy block data from flushing to active BEFORE transitioning
            // state. If we transition SYNCING→DIRTY first, a concurrent read
            // sees DIRTY, reads the new active file (which is empty for these
            // blocks), and serves zeros.
            //
            // Write-lock data_file to prevent concurrent writes from
            // interleaving with the copy. Without this, a write could:
            // 1. CAS SYNCING→DIRTY, promote, pwrite new data
            // 2. Then our copy overwrites that new data with stale flushing data
            // Lock ordering: data_file.write() THEN flushing_file.lock().
            // Must match sync_read_local_block (data_file.read() → flushing_file)
            // to avoid deadlock.
            let block_size = self.inner.config.block_size;
            let df = self.inner.data_file.write();
            let mut recovered = std::collections::HashSet::with_capacity(snapshot.len());
            if let Some(ref ff) = *self.inner.flushing_file.lock() {
                for &idx in &snapshot {
                    if self.inner.state_map.get(idx) != SparseBlockState::SYNCING {
                        // Already re-dirtied by a concurrent write — data is
                        // in the active file. Safe to transition.
                        recovered.insert(idx);
                        continue;
                    }
                    let offset = idx as u64 * block_size as u64;
                    let valid_bytes = std::cmp::min(
                        block_size as u64,
                        self.inner.config.device_size.saturating_sub(offset),
                    ) as usize;
                    if valid_bytes > 0 {
                        let mut buf = vec![0u8; valid_bytes];
                        match ff.read_exact_at(&mut buf, offset) {
                            Ok(()) => {
                                if let Err(e) = df.write_all_at(&buf, offset) {
                                    // Cannot copy block to active file. Leave it
                                    // SYNCING — crash recovery will handle it.
                                    // Do NOT mark dirty: the active file has zeros.
                                    warn!(
                                        block = idx,
                                        error = %e,
                                        "pwrite to active file failed during flush \
                                         recovery — block left SYNCING"
                                    );
                                    continue;
                                }
                                recovered.insert(idx);
                            }
                            Err(e) => {
                                // Cannot read block from flushing file. Leave it
                                // SYNCING — crash recovery will handle it.
                                // Do NOT mark dirty: the active file has zeros.
                                warn!(
                                    block = idx,
                                    error = %e,
                                    "pread from flushing file failed during flush \
                                     recovery — block left SYNCING"
                                );
                                continue;
                            }
                        }
                    } else {
                        recovered.insert(idx);
                    }
                }
            } else {
                // No flushing file — blocks not in SYNCING were already
                // handled by concurrent writes. Only transition those.
                for &idx in &snapshot {
                    if self.inner.state_map.get(idx) != SparseBlockState::SYNCING {
                        recovered.insert(idx);
                    }
                }
            }
            // Check if any blocks are still stranded in SYNCING (recovery
            // failed for them — both the S3 upload and the SSD copy failed).
            let stranded_syncing = snapshot
                .iter()
                .any(|&idx| {
                    !recovered.contains(&idx)
                        && self.inner.state_map.get(idx) == SparseBlockState::SYNCING
                });

            drop(df);
            self.inner.flushing_active.store(false, Ordering::Release);
            self.inner.rotation_seq.store(0, Ordering::Release);

            if stranded_syncing {
                // Stranded SYNCING blocks: both the S3 upload failed AND the
                // SSD copy (flushing→active) failed. The data exists only in
                // the flushing file's open fd, but the SSD can't read it
                // reliably (that's why recovery failed). Keeping the flushing
                // file alive would permanently block all future flushes —
                // every subsequent flush_dirty_inner call sees the flushing
                // file on disk + flushing_file=Some and returns early.
                //
                // Accept data loss for the unrecoverable blocks: transition
                // them to NOT_PRESENT so the read path fetches from S3 (if a
                // previous flush succeeded) or returns zeros (if not). This
                // is strictly better than losing ALL future flushes for every
                // block on this export.
                let mut stranded_count = 0u64;
                for &idx in &snapshot {
                    if !recovered.contains(&idx)
                        && self.inner.state_map.get(idx) == SparseBlockState::SYNCING
                    {
                        self.inner.transition_syncing_to_not_present(idx);
                        stranded_count += 1;
                    }
                }
                tracing::error!(
                    stranded_blocks = stranded_count,
                    "DATA LOSS: {stranded_count} blocks could not be recovered \
                     after flush failure (S3 upload failed + SSD copy failed). \
                     Blocks marked NOT_PRESENT to unblock future flushes."
                );
            }
            // Drop the flushing file and delete the physical file so future
            // flush cycles can rotate normally. For the stranded case, the
            // unrecoverable blocks have already been marked NOT_PRESENT above.
            drop(self.inner.flushing_file.lock().take());
            let flushing_path = self.inner.config.flushing_path();
            if flushing_path.exists() {
                let _ = std::fs::remove_file(&flushing_path);
            }
            // Only transition blocks whose data was successfully recovered.
            for &idx in &recovered {
                self.inner.transition_to_dirty(idx);
            }
        }

        result.map(|stats| (stats, seq_cutpoint))
    }

    /// Inner body of flush_dirty_inner, factored out for error recovery.
    ///
    /// Per-chunk: warm PackIndexCache, compute_flush_batch (rayon: pread +
    /// CRC32 + BLAKE3 + LZ4), assemble GLPK v3 pack, upload to S3, update
    /// PackIndexCache. Manifest appends are staged and applied atomically
    /// after all chunk uploads succeed. No .meta upload.
    async fn flush_dirty_body(
        &self,
        snapshot: &[usize],
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
        clean_cache: Option<&Arc<dyn BlockCache>>,
        crcs: HashMap<usize, Box<[u32]>>,
    ) -> Result<FlushStats, CacheError> {
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
        // extra S3 round-trip per pack. Fetches are parallelized to avoid
        // sequential S3 latency on cold start (O(chunks × packs) calls).
        {
            use futures::stream::{self, StreamExt};

            let packs_to_warm: Vec<(u32, crate::block::pack::PackId)> = {
                let vm = volume_manifest.read();
                per_chunk
                    .keys()
                    .flat_map(|&chunk_idx| {
                        vm.chunk_pack_ids(chunk_idx)
                            .map(|ids| ids.to_vec())
                            .unwrap_or_default()
                            .into_iter()
                            .map(move |pid| (chunk_idx, pid))
                    })
                    .collect()
            };

            stream::iter(packs_to_warm)
                .for_each_concurrent(16, |(chunk_idx, pid)| async move {
                    if pack_index_cache.get_entries(pid).await.is_some() {
                        return;
                    }
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
                })
                .await;
        }

        let blocks_per_chunk = {
            let vm = volume_manifest.read();
            vm.blocks_per_chunk()
        };

        let mut total_stats = FlushStats::default();
        let mut flushed_blocks: Vec<usize> = Vec::new();
        let mut staged_appends: Vec<(u32, crate::block::pack::PackId)> = Vec::new();
        let crcs = Arc::new(crcs);

        // Pipelined flush: compute batches sequentially (needs flushing file),
        // upload packs concurrently via FuturesUnordered. Each chunk's upload
        // starts as soon as its compute finishes — overlapping S3 I/O with
        // the next chunk's compute. Memory bounded to in-flight uploads only.
        use futures::stream::{FuturesUnordered, StreamExt};
        use futures::FutureExt;

        type UploadResult = Result<
            (u32, crate::block::pack::PackId, Vec<crate::block::pack::PackIndexEntry>),
            CacheError,
        >;
        let mut in_flight: FuturesUnordered<std::pin::Pin<Box<dyn std::future::Future<Output = UploadResult> + Send + '_>>> =
            FuturesUnordered::new();

        for (chunk_idx, chunk_blocks) in per_chunk {
            // Compute batch (pread + CRC32 + BLAKE3 + LZ4)
            let zero_hash = self.inner.zero_block_hash;
            let inner = Arc::clone(&self.inner);
            let clean_cache_clone = clean_cache.map(Arc::clone);
            let crcs = Arc::clone(&crcs);
            let mut batch = crate::task::spawn_blocking_named("flush-compute", move || {
                compute_flush_batch(&inner, &chunk_blocks, zero_hash, clean_cache_clone, &crcs)
            })
            .await
            .map_err(|e| CacheError::Io(std::io::Error::other(e)))??;

            total_stats.blocks_claimed += batch.stats.blocks_claimed;
            total_stats.blocks_deduped += batch.stats.blocks_deduped;
            total_stats.blocks_cas_failed += batch.stats.blocks_cas_failed;
            total_stats.blocks_crc_mismatched += batch.stats.blocks_crc_mismatched;

            // Transition skipped blocks back to DIRTY.
            //
            // Blocks still SYNCING (CRC-mismatched, partial-not-re-dirtied)
            // need data promoted from flushing → active before transition,
            // since no guest write triggered promote-on-write for them.
            // Blocks already DIRTY (re-dirtied by guest write) were promoted
            // by the write path — don't overwrite active with stale data.
            {
                // Lock ordering: data_file.write() THEN flushing_file.lock()
                // (same order as sync_read_local_block to avoid deadlock).
                let df = self.inner.data_file.write();
                let flushing_guard = self.inner.flushing_file.lock();
                let block_size = self.inner.config.block_size;
                for &idx in &batch.skipped {
                    if self.inner.state_map.get(idx) == SparseBlockState::SYNCING
                        && let Some(ref ff) = *flushing_guard
                    {
                        let offset = idx as u64 * block_size as u64;
                        let valid = std::cmp::min(
                            block_size as u64,
                            self.inner.config.device_size.saturating_sub(offset),
                        ) as usize;
                        if valid > 0 {
                            let mut buf = vec![0u8; valid];
                            if ff.read_exact_at(&mut buf, offset).is_ok() {
                                if let Err(e) = df.write_all_at(&buf, offset) {
                                    warn!(
                                        block = idx,
                                        error = %e,
                                        "pwrite to active file failed during \
                                         skipped-block recovery — block left SYNCING"
                                    );
                                    continue;
                                }
                            } else {
                                // Cannot read from flushing file. Leave SYNCING
                                // so crash recovery can attempt it. Do NOT mark
                                // dirty: the active file has zeros for this block.
                                warn!(
                                    block = idx,
                                    "pread from flushing file failed during \
                                     skipped-block recovery — block left SYNCING"
                                );
                                continue;
                            }
                        }
                    }
                    self.inner.transition_to_dirty(idx);
                }
            }

            // Build hash → compressed data map from the unique-hash upload set.
            // Within-batch dedup (seen_hashes in compute_flush_batch) ensures one
            // copy of compressed data per unique hash.
            let hash_to_compressed: HashMap<Blake3Hash, Bytes> =
                std::mem::take(&mut batch.to_upload).into_iter().collect();

            // Cross-flush dedup: build merged view of existing blocks for this
            // chunk. Pack indices are already warmed (above), so get_entries
            // hits the in-memory cache (~100ns per pack). Returns None if any
            // pack index is missing — dedup is unsafe without the full picture.
            let existing_hashes: Option<std::collections::HashMap<u32, Blake3Hash>> = {
                let pack_ids = volume_manifest.read()
                    .chunk_pack_ids(chunk_idx)
                    .map(|ids| ids.to_vec())
                    .unwrap_or_default();
                pack_index_cache.existing_block_hashes(&pack_ids).await
            };

            // Build one pack entry per dirty block, each at its actual
            // chunk_offset. Two blocks with the same hash but different chunk_offsets
            // both get entries (clone of compressed bytes), ensuring both are
            // resolvable by the read path.
            //
            // Zero blocks get a tombstone entry with empty compressed data
            // (comp_length = 0 in the pack index). This ensures the "newest wins"
            // semantic is preserved across forks/migrations: without a tombstone,
            // a block overwritten with zeros would resolve to its previous non-zero
            // pack entry on a fork (where the local SSD is empty).
            //
            // Cross-flush dedup: skip blocks whose (chunk_offset, hash) already
            // exists in a prior pack. The read path will find the existing entry.
            let zero_hash = self.inner.zero_block_hash;
            let mut blocks_for_pack: Vec<(Blake3Hash, u32, Bytes)> = Vec::new();
            let mut packed_indices: Vec<usize> = Vec::new();
            {
                let vm = volume_manifest.read();
                for &(block_index, hash) in &batch.computed {
                    let chunk_offset = vm.block_offset_in_chunk(block_index as u64);

                    // Cross-flush dedup: skip if existing pack has same
                    // content at this offset, OR if the block is zero and
                    // no prior entry exists (reads return zeros by default).
                    // Only safe when ALL pack indices are cached (Some map).
                    if let Some(ref existing) = existing_hashes {
                        let dominated = match existing.get(&chunk_offset) {
                            Some(&existing_hash) => existing_hash == hash,
                            None => hash == zero_hash,
                        };
                        if dominated {
                            total_stats.blocks_cross_deduped += 1;
                            // Add to packed_indices so the block gets evicted
                            // SYNCING→NP at the same time as uploaded blocks
                            // (after all uploads complete). Same crash-safety
                            // as uploaded blocks: flushing file is the safety
                            // net until checkpoint.
                            packed_indices.push(block_index);
                            continue;
                        }
                    }

                    let compressed = if hash == zero_hash {
                        Bytes::new() // zero block tombstone: comp_length = 0
                    } else {
                        match hash_to_compressed.get(&hash) {
                            Some(data) => data.clone(),
                            None => {
                                // Invariant violation: every non-zero hash in
                                // computed must have its first occurrence in
                                // to_upload (and thus hash_to_compressed).
                                // Treat as skipped to prevent silent data loss.
                                warn!(
                                    block_index,
                                    "BUG: non-zero hash missing from hash_to_compressed, \
                                     treating as skipped to prevent data loss"
                                );
                                self.inner.transition_to_dirty(block_index);
                                continue;
                            }
                        }
                    };
                    blocks_for_pack.push((hash, chunk_offset, compressed));
                    packed_indices.push(block_index);
                }
            }

            // All computed blocks (including cross-deduped) need SYNCING→NP.
            flushed_blocks.extend(&packed_indices);

            if blocks_for_pack.is_empty() {
                // All blocks cross-deduped or skipped. No pack needed.
                continue;
            }

            // Content-addressed pack ID: deterministic from block content.
            let pack_id = crate::block::pack::content_pack_id(&blocks_for_pack);

            // Per-export dedup: if this exact pack is already in the manifest
            // (same content → same pack_id), skip upload entirely.
            {
                let vm = volume_manifest.read();
                if let Some(pack_ids) = vm.chunk_pack_ids(chunk_idx) {
                    if pack_ids.contains(&pack_id) {
                        total_stats.packs_skipped += 1;
                        continue;
                    }
                }
            }

            // Cross-export dedup: content-addressed pack_id means identical
            // packs from different exports map to the same S3 key. The PUT is
            // idempotent — writing the same bytes to the same key is safe.
            // We always upload rather than HEAD-then-skip to avoid a race with
            // GC (GC could delete the pack between our HEAD and manifest sync).

            // Push upload future — runs concurrently with next chunk's compute.
            let cs = &content_store;
            in_flight.push(Box::pin(async move {
                let entries = cs
                    .stream_chunk_pack(chunk_idx, pack_id, blocks_for_pack, blocks_per_chunk)
                    .await?;
                Ok((chunk_idx, pack_id, entries))
            }));

            // Drain completed uploads to bound memory (don't block compute).
            while let Some(result) = in_flight.next().now_or_never().flatten() {
                let (ci, pid, index_entries): (u32, crate::block::pack::PackId, Vec<crate::block::pack::PackIndexEntry>) = result?;
                total_stats.packs_uploaded += 1;
                total_stats.bytes_uploaded += index_entries
                    .iter()
                    .map(|e| e.comp_length as u64)
                    .sum::<u64>();
                pack_index_cache.insert_entries(pid, &index_entries);
                staged_appends.push((ci, pid));
            }
        }

        // Wait for remaining in-flight uploads.
        while let Some(result) = in_flight.next().await {
            let (chunk_idx, pack_id, index_entries) = result?;
            total_stats.packs_uploaded += 1;
            total_stats.bytes_uploaded += index_entries
                .iter()
                .map(|e| e.comp_length as u64)
                .sum::<u64>();
            pack_index_cache.insert_entries(pack_id, &index_entries);
            staged_appends.push((chunk_idx, pack_id));
        }

        // Apply all staged manifest appends atomically.
        // Only reached if every chunk upload succeeded.
        if !staged_appends.is_empty() {
            let mut vm = volume_manifest.write();
            for (chunk_idx, pack_id) in staged_appends {
                vm.append_pack(chunk_idx, pack_id);
            }
        }

        #[cfg(feature = "test-utils")]
        self.flush_gate(super::inner::FlushStep::AfterCompute, flushed_blocks.len()).await;

        #[cfg(feature = "test-utils")]
        self.flush_gate(super::inner::FlushStep::BeforeEvict, flushed_blocks.len()).await;

        // Finalize flushed blocks: SYNCING→NOT_PRESENT.
        //
        // Always evict from the data file. The flush path already inserted
        // every block into foyer (clean_cache) during compute_flush_batch,
        // so reads after eviction hit the in-memory S3-FIFO cache. This
        // keeps the data file as a pure write buffer — born, filled,
        // consumed, deleted — with no CLEAN blocks to carry forward
        // across rotations.
        //
        // CAS failure means a guest write re-dirtied the block during flush.
        // Promote-on-write already copied the full block from flushing → active
        // before the guest pwrite, so the active file has complete data.
        for &chunk_index in &flushed_blocks {
            if !self.inner.transition_syncing_to_not_present(chunk_index) {
                total_stats.blocks_cas_failed += 1;
            }
        }

        #[cfg(feature = "test-utils")]
        self.flush_gate(super::inner::FlushStep::AfterEvict, flushed_blocks.len()).await;

        // Drop flushing file handle but keep the file on disk.
        //
        // The flushing file is the crash-safety net for the window between
        // pack upload and manifest sync + checkpoint. If the host crashes
        // before the manifest reaches S3 and checkpoint persists block states,
        // recovery uses the flushing file to restore block data. The file is
        // deleted by checkpoint() after block states are durably persisted.
        //
        // Clear flushing_active and rotation_seq BEFORE dropping the handle so
        // resolve_read_plan re-enables LocalSsd only after cleanup is complete.
        self.inner.flushing_active.store(false, Ordering::Release);
        self.inner.rotation_seq.store(0, Ordering::Release);
        drop(self.inner.flushing_file.lock().take());

        info!(
            blocks_claimed = total_stats.blocks_claimed,
            blocks_deduped = total_stats.blocks_deduped,
            blocks_cross_deduped = total_stats.blocks_cross_deduped,
            blocks_cas_failed = total_stats.blocks_cas_failed,
            blocks_crc_mismatched = total_stats.blocks_crc_mismatched,
            packs_uploaded = total_stats.packs_uploaded,
            packs_skipped = total_stats.packs_skipped,
            bytes_uploaded = total_stats.bytes_uploaded,
            "flush complete"
        );

        Ok(total_stats)
    }

    /// Flush dirty blocks to S3 as chunk-scoped GLPK v3 packs (no manifest upload).
    ///
    /// Returns (stats, seq_cutpoint). Compaction is the caller's responsibility
    /// and should run outside the flush lock to avoid blocking concurrent flushes.
    #[must_use = "flush errors must be handled to avoid silent data loss"]
    pub async fn flush_packs(
        &self,
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
        clean_cache: Option<&Arc<dyn BlockCache>>,
    ) -> Result<(FlushStats, u64), CacheError> {
        self.flush_dirty_inner(content_store, pack_index_cache, volume_manifest, clean_cache)
            .await
    }

    /// Upload the VolumeManifest (binary GLVM) to S3.
    ///
    /// Uses conditional PUT (`If-Match`) when we have a known ETag, preventing
    /// a concurrent writer (e.g., another host after failover) from having its
    /// manifest silently overwritten.
    #[must_use = "manifest sync errors must be handled to avoid orphaned packs"]
    pub async fn sync_manifest(
        &self,
        content_store: &ContentStore,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<(), CacheError> {
        let manifest_bytes = volume_manifest.read().serialize()?;
        let expected_etag = self.inner.manifest_etag.lock().clone();
        let new_etag = content_store
            .put_manifest(
                &self.inner.export_name,
                manifest_bytes,
                expected_etag.as_deref(),
            )
            .await?;
        *self.inner.manifest_etag.lock() = new_etag;
        self.checkpoint().await?;
        // Manifest synced and checkpoint persisted — the flushing file is no
        // longer needed as a crash-safety net. Delete it so the next flush
        // cycle can rotate the data file.
        let flushing_path = self.inner.config.flushing_path();
        if flushing_path.exists()
            && let Err(e) = std::fs::remove_file(&flushing_path)
        {
                warn!(error = %e, "failed to remove flushing file after manifest sync");
        }
        Ok(())
    }

    /// Flush dirty blocks to S3 + upload manifest (drain/snapshot path).
    ///
    /// Retries manifest upload up to 3 times before propagating the error,
    /// mirroring flush_scheduler's pattern. This prevents spurious drain
    /// failures when blocks are already clean but a transient S3 error
    /// prevents the manifest upload.
    #[must_use = "flush errors must be handled to avoid silent data loss"]
    #[instrument(skip(self, content_store, pack_index_cache, volume_manifest))]
    pub async fn flush_to_s3(
        &self,
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<FlushStats, CacheError> {
        let _flush_guard = self.inner.flush_lock.lock().await;
        let (stats, _seq_cutpoint) = self
            .flush_dirty_inner(content_store, pack_index_cache, volume_manifest, None)
            .await?;
        let manifest_bytes = volume_manifest.read().serialize()?;
        let expected_etag = self.inner.manifest_etag.lock().clone();
        let mut last_err = None;
        for attempt in 0..3u32 {
            match content_store
                .put_manifest(
                    &self.inner.export_name,
                    manifest_bytes.clone(),
                    expected_etag.as_deref(),
                )
                .await
            {
                Ok(new_etag) => {
                    *self.inner.manifest_etag.lock() = new_etag;
                    last_err = None;
                    break;
                }
                Err(e @ ContentStoreError::PreconditionFailed(_)) => {
                    // Another host owns this manifest. Don't retry — every
                    // attempt will fail with the same stale ETag.
                    return Err(e.into());
                }
                Err(e) => {
                    warn!(
                        error = %e, attempt = attempt + 1,
                        "manifest upload failed in flush_to_s3, retrying"
                    );
                    last_err = Some(e);
                    if attempt < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100 * (1 << attempt),
                        ))
                        .await;
                    }
                }
            }
        }
        if let Some(e) = last_err {
            return Err(e.into());
        }
        self.checkpoint().await?;
        // Manifest synced and checkpoint persisted — delete the flushing file.
        let flushing_path = self.inner.config.flushing_path();
        if flushing_path.exists()
            && let Err(e) = std::fs::remove_file(&flushing_path)
        {
                warn!(error = %e, "failed to remove flushing file after flush_to_s3");
        }
        Ok(stats)
    }

    /// Take a point-in-time snapshot.
    #[must_use = "snapshot errors must be handled"]
    #[instrument(skip(self, content_store, pack_index_cache, volume_manifest))]
    pub async fn snapshot(
        &self,
        content_store: &ContentStore,
        pack_index_cache: &Arc<crate::block::pack_index_cache::PackIndexCache>,
        volume_manifest: &Arc<parking_lot::RwLock<crate::block::volume_manifest::VolumeManifest>>,
    ) -> Result<SnapshotResult, CacheError> {
        let _flush_guard = self.inner.flush_lock.lock().await;
        let (stats, seq_cutpoint) = self
            .flush_dirty_inner(content_store, pack_index_cache, volume_manifest, None)
            .await?;

        let manifest_bytes = volume_manifest.read().serialize()?;
        let expected_etag = self.inner.manifest_etag.lock().clone();
        let manifest_etag = content_store
            .put_manifest(
                &self.inner.export_name,
                manifest_bytes.clone(),
                expected_etag.as_deref(),
            )
            .await?;
        *self.inner.manifest_etag.lock() = manifest_etag.clone();

        let snapshot_persisted = {
            let mut persisted = false;
            for attempt in 0..3u32 {
                match content_store
                    .put_snapshot(&self.inner.export_name, seq_cutpoint, manifest_bytes.clone())
                    .await
                {
                    Ok(_) => {
                        persisted = true;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            error = %e, sequence = seq_cutpoint, attempt = attempt + 1,
                            "failed to persist versioned snapshot, retrying"
                        );
                        if attempt < 2 {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << attempt),
                            ))
                            .await;
                        }
                    }
                }
            }
            persisted
        };

        self.checkpoint().await?;
        // Manifest synced and checkpoint persisted — delete the flushing file.
        let flushing_path = self.inner.config.flushing_path();
        if flushing_path.exists()
            && let Err(e) = std::fs::remove_file(&flushing_path)
        {
                warn!(error = %e, "failed to remove flushing file after snapshot");
        }
        Ok(SnapshotResult {
            manifest_etag,
            sequence: seq_cutpoint,
            snapshot_persisted,
            stats,
            manifest_bytes,
        })
    }
}
