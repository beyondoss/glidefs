use std::collections::{BTreeMap, HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tracing::{debug, info, instrument, warn};

use crate::block::block_map::{Blake3Hash, SparseBlockState, blake3_128, lz4_compress};
use crate::block::cache::BlockCache;
use crate::block::content_store::ContentStore;
use crate::block::state::{Active, Draining};

use super::inner::{CRC_SENTINEL, CacheInner, is_zero_block};
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
/// CRC verification uses the crc_map (SparseCrcMap) and state discrimination:
/// - CRC mismatch + state == SYNCING → real SSD corruption
/// - CRC mismatch + state != SYNCING → concurrent write re-dirtied the block
fn compute_flush_batch(
    inner: &CacheInner,
    snapshot: &[usize],
    zero_hash: Blake3Hash,
    clean_cache: Option<Arc<dyn BlockCache>>,
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
        .expect("compute_flush_batch called without prior rotation")
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

            // Verify CRC32 if available (detects SSD corruption before BLAKE3).
            // CRC was stored at checkpoint time. Use state discrimination to
            // distinguish corruption from concurrent writes:
            // - Block still SYNCING → no concurrent write → real corruption
            // - Block now DIRTY → write re-dirtied it → stale CRC, not corruption
            // - CRC_SENTINEL → write invalidated the CRC, skip verification
            if let Some(stored_crc) = inner.crc_take(chunk_index)
                && stored_crc != CRC_SENTINEL
            {
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

            // Fast zero-block detection (AVX2 on x86_64) before BLAKE3.
            // Avoids the full hash + compress for trimmed/unwritten blocks.
            if is_zero_block(&chunk_buf) {
                return Ok(BlockResult::Computed {
                    chunk_index,
                    hash: zero_hash,
                    compressed: None,
                });
            }

            let hash = blake3_128(&chunk_buf);

            // Warm clean_cache: decompressed block data
            // goes into the Foyer S3-FIFO cache so reads after eviction hit
            // cache instead of S3. S3-FIFO handles scan resistance.
            if let Some(ref cache) = clean_cache {
                cache.insert(hash, Bytes::from(chunk_buf.clone()));
            }

            let compressed = Some(lz4_compress(&chunk_buf[..]));

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
                stats.blocks_claimed += 1;
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

/// Compute CRC32 checksums for dirty blocks that don't have one yet.
///
/// Stores CRCs in the crc_map (SparseCrcMap) for later verification by the
/// flush path. Writes invalidate stale CRCs by storing CRC_SENTINEL.
///
/// Sentinel handling: when we encounter a sentinel entry, we remove it
/// before reading the block. If a concurrent write arrives between our
/// remove and the subsequent `try_insert`, the write stores a fresh
/// sentinel that prevents our (possibly stale) CRC from being stored.
///
/// Capped at MAX_CRC_ENTRIES to bound memory. Blocks beyond the cap skip
/// CRC verification at flush time — the SYNCING state machine still
/// guarantees correctness; we just lose SSD corruption detection for those
/// blocks.
pub(super) fn compute_dirty_crc32s(inner: &CacheInner) -> usize {
    use rayon::prelude::*;
    use std::sync::atomic::AtomicU64;

    /// Maximum crc_map entries per export. 10M entries × 4 bytes
    /// (SparseCrcMap: AtomicU32 per entry, 4KB pages) ≈ 40MB.
    /// Covers a fully-dirty 1TB device (8M blocks of 128KB) with headroom.
    /// Prevents unbounded growth if device_size is ever misconfigured.
    const MAX_CRC_ENTRIES: usize = 10_000_000;

    // Already at cap — skip entirely.
    if inner.crc_map.count() >= MAX_CRC_ENTRIES {
        return 0;
    }

    // Collect dirty block indices up front so rayon can partition the work.
    // Capped to leave headroom in the crc_map.
    let dirty_indices: Vec<usize> = inner
        .state_map
        .iter_with_state(SparseBlockState::DIRTY)
        .take(MAX_CRC_ENTRIES.saturating_sub(inner.crc_map.count()))
        .collect();

    if dirty_indices.is_empty() {
        return 0;
    }

    let block_size = inner.config.block_size;
    let device_size = inner.config.device_size;
    let computed = AtomicU64::new(0);
    let read_errors = AtomicU64::new(0);

    // Parallel CRC32 computation: each rayon task allocates its own read
    // buffer (peak memory = num_threads × block_size, same as compute_flush_batch).
    dirty_indices.par_iter().for_each(|&idx| {
        // Soft cap: stop computing CRCs once the map is full. Racy but harmless —
        // exceeding the cap slightly is fine, the bound prevents runaway growth.
        if inner.crc_map.count() >= MAX_CRC_ENTRIES {
            return;
        }

        // Skip blocks that already have a valid CRC.
        // If a sentinel is present (invalidated by a concurrent write),
        // clear it and recompute.
        if let Some(existing) = inner.crc_map.load(idx) {
            if existing != CRC_SENTINEL {
                return; // valid CRC exists, skip
            }
            // Clear sentinel before reading. If a concurrent write arrives
            // between this remove and our try_insert below, the write will
            // store a fresh sentinel that prevents our (potentially stale)
            // CRC from being stored.
            inner.crc_map.remove(idx);
        }

        let offset = idx as u64 * block_size as u64;
        let valid_bytes =
            std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;
        if valid_bytes == 0 {
            return;
        }

        let mut buf = vec![0u8; block_size];
        if let Err(e) = inner
            .data_file
            .read()
            .read_exact_at(&mut buf[..valid_bytes], offset)
        {
            warn!(
                chunk_index = idx,
                error = %e,
                "failed to read block for CRC32 computation"
            );
            read_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if valid_bytes < block_size {
            buf[valid_bytes..].fill(0);
        }

        let crc = crc32fast::hash(&buf);
        inner.crc_store(idx, crc);
        computed.fetch_add(1, Ordering::Relaxed);
    });

    let computed = computed.load(Ordering::Relaxed);
    if computed > 0 {
        debug!(computed, "computed CRC32 checksums for dirty blocks");
    }

    read_errors.load(Ordering::Relaxed) as usize
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

    /// Get the raw block state.
    #[inline]
    pub fn block_state(&self, block_idx: usize) -> u8 {
        self.inner.state_map.get(block_idx)
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
    /// the Tokio runtime with `sync_all()` + `rename()` syscalls in
    /// `save_block_states`.
    ///
    async fn checkpoint(&self) -> Result<(), CacheError> {
        let inner = Arc::clone(&self.inner);
        crate::task::spawn_blocking_named("checkpoint", move || {
            inner.save_block_states()?;
            inner.wal.truncate()?;
            Ok(())
        })
        .await
        .map_err(|e| CacheError::Io(std::io::Error::other(e)))?
    }

    /// Local checkpoint: compute CRC32s for dirty blocks, persist state, truncate WAL.
    ///
    /// Runs on a blocking thread via `spawn_blocking` to avoid starving the
    /// async runtime with synchronous pread + CRC32 computation across
    /// potentially thousands of dirty blocks.
    ///
    /// Independent of S3 — keeps the WAL bounded in demand-driven mode
    /// where S3 flushes may be infrequent. Should run every ~5s when
    /// there are dirty blocks.
    pub async fn local_checkpoint(&self) -> Result<(), CacheError> {
        let inner = Arc::clone(&self.inner);
        crate::task::spawn_blocking_named("local-checkpoint", move || {
            compute_dirty_crc32s(&inner);
            inner.save_block_states()?;
            inner.wal.truncate()?;
            debug!("local checkpoint complete");
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
    /// The entire rotation is one atomic critical section under a single
    /// write-lock acquisition. This eliminates a class of races where
    /// state changes (CAS, file swap) span non-atomic windows. The lock
    /// holds for ~15µs on typical hardware — rename is an inode pointer
    /// swap, file creation is a single syscall, and the CAS loop touches
    /// only cache-hot atomics.
    ///
    /// All DIRTY blocks are claimed (CAS DIRTY→SYNCING).
    fn rotate_data_file_inner(&self, snapshot: bool) -> Result<(Vec<usize>, ()), CacheError> {
        let active_path = self.inner.config.data_path();
        let flushing_path = self.inner.config.flushing_path();

        // === Single write lock: everything below is atomic w.r.t. I/O ===
        let mut data_file_guard = self.inner.data_file.write();

        // Signal flush rotation in progress. Under the lock so no reader
        // can observe flushing_active=true with stale file state.
        self.inner.flushing_active.store(true, Ordering::Release);

        // Rename active → flushing. The file handle in data_file_guard
        // still refers to the same inode (now at flushing_path).
        std::fs::rename(&active_path, &flushing_path)?;

        // Create new sparse active file.
        let new_file = super::inner::SyncFile::open(
            &active_path,
            true,
            self.inner.config.device_size,
        )?;

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

        if snapshot.is_empty() {
            debug!("flush: no dirty blocks to flush");
            // Clean up the empty flushing file (rotated but no dirty blocks)
            self.inner.flushing_active.store(false, Ordering::Release);
            drop(self.inner.flushing_file.lock().take());
            let flushing_path = self.inner.config.flushing_path();
            if flushing_path.exists() {
                let _ = std::fs::remove_file(&flushing_path);
            }
            return Ok((FlushStats::default(), seq_cutpoint));
        }

        info!(dirty_blocks = snapshot.len(), seq_cutpoint, "starting flush");

        let result = self
            .flush_dirty_body(&snapshot, content_store, pack_index_cache, volume_manifest, clean_cache)
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
            if let Some(ref ff) = *self.inner.flushing_file.lock() {
                for &idx in &snapshot {
                    if self.inner.state_map.get(idx) != SparseBlockState::SYNCING {
                        continue;
                    }
                    let offset = idx as u64 * block_size as u64;
                    let valid_bytes = std::cmp::min(
                        block_size as u64,
                        self.inner.config.device_size.saturating_sub(offset),
                    ) as usize;
                    if valid_bytes > 0 {
                        let mut buf = vec![0u8; valid_bytes];
                        if ff.read_exact_at(&mut buf, offset).is_ok() {
                            let _ = df.write_all_at(&buf, offset);
                        }
                    }
                }
            }
            drop(df);
            self.inner.flushing_active.store(false, Ordering::Release);
            drop(self.inner.flushing_file.lock().take());
            let flushing_path = self.inner.config.flushing_path();
            if flushing_path.exists() {
                let _ = std::fs::remove_file(&flushing_path);
            }
            // Now transition all snapshot blocks to DIRTY (after data is in active file).
            for &idx in &snapshot {
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
    ) -> Result<FlushStats, CacheError> {
        use crate::block::pack::new_pack_id;

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

        // Per-chunk flush
        for (chunk_idx, chunk_blocks) in per_chunk {
            // Compute batch (pread + CRC32 + BLAKE3 + LZ4)
            let zero_hash = self.inner.zero_block_hash;
            let inner = Arc::clone(&self.inner);
            let clean_cache_clone = clean_cache.map(Arc::clone);
            let mut batch = crate::task::spawn_blocking_named("flush-compute", move || {
                compute_flush_batch(&inner, &chunk_blocks, zero_hash, clean_cache_clone)
            })
            .await
            .map_err(|e| CacheError::Io(std::io::Error::other(e)))??;

            total_stats.blocks_claimed += batch.stats.blocks_claimed;
            total_stats.blocks_deduped += batch.stats.blocks_deduped;
            total_stats.blocks_cas_failed += batch.stats.blocks_cas_failed;
            total_stats.blocks_corrupted += batch.stats.blocks_corrupted;

            // Transition skipped blocks back to DIRTY.
            //
            // Blocks still SYNCING (corrupted, partial-not-re-dirtied) need
            // data promoted from flushing → active before transition, since
            // no guest write triggered promote-on-write for them.
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
                                let _ = df.write_all_at(&buf, offset);
                            }
                        }
                    }
                    self.inner.transition_to_dirty(idx);
                }
            }

            // Build hash → compressed data map from the unique-hash upload set.
            // Within-batch dedup (seen_hashes in compute_flush_batch) ensures one
            // copy of compressed data per unique hash.
            let hash_to_compressed: HashMap<Blake3Hash, Vec<u8>> =
                std::mem::take(&mut batch.to_upload).into_iter().collect();

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
            let zero_hash = self.inner.zero_block_hash;
            let mut blocks_for_pack: Vec<(Blake3Hash, u32, Vec<u8>)> = Vec::new();
            let mut packed_indices: Vec<usize> = Vec::new();
            {
                let vm = volume_manifest.read();
                for &(block_index, hash) in &batch.computed {
                    let compressed = if hash == zero_hash {
                        Vec::new() // zero block tombstone: comp_length = 0
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
                    let chunk_offset = vm.block_offset_in_chunk(block_index as u64);
                    blocks_for_pack.push((hash, chunk_offset, compressed));
                    packed_indices.push(block_index);
                }
            }

            if blocks_for_pack.is_empty() {
                // All blocks in this chunk were skipped (CRC mismatch, concurrent
                // re-dirty, or invariant violation). Nothing to upload — skipped
                // blocks were already transitioned back to DIRTY above.
                continue;
            }

            // Stream GLPK v3 pack to S3 (blocks freed as they upload)
            let pack_id = new_pack_id();

            let index_entries = content_store
                .stream_chunk_pack(chunk_idx, pack_id, blocks_for_pack, blocks_per_chunk)
                .await?;

            total_stats.packs_uploaded += 1;
            total_stats.bytes_uploaded += index_entries
                .iter()
                .map(|e| e.comp_length as u64)
                .sum::<u64>();

            // Update PackIndexCache with new entries
            pack_index_cache.insert_entries(pack_id, &index_entries);

            // Stage manifest append (applied after all chunk uploads succeed).
            // This avoids orphaned manifest entries if a later chunk's S3 upload fails.
            staged_appends.push((chunk_idx, pack_id));

            flushed_blocks.extend(packed_indices);
        }

        // Apply all staged manifest appends atomically.
        // Only reached if every chunk upload succeeded.
        if !staged_appends.is_empty() {
            let mut vm = volume_manifest.write();
            for (chunk_idx, pack_id) in staged_appends {
                vm.append_pack(chunk_idx, pack_id);
            }
        }

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

        // Drop flushing file handle and delete the file.
        // Clear flushing_active BEFORE dropping the file so resolve_read_plan
        // re-enables LocalSsd only after the cleanup is complete.
        self.inner.flushing_active.store(false, Ordering::Release);
        drop(self.inner.flushing_file.lock().take());
        let flushing_path = self.inner.config.flushing_path();
        if flushing_path.exists()
            && let Err(e) = std::fs::remove_file(&flushing_path)
        {
            warn!(error = %e, "failed to remove flushing file");
        }

        info!(
            blocks_claimed = total_stats.blocks_claimed,
            blocks_deduped = total_stats.blocks_deduped,
            blocks_cas_failed = total_stats.blocks_cas_failed,
            blocks_corrupted = total_stats.blocks_corrupted,
            packs_uploaded = total_stats.packs_uploaded,
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
        let manifest_bytes = volume_manifest.read().serialize();
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
        let manifest_bytes = volume_manifest.read().serialize();
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

        let manifest_bytes = volume_manifest.read().serialize();
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
        Ok(SnapshotResult {
            manifest_etag,
            sequence: seq_cutpoint,
            snapshot_persisted,
            stats,
        })
    }
}
