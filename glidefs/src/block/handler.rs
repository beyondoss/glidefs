//! Transport-agnostic block I/O handler.
//!
//! This module provides:
//! - `BlockHandler`: Thin handler that uses WriteCache for all I/O
//! - `BlockDevice`: Device descriptor used during transmission phase

use super::cache::BlockCache;
use super::content_store::ContentStore;
use super::error::{CommandError, CommandResult};
#[cfg(all(target_os = "linux", feature = "ublk"))]
use super::write_cache::CacheError;
use super::metrics::ExportMetrics;
use super::pack_index_cache::PackIndexCache;
use super::readahead::SequentialDetector;
use super::state::Active;
use super::volume_manifest::VolumeManifest;
use super::write_cache::WriteCache;
use super::write_trace::WriteTracer;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::Notify;

// ============================================================================
// Test-only: deterministic interleaving infrastructure
// ============================================================================

/// Steps in the backfill_and_write path where concurrent writers can interleave.
#[cfg(feature = "test-utils")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillStep {
    /// After initial block state check (knows if NOT_PRESENT/CLEAN/DIRTY/SYNCING).
    StateChecked,
    /// After resolve_block_for_backfill returns (has `prior` data from S3 or SSD).
    S3FetchDone,
    /// After post-fetch state re-check.
    PostFetchRecheck,
    /// Before try_claim_block CAS.
    BeforeCas,
    /// After CAS result (winner or loser decided).
    AfterCas,
    /// Before cache.write (full-block merge for winner, sub-block for loser/fast-path).
    BeforeWrite,
}

/// Event sent by a writer at each sync point.
#[cfg(feature = "test-utils")]
#[derive(Debug, Clone)]
pub struct BackfillEvent {
    pub writer_id: u64,
    pub step: BackfillStep,
    pub block_idx: usize,
    /// Block state at the time of the event.
    pub block_state: u8,
}

/// Sync point controller for deterministic interleaving tests.
///
/// Each writer sends events at critical points and waits for the test to
/// release it. The test receives events, decides ordering, and sends
/// proceed signals to advance specific writers.
#[cfg(feature = "test-utils")]
pub struct BackfillSyncPoints {
    /// Writer → test: "I'm at step X".
    event_tx: tokio::sync::mpsc::UnboundedSender<BackfillEvent>,
    /// Per-writer proceed channels. Test sends () to release a specific writer.
    /// Keyed by writer_id. Writers register on first use.
    proceed_channels: parking_lot::Mutex<
        std::collections::HashMap<u64, tokio::sync::mpsc::UnboundedSender<()>>,
    >,
    /// Counter for assigning writer IDs.
    next_writer_id: AtomicU64,
}

#[cfg(feature = "test-utils")]
impl BackfillSyncPoints {
    /// Create a new sync point controller. Returns (controller, event_receiver).
    pub fn new() -> (
        Arc<Self>,
        tokio::sync::mpsc::UnboundedReceiver<BackfillEvent>,
    ) {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let sp = Arc::new(Self {
            event_tx,
            proceed_channels: parking_lot::Mutex::new(std::collections::HashMap::new()),
            next_writer_id: AtomicU64::new(0),
        });
        (sp, event_rx)
    }

    /// Allocate a writer ID and return its proceed receiver.
    fn register_writer(&self) -> (u64, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let id = self.next_writer_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.proceed_channels.lock().insert(id, tx);
        (id, rx)
    }

    /// Release a specific writer to proceed past its current gate.
    pub fn release(&self, writer_id: u64) {
        let channels = self.proceed_channels.lock();
        if let Some(tx) = channels.get(&writer_id) {
            let _ = tx.send(());
        }
    }
}

/// Block device descriptor used during transmission phase.
#[derive(Clone)]
pub struct BlockDevice {
    pub name: Vec<u8>,
    pub size: u64,
}

/// Handler for block I/O operations using write-behind cache.
///
/// This is a thin layer that delegates all I/O to the WriteCache.
/// The key performance benefit: `flush()` only syncs to local SSD,
/// not to S3 (which happens asynchronously in the background).
///
/// Reads use read-through caching: if a block isn't present locally,
/// it's fetched from S3 on demand.
///
/// Reject writes to new blocks when SSD utilization exceeds this ratio.
/// Overwrites to already-present blocks are allowed (no new SSD space).
const WRITE_REJECT_THRESHOLD: f64 = 0.95;

/// Transport-agnostic: used by both NBD and ublk frontends.
pub struct BlockHandler {
    /// The write-behind cache (must be in Active state)
    cache: Arc<WriteCache<Active>>,

    /// Content-addressed S3 store for v2 pack reads
    content_store: Arc<ContentStore>,

    /// v2 clean block cache (decompressed blocks from S3 packs)
    clean_cache: Arc<dyn BlockCache>,

    /// Pack index cache for v4 block resolution
    pack_index_cache: Arc<PackIndexCache>,

    /// Volume manifest mapping chunk indices to pack lists (v4)
    volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,

    /// Device size in bytes (atomic for live resize)
    device_size: AtomicU64,

    /// Whether this export is readonly (rejects writes)
    /// Uses AtomicBool so promote_export can change it safely
    readonly: AtomicBool,

    /// I/O metrics for this export
    metrics: Arc<ExportMetrics>,

    /// Sequential read-ahead detector
    readahead: Mutex<SequentialDetector>,

    /// SSD utilization ratio shared from ExportRouter.
    /// Used to reject writes to new blocks when SSD > 95%.
    ssd_utilization: Arc<AtomicU64>,

    /// Notifies the flush scheduler when dirty blocks reach the threshold.
    flush_notify: Arc<Notify>,

    /// Flush threshold: auto-flush when dirty blocks reach this count.
    /// 0 = manual mode (no auto-flush, drain/snapshot only).
    blocks_per_pack: usize,

    /// Optional write trace recorder. Zero cost when None.
    write_tracer: Option<Arc<WriteTracer>>,

    /// Test-only: sync points for deterministic interleaving tests.
    #[cfg(feature = "test-utils")]
    backfill_sync: Option<Arc<BackfillSyncPoints>>,
}

impl BlockHandler {
    /// Create a new block handler.
    ///
    /// # Arguments
    /// * `cache` - Write-behind cache in Active state
    /// * `device_size` - Size of the block device in bytes
    /// * `readonly` - Whether this export rejects writes
    /// * `metrics` - Shared metrics for tracking I/O statistics
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cache: Arc<WriteCache<Active>>,
        content_store: Arc<ContentStore>,
        clean_cache: Arc<dyn BlockCache>,
        pack_index_cache: Arc<PackIndexCache>,
        volume_manifest: Arc<parking_lot::RwLock<VolumeManifest>>,
        device_size: u64,
        readonly: bool,
        metrics: Arc<ExportMetrics>,
        ssd_utilization: Arc<AtomicU64>,
        flush_notify: Arc<Notify>,
        blocks_per_pack: usize,
        write_tracer: Option<Arc<WriteTracer>>,
    ) -> Self {
        Self {
            cache,
            content_store,
            clean_cache,
            pack_index_cache,
            volume_manifest,
            device_size: AtomicU64::new(device_size),
            readonly: AtomicBool::new(readonly),
            metrics,
            readahead: Mutex::new(SequentialDetector::new()),
            ssd_utilization,
            flush_notify,
            blocks_per_pack,
            write_tracer,
            #[cfg(feature = "test-utils")]
            backfill_sync: None,
        }
    }

    /// Attach sync points for deterministic interleaving tests.
    #[cfg(feature = "test-utils")]
    pub fn with_backfill_sync(mut self, sync: Arc<BackfillSyncPoints>) -> Self {
        self.backfill_sync = Some(sync);
        self
    }

    /// Check if this handler is readonly.
    pub fn is_readonly(&self) -> bool {
        self.readonly.load(Ordering::Relaxed)
    }

    /// Set the readonly flag.
    /// Used by promote_export to allow writes after migration.
    pub fn set_readonly(&self, readonly: bool) {
        self.readonly.store(readonly, Ordering::Relaxed);
    }

    /// Get the device size.
    #[inline]
    pub fn device_size(&self) -> u64 {
        self.device_size.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn block_size(&self) -> usize {
        self.cache.block_size()
    }

    /// Set the device size (for resize).
    /// Only safe to call after draining dirty blocks.
    #[allow(dead_code)]
    pub fn set_device_size(&self, new_size: u64) {
        self.device_size.store(new_size, Ordering::Relaxed);
    }

    /// Notify the flush scheduler if dirty blocks have reached the threshold.
    /// No-op when blocks_per_pack == 0 (manual flush mode).
    #[inline]
    fn check_flush_threshold(&self) {
        if self.blocks_per_pack > 0
            && self.cache.dirty_block_count() >= self.blocks_per_pack as u64
        {
            self.flush_notify.notify_one();
        }
    }

    /// Backfill NOT_PRESENT blocks in a range without writing guest data.
    ///
    /// Used by trim/write_zeroes: fetch complete blocks from foyer/S3 so
    /// the zero operation only destroys data in the zeroed sub-range, not
    /// in the rest of the block.
    async fn backfill_blocks_in_range(&self, offset: u64, len: u64) -> CommandResult<()> {
        let block_size = self.cache.block_size();
        let block_size_u64 = block_size as u64;
        let start_block = offset / block_size_u64;
        let end_block = (offset + len - 1) / block_size_u64;

        for block in start_block..=end_block {
            let idx = block as usize;
            if self.cache.is_block_present(idx) {
                continue;
            }
            // Check if the zero covers the full block — no backfill needed.
            let block_start = block * block_size_u64;
            let zero_start = offset.max(block_start);
            let zero_end = (offset + len).min(block_start + block_size_u64);
            if zero_end - zero_start >= block_size_u64 {
                continue;
            }
            // Check if this block has S3 data before doing the expensive resolve.
            let has_s3_data = {
                let (bo, pids) = {
                    let vm = self.volume_manifest.read();
                    let ci = vm.chunk_idx_for_block(block);
                    let bo = vm.block_offset_in_chunk(block);
                    let pids = vm.chunk_pack_ids(ci).map(|ids| ids.to_vec()).unwrap_or_default();
                    (bo, pids)
                };
                if pids.is_empty() {
                    false
                } else {
                    let mut found = false;
                    for &pid in pids.iter().rev() {
                        if self.pack_index_cache.lookup_block(pid, bo).await.is_some() {
                            found = true;
                            break;
                        }
                        if self.pack_index_cache.get_entries(pid).await.is_none() {
                            found = true;
                            break;
                        }
                    }
                    found
                }
            };
            if !has_s3_data {
                continue;
            }
            // Fetch prior data from S3 and write the full block.
            let block_data = match self.cache.resolve_block_for_backfill(
                idx,
                self.clean_cache.as_ref(),
                &self.pack_index_cache,
                &self.volume_manifest,
                &self.content_store,
                Some(&self.metrics),
            ).await {
                Ok(data) => data,
                Err(e) => return Err(e.into()),
            };
            // Skip if the resolved data is all zeros — the data file already
            // has zeros (sparse or set_len). Writing zeros would create
            // unnecessary dirty blocks and WAL entries, and on ublk could
            // trigger a flush rotation that invalidates the io_uring
            // registered fd.
            if block_data.iter().all(|&b| b == 0) {
                continue;
            }
            self.cache.write(block_start, &block_data)?;
        }

        Ok(())
    }

    /// Backfill NOT_PRESENT blocks and write guest data in one operation.
    ///
    /// For each block in the write range:
    /// - If the block is already present, just pwrite the guest data.
    /// - If the block is NOT_PRESENT and the write covers the full block,
    ///   just pwrite (no prior data to preserve).
    /// - If the block is NOT_PRESENT and the write is a sub-block, fetch
    ///   the full block from foyer/S3, overlay the guest's sub-block in
    ///   memory, and pwrite the merged full block.
    ///
    /// # Concurrency
    ///
    /// The kernel can split a single pwrite() into multiple NBD requests
    /// at arbitrary sector boundaries (NBD doesn't advertise our block
    /// size as physical_block_size). Two concurrent requests can target
    /// different sub-ranges of the same block. Without synchronization,
    /// both would fetch the same prior data from S3, merge their
    /// respective sub-ranges, and write the full block — the second
    /// pwrite clobbers the first's data.
    ///
    /// Fix: use `try_claim_block` (CAS NOT_PRESENT→CLEAN) as a gate.
    /// Only the "winner" does the merge+write. "Losers" yield until the
    /// block transitions to DIRTY (winner's `cache.write` completed),
    /// then write their sub-range on top.
    async fn backfill_and_write(&self, offset: u64, data: &[u8]) -> CommandResult<()> {
        let block_size = self.cache.block_size();
        let block_size_u64 = block_size as u64;
        let start_block = offset / block_size_u64;
        let end_block = (offset + data.len() as u64 - 1) / block_size_u64;

        // Test-only: register this writer and get a proceed channel.
        #[cfg(feature = "test-utils")]
        let (mut _sync_writer_id, mut _sync_proceed_rx): (Option<u64>, Option<tokio::sync::mpsc::UnboundedReceiver<()>>) =
            match &self.backfill_sync {
                Some(sp) => {
                    let (id, rx) = sp.register_writer();
                    (Some(id), Some(rx))
                }
                None => (None, None),
            };

        // Test-only gate macro: send event, wait for proceed signal.
        macro_rules! _backfill_gate {
            ($step:expr, $block_idx:expr, $state:expr) => {
                #[cfg(feature = "test-utils")]
                {
                    if let Some(sp) = &self.backfill_sync {
                        if let (Some(id), Some(rx)) = (_sync_writer_id, &mut _sync_proceed_rx) {
                            let _ = sp.event_tx.send(BackfillEvent {
                                writer_id: id,
                                step: $step,
                                block_idx: $block_idx,
                                block_state: $state,
                            });
                            let _ = rx.recv().await;
                        }
                    }
                }
            };
        }

        // Fast path: all blocks already present AND fully written.
        // DIRTY/SYNCING = data is on local SSD (or promotable from flushing file).
        // CLEAN = claimed but not yet written — must NOT write sub-range yet.
        // NOT_PRESENT = needs backfill.
        {
            let all_ready = (start_block..=end_block).all(|b| {
                self.cache.block_state(b as usize).has_local_data()
            });
            if all_ready {
                match self.cache.write_with_eviction_check(offset, data) {
                    Ok(()) => return Ok(()),
                    Err(super::write_cache::CacheError::BlockEvicted) => {
                        // Flush evicted a block between state check and pwrite.
                        // Fall through to slow path which retries per-block.
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }

        // Slow path: at least one block needs backfill.
        for block in start_block..=end_block {
            let idx = block as usize;
            let block_start = block * block_size_u64;

            // Slice of guest data that falls within this block.
            let write_start = offset.max(block_start);
            let write_end = (offset + data.len() as u64).min(block_start + block_size_u64);
            let data_offset = (write_start - offset) as usize;
            let block_local_start = (write_start - block_start) as usize;
            let write_len = (write_end - write_start) as usize;

            // Resolve the block state. DIRTY/SYNCING blocks have data on
            // local SSD (or promotable from the flushing file via
            // promote_syncing_blocks inside cache.write). Safe to pwrite
            // our sub-range directly. CLEAN means another writer claimed
            // the block but hasn't written yet — we must wait. NOT_PRESENT
            // means the block needs backfill from S3.
            //
            // After any wait or backfill, re-check state: flush rotation
            // can transition the block through DIRTY→SYNCING→NOT_PRESENT
            // at any time. If that happens, we must restart the backfill
            // for this block rather than writing into an empty file.
            'block_retry: loop {
                let state = self.cache.block_state(idx);

                _backfill_gate!(BackfillStep::StateChecked, idx, state.raw());

                match () {
                    _ if state.has_local_data() => {
                        // Block has data locally. cache.write handles SYNCING
                        // via promote_syncing_blocks (copies from flushing file).
                        // Use eviction check: if flush evicted the block between
                        // our state check and the pwrite, BlockEvicted retries.
                        match self.cache.write_with_eviction_check(write_start, &data[data_offset..data_offset + write_len]) {
                            Ok(()) => break 'block_retry,
                            Err(super::write_cache::CacheError::BlockEvicted) => continue 'block_retry,
                            Err(e) => return Err(e.into()),
                        }
                    }
                    _ if state.is_clean() => {
                        // Another writer claimed this block but hasn't written
                        // the merged data yet. Wait for completion.
                        loop {
                            if !self.cache.block_state(idx).is_clean() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_micros(50)).await;
                        }
                        // State changed — re-enter the outer match to handle
                        // whatever it became (DIRTY, SYNCING, or NOT_PRESENT
                        // if flush evicted it).
                        continue 'block_retry;
                    }
                    _ => {
                        // NOT_PRESENT — needs backfill. Fall through.
                    }
                }

                // === NOT_PRESENT: backfill from S3 ===

                if write_len >= block_size {
                    // Full block overwrite — no prior data to preserve.
                    self.cache.write(write_start, &data[data_offset..data_offset + write_len])?;
                    break 'block_retry;
                }

                // Check if this block has S3 data.
                let has_s3_data = {
                    let (bo, pids) = {
                        let vm = self.volume_manifest.read();
                        let ci = vm.chunk_idx_for_block(block as u64);
                        let bo = vm.block_offset_in_chunk(block as u64);
                        let pids = vm.chunk_pack_ids(ci).map(|ids| ids.to_vec()).unwrap_or_default();
                        (bo, pids)
                    };
                    if pids.is_empty() {
                        false
                    } else {
                        let mut found = false;
                        for &pid in pids.iter().rev() {
                            match self.pack_index_cache.get_entries(pid).await {
                                Some(entries) => {
                                    // Pack index is cached. Check if this block
                                    // offset is in the entries. Use linear scan
                                    // (not binary search via lookup_block) to
                                    // avoid a race where foyer's async insertion
                                    // hasn't committed for lookup_block's .get()
                                    // but has for get_entries' .get().
                                    if entries.iter().any(|e| e.chunk_offset == bo) {
                                        found = true;
                                        break;
                                    }
                                    // Pack is cached but block isn't in it — try older packs.
                                }
                                None => {
                                    // Pack index not cached — conservatively assume
                                    // it has data for this block.
                                    found = true;
                                    break;
                                }
                            }
                        }
                        found
                    }
                };

                if !has_s3_data {
                    // No prior data in S3 — just write the sub-block directly.
                    self.cache.write(write_start, &data[data_offset..data_offset + write_len])?;
                    break 'block_retry;
                }

                // Fetch prior data from S3 BEFORE claiming.
                let prior = self.cache.resolve_block_for_backfill(
                    idx,
                    self.clean_cache.as_ref(),
                    &self.pack_index_cache,
                    &self.volume_manifest,
                    &self.content_store,
                    Some(&self.metrics),
                ).await.map_err(|e| {
                    tracing::warn!(
                        block = idx,
                        error = %e,
                        "S3 backfill failed for sub-block write — rejecting write to prevent data corruption"
                    );
                    CommandError::IoError
                })?;

                _backfill_gate!(BackfillStep::S3FetchDone, idx, self.cache.block_state(idx).raw());

                // Re-check state after the async fetch. Another writer or
                // flush rotation may have changed the block state.
                if !self.cache.block_state(idx).is_not_present() {
                    // State changed during fetch — re-enter outer match.
                    continue 'block_retry;
                }

                // If prior data is all zeros, just write the sub-block.
                if prior.is_empty() || prior.iter().all(|&b| b == 0) {
                    self.cache.write(write_start, &data[data_offset..data_offset + write_len])?;
                    break 'block_retry;
                }

                _backfill_gate!(BackfillStep::BeforeCas, idx, crate::block::block_map::SparseBlockState::NOT_PRESENT);

                // Non-zero prior: claim the block, merge, and write.
                if !self.cache.try_claim_block(idx) {
                    // Another writer claimed it. Re-enter outer match to
                    // wait for their write to complete.
                    continue 'block_retry;
                }

                _backfill_gate!(BackfillStep::AfterCas, idx, self.cache.block_state(idx).raw());

                // We won the claim. Merge guest data onto prior block.
                let mut block_buf = prior.to_vec();
                if block_buf.len() != block_size {
                    tracing::error!(
                        block = idx,
                        expected = block_size,
                        actual = block_buf.len(),
                        "backfill returned truncated block"
                    );
                    return Err(CommandError::IoError);
                }
                let end = (block_local_start + write_len).min(block_buf.len());
                block_buf[block_local_start..end]
                    .copy_from_slice(&data[data_offset..data_offset + write_len]);

                _backfill_gate!(BackfillStep::BeforeWrite, idx, self.cache.block_state(idx).raw());

                self.cache.write(block_start, &block_buf)?;
                break 'block_retry;
            }
        }

        Ok(())
    }

    // ========================================================================
    // Block I/O Operations
    // ========================================================================

    /// Read data from the cache, fetching from S3 if not present locally.
    ///
    /// Uses read-through caching: blocks not present locally are fetched from S3.
    pub async fn read(&self, offset: u64, length: u32) -> CommandResult<Bytes> {
        let start = Instant::now();

        if offset >= self.device_size() {
            // Entirely beyond the device — return zeros. The kernel NBD
            // driver doesn't clamp requests to the device size for partition
            // table probing, so we must handle this gracefully.
            return Ok(Bytes::from(vec![0u8; length as usize]));
        }

        if length == 0 {
            return Ok(Bytes::new());
        }

        // Clamp reads that partially extend past the device boundary:
        // return real data for the on-device portion, zeros for the rest.
        let clamped_len = std::cmp::min(
            length as u64,
            self.device_size() - offset,
        ) as u32;

        self.metrics.record_guest_read(clamped_len as u64);

        let data = self
            .cache
            .read(
                offset,
                clamped_len as usize,
                self.clean_cache.as_ref(),
                &self.pack_index_cache,
                &self.volume_manifest,
                &self.content_store,
                &self.metrics,
            )
            .await?;

        self.trigger_readahead(offset);

        self.metrics.record_read_latency(start.elapsed());

        // Zero-pad if we clamped the read at the device boundary.
        if clamped_len < length {
            let mut padded = BytesMut::with_capacity(length as usize);
            padded.extend_from_slice(&data);
            padded.resize(length as usize, 0);
            Ok(padded.freeze())
        } else {
            Ok(data)
        }
    }

    /// Read data directly into a caller-provided buffer.
    ///
    /// Same as `read()` but writes directly into `buf` instead of returning `Bytes`,
    /// eliminating the intermediate allocation in the multi-chunk path.
    /// Used by the ublk transport where the kernel provides the destination buffer.
    #[cfg_attr(not(all(target_os = "linux", feature = "ublk")), allow(dead_code))]
    pub async fn read_into(
        &self,
        offset: u64,
        length: u32,
        buf: &mut [u8],
    ) -> CommandResult<usize> {
        let start = Instant::now();

        // Match read() behavior: OOB reads return zeros instead of errors.
        // The kernel sends reads past the device boundary during partition
        // table probing on both NBD and ublk transports.
        if offset >= self.device_size() {
            buf[..length as usize].fill(0);
            return Ok(length as usize);
        }

        if length == 0 {
            return Ok(0);
        }

        // Clamp reads that partially extend past the device boundary.
        let clamped_len = std::cmp::min(
            length as u64,
            self.device_size() - offset,
        ) as u32;

        self.metrics.record_guest_read(clamped_len as u64);

        // Fast path: all blocks present on local SSD → pread directly into
        // caller buffer. Zero allocation, zero memcpy.
        if let Some(result) = self.cache.try_pread_local(offset, clamped_len as usize, buf) {
            let n = result?;
            // Zero-pad if we clamped.
            if clamped_len < length {
                buf[n..length as usize].fill(0);
            }
            self.trigger_readahead(offset);
            self.metrics.record_read_latency(start.elapsed());
            return Ok(length as usize);
        }

        let data = self
            .cache
            .read(
                offset,
                clamped_len as usize,
                self.clean_cache.as_ref(),
                &self.pack_index_cache,
                &self.volume_manifest,
                &self.content_store,
                &self.metrics,
            )
            .await?;
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);

        // Zero-pad if we clamped.
        if clamped_len < length {
            buf[n..length as usize].fill(0);
        }

        self.trigger_readahead(offset);

        self.metrics.record_read_latency(start.elapsed());
        Ok(length as usize)
    }

    /// Write data to the cache.
    ///
    /// Writes go to local SSD immediately. S3 sync happens in background.
    /// Returns error if the export is readonly or SSD is near-full and
    /// the write touches blocks not yet present on SSD.
    pub async fn write(&self, offset: u64, data: &[u8], fua: bool) -> CommandResult<()> {
        let start = Instant::now();

        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset.checked_add(data.len() as u64).is_none_or(|end| end > self.device_size()) {
            return Err(CommandError::InvalidArgument);
        }

        if data.is_empty() {
            return Ok(());
        }

        let util = f64::from_bits(self.ssd_utilization.load(Ordering::Relaxed));
        if util > WRITE_REJECT_THRESHOLD && self.cache.has_new_blocks(offset, data.len()) {
            return Err(CommandError::NoSpace);
        }

        self.metrics.record_guest_write(data.len() as u64);

        // Backfill NOT_PRESENT blocks that receive sub-block writes.
        //
        // When a forked export receives its first write to a block, the local
        // file has no data (it lives in S3 via parent packs). If the write
        // doesn't cover the full block, fetch the complete block from
        // foyer/S3, overlay the guest's sub-block data in memory, and write
        // the merged block. The local file always has complete data —
        // no sparse holes, no merge needed at flush or read time.
        //
        // Cost: one backfill per block per eviction cycle. Hot blocks pay
        // once after each flush; cold blocks pay once ever.
        //
        self.backfill_and_write(offset, data).await?;

        if let Some(ref tracer) = self.write_tracer {
            tracer.record(
                offset,
                data.len() as u64,
                super::write_trace::TraceOp::Write,
            );
        }
        self.check_flush_threshold();

        if fua {
            self.flush()?;
        }

        self.metrics.record_write_latency(start.elapsed());
        Ok(())
    }

    /// Trim (discard) a range of blocks.
    ///
    /// Writes zeros to the specified range using optimized platform-specific
    /// methods (fallocate on Linux, static buffer fallback elsewhere).
    /// Returns error if the export is readonly.
    pub async fn trim(&self, offset: u64, length: u32, fua: bool) -> CommandResult<()> {
        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset.checked_add(length as u64).is_none_or(|end| end > self.device_size()) {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        self.backfill_blocks_in_range(offset, length as u64).await?;
        self.cache.zero_range(offset, length as u64)?;

        if let Some(ref tracer) = self.write_tracer {
            tracer.record(offset, length as u64, super::write_trace::TraceOp::Trim);
        }
        self.check_flush_threshold();

        if fua {
            self.flush()?;
        }

        Ok(())
    }

    /// Write zeros to a range.
    ///
    /// Uses optimized platform-specific methods:
    /// - Linux: fallocate(FALLOC_FL_ZERO_RANGE) - no data written, kernel marks as zeros
    /// - Other: Static zero buffer to avoid per-call allocation
    ///
    /// Returns error if the export is readonly.
    pub async fn write_zeroes(&self, offset: u64, length: u32, fua: bool) -> CommandResult<()> {
        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset.checked_add(length as u64).is_none_or(|end| end > self.device_size()) {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        self.backfill_blocks_in_range(offset, length as u64).await?;
        self.cache.zero_range(offset, length as u64)?;

        if let Some(ref tracer) = self.write_tracer {
            tracer.record(
                offset,
                length as u64,
                super::write_trace::TraceOp::WriteZeroes,
            );
        }
        self.check_flush_threshold();

        if fua {
            self.flush()?;
        }

        Ok(())
    }

    /// Cache hint - no-op for our implementation.
    pub fn cache(&self, offset: u64, length: u32) -> CommandResult<()> {
        if offset.checked_add(length as u64).is_none_or(|end| end > self.device_size()) {
            return Err(CommandError::InvalidArgument);
        }
        Ok(())
    }

    /// Flush data to durable storage.
    ///
    /// **CRITICAL**: This only flushes to local SSD, not to S3!
    /// This is the key performance optimization - ZFS snapshots/clones
    /// return in <100ms instead of 8-15 seconds.
    ///
    /// S3 sync happens asynchronously in the background via the sync worker.
    pub fn flush(&self) -> CommandResult<()> {
        self.cache.flush().map_err(CommandError::from)
    }

    // ========================================================================
    // ublk zero-copy bridge methods
    // ========================================================================

    /// Get the raw fd of the data file (for io_uring fixed-file registration).
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn data_file_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.cache.inner().data_file_fd()
    }

    /// Phase 1 of ublk zero-copy write: prepare metadata before data write.
    ///
    /// Validates the request (readonly, SSD-full, bounds), then marks blocks
    /// present and clears CRC32. Call BEFORE the data write.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn pre_write(&self, offset: u64, length: u64) -> CommandResult<()> {
        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset.checked_add(length).is_none_or(|end| end > self.device_size()) {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        let util = f64::from_bits(self.ssd_utilization.load(Ordering::Relaxed));
        if util > WRITE_REJECT_THRESHOLD && self.cache.has_new_blocks(offset, length as usize) {
            return Err(CommandError::NoSpace);
        }

        // Backfill NOT_PRESENT blocks before the io_uring pwrite lands.
        // Phase 2 (pwrite_and_commit) is synchronous and can't fetch from S3.
        self.backfill_blocks_in_range(offset, length).await?;
        self.cache.pre_write(offset, length)?;

        // Guard against a flush completing between backfill and
        // pwrite_and_commit. If any block was SYNCING during backfill
        // (skipped as "present"), the flush may have evicted it
        // (SYNCING→NOT_PRESENT) and taken the flushing file by now.
        // pwrite_and_commit can't recover from this (sync, no S3 access).
        // Re-backfill any blocks that were evicted during the gap.
        {
            let block_size = self.cache.block_size() as u64;
            let start_block = offset / block_size;
            let end_block = (offset + length - 1) / block_size;
            for block in start_block..=end_block {
                let idx = block as usize;
                let state = self.cache.block_state(idx);
                if state.is_syncing() || state.is_not_present() {
                    // Block was evicted or is still being flushed.
                    // Re-backfill to ensure it's present before pwrite.
                    self.backfill_blocks_in_range(
                        block * block_size,
                        block_size,
                    )
                    .await?;
                    self.cache.pre_write(block * block_size, block_size)?;
                }
            }
        }

        Ok(())
    }

    /// Phase 2 of ublk zero-copy write: pwrite data + commit metadata atomically.
    ///
    /// Holds the data_file read lock across both pwrite and dirty marking to
    /// prevent rotate_and_snapshot() from interleaving. Without this, rotation
    /// can snapshot dirty blocks between pwrite and transition_to_dirty, leaving
    /// the block's data stranded in the flushing file (deleted after flush).
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn pwrite_and_commit(
        &self,
        offset: u64,
        data: &[u8],
        fua: bool,
    ) -> CommandResult<()> {
        let length = data.len() as u64;
        if length == 0 {
            return Ok(());
        }

        let start = Instant::now();

        self.metrics.record_guest_write(length);
        self.cache
            .pwrite_and_commit(offset, data)
            .map_err(|e| match e {
                CacheError::BlockEvicted => CommandError::BlockEvicted,
                other => {
                    tracing::warn!(error = %other, "pwrite_and_commit failed");
                    CommandError::IoError
                }
            })?;

        if let Some(ref tracer) = self.write_tracer {
            tracer.record(offset, length, super::write_trace::TraceOp::Write);
        }
        self.check_flush_threshold();

        if fua {
            self.flush()?;
        }

        self.metrics.record_write_latency(start.elapsed());
        Ok(())
    }

    /// Build a read plan for the ublk zero-copy path.
    ///
    /// Returns a `ReadPlan` describing where each chunk's data lives,
    /// so the caller can issue io_uring reads for local chunks and memcpy
    /// for in-memory chunks directly into bio pages.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn resolve_read(
        &self,
        offset: u64,
        length: u32,
    ) -> CommandResult<super::write_cache::ReadPlan> {
        if offset.checked_add(length as u64).is_none_or(|end| end > self.device_size()) {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(super::write_cache::ReadPlan {
                entries: Vec::new(),
            });
        }

        self.metrics.record_guest_read(length as u64);

        let plan = self
            .cache
            .resolve_read_plan(
                offset,
                length as usize,
                self.clean_cache.as_ref(),
                &self.pack_index_cache,
                &self.volume_manifest,
                &self.content_store,
                &self.metrics,
            )
            .await?;

        Ok(plan)
    }

    /// Trigger sequential readahead detection and prefetch.
    ///
    /// Detects sequential access patterns and spawns a background prefetch
    /// for the next chunk. Used by the NBD read path and ublk zero-copy path.
    pub fn trigger_readahead(&self, offset: u64) {
        let block_size = self.cache.block_size() as u64;
        let block_idx = offset / block_size;
        if let Some(readahead_block) = self.readahead.lock().record(block_idx) {
            // Compute the v4 chunk_idx for the readahead block
            let chunk_idx = self
                .volume_manifest
                .read()
                .chunk_idx_for_block(readahead_block);
            let cache = Arc::clone(&self.cache);
            let pack_index_cache = Arc::clone(&self.pack_index_cache);
            let volume_manifest = Arc::clone(&self.volume_manifest);
            let content_store = Arc::clone(&self.content_store);
            tokio::spawn(async move {
                let _ = cache
                    .prefetch_chunk(
                        chunk_idx,
                        &pack_index_cache,
                        &volume_manifest,
                        &content_store,
                    )
                    .await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::cache::SimpleBlockCache;
    use crate::block::pack::DEFAULT_BLOCKS_PER_PACK;
    use crate::block::pack_index_cache::PackIndexCache;
    use crate::block::volume_manifest::VolumeManifest;
    use crate::block::write_cache::WriteCacheConfig;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    async fn test_handler() -> (BlockHandler, TempDir) {
        test_handler_with_readonly(false).await
    }

    async fn test_handler_with_readonly(readonly: bool) -> (BlockHandler, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "test".to_string(),
            device_size: 1024 * 1024, // 1MB
            block_size: 4096,
            wal_sync: false,
        };

        // Create in-memory S3 store for tests
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(
            VolumeManifest::new(1024 * 1024, 4096),
        ));

        // Create metrics for this handler
        let metrics = Arc::new(ExportMetrics::new());

        // open() returns WriteCache<Recovering>
        let cache = WriteCache::open(config).unwrap();
        // Skip recovery for test - go straight to active
        let cache = cache.skip_recovery_for_test();
        let handler = BlockHandler::new(
            Arc::new(cache),
            content_store,
            clean_cache,
            pack_index_cache,
            volume_manifest,
            1024 * 1024,
            readonly,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::new(Notify::const_new()),
            DEFAULT_BLOCKS_PER_PACK,
            None,
        );

        (handler, temp_dir)
    }

    #[tokio::test]
    async fn test_read_write() {
        let (handler, _temp) = test_handler().await;

        let data = vec![42u8; 4096];
        handler.write(0, &data, false).await.unwrap();

        let read_data = handler.read(0, 4096).await.unwrap();
        assert_eq!(read_data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_read_unwritten_returns_zeros() {
        let (handler, _temp) = test_handler().await;

        // Unwritten blocks should fetch from S3 (empty) and return zeros
        let data = handler.read(0, 1024).await.unwrap();
        assert!(data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_write_beyond_device_size() {
        let (handler, _temp) = test_handler().await;

        let data = vec![42u8; 4096];
        let result = handler.write(1024 * 1024, &data, false).await;
        assert!(matches!(result, Err(CommandError::InvalidArgument)));
    }

    #[tokio::test]
    async fn test_readonly_rejects_write() {
        let (handler, _temp) = test_handler_with_readonly(true).await;

        let data = vec![42u8; 4096];
        let result = handler.write(0, &data, false).await;
        assert!(matches!(result, Err(CommandError::ReadOnly)));
    }

    #[tokio::test]
    async fn test_readonly_rejects_trim() {
        let (handler, _temp) = test_handler_with_readonly(true).await;

        let result = handler.trim(0, 4096, false).await;
        assert!(matches!(result, Err(CommandError::ReadOnly)));
    }

    #[tokio::test]
    async fn test_readonly_rejects_write_zeroes() {
        let (handler, _temp) = test_handler_with_readonly(true).await;

        let result = handler.write_zeroes(0, 4096, false).await;
        assert!(matches!(result, Err(CommandError::ReadOnly)));
    }

    #[tokio::test]
    async fn test_readonly_allows_read() {
        let (handler, _temp) = test_handler_with_readonly(true).await;

        // Reads should still work on readonly exports
        let result = handler.read(0, 1024).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_promote_readonly_to_readwrite() {
        let (handler, _temp) = test_handler_with_readonly(true).await;

        // Initially readonly - writes fail
        let data = vec![42u8; 4096];
        assert!(matches!(
            handler.write(0, &data, false).await,
            Err(CommandError::ReadOnly)
        ));

        // Promote to read-write
        handler.set_readonly(false);
        assert!(!handler.is_readonly());

        // Now writes work
        assert!(handler.write(0, &data, false).await.is_ok());
    }

    #[tokio::test]
    async fn test_read_beyond_device_size_returns_zeros() {
        let (handler, _temp) = test_handler().await;

        // Reads beyond device boundary return zeros (not an error).
        // The kernel NBD driver sends partition-probe reads past the
        // device size, so we must handle this gracefully.
        let data = handler.read(1024 * 1024, 4096).await.unwrap();
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_flush() {
        let (handler, _temp) = test_handler().await;

        let data = vec![42u8; 4096];
        handler.write(0, &data, false).await.unwrap();
        handler.flush().unwrap();

        // Verify data persists
        let read_data = handler.read(0, 4096).await.unwrap();
        assert_eq!(read_data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_write_with_fua() {
        let (handler, _temp) = test_handler().await;

        let data = vec![42u8; 4096];
        // FUA flag should trigger flush
        handler.write(0, &data, true).await.unwrap();

        let read_data = handler.read(0, 4096).await.unwrap();
        assert_eq!(read_data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_write_zeroes() {
        let (handler, _temp) = test_handler().await;

        // Write some data
        let data = vec![42u8; 4096];
        handler.write(0, &data, false).await.unwrap();

        // Write zeros over it
        handler.write_zeroes(0, 4096, false).await.unwrap();

        // Verify zeros
        let read_data = handler.read(0, 4096).await.unwrap();
        assert!(read_data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_trim() {
        let (handler, _temp) = test_handler().await;

        // Write some data
        let data = vec![42u8; 4096];
        handler.write(0, &data, false).await.unwrap();

        // Trim the region
        handler.trim(0, 4096, false).await.unwrap();

        // Verify zeros
        let read_data = handler.read(0, 4096).await.unwrap();
        assert!(read_data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_write_zeroes_with_fua() {
        let (handler, _temp) = test_handler().await;

        // Write non-zero data
        let data = vec![0xABu8; 4096];
        handler.write(0, &data, false).await.unwrap();

        // Write zeros with FUA — should trigger flush and persist zeros
        handler.write_zeroes(0, 4096, true).await.unwrap();

        // Verify zeros survived the FUA flush
        let read_data = handler.read(0, 4096).await.unwrap();
        assert!(
            read_data.iter().all(|&b| b == 0),
            "write_zeroes with FUA should produce durable zeros"
        );
    }

    #[tokio::test]
    async fn test_write_zeroes_sub_block() {
        let (handler, _temp) = test_handler().await;

        // Write a full block of non-zero data
        let data = vec![0xCDu8; 4096];
        handler.write(0, &data, false).await.unwrap();

        // Write zeros to only the first 512 bytes
        handler.write_zeroes(0, 512, false).await.unwrap();

        // Read back the full block
        let read_data = handler.read(0, 4096).await.unwrap();

        // First 512 bytes must be zeros
        assert!(
            read_data[..512].iter().all(|&b| b == 0),
            "first 512 bytes should be zeroed"
        );
        // Remaining bytes must be unchanged
        assert!(
            read_data[512..].iter().all(|&b| b == 0xCD),
            "bytes after the zeroed region should be unchanged"
        );
    }

    #[tokio::test]
    async fn test_readonly_flush_succeeds() {
        let (handler, _temp) = test_handler_with_readonly(true).await;

        // FLUSH on a readonly export should succeed (local SSD sync is harmless)
        assert!(handler.flush().is_ok(), "flush on readonly export must succeed");
    }

    #[tokio::test]
    async fn test_zero_length_read() {
        let (handler, _temp) = test_handler().await;

        // Zero-length READ should succeed with empty response
        let result = handler.read(0, 0).await.unwrap();
        assert!(result.is_empty(), "zero-length read should return empty bytes");
    }

    #[tokio::test]
    async fn test_zero_length_write() {
        let (handler, _temp) = test_handler().await;

        // Zero-length WRITE should succeed as a no-op
        handler.write(0, &[], false).await.unwrap();

        // Verify nothing was written (still zeros)
        let read_data = handler.read(0, 4096).await.unwrap();
        assert!(read_data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_max_payload_write() {
        // Create a handler with a 1MB device and write the entire device in one call.
        // This exercises the handler's ability to accept a large write spanning all blocks.
        let (handler, _temp) = test_handler().await;

        let device_size = 1024 * 1024usize; // 1MB = 256 blocks × 4096
        let data: Vec<u8> = (0..device_size).map(|i| (i % 251) as u8).collect();
        handler.write(0, &data, false).await.unwrap();

        // Read back in full and verify
        let read_data = handler.read(0, device_size as u32).await.unwrap();
        assert_eq!(read_data.as_ref(), &data[..]);
    }

    // =========================================================================
    // Per-export flush threshold tests
    // =========================================================================

    /// Helper: create a handler with a specific blocks_per_pack and a shared
    /// Notify so we can observe whether auto-flush was triggered.
    async fn test_handler_with_flush_config(
        blocks_per_pack: usize,
    ) -> (BlockHandler, Arc<Notify>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        // 256 blocks × 4096 = 1MB device, enough for threshold tests
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "flush-test".to_string(),
            device_size: 1024 * 1024,
            block_size: 4096,
            wal_sync: false,
        };

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(
            VolumeManifest::new(1024 * 1024, 4096),
        ));
        let metrics = Arc::new(ExportMetrics::new());
        let flush_notify = Arc::new(Notify::new());

        let cache = WriteCache::open(config).unwrap();
        let cache = cache.skip_recovery_for_test();
        let handler = BlockHandler::new(
            Arc::new(cache),
            content_store,
            clean_cache,
            pack_index_cache,
            volume_manifest,
            1024 * 1024,
            false,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::clone(&flush_notify),
            blocks_per_pack,
            None,
        );

        (handler, flush_notify, temp_dir)
    }

    #[tokio::test]
    async fn test_manual_mode_never_notifies() {
        // blocks_per_pack = 0 → manual mode, no auto-flush
        let (handler, flush_notify, _temp) = test_handler_with_flush_config(0).await;

        // Write 200 blocks — well above any reasonable threshold
        for i in 0..200u64 {
            handler.write(i * 4096, &[0xAA; 4096], false).await.unwrap();
        }

        // flush_notify should NOT have been triggered.
        // Notify doesn't have try_recv, so we check via a zero-timeout wait.
        let was_notified = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            flush_notify.notified(),
        )
        .await
        .is_ok();
        assert!(
            !was_notified,
            "manual mode (blocks_per_pack=0) should never notify flush scheduler"
        );
    }

    #[tokio::test]
    async fn test_custom_threshold_triggers_at_configured_value() {
        // blocks_per_pack = 5 → auto-flush after 5 dirty blocks
        let (handler, flush_notify, _temp) = test_handler_with_flush_config(5).await;

        // Write 4 blocks — below threshold, should NOT notify
        for i in 0..4u64 {
            handler.write(i * 4096, &[0xBB; 4096], false).await.unwrap();
        }

        let notified_early = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            flush_notify.notified(),
        )
        .await
        .is_ok();
        assert!(!notified_early, "should not notify below threshold (4 < 5)");

        // Write the 5th block — reaches threshold, SHOULD notify
        handler.write(4 * 4096, &[0xCC; 4096], false).await.unwrap();

        let notified_at_threshold = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            flush_notify.notified(),
        )
        .await
        .is_ok();
        assert!(
            notified_at_threshold,
            "should notify when dirty blocks reach threshold (5 >= 5)"
        );
    }

    #[tokio::test]
    async fn test_default_threshold_matches_default_blocks_per_pack() {
        // Verify the default handler uses DEFAULT_BLOCKS_PER_PACK
        let (handler, _temp) = test_handler().await;
        assert_eq!(
            handler.blocks_per_pack, DEFAULT_BLOCKS_PER_PACK,
            "default handler should use DEFAULT_BLOCKS_PER_PACK"
        );
    }

    // =========================================================================
    // SSD pressure / ENOSPC tests
    // =========================================================================

    /// Helper: create a handler and return the shared SSD utilization atomic
    /// so tests can simulate disk pressure.
    async fn test_handler_with_ssd_util() -> (BlockHandler, Arc<AtomicU64>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "enospc-test".to_string(),
            device_size: 1024 * 1024,
            block_size: 4096,
            wal_sync: false,
        };

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(
            VolumeManifest::new(1024 * 1024, 4096),
        ));
        let metrics = Arc::new(ExportMetrics::new());
        let ssd_util = Arc::new(AtomicU64::new(0f64.to_bits()));

        let cache = WriteCache::open(config).unwrap();
        let cache = cache.skip_recovery_for_test();
        let handler = BlockHandler::new(
            Arc::new(cache),
            content_store,
            clean_cache,
            pack_index_cache,
            volume_manifest,
            1024 * 1024,
            false,
            metrics,
            Arc::clone(&ssd_util),
            Arc::new(Notify::const_new()),
            DEFAULT_BLOCKS_PER_PACK,
            None,
        );

        (handler, ssd_util, temp_dir)
    }

    // =========================================================================
    // Backfill data corruption test
    // =========================================================================

    /// Object store wrapper that can selectively fail GETs to simulate S3 outages.
    #[derive(Debug)]
    struct FailingObjectStore {
        inner: InMemory,
        fail_gets: std::sync::atomic::AtomicBool,
    }

    impl FailingObjectStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                fail_gets: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn set_fail_gets(&self, fail: bool) {
            self.fail_gets
                .store(fail, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl std::fmt::Display for FailingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingObjectStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FailingObjectStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if self.fail_gets.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "FailingObjectStore",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "Simulated S3 GET failure",
                    )),
                });
            }
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// A sub-block write to an evicted block with S3 data must fail (not
    /// silently corrupt) when S3 is unreachable. Without the fix, the
    /// backfill error was swallowed and zeros were substituted for the
    /// non-written portion of the block.
    #[tokio::test]
    async fn test_subblock_write_fails_when_s3_backfill_unavailable() {
        let temp_dir = TempDir::new().unwrap();
        let s3 = Arc::new(FailingObjectStore::new());
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "backfill-test".to_string(),
            device_size: 1024 * 1024, // 1MB
            block_size: 4096,
            wal_sync: false,
        };

        let content_store = Arc::new(ContentStore::new(
            Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>,
            "test",
        ));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            1024 * 1024,
            4096,
        )));
        let metrics = Arc::new(ExportMetrics::new());

        let cache = WriteCache::open(config).unwrap();
        let cache = cache.skip_recovery_for_test();
        let cache = Arc::new(cache);

        let handler = BlockHandler::new(
            Arc::clone(&cache),
            Arc::clone(&content_store),
            Arc::clone(&clean_cache),
            Arc::clone(&pack_index_cache),
            Arc::clone(&volume_manifest),
            1024 * 1024,
            false,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::new(Notify::const_new()),
            DEFAULT_BLOCKS_PER_PACK,
            None,
        );

        // Step 1: Write a full block of 0xAA.
        let original_data = vec![0xAAu8; 4096];
        handler.write(0, &original_data, false).await.unwrap();

        // Step 2: Flush + drain to S3 (evicts block from local SSD).
        cache
            .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
            .await
            .unwrap();
        assert!(
            !cache.is_block_present(0),
            "block should be evicted after drain"
        );

        // Step 3: Fail all S3 GETs.
        s3.set_fail_gets(true);

        // Step 4: Sub-block write must return an error — not silently corrupt.
        let sub_block = vec![0xBBu8; 512];
        let result = handler.write(0, &sub_block, false).await;
        assert!(
            result.is_err(),
            "sub-block write must fail when S3 backfill is unavailable"
        );

        // Step 5: Re-enable S3 and verify original data is intact.
        s3.set_fail_gets(false);
        let read_back = handler.read(0, 4096).await.unwrap();
        assert_eq!(
            &read_back[..],
            &original_data[..],
            "original S3 data must be preserved after failed write"
        );
    }

    /// Sub-block write succeeds and preserves surrounding data when S3 is
    /// reachable — the backfill correctly merges guest data with prior S3 data.
    #[tokio::test]
    async fn test_subblock_write_preserves_data_with_working_s3() {
        let temp_dir = TempDir::new().unwrap();
        let s3 = Arc::new(FailingObjectStore::new());
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "backfill-ok-test".to_string(),
            device_size: 1024 * 1024,
            block_size: 4096,
            wal_sync: false,
        };

        let content_store = Arc::new(ContentStore::new(
            Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>,
            "test",
        ));
        let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            1024 * 1024,
            4096,
        )));
        let metrics = Arc::new(ExportMetrics::new());

        let cache = WriteCache::open(config).unwrap();
        let cache = cache.skip_recovery_for_test();
        let cache = Arc::new(cache);

        let handler = BlockHandler::new(
            Arc::clone(&cache),
            Arc::clone(&content_store),
            Arc::clone(&clean_cache),
            Arc::clone(&pack_index_cache),
            Arc::clone(&volume_manifest),
            1024 * 1024,
            false,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::new(Notify::const_new()),
            DEFAULT_BLOCKS_PER_PACK,
            None,
        );

        // Write full block, flush+drain to S3, evict locally.
        let original_data = vec![0xAAu8; 4096];
        handler.write(0, &original_data, false).await.unwrap();
        cache
            .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
            .await
            .unwrap();
        assert!(!cache.is_block_present(0));

        // Sub-block write with S3 working — should succeed and merge.
        let sub_block = vec![0xBBu8; 512];
        handler.write(0, &sub_block, false).await.unwrap();

        let result = handler.read(0, 4096).await.unwrap();
        assert_eq!(
            &result[..512],
            &[0xBBu8; 512][..],
            "sub-block write data should be present"
        );
        assert_eq!(
            &result[512..],
            &vec![0xAAu8; 4096 - 512][..],
            "remaining bytes must be original S3 data, not zeros"
        );
    }

    /// At >95% SSD utilization, writes to NEW blocks return ENOSPC while
    /// overwrites to existing blocks are still allowed (they don't grow the
    /// data file). Lowering utilization re-enables new-block writes.
    #[tokio::test]
    async fn test_enospc_rejects_new_blocks_allows_overwrites() {
        let (handler, ssd_util, _temp) = test_handler_with_ssd_util().await;

        // Write block 0 at normal pressure — establishes it as "present" on SSD
        handler.write(0, &[0xAA; 4096], false).await.unwrap();

        // Simulate 96% SSD utilization (above WRITE_REJECT_THRESHOLD of 0.95)
        ssd_util.store(0.96f64.to_bits(), Ordering::Relaxed);

        // Overwrite block 0 — should succeed (existing block, no new SSD allocation)
        assert!(
            handler.write(0, &[0xBB; 4096], false).await.is_ok(),
            "overwrite of existing block should succeed even at 96% utilization"
        );

        // Write to block 1 — should fail (new block requires SSD allocation)
        assert!(
            matches!(handler.write(4096, &[0xCC; 4096], false).await, Err(CommandError::NoSpace)),
            "write to new block should return ENOSPC at 96% utilization"
        );

        // Lower utilization back to normal
        ssd_util.store(0.50f64.to_bits(), Ordering::Relaxed);

        // Write to block 1 again — should succeed now
        assert!(
            handler.write(4096, &[0xDD; 4096], false).await.is_ok(),
            "write to new block should succeed after pressure drops"
        );
    }
}
