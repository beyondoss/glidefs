//! Transport-agnostic block I/O handler.
//!
//! This module provides:
//! - `BlockHandler`: Thin handler that uses WriteCache for all I/O
//! - `BlockDevice`: Device descriptor used during transmission phase

use super::cache::BlockCache;
use super::content_store::ContentStore;
use super::error::{CommandError, CommandResult};
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
        }
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
        if self.blocks_per_pack > 0 && self.cache.dirty_block_count() >= self.blocks_per_pack as u64
        {
            self.flush_notify.notify_one();
        }
    }

    // ========================================================================
    // Block I/O Operations
    // ========================================================================

    /// Prepare cache blocks for sub-block writes.
    ///
    /// For writes smaller than the cache block size, if the affected block
    /// exists in S3 but not on local SSD, we track it as a "partial block"
    /// and spawn a background task to fetch the full block from S3. The
    /// write proceeds immediately on the SSD without waiting for S3.
    ///
    /// The bitmap in partial_blocks tracks which 4KB sub-regions have valid
    /// guest data. Background backfill fills unwritten sub-regions from S3.
    /// The read and flush paths are aware of partial blocks and handle them
    /// correctly.
    ///
    /// Falls back to synchronous S3 fetch when:
    /// - MAX_PARTIAL_BLOCKS cap is reached (memory bound)
    /// - Block is already fully present and not partial
    /// - Write fully covers the block (no preservation needed)
    /// - Block has no S3 data (fresh block, zeros are fine)
    /// Prepare partial-block tracking for sub-block writes.
    ///
    /// Returns the list of block indices that need background backfill.
    /// The caller MUST spawn background backfill AFTER the guest write completes,
    /// so the bitmap bits are set and the pwrite is done before the backfill
    /// task can race with the written data.
    async fn backfill_missing_blocks(&self, offset: u64, length: u64) -> CommandResult<Vec<u64>> {
        use super::write_cache::inner::{MAX_PARTIAL_BLOCKS, PartialBlockState};

        let block_size = self.cache.block_size() as u64;
        let start_block = offset / block_size;
        let end_block = (offset + length - 1) / block_size;
        let mut needs_backfill = Vec::new();

        for block_idx in start_block..=end_block {
            let idx = block_idx as usize;

            // Fast path: block is fully present on local SSD (not partial)
            if self.cache.is_block_present(idx) && !self.cache.inner().is_partial(idx) {
                continue;
            }

            // Already tracked as partial — bitmap will be updated in write path
            if self.cache.inner().is_partial(idx) {
                continue;
            }

            // Check if the write fully covers this block — no preservation needed
            let block_start = block_idx * block_size;
            let block_end = block_start + block_size;
            if offset <= block_start && offset + length >= block_end {
                continue;
            }

            // Check if THIS SPECIFIC BLOCK has S3 data that needs preserving.
            //
            // Must check per-block, not per-chunk: after the flush scheduler runs,
            // the chunk has pack_ids for previously-flushed blocks, but THIS block
            // (which is getting its first write) was never in any pack. A chunk-level
            // check falsely marks such blocks as partial, causing compute_flush_batch
            // to skip them (partial blocks are skipped to avoid flushing incomplete
            // backfill data). Under the right timing, this leads to data loss.
            let has_s3_data = {
                let (chunk_idx, chunk_offset, pack_ids) = {
                    let vm = self.volume_manifest.read();
                    let ci = vm.chunk_idx_for_block(block_idx);
                    let co = vm.block_offset_in_chunk(block_idx);
                    let pids = vm
                        .chunk_pack_ids(ci)
                        .map(|ids| ids.to_vec())
                        .unwrap_or_default();
                    (ci, co, pids)
                };
                if pack_ids.is_empty() {
                    false
                } else {
                    // Ensure pack indices are loaded into cache (they may not be
                    // if this is a forked export and no reads have happened yet).
                    for &pid in &pack_ids {
                        if self.pack_index_cache.get_entries(pid).await.is_none() {
                            match self.content_store.get_pack_index(chunk_idx, pid).await {
                                Ok(entries) => {
                                    self.pack_index_cache.insert_entries(pid, &entries);
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        pack_id = pid,
                                        error = %e,
                                        "failed to fetch pack index for has_s3_data check"
                                    );
                                }
                            }
                        }
                    }

                    // Search packs newest-first for this block's chunk_offset.
                    let mut found = false;
                    for &pid in pack_ids.iter().rev() {
                        if self
                            .pack_index_cache
                            .lookup_block(pid, chunk_offset)
                            .await
                            .is_some()
                        {
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

            // Check MAX_PARTIAL_BLOCKS cap — fall back to sync backfill if exceeded
            if self.cache.inner().partial_blocks.len() >= MAX_PARTIAL_BLOCKS {
                // Synchronous fallback: fetch full block from S3
                let block_data = self
                    .cache
                    .read(
                        block_start,
                        block_size as usize,
                        self.clean_cache.as_ref(),
                        &self.pack_index_cache,
                        &self.volume_manifest,
                        &self.content_store,
                        &self.metrics,
                    )
                    .await?;
                self.cache.backfill_block(idx, &block_data)?;
                continue;
            }

            // Insert as partial block with empty bitmap (no sub-regions valid yet).
            // The write path will set bits for sub-regions it writes.
            // Use entry().or_insert_with() — NOT insert() — to avoid replacing
            // an existing AtomicU32 if a concurrent write already inserted one.
            // DashMap::insert would drop the existing AtomicU32 and any bitmap
            // bits set by the other write's mark_sub_regions.
            self.cache
                .inner()
                .partial_blocks
                .entry(idx)
                .or_insert_with(|| PartialBlockState {
                    bitmap: std::sync::atomic::AtomicU32::new(0),
                    write_lock: parking_lot::Mutex::new(()),
                });

            needs_backfill.push(block_idx);
        }

        Ok(needs_backfill)
    }

    /// Spawn a background task to backfill a partial block from S3.
    ///
    /// Fetches the full block from S3, writes unwritten sub-regions to SSD
    /// (checking the bitmap to skip guest-written sub-regions), and removes
    /// the entry from partial_blocks.
    fn spawn_background_backfill(&self, block_idx: u64) {
        use super::write_cache::inner::SUB_BLOCK_SIZE;

        let cache = Arc::clone(&self.cache);
        let content_store = Arc::clone(&self.content_store);
        let clean_cache = Arc::clone(&self.clean_cache);
        let pack_index_cache = Arc::clone(&self.pack_index_cache);
        let volume_manifest = Arc::clone(&self.volume_manifest);
        let metrics = Arc::clone(&self.metrics);
        let block_size = self.cache.block_size();

        tokio::spawn(async move {
            let block_start = block_idx * block_size as u64;
            let idx = block_idx as usize;

            // Fetch full block from S3 via the read path (uses download semaphore)
            let s3_data = match cache
                .read(
                    block_start,
                    block_size,
                    clean_cache.as_ref(),
                    &pack_index_cache,
                    &volume_manifest,
                    &content_store,
                    &metrics,
                )
                .await
            {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!(
                        block_idx,
                        error = %e,
                        "background backfill failed — block stays partial, on-demand read will complete it"
                    );
                    return;
                }
            };

            // Write each unwritten sub-region from S3 data to SSD.
            // Hold the per-block write_lock to prevent TOCTOU race where a
            // guest write marks the bitmap and pwrites between our bitmap
            // read and our pwrite (the guest re-pwrites under the same lock).
            let inner = cache.inner();
            {
                let state = match inner.partial_blocks.get(&idx) {
                    Some(s) => s,
                    None => return, // block was completed by on-demand read
                };
                let _guard = state.value().write_lock.lock();

                for sub in 0..(block_size / SUB_BLOCK_SIZE) {
                    let bitmap = state.value().bitmap.load(std::sync::atomic::Ordering::Acquire);
                    if bitmap & (1 << sub) != 0 {
                        continue; // guest already wrote this sub-region
                    }
                    let sub_offset = block_start + (sub * SUB_BLOCK_SIZE) as u64;
                    let sub_data = &s3_data[sub * SUB_BLOCK_SIZE..(sub + 1) * SUB_BLOCK_SIZE];
                    if let Err(e) = inner.write_sub_region(sub_offset, sub_data) {
                        tracing::warn!(
                            block_idx,
                            sub_region = sub,
                            error = %e,
                            "backfill pwrite failed"
                        );
                        return;
                    }
                }
            }

            // Backfill complete — remove from partial tracking.
            // If an on-demand read already completed it, this is a no-op.
            inner.complete_partial(idx);
        });
    }

    /// Spawn background backfill for all partial blocks recovered from WAL.
    ///
    /// Call this after the handler is fully constructed (following crash recovery)
    /// so that partial blocks get filled from S3 even if the guest never reads them.
    /// Without this, partial blocks would stay dirty and unflushed until a guest read
    /// triggers on-demand merge.
    pub fn spawn_recovery_backfills(&self) {
        let partial_block_indices: Vec<u64> = self
            .cache
            .inner()
            .partial_blocks
            .iter()
            .map(|entry| *entry.key() as u64)
            .collect();

        if !partial_block_indices.is_empty() {
            tracing::info!(
                count = partial_block_indices.len(),
                "spawning background backfill for recovered partial blocks"
            );
            for block_idx in partial_block_indices {
                self.spawn_background_backfill(block_idx);
            }
        }
    }

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

        if offset + length as u64 > self.device_size() {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(0);
        }

        self.metrics.record_guest_read(length as u64);

        // Fast path: all blocks present on local SSD → pread directly into
        // caller buffer. Zero allocation, zero memcpy.
        if let Some(result) = self.cache.try_pread_local(offset, length as usize, buf) {
            let n = result?;
            self.trigger_readahead(offset);
            self.metrics.record_read_latency(start.elapsed());
            return Ok(n);
        }

        let data = self
            .cache
            .read(
                offset,
                length as usize,
                self.clean_cache.as_ref(),
                &self.pack_index_cache,
                &self.volume_manifest,
                &self.content_store,
                &self.metrics,
            )
            .await?;
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);

        self.trigger_readahead(offset);

        self.metrics.record_read_latency(start.elapsed());
        Ok(n)
    }

    /// Write data to the cache.
    ///
    /// Writes go to local SSD immediately. S3 sync happens in background.
    /// Returns error if the export is readonly or SSD is near-full and
    /// the write touches blocks not yet present on SSD.
    ///
    /// For sub-block writes to blocks that exist only in S3, fetches the
    /// full block from S3 first to preserve unwritten portions.
    pub async fn write(&self, offset: u64, data: &[u8], fua: bool) -> CommandResult<()> {
        let start = Instant::now();

        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset + data.len() as u64 > self.device_size() {
            return Err(CommandError::InvalidArgument);
        }

        if data.is_empty() {
            return Ok(());
        }

        let util = f64::from_bits(self.ssd_utilization.load(Ordering::Relaxed));
        if util > WRITE_REJECT_THRESHOLD && self.cache.has_new_blocks(offset, data.len()) {
            return Err(CommandError::NoSpace);
        }

        let backfill_blocks = self
            .backfill_missing_blocks(offset, data.len() as u64)
            .await?;

        self.metrics.record_guest_write(data.len() as u64);
        self.cache.write(offset, data, self.clean_cache.as_ref())?;

        // Spawn background backfill AFTER write completes — the bitmap bits are
        // now set and the pwrite is done, so the backfill task won't race with
        // guest-written data.
        for block_idx in backfill_blocks {
            self.spawn_background_backfill(block_idx);
        }

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

        if offset + length as u64 > self.device_size() {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        let backfill_blocks = self
            .backfill_missing_blocks(offset, length as u64)
            .await?;

        self.cache.zero_range(offset, length as u64)?;

        // Spawn background backfill AFTER zero_range completes
        for block_idx in backfill_blocks {
            self.spawn_background_backfill(block_idx);
        }

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

        if offset + length as u64 > self.device_size() {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        let backfill_blocks = self
            .backfill_missing_blocks(offset, length as u64)
            .await?;

        self.cache.zero_range(offset, length as u64)?;

        for block_idx in backfill_blocks {
            self.spawn_background_backfill(block_idx);
        }

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
        if offset + length as u64 > self.device_size() {
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

    /// Phase 1 of ublk zero-copy write: prepare metadata before io_uring write.
    ///
    /// Validates the request (readonly, SSD-full, bounds), then marks blocks
    /// present and clears CRC32. Call BEFORE the io_uring write SQE.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub async fn pre_write(&self, offset: u64, length: u64) -> CommandResult<()> {
        if self.is_readonly() {
            return Err(CommandError::ReadOnly);
        }

        if offset + length > self.device_size() {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        let util = f64::from_bits(self.ssd_utilization.load(Ordering::Relaxed));
        if util > WRITE_REJECT_THRESHOLD && self.cache.has_new_blocks(offset, length as usize) {
            return Err(CommandError::NoSpace);
        }

        let backfill_blocks = self.backfill_missing_blocks(offset, length).await?;

        self.cache.pre_write(offset, length)?;

        // Spawn after pre_write — bitmap bits are set, safe for backfill
        for block_idx in backfill_blocks {
            self.spawn_background_backfill(block_idx);
        }

        Ok(())
    }

    /// Phase 2 of ublk zero-copy write: commit metadata after io_uring write.
    ///
    /// Records metrics, marks blocks dirty, appends WAL entries.
    /// Call ONLY after the io_uring write SQE has completed successfully.
    #[cfg(all(target_os = "linux", feature = "ublk"))]
    pub fn post_write(&self, offset: u64, length: u64, fua: bool) -> CommandResult<()> {
        if length == 0 {
            return Ok(());
        }

        let start = Instant::now();

        self.metrics.record_guest_write(length);
        self.cache.post_write(offset, length)?;

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
        if offset + length as u64 > self.device_size() {
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
            wal_sync: false, bottomless: false,
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
            wal_sync: false, bottomless: false,
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
            wal_sync: false, bottomless: false,
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
