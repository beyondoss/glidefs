use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

use crate::nbd::block_map::{
    BlockMapKind, Blake3Hash, SparseBlockState, blake3_128, lz4_compress,
};
use crate::nbd::content_store::ContentStore;
use crate::nbd::manifest::{Manifest, ManifestBlockEntry};
use crate::nbd::pack::{self, PackLocation, BLOCKS_PER_PACK};
use crate::nbd::pack_index::HostPackIndex;
use crate::nbd::pack_registry::PackRegistry;
use crate::nbd::state::{Active, Draining};

use super::inner::CacheInner;
use super::{CacheError, FlushStats, SnapshotResult, WriteCache};

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
    pub fn rebuild_manifest_hashes(&self, host_pack_index: &HostPackIndex) {
        let snap = self.inner.block_map_snapshot();
        let hashes = host_pack_index
            .derive_for_block_map(&snap)
            .into_iter()
            .map(|e| e.hash)
            .collect();
        *self.inner.manifest_pack_hashes.lock() = hashes;
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
        host_pack_index: &HostPackIndex,
    ) -> Result<(FlushStats, u64), CacheError> {
        let mut stats = FlushStats::default();
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
            return Ok((stats, seq_cutpoint));
        }

        info!(dirty_blocks = snapshot.len(), seq_cutpoint, "starting flush");

        // 3. Read each dirty block from SSD, compute hash, dedup, compress.
        let zero_hash = self.inner.zero_block_hash;
        let mut to_upload: Vec<(Blake3Hash, Vec<u8>)> = Vec::new();
        let mut seen_hashes = std::collections::HashSet::new();
        // Track (chunk_index, snapshot_seq, computed_hash) for step 5.
        let mut computed: Vec<(usize, u64, Blake3Hash)> = Vec::new();

        let block_size = self.inner.config.block_size;
        let mut chunk_buf = vec![0u8; block_size];

        for &(chunk_index, snapshot_seq) in &snapshot {
            stats.blocks_flushed += 1;

            // Read block data from SSD and compute content hash.
            let offset = chunk_index as u64 * block_size as u64;
            let device_size = self.inner.config.device_size;
            let valid_bytes = std::cmp::min(block_size as u64, device_size.saturating_sub(offset)) as usize;
            if valid_bytes > 0 {
                self.inner.data_file.read_exact_at(&mut chunk_buf[..valid_bytes], offset)?;
            }
            if valid_bytes < block_size {
                chunk_buf[valid_bytes..].fill(0);
            }

            let hash = blake3_128(&chunk_buf);
            computed.push((chunk_index, snapshot_seq, hash));

            // Skip zero blocks — read path returns zeros directly
            if hash == zero_hash {
                stats.blocks_deduped += 1;
                continue;
            }

            // Skip blocks already in the host pack index (cross-VM dedup)
            if host_pack_index.contains(&hash) {
                stats.blocks_deduped += 1;
                continue;
            }

            // Skip duplicate hashes within this flush batch
            if !seen_hashes.insert(hash) {
                stats.blocks_deduped += 1;
                continue;
            }

            let compressed = lz4_compress(&chunk_buf[..]);
            to_upload.push((hash, compressed));
        }

        // 4. Assemble packs then upload concurrently.
        //    Separate assembly from upload so we can fire all S3 PUTs at once.
        let mut pack_meta: Vec<(uuid::Uuid, u64, Vec<pack::PackIndexEntry>)> = Vec::new();
        let mut upload_pairs: Vec<(uuid::Uuid, Vec<u8>)> = Vec::new();

        for chunk in to_upload.chunks(BLOCKS_PER_PACK) {
            let pack_id = uuid::Uuid::new_v4();
            let (pack_bytes, index_entries) = pack::assemble_pack(chunk, chunk_size);
            let pack_size = pack_bytes.len() as u64;
            pack_meta.push((pack_id, pack_size, index_entries));
            upload_pairs.push((pack_id, pack_bytes));
        }

        // Upload all packs concurrently
        let upload_futs: Vec<_> = upload_pairs
            .into_iter()
            .map(|(id, bytes)| content_store.put_pack(id, bytes))
            .collect();
        futures::future::try_join_all(upload_futs).await?;

        // Register in host pack index for future dedup
        for (pack_id, pack_size, index_entries) in &pack_meta {
            stats.packs_uploaded += 1;
            stats.bytes_uploaded += *pack_size;
            stats.new_pack_ids.push(*pack_id);
            for entry in index_entries {
                host_pack_index.insert(
                    entry.hash,
                    PackLocation {
                        pack_id: *pack_id,
                        offset: entry.offset,
                        comp_length: entry.comp_length,
                    },
                );
            }
        }

        // 5. Set real hashes in block_map + clear dirty flags.
        //    Sequence number serves as version token: if a concurrent write
        //    changed the sequence, leave the block dirty for the next flush.
        for &(chunk_index, snapshot_seq, actual_hash) in &computed {
            let (_hash, current_seq) = self.inner.block_map_get(chunk_index);
            if current_seq != snapshot_seq {
                // Concurrent write changed this block — leave dirty.
                stats.blocks_cas_failed += 1;
                continue;
            }
            // Replace ZERO placeholder with real content hash.
            self.inner.block_map_set(chunk_index, actual_hash, snapshot_seq)?;
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
            packs_uploaded = stats.packs_uploaded,
            bytes_uploaded = stats.bytes_uploaded,
            "flush dirty inner complete"
        );

        Ok((stats, seq_cutpoint))
    }

    /// Build and upload a manifest from current state.
    ///
    /// Returns the S3 ETag if the backend provides one.
    async fn upload_manifest(
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
        let pack_entries = host_pack_index.derive_for_block_map(&post_flush_snap);

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

        Ok(etag)
    }

    /// Flush dirty blocks to S3 as packs (no manifest upload).
    ///
    /// Returns flush statistics and the sequence cutpoint for a subsequent
    /// `sync_manifest()` call. Used by the flush scheduler to separate
    /// pack flushes (~5s) from manifest syncs (~60s).
    pub async fn flush_packs(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
    ) -> Result<(FlushStats, u64), CacheError> {
        self.flush_dirty_inner(content_store, host_pack_index).await
    }

    /// Upload a manifest reflecting the current block map state.
    ///
    /// Call after `flush_packs()` with the returned `seq_cutpoint` to persist
    /// a recovery manifest. Separated from pack flushes so the scheduler can
    /// sync manifests at a lower frequency.
    pub async fn sync_manifest(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
        seq_cutpoint: u64,
    ) -> Result<(), CacheError> {
        self.upload_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        // Checkpoint: block map outside lock, fast save + truncate under lock.
        self.inner.persist_block_map()?;
        let mut wal = self.inner.wal.lock();
        self.inner.save_block_states()?;
        wal.truncate()?;
        Ok(())
    }

    /// Local checkpoint: persist block states + truncate WAL.
    ///
    /// Independent of S3 — keeps the WAL bounded in demand-driven mode
    /// where S3 flushes may be infrequent. Should run every ~5s when
    /// there are dirty blocks.
    pub fn local_checkpoint(&self) -> Result<(), CacheError> {
        // Block map outside lock (safe: clean blocks don't change).
        self.inner.persist_block_map()?;
        // Fast save + truncate under lock.
        let mut wal = self.inner.wal.lock();
        self.inner.save_block_states()?;
        wal.truncate()?;
        debug!("local checkpoint complete");
        Ok(())
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
        host_pack_index: &HostPackIndex,
    ) -> Result<FlushStats, CacheError> {
        let (stats, seq_cutpoint) = self.flush_dirty_inner(content_store, host_pack_index).await?;
        self.upload_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        self.update_registry(content_store, &stats.new_pack_ids).await;
        // Checkpoint: persist block map outside the lock (safe for clean blocks),
        // then hold the WAL lock only for fast block-states save + truncate.
        self.inner.persist_block_map()?;
        let mut wal = self.inner.wal.lock();
        self.inner.save_block_states()?;
        wal.truncate()?;
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
        host_pack_index: &HostPackIndex,
    ) -> Result<SnapshotResult, CacheError> {
        let (stats, seq_cutpoint) = self.flush_dirty_inner(content_store, host_pack_index).await?;
        let manifest_etag = self.upload_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        self.update_registry(content_store, &stats.new_pack_ids).await;
        // Checkpoint: block map outside lock, fast save + truncate under lock.
        self.inner.persist_block_map()?;
        let mut wal = self.inner.wal.lock();
        self.inner.save_block_states()?;
        wal.truncate()?;
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
