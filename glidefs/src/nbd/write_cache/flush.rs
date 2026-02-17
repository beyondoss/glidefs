use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, info, instrument};

use crate::nbd::block_map::{
    BlockMapEntry, BlockMapKind, Blake3Hash, lz4_compress,
};
use crate::nbd::block_store::S3BlockStore;
use crate::nbd::content_store::ContentStore;
use crate::nbd::manifest::{Manifest, ManifestBlockEntry};
use crate::nbd::pack::{self, PackLocation, BLOCKS_PER_PACK};
use crate::nbd::pack_index::HostPackIndex;
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

    /// Save metadata to disk.
    pub fn save_metadata(&self) -> Result<(), CacheError> {
        self.inner.save_metadata()
    }

    /// Graceful shutdown: save metadata and transition to Draining.
    ///
    /// The v2 flush scheduler is responsible for ensuring dirty blocks are
    /// flushed to S3 before shutdown. This method only persists local metadata
    /// and transitions the cache state.
    #[instrument(skip(self, _s3))]
    pub async fn shutdown(self, _s3: &S3BlockStore) -> Result<WriteCache<Draining>, CacheError> {
        info!("starting graceful shutdown");

        // Save final metadata
        self.inner.save_metadata()?;
        info!("shutdown complete");

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }

    /// Shared inner logic for flush/snapshot: snapshot dirty entries, dedup,
    /// compress, pack upload, CAS-clear dirty flags.
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

        // 2. Snapshot dirty entries: non-empty, dirty, sequence <= cutpoint
        let snapshot: Vec<(usize, Blake3Hash)> = {
            let block_map_snap = self.inner.block_map_snapshot();
            block_map_snap
                .iter_non_empty()
                .filter(|(_, entry)| {
                    entry.is_dirty() && entry.sequence <= seq_cutpoint
                })
                .map(|(idx, entry)| (idx, entry.hash))
                .collect()
        };

        if snapshot.is_empty() {
            debug!("flush: no dirty blocks to flush");
            return Ok((stats, seq_cutpoint));
        }

        info!(dirty_blocks = snapshot.len(), seq_cutpoint, "starting flush");

        // 3. Dedup check + compress new blocks
        let zero_hash = self.inner.zero_block_hash;
        let mut to_upload: Vec<(Blake3Hash, Vec<u8>)> = Vec::new();
        let mut seen_hashes = std::collections::HashSet::new();

        for &(_chunk_index, hash) in &snapshot {
            stats.blocks_flushed += 1;

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

            // Get data from dirty store
            let data = self
                .inner
                .dirty_store
                .lock()
                .get(&hash)
                .cloned();

            let Some(data) = data else {
                // Data missing from dirty store — could have been cleaned by a
                // concurrent flush. Skip; it's already in S3.
                stats.blocks_deduped += 1;
                continue;
            };

            let compressed = lz4_compress(&data);
            to_upload.push((hash, compressed));
        }

        // 4. Assemble packs and upload
        for chunk in to_upload.chunks(BLOCKS_PER_PACK) {
            let pack_id = uuid::Uuid::new_v4();
            let (pack_bytes, index_entries) = pack::assemble_pack(chunk, chunk_size);
            let pack_size = pack_bytes.len() as u64;

            content_store.put_pack(pack_id, pack_bytes).await?;
            stats.packs_uploaded += 1;
            stats.bytes_uploaded += pack_size;

            // Register in host pack index for future dedup
            for entry in &index_entries {
                host_pack_index.insert(
                    entry.hash,
                    PackLocation {
                        pack_id,
                        offset: entry.offset,
                        comp_length: entry.comp_length,
                    },
                );
            }
        }

        // 5. Clear dirty state for blocks whose hash hasn't changed.
        //
        // Track which hashes still have dirty references so we only remove
        // from the dirty store when the last block referencing that hash is
        // cleaned. This prevents premature eviction when two chunks share
        // the same content hash.
        let mut dirty_hash_refs: std::collections::HashMap<Blake3Hash, usize> =
            std::collections::HashMap::new();
        for &(_, hash) in &snapshot {
            *dirty_hash_refs.entry(hash).or_insert(0) += 1;
        }

        for &(chunk_index, snapshot_hash) in &snapshot {
            let (current_hash, _seq) = self.inner.block_map_get(chunk_index);
            if current_hash == snapshot_hash {
                // Hash unchanged since snapshot — safe to clear dirty flag.
                // CAS Dirty → Clean on the block_states AtomicU8.
                if self.inner.block_states[chunk_index]
                    .compare_exchange(
                        BlockMapEntry::FLAG_DIRTY,
                        0, // clean
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.inner
                        .dirty_block_count
                        .fetch_sub(1, Ordering::Relaxed);
                    self.inner
                        .dirty_bytes
                        .fetch_sub(chunk_size as u64, Ordering::Relaxed);
                }

                // Decrement refcount for this hash
                if let Some(count) = dirty_hash_refs.get_mut(&snapshot_hash) {
                    *count -= 1;
                    if *count == 0 {
                        // Last block referencing this hash — safe to remove from dirty store
                        self.inner
                            .dirty_store
                            .lock()
                            .remove(&snapshot_hash);
                    }
                }
            } else {
                // Concurrent write produced a new hash — leave dirty for next flush.
                // Also decrement refcount since this block no longer holds the old hash.
                if let Some(count) = dirty_hash_refs.get_mut(&snapshot_hash) {
                    *count -= 1;
                    // Don't remove from dirty store — another block may still reference it
                }
            }
        }

        info!(
            blocks_flushed = stats.blocks_flushed,
            blocks_deduped = stats.blocks_deduped,
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
        // Checkpoint: persist block states then truncate the WAL.
        // Hold the WAL lock across both operations so no writes can append
        // entries between save_metadata and truncation.
        let mut wal = self.inner.wal.lock();
        self.inner.save_metadata()?;
        wal.truncate()?;
        Ok(())
    }

    /// Current dirty bytes outstanding (unflushed writes).
    pub fn dirty_bytes(&self) -> u64 {
        self.inner.dirty_bytes.load(Ordering::Relaxed)
    }

    /// Flush dirty blocks to S3 as content-addressed packs + manifest.
    ///
    /// This is the v2 flush path:
    /// 1. Snapshot dirty block map entries up to the current sequence
    /// 2. Dedup against the host pack index (blocks already in S3)
    /// 3. LZ4-compress new blocks and assemble into 25-block packs
    /// 4. Upload packs to S3
    /// 5. Clear dirty flags (with concurrent-write safety)
    /// 6. Upload a self-contained manifest
    #[instrument(skip(self, content_store, host_pack_index))]
    pub async fn flush_to_s3(
        &self,
        content_store: &ContentStore,
        host_pack_index: &HostPackIndex,
    ) -> Result<FlushStats, CacheError> {
        let (stats, seq_cutpoint) = self.flush_dirty_inner(content_store, host_pack_index).await?;
        self.upload_manifest(content_store, host_pack_index, seq_cutpoint).await?;
        // Checkpoint: persist block states then truncate the WAL.
        let mut wal = self.inner.wal.lock();
        self.inner.save_metadata()?;
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
        // Checkpoint: persist block states then truncate the WAL.
        let mut wal = self.inner.wal.lock();
        self.inner.save_metadata()?;
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
            if let BlockMapKind::Forked(f) = &*bm {
                if f.should_flatten() {
                    let full = f.flatten();
                    *bm = BlockMapKind::Full(full);
                    info!("flattened forked block map");
                }
            }
        }
    }

    /// Get a clone of the inner Arc for sharing with the sync worker.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> Arc<CacheInner> {
        Arc::clone(&self.inner)
    }
}
