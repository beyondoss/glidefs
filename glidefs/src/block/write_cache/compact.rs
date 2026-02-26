//! V4 inline compaction: merge delta packs into a single base pack.
//!
//! Runs inline after flush when a chunk's pack count exceeds the threshold.
//! Reads all live blocks from existing packs (newest wins per chunk_offset),
//! assembles a new GLPK v2 base pack, uploads it, and replaces the chunk's
//! pack list in the manifest. Old packs are returned for deletion by the caller.
//!
//! Key property: no decompress/recompress cycle. Compressed block data is
//! copied directly from source packs to the new base pack.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::block::content_store::ContentStore;
use crate::block::pack::{
    PackId, PackIndexEntry, assemble_pack, new_pack_id,
};
use crate::block::pack_index_cache::PackIndexCache;
use crate::block::volume_manifest::VolumeManifest;
use crate::block::block_map::Blake3Hash;

use super::CacheError;

type FetchResult = Result<(Blake3Hash, u32, Vec<u8>), CacheError>;

/// Default compaction threshold: compact when a chunk has more than this many packs.
pub const DEFAULT_COMPACTION_THRESHOLD: usize = 16;

/// Result of compacting a single chunk.
pub struct CompactionResult {
    /// Chunk index that was compacted.
    pub chunk_idx: u32,
    /// New base pack ID.
    pub new_pack_id: PackId,
    /// Old pack IDs that were replaced (caller may delete if not referenced by snapshots).
    pub old_pack_ids: Vec<PackId>,
    /// Number of live blocks in the new base pack.
    pub live_blocks: usize,
    /// Size of the new base pack in bytes.
    pub new_pack_size: u64,
}

/// Compact a single chunk: merge N delta packs into 1 base pack.
///
/// Algorithm:
/// 1. Load all pack indices (from cache or S3)
/// 2. Build merged block map: newest entry wins per chunk_offset
/// 3. For each live block: S3 range-read compressed data from source pack
/// 4. Assemble new GLPK v2 base pack (no decompress/recompress)
/// 5. Upload base pack
/// 6. Update manifest: replace_packs(chunk_idx, [new_pack_id])
/// 7. Update PackIndexCache with new base pack entries
/// 8. Return old pack_ids for deletion by caller
pub async fn compact_chunk(
    chunk_idx: u32,
    pack_ids: &[PackId],
    blocks_per_chunk: u32,
    content_store: &ContentStore,
    pack_index_cache: &Arc<PackIndexCache>,
    volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
) -> Result<CompactionResult, CacheError> {
    info!(
        chunk_idx,
        pack_count = pack_ids.len(),
        "starting compaction"
    );

    // 1. Load all pack indices (from cache, or fetch from S3 on miss)
    let mut all_entries: Vec<(PackId, Vec<PackIndexEntry>)> = Vec::new();
    for &pid in pack_ids {
        let entries = match pack_index_cache.get_entries(pid).await {
            Some(e) => e,
            None => {
                // Cache miss: fetch from S3
                let fetched = content_store
                    .get_pack_index(chunk_idx, pid)
                    .await
                    .map_err(|e| CacheError::Io(std::io::Error::other(e.to_string())))?;
                pack_index_cache.insert_entries(pid, &fetched);
                fetched
            }
        };
        all_entries.push((pid, entries));
    }

    // 2. Build merged block map: newest entry wins per chunk_offset.
    // pack_ids are ordered oldest-to-newest, so we iterate forward and overwrite.
    let mut merged: HashMap<u32, (PackId, Blake3Hash, u32, u32)> = HashMap::new();
    for (pid, entries) in &all_entries {
        for entry in entries {
            merged.insert(
                entry.chunk_offset,
                (*pid, entry.hash, entry.offset, entry.comp_length),
            );
        }
    }

    if merged.is_empty() {
        // Edge case: all packs were empty. Just replace with empty state.
        let base_pack_id = new_pack_id();
        let (pack_bytes, index_entries) = assemble_pack(Vec::new(), blocks_per_chunk)?;
        let pack_size = pack_bytes.len() as u64;

        content_store
            .put_chunk_pack(chunk_idx, base_pack_id, pack_bytes)
            .await
            .map_err(|e| CacheError::Io(std::io::Error::other(e.to_string())))?;

        pack_index_cache.insert_entries(base_pack_id, &index_entries);

        {
            let mut vm = volume_manifest.write();
            vm.replace_packs(chunk_idx, vec![base_pack_id]);
        }

        return Ok(CompactionResult {
            chunk_idx,
            new_pack_id: base_pack_id,
            old_pack_ids: pack_ids.to_vec(),
            live_blocks: 0,
            new_pack_size: pack_size,
        });
    }

    // 3. Fetch compressed block data from source packs.
    // Use concurrent range-reads bounded by a semaphore.
    use futures::stream::{self, StreamExt};

    let blocks_to_fetch: Vec<(u32, PackId, Blake3Hash, u32, u32)> = merged
        .into_iter()
        .map(|(chunk_offset, (pid, hash, pack_offset, comp_length))| {
            (chunk_offset, pid, hash, pack_offset, comp_length)
        })
        .collect();

    let live_block_count = blocks_to_fetch.len();

    let fetched_blocks: Vec<FetchResult> =
        stream::iter(blocks_to_fetch)
            .map(|(chunk_offset, pid, hash, pack_offset, comp_length)| {
                let cs = content_store;
                async move {
                    let data = cs
                        .get_chunk_block(chunk_idx, pid, pack_offset, comp_length)
                        .await
                        .map_err(|e| CacheError::Io(std::io::Error::other(e.to_string())))?;
                    Ok((hash, chunk_offset, data.to_vec()))
                }
            })
            .buffer_unordered(8) // bounded concurrency for S3 reads
            .collect()
            .await;

    // Collect results, propagating errors
    let mut blocks_for_pack: Vec<(Blake3Hash, u32, Vec<u8>)> = Vec::with_capacity(live_block_count);
    for result in fetched_blocks {
        blocks_for_pack.push(result?);
    }

    // 4. Assemble new GLPK v2 base pack (blocks already compressed, just repack)
    let (pack_bytes, index_entries) = assemble_pack(blocks_for_pack, blocks_per_chunk)?;
    let pack_size = pack_bytes.len() as u64;
    let base_pack_id = new_pack_id();

    // 5. Upload base pack
    content_store
        .put_chunk_pack(chunk_idx, base_pack_id, pack_bytes)
        .await
        .map_err(|e| CacheError::Io(std::io::Error::other(e.to_string())))?;

    // 6. Update manifest: replace N packs with 1
    {
        let mut vm = volume_manifest.write();
        vm.replace_packs(chunk_idx, vec![base_pack_id]);
    }

    // 7. Update PackIndexCache
    pack_index_cache.insert_entries(base_pack_id, &index_entries);

    info!(
        chunk_idx,
        live_blocks = live_block_count,
        old_packs = pack_ids.len(),
        new_pack_size = pack_size,
        "compaction complete"
    );

    Ok(CompactionResult {
        chunk_idx,
        new_pack_id: base_pack_id,
        old_pack_ids: pack_ids.to_vec(),
        live_blocks: live_block_count,
        new_pack_size: pack_size,
    })
}

/// Check all chunks in the manifest and compact any that exceed the threshold.
///
/// Called inline after flush. Returns the list of old pack IDs that were replaced
/// (the caller is responsible for deleting them, respecting snapshot references).
pub async fn compact_if_needed(
    threshold: usize,
    content_store: &ContentStore,
    pack_index_cache: &Arc<PackIndexCache>,
    volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
) -> Result<Vec<CompactionResult>, CacheError> {
    // Collect chunks that need compaction (snapshot under read lock)
    let chunks_to_compact: Vec<(u32, Vec<PackId>)> = {
        let vm = volume_manifest.read();
        vm.chunks
            .iter()
            .filter(|(_, entry)| entry.packs.len() > threshold)
            .map(|(&idx, entry)| (idx, entry.packs.clone()))
            .collect()
    };

    if chunks_to_compact.is_empty() {
        return Ok(Vec::new());
    }

    debug!(
        chunks = chunks_to_compact.len(),
        threshold, "compacting chunks"
    );

    let mut results = Vec::new();

    let blocks_per_chunk = {
        let vm = volume_manifest.read();
        vm.blocks_per_chunk()
    };

    for (chunk_idx, pack_ids) in chunks_to_compact {
        match compact_chunk(
            chunk_idx,
            &pack_ids,
            blocks_per_chunk,
            content_store,
            pack_index_cache,
            volume_manifest,
        )
        .await
        {
            Ok(result) => results.push(result),
            Err(e) => {
                warn!(
                    chunk_idx,
                    error = %e,
                    "compaction failed for chunk, will retry next cycle"
                );
                // Non-fatal: chunk stays with multiple packs, compacted next time
            }
        }
    }

    Ok(results)
}

/// Delete old packs from S3 after compaction, skipping any still referenced by snapshots.
///
/// Loads all snapshot manifests for this export and builds a set of pinned
/// (chunk_idx, pack_id) pairs. Old packs in the pinned set are preserved;
/// the rest are deleted best-effort.
pub async fn delete_old_packs(
    results: &[CompactionResult],
    content_store: &ContentStore,
    export_name: &str,
) {
    // Build the set of pack IDs pinned by snapshots.
    let pinned = match load_snapshot_pack_ids(content_store, export_name).await {
        Ok(set) => set,
        Err(e) => {
            // If we can't read snapshots, don't delete anything — safe default.
            warn!(
                error = %e,
                "failed to load snapshot manifests; skipping pack deletion to avoid data loss"
            );
            return;
        }
    };

    for result in results {
        for &old_pid in &result.old_pack_ids {
            if pinned.contains(&(result.chunk_idx, old_pid)) {
                debug!(
                    chunk_idx = result.chunk_idx,
                    pack_id = old_pid,
                    "skipping pack deletion — referenced by snapshot"
                );
                continue;
            }
            if let Err(e) = content_store
                .delete_chunk_pack(result.chunk_idx, old_pid)
                .await
            {
                warn!(
                    chunk_idx = result.chunk_idx,
                    pack_id = old_pid,
                    error = %e,
                    "failed to delete old pack after compaction"
                );
            }
        }
    }
}

/// Load all snapshot manifests for an export and return the union of their pack IDs.
async fn load_snapshot_pack_ids(
    content_store: &ContentStore,
    export_name: &str,
) -> Result<std::collections::HashSet<(u32, PackId)>, super::CacheError> {
    use std::collections::HashSet;
    use crate::block::volume_manifest::VolumeManifest;

    let sequences = content_store
        .list_snapshots(export_name)
        .await
        .map_err(|e| CacheError::Io(std::io::Error::other(e.to_string())))?;

    let mut pinned: HashSet<(u32, PackId)> = HashSet::new();

    for seq in sequences {
        let data = match content_store.get_snapshot(export_name, seq).await {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => {
                warn!(
                    export = %export_name, sequence = seq, error = %e,
                    "failed to fetch snapshot manifest, treating its packs as pinned"
                );
                // Can't determine what this snapshot references — abort to be safe.
                return Err(CacheError::Io(std::io::Error::other(
                    format!("failed to load snapshot {seq}: {e}")
                )));
            }
        };
        match VolumeManifest::deserialize(&data) {
            Ok(vm) => {
                pinned.extend(vm.all_pack_ids());
            }
            Err(e) => {
                warn!(
                    export = %export_name, sequence = seq, error = %e,
                    "corrupt snapshot manifest, treating its packs as pinned"
                );
                // Can't parse — could reference anything. Abort to be safe.
                return Err(CacheError::Io(std::io::Error::other(
                    format!("corrupt snapshot {seq}: {e}")
                )));
            }
        }
    }

    Ok(pinned)
}
