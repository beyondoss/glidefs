//! V4 inline compaction: merge delta packs into a single base pack.
//!
//! Runs inline after flush when a chunk's pack count exceeds the threshold.
//! Reads all live blocks from existing packs (newest wins per chunk_offset),
//! assembles a new GLPK v3 base pack, uploads it, and replaces the chunk's
//! pack list in the manifest. Old packs are returned for deletion by the caller.
//!
//! Key property: no decompress/recompress cycle. Compressed block data is
//! copied directly from source packs to the new base pack.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{self, StreamExt};
use tracing::{debug, info, warn};

use crate::block::cache::BlockCache;
use crate::block::content_store::ContentStore;
use crate::block::pack::{PackId, PackIndexEntry};
use crate::block::pack_index_cache::PackIndexCache;
use crate::block::volume_manifest::VolumeManifest;
use crate::block::block_map::Blake3Hash;

use super::CacheError;

type FetchResult = Result<(Blake3Hash, u32, Bytes), CacheError>;

/// Default compaction threshold: compact when a chunk has more than this many packs.
pub const DEFAULT_COMPACTION_THRESHOLD: usize = 16;

/// Default dead-block ratio threshold: compact when >50% of entries across
/// a chunk's packs are superseded by newer entries.
pub const DEFAULT_DEAD_RATIO_THRESHOLD: f32 = 0.5;

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

/// Calculate the ratio of dead (superseded) blocks across all packs for a chunk.
///
/// A block is "dead" if a newer pack contains an entry for the same chunk_offset.
/// Returns the ratio (0.0 = no dead blocks, 1.0 = all dead).
async fn dead_block_ratio(
    pack_ids: &[PackId],
    pack_index_cache: &Arc<PackIndexCache>,
    content_store: &ContentStore,
    chunk_idx: u32,
) -> Result<f32, CacheError> {
    if pack_ids.len() < 2 {
        return Ok(0.0);
    }

    // Fetch all pack indices concurrently (cache hits resolve immediately,
    // S3 misses run in parallel via buffer_unordered).
    let results: Vec<Result<(usize, Arc<[PackIndexEntry]>), CacheError>> =
        stream::iter(pack_ids.iter().copied().enumerate())
            .map(|(pack_idx, pack_id)| async move {
                let entries: Arc<[PackIndexEntry]> =
                    match pack_index_cache.get_entries(pack_id).await {
                        Some(entries) => entries,
                        None => {
                            let fetched =
                                content_store.get_pack_index(chunk_idx, pack_id).await?;
                            pack_index_cache.insert_entries(pack_id, &fetched);
                            Arc::from(fetched)
                        }
                    };
                Ok((pack_idx, entries))
            })
            .buffer_unordered(8)
            .collect()
            .await;

    // Build ownership map — highest pack_idx wins per offset (order-independent).
    let mut total_entries = 0usize;
    let mut owner: HashMap<u32, usize> = HashMap::new();

    for result in results {
        let (pack_idx, entries) = result?;
        total_entries += entries.len();
        for entry in entries.iter() {
            owner
                .entry(entry.chunk_offset)
                .and_modify(|existing| *existing = (*existing).max(pack_idx))
                .or_insert(pack_idx);
        }
    }

    if total_entries == 0 {
        return Ok(0.0);
    }

    // Dead entries = total - unique offsets (each unique offset has one live owner)
    let live = owner.len();
    let dead = total_entries - live;
    Ok(dead as f32 / total_entries as f32)
}

/// Compact a single chunk: merge N delta packs into 1 base pack.
///
/// Algorithm:
/// 1. Load all pack indices (from cache or S3)
/// 2. Build merged block map: newest entry wins per chunk_offset
/// 3. For each live block: S3 range-read compressed data from source pack
/// 4. Assemble new GLPK v3 base pack (no decompress/recompress)
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
    _clean_cache: &Arc<dyn BlockCache>,
) -> Result<CompactionResult, CacheError> {
    info!(
        chunk_idx,
        pack_count = pack_ids.len(),
        "starting compaction"
    );

    // 1. Load all pack indices in parallel (from cache, or fetch from S3 on miss)
    let all_entries: Vec<(PackId, std::sync::Arc<[PackIndexEntry]>)> = {
        let results: Vec<Result<(PackId, std::sync::Arc<[PackIndexEntry]>), CacheError>> =
            stream::iter(pack_ids.iter().copied())
                .map(|pid| async move {
                    let entries: std::sync::Arc<[PackIndexEntry]> = match pack_index_cache.get_entries(pid).await {
                        Some(e) => e,
                        None => {
                            let fetched = content_store
                                .get_pack_index(chunk_idx, pid)
                                .await?;
                            pack_index_cache.insert_entries(pid, &fetched);
                            std::sync::Arc::from(fetched)
                        }
                    };
                    Ok((pid, entries))
                })
                .buffer_unordered(16)
                .collect()
                .await;
        let mut entries = Vec::with_capacity(results.len());
        for r in results {
            entries.push(r?);
        }
        // Restore original oldest-to-newest ordering (buffer_unordered
        // completes out of order). Needed for "newest wins" merge below.
        let id_order: HashMap<PackId, usize> = pack_ids
            .iter()
            .enumerate()
            .map(|(i, &pid)| (pid, i))
            .collect();
        entries.sort_by_key(|(pid, _)| id_order[pid]);
        entries
    };

    // 2. Build merged block map: newest entry wins per chunk_offset.
    // pack_ids are ordered oldest-to-newest, so we iterate forward and overwrite.
    let mut merged: HashMap<u32, (PackId, Blake3Hash, u32, u32)> = HashMap::new();
    for (pid, entries) in &all_entries {
        for entry in entries.iter() {
            merged.insert(
                entry.chunk_offset,
                (*pid, entry.hash, entry.offset, entry.comp_length),
            );
        }
    }

    if merged.is_empty() {
        // Edge case: all packs were empty. Remove them from the manifest
        // without uploading an empty pack to S3.
        {
            let mut vm = volume_manifest.write();
            if !vm.replace_packs_cas(chunk_idx, pack_ids, vec![]) {
                warn!(
                    chunk_idx,
                    "compaction aborted: pack list changed during compaction (empty merge)"
                );
                return Err(CacheError::Io(std::io::Error::other(
                    "compaction aborted: concurrent manifest modification",
                )));
            }
        }

        return Ok(CompactionResult {
            chunk_idx,
            new_pack_id: 0,
            old_pack_ids: pack_ids.to_vec(),
            live_blocks: 0,
            new_pack_size: 0,
        });
    }

    // 3. Fetch compressed block data from source packs.
    // Partition: zero tombstones (comp_length == 0) go directly into the output
    // without an S3 fetch. Non-zero blocks use concurrent range-reads.
    let mut zero_tombstones: Vec<(Blake3Hash, u32, Bytes)> = Vec::new();
    let mut blocks_to_fetch: Vec<(u32, PackId, Blake3Hash, u32, u32)> = Vec::new();
    for (chunk_offset, (pid, hash, pack_offset, comp_length)) in merged {
        if comp_length == 0 {
            zero_tombstones.push((hash, chunk_offset, Bytes::new()));
        } else {
            blocks_to_fetch.push((chunk_offset, pid, hash, pack_offset, comp_length));
        }
    }

    let live_block_count = blocks_to_fetch.len() + zero_tombstones.len();

    // S3-first fetch: S3 range-reads return already-compressed data (zero-copy).
    // The clean cache stores uncompressed blocks, so hits would require LZ4
    // recompression (~40µs/block) — wasteful when the compressed bytes are
    // already sitting in S3. Only fall back to the clean cache if the S3
    // fetch fails.
    let need_s3 = blocks_to_fetch;

    let fetched_blocks: Vec<FetchResult> =
        stream::iter(need_s3)
            .map(|(chunk_offset, pid, hash, pack_offset, comp_length)| {
                let cs = content_store;
                async move {
                    let compressed = cs
                        .get_chunk_block(chunk_idx, pid, pack_offset, comp_length)
                        .await?;
                    // Verify integrity: decompress and check blake3 hash.
                    // Compaction is the one data-movement path where corrupted
                    // S3 data could become the sole copy after GC deletes the
                    // original packs. The decompress+hash cost is acceptable
                    // since compaction is not latency-sensitive.
                    let decompressed = crate::block::block_map::decompress_block(&compressed)
                        .map_err(|e| CacheError::DecompressFailed(format!(
                            "compaction: chunk {} pack {:016x} offset {}: {}",
                            chunk_idx, pid, pack_offset, e
                        )))?;
                    let actual_hash = crate::block::block_map::blake3_128(&decompressed);
                    if actual_hash != hash {
                        return Err(CacheError::HashMismatch {
                            expected: format!("{:?}", hash),
                            actual: format!("{:?}", actual_hash),
                        });
                    }
                    Ok((hash, chunk_offset, Bytes::copy_from_slice(&compressed)))
                }
            })
            .buffer_unordered(8) // bounded concurrency for S3 reads
            .collect()
            .await;

    // Collect results, propagating errors
    let mut blocks_for_pack: Vec<(Blake3Hash, u32, Bytes)> =
        Vec::with_capacity(live_block_count);
    blocks_for_pack.extend(zero_tombstones);
    for result in fetched_blocks {
        blocks_for_pack.push(result?);
    }

    // 4. Stream new GLPK v3 base pack to S3
    // Sort by chunk_offset for deterministic content-addressed pack ID.
    blocks_for_pack.sort_by_key(|&(_, co, _)| co);
    let base_pack_id = crate::block::pack::content_pack_id(&blocks_for_pack);
    let index_entries = content_store
        .stream_chunk_pack(chunk_idx, base_pack_id, blocks_for_pack, blocks_per_chunk)
        .await?;

    // Derive pack size from index entries (for logging/stats only).
    let pack_size: u64 = index_entries
        .iter()
        .map(|e| u64::from(e.offset) + u64::from(e.comp_length))
        .max()
        .unwrap_or(0)
        + (index_entries.len() as u64) * (crate::block::pack::PACK_INDEX_ENTRY_SIZE as u64)
        + (crate::block::pack::TRAILER_SIZE as u64);

    // 5. Update manifest: replace N packs with 1, preserving concurrent appends
    {
        let mut vm = volume_manifest.write();
        if !vm.replace_packs_cas(chunk_idx, pack_ids, vec![base_pack_id]) {
            warn!(
                chunk_idx,
                "compaction aborted: pack list changed during compaction"
            );
            return Err(CacheError::Io(std::io::Error::other(
                "compaction aborted: concurrent manifest modification",
            )));
        }
    }

    // 6. Update PackIndexCache
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
    dead_ratio_threshold: f32,
    cooldown: u64,
    chunk_idle_age: &HashMap<u32, u64>,
    content_store: &ContentStore,
    pack_index_cache: &Arc<PackIndexCache>,
    volume_manifest: &Arc<parking_lot::RwLock<VolumeManifest>>,
    clean_cache: &Arc<dyn BlockCache>,
) -> Result<Vec<CompactionResult>, CacheError> {
    // Pass 1: collect chunks exceeding pack-count threshold (existing behavior)
    // Pass 2: collect chunks with 2+ packs for dead-ratio evaluation
    #[allow(clippy::type_complexity)]
    let (mut chunks_to_compact, ratio_candidates): (
        Vec<(u32, Vec<PackId>)>,
        Vec<(u32, Vec<PackId>)>,
    ) = {
        let vm = volume_manifest.read();
        let mut compact = Vec::new();
        let mut candidates = Vec::new();
        for (&idx, entry) in &vm.chunks {
            if entry.packs.len() > threshold {
                // Pack-count cap: unconditional greedy fallback, independent of
                // cooldown — a still-hot chunk that piles up packs is still
                // bounded.
                compact.push((idx, entry.packs.clone()));
            } else if entry.packs.len() >= 2 {
                // Dead-ratio candidate. Cooldown gate (F2FS-style age deferral):
                // when enabled, defer evaluating a chunk for dead-ratio
                // compaction until it has been idle (unwritten) for `cooldown`
                // flush cycles. Skipping here also avoids the candidate's
                // pack-index fetches in `dead_block_ratio`. A chunk absent from
                // the age map is treated as age 0 (just written) → deferred.
                let age_ok = cooldown == 0
                    || chunk_idle_age.get(&idx).copied().unwrap_or(0) >= cooldown;
                if age_ok {
                    candidates.push((idx, entry.packs.clone()));
                }
            }
        }
        (compact, candidates)
    };

    // Evaluate dead-block ratio for candidates concurrently.
    {
        use futures::stream::{self, StreamExt};

        let ratio_results: Vec<(u32, Vec<PackId>, Result<f32, CacheError>)> =
            stream::iter(ratio_candidates)
                .map(|(chunk_idx, pack_ids)| {
                    let cs = &content_store;
                    let pic = &pack_index_cache;
                    async move {
                        let result = dead_block_ratio(&pack_ids, pic, cs, chunk_idx).await;
                        (chunk_idx, pack_ids, result)
                    }
                })
                .buffer_unordered(16)
                .collect()
                .await;

        for (chunk_idx, pack_ids, result) in ratio_results {
            match result {
                Ok(ratio) if ratio > dead_ratio_threshold => {
                    debug!(
                        chunk_idx,
                        packs = pack_ids.len(),
                        dead_ratio = format!("{:.1}%", ratio * 100.0),
                        "dead-block ratio exceeds threshold"
                    );
                    chunks_to_compact.push((chunk_idx, pack_ids));
                }
                Ok(_) => {} // below threshold, skip
                Err(e) => {
                    // Non-fatal: skip this candidate, will retry next cycle
                    warn!(
                        chunk_idx,
                        error = %e,
                        "failed to compute dead-block ratio, skipping"
                    );
                }
            }
        }
    }

    if chunks_to_compact.is_empty() {
        return Ok(Vec::new());
    }

    debug!(
        chunks = chunks_to_compact.len(),
        threshold, "compacting chunks"
    );

    let blocks_per_chunk = {
        let vm = volume_manifest.read();
        vm.blocks_per_chunk()
    };

    // Compact chunks concurrently. Each compact_chunk does S3 reads +
    // upload + manifest CAS independently.
    use futures::stream::{self, StreamExt};

    let compact_results: Vec<(u32, Result<CompactionResult, CacheError>)> =
        stream::iter(chunks_to_compact)
            .map(|(chunk_idx, pack_ids)| {
                let cs = &content_store;
                let pic = &pack_index_cache;
                let vm = &volume_manifest;
                let cc = &clean_cache;
                async move {
                    let result = compact_chunk(
                        chunk_idx,
                        &pack_ids,
                        blocks_per_chunk,
                        cs,
                        pic,
                        vm,
                        cc,
                    )
                    .await;
                    (chunk_idx, result)
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;

    let mut results = Vec::new();
    for (chunk_idx, result) in compact_results {
        match result {
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

