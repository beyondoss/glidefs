#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! Shared core for writing an ext4 byte stream into content-addressed storage.
//!
//! This is the streaming engine behind every bless/store path: read fixed-size
//! blocks, skip zeros, dedup within a 128 MiB chunk, and upload each chunk as a
//! content-addressed pack (overlapping upload with the next chunk's reads),
//! producing a [`VolumeManifest`]. It lives in the always-on library (not the
//! feature-gated `cli` module) so both `cli::bless` and `oci::layer_store` can
//! use it.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;

use crate::block::block_map::{blake3_128, compress_block, shared_zero_block, Blake3Hash};
use crate::block::content_store::ContentStore;
use crate::block::pack::{content_pack_id, PackId};
use crate::block::volume_manifest::VolumeManifest;

/// Fixed block size for the chunked architecture: 128 KiB.
pub const BLOCK_SIZE: u32 = 131_072;

/// Derive a deterministic, stable ext4 filesystem UUID from a content-addressed
/// digest (an OCI manifest digest, or a single layer digest).
///
/// The same content always resolves to the same digest, so hashing it yields
/// the same UUID on every run — which makes the produced ext4 byte-identical and
/// therefore content-dedupable. We hash rather than slice the digest so the
/// result is uniformly distributed, then stamp the RFC 4122 version (8 = custom)
/// and variant bits so it is a well-formed UUID.
pub fn deterministic_uuid(digest: &str) -> [u8; 16] {
    let mut uuid = blake3_128(digest.as_bytes()).0;
    uuid[6] = (uuid[6] & 0x0f) | 0x80; // version 8 (custom)
    uuid[8] = (uuid[8] & 0x3f) | 0x80; // variant 1 (RFC 4122)
    uuid
}

/// Upload stats for a stream.
#[derive(Default, Debug, Clone)]
pub struct StoreStats {
    pub zero_blocks: usize,
    pub unique_blocks: usize,
    pub packs_uploaded: usize,
    pub bytes_uploaded: u64,
    pub chunks_written: usize,
}

/// Stream an ext4 byte source into content-addressed packs + a `VolumeManifest`
/// under `content_store`'s base path.
///
/// Returns the manifest and upload stats. The caller owns persisting the
/// manifest under whatever name it chooses. (Index warming on fork comes from
/// the manifest's pack list; the boot working set is captured at runtime, so no
/// "hot set" is produced here.)
///
/// `source` is read for exactly `ceil(device_size / BLOCK_SIZE)` blocks; bytes
/// past EOF are treated as zero (so an ext4 image smaller than `device_size`
/// just yields trailing zero blocks, which are skipped).
pub async fn store_ext4_stream<R: Read>(
    content_store: &Arc<ContentStore>,
    mut source: R,
    device_size: u64,
    // Block compression level (`block_map::COMPRESSION_LZ4` = LZ4, else zstd level).
    level: i32,
) -> Result<(VolumeManifest, StoreStats)> {
    let vm_template = VolumeManifest::new(device_size, BLOCK_SIZE);
    let total_blocks = device_size.div_ceil(u64::from(BLOCK_SIZE)) as usize;
    let (_, zero_hash) = shared_zero_block(BLOCK_SIZE as usize);
    let mut buf = vec![0u8; BLOCK_SIZE as usize];

    let mut volume_manifest = VolumeManifest::new(device_size, BLOCK_SIZE);
    let mut stats = StoreStats::default();
    let mut pending_chunk: Option<(u32, Vec<BlockInfo>)> = None;
    let mut in_flight: Option<tokio::task::JoinHandle<Result<ChunkUploadResult>>> = None;

    for block_index in 0..total_blocks {
        let bytes_read = read_full(&mut source, &mut buf)?;
        if bytes_read < BLOCK_SIZE as usize {
            buf[bytes_read..].fill(0);
        }

        let hash = blake3_128(&buf);
        if hash == zero_hash {
            stats.zero_blocks += 1;
            continue;
        }

        let chunk_idx = vm_template.chunk_idx_for_block(block_index as u64);
        let block_offset = vm_template.block_offset_in_chunk(block_index as u64);
        stats.unique_blocks += 1;
        let compressed = Bytes::from(compress_block(&buf, level));

        if pending_chunk.as_ref().is_some_and(|(idx, _)| *idx != chunk_idx) {
            let (completed_idx, blocks) = pending_chunk.take().unwrap();
            in_flight = start_chunk_upload(
                content_store,
                &mut volume_manifest,
                &mut stats,
                in_flight,
                completed_idx,
                blocks,
            )
            .await?;
        }

        pending_chunk
            .get_or_insert_with(|| (chunk_idx, Vec::new()))
            .1
            .push(BlockInfo {
                block_offset,
                hash,
                compressed,
            });
    }

    if let Some((chunk_idx, blocks)) = pending_chunk.take() {
        in_flight = start_chunk_upload(
            content_store,
            &mut volume_manifest,
            &mut stats,
            in_flight,
            chunk_idx,
            blocks,
        )
        .await?;
    }

    join_upload(&mut volume_manifest, &mut stats, in_flight).await?;
    Ok((volume_manifest, stats))
}

/// Result of a completed chunk upload.
struct ChunkUploadResult {
    chunk_idx: u32,
    pack_id: PackId,
    pack_size: u64,
}

/// Join the previous in-flight upload (if any) and apply its results.
async fn join_upload(
    volume_manifest: &mut VolumeManifest,
    stats: &mut StoreStats,
    in_flight: Option<tokio::task::JoinHandle<Result<ChunkUploadResult>>>,
) -> Result<()> {
    if let Some(handle) = in_flight {
        let result = handle.await.context("upload task panicked")??;
        volume_manifest.append_pack(result.chunk_idx, result.pack_id);
        stats.packs_uploaded += 1;
        stats.bytes_uploaded += result.pack_size;
        stats.chunks_written += 1;
    }
    Ok(())
}

/// Dedup + assemble pack (CPU), then spawn S3 upload overlapped with next chunk's reads.
///
/// Joins the previous in-flight upload before spawning a new one, so at most
/// one upload is in flight at a time.
async fn start_chunk_upload(
    content_store: &Arc<ContentStore>,
    volume_manifest: &mut VolumeManifest,
    stats: &mut StoreStats,
    prev_in_flight: Option<tokio::task::JoinHandle<Result<ChunkUploadResult>>>,
    chunk_idx: u32,
    blocks: Vec<BlockInfo>,
) -> Result<Option<tokio::task::JoinHandle<Result<ChunkUploadResult>>>> {
    // Share compressed Bytes across duplicate hashes (avoids redundant allocations)
    // but keep every block offset in the pack index. Two blocks with the same
    // hash but different chunk_offsets both need entries — otherwise the read
    // path can't find the second block and returns zeros (BlockLocation::Zero).
    let mut first_seen: HashMap<Blake3Hash, Bytes> = HashMap::new();
    let mut pack_blocks: Vec<(Blake3Hash, u32, Bytes)> = Vec::new();

    for block in blocks {
        let compressed = first_seen
            .entry(block.hash)
            .or_insert_with(|| block.compressed.clone())
            .clone();
        pack_blocks.push((block.hash, block.block_offset, compressed));
    }

    if pack_blocks.is_empty() {
        // All-zero chunk — just join previous and move on.
        join_upload(volume_manifest, stats, prev_in_flight).await?;
        stats.chunks_written += 1;
        return Ok(None);
    }

    // Sort by chunk_offset for canonical ordering before computing the
    // content-addressed pack ID (same as flush and compaction paths).
    pack_blocks.sort_by_key(|(_, co, _)| *co);
    let pack_id = content_pack_id(&pack_blocks);

    // Join previous upload before spawning next (keeps at most 1 in flight).
    join_upload(volume_manifest, stats, prev_in_flight).await?;

    // Spawn streaming S3 upload — runs concurrently with next chunk's disk reads.
    let cs = Arc::clone(content_store);
    let handle = tokio::spawn(async move {
        let entries = cs
            .stream_chunk_pack(chunk_idx, pack_id, pack_blocks, BLOCK_SIZE)
            .await
            .context("Failed to stream chunk pack")?;
        let pack_size = entries.iter().map(|e| u64::from(e.comp_length)).sum::<u64>();
        Ok(ChunkUploadResult {
            chunk_idx,
            pack_id,
            pack_size,
        })
    });

    Ok(Some(handle))
}

/// Block info accumulated during the image scan.
struct BlockInfo {
    block_offset: u32,
    hash: Blake3Hash,
    compressed: Bytes,
}

/// Read exactly buf.len() bytes, or fewer at EOF.
fn read_full<R: Read>(file: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match file.read(&mut buf[total..])? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}
