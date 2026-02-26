use crate::block::block_map::{blake3_128, lz4_compress, zero_block_hash, Blake3Hash};
use crate::block::content_store::ContentStore;
use crate::block::manifest::serialize_hot_set;
use crate::block::pack::{assemble_pack, new_pack_id, PackId};
use crate::block::volume_manifest::VolumeManifest;
use crate::config::Settings;
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// Fixed block size for the chunked architecture: 128KB.
const BLOCK_SIZE: u32 = 131_072;

pub async fn run_bless(
    image_path: PathBuf,
    name: String,
    s3_prefix: String,
    config_path: PathBuf,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let start = Instant::now();

    // --- Setup ---
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(
        &url.parse()?,
        env_vars.into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = path_from_url.to_string();

    let base = format!("{}/exports/{}", db_path, s3_prefix);
    let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), &base));

    info!(image = %image_path.display(), name = %name, "starting bless");

    // --- Read image ---
    let mut file = std::fs::File::open(&image_path)
        .with_context(|| format!("Failed to open image {}", image_path.display()))?;
    let device_size = file.metadata()?.len();

    let volume_manifest_template = VolumeManifest::new(device_size, BLOCK_SIZE);
    let blocks_per_chunk = volume_manifest_template.blocks_per_chunk();
    let total_blocks = device_size.div_ceil(BLOCK_SIZE as u64) as usize;

    info!(device_size, total_blocks, blocks_per_chunk, "reading image");

    // --- Stream image: read blocks, upload each chunk as it completes ---
    let zero_hash = zero_block_hash(BLOCK_SIZE as usize);
    let mut buf = vec![0u8; BLOCK_SIZE as usize];

    let mut volume_manifest = VolumeManifest::new(device_size, BLOCK_SIZE);
    let mut stats = BlessStats::default();
    let mut hot_set_indices: Vec<u64> = Vec::new();

    // Current chunk accumulator — flushed when we move to the next chunk.
    let mut pending_chunk: Option<(u32, Vec<BlockInfo>)> = None;
    // In-flight S3 upload — overlaps with reading the next chunk.
    let mut in_flight: Option<tokio::task::JoinHandle<Result<ChunkUploadResult>>> = None;

    for block_index in 0..total_blocks {
        let bytes_read = read_full(&mut file, &mut buf)?;
        if bytes_read < BLOCK_SIZE as usize {
            buf[bytes_read..].fill(0);
        }

        let hash = blake3_128(&buf);

        // Skip zero blocks entirely
        if hash == zero_hash {
            stats.zero_blocks += 1;
            continue;
        }

        // Record non-zero block index for hot set (prefetch at boot)
        hot_set_indices.push(block_index as u64);

        let chunk_idx = volume_manifest_template.chunk_idx_for_block(block_index as u64);
        let block_offset = volume_manifest_template.block_offset_in_chunk(block_index as u64);

        stats.unique_blocks += 1;

        let compressed = lz4_compress(&buf);

        // If we've moved to a new chunk, prepare and upload the previous one.
        if pending_chunk.as_ref().is_some_and(|(idx, _)| *idx != chunk_idx) {
            let (completed_idx, blocks) = pending_chunk.take().unwrap();
            in_flight = start_chunk_upload(
                &content_store,
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

    // Flush the final chunk.
    if let Some((chunk_idx, blocks)) = pending_chunk.take() {
        in_flight = start_chunk_upload(
            &content_store,
            &mut volume_manifest,
            &mut stats,
            in_flight,
            chunk_idx,
            blocks,
        )
        .await?;
    }

    // Wait for last upload.
    join_upload(&mut volume_manifest, &mut stats, in_flight).await?;

    // --- Upload manifest ---
    let manifest_key = format!("bases/{}", name);
    content_store
        .put_manifest(&manifest_key, volume_manifest.serialize())
        .await
        .context("Failed to upload manifest")?;

    // --- Upload hot set (block indices needed at boot for prefetching) ---
    let hot_set_data = serialize_hot_set(&hot_set_indices);
    content_store
        .put_hot_set(&name, hot_set_data)
        .await
        .context("Failed to upload hot set")?;

    let elapsed = start.elapsed();

    info!(
        name = %name,
        total_blocks,
        zero_blocks = stats.zero_blocks,
        unique_blocks = stats.unique_blocks,
        packs_uploaded = stats.packs_uploaded,
        bytes_uploaded = stats.bytes_uploaded,
        chunks_written = stats.chunks_written,
        elapsed_secs = elapsed.as_secs_f64(),
        "bless complete"
    );

    println!("Blessed '{}' successfully:", name);
    println!("  Image size:      {:.1} GB", device_size as f64 / 1e9);
    println!("  Total blocks:    {}", total_blocks);
    println!("  Zero blocks:     {} (skipped)", stats.zero_blocks);
    println!("  Unique blocks:   {} (uploaded)", stats.unique_blocks);
    println!("  Packs uploaded:  {}", stats.packs_uploaded);
    println!(
        "  Bytes uploaded:  {:.1} MB",
        stats.bytes_uploaded as f64 / 1e6
    );
    println!("  Chunks written:  {}", stats.chunks_written);
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());
    println!("  Manifest:        manifests/{}", manifest_key);

    Ok(())
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
    stats: &mut BlessStats,
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
    stats: &mut BlessStats,
    prev_in_flight: Option<tokio::task::JoinHandle<Result<ChunkUploadResult>>>,
    chunk_idx: u32,
    blocks: Vec<BlockInfo>,
) -> Result<Option<tokio::task::JoinHandle<Result<ChunkUploadResult>>>> {
    // CPU: dedup + assemble pack
    let mut seen: HashSet<Blake3Hash> = HashSet::new();
    let mut pack_blocks: Vec<(Blake3Hash, u32, Vec<u8>)> = Vec::new();

    for block in blocks {
        if seen.insert(block.hash) {
            pack_blocks.push((block.hash, block.block_offset, block.compressed));
        }
    }

    if pack_blocks.is_empty() {
        // All-zero chunk — just join previous and move on.
        join_upload(volume_manifest, stats, prev_in_flight).await?;
        stats.chunks_written += 1;
        return Ok(None);
    }

    let pack_id = new_pack_id();
    let (pack_bytes, _entries) = assemble_pack(pack_blocks, BLOCK_SIZE)?;

    // Join previous upload before spawning next (keeps at most 1 in flight).
    join_upload(volume_manifest, stats, prev_in_flight).await?;

    // Spawn S3 upload — runs concurrently with next chunk's disk reads.
    let cs = Arc::clone(content_store);
    let pack_size = pack_bytes.len() as u64;
    let handle = tokio::spawn(async move {
        cs.put_chunk_pack(chunk_idx, pack_id, pack_bytes)
            .await
            .context("Failed to upload chunk pack")?;
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
    compressed: Vec<u8>,
}

#[derive(Default)]
struct BlessStats {
    zero_blocks: usize,
    unique_blocks: usize,
    packs_uploaded: usize,
    bytes_uploaded: u64,
    chunks_written: usize,
}

/// Read exactly buf.len() bytes, or fewer at EOF.
fn read_full(file: &mut std::fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match file.read(&mut buf[total..])? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::block_map::lz4_decompress;
    use crate::block::pack::{extract_block, lookup_block_in_index, parse_pack_index, PackId};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::ObjectStore;

    /// Helper: run the bless pipeline directly against an InMemory object store.
    async fn bless_bytes(
        content_store: &ContentStore,
        name: &str,
        image_data: &[u8],
    ) -> Result<BlessStats> {
        let device_size = image_data.len() as u64;
        let vm_template = VolumeManifest::new(device_size, BLOCK_SIZE);
        let total_blocks = device_size.div_ceil(BLOCK_SIZE as u64) as usize;
        let zero_hash = zero_block_hash(BLOCK_SIZE as usize);

        let content_store = Arc::new(ContentStore::new(
            content_store.object_store().clone(),
            content_store.base_path(),
        ));
        let mut volume_manifest = VolumeManifest::new(device_size, BLOCK_SIZE);
        let mut stats = BlessStats::default();
        let mut hot_set_indices: Vec<u64> = Vec::new();
        let mut pending_chunk: Option<(u32, Vec<BlockInfo>)> = None;
        let mut in_flight: Option<tokio::task::JoinHandle<Result<ChunkUploadResult>>> = None;

        for block_index in 0..total_blocks {
            let start = block_index * BLOCK_SIZE as usize;
            let end = (start + BLOCK_SIZE as usize).min(image_data.len());
            let mut buf = vec![0u8; BLOCK_SIZE as usize];
            buf[..end - start].copy_from_slice(&image_data[start..end]);

            let hash = blake3_128(&buf);

            if hash == zero_hash {
                stats.zero_blocks += 1;
                continue;
            }

            hot_set_indices.push(block_index as u64);

            let chunk_idx = vm_template.chunk_idx_for_block(block_index as u64);
            let block_offset = vm_template.block_offset_in_chunk(block_index as u64);

            stats.unique_blocks += 1;

            let compressed = lz4_compress(&buf);

            if pending_chunk.as_ref().is_some_and(|(idx, _)| *idx != chunk_idx) {
                let (completed_idx, blocks) = pending_chunk.take().unwrap();
                in_flight = start_chunk_upload(
                    &content_store,
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
                &content_store,
                &mut volume_manifest,
                &mut stats,
                in_flight,
                chunk_idx,
                blocks,
            )
            .await?;
        }

        join_upload(&mut volume_manifest, &mut stats, in_flight).await?;

        content_store
            .put_manifest(&format!("bases/{}", name), volume_manifest.serialize())
            .await?;

        let hot_set_data = serialize_hot_set(&hot_set_indices);
        content_store
            .put_hot_set(name, hot_set_data)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(stats)
    }

    fn test_store() -> (Arc<InMemory>, ContentStore) {
        let store = Arc::new(InMemory::new());
        let cs = ContentStore::new(store.clone(), "test");
        (store, cs)
    }

    /// Fetch a full pack from the InMemory store and parse its index.
    async fn fetch_pack_index(
        store: &Arc<InMemory>,
        base_path: &str,
        chunk_idx: u32,
        pack_id: PackId,
    ) -> Vec<crate::block::pack::PackIndexEntry> {
        let key = format!("{}/chunks/{:04}/{:016x}.pack", base_path, chunk_idx, pack_id);
        let path = ObjectPath::from(key);
        let response = store.get(&path).await.unwrap();
        let bytes = response.bytes().await.unwrap();
        let index = parse_pack_index(&bytes).unwrap();
        index.entries
    }

    /// Fetch full pack bytes from the InMemory store.
    async fn fetch_pack_bytes(
        store: &Arc<InMemory>,
        base_path: &str,
        chunk_idx: u32,
        pack_id: PackId,
    ) -> Vec<u8> {
        let key = format!("{}/chunks/{:04}/{:016x}.pack", base_path, chunk_idx, pack_id);
        let path = ObjectPath::from(key);
        let response = store.get(&path).await.unwrap();
        response.bytes().await.unwrap().to_vec()
    }

    #[tokio::test]
    async fn test_bless_and_read_back() {
        let (store, cs) = test_store();

        // Create a 1MB image with known pattern (8 x 128KB blocks)
        let mut image = vec![0u8; 8 * BLOCK_SIZE as usize];
        for i in 0..8 {
            let start = i * BLOCK_SIZE as usize;
            image[start..start + BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        let stats = bless_bytes(&cs, "test-image", &image).await.unwrap();

        assert_eq!(stats.zero_blocks, 0);
        assert_eq!(stats.unique_blocks, 8);

        // Load VolumeManifest and verify
        let manifest_data = cs
            .get_manifest("bases/test-image")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();

        assert_eq!(vm.size, image.len() as u64);
        assert_eq!(vm.block_size, BLOCK_SIZE);
        // All 8 blocks fit in chunk 0 (128 MiB chunks, 1MB image)
        assert_eq!(vm.chunks.len(), 1);
        assert!(vm.chunks.contains_key(&0));

        // Get the pack_id for chunk 0
        let pack_ids = vm.chunk_pack_ids(0).unwrap();
        assert_eq!(pack_ids.len(), 1);
        let pack_id = pack_ids[0];

        // Fetch full pack and parse index
        let pack_bytes = fetch_pack_bytes(&store, "test", 0, pack_id).await;
        let entries = parse_pack_index(&pack_bytes).unwrap().entries;

        assert_eq!(entries.len(), 8);

        // Verify every block can be fetched and matches original data
        for entry in &entries {
            let block_index = entry.chunk_offset as usize;
            let original_start = block_index * BLOCK_SIZE as usize;
            let original_block = &image[original_start..original_start + BLOCK_SIZE as usize];

            // Extract and decompress the block from pack
            let compressed =
                extract_block(&pack_bytes, entry.offset, entry.comp_length).unwrap();
            let decompressed = lz4_decompress(compressed).unwrap();

            assert_eq!(blake3_128(&decompressed), entry.hash);
            assert_eq!(&decompressed[..], original_block);
        }
    }

    #[tokio::test]
    async fn test_bless_idempotent() {
        let (_, cs) = test_store();

        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        for i in 0..4 {
            image[i * BLOCK_SIZE as usize..(i + 1) * BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        // First bless
        let stats1 = bless_bytes(&cs, "idempotent", &image).await.unwrap();
        assert_eq!(stats1.unique_blocks, 4);
        assert!(stats1.packs_uploaded > 0);

        // Second bless of the same image under a different name -- no cross-base
        // dedup in v4, so all blocks are uploaded again as a self-contained base.
        let stats2 = bless_bytes(&cs, "idempotent-2", &image).await.unwrap();
        assert_eq!(
            stats2.unique_blocks, 4,
            "v4 bless uploads all blocks (no cross-base dedup)"
        );
        assert!(
            stats2.packs_uploaded > 0,
            "v4 bless creates packs for every base"
        );
    }

    #[tokio::test]
    async fn test_bless_sparse_image() {
        let (store, cs) = test_store();

        // 4-block image: first block has data, rest are zeros
        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        image[..BLOCK_SIZE as usize].fill(0xAB);

        let stats = bless_bytes(&cs, "sparse", &image).await.unwrap();

        assert_eq!(stats.zero_blocks, 3, "3 zero blocks should be skipped");
        assert_eq!(stats.unique_blocks, 1, "1 data block should be uploaded");
        assert_eq!(stats.packs_uploaded, 1);

        // Verify VolumeManifest only has 1 chunk entry (chunk 0 with 1 pack)
        let manifest_data = cs
            .get_manifest("bases/sparse")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
        assert_eq!(vm.chunks.len(), 1);

        // Verify pack has 1 entry at chunk_offset 0
        let pack_ids = vm.chunk_pack_ids(0).unwrap();
        assert_eq!(pack_ids.len(), 1);

        let entries = fetch_pack_index(&store, "test", 0, pack_ids[0]).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].chunk_offset, 0);
    }

    #[tokio::test]
    async fn test_bless_generates_hot_set() {
        use crate::block::manifest::deserialize_hot_set;

        let (_, cs) = test_store();

        // 4-block image: block 0 has data, blocks 1-2 are zero, block 3 has data
        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        image[..BLOCK_SIZE as usize].fill(0xAA);
        image[3 * BLOCK_SIZE as usize..4 * BLOCK_SIZE as usize].fill(0xBB);

        bless_bytes(&cs, "hot-test", &image).await.unwrap();

        // Fetch and verify hot set
        let hot_set_data = cs
            .get_hot_set("hot-test")
            .await
            .unwrap()
            .expect("hot set should exist");
        let hot_set = deserialize_hot_set(&hot_set_data).unwrap();

        // Only block indices 0 and 3 are non-zero
        assert_eq!(hot_set, vec![0, 3]);
    }

    #[tokio::test]
    async fn test_bless_within_batch_dedup() {
        let (store, cs) = test_store();

        // 4-block image where block 0 and block 2 have identical content.
        // Within-batch dedup should store only 3 unique blocks in the pack.
        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        image[0..BLOCK_SIZE as usize].fill(0xAA); // block 0
        image[BLOCK_SIZE as usize..2 * BLOCK_SIZE as usize].fill(0xBB); // block 1
        image[2 * BLOCK_SIZE as usize..3 * BLOCK_SIZE as usize].fill(0xAA); // block 2 = same as block 0
        image[3 * BLOCK_SIZE as usize..4 * BLOCK_SIZE as usize].fill(0xCC); // block 3

        let stats = bless_bytes(&cs, "dedup-test", &image).await.unwrap();

        // All 4 blocks are non-zero and counted as unique (within-batch dedup
        // only affects pack assembly, not the stats counter)
        assert_eq!(stats.unique_blocks, 4);
        assert_eq!(stats.packs_uploaded, 1);

        // The pack should contain only 3 entries (blocks 0 and 2 share a hash,
        // so only the first occurrence is stored)
        let manifest_data = cs
            .get_manifest("bases/dedup-test")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
        let pack_ids = vm.chunk_pack_ids(0).unwrap();
        assert_eq!(pack_ids.len(), 1);

        let entries = fetch_pack_index(&store, "test", 0, pack_ids[0]).await;
        assert_eq!(
            entries.len(),
            3,
            "within-batch dedup should store 3 unique blocks"
        );
    }

    #[tokio::test]
    async fn test_bless_self_contained_bases() {
        let (store, cs) = test_store();

        // Image A: 10 blocks of unique data
        let mut image_a = vec![0u8; 10 * BLOCK_SIZE as usize];
        for i in 0..10 {
            image_a[i * BLOCK_SIZE as usize..(i + 1) * BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        // Image B: first 8 blocks same as A, last 2 different
        let mut image_b = image_a.clone();
        image_b[8 * BLOCK_SIZE as usize..9 * BLOCK_SIZE as usize].fill(0xFE);
        image_b[9 * BLOCK_SIZE as usize..10 * BLOCK_SIZE as usize].fill(0xFF);

        // Bless A
        let stats_a = bless_bytes(&cs, "image-a", &image_a).await.unwrap();
        assert_eq!(stats_a.unique_blocks, 10);

        // Bless B -- v4 has no cross-base dedup, so all 10 blocks are uploaded
        let stats_b = bless_bytes(&cs, "image-b", &image_b).await.unwrap();
        assert_eq!(
            stats_b.unique_blocks, 10,
            "v4 bless uploads all blocks (self-contained)"
        );

        // Verify each base is independently readable
        for (name, image) in [("image-a", &image_a), ("image-b", &image_b)] {
            let manifest_data = cs
                .get_manifest(&format!("bases/{}", name))
                .await
                .unwrap()
                .expect("manifest should exist");
            let vm = VolumeManifest::deserialize(&manifest_data).unwrap();

            for (&chunk_idx, entry) in &vm.chunks {
                for &pack_id in &entry.packs {
                    let pack_bytes =
                        fetch_pack_bytes(&store, "test", chunk_idx, pack_id).await;
                    let index = parse_pack_index(&pack_bytes).unwrap();

                    for pie in &index.entries {
                        let block_index = chunk_idx as usize
                            * vm.blocks_per_chunk() as usize
                            + pie.chunk_offset as usize;
                        let original_start = block_index * BLOCK_SIZE as usize;
                        let original_block =
                            &image[original_start..original_start + BLOCK_SIZE as usize];

                        let compressed = extract_block(
                            &pack_bytes,
                            pie.offset,
                            pie.comp_length,
                        )
                        .unwrap();
                        let decompressed = lz4_decompress(compressed).unwrap();
                        assert_eq!(
                            &decompressed[..],
                            original_block,
                            "data mismatch at block {} in base {}",
                            block_index,
                            name,
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_bless_block_lookup_by_chunk_offset() {
        let (store, cs) = test_store();

        // Create a 4-block image with distinct patterns
        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        for i in 0..4 {
            image[i * BLOCK_SIZE as usize..(i + 1) * BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        bless_bytes(&cs, "lookup-test", &image).await.unwrap();

        let manifest_data = cs
            .get_manifest("bases/lookup-test")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
        let pack_ids = vm.chunk_pack_ids(0).unwrap();
        let pack_bytes = fetch_pack_bytes(&store, "test", 0, pack_ids[0]).await;
        let index = parse_pack_index(&pack_bytes).unwrap();

        // Look up each block by chunk_offset and verify data
        for offset in 0..4u32 {
            let (hash, pack_offset, comp_length) =
                lookup_block_in_index(&index.entries, offset)
                    .unwrap_or_else(|| panic!("block at chunk_offset {} not found", offset));

            let compressed =
                extract_block(&pack_bytes, pack_offset, comp_length).unwrap();
            let decompressed = lz4_decompress(compressed).unwrap();

            assert_eq!(blake3_128(&decompressed), hash);
            let expected = vec![(offset + 1) as u8; BLOCK_SIZE as usize];
            assert_eq!(decompressed, expected);
        }
    }
}
