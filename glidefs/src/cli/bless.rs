use crate::block::block_map::{blake3_128, lz4_compress, zero_block_hash, Blake3Hash};
use crate::block::chunk_meta::{ChunkMeta, ChunkMetaEntry};
use crate::block::content_store::ContentStore;
use crate::block::manifest::serialize_hot_set;
use crate::block::pack::assemble_pack;
use crate::block::volume_manifest::{VolumeManifest, DEFAULT_CHUNK_SIZE};
use crate::config::Settings;
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

/// Fixed block size for the chunked architecture: 128KB.
const BLOCK_SIZE: u32 = 131_072;

pub async fn run_bless(image_path: PathBuf, name: String, s3_prefix: String, config_path: PathBuf) -> Result<()> {
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
    let content_store = ContentStore::new(Arc::clone(&object_store), &base);

    info!(image = %image_path.display(), name = %name, "starting bless");

    // --- Load existing base manifests for cross-image dedup ---
    let (known_hashes, existing_entries) =
        load_dedup_index(&content_store).await?;

    // --- Read image ---
    let mut file = std::fs::File::open(&image_path)
        .with_context(|| format!("Failed to open image {}", image_path.display()))?;
    let device_size = file.metadata()?.len();

    let volume_manifest_template = VolumeManifest::new(device_size, BLOCK_SIZE);
    let blocks_per_chunk = volume_manifest_template.blocks_per_chunk();
    let total_blocks = device_size.div_ceil(BLOCK_SIZE as u64) as usize;

    info!(device_size, total_blocks, blocks_per_chunk, "reading image");

    // --- Stream image through pipeline: read blocks and group by chunk ---
    let zero_hash = zero_block_hash(BLOCK_SIZE as usize);
    let mut buf = vec![0u8; BLOCK_SIZE as usize];

    // Per-chunk accumulator: chunk_idx -> list of (block_offset_in_chunk, hash, compressed_data_or_None)
    let mut per_chunk_blocks: BTreeMap<u32, Vec<BlockInfo>> = BTreeMap::new();
    let mut stats = BlessStats::default();
    let mut hot_set_indices: Vec<u64> = Vec::new();

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

        // Check if already known from existing bases
        let is_deduped = known_hashes.contains(&hash);
        if is_deduped {
            stats.deduped_blocks += 1;
        } else {
            stats.unique_blocks += 1;
        }

        let compressed = if is_deduped { None } else { Some(lz4_compress(&buf)) };

        per_chunk_blocks.entry(chunk_idx).or_default().push(BlockInfo {
            block_offset,
            hash,
            compressed,
        });
    }

    // --- Per-chunk processing: assemble packs, build ChunkMeta, upload ---
    let mut volume_manifest = VolumeManifest::new(device_size, BLOCK_SIZE);

    for (chunk_idx, blocks) in &per_chunk_blocks {
        let chunk_idx = *chunk_idx;

        // Separate new blocks (need upload) from deduped blocks (have existing pack locations)
        let mut new_blocks: Vec<(Blake3Hash, Vec<u8>)> = Vec::new();
        let mut within_batch_seen: HashSet<Blake3Hash> = HashSet::new();

        for block in blocks {
            if let Some(ref compressed) = block.compressed {
                // Only upload if not already seen within this chunk's batch
                if within_batch_seen.insert(block.hash) {
                    new_blocks.push((block.hash, compressed.clone()));
                }
            }
        }

        // Assemble all new blocks in this chunk as a single pack (1-chunk packs).
        // Base images are known upfront and read-only — fewer, larger packs mean
        // fewer S3 objects, faster ingestion, and less GC work with no read penalty
        // (blocks are fetched via S3 range GET regardless of pack size).
        let mut hash_to_pack_loc: HashMap<Blake3Hash, (Uuid, u32, u32)> = HashMap::new();

        if !new_blocks.is_empty() {
            let pack_id = Uuid::new_v4();
            let (pack_bytes, index_entries) =
                assemble_pack(new_blocks, BLOCK_SIZE)?;
            let pack_size = pack_bytes.len() as u64;

            content_store
                .put_chunk_pack(chunk_idx, pack_id, pack_bytes)
                .await
                .context("Failed to upload chunk pack")?;

            for entry in &index_entries {
                hash_to_pack_loc.insert(
                    entry.hash,
                    (pack_id, entry.offset, entry.comp_length),
                );
            }

            stats.packs_uploaded += 1;
            stats.bytes_uploaded += pack_size;
        }

        // Build ChunkMetaEntry for every non-zero block in this chunk
        let mut entries: Vec<ChunkMetaEntry> = Vec::with_capacity(blocks.len());

        for block in blocks {
            // Try newly uploaded packs first, then existing entries from dedup
            let pack_loc = hash_to_pack_loc
                .get(&block.hash)
                .copied()
                .or_else(|| existing_entries.get(&block.hash).copied());

            if let Some((pack_id, pack_offset, comp_length)) = pack_loc {
                entries.push(ChunkMetaEntry {
                    offset: block.block_offset,
                    hash: block.hash,
                    pack_id,
                    pack_offset,
                    comp_length,
                });
            }
        }

        entries.sort_by_key(|e| e.offset);

        let chunk_meta = ChunkMeta {
            chunk_idx,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: BLOCK_SIZE,
            entries,
        };

        let chunk_hash = chunk_meta.content_hash();
        let meta_bytes = chunk_meta.serialize();
        let hash_hex = format_hash(&chunk_hash);

        content_store
            .put_chunk_meta(chunk_idx, &hash_hex, meta_bytes)
            .await
            .context("Failed to upload chunk meta")?;

        volume_manifest.set_chunk_hash(chunk_idx, chunk_hash);
    }

    // --- Upload VolumeManifest ---
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
        deduped_blocks = stats.deduped_blocks,
        unique_blocks = stats.unique_blocks,
        packs_uploaded = stats.packs_uploaded,
        bytes_uploaded = stats.bytes_uploaded,
        chunks_written = per_chunk_blocks.len(),
        elapsed_secs = elapsed.as_secs_f64(),
        "bless complete"
    );

    println!("Blessed '{}' successfully:", name);
    println!("  Image size:      {:.1} GB", device_size as f64 / 1e9);
    println!("  Total blocks:    {}", total_blocks);
    println!("  Zero blocks:     {} (skipped)", stats.zero_blocks);
    println!(
        "  Deduped blocks:  {} (already in S3)",
        stats.deduped_blocks
    );
    println!("  Unique blocks:   {} (uploaded)", stats.unique_blocks);
    println!("  Packs uploaded:  {}", stats.packs_uploaded);
    println!(
        "  Bytes uploaded:  {:.1} MB",
        stats.bytes_uploaded as f64 / 1e6
    );
    println!("  Chunks written:  {}", per_chunk_blocks.len());
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());
    println!("  Manifest:        manifests/{}", manifest_key);

    Ok(())
}

/// Block info accumulated during the image scan.
struct BlockInfo {
    block_offset: u32,
    hash: Blake3Hash,
    /// Some(compressed) for new blocks, None for deduped blocks.
    compressed: Option<Vec<u8>>,
}

#[derive(Default)]
struct BlessStats {
    zero_blocks: usize,
    deduped_blocks: usize,
    unique_blocks: usize,
    packs_uploaded: usize,
    bytes_uploaded: u64,
}

/// Load existing VolumeManifests and ChunkMetas from S3 to build a dedup index.
///
/// Returns:
/// - `known_hashes`: set of all block hashes across existing bases (for deciding whether to upload)
/// - `existing_entries`: map from block hash -> (pack_id, pack_offset, comp_length) for resolving
///   deduped blocks to their pack locations
async fn load_dedup_index(
    content_store: &ContentStore,
) -> Result<(HashSet<Blake3Hash>, HashMap<Blake3Hash, (Uuid, u32, u32)>)> {
    let base_names = content_store.list_base_manifests().await?;

    if base_names.is_empty() {
        return Ok((HashSet::new(), HashMap::new()));
    }

    info!(
        count = base_names.len(),
        "loading existing base manifests for dedup"
    );

    let mut known_hashes = HashSet::new();
    let mut existing_entries: HashMap<Blake3Hash, (Uuid, u32, u32)> = HashMap::new();

    for base_name in &base_names {
        let key = format!("bases/{}", base_name);
        let Some(data) = content_store.get_manifest(&key).await? else {
            continue;
        };

        let vm = match VolumeManifest::deserialize(&data) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!(name = %base_name, error = %e, "skipping corrupt manifest");
                continue;
            }
        };

        // For each chunk in the manifest, fetch and parse the ChunkMeta
        for (&chunk_idx, chunk_hash_hex) in &vm.chunks {
            let Some(meta_data) = content_store
                .get_chunk_meta(chunk_idx, chunk_hash_hex)
                .await?
            else {
                tracing::warn!(
                    base = %base_name,
                    chunk_idx,
                    "chunk meta not found in S3, skipping"
                );
                continue;
            };

            let chunk_meta = match ChunkMeta::deserialize(&meta_data) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        base = %base_name,
                        chunk_idx,
                        error = %e,
                        "skipping corrupt chunk meta"
                    );
                    continue;
                }
            };

            for entry in &chunk_meta.entries {
                known_hashes.insert(entry.hash);
                existing_entries
                    .entry(entry.hash)
                    .or_insert((entry.pack_id, entry.pack_offset, entry.comp_length));
            }
        }
    }

    info!(
        known_hashes = known_hashes.len(),
        "dedup index loaded from {} base(s)",
        base_names.len()
    );

    Ok((known_hashes, existing_entries))
}

/// Format a Blake3Hash as lowercase hex string.
fn format_hash(hash: &Blake3Hash) -> String {
    hash.0.iter().map(|b| format!("{b:02x}")).collect()
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
    use object_store::memory::InMemory;

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

        // Load dedup index
        let (known_hashes, existing_entries) =
            load_dedup_index(content_store).await?;

        // Scan blocks
        let mut per_chunk_blocks: BTreeMap<u32, Vec<BlockInfo>> = BTreeMap::new();
        let mut stats = BlessStats::default();
        let mut hot_set_indices: Vec<u64> = Vec::new();

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

            let is_deduped = known_hashes.contains(&hash);
            if is_deduped {
                stats.deduped_blocks += 1;
            } else {
                stats.unique_blocks += 1;
            }

            let compressed = if is_deduped {
                None
            } else {
                Some(lz4_compress(&buf))
            };

            per_chunk_blocks.entry(chunk_idx).or_default().push(BlockInfo {
                block_offset,
                hash,
                compressed,
            });
        }

        // Per-chunk processing
        let mut volume_manifest = VolumeManifest::new(device_size, BLOCK_SIZE);

        for (chunk_idx, blocks) in &per_chunk_blocks {
            let chunk_idx = *chunk_idx;

            let mut new_blocks: Vec<(Blake3Hash, Vec<u8>)> = Vec::new();
            let mut within_batch_seen: HashSet<Blake3Hash> = HashSet::new();

            for block in blocks {
                if let Some(ref compressed) = block.compressed
                    && within_batch_seen.insert(block.hash)
                {
                    new_blocks.push((block.hash, compressed.clone()));
                }
            }

            let mut hash_to_pack_loc: HashMap<Blake3Hash, (Uuid, u32, u32)> = HashMap::new();

            if !new_blocks.is_empty() {
                let pack_id = Uuid::new_v4();
                let (pack_bytes, index_entries) =
                    assemble_pack(new_blocks, BLOCK_SIZE)?;
                let pack_size = pack_bytes.len() as u64;

                content_store
                    .put_chunk_pack(chunk_idx, pack_id, pack_bytes)
                    .await?;

                for entry in &index_entries {
                    hash_to_pack_loc.insert(
                        entry.hash,
                        (pack_id, entry.offset, entry.comp_length),
                    );
                }

                stats.packs_uploaded += 1;
                stats.bytes_uploaded += pack_size;
            }

            let mut entries: Vec<ChunkMetaEntry> = Vec::with_capacity(blocks.len());

            for block in blocks {
                let pack_loc = hash_to_pack_loc
                    .get(&block.hash)
                    .copied()
                    .or_else(|| existing_entries.get(&block.hash).copied());

                if let Some((pack_id, pack_offset, comp_length)) = pack_loc {
                    entries.push(ChunkMetaEntry {
                        offset: block.block_offset,
                        hash: block.hash,
                        pack_id,
                        pack_offset,
                        comp_length,
                    });
                }
            }

            entries.sort_by_key(|e| e.offset);

            let chunk_meta = ChunkMeta {
                chunk_idx,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: BLOCK_SIZE,
                entries,
            };

            let chunk_hash = chunk_meta.content_hash();
            let meta_bytes = chunk_meta.serialize();
            let hash_hex = format_hash(&chunk_hash);

            content_store
                .put_chunk_meta(chunk_idx, &hash_hex, meta_bytes)
                .await?;

            volume_manifest.set_chunk_hash(chunk_idx, chunk_hash);
        }

        content_store
            .put_manifest(&format!("bases/{}", name), volume_manifest.serialize())
            .await?;

        let hot_set_data = serialize_hot_set(&hot_set_indices);
        content_store.put_hot_set(name, hot_set_data).await.map_err(|e| anyhow::anyhow!(e))?;

        Ok(stats)
    }

    fn test_store() -> (Arc<InMemory>, ContentStore) {
        let store = Arc::new(InMemory::new());
        let cs = ContentStore::new(store.clone(), "test");
        (store, cs)
    }

    #[tokio::test]
    async fn test_bless_and_read_back() {
        let (_, cs) = test_store();

        // Create a 1MB image with known pattern (8 x 128KB blocks)
        let mut image = vec![0u8; 8 * BLOCK_SIZE as usize];
        for i in 0..8 {
            let start = i * BLOCK_SIZE as usize;
            image[start..start + BLOCK_SIZE as usize].fill((i + 1) as u8);
        }

        let stats = bless_bytes(&cs, "test-image", &image).await.unwrap();

        assert_eq!(stats.zero_blocks, 0);
        assert_eq!(stats.unique_blocks, 8);
        assert_eq!(stats.deduped_blocks, 0);

        // Load VolumeManifest and verify
        let manifest_data = cs
            .get_manifest("bases/test-image")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();

        assert_eq!(vm.size, image.len() as u64);
        assert_eq!(vm.block_size, BLOCK_SIZE);
        // All 8 blocks fit in chunk 0 (10GB chunks, 1MB image)
        assert_eq!(vm.chunks.len(), 1);
        assert!(vm.chunks.contains_key(&0));

        // Load ChunkMeta for chunk 0 and verify blocks
        let chunk_hash_hex = vm.chunks.get(&0).unwrap();
        let meta_data = cs
            .get_chunk_meta(0, chunk_hash_hex)
            .await
            .unwrap()
            .expect("chunk meta should exist");
        let chunk_meta = ChunkMeta::deserialize(&meta_data).unwrap();

        assert_eq!(chunk_meta.entries.len(), 8);

        // Verify every block can be fetched and matches original data
        for entry in &chunk_meta.entries {
            let block_index = entry.offset as usize;
            let original_start = block_index * BLOCK_SIZE as usize;
            let original_block =
                &image[original_start..original_start + BLOCK_SIZE as usize];

            // Fetch the pack, extract and decompress the block
            let pack_data = cs
                .get_chunk_block(0, entry.pack_id, entry.pack_offset, entry.comp_length)
                .await
                .unwrap();
            let decompressed = lz4_decompress(&pack_data).unwrap();

            assert_eq!(blake3_128(&decompressed), entry.hash);
            assert_eq!(&decompressed[..], original_block);
        }
    }

    #[tokio::test]
    async fn test_bless_idempotent() {
        let (_, cs) = test_store();

        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        for i in 0..4 {
            image[i * BLOCK_SIZE as usize..(i + 1) * BLOCK_SIZE as usize]
                .fill((i + 1) as u8);
        }

        // First bless
        let stats1 = bless_bytes(&cs, "idempotent", &image).await.unwrap();
        assert_eq!(stats1.unique_blocks, 4);
        assert!(stats1.packs_uploaded > 0);

        // Second bless -- should dedup everything
        let stats2 = bless_bytes(&cs, "idempotent", &image).await.unwrap();
        assert_eq!(
            stats2.unique_blocks, 0,
            "re-bless should upload zero new blocks"
        );
        assert_eq!(
            stats2.packs_uploaded, 0,
            "re-bless should upload zero new packs"
        );
        assert_eq!(stats2.deduped_blocks, 4, "all blocks should be deduped");
    }

    #[tokio::test]
    async fn test_bless_sparse_image() {
        let (_, cs) = test_store();

        // 4-block image: first block has data, rest are zeros
        let mut image = vec![0u8; 4 * BLOCK_SIZE as usize];
        image[..BLOCK_SIZE as usize].fill(0xAB);

        let stats = bless_bytes(&cs, "sparse", &image).await.unwrap();

        assert_eq!(stats.zero_blocks, 3, "3 zero blocks should be skipped");
        assert_eq!(stats.unique_blocks, 1, "1 data block should be uploaded");
        assert_eq!(stats.packs_uploaded, 1);

        // Verify VolumeManifest only has 1 chunk entry (chunk 0 with 1 block)
        let manifest_data = cs
            .get_manifest("bases/sparse")
            .await
            .unwrap()
            .expect("manifest should exist");
        let vm = VolumeManifest::deserialize(&manifest_data).unwrap();
        assert_eq!(vm.chunks.len(), 1);

        // Verify ChunkMeta has 1 entry
        let chunk_hash_hex = vm.chunks.get(&0).unwrap();
        let meta_data = cs
            .get_chunk_meta(0, chunk_hash_hex)
            .await
            .unwrap()
            .expect("chunk meta should exist");
        let chunk_meta = ChunkMeta::deserialize(&meta_data).unwrap();
        assert_eq!(chunk_meta.entries.len(), 1);
        assert_eq!(chunk_meta.entries[0].offset, 0);
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
    async fn test_bless_cross_image_dedup() {
        let (_, cs) = test_store();

        // Image A: 10 blocks of unique data
        let mut image_a = vec![0u8; 10 * BLOCK_SIZE as usize];
        for i in 0..10 {
            image_a[i * BLOCK_SIZE as usize..(i + 1) * BLOCK_SIZE as usize]
                .fill((i + 1) as u8);
        }

        // Image B: first 8 blocks same as A, last 2 different
        let mut image_b = image_a.clone();
        image_b[8 * BLOCK_SIZE as usize..9 * BLOCK_SIZE as usize].fill(0xFE);
        image_b[9 * BLOCK_SIZE as usize..10 * BLOCK_SIZE as usize].fill(0xFF);

        // Bless A
        let stats_a = bless_bytes(&cs, "image-a", &image_a).await.unwrap();
        assert_eq!(stats_a.unique_blocks, 10);

        // Bless B -- should dedup the 8 shared blocks
        let stats_b = bless_bytes(&cs, "image-b", &image_b).await.unwrap();
        assert_eq!(
            stats_b.deduped_blocks, 8,
            "8 shared blocks should be deduped"
        );
        assert_eq!(
            stats_b.unique_blocks, 2,
            "only 2 new blocks should be uploaded"
        );
    }
}
