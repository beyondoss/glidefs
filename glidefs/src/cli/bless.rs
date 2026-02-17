use crate::config::Settings;
use crate::nbd::block_map::{blake3_128, lz4_compress, BlockMap, BlockMapEntry, ZERO_BLOCK_HASH};
use crate::nbd::content_store::ContentStore;
use crate::nbd::manifest::{serialize_hot_set, Manifest, ManifestBlockEntry};
use crate::nbd::pack::{assemble_pack, PackLocation, BLOCKS_PER_PACK};
use crate::nbd::pack_index::HostPackIndex;
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

pub async fn run_bless(
    image_path: PathBuf,
    name: String,
    config_path: PathBuf,
    chunk_size: u32,
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

    let content_store = ContentStore::new(Arc::clone(&object_store), &db_path);

    info!(image = %image_path.display(), name = %name, chunk_size, "starting bless");

    // --- Load existing base manifests for cross-image dedup ---
    let pack_index = HostPackIndex::new();
    let base_names = content_store.list_base_manifests().await?;
    if !base_names.is_empty() {
        info!(count = base_names.len(), "loading existing base manifests for dedup");
        let mut manifests = Vec::new();
        for base_name in &base_names {
            let key = format!("bases/{}", base_name);
            if let Some(data) = content_store.get_manifest(&key).await? {
                match Manifest::deserialize(&data) {
                    Ok(m) => manifests.push(m),
                    Err(e) => tracing::warn!(name = %base_name, error = %e, "skipping corrupt manifest"),
                }
            }
        }
        pack_index.rebuild(&manifests);
        info!(
            entries = pack_index.len(),
            "dedup index loaded from {} manifest(s)",
            manifests.len()
        );
    }

    // --- Stream image through pipeline ---
    let mut file = std::fs::File::open(&image_path)
        .with_context(|| format!("Failed to open image {}", image_path.display()))?;
    let device_size = file.metadata()?.len();
    let num_chunks = device_size.div_ceil(chunk_size as u64) as usize;

    info!(device_size, num_chunks, "reading image");

    let mut block_entries: Vec<ManifestBlockEntry> = Vec::with_capacity(num_chunks);
    let mut batch: Vec<(crate::nbd::block_map::Blake3Hash, Vec<u8>)> = Vec::with_capacity(BLOCKS_PER_PACK);
    let mut buf = vec![0u8; chunk_size as usize];

    let mut stats = BlessStats::default();

    for chunk_index in 0..num_chunks {
        // Read chunk (last chunk may be short, pad with zeros)
        let bytes_read = read_full(&mut file, &mut buf)?;
        if bytes_read < chunk_size as usize {
            buf[bytes_read..].fill(0);
        }

        let hash = blake3_128(&buf);

        // Skip zero blocks entirely — read path returns zeros by convention
        if hash == *ZERO_BLOCK_HASH {
            stats.zero_chunks += 1;
            continue;
        }

        // Record in manifest block map
        block_entries.push(ManifestBlockEntry {
            chunk_index: chunk_index as u64,
            hash,
            flags: 0,
        });

        // Dedup: skip if already in pack index (from prior base images)
        if pack_index.contains(&hash) {
            stats.deduped_chunks += 1;
            continue;
        }

        // Compress and accumulate
        let compressed = lz4_compress(&buf);
        stats.unique_chunks += 1;
        batch.push((hash, compressed));

        // Flush pack when full
        if batch.len() >= BLOCKS_PER_PACK {
            upload_pack(&content_store, &pack_index, &mut batch, chunk_size, &mut stats).await?;
        }
    }

    // Flush remaining partial pack
    if !batch.is_empty() {
        upload_pack(&content_store, &pack_index, &mut batch, chunk_size, &mut stats).await?;
    }

    // --- Build and upload manifest ---
    let mut block_map = BlockMap::new(device_size, chunk_size);
    for entry in &block_entries {
        block_map.set(
            entry.chunk_index as usize,
            BlockMapEntry {
                hash: entry.hash,
                flags: 0,
                sequence: 1,
            },
        );
    }

    let pack_entries = pack_index.derive_for_block_map(&block_map);

    // Collect hot set chunk indices before block_entries is moved into the manifest
    let hot_set_chunks: Vec<u64> = block_entries.iter().map(|e| e.chunk_index).collect();

    let manifest = Manifest {
        name: name.clone(),
        sequence: 1,
        chunk_size,
        device_size,
        block_map: block_entries,
        pack_index: pack_entries,
    };

    let manifest_data = manifest.serialize();
    let manifest_key = format!("bases/{}", name);
    content_store
        .put_manifest(&manifest_key, manifest_data)
        .await
        .context("Failed to upload manifest")?;

    // Upload boot hot set (all non-zero chunk indices)
    let hot_set_data = serialize_hot_set(&hot_set_chunks);
    content_store
        .put_hot_set(&name, hot_set_data)
        .await
        .context("Failed to upload hot set")?;

    let elapsed = start.elapsed();

    info!(
        name = %name,
        total_chunks = num_chunks,
        zero_chunks = stats.zero_chunks,
        deduped_chunks = stats.deduped_chunks,
        unique_chunks = stats.unique_chunks,
        packs_uploaded = stats.packs_uploaded,
        bytes_uploaded = stats.bytes_uploaded,
        elapsed_secs = elapsed.as_secs_f64(),
        "bless complete"
    );

    println!("Blessed '{}' successfully:", name);
    println!("  Image size:      {:.1} GB", device_size as f64 / 1e9);
    println!("  Total chunks:    {}", num_chunks);
    println!("  Zero chunks:     {} (skipped)", stats.zero_chunks);
    println!("  Deduped chunks:  {} (already in S3)", stats.deduped_chunks);
    println!("  Unique chunks:   {} (uploaded)", stats.unique_chunks);
    println!("  Packs uploaded:  {}", stats.packs_uploaded);
    println!("  Bytes uploaded:  {:.1} MB", stats.bytes_uploaded as f64 / 1e6);
    println!("  Hot set:         {} chunks", hot_set_chunks.len());
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());
    println!("  Manifest:        manifests/{}", manifest_key);

    Ok(())
}

#[derive(Default)]
struct BlessStats {
    zero_chunks: usize,
    deduped_chunks: usize,
    unique_chunks: usize,
    packs_uploaded: usize,
    bytes_uploaded: u64,
}

async fn upload_pack(
    content_store: &ContentStore,
    pack_index: &HostPackIndex,
    batch: &mut Vec<(crate::nbd::block_map::Blake3Hash, Vec<u8>)>,
    chunk_size: u32,
    stats: &mut BlessStats,
) -> Result<()> {
    let pack_id = Uuid::new_v4();
    let (pack_bytes, index_entries) = assemble_pack(batch, chunk_size);
    let pack_size = pack_bytes.len() as u64;

    content_store
        .put_pack(pack_id, pack_bytes)
        .await
        .context("Failed to upload pack")?;

    for entry in &index_entries {
        pack_index.insert(
            entry.hash,
            PackLocation {
                pack_id,
                offset: entry.offset,
                comp_length: entry.comp_length,
            },
        );
    }

    stats.packs_uploaded += 1;
    stats.bytes_uploaded += pack_size;
    batch.clear();
    Ok(())
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
    use crate::nbd::block_map::{lz4_decompress, Blake3Hash};
    use crate::nbd::manifest::deserialize_hot_set;
    use crate::nbd::pack::extract_block;
    use object_store::memory::InMemory;

    const TEST_CHUNK_SIZE: u32 = 131072; // 128KB

    /// Helper: run the bless pipeline directly against an InMemory object store.
    async fn bless_bytes(
        content_store: &ContentStore,
        name: &str,
        image_data: &[u8],
        chunk_size: u32,
    ) -> Result<BlessStats> {
        let pack_index = HostPackIndex::new();

        // Load existing base manifests for dedup
        let base_names = content_store.list_base_manifests().await?;
        if !base_names.is_empty() {
            let mut manifests = Vec::new();
            for base_name in &base_names {
                let key = format!("bases/{}", base_name);
                if let Some(data) = content_store.get_manifest(&key).await? {
                    if let Ok(m) = Manifest::deserialize(&data) {
                        manifests.push(m);
                    }
                }
            }
            pack_index.rebuild(&manifests);
        }

        let device_size = image_data.len() as u64;
        let num_chunks = device_size.div_ceil(chunk_size as u64) as usize;

        let mut block_entries: Vec<ManifestBlockEntry> = Vec::new();
        let mut batch: Vec<(Blake3Hash, Vec<u8>)> = Vec::new();
        let mut stats = BlessStats::default();

        for chunk_index in 0..num_chunks {
            let start = chunk_index * chunk_size as usize;
            let end = (start + chunk_size as usize).min(image_data.len());
            let mut buf = vec![0u8; chunk_size as usize];
            buf[..end - start].copy_from_slice(&image_data[start..end]);

            let hash = blake3_128(&buf);

            if hash == *ZERO_BLOCK_HASH {
                stats.zero_chunks += 1;
                continue;
            }

            block_entries.push(ManifestBlockEntry {
                chunk_index: chunk_index as u64,
                hash,
                flags: 0,
            });

            if pack_index.contains(&hash) {
                stats.deduped_chunks += 1;
                continue;
            }

            let compressed = lz4_compress(&buf);
            stats.unique_chunks += 1;
            batch.push((hash, compressed));

            if batch.len() >= BLOCKS_PER_PACK {
                upload_pack(content_store, &pack_index, &mut batch, chunk_size, &mut stats).await?;
            }
        }

        if !batch.is_empty() {
            upload_pack(content_store, &pack_index, &mut batch, chunk_size, &mut stats).await?;
        }

        // Build manifest
        let mut block_map = BlockMap::new(device_size, chunk_size);
        for entry in &block_entries {
            block_map.set(
                entry.chunk_index as usize,
                BlockMapEntry {
                    hash: entry.hash,
                    flags: 0,
                    sequence: 1,
                },
            );
        }

        let pack_entries = pack_index.derive_for_block_map(&block_map);

        // Collect hot set chunk indices before block_entries is moved into the manifest
        let hot_set_chunks: Vec<u64> = block_entries.iter().map(|e| e.chunk_index).collect();

        let manifest = Manifest {
            name: name.to_string(),
            sequence: 1,
            chunk_size,
            device_size,
            block_map: block_entries,
            pack_index: pack_entries,
        };

        content_store
            .put_manifest(&format!("bases/{}", name), manifest.serialize())
            .await?;

        // Upload boot hot set (all non-zero chunk indices)
        let hot_set_data = serialize_hot_set(&hot_set_chunks);
        content_store.put_hot_set(name, hot_set_data).await?;

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

        // Create a 1MB image with known pattern (8 x 128KB chunks)
        let mut image = vec![0u8; 8 * TEST_CHUNK_SIZE as usize];
        for i in 0..8 {
            let start = i * TEST_CHUNK_SIZE as usize;
            // Fill each chunk with a distinct byte pattern
            image[start..start + TEST_CHUNK_SIZE as usize].fill((i + 1) as u8);
        }

        let stats = bless_bytes(&cs, "test-image", &image, TEST_CHUNK_SIZE)
            .await
            .unwrap();

        assert_eq!(stats.zero_chunks, 0);
        assert_eq!(stats.unique_chunks, 8);
        assert_eq!(stats.deduped_chunks, 0);

        // Load manifest and verify
        let manifest_data = cs
            .get_manifest("bases/test-image")
            .await
            .unwrap()
            .expect("manifest should exist");
        let manifest = Manifest::deserialize(&manifest_data).unwrap();

        assert_eq!(manifest.name, "test-image");
        assert_eq!(manifest.block_map.len(), 8);
        assert_eq!(manifest.device_size, image.len() as u64);

        // Verify every block can be fetched and matches original data
        for entry in &manifest.block_map {
            let chunk_index = entry.chunk_index as usize;
            let original_start = chunk_index * TEST_CHUNK_SIZE as usize;
            let original_chunk = &image[original_start..original_start + TEST_CHUNK_SIZE as usize];

            // Find the pack entry for this hash
            let pack_entry = manifest
                .pack_index
                .iter()
                .find(|pe| pe.hash == entry.hash)
                .expect("block should have a pack entry");

            // Fetch the pack, extract and decompress the block
            let pack_data = cs.get_pack(pack_entry.pack_id).await.unwrap();
            let compressed = extract_block(&pack_data, pack_entry.offset, pack_entry.comp_length)
                .expect("block should be extractable from pack");
            let decompressed = lz4_decompress(compressed).unwrap();

            assert_eq!(blake3_128(&decompressed), entry.hash);
            assert_eq!(&decompressed[..], original_chunk);
        }
    }

    #[tokio::test]
    async fn test_bless_idempotent() {
        let (_, cs) = test_store();

        let mut image = vec![0u8; 4 * TEST_CHUNK_SIZE as usize];
        for i in 0..4 {
            image[i * TEST_CHUNK_SIZE as usize..(i + 1) * TEST_CHUNK_SIZE as usize]
                .fill((i + 1) as u8);
        }

        // First bless
        let stats1 = bless_bytes(&cs, "idempotent", &image, TEST_CHUNK_SIZE)
            .await
            .unwrap();
        assert_eq!(stats1.unique_chunks, 4);
        assert!(stats1.packs_uploaded > 0);

        // Second bless — should dedup everything
        let stats2 = bless_bytes(&cs, "idempotent", &image, TEST_CHUNK_SIZE)
            .await
            .unwrap();
        assert_eq!(stats2.unique_chunks, 0, "re-bless should upload zero new blocks");
        assert_eq!(stats2.packs_uploaded, 0, "re-bless should upload zero new packs");
        assert_eq!(stats2.deduped_chunks, 4, "all blocks should be deduped");
    }

    #[tokio::test]
    async fn test_bless_sparse_image() {
        let (_, cs) = test_store();

        // 4-chunk image: first chunk has data, rest are zeros
        let mut image = vec![0u8; 4 * TEST_CHUNK_SIZE as usize];
        image[..TEST_CHUNK_SIZE as usize].fill(0xAB);

        let stats = bless_bytes(&cs, "sparse", &image, TEST_CHUNK_SIZE)
            .await
            .unwrap();

        assert_eq!(stats.zero_chunks, 3, "3 zero chunks should be skipped");
        assert_eq!(stats.unique_chunks, 1, "1 data chunk should be uploaded");
        assert_eq!(stats.packs_uploaded, 1);

        // Verify manifest only has 1 block entry (zero chunks excluded)
        let manifest_data = cs
            .get_manifest("bases/sparse")
            .await
            .unwrap()
            .expect("manifest should exist");
        let manifest = Manifest::deserialize(&manifest_data).unwrap();
        assert_eq!(manifest.block_map.len(), 1);
        assert_eq!(manifest.block_map[0].chunk_index, 0);
    }

    #[tokio::test]
    async fn test_bless_cross_image_dedup() {
        let (_, cs) = test_store();

        // Image A: 10 chunks of unique data
        let mut image_a = vec![0u8; 10 * TEST_CHUNK_SIZE as usize];
        for i in 0..10 {
            image_a[i * TEST_CHUNK_SIZE as usize..(i + 1) * TEST_CHUNK_SIZE as usize]
                .fill((i + 1) as u8);
        }

        // Image B: first 8 chunks same as A, last 2 different
        let mut image_b = image_a.clone();
        image_b[8 * TEST_CHUNK_SIZE as usize..9 * TEST_CHUNK_SIZE as usize].fill(0xFE);
        image_b[9 * TEST_CHUNK_SIZE as usize..10 * TEST_CHUNK_SIZE as usize].fill(0xFF);

        // Bless A
        let stats_a = bless_bytes(&cs, "image-a", &image_a, TEST_CHUNK_SIZE)
            .await
            .unwrap();
        assert_eq!(stats_a.unique_chunks, 10);

        // Bless B — should dedup the 8 shared chunks
        let stats_b = bless_bytes(&cs, "image-b", &image_b, TEST_CHUNK_SIZE)
            .await
            .unwrap();
        assert_eq!(stats_b.deduped_chunks, 8, "8 shared chunks should be deduped");
        assert_eq!(stats_b.unique_chunks, 2, "only 2 new chunks should be uploaded");
    }

    #[test]
    fn test_hot_set_round_trip() {
        let chunks = vec![0, 5, 10, 100, 50000];
        let data = serialize_hot_set(&chunks);
        let restored = deserialize_hot_set(&data).unwrap();
        assert_eq!(restored, chunks);
    }

    #[test]
    fn test_hot_set_empty() {
        let chunks: Vec<u64> = vec![];
        let data = serialize_hot_set(&chunks);
        let restored = deserialize_hot_set(&data).unwrap();
        assert!(restored.is_empty());
    }

    #[tokio::test]
    async fn test_bless_uploads_hot_set() {
        let (_, cs) = test_store();

        let mut image = vec![0u8; 4 * TEST_CHUNK_SIZE as usize];
        for i in 0..4 {
            image[i * TEST_CHUNK_SIZE as usize..(i + 1) * TEST_CHUNK_SIZE as usize]
                .fill((i + 1) as u8);
        }

        bless_bytes(&cs, "hot-set-test", &image, TEST_CHUNK_SIZE)
            .await
            .unwrap();

        // Verify hot set was uploaded
        let hot_set_data = cs
            .get_hot_set("hot-set-test")
            .await
            .unwrap()
            .expect("hot set should exist");
        let chunks = deserialize_hot_set(&hot_set_data).unwrap();
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks, vec![0, 1, 2, 3]);
    }
}
