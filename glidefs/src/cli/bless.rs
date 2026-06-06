#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
use crate::block::cache::{BlockCache, FoyerBlockCache, FoyerCacheConfig};
use crate::block::content_store::ContentStore;
use crate::block::handler::BlockHandler;
use crate::block::manifest::serialize_hot_set;
use crate::block::metrics::ExportMetrics;
use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
use crate::block::pack_index_cache::PackIndexCache;
use crate::block::volume_manifest::VolumeManifest;
use crate::block::write_cache::{WriteCache, WriteCacheConfig};
use crate::config::Settings;
use crate::oci::ext4_store::{deterministic_uuid, store_ext4_stream, BLOCK_SIZE};
use crate::oci::ingest::IngestOptions;
use crate::oci::layer_store::{
    ensure_layer_stored, put_image_descriptor, ImageDescriptor,
};
use crate::oci::pull::{pull_image, pull_layer_to_tempfile};
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use ext4::writer::WriterOption;
use oci_registry::{Credentials, RegistryClient};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;
use tokio::sync::Notify;
use tracing::info;

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
    let file = std::fs::File::open(&image_path)
        .with_context(|| format!("Failed to open image {}", image_path.display()))?;
    let device_size = file.metadata()?.len();
    let total_blocks = device_size.div_ceil(u64::from(BLOCK_SIZE)) as usize;

    info!(device_size, total_blocks, "reading image");

    // --- Stream image: read blocks, upload each chunk as it completes ---
    let (volume_manifest, hot_set_indices, stats) =
        store_ext4_stream(&content_store, file, device_size).await?;

    // --- Upload manifest ---
    let manifest_key = format!("bases/{}", name);
    content_store
        .put_manifest(&manifest_key, volume_manifest.serialize()?, None)
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

/// Bless an OCI image into a content-addressed base image.
///
/// Pulls layers from the registry, converts to ext4, writes through
/// BlockHandler → WriteCache, drains to S3, then generates a hot set
/// and saves the manifest as a base.
pub async fn run_bless_oci(
    image_ref: String,
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

    // --- S3 setup (same as run_bless) ---
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

    // --- Resolve OCI image to get layer sizes for device size estimation ---
    let registry_client = RegistryClient::new();
    let image: oci_registry::Reference = image_ref
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid image reference: {e}"))?;

    info!(image = %image_ref, name = %name, "resolving OCI image");

    let resolved = registry_client
        .resolve(&image, &Credentials::Anonymous)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve image: {e}"))?;

    // Estimate device size: sum compressed layer sizes × 3 (decompression + ext4 overhead).
    // Round up to next power-of-2 MiB boundary. Minimum 64 MiB.
    let total_compressed: u64 = resolved.layers.iter().map(|l| l.size as u64).sum();
    let estimated = (total_compressed * 3).max(64 * 1024 * 1024);
    let device_size = estimated.next_power_of_two();

    info!(
        layers = resolved.layers.len(),
        total_compressed,
        device_size,
        "estimated device size"
    );

    // --- Create temporary export infrastructure ---
    let temp_dir = tempfile::TempDir::new().context("failed to create temp dir")?;
    let cache_config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: format!("bless-oci-{}", name),
        device_size,
        block_size: BLOCK_SIZE as usize,
        wal_sync: false,
    };

    let cache = Arc::new(WriteCache::open_fresh_active(cache_config)?);

    let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        device_size, BLOCK_SIZE,
    )));

    let pack_index_cache = Arc::new(
        PackIndexCache::open(temp_dir.path())
            .await
            .context("failed to open pack index cache")?,
    );

    // Minimal clean cache — OCI ingest is write-only, no reads from S3.
    let foyer_dir = temp_dir.path().join("foyer");
    std::fs::create_dir_all(&foyer_dir)?;
    let clean_cache: Arc<dyn BlockCache> = Arc::new(
        FoyerBlockCache::open(FoyerCacheConfig {
            memory_bytes: 4 * 1024 * 1024,
            ssd_bytes: 16 * 1024 * 1024,
            ssd_dir: foyer_dir,
            direct: false, // ephemeral CLI cache; buffered is fine
            io_uring: false, // ephemeral CLI cache; psync avoids the idle-spin
        })
        .await
        .context("failed to open block cache")?,
    );

    let metrics = Arc::new(ExportMetrics::new());
    let flush_notify = Arc::new(Notify::const_new());

    let handler = Arc::new(BlockHandler::new(
        Arc::clone(&cache),
        Arc::clone(&content_store),
        Arc::clone(&clean_cache),
        Arc::clone(&pack_index_cache),
        Arc::clone(&volume_manifest),
        device_size,
        false,
        metrics,
        Arc::new(AtomicU64::new(0f64.to_bits())),
        flush_notify,
        DEFAULT_FLUSH_THRESHOLD,
        None,
    ));

    // --- Pull + ingest OCI image ---
    // Derive the filesystem UUID deterministically from the resolved manifest
    // digest so that blessing the same image (same content-addressed manifest)
    // produces a byte-for-byte identical ext4 image every time. The UUID feeds
    // the superblock and the directory hash seed, so a random UUID would make
    // the whole pipeline non-reproducible.
    let uuid = deterministic_uuid(&resolved.manifest_digest);
    let ingest_opts = IngestOptions {
        writer_options: vec![
            WriterOption::MaximumDiskSize(device_size as i64),
            WriterOption::Uuid(uuid),
            WriterOption::Journal(1024), // 4 MiB journal
        ],
    };

    info!("pulling and ingesting layers");

    pull_image(
        &registry_client,
        &image,
        &Credentials::Anonymous,
        Arc::clone(&handler),
        ingest_opts,
    )
    .await
    .map_err(|e| anyhow::anyhow!("pull failed: {e}"))?;

    // --- Drain to S3 ---
    info!("draining to S3");

    let max_drain_iterations = 100;
    let mut drained = false;
    for i in 0..max_drain_iterations {
        let stats = cache
            .flush_to_s3(&content_store, &pack_index_cache, &volume_manifest)
            .await
            .map_err(|e| anyhow::anyhow!("flush failed: {e}"))?;
        if stats.blocks_claimed == 0 {
            info!(iterations = i + 1, "drain complete");
            drained = true;
            break;
        }
    }
    if !drained {
        anyhow::bail!(
            "drain did not converge after {max_drain_iterations} iterations — \
             cache still has dirty blocks, refusing to upload incomplete manifest"
        );
    }

    // --- Generate hot set from VolumeManifest ---
    let hot_set = {
        // Collect chunk data under the read lock, then release before awaiting.
        let (blocks_per_chunk, chunk_packs): (u64, Vec<(u32, Vec<u64>)>) = {
            let vm = volume_manifest.read();
            let bpc = u64::from(vm.blocks_per_chunk());
            let cp = vm
                .chunks
                .iter()
                .map(|(&idx, entry)| (idx, entry.packs.clone()))
                .collect();
            (bpc, cp)
        };

        let mut indices: Vec<u64> = Vec::new();
        for (chunk_idx, packs) in &chunk_packs {
            for &pack_id in packs {
                match pack_index_cache.get_entries(pack_id).await {
                    Some(entries) => {
                        for e in entries.iter() {
                            let global_block = u64::from(*chunk_idx) * blocks_per_chunk + u64::from(e.chunk_offset);
                            indices.push(global_block);
                        }
                    }
                    None => {
                        tracing::warn!(pack_id, chunk_idx, "pack index missing from cache, hot set may be incomplete");
                    }
                }
            }
        }

        indices.sort_unstable();
        indices.dedup();
        indices
    };

    let hot_set_data = serialize_hot_set(&hot_set);
    content_store
        .put_hot_set(&name, hot_set_data)
        .await
        .map_err(|e| anyhow::anyhow!("failed to upload hot set: {e}"))?;

    // --- Save manifest as base ---
    let manifest_key = format!("bases/{}", name);
    let manifest_data = volume_manifest.read().serialize()?;
    content_store
        .put_manifest(&manifest_key, manifest_data, None)
        .await
        .map_err(|e| anyhow::anyhow!("failed to upload manifest: {e}"))?;

    let elapsed = start.elapsed();

    println!("Blessed '{}' from OCI image successfully:", name);
    println!("  Image:           {}", image_ref);
    println!("  Layers:          {}", resolved.layers.len());
    println!("  Device size:     {:.1} MB", device_size as f64 / 1e6);
    println!("  Hot set blocks:  {}", hot_set.len());
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());
    println!("  Manifest:        manifests/{}", manifest_key);

    Ok(())
}

/// Bless an OCI image as **content-addressed layers** (layers survive).
///
/// Each layer is converted independently to a deterministic, overlay-preserving
/// ext4 and stored once under a global `layers/{digest}` namespace; the image is
/// recorded as an ordered list of layer digests under `images/{name}`. Two
/// images that share a layer share its storage — the dedup that flattening into
/// one merged ext4 cannot achieve.
pub async fn run_bless_oci_layered(
    image_ref: String,
    name: String,
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

    // --- S3 setup ---
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

    // --- Resolve image ---
    let registry_client = RegistryClient::new();
    let image: oci_registry::Reference = image_ref
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid image reference: {e}"))?;

    info!(image = %image_ref, name = %name, "resolving OCI image (layered)");
    let resolved = registry_client
        .resolve(&image, &Credentials::Anonymous)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve image: {e}"))?;

    // --- Store each layer once (content-addressed) ---
    let mut layer_digests = Vec::with_capacity(resolved.layers.len());
    let mut layer_sizes = Vec::with_capacity(resolved.layers.len());
    let mut total_stored: u64 = 0;
    let mut reused = 0usize;

    for (i, layer) in resolved.layers.iter().enumerate() {
        info!(layer = i, digest = %layer.digest, size = layer.size, "ensuring layer");
        let decompressed =
            pull_layer_to_tempfile(&registry_client, &image, layer, &Credentials::Anonymous)
                .await
                .with_context(|| format!("pull layer {}", layer.digest))?;
        let stored = ensure_layer_stored(&object_store, &db_path, &layer.digest, decompressed)
            .await
            .with_context(|| format!("store layer {}", layer.digest))?;
        if stored.already_present {
            reused += 1;
        }
        total_stored += stored.stored_bytes;
        layer_digests.push(layer.digest.clone());
        layer_sizes.push(layer.size as u64);
    }

    // --- Record the image descriptor ---
    let descriptor = ImageDescriptor {
        image_ref: image_ref.clone(),
        config_digest: resolved.manifest.config.digest.clone(),
        layers: layer_digests,
        layer_sizes,
    };
    put_image_descriptor(&object_store, &db_path, &name, &descriptor)
        .await
        .context("write image descriptor")?;

    let elapsed = start.elapsed();
    println!("Blessed '{}' from OCI image (layered) successfully:", name);
    println!("  Image:           {}", image_ref);
    println!("  Layers:          {}", resolved.layers.len());
    println!("  Layers reused:   {} (already stored)", reused);
    println!("  Bytes uploaded:  {:.1} MB", total_stored as f64 / 1e6);
    println!("  Elapsed:         {:.1}s", elapsed.as_secs_f64());
    println!("  Descriptor:      images/{}", name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::block_map::{blake3_128, lz4_decompress};
    use crate::block::pack::{extract_block, lookup_block_in_index, parse_pack_index, PackId};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::ObjectStore;

    #[test]
    fn deterministic_uuid_is_stable_and_content_addressed() {
        let digest = "sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        // Same digest → same UUID, every time.
        assert_eq!(deterministic_uuid(digest), deterministic_uuid(digest));

        // Different digest → different UUID (no collision on a trivial change).
        let other = "sha256:00000000000000000000000000000000000000000000000000000000deadbeef";
        assert_ne!(deterministic_uuid(digest), deterministic_uuid(other));

        // Well-formed RFC 4122 v8 UUID: version nibble = 8, variant top bits = 10.
        let uuid = deterministic_uuid(digest);
        assert_eq!(uuid[6] & 0xf0, 0x80, "version must be 8");
        assert_eq!(uuid[8] & 0xc0, 0x80, "variant must be RFC 4122");

        // No randomness leaked in: the value is a pure function of the digest,
        // so it is reproducible across process runs (regression guard against
        // reintroducing rand::random()).
        assert_eq!(
            deterministic_uuid("sha256:abc"),
            deterministic_uuid("sha256:abc"),
        );
    }

    /// Helper: run the bless pipeline directly against an InMemory object store.
    async fn bless_bytes(
        content_store: &ContentStore,
        name: &str,
        image_data: &[u8],
    ) -> Result<crate::oci::ext4_store::StoreStats> {
        let device_size = image_data.len() as u64;
        let content_store = Arc::new(ContentStore::new(
            content_store.object_store().clone(),
            content_store.base_path(),
        ));

        let (volume_manifest, hot_set_indices, stats) =
            store_ext4_stream(&content_store, std::io::Cursor::new(image_data.to_vec()), device_size)
                .await?;

        content_store
            .put_manifest(&format!("bases/{}", name), volume_manifest.serialize()?, None)
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
        let (manifest_data, _) = cs
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
        let (manifest_data, _) = cs
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

        // The pack must contain 4 entries — one per block offset — even though
        // blocks 0 and 2 share a hash. Every block offset needs a pack index
        // entry so the read path can resolve it; missing entries return zeros.
        let (manifest_data, _) = cs
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
            4,
            "every block offset must have a pack index entry, even with duplicate hashes"
        );

        // Verify all 4 offsets are present
        let offsets: Vec<u32> = entries.iter().map(|e| e.chunk_offset).collect();
        for expected in 0..4u32 {
            assert!(
                offsets.contains(&expected),
                "missing pack index entry for chunk_offset {}",
                expected,
            );
        }
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
            let (manifest_data, _) = cs
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

        let (manifest_data, _) = cs
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
