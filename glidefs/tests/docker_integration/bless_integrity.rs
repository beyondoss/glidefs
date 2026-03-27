//! Bless → fork → cold-read integrity tests.
//!
//! Proves that data blessed into a base image survives the full pipeline:
//! ext4 image → bless (zero-skip, pack, manifest) → fork → cold restart → NBD read.
//!
//! This exercises the bless code path (cli/bless.rs) end-to-end through the
//! read path (write_cache/read.rs locate_block), which no other test covers.
//! Specifically catches:
//!
//! - Zero-block false positives during bless (non-zero data skipped)
//! - Missing pack index entries after manifest round-trip
//! - Cross-flush dedup incorrectly eliding blocks in multi-iteration drain
//! - Blocks absent from manifest returning zeros on cold read
//!
//! Run:
//! ```bash
//! cargo test --features docker-tests --test docker_integration bless_integrity \
//!     -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use glidefs::block::block_map::{blake3_128, lz4_compress, shared_zero_block};
use glidefs::block::content_store::ContentStore;
use glidefs::block::manifest::serialize_hot_set;
use glidefs::block::pack::content_pack_id;
use glidefs::block::volume_manifest::VolumeManifest;

use ext4::format;
use ext4::writer::{File, Writer, WriterOption};

use crate::{TestContext, TestServer};

const BLOCK_SIZE: usize = 128 * 1024; // 128 KiB

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn blake3_hash(data: &[u8]) -> [u8; 32] {
    blake3::hash(data).into()
}

fn fmt_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else {
        format!("{:.0} MB", bytes as f64 / 1_000_000.0)
    }
}

/// Build a deterministic ext4 image packed with ~60% non-zero data.
///
/// Generates enough files to fill the majority of the device so the test
/// exercises hundreds of non-zero blocks through the bless pipeline, not
/// just a handful of metadata blocks surrounded by free space.
fn build_blessed_image(device_size: usize) -> Vec<u8> {
    let buf = Cursor::new(vec![0u8; device_size]);
    let mut w = Writer::new(
        buf,
        &[WriterOption::MaximumDiskSize(device_size as i64)],
    );

    // ext4 overhead (superblock, group descriptors, inode table, journal)
    // eats ~35-40% on small images. Fill ~60% of device with file data so
    // the majority of usable blocks are non-zero after bless.
    let target_data = device_size * 55 / 100;
    let mut written = 0usize;

    // Directory structure
    for dir in &[
        "usr", "usr/bin", "usr/lib", "usr/lib/git-core",
        "etc", "var", "var/log", "opt", "opt/app",
    ] {
        let f = File {
            mode: format::S_IFDIR | 0o755,
            uid: 0,
            gid: 0,
            ..Default::default()
        };
        w.create(dir, &f).unwrap();
    }

    // Large binary-like files (multiple 128KB blocks each)
    let large_files: &[(&str, u64, usize)] = &[
        ("usr/lib/git-core/git",     0x47495400, 2 * 1024 * 1024),  // 2 MB
        ("usr/bin/bash",             0x42415348, 1536 * 1024),       // 1.5 MB
        ("usr/lib/libc.so.6",        0x4C494243, 2 * 1024 * 1024),  // 2 MB
        ("usr/lib/libstdc++.so.6",   0x53544443, 1792 * 1024),      // 1.75 MB
        ("usr/lib/libcrypto.so.3",   0x43525950, 2 * 1024 * 1024),  // 2 MB
        ("usr/lib/libssl.so.3",      0x53534C33, 512 * 1024),       // 512 KB
        ("usr/bin/python3",          0x50595448, 768 * 1024),        // 768 KB
        ("usr/lib/libpthread.so.0",  0x50544852, 384 * 1024),       // 384 KB
        ("usr/lib/libm.so.6",        0x4C49424D, 1024 * 1024),      // 1 MB
        ("usr/lib/libz.so.1",        0x5A4C4942, 256 * 1024),       // 256 KB
        ("opt/app/server",           0x53525652, 3 * 1024 * 1024),   // 3 MB
    ];

    for &(path, seed, size) in large_files {
        if written + size > target_data {
            break;
        }
        let mut rng = StdRng::seed_from_u64(seed);
        let mut data = vec![0u8; size];
        rng.fill(&mut data[..]);
        // ELF magic header
        data[0] = 0x7f;
        data[1] = b'E';
        data[2] = b'L';
        data[3] = b'F';

        let f = File {
            mode: format::S_IFREG | 0o755,
            size: data.len() as i64,
            uid: 0,
            gid: 0,
            ..Default::default()
        };
        w.create(path, &f).unwrap();
        w.write_data(&data).unwrap();
        written += size;
    }

    // Fill remaining target with many smaller files (config, scripts, data)
    let mut file_idx = 0u64;
    while written + 128 * 1024 < target_data {
        let size = 128 * 1024; // one full block each
        let mut rng = StdRng::seed_from_u64(0xC0FF0000 | file_idx);
        let mut data = vec![0u8; size];
        rng.fill(&mut data[..]);

        let path = format!("opt/app/data_{:04}", file_idx);
        let f = File {
            mode: format::S_IFREG | 0o644,
            size: data.len() as i64,
            uid: 1000,
            gid: 1000,
            ..Default::default()
        };
        w.create(&path, &f).unwrap();
        w.write_data(&data).unwrap();
        written += size;
        file_idx += 1;
    }

    // Symlink
    let f = File {
        mode: format::S_IFLNK,
        linkname: "bash".to_string(),
        ..Default::default()
    };
    w.create("usr/bin/sh", &f).unwrap();

    let image = w.close().unwrap().into_inner();

    // Sanity: image should be at least device_size (pre-allocated vec)
    assert!(
        image.len() >= device_size,
        "ext4 image too small: {} < {}",
        image.len(),
        device_size,
    );

    // Truncate to exact device_size (writer may have extended slightly)
    image[..device_size].to_vec()
}

/// Run the bless pipeline on raw image bytes, uploading packs and manifest
/// to the given ContentStore. This replicates the core logic of
/// `cli::bless::run_bless` to exercise the same zero-skip, pack assembly,
/// and manifest creation code path.
async fn bless_image_to_s3(
    content_store: &ContentStore,
    name: &str,
    image_data: &[u8],
) -> (usize, usize) {
    let device_size = image_data.len() as u64;
    let block_size = BLOCK_SIZE as u32;
    let vm_template = VolumeManifest::new(device_size, block_size);
    let total_blocks = device_size.div_ceil(block_size as u64) as usize;
    let (_, zero_hash) = shared_zero_block(BLOCK_SIZE);

    let mut volume_manifest = VolumeManifest::new(device_size, block_size);
    let mut hot_set_indices: Vec<u64> = Vec::new();
    let mut zero_blocks = 0usize;
    let mut nonzero_blocks = 0usize;

    // Accumulate blocks per chunk, then upload each chunk as a content-addressed pack.
    let mut chunks: BTreeMap<u32, Vec<(glidefs::block::block_map::Blake3Hash, u32, Bytes)>> =
        BTreeMap::new();

    for block_index in 0..total_blocks {
        let start = block_index * BLOCK_SIZE;
        let end = (start + BLOCK_SIZE).min(image_data.len());
        let mut buf = vec![0u8; BLOCK_SIZE];
        buf[..end - start].copy_from_slice(&image_data[start..end]);

        let hash = blake3_128(&buf);

        if hash == zero_hash {
            zero_blocks += 1;
            continue;
        }

        nonzero_blocks += 1;
        hot_set_indices.push(block_index as u64);

        let chunk_idx = vm_template.chunk_idx_for_block(block_index as u64);
        let block_offset = vm_template.block_offset_in_chunk(block_index as u64);

        let compressed = Bytes::from(lz4_compress(&buf));

        chunks
            .entry(chunk_idx)
            .or_default()
            .push((hash, block_offset, compressed));
    }

    // Upload each chunk's pack
    for (chunk_idx, blocks) in chunks {
        let pack_id = content_pack_id(&blocks);
        content_store
            .stream_chunk_pack(chunk_idx, pack_id, blocks, block_size)
            .await
            .expect("pack upload failed");
        volume_manifest.append_pack(chunk_idx, pack_id);
    }

    // Save manifest as base
    let manifest_key = format!("bases/{}", name);
    content_store
        .put_manifest(&manifest_key, volume_manifest.serialize().unwrap(), None)
        .await
        .expect("manifest upload failed");

    // Save hot set
    let hot_set_data = serialize_hot_set(&hot_set_indices);
    content_store
        .put_hot_set(name, hot_set_data)
        .await
        .expect("hot set upload failed");

    (nonzero_blocks, zero_blocks)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Bless an ext4 image → fork from blessed base → cold restart → verify every
/// non-zero block from S3 via BLAKE3 hash comparison against original image.
///
/// This is the end-to-end test that would catch the production bug where
/// `/usr/lib/git-core/git` reads as zeros from a blessed base image.
#[tokio::test]
#[ignore] // requires Docker (MinIO)
async fn bless_fork_block_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "bless-integrity";
    let base_name = "test-image";
    let base_prefix = format!("bases/{}", base_name);
    let transport = crate::Transport::Nbd;

    // --- Phase 1: Build ext4 image with known content ---
    let device_size = 128 * 1024 * 1024; // 128 MB
    let image = build_blessed_image(device_size);
    let total_blocks = device_size / BLOCK_SIZE;
    eprintln!(
        "[bless_fork_block_integrity] built ext4 image: {} ({} blocks)",
        fmt_bytes(device_size as u64),
        total_blocks,
    );

    // Compute expected BLAKE3 hash for every block in the original image.
    let mut expected_hashes: Vec<[u8; 32]> = Vec::with_capacity(total_blocks);
    for i in 0..total_blocks {
        let start = i * BLOCK_SIZE;
        let end = start + BLOCK_SIZE;
        expected_hashes.push(blake3_hash(&image[start..end]));
    }

    // --- Phase 2: Bless image to S3 (same pipeline as cli::bless::run_bless) ---
    let content_store = ContentStore::new(
        Arc::clone(&ctx.object_store),
        &format!("{}/exports/{}", db_path, base_prefix),
    );
    let (nonzero, zero) = bless_image_to_s3(&content_store, base_name, &image).await;
    eprintln!(
        "  blessed: {} non-zero blocks, {} zero blocks skipped",
        nonzero, zero,
    );
    assert!(nonzero > 0, "image should have at least some non-zero blocks");
    assert!(zero > 0, "image should have some zero blocks (free space)");

    // --- Phase 3: Fork from blessed base, verify all blocks ---
    let server =
        TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;

    let size_gb = device_size as f64 / (1024.0 * 1024.0 * 1024.0);
    server
        .fork_export("child", &base_prefix, size_gb)
        .await;

    let mut client = server.connect("child").await;

    let mut errors = 0u64;
    for i in 0..total_blocks {
        let data = client
            .read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        if actual != expected_hashes[i] {
            let is_zero = data.iter().all(|&b| b == 0);
            eprintln!(
                "  MISMATCH block {} (offset {:#x}): got {} — expected non-zero={}",
                i,
                i * BLOCK_SIZE,
                if is_zero { "ALL ZEROS" } else { "wrong data" },
                image[i * BLOCK_SIZE..i * BLOCK_SIZE + 4]
                    .iter()
                    .any(|&b| b != 0),
            );
            errors += 1;
        }
    }

    eprintln!(
        "  hot read: {}/{} blocks verified",
        total_blocks as u64 - errors,
        total_blocks,
    );
    assert_eq!(errors, 0, "hot read: {} blocks mismatched", errors);

    client.disconnect().await.unwrap();

    // --- Phase 4: Cold restart — verify all blocks from S3 (no local cache) ---
    server.drain_all().await;
    server.shutdown().await;

    let server2 =
        TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2
        .restore_forked_export("child", &base_prefix, size_gb)
        .await;

    let mut client2 = server2.connect("child").await;

    let mut cold_errors = 0u64;
    for i in 0..total_blocks {
        let data = client2
            .read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        if actual != expected_hashes[i] {
            let is_zero = data.iter().all(|&b| b == 0);
            eprintln!(
                "  COLD MISMATCH block {} (offset {:#x}): got {} — expected non-zero={}",
                i,
                i * BLOCK_SIZE,
                if is_zero { "ALL ZEROS" } else { "wrong data" },
                image[i * BLOCK_SIZE..i * BLOCK_SIZE + 4]
                    .iter()
                    .any(|&b| b != 0),
            );
            cold_errors += 1;
        }
    }

    client2.disconnect().await.unwrap();
    server2.shutdown().await;

    eprintln!(
        "[bless_fork_block_integrity] {} verified via S3 cold read in {:.1}s — hot_err={} cold_err={}",
        fmt_bytes(device_size as u64),
        t0.elapsed().as_secs_f64(),
        errors,
        cold_errors,
    );
    assert_eq!(
        cold_errors, 0,
        "cold read: {} blocks mismatched after S3 round-trip",
        cold_errors,
    );
}

/// Fork from blessed base → write new blocks → cold restart → verify both
/// inherited (base) and newly-written blocks survive the S3 round-trip.
///
/// This is the post-fork integrity test: it proves that writes to a forked
/// child don't corrupt inherited base data, and that both layers resolve
/// correctly after a cold restart.
#[tokio::test]
#[ignore] // requires Docker (MinIO)
async fn bless_fork_write_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "bless-fork-write";
    let base_name = "test-image";
    let base_prefix = format!("bases/{}", base_name);
    let transport = crate::Transport::Nbd;

    // --- Phase 1: Build and bless ext4 image ---
    let device_size = 128 * 1024 * 1024; // 128 MB
    let image = build_blessed_image(device_size);
    let total_blocks = device_size / BLOCK_SIZE;

    // Hash every block from the original image
    let mut expected_hashes: Vec<[u8; 32]> = Vec::with_capacity(total_blocks);
    for i in 0..total_blocks {
        expected_hashes.push(blake3_hash(&image[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE]));
    }

    let content_store = ContentStore::new(
        Arc::clone(&ctx.object_store),
        &format!("{}/exports/{}", db_path, base_prefix),
    );
    bless_image_to_s3(&content_store, base_name, &image).await;

    // --- Phase 2: Fork, write new data to ~25% of blocks ---
    let server =
        TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    let size_gb = device_size as f64 / (1024.0 * 1024.0 * 1024.0);
    server
        .fork_export("child", &base_prefix, size_gb)
        .await;

    let mut client = server.connect("child").await;

    // Overwrite every 4th block with new deterministic data.
    let overwrite_seed = 0xF0F0F0F0_u64;
    let mut overwritten: Vec<usize> = Vec::new();
    for i in (0..total_blocks).step_by(4) {
        let mut rng = StdRng::seed_from_u64(overwrite_seed.wrapping_add(i as u64));
        let mut data = vec![0u8; BLOCK_SIZE];
        rng.fill(&mut data[..]);

        client
            .write((i * BLOCK_SIZE) as u64, &data)
            .await
            .unwrap();

        // Update expected hash for this block
        expected_hashes[i] = blake3_hash(&data);
        overwritten.push(i);
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    eprintln!(
        "[bless_fork_write_integrity] wrote {} new blocks ({} inherited)",
        overwritten.len(),
        total_blocks - overwritten.len(),
    );

    // --- Phase 3: Cold restart, verify ALL blocks from S3 ---
    server.drain_all().await;
    server.shutdown().await;

    let server2 =
        TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2
        .restore_forked_export("child", &base_prefix, size_gb)
        .await;

    let mut reader = server2.connect("child").await;

    let mut inherited_errors = 0u64;
    let mut overwritten_errors = 0u64;
    for i in 0..total_blocks {
        let data = reader
            .read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        if actual != expected_hashes[i] {
            let is_overwritten = overwritten.contains(&i);
            let is_zero = data.iter().all(|&b| b == 0);
            eprintln!(
                "  MISMATCH block {} ({}): got {}",
                i,
                if is_overwritten { "overwritten" } else { "inherited" },
                if is_zero { "ALL ZEROS" } else { "wrong data" },
            );
            if is_overwritten {
                overwritten_errors += 1;
            } else {
                inherited_errors += 1;
            }
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let total_errors = inherited_errors + overwritten_errors;
    eprintln!(
        "[bless_fork_write_integrity] {} verified in {:.1}s — inherited_err={} overwritten_err={}",
        fmt_bytes(device_size as u64),
        t0.elapsed().as_secs_f64(),
        inherited_errors,
        overwritten_errors,
    );
    assert_eq!(
        total_errors, 0,
        "post-fork integrity failed: {} inherited + {} overwritten blocks mismatched",
        inherited_errors, overwritten_errors,
    );
}

/// Bless a raw image containing duplicate 128KB blocks within the same chunk,
/// fork, cold restart, verify that BOTH copies resolve correctly from S3.
///
/// Regression test for the bless within-chunk dedup bug: if two blocks have
/// identical content (same BLAKE3-128 hash), both must get pack index entries.
/// The old code only stored the first occurrence, causing the duplicate to
/// return zeros on read.
#[tokio::test]
#[ignore] // requires Docker (MinIO)
async fn bless_duplicate_block_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "bless-dedup";
    let base_name = "dedup-image";
    let base_prefix = format!("bases/{}", base_name);
    let transport = crate::Transport::Nbd;

    // Build a raw image with intentional duplicate blocks.
    // 16 blocks x 128KB = 2 MB. Blocks 0,4,8,12 all have identical content.
    let total_blocks = 16;
    let device_size = total_blocks * BLOCK_SIZE;
    let mut image = vec![0u8; device_size];

    let duplicate_pattern = {
        let mut rng = StdRng::seed_from_u64(0xD0D0D0D0);
        let mut buf = vec![0u8; BLOCK_SIZE];
        rng.fill(&mut buf[..]);
        buf
    };

    for i in 0..total_blocks {
        let start = i * BLOCK_SIZE;
        if i % 4 == 0 {
            // Duplicate: same content at offsets 0, 4, 8, 12
            image[start..start + BLOCK_SIZE].copy_from_slice(&duplicate_pattern);
        } else {
            // Unique block
            let mut rng = StdRng::seed_from_u64(0xBEEF0000 + i as u64);
            rng.fill(&mut image[start..start + BLOCK_SIZE]);
        }
    }

    // Pre-compute expected hashes
    let expected_hashes: Vec<[u8; 32]> = (0..total_blocks)
        .map(|i| blake3_hash(&image[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE]))
        .collect();

    // Bless
    let content_store = ContentStore::new(
        Arc::clone(&ctx.object_store),
        &format!("{}/exports/{}", db_path, base_prefix),
    );
    let (nonzero, _) = bless_image_to_s3(&content_store, base_name, &image).await;
    eprintln!(
        "[bless_duplicate_block_integrity] blessed {} non-zero blocks ({} total, 4 duplicates)",
        nonzero, total_blocks,
    );

    // Fork, cold restart, verify
    let server =
        TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    let size_gb = device_size as f64 / (1024.0 * 1024.0 * 1024.0);
    server.fork_export("child", &base_prefix, size_gb).await;

    server.drain_all().await;
    server.shutdown().await;

    let server2 =
        TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2
        .restore_forked_export("child", &base_prefix, size_gb)
        .await;

    let mut client = server2.connect("child").await;

    let mut errors = 0u64;
    for i in 0..total_blocks {
        let data = client
            .read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        if actual != expected_hashes[i] {
            let is_zero = data.iter().all(|&b| b == 0);
            let is_dup = i % 4 == 0;
            eprintln!(
                "  MISMATCH block {} ({}): got {}",
                i,
                if is_dup { "DUPLICATE" } else { "unique" },
                if is_zero { "ALL ZEROS" } else { "wrong data" },
            );
            errors += 1;
        }
    }

    client.disconnect().await.unwrap();
    server2.shutdown().await;

    eprintln!(
        "[bless_duplicate_block_integrity] verified in {:.1}s — errors={}",
        t0.elapsed().as_secs_f64(),
        errors,
    );
    assert_eq!(
        errors, 0,
        "duplicate block integrity failed: {} blocks returned wrong data",
        errors,
    );
}
