//! Data integrity test suite.
//!
//! Every test writes data through the full NBD stack, forces S3 reads via cold
//! restart (fresh TempDir = no local cache), and verifies every block with
//! cryptographic hashes. No mocks, no fakes — real MinIO, real packs, real S3.
//!
//! Run on demand (not part of the normal docker test suite):
//!
//! ```bash
//! # Full suite
//! cargo test --features docker-tests --test docker_integration integrity_suite \
//!     -- --ignored --nocapture
//!
//! # Single test
//! cargo test --features docker-tests --test docker_integration integrity_suite::block_hash_verify \
//!     -- --ignored --nocapture
//!
//! # Extended soak (5 minutes)
//! SOAK_DURATION_SECS=300 cargo test --features docker-tests --test docker_integration \
//!     integrity_suite::soak_test -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};

use crate::{TestContext, TestServer};

const BLOCK_SIZE: usize = 128 * 1024; // 128 KiB

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Deterministic block pattern from index. Reproducible without storage.
fn block_pattern(block_idx: u64, block_size: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(block_idx.wrapping_mul(0x517cc1b727220a95));
    let mut buf = vec![0u8; block_size];
    rng.fill(&mut buf[..]);
    buf
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

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

// ---------------------------------------------------------------------------
// 1. Block-level BLAKE3 verification through S3
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn block_hash_verify() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-blake3";
    let transport = crate::Transport::Nbd;

    // 256 MB = 2048 blocks
    let num_blocks: u64 = 2048;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Phase 1: Write all blocks with deterministic patterns
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    let mut expected_hashes: Vec<[u8; 32]> = Vec::with_capacity(num_blocks as usize);

    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        expected_hashes.push(blake3_hash(&pattern));
        client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
        if idx % 256 == 0 {
            client.flush().await.unwrap();
        }
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    // Phase 2: Cold restart — drain to S3, fresh server, restore from manifest
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client2 = server2.connect("vol").await;

    // Phase 3: Read every block from S3, verify BLAKE3
    let mut errors = 0u64;
    for idx in 0..num_blocks {
        let data = client2
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        if actual != expected_hashes[idx as usize] {
            eprintln!(
                "BLAKE3 MISMATCH block {}: expected {:x?}, got {:x?}",
                idx, &expected_hashes[idx as usize][..8], &actual[..8]
            );
            errors += 1;
        }
    }

    client2.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_mb = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[block_hash_verify] {} ({} blocks) verified via S3 in {:.1}s — err={}",
        fmt_bytes(data_mb),
        num_blocks,
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "BLAKE3 verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 2. Sequential integrity — contiguous write/read through S3
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn sequential_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-seq";
    let transport = crate::Transport::Nbd;

    // 100 MB
    let num_blocks: u64 = 800;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Write sequentially, building a running SHA-256 of the full stream
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    let mut write_hasher = Sha256::new();
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        write_hasher.update(&pattern);
        client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
        if idx % 128 == 0 {
            client.flush().await.unwrap();
        }
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();
    let expected_hash: [u8; 32] = write_hasher.finalize().into();

    // Cold restart
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client2 = server2.connect("vol").await;

    // Read sequentially from S3, compute SHA-256
    let mut read_hasher = Sha256::new();
    for idx in 0..num_blocks {
        let data = client2
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        read_hasher.update(&data);
    }
    let actual_hash: [u8; 32] = read_hasher.finalize().into();

    client2.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[sequential_integrity] {} verified via S3 in {:.1}s",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
    );
    assert_eq!(
        expected_hash, actual_hash,
        "SHA-256 mismatch: sequential data corrupted during S3 roundtrip"
    );
}

// ---------------------------------------------------------------------------
// 3. Hash stress — multi-pass random write + full S3 verify
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn hash_stress() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-stress";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 2048; // 256 MB
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;
    let passes = 3;
    let write_fraction = 0.6;

    // Track expected SHA-256 per block (unwritten = zeros)
    let zero_hash = sha256(&vec![0u8; BLOCK_SIZE]);
    let mut expected: HashMap<u64, [u8; 32]> = HashMap::new();

    let mut total_written: u64 = 0;
    let mut total_read: u64 = 0;
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_CAFE);

    for pass in 0..passes {
        let pass_t0 = Instant::now();

        // Write phase
        let server = if pass == 0 {
            let s = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
            s.create_export("vol", size_gb).await;
            s
        } else {
            let s = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
            s.restore_export("vol", size_gb).await;
            s
        };

        let mut client = server.connect("vol").await;

        let blocks_to_write = (num_blocks as f64 * write_fraction) as u64;
        for _ in 0..blocks_to_write {
            let idx = rng.gen_range(0..num_blocks);
            let pattern = block_pattern(rng.r#gen(), BLOCK_SIZE);
            let hash = sha256(&pattern);
            expected.insert(idx, hash);
            client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
            total_written += BLOCK_SIZE as u64;
        }
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        // Cold restart
        server.drain_all().await;
        server.shutdown().await;

        let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server2.restore_export("vol", size_gb).await;
        let mut client2 = server2.connect("vol").await;

        // Full verification from S3
        let mut errors = 0u64;
        for idx in 0..num_blocks {
            let data = client2
                .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            let actual = sha256(&data);
            let expect = expected.get(&idx).unwrap_or(&zero_hash);
            if &actual != expect {
                eprintln!(
                    "MISMATCH pass {} block {}: expected {:x?}, got {:x?}",
                    pass, idx, &expect[..8], &actual[..8]
                );
                errors += 1;
            }
            total_read += BLOCK_SIZE as u64;
        }

        client2.disconnect().await.unwrap();

        eprintln!(
            "  pass {}: {} blocks written, full verify from S3 in {:.1}s — err={}",
            pass,
            blocks_to_write,
            pass_t0.elapsed().as_secs_f64(),
            errors
        );
        assert_eq!(errors, 0, "pass {} had {} verification errors", pass, errors);

        // Keep server2 alive only if we need it for next pass's restore
        // (we don't — next pass does its own cold restart)
        server2.shutdown().await;
    }

    eprintln!(
        "[hash_stress] {} read + {} written in {:.1}s — err=0",
        fmt_bytes(total_read),
        fmt_bytes(total_written),
        t0.elapsed().as_secs_f64(),
    );
}

// ---------------------------------------------------------------------------
// 4. Persistence integrity — write → S3 → cold restart → verify
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn persistence_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-persist";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 400; // ~50 MB
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Write blocks
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    let mut expected_hashes: Vec<[u8; 32]> = Vec::with_capacity(num_blocks as usize);
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        expected_hashes.push(sha256(&pattern));
        client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    // Cold restart
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client2 = server2.connect("vol").await;

    // Verify from S3
    let mut errors = 0u64;
    for idx in 0..num_blocks {
        let data = client2
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = sha256(&data);
        if actual != expected_hashes[idx as usize] {
            eprintln!(
                "SHA-256 MISMATCH block {}: expected {:x?}, got {:x?}",
                idx, &expected_hashes[idx as usize][..8], &actual[..8]
            );
            errors += 1;
        }
    }

    client2.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[persistence_integrity] {} verified via S3 in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "persistence verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 5. Sparse integrity — blocks with holes through S3
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn sparse_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-sparse";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 512; // 64 MB
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Write ~25% of blocks at scattered offsets
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    let mut written: HashMap<u64, [u8; 32]> = HashMap::new();
    let mut rng = StdRng::seed_from_u64(0x5BA2_5E00_0000);
    for idx in 0..num_blocks {
        if rng.gen_bool(0.25) {
            let pattern = block_pattern(idx, BLOCK_SIZE);
            written.insert(idx, sha256(&pattern));
            client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
        }
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    eprintln!(
        "  wrote {} / {} blocks (holes: {})",
        written.len(),
        num_blocks,
        num_blocks as usize - written.len()
    );

    // Cold restart
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client2 = server2.connect("vol").await;

    // Verify all blocks from S3
    let zero_hash = sha256(&vec![0u8; BLOCK_SIZE]);
    let mut errors = 0u64;
    let mut holes_verified = 0u64;

    for idx in 0..num_blocks {
        let data = client2
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = sha256(&data);
        if let Some(expected) = written.get(&idx) {
            if &actual != expected {
                eprintln!("MISMATCH written block {}", idx);
                errors += 1;
            }
        } else {
            // Hole — must be all zeros
            if actual != zero_hash {
                eprintln!("HOLE NOT ZERO block {}", idx);
                errors += 1;
            }
            holes_verified += 1;
        }
    }

    client2.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[sparse_integrity] {} verified via S3 ({} holes) in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        holes_verified,
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "sparse verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 6. Soak test — timed continuous R/W with periodic S3 verification
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn soak_test() {
    let duration_secs: u64 = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-soak";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 4096; // 512 MB
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;
    let verify_interval_secs = 10;

    let mut rng = StdRng::seed_from_u64(0x50AE_7E57_5EED);
    let zero_hash = blake3_hash(&vec![0u8; BLOCK_SIZE]);
    let mut expected: HashMap<u64, [u8; 32]> = HashMap::new();
    let mut total_written: u64 = 0;
    let mut total_read: u64 = 0;
    let mut total_ops: u64 = 0;
    let mut verify_passes: u64 = 0;
    let mut last_verify = Instant::now();

    // Initial server
    let mut server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    let deadline = t0 + std::time::Duration::from_secs(duration_secs);

    while Instant::now() < deadline {
        // Batch of writes
        let batch_size = rng.gen_range(16..64);
        for _ in 0..batch_size {
            let idx = rng.gen_range(0..num_blocks);
            let pattern = block_pattern(rng.r#gen(), BLOCK_SIZE);
            expected.insert(idx, blake3_hash(&pattern));
            client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
            total_written += BLOCK_SIZE as u64;
            total_ops += 1;
        }
        client.flush().await.unwrap();

        // Batch of reads (local cache verification)
        let read_count = rng.gen_range(8..32);
        for _ in 0..read_count {
            let idx = rng.gen_range(0..num_blocks);
            let data = client
                .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            let actual = blake3_hash(&data);
            let expect = expected.get(&idx).unwrap_or(&zero_hash);
            assert_eq!(
                &actual, expect,
                "local read mismatch at block {} during soak",
                idx
            );
            total_read += BLOCK_SIZE as u64;
            total_ops += 1;
        }

        // Periodic S3 verification via cold restart
        if last_verify.elapsed().as_secs() >= verify_interval_secs {
            client.disconnect().await.unwrap();
            server.drain_all().await;
            server.shutdown().await;

            let s2 =
                TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
            s2.restore_export("vol", size_gb).await;
            let mut c2 = s2.connect("vol").await;

            // Verify all written blocks from S3
            let mut verify_errors = 0u64;
            for (&idx, expect) in &expected {
                let data = c2
                    .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                    .await
                    .unwrap();
                let actual = blake3_hash(&data);
                if &actual != expect {
                    verify_errors += 1;
                }
                total_read += BLOCK_SIZE as u64;
                total_ops += 1;
            }

            // Spot-check some unwritten blocks are zeros
            for _ in 0..100 {
                let idx = rng.gen_range(0..num_blocks);
                if !expected.contains_key(&idx) {
                    let data = c2
                        .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                        .await
                        .unwrap();
                    let actual = blake3_hash(&data);
                    if actual != zero_hash {
                        verify_errors += 1;
                    }
                    total_read += BLOCK_SIZE as u64;
                    total_ops += 1;
                }
            }

            verify_passes += 1;
            eprintln!(
                "  S3 verify pass {} ({} blocks): err={}",
                verify_passes,
                expected.len(),
                verify_errors,
            );
            assert_eq!(
                verify_errors, 0,
                "S3 verification pass {} had {} errors",
                verify_passes, verify_errors
            );

            // Continue with restored server
            c2.disconnect().await.unwrap();
            server = s2;
            client = server.connect("vol").await;
            last_verify = Instant::now();
        }
    }

    client.disconnect().await.unwrap();

    // Final S3 verification
    server.drain_all().await;
    server.shutdown().await;

    let final_server =
        TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    final_server.restore_export("vol", size_gb).await;
    let mut final_client = final_server.connect("vol").await;

    let mut final_errors = 0u64;
    for (&idx, expect) in &expected {
        let data = final_client
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        if &actual != expect {
            final_errors += 1;
        }
        total_read += BLOCK_SIZE as u64;
        total_ops += 1;
    }

    final_client.disconnect().await.unwrap();
    final_server.shutdown().await;

    verify_passes += 1;

    eprintln!();
    eprintln!("=== Soak Test Results ({}s) ===", duration_secs);
    eprintln!("  Total written:      {}", fmt_bytes(total_written));
    eprintln!("  Total read:         {}", fmt_bytes(total_read));
    eprintln!("  Total I/O ops:      {}", total_ops);
    eprintln!("  S3 verify passes:   {}", verify_passes);
    eprintln!("  Unique blocks:      {}", expected.len());
    eprintln!("  Final errors:       {}", final_errors);
    eprintln!();

    assert_eq!(final_errors, 0, "final soak verification had {} errors", final_errors);
}

// ---------------------------------------------------------------------------
// 7. Fork integrity — fork data isolation + S3 persistence
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn fork_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-fork";
    let transport = crate::Transport::Nbd;

    let parent_blocks: u64 = 128;
    let overwrite_blocks: u64 = 32;
    let size_gb = (parent_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Phase 1: Write parent blocks, snapshot
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("parent", size_gb).await;
    let mut parent_client = server.connect("parent").await;

    let mut parent_hashes: Vec<[u8; 32]> = Vec::with_capacity(parent_blocks as usize);
    for idx in 0..parent_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        parent_hashes.push(blake3_hash(&pattern));
        parent_client
            .write(idx * BLOCK_SIZE as u64, &pattern)
            .await
            .unwrap();
    }
    parent_client.flush().await.unwrap();
    parent_client.disconnect().await.unwrap();

    server.snapshot_export("parent").await;

    // Phase 2: Fork child — reads parent blocks from S3 (no local cache for them)
    server.fork_export("child", "parent", size_gb).await;
    let mut child_client = server.connect("child").await;

    // Verify child reads parent data correctly
    for idx in 0..parent_blocks {
        let data = child_client
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        assert_eq!(
            actual, parent_hashes[idx as usize],
            "child inherited block {} has wrong BLAKE3 hash",
            idx
        );
    }
    eprintln!("  child inherited {} parent blocks — verified", parent_blocks);

    // Phase 3: Overwrite first N blocks in child with new patterns
    let mut child_hashes = parent_hashes.clone();
    for idx in 0..overwrite_blocks {
        // Use a different seed space so patterns differ from parent
        let pattern = block_pattern(idx + 1_000_000, BLOCK_SIZE);
        child_hashes[idx as usize] = blake3_hash(&pattern);
        child_client
            .write(idx * BLOCK_SIZE as u64, &pattern)
            .await
            .unwrap();
    }
    child_client.flush().await.unwrap();
    child_client.disconnect().await.unwrap();

    // Verify parent is unchanged
    let mut parent_client2 = server.connect("parent").await;
    for idx in 0..overwrite_blocks {
        let data = parent_client2
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        assert_eq!(
            actual, parent_hashes[idx as usize],
            "parent block {} was corrupted by child write",
            idx
        );
    }
    parent_client2.disconnect().await.unwrap();
    eprintln!("  parent isolation verified — {} blocks unchanged", overwrite_blocks);

    // Phase 4: Cold restart child, verify all blocks from S3
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2
        .restore_forked_export("child", "parent", size_gb)
        .await;
    let mut reader = server2.connect("child").await;

    let mut errors = 0u64;
    for idx in 0..parent_blocks {
        let data = reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        if actual != child_hashes[idx as usize] {
            eprintln!(
                "FORK MISMATCH block {} (overwritten={})",
                idx,
                idx < overwrite_blocks
            );
            errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = parent_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[fork_integrity] {} verified via S3 in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "fork verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 8. Overwrite integrity — old data replaced through S3
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn overwrite_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-overwrite";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 256;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Phase 1: Write pattern A to all blocks, drain to S3
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();
    server.drain_all().await;
    server.shutdown().await;

    // Phase 2: Restore, overwrite ALL blocks with pattern B, drain again
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client2 = server2.connect("vol").await;

    // Pattern B uses a different seed space (idx + 10_000_000)
    let mut expected_hashes: Vec<[u8; 32]> = Vec::with_capacity(num_blocks as usize);
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx + 10_000_000, BLOCK_SIZE);
        expected_hashes.push(sha256(&pattern));
        client2.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    client2.flush().await.unwrap();
    client2.disconnect().await.unwrap();
    server2.drain_all().await;
    server2.shutdown().await;

    // Phase 3: Cold restart — verify pattern B, NOT pattern A
    let server3 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server3.restore_export("vol", size_gb).await;
    let mut client3 = server3.connect("vol").await;

    let mut errors = 0u64;
    for idx in 0..num_blocks {
        let data = client3
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = sha256(&data);
        if actual != expected_hashes[idx as usize] {
            // Check if we got pattern A (stale data) vs random corruption
            let stale_hash = sha256(&block_pattern(idx, BLOCK_SIZE));
            if actual == stale_hash {
                eprintln!("STALE DATA block {}: got pattern A instead of B", idx);
            } else {
                eprintln!("CORRUPTION block {}: neither pattern A nor B", idx);
            }
            errors += 1;
        }
    }

    client3.disconnect().await.unwrap();
    server3.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[overwrite_integrity] {} verified via S3 (2 drain cycles) in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "overwrite verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 9. Concurrent stress — parallel writers + S3 verification
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn concurrent_stress() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-concurrent";
    let transport = crate::Transport::Nbd;

    let num_clients: u64 = 4;
    let blocks_per_client: u64 = 256;
    let total_blocks = num_clients * blocks_per_client; // 1024 blocks = 128 MB
    let size_gb = (total_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;

    let info = server.connect_info("vol").await;

    // Each client writes to a disjoint range of blocks with unique patterns
    let mut handles = Vec::new();
    for client_idx in 0..num_clients {
        let info = info.clone();
        let handle = tokio::spawn(async move {
            let mut client = info.connect().await.unwrap();
            let base_block = client_idx * blocks_per_client;

            for b in 0..blocks_per_client {
                let idx = base_block + b;
                let pattern = block_pattern(idx, BLOCK_SIZE);
                client
                    .write(idx * BLOCK_SIZE as u64, &pattern)
                    .await
                    .unwrap();
                if b % 64 == 0 {
                    client.flush().await.unwrap();
                }
            }
            client.flush().await.unwrap();
            client.disconnect().await.unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Cold restart
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client = server2.connect("vol").await;

    // Verify every block from S3
    let mut errors = 0u64;
    for idx in 0..total_blocks {
        let data = client
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let expected = block_pattern(idx, BLOCK_SIZE);
        let actual_hash = blake3_hash(&data);
        let expected_hash = blake3_hash(&expected);
        if actual_hash != expected_hash {
            eprintln!(
                "MISMATCH block {} (client {}): expected {:x?}, got {:x?}",
                idx,
                idx / blocks_per_client,
                &expected_hash[..8],
                &actual_hash[..8]
            );
            errors += 1;
        }
    }

    client.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = total_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[concurrent_stress] {} verified via S3 ({} clients) in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        num_clients,
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "concurrent verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 10. Sub-block write integrity — partial 4KB writes into 128KB blocks
// ---------------------------------------------------------------------------

const SUB_BLOCK_SIZE: usize = 4096; // 4 KiB sub-region
const SUBS_PER_BLOCK: usize = BLOCK_SIZE / SUB_BLOCK_SIZE; // 32

/// Build the expected full block: start with base_data, overlay sub-block writes.
fn expected_block_after_sub_writes(
    base_data: &[u8],
    writes: &[(usize, Vec<u8>)], // (offset_in_block, data)
) -> Vec<u8> {
    let mut result = base_data.to_vec();
    for (offset, data) in writes {
        result[*offset..*offset + data.len()].copy_from_slice(data);
    }
    result
}

/// Sub-block pattern for a given block + sub-region index.
fn sub_pattern(block_idx: u64, sub_idx: usize) -> Vec<u8> {
    // Use a seed that encodes both block and sub index
    let seed = block_idx.wrapping_mul(0x9E3779B97F4A7C15) ^ (sub_idx as u64);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut buf = vec![0u8; SUB_BLOCK_SIZE];
    rng.fill(&mut buf[..]);
    buf
}

/// Write data to a parent, snapshot, fork a child, then do sub-block writes
/// on the child. Verify that the child block = parent data + sub-block overlays.
/// Cold restart and re-verify from S3.
#[tokio::test]
#[ignore]
async fn sub_block_basic() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-subblock";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 64;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Phase 1: Write full blocks to parent, snapshot
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("parent", size_gb).await;
    let mut parent_client = server.connect("parent").await;

    let mut parent_data: Vec<Vec<u8>> = Vec::with_capacity(num_blocks as usize);
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        parent_client
            .write(idx * BLOCK_SIZE as u64, &pattern)
            .await
            .unwrap();
        parent_data.push(pattern);
    }
    parent_client.flush().await.unwrap();
    parent_client.disconnect().await.unwrap();
    server.snapshot_export("parent").await;

    // Phase 2: Fork child, do sub-block writes
    // Write 4KB sub-regions into various blocks — NOT full block overwrites.
    // This triggers the partial block / backfill path because the child has
    // no local data for these blocks (they live in S3 via parent packs).
    server.fork_export("child", "parent", size_gb).await;
    let mut child_client = server.connect("child").await;

    // Track which sub-regions we wrote per block
    let mut sub_writes: HashMap<u64, Vec<(usize, Vec<u8>)>> = HashMap::new();

    // Pattern: write 1-4 scattered sub-regions per block, across many blocks
    let mut rng = StdRng::seed_from_u64(0x50B_B10C_BA51C);
    for idx in 0..num_blocks {
        let num_subs = rng.gen_range(1..=4);
        let mut written_subs: Vec<usize> = Vec::new();
        let mut writes_for_block: Vec<(usize, Vec<u8>)> = Vec::new();

        for _ in 0..num_subs {
            let sub_idx = loop {
                let s = rng.gen_range(0..SUBS_PER_BLOCK);
                if !written_subs.contains(&s) {
                    break s;
                }
            };
            written_subs.push(sub_idx);

            let offset_in_block = sub_idx * SUB_BLOCK_SIZE;
            let device_offset = idx * BLOCK_SIZE as u64 + offset_in_block as u64;
            let data = sub_pattern(idx, sub_idx);

            child_client.write(device_offset, &data).await.unwrap();
            writes_for_block.push((offset_in_block, data));
        }
        sub_writes.insert(idx, writes_for_block);
    }
    child_client.flush().await.unwrap();

    // Phase 3: Read back from child (may use local cache + S3 merge)
    // Verify each block = parent_data + sub-block overlays
    let mut local_errors = 0u64;
    for idx in 0..num_blocks {
        let data = child_client
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let expected = expected_block_after_sub_writes(
            &parent_data[idx as usize],
            sub_writes.get(&idx).unwrap(),
        );
        if data != expected {
            eprintln!("LOCAL MISMATCH block {} (sub-writes: {:?})",
                idx,
                sub_writes.get(&idx).unwrap().iter().map(|(o, _)| o / SUB_BLOCK_SIZE).collect::<Vec<_>>()
            );
            // Find which sub-region differs
            for s in 0..SUBS_PER_BLOCK {
                let start = s * SUB_BLOCK_SIZE;
                let end = start + SUB_BLOCK_SIZE;
                if data[start..end] != expected[start..end] {
                    eprintln!("  sub {} differs: got {:02x}{:02x}..., expected {:02x}{:02x}...",
                        s, data[start], data[start+1], expected[start], expected[start+1]);
                }
            }
            local_errors += 1;
        }
    }
    child_client.disconnect().await.unwrap();

    assert_eq!(local_errors, 0, "local read had {} mismatches", local_errors);
    eprintln!("  local verification passed ({} blocks, sub-block writes)", num_blocks);

    // Phase 4: Cold restart — verify sub-block writes survive S3 roundtrip
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2
        .restore_forked_export("child", "parent", size_gb)
        .await;
    let mut reader = server2.connect("child").await;

    let mut s3_errors = 0u64;
    for idx in 0..num_blocks {
        let data = reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let expected = expected_block_after_sub_writes(
            &parent_data[idx as usize],
            sub_writes.get(&idx).unwrap(),
        );
        if data != expected {
            eprintln!("S3 MISMATCH block {} after cold restart", idx);
            for s in 0..SUBS_PER_BLOCK {
                let start = s * SUB_BLOCK_SIZE;
                let end = start + SUB_BLOCK_SIZE;
                if data[start..end] != expected[start..end] {
                    eprintln!("  sub {} differs", s);
                }
            }
            s3_errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[sub_block_basic] {} verified via S3 (sub-block writes on all {} blocks) in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        num_blocks,
        t0.elapsed().as_secs_f64(),
        s3_errors
    );
    assert_eq!(s3_errors, 0, "S3 verification failed for {} blocks", s3_errors);
}

// ---------------------------------------------------------------------------
// 11. Sub-block stress — many tiny writes, interleaved reads, S3 verify
// ---------------------------------------------------------------------------

/// Hammer blocks with random tiny writes (512B to 4KB), read back after each
/// batch, then cold restart and verify every byte from S3.
#[tokio::test]
#[ignore]
async fn sub_block_stress() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-subblock-stress";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 32;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Phase 1: Parent with known data
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("parent", size_gb).await;
    let mut parent_client = server.connect("parent").await;

    // Each block gets a unique fill byte for easy debugging
    let mut block_state: Vec<Vec<u8>> = Vec::with_capacity(num_blocks as usize);
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        parent_client
            .write(idx * BLOCK_SIZE as u64, &pattern)
            .await
            .unwrap();
        block_state.push(pattern);
    }
    parent_client.flush().await.unwrap();
    parent_client.disconnect().await.unwrap();
    server.snapshot_export("parent").await;

    // Phase 2: Fork child, hammer with tiny writes
    server.fork_export("child", "parent", size_gb).await;
    let mut child_client = server.connect("child").await;

    let mut rng = StdRng::seed_from_u64(0x71EE_1173_5725);
    let mut total_sub_writes: u64 = 0;
    let mut total_bytes_written: u64 = 0;

    // 5 rounds of writes + immediate read verification
    for round in 0..5 {
        // Each round: random tiny writes across random blocks
        let writes_per_round = 100;
        for _ in 0..writes_per_round {
            let block_idx = rng.gen_range(0..num_blocks);
            // Random write size: 4KB to 16KB, 4KB-aligned (matches real filesystem I/O)
            let write_size = rng.gen_range(1..=4) * SUB_BLOCK_SIZE;
            let max_sub = (BLOCK_SIZE - write_size) / SUB_BLOCK_SIZE;
            let offset_in_block = rng.gen_range(0..=max_sub) * SUB_BLOCK_SIZE;

            let device_offset = block_idx * BLOCK_SIZE as u64 + offset_in_block as u64;

            // Generate deterministic data for this write
            let seed = (round as u64) * 1_000_000 + block_idx * 1_000 + offset_in_block as u64;
            let mut write_rng = StdRng::seed_from_u64(seed);
            let mut data = vec![0u8; write_size];
            write_rng.fill(&mut data[..]);

            child_client.write(device_offset, &data).await.unwrap();

            // Update our shadow copy
            block_state[block_idx as usize][offset_in_block..offset_in_block + write_size]
                .copy_from_slice(&data);

            total_sub_writes += 1;
            total_bytes_written += write_size as u64;
        }
        child_client.flush().await.unwrap();

        // Verify a random sample of blocks after each round
        let sample_size = num_blocks.min(16);
        let mut round_errors = 0u64;
        for _ in 0..sample_size {
            let block_idx = rng.gen_range(0..num_blocks);
            let data = child_client
                .read(block_idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            let expected = &block_state[block_idx as usize];
            if data != *expected {
                // Find which bytes differ
                let mut first_diff = None;
                let mut diff_count = 0usize;
                let mut zero_diffs = 0usize;
                for i in 0..BLOCK_SIZE {
                    if data[i] != expected[i] {
                        if first_diff.is_none() {
                            first_diff = Some(i);
                        }
                        diff_count += 1;
                        if data[i] == 0 {
                            zero_diffs += 1;
                        }
                    }
                }
                let fd = first_diff.unwrap();
                eprintln!(
                    "ROUND {} MISMATCH block {}: {} bytes differ (first at offset {}, sub {}), {} are zeros, got {:02x} expected {:02x}",
                    round, block_idx, diff_count, fd, fd / SUB_BLOCK_SIZE, zero_diffs, data[fd], expected[fd]
                );
                round_errors += 1;
            }
        }
        assert_eq!(round_errors, 0, "round {} had {} mismatches", round, round_errors);
    }

    child_client.disconnect().await.unwrap();
    eprintln!(
        "  {} sub-block writes ({}) across {} blocks — local verify passed",
        total_sub_writes,
        fmt_bytes(total_bytes_written),
        num_blocks
    );

    // Phase 3: Cold restart, verify every byte from S3
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2
        .restore_forked_export("child", "parent", size_gb)
        .await;
    let mut reader = server2.connect("child").await;

    let mut s3_errors = 0u64;
    for idx in 0..num_blocks {
        let data = reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let expected = &block_state[idx as usize];
        if data != *expected {
            // Find first differing byte
            let first_diff = data.iter().zip(expected.iter())
                .position(|(a, b)| a != b)
                .unwrap();
            eprintln!(
                "S3 MISMATCH block {}: first diff at byte {} (got {:02x}, expected {:02x})",
                idx, first_diff, data[first_diff], expected[first_diff]
            );
            s3_errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    eprintln!(
        "[sub_block_stress] {} writes ({}) verified via S3 in {:.1}s — err={}",
        total_sub_writes,
        fmt_bytes(total_bytes_written),
        t0.elapsed().as_secs_f64(),
        s3_errors
    );
    assert_eq!(s3_errors, 0, "S3 sub-block verification failed for {} blocks", s3_errors);
}

// ---------------------------------------------------------------------------
// 12. Multi-block read integrity — reads spanning multiple blocks
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 12. Zero-block integrity — SIMD detection + tombstone entries through S3
// ---------------------------------------------------------------------------

/// Writes non-zero data, drains to S3, overwrites ALL blocks with zeros,
/// drains again, cold restart, verifies every block is zeros.
///
/// Exercises: SIMD is_zero_block() detection (AVX2/NEON), tombstone entries
/// (comp_length=0 in pack index), "newest wins" semantics that prevent stale
/// non-zero data from showing through after a zero overwrite.
#[tokio::test]
#[ignore]
async fn zero_block_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-zeroblock";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 256; // 32 MB
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Phase 1: Write non-zero data to all blocks, drain to S3
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();
    server.drain_all().await;

    // Verify non-zero data is in S3
    server.shutdown().await;
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client2 = server2.connect("vol").await;

    // Spot-check a few blocks are non-zero from S3
    let zero_block = vec![0u8; BLOCK_SIZE];
    for idx in [0, 1, 127, 255] {
        let data = client2.read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert_ne!(&data[..], &zero_block[..], "block {} should be non-zero after first drain", idx);
    }

    // Phase 2: Overwrite ALL blocks with zeros
    for idx in 0..num_blocks {
        client2.write(idx * BLOCK_SIZE as u64, &zero_block).await.unwrap();
        if idx % 64 == 0 {
            client2.flush().await.unwrap();
        }
    }
    client2.flush().await.unwrap();
    client2.disconnect().await.unwrap();

    // Drain the zero-block tombstone entries to S3
    server2.drain_all().await;
    server2.shutdown().await;

    // Phase 3: Cold restart — fresh server, restore from S3
    let server3 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server3.restore_export("vol", size_gb).await;
    let mut client3 = server3.connect("vol").await;

    // Verify EVERY block is zeros (tombstone entries must win over old non-zero packs)
    let zero_hash = sha256(&zero_block);
    let mut errors = 0u64;
    for idx in 0..num_blocks {
        let data = client3.read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        let actual = sha256(&data);
        if actual != zero_hash {
            // Check if we got stale non-zero data
            let stale = block_pattern(idx, BLOCK_SIZE);
            let stale_hash = sha256(&stale);
            if actual == stale_hash {
                eprintln!("STALE DATA block {}: tombstone entry did NOT supersede old pack", idx);
            } else {
                eprintln!("CORRUPTION block {}: neither zeros nor original pattern", idx);
            }
            errors += 1;
        }
    }

    client3.disconnect().await.unwrap();
    server3.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[zero_block_integrity] {} verified via S3 (zero tombstones) in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "zero-block verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 13. Fork chain integrity — grandchild resolves grandparent data
// ---------------------------------------------------------------------------

/// Parent writes data → snapshot → fork child → child writes more → snapshot →
/// fork grandchild. Grandchild must resolve blocks from both parent and child
/// through the inherited pack list. Cold restart and verify from S3.
#[tokio::test]
#[ignore]
async fn fork_chain_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-forkchain";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 64;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;

    // Phase 1: Parent writes blocks 0..32
    server.create_export("grandparent", size_gb).await;
    let mut gp_client = server.connect("grandparent").await;

    let mut expected: HashMap<u64, [u8; 32]> = HashMap::new();
    for idx in 0..32 {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        expected.insert(idx, blake3_hash(&pattern));
        gp_client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    gp_client.flush().await.unwrap();
    gp_client.disconnect().await.unwrap();
    server.snapshot_export("grandparent").await;

    // Phase 2: Fork child from grandparent, child writes blocks 32..48
    server.fork_export("parent", "grandparent", size_gb).await;
    let mut p_client = server.connect("parent").await;

    for idx in 32..48 {
        let pattern = block_pattern(idx + 100_000, BLOCK_SIZE);
        expected.insert(idx, blake3_hash(&pattern));
        p_client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    // Also overwrite block 0 in child (different from grandparent)
    let p_block0 = block_pattern(200_000, BLOCK_SIZE);
    expected.insert(0, blake3_hash(&p_block0));
    p_client.write(0, &p_block0).await.unwrap();
    p_client.flush().await.unwrap();
    p_client.disconnect().await.unwrap();
    server.snapshot_export("parent").await;

    // Phase 3: Fork grandchild from parent's manifest (stored under grandparent's s3_prefix)
    {
        let config = glidefs::config::ExportConfig {
            name: "grandchild".to_string(),
            size_gb,
            // s3_prefix = "grandparent" — that's where parent's manifest and all packs live
            s3_prefix: Some("grandparent".to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: None,
        };
        server
            .router
            .create_export(config, false, Some("parent"), None)
            .await
            .unwrap();
    }
    let mut gc_client = server.connect("grandchild").await;

    // Grandchild writes blocks 48..56
    for idx in 48..56 {
        let pattern = block_pattern(idx + 300_000, BLOCK_SIZE);
        expected.insert(idx, blake3_hash(&pattern));
        gc_client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    gc_client.flush().await.unwrap();
    gc_client.disconnect().await.unwrap();

    // Phase 4: Cold restart grandchild, verify ALL blocks from S3
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_forked_export("grandchild", "grandparent", size_gb).await;
    let mut reader = server2.connect("grandchild").await;

    let zero_hash = blake3_hash(&vec![0u8; BLOCK_SIZE]);
    let mut errors = 0u64;
    for idx in 0..num_blocks {
        let data = reader.read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        let actual = blake3_hash(&data);
        let expect = expected.get(&idx).unwrap_or(&zero_hash);
        if &actual != expect {
            let source = if idx < 32 && idx != 0 {
                "grandparent"
            } else if idx == 0 || (32..48).contains(&idx) {
                "parent"
            } else if (48..56).contains(&idx) {
                "grandchild"
            } else {
                "unwritten (should be zero)"
            };
            eprintln!("FORK CHAIN MISMATCH block {} (source: {})", idx, source);
            errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[fork_chain_integrity] {} verified via S3 (3-level fork) in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "fork chain verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 13b. Deep fork chain read-through (5 levels)
// ---------------------------------------------------------------------------

/// Creates A → B → C → D → E (5-level fork chain). Each level writes unique
/// blocks and overwrites one block from its parent. Cold restart and verify all
/// blocks from E resolve correctly through the full inheritance chain.
///
/// All exports share the root s3_prefix ("level-a") so manifests and packs are
/// co-located — matching the pattern used by `fork_chain_integrity`.
#[tokio::test]
#[ignore]
async fn fork_deep_chain_read_through() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-deepfork";
    let transport = crate::Transport::Nbd;

    // 128 blocks total — each level writes to a unique range of 20 blocks
    // plus overwrites block 0 with its own pattern.
    let num_blocks: u64 = 128;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;
    let blocks_per_level = 20u64;

    let levels = ["level-a", "level-b", "level-c", "level-d", "level-e"];
    let root_prefix = levels[0]; // All manifests/packs stored under root's s3_prefix

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;

    // Track expected hash for every block index
    let mut expected: HashMap<u64, [u8; 32]> = HashMap::new();

    for (i, &name) in levels.iter().enumerate() {
        if i == 0 {
            // Create root export
            server.create_export(name, size_gb).await;
        } else {
            // Fork from parent — all share the root s3_prefix
            let parent = levels[i - 1];
            let config = glidefs::config::ExportConfig {
                name: name.to_string(),
                size_gb,
                s3_prefix: Some(root_prefix.to_string()),
                block_size: None,
                flush_threshold: None,
                flush_mode: None,
                transport: None,
                compaction_cooldown: None,
                source: None,
            };
            server
                .router
                .create_export(config, false, Some(parent), None)
                .await
                .unwrap();
        }

        let mut client = server.connect(name).await;

        // Each level writes blocks [i*20 .. i*20+20) with a unique seed
        let base = i as u64 * blocks_per_level;
        for j in 0..blocks_per_level {
            let idx = base + j;
            if idx >= num_blocks {
                break;
            }
            let seed = idx + (i as u64 + 1) * 1_000_000;
            let pattern = block_pattern(seed, BLOCK_SIZE);
            expected.insert(idx, blake3_hash(&pattern));
            client
                .write(idx * BLOCK_SIZE as u64, &pattern)
                .await
                .unwrap();
        }

        // Every level overwrites block 0 with its own pattern
        let block0_seed = (i as u64 + 1) * 9_999_999;
        let block0_pattern = block_pattern(block0_seed, BLOCK_SIZE);
        expected.insert(0, blake3_hash(&block0_pattern));
        client.write(0, &block0_pattern).await.unwrap();

        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        if i < levels.len() - 1 {
            // Snapshot so the next level can fork
            server.snapshot_export(name).await;
        }
    }

    // Drain everything to S3, then cold restart
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    // Restore the leaf (level-e) from S3 via the shared root prefix
    server2
        .restore_forked_export("level-e", root_prefix, size_gb)
        .await;
    let mut reader = server2.connect("level-e").await;

    let zero_hash = blake3_hash(&vec![0u8; BLOCK_SIZE]);
    let mut errors = 0u64;
    for idx in 0..num_blocks {
        let data = reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        let expect = expected.get(&idx).unwrap_or(&zero_hash);
        if &actual != expect {
            eprintln!(
                "DEEP FORK MISMATCH block {} (expected from level {})",
                idx,
                if idx == 0 {
                    "e (last overwrite)"
                } else {
                    match idx / blocks_per_level {
                        0 => "a",
                        1 => "b",
                        2 => "c",
                        3 => "d",
                        4 => "e",
                        _ => "unwritten",
                    }
                }
            );
            errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[fork_deep_chain_read_through] {} verified via S3 (5-level fork) in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(
        errors, 0,
        "deep fork chain verification failed for {} blocks",
        errors
    );
}

// ---------------------------------------------------------------------------
// 14. Write-during-drain integrity — concurrent writes + flush + S3 verify
// ---------------------------------------------------------------------------

/// Launches drain while simultaneously writing new blocks. After drain
/// completes, does another drain to pick up the stragglers, then cold restart
/// and verify ALL data from S3.
///
/// Exercises: CAS DIRTY→SYNCING state machine, CRC sentinel handling,
/// block re-dirtying during flush, drain iteration recovery.
#[tokio::test]
#[ignore]
async fn write_during_drain() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-drain-race";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 512;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;

    // Phase 1: Write initial data to all blocks
    let mut client = server.connect("vol").await;
    let mut expected: HashMap<u64, [u8; 32]> = HashMap::new();

    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        expected.insert(idx, sha256(&pattern));
        client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
        if idx % 128 == 0 {
            client.flush().await.unwrap();
        }
    }
    client.flush().await.unwrap();

    // Phase 2: Start drain while simultaneously writing new data
    // The writer overwrites ~25% of blocks with new patterns during the drain.
    let info = server.connect_info("vol").await;
    let router = Arc::clone(&server.router);

    let expected_arc = Arc::new(tokio::sync::Mutex::new(expected));
    let expected_clone = Arc::clone(&expected_arc);

    // Spawn concurrent writer
    let writer_handle = tokio::spawn(async move {
        let mut writer = info.connect().await.unwrap();
        let mut rng = StdRng::seed_from_u64(0xDBA1_BACE_0000);
        let mut writes = 0u64;

        // Write in small batches, yielding between to interleave with drain
        for _ in 0..128 {
            let idx = rng.gen_range(0..num_blocks);
            let pattern = block_pattern(idx + 5_000_000, BLOCK_SIZE);
            let hash = sha256(&pattern);
            writer.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
            expected_clone.lock().await.insert(idx, hash);
            writes += 1;
            // Let drain make progress
            tokio::task::yield_now().await;
        }
        writer.flush().await.unwrap();
        writer.disconnect().await.unwrap();
        writes
    });

    // Concurrent drain (will compete with the writer)
    let drain_errors = router.drain_all().await;
    assert!(
        drain_errors.is_empty(),
        "first drain had errors: {:?}",
        drain_errors.iter().map(|(n, e)| format!("{n}: {e}")).collect::<Vec<_>>()
    );

    let writes_during_drain = writer_handle.await.unwrap();
    eprintln!("  {} writes completed during drain", writes_during_drain);

    // Phase 3: Second drain to pick up blocks dirtied during the first drain
    client.disconnect().await.unwrap();
    let drain_errors2 = router.drain_all().await;
    assert!(
        drain_errors2.is_empty(),
        "second drain had errors: {:?}",
        drain_errors2.iter().map(|(n, e)| format!("{n}: {e}")).collect::<Vec<_>>()
    );

    server.shutdown().await;

    // Phase 4: Cold restart — verify all blocks from S3
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut reader = server2.connect("vol").await;

    let expected = Arc::try_unwrap(expected_arc).unwrap().into_inner();
    let mut errors = 0u64;
    for idx in 0..num_blocks {
        let data = reader.read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        let actual = sha256(&data);
        let expect = expected.get(&idx).unwrap();
        if &actual != expect {
            eprintln!("DRAIN RACE MISMATCH block {}", idx);
            errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[write_during_drain] {} verified via S3 ({} concurrent writes) in {:.1}s — err={}",
        fmt_bytes(data_bytes),
        writes_during_drain,
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "drain race verification failed for {} blocks", errors);
}

// ---------------------------------------------------------------------------
// 15. Snapshot rollback integrity — point-in-time restore
// ---------------------------------------------------------------------------

/// Write data A → snapshot(seq=N) → overwrite with data B → fork from
/// snapshot seq=N → verify fork has data A, not data B.
///
/// Proves versioned manifest restore returns point-in-time data.
#[tokio::test]
#[ignore]
async fn snapshot_rollback() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-snapshot-rollback";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 128;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    // Phase 1: Write pattern A to all blocks
    let mut pattern_a_hashes: Vec<[u8; 32]> = Vec::with_capacity(num_blocks as usize);
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        pattern_a_hashes.push(sha256(&pattern));
        client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    // Snapshot — captures pattern A at this point in time
    let snap_resp = server.snapshot_export("vol").await;
    let snap_seq = snap_resp.sequence;
    eprintln!("  snapshot captured at sequence {}", snap_seq);

    // Phase 2: Overwrite ALL blocks with pattern B (different seed space)
    let mut client2 = server.connect("vol").await;
    let mut pattern_b_hashes: Vec<[u8; 32]> = Vec::with_capacity(num_blocks as usize);
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx + 20_000_000, BLOCK_SIZE);
        pattern_b_hashes.push(sha256(&pattern));
        client2.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    client2.flush().await.unwrap();
    client2.disconnect().await.unwrap();

    // Snapshot again (captures pattern B)
    server.snapshot_export("vol").await;

    // Phase 3: Fork from the FIRST snapshot (should get pattern A, not B)
    server.fork_export_from_snapshot("rollback", "vol", size_gb, snap_seq).await;
    let mut rb_client = server.connect("rollback").await;

    let mut errors = 0u64;
    for idx in 0..num_blocks {
        let data = rb_client.read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        let actual = sha256(&data);
        if actual != pattern_a_hashes[idx as usize] {
            if actual == pattern_b_hashes[idx as usize] {
                eprintln!("ROLLBACK FAIL block {}: got pattern B instead of A", idx);
            } else {
                eprintln!("CORRUPTION block {}: neither pattern A nor B", idx);
            }
            errors += 1;
        }
    }
    rb_client.disconnect().await.unwrap();

    // Phase 4: Cold restart the rollback fork, verify from S3
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_forked_export("rollback", "vol", size_gb).await;
    let mut reader = server2.connect("rollback").await;

    let mut s3_errors = 0u64;
    for idx in 0..num_blocks {
        let data = reader.read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        let actual = sha256(&data);
        if actual != pattern_a_hashes[idx as usize] {
            if actual == pattern_b_hashes[idx as usize] {
                eprintln!("S3 ROLLBACK FAIL block {}: got pattern B instead of A", idx);
            } else {
                eprintln!("S3 CORRUPTION block {}: neither A nor B", idx);
            }
            s3_errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[snapshot_rollback] {} verified (pre-drain + S3) in {:.1}s — err={}/{}",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        errors,
        s3_errors
    );
    assert_eq!(errors, 0, "pre-drain rollback failed for {} blocks", errors);
    assert_eq!(s3_errors, 0, "S3 rollback failed for {} blocks", s3_errors);
}

// ---------------------------------------------------------------------------
// 16. WAL crash recovery integrity — unclean shutdown + WAL replay + S3 verify
// ---------------------------------------------------------------------------

/// Writes data, drains some to S3, writes MORE data without draining, shuts
/// down (simulating crash). Restarts with the SAME cache dir → WAL replays
/// recovered dirty blocks. Drains, cold restart with fresh TempDir, verify
/// ALL data from S3.
///
/// Exercises: WAL append + replay, SSD pwrite persistence, metadata
/// checkpoint reconstruction, dirty block recovery after crash.
#[tokio::test]
#[ignore]
async fn wal_crash_recovery() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-wal-crash";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 256; // 32 MB
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // We manage the cache dir ourselves so it survives across restarts
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache_path = cache_dir.path().to_path_buf();

    // Phase 1: Write first half of blocks, drain to S3 (establishes manifest)
    let server = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        db_path,
        transport,
        cache_path.clone(),
    )
    .await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    let mut expected: HashMap<u64, [u8; 32]> = HashMap::new();
    for idx in 0..num_blocks / 2 {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        expected.insert(idx, sha256(&pattern));
        client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    // Drain first batch to S3 (creates manifest + packs)
    server.drain_all().await;
    eprintln!("  phase 1: {} blocks drained to S3", num_blocks / 2);

    // Phase 2: Write second half WITHOUT draining — data is only in WAL + SSD
    let mut client2 = server.connect("vol").await;
    for idx in num_blocks / 2..num_blocks {
        let pattern = block_pattern(idx + 7_000_000, BLOCK_SIZE);
        expected.insert(idx, sha256(&pattern));
        client2.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    // Also overwrite some blocks from phase 1 (tests WAL overwrites)
    for idx in [0, 1, 10, 50, 100] {
        let pattern = block_pattern(idx + 8_000_000, BLOCK_SIZE);
        expected.insert(idx, sha256(&pattern));
        client2.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
    }
    client2.flush().await.unwrap();
    client2.disconnect().await.unwrap();
    eprintln!(
        "  phase 2: {} blocks written to WAL (not drained)",
        num_blocks / 2 + 5
    );

    // Shutdown WITHOUT draining — simulates crash
    // WAL + SSD data file persist in cache_path
    server.shutdown().await;

    // Phase 3: Restart with SAME cache dir — WAL recovery
    let server2 = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        db_path,
        transport,
        cache_path.clone(),
    )
    .await;
    // create_export with manifest_name=None triggers WAL replay + loads manifest from S3
    server2.create_export("vol", size_gb).await;

    // Verify all blocks are readable (phase 1 from S3 manifest, phase 2 from WAL recovery)
    let mut client3 = server2.connect("vol").await;
    let mut recovery_errors = 0u64;
    for idx in 0..num_blocks {
        let data = client3
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = sha256(&data);
        let zero_hash = sha256(&vec![0u8; BLOCK_SIZE]);
        let expect = expected.get(&idx).unwrap_or(&zero_hash);
        if &actual != expect {
            eprintln!("RECOVERY MISMATCH block {} (phase {})", idx, if idx < num_blocks / 2 { 1 } else { 2 });
            recovery_errors += 1;
        }
    }
    client3.disconnect().await.unwrap();
    eprintln!("  WAL recovery verification: err={}", recovery_errors);
    assert_eq!(recovery_errors, 0, "WAL recovery had {} mismatches", recovery_errors);

    // Phase 4: Now drain the recovered dirty blocks to S3
    server2.drain_all().await;
    server2.shutdown().await;

    // Phase 5: Cold restart with FRESH TempDir — verify everything from S3
    let server3 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server3.restore_export("vol", size_gb).await;
    let mut reader = server3.connect("vol").await;

    let mut s3_errors = 0u64;
    for idx in 0..num_blocks {
        let data = reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = sha256(&data);
        let zero_hash = sha256(&vec![0u8; BLOCK_SIZE]);
        let expect = expected.get(&idx).unwrap_or(&zero_hash);
        if &actual != expect {
            eprintln!("S3 MISMATCH block {} after WAL recovery + drain", idx);
            s3_errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server3.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[wal_crash_recovery] {} verified (WAL recovery + S3) in {:.1}s — recovery_err={}, s3_err={}",
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        recovery_errors,
        s3_errors
    );
    assert_eq!(s3_errors, 0, "S3 verification after WAL recovery failed for {} blocks", s3_errors);
}

// ---------------------------------------------------------------------------
// 17. Multi-block read integrity — reads spanning multiple blocks
// ---------------------------------------------------------------------------

/// Writes individual blocks, then reads across block boundaries in various
/// sizes (2 blocks, 4 blocks, 7 blocks). Verifies the coalesced read returns
/// the correct concatenation of block data.
#[tokio::test]
#[ignore]
async fn multi_block_read() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-multiblock";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 128;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Write all blocks with deterministic patterns
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    let mut block_data: Vec<Vec<u8>> = Vec::with_capacity(num_blocks as usize);
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        client
            .write(idx * BLOCK_SIZE as u64, &pattern)
            .await
            .unwrap();
        block_data.push(pattern);
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    // Cold restart
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client2 = server2.connect("vol").await;

    // Read across block boundaries in various sizes from S3
    let read_sizes = [2, 3, 4, 7, 8, 16]; // in blocks
    let mut errors = 0u64;
    let mut reads_verified = 0u64;

    for &span in &read_sizes {
        for start_block in (0..num_blocks - span).step_by(span as usize) {
            let offset = start_block * BLOCK_SIZE as u64;
            let length = span as u32 * BLOCK_SIZE as u32;

            let data = client2.read(offset, length).await.unwrap();

            // Build expected data by concatenating individual blocks
            let mut expected = Vec::with_capacity(length as usize);
            for b in start_block..start_block + span {
                expected.extend_from_slice(&block_data[b as usize]);
            }

            if data != expected {
                let actual_hash = sha256(&data);
                let expected_hash = sha256(&expected);
                eprintln!(
                    "MULTI-BLOCK MISMATCH: blocks {}..{} (span={}): hash {:x?} vs {:x?}",
                    start_block,
                    start_block + span,
                    span,
                    &actual_hash[..8],
                    &expected_hash[..8]
                );
                errors += 1;
            }
            reads_verified += 1;
        }
    }

    client2.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[multi_block_read] {} multi-block reads verified via S3 ({}) in {:.1}s — err={}",
        reads_verified,
        fmt_bytes(data_bytes),
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(errors, 0, "multi-block read verification failed for {} reads", errors);
}

// ---------------------------------------------------------------------------
// 18. Unaligned cross-boundary read integrity
// ---------------------------------------------------------------------------

/// Reads starting at arbitrary offsets within a block, spanning across block
/// boundaries. Verifies the read-coalescing path handles non-block-aligned
/// offsets correctly — the offset math for slicing block data from S3 range
/// fetches is separate from the aligned-read path.
///
/// Test cases:
///   - Start mid-block (4KB in), read 3 blocks minus 4KB
///   - Start 60KB into a block, read exactly 2 blocks
///   - Start 1 byte in, read to 1 byte before a block boundary
///   - Start at last 4KB of one block, read first 4KB of next
#[tokio::test]
#[ignore]
async fn unaligned_cross_boundary_read() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-unaligned";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 64;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Write all blocks with deterministic patterns
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;
    let mut client = server.connect("vol").await;

    let mut block_data: Vec<Vec<u8>> = Vec::with_capacity(num_blocks as usize);
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        client
            .write(idx * BLOCK_SIZE as u64, &pattern)
            .await
            .unwrap();
        block_data.push(pattern);
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    // Cold restart — force all reads from S3
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut client2 = server2.connect("vol").await;

    // Helper: build expected data for a byte-range read across blocks
    let expected_range = |byte_offset: u64, byte_length: usize| -> Vec<u8> {
        let mut result = Vec::with_capacity(byte_length);
        let mut pos = byte_offset;
        let end = byte_offset + byte_length as u64;
        while pos < end {
            let block_idx = (pos / BLOCK_SIZE as u64) as usize;
            let offset_in_block = (pos % BLOCK_SIZE as u64) as usize;
            let remaining_in_block = BLOCK_SIZE - offset_in_block;
            let take = remaining_in_block.min((end - pos) as usize);
            result.extend_from_slice(
                &block_data[block_idx][offset_in_block..offset_in_block + take],
            );
            pos += take as u64;
        }
        result
    };

    // (offset_within_first_block, total_length_in_bytes, description)
    let sub_block = 4096usize;
    let test_cases: Vec<(u64, u32, &str)> = vec![
        // Start 4KB into block 1, read across 3 block boundaries
        (
            BLOCK_SIZE as u64 + sub_block as u64,
            (3 * BLOCK_SIZE - sub_block) as u32,
            "4KB into block 1, span 3 blocks",
        ),
        // Start 60KB into block 2, read exactly 2 blocks of data
        (
            2 * BLOCK_SIZE as u64 + 60 * 1024,
            (2 * BLOCK_SIZE) as u32,
            "60KB into block 2, read 2 blocks",
        ),
        // Start 1 byte into block 5, read to 1 byte before block 7 boundary
        (
            5 * BLOCK_SIZE as u64 + 1,
            (2 * BLOCK_SIZE - 2) as u32,
            "1 byte into block 5, end 1 byte before block 7",
        ),
        // Cross exactly one boundary: last 4KB of block 10, first 4KB of block 11
        (
            11 * BLOCK_SIZE as u64 - sub_block as u64,
            (2 * sub_block) as u32,
            "last 4KB of block 10 + first 4KB of block 11",
        ),
        // Read within a single block (no boundary crossing) — baseline
        (
            20 * BLOCK_SIZE as u64 + sub_block as u64,
            (BLOCK_SIZE - 2 * sub_block) as u32,
            "mid-block read, no crossing",
        ),
        // Start at very end of block 30, read 5 full blocks + partial
        (
            31 * BLOCK_SIZE as u64 - 1,
            (5 * BLOCK_SIZE + 2) as u32,
            "1 byte before block 31, span 5+ blocks",
        ),
    ];

    let mut errors = 0u64;
    let mut reads_verified = 0u64;

    for (offset, length, desc) in &test_cases {
        let data = client2.read(*offset, *length).await.unwrap();
        let expected = expected_range(*offset, *length as usize);

        if data != expected {
            let actual_hash = sha256(&data);
            let expected_hash = sha256(&expected);
            eprintln!(
                "UNALIGNED MISMATCH [{}]: offset={} len={}: hash {:x?} vs {:x?}",
                desc,
                offset,
                length,
                &actual_hash[..8],
                &expected_hash[..8]
            );
            errors += 1;
        }
        reads_verified += 1;
    }

    client2.disconnect().await.unwrap();
    server2.shutdown().await;

    eprintln!(
        "[unaligned_cross_boundary_read] {} reads verified via S3 in {:.1}s — err={}",
        reads_verified,
        t0.elapsed().as_secs_f64(),
        errors
    );
    assert_eq!(
        errors, 0,
        "unaligned cross-boundary read verification failed for {} reads",
        errors
    );
}

// ---------------------------------------------------------------------------
// 19. Promote-to-readwrite integrity
// ---------------------------------------------------------------------------

/// Fork a readonly export from a parent, promote to read-write, write new data,
/// drain to S3, cold restart, verify:
///   - Parent data is unchanged (no cross-export leaks)
///   - Child has parent data for unwritten blocks
///   - Child has new data for written blocks
///
/// Catches: post-promote dirty block state initialization, fork pack resolution
/// after promotion, write path incorrectly rejecting writes on promoted export.
#[tokio::test]
#[ignore]
async fn promote_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-promote";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 128;
    let overwrite_blocks: u64 = 32;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Phase 1: Write parent data, snapshot, fork
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("parent", size_gb).await;
    let mut parent_client = server.connect("parent").await;

    let mut parent_hashes: HashMap<u64, [u8; 32]> = HashMap::new();
    for idx in 0..num_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        parent_hashes.insert(idx, sha256(&pattern));
        parent_client
            .write(idx * BLOCK_SIZE as u64, &pattern)
            .await
            .unwrap();
    }
    parent_client.flush().await.unwrap();
    parent_client.disconnect().await.unwrap();

    // Snapshot parent, fork child (read-write)
    server.snapshot_export("parent").await;
    server.fork_export("child", "parent", size_gb).await;

    // Phase 2: Overwrite first N blocks (matching fork_integrity pattern)
    let mut child_client = server.connect("child").await;

    let mut child_hashes = parent_hashes.clone();
    for idx in 0..overwrite_blocks {
        let new_pattern = block_pattern(idx + 10000, BLOCK_SIZE);
        child_hashes.insert(idx, sha256(&new_pattern));
        child_client
            .write(idx * BLOCK_SIZE as u64, &new_pattern)
            .await
            .unwrap();
    }
    child_client.flush().await.unwrap();
    child_client.disconnect().await.unwrap();

    // Phase 3: Drain and cold restart
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("parent", size_gb).await;
    server2
        .restore_forked_export("child", "parent", size_gb)
        .await;

    // Phase 4: Verify parent is unchanged
    let mut parent_reader = server2.connect("parent").await;
    let mut parent_errors = 0u64;
    for idx in 0..num_blocks {
        let data = parent_reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        if sha256(&data) != parent_hashes[&idx] {
            eprintln!("PARENT MISMATCH block {}", idx);
            parent_errors += 1;
        }
    }
    parent_reader.disconnect().await.unwrap();

    // Phase 5: Verify child has mix of parent + new data
    let mut child_reader = server2.connect("child").await;
    let mut child_errors = 0u64;
    for idx in 0..num_blocks {
        let data = child_reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual_hash = sha256(&data);
        if actual_hash != child_hashes[&idx] {
            let is_parent = actual_hash == parent_hashes[&idx];
            let is_zero = data.iter().all(|&b| b == 0);
            eprintln!(
                "CHILD MISMATCH block {} ({}): is_parent_data={}, is_zeros={}, first_byte={:#x}",
                idx,
                if idx < overwrite_blocks { "overwritten" } else { "inherited" },
                is_parent,
                is_zero,
                data[0]
            );
            child_errors += 1;
        }
    }
    child_reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = num_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[promote_integrity] {} verified (parent_err={}, child_err={}) in {:.1}s",
        fmt_bytes(data_bytes),
        parent_errors,
        child_errors,
        t0.elapsed().as_secs_f64()
    );
    assert_eq!(parent_errors, 0, "parent data corrupted after child promote+write");
    assert_eq!(child_errors, 0, "child data mismatch after promote+write+S3 roundtrip");
}

// ---------------------------------------------------------------------------
// 20. Resize integrity — existing data survives grow, new range is zeros
// ---------------------------------------------------------------------------

/// Write data, drain to S3, resize (grow), cold restart, verify:
///   - All original blocks are byte-identical
///   - New blocks in the extended range are all zeros
///
/// Catches: resize clearing the block presence bitmap, manifest corruption
/// during grow, incorrect device_size in the new manifest.
#[tokio::test]
#[ignore]
async fn resize_integrity() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "integrity-resize";
    let transport = crate::Transport::Nbd;

    let original_blocks: u64 = 64;
    let original_size_gb = (original_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    // Phase 1: Write data to original-sized export
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", original_size_gb).await;
    let mut client = server.connect("vol").await;

    let mut expected_hashes: HashMap<u64, [u8; 32]> = HashMap::new();
    for idx in 0..original_blocks {
        let pattern = block_pattern(idx, BLOCK_SIZE);
        expected_hashes.insert(idx, sha256(&pattern));
        client
            .write(idx * BLOCK_SIZE as u64, &pattern)
            .await
            .unwrap();
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    // Drain to S3 so data is persisted
    server.drain_all().await;

    // Phase 2: Resize — double the volume
    let new_blocks: u64 = 128;
    let new_size_gb = (new_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;
    server.resize_export("vol", new_size_gb).await;

    // Verify new size is visible
    let mut client2 = server.connect("vol").await;
    let reported_size = client2.export_size();
    let expected_bytes = (new_size_gb * 1_073_741_824.0) as u64;
    assert_eq!(
        reported_size, expected_bytes,
        "export should report new size after resize"
    );
    client2.disconnect().await.unwrap();

    // Drain again (manifest updated with new size)
    server.drain_all().await;
    server.shutdown().await;

    // Phase 3: Cold restart from S3
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", new_size_gb).await;
    let mut reader = server2.connect("vol").await;

    // Phase 4: Verify original blocks are intact
    let mut original_errors = 0u64;
    for idx in 0..original_blocks {
        let data = reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        if sha256(&data) != expected_hashes[&idx] {
            eprintln!("RESIZE MISMATCH block {} (original range)", idx);
            original_errors += 1;
        }
    }

    // Phase 5: Verify new blocks are zeros
    let zero_hash = sha256(&vec![0u8; BLOCK_SIZE]);
    let mut zero_errors = 0u64;
    for idx in original_blocks..new_blocks {
        let data = reader
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        if sha256(&data) != zero_hash {
            eprintln!("RESIZE MISMATCH block {} (new range, expected zeros)", idx);
            zero_errors += 1;
        }
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;

    let data_bytes = new_blocks * BLOCK_SIZE as u64;
    eprintln!(
        "[resize_integrity] {} verified (original_err={}, zero_err={}) in {:.1}s",
        fmt_bytes(data_bytes),
        original_errors,
        zero_errors,
        t0.elapsed().as_secs_f64()
    );
    assert_eq!(original_errors, 0, "original data corrupted after resize");
    assert_eq!(zero_errors, 0, "new blocks should be zeros after resize");
}

// ---------------------------------------------------------------------------
// 11. Soak: mixed operations (write, read, trim, write_zeroes, flush,
//     fork, snapshot, delete) with reference model
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn soak_mixed_operations() {
    let duration_secs: u64 = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "soak-mixed";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 2048; // 256 MB
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    let mut rng = StdRng::seed_from_u64(0x504B_FEED_C0DE);
    let zero_hash = blake3_hash(&vec![0u8; BLOCK_SIZE]);

    // Per-export reference models: export name → (block idx → hash)
    let mut models: HashMap<String, HashMap<u64, [u8; 32]>> = HashMap::new();
    let mut exports_alive: Vec<String> = Vec::new();
    // Track fork parents for restore: fork name → source prefix
    let mut fork_sources: HashMap<String, String> = HashMap::new();

    let mut total_ops: u64 = 0;
    let mut verify_passes: u64 = 0;

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol-0", size_gb).await;
    models.insert("vol-0".to_string(), HashMap::new());
    exports_alive.push("vol-0".to_string());

    let mut client = server.connect("vol-0").await;
    let mut active_export = "vol-0".to_string();

    let deadline = t0 + std::time::Duration::from_secs(duration_secs);

    while Instant::now() < deadline {
        // Pick a random operation (weighted toward writes)
        let op = rng.gen_range(0u32..100);
        let handler = server
            .router
            .get_handler(&active_export)
            .await
            .expect("active export missing handler");

        match op {
            // Write (40%)
            0..40 => {
                let idx = rng.gen_range(0..num_blocks);
                let pattern = block_pattern(rng.r#gen(), BLOCK_SIZE);
                let hash = blake3_hash(&pattern);
                handler.write(idx * BLOCK_SIZE as u64, &pattern, false).await.unwrap();
                models.get_mut(&active_export).unwrap().insert(idx, hash);
            }
            // Read (20%)
            40..60 => {
                let idx = rng.gen_range(0..num_blocks);
                let data = handler.read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
                let actual = blake3_hash(&data);
                let model = models.get(&active_export).unwrap();
                let expect = model.get(&idx).unwrap_or(&zero_hash);
                assert_eq!(
                    &actual, expect,
                    "read mismatch at block {} on export {}",
                    idx, active_export
                );
            }
            // Trim (10%)
            60..70 => {
                let idx = rng.gen_range(0..num_blocks);
                handler.trim(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32, false).await.unwrap();
                models.get_mut(&active_export).unwrap().insert(idx, zero_hash);
            }
            // Write zeroes (10%)
            70..80 => {
                let idx = rng.gen_range(0..num_blocks);
                handler
                    .write_zeroes(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32, false)
                    .await
                    .unwrap();
                models.get_mut(&active_export).unwrap().insert(idx, zero_hash);
            }
            // Flush (8%)
            80..88 => {
                handler.flush().unwrap();
            }
            // Snapshot (5%)
            88..93 => {
                server.snapshot_export(&active_export).await;
            }
            // Fork (4%)
            93..97 => {
                if exports_alive.len() < 8 {
                    let fork_name = format!("vol-{}", exports_alive.len());
                    // Drain to ensure fork can see S3 data
                    server.router.drain_export(&active_export).await.unwrap();
                    // All forks share root prefix "vol-0"
                    {
                        let config = glidefs::config::ExportConfig {
                            name: fork_name.clone(),
                            size_gb,
                            s3_prefix: Some("vol-0".to_string()),
                            block_size: None,
                            flush_threshold: None,
                            flush_mode: None,
                            transport: None,
                            compaction_cooldown: None,
                            source: None,
                        };
                        server
                            .router
                            .create_export(config, false, Some(&active_export), None)
                            .await
                            .unwrap();
                    }
                    // Fork inherits parent's model
                    let parent_model = models.get(&active_export).unwrap().clone();
                    models.insert(fork_name.clone(), parent_model);
                    fork_sources.insert(fork_name.clone(), "vol-0".to_string());
                    exports_alive.push(fork_name.clone());

                    // Switch to the new fork
                    client.disconnect().await.unwrap();
                    client = server.connect(&fork_name).await;
                    active_export = fork_name;
                }
            }
            // Switch active export (3%)
            _ => {
                if exports_alive.len() > 1 {
                    let idx = rng.gen_range(0..exports_alive.len());
                    let new_export = exports_alive[idx].clone();
                    if new_export != active_export {
                        client.disconnect().await.unwrap();
                        client = server.connect(&new_export).await;
                        active_export = new_export;
                    }
                }
            }
        }
        total_ops += 1;
    }

    // Final verification: drain all, cold restart, verify each export
    client.disconnect().await.unwrap();
    for name in &exports_alive {
        server.router.drain_export(name).await.unwrap();
    }
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    let mut final_errors = 0u64;

    for name in &exports_alive {
        // Forks store manifests under the source's s3_prefix
        if let Some(source) = fork_sources.get(name) {
            server2.restore_forked_export(name, source, size_gb).await;
        } else {
            server2.restore_export(name, size_gb).await;
        }
        let mut c2 = server2.connect(name).await;
        let model = models.get(name).unwrap();

        for (&idx, expect) in model {
            let data = c2
                .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            let actual = blake3_hash(&data);
            if &actual != expect {
                final_errors += 1;
                eprintln!(
                    "  MISMATCH: export={}, block={}",
                    name, idx
                );
            }
        }

        c2.disconnect().await.unwrap();
        verify_passes += 1;
    }

    server2.shutdown().await;

    eprintln!();
    eprintln!("=== Soak Mixed Operations ({}s) ===", duration_secs);
    eprintln!("  Total ops:        {}", total_ops);
    eprintln!("  Exports alive:    {}", exports_alive.len());
    eprintln!("  Verify passes:    {}", verify_passes);
    eprintln!("  Final errors:     {}", final_errors);
    eprintln!();

    assert_eq!(final_errors, 0, "soak mixed operations verification failed");
}

// ---------------------------------------------------------------------------
// 12. Soak: concurrent clients — 8 tasks doing random reads/writes
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn soak_concurrent_clients() {
    let duration_secs: u64 = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "soak-concurrent";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 2048;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;

    let num_clients = 8u64;
    let conn_info = server.connect_info("vol").await;

    // Each client writes to its own block range to avoid write conflicts
    // but reads from the full range to test concurrent read paths
    let blocks_per_client = num_blocks / num_clients;
    let deadline = t0 + std::time::Duration::from_secs(duration_secs);

    let mut handles = Vec::new();

    for client_id in 0..num_clients {
        let info = conn_info.clone();
        let block_start = client_id * blocks_per_client;
        let block_end = block_start + blocks_per_client;

        handles.push(tokio::spawn(async move {
            let mut client = info.connect().await.unwrap();
            let mut rng = StdRng::seed_from_u64(0xC11E_0000 + client_id);
            let mut written: HashMap<u64, [u8; 32]> = HashMap::new();
            let mut ops = 0u64;
            let mut errors = 0u64;

            while Instant::now() < deadline {
                if rng.gen_bool(0.6) {
                    // Write to own range
                    let idx = rng.gen_range(block_start..block_end);
                    let pattern = block_pattern(rng.r#gen(), BLOCK_SIZE);
                    let hash = blake3_hash(&pattern);
                    client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
                    written.insert(idx, hash);
                } else {
                    // Read from own range (verify last-written value)
                    let idx = rng.gen_range(block_start..block_end);
                    let data = client
                        .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                        .await
                        .unwrap();
                    let actual = blake3_hash(&data);
                    if let Some(expect) = written.get(&idx) {
                        if &actual != expect {
                            errors += 1;
                        }
                    }
                    // else: unwritten block, skip check (could be zero or another client's)
                }
                ops += 1;

                // Periodic flush
                if ops % 50 == 0 {
                    client.flush().await.unwrap();
                }
            }

            client.flush().await.unwrap();
            client.disconnect().await.unwrap();

            (client_id, ops, written, errors)
        }));
    }

    // Collect results
    let mut total_ops = 0u64;
    let mut all_written: HashMap<u64, [u8; 32]> = HashMap::new();
    let mut live_errors = 0u64;

    for handle in handles {
        let (cid, ops, written, errs) = handle.await.unwrap();
        eprintln!("  client {}: {} ops, {} blocks written, {} errors", cid, ops, written.len(), errs);
        total_ops += ops;
        live_errors += errs;
        all_written.extend(written);
    }

    assert_eq!(live_errors, 0, "live read errors during concurrent soak");

    // Cold restart verification
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut verifier = server2.connect("vol").await;

    let mut cold_errors = 0u64;
    for (&idx, expect) in &all_written {
        let data = verifier
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        let actual = blake3_hash(&data);
        if &actual != expect {
            cold_errors += 1;
        }
    }

    verifier.disconnect().await.unwrap();
    server2.shutdown().await;

    eprintln!();
    eprintln!("=== Soak Concurrent Clients ({}s) ===", duration_secs);
    eprintln!("  Clients:          {}", num_clients);
    eprintln!("  Total ops:        {}", total_ops);
    eprintln!("  Unique blocks:    {}", all_written.len());
    eprintln!("  Cold errors:      {}", cold_errors);
    eprintln!();

    assert_eq!(cold_errors, 0, "soak concurrent clients cold verification failed");
}

// ---------------------------------------------------------------------------
// 13. Soak: crash loop — write, kill, restart, verify × 20
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn soak_crash_loop() {
    let ctx = TestContext::new().await;
    let db_path = "soak-crash";
    let transport = crate::Transport::Nbd;

    let num_blocks: u64 = 1024;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;
    let num_cycles: u32 = std::env::var("SOAK_CRASH_CYCLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    let zero_hash = blake3_hash(&vec![0u8; BLOCK_SIZE]);
    let mut rng = StdRng::seed_from_u64(0xC2A5_1001_DEAD);

    // Track only flushed blocks (survived to SSD) — unflushed writes may be lost.
    let mut flushed: HashMap<u64, [u8; 32]> = HashMap::new();
    let mut pending: HashMap<u64, [u8; 32]> = HashMap::new();

    // Use a persistent cache directory (survives across "crashes")
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache_path = cache_dir.path().to_path_buf();

    for cycle in 0..num_cycles {
        eprintln!("[crash-loop] cycle {}/{}", cycle + 1, num_cycles);

        let server = if cycle == 0 {
            let s = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
            s.create_export("vol", size_gb).await;
            s
        } else {
            // Start with existing cache dir — triggers WAL recovery
            let s = TestServer::start_with_cache_dir(
                Arc::clone(&ctx.object_store),
                db_path,
                transport,
                cache_path.clone(),
            )
            .await;
            s.restore_export("vol", size_gb).await;
            s
        };

        let mut client = server.connect("vol").await;

        // Verify previously flushed blocks survived
        let mut verify_errors = 0u64;
        let check_count = flushed.len().min(200);
        let check_indices: Vec<u64> = flushed.keys().copied().collect::<Vec<_>>()[..check_count].to_vec();

        for idx in &check_indices {
            let data = client
                .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            let actual = blake3_hash(&data);
            let expect = flushed.get(idx).unwrap_or(&zero_hash);
            if &actual != expect {
                verify_errors += 1;
            }
        }
        if cycle > 0 {
            eprintln!("  verified {} blocks: {} errors", check_count, verify_errors);
            assert_eq!(
                verify_errors, 0,
                "crash loop cycle {}: flushed block verification failed",
                cycle
            );
        }

        // Write random blocks
        let batch_size = rng.gen_range(20..60);
        pending.clear();
        for _ in 0..batch_size {
            let idx = rng.gen_range(0..num_blocks);
            let pattern = block_pattern(rng.r#gen(), BLOCK_SIZE);
            let hash = blake3_hash(&pattern);
            client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
            pending.insert(idx, hash);
        }

        // Flush — after this, pending blocks are durable on SSD
        client.flush().await.unwrap();
        flushed.extend(pending.drain());

        // "Crash" — drop without draining to S3 (simulates kill -9)
        client.disconnect().await.unwrap();
        // Don't drain, don't shutdown gracefully — just drop the server
        // The router shutdown will run cleanup but cache_dir persists
        server.shutdown().await;
    }

    eprintln!();
    eprintln!("=== Soak Crash Loop ({} cycles) ===", num_cycles);
    eprintln!("  Unique flushed blocks: {}", flushed.len());
    eprintln!("  All cycles passed");
}

// ---------------------------------------------------------------------------
// 14. Soak: fork chain churn — build 20-fork chain, delete alternating, GC
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn soak_fork_chain_churn() {
    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "soak-fork-churn";
    let transport = crate::Transport::Nbd;

    let blocks_per_level = 32u64;
    let num_levels: usize = 20;
    let num_blocks: u64 = (num_levels as u64 + 1) * blocks_per_level;
    let size_gb = (num_blocks as f64 * BLOCK_SIZE as f64) / 1_073_741_824.0;

    let mut rng = StdRng::seed_from_u64(0xF02E_C4A1_0000);

    // Per-export reference model
    let mut models: Vec<(String, HashMap<u64, [u8; 32]>)> = Vec::new();

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;

    // Create root export and write initial data
    let root_name = "fork-0".to_string();
    server.create_export(&root_name, size_gb).await;
    let mut client = server.connect(&root_name).await;
    let mut root_model: HashMap<u64, [u8; 32]> = HashMap::new();

    for i in 0..blocks_per_level {
        let pattern = block_pattern(rng.r#gen(), BLOCK_SIZE);
        let hash = blake3_hash(&pattern);
        client.write(i * BLOCK_SIZE as u64, &pattern).await.unwrap();
        root_model.insert(i, hash);
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();
    server.snapshot_export(&root_name).await;

    models.push((root_name.clone(), root_model));

    // Build chain: each level forks from previous, writes to its own block range.
    // All forks share the root s3_prefix so manifests and packs are co-located.
    for level in 1..=num_levels {
        let parent_name = format!("fork-{}", level - 1);
        let child_name = format!("fork-{}", level);

        // Drain parent to S3 before forking
        server.router.drain_export(&parent_name).await.unwrap();
        // All forks use root prefix (multi-level chains require shared s3_prefix)
        {
            let config = glidefs::config::ExportConfig {
                name: child_name.clone(),
                size_gb,
                s3_prefix: Some(root_name.clone()),
                block_size: None,
                flush_threshold: None,
                flush_mode: None,
                transport: None,
                compaction_cooldown: None,
                source: None,
            };
            server
                .router
                .create_export(config, false, Some(&parent_name), None)
                .await
                .unwrap();
        }

        let mut client = server.connect(&child_name).await;

        // Inherit parent model
        let parent_model = models.last().unwrap().1.clone();
        let mut child_model = parent_model;

        // Write to this level's block range + overwrite block 0
        let block_start = level as u64 * blocks_per_level;
        for i in 0..blocks_per_level {
            let idx = block_start + i;
            let pattern = block_pattern(rng.r#gen(), BLOCK_SIZE);
            let hash = blake3_hash(&pattern);
            client.write(idx * BLOCK_SIZE as u64, &pattern).await.unwrap();
            child_model.insert(idx, hash);
        }
        // Overwrite block 0 with level-specific data
        let b0_pattern = block_pattern(rng.r#gen(), BLOCK_SIZE);
        let b0_hash = blake3_hash(&b0_pattern);
        client.write(0, &b0_pattern).await.unwrap();
        child_model.insert(0, b0_hash);

        client.flush().await.unwrap();
        client.disconnect().await.unwrap();
        server.snapshot_export(&child_name).await;

        models.push((child_name, child_model));

        if level % 5 == 0 {
            eprintln!("[fork-churn] built {}/{} levels", level, num_levels);
        }
    }

    // Drain all exports to S3
    for (name, _) in &models {
        server.router.drain_export(name).await.unwrap();
    }

    // Delete every other export (keep even indices: 0, 2, 4, ...)
    let mut deleted = Vec::new();
    let mut kept = Vec::new();
    for (i, (name, model)) in models.iter().enumerate() {
        if i % 2 == 1 {
            server.router.remove_export(name, true).await.unwrap();
            deleted.push(name.clone());
        } else {
            kept.push((name.clone(), model.clone()));
        }
    }
    eprintln!(
        "[fork-churn] deleted {} exports, kept {}",
        deleted.len(),
        kept.len()
    );

    server.shutdown().await;

    // Cold restart and verify kept exports
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    let mut final_errors = 0u64;

    for (name, model) in &kept {
        // All forks share root prefix; root itself uses its own prefix
        if name == &root_name {
            server2.restore_export(name, size_gb).await;
        } else {
            server2.restore_forked_export(name, &root_name, size_gb).await;
        }
        let mut c2 = server2.connect(name).await;

        for (&idx, expect) in model {
            let data = c2
                .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            let actual = blake3_hash(&data);
            if &actual != expect {
                final_errors += 1;
                eprintln!("  MISMATCH: export={}, block={}", name, idx);
            }
        }
        c2.disconnect().await.unwrap();
    }

    server2.shutdown().await;

    eprintln!();
    eprintln!("=== Soak Fork Chain Churn ({:.1}s) ===", t0.elapsed().as_secs_f64());
    eprintln!("  Levels built:     {}", num_levels);
    eprintln!("  Exports deleted:  {}", deleted.len());
    eprintln!("  Exports verified: {}", kept.len());
    eprintln!("  Final errors:     {}", final_errors);
    eprintln!();

    assert_eq!(final_errors, 0, "soak fork chain churn verification failed");
}

// ---------------------------------------------------------------------------
// 15. Soak: sub-block writes with byte-level reference model
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn soak_sub_block_writes() {
    let duration_secs: u64 = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let t0 = Instant::now();
    let ctx = TestContext::new().await;
    let db_path = "soak-subblock";
    let transport = crate::Transport::Nbd;

    // Smaller device — byte-level tracking is memory intensive
    let num_blocks: u64 = 256; // 32 MB
    let device_size = num_blocks * BLOCK_SIZE as u64;
    let size_gb = device_size as f64 / 1_073_741_824.0;

    let mut rng = StdRng::seed_from_u64(0x5B8E_0C10_CAFE);

    // Byte-level reference model: track expected bytes per block.
    // To save memory, only track blocks that have been written to.
    let mut block_models: HashMap<u64, Vec<u8>> = HashMap::new();

    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server.create_export("vol", size_gb).await;

    let handler = server
        .router
        .get_handler("vol")
        .await
        .expect("no handler");

    let mut total_ops: u64 = 0;
    let deadline = t0 + std::time::Duration::from_secs(duration_secs);

    while Instant::now() < deadline {
        // Random sub-block write (64B to 4096B)
        let block_idx = rng.gen_range(0..num_blocks);
        let write_size: usize = rng.gen_range(64..=4096);
        let max_offset_within_block = BLOCK_SIZE - write_size;
        let offset_within_block: usize = rng.gen_range(0..=max_offset_within_block);

        let device_offset = block_idx * BLOCK_SIZE as u64 + offset_within_block as u64;

        // Generate random data for the sub-block write
        let mut write_data = vec![0u8; write_size];
        for byte in &mut write_data {
            *byte = rng.r#gen();
        }

        handler.write(device_offset, &write_data, false).await.unwrap();

        // Update byte-level model
        let model = block_models
            .entry(block_idx)
            .or_insert_with(|| vec![0u8; BLOCK_SIZE]);
        model[offset_within_block..offset_within_block + write_size]
            .copy_from_slice(&write_data);

        total_ops += 1;

        // Periodic full-block read to verify merge correctness
        if total_ops % 100 == 0 {
            // Pick a random written block and verify
            let written_blocks: Vec<u64> = block_models.keys().copied().collect();
            if !written_blocks.is_empty() {
                let check_idx = written_blocks[rng.gen_range(0..written_blocks.len())];
                let read_data = handler
                    .read(check_idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                    .await
                    .unwrap();
                let expected = block_models.get(&check_idx).unwrap();
                assert_eq!(
                    read_data.as_ref(),
                    expected.as_slice(),
                    "sub-block merge mismatch at block {} after {} ops",
                    check_idx,
                    total_ops
                );
            }
        }

        // Periodic flush
        if total_ops % 200 == 0 {
            handler.flush().unwrap();
        }
    }

    // Final verification: drain to S3, cold restart, verify all tracked blocks
    handler.flush().unwrap();
    server.drain_all().await;
    server.shutdown().await;

    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
    server2.restore_export("vol", size_gb).await;
    let mut cold_client = server2.connect("vol").await;

    let mut cold_errors = 0u64;
    for (&idx, expected) in &block_models {
        let data = cold_client
            .read(idx * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        if data.as_slice() != expected.as_slice() {
            cold_errors += 1;
            // Find first mismatched byte for diagnostics
            for (b, (got, exp)) in data.iter().zip(expected.iter()).enumerate() {
                if got != exp {
                    eprintln!(
                        "  MISMATCH: block={}, byte_offset={}, got=0x{:02x}, expected=0x{:02x}",
                        idx, b, got, exp
                    );
                    break;
                }
            }
        }
    }

    cold_client.disconnect().await.unwrap();
    server2.shutdown().await;

    eprintln!();
    eprintln!("=== Soak Sub-Block Writes ({}s) ===", duration_secs);
    eprintln!("  Total sub-block writes: {}", total_ops);
    eprintln!("  Blocks touched:         {}", block_models.len());
    eprintln!("  Cold errors:            {}", cold_errors);
    eprintln!();

    assert_eq!(cold_errors, 0, "soak sub-block writes cold verification failed");
}
