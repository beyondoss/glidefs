use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

#[tokio::test]
async fn test_live_migration_readonly_fork_promote() {
    let ctx = TestContext::new().await;

    // --- Server A: create export, write 4 blocks, snapshot ---
    let server_a = TestServer::start(Arc::clone(&ctx.object_store), "mig-a").await;
    server_a.create_export("vm1", 0.01).await;

    let mut client_a = server_a.connect("vm1").await;
    client_a.write(0, &vec![0xAA; BLOCK_SIZE]).await.unwrap();
    client_a.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).await.unwrap();
    client_a.write(2 * BLOCK_SIZE as u64, &vec![0xCC; BLOCK_SIZE]).await.unwrap();
    client_a.write(3 * BLOCK_SIZE as u64, &vec![0xDD; BLOCK_SIZE]).await.unwrap();
    client_a.flush().await.unwrap();

    server_a.snapshot_export("vm1").await;

    // --- Server B: fork "vm1" as readonly from source manifest ---
    // Same db_path so server B can see server A's manifests and packs in S3.
    let server_b = TestServer::start(Arc::clone(&ctx.object_store), "mig-a").await;
    server_b.fork_export_readonly("vm1", "vm1", 0.01).await;

    let mut client_b = server_b.connect("vm1").await;

    // Verify all 4 blocks read back correctly on Server B
    let patterns: &[u8] = &[0xAA, 0xBB, 0xCC, 0xDD];
    for (i, &expected) in patterns.iter().enumerate() {
        let data = client_b.read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            data.iter().all(|&b| b == expected),
            "block {} on readonly fork should be {:#x}, got {:#x}",
            i, expected, data[0],
        );
    }

    // Attempt write to readonly fork -- must be rejected
    let err_code = client_b
        .write_raw(4 * BLOCK_SIZE as u64, &vec![0xEE; BLOCK_SIZE])
        .await
        .unwrap();
    assert_ne!(err_code, 0, "write to readonly fork should return non-zero error code");

    // Promote the fork to read-write
    server_b.promote_export("vm1").await;

    // Reconnect after promote (the old connection may still see old state)
    client_b.disconnect().await.unwrap();
    let mut client_b = server_b.connect("vm1").await;

    // Write new data to block 4
    client_b.write(4 * BLOCK_SIZE as u64, &vec![0xEE; BLOCK_SIZE]).await.unwrap();
    client_b.flush().await.unwrap();

    // Read blocks 0-4 on Server B -- old data + new block
    let all_patterns: &[u8] = &[0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
    for (i, &expected) in all_patterns.iter().enumerate() {
        let data = client_b.read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            data.iter().all(|&b| b == expected),
            "block {} on promoted fork should be {:#x}, got {:#x}",
            i, expected, data[0],
        );
    }

    // Read blocks 0-3 on Server A -- source data must be unaffected
    for (i, &expected) in patterns.iter().enumerate() {
        let data = client_a.read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            data.iter().all(|&b| b == expected),
            "block {} on source should still be {:#x}, got {:#x}",
            i, expected, data[0],
        );
    }

    // Cleanup
    client_a.disconnect().await.unwrap();
    client_b.disconnect().await.unwrap();
    server_a.shutdown().await;
    server_b.shutdown().await;
}

#[tokio::test]
async fn test_migration_survives_cold_wake() {
    let ctx = TestContext::new().await;

    // --- Server A: create export, write 2 blocks, snapshot, shutdown ---
    let server_a = TestServer::start(Arc::clone(&ctx.object_store), "cold-mig-a").await;
    server_a.create_export("vm1", 0.01).await;

    let mut client_a = server_a.connect("vm1").await;
    client_a.write(0, &vec![0xAA; BLOCK_SIZE]).await.unwrap();
    client_a.write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).await.unwrap();
    client_a.flush().await.unwrap();
    client_a.disconnect().await.unwrap();

    server_a.snapshot_export("vm1").await;
    server_a.shutdown().await;

    // --- Server B: fork readonly, promote, write 1 new block, drain, shutdown ---
    // Same db_path so server B can see server A's S3 data.
    let server_b = TestServer::start(Arc::clone(&ctx.object_store), "cold-mig-a").await;
    server_b.fork_export_readonly("vm1", "vm1", 0.01).await;
    server_b.promote_export("vm1").await;

    let mut client_b = server_b.connect("vm1").await;
    client_b.write(2 * BLOCK_SIZE as u64, &vec![0xCC; BLOCK_SIZE]).await.unwrap();
    client_b.flush().await.unwrap();
    client_b.disconnect().await.unwrap();

    server_b.drain_all().await;
    server_b.shutdown().await;

    // --- Server C: fresh node, restore from manifest, verify all 3 blocks ---
    let server_c = TestServer::start(Arc::clone(&ctx.object_store), "cold-mig-a").await;
    server_c.restore_export("vm1", 0.01).await;

    let mut client_c = server_c.connect("vm1").await;

    let expected: &[u8] = &[0xAA, 0xBB, 0xCC];
    for (i, &pattern) in expected.iter().enumerate() {
        let data = client_c.read(i as u64 * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            data.iter().all(|&b| b == pattern),
            "block {} after cold wake should be {:#x}, got {:#x}",
            i, pattern, data[0],
        );
    }

    client_c.disconnect().await.unwrap();
    server_c.shutdown().await;
}
