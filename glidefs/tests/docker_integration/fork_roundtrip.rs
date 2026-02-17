use std::sync::Arc;

use crate::nbd_client::NbdClient;
use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

/// Fork a child from a snapshotted parent, write new blocks to the child,
/// drain everything, restart on a fresh server, and verify all 6 blocks
/// (4 inherited from parent + 2 written directly to child) survive the
/// cold-wake round-trip.
#[tokio::test]
async fn test_fork_flush_cold_wake() {
    let ctx = TestContext::new().await;

    // --- Phase 1: create parent, write 4 blocks, snapshot ---
    let server = TestServer::start(Arc::clone(&ctx.object_store), "fork-cold-wake").await;
    server.create_export("parent", 0.01).await;

    let mut client = NbdClient::connect(server.addr, "parent").await.unwrap();
    let patterns: [u8; 4] = [0xAA, 0xBB, 0xCC, 0xDD];
    for (i, &pat) in patterns.iter().enumerate() {
        client
            .write((i * BLOCK_SIZE) as u64, &vec![pat; BLOCK_SIZE])
            .await
            .unwrap();
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    server.snapshot_export("parent").await;

    // --- Phase 2: fork child from parent, write 2 new blocks ---
    server
        .fork_export("child", "parent", 0.01)
        .await;

    let mut child_client = NbdClient::connect(server.addr, "child").await.unwrap();
    let fork_patterns: [u8; 2] = [0xEE, 0xFF];
    for (i, &pat) in fork_patterns.iter().enumerate() {
        let offset = ((4 + i) * BLOCK_SIZE) as u64;
        child_client
            .write(offset, &vec![pat; BLOCK_SIZE])
            .await
            .unwrap();
    }
    child_client.flush().await.unwrap();
    child_client.disconnect().await.unwrap();

    // --- Phase 3: drain child, shutdown ---
    server.router.drain_all().await.unwrap();
    server.shutdown().await;

    // --- Phase 4: fresh server, restore child, verify all 6 blocks ---
    // Same db_path so packs and manifests are visible in S3.
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), "fork-cold-wake").await;
    server2.restore_export("child", 0.01).await;

    let mut reader = NbdClient::connect(server2.addr, "child").await.unwrap();

    let all_patterns: [u8; 6] = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF];
    for (i, &expected) in all_patterns.iter().enumerate() {
        let data = reader
            .read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == expected),
            "block {} should be {:#04x}, got first byte {:#04x}",
            i,
            expected,
            data[0],
        );
    }

    reader.disconnect().await.unwrap();
    server2.shutdown().await;
}

/// Fork a child from a parent, then mutate both independently. Verify that
/// writes to the child do not affect the parent and vice versa -- both
/// online (hot reads) and after a cold-wake restore of the child.
#[tokio::test]
async fn test_fork_independent_of_parent() {
    let ctx = TestContext::new().await;

    // --- Phase 1: create parent, write block 0, snapshot ---
    let server = TestServer::start(Arc::clone(&ctx.object_store), "fork-isolation").await;
    server.create_export("parent", 0.01).await;

    let mut parent_client = NbdClient::connect(server.addr, "parent").await.unwrap();
    parent_client
        .write(0, &vec![0x11; BLOCK_SIZE])
        .await
        .unwrap();
    parent_client.flush().await.unwrap();

    server.snapshot_export("parent").await;

    // --- Phase 2: fork child (read-write) ---
    server
        .fork_export("child", "parent", 0.01)
        .await;

    let mut child_client = NbdClient::connect(server.addr, "child").await.unwrap();

    // --- Phase 3: overwrite block 0 on child, then on parent ---
    child_client
        .write(0, &vec![0x99; BLOCK_SIZE])
        .await
        .unwrap();
    child_client.flush().await.unwrap();

    parent_client
        .write(0, &vec![0x22; BLOCK_SIZE])
        .await
        .unwrap();
    parent_client.flush().await.unwrap();

    // --- Phase 4: verify isolation while both are live ---
    let child_data = child_client.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        child_data.iter().all(|&b| b == 0x99),
        "child block 0 should be 0x99, got {:#04x}",
        child_data[0],
    );

    let parent_data = parent_client.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        parent_data.iter().all(|&b| b == 0x22),
        "parent block 0 should be 0x22, got {:#04x}",
        parent_data[0],
    );

    child_client.disconnect().await.unwrap();
    parent_client.disconnect().await.unwrap();

    // --- Phase 5: drain, shutdown, cold-wake child, re-verify ---
    server.router.drain_all().await.unwrap();
    server.shutdown().await;

    // Same db_path so packs and manifests are visible in S3.
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), "fork-isolation").await;
    server2.restore_export("child", 0.01).await;

    let mut reader = NbdClient::connect(server2.addr, "child").await.unwrap();
    let restored = reader.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        restored.iter().all(|&b| b == 0x99),
        "restored child block 0 should be 0x99, got {:#04x}",
        restored[0],
    );

    reader.disconnect().await.unwrap();
    server2.shutdown().await;
}
