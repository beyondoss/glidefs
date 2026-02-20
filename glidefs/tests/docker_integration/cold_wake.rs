use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

#[tokio::test]
async fn test_cold_wake_from_different_node() {
    let ctx = TestContext::new().await;
    let db_path = "wake-test";

    // --- Server A: write data, drain, shutdown ---
    let server_a = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server_a.create_export("vol1", 0.01).await;

    let mut client_a = server_a.connect("vol1").await;

    let pattern: Vec<u8> = (0..BLOCK_SIZE).map(|i| ((i * 7 + 13) % 256) as u8).collect();
    client_a.write(0, &pattern).await.unwrap();
    client_a.flush().await.unwrap();
    client_a.disconnect().await.unwrap();

    server_a.drain_all().await;
    server_a.shutdown().await;

    // --- Server B: completely fresh node, different cache dir, same S3 ---
    let server_b = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server_b.restore_export("vol1", 0.01).await;

    let mut client_b = server_b.connect("vol1").await;

    let read_data = client_b.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert_eq!(read_data, pattern, "data should be readable from cold node via S3");

    client_b.disconnect().await.unwrap();
    server_b.shutdown().await;
}

#[tokio::test]
async fn test_cold_wake_multiple_blocks() {
    let ctx = TestContext::new().await;
    let db_path = "wake-multi";

    // Write multiple blocks with distinct patterns
    let server_a = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server_a.create_export("vol1", 0.01).await;

    let mut client_a = server_a.connect("vol1").await;

    for i in 0..8u8 {
        let data = vec![i + 1; BLOCK_SIZE];
        client_a.write((i as u64) * BLOCK_SIZE as u64, &data).await.unwrap();
    }
    client_a.flush().await.unwrap();
    client_a.disconnect().await.unwrap();

    server_a.drain_all().await;
    server_a.shutdown().await;

    // Cold reader verifies all blocks
    let server_b = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server_b.restore_export("vol1", 0.01).await;

    let mut client_b = server_b.connect("vol1").await;

    for i in 0..8u8 {
        let data = client_b
            .read((i as u64) * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert!(
            data.iter().all(|&b| b == i + 1),
            "block {} should contain pattern {:#x}",
            i,
            i + 1
        );
    }

    client_b.disconnect().await.unwrap();
    server_b.shutdown().await;
}
