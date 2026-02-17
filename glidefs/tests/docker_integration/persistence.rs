use std::sync::Arc;

use crate::nbd_client::NbdClient;
use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

#[tokio::test]
async fn test_persistence_through_restart() {
    let ctx = TestContext::new().await;
    let db_path = "persist-test";

    // --- Phase 1: Write data, drain, shutdown ---
    let server1 = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server1.create_export("vol1", 0.01).await;

    let mut client = NbdClient::connect(server1.addr, "vol1").await.unwrap();

    let block_data: Vec<Vec<u8>> = (0..5)
        .map(|i| vec![(i + 1) as u8; BLOCK_SIZE])
        .collect();

    for (i, data) in block_data.iter().enumerate() {
        client.write((i * BLOCK_SIZE) as u64, data).await.unwrap();
    }
    client.flush().await.unwrap();
    client.disconnect().await.unwrap();

    server1.drain_all().await;
    server1.shutdown().await;

    // --- Phase 2: New server (fresh cache), same S3 path, restore from manifest ---
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server2.restore_export("vol1", 0.01).await;

    let mut client2 = NbdClient::connect(server2.addr, "vol1").await.unwrap();

    for (i, expected) in block_data.iter().enumerate() {
        let read = client2
            .read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert_eq!(&read, expected, "block {} data mismatch after restart", i);
    }

    client2.disconnect().await.unwrap();
    server2.shutdown().await;
}

#[tokio::test]
async fn test_overwrite_survives_restart() {
    let ctx = TestContext::new().await;
    let db_path = "overwrite-persist";

    // Write initial data
    let server1 = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server1.create_export("vol1", 0.01).await;

    let mut client = NbdClient::connect(server1.addr, "vol1").await.unwrap();
    client.write(0, &vec![0x11; BLOCK_SIZE]).await.unwrap();
    client.flush().await.unwrap();

    // Drain first batch
    server1.drain_all().await;

    // Overwrite
    let mut client2 = NbdClient::connect(server1.addr, "vol1").await.unwrap();
    client2.write(0, &vec![0xFF; BLOCK_SIZE]).await.unwrap();
    client2.flush().await.unwrap();

    // Drain again with new data
    server1.drain_all().await;

    client.disconnect().await.unwrap();
    client2.disconnect().await.unwrap();
    server1.shutdown().await;

    // Restart and verify the overwrite survived
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server2.restore_export("vol1", 0.01).await;

    let mut reader = NbdClient::connect(server2.addr, "vol1").await.unwrap();
    let data = reader.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        data.iter().all(|&b| b == 0xFF),
        "overwritten data should survive restart"
    );

    reader.disconnect().await.unwrap();
    server2.shutdown().await;
}
