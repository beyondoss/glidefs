//! Transport stress tests.
//!
//! Tests rapid client connect/disconnect, high concurrency, and
//! connection handling edge cases at the protocol level.
//!
//! Run with: `cargo test --features docker-tests --test docker_integration transport_stress`

use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

// =============================================================================
// Client storm — 100 rapid connect/disconnect cycles
// =============================================================================

transport_test! {
    async fn test_client_storm(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "client-storm", transport).await;
        server.create_export("vol1", 0.01).await;

        // Write a block so reads have data
        let mut setup = server.connect("vol1").await;
        setup.write(0, &vec![0xAA; BLOCK_SIZE]).await.unwrap();
        setup.flush().await.unwrap();
        setup.disconnect().await.unwrap();

        let info = server.connect_info("vol1").await;

        // 100 clients: connect, read one block, disconnect
        let mut handles = Vec::new();
        for _ in 0..100u64 {
            let info = info.clone();
            handles.push(tokio::spawn(async move {
                let mut client = info.connect().await.unwrap();
                let data = client.read(0, BLOCK_SIZE as u32).await.unwrap();
                assert!(
                    data.iter().all(|&b| b == 0xAA),
                    "storm client read unexpected data"
                );
                client.disconnect().await.unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Server should still be healthy — one more read to verify
        let mut verify = server.connect("vol1").await;
        let data = verify.read(0, BLOCK_SIZE as u32).await.unwrap();
        assert!(data.iter().all(|&b| b == 0xAA));
        verify.disconnect().await.unwrap();

        server.shutdown().await;
    }
}

// =============================================================================
// Request backpressure — many concurrent requests from one client
// =============================================================================

transport_test! {
    async fn test_request_backpressure(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "backpressure", transport).await;
        server.create_export("vol1", 0.10).await;  // 100MB

        // Write 20 blocks
        let mut writer = server.connect("vol1").await;
        for i in 0..20u64 {
            writer.write(i * BLOCK_SIZE as u64, &vec![(i as u8) + 1; BLOCK_SIZE]).await.unwrap();
        }
        writer.flush().await.unwrap();
        writer.disconnect().await.unwrap();

        let info = server.connect_info("vol1").await;

        // 50 concurrent readers each reading different blocks
        let mut handles = Vec::new();
        for reader_id in 0..50u64 {
            let info = info.clone();
            handles.push(tokio::spawn(async move {
                let mut client = info.connect().await.unwrap();
                let block = reader_id % 20;
                let data = client.read(block * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
                assert!(
                    data.iter().all(|&b| b == (block as u8) + 1),
                    "reader {} block {} data mismatch",
                    reader_id,
                    block
                );
                client.disconnect().await.unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        server.shutdown().await;
    }
}

// =============================================================================
// [NBD-only] Connection close during inflight request
// =============================================================================

/// Write request in-flight, client drops connection. Server should cancel
/// gracefully — no crash, no task leak. Follow-up operations work.
#[tokio::test]
async fn test_connection_close_during_inflight() {
    let ctx = TestContext::new().await;
    let server = TestServer::start(
        Arc::clone(&ctx.object_store),
        "close-inflight",
        crate::Transport::Nbd,
    )
    .await;
    server.create_export("vol1", 0.01).await;

    // Client 1: connect, write, then drop without disconnect (simulates crash)
    {
        let mut client = server.connect("vol1").await;
        client.write(0, &vec![0xAA; BLOCK_SIZE]).await.unwrap();
        // Drop without disconnect — the TCP connection will be reset
    }

    // Small delay for server to process the disconnection
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Client 2: should connect and operate normally
    let mut client2 = server.connect("vol1").await;
    client2
        .write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE])
        .await
        .unwrap();
    client2.flush().await.unwrap();

    let data = client2.read(BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        data.iter().all(|&b| b == 0xBB),
        "second client should work after first client dropped"
    );

    client2.disconnect().await.unwrap();
    server.shutdown().await;
}

// =============================================================================
// [NBD-only] Rapid reconnect after disconnect
// =============================================================================

/// Rapidly disconnect and reconnect 20 times. Server should handle cleanly.
#[tokio::test]
async fn test_rapid_reconnect() {
    let ctx = TestContext::new().await;
    let server = TestServer::start(
        Arc::clone(&ctx.object_store),
        "rapid-reconnect",
        crate::Transport::Nbd,
    )
    .await;
    server.create_export("vol1", 0.01).await;

    for i in 0..20u64 {
        let mut client = server.connect("vol1").await;
        let fill = (i as u8) + 1;
        client.write(0, &vec![fill; BLOCK_SIZE]).await.unwrap();
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();
    }

    // Final verification: last write should win
    let mut verify = server.connect("vol1").await;
    let data = verify.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        data.iter().all(|&b| b == 20),
        "last write (fill=20) should be visible"
    );
    verify.disconnect().await.unwrap();

    server.shutdown().await;
}
