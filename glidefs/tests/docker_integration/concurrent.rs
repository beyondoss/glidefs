use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

transport_test! {
    async fn test_concurrent_clients_same_export(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "concurrent-clients", transport).await;
        server.create_export("vol1", 0.01).await;

        let info = server.connect_info("vol1").await;
        let fill_bytes: [u8; 4] = [0xA0, 0xB0, 0xC0, 0xD0];

        let mut handles = Vec::new();
        for client_idx in 0..4u64 {
            let fill = fill_bytes[client_idx as usize];
            let info = info.clone();
            let handle = tokio::spawn(async move {
                let mut client = info.connect().await.unwrap();

                // Each client writes 3 blocks at non-overlapping offsets
                let base_block = client_idx * 3;
                for b in 0..3 {
                    let offset = (base_block + b) * BLOCK_SIZE as u64;
                    client.write(offset, &vec![fill; BLOCK_SIZE]).await.unwrap();
                }
                client.flush().await.unwrap();
                client.disconnect().await.unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Single reader verifies all 12 blocks
        let mut reader = server.connect("vol1").await;
        for client_idx in 0..4u64 {
            let expected = fill_bytes[client_idx as usize];
            let base_block = client_idx * 3;
            for b in 0..3 {
                let offset = (base_block + b) * BLOCK_SIZE as u64;
                let data = reader.read(offset, BLOCK_SIZE as u32).await.unwrap();
                assert!(
                    data.iter().all(|&byte| byte == expected),
                    "block {} (client {}) should contain {:#x}",
                    base_block + b,
                    client_idx,
                    expected,
                );
            }
        }

        reader.disconnect().await.unwrap();
        server.shutdown().await;
    }
}

transport_test! {
    async fn test_drain_during_active_writes(transport) {
        let ctx = TestContext::new().await;
        let db_path = "drain-active-writes";
        let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server.create_export("vol1", 0.01).await;

        let info = server.connect_info("vol1").await;
        let router = Arc::clone(&server.router);

        // Writer: blocks 0-4, then signal, then blocks 5-9
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let writer_handle = tokio::spawn(async move {
            let mut client = info.connect().await.unwrap();

            // Phase 1: write blocks 0-4
            for i in 0..5u64 {
                let offset = i * BLOCK_SIZE as u64;
                client
                    .write(offset, &vec![(i + 1) as u8; BLOCK_SIZE])
                    .await
                    .unwrap();
                client.flush().await.unwrap();
            }

            // Signal that first batch is done so drain can start
            tx.send(()).unwrap();

            // Phase 2: write blocks 5-9 (concurrent with drain)
            for i in 5..10u64 {
                let offset = i * BLOCK_SIZE as u64;
                client
                    .write(offset, &vec![(i + 1) as u8; BLOCK_SIZE])
                    .await
                    .unwrap();
                client.flush().await.unwrap();
            }

            client.disconnect().await.unwrap();
        });

        // Drain: wait for first batch, then drain concurrently with second batch
        let drain_router = Arc::clone(&router);
        let drain_handle = tokio::spawn(async move {
            rx.await.unwrap();
            let failed = drain_router.drain_all().await;
            assert!(failed.is_empty(), "drain_all failed for: {:?}", failed);
        });

        writer_handle.await.unwrap();
        drain_handle.await.unwrap();

        // Verify all 10 blocks are correct on the live server
        let mut reader = server.connect("vol1").await;
        for i in 0..10u64 {
            let data = reader
                .read(i * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            assert!(
                data.iter().all(|&b| b == (i + 1) as u8),
                "block {} should contain {:#x} before restart",
                i,
                i + 1,
            );
        }
        reader.disconnect().await.unwrap();

        // Second drain catches any blocks written after the first drain
        server.drain_all().await;
        server.shutdown().await;

        // Fresh server: restore and verify all 10 blocks survived
        let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server2.restore_export("vol1", 0.01).await;

        let mut reader2 = server2.connect("vol1").await;
        for i in 0..10u64 {
            let data = reader2
                .read(i * BLOCK_SIZE as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            assert!(
                data.iter().all(|&b| b == (i + 1) as u8),
                "block {} should contain {:#x} after restart",
                i,
                i + 1,
            );
        }

        reader2.disconnect().await.unwrap();
        server2.shutdown().await;
    }
}
