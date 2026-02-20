use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: u32 = 128 * 1024;

transport_test! {
    async fn test_write_read_roundtrip(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "roundtrip", transport).await;
        server.create_export("vol1", 0.01).await; // 10MB

        let mut client = server.connect("vol1").await;

        // Write a known pattern
        let data: Vec<u8> = (0..BLOCK_SIZE as usize).map(|i| (i % 256) as u8).collect();
        client.write(0, &data).await.unwrap();
        client.flush().await.unwrap();

        // Read back and verify
        let read_data = client.read(0, BLOCK_SIZE).await.unwrap();
        assert_eq!(read_data, data);

        client.disconnect().await.unwrap();
        server.shutdown().await;
    }
}

transport_test! {
    async fn test_unwritten_blocks_return_zeros(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "zeros", transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;

        let data = client.read(512 * 1024, 4096).await.unwrap();
        assert!(data.iter().all(|&b| b == 0), "unwritten blocks should be zeros");

        client.disconnect().await.unwrap();
        server.shutdown().await;
    }
}

transport_test! {
    async fn test_last_write_wins(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "last-write", transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;

        // Write block, then overwrite
        client.write(0, &vec![0x11; BLOCK_SIZE as usize]).await.unwrap();
        client.write(0, &vec![0x22; BLOCK_SIZE as usize]).await.unwrap();
        client.flush().await.unwrap();

        let data = client.read(0, BLOCK_SIZE).await.unwrap();
        assert!(data.iter().all(|&b| b == 0x22), "last write should win");

        client.disconnect().await.unwrap();
        server.shutdown().await;
    }
}
