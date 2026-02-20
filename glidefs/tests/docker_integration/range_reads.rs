use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

// Sub-block and cross-block reads through the full block I/O path.
//
// Writes two adjacent blocks with distinct patterns, then reads:
// - A sub-block slice from the middle of block 0
// - A cross-block slice spanning the boundary between blocks 0 and 1
// - A tiny read (1 byte) at the exact block boundary
//
// All reads go through the live server path (SSD-backed, not yet drained).
transport_test! {
    async fn test_sub_block_and_cross_block_reads(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "range-reads", transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;

        // Write block 0 = 0xAA, block 1 = 0xBB
        client.write(0, &vec![0xAA; BLOCK_SIZE]).await.unwrap();
        client
            .write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE])
            .await
            .unwrap();
        client.flush().await.unwrap();

        // --- Sub-block read: 512 bytes from the middle of block 0 ---
        let mid_offset = (BLOCK_SIZE / 2) as u64;
        let sub = client.read(mid_offset, 512).await.unwrap();
        assert_eq!(sub.len(), 512);
        assert!(
            sub.iter().all(|&b| b == 0xAA),
            "sub-block read from middle of block 0 should be 0xAA"
        );

        // --- Cross-block read: last 256 bytes of block 0 + first 256 bytes of block 1 ---
        let cross_offset = (BLOCK_SIZE - 256) as u64;
        let cross = client.read(cross_offset, 512).await.unwrap();
        assert_eq!(cross.len(), 512);
        assert!(
            cross[..256].iter().all(|&b| b == 0xAA),
            "first half of cross-block read should be 0xAA (end of block 0)"
        );
        assert!(
            cross[256..].iter().all(|&b| b == 0xBB),
            "second half of cross-block read should be 0xBB (start of block 1)"
        );

        // --- Tiny read: 1 byte at the exact block boundary ---
        let boundary = client.read(BLOCK_SIZE as u64, 1).await.unwrap();
        assert_eq!(boundary, vec![0xBB]);

        // --- Tiny read: 1 byte just before the boundary ---
        let before_boundary = client
            .read(BLOCK_SIZE as u64 - 1, 1)
            .await
            .unwrap();
        assert_eq!(before_boundary, vec![0xAA]);

        client.disconnect().await.unwrap();
        server.shutdown().await;
    }
}

// Same range reads but after drain + cold wake, so every read goes through
// the full S3 fetch -> LZ4 decompress -> BLAKE3 verify pipeline.
transport_test! {
    async fn test_range_reads_after_cold_wake(transport) {
        let ctx = TestContext::new().await;
        let db_path = "range-cold";

        // Phase 1: write two blocks, drain, shutdown.
        let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;
        client.write(0, &vec![0x11; BLOCK_SIZE]).await.unwrap();
        client
            .write(BLOCK_SIZE as u64, &vec![0x22; BLOCK_SIZE])
            .await
            .unwrap();
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        server.drain_all().await;
        server.shutdown().await;

        // Phase 2: fresh server, restore from manifest — no local SSD data.
        let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server2.restore_export("vol1", 0.01).await;

        let mut client2 = server2.connect("vol1").await;

        // Sub-block: 1KB from offset 1000 within block 0
        let sub = client2.read(1000, 1024).await.unwrap();
        assert_eq!(sub.len(), 1024);
        assert!(
            sub.iter().all(|&b| b == 0x11),
            "sub-block cold read should return correct data from S3"
        );

        // Cross-block: spans block 0 -> block 1
        let cross_offset = (BLOCK_SIZE - 512) as u64;
        let cross = client2.read(cross_offset, 1024).await.unwrap();
        assert_eq!(cross.len(), 1024);
        assert!(
            cross[..512].iter().all(|&b| b == 0x11),
            "cross-block cold read: first half from block 0"
        );
        assert!(
            cross[512..].iter().all(|&b| b == 0x22),
            "cross-block cold read: second half from block 1"
        );

        // Multi-block: read both full blocks in one request (256KB)
        let multi = client2.read(0, (BLOCK_SIZE * 2) as u32).await.unwrap();
        assert_eq!(multi.len(), BLOCK_SIZE * 2);
        assert!(
            multi[..BLOCK_SIZE].iter().all(|&b| b == 0x11),
            "multi-block read: first block"
        );
        assert!(
            multi[BLOCK_SIZE..].iter().all(|&b| b == 0x22),
            "multi-block read: second block"
        );

        client2.disconnect().await.unwrap();
        server2.shutdown().await;
    }
}
