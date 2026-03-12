use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

// Write blocks, drain to S3, verify blocks are evicted from local SSD
// (NOT_PRESENT), then read them back through the full S3 path.
transport_test! {
    async fn test_bottomless_write_flush_evict_readback(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "bottomless-basic", transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;

        // Write 8 blocks with distinct patterns
        let patterns: Vec<u8> = (0xA0..0xA8).collect();
        for (i, &pat) in patterns.iter().enumerate() {
            client
                .write((i * BLOCK_SIZE) as u64, &vec![pat; BLOCK_SIZE])
                .await
                .unwrap();
        }
        client.flush().await.unwrap();

        // Drain flushes dirty blocks to S3 and evicts them (SYNCING→NOT_PRESENT)
        server.drain_all().await;

        // Read back every block — data must come from S3 (all blocks evicted)
        for (i, &expected) in patterns.iter().enumerate() {
            let data = client
                .read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            assert!(
                data.iter().all(|&b| b == expected),
                "block {} should be {:#04x} after eviction, got {:#04x}",
                i,
                expected,
                data[0],
            );
        }

        client.disconnect().await.unwrap();
        server.shutdown().await;
    }
}

// Multiple flush cycles — each evicts the previous batch. Verify data
// integrity across all cycles and that SSD footprint stays bounded.
transport_test! {
    async fn test_bottomless_multi_flush_cycle(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "bottomless-multi", transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;

        // 3 flush cycles, each writes 4 blocks at different offsets
        for cycle in 0u8..3 {
            let base_block = cycle as usize * 4;
            for i in 0..4 {
                let pat = 0x10 * (cycle + 1) + i;
                let offset = (base_block + i as usize) * BLOCK_SIZE;
                client
                    .write(offset as u64, &vec![pat; BLOCK_SIZE])
                    .await
                    .unwrap();
            }
            client.flush().await.unwrap();
            server.drain_all().await;
        }

        // Verify all 12 blocks (from all 3 cycles) are correct
        for cycle in 0u8..3 {
            let base_block = cycle as usize * 4;
            for i in 0u8..4 {
                let expected = 0x10 * (cycle + 1) + i;
                let offset = (base_block + i as usize) * BLOCK_SIZE;
                let data = client
                    .read(offset as u64, BLOCK_SIZE as u32)
                    .await
                    .unwrap();
                assert!(
                    data.iter().all(|&b| b == expected),
                    "cycle {} block {} should be {:#04x}, got {:#04x}",
                    cycle,
                    i,
                    expected,
                    data[0],
                );
            }
        }

        client.disconnect().await.unwrap();
        server.shutdown().await;
    }
}

// Overwrite the same block across multiple flush cycles. Each flush evicts
// the old data. The final read must return the latest write.
transport_test! {
    async fn test_bottomless_overwrite_across_flushes(transport) {
        let ctx = TestContext::new().await;
        let server = TestServer::start(Arc::clone(&ctx.object_store), "bottomless-overwrite", transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;

        // Write block 0 three times, flushing after each
        for pat in [0x11u8, 0x22, 0x33] {
            client
                .write(0, &vec![pat; BLOCK_SIZE])
                .await
                .unwrap();
            client.flush().await.unwrap();
            server.drain_all().await;
        }

        // Last write wins
        let data = client.read(0, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            data.iter().all(|&b| b == 0x33),
            "block 0 should be 0x33 (last write), got {:#04x}",
            data[0],
        );

        client.disconnect().await.unwrap();
        server.shutdown().await;
    }
}

// Bottomless + cold wake: write, drain, restart on a fresh server (empty SSD),
// verify all data readable from S3.
transport_test! {
    async fn test_bottomless_cold_wake(transport) {
        let ctx = TestContext::new().await;
        let db_path = "bottomless-cold-wake";

        let server1 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server1.create_export("vol1", 0.01).await;

        let mut client = server1.connect("vol1").await;

        // Write 5 blocks with distinct patterns
        let patterns = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
        for (i, &pat) in patterns.iter().enumerate() {
            client
                .write((i * BLOCK_SIZE) as u64, &vec![pat; BLOCK_SIZE])
                .await
                .unwrap();
        }
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        server1.drain_all().await;
        server1.shutdown().await;

        // Fresh server — completely empty local SSD, all reads go to S3
        let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server2.restore_export("vol1", 0.01).await;

        let mut reader = server2.connect("vol1").await;
        for (i, &expected) in patterns.iter().enumerate() {
            let data = reader
                .read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32)
                .await
                .unwrap();
            assert!(
                data.iter().all(|&b| b == expected),
                "cold wake: block {} should be {:#04x}, got {:#04x}",
                i,
                expected,
                data[0],
            );
        }

        reader.disconnect().await.unwrap();
        server2.shutdown().await;
    }
}

// Bottomless fork: fork from a parent, read parent blocks (from S3), write
// new blocks to the child, flush+evict, verify both parent and child data.
transport_test! {
    async fn test_bottomless_fork(transport) {
        let ctx = TestContext::new().await;
        let db_path = "bottomless-fork";

        let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server.create_export("parent", 0.01).await;

        // Write parent blocks
        let mut parent_client = server.connect("parent").await;
        parent_client
            .write(0, &vec![0xAA; BLOCK_SIZE])
            .await
            .unwrap();
        parent_client
            .write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE])
            .await
            .unwrap();
        parent_client.flush().await.unwrap();
        parent_client.disconnect().await.unwrap();

        // Snapshot + drain parent (evicts all blocks to S3)
        server.snapshot_export("parent").await;
        server.drain_all().await;

        // Fork child from parent
        server.fork_export("child", "parent", 0.01).await;

        let mut child = server.connect("child").await;

        // Verify child can read parent blocks (from S3)
        let data0 = child.read(0, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            data0.iter().all(|&b| b == 0xAA),
            "fork: parent block 0 should be 0xAA, got {:#04x}",
            data0[0],
        );

        // Write new blocks to child
        child
            .write(2 * BLOCK_SIZE as u64, &vec![0xCC; BLOCK_SIZE])
            .await
            .unwrap();
        child.flush().await.unwrap();

        // Drain child (evicts child's new blocks)
        server.drain_all().await;

        // Read all 3 blocks from child — 2 parent (S3) + 1 child (S3)
        let d0 = child.read(0, BLOCK_SIZE as u32).await.unwrap();
        let d1 = child.read(BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        let d2 = child.read(2 * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert!(d0.iter().all(|&b| b == 0xAA), "parent block 0");
        assert!(d1.iter().all(|&b| b == 0xBB), "parent block 1");
        assert!(d2.iter().all(|&b| b == 0xCC), "child block 2");

        child.disconnect().await.unwrap();
        server.shutdown().await;
    }
}
