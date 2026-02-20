use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

// Resize preserves pre-resize data, grows the device, and data in the
// extended region survives a full drain-shutdown-restore cycle.
//
// `resize_export` drains to S3, removes the export, then recreates it at
// the new size loading from the manifest so pre-resize data is accessible.
transport_test! {
    async fn test_resize_preserves_data_and_extends(transport) {
        let ctx = TestContext::new().await;
        let db_path = "resize-test";

        // --- Phase 1: Create a small export and write data ---
        let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;
        let old_size = client.export_size();

        client.write(0, &vec![0x11; BLOCK_SIZE]).await.unwrap();
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        // --- Phase 2: Resize to double the capacity ---
        server.resize_export("vol1", 0.02).await;

        // Reconnect — the client must reconnect to observe the new size.
        let mut client = server.connect("vol1").await;
        let new_size = client.export_size();
        assert!(
            new_size > old_size,
            "export_size should grow after resize: old={old_size} new={new_size}"
        );

        // Pre-resize data should still be readable (resize loads manifest)
        let block0 = client.read(0, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            block0.iter().all(|&b| b == 0x11),
            "pre-resize data should survive resize"
        );

        // --- Phase 3: Write data in the extended region ---
        let extended_offset = old_size;
        client
            .write(extended_offset, &vec![0x33; BLOCK_SIZE])
            .await
            .unwrap();
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        // --- Phase 4: Drain + shutdown ---
        server.drain_all().await;
        server.shutdown().await;

        // --- Phase 5: Fresh server, restore and verify everything survived ---
        let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server2.restore_export("vol1", 0.02).await;

        let mut client = server2.connect("vol1").await;
        assert_eq!(
            client.export_size(), new_size,
            "restored export should match the resized capacity"
        );

        let block0 = client.read(0, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            block0.iter().all(|&b| b == 0x11),
            "pre-resize data should survive drain + restore"
        );

        let extended_block = client
            .read(extended_offset, BLOCK_SIZE as u32)
            .await
            .unwrap();
        assert!(
            extended_block.iter().all(|&b| b == 0x33),
            "data in extended region should survive drain + restore"
        );

        client.disconnect().await.unwrap();
        server2.shutdown().await;
    }
}
