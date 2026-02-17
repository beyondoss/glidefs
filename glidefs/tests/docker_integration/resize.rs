use std::sync::Arc;

use crate::nbd_client::NbdClient;
use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

/// Resize grows the device and I/O to the extended region works.
///
/// `resize_export` internally drains to S3, removes the export (keeping cache
/// files), and recreates it at the new size with an empty block_map (`None`
/// manifest). This means data written *before* resize is not in the post-resize
/// block_map—it lives in the pre-resize S3 manifest. A subsequent drain
/// uploads a new manifest containing only post-resize writes.
///
/// This test validates:
///   1. The reported device size grows after resize.
///   2. I/O to the extended region works.
///   3. Post-resize data survives drain + cold-wake restore.
#[tokio::test]
async fn test_resize_grows_and_extends() {
    let ctx = TestContext::new().await;
    let db_path = "resize-test";

    // --- Phase 1: Create a small export ---
    let server = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server.create_export("vol1", 0.01).await;

    let mut client = NbdClient::connect(server.addr, "vol1").await.unwrap();
    let old_size = client.export_size;
    client.disconnect().await.unwrap();

    // --- Phase 2: Resize to double the capacity ---
    server.resize_export("vol1", 0.02).await;

    // Reconnect — the client must reconnect to observe the new size.
    let mut client = NbdClient::connect(server.addr, "vol1").await.unwrap();
    let new_size = client.export_size;
    assert!(
        new_size > old_size,
        "export_size should grow after resize: old={old_size} new={new_size}"
    );

    // --- Phase 3: Write data in the extended region ---
    let extended_offset = old_size; // first byte of the new region
    client
        .write(extended_offset, &vec![0x33; BLOCK_SIZE])
        .await
        .unwrap();
    // Also write to the original region (block 0) after resize
    client.write(0, &vec![0x11; BLOCK_SIZE]).await.unwrap();
    client.flush().await.unwrap();

    // Verify reads work on the live server
    let block0 = client.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        block0.iter().all(|&b| b == 0x11),
        "block 0 should be readable on live server"
    );
    let ext = client.read(extended_offset, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        ext.iter().all(|&b| b == 0x33),
        "extended block should be readable on live server"
    );
    client.disconnect().await.unwrap();

    // --- Phase 4: Drain + shutdown ---
    server.router.drain_all().await.unwrap();
    server.shutdown().await;

    // --- Phase 5: Fresh server, restore from S3 manifest at the new size ---
    let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path).await;
    server2.restore_export("vol1", 0.02).await;

    let mut client = NbdClient::connect(server2.addr, "vol1").await.unwrap();
    assert_eq!(
        client.export_size, new_size,
        "restored export should match the resized capacity"
    );

    // Both blocks (written after resize) should survive drain + restore.
    let block0 = client.read(0, BLOCK_SIZE as u32).await.unwrap();
    assert!(
        block0.iter().all(|&b| b == 0x11),
        "block 0 written after resize should survive drain + restore"
    );

    let extended_block = client
        .read(extended_offset, BLOCK_SIZE as u32)
        .await
        .unwrap();
    assert!(
        extended_block.iter().all(|&b| b == 0x33),
        "block in extended region should survive drain + restore"
    );

    client.disconnect().await.unwrap();
    server2.shutdown().await;
}
