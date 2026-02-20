use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

#[tokio::test]
async fn test_multiple_exports_isolation() {
    let ctx = TestContext::new().await;
    let server = TestServer::start(Arc::clone(&ctx.object_store), "multi-export").await;

    server.create_export("vol-a", 0.01).await;
    server.create_export("vol-b", 0.01).await;

    let mut client_a = server.connect("vol-a").await;
    let mut client_b = server.connect("vol-b").await;

    // Write different data to each export
    client_a.write(0, &[0x11; 4096]).await.unwrap();
    client_b.write(0, &[0x22; 4096]).await.unwrap();
    client_a.flush().await.unwrap();
    client_b.flush().await.unwrap();

    // Read back and verify isolation
    let data_a = client_a.read(0, 4096).await.unwrap();
    let data_b = client_b.read(0, 4096).await.unwrap();
    assert!(data_a.iter().all(|&b| b == 0x11), "vol-a should have 0x11");
    assert!(data_b.iter().all(|&b| b == 0x22), "vol-b should have 0x22");

    // Verify vol-a didn't leak into vol-b
    let data_a_offset = client_a.read(4096, 4096).await.unwrap();
    let data_b_offset = client_b.read(4096, 4096).await.unwrap();
    assert!(data_a_offset.iter().all(|&b| b == 0), "vol-a unwritten should be zeros");
    assert!(data_b_offset.iter().all(|&b| b == 0), "vol-b unwritten should be zeros");

    client_a.disconnect().await.unwrap();
    client_b.disconnect().await.unwrap();
    server.shutdown().await;
}
