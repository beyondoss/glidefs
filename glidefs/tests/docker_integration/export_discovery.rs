use std::sync::Arc;

use crate::TestContext;
use crate::TestServer;

transport_test! {
    async fn test_export_discovery_from_s3(transport) {
        let ctx = TestContext::new().await;
        let db_path = "discovery-test";

        // --- Server 1: create export, save to S3, write data, drain ---
        let server1 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server1.create_export("dynamic-vol", 0.01).await;
        server1.save_export("dynamic-vol", 0.01).await;

        let mut client = server1.connect("dynamic-vol").await;
        client.write(0, &[0xDD; 4096]).await.unwrap();
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        server1.drain_all().await;
        server1.shutdown().await;

        // --- Server 2: no static exports, discover from S3 ---
        let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;

        let discovered = server2.router.discover_exports().await.unwrap();
        assert!(
            !discovered.is_empty(),
            "should discover at least one export from S3"
        );

        let vol = discovered.iter().find(|e| e.name == "dynamic-vol");
        assert!(vol.is_some(), "should discover 'dynamic-vol'");

        // Create the discovered export, restoring from its manifest
        for config in discovered {
            let manifest_name = config.name.clone();
            server2
                .router
                .create_export(config, false, Some(&manifest_name))
                .await
                .unwrap();
            // Register ublk device for discovered exports (no-op for NBD).
            server2.register_ublk_device(&manifest_name).await;
        }

        // Verify data is readable
        let mut client2 = server2.connect("dynamic-vol").await;
        let read = client2.read(0, 4096).await.unwrap();
        assert_eq!(&read[..], &[0xDD; 4096], "discovered export should have original data");

        client2.disconnect().await.unwrap();
        server2.shutdown().await;
    }
}
