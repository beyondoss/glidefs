use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path as ObjPath, ObjectStore};

use glidefs::config::ExportConfig;

use crate::TestContext;
use crate::TestServer;

const BLOCK_SIZE: usize = 128 * 1024;

transport_test! {
    async fn test_corrupt_pack_detected(transport) {
        let ctx = TestContext::new().await;
        let db_path = "corrupt-pack";

        // Phase 1: Write two blocks with distinct patterns, drain to S3, shutdown.
        let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;
        client
            .write(0, &vec![0xAA; BLOCK_SIZE])
            .await
            .unwrap();
        client
            .write(BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE])
            .await
            .unwrap();
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        server.drain_all().await;
        server.shutdown().await;

        // Phase 2: List packs in S3 and corrupt the first one.
        // Packs are stored at {db_path}/exports/{export_name}/packs/{prefix}/{uuid}
        let prefix = ObjPath::from(format!("{}/exports/vol1/packs/", db_path));
        let mut stream = ctx.object_store.list(Some(&prefix));
        let mut pack_paths = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.unwrap();
            pack_paths.push(meta.location);
        }
        assert!(!pack_paths.is_empty(), "expected at least one pack in S3");

        // Corrupt ALL packs to ensure we hit the corruption regardless of which
        // pack each block is in.
        for path in &pack_paths {
            ctx.object_store
                .put(path, Bytes::from(vec![0xDE, 0xAD]).into())
                .await
                .unwrap();
        }

        // Verify the corruption stuck by reading the raw object back
        let corrupted = ctx.object_store.get(&pack_paths[0]).await.unwrap();
        let corrupted_bytes = corrupted.bytes().await.unwrap();
        assert_eq!(
            &corrupted_bytes[..],
            &[0xDE, 0xAD],
            "pack should be corrupted in S3"
        );

        // Phase 3: Fresh server, restore from manifest.
        let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server2.restore_export("vol1", 0.01).await;

        let mut client2 = server2.connect("vol1").await;

        // Phase 4: Read both blocks. At least one should fail due to BLAKE3 hash mismatch
        // or decompression failure from the corrupted pack.
        let r0 = client2.read(0, BLOCK_SIZE as u32).await;
        let r1 = client2.read(BLOCK_SIZE as u64, BLOCK_SIZE as u32).await;

        assert!(
            r0.is_err() || r1.is_err(),
            "at least one read should fail after pack corruption, but r0={:?}, r1={:?}",
            r0.as_ref().map(|d| d.len()),
            r1.as_ref().map(|d| d.len()),
        );

        server2.shutdown().await;
    }
}

transport_test! {
    async fn test_corrupt_manifest_rejected(transport) {
        let ctx = TestContext::new().await;
        let db_path = "corrupt-manifest";

        // Phase 1: Write data, drain, shutdown.
        let server = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;
        server.create_export("vol1", 0.01).await;

        let mut client = server.connect("vol1").await;
        client
            .write(0, &vec![0xCC; BLOCK_SIZE])
            .await
            .unwrap();
        client.flush().await.unwrap();
        client.disconnect().await.unwrap();

        server.drain_all().await;
        server.shutdown().await;

        // Phase 2: Overwrite the manifest with garbage.
        // Manifest stored at {db_path}/exports/{export_name}/manifests/{export_name}
        let manifest_path = ObjPath::from(format!("{}/exports/vol1/manifests/vol1", db_path));
        ctx.object_store
            .put(&manifest_path, Bytes::from(vec![0xBA, 0xAD]).into())
            .await
            .unwrap();

        // Phase 3: Fresh server — attempt to restore should fail cleanly.
        let server2 = TestServer::start(Arc::clone(&ctx.object_store), db_path, transport).await;

        let config = ExportConfig {
            name: "vol1".to_string(),
            size_gb: 0.01,
            s3_prefix: None,
            block_size: None,
            blocks_per_pack: None,
            flush_mode: None,
            transport: None,
        };
        let result = server2
            .router
            .create_export(config, false, Some("vol1"))
            .await;

        assert!(result.is_err(), "corrupt manifest should be rejected");

        server2.shutdown().await;
    }
}
