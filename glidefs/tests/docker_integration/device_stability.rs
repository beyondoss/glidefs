//! Prove that device paths AND data are stable across crash+restart for both
//! NBD and ublk.
//!
//! Each test:
//! 1. Register a kernel device, write a known data pattern through it
//! 2. Kill the kernel device via crash_disconnect/crash_remove (map preserved)
//! 3. New device manager from the same cache dir (simulates process restart)
//! 4. Register again — assert same device path
//! 5. Read through the new device — assert data matches
//!
//! No manual file writes. The persisted map is written by add_device() in step 1
//! and read back by the new manager in step 3.

use std::sync::Arc;

use crate::{TestContext, TestServer, Transport};

const BLOCK_SIZE: usize = 128 * 1024;

// ---------------------------------------------------------------------------
// NBD kernel device stability
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn test_nbd_kernel_device_stable_after_crash() {
    use crate::nbd_kernel_client::NbdKernelClient;

    if !crate::nbd_kernel_available() {
        eprintln!("skipping: nbd module not loaded or insufficient privileges");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = tempfile::TempDir::new().unwrap();

    let server = TestServer::start(Arc::clone(&ctx.object_store), "nbd-stable", Transport::Nbd).await;
    server.create_export("stable-test", 0.01).await;

    let handler = server
        .router
        .get_handler("stable-test")
        .await
        .expect("handler must exist");
    let size = handler.device_size();

    // --- Phase 1: Register device, write data ---
    let mut manager1 = glidefs::block::nbd::NbdDeviceManager::new()
        .with_cache_dir(cache_dir.path().to_path_buf());

    let path1 = manager1
        .add_device("stable-test", Arc::clone(&server.router), size)
        .await
        .expect("first registration should succeed");

    let map_path = cache_dir.path().join("nbd_devices.json");
    assert!(map_path.exists(), "nbd_devices.json must exist after add_device");

    // Write a recognizable pattern through the kernel device.
    let pattern: Vec<u8> = (0..BLOCK_SIZE).map(|i| (i % 251) as u8).collect();
    {
        let mut client = NbdKernelClient::open(&path1, size).unwrap();
        client.write(0, &pattern).await.unwrap();
        client.write(BLOCK_SIZE as u64, &pattern).await.unwrap();
        client.flush().await.unwrap();

        // Sanity: read back before crash.
        let readback = client.read(0, BLOCK_SIZE as u32).await.unwrap();
        assert_eq!(readback, pattern, "pre-crash read must match write");
    }

    // --- Phase 2: Simulate crash ---
    manager1
        .crash_disconnect("stable-test")
        .await
        .expect("crash_disconnect should succeed");
    drop(manager1);

    assert!(map_path.exists(), "nbd_devices.json must survive crash");

    // --- Phase 3: Restart — new manager, same cache dir ---
    let mut manager2 = glidefs::block::nbd::NbdDeviceManager::new()
        .with_cache_dir(cache_dir.path().to_path_buf());

    let path2 = manager2
        .add_device("stable-test", Arc::clone(&server.router), size)
        .await
        .expect("second registration should succeed");

    assert_eq!(
        path1, path2,
        "NBD device path must be stable across crash: {} != {}",
        path1.display(),
        path2.display()
    );

    // Read through the recovered device — data must survive.
    {
        let mut client = NbdKernelClient::open(&path2, size).unwrap();
        let block0 = client.read(0, BLOCK_SIZE as u32).await.unwrap();
        let block1 = client.read(BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert_eq!(block0, pattern, "block 0 data must survive crash");
        assert_eq!(block1, pattern, "block 1 data must survive crash");
    }

    manager2
        .remove_device("stable-test")
        .await
        .expect("cleanup remove should succeed");
    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// ublk device stability
// ---------------------------------------------------------------------------

#[cfg(all(target_os = "linux", feature = "ublk"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ublk_device_stable_after_crash() {
    use crate::ublk_client::UblkClient;

    if !crate::ublk_available() {
        eprintln!("skipping: ublk_drv not available or insufficient privileges");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = tempfile::TempDir::new().unwrap();

    let server = TestServer::start(Arc::clone(&ctx.object_store), "ublk-stable", Transport::Nbd).await;
    server.create_export("stable-test", 0.01).await;

    let handler = server
        .router
        .get_handler("stable-test")
        .await
        .expect("handler must exist");
    let size = handler.device_size();

    // --- Phase 1: Register device, write data ---
    let mut ublk1 = glidefs::block::ublk::UblkServer::new()
        .with_cache_dir(cache_dir.path().to_path_buf());

    let path1 = ublk1
        .add_device("stable-test", Arc::clone(&handler))
        .await
        .expect("first registration should succeed");

    let map_path = cache_dir.path().join("ublk_devices.json");
    assert!(map_path.exists(), "ublk_devices.json must exist after add_device");

    // Write a recognizable pattern through the kernel device.
    let pattern: Vec<u8> = (0..BLOCK_SIZE).map(|i| (i % 251) as u8).collect();
    {
        let mut client = UblkClient::open(&path1, size).unwrap();
        client.write(0, &pattern).await.unwrap();
        client.write(BLOCK_SIZE as u64, &pattern).await.unwrap();
        client.flush().await.unwrap();

        // Sanity: read back before crash.
        let readback = client.read(0, BLOCK_SIZE as u32).await.unwrap();
        assert_eq!(readback, pattern, "pre-crash read must match write");
    }

    // --- Phase 2: Simulate crash ---
    ublk1
        .crash_remove("stable-test")
        .await
        .expect("crash_remove should succeed");
    drop(ublk1);

    assert!(map_path.exists(), "ublk_devices.json must survive crash");

    // --- Phase 3: Restart — new server, same cache dir ---
    let mut ublk2 = glidefs::block::ublk::UblkServer::new()
        .with_cache_dir(cache_dir.path().to_path_buf());

    let path2 = ublk2
        .add_device("stable-test", Arc::clone(&handler))
        .await
        .expect("second registration should succeed");

    assert_eq!(
        path1, path2,
        "ublk device path must be stable across crash: {} != {}",
        path1.display(),
        path2.display()
    );

    // Read through the recovered device — data must survive.
    {
        let mut client = UblkClient::open(&path2, size).unwrap();
        let block0 = client.read(0, BLOCK_SIZE as u32).await.unwrap();
        let block1 = client.read(BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert_eq!(block0, pattern, "block 0 data must survive crash");
        assert_eq!(block1, pattern, "block 1 data must survive crash");
    }

    ublk2
        .remove_device("stable-test")
        .await
        .expect("cleanup remove should succeed");
    server.shutdown().await;
}
