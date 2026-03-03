//! Prove that device paths are stable across crash+restart for both NBD and ublk.
//!
//! These tests exercise the full crash recovery flow:
//! 1. Register a device, record the assigned path
//! 2. Kill the kernel device via crash_disconnect/crash_remove (map preserved)
//! 3. New device manager from the same cache dir (simulates process restart)
//! 4. Register again — assert same device path
//!
//! No manual file writes. The persisted map is written by add_device() in step 1
//! and read back by the new manager in step 3. The crash helpers kill the kernel
//! device without touching the map — exactly what happens in a real crash.

use std::sync::Arc;

use crate::{TestContext, TestServer, Transport};

// ---------------------------------------------------------------------------
// NBD kernel device stability
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_nbd_kernel_device_index_stable_after_crash() {
    if !crate::nbd_kernel_available() {
        eprintln!("skipping: nbd module not loaded or insufficient privileges");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = tempfile::TempDir::new().unwrap();

    // Stand up a server with an export (uses plain NBD transport for the protocol server).
    let server = TestServer::start(Arc::clone(&ctx.object_store), "nbd-stable", Transport::Nbd).await;
    server.create_export("stable-test", 0.01).await;

    let handler = server
        .router
        .get_handler("stable-test")
        .await
        .expect("handler must exist");
    let size = handler.device_size();

    // --- Phase 1: Register device, record path ---
    let mut manager1 = glidefs::block::nbd::NbdDeviceManager::new()
        .with_cache_dir(cache_dir.path().to_path_buf());

    let path1 = manager1
        .add_device("stable-test", Arc::clone(&server.router), size)
        .await
        .expect("first registration should succeed");

    // Verify the persisted map was written by add_device().
    let map_path = cache_dir.path().join("nbd_devices.json");
    assert!(map_path.exists(), "nbd_devices.json must exist after add_device");

    // --- Phase 2: Simulate crash ---
    // Kill the kernel device but leave nbd_devices.json intact.
    // This is what happens in a real crash: process dies, dead_conn_timeout
    // expires, device is gone, but the file on disk survives.
    manager1
        .crash_disconnect("stable-test")
        .await
        .expect("crash_disconnect should succeed");
    drop(manager1);

    // The map file must still exist (crash_disconnect does not touch it).
    assert!(map_path.exists(), "nbd_devices.json must survive crash");

    // --- Phase 3: Restart — new manager, same cache dir ---
    // Simulates a new glidefs process starting with the same local storage.
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

    // Clean up kernel device.
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
async fn test_ublk_device_index_stable_after_crash() {
    if !crate::ublk_available() {
        eprintln!("skipping: ublk_drv not available or insufficient privileges");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = tempfile::TempDir::new().unwrap();

    // Stand up a server with an export.
    let server = TestServer::start(Arc::clone(&ctx.object_store), "ublk-stable", Transport::Nbd).await;
    server.create_export("stable-test", 0.01).await;

    let handler = server
        .router
        .get_handler("stable-test")
        .await
        .expect("handler must exist");

    // --- Phase 1: Register device, record path ---
    let mut ublk1 = glidefs::block::ublk::UblkServer::new()
        .with_cache_dir(cache_dir.path().to_path_buf());

    let path1 = ublk1
        .add_device("stable-test", Arc::clone(&handler))
        .await
        .expect("first registration should succeed");

    // Verify the persisted map was written by add_device().
    let map_path = cache_dir.path().join("ublk_devices.json");
    assert!(map_path.exists(), "ublk_devices.json must exist after add_device");

    // --- Phase 2: Simulate crash ---
    // Kill the kernel device but leave ublk_devices.json intact.
    ublk1
        .crash_remove("stable-test")
        .await
        .expect("crash_remove should succeed");
    drop(ublk1);

    // The map file must still exist.
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

    // Clean up kernel device.
    ublk2
        .remove_device("stable-test")
        .await
        .expect("cleanup remove should succeed");
    server.shutdown().await;
}
