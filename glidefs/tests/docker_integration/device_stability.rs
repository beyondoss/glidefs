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
// Hang diagnostics
// ---------------------------------------------------------------------------

/// Run one phase of a kernel-device test with a deadline. A device teardown or
/// a read through a device whose daemon is gone blocks in the kernel with no
/// timeout of its own, so without this a stuck phase holds CI for the whole job
/// limit and leaves no trace of where it stuck. On expiry: name the phase, dump
/// every thread's kernel stack, and exit the process. `panic!` is not enough:
/// the runtime drop would wait forever on the same stuck blocking thread.
#[cfg(all(target_os = "linux", feature = "ublk"))]
async fn phase<T>(label: &str, fut: impl std::future::Future<Output = T>) -> T {
    const PHASE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
    match tokio::time::timeout(PHASE_DEADLINE, fut).await {
        Ok(v) => v,
        Err(_) => {
            eprintln!("HANG: phase '{label}' did not finish in {PHASE_DEADLINE:?}");
            dump_thread_stacks();
            std::process::exit(1);
        }
    }
}

/// Print comm, wchan and the kernel stack of every thread of this process
/// (`/proc/self/task/*`; the stack file needs root, which docker-tests runs as).
#[cfg(all(target_os = "linux", feature = "ublk"))]
fn dump_thread_stacks() {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return;
    };
    for task in tasks.flatten() {
        let dir = task.path();
        let read = |f: &str| {
            std::fs::read_to_string(dir.join(f))
                .unwrap_or_default()
                .trim_end()
                .to_string()
        };
        let stack = read("stack");
        let wchan = read("wchan");
        // Threads parked in userspace have an empty stack and no wchan: skip them.
        if stack.is_empty() && (wchan.is_empty() || wchan == "0") {
            continue;
        }
        eprintln!(
            "--- tid {} comm {} wchan {}\n{}",
            task.file_name().to_string_lossy(),
            read("comm"),
            wchan,
            stack
        );
    }
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

    let path1 = phase("first add_device", ublk1.add_device("stable-test", Arc::clone(&handler)))
        .await
        .expect("first registration should succeed");

    let map_path = cache_dir.path().join("ublk_devices.json");
    assert!(map_path.exists(), "ublk_devices.json must exist after add_device");

    // Write a recognizable pattern through the kernel device.
    let pattern: Vec<u8> = (0..BLOCK_SIZE).map(|i| (i % 251) as u8).collect();
    {
        let mut client = UblkClient::open(&path1, size).unwrap();
        phase("pre-crash write 0", client.write(0, &pattern)).await.unwrap();
        phase("pre-crash write 1", client.write(BLOCK_SIZE as u64, &pattern)).await.unwrap();
        phase("pre-crash flush", client.flush()).await.unwrap();

        // Sanity: read back before crash.
        let readback = phase("pre-crash read", client.read(0, BLOCK_SIZE as u32)).await.unwrap();
        assert_eq!(readback, pattern, "pre-crash read must match write");
    }

    // --- Phase 2: Simulate crash ---
    // `crash_remove` is the full graceful `unregister`: RemoveQueue acks, STOP_DEV,
    // cdev close, DEL_DEV. Each can block without a deadline, hence the phase.
    phase("crash_remove (unregister)", ublk1.crash_remove("stable-test"))
        .await
        .expect("crash_remove should succeed");
    drop(ublk1);

    assert!(map_path.exists(), "ublk_devices.json must survive crash");

    // --- Phase 3: Restart — new server, same cache dir ---
    let mut ublk2 = glidefs::block::ublk::UblkServer::new()
        .with_cache_dir(cache_dir.path().to_path_buf());

    let path2 = phase("second add_device", ublk2.add_device("stable-test", Arc::clone(&handler)))
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
        let block0 = phase("post-restart read 0", client.read(0, BLOCK_SIZE as u32)).await.unwrap();
        let block1 = phase("post-restart read 1", client.read(BLOCK_SIZE as u64, BLOCK_SIZE as u32))
            .await
            .unwrap();
        assert_eq!(block0, pattern, "block 0 data must survive crash");
        assert_eq!(block1, pattern, "block 1 data must survive crash");
    }

    phase("cleanup remove_device", ublk2.remove_device("stable-test"))
        .await
        .expect("cleanup remove should succeed");
    server.shutdown().await;
}
