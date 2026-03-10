//! Filesystem-level crash recovery tests.
//!
//! Mount ext4 on a GlideFS kernel block device, do filesystem work, crash the
//! server, restart with WAL recovery, and verify filesystem integrity via e2fsck
//! + data verification.
//!
//! Requirements: Linux, root privileges, nbd/ublk kernel modules, Docker (MinIO).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use tempfile::TempDir;

use crate::{TestContext, TestServer, Transport};

const EXPORT_SIZE_GB: f64 = 0.05; // 50MB — enough for ext4 + journal

// ---------------------------------------------------------------------------
// Shell helpers
// ---------------------------------------------------------------------------

fn is_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "0")
        .unwrap_or(false)
}

fn mkfs_ext4(dev: &Path) -> bool {
    Command::new("mkfs.ext4")
        .args(["-F", "-q", "-b", "4096"])
        .arg(dev)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn mount_ext4(dev: &Path, mountpoint: &Path, extra_opts: &[&str]) -> bool {
    let mut cmd = Command::new("mount");
    cmd.arg("-t").arg("ext4");
    if !extra_opts.is_empty() {
        cmd.arg("-o").arg(extra_opts.join(","));
    }
    cmd.arg(dev).arg(mountpoint);
    cmd.status().map(|s| s.success()).unwrap_or(false)
}

fn umount_lazy(mountpoint: &Path) -> bool {
    Command::new("umount")
        .arg("-l")
        .arg(mountpoint)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run e2fsck in repair mode (-y). Returns (clean, output).
/// Exit 0 = clean, 1 = errors corrected — both are acceptable.
fn e2fsck_repair(dev: &Path) -> (bool, String) {
    let output = Command::new("e2fsck")
        .args(["-f", "-y"])
        .arg(dev)
        .output()
        .expect("failed to run e2fsck");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let code = output.status.code().unwrap_or(99);
    (
        code <= 1,
        format!("exit={code}\nstdout: {stdout}\nstderr: {stderr}"),
    )
}

fn write_test_file(dir: &Path, name: &str, content: &[u8]) {
    std::fs::write(dir.join(name), content)
        .unwrap_or_else(|e| panic!("write {name}: {e}"));
}

fn fsync_file(path: &Path) {
    let f = std::fs::File::open(path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    f.sync_all()
        .unwrap_or_else(|e| panic!("fsync {}: {e}", path.display()));
}

fn fsync_dir(dir: &Path) {
    let f = std::fs::File::open(dir)
        .unwrap_or_else(|e| panic!("open dir {}: {e}", dir.display()));
    f.sync_all()
        .unwrap_or_else(|e| panic!("fsync dir {}: {e}", dir.display()));
}

fn read_test_file(dir: &Path, name: &str) -> Vec<u8> {
    std::fs::read(dir.join(name))
        .unwrap_or_else(|e| panic!("read {name}: {e}"))
}

// ---------------------------------------------------------------------------
// RAII mount guard — lazy-unmounts on drop to prevent leaked mounts
// ---------------------------------------------------------------------------

struct MountGuard {
    mountpoint: PathBuf,
    active: bool,
}

impl MountGuard {
    fn new(mountpoint: &Path) -> Self {
        Self {
            mountpoint: mountpoint.to_path_buf(),
            active: false,
        }
    }

    fn mount(&mut self, dev: &Path, extra_opts: &[&str]) {
        assert!(
            mount_ext4(dev, &self.mountpoint, extra_opts),
            "mount failed: {} -> {}",
            dev.display(),
            self.mountpoint.display(),
        );
        self.active = true;
    }

    fn unmount(&mut self) {
        if self.active {
            umount_lazy(&self.mountpoint);
            self.active = false;
        }
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        self.unmount();
    }
}

// ---------------------------------------------------------------------------
// NBD kernel device helpers
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
struct NbdKernelDevice {
    manager: glidefs::block::nbd::NbdDeviceManager,
    dev_path: PathBuf,
    export_name: String,
}

#[cfg(target_os = "linux")]
impl NbdKernelDevice {
    async fn register(
        _cache_dir: &Path,
        export_name: &str,
        router: &Arc<glidefs::block::router::ExportRouter>,
        device_size: u64,
    ) -> Self {
        // Don't use with_cache_dir() here — persisted device indices cause
        // collisions when multiple crash-recovery tests run in parallel.
        // Each test auto-assigns a fresh /dev/nbdN index via the kernel.
        let mut manager = glidefs::block::nbd::NbdDeviceManager::new();
        let dev_path = manager
            .add_device(export_name, Arc::clone(router), device_size)
            .await
            .unwrap_or_else(|e| panic!("add_device failed: {e}"));
        Self {
            manager,
            dev_path,
            export_name: export_name.to_string(),
        }
    }

    async fn crash(mut self) {
        self.manager
            .crash_disconnect(&self.export_name)
            .await
            .unwrap_or_else(|e| panic!("crash_disconnect failed: {e}"));
    }

    async fn remove(mut self) {
        self.manager
            .remove_device(&self.export_name)
            .await
            .unwrap_or_else(|e| panic!("remove_device failed: {e}"));
    }
}

// ---------------------------------------------------------------------------
// Test: fsync'd file must survive crash
// ---------------------------------------------------------------------------

// Mount ext4, write file, fsync, crash glidefs, restart, e2fsck, verify file.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn test_fs_crash_fsync_honored_nbd_kernel() {
    if !crate::nbd_kernel_available() || !is_root() {
        eprintln!("skipping: requires nbd module + root");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = TempDir::new().unwrap();
    let mountpoint = TempDir::new().unwrap();
    let mut mount = MountGuard::new(mountpoint.path());

    // --- Phase 1: write + fsync ---
    let server = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-fsync",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler = server.router.get_handler("vol1").await.unwrap();
    let size = handler.device_size();
    drop(handler);

    let dev = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server.router,
        size,
    )
    .await;

    assert!(mkfs_ext4(&dev.dev_path), "mkfs.ext4 failed");
    mount.mount(&dev.dev_path, &[]);

    let content = b"important data that MUST survive a crash";
    write_test_file(mountpoint.path(), "synced.txt", content);
    fsync_file(&mountpoint.path().join("synced.txt"));
    fsync_dir(mountpoint.path());

    // --- Phase 2: crash ---
    // Crash kernel device first (sockets close → kernel returns I/O errors).
    // Then unmount the dead mount. Then drop the server (simulates process crash).
    dev.crash().await;
    mount.unmount();
    server.shutdown.cancel();
    drop(server);

    // --- Phase 3: recover ---
    let server2 = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-fsync",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server2.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler2 = server2.router.get_handler("vol1").await.unwrap();
    let size2 = handler2.device_size();
    drop(handler2);

    let dev2 = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server2.router,
        size2,
    )
    .await;

    // e2fsck: journal replay + verify
    let (clean, output) = e2fsck_repair(&dev2.dev_path);
    assert!(clean, "e2fsck failed:\n{output}");

    // Remount and verify
    mount.mount(&dev2.dev_path, &[]);
    let recovered = read_test_file(mountpoint.path(), "synced.txt");
    assert_eq!(recovered, content, "fsync'd file must survive crash");

    mount.unmount();
    dev2.remove().await;
    server2.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test: unsynced write lost cleanly (no corruption)
// ---------------------------------------------------------------------------

// Write file A (fsync), write file B (no fsync), crash, verify e2fsck clean.
// File A must exist. File B may or may not exist, but no corruption.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn test_fs_crash_unsynced_write_lost_cleanly_nbd_kernel() {
    if !crate::nbd_kernel_available() || !is_root() {
        eprintln!("skipping: requires nbd module + root");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = TempDir::new().unwrap();
    let mountpoint = TempDir::new().unwrap();
    let mut mount = MountGuard::new(mountpoint.path());

    let server = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-unsync",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler = server.router.get_handler("vol1").await.unwrap();
    let size = handler.device_size();
    drop(handler);

    let dev = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server.router,
        size,
    )
    .await;

    assert!(mkfs_ext4(&dev.dev_path));
    mount.mount(&dev.dev_path, &[]);

    // File A: fsync'd — must survive
    let content_a = b"synced file content";
    write_test_file(mountpoint.path(), "fileA.txt", content_a);
    fsync_file(&mountpoint.path().join("fileA.txt"));
    fsync_dir(mountpoint.path());

    // File B: NOT fsync'd — may or may not survive, but filesystem must be clean
    write_test_file(mountpoint.path(), "fileB.txt", b"unsynced content");
    // deliberately no fsync

    // Crash
    dev.crash().await;
    mount.unmount();
    server.shutdown.cancel();
    drop(server);

    // Recover
    let server2 = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-unsync",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server2.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler2 = server2.router.get_handler("vol1").await.unwrap();
    let size2 = handler2.device_size();
    drop(handler2);

    let dev2 = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server2.router,
        size2,
    )
    .await;

    // e2fsck must report clean filesystem (errors corrected is OK)
    let (clean, output) = e2fsck_repair(&dev2.dev_path);
    assert!(clean, "e2fsck should be clean after crash:\n{output}");

    // Mount and verify: file A must exist, file B is optional
    mount.mount(&dev2.dev_path, &[]);
    let recovered_a = read_test_file(mountpoint.path(), "fileA.txt");
    assert_eq!(recovered_a, content_a, "fsync'd file A must survive");

    // File B may or may not exist — both are correct.
    // What matters is that the filesystem is consistent (e2fsck passed).
    let b_exists = mountpoint.path().join("fileB.txt").exists();
    eprintln!(
        "fileB.txt after crash: {}",
        if b_exists { "survived" } else { "lost (expected)" }
    );

    mount.unmount();
    dev2.remove().await;
    server2.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test: journal replay with data=journal mode
// ---------------------------------------------------------------------------

// Mount with data=journal, write many files, crash mid-write, e2fsck clean.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn test_fs_crash_journal_replay_nbd_kernel() {
    if !crate::nbd_kernel_available() || !is_root() {
        eprintln!("skipping: requires nbd module + root");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = TempDir::new().unwrap();
    let mountpoint = TempDir::new().unwrap();
    let mut mount = MountGuard::new(mountpoint.path());

    let server = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-journal",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler = server.router.get_handler("vol1").await.unwrap();
    let size = handler.device_size();
    drop(handler);

    let dev = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server.router,
        size,
    )
    .await;

    assert!(mkfs_ext4(&dev.dev_path));
    mount.mount(&dev.dev_path, &["data=journal"]);

    // Write many files to stress the journal
    for i in 0..50 {
        let content: Vec<u8> = (0..4096).map(|b| ((i + b) % 251) as u8).collect();
        write_test_file(mountpoint.path(), &format!("file_{i:03}.dat"), &content);
    }
    // Fsync a subset — the rest are in-flight
    for i in 0..25 {
        fsync_file(&mountpoint.path().join(format!("file_{i:03}.dat")));
    }
    fsync_dir(mountpoint.path());

    // Crash mid-write (files 25-49 may not be durable)
    dev.crash().await;
    mount.unmount();
    server.shutdown.cancel();
    drop(server);

    // Recover
    let server2 = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-journal",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server2.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler2 = server2.router.get_handler("vol1").await.unwrap();
    let size2 = handler2.device_size();
    drop(handler2);

    let dev2 = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server2.router,
        size2,
    )
    .await;

    // e2fsck must be clean — journal replay should fix everything
    let (clean, output) = e2fsck_repair(&dev2.dev_path);
    assert!(clean, "e2fsck should be clean after journal replay:\n{output}");

    // Mount and verify fsync'd files
    mount.mount(&dev2.dev_path, &[]);
    for i in 0..25 {
        let name = format!("file_{i:03}.dat");
        let expected: Vec<u8> = (0..4096).map(|b| ((i + b) % 251) as u8).collect();
        let actual = read_test_file(mountpoint.path(), &name);
        assert_eq!(actual, expected, "fsync'd {name} must survive crash");
    }

    mount.unmount();
    dev2.remove().await;
    server2.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test: repeated crash recovery (5 cycles)
// ---------------------------------------------------------------------------

// Mount, write, crash, recover — 5 cycles. No cumulative corruption.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn test_fs_repeated_crash_recovery_nbd_kernel() {
    if !crate::nbd_kernel_available() || !is_root() {
        eprintln!("skipping: requires nbd module + root");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = TempDir::new().unwrap();
    let mountpoint = TempDir::new().unwrap();

    // Format filesystem on first iteration
    {
        let server = TestServer::start_with_cache_dir(
            Arc::clone(&ctx.object_store),
            "fs-crash-repeat",
            Transport::Nbd,
            cache_dir.path().to_path_buf(),
        )
        .await;
        server.create_export("vol1", EXPORT_SIZE_GB).await;

        let handler = server.router.get_handler("vol1").await.unwrap();
        let size = handler.device_size();
        drop(handler);

        let dev = NbdKernelDevice::register(
            cache_dir.path(),
            "vol1",
            &server.router,
            size,
        )
        .await;

        assert!(mkfs_ext4(&dev.dev_path), "initial mkfs.ext4 failed");
        dev.remove().await;
        server.shutdown().await;
    }

    // 5 crash-recovery cycles
    for cycle in 0..5u32 {
        let mut mount = MountGuard::new(mountpoint.path());

        // Start server, recover WAL
        let server = TestServer::start_with_cache_dir(
            Arc::clone(&ctx.object_store),
            "fs-crash-repeat",
            Transport::Nbd,
            cache_dir.path().to_path_buf(),
        )
        .await;
        server.create_export("vol1", EXPORT_SIZE_GB).await;

        let handler = server.router.get_handler("vol1").await.unwrap();
        let size = handler.device_size();
        drop(handler);

        let dev = NbdKernelDevice::register(
            cache_dir.path(),
            "vol1",
            &server.router,
            size,
        )
        .await;

        // e2fsck before mount (except first cycle where we just formatted)
        if cycle > 0 {
            let (clean, output) = e2fsck_repair(&dev.dev_path);
            assert!(
                clean,
                "e2fsck failed at cycle {cycle} before mount:\n{output}"
            );
        }

        mount.mount(&dev.dev_path, &[]);

        // Verify previous cycles' files survived
        for prev in 0..cycle {
            let name = format!("cycle_{prev}.txt");
            let expected = format!("data from cycle {prev}");
            let actual = read_test_file(mountpoint.path(), &name);
            assert_eq!(
                actual,
                expected.as_bytes(),
                "cycle {prev} file missing at cycle {cycle}"
            );
        }

        // Write this cycle's file + fsync
        let name = format!("cycle_{cycle}.txt");
        let content = format!("data from cycle {cycle}");
        write_test_file(mountpoint.path(), &name, content.as_bytes());
        fsync_file(&mountpoint.path().join(&name));
        fsync_dir(mountpoint.path());

        // Crash
        dev.crash().await;
        mount.unmount();
        server.shutdown.cancel();
        drop(server);
    }

    // Final verification: recover and check all 5 files
    let server = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-repeat",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler = server.router.get_handler("vol1").await.unwrap();
    let size = handler.device_size();
    drop(handler);

    let dev = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server.router,
        size,
    )
    .await;

    let (clean, output) = e2fsck_repair(&dev.dev_path);
    assert!(clean, "final e2fsck failed:\n{output}");

    let mut mount = MountGuard::new(mountpoint.path());
    mount.mount(&dev.dev_path, &[]);

    for cycle in 0..5u32 {
        let name = format!("cycle_{cycle}.txt");
        let expected = format!("data from cycle {cycle}");
        let actual = read_test_file(mountpoint.path(), &name);
        assert_eq!(
            actual,
            expected.as_bytes(),
            "cycle {cycle} file missing in final verification"
        );
    }

    mount.unmount();
    dev.remove().await;
    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test: crash during flush to S3 — filesystem survives on local SSD
// ---------------------------------------------------------------------------

// Write files, trigger drain, crash mid-drain, restart, filesystem intact,
// re-drain pushes everything to S3.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn test_fs_crash_during_flush_to_s3_nbd_kernel() {
    if !crate::nbd_kernel_available() || !is_root() {
        eprintln!("skipping: requires nbd module + root");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = TempDir::new().unwrap();
    let mountpoint = TempDir::new().unwrap();
    let mut mount = MountGuard::new(mountpoint.path());

    // Phase 1: write + fsync + start drain
    let server = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-drain",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler = server.router.get_handler("vol1").await.unwrap();
    let size = handler.device_size();
    drop(handler);

    let dev = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server.router,
        size,
    )
    .await;

    assert!(mkfs_ext4(&dev.dev_path));
    mount.mount(&dev.dev_path, &[]);

    // Write several files
    let mut expected_files = Vec::new();
    for i in 0..20 {
        let name = format!("drain_file_{i:02}.dat");
        let content: Vec<u8> = vec![(i + 0x40) as u8; 4096];
        write_test_file(mountpoint.path(), &name, &content);
        fsync_file(&mountpoint.path().join(&name));
        expected_files.push((name, content));
    }
    fsync_dir(mountpoint.path());

    // Unmount cleanly so filesystem is consistent, then we'll crash mid-drain
    mount.unmount();

    // Start drain but crash before it completes (drop server)
    let _drain = tokio::spawn({
        let router = Arc::clone(&server.router);
        async move {
            let _ = router.drain_all().await;
        }
    });

    // Brief delay then crash (drain may or may not complete — doesn't matter)
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    dev.crash().await;
    server.shutdown.cancel();
    drop(server);

    // Phase 2: recover — data must be on local SSD regardless of S3 state
    let server2 = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-drain",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server2.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler2 = server2.router.get_handler("vol1").await.unwrap();
    let size2 = handler2.device_size();
    drop(handler2);

    let dev2 = NbdKernelDevice::register(
        cache_dir.path(),
        "vol1",
        &server2.router,
        size2,
    )
    .await;

    let (clean, output) = e2fsck_repair(&dev2.dev_path);
    assert!(clean, "e2fsck failed after drain crash:\n{output}");

    mount.mount(&dev2.dev_path, &[]);
    for (name, content) in &expected_files {
        let actual = read_test_file(mountpoint.path(), name);
        assert_eq!(&actual, content, "{name} must survive drain crash");
    }

    mount.unmount();

    // Re-drain should succeed now
    server2.drain_all().await;

    dev2.remove().await;
    server2.shutdown().await;
}

// ---------------------------------------------------------------------------
// ublk variant: fsync honored
// ---------------------------------------------------------------------------

#[cfg(all(target_os = "linux", feature = "ublk"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_fs_crash_fsync_honored_ublk() {
    if !crate::ublk_available() || !is_root() {
        eprintln!("skipping: requires ublk_drv + root");
        return;
    }

    let ctx = TestContext::new().await;
    let cache_dir = TempDir::new().unwrap();
    let mountpoint = TempDir::new().unwrap();
    let mut mount = MountGuard::new(mountpoint.path());

    // --- Phase 1: write + fsync ---
    let server = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-ublk",
        Transport::Nbd, // NBD for server transport; ublk for kernel device
        cache_dir.path().to_path_buf(),
    )
    .await;
    server.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler = server
        .router
        .get_handler("vol1")
        .await
        .expect("handler must exist");
    let size = handler.device_size();

    let mut ublk = glidefs::block::ublk::UblkServer::new()
        .with_cache_dir(cache_dir.path().to_path_buf());
    let dev_path = ublk
        .add_device("vol1", Arc::clone(&handler))
        .await
        .expect("ublk add_device");
    drop(handler);

    assert!(mkfs_ext4(&dev_path), "mkfs.ext4 failed on ublk device");
    mount.mount(&dev_path, &[]);

    let content = b"ublk crash recovery test data";
    write_test_file(mountpoint.path(), "synced.txt", content);
    fsync_file(&mountpoint.path().join("synced.txt"));
    fsync_dir(mountpoint.path());

    // --- Phase 2: crash ---
    // Simulate crash by tearing down without draining to S3.
    // Data survives on SSD via WAL; the ublk teardown order doesn't affect
    // the SSD/WAL recovery path being tested.
    mount.unmount();
    if let Err(e) = ublk.shutdown().await {
        eprintln!("ublk shutdown: {e}");
    }
    server.shutdown.cancel();
    drop(server);

    // --- Phase 3: recover ---
    let server2 = TestServer::start_with_cache_dir(
        Arc::clone(&ctx.object_store),
        "fs-crash-ublk",
        Transport::Nbd,
        cache_dir.path().to_path_buf(),
    )
    .await;
    server2.create_export("vol1", EXPORT_SIZE_GB).await;

    let handler2 = server2
        .router
        .get_handler("vol1")
        .await
        .expect("handler must exist");

    let mut ublk2 = glidefs::block::ublk::UblkServer::new()
        .with_cache_dir(cache_dir.path().to_path_buf());
    let dev_path2 = ublk2
        .add_device("vol1", Arc::clone(&handler2))
        .await
        .expect("ublk add_device (recover)");
    drop(handler2);

    let (clean, output) = e2fsck_repair(&dev_path2);
    assert!(clean, "e2fsck failed on ublk after crash:\n{output}");

    mount.mount(&dev_path2, &[]);
    let recovered = read_test_file(mountpoint.path(), "synced.txt");
    assert_eq!(recovered, content, "fsync'd file must survive ublk crash");

    mount.unmount();
    ublk2
        .remove_device("vol1")
        .await
        .expect("ublk cleanup remove");
    server2.shutdown().await;
}
