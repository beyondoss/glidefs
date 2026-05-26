//! End-to-end smoke test for kernel zero-copy I/O via `UBLK_F_AUTO_BUF_REG`.
//!
//! Exercises [`ublk_core::zc::run_zc_queue`] against a real ublk kernel
//! device with a memfd-backed target. The kernel auto-registers each bio
//! at the io_uring sparse-buffer slot we name in the FETCH's
//! `ublk_auto_buf_reg`; our worker submits `READ_FIXED` / `WRITE_FIXED`
//! against the memfd with `buf_index=tag`; the kernel DMAs directly
//! between the bio pages and the memfd. No userspace memcpy of bio data.
//!
//! Skips when `/dev/ublk-control` is absent (no kernel ublk driver), when
//! the process isn't root (cdev is 0600), or when the kernel doesn't
//! advertise `UBLK_F_AUTO_BUF_REG + UBLK_F_SUPPORT_ZERO_COPY` via
//! `UBLK_CMD_GET_FEATURES` (the auto-detect leaves the dev flags alone,
//! the test would just exercise copy-mode).

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use ublk_core::ctrl::UblkCtrlBuilder;
use ublk_core::io::UblkDev;
use ublk_core::sys;
use ublk_core::zc::{
    run_zc_queue, ZcAction, ZcChunk, ZcChunkOp, ZcDispatch, ZcQueueHandle, ZcTarget,
};
use ublk_core::UblkFlags;

const DEV_SIZE: u64 = 1 << 20;
const NR_QUEUES: u16 = 1;
const QUEUE_DEPTH: u16 = 4;
const MAX_IO_BUF_BYTES: u32 = 64 * 1024;

fn ublk_runnable() -> bool {
    if !Path::new("/dev/ublk-control").exists() {
        return false;
    }
    unsafe { libc::geteuid() == 0 }
}

struct MemfdTarget {
    fd: RawFd,
}

impl ZcTarget for MemfdTarget {
    fn dispatch(
        &self,
        op: u8,
        offset: u64,
        length: u32,
        _tag: u16,
        _handle: &ZcQueueHandle,
    ) -> ZcDispatch {
        let action = match u32::from(op) {
            sys::UBLK_IO_OP_READ => ZcAction::Chunks(vec![ZcChunk {
                op: ZcChunkOp::ReadFixed { fd: self.fd, src_offset: offset },
                buf_offset: 0,
                length,
            }]),
            sys::UBLK_IO_OP_WRITE => ZcAction::Chunks(vec![ZcChunk {
                op: ZcChunkOp::WriteFixed { fd: self.fd, dst_offset: offset },
                buf_offset: 0,
                length,
            }]),
            sys::UBLK_IO_OP_FLUSH => ZcAction::Complete(0),
            _ => ZcAction::Complete(-libc::EIO),
        };
        ZcDispatch::Inline { action, keepalive: None }
    }
}

#[test]
fn zero_copy_roundtrip() {
    if !ublk_runnable() {
        eprintln!("skip: /dev/ublk-control absent or not root");
        return;
    }

    let _ = env_logger::builder().is_test(true).try_init();

    let ctrl = UblkCtrlBuilder::default()
        .name("zc-rt")
        .nr_queues(NR_QUEUES)
        .depth(QUEUE_DEPTH)
        .io_buf_bytes(MAX_IO_BUF_BYTES)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV | UblkFlags::UBLK_DEV_F_PREFER_ZERO_COPY)
        .build()
        .expect("ublk add_dev");

    let dev_info = ctrl.dev_info();
    let zc_on = (dev_info.flags & u64::from(sys::UBLK_F_AUTO_BUF_REG)) != 0
        && (dev_info.flags & u64::from(sys::UBLK_F_SUPPORT_ZERO_COPY)) != 0;
    eprintln!(
        "kernel features={:#x} dev_info.flags={:#x} zc_on={}",
        ctrl.get_driver_features().unwrap_or(0),
        dev_info.flags,
        zc_on
    );
    if !zc_on {
        eprintln!("skip: kernel does not advertise AUTO_BUF_REG + SUPPORT_ZERO_COPY");
        return;
    }

    let dev = UblkDev::new(
        "zc-rt".into(),
        |dev| {
            dev.set_default_params(DEV_SIZE);
            Ok(())
        },
        &ctrl,
    )
    .expect("UblkDev::new");

    ctrl.set_params(&dev.tgt.params).expect("set_params");

    let bdev_path = ctrl.get_bdev_path();
    let cdev_fd: RawFd = dev.tgt.fds[0];

    // Memfd-backed storage.
    let backing_fd: RawFd = unsafe {
        let name = b"zc-rt-backing\0";
        libc::syscall(
            libc::SYS_memfd_create,
            name.as_ptr() as *const libc::c_char,
            0u32,
        ) as RawFd
    };
    assert!(backing_fd >= 0, "memfd_create");
    assert!(unsafe { libc::ftruncate(backing_fd, DEV_SIZE as i64) } == 0, "ftruncate");
    let _backing_owner = unsafe { File::from_raw_fd(backing_fd) };

    let target: Arc<dyn ZcTarget> = Arc::new(MemfdTarget { fd: backing_fd });
    let stop = Arc::new(AtomicBool::new(false));
    // Eventfd ownership moved to the caller in this revision; in this
    // smoke test we create + leak (the worker breaks via kernel ABORT
    // and the test process exits soon after).
    let wake_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    assert!(wake_fd >= 0, "eventfd");
    let worker = {
        let target = Arc::clone(&target);
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("zc-rt-worker".into())
            .spawn(move || run_zc_queue(cdev_fd, 0, QUEUE_DEPTH, target, stop, wake_fd))
            .expect("spawn worker")
    };

    ctrl.start_dev(&dev).expect("start_dev");

    let bdev = wait_for_bdev(&bdev_path).expect("bdev appears");
    thread::sleep(Duration::from_millis(200));

    let pattern: Vec<u8> = (0..4096usize).map(|i| (i & 0xff) as u8).collect();
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&bdev)
            .expect("open bdev O_DIRECT");
        let aligned = aligned_4k(&pattern);
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&aligned).expect("bdev write");
    }
    {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&bdev)
            .expect("re-open bdev for read");
        let mut readback = aligned_4k(&vec![0u8; 4096]);
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut readback).expect("bdev read");
        assert_eq!(
            &readback[..pattern.len()],
            pattern.as_slice(),
            "zero-copy roundtrip data mismatch"
        );
    }
    eprintln!("ROUND-TRIP MATCH");

    stop.store(true, Ordering::SeqCst);
    let _ = ctrl.stop_dev();
    let start = std::time::Instant::now();
    while !worker.is_finished() && start.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_millis(50));
    }
    if worker.is_finished() {
        let _ = worker.join();
    } else {
        // Worker still parked in submit_and_wait; data plane already
        // verified, drop ownership and process::exit so libtest doesn't
        // wait on the OS thread.
        std::mem::forget(ctrl);
        std::mem::forget(dev);
        std::process::exit(0);
    }
}

fn wait_for_bdev(path: &str) -> Option<String> {
    for _ in 0..200 {
        if Path::new(path).exists() {
            return Some(path.to_string());
        }
        thread::sleep(Duration::from_millis(10));
    }
    None
}

fn aligned_4k(src: &[u8]) -> AlignedBuf {
    let mut buf = AlignedBuf::new(src.len());
    buf.as_mut_slice()[..src.len()].copy_from_slice(src);
    buf
}

struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
}
impl AlignedBuf {
    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, 4096).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        Self { ptr, len }
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}
impl std::ops::Deref for AlignedBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}
impl std::ops::DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len, 4096).unwrap();
        unsafe { std::alloc::dealloc(self.ptr, layout) };
    }
}
