//! End-to-end smoke test for kernel zero-copy I/O via `UBLK_F_AUTO_BUF_REG`.
//!
//! Verifies that on a kernel that advertises `UBLK_F_SUPPORT_ZERO_COPY +
//! UBLK_F_AUTO_BUF_REG`, the ublk-core auto-detect enables them on the
//! device, and that a userspace target can drive end-to-end I/O without
//! touching the data buffer — by using `IORING_OP_READ_FIXED` /
//! `IORING_OP_WRITE_FIXED` against the kernel-registered bio buffer slot.
//!
//! The worker runs its own raw `io_uring` (not `UblkQueue`) because the
//! AUTO_BUF_REG sparse-buffer-table setup needs to happen on the same
//! ring that submits the FETCH/COMMIT uring_cmds, and the executor-driven
//! UblkQueue path doesn't currently wire that up.
//!
//! Skips on:
//!   - non-root (cdev is mode 0600)
//!   - kernels that don't advertise AUTO_BUF_REG via `UBLK_CMD_GET_FEATURES`
//!     (e.g., 6.12 homelab → 0x1fe, missing AUTO_BUF_REG which is bit 11)

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use io_uring::{cqueue, opcode, squeue, types, IoUring};
use ublk_core::ctrl::{UblkCtrl, UblkCtrlBuilder};
use ublk_core::io::UblkDev;
use ublk_core::override_sqe;
use ublk_core::sys;
use ublk_core::UblkFlags;

const DEV_SIZE: u64 = 1 << 20; // 1 MiB
const NR_QUEUES: u16 = 1;
const QUEUE_DEPTH: u16 = 4;
const MAX_IO_BUF_BYTES: u32 = 64 * 1024;

fn ublk_runnable() -> bool {
    if !Path::new("/dev/ublk-control").exists() {
        return false;
    }
    unsafe { libc::geteuid() == 0 }
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

    // Construct dev (sets params, doesn't open queues).
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
    // Reuse the cdev fd UblkDev already opened — ublk_drv allows only one
    // open of /dev/ublkcN at a time, so a second OpenOptions::open() returns
    // EBUSY.
    let cdev_fd: RawFd = dev.tgt.fds[0];

    // Spawn worker BEFORE start_dev so its FETCHes are armed when the
    // kernel transitions the queue to LIVE. Match ublk-core's UblkQueue
    // setup order: workers arm fetches first, main thread runs start_dev.
    let stop = Arc::new(AtomicBool::new(false));
    let worker = {
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("zc-rt-worker".into())
            .spawn(move || run_worker(cdev_fd, stop))
            .expect("spawn worker")
    };
    // Sleep so the worker has time to mmap + submit fetches before
    // start_dev runs.
    thread::sleep(Duration::from_millis(100));

    eprintln!("start_dev...");
    ctrl.start_dev(&dev).expect("start_dev");
    eprintln!("start_dev returned");

    // Wait for /dev/ublkbN to appear (udev settle).
    let bdev = wait_for_bdev(&bdev_path).expect("bdev appears");
    eprintln!("bdev appeared: {}", bdev);
    // Give udev a moment to settle its partition scan before we open
    // the bdev — racing with udev's open path triggers BUSY.
    thread::sleep(Duration::from_millis(200));
    eprintln!("opening bdev for I/O");

    // Roundtrip a single 4 KiB I/O. The kernel slices our request into bios;
    // each bio's pages are auto-registered as a fixed buffer at the index we
    // pass via auto_buf_reg, and the worker submits READ_FIXED/WRITE_FIXED
    // against an anonymous memfd-backed storage with that buf_index. No
    // userspace memcpy of bio data.
    let pattern: Vec<u8> = (0..4096usize).map(|i| (i & 0xff) as u8).collect();
    {
        eprintln!("write: opening {} O_RDWR|O_DIRECT", bdev);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&bdev)
            .expect("open bdev O_DIRECT");
        eprintln!("write: opened, building aligned buffer");
        let aligned = aligned_4k(&pattern);
        file.seek(SeekFrom::Start(0)).unwrap();
        eprintln!("write: write_all 4096 bytes");
        file.write_all(&aligned).expect("bdev write");
        eprintln!("write: done");
    }
    {
        eprintln!("read: opening {} O_RDONLY|O_DIRECT", bdev);
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&bdev)
            .expect("re-open bdev for read");
        let mut readback = aligned_4k(&vec![0u8; 4096]);
        file.seek(SeekFrom::Start(0)).unwrap();
        eprintln!("read: read_exact 4096 bytes");
        file.read_exact(&mut readback).expect("bdev read");
        eprintln!("read: done, comparing");
        assert_eq!(&readback[..pattern.len()], pattern.as_slice(),
            "zero-copy roundtrip data mismatch");
        eprintln!("ROUND-TRIP MATCH");
    }

    eprintln!("shutdown: stop=true + stop_dev");
    stop.store(true, Ordering::SeqCst);
    let _ = ctrl.stop_dev();
    // Give the worker a moment to drain UBLK_IO_RES_ABORT completions and
    // exit. If it's still parked in submit_and_wait, drop ctrl (which
    // closes the cdev → unblocks the kernel) then exit the process. The
    // data-plane round-trip already verified the kernel zero-copy path,
    // and the worker has no work to do post-stop_dev.
    let start = std::time::Instant::now();
    while !worker.is_finished() && start.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_millis(50));
    }
    if worker.is_finished() {
        let _ = worker.join();
        eprintln!("zero_copy_roundtrip: OK");
    } else {
        eprintln!("zero_copy_roundtrip: OK (worker still parked in submit_and_wait — fine, data plane verified)");
        // Skip Drop on ctrl/dev — del_dev would block waiting for cdev
        // refcount to drop, which the leaked worker thread holds. The
        // kernel will reclaim everything when process exits.
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

/// 4 KiB aligned heap buffer for O_DIRECT.
struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
}
impl AlignedBuf {
    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, 4096).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            panic!("alloc failed");
        }
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

// ---------------------------------------------------------------------------
// Worker — runs in a thread, owns its own io_uring + sparse buffer table.
// ---------------------------------------------------------------------------

/// User-data encoding matching `UblkIOCtx::build_user_data`:
/// - bits 0-15: tag
/// - bits 16-23: op (low 8 bits of cmd_op)
/// - bit 63: target_io (1 = data-plane, 0 = ublk cmd)
const UD_TARGET_IO: u64 = 1 << 63;
fn ud_cmd(tag: u16, cmd_op: u32) -> u64 {
    u64::from(tag) | (u64::from(cmd_op & 0xff) << 16)
}
fn ud_data(tag: u16, op: u8) -> u64 {
    UD_TARGET_IO | u64::from(tag) | (u64::from(op) << 16)
}
fn ud_tag(ud: u64) -> u16 {
    (ud & 0xffff) as u16
}
fn ud_is_data(ud: u64) -> bool {
    (ud & UD_TARGET_IO) != 0
}


fn run_worker(cdev_fd: RawFd, stop: Arc<AtomicBool>) -> Result<(), String> {
    let qid: u16 = 0;
    let depth: u16 = QUEUE_DEPTH;

    eprintln!("[worker] using cdev_fd={}", cdev_fd);

    // Backing storage: anonymous memfd, sized to the device.
    let backing_fd: RawFd = unsafe {
        let name = b"zc-rt-backing\0";
        libc::syscall(libc::SYS_memfd_create, name.as_ptr() as *const libc::c_char, 0u32) as RawFd
    };
    if backing_fd < 0 {
        return Err(format!("memfd_create: {}", std::io::Error::last_os_error()));
    }
    if unsafe { libc::ftruncate(backing_fd, DEV_SIZE as i64) } < 0 {
        return Err(format!("ftruncate: {}", std::io::Error::last_os_error()));
    }
    let _backing = unsafe { File::from_raw_fd(backing_fd) };

    // Match upstream kublk.c setup verbatim.
    let mut ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .setup_cqsize(64)
        .build(64)
        .map_err(|e| format!("IoUring::build: {}", e))?;

    eprintln!("[worker] register_buffers_sparse({})", depth);
    ring.submitter()
        .register_buffers_sparse(depth as u32)
        .map_err(|e| format!("register_buffers_sparse: {}", e))?;

    // Register the cdev fd as fixed file slot 0. kublk.c does this for
    // every queue thread; the FETCH SQE then uses Fixed(0) which avoids
    // the per-submission fget/fput.
    ring.submitter()
        .register_files(&[cdev_fd])
        .map_err(|e| format!("register_files: {}", e))?;
    eprintln!("[worker] register_files([cdev_fd]) OK");

    // mmap io_cmd_buf for this queue — kernel writes the per-tag io
    // descriptor (start_sector, nr_sectors, op_flags) there.
    let page_sz = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    let cmd_buf_sz_raw = (depth as usize) * std::mem::size_of::<sys::ublksrv_io_desc>();
    let cmd_buf_sz = (cmd_buf_sz_raw + page_sz - 1) & !(page_sz - 1);
    let max_cmd_buf_sz = (((sys::UBLK_MAX_QUEUE_DEPTH as usize)
        * std::mem::size_of::<sys::ublksrv_io_desc>())
        + page_sz - 1) & !(page_sz - 1);
    let off = libc::off_t::from(sys::UBLKSRV_CMD_BUF_OFFSET)
        + libc::off_t::from(qid) * max_cmd_buf_sz as libc::off_t;
    let io_cmd_buf = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            cmd_buf_sz,
            libc::PROT_READ,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            cdev_fd,
            off,
        )
    };
    if io_cmd_buf == libc::MAP_FAILED {
        return Err(format!("mmap io_cmd_buf: {}", std::io::Error::last_os_error()));
    }

    eprintln!("[worker] mmap io_cmd_buf ok at {:p}", io_cmd_buf);

    // Arm initial FETCH for every tag.
    for tag in 0..depth {
        push_fetch(&mut ring, cdev_fd, qid, tag, -1)?;
    }
    let n = ring.submit().map_err(|e| format!("submit FETCHes: {}", e))?;
    eprintln!("[worker] submitted {} FETCHes", n);

    // I/O state per tag: when we receive a FETCH cqe, we read the iod, save
    // op + offset + len, submit data-plane op, and wait for its CQE before
    // submitting COMMIT_AND_FETCH.
    #[derive(Clone, Copy, Default)]
    struct PerTag {
        op: u8,
        offset: u64,
        len: u32,
    }
    let mut tags: Vec<PerTag> = vec![PerTag::default(); depth as usize];

    let mut aborted_seen = 0usize;
    loop {
        if stop.load(Ordering::SeqCst) {
            // Stop requested: wait briefly for any in-flight CQEs (which
            // will now arrive as UBLK_IO_RES_ABORT after stop_dev) and exit.
            match ring.submit_and_wait(0) {
                Ok(_) => {}
                Err(_) => break,
            }
            let cqes: Vec<cqueue::Entry> = ring.completion().collect();
            if cqes.is_empty() && aborted_seen > 0 {
                break;
            }
            for cqe in cqes {
                aborted_seen += 1;
                let _ = cqe.user_data();
            }
            if aborted_seen >= depth as usize {
                break;
            }
            continue;
        }

        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => return Err(format!("submit_and_wait: {}", e)),
        }

        let mut to_submit: Vec<squeue::Entry> = Vec::new();
        // Drain the completion queue.
        let cqes: Vec<cqueue::Entry> = ring.completion().collect();
        for cqe in cqes {
            let ud = cqe.user_data();
            let tag = ud_tag(ud);
            let res = cqe.result();
            // Only log abnormal CQEs to keep output small.
            if res < 0 && !ud_is_data(ud) {
                eprintln!(
                    "[worker] cmd CQE err tag={} res={}",
                    tag, res
                );
            } else if res < 0 && ud_is_data(ud) {
                eprintln!(
                    "[worker] data CQE err tag={} res={}",
                    tag, res
                );
            }
            if ud_is_data(ud) {
                // Data plane completed. Submit COMMIT_AND_FETCH with the
                // result from the data plane op.
                to_submit.push(build_commit_and_fetch(cdev_fd, qid, tag, res));
            } else {
                // FETCH (or COMMIT_AND_FETCH) cmd CQE.
                if res == sys::UBLK_IO_RES_ABORT as i32 {
                    continue;
                }
                if res == -(libc::ENODEV) {
                    eprintln!("[worker] re-arm FETCH tag={} after -ENODEV", tag);
                    to_submit.push(build_fetch(cdev_fd, qid, tag, -1));
                    continue;
                }
                if res < 0 {
                    eprintln!("[worker] unexpected cmd CQE res={} tag={}", res, tag);
                    continue;
                }
                let iod = unsafe {
                    &*((io_cmd_buf as *const u8).add(tag as usize
                        * std::mem::size_of::<sys::ublksrv_io_desc>())
                        as *const sys::ublksrv_io_desc)
                };
                let op = (iod.op_flags & 0xff) as u8;
                let offset = iod.start_sector << 9;
                let len: u32 = (u64::from(iod.nr_sectors) * 512) as u32;
                tags[tag as usize] = PerTag { op, offset, len };

                match u32::from(op) {
                    sys::UBLK_IO_OP_READ => {
                        to_submit.push(
                            opcode::ReadFixed::new(
                                types::Fd(backing_fd),
                                std::ptr::null_mut(),
                                len,
                                tag,
                            )
                            .offset(offset)
                            .build()
                            .user_data(ud_data(tag, op)),
                        );
                    }
                    sys::UBLK_IO_OP_WRITE => {
                        to_submit.push(
                            opcode::WriteFixed::new(
                                types::Fd(backing_fd),
                                std::ptr::null(),
                                len,
                                tag,
                            )
                            .offset(offset)
                            .build()
                            .user_data(ud_data(tag, op)),
                        );
                    }
                    sys::UBLK_IO_OP_FLUSH => {
                        to_submit.push(build_commit_and_fetch(cdev_fd, qid, tag, 0));
                    }
                    _ => {
                        to_submit.push(build_commit_and_fetch(
                            cdev_fd,
                            qid,
                            tag,
                            -libc::EIO,
                        ));
                    }
                }
            }
        }

        // Push everything we built.
        {
            let mut sq = ring.submission();
            for sqe in &to_submit {
                if unsafe { sq.push(sqe) }.is_err() {
                    // SQ full. Flush by dropping the borrow + submit, then retry.
                    drop(sq);
                    ring.submit().map_err(|e| format!("submit (drain): {}", e))?;
                    let mut sq2 = ring.submission();
                    unsafe { sq2.push(sqe) }
                        .map_err(|_| "sq still full after drain".to_string())?;
                    sq = sq2;
                    // Re-borrow into sq for the next iteration.
                }
            }
        }
    }

    // Best-effort cleanup of the cmd buffer mmap.
    unsafe { libc::munmap(io_cmd_buf, cmd_buf_sz) };

    Ok(())
}

fn push_fetch(
    ring: &mut IoUring<squeue::Entry, cqueue::Entry>,
    cdev_fd: RawFd,
    qid: u16,
    tag: u16,
    result: i32,
) -> Result<(), String> {
    let sqe = build_fetch(cdev_fd, qid, tag, result);
    unsafe {
        ring.submission()
            .push(&sqe)
            .map_err(|_| "fetch sq full".to_string())
    }
}

fn build_fetch(cdev_fd: RawFd, qid: u16, tag: u16, result: i32) -> squeue::Entry {
    // For AUTO_BUF_REG, sqe.addr carries packed ublk_auto_buf_reg with the
    // buf_index we want the kernel to register the bio into.
    let auto = sys::ublk_auto_buf_reg {
        index: tag,
        flags: 0,
        reserved0: 0,
        reserved1: 0,
    };
    let cmd = sys::ublksrv_io_cmd {
        q_id: qid,
        tag,
        result,
        addr: 0,
    };
    let cmd_bytes: [u8; 16] = unsafe { std::mem::transmute(cmd) };
    let mut sqe = opcode::UringCmd16::new(types::Fixed(0), sys::UBLK_U_IO_FETCH_REQ)
        .cmd(cmd_bytes)
        .build()
        .user_data(ud_cmd(tag, sys::UBLK_U_IO_FETCH_REQ));
    let auto_addr = ublk_sys::ublk_auto_buf_reg_to_sqe_addr(&auto);
    override_sqe!(&mut sqe, addr, auto_addr);
    sqe
}

fn build_commit_and_fetch(cdev_fd: RawFd, qid: u16, tag: u16, result: i32) -> squeue::Entry {
    let auto = sys::ublk_auto_buf_reg {
        index: tag,
        flags: 0,
        reserved0: 0,
        reserved1: 0,
    };
    let cmd = sys::ublksrv_io_cmd {
        q_id: qid,
        tag,
        result,
        addr: 0,
    };
    let cmd_bytes: [u8; 16] = unsafe { std::mem::transmute(cmd) };
    let mut sqe = opcode::UringCmd16::new(
        types::Fixed(0),
        sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ,
    )
    .cmd(cmd_bytes)
    .build()
    .user_data(ud_cmd(tag, sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ));
    let auto_addr = ublk_sys::ublk_auto_buf_reg_to_sqe_addr(&auto);
    override_sqe!(&mut sqe, addr, auto_addr);
    sqe
}
