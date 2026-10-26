//! The ZC worker multiplexes queues: thread count is the worker count, not
//! the queue count, and a queue's slot is reusable once it is retired.
//!
//! No ublk kernel support needed. Each "cdev" is a memfd: the worker mmaps
//! its io_desc area exactly as it would a ublk char device, and the FETCH
//! uring_cmds against it complete with an error (a memfd has no
//! `->uring_cmd`) — which the worker treats as non-terminal, leaving the
//! queue parked until it is removed. That exercises everything except the
//! kernel's own ublk behaviour: slot/buffer-index allocation, fixed-file
//! table updates, retirement, shutdown.

use std::os::fd::RawFd;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use ublk_core::zc::{
    run_zc_worker, zc_worker_channel, ZcDispatch, ZcQueueHandle, ZcQueueSpec, ZcTarget,
    ZcWorkerHandle, ZC_MAX_QUEUES,
};

struct Never;
impl ZcTarget for Never {
    fn dispatch(&self, _: u8, _: u64, _: u32, _: u16, _: &ZcQueueHandle) -> ZcDispatch {
        unreachable!("no IO is ever delivered to a fake cdev")
    }
}

/// A memfd large enough that every qid's io_desc mmap window exists.
fn fake_cdev() -> RawFd {
    let name = b"fake-cdev\0";
    // SAFETY: NUL-terminated name; the fd is checked.
    let fd = unsafe { libc::syscall(libc::SYS_memfd_create, name.as_ptr(), 0u32) } as RawFd;
    assert!(fd >= 0, "memfd_create");
    // SAFETY: valid fd.
    assert_eq!(unsafe { libc::ftruncate(fd, 4 << 20) }, 0);
    fd
}

fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task").unwrap().count()
}

struct Added {
    ready: mpsc::Receiver<Result<(), String>>,
    done: mpsc::Receiver<()>,
}

fn add(w: &ZcWorkerHandle, key: u64, cdev: RawFd, qid: u16) -> Added {
    let (rtx, ready) = mpsc::channel();
    let (dtx, done) = mpsc::channel();
    w.add_queue(ZcQueueSpec {
        key,
        cdev_fd: cdev,
        qid,
        queue_depth: 8,
        target: Arc::new(Never),
        ready: Box::new(move |r| {
            let _ = rtx.send(r.map_err(|e| format!("{e:?}")));
        }),
        done: Box::new(move |_| {
            let _ = dtx.send(());
        }),
    });
    Added { ready, done }
}

fn ring_available() -> bool {
    io_uring::IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .build(8)
        .is_ok()
}

#[test]
fn many_queues_one_thread_and_slots_recycle() {
    if !ring_available() {
        eprintln!("skip: kernel lacks SINGLE_ISSUER + DEFER_TASKRUN io_uring");
        return;
    }
    let (worker, inbox) = zc_worker_channel().unwrap();
    let before = thread_count();
    let join = std::thread::spawn(move || run_zc_worker(inbox));
    let cdevs: Vec<RawFd> = (0..8).map(|_| fake_cdev()).collect();

    // Fill the worker: every queue is armed on the one thread.
    let mut added = Vec::new();
    for k in 0..ZC_MAX_QUEUES as u64 {
        added.push(add(&worker, k + 1, cdevs[(k % 8) as usize], (k % 4) as u16));
    }
    for a in &added {
        assert_eq!(a.ready.recv_timeout(Duration::from_secs(10)).unwrap(), Ok(()));
    }
    assert_eq!(
        thread_count(),
        before + 1,
        "{ZC_MAX_QUEUES} queues must cost exactly the worker thread"
    );

    // One more does not fit; the error says so and nothing else is disturbed.
    let over = add(&worker, 9_999, cdevs[0], 0);
    assert!(over.ready.recv_timeout(Duration::from_secs(10)).unwrap().is_err());
    over.done.recv_timeout(Duration::from_secs(10)).unwrap();

    // Removing a queue retires it (done fires) and frees its slot.
    worker.remove_queue(1);
    added[0].done.recv_timeout(Duration::from_secs(10)).unwrap();
    let again = add(&worker, 10_000, cdevs[0], 0);
    assert_eq!(again.ready.recv_timeout(Duration::from_secs(10)).unwrap(), Ok(()));

    // Shutdown ends every remaining queue and the thread.
    worker.shutdown();
    join.join().unwrap();
    for a in added.iter().skip(1) {
        a.done.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    again.done.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(thread_count(), before, "worker thread must be gone");
    for fd in cdevs {
        // SAFETY: fds opened above, closed once.
        unsafe { libc::close(fd) };
    }
}
