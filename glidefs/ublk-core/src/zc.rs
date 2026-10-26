//! Zero-copy queue worker for `UBLK_F_AUTO_BUF_REG + UBLK_F_SUPPORT_ZERO_COPY`.
//!
//! The kernel auto-registers each bio's pages as an io_uring fixed buffer at
//! the slot index packed into the FETCH SQE's `addr` field. The userspace
//! target responds with `IORING_OP_READ_FIXED` / `WRITE_FIXED` against a
//! source/sink file descriptor using the registered `buf_index` — the kernel
//! does the DMA directly between the bio pages and the target file. No
//! userspace memcpy of bio data.
//!
//! ## One worker, many queues
//!
//! A [`run_zc_worker`] thread owns **one** io_uring and multiplexes every
//! queue (from any number of devices) that is added to it. Thread count,
//! ring memory and io-wq workers therefore scale with the number of
//! *workers* (a host constant chosen by the caller), never with the number
//! of devices. An idle queue costs two table slots and a handful of pending
//! uring_cmds — no thread.
//!
//! Ring layout (per worker):
//! - `IORING_SETUP_COOP_TASKRUN | SINGLE_ISSUER | DEFER_TASKRUN | CQSIZE`
//! - `register_buffers_sparse(ZC_BUF_TABLE)`. Queue slot `s` owns the buffer
//!   indices `[s * ZC_MAX_QUEUE_DEPTH, (s + 1) * ZC_MAX_QUEUE_DEPTH)`; tag `t`
//!   of that queue is `s * ZC_MAX_QUEUE_DEPTH + t`. The kernel populates the
//!   entry per-IO via auto_buf_reg (the index is packed into the FETCH /
//!   COMMIT_AND_FETCH SQE's `addr`).
//! - `register_files_sparse(ZC_MAX_QUEUES)`. Queue slot `s` registers its cdev
//!   at fixed-file index `s`; the uring_cmds use `types::Fixed(s)`.
//! - `register_iowq_max_workers` — see [`ZC_IOWQ_BOUNDED`].
//!
//! ## Concurrent dispatch + multi-chunk I/Os
//!
//! A target's dispatch logic may be async (e.g. await a backfill or read
//! plan) or fan out one bio into several `READ_FIXED` / `WRITE_FIXED` SQEs
//! (one per cache chunk). The worker exposes both:
//!
//! - [`ZcQueueHandle`] (cloneable, `Send`) posts a message to the worker's
//!   inbox and writes the worker's wake `eventfd`. Async dispatch tasks call
//!   [`submit`](ZcQueueHandle::submit) when they're ready with the final
//!   [`ZcAction`] (which itself can be `Chunks(Vec<ZcChunk>)`).
//! - [`ZcTarget::dispatch`] returns [`ZcDispatch::Inline`] (worker submits
//!   immediately, hot path) OR [`ZcDispatch::Deferred`] (target spawned an
//!   async task that will later call [`ZcQueueHandle::submit`]).
//! - The worker `PollAdd`s the eventfd. When a message lands, the PollAdd CQE
//!   wakes the worker, which drains the inbox: queue adds/removes, deferred
//!   submissions and shutdown all travel the same channel.
//!
//! `submit` also takes an optional **keepalive** (`Box<dyn Any + Send>`).
//! The worker holds it in the per-tag inflight slot until every chunk's CQE
//! has been observed and `after_*` has run. This lets the target keep
//! references alive (e.g. a `parking_lot::ArcRwLockReadGuard` gating file
//! rotation) for the entire kernel I/O lifetime — including the metadata
//! commit — without having to thread the lifetime through async boundaries.
//!
//! Only the worker thread ever pushes to the io_uring SQ (single-issuer
//! invariant). The channel + eventfd is the cross-thread bridge.
//!
//! ## Blocking inside a worker
//!
//! `dispatch` and `after_*` run on the worker thread and are shared by every
//! queue hosted on it. They MUST NOT block (see [`ZcTarget`]); a blocking
//! call stalls all queues on that worker, not just one device.
//!
//! ## Queue lifetime
//!
//! A queue ends when the kernel aborts every armed FETCH (`STOP_DEV` /
//! `DEL_DEV`), after which its cdev fixed-file slot is cleared (the kernel's
//! `DEL_DEV` waits for that reference), its command buffer is unmapped and
//! `done` fires. [`ZcWorkerHandle::remove_queue`] stops the worker from
//! dispatching or committing for a queue but cannot make the kernel release
//! the cdev — io_uring has no cancel for an armed ublk uring_cmd short of the
//! ring closing. The kernel LIVE -> QUIESCED transition (daemon handoff)
//! therefore needs the *worker* to exit ([`ZcWorkerHandle::shutdown`] + join),
//! which closes the ring.

use std::any::Any;
use std::collections::HashMap;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::mpsc;
use std::sync::Arc;

use io_uring::{cqueue, opcode, squeue, types, IoUring};

use crate::sys;
use crate::UblkError;

// The FETCH/COMMIT SQEs carry a `ublksrv_io_cmd` punned into the 16-byte
// `cmd` inline field of `UringCmd16`. That pun is only sound if the bindgen
// struct is exactly 16 bytes; a kernel-header change or alignment shift would
// otherwise read/write out of bounds. Guard it at compile time so a layout
// regression fails the build instead of corrupting SQEs at runtime.
const _: () = assert!(
    std::mem::size_of::<sys::ublksrv_io_cmd>() == 16,
    "ublksrv_io_cmd must be exactly 16 bytes to transmute into UringCmd16::cmd",
);

/// Deepest queue a worker will host. Fixes the width of each queue's slice
/// of the ring's registered-buffer table.
pub const ZC_MAX_QUEUE_DEPTH: u16 = 64;

/// Registered-buffer table size per worker ring. The kernel caps a ring at
/// `IORING_MAX_REG_BUFFERS` (1 << 14), and `ublk_auto_buf_reg.index` is a
/// u16, so this is also the largest index space a ring can address.
pub const ZC_BUF_TABLE: u32 = 1 << 14;

/// Queues one worker can host: its buffer table divided into
/// [`ZC_MAX_QUEUE_DEPTH`]-wide slices. Callers place queues on another
/// worker when this one is full.
pub const ZC_MAX_QUEUES: usize = (ZC_BUF_TABLE / ZC_MAX_QUEUE_DEPTH as u32) as usize;

/// Cap on io-wq workers the kernel may run for regular files (buffered
/// cache-file READ/WRITE_FIXED that cannot complete inline) per worker
/// ring. Without a cap io-wq spawns a worker per blocked request, up to the
/// SQ size, per ring. Total across a host is `workers * ZC_IOWQ_BOUNDED`.
pub const ZC_IOWQ_BOUNDED: u32 = 16;

/// Cap on io-wq workers for non-regular files (`/dev/zero`, memfd scratch).
pub const ZC_IOWQ_UNBOUNDED: u32 = 4;

/// SQ depth of a worker ring. `push_sqes` submits when it fills, so this
/// only bounds how much is batched per `io_uring_enter`.
const ZC_SQ_ENTRIES: u32 = 4096;

/// CQ depth of a worker ring: one slot per tag of a full worker
/// (`ZC_BUF_TABLE`), plus headroom for data-plane CQEs. Overflow is safe
/// (the kernel keeps the excess on a list) but slower.
const ZC_CQ_ENTRIES: u32 = ZC_BUF_TABLE * 2;

/// One data-plane SQE against the auto-registered bio at the queue's
/// `buf_index=tag`.
///
/// `buf_offset` is the offset within the kernel-registered bio buffer. For
/// a single-chunk I/O it's 0. For multi-chunk I/Os it's the cumulative byte
/// position of this chunk within the bio (`slice 0 spans [0..N0)`, `slice 1
/// spans [N0..N0+N1)`, etc.). The kernel adds it to `iov_base` of the
/// registered buffer at submission time.
#[derive(Clone, Copy)]
pub struct ZcChunk {
    pub op: ZcChunkOp,
    pub buf_offset: u32,
    pub length: u32,
}

#[derive(Clone, Copy)]
pub enum ZcChunkOp {
    /// `READ_FIXED(fd, src_offset, length, buf_index=tag, addr=buf_offset)`.
    /// Kernel reads from `fd` at `src_offset` into the auto-registered bio
    /// at `buf_offset`.
    ReadFixed { fd: RawFd, src_offset: u64 },
    /// `WRITE_FIXED(fd, dst_offset, length, buf_index=tag, addr=buf_offset)`.
    /// Kernel reads from the auto-registered bio at `buf_offset` and writes
    /// to `fd` at `dst_offset`.
    WriteFixed { fd: RawFd, dst_offset: u64 },
}

/// What the target wants done for one I/O.
///
/// `Chunks(Vec<_>)` submits one SQE per element; all chunks target the same
/// `buf_index=tag`. The worker fires `after_*` and `COMMIT_AND_FETCH` only
/// after every chunk's CQE has been observed.
pub enum ZcAction {
    /// One or more data-plane SQEs. The vec must be non-empty.
    Chunks(Vec<ZcChunk>),
    /// Complete immediately. No data plane; used for FLUSH / DISCARD /
    /// errors.
    Complete(i32),
}

/// Per-I/O keepalive — any `Send + 'static` box the target wants the
/// worker to drop only after the kernel I/O fully completes.
pub type Keepalive = Box<dyn Any + Send + 'static>;

/// What [`ZcTarget::dispatch`] resolved to right now.
pub enum ZcDispatch {
    /// Worker submits the action in the same iteration. No channel hop.
    ///
    /// `keepalive` extends the lifetime of any borrowed handles (e.g. a
    /// rotation-gate guard) past the kernel-side I/O — same semantics
    /// as [`ZcQueueHandle::submit`]'s `keepalive` arg, but without
    /// crossing the channel/eventfd. Used by the inline fast path
    /// (uncontended gate + synchronous pre-flight) to skip the
    /// `runtime.spawn` + cross-thread hop that dominates per-IO cost
    /// at small block sizes.
    Inline {
        action: ZcAction,
        keepalive: Option<Keepalive>,
    },
    /// Target spawned async work (or otherwise needs more time) and will
    /// call [`ZcQueueHandle::submit`] later.
    Deferred,
}

enum Msg {
    Add(Box<ZcQueueSpec>),
    Remove {
        key: u64,
    },
    Submit {
        key: u64,
        tag: u16,
        action: ZcAction,
        keepalive: Option<Keepalive>,
    },
    Shutdown,
}

/// Cross-thread handle to one worker. `Clone` + `Send`. The wake eventfd is
/// owned by the handle (shared by clones) so a late `submit` after the
/// worker exits writes to a still-open fd, never to a recycled one.
#[derive(Clone)]
pub struct ZcWorkerHandle {
    tx: mpsc::Sender<Msg>,
    wake: Arc<OwnedFd>,
}

/// The worker end of a [`ZcWorkerHandle`]; consumed by [`run_zc_worker`].
pub struct ZcWorkerInbox {
    rx: mpsc::Receiver<Msg>,
    /// A handle to this same worker, from which each hosted queue's
    /// [`ZcQueueHandle`] is made.
    handle: ZcWorkerHandle,
}

/// Create a worker's inbox + eventfd. Spawn a thread running
/// [`run_zc_worker`] on the returned inbox; the handle is how everything
/// else talks to it.
pub fn zc_worker_channel() -> std::io::Result<(ZcWorkerHandle, ZcWorkerInbox)> {
    // EFD_NONBLOCK so loop reads never block; EFD_CLOEXEC because nothing
    // in glidefs forks.
    // SAFETY: eventfd takes no pointers; the returned fd is checked and
    // immediately owned.
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `fd` is a fresh, valid eventfd that nothing else owns.
    let wake = Arc::new(unsafe { OwnedFd::from_raw_fd(fd) });
    let (tx, rx) = mpsc::channel();
    let handle = ZcWorkerHandle { tx, wake };
    Ok((handle.clone(), ZcWorkerInbox { rx, handle }))
}

impl ZcWorkerHandle {
    fn send(&self, msg: Msg) {
        // A closed channel means the worker has exited and the kernel has
        // aborted (or will abort) everything it armed; dropping is correct.
        let _ = self.tx.send(msg);
        let one = 1u64.to_ne_bytes();
        // SAFETY: eventfd write of 8 bytes is always defined; `wake` keeps
        // the fd open for as long as this handle exists.
        unsafe {
            libc::write(self.wake.as_raw_fd(), one.as_ptr() as *const _, 8);
        }
    }

    /// Host a queue on this worker. `spec.ready` fires on the worker thread
    /// once the queue's FETCHes are armed (or with the error that prevented
    /// it); `spec.done` fires when the queue has ended and released its
    /// slot, or when the worker exits.
    pub fn add_queue(&self, spec: ZcQueueSpec) {
        self.send(Msg::Add(Box::new(spec)));
    }

    /// Stop dispatching/committing for a queue. It is retired (and `done`
    /// fires) once the kernel has returned everything outstanding for it;
    /// see the module docs for why that needs the device stopped or the
    /// worker shut down.
    pub fn remove_queue(&self, key: u64) {
        self.send(Msg::Remove { key });
    }

    /// Ask the worker to exit. The ring closes when [`run_zc_worker`]
    /// returns; join the thread to wait for that.
    pub fn shutdown(&self) {
        self.send(Msg::Shutdown);
    }
}

/// A queue to host on a worker.
pub struct ZcQueueSpec {
    /// Caller-unique identity, used by [`ZcQueueHandle`] and
    /// [`ZcWorkerHandle::remove_queue`]. A stale key (queue already gone)
    /// is ignored, so keys must not be reused.
    pub key: u64,
    /// The ublk char device. Borrowed: the caller keeps it open until `done`.
    pub cdev_fd: RawFd,
    pub qid: u16,
    pub queue_depth: u16,
    pub target: Arc<dyn ZcTarget>,
    pub ready: Box<dyn FnOnce(Result<(), UblkError>) + Send>,
    pub done: Box<dyn FnOnce(Result<(), UblkError>) + Send>,
}

/// Cross-thread submission handle held by the target.
///
/// `Clone` + `Send`: the target hands clones into spawned async tasks.
/// When async work resolves, the task calls [`submit`](Self::submit) with
/// the final [`ZcAction`] (and an optional keepalive); the worker is woken
/// via the eventfd and pushes the matching SQE(s).
#[derive(Clone)]
pub struct ZcQueueHandle {
    worker: ZcWorkerHandle,
    key: u64,
}

impl ZcQueueHandle {
    /// Submit a data-plane action for `tag` plus an optional keepalive.
    ///
    /// `keepalive` is held by the worker until every chunk's CQE has been
    /// observed and `after_*` has run — i.e. until the kernel I/O is fully
    /// done. Use it to extend the lifetime of references (rotation-gate
    /// guards, fd handles, etc.) across the kernel boundary.
    ///
    /// Safe to call from any thread. If the worker has already torn down or
    /// the queue is gone, the entry drops silently — the kernel has aborted
    /// in-flight FETCHes.
    pub fn submit(&self, tag: u16, action: ZcAction, keepalive: Option<Keepalive>) {
        self.worker.send(Msg::Submit {
            key: self.key,
            tag,
            action,
            keepalive,
        });
    }
}

/// Target plugged into the ZC worker.
///
/// `dispatch` is called from the worker thread when a FETCH delivers I/O.
/// It can return immediately ([`ZcDispatch::Inline`]) for hot paths that
/// know the source/sink fd without async work, or return
/// [`ZcDispatch::Deferred`] after spawning a task that will call
/// `handle.submit(...)` later.
///
/// `after_write` / `after_read` are called on the worker thread when EVERY
/// chunk's data-plane SQE has completed (or any chunk has errored), before
/// the COMMIT_AND_FETCH goes out. Use them for synchronous metadata commit
/// (mark-dirty + WAL append). They MUST NOT block on async work — that
/// belongs on the deferred path.
pub trait ZcTarget: Send + Sync {
    fn dispatch(
        &self,
        op: u8,
        offset: u64,
        length: u32,
        tag: u16,
        handle: &ZcQueueHandle,
    ) -> ZcDispatch;

    fn after_write(
        &self,
        _op: u8,
        _offset: u64,
        _length: u32,
        _tag: u16,
        result: i32,
        _keepalive: Option<&(dyn Any + Send)>,
    ) -> i32 {
        result
    }

    fn after_read(
        &self,
        _op: u8,
        _offset: u64,
        _length: u32,
        _tag: u16,
        result: i32,
        _keepalive: Option<&(dyn Any + Send)>,
    ) -> i32 {
        result
    }
}

// user_data layout: bit 63 = data-plane chunk, bit 62 = eventfd poll,
// bits 32..48 = queue slot, bits 16..24 = op / cmd_op, bits 0..16 = tag.
const UD_TARGET_IO: u64 = 1 << 63;
const UD_EVENTFD: u64 = 1 << 62;

#[inline]
fn ud_cmd(slot: u16, tag: u16, cmd_op: u32) -> u64 {
    u64::from(tag) | (u64::from(cmd_op & 0xff) << 16) | (u64::from(slot) << 32)
}

#[inline]
fn ud_data(slot: u16, tag: u16, op: u8) -> u64 {
    UD_TARGET_IO | u64::from(tag) | (u64::from(op) << 16) | (u64::from(slot) << 32)
}

#[inline]
fn ud_tag(ud: u64) -> u16 {
    (ud & 0xffff) as u16
}

#[inline]
fn ud_slot(ud: u64) -> u16 {
    ((ud >> 32) & 0xffff) as u16
}

#[inline]
fn ud_is_data(ud: u64) -> bool {
    (ud & UD_TARGET_IO) != 0
}

#[inline]
fn ud_is_eventfd(ud: u64) -> bool {
    (ud & UD_EVENTFD) != 0
}

/// Ring buffer-table index for `tag` of the queue in `slot`.
#[inline]
fn buf_index(slot: u16, tag: u16) -> u16 {
    // slot < ZC_MAX_QUEUES and tag < ZC_MAX_QUEUE_DEPTH, so this is < 1<<14.
    slot * ZC_MAX_QUEUE_DEPTH + tag
}

fn build_io_cmd_sqe(slot: u16, qid: u16, tag: u16, result: i32, cmd_op: u32) -> squeue::Entry {
    let auto = sys::ublk_auto_buf_reg {
        index: buf_index(slot, tag),
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
    // SAFETY: `ublksrv_io_cmd` is exactly 16 bytes (asserted at module scope)
    // and is `#[repr(C)]` POD — every bit pattern is a valid `[u8; 16]`.
    let cmd_bytes: [u8; 16] = unsafe { std::mem::transmute(cmd) };
    let mut sqe = opcode::UringCmd16::new(types::Fixed(u32::from(slot)), cmd_op)
        .cmd(cmd_bytes)
        .build()
        .user_data(ud_cmd(slot, tag, cmd_op));
    let auto_addr = ublk_sys::ublk_auto_buf_reg_to_sqe_addr(&auto);
    crate::override_sqe!(&mut sqe, addr, auto_addr);
    sqe
}

fn build_fetch_sqe(slot: u16, qid: u16, tag: u16) -> squeue::Entry {
    build_io_cmd_sqe(slot, qid, tag, -1, sys::UBLK_U_IO_FETCH_REQ)
}

fn build_commit_sqe(slot: u16, qid: u16, tag: u16, result: i32) -> squeue::Entry {
    build_io_cmd_sqe(slot, qid, tag, result, sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ)
}

/// Build one chunk SQE. `addr_offset` is the offset within the bio buffer
/// at the tag's registered `buf_index` (sparse-registered, populated per-IO
/// by auto_buf_reg).
fn build_chunk_sqe(chunk: ZcChunk, slot: u16, tag: u16, op: u8) -> squeue::Entry {
    let buf_addr_offset = chunk.buf_offset as usize;
    let bi = buf_index(slot, tag);
    match chunk.op {
        ZcChunkOp::ReadFixed { fd, src_offset } => {
            opcode::ReadFixed::new(types::Fd(fd), buf_addr_offset as *mut u8, chunk.length, bi)
                .offset(src_offset)
                .build()
                .user_data(ud_data(slot, tag, op))
        }
        ZcChunkOp::WriteFixed { fd, dst_offset } => {
            opcode::WriteFixed::new(types::Fd(fd), buf_addr_offset as *const u8, chunk.length, bi)
                .offset(dst_offset)
                .build()
                .user_data(ud_data(slot, tag, op))
        }
    }
}

fn build_eventfd_poll_sqe(efd: RawFd) -> squeue::Entry {
    opcode::PollAdd::new(types::Fd(efd), libc::POLLIN as u32)
        .build()
        .user_data(UD_EVENTFD)
}

/// Per-tag in-flight context. Captured at FETCH-CQE receipt, accumulated
/// across chunk CQEs, consumed when the last chunk completes.
#[derive(Default)]
struct InFlight {
    op: u8,
    offset: u64,
    length: u32,
    outstanding: u32,
    /// First non-positive `cqe.result` observed across chunks. Once
    /// negative we still wait for the rest to drain (so io_uring's
    /// fixed-buffer accounting stays clean), but we deliver this as
    /// the final result.
    first_error: Option<i32>,
    keepalive: Option<Keepalive>,
}

/// One hosted queue.
struct Queue {
    key: u64,
    qid: u16,
    depth: u16,
    cdev_fd: RawFd,
    io_cmd_buf: *mut libc::c_void,
    cmd_buf_sz: usize,
    target: Arc<dyn ZcTarget>,
    handle: ZcQueueHandle,
    inflight: Vec<InFlight>,
    /// FETCH/COMMIT CQEs carrying `UBLK_IO_RES_ABORT`. At `depth` the
    /// kernel has taken back every armed command.
    aborted: usize,
    /// SQEs submitted for this queue whose CQE has not been reaped. The
    /// slot (and its buffer-index range) may only be reused at zero, or a
    /// late CQE would be routed to the next tenant.
    pending: u32,
    /// `remove_queue` seen: reap CQEs, dispatch and commit nothing.
    closing: bool,
    done: Option<Box<dyn FnOnce(Result<(), UblkError>) + Send>>,
}

impl Queue {
    fn retirable(&self) -> bool {
        self.pending == 0 && (self.closing || self.aborted >= self.depth as usize)
    }
}

struct Worker {
    ring: IoUring<squeue::Entry, cqueue::Entry>,
    handle: ZcWorkerHandle,
    wake_fd: RawFd,
    queues: Vec<Option<Queue>>,
    by_key: HashMap<u64, u16>,
    free: Vec<u16>,
    /// Slots whose state changed this iteration and may be retirable.
    touched: Vec<u16>,
    shutdown: bool,
}

fn build_ring() -> Result<IoUring<squeue::Entry, cqueue::Entry>, UblkError> {
    let ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .setup_cqsize(ZC_CQ_ENTRIES)
        .build(ZC_SQ_ENTRIES)
        .map_err(UblkError::IOError)?;
    ring.submitter()
        .register_buffers_sparse(ZC_BUF_TABLE)
        .map_err(UblkError::IOError)?;
    ring.submitter()
        .register_files_sparse(ZC_MAX_QUEUES as u32)
        .map_err(UblkError::IOError)?;
    // Bound io-wq. Requests that cannot complete inline (a regular file
    // without nowait support, a cold page, an extending write) are punted
    // to a kernel worker that blocks for the duration; unbounded, each ring
    // grows a worker per such request. Failure is non-fatal: an old kernel
    // just keeps the default.
    let mut max = [ZC_IOWQ_BOUNDED, ZC_IOWQ_UNBOUNDED];
    if let Err(e) = ring.submitter().register_iowq_max_workers(&mut max) {
        log::warn!("zc: register_iowq_max_workers failed: {e}");
    }
    Ok(ring)
}

/// Run a zero-copy worker until [`ZcWorkerHandle::shutdown`]. Returns after
/// the ring is torn down (its fd closed); ring setup failure is reported to
/// every `add_queue` it receives until shutdown.
///
/// See module docs for the architecture.
pub fn run_zc_worker(inbox: ZcWorkerInbox) {
    let ZcWorkerInbox { rx, handle } = inbox;
    let ring = match build_ring() {
        Ok(r) => r,
        Err(e) => {
            log::error!("zc worker: io_uring setup failed: {e:?}");
            while let Ok(msg) = rx.recv() {
                match msg {
                    Msg::Add(spec) => {
                        let ZcQueueSpec { ready, done, .. } = *spec;
                        ready(Err(UblkError::OtherError(-libc::EIO)));
                        done(Err(UblkError::OtherError(-libc::EIO)));
                    }
                    Msg::Shutdown => break,
                    _ => {}
                }
            }
            return;
        }
    };
    let wake_fd = handle.wake.as_raw_fd();
    let mut w = Worker {
        ring,
        handle,
        wake_fd,
        queues: (0..ZC_MAX_QUEUES).map(|_| None).collect(),
        by_key: HashMap::new(),
        free: (0..ZC_MAX_QUEUES as u16).rev().collect(),
        touched: Vec::new(),
        shutdown: false,
    };
    w.run(&rx);
    w.teardown();
}

impl Worker {
    fn run(&mut self, rx: &mpsc::Receiver<Msg>) {
        let mut to_submit: Vec<squeue::Entry> = vec![build_eventfd_poll_sqe(self.wake_fd)];
        loop {
            if let Err(e) = self.push_sqes(&to_submit) {
                log::error!("zc worker: SQ push failed: {e:?}");
                return;
            }
            to_submit.clear();

            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => {
                    log::error!("zc worker: io_uring_enter failed: {e}");
                    return;
                }
            }

            let cqes: Vec<cqueue::Entry> = self.ring.completion().collect();
            for cqe in cqes {
                self.on_cqe(cqe.user_data(), cqe.result(), rx, &mut to_submit);
            }

            for slot in std::mem::take(&mut self.touched) {
                if self.queues[slot as usize]
                    .as_ref()
                    .is_some_and(Queue::retirable)
                {
                    self.retire(slot);
                }
            }
            if self.shutdown {
                return;
            }
        }
    }

    fn push_sqes(&mut self, to_submit: &[squeue::Entry]) -> Result<(), UblkError> {
        for sqe in to_submit {
            // SAFETY: each `sqe` is a fully-initialized entry built by
            // `build_*_sqe` and copied into the ring by push(). Any buffer
            // it references (the per-tag keepalive) is held alive in
            // `inflight` until the matching completion.
            if unsafe { self.ring.submission().push(sqe) }.is_err() {
                // SQ full: flush it to the kernel and retry once.
                self.ring.submit().map_err(UblkError::IOError)?;
                // SAFETY: as above; the submit() freed the whole SQ.
                unsafe { self.ring.submission().push(sqe) }
                    .map_err(|_| UblkError::OtherError(-libc::EAGAIN))?;
            }
        }
        Ok(())
    }

    fn on_cqe(
        &mut self,
        ud: u64,
        res: i32,
        rx: &mpsc::Receiver<Msg>,
        to_submit: &mut Vec<squeue::Entry>,
    ) {
        if ud_is_eventfd(ud) {
            // Drain eventfd counter, then the inbox, then re-arm. A send
            // landing between the drain and the re-arm leaves the counter
            // non-zero, so the new PollAdd completes immediately.
            let mut buf = [0u8; 8];
            // SAFETY: `wake_fd` is open for the worker's lifetime (the
            // handle owns it); `buf` is a live 8-byte stack buffer.
            while unsafe { libc::read(self.wake_fd, buf.as_mut_ptr() as *mut _, 8) } == 8 {}
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    Msg::Add(spec) => self.add_queue(*spec, to_submit),
                    Msg::Remove { key } => {
                        if let Some(&slot) = self.by_key.get(&key) {
                            if let Some(q) = self.queues[slot as usize].as_mut() {
                                q.closing = true;
                            }
                            self.touched.push(slot);
                        }
                    }
                    Msg::Submit {
                        key,
                        tag,
                        action,
                        keepalive,
                    } => {
                        if let Some(&slot) = self.by_key.get(&key) {
                            self.apply_action(slot, tag, action, keepalive, to_submit);
                        }
                    }
                    Msg::Shutdown => self.shutdown = true,
                }
            }
            to_submit.push(build_eventfd_poll_sqe(self.wake_fd));
            return;
        }

        let slot = ud_slot(ud);
        let tag = ud_tag(ud);
        let Some(q) = self.queues.get_mut(slot as usize).and_then(Option::as_mut) else {
            return;
        };
        q.pending = q.pending.saturating_sub(1);
        self.touched.push(slot);

        if ud_is_data(ud) {
            // One chunk CQE.
            let inf = &mut q.inflight[tag as usize];
            if res < 0 && inf.first_error.is_none() {
                inf.first_error = Some(res);
            }
            debug_assert!(inf.outstanding > 0);
            inf.outstanding = inf.outstanding.saturating_sub(1);
            if inf.outstanding == 0 {
                self.finalize(slot, tag, to_submit);
            }
        } else if res == sys::UBLK_IO_RES_ABORT as i32 {
            // Kernel `STOP_DEV` aborts every armed FETCH with the
            // `UBLK_IO_RES_ABORT` sentinel (= -ENODEV). Once we've drained
            // one abort per tag the queue is over. *Only* the -ENODEV
            // sentinel counts. Other negative returns on a non-data CQE
            // (transient `-EAGAIN`, `-EINTR`, pre-START_DEV `-EINVAL`, etc.)
            // are not queue-terminal — counting them once caused a CI hang
            // where ~queue_depth pre-START_DEV transient errors ended the
            // queue before the device was even live.
            q.aborted = q.aborted.saturating_add(1);
        } else if res < 0 {
            // Transient or unexpected error on a non-data CQE — skip. The
            // kernel will re-issue or STOP_DEV will eventually flush the
            // queue with ABORTs.
        } else if !q.closing {
            // SAFETY: `io_cmd_buf` is the live PROT_READ mmap of the
            // kernel's per-queue io_desc array, sized for `depth` entries.
            // `tag < depth` (it indexes `inflight`), so the offset stays
            // within the mapping and is naturally aligned for
            // `ublksrv_io_desc`. The descriptor is populated by the kernel
            // before this FETCH CQE is delivered, and we only read it.
            let iod = unsafe {
                &*((q.io_cmd_buf as *const u8)
                    .add(tag as usize * std::mem::size_of::<sys::ublksrv_io_desc>())
                    as *const sys::ublksrv_io_desc)
            };
            let op = (iod.op_flags & 0xff) as u8;
            let offset = iod.start_sector << 9;
            let length: u32 = (u64::from(iod.nr_sectors) * 512) as u32;
            let inf = &mut q.inflight[tag as usize];
            inf.op = op;
            inf.offset = offset;
            inf.length = length;
            inf.outstanding = 0;
            inf.first_error = None;
            inf.keepalive = None;

            let target = Arc::clone(&q.target);
            let qh = q.handle.clone();
            match target.dispatch(op, offset, length, tag, &qh) {
                ZcDispatch::Inline { action, keepalive } => {
                    self.apply_action(slot, tag, action, keepalive, to_submit);
                }
                ZcDispatch::Deferred => {
                    // Target will call handle.submit later.
                }
            }
        }
    }

    /// Turn a target's decision for `tag` into SQEs (or an immediate commit).
    fn apply_action(
        &mut self,
        slot: u16,
        tag: u16,
        action: ZcAction,
        keepalive: Option<Keepalive>,
        to_submit: &mut Vec<squeue::Entry>,
    ) {
        let Some(q) = self.queues[slot as usize].as_mut() else {
            return;
        };
        if q.closing || tag >= q.depth {
            return;
        }
        let inf = &mut q.inflight[tag as usize];
        // The keepalive (e.g. a rotation-gate guard) must outlive the chunk
        // SQEs and the commit; it is consumed by `finalize`.
        inf.keepalive = keepalive;
        match action {
            ZcAction::Chunks(chunks) if chunks.is_empty() => {
                // Invalid — treat as immediate error.
                inf.first_error = Some(-libc::EINVAL);
                inf.outstanding = 0;
                self.finalize(slot, tag, to_submit);
            }
            ZcAction::Chunks(chunks) => {
                #[allow(clippy::cast_possible_truncation)]
                {
                    inf.outstanding = chunks.len() as u32;
                }
                let op = inf.op;
                q.pending += chunks.len() as u32;
                for ch in chunks {
                    to_submit.push(build_chunk_sqe(ch, slot, tag, op));
                }
            }
            ZcAction::Complete(r) => {
                inf.first_error = Some(r);
                inf.outstanding = 0;
                self.finalize(slot, tag, to_submit);
            }
        }
    }

    /// Run `after_*`, drop the keepalive and emit the COMMIT_AND_FETCH.
    fn finalize(&mut self, slot: u16, tag: u16, to_submit: &mut Vec<squeue::Entry>) {
        let Some(q) = self.queues[slot as usize].as_mut() else {
            return;
        };
        // Consume the inflight slot AT END (drops keepalive). We pass the
        // keepalive by reference to after_* so the target can downcast it to
        // recover handles (e.g. a rotation-gate ArcRwLockReadGuard) and
        // reuse them instead of re-acquiring (which would deadlock against a
        // queued writer).
        let inf = std::mem::take(&mut q.inflight[tag as usize]);
        if q.closing {
            return;
        }
        let aggregate_res = if let Some(err) = inf.first_error {
            err
        } else {
            #[allow(clippy::cast_possible_wrap)]
            let r = inf.length as i32;
            r
        };
        let final_res = match u32::from(inf.op) {
            sys::UBLK_IO_OP_WRITE => q.target.after_write(
                inf.op,
                inf.offset,
                inf.length,
                tag,
                aggregate_res,
                inf.keepalive.as_deref(),
            ),
            sys::UBLK_IO_OP_READ => q.target.after_read(
                inf.op,
                inf.offset,
                inf.length,
                tag,
                aggregate_res,
                inf.keepalive.as_deref(),
            ),
            _ => aggregate_res,
        };
        drop(inf);
        q.pending += 1;
        to_submit.push(build_commit_sqe(slot, q.qid, tag, final_res));
    }

    fn add_queue(&mut self, spec: ZcQueueSpec, to_submit: &mut Vec<squeue::Entry>) {
        let ZcQueueSpec {
            key,
            cdev_fd,
            qid,
            queue_depth,
            target,
            ready,
            done,
        } = spec;
        match self.try_add_queue(key, cdev_fd, qid, queue_depth, target, done, to_submit) {
            Ok(()) => ready(Ok(())),
            Err((e, done)) => {
                ready(Err(e));
                done(Err(UblkError::OtherError(-libc::EIO)));
            }
        }
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn try_add_queue(
        &mut self,
        key: u64,
        cdev_fd: RawFd,
        qid: u16,
        depth: u16,
        target: Arc<dyn ZcTarget>,
        done: Box<dyn FnOnce(Result<(), UblkError>) + Send>,
        to_submit: &mut Vec<squeue::Entry>,
    ) -> Result<(), (UblkError, Box<dyn FnOnce(Result<(), UblkError>) + Send>)> {
        if depth == 0 || depth > ZC_MAX_QUEUE_DEPTH || self.by_key.contains_key(&key) {
            return Err((UblkError::OtherError(-libc::EINVAL), done));
        }
        let Some(slot) = self.free.pop() else {
            return Err((UblkError::OtherError(-libc::ENOSPC), done));
        };

        // mmap the kernel's io_cmd_buf for this queue.
        // SAFETY: sysconf is a pure query syscall taking a constant name and
        // touching no userspace memory.
        let page_sz = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let cmd_buf_sz_raw = (depth as usize) * std::mem::size_of::<sys::ublksrv_io_desc>();
        let cmd_buf_sz = (cmd_buf_sz_raw + page_sz - 1) & !(page_sz - 1);
        let max_cmd_buf_sz = (((sys::UBLK_MAX_QUEUE_DEPTH as usize)
            * std::mem::size_of::<sys::ublksrv_io_desc>())
            + page_sz
            - 1)
            & !(page_sz - 1);
        let off = libc::off_t::from(sys::UBLKSRV_CMD_BUF_OFFSET)
            + libc::off_t::from(qid) * max_cmd_buf_sz as libc::off_t;
        // SAFETY: a null `addr` lets the kernel choose the mapping;
        // `cmd_buf_sz` is the page-rounded size of this queue's io_desc
        // array; `cdev_fd` is a valid open ublk char-device fd (the caller
        // keeps it open until `done`) and `off` is the kernel-defined
        // per-queue command-buffer offset. PROT_READ only — we never write
        // the mapping. The return value is checked against MAP_FAILED before
        // any use.
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
            self.free.push(slot);
            return Err((UblkError::IOError(std::io::Error::last_os_error()), done));
        }
        if let Err(e) = self
            .ring
            .submitter()
            .register_files_update(u32::from(slot), &[cdev_fd])
        {
            // SAFETY: exact base+length of the mmap above, not yet used.
            unsafe { libc::munmap(io_cmd_buf, cmd_buf_sz) };
            self.free.push(slot);
            return Err((UblkError::IOError(e), done));
        }

        let handle = ZcQueueHandle {
            worker: self.handle.clone(),
            key,
        };
        // Arm initial FETCHes.
        for tag in 0..depth {
            to_submit.push(build_fetch_sqe(slot, qid, tag));
        }
        self.queues[slot as usize] = Some(Queue {
            key,
            qid,
            depth,
            cdev_fd,
            io_cmd_buf,
            cmd_buf_sz,
            target,
            handle,
            inflight: (0..depth).map(|_| InFlight::default()).collect(),
            aborted: 0,
            pending: u32::from(depth),
            closing: false,
            done: Some(done),
        });
        self.by_key.insert(key, slot);
        Ok(())
    }

    /// Release a queue's slot: clear its cdev fixed-file entry (the kernel's
    /// `DEL_DEV` waits for that reference), unmap its command buffer and
    /// tell the owner it is over.
    fn retire(&mut self, slot: u16) {
        let Some(mut q) = self.queues[slot as usize].take() else {
            return;
        };
        self.by_key.remove(&q.key);
        // -1 clears the slot. Failure leaves a stale reference to the cdev,
        // which the kernel will complain about at DEL_DEV; nothing better to
        // do than say so.
        if let Err(e) = self
            .ring
            .submitter()
            .register_files_update(u32::from(slot), &[-1])
        {
            log::warn!("zc: clearing fixed-file slot {slot} failed: {e}");
        }
        // SAFETY: exact base+length of this queue's successful mmap; the
        // mapping is never touched after the queue is taken out of the table.
        unsafe { libc::munmap(q.io_cmd_buf, q.cmd_buf_sz) };
        self.free.push(slot);
        let _ = q.cdev_fd;
        if let Some(done) = q.done.take() {
            done(Ok(()));
        }
    }

    /// Worker exit: every hosted queue is over (the ring closes next, which
    /// is what hands the devices to the kernel's recovery path).
    fn teardown(&mut self) {
        for slot in 0..self.queues.len() {
            if let Some(mut q) = self.queues[slot].take() {
                // SAFETY: exact base+length of this queue's mmap.
                unsafe { libc::munmap(q.io_cmd_buf, q.cmd_buf_sz) };
                if let Some(done) = q.done.take() {
                    done(Ok(()));
                }
            }
        }
        self.by_key.clear();
    }
}
