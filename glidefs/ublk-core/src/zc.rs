//! Zero-copy queue worker for `UBLK_F_AUTO_BUF_REG + UBLK_F_SUPPORT_ZERO_COPY`.
//!
//! The kernel auto-registers each bio's pages as an io_uring fixed buffer at
//! the slot index packed into the FETCH SQE's `addr` field. The userspace
//! target responds with `IORING_OP_READ_FIXED` / `WRITE_FIXED` against a
//! source/sink file descriptor using `buf_index=tag` — the kernel does the
//! DMA directly between the bio pages and the target file. No userspace
//! memcpy of bio data.
//!
//! Setup (matches `tools/testing/selftests/ublk/kublk.c`):
//! - `IORING_SETUP_COOP_TASKRUN | SINGLE_ISSUER | DEFER_TASKRUN | CQSIZE`
//! - `register_buffers_sparse(queue_depth)` — kernel populates each slot
//!   per-IO via auto_buf_reg
//! - `register_files([cdev_fd])` — cdev as fixed-file slot 0, FETCH SQEs use
//!   `types::Fixed(0)`
//!
//! ## Concurrent dispatch + multi-chunk I/Os
//!
//! A target's dispatch logic may be async (e.g. await a backfill or read
//! plan) or fan out one bio into several `READ_FIXED` / `WRITE_FIXED` SQEs
//! (one per cache chunk). The worker exposes both:
//!
//! - [`ZcQueueHandle`] (cloneable, `Send`) carries a `Sender<Deferred>` plus
//!   the queue's wake `eventfd`. Async dispatch tasks call
//!   [`submit`](ZcQueueHandle::submit) when they're ready with the final
//!   [`ZcAction`] (which itself can be `Chunks(Vec<ZcChunk>)`).
//! - [`ZcTarget::dispatch`] returns [`ZcDispatch::Inline`] (worker submits
//!   immediately, hot path) OR [`ZcDispatch::Deferred`] (target spawned an
//!   async task that will later call [`ZcQueueHandle::submit`]).
//! - The worker `PollAdd`s the eventfd. When a deferred submission lands,
//!   the target writes the eventfd → the PollAdd CQE wakes the worker →
//!   the worker drains the channel and pushes the matching SQE(s).
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

use std::any::Any;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

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

/// One data-plane SQE against the auto-registered bio at `buf_index=tag`.
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
    /// crossing the mpsc/eventfd. Used by the inline fast path
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

/// Cross-thread submission handle held by the target.
///
/// `Clone` + `Send`: the target hands clones into spawned async tasks.
/// When async work resolves, the task calls [`submit`](Self::submit) with
/// the final [`ZcAction`] (and an optional keepalive); the worker is woken
/// via the eventfd and pushes the matching SQE(s).
#[derive(Clone)]
pub struct ZcQueueHandle {
    tx: mpsc::Sender<DeferredEntry>,
    eventfd_fd: RawFd,
}

struct DeferredEntry {
    tag: u16,
    action: ZcAction,
    keepalive: Option<Keepalive>,
}

impl ZcQueueHandle {
    /// Submit a data-plane action for `tag` plus an optional keepalive.
    ///
    /// `keepalive` is held by the worker until every chunk's CQE has been
    /// observed and `after_*` has run — i.e. until the kernel I/O is fully
    /// done. Use it to extend the lifetime of references (rotation-gate
    /// guards, fd handles, etc.) across the kernel boundary.
    ///
    /// Safe to call from any thread. If the worker has already torn down,
    /// the send drops silently — the kernel has aborted in-flight FETCHes.
    pub fn submit(&self, tag: u16, action: ZcAction, keepalive: Option<Keepalive>) {
        let _ = self.tx.send(DeferredEntry { tag, action, keepalive });
        let one = 1u64.to_ne_bytes();
        // SAFETY: eventfd write of 8 bytes is always defined; the worst case
        // on a closed fd is EBADF, which we ignore.
        unsafe {
            libc::write(self.eventfd_fd, one.as_ptr() as *const _, 8);
        }
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

const UD_TARGET_IO: u64 = 1 << 63;
const UD_EVENTFD: u64 = 1 << 62;

#[inline]
fn ud_cmd(tag: u16, cmd_op: u32) -> u64 {
    u64::from(tag) | (u64::from(cmd_op & 0xff) << 16)
}

#[inline]
fn ud_data(tag: u16, op: u8) -> u64 {
    UD_TARGET_IO | u64::from(tag) | (u64::from(op) << 16)
}

#[inline]
fn ud_tag(ud: u64) -> u16 {
    (ud & 0xffff) as u16
}

#[inline]
fn ud_is_data(ud: u64) -> bool {
    (ud & UD_TARGET_IO) != 0
}

#[inline]
fn ud_is_eventfd(ud: u64) -> bool {
    (ud & UD_EVENTFD) != 0
}

fn build_fetch_sqe(qid: u16, tag: u16, result: i32) -> squeue::Entry {
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
    // SAFETY: `ublksrv_io_cmd` is exactly 16 bytes (asserted at module scope)
    // and is `#[repr(C)]` POD — every bit pattern is a valid `[u8; 16]`.
    let cmd_bytes: [u8; 16] = unsafe { std::mem::transmute(cmd) };
    let mut sqe = opcode::UringCmd16::new(types::Fixed(0), sys::UBLK_U_IO_FETCH_REQ)
        .cmd(cmd_bytes)
        .build()
        .user_data(ud_cmd(tag, sys::UBLK_U_IO_FETCH_REQ));
    let auto_addr = ublk_sys::ublk_auto_buf_reg_to_sqe_addr(&auto);
    crate::override_sqe!(&mut sqe, addr, auto_addr);
    sqe
}

fn build_commit_sqe(qid: u16, tag: u16, result: i32) -> squeue::Entry {
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
    // SAFETY: `ublksrv_io_cmd` is exactly 16 bytes (asserted at module scope)
    // and is `#[repr(C)]` POD — every bit pattern is a valid `[u8; 16]`.
    let cmd_bytes: [u8; 16] = unsafe { std::mem::transmute(cmd) };
    let mut sqe = opcode::UringCmd16::new(
        types::Fixed(0),
        sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ,
    )
    .cmd(cmd_bytes)
    .build()
    .user_data(ud_cmd(tag, sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ));
    let auto_addr = ublk_sys::ublk_auto_buf_reg_to_sqe_addr(&auto);
    crate::override_sqe!(&mut sqe, addr, auto_addr);
    sqe
}

/// Build one chunk SQE. `addr_offset` is the offset within the bio buffer
/// at `buf_index=tag` (sparse-registered, populated per-IO by auto_buf_reg).
fn build_chunk_sqe(chunk: ZcChunk, tag: u16, op: u8) -> squeue::Entry {
    let buf_addr_offset = chunk.buf_offset as usize;
    match chunk.op {
        ZcChunkOp::ReadFixed { fd, src_offset } => {
            opcode::ReadFixed::new(
                types::Fd(fd),
                buf_addr_offset as *mut u8,
                chunk.length,
                tag,
            )
            .offset(src_offset)
            .build()
            .user_data(ud_data(tag, op))
        }
        ZcChunkOp::WriteFixed { fd, dst_offset } => {
            opcode::WriteFixed::new(
                types::Fd(fd),
                buf_addr_offset as *const u8,
                chunk.length,
                tag,
            )
            .offset(dst_offset)
            .build()
            .user_data(ud_data(tag, op))
        }
    }
}

fn build_eventfd_poll_sqe(efd: RawFd) -> squeue::Entry {
    opcode::PollAdd::new(types::Fd(efd), libc::POLLIN as u32)
        .build()
        .user_data(UD_EVENTFD)
}

/// Run a zero-copy queue worker until `stop` is set.
///
/// `wake_fd` is an externally-owned eventfd (created by the caller via
/// [`libc::eventfd`] with `EFD_NONBLOCK | EFD_CLOEXEC`). The loop arms
/// a `PollAdd` on it so that cross-thread `handle.submit` AND an
/// out-of-band `stop` signal can both wake the io_uring loop:
///
///   * `submit` writes 8 bytes to `wake_fd` → PollAdd CQE → loop drains
///     the mpsc and pushes the deferred SQEs.
///   * Drop-guard / cancellation flips `stop` then writes 8 bytes to
///     `wake_fd` → PollAdd CQE → loop checks `stop` → drains in-flight
///     CQEs once non-blockingly → breaks. Closing the `IoUring` drops
///     its fd; the kernel sees the last uring on `cdev_fd` close and
///     transitions the device LIVE → QUIESCED (USER_RECOVERY contract).
///
/// Without external ownership of `wake_fd`, a caller that wanted to
/// stop the loop synchronously had no way to wake it from
/// `submit_and_wait(1)`. The handoff predecessor's worker-pool drop
/// hits exactly that case — see
/// `tests/handoff_durability::handoff_durability_crh_per_pr`.
///
/// See module docs for the architecture.
pub fn run_zc_queue(
    cdev_fd: RawFd,
    qid: u16,
    queue_depth: u16,
    target: Arc<dyn ZcTarget>,
    stop: Arc<AtomicBool>,
    wake_fd: RawFd,
) -> Result<(), UblkError> {
    if queue_depth == 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    // Ring sized for: queue_depth FETCHes/COMMITs + worst-case multi-chunk
    // fan-out + the eventfd PollAdd. 16x queue_depth gives generous slack
    // for 128 KB I/Os against 4 KB chunks (32 chunks worst case).
    let ring_size = (u32::from(queue_depth) * 64).max(64);
    let mut ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .setup_cqsize(ring_size)
        .build(ring_size)
        .map_err(UblkError::IOError)?;

    ring.submitter()
        .register_buffers_sparse(u32::from(queue_depth))
        .map_err(UblkError::IOError)?;
    ring.submitter()
        .register_files(&[cdev_fd])
        .map_err(UblkError::IOError)?;

    let eventfd_fd = wake_fd;
    let (tx, rx) = mpsc::channel::<DeferredEntry>();
    let handle = ZcQueueHandle { tx, eventfd_fd };

    // mmap the kernel's io_cmd_buf for this queue.
    let page_sz = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    let cmd_buf_sz_raw = (queue_depth as usize) * std::mem::size_of::<sys::ublksrv_io_desc>();
    let cmd_buf_sz = (cmd_buf_sz_raw + page_sz - 1) & !(page_sz - 1);
    let max_cmd_buf_sz = (((sys::UBLK_MAX_QUEUE_DEPTH as usize)
        * std::mem::size_of::<sys::ublksrv_io_desc>())
        + page_sz - 1)
        & !(page_sz - 1);
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
        // `wake_fd` is owned by the caller — don't close it here.
        return Err(UblkError::IOError(std::io::Error::last_os_error()));
    }

    // Arm initial FETCHes + the eventfd PollAdd.
    {
        let mut sq = ring.submission();
        for tag in 0..queue_depth {
            let sqe = build_fetch_sqe(qid, tag, -1);
            unsafe { sq.push(&sqe) }.map_err(|_| UblkError::OtherError(-libc::EAGAIN))?;
        }
        let sqe = build_eventfd_poll_sqe(eventfd_fd);
        unsafe { sq.push(&sqe) }.map_err(|_| UblkError::OtherError(-libc::EAGAIN))?;
    }
    ring.submit().map_err(UblkError::IOError)?;

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
    let mut inflight: Vec<InFlight> = (0..queue_depth).map(|_| InFlight::default()).collect();
    let mut aborted = 0usize;

    let push_sqes = |ring: &mut IoUring<squeue::Entry, cqueue::Entry>,
                     to_submit: &[squeue::Entry]|
     -> Result<(), UblkError> {
        if to_submit.is_empty() {
            return Ok(());
        }
        let mut sq = ring.submission();
        for sqe in to_submit {
            if unsafe { sq.push(sqe) }.is_err() {
                drop(sq);
                ring.submit().map_err(UblkError::IOError)?;
                let mut sq2 = ring.submission();
                unsafe { sq2.push(sqe) }
                    .map_err(|_| UblkError::OtherError(-libc::EAGAIN))?;
                sq = sq2;
            }
        }
        Ok(())
    };

    let finalize = |tag: u16,
                    inflight: &mut [InFlight],
                    target: &Arc<dyn ZcTarget>,
                    qid: u16|
     -> squeue::Entry {
        // Consume the inflight slot AT END (drops keepalive) and emit the
        // COMMIT. We pass the keepalive by reference to after_* so the
        // target can downcast it to recover handles (e.g. a rotation-gate
        // ArcRwLockReadGuard) and reuse them instead of re-acquiring
        // (which would deadlock against a queued writer).
        let mut slot = std::mem::take(&mut inflight[tag as usize]);
        let aggregate_res = if let Some(err) = slot.first_error {
            err
        } else {
            #[allow(clippy::cast_possible_wrap)]
            let r = slot.length as i32;
            r
        };
        let final_res = match u32::from(slot.op) {
            sys::UBLK_IO_OP_WRITE => target.after_write(
                slot.op,
                slot.offset,
                slot.length,
                tag,
                aggregate_res,
                slot.keepalive.as_deref(),
            ),
            sys::UBLK_IO_OP_READ => target.after_read(
                slot.op,
                slot.offset,
                slot.length,
                tag,
                aggregate_res,
                slot.keepalive.as_deref(),
            ),
            _ => aggregate_res,
        };
        // keepalive drops here (slot is consumed).
        drop(slot);
        build_commit_sqe(qid, tag, final_res)
    };

    'outer: loop {
        if stop.load(Ordering::SeqCst) {
            // Two exit paths land here:
            //
            // (a) Kernel `STOP_DEV` issued an abort wave for every armed
            //     FETCH and the caller flipped `stop` to drain. We can
            //     still wait briefly for those CQEs so the slot/keepalive
            //     drops run on the loop thread (correct lifetime for the
            //     gate guards).
            //
            // (b) Userspace-driven graceful exit — the most important is
            //     the handoff predecessor's cutover, which needs the
            //     `cdev_fd` io_uring closed so the kernel transitions the
            //     ublk device to QUIESCED (UBLK_F_USER_RECOVERY contract:
            //     LIVE → QUIESCED happens iff every uring touching cdev
            //     closes). No abort wave is coming because the predecessor
            //     never issued `STOP_DEV`. If we wait for one we hang the
            //     handoff — which is exactly what kept the per-PR
            //     handoff_durability test (successor saw state=1, skipped
            //     recovery, then UblkCtrl::new EOPNOTSUPP'd on the
            //     still-LIVE device).
            //
            // Resolution: drain a single non-blocking pass so any CQE
            // already in the ring gets processed (preserves the
            // STOP_DEV-driven drain semantics), then break unconditionally.
            // Dropping the `IoUring` closes its fd, the kernel sees the
            // last uring on cdev gone, the device transitions to QUIESCED,
            // and the successor can take over.
            let _ = ring.submit_and_wait(0);
            let _drain: Vec<cqueue::Entry> = ring.completion().collect();
            break;
        }

        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
                unsafe { libc::munmap(io_cmd_buf, cmd_buf_sz) };
                // `wake_fd` is owned by the caller — don't close it here.
                return Err(UblkError::IOError(e));
            }
        }

        let mut to_submit: Vec<squeue::Entry> = Vec::new();
        let cqes: Vec<cqueue::Entry> = ring.completion().collect();
        for cqe in cqes {
            let ud = cqe.user_data();
            let res = cqe.result();

            if ud_is_eventfd(ud) {
                // Drain eventfd counter.
                let mut buf = [0u8; 8];
                while unsafe {
                    libc::read(eventfd_fd, buf.as_mut_ptr() as *mut _, 8)
                } == 8 {}
                while let Ok(entry) = rx.try_recv() {
                    match entry.action {
                        ZcAction::Chunks(chunks) => {
                            if chunks.is_empty() {
                                // Invalid — treat as immediate error.
                                let slot = &mut inflight[entry.tag as usize];
                                slot.keepalive = entry.keepalive;
                                slot.first_error = Some(-libc::EINVAL);
                                slot.outstanding = 0;
                                to_submit.push(finalize(entry.tag, &mut inflight, &target, qid));
                                continue;
                            }
                            let slot = &mut inflight[entry.tag as usize];
                            slot.keepalive = entry.keepalive;
                            #[allow(clippy::cast_possible_truncation)]
                            { slot.outstanding = chunks.len() as u32; }
                            for ch in chunks {
                                to_submit.push(build_chunk_sqe(ch, entry.tag, slot.op));
                            }
                        }
                        ZcAction::Complete(r) => {
                            let slot = &mut inflight[entry.tag as usize];
                            slot.keepalive = entry.keepalive;
                            slot.first_error = Some(r);
                            slot.outstanding = 0;
                            to_submit.push(finalize(entry.tag, &mut inflight, &target, qid));
                        }
                    }
                }
                to_submit.push(build_eventfd_poll_sqe(eventfd_fd));
                continue;
            }

            let tag = ud_tag(ud);

            if ud_is_data(ud) {
                // One chunk CQE.
                let slot = &mut inflight[tag as usize];
                if res < 0 && slot.first_error.is_none() {
                    slot.first_error = Some(res);
                }
                debug_assert!(slot.outstanding > 0);
                slot.outstanding = slot.outstanding.saturating_sub(1);
                if slot.outstanding == 0 {
                    to_submit.push(finalize(tag, &mut inflight, &target, qid));
                }
            } else if res == sys::UBLK_IO_RES_ABORT as i32 {
                // Kernel `STOP_DEV` aborts every armed FETCH with the
                // `UBLK_IO_RES_ABORT` sentinel (= -ENODEV). Once we've
                // drained one abort per tag, no more CQEs are coming
                // (no in-flight chunk SQEs, no re-armed FETCHes), and
                // the next `submit_and_wait(1)` would sleep forever.
                // Count aborts; when we've seen them all, break the
                // outer loop so the caller's `done_rx` resolves and
                // `UblkDevice::unregister` can finish.
                //
                // *Only* the -ENODEV sentinel counts. Other negative
                // returns on a non-data CQE (transient `-EAGAIN`,
                // `-EINTR`, pre-START_DEV `-EINVAL`, etc.) are not
                // queue-terminal — counting them caused a CI hang where
                // ~queue_depth pre-START_DEV transient errors broke the
                // loop before the device was even live, leaving the
                // kernel device with no userspace handler.
                aborted = aborted.saturating_add(1);
                if aborted >= queue_depth as usize {
                    break 'outer;
                }
                continue;
            } else if res < 0 {
                // Transient or unexpected error on a non-data CQE — skip
                // and keep the loop running. The kernel will re-issue or
                // STOP_DEV will eventually flush the queue with ABORTs.
                continue;
            } else {
                let iod = unsafe {
                    &*((io_cmd_buf as *const u8)
                        .add(tag as usize * std::mem::size_of::<sys::ublksrv_io_desc>())
                        as *const sys::ublksrv_io_desc)
                };
                let op = (iod.op_flags & 0xff) as u8;
                let offset = iod.start_sector << 9;
                let length: u32 = (u64::from(iod.nr_sectors) * 512) as u32;
                let slot = &mut inflight[tag as usize];
                slot.op = op;
                slot.offset = offset;
                slot.length = length;
                slot.outstanding = 0;
                slot.first_error = None;
                slot.keepalive = None;

                match target.dispatch(op, offset, length, tag, &handle) {
                    ZcDispatch::Inline { action, keepalive } => {
                        // Stash keepalive BEFORE pushing chunk SQEs — the
                        // kernel may complete chunks fast enough that
                        // `finalize` runs before this `match` arm returns
                        // (no, that's not possible on this thread, but the
                        // intent is: the gate guard must outlive the
                        // WRITE_FIXED submission, and consumption happens
                        // in `finalize` via the slot field).
                        slot.keepalive = keepalive;
                        match action {
                            ZcAction::Chunks(chunks) => {
                                if chunks.is_empty() {
                                    slot.first_error = Some(-libc::EINVAL);
                                    to_submit.push(finalize(tag, &mut inflight, &target, qid));
                                } else {
                                    #[allow(clippy::cast_possible_truncation)]
                                    { slot.outstanding = chunks.len() as u32; }
                                    for ch in chunks {
                                        to_submit.push(build_chunk_sqe(ch, tag, op));
                                    }
                                }
                            }
                            ZcAction::Complete(r) => {
                                slot.first_error = Some(r);
                                to_submit.push(finalize(tag, &mut inflight, &target, qid));
                            }
                        }
                    }
                    ZcDispatch::Deferred => {
                        // Target will call handle.submit later.
                    }
                }
            }
        }

        push_sqes(&mut ring, &to_submit)?;
    }

    unsafe { libc::munmap(io_cmd_buf, cmd_buf_sz) };
    // `wake_fd` is owned by the caller — don't close it here.
    Ok(())
}
