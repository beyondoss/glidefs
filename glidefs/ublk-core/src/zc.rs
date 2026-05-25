//! Zero-copy queue worker for `UBLK_F_AUTO_BUF_REG + UBLK_F_SUPPORT_ZERO_COPY`.
//!
//! The kernel auto-registers each bio's pages as an io_uring fixed buffer
//! at the slot index packed into the FETCH SQE's `addr` field. The userspace
//! target responds with `IORING_OP_READ_FIXED` / `WRITE_FIXED` against a
//! source/sink file descriptor using `buf_index=tag` — the kernel does the
//! DMA directly between the bio pages and the target file. No userspace
//! memcpy of bio data.
//!
//! Setup (matches `tools/testing/selftests/ublk/kublk.c`):
//! - `IORING_SETUP_COOP_TASKRUN | SINGLE_ISSUER | DEFER_TASKRUN | CQSIZE`
//! - `register_buffers_sparse(queue_depth)` — kernel populates each slot
//!   per-IO via auto_buf_reg
//! - `register_files([cdev_fd])` — cdev as fixed-file slot 0, FETCH SQEs
//!   use `types::Fixed(0)`
//!
//! Without any one of these, the kernel either rejects the FETCH SQEs at
//! submission or aborts them when the queue transitions to LIVE. Discovered
//! the hard way; documented here so the next person doesn't.

use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use io_uring::{cqueue, opcode, squeue, types, IoUring};

use crate::sys;
use crate::UblkError;

/// What the target wants done for one I/O delivered by a FETCH.
///
/// Returned by [`ZcTarget::dispatch`]. The worker translates this into
/// `IORING_OP_*_FIXED` ops against the worker's io_uring (the same ring
/// the kernel auto-registers each bio into).
pub enum ZcAction {
    /// `READ_FIXED(fd, src_offset, length, buf_index=tag)`. Kernel reads
    /// from `fd` at `src_offset` into the auto-registered bio. Used when
    /// the source data already lives at `(fd, src_offset)`.
    ReadFixedFrom { fd: RawFd, src_offset: u64 },
    /// `WRITE_FIXED(fd, dst_offset, length, buf_index=tag)`. Kernel reads
    /// from the auto-registered bio and writes to `fd` at `dst_offset`.
    /// Used to drain bio data to a sink file.
    WriteFixedTo { fd: RawFd, dst_offset: u64 },
    /// Complete the I/O immediately with this result. No data plane op
    /// submitted. Use for FLUSH/DISCARD/WRITE_ZEROES that don't need bio
    /// data movement, or for early errors.
    Complete(i32),
}

/// Target plugged into the ZC worker. `dispatch` is called from the
/// worker's thread when a FETCH delivers I/O; it's `&self` and must be
/// `Send + Sync` because the worker thread accesses it directly.
pub trait ZcTarget: Send + Sync {
    /// Decide what to do for one I/O. `op` is `UBLK_IO_OP_*`,
    /// `offset` is the byte offset in the device, `length` is the
    /// request size in bytes, `tag` is the I/O slot (also the
    /// auto_buf_reg index for this I/O).
    ///
    /// May block — the worker is single-threaded so a slow dispatch
    /// holds up the queue's other inflight I/Os. Keep it fast (e.g.,
    /// hot-cache lookup, fall through to ZC), or pre-warm before
    /// returning so the kernel-side data plane is hot.
    fn dispatch(&self, op: u8, offset: u64, length: u32, tag: u16) -> ZcAction;

    /// Called after a `WriteFixedTo` data-plane SQE completes, before the
    /// kernel commits the I/O to the bio originator. Use this to update
    /// any metadata (dirty bits, WAL entries, etc) that depends on the
    /// write having landed. Return the result to deliver to the kernel
    /// (`result` from the data-plane SQE by default).
    fn after_write(&self, _op: u8, _offset: u64, _length: u32, _tag: u16, result: i32) -> i32 {
        result
    }

    /// Called after a `ReadFixedFrom` data-plane SQE completes. Symmetric
    /// to `after_write`. Default: pass through.
    fn after_read(&self, _op: u8, _offset: u64, _length: u32, _tag: u16, result: i32) -> i32 {
        result
    }
}

const UD_TARGET_IO: u64 = 1 << 63;

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

/// Run a zero-copy queue worker until `stop` is set.
///
/// `cdev_fd` is the per-device char device file descriptor (typically
/// from `UblkDev::tgt.fds[0]`). The worker:
/// - Builds its own io_uring with the kernel-required setup flags
/// - Registers sparse buffers (sized for `queue_depth`) and the cdev as
///   fixed-file slot 0
/// - mmaps the kernel's io_cmd_buf for this queue
/// - Arms `queue_depth` initial FETCH SQEs with `ublk_auto_buf_reg.index=tag`
/// - Loops: drain CQEs, dispatch each I/O to `target`, translate the
///   `ZcAction` into a `READ_FIXED`/`WRITE_FIXED` SQE (or immediate
///   COMMIT), wait for completion, submit `COMMIT_AND_FETCH`
///
/// Returns when `stop` is true and the kernel has aborted in-flight
/// FETCHes (post-`stop_dev`).
pub fn run_zc_queue(
    cdev_fd: RawFd,
    qid: u16,
    queue_depth: u16,
    target: Arc<dyn ZcTarget>,
    stop: Arc<AtomicBool>,
) -> Result<(), UblkError> {
    if queue_depth == 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    let mut ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .setup_cqsize(u32::from(queue_depth) * 4)
        .build(u32::from(queue_depth) * 4)
        .map_err(UblkError::IOError)?;

    ring.submitter()
        .register_buffers_sparse(u32::from(queue_depth))
        .map_err(UblkError::IOError)?;
    ring.submitter()
        .register_files(&[cdev_fd])
        .map_err(UblkError::IOError)?;

    // mmap the kernel's io_cmd_buf for this queue. Each tag's
    // `ublksrv_io_desc` lives at io_cmd_buf + tag * sizeof(desc).
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
        return Err(UblkError::IOError(std::io::Error::last_os_error()));
    }

    // Arm initial FETCHes.
    {
        let mut sq = ring.submission();
        for tag in 0..queue_depth {
            let sqe = build_fetch_sqe(qid, tag, -1);
            unsafe { sq.push(&sqe) }.map_err(|_| UblkError::OtherError(-libc::EAGAIN))?;
        }
    }
    ring.submit().map_err(UblkError::IOError)?;

    // Per-tag in-flight context — captured at FETCH dispatch, consumed
    // when the data-plane CQE arrives so we can call `after_read/write`
    // with the original (op, offset, length).
    #[derive(Clone, Copy, Default)]
    struct InFlight {
        op: u8,
        offset: u64,
        length: u32,
    }
    let mut inflight: Vec<InFlight> = vec![InFlight::default(); queue_depth as usize];
    let mut aborted = 0usize;

    loop {
        if stop.load(Ordering::SeqCst) {
            match ring.submit_and_wait(0) {
                Ok(_) => {}
                Err(_) => break,
            }
            let cqes: Vec<cqueue::Entry> = ring.completion().collect();
            if cqes.is_empty() && aborted > 0 {
                break;
            }
            for _cqe in cqes {
                aborted += 1;
            }
            if aborted >= queue_depth as usize {
                break;
            }
            continue;
        }

        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
                unsafe { libc::munmap(io_cmd_buf, cmd_buf_sz) };
                return Err(UblkError::IOError(e));
            }
        }

        let mut to_submit: Vec<squeue::Entry> = Vec::new();
        let cqes: Vec<cqueue::Entry> = ring.completion().collect();
        for cqe in cqes {
            let ud = cqe.user_data();
            let tag = ud_tag(ud);
            let res = cqe.result();

            if ud_is_data(ud) {
                // Data-plane SQE completed. Let the target do any
                // post-completion metadata work before we send the result
                // back to the kernel via COMMIT_AND_FETCH.
                let ctx = inflight[tag as usize];
                let final_res = match u32::from(ctx.op) {
                    sys::UBLK_IO_OP_WRITE => {
                        target.after_write(ctx.op, ctx.offset, ctx.length, tag, res)
                    }
                    sys::UBLK_IO_OP_READ => {
                        target.after_read(ctx.op, ctx.offset, ctx.length, tag, res)
                    }
                    _ => res,
                };
                to_submit.push(build_commit_sqe(qid, tag, final_res));
            } else if res == sys::UBLK_IO_RES_ABORT as i32 || res < 0 {
                // Kernel aborted this tag (or some other negative
                // result). With stop=false this shouldn't happen on a
                // healthy device. Drop the tag silently.
                continue;
            } else {
                let iod = unsafe {
                    &*((io_cmd_buf as *const u8).add(
                        tag as usize * std::mem::size_of::<sys::ublksrv_io_desc>(),
                    ) as *const sys::ublksrv_io_desc)
                };
                let op = (iod.op_flags & 0xff) as u8;
                let offset = iod.start_sector << 9;
                let length: u32 = (u64::from(iod.nr_sectors) * 512) as u32;
                inflight[tag as usize] = InFlight { op, offset, length };

                let action = target.dispatch(op, offset, length, tag);
                match action {
                    ZcAction::ReadFixedFrom { fd, src_offset } => {
                        to_submit.push(
                            opcode::ReadFixed::new(
                                types::Fd(fd),
                                std::ptr::null_mut(),
                                length,
                                tag,
                            )
                            .offset(src_offset)
                            .build()
                            .user_data(ud_data(tag, op)),
                        );
                    }
                    ZcAction::WriteFixedTo { fd, dst_offset } => {
                        to_submit.push(
                            opcode::WriteFixed::new(
                                types::Fd(fd),
                                std::ptr::null(),
                                length,
                                tag,
                            )
                            .offset(dst_offset)
                            .build()
                            .user_data(ud_data(tag, op)),
                        );
                    }
                    ZcAction::Complete(result) => {
                        to_submit.push(build_commit_sqe(qid, tag, result));
                    }
                }
            }
        }

        if !to_submit.is_empty() {
            let mut sq = ring.submission();
            for sqe in &to_submit {
                if unsafe { sq.push(sqe) }.is_err() {
                    drop(sq);
                    ring.submit().map_err(UblkError::IOError)?;
                    let mut sq2 = ring.submission();
                    unsafe { sq2.push(sqe) }
                        .map_err(|_| UblkError::OtherError(-libc::EAGAIN))?;
                    sq = sq2;
                }
            }
        }
    }

    unsafe { libc::munmap(io_cmd_buf, cmd_buf_sz) };
    Ok(())
}
