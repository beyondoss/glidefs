use crate::io::UblkQueue;
use crate::with_queue_ring_internal;
use crate::with_queue_ring_mut_internal;
use crate::UblkError;
use io_uring::{cqueue, squeue, types, IoUring};
use slab::Slab;
use std::cell::RefCell;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct FutureData {
    waker: Option<Waker>,
    result: Option<i32>,
}

std::thread_local! {
    static MY_SLAB: RefCell<Slab<FutureData>> = RefCell::new(Slab::new());
}

/// User code creates one future with user_data used for submitting
/// uring OP, then future.await returns this uring OP's result.
pub struct UblkUringOpFuture {
    pub user_data: u64,
}

impl UblkUringOpFuture {
    fn get_key(data: u64) -> usize {
        ((data >> 16) & 0xffffffff) as usize
    }
    pub fn new(tgt_io: u64) -> Self {
        MY_SLAB.with(|refcell| {
            let mut map = refcell.borrow_mut();

            let key = map.insert(FutureData {
                waker: None,
                result: None,
            });
            let user_data = ((key as u32) << 16) as u64 | tgt_io;
            log::trace!("uring: new future data {:x}/{:x}", user_data, key);
            UblkUringOpFuture { user_data }
        })
    }

    pub fn new_validate(data: u64) -> Result<Self, UblkError> {
        if Self::get_key(data) != 0 {
            return Err(UblkError::InvalidVal);
        }

        Ok(Self::new(data))
    }
}

impl Future for UblkUringOpFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        MY_SLAB.with(|refcell| {
            let mut map = refcell.borrow_mut();
            let key = Self::get_key(self.user_data);
            match map.get_mut(key) {
                None => {
                    log::trace!("uring: null slab data {:x}/{:x}", self.user_data, key);
                    Poll::Pending
                }
                Some(fd) => match fd.result {
                    Some(result) => {
                        map.remove(key);
                        log::trace!(
                            "uring: uring io ready data {:x}/{:x} ready",
                            self.user_data,
                            key
                        );
                        Poll::Ready(result)
                    }
                    None => {
                        fd.waker = Some(cx.waker().clone());
                        log::trace!(
                            "uring: uring io pending data {:x}/{:x}",
                            self.user_data,
                            key
                        );
                        Poll::Pending
                    }
                },
            }
        })
    }
}

/// Wakeup the pending task. Stores the CQE result and wakes the stored waker.
#[inline]
pub fn ublk_wake_task(data: u64, cqe: &cqueue::Entry) {
    MY_SLAB.with(|refcell| {
        let mut map = refcell.borrow_mut();

        log::trace!(
            "ublk_wake_task: data {:x} user_data {:x} result {}",
            data,
            cqe.user_data(),
            cqe.result()
        );
        let key = UblkUringOpFuture::get_key(data);
        if let Some(fd) = map.get_mut(key) {
            fd.result = Some(cqe.result());
            if let Some(w) = &fd.waker {
                w.wake_by_ref();
            }
        }
    })
}

/// Abstract uring task runner that doesn't depend on specific async executor.
///
/// Drives an event loop: poll io_uring → reap CQEs → run executor → check done.
pub async fn run_uring_tasks<R, I, P, F, W>(
    mut poll_uring: P,
    reap_event_ops: W,
    run_ops: R,
    is_done: I,
) -> Result<(), UblkError>
where
    R: Fn(),
    I: Fn() -> bool,
    P: FnMut() -> F,
    F: std::future::Future<Output = Result<bool, UblkError>>,
    W: Fn(bool) -> Result<bool, UblkError>,
{
    run_ops();
    loop {
        let (poll_timeout, failed) = match poll_uring().await {
            Ok(t) => (t, false),
            _ => (false, true),
        };

        let aborted = reap_event_ops(poll_timeout)?;
        run_ops();

        if (aborted || failed) && is_done() {
            break;
        }
    }
    Ok(())
}

/// Reap completion queue entries and handle them with a custom closure.
pub fn ublk_reap_events_with_handler<T, F>(
    ring: &mut io_uring::IoUring<T>,
    mut cqe_handler: F,
) -> Result<bool, UblkError>
where
    T: io_uring::squeue::EntryMarker,
    F: FnMut(&io_uring::cqueue::Entry),
{
    let mut aborted = false;
    loop {
        match ring.completion().next() {
            Some(cqe) => {
                cqe_handler(&cqe);
                if cqe.result() == crate::sys::UBLK_IO_RES_ABORT {
                    aborted = true;
                }
            }
            _ => break,
        };
    }
    Ok(aborted)
}

/// Reap completion queue entries with queue state update and idle management.
pub fn ublk_reap_io_events_with_update_queue<F>(
    q: &UblkQueue<'_>,
    poll_timeout: bool,
    timeout_data: Option<u64>,
    mut waker_ops: F,
) -> Result<bool, UblkError>
where
    F: FnMut(&io_uring::cqueue::Entry),
{
    crate::io::with_queue_ring_mut_internal!(|ring: &mut IoUring<squeue::Entry>| {
        let mut cmd_cnt = 0u32;
        let mut aborted = false;
        let mut has_timeout = poll_timeout;

        let builtin_closure = |cqe: &io_uring::cqueue::Entry| {
            let user_data = cqe.user_data();

            if let Some(timeout_user_data) = timeout_data {
                log::debug!("Timeout CQE received, result: {}", cqe.result());
                if user_data == timeout_user_data && cqe.result() == -libc::ETIME {
                    has_timeout = true;
                }
            }

            if crate::io::UblkIOCtx::is_io_command(user_data) {
                cmd_cnt += 1;
                if cqe.result() == crate::sys::UBLK_IO_RES_ABORT {
                    aborted = true;
                }
            }

            waker_ops(cqe);
        };

        let result = ublk_reap_events_with_handler(ring, builtin_closure);
        if has_timeout {
            if ring.submission().is_empty() {
                q.enter_queue_idle();
            }
        } else {
            q.exit_queue_idle();
        }

        if cmd_cnt > 0 {
            q.update_state_batch(cmd_cnt, aborted);
        }

        result
    })
}

/// Wait and handle I/O events for a ublk queue.
///
/// High-level convenience wrapper around `run_uring_tasks`.
pub async fn wait_and_handle_io_events<R, I>(
    q: &UblkQueue<'_>,
    idle_secs: Option<u64>,
    run_ops: R,
    is_done: I,
) -> Result<(), UblkError>
where
    R: Fn(),
    I: Fn() -> bool,
{
    let poll_uring = || async {
        let timeout = idle_secs.map(|secs| io_uring::types::Timespec::new().sec(secs));
        uring_poll_io_fn::<io_uring::squeue::Entry>(q, timeout, 1)
    };

    let reap_event = |poll_timeout| {
        ublk_reap_io_events_with_update_queue(q, poll_timeout, None, |cqe| {
            ublk_wake_task(cqe.user_data(), cqe)
        })
    };

    run_uring_tasks(poll_uring, reap_event, run_ops, is_done).await
}

pub(crate) fn uring_poll_fn<T>(
    r: &mut io_uring::IoUring<T>,
    timeout: Option<io_uring::types::Timespec>,
    to_wait: usize,
) -> Result<bool, UblkError>
where
    T: io_uring::squeue::EntryMarker,
{
    let ret = if let Some(ts) = timeout {
        let args = io_uring::types::SubmitArgs::new().timespec(&ts);
        r.submitter().submit_with_args(to_wait, &args)
    } else {
        r.submit_and_wait(to_wait)
    };

    match ret {
        Err(ref err) if err.raw_os_error() == Some(libc::ETIME) => Ok(true),
        Err(err) => Err(UblkError::IOError(err)),
        Ok(_) => Ok(false),
    }
}

pub fn uring_poll_io_fn<T>(
    q: &UblkQueue,
    timeout: Option<io_uring::types::Timespec>,
    to_wait: usize,
) -> Result<bool, UblkError>
where
    T: io_uring::squeue::EntryMarker,
{
    crate::io::with_queue_ring_mut_internal!(|r: &mut IoUring<squeue::Entry>| {
        let stopping = q.is_stopping();
        let res = uring_poll_fn(r, timeout, if stopping { 0 } else { to_wait });
        if stopping {
            Err(UblkError::QueueIsDown)
        } else {
            res
        }
    })
}

#[inline]
pub(crate) fn __ublk_submit_sqe_async(
    sqe: io_uring::squeue::Entry,
    user_data: u64,
) -> Result<UblkUringOpFuture, UblkError> {
    let f = UblkUringOpFuture::new_validate(user_data)?;
    let sqe = sqe.user_data(f.user_data);

    loop {
        let res = with_queue_ring_mut_internal!(|r: &mut IoUring<squeue::Entry>| unsafe {
            r.submission().push(&sqe)
        });

        let _ = match res {
            Ok(_) => break,
            Err(_) => {
                log::debug!("ublk_submit_sqe: flush and retry");
                with_queue_ring_internal!(|r: &IoUring<squeue::Entry>| r.submit_and_wait(0))
            }
        };
    }

    Ok(f)
}

/// Submit an io_uring SQE asynchronously.
///
/// Returns a future that resolves to the CQE result.
pub async fn ublk_submit_sqe_async(
    sqe: io_uring::squeue::Entry,
    user_data: u64,
) -> Result<i32, UblkError> {
    let f = __ublk_submit_sqe_async(sqe, user_data)?;

    Ok(f.await)
}
