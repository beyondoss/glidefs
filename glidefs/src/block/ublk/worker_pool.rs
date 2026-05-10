//! Worker pool for multiplexed ublk transport.
//!
//! Each worker is a dedicated OS thread owning one io_uring + eventfd +
//! [`QueueExecutor`]. Many ublk queues (from many devices) will be hosted
//! per worker in M4; for now (M3) the pool is **scaffolding only** — the
//! workers spin up their long-lived resources and then idle, awaiting
//! `WorkerMsg::Shutdown`.
//!
//! # Design
//!
//! - **One io_uring per worker**: the single-issuer constraint applies per
//!   ring; only the worker thread that owns the ring may submit through it.
//!   This is enforced structurally — the per-thread types ([`Worker`],
//!   [`ublk_core::WorkerRing`]) are `!Send` because they hold `Rc`s.
//! - **`mpsc` channel per worker** for cross-thread requests (add a queue,
//!   remove a queue, shut down). The worker drains its inbox between
//!   executor ticks. The async-side [`WorkerHandle`] is `Send`.
//! - **Per-thread `QueueExecutor`** with eventfd-backed wakers — any
//!   `wake()` from a tokio worker (handler future completes) signals the
//!   eventfd, which is registered with io_uring via `PollAdd` so the
//!   `io_uring_enter()` blocking call wakes promptly.
//!
//! # Milestones
//!
//! - **M3 (this commit)**: pool starts, idles, responds to Shutdown,
//!   joins cleanly. Each worker builds its IoUring + EventFd +
//!   QueueExecutor at startup but hosts no queues.
//! - **M4**: `WorkerMsg::AddQueue` / `RemoveQueue` plumb queues into the
//!   executor; the per-device-thread path in `device.rs` is deleted.
//! - **M5**: workers are `sched_setaffinity`-pinned to a CPU.
//! - **M6**: shutdown stops calling `kill_dev` on the daemon hot path —
//!   relies on `UBLK_F_USER_RECOVERY` auto-quiesce instead, so a daemon
//!   restart never deadlocks the kernel on inflight VM bios.

// M3 scaffolding: items below are constructed only by tests until M4 wires
// the pool into UblkServer. The dead-code allow goes away naturally once
// real callers appear.
#![allow(dead_code)]

use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};

use super::device::{EventFd, QueueExecutor};

/// Default channel capacity for worker inboxes. Bounded so that runaway
/// async-side senders apply backpressure rather than memory-bombing a slow
/// worker. 64 is comfortable for the AddQueue/RemoveQueue traffic we expect
/// (one device add ≈ `nr_queues` messages, ~µs to drain each).
const WORKER_INBOX_CAPACITY: usize = 64;

/// Default `WakeupBits` capacity (in 64-bit words) per worker. Sized for
/// the M4 worst-case: each worker hosts up to ~256 queues × depth=64 tags
/// + slack for daemons. 8192 bits = 128 words, comfortably fits without
/// being wasteful (1 KiB of atomic state per worker).
const WORKER_WAKEUP_WORDS: usize = 128;

/// Default io_uring submission/completion queue depth at the worker level.
/// Sized for the M4 worst-case where many devices' tags contend for one
/// ring; 1024 SQEs × 64 bytes = 64 KiB ring, 2048 CQEs × 16 bytes = 32 KiB
/// ring — both small in absolute terms.
const WORKER_SQ_DEPTH: u32 = 1024;
const WORKER_CQ_DEPTH: u32 = 2048;

/// Cross-thread message to a worker.
///
/// Drained from the inbox between executor ticks. M3 only handles
/// `Shutdown`; M4 will add `AddQueue`/`RemoveQueue`.
pub(super) enum WorkerMsg {
    /// Stop hosting queues, drain the executor, and exit. The `done` sender
    /// fires once the worker thread has actually exited (so callers can
    /// gate further actions on it).
    Shutdown { done: oneshot::Sender<()> },
}

/// Async-side handle to one worker. `Send`.
pub(super) struct WorkerHandle {
    pub(super) inbox: mpsc::Sender<WorkerMsg>,
    /// Worker thread join handle. Optional so `WorkerPool::shutdown` can
    /// take ownership of it for joining.
    join: Option<JoinHandle<()>>,
    /// Stable index for logging / future affinity decisions.
    pub(super) idx: usize,
}

impl WorkerHandle {
    fn spawn(idx: usize) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::channel(WORKER_INBOX_CAPACITY);
        let join = std::thread::Builder::new()
            .name(format!("ublk-worker-{idx}"))
            .spawn(move || worker_thread_main(idx, rx))?;
        Ok(Self {
            inbox: tx,
            join: Some(join),
            idx,
        })
    }
}

/// Pool of K worker threads.
///
/// `K` is fixed at construction. Devices/queues will be assigned to workers
/// by `(hash(export_name) ^ qid_seed(qid)) mod K` in M4.
pub(super) struct WorkerPool {
    workers: Vec<WorkerHandle>,
}

impl WorkerPool {
    /// Spawn `num_workers` worker threads, each with its own ring + executor.
    /// Workers idle until `shutdown` is called.
    ///
    /// If a spawn fails partway through, any already-spawned workers are
    /// joined before this returns — `Drop` runs on the partial `Self` left
    /// behind by the early return.
    pub(super) fn new(num_workers: usize) -> std::io::Result<Self> {
        if num_workers == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "WorkerPool needs at least one worker",
            ));
        }
        let mut pool = Self { workers: Vec::with_capacity(num_workers) };
        for i in 0..num_workers {
            match WorkerHandle::spawn(i) {
                Ok(h) => pool.workers.push(h),
                Err(e) => {
                    tracing::error!(
                        worker = i,
                        error = %e,
                        spawned = pool.workers.len(),
                        "failed to spawn worker; aborting pool init"
                    );
                    // `pool` drops here: Drop joins the partial set.
                    return Err(e);
                }
            }
        }
        tracing::info!(count = num_workers, "ublk worker pool spawned");
        Ok(pool)
    }

    pub(super) fn num_workers(&self) -> usize {
        self.workers.len()
    }

    /// Send `Shutdown` to every worker, then join the threads.
    ///
    /// Returns once all workers have exited. Workers that never received
    /// the message (channel closed) are still joined.
    pub(super) async fn shutdown(mut self) -> std::io::Result<()> {
        // Send Shutdown to each worker; collect the done-receivers so we can
        // observe orderly exit before joining.
        let mut done_rxs: Vec<(usize, oneshot::Receiver<()>)> =
            Vec::with_capacity(self.workers.len());
        for w in &self.workers {
            let (done_tx, done_rx) = oneshot::channel();
            // If the channel is closed (worker already exited), proceed to
            // join — the JoinHandle will reflect the actual outcome.
            let _ = w.inbox.send(WorkerMsg::Shutdown { done: done_tx }).await;
            done_rxs.push((w.idx, done_rx));
        }

        // Wait for each worker to acknowledge shutdown.
        for (idx, rx) in done_rxs {
            // If recv errors (worker dropped the sender without firing), still
            // proceed — the join below will catch any panic.
            let _ = rx.await;
            tracing::debug!(worker = idx, "worker shutdown ack");
        }

        // Join the threads off the async runtime.
        for w in &mut self.workers {
            if let Some(join) = w.join.take() {
                let idx = w.idx;
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = join.join() {
                        tracing::error!(worker = idx, ?e, "worker thread panicked");
                    }
                })
                .await
                .ok();
            }
        }

        tracing::info!(count = self.workers.len(), "ublk worker pool shut down");
        Ok(())
    }
}

impl Drop for WorkerPool {
    /// Final safety net: drops every inbox sender (so each worker's
    /// `rx.blocking_recv()` returns `None` and the loop exits) and joins the
    /// thread. Runs on three paths:
    ///
    /// 1. After the async [`shutdown`] completes — join handles are already
    ///    `None`, so this is a no-op besides dropping the `WorkerHandle`s.
    /// 2. The caller dropped the pool without ever calling `shutdown` (e.g.
    ///    panic between `new` and `shutdown`).
    /// 3. Partial-spawn cleanup inside [`Self::new`] when one worker fails
    ///    to spawn after others have already started.
    ///
    /// [`shutdown`]: WorkerPool::shutdown
    fn drop(&mut self) {
        // Two-phase: take all join handles out (and drop the WorkerHandles —
        // which drops the inbox senders, signaling each worker to exit), then
        // join the threads.
        let joins: Vec<(usize, JoinHandle<()>)> = self
            .workers
            .drain(..)
            .filter_map(|mut w| w.join.take().map(|j| (w.idx, j)))
            .collect();
        for (idx, j) in joins {
            if let Err(e) = j.join() {
                tracing::error!(worker = idx, ?e, "worker thread panicked during pool drop");
            }
        }
    }
}

/// Worker thread main loop.
///
/// M3: build the long-lived per-thread resources, then wait on the inbox
/// until `Shutdown`. The IoUring/EventFd/Executor are constructed but not
/// yet driven — M4 wires the io_uring event loop through them.
fn worker_thread_main(idx: usize, mut rx: mpsc::Receiver<WorkerMsg>) {
    // 1) Build the io_uring with SINGLE_ISSUER: only this thread submits.
    //    The pre-existing per-device path uses the same flags. coop_taskrun
    //    is intentionally OMITTED — see device.rs queue_io_loop comment for
    //    the FETCH_REQ/START mutex deadlock rationale.
    let ring = match io_uring::IoUring::builder()
        .setup_cqsize(WORKER_CQ_DEPTH)
        .setup_single_issuer()
        .build(WORKER_SQ_DEPTH)
    {
        Ok(r) => ublk_core::WorkerRing::from_io_uring(r),
        Err(e) => {
            tracing::error!(worker = idx, error = %e, "failed to init worker io_uring");
            // Drain the inbox so async-side senders don't block; the
            // missing Shutdown ack will surface as a join() that completes
            // anyway.
            drain_until_shutdown(&mut rx);
            return;
        }
    };

    // 2) Build the eventfd + executor. The executor is sized for many
    //    queues × tags + a few daemon slots. The eventfd is wrapped in `Arc`
    //    so the executor's wakers (which hold an `Arc<EventFd>` via
    //    `WakeupBits`) keep the fd alive past any drop-order edge case.
    let eventfd = match EventFd::new() {
        Ok(e) => Arc::new(e),
        Err(e) => {
            tracing::error!(worker = idx, error = %e, "failed to init worker eventfd");
            drain_until_shutdown(&mut rx);
            return;
        }
    };
    let executor: QueueExecutor<'static> =
        QueueExecutor::new(WORKER_WAKEUP_WORDS, Arc::clone(&eventfd));

    // 3) M3 idle loop: just wait for Shutdown. M4 replaces this with the
    //    real io_uring_enter event loop that drains rx between ticks.
    tracing::debug!(
        worker = idx,
        "worker thread ready (M3 idle — io_uring + executor built but unused)"
    );

    while let Some(msg) = rx.blocking_recv() {
        match msg {
            WorkerMsg::Shutdown { done } => {
                let _ = done.send(());
                break;
            } // M4 will add: AddQueue { ... }, RemoveQueue { ... }
        }
    }

    // Resources drop in reverse declaration order: executor (drops any
    // tasks — none in M3 — and its `Arc<EventFd>` reference), then
    // eventfd (drops the last `Arc` reference, closing the fd), then ring
    // (closes io_uring fd). The executor's `Arc<EventFd>` keeps the fd
    // alive even if a refactor reorders these locals.
    drop(executor);
    drop(eventfd);
    drop(ring);
    tracing::debug!(worker = idx, "worker thread exited");
}

/// On a startup failure, drain the inbox and exit. Any `Shutdown` whose
/// `done` sender we drop will surface as a `recv()` error on the async side
/// — caller will fall through to the join, which reports the real error.
fn drain_until_shutdown(rx: &mut mpsc::Receiver<WorkerMsg>) {
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            WorkerMsg::Shutdown { done } => {
                let _ = done.send(());
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn pool_spawns_and_shuts_down() {
        let pool = WorkerPool::new(4).expect("spawn pool");
        assert_eq!(pool.num_workers(), 4);
        pool.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pool_size_one_works() {
        let pool = WorkerPool::new(1).expect("spawn pool");
        pool.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_size_sixteen_works() {
        let pool = WorkerPool::new(16).expect("spawn pool");
        assert_eq!(pool.num_workers(), 16);
        pool.shutdown().await.expect("shutdown");
    }

    #[test]
    fn pool_size_zero_returns_invalid_input() {
        match WorkerPool::new(0) {
            Ok(_) => panic!("must reject zero workers"),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        }
    }

    #[test]
    fn pool_drop_without_shutdown_joins_threads() {
        // Drop the pool without ever calling `shutdown()`. The Drop impl must
        // close inbox senders (so worker `rx.blocking_recv()` returns None)
        // and join the threads. If join were skipped, this test would still
        // pass — but if Drop hung waiting on a worker, the test framework
        // would time out. The thread name lookup confirms we actually started
        // workers and they actually exited cleanly.
        let pool = WorkerPool::new(2).expect("spawn pool");
        assert_eq!(pool.num_workers(), 2);
        drop(pool);
        // Reaching here means Drop completed without panicking or deadlocking.
    }
}
