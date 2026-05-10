//! Worker pool for the multiplexed ublk transport.
//!
//! Each worker is a dedicated OS thread owning one io_uring + eventfd +
//! [`QueueExecutor`]. Many ublk queues (from many devices) are hosted per
//! worker; their `io_task` futures all coexist in the worker's executor and
//! their submissions all flow through the worker's single `WorkerRing`.
//!
//! # Cross-thread protocol
//!
//! Async-land code interacts with a worker through its [`WorkerHandle`] —
//! `Send` because it's just an `mpsc::Sender + JoinHandle`. The worker
//! thread itself owns `Rc`-backed state (queues, executor, ring) so it is
//! `!Send` by construction; the single-issuer io_uring constraint cannot
//! be violated by accident.
//!
//! Every `WorkerHandle::send` writes to the worker's eventfd in addition
//! to pushing into the channel — that wakes the worker's `io_uring_enter`
//! immediately, so AddQueue/RemoveQueue/Shutdown latency is bounded by
//! one wake hop, not by the submit timeout.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};

use crate::block::handler::BlockHandler;
use ublk_core::io::{UblkDev, UblkQueue, WorkerRing};
use ublk_core::UblkUringData;

use super::device::{drain_eventfd, io_task, signal_eventfd, EventFd, QueueExecutor};

/// Bounded inbox capacity. 64 is comfortable for the AddQueue/RemoveQueue
/// traffic we expect (one device add ≈ `nr_queues` messages).
const WORKER_INBOX_CAPACITY: usize = 64;

/// `WakeupBits` capacity in 64-bit words. 8192 bits = 128 words covers
/// `~256 queues × depth=64 tags + slack for daemons`.
const WORKER_WAKEUP_WORDS: usize = 128;

/// Worker io_uring SQ/CQ depth. Sized so many queues' tags can submit
/// concurrently without spilling. Worker SQs need to fit
/// `nr_queues × depth` outstanding ublk commands plus the eventfd
/// PollAdd; 1024 is generous.
const WORKER_SQ_DEPTH: u32 = 1024;
const WORKER_CQ_DEPTH: u32 = 2048;

/// Time spent in `io_uring_enter` before falling through to drain the
/// inbox / re-tick. With the eventfd fast-wake on `WorkerHandle::send`,
/// this is just an upper bound on idle latency, not the common path.
const WORKER_IDLE_NSEC: u32 = 250_000_000; // 250 ms

/// Stable identifier for a hosted queue. `(dev_id, qid)` is unique
/// across the daemon (the kernel guarantees unique `dev_id`s within
/// glidefs's namespace).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct QueueKey {
    pub dev_id: i32,
    pub qid: u16,
}

/// Cross-thread message to a worker.
pub(super) enum WorkerMsg {
    /// Stop hosting queues, drain the executor, and exit.
    Shutdown { done: oneshot::Sender<()> },
    /// Construct and host a new queue. The worker builds the
    /// [`UblkQueue`] from the given `dev` + its own ring, then spawns
    /// one `io_task` per tag into its executor. `ready` fires when the
    /// queue is fully wired up — at that point the kernel's
    /// `START_DEV` may proceed safely.
    AddQueue {
        dev: Arc<UblkDev>,
        handler: Arc<BlockHandler>,
        qid: u16,
        ready: oneshot::Sender<Result<(), String>>,
    },
    /// Drop the named queue's `Rc<UblkQueue>`. The queue's `io_task`
    /// futures are still running in the executor — they exit naturally
    /// when the kernel signals `UBLK_IO_RES_ABORT` on `kill_dev`. M4
    /// minimal: this is best-effort; M6 makes the drain explicit.
    RemoveQueue {
        key: QueueKey,
        done: oneshot::Sender<()>,
    },
}

/// Async-side handle to one worker. `Send`.
pub(super) struct WorkerHandle {
    inbox: mpsc::Sender<WorkerMsg>,
    /// Worker's eventfd, cloned here so that every `send` can write to
    /// it — the write generates a CQE on the worker's PollAdd watcher,
    /// which immediately unblocks `io_uring_enter`. Without this, msgs
    /// would sit in the inbox until the next ~250ms timeout fires.
    eventfd: Arc<EventFd>,
    join: Option<JoinHandle<()>>,
    pub(super) idx: usize,
}

/// Send-able snapshot of a worker's send-side state for use from
/// `spawn_blocking` contexts where the caller can't borrow
/// `&WorkerPool`. Bundles the inbox sender + eventfd so blocking-side
/// code can `inbox.blocking_send(msg)` then signal the eventfd to wake
/// the worker's `io_uring_enter` immediately.
#[derive(Clone)]
pub(super) struct WorkerHandleSnapshot {
    pub(super) inbox: mpsc::Sender<WorkerMsg>,
    pub(super) eventfd: Arc<EventFd>,
}

impl WorkerHandle {
    fn spawn(
        idx: usize,
        tokio_handle: tokio::runtime::Handle,
    ) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::channel(WORKER_INBOX_CAPACITY);
        // Build the eventfd async-side so we can hand a clone to the
        // handle (for fast-wake on send) and a clone to the worker
        // (for the executor wakers + PollAdd watcher).
        let eventfd = Arc::new(EventFd::new()?);
        let efd_for_thread = Arc::clone(&eventfd);
        let join = std::thread::Builder::new()
            .name(format!("ublk-worker-{idx}"))
            .spawn(move || worker_thread_main(idx, rx, tokio_handle, efd_for_thread))?;
        Ok(Self { inbox: tx, eventfd, join: Some(join), idx })
    }

    /// Send a message and wake the worker's `io_uring_enter`. Returns
    /// `Err` only if the worker has exited and the channel is closed.
    pub(super) async fn send(&self, msg: WorkerMsg) -> Result<(), mpsc::error::SendError<WorkerMsg>> {
        self.inbox.send(msg).await?;
        signal_eventfd(self.eventfd.fd());
        Ok(())
    }
}

/// Pool of K worker threads, fixed-size.
pub(super) struct WorkerPool {
    workers: Vec<WorkerHandle>,
}

impl WorkerPool {
    /// Spawn `num_workers` threads. Caller must be inside a tokio runtime
    /// context (`tokio::runtime::Handle::current()` succeeds) so workers
    /// can re-enter it for `tokio::spawn`-using handler futures.
    pub(super) fn new(num_workers: usize) -> std::io::Result<Self> {
        if num_workers == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "WorkerPool needs at least one worker",
            ));
        }
        let handle = tokio::runtime::Handle::current();
        let mut pool = Self { workers: Vec::with_capacity(num_workers) };
        for i in 0..num_workers {
            match WorkerHandle::spawn(i, handle.clone()) {
                Ok(h) => pool.workers.push(h),
                Err(e) => {
                    tracing::error!(
                        worker = i,
                        error = %e,
                        spawned = pool.workers.len(),
                        "failed to spawn worker; aborting pool init"
                    );
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

    /// Send-able snapshot of `worker_for(export_name, qid)`'s send
    /// side. Used from `spawn_blocking` callers that can't hold a
    /// `&WorkerPool` borrow across the closure.
    pub(super) fn worker_snapshot(&self, export_name: &str, qid: u16) -> WorkerHandleSnapshot {
        let w = self.worker_for(export_name, qid);
        WorkerHandleSnapshot {
            inbox: w.inbox.clone(),
            eventfd: Arc::clone(&w.eventfd),
        }
    }

    /// Pick the worker that should host this `(export_name, qid)`.
    /// Hash by name then xor-rotate by qid so a device's `nr_queues`
    /// queues spread across distinct workers (assuming `K ≥ nr_queues`).
    pub(super) fn worker_for(&self, export_name: &str, qid: u16) -> &WorkerHandle {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        export_name.hash(&mut h);
        // Mix qid in via a multiplier large enough to distribute across
        // the low bits regardless of name-hash; doesn't need to be a
        // cryptographic PRP, just a permutation.
        let qid_seed: u64 = (qid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let bucket = (h.finish() ^ qid_seed) as usize % self.workers.len();
        &self.workers[bucket]
    }

    /// Send `Shutdown` to every worker and join the threads.
    pub(super) async fn shutdown(mut self) -> std::io::Result<()> {
        let mut done_rxs: Vec<(usize, oneshot::Receiver<()>)> =
            Vec::with_capacity(self.workers.len());
        for w in &self.workers {
            let (done_tx, done_rx) = oneshot::channel();
            let _ = w.send(WorkerMsg::Shutdown { done: done_tx }).await;
            done_rxs.push((w.idx, done_rx));
        }
        for (idx, rx) in done_rxs {
            let _ = rx.await;
            tracing::debug!(worker = idx, "worker shutdown ack");
        }
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
    /// Final safety net: dropping the pool drops every inbox sender (so
    /// each worker's `rx.recv()` returns `None` and the loop exits) and
    /// joins the thread.
    fn drop(&mut self) {
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

// ---------------------------------------------------------------------------
// Worker thread
// ---------------------------------------------------------------------------

/// Per-worker state hosted on the worker thread. `!Send` (holds `Rc`).
struct WorkerState<'a> {
    ring: WorkerRing,
    eventfd: Arc<EventFd>,
    executor: QueueExecutor<'a>,
    /// Queues currently hosted on this worker. Each `Rc<UblkQueue>` is
    /// also captured by its tag's `io_task` futures in the executor; the
    /// queue is dropped only when both this map's entry is removed AND
    /// every io_task referencing it has exited.
    hosted: HashMap<QueueKey, Rc<UblkQueue>>,
    /// Set when a `WorkerMsg::Shutdown` arrives. The main loop exits as
    /// soon as the inbox is fully drained for this iteration.
    shutdown_done: Option<oneshot::Sender<()>>,
}

fn worker_thread_main(
    idx: usize,
    mut rx: mpsc::Receiver<WorkerMsg>,
    tokio_handle: tokio::runtime::Handle,
    eventfd: Arc<EventFd>,
) {
    // Build the per-thread io_uring with SINGLE_ISSUER. coop_taskrun is
    // intentionally OMITTED — see device.rs::queue_io_loop for the
    // FETCH_REQ/START mutex deadlock rationale that applies here too.
    let ring = match io_uring::IoUring::builder()
        .setup_cqsize(WORKER_CQ_DEPTH)
        .setup_single_issuer()
        .build(WORKER_SQ_DEPTH)
        .map_err(ublk_core::UblkError::IOError)
        .and_then(WorkerRing::from_io_uring)
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(worker = idx, error = ?e, "failed to init worker io_uring");
            drain_until_shutdown(&mut rx);
            return;
        }
    };

    let mut state = WorkerState {
        ring: ring.clone(),
        eventfd: Arc::clone(&eventfd),
        executor: QueueExecutor::new(WORKER_WAKEUP_WORDS, Arc::clone(&eventfd)),
        hosted: HashMap::new(),
        shutdown_done: None,
    };

    // Tokio handlers (S3 fetches, write_cache flush, etc.) call
    // `tokio::spawn`, `Notify`, etc. The runtime context must be set
    // before any io_task runs.
    let _tokio_guard = tokio_handle.enter();

    // Eventfd watcher daemon — keeps a `PollAdd` registered on the
    // eventfd so cross-thread wakes (executor wakers OR inbox sends)
    // generate a CQE that unblocks `io_uring_enter`.
    {
        let ring_for_daemon = ring.clone();
        let efd_fd = eventfd.fd();
        state.executor.spawn_daemon(async move {
            loop {
                let sqe = io_uring::opcode::PollAdd::new(
                    io_uring::types::Fd(efd_fd),
                    libc::POLLIN as u32,
                )
                .build();
                let result = match ublk_core::uring_async::ublk_submit_sqe_async(
                    &ring_for_daemon,
                    sqe,
                    UblkUringData::Target as u64,
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => break,
                };
                if result < 0 {
                    break;
                }
                drain_eventfd(efd_fd);
            }
        });
    }

    tracing::debug!(worker = idx, "worker thread ready");

    // Main event loop. All async machinery happens inside `block_on`
    // because the io_task futures use `.await`.
    let main_loop = run_worker_loop(idx, &ring, &eventfd, &mut state, &mut rx);
    block_on(main_loop);

    // Fire shutdown ack only after the loop fully unwinds (resources
    // dropped). Async-side caller blocks on this, so it must be the
    // last signal.
    let ack = state.shutdown_done.take();
    drop(state); // drops executor (tasks), hosted queues, eventfd Arc
    if let Some(done) = ack {
        let _ = done.send(());
    }
    tracing::debug!(worker = idx, "worker thread exited");
}

/// Worker main loop. Drains the inbox, drives io_uring, ticks the
/// executor, repeats. Exits on `Shutdown` or channel close.
async fn run_worker_loop(
    idx: usize,
    ring: &WorkerRing,
    eventfd: &Arc<EventFd>,
    state: &mut WorkerState<'_>,
    rx: &mut mpsc::Receiver<WorkerMsg>,
) {
    let _ = eventfd; // captured for lifetime; signaling goes via WorkerHandle::send.

    loop {
        // Drain inbox non-blockingly. Multiple messages can arrive between
        // ticks (especially during a multi-queue device add).
        loop {
            use mpsc::error::TryRecvError;
            match rx.try_recv() {
                Ok(WorkerMsg::Shutdown { done }) => {
                    state.shutdown_done = Some(done);
                    tracing::debug!(worker = idx, "shutdown received");
                    return;
                }
                Ok(WorkerMsg::AddQueue { dev, handler, qid, ready }) => {
                    handle_add_queue(idx, ring, state, dev, handler, qid, ready);
                }
                Ok(WorkerMsg::RemoveQueue { key, done }) => {
                    state.hosted.remove(&key);
                    let _ = done.send(());
                    tracing::debug!(worker = idx, ?key, "queue removed (Rc dropped)");
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    tracing::debug!(worker = idx, "inbox disconnected, exiting");
                    return;
                }
            }
        }

        // Drive io_uring. Block in the kernel for up to WORKER_IDLE_NSEC
        // unless an SQE completes or eventfd fires. With nothing hosted,
        // we still spin a short timeout so Shutdown via channel-close is
        // promptly noticed.
        let to_wait = if state.executor.all_done() { 0 } else { 1 };
        let ts = io_uring::types::Timespec::new().nsec(WORKER_IDLE_NSEC);
        let _submit = ring.with_mut(|r| {
            let args = io_uring::types::SubmitArgs::new().timespec(&ts);
            r.submitter().submit_with_args(to_wait, &args)
        });

        // Reap CQEs and route to futures via slab.
        ring.with_mut(|r| {
            while let Some(cqe) = r.completion().next() {
                ublk_core::uring_async::ublk_wake_task(cqe.user_data(), &cqe);
            }
        });

        // Drain eventfd writes accumulated since last tick — the PollAdd
        // daemon does this on its own awakening, but if we got here via
        // the inbox-send signal alone (no PollAdd CQE yet), we need to
        // clear the counter so the daemon's next PollAdd actually fires
        // on the *next* signal.
        drain_eventfd(eventfd.fd());

        // Run the executor.
        state.executor.tick();
    }
}

fn handle_add_queue(
    worker_idx: usize,
    ring: &WorkerRing,
    state: &mut WorkerState<'_>,
    dev: Arc<UblkDev>,
    handler: Arc<BlockHandler>,
    qid: u16,
    ready: oneshot::Sender<Result<(), String>>,
) {
    let dev_id = dev.dev_info.dev_id as i32;
    let queue_depth = dev.dev_info.queue_depth;
    let key = QueueKey { dev_id, qid };

    if state.hosted.contains_key(&key) {
        let _ = ready.send(Err(format!("queue {key:?} already hosted")));
        return;
    }

    let q = match UblkQueue::new(qid, Arc::clone(&dev), ring) {
        Ok(q) => Rc::new(q),
        Err(e) => {
            let _ = ready.send(Err(format!("UblkQueue::new failed: {e:?}")));
            return;
        }
    };

    // Spawn one io_task per tag. Each future captures `Rc<UblkQueue>`
    // by value, so the queue stays alive as long as any tag's task is
    // pending.
    for tag in 0..queue_depth {
        let q_for_task = q.clone();
        let h_for_task = Arc::clone(&handler);
        state.executor.spawn(async move {
            match io_task(&q_for_task, tag, &h_for_task).await {
                Ok(()) => {}
                Err(ublk_core::UblkError::QueueIsDown) => {
                    // Normal kill_dev / shutdown path.
                }
                Err(e) => {
                    tracing::error!(qid, tag, error = ?e, "ublk io_task failed");
                }
            }
        });
    }

    state.hosted.insert(key, q);
    tracing::debug!(
        worker = worker_idx,
        ?key,
        depth = queue_depth,
        "queue hosted"
    );
    let _ = ready.send(Ok(()));
}

/// Drain the inbox after a startup failure, sending acks where requested
/// so async-side callers don't hang forever.
fn drain_until_shutdown(rx: &mut mpsc::Receiver<WorkerMsg>) {
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            WorkerMsg::Shutdown { done } => {
                let _ = done.send(());
                break;
            }
            WorkerMsg::AddQueue { ready, .. } => {
                let _ = ready.send(Err("worker startup failed".into()));
            }
            WorkerMsg::RemoveQueue { done, .. } => {
                let _ = done.send(());
            }
        }
    }
}

/// Minimal `block_on` for the worker thread. Drives a single future with
/// a thread-park/unpark waker. We don't need a full executor here because
/// the future internally pumps `io_uring_enter`, which is what actually
/// blocks; this is just the outermost driver.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::sync::Arc as StdArc;
    use std::task::{Context, Poll, Wake, Waker};

    struct ThreadWaker(std::thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: StdArc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &StdArc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(StdArc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(future);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => std::thread::park(),
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

    #[tokio::test(flavor = "current_thread")]
    async fn pool_size_zero_returns_invalid_input() {
        match WorkerPool::new(0) {
            Ok(_) => panic!("must reject zero workers"),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pool_drop_without_shutdown_joins_threads() {
        let pool = WorkerPool::new(2).expect("spawn pool");
        assert_eq!(pool.num_workers(), 2);
        drop(pool);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_for_spreads_qids_across_workers() {
        let pool = WorkerPool::new(4).expect("spawn pool");
        // For nr_queues=4 we expect distinct workers per qid for the
        // same export name (assuming the qid_seed actually distributes).
        let mut seen = std::collections::HashSet::new();
        for qid in 0..4u16 {
            seen.insert(pool.worker_for("test-export", qid).idx);
        }
        // Best-effort: ≥ 2 distinct workers across 4 qids. (4/4 distinct
        // is not guaranteed for arbitrary inputs but the seed is chosen
        // to be a permutation; if this fails we want to know.)
        assert!(seen.len() >= 2, "qid_seed didn't spread: only {} workers", seen.len());
        pool.shutdown().await.expect("shutdown");
    }
}
