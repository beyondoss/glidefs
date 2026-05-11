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
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};

use crate::block::handler::BlockHandler;
use ublk_core::io::{UblkDev, UblkQueue, WorkerRing};
use ublk_core::UblkUringData;

use super::device::{drain_eventfd, io_task, signal_eventfd, EventFd, QueueExecutor};

/// Bounded inbox capacity. 64 is comfortable for the AddQueue/RemoveQueue
/// traffic we expect (one device add ≈ `nr_queues` messages).
const WORKER_INBOX_CAPACITY: usize = 64;

/// `WakeupBits` capacity in 64-bit words. 1024 words = 65 536 bits =
/// ~1024 queues × depth=64 tags + slack for daemons. At 16 workers and
/// `nr_queues=4` this supports up to ~4096 devices. Memory cost per
/// worker is ~8 KiB of atomic state — negligible.
///
/// Originally 128 words (8192 bits, ~128 queues/worker). At 1000 devices
/// × 4 queues hashed across 16 workers (~250 queues/worker average) this
/// overflowed and `QueueExecutor::spawn`'s capacity assert panicked,
/// killing the worker thread mid-`AddQueue`. Surfaced during M7 scale
/// bring-up.
const WORKER_WAKEUP_WORDS: usize = 1024;

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

/// Lock-free per-worker counters published by the worker thread after
/// every AddQueue / RemoveQueue / tick that frees a slot. Read by anyone
/// (HTTP `/metrics`, debug-log emitters, scheduling heuristics).
///
/// All reads use `Ordering::Relaxed`; these are observability counters,
/// not synchronization. Inconsistent snapshots (e.g., `used` momentarily
/// higher than `hosted * queue_depth`) are acceptable.
pub(super) struct WorkerStats {
    /// Task slots currently holding a future (live io_tasks + daemons).
    pub used: AtomicUsize,
    /// `WakeupBits` total slot capacity — constant after worker startup.
    pub capacity: AtomicUsize,
    /// Number of ublk queues hosted on this worker (= `hosted.len()`).
    pub hosted_queues: AtomicUsize,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            used: AtomicUsize::new(0),
            capacity: AtomicUsize::new(0),
            hosted_queues: AtomicUsize::new(0),
        }
    }
}

/// Async-side handle to one worker. `Send`.
pub(super) struct WorkerHandle {
    inbox: mpsc::Sender<WorkerMsg>,
    /// Worker's eventfd, cloned here so that every `send` can write to
    /// it — the write generates a CQE on the worker's PollAdd watcher,
    /// which immediately unblocks `io_uring_enter`. Without this, msgs
    /// would sit in the inbox until the next ~250ms timeout fires.
    eventfd: Arc<EventFd>,
    /// Live stats published by the worker thread. Cloned into the
    /// worker so updates and reads share the same Arc'd counters.
    stats: Arc<WorkerStats>,
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
        let stats = Arc::new(WorkerStats::new());
        let stats_for_thread = Arc::clone(&stats);
        let join = std::thread::Builder::new()
            .name(format!("ublk-worker-{idx}"))
            .spawn(move || {
                worker_thread_main(idx, rx, tokio_handle, efd_for_thread, stats_for_thread)
            })?;
        Ok(Self { inbox: tx, eventfd, stats, join: Some(join), idx })
    }

    /// Borrow the live `WorkerStats` for read-only observation.
    #[allow(dead_code)] // wired through pool below
    pub(super) fn stats(&self) -> &Arc<WorkerStats> {
        &self.stats
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
///
/// # Shutdown
///
/// Prefer `shutdown().await` over relying on `Drop`. The `Drop` impl is a
/// safety net only: it drops the inbox senders so workers exit on
/// `Disconnected`, but it does **not** join the threads — joining would
/// block the caller (and a tokio runtime thread, if dropped from async)
/// for up to `WORKER_IDLE_NSEC × num_workers` while workers wait out
/// their `io_uring_enter` timeouts. Use `shutdown()` for a clean,
/// non-blocking join via `spawn_blocking`.
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

    /// Snapshot per-worker capacity / utilization for observability.
    /// Cheap (relaxed atomic loads, no roundtrip to the worker thread).
    /// Returns one `(idx, used, capacity, hosted_queues)` tuple per worker.
    #[allow(dead_code)] // exposed for /metrics + debug logging
    pub(super) fn capacity_snapshot(&self) -> Vec<(usize, usize, usize, usize)> {
        self.workers
            .iter()
            .map(|w| {
                let s = w.stats();
                (
                    w.idx,
                    s.used.load(Ordering::Relaxed),
                    s.capacity.load(Ordering::Relaxed),
                    s.hosted_queues.load(Ordering::Relaxed),
                )
            })
            .collect()
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
    ///
    /// Uses inline FNV-1a rather than `DefaultHasher` because the std
    /// hasher's algorithm is explicitly not stable across Rust versions —
    /// keeping this deterministic means a `(export_name, qid)` pair always
    /// lands on the same worker index given the same `K`, which makes
    /// placement reproducible in tests and across deployments.
    pub(super) fn worker_for(&self, export_name: &str, qid: u16) -> &WorkerHandle {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a 64-bit offset basis
        for &b in export_name.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
        }
        // Mix qid in via a multiplier large enough to distribute across
        // the low bits regardless of name-hash; doesn't need to be a
        // cryptographic PRP, just a permutation.
        let qid_seed: u64 = (qid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let bucket = (h ^ qid_seed) as usize % self.workers.len();
        &self.workers[bucket]
    }

    /// Send `Shutdown` to every worker and join the threads. Acks and
    /// thread joins run concurrently so total shutdown latency is
    /// bounded by the slowest single worker, not by N×latency.
    pub(super) async fn shutdown(mut self) -> std::io::Result<()> {
        let mut done_rxs: Vec<(usize, oneshot::Receiver<()>)> =
            Vec::with_capacity(self.workers.len());
        for w in &self.workers {
            let (done_tx, done_rx) = oneshot::channel();
            let _ = w.send(WorkerMsg::Shutdown { done: done_tx }).await;
            done_rxs.push((w.idx, done_rx));
        }

        // Await every ack in parallel.
        let ack_futs = done_rxs.into_iter().map(|(idx, rx)| async move {
            match rx.await {
                Ok(()) => tracing::debug!(worker = idx, "worker shutdown ack"),
                // Sender dropped without ack — worker exited unexpectedly
                // (panic, channel-close path, etc.). The thread join below
                // will surface the panic itself; this just makes the early
                // exit visible in the shutdown trace.
                Err(_) => tracing::warn!(
                    worker = idx,
                    "worker exited without sending shutdown ack"
                ),
            }
        });
        futures::future::join_all(ack_futs).await;

        // Join every thread in parallel. `JoinHandle::join` blocks, so
        // each goes through its own `spawn_blocking` and we wait for
        // the set with `join_all`.
        let join_futs: Vec<_> = self
            .workers
            .iter_mut()
            .filter_map(|w| w.join.take().map(|j| (w.idx, j)))
            .map(|(idx, join)| {
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = join.join() {
                        tracing::error!(worker = idx, ?e, "worker thread panicked");
                    }
                })
            })
            .collect();
        futures::future::join_all(join_futs).await;

        tracing::info!(count = self.workers.len(), "ublk worker pool shut down");
        Ok(())
    }
}

impl Drop for WorkerPool {
    /// Final safety net: dropping the pool drops every inbox sender (so
    /// each worker's `rx.try_recv()` returns `Disconnected` and the loop
    /// exits). We deliberately do **not** `JoinHandle::join` here —
    /// joining would block the caller for up to
    /// `WORKER_IDLE_NSEC × num_workers` while workers wait out their
    /// `io_uring_enter` timeouts, which is unacceptable if `drop()` runs
    /// on a tokio runtime thread. Callers that need a clean join must
    /// call `shutdown().await` before dropping.
    fn drop(&mut self) {
        let leftover: Vec<usize> = self
            .workers
            .iter()
            .filter(|w| w.join.is_some())
            .map(|w| w.idx)
            .collect();
        if !leftover.is_empty() {
            tracing::warn!(
                workers = ?leftover,
                "WorkerPool dropped without shutdown(); threads will exit on \
                 channel-disconnect but are not joined here to avoid blocking"
            );
        }
        // Senders drop with `self.workers`; JoinHandles drop unjoined.
    }
}

// ---------------------------------------------------------------------------
// Worker thread
// ---------------------------------------------------------------------------

/// A RemoveQueue that has dropped its `hosted` entry but is still
/// waiting for the io_task futures to drop their `Rc<UblkQueue>`
/// clones. We `weak.upgrade().is_none()` after each executor tick to
/// detect when the last clone has been dropped — at which point
/// `UblkQueue::Drop` has *just run* on this thread (it runs wherever
/// the last `Rc` lives, which is the worker thread because the io_task
/// futures and their `Rc` clones never leave it). Drop unmaps the
/// cdev's `io_cmd_buf` and releases the io_uring fixed-file slot —
/// both of which the kernel `UBLK_CMD_DEL_DEV` blocks on. So acking
/// `done` here gates the caller's `del_dev` ioctl on the actual mmap
/// release, not just on the bookkeeping removal.
struct PendingRemove {
    key: QueueKey,
    weak: Weak<UblkQueue>,
    done: Option<oneshot::Sender<()>>,
}

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
    /// RemoveQueue requests that have dropped their `hosted` entry but
    /// are waiting for io_tasks to release their `Rc<UblkQueue>` clones.
    /// Drained after each `tick()`: entries whose `weak.upgrade()`
    /// returns `None` are acked and removed. See `PendingRemove`.
    pending_removes: Vec<PendingRemove>,
    /// Set when a `WorkerMsg::Shutdown` arrives. The main loop exits as
    /// soon as the inbox is fully drained for this iteration.
    shutdown_done: Option<oneshot::Sender<()>>,
    /// Live counters this worker publishes to anyone reading via
    /// `WorkerHandle::stats()`. Updated after AddQueue / RemoveQueue /
    /// every `tick()` so used/hosted_queues reflect the latest state.
    stats: Arc<WorkerStats>,
}

fn worker_thread_main(
    idx: usize,
    mut rx: mpsc::Receiver<WorkerMsg>,
    tokio_handle: tokio::runtime::Handle,
    eventfd: Arc<EventFd>,
    stats: Arc<WorkerStats>,
) {
    // M5: Pin this worker thread to one CPU. With K = num_cpus capped
    // at 32, each worker gets a dedicated CPU on a single-socket box;
    // io_uring submission and CQ processing then stay on-core, the
    // executor's task storage hits L1 cache instead of bouncing
    // between CPUs, and the kernel scheduler stops migrating us.
    //
    // Best-effort: a failure here just means the kernel's default
    // load-balancer continues to manage placement — degraded but
    // correct. Logged as a warning so it's visible.
    //
    // TODO multi-NUMA: on a multi-socket box (`/sys/devices/system/node/online`
    // shows N > 1), partition workers per node by NVMe NUMA node and
    // hash devices to a worker WITHIN their node. For now this lazy
    // round-robin is correct on single-socket and degrades gracefully
    // on multi-socket (~5-10% NVMe-cross-node latency, no correctness
    // hit).
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let cpu = idx % num_cpus;
    // SAFETY:
    // - `cpu_set_t` is a plain C struct with no invalid bit patterns; zero is
    //   the correct initial state and CPU_ZERO is functionally a memset(0).
    // - `set` is stack-allocated, properly aligned, and outlives the
    //   `sched_setaffinity` call (we don't return the address).
    // - `gettid()` always succeeds and returns the current thread's tid, which
    //   is a valid target for the calling process's `sched_setaffinity`.
    // - `size_of::<cpu_set_t>()` matches the kernel's expected mask size on
    //   the libc-supported architectures (x86-64, aarch64).
    // - `cpu < CPU_SETSIZE` invariant for `CPU_SET`: `cpu = idx % num_cpus`
    //   where `num_cpus = available_parallelism()` is bounded by the kernel's
    //   NR_CPUS, which is ≤ CPU_SETSIZE (1024) on all libc-supported archs.
    //   `CPU_SET` writes one bit; out-of-range would be OOB.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let tid = libc::gettid();
        let ret = libc::sched_setaffinity(
            tid,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        );
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            tracing::warn!(
                worker = idx,
                cpu,
                error = %err,
                "sched_setaffinity failed; worker will run with default scheduling"
            );
        } else {
            tracing::debug!(worker = idx, cpu, "worker pinned to CPU");
        }
    }

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
        pending_removes: Vec::new(),
        shutdown_done: None,
        stats: Arc::clone(&stats),
    };
    // Publish the fixed slot capacity once so readers see a sensible
    // value before the first AddQueue.
    state.stats.capacity.store(state.executor.capacity(), Ordering::Relaxed);

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
                    publish_stats(state);
                }
                Ok(WorkerMsg::RemoveQueue { key, done }) => {
                    // Drop the `hosted` map's `Rc<UblkQueue>` clone. The
                    // io_task futures still hold their own clones — they
                    // get dropped only after `kill_dev` fires abort CQEs
                    // and the io_tasks see `QueueIsDown`. Park the ack
                    // in `pending_removes` and gate it on `Weak::upgrade
                    // ().is_none()` after each tick, which is the moment
                    // `UblkQueue::Drop` has run on this thread (it
                    // munmaps `io_cmd_buf` and clears the io_uring
                    // fixed-file slot — both required for `del_dev` to
                    // unblock on the caller side).
                    if let Some(queue_rc) = state.hosted.remove(&key) {
                        let weak = Rc::downgrade(&queue_rc);
                        drop(queue_rc);
                        if weak.upgrade().is_none() {
                            // Already fully gone (no io_tasks captured a
                            // clone — possible after AddQueue failure).
                            let _ = done.send(());
                        } else {
                            state.pending_removes.push(PendingRemove {
                                key,
                                weak,
                                done: Some(done),
                            });
                            tracing::debug!(
                                worker = idx, ?key,
                                "queue removal pending io_task drain"
                            );
                        }
                    } else {
                        // Not hosted — idempotent ack.
                        let _ = done.send(());
                    }
                    publish_stats(state);
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
        let submit_result = ring.with_mut(|r| {
            let args = io_uring::types::SubmitArgs::new().timespec(&ts);
            r.submitter().submit_with_args(to_wait, &args)
        });
        // ETIME is the expected wakeup path when nothing else completed within
        // WORKER_IDLE_NSEC; any other error means the ring is in a degraded
        // state (EAGAIN/EBUSY/EINVAL...) and silently swallowing it would mask
        // a stuck worker behind apparent normal ticking.
        if let Err(e) = submit_result
            && e.raw_os_error() != Some(libc::ETIME)
        {
            tracing::warn!(
                worker = idx,
                error = %e,
                "io_uring submit_with_args failed"
            );
        }

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

        // Tick may have caused io_tasks to complete (`kill_dev` abort
        // CQEs, normal completion, panic). Their captured `Rc<UblkQueue>`
        // clones dropped along with the task slots. For any RemoveQueue
        // that's waiting on its queue to fully die, check whether the
        // last clone is now gone — if so, `UblkQueue::Drop` just ran on
        // this thread (munmapping `io_cmd_buf` and clearing the cdev
        // fixed-file slot), and we can ack the caller. They'll then
        // safely run `del_dev`, which kernel-blocks on exactly those
        // two cleanups.
        state.pending_removes.retain_mut(|pending| {
            if pending.weak.upgrade().is_none() {
                if let Some(done) = pending.done.take() {
                    tracing::debug!(
                        worker = idx,
                        key = ?pending.key,
                        "queue fully drained — acking RemoveQueue"
                    );
                    let _ = done.send(());
                }
                false
            } else {
                true
            }
        });

        // Tick may have freed slots (task completion / panic / shutdown).
        // Refresh `used` so observability reflects the latest state and
        // future preflight `available()` checks see the right headroom
        // through the published counter (handle_add_queue uses the live
        // executor directly, but external readers see this counter).
        publish_stats(state);
    }
}

/// Update the worker's published `WorkerStats` snapshot. Cheap (two
/// relaxed atomic stores). Called after every state-changing event so
/// readers see fresh counters without an extra round-trip to the worker.
///
/// `capacity` is intentionally NOT republished here: the executor's slot
/// capacity (`WORKER_WAKEUP_WORDS × 64`) is fixed at construction and
/// cannot change for the worker's lifetime — it's stored once at startup
/// (see `worker_thread_main`). If executor capacity ever becomes mutable
/// (e.g. dynamic `WakeupBits` resize), update this function to match.
fn publish_stats(state: &WorkerState<'_>) {
    state.stats.used.store(state.executor.used(), Ordering::Relaxed);
    state.stats.hosted_queues.store(state.hosted.len(), Ordering::Relaxed);
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

    // Preflight: reserve `queue_depth` slots in the executor before we
    // construct UblkQueue. If we can't fit all tags, fail the AddQueue
    // cleanly without leaving the kernel device half-attached. The
    // executor's `available()` returns `free_list.len() + (capacity -
    // high_water_mark)` — the count of slots we can spawn into without
    // hitting AtCapacity. Because the worker is single-threaded, this
    // count is stable between here and the final spawn below.
    let needed = queue_depth as usize;
    let avail = state.executor.available();
    if avail < needed {
        tracing::error!(
            worker = worker_idx, ?key,
            needed,
            available = avail,
            capacity = state.executor.capacity(),
            "AddQueue refused — executor cannot fit queue_depth tags"
        );
        let _ = ready.send(Err(format!(
            "worker {worker_idx} at capacity (need {needed} slots, {avail} available of {})",
            state.executor.capacity()
        )));
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
    // pending. The preflight above guarantees every spawn succeeds —
    // an Err here would be a freelist/capacity-accounting bug.
    //
    // Failure mode if the preflight invariant *does* break: the panic
    // propagates up through `block_on` and kills this worker thread.
    // Tasks for tags 0..panic-point are live in the executor holding
    // `Rc<UblkQueue>` clones; they're dropped when the executor is
    // dropped on thread exit, freeing the queue. The kernel device is
    // left half-attached (this worker hosted some of its queues, none
    // of which can now process IO) — the device add as a whole fails
    // because `ready` below never fires. All other queues hosted on
    // this worker are also lost. This is a "should be unreachable"
    // path; if it ever fires, it's a bug in `QueueExecutor::available`.
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
        }).expect("preflight reserved queue_depth slots; spawn must not fail");
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
    async fn pool_drop_without_shutdown_does_not_block() {
        // Drop must NOT join — joining would block the caller (a tokio
        // thread, here) for up to WORKER_IDLE_NSEC per worker. With 2
        // workers at 250ms each, a blocking drop would take ~500ms;
        // we require it to return in well under that.
        let pool = WorkerPool::new(2).expect("spawn pool");
        assert_eq!(pool.num_workers(), 2);
        let start = std::time::Instant::now();
        drop(pool);
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "Drop blocked for {elapsed:?}; should be non-blocking"
        );
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
