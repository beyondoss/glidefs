//! Worker pool for the zero-copy ublk transport.
//!
//! The user-copy path multiplexes every queue onto `K` worker threads
//! ([`super::worker_pool`]). The zero-copy path needs its own ring per
//! worker — the auto-registered bio buffers live in the ring's registered-
//! buffer table, and the ring is set up `DEFER_TASKRUN`, neither of which
//! the user-copy worker ring is — so it gets its own `K` threads, one per
//! user-copy worker, paired by index. A ZC queue's hot path therefore runs
//! on a thread pinned to the CPU of the worker its `(export, qid)` hash
//! selected, with the same NUMA placement.
//!
//! Threads start lazily: a host that never sees a zero-copy device (a 6.12
//! kernel) never creates one. Once started a thread serves up to
//! [`ZC_MAX_QUEUES`] queues; placement spills to the next worker (same node
//! first) when the preferred one is full.
//!
//! Host-level bounds, independent of device count:
//! - threads: `K` (`<= 32`) ZC workers, plus the shared dispatch runtime
//! - io_uring rings: `K`
//! - io-wq workers: `K * (ZC_IOWQ_BOUNDED + ZC_IOWQ_UNBOUNDED)`
//! - hosted queues: `K * ZC_MAX_QUEUES` (8192 at `K = 32`)

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use tokio::sync::oneshot;
use ublk_core::UblkError;
use ublk_core::zc::{
    ZC_MAX_QUEUES, ZcQueueSpec, ZcTarget, ZcWorkerHandle, run_zc_worker, zc_worker_channel,
};

struct Started {
    handle: ZcWorkerHandle,
    join: Option<JoinHandle<()>>,
}

struct Slot {
    cpu: usize,
    node: usize,
    /// Queues reserved on (or hosted by) this worker. Reserved before the
    /// add message is sent so concurrent adds cannot overshoot the ring's
    /// table; released when the queue is done or never made it.
    queues: Arc<AtomicUsize>,
    started: Mutex<Option<Started>>,
}

/// Pool of lazily started ZC worker threads, indexed like the user-copy
/// [`super::worker_pool::WorkerPool`].
pub(super) struct ZcPool {
    slots: Vec<Slot>,
    next_key: AtomicU64,
    /// Queues one worker accepts: [`ZC_MAX_QUEUES`] (the ring's table), or
    /// fewer in tests that exercise the full-pool path.
    max_queues: usize,
}

/// A queue placed on a ZC worker.
pub(super) struct ZcTicket {
    pub(super) worker_idx: usize,
    pub(super) worker: ZcWorkerHandle,
    pub(super) key: u64,
    /// Resolves once the worker has armed the queue's FETCHes, or with why
    /// it could not.
    pub(super) ready: oneshot::Receiver<Result<(), String>>,
    /// Resolves when the queue has ended and its slot is released, or when
    /// the worker exits.
    pub(super) done: oneshot::Receiver<()>,
}

impl ZcPool {
    /// `layout[i]` is `(node_index, cpu)` of user-copy worker `i`.
    pub(super) fn new(layout: &[(usize, usize)]) -> Self {
        Self {
            slots: layout
                .iter()
                .map(|&(node, cpu)| Slot {
                    cpu,
                    node,
                    queues: Arc::new(AtomicUsize::new(0)),
                    started: Mutex::new(None),
                })
                .collect(),
            next_key: AtomicU64::new(1),
            max_queues: ZC_MAX_QUEUES,
        }
    }

    /// Lower the per-worker queue limit (never above the ring's real limit).
    pub(super) fn with_max_queues(mut self, max_queues: usize) -> Self {
        self.max_queues = max_queues.min(ZC_MAX_QUEUES);
        self
    }

    /// Workers that have been started (threads that exist).
    #[cfg(test)]
    pub(super) fn started_workers(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.started.lock().unwrap().is_some())
            .count()
    }

    /// Queues currently placed across all workers.
    #[cfg(test)]
    pub(super) fn hosted_queues(&self) -> usize {
        self.slots
            .iter()
            .map(|s| s.queues.load(Ordering::Relaxed))
            .sum()
    }

    /// Placement order for a queue whose hash picked `preferred`: that
    /// worker, then the rest of its node, then every other node.
    fn candidates(&self, preferred: usize) -> impl Iterator<Item = usize> + '_ {
        let n = self.slots.len();
        let preferred = preferred % n;
        let node = self.slots[preferred].node;
        let rotated = move || (0..n).map(move |i| (preferred + i) % n);
        rotated()
            .filter(move |&i| self.slots[i].node == node)
            .chain(rotated().filter(move |&i| self.slots[i].node != node))
    }

    /// Reserve a queue slot, trying `preferred`'s worker, then the rest of
    /// its node, then every other node. `None` only when every worker's
    /// ring is full (`K * ZC_MAX_QUEUES` queues host-wide).
    fn reserve(&self, preferred: usize) -> Option<usize> {
        self.candidates(preferred).find(|&idx| {
            let queues = &self.slots[idx].queues;
            if queues.fetch_add(1, Ordering::AcqRel) >= self.max_queues {
                queues.fetch_sub(1, Ordering::AcqRel);
                false
            } else {
                true
            }
        })
    }

    /// Place a queue on a ZC worker (starting its thread if needed).
    ///
    /// Synchronous so the caller can refuse the queue *before* the device
    /// starts: a queue that cannot be placed fails the export with an error,
    /// instead of leaving the kernel's `START_DEV` waiting forever for
    /// FETCHes that will never arrive.
    pub(super) fn add_queue(
        &self,
        preferred: usize,
        cdev_fd: std::os::fd::RawFd,
        qid: u16,
        queue_depth: u16,
        target: Arc<dyn ZcTarget>,
    ) -> Result<ZcTicket, UblkError> {
        let Some(idx) = self.reserve(preferred) else {
            tracing::error!(
                workers = self.slots.len(),
                capacity = self.slots.len() * self.max_queues,
                "ZC worker pool is full; refusing queue"
            );
            return Err(UblkError::OtherError(-libc::ENOSPC));
        };
        let slot = &self.slots[idx];
        let worker = match self.worker(idx) {
            Ok(w) => w,
            Err(e) => {
                slot.queues.fetch_sub(1, Ordering::AcqRel);
                return Err(UblkError::IOError(e));
            }
        };
        let key = self.next_key.fetch_add(1, Ordering::Relaxed);
        let (ready_tx, ready) = oneshot::channel();
        let (done_tx, done) = oneshot::channel();
        let queues = Arc::clone(&slot.queues);
        worker.add_queue(ZcQueueSpec {
            key,
            cdev_fd,
            qid,
            queue_depth,
            target,
            ready: Box::new(move |r| {
                let _ = ready_tx.send(r.map_err(|e| format!("{e:?}")));
            }),
            done: Box::new(move |_| {
                queues.fetch_sub(1, Ordering::AcqRel);
                let _ = done_tx.send(());
            }),
        });
        Ok(ZcTicket {
            worker_idx: idx,
            worker,
            key,
            ready,
            done,
        })
    }

    /// ZC worker slots (started or not).
    #[cfg(test)]
    pub(super) fn num_slots(&self) -> usize {
        self.slots.len()
    }

    /// Queues reserved on worker `idx`.
    #[cfg(test)]
    pub(super) fn queues_on(&self, idx: usize) -> usize {
        self.slots[idx].queues.load(Ordering::Relaxed)
    }

    /// Reserve without starting a worker (placement-only tests).
    #[cfg(test)]
    pub(super) fn reserve_for_test(&self, preferred: usize) -> Option<usize> {
        self.reserve(preferred)
    }

    fn worker(&self, idx: usize) -> std::io::Result<ZcWorkerHandle> {
        let slot = &self.slots[idx];
        let mut started = slot.started.lock().unwrap();
        if let Some(s) = started.as_ref() {
            return Ok(s.handle.clone());
        }
        let (handle, inbox) = zc_worker_channel()?;
        let cpu = slot.cpu;
        let join = std::thread::Builder::new()
            .name(format!("glidefs-zc-w{idx}"))
            .spawn(move || {
                pin_to_cpu(idx, cpu);
                run_zc_worker(inbox);
            })?;
        *started = Some(Started {
            handle: handle.clone(),
            join: Some(join),
        });
        tracing::info!(worker = idx, cpu, "ublk ZC worker started");
        Ok(handle)
    }

    /// Ask every started worker to exit, without waiting.
    fn signal_shutdown(&self) -> Vec<(usize, JoinHandle<()>)> {
        let mut joins = Vec::new();
        for (idx, slot) in self.slots.iter().enumerate() {
            if let Some(s) = slot.started.lock().unwrap().as_mut() {
                s.handle.shutdown();
                if let Some(j) = s.join.take() {
                    joins.push((idx, j));
                }
            }
        }
        joins
    }

    /// Stop every started worker and wait for its ring to close. The kernel
    /// moves a device LIVE -> QUIESCED when the last ring touching its cdev
    /// closes, so this is the synchronisation point a daemon handoff needs.
    pub(super) async fn shutdown(&self) {
        let joins = self.signal_shutdown();
        let futs = joins.into_iter().map(|(idx, j)| {
            tokio::task::spawn_blocking(move || {
                if j.join().is_err() {
                    tracing::error!(worker = idx, "ZC worker thread panicked");
                }
            })
        });
        futures::future::join_all(futs).await;
    }
}

impl Drop for ZcPool {
    /// Safety net, as for the user-copy pool: signal, never join (the
    /// caller may be a tokio thread).
    fn drop(&mut self) {
        let _ = self.signal_shutdown();
    }
}

fn pin_to_cpu(idx: usize, cpu: usize) {
    // SAFETY: `cpu_set_t` is plain data, zero is its valid empty state;
    // `cpu` comes from sysfs cpulists (< CPU_SETSIZE); `gettid` always
    // succeeds and names this thread.
    let ret = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(libc::gettid(), std::mem::size_of::<libc::cpu_set_t>(), &set)
    };
    if ret != 0 {
        tracing::warn!(
            worker = idx,
            cpu,
            error = %std::io::Error::last_os_error(),
            "ZC worker sched_setaffinity failed; running unpinned"
        );
    }
}
