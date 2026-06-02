//! Per-worker fixed-size bounce buffer pool for the USER_COPY io_task path.
//!
//! Sized to bound system-wide bounce RSS regardless of device count. One pool
//! per worker thread, mmap-backed (not jemalloc) so the upper limit is a true
//! constant rather than an elastic heap. LIFO free-list keeps recently-released
//! slots hot in L1/L2.
//!
//! With `K` workers × `POOL_SLOTS` × `SLOT_SIZE`, the total bounce footprint
//! is fixed. At the defaults (K=16, slots=256, slot=128 KB) that's **512 MB
//! system-wide** — vs ~20 GB for the per-tag-stable bounce protocol at 5k
//! devices.
//!
//! The size 256-per-worker is sized for *concurrent buffer-holding futures*,
//! not active CPU. Each worker hosts many queues (potentially hundreds of
//! tags); those tags can be in `handle_io.await` (S3 fetch, write_cache
//! flush) holding a buffer. 256 covers the realistic burst before falling
//! back to the malloc path on pool exhaustion.
//!
//! When the pool is exhausted, `acquire` returns `None` and the caller falls
//! back to `vec![0u8; len]`. The fallback is correct but defeats the RSS
//! bound — so we instrument it and alert on sustained exhaustion.

use std::cell::{OnceCell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

// Global aggregates across all per-worker pools. `Send`-safe so the
// /metrics endpoint (which runs on an arbitrary tokio worker, not a
// ublk worker) can read them. Per-pool atomics remain for diagnostics.
pub static GLOBAL_ACQUIRES: AtomicU64 = AtomicU64::new(0);
pub static GLOBAL_BACKPRESSURE_WAITS: AtomicU64 = AtomicU64::new(0);
pub static GLOBAL_POOLS_INITIALIZED: AtomicU64 = AtomicU64::new(0);

/// Slots per worker. 256 × 128 KB = 32 MB per worker.
const POOL_SLOTS: usize = 256;

/// Slot size in bytes. Single source of truth is `device::IO_BUF_BYTES`;
/// the pool re-exports it under this name for readability inside this
/// module. Changing the daemon's max I/O buffer size touches exactly one
/// constant, not two.
pub const SLOT_SIZE: usize = super::device::IO_BUF_BYTES_USIZE;

pub struct WorkerBufferPool {
    region: *mut u8,
    region_size: usize,
    /// Free indices, LIFO. `RefCell` is safe because the pool is
    /// thread-local — never shared across threads.
    free: RefCell<Vec<u16>>,
    /// FIFO queue of futures awaiting a slot. On `PoolSlot::Drop`, the
    /// oldest waiter is woken. Single-threaded => `RefCell`.
    waiters: RefCell<VecDeque<Waker>>,
    /// Diagnostic counters — atomics so background tooling can read them
    /// without holding the `RefCell`.
    pub acquires: AtomicU64,
    /// Count of acquires that had to *wait* because the pool was empty.
    /// Non-zero sustained means the pool is undersized for the workload's
    /// concurrent-buffer-holding pattern; raise `POOL_SLOTS`.
    pub backpressure_waits: AtomicU64,
}

// NOTE: `WorkerBufferPool` is deliberately NOT `Send`. It contains `RefCell`
// (no internal synchronization) and a raw `*mut u8` region, and it is only
// ever held via `Rc` — in the per-thread `WORKER_POOL` thread-local and in
// `PoolSlot`/`AcquireFut`. `Rc` is itself `!Send`, so every holder is already
// pinned to its originating worker thread. The futures that touch the pool run
// on the worker's single-threaded executor (`Executor::spawn` carries no `Send`
// bound). An `unsafe impl Send` here would contradict the `RefCell` invariant
// and silently permit a cross-thread move that races `free`/`waiters`.

impl WorkerBufferPool {
    fn new() -> std::io::Result<Self> {
        let region_size = SLOT_SIZE * POOL_SLOTS;
        let region = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                region_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if region == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        // Pre-fault every page on the calling worker thread. Two reasons:
        //
        //   1) Latency: first I/O on a tag doesn't pay per-page faults
        //      (32 KB of bounce = 8 minor faults pre-amortized to init).
        //   2) NUMA correctness: Linux's MPOL_DEFAULT first-touch policy
        //      places each freshly-faulted page on the CPU's local NUMA
        //      node. Worker threads are CPU-pinned via `sched_setaffinity`
        //      BEFORE the pool's lazy init runs, so the pages land on the
        //      worker's local node automatically — no `mbind` syscall
        //      needed. Multi-socket production hosts get NUMA-correct
        //      placement by construction.
        //
        // Volatile per-page write prevents any compiler/LLVM elision of
        // the memset (which it might attempt on freshly-mmapped zero
        // memory). Each write triggers the page fault that allocates the
        // real backing page on the local node.
        unsafe {
            let mut off = 0;
            while off < region_size {
                std::ptr::write_volatile((region as *mut u8).add(off), 0u8);
                off += 4096;
            }
        }

        let mut free = Vec::with_capacity(POOL_SLOTS);
        for i in (0..POOL_SLOTS).rev() {
            free.push(i as u16);
        }

        Ok(Self {
            region: region as *mut u8,
            region_size,
            free: RefCell::new(free),
            waiters: RefCell::new(VecDeque::new()),
            acquires: AtomicU64::new(0),
            backpressure_waits: AtomicU64::new(0),
        })
    }

    /// Try to acquire a slot synchronously. Returns `None` if pool is
    /// exhausted. Prefer `acquire()` from async code — this is for tests
    /// and synchronous diagnostics.
    pub fn try_acquire(self: &Rc<Self>) -> Option<PoolSlot> {
        let idx = self.free.borrow_mut().pop()?;
        self.acquires.fetch_add(1, Ordering::Relaxed);
        GLOBAL_ACQUIRES.fetch_add(1, Ordering::Relaxed);
        let offset = (idx as usize) * SLOT_SIZE;
        let ptr = unsafe { self.region.add(offset) };
        Some(PoolSlot {
            pool: Rc::clone(self),
            idx,
            ptr,
        })
    }

    /// Acquire a slot. If the pool is empty, parks the future on the
    /// waiter queue until a slot is released; wakes are FIFO. This is the
    /// async-backpressure path that makes pool exhaustion impossible to
    /// silently break the RSS bound — futures wait, RSS stays flat,
    /// throughput drops gracefully. Increments `backpressure_waits` so
    /// undersizing the pool is observable.
    pub fn acquire(self: &Rc<Self>) -> AcquireFut {
        AcquireFut {
            pool: Rc::clone(self),
            registered: false,
        }
    }
}

/// Future returned by `WorkerBufferPool::acquire`. Polls the free list;
/// if empty, registers its waker and returns `Pending` until a slot is
/// released and our waker fires.
pub struct AcquireFut {
    pool: Rc<WorkerBufferPool>,
    /// Once we've parked ourselves on the waiter queue, don't double-park.
    /// (We're polled again after release; we re-check the queue.)
    registered: bool,
}

impl Future for AcquireFut {
    type Output = PoolSlot;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Fast path: a slot is available now.
        if let Some(slot) = self.pool.try_acquire() {
            return Poll::Ready(slot);
        }
        // Slow path: park. Each `Pending` return represents one slot of
        // backpressure on this worker.
        let this = self.get_mut();
        if !this.registered {
            this.pool
                .backpressure_waits
                .fetch_add(1, Ordering::Relaxed);
            GLOBAL_BACKPRESSURE_WAITS.fetch_add(1, Ordering::Relaxed);
        }
        this.pool
            .waiters
            .borrow_mut()
            .push_back(cx.waker().clone());
        this.registered = true;
        Poll::Pending
    }
}

impl Drop for WorkerBufferPool {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.region as *mut libc::c_void, self.region_size);
        }
    }
}

/// Owned RAII handle to a pool slot. On Drop, returns the slot to the
/// pool's free list. Holds `Rc<WorkerBufferPool>` so the slot is valid
/// even if the originating function returns before the slot is dropped.
pub struct PoolSlot {
    pool: Rc<WorkerBufferPool>,
    idx: u16,
    ptr: *mut u8,
}

impl PoolSlot {
    #[inline]
    pub fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        debug_assert!(len <= SLOT_SIZE, "slot len {len} exceeds slot size {SLOT_SIZE}");
        unsafe { std::slice::from_raw_parts_mut(self.ptr, len) }
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for PoolSlot {
    fn drop(&mut self) {
        self.pool.free.borrow_mut().push(self.idx);
        // Wake exactly one waiter, FIFO. The woken future will be polled
        // by the worker's executor on the next tick; it pops from `free`
        // (which we just pushed to) and proceeds. If many futures are
        // waiting, releases ripple one wake at a time — no thundering
        // herd, no missed wakes.
        if let Some(waker) = self.pool.waiters.borrow_mut().pop_front() {
            waker.wake();
        }
    }
}

thread_local! {
    static WORKER_POOL: OnceCell<Rc<WorkerBufferPool>> = const { OnceCell::new() };
}

/// Get (or lazily initialize) the calling worker thread's buffer pool.
/// Panics if mmap fails — at worker init time that means OOM and the
/// daemon cannot continue.
pub fn worker_pool() -> Rc<WorkerBufferPool> {
    WORKER_POOL.with(|cell| {
        cell.get_or_init(|| {
            let pool = Rc::new(
                WorkerBufferPool::new()
                    .expect("worker buffer pool mmap failed — OOM at init"),
            );
            GLOBAL_POOLS_INITIALIZED.fetch_add(1, Ordering::Relaxed);
            pool
        })
        .clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll};

    fn noop_waker() -> Waker {
        use std::task::{RawWaker, RawWakerVTable};
        const VTABLE: RawWakerVTable =
            RawWakerVTable::new(|_| RawWaker::new(std::ptr::null(), &VTABLE), |_| {}, |_| {}, |_| {});
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    #[test]
    fn try_acquire_round_trip() {
        let pool = Rc::new(WorkerBufferPool::new().unwrap());
        let mut slot = pool.try_acquire().unwrap();
        let buf = slot.as_mut_slice(4096);
        buf[0] = 0x42;
        buf[4095] = 0x42;
        assert_eq!(pool.acquires.load(Ordering::Relaxed), 1);
        drop(slot);
        assert_eq!(pool.free.borrow().len(), POOL_SLOTS);
    }

    #[test]
    fn try_acquire_returns_none_on_exhaustion() {
        let pool = Rc::new(WorkerBufferPool::new().unwrap());
        let mut held: Vec<PoolSlot> = Vec::new();
        for _ in 0..POOL_SLOTS {
            held.push(pool.try_acquire().unwrap());
        }
        assert!(pool.try_acquire().is_none());
        held.pop();
        assert!(pool.try_acquire().is_some());
    }

    #[test]
    fn lifo_locality() {
        let pool = Rc::new(WorkerBufferPool::new().unwrap());
        let a = pool.try_acquire().unwrap();
        let a_ptr = a.as_ptr();
        drop(a);
        let b = pool.try_acquire().unwrap();
        // LIFO: the just-released slot should be the next acquired.
        assert_eq!(a_ptr, b.as_ptr());
    }

    #[test]
    fn async_acquire_parks_then_wakes_on_release() {
        // Verify the structural promise: when pool is empty, async acquire
        // parks; a release wakes one waiter and they get the slot.
        let pool = Rc::new(WorkerBufferPool::new().unwrap());

        // Drain to exhaustion.
        let mut held: Vec<PoolSlot> = Vec::new();
        for _ in 0..POOL_SLOTS {
            held.push(pool.try_acquire().unwrap());
        }

        // Construct an async acquire future; poll it once with a waker.
        // It should park (return Pending) and register on the waiter queue.
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = Box::pin(pool.acquire());
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(pool.waiters.borrow().len(), 1);
        assert_eq!(pool.backpressure_waits.load(Ordering::Relaxed), 1);

        // Release a slot — waiter queue should be drained.
        held.pop();
        assert_eq!(pool.waiters.borrow().len(), 0);

        // Poll again — slot should be available.
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(_) => {}
            Poll::Pending => panic!("expected slot to be available after release"),
        }
    }

    #[test]
    fn waiters_are_fifo() {
        let pool = Rc::new(WorkerBufferPool::new().unwrap());
        // Drain
        let mut held: Vec<PoolSlot> = Vec::new();
        for _ in 0..POOL_SLOTS {
            held.push(pool.try_acquire().unwrap());
        }
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut fut_a = Box::pin(pool.acquire());
        let mut fut_b = Box::pin(pool.acquire());
        assert!(matches!(fut_a.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(fut_b.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(pool.waiters.borrow().len(), 2);

        // Release one slot — only the older waiter (fut_a) should be
        // woken via the FIFO waiter queue. fut_b's waker stays parked.
        held.pop();
        assert_eq!(pool.waiters.borrow().len(), 1, "exactly one waker should have been popped");

        // Poll fut_a — receives the just-released slot. We BIND the slot
        // here so it's not dropped mid-expression (which would re-release
        // and wake fut_b prematurely).
        let slot_a = match fut_a.as_mut().poll(&mut cx) {
            Poll::Ready(s) => s,
            Poll::Pending => panic!("fut_a should be ready after release"),
        };
        // While slot_a is held, fut_b's poll must still be Pending.
        assert!(matches!(fut_b.as_mut().poll(&mut cx), Poll::Pending));
        // Releasing slot_a now wakes fut_b.
        drop(slot_a);
        assert!(matches!(fut_b.as_mut().poll(&mut cx), Poll::Ready(_)));
    }
}
