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
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Slots per worker. 256 × 128 KB = 32 MB per worker.
const POOL_SLOTS: usize = 256;

/// Slot size in bytes. Matches `IO_BUF_BYTES` in `device.rs`. If
/// `IO_BUF_BYTES` ever exceeds this, raise both — the pool is sized at
/// build time, not runtime, by design.
pub const SLOT_SIZE: usize = 128 * 1024;

pub struct WorkerBufferPool {
    region: *mut u8,
    region_size: usize,
    /// Free indices, LIFO. `RefCell` is safe because the pool is
    /// thread-local — never shared across threads.
    free: RefCell<Vec<u16>>,
    /// Diagnostic counters — atomics so background tooling can read them
    /// without holding the `RefCell`.
    pub acquires: AtomicU64,
    pub exhaust_fallbacks: AtomicU64,
}

unsafe impl Send for WorkerBufferPool {}

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

        // Pre-fault the region so the first I/O on a worker doesn't pay
        // 256 page faults serially. The cost (32 MB memset) is paid once
        // per worker thread.
        unsafe {
            std::ptr::write_bytes(region as *mut u8, 0, region_size);
        }

        let mut free = Vec::with_capacity(POOL_SLOTS);
        for i in (0..POOL_SLOTS).rev() {
            free.push(i as u16);
        }

        Ok(Self {
            region: region as *mut u8,
            region_size,
            free: RefCell::new(free),
            acquires: AtomicU64::new(0),
            exhaust_fallbacks: AtomicU64::new(0),
        })
    }

    /// Try to acquire a slot. Returns `None` if pool is exhausted; caller
    /// should fall back to heap allocation. Always increments either the
    /// acquire or exhaust counter, so saturation is observable.
    pub fn acquire(self: &Rc<Self>) -> Option<PoolSlot> {
        let idx = match self.free.borrow_mut().pop() {
            Some(i) => i,
            None => {
                self.exhaust_fallbacks.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        self.acquires.fetch_add(1, Ordering::Relaxed);
        let offset = (idx as usize) * SLOT_SIZE;
        let ptr = unsafe { self.region.add(offset) };
        Some(PoolSlot {
            pool: Rc::clone(self),
            idx,
            ptr,
        })
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
            Rc::new(
                WorkerBufferPool::new()
                    .expect("worker buffer pool mmap failed — OOM at init"),
            )
        })
        .clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release_round_trip() {
        let pool = Rc::new(WorkerBufferPool::new().unwrap());
        let mut slot = pool.acquire().unwrap();
        let buf = slot.as_mut_slice(4096);
        buf[0] = 0x42;
        buf[4095] = 0x42;
        assert_eq!(pool.acquires.load(Ordering::Relaxed), 1);
        drop(slot);
        assert_eq!(pool.free.borrow().len(), POOL_SLOTS);
    }

    #[test]
    fn exhaustion_returns_none() {
        let pool = Rc::new(WorkerBufferPool::new().unwrap());
        let mut held: Vec<PoolSlot> = Vec::new();
        for _ in 0..POOL_SLOTS {
            held.push(pool.acquire().unwrap());
        }
        assert!(pool.acquire().is_none());
        assert_eq!(pool.exhaust_fallbacks.load(Ordering::Relaxed), 1);
        // Releasing one re-enables acquire
        held.pop();
        assert!(pool.acquire().is_some());
    }

    #[test]
    fn lifo_locality() {
        let pool = Rc::new(WorkerBufferPool::new().unwrap());
        let a = pool.acquire().unwrap();
        let a_ptr = a.as_ptr();
        drop(a);
        let b = pool.acquire().unwrap();
        // LIFO: the just-released slot should be the next acquired.
        assert_eq!(a_ptr, b.as_ptr());
    }
}
