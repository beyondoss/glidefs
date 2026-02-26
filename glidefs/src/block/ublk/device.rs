//! Single ublk device: registration, per-queue I/O loop, teardown.
//!
//! Each `UblkDevice` corresponds to one `/dev/ublkbN` block device backed
//! by a `BlockHandler`. The device runs per-queue I/O threads that receive
//! commands via io_uring and dispatch to the handler.

use anyhow::Context as _;
use crate::block::handler::BlockHandler;
use ublk_core::ctrl::{UblkCtrl, UblkCtrlBuilder};
use ublk_core::helpers::IoBuf;
use ublk_core::io::{UblkDev, UblkQueue};
use ublk_core::{sys, BufDesc, UblkError, UblkFlags};
use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll, Wake, Waker};
use parking_lot::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::block::write_cache::ChunkSource;

/// Per-queue I/O depth (max inflight commands per queue).
const QUEUE_DEPTH: u16 = 128;

/// Max I/O buffer size per tag. 512KB covers our 128KB block size with room for large I/Os.
const IO_BUF_BYTES: u32 = 512 * 1024;

/// io_uring idle timeout in seconds. Controls worst-case latency from `kill_dev()` to queue exit.
///
/// When a queue has no inflight I/O, the thread blocks in `io_uring_enter()` for up to this
/// duration. Lower = faster shutdown of idle devices; the idle wakeup cost is negligible
/// (3 atomic swaps + Cell check).
const URING_IDLE_SECS: u64 = 2;

/// io_uring fixed-file index for the data file.
///
/// ublk_core auto-registers `fds[0]` as the ublk control device fd. We register
/// the data file at `fds[1]` in `tgt_init`, so io_uring Read/Write ops use
/// `types::Fixed(DATA_FILE_FD_INDEX)` with `Flags::FIXED_FILE`.
const DATA_FILE_FD_INDEX: u32 = 1;

// ---------------------------------------------------------------------------
// Kernel feature detection
// ---------------------------------------------------------------------------

/// Kernel ublk capabilities detected at startup.
#[derive(Debug, Clone)]
pub(crate) struct KernelFeatures {
    /// `UBLK_F_USER_RECOVERY` — device survives daemon crash in QUIESCED state.
    pub recovery: bool,
    /// `UBLK_F_SUPPORT_ZERO_COPY` + `UBLK_F_AUTO_BUF_REG` — DMA-mapped buffers,
    /// no kernel↔userspace memcpy.
    pub zero_copy: bool,
}

/// Probe the running kernel for supported ublk feature flags.
///
/// Returns conservative defaults (all false) on pre-6.5 kernels where
/// `get_features()` is unavailable.
pub(crate) fn detect_features() -> KernelFeatures {
    let raw = UblkCtrl::get_features().unwrap_or(0);
    let recovery = (raw & sys::UBLK_F_USER_RECOVERY as u64) != 0;
    let zero_copy = (raw & sys::UBLK_F_SUPPORT_ZERO_COPY as u64) != 0
        && (raw & sys::UBLK_F_AUTO_BUF_REG as u64) != 0;

    tracing::info!(
        recovery,
        zero_copy,
        raw_features = raw,
        "ublk kernel feature detection"
    );

    KernelFeatures { recovery, zero_copy }
}

// ---------------------------------------------------------------------------
// Device mode
// ---------------------------------------------------------------------------

/// Whether to create a fresh device or recover a QUIESCED one.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DeviceMode {
    /// Allocate a new device ID.
    Add,
    /// Recover a QUIESCED device with a known ID.
    Recover { dev_id: i32 },
}

/// A registered ublk block device.
///
/// Owns the worker thread running `ctrl.run_target()`. The device appears
/// at `/dev/ublkbN` once `register()` returns, and disappears when
/// `unregister()` completes.
#[must_use = "call .unregister() to cleanly shut down the device"]
pub struct UblkDevice {
    dev_id: i32,
    dev_path: PathBuf,
    worker: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
}

impl UblkDevice {
    /// Register a new ublk device backed by the given handler.
    ///
    /// Allocates a device ID, sets parameters, spawns per-queue I/O threads,
    /// and waits for the kernel to confirm the device is serving I/O.
    /// Returns once `/dev/ublkbN` is ready.
    pub async fn register(
        handler: Arc<BlockHandler>,
        nr_queues: u16,
        export_name: String,
        features: &KernelFeatures,
    ) -> anyhow::Result<Self> {
        Self::start_worker(handler, nr_queues, export_name, DeviceMode::Add, features).await
    }

    /// Recover a QUIESCED ublk device after a daemon crash.
    ///
    /// Sends `START_USER_RECOVERY`, then re-runs the I/O loop with
    /// `UBLK_DEV_F_RECOVER_DEV`. The kernel replays in-flight I/Os via
    /// `UBLK_F_USER_RECOVERY_REISSUE` (safe — our write cache is idempotent).
    pub async fn recover(
        dev_id: i32,
        handler: Arc<BlockHandler>,
        nr_queues: u16,
        export_name: String,
        features: &KernelFeatures,
    ) -> anyhow::Result<Self> {
        // Control-plane: tell the kernel we're taking over.
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let ctrl = UblkCtrl::new_simple(dev_id)
                .map_err(|e| anyhow::anyhow!("UblkCtrl::new_simple({dev_id}) failed: {e}"))?;
            ctrl.start_user_recover()
                .map_err(|e| anyhow::anyhow!("start_user_recover({dev_id}) failed: {e}"))?;
            Ok(())
        })
        .await??;

        Self::start_worker(handler, nr_queues, export_name, DeviceMode::Recover { dev_id }, features).await
    }

    /// Shared helper: spawn the worker thread running `run_device`.
    async fn start_worker(
        handler: Arc<BlockHandler>,
        nr_queues: u16,
        export_name: String,
        mode: DeviceMode,
        features: &KernelFeatures,
    ) -> anyhow::Result<Self> {
        let dev_size = handler.device_size();
        let tokio_handle = tokio::runtime::Handle::current();
        let features = features.clone();

        // The worker thread signals back the dev_id + path once the device is started,
        // or an error if setup fails.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<anyhow::Result<(i32, String)>>();

        let thread_name = format!("ublk-{export_name}");
        let worker = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run_device(dev_size, nr_queues, handler, tokio_handle, ready_tx, export_name, mode, &features)
            })?;

        // Wait for device to be ready (or error during setup).
        let (dev_id, dev_path_str) = ready_rx
            .await
            .map_err(|_| anyhow::anyhow!("ublk worker thread failed during setup"))??;

        let dev_path = PathBuf::from(dev_path_str);
        tracing::info!(dev_id, path = %dev_path.display(), "ublk device registered");

        Ok(Self {
            dev_id,
            dev_path,
            worker: Some(worker),
        })
    }

    /// The block device path (e.g., `/dev/ublkb0`).
    pub fn dev_path(&self) -> &Path {
        &self.dev_path
    }

    /// Stop and unregister this ublk device.
    ///
    /// Sends `UBLK_CMD_STOP_DEV` + `UBLK_CMD_DEL_DEV` to the kernel.
    /// Queue I/O loops receive `QueueIsDown` and exit. The worker thread
    /// is joined with a timeout to avoid hanging indefinitely.
    pub async fn unregister(mut self) -> anyhow::Result<()> {
        tracing::info!(dev_id = self.dev_id, "unregistering ublk device");

        let dev_id = self.dev_id;
        let kill_result = tokio::task::spawn_blocking(move || -> Result<(), UblkError> {
            let ctrl = UblkCtrl::new_simple(dev_id)?;
            ctrl.kill_dev()?;
            Ok(())
        })
        .await?;

        // If kill_dev failed, don't try to join — the worker may not exit.
        // Drop will retry kill_dev as a safety net.
        kill_result.map_err(|e| anyhow::anyhow!("ublk kill_dev failed: {}", e))?;

        // Join the worker with a timeout. The io_uring idle timeout bounds
        // worst-case exit latency, so we allow slightly more than that.
        if let Some(worker) = self.worker.take() {
            let join_timeout = std::time::Duration::from_secs(URING_IDLE_SECS + 5);
            let join_task = tokio::task::spawn_blocking(move || worker.join());

            let thread_result = tokio::time::timeout(join_timeout, join_task)
                .await
                .map_err(|_elapsed| {
                    tracing::warn!(
                        dev_id,
                        timeout_secs = join_timeout.as_secs(),
                        "ublk worker thread did not exit in time; detaching"
                    );
                    anyhow::anyhow!(
                        "ublk worker thread for dev_id {dev_id} did not exit within {}s",
                        join_timeout.as_secs()
                    )
                })?
                .map_err(|e| anyhow::anyhow!("join task failed: {}", e))?
                .map_err(|_panic| anyhow::anyhow!("ublk worker thread panicked"))?;

            thread_result.context("ublk worker thread exited with error")?;
        }

        Ok(())
    }
}

impl Drop for UblkDevice {
    fn drop(&mut self) {
        if self.worker.is_none() {
            return; // unregister() already ran
        }
        // Worker still running — unregister() was not called (or panicked).
        // Best-effort kill_dev so the kernel device doesn't become a zombie.
        // We cannot join the thread in Drop, but kill_dev triggers QueueIsDown
        // which causes the worker to exit within URING_IDLE_SECS.
        tracing::warn!(
            dev_id = self.dev_id,
            path = %self.dev_path.display(),
            "UblkDevice dropped without unregister — issuing best-effort kill_dev"
        );
        match UblkCtrl::new_simple(self.dev_id) {
            Ok(ctrl) => {
                if let Err(e) = ctrl.kill_dev() {
                    tracing::error!(dev_id = self.dev_id, error = ?e, "kill_dev in Drop failed");
                }
            }
            Err(e) => {
                tracing::error!(dev_id = self.dev_id, error = ?e, "UblkCtrl::new_simple in Drop failed");
            }
        }
        // JoinHandle is dropped here → thread detaches. It will exit on its own.
    }
}

/// Tracks queue thread initialization for fail-fast on queue setup errors.
///
/// Each queue thread signals success or failure. The `on_started` callback
/// waits until all queues have reported, then either proceeds or aborts.
struct QueueLatch {
    total: u16,
    state: Mutex<LatchState>,
    condvar: Condvar,
}

struct LatchState {
    reported: u16,
    failed: u16,
}

impl QueueLatch {
    fn new(total: u16) -> Self {
        Self {
            total,
            state: Mutex::new(LatchState {
                reported: 0,
                failed: 0,
            }),
            condvar: Condvar::new(),
        }
    }

    fn signal_ready(&self) {
        let mut state = self.state.lock();
        state.reported += 1;
        self.condvar.notify_one();
    }

    fn signal_failed(&self) {
        let mut state = self.state.lock();
        state.failed += 1;
        state.reported += 1;
        self.condvar.notify_one();
    }

    /// Wait until all queues have reported. Returns `true` if all succeeded.
    fn wait_all(&self, timeout: Duration) -> bool {
        let mut state = self.state.lock();
        let deadline = Instant::now() + timeout;
        while state.reported < self.total {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let result = self.condvar.wait_for(&mut state, remaining);
            if result.timed_out() && state.reported < self.total {
                return false;
            }
        }
        state.failed == 0
    }

    /// Get current (reported, failed) counts.
    fn counts(&self) -> (u16, u16) {
        let state = self.state.lock();
        (state.reported, state.failed)
    }
}

// ---------------------------------------------------------------------------
// eventfd + QueueExecutor (cross-thread wakeup for tokio → io_uring)
// ---------------------------------------------------------------------------
//
// Problem: tasks awaiting tokio futures (e.g., S3 fetches) get stuck when
// `io_uring_enter()` blocks the queue thread. When tokio completes a future
// on its worker thread and calls `wake()`, the executor must unblock
// `io_uring_enter()` to poll the woken task.
//
// Fix: QueueExecutor — a minimal single-threaded executor where EVERY waker
// writes to an eventfd. The eventfd is registered with io_uring via PollAdd,
// so any wake() from any source (tokio, io_uring CQE, internal) generates a
// CQE that unblocks io_uring_enter(). No wrapper or combined waker needed —
// the eventfd signal is baked into every waker by construction.

/// Wrapper around Linux `eventfd(2)` for cross-thread signaling.
struct EventFd(RawFd);

impl EventFd {
    fn new() -> std::io::Result<Self> {
        // SAFETY: eventfd is a well-defined Linux syscall.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self(fd))
    }

    fn fd(&self) -> RawFd {
        self.0
    }
}

impl Drop for EventFd {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.0);
        }
    }
}

/// Write 1 to an eventfd, waking any io_uring PollAdd watching it.
///
/// Called from tokio worker threads via the QueueExecutor waker. The write is
/// non-blocking (`EFD_NONBLOCK`); failure is silently ignored since it
/// only means the eventfd counter is already at u64::MAX (practically
/// impossible).
fn signal_eventfd(fd: RawFd) {
    let val: u64 = 1;
    let ret = unsafe { libc::write(fd, &val as *const u64 as *const libc::c_void, 8) };
    debug_assert!(ret == 8 || ret == -1, "eventfd write returned unexpected {ret}");
}

/// Drain accumulated eventfd signals (non-blocking read).
fn drain_eventfd(fd: RawFd) {
    let mut val: u64 = 0;
    let ret = unsafe { libc::read(fd, &mut val as *mut u64 as *mut libc::c_void, 8) };
    // EAGAIN is expected when no signals are pending (EFD_NONBLOCK).
    debug_assert!(ret == 8 || ret == -1, "eventfd read returned unexpected {ret}");
}

// ---------------------------------------------------------------------------
// QueueExecutor: minimal single-threaded executor with eventfd wakers
// ---------------------------------------------------------------------------

/// Atomic bitmask for task wakeups.
///
/// 3 × `AtomicU64` = 192 bits — enough for `QUEUE_DEPTH` (128) + overhead
/// tasks (eventfd watcher, etc.).
///
/// `wake()` sets one bit with a single `fetch_or` + signals the eventfd.
/// Duplicate wakeups collapse naturally (OR is idempotent).
/// `drain()` atomically grabs all pending bits in three swaps.
struct WakeupBits {
    words: [AtomicU64; 3],
    efd: RawFd,
}

impl WakeupBits {
    fn new(efd: RawFd) -> Self {
        Self {
            words: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
            efd,
        }
    }

    /// Mark a task as needing a poll + signal the eventfd.
    #[inline]
    fn wake(&self, idx: usize) {
        self.words[idx / 64].fetch_or(1u64 << (idx % 64), Ordering::Release);
        signal_eventfd(self.efd);
    }

    /// Atomically drain all pending wakeup bits.
    ///
    /// Bits that arrive between word swaps are deferred to the next drain
    /// (the eventfd signal ensures prompt re-entry to the event loop).
    fn drain(&self) -> [u64; 3] {
        [
            self.words[0].swap(0, Ordering::Acquire),
            self.words[1].swap(0, Ordering::Acquire),
            self.words[2].swap(0, Ordering::Acquire),
        ]
    }
}

/// Per-task waker: sets an atomic bit + signals eventfd on `wake()`.
///
/// Uses `std::task::Wake` — no raw vtable. Clone = `Arc::clone` (one atomic
/// increment). Wake = one `fetch_or` + one `write(2)` syscall. Zero heap
/// allocation on the hot path.
struct TaskWaker {
    bits: Arc<WakeupBits>,
    idx: usize,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.bits.wake(self.idx);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.bits.wake(self.idx);
    }
}

/// Minimal single-threaded async executor with atomic-bitmask wakers.
///
/// Every waker signals an eventfd, so any `wake()` from any thread (tokio
/// workers, io_uring CQE handlers, internal) unblocks `io_uring_enter()`.
/// No wrapper or combined waker needed — the signal is baked into every waker.
///
/// Hot-path costs:
/// - `tick()`: three atomic swaps to drain, then iterate set bits (zero alloc)
/// - `wake()`: one atomic `fetch_or` + one `write(2)` syscall
/// - Waker clone: one atomic increment (`Arc::clone`)
/// - Duplicate wakeups collapse (OR is idempotent → one poll per tick)
/// - `all_done()`: O(1) counter check
struct QueueExecutor<'a> {
    /// Task futures. `UnsafeCell` for interior mutability (single-threaded).
    tasks: Vec<UnsafeCell<Option<Pin<Box<dyn Future<Output = ()> + 'a>>>>>,
    /// Pre-allocated wakers, one per task. Passed by reference in `tick()` —
    /// zero atomic ops unless the polled future internally clones the waker.
    wakers: Vec<Waker>,
    /// Shared atomic bitmask + eventfd.
    bits: Arc<WakeupBits>,
    /// Number of live I/O tasks (excludes daemons). O(1) completion check.
    alive: Cell<usize>,
    /// Number of daemon tasks (spawned first, indices 0..num_daemons).
    /// Daemon tasks don't gate shutdown — when all I/O tasks complete,
    /// the event loop exits and daemon futures are dropped.
    num_daemons: usize,
}

impl<'a> QueueExecutor<'a> {
    fn new(efd: RawFd) -> Self {
        Self {
            tasks: Vec::new(),
            wakers: Vec::new(),
            bits: Arc::new(WakeupBits::new(efd)),
            alive: Cell::new(0),
            num_daemons: 0,
        }
    }

    /// Spawn a daemon task that does NOT count toward `all_done()`.
    ///
    /// Must be called before any `spawn()` calls. Used for helper tasks
    /// (e.g., the eventfd PollAdd watcher) whose lifetime should not gate
    /// the event loop exit.
    fn spawn_daemon(&mut self, future: impl Future<Output = ()> + 'a) {
        debug_assert_eq!(
            self.alive.get(), 0,
            "spawn_daemon must be called before spawn"
        );
        let idx = self.tasks.len();
        assert!(idx < 192, "QueueExecutor supports at most 192 tasks");
        self.tasks.push(UnsafeCell::new(Some(Box::pin(future))));
        self.wakers.push(Waker::from(Arc::new(TaskWaker {
            bits: Arc::clone(&self.bits),
            idx,
        })));
        self.num_daemons = idx + 1;
        // Mark for initial poll.
        self.bits.words[idx / 64].fetch_or(1u64 << (idx % 64), Ordering::Release);
    }

    /// Spawn an I/O task that counts toward `all_done()`.
    fn spawn(&mut self, future: impl Future<Output = ()> + 'a) {
        let idx = self.tasks.len();
        assert!(idx < 192, "QueueExecutor supports at most 192 tasks");
        self.tasks.push(UnsafeCell::new(Some(Box::pin(future))));
        self.wakers.push(Waker::from(Arc::new(TaskWaker {
            bits: Arc::clone(&self.bits),
            idx,
        })));
        self.alive.set(self.alive.get() + 1);
        // Mark for initial poll.
        self.bits.words[idx / 64].fetch_or(1u64 << (idx % 64), Ordering::Release);
    }

    /// Poll all woken tasks. Called after `io_uring_enter()` returns.
    ///
    /// Drains the atomic bitmask in one shot, then iterates set bits.
    /// Tasks woken during this call are deferred to the next `tick()`
    /// (the eventfd signal ensures prompt re-entry to the event loop).
    fn tick(&self) {
        let words = self.bits.drain();
        for (word_idx, mut word) in words.into_iter().enumerate() {
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                word &= word - 1; // clear lowest set bit
                let idx = word_idx * 64 + bit;
                if idx >= self.tasks.len() {
                    continue;
                }
                // SAFETY: single-threaded — only this thread accesses the tasks vec.
                // Wakers only touch the atomic WakeupBits, never the task storage.
                let slot = unsafe { &mut *self.tasks[idx].get() };
                if let Some(task) = slot {
                    let mut cx = TaskContext::from_waker(&self.wakers[idx]);
                    if task.as_mut().poll(&mut cx).is_ready() {
                        *slot = None;
                        if idx >= self.num_daemons {
                            self.alive.set(self.alive.get() - 1);
                        }
                    }
                }
            }
        }
    }

    /// Check if all I/O tasks have completed (daemons excluded).
    fn all_done(&self) -> bool {
        self.alive.get() == 0
    }
}

/// Waker that unparks a thread. Used by `block_on`.
struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Minimal `block_on`: parks the current thread and polls a single future.
///
/// Used to drive the top-level async event loop on the queue thread.
/// The future internally calls `io_uring_enter()` which blocks, so this
/// rarely actually parks — the future drives forward via CQEs.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = TaskContext::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => std::thread::park(),
        }
    }
}

// ---------------------------------------------------------------------------

/// Run the ublk device lifecycle on a dedicated thread.
///
/// 1. Build the ublk control device (allocates `/dev/ublkbN`)
/// 2. `run_target()` sets params, spawns queue threads, starts the device
/// 3. Blocks until `kill_dev()` triggers shutdown
#[allow(clippy::too_many_arguments)]
fn run_device(
    dev_size: u64,
    nr_queues: u16,
    handler: Arc<BlockHandler>,
    tokio_handle: tokio::runtime::Handle,
    ready_tx: tokio::sync::oneshot::Sender<anyhow::Result<(i32, String)>>,
    export_name: String,
    mode: DeviceMode,
    features: &KernelFeatures,
) -> anyhow::Result<()> {
    // Compute device + kernel feature flags from mode + features.
    let dev_flags = match mode {
        DeviceMode::Add => UblkFlags::UBLK_DEV_F_ADD_DEV,
        DeviceMode::Recover { .. } => UblkFlags::UBLK_DEV_F_RECOVER_DEV,
    };
    let dev_id: i32 = match mode {
        DeviceMode::Add => -1,
        DeviceMode::Recover { dev_id } => dev_id,
    };

    let mut ctrl_flags: u64 = 0;
    if features.recovery {
        ctrl_flags |=
            sys::UBLK_F_USER_RECOVERY as u64 | sys::UBLK_F_USER_RECOVERY_REISSUE as u64;
    }
    if features.zero_copy {
        ctrl_flags |=
            sys::UBLK_F_SUPPORT_ZERO_COPY as u64 | sys::UBLK_F_AUTO_BUF_REG as u64;
    }

    let ctrl = match UblkCtrlBuilder::default()
        .name("glidefs")
        .id(dev_id)
        .nr_queues(nr_queues)
        .depth(QUEUE_DEPTH)
        .io_buf_bytes(IO_BUF_BYTES)
        .dev_flags(dev_flags)
        .ctrl_flags(ctrl_flags)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            let err = anyhow::anyhow!("ublk build failed: {}", e);
            let _ = ready_tx.send(Err(anyhow::anyhow!("{:#}", &err)));
            return Err(err);
        }
    };

    // Extract data file fd for io_uring zero-copy registration.
    let data_file_fd = if features.zero_copy {
        Some(handler.data_file_raw_fd())
    } else {
        None
    };

    // Target init: set device size, block parameters, and export metadata.
    let tgt_init = move |dev: &mut UblkDev| {
        dev.tgt.dev_size = dev_size;
        dev.set_target_json(serde_json::json!({ "export_name": export_name }));
        dev.tgt.params = sys::ublk_params {
            types: sys::UBLK_PARAM_TYPE_BASIC | sys::UBLK_PARAM_TYPE_DISCARD,
            basic: sys::ublk_param_basic {
                // Volatile cache + FUA: writes land in local SSD cache,
                // FUA forces an fdatasync before returning.
                attrs: sys::UBLK_ATTR_VOLATILE_CACHE | sys::UBLK_ATTR_FUA,
                logical_bs_shift: 9,   // 512 bytes (standard sector)
                physical_bs_shift: 17, // 128KB (our block size)
                io_opt_shift: 17,      // 128KB optimal I/O
                io_min_shift: 9,       // 512 bytes minimum
                max_sectors: dev.dev_info.max_io_buf_bytes >> 9,
                dev_sectors: dev_size >> 9,
                ..Default::default()
            },
            discard: sys::ublk_param_discard {
                discard_alignment: 0,
                discard_granularity: 4096,
                max_discard_sectors: 1 << 15, // 16MB
                max_write_zeroes_sectors: 1 << 15,
                max_discard_segments: 1,
                reserved0: 0,
            },
            ..Default::default()
        };

        // Register data file with io_uring for zero-copy I/O.
        // ublk_core auto-sets fds[0] = ublk cdev fd. We put the data file
        // at the next slot → io_uring ops use types::Fixed(DATA_FILE_FD_INDEX).
        if let Some(fd) = data_file_fd {
            let idx = dev.tgt.nr_fds as usize;
            debug_assert_eq!(
                idx, DATA_FILE_FD_INDEX as usize,
                "expected data file at fd index {}, but nr_fds is {}",
                DATA_FILE_FD_INDEX, idx,
            );
            dev.tgt.fds[idx] = fd;
            dev.tgt.nr_fds += 1;
        }

        Ok(())
    };

    // Queue latch: tracks per-queue initialization for fail-fast.
    let latch = Arc::new(QueueLatch::new(nr_queues));

    // Per-queue I/O handler — runs on a dedicated thread per queue.
    // Cloned once per queue by run_target().
    let q_handler = {
        let latch = Arc::clone(&latch);
        move |qid: u16, dev: &UblkDev| {
            queue_io_loop(qid, dev, &handler, &tokio_handle, &latch);
        }
    };

    // Called after the device is started and serving I/O.
    let on_started = move |ctrl: &UblkCtrl| {
        // Wait for all queue threads to report initialization status.
        if !latch.wait_all(Duration::from_secs(5)) {
            let (reported, failed) = latch.counts();
            if ready_tx
                .send(Err(anyhow::anyhow!(
                    "ublk queue init failed: {failed} failed, {reported}/{} reported",
                    latch.total,
                )))
                .is_err()
            {
                tracing::warn!("ublk ready channel closed during queue init failure");
            }
            if let Err(e) = ctrl.kill_dev() {
                tracing::error!(error = ?e, "kill_dev after queue init failure");
            }
            return;
        }

        let dev_id = match i32::try_from(ctrl.dev_info().dev_id) {
            Ok(id) => id,
            Err(_) => {
                if ready_tx
                    .send(Err(anyhow::anyhow!(
                        "ublk dev_id {} overflows i32",
                        ctrl.dev_info().dev_id,
                    )))
                    .is_err()
                {
                    tracing::warn!("ublk ready channel closed during dev_id overflow");
                }
                if let Err(e) = ctrl.kill_dev() {
                    tracing::error!(error = ?e, "kill_dev after dev_id overflow");
                }
                return;
            }
        };

        let dev_path = ctrl.get_bdev_path();
        if ready_tx.send(Ok((dev_id, dev_path))).is_err() {
            // Receiver dropped — caller gave up. Kill the device so it
            // doesn't become an orphan with no UblkDevice tracking it.
            tracing::warn!("ublk ready channel closed — killing orphaned device");
            if let Err(e) = ctrl.kill_dev() {
                tracing::error!(error = ?e, "kill_dev after channel drop");
            }
        }
    };

    ctrl.run_target(tgt_init, q_handler, on_started)
        .map_err(|e| anyhow::anyhow!("ublk run_target failed: {}", e))?;

    Ok(())
}

/// Per-queue async I/O loop using QueueExecutor.
///
/// Each tag (0..queue_depth) gets its own task that loops:
///   fetch command → dispatch to BlockHandler → commit result
///
/// The QueueExecutor's wakers signal an eventfd, ensuring io_uring_enter()
/// returns whenever a task is woken from any thread (tokio, io_uring, etc.).
fn queue_io_loop(
    qid: u16,
    dev: &UblkDev,
    handler: &Arc<BlockHandler>,
    tokio_handle: &tokio::runtime::Handle,
    latch: &QueueLatch,
) {
    // Initialize the thread-local io_uring before UblkQueue::new() so we can
    // set SINGLE_ISSUER — each queue thread is the sole submitter to its ring.
    let sq_depth = dev.tgt.sq_depth as u32;
    let cq_depth = dev.tgt.cq_depth as u32;
    if let Err(e) = ublk_core::ublk_init_task_ring(|cell| {
        if cell.get().is_none() {
            let ring = io_uring::IoUring::builder()
                .setup_cqsize(cq_depth)
                .setup_coop_taskrun()
                .setup_single_issuer()
                .build(sq_depth)
                .map_err(ublk_core::UblkError::IOError)?;
            cell.set(std::cell::RefCell::new(ring))
                .map_err(|_| ublk_core::UblkError::OtherError(-libc::EEXIST))?;
        }
        Ok(())
    }) {
        tracing::error!(qid, error = ?e, "failed to init io_uring for ublk queue");
        latch.signal_failed();
        return;
    }

    let q_rc = match UblkQueue::new(qid, dev) {
        Ok(q) => Rc::new(q),
        Err(e) => {
            tracing::error!(qid, error = ?e, "failed to create ublk queue");
            latch.signal_failed();
            return;
        }
    };

    // Create eventfd for cross-thread wakeup signaling.
    let efd = match EventFd::new() {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(qid, error = ?e, "failed to create eventfd");
            latch.signal_failed();
            return;
        }
    };
    let efd_fd = efd.fd();

    latch.signal_ready();
    let zero_copy = q_rc.support_auto_buf_zc();
    let mut exe = QueueExecutor::new(efd_fd);

    // Enter the tokio runtime context once for the entire queue thread.
    // This must NOT be done per-task because the QueueExecutor polls multiple
    // futures on the same thread — per-task guards interleave and get dropped
    // out of LIFO order, causing a tokio panic.
    let _tokio_guard = tokio_handle.enter();

    // Spawn eventfd watcher as a daemon — it keeps a PollAdd registered on the
    // eventfd so that eventfd writes (from wakers on tokio threads) generate CQEs
    // that unblock io_uring_enter(). Daemon: doesn't gate shutdown.
    {
        let q = q_rc.clone();
        exe.spawn_daemon(async move {
            loop {
                let sqe = io_uring::opcode::PollAdd::new(
                    io_uring::types::Fd(efd_fd),
                    libc::POLLIN as u32,
                )
                .build();
                let result = q.ublk_submit_sqe(sqe).await;
                if result < 0 {
                    break;
                }
                drain_eventfd(efd_fd);
            }
        });
    }

    // Spawn per-tag I/O tasks.
    for tag in 0..dev.dev_info.queue_depth {
        let q = q_rc.clone();
        let handler = Arc::clone(handler);

        exe.spawn(async move {
            let result = if zero_copy {
                io_task_zc(&q, tag, &handler).await
            } else {
                io_task(&q, tag, &handler).await
            };
            if let Err(e) = result {
                match e {
                    UblkError::QueueIsDown => {} // normal shutdown
                    _ => tracing::error!(qid, tag, error = ?e, "ublk io_task failed"),
                }
            }
        });
    }

    // Drive the QueueExecutor via ublk_core's io_uring event loop.
    let q = q_rc.clone();
    block_on(async {
        let run_tasks = || exe.tick();
        let all_done = || exe.all_done();
        if let Err(e) =
            ublk_core::wait_and_handle_io_events(&q, Some(URING_IDLE_SECS), run_tasks, all_done).await
        {
            match e {
                UblkError::QueueIsDown => {}
                _ => tracing::error!(qid, error = ?e, "ublk event loop failed"),
            }
        }
    });
}

/// Per-tag async I/O task (non-zero-copy path).
///
/// Allocates a per-tag `IoBuf` for kernel↔userspace data transfer.
/// The kernel copies data into/out of this buffer on each I/O.
async fn io_task(
    q: &UblkQueue<'_>,
    tag: u16,
    handler: &BlockHandler,
) -> Result<(), UblkError> {
    let mut buffer = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);

    // Initial fetch.
    q.submit_io_prep_cmd(tag, BufDesc::Slice(buffer.as_slice()), 0, Some(&buffer))
        .await?;

    loop {
        let iod = q.get_iod(tag);
        let op = iod.op_flags & 0xff;
        let fua = (iod.op_flags & sys::UBLK_IO_F_FUA) != 0;
        let offset = iod.start_sector << 9;
        let byte_len = u64::from(iod.nr_sectors) * 512;
        debug_assert!(
            byte_len <= u64::from(u32::MAX),
            "nr_sectors {nr_sectors} exceeds u32 byte range",
            nr_sectors = iod.nr_sectors,
        );
        let length = byte_len as u32;

        let result = dispatch_io(op, offset, length, fua, &mut buffer, handler).await;

        q.submit_io_commit_cmd(tag, BufDesc::Slice(buffer.as_slice()), result)
            .await?;
    }
}

/// Per-tag async I/O task (zero-copy path).
///
/// The kernel maps bio pages into our address space via `UBLK_F_AUTO_BUF_REG`
/// and registers them as io_uring fixed buffers. For READ/WRITE, we submit
/// io_uring Read/Write SQEs that transfer data directly between bio pages
/// and the data file fd — no userspace buffer, no memcpy.
///
/// For chunks served from clean cache or S3, we fall back to
/// `ptr::copy_nonoverlapping` into the bio pages (one unavoidable copy).
async fn io_task_zc(
    q: &UblkQueue<'_>,
    tag: u16,
    handler: &BlockHandler,
) -> Result<(), UblkError> {
    let auto_reg = sys::ublk_auto_buf_reg {
        index: tag,
        flags: sys::UBLK_AUTO_BUF_REG_FALLBACK as u8,
        reserved0: 0,
        reserved1: 0,
    };

    // Initial fetch with auto buffer registration.
    q.submit_io_prep_cmd(tag, BufDesc::AutoReg(auto_reg), 0, None)
        .await?;

    loop {
        let iod = q.get_iod(tag);
        let op = iod.op_flags & 0xff;
        let fua = (iod.op_flags & sys::UBLK_IO_F_FUA) != 0;

        // If auto buffer registration failed, manually register before I/O.
        if (iod.op_flags & sys::UBLK_IO_F_NEED_REG_BUF) != 0 {
            let res = q.submit_register_io_buf(tag, tag).await;
            if res < 0 {
                q.submit_io_commit_cmd(tag, BufDesc::AutoReg(auto_reg), -libc::EIO)
                    .await?;
                continue;
            }
        }

        let offset = iod.start_sector << 9;
        let byte_len = u64::from(iod.nr_sectors) * 512;
        debug_assert!(
            byte_len <= u64::from(u32::MAX),
            "nr_sectors {} exceeds u32 byte range",
            iod.nr_sectors,
        );
        let length = byte_len as u32;
        let addr = iod.addr;

        let result = if let Some(r) = dispatch_passthrough(op, offset, length, fua, handler) {
            r
        } else {
            match op {
                sys::UBLK_IO_OP_WRITE => {
                    handle_write_zc(q, offset, length, fua, addr, handler).await
                }
                sys::UBLK_IO_OP_READ => {
                    handle_read_zc(q, offset, length, addr, handler).await
                }
                _ => -libc::EINVAL,
            }
        };

        q.submit_io_commit_cmd(tag, BufDesc::AutoReg(auto_reg), result)
            .await?;
    }
}

/// Zero-copy WRITE: bio pages → io_uring Write → data file.
///
/// Three-phase protocol:
/// 1. `pre_write`: mark blocks present, clear CRC32 (metadata prep)
/// 2. io_uring Write SQE: transfer data from bio pages to data file
/// 3. `post_write`: mark blocks dirty, append WAL entries
///
/// If the io_uring write fails, only phase 1 has run — blocks are marked
/// present (not dirty) with cleared CRCs. Recovery handles this safely.
async fn handle_write_zc(
    q: &UblkQueue<'_>,
    offset: u64,
    length: u32,
    fua: bool,
    addr: u64,
    handler: &BlockHandler,
) -> i32 {
    if length == 0 {
        return 0;
    }

    // Phase 1: prepare metadata before data lands on disk.
    if let Err(e) = handler.pre_write(offset, length as u64) {
        return -e.to_linux_errno();
    }

    // Phase 2: io_uring Write from bio pages to data file.
    // types::Fixed(1) = data file fd (registered in tgt_init at fds[1]).
    let sqe = io_uring::opcode::Write::new(
        io_uring::types::Fixed(DATA_FILE_FD_INDEX),
        addr as *const u8,
        length,
    )
    .offset(offset)
    .build()
    .flags(io_uring::squeue::Flags::FIXED_FILE);

    let cqe_result = q.ublk_submit_sqe(sqe).await;
    if cqe_result < 0 {
        return cqe_result;
    }
    if cqe_result as u32 != length {
        return -libc::EIO;
    }

    // Phase 3: commit metadata after data is on disk.
    if let Err(e) = handler.post_write(offset, length as u64, fua) {
        return -e.to_linux_errno();
    }

    length as i32
}

/// Zero-copy READ: data file → io_uring Read → bio pages.
///
/// Builds a read plan to determine each chunk's data source, then fills
/// the bio buffer:
/// - `LocalSsd`: io_uring Read from data file directly into bio pages (zero-copy)
/// - `InMemory`: ptr::copy_nonoverlapping from clean cache / S3 data (one copy)
/// - `Zero`: ptr::write_bytes
async fn handle_read_zc(
    q: &UblkQueue<'_>,
    offset: u64,
    length: u32,
    addr: u64,
    handler: &BlockHandler,
) -> i32 {
    if length == 0 {
        return 0;
    }

    let plan = match handler.resolve_read(offset, length).await {
        Ok(p) => p,
        Err(e) => return -e.to_linux_errno(),
    };

    let mut dst_offset: usize = 0;
    for entry in &plan.entries {
        debug_assert!(
            dst_offset + entry.slice_len <= length as usize,
            "ReadPlan exceeds I/O length: dst_offset={dst_offset} + slice_len={} > length={length}",
            entry.slice_len,
        );
        let dst_ptr = (addr as usize + dst_offset) as *mut u8;

        match &entry.source {
            ChunkSource::Zero => {
                // SAFETY: dst_ptr points into kernel-mapped bio pages, valid for
                // the duration of this I/O request (between get_iod and commit).
                unsafe {
                    std::ptr::write_bytes(dst_ptr, 0, entry.slice_len);
                }
            }
            ChunkSource::LocalSsd { file_offset } => {
                // io_uring Read from data file directly into bio pages.
                let read_offset = file_offset + entry.slice_start as u64;
                let sqe = io_uring::opcode::Read::new(
                    io_uring::types::Fixed(DATA_FILE_FD_INDEX),
                    dst_ptr,
                    entry.slice_len as u32,
                )
                .offset(read_offset)
                .build()
                .flags(io_uring::squeue::Flags::FIXED_FILE);

                let cqe_result = q.ublk_submit_sqe(sqe).await;
                if cqe_result < 0 {
                    return cqe_result;
                }
                if cqe_result as u32 != entry.slice_len as u32 {
                    return -libc::EIO;
                }
            }
            ChunkSource::InMemory(data) => {
                // memcpy from in-memory buffer to bio pages.
                let src = &data[entry.slice_start..entry.slice_start + entry.slice_len];
                // SAFETY: dst_ptr points into kernel-mapped bio pages, src is valid.
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr(), dst_ptr, entry.slice_len);
                }
            }
        }

        dst_offset += entry.slice_len;
    }

    handler.trigger_readahead(offset);
    length as i32
}

/// Dispatch a metadata-only I/O op (FLUSH, DISCARD, WRITE_ZEROES) to the handler.
///
/// Returns `Some(result)` if the op was handled, `None` if it requires a
/// data-path (READ/WRITE) which differs between ZC and non-ZC paths.
fn dispatch_passthrough(
    op: u32,
    offset: u64,
    length: u32,
    fua: bool,
    handler: &BlockHandler,
) -> Option<i32> {
    match op {
        sys::UBLK_IO_OP_FLUSH => Some(match handler.flush() {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        }),
        sys::UBLK_IO_OP_DISCARD => Some(match handler.trim(offset, length, fua) {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        }),
        sys::UBLK_IO_OP_WRITE_ZEROES => Some(match handler.write_zeroes(offset, length, fua) {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        }),
        _ => None,
    }
}

/// Dispatch a single I/O op to the BlockHandler (non-zero-copy path).
///
/// `buf` must be pre-sliced to `length` bytes. For WRITE ops, it contains the
/// data to write (kernel filled it). For READ ops, it will be filled with read
/// data. For FLUSH/DISCARD/WRITE_ZEROES, `buf` is unused (pass `&mut []`).
///
/// Returns bytes transferred (positive) on success, negative errno on error.
async fn handle_io(
    op: u32,
    offset: u64,
    length: u32,
    fua: bool,
    buf: &mut [u8],
    handler: &BlockHandler,
) -> i32 {
    if let Some(result) = dispatch_passthrough(op, offset, length, fua, handler) {
        return result;
    }
    match op {
        sys::UBLK_IO_OP_READ => {
            debug_assert!(buf.len() >= length as usize, "read buf too small");
            match handler.read_into(offset, length, buf).await {
                Ok(n) => i32::try_from(n).unwrap_or(-libc::EIO),
                Err(e) => -e.to_linux_errno(),
            }
        }
        sys::UBLK_IO_OP_WRITE => {
            debug_assert_eq!(buf.len(), length as usize, "write buf/length mismatch");
            match handler.write(offset, buf, fua) {
                Ok(()) => i32::try_from(length).unwrap_or(-libc::EIO),
                Err(e) => -e.to_linux_errno(),
            }
        }
        _ => -libc::EINVAL,
    }
}

/// Dispatch an I/O command from the io_uring queue to the BlockHandler.
///
/// The tokio runtime context is entered once per queue thread in
/// `queue_io_loop`, so handler methods can use `tokio::spawn` and
/// `tokio::sync::Notify` without per-call setup.
async fn dispatch_io(
    op: u32,
    offset: u64,
    length: u32,
    fua: bool,
    buffer: &mut IoBuf<u8>,
    handler: &BlockHandler,
) -> i32 {
    let buf = &mut buffer.as_mut_slice()[..length as usize];
    handle_io(op, offset, length, fua, buf, handler).await
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use super::*;
    use crate::block::cache::SimpleBlockCache;
    use crate::block::content_store::ContentStore;
    use crate::block::metrics::ExportMetrics;
    use crate::block::pack::DEFAULT_BLOCKS_PER_PACK;
    use crate::block::pack_index_cache::PackIndexCache;
    use crate::block::volume_manifest::VolumeManifest;
    use crate::block::write_cache::{WriteCache, WriteCacheConfig};
    use object_store::memory::InMemory;
    use std::sync::atomic::AtomicU64;
    use tempfile::TempDir;
    use tokio::sync::Notify;

    const DEVICE_SIZE: u64 = 1024 * 1024; // 1MB
    const BLOCK_SIZE: usize = 4096;

    async fn make_handler(readonly: bool) -> (BlockHandler, TempDir) {
        let temp = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp.path().to_path_buf(),
            device_name: "ublk-test".to_string(),
            device_size: DEVICE_SIZE,
            block_size: BLOCK_SIZE,
            wal_sync: false,
        };
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
        let clean_cache: Arc<dyn crate::block::cache::BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(
            VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE as u32),
        ));
        let metrics = Arc::new(ExportMetrics::new());
        let cache = WriteCache::open(config).unwrap().skip_recovery_for_test();
        let handler = BlockHandler::new(
            Arc::new(cache),
            content_store,
            clean_cache,
            pack_index_cache,
            volume_manifest,
            DEVICE_SIZE,
            readonly,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::new(Notify::const_new()),
            DEFAULT_BLOCKS_PER_PACK,
            None,
        );
        (handler, temp)
    }

    // Tests exercise handle_io directly — the same function dispatch_io calls —
    // so dispatch logic can't silently diverge from tests.

    #[tokio::test]
    async fn write_read_roundtrip() {
        let (handler, _dir) = make_handler(false);

        let mut buf = vec![0x42u8; BLOCK_SIZE];
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(result, BLOCK_SIZE as i32);

        let mut buf = vec![0u8; BLOCK_SIZE];
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_READ, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(result, BLOCK_SIZE as i32);
        assert_eq!(buf, vec![0x42u8; BLOCK_SIZE]);
    }

    #[tokio::test]
    async fn flush_returns_ok() {
        let (handler, _dir) = make_handler(false);
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_FLUSH, 0, 0, false, &mut [], &handler).await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn discard_returns_ok() {
        let (handler, _dir) = make_handler(false);
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_DISCARD, 0, BLOCK_SIZE as u32, false, &mut [], &handler).await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn write_zeroes_clears_data() {
        let (handler, _dir) = make_handler(false);

        // Write non-zero data.
        let mut buf = vec![0xFFu8; BLOCK_SIZE];
        handle_io(ublk_core::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;

        // Write zeroes over it.
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_WRITE_ZEROES, 0, BLOCK_SIZE as u32, false, &mut [], &handler).await;
        assert_eq!(result, 0);

        // Read back — should be zeros.
        let mut buf = vec![0xFFu8; BLOCK_SIZE];
        handle_io(ublk_core::sys::UBLK_IO_OP_READ, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(buf, vec![0u8; BLOCK_SIZE]);
    }

    #[tokio::test]
    async fn unknown_op_returns_einval() {
        let (handler, _dir) = make_handler(false);
        let result = handle_io(0xFF, 0, 0, false, &mut [], &handler).await;
        assert_eq!(result, -libc::EINVAL);
    }

    #[tokio::test]
    async fn read_beyond_device_returns_error() {
        let (handler, _dir) = make_handler(false);
        let mut buf = vec![0u8; BLOCK_SIZE];
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_READ, DEVICE_SIZE, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert!(result < 0, "expected negative errno, got {result}");
    }

    #[tokio::test]
    async fn write_with_fua_succeeds() {
        let (handler, _dir) = make_handler(false);
        let mut buf = vec![0xABu8; BLOCK_SIZE];
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, true, &mut buf, &handler).await;
        assert_eq!(result, BLOCK_SIZE as i32);
    }

    #[tokio::test]
    async fn write_readonly_returns_erofs() {
        let (handler, _dir) = make_handler(true);
        let mut buf = vec![0x42u8; BLOCK_SIZE];
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(result, -libc::EROFS);
    }

    #[tokio::test]
    async fn zero_length_read_returns_zero() {
        let (handler, _dir) = make_handler(false);
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_READ, 0, 0, false, &mut [], &handler).await;
        assert_eq!(result, 0);
    }

    #[test]
    fn queue_latch_all_ready() {
        let latch = QueueLatch::new(3);
        latch.signal_ready();
        latch.signal_ready();
        latch.signal_ready();
        assert!(latch.wait_all(Duration::from_secs(1)));
    }

    #[test]
    fn queue_latch_one_failed() {
        let latch = QueueLatch::new(3);
        latch.signal_ready();
        latch.signal_failed();
        latch.signal_ready();
        assert!(!latch.wait_all(Duration::from_secs(1)));
    }

    #[test]
    fn queue_latch_timeout() {
        let latch = QueueLatch::new(3);
        latch.signal_ready(); // only 1 of 3
        assert!(!latch.wait_all(Duration::from_millis(50)));
    }

    #[test]
    fn queue_latch_concurrent_signals() {
        let latch = Arc::new(QueueLatch::new(4));
        let barrier = Arc::new(std::sync::Barrier::new(5)); // 4 signalers + 1 waiter

        for i in 0..4u16 {
            let latch = Arc::clone(&latch);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                if i == 2 {
                    latch.signal_failed();
                } else {
                    latch.signal_ready();
                }
            });
        }

        barrier.wait(); // release all signalers
        let all_ok = latch.wait_all(Duration::from_secs(5));
        assert!(!all_ok, "expected failure from queue 2");
        let (reported, failed) = latch.counts();
        assert_eq!(reported, 4);
        assert_eq!(failed, 1);
    }
}
