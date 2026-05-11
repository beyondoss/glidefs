//! Single ublk device record + executor primitives shared with the
//! worker pool. The per-device-thread / `run_target` model was deleted
//! in M7; what remains:
//!
//! - [`UblkDevice`]: a thin data record (`dev_id`, `dev_path`,
//!   `Arc<UblkDev>`). Construction (`register`/`recover`) runs the
//!   full control-plane sequence on one `spawn_blocking` thread and
//!   dispatches per-queue `AddQueue` to the worker pool.
//! - [`EventFd`], [`WakeupBits`], [`QueueExecutor`]: shared by the
//!   worker pool's `worker_thread_main` event loop.
//! - [`io_task`]: per-tag FETCH/COMMIT loop, spawned into the worker's
//!   executor via `WorkerMsg::AddQueue`.

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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll, Wake, Waker};

/// Per-queue I/O depth (max inflight commands per queue).
///
/// Empirically (see plan: we-do-not-need-dazzling-dewdrop) depth=64 captures
/// 98% of the read-path peak (452k → 444k IOPS on local cache hits) and 100% of
/// the write-path peak. Going higher inflates IoBuf virtual memory without
/// material throughput. Going lower starts to leave IOPS on the table at
/// jobs ≥ 4.
const QUEUE_DEPTH: u16 = 64;

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
    /// `UBLK_F_CMD_IOCTL_ENCODE` — uring commands use ioctl encoding.
    /// Required on kernels built without `CONFIG_BLKDEV_UBLK_LEGACY_OPCODES`.
    pub ioctl_encode: bool,
}

/// Probe the running kernel for supported ublk feature flags.
///
/// Returns conservative defaults (all false) on pre-6.5 kernels where
/// `get_features()` is unavailable.
pub(crate) fn detect_features() -> KernelFeatures {
    let raw = UblkCtrl::get_features().unwrap_or(0);
    let recovery = (raw & sys::UBLK_F_USER_RECOVERY as u64) != 0;
    let ioctl_encode = (raw & sys::UBLK_F_CMD_IOCTL_ENCODE as u64) != 0;

    tracing::info!(
        recovery,
        ioctl_encode,
        raw_features = format_args!("0x{:x}", raw),
        "ublk kernel feature detection"
    );

    KernelFeatures { recovery, ioctl_encode }
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
/// **M4 model**: queues are hosted in the [`WorkerPool`]; this struct just
/// keeps the device record alive and tracks which workers host it. There
/// is no per-device OS thread anymore — `nr_queues` independent worker
/// threads (chosen by hash) drive this device's queues alongside queues
/// from many other devices.
///
/// Drop ordering: the device's `Arc<UblkDev>` is held here AND in each
/// hosted [`UblkQueue`] in the workers. Even if `unregister` is skipped
/// (Drop path), the workers' `Rc<UblkQueue>` keeps the device alive
/// until `kill_dev` triggers `UBLK_IO_RES_ABORT` and io_task futures
/// return.
///
/// [`WorkerPool`]: super::worker_pool::WorkerPool
#[must_use = "call .unregister() to cleanly shut down the device"]
pub struct UblkDevice {
    dev_id: i32,
    dev_path: PathBuf,
    nr_queues: u16,
    /// Lookup key used by the worker pool to pick a worker for each
    /// queue. Same key passed to `pool.worker_for(name, qid)` at
    /// register-time so removal hits the same worker.
    export_name: String,
    /// Send-side handles to the workers hosting each queue.
    /// `worker_handles[qid as usize]` is the worker that owns
    /// `(dev_id, qid)`. Stored here so `unregister` can send
    /// `RemoveQueue` messages without re-acquiring the `WorkerPool`
    /// (which lives behind a `tokio::Mutex<UblkServer>` that the caller
    /// must release before driving the slow `kill_dev` ioctl).
    ///
    /// **Why this is load-bearing**: the worker's `hosted` HashMap
    /// holds an `Rc<UblkQueue>` for this device. `UblkQueue::Drop`
    /// calls `munmap` on the cdev's `io_cmd_buf`, and **the kernel
    /// `UBLK_CMD_DEL_DEV` ioctl blocks until that mmap is released**.
    /// Without sending `RemoveQueue` before `del_dev`, the `Rc` in
    /// `hosted` keeps the queue alive, the munmap never runs, and
    /// `del_dev` hangs forever — observed during M7 1000-device
    /// teardown as 10+ minute DELETE latencies with the kernel stack
    /// stuck in `ublk_ctrl_del_dev+0x116`.
    worker_handles: Vec<super::worker_pool::WorkerHandleSnapshot>,
    /// The owning Arc of the kernel device record. Held to keep the
    /// underlying mmaps + control fd alive while workers' UblkQueues
    /// reference it. Dropped in `unregister`/`Drop`.
    _dev: Arc<UblkDev>,
}

impl UblkDevice {
    /// Register a new ublk device backed by the given handler.
    ///
    /// Builds a fresh `UblkCtrl` + `UblkDev`, dispatches one `AddQueue`
    /// per qid to the appropriate worker (via
    /// [`WorkerPool::worker_for`]), waits for all queues to come up,
    /// then issues `START_DEV`. Returns once `/dev/ublkbN` is serving
    /// I/O.
    ///
    /// `preferred_id`: request a specific device ID to reclaim a device
    /// path after a crash. `None` = auto-assign.
    ///
    /// [`WorkerPool::worker_for`]: super::worker_pool::WorkerPool::worker_for
    pub async fn register(
        handler: Arc<BlockHandler>,
        nr_queues: u16,
        export_name: String,
        features: &KernelFeatures,
        preferred_id: Option<i32>,
        preferred_node: Option<usize>,
        pool: &super::worker_pool::WorkerPool,
    ) -> anyhow::Result<Self> {
        Self::register_inner(
            handler,
            nr_queues,
            export_name,
            DeviceMode::Add,
            features,
            preferred_id,
            preferred_node,
            pool,
        )
        .await
    }

    /// Recover a QUIESCED ublk device after a daemon crash.
    ///
    /// Sends `START_USER_RECOVERY`, then re-attaches via the worker pool
    /// using `UBLK_DEV_F_RECOVER_DEV`. The kernel replays in-flight I/Os
    /// via `UBLK_F_USER_RECOVERY_REISSUE` (safe — our write cache is
    /// idempotent).
    pub async fn recover(
        dev_id: i32,
        handler: Arc<BlockHandler>,
        nr_queues: u16,
        export_name: String,
        features: &KernelFeatures,
        preferred_node: Option<usize>,
        pool: &super::worker_pool::WorkerPool,
    ) -> anyhow::Result<Self> {
        // Control-plane: tell the kernel we're taking over. This is
        // synchronous w.r.t. the kernel mutex, so we go through
        // spawn_blocking to keep the async runtime responsive.
        let dev_id_for_recover = dev_id;
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let ctrl = UblkCtrl::new_simple(dev_id_for_recover)
                .map_err(|e| anyhow::anyhow!("UblkCtrl::new_simple({dev_id_for_recover}) failed: {e}"))?;
            ctrl.start_user_recover()
                .map_err(|e| anyhow::anyhow!("start_user_recover({dev_id_for_recover}) failed: {e}"))?;
            Ok(())
        })
        .await??;

        Self::register_inner(
            handler,
            nr_queues,
            export_name,
            DeviceMode::Recover { dev_id },
            features,
            None,
            preferred_node,
            pool,
        )
        .await
    }

    /// Shared body for `register` and `recover`. Builds the kernel
    /// device, hands queues to workers, starts the device. **All
    /// control-plane work happens on a single `spawn_blocking` thread**
    /// because `ublk-core`'s `CTRL_URING` is `thread_local!` and the
    /// `UblkCtrl` handle is bound to its building thread for the life
    /// of any control op (including `Drop`). Worker AddQueue dispatch
    /// uses `mpsc::Sender::blocking_send` + `oneshot::Receiver::blocking_recv`
    /// to talk to the async-side workers without needing to leave
    /// the blocking thread.
    async fn register_inner(
        handler: Arc<BlockHandler>,
        nr_queues: u16,
        export_name: String,
        mode: DeviceMode,
        features: &KernelFeatures,
        preferred_id: Option<i32>,
        preferred_node: Option<usize>,
        pool: &super::worker_pool::WorkerPool,
    ) -> anyhow::Result<Self> {
        let dev_size = handler.device_size();
        let bs_shift = handler.block_size().trailing_zeros() as u8;

        let dev_flags = match mode {
            DeviceMode::Add => UblkFlags::UBLK_DEV_F_ADD_DEV,
            DeviceMode::Recover { .. } => UblkFlags::UBLK_DEV_F_RECOVER_DEV,
        };
        let preferred_dev_id: i32 = match mode {
            DeviceMode::Add => preferred_id.unwrap_or(-1),
            DeviceMode::Recover { dev_id } => dev_id,
        };

        let mut ctrl_flags: u64 = 0;
        if features.recovery {
            ctrl_flags |=
                sys::UBLK_F_USER_RECOVERY as u64 | sys::UBLK_F_USER_RECOVERY_REISSUE as u64;
        }
        if features.ioctl_encode {
            ctrl_flags |= sys::UBLK_F_CMD_IOCTL_ENCODE as u64;
        }

        // Snapshot per-worker handles so the spawn_blocking closure can
        // reach them without borrowing `pool`. Each entry is a Send
        // tuple (`mpsc::Sender` + `Arc<EventFd>` are both Send).
        let nr_queues_max = nr_queues;
        let mut worker_dispatch: Vec<(usize, super::worker_pool::WorkerHandleSnapshot)> =
            Vec::with_capacity(nr_queues_max as usize);
        for qid in 0..nr_queues_max {
            worker_dispatch.push((
                qid as usize,
                pool.worker_snapshot(&export_name, qid, preferred_node),
            ));
        }

        let export_for_json = export_name.clone();
        let handler_for_workers = Arc::clone(&handler);

        // All control-plane ops in one blocking task — same thread for
        // build, UblkDev::new, start_dev, and the original ctrl's Drop.
        // CTRL_URING is initialized once (during build) and used for
        // every subsequent op including Drop.
        let outcome: anyhow::Result<(i32, PathBuf, Arc<UblkDev>, u16)> =
            tokio::task::spawn_blocking(move || {
                let ctrl = UblkCtrlBuilder::default()
                    .name("glidefs")
                    .id(preferred_dev_id)
                    .nr_queues(nr_queues_max)
                    .depth(QUEUE_DEPTH)
                    .io_buf_bytes(IO_BUF_BYTES)
                    .dev_flags(dev_flags)
                    .ctrl_flags(ctrl_flags)
                    .build()
                    .map_err(|e| anyhow::anyhow!("ublk ctrl build failed: {e:?}"))?;

                let tgt_init = move |d: &mut UblkDev| {
                    d.tgt.dev_size = dev_size;
                    d.set_target_json(
                        serde_json::json!({ "export_name": export_for_json }),
                    );
                    d.tgt.params = sys::ublk_params {
                        types: sys::UBLK_PARAM_TYPE_BASIC | sys::UBLK_PARAM_TYPE_DISCARD,
                        basic: sys::ublk_param_basic {
                            attrs: sys::UBLK_ATTR_VOLATILE_CACHE | sys::UBLK_ATTR_FUA,
                            logical_bs_shift: 9,
                            physical_bs_shift: bs_shift,
                            io_opt_shift: bs_shift,
                            io_min_shift: 9,
                            max_sectors: d.dev_info.max_io_buf_bytes >> 9,
                            dev_sectors: dev_size >> 9,
                            ..Default::default()
                        },
                        discard: sys::ublk_param_discard {
                            discard_alignment: 0,
                            discard_granularity: 4096,
                            max_discard_sectors: 0,
                            max_write_zeroes_sectors: 1 << 15,
                            max_discard_segments: 0,
                            reserved0: 0,
                        },
                        ..Default::default()
                    };
                    Ok(())
                };
                let dev = Arc::new(
                    UblkDev::new(ctrl.get_name(), tgt_init, &ctrl)
                        .map_err(|e| anyhow::anyhow!("UblkDev::new failed: {e:?}"))?,
                );

                let actual_nr_queues = dev.dev_info.nr_hw_queues;

                // AddQueue dispatch via blocking sends. Each worker's
                // mpsc::Sender accepts blocking_send from a sync ctx.
                let mut readys = Vec::with_capacity(actual_nr_queues as usize);
                for qid in 0..actual_nr_queues {
                    let (worker_idx, snap) = &worker_dispatch[qid as usize];
                    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                    snap.inbox
                        .blocking_send(super::worker_pool::WorkerMsg::AddQueue {
                            dev: Arc::clone(&dev),
                            handler: Arc::clone(&handler_for_workers),
                            qid,
                            ready: ready_tx,
                        })
                        .map_err(|_| {
                            anyhow::anyhow!("worker {worker_idx} channel closed")
                        })?;
                    super::device::signal_eventfd(snap.eventfd.fd());
                    readys.push((*worker_idx, ready_rx));
                }
                for (qid_, (worker_idx, ready_rx)) in readys.into_iter().enumerate() {
                    let result = ready_rx.blocking_recv().map_err(|_| {
                        anyhow::anyhow!("worker {worker_idx} dropped ready sender")
                    })?;
                    result.map_err(|s| {
                        anyhow::anyhow!("worker {worker_idx} AddQueue failed: {s}")
                    })?;
                    // `configure_queue` records the queue's owner thread
                    // tid and, on the last queue, calls `build_json` —
                    // which is what populates `/run/ublksrvd/{dev_id}.json`
                    // with `target_data.export_name`. Without it the next
                    // daemon's recover_quiesced_devices can't identify
                    // which export each kernel device belongs to.
                    //
                    // We pass `0` as the tid because under the worker-
                    // pool model many queues share one worker thread —
                    // there's no meaningful 1:1 mapping. The tid is only
                    // used for diagnostics; recovery doesn't depend on it.
                    let _ = ctrl.configure_queue(&dev, qid_ as u16, 0);
                }

                // All queues attached. Issue START_DEV (or
                // END_USER_RECOVERY for the recovery path; ublk-core's
                // start_dev() picks based on device state).
                ctrl.start_dev(&dev).map_err(|e| {
                    anyhow::anyhow!("start_dev failed: {e:?}")
                })?;

                let dev_id = i32::try_from(ctrl.dev_info().dev_id)
                    .map_err(|_| anyhow::anyhow!("dev_id overflows i32"))?;
                let dev_path = PathBuf::from(ctrl.get_bdev_path());

                // Disarm the ctrl's destructive Drop. Its kernel device
                // is now LIVE and owned by the worker pool's queues;
                // when the caller wants removal, they go through
                // `unregister` (which builds a fresh `new_simple` ctrl
                // and calls kill_dev). The original ctrl drops cleanly
                // on this thread when the closure returns.
                ctrl.disarm_drop();

                Ok((dev_id, dev_path, dev, actual_nr_queues))
            })
            .await?;

        let (dev_id_assigned, dev_path, dev, actual_nr_queues) = outcome?;
        if actual_nr_queues != nr_queues {
            tracing::warn!(
                requested = nr_queues,
                actual = actual_nr_queues,
                "ublk allocated different nr_queues than requested"
            );
        }

        // Tune kernel block queue for a userspace device:
        // - wbt (write-back throttle): default 2ms target is too
        //   aggressive for ublk under load; disable.
        // - scheduler: mq-deadline adds pointless overhead — userspace
        //   handles I/O ordering.
        if let Some(dev_name) = dev_path.file_name().and_then(|n| n.to_str()) {
            let queue = format!("/sys/block/{dev_name}/queue");
            for (param, value) in [("wbt_lat_usec", "0"), ("scheduler", "none")] {
                let path = format!("{queue}/{param}");
                if let Err(e) = std::fs::write(&path, value) {
                    tracing::warn!(path = %path, error = %e, "failed to set block queue param");
                }
            }
        }

        tracing::info!(
            dev_id = dev_id_assigned,
            path = %dev_path.display(),
            queues = actual_nr_queues,
            "ublk device registered (worker-pool hosted)"
        );

        // Stash one WorkerHandleSnapshot per queue so `unregister` can
        // send RemoveQueue messages without holding the UblkServer mutex
        // (the mutex serializes unrelated operations; the slow path here
        // is `kill_dev` which we drive outside the lock).
        let worker_handles: Vec<super::worker_pool::WorkerHandleSnapshot> =
            (0..actual_nr_queues)
                .map(|qid| pool.worker_snapshot(&export_name, qid, preferred_node))
                .collect();

        Ok(Self {
            dev_id: dev_id_assigned,
            dev_path,
            nr_queues: actual_nr_queues,
            export_name,
            worker_handles,
            _dev: dev,
        })
    }

    /// The kernel-assigned device ID.
    pub fn dev_id(&self) -> i32 {
        self.dev_id
    }

    /// The block device path (e.g., `/dev/ublkb0`).
    pub fn dev_path(&self) -> &Path {
        &self.dev_path
    }

    /// Stop and unregister this ublk device.
    ///
    /// Sends `kill_dev` (STOP_DEV + DEL_DEV) — io_task futures see
    /// `QueueIsDown` and exit. Then sends `RemoveQueue` to each hosting
    /// worker so they drop their `Rc<UblkQueue>` reference. The
    /// device's `Arc<UblkDev>` drops at the end of this function;
    /// the kernel device disappears once the last reference to its
    /// underlying mmaps/fds is gone.
    ///
    /// **NOTE (M6 future fix)**: `kill_dev` will deadlock the kernel
    /// if VMs have inflight bios at the moment of call (see plan,
    /// M6.1). For M4 we accept this hazard — production removal goes
    /// through `box-manager` which quiesces VMs first. M6 replaces
    /// `kill_dev`-on-shutdown with `UBLK_F_USER_RECOVERY` auto-quiesce.
    pub async fn unregister(self) -> anyhow::Result<()> {
        tracing::info!(dev_id = self.dev_id, "unregistering ublk device");

        // Take ownership of every field so we control individual drop
        // points. In particular `_dev: Arc<UblkDev>` owns the open
        // `cdev_file` (`fs::File`), and the kernel `DEL_DEV` ioctl
        // blocks until that fd is `close()`d at the kernel-side
        // `f_count == 0`. We must drop the Arc *before* calling
        // `del_dev`, otherwise the daemon's own reference keeps the
        // cdev alive and the kernel waits forever on it.
        //
        // `UblkDevice` implements `Drop` (purely informational tracing),
        // so we can't destructure it directly. Wrap in `ManuallyDrop`
        // and pull fields out via `ptr::read`. We don't drop the
        // `ManuallyDrop<UblkDevice>` itself — we'd double-free the
        // fields — and the original Drop impl is just a `trace!`, so
        // skipping it is benign.
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: each field is read exactly once and ownership is
        // tracked statically below. `this` is never dropped.
        let dev_id = this.dev_id;
        let worker_handles = unsafe { std::ptr::read(&this.worker_handles) };
        let _dev = unsafe { std::ptr::read(&this._dev) };
        // Drop the remaining `Copy`/leaf fields. `dev_path` is a
        // `PathBuf` — it must be dropped to free its allocation. Same
        // for `export_name`.
        let _dev_path = unsafe { std::ptr::read(&this.dev_path) };
        let _export_name = unsafe { std::ptr::read(&this.export_name) };
        let _ = this; // emphasis: we intentionally don't drop `this`.

        // Four things have to happen, in a partial order, for the
        // kernel device to fully go away:
        //
        // (a) `STOP_DEV` (kernel): transitions LIVE→STOPPED and aborts
        //     every pending ublk FETCH command via
        //     `UBLK_IO_RES_ABORT`. The userspace io_task futures see
        //     `QueueIsDown` and return.
        //
        // (b) `UblkQueue::Drop` (worker thread): runs only when every
        //     `Rc<UblkQueue>` clone has been dropped — the `hosted`
        //     map's clone plus one per io_task. Drop munmaps
        //     `io_cmd_buf` and clears the cdev's io_uring fixed-file
        //     slot. UblkQueue's `dev: Arc<UblkDev>` is also dropped
        //     here, dropping one of the daemon-side Arc clones.
        //
        // (c) Drop the daemon's last `Arc<UblkDev>` (the `_dev` we
        //     just destructured). `UblkDev::Drop` runs `deinit_cdev`
        //     which `close()`s the kernel cdev fd that the daemon
        //     opened at `UblkQueue::new`. *This is the step my
        //     previous fix missed* — keeping `_dev` alive across the
        //     `del_dev` call deadlocked us at
        //     `ublk_ctrl_del_dev+0x116`.
        //
        // (d) `DEL_DEV` (kernel): the kernel waits for `f_count == 0`
        //     on the cdev (i.e., until every userspace fd referencing
        //     it is closed — including the one (c) just closed) and
        //     for in-flight commands to drain.
        //
        // We run (a) on a blocking thread concurrently with (b) (the
        // worker's tick processes the abort CQEs from (a)). Once both
        // have reported done, we explicitly drop `_dev` for (c), then
        // call `del_dev` on a blocking thread for (d).
        use futures::stream::{FuturesUnordered, StreamExt};
        let mut remove_acks = FuturesUnordered::new();
        for (qid, handle) in worker_handles.iter().enumerate() {
            let key = super::worker_pool::QueueKey { dev_id, qid: qid as u16 };
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let msg = super::worker_pool::WorkerMsg::RemoveQueue { key, done: done_tx };
            match handle.inbox.try_send(msg) {
                Ok(()) => {
                    signal_eventfd(handle.eventfd.fd());
                    remove_acks.push(done_rx);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(dev_id, qid, "worker inbox full during unregister");
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::warn!(
                        dev_id, qid,
                        "worker inbox closed during unregister (worker thread exited?)"
                    );
                }
            }
        }

        // (a) STOP_DEV on a blocking thread, in parallel with the
        //     worker-side drain.
        let t0 = std::time::Instant::now();
        let kill_handle = tokio::task::spawn_blocking(move || -> Result<(), UblkError> {
            let ctrl = UblkCtrl::new_simple(dev_id)?;
            ctrl.kill_dev()?;
            Ok(())
        });

        // Await (b) acks + (a). Either alone is insufficient.
        while remove_acks.next().await.is_some() {}
        let t_acks = t0.elapsed();
        kill_handle
            .await?
            .map_err(|e| anyhow::anyhow!("ublk kill_dev failed: {}", e))?;
        let t_kill = t0.elapsed();

        // (c) Drop the daemon's last `Arc<UblkDev>` so the kernel cdev
        //     fd is `close()`d before we ask the kernel to remove it.
        //     With the worker queues' `Arc<UblkDev>` clones already
        //     dropped (in step (b)), this Arc is the last reference —
        //     `UblkDev::Drop` runs synchronously here, closes the fd,
        //     and only then does the cdev's kernel-side `f_count`
        //     reach 0.
        drop(_dev);
        let t_dropdev = t0.elapsed();

        // (d) DEL_DEV on a blocking thread. With (c) done, this
        //     returns in milliseconds. Without (c) it deadlocks the
        //     kernel io_wq worker.
        tokio::task::spawn_blocking(move || -> Result<(), UblkError> {
            let ctrl = UblkCtrl::new_simple(dev_id)?;
            ctrl.del_dev()?;
            Ok(())
        })
        .await?
        .map_err(|e| anyhow::anyhow!("ublk del_dev failed: {}", e))?;
        let t_del = t0.elapsed();

        tracing::info!(
            dev_id,
            ms_acks = t_acks.as_millis() as u64,
            ms_kill = t_kill.as_millis() as u64,
            ms_dropdev = t_dropdev.as_millis() as u64,
            ms_total = t_del.as_millis() as u64,
            "unregister timing"
        );

        Ok(())
    }

    /// Drain this device's queues from the worker pool without sending
    /// `kill_dev`. Caller still owns the kernel device. Useful for
    /// tests and for the M6 shutdown path.
    ///
    /// Uses the `worker_handles` snapshots cached at register time so
    /// routing stays consistent regardless of pool hash topology.
    pub async fn drain_queues(&self) -> anyhow::Result<()> {
        for (qid, handle) in self.worker_handles.iter().enumerate() {
            let key = super::worker_pool::QueueKey {
                dev_id: self.dev_id,
                qid: qid as u16,
            };
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            handle
                .inbox
                .send(super::worker_pool::WorkerMsg::RemoveQueue {
                    key,
                    done: done_tx,
                })
                .await
                .map_err(|_| anyhow::anyhow!("worker channel closed for qid {qid}"))?;
            super::device::signal_eventfd(handle.eventfd.fd());
            // Best-effort; the Rc<UblkQueue> drop happens in the
            // worker thread.
            let _ = done_rx.await;
        }
        Ok(())
    }
}

impl Drop for UblkDevice {
    /// **M6.1**: Drop does NOT call `kill_dev`. Letting the kernel
    /// device persist (transitioning to QUIESCED via
    /// `UBLK_F_USER_RECOVERY` when worker pool fds close) is the
    /// safe default — `kill_dev` deadlocks if VMs are still writing
    /// (see `UblkServer::shutdown` doc).
    ///
    /// Three Drop paths:
    /// - **Daemon shutdown** (intended): worker pool dropped first,
    ///   then this Drop runs. Kernel device stays QUIESCED for the
    ///   next daemon to recover.
    /// - **`remove_device`** (operator): `unregister()` already ran
    ///   and called `kill_dev` explicitly. Drop here is a no-op.
    /// - **Panic / error path** (unintended): we leak the kernel
    ///   device into QUIESCED. The next daemon's
    ///   `recover_quiesced_devices` will reattach it (or
    ///   operator can clean up via `kill_dev` from a fresh
    ///   `UblkCtrl::new_simple`).
    fn drop(&mut self) {
        tracing::trace!(
            dev_id = self.dev_id,
            path = %self.dev_path.display(),
            "UblkDevice dropped (kernel device stays QUIESCED until next recover or explicit kill)"
        );
    }
}

// QueueLatch + run_target's per-queue thread coordination removed in M7.
// The worker-pool path (M4+) uses `oneshot` channels from
// `WorkerMsg::AddQueue.ready` for the same purpose, with parallel
// dispatch through `buffer_unordered`.

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
pub(super) struct EventFd(RawFd);

impl EventFd {
    pub(super) fn new() -> std::io::Result<Self> {
        // SAFETY: eventfd is a well-defined Linux syscall.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self(fd))
    }

    pub(super) fn fd(&self) -> RawFd {
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
pub(super) fn signal_eventfd(fd: RawFd) {
    let val: u64 = 1;
    let ret = unsafe { libc::write(fd, &val as *const u64 as *const libc::c_void, 8) };
    debug_assert!(ret == 8 || ret == -1, "eventfd write returned unexpected {ret}");
}

/// Drain accumulated eventfd signals (non-blocking read).
pub(super) fn drain_eventfd(fd: RawFd) {
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
/// Runtime-sized — capacity (in 64-bit words) is fixed at construction. The
/// per-device executor passes 3 (192 bits = `QUEUE_DEPTH` + eventfd daemon +
/// slack). The worker-pool executor will pass enough to fit Σ-tasks across
/// all hosted queues + daemons.
///
/// `wake()` sets one bit with a single `fetch_or` + signals the eventfd.
/// Duplicate wakeups collapse naturally (OR is idempotent).
/// `drain_with()` swaps each word atomically and yields each set bit's index
/// to the caller — no allocation.
pub(super) struct WakeupBits {
    words: Box<[AtomicU64]>,
    /// Owns a reference to the eventfd. Holding the `Arc` here (cloned into
    /// every `TaskWaker` via `WakeupBits` itself) guarantees the fd cannot
    /// close while any waker is still live — no use-after-close possible
    /// from a stray `wake()` after the executor's owner drops the eventfd.
    efd: Arc<EventFd>,
}

impl WakeupBits {
    pub(super) fn new(num_words: usize, efd: Arc<EventFd>) -> Self {
        assert!(num_words > 0, "WakeupBits needs at least one word");
        let words: Vec<AtomicU64> = (0..num_words).map(|_| AtomicU64::new(0)).collect();
        Self { words: words.into_boxed_slice(), efd }
    }

    /// Total task index capacity (number of bits).
    #[inline]
    pub(super) fn capacity(&self) -> usize {
        self.words.len() * 64
    }

    /// Mark a task as needing a poll + signal the eventfd.
    #[inline]
    pub(super) fn wake(&self, idx: usize) {
        self.words[idx / 64].fetch_or(1u64 << (idx % 64), Ordering::Release);
        signal_eventfd(self.efd.fd());
    }

    /// Atomically drain all pending wakeup bits, yielding each task index to
    /// `f`. Bits that arrive between word swaps are deferred to the next
    /// drain (the eventfd signal ensures prompt re-entry to the event loop).
    pub(super) fn drain_with(&self, mut f: impl FnMut(usize)) {
        for (word_idx, word_atomic) in self.words.iter().enumerate() {
            let mut word = word_atomic.swap(0, Ordering::Acquire);
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                word &= word - 1;
                f(word_idx * 64 + bit);
            }
        }
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
/// Slot storage is a **slab with freelist**: completed (or panicked) task
/// slots are reused on the next `spawn`. This is what makes the executor
/// safe under the worker-pool churn pattern (devices added and removed over
/// the lifetime of the daemon). The previous append-only design leaked one
/// slot per add/remove cycle and would have hit `AtCapacity` after a few
/// hundred churn iterations on any one worker.
///
/// Hot-path costs:
/// - `tick()`: word-by-word atomic swaps to drain, then iterate set bits
///   (zero alloc)
/// - `wake()`: one atomic `fetch_or` + one `write(2)` syscall
/// - Waker clone: one atomic increment (`Arc::clone`)
/// - Duplicate wakeups collapse (OR is idempotent → one poll per tick)
/// - `all_done()`: O(1) counter check
/// - `spawn`: O(1) — pops from freelist when available, else grows the slab
///   by one (waker allocated once per slot, then reused on every refill)
pub(super) struct QueueExecutor<'a> {
    /// Task futures. `UnsafeCell` for interior mutability (single-threaded).
    /// A slot may be `None` if its task has completed/panicked and the slot
    /// is in `free_list`, awaiting reuse.
    tasks: Vec<UnsafeCell<Option<Pin<Box<dyn Future<Output = ()> + 'a>>>>>,
    /// Pre-allocated wakers, one per slot. The waker for slot `idx` carries
    /// `(WakeupBits, idx)` and is constructed once when the slot is first
    /// allocated — subsequent spawns into the same slot reuse the waker.
    /// Passed by reference in `tick()` so the polled future only takes an
    /// atomic increment if it internally clones the waker.
    wakers: Vec<Waker>,
    /// Shared atomic bitmask + eventfd.
    bits: Arc<WakeupBits>,
    /// Number of live I/O tasks (excludes daemons). O(1) completion check.
    alive: Cell<usize>,
    /// Number of daemon tasks (spawned first, indices 0..num_daemons).
    /// Daemon tasks don't gate shutdown and are never freed (they live for
    /// the executor's whole lifetime), so they never enter `free_list`.
    num_daemons: usize,
    /// LIFO of slot indices that completed and can be reused by the next
    /// `spawn`. Only contains indices >= `num_daemons`. Single-threaded
    /// access — no atomic ops needed.
    free_list: Vec<usize>,
}

/// Error returned by [`QueueExecutor::spawn`] when the bitmap slot
/// space is exhausted. Propagated up the worker's `handle_add_queue`
/// path as a soft AddQueue failure (the requesting `UblkDevice::
/// register_inner` returns it as an `anyhow::Error` rather than
/// killing the worker thread).
#[derive(Debug)]
pub(super) enum SpawnError {
    AtCapacity { capacity: usize },
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpawnError::AtCapacity { capacity } => write!(
                f,
                "QueueExecutor at capacity ({capacity} tasks); host \
                 fewer queues per worker or bump WORKER_WAKEUP_WORDS"
            ),
        }
    }
}

impl std::error::Error for SpawnError {}

impl<'a> QueueExecutor<'a> {
    /// Construct an executor with `num_words × 64` task index slots.
    ///
    /// Takes an `Arc<EventFd>` so the eventfd outlives any `TaskWaker`
    /// reachable through `Arc<WakeupBits>` — preventing use-after-close from
    /// late `wake()` calls.
    pub(super) fn new(num_words: usize, efd: Arc<EventFd>) -> Self {
        Self {
            tasks: Vec::new(),
            wakers: Vec::new(),
            bits: Arc::new(WakeupBits::new(num_words, efd)),
            alive: Cell::new(0),
            num_daemons: 0,
            free_list: Vec::new(),
        }
    }

    /// Total task-slot capacity (the upper bound `spawn` will refuse past).
    #[inline]
    pub(super) fn capacity(&self) -> usize {
        self.bits.capacity()
    }

    /// Number of slots currently holding a task (daemons + live I/O tasks).
    /// `capacity() - used()` is an upper bound on how many more `spawn`s
    /// will succeed before AtCapacity (lower bound: 0, since spawns from
    /// other code paths between calls can consume slots — though in
    /// practice every executor is single-owner so this is exact).
    #[inline]
    pub(super) fn used(&self) -> usize {
        // tasks.len() is the high-water mark of distinct slot indices
        // allocated; free_list holds slots that are currently empty.
        self.tasks.len() - self.free_list.len()
    }

    /// Slots available for `spawn` without growing the slab past capacity.
    /// Use this as a preflight check before a batch of spawns where you
    /// need all-or-nothing semantics (e.g., spawning `queue_depth` tags
    /// for one ublk queue — half-spawned tags would leak slots forever).
    #[inline]
    pub(super) fn available(&self) -> usize {
        self.free_list.len() + (self.bits.capacity() - self.tasks.len())
    }

    /// Spawn a daemon task that does NOT count toward `all_done()`.
    ///
    /// Must be called before any `spawn()` calls. Used for helper tasks
    /// (e.g., the eventfd PollAdd watcher) whose lifetime should not gate
    /// the event loop exit.
    pub(super) fn spawn_daemon(&mut self, future: impl Future<Output = ()> + 'a) {
        debug_assert_eq!(
            self.alive.get(), 0,
            "spawn_daemon must be called before spawn"
        );
        let idx = self.tasks.len();
        assert!(
            idx < self.bits.capacity(),
            "QueueExecutor capacity {} exceeded by spawn_daemon", self.bits.capacity()
        );
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
    ///
    /// Reuses a slot from `free_list` if one is available (the case after
    /// a device has been removed from this worker); otherwise grows the
    /// slab by one slot. Returns `Err(SpawnError::AtCapacity)` when both
    /// the freelist is empty *and* the slab is at `bits.capacity()`.
    ///
    /// Callers that need all-or-nothing batch semantics (e.g., spawning
    /// `queue_depth` tags for one ublk queue) should preflight with
    /// `available() >= n` before the first spawn — otherwise a mid-batch
    /// failure leaves earlier tags running but partially wired up.
    pub(super) fn spawn(
        &mut self,
        future: impl Future<Output = ()> + 'a,
    ) -> Result<(), SpawnError> {
        if let Some(idx) = self.free_list.pop() {
            // Reuse: the waker for this idx is already in `wakers` and
            // remains valid (it points to the same WakeupBits + idx).
            // SAFETY: single-threaded; this slot is None (was just freed)
            // and no one else holds a borrow into it.
            unsafe {
                *self.tasks[idx].get() = Some(Box::pin(future));
            }
            self.alive.set(self.alive.get() + 1);
            // Mark for initial poll. Note: a stale `wake()` from a waker
            // clone held by the previous tenant's leftover tokio future
            // would land on this same idx — that's a spurious wakeup of
            // the new task, which is contract-compliant for futures and
            // costs at most one extra `poll()` per stale wake.
            self.bits.words[idx / 64].fetch_or(1u64 << (idx % 64), Ordering::Release);
            return Ok(());
        }
        let idx = self.tasks.len();
        if idx >= self.bits.capacity() {
            return Err(SpawnError::AtCapacity {
                capacity: self.bits.capacity(),
            });
        }
        self.tasks.push(UnsafeCell::new(Some(Box::pin(future))));
        self.wakers.push(Waker::from(Arc::new(TaskWaker {
            bits: Arc::clone(&self.bits),
            idx,
        })));
        self.alive.set(self.alive.get() + 1);
        // Mark for initial poll.
        self.bits.words[idx / 64].fetch_or(1u64 << (idx % 64), Ordering::Release);
        Ok(())
    }

    /// Poll all woken tasks. Called after `io_uring_enter()` returns.
    ///
    /// Drains the atomic bitmask in one shot, then iterates set bits.
    /// Tasks woken during this call are deferred to the next `tick()`
    /// (the eventfd signal ensures prompt re-entry to the event loop).
    ///
    /// A panic from a task's `poll` is caught: the task is dropped and (if
    /// it's an I/O task) `alive` is decremented. The panic does not propagate
    /// to the worker thread — co-tenant tasks keep running. Required for the
    /// multi-tenant worker-pool model where one device's bug must not take
    /// down the whole worker.
    pub(super) fn tick(&mut self) {
        // We collect freed indices via this fixed-size local rather than
        // borrowing `&mut self.free_list` inside `drain_with`'s closure
        // (which captures `&self.bits`/`&self.tasks`/`&self.wakers`).
        // Daemon completions are not pushed onto the freelist — daemons
        // are never reissued. In practice the per-tick freed count is
        // tiny (one per task that finishes this tick), so this stays
        // O(woken-tasks) like the previous implementation.
        let num_daemons = self.num_daemons;
        let tasks = &self.tasks;
        let wakers = &self.wakers;
        let alive = &self.alive;
        let mut freed_in_tick: smallvec::SmallVec<[usize; 16]> = smallvec::SmallVec::new();
        self.bits.drain_with(|idx| {
            if idx >= tasks.len() {
                return;
            }
            // SAFETY: single-threaded — only this thread accesses the tasks vec.
            // Wakers only touch the atomic WakeupBits, never the task storage.
            let slot = unsafe { &mut *tasks[idx].get() };
            if let Some(task) = slot {
                let mut cx = TaskContext::from_waker(&wakers[idx]);
                let outcome = std::panic::catch_unwind(
                    std::panic::AssertUnwindSafe(|| task.as_mut().poll(&mut cx)),
                );
                let done = match outcome {
                    Ok(Poll::Ready(())) => true,
                    Ok(Poll::Pending) => false,
                    Err(payload) => {
                        let msg = panic_payload_to_str(&payload);
                        tracing::error!(task_idx = idx, panic = %msg, "ublk task panicked; dropping");
                        true
                    }
                };
                if done {
                    *slot = None;
                    if idx >= num_daemons {
                        alive.set(alive.get() - 1);
                        freed_in_tick.push(idx);
                    }
                }
            }
        });
        // Return freed slots to the freelist so the next `spawn` reuses
        // them instead of growing the slab. Without this, every device
        // add/remove cycle leaks `nr_queues × queue_depth` slots per
        // worker — a hard ceiling at ~256 cycles for the production
        // 1024-word / 4-queue / 64-depth configuration.
        for idx in freed_in_tick {
            self.free_list.push(idx);
        }
    }

    /// Check if all I/O tasks have completed (daemons excluded).
    pub(super) fn all_done(&self) -> bool {
        self.alive.get() == 0
    }
}

/// Best-effort decode of a panic payload for logging.
fn panic_payload_to_str(payload: &Box<dyn std::any::Any + Send>) -> &str {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "<non-string panic>"
    }
}


/// Per-tag async I/O task. Runs in a worker's [`QueueExecutor`] and
/// pumps the FETCH → dispatch → COMMIT cycle for one tag's lifetime.
///
/// Allocates a per-tag `IoBuf` for kernel↔userspace data transfer
/// (non-zero-copy is the only path glidefs ships).
pub(super) async fn io_task(
    q: &UblkQueue,
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

/// Handle cold I/O ops (FLUSH, DISCARD, WRITE_ZEROES).
///
/// Called via `Box::pin` from the I/O loop so these large async state machines
/// (trim/write_zeroes chain into backfill → S3) don't inflate the hot future.
async fn dispatch_cold(
    op: u32,
    offset: u64,
    length: u32,
    fua: bool,
    handler: &BlockHandler,
) -> i32 {
    match op {
        sys::UBLK_IO_OP_FLUSH => match handler.flush() {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        },
        sys::UBLK_IO_OP_DISCARD => match handler.trim(offset, length, fua).await {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        },
        sys::UBLK_IO_OP_WRITE_ZEROES => match handler.write_zeroes(offset, length, fua).await {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        },
        _ => -libc::EINVAL,
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
            match handler.write(offset, buf, fua).await {
                Ok(()) => i32::try_from(length).unwrap_or(-libc::EIO),
                Err(e) => -e.to_linux_errno(),
            }
        }
        _ => Box::pin(dispatch_cold(op, offset, length, fua, handler)).await,
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
    // Handle metadata-only ops (FLUSH, DISCARD, WRITE_ZEROES) before slicing the
    // buffer — these don't need data and their length may exceed the buffer size
    // (e.g. max_discard_sectors = 16MB vs 512KB IO_BUF_BYTES).
    match op {
        sys::UBLK_IO_OP_FLUSH | sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => {
            return Box::pin(dispatch_cold(op, offset, length, fua, handler)).await;
        }
        _ => {}
    }
    let buf = &mut buffer.as_mut_slice()[..length as usize];
    handle_io(op, offset, length, fua, buf, handler).await
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use super::*;
    use crate::block::cache::SimpleBlockCache;
    use crate::block::content_store::ContentStore;
    use crate::block::metrics::ExportMetrics;
    use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
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
            DEFAULT_FLUSH_THRESHOLD,
            None,
        );
        (handler, temp)
    }

    // Tests exercise handle_io directly — the same function dispatch_io calls —
    // so dispatch logic can't silently diverge from tests.

    #[tokio::test]
    async fn write_read_roundtrip() {
        let (handler, _dir) = make_handler(false).await;

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
        let (handler, _dir) = make_handler(false).await;
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_FLUSH, 0, 0, false, &mut [], &handler).await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn discard_returns_ok() {
        let (handler, _dir) = make_handler(false).await;
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_DISCARD, 0, BLOCK_SIZE as u32, false, &mut [], &handler).await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn write_zeroes_clears_data() {
        let (handler, _dir) = make_handler(false).await;

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
        let (handler, _dir) = make_handler(false).await;
        let result = handle_io(0xFF, 0, 0, false, &mut [], &handler).await;
        assert_eq!(result, -libc::EINVAL);
    }

    #[tokio::test]
    async fn read_beyond_device_returns_zeros() {
        let (handler, _dir) = make_handler(false).await;
        let mut buf = vec![0xFFu8; BLOCK_SIZE];
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_READ, DEVICE_SIZE, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(result, BLOCK_SIZE as i32, "OOB read should succeed with zero-fill");
        assert!(buf.iter().all(|&b| b == 0), "OOB read should return zeros");
    }

    #[tokio::test]
    async fn write_with_fua_succeeds() {
        let (handler, _dir) = make_handler(false).await;
        let mut buf = vec![0xABu8; BLOCK_SIZE];
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, true, &mut buf, &handler).await;
        assert_eq!(result, BLOCK_SIZE as i32);
    }

    #[tokio::test]
    async fn write_readonly_returns_erofs() {
        let (handler, _dir) = make_handler(true).await;
        let mut buf = vec![0x42u8; BLOCK_SIZE];
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(result, -libc::EROFS);
    }

    #[tokio::test]
    async fn zero_length_read_returns_zero() {
        let (handler, _dir) = make_handler(false).await;
        let result =
            handle_io(ublk_core::sys::UBLK_IO_OP_READ, 0, 0, false, &mut [], &handler).await;
        assert_eq!(result, 0);
    }

    // ---- WakeupBits / QueueExecutor tests --------------------------------

    /// A throwaway eventfd for tests where we don't actually wait on it.
    fn test_efd() -> Arc<EventFd> {
        Arc::new(EventFd::new().expect("eventfd"))
    }

    #[test]
    fn wakeup_bits_capacity_matches_word_count() {
        let efd = test_efd();
        let bits = WakeupBits::new(5, efd);
        assert_eq!(bits.capacity(), 320);
    }

    #[test]
    fn wakeup_bits_roundtrip_with_runtime_size() {
        // Pick indices that span multiple words (across the boundary at 64).
        let efd = test_efd();
        let bits = WakeupBits::new(4, efd); // 256 bits
        bits.wake(3);
        bits.wake(63);
        bits.wake(64);
        bits.wake(200);
        let mut seen: Vec<usize> = Vec::new();
        bits.drain_with(|i| seen.push(i));
        seen.sort_unstable();
        assert_eq!(seen, vec![3, 63, 64, 200]);
        // After drain, all bits cleared.
        let mut second: Vec<usize> = Vec::new();
        bits.drain_with(|i| second.push(i));
        assert!(second.is_empty(), "second drain should be empty");
    }

    #[test]
    fn wakeup_bits_idempotent_double_wake() {
        let efd = test_efd();
        let bits = WakeupBits::new(2, efd);
        bits.wake(7);
        bits.wake(7);
        bits.wake(7);
        let mut count = 0;
        bits.drain_with(|_| count += 1);
        assert_eq!(count, 1, "duplicate wakes must collapse");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn executor_catches_panic_in_task() {
        // A task that panics once should be dropped; another task on the same
        // executor must keep running. This is the M1 isolation guarantee.
        use std::cell::RefCell;
        use std::rc::Rc;

        let efd = test_efd();
        let mut exe = QueueExecutor::new(2, efd);

        let healthy_polled = Rc::new(RefCell::new(0u32));
        let healthy_polled_c = Rc::clone(&healthy_polled);

        // Panicking task: panics on first poll.
        exe.spawn(async {
            panic!("intentional test panic");
        })
        .expect("spawn within capacity");

        // Healthy task: increments a counter then returns Ready.
        exe.spawn(async move {
            *healthy_polled_c.borrow_mut() += 1;
        })
        .expect("spawn within capacity");

        exe.tick();
        // Both tasks should have completed (the panicking one via drop, the
        // healthy one via Ready). Executor must report all_done.
        assert!(exe.all_done(), "panicking task must not block all_done");
        assert_eq!(*healthy_polled.borrow(), 1, "healthy co-tenant must run");
    }

    #[test]
    fn executor_capacity_fails_soft() {
        // Spawn until capacity is reached, then verify the next spawn returns
        // SpawnError::AtCapacity rather than panicking.
        let efd = test_efd();
        let mut exe = QueueExecutor::new(1, efd); // 64 slots
        for _ in 0..64 {
            exe.spawn(async {}).expect("spawn within capacity");
        }
        let err = exe.spawn(async {}).expect_err("must fail at capacity");
        assert!(matches!(err, SpawnError::AtCapacity { capacity: 64 }));
    }

    #[test]
    fn executor_slot_reuse_across_completion_cycles() {
        // The defining property of the slab+freelist design: completing tasks
        // free their slots, and a subsequent `spawn` reuses them instead of
        // growing the slab. Without this, every add_device → remove_device
        // cycle leaks `nr_queues × queue_depth` slots per worker — the
        // production hazard the slab replaces.
        let efd = test_efd();
        let mut exe = QueueExecutor::new(1, efd); // 64 slots
        // Fill, drain, fill again — repeated many more times than the slab
        // would tolerate under append-only semantics (64 slots → 64 / 64 = 1
        // pre-slab churn cycle was the old ceiling).
        for cycle in 0..10 {
            assert_eq!(exe.used(), 0, "cycle {cycle} should start empty");
            for _ in 0..64 {
                exe.spawn(async {}).expect("spawn within capacity");
            }
            assert_eq!(exe.used(), 64, "cycle {cycle} fills the slab");
            assert_eq!(exe.available(), 0, "no slots free until tick");
            exe.tick();
            assert!(exe.all_done(), "all tasks completed on first poll");
            assert_eq!(exe.used(), 0, "cycle {cycle} drained the slab");
            assert_eq!(exe.available(), 64, "all slots reusable after tick");
        }
    }

    #[test]
    fn executor_panic_releases_slot_to_freelist() {
        // Companion to `executor_catches_panic_in_task`: not only must the
        // panic not kill the worker, it must also return the slot to the
        // freelist so the next spawn can reuse it. Otherwise panics leak
        // slots over time and become a slower variant of the same
        // exhaustion bug.
        let efd = test_efd();
        let mut exe = QueueExecutor::new(1, efd); // 64 slots
        // Fill with panicking tasks. (Some will be polled in the order set
        // by the bitmap drain, but every task eventually panics on first
        // poll.)
        for _ in 0..64 {
            exe.spawn(async { panic!("expected") })
                .expect("spawn within capacity");
        }
        assert_eq!(exe.used(), 64);
        exe.tick();
        // Every slot must be back on the freelist.
        assert_eq!(exe.used(), 0, "panicked tasks must free their slots");
        assert_eq!(exe.available(), 64);
        // And the slab must be reusable for normal tasks.
        for _ in 0..64 {
            exe.spawn(async {}).expect("freed slots must be reusable");
        }
    }

    #[test]
    fn executor_available_preflight_is_exact() {
        // Verify the invariant `worker_pool::handle_add_queue` relies on:
        // after a successful preflight `available() >= n`, the next n
        // `spawn()` calls always succeed. (Single-threaded by construction,
        // so this is mechanical — but a regression would silently break
        // the all-or-nothing AddQueue contract.)
        let efd = test_efd();
        let mut exe = QueueExecutor::new(1, efd); // 64 slots

        // Establish a mix of fresh + freelist slots: spawn 32, complete
        // them, then mix with 16 unrelated spawns. Freelist has 32; slab
        // high-water is 48; capacity 64 → available = 32 + (64 - 48) = 48.
        for _ in 0..32 {
            exe.spawn(async {}).expect("spawn within capacity");
        }
        exe.tick();
        for _ in 0..16 {
            exe.spawn(async { std::future::pending::<()>().await })
                .expect("spawn within capacity");
        }
        let n = exe.available();
        assert_eq!(n, 48);
        for _ in 0..n {
            exe.spawn(async { std::future::pending::<()>().await })
                .expect("preflight guaranteed this spawn must succeed");
        }
        assert_eq!(exe.available(), 0);
        assert!(matches!(
            exe.spawn(async {}).unwrap_err(),
            SpawnError::AtCapacity { .. }
        ));
    }
}
