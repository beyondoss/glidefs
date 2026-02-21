//! Single ublk device: registration, per-queue I/O loop, teardown.
//!
//! Each `UblkDevice` corresponds to one `/dev/ublkbN` block device backed
//! by a `BlockHandler`. The device runs per-queue I/O threads that receive
//! commands via io_uring and dispatch to the handler.

use anyhow::Context as _;
use crate::block::handler::BlockHandler;
use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
use libublk::helpers::IoBuf;
use libublk::io::{UblkDev, UblkQueue};
use libublk::{sys, BufDesc, UblkError, UblkFlags};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-queue I/O depth (max inflight commands per queue).
const QUEUE_DEPTH: u16 = 128;

/// Max I/O buffer size per tag. 512KB covers our 128KB block size with room for large I/Os.
const IO_BUF_BYTES: u32 = 512 * 1024;

/// io_uring idle timeout in seconds. Controls worst-case latency from `kill_dev()` to queue exit.
const URING_IDLE_SECS: u64 = 20;

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
        raw_features = format!("{raw:#x}"),
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

/// Run the ublk device lifecycle on a dedicated thread.
///
/// 1. Build the ublk control device (allocates `/dev/ublkbN`)
/// 2. `run_target()` sets params, spawns queue threads, starts the device
/// 3. Blocks until `kill_dev()` triggers shutdown
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

/// Per-queue async I/O loop using smol.
///
/// Each tag (0..queue_depth) gets its own smol task that loops:
///   fetch command → dispatch to BlockHandler → commit result
///
/// smol runs on the queue's dedicated thread (CPU-pinned by libublk).
fn queue_io_loop(
    qid: u16,
    dev: &UblkDev,
    handler: &Arc<BlockHandler>,
    tokio_handle: &tokio::runtime::Handle,
    latch: &QueueLatch,
) {
    let q_rc = match UblkQueue::new(qid, dev) {
        Ok(q) => {
            latch.signal_ready();
            Rc::new(q)
        }
        Err(e) => {
            tracing::error!(qid, error = ?e, "failed to create ublk queue");
            latch.signal_failed();
            return;
        }
    };
    let exe = smol::LocalExecutor::new();

    let mut tasks = Vec::new();
    for tag in 0..dev.dev_info.queue_depth {
        let q = q_rc.clone();
        let handler = Arc::clone(handler);
        let tokio_handle = tokio_handle.clone();

        tasks.push(exe.spawn(async move {
            if let Err(e) = io_task(&q, tag, &handler, &tokio_handle).await {
                match e {
                    UblkError::QueueIsDown => {} // normal shutdown
                    _ => tracing::error!(qid, tag, error = ?e, "ublk io_task failed"),
                }
            }
        }));
    }

    // Drive the smol executor via libublk's io_uring event loop.
    let q = q_rc.clone();
    smol::block_on(exe.run(async {
        let run_tasks = || while exe.try_tick() {};
        let all_done = || tasks.iter().all(|t| t.is_finished());
        if let Err(e) =
            libublk::wait_and_handle_io_events(&q, Some(URING_IDLE_SECS), run_tasks, all_done).await
        {
            match e {
                UblkError::QueueIsDown => {}
                _ => tracing::error!(qid, error = ?e, "ublk event loop failed"),
            }
        }
    }));
}

/// Per-tag async I/O task.
///
/// When the kernel supports auto buffer registration (`UBLK_F_AUTO_BUF_REG` +
/// `UBLK_F_SUPPORT_ZERO_COPY`), we access kernel bio pages directly via
/// `iod.addr` — eliminating a 128KB memcpy per READ/WRITE. On older kernels
/// we fall back to `IoBuf`-based I/O.
async fn io_task(
    q: &UblkQueue<'_>,
    tag: u16,
    handler: &BlockHandler,
    tokio_handle: &tokio::runtime::Handle,
) -> Result<(), UblkError> {
    let zero_copy = q.support_auto_buf_zc();

    // Traditional: allocate IoBuf. Zero-copy: no daemon buffer needed.
    let mut buffer = if zero_copy {
        None
    } else {
        Some(IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize))
    };

    let auto_reg = sys::ublk_auto_buf_reg {
        index: tag,
        flags: 0,
        reserved0: 0,
        reserved1: 0,
    };

    // Initial fetch.
    if zero_copy {
        q.submit_io_prep_cmd(tag, BufDesc::AutoReg(auto_reg), 0, None)
            .await?;
    } else {
        let buf = buffer.as_ref().unwrap();
        q.submit_io_prep_cmd(tag, BufDesc::Slice(buf.as_slice()), 0, Some(buf))
            .await?;
    }

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

        let result = if zero_copy {
            let _guard = tokio_handle.enter();
            let needs_data = matches!(op, sys::UBLK_IO_OP_READ | sys::UBLK_IO_OP_WRITE);
            if needs_data && length > 0 {
                // SAFETY: The kernel guarantees `iod.addr` points to valid, mapped
                // bio pages for the duration of this I/O request (from `get_iod` to
                // `submit_io_commit_cmd`). The smol `LocalExecutor` never moves tasks
                // between threads, so the pointer stays valid throughout the await.
                let buf = unsafe {
                    std::slice::from_raw_parts_mut(iod.addr as *mut u8, length as usize)
                };
                handle_io(op, offset, length, fua, buf, handler).await
            } else {
                handle_io(op, offset, length, fua, &mut [], handler).await
            }
        } else {
            dispatch_io(
                op,
                offset,
                length,
                fua,
                buffer.as_mut().unwrap(),
                handler,
                tokio_handle,
            )
            .await
        };

        // Commit result and fetch next command.
        if zero_copy {
            q.submit_io_commit_cmd(tag, BufDesc::AutoReg(auto_reg), result)
                .await?;
        } else {
            q.submit_io_commit_cmd(
                tag,
                BufDesc::Slice(buffer.as_ref().unwrap().as_slice()),
                result,
            )
            .await?;
        }
    }
}

/// Dispatch a single I/O op to the BlockHandler.
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
        libublk::sys::UBLK_IO_OP_READ => {
            debug_assert!(buf.len() >= length as usize, "read buf too small");
            match handler.read_into(offset, length, buf).await {
                Ok(n) => i32::try_from(n).unwrap_or(-libc::EIO),
                Err(e) => -e.to_linux_errno(),
            }
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            debug_assert_eq!(buf.len(), length as usize, "write buf/length mismatch");
            match handler.write(offset, buf, fua) {
                Ok(()) => i32::try_from(length).unwrap_or(-libc::EIO),
                Err(e) => -e.to_linux_errno(),
            }
        }
        libublk::sys::UBLK_IO_OP_FLUSH => match handler.flush() {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        },
        libublk::sys::UBLK_IO_OP_DISCARD => match handler.trim(offset, length, fua) {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        },
        libublk::sys::UBLK_IO_OP_WRITE_ZEROES => match handler.write_zeroes(offset, length, fua) {
            Ok(()) => 0,
            Err(e) => -e.to_linux_errno(),
        },
        _ => -libc::EINVAL,
    }
}

/// Dispatch an I/O command from the io_uring queue to the BlockHandler.
///
/// Installs the tokio runtime context so that tokio-dependent futures
/// (Notify, tokio::spawn for read-ahead, reqwest for S3) work inside
/// this smol-polled task. The EnterGuard is !Send, which is fine —
/// smol's LocalExecutor never moves tasks across threads.
async fn dispatch_io(
    op: u32,
    offset: u64,
    length: u32,
    fua: bool,
    buffer: &mut IoBuf<u8>,
    handler: &BlockHandler,
    tokio_handle: &tokio::runtime::Handle,
) -> i32 {
    let _guard = tokio_handle.enter();
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
    use crate::block::pack_index::HostPackIndex;
    use crate::block::write_cache::{WriteCache, WriteCacheConfig};
    use object_store::memory::InMemory;
    use std::sync::atomic::AtomicU64;
    use tempfile::TempDir;
    use tokio::sync::Notify;

    const DEVICE_SIZE: u64 = 1024 * 1024; // 1MB
    const BLOCK_SIZE: usize = 4096;

    fn make_handler(readonly: bool) -> (BlockHandler, TempDir) {
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
        let pack_index =
            Arc::new(HostPackIndex::open(temp.path().join("pack_index.redb")).unwrap());
        let metrics = Arc::new(ExportMetrics::new());
        let cache = WriteCache::open(config).unwrap().skip_recovery_for_test();
        let handler = BlockHandler::new(
            Arc::new(cache),
            content_store,
            clean_cache,
            pack_index,
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
            handle_io(libublk::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(result, BLOCK_SIZE as i32);

        let mut buf = vec![0u8; BLOCK_SIZE];
        let result =
            handle_io(libublk::sys::UBLK_IO_OP_READ, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(result, BLOCK_SIZE as i32);
        assert_eq!(buf, vec![0x42u8; BLOCK_SIZE]);
    }

    #[tokio::test]
    async fn flush_returns_ok() {
        let (handler, _dir) = make_handler(false);
        let result =
            handle_io(libublk::sys::UBLK_IO_OP_FLUSH, 0, 0, false, &mut [], &handler).await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn discard_returns_ok() {
        let (handler, _dir) = make_handler(false);
        let result =
            handle_io(libublk::sys::UBLK_IO_OP_DISCARD, 0, BLOCK_SIZE as u32, false, &mut [], &handler).await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn write_zeroes_clears_data() {
        let (handler, _dir) = make_handler(false);

        // Write non-zero data.
        let mut buf = vec![0xFFu8; BLOCK_SIZE];
        handle_io(libublk::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;

        // Write zeroes over it.
        let result =
            handle_io(libublk::sys::UBLK_IO_OP_WRITE_ZEROES, 0, BLOCK_SIZE as u32, false, &mut [], &handler).await;
        assert_eq!(result, 0);

        // Read back — should be zeros.
        let mut buf = vec![0xFFu8; BLOCK_SIZE];
        handle_io(libublk::sys::UBLK_IO_OP_READ, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
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
            handle_io(libublk::sys::UBLK_IO_OP_READ, DEVICE_SIZE, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert!(result < 0, "expected negative errno, got {result}");
    }

    #[tokio::test]
    async fn write_with_fua_succeeds() {
        let (handler, _dir) = make_handler(false);
        let mut buf = vec![0xABu8; BLOCK_SIZE];
        let result =
            handle_io(libublk::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, true, &mut buf, &handler).await;
        assert_eq!(result, BLOCK_SIZE as i32);
    }

    #[tokio::test]
    async fn write_readonly_returns_erofs() {
        let (handler, _dir) = make_handler(true);
        let mut buf = vec![0x42u8; BLOCK_SIZE];
        let result =
            handle_io(libublk::sys::UBLK_IO_OP_WRITE, 0, BLOCK_SIZE as u32, false, &mut buf, &handler).await;
        assert_eq!(result, -libc::EROFS);
    }

    #[tokio::test]
    async fn zero_length_read_returns_zero() {
        let (handler, _dir) = make_handler(false);
        let result =
            handle_io(libublk::sys::UBLK_IO_OP_READ, 0, 0, false, &mut [], &handler).await;
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
