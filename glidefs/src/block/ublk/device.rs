//! Single ublk device: registration, per-queue I/O loop, teardown.
//!
//! Each `UblkDevice` corresponds to one `/dev/ublkbN` block device backed
//! by a `BlockHandler`. The device runs per-queue I/O threads that receive
//! commands via io_uring and dispatch to the handler.

use crate::block::handler::BlockHandler;
use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
use libublk::helpers::IoBuf;
use libublk::io::{UblkDev, UblkQueue};
use libublk::{BufDesc, UblkError, UblkFlags};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

/// Per-queue I/O depth (max inflight commands per queue).
const QUEUE_DEPTH: u16 = 128;

/// Max I/O buffer size per tag. 512KB covers our 128KB block size with room for large I/Os.
const IO_BUF_BYTES: u32 = 512 * 1024;

/// io_uring idle timeout in seconds. Controls worst-case latency from `kill_dev()` to queue exit.
const URING_IDLE_SECS: u64 = 20;

/// A registered ublk block device.
///
/// Owns the worker thread running `ctrl.run_target()`. The device appears
/// at `/dev/ublkbN` once `register()` returns, and disappears when
/// `unregister()` completes.
pub struct UblkDevice {
    dev_id: i32,
    dev_path: PathBuf,
    worker: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
}

// SAFETY: `JoinHandle<T>` is `!Sync` due to internal raw pointers.
// This impl is sound because no `&self` method accesses `self.worker` —
// only `unregister(self)` (by value) and `Drop::drop(&mut self)` touch it,
// both requiring exclusive access.
unsafe impl Sync for UblkDevice {}

impl UblkDevice {
    /// Register a new ublk device backed by the given handler.
    ///
    /// Allocates a device ID, sets parameters, spawns per-queue I/O threads,
    /// and waits for the kernel to confirm the device is serving I/O.
    /// Returns once `/dev/ublkbN` is ready.
    pub async fn register(handler: Arc<BlockHandler>, nr_queues: u16) -> anyhow::Result<Self> {
        let dev_size = handler.device_size();
        let tokio_handle = tokio::runtime::Handle::current();

        // The worker thread signals back the dev_id + path once the device is started,
        // or an error if setup fails.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<anyhow::Result<(i32, String)>>();

        let worker = std::thread::Builder::new()
            .name("ublk".to_string())
            .spawn(move || run_device(dev_size, nr_queues, handler, tokio_handle, ready_tx))?;

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
        kill_result.map_err(|e| anyhow::anyhow!("ublk kill_dev failed: {:?}", e))?;

        // Join the worker with a timeout. The io_uring idle timeout bounds
        // worst-case exit latency, so we allow slightly more than that.
        if let Some(worker) = self.worker.take() {
            let timeout = std::time::Duration::from_secs(URING_IDLE_SECS + 5);
            match tokio::time::timeout(timeout, tokio::task::spawn_blocking(move || worker.join()))
                .await
            {
                Ok(Ok(Ok(Ok(())))) => {}
                Ok(Ok(Ok(Err(e)))) => return Err(e.context("ublk worker thread exited with error")),
                Ok(Ok(Err(_panic))) => return Err(anyhow::anyhow!("ublk worker thread panicked")),
                Ok(Err(e)) => return Err(anyhow::anyhow!("join task failed: {}", e)),
                Err(_elapsed) => {
                    tracing::warn!(
                        dev_id,
                        timeout_secs = timeout.as_secs(),
                        "ublk worker thread did not exit in time; detaching"
                    );
                }
            }
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
) -> anyhow::Result<()> {
    let ctrl = match UblkCtrlBuilder::default()
        .name("glidefs")
        .id(-1_i32)
        .nr_queues(nr_queues)
        .depth(QUEUE_DEPTH)
        .io_buf_bytes(IO_BUF_BYTES)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            let err_msg = format!("ublk build failed: {:?}", e);
            let _ = ready_tx.send(Err(anyhow::anyhow!("{}", &err_msg)));
            return Err(anyhow::anyhow!("{}", err_msg));
        }
    };

    // Target init: set device size and block parameters.
    let tgt_init = move |dev: &mut UblkDev| {
        dev.tgt.dev_size = dev_size;
        dev.tgt.params = libublk::sys::ublk_params {
            types: libublk::sys::UBLK_PARAM_TYPE_BASIC | libublk::sys::UBLK_PARAM_TYPE_DISCARD,
            basic: libublk::sys::ublk_param_basic {
                // Volatile cache + FUA: writes land in local SSD cache,
                // FUA forces an fdatasync before returning.
                attrs: libublk::sys::UBLK_ATTR_VOLATILE_CACHE | libublk::sys::UBLK_ATTR_FUA,
                logical_bs_shift: 9,   // 512 bytes (standard sector)
                physical_bs_shift: 17, // 128KB (our block size)
                io_opt_shift: 17,      // 128KB optimal I/O
                io_min_shift: 9,       // 512 bytes minimum
                max_sectors: dev.dev_info.max_io_buf_bytes >> 9,
                dev_sectors: dev_size >> 9,
                ..Default::default()
            },
            discard: libublk::sys::ublk_param_discard {
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

    // Per-queue I/O handler — runs on a dedicated thread per queue.
    // Cloned once per queue by run_target().
    let q_handler = move |qid: u16, dev: &UblkDev| {
        queue_io_loop(qid, dev, &handler, &tokio_handle);
    };

    // Called after the device is started and serving I/O.
    let on_started = move |ctrl: &UblkCtrl| {
        let dev_id = ctrl.dev_info().dev_id as i32;
        let dev_path = ctrl.get_bdev_path();
        let _ = ready_tx.send(Ok((dev_id, dev_path)));
    };

    ctrl.run_target(tgt_init, q_handler, on_started)
        .map_err(|e| anyhow::anyhow!("ublk run_target failed: {:?}", e))?;

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
) {
    let q_rc = match UblkQueue::new(qid, dev) {
        Ok(q) => Rc::new(q),
        Err(e) => {
            tracing::error!(qid, error = ?e, "failed to create ublk queue");
            return;
        }
    };
    let exe = smol::LocalExecutor::new();

    let mut tasks = Vec::new();
    for tag in 0..dev.dev_info.queue_depth as u16 {
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
/// Registers a buffer with the kernel, then loops: receive command, dispatch
/// to the BlockHandler, commit the result and fetch the next command.
async fn io_task(
    q: &UblkQueue<'_>,
    tag: u16,
    handler: &BlockHandler,
    tokio_handle: &tokio::runtime::Handle,
) -> Result<(), UblkError> {
    let buf_size = q.dev.dev_info.max_io_buf_bytes as usize;
    let mut buffer = IoBuf::<u8>::new(buf_size);

    // Initial fetch — registers this tag's buffer with the kernel.
    q.submit_io_prep_cmd(tag, BufDesc::Slice(buffer.as_slice()), 0, Some(&buffer))
        .await?;

    loop {
        let iod = q.get_iod(tag);
        let op = iod.op_flags & 0xff;
        let fua = (iod.op_flags & libublk::sys::UBLK_IO_F_FUA) != 0;
        let offset = (iod.start_sector << 9) as u64;
        let length = (iod.nr_sectors << 9) as u32;

        let result = dispatch_io(op, offset, length, fua, &mut buffer, handler, tokio_handle).await;

        // Commit result and fetch next command.
        q.submit_io_commit_cmd(tag, BufDesc::Slice(buffer.as_slice()), result)
            .await?;
    }
}

/// Dispatch a single I/O command to the BlockHandler.
///
/// Returns bytes transferred (positive) on success, negative errno on error.
///
/// Async so that reads can `.await` `BlockHandler::read()` on the smol executor,
/// allowing other tags to make progress while this tag waits on S3. Write, flush,
/// discard, and write-zeroes are synchronous in the handler and never suspend.
async fn dispatch_io(
    op: u32,
    offset: u64,
    length: u32,
    fua: bool,
    buffer: &mut IoBuf<u8>,
    handler: &BlockHandler,
    tokio_handle: &tokio::runtime::Handle,
) -> i32 {
    match op {
        libublk::sys::UBLK_IO_OP_READ => {
            // Install the tokio runtime context so that tokio::spawn (used by
            // read-ahead) and Notify work inside this smol-polled future.
            // The EnterGuard is !Send, which is fine — smol's LocalExecutor
            // never moves tasks across threads. Network I/O (S3 via reqwest)
            // is driven by tokio's reactor on its worker threads.
            let _guard = tokio_handle.enter();
            match handler.read(offset, length).await {
                Ok(data) => {
                    buffer.as_mut_slice()[..data.len()].copy_from_slice(&data);
                    data.len() as i32
                }
                Err(e) => -e.to_linux_errno(),
            }
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            let data = &buffer.as_slice()[..length as usize];
            match handler.write(offset, data, fua) {
                Ok(()) => length as i32,
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
