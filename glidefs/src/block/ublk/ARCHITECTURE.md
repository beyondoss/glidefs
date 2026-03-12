# ublk Architecture

Takes I/O commands from the Linux ublk kernel driver over io_uring, dispatches them to a `BlockHandler`, and commits results back to the kernel — exposing a `/dev/ublkbN` block device for each named export.

## Data Flow

```
                        ┌─────────────────────────────────────────┐
                        │           Linux Kernel (ublk)           │
                        │                                         │
                        │  /dev/ublkbN ──► io_uring SQE (cmd) ──► │
                        │               ◄── io_uring CQE (result)─│
                        └────────────────────────┬────────────────┘
                                                 │ ublk cdev fd (fd[0])
                                    ┌────────────▼────────────┐
                                    │   queue_io_loop (thread)│
                                    │   ───────────────────   │
                                    │   QueueExecutor::tick() │
                                    │    ├─ io_task(tag0)     │
                                    │    ├─ io_task(tag1)     │
                                    │    └─ io_task(tagN)     │
                                    │   [QUEUE_DEPTH = 128]   │
                                    └────────────┬────────────┘
                                                 │ async dispatch
                                    ┌────────────▼────────────┐
                                    │     BlockHandler        │
                                    │ (WriteCache/S3/local    │
                                    │  SSD read/write/flush)  │
                                    └─────────────────────────┘

Wakeup path (cross-thread):
  tokio worker ──► TaskWaker::wake()
               ──► WakeupBits::set(idx)    [fetch_or on AtomicU64]
               ──► write(efd, 1)           [eventfd signal]
               ──► io_uring PollAdd CQE    [unblocks io_uring_enter]
               ──► QueueExecutor::tick()   [polls all set bits]
```

**Zero-copy path** (when `UBLK_F_SUPPORT_ZERO_COPY` is available):

```
Kernel bio pages ──► auto-registered fixed-buf ──► handler reads/writes directly
                     (no userspace bounce buffer)
```

**Error paths:**

```
Queue thread init fail ──► latch.signal_failed() ──► on_started kills device ──► register() returns Err
I/O dispatch error     ──► negative errno to kernel ──► caller sees EIO/EINVAL/etc.
Device crash           ──► QUIESCED state ──► recover_quiesced_devices() reattaches
```

## Concepts & Terminology

| Term | What It Controls | NOT |
|------|-----------------|-----|
| `UblkServer` | Lifecycle of all devices (add, recover, shutdown); persists export→ID mapping | Does not handle I/O — only device management |
| `UblkDevice` | One device's worker thread + join handle | Not a queue — just the outer lifecycle |
| `QueueExecutor` | Single-threaded async executor for one io_uring queue; polls futures when woken | Not tokio — fully custom, no thread pool |
| `WakeupBits` | 192-bit atomic bitmask tracking which tasks have pending wakeups | Not a queue — duplicates collapse (OR is idempotent) |
| `QueueLatch` | Barrier to wait for all queue threads to signal ready or failed before device is live | Not a shutdown barrier |
| `KernelFeatures` | Probed once at startup: which ublk kernel features are available | Not per-device — shared across all devices |
| `QUEUE_DEPTH` | 128 — max inflight I/Os per queue, also number of per-tag tasks | Not configurable at runtime |
| `IO_BUF_BYTES` | 512 KB — per-tag bounce buffer size for non-zero-copy path | Not the block size |
| `DeviceMode` | Whether to create a fresh device (`Add`) or reattach to a QUIESCED device (`Recover`) | |
| daemon task | eventfd watcher task in QueueExecutor; does not gate shutdown | Not an I/O task — purely internal signaling |

## Core Mechanism

### Queue Executor

Each queue runs on a dedicated OS thread. The thread owns:

1. An `io_uring` ring (`SQPOLL` disabled; `SINGLE_ISSUER` for kernel-side optimization)
2. A `QueueExecutor` with `QUEUE_DEPTH + overhead` task slots
3. One `EventFd` used as a cross-thread wakeup signal

The executor is not tokio. It is a minimal custom async executor:

```
loop {
    io_uring_enter(URING_IDLE_SECS)   // block until CQE or timeout
    exe.tick()                         // poll all tasks whose bit is set in WakeupBits
    if exe.all_done() { break }        // shutdown: all I/O tasks returned
}
```

`tick()` drains the bitmask atomically (swap with 0), then polls each set bit's future. No allocation, no VecDeque, no lock.

### Wakeup Path

When a tokio future (S3 read, write cache flush) completes on a tokio worker thread, it calls `wake()` on a `TaskWaker`. `TaskWaker::wake()`:

1. Sets bit `idx` in `WakeupBits` with `fetch_or` (lock-free, O(1))
2. Writes `1` to the eventfd

The eventfd is registered with io_uring via `PollAdd`. The write generates a CQE, which unblocks `io_uring_enter()`. This is the _only_ wakeup mechanism — every waker always signals the eventfd by construction. No conditional logic, no missed wakeups.

Duplicate wakeups (two tokio futures completing before `tick()` runs) safely coalesce: `fetch_or` on the same bit is idempotent, and the eventfd counter just increments — both are drained in one `tick()` pass.

### Per-Tag I/O Task

Each of the 128 tags runs a persistent future (`io_task` or `io_task_zc`) that loops forever until shutdown:

```
submit_io_prep_cmd(tag)       // tell kernel we're ready for next command
loop {
    get_iod(tag)              // read command descriptor from kernel
    dispatch_io(op, ...)      // async: may suspend waiting for S3/SSD
    submit_io_commit_cmd(tag) // commit result + fetch next (combined SQE)
}
```

The `submit_io_commit_cmd` SQE combines result commit and next-command fetch into one kernel round-trip.

### Zero-Copy Path

When `UBLK_F_SUPPORT_ZERO_COPY + UBLK_F_AUTO_BUF_REG` are available:

- Kernel auto-registers bio pages into io_uring fixed-buffer slots
- `handler.resolve_read()` returns a `ReadPlan` describing data sources per block
- For each `ReadPlan` entry:
  - `Zero` → `ptr::write_bytes()` (no copy)
  - `InMemory` → `ptr::copy_nonoverlapping()` (copy from write cache into kernel bio page)
  - `LocalSsd` → io_uring Read directly into fixed buf (zero kernel↔userspace copy)

### Three-Phase Write (zero-copy)

To maintain recoverability if a write is interrupted:

```
pre_write:   mark blocks present, clear CRCs
             [crash here → blocks neither present nor dirty → safe to retry]
pwrite:      write data to local SSD via RwLock'd fd (not io_uring fixed fd)
             [crash here → blocks present but not dirty → recoverable]
post_write:  mark blocks dirty, append WAL
```

The `pwrite` uses the `RwLock`-guarded `data_file` rather than the io_uring-registered fd because the active data file may be swapped during flush rotation (old inode renamed, new fd registered). The io_uring fixed fd would stale; the lock always has the current fd.

### Device Persistence

`UblkServer` stores an `export_name → device_id` map to `ublk_devices.json` in `cache_dir`. On restart, the preferred device ID is passed to the kernel so `/dev/ublkb0` stays stable across daemon restarts. Stale IDs in the map are harmless — the kernel auto-assigns a new ID if the preferred one is taken.

## State Machines

### Device Lifecycle

```
[add_device called]
        │
        ▼
  spawn worker thread
        │
        ▼
  run_device(DeviceMode::Add)
        │ UblkCtrlBuilder.build() → /dev/ublkbN allocated
        │ per-queue threads spawned
        │ QueueLatch::wait_all(5s)
        ├──────── queue thread: latch.signal_failed() ──────────► [device dies, Err returned]
        │
        ▼ all queues signaled ready
  on_started: send (dev_id, path) via oneshot
        │
        ▼
  UblkDevice { dev_id, dev_path, worker }
  add_device returns PathBuf (/dev/ublkbN)
        │
        ▼
  [serving I/O]
        │
  unregister() / shutdown()
        │ kill_dev() → SIGKILL equivalent to ublk cdev
        │ queue threads detect shutdown → all_done() → exit loop
        │ worker thread join with timeout
        ▼
  [device removed]
```

### Recovery Lifecycle (crash/restart)

```
[server restarts, quiesced devices found]
        │
        ▼
  recover_quiesced_devices(get_handler)
        │ scan kernel for ublk devices
        │ filter: name="glidefs" AND state=QUIESCED AND has_handler
        │
        ▼
  device::UblkDevice::recover(dev_id, handler, ...)
        │ run_device(DeviceMode::Recover { dev_id })
        │ reattach to existing kernel device (no new /dev/ublkbN allocated)
        │ queue threads resume serving I/O
        ▼
  persist_devices()
  returns recovered_count
```

### Queue Thread Lifecycle

```
queue_io_loop() start
        │ ublk_init_task_ring() ──────────────────────► [fail → signal_failed()]
        │ UblkQueue::new()      ──────────────────────► [fail → signal_failed()]
        │ EventFd::new()        ──────────────────────► [fail → signal_failed()]
        │
        ▼
  latch.signal_ready()
        │
        ▼
  spawn daemon task (eventfd PollAdd watcher)
  spawn QUEUE_DEPTH io_task futures
        │
        ▼
  [I/O loop: io_uring_enter → tick() → io_uring_enter → ...]
        │
  kill_dev() received
        │ io_task loops see shutdown condition → return
        │ alive count → 0 (daemons excluded)
        │ exe.all_done() → true
        ▼
  queue thread exits
```

## Why It Behaves This Way

### Why a custom executor instead of spawning tokio tasks

The ublk protocol requires one io_uring per queue thread — the io_uring ring is `SINGLE_ISSUER`, meaning only the owning thread may submit. Tokio's work-stealing scheduler would move futures across threads, breaking this invariant. The custom `QueueExecutor` pins all futures to one thread, keeping the io_uring single-issuer constraint satisfied without synchronization overhead.

### Why the eventfd is in every waker

The alternative — a condvar or parking_lot::Condvar — would require the queue thread to check a flag after every `io_uring_enter` timeout. With eventfd-in-every-waker, any tokio future completing immediately unblocks `io_uring_enter`, keeping latency low. The eventfd is a kernel primitive; the write from the tokio worker thread races safely against the queue thread's PollAdd — the kernel handles it.

### Why WakeupBits uses 3 × AtomicU64 instead of a Vec

Three words covers 192 bits: QUEUE_DEPTH (128 I/O tasks) + 1 daemon + slack. Fixed-size avoids heap allocation on the hot wakeup path. `drain()` is three atomic swaps; no lock, no CAS loop. For a system where wake() is called tens of thousands of times per second, allocation-free matters.

### Why nr_queues defaults to 1

One queue per export is sufficient for most workloads: the bottleneck is S3 latency (100–200 ms), not io_uring dispatch throughput. Multiple queues add per-CPU parallelism but also increase contention on shared structures (write cache lock, S3 semaphore). The default is conservative; operators can raise it for SSD-heavy local workloads.

### Why pwrite uses the RwLock'd fd instead of the io_uring fixed fd

During flush rotation the active data file is renamed and a new file opened. The io_uring-registered fd (slot `DATA_FILE_FD_INDEX`) still refers to the old inode. Writes through the fixed fd would go to the old file. The `RwLock<File>` is swapped atomically during rotation, so pwrite via the lock always hits the current file.

### Why preferred_id is passed for recovery but not required

The kernel may reject a preferred ID (taken by another process, or driver reloaded). The preferred ID is a hint for stable `/dev/ublkbN` paths, not a requirement. Stale entries in `ublk_devices.json` are silently ignored.

## Package Structure

| File | What It Does |
|------|-------------|
| `mod.rs` | `UblkServer`: add/remove/recover/shutdown devices; persists export→device-ID map to `ublk_devices.json`; probes `KernelFeatures` at startup |
| `device.rs` | `UblkDevice`: worker thread lifecycle; `QueueExecutor` + `WakeupBits` custom executor; per-tag `io_task` / `io_task_zc` I/O loops; zero-copy and buffered read/write dispatch |

## Configuration

| Constant / Option | Default | What It Controls at Runtime |
|------------------|---------|----------------------------|
| `QUEUE_DEPTH` | 128 | Inflight I/O capacity per queue; also the number of per-tag tasks spawned |
| `IO_BUF_BYTES` | 512 KB | Per-tag bounce buffer for non-zero-copy path; must fit largest supported block |
| `URING_IDLE_SECS` | 2 s | `io_uring_enter` timeout; bounds shutdown latency (queue thread wakes within this interval on kill) |
| `DATA_FILE_FD_INDEX` | 1 | Fixed-file slot index for data file in io_uring; slot 0 is the ublk cdev |
| `UblkServer::nr_queues` | 1 | Per-device queue count; more queues = per-CPU parallelism at cost of increased contention |
| `UblkServer::cache_dir` | None | Directory for `ublk_devices.json`; if unset, device IDs are ephemeral (new IDs on restart) |

## Failure Modes

| Failure | What Actually Happens | Recovery |
|---------|----------------------|---------|
| Queue thread fails to init (ring, queue, eventfd) | `latch.signal_failed()` → `on_started` sees failure → `kill_dev()` → `register()` returns `Err` | Caller retries `add_device()` |
| I/O dispatch error (handler returns `Err`) | Negative errno sent to kernel; kernel propagates to application | Application retries; no device state corruption |
| `pwrite` fails mid-write | Blocks are marked present but not dirty (phase 2 of 3-phase write) | Next read sees stale data; WAL replay corrects on recovery |
| `pre_write` fails | Blocks are neither present nor dirty | Safe to retry entire write |
| Worker thread doesn't exit within timeout | `unregister()` returns `Err` after `URING_IDLE_SECS + 5s` | Device may leak; operator must check `lsblk` |
| Daemon crash (server killed) | Kernel transitions device to `QUIESCED` state; I/O queued by kernel | `recover_quiesced_devices()` reattaches within seconds of restart |
| Preferred device ID already taken | Kernel auto-assigns a new ID; `/dev/ublkbN` path changes | `persist_devices()` updates map; callers re-resolve path |
| `persist_devices()` write fails | Warning logged; operation succeeds; device IDs will change on next restart | On restart, new IDs assigned; exports still functional |
| `kill_dev()` fails during drop | Warning logged; best-effort cleanup; device may remain registered | Operator must manually remove via `ublk del -n <id>` |
