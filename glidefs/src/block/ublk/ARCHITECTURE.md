# ublk Architecture

Takes I/O commands from the Linux ublk kernel driver over io_uring, dispatches them to a `BlockHandler`, and commits results back to the kernel — exposing a `/dev/ublkbN` block device for each named export.

The userspace side is a fixed-size **worker pool**: queues from thousands of devices are multiplexed across `K = min(num_cpus, 32)` worker threads, each owning one io_uring and one async executor. There is no per-device thread.

## Data Flow

```
                        ┌─────────────────────────────────────────┐
                        │           Linux Kernel (ublk)           │
                        │                                         │
                        │  /dev/ublkbN ──► io_uring SQE (cmd) ──► │
                        │               ◄── io_uring CQE (result)─│
                        └─────────────────────────┬───────────────┘
                                                  │  cdev fd per device
                                                  │  (registered as fixed file)
                ┌─────────────────────────────────┴──────────────────────────────┐
                │                                                                │
   ┌────────────▼────────────┐                                     ┌─────────────▼─────────┐
   │     Worker thread 0     │   ...    K − 1 more like this  ...  │   Worker thread K−1   │
   │  ┌───────────────────┐  │                                     │  ┌─────────────────┐  │
   │  │ io_uring (shared) │  │                                     │  │   io_uring      │  │
   │  │  fixed-file table │  │                                     │  │  (independent)  │  │
   │  │  (sparse, 4096)   │  │                                     │  └─────────────────┘  │
   │  └───────────────────┘  │                                     │                       │
   │  ┌───────────────────┐  │                                     │   QueueExecutor       │
   │  │  QueueExecutor    │  │                                     │   65 536 task slots   │
   │  │   ├─ io_task(D0,Q1,T0)                                     │   (bitmap-driven)     │
   │  │   ├─ io_task(D0,Q1,T1)         each task slot belongs      │                       │
   │  │   ├─ io_task(D1,Q2,T0)         to one (device, queue, tag) │                       │
   │  │   └─ … hosted queues for many devices                      │                       │
   │  └───────────────────┘  │                                     │                       │
   └────────────┬────────────┘                                     └─────────────┬─────────┘
                │ async dispatch (futures suspend on tokio work)                 │
                └─────────────────────────┬──────────────────────────────────────┘
                                          ▼
                              ┌─────────────────────────┐
                              │     BlockHandler        │
                              │ (WriteCache / S3 /      │
                              │  local-SSD read/write)  │
                              └─────────────────────────┘
```

Cross-thread wakeup (per worker):

```
tokio runtime ──► TaskWaker::wake()
              ──► WakeupBits::set(idx)    [fetch_or on AtomicU64]
              ──► write(efd, 1)           [worker's eventfd]
              ──► io_uring PollAdd CQE    [unblocks io_uring_enter on the worker]
              ──► QueueExecutor::tick()   [polls all set bits]
```

Error paths:

```
AddQueue fails        ──► ready oneshot returns Err ──► add_device returns Err
I/O dispatch error    ──► negative errno to kernel ──► caller sees EIO/EINVAL/etc.
Daemon crash          ──► kernel transitions devices to QUIESCED
                       ──► next daemon recover_quiesced_devices() reattaches
Worker pool drop      ──► all io_uring fds close ──► kernel quiesces all attached devices
```

## Concepts & Terminology

| Term | What It Controls | NOT |
|------|------------------|-----|
| `UblkServer` | Lifecycle of all devices (add, recover, remove); owns the `WorkerPool`; persists export→ID mapping | Does not handle I/O — only control plane |
| `WorkerPool` | A fixed set of `K` worker threads; routes per-queue messages to a worker | Not a thread-per-device pool — worker count is decoupled from device count |
| `Worker` | One OS thread, one io_uring, one `QueueExecutor`, one `EventFd`; hosts many queues from many devices | Not pinned to a device — it serves any queue routed to it |
| `WorkerRing` | `Rc<RefCell<IoUring<…>>>` shared inside one worker; sparse fixed-file table holds all hosted cdev fds | Not `Send` — pinned to its worker thread |
| `UblkDevice` | A data record of `{dev_id, dev_path, hosted_queues}`; lives in `UblkServer` | Not a thread — has none |
| `HostedQueue` | A single ublk queue (one `(dev, qid)`) installed inside a worker; owns its task-slot range | Not aware of other queues on the same worker |
| `QueueExecutor` | Single-threaded async executor inside a worker; polls futures when their bit is set | Not tokio — fully custom, no work stealing |
| `WakeupBits` | Runtime-sized atomic bitmask (`Box<[AtomicU64]>`); one bit per task slot | Not a queue — duplicates collapse (OR is idempotent) |
| `TaskWaker` | A worker-scoped waker carrying `(bits, idx, eventfd)`; wakes a single task slot | Not device-scoped — `idx` is global within the worker |
| `KernelFeatures` | Probed once at `UblkServer::new`: which ublk kernel features are available | Not per-device |
| `QUEUE_DEPTH` | 64 — max inflight I/Os per queue; also number of per-tag tasks | Not configurable at runtime |
| `IO_BUF_BYTES` | 512 KB — per-tag bounce buffer | Not the block size |
| `DeviceMode` | Whether to `Add` a fresh device or `Recover` an already-QUIESCED one | |

## Core Mechanism

### Worker pool

`K = min(num_cpus, 32)` worker threads are spawned at `UblkServer` construction.
Each worker:

1. Builds its own `io_uring` ring (`SINGLE_ISSUER`, sparse fixed-file table of `MAX_FIXED_FILES = 4096` slots).
2. Builds a `QueueExecutor` with a runtime-sized `WakeupBits` (`WORKER_WAKEUP_WORDS = 1024` → 65 536 task slots).
3. Owns one `EventFd` registered with the ring via `PollAdd`.
4. Is pinned to CPU `i % num_cpus` via `sched_setaffinity` (single-NUMA; multi-NUMA partitioning is left as a TODO).
5. Sits in an event loop: drain the `mpsc::Receiver<WorkerMsg>` between executor ticks, then `io_uring_enter` until the next CQE or eventfd wake.

Queues are assigned to workers by a hash:

```
worker_idx = (hash(export_name) ^ qid_seed(qid)) mod K
```

The `qid_seed` ensures a device's `nr_queues` queues land on distinct workers so per-device parallelism is preserved when `K ≥ nr_queues`.

### Adding a queue (control plane → worker)

`UblkServer::add_device` does the entire control-plane sequence on one `spawn_blocking` thread (control-plane uring is thread-local), then for each `qid in 0..nr_queues`:

1. Allocate a contiguous task-slot range of size `QUEUE_DEPTH` in the chosen worker's `QueueExecutor`.
2. Send `WorkerMsg::AddQueue { dev, qid, handler, slot_base, ready }` over the mpsc channel.
3. The worker registers the device's cdev fd into the next free slot of its sparse fixed-file table (via `register_files_update`), constructs a `UblkQueue::new(qid, &dev, &worker_ring)`, calls `ctrl.configure_queue`, then spawns `QUEUE_DEPTH` `io_task(q, tag)` futures, each carrying a `TaskWaker { bits, idx: slot_base + tag, efd }`.
4. The worker replies via the `ready` oneshot. If the executor is at capacity (`SpawnError::AtCapacity`) the worker fails soft — no panic, the error is propagated back to the API caller.

### Per-tag I/O task

Each tag's persistent future loops until shutdown:

```
submit_io_prep_cmd(tag)        // tell kernel we're ready
loop {
    get_iod(tag)               // read command descriptor
    dispatch_io(op, ...)       // async: may suspend on tokio (S3, write cache)
    submit_io_commit_cmd(tag)  // commit result + fetch next (combined SQE)
}
```

`submit_io_commit_cmd` combines result commit and next-command fetch into one kernel round-trip.

When a future suspends on tokio work, its `TaskWaker` will be invoked from a tokio worker thread; that wake flips a bit in the worker's `WakeupBits` and writes the worker's eventfd, which unblocks the worker's `io_uring_enter`.

### Wakeup path

Single mechanism — every waker always signals the eventfd by construction:

1. `TaskWaker::wake()` sets bit `idx` in the worker's `WakeupBits` via `fetch_or` (lock-free, O(1)).
2. Writes `1` to the worker's eventfd.

The eventfd is registered with the worker's io_uring via `PollAdd`. The write produces a CQE, which unblocks `io_uring_enter`. Duplicate wakes safely coalesce: `fetch_or` is idempotent, the eventfd counter just increments, and `tick()` drains everything in one pass via atomic swap-with-zero.

### Panic isolation

`QueueExecutor::tick` polls each task inside `catch_unwind`. A panic in one device's task is reported and the task is dropped; co-tenants on the same worker keep running. This is essential at the worker-pool topology: a bug serving one export must not take out every export hosted on that worker.

### Sparse fixed-file table

A worker's io_uring registers a sparse fixed-file table of `MAX_FIXED_FILES` slots. Each `UblkQueue` allocates exactly one slot for its cdev fd via `register_files_update`. SQEs use `types::Fixed(cdev_slot)` so the kernel reads/writes the right device. When a queue is removed, its slot is released (via `release_cdev_slot`) and may be reused. This is what lets a single shared ring serve many devices without conflict.

### Device persistence

`UblkServer` stores an `export_name → device_id` map to `ublk_devices.json` in `cache_dir`. On restart, the preferred device ID is passed to the kernel so `/dev/ublkbN` stays stable across daemon restarts. Stale IDs in the map are harmless — the kernel auto-assigns a new ID if the preferred one is taken.

## State Machines

### Device add lifecycle

```
add_device(name, handler)
        │
        ▼
spawn_blocking {                       // control-plane uring is thread-local
        UblkCtrlBuilder.build()        // /dev/ublkbN allocated
        UblkDev::new(...)              // shared device record (Arc)
        for qid in 0..nr_queues:
                send AddQueue → worker_for(name, qid)   // mpsc::blocking_send
                await ready oneshot                      // recv_blocking
                if Err(SpawnError::AtCapacity):
                        abort_device()                   // STOP+DEL, ready oneshots all fail
                        return Err
        ctrl.start_dev()
        ctrl.disarm_drop()             // ownership transferred; don't STOP+DEL on Drop
}
        │
        ▼
UblkDevice { dev_id, dev_path, hosted_queues } registered in UblkServer
add_device returns PathBuf (/dev/ublkbN)
```

### Recovery lifecycle (crash/restart)

```
[daemon restarts; some ublk devices are in QUIESCED state]
        │
        ▼
recover_quiesced_devices(get_handler)
        │ scan /sys/class/ublk-char/ for ublk devices owned by "glidefs"
        │ probe each in parallel (buffer_unordered(RECOVERY_CONCURRENCY))
        │ filter: state == QUIESCED && has matching handler
        │
        ▼
for each match (in parallel):
        spawn_blocking {
                START_USER_RECOVERY(dev_id)
                for qid in 0..nr_queues:
                        send AddQueue → worker_for(name, qid)
                END_USER_RECOVERY(dev_id)
        }
persist_devices()
returns recovered_count

[orphan sweep]
        │
        ▼
for each ublk-char device NOT recovered and NOT in API exports:
        kill_dev(dev_id) + del_dev(dev_id)  (parallel, ORPHAN_SWEEP_CONCURRENCY)
```

### Worker lifecycle

```
WorkerPool::new(K) → spawns K threads:

        bind CPU affinity (i % num_cpus)
        build io_uring + sparse fixed-file table
        build QueueExecutor + WakeupBits
        register EventFd via PollAdd
        loop {
                drain mpsc::Receiver<WorkerMsg>:
                        AddQueue { ... }     → install queue, reply ready
                        RemoveQueue { ... }  → drain inflight, deregister cdev slot
                        Shutdown             → break
                exe.tick()
                if no hosted queues and no Shutdown:
                        io_uring_enter(URING_IDLE_SECS)   // block until next signal
                else:
                        io_uring_enter(0)                 // poll
        }
        all io_uring fds close → kernel quiesces all attached devices
```

## Why It Behaves This Way

### Why a worker pool instead of one thread per device

Per-device threading is `O(N)` in OS threads, io_urings, and registered virtual memory. At 1000 devices × 4 queues × 64 tags × 512 KB = 128 GB of virtual address space and ~5000 threads. The worker pool collapses this to `K` threads and `K` rings — the io_uring fixed-file table is sized for the device count, but everything else is `O(K)`.

### Why a custom executor instead of tokio for the I/O tasks

The ublk protocol pins one io_uring per submitter (`SINGLE_ISSUER`). Tokio's work-stealing scheduler would move futures across threads, breaking that invariant. `QueueExecutor` pins all futures to one thread and uses `Rc<RefCell<…>>` for the ring, making the single-issuer constraint a type-system property.

### Why the eventfd is in every waker

The alternative — a condvar — would require the worker thread to check a flag after every `io_uring_enter` timeout. With eventfd-in-every-waker, any tokio future completing immediately unblocks `io_uring_enter`, keeping latency low. The kernel handles the race between the tokio thread's `write` and the worker thread's `PollAdd` safely.

### Why hash queues across workers instead of binding a device to one worker

A device's queues need to run concurrently to use its full `nr_queues × queue_depth` parallelism. Pinning a device to one worker would cap single-device throughput at one CPU. The XOR-with-qid distribution ensures the 4 queues of a single device land on 4 distinct workers (when `K ≥ 4`), preserving per-device parallelism while still multiplexing many devices into `K` worker threads.

### Why `spawn` returns `Result<(), SpawnError>` instead of panicking

`WORKER_WAKEUP_WORDS = 1024` → 65 536 task slots per worker. With 4 queues × 64 tags per device, that allows ~256 devices on a single worker before saturation. If the hash distribution piles devices onto one worker faster than expected, the API should return an error to the caller (`add_device` → 500) rather than panic the worker thread, which would take out every co-tenant queue.

### Why shutdown does not call kill_dev

`kill_dev` (UBLK_CMD_STOP_DEV) blocks until inflight bios drain, which requires the userspace COMMIT loop. Calling it while VM writers hold dirty bios deadlocks: the bios won't drain because the userspace daemon is exiting and can't COMMIT. With `UBLK_F_USER_RECOVERY` (always on), simply dropping the worker pool — closing all io_uring fds — causes the kernel to transition each device to QUIESCED. The next daemon recovers them in seconds. `kill_dev` is reserved for explicit `remove_device` (operator removed an export), where the caller is responsible for ensuring nothing is using it.

## Package Structure

| File | What It Does |
|------|-------------|
| `mod.rs` | `UblkServer`: add/remove/recover devices; owns the `WorkerPool`; persists export→device-ID map to `ublk_devices.json`; probes `KernelFeatures` at startup; orphan sweep on recovery |
| `worker_pool.rs` | `WorkerPool` + `Worker` + `WorkerMsg`: fixed-size thread pool, mpsc message routing, CPU affinity, runtime-sized `WakeupBits`, hash-based queue assignment |
| `device.rs` | `UblkDevice` record + `QueueExecutor` + `WakeupBits` + `TaskWaker` + per-tag `io_task` I/O loop + `dispatch_io` / `handle_io` |

## Configuration

| Constant / Option | Default | What It Controls at Runtime |
|------------------|---------|----------------------------|
| `K` (workers) | `min(num_cpus, 32)` | Number of worker threads; each owns one io_uring |
| `WORKER_WAKEUP_WORDS` | 1024 | `WakeupBits` size per worker → 65 536 task slots per worker |
| `MAX_FIXED_FILES` | 4096 | io_uring sparse fixed-file slots per worker (one slot per hosted cdev fd) |
| `QUEUE_DEPTH` | 64 | Inflight I/O capacity per queue; also the per-tag task count |
| `IO_BUF_BYTES` | 512 KB | Per-tag bounce buffer |
| `URING_IDLE_SECS` | 2 s | `io_uring_enter` timeout when a worker has no hosted queues |
| `RECOVERY_CONCURRENCY` | 16 | Parallel reattaches at startup |
| `ORPHAN_SWEEP_CONCURRENCY` | 16 | Parallel kill+del for unowned ublk-char devices |
| `UblkServer::nr_queues` | from `[servers.ublk].nr_queues` (config) | Per-device queue count |
| `UblkServer::cache_dir` | None | Directory for `ublk_devices.json`; if unset, device IDs are ephemeral |

## Failure Modes

| Failure | What Actually Happens | Recovery |
|---------|----------------------|---------|
| `register_files_update` fails on AddQueue | `ready` oneshot returns `Err`; `add_device` rolls back the device | Caller retries; no leaked kernel device |
| `QueueExecutor::spawn` at capacity | `SpawnError::AtCapacity` returned via `ready` oneshot; `add_device` aborts | Caller sees 500; operator distributes load or raises `WORKER_WAKEUP_WORDS` |
| A task panics inside `tick()` | `catch_unwind` captures it; task is dropped; co-tenants keep running | The kernel sees no COMMIT for that tag and may eventually timeout the I/O |
| I/O dispatch error (handler returns `Err`) | Negative errno sent to kernel; kernel propagates to application | Application retries; no device state corruption |
| Daemon crash (`kill -9`) | All io_uring fds close → kernel quiesces every attached device | `recover_quiesced_devices()` reattaches all devices within seconds of next start |
| Daemon graceful exit (SIGTERM) | Worker pool dropped; same path as crash. **`kill_dev` is intentionally not called.** | Same as crash |
| Worker thread panics outside `tick()` | Worker join handle reports the error on shutdown | Pool exits; daemon restarts via systemd; recovery reattaches devices |
| `kill_dev()` fails during explicit `remove_device` | Warning logged; best-effort cleanup; export is removed from the API | Orphan sweep on the next daemon start kills the leftover kernel device |
| `persist_devices()` write fails | Warning logged; operation succeeds; device IDs may change on next restart | New IDs on restart; exports still functional |
