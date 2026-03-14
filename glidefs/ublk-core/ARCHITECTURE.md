# ublk-core Architecture

Takes kernel-delivered block I/O commands from `/dev/ublkcN` via io_uring and dispatches them to Rust async handlers, then commits results back to the kernel. The block device (`/dev/ublkbN`) becomes visible to the Linux block layer only after the userspace daemon has started and registered its queues.

## Data Flow

### Happy Path (per-queue I/O loop)

```
Kernel block layer
      │ issues I/O to /dev/ublkbN
      ▼
UblkQueue::submit_io_prep_cmd(tag, buf_desc)
      │ UBLK_U_IO_FETCH_REQ sqe → io_uring
      ▼
io_uring CQE arrives (ublk_wake_task delivers result)
      │
      ▼
UblkIOCtx — caller reads iod = queue.get_iod(tag)
      │ ublksrv_io_desc: op (read/write/flush/discard), lba, sectors, buf_addr
      │
      ▼
BlockHandler::handle_io() — application logic
      │ reads/writes data, resolves result
      ▼
UblkQueue::submit_io_commit_cmd(tag, buf_desc, result)
      │ UBLK_U_IO_COMMIT_AND_FETCH_REQ sqe → io_uring
      │ commits result AND pre-fetches next command for this tag
      ▼
Next I/O command arrives for this tag (loop repeats)
```

### Rejection / Error Paths

```
submit_io_prep_cmd() while is_stopping = true
      └─► return Ok(()) immediately — no submit, tag abandoned

CQE result = UBLK_IO_RES_ABORT
      └─► propagate to handler → queue tears down after cmd_inflight = 0

mlock() fails during buffer registration
      └─► state.mlock_failed = true → notify_buffer_registration_complete(true)
          └─► Device waits for all queues → returns EPERM → abort startup

Buffer descriptor type mismatch (e.g., AutoReg on device without UBLK_F_AUTO_BUF_REG)
      └─► validate_compatibility() → UblkError::InvalidVal
```

### Device Lifecycle

```
UblkCtrlBuilder::build()
      │ open("/dev/ublk-control"), init control io_uring
      │ ioctl UBLK_CMD_ADD_DEV → kernel assigns dev_id
      ▼
UblkDev::new(tgt_init_fn, &ctrl)
      │ open("/dev/ublkcN") with 3s retry loop
      │ tgt_init_fn(&mut dev) — set_default_params(), target metadata
      ▼
ctrl.run_target() — spawns one thread per queue
      │
      ├─ per-queue thread: UblkQueue::new(q_id, &dev)
      │    mmap io_cmd_buf, register_files, register_buffers_sparse (if AUTO_BUF_REG)
      │    submit_fetch_commands_unified() — batch-submit q_depth FETCH_REQ sqes
      │    async loop: wait CQE → handle → commit+fetch
      │
      └─ /dev/ublkbN appears to Linux block layer
```

### Recovery (after crash)

```
UblkCtrl::new(flags=UBLK_DEV_F_RECOVER_DEV, id=dev_id)
      │ read_dev_info() — ioctl recovers existing kernel device
      │ reload_json() — restore target params from /run/ublk/{id}.json
      ▼
ctrl.start_user_recover()
      │ ioctl UBLK_CMD_START_USER_RECOVER — kernel enters QUIESCED state
      ▼
UblkDev::new() + queue threads restart
      │ submit_fetch_commands_unified() re-registers queues
      ▼
If UBLK_F_USER_RECOVERY_REISSUE: kernel re-delivers in-flight I/Os
```

### Buffer Registration Barrier (UBLK_DEV_F_MLOCK_IO_BUFFER)

```
UblkQueue::submit_fetch_commands_unified(BufDescList::Slices(Some(bufs)))
      │
      ├─ for each tag: register_io_buf_internal(tag, &bufs[tag])
      │    buf.mlock() — lock pages in RAM (requires CAP_IPC_LOCK)
      │    store ptr in queue.bufs[tag]
      │    increment buf_reg_counter
      │
      └─ when counter == q_depth: add_permits(q_depth) → unblock semaphore

Each tag's async task: submit_io_prep_cmd()
      └─ wait_for_all_buffer_registrations().await
           └─ semaphore.acquire() — blocks until all buffers registered

Device main thread: wait_for_buffer_registration(nr_hw_queues)
      └─ Condvar waits until all queues report complete (or mlock_failed)
```

## Concepts & Terminology

| Term | What It Controls | NOT |
|------|-----------------|-----|
| `UblkCtrl` | Device lifecycle: create, start, stop, recover; talks to `/dev/ublk-control` | Not I/O dispatch |
| `UblkDev` | Shared device config accessed by all queue threads; wraps kernel `ublksrv_ctrl_dev_info` | Not per-queue state |
| `UblkQueue` | One io_uring ring per queue thread; submits FETCH/COMMIT commands; owns `io_cmd_buf` | Not shared across threads |
| `UblkIOCtx` | Wraps a single CQE; gives handler access to the I/O descriptor (`get_iod`) | Not the ring or queue state |
| `BufDesc` | Specifies where data lives for one I/O: copy slice, zero-copy auto-reg, or zone-append LBA | Not the buffer allocator |
| `IoBuf<T>` | 4096-aligned allocation with optional mlock; the actual memory backing an I/O buffer | Not the ublk buffer descriptor |
| `tag` | Per-queue slot index (0..q_depth); one in-flight I/O per tag at a time | Not a global I/O ID |
| `io_cmd_buf` | mmap'd kernel buffer; holds `ublksrv_io_desc` for each tag (read by `get_iod`) | Not where I/O data lives |
| FETCH_REQ | Submit sqe: "give me the next command for this tag" | Not the same as reading data |
| COMMIT_AND_FETCH | Submit sqe: "I'm done with tag T (result=N), give me the next command" | Not a separate commit + separate fetch |
| QUIESCED | Kernel device state after daemon crash; holds I/Os until recovery daemon re-registers | Not stopped or deleted |

## Core Mechanism

### io_uring Protocol

Every queue maintains one thread-local io_uring ring. The protocol per tag is:

1. Submit `UBLK_U_IO_FETCH_REQ` sqe — tells kernel "I'm ready for a command on tag T"
2. Kernel delivers I/O from the block layer as a CQE
3. Read `io_cmd_buf[tag]` → `ublksrv_io_desc` (op, start_sector, nr_sectors, addr)
4. Handle the I/O (read from/write to the backing store)
5. Submit `UBLK_U_IO_COMMIT_AND_FETCH_REQ` sqe — commits result and pre-fetches next

The kernel guarantees that `io_cmd_buf[tag]` is stable between FETCH and COMMIT for the same tag. Tags are independent: q_depth tags run concurrently within one queue thread via async/await.

### Async Futures

`UblkUringOpFuture` wraps one sqe submission. On poll, it encodes `user_data` with a waker token stored in a thread-local slab. When the CQE arrives in `run_uring_tasks`, `ublk_wake_task(user_data, cqe)` writes the result into the slab and calls the waker. The future resolves to `i32` (CQE result).

This means there are no OS thread blocks or mutexes in the hot path — the queue thread drives its own event loop via `run_uring_tasks` / `wait_and_handle_io_events`.

### Buffer Descriptor Dispatch

```rust
pub enum BufDesc<'a> {
    Slice(&'a [u8]),             // userspace copy (always supported)
    AutoReg(ublk_auto_buf_reg),  // zero-copy DMA (UBLK_F_AUTO_BUF_REG)
    ZonedAppendLba(u64),         // zone append result LBA (UBLK_F_ZONED)
    RawAddress(*const u8),       // unsafe escape hatch
}
```

`validate_compatibility(dev_flags)` enforces that `AutoReg` is only used when the kernel supports `UBLK_F_AUTO_BUF_REG`. Callers that use `BufDescList` for batch fetches get the same dispatch at queue startup.

### Fixed File Registration

Each queue ring registers two fixed files on init:
- Slot 0: `/dev/ublkcN` (character device fd)
- Slot 1: data file (backing store fd, if any)

All control/I/O sqes reference these by fixed index, avoiding per-sqe `SCM_RIGHTS` overhead.

## State Machine

### Queue State

```
INIT ──submit_fetch_commands_unified()──► RUNNING
                                              │
                                    cmd_inflight > 0 while
                                    handling I/Os concurrently
                                              │
                              kill_dev() sets is_stopping = true
                                              │
                                    cmd_inflight drops to 0
                                    + CQE_ABORT received
                                              │
                                              ▼
                                           STOPPED (thread exits)
```

| State | `cmd_inflight` | `is_stopping` | Behavior |
|-------|---------------|---------------|----------|
| INIT | 0 | false | Buffers being registered |
| RUNNING | > 0 | false | Normal I/O dispatch |
| DRAINING | > 0 | true | No new submits; completing inflight |
| STOPPED | 0 | true | Thread exits loop |

### Device Lifecycle State

```
─► CREATED ──start_dev()──► LIVE ──kill_dev()──► DEAD
                               │
                          crash / kill
                               │
                               ▼
                          QUIESCED ──start_user_recover()──► RECOVERING ──► LIVE
```

## Why It Behaves This Way

### Why one io_uring ring per queue thread

Each queue thread owns its ring exclusively via `thread_local!`. This eliminates all lock contention on the submission/completion path. The tradeoff: control-plane ioctls require a separate ring (`CTRL_URING`), also thread-local to the control thread.

### Why COMMIT_AND_FETCH instead of separate COMMIT + FETCH

The kernel fuses the two operations: committing a result immediately makes the tag available for a new command, and the fetch pre-arms it. This halves the number of io_uring syscalls on the steady-state path.

### Why the 3-second retry loop opening `/dev/ublkcN`

The kernel creates the character device asynchronously after `UBLK_CMD_ADD_DEV` returns. The cdev may not exist yet when userspace tries to open it. The loop retries every 100ms rather than synchronizing via poll/inotify because the window is typically < 10ms and the code avoids adding another fd to watch.

### Why JSON persistence at `/run/ublk/{id}.json`

Recovery requires the daemon to know the target's parameters (sector count, block size, etc.) without being able to reconstruct them from the kernel. The kernel holds device state in `QUIESCED` across daemon crashes; userspace holds target metadata in JSON. Together they make recovery fully stateless from the application's perspective.

### Why `BufDesc::AutoReg` packs index+flags into `sqe.addr`

The `UBLK_F_AUTO_BUF_REG` protocol encodes buffer info into the sqe rather than passing it out-of-band. `ublk_auto_buf_reg_to_sqe_addr()` / `ublk_sqe_addr_to_auto_buf_reg()` are the FFI helpers that pack/unpack this encoding. The unified `BufDesc` enum keeps this detail invisible to callers.

## Package Structure

| File | What It Does |
|------|-------------|
| `src/lib.rs` | `UblkError` enum, `UblkFlags` bitflags, module re-exports |
| `src/ctrl.rs` | `UblkCtrl` + `UblkCtrlBuilder`: device create/recover/destroy; `UblkJsonManager`: persist target metadata to `/run/ublk/`; `UblkQueueAffinity`: CPU pinning |
| `src/io.rs` | `UblkDev`: shared device config; `UblkQueue`: per-queue io_uring ring, FETCH/COMMIT submission; `BufDesc`/`BufDescList`: buffer descriptor types; `UblkIOCtx`: CQE wrapper |
| `src/helpers.rs` | `IoBuf<T>`: 4096-aligned allocation with mlock/munlock; `Drop` implementation cleans up |
| `src/uring_async.rs` | `UblkUringOpFuture`: per-sqe async future backed by thread-local slab; `run_uring_tasks`: core event loop; `wait_and_handle_io_events`: idle-timeout aware wrapper |
| `src/bindings.rs` / `src/sys.rs` | Re-export ublk-sys FFI types |
| `ublk-sys/src/lib.rs` | bindgen-generated bindings to `<linux/ublk_cmd.h>` + `ublk_auto_buf_reg` helpers |

## Configuration

### UblkCtrlBuilder fields

| Field | Default | What It Controls |
|-------|---------|-----------------|
| `nr_queues` | 1 | Number of queue threads spawned; each gets its own io_uring ring |
| `depth` | 64 | Slots per queue (tags 0..depth); bounds max concurrent I/Os per queue |
| `io_buf_bytes` | 512 KiB | Size of each I/O buffer in the pre-allocated pool |
| `ctrl_flags` | 0 | `UBLK_F_*` kernel feature flags (USER_RECOVERY, AUTO_BUF_REG, etc.) |
| `dev_flags` | 0 | `UBLK_DEV_F_*` daemon behavior flags (MLOCK_IO_BUFFER, COMP_BATCH, etc.) |
| `name` | `""` | Device name stored in JSON; used to look up target state on recovery |

### Key Device Flags

| Flag | Runtime Effect |
|------|---------------|
| `UBLK_DEV_F_MLOCK_IO_BUFFER` | mlock all I/O buffers on queue init; requires `CAP_IPC_LOCK`; blocks startup until all queues complete |
| `UBLK_DEV_F_RECOVER_DEV` | Read device info from kernel (device already exists); reload JSON for target params |
| `UBLK_DEV_F_COMP_BATCH` | Enable `UblkFatRes` — one CQE can complete multiple tags (requires `fat_complete` cargo feature) |
| `UBLK_DEV_F_SINGLE_CPU_AFFINITY` | Pin each queue thread to one CPU via `get_random_cpu()`; otherwise uses the full affinity set |

### Kernel Feature Flags (queried via `detect_features()`)

| Flag | Required For |
|------|-------------|
| `UBLK_F_USER_RECOVERY` | Device survives daemon crash; kernel holds I/Os in QUIESCED |
| `UBLK_F_USER_RECOVERY_REISSUE` | Kernel re-delivers in-flight I/Os to recovering daemon |
| `UBLK_F_AUTO_BUF_REG` | Zero-copy DMA buffers via `BufDesc::AutoReg`; kernel 6.14+ |
| `UBLK_F_USER_COPY` | Kernel copies I/O data on behalf of userspace (older alternative to AUTO_BUF_REG) |
| `UBLK_F_ZONED` | Zoned block device support; enables `BufDesc::ZonedAppendLba` |

## Failure Modes

| Failure | What Actually Happens | Recovery |
|---------|----------------------|----------|
| Daemon crash while I/Os in-flight | Kernel enters QUIESCED; block layer stalls I/Os | New daemon calls `start_user_recover()`; if `REISSUE`, kernel re-delivers |
| `mlock()` fails (no `CAP_IPC_LOCK`) | `state.mlock_failed = true`; queue notifies device; device startup returns `EPERM` | Drop `UBLK_DEV_F_MLOCK_IO_BUFFER` flag or grant capability |
| `/dev/ublkcN` not yet present | Retry loop (100ms intervals, 3s timeout) in `UblkDev::new()` | If 3s exceeded, returns `IOError(EACCES)` |
| CQE result = `UBLK_IO_RES_ABORT` | Queue sets `is_stopping`; drains inflight; thread exits | Control plane calls `kill_dev()` to initiate; expected on shutdown |
| `io_uring` submission error | `UblkError::UringIOError(errno)` returned to handler | Handler decides: propagate or retry |
| Buffer type mismatch | `validate_compatibility()` returns `UblkError::InvalidVal` | Fix caller to use correct `BufDesc` variant |
| Control ring not initialized | Panic in `with_ctrl_ring!()` macro | Always init via `UblkCtrl::new()` before using control methods |

## Trust Boundaries

**What the system checks:**
- `BufDesc` variant compatibility with device flags (`validate_compatibility`)
- Tag bounds (buf access uses `tag as usize` index into pre-allocated vec; vec length = q_depth)
- JSON schema validity on reload (serde deserialization errors propagate)

**What passes through unchecked:**
- I/O descriptor content — `ublksrv_io_desc` values (sector ranges, op codes) come from the kernel; the handler must bounds-check them against the backing store
- Handler return values — the `result` passed to `submit_io_commit_cmd` is forwarded verbatim to the kernel; negative errno values are expected
- Recovery JSON — no integrity check on `/run/ublk/{id}.json`; corrupted file produces deserialization error

**Why these boundaries exist:**
- The kernel is trusted to deliver valid `ublksrv_io_desc` structures; validating them in userspace would duplicate kernel checks
- I/O buffer bounds are enforced at the `IoBuf::subslice()` / `subslice_mut()` level when handlers slice the buffer, not at the ublk protocol layer
