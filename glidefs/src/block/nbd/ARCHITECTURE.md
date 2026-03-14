# nbd Architecture

Takes a named export backed by a `BlockHandler` and exposes it as a `/dev/nbdN` Linux block device — wiring the kernel NBD driver to the NBD session handler via a Unix socketpair and registering the device through generic netlink.

## Data Flow

```
                   ┌──────────────────────────────────────────────────────┐
                   │                  Linux Kernel                        │
                   │                                                      │
                   │   /dev/nbdN ◄──► NBD driver ◄──► socketpair[0]       │
                   └──────────────────────────────────────────────────────┘
                                                             │  blocking Unix socket
                                               ┌────────────▼────────────────┐
                                               │  handle_client_stream()     │
                                               │  (block/server.rs)          │
                                               │  reads NBD commands,        │
                                               │  dispatches to BlockHandler │
                                               └────────────┬────────────────┘
                                                            │
                                               ┌────────────▼────────────────┐
                                               │  BlockHandler               │
                                               │  WriteCache + S3            │
                                               └─────────────────────────────┘

Handshake (at device creation):
  client_handshake() ──► socketpair[1]
    1. read server magic + flags
    2. write client flags
    3. write NBD_OPT_GO (export name)
    4. read NBD_REP_INFO* + NBD_REP_ACK
  [socketpair[1] handed to kernel via netlink]
```

**Hot reload path (zero-downtime restart):**

```
Old process drops NbdDeviceManager (no shutdown)
  → nbd_devices.json persists                 [kernel keeps /dev/nbdN alive]
  → dead_conn_timeout queues I/O in kernel    [default 30s]
  ↓
New process calls add_device("foo")
  → loads nbd_devices.json → preferred_index = N
  → /sys/block/nbdN/pid exists? → device alive
  → create new socketpair + handshake
  → NBD_CMD_RECONFIGURE with new socket fd    [kernel swaps fds atomically]
  → /dev/nbdN path unchanged                  [no remount needed]
```

**Error paths:**

```
client_handshake fails  → cancel session token → abort session task → return Err
netlink EBUSY/EINVAL    → retry (max 10×, 100ms delay) → transient kernel teardown
netlink permanent error → return Err immediately (ENOSPC, EPERM, etc.)
device not exclusive    → poll O_EXCL every 50ms, max 5s → Err on timeout
```

## Concepts & Terminology

| Term | What It Controls | NOT |
|------|-----------------|-----|
| `NbdDeviceManager` | Lifecycle of all `/dev/nbdN` devices for one server process | Does not handle I/O — only device creation and wiring |
| `NbdDevice` | One device's session task + shutdown token | Not a connection — the kernel holds the actual connection |
| socketpair | The pipe connecting kernel NBD driver to the session handler | Not a network socket — Unix domain, in-process |
| `dead_conn_timeout` | Seconds the kernel queues I/O when the socket dies | Not a read/write timeout — only applies to socket-closed state |
| `preferred_index` | Requested device number for stable `/dev/nbdN` path | Not guaranteed — kernel may assign a different index |
| `nbd_devices.json` | Persisted export→device-index map enabling hot reload | Not authoritative — stale entries are silently ignored |
| `NBD_CMD_RECONFIGURE` | Swaps the socket fd without removing the device | Not a reconnect — device stays mounted, I/O resumes |
| `NBD_CMD_CONNECT` | Creates a new `/dev/nbdN` or reclaims a specific index | Not idempotent — will create a new device if index is free |
| `TRANSMISSION_FLAGS` | Bitmask of NBD capabilities advertised to the kernel | Not a negotiation — kernel accepts what server offers |

## Core Mechanism

### Socketpair Wiring

When `add_device()` is called, the module creates a Unix socketpair. The two ends serve distinct roles:

- **`server_stream` (fd[0])**: Owned by `handle_client_stream()` in a spawned tokio task. The session handler reads NBD commands, dispatches to the `BlockHandler`, and writes results back.
- **`client_stream` (fd[1])**: Used for the initial handshake, then transferred to the kernel via netlink. After `NBD_CMD_CONNECT`, the kernel NBD driver sends I/O commands down this socket.

**Critical:** Tokio sets sockets to non-blocking mode. The kernel NBD driver uses blocking `kernel_recvmsg`. Non-blocking sockets cause the kernel to interpret `EAGAIN` as fatal `EIO`. Before passing `client_stream` to the kernel, the socket is explicitly switched back to blocking (`set_nonblocking(false)`).

After netlink registration, the fd is released with `into_raw_fd()` so Rust's `Drop` does not close it — ownership transfers to the kernel.

### Client Handshake

The `client_handshake()` function runs on `client_stream` immediately after socketpair creation, before the fd is handed to the kernel. It negotiates a named export with the session handler:

```
Server → Client:  [8B] NBD_MAGIC (0x4e42444d41474943)
                  [8B] NBD_IHAVEOPT (0x49484156454f5054)
                  [2B] flags: FIXED_NEWSTYLE | NO_ZEROES

Client → Server:  [4B] client flags: FIXED_NEWSTYLE | NO_ZEROES

Client → Server:  [8B] ihaveopt
                  [4B] NBD_OPT_GO (7)
                  [4B] data length
                  [4B] export name length
                  [NB] export name bytes
                  [2B] info requests count (0)

Server → Client:  [8B] reply magic (0x3e889045565a9)   ← NBD_REP_INFO (repeated)
                  [4B] NBD_OPT_GO
                  [4B] NBD_REP_INFO (3)
                  [4B] data length
                  ...data (export size, flags, etc.)...

                  [8B] reply magic
                  [4B] NBD_OPT_GO
                  [4B] NBD_REP_ACK (1)     ← handshake complete
                  [4B] 0
```

After `NBD_REP_ACK`, both sides enter **transmission mode**. The kernel NBD driver then drives the protocol on its end; the session handler on `server_stream` processes I/O commands.

If the server sends an error reply type (`≥ 0x80000000`), `client_handshake()` returns an error, the session task is cancelled, and `add_device()` fails.

### Device Index Persistence

`nbd_devices.json` maps export names to device indices. This file:
- Is written after each successful `add_device()`
- Is **not deleted** when the process drops `NbdDeviceManager` (hot reload path)
- Is **deleted** on `shutdown()` (clean exit path)
- Is silently ignored if missing or corrupted

On `add_device()`, if a persisted index exists:
1. Check `/sys/block/nbdN/pid` — is the device still alive in kernel?
2. **Alive**: call `NBD_CMD_RECONFIGURE` (swap socket fd, I/O queued)
3. **Dead**: call `NBD_CMD_CONNECT` with `preferred_index` (reclaim same number)

Stale indices (from a prior clean shutdown that left a stale file) are harmless: `NBD_CMD_CONNECT` with a free index just creates the device there.

### Exclusive Access Wait

After device creation or recovery, `wait_for_exclusive_access()` polls the device with `O_EXCL | O_RDONLY` every 50 ms, up to 5 seconds. This detects when a prior filesystem (ext4, etc.) has fully released the device. The `O_EXCL` probe matches what `e2fsck` and similar tools use to check device availability.

### Netlink Protocol

`netlink.rs` implements the `NBD_GENL` generic netlink family from scratch using raw `libc` syscalls. The key operations:

| Command | Netlink Attributes | Kernel Effect |
|---------|-------------------|---------------|
| `NBD_CMD_CONNECT` | index (opt), size, block_size, flags, socket fd, timeout | Creates `/dev/nbdN`; starts I/O processing |
| `NBD_CMD_DISCONNECT` | index | Removes `/dev/nbdN`; terminates I/O |
| `NBD_CMD_RECONFIGURE` | index, socket fd | Swaps socket; queues I/O for `dead_conn_timeout` seconds |

**Auto-assign vs preferred index**: Without `preferred_index`, the kernel chooses a free index and returns it in a genl response. With `preferred_index`, the kernel returns an `NLMSG_ERROR` ACK (errno=0 on success). The response format differs, and older kernels may not return the index at all in the genl path — hence three fallback strategies in `connect()`: parse genl response → check ACK payload → scan `/sys/block/` for newest nbd device.

**Family ID resolution**: The first call to any NBD netlink operation queries `GENL_ID_CTRL` (`nlattr CTRL_CMD_GETFAMILY`) to get the dynamic family ID for "nbd". If the nbd kernel module is not loaded, this returns a helpful error message rather than an opaque errno.

## State Machine

### Device Lifecycle

```
[add_device("foo")]
        │
        ├── "foo" already in devices? ──► Err("already registered")
        │
        ▼
  load nbd_devices.json → preferred_index
  create socketpair
  spawn session task (server_stream)
        │
        ▼
  client_handshake(client_stream) ─────────────────────────► Err
        │ success                                              │ cancel session task
        ▼                                                      ▼
  set_nonblocking(false) on client_stream                    return Err
        │
        ▼
  retry loop (max 10, 100ms backoff):
    ├── preferred_index + device_alive? → NBD_CMD_RECONFIGURE
    │     ├── Ok  ─────────────────────────────────────────────► [running, path stable]
    │     └── EINVAL (dying) ──► fall through to CONNECT
    ├── preferred_index + device_dead? → NBD_CMD_CONNECT(preferred_index)
    │     └── Ok ──────────────────────────────────────────────► [running, path reclaimed]
    └── no preferred_index → NBD_CMD_CONNECT(auto)
          └── Ok ──────────────────────────────────────────────► [running, new path]
    [EBUSY/EINVAL → retry; other errno → Err]
        │
        ▼
  wait_for_exclusive_access() ──── timeout 5s ──────────────► Err
        │ exclusive
        ▼
  persist nbd_devices.json
  return PathBuf("/dev/nbdN")
```

| From | Event | To | What Actually Happens |
|------|-------|----|-----------------------|
| absent | `add_device()` | running | socketpair created, session spawned, kernel device created |
| running | `remove_device()` | absent | kernel device disconnected, session task cancelled |
| running | process drop (hot reload) | running | devices stay alive in kernel; nbd_devices.json persists |
| running | `shutdown()` | absent | all devices disconnected, nbd_devices.json deleted |
| running | kernel EBUSY | retrying | 100ms wait, retry up to 10× |
| retrying | max retries | absent | session task cancelled, return Err |

### Connection Lifecycle (per device)

```
socketpair created
        │
        ▼
client_handshake ──────────────────► failed → session cancelled
        │ success
        ▼
socket set to blocking
        │
        ▼
fd handed to kernel (NBD_CMD_CONNECT / RECONFIGURE)
        │
        ▼
[transmission phase]
kernel sends I/O commands → server_stream → session handler → BlockHandler
        │
  disconnect / process exit
        ▼
kernel closes fd
session task sees EOF → handle_client_stream returns
```

## Why It Behaves This Way

### Why Unix socketpair instead of a real TCP/Unix socket server

The NBD protocol requires a listening server, but the kernel driver is on the same machine. A socketpair eliminates the networking stack, port allocation, and auth entirely — the kernel and session handler communicate in-process. This also means there's no exposed port for remote clients to connect to the internal protocol.

### Why the socket must be blocking before kernel handoff

Tokio sets all sockets to non-blocking because its event loop depends on `EAGAIN` to know when to yield. The kernel NBD driver, however, uses `kernel_recvmsg` in blocking mode and treats `EAGAIN` from a non-blocking socket as a fatal connection error, returning `EIO` to every I/O request. The explicit `set_nonblocking(false)` call undoes Tokio's default before the fd is transferred.

### Why NBD_CMD_RECONFIGURE instead of disconnect + reconnect

Disconnect removes the block device from the kernel. If the device is mounted, the filesystem must unmount, which requires flushing dirty pages. The remount on the new process takes additional time. With `NBD_CMD_RECONFIGURE`, the device persists — the kernel queues pending I/O for `dead_conn_timeout` seconds while the new socket is being wired up. The filesystem never sees an unmount event; the block device comes back within milliseconds after the new process is ready.

### Why persist nbd_devices.json but delete it on clean shutdown

On clean shutdown, the kernel devices are explicitly disconnected (via `NBD_CMD_DISCONNECT`). The next startup should not try to reclaim indices that no longer exist. On hot reload (process drop without shutdown), devices are alive and the persisted map enables reconfiguration. Deleting only on clean shutdown makes the file presence an accurate signal: "file exists = devices may still be running in kernel."

### Why retry on EBUSY and EINVAL specifically

`EBUSY` (errno 16): The kernel is in the middle of tearing down the old NBD session. The device exists but is not yet accepting new connections. `EINVAL` (errno 22): The device pid file has not been cleaned up yet even though the device is dying. Both are transient conditions during the brief window between process restart and kernel cleanup. Other errno values (EPERM, ENOSPC, etc.) are permanent and retrying would be wasteful.

### Why O_EXCL for the exclusivity check

`O_EXCL` on a block device is the standard Linux mechanism for exclusive access — the same flag used by `e2fsck`, `mkfs`, and `mount`. Probing with `O_EXCL` detects whether any other kernel subsystem (filesystem layer, fsck) still holds the device. A raw `open()` without `O_EXCL` would succeed even if the device is in use, giving a false positive.

### Why three fallback strategies for auto-assign device index

Kernel versions differ in how they return the assigned index for `NBD_CMD_CONNECT` without `preferred_index`:
1. Some return it in a genl response before the ACK
2. Some batch the index into the `NLMSG_ERROR` ACK payload
3. Some return nothing (old kernels)

Rather than gating on a kernel version check, the code tries each strategy in sequence. The last resort (scan `/sys/block/` for the newest nbd device) is reliable across all kernel versions and requires no kernel API.

## Package Structure

| File | What It Does |
|------|-------------|
| `mod.rs` | `NbdDeviceManager`: device lifecycle (add/remove/shutdown), socketpair creation, client-side handshake, hot reload via `nbd_devices.json`, exclusive access polling |
| `netlink.rs` | Raw `NBD_GENL` generic netlink: `connect()`, `disconnect()`, `reconfigure()`; family ID resolution; netlink message construction and parsing; fallback device index detection via `/sys/block/` |

## Configuration

| Option | Default | What It Controls at Runtime |
|--------|---------|----------------------------|
| `dead_conn_timeout` | 30 s | Seconds the kernel queues I/O when the socket is disconnected; enables hot reload without client timeouts; `0` disables queueing (I/O fails immediately on disconnect) |
| `cache_dir` | None | Directory for `nbd_devices.json`; if unset, no persistence and no hot reload (new device indices on every restart) |
| `preferred_index` (internal) | from persisted map | Requested `/dev/nbdN` number for stable block device paths |
| netlink `block_size` | 512 B | Sector size advertised to kernel; affects alignment of I/O requests |

## Failure Modes

| Failure | What Actually Happens | Recovery |
|---------|----------------------|---------|
| NBD module not loaded | Netlink family resolution fails with "Is the nbd kernel module loaded?" error | `modprobe nbd` then retry |
| Device already registered | `add_device()` returns `Err` immediately | Caller must remove first |
| `client_handshake` fails (protocol error) | Session task cancelled, return `Err` | Caller retries `add_device()` |
| `NBD_CMD_RECONFIGURE` returns EINVAL | Falls through to `NBD_CMD_CONNECT`; new device created | Device path may change if index is taken |
| Netlink EBUSY/EINVAL (transient) | Retry up to 10× with 100ms delay | Auto-recovers during kernel teardown window |
| Netlink permanent error (EPERM, ENOSPC) | Return `Err` after first attempt | Operator intervention required |
| Exclusive access timeout (5 s) | `add_device()` returns `Err`; session cancelled | Device held by unmount in progress; retry after cleanup |
| Session handler exits (handler error) | tokio task returns; kernel sees EOF; kernel returns EIO to all I/O | `remove_device()` + `add_device()` to recreate |
| Process killed without shutdown | `nbd_devices.json` persists; kernel devices remain in QUIESCED/dead state | Hot reload path in next process (reconfigure or reclaim) |
| `persist_devices()` fails | Warning logged; device operational; hot reload may use stale indices next restart | No immediate impact; `/dev/nbdN` path may change on restart |
