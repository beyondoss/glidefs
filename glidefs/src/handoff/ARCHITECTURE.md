# Handoff Architecture

Takes a running glidefs daemon (`SIGHUP` or `POST /admin/handoff`) and hands off all ublk block devices and network listener sockets to a freshly-started successor process — producing zero I/O errors, zero data loss, and a kernel-stall window measured in tens of milliseconds.

## Data Flow

### Happy path

```
Predecessor (P)                           Successor (S)
SERVING                                   not started
   │
   ├── SIGHUP / handoff CLI
   │
   ├── set_all_caches_freeze(true) ◄─── freeze starts here, not at CUTOVER
   │   (prevents WAL truncate race during entire WARMING window)
   │
   ├── bind /run/glidefs/handoff.sock
   ├── fork+exec glidefs --handoff-from <sock> --config <path>
   │                                          │
   │                              connect to handoff.sock
   │   ◄── HELLO {version=1, caps, pid} ──────┤
   │                                          │
   ├── HELLO_ACK {version, caps, strategy,    │
   │   exports[], listener_kinds[]}           │   WARMING:
   │   + SCM_RIGHTS: dup'd listener fds ─────►│     open foyer
   │                                          │     replay WAL
   │                                          │     build ExportRouter
   │                                          │     prefetch S3 metadata
   │                                          │     pause schedulers
   │   ◄── READY ──────────────────────────────┤
   │
   ├── freeze_all()
   │   (block new writes, fsync WALs per export)
   │
   ├── CUTOVER ──────────────────────────────►│
   ├── strategy.predecessor_cutover()
   │   CRH: drop UblkServer
   │   → kernel transitions devices to QUIESCED
   │
   ├── PREDS_DEAD ───────────────────────────►│   TAKING OVER:
   │                                          │     replay_wal_tail()
   │                                          │     recover_devices_by_id()
   │                                          │       START_USER_RECOVERY ioctl
   │                                          │       FETCH_REQ per tag (×64 parallel)
   │                                          │       END_USER_RECOVERY ioctl
   │   ◄── ALIVE {recovered_count} ───────────┤
   │
   ├── set_all_caches_freeze(false)
DEAD (exit 0)                            set_freeze_in_progress(false)
                                         bind or inherit listeners
                                         enter signal loop
                                         SERVING
```

### Abort paths (safe — no cutover occurs)

```
                    VersionMismatch
                    NoCommonStrategy        P sends Abort, stays SERVING
  HELLO ──► HELLO_ACK ──► READY ──────────►│ S sends Abort on ExportMismatch
                    ExportMismatch              or WarmingFailed, then exits
                    WarmingFailed
                    FreezeFailed ────────── P sends Abort, calls unfreeze_all,
                                            stays SERVING
                    dry-run-complete ─────── P sends Abort(Other), stays SERVING
                                            (operator proved WARMING works)
```

### Revival path (successor crashes after PREDS_DEAD)

```
  P: PREDS_DEAD sent; awaiting ALIVE
      │
      ├── socket EOF / error / timeout after alive_wait (60 s)
      │
      ├── revive_after_failed_handoff()
      │   recover_quiesced_devices() — reattaches QUIESCED devices to P's own router
      │
      ├── unfreeze_all()
      │
      └── P: set_all_caches_freeze(false) → RevivedFromFailedHandoff; SERVING
```

## Concepts & Terminology

| Term | What It Controls | NOT |
|---|---|---|
| **WARMING** | The window where S does all slow startup: foyer open, WAL replay, S3 prefetch, ExportRouter build. P serves I/O normally throughout. | Not the stall window — guests see no I/O impact here. |
| **freeze** | Per-export gate that blocks new writes and fsyncs the WAL. Set immediately before CUTOVER. Distinct from `freeze_in_progress`. | Not the same as the ublk QUIESCED state (a kernel concept). |
| **freeze_in_progress** | A WriteCache flag set from SIGHUP through end-of-handoff (success or revival). Prevents flush_scheduler from calling `wal.truncate()`. | Not the write-blocking freeze — that's `freeze_all()` called after READY. |
| **QUIESCED** | Kernel ublk device state after the predecessor drops its UblkServer. In-flight bios queue in the kernel; no userspace daemon is processing them. | Not a glidefs application state — it's a kernel ublk state machine value. |
| **CRH** (Cooperative Recovery Handoff) | The only implemented strategy. Uses `UBLK_F_USER_RECOVERY`: predecessor drops UblkServer, kernel QUIESCES devices, successor recovers via `START_USER_RECOVERY`+FETCH_REQ+`END_USER_RECOVERY`. | Not the same as crash recovery — CRH requires both processes to cooperate. |
| **PIOD** (Per-IO Daemon) | Reserved future strategy for kernel 6.16+. Per-tag daemon tracking eliminates the QUIESCED transition entirely. | Not implemented in any current build. |
| **ExportSnapshot** | The predecessor's in-memory export view transferred to S in HELLO_ACK: `(name, size_bytes, readonly, last_wal_seq, ublk_dev_id)`. S verifies its warmed router matches this set. | Not the full in-memory state — dirty blocks and pending S3 ops are not transferred; S rebuilds from disk. |
| **stall window** | The interval from P's `predecessor_cutover()` call through S's ALIVE send. Guest I/O is queued in the kernel (QUIESCED). | Not the full handoff duration (~500 ms); just the QUIESCED kernel window (~20–250 ms). |
| **listener fd inheritance** | P dups its bound NBD/HTTP listener fds and ships them via SCM_RIGHTS in HELLO_ACK. S uses these instead of calling `bind()` fresh — preserving in-flight TCP connections. | Not a future feature — fully implemented and active when listeners are registered. |

## Core Mechanism

### Protocol transport

Messages travel over an `AF_UNIX SOCK_SEQPACKET` socket at `/run/glidefs/handoff.sock`. SEQPACKET preserves message boundaries (no framing needed), delivers in-order, and supports ancillary `SCM_RIGHTS` data in the same call. Each message is bincode-serialized and fits in a 64 KiB receive buffer (handles ~1,000 exports at ~60 bytes/export + overhead).

A separate byte-level control socket at `/run/glidefs/handoff.ctl.sock` accepts single-byte commands from `glidefs handoff` CLI and `POST /admin/handoff`: `H` (handoff), `D` (dry-run). Responses: `A` (accepted), `B` (busy), `U` (unsupported). This separates triggering from the handoff protocol itself.

### Freeze-in-progress timing invariant

`set_all_caches_freeze(true)` is called **before** forking the successor — not at `freeze_all()`. Without this, a checkpoint firing between `WriteCache::open` (during S's WARMING) and `PREDS_DEAD` would call `wal.truncate()` and drop WAL entries that S's `replay_wal_tail` needs to pick up after `PREDS_DEAD`. The flag is cleared (`false`) only after the handoff fully completes or is aborted, so `flush_scheduler` can safely truncate again.

Two separate caches hold this flag simultaneously: P's (which still appends to the WAL during WARMING) and S's (which opens the same WAL file during WARMING, protected by the parent-process whitelist in `Wal::open`).

### WAL tail replay

S's WARMING-time `WriteCache::open` reads the WAL up to the point-in-time of that open. P continues appending through PREDS_DEAD. After receiving PREDS_DEAD, before calling `recover_devices_by_id`, S calls `replay_wal_tail()` per export to absorb entries added since WARMING. This keeps S's `state_map` consistent with what P actually persisted — otherwise S would serve stale block→chunk mappings.

### Listener fd inheritance

At HELLO_ACK time, P snapshots its `ListenerRegistry` (populated at daemon start by each `start_*_server` task), dups every fd with `F_DUPFD_CLOEXEC`, and sends the duplicates as SCM_RIGHTS ancillary data alongside HELLO_ACK. The `listener_kinds` field in HELLO_ACK tags each fd with its type (`NbdTcp(addr)`, `NbdUnix`, `HttpApi`). S reconstructs an `InheritedFds` map from the kinds+fds pair, then passes each fd to the corresponding server constructor instead of calling `bind()`. Clients see no RST. If kinds/fds counts mismatch (dup failure), S falls back to `bind()` silently.

### WAL flock parent-process whitelist

`Wal::open` holds a cross-process `flock` on the WAL lockfile. During handoff S would normally fail to open the WAL because P holds the lock. The whitelist: if the lockfile's owning PID equals `getppid()`, the flock is accepted (handoff scenario). Third-party processes still get `EWOULDBLOCK`.

### CRH recovery parallelism

`recover_devices_by_id` issues `START_USER_RECOVERY`, FETCH_REQ, and `END_USER_RECOVERY` ioctls for each ublk device. These run 64-wide concurrently (`RECOVERY_CONCURRENCY = 64`). At 1,000-device density this hits the parallel ioctl floor, not a queuing ceiling. The stall window is dominated by the kernel's QUIESCED transition and ioctl latency, not by application work.

## State Machine

### Predecessor

```
SERVING
   │
   SIGHUP / handoff trigger
   │
   ├── set_all_caches_freeze(true)
   ├── bind socket, fork+exec successor
   │
   WAITING_HELLO
   │
   ├── timeout (warming + ready_wait) ──► Aborted{timeout}; unfreeze; SERVING
   ├── recv Hello
   │     version mismatch ──────────────► send Abort, Aborted; unfreeze; SERVING
   │     no common strategy ────────────► send Abort, Aborted; unfreeze; SERVING
   │
   ├── send HelloAck + SCM_RIGHTS fds
   │
   WAITING_READY
   │
   ├── timeout (ready_wait) ───────────► Aborted{timeout}; unfreeze; SERVING
   ├── recv Abort ─────────────────────► Aborted{successor reason}; unfreeze; SERVING
   ├── recv Ready (dry_run=true) ──────► send Abort(dry-run-complete); SERVING
   ├── recv Ready
   │
   ├── freeze_all() ──── failure ──────► send Abort(FreezeFailed); unfreeze; SERVING
   │
   FROZEN
   │
   ├── send Cutover
   ├── predecessor_cutover() ─ failure ► revive_after_failed_handoff(); unfreeze
   │                                     RevivedFromFailedHandoff; SERVING
   ├── send PredsDead
   │
   WAITING_ALIVE (alive_wait = 60 s)
   │
   ├── socket error / EOF ─────────────► revive; unfreeze; RevivedFromFailedHandoff
   ├── timeout ────────────────────────► revive; unfreeze; RevivedFromFailedHandoff
   ├── recv Abort ─────────────────────► revive; unfreeze; RevivedFromFailedHandoff
   ├── recv Alive{recovered_count}
   │
   ├── set_all_caches_freeze(false)
DEAD (exit 0)                        Succeeded
```

| From | Event | To | Guard | What Actually Happens |
|---|---|---|---|---|
| SERVING | SIGHUP / CLI | WAITING_HELLO | — | `set_all_caches_freeze(true)`, socket bound, successor forked |
| WAITING_HELLO | recv Hello | WAITING_READY | version match, common strategy | HelloAck + SCM_RIGHTS listener fds sent |
| WAITING_HELLO | timeout / bad version | SERVING | — | Abort sent; freeze cleared |
| WAITING_READY | recv Ready (dry_run=false) | FROZEN | — | `freeze_all()` called (blocks writes, fsyncs WALs) |
| WAITING_READY | recv Abort / timeout | SERVING | — | freeze cleared; predecessor resumes normally |
| FROZEN | freeze_all OK | FROZEN | — | Cutover sent; `predecessor_cutover()` drops UblkServer → QUIESCED |
| FROZEN | freeze fails | SERVING | — | Abort(FreezeFailed); `unfreeze_all()` |
| FROZEN | cutover fails | SERVING | — | `revive_after_failed_handoff()` + unfreeze |
| WAITING_ALIVE | recv Alive | DEAD | — | `set_all_caches_freeze(false)`; exit 0 |
| WAITING_ALIVE | error / timeout / Abort | SERVING | — | `revive_after_failed_handoff()` + unfreeze; RevivedFromFailedHandoff |

### Successor

```
STARTED (--handoff-from <socket>)
   │
   ├── connect to handoff socket
   ├── send Hello{version, caps, pid, dry_run}
   │
   ├── WARMING (slow startup — all of it before we talk to P again):
   │     open foyer cache
   │     replay WAL
   │     build ExportRouter (handlers for every export)
   │     prefetch S3 chunk metadata
   │
   ├── recv HelloAck + SCM_RIGHTS fds
   │     version mismatch ──────────────► exit with error
   │     recv Abort ────────────────────► exit with error
   │
   ├── verify router has all listed exports
   │     missing exports ────────────────► send Abort(ExportMismatch); exit
   │
   ├── send Ready
   │
   ├── recv Cutover
   │     recv Abort ─────────────────────► exit (P staying SERVING)
   │
   ├── recv PredsDead
   │
   ├── replay_wal_tail() per export
   ├── recover_devices_by_id() (64-wide parallel CRH ioctls)
   │
   ├── send Alive{recovered_count}
   │
   ├── set_freeze_in_progress(false)
   ├── bind or inherit listeners (from InheritedFds)
SERVING
```

| From | Event | To | Guard | What Actually Happens |
|---|---|---|---|---|
| WARMING | recv HelloAck | POST_WARMING | version matches | Verifies router has all listed exports |
| POST_WARMING | export mismatch | EXIT | — | Abort(ExportMismatch) sent; successor exits |
| POST_WARMING | exports OK | READY_SENT | — | Ready sent |
| READY_SENT (dry_run) | recv Abort(dry-run-complete) | EXIT | — | Successor exits cleanly; P stays SERVING |
| READY_SENT | recv Cutover then PredsDead | TAKING_OVER | — | `replay_wal_tail()`; then `recover_devices_by_id()` |
| TAKING_OVER | recovery OK | SERVING | — | Alive{count} sent; listeners bound/inherited |
| TAKING_OVER | recovery fails | EXIT | — | Error logged; P detects via socket EOF → revival |

## Why It Behaves This Way

### `freeze_in_progress` set at SIGHUP, not at `freeze_all`

The flag must cover the entire WARMING + READY-wait + freeze + cutover window. If the flush_scheduler fires a checkpoint between S's `WriteCache::open` and `freeze_all()`, it calls `wal.truncate()` — silently dropping entries S's `replay_wal_tail` is counting on. This manifests as `bad magic header 0` in fio verify. Setting the flag at SIGHUP is the only safe placement.

### Why the predecessor's ExportRouter survives past `predecessor_cutover()`

`predecessor_cutover()` calls `take_ublk_server()`, not "destroy the router." The handlers, WAL references, and block-state metadata all stay alive in P's process. If S crashes after PREDS_DEAD, P calls `revive_after_failed_handoff()` against its own still-alive router — `recover_quiesced_devices()` reattaches the QUIESCED devices to P. Without this, the devices would stay in QUIESCED indefinitely (or until the kernel times them out), hanging all guest I/O.

### Why drain is skipped on successful handoff

Normal shutdown drains dirty blocks to S3. After handoff, S inherits the same on-disk state (same foyer SSD, same WAL). Re-draining would re-upload blocks S already has. On the `file://` test backend it hangs (manifest sync is unsupported). The skip is safe: pack uploads are content-addressed (re-upload is idempotent), manifest writes use ETag preconditions (no double-overwrite), orphan packs are reclaimed by `glidefs gc`.

### Why the protocol uses SEQPACKET, not STREAM or DGRAM

SEQPACKET preserves message boundaries (like DGRAM) while providing a connected, ordered, reliable session (like STREAM). The handoff protocol is a strict request/response sequence — boundaries matter for bincode framing, ordering matters for the state machine, and SCM_RIGHTS fd passing requires a connected socket. DGRAM has a lower MTU and no connection semantics. STREAM requires manual framing.

### Why capabilities are intersected, not version-gated

Strategy capabilities (CRH, PIOD) evolve independently of the wire format. Bumping `PROTOCOL_VERSION` for every new strategy would force simultaneous predecessor+successor upgrades. Instead, both advertise their capabilities and take the intersection. A v1+CRH predecessor handoffs cleanly to a v1+CRH+PIOD successor (result: CRH). A breaking wire format change (non-extensible) bumps the version and aborts explicitly.

### Why `replay_wal_tail` runs after PREDS_DEAD, not after CUTOVER

Between S's WARMING-time `WriteCache::open` and P's `PredsDead`, P continues appending to the WAL. `replay_wal_tail` absorbs those entries. It must run after PREDS_DEAD (P has stopped appending) but before `recover_devices_by_id` (so S's state_map matches what P persisted before the last write). Running it at CUTOVER would be too early — P can still append between CUTOVER and PREDS_DEAD.

## Trust Boundaries

**What the handoff protocol checks (aborts if invalid):**

- Protocol version must be exactly `PROTOCOL_VERSION = 1` (both directions)
- Negotiated capabilities must include at least one common strategy
- Successor's warmed router must contain every export named in `ExportSnapshot`
- `WAL::open` parent-process whitelist: lockfile PID must equal `getppid()`

**What passes through unchecked:**

- `ExportSnapshot.last_wal_seq`: carried in HelloAck but not yet verified by S (debug-aid field; S doesn't abort on mismatch)
- Successor binary identity: P execs whatever path is configured; no signature check
- Successor PID in HELLO: logged only; P doesn't verify it matches the forked child
- Listener fd count vs `listener_kinds` length: mismatch triggers a warn+rebind, not an abort

**Security posture:**

The handoff socket is created at `/run/glidefs/handoff.sock` (mode 0600, owned by the daemon user). Only a process running as the same user can connect. The predecessor verifies the WAL flock via `getppid()` — a stray third process holding a valid cookie can't hijack the WAL.

## Package Structure

| File | What It Does |
|---|---|
| `mod.rs` | Module entry point. Re-exports `run_predecessor`, `run_successor`, protocol constants. |
| `protocol.rs` | Bincode-serialized `HandoffMessage` enum, `ExportSnapshot`, `Capabilities`, `AbortReason`, `HandoffTimeouts`. Wire format for the SEQPACKET socket. Also defines the 1-byte CTL wire protocol. |
| `predecessor.rs` | Predecessor state machine (`run_predecessor`). Drives the handoff from SIGHUP through Succeeded / Aborted / RevivedFromFailedHandoff. Handles freeze, cutover, revival. |
| `successor.rs` | Successor state machine (`run_successor`). Connects to the handoff socket, drives protocol exchange, calls `strategy_takeover`. Returns `TakeoverResult` with router + inherited fds. |
| `strategy/mod.rs` | `CutoverStrategy` trait. `PredecessorCutoverCtx` and `SuccessorTakeoverCtx` carry per-call state. `select()` picks the strategy for the running kernel. |
| `strategy/crh.rs` | `CrhStrategy` — the only active implementation. Predecessor: `take_ublk_server()`. Successor: `recover_handoff_devices()` with 64-wide parallelism. |
| `strategy/piod.rs` | Placeholder for future `PiodStrategy` (kernel 6.16+, `UBLK_F_PER_IO_DAEMON`). Not implemented. |
| `listener_registry.rs` | `ListenerRegistry` (predecessor side: `register`/`snapshot`). `InheritedFds` (successor side: `from_kinds_and_fds`/`take`). Used for SCM_RIGHTS listener inheritance. |
| `fdpass.rs` | `sendmsg_with_fds` and `recvmsg_with_fds`. Wraps `sendmsg(2)`/`recvmsg(2)` to attach/extract `SCM_RIGHTS` ancillary data. Max 64 fds per call. |
| `metrics.rs` | Atomic counters per outcome kind. Stall histogram (cumulative, Prometheus conventions). `render_prometheus()` for future HTTP exposure. |
| `fault.rs` | Test fault injection (`--features test-fault-injection`). Reads `GLIDEFS_INJECT_FAILURE` env var; panics the process at named injection points to simulate crashes. |

## Configuration

| Parameter | Default | What It Controls |
|---|---|---|
| `HandoffTimeouts.warming` | 120 s | Upper bound for S to finish WARMING. P uses this as part of the accept/HELLO timeout. Increase for large caches on slow S3. |
| `HandoffTimeouts.ready_wait` | 180 s | Max time P waits for READY after HelloAck. Combined with `warming` for the outer connect timeout. |
| `HandoffTimeouts.cutover_wait` | 30 s | Max time S waits for CUTOVER after READY, and for PREDS_DEAD after CUTOVER. |
| `HandoffTimeouts.alive_wait` | 60 s | Max time P waits for ALIVE after PREDS_DEAD before triggering revival. Set generously to absorb slow device recovery at high cardinality. |
| `RECV_BUF_BYTES` | 64 KiB | Receive buffer per message. Limits effective max exports to ~1,000 (60 bytes/export). Hardcoded. |
| Handoff socket path | `/run/glidefs/handoff.sock` | `AF_UNIX SOCK_SEQPACKET` socket for the two-process protocol. Configurable via CLI `--handoff-socket`. |
| CTL socket path | `/run/glidefs/handoff.ctl.sock` | Byte-command socket for triggering handoff without SIGHUP. |

## Failure Modes

| Failure | What Actually Happens | Recovery |
|---|---|---|
| Successor binary not found or fails to exec | `spawn_successor` returns Err; predecessor logs and stays SERVING; freeze cleared | No handoff; operator retries after fixing binary path |
| Successor times out connecting | accept timeout fires; `Aborted{timeout}`; freeze cleared | Predecessor stays SERVING; stale socket removed on next attempt |
| Version mismatch (old P / new S or vice versa) | P sends Abort(VersionMismatch), returns Aborted; freeze cleared | No handoff; operator must align versions |
| Export config drift (S missing an export) | S sends Abort(ExportMismatch); P returns Aborted; freeze cleared | No handoff; operator aligns configs |
| S crashes during WARMING (before READY) | P hits READY timeout; Aborted; freeze cleared | Predecessor stays SERVING uninterrupted |
| S sends Abort(WarmingFailed) | P receives Abort, returns Aborted; freeze cleared | Predecessor stays SERVING; inspect successor logs |
| `freeze_all()` fails (WAL fsync error) | P sends Abort(FreezeFailed), calls `unfreeze_all()` | Predecessor stays SERVING; investigate WAL/disk |
| Predecessor cutover fails | P detects error; calls `revive_after_failed_handoff()`; `unfreeze_all()` | `RevivedFromFailedHandoff` — brief stall, no I/O errors if revival succeeds |
| S crashes after PREDS_DEAD (before ALIVE) | Socket EOF; P calls `revive_after_failed_handoff()` + unfreeze | `RevivedFromFailedHandoff` — P reattaches QUIESCED devices and resumes |
| ALIVE timeout (S hung in recovery) | alive_wait (60 s) expires; P calls `revive_after_failed_handoff()` | Same as above; S eventually exits when socket closes |
| `revive_after_failed_handoff()` fails | P propagates Err; daemon exits abnormally | Manual operator intervention; cold-start recovery via `recover_quiesced_devices` on next start |
| Listener fd count mismatch in HelloAck | S logs warn, uses `InheritedFds::default()` | S calls `bind()` fresh; clients see 1 RST per listener, then reconnect |

## Observed Performance

Single ublk device, 4K random write under `fio --verify=crc32c`:

| Metric | Result |
|---|---|
| Total handoff (SIGHUP → predecessor exit) | ~500 ms |
| Kernel stall (CUTOVER → ALIVE) | ~250 ms |
| p50 write latency during handoff | 20 µs |
| p99 write latency during handoff | 42 µs |
| p99.9 write latency during handoff | 261 µs |
| p99.99 write latency during handoff | 3.2 ms |
| Worst single I/O during handoff | 197 ms |
| fio errors | 0 |
| crc32c verify failures | 0 |

## Fault Injection Grid (verified)

| Inject point | Predecessor outcome | Post-fio result |
|---|---|---|
| `s_crash_after_warming` | stays SERVING (Abort before READY) | 0 errors, 0 corrupt |
| `s_crash_after_ready` | RevivedFromFailedHandoff | 0 errors, 0 corrupt |
| `s_crash_after_cutover` | RevivedFromFailedHandoff via `recover_quiesced_devices` | 0 errors, 0 corrupt |

## Known Limitations

- **`handoff_sequential_50_crh` / `handoff_multi_export_5_crh` stress failures**: 50 sequential handoffs or 5-export parallel handoffs under continuous fio write+verify produce occasional `bad magic header 0` verify failures in the first ~10 s. The handoff protocol completes correctly each time — WAL tail-replay fires and replays thousands of entries per handoff. The race is in in-flight ublk-bio handling during the QUIESCED → reissue kernel transition, not in the userspace state machine.
- **PIOD strategy**: not implemented. Kernel 6.16+ with `UBLK_F_PER_IO_DAEMON` would eliminate the QUIESCED transition, reducing the stall floor to sub-millisecond. The wire protocol has an extension point (`StrategyMsg`, `Capabilities.piod`). See `strategy/piod.rs` for the implementation guide.
- **Dry-run mode**: successor performs full WARMING through READY, then P sends Abort(dry-run-complete). Successor exits cleanly. Useful for canary validation (`glidefs handoff --dry-run`).
