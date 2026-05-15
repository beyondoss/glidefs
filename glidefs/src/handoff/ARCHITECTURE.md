# Graceful zero-downtime daemon handoff

## Why

A glidefs restart used to mean: kill daemon, wait for new daemon to cold-start (S3 export discovery, foyer cache open, WAL replay, chunk metadata prefetch, then ublk recovery — total 0.5–15s), guest VMs see I/O hang for the full window. Connection pools timeout, etcd loses quorum, k8s probes fail. Bad.

This module makes a planned restart invisible to guests: target sub-50 ms p99 stall, zero I/O errors, zero data loss, no operator coordination.

## The protocol — Cooperative Recovery Handoff (CRH)

Triggered by `SIGHUP` (or `glidefs handoff` CLI). Two processes coordinate over an `AF_UNIX SOCK_SEQPACKET` socket at `/run/glidefs/handoff.sock`:

```
Predecessor (P)                         Successor (S)
SERVING                                  not started
   │
   ├── SIGHUP ──► fork+exec S
   │              cmd: glidefs --handoff-from <socket> --config <path>
   │
   │              ◄────── starting in successor mode ──┐
   │                                                    │
   │              ◄── HELLO {version, caps, pid} ──────┤
   │                                                    │
   ├── HELLO_ACK {version, caps, strategy, exports,    │
   │              pid} ──────────────────────────────► │  WARMING:
   │                                                    │   open foyer
   │                                                    │   replay WAL
   │                                                    │   build router
   │                                                    │   prefetch S3
   │                                                    │   pause schedulers
   │                                                    │   (freeze_in_progress=true)
   │                                                    │
   │              ◄── READY ───────────────────────────┤
   │                                                    │
   ├── freeze_all() — handler.freeze() per export      │
   │                  + cache.set_freeze_in_progress() │
   │                  + cache.flush() (fsync WAL)      │
   │                                                    │
   ├── CUTOVER ───────────────────────────────────────►│
   │                                                    │
   ├── strategy.predecessor_cutover() ─ drops UblkServer
   │   ─► kernel transitions devices to QUIESCED
   │                                                    │
   ├── PREDS_DEAD ────────────────────────────────────►│  TAKING_OVER:
   │                                                    │   replay_wal_tail()
   │                                                    │   for each export
   │                                                    │   recover_devices_by_id()
   │                                                    │   (start_user_recover +
   │                                                    │    submit FETCH_REQ +
   │                                                    │    end_user_recover)
   │                                                    │
   │              ◄── ALIVE {recovered_count} ─────────┤
   │
DEAD (P exits 0)                         set_freeze_in_progress(false)
                                         bind listeners (NBD/HTTP)
                                         enter signal loop
                                         SERVING
```

## Why each piece exists

### Versioned wire protocol (`protocol.rs`)
First message includes `protocol_version` and `Capabilities` bitset. Future PIOD support slots into the capabilities without breaking the CRH-only successor.

### `freeze_in_progress` flag on every WriteCache
Two consumers need it:

1. **Predecessor's flush_scheduler**: between SIGHUP and CUTOVER, the per-export checkpoint timer can fire. Its `wal.truncate()` would drop entries the successor's `replay_wal_tail` needs to pick up. With this flag, checkpoint still saves block-state metadata but skips the truncate.

2. **Successor's flush_scheduler**: built during WARMING. The successor's WriteCache opens against the same WAL file the predecessor is appending to (parent-process whitelist in `Wal::open`). Without the flag, the successor's checkpoint would race the predecessor's appends and truncate the WAL too. Set during WARMING, cleared after takeover completes.

### `replay_wal_tail` on the successor
The successor's WARMING-time `WriteCache::open` reads the WAL up to whatever's there at that moment. Predecessor keeps appending after that. After PREDS_DEAD, before reissuing FETCH_REQ, `recover_handoff_devices` calls `replay_wal_tail` on each export to absorb the new entries — keeps the successor's `state_map` in sync with what the predecessor actually persisted.

### Parent-process whitelist on the WAL flock (`wal.rs`)
The cross-process flock would otherwise block the successor's `Wal::open` (the predecessor still holds the lockfile). The successor checks `getppid()` against the lockfile's PID — if our parent owns it, accept (handoff scenario). Cross-process attacks (a stray third process) still fail.

### Predecessor revival fallback
If the successor crashes after PREDS_DEAD but before ALIVE, the predecessor's `revive_after_failed_handoff` calls `recover_quiesced_devices` against its own router (still alive — `take_ublk_server` only dropped the UblkServer, not the router or its handlers). Brief stall, no I/O errors, no data loss. The `unfreeze_all` step also clears `freeze_in_progress` so checkpointing resumes.

### Drain-skip on successful handoff
Predecessor's normal shutdown drains dirty blocks to S3. After a successful handoff, the successor inherits the same on-disk state — re-draining is wasteful and on the file:// test backend it hangs (manifest sync isn't supported). Skip is safe: pack uploads are content-addressed (re-upload is idempotent), manifest writes use ETag preconditions (no double-overwrite), orphan packs are reclaimed by `glidefs gc`.

## What's NOT this module's job

- **Crash recovery** (predecessor died ungracefully): handled by `recover_quiesced_devices` on cold start. CRH only handles planned restarts.
- **Listener fd inheritance**: NBD TCP/Unix and HTTP API listeners still close on predecessor exit; clients reconnect. SCM_RIGHTS-based fd passing is Phase 2 (task 2.1) and removes the one-RST-per-client cost.
- **NBD-transport handoff**: NBD's kernel-side connection-drop semantics + `nbd_dead_conn_timeout` provide a similar QUIESCED-equivalent. Task 1A.4 wires the test path.

## Observed performance (single ublk device, 4k random write under fio --verify=crc32c)

| Metric | Result |
|---|---|
| Total handoff (SIGHUP → predecessor exit) | ~500 ms |
| Kernel-stall window (CUTOVER → ALIVE) | ~250 ms |
| p50 write latency under load | 20 µs |
| p99 write latency under load | 42 µs |
| p99.9 write latency under load | 261 µs |
| p99.99 write latency under load | 3.2 ms |
| Worst single I/O during handoff | 197 ms |
| fio errors during handoff | 0 |
| crc32c verify failures | 0 |
| Side-channel oracle corrupt blocks | 0 |

## Failure-injection grid (verified)

| Inject point | Predecessor outcome | Asserted |
|---|---|---|
| `s_crash_after_warming` | stays SERVING (S aborts before READY) | post-fio 0 errors, 0 corrupt |
| `s_crash_after_ready` | revives via `revive_after_failed_handoff` | post-fio 0 errors, 0 corrupt |
| `s_crash_after_cutover` | revives via `recover_quiesced_devices` | post-fio 0 errors, 0 corrupt |

## Future work

- **PIOD strategy** (kernel 6.16+): per-tag handoff via `UBLK_F_PER_IO_DAEMON`. Sub-millisecond stall floor. Slots in via `strategy::select` runtime branch — protocol unchanged.
- **Pre-warm worker queue slots during WARMING**: currently `recover_devices_by_id`'s parallel `AddQueue` dispatch is the largest contributor to the 250 ms kernel-stall. Pre-allocating worker placements drops it to ~150 ms.
- **Skip the QUIESCED probe**: call `START_USER_RECOVERY` directly and retry on `-EBUSY`. Saves the 50µs–1ms poll round-trips.
- **Listener fd inheritance via SCM_RIGHTS**: eliminates the bind-retry on the successor side. Phase 2.
- **`glidefs handoff --dry-run`**: walks WARMING through READY then aborts, useful for canary validation.
