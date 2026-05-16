# GlideFS

Block device server that turns S3 into fast local storage. Writes hit local SSD in 5 microseconds. Background sync uploads to S3 as content-addressed packs.

Built for microVM storage at [Beyond](https://beyond.dev).

## How It Works

Guests see a standard block device (NBD or ublk). Writes go to local SSD immediately. A background scheduler packs dirty blocks, compresses with LZ4, and uploads to S3. Reads serve from local cache; misses pull from S3, verify BLAKE3 hashes, and cache locally.

```
Write path:  Guest → NBD/ublk → local SSD pwrite() → return OK      ~5µs
Read path:   Guest → NBD/ublk → local cache hit → return data       ~500µs
             Guest → NBD/ublk → cache miss → S3 GET → LZ4 → verify → cache → return   50-300ms
```

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/paraglidehq/glidefs/main/install.sh | sh
```

Or build from source:

```sh
cargo build --release -p glidefs
```

## Quick Start

```sh
# Generate config
glidefs init glidefs.toml

# Edit glidefs.toml with your S3 bucket and cache directory, then:
glidefs run --config glidefs.toml
```

## Configuration

```toml
[cache]
dir = "/var/cache/glidefs"
disk_size_gb = 100.0
memory_size_gb = 1.0
ssd_cache_size_gb = 10.0

[storage]
url = "s3://my-bucket/vms"

[servers.nbd]
unix_socket = "/var/run/glidefs.sock"
api_address = "127.0.0.1:8080"
```

Supports S3, Azure Blob Storage, and GCS. Cloud credentials are configured via `[aws]`, `[azure]`, or `[gcp]` sections, or standard environment variables.

## API

Exports are virtual block devices. The API creates them, returns a device path, and handles teardown. The orchestrator never touches NBD or ublk directly.

```sh
# Create a 500GB export — returns device path
curl -X PUT localhost:8080/api/exports/my-vm \
  -d '{"size_gb": 500}'
# → {"name":"my-vm","size_bytes":500000000000,"readonly":false,"transport":"nbd","device":"/dev/nbd0"}

# Fork from the current state of an existing export
curl -X PUT localhost:8080/api/exports/my-vm-fork \
  -d '{"size_gb": 500, "manifest_name": "my-vm"}'

# Fork from a specific snapshot (returns sequence from POST /snapshot)
curl -X PUT localhost:8080/api/exports/my-vm-fork \
  -d '{"size_gb": 500, "manifest_name": "my-vm", "snapshot_sequence": 42}'

# Use ublk transport (Linux 6.0+, requires --features ublk)
curl -X PUT localhost:8080/api/exports/my-vm \
  -d '{"size_gb": 500, "transport": "ublk"}'
# → {"name":"my-vm","size_bytes":500000000000,"readonly":false,"transport":"ublk","device":"/dev/ublkb0"}

# Snapshot to S3 — returns {"sequence": 42, "manifest_etag": "..."}
curl -X POST localhost:8080/api/exports/my-vm/snapshot

# List snapshots
curl localhost:8080/api/exports/my-vm/snapshots
# → [1, 5, 42]

# Delete a snapshot
curl -X DELETE localhost:8080/api/exports/my-vm/snapshots/5

# Drain (flush all dirty blocks, prepare for migration)
curl -X POST localhost:8080/api/exports/my-vm/drain

# Delete (removes kernel device + export)
curl -X DELETE localhost:8080/api/exports/my-vm
```

PUT is idempotent. Same size → returns current state. Larger size → grows the export. All endpoints that modify state are idempotent.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/exports` | GET | List exports (includes transport + device path) |
| `/api/exports/{name}` | PUT | Create or resize export. `manifest_name` + optional `snapshot_sequence` to fork. |
| `/api/exports/{name}` | GET | Get export info |
| `/api/exports/{name}` | DELETE | Remove export. `?purge=true` deletes local cache and all S3 snapshots. |
| `/api/exports/{name}/drain` | POST | Flush all dirty blocks to S3 (no snapshot created) |
| `/api/exports/{name}/snapshot` | POST | Flush dirty blocks + create versioned snapshot. Optional body `{"tag": "..."}`. Returns `{sequence, manifest_etag, tag}`. |
| `/api/exports/{name}/snapshots` | GET | List snapshot sequences in ascending order |
| `/api/exports/{name}/snapshots/{seq}` | DELETE | Delete a specific snapshot (idempotent) |
| `/api/exports/{name}/tag` | POST | Tag the current manifest without flushing. Body: `{"tag": "..."}`. |
| `/api/exports/{name}/promote` | POST | Promote readonly to read-write |
| `/api/manifests/{s3_prefix}/{name}` | HEAD | 200 if manifest exists, 404 if not. No running export required. |
| `/health/ready` | GET | Readiness check |
| `/api/exports/{name}/metrics` | GET | I/O metrics |
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |

## Base Images

Create content-addressed base images from raw disk files:

```sh
glidefs bless --image ubuntu-22.04.raw --name ubuntu-22.04-v1 --s3-prefix bases --config glidefs.toml
```

Exports forked from base images share blocks via content addressing. Identical data is stored once.

Fork from a blessed image using `manifest_name: "bases/{name}"`:

```sh
curl -X PUT localhost:8080/api/exports/vm-1 \
  -d '{"size_gb": 50, "manifest_name": "bases/ubuntu-22.04-v1"}'
```

## Deployments

Fork is instant — parent blocks are copy-on-write, not copied. Two flows.

### Code deploy (setup unchanged)

Parent VM already has OS + runtime. Deploy is just new app code.

```sh
# 1. Snapshot production (safety net + rollback point)
curl -sX POST localhost:8080/api/exports/prod/snapshot \
  -d '{"tag": "pre-deploy-7"}'
# → {"sequence": 42, "manifest_etag": "...", "tag": "pre-deploy-7"}

# 2. Fork — instant CoW, no data copied
curl -X PUT localhost:8080/api/exports/vm-deploy-7 \
  -d '{"size_gb": 50, "manifest_name": "prod"}'
# → {"device": "/dev/nbd1", ...}

# 3. Mount + sync code + start
mount /dev/nbd1 /mnt
rsync -a ./dist/ /mnt/app/code/
systemctl start my-app

# 4. Health check → swap traffic → delete old export
curl localhost:8080/api/exports/vm-deploy-7/metrics  # verify
curl -X DELETE localhost:8080/api/exports/prod-old
```

Setup is untouched. Deploy is seconds.

### Setup change (new dependency or runtime bump)

Compute a content-derived hash from image + deps. Tag IS the cache key — no external state needed.

```
hash = blake3(image_id + lockfile_hash)

HEAD /api/manifests/bases/setup-{hash}
              │
         ┌────┴────┐
        200        404
         │          │
   fork from    fork from base
   cached       → run setup
   setup        → snapshot+tag("setup-{hash}")
         │          │
         └────┬─────┘
              │
    sync code → deploy → swap traffic
```

```sh
SETUP_HASH=$(echo "${IMAGE_ID}:${LOCKFILE_HASH}" | blake3)

# Check if this setup was already built
STATUS=$(curl -so /dev/null -w "%{http_code}" \
  -X HEAD localhost:8080/api/manifests/bases/setup-${SETUP_HASH})

if [ "$STATUS" -eq 200 ]; then
  # Hit: fork from cached setup, skip straight to code sync
  SOURCE="setup-${SETUP_HASH}"
else
  # Miss: fork from base, run setup, tag result
  curl -X PUT localhost:8080/api/exports/setup-work \
    -d '{"size_gb": 50, "manifest_name": "bases/ubuntu-24.04-v1"}'

  mount /dev/nbd1 /mnt
  mise install node@22 && npm ci --prefix /mnt/app
  umount /mnt

  curl -X POST localhost:8080/api/exports/setup-work/snapshot \
    -d "{\"tag\": \"setup-${SETUP_HASH}\"}"
  curl -X DELETE localhost:8080/api/exports/setup-work

  SOURCE="setup-${SETUP_HASH}"
fi

# Fork from setup state, sync code, deploy
curl -X PUT localhost:8080/api/exports/vm-deploy-8 \
  -d "{\"size_gb\": 50, \"manifest_name\": \"${SOURCE}\"}"
```

Same `IMAGE_ID + LOCKFILE_HASH` next deploy → HEAD returns 200 → setup is skipped entirely.

## Operations

### Cache Sizing

The clean cache (foyer) is shared across all exports via content addressing. Identical blocks are stored once.

| Component | What it holds | Sizing guidance |
|---|---|---|
| Memory cache (`memory_size_gb`) | Hot decompressed blocks | 1-4GB. Serves ~100ns reads. |
| SSD cache (`ssd_cache_size_gb`) | Warm blocks evicted from memory | Size for your unique working set. Shared OS/runtime blocks deduplicate automatically. |
| Dirty data + WAL | Unflushed writes, per-export | Grows between flush cycles. Budget 10-100MB per active export. |

For 2,000 VMs on one host with a shared base image: the OS/runtime blocks (~2-3GB) are stored once in cache. Per-VM unique data (app state, DB pages) is what scales. A 2TB NVMe comfortably handles this.

### Transport

Two options. NBD works everywhere. ublk is opt-in on Linux 6.0+ for lower overhead.

#### NBD (default)

Unix domain socket. TCP adds latency and firewall surface for no benefit on the same host.

```toml
[servers.nbd]
unix_socket = "/var/run/glidefs.sock"
```

TCP is available for serving to a different host.

#### ublk (Linux 6.0+)

io_uring-based userspace block device. No socket overhead, no protocol serialization, native multi-queue.

```sh
cargo build --release -p glidefs --features ublk
```

Requires `CONFIG_BLK_DEV_UBLK=y` in the host kernel. One `/dev/ublkbN` device per export — the block device appears when the export is created. No client tool needed.

### Device Setup

The API handles kernel device creation automatically. `PUT /api/exports/{name}` returns a `device` path. Pass it to your hypervisor.

```
PUT /api/exports/vm-abc {"size_gb": 500}
→ {"device": "/dev/nbd0", "transport": "nbd", ...}

Orchestrator passes /dev/nbd0 to Firecracker. Done.
```

**NBD** (Linux 4.10+): GlideFS creates `/dev/nbdN` via generic netlink (`NBD_GENL`). Internal socketpair — no external `nbd-client`. The NBD protocol server still accepts external connections over Unix socket / TCP for debugging or cross-host access.

**ublk** (Linux 6.0+): GlideFS creates `/dev/ublkbN` via io_uring. No socket, no protocol overhead.

Both transports: the orchestrator doesn't know or care which one is in use. It gets a device path from the API and passes it to the VM.

### Restart Behavior

There are two restart paths. Prefer the first whenever you control when the restart happens (binary upgrades, config rollouts, planned maintenance):

#### Graceful handoff (preferred)

The currently-serving daemon spawns a successor, the successor does *all* slow startup work in parallel (foyer open, WAL replay, ExportRouter build, S3 prefetch, manifest load) while the predecessor keeps serving I/O, then they coordinate a sub-second cutover. Both processes share the same WAL, SSD cache, and ublk character-device fds; SCM_RIGHTS passes the NBD TCP/Unix and HTTP API listener fds across so existing client TCP connections survive without a reset.

Trigger it any of these ways:

```bash
# CLI (operator-friendly, no PID needed)
glidefs handoff

# CLI dry-run — proves a new binary can WARM successfully without
# committing to the cutover. Useful as a canary before flipping the
# real switch.
glidefs handoff --dry-run

# SIGHUP — for orchestration tools that already know how to send signals
kill -HUP $(pidof glidefs)

# HTTP API — for in-cluster controllers
curl -X POST http://127.0.0.1:8080/admin/handoff
curl -X POST 'http://127.0.0.1:8080/admin/handoff?dry_run=true'

# systemd
systemctl reload glidefs   # ExecReload=/bin/kill -HUP $MAINPID
```

What guests see during a planned handoff:

- **ublk:** kernel-stall window ~50–100 ms (sub-50 ms p99 for healthy daemons; the protocol overhead itself is ~670 ms but pipelined with the kernel transition). Same `/dev/ublkbN` paths, same VMs, zero I/O errors, zero data loss. fio with `--verify=crc32c` running through the cutover sees no errors.
- **NBD TCP/Unix:** existing client TCP connections survive — the listener fd is inherited via SCM_RIGHTS, so the kernel socket structure is preserved across the new process.
- **HTTP API:** same — listener fd inherited, in-flight HTTP requests complete on the successor.

Failure handling:

- If the successor crashes after `READY` but before takeover, the predecessor revives via its own `recover_quiesced_devices` path — brief stall, no I/O errors, no data loss.
- If the predecessor crashes mid-handoff, the successor falls through to the standard crash-recovery path (next case).

Operational guides: `glidefs/src/handoff/ARCHITECTURE.md` (protocol diagram + per-stage breakdown), `glidefs/src/handoff/RUNBOOK.md` (oncall guide for every failure mode), `glidefs_handoff_*` Prometheus metrics (outcome counter + stall histogram).

#### Crash recovery (backstop)

When the daemon dies ungracefully (OOM, bug, host reboot), the kernel keeps `/dev/ublkbN` alive in QUIESCED state (Linux 6.3+ with `UBLK_F_USER_RECOVERY_REISSUE`) and the next daemon that starts picks it up:

```
1. SIGTERM → ublk devices enter QUIESCED state
2. Start new binary (same config, same cache dir)
3. New process discovers exports, recovers from WAL
4. Scans for QUIESCED glidefs devices, resumes them via START_USER_RECOVERY
5. Kernel reissues queued I/O to new process
6. /health/ready returns 200
```

This is the unplanned restart path — slower (full cold start) but correct. NBD's equivalent is `nbd_dead_conn_timeout`: the kernel queues I/O during the gap; `dead_conn_timeout` must exceed `restart + recovery` (default 30 s).

On kernels before 6.3 (no `UBLK_F_USER_RECOVERY`), ublk devices are removed on process exit; VMs get I/O errors and must be re-attached to new device paths. Use the graceful handoff path on those kernels.

**Full compute node reboot:** Everything starts fresh. Orchestrator creates exports via API, gets device paths, starts VMs.

### Database Workloads

Mount the database's WAL directory on a separate volume that's not GlideFS. Keep GlideFS for the OS, application code, and data files.

```
/dev/vda → GlideFS    (OS, app, DB data files)
/dev/vdb → local NVMe  (WAL only)
```

```sh
# PostgreSQL
initdb --waldir=/mnt/wal

# MySQL/InnoDB
innodb_log_group_home_dir = /mnt/wal
```

**Why:** Database WAL is high-frequency sequential writes to blocks the DB recycles within minutes. A busy Postgres writing 100MB/s of WAL generates ~8 pack uploads/second per VM — all for data that's transient. At 2000 VMs, that's 16,000 S3 PUTs/second of dead WAL segments.

**Durability is unchanged.** GlideFS is write-behind: the DB fsyncs WAL to local SSD, but that data isn't in S3 until the next flush cycle. Host death loses unflushed WAL either way. Separating it stops paying S3 costs for durability you didn't have.

**Migration:** Force a checkpoint before migrating (`CHECKPOINT` in Postgres). The WAL volume is local-only — GlideFS drain + wake handles the data files, the DB recovers from the checkpoint.

**Forks:** Fork gets the CoW snapshot of data files but no WAL. The forked DB starts from the last checkpoint — clean state, no in-flight transactions.

### Snapshots

`POST /snapshot` flushes dirty blocks to S3 and writes a versioned manifest at a stable S3 key. Background syncs never touch snapshot keys — they accumulate until you delete them.

```sh
# Take a snapshot — record the sequence number
SEQ=$(curl -sX POST localhost:8080/api/exports/my-vm/snapshot | jq .sequence)
# → 42

# List all snapshots for an export
curl localhost:8080/api/exports/my-vm/snapshots
# → [1, 5, 42]

# Fork a new export from snapshot 42 (read-only parent blocks, CoW overlay for writes)
curl -X PUT localhost:8080/api/exports/my-vm-test \
  -d "{\"size_gb\": 500, \"manifest_name\": \"my-vm\", \"snapshot_sequence\": $SEQ}"

# Delete a snapshot when done
curl -X DELETE localhost:8080/api/exports/my-vm/snapshots/5
```

`snapshot_sequence` is optional. Omit it to fork from the current state.

**GC and snapshots**: GC scans all snapshot manifests before deleting any pack. Packs referenced by a snapshot are kept alive even if they're no longer in the current manifest. Deleting a snapshot unpins its exclusive packs — they become eligible for GC after the grace period (default 24h).

**Rollback**: There is no in-place rollback. To restore an export to a prior snapshot:

```sh
# 1. Remove the export without purge (snapshots stay in S3)
curl -X DELETE localhost:8080/api/exports/my-vm

# 2. Fork from the target snapshot into the same name
curl -X PUT localhost:8080/api/exports/my-vm \
  -d '{"size_gb": 500, "manifest_name": "my-vm", "snapshot_sequence": 5}'
```

No data is copied — the new export reads parent blocks from the existing S3 packs via the CoW overlay.

**Blue/green rollback**: Fork to a new name first, verify, then cut over:

```sh
curl -X PUT localhost:8080/api/exports/my-vm-rollback \
  -d '{"size_gb": 500, "manifest_name": "my-vm", "snapshot_sequence": 5}'
# verify my-vm-rollback, then swap at the load balancer
```

### Garbage Collection

Writes accumulate new packs in S3. Run GC periodically to delete packs no longer referenced by any manifest or snapshot.

```sh
glidefs gc --config glidefs.toml
```

GC reads pack IDs from all current and snapshot manifests, lists packs in S3, and deletes anything unreferenced. State is persisted between runs (default `gc-state.json`) to enforce the grace period.

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | false | Report what would be deleted without deleting |
| `--grace-period` | `24h` | Protect recently-dead packs from deletion |
| `--max-deletes` | `100000` | Cap deletes per run |
| `--state-file` | `gc-state.json` | Path to grace period state file |
| `--snapshot-retention` | disabled | Auto-delete snapshots older than this (e.g. `30d`) |

**When to run:** Daily cron is sufficient. Run immediately after bulk snapshot deletion to reclaim space faster.

**Grace period:** Packs marked dead for less than `--grace-period` are never deleted. Protects against races between concurrent writes and GC. The grace period must be longer than your longest flush cycle.

### Flush and Durability

Writes are durable on local SSD immediately. They are **not** in S3 until flushed. Local disk loss before flush = data loss for unflushed blocks.

- Automatic flush runs on a background schedule (configurable)
- `POST /api/exports/{name}/drain` forces a full flush before shutdown or migration (no snapshot created)
- `POST /api/exports/{name}/snapshot` flushes + creates a versioned snapshot in S3 (returns `sequence` for forking)

### Scrubber

Background integrity verification is disabled by default (`scrubber_blocks_per_second = 0`). The read path already verifies BLAKE3 hashes on S3 fetch. The scrubber re-hashes blocks in the local cache to detect silent SSD corruption — enable it if your workload demands it.

```toml
[servers.nbd]
scrubber_blocks_per_second = 1000  # verify 1000 cached blocks/sec
```

At 1,000 blocks/sec with 128KB blocks: ~2% of one core for BLAKE3 hashing, ~128MB/sec of cache reads. Full pass time depends on cache size.

## Key Design Choices

- **128KB blocks** match ZFS recordsize. Each flush creates one LZ4-compressed pack per modified 128MiB chunk.
- **BLAKE3-128 hashing** for content addressing and integrity verification. Truncated from 256-bit; 128-bit collision resistance is sufficient for dedup.
- **Lock-free write path** using `pread`/`pwrite`, atomic block map with CAS, and monotonic sequence numbers.
- **Typestate pattern** enforces valid lifecycle transitions at compile time. Can't write to a recovering cache.
- **WAL with CRC32** for crash recovery. Torn writes detected and discarded on replay.

See [ARCHITECTURE.md](ARCHITECTURE.md) for wire formats, state machines, and detailed design rationale.

## License

AGPL-3.0
