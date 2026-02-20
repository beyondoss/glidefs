# GlideFS

Block device server that turns S3 into fast local storage. Writes hit local SSD in 5 microseconds. Background sync uploads to S3 as content-addressed packs.

Built for microVM storage at [Paraglide](https://paraglide.sh).

## How It Works

Guests see a standard block device (NBD or ublk). Writes go to local SSD immediately. A background scheduler packs dirty blocks, compresses with LZ4, and uploads to S3. Reads serve from local cache; misses pull from S3, verify BLAKE3 hashes, and cache locally.

```
Write path:  Guest → NBD → local SSD pwrite() → return OK      ~5µs
Read path:   Guest → NBD → local cache hit → return data       ~500µs
             Guest → NBD → cache miss → S3 GET → LZ4 → verify → cache → return   50-300ms
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

Exports are virtual block devices. Manage them over HTTP:

```sh
# Create a 500GB export
curl -X PUT localhost:8080/api/exports/my-vm \
  -d '{"size_gb": 500}'

# Fork from an existing manifest
curl -X PUT localhost:8080/api/exports/my-vm-fork \
  -d '{"size_gb": 500, "manifest_name": "my-vm"}'

# Snapshot to S3
curl -X POST localhost:8080/api/exports/my-vm/snapshot

# Drain (flush all dirty blocks, prepare for migration)
curl -X POST localhost:8080/api/exports/my-vm/drain

# Delete
curl -X DELETE localhost:8080/api/exports/my-vm
```

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/exports` | GET | List exports |
| `/api/exports/{name}` | PUT | Create or update export |
| `/api/exports/{name}` | GET | Get export info |
| `/api/exports/{name}` | DELETE | Remove export |
| `/api/exports/{name}/drain` | POST | Flush to S3 for shutdown/migration |
| `/api/exports/{name}/snapshot` | POST | Point-in-time snapshot to S3 |
| `/api/exports/{name}/promote` | POST | Promote readonly to read-write |
| `/health/ready` | GET | Readiness check |
| `/api/exports/{name}/metrics` | GET | I/O metrics |
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |

## Base Images

Create content-addressed base images from raw disk files:

```sh
glidefs bless --image ubuntu-22.04.raw --name ubuntu-22.04-v1 --config glidefs.toml
```

Exports forked from base images share blocks via content addressing. Identical data is stored once.

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

### Device Setup (NBD)

Three ways to attach `/dev/nbdN` to the server:

| Method | Kernel | Notes |
|---|---|---|
| `nbd-client` | Any | External dependency. `nbd-client -N myexport -u /var/run/glidefs.sock /dev/nbd0` |
| ioctl | Any | `NBD_SET_SOCK` + `NBD_SET_SIZE` + `NBD_DO_IT`. Blocks a thread per device. No reconnect. |
| Netlink (`NBD_GENL`) | 4.10+ | Preferred. Non-blocking, supports `NBD_CMD_RECONFIGURE` for live resize, multiple sockets per device for failover. No external tools. |

Netlink is the right choice for NBD production. Create the export via HTTP API, configure the kernel device via netlink, connect over UDS — single binary, no moving parts.

ublk devices need no client setup — `/dev/ublkbN` appears when the export registers and disappears when it's removed.

### Binary Upgrades (Zero-Downtime)

GlideFS supports binary upgrades without VM disruption when using netlink-based NBD (`NBD_GENL`). The kernel holds the socket fd and queues I/O during the restart window.

```
1. Set NBD_ATTR_DEAD_CONN_TIMEOUT on device setup (e.g. 30s)
2. SIGUSR1 → drain all exports to S3
3. SIGTERM → graceful shutdown
4. Start new binary (same config, same cache dir)
5. New process recovers exports in parallel from local WAL + redb
6. /health/ready returns 200 → all exports serving
7. NBD_CMD_RECONFIGURE with new socket fds
8. Kernel resumes queued I/O
```

The block device stays alive throughout. Firecracker never sees a disconnect.

Recovery is local — the new process reads WAL and redb from the same SSD, not S3. Discovery (S3) runs 32-wide parallel, export creation (local I/O) runs 16-wide parallel. No S3 writes on the recovery path. The `/health/ready` endpoint gates on all exports being loaded, cache writable, and S3 reachable.

`dead_conn_timeout` must exceed: drain time + process restart + discovery + parallel WAL recovery. 2000 exports recover in ~6 seconds. 30 seconds is conservative.

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

### Flush and Durability

Writes are durable on local SSD immediately. They are **not** in S3 until flushed. Local disk loss before flush = data loss for unflushed blocks.

- Automatic flush runs on a background schedule (configurable)
- `POST /api/exports/{name}/drain` forces a full flush before shutdown or migration
- `POST /api/exports/{name}/snapshot` creates a point-in-time manifest in S3

### Scrubber

Background integrity verification is disabled by default (`scrubber_blocks_per_second = 0`). The read path already verifies BLAKE3 hashes on S3 fetch. The scrubber re-hashes blocks in the local cache to detect silent SSD corruption — enable it if your workload demands it.

```toml
[servers.nbd]
scrubber_blocks_per_second = 1000  # verify 1000 cached blocks/sec
```

At 1,000 blocks/sec with 128KB blocks: ~2% of one core for BLAKE3 hashing, ~128MB/sec of cache reads. Full pass time depends on cache size.

## Key Design Choices

- **128KB blocks** match ZFS recordsize. One S3 object holds 25 compressed blocks (~3.2MB).
- **BLAKE3-128 hashing** for content addressing and integrity verification. Truncated from 256-bit; 128-bit collision resistance is sufficient for dedup.
- **Lock-free write path** using `pread`/`pwrite`, atomic block map with CAS, and monotonic sequence numbers.
- **Typestate pattern** enforces valid lifecycle transitions at compile time. Can't write to a recovering cache.
- **WAL with CRC32** for crash recovery. Torn writes detected and discarded on replay.

See [ARCHITECTURE.md](ARCHITECTURE.md) for wire formats, state machines, and detailed design rationale.

## License

AGPL-3.0
