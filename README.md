# GlideFS

NBD block device server that turns S3 into fast local storage. Writes hit local SSD in 20 microseconds. Background sync uploads to S3 as content-addressed packs.

Built for microVM storage at [Paraglide](https://paraglide.sh).

## How It Works

Guests see a block device over NBD. Writes go to local SSD immediately. A background scheduler packs dirty blocks, compresses with LZ4, and uploads to S3. Reads serve from local cache; misses pull from S3, verify BLAKE3 hashes, and cache locally.

```
Write path:  Guest → NBD → local SSD pwrite() → return OK     ~20µs
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
| `/api/exports/{name}/flush-mode` | POST | Set flush mode |
| `/api/exports/{name}/metrics` | GET | I/O metrics |
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |

## Base Images

Create content-addressed base images from raw disk files:

```sh
glidefs bless --image ubuntu-22.04.raw --name ubuntu-22.04-v1 --config glidefs.toml
```

Exports forked from base images share blocks via content addressing. Identical data is stored once.

## Key Design Choices

- **128KB blocks** match ZFS recordsize. One S3 object holds 25 compressed blocks (~3.2MB).
- **BLAKE3-128 hashing** for content addressing and integrity verification. Truncated from 256-bit; 128-bit collision resistance is sufficient for dedup.
- **Lock-free write path** using `pread`/`pwrite`, atomic block map with CAS, and monotonic sequence numbers.
- **Typestate pattern** enforces valid lifecycle transitions at compile time. Can't write to a recovering cache.
- **WAL with CRC32** for crash recovery. Torn writes detected and discarded on replay.

See [ARCHITECTURE.md](ARCHITECTURE.md) for wire formats, state machines, and detailed design rationale.

## License

AGPL-3.0
