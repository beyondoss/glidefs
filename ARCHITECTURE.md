# GlideFS Architecture

High-performance NBD server that turns S3 into fast block storage for microVMs, using local SSD as a write-behind cache.

## Data Flow

### Write Path (20us)

```
Guest VM
    │ NBD WRITE
    ▼
NBDServer ──► ExportRouter ──► NBDBlockHandler ──► WriteCache<Active>
                                                        │
                                            ┌───────────┼───────────┐
                                            ▼           ▼           ▼
                                      BLAKE3-128    pwrite()    AtomicBlockMap
                                       (hash)     (local SSD)   (Dirty flag)
                                                        │
                                                   WAL append
                                                        │
                                                    return OK     ◄── 20µs
```

### Read Path (500us hit, 50-300ms miss)

```
Guest VM
    │ NBD READ
    ▼
WriteCache ──► AtomicBlockMap ──► Is block present locally?
                                        │
                         ┌──── YES ─────┼───── NO ──────┐
                         ▼                               ▼
                   pread() from              HostPackIndex lookup
                   local SSD file                    │
                         │                           ▼
                    return data           ContentStore::get_pack()
                     (~500µs)                   (S3 GET)
                                                     │
                                                     ▼
                                              LZ4 decompress
                                                     │
                                                     ▼
                                            Verify BLAKE3 hash
                                                     │
                                                     ▼
                                           Insert into clean cache
                                                     │
                                                return data
                                               (50-300ms)
```

### Background Sync (S3 upload)

```
FlushScheduler
    │
    ▼
Scan AtomicBlockMap for Dirty blocks
    │
    ▼
Batch into packs (25 blocks × 128KB = 3.2MB)
    │
    ├──► LZ4 compress each block
    ├──► Assemble pack (header + index + data)
    └──► ContentStore::put_pack() ──► S3 PUT
              │
              ▼
        HostPackIndex.insert(hash → pack location)
              │
              ▼
        Mark blocks Clean in AtomicBlockMap
              │
              ▼
        Upload manifest to S3 (point-in-time snapshot)
```

## Concepts & Terminology

| Term | Definition | NOT |
|------|-----------|-----|
| Export | A virtual block device served over NBD, with its own cache and S3 prefix | Not a filesystem — raw blocks only |
| Block/Chunk | Fixed-size unit of data (default 128KB to match ZFS recordsize) | Not variable-sized |
| Pack | S3 object containing 25 LZ4-compressed blocks with a self-describing index | Not a single block per S3 object |
| Manifest | Binary snapshot of an export's block map + pack index, stored in S3 | Not a log — it's a point-in-time image |
| Block Map | Per-chunk metadata: BLAKE3-128 hash, dirty flag, sequence number | Not the data itself |
| Pack Index | Host-level hash→pack location mapping for content-addressed lookups | Not per-export — shared across all exports |
| Drain | Flush all dirty blocks to S3 so the export can be stopped or migrated | Not a delete — S3 data is preserved |
| Clean Cache | Read-through cache (Foyer HybridCache) for blocks already in S3 | Not the write cache (dirty blocks) |

## Core Mechanism: Write-Behind Cache

GlideFS decouples write latency from S3 round-trip time. Writes land on local SSD immediately; a background scheduler uploads dirty blocks to S3 as content-addressed packs.

### Lock-Free Hot Path

The write path avoids all locks. Three techniques make this possible:

1. **Positional I/O** (`pread`/`pwrite`): The `SyncFile` wrapper uses POSIX positional I/O, which is atomic per-syscall and doesn't use the file position pointer. No locking needed for concurrent block reads and writes. (`write_cache.rs:SyncFile`)

2. **Atomic block map**: `AtomicBlockMap` stores per-block metadata in parallel arrays of `AtomicU64` (hash halves, sequence) and `AtomicU8` (flags). State transitions use compare-and-swap. (`block_map.rs:AtomicBlockMap`)

3. **Sequence numbers**: A monotonic `AtomicU64` counter provides snapshot consistency. Each write bumps the sequence; flush snapshots capture a consistent cut across all blocks by recording the sequence at snapshot time. (`write_cache.rs:SequenceNumber`)

### Content Addressing

Every block is identified by its BLAKE3-128 hash (16 bytes, truncated from 256-bit). This enables:

- **Deduplication**: Identical blocks across all exports on a host share a single pack entry
- **Integrity verification**: Read path verifies hash after S3 fetch and LZ4 decompression
- **Sparse manifests**: Only non-zero, written chunks are stored — a 500GB export with 2GB of data has a 2GB manifest

The well-known hash of a 128KB zero block (`ZERO_BLOCK_HASH`) lets unwritten regions return zeros without any storage or S3 interaction. (`block_map.rs:77`)

## Block State Machine

```
     ┌──────── write during sync ─────────┐
     │                                    │
     ▼                                    │
  Clean ───[write]───► Dirty ───[sync start]───► Syncing
     ▲                                              │
     │                                              │
     └───────────── upload success ─────────────────┘
                                                    │
                        upload failure ─────────────┘
                             │
                             ▼
                           Dirty (retry next cycle)
```

| From | Event | To | Notes |
|------|-------|----|-------|
| Clean | Guest write | Dirty | Hash computed, data written to SSD, WAL appended |
| Dirty | Sync worker claims | Syncing | CAS on AtomicU8 flag |
| Syncing | S3 PUT success | Clean | Block is durable in S3 |
| Syncing | S3 PUT failure | Dirty | Conservative: re-sync next cycle |
| Syncing | Guest write during sync | Dirty | New data overwrites; block needs re-sync |

Unknown states (e.g., from corruption) default to Dirty — conservative, will re-sync. (`state.rs:59`)

## Device Lifecycle (Typestate)

Compile-time enforcement via Rust's typestate pattern. `WriteCache<S>` is generic over a state marker; only `WriteCache<Active>` exposes read/write/flush methods. Transitions consume `self` and return the new state — you can't accidentally serve I/O during recovery.

```
WriteCache<Initializing>
         │
         │  load local cache, scan WAL
         ▼
WriteCache<Recovering>
         │
         │  re-upload dirty blocks from crash
         ▼
WriteCache<Active>
         │
         │  serve I/O (read/write/flush)
         │  drain triggered
         ▼
WriteCache<Draining>
         │
         │  final flush to S3, no new writes
         ▼
      shutdown
```

(`state.rs:81-95`)

## Wire Formats

### Pack Format (`GLPK`)

Self-describing S3 object. 25 LZ4-compressed blocks with a content-addressed index.

```
┌─────────────────────────── Pack ───────────────────────────┐
│ Header (16 bytes)                                          │
│   magic: "GLPK"  version: 1  block_count  chunk_size       │
├────────────────────────────────────────────────────────────┤
│ Block Index (24 bytes × block_count)                       │
│   [hash:16][offset:u32 LE][comp_length:u32 LE]             │
├────────────────────────────────────────────────────────────┤
│ Block Data                                                 │
│   [LZ4-compressed blocks, concatenated]                    │
│   Offsets in index point into this region                  │
└────────────────────────────────────────────────────────────┘
```

S3 key: `packs/{first-2-hex-of-uuid}/{uuid}` — 256-way prefix sharding for S3 throughput. (`pack.rs:216`)

### Manifest Format (`GLDE`)

Binary snapshot of export state. Sparse: only written chunks are stored.

```
┌─────────────────────────── Manifest ────────────────────────┐
│ Header (46 + name_len bytes)                                │
│   magic: "GLDE"  version: 1  flags  name_len  sequence      │
│   chunk_size  device_size  block_map_count  pack_index_count│
│   name (variable length)                                    │
├─────────────────────────────────────────────────────────────┤
│ Block Map (25 bytes × block_map_count)                      │
│   [chunk_index:u64][hash:16][flags:u8]                      │
├─────────────────────────────────────────────────────────────┤
│ Pack Index (40 bytes × pack_index_count)                    │
│   [hash:16][pack_id:16][offset:u32][comp_length:u32]        │
└─────────────────────────────────────────────────────────────┘
```

S3 key: `manifests/{name}`. Atomic overwrite — the latest manifest is always a consistent snapshot. (`manifest.rs:192`)

### WAL Entry Format

Append-only on local SSD. Metadata only — block data lives in the cache file.

```
[name_len:u16][name][chunk_index:u64][hash:16][sequence:u64][crc32:u32]
```

CRC32 trailer detects torn writes. On recovery, replay stops at the first corrupt entry — the torn tail is discarded, not an error. WAL is truncated after each block map persistence. (`wal.rs:59`)

## Design Decisions

### Why write-behind instead of write-through?

S3 PUT latency is 50-200ms. ZFS issues FLUSH after every snapshot. Write-through would make snapshots take 5-15 seconds instead of <100ms.

Write-behind trades durability for latency: data between the last FLUSH and the next S3 sync is at risk if the host dies. This is acceptable because:

1. Background sync keeps the dirty window small (5s in continuous mode)
2. SIGTERM triggers a drain before exit
3. The workload (microVMs) is ephemeral — VMs can be recreated from base images
4. Production VMs use continuous flush mode to minimize the window

### Why content-addressed packs instead of per-block S3 objects?

Per-block storage means one S3 PUT per 128KB write. At 28K IOPS, that's 28K PUTs/second — prohibitively expensive ($0.14/hour in S3 API costs alone).

Packing 25 blocks per S3 object reduces PUTs by 25x. Content addressing (hash as identity) enables cross-export deduplication on the same host without coordination.

Trade-off: read amplification. A cache miss fetches the entire pack (~3.2MB) even if only one block (128KB) is needed. The 99.67% cache hit rate makes this acceptable — misses are rare, and the extra data often prefills the cache for subsequent reads.

### Why BLAKE3-128 instead of full BLAKE3-256?

128-bit collision resistance is sufficient for content deduplication (birthday bound: 2^64 operations). 16 bytes fits in two `AtomicU64`s for lock-free storage in the block map. Halves per-entry metadata cost vs full 256-bit hash.

### Why 128KB block size?

Matches ZFS default recordsize. Analysis in `BLOCK_SIZE_ANALYSIS.md` shows 128KB is the sweet spot: smaller blocks (16-32KB) reduce write amplification for random I/O but increase metadata overhead and S3 API costs. Larger blocks (256KB+) improve sequential throughput but waste bandwidth for small random writes.

### Why typestate instead of runtime state checks?

Compile-time prevention of invalid operations. You literally cannot call `write()` on a `WriteCache<Recovering>` — the method doesn't exist for that type parameter. No runtime cost, no forgotten state checks.

### Why no block-level compression?

1. ZFS handles compression at its layer — double-compression wastes CPU
2. Would break the fixed block size assumption (compressed blocks vary in size)
3. Content addressing relies on consistent hashing — compressing before hashing would prevent dedup across compression changes

LZ4 compression is applied *inside packs* for S3 storage efficiency, not at the block device layer. (`config.rs:10-14`)

## Package Structure

| File | Purpose |
|------|---------|
| `nbd/server.rs` | TCP/Unix socket listener, NBD protocol negotiation, concurrent request dispatch |
| `nbd/router.rs` | Multi-tenant export manager: create, delete, drain, promote, resize |
| `nbd/handler.rs` | Thin wrapper dispatching NBD commands (read/write/flush) to WriteCache |
| `nbd/write_cache.rs` | Core write-behind cache: local SSD I/O, block map, S3 flush, snapshot |
| `nbd/block_map.rs` | Content-addressed block metadata: `Blake3Hash`, `AtomicBlockMap`, LZ4 helpers |
| `nbd/state.rs` | `BlockState` enum + typestate markers (`Initializing`, `Active`, etc.) |
| `nbd/pack.rs` | Pack wire format: assemble, parse, extract blocks |
| `nbd/pack_index.rs` | Host-level `DashMap<Blake3Hash, PackLocation>` for cross-export dedup |
| `nbd/content_store.rs` | S3 PUT/GET for packs and manifests via `object_store` crate |
| `nbd/manifest.rs` | Binary manifest serialization/deserialization + hot set format |
| `nbd/flush_scheduler.rs` | DemandDriven or Continuous flush modes with `tokio::select!` |
| `nbd/wal.rs` | Append-only WAL for crash recovery with CRC32 integrity |
| `nbd/cache.rs` | `BlockCache` trait + `FoyerBlockCache` (memory + SSD hybrid clean cache) |
| `nbd/readahead.rs` | Sequential read detector: 3+ consecutive chunks triggers pack prefetch |
| `nbd/scrubber.rs` | Background corruption detection: re-hash cached blocks, evict on mismatch |
| `nbd/metrics.rs` | Prometheus-compatible I/O telemetry |
| `nbd/protocol.rs` | NBD wire format: handshake, transmission, simple_reply |
| `nbd/api.rs` | HTTP REST API for export CRUD, drain, promote, metrics |
| `nbd/error.rs` | Error types: `NBDError`, `CommandError`, `RouterError` |
| `config.rs` | TOML configuration parsing with environment variable expansion |
| `cli/server.rs` | `glidefs run` command: wire up config → router → server → API |
| `cli/bless.rs` | `glidefs bless` command: create golden images from local directories |

## Configuration

```toml
# glidefs.toml
[cache]
dir = "/var/cache/glidefs"
disk_size_gb = 100.0
memory_size_gb = 1.0          # Foyer memory tier
ssd_cache_size_gb = 10.0      # Foyer SSD tier

[storage]
url = "s3://my-bucket/vms"

[servers.nbd]
unix_socket = "/var/run/glidefs.sock"
api_address = "127.0.0.1:8080"
auto_create_size_gb = 500.0   # Auto-create exports on connect
```

| Variable | Default | Why |
|----------|---------|-----|
| `block_size` | 128KB | Matches ZFS recordsize |
| `blocks_per_batch` | 25 | 3.2MB per S3 object — balances PUT cost vs read amplification |
| `sync_delay_ms` | 8000 | 8s coalescing window — fewer PUTs, larger dirty window |
| `dirty_budget_gb` | 5.0 | Trigger flush when unflushed data exceeds this |
| `scrubber_blocks_per_second` | 1000 | Background integrity verification rate (0 = disabled) |
| `memory_size_gb` | 1.0 | Foyer in-memory cache for hot blocks |
| `ssd_cache_size_gb` | 10.0 | Foyer SSD tier catches memory evictions |

## Flush Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `DemandDriven` | Flush only on explicit trigger (budget exceeded, API call, shutdown) | Dev/preview VMs where some data loss on host death is acceptable |
| `Continuous` | Periodic pack flush (~5s) + manifest sync (~60s) | Production VMs that need a small dirty window |

Mode can be changed at runtime via `watch::Sender`. The scheduler re-reads the mode on each loop iteration. (`flush_scheduler.rs:74-119`)

## Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Host death before S3 sync | Data loss (writes since last sync) | Recreate VM from base image or last manifest |
| Host death after S3 sync | No data loss | Wake on any node, reads pull from S3 |
| S3 read failure on cache miss | `EIO` to guest | Guest retries; next attempt may succeed |
| S3 write failure during flush | Blocks remain Dirty | Scheduler retries next cycle |
| Manifest upload failure | Blocks not marked Clean | Re-uploaded on next flush |
| Silent data corruption | Stale/wrong data served | Scrubber detects via hash mismatch, evicts; next read re-fetches from S3 |
| Process crash mid-WAL-write | Torn WAL entry | CRC32 detects; replay stops at corruption, discards torn tail |
| Process crash mid-S3-sync | Orphaned packs in S3 | Harmless: packs are immutable, GC can clean up unreferenced packs |