# GlideFS Architecture

High-performance NBD server that turns S3 into fast block storage for microVMs, using local SSD as a write-behind cache.

## Data Flow

### Write Path (~5µs)

```
Guest VM
    │ NBD WRITE
    ▼
NBDServer ──► ExportRouter ──► NBDBlockHandler ──► WriteCache<Active>
                                                        │
                                            ┌───────────┼───────────┐
                                            ▼           ▼           ▼
                                       pwrite()   block_map_set  transition_to_dirty
                                      (local SSD)  (ZERO, seq)    (CAS on SparseStateMap)
                                                        │
                                                   WAL append(ZERO, seq)
                                                        │
                                                    return OK     ◄── ~5µs
```

Hash computation is **deferred to flush time**. The write path stores `Blake3Hash::ZERO`
as a placeholder — it never reads back from SSD, never hashes, never touches the clean
cache. See [Deferred Hashing](#deferred-hashing).

### Read Path (tiered, ~100ns to ~300ms)

```
Guest VM
    │ NBD READ
    ▼
WriteCache ──► AtomicBlockMap lookup
                      │
              ┌── ZERO hash + state≠0? ──► SSD pread   (dirty, hash deferred)
              ├── ZERO hash + state=0? ──► return zeros (never written, NotPresent)
              ├── zero_block_hash? ──► return zeros          (trimmed)
              │
              ├── Tier 1: CleanCache memory (Foyer)           ~100ns
              │           ├─ hit → return
              │           └─ miss ▼
              │
              ├── Tier 2: CleanCache SSD (Foyer)              ~100µs
              │           ├─ hit → return
              │           └─ miss ▼
              │
              ├── Tier 3: S3 pack fetch                       50-300ms
              │           ├─ HostPackIndex lookup (hash → pack location)
              │           ├─ ContentStore::get_block() (S3 range GET)
              │           ├─ LZ4 decompress
              │           ├─ Verify BLAKE3 hash
              │           ├─ Insert into CleanCache
              │           └─ return
              │
              └── Tier 4: SSD pread fallback (dirty block)    ~500µs
                          └─ block present locally but not yet in S3
```

Multi-chunk reads fan out with `futures::future::try_join_all()`. Sequential access (3+ consecutive chunks) triggers pack prefetch to hide S3 latency. (`readahead.rs`)

### Background Sync (S3 upload)

```
FlushScheduler
    │
    ▼
Scan SparseStateMap for Dirty pages (skip unallocated pages)
    │
    ▼
For each dirty block:
    ├── Record (chunk_index, sequence) at snapshot time
    ├── Skip: zero_block_hash entries, blocks already in HostPackIndex
    ├── Read block data from SSD → compute BLAKE3-128 hash (deferred from write)
    └── Dedup check against HostPackIndex
    │
    ▼
Batch into packs (25 blocks × 128KB = ~3.2MB)
    │
    ├──► LZ4 compress each block
    ├──► Assemble pack (GLPK header + index + data)
    └──► ContentStore::put_pack() ──► S3 PUT (concurrent via try_join_all)
              │
              ▼
        HostPackIndex.insert(hash → pack location)
              │
              ▼
        CAS-clear Dirty flags (only if sequence unchanged since snapshot)
              │
              ▼
        Upload manifest to S3 (point-in-time snapshot)
```

## Concepts & Terminology

| Term | Definition | NOT |
|------|-----------|-----|
| Export | A virtual block device served over NBD, with its own cache and S3 prefix | Not a filesystem — raw blocks only |
| Block/Chunk | Fixed-size unit of data (default 128KB to match ZFS recordsize) | Not variable-sized |
| Pack | S3 object containing up to 25 LZ4-compressed blocks with a self-describing index | Not a single block per S3 object |
| Manifest | Binary snapshot of an export's block map + pack index, stored in S3 | Not a log — it's a point-in-time image |
| Block Map | Per-chunk metadata: BLAKE3-128 hash, dirty flag, sequence number | Not the data itself |
| Pack Index | Host-level hash→pack location mapping for content-addressed lookups. Pruned on export removal to bound memory to active exports. | Not per-export — shared across all exports on a host |
| Drain | Flush all dirty blocks to S3 so the export can be stopped or migrated | Not a delete — S3 data is preserved |
| Clean Cache | Read-through Foyer HybridCache (memory + SSD tiers) for blocks fetched from S3 | Not the write cache (dirty blocks on local SSD) |
| Circuit Breaker | Lock-free S3 failure detector that fast-fails requests during outages | Not a retry mechanism — it prevents retries |

## Core Mechanism: Write-Behind Cache

GlideFS decouples write latency from S3 round-trip time. Writes land on local SSD immediately; a background scheduler uploads dirty blocks to S3 as content-addressed packs.

### Lock-Free Hot Path

The write path avoids all locks. Three techniques make this possible:

1. **Positional I/O** (`pread`/`pwrite`): The `SyncFile` wrapper uses POSIX positional I/O, which is atomic per-syscall and doesn't use the file position pointer. No locking needed for concurrent block reads and writes. (`write_cache/inner.rs:SyncFile`)

2. **Atomic block map** (SeqLock + page table): `AtomicBlockMap` stores per-block metadata in a two-level page table. A directory of `AtomicPtr<HashPage>` points to 4KB pages, each holding 128 `HashEntry` structs (version + hash + sequence). Pages are allocated on first write via CAS — empty exports use ~530KB. Each entry uses a per-entry `AtomicU32` version counter (SeqLock); readers spin-retry if the version is odd. (`block_map.rs:AtomicBlockMap`)

3. **Sequence numbers**: A monotonic `AtomicU64` counter provides snapshot consistency and race detection. Each write bumps the sequence; flush captures the sequence at snapshot time and only clears dirty flags on blocks whose sequence hasn't changed (no concurrent write). (`block_map.rs:SequenceNumber`)

### Content Addressing

Every block is identified by its BLAKE3-128 hash (16 bytes, truncated from 256-bit), computed at flush time (not on the write path). This enables:

- **Cross-export deduplication**: Identical blocks across all exports on a host resolve to the same hash in `HostPackIndex` — only stored once in S3. Ten identical 10GB VMs use ~10GB of S3, not 100GB.
- **Integrity verification**: Read path verifies hash after S3 fetch and LZ4 decompression. Background scrubber re-hashes cached blocks to detect bit rot.
- **Sparse manifests**: Only non-zero, written chunks are stored — a 500GB export with 2GB of data has a tiny manifest.

The well-known hash of a 128KB zero block (`ZERO_BLOCK_HASH`) lets unwritten regions return zeros without any storage or S3 interaction. (`block_map.rs:77`)

### Circuit Breaker (S3 Resilience)

A lock-free circuit breaker protects against S3 outages. All mutable state is packed into a single `AtomicU64` — state transitions use compare-and-swap, no multi-variable coordination.

```
                failure_threshold reached
    ┌─────────┐ ──────────────────────────► ┌────────┐
    │ Closed  │                             │  Open  │
    └─────────┘ ◄────────────────────────── └────────┘
         ▲        success in half-open           │
         │                                       │ reset_timeout elapsed
         │        ┌─────────────┐                │
         └─────── │  Half-Open  │ ◄──────────────┘
           success└─────────────┘
                        │ failure
                        ▼
                   back to Open
```

| From | Event | To | Default |
|------|-------|----|---------|
| Closed | N consecutive connectivity failures | Open | N = 5 |
| Open | Reset timeout elapsed | Half-Open | 30s |
| Half-Open | Probe request succeeds (×3) | Closed | 3 probes |
| Half-Open | Any probe fails | Open | — |

Two failure policies: **Consecutive** (N failures in a row) and **Windowed** (N failures within a time window). Only connectivity errors count — business logic errors (404, etc.) don't trip the breaker. (`circuit_breaker.rs`)

## Block State Machine

Stored in `SparseStateMap` — a sparse page table of `AtomicU8` values, lock-free, outside the block map's `RwLock`. Encoding: `NotPresent=0, Clean=1, Dirty=2, Syncing=3`. NotPresent=0 means unallocated pages are implicitly "never written" with no memory cost.

```
NotPresent ───[write]───► Dirty ───[sync start]───► Syncing
                            ▲           │                │
                            │    write during sync ──────┤
                            │                            │
                          Clean ◄── upload success ──────┘
                            │                            │
                            └──[write]──► Dirty    upload failure
                                                         │
                                                         ▼
                                                   Dirty (retry)
```

| From | Event | To | Encoding | Notes |
|------|-------|----|----------|-------|
| NotPresent | Guest write | Dirty | 0 → 2 | Page allocated on first touch, SSD pwrite, WAL appended |
| Clean | Guest write | Dirty | 1 → 2 | Block already on SSD, re-dirtied |
| Dirty | Sync worker claims | Syncing | 2 → 3 | CAS on SparseStateMap entry |
| Syncing | S3 PUT success | Clean | 3 → 1 | Block is durable in S3 |
| Syncing | S3 PUT failure | Dirty | 3 → 2 | Conservative: re-sync next cycle |
| Syncing | Guest write during sync | Dirty | 3 → 2 | New data overwrites; block needs re-sync |

Presence is derived: `is_present = state != 0`. This eliminates the separate `present_chunks` bitmap. (`block_map.rs:SparseStateMap`)

## Fork Overlay (BlockMapKind)

Forking a VM copies the block map — but a fork is 99% identical to its parent until it diverges. With 180 preview VMs forked from 10 production VMs, full copies waste memory: 180 nearly-identical arrays.

`BlockMapKind` dispatches between two runtime representations:

```
BlockMapKind
    ├── Full(AtomicBlockMap)       ← normal VMs, or flattened forks
    └── Forked(ForkedBlockMap)     ← freshly forked VMs
```

`ForkedBlockMap` holds a shared reference to the parent and a sparse `AtomicBlockMap` overlay of diverged entries:

```
ForkedBlockMap
    parent:   Arc<BlockMap>       ← shared, immutable
    overlay:  AtomicBlockMap      ← sparse page table, lock-free (SeqLock)

Read(chunk_index):
    overlay.get(chunk_index)
        → (ZERO, 0)?  parent[chunk_index]     ← not in overlay, fall through
        → (hash, seq)? return it               ← fork has written here

Write(chunk_index, hash, seq):
    overlay.set(chunk_index, hash, seq)        ← never touches parent
```

The overlay distinguishes "not written by fork" from "wrote ZERO placeholder" using the sequence number: `SequenceNumber` starts at 1, so `(ZERO hash, seq=0)` = not in overlay, `(ZERO hash, seq>0)` = fork wrote deferred-hash placeholder.

180 forks with ~1% divergence: 10 × 1.3MB (parents) + 180 × ~540KB (overlay directory + pages) = ~98MB. More than a DashMap overlay (~4MB) but genuinely lock-free — no shard locks on the write hot path.

### Auto-Flatten

When the overlay exceeds 50% of the parent's entries, the fork has diverged enough that overlay lookup overhead isn't worth the memory savings. `try_flatten_block_map()` merges parent + overlay into a full `AtomicBlockMap`, replacing the `Forked` variant with `Full`. Double-checked under write lock to prevent TOCTOU races. Called from the write and zero_range paths. (`write_cache/flush.rs`)

### State Is Separate

`SparseStateMap` (dirty/syncing/clean/not-present) lives outside `BlockMapKind` in `CacheInner`, using the same sparse page-table pattern as `AtomicBlockMap` but with 4096 `AtomicU8` entries per page. This separation is deliberate:

- **Lock-free state transitions**: State ops (`set_present`, `transition_to_dirty`) use direct CAS on `AtomicU8` — no RwLock. Co-locating state inside `AtomicBlockMap` would force every state transition through the block map's read lock.
- **Independent per-fork state**: Each fork has its own `SparseStateMap` while sharing the parent's hash data through `Arc<BlockMap>`. The parent never needs mutation.
- **Shared memory budget**: Both `AtomicBlockMap` and `SparseStateMap` share an `Arc<AtomicUsize>` page budget. When exhausted, writes fail with ENOSPC propagated to the NBD client.

Created via `open_from_manifest()` when a parent block map is provided. (`block_map.rs:SparseStateMap`, `write_cache/init.rs:open_from_manifest`)

## Device Lifecycle (Typestate)

Compile-time enforcement via Rust's typestate pattern. `WriteCache<S>` is generic over a sealed state marker; only `WriteCache<Active>` exposes read/write/flush methods. Transitions consume `self` and return the new state — you can't accidentally serve I/O during recovery.

```
WriteCache<Initializing>
         │
         │  load local cache, scan WAL
         ▼
WriteCache<Recovering>
         │
         │  verify dirty block hashes against SSD
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

(`state.rs:81-115`, `write_cache/mod.rs`)

## Wire Formats

### Pack Format (`GLPK`)

Self-describing S3 object. Up to 25 LZ4-compressed blocks with a content-addressed index.

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

S3 key: `packs/{first-2-hex-of-uuid}/{uuid}` — 256-way prefix sharding for S3 throughput. (`pack.rs`)

### Manifest Format (`GLDE`)

Binary snapshot of export state. Sparse: only written chunks are stored. CRC32 trailer for integrity.

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
├─────────────────────────────────────────────────────────────┤
│ Trailing CRC32 (4 bytes)                                    │
└─────────────────────────────────────────────────────────────┘
```

S3 key: `manifests/{name}`. Atomic overwrite — the latest manifest is always a consistent snapshot. (`manifest.rs`)

### WAL Entry Format

Append-only on local SSD. Metadata only — block data lives in the cache file.

```
[name_len:u16][name][chunk_index:u64][hash:16][sequence:u64][crc32:u32]
```

CRC32 trailer detects torn writes. On recovery, replay stops at the first corrupt entry — the torn tail is discarded, not an error. WAL is truncated after each block map persistence. (`wal.rs`)

## Background Subsystems

### Scrubber (Integrity Verification)

Rate-limited background task that re-hashes blocks in the CleanCache against their content address. On mismatch (bit rot, memory corruption), evicts the block — the next read re-fetches from S3, which is the authoritative source.

- Iterates all hashes in `HostPackIndex`, checks if each is in CleanCache
- Rate-limited: `scrubber_blocks_per_second` (default 1000, 0 = disabled)
- 60s sleep between full passes
- Prometheus counters: `blocks_checked`, `blocks_evicted`

(`scrubber.rs`)

### Sequential Readahead

Ring buffer (4 entries) tracks recent chunk accesses. When 3+ consecutive chunks are read (boot, large file copy), triggers prefetch of the next pack's first chunk. Deduplicates triggers per pack boundary.

This hides S3 latency for sequential workloads — the next pack is already being fetched while the current one is being served. (`readahead.rs`)

## Management API

HTTP REST API for orchestrators (scale-to-zero, live migration). (`api.rs`)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/exports/{name}` | `PUT` | Create or resize export (idempotent) |
| `/api/exports/{name}` | `GET` | Get export info (size, readonly, flush mode) |
| `/api/exports/{name}` | `DELETE` | Remove export (after drain) |
| `/api/exports` | `GET` | List all exports |
| `/api/exports/{name}/snapshot` | `POST` | Drain dirty blocks + upload manifest |
| `/api/exports/{name}/promote` | `POST` | Toggle readonly flag |
| `/api/exports/{name}/metrics` | `GET` | Per-export metrics snapshot (JSON) |
| `/metrics` | `GET` | Prometheus scrape endpoint (all exports) |

## Observability

Per-export Prometheus metrics exposed at `/metrics`. Latency histograms are sampled 1:64 to reduce mutex contention at high IOPS. (`metrics.rs`)

| Metric | Type | What It Tells You |
|--------|------|-------------------|
| `glidefs_guest_write_ops_total` | Counter | Guest write IOPS |
| `glidefs_guest_bytes_written_total` | Counter | Guest write throughput |
| `glidefs_guest_read_ops_total` | Counter | Guest read IOPS |
| `glidefs_guest_bytes_read_total` | Counter | Guest read throughput |
| `glidefs_s3_batches_written_total` | Counter | Pack upload rate |
| `glidefs_s3_bytes_read_total` | Counter | Bytes fetched from S3 (compressed, on cache miss) |
| `glidefs_s3_read_ops_total` | Counter | S3 GET operations (individual block fetches) |
| `glidefs_cache_hits_total` | Counter | Reads served from CleanCache (Foyer memory/SSD) |
| `glidefs_cache_misses_total` | Counter | Reads that required S3 fetch |
| `glidefs_cache_hit_rate` | Gauge | `cache_hits / (hits + misses)` — read cache effectiveness |
| `glidefs_write_amplification` | Gauge | S3 bytes / guest bytes (should be ~1.0) |
| `glidefs_coalesce_ratio` | Gauge | Guest writes per S3 batch (higher = better batching) |
| `glidefs_dirty_blocks` | Gauge | Blocks waiting for S3 sync |
| `glidefs_syncing_blocks` | Gauge | Blocks currently uploading |
| `glidefs_read_latency_seconds` | Histogram | End-to-end NBD read latency |
| `glidefs_write_latency_seconds` | Histogram | End-to-end NBD write latency |
| `glidefs_s3_fetch_latency_seconds` | Histogram | S3 GET latency (cache misses) |
| `glidefs_s3_put_latency_seconds` | Histogram | S3 PUT latency (pack uploads) |
| `glidefs_s3_put_errors_total` | Counter | S3 upload failures |
| `glidefs_s3_get_errors_total` | Counter | S3 fetch failures |
| `glidefs_flush_errors_total` | Counter | Failed flush cycles |
| `glidefs_flush_blocks_cas_failed_total` | Counter | Blocks left dirty per flush due to concurrent writes (flush starvation indicator) |

Histogram buckets: `<100µs`, `<1ms`, `<10ms`, `<100ms`, `<1s`, `>=1s`.

## Design Decisions

### Why write-behind instead of write-through?

S3 PUT latency is 50-200ms. ZFS issues FLUSH after every snapshot. Write-through would make snapshots take 5-15 seconds instead of <100ms.

Write-behind trades durability for latency: data between the last FLUSH and the next S3 sync is at risk if the host dies. This is acceptable because:

1. Background sync keeps the dirty window small (5s in continuous mode)
2. SIGTERM triggers a drain before exit
3. The workload (microVMs) is ephemeral — VMs can be recreated from base images
4. Production VMs use continuous flush mode to minimize the window

### Why defer hashing to flush time?

The previous design computed BLAKE3 on every write. For a 4KB write to a 128KB block:

| Operation | Cost |
|-----------|------|
| pread 128KB from SSD | ~15-25µs |
| blake3(128KB) | ~20-30µs |
| Bytes::copy_from_slice(128KB) into clean cache | ~5-10µs |
| **Total overhead** | **~50-65µs** |

None of this work is needed until flush time. With deferred hashing, the write path is ~5µs for 4KB random writes (just pwrite + atomics + WAL). The hash computation moves to the flush path, which already reads every dirty block from SSD to build packs — so the work happens exactly once.

**Write coalescing is free**: write the same block 100 times before flush, hash it once. Previously: 100 hashes, 99 thrown away.

The read path distinguishes ZERO-placeholder from never-written using the **state map**: ZERO hash + state != NotPresent = deferred hash (SSD pread), ZERO hash + NotPresent = never written (return zeros).

Sequence numbers replace hashes as the race detection token in flush. There is a narrow TOCTOU window between checking the sequence and doing the CAS (nanoseconds), identical to the previous hash-based approach and self-healing.

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

LZ4 compression is applied *inside packs* for S3 storage efficiency, not at the block device layer.

### Why a lock-free circuit breaker?

S3 outages shouldn't cascade into mutex contention on the hot path. All circuit breaker state is packed into a single `AtomicU64` — no locks, no multi-variable coordination. CAS loops guarantee consistent state transitions even under high concurrency.

We use a consecutive-failure policy (not windowed) by default because S3 outages tend to be total, not partial.

### Why sparse page tables instead of dense arrays?

Dense arrays pre-allocate for all blocks: a 1TB export with 128KB blocks has 8M entries. `AtomicBlockMap` alone cost ~224MB per export — at 2,000 VMs per compute node, that's 448GB just for hash metadata. Impossible.

Sparse page tables allocate on first write. The directory (one pointer per page) costs ~530KB. Each 4KB page covers 128 hash entries or 4096 state entries. An empty export: ~530KB. A 1%-written export: ~5MB. The cost is one extra pointer dereference on the hot path — a branch that predicts correctly almost every time and is noise next to the SSD pwrite that follows it.

We considered co-locating state into `HashEntry` (add `state: AtomicU8`, bump to 40 bytes, 102 entries/page). Rejected because state transitions (`set_present`, `transition_to_dirty`) are lock-free today — direct CAS on `AtomicU8`. `AtomicBlockMap` is behind a `RwLock` (for fork-overlay swaps). Co-locating would force every state transition through a read lock. Two separate sparse structures preserve lock-free state transitions while achieving the same memory savings.

### Why SeqLock instead of RwLock for the block map?

Each block's metadata (hash + sequence) spans multiple `AtomicU64`s. A torn read (seeing half-old, half-new) would produce a wrong hash. SeqLock solves this with per-entry version counters — readers spin-retry if the version changed during read. Cost: near-zero, because writes to a specific chunk are rare relative to reads, and each chunk has its own version counter.

We considered `RwLock` but rejected it: even uncontended lock/unlock has ~25ns overhead per operation. At 28K IOPS with multi-chunk reads, that's significant. SeqLock adds ~2ns on the reader fast path.

**C11 memory ordering**: The writer stores data fields with `Release` ordering, not `Relaxed`. Under the C11 model (which matters on ARM/Graviton), a `Relaxed` data store can be observed by a reader via a `Relaxed` load without establishing any happens-before relationship — the reader's subsequent `Relaxed` v2 load might miss the writer's version change entirely, producing a torn read. With `Release` data stores, the reader's `Acquire` fence (between data loads and v2 load) synchronizes-with the observed Release, making the writer's odd version visible to v2 and forcing a retry. Loom tests exhaustively verify this property. SeqLock is single-writer by design; concurrent writers break the version parity invariant.

## Package Structure

| File | Purpose |
|------|---------|
| `nbd/server.rs` | TCP/Unix socket listener, NBD protocol negotiation, concurrent request dispatch |
| `nbd/router.rs` | Multi-tenant export manager: create, delete, drain, promote, resize |
| `nbd/handler.rs` | Thin wrapper dispatching NBD commands (read/write/flush) to WriteCache |
| `nbd/write_cache/mod.rs` | `WriteCache<S>` typestate wrapper, `FlushStats`, `SnapshotResult` |
| `nbd/write_cache/inner.rs` | `CacheInner`: shared state, `SyncFile`, `SparseStateMap` integration, metadata persistence |
| `nbd/write_cache/write.rs` | Write path: pwrite + ZERO placeholder + WAL (deferred hash) |
| `nbd/write_cache/read.rs` | Read path: tiered cache resolution (CleanCache → S3 → SSD fallback) |
| `nbd/write_cache/flush.rs` | Dirty block scan, pack assembly, S3 upload, manifest sync |
| `nbd/write_cache/init.rs` | Cache file creation, pre-allocation, metadata loading |
| `nbd/write_cache/recovery.rs` | WAL replay, dirty block verification after crash |
| `nbd/write_cache/config.rs` | `WriteCacheConfig` with per-export overrides |
| `nbd/write_cache/error.rs` | `CacheError` type |
| `nbd/block_map.rs` | `Blake3Hash`, `AtomicBlockMap` (sparse page-table + SeqLock), `SparseStateMap`, `SequenceNumber`, LZ4 helpers |
| `nbd/state.rs` | `BlockState` enum + sealed typestate markers (`Initializing`, `Active`, etc.) |
| `nbd/pack.rs` | Pack wire format (GLPK): assemble, parse, extract blocks |
| `nbd/pack_index.rs` | `HostPackIndex`: `DashMap<Blake3Hash, PackLocation>` for cross-export dedup |
| `nbd/pack_registry.rs` | Per-export pack ID tracking for garbage collection |
| `nbd/content_store.rs` | S3 PUT/GET for packs and manifests via `object_store` crate |
| `nbd/manifest.rs` | Binary manifest serialization/deserialization (GLDE format) |
| `nbd/flush_scheduler.rs` | `DemandDriven` or `Continuous` flush modes with `tokio::select!` |
| `nbd/wal.rs` | Append-only WAL for crash recovery with CRC32 integrity |
| `nbd/cache.rs` | `BlockCache` trait + `FoyerBlockCache` (memory + SSD hybrid) |
| `nbd/readahead.rs` | Sequential read detector: 3+ consecutive chunks triggers pack prefetch |
| `nbd/scrubber.rs` | Background corruption detection: re-hash cached blocks, evict on mismatch |
| `nbd/sync.rs` | Loom/std compatibility shim: re-exports atomics for exhaustive interleaving tests |
| `nbd/metrics.rs` | Per-export Prometheus-compatible telemetry with sampled latency histograms |
| `nbd/protocol.rs` | NBD wire format: handshake options, transmission commands, reply serialization |
| `nbd/api.rs` | HTTP REST API for export CRUD, drain, promote, metrics |
| `nbd/error.rs` | Error types: `NBDError`, `CommandError`, `RouterError` |
| `circuit_breaker.rs` | Lock-free S3 circuit breaker (single AtomicU64, CAS transitions) |
| `config.rs` | TOML configuration parsing with environment variable expansion |
| `cli/server.rs` | `glidefs run`: wire up config → router → server → API |
| `cli/bless.rs` | `glidefs bless`: create golden images from local directories |

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
```

| Variable | Default | Why |
|----------|---------|-----|
| `block_size` | 128KB | Matches ZFS recordsize |
| `scrubber_blocks_per_second` | 1000 | Background integrity rate; 0 = disabled |
| `memory_size_gb` | 1.0 | Foyer in-memory cache for hot blocks |
| `ssd_cache_size_gb` | 10.0 | Foyer SSD tier catches memory evictions |
| `connect_timeout_secs` | 10 | S3 connection timeout |
| `request_timeout_secs` | 300 | S3 request timeout (large packs take time) |
| `shutdown_timeout_secs` | 30 | Grace period for drain on SIGTERM |
| `wal_sync` | false | fsync WAL per batch; true = slower but crash-safe metadata |

## Flush Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `DemandDriven` | Flush only on explicit trigger (API call, drain, shutdown). Local checkpoint every 5s keeps WAL bounded. | Dev/preview VMs where some data loss on host death is acceptable |
| `Continuous` | Periodic pack flush (~5s) + manifest sync (~60s) | Production VMs that need a small dirty window |

Mode can be changed at runtime via `watch::Sender`. The scheduler re-reads the mode on each loop iteration. (`flush_scheduler.rs`)

## Memory Overhead

Both `AtomicBlockMap` and `SparseStateMap` use sparse page tables — pages are allocated on first write, not upfront. An empty export costs only its directory arrays (~530KB). Memory grows proportionally to blocks actually written.

Previously, three dense arrays were pre-allocated for all 8M blocks regardless of usage: `AtomicBlockMap` (4 parallel arrays of AtomicU64/AtomicU32 = ~224MB), `block_states` (1 byte × 8M = 8MB), and `present_chunks` (1 bit × 8M = 1MB) — **~233MB per export**. At 2,000 VMs: 466GB. The sparse page tables replace all three with allocate-on-write directories: **~530KB per empty export** — a 450× reduction.

```
AtomicBlockMap (hash + sequence storage)
├── directory: Box<[AtomicPtr<HashPage>]>    512 KB  (65536 entries for 8M blocks)
│   ├── [0] → HashPage { entries: [HashEntry; 128] }  4096 bytes
│   ├── [1] → null  (unwritten — zero cost)
│   └── ...
│   HashEntry: 32 bytes (#[repr(C)]): version(u32) + _pad(u32) + hash_lo(u64) + hash_hi(u64) + seq(u64)

SparseStateMap (block state: NotPresent/Clean/Dirty/Syncing)
├── directory: Box<[AtomicPtr<StatePage>]>    16 KB  (2048 entries for 8M blocks)
│   ├── [0] → StatePage { states: [AtomicU8; 4096] }  4096 bytes
│   ├── [1] → null  (unwritten — zero cost)
│   └── ...
```

| Component | Per-Export (fixed) | Per-Written-Page | Shared | Notes |
|-----------|-------------------|-----------------|--------|-------|
| `AtomicBlockMap` directory | ~512 KB | 4 KB per 128 blocks written | — | 32 bytes/entry × 128 entries/page |
| `SparseStateMap` directory | ~16 KB | 4 KB per 4096 blocks written | — | 1 byte/entry × 4096 entries/page |
| `HostPackIndex` | — | — | ~56 bytes/unique hash | DashMap, pruned on export removal |
| `CleanCache` (memory) | — | — | `memory_size_gb` | Configured, default 1GB |
| `CleanCache` (SSD) | — | — | `ssd_cache_size_gb` | Configured, default 10GB |

| Scenario (1TB/128KB = 8M blocks) | Memory |
|---|---|
| Empty export | ~530 KB |
| 1% written (84K blocks) | ~5 MB |
| 100% written | ~260 MB |
| 2,000 empty exports | ~1 GB |
| 2,000 exports, 1% written | ~10 GB |

### Memory Budget

Each export's page tables share an `Arc<AtomicUsize>` budget counter. Page allocation decrements the budget; when exhausted, writes return `MetadataLimitExceeded` → NBD ENOSPC. This prevents a runaway guest from consuming memory budgeted for other VMs. (`block_map.rs:ensure_page`)

## Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Host death before S3 sync | Data loss (writes since last sync) | Recreate VM from base image or last manifest |
| Host death after S3 sync | No data loss | Wake on any node, reads pull from S3 |
| S3 read failure on cache miss | `EIO` to guest | Guest retries; circuit breaker fast-fails if S3 is down |
| S3 write failure during flush | Blocks remain Dirty | Scheduler retries next cycle; circuit breaker limits attempts |
| Manifest upload failure | Stale manifest in S3 | Re-uploaded on next flush cycle |
| Silent data corruption | Stale/wrong data served | Scrubber detects via hash mismatch, evicts; next read re-fetches from S3 |
| Process crash mid-WAL-write | Torn WAL entry | CRC32 detects; replay stops at corruption, discards torn tail |
| Process crash mid-S3-sync | Orphaned packs in S3 | Harmless: packs are immutable, GC can clean up unreferenced packs |
| S3 sustained outage | Circuit breaker opens | Reads from local SSD continue; writes accumulate locally; breaker probes S3 every 30s |
| Metadata budget exhaustion | `ENOSPC` to guest | Guest sees write failure; existing data intact; flush frees budget by clearing dirty blocks |

## Testing

| Suite | Command | Count | What It Covers |
|-------|---------|-------|----------------|
| Unit | `cargo test --features test-utils --lib` | ~307 | Lock-free atomics, wire format round-trips, state transitions, sparse page tables |
| Integration | `cargo test --features test-utils --test integration` | ~46 | Crash recovery, concurrent writes, flush consistency (no Docker) |
| Docker | `cargo test --features docker-tests --test docker_integration` | ~9 | Real S3 via MinIO (testcontainers-rs), end-to-end pack upload/download |
| Loom | `cd loom-tests && cargo test --release` | — | Exhaustive interleaving of lock-free algorithms (AtomicBlockMap, SeqLock) |
