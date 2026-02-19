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
                                                   clear_crc32(0)
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
              │           ├─ HostPackIndex lookup (redb, hash → pack location)
              │           ├─ ContentStore::get_block() (S3 range GET, semaphore-gated)
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
FlushScheduler (event-driven: Notify from write path when dirty_count ≥ 100)
    │
    ▼
Scan SparseStateMap for Dirty pages (skip unallocated pages)
    │
    ▼
For each dirty block (via spawn_blocking — off async runtime):
    ├── Record (chunk_index, sequence) at snapshot time
    ├── Skip: zero_block_hash entries, blocks already in HostPackIndex
    ├── Read block data from SSD → CRC32 verify → BLAKE3-128 hash → LZ4 compress
    └── Dedup check against HostPackIndex
    │
    ▼
Assemble packs (up to 100 blocks × 128KB = ~12.8MB, async)
    │
    └──► ContentStore::put_pack() ──► S3 PUT (concurrent, semaphore-gated)
              │
              ▼
        HostPackIndex.insert_batch(hash → pack location)
              │
              ▼
        CAS-clear Dirty flags (only if sequence unchanged since snapshot)

Manifest sync (delta or full) after successful pack upload.
Ensures flushed packs are discoverable on cross-host recovery.
```

### Delta Manifests

Background sync uploads a **delta manifest** containing only blocks that changed since the last full (base) manifest. This reduces S3 bandwidth from O(all blocks) to O(changed blocks) — a flush that touched 100 blocks uploads ~2.5KB instead of the full manifest.

**S3 layout**: `manifests/{name}` (full/base) + `manifests/{name}.delta` (single delta, no chains). At most 2 S3 GETs to restore.

**How it works**: The flush path caches the last full manifest's block map as a `HashMap<u64, Blake3Hash>`. On sync, it diffs the current block map against the cached base — upserts are entries that differ, deletes are entries in the base but absent from the current map. The delta is serialized and uploaded to `manifests/{name}.delta`, replacing any previous delta.

**Compaction** (fall back to full manifest upload):
1. No base state yet (first sync after open)
2. Delta block size > 50% of estimated full manifest size
3. `syncs_since_base >= 10`
4. Explicit `snapshot()` or `flush_to_s3()` (fork, migration)

Compaction uploads a full manifest and deletes the `.delta` file.

**Restore**: `get_effective_manifest()` fetches the base, optionally fetches the delta. If the delta's `base_sequence` matches the base's `sequence`, it merges them via `apply_to()`. Stale deltas (wrong `base_sequence`) are ignored with a warning — the base is used as-is.

**GC safety**: GC takes the conservative union of pack entries from both base and delta manifests. Dead packs from overwritten blocks survive one compaction cycle, well within the 24h grace period. (`manifest.rs`, `content_store.rs`, `write_cache/flush.rs`)

## Concepts & Terminology

| Term | Definition | NOT |
|------|-----------|-----|
| Export | A virtual block device served over NBD, with its own cache and S3 prefix | Not a filesystem — raw blocks only |
| Block/Chunk | Fixed-size unit of data (default 128KB to match ZFS recordsize) | Not variable-sized |
| Pack | S3 object containing up to 100 LZ4-compressed blocks with a self-describing index | Not a single block per S3 object |
| Manifest | Binary snapshot of an export's block map + pack index, stored in S3. Synced as delta (changed blocks only) with periodic compaction to full snapshot. | Not a log — it's a point-in-time image |
| Block Map | Per-chunk metadata: BLAKE3-128 hash, dirty flag, sequence number | Not the data itself |
| Pack Index | Host-level hash→pack location mapping for content-addressed lookups. Backed by redb (disk-resident, not RAM). Pruned on export removal to bound size to active exports. | Not per-export — shared across all exports on a host |
| Drain | Flush all dirty blocks to S3 so the export can be stopped or migrated | Not a delete — S3 data is preserved |
| Clean Cache | Read-through Foyer HybridCache (memory + SSD tiers) for blocks fetched from S3 | Not the write cache (dirty blocks on local SSD) |
| Circuit Breaker | Lock-free S3 failure detector that fast-fails requests during outages | Not a retry mechanism — it prevents retries |
| Pack Registry | Per-export append-only list of pack IDs created by or inherited by that export | Not a pack index — no hash mappings, just UUIDs for GC enumeration |
| Hot Set | List of non-zero chunk indices for a base image, used to prefetch blocks on fork boot | Not a cache — it's a prefetch hint |
| Bless | CLI command that converts a raw disk image into a content-addressed base image in S3 | Not a runtime operation — offline image preparation |

## S3 Object Layout

```
{db_path}/
├── nbd/{export_name}/
│   ├── export.json                          ← Export definition (name, size_gb, s3_prefix)
│   ├── manifests/
│   │   ├── {export_name}                    ← Full manifest snapshot (GLDE format, base)
│   │   ├── {export_name}.delta              ← Delta manifest since last base (optional)
│   │   └── bases/
│   │       ├── {image_name}                 ← Base image manifest (from bless)
│   │       └── {image_name}.hot-set         ← Boot hot set (chunk indices)
│   ├── packs/
│   │   └── {first-2-hex-of-uuid}/{uuid}     ← Content-addressed packs (GLPK format)
│   └── pack-registries/
│       └── {export_name}                    ← Pack ID list for GC (GLPR format)
```

Packs use 256-way prefix sharding (`packs/ab/{uuid}`) to avoid S3 LIST performance degradation. Manifests are atomic overwrites — the latest is always consistent. The `.delta` file, when present, contains only blocks changed since the last full manifest (see [Delta Manifests](#delta-manifests)). Pack registries are append-only and compacted by GC.

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
- **Integrity verification**: Read path verifies hash after S3 fetch and LZ4 decompression. Optional background scrubber can re-hash cached blocks to detect bit rot.
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

Self-describing S3 object. Up to 100 LZ4-compressed blocks with a content-addressed index.

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

Binary snapshot of export state. Sparse: only written chunks are stored. CRC32 trailer for integrity. Version 2 (current); version 1 still accepted on read for backward compatibility.

#### Full Manifest (flags = 0x0000)

```
┌─────────────────────────── Manifest ────────────────────────┐
│ Header (46 + name_len bytes)                                │
│   magic: "GLDE"  version: 2  flags: 0x0000  name_len        │
│   sequence  chunk_size  device_size                         │
│   block_map_count  pack_index_count                         │
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

S3 key: `manifests/{name}`. Atomic overwrite — the latest full manifest is always a consistent snapshot.

#### Delta Manifest (flags = 0x0001)

Contains only blocks changed since the last full manifest. See [Delta Manifests](#delta-manifests).

```
┌──────────────────────── Delta Manifest ─────────────────────┐
│ Header (46 + name_len bytes)                                │
│   magic: "GLDE"  version: 2  flags: 0x0001  name_len        │
│   sequence  chunk_size  device_size                         │
│   block_map_count (= upsert count)                          │
│   pack_index_count (= new pack count)                       │
│   name (variable length)                                    │
├─────────────────────────────────────────────────────────────┤
│ Delta-specific (16 bytes)                                   │
│   base_sequence: u64 LE                                     │
│   deleted_count: u64 LE                                     │
├─────────────────────────────────────────────────────────────┤
│ Upserted Blocks (25 bytes × block_map_count)                │
│   [chunk_index:u64][hash:16][flags:u8]                      │
├─────────────────────────────────────────────────────────────┤
│ Deleted Chunk Indices (8 bytes × deleted_count)             │
│   [chunk_index:u64]                                         │
├─────────────────────────────────────────────────────────────┤
│ New Pack Entries (40 bytes × pack_index_count)              │
│   [hash:16][pack_id:16][offset:u32][comp_length:u32]        │
├─────────────────────────────────────────────────────────────┤
│ Trailing CRC32 (4 bytes)                                    │
└─────────────────────────────────────────────────────────────┘
```

S3 key: `manifests/{name}.delta`. Replaced on each sync cycle; deleted on compaction. (`manifest.rs`)

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
- Rate-limited: `scrubber_blocks_per_second` (default 0 = disabled, set e.g. 1000 to enable)
- 60s sleep between full passes
- Prometheus counters: `blocks_checked`, `blocks_evicted`

(`scrubber.rs`)

### Sequential Readahead

Ring buffer (4 entries) tracks recent chunk accesses. When 3+ consecutive chunks are read (boot, large file copy), triggers prefetch of the next pack's first chunk. Deduplicates triggers per pack boundary.

This hides S3 latency for sequential workloads — the next pack is already being fetched while the current one is being served. (`readahead.rs`)

### Boot Hot Set Prefetch

When a fork is created from a base image manifest, the router checks S3 for a corresponding `.hot-set` file — a list of chunk indices that contain non-zero data. If found, a background task prefetches those chunks into the CleanCache before the VM reads them, hiding S3 latency during boot.

The hot set is created by `glidefs bless` and covers every non-zero block in the base image. For a typical 2GB Ubuntu image on a 10GB device, the hot set is ~16K chunk indices (~128KB file). (`router.rs:create_export`, `manifest.rs:serialize_hot_set`)

### Garbage Collection (`glidefs gc`)

Orphaned packs accumulate in S3 when exports are deleted or blocks are overwritten and flushed. The GC command reconciles pack registries (what packs exist) against manifests (what packs are live) and deletes the difference.

```
For each S3 prefix:
    1. List all manifests (full + delta) → extract live pack IDs
       (conservative union: packs from base + packs from delta)
    2. List all pack registries → extract known pack IDs
    3. dead = known - live
    4. Mark newly dead packs with timestamp in GC state file
    5. Revive packs that reappeared in live set
    6. Delete packs dead longer than grace period
    7. Compact registries (remove deleted pack IDs)
    8. Delete empty registries for exports with no manifest
```

**Grace period**: Dead packs are not deleted immediately. GC records the first-seen-dead timestamp in a local JSON state file (`gc-state.json`). Packs are only eligible for deletion after the grace period (default 24h). This prevents races where a flush creates a pack and uploads it to a registry, but the manifest hasn't been uploaded yet — without the grace period, GC would see the pack as dead and delete it.

**Safety controls**: `--dry-run` reports without deleting. `--max-deletes` caps deletions per run. Corrupt manifests are skipped (not fatal). (`cli/gc.rs`)

#### Pack Registry Format (`GLPR`)

Per-export append-only list of pack UUIDs, stored in S3 at `pack-registries/{name}`.

```
┌──────────────── Pack Registry ─────────────────┐
│ Header (8 bytes)                                │
│   magic: "GLPR"  count: u32 LE                  │
├─────────────────────────────────────────────────┤
│ Pack IDs (16 bytes × count)                     │
│   [uuid:16][uuid:16]...                         │
└─────────────────────────────────────────────────┘
```

Written by the flush path after uploading packs. Inherited by forks (the fork's registry starts with the parent's pack IDs). Compacted by GC after dead packs are deleted. (`pack_registry.rs`)

## Data Integrity

Every layer has a verification mechanism. The goal: corruption is detected before it reaches the guest or S3.

### Verification Chain

| Layer | What's Protected | Hash/Check | When Verified | On Failure |
|-------|-----------------|------------|---------------|------------|
| S3 packs | Block data in transit/at rest | BLAKE3-128 | Read path: after S3 fetch + LZ4 decompress | `HashMismatch` error → re-fetch from S3 |
| Clean cache (Foyer) | Cached blocks on SSD/memory | BLAKE3-128 | Background scrubber re-hashes against content address | Evict from cache → next read re-fetches from S3 |
| Manifest | Block map + pack index snapshot | CRC32 trailer | On deserialization (load from S3) | Reject manifest, return error |
| WAL entries | Per-entry metadata | CRC32 trailer | On replay (crash recovery) | Stop replay at first corrupt entry, discard torn tail |
| Dirty blocks (SSD) | Block data between write and flush | CRC32 in `HashEntry` | Flush time: before BLAKE3 computation | Skip block (stays dirty), do NOT launder to S3 |

### Dirty Block CRC32

Dirty blocks sit on local SSD between guest writes and S3 flush — up to ~100 blocks per export (pack-size trigger). During this window, SSD bit rot or firmware bugs could silently corrupt the data. Without verification, the flush path would compute BLAKE3 over corrupted data, producing a valid-looking but wrong hash, and upload it to S3 — permanently laundering the corruption.

The `crc32` field in `HashEntry` (repurposed from `_pad`) catches this:

```
Write path (~5µs):
    pwrite(data) → block_map_set(ZERO, seq) → clear_crc32(0)
                                                     │
                                              AtomicU32 store, negligible

Checkpoint (background, every ~5s):
    for each dirty block where crc32 == 0:
        seq_before = block_map_get(idx).seq
        data = pread(block from SSD)
        seq_after = block_map_get(idx).seq
        if seq_before != seq_after → skip (concurrent write)
        crc32 = crc32fast::hash(data)
        CAS(0, crc32)                        ← only store if still 0

Flush (background):
    for each dirty block:
        data = pread(block from SSD)
        stored_crc = get_crc32(idx)
        if stored_crc != 0:
            computed_crc = crc32fast::hash(data)
            if computed_crc != stored_crc:
                if seq changed → concurrent write, skip (CAS failure)
                else → SSD corruption detected, skip block
        hash = blake3_128(data)              ← only reached if CRC32 passes
        ... pack, upload, clear dirty
```

Each dirty block gets CRC32 computed **once** (at first checkpoint after write) and verified **once** (at flush before BLAKE3). No redundant hashing. Not on the read or write hot paths.

### What Is NOT Verified

| Gap | Why Acceptable |
|-----|---------------|
| Dirty block reads (guest reads dirty data from SSD) | Read path returns raw pread data — no checksum. The guest sees whatever is on disk. If SSD corrupts a dirty block and the guest reads it before checkpoint, the guest gets corrupt data. Mitigation: checkpoint runs every ~5s, so the window is small. Adding CRC32 verification on the read path would cause false positives from concurrent write races (write changes data between CRC32 compute and pread). |
| SSD data file between flush cycles | Once a block is flushed (Dirty → Clean), its CRC32 is cleared. The block remains on SSD but is only served as a fallback — reads prefer the clean cache or S3. If the SSD corrupts a clean block, the scrubber catches it in the clean cache; if the block isn't in the clean cache, the next read fetches from S3. |
| redb pack index | Derived data — rebuildable from S3 manifests. If corrupted, `rebuild()` repopulates from scratch. |

## CLI Commands

| Command | Purpose |
|---------|---------|
| `glidefs init [path]` | Generate a default `glidefs.toml` config file |
| `glidefs run -c glidefs.toml` | Start the NBD server with HTTP management API |
| `glidefs bless --image disk.raw --name ubuntu-22.04 -c glidefs.toml` | Convert a raw disk image into a content-addressed base image in S3 |
| `glidefs gc -c glidefs.toml [--dry-run] [--grace-period 24h]` | Delete orphaned packs in S3 |

### Bless Pipeline

`glidefs bless` reads a raw disk image sequentially, hashes each chunk, deduplicates against existing base manifests in S3, compresses unique blocks into packs, and uploads them. Output: a manifest at `manifests/bases/{name}` and a hot set at `manifests/bases/{name}.hot-set`.

Cross-image dedup: if blessing `ubuntu-22.04-node20-v3` after `ubuntu-22.04-node20-v2`, shared chunks (kernel, base packages) are detected via the pack index and skipped — only delta blocks are uploaded. (`cli/bless.rs`)

## Management API

HTTP REST API for orchestrators (scale-to-zero, live migration). (`api.rs`)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/exports/{name}` | `PUT` | Create or resize export (idempotent) |
| `/api/exports/{name}` | `GET` | Get export info (size, readonly) |
| `/api/exports/{name}` | `DELETE` | Remove export (after drain) |
| `/api/exports` | `GET` | List all exports |
| `/api/exports/{name}/snapshot` | `POST` | Drain dirty blocks + upload manifest |
| `/api/exports/{name}/promote` | `POST` | Toggle readonly flag |
| `/api/exports/{name}/metrics` | `GET` | Per-export metrics snapshot (JSON) |
| `/metrics` | `GET` | Prometheus scrape endpoint (all exports) |

### Export Persistence & Discovery

Export definitions are saved to S3 as `{db_path}/nbd/{name}/export.json` by the API and static config paths (not on the recovery path — discovered exports skip the redundant S3 PUT). On startup, `discover_exports()` lists all `export.json` files under the `nbd/` prefix and loads them 32-wide parallel, then `create_export()` recovers each from local WAL + redb 16-wide parallel. No S3 writes on the recovery path. This enables both stateless restarts (new node from S3) and fast binary upgrades (same node, local state intact — 2000 exports in ~6s). (`router.rs:save_export`, `router.rs:discover_exports`, `cli/server.rs`)

### Storage Compatibility

On startup, GlideFS verifies that the S3-compatible backend supports conditional writes (`PutMode::Create`). This is required for future fencing support (preventing split-brain when two nodes claim the same export). Backends that don't support this (some MinIO versions, some S3-compatible proxies) are rejected with a clear error. (`storage_compatibility.rs`)

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

S3 PUT latency is 50-200ms. Write-through would make snapshots take 5-15 seconds instead of <100ms.

Write-behind trades durability for latency: data between the last FLUSH and the next S3 sync is at risk if the host dies. This is acceptable because:

1. Pack-size flush keeps the dirty window small (flush triggers at 100 dirty blocks = ~12.8MB per export)
2. SIGTERM triggers a drain before exit
3. The workload (microVMs) is ephemeral — VMs can be recreated from base images
4. Max dirty data node-wide is bounded: 99 blocks × 128KB × 2000 exports ≈ 25GB

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

### Why redb for the HostPackIndex?

The `HostPackIndex` maps BLAKE3 hash → S3 pack location. It's the cross-export dedup index — shared across all exports on a host. Every `get()` is followed by a 5-50ms S3 fetch, so microsecond lookup latency from an embedded database is fine.

Previously this was an in-memory `DashMap` that grew unbounded with total unique blocks across all VMs. For 5,000 VMs with 100K unique blocks each, that's 500M entries × ~56 bytes = ~28GB of RAM just for the dedup index. Moving to redb puts this on disk where it belongs.

**Why redb specifically**: (1) already in our stack elsewhere, (2) embedded — no server to manage, (3) supports concurrent read transactions with single writer, (4) mmap-based — OS page cache handles hot entries transparently.

**Durability**: `Durability::None` — the pack index is derived data, rebuildable from S3 manifests via `rebuild()`. No fsync overhead.

**Batch inserts**: `insert_batch()` groups entries into a single write transaction. Flush, fork, and bless all produce entries in batches — one transaction per batch instead of per-entry.

**On cold start**: the redb file persists at `{cache_dir}/pack_index.redb`. If present, entries survive restart — no need to rebuild from manifests. If the file is deleted or corrupted, the index starts empty and repopulates lazily as the flush scheduler runs. The read path has a fallback chain that handles missing entries:

```
block_map_get(hash)
  ├─ ZERO + present     → SSD pread (deferred dirty block)
  ├─ ZERO + not present → return zeros
  ├─ zero_block_hash    → return zeros
  ├─ clean_cache hit    → return cached
  ├─ pack_index hit     → S3 range GET  ← redb lookup (~µs)
  └─ SSD pread fallback → local block   ← catches everything else
```

After flush, blocks transition `DIRTY → CLEAN` but **remain on the SSD data file** — blocks are never evicted from their slot. So CLEAN blocks with known hashes but no pack_index entry fall through to Tier 4 (SSD pread), which is actually *faster* (~µs) than an S3 fetch (~50ms).

**Required for fork correctness**: forked exports have blocks that exist only in S3 (inherited from the parent). These blocks have no local SSD data — the pread fallback would return zeros, causing silent data corruption. The pack index entry is the only path to the S3 pack. This is why `prune_unreferenced()` must be careful: it only prunes entries not referenced by any active export's manifest.

Sequence numbers replace hashes as the race detection token in flush. There is a narrow TOCTOU window between checking the sequence and doing the CAS (nanoseconds), identical to the previous hash-based approach and self-healing.

### Why content-addressed packs instead of per-block S3 objects?

Per-block storage means one S3 PUT per 128KB write. At 28K IOPS, that's 28K PUTs/second — prohibitively expensive ($0.14/hour in S3 API costs alone).

Packing up to 100 blocks per S3 object reduces PUTs by up to 100x. Content addressing (hash as identity) enables cross-export deduplication on the same host without coordination.

Trade-off: read amplification. A cache miss fetches the entire pack (up to ~12.8MB) even if only one block (128KB) is needed. The 99.67% cache hit rate makes this acceptable — misses are rare, and the extra data often prefills the cache for subsequent reads.

### Why BLAKE3-128 instead of full BLAKE3-256?

128-bit collision resistance is sufficient for content deduplication (birthday bound: 2^64 operations). 16 bytes fits in two `AtomicU64`s for lock-free storage in the block map. Halves per-entry metadata cost vs full 256-bit hash.

### Why 128KB block size?

Matches ZFS default recordsize. Analysis in `BLOCK_SIZE_ANALYSIS.md` shows 128KB is the sweet spot: smaller blocks (16-32KB) reduce write amplification for random I/O but increase metadata overhead and S3 API costs. Larger blocks (256KB+) improve sequential throughput but waste bandwidth for small random writes.

### Why typestate instead of runtime state checks?

Compile-time prevention of invalid operations. You literally cannot call `write()` on a `WriteCache<Recovering>` — the method doesn't exist for that type parameter. No runtime cost, no forgotten state checks.

### Why a lock-free circuit breaker?

S3 outages shouldn't cascade into mutex contention on the hot path. All circuit breaker state is packed into a single `AtomicU64` — no locks, no multi-variable coordination. CAS loops guarantee consistent state transitions even under high concurrency.

We use a consecutive-failure policy (not windowed) by default because S3 outages tend to be total, not partial.

### Why sparse page tables instead of dense arrays?

Dense arrays pre-allocate for all blocks: a 1TB export with 128KB blocks has 8M entries. `AtomicBlockMap` alone cost ~224MB per export — at 2,000 VMs per compute node, that's 448GB just for hash metadata. Impossible.

Sparse page tables allocate on first write. The directory (one pointer per page) costs ~530KB. Each 4KB page covers 128 hash entries or 4096 state entries. An empty export: ~530KB. A 1%-written export: ~5MB. The cost is one extra pointer dereference on the hot path — a branch that predicts correctly almost every time and is noise next to the SSD pwrite that follows it.

State is kept in a separate `SparseStateMap` rather than co-located in `HashEntry`. State transitions (`set_present`, `transition_to_dirty`) are direct CAS on `AtomicU8` — fully lock-free. The `AtomicBlockMap` is behind a `RwLock<BlockMapKind>` for fork-overlay swaps, so co-locating state there would force every state transition through a read lock. Two separate sparse structures keep state transitions lock-free while achieving the same memory savings.

### Why SeqLock instead of RwLock for the block map?

Each block's metadata (hash + sequence) spans multiple `AtomicU64`s. A torn read (seeing half-old, half-new) would produce a wrong hash. SeqLock solves this with per-entry version counters — readers spin-retry if the version changed during read. Cost: near-zero, because writes to a specific chunk are rare relative to reads, and each chunk has its own version counter.

We considered `RwLock` but rejected it: even uncontended lock/unlock has ~25ns overhead per operation. At 28K IOPS with multi-chunk reads, that's significant. SeqLock adds ~2ns on the reader fast path.

**C11 memory ordering**: The writer stores data fields with `Release` ordering, not `Relaxed`. Under the C11 model (which matters on ARM/Graviton), a `Relaxed` data store can be observed by a reader via a `Relaxed` load without establishing any happens-before relationship — the reader's subsequent `Relaxed` v2 load might miss the writer's version change entirely, producing a torn read. With `Release` data stores, the reader's `Acquire` fence (between data loads and v2 load) synchronizes-with the observed Release, making the writer's odd version visible to v2 and forcing a retry. Loom tests exhaustively verify this property. SeqLock is single-writer by design; concurrent writers break the version parity invariant.

### S3 Concurrency Limits

With 5,000 VMs on a single host, unbounded S3 concurrency is a problem. Each export's flush can upload multiple packs concurrently, and each cache miss triggers an S3 GET. In the worst case: 5,000 VMs × 25 packs = 125K concurrent uploads, or 5,000 × 256 in-flight NBD reads = 1.28M concurrent GETs.

Two host-level `tokio::Semaphore`s bound this — one for uploads (background flush), one for downloads (read path). The semaphores live on `ExportRouter` and are shared (via `Arc`) to every `ContentStore` instance. This is a global gate, not per-export.

| Semaphore | Default | Rationale |
|-----------|---------|-----------|
| `max_s3_uploads` | 128 | Background flush is not latency-sensitive; caps inflight PUTs |
| `max_s3_downloads` | 512 | Read path is latency-sensitive (NBD client waiting); higher limit |

Set to 0 for unlimited (tests use this). Permit is acquired before the S3 call and held for the duration of the request. For streaming downloads (`get_pack_stream`), the permit is held until the stream is fully consumed.

(`content_store.rs`, `router.rs`, `config.rs`)

### SSD Backpressure

With 2000 sparse cache files on a 100GB SSD, physical space can be exhausted. Two mechanisms provide SSD backpressure:

**1. Write rejection at 95%**: The NBD write handler checks SSD utilization before each write. If SSD > 95% and the write targets blocks not yet present on SSD (would allocate new space), it rejects with `ENOSPC`. Overwrites to already-present blocks are allowed — a VM doing in-place database updates keeps working. (`handler.rs:WRITE_REJECT_THRESHOLD`)

**2. Pressure flush**: A background capacity monitor polls `statvfs` every 5 seconds and takes escalating action:

| SSD Utilization | Action |
|---|---|
| < 80% | Normal — no intervention |
| ≥ 80% | Warn — log + `glidefs_ssd_utilization_ratio` gauge for alerting |
| ≥ 90% | Escalate — pressure-flush the 8 dirtiest exports to S3 |
| ≥ 95% | Reject — new-block writes return ENOSPC (per-write check in handler) |
| < 80% (recovery) | Normal — pressure resolved |

The pressure flush directly flushes dirty packs from the exports with the most dirty blocks, prioritizing data that frees the most SSD space.

**Why not hole-punch clean blocks?** `fallocate(PUNCH_HOLE)` on CLEAN block regions would reclaim physical SSD space, but it races with `pwrite` at the kernel level. A concurrent guest write could land data via pwrite, then the punch deallocates it — silent data corruption. No userspace CAS ordering prevents this because both are kernel syscalls on the same inode. The pressure flush is the real backpressure: it converts dirty blocks to clean blocks faster, and clean blocks don't contribute to ENOSPC pressure.

## Package Structure

| File | Purpose |
|------|---------|
| `nbd/server.rs` | TCP/Unix socket listener, NBD protocol negotiation, concurrent request dispatch |
| `nbd/router.rs` | Multi-tenant export manager: create, delete, drain (concurrent, 16-wide), promote, resize |
| `nbd/handler.rs` | NBD command dispatch (read/write/flush) with SSD write rejection at 95% |
| `nbd/write_cache/mod.rs` | `WriteCache<S>` typestate wrapper, `FlushStats`, `SnapshotResult` |
| `nbd/write_cache/inner.rs` | `CacheInner`: shared state, `SyncFile`, `SparseStateMap` integration, metadata persistence |
| `nbd/write_cache/write.rs` | Write path: pwrite + ZERO placeholder + WAL (deferred hash) |
| `nbd/write_cache/read.rs` | Read path: tiered cache resolution (CleanCache → S3 → SSD fallback) |
| `nbd/write_cache/flush.rs` | Dirty block scan, CRC32 checkpoint compute + flush verify (spawn_blocking), pack assembly, S3 upload, manifest sync |
| `nbd/write_cache/init.rs` | Cache file creation, pre-allocation, metadata loading |
| `nbd/write_cache/recovery.rs` | WAL replay, dirty block verification after crash |
| `nbd/write_cache/config.rs` | `WriteCacheConfig` with per-export overrides |
| `nbd/write_cache/error.rs` | `CacheError` type |
| `nbd/block_map.rs` | `Blake3Hash`, `AtomicBlockMap` (sparse page-table + SeqLock + per-entry CRC32), `SparseStateMap`, `SequenceNumber`, LZ4 helpers |
| `nbd/state.rs` | `BlockState` enum + sealed typestate markers (`Initializing`, `Active`, etc.) |
| `nbd/pack.rs` | Pack wire format (GLPK): assemble, parse, extract blocks |
| `nbd/pack_index.rs` | `HostPackIndex`: redb-backed `Blake3Hash → PackLocation` index for cross-export dedup |
| `nbd/pack_registry.rs` | Per-export pack ID tracking for garbage collection |
| `nbd/capacity_monitor.rs` | SSD capacity monitor: `statvfs` polling, pressure flush on dirtiest exports |
| `nbd/content_store.rs` | S3 PUT/GET for packs and manifests via `object_store` crate |
| `nbd/manifest.rs` | Binary manifest serialization/deserialization (GLDE format) |
| `nbd/flush_scheduler.rs` | Event-driven pack flush (Notify) + periodic WAL checkpoint (5s) |
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
| `storage_compatibility.rs` | S3 conditional write check (`PutMode::Create`) for fencing support |
| `parse_object_store.rs` | Vendored URL → ObjectStore factory (S3, GCS, Azure, local, memory) |
| `task.rs` | Named Tokio task spawning helpers for debuggability |
| `deku_bytes.rs` | `Bytes` adapter for deku binary protocol parsing (NBD wire format) |
| `cli/server.rs` | `glidefs run`: wire up config → router → server → API |
| `cli/bless.rs` | `glidefs bless`: create golden images from local directories |
| `cli/gc.rs` | `glidefs gc`: orphaned pack garbage collection with grace period |
| `loom-tests/src/lib.rs` | Exhaustive concurrency tests for lock-free CAS state machine |

## Configuration

```toml
# glidefs.toml
[cache]
dir = "/var/cache/glidefs"
disk_size_gb = 100.0
memory_size_gb = 1.0          # Foyer memory tier
ssd_cache_size_gb = 10.0      # Foyer SSD tier

[storage]
url = "s3://my-bucket/vms"    # Also: gs://, az://, file://, memory://

[servers.nbd]
unix_socket = "/var/run/glidefs.sock"
api_address = "127.0.0.1:8080"
max_s3_uploads = 128             # Global S3 PUT concurrency (0 = unlimited)
max_s3_downloads = 512            # Global S3 GET concurrency (0 = unlimited)

[[servers.nbd.exports]]
name = "vm-prod-1"
size_gb = 100.0

# Cloud credentials (all values support ${ENV_VAR} expansion)
[aws]
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
region = "us-east-1"
```

Supported storage backends: **Amazon S3** (`s3://`), **Google Cloud Storage** (`gs://`), **Azure Blob** (`az://`, `abfs://`), **local filesystem** (`file://`), **in-memory** (`memory://`). All config values support `${ENV_VAR}` expansion via `shellexpand`. (`config.rs`, `parse_object_store.rs`)

| Variable | Default | Why |
|----------|---------|-----|
| `block_size` | 128KB | Matches ZFS recordsize |
| `scrubber_blocks_per_second` | 0 | Background integrity rate (disabled by default); set to e.g. 1000 to enable |
| `memory_size_gb` | 1.0 | Foyer in-memory cache for hot blocks |
| `ssd_cache_size_gb` | 10.0 | Foyer SSD tier catches memory evictions |
| `connect_timeout_secs` | 10 | S3 connection timeout |
| `request_timeout_secs` | 300 | S3 request timeout (large packs take time) |
| `shutdown_timeout_secs` | 30 | Grace period for drain on SIGTERM |
| `max_s3_uploads` | 128 | Global S3 PUT concurrency limit (0 = unlimited) |
| `max_s3_downloads` | 512 | Global S3 GET concurrency limit (0 = unlimited) |
| `wal_sync` | false | fsync WAL per batch; true = slower but crash-safe metadata |

## Flush Scheduling

One unified policy for all exports — no modes, no configuration:

- **Pack-size trigger**: When an export accumulates `BLOCKS_PER_PACK` (100) dirty blocks, the write path notifies the flush scheduler via `tokio::sync::Notify`. The scheduler wakes, flushes dirty blocks as content-addressed packs to S3, syncs the manifest (delta or full), and checkpoints. Event-driven, not polled.
- **Local checkpoint** (5s): Periodic WAL truncation + block state persistence. No S3 involvement.
- **Manifest sync**: After every successful pack upload, the scheduler syncs the manifest so flushed packs are immediately discoverable on cross-host recovery (host death without drain). Uses delta manifests when possible to minimize S3 bandwidth.

The dirty block counter only increments on `Clean→Dirty` transitions, so rewriting the same block 100 times counts as 1 dirty block — natural write coalescing.

At 2K microVMs per node: max ~25GB dirty data node-wide (99 blocks × 128KB × 2000 exports). (`flush_scheduler.rs`, `handler.rs:check_flush_threshold`)

## Memory Overhead

Both `AtomicBlockMap` and `SparseStateMap` use sparse page tables — pages are allocated on first write, not upfront. An empty export costs only its directory arrays (~530KB). Memory grows proportionally to blocks actually written.

Previously, three dense arrays were pre-allocated for all 8M blocks regardless of usage: `AtomicBlockMap` (4 parallel arrays of AtomicU64/AtomicU32 = ~224MB), `block_states` (1 byte × 8M = 8MB), and `present_chunks` (1 bit × 8M = 1MB) — **~233MB per export**. At 2,000 VMs: 466GB. The sparse page tables replace all three with allocate-on-write directories: **~530KB per empty export** — a 450× reduction.

```
AtomicBlockMap (hash + sequence storage)
├── directory: Box<[AtomicPtr<HashPage>]>    512 KB  (65536 entries for 8M blocks)
│   ├── [0] → HashPage { entries: [HashEntry; 128] }  4096 bytes
│   ├── [1] → null  (unwritten — zero cost)
│   └── ...
│   HashEntry: 32 bytes (#[repr(C)]): version(u32) + crc32(u32) + hash_lo(u64) + hash_hi(u64) + seq(u64)

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
| `HostPackIndex` | — | — | ~40 bytes/unique hash (on disk) | redb, disk-resident, pruned on export removal |
| `CleanCache` (memory) | — | — | `memory_size_gb` | Configured, default 1GB |
| `CleanCache` (SSD) | — | — | `ssd_cache_size_gb` | Configured, default 10GB |

| Scenario (1TB/128KB = 8M blocks) | Memory |
|---|---|
| Empty export | ~530 KB |
| 1% written (84K blocks) | ~5 MB |
| 100% written | ~260 MB |
| 2,000 empty exports | ~1 GB |
| 2,000 exports, 1% written | ~10 GB |

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
| Local SSD full | `ENOSPC` to guest (NBD_ENOSPC, not EIO) | Write handler rejects new-block writes at >95% SSD utilization; capacity monitor pressure-flushes dirtiest exports to S3, freeing physical space. Overwrites to already-present blocks are still allowed. |
| SSD failure | Same as host death — writes since last manifest sync are lost | Recreate from last S3 manifest; `rebuild()` repopulates pack index from manifest's pack index section. Packs from un-synced flushes are orphaned (GC cleans up after grace period). |

## Testing

| Suite | Command | Count | What It Covers |
|-------|---------|-------|----------------|
| Unit | `cargo test --features test-utils --lib` | ~327 | Lock-free atomics, wire format round-trips, state transitions, sparse page tables, CRC32 integrity, delta manifest serde |
| Integration | `cargo test --features test-utils --test integration` | ~55 | Crash recovery, concurrent writes, flush consistency, delta manifests (no Docker) |
| Docker | `cargo test --features docker-tests --test docker_integration` | ~20 | Real S3 via MinIO (testcontainers-rs), end-to-end pack upload/download |
| Loom | `cd loom-tests && cargo test --release` | — | Exhaustive interleaving of lock-free algorithms (AtomicBlockMap, SeqLock) |
