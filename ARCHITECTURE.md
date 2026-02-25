# GlideFS Architecture

High-performance block storage server that turns S3 into fast block storage for microVMs, using local SSD as a write-behind cache. Transport-agnostic: NBD (default, cross-platform) and ublk (Linux 6.0+, io_uring-based, opt-in via `--features ublk`).

## Data Flow

### Write Path (~5µs)

```
Guest VM
    │ WRITE (NBD or ublk)
    ▼
Transport ──► ExportRouter ──► BlockHandler ──► WriteCache<Active>
                                                        │
                                            ┌───────────┼───────────┐
                                            ▼           ▼           ▼
                                       set_present   pwrite()   transition_to_dirty
                                      (SparseStateMap)(local SSD) (CAS on SparseStateMap)
                                                        │
                                                   WAL append(block_index, seq)
                                                        │
                                                    return OK     ◄── ~5µs
```

Hash computation is **deferred to flush time**. The write path does zero hash or CRC work —
it only claims the blocks (set_present), writes data, marks them dirty, and appends to the WAL.
BLAKE3 is computed at flush time when the block is read from SSD anyway.

### Read Path (tiered, ~100ns to ~300ms)

```
Guest VM
    │ READ (NBD or ublk)
    ▼
WriteCache ──► is_present(block_idx)?
                      │
              ┌── YES ──► SSD pread                          ~5µs  (hot path: dirty or clean-SSD)
              │
              └── NO (not yet written / fork from S3)
                      │
                      ├── VolumeManifest: block_idx → chunk_idx → chunk_hash
                      │   (if chunk_hash missing → block never written → return zeros)
                      │
                      ├── Tier 1: CleanCache memory (Foyer)           ~100ns
                      │           ├─ hit → return
                      │           └─ miss ▼
                      │
                      ├── Tier 2: CleanCache SSD (Foyer)              ~100µs
                      │           ├─ hit → return
                      │           └─ miss ▼
                      │
                      └── Tier 3: S3 chunk fetch                      50-300ms
                                  ├─ ChunkMetaCache: chunk_hash → pack location
                                  │   (memory LRU → SSD flat file → miss → S3 GET .meta)
                                  ├─ ContentStore::get_chunk_block() (S3 range GET, semaphore-gated)
                                  ├─ LZ4 decompress
                                  ├─ Verify BLAKE3 hash
                                  ├─ Insert into CleanCache
                                  └─ return
```

Multi-block reads fan out with `futures::future::try_join_all()`. Sequential access (3+ consecutive blocks) triggers prefetch to hide S3 latency. (`readahead.rs`)

### Background Sync (S3 upload)

```
FlushScheduler (event-driven: Notify from write path when dirty_count ≥ 100)
    │
    ▼
Phase 1 — Claim: CAS DIRTY→SYNCING for each dirty block (atomic snapshot)
    │           └─ Concurrent guest writes CAS SYNCING→DIRTY (blocks re-dirtied)
    ▼
Partition claimed (SYNCING) blocks by volume chunk (block_index / blocks_per_chunk)
    │
    ▼
For each volume chunk (parallel, spawn_blocking for CPU work):
    ├── Load current ChunkMeta from ChunkMetaCache
    │   (memory LRU → SSD flat file → S3 GET if miss)
    ├── Build HashSet<Blake3Hash> of known entries (within-chunk dedup)
    ├── For each SYNCING block in this chunk:
    │   ├── CRC32 verify from crc_map (if available): state==SYNCING → corruption; state==DIRTY → concurrent write
    │   ├── Skip: zero_block_hash entries, blocks already in ChunkMeta, CRC-failed blocks
    │   ├── Read block data from SSD → BLAKE3-128 hash → LZ4 compress
    │   └── Accumulate into pack buffer
    ├── ContentStore::put_chunk_pack() ──► S3 PUT at chunks/{idx:04}/{uuid}.pack
    ├── ChunkMeta::merge(old_entries, new_entries) → new ChunkMeta
    ├── content_hash = BLAKE3-128 of sorted (offset, block_hash) pairs
    ├── ContentStore::put_chunk_meta() ──► S3 PUT at chunks/{idx:04}/{hash}.meta
    ├── Update ChunkMetaCache (memory + SSD)
    └── Phase 3 — Release: CAS SYNCING→CLEAN for each uploaded block
        └─ CAS fails if block was re-dirtied (SYNCING→DIRTY) → stays Dirty for next cycle
    │
    ▼
Atomic commit: PUT VolumeManifest with updated chunk_hashes
Ensures all flushed chunks are discoverable on cross-host recovery.
```

## Concepts & Terminology

| Term | Definition | NOT |
|------|-----------|-----|
| Transport | The kernel-to-userspace block I/O channel: NBD (TCP/Unix socket, cross-platform) or ublk (io_uring, Linux 6.0+) | Not the storage layer — both transports use the same `BlockHandler` |
| BlockHandler | Transport-agnostic I/O handler: read/write/flush/trim/write_zeroes/cache. Used by both NBD and ublk. | Not protocol-specific — knows nothing about NBD or ublk wire formats |
| Export | A virtual block device served over a transport, with its own cache and S3 prefix | Not a filesystem — raw blocks only |
| Block | Fixed-size unit of data (default 128KB to match ZFS recordsize) | Not variable-sized |
| Volume Chunk | 10GB range of blocks (~81,920 blocks of 128KB). The unit of metadata management: each chunk has its own ChunkMeta file in S3. | Not a 128KB block — "chunk" in the S3 layout means 10GB range |
| Pack | S3 object containing up to 100 LZ4-compressed blocks, scoped to one volume chunk. | Not a single block per S3 object; not cross-chunk |
| VolumeManifest | JSON file (~1KB) mapping `chunk_idx → chunk_content_hash`. The root of an export's metadata. Synced to S3 after every flush. | Not the full block index — it only records which chunks have data and their content hash |
| ChunkMeta (GLCM) | Immutable binary file listing every block in a volume chunk: block offset → pack location. Content-addressed: the file name IS its BLAKE3-128 hash. | Not mutable — each flush writes a NEW file with a new hash |
| ChunkMetaCache | Two-tier (memory LRU + SSD flat files) cache of loaded ChunkMeta objects, keyed by chunk_hash. Shared across all exports on a host — forks that share chunks share the same ChunkMeta. | Not per-export — global content-addressed cache |
| Block State Map | Per-block state (NotPresent / Clean / Dirty / Syncing). Lock-free sparse page table with 2-bit packed `AtomicU8` (4 entries per byte, 16,384 entries per 4KB page). | Not the data itself — no hashes stored per-block; hashes are computed at flush time from SSD |
| Drain | Flush all dirty blocks to S3 so the export can be stopped or migrated | Not a delete — S3 data is preserved |
| Clean Cache | Read-through Foyer HybridCache (memory + SSD tiers) for blocks fetched from S3 | Not the write cache (dirty blocks on local SSD) |
| Circuit Breaker | Lock-free S3 failure detector that fast-fails requests during outages | Not a retry mechanism — it prevents retries |
| Hot Set | List of non-zero block indices for a base image, used to prefetch blocks on fork boot | Not a cache — it's a prefetch hint |
| Bless | CLI command that converts a raw disk image into a content-addressed base image in S3 | Not a runtime operation — offline image preparation |
| Snapshot | An explicit, versioned copy of an export's VolumeManifest stored at a stable S3 key. Never overwritten by background syncs. Pinned in S3 until explicitly deleted. | Not the same as a background `sync_manifest` — background syncs update `manifests/{name}` and do NOT create versioned snapshot keys |
| Fork | A new export whose VolumeManifest is copied from a parent's. The fork starts with an empty local SparseStateMap; unwritten blocks resolve through VolumeManifest → ChunkMetaCache → S3. | Not a full copy — the parent's ChunkMeta and pack files are never duplicated |
| Snapshot Sequence | A monotonic counter (`u64`) that increments on every flush. Identifies the point in time at which a snapshot was taken and used to fork from a specific historical state. | Not a timestamp — purely an ordering counter |
| Manifest Tag | A named alias for a VolumeManifest, stored at `manifests/{tag}` within an export's S3 namespace. Created via `snapshot(tag=...)` or `tag_export(tag)`. Forkable by name; used by stateless orchestrators as a content-derived skip key. | Not a snapshot sequence — not versioned or immutable; overwriting the tag name updates the pointer |

## S3 Object Layout

```
{db_path}/
└── exports/{s3_prefix}/                         ← ContentStore root (shared by exports + bless)
    ├── manifests/{export_name}                  ← VolumeManifest JSON (chunk_idx → chunk_hash)
    ├── manifests/{tag_name}                     ← Named manifest tag (same format, arbitrary name)
    ├── manifests/bases/{image_name}             ← Blessed base image VolumeManifest (glidefs bless)
    ├── manifests/bases/{image_name}.hot-set     ← Boot hot set for base image (block indices)
    ├── snapshots/{export_name}/{sequence:020}   ← Versioned VolumeManifest (zero-padded)
    └── chunks/{chunk_idx:04}/
        ├── {hex_chunk_hash}.meta                ← ChunkMeta (GLCM binary, content-addressed)
        └── {uuid}.pack                          ← Block data packs (GLPK binary)
```

Chunk directories use 4-digit zero-padded indices (`chunks/0000/`, `chunks/0001/`, ...). A 1TB device with 128KB blocks has up to 98 volume chunks (10GB each). Manifests are atomic overwrites — the latest is always consistent. ChunkMeta files are immutable and content-addressed — their filename IS their BLAKE3-128 hash, so old files are orphaned (not overwritten) when chunks are updated and reclaimed by GC.

## Core Mechanism: Write-Behind Cache

GlideFS decouples write latency from S3 round-trip time. Writes land on local SSD immediately; a background scheduler uploads dirty blocks to S3 as content-addressed packs.

### Lock-Free Hot Path

The write path avoids all locks. Three techniques make this possible:

1. **Positional I/O** (`pread`/`pwrite`): The `SyncFile` wrapper uses POSIX positional I/O, which is atomic per-syscall and doesn't use the file position pointer. No locking needed for concurrent block reads and writes. (`write_cache/inner.rs:SyncFile`)

2. **Sparse state map** (CAS + sparse page table): `SparseStateMap` stores per-block state in a two-level page table with 2-bit packing — 4 entries per `AtomicU8`, 16,384 entries per 4KB page. Pages are allocated on first write via CAS — empty exports use ~4KB for the directory. State transitions (`set_present`, `transition_to_dirty`, `transition_dirty_to_syncing`, `transition_syncing_to_clean`) are CAS-on-byte loops with no global lock. When a CAS on one 2-bit field fails because an adjacent field in the same byte was modified concurrently, the loop retries — this costs nanoseconds against pwrite latency. (`block_map.rs:SparseStateMap`)

3. **Sequence numbers**: A monotonic `AtomicU64` counter (`SequenceNumber`) provides WAL ordering. Each write bumps the sequence; the max sequence is persisted in `block_states` metadata so it survives crash recovery. (`block_map.rs:SequenceNumber`)

### Content Addressing

Every block is identified by its BLAKE3-128 hash (16 bytes, truncated from 256-bit), computed at flush time (not on the write path). This enables:

- **Within-chunk deduplication**: When flushing, the existing ChunkMeta for each volume chunk provides a `HashSet<Blake3Hash>` of already-stored blocks. Identical blocks are skipped — only unique blocks are packed and uploaded. Ten identical 10GB VMs sharing the same volume chunks share the same .meta files and pack objects.
- **Integrity verification**: Read path verifies hash after S3 fetch and LZ4 decompression. Optional background scrubber can re-hash cached blocks to detect bit rot.
- **Sparse manifests**: VolumeManifest only stores chunks that have been written — a 500GB export with 2GB of data has a ~1KB manifest with ~200 entries.

The well-known hash of a 128KB zero block (`zero_block_hash()`) lets the flush path skip blocks that are all-zeros — they're deduplicated against the well-known sentinel without storage or S3 interaction. (`block_map.rs`)

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

Stored in `SparseStateMap` — a sparse page table with 2-bit packed `AtomicU8` values (4 entries per byte), fully lock-free. Encoding: `NotPresent=0, Clean=1, Dirty=2, Syncing=3`. NotPresent=0 means unallocated pages (and zero bytes within allocated pages) are implicitly "never written" with no memory cost.

```
NotPresent ──[write]──► Dirty ──[flush claim]──► Syncing ──[upload OK + no concurrent write]──► Clean
                           ▲                         │                                              │
                           └──────[write]────────────┘ (CAS SYNCING→DIRTY)                         │
                           └──────────────────────────────────[write]──────────────────────────────┘
```

The flush path uses **SYNCING-based CAS**: a dirty block is atomically claimed (`CAS DIRTY→SYNCING`) before CPU work begins. After upload, `CAS SYNCING→CLEAN` releases it. If a guest write lands during the flush, `transition_to_dirty()` CAS-loops `SYNCING→DIRTY` — the flush's final `CAS SYNCING→CLEAN` then fails, and the block stays Dirty for the next cycle.

| From | Event | To | Encoding | Notes |
|------|-------|----|----------|-------|
| NotPresent | Guest write | Dirty | 0 → 2 | Page allocated on first touch, set_present + SSD pwrite, WAL appended |
| Clean | Guest write | Dirty | 1 → 2 | Block already on SSD, re-dirtied |
| Dirty | Flush claim | Syncing | 2 → 3 | `transition_dirty_to_syncing()` — atomic snapshot of dirty block |
| Syncing | Concurrent guest write | Dirty | 3 → 2 | `transition_to_dirty()` CAS loop; flush's SYNCING→CLEAN will fail |
| Syncing | Upload success, no concurrent write | Clean | 3 → 1 | `transition_syncing_to_clean()` — CAS fails if write re-dirtied |
| Syncing | Upload success, concurrent write | Dirty | — | Final CAS fails; block stays dirty (re-flushed next cycle) |
| Any | Crash recovery load | Dirty | — | `load_metadata()` converts Syncing→Dirty on startup |

Presence is derived: `is_present = state != 0`. This eliminates the separate `present_chunks` bitmap. (`block_map.rs:SparseStateMap`)

## Fork Path

Forking a VM copies the VolumeManifest — a ~1KB JSON blob mapping volume chunk indices to their content hashes. That's 2 S3 operations (GET parent manifest, PUT fork manifest). No block data is copied, no local metadata is duplicated.

```
router.create_export(config, fork_from="parent-vm")
    │
    ├── ContentStore::get_manifest("parent-vm")   ← GET manifests/parent-vm (~1KB)
    ├── ContentStore::put_manifest("fork-vm", manifest_bytes)  ← PUT manifests/fork-vm
    └── WriteCache::open_fresh_active(config)      ← empty SparseStateMap (all NotPresent)
    │
    ▼
Fork is live:
  - Writes: set_present + pwrite to fork's local SSD; flushed to chunks/ as new .meta and .pack files
  - Reads for blocks never written by the fork (is_present = false):
      VolumeManifest lookup (chunk_hash) → ChunkMetaCache → S3 range GET from parent's chunks/{idx}/{uuid}.pack
```

**Content-addressed sharing**: Chunks the fork hasn't modified share their ChunkMeta files with the parent. The ChunkMetaCache is global and keyed by content hash, so if a chunk hash appears in both the parent's and fork's VolumeManifest, it loads and caches once on the host. 180 forks from the same base image load each common chunk's .meta exactly once.

**No in-memory overlay**: Forks don't need `ForkedBlockMap` because reads fall through to S3 via the VolumeManifest — the parent's pack files are still in S3 under their original `chunks/` paths.

### State Map

`SparseStateMap` (NotPresent / Clean / Dirty / Syncing) lives in `CacheInner`. It uses a sparse page-table with 2-bit packed `AtomicU8` entries — 4 states per byte, 16,384 entries per 4KB page. State transitions (`set_present`, `transition_to_dirty`, `transition_dirty_to_syncing`, `transition_syncing_to_clean`) are CAS-on-byte loops — fully lock-free, no `RwLock`. (`block_map.rs:SparseStateMap`)

## Snapshots

A snapshot is an explicit, versioned copy of an export's VolumeManifest stored at a stable S3 key. Unlike background syncs (which continuously overwrite `manifests/{name}`), snapshots accumulate and are never overwritten by the background flush path. The control plane manages their lifecycle via list/delete APIs.

### Snapshot vs Sync

```
background sync_manifest() →  manifests/{name}           ← always the current state, overwritten
explicit snapshot()        →  snapshots/{name}/{seq:020}  ← immutable, accumulates, never touched by sync
```

Zero-padding the sequence to 20 digits (`{seq:020}`) ensures S3 LIST returns results in lexicographic = numeric order without sorting.

### Snapshot Lifecycle

```
Control Plane
    │
    │  POST /api/exports/{name}/snapshot
    ▼
ExportRouter::snapshot_export()
    │
    ▼
WriteCache::snapshot()
    ├── 1. flush_dirty_inner()          flush all dirty blocks → S3 chunks
    ├── 2. upload_volume_manifest()     overwrite manifests/{name} (current state)
    ├── 3. put_snapshot()               write snapshots/{name}/{seq:020}  ← best-effort
    └── 4. checkpoint()                 persist block_states + max_seq, truncate WAL
    │
    ▼ (if tag provided)
put_manifest(tag)                       write manifests/{tag}  ← same bytes, named alias
    │
    ▼
Returns: { sequence, manifest_etag, tag? }
```

Step 3 is best-effort. If the versioned snapshot upload fails, the base manifest (step 2) is already consistent — background flushes continue and the control plane can retry `snapshot()` to get a new versioned key. The sequence returned is the pack-flush sequence, not a separate counter.

### Fork from Snapshot

The control plane forks a VM disk from a specific historical snapshot by specifying `manifest_name` + `snapshot_sequence` on export creation:

```
PUT /api/exports/fork-vm
    { "manifest_name": "prod-vm", "snapshot_sequence": 42, "size_gb": 10 }
    │
    ▼
router.create_export(config, readonly=false, manifest_name=Some("prod-vm"), snapshot_sequence=Some(42))
    │
    ├── content_store.get_snapshot("prod-vm", 42)   ← GET snapshots/prod-vm/00000000000000000042
    ├── VolumeManifest::deserialize()
    ├── ContentStore::put_manifest("fork-vm", manifest_bytes)  ← PUT manifests/fork-vm
    └── WriteCache::open_fresh_active(config)        ← empty local block map
    │
    ▼
Fork is live — reads serve parent data via VolumeManifest → ChunkMetaCache → S3
                writes go to fork's local SSD and new chunks/
```

Omitting `snapshot_sequence` forks from the current effective manifest (`manifests/{name}`), which is the "live state" fork path.

### Snapshot API

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/exports/{name}/snapshot` | `POST` | Flush + upload versioned snapshot. Optional body `{"tag":"name"}` also publishes as named alias. Returns `{sequence, manifest_etag, tag?}`. |
| `/api/exports/{name}/snapshots` | `GET` | List snapshot sequences in ascending order. Returns `[seq, ...]`. |
| `/api/exports/{name}/snapshots/{seq}` | `DELETE` | Delete a specific snapshot (idempotent). |
| `/api/exports/{name}/tag` | `POST` | Publish current manifest under a tag name without re-flushing. Body: `{"tag":"name"}`. |
| `/api/manifests/{s3_prefix}/{name}` | `HEAD` | Check if a named manifest exists (200/404). No data transfer. Does not require a running export. |
| `/api/exports/{name}` | `PUT` | With `manifest_name` + optional `snapshot_sequence`: fork from parent, specific snapshot, or named tag. |

### GC and Snapshot Packs

Packs and .meta files referenced only by snapshots (not by the current manifest) are kept alive by the GC's snapshot scan. GC extends its live-chunk set with every chunk referenced by any snapshot manifest before computing dead files:

```
GC reconcile_prefix():
    1. Fetch manifests/{name}        → live chunk_hashes per chunk_idx
    2. List + fetch snapshots/{name}/* (ALL snapshots) → extend live chunk_hashes
       (if snapshot scan fails → skip entire export, delete nothing)
    3. For each chunk_idx directory:
       a. Identify live .meta files (chunk_hash in live set)
       b. For live .meta files: load → extract pack_ids → live_packs
       c. dead_metas = listed .meta files - live_metas
       d. dead_packs = listed .pack files - live_packs
       e. Delete files dead longer than grace period
```

If the snapshot scan fails (S3 error), GC skips the entire export rather than risking deletion of snapshot-pinned data. (`cli/gc.rs:reconcile_prefix`)

**Deleting a snapshot unpins its exclusive chunks** — .meta and .pack files not referenced by any other snapshot or the current manifest become orphaned and eligible for GC after the grace period.

### Manifest Tags

A manifest tag is a named alias for a VolumeManifest — stored at `manifests/{tag}` within the export's S3 namespace alongside the regular `manifests/{export_name}`. Tags are forkable by name (pass as `manifest_name` on export creation) and support a lightweight `HEAD` existence check.

**Primary use case: stateless skip-ahead for orchestrators.** An orchestrator that has no database computes a content-derived hash:

```
setup_hash = blake3(image_id || setup_command || lockfile_hash)
```

Then:

```
HEAD /api/manifests/{s3_prefix}/setup-{hash}
  → 200: fork from tag directly, skip setup entirely
  → 404: build from base, run setup_command, then:
         POST /api/exports/{name}/snapshot {"tag": "setup-{hash}"}
         → tags the result for future deploys
```

The naming convention is the index. No external database. No drift. The tag lives next to the manifest data in S3.

**Tag vs Snapshot sequence:**

| | Snapshot Sequence | Manifest Tag |
|---|---|---|
| Identity | Monotonic `u64` per export | Arbitrary string |
| Mutable | No — immutable once written | Yes — overwriting the tag updates the alias |
| GC pinning | Yes — GC scans all snapshot keys | No — tags are `manifests/` keys, same as live manifest |
| Use case | Rollback to historical state | Content-derived fork key for stateless orchestrators |

(`router.rs:tag_export`, `router.rs:head_manifest`, `content_store.rs:head_manifest`, `api.rs:POST /tag`, `api.rs:HEAD /manifests`)

### Snapshot Invariants

| Invariant | Mechanism |
|-----------|-----------|
| Background sync never creates snapshot keys | `sync_manifest()` only writes to `manifests/{name}` — snapshot path only reachable via explicit `snapshot()` |
| Snapshots accumulate (never overwritten) | Each snapshot has a unique `{seq:020}` key; GC only deletes via explicit `delete_snapshot()` |
| Fork reads parent blocks | VolumeManifest lookup → ChunkMetaCache → S3 range GET on parent's pack files |
| Snapshot deletion is idempotent | S3 NotFound on delete returns Ok — safe for control plane retry loops |
| Non-purge remove preserves snapshots | `remove_export(name, purge=false)` does not call `delete_all_snapshots()` — snapshots survive export restart |
| Snapshot on empty export succeeds | Returns sequence=0 and an empty VolumeManifest — valid for control plane idempotency |

(`write_cache/flush.rs:snapshot`, `content_store.rs:put_snapshot/list_snapshots/delete_snapshot`, `router.rs:snapshot_export`, `tests/integration/snapshots.rs`)

### Rollback / Restore

There is no in-place rollback primitive. To restore an export to a prior snapshot, the control plane does a **remove-and-refork**:

```
1. POST /api/exports/prod-vm/snapshot          (optional: snapshot current state before rollback)
2. DELETE /api/exports/prod-vm                 (remove WITHOUT purge — snapshots survive in S3)
3. PUT /api/exports/prod-vm                    (fork from target snapshot)
   { "manifest_name": "prod-vm",
     "snapshot_sequence": <target_seq>,
     "size_gb": <original_size> }
```

Step 2 uses the no-purge path so that all historical snapshots remain in S3. The new export in step 3 starts with the target snapshot's VolumeManifest, reading unmodified blocks through ChunkMetaCache → S3. No data is copied.

**Blue/green alternative**: Fork the snapshot to a new name, test it, then cut traffic over:

```
PUT /api/exports/prod-vm-rollback
    { "manifest_name": "prod-vm", "snapshot_sequence": <target_seq>, "size_gb": ... }
→ verify prod-vm-rollback is correct
→ remove prod-vm, rename prod-vm-rollback to prod-vm (or update load balancer)
```

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

Self-describing S3 object. Up to 100 LZ4-compressed blocks with a content-addressed index. Scoped to one volume chunk (`chunks/{chunk_idx:04}/{uuid}.pack`).

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

S3 key: `chunks/{chunk_idx:04}/{uuid}` — per-chunk directories naturally bound list sizes. (`pack.rs`)

### VolumeManifest (JSON)

Lightweight root of an export's metadata. Maps volume chunk indices to their ChunkMeta content hashes. ~1KB for a 1TB device (one entry per 10GB chunk with data).

```json
{
  "version": 3,
  "size": 10737418240,
  "chunk_size": 10737418240,
  "block_size": 131072,
  "chunks": {
    "0": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
    "3": "f7e6d5c4b3a2918071605f4e3d2c1b0a"
  }
}
```

S3 key: `manifests/{export_name}`. Atomic overwrite on every flush — the latest is always a consistent snapshot. Forking = GET parent + PUT fork. (`volume_manifest.rs`)

### ChunkMeta Format (`GLCM`)

Immutable binary index for one 10GB volume chunk. Stores a sorted array of block locations — one entry per block written within the chunk. **Content-addressed**: the S3 filename is the BLAKE3-128 hash of the file's logical content (sorted `(offset, block_hash)` pairs). Identical block mappings produce identical filenames across forks.

```
┌─────────────────────────── ChunkMeta ───────────────────────────┐
│ Header (32 bytes)                                               │
│   magic: "GLCM"  version: 1  chunk_idx: u32  block_count: u32   │
│   chunk_size: u64  block_size: u32  _reserved: u32              │
├─────────────────────────────────────────────────────────────────┤
│ Entries (44 bytes × block_count, sorted by block offset)        │
│   [offset:u32][hash:16][pack_id:16][pack_offset:u32][comp_len:u32] 
├─────────────────────────────────────────────────────────────────┤
│ Trailing CRC32 (4 bytes)                                        │
└─────────────────────────────────────────────────────────────────┘
```

- `offset`: block index within the volume chunk (0–81,919 for 10GB/128KB)
- `hash`: BLAKE3-128 of the uncompressed block
- `pack_id`: UUID of the `.pack` file containing this block
- `pack_offset`: byte offset within the pack's block data region
- `comp_len`: compressed size in the pack

S3 key: `chunks/{chunk_idx:04}/{hex_content_hash}.meta`. Each flush that modifies a chunk writes a new `.meta` file at a new content hash; the old file is orphaned and collected by GC. (`chunk_meta.rs`)

### WAL Entry Format

Append-only on local SSD. Metadata only — block data lives in the cache file.

```
[name_len:u16][name][block_index:u64][sequence:u64][crc32:u32]
```

CRC32 trailer detects torn writes. On recovery, replay stops at the first corrupt entry — the torn tail is discarded, not an error. Hash is not stored in the WAL — recovery re-reads block data from the SSD cache file and recomputes BLAKE3. WAL is truncated after each block map persistence. (`wal.rs`)

## Background Subsystems

### Scrubber (Integrity Verification)

Rate-limited background task that re-hashes blocks in the CleanCache against their content address. On mismatch (bit rot, memory corruption), evicts the block — the next read re-fetches from S3, which is the authoritative source.

The scrubber uses the `HashSource` trait to collect known block hashes without iterating the cache (Foyer doesn't support key enumeration). `RouterHashSource` implements `HashSource` by walking each active export's VolumeManifest → loaded ChunkMeta entries to gather all hashes known to be in S3. Any of those hashes that happen to be present in CleanCache are re-verified.

- Rate-limited: `scrubber_blocks_per_second` (default 0 = disabled, set e.g. 1000 to enable)
- 60s sleep between full passes
- Prometheus counters: `blocks_checked`, `blocks_evicted`

(`scrubber.rs`, `router.rs:collect_block_hashes`)

### Sequential Readahead

Ring buffer (4 entries) tracks recent block accesses. When 3+ consecutive blocks are read (boot, large file copy), triggers prefetch of the next chunk's first block. Deduplicates triggers per pack boundary.

This hides S3 latency for sequential workloads — the next pack is already being fetched while the current one is being served. (`readahead.rs`)

### Boot Hot Set Prefetch

When a fork is created from a base image manifest, the router checks S3 for a corresponding `.hot-set` file — a list of block indices that contain non-zero data. If found, a background task prefetches those blocks into the CleanCache before the VM reads them, hiding S3 latency during boot.

The hot set is created by `glidefs bless` and covers every non-zero block in the base image. For a typical 2GB Ubuntu image on a 10GB device, the hot set is ~16K block indices (~128KB file). (`router.rs:create_export`, `manifest.rs:serialize_hot_set`)

### Garbage Collection (`glidefs gc`)

Orphaned pack files and .meta files accumulate in S3 when exports are deleted, blocks are overwritten, or forks diverge from their parent. The GC command walks the `chunks/` directories, identifies live vs. dead objects by cross-referencing all VolumeManifests (current + snapshots), and deletes the orphans.

```
For each export in S3:
    1.  Fetch manifests/{name}         → live chunk_hashes per chunk_idx
    2.  List + fetch snapshots/{name}/* → extend live chunk_hashes
        (if snapshot scan fails → skip entire export, delete nothing)
    3.  For each chunks/{chunk_idx} directory:
        a.  List all .meta files
        b.  live_metas = .meta files whose name (hash) is in live chunk_hashes
        c.  For each live .meta: fetch + parse → extract pack_ids → live_packs
        d.  dead_metas = all .meta files - live_metas
        e.  dead_packs = all .pack files - live_packs
        f.  Mark newly-dead files with first-seen timestamp in GC state file
        g.  Revive files that reappeared in live set
        h.  Delete files dead longer than grace period
```

**Grace period**: Dead objects are not deleted immediately. GC records the first-seen-dead timestamp in a local JSON state file (`gc-state.json`). Files are only eligible for deletion after the grace period (default 24h). This prevents races where a flush uploads a pack but the manifest hasn't been committed yet — without the grace period, GC would see the pack as dead and delete it.

**Safety controls**: `--dry-run` reports without deleting. `--max-deletes` caps deletions per run. Corrupt manifests are skipped (not fatal). (`cli/gc.rs`)

## Data Integrity

Every layer has a verification mechanism. The goal: corruption is detected before it reaches the guest or S3.

### Verification Chain

| Layer | What's Protected | Hash/Check | When Verified | On Failure |
|-------|-----------------|------------|---------------|------------|
| S3 packs | Block data in transit/at rest | BLAKE3-128 | Read path: after S3 fetch + LZ4 decompress | `HashMismatch` error → re-fetch from S3 |
| Clean cache (Foyer) | Cached blocks on SSD/memory | BLAKE3-128 | Background scrubber re-hashes against content address | Evict from cache → next read re-fetches from S3 |
| VolumeManifest | Chunk metadata root | (JSON, no checksum — content-addressed chain below) | On deserialization | Reject manifest, return error |
| ChunkMeta | Block location index | CRC32 trailer | On deserialization (load from S3 or SSD cache) | Reject ChunkMeta, return error |
| WAL entries | Per-entry metadata | CRC32 trailer | On replay (crash recovery) | Stop replay at first corrupt entry, discard torn tail |
| Dirty blocks (SSD) | Block data between write and flush | CRC32 in `crc_map` | Flush time: before BLAKE3 computation, using block state to discriminate | Skip block (stays dirty), do NOT launder to S3 |

### Dirty Block CRC32

Dirty blocks sit on local SSD between guest writes and S3 flush — up to ~100 blocks per export (pack-size trigger). During this window, SSD bit rot or firmware bugs could silently corrupt the data. Without verification, the flush path would compute BLAKE3 over corrupted data, producing a valid-looking but wrong hash, and upload it to S3 — permanently laundering the corruption.

CRC32 is stored in `crc_map: DashMap<usize, u32>` on `CacheInner` — sized proportional to dirty blocks, not device size. At 10GB/s write rate with 5s flush interval: max ~80K dirty blocks × ~20 bytes (DashMap overhead: bucket metadata + alignment + load factor) = ~1.6MB. At idle: 0 bytes.

**Key difference from the write path**: the write path does NOT touch the CRC map. SYNCING state (not sequence numbers) discriminates corruption from concurrent writes:

```
Write path (~5µs):
    set_present(idx) → pwrite(data) → transition_to_dirty(idx) → WAL append
    ↑ zero CRC map interaction

Checkpoint (background, every ~5s):
    for each dirty block:
        data = pread(block from SSD)
        crc_map.entry(idx).or_insert(crc32fast::hash(data))  ← store only if not already set

Flush (background):
    Phase 1: CAS DIRTY→SYNCING for each dirty block (claims the block)

    Phase 2: for each SYNCING block:
        if let Some(stored_crc) = crc_map.remove(&idx):
            computed_crc = crc32fast::hash(pread(data))
            if computed_crc != stored_crc:
                if state_map.get(idx) != SYNCING:
                    → block was re-dirtied by concurrent write (state is DIRTY now)
                    → skip (not corruption — stale CRC)
                else:
                    → still SYNCING, no concurrent write → real SSD corruption
                    → skip block (stays in Syncing, reverted to Dirty by error handler)
        hash = blake3_128(data)   ← only reached if CRC32 passes
        ... pack, upload

    Phase 3: CAS SYNCING→CLEAN for successfully uploaded blocks
```

Each dirty block gets CRC32 computed **once** (at checkpoint) and verified **once** (at flush before BLAKE3). No CRC work on the write hot path. SYNCING state provides unambiguous discrimination: a mismatch when still SYNCING = corruption; a mismatch when now DIRTY = concurrent write rewrote the block.

### What Is NOT Verified

| Gap | Why Acceptable |
|-----|---------------|
| Dirty block reads (guest reads dirty data from SSD) | Read path returns raw pread data — no checksum. The guest sees whatever is on disk. If SSD corrupts a dirty block and the guest reads it before checkpoint, the guest gets corrupt data. Mitigation: checkpoint runs every ~5s, so the window is small. Adding CRC32 verification on the read path would cause false positives from concurrent write races (write changes data between CRC32 compute and pread). |
| SSD data file between flush cycles | Once a block is flushed (Dirty/Syncing → Clean), its CRC32 is removed from `crc_map`. The block remains on SSD but reads prefer the clean cache or S3. If the SSD corrupts a clean block, the scrubber catches it in the clean cache; if not in cache, the next read fetches from S3. |
| ChunkMetaCache SSD files | Derived data — rebuildable from S3 `.meta` files. If a cached `.meta` file is corrupted, the CRC32 check on deserialization detects it; the cache falls back to fetching from S3. |

## CLI Commands

| Command | Purpose |
|---------|---------|
| `glidefs init [path]` | Generate a default `glidefs.toml` config file |
| `glidefs run -c glidefs.toml` | Start the block server (NBD + optional ublk) with HTTP management API |
| `glidefs bless --image disk.raw --name ubuntu-22.04 --s3-prefix bases -c glidefs.toml` | Convert a raw disk image into a content-addressed base image in S3 |
| `glidefs gc -c glidefs.toml [--dry-run] [--grace-period 24h]` | Delete orphaned packs and .meta files in S3 |

### Bless Pipeline

`glidefs bless` reads a raw disk image sequentially, partitions 128KB blocks by 10GB volume chunk, deduplicates against existing ChunkMeta files in S3, compresses unique blocks into chunk-scoped packs (one pack per volume chunk), uploads them, and builds a VolumeManifest. Output: a VolumeManifest at `exports/{s3_prefix}/manifests/bases/{name}` and a hot set at `exports/{s3_prefix}/manifests/bases/{name}.hot-set`. Bless shares the same ContentStore namespace as runtime exports, so forks from base images work via `manifest_name: "bases/{name}"` with no cross-namespace copying.

Cross-image dedup: if blessing `ubuntu-22.04-node20-v3` after `ubuntu-22.04-node20-v2`, shared volume chunks produce identical ChunkMeta content hashes — the existing `.meta` files are reused and only delta blocks are uploaded. (`cli/bless.rs`)

## Management API

HTTP REST API for orchestrators (scale-to-zero, live migration). (`api.rs`)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/exports/{name}` | `PUT` | Create or resize export (idempotent). With `manifest_name` + optional `snapshot_sequence`: fork from parent or specific snapshot. |
| `/api/exports/{name}` | `GET` | Get export info (size, readonly, transport, device path) |
| `/api/exports/{name}` | `DELETE` | Remove export. `?purge=true` also deletes local cache and all S3 snapshots. |
| `/api/exports` | `GET` | List all active exports |
| `/api/exports/{name}/snapshot` | `POST` | Flush dirty blocks → S3, upload versioned manifest. Optional body `{"tag":"name"}` also publishes named alias. Returns `{sequence, manifest_etag, tag?}`. |
| `/api/exports/{name}/snapshots` | `GET` | List snapshot sequences in ascending order |
| `/api/exports/{name}/snapshots/{seq}` | `DELETE` | Delete a specific snapshot (idempotent) |
| `/api/exports/{name}/tag` | `POST` | Publish current manifest under a named alias without re-flushing. Body: `{"tag":"name"}`. |
| `/api/manifests/{s3_prefix}/{name}` | `HEAD` | Check manifest existence (200/404). No data transfer, no running export required. |
| `/api/exports/{name}/drain` | `POST` | Flush all dirty blocks to S3 (no versioned snapshot) |
| `/api/exports/{name}/promote` | `POST` | Toggle readonly → read-write |
| `/api/exports/{name}/metrics` | `GET` | Per-export metrics snapshot (JSON) |
| `/metrics` | `GET` | Prometheus scrape endpoint (all exports) |

### Export Persistence & Discovery

Export definitions are saved to S3 as `{db_path}/exports/{name}/export.json` by the API and static config paths (not on the recovery path — discovered exports skip the redundant S3 PUT). On startup, `discover_exports()` lists all `export.json` files under the `exports/` prefix and loads them 32-wide parallel, then `create_export()` recovers each from local WAL 16-wide parallel. No S3 writes on the recovery path. This enables both stateless restarts (new node from S3) and fast binary upgrades (same node, local state intact — 2000 exports in ~6s). (`router.rs:save_export`, `router.rs:discover_exports`, `cli/server.rs`)

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
| `glidefs_read_latency_seconds` | Histogram | End-to-end block read latency |
| `glidefs_write_latency_seconds` | Histogram | End-to-end block write latency |
| `glidefs_s3_fetch_latency_seconds` | Histogram | S3 GET latency (cache misses) |
| `glidefs_s3_put_latency_seconds` | Histogram | S3 PUT latency (pack uploads) |
| `glidefs_s3_put_errors_total` | Counter | S3 upload failures |
| `glidefs_s3_get_errors_total` | Counter | S3 fetch failures |
| `glidefs_flush_errors_total` | Counter | Failed flush cycles |
| `glidefs_flush_blocks_cas_failed_total` | Counter | Blocks left dirty per flush due to concurrent writes (flush starvation indicator) |

Histogram buckets: `<100µs`, `<1ms`, `<10ms`, `<100ms`, `<1s`, `>=1s`.

## Design Decisions

### Why NBD + ublk instead of just one transport?

NBD is cross-platform and battle-tested — it works on macOS (dev), Linux (prod), and anywhere with a TCP stack. But it has inherent overhead: socket read/write syscalls per I/O, protocol framing (28-byte headers with magic numbers), and single-connection architecture.

ublk eliminates all of that on Linux 6.0+: io_uring shared memory replaces sockets, fixed mmap'd descriptors replace protocol parsing, and native multi-queue gives per-CPU I/O parallelism. Benchmarks show 2-3x IOPS improvement for random 4K reads.

The storage layer (`BlockHandler`, `WriteCache`, `ContentStore`) is transport-agnostic — both frontends call the same 6 methods. The `BlockHandler` command interface maps 1:1 between transports:

| NBD | ublk | BlockHandler |
|-----|------|---|
| `NBD_CMD_READ` | `UBLK_IO_OP_READ` | `handler.read(offset, length)` |
| `NBD_CMD_WRITE` | `UBLK_IO_OP_WRITE` | `handler.write(offset, data, fua)` |
| `NBD_CMD_FLUSH` | `UBLK_IO_OP_FLUSH` | `handler.flush()` |
| `NBD_CMD_TRIM` | `UBLK_IO_OP_DISCARD` | `handler.trim(offset, length, fua)` |
| `NBD_CMD_WRITE_ZEROES` | `UBLK_IO_OP_WRITE_ZEROES` | `handler.write_zeroes(offset, length, fua)` |

Errors use transport-specific mapping: `CommandError::to_nbd_errno()` for NBD wire format, `CommandError::to_linux_errno()` for ublk. The values happen to match today (NBD uses Linux errno on the wire), but encoding them separately makes the contract explicit.

NBD remains the default for development and broad compatibility. ublk is opt-in for production Linux deployments where per-I/O overhead matters.

### Why write-behind instead of write-through?

S3 PUT latency is 50-200ms. Write-through would make snapshots take 5-15 seconds instead of <100ms.

Write-behind trades durability for latency: data between the last FLUSH and the next S3 sync is at risk if the host dies. This is acceptable because:

1. Pack-size flush keeps the dirty window small (flush triggers at 100 dirty blocks = ~12.8MB per export)
2. SIGTERM triggers a drain before exit
3. The workload (microVMs) is ephemeral — VMs can be recreated from base images
4. Max dirty data node-wide is bounded: 99 blocks × 128KB × 2000 exports ≈ 25GB

### Why defer hashing to flush time?

An earlier design computed BLAKE3 on every write. For a 4KB write to a 128KB block:

| Operation | Cost |
|-----------|------|
| pread 128KB from SSD | ~15-25µs |
| blake3(128KB) | ~20-30µs |
| Bytes::copy_from_slice(128KB) into clean cache | ~5-10µs |
| **Total overhead** | **~50-65µs** |

None of this work is needed until flush time. With deferred hashing, the write path is ~5µs for 4KB random writes (just `set_present` + `pwrite` + `transition_to_dirty` + WAL). The hash computation moves to the flush path, which already reads every dirty block from SSD to build packs — so the work happens exactly once.

**Write coalescing is free**: write the same block 100 times before flush, hash it once. Previously: 100 hashes, 99 thrown away.

The read path uses `is_present(idx)` to decide between paths: present → pread from SSD (hot path, ~5µs); not-present → VolumeManifest → ChunkMetaCache → S3. No per-block hash is stored; the SSD is always authoritative for present blocks.

### Why chunked block index instead of a flat manifest?

The original design stored an export's entire block map + pack index in a single binary manifest (`GLDE` format). This had several scaling problems:

1. **Manifest size grows with device size, not working set**: A 1TB export with 100MB of data still serializes 8M block entries on every flush. At 1000 exports, manifest uploads dominated S3 bandwidth.
2. **Delta manifests added complexity**: Tracking "base state" for delta computation, merge-on-read logic, compaction heuristics, staleness checks — all to work around the fundamental scaling issue.
3. **Fork required rehydrating the full pack index**: Every fork loaded the parent's entire manifest (potentially hundreds of MB) to populate HostPackIndex. With 180 fork VMs, 180× repeated deserialization of the same data.
4. **HostPackIndex (redb) was a shared mutable bottleneck**: All exports on a host competed for a single redb write transaction. The index grew unbounded unless aggressively pruned, and pruning required cross-export coordination.

The chunked block index fixes all four:

1. **VolumeManifest is O(volume chunks with data)**: A 1TB device with 100MB written has ≤10 chunks with data, so the manifest is ≤10 lines of JSON (~300 bytes). Flush uploads only the modified chunk's `.meta` (not the whole block map).
2. **No deltas**: ChunkMeta files are immutable and content-addressed. "Updating" a chunk = upload a new `.meta` file + update VolumeManifest. No base state, no compaction, no merge logic.
3. **Fork is 2 S3 ops**: GET parent's VolumeManifest (1KB) + PUT fork's VolumeManifest. No pack index hydration — the ChunkMetaCache loads chunks lazily on first read access, shared across all forks by content hash.
4. **No shared mutable index**: ChunkMetaCache is a read-through cache (LRU + SSD), not a write-through index. Each flush writes new immutable files and updates its own export's VolumeManifest. No cross-export coordination.

Trade-off: cold reads require 2 round-trips (GET VolumeManifest + GET ChunkMeta) before fetching the block data. In practice, ChunkMetaCache has high hit rates — the first read of a VM's boot sequence loads the relevant chunks into cache, and subsequent reads are served from cache.

### Why content-addressed packs instead of per-block S3 objects?

Per-block storage means one S3 PUT per 128KB write. At 28K IOPS, that's 28K PUTs/second — prohibitively expensive ($0.14/hour in S3 API costs alone).

Packing up to 100 blocks per S3 object reduces PUTs by up to 100x. Content addressing (hash as identity) enables within-chunk deduplication without coordination — the ChunkMeta provides a hash set of already-stored blocks, so identical blocks (e.g., kernel pages shared across VM forks) are detected at flush time and skipped.

Trade-off: read amplification. A cache miss fetches the entire pack (up to ~12.8MB) even if only one block (128KB) is needed. The 99.67% cache hit rate makes this acceptable — misses are rare, and the extra data often prefills the cache for subsequent reads.

### Why BLAKE3-128 instead of full BLAKE3-256?

128-bit collision resistance is sufficient for content deduplication (birthday bound: 2^64 operations). 16 bytes fits in two `u64`s and is compact in ChunkMeta entries. Halves per-entry metadata cost vs full 256-bit hash.

### Why 128KB block size?

Matches ZFS default recordsize. Analysis in `BLOCK_SIZE_ANALYSIS.md` shows 128KB is the sweet spot: smaller blocks (16-32KB) reduce write amplification for random I/O but increase metadata overhead and S3 API costs. Larger blocks (256KB+) improve sequential throughput but waste bandwidth for small random writes.

### Why typestate instead of runtime state checks?

Compile-time prevention of invalid operations. You literally cannot call `write()` on a `WriteCache<Recovering>` — the method doesn't exist for that type parameter. No runtime cost, no forgotten state checks.

### Why a lock-free circuit breaker?

S3 outages shouldn't cascade into mutex contention on the hot path. All circuit breaker state is packed into a single `AtomicU64` — no locks, no multi-variable coordination. CAS loops guarantee consistent state transitions even under high concurrency.

We use a consecutive-failure policy (not windowed) by default because S3 outages tend to be total, not partial.

### Why sparse page tables instead of dense arrays?

Dense arrays pre-allocate for all blocks: a 1TB export with 128KB blocks has 8M entries. At 1 byte per block state, that's 8MB per export — manageable. But the old `AtomicBlockMap` (32 bytes per block for hash + sequence + CRC32) cost 256MB per 1TB export. At 10,000 exports: 2.5TB just for hash metadata. The directories alone (needed even for empty exports) cost 5.1GB at that scale.

Sparse page tables allocate on first write. With 2-bit packing (4 entries per `AtomicU8`), each 4KB page holds 16,384 entries — 4× denser than a naive 1-byte-per-entry layout. The directory (one pointer per page) costs ~4KB for a 1TB export (512 entries). An empty export: ~4KB. A fully-written 1TB export: ~2MB. The cost is one extra pointer dereference on the hot path — a branch that predicts correctly almost every time.

After removing `AtomicBlockMap`, only `SparseStateMap` (2 bits/block) remains for per-block metadata. CRC32 is now in a `DashMap<usize, u32>` sized to dirty blocks (~0 at idle, ~1.6MB at 80K dirty blocks). No per-block hashes stored anywhere — hashes are computed fresh from SSD at flush time.

### Why explicit versioned snapshots instead of manifest history?

Two alternatives were considered: (1) keep a log of all past manifests (append-only, like a WAL), (2) use S3 versioning to recover old manifests.

We chose explicit versioned snapshots because:

1. **Control plane owns the lifecycle**. The orchestrator decides which checkpoints are worth keeping — not every background sync. A nightly snapshot policy means 365 objects per year, not thousands of unlabeled manifests.
2. **GC is simple and safe**. With a well-defined `snapshots/{name}/{seq}` namespace, GC knows exactly where to look to determine chunk liveness. Implicit manifest history would require GC to scan S3 version metadata, which is expensive and not portable across S3-compatible backends.
3. **Fork precision**. The `sequence` number is the exact block-flush cutpoint. The orchestrator records it at snapshot time and uses it later to fork — even months later. S3 versioning uses timestamps, which are subject to clock skew and don't map cleanly to GlideFS's internal sequence counter.
4. **Zero cost for exports that never snapshot**. Exports that only use `sync_manifest` (background flush) don't create any objects under `snapshots/`. GC skips the `snapshots/` prefix if it's empty.

The trade-off: snapshot creation requires an explicit API call. Background flushes don't create snapshots. If the orchestrator crashes between syncs, the latest state is in `manifests/{name}` (recoverable) but there's no new versioned snapshot. Operators must call `POST /snapshot` at the right moment.

### Why best-effort for the versioned snapshot upload?

In `WriteCache::snapshot()`, the critical step is uploading the VolumeManifest (`manifests/{name}`) — this is what S3 recovery uses. The versioned snapshot (`snapshots/{name}/{seq}`) is best-effort because:

1. The base manifest is already consistent after step 2 — background flushes and WAL recovery work correctly without the versioned key.
2. S3 transient failures shouldn't fail the entire snapshot operation. The control plane can detect the missing snapshot via `GET /snapshots` and retry just the snapshot, not a full flush.
3. Orphaned base manifest writes (step 2 succeeds, step 3 fails) are harmless — they're idempotent overwrites of the current state.

### Why SYNCING-based flush instead of sequence-number CAS?

The previous design used per-block sequence numbers to detect concurrent writes during flush:

1. At snapshot time: record `(block_idx, seq)` for each dirty block.
2. After upload: only clear dirty if `current_seq == snapshot_seq`.

This required storing 8 bytes per block (the sequence) in the block map, plus a global sequence counter bump on every write.

SYNCING-based flush achieves the **same guarantee with zero per-block sequence storage**:

1. **Claim**: `CAS DIRTY→SYNCING` atomically snapshots the block into the flush pipeline.
2. **Concurrent write**: `transition_to_dirty()` CAS-loops `SYNCING→DIRTY` — the write always succeeds regardless of ordering.
3. **Release**: `CAS SYNCING→CLEAN` fails if state is now DIRTY (concurrent write happened).

The no-ABA guarantee: the flush scheduler is sequential per export (one `tokio::select!` loop). There are never two concurrent flush cycles for the same export, so a block can't transition `SYNCING→DIRTY→SYNCING` from two different flushes.

**CRC32 state discrimination**: The SYNCING state also replaces the sequence-based CRC discrimination:
- Old: `computed_crc != stored_crc` AND `current_seq == snapshot_seq` → corruption.
- New: `computed_crc != stored_crc` AND `state == SYNCING` → corruption (no write happened).
  `computed_crc != stored_crc` AND `state == DIRTY` → concurrent write (stale CRC, not corruption).

This eliminates all per-block sequence storage and all CRC clearing on the write path — two hot-path atomic operations removed per write.

### S3 Concurrency Limits

With 5,000 VMs on a single host, unbounded S3 concurrency is a problem. Each export's flush can upload multiple packs concurrently, and each cache miss triggers an S3 GET. In the worst case: 5,000 VMs × 25 packs = 125K concurrent uploads, or 5,000 × 256 in-flight NBD reads = 1.28M concurrent GETs.

Two host-level `tokio::Semaphore`s bound this — one for uploads (background flush), one for downloads (read path). The semaphores live on `ExportRouter` and are shared (via `Arc`) to every `ContentStore` instance. This is a global gate, not per-export.

| Semaphore | Default | Rationale |
|-----------|---------|-----------|
| `max_s3_uploads` | 128 | Background flush is not latency-sensitive; caps inflight PUTs |
| `max_s3_downloads` | 512 | Read path is latency-sensitive (NBD client waiting); higher limit |

Set to 0 for unlimited (tests use this). Permit is acquired before the S3 call and held for the duration of the request.

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
| `block/server.rs` | NBD transport: TCP/Unix socket listener, protocol negotiation, concurrent request dispatch |
| `block/ublk/mod.rs` | ublk transport: per-export `/dev/ublkbN` device management (Linux 6.0+, `--features ublk`) |
| `block/ublk/device.rs` | Single ublk device: io_uring registration, per-queue I/O loop, teardown |
| `block/router.rs` | Multi-tenant export manager: create, delete, drain (concurrent, 16-wide), promote, resize |
| `block/handler.rs` | Transport-agnostic block I/O dispatch (read/write/flush/trim) with SSD write rejection at 95% |
| `block/write_cache/mod.rs` | `WriteCache<S>` typestate wrapper, `FlushStats`, `SnapshotResult` |
| `block/write_cache/inner.rs` | `CacheInner`: shared state, `SyncFile`, `SparseStateMap` integration, metadata persistence |
| `block/write_cache/write.rs` | Write path: set_present + pwrite + transition_to_dirty + WAL (no hash, no CRC on write) |
| `block/write_cache/read.rs` | Read path: `is_present` → pread (hot path); else VolumeManifest → ChunkMetaCache → S3 |
| `block/write_cache/flush.rs` | SYNCING-based flush: CAS DIRTY→SYNCING (claim), rayon CRC32+BLAKE3+LZ4, S3 upload, CAS SYNCING→CLEAN; VolumeManifest sync |
| `block/write_cache/init.rs` | Cache file creation, pre-allocation, metadata loading (v5 format: block_states + max_seq) |
| `block/write_cache/recovery.rs` | WAL replay, `verify_dirty_blocks_readable()` (SSD readability check, no hash comparison) |
| `block/write_cache/config.rs` | `WriteCacheConfig` with per-export overrides |
| `block/write_cache/error.rs` | `CacheError` type |
| `block/block_map.rs` | `Blake3Hash`, `SparseStateMap` (lock-free sparse page-table, 2-bit packed, 4 entries/byte), `SequenceNumber`, LZ4 helpers |
| `block/volume_manifest.rs` | `VolumeManifest`: JSON root mapping chunk_idx → chunk_hash, address translation helpers |
| `block/chunk_meta.rs` | `ChunkMeta` (GLCM): binary block location index; serialize/deserialize/merge/lookup/content_hash |
| `block/chunk_cache.rs` | `ChunkMetaCache`: two-tier LRU + SSD cache for `ChunkMeta` objects, keyed by content hash |
| `block/state.rs` | Sealed typestate markers (`Initializing`, `Recovering`, `Active`, `Draining`) |
| `block/pack.rs` | Pack wire format (GLPK): assemble, parse, extract blocks |
| `block/capacity_monitor.rs` | SSD capacity monitor: `statvfs` polling, pressure flush on dirtiest exports |
| `block/content_store.rs` | S3 PUT/GET for packs, chunk .meta files, and manifests via `object_store` crate |
| `block/manifest.rs` | Hot set format: `serialize_hot_set` / `deserialize_hot_set`; S3 key helpers |
| `block/flush_scheduler.rs` | Event-driven pack flush (Notify) + periodic WAL checkpoint (5s) |
| `block/wal.rs` | Append-only WAL for crash recovery with CRC32 integrity |
| `block/cache.rs` | `BlockCache` trait + `FoyerBlockCache` (memory + SSD hybrid) |
| `block/readahead.rs` | Sequential read detector: 3+ consecutive blocks triggers prefetch |
| `block/scrubber.rs` | Background corruption detection: `HashSource` trait + `RouterHashSource`, re-hash cached blocks, evict on mismatch |
| `block/sync.rs` | Loom/std compatibility shim: re-exports atomics for exhaustive interleaving tests |
| `block/metrics.rs` | Per-export Prometheus-compatible telemetry with sampled latency histograms |
| `block/protocol.rs` | NBD wire format: handshake options, transmission commands, reply serialization |
| `block/api.rs` | HTTP REST API for export CRUD, drain, promote, metrics |
| `block/error.rs` | Error types: `NBDError` (protocol-specific), `CommandError` (transport-agnostic, maps to NBD errno or Linux errno) |
| `circuit_breaker.rs` | Lock-free S3 circuit breaker (single AtomicU64, CAS transitions) |
| `config.rs` | TOML configuration parsing with environment variable expansion |
| `storage_compatibility.rs` | S3 conditional write check (`PutMode::Create`) for fencing support |
| `parse_object_store.rs` | Vendored URL → ObjectStore factory (S3, GCS, Azure, local, memory) |
| `task.rs` | Named Tokio task spawning helpers for debuggability |
| `deku_bytes.rs` | `Bytes` adapter for deku binary protocol parsing (NBD wire format) |
| `cli/server.rs` | `glidefs run`: wire up config → router → server → API |
| `cli/bless.rs` | `glidefs bless`: create golden images from local directories |
| `cli/gc.rs` | `glidefs gc`: orphaned pack and .meta garbage collection with grace period |
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

- **Pack-size trigger**: When an export accumulates `BLOCKS_PER_PACK` (100) dirty blocks, the write path notifies the flush scheduler via `tokio::sync::Notify`. The scheduler wakes, flushes dirty blocks as content-addressed packs to S3, syncs the VolumeManifest, and checkpoints. Event-driven, not polled.
- **Local checkpoint** (5s): Periodic WAL truncation + block state persistence. No S3 involvement.
- **Manifest sync**: After every successful pack upload, the scheduler syncs the VolumeManifest so flushed chunks are immediately discoverable on cross-host recovery (host death without drain).

The dirty block counter only increments on `Clean→Dirty` transitions, so rewriting the same block 100 times counts as 1 dirty block — natural write coalescing.

At 2K microVMs per node: max ~25GB dirty data node-wide (99 blocks × 128KB × 2000 exports). (`flush_scheduler.rs`, `handler.rs:check_flush_threshold`)

## Memory Overhead

`SparseStateMap` uses a sparse page table with 2-bit packing — 4 entries per `AtomicU8`, 16,384 entries per 4KB page. Pages are allocated on first write, not upfront. An empty export costs only its directory (~4KB). CRC32 is stored in a `DashMap` sized to dirty blocks, not device size.

```
SparseStateMap (block state: NotPresent/Clean/Dirty/Syncing, 2-bit packed)
├── directory: Box<[AtomicPtr<StatePage>]>    4 KB  (512 entries for 8M blocks)
│   ├── [0] → StatePage { data: [AtomicU8; 4096] }  4096 bytes (16,384 entries)
│   ├── [1] → null  (unwritten — zero cost)
│   └── ...
│
│   Byte layout within StatePage:
│   ┌─────────────────────────────────────────────┐
│   │ bits [7:6]=entry3 [5:4]=entry2 [3:2]=entry1 [1:0]=entry0 │
│   └─────────────────────────────────────────────┘
│   CAS on one entry may spuriously fail if an adjacent entry in the
│   same byte was modified concurrently — the CAS loop retries (~ns).

crc_map: DashMap<usize, u32>  — only populated during flush window
├── Populated by checkpoint (every 5s) for dirty blocks not yet flushed
├── Consumed by flush (remove-and-verify per block)
├── Hard cap: 10M entries (~200MB) — covers a fully-dirty 1TB device (8M blocks)
│   Beyond the cap, new blocks skip CRC verification (SYNCING state still guarantees correctness)
└── At idle: 0 entries, 0 bytes
    At max dirty rate (10GB/s writes, 5s interval): ~80K entries × ~20B = ~1.6MB
```

| Component | Per-Export (fixed) | Per-Written-Page | Shared | Notes |
|-----------|-------------------|-----------------|--------|-------|
| `SparseStateMap` directory | ~4 KB | 4 KB per 16,384 blocks written | — | 2 bits/entry × 16,384 entries/page |
| `crc_map` | 0 (transient) | — | — | DashMap entry per dirty block between checkpoint and flush; ~20B each (bucket metadata + alignment + load factor). Hard cap: 10M entries (~200MB) |
| `ChunkMetaCache` | — | — | ~32 entries × ~4MB/entry max | LRU, disk-resident for persistence; shared across all exports by content hash |
| `CleanCache` (memory) | — | — | `memory_size_gb` | Configured, default 1GB |
| `CleanCache` (SSD) | — | — | `ssd_cache_size_gb` | Configured, default 10GB |

| Scenario (1TB/128KB = 8M blocks) | Before (AtomicBlockMap era) | After (2-bit packed) | Savings |
|---|---|---|---|
| Empty export | ~530 KB | ~4 KB | 132× |
| 1% written (84K blocks) | ~5 MB | ~24 KB | 208× |
| 100% written | **~260 MB** | **~2 MB** | **130×** |
| 10,000 empty exports | **~5.1 GB** (directories alone) | **~40 MB** | **128×** |
| 10,000 exports, 100% written | **~2.5 TB** | **~20 GB** | **128×** |

## Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Host death before S3 sync | Data loss (writes since last sync) | Recreate VM from base image or last manifest |
| Host death after S3 sync | No data loss | Wake on any node, reads pull from S3 |
| S3 read failure on cache miss | `EIO` to guest | Guest retries; circuit breaker fast-fails if S3 is down |
| S3 write failure during flush | Blocks remain Dirty | Scheduler retries next cycle; circuit breaker limits attempts |
| VolumeManifest upload failure | Stale manifest in S3 | Re-uploaded on next flush cycle |
| Silent data corruption | Stale/wrong data served | Scrubber detects via hash mismatch, evicts; next read re-fetches from S3 |
| Process crash mid-WAL-write | Torn WAL entry | CRC32 detects; replay stops at corruption, discards torn tail |
| Process crash mid-S3-sync | Orphaned packs or .meta files in S3 | Harmless: files are immutable; GC cleans up unreferenced packs after grace period |
| S3 sustained outage | Circuit breaker opens | Reads from local SSD continue; writes accumulate locally; breaker probes S3 every 30s |
| Local SSD full | `ENOSPC` to guest | Write handler rejects new-block writes at >95% SSD utilization; capacity monitor pressure-flushes dirtiest exports to S3, freeing physical space. Overwrites to already-present blocks are still allowed. |
| SSD failure | Same as host death — writes since last manifest sync are lost | Recreate from last S3 VolumeManifest; ChunkMetaCache repopulates lazily from S3 `.meta` files on first read. |

## Testing

| Suite | Command | Count | What It Covers |
|-------|---------|-------|----------------|
| Unit | `cargo test --features test-utils --lib` | ~330 | Lock-free atomics, wire format round-trips (GLPK/GLCM), state transitions, sparse page tables, CRC32 integrity, VolumeManifest + ChunkMeta serde |
| Integration | `cargo test --features test-utils --test integration` | ~52 | Crash recovery, concurrent writes, flush consistency, chunked S3 layout, snapshot + fork correctness (no Docker) |
| Docker | `cargo test --features docker-tests --test docker_integration` | ~20 | Real S3 via MinIO (testcontainers-rs), end-to-end via `TestServer.connect()` (transport-agnostic client abstraction) |
| Loom | `cd loom-tests && cargo test --release` | — | Exhaustive interleaving of lock-free CAS algorithms (SparseStateMap DIRTY↔SYNCING↔CLEAN transitions) |
