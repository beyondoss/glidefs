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
              ┌── YES ──► SSD pread    ~5µs  (hot path: dirty or clean-SSD)
              │           (SYNCING blocks → flushing file during flush)
              │
              └── NO (not yet written / fork from S3)
                      │
                      ├── VolumeManifest: block_idx → chunk_idx → [pack_id, ...]
                      │   (if no packs for chunk → block never written → return zeros)
                      │
                      ├── PackIndexCache: newest-first lookup by chunk_offset
                      │   ├── hit (index cached) → (hash, pack_offset, comp_length)
                      │   └── miss → parallel prefetch ALL pack indices for chunk from S3
                      │
                      ├── Tier 1: CleanCache memory (Foyer)           ~100ns
                      │           ├─ hit → return
                      │           └─ miss ▼
                      │
                      ├── Tier 2: CleanCache SSD (Foyer)              ~100µs
                      │           ├─ hit → return
                      │           └─ miss ▼
                      │
                      └── Tier 3: S3 range GET                        50-300ms
                                  ContentStore::get_chunk_block(chunk_idx, pack_id, offset, comp_length)
                                  → LZ4 decompress → verify BLAKE3 → insert CleanCache → return
```

Multi-block reads fan out with `futures::future::try_join_all()`. Sequential access (3+ consecutive chunk accesses) triggers prefetch of the next pack boundary to hide S3 latency. (`readahead.rs`)

**Parallel prefetch on cache miss**: On the first cache miss for a chunk, ALL pack indices for that chunk are fetched in parallel (up to N concurrent S3 range-reads). This collapses N sequential round-trips into 1 parallel batch (~50ms). (`write_cache/read.rs:resolve_block`)

### Background Sync (S3 upload)

```
FlushScheduler (event-driven: Notify from write path when dirty_count ≥ 500)
    │
    ▼
Phase 1 — Claim: CAS DIRTY→SYNCING for each dirty block (atomic snapshot)
    │           └─ Concurrent guest writes CAS SYNCING→DIRTY (blocks re-dirtied)
    ▼
Partition claimed (SYNCING) blocks by chunk (block_index / blocks_per_chunk)
    │
    ▼
Warm PackIndexCache for all affected chunks (cold start: fetch pack indices from S3)
    │
    ▼
For each chunk (one pack per chunk per flush cycle):
    ├── Build (hash, chunk_offset, compressed) triples via rayon parallel:
    │   ├── pread block from SSD
    │   ├── CRC32 verify from SparseCrcMap (if available)
    │   ├── Skip zero blocks (well-known hash sentinel)
    │   ├── BLAKE3-128 hash → LZ4 compress
    │   └── Collect into Vec<(hash, chunk_offset, compressed)>
    ├── ContentStore::stream_chunk_pack():
    │   ├── WriteMultipart::new(put_multipart_opts(...))   ← streaming S3 upload
    │   ├── Write 16-byte header
    │   ├── For each block: writer.put(Bytes::from(compressed))  ← freed immediately
    │   ├── Write block index footer (N × 28 bytes, sorted by chunk_offset)
    │   ├── Write 8-byte GLIX trailer
    │   └── writer.finish()  ──► S3 at chunks/{idx:04}/{pack_id:016x}.pack
    ├── PackIndexCache::insert_entries(pack_id, entries)
    └── VolumeManifest::append_pack(chunk_idx, pack_id)
    │   (manifest now has [old_packs..., new_pack_id])
    │
    ▼
Phase 3 — Release:
    CAS SYNCING→NOT_PRESENT (evict from SSD)
        ├── Insert decompressed blocks into CleanCache (S3-FIFO probationary queue)
        ├── Copy failed/skipped blocks from flushing file to active file
        └── unlink("{name}.flushing")
    │
    ▼
Inline compaction: if chunk.packs.len() > threshold (default 16)
                    OR dead-block ratio > 50% (superseded entries across packs):
    └── compact_chunk(): merge N delta packs → 1 base pack
        ├── Load all pack indices from PackIndexCache (or S3 on miss)
        ├── Build merged block map: newest entry wins per chunk_offset
        ├── Fetch live blocks: clean cache (foyer) first, then S3 range-reads on miss
        ├── ContentStore::stream_chunk_pack() → stream new base pack to S3
        ├── VolumeManifest::replace_packs(chunk_idx, [new_pack_id])
        └── Old pack_ids removed from manifest → GC collects them eventually
    │
    ▼
Atomic commit: PUT VolumeManifest (binary GLVM) to S3
```

Each pack is self-describing — the block index is a footer (trailer → index entries), enabling suffix-only reads without a full object fetch.

## Concepts & Terminology

| Term | Definition | NOT |
|------|-----------|-----|
| Transport | The kernel-to-userspace block I/O channel: NBD (TCP/Unix socket, cross-platform) or ublk (io_uring, Linux 6.0+) | Not the storage layer — both transports use the same `BlockHandler` |
| BlockHandler | Transport-agnostic I/O handler: read/write/flush/trim/write_zeroes/cache. Used by both NBD and ublk. | Not protocol-specific — knows nothing about NBD or ublk wire formats |
| Export | A virtual block device served over a transport, with its own cache and S3 prefix | Not a filesystem — raw blocks only |
| Block | Fixed-size unit of data (default 128 KB to match ZFS recordsize) | Not variable-sized |
| Volume Chunk | 128 MiB range of blocks (1,024 blocks of 128 KB = 1 ext4 block group). The unit of pack scoping, compaction, and metadata management. | Not a 128 KB block — "chunk" means 128 MiB range. Aligns with ext4 block groups, bounding database scatter to 2–3 chunks per flush |
| Pack | GLPK S3 object containing LZ4-compressed blocks scoped to one volume chunk. Footer-indexed: header + block data + index footer + GLIX trailer. | Not cross-chunk |
| PackId | 8-byte random `u64` identifying one pack within its chunk. Hex string in S3 key. | Not a UUID. Collision-safe: birthday bound ~4.3 billion per chunk, and chunks see hundreds of IDs over their lifetime |
| VolumeManifest (GLVM) | Binary file mapping `chunk_idx → [pack_id, ...]`. Sparse: only written chunks appear. The root of an export's metadata. CRC32-protected. | Not the full block index — pack IDs point to self-describing packs that contain the block-level index |
| ChunkEntry | `Vec<PackId>` for one chunk, ordered oldest-to-newest. After compaction: single entry. | Not block-level index — that lives in each pack's embedded index |
| PackIndexCache | Two-tier Foyer HybridCache (64 MB memory + 2 GB SSD) keyed by `PackId → Vec<PackIndexEntry>`. SSD tier stores entries extent-encoded + zstd-compressed (~17 bytes/entry vs 28 raw). 2 GB holds ~117M entries = index coverage for ~7.5 TB of block data. Enables block-level resolution without S3 round-trips. | Not the block data itself — only the index entries. Not per-export — shared cache keyed by PackId |
| Pack Accumulation | Each flush `append_pack`s a new pack_id to the chunk's list. Overwriting the same block creates a new pack; the old pack still exists in S3 until compaction removes it from the manifest and GC deletes it. | Not immediate deletion of old data — compaction + GC handles that |
| Compaction | Inline merge of N delta packs → 1 base pack after flush. Triggered by pack count > 16 or dead-block ratio > 50%. Old pack_ids removed from manifest; GC deletes the S3 objects. | NOT inline pack deletion — compaction only updates the manifest. GC handles the S3 DELETE |
| Block State Map | Per-block state (NotPresent / Clean / Dirty / Syncing). Lock-free sparse page table with 2-bit packed `AtomicU8`. | Not the data itself — no hashes stored per-block |
| Drain | Flush all dirty blocks to S3 so the export can be stopped or migrated | Not a delete — S3 data is preserved |
| Clean Cache | Read-through Foyer HybridCache (memory + SSD tiers) for blocks fetched from S3 | Not the write cache (dirty blocks on local SSD) |
| Circuit Breaker | Lock-free S3 failure detector that fast-fails requests during outages | Not a retry mechanism — it prevents retries |
| Hot Set | List of non-zero block indices for a base image, used to prefetch blocks on fork boot | Not a cache — it's a prefetch hint |
| Bless | CLI command that converts a raw disk image into a content-addressed base image in S3 | Not a runtime operation — offline image preparation |
| Snapshot | An explicit, versioned copy of an export's VolumeManifest stored at a stable S3 key. Never overwritten by background syncs. Pinned in S3 until explicitly deleted. | Not the same as a background `sync_manifest` — background syncs update `manifests/{name}` and do NOT create versioned snapshot keys |
| Fork | A new export whose VolumeManifest is copied from a parent's. The fork starts with an empty local SparseStateMap; unwritten blocks resolve through VolumeManifest → PackIndexCache → S3. | Not a full copy — parent pack files are never duplicated |
| Snapshot Sequence | A monotonic counter (`u64`) that increments on every flush. | Not a timestamp — purely an ordering counter |
| Manifest Tag | A named alias for a VolumeManifest, stored at `manifests/{tag}`. Forkable by name. | Not a snapshot sequence — not versioned; overwriting updates the pointer |
| File Rotation | After flush, blocks are evicted (SYNCING→NOT_PRESENT) and the flushing file is deleted. Local SSD is a bounded write-back buffer. Reads for evicted blocks go through CleanCache → S3. | Not optional — this is the only storage mode. CLEAN state exists only transiently during migration from older metadata |

## S3 Object Layout

```
{db_path}/
└── exports/{s3_prefix}/                         ← ContentStore root (shared by exports + bless)
    ├── manifests/{export_name}                  ← VolumeManifest GLVM (chunk_idx → [pack_ids])
    ├── manifests/{tag_name}                     ← Named manifest tag (same format, arbitrary name)
    ├── manifests/bases/{image_name}             ← Blessed base image VolumeManifest (glidefs bless)
    ├── manifests/bases/{image_name}.hot-set     ← Boot hot set for base image (block indices)
    ├── snapshots/{export_name}/{sequence:020}   ← Versioned VolumeManifest (zero-padded sequence)
    └── chunks/{chunk_idx:04}/
        └── {pack_id:016x}.pack                 ← GLPK pack (self-describing: header+index+data)
```

Chunk directories use 4-digit zero-padded indices (`chunks/0000/`, `chunks/0001/`, ...). A 1 TB device has 8,192 chunks (128 MiB each). A compacted chunk has exactly 1 pack file; an uncompacted chunk may have up to `DEFAULT_COMPACTION_THRESHOLD` (16) packs.

**Manifest size by scenario:**

| Scenario | GLVM size |
|----------|-------------|
| 100 GiB, 10% written, compacted | ~1.2 KB |
| 1 TB, 50% written, compacted | ~57 KB |
| 1 TB, 50% written, 4 packs/chunk | ~155 KB |
| 1 TB, 100% written, 4 packs/chunk | ~311 KB |

## Core Mechanism: Write-Behind Cache

GlideFS decouples write latency from S3 round-trip time. Writes land on local SSD immediately; a background scheduler uploads dirty blocks to S3 as content-addressed packs.

### Lock-Free Hot Path

The write path avoids all locks. Three techniques make this possible:

1. **Positional I/O** (`pread`/`pwrite`): The `SyncFile` wrapper uses POSIX positional I/O, which is atomic per-syscall and doesn't use the file position pointer. No locking needed for concurrent block reads and writes. (`write_cache/inner.rs:SyncFile`)

2. **Sparse state map** (CAS + sparse page table): `SparseStateMap` stores per-block state in a two-level page table with 2-bit packing — 4 entries per `AtomicU8`, 16,384 entries per 4KB page. Pages are allocated on first write via CAS — empty exports use ~4KB for the directory. State transitions (`set_present`, `transition_to_dirty`, `transition_dirty_to_syncing`, `transition_syncing_to_not_present`) are CAS-on-byte loops with no global lock. (`block_map.rs:SparseStateMap`)

3. **Sparse CRC map** (AtomicU32 page table): `SparseCrcMap` tracks CRC32 checksums for dirty blocks using the same two-level page table pattern as `SparseStateMap` — 1,024 `AtomicU32` entries per 4KB page, lazily allocated. Checkpoint computes CRC32s; flush verifies them to detect SSD corruption. Lock-free: stores, loads, and CAS operations on `AtomicU32` with no shard locks. (`block_map.rs:SparseCrcMap`)

4. **Sequence numbers**: A monotonic `AtomicU64` counter (`SequenceNumber`) provides WAL ordering. Each write bumps the sequence; the max sequence is persisted in `block_states` metadata so it survives crash recovery. (`block_map.rs:SequenceNumber`)

### Content Addressing

Every block is identified by its BLAKE3-128 hash (16 bytes, truncated from 256-bit), computed at flush time (not on the write path). This enables:

- **Within-batch deduplication**: During flush, zero blocks and within-batch duplicates are deduplicated (seen_hashes set). Two blocks at different `chunk_offsets` with the same hash each get their own index entry — required for the read path to resolve them by position.
- **Integrity verification**: Read path verifies hash after S3 fetch and LZ4 decompression.
- **Sparse manifests**: VolumeManifest only stores chunks that have been written — a 500 GB export with 2 GB of data has a tiny manifest.

The well-known hash of a 128 KB zero block (`zero_block_hash()`) lets the flush path skip blocks that are all-zeros — they're deduplicated without storage or S3 interaction. (`block_map.rs`)

### Pack Accumulation vs Compaction

Each flush cycle produces one delta pack per dirty chunk and appends its ID to the manifest. Overwriting the same block does NOT remove the old pack — both the old and new packs exist in S3. Only compaction removes stale packs from the manifest.

**Why accumulate instead of in-place update?** S3 objects are immutable. "Updating" a pack requires creating a new pack, uploading it, and atomically swapping the manifest. Accumulating packs amortizes this cost: compaction runs infrequently rather than on every flush.

**Compaction triggers** (two-pass evaluation after each flush):
1. **Pack count**: chunk has more than `DEFAULT_COMPACTION_THRESHOLD` (16) packs
2. **Dead-block ratio**: >50% of entries across a chunk's packs are superseded by newer entries (catches heavy overwrites on few-pack chunks)

**Compaction algorithm** (`compact_chunk()`):
1. Merge all pack indices — newest entry wins per `chunk_offset`
2. Fetch live blocks: try clean cache (foyer) first, fall back to S3 range-reads on miss
3. Stream to a single GLPK base pack via `stream_chunk_pack()`
4. Upload completes — index and trailer appended by the streaming writer
5. `replace_packs_cas(chunk_idx, old_pack_ids, [new_pack_id])` — CAS-replaces old pack_ids in manifest, detecting concurrent appends

Old packs are NOT deleted inline. After `replace_packs`, the old pack_ids are absent from the live manifest. GC identifies them as dead and deletes them after the grace period. This design keeps the flush path simple (no snapshot scanning inline) and leverages GC's existing safety mechanisms (grace period, max-delete cap, snapshot pinning). (`write_cache/compact.rs`)

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

## File Rotation & Eviction

Local SSD is a bounded write-back buffer, not a persistent cache. After each flush to S3, blocks are evicted (SYNCING→NOT_PRESENT) and the flushing file is deleted. SSD footprint per export: `(dirty + syncing) × block_size` — only blocks modified since the last flush consume local space.

### File Rotation

Two files, deterministic naming. Writes accumulate in the active file. At flush time, the active file becomes the flushing file (atomic rename), a new sparse active file is created, and the flush reads from the flushing file. Direct `block_idx × block_size` addressing preserved throughout.

```
Normal:        {name}.cache          ← active, receives all writes
During flush:  {name}.cache          ← new active (sparse, receives new writes)
               {name}.flushing       ← old active (immutable, being uploaded)
After flush:   {name}.cache          ← active
               (flushing deleted)
```

**Rotation sequence** (inside `flush_dirty_inner`, under `flush_lock`):

1. `flushing_active.store(true)` — signals ublk to stop using io_uring registered fd
2. `rename("{name}.cache", "{name}.flushing")` — crash recovery boundary
3. Create new sparse `SyncFile` at `"{name}.cache"` (set_len to device_size)
4. Swap `data_file` RwLock (write-locked briefly, ~2ns read-lock for all I/O)
5. Store old handle in `flushing_file: Mutex<Option<SyncFile>>`
6. CAS DIRTY→SYNCING for all dirty blocks
7. `compute_flush_batch` reads from `flushing_file`
8. Upload to S3
9. Finalize: CAS SYNCING→NOT_PRESENT (evict), copy skipped blocks flushing→active
10. `unlink("{name}.flushing")`, `flushing_active.store(false)`

**SSD footprint**: With 128KB blocks, 500 blocks_per_pack, 5s flush interval: ~64MB active + ~64MB flushing = ~128MB per export during flush. Outside flush: just dirty blocks since last flush.

### Read Path After Eviction

Evicted blocks (NOT_PRESENT) resolve through the standard cold path: VolumeManifest → PackIndexCache → CleanCache → S3. The flush path warms CleanCache with decompressed block data during upload, so the first read after eviction typically hits the S3-FIFO probationary queue (~100ns) rather than making an S3 round-trip.

### Crash Recovery

If a crash occurs mid-flush, the flushing file persists on disk. On startup:

1. `load_metadata()` converts SYNCING→DIRTY (conservative)
2. If `{name}.flushing` exists: copy all DIRTY block data from flushing file to active file
3. Delete flushing file

**Invariant**: after recovery, no flushing file exists. All blocks are DIRTY (in active) or NOT_PRESENT (in S3).

### ublk Interaction

The io_uring registered fd (captured at device startup) becomes stale after rotation. `flushing_active: AtomicBool` causes `resolve_read_plan` to skip the `LocalSsd` fast path during flush, falling back to pread via `data_file.read()` (correct fd). Outside flush, zero overhead.

## Block State Machine

Stored in `SparseStateMap` — a sparse page table with 2-bit packed `AtomicU8` values (4 entries per byte), fully lock-free. Encoding: `NotPresent=0, Clean=1, Dirty=2, Syncing=3`. NotPresent=0 means unallocated pages (and zero bytes within allocated pages) are implicitly "never written" with no memory cost.

```
NotPresent ──[write]──► Dirty ──[flush claim]──► Syncing ──[upload OK]──► NotPresent
     ▲                     ▲                         │                    (evict from SSD)
     │                     └──────[write]────────────┘ (CAS SYNCING→DIRTY)
```

The flush path uses **SYNCING-based CAS**: a dirty block is atomically claimed (`CAS DIRTY→SYNCING`) before CPU work begins. After upload, `CAS SYNCING→NOT_PRESENT` evicts it from local SSD. If a guest write lands during the flush, `transition_to_dirty()` CAS-loops `SYNCING→DIRTY` — the flush's final CAS then fails, and the block stays Dirty for the next cycle.

| From | Event | To | Encoding | Notes |
|------|-------|----|----------|-------|
| NotPresent | Guest write | Dirty | 0 → 1 → 2 | `set_present` (CAS 0→1) + SSD pwrite + `transition_to_dirty` (CAS 1→2) + WAL append |
| Clean | Guest write | Dirty | 1 → 2 | Legacy: CLEAN blocks migrated to NOT_PRESENT on startup; this transition only during migration window |
| Dirty | Flush claim | Syncing | 2 → 3 | `transition_dirty_to_syncing()` — atomic snapshot of dirty block |
| Syncing | Concurrent guest write | Dirty | 3 → 2 | `transition_to_dirty()` CAS loop; flush's SYNCING→NOT_PRESENT will fail |
| Syncing | Upload success, no concurrent write | NotPresent | 3 → 0 | `transition_syncing_to_not_present()` — evict from SSD |
| Syncing | Upload success, concurrent write | Dirty | — | Final CAS fails; block stays dirty (re-flushed next cycle) |
| Any | Crash recovery load | Dirty | — | `load_metadata()` converts Syncing→Dirty on startup |

Presence is derived: `is_present = state != 0`. This eliminates a separate `present_chunks` bitmap. (`block_map.rs:SparseStateMap`)

## Fork Path

Forking a VM copies the VolumeManifest — a compact binary blob mapping chunk indices to pack ID lists. That's 2 S3 operations (GET parent manifest, PUT fork manifest). No block data is copied, no local metadata is duplicated.

```
router.create_export(config, fork_from="parent-vm")
    │
    ├── ContentStore::get_manifest("parent-vm")    ← GET manifests/parent-vm (~53KB for 1TB)
    ├── VolumeManifest::deserialize()              ← parse binary GLVM
    ├── VolumeManifest held in memory              ← NOT uploaded to S3 at create time
    └── WriteCache::open_fresh_active(config)      ← empty SparseStateMap (all NotPresent)
    │
    ▼
Fork manifest uploaded to S3 on first flush cycle (sync_manifest)
or explicit drain/snapshot. Crash before first flush → re-fork from parent.
    │
    ▼
Fork is live:
  - Writes: set_present + pwrite to fork's local SSD; flushed as new GLPK packs
  - Reads for blocks never written by the fork (is_present = false):
      VolumeManifest lookup (pack_ids) → PackIndexCache → S3 range GET from parent's packs
```

**Content sharing via PackIndexCache**: The cache is keyed by `PackId`. A fork that hasn't modified a chunk shares the same pack_ids as the parent — those pack indices load once into the cache and are shared across all forks on the host. 180 forks from the same base image load each chunk's pack index once.

**No in-memory overlay**: Forks don't need a `ForkedBlockMap` because reads fall through to S3 via the VolumeManifest — the parent's pack files are still in S3 under their original `chunks/` paths.

## Snapshots

A snapshot is an explicit, versioned copy of an export's VolumeManifest stored at a stable S3 key. Unlike background syncs (which continuously overwrite `manifests/{name}`), snapshots accumulate and are never overwritten by the background flush path.

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
    ├── 1. flush_dirty_inner()          flush all dirty blocks → S3 packs
    ├── 2. put_manifest()               overwrite manifests/{name} (current state)
    ├── 3. put_snapshot()               write snapshots/{name}/{seq:020}  ← best-effort
    └── 4. checkpoint()                 persist block_states + max_seq, truncate WAL
```

Step 3 is best-effort. If the versioned snapshot upload fails, the base manifest (step 2) is already consistent.

### GC and Snapshot Packs

After compaction, old pack_ids are removed from the live manifest via `replace_packs`. If a snapshot was taken before compaction, that snapshot's manifest still references the old pack_ids. GC handles this with a two-pass algorithm that keeps memory independent of snapshot count:

```
GC reconcile_prefix():
  Pass 1a: Load live manifests/{name}        → live_packs via all_pack_ids()
  Pass 1b: Stream .pack files from S3        → classify:
           in live_packs? → revive if previously dead
           not in live_packs? → maybe_dead set
           drop(live_packs)                  ← free memory before pass 2

  Pass 2:  Stream snapshot manifests one at a time:
           for each: remove referenced packs from maybe_dead, then drop manifest
           early exit if maybe_dead empties
           if any snapshot fails to load → abort, delete nothing

  Survivors in maybe_dead are truly dead → grace period check → delete
```

**Deleting a snapshot unpins its exclusive packs** — packs not referenced by any other snapshot or the current manifest become orphaned and eligible for GC after the grace period. Snapshot lifecycle is managed by the orchestrator, not GC.

### Fork from Snapshot

```
PUT /api/exports/fork-vm
    { "manifest_name": "prod-vm", "snapshot_sequence": 42, "size_gb": 10 }
    │
    ▼
router.create_export(config, readonly=false, manifest_name=Some("prod-vm"), snapshot_sequence=Some(42))
    │
    ├── content_store.get_snapshot("prod-vm", 42)   ← GET snapshots/prod-vm/00000000000000000042
    ├── VolumeManifest::deserialize()
    ├── ContentStore::put_manifest("fork-vm", ...)  ← PUT manifests/fork-vm
    └── WriteCache::open_fresh_active(config)        ← empty local block map
```

### Snapshot Invariants

| Invariant | Mechanism |
|-----------|-----------|
| Background sync never creates snapshot keys | `sync_manifest()` only writes to `manifests/{name}` |
| Snapshots accumulate | Each snapshot has unique `{seq:020}` key; deleted only by the orchestrator via `DELETE /api/exports/{name}/snapshots/{seq}` |
| Fork reads parent blocks | VolumeManifest → PackIndexCache → S3 range GET on parent's pack files |
| Snapshot deletion is idempotent | S3 NotFound on delete returns Ok |
| Non-purge remove preserves snapshots | `remove_export(name, purge=false)` does not call `delete_all_snapshots()` |
| Snapshot on empty export succeeds | Returns sequence=0 and an empty VolumeManifest |

(`write_cache/flush.rs:snapshot`, `content_store.rs`, `tests/integration/snapshots.rs`)

## Device Lifecycle (Typestate)

Compile-time enforcement via Rust's typestate pattern. `WriteCache<S>` is generic over a sealed state marker; only `WriteCache<Active>` exposes read/write/flush methods.

```
WriteCache<Initializing>
         │
         │  load local cache, scan WAL
         ▼
WriteCache<Recovering>
         │
         │  verify dirty blocks readable from SSD, compute CRC32 baselines
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

(`state.rs`, `write_cache/mod.rs`)

## Wire Formats

### Pack Format (GLPK)

Self-describing S3 object. Each pack is scoped to one volume chunk. The block index is a **footer** — enabling streaming writes (blocks stream to S3 as they're produced; index is appended at the end) and suffix-only reads when fetching the index.

```
┌─────────────────────────── Pack ───────────────────────────┐
│ Header (16 bytes)                                          │
│   magic: "GLPK"  version: u16  block_count: u16            │
│   chunk_size: u32 LE  _reserved: [u8; 4]                   │
├────────────────────────────────────────────────────────────┤
│ Block Data (immediately after header)                      │
│   [LZ4-compressed blocks, concatenated]                    │
│   Offsets in index are absolute from pack start            │
├────────────────────────────────────────────────────────────┤
│ Block Index footer (28 bytes × block_count)                │
│   [hash:16][chunk_offset:u32 LE][offset:u32 LE][comp_length:u32 LE]
│   Sorted by chunk_offset for binary search                 │
├────────────────────────────────────────────────────────────┤
│ Trailer (8 bytes)                                          │
│   block_count: u16 LE  _reserved: [u8; 2]  magic: "GLIX"   │
└────────────────────────────────────────────────────────────┘
```

- `hash`: BLAKE3-128 of the uncompressed block (16 bytes)
- `chunk_offset`: block's position within its chunk (0–1023 for 128 MiB / 128 KB)
- `offset`: absolute byte offset from start of pack to the compressed block data
- `comp_length`: compressed size in bytes

S3 key: `chunks/{chunk_idx:04}/{pack_id:016x}.pack` (`content_store.rs:stream_chunk_pack`)

**Streaming upload**: `ContentStore::stream_chunk_pack()` uses `object_store::WriteMultipart` — blocks are written to the multipart upload as they're produced. Only ~5 MB (one multipart part) is buffered at a time; each block `Vec<u8>` is freed immediately after `writer.put()`. Index (~28 KB) and trailer (8 bytes) are written last. (`pack.rs:stream_pack_to_writer`)

**Fetching the pack index**: `ContentStore::get_pack_index()` suffix-reads the last `1,024 × 28 + 8 = 28,680` bytes. `parse_pack_index` validates the `GLIX` trailer magic, reads `block_count`, then parses the preceding `block_count × 28` bytes as index entries. One S3 round trip, independent of pack size. (`content_store.rs:get_pack_index`, `pack.rs:parse_pack_index`)

### VolumeManifest (GLVM)

Compact binary format mapping chunk indices to ordered pack ID lists. CRC32-protected. S3 key: `manifests/{export_name}`.

```
Header (32 bytes):
  magic:        [u8; 4]  = b"GLVM"
  version:      u16 LE   = 5
  chunk_count:  u32 LE   (number of sparse chunk entries)
  chunk_size:   u32 LE   (bytes per chunk, 134217728 = 128 MiB)
  block_size:   u32 LE   (131072 = 128 KB)
  device_size:  u64 LE
  reserved:     [u8; 6]

Chunk entries (chunk_count entries, sorted by chunk_idx):
  chunk_idx:    u32 LE
  pack_count:   u16 LE   (1–65535, typically 1–16 before compaction, 1 after)
  packs:        [u64 LE; pack_count]  (pack IDs, ordered oldest-to-newest)

CRC32 trailer: 4 bytes
```

Sparse: only chunks with written data appear. Absent chunk = all-zero / unwritten.

`VolumeManifest::all_pack_ids()` returns `HashSet<(u32, PackId)>` — GC uses the composite `(chunk_idx, pack_id)` key, not pack_id alone, since the same 64-bit value can theoretically appear in different chunks. (`volume_manifest.rs`)

### WAL Entry Format

Append-only on local SSD. Metadata only — block data lives in the cache file.

```
[block_index:u64][sequence:u64][crc32:u32]   (20 bytes per entry)
```

CRC32 trailer detects torn writes. On recovery, replay stops at the first corrupt entry — the torn tail is discarded, not an error. WAL is truncated after each block map persistence. (`wal.rs`)

## Garbage Collection (`glidefs gc`)

Pack IDs are read directly from binary manifests via `VolumeManifest::all_pack_ids()` — no extra S3 fetches beyond the manifests themselves.

Two-pass algorithm with memory independent of snapshot count:

```
For each export prefix in S3 (16-wide parallel):

  Pass 1a — Build live set from live manifests only:
    Load manifests/{name} → live_packs HashSet<(chunk_idx, pack_id)>
    (bounded by current volume pack count, not snapshot count)

  Pass 1b — Stream S3 pack list, classify against live set:
    Stream chunks/{chunk_idx:04}/*.pack
    In live_packs? → revive if previously marked dead in state file
    Not in live_packs? → add to maybe_dead HashSet
    drop(live_packs)  ← free memory before pass 2

  Pass 2 — Stream snapshot manifests one at a time:
    For each snapshots/{name}/{seq}:
      Deserialize → remove referenced packs from maybe_dead → drop manifest
      Early exit if maybe_dead empties
      If any snapshot fails to load → abort prefix, delete nothing

  Survivors in maybe_dead are truly dead:
    Mark newly-dead with first-seen timestamp in state file
    Check grace period eligibility
    Delete up to max_deletes (32-wide parallel)
```

**Memory**: O(live_manifest_packs + maybe_dead + max_deletes). Snapshot manifests are never held simultaneously — each is deserialized, used to shrink `maybe_dead`, and dropped. 1,000 snapshots use the same memory as 1.

**Grace period**: Dead packs are not deleted immediately. GC records the first-seen-dead timestamp in a local JSON state file (`gc-state.json`). Packs are only eligible for deletion after the grace period (default 24h). This prevents races where a flush uploads a pack but the manifest hasn't been committed yet.

**Snapshot lifecycle**: GC never creates or deletes snapshots. Snapshot retention is the orchestrator's responsibility — `DELETE /api/exports/{name}/snapshots/{seq}`. Once a snapshot is deleted, its exclusive packs become orphaned and eligible for GC after the grace period.

**Safety controls**: `--dry-run` reports without deleting. `--max-deletes` caps deletions per run (default 100,000). Corrupt or unreadable manifests cause the entire prefix to be skipped — no packs deleted when uncertain. (`cli/gc.rs`)

## Background Subsystems

### Scrubber (Integrity Verification)

Rate-limited background task that re-hashes blocks in the CleanCache against their content address. On mismatch (bit rot, memory corruption), evicts the block — the next read re-fetches from S3, which is the authoritative source.

- Rate-limited: `scrubber_blocks_per_second` (default 0 = disabled)
- 60s sleep between full passes
- Prometheus counters: `blocks_checked`, `blocks_evicted`

(`scrubber.rs`)

### Sequential Readahead

Ring buffer (4 entries) tracks recent chunk accesses. When 3+ consecutive chunks are read (boot, large file copy), triggers prefetch of the next pack boundary. This hides S3 latency for sequential workloads. (`readahead.rs`)

### Boot Hot Set Prefetch

When a fork is created from a base image manifest, the router checks S3 for a corresponding `.hot-set` file — a list of block indices that contain non-zero data. If found, a background task prefetches those blocks into the CleanCache before the VM reads them. The hot set is created by `glidefs bless`. (`router.rs:create_export`, `manifest.rs:serialize_hot_set`)

## Data Integrity

Every layer has a verification mechanism. The goal: corruption is detected before it reaches the guest or S3.

### Verification Chain

| Layer | What's Protected | Hash/Check | When Verified | On Failure |
|-------|-----------------|------------|---------------|------------|
| S3 packs | Block data in transit/at rest | BLAKE3-128 | Read path: after S3 fetch + LZ4 decompress | `HashMismatch` error |
| Clean cache (Foyer) | Cached blocks on SSD/memory | BLAKE3-128 | Background scrubber | Evict from cache → re-fetch from S3 |
| VolumeManifest | Chunk pack list root | CRC32 trailer | On deserialization | Reject manifest |
| GLPK pack | Block index + data | BLAKE3-128 per block | On block read from S3 | `HashMismatch` error |
| WAL entries | Per-entry metadata | CRC32 trailer | On replay (crash recovery) | Stop replay, discard torn tail |
| Block state metadata (.meta) | Per-block state + sequence | CRC32 trailer | On load (open/recovery) | Reject file (`InvalidMetadata`) |
| Dirty blocks (SSD) | Block data between write and flush | CRC32 in `SparseCrcMap` | Flush time: before BLAKE3 computation | Skip block (stays dirty), do NOT launder to S3 |

### Dirty Block CRC32

CRC32 is stored in `crc_map: SparseCrcMap` on `CacheInner` — a lock-free two-level page table with `AtomicU32` leaves (1,024 entries per 4KB page, lazily allocated). At checkpoint time (every ~5s), dirty blocks without a CRC get one computed. At flush time, the CRC is verified before BLAKE3 is computed. SYNCING state provides unambiguous discrimination:

- CRC mismatch + state == SYNCING → real SSD corruption → skip block
- CRC mismatch + state != SYNCING → concurrent write re-dirtied the block → stale CRC, not corruption

(`write_cache/flush.rs:compute_dirty_crc32s`, `write_cache/flush.rs:compute_flush_batch`)

### What Is NOT Verified

| Gap | Why Acceptable |
|-----|---------------|
| Dirty block reads (guest reads dirty data from SSD) | Read path returns raw pread data — no checksum. Checkpoint runs every ~5s; the window is small. |
| SSD data file between flush cycles | Once a block is flushed, it's evicted from SSD (NOT_PRESENT). Reads go through CleanCache or S3. Only dirty blocks (not yet flushed) are on SSD. |
| PackIndexCache SSD files | Derived data — rebuildable from S3 pack objects. Foyer corruption falls back to S3 on cache miss. |

## CLI Commands

| Command | Purpose |
|---------|---------|
| `glidefs init [path]` | Generate a default `glidefs.toml` config file |
| `glidefs run -c glidefs.toml` | Start the block server (NBD + optional ublk) with HTTP management API |
| `glidefs bless --image disk.raw --name ubuntu-22.04 --s3-prefix bases -c glidefs.toml` | Convert a raw disk image into a content-addressed base image in S3 |
| `glidefs gc -c glidefs.toml [--dry-run] [--grace-period 24h] [--max-deletes 100000] [--state-file gc-state.json]` | Delete orphaned packs in S3 |

### Bless Pipeline

`glidefs bless` reads a raw disk image sequentially, processes one 128 MiB volume chunk at a time (streaming — never accumulates the full image). For each chunk: deduplicates 128 KB blocks by hash, then `stream_chunk_pack()` uploads the chunk as a GLPK pack via `WriteMultipart` — the previous chunk's upload runs concurrently with the next chunk's disk reads (one upload in flight at a time). Builds a GLVM VolumeManifest from the uploaded pack IDs. Output: VolumeManifest at `exports/{s3_prefix}/manifests/bases/{name}` and a hot set at `exports/{s3_prefix}/manifests/bases/{name}.hot-set`. (`cli/bless.rs`)

## Management API

HTTP REST API for orchestrators (scale-to-zero, live migration). (`api.rs`)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/exports/{name}` | `PUT` | Create or resize export (idempotent). With `manifest_name` + optional `snapshot_sequence`: fork from parent or specific snapshot. |
| `/api/exports/{name}` | `GET` | Get export info (size, readonly, transport, device path) |
| `/api/exports/{name}` | `DELETE` | Remove export. `?purge=true` also deletes local cache and all S3 snapshots. |
| `/api/exports` | `GET` | List all active exports |
| `/api/exports/{name}/snapshot` | `POST` | Flush dirty blocks → S3, upload versioned manifest. Optional body `{"tag":"name"}` also publishes named alias. Returns `{sequence, manifest_etag}`. |
| `/api/exports/{name}/snapshots` | `GET` | List snapshot sequences in ascending order |
| `/api/exports/{name}/snapshots/{seq}` | `DELETE` | Delete a specific snapshot (idempotent) |
| `/api/exports/{name}/tag` | `POST` | Publish current manifest under a named alias without re-flushing. Body: `{"tag":"name"}`. |
| `/api/manifests/{s3_prefix}/{name}` | `HEAD` | Check manifest existence (200/404). No data transfer, no running export required. |
| `/api/exports/{name}/drain` | `POST` | Flush all dirty blocks to S3 (no versioned snapshot) |
| `/api/exports/{name}/promote` | `POST` | Toggle readonly → read-write |
| `/api/exports/{name}/metrics` | `GET` | Per-export metrics snapshot (JSON) |
| `/metrics` | `GET` | Prometheus scrape endpoint (all exports) |
| `/health` | `GET` | Liveness probe (always 200) |
| `/health/ready` | `GET` | Readiness probe (200 if all exports healthy) |

### Export Persistence & Discovery

Export definitions are saved to S3 as `{db_path}/exports/{name}/export.json`. On startup, `discover_exports()` lists all `export.json` files under the `exports/` prefix and loads them 32-wide parallel, then `create_export()` recovers each from local WAL 16-wide parallel. No S3 writes on the recovery path. (`router.rs:save_export`, `router.rs:discover_exports`, `cli/server.rs`)

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

## Package Structure

| File | Purpose |
|------|---------|
| `block/pack.rs` | GLPK format: `stream_pack_to_writer`, `assemble_pack` (tests/bench), `parse_pack_index` (suffix), `PackId = u64`, `PackIndexEntry` |
| `block/volume_manifest.rs` | Binary GLVM: `VolumeManifest`, `ChunkEntry`, `append_pack`, `replace_packs`, `all_pack_ids`, CRC32 |
| `block/pack_index_cache.rs` | `PackIndexCache`: Foyer HybridCache keyed by `PackId`; `lookup_block`, `insert_entries`, `known_hashes` |
| `block/content_store.rs` | S3 typed I/O: `stream_chunk_pack` (WriteMultipart), `get_chunk_block`, `get_pack_index` (suffix-read), manifests, snapshots |
| `block/write_cache/flush.rs` | Flush orchestration: CAS claim, rayon compute, per-chunk GLPK streaming upload, manifest append, inline compaction trigger |
| `block/write_cache/compact.rs` | Inline compaction: merge N delta packs → 1 base pack via `stream_chunk_pack`; `compact_chunk`, `compact_if_needed` |
| `block/write_cache/read.rs` | Read path: `resolve_block` (SSD → PackIndexCache → parallel prefetch → S3); `prefetch_chunk` |
| `block/write_cache/mod.rs` | `WriteCache<S>` typestate; `FlushStats`, `SnapshotResult` |
| `block/write_cache/write.rs` | Write path: `set_present` + `pwrite` + `transition_to_dirty` + WAL append |
| `block/write_cache/init.rs` | Cache initialization: `open()` (Initializing→Recovering), `open_fresh_active()` (forks) |
| `block/write_cache/inner.rs` | `CacheInner`: shared state (data file, state map, WAL, sequence, CRC map) |
| `block/write_cache/recovery.rs` | Crash recovery: Syncing→Dirty on startup |
| `block/block_map.rs` | `SparseStateMap`, `SparseCrcMap`, `Blake3Hash`, `blake3_128`, `lz4_compress`, `lz4_decompress` |
| `block/cache.rs` | `BlockCache` trait (CleanCache) + Foyer implementation |
| `block/flush_scheduler.rs` | Per-export flush scheduling: event-driven (dirty count threshold), periodic checkpoint |
| `block/handler.rs` | `BlockHandler`: transport-agnostic read/write/flush/trim/write_zeroes |
| `block/manifest.rs` | S3 key helpers: `manifest_s3_key`, `snapshot_s3_key` |
| `cli/gc.rs` | GC: two-pass algorithm (live manifests → stream S3 → stream snapshots), grace period, max-delete cap |
| `cli/bless.rs` | Base image preparation: raw disk → GLPK packs + GLVM manifest + hot set |
| `router.rs` | `ExportRouter`: export lifecycle, fork, snapshot, drain |
| `circuit_breaker.rs` | Lock-free S3 circuit breaker (`AtomicU64` packed state) |
| `api.rs` | HTTP REST API handlers |
| `metrics.rs` | Per-export Prometheus metrics |
| `scrubber.rs` | Background hash verification for CleanCache entries |
| `readahead.rs` | Sequential read detection and pack index prefetch |
| `wal.rs` | Append-only WAL with CRC32 per entry |

## Design Decisions

### Why footer index?

With a header index, all compressed block sizes must be known before writing the first byte — so the entire pack (~30–60 MB) must be buffered in memory before upload. With N concurrent flushes, this scales linearly.

With a footer index, blocks stream to S3 as they're produced via `WriteMultipart`. Only ~5 MB (one multipart part) is buffered at a time; each block `Vec<u8>` is freed immediately after `writer.put()`. The index (≤28 KB) and trailer (8 bytes) are written last.

### Why NBD + ublk instead of just one transport?

NBD is cross-platform and battle-tested — it works on macOS (dev), Linux (prod), and anywhere with a TCP stack. ublk eliminates socket overhead on Linux 6.0+: io_uring shared memory, fixed mmap'd descriptors, native multi-queue. Benchmarks show 2-3x IOPS improvement for random 4K reads.

The storage layer (`BlockHandler`, `WriteCache`, `ContentStore`) is transport-agnostic — both frontends call the same 6 methods. NBD remains the default; ublk is opt-in for production Linux deployments where per-I/O overhead matters.

### Why 128 MiB chunks?

128 MiB = 1 ext4 block group. This matters for database workloads:

- PostgreSQL `shared_buffers` default = 128 MB → fits in 1 chunk
- ext4 allocates inodes and bitmaps per block group → heavy metadata scatter is bounded to 1–2 chunks per flush
- 1 TB device = 8,192 chunks → manifest has 8,192 × (4+1+8) = ~107 KB entries fully written (compacted)

We considered 64 MiB (2x more chunks, smaller manifests) but 128 MiB better matches ext4 block group alignment. Cross-chunk packs were analyzed and rejected ($3.60/mo savings for significant reference-tracking complexity).

### Why binary GLVM instead of JSON manifest?

Binary GLVM maps `chunk_idx → [pack_id, ...]` directly, so:

1. **GC reads zero extra objects**: `manifest.all_pack_ids()` returns all live `(chunk_idx, pack_id)` pairs from a single compact blob (~57 KB for 1 TB 50% written)
2. **Forks are instant**: GET parent GLVM + PUT fork GLVM = 2 S3 ops
3. **CRC32 integrity**: Corrupt manifests are detected at deserialization
4. **Compact at scale**: 4 packs/chunk = ~155 KB. Still fits in a single S3 GET.

We considered JSON but it becomes verbose and hard to parse efficiently at thousands of pack entries. Binary is ~2x smaller and an order of magnitude faster to serialize/deserialize.

### Why u64 PackId instead of UUID?

1. **Half the size**: 8 bytes vs 16 bytes per pack ID entry in the manifest
2. **S3 key uniformity**: `{pack_id:016x}` distributes uniformly across S3 prefixes without additional hashing
3. **Collision safety**: Birthday bound ~4.3 billion per chunk. A chunk sees hundreds of pack IDs over its lifetime — nowhere near the bound. GC uses `(chunk_idx, pack_id)` composite keys so the same `u64` in different chunks is not confused.

### Why write-behind instead of write-through?

S3 PUT latency is 50-200ms. Write-through would make snapshots take 5-15 seconds instead of <100ms.

Write-behind trades durability for latency: data between the last FLUSH and the next S3 sync is at risk if the host dies. This is acceptable because:

1. Pack-size flush keeps the dirty window small (flush triggers at 500 dirty blocks = ~64 MB per export)
2. SIGTERM triggers a drain before exit
3. The workload (microVMs) is ephemeral — VMs can be recreated from base images
4. Max dirty data node-wide is bounded: 499 blocks × 128KB × 2000 exports ≈ 122 GB

### Why defer hashing to flush time?

An earlier design computed BLAKE3 on every write. For a 4KB write to a 128KB block:

| Operation | Cost |
|-----------|------|
| pread 128KB from SSD | ~15-25µs |
| blake3(128KB) | ~20-30µs |
| Bytes::copy_from_slice(128KB) | ~5-10µs |
| **Total overhead** | **~50-65µs** |

None of this work is needed until flush time. With deferred hashing, the write path is ~5µs. **Write coalescing is free**: write the same block 100 times before flush, hash it once. Previously: 100 hashes, 99 thrown away.

### Why no inline pack deletion during compaction?

After compaction calls `replace_packs(chunk_idx, [new_pack_id])`, the old pack_ids are no longer in the live manifest. GC will find them as dead on the next run.

We considered inline deletion (delete old packs immediately after `replace_packs`), but rejected it:

1. **Redundant with GC**: GC already handles orphaned packs safely. Inline deletion duplicates that logic with fewer safety mechanisms.
2. **No grace period**: GC has a configurable grace period (default 24h). Inline deletion would immediately delete packs that a recently-uploaded-but-not-yet-visible manifest might reference.
3. **No max-delete cap**: GC limits deletions per run. Inline deletion on a heavily-fragmented volume could issue thousands of DELETE requests synchronously on the flush path.
4. **Snapshot safety**: Old packs may be pinned by snapshots. Safe inline deletion would require checking all snapshot manifests per compaction. GC already does this efficiently with its two-pass streaming algorithm, amortized across all exports.

The insight: compaction's job is to reorganize pack layout and update the manifest. Cleanup is GC's job.

### Why chunk-scoped packs instead of cross-chunk packs?

Cross-chunk packs would allow a single pack to contain blocks from multiple chunks, potentially improving compression ratios and reducing pack count. We analyzed and rejected this:

1. **Reference tracking complexity**: GC must track which chunks reference each pack. With chunk-scoped packs, the composite key is `(chunk_idx, pack_id)` and S3 directory listing is per-chunk. Cross-chunk packs require a separate index mapping pack_id → chunk_ids.
2. **Compaction complexity**: Compaction must read all referenced chunks to decide which blocks to include in the new base pack. Chunk-scoped compaction is embarrassingly parallel; cross-chunk is not.
3. **Savings are small**: Estimated savings at scale: $3.60/mo — not worth the complexity.

### Why content-addressed packs instead of per-block S3 objects?

Per-block storage means one S3 PUT per 128KB write. At 28K IOPS, that's 28K PUTs/second — prohibitively expensive.

Packing multiple blocks per S3 object reduces PUTs by up to 500x. Content addressing (BLAKE3-128 hash) enables within-batch deduplication without coordination — identical blocks in the same flush batch are compressed once and shared across entries.

### Why BLAKE3-128 instead of full BLAKE3-256?

128-bit collision resistance is sufficient for content deduplication (birthday bound: 2^64 operations). 16 bytes fits in two `u64`s and is compact in pack index entries. Halves per-entry metadata cost vs full 256-bit hash.

### Why typestate instead of runtime state checks?

Compile-time prevention of invalid operations. You literally cannot call `write()` on a `WriteCache<Recovering>` — the method doesn't exist for that type parameter. No runtime cost, no forgotten state checks.

### Why a lock-free circuit breaker?

S3 outages shouldn't cascade into mutex contention on the hot path. All circuit breaker state is packed into a single `AtomicU64` — no locks, no multi-variable coordination. CAS loops guarantee consistent state transitions under high concurrency.

We use a consecutive-failure policy (not windowed) by default because S3 outages tend to be total, not partial.

### Why sparse page tables instead of dense arrays?

Dense arrays pre-allocate for all blocks: a 1TB export with 128KB blocks has 8M entries. At 1 byte per block state, that's 8MB per export — manageable. But at 10,000 exports: 80GB just for state. An empty export with a dense array wastes the same memory as a full one.

Sparse page tables allocate on first write. With 2-bit packing (4 entries per `AtomicU8`), each 4KB page holds 16,384 entries. The directory (one pointer per page) costs ~4KB for a 1TB export. An empty export: ~4KB. A fully-written 1TB export: ~2MB. The cost is one extra pointer dereference on the hot path — a branch that predicts correctly almost every time.

### Why explicit versioned snapshots instead of manifest history?

Background syncs continuously overwrite `manifests/{name}` — there's no history there. Versioned snapshots need stable, immutable S3 keys that accumulate over time without being overwritten.

Each snapshot gets a unique `snapshots/{name}/{seq:020}` key. The sequence is zero-padded so S3 LIST returns results in chronological order. Background sync never writes to the `snapshots/` prefix — it's a separate namespace.

This also means snapshot retention is the orchestrator's responsibility: each versioned key exists until explicitly deleted via the management API. GC streams snapshot manifests to check for pinned packs — packs referenced by any snapshot are preserved, with memory independent of snapshot count.
