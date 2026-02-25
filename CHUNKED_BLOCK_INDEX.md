# Chunked Block Index Design

**Status**: Proposal
**Date**: 2026-02-24

## Motivation

The current manifest is a flat binary blob containing every block entry and pack reference for the entire volume. This creates three scaling issues as volume sizes grow:

1. **Delta complexity.** Background sync uploads a delta manifest (changed blocks since last full sync) to avoid re-uploading the full manifest on every flush. This requires staleness checking (`base_sequence` must match), compaction heuristics (50% threshold, `syncs_since_base >= 10`), merge logic on restore (`get_effective_manifest`), and conservative GC (union of base + delta packs). The delta mechanism works, but adds code and edge cases for a problem that disappears if manifests are small.

2. **Fork is not atomic.** Forking from live state requires two S3 GETs (base manifest + optional delta) and a merge step. If the base is updated between the two GETs, the delta may be stale. The code handles this correctly (stale deltas are ignored), but the fork may not reflect the latest flush.

3. **Parent block map is dense.** `ForkedBlockMap` holds an `Arc<BlockMap>` where `BlockMap` is a dense `Vec<BlockMapEntry>` covering the full device. A 256GB volume allocates 2M entries × 32 bytes = 64MB per parent regardless of how much data has been written.

4. **Metadata doesn't scale to unlimited storage.** If the goal is to offer arbitrarily large volumes, the current design's dense parent block map grows linearly with device size (not written data), and the flat manifest grows linearly with written data. Both must fit in memory or be downloaded in full for fork/restore operations.

### Why not just delete deltas

Deleting the delta code and always uploading the full manifest achieves goals #1 and #2 with zero new abstractions. For 256GB volumes with 10-30GB of written data, manifests are 5-15MB — trivially small. This is the minimum effective move and a viable intermediate step.

The chunked design goes further: it solves #3 (sparse parent memory), #4 (unlimited storage), enables parallel per-chunk flush, eliminates `HostPackIndex` (redb), and provides a foundation that scales to any volume size without manifest growth.

## Design

### S3 Layout

```
{db_path}/
├── exports/{export_name}/
│   ├── export.json                          ← Export definition (unchanged)
│   ├── manifests/
│   │   ├── {export_name}                    ← Volume manifest (chunk_idx → chunk_hash map, ~1KB)
│   │   └── bases/
│   │       ├── {image_name}                 ← Base image manifest (from bless)
│   │       └── {image_name}.hot-set         ← Boot hot set (chunk indices)
│   ├── snapshots/
│   │   └── {export_name}/
│   │       └── {sequence:020}               ← Versioned snapshot (same format as manifest, ~1KB)
│   └── chunks/
│       └── {chunk_idx}/
│           ├── {chunk_hash}.meta            ← Block map + pack locations for this chunk state
│           └── {pack_uuid}.pack             ← Incremental packs, shared across chunk states
```

Compared to the current layout:
- `manifests/{export_name}` changes from flat GLDE binary to a chunk_idx → chunk_hash map (~1KB)
- `manifests/{export_name}.delta` is removed (no deltas)
- `packs/` moves from `packs/{hex-prefix}/{uuid}` to `chunks/{chunk_idx}/{uuid}.pack` (packs scoped to chunk)
- `pack-registries/` is removed (GC uses chunk .meta pack lists instead)
- `chunks/` is new — contains chunk state .meta files and their packs

Forks use the parent's `s3_prefix` (same as the current design — forks share the parent's S3 namespace).

### Volume Manifest

```json
{
  "size": 274877906944,
  "version": 3,
  "chunk_size": 10737418240,
  "block_size": 131072,
  "chunks": {
    "0": "a1b2c3d4e5f6...",
    "1": "f6e5d4c3b2a1...",
    "5": "1234abcd5678..."
  }
}
```

~1KB. Volume size, format version, and a sparse map of chunk index to chunk hash. Unwritten chunk indices are absent.

**Resize** is a manifest-only operation: update the `size` field, PUT the manifest. New block ranges become addressable immediately. No reallocation, no copying, no coordination with readers. Shrinking orphans chunks beyond the new boundary; GC cleans them up.

### Chunk State (.meta)

A chunk covers a fixed range of blocks. At 10GB per chunk with 128KB blocks, each chunk covers 81,920 blocks (LBA 0–81,919 for chunk 0, 81,920–163,839 for chunk 1, etc.).

The `chunk_hash` is the content hash (BLAKE3-128) of the block map — the sorted `(offset, block_hash)` pairs. The `.meta` file at that hash is a **self-contained, flat binary array** of fixed-size entries:

```
Header:
  magic: "GLCM"          (4 bytes)
  version: u16 LE         (2 bytes)
  chunk_idx: u32 LE       (4 bytes)
  block_count: u32 LE     (4 bytes, number of non-zero blocks)
  chunk_size: u32 LE      (4 bytes, bytes per chunk, e.g. 10GB)
  block_size: u32 LE      (4 bytes, e.g. 131072)
  reserved: [u8; 10]      (10 bytes)

Entry array (48 bytes × block_count, sorted by offset):
  offset:       u32 LE     (block index within chunk, 0–81919)
  hash:         [u8; 16]   (BLAKE3-128 of uncompressed block)
  pack_id:      [u8; 16]   (UUID of pack containing this block)
  pack_offset:  u32 LE     (byte offset within pack)
  comp_length:  u32 LE     (compressed size in bytes)

Trailing CRC32: 4 bytes
```

Each entry is **self-contained**: given a block offset, you can resolve it to a pack location in a single lookup. No external index needed.

Worst-case .meta size (all 81,920 blocks written): 32 (header) + 81,920 × 48 (entries) + 4 (CRC32) = **3.8MB**.

### Pack Location Resolution (Replaces HostPackIndex)

The current design uses `HostPackIndex` (redb, on-disk B-tree) to resolve block hashes to pack locations. The chunked design eliminates redb entirely. The .meta IS the index.

**Tiered caching** (same pattern as block data):

```
Read path for metadata:
  Memory (LRU cache of hot chunk .metas)     → ~100ns
  Local SSD (cached .meta flat files)        → ~100µs (single seek, read 48 bytes)
  S3 (fetch .meta on first access)           → ~20ms
```

The .meta is a flat array of fixed-size entries. On local SSD, resolving a block is a positional read: seek to `entry_index × 48`, read 48 bytes. No B-tree traversal, no database machinery.

**Comparison with redb:**

| | redb (HostPackIndex) | Cached .meta on SSD |
|---|---|---|
| Per entry | ~80-100 bytes (key + value + B-tree overhead) | 48 bytes (flat array) |
| Lookup | B-tree traversal (multiple pages) | Single positional read |
| 10GB chunk (81,920 entries) | ~6.5-8MB | 3.8MB |
| Content-addressed sharing | No (mutable, host-local) | Yes (same chunk hash = same .meta file) |
| Crash recovery | Rebuild from manifests | Re-fetch from S3 (or already cached) |
| Compaction | Needed (redb internal) | Not needed (immutable flat files) |

**Memory scaling:**

| Volume usage | Chunks in memory (hot) | Memory | Current (dense parent + redb page cache) |
|---|---|---|---|
| 10GB written | 1 | 3.8MB | 64MB+ |
| 30GB written | 3 | 11.4MB | 64MB+ |
| 100GB written | 10 (or fewer if cold) | 38MB | 64MB+ |
| 1TB written | working set only | O(hot chunks) | 256MB+ |

Memory is **O(working set)**, not O(device size) or O(total written). A 10TB volume where the VM actively touches 20GB has 2 chunk .metas hot in memory (~7.6MB). Cold chunks are served from local SSD cache or re-fetched from S3.

SSD footprint for cached .metas: 3.8MB per chunk. A fully-accessed 10TB volume: 1000 chunks × 3.8MB = 3.8GB on local SSD — negligible against a 2TB NVMe.

### Within-Chunk Dedup

During flush, the dedup check is: "is this block hash already in one of this chunk's packs?" Build a transient `HashSet<Blake3Hash>` from the chunk .meta's block hashes for chunks being flushed. Typically 1-2 chunks per flush cycle = ~4-8MB transient memory. Built on flush, dropped after.

No persistent dedup index. No cross-chunk dedup (6% at 128KB — not worth it per BLOCK_SIZE_ANALYSIS.md).

### Packs

Packs are stored alongside `.meta` files under `chunks/{chunk_idx}/`. Pack format (GLPK) is unchanged — same LZ4-compressed blocks, same content-addressed block index. Packs are shared across chunk states within the same chunk index: when a chunk's block map changes, the new `.meta` references the old packs plus any new ones.

Packs are NOT shared across chunk indices. Block content that appears in both chunk 0 and chunk 30 is stored in separate packs. This is acceptable: measured post-fork dedup at 128KB is 6% (BLOCK_SIZE_ANALYSIS.md §Post-fork write dedup). Cross-chunk dedup would recover at most 6% of post-fork writes — not worth the complexity.

The current 256-way prefix sharding (`packs/{hex}/{uuid}`) is no longer needed — packs are naturally partitioned by chunk index.

### Content Addressing

Chunk states are **immutable and content-addressed**. Two volumes with identical block maps for chunk 0 reference the same `{chunk_hash}.meta` and the same packs. This is the dedup mechanism:

- **Base image blocks**: All forks from the same blessed base share chunk states by hash. Zero duplication.
- **Post-fork writes**: Each fork creates new chunk states for modified ranges. No cross-volume dedup for these (same as current design — 6% dedup at 128KB doesn't justify the complexity).

## Operations

### Flush

Dirty blocks: LBA 4200, LBA 4300 (chunk 0), LBA 2000000 (chunk 30).

For chunk 0:
1. Build transient `HashSet<Blake3Hash>` from chunk 0's current .meta (within-chunk dedup)
2. Skip blocks whose hashes already exist in the HashSet
3. Assemble and upload new pack: `exports/{name}/chunks/0000/{uuid-new}.pack`
4. Apply dirty blocks to chunk 0's block map (update entries with new hash + pack location)
5. Hash new block map → `new_chunk_hash`
6. Write `exports/{name}/chunks/0000/{new_chunk_hash}.meta`
7. Update local SSD cache with new .meta; update in-memory LRU cache

For chunk 30: same process, independently. Chunks flush in parallel.

Atomic commit:
```
PUT manifests/{vol}
{ "chunks": { "0": "new_chunk_hash", "1": "unchanged", "30": "new_chunk_hash_30" } }
```

Each chunk flushes independently using the same dedup/pack logic that exists today, just bounded to a fixed-size block range.

**Failure modes**: Any step before the manifest PUT can fail, leaving orphaned packs or .meta files. These are cleaned up by GC. The manifest PUT is the atomic commit point — until it succeeds, the volume's state is unchanged.

### Fork

```
1. GET exports/{parent}/manifests/{parent}       → 1KB manifest with chunk hashes
2. PUT exports/{fork}/manifests/{fork}           → same chunk hashes, new volume name
```

Two S3 operations. The forked volume references the same chunk states and packs as the parent (shared `s3_prefix`). When the fork writes to a block, it creates new packs and chunk states under the shared `chunks/` directory and updates its own manifest. The parent's manifest is untouched.

**Cross-host fork**: GET the 1KB manifest. Chunk .metas are fetched lazily on first read to each chunk range (~3.8MB worst case, ~20ms per chunk). For boot, eagerly fetch chunk 0.

**Same-host fork**: If the parent's chunk .metas are already cached (memory or SSD), the fork shares them (same chunk hash = same cached file). No S3 operations needed beyond the manifest copy.

### Snapshot

A snapshot is a versioned copy of the manifest:

```
PUT exports/{name}/snapshots/{name}/{seq:020}       → 1KB manifest copy
```

Since chunk states are immutable (content-addressed), pinning a manifest pins its chunk hashes, which pins the .meta files, which pin the packs. No data is copied. GC checks snapshot manifests when determining live chunk hashes.

Fork from snapshot:
```
GET exports/{parent}/snapshots/{parent}/{seq:020}   → 1KB manifest
PUT exports/{fork}/manifests/{fork}                 → same chunk hashes
```

### Resize

```
GET manifests/{vol}                                 → current manifest
PUT manifests/{vol}                                 → same manifest with updated "size"
```

One read, one write. Growing: new block ranges become addressable, new chunks created on first write. Shrinking: chunks beyond the new boundary orphaned, GC cleans up. No data structures reallocated. No existing chunks modified.

### Garbage Collection

GC is naturally partitioned by chunk index.

```
For each export:
  1. Read manifest + all snapshots → collect all referenced chunk_hashes per chunk_idx

  For each chunk_idx in exports/{name}/chunks/:
    2. Delete orphaned .meta files:
       - List chunks/{chunk_idx}/*.meta
       - Delete any .meta whose hash is not in the live set (after grace period)
    3. Delete orphaned packs:
       - Union all pack UUIDs referenced by live .meta files → live_packs
       - List chunks/{chunk_idx}/*.pack
       - Delete any pack not in live_packs (after grace period)
```

No global pack registries needed. Each chunk directory is self-contained. If the manifest/snapshot scan fails for an export, skip it rather than risking deletion of live data.

## Tradeoffs

### What you gain

| Gain | Impact |
|---|---|
| No delta mechanism | Eliminates staleness checks, compaction heuristics, merge logic, conservative GC union. Chunk states ARE the incremental mechanism — each .meta is a complete snapshot of its range. |
| Atomic fork | One S3 PUT (1KB manifest). No two-phase GET+merge, no staleness window. |
| Sparse metadata | Memory and SSD proportional to working set, not device size. 10GB written on a 1TB volume: 3.8MB in memory vs 256MB dense parent today. |
| No redb | Eliminates HostPackIndex (on-disk B-tree). .meta flat files are smaller (~half), faster (positional read vs B-tree traversal), immutable, content-addressed, and need no compaction or crash recovery. |
| Manifest decoupled from data volume | ~1KB regardless of volume size or data written. |
| Trivial resize | Update one field in the manifest. No reallocation. |
| Parallel per-chunk flush | Chunks flush independently. Same logic as current per-volume flush, just bounded to a fixed range. |
| Partitioned GC | Each `chunks/{chunk_idx}/` is self-contained. |
| Simpler snapshots | Pin a 1KB manifest instead of copying a multi-MB manifest. |
| Unlimited storage | Metadata scales with working set, not device size. A 10TB volume with 20GB hot data uses ~7.6MB in memory. |

### What you lose

| Loss | Impact |
|---|---|
| Cross-chunk block dedup | Blocks with identical content in different chunks are stored in separate packs. Measured at 6% post-fork dedup at 128KB — negligible (BLOCK_SIZE_ANALYSIS.md). |
| Unbounded pack batching | Current flush batches all dirty blocks into packs regardless of location. Chunked flush scopes packs to their chunk. With 10GB chunks (81,920 blocks), there's ample batching room. Writes spanning a chunk boundary produce two packs instead of one — rare per spatial clustering data (PACK_SIZE_ANALYSIS.md §Spatial clustering). |

### What you pay

| Cost | Notes |
|---|---|
| More S3 PUTs per flush | +1 .meta PUT per modified chunk + 1 manifest PUT. For typical workloads (1-2 chunks modified): +2-3 small PUTs per flush. |
| .meta size | 3.8MB worst case per fully-written chunk (vs 1.2MB with lean .meta + external index). Tradeoff for self-containment and eliminating redb. |
| Chunk .meta fetch on cold start | ~3.8MB per chunk, ~20ms per S3 GET. Cached on local SSD after first fetch. |
| Two-level GC | Must track live chunk hashes AND live packs within chunks. Naturally partitioned, similar total complexity to current GC. |
| Migration effort | Manifest layer rewritten (simpler). Content store gets new S3 layout. Flush partitions dirty blocks by chunk. GC rewritten (partitioned). Fork path simplified. Delta code and redb dependency deleted. Core flush/pack/dedup logic reused. |
| New .meta wire format | Needs testing and validation. Simpler than current GLDE format — flat array of fixed-size entries with CRC32. |

### Neutral

| Factor | Notes |
|---|---|
| Write path | Identical. Local SSD + in-memory block map update. |
| Read path (warm) | Identical. Cache hit → return. |
| S3 storage cost | Same blocks, same packs, same compression. Chunk .metas add ~3.8MB per written 10GB range — negligible. |
| Pack format | Unchanged (GLPK). |

## Migration Path

### Phase 1: Delete deltas (no new abstractions)

Remove the delta manifest code path. Always upload the full manifest on every sync. This achieves two of the three goals immediately:
- No delta complexity (staleness, compaction, merge)
- Atomic fork (single GET, no merge step)

For 256GB volumes with typical 10-30GB of written data, full manifests are 5-15MB. Sub-second uploads. No performance concern.

**Effort**: Delete ~200 lines of delta/compaction code from `flush.rs`, `manifest.rs`, `content_store.rs`. Remove delta-related GC logic. Update tests.

### Phase 2: Chunked block index

Refactor the manifest layer to chunk-based storage. The core flush/pack/dedup logic is reused — the main work is:

1. **manifest.rs**: New format (chunk_idx → chunk_hash map). Simpler than current GLDE format.
2. **content_store.rs**: New S3 layout. Add chunk .meta read/write operations.
3. **flush.rs**: Partition dirty blocks by chunk range before existing per-pack logic. Build transient HashSet for dedup.
4. **router.rs**: Simplify fork path (copy manifest, no block map construction from manifest).
5. **GC**: Rewrite with per-chunk partitioning.
6. **Metadata cache**: Tiered .meta caching (memory LRU + local SSD flat files). Replaces redb HostPackIndex.
7. **Delete**: `ForkedBlockMap` dense parent, `HostPackIndex` (redb), delta manifest code, pack registries.

The in-memory block map (`AtomicBlockMap`, `SparseStateMap`) can remain for the write path — it's already sparse. The change is in how metadata is persisted, loaded, and resolved for reads.

## Open Questions

1. **Chunk .meta sparse vs dense encoding.** The format above uses sparse entries (only non-zero blocks). Alternative: dense array where entry N corresponds to block N within the chunk. Dense is simpler (direct index, no binary search) but wastes space for sparse chunks. At 48 bytes per entry, a dense 10GB chunk is always 3.8MB regardless of fill. Sparse is smaller for lightly-written chunks but requires sorted lookup.

2. **Eager vs lazy chunk loading.** On export open, fetch all chunk .metas immediately (same latency profile as current manifest download) or lazily on first read (faster open, +20ms on first cold read per chunk)? Boot hot-set prefetch suggests eager loading of chunk 0 at minimum.

3. **Chunk .meta caching across volumes.** Content-addressed chunks are shared by hash. The host should maintain a cache of .meta files on local SSD keyed by chunk hash, so multiple forks from the same base don't re-fetch and re-parse the same .meta.

4. **Memory LRU sizing.** How many chunk .metas to keep in memory vs evict to SSD? Could be adaptive based on access frequency, or fixed (e.g., keep the last N accessed chunks per export hot).

5. **Pack lifecycle within a chunk.** Packs accumulate across chunk states. A pack is live if any live .meta references it. Over time, packs may contain mostly dead blocks (overwritten in newer states). Compaction (rewriting a pack to drop dead blocks) may eventually be needed, same as current design.

6. **Chunk size.** 10GB is proposed. This means a 256GB volume has 26 chunks. Tradeoffs: larger chunks = fewer chunks to manage, larger .metas, less granular GC. Smaller chunks = more chunks, smaller .metas, more granular. 10GB aligns with typical base image size, so the blessed base fits in one chunk.

## Measured Data References

All claims in this document are grounded in empirical measurements:

- **Post-fork dedup (6% at 128KB)**: BLOCK_SIZE_ANALYSIS.md §Post-fork write dedup
- **Spatial clustering (56-95% in runs of 5+)**: PACK_SIZE_ANALYSIS.md §Spatial clustering
- **Steady-state write rates (0.1-0.3 blk/s app, 16-21 blk/s postgres)**: PACK_SIZE_ANALYSIS.md §Measured steady-state write rates
- **Deploy churn (1-3 packs per npm install)**: PACK_SIZE_ANALYSIS.md §Measured write volume
- **Full metadata budget (366MB for 600 VMs at 128KB)**: BLOCK_SIZE_ANALYSIS.md §Total Metadata Budget
- **Manifest size (5.2MB per 10GB at 128KB)**: BLOCK_SIZE_ANALYSIS.md §Manifest Size
