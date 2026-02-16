# Phase 3 — New Read Path

**Goal:** Reads resolve through block map -> dirty store -> cache -> S3. Replaces v1's position-based reads with hash-based block resolution.

**Depends on:** Phase 1 (block map for hash lookup).
**Can run in parallel with:** Phase 2.
**Critical path:** Yes.
**Estimated LOC:** ~700 production, ~1,000 tests.

**Design doc references:** [Read Path](../GLIDEv2.md#read-path), [Block Resolution](../GLIDEv2.md#block-resolution), [Cache Design](../GLIDEv2.md#cache-design).

---

## Deliverables

### Block Resolution

The new read path resolves a byte offset through multiple tiers:

```
read(offset, length):
  1. chunk_index = offset / chunk_size
  2. hash = block_map[chunk_index].hash
     if hash == ZERO_BLOCK_HASH:
       return zeros(chunk_size)              // unwritten region

  3. if let Some(data) = dirty_store.get(hash):
       return data[sub_offset..sub_offset+length]   // ~100ns, pinned

  4. if let Some(data) = clean_cache.get(hash):
       return data[sub_offset..sub_offset+length]   // ~100ns mem, ~100us SSD

  5. // S3 cache miss
     pack_loc = pack_index.get(hash)
     pack_data = s3.get(pack_key(pack_loc.pack_id))
     blocks = unpack(pack_data)              // decompress all 25 blocks

     for (block_hash, block_data) in blocks:
       verify blake3_128(block_data) == block_hash
       clean_cache.insert(block_hash, block_data)  // warm 25 entries

     return blocks[hash][sub_offset..sub_offset+length]
```

**Sub-chunk reads.** The VM may read less than a full chunk (e.g., 4KB from a 128KB chunk). The block map resolves the full chunk, the read path returns the requested slice. The full chunk is cached.

### Simple Cache (temporary)

Start with a bounded, hash-keyed cache. foyer integration (Phase 5a) replaces this later.

- Interface: `get(hash: &Blake3Hash) -> Option<Bytes>` / `insert(hash: Blake3Hash, data: Bytes)`.
- Implementation: `HashMap<Blake3Hash, Bytes>` with a size bound and simple eviction (e.g., random eviction when full, or a basic LRU via `linked_hash_map`).
- The interface is the contract — Phase 5a swaps the backend without changing callers.

### Pack-Level Prefetch

On S3 cache miss, fetch the entire pack (25 blocks), decompress all, cache all.

- One cache miss warms 25 entries.
- Temporal locality: blocks written together (same flush cycle) are often accessed together.
- S3 first-byte latency (10-50ms) dominates — the extra ~3MB of transfer is negligible.
- Integrity verification: `blake3_128(decompressed) == expected_hash` for every block in the pack. Reject and re-fetch on mismatch.

---

## What This Unblocks

- **Phase 4** (fork needs both write and read paths working — the forked VM reads through this path)
- **Phase 5a** (foyer replaces the simple cache behind the same interface)

---

## Testable Milestone

1. Write blocks via Phase 1, flush via Phase 2, clear local dirty store. Read back — verify data comes from S3 via pack fetch.
2. Read again — verify cache hit (no S3 call).
3. Verify pack-level prefetch: read one block from a pack, then read a sibling block from the same pack — second read should be a cache hit.
4. Verify hash verification: inject corrupted pack data, confirm the read path detects the mismatch and rejects it.
5. Verify zero-block reads: read an offset that was never written, get zeros.
6. End-to-end: NBD read/write cycle through the full v2 path (write -> flush -> read back).

---

## Key Decisions

- **Simple cache first, foyer later.** The cache interface is trivial (`get`/`insert`). Starting simple avoids pulling in foyer's configuration complexity before we know the read path works correctly. Phase 5a is a mechanical swap.
- **Always fetch the full pack on cache miss.** Fetching individual blocks from a pack would require range reads or a separate block-level S3 layout. Full-pack fetch is simpler, and the prefetch benefit (25 entries per miss) outweighs the extra transfer.
- **Verify on ingestion, not on every read.** Blocks are verified when entering the cache from S3. Cache reads trust local storage. Background scrubber (Phase 7) catches silent bit rot.

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/handler.rs` | **Modification.** Wire new read path (block resolution). |
| `src/nbd/write_cache.rs` | **Modification.** Read method evolves to use block map + dirty store + cache + S3. |
| `src/nbd/cache.rs` | **New.** Simple cache implementation + `BlockCache` trait. |
| `src/nbd/block_store.rs` | **Modification.** Pack reading (unpack all blocks, return map of hash -> data). |
