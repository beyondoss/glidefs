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

## Suggested Verifications

### Unit Tests — Block Resolution

- **`test_read_from_dirty_store`**: Write a block (dirty, not flushed). Read it back. Data comes from dirty store. No S3 call. Verify data matches.
- **`test_read_from_clean_cache`**: Write a block, flush to S3, verify it moved to clean cache. Read it back. Data comes from cache. No S3 call.
- **`test_read_zero_block`**: Read an offset that was never written. Returns all zeros. No S3 call. No cache lookup (short-circuit on zero-block hash).
- **`test_read_sub_chunk`**: Write a full 128KB chunk. Read only bytes [4096..8192] (4KB slice from the middle). Returns correct 4KB slice.
- **`test_read_triggers_s3_fetch`**: Write blocks, flush, clear local cache AND dirty store (simulate cross-host wake). Read a block. Verify S3 GET is made. Data is correct.

### Integration Tests — Pack Prefetch

- **`test_pack_prefetch_warms_siblings`**: Flush 25 blocks (one pack). Clear local cache. Read block 0 (cache miss, fetches pack from S3). Immediately read blocks 1-24 — all should be cache hits (no additional S3 calls). Count total S3 GETs: exactly 1.
- **`test_pack_prefetch_different_packs`**: Flush 50 blocks (two packs). Clear cache. Read block 0 (fetches pack 1). Read block 25 (fetches pack 2). Total S3 GETs: 2. Then read blocks 1-24 and 26-49 — all cache hits.

### Integration Tests — Hash Verification

- **`test_s3_ingestion_verifies_hash`**: Flush blocks to S3. Corrupt a block in S3 (flip a byte in the pack data). Clear local cache. Read the corrupted block. Verify the read path detects the BLAKE3 mismatch and returns an error (not silently corrupt data).
- **`test_valid_pack_passes_verification`**: Flush blocks normally. Clear cache. Read back. All hash verifications pass. Data matches original writes.

### End-to-End Tests

- **`test_full_read_write_cycle`**: Write 200 blocks with known patterns. Flush. Clear all local state (simulate restart on new host). Load manifest from S3. Read all 200 blocks back. Every block matches the original data.
- **`test_mixed_dirty_and_clean_reads`**: Write 50 blocks. Flush. Write 50 more blocks (dirty). Read all 100 blocks. First 50 come from cache (flushed). Last 50 come from dirty store (not flushed). All data correct.
- **`test_nbd_read_write_through_v2_path`**: Full NBD protocol test. Connect NBD client, write blocks via NBD_CMD_WRITE, read back via NBD_CMD_READ. Data matches. This verifies the entire stack from protocol to block resolution.

### Performance Assertions

- **`test_dirty_store_read_latency`**: Write 1000 blocks. Read each from dirty store. Assert p99 latency < 1us (in-memory HashMap lookup).
- **`test_cache_hit_latency`**: Populate cache with 1000 blocks. Read each. Assert p99 latency < 10us for in-memory cache (will be ~100ns in production, but test overhead is higher).

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
