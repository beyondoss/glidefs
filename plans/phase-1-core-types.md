# Phase 1 — Core Types + Write Path

**Goal:** Writes flow through the new content-addressed architecture. Block map is the source of truth. BLAKE3 hashes tracked on every write.

**Depends on:** v1 codebase (foundation).
**Critical path:** Yes.
**Estimated LOC:** ~1,800 production, ~2,500 tests.

**Design doc references:** [Block Map Design](../GLIDEv2.md#block-map-design), [Write Path](../GLIDEv2.md#write-path), [Crash Recovery (WAL)](../GLIDEv2.md#crash-recovery-wal).

---

## Deliverables

### Data Structures (independent, parallelizable)

**`BlockMap`** — The critical metadata structure. Every read and write resolves through it.

- Ordered array mapping chunk index -> `(Blake3Hash, flags)`, 17 bytes per entry.
- Sparse representation: only non-zero entries stored. Unwritten regions resolve to the zero-block hash.
- `flags` byte: bit 0 = dirty (not yet flushed to S3).
- Each entry also tracks a `sequence: u64` for snapshot consistency (see Phase 4).
- Local persistence: serialize to SSD file every ~5s via fsync. Load on daemon restart.
- Unit tests: insert, lookup, persist/load round-trip, sparse correctness, zero-hash default.

**`DirtySet`** — Tracks which block offsets have unflushed writes.

- `HashSet<u64>` of chunk offsets where `dirty == true` in the block map.
- O(1) insert on write, O(D) drain on flush.
- Avoids O(B) full block map scan to find dirty entries.

**`SequenceNumber`** — Monotonic counter for snapshot consistency.

- `AtomicU64`, incremented on every write. Each block map entry records the sequence of its last write.
- Single atomic load for snapshot cut point (see Phase 4).
- No lock, no contention on the write path.

**`Blake3Hash`** — 16-byte content hash newtype.

- `blake3_128(raw_data)` helper function. ~5us per 128KB block.
- Well-known zero-block hash constant (`blake3_128([0u8; 131072])`).
- Implement `Hash`, `Eq`, `Ord` for use as HashMap/DashMap key.

**LZ4 helpers** — Compress/decompress functions.

- Used in Phase 2 (flush) but standalone and testable now.
- `lz4_compress(raw: &[u8]) -> Vec<u8>`
- `lz4_decompress(compressed: &[u8], expected_size: usize) -> Vec<u8>`

### Write Path Evolution (depends on data structures above)

Evolve `WriteCache::write()` to the new architecture:

```
write(offset, data):
  1. hash = blake3_128(data)
  2. seq = sequence_number.fetch_add(1)
  3. chunk_index = offset / chunk_size
  4. block_map[chunk_index] = (hash, dirty=true, seq)
  5. dirty_set.insert(chunk_index)
  6. wal.append(vm_id, chunk_index, hash, data, seq)
  7. dirty_store[hash] = data   // pinned, not evictable
  8. return Ok(())
```

**WAL format.** Each entry is self-contained for replay:

```
WAL Entry:
  vm_id:        [u8; 16]    (UUID)
  chunk_index:  u64
  hash:         [u8; 16]    (BLAKE3-128)
  sequence:     u64
  data_length:  u32
  data:         [u8; data_length]   (raw, uncompressed)
  crc32:        u32          (over all preceding fields)
```

- Append-only on local SSD. Sequential writes only.
- Truncated after each local block map persistence (~5s).
- On replay: for each entry, update block map + dirty set + dirty store. Skip entries with `sequence <= block_map_persisted_sequence`.
- Incomplete final entry (detected by CRC mismatch or truncation) is discarded — one in-flight write lost, same as bare metal power loss.

**Dirty block store.** `HashMap<Blake3Hash, Bytes>`, no eviction.

- Serves reads at ~100ns (in-memory).
- Blocks move to clean cache on S3 flush (Phase 2).
- Per-VM dirty data counter: `dirty_bytes += chunk_size` on insert, `dirty_bytes -= chunk_size` on removal. Used for budget enforcement in Phase 6.

**Local block map persistence.**

- Every ~5s: serialize block map to SSD file, fsync, truncate WAL.
- Format: same as S3 manifest block map section (see [Wire Formats](../GLIDEv2.md#wire-formats)) but without the pack index (packs are S3-only).
- On daemon restart: load local block map file, replay WAL entries after persisted sequence.

---

## What This Unblocks

- **Phase 2** (flush path needs dirty set + hashed blocks to drain)
- **Phase 3** (read path needs block map lookups to resolve offsets to hashes)

---

## Testable Milestone

1. Write blocks through the new path. Verify block map tracks correct hashes.
2. Read back from dirty store. Verify data integrity (re-hash matches).
3. Kill daemon, restart. Verify WAL replay reconstructs the block map and dirty store.
4. Verify dirty set tracks the correct offsets (insert on write, contains only dirty entries).
5. Verify sparse block map: unwritten offsets return zero-block hash.
6. Verify sequence numbers are monotonically increasing across writes.
7. All unit-testable with in-memory stores (no S3 dependency).

---

## Key Decisions

- **Block map is an array, not a B-tree.** The chunk index IS the array index. O(1) lookup. No key comparison. The sparse representation is handled by skipping zero entries during serialization, not by using a sparse data structure in memory.
- **WAL stores raw data, not compressed.** Compression happens at the S3 boundary (Phase 2), not on the write path. This keeps write latency low.
- **Dirty store is a separate HashMap, not part of the cache.** Dirty blocks are pinned (not evictable). They move to the clean cache only after S3 flush confirms. This separation keeps the eviction policy simple (Phase 5a).

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/block_map.rs` | **New.** BlockMap, DirtySet, SequenceNumber, Blake3Hash types. |
| `src/nbd/write_cache.rs` | **Heavy modification.** New write path, WAL format, dirty store. |
| `src/nbd/handler.rs` | **Minor.** Wire new write path. |
| `Cargo.toml` | **Minor.** Add `blake3`, `lz4_flex` dependencies. |
