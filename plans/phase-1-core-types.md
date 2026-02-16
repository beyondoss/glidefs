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

## Suggested Verifications

### Unit Tests — BlockMap

- **`test_block_map_insert_and_lookup`**: Write entry at chunk index 42, read it back. Hash and flags match.
- **`test_block_map_sparse_default`**: Read an index that was never written. Returns `ZERO_BLOCK_HASH`.
- **`test_block_map_overwrite`**: Write to the same index twice. Second hash replaces first. Dirty flag set on both writes.
- **`test_block_map_persist_and_load`**: Write 1,000 entries, persist to temp file, load into a new BlockMap. All entries match. Unwritten indices still return zero hash.
- **`test_block_map_sparse_serialization`**: Write entries at indices 0, 100, 50000. Serialize. Verify only 3 entries are written to disk (not 50001).
- **`test_block_map_sequence_tracking`**: Write entries with increasing sequence numbers. Each entry records its write sequence. Query by sequence range returns correct entries.

### Unit Tests — DirtySet

- **`test_dirty_set_insert_and_contains`**: Insert offset, verify contains returns true. Non-inserted offset returns false.
- **`test_dirty_set_drain`**: Insert 100 offsets, drain. Returns all 100. Set is empty after drain.
- **`test_dirty_set_idempotent_insert`**: Insert the same offset twice. Drain returns it once.

### Unit Tests — Blake3Hash

- **`test_blake3_deterministic`**: Hash the same data twice. Same hash both times.
- **`test_blake3_different_data`**: Hash two different blocks. Different hashes.
- **`test_blake3_zero_block`**: Hash a 128KB zero block. Matches the `ZERO_BLOCK_HASH` constant.
- **`test_blake3_performance`**: Hash a 128KB block. Assert completes in <50us (10x margin over expected 5us).

### Unit Tests — WAL

- **`test_wal_append_and_replay`**: Append 10 entries, close, replay. All 10 entries recovered with correct vm_id, chunk_index, hash, sequence, and data.
- **`test_wal_truncated_entry`**: Append 5 entries, then write a partial 6th entry (truncate mid-write). Replay recovers exactly 5 entries.
- **`test_wal_crc_corruption`**: Append 3 entries, flip a bit in entry 2's CRC. Replay recovers entry 1, stops at entry 2 (or skips to entry 3 depending on recovery strategy).
- **`test_wal_truncate_after_persist`**: Append entries, truncate WAL. Verify WAL is empty. New appends work correctly.

### Integration Tests — Write Path

- **`test_write_path_end_to_end`**: Write 100 blocks through the new path. For each block: verify block map has correct hash, dirty set contains the offset, dirty store contains the data, sequence is monotonically increasing.
- **`test_write_then_read_from_dirty_store`**: Write a block, read it back via the dirty store. Data matches. Re-hash the returned data, matches the block map hash.
- **`test_overwrite_updates_everything`**: Write block at offset 0 (hash=A), then overwrite with different data (hash=B). Block map has hash B. Dirty store has both A and B (A is still there — referenced by old WAL entry until cleanup). Dirty set still contains offset 0.

### Crash Recovery Tests

- **`test_crash_recovery_wal_replay`**: Write 50 blocks. Kill the daemon (drop without clean shutdown). Restart, load block map from last persist + replay WAL. Verify all 50 blocks are present with correct hashes. Read each block from dirty store — data matches.
- **`test_crash_recovery_partial_persist`**: Write 50 blocks. Persist block map (captures all 50). Write 20 more blocks. Kill daemon. Restart — load persisted block map (50 entries) + replay WAL (20 entries). Verify all 70 blocks present.
- **`test_crash_recovery_empty_wal`**: Persist block map, then kill daemon with no new writes. Restart — load block map, WAL is empty. All previously persisted entries present.

### Property Tests (proptest)

- **`prop_any_write_sequence_produces_consistent_state`**: Generate random sequence of writes (random offsets, random data). After all writes: every offset in dirty set has a corresponding dirty entry in block map. Every dirty entry's hash matches `blake3_128(data)` in dirty store. Sequence numbers are strictly increasing.
- **`prop_persist_load_roundtrip`**: Generate random block map state. Persist, load. Identical.

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
