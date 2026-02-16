# Phase 5a — foyer Cache Integration

**Goal:** Replace the simple cache from Phase 3 with foyer's two-tier S3-FIFO cache (memory + SSD). Production-grade eviction that handles VM boot scan patterns without polluting the hot set.

**Depends on:** Phase 3 (read path with cache interface).
**Can run in parallel with:** Phase 4, 5b, 5c.
**Critical path:** No.
**Estimated LOC:** ~500 production, ~500 tests.

**Design doc references:** [Cache Design](../GLIDEv2.md#cache-design).

---

## Deliverables

### Memory Tier

- Configurable size per host (`memory_cache_gb`).
- S3-FIFO eviction policy (via foyer). Scan-resistant — one-time reads (boot blocks) wash through the small queue without evicting genuinely hot blocks.
- ~100ns reads.
- Evicted blocks fall through to SSD tier (not lost).

### SSD Tier

- Bounded by available SSD space (configurable).
- S3-FIFO eviction policy (via foyer).
- ~100us reads. Stores uncompressed blocks.
- Evicted blocks must be re-fetched from S3 on next access.

### foyer Integration

Drop-in replacement for the simple cache from Phase 3.

- Implement the `BlockCache` trait (defined in Phase 3) using foyer's `HybridCache`.
- Same interface: `get(hash) -> Option<Bytes>` / `insert(hash, data)`.
- Configuration: memory tier size, SSD tier size, SSD cache directory path.
- Initialization: create foyer cache at daemon startup, pass to read path.

### Dirty Store Remains Separate

- The dirty store (`HashMap<Blake3Hash, Bytes>`) is NOT part of foyer.
- Dirty blocks are pinned — they must not be evicted (they're the only copy until S3 flush).
- Read path checks dirty store BEFORE foyer (Phase 3 logic unchanged).

---

## Testable Milestone

1. Boot a VM. Verify memory tier serves hot blocks (blocks accessed more than once get promoted to main queue).
2. Verify SSD tier catches evicted memory-tier blocks (read a block, evict from memory, read again — should hit SSD, not S3).
3. Verify S3-FIFO scan resistance: simulate boot pattern (sequential read of 1000 blocks, then re-read 10 "hot" blocks). Hot blocks should still be in cache despite the sequential scan.
4. Benchmark: compare hit rates and p99 read latencies against the simple cache from Phase 3.
5. Verify configuration: set memory_cache_gb to a small value, confirm eviction happens at the expected threshold.

---

## Key Decisions

- **foyer, not a custom implementation.** foyer provides S3-FIFO with memory + disk tiers out of the box. Well-tested, actively maintained Rust library. Writing our own two-tier S3-FIFO cache would be ~2,000+ LOC of complex concurrent code for no differentiation.
- **Uncompressed blocks in cache.** Decompression on the hot path would add latency to every cache hit. Store raw data, pay the extra SSD space.
- **Single shared cache, not per-VM.** All VMs on a host share the same foyer cache. Content-addressing means identical blocks (base image, common packages) are stored once. A shared cache maximizes hit rates.

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/cache.rs` | **Modification.** Replace simple cache impl with foyer-backed impl behind same trait. |
| `src/config.rs` | **Minor.** Add cache configuration fields (memory_cache_gb, ssd_cache_gb, ssd_cache_dir). |
| `src/cli/server.rs` | **Minor.** Initialize foyer cache at startup. |
| `Cargo.toml` | **Minor.** Add `foyer` dependency. |
