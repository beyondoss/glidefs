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

## Suggested Verifications

### Integration Tests — Tier Behavior

- **`test_memory_tier_serves_hot_blocks`**: Insert a block into cache. Read it twice (access count > 1, promoted to main queue). Verify reads complete in <1us (memory tier, not SSD).
- **`test_ssd_tier_catches_evictions`**: Configure memory tier to hold 10 blocks. Insert 20 blocks. Read block 0 (evicted from memory). Verify it's still available (fetched from SSD tier, not S3). Count S3 GETs: 0.
- **`test_eviction_to_s3_on_ssd_full`**: Configure both tiers to be very small. Insert more blocks than both tiers can hold. Read an evicted block. Verify S3 GET is triggered (block fell out of both tiers).

### Integration Tests — S3-FIFO Scan Resistance

- **`test_boot_scan_does_not_evict_hot_blocks`**: Insert 10 "hot" blocks, read each twice (promoted to main queue). Then do a sequential scan of 1000 "boot" blocks (read each once). After the scan, read the 10 hot blocks again. All 10 should still be cache hits (not evicted by the scan). This is the core S3-FIFO property.
- **`test_one_time_reads_wash_through`**: Read 500 blocks once each (simulating boot). Verify they enter the small queue. Read 500 different blocks once each. Verify the first 500 are evicted (one-time access, never promoted to main queue). Main queue should still have space.

### Integration Tests — BlockCache Trait Compatibility

- **`test_foyer_implements_block_cache_trait`**: The foyer-backed cache implements the same `BlockCache` trait as the simple cache from Phase 3. Swap in the foyer cache, run all Phase 3 read path tests. All pass without modification.
- **`test_dirty_store_priority_over_cache`**: Write a dirty block (in dirty store). Also insert stale data for the same hash in the clean cache. Read the block. Verify data comes from dirty store (not the stale cache entry).

### Configuration Tests

- **`test_memory_cache_size_limit`**: Set `memory_cache_gb = 0.001` (~1MB). Insert 100 blocks (100 x 128KB = 12.8MB). Verify memory usage stays near 1MB (eviction is working). Blocks are still accessible via SSD tier fallback.
- **`test_cache_survives_restart`**: Insert blocks into cache (SSD tier persists). Restart the daemon. Verify SSD-cached blocks are still accessible without S3 fetch.

### Benchmarks

- **`bench_memory_cache_read_latency`**: Populate memory cache. Read 10,000 blocks. Report p50, p99, p999 latency. Expect p99 < 500ns.
- **`bench_ssd_cache_read_latency`**: Evict from memory, read from SSD tier. Report p50, p99. Expect p99 < 500us.
- **`bench_cache_hit_rate_under_boot_pattern`**: Simulate VM boot (sequential read of 2000 blocks, then steady-state access of 200 hot blocks). Report hit rate after warmup. Expect > 99% for steady-state reads.

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
