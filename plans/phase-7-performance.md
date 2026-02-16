# Phase 7 — Performance + Polish

**Goal:** Production-grade performance. Sub-second boot. Optimized cross-host wake. Storage reclamation via TRIM. Data integrity verification.

**Depends on:** Phase 5a (foyer cache — prefetch targets the cache).
**Can run in parallel with:** Phase 5b, 6.
**Critical path:** No.
**Estimated LOC:** ~1,000 production, ~1,000 tests.

**Design doc references:** [Boot Hot Set Prefetching](../GLIDEv2.md#boot-hot-set-prefetching), [Sequential read-ahead](../GLIDEv2.md#read-path), [TRIM / Discard](../GLIDEv2.md#trim--discard), [Block Integrity Verification](../GLIDEv2.md#block-integrity-verification).

---

## Deliverables

### Boot Hot Set Prefetching

The `glidefs bless` pipeline already processes every block. Extend it to record which blocks are accessed during boot.

**During bless:**
1. Boot the base image in a reference VM.
2. Record block offsets accessed during the first 10 seconds (kernel, init, core libs, runtime).
3. Store as a boot manifest alongside the base image manifest: `manifests/bases/{name}.boot-hot-set`.

**On VM start (before Firecracker launches):**
1. Load the boot hot set for this VM's base image.
2. Prefetch those blocks into the memory cache in parallel.
3. For blocks already cached from sibling VMs: no-op (already in foyer).
4. For cold hosts: pulls boot-critical blocks from S3 in parallel, hiding latency before the VM needs them.

**Expected impact:** Boot reads hit memory cache (~100ns) instead of S3 (~10-50ms). Sub-second to first instruction is achievable for warm hosts.

### Sequential Read-Ahead

Detect sequential access patterns and proactively fetch the next pack before it's requested.

```
struct SequentialDetector {
    last_chunks: RingBuffer<u64, 4>,  // last 4 chunk indices per VM
}

impl SequentialDetector {
    fn record(&mut self, chunk_index: u64) -> bool {
        self.last_chunks.push(chunk_index);
        // Sequential if last 3+ reads are to consecutive chunks
        self.last_chunks.windows(2).all(|w| w[1] == w[0] + 1)
    }
}
```

- On cache miss with sequential pattern detected: fetch the requested pack AND the next pack.
- Hides S3 latency for sequential workloads (boot, large file reads, package installs).
- Resets on non-sequential read (pattern broken).
- Low overhead: one ring buffer per VM, O(1) check per read.

### TRIM Handler

When a guest filesystem deletes files, it issues TRIM commands to reclaim blocks.

- Handle `NBD_CMD_TRIM`: reset trimmed block map entries to the zero-block hash.
- **Metadata-only.** No S3 upload. The WAL records the TRIM for crash recovery, but the entry carries no block data — just the offset and zero hash.
- **Immediate memory savings.** Sparse block map drops zero entries. A VM that wrote 50GB and trimmed 40GB holds ~80K entries (~1.3MB), not ~400K (~6.5MB).
- **Storage reclaimed by GC.** Orphaned blocks are swept on the next GC cycle (subject to grace period). No special TRIM-aware GC needed.
- **Guest opt-in.** Guest filesystem must be mounted with `discard` option or use periodic `fstrim`.

### Background Scrubber

Periodically verify cached blocks by re-hashing.

- Off the hot path — runs at low priority during idle time.
- For each cached block: `blake3_128(data) == expected_hash`.
- On mismatch: evict the block from cache (it's backed by S3, safe to re-fetch). Log a warning. Re-fetch on next access.
- Catches silent bit rot on SSD. Not a hot path concern — verified on S3 ingestion (Phase 3) and periodically by scrubber.
- Configurable rate: scrub N blocks per second. Default: ~1000 blocks/s (~128MB/s). Full cache scan completes in hours, not minutes — low I/O impact.

---

## Suggested Verifications

### Integration Tests — Boot Hot Set Prefetch

- **`test_hot_set_recorded_during_bless`**: Bless a test image with boot recording enabled. Verify a `.boot-hot-set` file appears alongside the base manifest in S3. Parse it — contains a list of chunk indices.
- **`test_hot_set_prefetch_warms_cache`**: Load a boot hot set (e.g., 100 chunk indices). Trigger prefetch. Verify all 100 blocks are in the memory cache before the VM starts. Count S3 GETs: equal to the number of packs containing hot set blocks (much less than 100 due to pack-level fetch).
- **`test_hot_set_prefetch_skips_cached`**: Pre-populate the cache with 50 of the 100 hot set blocks (simulating sibling VMs). Trigger prefetch. Verify: only the remaining 50 blocks are fetched from S3. The 50 cached blocks are not re-fetched.

### Benchmarks — Boot Prefetch

- **`bench_cold_boot_with_prefetch`**: Create VM from base image on a cold host (empty cache). Measure time from VM start to first NBD read completion. Compare with and without hot set prefetch. Target: >2x improvement.
- **`bench_warm_boot_with_prefetch`**: Same test but with sibling VMs having warmed the cache. Prefetch should be mostly no-ops. Verify zero or near-zero S3 GETs.

### Integration Tests — Sequential Read-Ahead

- **`test_sequential_detection`**: Read chunks at indices 0, 1, 2, 3 (sequential). Verify the detector identifies this as sequential. Read chunk at index 100 (non-sequential). Verify the detector resets.
- **`test_readahead_prefetches_next_pack`**: Clear cache. Read chunk 0 (cache miss, fetches pack 0 from S3). Read chunks 1, 2, 3 (sequential pattern detected). Verify: the pack containing chunks 25-49 is proactively fetched (read-ahead). When the VM reads chunk 25, it's a cache hit.
- **`test_readahead_disabled_on_random`**: Read chunks at random offsets. Verify: no proactive fetches beyond the normal pack-level prefetch (25 blocks per miss). Read-ahead does not trigger.

### Benchmarks — Sequential Read-Ahead

- **`bench_sequential_read_throughput`**: Read 10,000 consecutive chunks. Measure throughput (MB/s) with and without read-ahead. Target: read-ahead should approach SSD throughput (S3 latency hidden by prefetch).

### Integration Tests — TRIM

- **`test_trim_resets_block_map`**: Write block at offset 0 (hash A). TRIM offset 0. Read block map entry at offset 0 — hash is `ZERO_BLOCK_HASH`.
- **`test_trim_returns_zeros`**: Write block at offset 0. TRIM offset 0. Read offset 0 — returns all zeros.
- **`test_trim_reduces_sparse_size`**: Write 1000 blocks. Verify block map has 1000 non-zero entries. TRIM 900 of them. Verify block map has 100 non-zero entries. Serialize — verify serialized size is ~100 entries (not 1000).
- **`test_trim_wal_entry`**: TRIM offset 0. Kill daemon, restart, replay WAL. Verify offset 0 is correctly trimmed (block map entry is zero hash).
- **`test_trim_range`**: Write blocks at offsets [0, 128KB, 256KB, 384KB, 512KB]. TRIM the range [128KB, 384KB]. Verify: offsets 128KB and 256KB are trimmed (zero hash). Offsets 0, 384KB, 512KB are unchanged.

### Integration Tests — Background Scrubber

- **`test_scrubber_detects_corruption`**: Insert a block into cache. Manually flip a byte in the cached data (simulating bit rot). Run scrubber. Verify: the corrupted block is evicted from cache. A warning is logged.
- **`test_scrubber_leaves_valid_blocks`**: Insert 100 valid blocks. Run scrubber. Verify: all 100 blocks still in cache. No warnings.
- **`test_scrubber_re_fetch_after_eviction`**: Corrupt a cached block. Run scrubber (evicts it). Read the block. Verify: S3 fetch occurs, correct data returned, block re-cached.
- **`test_scrubber_rate_limiting`**: Configure scrubber to check 10 blocks/second. Insert 100 blocks. Start scrubber. Verify: takes ~10 seconds to complete a full pass (rate limited, not a burst).

---

## Key Decisions

- **Boot hot set is per-base-image, not per-VM.** All VMs from the same base image boot the same way (kernel, init, libs). Per-VM hot sets would require recording during runtime — complex and not needed for the boot case.
- **Sequential detection is per-VM, not global.** Different VMs have different access patterns. A ring buffer per VM is negligible memory (32 bytes per VM).
- **Scrubber trusts S3.** If a cached block fails verification, evict and re-fetch. S3 has its own integrity guarantees (CRC on GET). If the re-fetched block also fails: alert, don't serve.
- **TRIM is metadata-only.** No "TRIM packs" or "TRIM-aware GC." Orphaned blocks from TRIM are handled by the same GC that handles overwritten blocks. One GC for everything.

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/prefetch.rs` | **New.** Boot hot set loading, sequential read-ahead detector. |
| `src/nbd/scrubber.rs` | **New.** Background integrity verification task. |
| `src/nbd/handler.rs` | **Modification.** Handle NBD_CMD_TRIM, wire read-ahead. |
| `src/nbd/block_map.rs` | **Modification.** TRIM support (reset entry to zero-block hash). |
| `src/cli/bless.rs` | **Modification.** Record boot hot set during bless. |
| `src/nbd/write_cache.rs` | **Minor.** Wire prefetch to read path. |
