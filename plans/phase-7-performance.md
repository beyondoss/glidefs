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

## Testable Milestone

1. **Boot prefetch:** Benchmark cold boot with and without hot set prefetch. Measure time to first instruction. Target: >2x improvement on cold host.
2. **Read-ahead:** Read a large file sequentially. Verify the next pack is fetched before it's requested (count S3 GETs vs cache hits). Compare sequential read throughput with and without read-ahead.
3. **TRIM:** Write blocks, TRIM them, verify block map entries reset to zero-block hash. Verify memory usage decreases. Read trimmed offset, verify zeros returned.
4. **Scrubber:** Inject bit corruption into a cached block (flip a byte). Verify scrubber detects the mismatch and evicts the block. Verify re-read fetches correct data from S3.

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
