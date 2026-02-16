# Phase 4 — Fork + Snapshot

**Goal:** Cross-host fork works. A running VM can be forked without pausing, producing an exact point-in-time copy via the snapshot mechanism.

**Depends on:** Phase 2 (flush to S3) + Phase 3 (read path for forked VM).
**Critical path:** Yes. This is the killer feature.
**Estimated LOC:** ~1,500 production, ~2,000 tests.

**Design doc references:** [Fork Operation](../GLIDEv2.md#fork-operation), [The Snapshot Mechanism](../GLIDEv2.md#the-snapshot-mechanism), [Snapshot Concurrency Model](../GLIDEv2.md#snapshot-concurrency-model), [Fork Overlay](../GLIDEv2.md#fork-overlay).

---

## Deliverables

### Snapshot Mechanism

The core of consistent fork. Captures the VM's state at a precise sequence number without pausing writes.

```
snapshot(vm) -> (manifest_etag, sequence):
  1. N = sequence_number.load(Acquire)         // atomic, no lock

  2. snapshot_entries = []                      // clone-and-filter approach
     read_lock(block_map):
       for (index, entry) in block_map.dirty_entries():
         if entry.sequence <= N:
           snapshot_entries.push((index, entry.hash))

  3. // Flush snapshot to S3 (same path as Phase 2 flush)
     // Source VM keeps writing — concurrent writes get seq > N
     // Content-addressing: new writes create new hash keys, don't touch snapshot data
     flush_entries(snapshot_entries)

  4. manifest = serialize_manifest(vm, block_map_at_seq_N)
     etag = s3.put(manifest_key(vm), manifest)

  5. // Cleanup: clear dirty flags for flushed entries
     for (index, hash) in snapshot_entries:
       if block_map[index].hash == hash:       // still same? (no concurrent overwrite)
         block_map[index].dirty = false
         dirty_set.remove(index)
         dirty_store.move_to_clean_cache(hash)

  return (etag, N)
```

See [Snapshot Concurrency Model](../GLIDEv2.md#snapshot-concurrency-model) for why this is safe under concurrent writes.

### Consistent Fork

The default fork mode. Control plane orchestrates:

```
1. CP -> Source Host: POST /api/exports/{vm}/snapshot
   Source flushes dirty blocks + manifest at sequence N.
   Returns { manifest_etag, sequence }.

2. CP -> S3: GET manifest (If-Match: etag), PUT to fork destination.
   manifests/{tenant}/vm-source  ->  manifests/{tenant}/vm-fork

3. CP -> Dest Host: POST /api/exports { name: "vm-fork", manifest: "..." }
   Dest loads manifest, block map materializes, VM boots.
   Cache misses pull from S3.
```

### Lazy Fork

Copy the S3 manifest as-is. No coordination with source host.

- Fork gets state as of last S3 flush (could be stale for demand-driven VMs).
- Zero latency — no flush needed.
- Useful when staleness is acceptable (forking a dev environment).

### Fork Overlay (Memory Optimization)

A forked VM is 99% identical to its parent until it diverges. Don't copy the block map — share it.

```rust
struct ForkedBlockMap {
    parent: Arc<BlockMap>,              // shared, immutable reference
    overlay: HashMap<u64, BlockEntry>,  // only entries that differ from parent
}

impl ForkedBlockMap {
    fn lookup(&self, index: u64) -> &BlockEntry {
        self.overlay.get(&index).unwrap_or_else(|| self.parent.get(index))
    }
}
```

- 180 forks with ~1% divergence: 10 x 1.3MB (parents) + 180 x ~13KB (overlays) = ~15MB instead of 260MB.
- **Flatten** when overlay exceeds ~50% of parent's entry count. Background operation: copy parent entries, apply overlay, replace with standalone BlockMap.
- **S3 manifest** always stores the full resolved block map (no overlay encoding). Overlay is a host-memory optimization only.

### API Endpoints

- `POST /api/exports/{name}/snapshot` — Trigger snapshot, return `{ manifest_etag, sequence }`.
- `POST /api/exports` — Existing export creation handles fork (loads manifest from S3 when `manifest_url` is provided).

---

## What This Unblocks

- **Phase 6** (flush scheduling needs fork triggers as one of the demand-driven events)
- **Product:** Preview environments, dev environments, promotion builds — all depend on fork.

---

## Suggested Verifications

### Unit Tests — Snapshot Capture

- **`test_snapshot_captures_at_sequence`**: Write blocks at seq 1-5. Take snapshot at seq=3. Snapshot contains entries with seq <= 3 only. Entries at seq 4-5 are NOT in the snapshot.
- **`test_snapshot_captures_latest_hash_per_offset`**: Write offset 0 at seq 1 (hash A), overwrite offset 0 at seq 2 (hash B). Snapshot at seq=3. Snapshot has (offset=0, hash=B), not hash A.
- **`test_snapshot_empty_dirty_set`**: No writes (or all previously flushed). Snapshot captures nothing. Manifest reflects last-known state. No S3 pack uploads.

### Unit Tests — Fork Overlay

- **`test_overlay_read_from_parent`**: Create parent with 100 entries. Fork (empty overlay). Read all 100 entries — all come from parent.
- **`test_overlay_write_diverges`**: Fork from parent. Write to offset 42 in the fork. Fork reads offset 42 — gets new data. Parent reads offset 42 — still has original data.
- **`test_overlay_flatten`**: Fork from parent (100 entries). Write 60 new entries to fork (>50% of parent). Trigger flatten. Verify fork is now a standalone BlockMap with all 100 parent entries + 60 overlay entries merged. Parent Arc refcount decreases.
- **`test_overlay_memory_sharing`**: Fork 10 times from same parent. Verify parent Arc refcount = 11 (1 original + 10 forks). Each fork's overlay is empty (0 entries). Total memory: ~1x parent + 10x empty HashMaps.

### Integration Tests — Consistent Fork

- **`test_consistent_fork_exact_state`**: VM-A writes blocks [0..99] with pattern 0xAA. Snapshot. Fork to VM-B. VM-B reads all 100 blocks — all contain 0xAA.
- **`test_fork_isolation_source_writes`**: VM-A writes blocks [0..49]. Snapshot + fork to VM-B. VM-A writes blocks [50..99] (AFTER snapshot). VM-B reads blocks [50..99] — gets zeros (not VM-A's post-snapshot data).
- **`test_fork_isolation_fork_writes`**: VM-A writes blocks [0..49]. Fork to VM-B. VM-B writes block 0 with 0xBB. VM-A reads block 0 — still 0xAA. VM-B reads block 0 — gets 0xBB.
- **`test_fork_of_fork`**: VM-A -> fork to VM-B -> fork VM-B to VM-C. Write to each after forking. All three have independent state. Shared blocks resolve correctly through the chain.
- **`test_lazy_fork_uses_last_flush`**: VM-A writes 50 blocks. Flush. Write 50 more blocks (NOT flushed). Lazy fork to VM-B (no snapshot). VM-B has only the first 50 blocks (the flushed ones). The 50 unflushed blocks are NOT in VM-B.

### Concurrency Tests

- **`test_snapshot_during_active_writes`**: Spawn a background writer doing 1000 writes/sec to VM-A. Take a snapshot at some point. Verify: (a) snapshot manifest is internally consistent, (b) every hash in the manifest has data in S3, (c) fork from this manifest reads correctly, (d) no blocks from after the snapshot sequence appear in the fork.
- **`test_concurrent_snapshots`**: Request two snapshots of VM-A simultaneously. Both succeed. Each manifest is internally consistent. Sequence numbers may differ (second snapshot captures writes that happened between the two requests).
- **`test_snapshot_does_not_pause_writes`**: Start a background writer tracking write latencies. Take a snapshot mid-stream. Verify: no write takes >10ms (snapshot should not block writes for more than the brief clone-and-filter lock, <1ms).

### Stress Tests

- **`test_fork_storm`**: Fork VM-A 50 times in rapid succession. All 50 forks have consistent state. Memory usage is ~1x parent + 50x empty overlays (not 50x full copies). Each fork can independently read and write.
- **`test_snapshot_under_heavy_write_load`**: Write at 500MB/s to VM-A. Take 10 snapshots 1 second apart. Each snapshot is internally consistent. Sequence numbers increase. No data corruption.

### API Tests

- **`test_snapshot_api_returns_etag`**: `POST /api/exports/vm-a/snapshot` returns `{ manifest_etag: "...", sequence: N }`. ETag matches the actual S3 object ETag. Sequence is a positive integer.
- **`test_fork_via_export_creation`**: Create export with `manifest_url` pointing to a previously-snapshotted manifest. Export loads successfully. Reads resolve correctly.

---

## Key Decisions

- **Clone-and-filter for dirty set capture.** A brief read lock during the block map scan (<1ms for ~80K entries). Lock-free epoch-based approach is available if this becomes a bottleneck, but measure first.
- **Overlay threshold at 50%.** Heuristic. If the overlay has more than half as many entries as the parent, the HashMap lookup overhead per read isn't worth the memory savings. Profile with real fork divergence patterns.
- **No pause, ever.** The source VM is never paused during snapshot or fork. The sequence number provides the consistency cut. Content-addressing prevents data corruption from concurrent writes.

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/block_map.rs` | **Modification.** Add ForkedBlockMap, overlay logic, flatten. |
| `src/nbd/write_cache.rs` | **Modification.** Snapshot operation (capture + flush). |
| `src/nbd/router.rs` | **Modification.** Snapshot API endpoint, fork export creation with manifest loading. |
| `src/nbd/api.rs` | **Modification.** HTTP endpoint for snapshot. |
