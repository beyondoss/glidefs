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

## Testable Milestone

1. Start VM A, write known data patterns to various offsets.
2. Trigger snapshot. Verify manifest appears in S3 with correct ETag.
3. Fork to VM B on the "same host" (simulated). Verify VM B reads exact data as of snapshot point.
4. Write more data to VM A after snapshot. Verify VM B does NOT see VM A's post-snapshot writes.
5. Write data to VM B. Verify VM A does NOT see VM B's writes.
6. Verify fork overlay shares memory: check that parent BlockMap has refcount > 1.
7. **Stress test:** Fork during heavy writes (background writer doing continuous writes to VM A while snapshots are taken). Verify every fork is internally consistent — no torn reads, no missing blocks.
8. Verify lazy fork: fork without snapshot, verify fork has state as of last flush.

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
