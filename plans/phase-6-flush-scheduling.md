# Phase 6 — Flush Scheduling

**Goal:** Per-VM flush policy. Demand-driven (default) and continuous (production) modes. Dirty budget enforcement prevents unbounded local state growth.

**Depends on:** Phase 4 (fork triggers need the flush path working).
**Critical path:** No.
**Estimated LOC:** ~1,000 production, ~1,000 tests.

**Design doc references:** [Flush Modes](../GLIDEv2.md#flush-modes), [Dirty Block Store (budget)](../GLIDEv2.md#dirty-block-store).

---

## Deliverables

### Demand-Driven Mode (Default)

Flush triggered only by cross-host transitions:

| Trigger | What happens |
|---------|-------------|
| Fork request | Snapshot + flush (Phase 4) |
| Portable sleep | Flush dirty blocks + manifest to S3, release host resources |
| Promote | Flush source, fork production, restore checkpoint, build |
| Migrate | Flush dirty blocks + manifest, start on destination |
| Dirty budget exceeded | Forced partial flush (see below) |

**During active operation:** Zero S3 traffic. All writes hit local SSD. All reads hit local cache.

### Continuous Mode (Production VMs)

Background flush keeps S3 current so forks don't stall:

| Interval | Operation |
|----------|-----------|
| ~5 seconds | Flush dirty blocks to S3 as packs |
| ~60 seconds | Sync manifest (block map + pack index) to S3 |

**Adaptive intervals:** Adjust based on dirty set size. Flush more frequently under heavy writes, skip entirely when the dirty set is empty.

```
flush_interval = max(1s, base_interval * (threshold / dirty_set.len()))
```

Clamped between 1s and the base interval. When `dirty_set.len() == 0`, skip the cycle entirely.

### Dirty Budget Enforcement

Per-VM dirty data budget prevents unbounded growth of local state:

```
write(offset, data):
  // ... normal write path ...
  dirty_bytes += chunk_size

  if dirty_bytes > dirty_budget:
    flush_oldest_dirty_blocks(dirty_bytes - dirty_budget)
```

- Default: 5GB per VM (`dirty_budget_gb` config field).
- O(1) check on every write (compare counter against threshold).
- When exceeded: flush the oldest dirty blocks (by sequence number) until back under budget — even in demand-driven mode.
- **This is the same flush path as Phase 2.** No special code. Same pack assembly, same refcount updates, same manifest write.
- Bounds SSD consumption: at 50 VMs, worst-case aggregate dirty data is 50 x 5GB = 250GB.
- Bounds migration latency: at most `budget` bytes to flush when moving a VM.

### Flush Scheduler State Machine

Per-export scheduler task:

```
enum FlushMode {
    DemandDriven,
    Continuous { block_interval: Duration, manifest_interval: Duration },
}

// Runs as a tokio task per export
async fn flush_scheduler(export, mode, shutdown):
  match mode:
    DemandDriven:
      // No background work. Flush only on explicit trigger.
      // Dirty budget enforcement happens in the write path.
      select! {
        _ = flush_trigger.recv() => flush(export),
        _ = shutdown => return,
      }

    Continuous { block_interval, manifest_interval }:
      let mut block_ticker = interval(block_interval);
      let mut manifest_ticker = interval(manifest_interval);
      loop {
        select! {
          _ = block_ticker.tick() => {
            if export.dirty_set.len() > 0:
              flush_blocks(export)
          }
          _ = manifest_ticker.tick() => {
            flush_manifest(export)
          }
          _ = flush_trigger.recv() => flush(export),  // explicit trigger still works
          _ = shutdown => return,
        }
      }
```

### API

- **Export creation:** `flush_mode` field (`"demand_driven"` or `"continuous"`). Default: `demand_driven`.
- **Runtime switch:** `POST /api/exports/{name}/flush-mode { "flush_mode": "continuous" }`. Takes effect immediately — starts or stops the background flush scheduler.

---

## Testable Milestone

1. Create export with demand-driven mode. Write heavily. Verify zero S3 PUTs until an explicit flush trigger.
2. Trigger a fork. Verify flush happens (packs + manifest in S3).
3. Switch to continuous mode. Verify background flushes happen on ~5s schedule.
4. Verify adaptive intervals: write heavily, observe shorter intervals. Stop writing, observe skipped cycles.
5. Exceed dirty budget. Verify forced partial flush: some packs uploaded, dirty_bytes drops below budget, but not all dirty data is flushed.
6. Verify runtime switch: start demand-driven, switch to continuous mid-operation, verify background flushes begin.

---

## Key Decisions

- **Demand-driven is the default.** Most VMs are ephemeral (previews, dev environments). Zero S3 traffic during active operation is the right tradeoff. Continuous is opt-in for VMs that are fork sources.
- **Budget enforcement in the write path, not a background task.** Checking `dirty_bytes > budget` is O(1) and synchronous. A background task could let the budget be exceeded between checks. The write path is the only place new dirty data appears.
- **Same flush path for all triggers.** Budget-triggered flushes, demand-triggered flushes, and continuous flushes all use the same Phase 2 code. No special cases.

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/flush_scheduler.rs` | **New.** FlushMode enum, per-export scheduler task, adaptive interval logic. |
| `src/nbd/write_cache.rs` | **Modification.** Dirty budget check in write path, oldest-first block selection for partial flush. |
| `src/nbd/router.rs` | **Modification.** Wire flush scheduler to export lifecycle, mode switching. |
| `src/nbd/api.rs` | **Modification.** Flush mode API endpoints. |
| `src/config.rs` | **Minor.** Add `flush_mode`, `dirty_budget_gb` config fields. |
