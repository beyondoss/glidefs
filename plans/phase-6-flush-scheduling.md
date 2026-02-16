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

## Suggested Verifications

### Integration Tests — Demand-Driven Mode

- **`test_demand_driven_zero_s3_traffic`**: Create export with `flush_mode: demand_driven`. Write 1000 blocks over 30 seconds. Count S3 PUTs during this window: exactly 0.
- **`test_demand_driven_fork_triggers_flush`**: Create demand-driven export, write 100 blocks. Trigger snapshot (fork request). Verify: packs appear in S3, manifest appears in S3, dirty set is empty after flush.
- **`test_demand_driven_sleep_triggers_flush`**: Create demand-driven export, write blocks. Trigger portable sleep. Verify flush completes (packs + manifest in S3).
- **`test_demand_driven_delete_no_flush`**: Create demand-driven export, write blocks. Delete the export. Verify: NO S3 PUTs (ephemeral data discarded). Local state cleaned up.

### Integration Tests — Continuous Mode

- **`test_continuous_flush_on_schedule`**: Create export with `flush_mode: continuous`, block interval 1s (shortened for test). Write blocks. Wait 3 seconds. Verify: at least 2 flush cycles occurred (packs in S3). Dirty set is drained periodically.
- **`test_continuous_skips_empty_cycles`**: Create continuous export, write 10 blocks. Wait for flush (packs uploaded, dirty set empty). Wait 5 more seconds with no writes. Verify: no additional S3 PUTs during idle period (cycles are skipped when dirty set is empty).
- **`test_continuous_manifest_sync_interval`**: Configure block flush at 1s, manifest sync at 5s. Write blocks over 10 seconds. Verify: packs uploaded every ~1s, manifest updated every ~5s (fewer manifest PUTs than pack PUTs).

### Integration Tests — Adaptive Intervals

- **`test_adaptive_faster_under_heavy_writes`**: Configure base interval 5s. Write heavily (thousands of blocks per second). Observe flush intervals. Verify: intervals shorter than 5s (adaptive formula kicks in). Dirty set stays bounded.
- **`test_adaptive_slower_under_light_writes`**: Configure base interval 5s. Write 1 block per second. Observe flush intervals. Verify: intervals close to 5s (not much to flush, default interval is fine).

### Integration Tests — Dirty Budget

- **`test_dirty_budget_enforced`**: Set `dirty_budget_gb = 0.001` (~1MB, small for test). Write 20 blocks (20 x 128KB = 2.5MB, exceeding budget). Verify: forced partial flush triggered. After the flush, dirty_bytes is below budget. Some (not all) blocks flushed to S3.
- **`test_dirty_budget_flushes_oldest_first`**: Write blocks A (seq=1), B (seq=2), C (seq=3). Budget allows 2 blocks. Writing block D exceeds budget. Verify: block A (oldest) is flushed first, then B if needed. C and D remain dirty.
- **`test_dirty_budget_demand_driven_still_flushes`**: Create demand-driven export with small dirty budget. Write past the budget. Verify: partial flush occurs even though mode is demand-driven. This is the safety valve.
- **`test_dirty_budget_counter_accurate`**: Write 10 blocks. Verify dirty_bytes = 10 * 128KB. Flush 5 blocks. Verify dirty_bytes = 5 * 128KB. Flush remaining. Verify dirty_bytes = 0.

### Integration Tests — Mode Switching

- **`test_switch_demand_to_continuous`**: Create demand-driven export. Write blocks. Switch to continuous via API. Verify: background flushes begin within one flush interval. Dirty data starts draining.
- **`test_switch_continuous_to_demand`**: Create continuous export. Verify background flushes are running. Switch to demand-driven. Wait 10 seconds with no triggers. Verify: no additional S3 PUTs after the switch (background scheduler stopped).

### API Tests

- **`test_create_export_with_flush_mode`**: `POST /api/exports { flush_mode: "continuous" }`. Verify export created with continuous flush active.
- **`test_flush_mode_switch_api`**: `POST /api/exports/{name}/flush-mode { flush_mode: "continuous" }`. Returns 200. Verify mode changed. `GET /api/exports/{name}` reflects new mode.

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
