# Phase 5b — Garbage Collection

**Goal:** Orphaned packs are cleaned up. S3 storage doesn't grow unbounded. Event-driven refcounts make GC O(1) per flush event, not O(N manifests).

**Depends on:** Phase 2 (packs + manifests exist in S3).
**Can run in parallel with:** Phase 4, 5a, 5c, 6.
**Critical path:** No (orphans accumulate slowly; GC can be added after the core product works).
**Estimated LOC:** ~1,500 production, ~1,200 tests.

**Design doc references:** [Garbage Collection](../GLIDEv2.md#garbage-collection), [Control Plane API](../GLIDEv2.md#control-plane-api).

---

## Deliverables

### Control Plane DB Schema

```sql
CREATE TABLE pack_refcounts (
    pack_id          UUID PRIMARY KEY,
    refcount         INTEGER NOT NULL DEFAULT 0,
    last_decremented TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- GC worker queries this: "packs with refcount 0, older than grace period"
CREATE INDEX idx_pack_refcounts_gc
    ON pack_refcounts (last_decremented)
    WHERE refcount = 0;
```

### Event-Driven Refcount Updates (Daemon Side)

The daemon calls the [Control Plane API](../GLIDEv2.md#control-plane-api) after each lifecycle event:

```
POST /api/internal/packs/refcounts
{ "increments": [...], "decrements": [...] }
```

| Event | Increments | Decrements | Typical size |
|-------|-----------|-----------|-------------|
| Flush (continuous, ~5s) | New packs | Packs with overwritten blocks | 2-5 entries |
| Flush (demand-driven) | New packs | Packs with overwritten blocks | Same |
| Fork | All packs in source manifest | (none) | ~3,200 entries |
| VM delete | (none) | All packs in manifest | ~3,200 entries |

**Tracking the delta.** During flush, the daemon knows which packs are new (just uploaded) and which packs lost blocks (old hash overwritten by new hash). The delta is computed from the flush operation itself — no manifest diff needed.

**Fork and delete.** Read the full pack list from the S3 manifest (already in hand from the fork/delete operation). Send as a single bulk request.

**Failure handling:**
- Retry with exponential backoff (1s, 2s, 4s, max 30s).
- On exhausted retries: log error, continue. Orphaned packs survive — no data loss. Monthly reconciliation catches the drift.
- Never block VM I/O on a refcount update.

### Pack Deletion Worker (Control Plane Side)

Background process that runs continuously:

```
loop:
  packs = SELECT pack_id FROM pack_refcounts
            WHERE refcount = 0
            AND last_decremented < now() - interval '24 hours'
            LIMIT 100;

  for pack in packs:
    s3.delete(pack_key(pack.pack_id))
    DELETE FROM pack_refcounts WHERE pack_id = pack.pack_id;

  sleep(5 seconds)
```

- **Grace period: 24 hours.** Protects against races: flush creates packs, then the refcount update fails transiently. The pack exists in S3 without a refcount for up to a few seconds. Grace period covers this.
- **Cap per cycle: 100 packs.** Limits blast radius of a bug. At 100 packs x ~3MB = 300MB per cycle, 5s interval = 60MB/s sustained delete throughput. More than enough for steady-state churn.
- **Dry-run mode:** Log what would be deleted without deleting. Enable in production first, verify behavior, then enable actual deletion.

### Reconciliation Tool (Monthly Safety Net)

An offline tool that verifies refcount consistency:

```
reconcile():
  1. List all manifests in S3 (manifests/{tenant}/{vm-id})
  2. For each manifest: parse pack index, collect all pack_ids -> live_packs set
  3. Query all pack_ids from pack_refcounts table
  4. Compare:
     - Pack in live_packs but refcount=0 in DB: increment to correct value
     - Pack in DB with refcount>0 but not in any manifest: decrement, log discrepancy
     - Pack in S3 (packs/ prefix) but not in any manifest AND not in DB: orphan, mark for deletion
  5. Report: total packs, live packs, orphans found, corrections made
```

- Run monthly, or on-demand after incidents.
- At 1M VMs: ~60 minutes. At 10M VMs: hours, run incrementally (rotate through tenant subsets weekly).
- **Read-only first.** Run in report-only mode before enabling corrections.

---

## What This Unblocks

- Nothing blocks on GC. It's a correctness and cost concern, not a feature dependency.

---

## Suggested Verifications

### Unit Tests — Refcount Client

- **`test_refcount_increment`**: Call the API with increments for 3 pack IDs. Verify DB has refcount=1 for each.
- **`test_refcount_decrement`**: Increment 3 packs, then decrement 1. Verify refcount=0 for the decremented pack, refcount=1 for the other two. `last_decremented` is set.
- **`test_refcount_batch_update`**: Single API call with 100 increments and 50 decrements. Verify all applied atomically (all-or-nothing on the DB side).
- **`test_refcount_idempotent_increment`**: Increment the same pack twice. Refcount = 2 (not an error, not 1 — legitimate for shared packs referenced by two VMs).

### Integration Tests — Lifecycle Events

- **`test_flush_updates_refcounts`**: Write 50 blocks, flush (creates 2 packs). Verify refcount=1 for both packs in DB.
- **`test_overwrite_decrements_old_pack`**: Write 25 blocks, flush (pack A). Overwrite all 25 blocks with new data, flush again (pack B). Verify: pack A refcount=0, pack B refcount=1. Pack A's `last_decremented` is set.
- **`test_fork_increments_all_packs`**: VM-A has 5 packs (from previous flushes). Fork VM-A to VM-B. Verify all 5 packs now have refcount=2.
- **`test_delete_decrements_all_packs`**: VM-A has 5 packs. Delete VM-A. Verify all 5 packs have refcount=0.
- **`test_fork_then_delete_source`**: VM-A -> fork to VM-B -> delete VM-A. Shared packs have refcount=1 (VM-B still references them). Packs are NOT deleted.
- **`test_fork_then_delete_both`**: VM-A -> fork to VM-B -> delete VM-A -> delete VM-B. All shared packs have refcount=0. After grace period, deletion worker removes them.

### Integration Tests — Pack Deletion Worker

- **`test_deletion_worker_respects_grace_period`**: Create a pack with refcount=0 and `last_decremented = now()`. Run deletion worker. Pack is NOT deleted (within grace period). Advance mock clock past 24 hours. Run again. Pack is deleted from S3 and DB.
- **`test_deletion_worker_skips_live_packs`**: Pack A has refcount=0 (grace period expired). Pack B has refcount=1. Run deletion worker. Pack A deleted. Pack B untouched.
- **`test_deletion_worker_caps_per_cycle`**: Create 500 packs with refcount=0 (all past grace period). Run one deletion cycle. Verify only 100 are deleted (cap per cycle). Run 4 more cycles — all 500 deleted.
- **`test_deletion_worker_dry_run`**: Enable dry-run mode. Run deletion worker with expired refcount=0 packs. Verify packs still exist in S3. Verify log output lists what would be deleted.

### Integration Tests — Reconciliation

- **`test_reconciliation_finds_no_drift`**: Normal operation — flush, fork, delete. Run reconciliation. Verify zero discrepancies.
- **`test_reconciliation_detects_missing_refcount`**: Delete a refcount row from DB manually (simulating a failed update). Run reconciliation. Verify it detects the pack is referenced by a manifest but has no refcount. Verify it corrects the refcount.
- **`test_reconciliation_detects_orphaned_pack`**: Insert a pack in S3 that isn't referenced by any manifest and has no DB refcount. Run reconciliation. Verify it identifies the orphan and marks it for deletion.
- **`test_reconciliation_detects_inflated_refcount`**: Set a pack's refcount to 5 when it's only referenced by 2 manifests. Run reconciliation. Verify it corrects the refcount to 2.

### Failure Handling Tests

- **`test_refcount_api_retry_on_failure`**: Mock the control plane API to fail twice, then succeed. Verify the daemon retries and the refcount is eventually updated. Verify VM I/O is not blocked during retries.
- **`test_refcount_api_total_failure`**: Mock the API to fail permanently. Verify the daemon logs an error after exhausting retries but continues operating. Packs become orphans — reconciliation would catch them later.

---

## Key Decisions

- **HTTP API, not direct DB connection.** Daemon communicates with control plane via HTTP (see [Control Plane API](../GLIDEv2.md#control-plane-api)). Decouples infrastructure from data layer. Control plane handles authorization, rate limiting, transaction isolation.
- **24-hour grace period.** Conservative. Could be shorter (1 hour) once we verify that refcount updates reliably complete within seconds. Start conservative, tighten later.
- **No refcount for individual blocks, only packs.** A pack is the GC unit. Mixed-liveness packs (some dead blocks, some live) are kept whole until every block is unreferenced. Simpler than block-level GC, bounded storage waste (~$0.02/month per deleted parent VM).

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/gc.rs` | **New.** Refcount update client (HTTP calls to control plane API). |
| `src/nbd/write_cache.rs` | **Modification.** Track pack delta during flush (new packs, dropped packs). |
| `src/nbd/router.rs` | **Modification.** Wire refcount updates to flush/fork/delete lifecycle events. |
| `src/cli/reconcile.rs` | **New.** Reconciliation CLI tool (`glidefs reconcile`). |
