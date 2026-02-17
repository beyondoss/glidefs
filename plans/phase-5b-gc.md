# Phase 5b — Garbage Collection

**Goal:** Orphaned packs are cleaned up. S3 storage doesn't grow unbounded. Per-VM pack registries track pack existence; periodic reconciliation against manifests identifies and deletes orphans.

**Depends on:** Phase 2 (packs + manifests exist in S3).
**Can run in parallel with:** Phase 4, 5a, 5c, 6.
**Critical path:** No (orphans accumulate slowly; GC can be added after the core product works).
**Estimated LOC:** ~800 production, ~800 tests.

**Design doc references:** [Garbage Collection](../GLIDEv2.md#garbage-collection).

---

## Deliverables

### Pack Registry (S3 Object)

Per-VM S3 object at `pack-registries/{tenant}/{vm-id}`. Append-only list of pack UUIDs.

```
Format:
  magic         [u8; 4]    = "GLPR"
  count         u32 LE     (number of pack IDs)
  pack_ids      [Uuid; count]  (16 bytes each, packed)

Typical size: 4 + 4 + 3,200 × 16 = ~51KB for a 10GB VM
```

Serialize/deserialize module in the daemon. Same pattern as manifest: magic + fixed header + packed entries.

### Registry Updates (Daemon Side)

The daemon updates the pack registry on flush and fork:

| Event | Operation | S3 ops |
|-------|-----------|--------|
| Flush | GET registry, append new pack IDs, PUT registry | 1 GET + 1 PUT (~51KB) |
| Fork | Create child registry from parent manifest's pack IDs | 1 PUT (~51KB) |
| VM delete | No-op (registry left for GC to clean up) | 0 |

**On flush:** After uploading packs and manifest, the daemon reads the current registry, appends the IDs of newly created packs, and writes it back. This is off the critical I/O path — the manifest is already uploaded, the flush is complete.

**On fork:** The child VM's registry is created from the parent manifest's pack index (pack IDs already in hand from the fork operation). This ensures every pack the child references is tracked independently of the parent.

**Failure handling:**
- If the registry PUT fails after packs are uploaded: the packs become untracked orphans. They waste storage but cause no data loss. A periodic S3 LIST fallback sweep can catch them if needed.
- Never block VM I/O on a registry update.
- No retry logic needed — orphans are harmless and bounded by flush crash frequency.

### GC Reconciliation Tool (Primary GC Mechanism)

CLI tool and/or scheduled job that identifies and deletes orphaned packs:

```
glidefs gc [--dry-run] [--tenant TENANT] [--grace-period 24h] [--max-deletes 10000]

reconcile():
  1. S3 LIST manifests/ prefix → all VM manifests
  2. For each manifest: parse pack index → collect pack_ids into live_packs set
  3. S3 LIST pack-registries/ prefix → all registries
  4. For each registry: parse → collect pack_ids into all_known_packs set
  5. dead_packs = all_known_packs - live_packs
  6. Filter: skip packs that haven't been dead for > grace period
     (track first-seen-dead timestamp in a local state file between runs)
  7. Delete dead packs from S3 (batched, capped at --max-deletes)
  8. Compact registries: rewrite without dead pack IDs
  9. Delete empty registries (no corresponding manifest = deleted VM, all packs cleaned)
  10. Report: total packs, live packs, dead found, deleted, registries compacted
```

**Grace period tracking:** The tool maintains a local state file (`gc-state.json`) mapping pack IDs to their first-seen-dead timestamp. On each run:
- New dead packs get timestamped
- Packs dead longer than grace period are eligible for deletion
- Deleted packs are removed from state
- Packs that become live again (re-referenced) are removed from state

**Performance:**

| Fleet size | Wall time (~500 concurrent S3 ops) | S3 cost per run |
|-----------|-----------------------------------|-----------------|
| 1K VMs | ~1 minute | ~$0.01 |
| 100K VMs | ~15 minutes | ~$0.25 |
| 1M VMs | ~60 minutes | ~$2.50 |

**Optimization:** Only the pack index section of each manifest is needed. Since the manifest header contains `block_map_count` and `pack_index_count`, an S3 range request can skip the block map and read only the pack index — cutting data volume by ~40%.

---

## What This Unblocks

- Nothing blocks on GC. It's a correctness and cost concern, not a feature dependency.

---

## Suggested Verifications

### Unit Tests — Pack Registry

- **`test_registry_round_trip`**: Serialize a registry with 100 pack UUIDs. Deserialize. Verify all UUIDs match.
- **`test_registry_empty`**: Serialize/deserialize an empty registry. Verify count=0.
- **`test_registry_large`**: Serialize a registry with 10,000 pack UUIDs (~160KB). Verify round-trip correctness and timing (<10ms).
- **`test_registry_append`**: Create a registry with 50 packs. Append 5 more. Verify the result has 55 packs with correct IDs.
- **`test_registry_invalid_magic`**: Attempt to deserialize data with wrong magic bytes. Verify error.
- **`test_registry_compact`**: Create a registry with 100 packs. Compact with a set of 30 packs to remove. Verify result has 70 packs.

### Integration Tests — Lifecycle Events

- **`test_flush_updates_registry`**: Write 50 blocks, flush (creates 2 packs). Verify pack registry in S3 contains both pack IDs.
- **`test_multiple_flushes_accumulate`**: Flush 3 times (creating 2 packs each). Verify registry contains all 6 pack IDs.
- **`test_fork_creates_child_registry`**: VM-A has 5 packs. Fork VM-A to VM-B. Verify VM-B's registry contains all 5 pack IDs from VM-A's manifest.
- **`test_fork_registries_independent`**: Fork VM-B from VM-A. Write new blocks to VM-B, flush. Verify VM-B's registry has VM-A's packs + VM-B's new packs. Verify VM-A's registry is unchanged.
- **`test_delete_leaves_registry`**: Delete a VM. Verify its registry still exists in S3 (GC handles cleanup).

### Integration Tests — GC Reconciliation

- **`test_gc_finds_no_orphans`**: Normal operation — flush, fork. Run GC. Verify zero deletions.
- **`test_gc_deletes_orphaned_packs`**: Write blocks, flush (packs A, B). Overwrite all blocks, flush again (packs C, D). Run GC past grace period. Verify packs A and B are deleted from S3. Verify packs C and D are untouched.
- **`test_gc_respects_grace_period`**: Create orphaned packs. Run GC immediately. Verify packs are NOT deleted (within grace period). Advance time past grace period. Run GC again. Verify packs are deleted.
- **`test_gc_respects_max_deletes`**: Create 500 orphaned packs (past grace period). Run GC with `--max-deletes 100`. Verify only 100 deleted. Run again — next 100 deleted.
- **`test_gc_dry_run`**: Create orphaned packs past grace period. Run GC with `--dry-run`. Verify packs still exist. Verify output lists what would be deleted.
- **`test_gc_fork_then_delete_source`**: VM-A has 5 packs. Fork to VM-B. Delete VM-A. Run GC. Verify packs are NOT deleted (VM-B's manifest still references them).
- **`test_gc_fork_then_delete_both`**: VM-A → fork to VM-B → delete VM-A → delete VM-B. Run GC past grace period. Verify all packs are deleted.
- **`test_gc_compacts_registries`**: Run GC that deletes orphaned packs. Verify the registries are compacted (dead pack IDs removed).
- **`test_gc_deletes_empty_registries`**: Delete a VM. Run GC until all its packs are deleted. Verify the VM's registry is also deleted.

### Failure Handling Tests

- **`test_registry_put_failure`**: Simulate S3 PUT failure for registry after successful pack upload. Verify packs exist in S3 but are not in the registry. Verify they survive as harmless orphans.
- **`test_gc_crash_recovery`**: Run GC partway through deletion, simulate crash. Run GC again. Verify it picks up where it left off (idempotent).
- **`test_gc_manifest_parse_error`**: Corrupt one manifest in S3. Run GC. Verify it skips that VM, logs an alert, and processes all other VMs normally.

---

## Key Decisions

- **S3-only, no database.** Pack registries and manifests both live in S3. No Postgres, no control plane API for GC. Fewer moving parts, no state synchronization between S3 and a database.
- **Manifests are the source of truth for liveness.** A pack is live if and only if some manifest references it. The registry is just an inventory of "packs that exist" — a fact, not a derived cache. This can never drift in a way that causes data loss.
- **Append-only registry.** The registry only grows (until GC compacts it). No decrements, no delta tracking, no ordering dependencies. Crash-safe by construction.
- **24-hour grace period.** Conservative. Protects against races where packs are created but the manifest or registry hasn't been updated yet. Can be shortened once we verify the window is reliably <1 minute.
- **No per-block GC, only per-pack.** A pack is the GC unit. Mixed-liveness packs (some dead blocks, some live) are kept whole until every block is unreferenced. Bounded storage waste (~$0.02/month per deleted parent VM).
- **GC reconciliation is the primary mechanism, not a safety net.** No real-time refcount tracking. Orphans accumulate between GC runs — bounded by run frequency (weekly) and VM churn rate. At typical churn, this is a few GB of orphaned packs per week. Negligible cost.

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/pack_registry.rs` | **New.** Pack registry serialize/deserialize, append, compact. |
| `src/nbd/write_cache/flush.rs` | **Modification.** Update pack registry after flush (append new pack IDs). |
| `src/nbd/router.rs` | **Modification.** Create child registry on fork. |
| `src/cli/gc.rs` | **New.** GC reconciliation CLI tool (`glidefs gc`). |
