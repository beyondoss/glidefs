# GlideFS Test Roadmap

Tests to add, organized by risk tier. Already-covered tests have been removed. Priority ordering at the bottom.

## Transport Coverage

Nearly all tests in this roadmap exercise `BlockHandler`, which is completely transport-agnostic — NBD and ublk both delegate to the same `read`/`write`/`trim`/`write_zeroes`/`flush` with identical FUA handling. Tests should use `transport_test!` to run on all three transports (NBD, NBD-kernel, ublk) unless marked **[NBD-only]**.

Since ublk is the production transport, it should be treated as the primary target. The existing `transport_test!` macro and CI workflow (`.github/workflows/rust.yml`) already parameterize docker integration tests across transports.

Tests requiring a kernel block device (filesystem-level crash recovery, fio verify) should run on both NBD-kernel and ublk transports where possible.

---

## Tier 1: Data Loss Prevention

These test gaps represent scenarios where data could be silently lost or corrupted.

### 1. Filesystem-level crash recovery

Crash tests today operate at the block level. The real user cares whether their *files* survive. These tests mount ext4 on the NBD device, do filesystem work, then crash the server.

- [x] **`fs_crash_fsync_honored`** — Mount ext4, write file, `fsync`, kill glidefs, restart, remount, verify file intact. Tests that FLUSH→SSD-sync is sufficient for ext4 journal recovery. *(docker_integration/fs_crash_recovery.rs — nbd_kernel + ublk variants)*
- [x] **`fs_crash_unsynced_write_lost_cleanly`** — Mount ext4, write file, do NOT fsync, kill glidefs, restart, `e2fsck`, verify filesystem is clean (file may or may not exist, but no corruption). Proves the block device doesn't leave ext4 in an unrecoverable state. *(docker_integration/fs_crash_recovery.rs — nbd_kernel)*
- **`fs_crash_rename_fsync_pattern`** — The atomic file update pattern: write tmpfile, fsync, rename over target, fsync parent dir. Kill after each step, verify the rename is either fully visible or not. This is the most common durability pattern in applications.
- [x] **`fs_crash_journal_replay`** — Mount ext4 with `data=journal`, write many files, kill mid-write, restart, remount. e2fsck should be clean. Tests that journal replay works correctly over the block device. *(docker_integration/fs_crash_recovery.rs — nbd_kernel)*
- [x] **`fs_crash_during_flush_to_s3`** — Mount ext4, write files, trigger `drain_export`, kill glidefs mid-drain, restart, remount. Filesystem should be intact (SSD has the data), and re-drain should push everything to S3. *(docker_integration/fs_crash_recovery.rs — nbd_kernel)*
- [x] **`fs_repeated_crash_recovery`** — Mount, write, crash, recover, write more, crash, recover — 5 cycles. Verifies no cumulative corruption from repeated dirty shutdowns. *(docker_integration/fs_crash_recovery.rs — nbd_kernel)*

### 2. Flush ordering and write visibility

The write-behind cache has subtle ordering properties. These verify that the durability contract is actually met.

- [x] **`flush_write_visibility_across_clients`** — Client A writes block 0, client A flushes, client B reads block 0. Must see A's write. Tests multi-client coherence after flush. *(docker_integration/concurrent.rs — transport_test)*
- [x] **`flush_fua_write_durable_before_response`** — Write with FUA flag set, crash (no explicit flush), recover. The FUA write must survive. *(integration/flush_ordering.rs)*
- [x] **`flush_ordering_guarantee`** — Write block A, flush, write block B (no flush), crash, recover. Block A must be present. Block B may or may not be present. Two variants: wal_sync=true and wal_sync=false. *(integration/flush_ordering.rs)*
- [x] **`flush_concurrent_with_client_flush`** — Issue client FLUSH command while background `pressure_flush` is in progress. Neither should deadlock, corrupt data, or skip blocks. Cold reader verifies all blocks. *(integration/flush_ordering.rs)*
- [x] **`flush_manifest_consistency_after_partial_drain`** — Write 100 blocks, drain with crashing S3, recover, drain to working S3. Cold reader must see all 100 blocks. *(integration/flush_ordering.rs)*

### 3. Partial block edge cases

Sub-block writes interact with S3 backfill in subtle ways.

Existing coverage: `test_flush_skips_partial_block_until_backfill` (fork read-through merge), `prop_multiple_sub_block_writes_reference_model` + `sub_block_stress` (multiple sub-writes), partial block crash before/after checkpoint.

Remaining gaps:

- [x] **`partial_block_cap_overflow`** — Write 4KB to 1025 different S3-only blocks (exceeding MAX_PARTIAL_BLOCKS=1024). Background backfills are delayed so the cap is hit. The 1025th block falls back to sync backfill. All reads return correct merged data. *(integration/partial_blocks.rs)*
- [x] **`partial_block_crash_during_backfill`** — Write sub-block, crash before backfill completes. Recover. Guest data survives on SSD, block is dirty and ready for handler-level backfill on next startup. *(integration/partial_blocks.rs)*
- [x] **`partial_block_concurrent_write_during_backfill`** — Write 4KB at offset 0, immediately write 4KB at offset 0 again with different data. Second write must win. *(integration/partial_blocks.rs)*

---

## Tier 2: Protocol Correctness

### 4. TRIM semantics

Basic trim-then-read is covered by `test_v2_read_trimmed_block` and `test_trim` (unit tests). These test the integration-level and edge-case gaps:

- **`trim_partial_range`** — TRIM 512 bytes in the middle of a 4096-byte block. Read full block. Trimmed region must be zeros, rest must be original data.
- **`trim_then_flush_to_s3`** — Write block, flush to S3, TRIM block, flush to S3. Cold reader must see zeros (tombstone in manifest). (Zero-write→S3 roundtrip covered indirectly by `prop_zero_block_roundtrip`, but not the TRIM command path specifically.)
- **`trim_fork_semantics`** — Parent has block in S3. Fork child, TRIM that block on child. Read from child returns zeros. Read from parent returns original data. Tests that TRIM creates proper tombstones in fork context.
- **`trim_crash_recovery`** — TRIM block, crash before metadata checkpoint. Recover. Read block. Result depends on WAL persistence — if TRIM was WAL'd, zeros; if not, original data. Either is correct, but it must be consistent.
- **`trim_large_range`** — TRIM a 100MB range spanning many blocks. All must return zeros. Tests batch trim efficiency.

### 5. WRITE_ZEROES semantics

Basic write_zeroes and tombstone creation are covered by `test_write_zeroes`, `test_zero_blocks_produce_tombstones`, and `test_mixed_zero_nonzero_batch`. Remaining gaps:

- [x] **`write_zeroes_with_fua`** — WRITE_ZEROES with FUA, crash, recover. Must see zeros. (`test_write_with_fua` covers write+FUA but not write_zeroes+FUA.) *(added in `handler.rs::test_write_zeroes_with_fua`)*
- [x] **`write_zeroes_sub_block`** — WRITE_ZEROES for 512 bytes within a block. Rest of block must be unchanged. *(added in `handler.rs::test_write_zeroes_sub_block`)*

### 6. Request boundary conditions

Device boundary and cross-block reads are covered by `test_write_beyond_device_size`, `test_read_beyond_device_size`, `unaligned_cross_boundary_read`, and `test_v2_read_spans_chunks`. Remaining gaps:

- [x] **`request_max_payload`** — Write exactly 32MB (MAX_REQUEST_PAYLOAD). Must succeed. *(added in `handler.rs::test_max_payload_write` — tests full-device write spanning all blocks)*
- **[NBD-only] `request_over_max_payload`** — Write 32MB + 1 byte. Must return EINVAL without corrupting protocol stream. Follow up with a valid write to prove stream is still in sync. (NBD-specific: requires draining payload bytes from socket to keep protocol stream in sync. ublk has no equivalent — the kernel splits large bios.)
- [x] **`request_zero_length_read`** — Zero-length READ. Must succeed with empty response. *(added in `handler.rs::test_zero_length_read`)*
- [x] **`request_zero_length_write`** — Zero-length WRITE. Must succeed. *(added in `handler.rs::test_zero_length_write`)*

### 7. Readonly enforcement

Readonly write/trim/write_zeroes rejection and promote-then-write are fully covered by `test_readonly_rejects_*`, `test_readonly_allows_read`, `test_promote_readonly_to_readwrite`, and `test_live_migration_readonly_fork_promote`. One gap:

- [x] **`readonly_flush_succeeds`** — FLUSH on readonly export must work (local SSD sync is harmless). No explicit test exists. *(added in `handler.rs::test_readonly_flush_succeeds`)*

---

## Tier 3: Resource Pressure & Degraded Operation

### 8. SSD capacity pressure

No existing tests exercise SSD pressure. `capacity_monitor.rs` has unit tests for classification logic only.

- **`ssd_full_rejects_new_blocks`** — Fill SSD to >95% utilization. Write to a never-before-written block. Must return ENOSPC. Write to an already-written block (overwrite). Must succeed.
- **`ssd_full_reads_still_work`** — SSD at 95%+. Reads from cache and from S3 must still work.
- **`ssd_full_flush_frees_space`** — SSD at 95%, flush to S3, verify dirty count drops. (Does flush actually free SSD space? If not, this documents that behavior.)
- **`ssd_full_recovery`** — SSD fills up, writes rejected, flush runs, space freed, writes resume. End-to-end pressure cycle.

### 9. S3 degraded operation

Existing coverage: `test_s3_failure_during_sync_marks_blocks_dirty` and `test_data_integrity_after_failure_recovery` cover instant S3 failures and recovery. No tests for slow operations, timeouts, or semaphore exhaustion.

- **`s3_slow_uploads_dont_block_writes`** — Inject 5-second delay on S3 PUTs. Writes to local SSD should still complete at full speed. Flush may be slow but should eventually succeed.
- **`s3_slow_downloads_timeout_gracefully`** — Inject delay on S3 GETs. Cache-miss reads should timeout with EIO, not hang forever. Verify timeout is bounded.
- **`s3_intermittent_failures_eventual_success`** — Fail every other S3 PUT. Flush should retry and eventually get all blocks uploaded. Verify no blocks are permanently stuck as DIRTY. (Existing tests fail all PUTs then succeed all; no intermittent pattern.)
- **`s3_manifest_put_fails_after_pack_upload`** — Packs upload successfully, manifest PUT fails. On retry, packs should not be re-uploaded (they exist). Manifest retry should succeed. Tests idempotent manifest sync.
- **`s3_download_semaphore_exhaustion`** — Issue 512+ concurrent reads to S3-only blocks (exceeding max_s3_downloads). Should queue, not crash. All reads should eventually complete.
- **`s3_upload_semaphore_exhaustion`** — Trigger flush with 128+ packs to upload (exceeding max_s3_uploads). Should queue, not drop packs. All packs should eventually upload.

### 10. Transport stress

- **`client_storm`** — Open 100 clients rapidly, each does one read, then disconnects. Server should handle without leak or crash. For ublk, this means rapid device add/remove cycles.
- **`request_backpressure`** — Send 300 concurrent requests from one client (exceeding MAX_INFLIGHT_REQUESTS=256). Server should apply backpressure (slow accept), not drop requests.
- **[NBD-only] `connection_close_during_inflight`** — Write request in-flight, client closes socket. Server should cancel gracefully, not crash or leak the request task. (Graceful disconnect tested, but not mid-operation close.)
- **[NBD-only] `connection_idle_timeout`** — Open connection, do nothing for extended period. Server should either keep it alive or clean it up gracefully.

---

## Tier 4: Fork & Snapshot Correctness

### 11. Fork edge cases

Existing coverage: `fork_chain_integrity` (3-level forks), `fork_independent_of_parent`, `test_fork_from_snapshot_zero_overwrite_sees_original` (zero overwrite inheritance). Remaining gaps:

- [x] **`fork_deep_chain_read_through`** — Create A → B → C → D → E (5-level fork chain). Write unique block at each level. Read all blocks from E. Tests that deep inheritance chains resolve correctly. (`fork_chain_integrity` tests 3 levels; this extends to 5.) *(added in `integrity_suite.rs::fork_deep_chain_read_through`)*
- [x] **`fork_parent_deleted_child_survives`** — Fork B from A, flush both to S3, delete A from router. Cold-wake B (without A). B must still read all inherited blocks from S3. *(added in `snapshots.rs::test_fork_parent_deleted_child_survives`)*
- [x] **`fork_concurrent_parent_write_during_fork`** — Parent is actively receiving writes while fork is created. Fork should see a consistent snapshot, not a partial view. *(added in `snapshots.rs::test_fork_concurrent_parent_write_during_fork`)*
- [x] **`fork_inherit_then_overwrite_all`** — Fork B from A (A has 100 blocks). Overwrite all 100 blocks on B with new data. Flush B to S3. Cold-wake B. All blocks must be B's data, none of A's. *(added in `snapshots.rs::test_fork_inherit_then_overwrite_all`)*
- [x] **`fork_from_snapshot_during_active_writes`** — Export is actively being written to. Take snapshot at seq=5. Continue writing. Fork from seq=5. Fork must see exactly the state at seq=5, not any later writes. *(added in `snapshots.rs::test_fork_from_snapshot_during_active_writes`)*
- [x] **`fork_both_children_from_same_parent`** — Fork B and C from A. Write different data to same block on B and C. Verify B and C see their own data, not each other's. Tests sibling fork isolation. *(added in `snapshots.rs::test_fork_both_children_from_same_parent`)*

### 12. Snapshot edge cases

Existing coverage: `snapshot_rollback` (point-in-time restore), `test_multiple_snapshots_accumulate`, `test_snapshot_concurrent_with_flush_and_compaction`. Remaining gaps:

- [x] **`snapshot_gc_race`** — Take snapshot, start GC, delete snapshot during GC scan. GC must not delete packs that were live at scan start. *(added in `snapshots.rs::test_snapshot_gc_race`)*
- [x] **`snapshot_manifest_growth`** — Take 1000 snapshots with small writes between each. Verify manifest storage doesn't grow unboundedly (or document the growth rate). (Existing tests accumulate a few snapshots; this tests at scale.) *(added in `snapshots.rs::test_snapshot_manifest_growth` — 100 snapshots, ~0.5x growth ratio, no unbounded growth)*

---

## Tier 5: Long-Running Correctness (Soak Tests)

These run for minutes to hours and catch bugs that only manifest over time.

### 13. Sustained workloads

The existing `integrity_suite::soak_test` covers random write/read with a reference model and periodic cold-wake verification. These tests extend it to cover additional dimensions:

- **`soak_mixed_operations`** — Random mix of: write, read, trim, write_zeroes, flush, fork, snapshot, delete. With reference model tracking expected state per export. Run for 10 minutes. Catches: state machine bugs in BlockMap, manifest corruption from operation interleaving. (Existing soak only does write+read+flush.)
- **`soak_concurrent_clients`** — 8 clients doing random reads/writes to same export simultaneously for 10 minutes. Each client tracks what it wrote. At end, each client verifies its last write to each offset is visible. Catches: lost writes, torn reads under real concurrency. (Existing soak is single-client.)
- **`soak_crash_loop`** — Write random blocks for 30 seconds, kill -9, restart, verify via reference model, repeat 20 times. Catches: cumulative state corruption from repeated unclean shutdowns. (Existing soak does graceful drain+shutdown, not crash.)
- **`soak_fork_chain_churn`** — Create export, write, snapshot, fork, write to fork, snapshot fork, fork again — build a chain of 20 forks. Then delete every other export. GC. Verify remaining exports still read correctly. Catches: GC bugs, manifest reference counting, pack sharing issues.
- **`soak_sub_block_writes`** — Random sub-block-sized writes (64B-2048B) to random offsets for 10 minutes, with reference model tracking byte-level expected state. Periodic full-block reads to verify merge correctness. Catches: partial block bitmap bugs, backfill race conditions.

### 14. fio verify workloads

Existing `fio_bench.rs` runs throughput benchmarks but does NOT use fio's `--verify` mode. These add data integrity verification:

- **`fio_verify_sequential`** — fio with `rw=write` then `rw=read`, `verify=crc32c`, sequential across full device. Catches: block offset calculation errors.
- **`fio_verify_random`** — fio with `rw=randwrite` then `rw=randread`, `verify=crc32c`, 30 minutes. Catches: cache coherence bugs under random access patterns.
- **`fio_verify_mixed`** — fio with `rw=randrw`, `verify=crc32c`, `rwmixwrite=50`, 30 minutes. Catches: read-write interleaving bugs.
- **`fio_verify_after_cold_wake`** — fio write phase, drain to S3, restart server from manifest, fio verify phase. Catches: data loss through the full persistence path.

---

## Tier 6: Correctness Under Compaction & GC

### 15. Compaction safety

Existing coverage: `test_concurrent_compaction_and_flush` (compaction + flush CAS abort), `test_compaction_cas_failure_orphan_cleaned_by_gc` (orphaned packs from CAS failure), `test_fork_during_compaction_sees_consistent_data` (fork consistency during compaction). Remaining gaps:

- [x] **`compaction_during_active_writes`** — Continuously write while compaction runs. No block data should be lost. New writes should not be compacted mid-flight. *(added in `data_safety.rs::test_compaction_during_active_writes`)*
- [x] **`compaction_crash_midway`** — Start compaction, crash after some packs are replaced but before manifest update. Restart. Either old or new packs should be valid — no orphaned references. *(added in `data_safety.rs::test_compaction_crash_midway`)*
- [x] **`compaction_dedup_correctness`** — Write same data to 100 different blocks. Compact. Verify dedup occurs (fewer packs). Read all 100 blocks, verify data. *(added in `data_safety.rs::test_compaction_dedup_correctness`)*

### 16. GC safety

Existing coverage: `test_gc_fork_then_delete_source` (GC after fork delete), `test_gc_deletes_orphaned_packs`, `test_gc_respects_snapshot_packs`. Remaining gaps:

- [x] **`gc_concurrent_with_flush`** — GC and flush running simultaneously. GC must not delete packs that flush just referenced in its manifest. *(added in `gc.rs::test_gc_concurrent_with_flush`)*
- [x] **`gc_with_in_flight_multipart_upload`** — A pack is being uploaded (multipart in progress) when GC scans. GC should not consider the pack orphaned. *(added in `gc.rs::test_gc_with_in_flight_multipart_upload` — grace period protects in-flight packs)*

---

## Priority Ordering

Ranked by expected bug-finding value:

1. **Tier 1 §1-2** (filesystem crash recovery, flush ordering) — highest risk of real data loss in production
2. **Tier 1 §3** (partial block cap overflow, backfill crash) — untested edge case paths
3. **Tier 2 §4** (TRIM semantics) — partially untested advertised feature
4. **Tier 5 §13-14** (soak tests) — find bugs that only manifest after thousands of operations
5. **Tier 3 §8** (SSD pressure) — completely untested rejection path that users will hit
6. **Tier 4 §11-12** (fork edge cases) — several scenarios not covered despite being architecturally critical
7. **Tier 6** (compaction/GC safety) — concurrent compaction + writes is a classic source of subtle corruption
8. **Tier 2 §5-6** (write_zeroes edge cases, request boundaries) — protocol correctness gaps
9. **Tier 3 §9-10** (S3 degraded, connection stress) — operational resilience
