# Data Integrity Test Suite

Proves — with cryptographic certainty — that every block written through the
full GlideFS stack comes back byte-identical after S3 roundtrips.

**No mocks. No fakes. Real MinIO. Real packs. Real S3.**

## Running

Requires Docker (for MinIO via testcontainers).

```bash
# Full suite (~2 minutes)
cargo test --features docker-tests --test docker_integration integrity_suite \
    -- --ignored --nocapture

# Single test
cargo test --features docker-tests --test docker_integration \
    integrity_suite::block_hash_verify -- --ignored --nocapture

# Extended soak (5 minutes)
SOAK_DURATION_SECS=300 cargo test --features docker-tests --test docker_integration \
    integrity_suite::soak_test -- --ignored --nocapture
```

## Methodology

Every test follows the same pattern:

1. **Write** — Blocks are written through the full NBD protocol stack with
   deterministic, pseudo-random patterns (seeded from block index via
   `StdRng`). Patterns are reproducible without storing them.

2. **Cold restart** — All dirty blocks are drained to S3 as content-addressed
   packs. The server is shut down. A **fresh server with a new TempDir** is
   started — zero local cache. This forces every subsequent read to fetch from
   S3, proving the full roundtrip:

   ```
   NBD write → WriteCache (SSD) → LZ4 compress → BLAKE3 hash
     → pack assembly → S3 multipart upload → manifest save
     → [server restart, empty cache]
     → manifest restore → pack index lookup → S3 GET range
     → BLAKE3 verify → LZ4 decompress → NBD read
   ```

3. **Verify** — Every block is read back and verified with a cryptographic hash
   (BLAKE3 or SHA-256) computed client-side. A single bit flip in any block
   across the entire pipeline will fail the test.

## Tests

| Test | What it proves | Data |
|------|---------------|------|
| `block_hash_verify` | BLAKE3 of every block survives S3 roundtrip | 256 MB (2048 blocks) |
| `sequential_integrity` | Contiguous stream SHA-256 matches after S3 | 100 MB |
| `hash_stress` | 3 passes of random overwrites, full S3 verify after each | ~800 MB R + ~480 MB W |
| `persistence_integrity` | Single write → S3 → cold restart → SHA-256 verify | 50 MB |
| `sparse_integrity` | Written blocks + zero holes correct from S3 | 64 MB (75% holes) |
| `soak_test` | Timed R/W with cold restart S3 verify every 10s | configurable (default 30s) |
| `fork_integrity` | Fork isolation: child inherits, overwrites, parent unchanged | 16 MB |
| `overwrite_integrity` | Write A → drain → write B → drain → cold restart → get B not A | 32 MB |
| `concurrent_stress` | 4 parallel writers to disjoint regions → cold restart → verify | 128 MB |
| `sub_block_basic` | 4KB sub-region writes into forked 128KB blocks → S3 verify | 8 MB |
| `sub_block_stress` | 500 random 4KB–16KB writes into forked blocks, 5 rounds + verify | 5 MB |
| `zero_block_integrity` | Non-zero → zeros → S3 → verify tombstones win (SIMD detection) | 32 MB |
| `fork_chain_integrity` | Parent → child → grandchild, multi-level pack resolution | 8 MB |
| `write_during_drain` | Concurrent writes during drain → second drain → S3 verify | 64 MB |
| `snapshot_rollback` | Write A → snapshot → write B → fork from snapshot → get A not B | 16 MB |
| `wal_crash_recovery` | Write → drain → write more → crash (no drain) → WAL replay → S3 verify | 32 MB |
| `multi_block_read` | Multi-block reads (2–16 blocks) across block boundaries from S3 | 17 MB |
| `unaligned_cross_boundary_read` | Reads at arbitrary byte offsets spanning block boundaries from S3 | 8 MB |
| `promote_integrity` | Fork readonly → promote → write → drain → cold restart → verify | 8 MB |
| `resize_integrity` | Write → drain → resize (grow) → cold restart → verify original + zeros | 16 MB |

### What each test catches

**block_hash_verify** — Per-block BLAKE3 verification through S3. Catches any
corruption in the pack format (header, index footer, block data offsets),
LZ4 compression/decompression, content addressing, or manifest chunk mapping.

**sequential_integrity** — Streams 100 MB sequentially and computes a single
SHA-256 over the entire device contents. Catches block reordering, missing
blocks, or off-by-one errors in sequential layout.

**hash_stress** — Three passes of random writes to ~60% of blocks, with a full
cold restart and S3 verification after each pass. Catches stale manifest
entries, pack index cache invalidation bugs, and data loss across multiple
flush cycles.

**persistence_integrity** — The simplest S3 roundtrip: write → drain → restart
→ verify. The baseline sanity check.

**sparse_integrity** — Writes 25% of blocks at random offsets, leaving 75%
as holes. Verifies both that written blocks survive S3 and that holes remain
all-zeros. Catches zero-block dedup bugs, sparse state map errors, and
false-positive "present" bits.

**soak_test** — Continuous random writes and reads for a configurable duration
(default 30s, `SOAK_DURATION_SECS=300` for 5 min). Every 10 seconds: cold
restart, full S3 verification of all written blocks, spot-check unwritten
blocks are zeros, then resume. Catches time-dependent races, flush scheduler
bugs, and gradual state corruption under sustained load.

**fork_integrity** — Creates a parent, writes blocks, snapshots, forks a child.
Verifies the child reads parent data from S3 (no local cache). Overwrites
blocks in the child, verifies the parent is unaffected. Cold restart, verify
child state from S3. Catches copy-on-write bugs, pack sharing errors, and
cross-export data leaks.

**overwrite_integrity** — Writes pattern A, drains to S3, restores, overwrites
every block with pattern B, drains again, cold restart, verifies pattern B.
Specifically checks that new pack uploads supersede old ones in the manifest
and that stale pack data is never returned.

**concurrent_stress** — Four NBD clients write to disjoint block ranges
simultaneously. After all writers complete: drain, cold restart, verify every
block from S3. Catches races in concurrent flush scheduling, pack assembly,
and manifest updates.

**sub_block_basic** — Writes 1–4 aligned 4KB sub-regions per block into a
forked child (parent data lives in S3). Verifies the merged result: parent
data in unwritten sub-regions, child data in written ones. Cold restart,
verify from S3. Catches backfill merge errors, bitmap tracking bugs, and
partial block state corruption.

**sub_block_stress** — Hammers forked blocks with 500 random writes (4KB–16KB,
4KB-aligned) across 5 rounds, interleaved with read verification after each
round. Cold restart, verify every byte from S3. Catches races between
concurrent backfill and guest writes, bitmap state corruption across multiple
overlapping writes to the same block, and flush scheduling of partial blocks.
Uses 4KB alignment to match real filesystem I/O (ext4/btrfs/xfs minimum block
size). Note: writes below 4KB are not filesystem-reachable but would expose a
sub-region bitmap granularity limitation in the backfill path.

**zero_block_integrity** — Writes non-zero data to all blocks, drains to S3,
overwrites ALL blocks with zeros, drains again (creating tombstone entries with
`comp_length=0`), cold restart, verifies every block is zeros. Catches: SIMD
`is_zero_block()` detection failures (AVX2/NEON/u64 fallback), missing tombstone
entries in pack index, "newest wins" resolution bugs where stale non-zero pack
data shows through after a zero overwrite.

**fork_chain_integrity** — Three-level fork chain: grandparent writes blocks 0–31,
child overwrites block 0 and writes blocks 32–47, grandchild writes blocks 48–55.
Cold restart, verify grandchild resolves all blocks correctly through the inherited
pack list. Catches: multi-level manifest pack resolution bugs, pack ordering errors
in forked manifests, block resolution across shared S3 prefixes.

**write_during_drain** — Writes initial data, then starts drain and concurrent
writer simultaneously. The writer overwrites ~25% of blocks during the drain.
After drain completes, a second drain picks up re-dirtied blocks. Cold restart,
verify ALL blocks from S3. Catches: CAS DIRTY→SYNCING state machine races,
CRC sentinel handling (u32::MAX), block re-dirtying during flush compute phase,
drain iteration recovery for blocks skipped due to concurrent writes.

**snapshot_rollback** — Writes pattern A → snapshot (captures seq=N) → overwrites
with pattern B → snapshot again → forks from seq=N → verifies pattern A, not B.
Cold restart, verify from S3. Catches: versioned manifest restore bugs, snapshot
sequence lookup errors, stale manifest data in fork-from-snapshot path.

**wal_crash_recovery** — Writes initial data and drains to S3, then writes MORE
data without draining and shuts down (simulating crash). Restarts with the SAME
cache directory → WAL replays recovered dirty blocks. Verifies all data readable
(phase 1 from manifest, phase 2 from WAL). Drains recovered blocks, cold restart
with fresh TempDir, verifies everything from S3. Catches: WAL append/replay bugs,
SSD pwrite persistence issues, metadata checkpoint reconstruction, dirty block
recovery after unclean shutdown, WAL overwrite handling (re-written blocks during
phase 2 that were already in S3 from phase 1).

**multi_block_read** — Reads spanning 2–16 contiguous blocks in a single NBD
read from S3. Verifies the read coalescing logic returns correctly concatenated
block data. Catches offset calculation bugs in coalesced S3 range fetches.

**promote_integrity** — Forks a readonly child from a parent, promotes to
read-write, overwrites half the blocks with new data, drains to S3, cold
restarts, and verifies: parent data unchanged, child inherits unwritten parent
blocks, child has new data for overwritten blocks. Catches post-promote dirty
block state initialization bugs, fork pack resolution after promotion, and
write path incorrectly rejecting writes on a promoted export.

**resize_integrity** — Writes data to all blocks, drains to S3, resizes (grows)
the volume to double its size, drains again, cold restarts from S3, and verifies:
all original blocks are byte-identical, all new blocks in the extended range are
zeros. Catches resize operations clearing the block presence bitmap, manifest
corruption during grow, and incorrect device_size in the post-resize manifest.

**unaligned_cross_boundary_read** — Reads at arbitrary byte offsets within
blocks, spanning across block boundaries. Test cases: 4KB into a block spanning
3 boundaries, 60KB offset spanning 2 blocks, 1-byte offset, last/first 4KB of
adjacent blocks, and a 5+ block span starting 1 byte before a boundary. Catches
offset calculation bugs in the read path's block-slicing logic that aligned reads
wouldn't exercise — the non-aligned path has separate math for computing the
start offset within the first block and the end offset within the last block.

## Data path coverage

The suite exercises every stage of the GlideFS data pipeline:

| Stage | Tested by |
|-------|-----------|
| NBD protocol write/read | All tests |
| WriteCache SSD storage | All tests (write phase) |
| LZ4 compression | All tests (verified by hash match after decompress) |
| BLAKE3-128 content addressing | All tests (internal + external verification) |
| Pack format (GLPK v3) | All tests (pack assembly → S3 → suffix read → unpack) |
| Manifest save/restore (GLVM v5) | All tests (cold restart cycle) |
| S3 multipart upload | All tests (drain phase) |
| S3 range read | All tests (verification phase) |
| Zero-block handling | `sparse_integrity`, `soak_test`, `zero_block_integrity` |
| Zero-block tombstone entries | `zero_block_integrity` (explicit non-zero→zero→S3→verify) |
| SIMD is_zero_block() detection | `zero_block_integrity` (all blocks zeroed, verified from S3) |
| Overwrite semantics | `overwrite_integrity`, `hash_stress` |
| Fork/snapshot (COW) | `fork_integrity`, `fork_chain_integrity` |
| Multi-level fork resolution | `fork_chain_integrity` (3-level: grandparent→parent→child) |
| Concurrent flush scheduling | `concurrent_stress`, `write_during_drain` |
| CAS state machine (DIRTY→SYNCING) | `write_during_drain` (writes during active drain) |
| Pack index cache | `hash_stress` (multi-pass), `soak_test` (periodic restart) |
| Manifest CRC32 validation | All tests (implicit — corrupt manifest would fail restore) |
| Versioned snapshot restore | `snapshot_rollback` (fork from specific sequence) |
| WAL append + replay | `wal_crash_recovery` (crash without drain → WAL recovery) |
| SSD pwrite persistence | `wal_crash_recovery` (block data survives unclean shutdown) |
| Metadata checkpoint recovery | `wal_crash_recovery` (state map reconstruction from WAL) |
| Sub-block writes (partial blocks) | `sub_block_basic`, `sub_block_stress` |
| Backfill merge (S3 + local overlay) | `sub_block_basic`, `sub_block_stress` |
| Read coalescing (multi-block) | `multi_block_read` |
| Unaligned cross-boundary reads | `unaligned_cross_boundary_read` |
| Promote readonly → readwrite | `promote_integrity` |
| Resize (grow) data preservation | `resize_integrity` |
