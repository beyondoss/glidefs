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
| `multi_block_read` | Multi-block reads (2–16 blocks) across block boundaries from S3 | 17 MB |

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

**multi_block_read** — Reads spanning 2–16 contiguous blocks in a single NBD
read from S3. Verifies the read coalescing logic returns correctly concatenated
block data. Catches offset calculation bugs in coalesced S3 range fetches.

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
| Zero-block handling | `sparse_integrity`, `soak_test` |
| Overwrite semantics | `overwrite_integrity`, `hash_stress` |
| Fork/snapshot (COW) | `fork_integrity` |
| Concurrent flush scheduling | `concurrent_stress` |
| Pack index cache | `hash_stress` (multi-pass), `soak_test` (periodic restart) |
| Manifest CRC32 validation | All tests (implicit — corrupt manifest would fail restore) |
| Sub-block writes (partial blocks) | `sub_block_basic`, `sub_block_stress` |
| Backfill merge (S3 + local overlay) | `sub_block_basic`, `sub_block_stress` |
| Read coalescing (multi-block) | `multi_block_read` |
