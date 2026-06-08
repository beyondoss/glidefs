# Stateright model fidelity audit (vs real write_cache code)

Goal: the model must faithfully mirror the real code so exhaustive checking is
meaningful. Audit each modeled operation against its source. Record every
deviation. A deviation that hides a real interleaving = a false GREEN.

## Real code source map
- Guest write: `handler.rs::backfill_and_write` → `write.rs::write_inner` →
  `inner.rs::promote_syncing_blocks`
- Flush: `flush.rs::flush_to_s3` → `flush_dirty_inner` → `rotate_and_snapshot`,
  `compute_flush_batch`, upload, manifest, `transition_syncing_to_not_present`,
  checkpoint, cleanup
- Read: `handler.rs::read_into` → `read.rs::read`/`resolve_block`/
  `locate_block`/`sync_read_local_block`
- Recovery: `recovery.rs::finish_recovery`

## Deviations found

### D1 — promote/set_present ORDER inverted  [CRITICAL]
Real (`write.rs:145,158`): `promote_syncing_blocks` runs BEFORE `set_present`.
The comment is explicit: promote must see NOT_PRESENT so it can recover data
from the flushing file; if set_present ran first it masks NP→CLEAN and promote
silently skips, "leaving zeros on the active file for sub-block writes."
Model (`lib.rs:195-196`): `AcquireLock → SetPresent(NP→Clean) → Promote`.
So the model's promote never sees NP → never promotes-stale. HIDES the bug.

### D2 — no sub-block (partial) writes  [CRITICAL]
Real: guest writes are 4 KiB into 128 KiB blocks. A sub-block write preserves
the rest of the block; the rest comes from promote (flushing) or backfill (S3).
If promote restored ZEROS (stale NP), the unwritten part is lost.
Model: each block is a single `Ver`; every write overwrites the whole block,
so promote-zeros are always clobbered by the write → corruption never surfaces.

(more to come as audit proceeds)

### D3 — no overlapping flush attempts / no flushing-file-exists skip  [minor]
Real (`flush.rs:854`): if a flushing file already exists (prev flush's manifest
not yet checkpointed), the next flush cycle SKIPS (returns default stats). Model
(`actions`) gates `SF` on `f ∈ {None, Done}` — only one flush at a time, never
overlapping. Likely not the corruption source (single-flush is a subset), but it
means the model can't exercise back-to-back flush generations where gen N+1's
flushing file holds zeros for a block evicted in gen N. RELEVANT to promote-stale.

### D4 — skipped-block recovery NOT modeled  [CRITICAL, 2nd copy site]
Real `compute_flush_batch` does a per-page CRC check on the flushing-file data;
CRC-mismatched / re-dirtied blocks are "skipped", and the skipped-block recovery
(`flush.rs:1203`) COPIES flushing→active for still-SYNCING skipped blocks and
re-marks them DIRTY — a 2nd flushing→active copy site that can propagate stale
data. Model `F::Compute` is a no-op (`F::Compute => F::Upload`). Entire CRC /
skip / recovery machinery is unmodeled.

### D5 — rotate: OK for NP/Clean, but verify
Real rotate: flushing = old active (all blocks), new active = sparse (all zeros).
Model rotate sets flu[b]=ssd[b] & ssd[b]=0 only for SYNCING. For NP/Clean blocks
ssd is already 0 so flu stays 0 (consistent). All DIRTY blocks are snapshotted
(Snapshot sets every Dirty→Syncing under the write lock; no write can interleave
since rotate holds the write lock). So no DIRTY-not-snapshotted. ACCEPTABLE.

## Plan to make the model faithful + expose corruption
1. Fix D1: promote BEFORE set_present.
2. Fix D2: model 2 pages/block; full write sets both, sub write sets one (the
   other must be preserved by promote/backfill).
3. Fix D3: allow a new flush to begin while a block is NP (back-to-back gens) so
   gen N+1's flushing file holds zeros for a gen-N-evicted block.
4. Fix D4: model the skipped-block recovery copy.
5. Invariant: per-page no-data-loss (every page's last-written value recoverable
   in ssd/flushing/s3); plus "evicted block's S3 matches expected per page".
6. Run → expect RED (promote-stale + maybe skipped-recovery). Apply code fix to
   the model → expect GREEN. That is the exhaustive proof.

## Findings from the faithful 2-writer model (src/faithful.rs)

### Bug A — promote-stale-zero  [FIXED]
Without the fix, the model finds: a fast-path (require_promotion) sub-write whose
block has cycled to NOT_PRESENT with the *current* flushing file holding zeros
(block evicted in an earlier gen) → promote copies zeros → sub-write loses the
other page. Needs TWO writers + TWO flush generations to reach (D6). The code fix
(promote: stale-NP-zero → BlockEvicted/skip) makes the model GREEN for this path.

### Bug B — stale-prior backfill RMW  [NOT yet fixed — real]
Even WITH the bug-A fix, the model finds: writer-0 fetches `prior` from S3 for an
NP block; the block fully cycles NP→Dirty→Syncing→NP (writer-1 writes + a flush
uploads+evicts), changing its S3 value; writer-0's recheck sees NP again and
`try_claim_block` (CAS NP→CLEAN, NO version guard) succeeds, so writer-0 merges
its STALE prior and clobbers writer-1's page. Confirmed against code:
`try_claim_block` = `try_set_present` (CAS NP→CLEAN), and the backfill recheck
(handler.rs:719) only checks `is_not_present()` — no sequence/version guard. So a
block that cycles back to NP during the async fetch is indistinguishable from one
that never changed. This is a pre-existing read-modify-write race.

→ The corruption is a CLASS of stale-data races. The fix must make stale data
  detectable (e.g., per-block write sequence the backfill captures at fetch and
  the claim re-validates) OR re-fetch prior after winning the claim.

## VERIFIED minimal fix (exhaustive, src/faithful.rs, 2 writers × 2 blocks × 2 pages)
Toggling each fix independently:
| promote-fix | claim-first | per-block serialize | result |
|---|---|---|---|
| ✗ | ✗ | ✗ | RED (corruption) |
| ✓ | ✓ | ✗ | RED (writer-vs-writer RMW survives) |
| ✗ | ✗ | ✓ | RED (writer-vs-flush promote-stale survives) |
| ✓ | ✗ | ✓ | **GREEN** ← minimal |
| ✓ | ✓ | ✓ | GREEN (claim-first redundant under serialization) |

→ Minimal sufficient fix = **promote-fix + per-block write serialization**. Both
  necessary: promote-fix kills the writer-vs-flush stale-NP-zero promote;
  serialization kills the writer-vs-writer races (stale-prior RMW + full-vs-sub),
  which are reachable because ublk has multiple queues (ublk_nr_queues=4), so a
  single fio job's 4 KiB sub-writes to one 128 KiB block run concurrently.
  Real-code translation: (1) promote-fix already in inner.rs; (2) serialize the
  write handler per block (sharded async lock keyed by block index across the
  backfill+write critical section).
