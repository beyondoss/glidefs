# Phase 5c — Bless Pipeline

**Goal:** `glidefs bless` produces content-addressed base images in the new pack format. Base images are the starting point for all VMs.

**Depends on:** Phase 2 (pack format + manifest format).
**Can run in parallel with:** everything else after Phase 2.
**Critical path:** No.
**Estimated LOC:** ~600 production, ~500 tests.

**Design doc references:** [Base Image Pipeline](../GLIDEv2.md#base-image-pipeline).

---

## Deliverables

### `glidefs bless` CLI Command

```bash
glidefs bless --image ubuntu-22.04-node20.raw --name ubuntu-22.04-node20-v3
```

Offline pipeline (no daemon involvement, CLI talks directly to S3):

```
bless(image_path, name):
  1. open raw disk image
  2. for each 128KB chunk:
       hash = blake3_128(chunk)
       compressed = lz4_compress(chunk)
       accumulate into current pack

  3. for each pack (25 blocks):
       pack_id = Uuid::new_v4()
       if s3.head(pack_key(pack_id)):   // already exists (re-bless)
         skip
       else:
         s3.put(pack_key(pack_id), pack)

  4. build manifest (block map + pack index)
     s3.put("manifests/bases/{name}", manifest)
```

**Properties:**
- **Global dedup.** If blocks already exist in S3 from a previous bless or from tenant VMs, they're not re-uploaded. Check is at the pack level (HEAD before PUT).
- **Idempotent.** Same image -> same hashes -> same manifest. Re-run is a no-op (all HEADs return 200, no PUTs needed).
- **No layers.** A base image is a flat disk image. Content-addressing handles deduplication across images.

### Creating a VM from a Base Image

The VM's manifest starts as a copy of the base manifest:

```
s3.copy("manifests/bases/{base}", "manifests/{tenant}/{vm-id}")
```

All entries point to blocks already in the global store. As the VM writes, individual entries diverge. Unmodified chunks continue resolving from the same packs.

---

## Testable Milestone

1. Bless a raw disk image. Verify packs appear in S3 with correct content.
2. Create VM from base manifest. Verify all blocks resolve (reads succeed through S3).
3. Bless again. Verify no re-uploads (all HEAD requests return 200).
4. Bless a similar image (same Ubuntu, different Node version). Verify ~95% of packs are deduped (shared OS blocks).
5. Verify manifest is valid: round-trip serialize/deserialize, all hashes have pack index entries.

---

## Key Decisions

- **HEAD-before-PUT, not content-hash pack IDs.** Pack IDs are UUIDs, not content hashes. So dedup at the pack level uses HEAD to check existence. This is a one-time cost during bless (offline pipeline, not latency-sensitive). An alternative would be deterministic pack IDs from content, but that adds complexity for a pipeline that runs infrequently.
- **Flat disk images, not layered.** No Docker-style layers. A base image is just a raw disk. Content-addressing handles the dedup that layers would provide, without the complexity of layer management.
- **Base manifests in a separate namespace.** `manifests/bases/{name}` vs `manifests/{tenant}/{vm-id}`. Keeps base images discoverable and separate from per-tenant state.

## Files Likely Touched

| File | Change |
|------|--------|
| `src/cli/bless.rs` | **New.** Bless CLI command implementation. |
| `src/main.rs` | **Minor.** Add `bless` subcommand to CLI. |
| `src/nbd/manifest.rs` | **Reuse.** Manifest serialization from Phase 2. |
| `src/nbd/block_store.rs` | **Reuse.** Pack assembly from Phase 2. |
