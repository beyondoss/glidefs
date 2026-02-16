# Phase 2 — Content-Addressed S3 (Flush Path)

**Goal:** Dirty blocks flush to S3 as content-addressed, LZ4-compressed packs. Manifests are self-contained and portable.

**Depends on:** Phase 1 (block map, dirty set, hashed blocks).
**Critical path:** Yes.
**Estimated LOC:** ~1,600 production, ~2,000 tests.

**Design doc references:** [Pack Files](../GLIDEv2.md#pack-files-s3-write-batching), [Compression](../GLIDEv2.md#compression), [Wire Formats](../GLIDEv2.md#wire-formats), [Write Path: S3 Flush](../GLIDEv2.md#s3-flush-when-triggered).

---

## Deliverables

### Pack Format

Evolve `S3BlockStore` from position-based batches to content-addressed packs.

- Pack key: `packs/{2-hex-prefix}/{pack-id}` where prefix = first 2 hex chars of pack-id UUID.
- Binary format per the [Wire Formats](../GLIDEv2.md#pack-format) spec: header (16 bytes) + block index (24 bytes/entry) + concatenated LZ4-compressed block data.
- 25 blocks per pack (~3.2MB uncompressed, ~1.6-2.1MB compressed), matching v1's proven batch size.
- Hash before compress: `blake3_128(raw_data)` is the block's identity. LZ4 compression happens after hashing.
- Pack ID: UUID v4, generated at pack creation time.

### Host Pack Index

A host-level lookup table mapping block hashes to their S3 pack locations. Shared across all VMs on the host.

- `DashMap<Blake3Hash, PackLocation>` where `PackLocation = { pack_id: Uuid, offset: u32, comp_length: u32 }`.
- **On flush:** Before uploading a block, check the pack index. If the hash exists, skip the upload — the block is already in S3 (uploaded by this VM or another VM on this host).
- **After flush:** Insert new entries for uploaded blocks.
- **Rebuild:** On VM arrive/depart, rebuild from active VMs' manifests. Debounce rapid lifecycle events (batch within 1s window). At 50 VMs x 80K entries = 4M entries, rebuild takes ~100ms.

### Manifest Serialization

Serialize the block map + pack index to S3 as a self-contained manifest.

- Binary format per the [Wire Formats](../GLIDEv2.md#manifest-format) spec.
- Per-VM pack index derived from the host pack index at serialization time: filter to hashes present in this VM's block map.
- Key: `manifests/{tenant}/{vm-id}`.
- Round-trip property: `deserialize(serialize(manifest)) == manifest`. Verify in tests.

### Flush Operation

The complete flush path, triggered manually for now (scheduling comes in Phase 6):

```
flush(vm):
  1. snapshot = dirty_set.drain()           // O(D) — collect dirty offsets
  2. entries = []
     for offset in snapshot:
       hash = block_map[offset].hash
       if host_pack_index.contains(hash):
         continue                           // dedup — already in S3
       data = dirty_store[hash]
       entries.push((hash, lz4_compress(data)))

  3. for chunk in entries.chunks(25):       // 25 blocks per pack
       pack_id = Uuid::new_v4()
       pack = assemble_pack(pack_id, chunk) // header + index + data
       s3.put(pack_key(pack_id), pack)
       for (hash, offset, len) in chunk:
         host_pack_index.insert(hash, PackLocation { pack_id, offset, len })

  4. for offset in snapshot:
       block_map[offset].dirty = false
       hash = block_map[offset].hash
       dirty_store.remove(hash)             // move to clean cache
       clean_cache.insert(hash, data)

  5. manifest = serialize_manifest(vm, block_map, host_pack_index)
     s3.put(manifest_key(vm), manifest)
```

**Note:** Step 4 must handle concurrent writes — if `block_map[offset].hash` changed since the snapshot (concurrent write during flush), leave the entry dirty. The snapshot used the old hash, which is still valid in the dirty store. See the [Snapshot Concurrency Model](../GLIDEv2.md#snapshot-concurrency-model).

---

## What This Unblocks

- **Phase 4** (fork needs flush + manifest to produce a portable snapshot)
- **Phase 5b** (GC needs pack format + manifests to exist for refcounting)
- **Phase 5c** (bless pipeline reuses the pack format + manifest format)

---

## Testable Milestone

1. Write blocks (Phase 1), trigger flush. Verify packs appear in S3 with correct content (decompress + hash check).
2. Verify manifest round-trips: serialize -> deserialize -> identical block map and pack index.
3. Verify host-level dedup: two VMs with overlapping content, second flush skips already-uploaded blocks. Count S3 PUTs to confirm.
4. Verify manifest is self-contained: every hash in the block map has a corresponding pack index entry. Every pack index entry references a pack that exists in S3.
5. Integration test: write -> flush -> delete local state -> load manifest from S3 -> verify block map matches.

---

## Key Decisions

- **Pack ID is UUID, not content hash.** Dedup happens at the block level (BLAKE3 hash), not the pack level. Two packs with overlapping blocks are fine — the pack index resolves individual blocks. Content-hashing packs would require hashing ~3MB of data per pack for no benefit.
- **Host pack index is DashMap, not per-VM HashMap.** Cross-VM dedup is the primary benefit. A shared DashMap avoids duplicating entries for blocks shared across VMs (base image, common packages). Lock-free concurrent reads from the flush and read paths.
- **Manifest includes full pack index, not references to host index.** The manifest must be self-contained for cross-host portability. The host pack index is ephemeral — rebuilt on each host from active VMs' manifests.

## Files Likely Touched

| File | Change |
|------|--------|
| `src/nbd/block_store.rs` | **Heavy modification.** Content-addressed pack format, new read/write methods. |
| `src/nbd/pack_index.rs` | **New.** Host-level DashMap, dedup check, rebuild logic. |
| `src/nbd/manifest.rs` | **New.** Serialize/deserialize manifest, wire format impl. |
| `src/nbd/write_cache.rs` | **Modification.** Flush operation wiring. |
| `src/nbd/router.rs` | **Minor.** Wire flush trigger to export lifecycle. |
| `Cargo.toml` | **Minor.** Add `dashmap` dependency (if not already present). |
