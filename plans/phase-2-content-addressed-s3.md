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

## Suggested Verifications

### Unit Tests — Pack Format

- **`test_pack_round_trip`**: Assemble a pack from 25 blocks (known data). Write to bytes. Parse back. All 25 blocks recovered with correct hashes and data.
- **`test_pack_single_block`**: Pack with 1 block (underful pack at end of flush). Round-trips correctly.
- **`test_pack_header_magic`**: Parse a pack. First 4 bytes are `"GLPK"`. Version is 1. Block count matches.
- **`test_pack_block_lookup_by_hash`**: Assemble pack, look up a specific block by its BLAKE3 hash. Returns correct offset and length. Decompress and verify data.
- **`test_pack_lz4_compression`**: Compress a block, decompress it. `blake3_128(decompressed) == blake3_128(original)`. Compressed size < original size for compressible data.
- **`test_pack_incompressible_data`**: Pack with random (incompressible) data. Still works — LZ4 handles it (compressed may be slightly larger, that's fine).

### Unit Tests — Manifest Format

- **`test_manifest_round_trip`**: Create manifest with 1,000 block map entries + 40 pack index entries. Serialize to bytes. Deserialize. All entries match.
- **`test_manifest_header_fields`**: Serialize manifest. Parse header. Magic = `"GLDE"`, version = 1, export name matches, sequence matches, chunk_size = 131072, device_size matches, entry counts match.
- **`test_manifest_sparse_encoding`**: Create block map with entries at indices 0, 500, 100000. Serialize. Byte count = 64 (header) + 3*25 (block map) + pack index. NOT 100001*25.
- **`test_manifest_empty`**: VM with zero writes. Manifest has 0 block map entries, 0 pack index entries. Serializes and deserializes correctly.
- **`test_manifest_large`**: 800K block map entries (fully written 100GB disk). Serialize and deserialize. Verify correctness. Measure time — should be <100ms.

### Unit Tests — Host Pack Index

- **`test_pack_index_insert_and_lookup`**: Insert entry, look up by hash. Returns correct PackLocation.
- **`test_pack_index_dedup_check`**: Insert hash A. Check if hash A exists — returns true. Check hash B — returns false.
- **`test_pack_index_concurrent_access`**: Spawn 10 tasks, each inserting 1000 entries concurrently. No panics, all entries present after completion.
- **`test_pack_index_rebuild`**: Build index from 3 manifests. Verify all hashes from all manifests are present. Verify no duplicates (same hash from different VMs stored once).

### Integration Tests — Flush

- **`test_flush_end_to_end`**: Write 100 blocks, trigger flush. Verify: (a) packs appear in S3, (b) each pack parses correctly, (c) each block in each pack has correct hash, (d) manifest appears in S3, (e) manifest is self-contained (every block map hash has a pack index entry, every pack index entry references an existing S3 object).
- **`test_flush_dedup_skips_existing`**: VM-A writes 50 blocks, flushes (uploads 2 packs). VM-B writes the exact same 50 blocks, flushes. Count S3 PUTs for VM-B's flush — should be 0 pack PUTs (all deduped via host pack index). VM-B's manifest should reference VM-A's packs.
- **`test_flush_partial_dedup`**: VM-A writes blocks [0..49], flushes. VM-B writes blocks [25..74] (50% overlap), flushes. VM-B should upload 1 pack (blocks 50-74) and skip 1 pack (blocks 25-49 already in host index).
- **`test_flush_clears_dirty_state`**: Write 50 blocks. Verify dirty_set has 50 entries. Flush. Verify dirty_set is empty. Verify dirty_store is empty. Verify block_map entries have dirty=false.
- **`test_flush_then_load_manifest`**: Write blocks, flush, delete all local state. Load manifest from S3. Verify block map matches pre-flush state (same hashes, same entries, dirty flags cleared).

### Corruption Tests

- **`test_pack_corruption_detected`**: Upload a valid pack to S3. Flip a byte in the compressed data. Read through the read path (Phase 3). Verify the hash check catches the corruption and returns an error.
- **`test_manifest_corruption_detected`**: Corrupt a manifest in S3. Load it. Verify deserialization fails (magic mismatch, or entry count doesn't match actual data).

### Property Tests

- **`prop_flush_preserves_all_data`**: Generate random writes. Flush. For every block written: the hash exists in a pack in S3, and `lz4_decompress(pack_block) == original_data`.
- **`prop_manifest_self_contained`**: After any flush, every hash in the manifest's block map has exactly one entry in the manifest's pack index, and that entry's pack_id exists in S3.

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
