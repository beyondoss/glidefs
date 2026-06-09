# Boot-set vs automatic readahead — DEFINITIVE empirical result (real images)

Method: real OCI images blessed to EROFS in MinIO via `glidefs bless`; real boot
traces captured by `boot_profile` (disposable ublk-serve + kernel-mount + chroot-run
entrypoint + device-layer read-fault record, NOT strace, NOT a production VM); replayed
through the REAL read path (fetch_with_window) with injected 40ms RTT / 100 MiB/s.
GET counts + bytes are EXACT/measured; wall = build-independent model (GETs*RTT + bytes/tput).
Boot-set arm is MEASURED: real boot blocks read out, laid contiguous, re-flushed with
real compression, replayed.

| image            | boot set | demand GET | readahead GET / fetched / ms | boot-set(measured) GET/MiB/ms | win   |
|------------------|----------|-----------|------------------------------|-------------------------------|-------|
| busybox (static) |  4.6 MiB |  31       |  1 /   1.9 MiB /   59        | 1 /  2.0 /  60                | 1.0x  |
| nginx            |  9.8 MiB |  72       |  4 /  47.9 MiB /  639        | 1 /  3.0 /  70                | 9.2x  |
| python:3.12-slim | 24.9 MiB | 193       |  4 /  41.8 MiB /  578        | 1 /  9.0 / 130                | 4.5x  |
| node:20-slim     | 44.4 MiB | 349       |  9 / 102.7 MiB / 1387        | 1 / 17.6 / 216                | 6.4x  |
| python:3.12 2GB  | 31.0 MiB | 242       | 15 / 387.1 MiB / 4471        | 1 / 11.0 / 150                | 29.8x |
| node:20 2GB      | 43.5 MiB | 342       | 13 / 309.3 MiB / 3613        | 1 / 17.4 / 214                | 16.9x |

Boot-set scatter (from boot_profile): python_full 248 blk / 32.5 MiB across 7 of 9 packs;
node_full 5 of 9 packs (truncated, conservative); slim images 1-2 packs but still 4-9
readahead GETs due to WITHIN-pack scatter.

## Certain conclusions
1. Real boot working sets are SMALL (0.5-6% of image) but SCATTERED across most packs
   (5-7 of 9) and within packs (directory/DFS layout, no reorder today).
2. Automatic 32MiB readahead is ESSENTIAL (demand 349 GET -> readahead 9) but FAR from
   optimal: 4-15 GETs and 2-12x OVER-FETCH for real app images (python_full pulls 387 MiB
   to serve 31 MiB).
3. A contiguous reorder (boot-set) = 1 GET, zero over-fetch, MEASURED. Win 4.5x-29.8x for
   real app images; grows with image size; robust across RTT (RTT savings) and throughput
   (byte savings) because over-fetch dominates.
4. Only trivially small single-pack images (busybox) see no benefit (1.0x tie). -> ship the
   boot-set for real images; skip it for tiny single-pack bases.

## BEST MECHANISM PER TYPE (empirically established — see per_type_mechanisms.txt)
The choice hinges on ONE binary property: can `bless` reorder the image layout? EROFS yes
(PriorityOrder); ext4/raw/layered no. Two mechanism classes, both proven:

- **EROFS → build-time reorder** (contiguous prefix, 1 GET): 60-150ms, guaranteed 1 RTT
  regardless of scatter. Best overall. Proven on real images.
- **ext4 / raw / layered → runtime parallel PRECISE warm** of the captured boot block list
  (fetch EXACT blocks concurrently at device open, window=0, zero over-fetch). ALL THREE
  DIRECTLY MEASURED on real images+traces (see per_type_mechanisms.txt):
    - ext4 (`bless --oci`): 770→349ms (2.2×).
    - raw (`bless --image`, real raw ext4 disk image, 3 of 5 packs): 584→351ms (1.7×).
    - layered (`--oci --layered`, real 4-layer overlay profile over 4 ublk devices): 528→352ms
      (1.5×). NEW: layering gives natural per-layer locality (each layer's boot files cluster in
      its own packs) → readahead already decent (4 GETs, 1.2× over-fetch); the warm's win is
      mostly bytes (7.2 vs 36.8 MiB). Best as a per-layer-COALESCED precise warm. Reorder
      impossible (shared immutable layers).
  Monolithic types (ext4/raw) scatter across the whole image → bigger warm win; the win scales
  with scatter and with warm concurrency (GET/byte counts are concurrency-independent).
- **Tiny single-pack (busybox): readahead already ~optimal** (59ms); reorder/warm marginal.

**CRITICAL negative result:** do NOT build the ext4/raw warm as a parallel 32MiB-window-
coalesced fetch — concurrent reads race and pull overlapping windows → 872-2261 MiB fetched
for a ~10 MiB boot set, 9-23s (~20x WORSE than readahead). Warm the PRECISE blocks only.

## Remaining (implementation, not fact-finding)
- Phase 2 derivation: map boot blocks -> file paths for PriorityOrder (EROFS); the profiler
  already produces the block set and IS the production source.
- Phase 3 wiring: EROFS PriorityOrder->prefetch_len->bounded 1-GET prefix warm; ext4/raw/layered
  bounded parallel PRECISE block-list warm on device open (NOT window-coalesced).
- Optional confirmatory bless+profile of a raw image (≈ext4) and a layered image (needs a small
  profiler tweak to serve images/<name>); mechanism is already determined by reorderability.
