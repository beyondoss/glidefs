# Boot-set vs automatic readahead — methodology & reproduction

**Question.** As of commit `07ae198` the trace-driven boot-set was removed in favor of the
automatic 32 MiB pack-window readahead. Can an explicit, precomputed boot-set beat that
baseline for cold image boot — derived WITHOUT strace and WITHOUT booting in production VMs —
and prove it empirically? (Full design: `~/.claude/plans/i-d-like-to-explore-enumerated-reddy.md`.)

## The two experiments

### A. Synthetic scatter sweep (controlled, statistical)
`glidefs/tests/integration/boot_replay.rs::full_study` (gated by `GLIDEFS_BOOT_REPLAY_FULL=1`).
- Builds images in InMemory S3 with a known boot set placed at controllable scatter
  (`scatter_stride`: 1 = clustered in 1 pack … 1024 = one boot block per pack).
- `LatencyStore` wraps the store and injects first-byte RTT + `bytes/throughput`, counting
  GETs and bytes by the returned RANGE (not full object).
- 4 arms, identical read path, differ only in prefetch policy / layout:
  `DemandOnly` (window=0), `Readahead` (32 MiB window = baseline), `BootSet` (reordered
  contiguous image + device-open prefix warm, TIMED on the critical path), `Warm` (lower bound).
- ≥25 trials/arm; reports median/p95, Mann-Whitney U (tie-corrected) + Hodges-Lehmann CI.
- Result: win is purely saved round-trips; clustered → tie, scattered → win ∝ (packs−1)×RTT.
  Raw: `synthetic_study_honest_timing.txt`, `synthetic_study_first_run_raw.txt`.

### B. Real-image study (the decisive facts)
1. **Bless** real OCI images to EROFS in MinIO via the real pipeline:
   `glidefs bless --oci <ref> --erofs --name <n> --s3-prefix prof --config repro-config.toml`.
2. **Profile** (capture the real boot working set) with the disposable profiler
   `glidefs/src/bin/boot_profile.rs` (ublk feature, run under sudo): it serves the blessed
   image through GlideFS over a throwaway ublk device with a read-fault recorder attached
   (`BlockHandler::with_read_tracer` → `TraceOp::Read` → `boot_set_from_trace`), kernel-mounts
   the EROFS, runs the entrypoint once in a chroot under a hard timeout, and records the blocks
   the kernel actually fetches in first-touch order. **No strace, no production VM** (eStargz
   `optimize` / REAP record model). Output: `*.bootset` (one block index per line).
3. **Replay** the captured trace through the REAL read path (`fetch_with_window`) against the
   real blessed image, via `boot_replay.rs::real_trace_study`: measures demand-only vs readahead
   GET count + bytes, and MEASURES the boot-set arm by reading the real boot blocks, laying them
   contiguous in a fresh image (real compression), and replaying.
- GET counts + bytes are deterministic/exact; wall is the build-independent model
  `GETs*RTT + bytes/throughput` (debug-build CPU excluded). Results: `real_study_results.txt`.

## Exact reproduction
```sh
# 0. MinIO (local throwaway)
docker run -d --name glidefs-minio -p 9100:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
docker run --rm --network host --entrypoint sh minio/mc -c \
  "mc alias set m http://127.0.0.1:9100 minioadmin minioadmin && mc mb -p m/glidefs"

# 1. bless (uses research-bootset/repro-config.toml → storage.url=s3://glidefs/prof @ :9100)
cargo run -p glidefs --bin glidefs -- bless --oci python:3.12 --erofs \
  --name python_full --s3-prefix prof --config research-bootset/repro-config.toml

# 2. profile (sudo: needs ublk + mount + chroot). GLIDEFS_PROFILE_TIMEOUT=secs.
cargo build -p glidefs --features ublk --bin boot_profile
sudo env GLIDEFS_PROFILE_TIMEOUT=90 GLIDEFS_BOOT_SET_OUT=/tmp/python_full.bootset \
  ./target/debug/boot_profile python_full prof research-bootset/repro-config.toml erofs \
  -- /usr/local/bin/python3 -c 'import json,ssl,sqlite3,urllib.request; print("ok")'

# 3. replay + measure
GLIDEFS_REAL_CONFIG=research-bootset/repro-config.toml GLIDEFS_REAL_PREFIX=prof \
GLIDEFS_REAL_RTT_MS=40 \
GLIDEFS_REAL_IMAGES="python_full:/tmp/python_full.bootset" \
  cargo test -p glidefs --features test-utils --test integration \
  boot_replay::real_trace_study -- --ignored --nocapture

# synthetic sweep
GLIDEFS_BOOT_REPLAY_FULL=1 cargo test -p glidefs --features test-utils --test integration \
  boot_replay::full_study -- --ignored --nocapture
```

## Files in this directory
- `FINDINGS.md` — the conclusions + headline table.
- `real_study_results.txt` — measured real-image table + scatter per image.
- `synthetic_study_honest_timing.txt` / `synthetic_study_first_run_raw.txt` — controlled sweep.
- `repro-config.toml` — the bless/profile config (local MinIO).
- `*.bootset` — captured real boot traces (block indices, first-touch order).

## Code (branch `jared/erofs`)
- `glidefs/src/block/write_cache/{inner,init,flush,read}.rs` — configurable readahead window.
- `glidefs/src/block/write_trace.rs` — `TraceOp::Read` + `boot_set_from_trace`.
- `glidefs/src/block/handler.rs` — `with_read_tracer` + read hook (None in prod).
- `glidefs/src/bin/boot_profile.rs` — disposable boot-fault profiler.
- `glidefs/tests/integration/boot_replay.rs` — harness (`full_study`, `real_trace_study`).
