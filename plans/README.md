# Glide v2 — Implementation Plans

Phasing strategy for GLIDEv2. Each phase produces a testable milestone, unblocks the next phase, and maximizes parallel work streams.

**Guiding principle:** The critical path is write -> flush -> fork. Everything else is either parallel work or optimization that can be layered in later. Never let the critical path wait on non-critical work.

**Design doc:** [GLIDEv2.md](../GLIDEv2.md)

---

## Dependency Graph

```
Phase 1: Core Types + Write Path
    |-->  Phase 2: Content-Addressed S3 (flush path)
    |         `--> Phase 4: Fork + Snapshot <--,
    |                  `--> Phase 6: Scheduling |
    `--> Phase 3: New Read Path ----------------'
              `--> Phase 5a: foyer Cache

Phase 5b: GC (starts after Phase 2, parallel with 4+)
Phase 5c: Bless Pipeline (starts after Phase 2, parallel with everything)
Phase 7: Performance Polish (after Phase 5a)
```

---

## Phases

| Phase | Description | Depends On | Parallel With | Critical Path? | Doc |
|-------|-------------|-----------|---------------|----------------|-----|
| **1** | Core types + write path | v1 codebase | -- | Yes | [phase-1-core-types.md](phase-1-core-types.md) |
| **2** | Content-addressed S3 | Phase 1 | Phase 3 | Yes | [phase-2-content-addressed-s3.md](phase-2-content-addressed-s3.md) |
| **3** | New read path | Phase 1 | Phase 2 | Yes | [phase-3-read-path.md](phase-3-read-path.md) |
| **4** | Fork + snapshot | Phase 2, 3 | Phase 5a, 5b, 5c | Yes | [phase-4-fork-snapshot.md](phase-4-fork-snapshot.md) |
| **5a** | foyer cache | Phase 3 | Phase 4, 5b, 5c, 6 | No | [phase-5a-foyer-cache.md](phase-5a-foyer-cache.md) |
| **5b** | Garbage collection | Phase 2 | Phase 4, 5a, 5c, 6 | No | [phase-5b-gc.md](phase-5b-gc.md) |
| **5c** | Bless pipeline | Phase 2 | Phase 4, 5a, 5b, 6 | No | [phase-5c-bless-pipeline.md](phase-5c-bless-pipeline.md) |
| **6** | Flush scheduling | Phase 4 | Phase 5a, 5b, 7 | No | [phase-6-flush-scheduling.md](phase-6-flush-scheduling.md) |
| **7** | Performance + polish | Phase 5a | Phase 5b, 6 | No | [phase-7-performance.md](phase-7-performance.md) |

---

## Critical Path

```
Phase 1 --> Phase 2 --,
                      +--> Phase 4 --> Phase 6
Phase 1 --> Phase 3 --'
```

Everything else is parallel work that doesn't block the core product.

## Parallelism Opportunities

After **Phase 1 completes**, two developers can split:
- Developer A: Phase 2 (flush path)
- Developer B: Phase 3 (read path)

After **Phase 2 completes**, additional parallel streams open:
- Phase 5b (GC) -- can start immediately
- Phase 5c (bless pipeline) -- can start immediately

After **Phase 4 completes** (fork works), the product is usable. Everything after Phase 4 is hardening, optimization, and operational maturity.

## Estimated LOC

| Phase | Production | Tests | Total |
|-------|-----------|-------|-------|
| **1** Core Types + Write Path | ~1,800 | ~2,500 | ~4,300 |
| **2** Content-Addressed S3 | ~1,600 | ~2,000 | ~3,600 |
| **3** New Read Path | ~700 | ~1,000 | ~1,700 |
| **4** Fork + Snapshot | ~1,500 | ~2,000 | ~3,500 |
| **5a** foyer Cache | ~500 | ~500 | ~1,000 |
| **5b** GC | ~1,500 | ~1,200 | ~2,700 |
| **5c** Bless Pipeline | ~600 | ~500 | ~1,100 |
| **6** Flush Scheduling | ~1,000 | ~1,000 | ~2,000 |
| **7** Performance + Polish | ~1,000 | ~1,000 | ~2,000 |
| **Total** | **~10,200** | **~11,700** | **~21,900** |
