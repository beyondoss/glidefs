# Write Cache Architecture

Write-behind block cache on local SSD with background flush to S3 as content-addressed packs.

## Data Flow

### Write Path (~10-15µs)

```
write(offset, data)
  │
  ├─ set_present(block)           atomic OR on bitmap
  ├─ pwrite(data, offset)         POSIX positional I/O, lock-free
  ├─ transition_to_dirty(block)   CAS on block_states
  ├─ block_map_set(ZERO, seq)     SeqLock write, placeholder hash
  └─ WAL append(ZERO, seq)        Mutex + BufWriter (uncontended)
```

Hash computation is **deferred to flush time**. The write path never reads back
from SSD, never hashes, never touches the clean cache.

### Read Path

```
resolve_chunk(idx)
  │
  ├─ block_map_get(idx) → hash
  │
  ├─ hash == ZERO + present? ────► SSD pread          (dirty, hash deferred)
  ├─ hash == ZERO + !present? ───► return zeros        (never written)
  ├─ hash == zero_block_hash? ───► return zeros        (trimmed)
  │
  ├─ Tier 1: clean_cache(hash) ─► hit? return          (~100ns mem)
  ├─ Tier 2: S3 pack(hash) ────► hit? decompress+cache (~10-50ms)
  └─ Tier 3: SSD pread ────────► fallback              (~µs, page cache)
```

### Flush Path (background, async)

```
flush_dirty_inner()
  │
  ├─ 1. Capture seq_cutpoint
  ├─ 2. Scan block_states for Dirty, collect (idx, seq)
  ├─ 3. For each: pread SSD → blake3 → dedup check → lz4 compress
  ├─ 4. Assemble packs → upload to S3 concurrently
  └─ 5. For each: if seq unchanged → block_map_set(real_hash) → CAS Dirty→Clean
```

## Concepts

| Term | Definition | NOT |
|------|-----------|-----|
| Block | Fixed-size chunk (typically 128KB) of the virtual device | Not a filesystem block |
| Present | Block has data on local SSD (bitmap flag) | Not "clean" — can be dirty AND present |
| Dirty | Block modified locally, not yet flushed to S3 | Not "absent" — dirty blocks are always present |
| Pack | S3 object containing up to 25 LZ4-compressed blocks | Not a single block — it's a batch |
| Block map | chunk_index → (Blake3Hash, sequence) mapping | Not the data itself — just metadata |
| Sequence | Monotonic counter incremented per write, used for snapshot consistency and race detection | Not a timestamp |
| ZERO hash | `Blake3Hash([0; 16])` sentinel meaning "hash not yet computed" | Not the hash of zero-filled data (that's `zero_block_hash`) |

## Block State Machine

```
            write()              flush complete
  Clean ──────────────► Dirty ──────────────► Clean
    ▲                     │  ▲
    │                     │  │ write() during flush
    │                     ▼  │  (Dirty→Dirty no-op)
    │                   Dirty
    │
    └── flush CAS ───── Dirty
        (seq mismatch?    │
         leave dirty) ────┘
```

| From | Event | To | Action |
|------|-------|----|--------|
| Clean | write() | Dirty | dirty_count += 1 |
| Dirty | write() | Dirty | no-op (already dirty) |
| Dirty | flush (seq match) | Clean | block_map_set(real_hash), dirty_count -= 1 |
| Dirty | flush (seq mismatch) | Dirty | skip — concurrent write detected |
| Syncing | write() | Dirty | syncing_count -= 1, dirty_count += 1 |

## Deferred Hashing

The write path stores `Blake3Hash::ZERO` as a placeholder in the block map.
The real content hash is computed at flush-to-S3 time, when the block is read
from SSD to build packs.

### Why defer hashing?

The previous design computed blake3 on every write. For a 4KB write to a 128KB
block, this meant:

| Operation | Cost | Purpose |
|-----------|------|---------|
| pread 128KB from SSD | ~15-25µs | Read full block for sub-block merging |
| blake3(128KB) | ~20-30µs | Content hash |
| Bytes::copy_from_slice(128KB) | ~5-10µs | Insert into clean cache |
| **Total overhead** | **~50-65µs** | None of this is needed until flush |

With deferred hashing, the write path is ~10-15µs (just pwrite + atomics + WAL).
The 128KB read/hash/copy moves to the flush path, which already reads every dirty
block from SSD to build packs — so the work happens exactly once, at the right time.

**Write coalescing is free**: write the same block 100 times before flush, hash it
once. Previously: 100 hashes, 99 thrown away.

### Why sequence numbers for race detection?

Flush needs to detect concurrent writes that happen between its SSD read and its
dirty-flag clear. Previously, the hash served as a version token (hash changed =
concurrent write). With deferred hashing, there's no hash at snapshot time.

The **sequence number** replaces the hash as the version token. Each write gets a
unique, monotonically increasing sequence. Flush records the sequence at snapshot
time and only clears the dirty flag if the sequence hasn't changed.

There is a narrow TOCTOU window between checking the sequence and doing the CAS
(nanoseconds). This is identical to the window in the previous hash-based approach
and is self-healing: the block will be re-written and re-dirtied by the next I/O.

### How the read path handles ZERO hashes

A ZERO hash in the block map means one of two things:

1. **Never written** — block has no data anywhere. Return zeros.
2. **Deferred hash** — block has data on SSD, hash not yet computed.

The read path distinguishes these using the **present bitmap**:

```rust
if hash.is_zero() {
    if self.inner.is_present(chunk_index) {
        return self.sync_read_local_block(chunk_index); // case 2
    }
    return Ok(self.inner.zero_block_bytes.clone());      // case 1
}
```

### Exception: zero_range

`zero_range()` uses the precomputed `zero_block_hash` (not ZERO) because the
content is known without reading SSD. Flush skips zero-hash blocks (they're never
uploaded), so this is both correct and efficient.

## Crash Recovery

Recovery replays the WAL and re-hashes dirty blocks from SSD:

```
open() → Recovering
  │
  ├─ Load persisted block_map from disk
  ├─ Replay WAL entries (re-read SSD, re-hash, update block_map)
  ├─ Load block_states from metadata (Syncing → Dirty)
  │
  └─ finish_recovery() → Active
       └─ verify_dirty_block_hashes()
            For each dirty block:
              pread SSD → blake3 → if hash != block_map → correct it
```

WAL entries store `Blake3Hash::ZERO` as the hash. Recovery ignores the WAL hash
and always re-computes from SSD — the SSD is the source of truth for block data.
The WAL only records *which blocks are dirty*.

## Concurrency Model

All hot-path operations are lock-free:

| Resource | Synchronization | Contention |
|----------|----------------|------------|
| `data_file` | pread/pwrite (POSIX atomic) | None — positional I/O is thread-safe |
| `block_states` | `AtomicU8` with CAS | Per-block — no false sharing |
| `present_chunks` | `AtomicU64` with fetch_or | Per-64-block word |
| `block_map` | SeqLock (per-entry `AtomicU32` version) | Per-block — sub-µs writes |
| `sequence` | `AtomicU64` fetch_add (Relaxed) | None |
| `wal` | `Mutex<Wal>` | Effectively uncontended (single writer per export) |
| `block_map` (RwLock) | parking_lot RwLock | Read lock on all ops; write lock only during rare flatten |

## Files

| File | Responsibility |
|------|---------------|
| `write.rs` | Write hot path: pwrite → dirty → WAL (deferred hash) |
| `read.rs` | Tiered read: block_map → cache → S3 → SSD fallback |
| `flush.rs` | Background S3 sync: hash → dedup → pack → upload → clear dirty |
| `init.rs` | Open/create cache, WAL replay, block_map loading |
| `recovery.rs` | Post-open: verify dirty block hashes against SSD |
| `inner.rs` | `CacheInner` shared state, `SyncFile`, metadata persistence |
| `config.rs` | `WriteCacheConfig` with file path helpers |
| `error.rs` | `CacheError` type |
| `mod.rs` | `WriteCache<S>` typestate wrapper, `FlushStats`, `SnapshotResult` |

## Performance

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Write (4KB random) | ~10-15µs | ~70-100K IOPS |
| Write (128KB sequential) | ~108µs | 1.1 GiB/s |
| Read (warm cache) | ~1.3µs | 733 GiB/s |
| Read (cold, SSD) | ~306µs | 3.2 GiB/s |
| Local flush (fsync) | <1µs | — |
| Flush to S3 (100MB) | ~18ms | 5.4 GiB/s |
| Manifest serialize (500K blocks) | 3.3ms | 149M entries/s |
