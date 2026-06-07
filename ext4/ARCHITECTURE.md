# ext4 Architecture

Takes an OCI container image (as tar streams or a programmatic file tree) and produces a deterministic, byte-identical filesystem image — either **ext4** (read-write, kernel-native) or **EROFS** (read-only, kernel-native). The same image built twice from the same input is byte-identical, enabling content-addressed deduplication across all GlideFS block storage.

## What This System Does

```
Input:  OCI tar layers (ordered bottom-to-top) OR programmatic create/write calls
Output: A valid, e2fsck-clean filesystem image with identical bytes for identical input

ext4 path  → mutable volume (VMs fork and write into it via CoW)
EROFS path → immutable rootfs (containers mount it read-only; writes go to an overlay upper)
```

Validated externally by `e2fsck`, `fsck.erofs`, real kernel loop-mounts, and ublk-served kernel mounts in CI.

## Data Flow

### OCI Merge → ext4 or EROFS

```
OCI layers [bottom-to-top]
     │
     ▼
Phase 1: build_ownership_map()        ← scan top-to-bottom
     │   owner: path → winning layer
     │   deleted: paths killed by .wh.*
     │   opaque: dirs whose lower contents are erased
     │
     ▼
Phase 2: write_layer_entries()        ← stream bottom-to-top into sink
     │   skip: whiteouts, deleted, other-owned
     │   write: make_parents + create + io::copy
     │
     ▼
  FsSink (ext4::Writer OR erofs::Writer)
     │
     ├─── ext4 path ──────────────────────────────────────────────────►  ext4 image
     │    Sequential streaming write (seeks only for superblock at close)
     │
     └─── EROFS path ─────────────────────────────────────────────────►  EROFS image
          Pass 1: assign inode slots (meta offsets)
          Pass 2: assign data block addresses
          Pass 3: stream to sink (1 MiB windows; spool file for content)
```

### Programmatic Write Path (ext4 Writer)

```
Writer::new(out, opts)
  │  reserves superblock + group descriptor space (seeked past, written at close)
  │
  ├── make_parents("a/b/c")        allocates inode per missing dir segment
  │
  ├── create("a/b/c/file", &File)
  │     ├── finishes any prior inode (writes extents + inode slot)
  │     ├── regular files: sets cur_inode, aligns if AlignData, records data_start_block
  │     ├── symlinks ≤59 bytes: inline target in inode.data
  │     ├── inline files (≤136 bytes): content in inode.data + system.data xattr
  │     └── dirs/devices/fifos: metadata committed immediately
  │
  ├── write(&[u8])                 streams into data blocks, skipping reserved regions
  │     └── when size reached → finish_inode()
  │           ├── physical_runs() — compute non-reserved spans
  │           ├── build extent tree (depth 0 inline, depth 1 on-disk)
  │           └── write inode to table via seek
  │
  └── close()
        ├── write_directory_recursive()  — all dirs, inode numbers now known
        ├── reserve_contiguous(journal)  — journal inode + jbd2 SB (if enabled)
        ├── reserve_contiguous(inodes)   — inode table (all groups)
        ├── reserve_contiguous(bitmaps)  — block + inode bitmaps
        └── seek to block 0: write superblock + group descriptors
```

### EROFS Write Path (Three Passes)

```
erofs::Writer collects a Node tree (in-memory; file content spooled to disk)

Pass 1 — Assign inode slots (32 bytes each, packed in metadata region)
  DFS traversal (priority files first if PriorityOrder set)
  → meta_off advances per inode; skips to next 4 KiB block on boundary crossing
  → each node gets nid = meta_off / 32

Pass 2 — Assign data block addresses
  In layout order:
  → if AlignData and file ≥ min_size and NOT priority: snap next_blk to grid boundary
  → blkaddr = next_blk; next_blk += nfull
  → track priority_end_blk (the prefetch_len boundary)

Pass 3 — Stream to output sink (bounded memory, 1 MiB window)
  For each node:
  → write inode header (32 bytes) at nid × 32
  → write inline xattrs (immediately after inode slot)
  → write inline tail (sub-block remainder, after xattrs)
  → write full blocks (streamed from spool file in 1 MiB chunks)
  → zero-fill any trailing <BLK gap to reach total_blocks × 4096
```

**Memory bound:** file content is spooled to `SpoolDir` (default `/var/tmp`, disk-backed) during ingest; serialize reads back in ≤ 1 MiB windows. Peak RAM is O(tree metadata), independent of image size.

### Read Path (ext4 → tar)

```
ext4 bytes
  │
  Reader::new()  → parse superblock (magic, block size, inode counts) + group descriptors
  │
  Reader::walk()
  │  └── walk_recursive(root_inode=2, prefix="")
  │        ├── read_inode() → GroupDescriptor → inode table offset → 256-byte inode
  │        ├── read_dir()   → read_data() → parse DirEntry records (skip ".", "..")
  │        └── for each child: read_inode + xattrs + symlink/device data; recurse dirs
  │
  Reader::to_tar()
  │  ├── walk() → Vec<WalkEntry>
  │  └── for each entry:
  │        ├── S_IFREG: tar Regular + PAX xattrs + streaming read_data_to()
  │        ├── S_IFDIR: tar Directory
  │        ├── S_IFLNK: tar Symlink
  │        ├── S_IFCHR/BLK: tar Char/Block with major/minor
  │        ├── S_IFIFO: tar Fifo
  │        └── hard link (links_count > 1, seen inode): tar Link (no data)
  └── tar stream → W
```

### Diff Path (ext4 A + ext4 B → delta tar)

```
Image A (base)  ──► walk() ──► BTreeMap<path, entry>
Image B (target) ─► walk() ──► iterate entries
                                     │
                   ┌─────────────────┼─────────────────┐
                   ▼                 ▼                  ▼
                 added           modified            deleted
                   │                │                   │
               full tar         full tar            .wh.<name>
               entry            entry              (single .wh. for dir;
                                                    child entries suppressed)
```

## Concepts & Terminology

| Term | What It Controls | NOT |
|------|-----------------|-----|
| Block | 4096-byte storage unit; all addressing is in blocks | Not a "sector" — no 512-byte units anywhere |
| Block group | 32,768-block region with its own inode table, bitmaps, and GD | Not a flex group — all metadata clusters at the end (flex_bg) |
| Inode | 256-byte on-disk record for one file/dir/symlink/device | Not a directory entry — dirents are separate and sorted |
| Extent | Contiguous disk run: `(logical_block, phys_block, length)` | Not a block pointer — no indirect blocks; extent tree only |
| Inline data | File content in inode.data (60 B) + system.data xattr (76 B) | Not a RAM buffer — it's on-disk in the inode slot itself |
| Whiteout | `.wh.<name>` → char device 0,0 marking deletion for overlayfs | Not a deletion — the inode exists; the overlay skips it |
| Opaque whiteout | `.wh..wh..opq` → `trusted.overlay.opaque=y` xattr on the dir | Not per-file — erases the entire directory's lower contents |
| Determinism | Same inputs → byte-identical output, every run | Not just "reproducible build" — includes inode slot layout, dir entry order, all padding bytes |
| nid | EROFS inode number = `byte_offset / 32` | Not an ext4 inode number — EROFS inodes are 32 bytes, not 256 |
| FLAT_INLINE | EROFS layout: full blocks in data region + sub-block tail inline after the inode | Not FLAT_PLAIN, which has no inline tail (used when tail+inode would cross a 4 KiB boundary) |
| Priority region | The contiguous leading byte range `[0, prefetch_len)` of an EROFS image covering the boot working set | Not the hot set — that was every non-zero block; this is trace-derived and bounded |
| Spool | Temp file on real disk (`/var/tmp`) where EROFS file content accumulates during ingest | Not in-memory — specifically avoids tmpfs so peak RAM stays bounded |

## On-Disk Layouts

### ext4 Image

```
Byte 0..1024:       [boot sector — zeros]
Byte 1024..2048:    SuperBlock (primary)
Byte 2048..4096:    [zeros]

Block 1..1+gd_blocks:   Group Descriptor Table (32 bytes per group)

--- Data region (streamed forward) ---
  file data, dir blocks, xattr blocks, extent index blocks, journal (optional)
  ⟂ Reserved holes at sparse_super groups (0, 1, 3, 5, 7, 9, 25, 27, ...):
    first (1 + gd_blocks) blocks of each group hold backup SB + GDT copy;
    data fragments around them; contiguous structures placed past them

--- Trailing metadata (flex_bg, single contiguous run at end) ---
  Inode Table   — groups × inodes_per_group × 256 bytes
  Block Bitmap  — 1 block per group (4096 × 8 = 32,768 bits)
  Inode Bitmap  — 1 block per group

Superblock and primary GDT written at close() via seek to block 0.
```

Key superblock values (all little-endian):

| Field | Value | Note |
|-------|-------|------|
| magic | 0xef53 | ext4 signature |
| log_block_size | 2 | block = 1024 << 2 = 4096 |
| blocks_per_group | 32,768 | 8 blocks × 4096 bits |
| inode_size | 256 | always |
| feature_incompat | FILETYPE \| EXTENTS \| FLEX_BG [+ INLINE_DATA] | |
| feature_ro_compat | SPARSE_SUPER \| LARGE_FILE \| HUGE_FILE \| EXTRA_ISIZE | |
| journal_uuid | 0 (always) | non-zero → kernel searches for external journal; we never want that |
| mtime, wtime | 0 | determinism |
| hash_seed | uuid[0..16] reinterpreted as 4 × u32 | directory hashing |

### EROFS Image

```
Block 0: [0..1024 reserved | 1024..1152 superblock | 1152.. metadata region]
Metadata region: compact inodes (32 bytes each) packed at nid × 32,
                 never crossing a 4 KiB boundary; inline xattrs + inline
                 tail immediately after each inode.
Data region:    full 4 KiB blocks for large file payloads, starting at the
                next block boundary after metadata. Priority files packed
                contiguously at the front; unprioritized files optionally
                grid-aligned (holes are zeros, never stored downstream).
```

Key superblock values (all little-endian):

| Field | Offset | Value |
|-------|--------|-------|
| magic | 0x00 | 0xE0F5_E1E2 |
| blkszbits | 0x0C | 12 (4 KiB blocks) |
| root_nid | 0x0E | nid of root inode |
| inos | 0x10 | total inode count |
| blocks | 0x24 | total block count |
| uuid | 0x30 | caller-provided (16 bytes) |

EROFS compact inode (32 bytes, little-endian):

| Field | Bytes | Note |
|-------|-------|------|
| i_format | 0..2 | datalayout << 1; bit 0 = 0 (compact version) |
| i_xattr_icount | 2..4 | inline xattr slots |
| i_mode | 4..6 | standard POSIX mode |
| i_nlink | 6..8 | |
| i_size | 8..12 | |
| i_u | 16..20 | blkaddr (data), rdev (devices), or 0xffffffff (inline-only) |
| i_ino | 20..24 | nid + 1 (informational) |
| i_uid | 24..26 | |
| i_gid | 26..28 | |

## Core Mechanisms

### Reserved-Block Skipping (ext4 Writer)

With `sparse_super`, block groups 0, 1, and every power of 3, 5, and 7 reserve their first `1 + gd_blocks` blocks for a backup superblock + GDT copy. **Data must never claim these blocks** — an overlapping extent is a kernel-rejected multiply-claimed block (`e2fsck` catches this; the in-crate reader is lenient and historically hid the bug).

The allocator keeps data off these blocks via two mechanisms:
- **File data** fragments around them: `write_file_data` streams up to the reserved boundary, seeks past it, and continues. The extent tree records logical-contiguous, physical-fragmented runs.
- **Contiguous structures** (journal, inode table, bitmaps) use `reserve_contiguous(n)` — places the entire run *past* any reserved region it would straddle; the skipped lead-in blocks become free holes cleared in the bitmap at `close()`.

### Extent Tree Construction (ext4 Writer)

```
finish_inode():
  runs = physical_runs(data_start..end)   // non-reserved spans only
  leaves = split each run into ≤32,768-block extents (logical offset
           advances over data blocks; reserved gaps are invisible to the file)
  ≤4 leaves    → inline in inode.data (depth 0)
  ≤4×340 leaves → one index level (depth 1); leaf blocks also skip reserved
  else          → error (file too large for two-level tree)
```

Logical block numbering never skips — the file appears contiguous. Physical block assignment skips reserved regions silently.

### Determinism

Same input → byte-identical output. Achieved by:
1. **No random state** — UUID is caller-provided or all-zeros; never `rand`
2. **Zero timestamps** — `mtime=0`, `wtime=0` in superblock; file times from caller
3. **Sorted collections** — `BTreeMap` for all children and xattrs
4. **Deterministic layout** — directory sort order is `(inode_number, name)`; xattr sort order is `(index, name_len, name)`; inode allocation is sequential insertion order; block group count is a pure function of max disk size
5. **Zero padding** — every byte explicitly written or zero-filled; no uninitialized memory

### OCI Layer Merge (Two-Phase)

**Phase 1** scans layers top-to-bottom, recording which layer wins each path and which paths `.wh.*` entries delete. Opaque whiteouts (`trusted.overlay.opaque`) are applied last: any path owned by a layer below the opaque layer and under the opaque dir is added to the deleted set.

**Phase 2** streams layers bottom-to-top through the filesystem sink, skipping deleted, whiteout-marker, and non-owning-layer entries. The sink sees only the final merged state.

The `FsSink` trait makes the merge output format-agnostic: the same two-phase driver targets `ext4::Writer` (default) or `erofs::Writer` (`--erofs`).

### EROFS Priority Ordering

When `PriorityOrder` is set, the layout order is: root, then each named priority path in caller order (deduplicated, missing paths silently skipped), then everything else in DFS name order.

Priority files are packed **tight** (no alignment gaps applied to them): holes between them break coalescing in the downstream block store, which reads at 128 KiB granularity. The priority region — everything from image byte 0 through the last priority file's data — is returned as `prefetch_len`, which the block server warms into the clean cache on device open so the guest's boot reads are cache hits.

## State Machine (ext4 Writer)

The writer enforces a forward-only ingest state:

```
UNINITIALIZED ──new()──► READY
                              │
                  ┌───────────┴───────────┐
                  ▼                       ▼
             create()              make_parents()
                  │                (stays READY)
                  ▼
           WRITING_INODE ──write()──► WRITING_INODE
                  │
          declared size reached
                  │
                  ▼
              finish_inode()
              (READY again)
                  │
                  ▼
              close()
                  │
                  ▼
            FINALIZED (image returned)
```

| From | Event | To | What Actually Happens |
|------|-------|----|-----------------------|
| READY | `create(regular)` | WRITING_INODE | Aligns if AlignData; records `data_start_block`; sets `cur_inode` |
| WRITING_INODE | `write(buf)` | WRITING_INODE | Streams into blocks, skipping reserved regions |
| WRITING_INODE | size reached | READY | `write_extents` → `write_inode` via seek; `cur_inode` cleared |
| WRITING_INODE | `create()` again | WRITING_INODE → READY → WRITING_INODE | Prior inode finished first (extent tree + inode write) |
| READY | `close()` | FINALIZED | All dirs written; journal, inode table, bitmaps placed; SB written |
| WRITING_INODE | `write()` past declared size | error | `io::Error("unexpected data")` — caller must write exactly `size` bytes |

## Design Decisions

### Why two output formats?

ext4 is writable — VMs fork-and-write into it via the GlideFS CoW block layer. EROFS is read-only — containers mount it read-only and write to an overlay upper. OCI image layers are immutable by definition, so EROFS is the semantically correct format for serving them daemonless (kernel `erofs` over ublk). Both share the same ingest and merge driver; only the sink differs.

### Why defer directory writes in ext4 to close()?

Directory entries contain inode numbers. Hard links and `link()` calls assign the same inode to multiple paths; link counts aren't final until all `create()` and `link()` calls are done. Writing directories at `close()` guarantees correct link counts and removes the need to rewrite directory blocks.

### Why three passes in the EROFS writer?

EROFS requires that every inode know its nid before any directory can be encoded (directories store child nids), and data blocks can only be assigned after the metadata region is sized. The three passes respect these dependencies without buffering the image in RAM: Pass 1 and 2 are pure arithmetic over the in-memory Node tree; Pass 3 streams to disk.

### Why spool to disk (not RAM) in the EROFS writer?

The system's `/tmp` is tmpfs (RAM-backed). Spooling file content there would accumulate the uncompressed image in RAM, defeating the bounded-memory design. `bless_scratch_dir()` → `$GLIDEFS_BLESS_WORKDIR` → `/var/tmp` (FHS persistent temp, on the NVMe) → system temp. `/var/tmp` is on real disk here and can absorb a multi-GB image.

### Why no checksums?

`METADATA_CSUM` and `GDT_CSUM` are not enabled. The ext4 images are content-addressed externally by BLAKE3 over the full bytes, and validated against real `e2fsck` and kernel mounts in CI. Internal checksums would be redundant and would break if we ever needed to rewrite a field (e.g., during handoff), since they're UUID-seeded.

### Why `journal_uuid = 0` always?

A non-zero `journal_uuid` signals to the kernel that the journal is external and triggers a search for a block device with that UUID. We never want this. The journal inode is inode 8, identified by `journal_inum` in the superblock. `journal_uuid` is set to all-zeros regardless of the filesystem UUID.

### Why sort directories by (inode_number, name)?

Inode number is stable across layer merges — the same file always gets the same inode slot within one image build. Sorting by inode_number first yields directory blocks that are stable across re-ordering of `create()` calls on the same file set, which directly serves determinism without requiring a second sort on name alone.

### Why is AlignData safe in EROFS but not (historically) in ext4?

EROFS has no interleaved block-group metadata — the data region is a flat run. Alignment holes are unreferenced zeros; they can never land on a reserved block because there are no reserved blocks. The ext4 writer has `is_reserved_block` + `skip_reserved_at_pos` to handle this correctly (the group-aware allocator landed in commit `74b07b2`), so `AlignData` is now safe in both formats.

### Why aligned = dedup + priority = cold-start speed?

Grid alignment (128 KiB = the GlideFS block store's content-addressed unit) makes a file's downstream block hashes stable regardless of upstream layout churn — the same file in two different images hits the same blake3 hash at the same block offset, so the block store deduplicates it. Priority ordering makes the boot working set contiguous, so the server can coalesce it into one S3 ranged GET instead of many scattered fetches. The two optimizations are **complementary, not alternatives**: priority files are packed tight (no alignment gaps) so their run coalesces; the tail of the image is aligned for at-rest dedup.

## Package Structure

| File | What It Actually Does |
|------|-----------------------|
| `src/lib.rs` | Public API: `Writer`, `Reader`, `File`, `WriterOption`, `convert_*` functions |
| `src/writer.rs` | ext4 streaming writer. Sequential forward allocation with reserved-block-aware fragmentation (`is_reserved_block`, `skip_reserved_at_pos`, `reserve_contiguous`). Extent tree construction. Optional alignment with free-hole accounting. Optional JBD2 journal. Superblock and group descriptors written at `close()`. |
| `src/erofs.rs` | EROFS writer. Three-pass layout: meta-offset assignment → data-block assignment → streaming serialization via spool. Priority ordering + grid alignment. `close_with_prefetch()` returns the boot working-set extent. Memory-bounded via disk spool (1 MiB stream window). |
| `src/reader.rs` | ext4 parser. Reads superblock, group descriptors, inode table, extent trees, directory entries, xattrs. Exports via `walk()` (metadata) and `to_tar()` (full content). Streaming `read_data_to()` for large files. |
| `src/format.rs` | On-disk binary structures shared by reader and writer: `SuperBlock`, `GroupDescriptor`, `ParsedInode`, `ExtentHeader/Leaf/Index`, `DirEntry`, xattr codec. Both serialization and deserialization. |
| `src/tar_convert.rs` | OCI layer merge driver. Two-phase (ownership scan → stream write). `FsSink` trait makes output format-agnostic. Handles `.wh.*` whiteouts, `.wh..wh..opq` opaque whiteouts, PAX xattrs, hard links. Exposes both merged (`convert_oci_layers_to_*`) and overlay-preserving single-layer (`convert_layer_to_*`) variants. |
| `src/diff.rs` | Incremental export: diffs two ext4 snapshots, produces a minimal OCI delta tar with whiteout markers. Directory deletion emits a single `.wh.` (child entries suppressed). |
| `tests/erofs_validity.rs` | EROFS correctness harness: byte-determinism, `fsck.erofs`, xattrs, alignment dedup proof (churn test), priority ordering (offset flip), edge cases (missing paths, empty list, mixed types). Kernel loop-mount gated on `EROFS_MOUNT_TEST=1`. |
| `tests/fsck_validity.rs` | ext4 correctness harness: `e2fsck -fn` on single/multi-group images, alignment, journal boundary, inode-table boundary, fuzz over random filesets. Kernel loop-mount gated on `EXT4_MOUNT_TEST=1`. |

## WriterOptions

| Option | Format | Default | What It Controls at Runtime |
|--------|--------|---------|----------------------------|
| `InlineData` | ext4 | off | Files ≤136 bytes stored in inode.data + system.data xattr; no data blocks allocated. |
| `MaximumDiskSize(n)` | ext4 | 16 GiB | Filesystem size cap; controls group descriptor count. Enforced on every `write_bytes()`. |
| `Uuid([u8;16])` | both | all-zeros | Filesystem UUID → superblock + directory hash seed. Content-derived for reproducibility. |
| `Journal(blocks)` | ext4 | off | Create JBD2 journal as inode 8; set `HAS_JOURNAL`. `journal_uuid` stays zero (non-zero → kernel searches for external journal). |
| `AlignData{align,min_size}` | both | off | Snap each file ≥ `min_size` to an `align`-byte data-region boundary; gap is a free hole (zeros, never stored downstream). Makes large files hash identically across layout churn. |
| `PriorityOrder(paths)` | EROFS | none | Place named files first and tight (no alignment) so the boot working set is one contiguous, coalescing-friendly run. Returns `prefetch_len` from `close_with_prefetch()`. |
| `SpoolDir(dir)` | EROFS | system temp | Write file content spool here instead of `$TMPDIR`. Use a real-disk path (e.g. `/var/tmp`) when `$TMPDIR` is tmpfs so the memory bound actually holds. |

## Limits

| Resource | Limit | Reason |
|----------|-------|--------|
| File size (ext4) | 128 GiB | Two-level extent tree: 4 index entries × ~340 leaves × 32,768 blocks |
| Inline data | 136 bytes | 60 (inode.data) + 104 (extra) − 28 (system.data xattr overhead) |
| Short symlink | 59 bytes | Stored in inode.data without extent tree |
| Hard links per inode | 65,000 | ext4 `MAX_LINKS` |
| xattr total per inode | ~4 KiB | One xattr block; no multi-block xattr support |
| ext4 filesystem | 16 TiB | `MAX_MAX_DISK_SIZE`; group descriptor table preallocated at `new()` |
| EROFS inode depth | flat | Compact inodes only; no large or extended inodes |
| EROFS block size | 4 KiB | Fixed; `BLKSZBITS = 12` |

## Failure Modes

| Failure | What Actually Happens |
|---------|-----------------------|
| `write()` before `create()` | Panic: `write_data called with no inode in progress` |
| Write more bytes than declared `size` | `io::Error("unexpected data after declared size")` |
| Write fewer bytes than declared `size` | `io::Error` detected at `finish_inode()` |
| xattr too large for inode + block | `io::Error` from `create()`: can't fit xattr |
| Link count overflow (>65,000) | `io::Error` from `link()` or `create()` |
| Overwrite dir with file (or vice versa) | `io::Error` from `create()` |
| Overwrite inode with extent data | `io::Error`: cannot replace inode with non-inline data |
| File larger than 128 GiB | `io::Error`: file too large for two-level extent tree |
| Bad superblock magic (reader) | `io::Error(InvalidData, "bad superblock magic: 0xNNNN")` |
| Indirect-block inode (reader) | `io::Error(Unsupported, "only extent-based inodes are supported")` |
| Bad extent header magic (reader) | `io::Error(InvalidData, "bad extent header magic: 0xNNNN")` |
| Directory cycle (reader) | `io::Error(InvalidData, "directory cycle detected at inode N")` |
| Non-UTF-8 name (reader) | `io::Error(InvalidData, "non-UTF-8 name")` — not silently dropped |
| EROFS spool dir missing | `io::Error` from first `write()` call — spool created lazily |
| EROFS priority path not found | Silently skipped — image still valid, just no priority for that path |
| EROFS priority path is a directory or symlink | Silently skipped — only regular files can be priority-ordered |

## Testing

Three tiers verify correctness from different angles:

**Kernel-grade oracles** — The in-crate reader is lenient. `fsck_validity.rs` and `erofs_validity.rs` validate against `e2fsck`/`fsck.erofs` and real kernel loop/ublk mounts (gated on env vars). These are the only tests that would have caught the original multi-group reserved-block corruption.

| ext4 Test | What It Proves |
|-----------|---------------|
| `fsck_single/multi_group_clean` | e2fsck clean at 1-group and 3-group boundaries |
| `fsck_multi_group_aligned_clean` | AlignData output is e2fsck-clean (padding marked free, never overlaps reserved blocks) |
| `content_survives_fragmentation` | Files split around reserved blocks read back byte-exact |
| `fsck_journal_straddles_boundary` | Journal never straddles a backup superblock (swept 120–136 MiB) |
| `fsck_inode_table_straddles_boundary` | Inode table never straddles a backup superblock |
| `fuzz_multigroup_validity_and_content` | Random filesets, both align modes: e2fsck-clean AND byte-exact content (`EXT4_FUZZ_SEEDS` to scale) |
| `kernel_mount_content` | Real loop-mount (`EXT4_MOUNT_TEST=1`): every file byte-exact vs known input |

| EROFS Test | What It Proves |
|------------|---------------|
| `erofs_merge_is_byte_deterministic` | Same layers + UUID → byte-identical image |
| `erofs_merge_fsck` | `fsck.erofs` clean on a representative multi-layer OCI merge |
| `erofs_merge_kernel_mount` | Real kernel mount (`EROFS_MOUNT_TEST=1`): override/whiteout/opaque/symlink/hardlink all correct |
| `erofs_alignment_recovers_dedup_under_churn` | 0/4 → 4/4 blocks dedup when 37 extra files are added (proves the grid-alignment claim) |
| `erofs_priority_order_places_boot_set_first` | Priority file (created last) lands before non-priority file (created first); fsck-clean |
| `erofs_priority_order_edge_cases` | Missing paths skipped; dirs/symlinks in list handled; tight packing; determinism |
| `erofs_xattrs` | user.*, security.capability, trusted.overlay.opaque round-trip via fsck + getfattr |
| `erofs_served_over_real_ublk_kernel_mount` | Real ublk device + kernel erofs mount over glidefs BlockHandler; content correct |
| `erofs_aligned_prioritized_served_over_ublk` | 1.5 MB multi-grid priority file + aligned mediums + 40 inline smalls + symlink, byte-exact over ublk |

**Writer unit tests** (`src/tests.rs`) cover basic files, hard links, symlinks, devices, inline data boundary, xattrs, file replacement, large files (600 MiB+), large dirs (50K entries), large filesystems (16 TiB), and SHA-256 determinism.

**Reader roundtrip tests** (`src/tests.rs`) write with the writer, read back with the reader, and assert metadata + content correctness for all inode types including hard links, devices, and inline data.

**Diff tests** verify added/modified/deleted detection; single `.wh.` entry for directories; metadata-only changes; and nested whiteout paths.

**BlockAdapter roundtrip** (`glidefs/src/oci/block_adapter.rs`) writes a merged EROFS into a real GlideFS volume (InMemory store) and reads it back byte-exact — the same path production bless uses.
