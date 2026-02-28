# ext4 Architecture

Bidirectional OCI bridge between tar archives and ext4 filesystem images. The writer produces deterministic, byte-identical ext4 images from tar archives or programmatic file trees. The reader parses those images back into tar streams for container export.

## Data Flow

### Write Path (tar → ext4 → bytes)

### Programmatic API

```
Caller
  │
  ├── Writer::new(output, options)
  │     └── reserves superblock + group descriptor space (seeked over)
  │
  ├── make_parents("a/b/c")   ── allocates inode per missing dir segment
  │
  ├── create("a/b/c/file", &mut File)
  │     ├── allocates inode number
  │     ├── if regular: sets cur_inode, prepares extent tree
  │     ├── if symlink ≤59 bytes: stores target inline in inode.data
  │     ├── if dir/device/fifo: writes inode immediately
  │     └── if inline_data: stores content in inode.data + xattr
  │
  ├── write(&[u8])             ── streams into data blocks, building extents
  │     └── finish_inode()    ── called when declared size is reached
  │           ├── writes pending extent index block (if depth 2)
  │           └── writes inode to inode table (seek + write)
  │
  └── close()
        ├── finalize_dirs()   ── for each dir inode: write directory block(s)
        ├── finalize_inodes() ── write xattr blocks, rewrite inode table entries
        └── write_superblock_and_groups() ── seek to block 0, fill metadata
              └── return output W
```

### Tar Conversion

```
tar::Archive<R>
  │
  for each entry:
  │
  ├── Whiteout? (.wh.*)
  │     ├── .wh..wh..opq  ──► stat(dir) + add "trusted.overlay.opaque=y" xattr + create(dir)
  │     └── .wh.<name>    ──► create char device 0,0 at stripped path
  │
  ├── Hard link            ──► link(target, name)
  │
  └── Regular/Symlink/Dir/Device/Fifo
        ├── make_parents(name)
        ├── create(name, &File{mode,size,uid,gid,mtime,xattrs,...})
        └── io::copy(entry → &mut fs)   ── only for S_IFREG with size>0
```

### Read Path (ext4 bytes → tar)

```
ext4 image (Read + Seek)
  │
  Reader::new()
  │  ├── seek to 1024, read SuperBlock (magic, block size, inode counts)
  │  └── read GroupDescriptors from block 1
  │
  Reader::walk()
  │  └── walk_recursive(root_inode=2, prefix="")
  │        ├── read_inode(dir_ino) ──► GroupDescriptor → inode table offset → 256-byte inode
  │        ├── read_dir(inode)     ──► read_data → parse DirEntry records (skip ".", "..")
  │        └── for each child entry:
  │              ├── read_inode(child_ino)
  │              ├── if S_IFLNK: read_symlink (inline ≤59 bytes, else extent data)
  │              ├── if S_IFCHR|S_IFBLK: decode_device from inode.block[4..8]
  │              ├── read_xattrs (inline extra space + optional xattr block)
  │              └── if S_IFDIR: recurse
  │
  Reader::to_tar()
  │  ├── walk() → Vec<WalkEntry>
  │  ├── seen_inodes: BTreeMap<InodeNumber, first_path>
  │  └── for each WalkEntry:
  │        ├── S_IFREG: emit Regular tar entry + PAX xattrs + read_data_to(w)
  │        ├── S_IFDIR: emit Directory entry + PAX xattrs
  │        ├── S_IFLNK: emit Symlink entry
  │        ├── S_IFCHR/BLK: emit Char/Block entry with major/minor
  │        ├── S_IFIFO: emit Fifo entry
  │        └── hard link (links_count > 1, seen inode): emit Link entry (no data)
  │
  └── tar stream written to W
```

**Hard link detection**: During `to_tar()`, a `BTreeMap<InodeNumber, String>` tracks the first path seen for each inode. When the same inode appears again with `links_count > 1`, a tar `Link` entry is emitted referencing the first path. This correctly reconstructs hard links without duplicating data.

### Diff Path (base ext4 + target ext4 → delta tar)

```
ext4 image A (base)      ext4 image B (target)
      │                        │
      ▼                        ▼
 Reader::walk()           Reader::walk()
      │                        │
      ▼                        ▼
 BTreeMap<path, entry>    iterate entries
      │                        │
      └────────┬───────────────┘
               ▼
       diff by path + metadata
               │
       ┌───────┼──────────┐
       ▼       ▼          ▼
     added   modified   deleted
       │       │          │
       ▼       ▼          ▼
    tar entry  tar entry  .wh. entry
               │
               ▼
       delta tar stream
```

`diff_to_tar(base, target, writer)` walks both snapshots and produces a minimal OCI-compatible delta layer:

- **Added**: in target but not base → full tar entry with data
- **Modified**: metadata differs (mode, uid, gid, size, mtime, symlink_target, devmajor, devminor, xattrs) → full tar entry with data
- **Deleted**: in base but not target → `.wh.<name>` whiteout marker
- **Deleted directory**: single `.wh.<dirname>` entry; child whiteouts are suppressed since the whiteout removes the entire subtree

Comparison ignores atime, ctime (volatile), inode_number (internal), and links_count (hard link topology tracked by the tar writer).

**Streaming reads**: `read_data_to(inode, &mut W)` streams file data block-by-block (4 KiB at a time) without buffering the whole file, suitable for large files.

## Concepts & Terminology

| Term | Definition | NOT |
|------|-----------|-----|
| Block | 4096-byte unit of disk storage | Not a "sector" — we use 4K throughout |
| Block group | Region of 32,768 blocks with its own inode table, bitmaps, and group descriptor | Not the same as a flex group |
| Inode | On-disk metadata record (256 bytes) tracking one file/directory/symlink | Not a directory entry — that's separate |
| Extent | Contiguous run of blocks described by `(logical_block, physical_block, length)` | Not a block pointer — ext4 uses extents, not indirect blocks |
| Inline data | File content stored directly inside the inode's 60-byte data area + 104-byte extra space | Not a memory buffer — it's on-disk in the inode itself |
| Whiteout | OCI overlay mechanism to mark a file as deleted; stored as char device 0,0 | Not a deletion — the inode still exists in the layer |
| Opaque whiteout | OCI overlay directive to hide the entire parent directory from lower layers | Not a file deletion — marks the directory itself |
| xattr | Extended attribute: named key-value metadata on an inode | Not part of the file content |
| Determinism | Same inputs → byte-identical output image, every run | Not just "reproducible build" — includes inode layout, directory entry order, timestamps |

## On-Disk Layout

```
Byte 0                          Block 0 (4096 bytes)
├─ [0..1024)    zeros           (boot sector area)
├─ [1024..2048) SuperBlock      (1024 bytes)
└─ [2048..4096) zeros

Block 1                         Group Descriptor Table
├─ 128 × GroupDescriptor        (32 bytes each = 4096 bytes)
└─ (repeated if >128 groups)

Block gd_end .. gd_end+N        Inode Table (per group)
├─ 16 inodes per block          (256 bytes each)
└─ N blocks = ceil(inodes_per_group / 16)

Block inode_end .. data_start   Block Bitmap + Inode Bitmap
├─ block_bitmap: 1 block        (1 bit per block in group)
└─ inode_bitmap: 1 block        (1 bit per inode in group)

Block data_start .. end         Data Blocks
├─ Directory blocks             (packed dir entries)
├─ File data blocks             (streamed content)
├─ xattr blocks                 (for large xattr sets)
└─ Extent index blocks          (for very large files)
```

## Inode Number Allocation

```
Inode 0:  reserved (invalid)
Inode 1:  bad block list (unused, but slot reserved)
Inode 2:  root directory "/"
Inode 3-10: reserved (ext4 convention; skipped)
Inode 11: lost+found (reserved but minimally created)
Inode 12+: user files, directories, symlinks, devices
```

Inode numbers are assigned sequentially. The group containing an inode is `(inode_num - 1) / inodes_per_group`. The slot within the group is `(inode_num - 1) % inodes_per_group`.

## Core Mechanism: Extent Tree

Ext4 files use an extent tree rather than block pointers. An extent describes a contiguous disk run:

```
ExtentHeader { magic=0xf30a, entries, max, depth, generation }
ExtentLeaf   { logical_block, length, start_high=0, start_low }  // depth=0
ExtentIndex  { logical_block, leaf_low, leaf_high=0, unused }    // depth>0
```

The 60-byte `inode.data` area holds:
- **1 header + up to 4 leaf extents** (depth 0) — fits most files ≤128 MiB
- **1 header + up to 4 index entries** (depth 1) pointing to extent blocks — for very large files

Each extent covers at most `MAX_BLOCKS_PER_EXTENT = 0x8000` (32,768) blocks = 128 MiB. Adjacent same-physical-run blocks are merged into one extent.

### Extent Building (writer.rs:write_extent)

```
on each data write:
  extend current_extent if blocks are contiguous
  else:
    flush current_extent to inode.data (if fits in 4 entries)
    or to pending extent_index_block (depth 2)
    start new extent

on finish_inode:
  flush last extent
  if depth==2: write extent_index_block to disk
  seek to inode slot, write inode
```

## xattr Storage Strategy

Extended attributes use a two-tier storage model:

```
Inode extra space (104 bytes)
├─ XATTR_INODE_OVERHEAD (8 bytes): magic + empty terminator
├─ inline xattr entries (≤96 bytes total)
│    entry = 16-byte header + padded-name + padded-value
└─ "system.data" marker (16 bytes) if INLINE_DATA enabled

Separate xattr block (4096 bytes, one per inode)
├─ XATTR_BLOCK_OVERHEAD (36 bytes): header + empty terminator
└─ block xattr entries (≤4060 bytes)
```

**Packing strategy** (`writer.rs:build_xattr_state`):
1. Sort xattrs, try to fit each into inode space first
2. Overflow to xattr block
3. If neither fits, return `io::Error`

**Name compression**: Common prefixes are replaced with a 1-byte index:

| Index | Prefix |
|-------|--------|
| 1 | `user.` |
| 2 | `system.posix_acl_access` |
| 3 | `system.posix_acl_default` |
| 4 | `trusted.` |
| 6 | `security.` |
| 7 | `system.` |
| 8 | `system.richacl` |

## Inline Data

When `WriterOption::InlineData` is set and a file's content fits in `INLINE_DATA_SIZE` (136 bytes), the content is stored directly in the inode:

```
inode.data[0..60]      ── first 60 bytes of file content
inode.xattr_inline     ── { magic, entry("system.data", remaining_content), terminator }
```

The `INLINE_DATA` flag is set in `inode.flags`. The kernel reads inline data from `inode.data` + the `system.data` xattr value. Inline data is mutually exclusive with the extent tree.

## Directory Serialization

Directories are finalized at `close()` time after all inodes are known. Each directory block contains packed entries:

```
[ DirEntry(".",  inode=self,   rec_len=12) ]
[ DirEntry("..", inode=parent, rec_len=12) ]
[ DirEntry("child1", inode=N, rec_len=...) ]  // sorted by (inode_num, name)
[ DirEntry("childN", inode=M, rec_len=<absorbs remaining block space>) ]
[ ... next 4K block if needed ... ]
```

Entry format: `{ inode: u32, rec_len: u16, name_len: u8, file_type: u8, name: [u8] }` padded to 4-byte alignment. The last entry in each block has `rec_len` extended to fill the block.

## Design Decisions

### Why write-only and forward-sequential?

We never need to read back what we wrote — the goal is to produce an image to be consumed by a container runtime or content-addressed by BLAKE3. Forward-only writes allow:
1. Streaming output (pipe to S3, socket, etc.)
2. No deserialization code — saves ~30% of the implementation
3. Single-pass over input data

The only seeks are to overwrite xattr blocks (when replacing an inode's xattrs) and to write the superblock/group descriptors at `close()`. The inode table is written in a single sequential pass at `close()` time — not per-inode via seeks.

### Why defer directory writes to close()?

Directory entries reference inode numbers. Hard links and `link()` calls can assign the same inode to multiple paths. We don't know the final link count or whether a path will be hard-linked until all `create()` and `link()` calls are done. Writing directories at `close()` guarantees correct link counts and avoids rewriting.

### Why deterministic output?

GlideFS content-addresses blocks with BLAKE3. If two nodes generate the same OCI layer, they must produce byte-identical ext4 images or they'll compute different hashes and store duplicate data. Determinism requires:
- No uninitialized bytes (zero all padding)
- No random UUIDs (UUID is all-zeros)
- No timestamps (`mtime=0`, `wtime=0` in superblock)
- Sorted directory entries (by inode number, then name)
- Sorted xattr entries
- `BTreeMap` for all child/xattr collections

### Why port from hcsshim instead of using an existing crate?

hcsshim's `compactext4` is the reference implementation for OCI-compatible ext4 generation, used in production by Microsoft for Windows container layers. No existing Rust crate offers the same combination of:
- Write-only streaming interface
- Deterministic output
- OCI whiteout support
- Inline data optimization

The port preserves the same on-disk layout, making images identical to those produced by the Go implementation.

### Why no journal?

Container layer images are read-only once mounted by the overlay filesystem. A journal adds ~128 MiB of overhead for no benefit. The `HAS_JOURNAL` compat feature is intentionally absent.

### Why no checksums?

`METADATA_CSUM` and `GDT_CSUM` are not enabled. Checksums require the UUID as a seed, but a zero UUID makes all checksums trivially zero — enabling the feature would silently produce invalid checksums. Since images are content-addressed externally, internal ext4 checksums are redundant.

## Package Structure

| File | Purpose |
|------|---------|
| `mod.rs` | Re-exports public API: `Writer`, `Reader`, `File`, `WriterOption`, `convert_tar_to_ext4` |
| `format.rs` | On-disk binary structures: `SuperBlock`, `GroupDescriptor`, `ParsedInode`, `ExtentHeader/Leaf/Index`, `DirEntry`, xattr helpers. Both serialization (`write_to`) and deserialization (`read_from`, `get_xattrs`) for shared on-disk types. |
| `writer.rs` | Core filesystem builder. Manages inode lifecycle, block allocation, extent tree construction, xattr packing, directory serialization, superblock finalization. |
| `reader.rs` | ext4 image parser. Reads superblock, group descriptors, inode table, extent trees, directory entries, and xattrs. Exports via `walk()` and `to_tar()`. |
| `tar_convert.rs` | tar→ext4 bridge. Maps tar entry types to writer operations, handles OCI whiteouts and PAX xattrs. |
| `diff.rs` | Incremental export: diffs two ext4 snapshots and produces an OCI-compatible delta tar layer with whiteout markers for deletions. |
| `tests.rs` | Integration tests: basic files, hard links, symlinks, devices, large files (>600 MiB), large dirs (50K entries), inline data, xattrs, determinism, reader roundtrips, tar roundtrips. |

## Configuration

| Option | Default | Effect |
|--------|---------|--------|
| `WriterOption::InlineData` | disabled | Store files ≤136 bytes inside the inode instead of allocating data blocks. Reduces image size for layers with many small files (e.g., config files, scripts). |
| `WriterOption::MaximumDiskSize(n)` | 16 GiB | Maximum filesystem size. Controls the number of block groups pre-allocated in the group descriptor table. Range: 0..16 TiB. |

## Limits

| Resource | Limit | Reason |
|----------|-------|--------|
| File size | 128 GiB | Two-level extent tree with 4 index entries × 340 leaves × 32K blocks |
| Inline data | 136 bytes | 60 (inode.data) + 104 (extra) − 28 (`system.data` xattr overhead: 8 magic/terminator + 16 entry header + 4 compressed name) |
| Short symlinks | 59 bytes | Stored inline in `inode.data` without extent tree |
| Hard links per inode | 65,000 | ext4 `MAX_LINKS` constant |
| xattr name+value | ~4 KiB | One xattr block per inode; no multi-block xattr support |
| Filesystem size | 16 TiB | 128 block groups × 32,768 blocks × 4096 bytes |

## Testing

Three test tiers in `tests.rs`:

**Writer unit tests** — create an ext4 image in memory, inspect metadata:

| Test | What it covers |
|------|---------------|
| `test_basic` | Files, symlinks, devices, hard links, directories |
| `test_large_directory` | 50K entries in one directory (multi-block dir) |
| `test_inline_data` | Files at the inline boundary (136 vs 137 bytes) |
| `test_xattrs` | Small and large xattr sets, inode vs block storage |
| `test_replace` | Overwrite files and directories, xattr updates |
| `test_large_file` | 1–600 MiB files (tests 2-level extent trees) |
| `test_large_disk` | 16 TiB filesystem (verifies group descriptor math) |
| `test_determinism` | SHA-256 hash of output must be identical across runs |
| `test_tar_determinism` | tar→ext4 output hash must be identical across runs |

**Reader roundtrip tests** — write with writer, read back with reader, assert bit-for-bit correctness:

| Test | What it covers |
|------|---------------|
| `test_reader_basic_roundtrip` | Files, dirs, symlinks, devices — metadata and content |
| `test_reader_xattr_roundtrip` | xattr key/value preservation |
| `test_reader_inline_data_roundtrip` | Inline data reading (INLINE_DATA flag) |
| `test_reader_large_directory_roundtrip` | 1K-entry directory |
| `test_reader_large_file_roundtrip` | 200 KiB file spanning multiple extents |
| `test_reader_hard_link_roundtrip` | Hard link detection via shared inode number |
| `test_reader_device_nodes_roundtrip` | Char/block devices, major/minor decoding |
| `test_reader_tar_roundtrip` | Full tar→ext4→tar roundtrip |
| `test_reader_to_tar_with_xattrs` | xattr preservation through tar→ext4→tar |

**Diff tests** — diff two in-memory ext4 images, verify delta tar contents:

| Test | What it covers |
|------|---------------|
| `test_diff_no_changes` | Identical images → empty tar |
| `test_diff_added_file` | New file appears in delta, unchanged file absent |
| `test_diff_deleted_file` | `.wh.` whiteout entry in delta |
| `test_diff_modified_file` | Changed content appears with new data |
| `test_diff_deleted_directory` | Single `.wh.` for dir, child entries suppressed |
| `test_diff_metadata_change` | Mode change detected without content change |
| `test_whiteout_nested_path` | Correct `.wh.` path for deeply nested files |

**Docker integration tests** (`--features docker-tests`) — validate against real `e2fsck` and `debugfs`:

| Test | What it covers |
|------|---------------|
| `test_ext4_fsck_with_verification` | e2fsck + debugfs content checks |
| `test_ext4_fsck_large_directory` | 10K-file directory via e2fsck |
| `test_ext4_fsck_tar_roundtrip` | tar→ext4 validated by e2fsck |
| `test_ext4_fsck_inline_data` | Inline data validated by e2fsck |

Run without Docker: `cargo test --features test-utils --lib` and `cargo test --features test-utils --test integration`

## Failure Modes

| Failure | Behavior |
|---------|----------|
| Write before `create()` | Panic: `no cur_inode` |
| Write more bytes than declared `size` | Error: `unexpected data after declared size` |
| Write fewer bytes than declared `size` | Error detected at `finish_inode` |
| xattr too large for inode + block | `io::Error` returned from `create()` |
| Link count overflow (>65,000) | `io::Error` returned from `link()` |
| Overwrite dir with file (or vice versa) | `io::Error` returned from `create()` |
| Overwrite inode that has extent data | `io::Error`: cannot replace inode with existing extent data |
| Reader: bad superblock magic | `io::Error(InvalidData)`: `"bad magic"` |
| Reader: unsupported block size | `io::Error(InvalidData)`: `"unsupported block size: N"` |
| Reader: bad extent header magic | `io::Error(InvalidData)`: `"bad extent header magic: 0xN"` |
| Reader: indirect-block inode (no EXTENTS flag) | `io::Error(Unsupported)`: `"only extent-based inodes are supported"` |
| Reader: inode number out of range | `io::Error(InvalidInput)` with group/count details |
