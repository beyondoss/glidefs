# OCI Bridge Architecture

Bidirectional pipeline that converts between OCI-compatible tar streams and GlideFS block storage via a deterministic ext4 filesystem, enabling container image layers to be ingested into and exported from the GlideFS block device.

## Data Flow

### Ingest (tar → block storage)

```
tar stream (OCI layer)
      │
      ▼
 ingest_tar()          ← async entry point; accepts any AsyncRead-able tar source
      │
      │  spawn_blocking (ext4 work is sync I/O)
      ▼
 convert_tar_to_ext4()  ← tar_convert.rs; maps tar entries to ext4 inodes
      │  whiteout markers, PAX xattrs, hard links
      ▼
 ext4::Writer           ← deterministic ext4 builder (sequential, seek for metadata)
      │  write_data() calls BlockAdapter
      ▼
 BlockAdapter           ← bridges std::io traits ↔ async BlockHandler
      │  writes: sync call to BlockHandler::write()
      │  reads:  rt.block_on(BlockHandler::read())
      ▼
 BlockHandler::write()  ← lands on local SSD write-behind cache (immediate)
      │
      ▼ (async, background)
 flush_to_s3()          ← content-addressed packs uploaded to S3
```

### Export (block storage → tar)

```
BlockHandler (loaded GlideFS export)
      │
      ▼
 export_tar()           ← async entry point
      │
      │  spawn_blocking
      ▼
 BlockAdapter           ← wraps handler as Read + Seek
      │
      ▼
 ext4::Reader::new()    ← parses superblock + group descriptors
      │
      ▼
 Reader::to_tar()       ← walks inode tree, streams tar entries
      │  hard-link dedup, PAX xattrs, extent-based data streaming
      ▼
 tar stream (OCI layer)
```

### Push (block storage → OCI registry)

```
BlockHandler (loaded GlideFS export)
      │
      ▼
 push_image()              ← async entry point
      │
      │  spawn_blocking (Phase 1: export + compress + hash)
      ▼
 BlockAdapter              ← wraps handler as Read + Seek
      │
      ▼
 ext4::Reader::to_tar()    ← walks inode tree, streams tar entries
      │
      ▼
 DigestWriter(uncompressed)   ← computes diff_id (sha256 of uncompressed tar)
      │
      ▼
 GzEncoder                 ← gzip compression
      │
      ▼
 DigestWriter(compressed)  ← computes layer digest (sha256 of compressed blob)
      │
      ▼
 BufWriter<File>           ← spools to temp file (bounded memory)
      │
      │  Phase 2: stream from temp file
      ▼
 ReaderStream → push_blob_stream()  ← chunked upload to registry (POST → PATCH → PUT)
      │
      │  Phase 3-4: config + manifest
      ▼
 OCI image in registry
```

### Incremental Push (two snapshots → two-layer OCI image)

```
BlockHandler A (base)      BlockHandler B (target)
      │                          │
      ▼                          ▼
 export_compressed_layer    export_delta_layer
      │                          │
      │                    ┌─────┴─────┐
      │                    ▼           ▼
      │              ext4::Reader  ext4::Reader
      │                    │           │
      │                    └─────┬─────┘
      │                          ▼
      │                   ext4::diff_to_tar()
      │                          │
      ▼                          ▼
 DigestWriter chain        DigestWriter chain
      │                          │
      ▼                          ▼
 temp file (base layer)    temp file (delta layer)
      │                          │
      ▼                          ▼
 push_blob_stream          push_blob_stream
 (idempotent)              (delta changes only)
      │                          │
      └──────────┬───────────────┘
                 ▼
          OCI manifest with two layers
          layer 0 = full base
          layer 1 = delta (adds/mods/whiteouts)
```

`push_delta_image()` exports base and delta layers in parallel via `spawn_blocking`, then stream-uploads both. The base layer is idempotent — `push_blob_stream` skips the upload if the blob already exists. The delta layer contains only changes: added/modified files with full data, `.wh.<name>` whiteout entries for deletions.

The compressed layer is spooled to a temp file so the digest can be computed before upload starts (OCI spec requires the digest in the PUT request). Memory usage is bounded to gzip buffers (~4MB), never proportional to volume size.

## Concepts & Terminology

| Term           | Definition                                                                  | NOT                                           |
| -------------- | --------------------------------------------------------------------------- | --------------------------------------------- |
| OCI layer      | A tar archive representing one filesystem diff in a container image         | Not a full container image                    |
| Whiteout       | An OCI deletion marker: `.wh.<name>` deletes a file; `.wh..wh..opq` opaques a directory | Not a real file                    |
| Ingest         | Write path: tar → ext4 → GlideFS blocks                                     | Not a streaming pass-through; produces ext4   |
| Export         | Read path: GlideFS blocks → ext4 parsing → tar                              | Not a raw block dump                          |
| BlockAdapter   | A `Read + Write + Seek` wrapper around `BlockHandler` for use in sync code  | Not a real disk device                        |
| BlockHandler   | GlideFS async block I/O handle; writes go to SSD, reads may hit S3         | Not a file handle                             |
| Deterministic  | Same tar input always produces identical ext4 bytes                         | Not reproducible in the general sense; byte-exact |
| Inline data    | Small files (≤200 B) stored entirely within the inode, no data blocks      | Not the same as sparse files                  |
| Extent tree    | B-tree structure within each inode mapping logical blocks → physical blocks | Not a global filesystem structure             |
| PAX xattrs     | Extended attributes in POSIX tar PAX format (`SCHILY.xattr.*` prefix)      | Not standard tar header fields                |

## Core Mechanism

### BlockAdapter: Bridging Sync and Async

The ext4 Writer/Reader use standard `std::io` traits (`Read`, `Write`, `Seek`). GlideFS block I/O is async. `BlockAdapter` resolves this mismatch:

```rust
// Write: sync, no bridging needed — BlockHandler::write() is synchronous
fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.handler.write(self.pos, buf)?;
    self.pos += buf.len() as u64;
    Ok(buf.len())
}

// Read: async → sync via block_on
fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let n = self.rt.block_on(self.handler.read(self.pos, buf))?;
    self.pos += n as u64;
    Ok(n)
}
```

**Critical constraint**: `BlockAdapter` must only be used inside `spawn_blocking`. Calling `block_on` from an async context panics.

### Ingest Pipeline

`ingest_tar()` (`ingest.rs:24`) spawns a blocking task that:

1. Creates a `BlockAdapter` wrapping the export's `BlockHandler`
2. Calls `convert_tar_to_ext4(tar_reader, adapter, options)` — streams tar → ext4
3. After the blocking task completes, flushes the BlockHandler to local SSD

S3 upload happens asynchronously via the flush scheduler — ingest returns before S3 is done.

### Tar → ext4 Conversion (`tar_convert.rs`)

`convert_tar_to_ext4` iterates tar entries in order and builds an ext4 filesystem:

| Tar Entry Type       | ext4 Operation                             |
| -------------------- | ------------------------------------------ |
| Regular / GNUSparse  | `create()` + `write_data()`                |
| Symlink              | `create()` with `linkname`                 |
| Directory            | `create()` with `S_IFDIR`                  |
| Char / Block / FIFO  | `create()` with device mode bits           |
| Hard link            | `link(oldname, newname)`                   |
| `.wh.<name>`         | `create()` char device 0,0 (OCI whiteout)  |
| `.wh..wh..opq`       | `stat()` + `create()` with opaque xattr    |

For each entry, `make_parents()` ensures parent directories exist (idempotent).

PAX xattrs with prefix `SCHILY.xattr.` are extracted and passed through to the ext4 inode.

### Export Pipeline

`export_tar()` (`export.rs:16`) spawns a blocking task that:

1. Wraps the `BlockHandler` in a `BlockAdapter`
2. Constructs `ext4::Reader::new(&mut adapter)` — parses superblock and group descriptors
3. Calls `Reader::to_tar(tar_writer)` — walks the inode tree and streams tar entries

Hard links are detected by tracking seen `(inode_number, links_count > 1)` pairs. On the second encounter, a tar `Link` entry is emitted instead of a full file.

### Determinism

The ext4 Writer produces identical bytes for identical input. This property is essential because GlideFS uses content-addressed packs — the same layer must hash identically regardless of when or where it is ingested.

Determinism is maintained by:

- `BTreeMap` for directory children (sorted iteration)
- Sorted directory entries: `.` and `..` first, then children by `(inode_number, name)`
- Fixed inode allocation order: directories → files → links
- Fixed layout: superblock at block 0, group descriptors at block 1, inode table immediately after
- Extent trees whose shape is determined solely by file size
- `Uuid` writer option sets UUID, `hash_seed`, and `journal_uuid` from a single value

## Package Structure

| File                          | Purpose                                                              |
| ----------------------------- | -------------------------------------------------------------------- |
| `oci/mod.rs`                  | Public re-exports: `BlockAdapter`, `ingest_tar`, `export_tar`, `push_image`, `push_delta_image` |
| `oci/block_adapter.rs`        | `Read + Write + Seek` bridge between sync ext4 code and async `BlockHandler` |
| `oci/ingest.rs`               | `ingest_tar()`: tar → ext4 → GlideFS blocks pipeline                |
| `oci/export.rs`               | `export_tar()`: GlideFS blocks → ext4 → tar stream pipeline         |
| `ext4/mod.rs`                 | Public re-exports for ext4 subsystem                                 |
| `ext4/format.rs`              | On-disk types, constants, parsing helpers (SuperBlock, ParsedInode, extents, xattrs) |
| `ext4/writer.rs`              | Deterministic ext4 writer; ported from Microsoft/hcsshim compactext4 |
| `ext4/reader.rs`              | ext4 reader: `walk()`, `to_tar()`, extent resolution, xattr parsing  |
| `ext4/tar_convert.rs`         | Tar→ext4 conversion with OCI whiteout and PAX xattr handling         |
| `oci/push.rs`                 | `push_image()` and `push_delta_image()`: export + compress + stream-upload to OCI registry |
| `oci/pull.rs`                 | `pull_image()`: resolve + download OCI image layers                  |
| `ext4/tests.rs`               | Comprehensive roundtrip tests (60+)                                  |

## Design Decisions

### Why deterministic ext4 over a dynamic filesystem?

GlideFS stores data as content-addressed packs keyed by BLAKE3 hash. If the same OCI layer produced different bytes on each ingest, deduplication would be impossible.

By constraining the ext4 writer to a fixed layout algorithm (sorted inodes, deterministic extent trees, fixed metadata positions), the same tar input always produces the same pack content — enabling cross-node and cross-time deduplication.

### Why ext4 over squashfs, erofs, or raw tar?

1. **Kernel-native, no FUSE**: ext4 is mounted directly. No userspace filesystem daemon needed at runtime.
2. **Widely supported**: Works with any standard Linux kernel without extra modules.
3. **Writable via NBD**: GlideFS exports an NBD block device. ext4 can be mounted read-write over NBD, enabling in-place modification without re-ingesting.
4. **Microsoft precedent**: The writer is ported from [hcsshim/pkg/compactext4](https://github.com/microsoft/hcsshim), a production system used by Windows containers.

### Why spawn_blocking for ext4 work?

The ext4 Writer and Reader use synchronous `std::io` traits with seek — not designed for async. Wrapping them in `spawn_blocking` keeps the async executor unblocked while ext4 I/O runs on a thread pool thread. `BlockAdapter` uses `rt.block_on()` for reads (which are async at the `BlockHandler` level) while writes are already synchronous.

### Why inline data?

Files ≤200 bytes (the `INLINE_DATA_SIZE` limit) are stored entirely within the inode — no data blocks allocated. Container images contain thousands of small configuration files, JSON, and scripts. Inline data eliminates block allocation overhead and improves read locality for these common cases.

Enabled via `WriterOption::InlineData`. Disabled by default for compatibility with older kernels that may not support `EXT4_FEATURE_INCOMPAT_INLINE_DATA`.

### Why OCI whiteouts as char devices?

The OCI image spec represents file deletions as `.wh.<name>` tar entries. The overlayfs kernel driver uses char device `0,0` as its whiteout representation on disk. By converting OCI whiteouts to char device `0,0` during ingest, the resulting ext4 filesystem is directly usable as an overlayfs lower layer without any translation layer at mount time.

Directory opaque whiteouts (`.wh..wh..opq`) set `trusted.overlay.opaque=y` xattr on the directory, matching overlayfs's own convention.

## OCI Images and VM Booting

OCI images produced by `push_image` / `push_delta_image` contain the rootfs only — the ext4 filesystem exported as gzip-compressed tar layers. They do **not** include:

- **Kernel** (`vmlinuz`) — the hypervisor or host provides this
- **Initramfs** (`initrd.img`) — same; provided by the VM runtime
- **Bootloader** (GRUB, systemd-boot) — not needed; the VM runtime boots the kernel directly
- **Partition table** (GPT/MBR) — the ext4 image is a raw filesystem, not a disk image

This matches how container runtimes (Docker, containerd) use OCI images: the image is a rootfs, the runtime provides the kernel. For VM-based workloads (like GlideFS), the hypervisor boots a host-provided kernel that mounts the ext4 rootfs from the NBD block device.

To boot a VM from a GlideFS volume:

1. **Pull** the OCI image → `ingest_tar` → blocks stored in GlideFS
2. **Export** the NBD block device
3. **Boot** the VM with a host kernel pointing at the NBD device as root (`root=/dev/nbd0`)
4. The kernel mounts the ext4 filesystem directly — no bootloader involved

## Key Invariants

1. **spawn_blocking required**: `BlockAdapter` must never be used from an async context. `block_on` inside an async context panics.
2. **Ingest is not atomic with S3**: `ingest_tar()` returns after local SSD flush. S3 upload is background-async. Callers must not assume S3 availability after ingest returns.
3. **Export reads committed state**: `export_tar()` reads whatever blocks exist in the BlockHandler at call time. Concurrent writes during export produce undefined results.
4. **Determinism requires ordered input**: The writer processes entries in the order they arrive from the tar stream. Tar archives with different entry orderings will produce different ext4 images. OCI layers are typically produced by image builders in a consistent order.

## Failure Modes

| Failure                          | Behavior                                              | Recovery                                      |
| -------------------------------- | ----------------------------------------------------- | --------------------------------------------- |
| S3 upload fails during ingest    | `ingest_tar()` succeeds (local flush succeeded); background flush retries | Flush scheduler retries with backoff       |
| BlockHandler read returns error  | `BlockAdapter::read()` propagates `io::Error`; `Reader` or `Writer` aborts | Re-export from S3 manifest                 |
| Tar stream truncated mid-ingest  | `convert_tar_to_ext4` returns `io::Error`; partial ext4 written | Caller discards the export and retries ingest |
| Device size exceeded during write | `BlockAdapter::write()` returns `WriteZero`; Writer sees short write | Caller must provision larger block device  |
| spawn_blocking task panics       | `JoinError` converted to `io::Error` via `map_err`; caller sees `io::ErrorKind::Other` | Inspect logs; usually a bug in ext4 code  |

## Configuration

`IngestOptions` controls ingest behavior:

| Field             | Default | Purpose                                                      |
| ----------------- | ------- | ------------------------------------------------------------ |
| `writer_options`  | `[]`    | Pass-through to ext4 Writer (see below)                      |

`WriterOption` values:

| Variant                    | Default            | Purpose                                                              |
| -------------------------- | ------------------ | -------------------------------------------------------------------- |
| `InlineData`               | disabled           | Store small files (≤200 B) inside inode; saves data block allocation |
| `MaximumDiskSize(i64)`     | 16 GiB             | Cap filesystem size; affects superblock and group descriptor count   |
| `Uuid([u8; 16])`           | random at format   | Set filesystem UUID (also `hash_seed` and `journal_uuid`)            |
| `Journal(u32)`             | disabled           | Enable JBD2 v2 journal with the given block count                   |
