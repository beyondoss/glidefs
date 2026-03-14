# OCI Module Architecture

Takes OCI container images from a registry (or tar streams) and writes them into GlideFS block storage as deterministic ext4 filesystems — and does the reverse, reading ext4 blocks back out as OCI-compatible tar layers for distribution.

## Data Flow

### Ingest (tar → block storage)

```
tar stream (OCI layer)
      │
      ▼
 ingest_tar()          ← async entry point; accepts any Read-able tar source
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

### Pull (registry → block storage)

```
OCI Registry
      │  HTTP streaming (gzip-compressed tar)
      ▼
 StreamReader (async → AsyncRead)
      │
      │  spawn_blocking
      ▼
 SyncIoBridge (AsyncRead → std::io::Read)
      │
      ▼
 GzDecoder              ← decompresses in-flight
      │
      ▼
 convert_tar_to_ext4()  ← same as ingest path
      │
      ▼
 BlockAdapter → BlockHandler → SSD
```

Layers are processed sequentially. If layer N fails, layers 0..N-1 are already committed to local SSD. After pull, data is on SSD but **not yet on S3** — async flush scheduler handles that.

### Push (block storage → OCI registry)

```
BlockHandler
      │
      │  spawn_blocking (Phase 1: export + compress + hash)
      ▼
 BlockAdapter
      │
      ▼
 ext4::Reader::to_tar()
      │
      ▼
 DigestWriter(uncompressed)   ← computes diff_id (sha256 of raw tar)
      │
      ▼
 GzEncoder                   ← gzip compression
      │
      ▼
 DigestWriter(compressed)    ← computes layer digest (sha256 of compressed blob)
      │
      ▼
 BufWriter<NamedTempFile>    ← spools to disk (memory is bounded to gzip buffer ~4 MB)
      │
      │  Phase 2: upload
      ▼
 ReaderStream → push_blob()  ← chunked stream upload; skips if digest exists
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
 export_full_layer         export_delta_layer
      │  (parallel spawn_blocking tasks)   │
      ▼                          ▼
 DigestWriter chain        DigestWriter chain
      │                          │
      ▼                          ▼
 temp file (base layer)    temp file (delta layer)
      │                          │
      ▼                          ▼
 push_blob (idempotent)    push_blob (delta changes only)
      │                          │
      └──────────┬───────────────┘
                 ▼
          OCI manifest: layer 0 = full base, layer 1 = delta
```

`push_delta_image()` exports base and delta in parallel. Layer 0 upload is idempotent — `push_blob` skips it if the digest already exists in the registry.

### Error Paths

```
pull_image ──► PullError::Registry  (auth, not found, network)
           └─► PullError::Io        (decompression, block write)

push_image ──► PushError::Registry  (upload failure)
           └─► PushError::Io        (export, temp file)

ingest_tar ──► io::Error            (tar parse, block write)
export_tar ──► io::Error            (block read, ext4 parse)
```

## Concepts & Terminology

| Term           | What It Controls / Means                                                    | NOT                                           |
| -------------- | --------------------------------------------------------------------------- | --------------------------------------------- |
| OCI layer      | A tar archive representing one filesystem diff in a container image         | Not a full container image                    |
| Whiteout       | OCI deletion marker: `.wh.<name>` deletes a file; `.wh..wh..opq` opaques a directory | Not a real file on the target system |
| Ingest         | Write path: tar → ext4 → GlideFS blocks                                     | Not a streaming pass-through; produces ext4   |
| Export         | Read path: GlideFS blocks → ext4 parsing → tar                              | Not a raw block dump                          |
| `BlockAdapter` | A `Read + Write + Seek` wrapper around `BlockHandler` for use in sync code  | Not a buffer; passes bytes directly through   |
| `BlockHandler` | GlideFS async block I/O handle; writes go to SSD, reads may hit S3         | Not a file handle                             |
| `diff_id`      | SHA-256 of **uncompressed** tar (required by OCI spec in image config)      | Not the registry blob hash                    |
| `digest`       | SHA-256 of **compressed** tar blob (what the registry indexes by)           | Not the content hash                          |
| Deterministic  | Same tar input always produces identical ext4 bytes                         | Not just reproducible; byte-exact             |
| Inline data    | Small files (≤200 B) stored entirely within the inode, no data blocks       | Not the same as sparse files                  |
| Extent tree    | B-tree structure within each inode mapping logical blocks → physical blocks  | Not a global filesystem structure             |
| PAX xattrs     | Extended attributes in POSIX tar PAX format (`SCHILY.xattr.*` prefix)       | Not standard POSIX tar header fields          |

## Core Mechanisms

### BlockAdapter: Bridging Sync and Async

The ext4 Writer/Reader use standard `std::io` traits (`Read`, `Write`, `Seek`). GlideFS block I/O is async. `BlockAdapter` resolves this mismatch:

```rust
// Write: BlockHandler::write() is sync
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

### Single-Pass Digest Computation (push.rs)

OCI requires two hashes per layer: `diff_id` (sha256 of uncompressed tar) and `digest` (sha256 of compressed blob). Push computes both in one write pass with no intermediate buffering:

```
tar bytes → DigestWriter → GzEncoder → DigestWriter → BufWriter → File
             (diff_id)                  (digest)
```

`DigestWriter<W>` implements `Write` by updating a `Sha256` hasher then forwarding to the inner writer. `.finish()` returns `(inner_writer, hex_digest)`. The compressed layer must be spooled to a file so the final `Content-Length` and `digest` are known before the upload begins.

### Tar → ext4 Conversion (tar_convert.rs)

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

For each entry, `make_parents()` ensures parent directories exist (idempotent). PAX xattrs with prefix `SCHILY.xattr.` are extracted and passed through to the ext4 inode.

### Determinism

The ext4 Writer produces identical bytes for identical input. This is essential because GlideFS content-addresses its packs — the same layer must hash identically regardless of when or where it is ingested.

Determinism is maintained by:

- `BTreeMap` for directory children (sorted iteration)
- Sorted directory entries: `.` and `..` first, then children by `(inode_number, name)`
- Fixed inode allocation order: directories → files → links
- Fixed layout: superblock at block 0, group descriptors at block 1, inode table immediately after
- Extent tree shape determined solely by file size
- `Uuid` writer option sets UUID, `hash_seed`, and `journal_uuid` from a single value

## Package Structure

| File                          | What It Does                                                              |
| ----------------------------- | ------------------------------------------------------------------------- |
| `oci/mod.rs`                  | Public re-exports only                                                    |
| `oci/block_adapter.rs`        | `Read + Write + Seek` bridge between sync ext4 code and async `BlockHandler` |
| `oci/ingest.rs`               | `ingest_tar()`: tar → ext4 → GlideFS blocks pipeline                     |
| `oci/export.rs`               | `export_tar()`: GlideFS blocks → ext4 → tar stream pipeline              |
| `oci/pull.rs`                 | `pull_image()`: resolve manifest, download + decompress layers, ingest    |
| `oci/push.rs`                 | `push_image()` / `push_delta_image()`: export + compress + upload to registry |
| `ext4/format.rs`              | On-disk types, constants, parsing (SuperBlock, ParsedInode, extents, xattrs) |
| `ext4/writer.rs`              | Deterministic ext4 writer; ported from Microsoft/hcsshim compactext4     |
| `ext4/reader.rs`              | ext4 reader: `walk()`, `to_tar()`, extent resolution, xattr parsing       |
| `ext4/tar_convert.rs`         | Tar → ext4 conversion with OCI whiteout and PAX xattr handling            |

## Why It Behaves This Way

### Why spawn_blocking for all entry points

The ext4 Writer and Reader are built on synchronous `std::io` traits with `seek`. Running them in an async task would block the Tokio executor. `spawn_blocking` puts the work on a thread pool thread where `rt.block_on()` is safe to call.

### Why pull streams instead of buffering layers to disk first

Buffering a layer to disk would double the I/O (registry → disk, disk → ext4). Streaming directly through decompression into ext4 blocks halves the work and removes any temp-file size limit. The chain holds only a few kilobytes in flight at any point.

### Why push spools to a temp file

OCI blob upload requires a known `Content-Length` and the `digest` must be in the PUT URL. The compressed size is unknown until the entire layer is compressed. Spooling to a temp file lets the digest and size be computed before the upload begins.

### Why delta images are two layers

If the base was previously pushed, Layer 0 is a no-op upload — `push_blob` detects the existing digest and skips it. The delta model avoids re-uploading base content on every push while staying compatible with standard OCI clients that apply layers in order.

### Why OCI whiteouts become char devices

overlayfs (the kernel driver used by container runtimes) represents file deletions as char device `0,0` on disk. By converting OCI `.wh.<name>` entries to char device `0,0` during ingest, the resulting ext4 filesystem can serve directly as an overlayfs lower layer without any runtime translation.

Directory opaque whiteouts (`.wh..wh..opq`) set `trusted.overlay.opaque=y` xattr on the directory, matching overlayfs's convention exactly.

### Why deterministic ext4 over squashfs, erofs, or raw tar

1. **Kernel-native, no FUSE**: ext4 is mounted directly; no userspace daemon needed at runtime
2. **Writable via NBD**: ext4 can be mounted read-write over GlideFS's NBD block device, enabling in-place modification without re-ingesting
3. **Content deduplication**: determinism means the same layer always produces the same pack hashes; cross-node and cross-time deduplication works transparently
4. **Microsoft precedent**: the writer is ported from [hcsshim/pkg/compactext4](https://github.com/microsoft/hcsshim), a production system used by Windows containers

### Why inline data

Container images contain thousands of small configuration files, JSON, and scripts. Files ≤200 bytes stored as inline data (inside the inode, no data block allocated) eliminate block allocation overhead and improve read locality. Disabled by default for compatibility with kernels that may not support `EXT4_FEATURE_INCOMPAT_INLINE_DATA`.

## OCI Images and VM Booting

OCI images produced by `push_image` / `push_delta_image` contain the rootfs only. They do **not** include:

- **Kernel** (`vmlinuz`) — the hypervisor provides this
- **Initramfs** — provided by the VM runtime
- **Bootloader** — not needed; the VM runtime boots the kernel directly
- **Partition table** — the ext4 image is a raw filesystem, not a disk image

To boot a VM from a GlideFS volume:

1. **Pull** the OCI image → `ingest_tar` → blocks stored in GlideFS
2. **Export** the NBD block device
3. **Boot** the VM with a host kernel pointing at the NBD device as root (`root=/dev/nbd0`)
4. The kernel mounts the ext4 filesystem directly — no bootloader involved

## Key Invariants

1. **`spawn_blocking` required**: `BlockAdapter` must never be constructed or used from an async context. `block_on` from async panics.
2. **Ingest is not atomic with S3**: `ingest_tar()` returns after local SSD flush. S3 upload is background-async. Callers must not assume S3 availability after ingest returns.
3. **Export reads point-in-time state**: `export_tar()` reads whatever blocks exist at call time. Concurrent writes during export produce undefined results.
4. **Determinism requires consistent tar ordering**: The writer processes entries in arrival order. Different entry orderings produce different ext4 images.

## Failure Modes

| Failure                          | What Actually Happens                                         | Recovery                                      |
| -------------------------------- | ------------------------------------------------------------- | --------------------------------------------- |
| S3 upload fails during/after ingest | `ingest_tar()` returns success (SSD flush succeeded); background S3 flush retries | Flush scheduler retries with backoff   |
| BlockHandler read returns error  | `BlockAdapter::read()` propagates `io::Error`; Reader or Writer aborts | Re-export from S3 manifest                 |
| Tar stream truncated mid-ingest  | `convert_tar_to_ext4` returns `io::Error`; partial ext4 written | Caller discards the export and retries ingest |
| Device size exceeded during write | `BlockAdapter::write()` returns `WriteZero`; Writer sees short write | Provision a larger block device            |
| Layer N fails during pull        | `PullError` returned; layers 0..N-1 already committed to SSD | Retry pull; block writes are idempotent    |
| Blob upload fails mid-push       | `PushError::Registry`; earlier blobs may be orphaned in registry | Retry push; `push_blob` skips blobs by digest |
| Manifest push fails              | Blobs uploaded, no manifest; image inaccessible               | Retry push; blobs reused by digest            |
| `spawn_blocking` task panics     | `JoinError` → `io::Error` via `map_err`; caller sees `ErrorKind::Other` | Investigate panic; usually an ext4 bug  |

## Configuration

`IngestOptions` controls ingest behavior:

| Field             | Default | Effect                                                         |
| ----------------- | ------- | -------------------------------------------------------------- |
| `writer_options`  | `[]`    | Pass-through to ext4 Writer (see below)                        |

`WriterOption` values:

| Variant                    | Default       | Runtime Effect                                                        |
| -------------------------- | ------------- | --------------------------------------------------------------------- |
| `InlineData`               | disabled      | Files ≤200 B stored in inode; no data blocks allocated for them       |
| `MaximumDiskSize(i64)`     | 16 GiB        | Caps filesystem size; sets group descriptor and superblock counts     |
| `Uuid([u8; 16])`           | random        | Sets filesystem UUID, `hash_seed`, and `journal_uuid`                 |
| `Journal(u32)`             | disabled      | Enables JBD2 v2 journal with the given block count                    |
