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
| `oci/boot_set.rs`             | **Static** boot-set derivation: ELF `DT_NEEDED` closure of the entrypoint, no execution |
| `oci/boot_capture_served.rs`  | **Runtime** boot-set capture: serve over ublk + read-tracer, run entrypoint via the sandbox, rank-merge |
| `oci/boot_capture.rs`         | fanotify file-capture fallback (no ublk); trusted-only, host mount ns |
| `oci/boot_meta.rs`            | `BootSetMeta` (fingerprint sidecar) + `RunSpec` (recorded entrypoint) JSON types |
| `oci/sandbox/`                | Pluggable isolation for the profiler run: `Sandbox` trait, `NamespaceSandbox`, `FirecrackerSandbox`, cgroup + seccomp; `vm_init/init.c` (the microVM init) |
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

### Two output formats: ext4 (writable) and EROFS (read-only)

The merge can target either filesystem; the choice is about **how the base is used**, not which is "better":

| | ext4 (default) | EROFS (`bless --oci --erofs`) |
|---|---|---|
| Mutability | read-**write** | read-**only** |
| Use case | VM bases that fork-and-write in place (CoW) | immutable container/OCI rootfs; writes go to an **overlay upper** |
| Mount | kernel-native, no FUSE | kernel-native, no FUSE (in-kernel `erofs` over ublk) |
| Determinism / dedup | yes (same layer → same pack hashes) | yes, **plus** large-file payloads grid-aligned for stronger cross-image block dedup (EROFS has no reserved blocks, so alignment is always safe) |
| Writer origin | ported from [hcsshim/compactext4](https://github.com/microsoft/hcsshim) | hand-rolled (`ext4/src/erofs.rs`) |

Both are kernel-native with no userspace daemon. Pick ext4 when the base must be writable; pick EROFS for an immutable rootfs that's overlaid at runtime (the format is read-only **by design** — container image layers are immutable, so this is the correct representation, and it's more compact: no journal, compact inodes, inline tails). `--layered` is a third option (per-layer content-addressed blobs that survive for overlay stacking).

### Why inline data

Container images contain thousands of small configuration files, JSON, and scripts. Files ≤200 bytes stored as inline data (inside the inode, no data block allocated) eliminate block allocation overhead and improve read locality. Disabled by default for compatibility with kernels that may not support `EXT4_FEATURE_INCOMPAT_INLINE_DATA`.

## Boot-set profiling & cold-start prefetch

A cold boot of an S3-backed image faults its working set one block at a time. The
automatic 32 MiB pack-window readahead helps, but real boot working sets are small
(0.5–6 % of the image) yet scattered across most packs — so readahead issues 4–15
GETs and over-fetches 2–12× (see `research-bootset/FINDINGS.md`). The **boot set**
is the precise list of blocks a boot actually reads; warming it on device open
turns those scattered faults into cache hits.

### Two producers

| Producer | Where | What it captures | When |
| --- | --- | --- | --- |
| **Static derivation** (`boot_set.rs`) | in `bless`, no execution | the entrypoint's ELF `PT_INTERP` + transitive `DT_NEEDED` closure + `ld.so.cache` + init | always (the floor) |
| **Runtime block-capture** (`boot_capture_served.rs`) | `glidefs profile` / `bless --profile` | the **exact** device blocks the kernel faults, *including fs metadata* (inode/dir blocks), by serving the blessed image over a throwaway ublk device with a read-tracer and running the entrypoint | opt-in (needs root + ublk) |

Static alone covers a compiled binary's boot (~92 % for nginx) but only ~50 % of an
interpreter (python `dlopen`s extensions static analysis can't see). The runtime
capture closes that gap and is the production source; the static closure is unioned
in as a backstop. Multiple runs are **rank-merged** (`--runs`, Borda by frequency
then earliest first-touch) to absorb boot nondeterminism.

### How the boot set is delivered (two mechanisms, both bounded)

- **EROFS** can reorder its layout: bless lays the static boot paths first via
  `WriterOption::PriorityOrder` → a contiguous prefix whose byte extent is stored as
  `VolumeManifest.prefetch_len` (manifest v6). On device open the router warms
  `[0, prefetch_len)` — one range GET.
- **ext4 / raw** can't reorder (packs are positional), so the precise captured block
  list is stored as a `bases/{name}.boot-set` sidecar and warmed PRECISELY at device
  open (coalesced runs, parallel, zero over-fetch). EROFS `--profile` also writes a
  `.boot-set` to cut the range warm's residual over-fetch.

The device-open warm lives in `router.rs:create_export` (tier-2 data warm, after the
tier-1 index warm); the readahead window always backstops any block the warm hasn't
reached. **Forks inherit automatically** — the warm keys off the base name, so
profiling a base once serves every fork/app image built on it.

### Decoupled, idempotent profiling: `glidefs profile`

Profiling RUNS the image, so it is kept **off the bless critical path**. Bless writes
the base fast (no run) and records a `bases/{name}.runspec` (the entrypoint argv/env
+ the static closure). A separate `glidefs profile --name <base> --s3-prefix <p>
--config <c>` then captures the boot set:

```
glidefs bless --oci python:3.12-slim --erofs --name py --s3-prefix prod -c cfg.toml
glidefs profile --name py --s3-prefix prod -c cfg.toml --cmd 'python3 -c "import ssl"'
```

- **Idempotent**: keyed on the base manifest's S3 ETag, written to a
  `bases/{name}.boot-set.meta` sidecar. Re-running on an unchanged base is a no-op;
  re-blessing (new content → new ETag) re-profiles. `--force` overrides.
- **Atomic publish**: `.boot-set` then `.boot-set.meta` (the commit marker) last.
- `--cmd` overrides the recorded entrypoint; `--runs 1..3` rank-merges; `--timeout`
  caps a long-running server (its startup reads ARE the boot set).

### The isolation sandbox

The profiler runs an image's entrypoint, so the run is wrapped in a pluggable
`Sandbox` (`oci/sandbox/`). The ublk read-tracer observes faults *below* the sandbox
boundary, so the backend is swappable without touching capture. Two concerns:

- **Accident protection** (every backend, always): a hard wall-clock timeout that
  kills the whole process tree, cgroup v2 cpu/memory/pids caps, and leak-proof RAII
  teardown of mounts + the ublk device — for a buggy *trusted* entrypoint that hangs
  or runs away.
- **Attack protection** (backend-dependent), selected via `[profile] sandbox` /
  `--sandbox`:
  - **`NamespaceSandbox`** (default, **trusted images**): mount + pid + net + ipc +
    uts namespaces, `pivot_root` onto the image, fresh `/proc` + minimal `/dev` + ro
    `/sys`, **all capabilities dropped** + `no_new_privs` + a seccomp allowlist,
    network off. Privileged setup as real root then drop everything (no user ns — a
    block-device fs isn't `FS_USERNS_MOUNT`). Fully contains untrusted *userspace*
    but shares the host kernel for the fs mount, so it is **trusted-only**;
    `--untrusted` + `ns` is refused.
  - **`FirecrackerSandbox`** (**untrusted images**): boots a microVM whose **guest
    kernel** mounts the untrusted fs (the ublk device is a `virtio-blk` drive), so a
    malicious filesystem image can't reach the host kernel. The host tracer still
    captures the faults (below the VM). A static-musl init (`vm_init/init.c`, baked
    into an initramfs by `build.rs`) mounts the image, runs the entrypoint, and
    reports its exit code over the console. ~0.85 s/run — no slower than namespaces.

Requirements: root + the `ublk` feature for capture; Firecracker additionally needs
`/dev/kvm`, a `firecracker` binary, and a guest kernel (`[profile] kernel_image`).
Profiling is a build/CI-node operation, never on the serving path.

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
