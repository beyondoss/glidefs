# OCI Bridge: Container Images ↔ GlideFS Volumes

Status: **idea / future work**

## What

A userspace ext4 implementation that reads and writes filesystem structure
directly against GlideFS blocks. This bridges OCI container images and
GlideFS VM volumes — same code path, both directions:

- **Ingest** (OCI → VM): pull an image, write ext4 into blocks, boot a VM
- **Export** (VM → OCI): read ext4 from blocks, produce layers, push to registry

No mounts. No kernel. No privileges. Just block reads and writes.

## Why

Today, containers and VMs are separate worlds. Different image formats,
different build tools, different registries, different distribution pipelines.
Users who know Docker have to learn an entirely new workflow to run VMs.

This eliminates that. OCI images become the universal format for both:

- **Users `docker push` to boot a VM.** Push a container image to Paraglide's
  registry, and it becomes a bootable VM volume. No image conversion step,
  no new tooling to learn.

- **Users `docker pull` to snapshot a VM.** Snapshot a running VM and pull it
  as a container image from any standard registry. Share environments with
  teammates, fork them, version them — all with tools they already have.

- **Layer dedup comes free.** 100 VMs based on `ubuntu:24.04` share the same
  base layers in S3. Snapshots push only what changed. The content-addressing
  GlideFS already does at the block level maps naturally to OCI's
  content-addressed layers.

### Why GlideFS is Uniquely Positioned

GlideFS is already 90% of the way there:

| What's needed | What we have |
|---|---|
| Content-addressed storage | Packs with BLAKE3 block hashes |
| Snapshots / versioning | VolumeManifest with snapshot sequences |
| Copy-on-write clones | Fork-on-write with shared packs |
| Lazy hydration | Read-through from S3 on cache miss |
| Deduplication | Block-level dedup across packs |

The only missing piece is a filesystem-aware layer that understands ext4's
on-disk format — translating between "OCI tar layer" and "ext4 blocks in
GlideFS."

## How

### Ingest: OCI Image → Bootable VM

```
docker push registry.paraglide.io/my-app:latest
                    │
                    ▼
         OCI Layer Resolution
         (registry API, dedup check)
                    │
                    ▼
        ┌───────────────────────┐
        │   Userspace ext4 Writer   │
        │                       │
        │  For each layer (base→top):│
        │    Apply tar entries as:  │
        │    - inode allocations    │
        │    - directory entries    │
        │    - file data blocks     │
        │    - symlinks, permissions│
        └───────────┬───────────┘
                    │
                    ▼
           GlideFS Block Writes
           (WriteCache → SSD → S3 packs)
                    │
                    ▼
            VolumeManifest saved
                    │
                    ▼
              NBD serve → VM boots
```

- ext4 writer operates against GlideFS's block interface — just
  `write(block_num, data)` calls, same as any other block client
- Layers applied in order (base → top), matching OCI overlay semantics
- Whiteout files (`.wh.*`) handled during apply to remove entries from
  lower layers
- Result is a fully laid-out ext4 filesystem in the block store

**Dedup on ingest:** After writing a layer's content, the resulting blocks
get BLAKE3 hashed and packed. Two images that produce identical blocks
naturally dedup through existing pack content-addressing. Fork-on-write
means multiple VMs from the same image share packs in S3 with independent
write caches.

### Export: VM Snapshot → OCI Image

```
  paraglide snapshot my-vm
                    │
                    ▼
        ┌───────────────────────┐
        │   Userspace ext4 Reader   │
        │                       │
        │  Walk inode table:        │
        │    - directory tree       │
        │    - file contents        │
        │    - metadata/permissions │
        └───────────┬───────────┘
                    │
                    ▼
          Produce OCI layer tar
          (or diff two snapshots)
                    │
                    ▼
         Push to registry with dedup
         (skip layers that already exist)
```

**Incremental export:** Diff two GlideFS snapshots at the filesystem level —
walk both, compare inodes, produce a minimal layer with only changes.
Push just the delta.

### The ext4 Implementation

The ext4 impl talks to GlideFS through the existing block interface:

```rust
trait BlockIO {
    fn read_block(&self, block_num: u64) -> Result<Bytes>;
    fn write_block(&mut self, block_num: u64, data: &[u8]) -> Result<()>;
    fn block_size(&self) -> usize;
}
```

**Block size mismatch:** ext4 uses 4 KiB blocks, GlideFS uses 128 KiB. The
`BlockIO` adapter maps ext4 blocks to offsets within GlideFS blocks (one
GlideFS block = 32 ext4 blocks). Reads/writes to the same GlideFS block
get coalesced.

```rust
// Ingest: OCI image → GlideFS volume
async fn ingest_oci_image(
    registry: &str,
    image_ref: &str,
    block_io: &mut impl BlockIO,
) -> Result<()> {
    let manifest = pull_manifest(registry, image_ref).await?;
    let mut fs = Ext4Writer::new(block_io, device_size)?;

    for layer in manifest.layers() {
        let tar_stream = pull_blob(registry, layer.digest()).await?;
        fs.apply_tar_layer(tar_stream)?;
    }

    fs.finalize()?;
    Ok(())
}

// Export: GlideFS volume → OCI image
async fn export_oci_image(
    block_io: &impl BlockIO,
    registry: &str,
    image_ref: &str,
) -> Result<()> {
    let fs = Ext4Reader::new(block_io)?;
    let layer_tar = fs.to_tar()?;
    let digest = push_blob(registry, &layer_tar).await?;
    push_manifest(registry, image_ref, &[digest]).await?;
    Ok(())
}
```

**What the ext4 impl needs:**
- Writer: superblock, block group descriptors, inode table, directory entries,
  file data blocks, symlinks, permissions, whiteout handling
- Reader: parse superblock, walk inodes and directories, read file data,
  reconstruct file tree with metadata

**What it does NOT need:**
- Journaling (we write once, not a live filesystem)
- Extent tree balancing (sequential layout)
- fsck / recovery (GlideFS handles block-level integrity)
- Mount / VFS / kernel anything

This is a well-defined subset of ext4. The on-disk format is stable and
thoroughly documented.

## Open Questions

- **Existing Rust ext4 crates**: `ext4-view` (read-only), `ext4-rs`. May work
  for export; ingest likely needs a custom writer or a minimal one from scratch.

- **mkfs bootstrap**: Could shell out to `mkfs.ext4` on a sparse file for the
  initial metadata layout, then write file data in userspace. Trades purity for
  speed to first working version.

- **Boot requirements**: VMs need a kernel, initramfs, bootloader — things OCI
  images don't include. Likely handled by a base template or boot layer that
  Paraglide provides.

- **Non-ext4**: Same pattern works for any filesystem with a documented on-disk
  format. ext4 is the obvious first target.
