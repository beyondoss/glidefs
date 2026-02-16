# Block Size Analysis: Empirical Dedup Measurement

## Methodology

Measured content-addressed deduplication effectiveness at 7 block sizes (4KB–256KB) using real ext4 filesystem images built from Ubuntu 22.04 + Node.js 20 workloads. All hashing uses BLAKE3-128 (same as GLIDEv2). Compression uses LZ4 (same as GLIDEv2).

Two test sets:

**Independent builds** (worst case): Each image built from scratch via Docker. Simulates VMs on different hosts without a blessed base image. 5 images: bare Ubuntu, 2x Express, Next.js, Express+app code.

**Forked images** (realistic GLIDEv2 scenario): One blessed base image (Ubuntu + Node 20), then byte-for-byte copied and mounted, with `npm install` run into each copy. Same ext4 allocator starting state. 3 images: base, 2x Express.

Tool: `cargo run --release --bin dedup_measure`
Script: `./scripts/gen_dedup_images.sh`

## Results: Forked Images (the GLIDEv2 scenario)

Base → fork → npm install express, two independent VMs.

### Overall Dedup + Compression

| Block Size | Dedup Ratio | LZ4 Ratio | Combined (Dedup+LZ4) | Block Map/10GB VM |
|-----------|------------|-----------|----------------------|-------------------|
| 4KB       | 3.60x      | 1.70x     | 5.57x                | 42.5MB            |
| 8KB       | 3.44x      | 1.75x     | 5.66x                | 21.2MB            |
| 16KB      | 3.35x      | 1.80x     | 5.76x                | 10.6MB            |
| 32KB      | 3.30x      | 1.85x     | 5.87x                | 5.3MB             |
| 64KB      | 3.21x      | 1.89x     | 5.88x                | 2.7MB             |
| 128KB     | 3.11x      | 1.91x     | 5.86x                | 1.3MB             |
| 256KB     | 3.00x      | 1.92x     | 5.79x                | 680KB             |

These numbers look stable across block sizes. But they're misleading — they're dominated by untouched base blocks.

### The real metric: post-fork write dedup

The overall Jaccard similarity (base <-> vm) is 97%+ at all block sizes. But that measures how much of the *base* survives in the fork. The question that matters is: **when two VMs both `npm install express` after forking, do the new blocks match?**

Derived from the data by subtracting base block overlap:

| Block Size | New blocks per VM | Shared between vm-0 & vm-1 | **Post-fork dedup** |
|-----------|------------------|---------------------------|---------------------|
| 4KB       | ~1,005           | ~825                      | **82%**             |
| 8KB       | ~609             | ~90                       | **15%**             |
| 16KB      | ~328             | ~23                       | **7%**              |
| 32KB      | ~170             | ~8                        | **5%**              |
| 64KB      | ~92              | ~5                        | **5%**              |
| 128KB     | ~50              | ~3                        | **6%**              |
| 256KB     | ~30              | ~2                        | **7%**              |

**At 4KB, 82% of npm install blocks match between two VMs. At 8KB, 15%. At 16KB+, under 7%.** The cliff is between 4KB and 8KB — one doubling of block size destroys post-fork dedup.

The mechanism: ext4 allocates in 4KB pages. At 4KB block size, each GlideFS block is exactly one ext4 page. Metadata pages (inode tables, journal entries, group descriptors) are isolated into their own blocks and don't poison file content hashes. At 8KB, you pair two ext4 pages per block — a metadata page and a content page can share a block, and different timestamps poison the hash even when the file data is identical. The dedup cliff is a fundamental property of the ext4 page size, not something tuning can fix.

### What the headline numbers hide

The 97% overall Jaccard at 128KB means: "of all blocks in both images, 97% match." But the base image contributes ~2,760 blocks that were never written to. The npm install writes ~50 new blocks. The 97% is (2,760 shared base) / (2,760 + 50 + 49 - 3 shared new). The base blocks were never going to diverge — they're byte-for-byte copies. The new blocks, the ones that actually represent post-fork work, dedup at 6%.

### Block map compression: can we make 4KB viable?

4KB gets 82% post-fork dedup but costs 42.5MB per 10GB VM in block map metadata. The hypothesis: LZ4-compress the block map to bring the cost down.

**Measured block map sizes (sparse format, extrapolated to 10GB VM):**

| Block Size | Sparse Raw | Sparse LZ4 | Dense Raw | Dense LZ4 | LZ4 Ratio |
|-----------|-----------|-----------|----------|----------|-----------|
| 4KB       | 44.7MB    | 35.8MB    | 42.5MB   | 26.9MB   | 1.2x      |
| 8KB       | 22.4MB    | 18.0MB    | 21.2MB   | 13.6MB   | 1.2x      |
| 16KB      | 11.2MB    | 9.1MB     | 10.6MB   | 6.9MB    | 1.2x      |
| 32KB      | 5.6MB     | 4.6MB     | 5.3MB    | 3.5MB    | 1.2x      |
| 64KB      | 2.8MB     | 2.3MB     | 2.7MB    | 1.8MB    | 1.2x      |
| 128KB     | 1.4MB     | 1.2MB     | 1.3MB    | 939KB    | 1.2x      |

**LZ4 compression barely helps — only 1.2x across all block sizes.** BLAKE3 hashes are pseudorandom bytes with no internal structure. No general-purpose compressor can shrink them significantly. The zero entries in the dense format compress (that's why dense LZ4 is smaller), but the hash data itself is incompressible.

At 4KB with a 10GB VM, even after LZ4 compression:
- 35.8MB per VM (sparse) or 26.9MB per VM (dense)
- 100 VMs per host: 3.6GB just for block maps
- 600 VMs per host: **21.5GB** just for block maps

**Compression does not make 4KB viable.** The metadata cost is fundamental, not an encoding problem.

### Fork overlay: the bright spot

The fork overlay (blocks that differ between base and forked VM) is tiny at every block size:

| Block Size | Overlay Entries | Overlay LZ4 | 10GB VM Extrapolated |
|-----------|----------------|-------------|---------------------|
| 4KB       | 1,302          | 24.8KB      | ~694KB              |
| 8KB       | 656            | 13.8KB      | ~385KB              |
| 16KB      | 333            | 7.1KB       | ~199KB              |
| 128KB     | 50             | 1.1KB       | ~30KB               |

The delta between base and fork is small. But the parent block map must be resident in memory for overlay lookups — the overlay saves transfer/storage, not memory.

## Results: Independent Builds (no shared base)

Same workloads, but each image built from scratch (separate ext4 filesystems).

### Dedup + Compression

| Block Size | Dedup Ratio | LZ4 Ratio | Combined (Dedup+LZ4) | Block Map/10GB VM |
|-----------|------------|-----------|----------------------|-------------------|
| 4KB       | 2.16x      | 1.69x     | 3.69x                | 42.5MB            |
| 16KB      | 1.64x      | 1.84x     | 3.24x                | 10.6MB            |
| 32KB      | 1.54x      | 1.90x     | 3.13x                | 5.3MB             |
| 64KB      | 1.46x      | 1.93x     | 3.05x                | 2.7MB             |
| 128KB     | 1.40x      | 1.96x     | 3.02x                | 1.3MB             |
| 256KB     | 1.35x      | 1.98x     | 2.97x                | 680KB             |

### Cross-Image Sharing (base <-> vm-0-express)

| Block Size | Jaccard Similarity | Shared Savings |
|-----------|-------------------|----------------|
| 4KB       | 45.2%             | 145MB          |
| 16KB      | 20.3%             | 83MB           |
| 32KB      | 16.0%             | 68MB           |
| 64KB      | 12.7%             | 57MB           |
| 128KB     | 12.1%             | 56MB           |
| 256KB     | 10.4%             | 50MB           |

Without a shared base, dedup collapses at every block size above 4KB.

## Implications for GLIDEv2

### 1. The dedup claim for post-fork content is false at any block size above 4KB

GLIDEv2.md claims "a thousand tenants running Next.js apps share one copy of the base OS, Node runtime, and common npm packages." The data shows:

- **Base OS and Node runtime**: yes, these dedup perfectly — they're byte-identical from the bless.
- **Common npm packages installed after fork**: no. At 128KB, only 6% of post-fork `npm install` blocks match between two VMs doing the same install. Even at 8KB it's only 15%. The claim that "common npm packages" are shared is wrong at any block size above 4KB.

The dedup story works for **content that's in the blessed base image and never written to**. It does not work for anything written after fork.

### 2. The bless pipeline carries the entire dedup story

At any block size above 4KB, cross-VM dedup is binary:
- Content in the blessed base: 100% dedup (byte-identical by construction)
- Content written after fork: ≤15% dedup (effectively no dedup)

This means the bless pipeline isn't just important — it's the only mechanism that produces meaningful dedup. **Every byte of dedup savings comes from what's in the blessed image.** Anything a tenant writes after fork (packages, dependencies, app code) is stored once per VM.

**Action:** The bless pipeline should include as much as possible. Don't just bless "Ubuntu 22.04" — bless "Ubuntu 22.04 + Node 20 + the top 50 npm packages pre-installed." The more you put in the base, the less tenants write after fork, the more dedup you get.

### 3. The dedup cliff is at the ext4 page size

The cliff from 82% to 15% happens at exactly one point: 4KB → 8KB. This is not a gradual degradation — it's a phase transition at the ext4 page size boundary. Above 4KB, post-fork dedup is noise (5-15%) regardless of block size. This means:

- Choosing between 8KB, 16KB, 32KB, 64KB, or 128KB has **no meaningful impact on post-fork dedup** — they're all bad
- The choice between those sizes is purely about metadata cost, write amplification, and S3 efficiency
- 4KB is the only size that gets real post-fork dedup, and it's not viable due to metadata cost

### 4. Block map compression does not change the equation

LZ4 achieves only 1.2x on block maps because BLAKE3 hashes are pseudorandom. At 4KB:
- Uncompressed: 42.5MB per 10GB VM
- LZ4 compressed: 35.8MB per 10GB VM
- At 600 VMs/host: 21.5GB just for block maps

Even with a hypothetical 3x compressor (which is physically impossible on random data), 4KB block maps would cost 14MB per VM / 8.4GB per host at 600 VMs. The metadata cost of 4KB blocks is a fundamental information-theoretic constraint, not an engineering problem to solve.

### 5. The regional bloom filter is not worth building

- For base blocks: unnecessary (already dedup from the bless)
- For post-fork blocks at 128KB: only 6% would match even with perfect global dedup
- For post-fork blocks at 4KB: 82% would match — but 4KB isn't viable

### 6. Compression partially compensates

LZ4 ratio improves from 1.70x (4KB) to 1.91x (128KB). Larger blocks give the compressor more context. This is why the combined dedup+LZ4 ratio stays flat despite worse dedup at larger sizes — compression picks up the slack. But compression doesn't produce *dedup* (shared blocks across VMs), it just shrinks the per-VM storage.

### 7. Block size recommendation

**Keep 128KB.** The choice between sizes above 4KB doesn't affect post-fork dedup (all are ≤15%). 128KB gives:
- 32x less metadata than 4KB
- Better LZ4 compression (1.91x vs 1.70x)
- Reasonable write amplification (32x worst case, ~1x for typical sequential workloads)
- Proven in v1

**The dedup strategy at 128KB is simple:** bless everything you can. Accept that post-fork writes don't dedup. Invest in the bless pipeline, not in block size changes.

### 8. Paths to actual post-fork dedup

If post-fork dedup turns out to matter (tenants write a lot after fork, bless coverage is insufficient), the only viable paths are:

1. **File-level storage (virtio-fs / FUSE):** Bypass the block device layer entirely. Dedup at the file level where metadata can't poison content hashes. Fundamental architecture change — no more NBD.

2. **Aggressive bless coverage:** Pre-install common packages in blessed bases. Language-specific bases (Node, Python, Go) with popular packages pre-installed. This doesn't fix dedup — it avoids the problem by reducing post-fork writes.

3. **4KB blocks with different memory model:** Use 4KB for addressing but don't keep the full block map resident. Page-fault style loading of block map regions. Complex, essentially building a page table for the page table.

None of these are simple. Option 2 (aggressive bless) is the pragmatic choice.

## Open questions

1. **How much do tenants write after fork in practice?** If it's 5% of disk, the block size barely matters. If it's 30%, the lack of post-fork dedup is significant. Need production data.

2. **Would a sub-block dirty tracking scheme help?** Track dirty regions at 4KB granularity within 128KB blocks. Hash and store at 128KB for metadata efficiency, but only re-hash the 128KB block when one of its 4KB pages changes. This doesn't help dedup, but could reduce write amplification.

3. **How compressible are block maps with better algorithms?** LZ4 gets 1.2x on hash data. zstd might get 1.3-1.5x. But even 2x compression doesn't make 4KB viable (21MB per 10GB VM). The hashes are fundamentally incompressible.

4. **Would a different guest filesystem change the picture?** The 4KB cliff is specific to ext4's page size. A filesystem that separates metadata from data (like a log-structured FS) might improve dedup at larger block sizes. But changing the guest filesystem may not be acceptable.

## Reproducing

```bash
# Generate test images (requires Docker)
./scripts/gen_dedup_images.sh /tmp/glidefs-dedup-test

# Run measurement
cargo run --release --bin dedup_measure -- /tmp/glidefs-dedup-test/forked/*.raw
cargo run --release --bin dedup_measure -- /tmp/glidefs-dedup-test/independent/*.raw

# Fine-grained block sizes (to see the cliff)
cargo run --release --bin dedup_measure -- --block-sizes 4096,8192,16384 /tmp/glidefs-dedup-test/forked/*.raw
```
