# Block Size Analysis: Empirical Dedup Measurement

## Methodology

Measured content-addressed deduplication effectiveness at 6 block sizes (4KB–256KB) using real ext4 filesystem images built from Ubuntu 22.04 + Node.js 20 workloads. All hashing uses BLAKE3-128 (same as GLIDEv2). Compression uses LZ4 (same as GLIDEv2).

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
| 16KB      | ~328             | ~23                       | **7%**              |
| 32KB      | ~170             | ~8                        | **5%**              |
| 64KB      | ~92              | ~5                        | **5%**              |
| 128KB     | ~50              | ~3                        | **6%**              |
| 256KB     | ~30              | ~2                        | **7%**              |

**At 4KB, 82% of npm install blocks match between two VMs. At 128KB, 6%.** Two VMs doing the identical `npm install express` from the same fork, and 47 out of 50 new blocks are unique.

The mechanism is clear once you see it: ext4 metadata (timestamps, journal, inode updates) is scattered across the same 128KB blocks that contain file content. Different timestamps poison the hash even when the file data is identical. At 4KB, metadata pages are isolated from content pages — hence 82%. At 16KB the cliff is immediate (82% → 7%) because you start straddling the boundary.

### What the headline numbers hide

The 97% overall Jaccard at 128KB means: "of all blocks in both images, 97% match." But the base image contributes ~2,760 blocks that were never written to. The npm install writes ~50 new blocks. The 97% is (2,760 shared base) / (2,760 + 50 + 49 - 3 shared new). The base blocks were never going to diverge — they're byte-for-byte copies. The new blocks, the ones that actually represent post-fork work, dedup at 6%.

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

### 1. The dedup claim for post-fork content is false at 128KB

GLIDEv2.md claims "a thousand tenants running Next.js apps share one copy of the base OS, Node runtime, and common npm packages." The data shows:

- **Base OS and Node runtime**: yes, these dedup perfectly — they're byte-identical from the bless.
- **Common npm packages installed after fork**: no. At 128KB, only 6% of post-fork `npm install` blocks match between two VMs doing the same install. The claim that "common npm packages" are shared is wrong.

The dedup story at 128KB works for **content that's in the blessed base image and never written to**. It does not work for anything written after fork.

### 2. The bless pipeline carries the entire dedup story

At 128KB, cross-VM dedup is binary:
- Content in the blessed base: 100% dedup (byte-identical by construction)
- Content written after fork: ~6% dedup (effectively no dedup)

This means the bless pipeline isn't just important — it's the only mechanism that produces meaningful dedup at 128KB. **Every byte of dedup savings comes from what's in the blessed image.** Anything a tenant writes after fork (packages, dependencies, app code) is stored once per VM.

**Action:** The bless pipeline should include as much as possible. Don't just bless "Ubuntu 22.04" — bless "Ubuntu 22.04 + Node 20 + the top 50 npm packages pre-installed." The more you put in the base, the less tenants write after fork, the more dedup you get.

### 3. Block size matters — but the tradeoff is different than expected

The combined dedup+LZ4 ratio across ALL blocks (base + new) is stable at ~5.8x regardless of block size. But this stability is misleading — it's because the base blocks dominate. For the post-fork content that tenants actually care about, block size matters enormously: 82% dedup at 4KB, 6% at 128KB.

The question becomes: **how much post-fork content do tenants write?** If the blessed base covers 95% of what they need, then the 6% dedup on the remaining 5% barely matters. If the base only covers 70% and tenants write 30%, the 6% vs 82% difference is significant.

### 4. The regional bloom filter question

Earlier analysis suggested a regional bloom filter for cross-host dedup. The data shows:

- For base blocks: unnecessary (already dedup from the bless)
- For post-fork blocks at 128KB: only 6% would match even with perfect global dedup
- For post-fork blocks at 4KB: 82% would match — but the metadata cost is prohibitive

**The bloom filter is not worth building at 128KB.** The post-fork blocks barely match even between VMs on the same host with the same ext4 allocator state. Cross-host would be worse.

### 5. Compression partially compensates

LZ4 ratio improves from 1.70x (4KB) to 1.91x (128KB). Larger blocks give the compressor more context. This is why the combined dedup+LZ4 ratio stays flat despite worse dedup at larger sizes — compression picks up the slack. But compression doesn't produce *dedup* (shared blocks across VMs), it just shrinks the per-VM storage.

### 6. Block size recommendation

**Keep 128KB, but be honest about what dedup you're getting.**

For the fork-based architecture where blessed bases cover most content:
- Dedup+LZ4 combined is ~5.8x across all block sizes (dominated by base)
- 128KB has 32x less metadata than 4KB
- The 6% post-fork dedup at 128KB is acceptable *if* the bless covers enough

**The risk:** if blessed base coverage is low, tenants write a lot after fork, and the 128KB block size means all that content is stored once per VM with no cross-VM sharing. The mitigation is aggressive bless coverage, not a different block size — because even 32KB only gets 5% post-fork dedup.

**The exception:** 4KB blocks get 82% post-fork dedup. If the metadata cost (42.5MB per 10GB VM, 25GB at 600 VMs) were acceptable, 4KB would be strictly better. At current scale it's not. But if the block map were compressed or sparse-encoded, it might become viable. This is worth revisiting.

## Open questions

1. **How much do tenants write after fork in practice?** If it's 5% of disk, the block size barely matters. If it's 30%, the post-fork dedup gap is significant. Need production data.

2. **Would a sub-block dirty tracking scheme help?** Track dirty regions at 4KB granularity within 128KB blocks. Hash and store at 128KB for metadata efficiency, but only re-hash the 128KB block when one of its 4KB pages changes. This doesn't help dedup, but could reduce write amplification.

3. **Can the block map be compressed?** At 4KB, the block map is 42.5MB per 10GB VM. But it's highly compressible (many entries are the zero-block hash, adjacent entries often share prefixes). Compressed block maps could make 4KB viable.

## Reproducing

```bash
# Generate test images (requires Docker)
./scripts/gen_dedup_images.sh /tmp/glidefs-dedup-test

# Run measurement
cargo run --release --bin dedup_measure -- /tmp/glidefs-dedup-test/forked/*.raw
cargo run --release --bin dedup_measure -- /tmp/glidefs-dedup-test/independent/*.raw
```
