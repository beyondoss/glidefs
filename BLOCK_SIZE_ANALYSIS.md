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

**Action:** The bless pipeline should include the OS and language runtime. Don't just bless "Ubuntu 22.04" — bless "Ubuntu 22.04 + Node 20 + system tools (git, curl, build-essential)." Application-level packages (npm, pip) can't be pre-installed because tenants pin specific versions via lockfiles. The blessed base ceiling is ~500-700MB per language runtime.

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

2. **Maximize bless coverage:** Include OS, language runtime, and system tools in blessed bases. Application-level packages can't be pre-installed (version pinning), so the bless ceiling is ~500-700MB per runtime. This doesn't fix dedup — it avoids the problem for the blessable portion, but everything tenants install after fork (all of `node_modules`) remains in the gap.

3. **4KB blocks with different memory model:** Use 4KB for addressing but don't keep the full block map resident. Page-fault style loading of block map regions. Complex, essentially building a page table for the page table.

None of these are simple. Option 2 (aggressive bless) is the pragmatic choice.

## Full Metadata Budget: 128KB vs 4KB

Previous analysis only counted block map memory. This section accounts for *every* metadata structure that scales with block count: runtime block maps, host pack index, manifest wire format, and cache indexes.

### Reference Host

- 256GB RAM, 2TB NVMe SSD
- 600 VMs, 10GB virtual disk each
- 10 production VMs (continuous flush), 590 forks (previews/dev)
- 32GB memory cache, 500GB SSD cache (foyer)
- Post-fork divergence: 5% baseline (500MB new writes per fork, typical npm install + build)

### Entry Sizes (from code, not design doc)

**Runtime block map (dense arrays, allocated for ALL chunks in virtual disk):**

| Field | Size | Source |
|-------|------|--------|
| AtomicBlockMap per chunk | 24 bytes | `hash_lo: AtomicU64` + `hash_hi: AtomicU64` + `sequences: AtomicU64` |
| `block_states` per chunk | 1 byte | `AtomicU8` (Clean/Dirty/Syncing) |
| `present_chunks` bitmap | 1 bit | `AtomicU64` packed, 1 per 64 blocks |
| **Total per chunk** | **~25 bytes** | `block_map.rs:162-169`, `write_cache.rs:335` |

**Manifest (serialized to S3, sparse — only non-zero entries):**

| Field | Size | Source |
|-------|------|--------|
| `ManifestBlockEntry` | 25 bytes | `chunk_index: u64` + `hash: [u8; 16]` + `flags: u8` |
| `ManifestPackEntry` | 40 bytes | `hash: [u8; 16]` + `pack_id: Uuid` (16B) + `offset: u32` + `comp_length: u32` |
| **Total per non-zero block** | **65 bytes** | `manifest.rs:23-24` |

**Host pack index (`DashMap<Blake3Hash, PackLocation>`, shared across all VMs):**

| Field | Size | Source |
|-------|------|--------|
| Key: `Blake3Hash` | 16 bytes | |
| Value: `PackLocation` | 24 bytes | `pack_id: Uuid` (16B) + `offset: u32` + `comp_length: u32` |
| DashMap overhead per entry | ~16 bytes | hashbrown control bytes, bucket metadata |
| **Total per unique block** | **~56 bytes** | `pack_index.rs:13-14` |

### Per-VM Runtime Memory (Current Implementation — Dense Arrays)

The runtime uses dense `AtomicBlockMap` — every chunk slot in the virtual disk is allocated, regardless of whether it's been written. This is the **actual code**, not the fork overlay design.

10GB virtual disk = 10GB / chunk_size chunks.

| Component | 128KB (81,920 chunks) | 4KB (2,621,440 chunks) |
|-----------|----------------------|------------------------|
| AtomicBlockMap (24 B/chunk) | 1.97MB | 62.9MB |
| block_states (1 B/chunk) | 80KB | 2.5MB |
| present_chunks (1 bit/chunk) | 10KB | 320KB |
| **Per VM** | **2.06MB** | **65.7MB** |
| **600 VMs** | **1.24GB** | **39.4GB (15.4% of host)** |

**At 4KB without fork overlays, block maps alone consume 39.4GB.** Fork overlays (GLIDEv2.md §Block Map Design) are designed but not implemented in the codebase. They are a prerequisite for 4KB viability.

### With Fork Overlays (Designed, Not Yet Implemented)

Fork overlay: `Arc<AtomicBlockMap>` (shared parent, dense) + `HashMap<u64, OverlayEntry>` (per-fork, sparse).

10 parents hold dense block maps. 590 forks hold only diverged entries.

**Parents (10 VMs, dense):**

| | 128KB | 4KB |
|---|---|---|
| Per parent | 2.06MB | 65.7MB |
| **10 parents** | **20.6MB** | **657MB** |

**Forks (590 VMs, HashMap with ~38 bytes per entry):**

| Divergence | 128KB entries/fork | 128KB 590 forks | 4KB entries/fork | 4KB 590 forks |
|------------|-------------------|-----------------|-----------------|---------------|
| 1% (100MB) | 819 | 18MB | 26,214 | 585MB |
| 5% (500MB) | 4,096 | 90MB | 131,072 | 2.93GB |
| 10% (1GB) | 8,192 | 180MB | 262,144 | 5.86GB |
| 20% (2GB) | 16,384 | 360MB | 524,288 | 11.7GB |

**Total block map memory with overlays:**

| Divergence | 128KB | 4KB | Ratio |
|------------|-------|-----|-------|
| 1% | 39MB | 1.24GB | 32x |
| **5%** | **111MB** | **3.59GB** | **32x** |
| 10% | 201MB | 6.52GB | 32x |
| 20% | 381MB | 12.4GB | 33x |

The ratio is ~32x at every divergence level. Fork overlays don't change the relative cost — they reduce the absolute cost at both sizes equally.

### Host Pack Index

One `DashMap` for all blocks uploaded to S3. Deduplicated — same hash appears once.

Estimate: 60 VMs flushed to S3 (10 production + 50 that forked/slept). With host-level dedup, base blocks stored once.

| | 128KB | 4KB |
|---|---|---|
| Unique base blocks | ~80K | ~2.56M |
| Unique post-fork blocks (60 VMs, ~50% cross-VM overlap) | ~220K | ~4.5M |
| **Total unique entries** | **~300K** | **~7M** |
| **Memory (56 B/entry)** | **17MB** | **392MB** |

### Manifest Size

Manifest is sparse (only non-zero entries). 65 bytes per block (25B block entry + 40B pack entry with UUID pack ID).

For a 10GB VM with 10GB of written data (all blocks non-zero):

| | 128KB (80K entries) | 4KB (2.56M entries) |
|---|---|---|
| Block map section | 2.0MB | 64.0MB |
| Pack index section | 3.2MB | 102.4MB |
| **Total manifest** | **5.2MB** | **166.4MB** |
| Time to upload (10Gbps) | ~4ms | ~133ms |

Continuous flush (manifest upload every ~60s per production VM):
- At 128KB: 5.2MB/min — negligible
- At 4KB: 166.4MB/min — 2.8MB/s sustained per production VM

Fork operation (S3 CopyObject of manifest):
- At 128KB: 5.2MB copy — instant
- At 4KB: 166.4MB copy — noticeable but bounded

### Cache Index Overhead (Estimated — Needs Measurement)

foyer maintains an in-memory index for cached entries in both memory and SSD tiers. Per-entry overhead depends on foyer internals. Estimates below use ~150B (memory tier, S3-FIFO queues) and ~50B (SSD tier, location tracking).

| Tier | Cache Size | 128KB | 4KB |
|------|-----------|-------|-----|
| Memory (150 B/entry est.) | 32GB | 38MB (256K entries) | 1.2GB (8M entries) |
| SSD (50 B/entry est.) | 500GB | 200MB (4M entries) | 6.4GB (128M entries) |
| **Cache index total** | | **238MB** | **7.6GB** |

The SSD cache index is the single largest metadata cost at 4KB. This estimate needs empirical validation with foyer.

### Dirty Block Scanning

Current code scans *all* `block_states` to find dirty blocks — `O(num_chunks)` per flush:
- At 128KB: 82K entries — fast
- At 4KB: 2.6M entries — 32x slower

The dirty set (`HashSet<u64>` of dirty offsets, making flush `O(dirty_count)`) is designed in GLIDEv2.md but not yet implemented. It becomes essential at 4KB.

### Total Metadata Budget

All structures combined, at 5% fork divergence:

| Structure | 128KB | 4KB | Source |
|-----------|-------|-----|--------|
| Block maps (fork overlays) | 111MB | 3.59GB | Dense parents + sparse fork HashMaps |
| Host pack index | 17MB | 392MB | Shared DashMap, deduplicated, 56 B/entry |
| SSD cache index | 200MB | 6.4GB | **Estimated**, needs measurement |
| Memory cache index | 38MB | 1.2GB | **Estimated**, needs measurement |
| **Total metadata** | **366MB** | **11.6GB** | |
| **% of 256GB host** | **0.14%** | **4.5%** | |

At 10% divergence:

| Structure | 128KB | 4KB |
|-----------|-------|-----|
| Block maps | 201MB | 6.52GB |
| Host pack index | 17MB | 392MB |
| SSD cache index | 200MB | 6.4GB |
| Memory cache index | 38MB | 1.2GB |
| **Total** | **456MB** | **14.5GB** |
| **% of 256GB host** | **0.18%** | **5.7%** |

### Corrections to Prior Analysis

Previous sessions cited "1.3GB for 600 VMs at 4KB with fork overlays" and "only 50% more than 128KB's 880MB." This was wrong:

1. **Block maps only.** Excluded host pack index (~336MB), SSD cache index (~6.4GB), memory cache index (~1.2GB).
2. **Assumed 1% divergence.** Based on npm-install-express on 512MB test images. Real Boxes workloads (full npm install + build) are 5-10% divergence.
3. **Used design doc entry sizes, not code.** Doc: 17 bytes/entry (hash + flags). Runtime: 25 bytes/chunk (AtomicBlockMap + block_states).
4. **Pack ID is UUID (16B).** ManifestPackEntry: 40 bytes. Host pack index entries: ~56 bytes. Snowflake IDs (8B) were considered but rejected — no coordinator exists for worker ID assignment, and the savings (~640KB per manifest) don't justify the complexity.

### Prerequisites for 4KB Viability

If 4KB were pursued, these would need to be implemented first:
1. **Fork overlays** — without them, dense block maps consume 39.4GB for 600 VMs
2. **Dirty set** — without it, flush scans 2.6M entries per VM per cycle
3. **Evaluation of foyer overhead** — the SSD cache index (6.4GB estimated) is the largest unknown

## Open questions

1. **How much do tenants write after fork in practice?** If it's 5% of disk, the block size barely matters. If it's 30%, the lack of post-fork dedup is significant. Need production data. This directly determines block map memory at 4KB (3.6GB at 5% divergence vs 12.4GB at 20%).

2. **Would a sub-block dirty tracking scheme help?** Track dirty regions at 4KB granularity within 128KB blocks. Hash and store at 128KB for metadata efficiency, but only re-hash the 128KB block when one of its 4KB pages changes. This doesn't help dedup, but could reduce write amplification.

3. ~~**How compressible are block maps with better algorithms?**~~ **Answered.** Measured LZ4 at 1.2x. The hashes are pseudorandom — no compressor can help. See §Block map compression.

4. **Would a different guest filesystem change the picture?** The 4KB cliff is specific to ext4's page size. A filesystem that separates metadata from data (like a log-structured FS) might improve dedup at larger block sizes. But changing the guest filesystem may not be acceptable.

5. **What is foyer's actual per-entry memory overhead?** The SSD cache index is estimated at ~6.4GB for 4KB blocks with 500GB cache. This is the single largest metadata unknown and the largest contributor to 4KB's memory cost. Needs empirical measurement.

6. ~~**GLIDEv2.md manifest size numbers are wrong.**~~ **Fixed.** GLIDEv2.md manifest sizes and pack index memory numbers updated to match code (UUID pack IDs, 40B pack entries).

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
