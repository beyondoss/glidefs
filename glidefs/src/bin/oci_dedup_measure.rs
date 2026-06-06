#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! Measure content-addressed dedup for OCI images: **merged** (one ext4 per image,
//! all layers flattened — what `bless --oci` does today) vs **layers-survive**
//! (each OCI layer stored as its own content-addressed ext4 artifact).
//!
//! Crucially this drives the *real* GlideFS code paths so the numbers reflect
//! production, not a re-implementation:
//!   - `ext4::tar_convert::convert_oci_layers_to_ext4` (the actual merge + writer)
//!   - the production deterministic-UUID derivation from `bless.rs`
//!   - `block_map::{blake3_128, lz4_compress}` (the actual flush hashing/compression)
//!   - `pack::content_pack_id` (the actual S3 pack key derivation)
//!
//! It reports dedup at TWO granularities:
//!   - BLOCK level: union of unique 128 KiB block hashes (theoretical upper bound;
//!     this is what `dedup_measure` computes).
//!   - PACK level: union of unique (chunk_idx, pack_id) packs — this is what GlideFS
//!     *actually* dedups against in S3, since the pack key is
//!     `chunks/{chunk_idx:04}/{pack_id:016x}.pack`.
//!
//! Usage:
//!   skopeo copy docker://debian:bookworm-slim dir:/tmp/oci/debian
//!   skopeo copy docker://python:3.12-slim-bookworm dir:/tmp/oci/python
//!   cargo run --release --bin oci_dedup_measure -- /tmp/oci/debian /tmp/oci/python ...

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use ext4::tar_convert::{ConvertOptions, convert_oci_layers_to_ext4};
use ext4::writer::WriterOption;
use glidefs::block::block_map::{Blake3Hash, blake3_128, lz4_compress};
use glidefs::block::pack::{PackId, content_pack_id};

const BLOCK_SIZE: usize = 128 * 1024; // 131072 — production BLOCK_SIZE
const CHUNK_SIZE: usize = 128 * 1024 * 1024; // 1 ext4 block group == 1 GlideFS chunk
const BLOCKS_PER_CHUNK: usize = CHUNK_SIZE / BLOCK_SIZE;

/// Exact copy of `bless.rs::deterministic_uuid` — content-addressed UUID so that
/// identical input (same manifest, or same layer) yields a byte-identical ext4.
fn deterministic_uuid(seed: &str) -> [u8; 16] {
    let mut uuid = *blake3_128(seed.as_bytes()).as_bytes();
    uuid[6] = (uuid[6] & 0x0f) | 0x80; // version 8 (custom)
    uuid[8] = (uuid[8] & 0x3f) | 0x80; // variant 1 (RFC 4122)
    uuid
}

/// Production device-size estimate from `bless.rs::run_bless_oci`.
fn device_size_for(total_compressed: u64) -> i64 {
    let estimated = (total_compressed * 3).max(64 * 1024 * 1024);
    estimated.next_power_of_two() as i64
}

/// Decompress a gzip or zstd OCI layer blob into a fresh seekable temp file.
fn decompress_layer(blob: &Path) -> std::io::Result<File> {
    let mut f = File::open(blob)?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)?;
    f.seek(SeekFrom::Start(0))?;

    let mut out = tempfile::tempfile()?;
    if magic[0] == 0x1f && magic[1] == 0x8b {
        let mut dec = flate2::read::GzDecoder::new(f);
        std::io::copy(&mut dec, &mut out)?;
    } else if magic == [0x28, 0xb5, 0x2f, 0xfd] {
        let mut dec = zstd::Decoder::new(f)?;
        std::io::copy(&mut dec, &mut out)?;
    } else {
        // already an uncompressed tar
        std::io::copy(&mut f, &mut out)?;
    }
    out.seek(SeekFrom::Start(0))?;
    Ok(out)
}

struct ImageSpec {
    name: String,
    manifest_digest_seed: String, // content-addressed seed for the merged UUID
    layers: Vec<LayerRef>,        // bottom-to-top
}

#[derive(Clone)]
struct LayerRef {
    digest: String,      // "sha256:..."
    blob: PathBuf,       // path to the (compressed) blob file
    compressed_size: u64,
}

fn load_image(dir: &Path) -> ImageSpec {
    let manifest_path = dir.join("manifest.json");
    let bytes = std::fs::read(&manifest_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", manifest_path.display()));
    // Content-addressed seed for the merged image. The literal value is irrelevant
    // to the comparison (production uses the sha256 manifest digest); all that
    // matters is that it is stable per-image and differs across images — which any
    // hash of the manifest guarantees.
    let seed = format!("blake3:{:032x}", u128::from_le_bytes(*blake3_128(&bytes).as_bytes()));

    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("parse manifest.json");
    let mut layers = Vec::new();
    for l in v["layers"].as_array().expect("manifest.layers[]") {
        let digest = l["digest"].as_str().expect("layer.digest").to_string();
        let hex = digest.strip_prefix("sha256:").unwrap_or(&digest);
        let blob = dir.join(hex);
        let compressed_size = std::fs::metadata(&blob).map(|m| m.len()).unwrap_or(0);
        layers.push(LayerRef { digest, blob, compressed_size });
    }

    ImageSpec {
        name: dir.file_name().unwrap_or_default().to_string_lossy().to_string(),
        manifest_digest_seed: seed,
        layers,
    }
}

/// Build a deterministic ext4 from `layer_blobs` (bottom-to-top), using the exact
/// production writer options. Returns the ext4 image bytes (as a temp File).
fn build_ext4(layer_blobs: &[PathBuf], total_compressed: u64, uuid_seed: &str) -> File {
    let mut layers: Vec<File> =
        layer_blobs.iter().map(|p| decompress_layer(p).expect("decompress layer")).collect();

    let opts = ConvertOptions {
        convert_backslash: false,
        writer_options: vec![
            WriterOption::MaximumDiskSize(device_size_for(total_compressed)),
            WriterOption::Uuid(deterministic_uuid(uuid_seed)),
            WriterOption::Journal(1024), // 4 MiB journal — same as bless
        ],
    };

    let out = tempfile::tempfile().expect("tempfile");
    let mut fs = convert_oci_layers_to_ext4(&mut layers, out, &opts).expect("convert to ext4");
    fs.seek(SeekFrom::Start(0)).expect("seek ext4");
    fs
}

fn is_zero(data: &[u8]) -> bool {
    let (prefix, chunks, suffix) = unsafe { data.align_to::<u64>() };
    prefix.iter().all(|&b| b == 0)
        && chunks.iter().all(|&w| w == 0)
        && suffix.iter().all(|&b| b == 0)
}

/// One ext4 image's content, decomposed exactly as GlideFS would store it.
struct ImageBlocks {
    /// unique non-zero block hash -> compressed length (block-level view)
    block_comp: HashMap<Blake3Hash, usize>,
    /// (chunk_idx, pack_id) -> pack stored bytes (pack-level view = real S3 objects)
    packs: HashMap<(u32, PackId), usize>,
    nonzero_blocks: usize,
    zero_blocks: usize,
}

/// Read an ext4 image and decompose into block-level and pack-level dedup units,
/// using the production hashing (`blake3_128`), compression (`lz4_compress`) and
/// pack-id (`content_pack_id`).
fn decompose(mut img: File) -> ImageBlocks {
    let mut block_comp: HashMap<Blake3Hash, usize> = HashMap::new();
    let mut packs: HashMap<(u32, PackId), usize> = HashMap::new();
    let mut nonzero_blocks = 0usize;
    let mut zero_blocks = 0usize;

    img.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = vec![0u8; BLOCK_SIZE];
    let mut block_idx: usize = 0;
    // Accumulate the current chunk's non-zero blocks for pack-id computation.
    let mut cur_chunk: u32 = 0;
    let mut cur_blocks: Vec<(Blake3Hash, u32, bytes::Bytes)> = Vec::new();

    let flush_pack =
        |chunk: u32, blocks: &mut Vec<(Blake3Hash, u32, bytes::Bytes)>, packs: &mut HashMap<(u32, PackId), usize>| {
            if blocks.is_empty() {
                return;
            }
            // content_pack_id requires blocks sorted by chunk_offset (the flush path
            // produces them in offset order); ours already are.
            let pid = content_pack_id(blocks);
            let stored: usize = blocks.iter().map(|(_, _, c)| c.len()).sum();
            packs.entry((chunk, pid)).or_insert(stored);
            blocks.clear();
        };

    loop {
        buf.fill(0);
        let mut filled = 0;
        while filled < BLOCK_SIZE {
            match img.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => panic!("read ext4: {e}"),
            }
        }
        if filled == 0 {
            break;
        }

        let chunk = (block_idx / BLOCKS_PER_CHUNK) as u32;
        if chunk != cur_chunk {
            flush_pack(cur_chunk, &mut cur_blocks, &mut packs);
            cur_chunk = chunk;
        }

        let data = &buf[..BLOCK_SIZE];
        if is_zero(data) {
            zero_blocks += 1; // zero blocks are never uploaded by GlideFS
        } else {
            nonzero_blocks += 1;
            let hash = blake3_128(data);
            let comp = lz4_compress(data);
            block_comp.entry(hash).or_insert(comp.len());
            let chunk_offset = (block_idx % BLOCKS_PER_CHUNK) as u32;
            cur_blocks.push((hash, chunk_offset, bytes::Bytes::from(comp)));
        }
        block_idx += 1;
    }
    flush_pack(cur_chunk, &mut cur_blocks, &mut packs);

    ImageBlocks { block_comp, packs, nonzero_blocks, zero_blocks }
}

fn human(bytes: usize) -> String {
    let b = bytes as f64;
    if bytes >= 1 << 30 {
        format!("{:.2} GiB", b / (1u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.1} MiB", b / (1u64 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.1} KiB", b / (1u64 << 10) as f64)
    } else {
        format!("{bytes} B")
    }
}

fn main() {
    let dirs: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    if dirs.len() < 2 {
        eprintln!("usage: oci_dedup_measure <skopeo-dir-1> <skopeo-dir-2> [...]");
        std::process::exit(2);
    }

    let images: Vec<ImageSpec> = dirs.iter().map(|d| load_image(d)).collect();

    // --- Layer inventory & shared-layer detection ---
    println!("Images & layers (bottom→top):");
    let mut layer_count: HashMap<String, usize> = HashMap::new();
    let mut layer_size: HashMap<String, u64> = HashMap::new();
    for img in &images {
        println!("  {} ({} layers)", img.name, img.layers.len());
        for l in &img.layers {
            *layer_count.entry(l.digest.clone()).or_insert(0) += 1;
            layer_size.insert(l.digest.clone(), l.compressed_size);
            println!("    {}  {}", &l.digest[7..7 + 16], human(l.compressed_size as usize));
        }
    }
    println!();
    println!("Shared layers (present in >1 image):");
    let mut any_shared = false;
    for (d, &c) in &layer_count {
        if c > 1 {
            any_shared = true;
            println!(
                "  {}  in {} images, {} compressed",
                &d[7..7 + 16],
                c,
                human(layer_size[d] as usize)
            );
        }
    }
    if !any_shared {
        println!("  (none — comparison will be uninformative)");
    }
    println!();

    // ============================================================
    // STRATEGY A — MERGED: one ext4 per image, all layers flattened.
    // ============================================================
    eprintln!("Building merged ext4 per image (real convert_oci_layers_to_ext4)...");
    let mut merged_blocks: HashMap<Blake3Hash, usize> = HashMap::new();
    let mut merged_packs: HashMap<(u32, PackId), usize> = HashMap::new();
    let mut merged_naive_block = 0usize; // sum of per-image unique (no cross-image dedup)
    let mut merged_naive_pack = 0usize;
    let mut per_image_rows: Vec<(String, usize, usize, usize)> = Vec::new();

    for img in &images {
        let total_compressed: u64 = img.layers.iter().map(|l| l.compressed_size).sum();
        let blobs: Vec<PathBuf> = img.layers.iter().map(|l| l.blob.clone()).collect();
        eprint!("  {} ...", img.name);
        let ext4 = build_ext4(&blobs, total_compressed, &img.manifest_digest_seed);
        let d = decompose(ext4);
        eprintln!(" {} non-zero blocks", d.nonzero_blocks);

        let img_block_bytes: usize = d.block_comp.values().sum();
        let img_pack_bytes: usize = d.packs.values().sum();
        merged_naive_block += img_block_bytes;
        merged_naive_pack += img_pack_bytes;
        per_image_rows.push((img.name.clone(), d.nonzero_blocks, img_block_bytes, img_pack_bytes));

        for (h, c) in d.block_comp {
            merged_blocks.entry(h).or_insert(c);
        }
        for (k, v) in d.packs {
            merged_packs.entry(k).or_insert(v);
        }
    }
    let merged_block_bytes: usize = merged_blocks.values().sum();
    let merged_pack_bytes: usize = merged_packs.values().sum();

    // ============================================================
    // STRATEGY B — LAYERS-SURVIVE: each unique layer is its own
    // content-addressed ext4. Shared layers stored once.
    // ============================================================
    eprintln!("Building per-layer ext4 (each unique layer once)...");
    let mut layers_block_bytes_total = 0usize;
    let mut layers_pack_bytes_total = 0usize;
    // Also compute the *naive* (no cross-image dedup) layers cost: sum over images
    // of each image's layer artifacts, counting shared layers once per image.
    let mut naive_layers_block = 0usize;
    let mut naive_layers_pack = 0usize;

    // Build each unique layer once.
    let mut unique_layers: Vec<&LayerRef> = Vec::new();
    let mut seen = HashSet::new();
    for img in &images {
        for l in &img.layers {
            if seen.insert(l.digest.clone()) {
                unique_layers.push(l);
            }
        }
    }
    let mut per_layer_cost: HashMap<String, (usize, usize)> = HashMap::new(); // digest -> (block_bytes, pack_bytes)
    for l in &unique_layers {
        eprint!("  layer {} ...", &l.digest[7..7 + 16]);
        let ext4 = build_ext4(&[l.blob.clone()], l.compressed_size, &l.digest);
        let d = decompose(ext4);
        let bb: usize = d.block_comp.values().sum();
        let pb: usize = d.packs.values().sum();
        eprintln!(" {} non-zero blocks", d.nonzero_blocks);
        layers_block_bytes_total += bb;
        layers_pack_bytes_total += pb;
        per_layer_cost.insert(l.digest.clone(), (bb, pb));
    }
    for img in &images {
        for l in &img.layers {
            let (bb, pb) = per_layer_cost[&l.digest];
            naive_layers_block += bb;
            naive_layers_pack += pb;
        }
    }

    // ============================================================
    // RESULTS
    // ============================================================
    println!("Per-image merged ext4 (non-zero blocks, then stored bytes):");
    for (name, nz, bb, pb) in &per_image_rows {
        println!("  {:32} {:>7} blk  block-lvl {:>10}  pack-lvl {:>10}", name, nz, human(*bb), human(*pb));
    }
    println!();

    println!("============ DEDUP COMPARISON ============");
    println!();
    println!("  NAIVE (no cross-image dedup, store each image whole):");
    println!("    merged images:   block-level {:>11}   pack-level {:>11}", human(merged_naive_block), human(merged_naive_pack));
    println!();
    println!("  BLOCK-LEVEL dedup (theoretical upper bound — global 128KiB CAS):");
    println!("    A) merged:          {:>11}", human(merged_block_bytes));
    println!("    B) layers-survive:  {:>11}", human(layers_block_bytes_total));
    if merged_block_bytes > 0 {
        let saved = merged_block_bytes as i64 - layers_block_bytes_total as i64;
        println!(
            "    → layers saves {} ({:.1}% smaller)",
            human(saved.max(0) as usize),
            100.0 * saved as f64 / merged_block_bytes as f64
        );
    }
    println!();
    println!("  PACK-LEVEL dedup (what GlideFS ACTUALLY stores in S3):");
    println!("    key = chunks/{{chunk_idx}}/{{pack_id}}.pack");
    println!("    A) merged (same export prefix, best case): {:>11}", human(merged_pack_bytes));
    println!("    B) layers-survive (each layer once):       {:>11}", human(layers_pack_bytes_total));
    if merged_pack_bytes > 0 {
        let saved = merged_pack_bytes as i64 - layers_pack_bytes_total as i64;
        println!(
            "    → layers saves {} ({:.1}% smaller)",
            human(saved.max(0) as usize),
            100.0 * saved as f64 / merged_pack_bytes as f64
        );
    }
    println!();
    println!("  (reference) naive layers-survive, shared layer per-image: block {} / pack {}",
        human(naive_layers_block), human(naive_layers_pack));

    // Cross-image block sharing actually realized by the merged ext4 layout.
    println!();
    println!("Cross-image BLOCK sharing in the MERGED layout (does ext4 alignment preserve it?):");
    // Re-decompose per image to get per-image hash sets (cheap-ish; rebuild).
    let mut sets: Vec<(String, HashSet<Blake3Hash>)> = Vec::new();
    for img in &images {
        let total_compressed: u64 = img.layers.iter().map(|l| l.compressed_size).sum();
        let blobs: Vec<PathBuf> = img.layers.iter().map(|l| l.blob.clone()).collect();
        let ext4 = build_ext4(&blobs, total_compressed, &img.manifest_digest_seed);
        let d = decompose(ext4);
        sets.push((img.name.clone(), d.block_comp.into_keys().collect()));
    }
    for i in 0..sets.len() {
        for j in (i + 1)..sets.len() {
            let shared = sets[i].1.intersection(&sets[j].1).count();
            let union = sets[i].1.union(&sets[j].1).count();
            let jac = if union > 0 { 100.0 * shared as f64 / union as f64 } else { 0.0 };
            println!(
                "  {} <-> {}: {} shared blocks ({:.1}% Jaccard) = {} if globally CAS'd",
                sets[i].0, sets[j].0, shared, jac, human(shared * BLOCK_SIZE)
            );
        }
    }
}
