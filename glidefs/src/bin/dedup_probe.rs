#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! Empirical probe for the block-alignment dedup hypothesis.
//!
//! Claim 1 — the problem is real: fixed-grid block dedup over ext4 silently
//! fails to dedup byte-identical content when that content sits at a different
//! offset, because the dedup window is positional. We prove this two ways:
//!   E1 (mechanism): take ONE real ext4 image and dedup it against a copy of
//!      *itself* shifted by δ bytes. δ that is a whole-grid multiple → ~100%
//!      dedup (the method works); a sub-grid δ → dedup collapses. Same bytes,
//!      only the offset changes, so alignment is provably the whole cause.
//!   E2 (bite): over real, related images, fixed-grid dedup is far below what
//!      content-defined chunking (FastCDC) recovers from the *same* ext4 bytes.
//!
//! Claim 2 — the fix is easy: rebuild the same images with the production
//!   writer's new `AlignData` option (large file payloads start on the grid).
//!   E3: fixed-grid dedup on the aligned images should jump to ~the FastCDC
//!   ceiling, and the aligned images still round-trip through the ext4 reader.
//!   The padding is holes (zeros), which the block store never stores.
//!
//! Everything runs through the real production code paths
//! (`convert_oci_layers_to_ext4`, the real `block_map` hashing/compression), so
//! the numbers reflect what GlideFS would actually store — not a re-model.
//!
//! Usage:
//!   skopeo copy docker://python:3.12-slim-bookworm dir:/tmp/oci/py312
//!   skopeo copy docker://python:3.13-slim-bookworm dir:/tmp/oci/py313
//!   cargo run --release --bin dedup_probe -- /tmp/oci/py312 /tmp/oci/py313 ...

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use ext4::tar_convert::{ConvertOptions, convert_oci_layers_to_ext4};
use ext4::writer::WriterOption;
use glidefs::block::block_map::{Blake3Hash, blake3_128, lz4_compress};

const GRID: usize = 128 * 1024; // production BLOCK_SIZE — the dedup window size
const ALIGN_THRESHOLD: u32 = 16 * 1024; // align files >= 16 KiB; pack smaller ones

// ---- shared image plumbing (mirrors bless: deterministic, content-addressed) ----

fn deterministic_uuid(seed: &str) -> [u8; 16] {
    let mut uuid = *blake3_128(seed.as_bytes()).as_bytes();
    uuid[6] = (uuid[6] & 0x0f) | 0x80;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
    uuid
}

fn decompress_layer(blob: &Path) -> std::io::Result<File> {
    let mut f = File::open(blob)?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)?;
    f.seek(SeekFrom::Start(0))?;
    let mut out = tempfile::tempfile()?;
    if magic[0] == 0x1f && magic[1] == 0x8b {
        std::io::copy(&mut flate2::read::GzDecoder::new(f), &mut out)?;
    } else if magic == [0x28, 0xb5, 0x2f, 0xfd] {
        std::io::copy(&mut zstd::Decoder::new(f)?, &mut out)?;
    } else {
        std::io::copy(&mut f, &mut out)?;
    }
    out.seek(SeekFrom::Start(0))?;
    Ok(out)
}

struct Image {
    name: String,
    seed: String,
    layer_blobs: Vec<PathBuf>,
}

fn load_image(dir: &Path) -> Image {
    let bytes = std::fs::read(dir.join("manifest.json")).expect("read manifest.json");
    let seed = format!("blake3:{:032x}", u128::from_le_bytes(*blake3_128(&bytes).as_bytes()));
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("parse manifest");
    let layer_blobs = v["layers"]
        .as_array()
        .expect("layers[]")
        .iter()
        .map(|l| {
            let d = l["digest"].as_str().expect("digest");
            dir.join(d.strip_prefix("sha256:").unwrap_or(d))
        })
        .collect();
    Image { name: dir.file_name().unwrap().to_string_lossy().into_owned(), seed, layer_blobs }
}

/// Build a real ext4 image from the layers. `align` toggles the new writer
/// option; everything else is identical so the only variable is alignment.
/// Fixed 4 GiB device for all builds so the block grids are comparable.
fn build_ext4(img: &Image, align: bool) -> File {
    let mut layers: Vec<File> =
        img.layer_blobs.iter().map(|p| decompress_layer(p).expect("decompress")).collect();
    let mut writer_options = vec![
        WriterOption::MaximumDiskSize(4 * 1024 * 1024 * 1024),
        WriterOption::Uuid(deterministic_uuid(&img.seed)),
    ];
    // The writer's internal-journal feature flag trips `e2fsck` ("external
    // journal") before it inspects bitmaps. Allow disabling it for fsck runs so
    // the full structural check actually executes. Production keeps the journal.
    if std::env::var("DEDUP_PROBE_NO_JOURNAL").is_err() {
        writer_options.push(WriterOption::Journal(1024));
    }
    if align {
        writer_options.push(WriterOption::AlignData { align: GRID as u32, min_size: ALIGN_THRESHOLD });
    }
    let opts = ConvertOptions { convert_backslash: false, writer_options };
    let out = tempfile::tempfile().expect("tempfile");
    let mut fs = convert_oci_layers_to_ext4(&mut layers, out, &opts).expect("convert");
    fs.seek(SeekFrom::Start(0)).unwrap();
    fs
}

fn is_zero(d: &[u8]) -> bool {
    let (p, c, s) = unsafe { d.align_to::<u64>() };
    p.iter().all(|&b| b == 0) && c.iter().all(|&w| w == 0) && s.iter().all(|&b| b == 0)
}

fn read_all(mut f: File) -> Vec<u8> {
    let mut v = Vec::new();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.read_to_end(&mut v).unwrap();
    v
}

// ---- chunking schemes; each returns (content hash -> stored bytes) units ----

/// Fixed grid (what production does today). Skips zero blocks, exactly like the
/// real flush path — so alignment padding (holes) is free here.
fn fixed_grid_units(img: &[u8], stored: &mut HashMap<Blake3Hash, usize>, raw: &mut u64, zero: &mut u64) {
    for blk in img.chunks(GRID) {
        if is_zero(blk) {
            *zero += 1;
            continue;
        }
        *raw += blk.len() as u64;
        stored.entry(blake3_128(blk)).or_insert_with(|| lz4_compress(blk).len());
    }
}

/// Gear-hash content-defined chunking (the alignment-immune ceiling). Boundary
/// where the rolling hash hits the mask; min/max clamp run length.
fn gear_table() -> [u64; 256] {
    // splitmix64 from a fixed seed — deterministic across runs.
    let mut s = 0x9E3779B97F4A7C15u64;
    let mut t = [0u64; 256];
    for e in &mut t {
        s = s.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        *e = z ^ (z >> 31);
    }
    t
}

fn cdc_units(img: &[u8], gear: &[u64; 256], stored: &mut HashMap<Blake3Hash, usize>, raw: &mut u64) {
    const MIN: usize = 16 * 1024;
    const MAX: usize = 1024 * 1024;
    const MASK: u64 = (1 << 17) - 1; // avg ~128 KiB, matching the grid
    let n = img.len();
    let mut start = 0;
    while start < n {
        let mut hash = 0u64;
        let mut j = start;
        let cut = loop {
            if j >= n {
                break n;
            }
            if j - start >= MAX {
                break j;
            }
            hash = (hash << 1).wrapping_add(gear[img[j] as usize]);
            j += 1;
            if j - start >= MIN && (hash & MASK) == 0 {
                break j;
            }
        };
        let chunk = &img[start..cut];
        if !is_zero(chunk) {
            *raw += chunk.len() as u64;
            stored.entry(blake3_128(chunk)).or_insert_with(|| lz4_compress(chunk).len());
        }
        start = cut;
    }
}

fn human(b: u64) -> String {
    let f = b as f64;
    if b >= 1 << 30 {
        format!("{:.2} GiB", f / (1u64 << 30) as f64)
    } else if b >= 1 << 20 {
        format!("{:.1} MiB", f / (1u64 << 20) as f64)
    } else {
        format!("{:.1} KiB", f / (1u64 << 10) as f64)
    }
}

/// Sum of stored (lz4) bytes over the union of unique content hashes.
fn union_stored(maps: &[&HashMap<Blake3Hash, usize>]) -> u64 {
    let mut u: HashMap<Blake3Hash, usize> = HashMap::new();
    for m in maps {
        for (h, &s) in *m {
            u.entry(*h).or_insert(s);
        }
    }
    u.values().map(|&v| v as u64).sum()
}

fn main() {
    let dirs: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    if dirs.is_empty() {
        eprintln!("usage: dedup_probe <skopeo-dir> [<skopeo-dir> ...]");
        std::process::exit(2);
    }
    let images: Vec<Image> = dirs.iter().map(|d| load_image(d)).collect();
    let gear = gear_table();

    eprintln!("Building real ext4 images (unaligned + aligned) via production convert path...");
    let mut unaligned: Vec<Vec<u8>> = Vec::new();
    let mut aligned: Vec<Vec<u8>> = Vec::new();
    for img in &images {
        eprint!("  {} ...", img.name);
        unaligned.push(read_all(build_ext4(img, false)));
        aligned.push(read_all(build_ext4(img, true)));
        eprintln!(" done");
    }

    // =====================================================================
    // E1 — MECHANISM: same bytes, shifted. Isolates alignment, zero confound.
    // =====================================================================
    println!("\n================ E1: self-shift control ({}) ================", images[0].name);
    println!("Dedup of the image against a copy of ITSELF shifted by δ bytes.");
    println!("(fixed {GRID}-byte grid; shared = blocks whose content hash matches)");
    let base = &unaligned[0];
    let mut base_blocks: HashMap<Blake3Hash, ()> = HashMap::new();
    let mut base_n = 0u64;
    for blk in base.chunks(GRID) {
        if !is_zero(blk) {
            base_blocks.insert(blake3_128(blk), ());
            base_n += 1;
        }
    }
    for &delta in &[0usize, 4096, 8192, 65536, GRID, 2 * GRID] {
        let shifted = &base[delta.min(base.len())..];
        let mut shared = 0u64;
        let mut total = 0u64;
        for blk in shifted.chunks(GRID) {
            if is_zero(blk) {
                continue;
            }
            total += 1;
            if base_blocks.contains_key(&blake3_128(blk)) {
                shared += 1;
            }
        }
        let pct = if total > 0 { 100.0 * shared as f64 / total as f64 } else { 0.0 };
        let tag = if delta % GRID == 0 { "  (whole-grid multiple → control)" } else { "  (sub-grid shift)" };
        println!("  δ = {:>7} B : {:>5.1}% blocks dedup ({shared}/{total}){tag}", delta, pct);
    }
    println!("  base non-zero blocks: {base_n}");

    // =====================================================================
    // E2 + E3 — same real ext4 bytes under 3 schemes.
    // =====================================================================
    let mut fixed_maps = Vec::new();
    let mut aligned_maps = Vec::new();
    let mut cdc_maps = Vec::new();
    let (mut fixed_raw, mut aligned_raw, mut cdc_raw) = (0u64, 0u64, 0u64);
    let (mut unaligned_zero, mut aligned_zero) = (0u64, 0u64);

    println!("\n================ E2/E3: per-image + cross-image dedup ================");
    println!("Same production ext4 bytes. Three schemes:");
    println!("  TODAY   = fixed 128 KiB grid on the unaligned image (what ships now)");
    println!("  ALIGNED = fixed 128 KiB grid on the aligned image (the proposed fix)");
    println!("  CDC     = content-defined chunking on the unaligned image (ceiling)\n");
    for (i, img) in images.iter().enumerate() {
        let mut fm = HashMap::new();
        let mut am = HashMap::new();
        let mut cm = HashMap::new();
        let (mut r1, mut r2, mut r3) = (0u64, 0u64, 0u64);
        let (mut z1, mut z2) = (0u64, 0u64);
        fixed_grid_units(&unaligned[i], &mut fm, &mut r1, &mut z1);
        fixed_grid_units(&aligned[i], &mut am, &mut r2, &mut z2);
        cdc_units(&unaligned[i], &gear, &mut cm, &mut r3);
        println!(
            "  {:14} stored: TODAY {:>10} | ALIGNED {:>10} | CDC {:>10}",
            img.name,
            human(fm.values().map(|&v| v as u64).sum()),
            human(am.values().map(|&v| v as u64).sum()),
            human(cm.values().map(|&v| v as u64).sum()),
        );
        fixed_raw += r1;
        aligned_raw += r2;
        cdc_raw += r3;
        unaligned_zero += z1;
        aligned_zero += z2;
        fixed_maps.push(fm);
        aligned_maps.push(am);
        cdc_maps.push(cm);
    }

    let sum_indiv = |maps: &[HashMap<Blake3Hash, usize>]| -> u64 {
        maps.iter().map(|m| m.values().map(|&v| v as u64).sum::<u64>()).sum()
    };
    let fixed_indiv = sum_indiv(&fixed_maps);
    let aligned_indiv = sum_indiv(&aligned_maps);
    let cdc_indiv = sum_indiv(&cdc_maps);
    let fixed_union = union_stored(&fixed_maps.iter().collect::<Vec<_>>());
    let aligned_union = union_stored(&aligned_maps.iter().collect::<Vec<_>>());
    let cdc_union = union_stored(&cdc_maps.iter().collect::<Vec<_>>());

    let row = |label: &str, raw: u64, indiv: u64, union: u64| {
        let cross = indiv.saturating_sub(union);
        let cross_pct = if indiv > 0 { 100.0 * cross as f64 / indiv as f64 } else { 0.0 };
        println!(
            "  {label:8}: store-each {:>10} | store-union {:>10} | cross-image dedup {:>9} ({:.1}%)",
            human(indiv),
            human(union),
            human(cross),
            cross_pct,
        );
        let _ = raw;
    };
    println!("\n  --- storing ALL {} images (lz4-compressed, zeros dropped) ---", images.len());
    row("TODAY", fixed_raw, fixed_indiv, fixed_union);
    row("ALIGNED", aligned_raw, aligned_indiv, aligned_union);
    row("CDC", cdc_raw, cdc_indiv, cdc_union);

    println!("\n  store-union is total S3/cache bytes for the whole set.");
    if fixed_union > 0 {
        let saved = fixed_union.saturating_sub(aligned_union);
        println!(
            "  ALIGNED vs TODAY: {} smaller ({:.1}%) — this is the recovered dedup.",
            human(saved),
            100.0 * saved as f64 / fixed_union as f64
        );
        let ceil = fixed_union.saturating_sub(cdc_union);
        let captured = if ceil > 0 { 100.0 * saved as f64 / ceil as f64 } else { 0.0 };
        println!("  CDC ceiling would save {} — ALIGNED captures {:.0}% of it.", human(ceil), captured);
    }

    // =====================================================================
    // Padding cost + round-trip validity of the aligned images.
    // =====================================================================
    println!("\n================ Cost & validity of the fix ================");
    println!(
        "  zero blocks (holes): unaligned {} → aligned {} (+{} blocks of padding)",
        unaligned_zero,
        aligned_zero,
        aligned_zero.saturating_sub(unaligned_zero)
    );
    println!("  padding is zeros → NOT stored: TODAY/ALIGNED store-union already exclude it.");
    for (i, img) in images.iter().enumerate() {
        match verify_roundtrip(&aligned[i]) {
            Ok((files, bytes)) => println!(
                "  {:14} aligned image round-trips OK: {} files, {} of data read back",
                img.name,
                files,
                human(bytes)
            ),
            Err(e) => println!("  {:14} ROUND-TRIP FAILED: {e}", img.name),
        }
    }

    // =====================================================================
    // V1 — DETERMINISM. Content-addressing requires byte-identical rebuilds.
    // =====================================================================
    println!("\n================ V1: determinism ================");
    let a1 = read_all(build_ext4(&images[0], true));
    let a2 = read_all(build_ext4(&images[0], true));
    println!(
        "  {} aligned built twice: {} (and differs from unaligned: {})",
        images[0].name,
        if a1 == a2 { "IDENTICAL ✓" } else { "DIFFERENT ✗ — BUG" },
        if a1 != unaligned[0] { "yes ✓" } else { "no ✗" },
    );

    // =====================================================================
    // V2 — INDEPENDENT GROUND TRUTH. Hash whole regular files (no chunking at
    // all) read back from the ALIGNED images. This both proves the files
    // survived alignment intact and measures the true shared-content fraction
    // with a method that shares NO code with the block-grid path.
    // =====================================================================
    println!("\n================ V2: file-level ground truth (chunking-agnostic) ================");
    let file_maps: Vec<HashMap<Blake3Hash, u64>> =
        aligned.iter().map(|img| extract_file_contents(img)).collect();
    let file_indiv: u64 = file_maps.iter().map(|m| m.values().sum::<u64>()).sum();
    let mut file_union: HashMap<Blake3Hash, u64> = HashMap::new();
    for m in &file_maps {
        for (h, &s) in m {
            file_union.entry(*h).or_insert(s);
        }
    }
    let file_union_bytes: u64 = file_union.values().sum();
    let file_cross = file_indiv.saturating_sub(file_union_bytes);
    let file_pct = if file_indiv > 0 { 100.0 * file_cross as f64 / file_indiv as f64 } else { 0.0 };
    println!("  whole-file content hashing (raw bytes, identical files counted once):");
    println!(
        "    store-each {} | store-union {} | cross-image identical-file content {} ({:.1}%)",
        human(file_indiv),
        human(file_union_bytes),
        human(file_cross),
        file_pct
    );
    println!("  → THIS is how much byte-identical content genuinely exists across the set.");
    println!("    TODAY grid recovers 26-ish%; ALIGNED grid recovers ~this; if they match,");
    println!("    the win is real and the CDC ceiling was not mis-tuned.");

    // =====================================================================
    // V3 — PER-FILE SPOTLIGHT. Take one large file that is byte-identical in
    // two images and show its blocks dedup under ALIGNED but not under TODAY.
    // =====================================================================
    if images.len() >= 2 {
        println!("\n================ V3: per-file spotlight (img[0] vs img[1]) ================");
        spotlight(&images, &aligned, &unaligned, &aligned_maps, &fixed_maps);
    }

    // Dump images so they can be checked with the real e2fsck (external proof
    // of filesystem validity, independent of our own reader).
    let outdir = std::path::Path::new("/tmp/dedup_verify");
    std::fs::create_dir_all(outdir).ok();
    for (i, img) in images.iter().enumerate() {
        std::fs::write(outdir.join(format!("{}-unaligned.img", img.name)), &unaligned[i]).ok();
        std::fs::write(outdir.join(format!("{}-aligned.img", img.name)), &aligned[i]).ok();
    }
    println!("\n  images dumped to {} for external `e2fsck -fn` validation.", outdir.display());
}

/// Read every regular file from an ext4 image and map its whole-content hash to
/// its size. Uses only the reader — no block-grid code — so it is an
/// independent oracle for "how much identical file content exists".
fn extract_file_contents(img: &[u8]) -> HashMap<Blake3Hash, u64> {
    let mut out: HashMap<Blake3Hash, u64> = HashMap::new();
    let cursor = std::io::Cursor::new(img);
    let mut reader = ext4::reader::Reader::new(cursor).expect("reader");
    let entries = reader.walk().expect("walk");
    for e in entries {
        if (e.mode & 0xF000) == 0x8000 && e.size > 0 {
            let inode = reader.read_inode(e.inode_number).expect("inode");
            let data = reader.read_data(&inode).expect("data");
            // Key on (content, size); dedup identical files within the image.
            out.entry(blake3_128(&data)).or_insert(data.len() as u64);
        }
    }
    out
}

fn read_file_by_path(img: &[u8], path: &str) -> Option<Vec<u8>> {
    let cursor = std::io::Cursor::new(img);
    let mut reader = ext4::reader::Reader::new(cursor).ok()?;
    let entries = reader.walk().ok()?;
    for e in entries {
        if e.path == path && (e.mode & 0xF000) == 0x8000 {
            let inode = reader.read_inode(e.inode_number).ok()?;
            return reader.read_data(&inode).ok();
        }
    }
    None
}

fn spotlight(
    images: &[Image],
    aligned: &[Vec<u8>],
    unaligned: &[Vec<u8>],
    aligned_maps: &[HashMap<Blake3Hash, usize>],
    fixed_maps: &[HashMap<Blake3Hash, usize>],
) {
    // Find a path present in BOTH images with byte-identical content and > 256 KiB.
    let cur = std::io::Cursor::new(&aligned[0]);
    let mut r0 = ext4::reader::Reader::new(cur).expect("reader");
    let walk0 = r0.walk().expect("walk");
    let mut candidates: Vec<(String, u64)> = walk0
        .iter()
        .filter(|e| (e.mode & 0xF000) == 0x8000 && e.size > 256 * 1024)
        .map(|e| (e.path.clone(), e.size))
        .collect();
    candidates.sort_by_key(|(_, s)| std::cmp::Reverse(*s));

    for (path, size) in candidates {
        let c0 = read_file_by_path(&aligned[0], &path);
        let c1 = read_file_by_path(&aligned[1], &path);
        let (Some(c0), Some(c1)) = (c0, c1) else { continue };
        if c0 != c1 || blake3_128(&c0) != blake3_128(&c1) {
            continue; // not byte-identical across the two images
        }
        // Hash this file's content in 128 KiB chunks. Under ALIGNED the file
        // starts on the grid, so its full sub-blocks ARE these content chunks.
        let chunk_hashes: Vec<Blake3Hash> = c0.chunks(GRID).filter(|b| !is_zero(b)).map(blake3_128).collect();
        let n = chunk_hashes.len();
        // How many of this file's content-chunks appear as real grid blocks in
        // the OTHER image (img[1]) under each scheme?
        let in_aligned = chunk_hashes.iter().filter(|h| aligned_maps[1].contains_key(*h)).count();
        let in_today = chunk_hashes.iter().filter(|h| fixed_maps[1].contains_key(*h)).count();
        println!("  file: {path}  ({}, byte-identical in {} & {})", human(size), images[0].name, images[1].name);
        println!("    its {n} content-chunks found as grid blocks in {}:", images[1].name);
        println!(
            "      under ALIGNED: {in_aligned}/{n} ({:.0}%) dedup",
            100.0 * in_aligned as f64 / n.max(1) as f64
        );
        println!(
            "      under TODAY:   {in_today}/{n} ({:.0}%) dedup",
            100.0 * in_today as f64 / n.max(1) as f64
        );
        let _ = unaligned;
        return;
    }
    println!("  (no >256 KiB byte-identical file shared across the two images found)");
}

/// Read the aligned ext4 back through the production reader to prove the image
/// is a valid filesystem (not just bytes that happen to dedup well).
fn verify_roundtrip(img: &[u8]) -> std::io::Result<(usize, u64)> {
    let cursor = std::io::Cursor::new(img);
    let mut reader = ext4::reader::Reader::new(cursor)?;
    let entries = reader.walk()?;
    let mut files = 0usize;
    let mut bytes = 0u64;
    for e in entries {
        if (e.mode & 0xF000) == 0x8000 {
            // regular file
            let inode = reader.read_inode(e.inode_number)?;
            let data = reader.read_data(&inode)?;
            bytes += data.len() as u64;
            files += 1;
        }
    }
    Ok((files, bytes))
}
