#![allow(clippy::cast_precision_loss)]
//! Measure per-block compression: LZ4 (production) vs zstd at several levels, on
//! real ext4 images. Production compresses each 128 KiB block *independently*
//! (see block_map::lz4_compress at flush), so we do the same here — whole-stream
//! compression would overstate the ratio. Zero blocks are skipped (never stored).
//!
//! Reports stored bytes per scheme = the S3 pack payload size (and thus the
//! storage + egress footprint) you'd get by switching the block compressor.
//!
//! Usage: cargo run --release --bin compress_probe -- img1.ext4 [img2 ...]

use std::fs::File;
use std::io::Read;
use std::time::Instant;

use glidefs::block::block_map::{lz4_compress, lz4_decompress};

const BLOCK: usize = 128 * 1024;
const ZSTD_LEVELS: [i32; 4] = [1, 3, 9, 19];

fn is_zero(d: &[u8]) -> bool {
    let (p, c, s) = unsafe { d.align_to::<u64>() };
    p.iter().all(|&b| b == 0) && c.iter().all(|&w| w == 0) && s.iter().all(|&b| b == 0)
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

#[derive(Default)]
struct Totals {
    nonzero_raw: u64,
    nonzero_blocks: u64,
    lz4: u64,
    zstd: [u64; 4],
    lz4_decode_ns: u128,
    zstd_decode_ns: u128, // decoding the zstd-3 form (representative; ~level-independent)
}

fn main() {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    if paths.is_empty() {
        eprintln!("usage: compress_probe <ext4-image> [...]");
        std::process::exit(2);
    }

    let mut grand = Totals::default();
    println!("Per-128KiB-block compression (zeros skipped):\n");

    for path in &paths {
        let mut f = File::open(path).unwrap_or_else(|e| panic!("open {path}: {e}"));
        let mut buf = vec![0u8; BLOCK];
        let mut t = Totals::default();
        loop {
            let mut filled = 0;
            while filled < BLOCK {
                match f.read(&mut buf[filled..]) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(e) => panic!("read {path}: {e}"),
                }
            }
            if filled == 0 {
                break;
            }
            let block = &buf[..BLOCK];
            if is_zero(block) {
                continue;
            }
            t.nonzero_blocks += 1;
            t.nonzero_raw += BLOCK as u64;
            let lz4c = lz4_compress(block);
            t.lz4 += lz4c.len() as u64;
            // Time LZ4 decode (read path on cache miss).
            let d0 = Instant::now();
            let _ = lz4_decompress(&lz4c).expect("lz4 decode");
            t.lz4_decode_ns += d0.elapsed().as_nanos();
            for (i, lvl) in ZSTD_LEVELS.iter().enumerate() {
                let zc = zstd::bulk::compress(block, *lvl).expect("zstd");
                t.zstd[i] += zc.len() as u64;
                if *lvl == 3 {
                    let d1 = Instant::now();
                    let _ = zstd::bulk::decompress(&zc, BLOCK).expect("zstd decode");
                    t.zstd_decode_ns += d1.elapsed().as_nanos();
                }
            }
        }

        let name = path.rsplit('/').next().unwrap_or(path);
        println!("{name}  ({} non-zero blocks, {} raw)", t.nonzero_blocks, human(t.nonzero_raw));
        println!("  LZ4 (today): {:>10}   ratio {:.2}x", human(t.lz4), t.nonzero_raw as f64 / t.lz4 as f64);
        for (i, lvl) in ZSTD_LEVELS.iter().enumerate() {
            let z = t.zstd[i];
            println!(
                "  zstd-{:<2}:      {:>10}   ratio {:.2}x   vs LZ4: {:.1}% smaller",
                lvl,
                human(z),
                t.nonzero_raw as f64 / z as f64,
                100.0 * (t.lz4 as f64 - z as f64) / t.lz4 as f64
            );
        }
        let n = t.nonzero_blocks.max(1) as f64;
        println!(
            "  decode/block: LZ4 {:.1} µs, zstd-3 {:.1} µs  (a 128KiB block; vs ~10–100ms per S3 GET → decode is ~0.1% of read latency)",
            t.lz4_decode_ns as f64 / 1000.0 / n,
            t.zstd_decode_ns as f64 / 1000.0 / n,
        );
        println!();

        grand.nonzero_raw += t.nonzero_raw;
        grand.nonzero_blocks += t.nonzero_blocks;
        grand.lz4 += t.lz4;
        grand.lz4_decode_ns += t.lz4_decode_ns;
        grand.zstd_decode_ns += t.zstd_decode_ns;
        for i in 0..4 {
            grand.zstd[i] += t.zstd[i];
        }
    }

    if paths.len() > 1 {
        println!("==== TOTAL across {} images ({} raw) ====", paths.len(), human(grand.nonzero_raw));
        println!("  LZ4 (today): {:>10}   ratio {:.2}x", human(grand.lz4), grand.nonzero_raw as f64 / grand.lz4 as f64);
        for (i, lvl) in ZSTD_LEVELS.iter().enumerate() {
            let z = grand.zstd[i];
            println!(
                "  zstd-{:<2}:      {:>10}   ratio {:.2}x   vs LZ4: {:.1}% smaller",
                lvl,
                human(z),
                grand.nonzero_raw as f64 / z as f64,
                100.0 * (grand.lz4 as f64 - z as f64) / grand.lz4 as f64
            );
        }
    }
}
