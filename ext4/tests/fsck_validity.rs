//! Filesystem-validity harness gated on the REAL `e2fsck`, not the in-crate
//! reader. The in-crate reader is lenient and hid a real multi-block-group
//! corruption bug for a long time; `e2fsck` (kernel-grade structural check)
//! catches it. Every writer change should be validated here.
//!
//! These tests shell out to `e2fsck`; they skip (pass with a notice) when it is
//! not installed so they do not break environments without e2fsprogs.
//!
//! Run with: `cargo test -p ext4 --test fsck_validity -- --include-ignored`

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use ext4::tar_convert::{ConvertOptions, convert_tar_to_ext4};
use ext4::writer::WriterOption;

/// Locate `e2fsck` (often in /sbin, not on a service PATH). Returns None to skip.
fn find_e2fsck() -> Option<PathBuf> {
    for p in ["/sbin/e2fsck", "/usr/sbin/e2fsck", "/usr/bin/e2fsck", "/bin/e2fsck"] {
        if std::path::Path::new(p).exists() {
            return Some(PathBuf::from(p));
        }
    }
    None
}

/// Deterministic, non-trivial file content (so blocks are actually allocated and
/// the layout is reproducible — no RNG).
fn content(seed: u64, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    let mut s = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    while v.len() < len {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        v.extend_from_slice(&s.to_le_bytes());
    }
    v.truncate(len);
    v
}

/// Build an in-memory tar of `(path, size)` files, convert to ext4 via the real
/// production path, write it to a temp file, and run `e2fsck -fn` on it.
/// Returns Ok(()) if e2fsck reports a clean filesystem (exit 0), else Err(report).
fn build_and_fsck(files: &[(&str, usize)], align: Option<(u32, u32)>) -> Result<(), String> {
    let owned: Vec<(String, usize)> = files.iter().map(|(p, s)| ((*p).to_string(), *s)).collect();
    e2fsck_clean(&build_image(&owned, align))
}

/// Build a real ext4 image (production convert path) from `(path, size)` files,
/// each filled with the deterministic `content(index, size)`.
fn build_image(files: &[(String, usize)], align: Option<(u32, u32)>) -> Vec<u8> {
    let mut tar = tar::Builder::new(Vec::new());
    for (i, (path, size)) in files.iter().enumerate() {
        let data = content(i as u64, *size);
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_mtime(0);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        tar.append_data(&mut h, path, &data[..]).unwrap();
    }
    let tar_bytes = tar.into_inner().unwrap();

    let mut writer_options = vec![
        WriterOption::MaximumDiskSize(4 * 1024 * 1024 * 1024),
        WriterOption::Uuid([0x11; 16]),
    ];
    if let Some((a, m)) = align {
        writer_options.push(WriterOption::AlignData { align: a, min_size: m });
    }
    let opts = ConvertOptions { convert_backslash: false, writer_options };
    let mut img: Vec<u8> = Vec::new();
    convert_tar_to_ext4(std::io::Cursor::new(tar_bytes), std::io::Cursor::new(&mut img), &opts)
        .unwrap();
    img
}

/// The oracle: run `e2fsck -fn` on the image. Ok == clean (exit 0). Skips
/// (returns Ok) when e2fsck is not installed.
fn e2fsck_clean(img: &[u8]) -> Result<(), String> {
    let Some(e2fsck) = find_e2fsck() else {
        eprintln!("SKIP: e2fsck not installed");
        return Ok(());
    };
    let mut tmp = tempfile::NamedTempFile::new().map_err(|e| format!("tmp: {e}"))?;
    tmp.write_all(img).map_err(|e| format!("write: {e}"))?;
    tmp.flush().ok();
    let output = Command::new(&e2fsck)
        .args(["-fn"])
        .arg(tmp.path())
        .output()
        .map_err(|e| format!("spawn e2fsck: {e}"))?;
    if output.status.code() == Some(0) {
        Ok(())
    } else {
        let mut report = format!("e2fsck exit={:?} (nonzero = filesystem errors)\n", output.status.code());
        report.push_str(&String::from_utf8_lossy(&output.stdout));
        // Trim the giant bitmap-difference dumps to keep failures readable.
        Err(report.lines().take(30).collect::<Vec<_>>().join("\n"))
    }
}

/// Read every file back via the reader (which assembles from the on-disk extent
/// tree) and assert byte-exact equality with the known input — catches any
/// logical-ordering bug introduced by fragmentation around reserved blocks.
fn content_matches(img: &[u8], files: &[(String, usize)]) -> Result<(), String> {
    let mut want: std::collections::HashMap<String, (u64, usize)> = std::collections::HashMap::new();
    for (i, (p, s)) in files.iter().enumerate() {
        want.insert(p.trim_start_matches('/').to_string(), (i as u64, *s));
    }
    let mut reader =
        ext4::reader::Reader::new(std::io::Cursor::new(img)).map_err(|e| format!("reader: {e}"))?;
    let entries = reader.walk().map_err(|e| format!("walk: {e}"))?;
    let mut checked = 0;
    for e in entries {
        if (e.mode & 0xF000) != 0x8000 {
            continue;
        }
        let path = e.path.trim_start_matches('/').to_string();
        let Some(&(idx, size)) = want.get(&path) else { continue };
        let inode = reader.read_inode(e.inode_number).map_err(|e| format!("{path}: inode: {e}"))?;
        let got = reader.read_data(&inode).map_err(|e| format!("{path}: read: {e}"))?;
        if got.len() != size {
            return Err(format!("{path}: size {} != {size}", got.len()));
        }
        if got != content(idx, size) {
            return Err(format!("{path}: CONTENT MISMATCH (fragmentation reordered bytes)"));
        }
        checked += 1;
    }
    if checked != files.len() {
        return Err(format!("read back {checked}/{} files", files.len()));
    }
    Ok(())
}

/// Baseline: a single-block-group filesystem (< 128 MiB) must be e2fsck-clean.
/// This proves the harness works and the writer is sound when it doesn't cross
/// a block-group boundary.
#[test]
fn fsck_single_group_clean() {
    let files = &[
        ("etc/hostname", 12),
        ("etc/config.toml", 4096),
        ("usr/bin/tool", 8 * 1024 * 1024),
        ("usr/lib/data.bin", 32 * 1024 * 1024),
        ("var/log/app.log", 1024),
    ];
    if let Err(report) = build_and_fsck(files, None) {
        panic!("single-group image is not e2fsck-clean:\n{report}");
    }
}

/// A filesystem that crosses a block-group boundary (> 128 MiB) must be
/// e2fsck-clean. Regression for the original corruption: the linear allocator
/// used to place file data on the Group 1 backup superblock / group descriptors
/// (block 32768+), producing multiply-claimed blocks. The group-aware allocator
/// now skips those reserved blocks and fragments files around them.
#[test]
fn fsck_multi_group_clean() {
    // ~160 MiB of file data guarantees crossing into block group 1 (32768 blocks
    // == 128 MiB), regardless of group-0 metadata overhead.
    let files = &[
        ("data/a.bin", 40 * 1024 * 1024),
        ("data/b.bin", 40 * 1024 * 1024),
        ("data/c.bin", 40 * 1024 * 1024),
        ("data/d.bin", 40 * 1024 * 1024),
    ];
    if let Err(report) = build_and_fsck(files, None) {
        panic!("multi-group image is not e2fsck-clean:\n{report}");
    }
}

/// Content correctness across fragmentation. e2fsck validates *structure*, not
/// that a file's logical blocks are in the right order. A file that spans a
/// reserved metadata region is split into multiple extents by the allocator; if
/// the logical offsets were assigned wrong, the bytes would come back reordered.
/// Build a multi-group image with KNOWN deterministic content, read every file
/// back through the reader (which assembles by the on-disk extent tree,
/// independent of the writer's allocation state), and assert byte-exact equality.
#[test]
fn content_survives_fragmentation() {
    // Files sized so several cross the block-group boundary (block 32768).
    let specs: &[(&str, usize)] = &[
        ("data/a.bin", 50 * 1024 * 1024),
        ("data/b.bin", 50 * 1024 * 1024),
        ("data/c.bin", 50 * 1024 * 1024),
        ("small/x", 1234),
        ("data/d.bin", 30 * 1024 * 1024),
    ];

    // Build the tar.
    let mut tar = tar::Builder::new(Vec::new());
    let mut expected: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    for (i, (path, size)) in specs.iter().enumerate() {
        let data = content(i as u64, *size);
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_mtime(0);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        tar.append_data(&mut h, path, &data[..]).unwrap();
        expected.insert((*path).to_string(), data);
    }
    let tar_bytes = tar.into_inner().unwrap();

    // Convert to ext4 (multi-group, group-aware allocator).
    let opts = ConvertOptions {
        convert_backslash: false,
        writer_options: vec![
            WriterOption::MaximumDiskSize(2 * 1024 * 1024 * 1024),
            WriterOption::Uuid([0x22; 16]),
        ],
    };
    let mut img: Vec<u8> = Vec::new();
    convert_tar_to_ext4(std::io::Cursor::new(tar_bytes), std::io::Cursor::new(&mut img), &opts).unwrap();

    // Read every file back via the reader and compare to known input.
    let mut reader = ext4::reader::Reader::new(std::io::Cursor::new(&img)).unwrap();
    let entries = reader.walk().unwrap();
    let mut checked = 0;
    for e in entries {
        if (e.mode & 0xF000) != 0x8000 {
            continue;
        }
        let path = e.path.trim_start_matches('/').to_string();
        let Some(want) = expected.get(&path) else { continue };
        let inode = reader.read_inode(e.inode_number).unwrap();
        let got = reader.read_data(&inode).unwrap();
        assert_eq!(got.len(), want.len(), "{path}: size mismatch");
        assert!(got == *want, "{path}: CONTENT MISMATCH (fragmentation reordered bytes)");
        checked += 1;
    }
    assert_eq!(checked, specs.len(), "did not read back all files");
}

/// Once the allocator is group-aware, the aligned multi-group build must ALSO be
/// e2fsck-clean (alignment padding must be marked free, and aligned file starts
/// must skip reserved blocks). Captures both the metadata-collision and the
/// padding-bitmap issues found via e2fsck.
#[test]
fn fsck_multi_group_aligned_clean() {
    let files = &[
        ("data/a.bin", 40 * 1024 * 1024),
        ("data/b.bin", 40 * 1024 * 1024),
        ("data/c.bin", 40 * 1024 * 1024),
        ("data/d.bin", 40 * 1024 * 1024),
    ];
    if let Err(report) = build_and_fsck(files, Some((128 * 1024, 16 * 1024))) {
        panic!("aligned multi-group image is not e2fsck-clean:\n{report}");
    }
}

/// Property fuzzer: random multi-group filesets must be e2fsck-clean AND read
/// back byte-exact — both unaligned and aligned. Seeds are deterministic so any
/// failure reproduces verbatim (the panic prints the seed and fileset). This is
/// the generalized gate: it sweeps the size/position space where the original
/// data-on-backup-superblock and alignment-bitmap bugs lived. Crank coverage
/// with `EXT4_FUZZ_SEEDS=64 cargo test -p ext4 --test fsck_validity fuzz`.
#[test]
fn fuzz_multigroup_validity_and_content() {
    if find_e2fsck().is_none() {
        eprintln!("SKIP: e2fsck not installed");
        return;
    }
    let seeds: u64 = std::env::var("EXT4_FUZZ_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    for seed in 0..seeds {
        let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        // Random fileset with a deliberate mix: small files (below the align
        // threshold), medium, and large (which straddle group boundaries).
        let nfiles = 4 + (next() % 10) as usize;
        let mut files: Vec<(String, usize)> = Vec::new();
        let mut total: u64 = 0;
        for k in 0..nfiles {
            let size = match next() % 10 {
                0..=3 => 1 + (next() % (64 * 1024)) as usize,
                4..=6 => 4096 + (next() % (8 * 1024 * 1024)) as usize,
                _ => 8 * 1024 * 1024 + (next() % (40 * 1024 * 1024)) as usize,
            };
            files.push((format!("d/s{seed}_f{k}.bin"), size));
            total += size as u64;
        }
        // Guarantee at least one block-group boundary (128 MiB) is crossed so the
        // reserved-block / fragmentation paths are always exercised.
        if total < 160 * 1024 * 1024 {
            let pad = (160 * 1024 * 1024 - total) as usize + 4 * 1024 * 1024;
            files.push((format!("d/s{seed}_big.bin"), pad));
        }

        for align in [None, Some((128 * 1024u32, 16 * 1024u32))] {
            let img = build_image(&files, align);
            if let Err(e) = e2fsck_clean(&img) {
                panic!("seed={seed} align={align:?} NOT e2fsck-clean:\n{e}\nfiles={files:?}");
            }
            if let Err(e) = content_matches(&img, &files) {
                panic!("seed={seed} align={align:?} content error: {e}\nfiles={files:?}");
            }
        }
    }
}
