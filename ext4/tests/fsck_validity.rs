//! Filesystem-validity harness gated on the REAL `e2fsck`, not the in-crate
//! reader. The in-crate reader is lenient and hid a real multi-block-group
//! corruption bug for a long time; `e2fsck` (kernel-grade structural check)
//! catches it. Every writer change should be validated here.
//!
//! These tests shell out to `e2fsck`; they skip (pass with a notice) when it is
//! not installed so they do not break environments without e2fsprogs.
//!
//! Run with: `cargo test -p ext4 --test fsck_validity -- --include-ignored`

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
    let Some(e2fsck) = find_e2fsck() else {
        eprintln!("SKIP: e2fsck not installed");
        return Ok(());
    };

    // 1. Synthesize a tar stream.
    let mut tar = tar::Builder::new(Vec::new());
    for (i, (path, size)) in files.iter().enumerate() {
        let data = content(i as u64, *size);
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_mtime(0);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        tar.append_data(&mut h, path, &data[..]).map_err(|e| format!("tar append: {e}"))?;
    }
    let tar_bytes = tar.into_inner().map_err(|e| format!("tar finish: {e}"))?;

    // 2. Convert to a real ext4 image (same code bless uses).
    let mut writer_options = vec![
        WriterOption::MaximumDiskSize(2 * 1024 * 1024 * 1024),
        WriterOption::Uuid([0x11; 16]),
    ];
    if let Some((a, m)) = align {
        writer_options.push(WriterOption::AlignData { align: a, min_size: m });
    }
    let opts = ConvertOptions { convert_backslash: false, writer_options };

    let tmp = tempfile::NamedTempFile::new().map_err(|e| format!("tmp: {e}"))?;
    let out = tmp.reopen().map_err(|e| format!("reopen: {e}"))?;
    convert_tar_to_ext4(std::io::Cursor::new(tar_bytes), out, &opts)
        .map_err(|e| format!("convert: {e}"))?;
    tmp.as_file().sync_all().ok();

    // 3. The oracle: e2fsck -fn. Exit 0 == clean.
    let output = Command::new(&e2fsck)
        .args(["-fn"])
        .arg(tmp.path())
        .output()
        .map_err(|e| format!("spawn e2fsck: {e}"))?;
    let code = output.status.code().unwrap_or(-1);
    if code == 0 {
        Ok(())
    } else {
        let mut report = format!("e2fsck exit={code} (nonzero = filesystem errors)\n");
        report.push_str(&String::from_utf8_lossy(&output.stdout));
        // Trim the giant bitmap-difference dumps to keep failures readable.
        let trimmed: String = report.lines().take(40).collect::<Vec<_>>().join("\n");
        Err(trimmed)
    }
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
/// e2fsck-clean. It currently is NOT: the writer's linear allocator places file
/// data on the Group 1 backup superblock / group descriptors (block 32768+),
/// producing multiply-claimed blocks the kernel rejects. Un-ignore once the
/// allocator reserves group metadata.
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
#[ignore = "KNOWN BUG: alignment padding marked used + lands on group metadata; needs metadata-aware align"]
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
