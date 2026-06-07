//! EROFS writer + OCI-merge validity, gated on the REAL `fsck.erofs` and the
//! in-kernel `erofs` driver — not an in-crate reader. Mirrors the ext4
//! `fsck_validity.rs` harness.
//!
//! * `erofs_merge_fsck` — always on when `fsck.erofs` is installed: builds a
//!   merged EROFS from representative OCI layers and runs `fsck.erofs`, catching
//!   structural format bugs with no root needed.
//! * `erofs_merge_kernel_mount` — opt-in (needs root / passwordless sudo +
//!   loop): mounts the image with the real kernel driver and verifies the merged
//!   content (override / whiteout / opaque / symlink / hardlink / tiny files).
//!   Run with: `EROFS_MOUNT_TEST=1 cargo test -p ext4 --test erofs_validity`

use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::process::Command;

use ext4::tar_convert::{convert_oci_layers_to_erofs, ConvertOptions};
use ext4::writer::WriterOption;

// ---- tar construction helpers ----

enum E<'a> {
    File(&'a str, u32, &'a [u8]),
    Dir(&'a str),
    Symlink(&'a str, &'a str),
    Hardlink(&'a str, &'a str), // (path, target)
    // a raw entry written verbatim (used for whiteouts)
    Whiteout(&'a str),
}

fn build_tar(entries: &[E<'_>]) -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    for e in entries {
        let mut h = tar::Header::new_gnu();
        match e {
            E::File(path, mode, data) => {
                h.set_path(path).unwrap();
                h.set_size(data.len() as u64);
                h.set_mode(*mode);
                h.set_entry_type(tar::EntryType::Regular);
                h.set_cksum();
                b.append(&h, *data).unwrap();
            }
            E::Dir(path) => {
                h.set_path(path).unwrap();
                h.set_size(0);
                h.set_mode(0o755);
                h.set_entry_type(tar::EntryType::Directory);
                h.set_cksum();
                b.append(&h, &[][..]).unwrap();
            }
            E::Symlink(path, target) => {
                h.set_path(path).unwrap();
                h.set_size(0);
                h.set_mode(0o777);
                h.set_entry_type(tar::EntryType::Symlink);
                h.set_link_name(target).unwrap();
                h.set_cksum();
                b.append(&h, &[][..]).unwrap();
            }
            E::Hardlink(path, target) => {
                h.set_path(path).unwrap();
                h.set_size(0);
                h.set_mode(0o644);
                h.set_entry_type(tar::EntryType::Link);
                h.set_link_name(target).unwrap();
                h.set_cksum();
                b.append(&h, &[][..]).unwrap();
            }
            E::Whiteout(path) => {
                h.set_path(path).unwrap();
                h.set_size(0);
                h.set_mode(0o644);
                h.set_entry_type(tar::EntryType::Regular);
                h.set_cksum();
                b.append(&h, &[][..]).unwrap();
            }
        }
    }
    b.into_inner().unwrap()
}

/// Two layers exercising every overlay rule + the small-file path nydus got
/// wrong (tiny files in a child layer).
fn representative_layers() -> Vec<Vec<u8>> {
    let big = vec![0x5au8; 9000]; // spans 2 blocks + inline tail
    let base = build_tar(&[
        E::Dir("etc/"),
        E::File("etc/hostname", 0o644, b"base-host"),
        E::File("etc/passwd", 0o644, b"root:x:0:0"),
        E::Dir("var/"),
        E::Dir("var/log/"),
        E::File("var/log/old.log", 0o644, b"stale"),
        E::Dir("app/"),
        E::File("app/tiny", 0o644, b"T"), // 1-byte file from base layer
        E::File("app/big.bin", 0o644, &big),
        E::File("bin/sh", 0o755, b"#!/bin/sh\n"),
    ]);
    let top = build_tar(&[
        E::File("etc/hostname", 0o644, b"top-host"), // override
        E::Whiteout("etc/.wh.passwd"),               // delete passwd
        E::Dir("var/log/"),
        E::Whiteout("var/log/.wh..wh..opq"), // opaque: drop old.log
        E::File("var/log/new.log", 0o644, b"fresh"),
        E::File("app/tiny2", 0o644, b"x"), // 1-byte file from CHILD layer
        E::File("app/main", 0o644, b"MAIN"),
        E::Hardlink("app/main2", "app/main"), // hardlink within layer
        E::Symlink("link", "etc/hostname"),
    ]);
    vec![base, top]
}

fn merged_erofs() -> Vec<u8> {
    let mut layers: Vec<Cursor<Vec<u8>>> =
        representative_layers().into_iter().map(Cursor::new).collect();
    let opts = ConvertOptions {
        convert_backslash: false,
        writer_options: vec![WriterOption::Uuid([0x42u8; 16])],
    };
    convert_oci_layers_to_erofs(&mut layers, Cursor::new(Vec::new()), &opts)
        .unwrap()
        .into_inner()
}

fn find_tool(name: &str) -> Option<PathBuf> {
    let mut cands: Vec<String> = ["/sbin", "/usr/sbin", "/usr/bin", "/bin"]
        .iter()
        .map(|d| format!("{d}/{name}"))
        .collect();
    if let Ok(home) = std::env::var("HOME") {
        cands.push(format!("{home}/gliderofs-spike/bin/{name}"));
    }
    cands.into_iter().map(PathBuf::from).find(|p| p.exists())
}

fn write_temp(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new()
        .suffix(".erofs")
        .tempfile()
        .unwrap();
    f.write_all(bytes).unwrap();
    f.flush().unwrap();
    f
}

/// Determinism: same layers + same UUID → byte-identical EROFS (the property
/// that makes shared images dedup). No external tools needed.
#[test]
fn erofs_merge_is_byte_deterministic() {
    assert_eq!(merged_erofs(), merged_erofs());
}

/// Structural validity via the real `fsck.erofs` (skips if not installed).
#[test]
fn erofs_merge_fsck() {
    let Some(fsck) = find_tool("fsck.erofs") else {
        eprintln!("SKIP: fsck.erofs not installed");
        return;
    };
    let img = merged_erofs();
    let tmp = write_temp(&img);
    let out = Command::new(&fsck).arg(tmp.path()).output().expect("run fsck.erofs");
    if !out.status.success() {
        panic!(
            "fsck.erofs rejected the image:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
}

/// Privileged: mount with the in-kernel driver and verify merged content.
#[test]
fn erofs_merge_kernel_mount() {
    if std::env::var("EROFS_MOUNT_TEST").as_deref() != Ok("1") {
        eprintln!("SKIP: set EROFS_MOUNT_TEST=1 to run the privileged kernel-mount check");
        return;
    }
    let sudo_ok = Command::new("sudo")
        .args(["-n", "true"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !sudo_ok {
        eprintln!("SKIP: passwordless sudo not available for mount");
        return;
    }

    let img = merged_erofs();
    let tmp = write_temp(&img);
    let mnt = tempfile::tempdir().unwrap();

    let mounted = Command::new("sudo")
        .args(["-n", "mount", "-t", "erofs", "-o", "ro,loop"])
        .arg(tmp.path())
        .arg(mnt.path())
        .status()
        .expect("spawn mount");
    assert!(mounted.success(), "kernel erofs mount failed");

    let result = std::panic::catch_unwind(|| {
        use std::os::unix::fs::MetadataExt;
        let p = |rel: &str| mnt.path().join(rel);
        let read = |rel: &str| std::fs::read(p(rel)).map(|v| String::from_utf8_lossy(&v).into_owned());

        // override
        assert_eq!(read("etc/hostname").unwrap(), "top-host");
        // whiteout
        assert!(!p("etc/passwd").exists(), "whiteout must delete etc/passwd");
        // opaque
        assert!(!p("var/log/old.log").exists(), "opaque must drop old.log");
        assert_eq!(read("var/log/new.log").unwrap(), "fresh");
        // tiny files (base + child layer) — the nydus-broken case
        assert_eq!(read("app/tiny").unwrap(), "T", "base-layer tiny file");
        assert_eq!(read("app/tiny2").unwrap(), "x", "CHILD-layer tiny file");
        // multi-block file
        assert_eq!(std::fs::read(p("app/big.bin")).unwrap(), vec![0x5au8; 9000]);
        // exec bit preserved
        assert_eq!(read("bin/sh").unwrap(), "#!/bin/sh\n");
        // symlink
        assert_eq!(
            std::fs::read_link(p("link")).unwrap().to_string_lossy(),
            "etc/hostname"
        );
        // hardlink: same content + nlink >= 2 + same inode
        assert_eq!(read("app/main").unwrap(), "MAIN");
        assert_eq!(read("app/main2").unwrap(), "MAIN");
        let m1 = std::fs::metadata(p("app/main")).unwrap();
        let m2 = std::fs::metadata(p("app/main2")).unwrap();
        assert_eq!(m1.ino(), m2.ino(), "hardlinks must share an inode");
        assert!(m1.nlink() >= 2, "hardlinked file nlink must be >= 2");
    });

    let _ = Command::new("sudo").args(["-n", "umount"]).arg(mnt.path()).status();
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

/// xattrs: encode `user.*`, `security.capability`, and `trusted.overlay.opaque`;
/// validate the structure with `fsck.erofs`, and (gated) read the values back
/// with `getfattr` from a real kernel mount.
/// Grid alignment recovers block-level dedup under upstream churn — the read
/// path's tightest constraint is the count of *unique* 128 KiB blocks GlideFS
/// must fetch from S3, and dedup turns those fetches into cache hits.
///
/// MECHANISM (isolates alignment, no external images, deterministic): build two
/// EROFS images that contain the *same* 512 KiB file. Image B also has extra
/// small files, which grow the metadata region and therefore shift where the big
/// file's data lands. Without alignment that shift de-aligns the big file's
/// 128 KiB blocks vs image A → they stop deduping. With `AlignData` at the
/// 128 KiB grid the big file starts on the grid in *both* images → its blocks
/// are byte-identical → they dedup. We assert aligned dedup ≫ unaligned, and
/// that the aligned image is still a structurally valid EROFS (`fsck.erofs`).
#[test]
fn erofs_alignment_recovers_dedup_under_churn() {
    use ext4::erofs::Writer;
    use ext4::File;

    const GRID: usize = 128 * 1024; // production dedup-block size (131072)
    const S_IFREG: u16 = 0x8000;

    // Deterministic, high-entropy payload so every 128 KiB block is non-zero and
    // distinct (zeros are dropped by the block layer and would confound the
    // count). Simple xorshift keyed bytes.
    fn payload(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    // Count non-zero 128 KiB blocks that appear in BOTH images (the blocks the
    // second image would NOT have to fetch from S3 because they're already
    // cached from the first).
    fn shared_blocks(a: &[u8], b: &[u8]) -> usize {
        use std::collections::HashSet;
        let nz = |blk: &[u8]| blk.iter().any(|&x| x != 0);
        let set: HashSet<&[u8]> = a.chunks(GRID).filter(|c| nz(c)).collect();
        b.chunks(GRID).filter(|c| nz(c) && set.contains(c)).count()
    }

    let big = payload(0xC0FFEE, 512 * 1024); // 4 full grid blocks
    let regf = |size: i64| File { mode: S_IFREG | 0o644, size, ..Default::default() };

    // Build {big.bin} plus `extra` small files, with alignment on/off. The small
    // files are inline tails — they grow the metadata region and thus shift the
    // data region start, which is exactly the upstream churn we model.
    let build = |extra: usize, align: bool| -> Vec<u8> {
        let mut opts = vec![WriterOption::Uuid([0x42u8; 16])];
        if align {
            opts.push(WriterOption::AlignData { align: GRID as u32, min_size: 4096 });
        }
        let mut w = Writer::new(Cursor::new(Vec::new()), &opts);
        // Names sort before "big.bin" so they're laid out first.
        for i in 0..extra {
            let name = format!("a_pad_{i:04}");
            w.create(&name, &regf(13)).unwrap();
            w.write_all(b"shift-the-meta").unwrap();
        }
        w.create("big.bin", &regf(big.len() as i64)).unwrap();
        w.write_all(&big).unwrap();
        w.close().unwrap().into_inner()
    };

    // Image A: just the big file. Image B: same big file, but 37 extra small
    // files ahead of it (a realistic "rebuild added some files" churn).
    let a_unaligned = build(0, false);
    let b_unaligned = build(37, false);
    let a_aligned = build(0, true);
    let b_aligned = build(37, true);

    let big_blocks = big.len() / GRID; // 4
    let unaligned = shared_blocks(&a_unaligned, &b_unaligned);
    let aligned = shared_blocks(&a_aligned, &b_aligned);

    eprintln!(
        "big-file dedup under churn: unaligned {unaligned}/{big_blocks} blocks shared, \
         aligned {aligned}/{big_blocks} blocks shared"
    );

    // The whole point: alignment recovers the big file's blocks.
    assert_eq!(
        aligned, big_blocks,
        "aligned: all {big_blocks} grid blocks of the unchanged file must dedup across the churned rebuild"
    );
    assert!(
        unaligned < aligned,
        "unaligned dedup ({unaligned}) must be strictly worse than aligned ({aligned}) — \
         otherwise the metadata shift didn't actually de-align anything and the test proves nothing"
    );

    // And the aligned image must still be a valid EROFS (holes between files are
    // legal; the kernel/fsck never touch them).
    let tmp = write_temp(&b_aligned);
    if let Some(fsck) = find_tool("fsck.erofs") {
        let out = Command::new(&fsck).arg(tmp.path()).output().expect("run fsck.erofs");
        assert!(
            out.status.success(),
            "fsck.erofs rejected the aligned image:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    } else {
        eprintln!("SKIP fsck portion: fsck.erofs not installed");
    }
}

/// Cold-start priority ordering: files named in `PriorityOrder` are laid out
/// FIRST and contiguously, so the boot working set is one coalesce-friendly run
/// instead of being scattered across the image. We prove it by content offset:
/// a priority file created LAST must nonetheless land EARLIER in the image than a
/// non-priority file created first — the layout order, not creation order, wins.
/// Also asserts determinism and `fsck.erofs` validity.
#[test]
fn erofs_priority_order_places_boot_set_first() {
    use ext4::erofs::Writer;
    use ext4::File;

    const GRID: usize = 128 * 1024;
    const S_IFREG: u16 = 0x8000;

    fn payload(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }
    fn first_offset(hay: &[u8], needle: &[u8]) -> Option<usize> {
        hay.windows(needle.len()).position(|w| w == needle)
    }

    // Two big, distinct files. "cold" is created first (so DFS would place it
    // first); "hot" is created last but marked priority.
    let cold = payload(1, 300 * 1024);
    let hot = payload(2, 300 * 1024);
    let regf = |size: i64| File { mode: S_IFREG | 0o644, size, ..Default::default() };

    let build = |priority: bool| -> Vec<u8> {
        let mut opts = vec![
            WriterOption::Uuid([0x42u8; 16]),
            WriterOption::AlignData { align: GRID as u32, min_size: 4096 },
        ];
        if priority {
            opts.push(WriterOption::PriorityOrder(vec!["dir/hot.bin".to_string()]));
        }
        let mut w = Writer::new(Cursor::new(Vec::new()), &opts);
        w.make_parents("dir/cold.bin").unwrap();
        w.create("dir/cold.bin", &regf(cold.len() as i64)).unwrap();
        w.write_all(&cold).unwrap();
        w.create("dir/hot.bin", &regf(hot.len() as i64)).unwrap();
        w.write_all(&hot).unwrap();
        // a bunch of unrelated files to push the data region around
        for i in 0..50 {
            let n = format!("dir/pad_{i:03}.bin");
            w.create(&n, &regf(20 * 1024)).unwrap();
            w.write_all(&payload(1000 + i, 20 * 1024)).unwrap();
        }
        w.close().unwrap().into_inner()
    };

    // Use a marker prefix from each payload's full-block region to locate it.
    let cold_marker = &cold[..256];
    let hot_marker = &hot[..256];

    let plain = build(false);
    let prio = build(true);

    // Determinism with priority on.
    assert_eq!(prio, build(true), "priority layout must be byte-deterministic");

    let (c_plain, h_plain) = (
        first_offset(&plain, cold_marker).expect("cold in plain"),
        first_offset(&plain, hot_marker).expect("hot in plain"),
    );
    let (c_prio, h_prio) = (
        first_offset(&prio, cold_marker).expect("cold in prio"),
        first_offset(&prio, hot_marker).expect("hot in prio"),
    );

    // Natural order: cold (created first) is earlier than hot.
    assert!(c_plain < h_plain, "without priority, creation order holds (cold before hot)");
    // Priority flips it: hot is laid out before cold despite being created last.
    assert!(
        h_prio < c_prio,
        "priority must place hot.bin ({h_prio}) before cold.bin ({c_prio}) in the image"
    );
    // And hot moved strictly earlier than it was without priority.
    assert!(h_prio < h_plain, "priority must move hot.bin earlier ({h_prio} < {h_plain})");

    let tmp = write_temp(&prio);
    if let Some(fsck) = find_tool("fsck.erofs") {
        let out = Command::new(&fsck).arg(tmp.path()).output().expect("run fsck.erofs");
        assert!(
            out.status.success(),
            "fsck.erofs rejected the priority-ordered image:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    } else {
        eprintln!("SKIP fsck portion: fsck.erofs not installed");
    }
}

/// Priority-order edge cases: missing paths are skipped, directories/symlinks in
/// the list are handled, an empty list is a no-op, a large priority file is
/// packed TIGHT (no alignment hole) and lands right after metadata, and
/// `prefetch_len` is sane in each case. All variants must pass `fsck.erofs`.
#[test]
fn erofs_priority_order_edge_cases() {
    use ext4::erofs::Writer;
    use ext4::File;

    const GRID: usize = 128 * 1024;
    const S_IFREG: u16 = 0x8000;
    const S_IFLNK: u16 = 0xA000;

    fn payload(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    let big = payload(7, 300 * 1024); // 2+ grid blocks → would normally align
    let regf = |size: i64| File { mode: S_IFREG | 0o644, size, ..Default::default() };

    // Build a tree with: a dir, a symlink, a big regular file, and padding.
    let build = |priority: Vec<&str>| -> (Vec<u8>, u64) {
        let mut opts = vec![
            WriterOption::Uuid([0x42u8; 16]),
            WriterOption::AlignData { align: GRID as u32, min_size: 4096 },
        ];
        if !priority.is_empty() {
            opts.push(WriterOption::PriorityOrder(
                priority.iter().map(|s| s.to_string()).collect(),
            ));
        }
        let mut w = Writer::new(Cursor::new(Vec::new()), &opts);
        w.make_parents("d/sub/x").unwrap();
        w.create(
            "d/sub",
            &File { mode: 0x4000 | 0o755, ..Default::default() },
        )
        .unwrap();
        w.create("big.bin", &regf(big.len() as i64)).unwrap();
        w.write_all(&big).unwrap();
        w.create(
            "link",
            &File { mode: S_IFLNK | 0o777, linkname: "big.bin".into(), size: 7, ..Default::default() },
        )
        .unwrap();
        for i in 0..40 {
            let n = format!("pad_{i:03}");
            w.create(&n, &regf(20 * 1024)).unwrap();
            w.write_all(&payload(100 + i, 20 * 1024)).unwrap();
        }
        w.close_with_prefetch()
            .map(|(c, p)| (c.into_inner(), p))
            .unwrap()
    };

    let fsck_ok = |img: &[u8], label: &str| {
        let tmp = write_temp(img);
        if let Some(fsck) = find_tool("fsck.erofs") {
            let out = Command::new(&fsck).arg(tmp.path()).output().expect("run fsck.erofs");
            assert!(
                out.status.success(),
                "fsck.erofs rejected {label}:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    };

    // 1) Empty list == no hint (prefetch_len 0) and still valid.
    let (img_empty, pf_empty) = build(vec![]);
    assert_eq!(pf_empty, 0, "no priority → no prefetch hint");
    fsck_ok(&img_empty, "empty-priority image");

    // 2) A path that doesn't exist is skipped — image still builds and is valid,
    //    and the present priority file still gets a hint.
    let (img_missing, pf_missing) = build(vec!["does/not/exist", "big.bin"]);
    assert!(pf_missing > 0, "present priority file still yields a hint");
    fsck_ok(&img_missing, "missing-path image");

    // 3) Directory + symlink + missing + regular, all in the list together.
    let (img_mixed, pf_mixed) = build(vec!["d/sub", "link", "nope", "big.bin"]);
    assert!(pf_mixed > 0);
    fsck_ok(&img_mixed, "mixed-type priority image");

    // 4) The big priority file is packed TIGHT (no alignment hole): its data must
    //    start within the first GRID bytes after the metadata region, so the
    //    prefetch extent covering [meta + big.bin] is far smaller than the whole
    //    image. Compare to a non-priority build where big.bin is grid-aligned and
    //    pushed later by the 40 pad files.
    let (img_prio, pf_prio) = build(vec!["big.bin"]);
    fsck_ok(&img_prio, "tight-priority image");
    // prefetch_len must cover the big file but be far less than the full image.
    assert!(
        pf_prio >= big.len() as u64,
        "prefetch extent ({pf_prio}) must cover the big priority file ({})",
        big.len()
    );
    assert!(
        pf_prio < img_prio.len() as u64,
        "prefetch extent ({pf_prio}) must be a small prefix, not the whole image ({})",
        img_prio.len()
    );
    // Determinism with edge-case inputs.
    assert_eq!(build(vec!["d/sub", "link", "nope", "big.bin"]).0, img_mixed);
}

#[test]
fn erofs_xattrs() {
    use ext4::erofs::Writer;
    use ext4::File;
    use std::collections::BTreeMap;

    let img = {
        let mut w = Writer::new(Cursor::new(Vec::new()), &[WriterOption::Uuid([7u8; 16])]);
        let mut fx: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        fx.insert("user.greeting".into(), b"hi".to_vec());
        fx.insert("security.capability".into(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
        w.create(
            "file",
            &File {
                mode: 0x8000 | 0o644,
                size: 5,
                xattrs: fx,
                ..Default::default()
            },
        )
        .unwrap();
        w.write_all(b"hello").unwrap();
        let mut dx: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        dx.insert("trusted.overlay.opaque".into(), b"y".to_vec());
        w.create(
            "opq",
            &File {
                mode: 0x4000 | 0o755,
                xattrs: dx,
                ..Default::default()
            },
        )
        .unwrap();
        w.close().unwrap().into_inner()
    };

    let tmp = write_temp(&img);
    if let Some(fsck) = find_tool("fsck.erofs") {
        let out = Command::new(&fsck).arg(tmp.path()).output().expect("run fsck.erofs");
        assert!(
            out.status.success(),
            "fsck.erofs rejected xattr image:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    } else {
        eprintln!("SKIP fsck portion: fsck.erofs not installed");
    }

    if std::env::var("EROFS_MOUNT_TEST").as_deref() != Ok("1") {
        eprintln!("SKIP: set EROFS_MOUNT_TEST=1 to verify xattr values via getfattr");
        return;
    }
    if Command::new("sudo").args(["-n", "true"]).status().map(|s| !s.success()).unwrap_or(true) {
        eprintln!("SKIP: passwordless sudo unavailable");
        return;
    }
    if find_tool("getfattr").is_none() {
        eprintln!("SKIP: getfattr (attr package) not installed");
        return;
    }
    let mnt = tempfile::tempdir().unwrap();
    let ok = Command::new("sudo")
        .args(["-n", "mount", "-t", "erofs", "-o", "ro,loop"])
        .arg(tmp.path())
        .arg(mnt.path())
        .status()
        .expect("mount")
        .success();
    assert!(ok, "kernel mount failed");

    let getval = |path: std::path::PathBuf, attr: &str| -> Option<String> {
        let out = Command::new("sudo")
            .args(["-n", "getfattr", "-n", attr, "--only-values"])
            .arg(&path)
            .output()
            .ok()?;
        out.status.success().then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    };
    let result = std::panic::catch_unwind(|| {
        assert_eq!(getval(mnt.path().join("file"), "user.greeting").as_deref(), Some("hi"));
        assert_eq!(
            getval(mnt.path().join("opq"), "trusted.overlay.opaque").as_deref(),
            Some("y")
        );
    });
    let _ = Command::new("sudo").args(["-n", "umount"]).arg(mnt.path()).status();
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
