/// Incremental export: diff two ext4 snapshots → OCI delta tar layer.
///
/// Compares two ext4 filesystems by walking both and diffing by path + metadata.
/// Produces a tar containing:
/// - Full file data for added/modified entries
/// - `.wh.<name>` whiteout entries for deletions (per OCI image spec)
use std::collections::BTreeMap;
use std::io::{self, Read, Seek, Write};

use crate::format::{self, InodeNumber, TYPE_MASK};
use crate::reader::{Reader, WalkEntry};

/// Returns true if two entries have identical metadata (same file state).
///
/// Ignores atime/ctime (volatile), inode_number (internal), and links_count
/// (hard link topology tracked separately by write_entry_to_tar).
fn metadata_equal(a: &WalkEntry, b: &WalkEntry) -> bool {
    a.mode == b.mode
        && a.uid == b.uid
        && a.gid == b.gid
        && a.size == b.size
        && a.mtime == b.mtime
        && a.symlink_target == b.symlink_target
        && a.devmajor == b.devmajor
        && a.devminor == b.devminor
        && a.xattrs == b.xattrs
}

/// Diff two ext4 snapshots and write an OCI-compatible delta tar.
///
/// The delta contains:
/// - Added files (in target but not base): full tar entries with data
/// - Modified files (metadata changed): full tar entries with data
/// - Deleted files (in base but not target): `.wh.<name>` whiteout markers
///
/// For deleted directories, a single `.wh.<dirname>` entry is emitted —
/// child entries are suppressed since the whiteout removes the entire subtree.
///
/// File data is streamed from `target` on demand (not buffered in full).
pub fn diff_to_tar<R1, R2, W>(
    base: &mut Reader<R1>,
    target: &mut Reader<R2>,
    w: W,
) -> io::Result<W>
where
    R1: Read + Seek,
    R2: Read + Seek,
    W: Write,
{
    let base_entries = base.walk()?;
    let target_entries = target.walk()?;

    let base_map: BTreeMap<&str, &WalkEntry> =
        base_entries.iter().map(|e| (e.path.as_str(), e)).collect();
    let target_map: BTreeMap<&str, &WalkEntry> =
        target_entries.iter().map(|e| (e.path.as_str(), e)).collect();

    let mut builder = tar::Builder::new(w);
    let mut seen_inodes: BTreeMap<InodeNumber, String> = BTreeMap::new();

    // Added and modified entries: iterate target, skip unchanged.
    for entry in &target_entries {
        let unchanged = base_map
            .get(entry.path.as_str())
            .is_some_and(|base_entry| metadata_equal(base_entry, entry));
        if unchanged {
            continue;
        }
        target.write_entry_to_tar(entry, &mut builder, &mut seen_inodes)?;
    }

    // Deleted entries: in base but not target.
    // Track deleted directory prefixes to suppress child whiteouts.
    let mut deleted_dir_prefixes: Vec<String> = Vec::new();

    for base_entry in &base_entries {
        if target_map.contains_key(base_entry.path.as_str()) {
            continue;
        }

        // Skip children of already-deleted directories.
        let under_deleted = deleted_dir_prefixes
            .iter()
            .any(|prefix| base_entry.path.starts_with(prefix.as_str()));
        if under_deleted {
            continue;
        }

        let is_dir = (base_entry.mode & TYPE_MASK) == format::S_IFDIR;
        if is_dir {
            deleted_dir_prefixes.push(format!("{}/", base_entry.path));
        }

        write_whiteout(&mut builder, &base_entry.path)?;
    }

    builder.into_inner()
}

/// Write an OCI whiteout entry for a deleted path.
///
/// Produces `.wh.<name>` in the parent directory. For example:
/// - `foo/bar.txt` → `foo/.wh.bar.txt`
/// - `top.txt` → `.wh.top.txt`
fn write_whiteout<W: Write>(builder: &mut tar::Builder<W>, path: &str) -> io::Result<()> {
    let whiteout_path = match path.rfind('/') {
        Some(slash) => format!("{}/.wh.{}", &path[..slash], &path[slash + 1..]),
        None => format!(".wh.{}", path),
    };

    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(0);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    builder.append_data(&mut header, &whiteout_path, io::empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format;
    use crate::writer::{File as Ext4File, Writer, WriterOption};
    use std::io::Cursor;

    const DEVICE_SIZE: i64 = 16 * 1024 * 1024;

    fn make_image(files: &[(&str, u16, &[u8])]) -> Vec<u8> {
        let buf = Cursor::new(vec![0u8; DEVICE_SIZE as usize]);
        let mut w = Writer::new(buf, &[WriterOption::MaximumDiskSize(DEVICE_SIZE)]);

        // Auto-create parent directories.
        let mut created_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
        for &(path, _, _) in files {
            let mut current = String::new();
            if let Some(parent) = path.rsplit_once('/') {
                for component in parent.0.split('/') {
                    if !current.is_empty() {
                        current.push('/');
                    }
                    current.push_str(component);
                    if created_dirs.insert(current.clone()) {
                        w.create(
                            &current,
                            &Ext4File {
                                mode: format::S_IFDIR | 0o755,
                                uid: 1000,
                                gid: 1000,
                                mtime: 1700000000,
                                ..Default::default()
                            },
                        )
                        .unwrap();
                    }
                }
            }
        }

        for &(path, mode, data) in files {
            w.create(
                path,
                &Ext4File {
                    size: data.len() as i64,
                    mode,
                    uid: 1000,
                    gid: 1000,
                    mtime: 1700000000,
                    ..Default::default()
                },
            )
            .unwrap();
            if !data.is_empty() {
                io::Write::write_all(&mut w, data).unwrap();
            }
        }
        w.close().unwrap().into_inner()
    }

    fn tar_entries(tar_data: &[u8]) -> Vec<(String, Vec<u8>)> {
        let mut archive = tar::Archive::new(Cursor::new(tar_data));
        let mut entries = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().to_string();
            // Skip PAX headers
            if entry.header().entry_type() == tar::EntryType::XHeader {
                continue;
            }
            let mut data = Vec::new();
            io::Read::read_to_end(&mut entry, &mut data).unwrap();
            entries.push((path, data));
        }
        entries
    }

    #[test]
    fn test_diff_no_changes() {
        let image = make_image(&[("hello.txt", format::S_IFREG | 0o644, b"hello")]);

        let mut base = Reader::new(Cursor::new(&image)).unwrap();
        let mut target = Reader::new(Cursor::new(&image)).unwrap();

        let delta = diff_to_tar(&mut base, &mut target, Vec::new()).unwrap();
        let entries = tar_entries(&delta);
        assert!(entries.is_empty(), "no changes should produce empty tar");
    }

    #[test]
    fn test_diff_added_file() {
        let base_img = make_image(&[("a.txt", format::S_IFREG | 0o644, b"aaa")]);
        let target_img = make_image(&[
            ("a.txt", format::S_IFREG | 0o644, b"aaa"),
            ("b.txt", format::S_IFREG | 0o644, b"bbb"),
        ]);

        let mut base = Reader::new(Cursor::new(&base_img)).unwrap();
        let mut target = Reader::new(Cursor::new(&target_img)).unwrap();

        let delta = diff_to_tar(&mut base, &mut target, Vec::new()).unwrap();
        let entries = tar_entries(&delta);

        let paths: Vec<&str> = entries.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"b.txt"), "added file should be in delta: {paths:?}");
        assert!(!paths.contains(&"a.txt"), "unchanged file should not be in delta: {paths:?}");

        let b_entry = entries.iter().find(|(p, _)| p == "b.txt").unwrap();
        assert_eq!(b_entry.1, b"bbb");
    }

    #[test]
    fn test_diff_deleted_file() {
        let base_img = make_image(&[
            ("a.txt", format::S_IFREG | 0o644, b"aaa"),
            ("b.txt", format::S_IFREG | 0o644, b"bbb"),
        ]);
        let target_img = make_image(&[("a.txt", format::S_IFREG | 0o644, b"aaa")]);

        let mut base = Reader::new(Cursor::new(&base_img)).unwrap();
        let mut target = Reader::new(Cursor::new(&target_img)).unwrap();

        let delta = diff_to_tar(&mut base, &mut target, Vec::new()).unwrap();
        let entries = tar_entries(&delta);

        let paths: Vec<&str> = entries.iter().map(|(p, _)| p.as_str()).collect();
        assert!(
            paths.contains(&".wh.b.txt"),
            "deleted file should have whiteout: {paths:?}"
        );
        assert!(!paths.contains(&"a.txt"), "unchanged file should not be in delta: {paths:?}");
    }

    #[test]
    fn test_diff_modified_file() {
        let base_img = make_image(&[("file.txt", format::S_IFREG | 0o644, b"old content")]);
        let target_img = make_image(&[("file.txt", format::S_IFREG | 0o644, b"new content!!")]);

        let mut base = Reader::new(Cursor::new(&base_img)).unwrap();
        let mut target = Reader::new(Cursor::new(&target_img)).unwrap();

        let delta = diff_to_tar(&mut base, &mut target, Vec::new()).unwrap();
        let entries = tar_entries(&delta);

        let paths: Vec<&str> = entries.iter().map(|(p, _)| p.as_str()).collect();
        assert!(
            paths.contains(&"file.txt"),
            "modified file should be in delta: {paths:?}"
        );

        let file_entry = entries.iter().find(|(p, _)| p == "file.txt").unwrap();
        assert_eq!(file_entry.1, b"new content!!");
    }

    #[test]
    fn test_diff_deleted_directory() {
        let base_img = make_image(&[
            ("keep.txt", format::S_IFREG | 0o644, b"keep"),
            ("dir/a.txt", format::S_IFREG | 0o644, b"aaa"),
            ("dir/b.txt", format::S_IFREG | 0o644, b"bbb"),
        ]);
        let target_img = make_image(&[("keep.txt", format::S_IFREG | 0o644, b"keep")]);

        let mut base = Reader::new(Cursor::new(&base_img)).unwrap();
        let mut target = Reader::new(Cursor::new(&target_img)).unwrap();

        let delta = diff_to_tar(&mut base, &mut target, Vec::new()).unwrap();
        let entries = tar_entries(&delta);

        let paths: Vec<&str> = entries.iter().map(|(p, _)| p.as_str()).collect();
        // Should have a single whiteout for the directory, not individual files
        assert!(
            paths.contains(&".wh.dir"),
            "deleted dir should have whiteout: {paths:?}"
        );
        assert!(
            !paths.contains(&"dir/.wh.a.txt"),
            "children of deleted dir should be suppressed: {paths:?}"
        );
        assert!(
            !paths.contains(&"dir/.wh.b.txt"),
            "children of deleted dir should be suppressed: {paths:?}"
        );
    }

    #[test]
    fn test_diff_metadata_change() {
        // Same file content but different mode
        let base_img = make_image(&[("script.sh", format::S_IFREG | 0o644, b"#!/bin/sh")]);
        let target_img = make_image(&[("script.sh", format::S_IFREG | 0o755, b"#!/bin/sh")]);

        let mut base = Reader::new(Cursor::new(&base_img)).unwrap();
        let mut target = Reader::new(Cursor::new(&target_img)).unwrap();

        let delta = diff_to_tar(&mut base, &mut target, Vec::new()).unwrap();
        let entries = tar_entries(&delta);

        let paths: Vec<&str> = entries.iter().map(|(p, _)| p.as_str()).collect();
        assert!(
            paths.contains(&"script.sh"),
            "mode change should appear in delta: {paths:?}"
        );
    }

    #[test]
    fn test_whiteout_nested_path() {
        let base_img = make_image(&[
            ("a/b/c.txt", format::S_IFREG | 0o644, b"nested"),
            ("a/b/d.txt", format::S_IFREG | 0o644, b"also nested"),
        ]);
        // Keep directory but delete one file
        let target_img =
            make_image(&[("a/b/d.txt", format::S_IFREG | 0o644, b"also nested")]);

        let mut base = Reader::new(Cursor::new(&base_img)).unwrap();
        let mut target = Reader::new(Cursor::new(&target_img)).unwrap();

        let delta = diff_to_tar(&mut base, &mut target, Vec::new()).unwrap();
        let entries = tar_entries(&delta);

        let paths: Vec<&str> = entries.iter().map(|(p, _)| p.as_str()).collect();
        assert!(
            paths.contains(&"a/b/.wh.c.txt"),
            "nested whiteout should have correct path: {paths:?}"
        );
    }
}
