#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap, clippy::cast_sign_loss)]
/// tar-to-ext4 conversion.
///
/// Ported from: github.com/Microsoft/hcsshim/ext4/tar2ext4
///
/// Iterates tar entries and maps them to ext4 writer operations.
/// Handles OCI whiteout entries for multi-layer merges, extracts PAX
/// xattr records, and handles all standard entry types.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::format;
use crate::writer::{File, Writer, WriterOption};

const WHITEOUT_PREFIX: &str = ".wh.";
const OPAQUE_WHITEOUT: &str = ".wh..wh..opq";

/// Options for tar-to-ext4 conversion.
#[derive(Default)]
pub struct ConvertOptions {
    pub convert_backslash: bool,
    pub writer_options: Vec<WriterOption>,
}

/// Convert a tar stream into a compact ext4 filesystem image.
///
/// Reads tar entries from `tar_reader`, writes ext4 to `output`.
/// The output must be seekable (e.g., a file or `Cursor<Vec<u8>>`).
pub fn convert_tar_to_ext4<R: Read, W: Read + Write + Seek>(
    tar_reader: R,
    output: W,
    options: &ConvertOptions,
) -> io::Result<W> {
    let mut archive = tar::Archive::new(tar_reader);
    let mut fs = Writer::new(output, &options.writer_options);

    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        let (name, link_name) = extract_names(entry.header(), options)?;

        fs.make_parents(&name)?;

        // Skip OCI whiteout entries — they're layer-deletion markers that are
        // meaningless in a single merged ext4 filesystem.
        if is_whiteout(&name) {
            continue;
        }

        write_tar_entry_with_pax(&mut fs, &mut entry, &name, &link_name)?;
    }

    fs.close()
}

/// Convert multiple OCI layers into a single ext4 filesystem.
///
/// `layers` must be in bottom-to-top order (layer 0 = base, last = topmost).
/// Each reader must be a seekable, decompressed tar stream (e.g., a temp file).
/// The merge respects OCI whiteout semantics: `.wh.<name>` deletes a file,
/// `.wh..wh..opq` deletes all lower-layer entries in a directory.
pub fn convert_oci_layers_to_ext4<R, W>(
    layers: &mut [R],
    output: W,
    options: &ConvertOptions,
) -> io::Result<W>
where
    R: Read + Seek,
    W: Read + Write + Seek,
{
    if layers.is_empty() {
        let fs = Writer::new(output, &options.writer_options);
        return fs.close();
    }

    // Phase 1: Build ownership map (scan top-to-bottom).
    let merge = build_ownership_map(layers, options)?;

    // Phase 2: Reset all readers to start.
    for layer in layers.iter_mut() {
        layer.seek(SeekFrom::Start(0))?;
    }

    // Phase 3: Stream entries bottom-to-top into a single ext4 Writer.
    let mut fs = Writer::new(output, &options.writer_options);
    for (layer_idx, layer) in layers.iter_mut().enumerate() {
        write_layer_entries(&mut fs, layer, layer_idx, &merge, options)?;
    }
    fs.close()
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Extract and normalize path names from a tar header.
fn extract_names(header: &tar::Header, options: &ConvertOptions) -> io::Result<(String, String)> {
    let mut name = header
        .path()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .to_string_lossy()
        .to_string();

    let mut link_name = header
        .link_name()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    if options.convert_backslash {
        name = name.replace('\\', "/");
        link_name = link_name.replace('\\', "/");
    }

    Ok((name, link_name))
}

/// Check if a tar entry name is an OCI whiteout marker.
fn is_whiteout(name: &str) -> bool {
    if let Some((_, file)) = split_dir_file(name) {
        file.starts_with(WHITEOUT_PREFIX)
    } else {
        false
    }
}

/// Write a single tar entry with PAX xattr support.
///
/// This variant takes a `tar::Entry` directly so it can extract PAX extensions.
fn write_tar_entry_with_pax<R: Read, W: Read + Write + Seek>(
    fs: &mut Writer<W>,
    entry: &mut tar::Entry<'_, R>,
    name: &str,
    link_name: &str,
) -> io::Result<()> {
    let header = entry.header().clone();
    let entry_type = header.entry_type();

    if entry_type == tar::EntryType::Link {
        fs.link(link_name, name)?;
        return Ok(());
    }

    let mode_bits = header
        .mode()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? as u16;

    let typ: u16 = match entry_type {
        tar::EntryType::Regular | tar::EntryType::GNUSparse => format::S_IFREG,
        tar::EntryType::Symlink => format::S_IFLNK,
        tar::EntryType::Char => format::S_IFCHR,
        tar::EntryType::Block => format::S_IFBLK,
        tar::EntryType::Directory => format::S_IFDIR,
        tar::EntryType::Fifo => format::S_IFIFO,
        _ => return Ok(()),
    };

    let uid = header.uid().unwrap_or(0) as u32;
    let gid = header.gid().unwrap_or(0) as u32;
    let size = header
        .size()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? as i64;
    let mtime = header.mtime().unwrap_or(0);
    let devmajor = header.device_major().ok().flatten().unwrap_or(0);
    let devminor = header.device_minor().ok().flatten().unwrap_or(0);

    let mut xattrs = BTreeMap::new();
    if let Some(pax) = entry.pax_extensions()? {
        for ext in pax {
            let ext = ext.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if let Some(attr_name) = ext
                .key()
                .ok()
                .and_then(|k| k.strip_prefix("SCHILY.xattr."))
            {
                xattrs.insert(attr_name.to_string(), ext.value_bytes().to_vec());
            }
        }
    }

    let fs_mtime = mtime & 0x3ffffffff;

    let f = File {
        mode: (mode_bits & !format::TYPE_MASK) | typ,
        size,
        uid,
        gid,
        atime: fs_mtime,
        ctime: fs_mtime,
        mtime: fs_mtime,
        crtime: fs_mtime,
        linkname: link_name.to_string(),
        devmajor,
        devminor,
        xattrs,
    };

    fs.create(name, &f)?;

    if typ == format::S_IFREG && size > 0 {
        io::copy(entry, fs)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-layer merge internals
// ---------------------------------------------------------------------------

/// Tracks which layer owns each path and which paths are deleted by whiteouts.
struct LayerMerge {
    /// path -> layer index that owns it (highest layer wins).
    owner: HashMap<String, usize>,
    /// Paths deleted by whiteout entries.
    deleted: HashSet<String>,
}

/// Scan all layers to determine path ownership and whiteout deletions.
///
/// Scans top-to-bottom (highest layer first) so the first insert for each
/// path wins (= highest layer owns it).
fn build_ownership_map<R: Read + Seek>(
    layers: &mut [R],
    options: &ConvertOptions,
) -> io::Result<LayerMerge> {
    let mut owner: HashMap<String, usize> = HashMap::new();
    let mut deleted: HashSet<String> = HashSet::new();
    // dir path -> layer index where the opaque whiteout appears.
    let mut opaque_dirs: HashMap<String, usize> = HashMap::new();

    // Scan top-to-bottom (highest layer first).
    for layer_idx in (0..layers.len()).rev() {
        layers[layer_idx].seek(SeekFrom::Start(0))?;
        let mut archive = tar::Archive::new(&mut layers[layer_idx]);
        for entry_result in archive.entries()? {
            let entry = entry_result?;
            let header = entry.header();
            let (name, _) = extract_names(header, options)?;
            let normalized = name.trim_end_matches('/').to_string();

            if let Some((dir, file)) = split_dir_file(&name) {
                if file == OPAQUE_WHITEOUT {
                    let dir_key = dir.trim_end_matches('/').to_string();
                    opaque_dirs.entry(dir_key).or_insert(layer_idx);
                    continue;
                }
                if let Some(stripped) = file.strip_prefix(WHITEOUT_PREFIX) {
                    let target = if dir.is_empty() {
                        stripped.to_string()
                    } else {
                        format!("{}{}", dir, stripped)
                    };
                    let target = target.trim_end_matches('/').to_string();
                    deleted.insert(target);
                    continue;
                }
            }

            // First insert wins (= highest layer, since we scan top-down).
            owner.entry(normalized).or_insert(layer_idx);
        }
    }

    // Apply opaque whiteouts: any path owned by a layer below the opaque
    // layer and under the opaque directory gets deleted.
    let owned_paths: Vec<(String, usize)> = owner
        .iter()
        .map(|(k, &v)| (k.clone(), v))
        .collect();
    for (path, owning_layer) in owned_paths {
        for (opaque_dir, &opaque_layer) in &opaque_dirs {
            if owning_layer < opaque_layer && is_child_of(&path, opaque_dir) {
                deleted.insert(path.clone());
                break;
            }
        }
    }

    Ok(LayerMerge { owner, deleted })
}

/// Check if `path` is a direct or nested child of `dir`.
fn is_child_of(path: &str, dir: &str) -> bool {
    path.starts_with(dir) && path.as_bytes().get(dir.len()) == Some(&b'/')
}

/// Write entries from a single layer, skipping those owned by other layers or deleted.
fn write_layer_entries<R: Read, W: Read + Write + Seek>(
    fs: &mut Writer<W>,
    layer: R,
    layer_idx: usize,
    merge: &LayerMerge,
    options: &ConvertOptions,
) -> io::Result<()> {
    let mut archive = tar::Archive::new(layer);
    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        let (name, link_name) = extract_names(entry.header(), options)?;
        let normalized = name.trim_end_matches('/').to_string();

        // Skip whiteout entries — they're control markers, not real files.
        if is_whiteout(&name) {
            continue;
        }

        // Skip if deleted by a whiteout from a higher layer.
        if merge.deleted.contains(&normalized) {
            continue;
        }

        // Skip if a different (higher) layer owns this path.
        if let Some(&owning_layer) = merge.owner.get(&normalized)
            && owning_layer != layer_idx {
                continue;
            }

        fs.make_parents(&name)?;
        write_tar_entry_with_pax(fs, &mut entry, &name, &link_name)?;
    }
    Ok(())
}

/// Split a path into (directory, filename).
/// Returns None if the path has no filename component.
fn split_dir_file(name: &str) -> Option<(&str, &str)> {
    // Find the last '/' that's not trailing
    let trimmed = name.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(n) => Some((&trimmed[..n + 1], &trimmed[n + 1..])),
        None => {
            if trimmed.is_empty() {
                None
            } else {
                Some(("", trimmed))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a tar archive in memory from a list of (path, content) pairs.
    fn build_tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for &(path, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            header.set_cksum();
            builder.append(&header, data).unwrap();
        }
        builder.into_inner().unwrap()
    }

    /// Build a tar archive with directory entries too.
    fn build_tar_with_dirs(entries: &[TarEntry<'_>]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for entry in entries {
            match entry {
                TarEntry::File(path, data) => {
                    let mut header = tar::Header::new_gnu();
                    header.set_path(path).unwrap();
                    header.set_size(data.len() as u64);
                    header.set_mode(0o644);
                    header.set_entry_type(tar::EntryType::Regular);
                    header.set_cksum();
                    builder.append(&header, *data).unwrap();
                }
                TarEntry::Dir(path) => {
                    let mut header = tar::Header::new_gnu();
                    header.set_path(*path).unwrap();
                    header.set_size(0);
                    header.set_mode(0o755);
                    header.set_entry_type(tar::EntryType::Directory);
                    header.set_cksum();
                    builder.append(&header, &[][..]).unwrap();
                }
                TarEntry::Whiteout(path) => {
                    let mut header = tar::Header::new_gnu();
                    header.set_path(*path).unwrap();
                    header.set_size(0);
                    header.set_mode(0o644);
                    header.set_entry_type(tar::EntryType::Regular);
                    header.set_cksum();
                    builder.append(&header, &[][..]).unwrap();
                }
            }
        }
        builder.into_inner().unwrap()
    }

    enum TarEntry<'a> {
        File(&'a str, &'a [u8]),
        Dir(&'a str),
        Whiteout(&'a str),
    }

    /// Read a file from ext4 image bytes using the reader.
    fn read_file(image: &[u8], path: &str) -> Option<Vec<u8>> {
        let mut reader = crate::reader::Reader::new(Cursor::new(image)).ok()?;
        let entries = reader.walk().ok()?;
        let normalized = path.trim_start_matches('/');
        let entry = entries.iter().find(|e| e.path == normalized)?;
        let inode = reader.read_inode(entry.inode_number).ok()?;
        let data = reader.read_data(&inode).ok()?;
        Some(data)
    }

    /// Check if a path exists in the ext4 image.
    fn path_exists(image: &[u8], path: &str) -> bool {
        let mut reader = match crate::reader::Reader::new(Cursor::new(image)) {
            Ok(r) => r,
            Err(_) => return false,
        };
        let entries = match reader.walk() {
            Ok(e) => e,
            Err(_) => return false,
        };
        let normalized = path.trim_start_matches('/');
        entries.iter().any(|e| e.path == normalized)
    }

    #[test]
    fn test_merge_two_layers() {
        let layer0 = build_tar(&[("a.txt", b"hello")]);
        let layer1 = build_tar(&[("b.txt", b"world")]);

        let mut layers: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(layer0),
            Cursor::new(layer1),
        ];
        let output = Cursor::new(Vec::new());
        let opts = ConvertOptions::default();
        let result = convert_oci_layers_to_ext4(&mut layers, output, &opts).unwrap();
        let image = result.into_inner();

        assert_eq!(read_file(&image, "/a.txt").unwrap(), b"hello");
        assert_eq!(read_file(&image, "/b.txt").unwrap(), b"world");
    }

    #[test]
    fn test_merge_override() {
        let layer0 = build_tar(&[("a.txt", b"old")]);
        let layer1 = build_tar(&[("a.txt", b"new")]);

        let mut layers: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(layer0),
            Cursor::new(layer1),
        ];
        let output = Cursor::new(Vec::new());
        let opts = ConvertOptions::default();
        let result = convert_oci_layers_to_ext4(&mut layers, output, &opts).unwrap();
        let image = result.into_inner();

        assert_eq!(read_file(&image, "/a.txt").unwrap(), b"new");
    }

    #[test]
    fn test_merge_whiteout() {
        let layer0 = build_tar(&[("a.txt", b"hello"), ("b.txt", b"keep")]);
        let layer1 = build_tar_with_dirs(&[
            TarEntry::Whiteout(".wh.a.txt"),
        ]);

        let mut layers: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(layer0),
            Cursor::new(layer1),
        ];
        let output = Cursor::new(Vec::new());
        let opts = ConvertOptions::default();
        let result = convert_oci_layers_to_ext4(&mut layers, output, &opts).unwrap();
        let image = result.into_inner();

        assert!(!path_exists(&image, "/a.txt"));
        assert_eq!(read_file(&image, "/b.txt").unwrap(), b"keep");
    }

    #[test]
    fn test_merge_opaque_whiteout() {
        let layer0 = build_tar_with_dirs(&[
            TarEntry::Dir("dir/"),
            TarEntry::File("dir/a.txt", b"from-base"),
            TarEntry::File("dir/b.txt", b"from-base"),
        ]);
        let layer1 = build_tar_with_dirs(&[
            TarEntry::Dir("dir/"),
            TarEntry::Whiteout("dir/.wh..wh..opq"),
            TarEntry::File("dir/c.txt", b"from-top"),
        ]);

        let mut layers: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(layer0),
            Cursor::new(layer1),
        ];
        let output = Cursor::new(Vec::new());
        let opts = ConvertOptions::default();
        let result = convert_oci_layers_to_ext4(&mut layers, output, &opts).unwrap();
        let image = result.into_inner();

        assert!(!path_exists(&image, "/dir/a.txt"));
        assert!(!path_exists(&image, "/dir/b.txt"));
        assert_eq!(read_file(&image, "/dir/c.txt").unwrap(), b"from-top");
        assert!(path_exists(&image, "/dir"));
    }

    #[test]
    fn test_merge_three_layers() {
        let layer0 = build_tar(&[
            ("a.txt", b"base-a"),
            ("b.txt", b"base-b"),
            ("c.txt", b"base-c"),
        ]);
        let layer1 = build_tar_with_dirs(&[
            TarEntry::File("b.txt", b"mid-b"),
            TarEntry::File("d.txt", b"mid-d"),
        ]);
        let layer2 = build_tar_with_dirs(&[
            TarEntry::Whiteout(".wh.c.txt"),
            TarEntry::File("d.txt", b"top-d"),
        ]);

        let mut layers: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(layer0),
            Cursor::new(layer1),
            Cursor::new(layer2),
        ];
        let output = Cursor::new(Vec::new());
        let opts = ConvertOptions::default();
        let result = convert_oci_layers_to_ext4(&mut layers, output, &opts).unwrap();
        let image = result.into_inner();

        assert_eq!(read_file(&image, "/a.txt").unwrap(), b"base-a");
        assert_eq!(read_file(&image, "/b.txt").unwrap(), b"mid-b");
        assert!(!path_exists(&image, "/c.txt"));
        assert_eq!(read_file(&image, "/d.txt").unwrap(), b"top-d");
    }

    #[test]
    fn test_single_layer_merge() {
        let layer0 = build_tar(&[("hello.txt", b"hello world")]);

        let mut layers: Vec<Cursor<Vec<u8>>> = vec![Cursor::new(layer0)];
        let output = Cursor::new(Vec::new());
        let opts = ConvertOptions::default();
        let result = convert_oci_layers_to_ext4(&mut layers, output, &opts).unwrap();
        let image = result.into_inner();

        assert_eq!(read_file(&image, "/hello.txt").unwrap(), b"hello world");
    }

    #[test]
    fn test_empty_layers() {
        let mut layers: Vec<Cursor<Vec<u8>>> = vec![];
        let output = Cursor::new(Vec::new());
        let opts = ConvertOptions::default();
        let result = convert_oci_layers_to_ext4(&mut layers, output, &opts);
        assert!(result.is_ok());
    }
}
