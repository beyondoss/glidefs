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
use crate::writer::{File, FsSink, Writer, WriterOption};

const WHITEOUT_PREFIX: &str = ".wh.";
const OPAQUE_WHITEOUT: &str = ".wh..wh..opq";
/// overlayfs marks an opaque directory with this xattr (value `"y"`).
const OVERLAY_OPAQUE_XATTR: &str = "trusted.overlay.opaque";

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
    let mut fs = Writer::new(output, &options.writer_options);
    merge_layers(layers, &mut fs, options)?;
    fs.close()
}

/// Merge multiple OCI layers into a single **EROFS** filesystem image.
///
/// Identical overlay semantics to [`convert_oci_layers_to_ext4`] (bottom-to-top,
/// higher layers win, `.wh.` deletes, `.wh..wh..opq` makes a dir opaque) but
/// emits the read-only EROFS format GlideFS serves as a daemonless block device.
pub fn convert_oci_layers_to_erofs<R, W>(
    layers: &mut [R],
    output: W,
    options: &ConvertOptions,
) -> io::Result<W>
where
    R: Read + Seek,
    W: Read + Write + Seek,
{
    Ok(convert_oci_layers_to_erofs_with_prefetch(layers, output, options)?.0)
}

/// Like [`convert_oci_layers_to_erofs`] but also returns the **prefetch length**
/// — the leading byte extent `[0, len)` covering the metadata region plus the
/// contiguous priority data run (0 when no
/// [`WriterOption::PriorityOrder`](crate::writer::WriterOption) was given).
/// Persist this in the volume manifest so the server warms the priority region
/// on open.
pub fn convert_oci_layers_to_erofs_with_prefetch<R, W>(
    layers: &mut [R],
    output: W,
    options: &ConvertOptions,
) -> io::Result<(W, u64)>
where
    R: Read + Seek,
    W: Read + Write + Seek,
{
    let mut fs = crate::erofs::Writer::new(output, &options.writer_options);
    merge_layers(layers, &mut fs, options)?;
    fs.close_with_prefetch()
}

/// Drive the OCI whiteout-aware merge into any [`FsSink`] (ext4 or EROFS).
fn merge_layers<R, S>(layers: &mut [R], fs: &mut S, options: &ConvertOptions) -> io::Result<()>
where
    R: Read + Seek,
    S: FsSink,
{
    if layers.is_empty() {
        return Ok(());
    }
    // Phase 1: ownership map (scan top-to-bottom).
    let merge = build_ownership_map(layers, options)?;
    // Phase 2: reset all readers.
    for layer in layers.iter_mut() {
        layer.seek(SeekFrom::Start(0))?;
    }
    // Phase 3: stream entries bottom-to-top into the sink.
    for (layer_idx, layer) in layers.iter_mut().enumerate() {
        write_layer_entries(fs, layer, layer_idx, &merge, options)?;
    }
    Ok(())
}

/// Convert a single OCI layer into an ext4 image that is a valid **overlayfs
/// lower layer** — i.e. layers *survive* instead of being flattened.
///
/// Unlike [`convert_oci_layers_to_ext4`], this does not merge or drop deletion
/// markers. Instead it translates OCI whiteouts to the on-disk form overlayfs
/// understands, so the resulting images can be stacked at runtime:
///   - `.wh.<name>`        → a character device `0,0` at `<dir>/<name>`
///   - `.wh..wh..opq`      → `trusted.overlay.opaque=y` xattr on `<dir>`
///
/// The output is deterministic for a fixed UUID, so the same layer (addressed
/// by its digest) always yields byte-identical ext4 — which is what makes
/// shared layers dedup across images.
///
/// `layer` must be a seekable decompressed tar stream (two passes: one to find
/// opaque directories, one to write).
pub fn convert_layer_to_ext4<R, W>(
    layer: R,
    output: W,
    options: &ConvertOptions,
) -> io::Result<W>
where
    R: Read + Seek,
    W: Read + Write + Seek,
{
    let mut fs = Writer::new(output, &options.writer_options);
    write_single_layer(layer, &mut fs, options)?;
    fs.close()
}

/// EROFS variant of [`convert_layer_to_ext4`]: emit a single OCI layer as an
/// overlay-preserving, deterministic **EROFS** image — the per-layer blob form
/// that is byte-identical across images and therefore dedups in storage.
pub fn convert_layer_to_erofs<R, W>(
    layer: R,
    output: W,
    options: &ConvertOptions,
) -> io::Result<W>
where
    R: Read + Seek,
    W: Read + Write + Seek,
{
    let mut fs = crate::erofs::Writer::new(output, &options.writer_options);
    write_single_layer(layer, &mut fs, options)?;
    fs.close()
}

/// Shared body: write one OCI layer into `fs`, preserving overlay whiteouts
/// (`.wh.<f>` → char-device 0,0; `.wh..wh..opq` → `trusted.overlay.opaque=y`).
fn write_single_layer<R, S>(mut layer: R, fs: &mut S, options: &ConvertOptions) -> io::Result<()>
where
    R: Read + Seek,
    S: FsSink,
{
    // Pass 1: find directories marked opaque so we can stamp the xattr when we
    // write the directory entry (the writer is forward-only — xattrs must be set
    // at create() time).
    let opaque_dirs = scan_opaque_dirs(&mut layer, options)?;
    layer.seek(SeekFrom::Start(0))?;

    let mut opaque_xattr = BTreeMap::new();
    opaque_xattr.insert(OVERLAY_OPAQUE_XATTR.to_string(), b"y".to_vec());
    let empty_xattrs: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    let mut seen_dirs: HashSet<String> = HashSet::new();

    let mut archive = tar::Archive::new(&mut layer);
    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        let (name, link_name) = extract_names(entry.header(), options)?;
        let normalized = name.trim_end_matches('/').to_string();

        if let Some((dir, file)) = split_dir_file(&name) {
            // Opaque marker: handled via the directory's xattr, not as a file.
            if file == OPAQUE_WHITEOUT {
                continue;
            }
            // Plain whiteout: emit an overlayfs char-device deletion marker.
            if let Some(stripped) = file.strip_prefix(WHITEOUT_PREFIX) {
                let target = if dir.is_empty() {
                    stripped.to_string()
                } else {
                    format!("{dir}{stripped}")
                };
                fs.make_parents(&target)?;
                let whiteout = File {
                    mode: format::S_IFCHR, // rdev 0,0 (devmajor/devminor default to 0)
                    ..Default::default()
                };
                fs.create(&target, &whiteout)?;
                continue;
            }
        }

        fs.make_parents(&name)?;
        let is_dir = entry.header().entry_type() == tar::EntryType::Directory;
        let extra = if is_dir && opaque_dirs.contains(&normalized) {
            &opaque_xattr
        } else {
            &empty_xattrs
        };
        write_tar_entry_inner(fs, &mut entry, &name, &link_name, extra)?;
        if is_dir {
            seen_dirs.insert(normalized);
        }
    }

    // Opaque directories that had no explicit entry in the tar: synthesize them
    // so the opaque xattr is recorded. `create()` treats caller xattrs as
    // authoritative, so this also fixes up dirs auto-created by make_parents.
    for dir in &opaque_dirs {
        if seen_dirs.contains(dir) {
            continue;
        }
        fs.make_parents(dir)?;
        let f = File {
            mode: format::S_IFDIR | 0o755,
            xattrs: opaque_xattr.clone(),
            ..Default::default()
        };
        fs.create(dir, &f)?;
    }

    Ok(())
}

/// Scan a single layer tar for directories carrying an opaque whiteout.
fn scan_opaque_dirs<R: Read + Seek>(
    layer: &mut R,
    options: &ConvertOptions,
) -> io::Result<HashSet<String>> {
    let mut opaque = HashSet::new();
    layer.seek(SeekFrom::Start(0))?;
    let mut archive = tar::Archive::new(layer);
    for entry_result in archive.entries()? {
        let entry = entry_result?;
        let (name, _) = extract_names(entry.header(), options)?;
        if let Some((dir, file)) = split_dir_file(&name)
            && file == OPAQUE_WHITEOUT
        {
            opaque.insert(dir.trim_end_matches('/').to_string());
        }
    }
    Ok(opaque)
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
fn write_tar_entry_with_pax<R: Read, S: FsSink>(
    fs: &mut S,
    entry: &mut tar::Entry<'_, R>,
    name: &str,
    link_name: &str,
) -> io::Result<()> {
    write_tar_entry_inner(fs, entry, name, link_name, &BTreeMap::new())
}

/// Write a tar entry, merging `extra_xattrs` on top of any PAX xattrs (used to
/// stamp `trusted.overlay.opaque` on opaque directories).
fn write_tar_entry_inner<R: Read, S: FsSink>(
    fs: &mut S,
    entry: &mut tar::Entry<'_, R>,
    name: &str,
    link_name: &str,
    extra_xattrs: &BTreeMap<String, Vec<u8>>,
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
    for (k, v) in extra_xattrs {
        xattrs.insert(k.clone(), v.clone());
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
fn write_layer_entries<R: Read, S: FsSink>(
    fs: &mut S,
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

    /// Build a representative multi-layer OCI image: base files, an override,
    /// a nested directory, a whiteout, and an opaque whiteout. Returns fresh
    /// tar bytes each call so repeated conversions never share mutable state.
    fn representative_layers() -> Vec<Vec<u8>> {
        let layer0 = build_tar_with_dirs(&[
            TarEntry::Dir("etc/"),
            TarEntry::File("etc/hostname", b"base-host"),
            TarEntry::File("etc/passwd", b"root:x:0:0"),
            TarEntry::Dir("var/"),
            TarEntry::Dir("var/log/"),
            TarEntry::File("var/log/old.log", b"stale"),
            TarEntry::File("readme.txt", b"base readme"),
        ]);
        let layer1 = build_tar_with_dirs(&[
            // Override a base file.
            TarEntry::File("etc/hostname", b"top-host"),
            // Delete a base file.
            TarEntry::Whiteout("etc/.wh.passwd"),
            // Opaque-whiteout the log dir, then add a new entry.
            TarEntry::Dir("var/log/"),
            TarEntry::Whiteout("var/log/.wh..wh..opq"),
            TarEntry::File("var/log/new.log", b"fresh"),
            // Add a brand new nested tree.
            TarEntry::Dir("app/"),
            TarEntry::File("app/main.bin", b"\x00\x01\x02\x03binary-ish\xff"),
        ]);
        vec![layer0, layer1]
    }

    /// Convert the representative layers with a fixed UUID and return the
    /// resulting ext4 image bytes.
    fn convert_with_fixed_uuid() -> Vec<u8> {
        let raw = representative_layers();
        let mut layers: Vec<Cursor<Vec<u8>>> = raw.into_iter().map(Cursor::new).collect();
        let opts = ConvertOptions {
            convert_backslash: false,
            writer_options: vec![
                WriterOption::MaximumDiskSize(64 * 1024 * 1024),
                // Pinning the UUID is what makes the output reproducible: it
                // seeds the superblock UUID and the directory hash seed.
                WriterOption::Uuid([0x42u8; 16]),
                WriterOption::Journal(1024),
            ],
        };
        let output = Cursor::new(Vec::new());
        convert_oci_layers_to_ext4(&mut layers, output, &opts)
            .unwrap()
            .into_inner()
    }

    /// The whole OCI→ext4 conversion must be byte-for-byte deterministic when
    /// the writer options (including UUID) are fixed. This is the invariant the
    /// `bless` pipeline relies on for content-addressed, reproducible images.
    #[test]
    fn test_conversion_is_byte_deterministic() {
        let a = convert_with_fixed_uuid();
        let b = convert_with_fixed_uuid();
        let c = convert_with_fixed_uuid();

        assert_eq!(a.len(), b.len(), "image length must be stable");
        assert_eq!(a, b, "two conversions of the same input must be byte-identical");
        assert_eq!(b, c, "conversion must be byte-identical across repeated runs");

        // Sanity: the image actually contains the merged result, not an empty fs.
        assert_eq!(read_file(&a, "/etc/hostname").unwrap(), b"top-host");
        assert!(!path_exists(&a, "/etc/passwd"), "whiteout must delete passwd");
        assert!(!path_exists(&a, "/var/log/old.log"), "opaque whiteout must drop old.log");
        assert_eq!(read_file(&a, "/var/log/new.log").unwrap(), b"fresh");
        assert_eq!(read_file(&a, "/app/main.bin").unwrap(), b"\x00\x01\x02\x03binary-ish\xff");
    }

    /// Overlay-preserving single-layer conversion must keep deletion markers in
    /// the on-disk form overlayfs understands: char-device `0,0` for `.wh.<f>`
    /// and `trusted.overlay.opaque=y` for `.wh..wh..opq`.
    #[test]
    fn test_layer_overlay_whiteout_and_opaque() {
        let layer = build_tar_with_dirs(&[
            TarEntry::Dir("etc/"),
            TarEntry::File("etc/keep", b"k"),
            TarEntry::Whiteout("etc/.wh.removed"),
            TarEntry::Dir("opq/"),
            TarEntry::Whiteout("opq/.wh..wh..opq"),
            TarEntry::File("opq/new", b"n"),
        ]);
        let opts = ConvertOptions {
            convert_backslash: false,
            writer_options: vec![WriterOption::Uuid([0x42u8; 16]), WriterOption::Journal(1024)],
        };
        let image = convert_layer_to_ext4(Cursor::new(layer), Cursor::new(Vec::new()), &opts)
            .unwrap()
            .into_inner();

        let mut reader = crate::reader::Reader::new(Cursor::new(&image)).unwrap();
        let entries = reader.walk().unwrap();
        let by = |p: &str| entries.iter().find(|e| e.path == p);

        assert!(by("etc/keep").is_some(), "normal file preserved");
        let wh = by("etc/removed").expect("whiteout present as a node");
        assert_eq!(wh.mode & format::TYPE_MASK, format::S_IFCHR, "whiteout is a char device");
        assert_eq!((wh.devmajor, wh.devminor), (0, 0), "whiteout rdev is 0,0");
        let opq = by("opq").expect("opaque dir present");
        assert_eq!(
            opq.xattrs.get(OVERLAY_OPAQUE_XATTR).map(Vec::as_slice),
            Some(b"y".as_slice()),
            "opaque dir carries trusted.overlay.opaque=y",
        );
        // The whiteout marker itself must not survive as a regular file.
        assert!(by("etc/.wh.removed").is_none());
        assert!(by("opq/.wh..wh..opq").is_none());
    }

    /// Single-layer overlay conversion is byte-deterministic for a fixed UUID —
    /// the property that lets a shared layer dedup across images.
    #[test]
    fn test_layer_overlay_is_byte_deterministic() {
        let opts = ConvertOptions {
            convert_backslash: false,
            writer_options: vec![
                WriterOption::MaximumDiskSize(64 * 1024 * 1024),
                WriterOption::Uuid([0x7u8; 16]),
                WriterOption::Journal(1024),
            ],
        };
        let make = || {
            let layer = build_tar_with_dirs(&[
                TarEntry::Dir("a/"),
                TarEntry::File("a/f", b"contents"),
                TarEntry::Whiteout("a/.wh.gone"),
            ]);
            convert_layer_to_ext4(Cursor::new(layer), Cursor::new(Vec::new()), &opts)
                .unwrap()
                .into_inner()
        };
        assert_eq!(make(), make(), "same layer + same UUID must be byte-identical");
    }

    /// A different UUID must change the bytes (proving the UUID genuinely flows
    /// into the image) while everything else stays fixed — so reproducibility
    /// depends solely on pinning the UUID, which `bless` now derives
    /// deterministically from the manifest digest.
    #[test]
    fn test_uuid_controls_output_bytes() {
        let with_a = {
            let raw = representative_layers();
            let mut layers: Vec<Cursor<Vec<u8>>> = raw.into_iter().map(Cursor::new).collect();
            let opts = ConvertOptions {
                convert_backslash: false,
                writer_options: vec![WriterOption::Uuid([0x11u8; 16])],
            };
            convert_oci_layers_to_ext4(&mut layers, Cursor::new(Vec::new()), &opts)
                .unwrap()
                .into_inner()
        };
        let with_b = {
            let raw = representative_layers();
            let mut layers: Vec<Cursor<Vec<u8>>> = raw.into_iter().map(Cursor::new).collect();
            let opts = ConvertOptions {
                convert_backslash: false,
                writer_options: vec![WriterOption::Uuid([0x22u8; 16])],
            };
            convert_oci_layers_to_ext4(&mut layers, Cursor::new(Vec::new()), &opts)
                .unwrap()
                .into_inner()
        };

        assert_eq!(with_a.len(), with_b.len(), "only the UUID changed; layout is identical");
        assert_ne!(with_a, with_b, "the UUID must actually flow into the image bytes");
    }
}
