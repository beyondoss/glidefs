/// tar-to-ext4 conversion.
///
/// Ported from: github.com/Microsoft/hcsshim/ext4/tar2ext4
///
/// Iterates tar entries and maps them to ext4 writer operations.
/// Handles OCI whiteouts, PAX xattr records, and all standard entry types.
use std::collections::BTreeMap;
use std::io::{self, Read, Seek, Write};

use crate::ext4::format;
use crate::ext4::writer::{File, Writer, WriterOption};

const WHITEOUT_PREFIX: &str = ".wh.";
const OPAQUE_WHITEOUT: &str = ".wh..wh..opq";

/// Options for tar-to-ext4 conversion.
#[derive(Default)]
pub struct ConvertOptions {
    pub convert_whiteout: bool,
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
        let header = entry.header().clone();

        let mut name = header.path()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_string_lossy()
            .to_string();

        let mut link_name = header.link_name()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        if options.convert_backslash {
            name = name.replace('\\', "/");
            link_name = link_name.replace('\\', "/");
        }

        fs.make_parents(&name)?;

        // Handle whiteouts
        if options.convert_whiteout {
            if let Some((dir, file)) = split_dir_file(&name) {
                if let Some(stripped) = file.strip_prefix(WHITEOUT_PREFIX) {
                    if file == OPAQUE_WHITEOUT {
                        // Update the directory with opaque xattr
                        let mut stat = fs.stat(dir)?;
                        stat.xattrs.insert("trusted.overlay.opaque".to_string(), b"y".to_vec());
                        fs.create(dir, &stat)?;
                    } else {
                        // Create overlay-style whiteout (char device 0,0)
                        let whiteout_path = if dir.is_empty() {
                            stripped.to_string()
                        } else {
                            format!("{dir}{stripped}")
                        };
                        let f = File {
                            mode: format::S_IFCHR,
                            devmajor: 0,
                            devminor: 0,
                            ..Default::default()
                        };
                        fs.create(&whiteout_path, &f)?;
                    }
                    continue;
                }
            }
        }

        let entry_type = header.entry_type();
        if entry_type == tar::EntryType::Link {
            fs.link(&link_name, &name)?;
        } else {
            let mode_bits = header.mode()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? as u16;

            let typ: u16 = match entry_type {
                tar::EntryType::Regular | tar::EntryType::GNUSparse => format::S_IFREG,
                tar::EntryType::Symlink => format::S_IFLNK,
                tar::EntryType::Char => format::S_IFCHR,
                tar::EntryType::Block => format::S_IFBLK,
                tar::EntryType::Directory => format::S_IFDIR,
                tar::EntryType::Fifo => format::S_IFIFO,
                _ => continue, // Skip unknown types
            };

            let uid = header.uid().unwrap_or(0) as u32;
            let gid = header.gid().unwrap_or(0) as u32;
            let size = header.size()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? as i64;
            let mtime = header.mtime().unwrap_or(0);
            let devmajor = header.device_major().ok().flatten().unwrap_or(0);
            let devminor = header.device_minor().ok().flatten().unwrap_or(0);

            // Extract PAX xattrs
            let mut xattrs = BTreeMap::new();
            if let Some(pax) = entry.pax_extensions()? {
                for ext in pax {
                    let ext = ext.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    if let Some(attr_name) = ext.key()
                        .ok()
                        .and_then(|k| k.strip_prefix("SCHILY.xattr."))
                    {
                        xattrs.insert(attr_name.to_string(), ext.value_bytes().to_vec());
                    }
                }
            }

            // Convert time: mtime as fs time (seconds in low 34 bits, 0 nanoseconds)
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
                linkname: link_name,
                devmajor,
                devminor,
                xattrs,
            };

            fs.create(&name, &f)?;

            // Copy file data
            if typ == format::S_IFREG && size > 0 {
                io::copy(&mut entry, &mut fs)?;
            }
        }
    }

    fs.close()
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
