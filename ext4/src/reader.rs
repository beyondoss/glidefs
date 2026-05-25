/// ext4 filesystem reader — parse an ext4 image and extract files.
///
/// Counterpart to the writer: reads superblock, group descriptors, inode table,
/// extent trees, directory entries, and xattrs from a seekable ext4 image.
///
/// Used by the OCI bridge export path: GlideFS blocks → ext4 reader → tar stream.
use std::collections::{BTreeMap, HashSet};
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::format::{
    self, DirEntry, ExtentHeader, ExtentIndex, ExtentLeaf, GroupDescriptor, InodeFlags,
    InodeNumber, ParsedInode, SuperBlock, BLOCK_SIZE, EXTENT_HEADER_MAGIC, EXTENT_NODE_SIZE,
    GROUP_DESCRIPTOR_SIZE, INODE_DATA_SIZE, INODE_FIRST, INODE_ROOT, INODE_SIZE,
    SMALL_SYMLINK_SIZE, TYPE_MASK, XATTR_HEADER_MAGIC,
};

/// Read-only ext4 filesystem.
pub struct Reader<R: Read + Seek> {
    inner: R,
    sb: SuperBlock,
    group_descs: Vec<GroupDescriptor>,
}

impl<R: Read + Seek> Reader<R> {
    /// Open an ext4 image for reading. Parses the superblock and group descriptors.
    pub fn new(mut inner: R) -> io::Result<Self> {
        // Read superblock at offset 1024
        inner.seek(SeekFrom::Start(1024))?;
        let mut sb_buf = [0u8; 1024];
        inner.read_exact(&mut sb_buf)?;
        let sb = SuperBlock::read_from(&sb_buf)?;

        // Validate block size
        let block_size = 1024u64 << sb.log_block_size;
        if block_size != BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported block size: {block_size} (expected {BLOCK_SIZE})"),
            ));
        }

        if sb.inodes_per_group == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inodes_per_group is zero",
            ));
        }

        // Read group descriptors from block 1
        let gd_size = if sb.desc_size > 0 && sb.desc_size as usize > GROUP_DESCRIPTOR_SIZE {
            sb.desc_size as usize
        } else {
            GROUP_DESCRIPTOR_SIZE
        };
        let num_groups = sb.inodes_count.div_ceil(sb.inodes_per_group);
        inner.seek(SeekFrom::Start(BLOCK_SIZE))?;

        let mut group_descs = Vec::with_capacity(num_groups as usize);
        for _ in 0..num_groups {
            let mut gd_buf = vec![0u8; gd_size];
            inner.read_exact(&mut gd_buf)?;
            group_descs.push(GroupDescriptor::read_from(&gd_buf)?);
        }

        Ok(Reader { inner, sb, group_descs })
    }

    /// Read a single 4096-byte block.
    fn read_block(&mut self, block_num: u64) -> io::Result<[u8; BLOCK_SIZE as usize]> {
        self.inner.seek(SeekFrom::Start(block_num * BLOCK_SIZE))?;
        let mut buf = [0u8; BLOCK_SIZE as usize];
        self.inner.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read `len` bytes starting at a byte offset.
    fn read_bytes_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.inner.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        self.inner.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Compute the byte offset of an inode in the image.
    fn inode_offset(&self, ino: InodeNumber) -> io::Result<u64> {
        if ino == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "inode 0 is invalid"));
        }
        let group = ((ino - 1) / self.sb.inodes_per_group) as usize;
        if group >= self.group_descs.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("inode {ino} is in group {group} but only {} groups exist", self.group_descs.len()),
            ));
        }
        let index = (ino - 1) % self.sb.inodes_per_group;
        let table_block = u64::from(self.group_descs[group].inode_table_low);
        Ok(table_block * BLOCK_SIZE + u64::from(index) * INODE_SIZE as u64)
    }

    /// Read and parse an inode by number.
    pub fn read_inode(&mut self, ino: InodeNumber) -> io::Result<ParsedInode> {
        let offset = self.inode_offset(ino)?;
        let buf = self.read_bytes_at(offset, INODE_SIZE)?;
        ParsedInode::read_from(&buf)
    }

    /// Resolve an extent tree into a list of (physical_block, length) pairs.
    fn resolve_extents(&mut self, block_data: &[u8; INODE_DATA_SIZE]) -> io::Result<Vec<(u64, u32)>> {
        let header = ExtentHeader::read_from(block_data);
        if header.magic != EXTENT_HEADER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad extent header magic: 0x{:04x}", header.magic),
            ));
        }

        if header.depth == 0 {
            // Leaf nodes directly in inode
            let max_entries = (INODE_DATA_SIZE - EXTENT_NODE_SIZE) / EXTENT_NODE_SIZE;
            if header.entries as usize > max_entries {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "extent header claims {} entries but inode data fits at most {}",
                        header.entries, max_entries,
                    ),
                ));
            }
            let mut extents = Vec::with_capacity(header.entries as usize);
            for i in 0..header.entries as usize {
                let off = EXTENT_NODE_SIZE + i * EXTENT_NODE_SIZE;
                let leaf = ExtentLeaf::read_from(&block_data[off..]);
                extents.push((leaf.start, u32::from(leaf.length)));
            }
            Ok(extents)
        } else {
            // Index nodes — read each leaf block
            let max_index_entries = (INODE_DATA_SIZE - EXTENT_NODE_SIZE) / EXTENT_NODE_SIZE;
            if header.entries as usize > max_index_entries {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "extent index claims {} entries but inode data fits at most {}",
                        header.entries, max_index_entries,
                    ),
                ));
            }
            let mut extents = Vec::new();
            for i in 0..header.entries as usize {
                let off = EXTENT_NODE_SIZE + i * EXTENT_NODE_SIZE;
                let index = ExtentIndex::read_from(&block_data[off..]);
                let leaf_block = self.read_block(index.leaf)?;
                let leaf_header = ExtentHeader::read_from(&leaf_block);
                if leaf_header.magic != EXTENT_HEADER_MAGIC {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bad extent leaf header magic",
                    ));
                }
                if leaf_header.depth != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "extent leaf block has depth {} (expected 0)",
                            leaf_header.depth,
                        ),
                    ));
                }
                let max_leaf_entries =
                    (BLOCK_SIZE as usize - EXTENT_NODE_SIZE) / EXTENT_NODE_SIZE;
                if leaf_header.entries as usize > max_leaf_entries {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "extent leaf claims {} entries but block fits at most {}",
                            leaf_header.entries, max_leaf_entries,
                        ),
                    ));
                }
                for j in 0..leaf_header.entries as usize {
                    let loff = EXTENT_NODE_SIZE + j * EXTENT_NODE_SIZE;
                    let leaf = ExtentLeaf::read_from(&leaf_block[loff..]);
                    extents.push((leaf.start, u32::from(leaf.length)));
                }
            }
            Ok(extents)
        }
    }

    /// Read file data for an inode. Returns the complete file content.
    pub fn read_data(&mut self, inode: &ParsedInode) -> io::Result<Vec<u8>> {
        if inode.size == 0 {
            return Ok(Vec::new());
        }

        if inode.flags.contains(InodeFlags::INLINE_DATA) {
            return self.read_inline_data(inode);
        }

        let mut data = Vec::with_capacity(inode.size as usize);
        let mut reader = self.extent_reader(inode)?;
        reader.read_to_end(&mut data)?;
        Ok(data)
    }

    /// Stream file data to a writer without buffering the full file.
    pub fn read_data_to<W: Write>(&mut self, inode: &ParsedInode, w: &mut W) -> io::Result<u64> {
        if inode.size == 0 {
            return Ok(0);
        }

        if inode.flags.contains(InodeFlags::INLINE_DATA) {
            let data = self.read_inline_data(inode)?;
            w.write_all(&data)?;
            return Ok(data.len() as u64);
        }

        let mut reader = self.extent_reader(inode)?;
        io::copy(&mut reader, w)
    }

    /// Create a streaming `ExtentReader` for an extent-based inode.
    fn extent_reader(&mut self, inode: &ParsedInode) -> io::Result<ExtentReader<'_, R>> {
        if !inode.flags.contains(InodeFlags::EXTENTS) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "only extent-based inodes are supported",
            ));
        }
        let extents = self.resolve_extents(&inode.block)?;
        Ok(ExtentReader::new(&mut self.inner, extents, inode.size))
    }

    /// Read inline data from inode block area + xattr overflow.
    fn read_inline_data(&mut self, inode: &ParsedInode) -> io::Result<Vec<u8>> {
        let size = inode.size as usize;
        let mut data = Vec::with_capacity(size);

        // First part: from inode block[0..60]
        let first = size.min(INODE_DATA_SIZE);
        data.extend_from_slice(&inode.block[..first]);

        // Overflow: from "system.data" xattr in extra inode space
        if size > INODE_DATA_SIZE {
            // The xattr inline area starts with a 4-byte magic, then xattr entries.
            // The "system.data" xattr holds the overflow bytes.
            let magic = u32::from_le_bytes([
                inode.xattr_inline[0], inode.xattr_inline[1],
                inode.xattr_inline[2], inode.xattr_inline[3],
            ]);
            if magic == XATTR_HEADER_MAGIC {
                let mut xattrs = BTreeMap::new();
                format::get_xattrs(&inode.xattr_inline[4..], &mut xattrs, 0);
                if let Some(overflow) = xattrs.get("system.data") {
                    data.extend_from_slice(overflow);
                }
            }
        }

        data.truncate(size);
        Ok(data)
    }

    /// Read directory entries from a directory inode. Skips "." and "..".
    pub fn read_dir(&mut self, inode: &ParsedInode) -> io::Result<Vec<DirEntry>> {
        let data = self.read_data(inode)?;
        let mut entries = Vec::new();
        let mut pos = 0usize;

        while pos < data.len() {
            if let Some(entry) = DirEntry::read_from(&data[pos..]) {
                let rec_len = entry.rec_len as usize;
                if entry.name != "." && entry.name != ".." {
                    entries.push(entry);
                }
                if rec_len == 0 {
                    break;
                }
                pos += rec_len;
            } else {
                // Padding entry — advance by rec_len
                if pos + 6 <= data.len() {
                    let rec_len = format::le_u16(&data, pos + 4) as usize;
                    if rec_len == 0 {
                        break;
                    }
                    pos += rec_len;
                } else {
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// Read xattrs for an inode (both inline and block xattrs).
    pub fn read_xattrs(&mut self, inode: &ParsedInode) -> io::Result<BTreeMap<String, Vec<u8>>> {
        let mut xattrs = BTreeMap::new();

        // Inline xattrs from extra inode space
        let magic = u32::from_le_bytes([
            inode.xattr_inline[0], inode.xattr_inline[1],
            inode.xattr_inline[2], inode.xattr_inline[3],
        ]);
        if magic == XATTR_HEADER_MAGIC {
            format::get_xattrs(&inode.xattr_inline[4..], &mut xattrs, 0);
        }

        // Block xattrs
        if inode.xattr_block_low != 0 {
            let block = self.read_block(u64::from(inode.xattr_block_low))?;
            let block_magic = u32::from_le_bytes([block[0], block[1], block[2], block[3]]);
            if block_magic == XATTR_HEADER_MAGIC {
                format::get_xattrs(&block[32..], &mut xattrs, 32);
            }
        }

        // Filter out the internal "system.data" xattr (used for inline data overflow)
        xattrs.remove("system.data");

        Ok(xattrs)
    }

    /// Read a symlink target.
    pub fn read_symlink(&mut self, inode: &ParsedInode) -> io::Result<String> {
        let size = inode.size as usize;
        if size <= SMALL_SYMLINK_SIZE {
            // Small symlink: target stored inline in inode block area
            let target = std::str::from_utf8(&inode.block[..size])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(target.to_string())
        } else {
            // Large symlink: stored in extent data blocks
            let data = self.read_data(inode)?;
            let target = std::str::from_utf8(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(target.to_string())
        }
    }

    /// Decode a device number from the inode block area (for char/block devices).
    /// Matches Linux `new_decode_dev`: 12-bit major, 20-bit minor.
    fn decode_device(block: &[u8; INODE_DATA_SIZE]) -> (u32, u32) {
        let dev = u32::from_le_bytes([block[4], block[5], block[6], block[7]]);
        let major = (dev & 0xfff00) >> 8;
        let minor = (dev & 0xff) | ((dev >> 12) & 0xfff00);
        (major, minor)
    }

    /// Walk the entire filesystem tree from root. Returns all entries with full paths.
    pub fn walk(&mut self) -> io::Result<Vec<WalkEntry>> {
        let mut entries = Vec::new();
        let mut visited = HashSet::new();
        self.walk_recursive(INODE_ROOT, "", &mut entries, &mut visited)?;
        Ok(entries)
    }

    fn walk_recursive(
        &mut self,
        dir_ino: InodeNumber,
        prefix: &str,
        entries: &mut Vec<WalkEntry>,
        visited: &mut HashSet<InodeNumber>,
    ) -> io::Result<()> {
        if !visited.insert(dir_ino) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("directory cycle detected at inode {dir_ino}"),
            ));
        }
        let dir_inode = self.read_inode(dir_ino)?;
        let dir_entries = self.read_dir(&dir_inode)?;

        for entry in dir_entries {
            // Skip reserved inodes (except root)
            if entry.inode < INODE_FIRST && entry.inode != INODE_ROOT {
                continue;
            }

            let path = if prefix.is_empty() {
                entry.name.clone()
            } else {
                format!("{prefix}/{}", entry.name)
            };

            let inode = self.read_inode(entry.inode)?;
            let file_type = inode.file_type_bits();

            let symlink_target = if file_type == format::S_IFLNK {
                Some(self.read_symlink(&inode)?)
            } else {
                None
            };

            let (devmajor, devminor) = if file_type == format::S_IFCHR || file_type == format::S_IFBLK {
                Self::decode_device(&inode.block)
            } else {
                (0, 0)
            };

            let xattrs = self.read_xattrs(&inode)?;

            entries.push(WalkEntry {
                path: path.clone(),
                mode: inode.mode,
                uid: inode.uid,
                gid: inode.gid,
                size: inode.size,
                atime: inode.atime,
                ctime: inode.ctime,
                mtime: inode.mtime,
                links_count: inode.links_count,
                inode_number: entry.inode,
                symlink_target,
                devmajor,
                devminor,
                xattrs,
            });

            // Recurse into directories
            if file_type == format::S_IFDIR {
                self.walk_recursive(entry.inode, &path, entries, visited)?;
            }
        }

        Ok(())
    }

    /// Export the filesystem as a tar stream.
    pub fn to_tar<W: Write>(&mut self, w: W) -> io::Result<W> {
        let mut builder = tar::Builder::new(w);
        let entries = self.walk()?;
        let mut seen_inodes: BTreeMap<InodeNumber, String> = BTreeMap::new();

        for entry in &entries {
            self.write_entry_to_tar(entry, &mut builder, &mut seen_inodes)?;
        }

        builder.into_inner()
    }

    /// Write a single WalkEntry to a tar builder, streaming file data.
    ///
    /// Handles all file types (regular, directory, symlink, char/block device, FIFO)
    /// and hard link detection via the `seen_inodes` map.
    pub(crate) fn write_entry_to_tar<W: Write>(
        &mut self,
        entry: &WalkEntry,
        builder: &mut tar::Builder<W>,
        seen_inodes: &mut BTreeMap<InodeNumber, String>,
    ) -> io::Result<()> {
        let file_type = entry.mode & TYPE_MASK;

        // Hard link detection: if we've seen this inode before and it has links > 1
        if entry.links_count > 1 && file_type == format::S_IFREG {
            if let Some(first_path) = seen_inodes.get(&entry.inode_number) {
                let mut header = tar::Header::new_gnu();
                header.set_entry_type(tar::EntryType::Link);
                header.set_size(0);
                header.set_mode(u32::from(entry.mode & !TYPE_MASK));
                header.set_uid(u64::from(entry.uid));
                header.set_gid(u64::from(entry.gid));
                header.set_mtime(entry.mtime & 0xFFFFFFFF);
                header.set_cksum();
                builder.append_link(&mut header, &entry.path, first_path)?;
                return Ok(());
            }
            seen_inodes.insert(entry.inode_number, entry.path.clone());
        }

        let mut header = tar::Header::new_gnu();
        header.set_mode(u32::from(entry.mode & !TYPE_MASK));
        header.set_uid(u64::from(entry.uid));
        header.set_gid(u64::from(entry.gid));
        header.set_mtime(entry.mtime & 0xFFFFFFFF);

        match file_type {
            format::S_IFREG => {
                header.set_entry_type(tar::EntryType::Regular);
                header.set_size(entry.size);
                header.set_cksum();
                append_pax_xattrs(builder, &entry.xattrs)?;

                // Stream file data without buffering the entire file.
                if entry.size == 0 {
                    builder.append_data(&mut header, &entry.path, io::empty())?;
                } else {
                    let inode = self.read_inode(entry.inode_number)?;
                    if inode.flags.contains(InodeFlags::INLINE_DATA) {
                        let data = self.read_inline_data(&inode)?;
                        builder.append_data(&mut header, &entry.path, &data[..])?;
                    } else {
                        let mut reader = self.extent_reader(&inode)?;
                        builder.append_data(&mut header, &entry.path, &mut reader)?;
                    }
                }
            }
            format::S_IFDIR => {
                header.set_entry_type(tar::EntryType::Directory);
                header.set_size(0);
                header.set_cksum();
                append_pax_xattrs(builder, &entry.xattrs)?;
                let path = format!("{}/", entry.path);
                builder.append_data(&mut header, &path, io::empty())?;
            }
            format::S_IFLNK => {
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                if let Some(ref target) = entry.symlink_target {
                    header.set_link_name(target)?;
                }
                header.set_cksum();
                append_pax_xattrs(builder, &entry.xattrs)?;
                builder.append_data(&mut header, &entry.path, io::empty())?;
            }
            format::S_IFCHR => {
                header.set_entry_type(tar::EntryType::Char);
                header.set_size(0);
                header.set_device_major(entry.devmajor)?;
                header.set_device_minor(entry.devminor)?;
                header.set_cksum();
                append_pax_xattrs(builder, &entry.xattrs)?;
                builder.append_data(&mut header, &entry.path, io::empty())?;
            }
            format::S_IFBLK => {
                header.set_entry_type(tar::EntryType::Block);
                header.set_size(0);
                header.set_device_major(entry.devmajor)?;
                header.set_device_minor(entry.devminor)?;
                header.set_cksum();
                append_pax_xattrs(builder, &entry.xattrs)?;
                builder.append_data(&mut header, &entry.path, io::empty())?;
            }
            format::S_IFIFO => {
                header.set_entry_type(tar::EntryType::Fifo);
                header.set_size(0);
                header.set_cksum();
                append_pax_xattrs(builder, &entry.xattrs)?;
                builder.append_data(&mut header, &entry.path, io::empty())?;
            }
            _ => {} // Skip unknown types
        }

        Ok(())
    }

    /// Access the parsed superblock.
    pub fn superblock(&self) -> &SuperBlock {
        &self.sb
    }

    /// Consume the reader and return the inner reader.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

/// An entry from a filesystem walk.
#[derive(Debug)]
pub struct WalkEntry {
    pub path: String,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub atime: u64,
    pub ctime: u64,
    pub mtime: u64,
    pub links_count: u16,
    pub inode_number: InodeNumber,
    pub symlink_target: Option<String>,
    pub devmajor: u32,
    pub devminor: u32,
    pub xattrs: BTreeMap<String, Vec<u8>>,
}

/// Emit PAX xattr extensions into the tar stream, if any.
pub(crate) fn append_pax_xattrs<W: Write>(
    builder: &mut tar::Builder<W>,
    xattrs: &BTreeMap<String, Vec<u8>>,
) -> io::Result<()> {
    if xattrs.is_empty() {
        return Ok(());
    }
    let pax: Vec<(String, Vec<u8>)> = xattrs
        .iter()
        .map(|(k, v)| (format!("SCHILY.xattr.{k}"), v.clone()))
        .collect();
    builder.append_pax_extensions(
        pax.iter().map(|(k, v)| (k.as_str(), v.as_slice())),
    )
}

/// Streaming reader over extent-based file data. Reads blocks on demand
/// without buffering the entire file, suitable for large files in `to_tar`.
struct ExtentReader<'a, R: Read + Seek> {
    inner: &'a mut R,
    extents: Vec<(u64, u32)>,
    remaining: u64,
    extent_idx: usize,
    block_in_extent: u32,
    offset_in_block: usize,
    block_buf: [u8; BLOCK_SIZE as usize],
    block_loaded: bool,
}

impl<'a, R: Read + Seek> ExtentReader<'a, R> {
    fn new(inner: &'a mut R, extents: Vec<(u64, u32)>, size: u64) -> Self {
        ExtentReader {
            inner,
            extents,
            remaining: size,
            extent_idx: 0,
            block_in_extent: 0,
            offset_in_block: 0,
            block_buf: [0u8; BLOCK_SIZE as usize],
            block_loaded: false,
        }
    }
}

impl<R: Read + Seek> Read for ExtentReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 || self.extent_idx >= self.extents.len() {
            return Ok(0);
        }

        if !self.block_loaded {
            let (phys_block, _) = self.extents[self.extent_idx];
            let block_num = phys_block + u64::from(self.block_in_extent);
            self.inner.seek(SeekFrom::Start(block_num * BLOCK_SIZE))?;
            self.inner.read_exact(&mut self.block_buf)?;
            self.block_loaded = true;
            self.offset_in_block = 0;
        }

        let available = (BLOCK_SIZE as usize - self.offset_in_block)
            .min(self.remaining as usize);
        let n = buf.len().min(available);
        buf[..n].copy_from_slice(
            &self.block_buf[self.offset_in_block..self.offset_in_block + n],
        );
        self.offset_in_block += n;
        self.remaining -= n as u64;

        // Advance to next block if this one is consumed.
        if self.offset_in_block >= BLOCK_SIZE as usize {
            self.block_in_extent += 1;
            let (_, length) = self.extents[self.extent_idx];
            if self.block_in_extent >= length {
                self.extent_idx += 1;
                self.block_in_extent = 0;
            }
            self.block_loaded = false;
        }

        Ok(n)
    }
}
