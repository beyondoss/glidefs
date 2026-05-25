#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
/// Core ext4 writer — deterministic, sequential, write-only.
///
/// Ported from: github.com/Microsoft/hcsshim/ext4/internal/compactext4
///
/// Produces identical output bytes for identical input, which is critical
/// for content-addressing with BLAKE3 block hashes.
use std::collections::BTreeMap;
use std::io::{self, BufWriter, Read, Seek, Write};

use crate::format::{
    self, FileType, InodeFlags, InodeNumber, DIR_ENTRY_HEADER_SIZE,
    INODE_DATA_SIZE, INODE_EXTRA_SIZE, INODE_FIRST, INODE_SIZE,
    INODE_USED_SIZE, SMALL_SYMLINK_SIZE, TYPE_MASK, XATTR_BLOCK_OVERHEAD,
    XATTR_HEADER_MAGIC, XATTR_INODE_OVERHEAD,
};

// Re-export for backward compatibility (tests import from writer)
pub use crate::format::{BLOCK_SIZE, INLINE_DATA_SIZE};

// ---- Constants ----

#[allow(clippy::cast_possible_truncation)]
const BLOCKS_PER_GROUP: u32 = (BLOCK_SIZE * 8) as u32; // 4096*8=32768 fits in u32
#[allow(clippy::cast_possible_truncation)]
const MAX_INODES_PER_GROUP: u32 = (BLOCK_SIZE * 8) as u32; // 4096*8=32768 fits in u32
#[allow(clippy::cast_possible_truncation)]
const INODES_PER_GROUP_INCREMENT: u32 = (BLOCK_SIZE as u32) / (INODE_SIZE as u32); // 4096/256=16 fits in u32
const DEFAULT_MAX_DISK_SIZE: i64 = 16 * 1024 * 1024 * 1024; // 16 GiB
const MAX_MAX_DISK_SIZE: i64 = 16 * 1024 * 1024 * 1024 * 1024; // 16 TiB
#[allow(clippy::cast_possible_truncation)]
const GROUPS_PER_DESCRIPTOR_BLOCK: u32 = (BLOCK_SIZE as u32) / format::GROUP_DESCRIPTOR_SIZE as u32; // 4096/32=128 fits in u32
const MAX_FILE_SIZE: i64 = 128 * 1024 * 1024 * 1024; // 128 GiB
const MAX_BLOCKS_PER_EXTENT: u32 = 0x8000;
const INODE_LOST_AND_FOUND: u32 = INODE_FIRST;
#[allow(clippy::cast_possible_truncation)]
const EXTRA_ISIZE: u16 = (INODE_USED_SIZE - 128) as u16; // 152-128=24 fits in u16

// ---- Public types ----

/// A file to be added to the ext4 filesystem.
#[derive(Default)]
pub struct File {
    pub linkname: String,
    pub size: i64,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub atime: u64,
    pub ctime: u64,
    pub mtime: u64,
    pub crtime: u64,
    pub devmajor: u32,
    pub devminor: u32,
    pub xattrs: BTreeMap<String, Vec<u8>>,
}


/// Option for configuring the Writer.
#[derive(Clone)]
pub enum WriterOption {
    InlineData,
    MaximumDiskSize(i64),
    /// Set the filesystem UUID (16 bytes). Also used as hash_seed and journal_uuid.
    Uuid([u8; 16]),
    /// Create an internal journal with the given size in 4 KiB blocks.
    /// Typical values: 1024 (4 MiB), 4096 (16 MiB), 16384 (64 MiB).
    Journal(u32),
}

// ---- Internal inode ----

struct Inode {
    #[allow(dead_code)] // Stored for debugging; accessed via Vec index
    number: InodeNumber,
    size: i64,
    mode: u16,
    uid: u32,
    gid: u32,
    atime: u64,
    ctime: u64,
    mtime: u64,
    crtime: u64,
    link_count: u32,
    xattr_block: u32,
    block_count: u32,
    devmajor: u32,
    devminor: u32,
    flags: InodeFlags,
    data: Vec<u8>,
    xattr_inline: Vec<u8>,
    children: BTreeMap<String, InodeNumber>,
}

impl Inode {
    fn file_type(&self) -> u16 {
        self.mode & TYPE_MASK
    }

    fn is_dir(&self) -> bool {
        self.file_type() == format::S_IFDIR
    }
}

// ---- Xattr helpers ----

use crate::format::{compress_xattr_name, get_xattrs};

fn hash_xattr_entry(name: &str, value: &[u8]) -> u32 {
    let mut hash: u32 = 0;
    for &b in name.as_bytes() {
        hash = (hash << 5) ^ (hash >> 27) ^ u32::from(b);
    }
    let mut i = 0;
    while i + 3 < value.len() {
        hash = (hash << 16) ^ (hash >> 16) ^ u32::from_le_bytes([value[i], value[i+1], value[i+2], value[i+3]]);
        i += 4;
    }
    if !value.len().is_multiple_of(4) {
        let start = value.len() & !3;
        let mut last = [0u8; 4];
        last[..value.len() - start].copy_from_slice(&value[start..]);
        hash = (hash << 16) ^ (hash >> 16) ^ u32::from_le_bytes(last);
    }
    hash
}

struct Xattr {
    name: String,
    index: u8,
    value: Vec<u8>,
}

impl Xattr {
    fn entry_len(&self) -> usize {
        ((self.name.len() + 3) & !3usize) + 16
    }

    fn value_len(&self) -> usize {
        (self.value.len() + 3) & !3usize
    }
}

struct XattrState {
    inode_attrs: Vec<Xattr>,
    block_attrs: Vec<Xattr>,
    inode_left: usize,
    block_left: usize,
}

impl XattrState {
    fn new() -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let block_left = BLOCK_SIZE as usize - XATTR_BLOCK_OVERHEAD; // usize >= u64 on 64-bit
        Self {
            inode_attrs: Vec::new(),
            block_attrs: Vec::new(),
            inode_left: INODE_EXTRA_SIZE - XATTR_INODE_OVERHEAD,
            block_left,
        }
    }

    fn add_xattr(&mut self, name: &str, value: &[u8]) -> bool {
        let (index, compressed_name) = compress_xattr_name(name);
        let x = Xattr {
            index,
            name: compressed_name.to_string(),
            value: value.to_vec(),
        };
        let length = x.entry_len() + x.value_len();
        if self.inode_left >= length {
            self.inode_left -= length;
            self.inode_attrs.push(x);
            true
        } else if self.block_left >= length {
            self.block_left -= length;
            self.block_attrs.push(x);
            true
        } else {
            false
        }
    }
}

fn put_xattrs(xattrs: &[Xattr], buf: &mut [u8], offset_delta: u16) {
    #[allow(clippy::cast_possible_truncation)]
    let buf_len = buf.len() as u16; // xattr buffers fit in u16
    let mut offset = buf_len + offset_delta;
    let mut entry_pos = 0usize;
    let mut data_end = buf.len();
    for xattr in xattrs {
        let vl = xattr.value_len();
        #[allow(clippy::cast_possible_truncation)]
        let vl_u16 = vl as u16; // xattr value length fits in u16
        offset -= vl_u16;
        #[allow(clippy::cast_possible_truncation)]
        let name_len = xattr.name.len() as u8; // compressed xattr names fit in u8
        buf[entry_pos] = name_len;
        buf[entry_pos + 1] = xattr.index;
        buf[entry_pos + 2..entry_pos + 4].copy_from_slice(&offset.to_le_bytes());
        // bytes 4..8 = value_inum (0)
        buf[entry_pos + 4..entry_pos + 8].copy_from_slice(&0u32.to_le_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let value_len = xattr.value.len() as u32; // single xattr values fit in u32
        buf[entry_pos + 8..entry_pos + 12].copy_from_slice(&value_len.to_le_bytes());
        buf[entry_pos + 12..entry_pos + 16].copy_from_slice(&hash_xattr_entry(&xattr.name, &xattr.value).to_le_bytes());
        let name_bytes = xattr.name.as_bytes();
        buf[entry_pos + 16..entry_pos + 16 + name_bytes.len()].copy_from_slice(name_bytes);
        entry_pos += xattr.entry_len();
        data_end -= vl;
        buf[data_end..data_end + xattr.value.len()].copy_from_slice(&xattr.value);
    }
}


// ---- Writer ----

/// Writes a compact, deterministic ext4 filesystem.
///
/// All paths use '/' as separator. Create files with `create()`, write data
/// with the `Write` trait, then call `close()` to finalize.
pub struct Writer<W: Read + Write + Seek> {
    f: BufWriter<W>,
    inodes: Vec<Option<Inode>>,
    cur_name: String,
    cur_inode: Option<usize>, // index into inodes
    pos: i64,
    data_written: i64,
    data_max: i64,
    initialized: bool,
    support_inline_data: bool,
    max_disk_size: i64,
    gd_blocks: u32,
    uuid: [u8; 16],
    journal_blocks: u32,
}

impl<W: Read + Write + Seek> Writer<W> {
    pub fn new(f: W, opts: &[WriterOption]) -> Self {
        let mut w = Writer {
            f: BufWriter::with_capacity(65536 * 8, f),
            inodes: Vec::new(),
            cur_name: String::new(),
            cur_inode: None,
            pos: 0,
            data_written: 0,
            data_max: 0,
            initialized: false,
            support_inline_data: false,
            max_disk_size: DEFAULT_MAX_DISK_SIZE,
            gd_blocks: 0,
            uuid: [0u8; 16],
            journal_blocks: 0,
        };
        for opt in opts {
            match opt {
                WriterOption::InlineData => w.support_inline_data = true,
                WriterOption::MaximumDiskSize(size) => {
                    if *size < 0 || *size > MAX_MAX_DISK_SIZE {
                        w.max_disk_size = MAX_MAX_DISK_SIZE;
                    } else if *size == 0 {
                        w.max_disk_size = DEFAULT_MAX_DISK_SIZE;
                    } else {
                        #[allow(clippy::cast_possible_truncation)]
                        let block_size = BLOCK_SIZE as i64; // BLOCK_SIZE (4096) fits in i64
                        w.max_disk_size = (*size + block_size - 1) & !(block_size - 1);
                    }
                }
                WriterOption::Uuid(u) => w.uuid = *u,
                WriterOption::Journal(blocks) => w.journal_blocks = *blocks,
            }
        }
        w
    }

    // ---- low-level I/O ----

    fn write_bytes(&mut self, b: &[u8]) -> io::Result<usize> {
        #[allow(clippy::cast_possible_truncation)]
        let b_len = b.len() as i64; // data sizes fit in i64
        if self.pos + b_len > self.max_disk_size {
            return Err(io::Error::other(
                format!("disk exceeded maximum size of {} bytes", self.max_disk_size),
            ));
        }
        let n = self.f.write(b)?;
        #[allow(clippy::cast_possible_truncation)]
        let n_i64 = n as i64; // write result fits in i64
        self.pos += n_i64;
        Ok(n)
    }

    fn write_zeros(&mut self, n: i64) -> io::Result<()> {
        if self.pos + n > self.max_disk_size {
            return Err(io::Error::other(
                format!("disk exceeded maximum size of {} bytes", self.max_disk_size),
            ));
        }
        let zeros = [0u8; 4096];
        let mut remaining = n;
        while remaining > 0 {
            #[allow(clippy::cast_possible_truncation)]
            let zeros_len = zeros.len() as i64; // 4096 fits in i64
            #[allow(clippy::cast_possible_truncation)]
            let chunk = remaining.min(zeros_len) as usize; // chunk fits in usize
            self.f.write_all(&zeros[..chunk])?;
            #[allow(clippy::cast_possible_truncation)]
            let chunk_i64 = chunk as i64; // chunk fits in i64
            self.pos += chunk_i64;
            remaining -= chunk_i64;
        }
        Ok(())
    }

    fn block(&self) -> u32 {
        (self.pos / BLOCK_SIZE as i64) as u32
    }

    fn seek_block(&mut self, block: u32) -> io::Result<()> {
        self.pos = i64::from(block) * BLOCK_SIZE as i64;
        self.f.flush()?;
        self.f.seek(io::SeekFrom::Start(self.pos as u64))?;
        Ok(())
    }

    fn next_block(&mut self) -> io::Result<()> {
        let rem = self.pos % BLOCK_SIZE as i64;
        if rem != 0 {
            self.write_zeros(BLOCK_SIZE as i64 - rem)?;
        }
        Ok(())
    }

    // ---- inode management ----

    fn get_inode(&self, i: InodeNumber) -> Option<&Inode> {
        if i == 0 || i as usize > self.inodes.len() {
            return None;
        }
        self.inodes[(i - 1) as usize].as_ref()
    }

    fn root_number(&self) -> InodeNumber {
        format::INODE_ROOT
    }

    fn find_path(&self, root: InodeNumber, p: &str) -> Option<InodeNumber> {
        let mut current = root;
        let mut remaining = p;
        while !remaining.is_empty() {
            let (name, rest) = split_first(remaining);
            remaining = rest;
            let inode = self.get_inode(current)?;
            current = *inode.children.get(name)?;
        }
        Some(current)
    }

    fn lookup(&self, name: &str, must_exist: bool) -> io::Result<(InodeNumber, Option<InodeNumber>, String)> {
        let root = self.root_number();
        let cleanname = clean_path(name);
        if cleanname.is_empty() {
            return Ok((root, Some(root), String::new()));
        }
        let (dirname, childname) = path_split(&cleanname);
        if childname.is_empty() || childname.len() > 0xff {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("{name}: invalid name")));
        }
        let dir_ino = self.find_path(root, dirname)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{name}: path not found")))?;
        let dir = self.get_inode(dir_ino)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{name}: path not found")))?;
        if !dir.is_dir() {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("{name}: path not found")));
        }
        let child_ino = dir.children.get(childname).copied();
        if child_ino.is_none() && must_exist {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("{name}: file not found")));
        }
        Ok((dir_ino, child_ino, childname.to_string()))
    }

    fn alloc_inode_number(&self) -> InodeNumber {
        self.inodes.len() as u32 + 1
    }

    fn make_inode(&mut self, f: &File, reuse_ino: Option<InodeNumber>) -> io::Result<InodeNumber> {
        let mut mode = f.mode;
        if mode & TYPE_MASK == 0 {
            mode |= format::S_IFREG;
        }
        let typ = mode & TYPE_MASK;

        let ino: InodeNumber;
        let is_new: bool;
        if let Some(existing_ino) = reuse_ino {
            let existing = self.inodes[(existing_ino - 1) as usize].as_ref().unwrap();
            if existing.flags.contains(InodeFlags::EXTENTS) {
                return Err(io::Error::other(
                    "cannot overwrite file with non-inline data",
                ));
            }
            ino = existing_ino;
            is_new = false;
        } else {
            ino = self.alloc_inode_number();
            is_new = true;
        }

        // Preserve children and link_count for directory reuse
        let (children, link_count) = if !is_new {
            let existing = self.inodes[(ino - 1) as usize].as_ref().unwrap();
            (existing.children.clone(), existing.link_count)
        } else if typ == format::S_IFDIR {
            (BTreeMap::new(), 1u32) // directory linked to itself
        } else {
            (BTreeMap::new(), 0u32)
        };

        let mut xstate = XattrState::new();

        let mut size: i64 = 0;
        let mut data: Vec<u8> = Vec::new();
        let mut flags = InodeFlags::empty();

        match typ {
            format::S_IFREG => {
                flags |= InodeFlags::HUGE_FILE;
                size = f.size;
                if f.size > MAX_FILE_SIZE {
                    return Err(io::Error::other(
                        format!("file too big: {} > {MAX_FILE_SIZE}", f.size),
                    ));
                }
                if f.size <= INLINE_DATA_SIZE as i64 && self.support_inline_data {
                    data = vec![0u8; f.size as usize];
                    let extra = if f.size > INODE_DATA_SIZE as i64 {
                        (f.size - INODE_DATA_SIZE as i64) as usize
                    } else {
                        0
                    };
                    // Add dummy entry — will be rewritten in writeInodeTable
                    let dummy = vec![0u8; extra];
                    if !xstate.add_xattr("system.data", &dummy) {
                        return Err(io::Error::other(
                            "not enough room for inline data",
                        ));
                    }
                    flags |= InodeFlags::INLINE_DATA;
                }
            }
            format::S_IFLNK => {
                flags |= InodeFlags::HUGE_FILE;
                mode |= 0o777; // symlinks should appear as ugw rwx
                size = f.linkname.len() as i64;
                if (size as usize) <= SMALL_SYMLINK_SIZE {
                    data = f.linkname.as_bytes().to_vec();
                }
            }
            format::S_IFDIR => {
                flags |= InodeFlags::HUGE_FILE;
            }
            format::S_IFIFO | format::S_IFSOCK | format::S_IFCHR | format::S_IFBLK => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid mode {mode:#o}"),
                ));
            }
        }

        // Caller's xattrs are authoritative — no merge with existing.
        // Callers that need to preserve existing xattrs should use stat() + merge + create().

        // Accumulate xattrs (BTreeMap iteration is sorted)
        for (name, value) in &f.xattrs {
            if !xstate.add_xattr(name, value) {
                return Err(io::Error::other(
                    format!("could not fit xattr {name}"),
                ));
            }
        }

        // Build xattr inline/block data
        let xattr_inline = if !xstate.inode_attrs.is_empty() {
            let mut buf = vec![0u8; INODE_EXTRA_SIZE];
            buf[..4].copy_from_slice(&XATTR_HEADER_MAGIC.to_le_bytes());
            put_xattrs(&xstate.inode_attrs, &mut buf[4..], 0);
            buf
        } else {
            Vec::new()
        };

        // Handle block xattrs
        let old_xattr_block = if !is_new {
            self.inodes[(ino - 1) as usize].as_ref().unwrap().xattr_block
        } else {
            0
        };
        let old_block_count = if !is_new {
            self.inodes[(ino - 1) as usize].as_ref().unwrap().block_count
        } else {
            0
        };

        let mut xattr_block = old_xattr_block;
        let mut block_count = if is_new { 0 } else { old_block_count };

        if !xstate.block_attrs.is_empty() || xattr_block != 0 {
            // Sort block xattrs: by (index, name.len(), name)
            xstate.block_attrs.sort_by(|a, b| {
                a.index.cmp(&b.index)
                    .then_with(|| a.name.len().cmp(&b.name.len()))
                    .then_with(|| a.name.cmp(&b.name))
            });

            let mut b = vec![0u8; BLOCK_SIZE as usize];
            b[..4].copy_from_slice(&XATTR_HEADER_MAGIC.to_le_bytes());
            b[4..8].copy_from_slice(&1u32.to_le_bytes()); // reference_count
            b[8..12].copy_from_slice(&1u32.to_le_bytes()); // blocks
            put_xattrs(&xstate.block_attrs, &mut b[32..], 32);

            let orig = self.block();
            if xattr_block == 0 {
                xattr_block = orig;
                block_count += 1;
                self.write_bytes(&b)?;
            } else {
                // Rewrite existing block
                self.seek_block(xattr_block)?;
                self.write_bytes(&b)?;
                self.seek_block(orig)?;
            }
        }

        // Now create/update the inode
        let inode = Inode {
            number: ino,
            size,
            mode,
            uid: f.uid,
            gid: f.gid,
            atime: f.atime,
            ctime: f.ctime,
            mtime: f.mtime,
            crtime: f.crtime,
            link_count,
            xattr_block,
            block_count,
            devmajor: f.devmajor,
            devminor: f.devminor,
            flags,
            data,
            xattr_inline,
            children,
        };

        if is_new {
            self.inodes.push(Some(inode));
        } else {
            self.inodes[(ino - 1) as usize] = Some(inode);
        }

        // Handle large symlinks (data written as file content)
        if typ == format::S_IFLNK && (size as usize) > SMALL_SYMLINK_SIZE {
            self.start_inode("", ino, size)?;
            self.write_data(f.linkname.as_bytes())?;
            self.finish_inode()?;
        }

        Ok(ino)
    }

    fn start_inode(&mut self, name: &str, ino: InodeNumber, size: i64) -> io::Result<()> {
        if self.cur_inode.is_some() {
            return Err(io::Error::other(
                "inode already in progress",
            ));
        }
        self.cur_name = name.to_string();
        self.cur_inode = Some((ino - 1) as usize);
        self.data_written = 0;
        self.data_max = size;
        Ok(())
    }

    fn finish_inode(&mut self) -> io::Result<()> {
        if !self.initialized {
            self.init()?;
        }
        let idx = match self.cur_inode {
            Some(idx) => idx,
            None => return Ok(()),
        };
        if self.data_written != self.data_max {
            return Err(io::Error::other(
                format!("did not write the right amount: {} != {}", self.data_written, self.data_max),
            ));
        }
        let has_inline = self.inodes[idx].as_ref().unwrap().flags.contains(InodeFlags::INLINE_DATA);
        if self.data_max != 0 && !has_inline {
            self.write_extents(idx)?;
        }
        self.data_written = 0;
        self.data_max = 0;
        self.cur_inode = None;
        Ok(())
    }

    fn write_extents(&mut self, idx: usize) -> io::Result<()> {
        let start = self.pos - self.data_written;
        if start % BLOCK_SIZE as i64 != 0 {
            return Err(io::Error::other(
                "data start position is not block-aligned",
            ));
        }
        self.next_block()?;

        let start_block = (start / BLOCK_SIZE as i64) as u32;
        let blocks = self.block() - start_block;
        let mut used_blocks = blocks;

        const EXTENT_NODE_SIZE: u32 = 12;
        const EXTENTS_PER_BLOCK: u32 = (BLOCK_SIZE as u32) / EXTENT_NODE_SIZE - 1;

        let extents = if blocks == 0 { 0 } else { blocks.div_ceil(MAX_BLOCKS_PER_EXTENT) };
        let mut data = Vec::new();

        if extents == 0 {
            // Nothing to do
        } else if extents <= 4 {
            // Fits in inode directly
            write_extent_header_to_vec(&mut data, extents as u16, 4, 0);
            for i in 0..extents {
                let block_offset = i * MAX_BLOCKS_PER_EXTENT;
                let mut length = blocks - block_offset;
                if length > MAX_BLOCKS_PER_EXTENT {
                    length = MAX_BLOCKS_PER_EXTENT;
                }
                write_extent_leaf_to_vec(&mut data, block_offset, length as u16, start_block + block_offset);
            }
            // Pad to 4 extents worth
            let padding = (4 - extents) * EXTENT_NODE_SIZE;
            data.extend(std::iter::repeat_n(0u8, padding as usize));
        } else if extents <= 4 * EXTENTS_PER_BLOCK {
            let extent_blocks = extents.div_ceil(EXTENTS_PER_BLOCK);
            used_blocks += extent_blocks;

            // Root: index nodes
            write_extent_header_to_vec(&mut data, extent_blocks as u16, 4, 1);
            // We'll fill in the index nodes after writing the leaf blocks
            let index_start = data.len();
            data.resize(index_start + 4 * EXTENT_NODE_SIZE as usize, 0);

            for i in 0..extent_blocks {
                let leaf_block = self.block();
                // Fill in the index node
                let idx_off = index_start + (i * EXTENT_NODE_SIZE) as usize;
                let block_off = i * EXTENTS_PER_BLOCK * MAX_BLOCKS_PER_EXTENT;
                data[idx_off..idx_off + 4].copy_from_slice(&block_off.to_le_bytes());
                data[idx_off + 4..idx_off + 8].copy_from_slice(&leaf_block.to_le_bytes());
                // idx_off + 8..12 stays zero (leaf_high + unused)

                let extents_in_block = (extents - i * EXTENTS_PER_BLOCK).min(EXTENTS_PER_BLOCK);
                let mut leaf_buf = vec![0u8; BLOCK_SIZE as usize];
                let mut leaf_pos = 0usize;

                // Write extent header
                leaf_buf[leaf_pos..leaf_pos + 2].copy_from_slice(&format::EXTENT_HEADER_MAGIC.to_le_bytes());
                leaf_pos += 2;
                leaf_buf[leaf_pos..leaf_pos + 2].copy_from_slice(&(extents_in_block as u16).to_le_bytes());
                leaf_pos += 2;
                leaf_buf[leaf_pos..leaf_pos + 2].copy_from_slice(&(EXTENTS_PER_BLOCK as u16).to_le_bytes());
                leaf_pos += 2;
                leaf_buf[leaf_pos..leaf_pos + 2].copy_from_slice(&0u16.to_le_bytes()); // depth
                leaf_pos += 2;
                leaf_buf[leaf_pos..leaf_pos + 4].copy_from_slice(&0u32.to_le_bytes()); // generation
                leaf_pos += 4;

                let offset = i * EXTENTS_PER_BLOCK * MAX_BLOCKS_PER_EXTENT;
                for j in 0..extents_in_block {
                    let block_off2 = offset + j * MAX_BLOCKS_PER_EXTENT;
                    let mut length = blocks - block_off2;
                    if length > MAX_BLOCKS_PER_EXTENT {
                        length = MAX_BLOCKS_PER_EXTENT;
                    }
                    let start = start_block + block_off2;
                    leaf_buf[leaf_pos..leaf_pos + 4].copy_from_slice(&block_off2.to_le_bytes());
                    leaf_pos += 4;
                    leaf_buf[leaf_pos..leaf_pos + 2].copy_from_slice(&(length as u16).to_le_bytes());
                    leaf_pos += 2;
                    leaf_buf[leaf_pos..leaf_pos + 2].copy_from_slice(&0u16.to_le_bytes()); // start_high
                    leaf_pos += 2;
                    leaf_buf[leaf_pos..leaf_pos + 4].copy_from_slice(&start.to_le_bytes());
                    leaf_pos += 4;
                }

                self.write_bytes(&leaf_buf)?;
            }
        } else {
            return Err(io::Error::other(
                "file too large for two-level extent tree",
            ));
        }

        let inode = self.inodes[idx].as_mut().unwrap();
        inode.data = data;
        inode.flags |= InodeFlags::EXTENTS;
        inode.block_count += used_blocks;
        Ok(())
    }

    // ---- init ----

    fn init(&mut self) -> io::Result<()> {
        // Skip the defective block inode (slot 0 = inode 1)
        self.inodes = Vec::with_capacity(32);
        self.inodes.push(None);

        // Create root directory (inode 2)
        let root_file = File {
            mode: format::S_IFDIR | 0o755,
            ..Default::default()
        };
        let root_ino = self.make_inode(&root_file, None)?;
        // Root is linked to itself
        self.inodes[(root_ino - 1) as usize].as_mut().unwrap().link_count += 1;

        // Pad to INODE_FIRST - 1, so the next make_inode gets inode INODE_FIRST.
        // (matches Go: append(w.inodes, make([]*inode, inodeFirst-len(w.inodes)-1)...))
        while self.inodes.len() < (INODE_FIRST - 1) as usize {
            self.inodes.push(None);
        }

        let max_blocks = ((self.max_disk_size - 1) / BLOCK_SIZE as i64 + 1) as u64;
        let max_groups = ((max_blocks - 1) / u64::from(BLOCKS_PER_GROUP) + 1) as u32;
        self.gd_blocks = (max_groups - 1) / GROUPS_PER_DESCRIPTOR_BLOCK + 1;

        // Skip past superblock and group descriptor table
        self.seek_block(1 + self.gd_blocks)?;
        self.initialized = true;

        // lost+found is required for e2fsck
        let lf_file = File {
            mode: format::S_IFDIR | 0o700,
            ..Default::default()
        };
        self.create("lost+found", &lf_file)?;
        Ok(())
    }

    // ---- public API ----

    /// Ensure parent directories exist (like `mkdir -p`).
    pub fn make_parents(&mut self, name: &str) -> io::Result<()> {
        self.finish_inode()?;
        let cleanname = clean_path(name);
        let (parent_dirs, _) = path_split(&cleanname);
        if parent_dirs.is_empty() {
            return Ok(());
        }

        let root_ino = self.root_number();
        let mut current_ino = root_ino;
        let mut current_path = String::new();
        let mut remaining = parent_dirs;

        while !remaining.is_empty() {
            let (dirname, rest) = split_first(remaining);
            remaining = rest;
            current_path.push('/');
            current_path.push_str(dirname);

            let parent = self.get_inode(current_ino).unwrap();
            if let Some(&child_ino) = parent.children.get(dirname) {
                current_ino = child_ino;
                continue;
            }

            // Copy root's metadata for the new directory
            let root = self.get_inode(root_ino).unwrap();
            let f = File {
                mode: root.mode,
                atime: root.atime,
                mtime: root.mtime,
                ctime: root.ctime,
                crtime: root.crtime,
                uid: root.uid,
                gid: root.gid,
                devmajor: root.devmajor,
                devminor: root.devminor,
                ..Default::default()
            };
            self.create(&current_path, &f)?;

            // Track the newly created directory's inode
            let (_, new_ino, _) = self.lookup(&current_path, true)?;
            current_ino = new_ino.unwrap();
        }
        Ok(())
    }

    /// Add a file to the filesystem.
    pub fn create(&mut self, name: &str, f: &File) -> io::Result<()> {
        self.finish_inode()?;
        let (dir_ino, existing_ino, childname) = self.lookup(name, false)?;

        let reuse_ino: Option<InodeNumber>;
        if let Some(existing) = existing_ino {
            let existing_inode = self.get_inode(existing).unwrap();
            if existing_inode.is_dir() {
                if f.mode & TYPE_MASK != format::S_IFDIR {
                    return Err(io::Error::other(
                        format!("{name}: cannot replace a directory with a file"),
                    ));
                }
                reuse_ino = Some(existing);
            } else if f.mode & TYPE_MASK == format::S_IFDIR {
                return Err(io::Error::other(
                    format!("{name}: cannot replace a file with a directory"),
                ));
            } else if existing_inode.link_count < 2 {
                reuse_ino = Some(existing);
            } else {
                reuse_ino = None;
            }
        } else {
            if f.mode & TYPE_MASK == format::S_IFDIR {
                let dir = self.get_inode(dir_ino).unwrap();
                if dir.link_count >= format::MAX_LINKS {
                    return Err(io::Error::other(
                        format!("{name}: exceeded parent directory maximum link count"),
                    ));
                }
            }
            reuse_ino = None;
        }

        let child_ino = self.make_inode(f, reuse_ino)?;

        if existing_ino != Some(child_ino) {
            if let Some(existing) = existing_ino {
                self.inodes[(existing - 1) as usize].as_mut().unwrap().link_count -= 1;
            }
            let dir = self.inodes[(dir_ino - 1) as usize].as_mut().unwrap();
            dir.children.insert(childname, child_ino);
            self.inodes[(child_ino - 1) as usize].as_mut().unwrap().link_count += 1;
            if self.inodes[(child_ino - 1) as usize].as_ref().unwrap().is_dir() {
                self.inodes[(dir_ino - 1) as usize].as_mut().unwrap().link_count += 1;
            }
        }

        if self.inodes[(child_ino - 1) as usize].as_ref().unwrap().mode & TYPE_MASK == format::S_IFREG {
            self.start_inode(name, child_ino, f.size)?;
        }
        Ok(())
    }

    /// Create a hard link.
    pub fn link(&mut self, oldname: &str, newname: &str) -> io::Result<()> {
        self.finish_inode()?;
        let (newdir_ino, existing_ino, newchildname) = self.lookup(newname, false)?;
        if let Some(existing) = existing_ino {
            let ex = self.get_inode(existing).unwrap();
            if ex.is_dir() || ex.link_count < 2 {
                return Err(io::Error::other(
                    format!("{newname}: cannot orphan existing file or directory"),
                ));
            }
        }

        let (_, old_ino, _) = self.lookup(oldname, true)?;
        let old_ino = old_ino.unwrap();
        let old_file = self.get_inode(old_ino).unwrap();
        if old_file.mode & TYPE_MASK == format::S_IFDIR {
            return Err(io::Error::other(
                format!("{newname}: link target cannot be a directory: {oldname}"),
            ));
        }
        if existing_ino != Some(old_ino) && old_file.link_count >= format::MAX_LINKS {
            return Err(io::Error::other(
                format!("{newname}: link target would exceed maximum link count: {oldname}"),
            ));
        }

        if let Some(existing) = existing_ino {
            self.inodes[(existing - 1) as usize].as_mut().unwrap().link_count -= 1;
        }
        self.inodes[(old_ino - 1) as usize].as_mut().unwrap().link_count += 1;
        self.inodes[(newdir_ino - 1) as usize].as_mut().unwrap().children.insert(newchildname, old_ino);
        Ok(())
    }

    /// Read back file metadata.
    ///
    /// For symlinks with targets longer than 59 bytes, the link target cannot
    /// be retrieved (it was written as file data in a forward-only pass).
    /// In that case, `linkname` will be empty and an error is returned.
    pub fn stat(&mut self, name: &str) -> io::Result<File> {
        self.finish_inode()?;
        let (_, node_ino, _) = self.lookup(name, true)?;
        let node_ino = node_ino.unwrap();
        let node = self.get_inode(node_ino).unwrap();
        let mut f = File {
            size: node.size,
            mode: node.mode,
            uid: node.uid,
            gid: node.gid,
            atime: node.atime,
            ctime: node.ctime,
            mtime: node.mtime,
            crtime: node.crtime,
            devmajor: node.devmajor,
            devminor: node.devminor,
            ..Default::default()
        };

        if node.xattr_block != 0 {
            let xattr_block = node.xattr_block;
            let orig = self.block();
            self.seek_block(xattr_block)?;
            let mut b = vec![0u8; BLOCK_SIZE as usize];
            self.f.get_mut().read_exact(&mut b)?;
            self.seek_block(orig)?;
            get_xattrs(&b[32..], &mut f.xattrs, 32);
        }
        let node = self.get_inode(node_ino).unwrap();
        if !node.xattr_inline.is_empty() {
            get_xattrs(&node.xattr_inline[4..], &mut f.xattrs, 0);
            f.xattrs.remove("system.data");
        }
        if node.file_type() == format::S_IFLNK {
            if node.size > SMALL_SYMLINK_SIZE as i64 {
                return Err(io::Error::other(
                    format!("{name}: cannot retrieve link information"),
                ));
            }
            f.linkname = String::from_utf8_lossy(&node.data).to_string();
        }
        Ok(f)
    }

    /// Write file data. Only valid after `create()` on a regular file.
    pub fn write_data(&mut self, b: &[u8]) -> io::Result<usize> {
        if b.is_empty() {
            return Ok(0);
        }
        if self.data_written + b.len() as i64 > self.data_max {
            return Err(io::Error::other(
                format!(
                    "{}: wrote too much: {} > {}",
                    self.cur_name,
                    self.data_written + b.len() as i64,
                    self.data_max
                ),
            ));
        }
        let idx = self.cur_inode.unwrap();
        let has_inline = self.inodes[idx].as_ref().unwrap().flags.contains(InodeFlags::INLINE_DATA);
        if has_inline {
            let inode = self.inodes[idx].as_mut().unwrap();
            let start = self.data_written as usize;
            inode.data[start..start + b.len()].copy_from_slice(b);
            self.data_written += b.len() as i64;
            Ok(b.len())
        } else {
            let n = self.write_bytes(b)?;
            self.data_written += n as i64;
            Ok(n)
        }
    }

    // ---- directory writing ----

    const DIR_ZEROS: [u8; BLOCK_SIZE as usize] = [0u8; BLOCK_SIZE as usize];

    fn write_directory(&mut self, dir_ino: InodeNumber, parent_ino: InodeNumber) -> io::Result<()> {
        self.finish_inode()?;
        // Start with max size; we'll fix it later
        self.start_inode("", dir_ino, 0x7fffffffffffffff)?;

        let mut left = BLOCK_SIZE as usize;

        // Collect entries: ".", "..", then children sorted by (inode_number, name)
        let dir = self.get_inode(dir_ino).unwrap();
        let children: Vec<(String, InodeNumber)> = dir.children.iter()
            .map(|(name, &ino)| (name.clone(), ino))
            .collect();

        // Sort children by (inode_number, name)
        let mut sorted_children = children;
        sorted_children.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        // Write "." and ".."
        let entries: Vec<(InodeNumber, String)> = std::iter::once((dir_ino, ".".to_string()))
            .chain(std::iter::once((parent_ino, "..".to_string())))
            .chain(sorted_children.into_iter().map(|(name, ino)| (ino, name)))
            .collect();

        for (ino, name) in &entries {
            let rlb = DIR_ENTRY_HEADER_SIZE + name.len();
            let rl = (rlb + 3) & !3;
            if left < rl + 12 {
                // Finish current block
                self.finish_dir_block(left)?;
                left = BLOCK_SIZE as usize;
            }
            let file_type = self.get_inode(*ino)
                .map(|i| format::mode_to_file_type(i.mode))
                .unwrap_or(FileType::Unknown);
            self.write_dir_entry(*ino, rl as u16, name.len() as u8, file_type)?;
            self.write_data(name.as_bytes())?;
            let padding = rl - rlb;
            if padding > 0 {
                self.write_data(&Self::DIR_ZEROS[..padding])?;
            }
            left -= rl;
        }

        // Finish the last block
        self.finish_dir_block(left)?;

        // Fix up the inode's size
        let idx = self.cur_inode.unwrap();
        let written = self.data_written;
        self.inodes[idx].as_mut().unwrap().size = written;
        self.data_max = written;
        Ok(())
    }

    fn finish_dir_block(&mut self, left: usize) -> io::Result<()> {
        if left > 0 {
            // Write a trailing empty entry
            self.write_dir_entry(0, left as u16, 0, FileType::Unknown)?;
            let padding = left - DIR_ENTRY_HEADER_SIZE;
            if padding < 4 {
                return Err(io::Error::other(
                    "not enough space for trailing directory entry",
                ));
            }
            self.write_data(&Self::DIR_ZEROS[..padding])?;
        }
        Ok(())
    }

    fn write_dir_entry(&mut self, inode: InodeNumber, rec_len: u16, name_len: u8, file_type: FileType) -> io::Result<()> {
        let mut buf = [0u8; DIR_ENTRY_HEADER_SIZE];
        buf[0..4].copy_from_slice(&inode.to_le_bytes());
        buf[4..6].copy_from_slice(&rec_len.to_le_bytes());
        buf[6] = name_len;
        buf[7] = file_type as u8;
        self.write_data(&buf)?;
        Ok(())
    }

    fn write_directory_recursive(&mut self, dir_ino: InodeNumber, parent_ino: InodeNumber) -> io::Result<()> {
        self.write_directory(dir_ino, parent_ino)?;

        // Collect children that are directories, sorted by (inode_number, name)
        let dir = self.get_inode(dir_ino).unwrap();
        let mut dir_children: Vec<(String, InodeNumber)> = dir.children.iter()
            .filter_map(|(name, &ino)| {
                self.get_inode(ino).and_then(|i| {
                    if i.is_dir() { Some((name.clone(), ino)) } else { None }
                })
            })
            .collect();
        dir_children.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        for (_, child_ino) in dir_children {
            self.write_directory_recursive(child_ino, dir_ino)?;
        }
        Ok(())
    }

    // ---- journal ----

    /// Write journal blocks and create journal inode (inode 8).
    ///
    /// Allocates `self.journal_blocks` contiguous blocks starting at the
    /// current position. The first block contains the JBD2 v2 superblock
    /// (empty/clean journal). Remaining blocks are zeros.
    ///
    /// Creates inode 8 (EXT4_JOURNAL_INO) with an extent pointing to the
    /// journal blocks. The superblock is updated in close() to set
    /// HAS_JOURNAL, journal_inum, and the journal_blocks backup.
    fn write_journal(&mut self) -> io::Result<()> {
        let journal_start = self.block();

        // Write JBD2 v2 superblock (first block of journal)
        // All multi-byte fields are big-endian per JBD2 spec.
        let mut jbd2_sb = [0u8; BLOCK_SIZE as usize];
        jbd2_sb[0..4].copy_from_slice(&format::JBD2_MAGIC.to_be_bytes());
        jbd2_sb[4..8].copy_from_slice(&format::JBD2_SUPERBLOCK_V2.to_be_bytes());
        // s_sequence = 1 (initial sequence number)
        jbd2_sb[8..12].copy_from_slice(&1u32.to_be_bytes());
        // s_blocksize
        jbd2_sb[12..16].copy_from_slice(&(BLOCK_SIZE as u32).to_be_bytes());
        // s_maxlen (total journal blocks)
        jbd2_sb[16..20].copy_from_slice(&self.journal_blocks.to_be_bytes());
        // s_first = 1 (first usable block, after this superblock)
        jbd2_sb[20..24].copy_from_slice(&1u32.to_be_bytes());
        // s_start = 0, s_sequence = 0 at offset 24-28 → clean journal (already zeros)
        // s_uuid (offset 48..64)
        jbd2_sb[48..64].copy_from_slice(&self.uuid);
        // s_nr_users = 1 (offset 64..68)
        jbd2_sb[64..68].copy_from_slice(&1u32.to_be_bytes());
        self.write_bytes(&jbd2_sb)?;

        // Zero-fill remaining journal blocks
        self.write_zeros((i64::from(self.journal_blocks) - 1) * BLOCK_SIZE as i64)?;

        // Build extent data for journal inode: single extent covering all journal blocks
        let mut extent_data = [0u8; INODE_DATA_SIZE];
        // Extent header (12 bytes): magic, entries=1, max=4, depth=0
        extent_data[0..2].copy_from_slice(&format::EXTENT_HEADER_MAGIC.to_le_bytes());
        extent_data[2..4].copy_from_slice(&1u16.to_le_bytes()); // entries
        extent_data[4..6].copy_from_slice(&4u16.to_le_bytes()); // max
        // depth=0, generation=0 (already zeros at bytes 6..12)
        // Extent leaf at offset 12 (12 bytes):
        //   ee_block(u32) + ee_len(u16) + ee_start_hi(u16) + ee_start_lo(u32)
        // ee_block = 0 (logical block 0, already zeros at 12..16)
        extent_data[16..18].copy_from_slice(&(self.journal_blocks as u16).to_le_bytes()); // ee_len
        // ee_start_hi = 0 (already zeros at 18..20)
        extent_data[20..24].copy_from_slice(&journal_start.to_le_bytes()); // ee_start_lo

        // Create journal inode at index 7 (inode 8)
        let journal_size = i64::from(self.journal_blocks) * BLOCK_SIZE as i64;
        self.inodes[7] = Some(Inode {
            number: format::INODE_JOURNAL,
            size: journal_size,
            mode: format::S_IFREG | 0o600,
            uid: 0,
            gid: 0,
            atime: 0,
            ctime: 0,
            mtime: 0,
            crtime: 0,
            link_count: 1,
            xattr_block: 0,
            block_count: self.journal_blocks * (BLOCK_SIZE as u32 / 512),
            devmajor: 0,
            devminor: 0,
            flags: InodeFlags::EXTENTS,
            data: extent_data.to_vec(),
            xattr_inline: Vec::new(),
            children: BTreeMap::new(),
        });

        Ok(())
    }

    /// Build the journal_blocks backup array for the superblock.
    ///
    /// The superblock stores the first 15 u32s of the journal inode's block[]
    /// field (extent data) plus the inode size and block count, for recovery.
    fn journal_blocks_backup(&self) -> [u32; 17] {
        let mut backup = [0u32; 17];
        if self.journal_blocks == 0 {
            return backup;
        }
        if let Some(Some(journal_inode)) = self.inodes.get(7) {
            // First 15 entries: the 60-byte block[] field as 15 u32s (extent data)
            for (i, slot) in backup.iter_mut().enumerate().take(15) {
                let off = i * 4;
                if off + 4 <= journal_inode.data.len() {
                    *slot = u32::from_le_bytes(
                        journal_inode.data[off..off + 4].try_into().unwrap(),
                    );
                }
            }
            // Entry 15: inode size (low 32 bits)
            backup[15] = journal_inode.size as u32;
            // Entry 16: inode size (high 32 bits)
            backup[16] = (journal_inode.size >> 32) as u32;
        }
        backup
    }

    // ---- inode table ----

    fn write_inode_table(&mut self, table_size: u32) -> io::Result<()> {
        let inode_count = self.inodes.len();
        let mut buf = Vec::with_capacity(INODE_SIZE);
        let zero_inode = [0u8; INODE_SIZE];
        for i in 0..inode_count {
            buf.clear();
            match &self.inodes[i] {
                Some(inode) => {
                    let mut block_data = [0u8; INODE_DATA_SIZE];
                    let typ = inode.mode & TYPE_MASK;
                    match typ {
                        format::S_IFDIR | format::S_IFREG | format::S_IFLNK => {
                            let n = inode.data.len().min(INODE_DATA_SIZE);
                            block_data[..n].copy_from_slice(&inode.data[..n]);
                            // If data overflows into xattr_inline, rewrite the first xattr
                            if inode.data.len() > INODE_DATA_SIZE {
                                let overflow = &inode.data[INODE_DATA_SIZE..];
                                let x = Xattr {
                                    name: "data".to_string(),
                                    index: 7, // "system."
                                    value: overflow.to_vec(),
                                };
                                if !inode.xattr_inline.is_empty() {
                                    let mut inline = inode.xattr_inline.clone();
                                    put_xattrs(&[x], &mut inline[4..], 0);
                                    format::write_inode(
                                        &mut buf,
                                        inode.mode,
                                        inode.uid,
                                        inode.gid,
                                        inode.size,
                                        inode.atime,
                                        inode.ctime,
                                        inode.mtime,
                                        inode.crtime,
                                        inode.link_count,
                                        inode.block_count,
                                        inode.flags,
                                        inode.xattr_block,
                                        &block_data,
                                        &inline,
                                    )?;
                                    self.write_bytes(&buf)?;
                                    continue;
                                }
                            }
                        }
                        format::S_IFBLK | format::S_IFCHR => {
                            let dev = (inode.devminor & 0xff)
                                | (inode.devmajor << 8)
                                | ((inode.devminor & 0xffffff00) << 12);
                            block_data[4..8].copy_from_slice(&dev.to_le_bytes());
                        }
                        _ => {}
                    }
                    format::write_inode(
                        &mut buf,
                        inode.mode,
                        inode.uid,
                        inode.gid,
                        inode.size,
                        inode.atime,
                        inode.ctime,
                        inode.mtime,
                        inode.crtime,
                        inode.link_count,
                        inode.block_count,
                        inode.flags,
                        inode.xattr_block,
                        &block_data,
                        &inode.xattr_inline,
                    )?;
                    self.write_bytes(&buf)?;
                }
                None => {
                    self.write_bytes(&zero_inode)?;
                }
            }
        }
        let rest = table_size - (inode_count as u32) * (INODE_SIZE as u32);
        self.write_zeros(i64::from(rest))?;
        Ok(())
    }

    // ---- finalize ----

    /// Finalize the filesystem. Must be called after all files are added.
    #[must_use = "close() finalizes the filesystem; the returned writer contains the complete image"]
    pub fn close(mut self) -> io::Result<W> {
        self.finish_inode()?;

        let root = self.root_number();
        self.write_directory_recursive(root, root)?;
        self.finish_inode()?;

        // Write journal blocks (if enabled)
        if self.journal_blocks > 0 {
            self.write_journal()?;
        }

        // Write the inode table
        let inode_table_offset = self.block();
        let (groups, inodes_per_group) = best_group_count(inode_table_offset, self.inodes.len() as u32);
        self.write_inode_table(groups * inodes_per_group * INODE_SIZE as u32)?;

        // Write bitmaps
        let bitmap_offset = self.block();
        let bitmap_size = groups * 2;
        let valid_data_size = bitmap_offset + bitmap_size;
        let mut disk_size = valid_data_size;
        let min_size = (groups - 1) * BLOCKS_PER_GROUP + 1;
        if disk_size < min_size {
            disk_size = min_size;
        }

        let used_gd_blocks = (groups - 1) / GROUPS_PER_DESCRIPTOR_BLOCK + 1;
        if used_gd_blocks > self.gd_blocks {
            return Err(io::Error::other(
                format!("disk exceeded maximum size of {} bytes", self.max_disk_size),
            ));
        }

        let gd_count = (self.gd_blocks * GROUPS_PER_DESCRIPTOR_BLOCK) as usize;
        let mut gds = vec![format::GroupDescriptor::default(); gd_count];
        let inode_table_size_per_group = inodes_per_group * INODE_SIZE as u32 / BLOCK_SIZE as u32;
        let mut total_used_blocks: u32 = 0;
        let mut total_used_inodes: u32 = 0;

        for g in 0..groups {
            let mut bitmap_buf = vec![0u8; BLOCK_SIZE as usize * 2];
            let mut dir_count: u16 = 0;
            let mut used_inode_count: u16 = 0;
            let mut used_block_count: u16 = 0;

            // Block bitmap
            if (g + 1) * BLOCKS_PER_GROUP <= valid_data_size {
                for b in &mut bitmap_buf[..BLOCK_SIZE as usize] {
                    *b = 0xff;
                }
                used_block_count = BLOCKS_PER_GROUP as u16;
            } else if g * BLOCKS_PER_GROUP < valid_data_size {
                for j in 0..(valid_data_size - g * BLOCKS_PER_GROUP) {
                    bitmap_buf[(j / 8) as usize] |= 1 << (j % 8);
                    used_block_count += 1;
                }
            }
            if g == 0 {
                // Clear unused group descriptor blocks
                for j in (1 + used_gd_blocks)..(1 + self.gd_blocks) {
                    bitmap_buf[(j / 8) as usize] &= !(1 << (j % 8));
                    used_block_count -= 1;
                }
            }
            if g == groups - 1 && !disk_size.is_multiple_of(BLOCKS_PER_GROUP) {
                for j in (disk_size % BLOCKS_PER_GROUP)..BLOCKS_PER_GROUP {
                    bitmap_buf[(j / 8) as usize] |= 1 << (j % 8);
                    used_block_count += 1;
                }
            }

            // Inode bitmap
            for j in 0..inodes_per_group {
                let ino: InodeNumber = 1 + g * inodes_per_group + j;
                let inode = self.get_inode(ino);
                if ino < INODE_FIRST || inode.is_some() {
                    bitmap_buf[BLOCK_SIZE as usize + (j / 8) as usize] |= 1 << (j % 8);
                    used_inode_count += 1;
                }
                if let Some(inode) = inode
                    && inode.mode & TYPE_MASK == format::S_IFDIR {
                        dir_count += 1;
                    }
            }
            // Padding: bits beyond inodes_per_group must be set to 1
            for j in inodes_per_group..(BLOCK_SIZE as u32 * 8) {
                bitmap_buf[BLOCK_SIZE as usize + (j / 8) as usize] |= 1 << (j % 8);
            }

            self.write_bytes(&bitmap_buf)?;

            gds[g as usize] = format::GroupDescriptor {
                block_bitmap_low: bitmap_offset + 2 * g,
                inode_bitmap_low: bitmap_offset + 2 * g + 1,
                inode_table_low: inode_table_offset + g * inode_table_size_per_group,
                used_dirs_count_low: dir_count,
                free_inodes_count_low: inodes_per_group as u16 - used_inode_count,
                free_blocks_count_low: BLOCKS_PER_GROUP as u16 - used_block_count,
                ..Default::default()
            };

            total_used_blocks += u32::from(used_block_count);
            total_used_inodes += u32::from(used_inode_count);
        }

        // Zero up to disk size
        let zero_blocks = disk_size - bitmap_offset - bitmap_size;
        self.write_zeros(i64::from(zero_blocks) * BLOCK_SIZE as i64)?;

        // Write group descriptors at block 1
        self.seek_block(1)?;
        for gd in &gds {
            gd.write_to(&mut self.f)?;
        }
        self.pos = i64::from(1 + self.gd_blocks) * BLOCK_SIZE as i64;

        // Write superblock at block 0 (offset 1024)
        let total_inodes = inodes_per_group * groups;
        let sb = format::SuperBlock {
            inodes_count: total_inodes,
            blocks_count_low: disk_size,
            free_blocks_count_low: BLOCKS_PER_GROUP * groups - total_used_blocks,
            free_inodes_count: total_inodes - total_used_inodes,
            first_data_block: 0,
            log_block_size: 2, // 2^(10+2) = 4096
            log_cluster_size: 2,
            blocks_per_group: BLOCKS_PER_GROUP,
            clusters_per_group: BLOCKS_PER_GROUP,
            inodes_per_group,
            magic: format::SUPER_BLOCK_MAGIC,
            state: 1,  // cleanly unmounted
            errors: 1, // continue on error
            creator_os: 0, // Linux
            revision_level: 1, // dynamic inode sizes
            first_inode: INODE_FIRST,
            lpf_inode: INODE_LOST_AND_FOUND,
            inode_size: INODE_SIZE as u16,
            feature_compat: format::CompatFeature::EXT_ATTR
                | if self.journal_blocks > 0 { format::CompatFeature::HAS_JOURNAL } else { format::CompatFeature::empty() },
            feature_incompat: format::IncompatFeature::FILETYPE
                | format::IncompatFeature::EXTENTS
                | format::IncompatFeature::FLEX_BG
                | if self.support_inline_data { format::IncompatFeature::INLINE_DATA } else { format::IncompatFeature::empty() },
            feature_ro_compat: format::RoCompatFeature::SPARSE_SUPER
                | format::RoCompatFeature::LARGE_FILE
                | format::RoCompatFeature::HUGE_FILE
                | format::RoCompatFeature::EXTRA_ISIZE,
            uuid: self.uuid,
            journal_uuid: self.uuid,
            journal_inum: if self.journal_blocks > 0 { format::INODE_JOURNAL } else { 0 },
            hash_seed: [
                u32::from_le_bytes(self.uuid[0..4].try_into().unwrap()),
                u32::from_le_bytes(self.uuid[4..8].try_into().unwrap()),
                u32::from_le_bytes(self.uuid[8..12].try_into().unwrap()),
                u32::from_le_bytes(self.uuid[12..16].try_into().unwrap()),
            ],
            journal_blocks: self.journal_blocks_backup(),
            min_extra_isize: EXTRA_ISIZE,
            want_extra_isize: EXTRA_ISIZE,
            log_groups_per_flex: 31,
            ..Default::default()
        };

        // Write block 0: 1024 bytes padding + 1024 byte superblock + rest zeros
        self.seek_block(0)?;
        let mut blk = vec![0u8; BLOCK_SIZE as usize];
        let mut sb_buf = Vec::with_capacity(1024);
        sb.write_to(&mut sb_buf)?;
        assert_eq!(sb_buf.len(), 1024, "superblock must serialize to exactly 1024 bytes");
        blk[1024..1024 + sb_buf.len()].copy_from_slice(&sb_buf);
        self.write_bytes(&blk)?;

        // Seek to end
        self.seek_block(disk_size)?;

        self.f.flush()?;
        self.f.into_inner().map_err(|e| e.into_error())
    }
}

// Implement std::io::Write so callers can use io::copy
impl<W: Read + Write + Seek> io::Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_data(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.f.flush()
    }
}

// ---- Helper functions ----

fn split_first(p: &str) -> (&str, &str) {
    match p.find('/') {
        Some(n) => (&p[..n], &p[n + 1..]),
        None => (p, ""),
    }
}

/// Equivalent to Go's path.Clean("/" + name)[1:]
fn clean_path(name: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for part in name.split('/') {
        match part {
            "" | "." => {}
            ".." => { parts.pop(); }
            _ => parts.push(part),
        }
    }
    parts.join("/")
}

/// Split a clean path into (directory, basename).
fn path_split(cleanname: &str) -> (&str, &str) {
    match cleanname.rfind('/') {
        Some(n) => (&cleanname[..n + 1], &cleanname[n + 1..]),
        None => ("", cleanname),
    }
}

fn group_count(blocks: u32, inodes: u32, inodes_per_group: u32) -> u32 {
    let inode_blocks_per_group = inodes_per_group * INODE_SIZE as u32 / BLOCK_SIZE as u32;
    let data_blocks_per_group = BLOCKS_PER_GROUP - inode_blocks_per_group - 2; // room for bitmaps

    let mut blocks = blocks;
    let min_blocks = if inodes > 0 {
        (inodes - 1) / inodes_per_group * data_blocks_per_group + 1
    } else {
        1
    };
    if blocks < min_blocks {
        blocks = min_blocks;
    }
    blocks.div_ceil(data_blocks_per_group)
}

fn best_group_count(blocks: u32, inodes: u32) -> (u32, u32) {
    let mut best_groups: u32 = 0xffffffff;
    let mut best_ipg: u32 = INODES_PER_GROUP_INCREMENT;
    let mut ipg = INODES_PER_GROUP_INCREMENT;
    while ipg <= MAX_INODES_PER_GROUP {
        let g = group_count(blocks, inodes, ipg);
        if g < best_groups {
            best_groups = g;
            best_ipg = ipg;
        }
        ipg += INODES_PER_GROUP_INCREMENT;
    }
    (best_groups, best_ipg)
}

fn write_extent_header_to_vec(buf: &mut Vec<u8>, entries: u16, max: u16, depth: u16) {
    buf.extend_from_slice(&format::EXTENT_HEADER_MAGIC.to_le_bytes());
    buf.extend_from_slice(&entries.to_le_bytes());
    buf.extend_from_slice(&max.to_le_bytes());
    buf.extend_from_slice(&depth.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // generation
}

fn write_extent_leaf_to_vec(buf: &mut Vec<u8>, block: u32, length: u16, start: u32) {
    buf.extend_from_slice(&block.to_le_bytes());
    buf.extend_from_slice(&length.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // start_high
    buf.extend_from_slice(&start.to_le_bytes());
}
