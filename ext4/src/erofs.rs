#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap, clippy::cast_sign_loss)]
//! Hand-rolled **EROFS** (uncompressed) writer for content-addressed OCI images.
//!
//! EROFS is a read-only filesystem the in-kernel `erofs` driver mounts directly
//! over a block device — no userspace daemon. We emit a deterministic,
//! uncompressed EROFS so that identical layers/images produce byte-identical
//! output and therefore dedup in GlideFS's content-addressed cache (foyer).
//!
//! This writer mirrors the public surface of [`crate::writer::Writer`]
//! (`new`/`make_parents`/`create`/`link`/`Write`/`close`) so the existing OCI
//! layer-merge driver in `tar_convert` can target it unchanged.
//!
//! ## On-disk layout (the subset we emit)
//! ```text
//! [ block 0: bytes 0..1024 reserved | 1024..1152 superblock | 1152.. inodes ]
//! [ more metadata blocks: inode (32B compact) + inline tail data, packed ]
//! [ data blocks: full 4 KiB blocks of large files / multi-block dirs ]
//! ```
//! * Compact 32-byte inodes; `nid = inode_byte_offset / 32` (meta starts at
//!   block 0, so the first inode lands at byte 1152 = nid 36).
//! * `FLAT_INLINE`: a file/dir's full 4 KiB blocks live in the data region; the
//!   sub-block tail is stored inline immediately after the inode (same block).
//! * Directories: arrays of 12-byte `erofs_dirent{nid,nameoff,file_type}` with
//!   names packed after, sorted (`.`,`..`,children) for the kernel's bsearch.
//!
//! Not yet emitted (tracked for follow-ups): xattrs, chunk-based files / the
//! RAFS-v6 multi-blob layout, compression. v1 targets a self-contained merged
//! image, which already foyer-dedups strongly (spike: 34/41 shared blocks).

use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::writer::{File, WriterOption};

// --- on-disk constants ---
const SUPER_OFFSET: u64 = 1024;
const MAGIC: u32 = 0xE0F5_E1E2;
const BLK: usize = 4096;
const BLKSZBITS: u8 = 12;
const ISLOT: usize = 32; // compact inode slot size
const SB_SIZE: usize = 128;
const FIRST_INODE_OFF: usize = SUPER_OFFSET as usize + SB_SIZE; // 1152

// datalayout (i_format = datalayout << 1, version bit 0 = 0 for compact)
const LAYOUT_FLAT_PLAIN: u16 = 0;
const LAYOUT_FLAT_INLINE: u16 = 2;

// mode type bits (standard)
const S_IFMT: u16 = 0xF000;
const S_IFSOCK: u16 = 0xC000;
const S_IFLNK: u16 = 0xA000;
const S_IFREG: u16 = 0x8000;
const S_IFBLK: u16 = 0x6000;
const S_IFDIR: u16 = 0x4000;
const S_IFCHR: u16 = 0x2000;
const S_IFIFO: u16 = 0x1000;

// EROFS dirent file types
const FT_REG: u8 = 1;
const FT_DIR: u8 = 2;
const FT_CHR: u8 = 3;
const FT_BLK: u8 = 4;
const FT_FIFO: u8 = 5;
const FT_SOCK: u8 = 6;
const FT_LNK: u8 = 7;

const DIRENT_SIZE: usize = 12;

fn ftype_of(mode: u16) -> u8 {
    match mode & S_IFMT {
        S_IFREG => FT_REG,
        S_IFDIR => FT_DIR,
        S_IFCHR => FT_CHR,
        S_IFBLK => FT_BLK,
        S_IFIFO => FT_FIFO,
        S_IFSOCK => FT_SOCK,
        S_IFLNK => FT_LNK,
        _ => FT_REG,
    }
}

/// Linux `new_encode_dev`: pack (major, minor) into a 32-bit rdev.
fn new_encode_dev(major: u32, minor: u32) -> u32 {
    (minor & 0xff) | (major << 8) | ((minor & !0xff) << 12)
}

/// EROFS xattr name-prefix indices (the stored name omits the prefix).
fn xattr_prefix(name: &str) -> (u8, &str) {
    const PREFIXES: &[(u8, &str)] = &[
        (1, "user."),
        (4, "trusted."),
        (6, "security."),
        (3, "system.posix_acl_default"),
        (2, "system.posix_acl_access"),
    ];
    for &(idx, pfx) in PREFIXES {
        if let Some(rest) = name.strip_prefix(pfx) {
            return (idx, rest);
        }
    }
    (0, name)
}

/// Encode an inode's inline xattr area: a 12-byte ibody header (no shared
/// xattrs) followed by 4-byte-aligned entries. Empty when there are no xattrs.
fn encode_xattrs(xattrs: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
    if xattrs.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    // erofs_xattr_ibody_header: h_name_filter(4)=0, h_shared_count(1)=0, reserved[7]
    out.extend_from_slice(&0u32.to_le_bytes());
    out.push(0);
    out.extend_from_slice(&[0u8; 7]);
    for (name, val) in xattrs {
        let (index, rest) = xattr_prefix(name);
        out.push(rest.len() as u8); // e_name_len
        out.push(index); // e_name_index
        out.extend_from_slice(&(val.len() as u16).to_le_bytes()); // e_value_size
        out.extend_from_slice(rest.as_bytes());
        out.extend_from_slice(val);
        while out.len() % 4 != 0 {
            out.push(0);
        }
    }
    out
}

/// A node in the in-memory tree we assemble before serializing.
struct Node {
    mode: u16,
    uid: u32,
    gid: u32,
    linkname: String,
    devmajor: u32,
    devminor: u32,
    xattrs: BTreeMap<String, Vec<u8>>,
    /// Small inline content held in RAM: a symlink's target bytes. Regular-file
    /// contents are NOT here — they're spooled to disk (see `spool`) so peak
    /// memory stays bounded regardless of file/image size. Empty for regular
    /// files and directories (dir bodies are recomputed at serialize time).
    data: Vec<u8>,
    /// Regular-file contents spooled to the writer's temp file: `(offset, len)`.
    /// `None` for dirs, symlinks, and empty/zero-length regular files.
    spool: Option<(u64, u64)>,
    /// name -> child node index (directories only).
    children: BTreeMap<String, usize>,
    nlink: u32,
    // assigned during layout:
    nid: u64,
    /// First data block (absolute block number) for full blocks, else 0.
    blkaddr: u32,
}

impl Node {
    fn new_dir(mode: u16) -> Self {
        Node {
            mode: S_IFDIR | (mode & 0o7777),
            uid: 0,
            gid: 0,
            linkname: String::new(),
            devmajor: 0,
            devminor: 0,
            xattrs: BTreeMap::new(),
            data: Vec::new(),
            spool: None,
            children: BTreeMap::new(),
            nlink: 0,
            nid: 0,
            blkaddr: 0,
        }
    }
    fn is_dir(&self) -> bool {
        self.mode & S_IFMT == S_IFDIR
    }
}

/// EROFS image writer. Mirrors `ext4::writer::Writer`'s surface.
pub struct Writer<W: Read + Write + Seek> {
    out: W,
    nodes: Vec<Node>, // nodes[0] = root
    uuid: [u8; 16],
    /// Index of the regular file currently being written (for `Write`).
    current: Option<usize>,
    /// Disk spool for regular-file contents (lazily created on first write) and
    /// its append cursor. Keeps peak RAM bounded; auto-deleted on drop.
    spool: Option<std::fs::File>,
    spool_pos: u64,
    /// Align each large regular file's data region start to this many bytes
    /// (the downstream dedup-block grid), padding the gap with holes. 0 = off.
    data_align: usize,
    /// Minimum file size (bytes) that triggers data alignment.
    data_align_min: usize,
    /// Cold-start "prioritized files": paths laid out first and contiguously,
    /// in this order, so the boot working set forms one tight, coalesce-friendly
    /// run at the front of the image. Empty = natural DFS order.
    priority: Vec<String>,
}

impl<W: Read + Write + Seek> Writer<W> {
    pub fn new(out: W, opts: &[WriterOption]) -> Self {
        let mut uuid = [0u8; 16];
        let mut data_align = 0usize;
        let mut data_align_min = 0usize;
        let mut priority = Vec::new();
        for o in opts {
            match o {
                WriterOption::Uuid(u) => uuid = *u,
                WriterOption::AlignData { align, min_size } => {
                    debug_assert!(*align == 0 || align.is_power_of_two());
                    data_align = *align as usize;
                    data_align_min = *min_size as usize;
                }
                WriterOption::PriorityOrder(paths) => priority = paths.clone(),
                _ => {}
            }
        }
        let root = Node::new_dir(0o755);
        Writer {
            out,
            nodes: vec![root],
            uuid,
            current: None,
            spool: None,
            spool_pos: 0,
            data_align,
            data_align_min,
            priority,
        }
    }

    /// Resolve a directory path to a node index, creating missing parents as
    /// 0755 dirs. `path` is the *directory* portion (no trailing filename).
    fn ensure_dir(&mut self, comps: &[&str]) -> io::Result<usize> {
        let mut cur = 0usize;
        for &c in comps {
            if c.is_empty() || c == "." {
                continue;
            }
            if let Some(&idx) = self.nodes[cur].children.get(c) {
                if !self.nodes[idx].is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("path component {c} is not a directory"),
                    ));
                }
                cur = idx;
            } else {
                let idx = self.nodes.len();
                self.nodes.push(Node::new_dir(0o755));
                self.nodes[cur].children.insert(c.to_string(), idx);
                cur = idx;
            }
        }
        Ok(cur)
    }

    /// Ensure every parent directory of `name` exists.
    pub fn make_parents(&mut self, name: &str) -> io::Result<()> {
        let comps: Vec<&str> = split_path(name);
        if comps.len() <= 1 {
            return Ok(());
        }
        self.ensure_dir(&comps[..comps.len() - 1])?;
        Ok(())
    }

    /// Create a filesystem node at `name` with metadata `f`.
    pub fn create(&mut self, name: &str, f: &File) -> io::Result<()> {
        self.current = None;
        let comps = split_path(name);
        if comps.is_empty() {
            return Ok(()); // root already exists
        }
        let (parent_comps, base) = comps.split_at(comps.len() - 1);
        let base = base[0];
        let parent = self.ensure_dir(parent_comps)?;

        let mode = f.mode;
        let is_dir = mode & S_IFMT == S_IFDIR;

        // If the node already exists (e.g. a dir auto-made by make_parents, or a
        // later layer overriding it), update it in place so caller metadata wins.
        if let Some(&idx) = self.nodes[parent].children.get(base) {
            let n = &mut self.nodes[idx];
            n.mode = mode;
            n.uid = f.uid;
            n.gid = f.gid;
            n.linkname = f.linkname.clone();
            n.devmajor = f.devmajor;
            n.devminor = f.devminor;
            n.xattrs = f.xattrs.clone();
            if !is_dir {
                n.data.clear();
                // Drop any previously-spooled content (a later layer overriding
                // an earlier file); fresh content will re-spool via `write`.
                n.spool = None;
                if mode & S_IFMT == S_IFLNK {
                    n.data = f.linkname.clone().into_bytes();
                }
                if mode & S_IFMT == S_IFREG {
                    self.current = Some(idx);
                }
            }
            return Ok(());
        }

        let mut node = Node {
            mode,
            uid: f.uid,
            gid: f.gid,
            linkname: f.linkname.clone(),
            devmajor: f.devmajor,
            devminor: f.devminor,
            xattrs: f.xattrs.clone(),
            data: Vec::new(),
            spool: None,
            children: BTreeMap::new(),
            nlink: 0,
            nid: 0,
            blkaddr: 0,
        };
        if mode & S_IFMT == S_IFLNK {
            node.data = f.linkname.clone().into_bytes();
        }
        let idx = self.nodes.len();
        self.nodes.push(node);
        self.nodes[parent].children.insert(base.to_string(), idx);
        if mode & S_IFMT == S_IFREG {
            self.current = Some(idx);
        }
        Ok(())
    }

    /// Create a hard link `newname` referencing the inode at `oldname`.
    pub fn link(&mut self, oldname: &str, newname: &str) -> io::Result<()> {
        self.current = None;
        let src = self
            .resolve(oldname)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("link source {oldname} not found")))?;
        let comps = split_path(newname);
        let (parent_comps, base) = comps.split_at(comps.len() - 1);
        let base = base[0];
        let parent = self.ensure_dir(parent_comps)?;
        self.nodes[parent].children.insert(base.to_string(), src);
        Ok(())
    }

    fn resolve(&self, path: &str) -> Option<usize> {
        let mut cur = 0usize;
        for c in split_path(path) {
            if c.is_empty() || c == "." {
                continue;
            }
            cur = *self.nodes[cur].children.get(c)?;
        }
        Some(cur)
    }

    /// Finalize: lay out and serialize the image to the output, returning it.
    pub fn close(self) -> io::Result<W> {
        Ok(self.close_with_prefetch()?.0)
    }

    /// Like [`close`](Self::close) but also returns the **prefetch length**: the
    /// byte extent `[0, len)` covering the metadata region plus the contiguous
    /// priority (boot working set) data run. Callers (bless) persist this in the
    /// volume manifest so the server warms exactly the boot set on device open.
    /// `0` when no `PriorityOrder` was given (no hint).
    pub fn close_with_prefetch(mut self) -> io::Result<(W, u64)> {
        // serialize() streams the image directly into `self.out`.
        let prefetch_len = self.serialize()?;
        self.out.flush()?;
        Ok((self.out, prefetch_len))
    }
}

impl<W: Read + Write + Seek> Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(idx) = self.current else { return Ok(buf.len()) };
        if buf.is_empty() {
            return Ok(0);
        }
        // Append to the on-disk spool (lazily created). All writes for a single
        // file are consecutive (the driver finishes one file before create()ing
        // the next), so each file occupies one contiguous spool range.
        let spool = match &mut self.spool {
            Some(f) => f,
            None => {
                self.spool = Some(tempfile::tempfile()?);
                self.spool.as_mut().unwrap()
            }
        };
        spool.seek(SeekFrom::Start(self.spool_pos))?;
        spool.write_all(buf)?;
        let start = self.spool_pos;
        self.spool_pos += buf.len() as u64;
        let n = &mut self.nodes[idx];
        n.spool = Some(match n.spool {
            Some((off, len)) => (off, len + buf.len() as u64),
            None => (start, buf.len() as u64),
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<W: Read + Write + Seek> crate::writer::FsSink for Writer<W> {
    fn make_parents(&mut self, name: &str) -> io::Result<()> {
        Writer::make_parents(self, name)
    }
    fn create(&mut self, name: &str, f: &File) -> io::Result<()> {
        Writer::create(self, name, f)
    }
    fn link(&mut self, oldname: &str, newname: &str) -> io::Result<()> {
        Writer::link(self, oldname, newname)
    }
}

/// Per-inode layout facts computed before assigning positions.
#[derive(Clone, Copy, Default)]
struct Layout {
    /// Logical data length (file bytes, dir-encoded bytes, or symlink target).
    data_len: usize,
    nfull: usize, // full 4 KiB blocks (go to data region)
    tail: usize,  // inline tail bytes (< BLK), stored after the inode
    xattr_isize: usize, // inline xattr area size (bytes), between inode and tail
}

impl<W: Read + Write + Seek> Writer<W> {
    /// Deterministic traversal order: root first, then DFS with children sorted
    /// by name (BTreeMap already sorts). Returns node indices, and sets a
    /// post-order-independent stable order (root, then recursively).
    fn traversal(&self) -> Vec<usize> {
        let mut order = Vec::with_capacity(self.nodes.len());
        let mut seen = vec![false; self.nodes.len()];
        // BFS-ish DFS from root; hardlinked nodes visited once.
        fn rec(nodes: &[Node], idx: usize, seen: &mut [bool], order: &mut Vec<usize>) {
            if seen[idx] {
                return;
            }
            seen[idx] = true;
            order.push(idx);
            for (_name, &child) in &nodes[idx].children {
                rec(nodes, child, seen, order);
            }
        }
        rec(&self.nodes, 0, &mut seen, &mut order);
        order
    }

    /// Encode a directory's entries into the logical byte stream (full blocks
    /// padded to 4 KiB, then a partial tail). `nid_of` maps child index -> nid;
    /// for the length-only pre-pass, nids are irrelevant (all 8 bytes).
    fn encode_dir(&self, idx: usize, self_nid: u64, parent_nid: u64) -> Vec<u8> {
        // (name, nid, ftype) sorted: ".", "..", then children by name.
        let mut entries: Vec<(String, u64, u8)> = Vec::new();
        entries.push((".".to_string(), self_nid, FT_DIR));
        entries.push(("..".to_string(), parent_nid, FT_DIR));
        for (name, &child) in &self.nodes[idx].children {
            entries.push((name.clone(), self.nodes[child].nid, ftype_of(self.nodes[child].mode)));
        }

        // Greedy block partition (names sorted => blocks stay sorted).
        let mut out: Vec<u8> = Vec::new();
        let mut i = 0;
        while i < entries.len() {
            // how many entries fit in this block?
            let mut k = 0;
            let mut names_len = 0usize;
            while i + k < entries.len() {
                let nl = entries[i + k].0.len();
                if (k + 1) * DIRENT_SIZE + names_len + nl > BLK {
                    break;
                }
                names_len += nl;
                k += 1;
            }
            // emit this block: [k dirents][names]
            let block_entries = &entries[i..i + k];
            let names_start = k * DIRENT_SIZE;
            let mut block = Vec::with_capacity(names_start + names_len);
            let mut name_off = names_start;
            for (name, nid, ft) in block_entries {
                block.extend_from_slice(&nid.to_le_bytes()); // 8
                block.extend_from_slice(&(name_off as u16).to_le_bytes()); // 2
                block.push(*ft); // 1
                block.push(0); // reserved
                name_off += name.len();
            }
            for (name, _, _) in block_entries {
                block.extend_from_slice(name.as_bytes());
            }
            i += k;
            // pad non-final blocks to BLK; the final block is the inline tail.
            if i < entries.len() {
                block.resize(BLK, 0);
            }
            out.extend_from_slice(&block);
        }
        out
    }

    /// Compute layout facts (lengths only — nid values don't change lengths).
    fn layout_facts(&self, order: &[usize], parent_of: &[usize]) -> Vec<Layout> {
        let mut facts = vec![Layout::default(); self.nodes.len()];
        for &idx in order {
            let n = &self.nodes[idx];
            let data_len = if n.is_dir() {
                // length-only encode with placeholder nids
                self.encode_dir(idx, 0, parent_of[idx] as u64).len()
            } else if let Some((_, len)) = n.spool {
                len as usize // regular file: spooled content length
            } else {
                n.data.len() // symlink target (or empty)
            };
            let xattr_isize = encode_xattrs(&n.xattrs).len();
            let mut nfull = data_len / BLK;
            let mut tail = data_len % BLK;
            // The inode header + inline xattrs + inline tail must fit within one
            // 4 KiB block. If they don't, fall back to FLAT_PLAIN (no inline
            // tail — all data goes to full blocks in the data region).
            if ISLOT + xattr_isize + tail > BLK {
                nfull = data_len.div_ceil(BLK);
                tail = 0;
            }
            facts[idx] = Layout {
                data_len,
                nfull,
                tail,
                xattr_isize,
            };
        }
        facts
    }

    /// Layout order: root first, then the cold-start priority files (in the
    /// caller's order, deduplicated), then everything else in DFS order. Both
    /// the inode (Pass 1) and data (Pass 2) passes walk this order, so priority
    /// files cluster their metadata AND data at the front of the image —
    /// collapsing a scattered boot read into one coalesce-friendly run. Returns
    /// the order plus the set of priority node indices (packed tight, no align).
    fn layout_order(&self) -> (Vec<usize>, std::collections::HashSet<usize>) {
        let dfs = self.traversal();
        if self.priority.is_empty() {
            return (dfs, std::collections::HashSet::new());
        }
        let mut prio_set = std::collections::HashSet::new();
        let mut order = Vec::with_capacity(dfs.len());
        let mut placed = vec![false; self.nodes.len()];
        order.push(0);
        placed[0] = true;
        for path in &self.priority {
            if let Some(idx) = self.resolve(path)
                && !placed[idx]
            {
                placed[idx] = true;
                prio_set.insert(idx);
                order.push(idx);
            }
        }
        for &idx in &dfs {
            if !placed[idx] {
                placed[idx] = true;
                order.push(idx);
            }
        }
        (order, prio_set)
    }

    /// Stream the image into `self.out` (bounded memory — never materializes the
    /// whole image or any file's content in RAM). Returns the prefetch length
    /// (boot working-set byte extent `[0, len)`; 0 when no `PriorityOrder`).
    fn serialize(&mut self) -> io::Result<u64> {
        let (order, prio_set) = self.layout_order();

        // parent map (for ".." nid). Root's parent is itself.
        let mut parent_of = vec![0usize; self.nodes.len()];
        for idx in 0..self.nodes.len() {
            let children: Vec<usize> = self.nodes[idx].children.values().copied().collect();
            for c in children {
                // a hardlinked file may appear under multiple dirs; ".." only
                // matters for directories, which are never hardlinked.
                if self.nodes[c].is_dir() {
                    parent_of[c] = idx;
                }
            }
        }

        // nlink: dirs = 2 + subdir count; non-dirs = the number of dirents
        // pointing at the inode (hardlinks => > 1).
        let mut refcount = vec![0u32; self.nodes.len()];
        for idx in 0..self.nodes.len() {
            for &c in self.nodes[idx].children.values() {
                refcount[c] += 1;
            }
        }
        for idx in 0..self.nodes.len() {
            if self.nodes[idx].is_dir() {
                let subdirs = self.nodes[idx]
                    .children
                    .values()
                    .filter(|&&c| self.nodes[c].is_dir())
                    .count();
                self.nodes[idx].nlink = (2 + subdirs) as u32;
            } else {
                self.nodes[idx].nlink = refcount[idx].max(1);
            }
        }

        let facts = self.layout_facts(&order, &parent_of);

        // --- Pass 1: assign meta offsets (nids), inode + inline tail packed,
        // never crossing a 4 KiB block boundary; 32-byte aligned. ---
        let mut meta_off = FIRST_INODE_OFF;
        for &idx in &order {
            let tail = facts[idx].tail;
            let need = ISLOT + facts[idx].xattr_isize + tail;
            let block_start = meta_off & !(BLK - 1);
            if meta_off + need > block_start + BLK {
                // would cross block boundary: advance to next block
                meta_off = block_start + BLK;
            }
            self.nodes[idx].nid = (meta_off / ISLOT) as u64;
            meta_off += need;
            // 32-byte align for the next inode
            meta_off = (meta_off + ISLOT - 1) & !(ISLOT - 1);
        }

        // meta region ends here; data region begins at next block boundary.
        let data_region_start_blk = meta_off.div_ceil(BLK);
        // --- Pass 2: assign data blocks for full blocks ---
        //
        // When data alignment is on, snap each large regular file's first data
        // block to the dedup-block grid (e.g. 128 KiB = 32 blocks). Identical
        // file content then yields identical 128 KiB blocks no matter what was
        // written before it, so content-addressed dedup survives upstream churn
        // (a rebuild that inserts one small file no longer shifts every big file
        // out of alignment). The skipped blocks are holes — zeros that the block
        // layer never stores. Unlike ext4, EROFS has no interleaved group
        // metadata, so aligned data can never collide with a reserved block.
        let align_blks = self.data_align / BLK; // 0 or 1 => no-op
        let mut next_blk = data_region_start_blk;
        // End of the contiguous priority (boot working set) data run. Priority
        // files come first in `order` and are packed tight, so this advances
        // only while we're still placing them. Starts at the data-region origin
        // so the extent always covers the whole metadata region (where small
        // priority files' inline tails live), even with no large priority files.
        let mut priority_end_blk = data_region_start_blk;
        for &idx in &order {
            if facts[idx].nfull > 0 {
                if align_blks > 1
                    && !prio_set.contains(&idx)
                    && self.nodes[idx].mode & S_IFMT == S_IFREG
                    && facts[idx].data_len >= self.data_align_min
                {
                    let rem = next_blk % align_blks;
                    if rem != 0 {
                        next_blk += align_blks - rem;
                    }
                }
                self.nodes[idx].blkaddr = next_blk as u32;
                next_blk += facts[idx].nfull;
                if prio_set.contains(&idx) {
                    priority_end_blk = next_blk;
                }
            }
        }
        // Prefetch hint: meta region + priority data run. 0 when no priority set.
        let prefetch_len = if prio_set.is_empty() {
            0u64
        } else {
            (priority_end_blk * BLK) as u64
        };
        let total_blocks = next_blk.max(data_region_start_blk).max(1);

        // --- Pass 3: stream the image directly into `self.out`, never holding
        // the whole image (or any file's contents) in RAM. Holes — the reserved
        // prefix, alignment gaps, and sub-block padding — are not written; a
        // fresh seekable sink reads them back as zero (`Cursor` zero-fills on
        // write-past-end; sparse files / zeroed block devices read holes as 0).
        let mut max_written: u64 = 0;
        let sb = self.superblock_bytes(total_blocks, self.nodes[0].nid);
        self.write_out_at(SUPER_OFFSET as usize, &sb, &mut max_written)?;

        for &idx in &order {
            let nfull = facts[idx].nfull;
            let tail = facts[idx].tail;
            let data_len = facts[idx].data_len;
            let meta_off = (self.nodes[idx].nid as usize) * ISLOT;

            // inode header, then inline xattrs.
            let inode = self.inode_bytes(idx, &facts[idx]);
            self.write_out_at(meta_off, &inode, &mut max_written)?;
            let xa = encode_xattrs(&self.nodes[idx].xattrs);
            if !xa.is_empty() {
                self.write_out_at(meta_off + ISLOT, &xa, &mut max_written)?;
            }

            // Data source: a dir body (recomputed now that nids are known) or a
            // symlink target live in RAM (both small); regular-file content is
            // read back from the on-disk spool in <= BLK chunks.
            enum Src {
                Mem(Vec<u8>),
                Spool(u64),
            }
            let src = if self.nodes[idx].is_dir() {
                let self_nid = self.nodes[idx].nid;
                let parent_nid = self.nodes[parent_of[idx]].nid;
                Src::Mem(self.encode_dir(idx, self_nid, parent_nid))
            } else if let Some((off, _)) = self.nodes[idx].spool {
                Src::Spool(off)
            } else {
                Src::Mem(self.nodes[idx].data.clone())
            };
            if let Src::Mem(ref v) = src {
                debug_assert_eq!(v.len(), data_len);
            }

            // inline tail (sub-block remainder), right after the xattrs.
            if tail > 0 {
                let t0 = meta_off + ISLOT + facts[idx].xattr_isize;
                let bytes = match &src {
                    Src::Mem(v) => v[nfull * BLK..].to_vec(),
                    Src::Spool(off) => self.read_spool(off + (nfull * BLK) as u64, tail)?,
                };
                self.write_out_at(t0, &bytes, &mut max_written)?;
            }
            // full data blocks (the last may be partial under the FLAT_PLAIN
            // fallback, so write at most data_len bytes), streamed block-by-block.
            if nfull > 0 {
                let base = self.nodes[idx].blkaddr as usize * BLK;
                let total = data_len.min(nfull * BLK);
                let mut w = 0;
                while w < total {
                    let chunk = (total - w).min(BLK);
                    let bytes = match &src {
                        Src::Mem(v) => v[w..w + chunk].to_vec(),
                        Src::Spool(off) => self.read_spool(off + w as u64, chunk)?,
                    };
                    self.write_out_at(base + w, &bytes, &mut max_written)?;
                    w += chunk;
                }
            }
        }

        // Guarantee the output is exactly `total_blocks * BLK` bytes: the final
        // block may end in a sub-block hole nothing wrote (the gap is < BLK).
        let end = (total_blocks * BLK) as u64;
        if max_written < end {
            let pad = vec![0u8; (end - max_written) as usize];
            self.out.seek(SeekFrom::Start(max_written))?;
            self.out.write_all(&pad)?;
        }

        Ok(prefetch_len)
    }

    /// Read `len` bytes at `off` from the spool file (regular-file content).
    fn read_spool(&mut self, off: u64, len: usize) -> io::Result<Vec<u8>> {
        let spool = self
            .spool
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "spool file missing"))?;
        spool.seek(SeekFrom::Start(off))?;
        let mut buf = vec![0u8; len];
        spool.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Write `bytes` at byte `offset` in the output, tracking the high-water mark.
    fn write_out_at(&mut self, offset: usize, bytes: &[u8], max_written: &mut u64) -> io::Result<()> {
        self.out.seek(SeekFrom::Start(offset as u64))?;
        self.out.write_all(bytes)?;
        *max_written = (*max_written).max(offset as u64 + bytes.len() as u64);
        Ok(())
    }

    /// Build the 32-byte compact inode for node `idx` (placed at its nid offset).
    fn inode_bytes(&self, idx: usize, fact: &Layout) -> [u8; ISLOT] {
        let n = &self.nodes[idx];
        let datalayout = if fact.tail > 0 || fact.data_len == 0 {
            LAYOUT_FLAT_INLINE
        } else {
            LAYOUT_FLAT_PLAIN
        };
        let i_format = datalayout << 1; // compact (version bit 0 = 0)

        let i_u: u32 = match n.mode & S_IFMT {
            S_IFCHR | S_IFBLK => new_encode_dev(n.devmajor, n.devminor),
            S_IFIFO | S_IFSOCK => 0,
            _ => {
                if fact.nfull > 0 {
                    n.blkaddr
                } else {
                    0xffff_ffff
                }
            }
        };

        let mut b = [0u8; ISLOT];
        let w16 = |b: &mut [u8], o: usize, v: u16| b[o..o + 2].copy_from_slice(&v.to_le_bytes());
        let w32 = |b: &mut [u8], o: usize, v: u32| b[o..o + 4].copy_from_slice(&v.to_le_bytes());

        let icount = if fact.xattr_isize == 0 {
            0u16
        } else {
            ((fact.xattr_isize - 12) / 4 + 1) as u16
        };
        w16(&mut b, 0x00, i_format);
        w16(&mut b, 0x02, icount); // i_xattr_icount
        w16(&mut b, 0x04, n.mode);
        w16(&mut b, 0x06, n.nlink as u16);
        w32(&mut b, 0x08, fact.data_len as u32); // i_size
        w32(&mut b, 0x10, i_u);
        w32(&mut b, 0x14, (n.nid as u32).wrapping_add(1)); // i_ino (informational)
        w16(&mut b, 0x18, n.uid as u16);
        w16(&mut b, 0x1A, n.gid as u16);
        b
    }

    /// Build the 128-byte superblock (placed at `SUPER_OFFSET`).
    fn superblock_bytes(&self, total_blocks: usize, root_nid: u64) -> [u8; SB_SIZE] {
        let mut b = [0u8; SB_SIZE];
        let w16 = |b: &mut [u8], o: usize, v: u16| b[o..o + 2].copy_from_slice(&v.to_le_bytes());
        let w32 = |b: &mut [u8], o: usize, v: u32| b[o..o + 4].copy_from_slice(&v.to_le_bytes());
        let w64 = |b: &mut [u8], o: usize, v: u64| b[o..o + 8].copy_from_slice(&v.to_le_bytes());

        w32(&mut b, 0x00, MAGIC);
        b[0x0C] = BLKSZBITS;
        w16(&mut b, 0x0E, root_nid as u16);
        w64(&mut b, 0x10, self.nodes.len() as u64); // inos
        w32(&mut b, 0x24, total_blocks as u32); // blocks
        b[0x30..0x40].copy_from_slice(&self.uuid);
        // checksum, features, timestamps, meta/xattr_blkaddr, volume_name = 0
        b
    }
}

/// Split a slash path into non-empty components.
fn split_path(name: &str) -> Vec<&str> {
    name.trim_matches('/')
        .split('/')
        .filter(|c| !c.is_empty() && *c != ".")
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn reg(size: i64) -> File {
        File {
            mode: S_IFREG | 0o644,
            size,
            ..Default::default()
        }
    }

    #[test]
    fn builds_superblock_and_root() {
        let mut w = Writer::new(Cursor::new(Vec::new()), &[WriterOption::Uuid([0u8; 16])]);
        w.create("hello.txt", &reg(12)).unwrap();
        w.write_all(b"hello erofs\n").unwrap();
        w.make_parents("sub/a.txt").unwrap();
        w.create("sub/a.txt", &reg(7)).unwrap();
        w.write_all(b"nested\n").unwrap();
        let img = w.close().unwrap().into_inner();

        // superblock magic
        assert_eq!(&img[1024..1028], &MAGIC.to_le_bytes());
        // root inode at first slot
        assert_eq!(u16::from_le_bytes([img[1024 + 0x0E], img[1024 + 0x0F]]), 36);
        // image is block-sized
        assert_eq!(img.len() % BLK, 0);
    }

    #[test]
    fn deterministic() {
        let build = || {
            let mut w = Writer::new(Cursor::new(Vec::new()), &[WriterOption::Uuid([0x42; 16])]);
            w.create("a.txt", &reg(3)).unwrap();
            w.write_all(b"abc").unwrap();
            w.close().unwrap().into_inner()
        };
        assert_eq!(build(), build());
    }
}
