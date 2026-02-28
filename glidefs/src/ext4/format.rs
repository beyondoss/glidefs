/// On-disk ext4 format types.
///
/// All structs are little-endian binary. This is a write-only implementation
/// for deterministic ext4 image generation — no deserialization needed.
///
/// Reference: https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout
/// Ported from: github.com/Microsoft/hcsshim/ext4/internal/format
use std::io::{self, Write};

// ---- Mode constants (POSIX file type + permission bits) ----

pub const S_IXOTH: u16 = 0x1;
pub const S_IWOTH: u16 = 0x2;
pub const S_IROTH: u16 = 0x4;
pub const S_IXGRP: u16 = 0x8;
pub const S_IWGRP: u16 = 0x10;
pub const S_IRGRP: u16 = 0x20;
pub const S_IXUSR: u16 = 0x40;
pub const S_IWUSR: u16 = 0x80;
pub const S_IRUSR: u16 = 0x100;
pub const S_ISVTX: u16 = 0x200;
pub const S_ISGID: u16 = 0x400;
pub const S_ISUID: u16 = 0x800;
pub const S_IFIFO: u16 = 0x1000;
pub const S_IFCHR: u16 = 0x2000;
pub const S_IFDIR: u16 = 0x4000;
pub const S_IFBLK: u16 = 0x6000;
pub const S_IFREG: u16 = 0x8000;
pub const S_IFLNK: u16 = 0xA000;
pub const S_IFSOCK: u16 = 0xC000;

pub const TYPE_MASK: u16 = 0xF000;

// ---- Inode number ----

pub type InodeNumber = u32;

pub const INODE_ROOT: InodeNumber = 2;

// ---- File type (used in directory entries) ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FileType {
    Unknown = 0,
    Regular = 1,
    Directory = 2,
    Character = 3,
    Block = 4,
    Fifo = 5,
    Socket = 6,
    SymbolicLink = 7,
}

pub fn mode_to_file_type(mode: u16) -> FileType {
    match mode & TYPE_MASK {
        S_IFREG => FileType::Regular,
        S_IFDIR => FileType::Directory,
        S_IFCHR => FileType::Character,
        S_IFBLK => FileType::Block,
        S_IFIFO => FileType::Fifo,
        S_IFSOCK => FileType::Socket,
        S_IFLNK => FileType::SymbolicLink,
        _ => FileType::Unknown,
    }
}

// ---- Inode flags ----

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct InodeFlags: u32 {
        const SECRM              = 0x1;
        const UNRM               = 0x2;
        const COMPRESSED         = 0x4;
        const SYNC               = 0x8;
        const IMMUTABLE          = 0x10;
        const APPEND             = 0x20;
        const NO_DUMP            = 0x40;
        const NO_ATIME           = 0x80;
        const DIRTY_COMPRESSED   = 0x100;
        const COMPRESSED_CLUSTERS = 0x200;
        const NO_COMPRESS        = 0x400;
        const ENCRYPTED          = 0x800;
        const HASHED_INDEX       = 0x1000;
        const MAGIC              = 0x2000;
        const JOURNAL_DATA       = 0x4000;
        const NO_TAIL            = 0x8000;
        const DIR_SYNC           = 0x10000;
        const TOP_DIR            = 0x20000;
        const HUGE_FILE          = 0x40000;
        const EXTENTS            = 0x80000;
        const EA_INODE           = 0x200000;
        const EOF_BLOCKS         = 0x400000;
        const SNAPFILE           = 0x01000000;
        const SNAPFILE_DELETED   = 0x04000000;
        const SNAPFILE_SHRUNK    = 0x08000000;
        const INLINE_DATA        = 0x10000000;
        const PROJECT_INHERIT    = 0x20000000;
        const RESERVED           = 0x80000000;
    }
}

// ---- Feature flags ----

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CompatFeature: u32 {
        const DIR_PREALLOC   = 0x1;
        const IMAGIC_INODES  = 0x2;
        const HAS_JOURNAL    = 0x4;
        const EXT_ATTR       = 0x8;
        const RESIZE_INODE   = 0x10;
        const DIR_INDEX      = 0x20;
        const LAZY_BG        = 0x40;
        const EXCLUDE_INODE  = 0x80;
        const EXCLUDE_BITMAP = 0x100;
        const SPARSE_SUPER2  = 0x200;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IncompatFeature: u32 {
        const COMPRESSION = 0x1;
        const FILETYPE    = 0x2;
        const RECOVER     = 0x4;
        const JOURNAL_DEV = 0x8;
        const META_BG     = 0x10;
        const EXTENTS     = 0x40;
        const BIT64       = 0x80;
        const MMP         = 0x100;
        const FLEX_BG     = 0x200;
        const EA_INODE    = 0x400;
        const DIRDATA     = 0x1000;
        const CSUM_SEED   = 0x2000;
        const LARGEDIR    = 0x4000;
        const INLINE_DATA = 0x8000;
        const ENCRYPT     = 0x10000;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RoCompatFeature: u32 {
        const SPARSE_SUPER   = 0x1;
        const LARGE_FILE     = 0x2;
        const BTREE_DIR      = 0x4;
        const HUGE_FILE      = 0x8;
        const GDT_CSUM       = 0x10;
        const DIR_NLINK      = 0x20;
        const EXTRA_ISIZE    = 0x40;
        const HAS_SNAPSHOT    = 0x80;
        const QUOTA           = 0x100;
        const BIGALLOC        = 0x200;
        const METADATA_CSUM   = 0x400;
        const REPLICA         = 0x800;
        const READONLY        = 0x1000;
        const PROJECT         = 0x2000;
    }
}

// ---- SuperBlock ----

pub const SUPER_BLOCK_MAGIC: u16 = 0xef53;

/// SuperBlock at offset 1024 in block 0. Total 1024 bytes.
pub struct SuperBlock {
    pub inodes_count: u32,
    pub blocks_count_low: u32,
    pub r_blocks_count_low: u32,
    pub free_blocks_count_low: u32,
    pub free_inodes_count: u32,
    pub first_data_block: u32,
    pub log_block_size: u32,
    pub log_cluster_size: u32,
    pub blocks_per_group: u32,
    pub clusters_per_group: u32,
    pub inodes_per_group: u32,
    pub mtime: u32,
    pub wtime: u32,
    pub mount_count: u16,
    pub max_mount_count: u16,
    pub magic: u16,
    pub state: u16,
    pub errors: u16,
    pub minor_revision_level: u16,
    pub last_check: u32,
    pub check_interval: u32,
    pub creator_os: u32,
    pub revision_level: u32,
    pub default_reserved_uid: u16,
    pub default_reserved_gid: u16,
    pub first_inode: u32,
    pub inode_size: u16,
    pub block_group_nr: u16,
    pub feature_compat: CompatFeature,
    pub feature_incompat: IncompatFeature,
    pub feature_ro_compat: RoCompatFeature,
    pub uuid: [u8; 16],
    pub volume_name: [u8; 16],
    pub last_mounted: [u8; 64],
    pub algorithm_usage_bitmap: u32,
    pub prealloc_blocks: u8,
    pub prealloc_dir_blocks: u8,
    pub reserved_gdt_blocks: u16,
    pub journal_uuid: [u8; 16],
    pub journal_inum: u32,
    pub journal_dev: u32,
    pub last_orphan: u32,
    pub hash_seed: [u32; 4],
    pub def_hash_version: u8,
    pub journal_backup_type: u8,
    pub desc_size: u16,
    pub default_mount_opts: u32,
    pub first_meta_bg: u32,
    pub mkfs_time: u32,
    pub journal_blocks: [u32; 17],
    pub blocks_count_high: u32,
    pub r_blocks_count_high: u32,
    pub free_blocks_count_high: u32,
    pub min_extra_isize: u16,
    pub want_extra_isize: u16,
    pub flags: u32,
    pub raid_stride: u16,
    pub mmp_interval: u16,
    pub mmp_block: u64,
    pub raid_stripe_width: u32,
    pub log_groups_per_flex: u8,
    pub checksum_type: u8,
    pub reserved_pad: u16,
    pub kbytes_written: u64,
    pub snapshot_inum: u32,
    pub snapshot_id: u32,
    pub snapshot_r_blocks_count: u64,
    pub snapshot_list: u32,
    pub error_count: u32,
    pub first_error_time: u32,
    pub first_error_inode: u32,
    pub first_error_block: u64,
    pub first_error_func: [u8; 32],
    pub first_error_line: u32,
    pub last_error_time: u32,
    pub last_error_inode: u32,
    pub last_error_line: u32,
    pub last_error_block: u64,
    pub last_error_func: [u8; 32],
    pub mount_opts: [u8; 64],
    pub user_quota_inum: u32,
    pub group_quota_inum: u32,
    pub overhead_blocks: u32,
    pub backup_bgs: [u32; 2],
    pub encrypt_algos: [u8; 4],
    pub encrypt_pw_salt: [u8; 16],
    pub lpf_inode: u32,
    pub project_quota_inum: u32,
    pub checksum_seed: u32,
    pub wtime_high: u8,
    pub mtime_high: u8,
    pub mkfs_time_high: u8,
    pub lastcheck_high: u8,
    pub first_error_time_high: u8,
    pub last_error_time_high: u8,
    pub pad: [u8; 2],
    pub reserved: [u32; 96],
    pub checksum: u32,
}

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            inodes_count: 0,
            blocks_count_low: 0,
            r_blocks_count_low: 0,
            free_blocks_count_low: 0,
            free_inodes_count: 0,
            first_data_block: 0,
            log_block_size: 0,
            log_cluster_size: 0,
            blocks_per_group: 0,
            clusters_per_group: 0,
            inodes_per_group: 0,
            mtime: 0,
            wtime: 0,
            mount_count: 0,
            max_mount_count: 0,
            magic: SUPER_BLOCK_MAGIC,
            state: 0,
            errors: 0,
            minor_revision_level: 0,
            last_check: 0,
            check_interval: 0,
            creator_os: 0,
            revision_level: 0,
            default_reserved_uid: 0,
            default_reserved_gid: 0,
            first_inode: 0,
            inode_size: 0,
            block_group_nr: 0,
            feature_compat: CompatFeature::empty(),
            feature_incompat: IncompatFeature::empty(),
            feature_ro_compat: RoCompatFeature::empty(),
            uuid: [0; 16],
            volume_name: [0; 16],
            last_mounted: [0; 64],
            algorithm_usage_bitmap: 0,
            prealloc_blocks: 0,
            prealloc_dir_blocks: 0,
            reserved_gdt_blocks: 0,
            journal_uuid: [0; 16],
            journal_inum: 0,
            journal_dev: 0,
            last_orphan: 0,
            hash_seed: [0; 4],
            def_hash_version: 0,
            journal_backup_type: 0,
            desc_size: 0,
            default_mount_opts: 0,
            first_meta_bg: 0,
            mkfs_time: 0,
            journal_blocks: [0; 17],
            blocks_count_high: 0,
            r_blocks_count_high: 0,
            free_blocks_count_high: 0,
            min_extra_isize: 0,
            want_extra_isize: 0,
            flags: 0,
            raid_stride: 0,
            mmp_interval: 0,
            mmp_block: 0,
            raid_stripe_width: 0,
            log_groups_per_flex: 0,
            checksum_type: 0,
            reserved_pad: 0,
            kbytes_written: 0,
            snapshot_inum: 0,
            snapshot_id: 0,
            snapshot_r_blocks_count: 0,
            snapshot_list: 0,
            error_count: 0,
            first_error_time: 0,
            first_error_inode: 0,
            first_error_block: 0,
            first_error_func: [0; 32],
            first_error_line: 0,
            last_error_time: 0,
            last_error_inode: 0,
            last_error_line: 0,
            last_error_block: 0,
            last_error_func: [0; 32],
            mount_opts: [0; 64],
            user_quota_inum: 0,
            group_quota_inum: 0,
            overhead_blocks: 0,
            backup_bgs: [0; 2],
            encrypt_algos: [0; 4],
            encrypt_pw_salt: [0; 16],
            lpf_inode: 0,
            project_quota_inum: 0,
            checksum_seed: 0,
            wtime_high: 0,
            mtime_high: 0,
            mkfs_time_high: 0,
            lastcheck_high: 0,
            first_error_time_high: 0,
            last_error_time_high: 0,
            pad: [0; 2],
            reserved: [0; 96],
            checksum: 0,
        }
    }
}

impl SuperBlock {
    /// Serialize to exactly 1024 bytes, little-endian.
    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.inodes_count.to_le_bytes())?;
        w.write_all(&self.blocks_count_low.to_le_bytes())?;
        w.write_all(&self.r_blocks_count_low.to_le_bytes())?;
        w.write_all(&self.free_blocks_count_low.to_le_bytes())?;
        w.write_all(&self.free_inodes_count.to_le_bytes())?;
        w.write_all(&self.first_data_block.to_le_bytes())?;
        w.write_all(&self.log_block_size.to_le_bytes())?;
        w.write_all(&self.log_cluster_size.to_le_bytes())?;
        w.write_all(&self.blocks_per_group.to_le_bytes())?;
        w.write_all(&self.clusters_per_group.to_le_bytes())?;
        w.write_all(&self.inodes_per_group.to_le_bytes())?;
        w.write_all(&self.mtime.to_le_bytes())?;
        w.write_all(&self.wtime.to_le_bytes())?;
        w.write_all(&self.mount_count.to_le_bytes())?;
        w.write_all(&self.max_mount_count.to_le_bytes())?;
        w.write_all(&self.magic.to_le_bytes())?;
        w.write_all(&self.state.to_le_bytes())?;
        w.write_all(&self.errors.to_le_bytes())?;
        w.write_all(&self.minor_revision_level.to_le_bytes())?;
        w.write_all(&self.last_check.to_le_bytes())?;
        w.write_all(&self.check_interval.to_le_bytes())?;
        w.write_all(&self.creator_os.to_le_bytes())?;
        w.write_all(&self.revision_level.to_le_bytes())?;
        w.write_all(&self.default_reserved_uid.to_le_bytes())?;
        w.write_all(&self.default_reserved_gid.to_le_bytes())?;
        w.write_all(&self.first_inode.to_le_bytes())?;
        w.write_all(&self.inode_size.to_le_bytes())?;
        w.write_all(&self.block_group_nr.to_le_bytes())?;
        w.write_all(&self.feature_compat.bits().to_le_bytes())?;
        w.write_all(&self.feature_incompat.bits().to_le_bytes())?;
        w.write_all(&self.feature_ro_compat.bits().to_le_bytes())?;
        w.write_all(&self.uuid)?;
        w.write_all(&self.volume_name)?;
        w.write_all(&self.last_mounted)?;
        w.write_all(&self.algorithm_usage_bitmap.to_le_bytes())?;
        w.write_all(&[self.prealloc_blocks])?;
        w.write_all(&[self.prealloc_dir_blocks])?;
        w.write_all(&self.reserved_gdt_blocks.to_le_bytes())?;
        w.write_all(&self.journal_uuid)?;
        w.write_all(&self.journal_inum.to_le_bytes())?;
        w.write_all(&self.journal_dev.to_le_bytes())?;
        w.write_all(&self.last_orphan.to_le_bytes())?;
        for s in &self.hash_seed {
            w.write_all(&s.to_le_bytes())?;
        }
        w.write_all(&[self.def_hash_version])?;
        w.write_all(&[self.journal_backup_type])?;
        w.write_all(&self.desc_size.to_le_bytes())?;
        w.write_all(&self.default_mount_opts.to_le_bytes())?;
        w.write_all(&self.first_meta_bg.to_le_bytes())?;
        w.write_all(&self.mkfs_time.to_le_bytes())?;
        for b in &self.journal_blocks {
            w.write_all(&b.to_le_bytes())?;
        }
        w.write_all(&self.blocks_count_high.to_le_bytes())?;
        w.write_all(&self.r_blocks_count_high.to_le_bytes())?;
        w.write_all(&self.free_blocks_count_high.to_le_bytes())?;
        w.write_all(&self.min_extra_isize.to_le_bytes())?;
        w.write_all(&self.want_extra_isize.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&self.raid_stride.to_le_bytes())?;
        w.write_all(&self.mmp_interval.to_le_bytes())?;
        w.write_all(&self.mmp_block.to_le_bytes())?;
        w.write_all(&self.raid_stripe_width.to_le_bytes())?;
        w.write_all(&[self.log_groups_per_flex])?;
        w.write_all(&[self.checksum_type])?;
        w.write_all(&self.reserved_pad.to_le_bytes())?;
        w.write_all(&self.kbytes_written.to_le_bytes())?;
        w.write_all(&self.snapshot_inum.to_le_bytes())?;
        w.write_all(&self.snapshot_id.to_le_bytes())?;
        w.write_all(&self.snapshot_r_blocks_count.to_le_bytes())?;
        w.write_all(&self.snapshot_list.to_le_bytes())?;
        w.write_all(&self.error_count.to_le_bytes())?;
        w.write_all(&self.first_error_time.to_le_bytes())?;
        w.write_all(&self.first_error_inode.to_le_bytes())?;
        w.write_all(&self.first_error_block.to_le_bytes())?;
        w.write_all(&self.first_error_func)?;
        w.write_all(&self.first_error_line.to_le_bytes())?;
        w.write_all(&self.last_error_time.to_le_bytes())?;
        w.write_all(&self.last_error_inode.to_le_bytes())?;
        w.write_all(&self.last_error_line.to_le_bytes())?;
        w.write_all(&self.last_error_block.to_le_bytes())?;
        w.write_all(&self.last_error_func)?;
        w.write_all(&self.mount_opts)?;
        w.write_all(&self.user_quota_inum.to_le_bytes())?;
        w.write_all(&self.group_quota_inum.to_le_bytes())?;
        w.write_all(&self.overhead_blocks.to_le_bytes())?;
        for b in &self.backup_bgs {
            w.write_all(&b.to_le_bytes())?;
        }
        w.write_all(&self.encrypt_algos)?;
        w.write_all(&self.encrypt_pw_salt)?;
        w.write_all(&self.lpf_inode.to_le_bytes())?;
        w.write_all(&self.project_quota_inum.to_le_bytes())?;
        w.write_all(&self.checksum_seed.to_le_bytes())?;
        w.write_all(&[self.wtime_high])?;
        w.write_all(&[self.mtime_high])?;
        w.write_all(&[self.mkfs_time_high])?;
        w.write_all(&[self.lastcheck_high])?;
        w.write_all(&[self.first_error_time_high])?;
        w.write_all(&[self.last_error_time_high])?;
        w.write_all(&self.pad)?;
        for r in &self.reserved {
            w.write_all(&r.to_le_bytes())?;
        }
        w.write_all(&self.checksum.to_le_bytes())?;
        Ok(())
    }
}

// ---- Group Descriptor (32 bytes) ----

#[derive(Default, Clone)]
pub struct GroupDescriptor {
    pub block_bitmap_low: u32,
    pub inode_bitmap_low: u32,
    pub inode_table_low: u32,
    pub free_blocks_count_low: u16,
    pub free_inodes_count_low: u16,
    pub used_dirs_count_low: u16,
    pub flags: u16,
    pub exclude_bitmap_low: u32,
    pub block_bitmap_csum_low: u16,
    pub inode_bitmap_csum_low: u16,
    pub itable_unused_low: u16,
    pub checksum: u16,
}

pub const GROUP_DESCRIPTOR_SIZE: usize = 32;

impl GroupDescriptor {
    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.block_bitmap_low.to_le_bytes())?;
        w.write_all(&self.inode_bitmap_low.to_le_bytes())?;
        w.write_all(&self.inode_table_low.to_le_bytes())?;
        w.write_all(&self.free_blocks_count_low.to_le_bytes())?;
        w.write_all(&self.free_inodes_count_low.to_le_bytes())?;
        w.write_all(&self.used_dirs_count_low.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&self.exclude_bitmap_low.to_le_bytes())?;
        w.write_all(&self.block_bitmap_csum_low.to_le_bytes())?;
        w.write_all(&self.inode_bitmap_csum_low.to_le_bytes())?;
        w.write_all(&self.itable_unused_low.to_le_bytes())?;
        w.write_all(&self.checksum.to_le_bytes())?;
        Ok(())
    }
}

// ---- On-disk Inode (256 bytes) ----

pub const INODE_SIZE: usize = 256;
/// Number of used bytes in the inode struct (up to and including CrtimeExtra).
pub const INODE_USED_SIZE: usize = 152;
pub const INODE_EXTRA_SIZE: usize = INODE_SIZE - INODE_USED_SIZE;
pub const INODE_DATA_SIZE: usize = 60;
pub const MAX_LINKS: u32 = 65000;

/// Write a single on-disk inode (256 bytes) to the writer.
///
/// `block` is the 60-byte inline data area (extent tree, symlink target, or device info).
/// `xattr_inline` is the extra inode space used for inline xattrs (up to INODE_EXTRA_SIZE).
#[allow(clippy::too_many_arguments)]
pub fn write_inode(
    w: &mut impl Write,
    mode: u16,
    uid: u32,
    gid: u32,
    size: i64,
    atime: u64,
    ctime: u64,
    mtime: u64,
    crtime: u64,
    links_count: u32,
    blocks_low: u32,
    flags: InodeFlags,
    xattr_block_low: u32,
    block: &[u8],
    xattr_inline: &[u8],
) -> io::Result<()> {
    let extra_isize = (INODE_USED_SIZE - 128) as u16;

    w.write_all(&mode.to_le_bytes())?;
    w.write_all(&(uid as u16).to_le_bytes())?; // uid low
    w.write_all(&(size as u32).to_le_bytes())?; // size low
    w.write_all(&(atime as u32).to_le_bytes())?;
    w.write_all(&(ctime as u32).to_le_bytes())?;
    w.write_all(&(mtime as u32).to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // dtime
    w.write_all(&(gid as u16).to_le_bytes())?; // gid low
    w.write_all(&(links_count as u16).to_le_bytes())?;
    w.write_all(&blocks_low.to_le_bytes())?;
    w.write_all(&flags.bits().to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // version

    // Block[60] — inline data or extent tree
    let mut block_buf = [0u8; INODE_DATA_SIZE];
    let copy_len = block.len().min(INODE_DATA_SIZE);
    block_buf[..copy_len].copy_from_slice(&block[..copy_len]);
    w.write_all(&block_buf)?;

    w.write_all(&0u32.to_le_bytes())?; // generation
    w.write_all(&xattr_block_low.to_le_bytes())?;
    w.write_all(&((size >> 32) as u32).to_le_bytes())?; // size high
    w.write_all(&0u32.to_le_bytes())?; // obsolete_fragment_addr
    w.write_all(&0u16.to_le_bytes())?; // blocks high
    w.write_all(&0u16.to_le_bytes())?; // xattr block high
    w.write_all(&((uid >> 16) as u16).to_le_bytes())?; // uid high
    w.write_all(&((gid >> 16) as u16).to_le_bytes())?; // gid high
    w.write_all(&0u16.to_le_bytes())?; // checksum low
    w.write_all(&0u16.to_le_bytes())?; // reserved
    w.write_all(&extra_isize.to_le_bytes())?;
    w.write_all(&0u16.to_le_bytes())?; // checksum high
    w.write_all(&((ctime >> 32) as u32).to_le_bytes())?; // ctime extra
    w.write_all(&((mtime >> 32) as u32).to_le_bytes())?; // mtime extra
    w.write_all(&((atime >> 32) as u32).to_le_bytes())?; // atime extra
    w.write_all(&(crtime as u32).to_le_bytes())?;
    w.write_all(&((crtime >> 32) as u32).to_le_bytes())?; // crtime extra
    // That's INODE_USED_SIZE = 152 bytes written.
    // Note: version_high and projid live in the extra inode space and are
    // covered by xattr_inline data/padding below (matching Go's Truncate).

    // Extra inode space — xattr inline data or zeros.
    let mut extra = [0u8; INODE_EXTRA_SIZE];
    let xattr_copy = xattr_inline.len().min(INODE_EXTRA_SIZE);
    extra[..xattr_copy].copy_from_slice(&xattr_inline[..xattr_copy]);
    w.write_all(&extra)?;
    // Total: 256 bytes.
    Ok(())
}

/// Write a zero (empty) inode slot.
pub fn write_zero_inode(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[0u8; INODE_SIZE])
}

// ---- Extent structures ----

pub const EXTENT_HEADER_MAGIC: u16 = 0xf30a;
pub const EXTENT_NODE_SIZE: usize = 12;

pub fn write_extent_header(
    w: &mut impl Write,
    entries: u16,
    max: u16,
    depth: u16,
) -> io::Result<()> {
    w.write_all(&EXTENT_HEADER_MAGIC.to_le_bytes())?;
    w.write_all(&entries.to_le_bytes())?;
    w.write_all(&max.to_le_bytes())?;
    w.write_all(&depth.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // generation
    Ok(())
}

pub fn write_extent_leaf(
    w: &mut impl Write,
    block: u32,
    length: u16,
    start: u32,
) -> io::Result<()> {
    w.write_all(&block.to_le_bytes())?;
    w.write_all(&length.to_le_bytes())?;
    w.write_all(&0u16.to_le_bytes())?; // start_high
    w.write_all(&start.to_le_bytes())?;
    Ok(())
}

pub fn write_extent_index(w: &mut impl Write, block: u32, leaf_low: u32) -> io::Result<()> {
    w.write_all(&block.to_le_bytes())?;
    w.write_all(&leaf_low.to_le_bytes())?;
    w.write_all(&0u16.to_le_bytes())?; // leaf_high
    w.write_all(&0u16.to_le_bytes())?; // unused
    Ok(())
}

// ---- Directory entry ----

/// Size of the fixed portion of a directory entry (before the name).
pub const DIR_ENTRY_HEADER_SIZE: usize = 8;

pub fn write_dir_entry(
    w: &mut impl Write,
    inode: InodeNumber,
    rec_len: u16,
    name_len: u8,
    file_type: FileType,
) -> io::Result<()> {
    w.write_all(&inode.to_le_bytes())?;
    w.write_all(&rec_len.to_le_bytes())?;
    w.write_all(&[name_len])?;
    w.write_all(&[file_type as u8])?;
    Ok(())
}

// ---- XAttr constants ----

pub const XATTR_HEADER_MAGIC: u32 = 0xea020000;
