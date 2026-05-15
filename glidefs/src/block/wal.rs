//! Write-Ahead Log for crash recovery.
//!
//! Append-only log on local SSD. Each entry records which chunk was modified
//! (metadata only — no block data). On recovery, block data is re-read from the
//! SSD cache file and re-hashed. Each entry has a CRC32 trailer so replay can
//! detect and discard a torn final write. Truncated after each block map
//! persistence (~5s).
//!
//! # Concurrency Model
//!
//! Uses O_APPEND for lock-free concurrent appends. The kernel's inode rwsem
//! serializes all `write()` calls on the fd, guaranteeing no interleaving of
//! bytes between concurrent writers. An `RwLock<()>` provides mutual exclusion
//! between appends (read guard) and truncation (write guard) — truncation is
//! rare (~5s).

use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};

use parking_lot::RwLock;

/// A single WAL entry: records that a chunk was modified.
///
/// Block data is NOT stored in the WAL — on recovery, the SSD cache file
/// is the source of truth for block contents (the SSD pwrite always completes
/// before the WAL append).
///
/// Used for replay (deserialized from WAL file).
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub block_index: u64,
    pub sequence: u64,
}

/// Serialize a normal WAL entry (20 bytes) into the buffer.
///
/// Wire format: `[block_index:u64 LE][sequence:u64 LE][crc32:u32 LE]`
pub fn serialize_entry(buf: &mut Vec<u8>, block_index: u64, sequence: u64) {
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);

    let block_index_le = block_index.to_le_bytes();
    hasher.update(&block_index_le);
    buf.extend_from_slice(&block_index_le);

    let sequence_le = sequence.to_le_bytes();
    hasher.update(&sequence_le);
    buf.extend_from_slice(&sequence_le);

    let crc = hasher.finalize() as u32;
    buf.extend_from_slice(&crc.to_le_bytes());
}

/// Append-only write-ahead log backed by a file on local SSD.
///
/// Uses O_APPEND for lock-free concurrent appends. Multiple threads can call
/// `append_batch()` concurrently — the kernel serializes the writes. An
/// internal `RwLock<()>` ensures `truncate()` has exclusive access (blocks
/// concurrent appends during the truncate window).
pub struct Wal {
    /// O_APPEND file descriptor for atomic appends.
    fd: RawFd,
    /// Owns the fd so it gets closed on drop.
    _file: File,
    /// RwLock: read = append (concurrent), write = truncate (exclusive).
    guard: RwLock<()>,
    #[allow(dead_code)]
    path: PathBuf,
}

// SAFETY: Wal only exposes:
// - append_batch(): O_APPEND write() under read guard — kernel serializes
// - sync()/flush(): fdatasync — thread-safe on an fd
// - truncate(): ftruncate under exclusive write guard
// The RawFd is derived from the owned File and never used for seek-based I/O.
unsafe impl Sync for Wal {}

impl Wal {
    /// Open an existing WAL file for appending, or create a new one.
    ///
    /// **Future enhancement** (tracked but not yet active): we'd like to
    /// acquire `flock(LOCK_EX | LOCK_NB)` here as defense-in-depth for
    /// the graceful handoff protocol. The CRH protocol already guarantees
    /// no double-open through its freeze+drop sequence, but a stray
    /// extra-process open (operator running `glidefs ublk_cleanup` while
    /// the daemon is live, etc.) wouldn't be caught.
    ///
    /// Why it's not active: `ExportRouter::resize_export` does a
    /// drop+recreate cycle that holds the old `Arc<Wal>` transiently
    /// across await points. A non-blocking flock fails there even though
    /// no other process is involved. Re-enabling requires either:
    /// (a) auditing the Drop path of WriteCache to guarantee fd close
    /// before re-open, or (b) using a per-pid lockfile separate from
    /// the WAL file itself.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .append(true)
            .open(path)?;

        let fd = file.as_raw_fd();

        Ok(Wal {
            fd,
            _file: file,
            guard: RwLock::new(()),
            path: path.to_path_buf(),
        })
    }

    /// Append a pre-serialized batch of WAL entries to the log.
    ///
    /// Takes a read guard (concurrent with other appends, exclusive with
    /// truncation). Issues a single `write()` syscall — atomic under
    /// O_APPEND because the kernel holds the inode rwsem for the duration.
    ///
    /// The `buf` should contain one or more entries serialized via
    /// `serialize_entry()`.
    pub fn append_batch(&self, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let _guard = self.guard.read();

        // O_APPEND write: single syscall, no retry loop.
        // Short writes cannot be retried with O_APPEND because each write()
        // atomically seeks to EOF — a retry would append at the new EOF,
        // leaving a gap of partial data. For regular files, write() of
        // small buffers (< page size) should never short-write.
        let written = unsafe {
            libc::write(
                self.fd,
                buf.as_ptr() as *const libc::c_void,
                buf.len(),
            )
        };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        let written = written as usize;
        if written != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("WAL short write: expected {} bytes, wrote {}", buf.len(), written),
            ));
        }

        Ok(())
    }

    /// Sync the WAL file to stable storage.
    ///
    /// Thread-safe — fsync/fdatasync operates on kernel state, not userspace buffers.
    /// Uses fdatasync on Linux (skips metadata update), fsync on other platforms.
    pub fn sync(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        let ret = unsafe { libc::fdatasync(self.fd) };
        #[cfg(not(target_os = "linux"))]
        let ret = unsafe { libc::fsync(self.fd) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Flush kernel buffers (no-op for direct write() — included for API
    /// symmetry with the old BufWriter-based WAL).
    ///
    /// With O_APPEND + direct write(), data goes straight to the kernel
    /// page cache on each write() call. There's no userspace buffer to flush.
    /// This method exists so callers don't need to know the implementation.
    pub fn flush(&self) -> io::Result<()> {
        // No-op: write() already pushed data to kernel page cache.
        // The old BufWriter needed an explicit flush; we don't.
        Ok(())
    }

    /// Truncate the WAL to zero length.
    ///
    /// Takes an exclusive write guard, blocking all concurrent appends.
    /// This is called by the checkpoint path (~every 5 seconds).
    pub fn truncate(&self) -> io::Result<()> {
        let _guard = self.guard.write();

        let ret = unsafe { libc::ftruncate(self.fd, 0) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Replay a WAL file, returning entries with sequence > min_sequence.
    ///
    /// Returns `Ok(vec![])` if the file does not exist. Stops at the first
    /// corrupted or truncated entry (torn tail is discarded, not an error).
    pub fn replay(path: &Path, min_sequence: u64) -> io::Result<Vec<WalEntry>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e),
        };

        let mut reader = io::BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            match Self::read_entry(&mut reader) {
                Ok(Some(entry)) => {
                    if entry.sequence > min_sequence {
                        entries.push(entry);
                    }
                }
                Ok(None) => break,  // clean EOF
                Err(e) => {
                    // Log corruption type (CRC mismatch vs short read) for diagnostics.
                    // This is expected for torn writes after unclean shutdown.
                    tracing::warn!(
                        recovered = entries.len(),
                        error = %e,
                        "WAL replay stopped at corrupted/truncated entry"
                    );
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// Try to read one entry from the reader. Returns:
    /// - `Ok(Some(entry))` on success
    /// - `Ok(None)` on clean EOF (zero bytes remaining)
    /// - `Err(_)` on CRC mismatch or short read (torn entry)
    fn read_entry(reader: &mut impl Read) -> io::Result<Option<WalEntry>> {
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);

        // block_index
        let mut buf8 = [0u8; 8];
        match reader.read_exact(&mut buf8) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        hasher.update(&buf8);
        let block_index = u64::from_le_bytes(buf8);

        // sequence
        reader.read_exact(&mut buf8)?;
        hasher.update(&buf8);
        let sequence = u64::from_le_bytes(buf8);

        // crc32
        let mut buf4 = [0u8; 4];
        reader.read_exact(&mut buf4)?;
        let stored_crc = u32::from_le_bytes(buf4);
        let computed_crc = hasher.finalize() as u32;

        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL entry CRC mismatch",
            ));
        }

        Ok(Some(WalEntry {
            block_index,
            sequence,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as IoWrite;
    use tempfile::TempDir;

    fn append_test_entry(wal: &Wal, seq: u64) {
        let mut buf = Vec::new();
        serialize_entry(&mut buf, seq * 10, seq);
        wal.append_batch(&buf).unwrap();
    }

    #[test]
    fn test_wal_append_and_replay() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = Wal::open(&wal_path).unwrap();
            for i in 1..=10 {
                append_test_entry(&wal, i);
            }
        }

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 10);

        for (i, entry) in entries.iter().enumerate() {
            let seq = (i + 1) as u64;
            assert_eq!(entry.block_index, seq * 10);
            assert_eq!(entry.sequence, seq);
        }
    }

    #[test]
    fn test_wal_replay_skip_old() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = Wal::open(&wal_path).unwrap();
            for i in 1..=10 {
                append_test_entry(&wal, i);
            }
        }

        let entries = Wal::replay(&wal_path, 5).unwrap();
        assert_eq!(entries.len(), 5);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, (i + 6) as u64);
        }
    }

    #[test]
    fn test_wal_truncated_entry() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = Wal::open(&wal_path).unwrap();
            for i in 1..=5 {
                append_test_entry(&wal, i);
            }
        }

        // Append a partial 6th entry by writing some garbage bytes at the end
        {
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            // Write a partial header (less than a full entry)
            file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02]).unwrap();
            file.flush().unwrap();
        }

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn test_wal_crc_corruption() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write 3 entries
        {
            let wal = Wal::open(&wal_path).unwrap();
            for i in 1..=3u64 {
                append_test_entry(&wal, i);
            }
        }

        // Corrupt the CRC of entry 2 (last 4 bytes of entry 2)
        {
            // Each entry is 20 bytes. Entry 2 ends at byte 40.
            let crc_offset = 40 - 4; // = 36
            let mut file = OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(crc_offset)).unwrap();
            let mut crc_bytes = [0u8; 4];
            file.read_exact(&mut crc_bytes).unwrap();
            crc_bytes[0] ^= 0xFF; // flip bits
            file.seek(SeekFrom::Start(crc_offset)).unwrap();
            file.write_all(&crc_bytes).unwrap();
            file.flush().unwrap();
        }

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 1, "should recover only entry 1 before corruption");
        assert_eq!(entries[0].sequence, 1);
    }

    #[test]
    fn test_wal_truncate() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        let wal = Wal::open(&wal_path).unwrap();
        for i in 1..=5 {
            append_test_entry(&wal, i);
        }

        // Truncate the WAL
        wal.truncate().unwrap();

        // New entries should work
        for i in 10..=12 {
            append_test_entry(&wal, i);
        }
        drop(wal);

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 10);
        assert_eq!(entries[1].sequence, 11);
        assert_eq!(entries[2].sequence, 12);
    }

    #[test]
    fn test_wal_single_entry() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = Wal::open(&wal_path).unwrap();
            let mut buf = Vec::new();
            serialize_entry(&mut buf, 42, 1);
            wal.append_batch(&buf).unwrap();
        }

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].block_index, 42);
        assert_eq!(entries[0].sequence, 1);
    }

    #[test]
    fn test_wal_first_entry_corrupted() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write 3 entries
        {
            let wal = Wal::open(&wal_path).unwrap();
            for i in 1..=3u64 {
                append_test_entry(&wal, i);
            }
        }

        // Corrupt the CRC of entry 1 (the very first entry)
        {
            // Entry 1: block_index(8) + seq(8) + crc(4) = 20 bytes
            let entry_1_end: u64 = 20;
            let crc_offset = entry_1_end - 4;
            let mut file = OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(crc_offset)).unwrap();
            let mut crc_bytes = [0u8; 4];
            file.read_exact(&mut crc_bytes).unwrap();
            crc_bytes[0] ^= 0xFF; // flip bits
            file.seek(SeekFrom::Start(crc_offset)).unwrap();
            file.write_all(&crc_bytes).unwrap();
            file.flush().unwrap();
        }

        // Replay should recover zero entries — corruption at entry 0 means everything is lost
        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 0, "corrupted first entry should yield zero recovered entries");
    }

    #[test]
    fn test_wal_all_entries_corrupted() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = Wal::open(&wal_path).unwrap();
            for i in 1..=3u64 {
                append_test_entry(&wal, i);
            }
        }

        // Overwrite entire file with garbage
        {
            let len = std::fs::metadata(&wal_path).unwrap().len() as usize;
            let garbage = vec![0xDE; len];
            std::fs::write(&wal_path, &garbage).unwrap();
        }

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert!(entries.is_empty(), "fully corrupted WAL should yield empty replay");
    }

    #[test]
    fn test_wal_empty_replay() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("empty.wal");

        // Create an empty file
        File::create(&wal_path).unwrap();

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_missing_file_replay() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("nonexistent.wal");

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_concurrent_appends() {
        use std::sync::Arc;

        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("concurrent.wal");

        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let wal = Arc::clone(&wal);
                std::thread::spawn(move || {
                    for i in 1..=100 {
                        let seq = (t * 1000 + i) as u64;
                        let mut buf = Vec::new();
                        serialize_entry(&mut buf, seq, seq);
                        wal.append_batch(&buf).unwrap();
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }
        drop(wal);

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 800, "all 8×100 entries should be recoverable");

        // All sequence numbers should be present (order may vary)
        let mut seqs: Vec<u64> = entries.iter().map(|e| e.sequence).collect();
        seqs.sort();
        let mut expected: Vec<u64> = (0..8)
            .flat_map(|t| (1..=100).map(move |i| (t * 1000 + i) as u64))
            .collect();
        expected.sort();
        assert_eq!(seqs, expected);
    }

    #[test]
    fn test_serialize_entry_format() {
        let mut buf = Vec::new();
        serialize_entry(&mut buf, 42, 7);
        assert_eq!(buf.len(), 20); // 8 + 8 + 4
    }

}
