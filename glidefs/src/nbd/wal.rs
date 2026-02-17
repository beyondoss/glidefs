//! Write-Ahead Log for crash recovery.
//!
//! Append-only log on local SSD. Each entry records which chunk was modified
//! (metadata only — no block data). On recovery, block data is re-read from the
//! SSD cache file and re-hashed. Each entry has a CRC32 trailer so replay can
//! detect and discard a torn final write. Truncated after each block map
//! persistence (~5s).

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write as IoWrite, Seek, SeekFrom, BufWriter};
use std::path::{Path, PathBuf};

use super::block_map::Blake3Hash;

/// A single WAL entry: records that a chunk was modified.
///
/// Block data is NOT stored in the WAL — on recovery, the SSD cache file
/// is the source of truth for block contents (the SSD pwrite always completes
/// before the WAL append).
///
/// Used for replay (deserialized from WAL file).
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub name: String,
    pub chunk_index: u64,
    pub hash: Blake3Hash,
    pub sequence: u64,
}

/// Borrowed WAL entry for zero-alloc appends on the write hot path.
pub struct WalEntryRef<'a> {
    pub name: &'a str,
    pub chunk_index: u64,
    pub hash: Blake3Hash,
    pub sequence: u64,
}

/// Append-only write-ahead log backed by a file on local SSD.
pub struct Wal {
    writer: BufWriter<File>,
    #[allow(dead_code)]
    path: PathBuf,
    offset: u64,
}

impl Wal {
    /// Open an existing WAL file for appending, or create a new one.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(false)
            .open(path)?;

        let offset = file.metadata()?.len();
        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::End(0))?;

        Ok(Wal {
            writer,
            path: path.to_path_buf(),
            offset,
        })
    }

    /// Serialize and append an entry in wire format.
    ///
    /// Wire format: [name_len:u16][name][chunk_index:u64][hash:16][sequence:u64][crc32:u32]
    ///
    /// Does NOT fsync -- the local SSD file provides durability guarantees.
    pub fn append(&mut self, entry: &WalEntryRef<'_>) -> io::Result<()> {
        let mut hasher = crc32fast::Hasher::new();

        let name_bytes = entry.name.as_bytes();
        let name_len = name_bytes.len() as u16;
        let name_len_le = name_len.to_le_bytes();
        hasher.update(&name_len_le);
        self.writer.write_all(&name_len_le)?;

        hasher.update(name_bytes);
        self.writer.write_all(name_bytes)?;

        let chunk_index_le = entry.chunk_index.to_le_bytes();
        hasher.update(&chunk_index_le);
        self.writer.write_all(&chunk_index_le)?;

        hasher.update(&entry.hash.0);
        self.writer.write_all(&entry.hash.0)?;

        let sequence_le = entry.sequence.to_le_bytes();
        hasher.update(&sequence_le);
        self.writer.write_all(&sequence_le)?;

        let crc = hasher.finalize();
        self.writer.write_all(&crc.to_le_bytes())?;

        // 2 + name_len + 8 + 16 + 8 + 4
        self.offset += 2 + name_len as u64 + 8 + 16 + 8 + 4;

        Ok(())
    }

    /// Flush the internal BufWriter to the OS.
    pub fn flush_buf(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Flush and fsync the WAL file to stable storage.
    pub fn sync(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        self.writer.get_ref().sync_all()
    }

    /// Flush, truncate the WAL to zero bytes, and reset the write position.
    pub fn truncate(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        let file = self.writer.get_mut();
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        self.offset = 0;
        Ok(())
    }

    /// Current logical size of the WAL in bytes.
    pub fn size(&self) -> u64 {
        self.offset
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
                Err(_) => break,    // corrupted or truncated entry
            }
        }

        Ok(entries)
    }

    /// Try to read one entry from the reader. Returns:
    /// - `Ok(Some(entry))` on success
    /// - `Ok(None)` on clean EOF (zero bytes remaining)
    /// - `Err(_)` on CRC mismatch or short read (torn entry)
    fn read_entry(reader: &mut impl Read) -> io::Result<Option<WalEntry>> {
        let mut hasher = crc32fast::Hasher::new();

        // name_len
        let mut buf2 = [0u8; 2];
        match reader.read_exact(&mut buf2) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        hasher.update(&buf2);
        let name_len = u16::from_le_bytes(buf2) as usize;

        // name
        let mut name_buf = vec![0u8; name_len];
        reader.read_exact(&mut name_buf)?;
        hasher.update(&name_buf);
        let name = String::from_utf8(name_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // chunk_index
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        hasher.update(&buf8);
        let chunk_index = u64::from_le_bytes(buf8);

        // hash
        let mut hash_buf = [0u8; 16];
        reader.read_exact(&mut hash_buf)?;
        hasher.update(&hash_buf);
        let hash = Blake3Hash(hash_buf);

        // sequence
        reader.read_exact(&mut buf8)?;
        hasher.update(&buf8);
        let sequence = u64::from_le_bytes(buf8);

        // crc32
        let mut buf4 = [0u8; 4];
        reader.read_exact(&mut buf4)?;
        let stored_crc = u32::from_le_bytes(buf4);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL entry CRC mismatch",
            ));
        }

        Ok(Some(WalEntry {
            name,
            chunk_index,
            hash,
            sequence,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::block_map::blake3_128;
    use std::io::{Seek, SeekFrom, Write as IoWrite};
    use tempfile::TempDir;

    fn make_entry_ref(seq: u64, data: &[u8]) -> (Blake3Hash, u64, u64) {
        (blake3_128(data), seq * 10, seq)
    }

    fn append_test_entry(wal: &mut Wal, seq: u64, data: &[u8]) {
        let (hash, chunk_index, sequence) = make_entry_ref(seq, data);
        let entry = WalEntryRef {
            name: "test-export",
            chunk_index,
            hash,
            sequence,
        };
        wal.append(&entry).unwrap();
    }

    #[test]
    fn test_wal_append_and_replay() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            for i in 1..=10 {
                let data = format!("block-data-{i}");
                append_test_entry(&mut wal, i, data.as_bytes());
            }
            wal.flush_buf().unwrap();
        }

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 10);

        for (i, entry) in entries.iter().enumerate() {
            let seq = (i + 1) as u64;
            let expected_data = format!("block-data-{seq}");
            assert_eq!(entry.name, "test-export");
            assert_eq!(entry.chunk_index, seq * 10);
            assert_eq!(entry.hash, blake3_128(expected_data.as_bytes()));
            assert_eq!(entry.sequence, seq);
        }
    }

    #[test]
    fn test_wal_replay_skip_old() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            for i in 1..=10 {
                append_test_entry(&mut wal, i, &[i as u8; 64]);
            }
            wal.flush_buf().unwrap();
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
            let mut wal = Wal::open(&wal_path).unwrap();
            for i in 1..=5 {
                append_test_entry(&mut wal, i, &[i as u8; 128]);
            }
            wal.flush_buf().unwrap();
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

        // Write 3 entries, tracking sizes to find the CRC location of entry 2
        let mut entry_offsets = Vec::new();
        {
            let mut wal = Wal::open(&wal_path).unwrap();
            for i in 1..=3u64 {
                let before = wal.size();
                append_test_entry(&mut wal, i, &[i as u8; 64]);
                let after = wal.size();
                entry_offsets.push((before, after));
            }
            wal.flush_buf().unwrap();
        }

        // Corrupt the CRC of entry 2 (last 4 bytes of entry 2)
        {
            let (_, end2) = entry_offsets[1];
            let crc_offset = end2 - 4;
            let mut file = OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
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
    fn test_wal_truncate_after_persist() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        let mut wal = Wal::open(&wal_path).unwrap();
        for i in 1..=5 {
            append_test_entry(&mut wal, i, &[i as u8; 32]);
        }
        wal.flush_buf().unwrap();

        wal.truncate().unwrap();
        assert_eq!(wal.size(), 0);

        for i in 10..=12 {
            append_test_entry(&mut wal, i, &[i as u8; 32]);
        }
        wal.flush_buf().unwrap();
        drop(wal);

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 10);
        assert_eq!(entries[1].sequence, 11);
        assert_eq!(entries[2].sequence, 12);
    }

    #[test]
    fn test_wal_trim_entry() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            let entry = WalEntryRef {
                name: "trim-export",
                chunk_index: 42,
                hash: blake3_128(&[]),
                sequence: 1,
            };
            wal.append(&entry).unwrap();
            wal.flush_buf().unwrap();
        }

        let entries = Wal::replay(&wal_path, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "trim-export");
        assert_eq!(entries[0].chunk_index, 42);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[0].hash, blake3_128(&[]));
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
}
