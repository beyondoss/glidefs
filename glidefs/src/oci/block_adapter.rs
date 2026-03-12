/// Adapter providing `Read + Write + Seek` over GlideFS block storage.
///
/// Bridges the ext4 Writer/Reader (which use std::io traits) with the
/// BlockHandler (which uses async byte-offset methods).
///
/// BlockHandler::write() is async (may fetch S3 data for sub-block backfill),
/// so the Write impl uses `Handle::block_on()` like the Read impl.
/// BlockHandler::read() is also async.
///
/// **Must be used from a blocking context** (`spawn_blocking` or a dedicated
/// thread). Calling `Read::read` from an async worker thread will panic.
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::block::handler::BlockHandler;

pub struct BlockAdapter<'a> {
    handler: &'a BlockHandler,
    rt: tokio::runtime::Handle,
    pos: u64,
    size: u64,
}

impl<'a> BlockAdapter<'a> {
    pub fn new(handler: &'a BlockHandler, rt: tokio::runtime::Handle) -> Self {
        Self {
            handler,
            rt,
            pos: 0,
            size: handler.device_size(),
        }
    }
}

impl Write for BlockAdapter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let remaining = self.size.saturating_sub(self.pos);
        if remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write beyond device size",
            ));
        }
        let n = buf.len().min(remaining as usize);
        self.rt
            .block_on(self.handler.write(self.pos, &buf[..n], false))
            .map_err(|e| io::Error::other(format!("{e:?}")))?;
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.handler
            .flush()
            .map_err(|e| io::Error::other(format!("{e:?}")))
    }
}

impl Read for BlockAdapter<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let remaining = self.size.saturating_sub(self.pos);
        if remaining == 0 {
            return Ok(0);
        }
        let n = buf.len().min(remaining as usize);
        let data = self
            .rt
            .block_on(self.handler.read(self.pos, n as u32))
            .map_err(|e| io::Error::other(format!("{e:?}")))?;
        let actual = data.len().min(buf.len());
        buf[..actual].copy_from_slice(&data[..actual]);
        self.pos += actual as u64;
        Ok(actual)
    }
}

impl Seek for BlockAdapter<'_> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(n) => i64::try_from(n).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "seek offset too large")
            })?,
            SeekFrom::End(n) => self.size as i64 + n,
            SeekFrom::Current(n) => self.pos as i64 + n,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::cache::SimpleBlockCache;
    use crate::block::content_store::ContentStore;
    use crate::block::metrics::ExportMetrics;
    use crate::block::pack::DEFAULT_BLOCKS_PER_PACK;
    use crate::block::pack_index_cache::PackIndexCache;
    use crate::block::volume_manifest::VolumeManifest;
    use crate::block::write_cache::{WriteCache, WriteCacheConfig};
    use object_store::memory::InMemory;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Notify;

    /// Create a test BlockHandler with 1 MiB device, 4 KiB blocks.
    async fn test_handler() -> (BlockHandler, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "block-adapter-test".to_string(),
            device_size: 1024 * 1024,
            block_size: 4096,
            wal_sync: false,
        };

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
        let clean_cache: Arc<dyn crate::block::cache::BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            1024 * 1024,
            4096,
        )));
        let metrics = Arc::new(ExportMetrics::new());

        let cache = WriteCache::open(config).unwrap();
        let cache = cache.skip_recovery_for_test();
        let handler = BlockHandler::new(
            Arc::new(cache),
            content_store,
            clean_cache,
            pack_index_cache,
            volume_manifest,
            1024 * 1024,
            false,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::new(Notify::const_new()),
            DEFAULT_BLOCKS_PER_PACK,
            None,
        );

        (handler, temp_dir)
    }

    #[tokio::test]
    async fn test_write_read_roundtrip() {
        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let mut adapter = BlockAdapter::new(&handler, rt);

            // Write pattern data
            let data: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
            adapter.write_all(&data).unwrap();

            // Seek back and read
            adapter.seek(SeekFrom::Start(0)).unwrap();
            let mut buf = vec![0u8; 4096];
            adapter.read_exact(&mut buf).unwrap();
            assert_eq!(buf, data);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_seek_positions() {
        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let mut adapter = BlockAdapter::new(&handler, rt);

            // Start
            assert_eq!(adapter.seek(SeekFrom::Start(100)).unwrap(), 100);
            assert_eq!(adapter.seek(SeekFrom::Start(0)).unwrap(), 0);

            // Current
            adapter.seek(SeekFrom::Start(50)).unwrap();
            assert_eq!(adapter.seek(SeekFrom::Current(10)).unwrap(), 60);
            assert_eq!(adapter.seek(SeekFrom::Current(-20)).unwrap(), 40);

            // End
            assert_eq!(
                adapter.seek(SeekFrom::End(0)).unwrap(),
                1024 * 1024
            );
            assert_eq!(
                adapter.seek(SeekFrom::End(-100)).unwrap(),
                1024 * 1024 - 100
            );

            // Negative seek should error
            assert!(adapter.seek(SeekFrom::Start(0)).is_ok());
            assert!(adapter.seek(SeekFrom::Current(-1)).is_err());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_unwritten_reads_zeros() {
        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let mut adapter = BlockAdapter::new(&handler, rt);

            let mut buf = vec![0xFFu8; 1024];
            adapter.read_exact(&mut buf).unwrap();
            assert!(buf.iter().all(|&b| b == 0));
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_write_at_end_returns_write_zero() {
        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let mut adapter = BlockAdapter::new(&handler, rt);

            // Seek to end
            adapter.seek(SeekFrom::End(0)).unwrap();

            // Write should return WriteZero
            let result = adapter.write(&[42u8; 100]);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WriteZero);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_read_at_end_returns_zero() {
        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let mut adapter = BlockAdapter::new(&handler, rt);

            adapter.seek(SeekFrom::End(0)).unwrap();
            let mut buf = [0u8; 100];
            let n = adapter.read(&mut buf).unwrap();
            assert_eq!(n, 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_large_write_spanning_blocks() {
        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let mut adapter = BlockAdapter::new(&handler, rt);

            // Write 256 KiB (spans multiple 4 KiB blocks and multiple 128 KiB GlideFS blocks)
            let size = 256 * 1024;
            let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            adapter.write_all(&data).unwrap();

            // Read back
            adapter.seek(SeekFrom::Start(0)).unwrap();
            let mut buf = vec![0u8; size];
            adapter.read_exact(&mut buf).unwrap();
            assert_eq!(buf, data);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_ext4_roundtrip_through_blocks() {
        use ext4::reader::Reader;
        use ext4::writer::{File, Writer, WriterOption};
        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let adapter = BlockAdapter::new(&handler, rt.clone());

            // Write ext4 image through the adapter
            let mut writer = Writer::new(adapter, &[WriterOption::MaximumDiskSize(1024 * 1024)]);

            // Create a regular file
            let file_data = b"hello from GlideFS blocks!";
            writer
                .create(
                    "test.txt",
                    &File {
                        size: file_data.len() as i64,
                        mode: 0o100644,
                        uid: 1000,
                        gid: 1000,
                        mtime: 1700000000,
                        ..Default::default()
                    },
                )
                .unwrap();
            writer.write_all(file_data).unwrap();

            // Create a directory
            writer
                .create(
                    "mydir",
                    &File {
                        mode: 0o040755,
                        uid: 0,
                        gid: 0,
                        ..Default::default()
                    },
                )
                .unwrap();

            // Create a symlink
            writer
                .create(
                    "link",
                    &File {
                        linkname: "test.txt".to_string(),
                        mode: 0o120777,
                        ..Default::default()
                    },
                )
                .unwrap();

            // Finalize
            let _adapter = writer.close().unwrap();

            // Now read it back through a fresh adapter
            let adapter = BlockAdapter::new(&handler, rt);
            let mut reader = Reader::new(adapter).unwrap();

            let entries = reader.walk().unwrap();

            // Find our file
            let file_entry = entries.iter().find(|e| e.path == "test.txt").unwrap();
            assert_eq!(file_entry.uid, 1000);
            assert_eq!(file_entry.gid, 1000);
            assert_eq!(file_entry.mode & 0o7777, 0o644);
            assert_eq!(file_entry.size, file_data.len() as u64);
            assert_eq!(file_entry.mtime, 1700000000);

            // Find directory
            let dir_entry = entries.iter().find(|e| e.path == "mydir").unwrap();
            assert_eq!(dir_entry.mode & 0o7777, 0o755);

            // Find symlink
            let link_entry = entries.iter().find(|e| e.path == "link").unwrap();
            assert_eq!(
                link_entry.symlink_target.as_deref(),
                Some("test.txt")
            );
        })
        .await
        .unwrap();
    }
}
