/// OCI ingest pipeline: tar stream → ext4 → GlideFS block storage.
///
/// Converts a tar archive into a compact ext4 filesystem and writes it
/// directly into GlideFS blocks via BlockAdapter. Runs the blocking ext4
/// work on a dedicated thread.
use std::io::{self, Read, Write};
use std::sync::Arc;

use crate::block::handler::BlockHandler;
use ext4::tar_convert::{ConvertOptions, convert_tar_to_ext4};
use ext4::writer::WriterOption;

use super::BlockAdapter;

/// Options for tar-to-ext4 ingest.
#[derive(Default)]
pub struct IngestOptions {
    pub writer_options: Vec<WriterOption>,
}

/// Ingest a tar stream into GlideFS block storage as an ext4 filesystem.
///
/// Runs the ext4 write on a blocking thread (required by BlockAdapter).
/// After writing, flushes to local SSD. S3 sync happens in the background
/// via the flush scheduler.
#[allow(dead_code)]
pub async fn ingest_tar<R: Read + Send + 'static>(
    handler: Arc<BlockHandler>,
    tar_reader: R,
    options: IngestOptions,
) -> io::Result<()> {
    let convert_opts = ConvertOptions {
        convert_backslash: false,
        writer_options: options.writer_options,
    };
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let adapter = BlockAdapter::new(&handler, rt);
        let mut adapter = convert_tar_to_ext4(tar_reader, adapter, &convert_opts)?;
        adapter.flush()?;
        Ok(())
    })
    .await
    .map_err(|e| io::Error::other(format!("blocking task panicked: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::cache::SimpleBlockCache;
    use crate::block::content_store::ContentStore;
    use crate::block::metrics::ExportMetrics;
    use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
    use crate::block::pack_index_cache::PackIndexCache;
    use crate::block::volume_manifest::VolumeManifest;
    use crate::block::write_cache::{WriteCache, WriteCacheConfig};
    use ext4::reader::Reader;
    use object_store::memory::InMemory;
    use std::sync::atomic::AtomicU64;
    use tempfile::TempDir;
    use tokio::sync::Notify;

    async fn test_handler(device_size: u64) -> (Arc<BlockHandler>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "ingest-test".to_string(),
            device_size,
            block_size: 4096,
            wal_sync: false,
        };

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let content_store = Arc::new(ContentStore::new(Arc::clone(&object_store), "test"));
        let clean_cache: Arc<dyn crate::block::cache::BlockCache> =
            Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let pack_index_cache = Arc::new(PackIndexCache::open(temp_dir.path()).await.unwrap());
        let volume_manifest = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
            device_size,
            4096,
        )));
        let metrics = Arc::new(ExportMetrics::new());

        let cache = WriteCache::open(config).unwrap();
        let cache = cache.skip_recovery_for_test();
        let handler = Arc::new(BlockHandler::new(
            Arc::new(cache),
            content_store,
            clean_cache,
            pack_index_cache,
            volume_manifest,
            device_size,
            false,
            metrics,
            Arc::new(AtomicU64::new(0f64.to_bits())),
            Arc::new(Notify::const_new()),
            DEFAULT_FLUSH_THRESHOLD,
            None,
        ));

        (handler, temp_dir)
    }

    fn build_test_tar() -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());

        // Add a regular file
        let data = b"hello from the ingest pipeline!";
        let mut header = tar::Header::new_gnu();
        header.set_path("greeting.txt").unwrap();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_uid(1000);
        header.set_gid(1000);
        header.set_mtime(1700000000);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder.append(&header, &data[..]).unwrap();

        // Add a directory
        let mut header = tar::Header::new_gnu();
        header.set_path("subdir/").unwrap();
        header.set_size(0);
        header.set_mode(0o755);
        header.set_entry_type(tar::EntryType::Directory);
        header.set_cksum();
        builder.append(&header, &[][..]).unwrap();

        // Add a symlink
        let mut header = tar::Header::new_gnu();
        header.set_path("link").unwrap();
        header.set_size(0);
        header.set_mode(0o777);
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_link_name("greeting.txt").unwrap();
        header.set_cksum();
        builder.append(&header, &[][..]).unwrap();

        builder.into_inner().unwrap()
    }

    #[tokio::test]
    async fn test_ingest_basic() {
        let device_size = 16 * 1024 * 1024; // 16 MiB
        let (handler, _temp) = test_handler(device_size).await;
        let tar_data = build_test_tar();

        ingest_tar(
            Arc::clone(&handler),
            std::io::Cursor::new(tar_data),
            IngestOptions {
                writer_options: vec![WriterOption::MaximumDiskSize(device_size as i64)],
            },
        )
        .await
        .unwrap();

        // Verify by reading back through BlockAdapter + Reader
        let rt = tokio::runtime::Handle::current();
        let result = tokio::task::spawn_blocking(move || {
            let adapter = BlockAdapter::new(&handler, rt);
            let mut reader = Reader::new(adapter).unwrap();
            reader.walk().unwrap()
        })
        .await
        .unwrap();

        let file = result.iter().find(|e| e.path == "greeting.txt").unwrap();
        assert_eq!(file.size, 31);
        assert_eq!(file.uid, 1000);
        assert_eq!(file.mode & 0o7777, 0o644);

        let dir = result.iter().find(|e| e.path == "subdir").unwrap();
        assert_eq!(dir.mode & 0o7777, 0o755);

        let link = result.iter().find(|e| e.path == "link").unwrap();
        assert_eq!(link.symlink_target.as_deref(), Some("greeting.txt"));
    }

    #[tokio::test]
    async fn test_ingest_with_journal() {
        let device_size = 32 * 1024 * 1024; // 32 MiB
        let (handler, _temp) = test_handler(device_size).await;
        let tar_data = build_test_tar();
        let uuid = [0x42u8; 16];

        ingest_tar(
            Arc::clone(&handler),
            std::io::Cursor::new(tar_data),
            IngestOptions {
                writer_options: vec![
                    WriterOption::MaximumDiskSize(device_size as i64),
                    WriterOption::Uuid(uuid),
                    WriterOption::Journal(256),
                ],
            },
        )
        .await
        .unwrap();

        // Verify superblock has journal
        let rt = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let adapter = BlockAdapter::new(&handler, rt);
            let reader = Reader::new(adapter).unwrap();
            let sb = reader.superblock();
            assert!(sb.feature_compat.contains(ext4::format::CompatFeature::HAS_JOURNAL));
            assert_eq!(sb.journal_inum, ext4::format::INODE_JOURNAL);
            assert_eq!(sb.uuid, uuid);
        })
        .await
        .unwrap();
    }
}
