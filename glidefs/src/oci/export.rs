/// OCI export pipeline: GlideFS block storage → ext4 → tar stream.
///
/// Reads an ext4 filesystem from GlideFS blocks and streams it as a tar
/// archive. Runs the blocking ext4 work on a dedicated thread.
use std::io::{self, Write};
use std::sync::Arc;

use crate::block::handler::BlockHandler;
use crate::ext4::reader::Reader;

use super::BlockAdapter;

/// Export GlideFS block storage as a tar stream.
///
/// Reads the ext4 filesystem from blocks and streams it as tar entries
/// including PAX xattrs. Runs on a blocking thread (required by BlockAdapter).
///
/// Returns the tar writer after all entries are written.
pub async fn export_tar<W: Write + Send + 'static>(
    handler: Arc<BlockHandler>,
    tar_writer: W,
) -> io::Result<W> {
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let adapter = BlockAdapter::new(&handler, rt);
        let mut reader = Reader::new(adapter)?;
        reader.to_tar(tar_writer)
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("blocking task panicked: {e}")))?
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
    use crate::ext4::writer::{File as Ext4File, Writer, WriterOption};
    use crate::ext4::format;
    use object_store::memory::InMemory;
    use std::io::Read;
    use std::sync::atomic::AtomicU64;
    use tempfile::TempDir;
    use tokio::sync::Notify;

    async fn test_handler(device_size: u64) -> (Arc<BlockHandler>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "export-test".to_string(),
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
            DEFAULT_BLOCKS_PER_PACK,
            None,
        ));

        (handler, temp_dir)
    }

    #[tokio::test]
    async fn test_export_basic() {
        let device_size = 16 * 1024 * 1024;
        let (handler, _temp) = test_handler(device_size).await;

        // Write ext4 image directly through BlockAdapter
        let rt = tokio::runtime::Handle::current();
        let handler2 = Arc::clone(&handler);
        tokio::task::spawn_blocking(move || {
            let adapter = BlockAdapter::new(&handler2, rt);
            let mut writer = Writer::new(adapter, &[WriterOption::MaximumDiskSize(device_size as i64)]);

            let data = b"export test content";
            writer
                .create(
                    "export.txt",
                    &Ext4File {
                        size: data.len() as i64,
                        mode: format::S_IFREG | 0o644,
                        uid: 500,
                        gid: 500,
                        mtime: 1700000000,
                        ..Default::default()
                    },
                )
                .unwrap();
            io::Write::write_all(&mut writer, data).unwrap();
            writer.close().unwrap();
        })
        .await
        .unwrap();

        // Export to tar
        let tar_data = export_tar(handler, Vec::new()).await.unwrap();

        // Parse the tar and verify
        let mut archive = tar::Archive::new(std::io::Cursor::new(tar_data));
        let mut found_file = false;
        for entry_result in archive.entries().unwrap() {
            let mut entry: tar::Entry<_> = entry_result.unwrap();
            let path = entry.path().unwrap().to_string_lossy().to_string();
            if path == "export.txt" {
                found_file = true;
                assert_eq!(entry.header().uid().unwrap(), 500);
                assert_eq!(entry.header().gid().unwrap(), 500);
                assert_eq!(entry.header().mode().unwrap(), 0o644);
                let mut content = String::new();
                entry.read_to_string(&mut content).unwrap();
                assert_eq!(content, "export test content");
            }
        }
        assert!(found_file, "export.txt not found in tar output");
    }

    #[tokio::test]
    async fn test_ingest_export_roundtrip() {
        use crate::oci::ingest::{ingest_tar, IngestOptions};

        let device_size = 16 * 1024 * 1024;
        let (handler, _temp) = test_handler(device_size).await;

        // Build input tar
        let mut builder = tar::Builder::new(Vec::new());
        let data = b"roundtrip data";
        let mut header = tar::Header::new_gnu();
        header.set_path("round.txt").unwrap();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_uid(42);
        header.set_gid(42);
        header.set_mtime(1700000000);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder.append(&header, &data[..]).unwrap();
        let input_tar = builder.into_inner().unwrap();

        // Ingest
        ingest_tar(
            Arc::clone(&handler),
            std::io::Cursor::new(input_tar),
            IngestOptions {
                writer_options: vec![WriterOption::MaximumDiskSize(device_size as i64)],
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Export
        let output_tar = export_tar(handler, Vec::new()).await.unwrap();

        // Verify roundtrip
        let mut archive = tar::Archive::new(std::io::Cursor::new(output_tar));
        let mut found = false;
        for entry_result in archive.entries().unwrap() {
            let mut entry: tar::Entry<_> = entry_result.unwrap();
            let path = entry.path().unwrap().to_string_lossy().to_string();
            if path == "round.txt" {
                found = true;
                assert_eq!(entry.header().uid().unwrap(), 42);
                let mut content = String::new();
                entry.read_to_string(&mut content).unwrap();
                assert_eq!(content, "roundtrip data");
            }
        }
        assert!(found, "round.txt not found in roundtrip tar");
    }
}
