#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
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
    use crate::block::pack::DEFAULT_FLUSH_THRESHOLD;
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
        test_handler_sized(1024 * 1024).await
    }

    /// Create a test BlockHandler with a custom device size (4 KiB blocks).
    async fn test_handler_sized(device_size: u64) -> (BlockHandler, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WriteCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            device_name: "block-adapter-test".to_string(),
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
        let handler = BlockHandler::new(
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

    /// A merged EROFS image (the glid(ero)fs format) must store into a real
    /// glidefs volume through the production `BlockAdapter` sink and read back
    /// byte-exact — proving GlideFS can serve EROFS images, not just ext4.
    #[tokio::test]
    async fn erofs_image_stores_and_reads_back_byte_exact() {
        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            use std::io::Cursor;
            let mk = |entries: &[(&str, &[u8])]| -> Vec<u8> {
                let mut b = tar::Builder::new(Vec::new());
                for (p, d) in entries {
                    let mut h = tar::Header::new_gnu();
                    h.set_path(p).unwrap();
                    h.set_size(d.len() as u64);
                    h.set_mode(0o644);
                    h.set_entry_type(tar::EntryType::Regular);
                    h.set_cksum();
                    b.append(&h, *d).unwrap();
                }
                b.into_inner().unwrap()
            };
            let l0 = mk(&[("etc/os", b"base"), ("bin/sh", b"#!/bin/sh\n")]);
            let l1 = mk(&[("etc/os", b"top"), ("app/run", b"hi")]);
            let opts = ext4::tar_convert::ConvertOptions {
                convert_backslash: false,
                writer_options: vec![ext4::writer::WriterOption::Uuid([9u8; 16])],
            };

            // Reference: the same merged EROFS built in memory.
            let ref_img = {
                let mut layers = vec![Cursor::new(l0.clone()), Cursor::new(l1.clone())];
                ext4::convert_oci_layers_to_erofs(&mut layers, Cursor::new(Vec::new()), &opts)
                    .unwrap()
                    .into_inner()
            };
            assert_eq!(&ref_img[1024..1028], &[0xe2, 0xe1, 0xf5, 0xe0], "EROFS magic");

            // Write the merged EROFS straight into the glidefs volume.
            let mut layers = vec![Cursor::new(l0), Cursor::new(l1)];
            let mut adapter = ext4::convert_oci_layers_to_erofs(
                &mut layers,
                BlockAdapter::new(&handler, rt),
                &opts,
            )
            .unwrap();
            adapter.flush().unwrap();

            // Read the whole device back; compare the image prefix byte-for-byte.
            adapter.seek(SeekFrom::Start(0)).unwrap();
            let mut buf = vec![0u8; 1024 * 1024];
            adapter.read_exact(&mut buf).unwrap();
            assert_eq!(
                &buf[..ref_img.len()],
                &ref_img[..],
                "EROFS image must round-trip byte-exact through glidefs storage"
            );
        })
        .await
        .unwrap();
    }

    /// END-TO-END on the homelab: serve a merged EROFS image (the glid(ero)fs
    /// format) over a REAL ublk block device and mount it with the in-kernel
    /// `erofs` driver — no daemon — then verify the overlay-merged content. This
    /// exercises the full production serve path: kernel EROFS → ublk → glidefs
    /// BlockHandler → cache/ContentStore. Requires the `ublk` feature + root +
    /// `/dev/ublk-control`.
    #[cfg(feature = "ublk")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn erofs_served_over_real_ublk_kernel_mount() {
        use std::process::Command;
        use std::sync::Arc;

        if !std::path::Path::new("/dev/ublk-control").exists() {
            eprintln!("SKIP: /dev/ublk-control not present");
            return;
        }

        let (handler, _temp) = test_handler().await;
        let rt = tokio::runtime::Handle::current();

        // Build a merged EROFS from two overlay layers and write it into the
        // glidefs volume through the production BlockAdapter sink.
        let handler = tokio::task::spawn_blocking(move || {
            use std::io::Cursor;
            let mk = |entries: &[(&str, &[u8])]| -> Vec<u8> {
                let mut b = tar::Builder::new(Vec::new());
                for (p, d) in entries {
                    let mut h = tar::Header::new_gnu();
                    h.set_path(p).unwrap();
                    h.set_size(d.len() as u64);
                    h.set_mode(0o644);
                    h.set_entry_type(tar::EntryType::Regular);
                    h.set_cksum();
                    b.append(&h, *d).unwrap();
                }
                b.into_inner().unwrap()
            };
            let l0 = mk(&[("etc/hello", b"from-base"), ("bin/run", b"#!/bin/sh\n")]);
            let l1 = mk(&[("etc/hello", b"from-top"), ("app/data", b"payload")]);
            let opts = ext4::tar_convert::ConvertOptions {
                convert_backslash: false,
                writer_options: vec![ext4::writer::WriterOption::Uuid([5u8; 16])],
            };
            let mut layers = vec![Cursor::new(l0), Cursor::new(l1)];
            {
                let mut a = ext4::convert_oci_layers_to_erofs(
                    &mut layers,
                    BlockAdapter::new(&handler, rt),
                    &opts,
                )
                .unwrap();
                a.flush().unwrap();
            }
            handler
        })
        .await
        .unwrap();

        // Serve it over a real ublk block device.
        let handler = Arc::new(handler);
        let mut server = crate::block::ublk::UblkServer::new();
        let dev = server
            .add_device("erofs-serve-test", Arc::clone(&handler))
            .await
            .expect("register ublk device");
        eprintln!("serving glid(ero)fs EROFS over real ublk device {}", dev.display());

        let mnt = tempfile::tempdir().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let m = Command::new("mount")
                .args(["-t", "erofs", "-o", "ro"])
                .arg(&dev)
                .arg(mnt.path())
                .status()
                .expect("spawn mount");
            assert!(m.success(), "in-kernel erofs mount over ublk failed");
            let read = |p: &str| std::fs::read_to_string(mnt.path().join(p)).unwrap();
            assert_eq!(read("etc/hello"), "from-top", "overlay override served over ublk");
            assert_eq!(read("app/data"), "payload");
            assert_eq!(read("bin/run"), "#!/bin/sh\n");
            let _ = Command::new("umount").arg(mnt.path()).status();
        }));
        server.remove_device("erofs-serve-test").await.ok();
        if let Err(e) = result {
            let _ = Command::new("umount").arg(mnt.path()).status();
            std::panic::resume_unwind(e);
        }
    }

    /// END-TO-END layout-at-scale: serve a realistically-sized EROFS image built
    /// with grid alignment (dedup) — holes + a multi-grid-block file + a long
    /// tail of inline small files — over a REAL ublk device + the in-kernel
    /// `erofs` driver, and verify every file reads back byte-exact. Tiny images
    /// can't expose layout bugs that only appear with alignment holes and
    /// multi-block files — this is the test that does.
    #[cfg(feature = "ublk")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn erofs_aligned_served_over_ublk() {
        use std::process::Command;
        use std::sync::Arc;

        if !std::path::Path::new("/dev/ublk-control").exists() {
            eprintln!("SKIP: /dev/ublk-control not present");
            return;
        }

        const GRID: u32 = 128 * 1024;
        // Deterministic, high-entropy payload (non-zero so blocks aren't holes).
        fn payload(seed: u64, len: usize) -> Vec<u8> {
            let mut x = seed | 1;
            (0..len)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    (x >> 24) as u8
                })
                .collect()
        }

        // Expected contents, built once so we can verify reads against them.
        let big = payload(0xB16, 1_500_000); // ~1.5 MB → spans many 128 KiB grids
        let med1 = payload(0x111, 200 * 1024);
        let med2 = payload(0x222, 200 * 1024);
        let smalls: Vec<(String, Vec<u8>)> =
            (0..40).map(|i| (format!("etc/f{i:02}"), payload(1000 + i, 1500 + i as usize * 7))).collect();

        let image = tokio::task::spawn_blocking({
            let big = big.clone();
            let med1 = med1.clone();
            let med2 = med2.clone();
            let smalls = smalls.clone();
            move || {
                use std::io::Cursor;
                let mut b = tar::Builder::new(Vec::new());
                let mut add = |path: &str, data: &[u8], mode: u32| {
                    let mut h = tar::Header::new_gnu();
                    h.set_path(path).unwrap();
                    h.set_size(data.len() as u64);
                    h.set_mode(mode);
                    h.set_entry_type(tar::EntryType::Regular);
                    h.set_cksum();
                    b.append(&h, data).unwrap();
                };
                add("big.bin", &big, 0o644);
                add("lib/med1.so", &med1, 0o644);
                add("lib/med2.so", &med2, 0o644);
                add("bin/run", b"#!/bin/sh\necho hi\n", 0o755);
                for (p, d) in &smalls {
                    add(p, d, 0o644);
                }
                // A symlink to the big file (read content through it too).
                let mut hl = tar::Header::new_gnu();
                hl.set_path("biglink").unwrap();
                hl.set_size(0);
                hl.set_mode(0o777);
                hl.set_entry_type(tar::EntryType::Symlink);
                hl.set_link_name("big.bin").unwrap();
                hl.set_cksum();
                b.append(&hl, &[][..]).unwrap();
                let layer = b.into_inner().unwrap();

                // Grid alignment for cross-image dedup: large file payloads snap
                // to the 128 KiB block grid (holes the block layer never stores).
                let opts = ext4::tar_convert::ConvertOptions {
                    convert_backslash: false,
                    writer_options: vec![
                        ext4::writer::WriterOption::Uuid([3u8; 16]),
                        ext4::writer::WriterOption::AlignData { align: GRID, min_size: 16 * 1024 },
                    ],
                };
                let mut layers = vec![Cursor::new(layer)];
                ext4::convert_oci_layers_to_erofs(
                    &mut layers,
                    Cursor::new(Vec::new()),
                    &opts,
                )
                .map(|c| c.into_inner())
                .unwrap()
            }
        })
        .await
        .unwrap();

        // Device large enough for the aligned (hole-inflated) image.
        let device = (image.len() as u64 + GRID as u64).next_power_of_two().max(32 * 1024 * 1024);
        let (handler, _temp) = test_handler_sized(device).await;
        let rt = tokio::runtime::Handle::current();

        // Write the image into the glidefs volume through the production sink.
        let handler = tokio::task::spawn_blocking(move || {
            let mut a = BlockAdapter::new(&handler, rt);
            a.write_all(&image).unwrap();
            a.flush().unwrap();
            handler
        })
        .await
        .unwrap();

        // Serve over real ublk + mount with the in-kernel erofs driver.
        let handler = Arc::new(handler);
        let mut server = crate::block::ublk::UblkServer::new();
        let dev = server
            .add_device("erofs-aligned", Arc::clone(&handler))
            .await
            .expect("register ublk device");

        let mnt = tempfile::tempdir().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let m = Command::new("mount")
                .args(["-t", "erofs", "-o", "ro"])
                .arg(&dev)
                .arg(mnt.path())
                .status()
                .expect("spawn mount");
            assert!(m.success(), "kernel erofs mount of aligned image failed");

            let rd = |p: &str| std::fs::read(mnt.path().join(p)).unwrap();
            assert_eq!(rd("big.bin"), big, "multi-grid file must read byte-exact");
            assert_eq!(rd("biglink"), big, "symlink to big file resolves + reads exact");
            assert_eq!(rd("lib/med1.so"), med1, "aligned medium file 1 byte-exact");
            assert_eq!(rd("lib/med2.so"), med2, "aligned medium file 2 byte-exact");
            assert_eq!(rd("bin/run"), b"#!/bin/sh\necho hi\n");
            // Every inline small file (the long tail) must be intact.
            for (p, d) in &smalls {
                assert_eq!(&rd(p), d, "small file {p} must read byte-exact");
            }
            let _ = Command::new("umount").arg(mnt.path()).status();
        }));
        server.remove_device("erofs-aligned").await.ok();
        if let Err(e) = result {
            let _ = Command::new("umount").arg(mnt.path()).status();
            std::panic::resume_unwind(e);
        }
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
