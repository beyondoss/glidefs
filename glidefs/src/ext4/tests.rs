#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{self, Cursor, Read};

    use crate::ext4::format::{self, MAX_LINKS, TYPE_MASK};
    use crate::ext4::writer::{File, Writer, WriterOption, BLOCK_SIZE, INLINE_DATA_SIZE};

    // ---- Test data ----

    fn test_data() -> Vec<u8> {
        let mut data = vec![0u8; BLOCK_SIZE as usize * 2];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i & 0xff) as u8;
        }
        data
    }

    fn test_name() -> String {
        let mut nameb = vec![0u8; 300];
        for (i, b) in nameb.iter_mut().enumerate() {
            *b = b'0' + (i % 10) as u8;
        }
        String::from_utf8(nameb).unwrap()
    }

    /// Position-based data reader for large file tests.
    /// Generates deterministic data based on byte offset.
    struct LargeData {
        pos: i64,
    }

    impl Read for LargeData {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let p = self.pos;
            for (i, b) in buf.iter_mut().enumerate() {
                let pos_bytes = ((p + i as i64) as u64).to_le_bytes();
                *b = pos_bytes[i % 8];
            }
            self.pos += buf.len() as i64;
            Ok(buf.len())
        }
    }

    // ---- Test file descriptor ----

    struct TestFile {
        path: String,
        file: Option<File>,
        data: Vec<u8>,
        data_size: i64,
        link: String,
        expect_error: bool,
    }

    impl TestFile {
        fn new_file(path: &str, f: File) -> Self {
            Self { path: path.to_string(), file: Some(f), data: Vec::new(), data_size: 0, link: String::new(), expect_error: false }
        }

        fn new_file_with_data(path: &str, f: File, data: &[u8]) -> Self {
            Self { path: path.to_string(), file: Some(f), data: data.to_vec(), data_size: 0, link: String::new(), expect_error: false }
        }

        fn new_file_large(path: &str, f: File, data_size: i64) -> Self {
            Self { path: path.to_string(), file: Some(f), data: Vec::new(), data_size, link: String::new(), expect_error: false }
        }

        fn new_link(path: &str, link: &str) -> Self {
            Self { path: path.to_string(), file: None, data: Vec::new(), data_size: 0, link: link.to_string(), expect_error: false }
        }

        fn with_error(mut self) -> Self {
            self.expect_error = true;
            self
        }

        fn reader(&self) -> Box<dyn Read> {
            if self.data_size != 0 {
                Box::new(io::Read::take(LargeData { pos: 0 }, self.data_size as u64))
            } else {
                Box::new(Cursor::new(self.data.clone()))
            }
        }
    }

    fn create_test_file(w: &mut Writer<Cursor<Vec<u8>>>, tf: &mut TestFile) -> Result<(), String> {
        let result = if let Some(ref mut file) = tf.file {
            file.size = if tf.data.is_empty() && tf.data_size > 0 { tf.data_size } else { tf.data.len() as i64 };
            w.create(&tf.path, file)
        } else {
            w.link(&tf.link, &tf.path)
        };

        match (tf.expect_error, &result) {
            (true, Ok(())) => return Err(format!("{}: expected error", tf.path)),
            (false, Err(e)) => return Err(format!("{}: {e}", tf.path)),
            (true, Err(_)) => return Ok(()), // Expected error, skip data write
            (false, Ok(())) => {}
        }

        // Write data
        let mut reader = tf.reader();
        io::copy(&mut reader, w).map_err(|e| format!("{}: write data: {e}", tf.path))?;
        Ok(())
    }

    fn clone_file(f: &File) -> File {
        File {
            linkname: f.linkname.clone(),
            size: f.size,
            mode: f.mode,
            uid: f.uid,
            gid: f.gid,
            atime: f.atime,
            ctime: f.ctime,
            mtime: f.mtime,
            crtime: f.crtime,
            devmajor: f.devmajor,
            devminor: f.devminor,
            xattrs: f.xattrs.clone(),
        }
    }

    fn expected_mode(f: &File) -> u16 {
        match f.mode & TYPE_MASK {
            0 => f.mode | format::S_IFREG,
            format::S_IFLNK => f.mode | 0o777,
            _ => f.mode,
        }
    }

    fn expected_size(f: &File) -> i64 {
        match f.mode & TYPE_MASK {
            0 | format::S_IFREG => f.size,
            format::S_IFLNK => f.linkname.len() as i64,
            _ => 0,
        }
    }

    fn file_equal(f1: &File, f2: &File) -> bool {
        f1.linkname == f2.linkname
            && expected_size(f1) == expected_size(f2)
            && expected_mode(f1) == expected_mode(f2)
            && f1.uid == f2.uid
            && f1.gid == f2.gid
            && f1.atime == f2.atime
            && f1.ctime == f2.ctime
            && f1.mtime == f2.mtime
            && f1.crtime == f2.crtime
            && f1.devmajor == f2.devmajor
            && f1.devminor == f2.devminor
            && f1.xattrs == f2.xattrs
    }

    fn run_tests_on_files(test_files: &mut [TestFile], opts: &[WriterOption]) {
        let buf = Cursor::new(Vec::new());
        let mut w = Writer::new(buf, opts);

        for tf in test_files.iter_mut() {
            if let Err(e) = create_test_file(&mut w, tf) {
                panic!("{e}");
            }
            // Stat check
            if !tf.expect_error && tf.file.is_some() {
                match w.stat(&tf.path) {
                    Ok(stat) => {
                        let original = tf.file.as_ref().unwrap();
                        let mut expected = clone_file(original);
                        expected.size = if tf.data.is_empty() && tf.data_size > 0 {
                            tf.data_size
                        } else {
                            tf.data.len() as i64
                        };
                        if !file_equal(&stat, &expected) {
                            panic!("{}: stat mismatch:\n  expected mode={:#o} size={}\n  got mode={:#o} size={}",
                                tf.path, expected_mode(&expected), expected_size(&expected),
                                expected_mode(&stat), expected_size(&stat));
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if !msg.contains("cannot retrieve") {
                            panic!("{}: stat error: {e}", tf.path);
                        }
                    }
                }
            }
        }

        // Close and get the output
        let output = w.close().expect("close failed");
        let bytes = output.into_inner();
        assert!(!bytes.is_empty(), "output should not be empty");

        // Verify superblock magic at offset 1024+56
        let magic = u16::from_le_bytes([bytes[1024 + 56], bytes[1024 + 57]]);
        assert_eq!(magic, format::SUPER_BLOCK_MAGIC, "superblock magic mismatch");
    }

    // ---- Tests ----

    #[test]
    fn test_basic() {
        let data = test_data();
        let name = test_name();
        let now = 1700000000u64; // a fixed timestamp for determinism

        let mut test_files = vec![
            TestFile::new_file("empty", File { mode: 0o644, ..Default::default() }),
            TestFile::new_file_with_data("small", File { mode: 0o644, ..Default::default() }, &data[..40]),
            TestFile::new_file("time", File {
                atime: now,
                ctime: now + 1,
                mtime: now + 3600,
                ..Default::default()
            }),
            TestFile::new_file_with_data("block_1", File { mode: 0o644, ..Default::default() }, &data[..BLOCK_SIZE as usize]),
            TestFile::new_file_with_data("block_2", File { mode: 0o644, ..Default::default() }, &data),
            TestFile::new_file("symlink", File { linkname: "block_1".to_string(), mode: format::S_IFLNK, ..Default::default() }),
            TestFile::new_file("symlink_59", File { linkname: name[..59].to_string(), mode: format::S_IFLNK, ..Default::default() }),
            TestFile::new_file("symlink_60", File { linkname: name[..60].to_string(), mode: format::S_IFLNK, ..Default::default() }),
            TestFile::new_file("symlink_120", File { linkname: name[..120].to_string(), mode: format::S_IFLNK, ..Default::default() }),
            TestFile::new_file("symlink_300", File { linkname: name[..300].to_string(), mode: format::S_IFLNK, ..Default::default() }),
            TestFile::new_file("dir", File { mode: format::S_IFDIR | 0o755, ..Default::default() }),
            TestFile::new_file("dir/fifo", File { mode: format::S_IFIFO, ..Default::default() }),
            TestFile::new_file("dir/sock", File { mode: format::S_IFSOCK, ..Default::default() }),
            TestFile::new_file("dir/blk", File { mode: format::S_IFBLK, devmajor: 0x5678, devminor: 0x1234, ..Default::default() }),
            TestFile::new_file("dir/chr", File { mode: format::S_IFCHR, devmajor: 0x5678, devminor: 0x1234, ..Default::default() }),
            TestFile::new_link("dir/hard_link", "small"),
        ];

        run_tests_on_files(&mut test_files, &[]);
    }

    #[test]
    fn test_large_directory() {
        let mut test_files = vec![
            TestFile::new_file("bigdir", File { mode: format::S_IFDIR | 0o755, ..Default::default() }),
        ];
        for i in 0..50_000 {
            test_files.push(TestFile::new_file(
                &format!("bigdir/{i}"),
                File { mode: 0o644, ..Default::default() },
            ));
        }
        run_tests_on_files(&mut test_files, &[]);
    }

    #[test]
    fn test_inline_data() {
        let data = test_data();
        let mut test_files = vec![
            TestFile::new_file_with_data("inline_30", File { mode: 0o644, ..Default::default() }, &data[..30]),
            TestFile::new_file_with_data("inline_60", File { mode: 0o644, ..Default::default() }, &data[..60]),
            TestFile::new_file_with_data("inline_120", File { mode: 0o644, ..Default::default() }, &data[..120]),
            TestFile::new_file_with_data("inline_full", File { mode: 0o644, ..Default::default() }, &data[..INLINE_DATA_SIZE]),
            TestFile::new_file_with_data("block_min", File { mode: 0o644, ..Default::default() }, &data[..INLINE_DATA_SIZE + 1]),
        ];
        run_tests_on_files(&mut test_files, &[WriterOption::InlineData]);
    }

    #[test]
    fn test_xattrs() {
        let data = test_data();
        let mut test_files = vec![
            TestFile::new_file("withsmallxattrs", File {
                mode: format::S_IFREG | 0o644,
                xattrs: BTreeMap::from([
                    ("user.foo".to_string(), b"test".to_vec()),
                    ("user.bar".to_string(), b"test2".to_vec()),
                ]),
                ..Default::default()
            }),
            TestFile::new_file("withlargexattrs", File {
                mode: format::S_IFREG | 0o644,
                xattrs: BTreeMap::from([
                    ("user.foo".to_string(), data[..100].to_vec()),
                    ("user.bar".to_string(), data[..50].to_vec()),
                ]),
                ..Default::default()
            }),
        ];
        run_tests_on_files(&mut test_files, &[]);
    }

    #[test]
    fn test_replace() {
        let data = test_data();
        let mut test_files = vec![
            TestFile::new_file("lost+found", File::default()).with_error(), // can't change type
            TestFile::new_file("lost+found", File { mode: format::S_IFDIR | 0o777, ..Default::default() }),
            TestFile::new_file("dir", File { mode: format::S_IFDIR | 0o777, ..Default::default() }),
            TestFile::new_file("dir/file", File::default()),
            TestFile::new_file("dir", File { mode: format::S_IFDIR | 0o700, ..Default::default() }),
            TestFile::new_file("file", File::default()),
            TestFile::new_file("file", File { mode: 0o600, ..Default::default() }),
            TestFile::new_file("file2", File::default()),
            TestFile::new_link("link", "file2"),
            TestFile::new_file("file2", File { mode: 0o600, ..Default::default() }),
            TestFile::new_file("nolinks", File::default()),
            TestFile::new_link("nolinks", "file").with_error(), // would orphan
            TestFile::new_file("onelink", File::default()),
            TestFile::new_link("onelink2", "onelink"),
            TestFile::new_link("onelink", "file"),
            TestFile::new_file("", File::default()).with_error(),
            TestFile::new_link("", "file").with_error(),
            TestFile::new_file("", File { mode: format::S_IFDIR | 0o777, ..Default::default() }),
            TestFile::new_file("smallxattr", File {
                xattrs: BTreeMap::from([("user.foo".to_string(), data[..4].to_vec())]),
                ..Default::default()
            }),
            TestFile::new_file("smallxattr", File {
                xattrs: BTreeMap::from([("user.foo".to_string(), data[..8].to_vec())]),
                ..Default::default()
            }),
            TestFile::new_file("smallxattr_delete", File {
                xattrs: BTreeMap::from([("user.foo".to_string(), data[..4].to_vec())]),
                ..Default::default()
            }),
            TestFile::new_file("smallxattr_delete", File::default()),
            TestFile::new_file("largexattr", File {
                xattrs: BTreeMap::from([
                    ("user.small".to_string(), data[..8].to_vec()),
                    ("user.foo".to_string(), data[..200].to_vec()),
                ]),
                ..Default::default()
            }),
            TestFile::new_file("largexattr", File {
                xattrs: BTreeMap::from([
                    ("user.small".to_string(), data[..12].to_vec()),
                    ("user.foo".to_string(), data[..400].to_vec()),
                ]),
                ..Default::default()
            }),
            TestFile::new_file("largexattr", File {
                xattrs: BTreeMap::from([("user.foo".to_string(), data[..200].to_vec())]),
                ..Default::default()
            }),
            TestFile::new_file("largexattr_delete", File::default()),
        ];
        run_tests_on_files(&mut test_files, &[]);
    }

    #[test]
    fn test_large_file() {
        let mib: i64 = 1024 * 1024;
        let mut test_files = vec![
            TestFile::new_file_large("small", File::default(), mib),
            TestFile::new_file_large("medium", File::default(), 200 * mib),
            TestFile::new_file_large("large", File::default(), 600 * mib),
        ];
        run_tests_on_files(&mut test_files, &[]);
    }

    #[test]
    fn test_file_link_limit() {
        let mut test_files = vec![
            TestFile::new_file("file", File::default()),
        ];
        for i in 0..MAX_LINKS as usize {
            test_files.push(TestFile::new_link(&format!("link{i}"), "file"));
        }
        // The last link should fail (would exceed max links)
        let last = test_files.last_mut().unwrap();
        last.expect_error = true;
        run_tests_on_files(&mut test_files, &[]);
    }

    #[test]
    fn test_dir_link_limit() {
        let mut test_files = vec![
            TestFile::new_file("dir", File { mode: format::S_IFDIR, ..Default::default() }),
        ];
        for i in 0..(MAX_LINKS as usize - 1) {
            test_files.push(TestFile::new_file(
                &format!("dir/{i}"),
                File { mode: format::S_IFDIR, ..Default::default() },
            ));
        }
        // The last directory should fail (parent exceeds max links)
        let last = test_files.last_mut().unwrap();
        last.expect_error = true;
        run_tests_on_files(&mut test_files, &[]);
    }

    #[test]
    fn test_large_disk() {
        let max_disk_size: i64 = 16 * 1024 * 1024 * 1024 * 1024; // 16 TiB
        let mut test_files = vec![
            TestFile::new_file("file", File::default()),
        ];
        run_tests_on_files(&mut test_files, &[WriterOption::MaximumDiskSize(max_disk_size)]);
    }

    #[test]
    fn test_determinism() {
        // Same input must produce identical output — critical for content-addressing
        let data = test_data();
        let make_files = || vec![
            TestFile::new_file("dir", File { mode: format::S_IFDIR | 0o755, ..Default::default() }),
            TestFile::new_file_with_data("dir/file1", File { mode: 0o644, ..Default::default() }, &data[..100]),
            TestFile::new_file("dir/file2", File {
                mode: format::S_IFLNK,
                linkname: "file1".to_string(),
                ..Default::default()
            }),
            TestFile::new_file_with_data("dir/file3", File {
                mode: format::S_IFREG | 0o600,
                uid: 1000,
                gid: 1000,
                xattrs: BTreeMap::from([("user.test".to_string(), b"value".to_vec())]),
                ..Default::default()
            }, &data[..200]),
        ];

        let output1 = {
            let buf = Cursor::new(Vec::new());
            let mut w = Writer::new(buf, &[]);
            for tf in &mut make_files() {
                create_test_file(&mut w, tf).unwrap();
            }
            w.close().unwrap().into_inner()
        };

        let output2 = {
            let buf = Cursor::new(Vec::new());
            let mut w = Writer::new(buf, &[]);
            for tf in &mut make_files() {
                create_test_file(&mut w, tf).unwrap();
            }
            w.close().unwrap().into_inner()
        };

        assert_eq!(output1.len(), output2.len(), "output lengths differ");
        assert_eq!(output1, output2, "outputs are not identical — determinism violated");
    }

    #[test]
    fn test_tar_determinism() {
        use crate::ext4::tar_convert::{convert_tar_to_ext4, ConvertOptions};
        use sha2::{Sha256, Digest};

        // Build a tar in memory
        fn make_tar() -> Vec<u8> {
            let mut builder = tar::Builder::new(Vec::new());
            // Add a directory
            let mut header = tar::Header::new_gnu();
            header.set_path("foo/").unwrap();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            header.set_cksum();
            builder.append(&header, io::empty()).unwrap();
            // Add a file
            let data = b"hello world";
            let mut header = tar::Header::new_gnu();
            header.set_path("foo/bar.txt").unwrap();
            header.set_mode(0o644);
            header.set_size(data.len() as u64);
            header.set_cksum();
            builder.append(&header, &data[..]).unwrap();
            builder.into_inner().unwrap()
        }

        let tar_data = make_tar();
        let opts = ConvertOptions::default();

        let out1 = convert_tar_to_ext4(
            Cursor::new(tar_data.clone()),
            Cursor::new(Vec::new()),
            &opts,
        ).unwrap().into_inner();

        let out2 = convert_tar_to_ext4(
            Cursor::new(tar_data),
            Cursor::new(Vec::new()),
            &opts,
        ).unwrap().into_inner();

        let hash1 = Sha256::digest(&out1);
        let hash2 = Sha256::digest(&out2);
        assert_eq!(hash1, hash2, "tar-to-ext4 conversion is not deterministic");
    }
}
