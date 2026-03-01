use std::collections::BTreeMap;
use std::io::{self, Cursor, Read, Write};

use crate::format::{self, MAX_LINKS, TYPE_MASK};
use crate::writer::{File, Writer, WriterOption, BLOCK_SIZE, INLINE_DATA_SIZE};

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
    use crate::tar_convert::{convert_tar_to_ext4, ConvertOptions};
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

// ---- Reader roundtrip tests ----

use crate::reader::Reader;

/// Helper: write an ext4 image, then open it with Reader.
fn write_and_read(
    test_files: &mut [TestFile],
    opts: &[WriterOption],
) -> (Vec<u8>, Reader<Cursor<Vec<u8>>>) {
    let buf = Cursor::new(Vec::new());
    let mut w = Writer::new(buf, opts);

    for tf in test_files.iter_mut() {
        create_test_file(&mut w, tf).unwrap();
    }

    let output = w.close().unwrap().into_inner();
    let reader = Reader::new(Cursor::new(output.clone())).unwrap();
    (output, reader)
}

#[test]
fn test_reader_basic_roundtrip() {
    let data = test_data();

    let mut test_files = vec![
        TestFile::new_file_with_data("hello.txt", File {
            mode: format::S_IFREG | 0o644,
            uid: 1000,
            gid: 1000,
            atime: 1700000000,
            mtime: 1700003600,
            ..Default::default()
        }, b"hello world"),
        TestFile::new_file("empty", File { mode: 0o644, ..Default::default() }),
        TestFile::new_file("subdir", File { mode: format::S_IFDIR | 0o755, ..Default::default() }),
        TestFile::new_file_with_data("subdir/nested.txt", File {
            mode: format::S_IFREG | 0o600,
            uid: 1000,
            gid: 1000,
            ..Default::default()
        }, b"nested content"),
        TestFile::new_file("link_to_hello", File {
            mode: format::S_IFLNK,
            linkname: "hello.txt".to_string(),
            ..Default::default()
        }),
        TestFile::new_file("subdir/null_dev", File {
            mode: format::S_IFCHR,
            devmajor: 1,
            devminor: 3,
            ..Default::default()
        }),
        TestFile::new_file("subdir/fifo", File {
            mode: format::S_IFIFO | 0o644,
            ..Default::default()
        }),
        TestFile::new_file_with_data("bigger.txt", File {
            mode: format::S_IFREG | 0o644,
            ..Default::default()
        }, &data[..200]),
    ];

    let (_output, mut reader) = write_and_read(&mut test_files, &[]);
    let entries = reader.walk().unwrap();

    // Check entry count (lost+found + our entries)
    // lost+found is a directory created by the writer. Walk skips it by default since
    // inode 11 >= INODE_FIRST, so it will be included.
    let names: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    assert!(names.contains(&"hello.txt"), "missing hello.txt in walk: {names:?}");
    assert!(names.contains(&"empty"), "missing empty in walk: {names:?}");
    assert!(names.contains(&"subdir"), "missing subdir in walk: {names:?}");
    assert!(names.contains(&"subdir/nested.txt"), "missing subdir/nested.txt: {names:?}");
    assert!(names.contains(&"link_to_hello"), "missing link_to_hello: {names:?}");
    assert!(names.contains(&"subdir/null_dev"), "missing subdir/null_dev: {names:?}");
    assert!(names.contains(&"subdir/fifo"), "missing subdir/fifo: {names:?}");

    // Verify file content
    let hello = entries.iter().find(|e| e.path == "hello.txt").unwrap();
    let inode = reader.read_inode(hello.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(content, b"hello world", "hello.txt content mismatch");

    // Verify nested file content
    let nested = entries.iter().find(|e| e.path == "subdir/nested.txt").unwrap();
    let inode = reader.read_inode(nested.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(content, b"nested content", "nested.txt content mismatch");

    // Verify symlink target
    let link = entries.iter().find(|e| e.path == "link_to_hello").unwrap();
    assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"), "symlink target mismatch");

    // Verify device node
    let dev = entries.iter().find(|e| e.path == "subdir/null_dev").unwrap();
    assert_eq!(dev.devmajor, 1, "devmajor mismatch");
    assert_eq!(dev.devminor, 3, "devminor mismatch");

    // Verify metadata
    assert_eq!(hello.uid, 1000);
    assert_eq!(hello.gid, 1000);
    assert_eq!(hello.mode & !TYPE_MASK, 0o644);
    assert_eq!(hello.atime, 1700000000);
    assert_eq!(hello.mtime, 1700003600);

    // Verify bigger file content
    let bigger = entries.iter().find(|e| e.path == "bigger.txt").unwrap();
    let inode = reader.read_inode(bigger.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(content, &data[..200], "bigger.txt content mismatch");
}

#[test]
fn test_reader_xattr_roundtrip() {
    let data = test_data();

    let mut test_files = vec![
        TestFile::new_file("with_xattrs", File {
            mode: format::S_IFREG | 0o644,
            xattrs: BTreeMap::from([
                ("user.test".to_string(), b"value123".to_vec()),
                ("user.other".to_string(), b"data".to_vec()),
            ]),
            ..Default::default()
        }),
        TestFile::new_file("with_large_xattrs", File {
            mode: format::S_IFREG | 0o644,
            xattrs: BTreeMap::from([
                ("user.big".to_string(), data[..100].to_vec()),
            ]),
            ..Default::default()
        }),
    ];

    let (_output, mut reader) = write_and_read(&mut test_files, &[]);
    let entries = reader.walk().unwrap();

    let entry = entries.iter().find(|e| e.path == "with_xattrs").unwrap();
    assert_eq!(entry.xattrs.get("user.test").map(|v| v.as_slice()), Some(b"value123".as_slice()));
    assert_eq!(entry.xattrs.get("user.other").map(|v| v.as_slice()), Some(b"data".as_slice()));

    let entry = entries.iter().find(|e| e.path == "with_large_xattrs").unwrap();
    assert_eq!(entry.xattrs.get("user.big").map(|v| v.as_slice()), Some(&data[..100] as &[u8]));
}

#[test]
fn test_reader_inline_data_roundtrip() {
    let data = test_data();

    let mut test_files = vec![
        TestFile::new_file_with_data("tiny.txt", File {
            mode: format::S_IFREG | 0o644,
            ..Default::default()
        }, b"tiny"),
        TestFile::new_file_with_data("bigger.txt", File {
            mode: format::S_IFREG | 0o644,
            ..Default::default()
        }, &data[..60]),
    ];

    let (_output, mut reader) = write_and_read(&mut test_files, &[WriterOption::InlineData]);
    let entries = reader.walk().unwrap();

    let tiny = entries.iter().find(|e| e.path == "tiny.txt").unwrap();
    let inode = reader.read_inode(tiny.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(content, b"tiny", "tiny.txt content mismatch");

    let bigger = entries.iter().find(|e| e.path == "bigger.txt").unwrap();
    let inode = reader.read_inode(bigger.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(&content, &data[..60], "bigger.txt content mismatch");
}

#[test]
fn test_reader_large_directory_roundtrip() {
    let mut test_files = vec![
        TestFile::new_file("bigdir", File { mode: format::S_IFDIR | 0o755, ..Default::default() }),
    ];
    for i in 0..1000 {
        test_files.push(TestFile::new_file(
            &format!("bigdir/file_{i:04}"),
            File { mode: 0o644, ..Default::default() },
        ));
    }

    let (_output, mut reader) = write_and_read(&mut test_files, &[]);
    let entries = reader.walk().unwrap();

    // Filter to bigdir/* entries
    let dir_entries: Vec<_> = entries.iter()
        .filter(|e| e.path.starts_with("bigdir/"))
        .collect();
    assert_eq!(dir_entries.len(), 1000, "expected 1000 entries in bigdir, got {}", dir_entries.len());

    // Verify specific entries exist
    assert!(dir_entries.iter().any(|e| e.path == "bigdir/file_0000"));
    assert!(dir_entries.iter().any(|e| e.path == "bigdir/file_0999"));
}

#[test]
fn test_reader_large_file_roundtrip() {
    // Multi-extent file: 128KB+ should require extent tree
    let data: Vec<u8> = (0..200_000u32).flat_map(|i| i.to_le_bytes()).collect();
    let file_data = &data[..200_000]; // ~200KB

    let mut test_files = vec![
        TestFile::new_file_with_data("large.bin", File {
            mode: format::S_IFREG | 0o644,
            ..Default::default()
        }, file_data),
    ];

    let (_output, mut reader) = write_and_read(&mut test_files, &[]);
    let entries = reader.walk().unwrap();

    let entry = entries.iter().find(|e| e.path == "large.bin").unwrap();
    assert_eq!(entry.size, file_data.len() as u64);

    let inode = reader.read_inode(entry.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(content.len(), file_data.len());
    assert_eq!(content, file_data, "large file content mismatch");
}

#[test]
fn test_reader_hard_link_roundtrip() {
    let mut test_files = vec![
        TestFile::new_file_with_data("original.txt", File {
            mode: format::S_IFREG | 0o644,
            ..Default::default()
        }, b"shared content"),
        TestFile::new_link("hardlink.txt", "original.txt"),
    ];

    let (_output, mut reader) = write_and_read(&mut test_files, &[]);
    let entries = reader.walk().unwrap();

    let orig = entries.iter().find(|e| e.path == "original.txt").unwrap();
    let link = entries.iter().find(|e| e.path == "hardlink.txt").unwrap();

    // Same inode number
    assert_eq!(orig.inode_number, link.inode_number, "hard link should share inode");
    // Link count should be 2
    assert_eq!(orig.links_count, 2, "expected link count 2");

    // Both should read the same content
    let inode = reader.read_inode(orig.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(content, b"shared content");
}

#[test]
fn test_reader_device_nodes_roundtrip() {
    let mut test_files = vec![
        TestFile::new_file("chr_dev", File {
            mode: format::S_IFCHR,
            devmajor: 0x56,
            devminor: 0x1234,
            ..Default::default()
        }),
        TestFile::new_file("blk_dev", File {
            mode: format::S_IFBLK,
            devmajor: 0x78,
            devminor: 0x12,
            ..Default::default()
        }),
    ];

    let (_output, mut reader) = write_and_read(&mut test_files, &[]);
    let entries = reader.walk().unwrap();

    let chr = entries.iter().find(|e| e.path == "chr_dev").unwrap();
    assert_eq!(chr.devmajor, 0x56, "chr devmajor mismatch");
    assert_eq!(chr.devminor, 0x1234, "chr devminor mismatch");

    let blk = entries.iter().find(|e| e.path == "blk_dev").unwrap();
    assert_eq!(blk.devmajor, 0x78, "blk devmajor mismatch");
    assert_eq!(blk.devminor, 0x12, "blk devminor mismatch");
}

#[test]
fn test_reader_tar_roundtrip() {
    use crate::tar_convert::{convert_tar_to_ext4, ConvertOptions};

    // Build a tar
    let mut builder = tar::Builder::new(Vec::new());

    let mut header = tar::Header::new_gnu();
    header.set_path("app/").unwrap();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_mode(0o755);
    header.set_size(0);
    header.set_cksum();
    builder.append(&header, io::empty()).unwrap();

    let file_data = b"#!/bin/sh\necho hello\n";
    let mut header = tar::Header::new_gnu();
    header.set_path("app/run.sh").unwrap();
    header.set_mode(0o755);
    header.set_size(file_data.len() as u64);
    header.set_uid(0);
    header.set_gid(0);
    header.set_cksum();
    builder.append(&header, &file_data[..]).unwrap();

    let mut header = tar::Header::new_gnu();
    header.set_path("app/link").unwrap();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_link_name("run.sh").unwrap();
    header.set_mode(0o777);
    header.set_size(0);
    header.set_cksum();
    builder.append(&header, io::empty()).unwrap();

    let config_data = b"config=value\n";
    let mut header = tar::Header::new_gnu();
    header.set_path("app/config.txt").unwrap();
    header.set_mode(0o644);
    header.set_size(config_data.len() as u64);
    header.set_cksum();
    builder.append(&header, &config_data[..]).unwrap();

    let tar_data = builder.into_inner().unwrap();

    // Convert tar -> ext4
    let opts = ConvertOptions::default();
    let ext4_data = convert_tar_to_ext4(
        Cursor::new(tar_data),
        Cursor::new(Vec::new()),
        &opts,
    ).unwrap().into_inner();

    // Read ext4 with Reader
    let mut reader = Reader::new(Cursor::new(ext4_data)).unwrap();
    let entries = reader.walk().unwrap();

    let names: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    assert!(names.contains(&"app"), "missing app directory: {names:?}");
    assert!(names.contains(&"app/run.sh"), "missing app/run.sh: {names:?}");
    assert!(names.contains(&"app/link"), "missing app/link: {names:?}");
    assert!(names.contains(&"app/config.txt"), "missing app/config.txt: {names:?}");

    // Verify file content
    let run_sh = entries.iter().find(|e| e.path == "app/run.sh").unwrap();
    let inode = reader.read_inode(run_sh.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(content, file_data.as_slice(), "run.sh content mismatch");

    let config = entries.iter().find(|e| e.path == "app/config.txt").unwrap();
    let inode = reader.read_inode(config.inode_number).unwrap();
    let content = reader.read_data(&inode).unwrap();
    assert_eq!(content, config_data.as_slice(), "config.txt content mismatch");

    // Verify symlink
    let link = entries.iter().find(|e| e.path == "app/link").unwrap();
    assert_eq!(link.symlink_target.as_deref(), Some("run.sh"));

    // Now test to_tar export
    let mut reader = Reader::new(Cursor::new(reader.into_inner().into_inner())).unwrap();
    let tar_output = reader.to_tar(Vec::new()).unwrap();
    assert!(!tar_output.is_empty(), "tar output should not be empty");

    // Parse the exported tar and verify entries
    let mut archive = tar::Archive::new(Cursor::new(tar_output));
    let mut found_run_sh = false;
    let mut found_config = false;
    for entry_result in archive.entries().unwrap() {
        let mut entry: tar::Entry<_> = entry_result.unwrap();
        let path = entry.path().unwrap().to_string_lossy().to_string();
        if path == "app/run.sh" {
            let mut content = Vec::new();
            entry.read_to_end(&mut content).unwrap();
            assert_eq!(content, file_data.as_slice(), "tar run.sh content mismatch");
            found_run_sh = true;
        } else if path == "app/config.txt" {
            let mut content = Vec::new();
            entry.read_to_end(&mut content).unwrap();
            assert_eq!(content, config_data.as_slice(), "tar config.txt content mismatch");
            found_config = true;
        }
    }
    assert!(found_run_sh, "run.sh not found in exported tar");
    assert!(found_config, "config.txt not found in exported tar");
}

#[test]
fn test_reader_to_tar_with_xattrs() {
    use crate::tar_convert::{convert_tar_to_ext4, ConvertOptions};

    // Build a tar with PAX xattrs
    let mut builder = tar::Builder::new(Vec::new());

    // Add PAX extensions for xattrs
    builder.append_pax_extensions([
        ("SCHILY.xattr.user.test", b"value123" as &[u8]),
    ]).unwrap();

    let file_data = b"xattr content";
    let mut header = tar::Header::new_gnu();
    header.set_path("xattr_file").unwrap();
    header.set_mode(0o644);
    header.set_size(file_data.len() as u64);
    header.set_cksum();
    builder.append(&header, &file_data[..]).unwrap();

    let tar_data = builder.into_inner().unwrap();

    // Convert tar -> ext4
    let opts = ConvertOptions::default();
    let ext4_data = convert_tar_to_ext4(
        Cursor::new(tar_data),
        Cursor::new(Vec::new()),
        &opts,
    ).unwrap().into_inner();

    // Read and verify xattrs
    let mut reader = Reader::new(Cursor::new(ext4_data)).unwrap();
    let entries = reader.walk().unwrap();

    let entry = entries.iter().find(|e| e.path == "xattr_file").unwrap();
    assert_eq!(
        entry.xattrs.get("user.test").map(|v| v.as_slice()),
        Some(b"value123".as_slice()),
        "xattr not preserved through roundtrip"
    );
}

// ---- UUID + Journal tests ----

#[test]
fn test_uuid_roundtrip() {
    let uuid: [u8; 16] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
        0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
    ];

    let buf = Cursor::new(Vec::new());
    let mut w = Writer::new(buf, &[WriterOption::Uuid(uuid)]);
    w.create("hello.txt", &File {
        size: 5,
        mode: format::S_IFREG | 0o644,
        ..Default::default()
    }).unwrap();
    w.write_all(b"hello").unwrap();
    let output = w.close().unwrap().into_inner();

    let reader = Reader::new(Cursor::new(output)).unwrap();
    let sb = reader.superblock();
    assert_eq!(sb.uuid, uuid, "UUID not preserved");
    assert_eq!(
        sb.hash_seed,
        [
            u32::from_le_bytes(uuid[0..4].try_into().unwrap()),
            u32::from_le_bytes(uuid[4..8].try_into().unwrap()),
            u32::from_le_bytes(uuid[8..12].try_into().unwrap()),
            u32::from_le_bytes(uuid[12..16].try_into().unwrap()),
        ],
        "hash_seed not derived from UUID"
    );
}

#[test]
fn test_journal_roundtrip() {
    let uuid: [u8; 16] = [0xAA; 16];
    let journal_size: u32 = 1024; // 4 MiB

    let buf = Cursor::new(Vec::new());
    let mut w = Writer::new(buf, &[
        WriterOption::Uuid(uuid),
        WriterOption::Journal(journal_size),
    ]);
    w.create("file.txt", &File {
        size: 4,
        mode: format::S_IFREG | 0o644,
        ..Default::default()
    }).unwrap();
    w.write_all(b"data").unwrap();
    let output = w.close().unwrap().into_inner();

    // Verify superblock journal fields
    let mut reader = Reader::new(Cursor::new(output.clone())).unwrap();
    {
        let sb = reader.superblock();
        assert!(
            sb.feature_compat.contains(format::CompatFeature::HAS_JOURNAL),
            "HAS_JOURNAL flag not set"
        );
        assert_eq!(sb.journal_inum, format::INODE_JOURNAL, "journal_inum should be 8");
        assert_eq!(sb.journal_uuid, uuid, "journal_uuid should match filesystem uuid");
        assert_ne!(sb.journal_blocks[0], 0, "journal_blocks backup should be populated");
    }

    // Verify journal inode (inode 8) exists and has correct size
    let journal_inode = reader.read_inode(format::INODE_JOURNAL).unwrap();
    assert_eq!(
        journal_inode.size,
        journal_size as u64 * BLOCK_SIZE,
        "journal inode size mismatch"
    );
    assert_eq!(journal_inode.mode & TYPE_MASK, format::S_IFREG);
    assert!(journal_inode.flags.contains(format::InodeFlags::EXTENTS));

    // Verify JBD2 superblock magic at start of journal data
    let journal_data = reader.read_data(&journal_inode).unwrap();
    let jbd2_magic = u32::from_be_bytes(journal_data[0..4].try_into().unwrap());
    assert_eq!(jbd2_magic, format::JBD2_MAGIC, "JBD2 magic mismatch");

    let jbd2_blocktype = u32::from_be_bytes(journal_data[4..8].try_into().unwrap());
    assert_eq!(jbd2_blocktype, format::JBD2_SUPERBLOCK_V2, "JBD2 blocktype mismatch");

    let jbd2_maxlen = u32::from_be_bytes(journal_data[16..20].try_into().unwrap());
    assert_eq!(jbd2_maxlen, journal_size, "JBD2 maxlen mismatch");

    // Verify user files still work alongside journal
    let entries = reader.walk().unwrap();
    let file = entries.iter().find(|e| e.path == "file.txt").unwrap();
    assert_eq!(file.size, 4);
}

#[test]
fn test_journal_with_large_content() {
    // Ensure journal works alongside substantial file data
    let uuid: [u8; 16] = [0xBB; 16];
    let journal_size: u32 = 256; // 1 MiB journal (small, to keep test fast)

    let buf = Cursor::new(Vec::new());
    let mut w = Writer::new(buf, &[
        WriterOption::Uuid(uuid),
        WriterOption::Journal(journal_size),
    ]);

    // Create a file larger than one extent (>128 KiB)
    let big_data: Vec<u8> = (0..256 * 1024u32).map(|i| (i % 251) as u8).collect();
    w.create("big.bin", &File {
        size: big_data.len() as i64,
        mode: format::S_IFREG | 0o644,
        ..Default::default()
    }).unwrap();
    w.write_all(&big_data).unwrap();

    let output = w.close().unwrap().into_inner();

    // Verify both journal and file data are intact
    let mut reader = Reader::new(Cursor::new(output)).unwrap();
    let sb = reader.superblock();
    assert!(sb.feature_compat.contains(format::CompatFeature::HAS_JOURNAL));

    let entries = reader.walk().unwrap();
    let file = entries.iter().find(|e| e.path == "big.bin").unwrap();
    assert_eq!(file.size, big_data.len() as u64);

    let inode = reader.read_inode(file.inode_number).unwrap();
    let read_data = reader.read_data(&inode).unwrap();
    assert_eq!(read_data, big_data, "file data corrupted with journal enabled");
}
