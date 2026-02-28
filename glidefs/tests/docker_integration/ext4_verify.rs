//! Docker-based ext4 filesystem verification tests.
//!
//! Generates ext4 images using the writer and validates them with `e2fsck`
//! and `debugfs` running inside an Alpine container with e2fsprogs.
//!
//! Run: `cargo test --features docker-tests --test docker_integration ext4`

use std::collections::BTreeMap;
use std::io::{self, Cursor, Write as _};

use tempfile::TempDir;
use testcontainers::core::{ExecCommand, Mount};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use glidefs::ext4::format;
use glidefs::ext4::tar_convert::{convert_tar_to_ext4, ConvertOptions};
use glidefs::ext4::writer::{File, Writer, WriterOption};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Start an Alpine container with e2fsprogs, bind-mounting `image_dir` at `/images`.
async fn start_verify_container(image_dir: &std::path::Path) -> ContainerAsync<GenericImage> {
    let container = GenericImage::new("alpine", "3.21")
        .with_mount(Mount::bind_mount(
            image_dir.to_str().expect("temp dir path must be valid UTF-8"),
            "/images",
        ))
        .with_cmd(["sleep", "infinity"])
        .start()
        .await
        .expect("failed to start Alpine container");

    // Install e2fsprogs (provides e2fsck and debugfs).
    // Retry: Alpine mirror can occasionally fail on first attempt.
    for attempt in 0..3 {
        let mut result = container
            .exec(ExecCommand::new([
                "apk", "add", "--no-cache", "e2fsprogs", "e2fsprogs-extra",
            ]))
            .await
            .expect("failed to exec apk add");
        let _ = result.stdout_to_vec().await;
        if result.exit_code().await.unwrap_or(Some(1)) == Some(0) {
            break;
        }
        if attempt == 2 {
            panic!("failed to install e2fsprogs after 3 attempts");
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    container
}

/// Execute a command in the container. Returns (stdout, exit_code).
async fn exec_cmd(
    container: &ContainerAsync<GenericImage>,
    cmd: &[&str],
) -> (String, Option<i64>) {
    let mut result = container
        .exec(ExecCommand::new(
            cmd.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        ))
        .await
        .expect("exec failed");
    let stdout = result.stdout_to_vec().await.unwrap_or_default();
    let exit_code = result.exit_code().await.unwrap_or(None);
    (String::from_utf8_lossy(&stdout).to_string(), exit_code)
}

/// Run e2fsck on an image inside the container. Panics on failure.
async fn run_e2fsck(container: &ContainerAsync<GenericImage>, image_name: &str) {
    let path = format!("/images/{image_name}");
    let (output, exit_code) = exec_cmd(container, &["e2fsck", "-v", "-f", "-n", &path]).await;
    assert_eq!(
        exit_code,
        Some(0),
        "e2fsck failed for {image_name} (exit code {exit_code:?}):\n{output}"
    );
}

/// Run a debugfs command and return stdout.
async fn debugfs_cmd(
    container: &ContainerAsync<GenericImage>,
    image_name: &str,
    command: &str,
) -> String {
    let path = format!("/images/{image_name}");
    let (output, exit_code) = exec_cmd(container, &["debugfs", "-R", command, &path]).await;
    assert_eq!(
        exit_code,
        Some(0),
        "debugfs command '{command}' failed for {image_name} (exit code {exit_code:?}):\n{output}"
    );
    output
}

/// Write bytes to a file in the temp directory.
fn write_image(dir: &std::path::Path, name: &str, data: &[u8]) {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path)
        .unwrap_or_else(|e| panic!("failed to create {}: {e}", path.display()));
    f.write_all(data)
        .unwrap_or_else(|e| panic!("failed to write {}: {e}", path.display()));
}

// ---------------------------------------------------------------------------
// Image builders
// ---------------------------------------------------------------------------

fn build_basic_image() -> Vec<u8> {
    let buf = Cursor::new(Vec::new());
    let mut w = Writer::new(buf, &[]);

    // Regular file with content
    let data = b"hello world";
    let mut f = File {
        mode: format::S_IFREG | 0o644,
        size: data.len() as i64,
        uid: 1000,
        gid: 1000,
        ..Default::default()
    };
    w.create("hello.txt", &mut f).unwrap();
    w.write_data(data).unwrap();

    // Empty file
    let mut f = File {
        mode: 0o644,
        ..Default::default()
    };
    w.create("empty", &mut f).unwrap();

    // Directory
    let mut f = File {
        mode: format::S_IFDIR | 0o755,
        ..Default::default()
    };
    w.create("subdir", &mut f).unwrap();

    // File in subdirectory
    let data2 = b"nested content here";
    let mut f = File {
        mode: format::S_IFREG | 0o600,
        size: data2.len() as i64,
        uid: 1000,
        gid: 1000,
        ..Default::default()
    };
    w.create("subdir/nested.txt", &mut f).unwrap();
    w.write_data(data2).unwrap();

    // Symlink
    let mut f = File {
        mode: format::S_IFLNK,
        linkname: "hello.txt".to_string(),
        ..Default::default()
    };
    w.create("link_to_hello", &mut f).unwrap();

    // Hard link
    w.link("hello.txt", "hard_link").unwrap();

    // Character device
    let mut f = File {
        mode: format::S_IFCHR,
        devmajor: 1,
        devminor: 3,
        ..Default::default()
    };
    w.create("subdir/null_dev", &mut f).unwrap();

    // FIFO
    let mut f = File {
        mode: format::S_IFIFO | 0o644,
        ..Default::default()
    };
    w.create("subdir/fifo", &mut f).unwrap();

    // Xattr file
    let xattr_data = b"xattr content";
    let mut xattrs = BTreeMap::new();
    xattrs.insert("user.test".to_string(), b"value123".to_vec());
    xattrs.insert("user.other".to_string(), b"data".to_vec());
    let mut f = File {
        mode: format::S_IFREG | 0o644,
        size: xattr_data.len() as i64,
        xattrs,
        ..Default::default()
    };
    w.create("xattr_file", &mut f).unwrap();
    w.write_data(xattr_data).unwrap();

    w.close().unwrap().into_inner()
}

fn build_large_dir_image() -> Vec<u8> {
    let buf = Cursor::new(Vec::new());
    let mut w = Writer::new(buf, &[]);

    let mut f = File {
        mode: format::S_IFDIR | 0o755,
        ..Default::default()
    };
    w.create("bigdir", &mut f).unwrap();

    for i in 0..10_000 {
        let mut f = File {
            mode: 0o644,
            ..Default::default()
        };
        w.create(&format!("bigdir/file_{i:05}"), &mut f).unwrap();
    }

    w.close().unwrap().into_inner()
}

fn build_tar_image() -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());

    // Directory
    let mut header = tar::Header::new_gnu();
    header.set_path("app/").unwrap();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_mode(0o755);
    header.set_size(0);
    header.set_cksum();
    builder.append(&header, io::empty()).unwrap();

    // Regular file
    let data = b"#!/bin/sh\necho hello\n";
    let mut header = tar::Header::new_gnu();
    header.set_path("app/run.sh").unwrap();
    header.set_mode(0o755);
    header.set_size(data.len() as u64);
    header.set_uid(0);
    header.set_gid(0);
    header.set_cksum();
    builder.append(&header, &data[..]).unwrap();

    // Symlink
    let mut header = tar::Header::new_gnu();
    header.set_path("app/link").unwrap();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_link_name("run.sh").unwrap();
    header.set_mode(0o777);
    header.set_size(0);
    header.set_cksum();
    builder.append(&header, io::empty()).unwrap();

    // Second file for content verification
    let data2 = b"config=value\n";
    let mut header = tar::Header::new_gnu();
    header.set_path("app/config.txt").unwrap();
    header.set_mode(0o644);
    header.set_size(data2.len() as u64);
    header.set_cksum();
    builder.append(&header, &data2[..]).unwrap();

    let tar_data = builder.into_inner().unwrap();

    let opts = ConvertOptions::default();
    convert_tar_to_ext4(Cursor::new(tar_data), Cursor::new(Vec::new()), &opts)
        .unwrap()
        .into_inner()
}

fn build_inline_data_image() -> Vec<u8> {
    let buf = Cursor::new(Vec::new());
    let mut w = Writer::new(buf, &[WriterOption::InlineData]);

    // Small file that fits in inode inline data
    let data = b"tiny";
    let mut f = File {
        mode: format::S_IFREG | 0o644,
        size: data.len() as i64,
        ..Default::default()
    };
    w.create("tiny.txt", &mut f).unwrap();
    w.write_data(data).unwrap();

    // File just above inline threshold — should use a block
    let block_data = vec![0xABu8; 60];
    let mut f = File {
        mode: format::S_IFREG | 0o644,
        size: block_data.len() as i64,
        ..Default::default()
    };
    w.create("bigger.txt", &mut f).unwrap();
    w.write_data(&block_data).unwrap();

    w.close().unwrap().into_inner()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Validates basic ext4 structure, file content, symlinks, xattrs, and devices.
#[tokio::test]
async fn test_ext4_fsck_with_verification() {
    let image_dir = TempDir::new().expect("failed to create temp dir");

    let basic = build_basic_image();
    write_image(image_dir.path(), "basic.ext4", &basic);

    let container = start_verify_container(image_dir.path()).await;

    // --- e2fsck: structural validation ---
    run_e2fsck(&container, "basic.ext4").await;

    // --- debugfs: content verification ---

    // File content
    let content = debugfs_cmd(&container, "basic.ext4", "cat hello.txt").await;
    assert!(
        content.contains("hello world"),
        "hello.txt content mismatch: {content}"
    );

    let content = debugfs_cmd(&container, "basic.ext4", "cat subdir/nested.txt").await;
    assert!(
        content.contains("nested content here"),
        "nested.txt content mismatch: {content}"
    );

    // Symlink target
    let stat = debugfs_cmd(&container, "basic.ext4", "stat link_to_hello").await;
    assert!(
        stat.contains("hello.txt"),
        "symlink should point to hello.txt:\n{stat}"
    );

    // Directory listing
    let ls = debugfs_cmd(&container, "basic.ext4", "ls subdir").await;
    assert!(ls.contains("nested.txt"), "subdir missing nested.txt: {ls}");
    assert!(ls.contains("null_dev"), "subdir missing null_dev: {ls}");
    assert!(ls.contains("fifo"), "subdir missing fifo: {ls}");

    // Xattrs
    let xattrs = debugfs_cmd(&container, "basic.ext4", "ea_list xattr_file").await;
    assert!(
        xattrs.contains("user.test"),
        "missing xattr user.test: {xattrs}"
    );
    assert!(
        xattrs.contains("user.other"),
        "missing xattr user.other: {xattrs}"
    );

    // Xattr content
    let content = debugfs_cmd(&container, "basic.ext4", "cat xattr_file").await;
    assert!(
        content.contains("xattr content"),
        "xattr_file content mismatch: {content}"
    );
}

/// Validates ext4 with 10K directory entries.
#[tokio::test]
async fn test_ext4_fsck_large_directory() {
    let image_dir = TempDir::new().expect("failed to create temp dir");

    let image = build_large_dir_image();
    write_image(image_dir.path(), "large.ext4", &image);

    let container = start_verify_container(image_dir.path()).await;
    run_e2fsck(&container, "large.ext4").await;
}

/// Validates ext4 generated from tar conversion.
#[tokio::test]
async fn test_ext4_fsck_tar_roundtrip() {
    let image_dir = TempDir::new().expect("failed to create temp dir");

    let image = build_tar_image();
    write_image(image_dir.path(), "tar.ext4", &image);

    let container = start_verify_container(image_dir.path()).await;
    run_e2fsck(&container, "tar.ext4").await;

    // Verify content
    let ls = debugfs_cmd(&container, "tar.ext4", "ls app").await;
    assert!(ls.contains("run.sh"), "app/ missing run.sh: {ls}");
    assert!(ls.contains("link"), "app/ missing link: {ls}");
    assert!(ls.contains("config.txt"), "app/ missing config.txt: {ls}");

    let content = debugfs_cmd(&container, "tar.ext4", "cat app/run.sh").await;
    assert!(
        content.contains("echo hello"),
        "run.sh content mismatch: {content}"
    );

    let content = debugfs_cmd(&container, "tar.ext4", "cat app/config.txt").await;
    assert!(
        content.contains("config=value"),
        "config.txt content mismatch: {content}"
    );
}

/// Validates ext4 with inline data enabled.
#[tokio::test]
async fn test_ext4_fsck_inline_data() {
    let image_dir = TempDir::new().expect("failed to create temp dir");

    let image = build_inline_data_image();
    write_image(image_dir.path(), "inline.ext4", &image);

    let container = start_verify_container(image_dir.path()).await;
    run_e2fsck(&container, "inline.ext4").await;

    let content = debugfs_cmd(&container, "inline.ext4", "cat tiny.txt").await;
    assert!(
        content.contains("tiny"),
        "tiny.txt content mismatch: {content}"
    );
}
