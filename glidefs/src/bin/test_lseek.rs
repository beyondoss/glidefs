use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;

fn data_regions(fd: i32, start: u64, len: u64) -> Vec<(u64, u64)> {
    let end = start + len;
    let mut regions = Vec::new();
    let mut pos = start;

    loop {
        if pos >= end {
            break;
        }

        let data_start = unsafe { libc::lseek(fd, pos as libc::off_t, libc::SEEK_DATA) };
        if data_start < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENXIO) {
                break;
            }
            panic!("lseek SEEK_DATA failed: {}", err);
        }
        let data_start = data_start as u64;
        if data_start >= end {
            break;
        }

        let hole_start =
            unsafe { libc::lseek(fd, data_start as libc::off_t, libc::SEEK_HOLE) };
        if hole_start < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENXIO) {
                regions.push((data_start, end - data_start));
                break;
            }
            panic!("lseek SEEK_HOLE failed: {}", err);
        }
        let hole_start = (hole_start as u64).min(end);
        regions.push((data_start, hole_start - data_start));
        pos = hole_start;
    }

    regions
}

fn main() {
    let path = "/tmp/test_lseek_sparse";
    let block_size: u64 = 131072; // 128KB
    let device_size: u64 = block_size * 4; // 4 blocks
    let sub_size: usize = 4096; // 4KB

    // Clean up
    let _ = std::fs::remove_file(path);

    // Create file with set_len (like SyncFile::open)
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    file.set_len(device_size).unwrap();
    let fd = file.as_raw_fd();

    println!("Created sparse file: {} bytes", device_size);

    // Check block 0 before any writes
    let regions = data_regions(fd, 0, block_size);
    println!("Block 0 before write: regions={:?}", regions);

    // Write a 4KB sub-block at offset 0 (sub-index 0 of block 0)
    let data = vec![0xABu8; sub_size];
    file.write_all_at(&data, 0).unwrap();

    let regions = data_regions(fd, 0, block_size);
    println!("Block 0 after 4KB write at offset 0: regions={:?}", regions);

    // Write another 4KB sub-block at offset 16384 (sub-index 4 of block 0)
    let data2 = vec![0xCDu8; sub_size];
    file.write_all_at(&data2, 16384).unwrap();

    let regions = data_regions(fd, 0, block_size);
    println!("Block 0 after 2nd 4KB write at offset 16384: regions={:?}", regions);

    // Check block 1 (no writes)
    let regions = data_regions(fd, block_size, block_size);
    println!("Block 1 (no writes): regions={:?}", regions);

    // Write sub-block to block 1
    file.write_all_at(&data, block_size).unwrap();
    let regions = data_regions(fd, block_size, block_size);
    println!("Block 1 after 4KB write: regions={:?}", regions);

    // Check has_holes for block 0
    let data_start = unsafe { libc::lseek(fd, 0, libc::SEEK_DATA) };
    let hole_start = unsafe { libc::lseek(fd, 0, libc::SEEK_HOLE) };
    println!("Block 0: SEEK_DATA from 0 = {}, SEEK_HOLE from 0 = {}", data_start, hole_start);

    // Test: does sync_all() cause holes to become allocated?
    println!("\n--- Testing sync_all() impact on sparseness ---");
    let regions_before = data_regions(fd, 0, block_size);
    println!("Block 0 before sync_all: regions={:?}", regions_before);

    file.sync_all().unwrap();

    let regions_after = data_regions(fd, 0, block_size);
    println!("Block 0 after sync_all: regions={:?}", regions_after);

    // Also test sync_data
    file.sync_data().unwrap();
    let regions_after2 = data_regions(fd, 0, block_size);
    println!("Block 0 after sync_data: regions={:?}", regions_after2);

    // Test: does rename preserve sparseness?
    let path2 = "/tmp/test_lseek_sparse_renamed";
    let _ = std::fs::remove_file(path2);

    // Create fresh sparse file
    let path3 = "/tmp/test_lseek_sparse2";
    let _ = std::fs::remove_file(path3);
    let file2 = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path3)
        .unwrap();
    file2.set_len(device_size).unwrap();
    let fd2 = file2.as_raw_fd();

    // Write sparse sub-blocks
    file2.write_all_at(&data, 0).unwrap();
    file2.write_all_at(&data2, 16384).unwrap();

    let regions = data_regions(fd2, 0, block_size);
    println!("\n--- Rename test ---");
    println!("Before rename: regions={:?}", regions);

    // Rename without closing file
    std::fs::rename(path3, path2).unwrap();

    let regions = data_regions(fd2, 0, block_size);
    println!("After rename (same fd): regions={:?}", regions);

    // Test: does NOT syncing preserve sparseness?
    // (The flushing file is never synced - only renamed)

    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path2);
    println!("\nDone.");
}
