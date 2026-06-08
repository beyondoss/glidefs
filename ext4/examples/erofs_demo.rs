//! Dev tool: build a small EROFS image with the hand-rolled writer and write it
//! to the given path, so it can be mounted with the in-kernel driver for
//! verification. Usage: `cargo run -p ext4 --example erofs_demo -- /tmp/x.erofs`
use std::io::{Cursor, Write};

use ext4::erofs::Writer;
use ext4::{File, WriterOption};

fn reg(size: i64) -> File {
    File {
        mode: 0x8000 | 0o644, // S_IFREG
        size,
        ..Default::default()
    }
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: erofs_demo <out.erofs>");
    let mut w = Writer::new(Cursor::new(Vec::new()), &[WriterOption::Uuid([0u8; 16])]);

    w.create("hello.txt", &reg(12)).unwrap();
    w.write_all(b"hello erofs\n").unwrap();

    w.make_parents("sub/a.txt").unwrap();
    w.create("sub/a.txt", &reg(7)).unwrap();
    w.write_all(b"nested\n").unwrap();

    // a file spanning multiple blocks (full blocks + inline tail)
    let big = vec![0x41u8; 10_000];
    w.create("big.bin", &reg(big.len() as i64)).unwrap();
    w.write_all(&big).unwrap();

    // a symlink
    w.create(
        "link",
        &File {
            mode: 0xA000 | 0o777, // S_IFLNK
            linkname: "hello.txt".to_string(),
            ..Default::default()
        },
    )
    .unwrap();

    let img = w.close().unwrap().into_inner();
    std::fs::write(&path, &img).unwrap();
    eprintln!("wrote {} ({} bytes)", path, img.len());
}
