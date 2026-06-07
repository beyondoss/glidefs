//! Convert a tar of a filesystem tree into an EROFS image (glid(ero)fs format).
//! Usage: tar_to_erofs <in.tar> <out.erofs>
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use ext4::tar_convert::ConvertOptions;
use ext4::writer::WriterOption;

fn main() {
    let tar = std::env::args().nth(1).expect("usage: tar_to_erofs <in.tar> <out.erofs>");
    let out = std::env::args().nth(2).expect("usage: tar_to_erofs <in.tar> <out.erofs>");
    let f = File::open(&tar).expect("open tar");
    let opts = ConvertOptions {
        convert_backslash: false,
        writer_options: vec![WriterOption::Uuid([0u8; 16])],
    };
    let t = std::time::Instant::now();
    let mut fs = ext4::convert_layer_to_erofs(f, std::io::Cursor::new(Vec::new()), &opts)
        .expect("convert tar -> erofs");
    fs.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    fs.read_to_end(&mut buf).unwrap();
    std::fs::write(&out, &buf).expect("write erofs");
    eprintln!("EROFS {out} ({} bytes) in {} ms", buf.len(), t.elapsed().as_millis());
}
