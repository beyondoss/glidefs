//! Build a merged EROFS image from a skopeo `dir:` OCI layout, for benchmarking
//! the daemonless mount/boot path. Usage:
//!   oci_to_erofs <skopeo-dir> <out.erofs>
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use ext4::tar_convert::ConvertOptions;
use ext4::writer::WriterOption;

fn decompress(blob: &Path) -> std::io::Result<File> {
    let mut f = File::open(blob)?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)?;
    f.seek(SeekFrom::Start(0))?;
    let mut out = tempfile::tempfile()?;
    if magic[0] == 0x1f && magic[1] == 0x8b {
        std::io::copy(&mut flate2::read::GzDecoder::new(f), &mut out)?;
    } else if magic == [0x28, 0xb5, 0x2f, 0xfd] {
        std::io::copy(&mut zstd::Decoder::new(f)?, &mut out)?;
    } else {
        std::io::copy(&mut f, &mut out)?;
    }
    out.seek(SeekFrom::Start(0))?;
    Ok(out)
}

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1).expect("usage: oci_to_erofs <dir> <out>"));
    let out = std::env::args().nth(2).expect("usage: oci_to_erofs <dir> <out>");
    let bytes = std::fs::read(dir.join("manifest.json")).expect("read manifest.json");
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("parse manifest");
    let mut layers: Vec<File> = v["layers"]
        .as_array()
        .expect("layers[]")
        .iter()
        .map(|l| {
            let d = l["digest"].as_str().expect("digest");
            decompress(&dir.join(d.strip_prefix("sha256:").unwrap_or(d))).expect("decompress")
        })
        .collect();
    // Snap large file payloads to the 128 KiB dedup grid: identical files then
    // produce identical content-addressed blocks regardless of upstream churn,
    // so a node already caching a sibling image fetches far fewer blocks from S3.
    // The padding is holes (zeros) the block layer never stores. EROFS has no
    // interleaved group metadata, so this is always structurally safe.
    const GRID: u32 = 128 * 1024;
    let writer_options = vec![
        WriterOption::Uuid([0u8; 16]),
        WriterOption::AlignData { align: GRID, min_size: 16 * 1024 },
    ];
    let opts = ConvertOptions { convert_backslash: false, writer_options };
    let t = std::time::Instant::now();
    let mut fs = ext4::convert_oci_layers_to_erofs(
        &mut layers,
        tempfile::tempfile().unwrap(),
        &opts,
    )
    .expect("convert erofs");
    fs.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    fs.read_to_end(&mut buf).unwrap();
    std::fs::write(&out, &buf).expect("write out");
    eprintln!("built {out} ({} bytes) in {} ms", buf.len(), t.elapsed().as_millis());
}
