//! Fuzz target for LZ4 decompression.
//!
//! `lz4_decompress` is called on every S3 cache-miss read. Corrupted
//! pack data (bit flips, partial uploads) must produce a DecompressError,
//! not a panic. This exercises lz4_flex's size-prepended format parser.

#![no_main]

use glidefs::block::block_map::lz4_decompress;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = lz4_decompress(data);
});
