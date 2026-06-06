//! Fuzz target for codec-detecting block decompression.
//!
//! `decompress_block` is called on every S3 cache-miss read. It sniffs the zstd
//! magic and dispatches to zstd or legacy LZ4. Corrupted pack data (bit flips,
//! partial uploads, adversarial size prefixes) must produce an error, never a
//! panic or unbounded allocation. Arbitrary input exercises both codec branches.

#![no_main]

use glidefs::block::block_map::decompress_block;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decompress_block(data);
});
