//! Fuzz target for NBDOptionHeader parsing.
//!
//! NBDOptionHeader is a 16-byte structure with:
//! - magic: u64 (asserted to be NBD_IHAVEOPT)
//! - option: u32
//! - length: u32
//!
//! The magic assertion can fail, which should return an error, not panic.

#![no_main]

use deku::prelude::*;
use glidefs::block::NBDOptionHeader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // NBDOptionHeader expects 16 bytes with magic validation
    // The parser should safely reject invalid magic without panic
    let _ = NBDOptionHeader::from_bytes((data, 0));

    // Test exact size input
    if data.len() >= 16 {
        let _ = NBDOptionHeader::from_bytes((&data[..16], 0));
    }
});
