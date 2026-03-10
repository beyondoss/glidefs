//! Fuzz target for pack index parsing.
//!
//! `parse_pack_index` must never panic regardless of input — it should
//! return Ok with valid entries or an Err for malformed data.

#![no_main]

use glidefs::block::pack::parse_pack_index;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = parse_pack_index(data);
});
