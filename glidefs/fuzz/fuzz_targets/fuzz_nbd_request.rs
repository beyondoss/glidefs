//! Fuzz target for NBDRequest parsing.
//!
//! NBDRequest is a 28-byte structure containing:
//! - magic: u32 (asserted to be NBD_REQUEST_MAGIC)
//! - flags: u16
//! - cmd_type: NBDCommand (2-byte enum with catch-all Unknown variant)
//! - cookie: u64
//! - offset: u64
//! - length: u32
//!
//! This is the most critical parsing target as it handles all client commands.

#![no_main]

use deku::prelude::*;
use glidefs::block::NBDRequest;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // NBDRequest expects 28 bytes with magic validation
    // Tests both magic validation and NBDCommand enum parsing
    let _ = NBDRequest::from_bytes((data, 0));

    // Test exact size input
    if data.len() >= 28 {
        let _ = NBDRequest::from_bytes((&data[..28], 0));
    }

    // Test with trailing data (should parse successfully and ignore trailing bytes)
    if data.len() > 28 {
        let result = NBDRequest::from_bytes((data, 0));
        if let Ok(((_remaining, _offset), _request)) = result {
            // Successfully parsed with remaining data - this is valid behavior
        }
    }
});
