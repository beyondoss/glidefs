//! Fuzz target for NBDClientFlags parsing.
//!
//! NBDClientFlags is a 4-byte structure sent by the client during handshake.
//! It contains capability flags that the server must parse safely.

#![no_main]

use deku::prelude::*;
use glidefs::block::NBDClientFlags;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // NBDClientFlags expects 4 bytes
    // Test that parsing never panics, only returns Ok/Err
    let _ = NBDClientFlags::from_bytes((data, 0));

    // Also test exact size input
    if data.len() >= 4 {
        let _ = NBDClientFlags::from_bytes((&data[..4], 0));
    }
});
