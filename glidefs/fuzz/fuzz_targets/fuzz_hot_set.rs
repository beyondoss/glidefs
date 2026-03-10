//! Fuzz target for boot hot set deserialization.
//!
//! `deserialize_hot_set` parses a length-prefixed list of chunk indices
//! loaded from S3. It must never panic on arbitrary input — a corrupted
//! count field or truncated data should produce an Err, not a crash.

#![no_main]

use glidefs::block::manifest::deserialize_hot_set;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = deserialize_hot_set(data);
});
