//! Fuzz target for full NBD protocol sequences.
//!
//! This fuzzer uses structured input to generate realistic protocol sequences:
//! 1. Client flags (handshake)
//! 2. Option headers with payloads (negotiation)
//! 3. Request commands (transmission)
//!
//! It also tests the manual option data parsing (name_len bounds checking).

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use deku::prelude::*;
use glidefs::block::{NBDClientFlags, NBDOptionHeader, NBDRequest};
use libfuzzer_sys::fuzz_target;

/// Structured input for protocol sequence fuzzing.
#[derive(Debug, Arbitrary)]
struct ProtocolInput {
    client_flags: u32,
    options: Vec<OptionInput>,
    requests: Vec<RequestInput>,
}

#[derive(Debug, Arbitrary)]
struct OptionInput {
    magic: u64,
    option: u32,
    length: u32,
    payload: Vec<u8>,
}

#[derive(Debug, Arbitrary)]
struct RequestInput {
    magic: u32,
    flags: u16,
    cmd_type: u16,
    cookie: u64,
    offset: u64,
    length: u32,
}

fuzz_target!(|data: &[u8]| {
    let mut unstructured = Unstructured::new(data);

    if let Ok(input) = ProtocolInput::arbitrary(&mut unstructured) {
        // Phase 1: Client flags parsing
        let flags_bytes = input.client_flags.to_be_bytes();
        let _ = NBDClientFlags::from_bytes((&flags_bytes, 0));

        // Phase 2: Option negotiation parsing
        for opt in &input.options {
            let mut header_bytes = Vec::with_capacity(16);
            header_bytes.extend_from_slice(&opt.magic.to_be_bytes());
            header_bytes.extend_from_slice(&opt.option.to_be_bytes());
            header_bytes.extend_from_slice(&opt.length.to_be_bytes());

            let _ = NBDOptionHeader::from_bytes((&header_bytes, 0));

            // Test manual option data parsing (name_len bounds check)
            // This mimics server.rs lines 380-386
            if opt.payload.len() >= 4 {
                let name_len = u32::from_be_bytes([
                    opt.payload[0],
                    opt.payload[1],
                    opt.payload[2],
                    opt.payload[3],
                ]) as usize;

                // Verify bounds checking doesn't panic
                let _ = name_len
                    .checked_add(4)
                    .filter(|&total| opt.payload.len() >= total);
            }
        }

        // Phase 3: Request parsing
        for req in &input.requests {
            let mut request_bytes = Vec::with_capacity(28);
            request_bytes.extend_from_slice(&req.magic.to_be_bytes());
            request_bytes.extend_from_slice(&req.flags.to_be_bytes());
            request_bytes.extend_from_slice(&req.cmd_type.to_be_bytes());
            request_bytes.extend_from_slice(&req.cookie.to_be_bytes());
            request_bytes.extend_from_slice(&req.offset.to_be_bytes());
            request_bytes.extend_from_slice(&req.length.to_be_bytes());

            let _ = NBDRequest::from_bytes((&request_bytes, 0));
        }
    }
});
