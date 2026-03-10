//! Fuzz target for WAL replay.
//!
//! Writes fuzzed bytes as a WAL file, then runs `Wal::replay`.
//! replay() must never panic on arbitrary data — it should return Ok
//! with whatever valid records it could parse, stopping at any
//! corruption boundary.

#![no_main]

use glidefs::block::wal::Wal;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::TempDir::new().expect("failed to create temp dir");
    let wal_path = dir.path().join("dirty.wal");

    std::fs::write(&wal_path, data).expect("failed to write WAL file");

    let _ = Wal::replay(&wal_path, 0);
});
