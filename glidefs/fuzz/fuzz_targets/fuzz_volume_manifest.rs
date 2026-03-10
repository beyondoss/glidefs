//! Fuzz target for VolumeManifest deserialization.
//!
//! `VolumeManifest::deserialize` parses manifests loaded from S3.
//! It must never panic on arbitrary input — corrupted or adversarial
//! S3 data should always produce an Err, not a crash.

#![no_main]

use glidefs::block::volume_manifest::VolumeManifest;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = VolumeManifest::deserialize(data);
});
