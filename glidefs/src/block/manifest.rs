//! Manifest utility functions.
//!
//! The legacy binary Manifest format (v2) has been replaced by VolumeManifest.
//! This module retains the S3 key helpers.

/// Generate S3 key for a manifest: "manifests/{name}"
pub fn manifest_s3_key(name: &str) -> String {
    format!("manifests/{name}")
}

/// Generate S3 key for a versioned snapshot: "snapshots/{name}/{sequence:020}"
///
/// Zero-padded to 20 digits so S3 LIST returns lexicographic = numeric order.
pub fn snapshot_s3_key(name: &str, sequence: u64) -> String {
    format!("snapshots/{name}/{sequence:020}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_s3_key() {
        assert_eq!(manifest_s3_key("my-vm"), "manifests/my-vm");
    }
}
