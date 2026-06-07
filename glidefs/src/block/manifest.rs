#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! Manifest utility functions and the block-list (boot set) format.
//!
//! The legacy binary Manifest format (v2) has been replaced by VolumeManifest.
//! This module retains S3 key helpers and a small codec for a list of u64 block
//! indices — used to persist the trace-captured **boot set** that the server
//! data-prefetches on device open.

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

// ============================================================================
// Block-list codec — persists a list of u64 block indices (the boot set).
// ============================================================================

const BLOCK_LIST_MAGIC: &[u8; 4] = b"GLHS";

/// Serialize a list of block indices into binary format.
pub fn serialize_block_list(blocks: &[u64]) -> Vec<u8> {
    let mut data = Vec::with_capacity(8 + blocks.len() * 8);
    data.extend_from_slice(BLOCK_LIST_MAGIC);
    data.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
    for &b in blocks {
        data.extend_from_slice(&b.to_le_bytes());
    }
    data
}

/// Deserialize a block-index list from binary format.
pub fn deserialize_block_list(data: &[u8]) -> anyhow::Result<Vec<u64>> {
    if data.len() < 8 {
        anyhow::bail!("block list too small");
    }
    if &data[..4] != BLOCK_LIST_MAGIC {
        anyhow::bail!("invalid block list magic");
    }
    let count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let expected_len = 8 + count * 8;
    if data.len() < expected_len {
        anyhow::bail!("block list truncated: expected {} bytes, got {}", expected_len, data.len());
    }
    let mut blocks = Vec::with_capacity(count);
    for i in 0..count {
        let offset = 8 + i * 8;
        blocks.push(u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()));
    }
    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_s3_key() {
        assert_eq!(manifest_s3_key("my-vm"), "manifests/my-vm");
    }

    #[test]
    fn test_block_list_round_trip_empty() {
        let indices: Vec<u64> = vec![];
        assert_eq!(deserialize_block_list(&serialize_block_list(&indices)).unwrap(), indices);
    }

    #[test]
    fn test_block_list_round_trip() {
        let indices = vec![0, 5, 42, 1000, u64::MAX];
        assert_eq!(deserialize_block_list(&serialize_block_list(&indices)).unwrap(), indices);
    }

    #[test]
    fn test_block_list_binary_format() {
        let data = serialize_block_list(&[1, 2]);
        assert_eq!(data.len(), 8 + 2 * 8);
        assert_eq!(&data[..4], b"GLHS");
        assert_eq!(u32::from_le_bytes([data[4], data[5], data[6], data[7]]), 2);
    }
}
