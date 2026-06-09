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

/// Magic for the boot block-list codec (a base's precise boot working set).
const BLOCK_LIST_MAGIC: &[u8; 4] = b"GLHS";

/// Serialize a bounded list of device block indices (the boot working set) for
/// storage as base metadata. Consumed by the device-open precise warm on
/// non-EROFS images, which cannot reorder their layout and so must fetch the
/// exact scattered blocks. See [`deserialize_block_list`].
pub fn serialize_block_list(blocks: &[u64]) -> Vec<u8> {
    let mut data = Vec::with_capacity(8 + blocks.len() * 8);
    data.extend_from_slice(BLOCK_LIST_MAGIC);
    data.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
    for &b in blocks {
        data.extend_from_slice(&b.to_le_bytes());
    }
    data
}

/// Parse a boot block list produced by [`serialize_block_list`]. Rejects a bad
/// magic or a truncated body rather than returning a partial list.
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
    fn block_list_round_trips() {
        let blocks = vec![9u64, 2, 3, 1000, u64::MAX];
        let enc = serialize_block_list(&blocks);
        assert_eq!(deserialize_block_list(&enc).unwrap(), blocks);
    }

    #[test]
    fn block_list_rejects_bad_magic_and_truncation() {
        assert!(deserialize_block_list(b"XXXX\0\0\0\0").is_err());
        let mut enc = serialize_block_list(&[1, 2, 3]);
        enc.truncate(enc.len() - 1);
        assert!(deserialize_block_list(&enc).is_err());
    }
}
