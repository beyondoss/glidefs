//! Manifest utility functions and hot set format.
//!
//! The legacy binary Manifest format (v2) has been replaced by VolumeManifest (JSON).
//! This module retains S3 key helpers and the boot hot set format.

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
// Boot Hot Set — list of chunk indices needed during boot
// ============================================================================

const HOT_SET_MAGIC: &[u8; 4] = b"GLHS";

/// Serialize a hot set (list of block indices) into binary format.
pub fn serialize_hot_set(chunks: &[u64]) -> Vec<u8> {
    let mut data = Vec::with_capacity(8 + chunks.len() * 8);
    data.extend_from_slice(HOT_SET_MAGIC);
    data.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for &chunk in chunks {
        data.extend_from_slice(&chunk.to_le_bytes());
    }
    data
}

/// Deserialize a hot set from binary format.
pub fn deserialize_hot_set(data: &[u8]) -> anyhow::Result<Vec<u64>> {
    if data.len() < 8 {
        anyhow::bail!("hot set too small");
    }
    if &data[..4] != HOT_SET_MAGIC {
        anyhow::bail!("invalid hot set magic");
    }
    let count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let expected_len = 8 + count * 8;
    if data.len() < expected_len {
        anyhow::bail!(
            "hot set truncated: expected {} bytes, got {}",
            expected_len,
            data.len()
        );
    }
    let mut chunks = Vec::with_capacity(count);
    for i in 0..count {
        let offset = 8 + i * 8;
        let chunk = u64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        chunks.push(chunk);
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_s3_key() {
        assert_eq!(manifest_s3_key("my-vm"), "manifests/my-vm");
    }

    #[test]
    fn test_hot_set_round_trip_empty() {
        let indices: Vec<u64> = vec![];
        let data = serialize_hot_set(&indices);
        let decoded = deserialize_hot_set(&data).unwrap();
        assert_eq!(decoded, indices);
    }

    #[test]
    fn test_hot_set_round_trip() {
        let indices = vec![0, 5, 42, 1000, u64::MAX];
        let data = serialize_hot_set(&indices);
        let decoded = deserialize_hot_set(&data).unwrap();
        assert_eq!(decoded, indices);
    }

    #[test]
    fn test_hot_set_binary_format() {
        let indices = vec![1, 2];
        let data = serialize_hot_set(&indices);
        // Header: 4 bytes magic + 4 bytes count + 2 * 8 bytes
        assert_eq!(data.len(), 8 + 2 * 8);
        assert_eq!(&data[..4], b"GLHS");
        let count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(count, 2);
    }
}
