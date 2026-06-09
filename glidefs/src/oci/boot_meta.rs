//! Sidecar metadata for decoupled, idempotent boot-set profiling.
//!
//! Two small JSON artifacts stored beside a base manifest:
//! * `bases/{name}.boot-set.meta` ([`BootSetMeta`]) — the fingerprint + provenance
//!   that lets `glidefs profile` skip an unchanged base ("profile once per base").
//! * `bases/{name}.runspec` ([`RunSpec`]) — the entrypoint argv/env recorded at
//!   bless time so the decoupled `profile` step can reconstruct the exact boot
//!   command without re-resolving the OCI image.
//!
//! Both are additive (old daemons ignore them) and deliberately separate from the
//! block-list codec ([`crate::block::manifest::serialize_block_list`]) and the v6
//! manifest, which stay byte-compatible.

use serde::{Deserialize, Serialize};

/// Current `BootSetMeta` schema version.
pub const BOOT_SET_META_VERSION: u32 = 1;

/// Provenance + fingerprint for a base's captured boot set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootSetMeta {
    pub version: u32,
    /// Fingerprint of the base this set was captured from — the base manifest's
    /// S3 ETag. A re-bless rewrites the manifest → new ETag → re-profile. This is
    /// a skip *optimization*, never a correctness gate (some stores synthesize
    /// ETags); `--force` always overrides.
    pub fingerprint: String,
    /// Filesystem the base was served as (`ext4` | `erofs`).
    pub fs_type: String,
    /// Number of blocks in the captured set.
    pub block_count: u64,
    /// When the profile ran (RFC 3339).
    pub profiled_at: String,
    /// OCI config digest, when known (secondary provenance).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_digest: Option<String>,
}

impl BootSetMeta {
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec_pretty(self).expect("BootSetMeta serializes")
    }
    pub fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// The boot command recorded at bless time for the decoupled profiler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSpec {
    pub argv: Vec<String>,
    pub env: Vec<String>,
    #[serde(default)]
    pub workdir: String,
    pub fs_type: String,
    /// The static ELF-closure boot paths ([`crate::oci::boot_set::derive_boot_paths`])
    /// recorded at bless, so the decoupled profiler can union them into the
    /// captured set (read under the tracer) without re-scanning the image layers.
    #[serde(default)]
    pub static_seed: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_digest: Option<String>,
}

impl RunSpec {
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec_pretty(self).expect("RunSpec serializes")
    }
    pub fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}
