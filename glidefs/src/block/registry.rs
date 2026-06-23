//! Logical → physical resolution: GlideFS owns the volume / image / snapshot
//! name → location mapping, so callers address everything by stable logical
//! names and never supply an `s3_prefix` or `manifest_name`.
//!
//! The physical S3 layout is unchanged. These durable, name-keyed index objects
//! live under `{db_path}/index/` (a sibling of `{db_path}/exports/`, where
//! `export.json` — the volume index — already lives), and are read on every
//! resolve so any node can locate a volume/image/snapshot from its name alone.

use serde::{Deserialize, Serialize};

/// A logical source reference for creating or forking a volume. Parsed from the
/// `from` field of a create request. Its canonical string form is persisted as
/// the resulting volume's lineage (`ExportConfig::source`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromRef {
    /// Fresh blank volume — no backing manifest.
    Blank,
    /// Fork from a blessed image by logical name.
    Image(String),
    /// Fork from another volume's current committed state.
    Volume(String),
    /// Fork from a specific snapshot by its stable id.
    Snapshot(String),
}

/// Error parsing a `from` ref.
#[derive(Debug, thiserror::Error)]
pub enum FromRefError {
    #[error(
        "invalid `from` ref '{0}': expected 'image:<name>', 'volume:<name>', or 'snapshot:<id>'"
    )]
    Invalid(String),
    #[error("`from` ref '{0}' has an empty target name")]
    EmptyTarget(String),
}

impl FromRef {
    /// Parse a `from` field. `None` or empty/whitespace → [`FromRef::Blank`].
    pub fn parse(s: Option<&str>) -> Result<FromRef, FromRefError> {
        let Some(s) = s.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(FromRef::Blank);
        };
        let (kind, target) = s
            .split_once(':')
            .ok_or_else(|| FromRefError::Invalid(s.to_string()))?;
        if target.is_empty() {
            return Err(FromRefError::EmptyTarget(s.to_string()));
        }
        match kind {
            "image" => Ok(FromRef::Image(target.to_string())),
            "volume" => Ok(FromRef::Volume(target.to_string())),
            "snapshot" => Ok(FromRef::Snapshot(target.to_string())),
            _ => Err(FromRefError::Invalid(s.to_string())),
        }
    }

    /// Canonical string form, persisted as a volume's lineage. `Blank` → `None`.
    pub fn as_source(&self) -> Option<String> {
        match self {
            FromRef::Blank => None,
            FromRef::Image(n) => Some(format!("image:{n}")),
            FromRef::Volume(n) => Some(format!("volume:{n}")),
            FromRef::Snapshot(id) => Some(format!("snapshot:{id}")),
        }
    }

    /// The target name/id this ref points at (`None` for `Blank`).
    pub fn target(&self) -> Option<&str> {
        match self {
            FromRef::Blank => None,
            FromRef::Image(n) | FromRef::Volume(n) | FromRef::Snapshot(n) => Some(n),
        }
    }
}

/// Physical coordinates a [`FromRef`] resolves to, fed into the existing fork
/// machinery (`create_export`'s `s3_prefix` / `manifest_name` /
/// `snapshot_sequence`).
#[derive(Debug, Clone)]
pub struct ResolvedSource {
    /// Pool (`s3_prefix`) the new volume must live in so CoW pack sharing works.
    /// `None` for a blank volume (it gets its own pool = its name).
    pub pool: Option<String>,
    /// Manifest name within the pool to fork from. `None` for a blank volume.
    pub manifest_name: Option<String>,
    /// Snapshot sequence, when forking from a snapshot.
    pub snapshot_sequence: Option<u64>,
}

impl ResolvedSource {
    /// A blank volume: its own pool, no backing manifest.
    pub fn blank() -> Self {
        ResolvedSource {
            pool: None,
            manifest_name: None,
            snapshot_sequence: None,
        }
    }
}

/// Durable image-index entry: where a blessed image's manifest physically lives.
/// Written when an image is blessed/promoted; read when a volume forks from
/// `image:<name>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageEntry {
    /// Logical image name.
    pub name: String,
    /// Pool holding the image's packs + manifest.
    pub pool: String,
    /// Manifest name within the pool (e.g. `"bases/<name>"`).
    pub manifest: String,
}

/// Durable snapshot-index entry: resolves a stable snapshot id to its physical
/// `(pool, owning volume, sequence)` and parent lineage. Written by
/// `snapshot_export`; read when a volume forks from `snapshot:<id>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    /// Stable snapshot id (`"{volume}@{sequence}"`).
    pub id: String,
    /// Pool holding the snapshot manifest + the volume's packs.
    pub pool: String,
    /// Volume the snapshot was taken from (`snapshot_s3_key` uses this name).
    pub volume: String,
    /// Monotonic sequence assigned by `snapshot_export`.
    pub sequence: u64,
    /// Optional human label supplied at snapshot time.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub label: Option<String>,
    /// Lineage: the source the owning volume was forked from, if any.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent: Option<String>,
}

/// Stable snapshot id from `(volume, sequence)`. Unique by construction
/// (sequences are monotonic and never reused for a volume).
pub fn snapshot_id(volume: &str, sequence: u64) -> String {
    format!("{volume}@{sequence}")
}

/// S3 path for an image-index entry, derived from `db_path` + name alone.
pub fn image_index_path(db_path: &str, name: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{db_path}/index/images/{name}.json"))
}

/// S3 path for a snapshot-index entry, derived from `db_path` + id alone.
pub fn snapshot_index_path(db_path: &str, id: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{db_path}/index/snapshots/{id}.json"))
}

/// Write an image-index entry to S3 (idempotent). Free function so both the
/// daemon (`ExportRouter`) and the `glidefs bless` CLI register images in the
/// same logical index — every bless path, HTTP or CLI, is resolvable by name.
pub async fn put_image_entry(
    object_store: &std::sync::Arc<dyn object_store::ObjectStore>,
    db_path: &str,
    entry: &ImageEntry,
) -> Result<(), object_store::Error> {
    let path = image_index_path(db_path, &entry.name);
    let json = serde_json::to_vec(entry).map_err(|e| object_store::Error::Generic {
        store: "image-index",
        source: Box::new(e),
    })?;
    object_store
        .put(&path, bytes::Bytes::from(json).into())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_blank() {
        assert_eq!(FromRef::parse(None).unwrap(), FromRef::Blank);
        assert_eq!(FromRef::parse(Some("")).unwrap(), FromRef::Blank);
        assert_eq!(FromRef::parse(Some("   ")).unwrap(), FromRef::Blank);
    }

    #[test]
    fn parse_kinds() {
        assert_eq!(
            FromRef::parse(Some("image:ubuntu")).unwrap(),
            FromRef::Image("ubuntu".into())
        );
        assert_eq!(
            FromRef::parse(Some("volume:inst1/vol0")).unwrap(),
            FromRef::Volume("inst1/vol0".into())
        );
        assert_eq!(
            FromRef::parse(Some("snapshot:vol0@7")).unwrap(),
            FromRef::Snapshot("vol0@7".into())
        );
    }

    #[test]
    fn parse_rejects_unknown_and_empty_target() {
        assert!(FromRef::parse(Some("bogus:x")).is_err());
        assert!(FromRef::parse(Some("noscheme")).is_err());
        assert!(FromRef::parse(Some("image:")).is_err());
    }

    #[test]
    fn roundtrip_as_source() {
        for s in ["image:ubuntu", "volume:v1", "snapshot:v1@3"] {
            let r = FromRef::parse(Some(s)).unwrap();
            assert_eq!(r.as_source().as_deref(), Some(s));
        }
        assert_eq!(FromRef::Blank.as_source(), None);
    }

    #[test]
    fn snapshot_id_format() {
        assert_eq!(snapshot_id("vol0", 7), "vol0@7");
    }
}
