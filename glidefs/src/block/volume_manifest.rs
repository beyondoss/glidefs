#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! Volume manifest v5: binary GLVM format with pack ID lists per chunk.
//!
//! The volume manifest maps chunk indices to ordered lists of pack IDs.
//! Stored as binary GLVM at `manifests/{export_name}` in S3.
//! Sparse: only chunks with written data appear. Absent = all-zero / unwritten.
//!
//! v5 changes from v4: pack_count widened from u8 to u16 to avoid silent
//! truncation when a chunk accumulates 256+ packs (possible if compaction
//! keeps failing).

use std::collections::BTreeMap;

use super::pack::PackId;

/// GLVM magic bytes.
pub const GLVM_MAGIC: &[u8; 4] = b"GLVM";

/// Volume manifest version 5 (v5: pack_count widened from u8 to u16).
pub const VOLUME_MANIFEST_VERSION: u16 = 5;

/// Version 6 adds an 8-byte `prefetch_len` trailer (before the CRC). Written
/// ONLY when a prefetch hint is present; manifests without one stay byte-for-byte
/// v5, so existing exports see zero churn. A v6-aware daemon reads both.
///
/// DEPLOY-ORDER CAVEAT: once any export persists a v6 manifest, an older daemon
/// that predates this constant will reject it with [`VolumeManifestError::
/// UnsupportedVersion`]. Roll out the v6-aware binary to every node that may
/// serve an export BEFORE enabling prefetch hints for it.
pub const VOLUME_MANIFEST_VERSION_PREFETCH: u16 = 6;

/// Version 7 adds an 8-byte `generation` trailer (the single-attach fencing
/// token, see [`super::fence`]) after the chunk entries, before the CRC.
/// Written ONLY when `generation > 0`, so an un-fenced manifest is byte-identical
/// to v5/v6. `prefetch_len` and `generation` are independent options, so the
/// four combinations get distinct versions:
///   - v5 — neither trailer
///   - v6 — prefetch_len only
///   - v7 — generation only
///   - v8 — both (prefetch_len first, then generation)
///
/// DEPLOY-ORDER CAVEAT (same as v6): once any export persists a v7/v8 manifest,
/// an older daemon that predates these constants rejects it with
/// [`VolumeManifestError::UnsupportedVersion`]. Roll out the v7-aware binary to
/// every node that may serve an export BEFORE enabling fencing for it.
pub const VOLUME_MANIFEST_VERSION_GENERATION: u16 = 7;

/// Version 8 — both the `prefetch_len` (v6) and `generation` (v7) trailers.
pub const VOLUME_MANIFEST_VERSION_PREFETCH_GENERATION: u16 = 8;

/// Version 9 widens the fencing token to the **composite** `(generation,
/// lease_revision)` (see [`super::fence`]). When `lease_revision > 0` the
/// manifest must persist both halves, so v9 carries a fixed 16-byte fence
/// trailer (`generation` u64 LE, then `lease_revision` u64 LE) after the chunk
/// entries (no prefetch trailer), before the CRC.
///
/// v9/v10 are emitted ONLY when `lease_revision > 0` — a participating same-node
/// token. A volume fenced on the orchestrator generation alone (lease_revision
/// == 0) stays byte-identical to v5/v6/v7/v8, so existing exports and
/// generation-only callers see zero churn. A v9/v10 manifest read by a daemon
/// that predates these constants is rejected as
/// [`VolumeManifestError::UnsupportedVersion`]; same deploy-order caveat as v7 —
/// roll out the v9-aware binary before enabling lease-revision fencing.
///   - v9  — composite fence trailer only (no prefetch_len)
///   - v10 — prefetch_len (8B) then composite fence trailer (16B)
pub const VOLUME_MANIFEST_VERSION_COMPOSITE: u16 = 9;

/// Version 10 — both the `prefetch_len` (v6) trailer and the v9 composite fence
/// trailer (`generation` then `lease_revision`). See [`VOLUME_MANIFEST_VERSION_COMPOSITE`].
pub const VOLUME_MANIFEST_VERSION_PREFETCH_COMPOSITE: u16 = 10;

/// Default chunk size for v4: 128 MiB (= 1 ext4 block group).
pub const DEFAULT_CHUNK_SIZE: u64 = 128 * 1024 * 1024;

/// GLVM header size in bytes.
const GLVM_HEADER_SIZE: usize = 32;

/// One chunk's pack list in the v4 manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkEntry {
    /// Ordered pack list: oldest first, newest last (last = highest priority).
    /// After compaction: single entry covering all written blocks.
    pub packs: Vec<PackId>,
    /// Blocks in this chunk whose newest pack-index entry is non-zero
    /// (`comp_length > 0`): the data actually stored, and what is billed.
    ///
    /// A pure function of the chunk's *logical* content, so compaction (which
    /// rewrites packs but not content) never changes it. Flush recomputes it
    /// from the merged pack indexes whenever it appends a pack. `None` =
    /// unknown (a manifest written before counts existed); while any chunk is
    /// unknown [`VolumeManifest::stored_bytes`] is `None` and the count is
    /// computed from the pack indexes before a figure is given.
    pub live_blocks: Option<u32>,
}

/// `live_blocks == None` on disk.
const LIVE_UNKNOWN: u32 = u32::MAX;

/// Header `reserved[0]` flag: the manifest carries the per-chunk live-block
/// counts trailer (`chunk_count` x u32 LE, in chunk order, after the fence
/// trailer, before the CRC). Readers that predate it stop before the trailer
/// and ignore it.
const FLAG_LIVE_COUNTS: u8 = 1;

/// Volume manifest v4: binary format mapping chunk indices to pack ID lists.
///
/// Stored as binary GLVM at the same `manifests/{export_name}` S3 key.
/// Sparse: only chunks with written data appear. Absent = all-zero / unwritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeManifest {
    /// Device size in bytes.
    pub size: u64,
    /// Bytes per chunk (default 128 MiB).
    pub chunk_size: u64,
    /// Bytes per block (default 131072 = 128 KB).
    pub block_size: u32,
    /// Sparse map: chunk_idx → ordered pack list.
    /// Only written chunks appear. Absent = all-zero / unwritten.
    pub chunks: BTreeMap<u32, ChunkEntry>,
    /// Cold-start prefetch hint: the boot working set lives in the first
    /// `prefetch_len` bytes (the trace-ordered priority region). On device open
    /// the server warms `[0, prefetch_len)` into the clean cache so the guest's
    /// first reads are cache hits. `None` = no hint (legacy / non-prioritized).
    pub prefetch_len: Option<u64>,
    /// Single-attach fencing token, high half: the orchestrator's per-instance
    /// placement generation. The highest generation that has owned this volume;
    /// checked (composed with [`Self::lease_revision`]) at attach and at every
    /// manifest sync to fence a partitioned/superseded writer (see
    /// [`super::fence`]). `0` = un-fenced on this half.
    pub generation: u64,
    /// Single-attach fencing token, low half: the node-lease claim revision.
    /// Orders same-`node_id` incarnations (a node-death re-fork onto the same
    /// node does NOT advance `generation`, so this is what fences it). The fence
    /// compares the composite `(generation, lease_revision)`; see
    /// [`super::fence::compose_token`]. `0` = un-fenced on this half (legacy
    /// export, generation-only caller, or non-participating caller). Persisted
    /// only when `> 0` (manifest v9/v10), so generation-only exports stay
    /// byte-identical to v5/v6/v7/v8.
    pub lease_revision: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum VolumeManifestError {
    #[error("unsupported volume manifest version: {0}")]
    UnsupportedVersion(u32),
    #[error("invalid config: block_size and chunk_size must be non-zero")]
    InvalidConfig,
    #[error("binary manifest data too short")]
    TooShort,
    #[error("bad GLVM magic bytes")]
    BadMagic,
    #[error("CRC32 mismatch: stored={stored:#010x}, computed={computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },
    #[error("chunk {chunk_idx} has {pack_count} packs, exceeds u16::MAX (65535)")]
    PackCountOverflow { chunk_idx: u32, pack_count: usize },
}

impl VolumeManifest {
    /// Create an empty v4 manifest for a new device.
    pub fn new(size: u64, block_size: u32) -> Self {
        Self {
            size,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size,
            chunks: BTreeMap::new(),
            prefetch_len: None,
            generation: 0,
            lease_revision: 0,
        }
    }

    /// Number of blocks that fit in one chunk.
    pub fn blocks_per_chunk(&self) -> u32 {
        (self.chunk_size / u64::from(self.block_size)) as u32
    }

    /// Estimated heap bytes — used as the weighter for the manifest cache so
    /// budgeting is in bytes, not entry count. A 10TB volume's manifest is
    /// ~80× larger than a 128GB volume's, and a count-based cap can't tell
    /// the difference. Conservative per-entry overhead (48B node + 24B Vec
    /// header) so we don't under-account on tightly-bounded caches.
    pub fn size_estimate(&self) -> usize {
        use std::mem::size_of;
        let header = size_of::<Self>();
        let chunks: usize = self
            .chunks
            .values()
            .map(|e| 48 + 24 + 8 * e.packs.len())
            .sum();
        header + chunks
    }

    /// Total number of chunks needed to cover the device.
    #[allow(dead_code)]
    pub fn num_chunks(&self) -> u32 {
        self.size.div_ceil(self.chunk_size) as u32
    }

    /// Which chunk index contains the given block index.
    pub fn chunk_idx_for_block(&self, block_index: u64) -> u32 {
        let block_byte_offset = block_index * u64::from(self.block_size);
        (block_byte_offset / self.chunk_size) as u32
    }

    /// Block offset within its chunk (0-based).
    pub fn block_offset_in_chunk(&self, block_index: u64) -> u32 {
        let blocks_per_chunk = u64::from(self.blocks_per_chunk());
        (block_index % blocks_per_chunk) as u32
    }

    /// Get pack IDs for a chunk. Returns None if unwritten.
    pub fn chunk_pack_ids(&self, chunk_idx: u32) -> Option<&[PackId]> {
        self.chunks.get(&chunk_idx).map(|e| e.packs.as_slice())
    }

    /// Collect all (chunk_idx, pack_id) pairs across the manifest.
    ///
    /// Pairs are structurally unique: each pack_id appears exactly once per chunk.
    /// Returns a Vec (no hashing overhead) — callers that need set semantics
    /// can collect into a HashSet.
    pub fn all_pack_ids(&self) -> Vec<(u32, PackId)> {
        let mut result = Vec::new();
        for (&chunk_idx, entry) in &self.chunks {
            for &pack_id in &entry.packs {
                result.push((chunk_idx, pack_id));
            }
        }
        result
    }

    /// Append a new pack to a chunk's pack list (after flush), recording the
    /// chunk's live-block count *after* the pack applies (`None` = unknown).
    pub fn append_pack(&mut self, chunk_idx: u32, pack_id: PackId, live_blocks: Option<u32>) {
        let entry = self
            .chunks
            .entry(chunk_idx)
            .or_insert_with(|| ChunkEntry { packs: Vec::new(), live_blocks: Some(0) });
        entry.packs.push(pack_id);
        entry.live_blocks = live_blocks;
    }

    /// Record a chunk's live-block count, computed from `packs`, if it is still
    /// unknown and the pack list is still exactly `packs`. A flush that landed
    /// meanwhile has set (or invalidated) the count itself; keep that.
    pub fn set_live_blocks_if_unknown(&mut self, chunk_idx: u32, packs: &[PackId], live_blocks: u32) {
        if let Some(e) = self.chunks.get_mut(&chunk_idx)
            && e.live_blocks.is_none()
            && e.packs == packs
        {
            e.live_blocks = Some(live_blocks);
        }
    }

    /// Chunks whose live-block count is unknown, with their pack lists.
    pub fn unknown_live_chunks(&self) -> Vec<(u32, Vec<PackId>)> {
        self.chunks
            .iter()
            .filter(|(_, e)| e.live_blocks.is_none())
            .map(|(&idx, e)| (idx, e.packs.clone()))
            .collect()
    }

    /// Bytes actually stored: non-zero blocks x block_size, summed over chunks.
    /// O(chunks), no I/O. `None` while any chunk's count is unknown: a partial
    /// sum is never returned, because a smaller figure would under-bill without
    /// bound.
    pub fn stored_bytes(&self) -> Option<u64> {
        let mut blocks = 0u64;
        for e in self.chunks.values() {
            blocks += u64::from(e.live_blocks?);
        }
        Some(blocks * u64::from(self.block_size))
    }

    /// Replace a chunk's entire pack list (after compaction).
    ///
    /// **WARNING**: This is a blind overwrite. Prefer `replace_packs_cas` for
    /// compaction to avoid losing packs appended by concurrent drain/flush.
    #[cfg(test)]
    pub fn replace_packs(&mut self, chunk_idx: u32, packs: Vec<PackId>) {
        if packs.is_empty() {
            self.chunks.remove(&chunk_idx);
        } else {
            self.chunks.insert(chunk_idx, ChunkEntry { packs, live_blocks: None });
        }
    }

    /// Compare-and-swap replacement: replaces `old_packs` with `new_packs`,
    /// preserving any packs appended after `old_packs` was snapshotted.
    ///
    /// Returns `true` if the replacement was applied, `false` if the pack list
    /// diverged (e.g., a concurrent drain appended packs during compaction).
    /// On `false`, the caller should abort — the orphaned new base pack in S3
    /// will be cleaned up by GC.
    pub fn replace_packs_cas(
        &mut self,
        chunk_idx: u32,
        old_packs: &[PackId],
        new_packs: Vec<PackId>,
    ) -> bool {
        let Some(entry) = self.chunks.get_mut(&chunk_idx) else {
            // Chunk vanished (concurrent remove). Abort.
            return false;
        };

        // old_packs must be an exact prefix of the current pack list.
        // If not, a concurrent operation modified the chunk.
        if entry.packs.len() < old_packs.len()
            || entry.packs[..old_packs.len()] != *old_packs
        {
            return false;
        }

        // Preserve any packs appended after the snapshot.
        let tail: Vec<PackId> = entry.packs[old_packs.len()..].to_vec();
        let mut merged = new_packs;
        merged.extend(tail);

        if merged.is_empty() {
            self.chunks.remove(&chunk_idx);
        } else {
            entry.packs = merged;
        }

        true
    }

    /// Serialize to binary GLVM format.
    ///
    /// ```text
    /// Header (32 bytes):
    ///   magic:        [u8; 4]  = b"GLVM"
    ///   version:      u16 LE   = 5
    ///   chunk_count:  u32 LE
    ///   chunk_size:   u32 LE
    ///   block_size:   u32 LE
    ///   device_size:  u64 LE
    ///   flags:        u8       (bit 0: live-block counts trailer present)
    ///   reserved:     [u8; 5]
    ///
    /// Chunk entries (sorted by chunk_idx):
    ///   chunk_idx:    u32 LE
    ///   pack_count:   u16 LE
    ///   packs:        [u64 LE; pack_count]
    ///
    /// Trailers, in order (each present per version): prefetch_len, fence,
    /// then live-block counts ([u32 LE; chunk_count], u32::MAX = unknown).
    ///
    /// CRC32 trailer: 4 bytes
    /// ```
    pub fn serialize(&self) -> Result<Vec<u8>, VolumeManifestError> {
        // Pre-compute size.
        let entries_size: usize = self
            .chunks
            .values()
            .map(|e| 4 + 2 + e.packs.len() * 8) // chunk_idx + pack_count(u16) + packs
            .sum();
        // v6 appends an 8-byte prefetch_len trailer; v7 an 8-byte generation
        // trailer; v8 both. v9/v10 widen the fence to a 16-byte composite
        // trailer (generation + lease_revision). Each is emitted only when
        // present, so an un-hinted, un-fenced manifest stays byte-identical to
        // v5 (no churn), and a generation-only fence stays byte-identical to
        // v7/v8 (the lease half only forces v9/v10 once it is non-zero).
        let has_prefetch = self.prefetch_len.is_some();
        let has_lease_revision = self.lease_revision > 0;
        let has_generation = self.generation > 0;
        let prefetch_trailer = if has_prefetch { 8 } else { 0 };
        // Fence trailer: 16 bytes (composite) when lease_revision participates,
        // else 8 bytes (generation only) when generation participates, else 0.
        let fence_trailer = if has_lease_revision {
            16
        } else if has_generation {
            8
        } else {
            0
        };
        let version = if has_lease_revision {
            if has_prefetch {
                VOLUME_MANIFEST_VERSION_PREFETCH_COMPOSITE
            } else {
                VOLUME_MANIFEST_VERSION_COMPOSITE
            }
        } else {
            match (has_prefetch, has_generation) {
                (false, false) => VOLUME_MANIFEST_VERSION,
                (true, false) => VOLUME_MANIFEST_VERSION_PREFETCH,
                (false, true) => VOLUME_MANIFEST_VERSION_GENERATION,
                (true, true) => VOLUME_MANIFEST_VERSION_PREFETCH_GENERATION,
            }
        };
        let live_trailer = self.chunks.len() * 4;
        let total = GLVM_HEADER_SIZE
            + entries_size
            + prefetch_trailer
            + fence_trailer
            + live_trailer
            + 4; // +4 for CRC32
        let mut buf = Vec::with_capacity(total);

        // Header (32 bytes).
        buf.extend_from_slice(GLVM_MAGIC); // 4
        buf.extend_from_slice(&version.to_le_bytes()); // 2
        buf.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes()); // 4
        // chunk_size: truncate to u32 (128 MiB fits)
        buf.extend_from_slice(&(self.chunk_size as u32).to_le_bytes()); // 4
        buf.extend_from_slice(&self.block_size.to_le_bytes()); // 4
        buf.extend_from_slice(&self.size.to_le_bytes()); // 8
        buf.push(FLAG_LIVE_COUNTS); // reserved[0]: feature flags
        buf.extend_from_slice(&[0u8; 5]); // 5 reserved

        // Chunk entries (sorted by chunk_idx via BTreeMap iteration order).
        for (&chunk_idx, entry) in &self.chunks {
            if entry.packs.len() > u16::MAX as usize {
                return Err(VolumeManifestError::PackCountOverflow {
                    chunk_idx,
                    pack_count: entry.packs.len(),
                });
            }
            buf.extend_from_slice(&chunk_idx.to_le_bytes());
            buf.extend_from_slice(&(entry.packs.len() as u16).to_le_bytes());
            for &pack_id in &entry.packs {
                buf.extend_from_slice(&pack_id.to_le_bytes());
            }
        }

        // Trailers (before CRC, covered by it). Order is fixed: prefetch_len
        // first, then the fence trailer — deserialize reads them in the same
        // order. The fence trailer is the composite (generation + lease_revision,
        // 16 bytes) when lease_revision participates (v9/v10), else generation
        // only (8 bytes, v7/v8).
        if let Some(len) = self.prefetch_len {
            buf.extend_from_slice(&len.to_le_bytes());
        }
        if has_lease_revision {
            buf.extend_from_slice(&self.generation.to_le_bytes());
            buf.extend_from_slice(&self.lease_revision.to_le_bytes());
        } else if has_generation {
            buf.extend_from_slice(&self.generation.to_le_bytes());
        }
        // Live-block counts, last so readers that predate them stop before it.
        for entry in self.chunks.values() {
            buf.extend_from_slice(&entry.live_blocks.unwrap_or(LIVE_UNKNOWN).to_le_bytes());
        }

        // CRC32 trailer.
        let crc = crc_fast::crc32_iscsi(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        debug_assert_eq!(buf.len(), total);
        Ok(buf)
    }

    /// Deserialize from binary GLVM format.
    pub fn deserialize(data: &[u8]) -> Result<Self, VolumeManifestError> {
        if data.len() < GLVM_HEADER_SIZE + 4 {
            return Err(VolumeManifestError::TooShort);
        }

        // Verify CRC32.
        let crc_offset = data.len() - 4;
        let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
        let computed_crc = crc_fast::crc32_iscsi(&data[..crc_offset]);
        if stored_crc != computed_crc {
            return Err(VolumeManifestError::CrcMismatch {
                stored: stored_crc,
                computed: computed_crc,
            });
        }

        // Parse header.
        if &data[0..4] != GLVM_MAGIC {
            return Err(VolumeManifestError::BadMagic);
        }
        let version = u16::from_le_bytes(data[4..6].try_into().unwrap());
        if !matches!(
            version,
            VOLUME_MANIFEST_VERSION
                | VOLUME_MANIFEST_VERSION_PREFETCH
                | VOLUME_MANIFEST_VERSION_GENERATION
                | VOLUME_MANIFEST_VERSION_PREFETCH_GENERATION
                | VOLUME_MANIFEST_VERSION_COMPOSITE
                | VOLUME_MANIFEST_VERSION_PREFETCH_COMPOSITE
        ) {
            return Err(VolumeManifestError::UnsupportedVersion(u32::from(version)));
        }
        let has_prefetch = matches!(
            version,
            VOLUME_MANIFEST_VERSION_PREFETCH
                | VOLUME_MANIFEST_VERSION_PREFETCH_GENERATION
                | VOLUME_MANIFEST_VERSION_PREFETCH_COMPOSITE
        );
        // v7/v8 carry a generation-only (8-byte) fence trailer.
        let has_generation = matches!(
            version,
            VOLUME_MANIFEST_VERSION_GENERATION | VOLUME_MANIFEST_VERSION_PREFETCH_GENERATION
        );
        // v9/v10 carry the composite (16-byte) fence trailer: generation + lease_revision.
        let has_composite = matches!(
            version,
            VOLUME_MANIFEST_VERSION_COMPOSITE | VOLUME_MANIFEST_VERSION_PREFETCH_COMPOSITE
        );
        let chunk_count = u32::from_le_bytes(data[6..10].try_into().unwrap()) as usize;
        let chunk_size = u64::from(u32::from_le_bytes(data[10..14].try_into().unwrap()));
        let block_size = u32::from_le_bytes(data[14..18].try_into().unwrap());
        let device_size = u64::from_le_bytes(data[18..26].try_into().unwrap());
        let has_live_counts = data[26] & FLAG_LIVE_COUNTS != 0;
        // reserved: data[27..32]

        if block_size == 0 || chunk_size == 0 {
            return Err(VolumeManifestError::InvalidConfig);
        }

        // Parse chunk entries (pack_count is u16 LE).
        let mut chunks = BTreeMap::new();
        let mut pos = GLVM_HEADER_SIZE;
        for _ in 0..chunk_count {
            if pos + 6 > crc_offset {
                return Err(VolumeManifestError::TooShort);
            }
            let chunk_idx = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            let pack_count =
                u16::from_le_bytes(data[pos + 4..pos + 6].try_into().unwrap()) as usize;
            pos += 6;

            if pos + pack_count * 8 > crc_offset {
                return Err(VolumeManifestError::TooShort);
            }
            let mut packs = Vec::with_capacity(pack_count);
            for _ in 0..pack_count {
                let pack_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                packs.push(pack_id);
                pos += 8;
            }

            chunks.insert(chunk_idx, ChunkEntry { packs, live_blocks: None });
        }

        // Trailers sit between the last chunk entry and the CRC, in fixed order:
        // prefetch_len (v6/v8) first, then generation (v7/v8).
        let prefetch_len = if has_prefetch {
            if pos + 8 > crc_offset {
                return Err(VolumeManifestError::TooShort);
            }
            let v = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            Some(v)
        } else {
            None
        };
        let (generation, lease_revision) = if has_composite {
            // v9/v10: composite (generation u64 LE, then lease_revision u64 LE).
            if pos + 16 > crc_offset {
                return Err(VolumeManifestError::TooShort);
            }
            let g = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let l = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
            pos += 16;
            (g, l)
        } else if has_generation {
            // v7/v8: generation only; lease_revision defaults to 0 (back-compat).
            if pos + 8 > crc_offset {
                return Err(VolumeManifestError::TooShort);
            }
            let g = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            (g, 0)
        } else {
            (0, 0)
        };

        // Live-block counts: one u32 per chunk entry, in chunk order. A manifest
        // without the flag predates them; every chunk stays unknown (`None`).
        if has_live_counts {
            if pos + chunk_count * 4 > crc_offset {
                return Err(VolumeManifestError::TooShort);
            }
            for entry in chunks.values_mut() {
                let n = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                entry.live_blocks = (n != LIVE_UNKNOWN).then_some(n);
                pos += 4;
            }
        }

        Ok(VolumeManifest {
            size: device_size,
            chunk_size,
            block_size,
            chunks,
            prefetch_len,
            generation,
            lease_revision,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without a prefetch hint, output stays byte-identical v5 (zero churn for
    /// existing exports) and round-trips to `None`.
    #[test]
    fn prefetch_none_stays_v5_and_roundtrips() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 0xdead_beef, None);
        let bytes = m.serialize().unwrap();
        // version field is bytes [4..6]
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION,
            "no hint must still be written as v5"
        );
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.prefetch_len, None);
        assert_eq!(back, m);
    }

    /// With a prefetch hint, output is v6 and the value round-trips; a v6-aware
    /// reader handles both formats.
    #[test]
    fn prefetch_some_is_v6_and_roundtrips() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        m.append_pack(0, 2, None);
        m.append_pack(3, 9, None);
        m.prefetch_len = Some(11 * 1024 * 1024);
        let bytes = m.serialize().unwrap();
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION_PREFETCH,
            "a hint must be written as v6"
        );
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.prefetch_len, Some(11 * 1024 * 1024));
        assert_eq!(back, m);
    }

    /// Generation 0 (un-fenced) stays byte-identical v5 — zero churn for
    /// existing exports — and round-trips to 0.
    #[test]
    fn generation_zero_stays_v5_and_roundtrips() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 0xdead_beef, None);
        let bytes = m.serialize().unwrap();
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION,
            "generation 0 must still be written as v5"
        );
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.generation, 0);
        assert_eq!(back, m);
    }

    /// A fenced manifest (generation > 0, no prefetch) is written as v7 and the
    /// token round-trips.
    #[test]
    fn generation_only_is_v7_and_roundtrips() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        m.append_pack(3, 9, None);
        m.generation = 42;
        let bytes = m.serialize().unwrap();
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION_GENERATION,
            "a fenced manifest must be written as v7"
        );
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.generation, 42);
        assert_eq!(back, m);
    }

    /// Both options present → v8, with prefetch_len first then generation, both
    /// round-tripping.
    #[test]
    fn prefetch_and_generation_is_v8_and_roundtrips() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        m.prefetch_len = Some(7 * 1024 * 1024);
        m.generation = 99;
        let bytes = m.serialize().unwrap();
        let live_len = m.chunks.len() * 4; // live-count trailer, after the fence trailer
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION_PREFETCH_GENERATION,
            "prefetch + generation must be written as v8"
        );
        // Trailer order: prefetch_len (8) then generation (8), before the CRC (4).
        let plen = &bytes[bytes.len() - 20 - live_len..bytes.len() - 12 - live_len];
        let gen_bytes = &bytes[bytes.len() - 12 - live_len..bytes.len() - 4 - live_len];
        assert_eq!(u64::from_le_bytes(plen.try_into().unwrap()), 7 * 1024 * 1024);
        assert_eq!(u64::from_le_bytes(gen_bytes.try_into().unwrap()), 99);
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.prefetch_len, Some(7 * 1024 * 1024));
        assert_eq!(back.generation, 99);
        assert_eq!(back.lease_revision, 0);
        assert_eq!(back, m);
    }

    /// lease_revision 0 (un-participating low half) never forces v9/v10: a
    /// generation-only fence stays byte-identical to v7, so existing fenced
    /// exports see zero churn.
    #[test]
    fn lease_revision_zero_stays_v7() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        m.generation = 42;
        m.lease_revision = 0;
        let bytes = m.serialize().unwrap();
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION_GENERATION,
            "lease_revision 0 must stay v7 (generation-only)"
        );
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.generation, 42);
        assert_eq!(back.lease_revision, 0);
        assert_eq!(back, m);
    }

    /// A composite fence (lease_revision > 0, no prefetch) is written as v9 and
    /// both halves round-trip. This is the same-node re-fork case: generation may
    /// be unchanged while the lease revision advances.
    #[test]
    fn composite_is_v9_and_roundtrips() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        m.append_pack(3, 9, None);
        m.generation = 7;
        m.lease_revision = 4;
        let bytes = m.serialize().unwrap();
        let live_len = m.chunks.len() * 4; // live-count trailer, after the fence trailer
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION_COMPOSITE,
            "a composite fence must be written as v9"
        );
        // Trailer: generation (8) then lease_revision (8), before the CRC (4).
        let gen_bytes = &bytes[bytes.len() - 20 - live_len..bytes.len() - 12 - live_len];
        let lease_bytes = &bytes[bytes.len() - 12 - live_len..bytes.len() - 4 - live_len];
        assert_eq!(u64::from_le_bytes(gen_bytes.try_into().unwrap()), 7);
        assert_eq!(u64::from_le_bytes(lease_bytes.try_into().unwrap()), 4);
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.generation, 7);
        assert_eq!(back.lease_revision, 4);
        assert_eq!(back, m);
    }

    /// A lease-only composite (generation 0, lease_revision > 0) still persists
    /// both halves as v9 and round-trips — a participating same-node token need
    /// not carry an orchestrator generation.
    #[test]
    fn composite_lease_only_is_v9_and_roundtrips() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        m.generation = 0;
        m.lease_revision = 5;
        let bytes = m.serialize().unwrap();
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION_COMPOSITE,
        );
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.generation, 0);
        assert_eq!(back.lease_revision, 5);
        assert_eq!(back, m);
    }

    /// prefetch + composite fence → v10, with prefetch_len first then the
    /// composite (generation, lease_revision), all round-tripping.
    #[test]
    fn prefetch_and_composite_is_v10_and_roundtrips() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        m.prefetch_len = Some(3 * 1024 * 1024);
        m.generation = 12;
        m.lease_revision = 8;
        let bytes = m.serialize().unwrap();
        let live_len = m.chunks.len() * 4; // live-count trailer, after the fence trailer
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            VOLUME_MANIFEST_VERSION_PREFETCH_COMPOSITE,
            "prefetch + composite must be written as v10"
        );
        // Trailer order: prefetch_len (8), generation (8), lease_revision (8), CRC (4).
        let plen = &bytes[bytes.len() - 28 - live_len..bytes.len() - 20 - live_len];
        let gen_bytes = &bytes[bytes.len() - 20 - live_len..bytes.len() - 12 - live_len];
        let lease_bytes = &bytes[bytes.len() - 12 - live_len..bytes.len() - 4 - live_len];
        assert_eq!(u64::from_le_bytes(plen.try_into().unwrap()), 3 * 1024 * 1024);
        assert_eq!(u64::from_le_bytes(gen_bytes.try_into().unwrap()), 12);
        assert_eq!(u64::from_le_bytes(lease_bytes.try_into().unwrap()), 8);
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(back.prefetch_len, Some(3 * 1024 * 1024));
        assert_eq!(back.generation, 12);
        assert_eq!(back.lease_revision, 8);
        assert_eq!(back, m);
    }

    /// Format stability: the v6 trailer is purely additive — a v6 manifest is
    /// exactly 8 bytes longer than the identical v5 one, and those 8 bytes are
    /// the LE `prefetch_len` sitting right before the CRC32.
    #[test]
    fn prefetch_trailer_is_additive_8_bytes() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 0x1122_3344_5566_7788, None);
        let v5 = m.serialize().unwrap();
        m.prefetch_len = Some(0x0123_4567_89ab_cdef);
        let v6 = m.serialize().unwrap();
        let live_len = m.chunks.len() * 4; // live-count trailer, after the fence trailer
        assert_eq!(v6.len(), v5.len() + 8, "v6 adds exactly 8 bytes");
        // The 8 bytes before the 4-byte CRC are the prefetch_len.
        let trailer = &v6[v6.len() - 12 - live_len..v6.len() - 4 - live_len];
        assert_eq!(u64::from_le_bytes(trailer.try_into().unwrap()), 0x0123_4567_89ab_cdef);
    }

    /// A v6 manifest whose prefetch trailer was truncated must be rejected, not
    /// silently read as v5 or read with garbage. (CRC would also catch it, but we
    /// reject on the length check first.)
    #[test]
    fn truncated_v6_trailer_is_rejected() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        m.prefetch_len = Some(999);
        let mut bytes = m.serialize().unwrap();
        let live_len = m.chunks.len() * 4; // live-count trailer, after the fence trailer
        // Drop 4 bytes from the middle of the trailer (keep CRC length valid-ish):
        // remove the prefetch trailer entirely but keep header claiming v6.
        bytes.drain(bytes.len() - 12 - live_len..bytes.len() - 4 - live_len); // remove the 8 trailer bytes
        let err = VolumeManifest::deserialize(&bytes);
        assert!(err.is_err(), "truncated v6 must not deserialize");
    }

    /// An unknown future version is rejected.
    #[test]
    fn unknown_version_rejected() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, None);
        let mut bytes = m.serialize().unwrap();
        bytes[4..6].copy_from_slice(&99u16.to_le_bytes()); // forge an unknown version
        // recompute CRC so we exercise the version check, not the CRC check
        let crc_off = bytes.len() - 4;
        let crc = crc_fast::crc32_iscsi(&bytes[..crc_off]);
        bytes[crc_off..].copy_from_slice(&crc.to_le_bytes());
        match VolumeManifest::deserialize(&bytes) {
            Err(VolumeManifestError::UnsupportedVersion(99)) => {}
            other => panic!("expected UnsupportedVersion(99), got {other:?}"),
        }
    }

    /// Regression guard for the bug that motivated dropping the `.hot-set`
    /// artifact: a *block* index is NOT a *chunk* index. The old boot prefetch
    /// stored block indices but fed them to `prefetch_chunks` (which expects
    /// chunk indices), so with 1024 blocks/chunk a hot-set entry like block 5000
    /// addressed "chunk 5000" — which doesn't exist → the prefetch was a no-op.
    /// Index warming must come from the manifest's real chunk indices instead.
    #[test]
    fn block_index_is_not_chunk_index() {
        let m = VolumeManifest::new(8 * 1024 * 1024 * 1024, 131072); // 128 KiB blocks
        assert_eq!(m.blocks_per_chunk(), 1024); // 128 MiB / 128 KiB
        // A block deep in the image maps to a small chunk index, NOT itself.
        assert_eq!(m.chunk_idx_for_block(5000), 4); // 5000 * 128KiB / 128MiB = 4
        assert_ne!(m.chunk_idx_for_block(5000), 5000);
        // Only block indices < blocks_per_chunk (all in chunk 0) coincidentally
        // map a value that is also a valid chunk index — which is why the old
        // code "worked" for tiny single-chunk images and silently did nothing
        // for the rest.
        assert_eq!(m.chunk_idx_for_block(1023), 0);
        assert_eq!(m.chunk_idx_for_block(1024), 1);
    }

    #[test]
    fn test_v4_new_manifest() {
        let m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072); // 1 TB
        assert_eq!(m.size, 1024 * 1024 * 1024 * 1024);
        assert_eq!(m.chunk_size, DEFAULT_CHUNK_SIZE);
        assert_eq!(m.block_size, 131072);
        assert!(m.chunks.is_empty());
        // 128 MiB / 128 KB = 1024 blocks per chunk
        assert_eq!(m.blocks_per_chunk(), 1024);
        // 1 TB / 128 MiB = 8192 chunks
        assert_eq!(m.num_chunks(), 8192);
    }

    #[test]
    fn test_v4_binary_round_trip() {
        let mut m = VolumeManifest::new(100 * 1024 * 1024 * 1024, 131072); // 100 GiB
        m.append_pack(0, 0xDEADBEEF01234567, None);
        m.append_pack(0, 0xCAFEBABE89ABCDEF, None);
        m.append_pack(5, 0x1111111111111111, None);
        m.append_pack(800, 0x2222222222222222, None);

        let bytes = m.serialize().unwrap();
        let m2 = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(m, m2);

        // Verify chunk lookups.
        assert_eq!(
            m2.chunk_pack_ids(0),
            Some([0xDEADBEEF01234567, 0xCAFEBABE89ABCDEF].as_slice())
        );
        assert_eq!(
            m2.chunk_pack_ids(5),
            Some([0x1111111111111111].as_slice())
        );
        assert_eq!(
            m2.chunk_pack_ids(800),
            Some([0x2222222222222222].as_slice())
        );
        assert_eq!(m2.chunk_pack_ids(1), None);
    }

    #[test]
    fn test_v4_sparse_only_written_chunks() {
        let m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072); // 1 TB, empty
        let bytes = m.serialize().unwrap();
        // Header (32) + CRC32 (4) = 36 bytes for empty manifest.
        assert_eq!(bytes.len(), 36);

        let m2 = VolumeManifest::deserialize(&bytes).unwrap();
        assert!(m2.chunks.is_empty());
        assert_eq!(m2.size, 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_v4_manifest_size_matches_predictions() {
        // 1 TB, 50% written, compacted (1 pack/chunk) = ~53 KB per plan.
        let mut m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072);
        for i in 0..4096 {
            // 50% of 8192 chunks
            m.append_pack(i * 2, 0x1234567890ABCDEF, None);
        }
        let bytes = m.serialize().unwrap();
        // 32 (header) + 4096 * (4 + 2 + 8) (entries) + 4096 * 4 (live counts) + 4 (crc)
        // = 32 + 57344 + 16384 + 4 = 73764
        assert_eq!(bytes.len(), 73764);

        // Verify round-trip.
        let m2 = VolumeManifest::deserialize(&bytes).unwrap();
        assert_eq!(m, m2);
    }

    #[test]
    fn test_v4_crc_corruption_detected() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 42, None);

        let mut bytes = m.serialize().unwrap();
        // Corrupt a data byte.
        bytes[GLVM_HEADER_SIZE + 2] ^= 0xFF;
        let err = VolumeManifest::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, VolumeManifestError::CrcMismatch { .. }));
    }

    #[test]
    fn test_v4_bad_magic() {
        let m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        let mut bytes = m.serialize().unwrap();
        bytes[0] = b'X';
        // Fix CRC so we test magic detection, not CRC.
        let crc_offset = bytes.len() - 4;
        let crc = crc_fast::crc32_iscsi(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());

        let err = VolumeManifest::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, VolumeManifestError::BadMagic));
    }

    #[test]
    fn test_v4_truncated_data() {
        let err = VolumeManifest::deserialize(&[0u8; 10]).unwrap_err();
        assert!(matches!(err, VolumeManifestError::TooShort));
    }

    #[test]
    fn test_v4_append_and_replace_packs() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100, None);
        m.append_pack(0, 200, None);
        m.append_pack(0, 300, None);
        assert_eq!(m.chunk_pack_ids(0), Some([100, 200, 300].as_slice()));

        // Compaction: replace 3 packs with 1.
        m.replace_packs(0, vec![400]);
        assert_eq!(m.chunk_pack_ids(0), Some([400].as_slice()));

        // Replace with empty removes the chunk.
        m.replace_packs(0, vec![]);
        assert_eq!(m.chunk_pack_ids(0), None);
        assert!(m.chunks.is_empty());
    }

    #[test]
    fn test_v4_replace_packs_cas_basic() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100, None);
        m.append_pack(0, 200, None);
        m.append_pack(0, 300, None);

        // CAS with correct old_packs succeeds.
        assert!(m.replace_packs_cas(0, &[100, 200, 300], vec![400]));
        assert_eq!(m.chunk_pack_ids(0), Some([400].as_slice()));
    }

    #[test]
    fn test_v4_replace_packs_cas_preserves_concurrent_appends() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100, None);
        m.append_pack(0, 200, None);

        // Simulate compaction snapshot: old_packs = [100, 200]
        let old_packs = vec![100u64, 200];

        // Simulate concurrent drain appending pack 300.
        m.append_pack(0, 300, None);
        // Now chunk 0 = [100, 200, 300]

        // CAS with old snapshot preserves the concurrent append.
        assert!(m.replace_packs_cas(0, &old_packs, vec![400]));
        assert_eq!(m.chunk_pack_ids(0), Some([400, 300].as_slice()));
    }

    #[test]
    fn test_v4_replace_packs_cas_fails_on_diverged_prefix() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100, None);
        m.append_pack(0, 200, None);

        // Another compaction already replaced the pack list.
        m.replace_packs(0, vec![999]);

        // CAS with stale snapshot fails.
        assert!(!m.replace_packs_cas(0, &[100, 200], vec![400]));
        // Manifest is unchanged.
        assert_eq!(m.chunk_pack_ids(0), Some([999].as_slice()));
    }

    #[test]
    fn test_v4_replace_packs_cas_fails_on_missing_chunk() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        // Chunk doesn't exist.
        assert!(!m.replace_packs_cas(0, &[100], vec![200]));
    }

    #[test]
    fn test_v4_all_pack_ids() {
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 100, None);
        m.append_pack(0, 200, None);
        m.append_pack(5, 300, None);
        m.append_pack(5, 100, None); // same pack_id in different chunk is distinct

        let ids = m.all_pack_ids();
        assert_eq!(ids.len(), 4);
        assert!(ids.contains(&(0, 100)));
        assert!(ids.contains(&(0, 200)));
        assert!(ids.contains(&(5, 300)));
        assert!(ids.contains(&(5, 100)));
    }

    #[test]
    fn test_v4_chunk_idx_for_block() {
        let m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072); // 1 TB
        // 128 MiB / 128 KB = 1024 blocks per chunk
        assert_eq!(m.chunk_idx_for_block(0), 0);
        assert_eq!(m.chunk_idx_for_block(1023), 0);
        assert_eq!(m.chunk_idx_for_block(1024), 1);
        assert_eq!(m.chunk_idx_for_block(2048), 2);
    }

    #[test]
    fn test_v4_block_offset_in_chunk() {
        let m = VolumeManifest::new(1024 * 1024 * 1024 * 1024, 131072);
        assert_eq!(m.block_offset_in_chunk(0), 0);
        assert_eq!(m.block_offset_in_chunk(1023), 1023);
        assert_eq!(m.block_offset_in_chunk(1024), 0);
        assert_eq!(m.block_offset_in_chunk(1025), 1);
    }

    #[test]
    fn test_v4_deserialize_empty() {
        let err = VolumeManifest::deserialize(&[]).unwrap_err();
        assert!(matches!(err, VolumeManifestError::TooShort));
    }

    #[test]
    fn test_v4_deserialize_random_bytes() {
        // 64 bytes of garbage — long enough to pass the length check,
        // but CRC won't match.
        let garbage: Vec<u8> = (0u8..64).collect();
        let err = VolumeManifest::deserialize(&garbage).unwrap_err();
        assert!(matches!(err, VolumeManifestError::CrcMismatch { .. }));
    }

    #[test]
    fn test_v4_deserialize_truncated_entry() {
        // Serialize a valid manifest, then chop it so the chunk entry data
        // is incomplete. We re-compute the CRC over the truncated payload
        // so that the CRC check passes but entry parsing hits TooShort.
        let mut m = VolumeManifest::new(1024 * 1024 * 1024, 131072);
        m.append_pack(0, 42, None);
        m.append_pack(0, 43, None);

        let full = m.serialize().unwrap();
        // Keep header + partial entry (first 6 bytes = chunk_idx + pack_count,
        // but not the pack data). Then append a valid CRC for this truncated body.
        let body = &full[..GLVM_HEADER_SIZE + 6];
        let mut truncated = body.to_vec();
        let crc = crc_fast::crc32_iscsi(&truncated);
        truncated.extend_from_slice(&crc.to_le_bytes());

        let err = VolumeManifest::deserialize(&truncated).unwrap_err();
        assert!(matches!(err, VolumeManifestError::TooShort));
    }

    /// Per-chunk live-block counts round-trip, including unknown, alongside
    /// every other trailer; `stored_bytes` sums known counts only.
    #[test]
    fn live_counts_roundtrip_and_sum() {
        let mut m = VolumeManifest::new(512 * 1024 * 1024, 131072);
        m.append_pack(0, 1, Some(10));
        m.append_pack(0, 2, Some(12));
        m.append_pack(3, 9, None);
        m.append_pack(2, 7, Some(5));
        m.prefetch_len = Some(4096);
        m.generation = 3;
        m.lease_revision = 9;
        let back = VolumeManifest::deserialize(&m.serialize().unwrap()).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.chunks[&0].live_blocks, Some(12), "append sets the count after the pack");
        assert_eq!(back.chunks[&3].live_blocks, None);
        assert_eq!(back.stored_bytes(), None, "an unknown chunk makes the figure unavailable, not smaller");
        let mut full = back.clone();
        full.chunks.get_mut(&3).unwrap().live_blocks = Some(2);
        assert_eq!(full.stored_bytes(), Some((12 + 5 + 2) * 131072));
        assert_eq!(back.unknown_live_chunks(), vec![(3, vec![9])]);
    }

    /// A manifest written before counts existed (flag clear, no trailer) loads
    /// with every count unknown and bills nothing.
    #[test]
    fn legacy_manifest_without_counts_loads_unknown() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, Some(4));
        m.append_pack(1, 2, Some(6));
        let mut bytes = m.serialize().unwrap();
        // Strip the counts trailer and the flag, re-seal the CRC.
        let crc_at = bytes.len() - 4;
        let trailer = m.chunks.len() * 4;
        bytes.truncate(crc_at - trailer);
        bytes[26] = 0;
        let crc = crc_fast::crc32_iscsi(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        let back = VolumeManifest::deserialize(&bytes).unwrap();
        assert!(back.chunks.values().all(|e| e.live_blocks.is_none()));
        assert_eq!(back.stored_bytes(), None, "unknown counts give no figure");
    }

    /// A CAS compaction swaps packs but never changes the count; a stale
    /// backfill must not overwrite a count a flush set, nor apply to a changed
    /// pack list.
    #[test]
    fn cas_keeps_count_and_backfill_is_guarded() {
        let mut m = VolumeManifest::new(8 * 1024 * 1024, 4096);
        m.append_pack(0, 1, Some(4));
        m.append_pack(0, 2, Some(6));
        assert!(m.replace_packs_cas(0, &[1, 2], vec![3]));
        assert_eq!(m.chunks[&0].live_blocks, Some(6));

        m.set_live_blocks_if_unknown(0, &[3], 99);
        assert_eq!(m.chunks[&0].live_blocks, Some(6), "known count is kept");

        m.append_pack(1, 8, None);
        m.set_live_blocks_if_unknown(1, &[7], 5);
        assert_eq!(m.chunks[&1].live_blocks, None, "stale pack list is ignored");
        m.set_live_blocks_if_unknown(1, &[8], 5);
        assert_eq!(m.chunks[&1].live_blocks, Some(5));
    }
}
