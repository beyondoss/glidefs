//! Backfill of per-chunk live-block counts for manifests written before counts
//! existed (see [`VolumeManifest::stored_bytes`]).
//!
//! Runs in the background after an export opens, never on the attach path. Each
//! unknown chunk's count is recomputed from its pack indexes and recorded only
//! if still unknown: a flush that landed meanwhile has already set an exact
//! value, and compaction never changes a chunk's logical content.

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tracing::{info, warn};

use super::content_store::ContentStore;
use super::pack_index_cache::{PackIndexCache, live_blocks_after};
use super::volume_manifest::VolumeManifest;

const BACKFILL_CONCURRENCY: usize = 4;

/// Whether any chunk's live-block count is unknown.
pub fn needs_backfill(vm: &parking_lot::RwLock<VolumeManifest>) -> bool {
    vm.read().chunks.values().any(|e| e.live_blocks.is_none())
}

/// Compute and record the live-block count of every chunk that lacks one.
pub async fn backfill_live_counts(
    vm: &Arc<parking_lot::RwLock<VolumeManifest>>,
    content_store: &ContentStore,
    pack_index_cache: &Arc<PackIndexCache>,
) {
    let unknown = vm.read().unknown_live_chunks();
    if unknown.is_empty() {
        return;
    }
    let total = unknown.len();
    stream::iter(unknown)
        .for_each_concurrent(BACKFILL_CONCURRENCY, |(chunk_idx, pack_ids)| async move {
            for &pid in &pack_ids {
                if pack_index_cache.get_entries(pid).await.is_some() {
                    continue;
                }
                match content_store.get_pack_index(chunk_idx, pid).await {
                    Ok(entries) => pack_index_cache.insert_entries(pid, &entries),
                    Err(e) => {
                        warn!(chunk_idx, pack_id = pid, error = %e, "live-count backfill: pack index unavailable, chunk stays unbilled");
                        return;
                    }
                }
            }
            if let Some(existing) = pack_index_cache.existing_blocks(&pack_ids).await {
                let live = live_blocks_after(&existing, []);
                vm.write().set_live_blocks_if_unknown(chunk_idx, &pack_ids, live);
            }
        })
        .await;
    info!(chunks = total, "live-block counts backfilled");
}
