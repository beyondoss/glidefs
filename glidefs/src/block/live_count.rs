//! Per-chunk live-block counts that are unknown (manifests written before
//! counts existed): computing them from the pack indexes, in the background
//! after export open and on demand when a figure is asked for.
//!
//! A count is recorded only if the chunk is still unknown and its pack list is
//! unchanged: a flush that landed meanwhile has already set an exact value, and
//! compaction never changes a chunk's logical content.
//!
//! Index fetches are capped process-wide ([`MAX_CONCURRENT_CHUNKS`]), so a host
//! with many unknown exports cannot stampede the object store. Flush is
//! foreground work and is never throttled by it.

use std::sync::{Arc, LazyLock};

use futures::stream::{self, StreamExt};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use super::content_store::{ContentStore, ContentStoreError};
use super::pack::{PackId, PackIndexEntry};
use super::pack_index_cache::{ExistingBlocks, PackIndexCache, live_blocks_after};
use super::volume_manifest::VolumeManifest;

/// Chunks whose indexes are being fetched at once, across every export in the
/// process. Each fetches its packs one at a time, so this is also the cap on
/// concurrent index GETs from this module.
pub const MAX_CONCURRENT_CHUNKS: usize = 8;

/// Chunks in flight per call (the global cap above is the real bound).
const PER_CALL_CONCURRENCY: usize = 4;

/// Attempts at making every count known before giving up on an exact figure
/// (a compaction can swap a chunk's packs between the read and the record).
const ENSURE_ROUNDS: usize = 3;

static CHUNK_PERMITS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(MAX_CONCURRENT_CHUNKS));

/// The merged (newest wins) view of a chunk's packs, fetching any index the
/// cache lacks from the object store. Never partial: a failed fetch is an error.
pub async fn load_existing(
    pack_index_cache: &PackIndexCache,
    content_store: &ContentStore,
    chunk_idx: u32,
    pack_ids: &[PackId],
) -> Result<ExistingBlocks, ContentStoreError> {
    let mut map = ExistingBlocks::new();
    for &pid in pack_ids {
        let entries: Arc<[PackIndexEntry]> = match pack_index_cache.get_entries(pid).await {
            Some(e) => e,
            None => {
                let fetched = content_store.get_pack_index(chunk_idx, pid).await?;
                pack_index_cache.insert_entries(pid, &fetched);
                Arc::from(fetched)
            }
        };
        for e in entries.iter() {
            map.insert(e.chunk_offset, (e.hash, e.comp_length));
        }
    }
    Ok(map)
}

/// Whether any chunk's live-block count is unknown.
pub fn needs_backfill(vm: &parking_lot::RwLock<VolumeManifest>) -> bool {
    vm.read().chunks.values().any(|e| e.live_blocks.is_none())
}

/// Compute and record every unknown count once. Fails on the first chunk whose
/// indexes could not be loaded (others are still recorded).
async fn ensure_once(
    vm: &Arc<parking_lot::RwLock<VolumeManifest>>,
    content_store: &ContentStore,
    pack_index_cache: &Arc<PackIndexCache>,
) -> Result<(), ContentStoreError> {
    let unknown = vm.read().unknown_live_chunks();
    let results: Vec<Result<(), ContentStoreError>> = stream::iter(unknown)
        .map(|(chunk_idx, pack_ids)| async move {
            let _permit = CHUNK_PERMITS.acquire().await.expect("semaphore is never closed");
            let existing = load_existing(pack_index_cache, content_store, chunk_idx, &pack_ids).await?;
            let live = live_blocks_after(&existing, []);
            vm.write().set_live_blocks_if_unknown(chunk_idx, &pack_ids, live);
            Ok(())
        })
        .buffer_unordered(PER_CALL_CONCURRENCY)
        .collect()
        .await;
    results.into_iter().collect()
}

/// The export's exact stored bytes, computing any unknown chunk counts first.
/// `None` when an exact figure could not be produced: never a partial sum.
pub async fn stored_bytes_exact(
    vm: &Arc<parking_lot::RwLock<VolumeManifest>>,
    content_store: &ContentStore,
    pack_index_cache: &Arc<PackIndexCache>,
) -> Option<u64> {
    for _ in 0..ENSURE_ROUNDS {
        if let Some(bytes) = vm.read().stored_bytes() {
            return Some(bytes);
        }
        if let Err(e) = ensure_once(vm, content_store, pack_index_cache).await {
            warn!(error = %e, "stored bytes unavailable: a chunk's pack indexes could not be loaded");
            return None;
        }
    }
    vm.read().stored_bytes()
}

/// Background form, after export open: warm the counts so the first API read
/// does not pay for them.
pub async fn backfill_live_counts(
    vm: &Arc<parking_lot::RwLock<VolumeManifest>>,
    content_store: &ContentStore,
    pack_index_cache: &Arc<PackIndexCache>,
) {
    if !needs_backfill(vm) {
        return;
    }
    if stored_bytes_exact(vm, content_store, pack_index_cache).await.is_some() {
        info!("live-block counts backfilled");
    }
}
