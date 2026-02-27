//! Background block integrity scrubber.
//!
//! Periodically verifies cached blocks by re-hashing their contents. On hash
//! mismatch (bit rot, memory corruption), evicts the block from the clean cache.
//! S3 is the authoritative source of truth; the next read will re-fetch.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::block_map::{Blake3Hash, blake3_128};
use super::cache::BlockCache;
use super::router::ExportRouter;

/// Configuration for the background scrubber.
pub struct ScrubberConfig {
    /// Maximum number of blocks to verify per second. 0 = disabled.
    pub blocks_per_second: u64,
}

/// Counters for scrubber observability via Prometheus.
pub struct ScrubberMetrics {
    /// Total blocks checked across all passes.
    pub blocks_checked: AtomicU64,
    /// Total corrupted blocks evicted across all passes.
    pub blocks_evicted: AtomicU64,
}

impl Default for ScrubberMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ScrubberMetrics {
    pub fn new() -> Self {
        Self {
            blocks_checked: AtomicU64::new(0),
            blocks_evicted: AtomicU64::new(0),
        }
    }
}

/// Trait for providing block hashes to the scrubber.
///
/// Implementations collect known block hashes from whatever metadata source
/// is active (e.g. VolumeManifest + ChunkMeta in the chunked architecture).
#[async_trait]
pub trait HashSource: Send + Sync {
    async fn all_hashes(&self) -> Vec<Blake3Hash>;
}

/// Hash source backed by the export router.
///
/// Walks each active export's VolumeManifest and ChunkMetaCache to collect
/// all known block hashes. The scrubber then verifies any of these hashes
/// that happen to be present in the clean cache.
pub struct RouterHashSource {
    router: Arc<ExportRouter>,
}

impl RouterHashSource {
    pub fn new(router: Arc<ExportRouter>) -> Self {
        Self { router }
    }
}

#[async_trait]
impl HashSource for RouterHashSource {
    async fn all_hashes(&self) -> Vec<Blake3Hash> {
        self.router.collect_block_hashes().await
    }
}

/// Run the background scrubber until cancelled.
///
/// Iterates all known hashes from the hash source, checks if each is in the
/// clean cache, and verifies its integrity by re-hashing. Corrupted blocks
/// are evicted (S3 will re-supply on next read).
pub async fn scrubber(
    clean_cache: Arc<dyn BlockCache>,
    hash_source: Arc<dyn HashSource>,
    config: ScrubberConfig,
    metrics: Arc<ScrubberMetrics>,
    shutdown: CancellationToken,
) {
    if config.blocks_per_second == 0 {
        return;
    }

    let interval = Duration::from_micros(1_000_000 / config.blocks_per_second);

    loop {
        let hashes = hash_source.all_hashes().await;
        let mut ticker = tokio::time::interval(interval);
        let mut checked = 0u64;
        let mut evicted = 0u64;

        for hash in &hashes {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if let Some(data) = clean_cache.get(hash).await {
                        let actual = blake3_128(&data);
                        if actual != *hash {
                            warn!(expected = ?hash, actual = ?actual, "scrubber: hash mismatch, evicting");
                            clean_cache.remove(hash);
                            evicted += 1;
                        }
                        checked += 1;
                    }
                }
            }
        }

        // Update cumulative counters
        metrics.blocks_checked.fetch_add(checked, Ordering::Relaxed);
        metrics.blocks_evicted.fetch_add(evicted, Ordering::Relaxed);

        if checked > 0 {
            tracing::info!(checked, evicted, "scrubber pass complete");
        }

        // Sleep between full passes
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(60)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::block_map::blake3_128;
    use crate::block::cache::SimpleBlockCache;
    use bytes::Bytes;

    /// Test hash source that returns a fixed set of hashes.
    struct StaticHashes(Vec<Blake3Hash>);
    #[async_trait]
    impl HashSource for StaticHashes {
        async fn all_hashes(&self) -> Vec<Blake3Hash> {
            self.0.clone()
        }
    }

    #[tokio::test]
    async fn test_scrubber_detects_corruption() {
        let cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        // Insert a valid block
        let data = Bytes::from(vec![0xAA; 4096]);
        let correct_hash = blake3_128(&data);
        cache.insert(correct_hash, data);

        let hash_source: Arc<dyn HashSource> = Arc::new(StaticHashes(vec![correct_hash]));

        // Now corrupt it: remove the correct entry and insert wrong data under the same key
        cache.remove(&correct_hash);
        let corrupted = Bytes::from(vec![0xFF; 4096]);
        cache.insert(correct_hash, corrupted);

        // Run scrubber briefly
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        let cache_clone = Arc::clone(&cache);
        let hs_clone = Arc::clone(&hash_source);

        let metrics = Arc::new(ScrubberMetrics::new());
        let metrics_clone = Arc::clone(&metrics);
        let handle = tokio::spawn(async move {
            scrubber(
                cache_clone,
                hs_clone,
                ScrubberConfig {
                    blocks_per_second: 10_000,
                },
                metrics_clone,
                shutdown_clone,
            )
            .await;
        });

        // Give the scrubber time to process
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown.cancel();
        handle.await.unwrap();

        // The corrupted block should have been evicted
        assert!(
            cache.get(&correct_hash).await.is_none(),
            "corrupted block should be evicted"
        );

        // Metrics should reflect the work done
        assert!(
            metrics.blocks_checked.load(Ordering::Relaxed) >= 1,
            "should have checked at least 1 block"
        );
        assert!(
            metrics.blocks_evicted.load(Ordering::Relaxed) >= 1,
            "should have evicted at least 1 corrupted block"
        );
    }

    #[tokio::test]
    async fn test_scrubber_leaves_valid_blocks() {
        let cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        // Insert valid blocks
        let data1 = Bytes::from(vec![0xAA; 4096]);
        let hash1 = blake3_128(&data1);
        cache.insert(hash1, data1.clone());

        let data2 = Bytes::from(vec![0xBB; 4096]);
        let hash2 = blake3_128(&data2);
        cache.insert(hash2, data2.clone());

        let hash_source: Arc<dyn HashSource> = Arc::new(StaticHashes(vec![hash1, hash2]));

        // Run scrubber briefly
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        let cache_clone = Arc::clone(&cache);
        let hs_clone = Arc::clone(&hash_source);

        let metrics = Arc::new(ScrubberMetrics::new());
        let metrics_clone = Arc::clone(&metrics);
        let handle = tokio::spawn(async move {
            scrubber(
                cache_clone,
                hs_clone,
                ScrubberConfig {
                    blocks_per_second: 10_000,
                },
                metrics_clone,
                shutdown_clone,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown.cancel();
        handle.await.unwrap();

        // Both valid blocks should still be cached
        assert!(
            cache.get(&hash1).await.is_some(),
            "valid block 1 should remain"
        );
        assert!(
            cache.get(&hash2).await.is_some(),
            "valid block 2 should remain"
        );

        // Metrics should show checks but no evictions
        assert!(
            metrics.blocks_checked.load(Ordering::Relaxed) >= 2,
            "should have checked at least 2 blocks"
        );
        assert_eq!(
            metrics.blocks_evicted.load(Ordering::Relaxed),
            0,
            "should not have evicted any valid blocks"
        );
    }

    #[tokio::test]
    async fn test_scrubber_shutdown_during_inter_pass_sleep() {
        let cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));

        // Insert a single valid block so the scrubber completes one pass quickly
        let data = Bytes::from(vec![0xCC; 4096]);
        let hash = blake3_128(&data);
        cache.insert(hash, data);

        let hash_source: Arc<dyn HashSource> = Arc::new(StaticHashes(vec![hash]));

        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        let metrics = Arc::new(ScrubberMetrics::new());
        let metrics_clone = Arc::clone(&metrics);

        let handle = tokio::spawn(async move {
            scrubber(
                cache,
                hash_source,
                ScrubberConfig {
                    blocks_per_second: 100_000, // Very fast — completes pass quickly
                },
                metrics_clone,
                shutdown_clone,
            )
            .await;
        });

        // Wait for at least one pass to complete (scrubber should enter 60s sleep)
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if metrics.blocks_checked.load(Ordering::Relaxed) >= 1 {
                break;
            }
        }
        assert!(
            metrics.blocks_checked.load(Ordering::Relaxed) >= 1,
            "scrubber should have completed at least one pass"
        );

        // Cancel during the 60s inter-pass sleep — should exit promptly
        shutdown.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            result.is_ok(),
            "scrubber should exit promptly when cancelled during inter-pass sleep"
        );
        result.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_scrubber_disabled_when_zero() {
        let cache = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
        let hash_source: Arc<dyn HashSource> = Arc::new(StaticHashes(vec![]));
        let shutdown = CancellationToken::new();

        // Should return immediately when blocks_per_second = 0
        let metrics = Arc::new(ScrubberMetrics::new());
        scrubber(
            cache,
            hash_source,
            ScrubberConfig {
                blocks_per_second: 0,
            },
            metrics,
            shutdown,
        )
        .await;
    }
}
