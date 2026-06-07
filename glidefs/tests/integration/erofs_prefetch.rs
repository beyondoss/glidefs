//! Cold-start serving-side data prefetch: a forked export whose base manifest
//! carries a `prefetch_len` hint must warm the boot working set into the clean
//! cache on device open, so the guest's first reads of that region are cache
//! hits — NOT S3 round-trips. This is the production VM-boot path (exports are
//! forked from a blessed base manifest).
//!
//! The test measures it both ways, A/B, through the REAL router + cold S3 +
//! real cache code (not a model): fork one VM from a base WITH the hint and one
//! from an identical base WITHOUT it, and count the S3 GETs each pays on the
//! read critical path.

use std::sync::Arc;
use std::time::Duration;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::router::{ExportRouter, RouterConfig};
use glidefs::config::ExportConfig;
use object_store::memory::InMemory;
use tempfile::TempDir;

use super::BLOCK_SIZE;

fn router_config(
    s3: &Arc<dyn object_store::ObjectStore>,
    dir: &TempDir,
) -> RouterConfig {
    RouterConfig {
        object_store: Arc::clone(s3),
        db_path: "test".to_string(),
        cache_dir: dir.path().to_path_buf(),
        block_size: BLOCK_SIZE,
        clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
        pack_index_cache: None,
        wal_sync: false,
        max_s3_uploads: 0,
        max_s3_downloads: 0,
        default_flush_threshold: 500,
        ublk_nr_queues: 1,
        nbd_dead_conn_timeout: 0,
        max_exports: 10_000,
        manifest_cache_bytes: glidefs::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
    }
}

/// A source and its fork must share an `s3_prefix` so they resolve to the same
/// content-store path (otherwise the fork can't find the base manifest/packs).
fn export_config(name: &str, s3_prefix: &str) -> ExportConfig {
    ExportConfig {
        name: name.to_string(),
        size_gb: 0.05, // 50 MiB device
        s3_prefix: Some(s3_prefix.to_string()),
        block_size: None,
        flush_threshold: None,
        flush_mode: None,
        transport: None,
        compaction_cooldown: None,
    }
}

/// Distinct, non-zero content per (salt, block) so nothing is dropped as a zero
/// block AND the two bases don't share content (the router's clean cache is
/// content-addressed and shared across forks — identical content would let one
/// fork's reads warm the other's working set and mask the effect under test).
fn block_data(salt: u64, i: usize) -> Vec<u8> {
    let mut v = vec![0u8; BLOCK_SIZE];
    for (j, x) in v.iter_mut().enumerate() {
        *x = ((((i as u64 * 31 + j as u64) ^ salt.wrapping_mul(0x9E37)) % 251) + 1) as u8;
    }
    v
}

#[tokio::test]
async fn cold_start_data_prefetch_eliminates_critical_path_s3_gets() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let ws_blocks = 8usize; // 8 × 128 KiB = 1 MiB boot working set
    let ws_len = (ws_blocks * BLOCK_SIZE) as u64;

    // --- Phase 1: build two identical bases, one WITH a prefetch hint, and
    // snapshot each under a tag so it can be forked. ---
    let dir1 = TempDir::new().unwrap();
    let writer = Arc::new(ExportRouter::new(router_config(&s3, &dir1)).await.unwrap());
    for (src, tag, prefix, salt, hint) in [
        ("src-prio", "base-prio", "img-prio", 1u64, true),
        ("src-plain", "base-plain", "img-plain", 2u64, false),
    ] {
        writer.create_export(export_config(src, prefix), false, None, None).await.unwrap();
        let h = writer.get_handler(src).await.unwrap();
        for i in 0..ws_blocks {
            h.write((i * BLOCK_SIZE) as u64, &block_data(salt, i), false).await.unwrap();
        }
        if hint {
            // The connector: record the boot working-set extent so the manifest
            // persists it (v6). In production the bless/EROFS-build path sets this
            // from the writer's reported prefetch_len.
            h.set_prefetch_len(ws_len);
        }
        // Snapshot under `tag` → persists packs + the (v6) manifest at that name.
        writer.snapshot_export(src, Some(tag)).await.unwrap();
    }
    drop(writer);

    // --- Phase 2: cold host. Fork a VM from each base. ---
    let dir2 = TempDir::new().unwrap();
    let cold = Arc::new(ExportRouter::new(router_config(&s3, &dir2)).await.unwrap());

    // Fork WITHOUT the hint = baseline: first reads pay S3 on the critical path.
    cold.create_export(export_config("vm-plain", "img-plain"), true, Some("base-plain"), None)
        .await
        .unwrap();
    let plain = cold.get_handler("vm-plain").await.unwrap();
    assert_eq!(plain.prefetch_len(), None, "plain base carries no hint");

    let s3_before = plain.metrics().snapshot().s3_read_ops;
    for i in 0..ws_blocks {
        let got = plain.read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32).await.unwrap();
        assert_eq!(&got[..], &block_data(2, i)[..], "plain content must be correct");
    }
    let baseline_gets = plain.metrics().snapshot().s3_read_ops - s3_before;
    assert!(
        baseline_gets > 0,
        "baseline cold reads must hit S3 (got {baseline_gets})"
    );

    // Fork WITH the hint = device-open prefetch warms the boot set in background.
    cold.create_export(export_config("vm-prio", "img-prio"), true, Some("base-prio"), None)
        .await
        .unwrap();
    let prio = cold.get_handler("vm-prio").await.unwrap();
    assert_eq!(
        prio.prefetch_len(),
        Some(ws_len),
        "prio base's prefetch_len must round-trip through S3 persist + fork load"
    );

    // Wait for the background prefetch to do its S3 work and plateau.
    let mut last = u64::MAX;
    let mut stable = 0;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = prio.metrics().snapshot().s3_read_ops;
        if now == last {
            stable += 1;
            if stable >= 4 && now > 0 {
                break;
            }
        } else {
            stable = 0;
            last = now;
        }
    }
    let background_gets = prio.metrics().snapshot().s3_read_ops;
    assert!(
        background_gets > 0,
        "device-open prefetch must have fetched the boot set from S3 in the background"
    );

    // The guest's first reads of the boot set: must add ZERO S3 GETs (warm) and
    // return correct content.
    let s3_before_reads = prio.metrics().snapshot().s3_read_ops;
    for i in 0..ws_blocks {
        let got = prio.read((i * BLOCK_SIZE) as u64, BLOCK_SIZE as u32).await.unwrap();
        assert_eq!(&got[..], &block_data(1, i)[..], "prio content must be correct");
    }
    let critical_path_gets = prio.metrics().snapshot().s3_read_ops - s3_before_reads;

    eprintln!(
        "cold-start boot working set ({} blocks / {} KiB):\n  \
         BASELINE (no prefetch): {} S3 GETs ON the guest read critical path\n  \
         PREFETCH (warm-on-open): {} S3 GETs on critical path ({} fetched in background, off critical path)",
        ws_blocks,
        ws_len / 1024,
        baseline_gets,
        critical_path_gets,
        background_gets,
    );

    assert_eq!(
        critical_path_gets, 0,
        "with warm-on-open, the guest's boot reads must not touch S3 (got {critical_path_gets})"
    );
}

/// Universal (format-agnostic) trace-driven prefetch: a base with a published
/// boot SET (a bounded, scattered block list — what a read trace produces for
/// ANY image, ext4 or EROFS) must, on cold fork-open, warm exactly those blocks
/// into the clean cache — and NOTHING else. This proves both halves: the boot
/// set is served warm (0 critical-path GETs), and it stays BOUNDED (a non-boot
/// block is still cold → lazy loading is preserved, not "download everything").
#[tokio::test]
async fn cold_start_boot_set_prefetch_is_warm_and_bounded() {
    use glidefs::block::content_store::ContentStore;
    use glidefs::block::manifest::serialize_block_list;

    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let total_blocks = 64usize; // 8 MiB image
    // A scattered boot set — a real trace touches a small, non-contiguous subset.
    let boot_blocks: Vec<u64> = vec![2, 3, 4, 17, 40, 41];
    let cold_block = 30u64; // deliberately NOT in the boot set

    // --- Phase 1: write the full image, publish the bounded boot set. ---
    let dir1 = TempDir::new().unwrap();
    let writer = Arc::new(ExportRouter::new(router_config(&s3, &dir1)).await.unwrap());
    writer.create_export(export_config("src", "img"), false, None, None).await.unwrap();
    let h = writer.get_handler("src").await.unwrap();
    for i in 0..total_blocks {
        h.write((i * BLOCK_SIZE) as u64, &block_data(7, i), false).await.unwrap();
    }
    writer.snapshot_export("src", Some("base")).await.unwrap();
    // Publish the boot set under the base name (same path the fork reads from).
    let base_cs = ContentStore::new(Arc::clone(&s3), "test/exports/img");
    base_cs.put_boot_set("base", serialize_block_list(&boot_blocks)).await.unwrap();
    drop(writer);

    // --- Phase 2: cold fork. Device-open data-prefetches the boot set. ---
    let dir2 = TempDir::new().unwrap();
    let cold = Arc::new(ExportRouter::new(router_config(&s3, &dir2)).await.unwrap());
    cold.create_export(export_config("vm", "img"), true, Some("base"), None).await.unwrap();
    let vm = cold.get_handler("vm").await.unwrap();

    // Wait for the background prefetch to fetch + plateau.
    let mut last = u64::MAX;
    let mut stable = 0;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = vm.metrics().snapshot().s3_read_ops;
        if now == last { stable += 1; if stable >= 4 && now > 0 { break; } } else { stable = 0; last = now; }
    }
    let background_gets = vm.metrics().snapshot().s3_read_ops;
    assert!(background_gets > 0, "boot-set prefetch must fetch from S3 in the background");
    // Bounded: it must fetch FAR fewer than the whole image (64 blocks). The boot
    // set is 6 blocks in 3 coalesced runs ([2,3,4],[17],[40,41]).
    assert!(
        background_gets < total_blocks as u64 / 2,
        "boot-set prefetch must stay bounded, got {background_gets} GETs for {total_blocks} blocks"
    );

    // The guest's reads of the boot set: warm (0 new S3) + correct.
    let before = vm.metrics().snapshot().s3_read_ops;
    for &b in &boot_blocks {
        let got = vm.read(b * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        assert_eq!(&got[..], &block_data(7, b as usize)[..], "boot block {b} content");
    }
    let warm_gets = vm.metrics().snapshot().s3_read_ops - before;
    assert_eq!(warm_gets, 0, "boot-set reads must be warm (0 S3), got {warm_gets}");

    // A NON-boot block must STILL be cold (proves we didn't pull the whole image).
    let before_cold = vm.metrics().snapshot().s3_read_ops;
    let _ = vm.read(cold_block * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
    let cold_gets = vm.metrics().snapshot().s3_read_ops - before_cold;
    assert!(
        cold_gets > 0,
        "a block outside the boot set must remain cold — lazy loading preserved"
    );

    eprintln!(
        "boot-set prefetch: {} boot blocks warmed via {} background GETs; non-boot block still cold ({} GET)",
        boot_blocks.len(), background_gets, cold_gets
    );
}

/// WALL-CLOCK: inject a fixed latency per S3 GET (ThrottledStore) so the
/// "GETs eliminated" turns into milliseconds. Time the guest's boot-set reads
/// from a cold fork of a base WITHOUT a boot set (pays the latency per coalesced
/// run, serially, on the critical path) vs a base WITH one prefetched on open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_start_wall_clock_under_injected_s3_latency() {
    use glidefs::block::content_store::ContentStore;
    use glidefs::block::manifest::serialize_block_list;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use std::time::Instant;

    const GET_LATENCY: Duration = Duration::from_millis(40); // plausible S3 RTT
    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    // Reads (forks) go through the throttle; writes (bless) use `inner` directly.
    let s3: Arc<dyn object_store::ObjectStore> =
        Arc::new(ThrottledStore::new(Arc::clone(&inner), ThrottleConfig {
            wait_get_per_call: GET_LATENCY,
            ..Default::default()
        }));

    let total_blocks = 48usize;
    let boot_blocks: Vec<u64> = vec![1, 2, 3, 10, 11, 20, 33, 34, 35];

    // Bless two identical bases under one prefix: `with-set` gets a boot set,
    // `no-set` doesn't. (Same content is fine — they're separate snapshots and we
    // measure each on its own cold fork.)
    let dir1 = TempDir::new().unwrap();
    let writer = Arc::new(ExportRouter::new(router_config(&inner, &dir1)).await.unwrap());
    writer.create_export(export_config("src", "img"), false, None, None).await.unwrap();
    let h = writer.get_handler("src").await.unwrap();
    for i in 0..total_blocks {
        h.write((i * BLOCK_SIZE) as u64, &block_data(9, i), false).await.unwrap();
    }
    writer.snapshot_export("src", Some("with-set")).await.unwrap();
    writer.snapshot_export("src", Some("no-set")).await.unwrap();
    ContentStore::new(Arc::clone(&inner), "test/exports/img")
        .put_boot_set("with-set", serialize_block_list(&boot_blocks))
        .await
        .unwrap();
    drop(writer);

    // Fork `base` cold (reads throttled), optionally wait for the open-time
    // prefetch to warm, then time the boot-set reads.
    async fn fork_and_time(
        s3: &Arc<dyn object_store::ObjectStore>,
        base: &str,
        wait_warm: bool,
        boot_blocks: &[u64],
    ) -> Duration {
        let dir = TempDir::new().unwrap();
        let cold = Arc::new(ExportRouter::new(router_config(s3, &dir)).await.unwrap());
        cold.create_export(export_config("vm", "img"), true, Some(base), None).await.unwrap();
        let vm = cold.get_handler("vm").await.unwrap();
        if wait_warm {
            let mut last = u64::MAX;
            let mut stable = 0;
            for _ in 0..600 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let now = vm.metrics().snapshot().s3_read_ops;
                if now == last { stable += 1; if stable >= 4 && now > 0 { break; } } else { stable = 0; last = now; }
            }
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await; // let open settle
        }
        let t = Instant::now();
        for &b in boot_blocks {
            vm.read(b * BLOCK_SIZE as u64, BLOCK_SIZE as u32).await.unwrap();
        }
        t.elapsed()
    }

    let warm = fork_and_time(&s3, "with-set", true, &boot_blocks).await;
    let baseline = fork_and_time(&s3, "no-set", false, &boot_blocks).await;

    eprintln!(
        "wall-clock boot-set reads @ {}ms/GET: BASELINE (no prefetch) {:?}  vs  WARM-ON-OPEN {:?}  ({:.0}x faster)",
        GET_LATENCY.as_millis(), baseline, warm,
        baseline.as_secs_f64() / warm.as_secs_f64().max(1e-6),
    );
    assert!(
        warm * 3 < baseline,
        "warm-on-open ({warm:?}) must be far faster than cold baseline ({baseline:?})"
    );
}
