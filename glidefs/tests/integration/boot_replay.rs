//! Boot-set cold-replay proof harness (Phase 1).
//!
//! Answers the central question of the boot-set project: **can an explicit,
//! precomputed boot set beat the automatic 32 MiB pack-window readahead, and by
//! how much — as a function of how *scattered* the boot working set is in the
//! image?**
//!
//! It drives the REAL read path (`WriteCache::read` → `locate_block` →
//! `fetch_with_window` → S3) against a cold cache, with an object store that
//! injects a first-byte RTT + throughput latency model and counts every GET and
//! every byte. For each "arm" it replays an access trace and measures boot
//! wall-clock, S3 GET count, and bytes fetched.
//!
//! ## Arms (identical read path; only the prefetch policy / layout differ)
//! - **DemandOnly**  — readahead window = 0; fetch exactly the requested blocks
//!   (coalesced if adjacent). The cold floor.
//! - **Readahead**   — the production 32 MiB pack-window. The baseline to beat.
//! - **BootSet**     — the boot working set is reordered into a contiguous
//!   leading run (a *different*, reordered image) and warmed in one prefix read
//!   at device-open (t=0); the trace then hits cache. The treatment.
//! - **Warm**        — cache fully pre-populated. The lower bound (the
//!   cold↔warm gap we're trying to close).
//!
//! ## Why two images
//! Out of the box the boot set is laid out in directory/DFS order and is
//! therefore *scattered* across packs — the baseline pays ~1 RTT per pack it
//! touches and over-fetches the gaps. The BootSet arm models the build-time
//! `PriorityOrder` reorder that *manufactures* contiguity, so the scattered set
//! becomes one tight front run. The three status-quo arms run on the scattered
//! image; BootSet runs on the reordered one. The delta between them is the value
//! of the reorder.
//!
//! ## Running
//! - `cargo test --features test-utils --test integration boot_replay` runs the
//!   fast smoke test (a few trials; asserts the harness invariants).
//! - `GLIDEFS_BOOT_REPLAY_FULL=1 cargo test --features test-utils --test \
//!   integration boot_replay::full_study -- --ignored --nocapture` runs the
//!   statistical study (RTT sweep × scatter sweep, ≥30 trials/arm, Mann-Whitney
//!   U + Hodges-Lehmann CI) and prints the report.

#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tempfile::TempDir;

use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::content_store::ContentStore;
use glidefs::block::metrics::ExportMetrics;
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::{WriteCache, WriteCacheConfig};

const BLOCK: usize = 128 * 1024; // production block size

// ============================================================================
// LatencyStore — first-byte RTT + throughput, counts GETs and bytes served
// ============================================================================

/// Wraps an in-memory store and injects a realistic S3 latency model on GETs:
/// `rtt` (first-byte) + `len / throughput` (transfer). Counts GETs and bytes so
/// a trial can report exactly how many round-trips and how much over-fetch a
/// prefetch policy cost. PUTs are passed through untimed (image build is not
/// part of the measured boot).
#[derive(Debug)]
struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    rtt: Duration,
    /// Bytes per second for the transfer component; `0` = infinite (RTT only).
    throughput: u64,
    get_count: AtomicU64,
    get_bytes: AtomicU64,
    timed: std::sync::atomic::AtomicBool,
}

impl LatencyStore {
    fn new(inner: Arc<dyn ObjectStore>, rtt: Duration, throughput: u64) -> Self {
        Self {
            inner,
            rtt,
            throughput,
            get_count: AtomicU64::new(0),
            get_bytes: AtomicU64::new(0),
            timed: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Reset the counters at the start of a measured replay.
    fn reset(&self) {
        self.get_count.store(0, Ordering::SeqCst);
        self.get_bytes.store(0, Ordering::SeqCst);
    }

    /// Disable latency injection (used during untimed cache pre-warm so the
    /// warm's transfer time isn't double-counted against wall-clock — its GETs
    /// and bytes are still counted as the boot set's fetch cost).
    fn set_timed(&self, on: bool) {
        self.timed.store(on, Ordering::SeqCst);
    }

    fn gets(&self) -> u64 {
        self.get_count.load(Ordering::SeqCst)
    }
    fn bytes(&self) -> u64 {
        self.get_bytes.load(Ordering::SeqCst)
    }

    async fn delay_for(&self, len: u64) {
        if !self.timed.load(Ordering::SeqCst) {
            return;
        }
        let mut d = self.rtt;
        if self.throughput > 0 {
            d += Duration::from_secs_f64(len as f64 / self.throughput as f64);
        }
        if !d.is_zero() {
            tokio::time::sleep(d).await;
        }
    }
}

impl std::fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LatencyStore")
    }
}

#[async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let result = self.inner.get_opts(location, options).await?;
        // Count the bytes actually transferred (the returned RANGE), not the full
        // object size — range GETs over a pack transfer only the requested window.
        let len = result.range.end.saturating_sub(result.range.start);
        self.get_count.fetch_add(1, Ordering::SeqCst);
        self.get_bytes.fetch_add(len, Ordering::SeqCst);
        self.delay_for(len).await;
        Ok(result)
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// ============================================================================
// Image / trace model
// ============================================================================

/// A logical boot working set + filler, realized into two physical layouts.
///
/// `device_blocks` is the device size in blocks. The boot set is
/// `boot_count` blocks; `scatter_packs` controls how far apart, in device
/// space, consecutive boot blocks are placed in the SCATTERED layout (more
/// spread → more packs touched → more RTTs for the readahead baseline).
struct ImageSpec {
    device_blocks: u64,
    boot_count: u64,
    /// Stride (in blocks) between consecutive scattered boot blocks.
    scatter_stride: u64,
}

impl ImageSpec {
    /// Physical block index of boot item `i` in the SCATTERED (status-quo)
    /// layout: spread across the device so the set spans many packs.
    fn scattered_block(&self, i: u64) -> u64 {
        (i * self.scatter_stride) % self.device_blocks
    }

    /// Physical block index of boot item `i` in the REORDERED layout: a tight
    /// contiguous front run in access order.
    fn reordered_block(&self, i: u64) -> u64 {
        i
    }

    /// The access trace (first-touch order) over the SCATTERED layout.
    fn scattered_trace(&self) -> Vec<u64> {
        (0..self.boot_count).map(|i| self.scattered_block(i)).collect()
    }

    /// The access trace over the REORDERED layout.
    fn reordered_trace(&self) -> Vec<u64> {
        (0..self.boot_count).map(|i| self.reordered_block(i)).collect()
    }
}

/// Deterministic, unique, non-zero block content (so blocks neither dedup to
/// each other nor read as sparse zeros).
fn block_data(block_idx: u64) -> Vec<u8> {
    let mut v = vec![0u8; BLOCK];
    let seed = block_idx.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    let mut x = seed;
    for chunk in v.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let b = x.to_le_bytes();
        chunk.copy_from_slice(&b[..chunk.len()]);
    }
    v
}

/// Build one image in `s3` under `name`: write `blocks` (each unique/non-zero),
/// flush to S3 (uploads packs + manifest). Returns nothing; the manifest is now
/// fetchable from `s3`.
async fn build_image(s3: Arc<InMemory>, name: &str, device_blocks: u64, blocks: &[u64]) {
    let dir = TempDir::new().unwrap();
    let device_size = device_blocks * BLOCK as u64;
    let cfg = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: BLOCK,
        wal_sync: false,
    };
    let cache = WriteCache::open_fresh_active(cfg).expect("open fresh cache");
    let content_store = ContentStore::new(s3 as Arc<dyn ObjectStore>, "test");
    let pic_dir = TempDir::new().unwrap();
    let pic = Arc::new(PackIndexCache::open(pic_dir.path()).await.unwrap());
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(
        device_size,
        BLOCK as u32,
    )));

    for &b in blocks {
        cache.write(b * BLOCK as u64, &block_data(b)).unwrap();
    }
    // Drain fully so every pack + the manifest are in S3.
    for _ in 0..100 {
        let stats = cache.flush_to_s3(&content_store, &pic, &vm).await.unwrap();
        if stats.blocks_claimed == 0 {
            break;
        }
    }
    // Keep the temp dir alive until flush completes.
    drop(dir);
    drop(pic_dir);
}

// ============================================================================
// Arms + trial runner
// ============================================================================

/// Populate the shared `pic` with `name`'s pack indices by reading every boot
/// block once through an untimed cold reader. Models the production
/// index-warm-on-device-open: after this, every arm sees a warm pack-index cache
/// so trials isolate the DATA fetch cost rather than re-measuring index GETs.
async fn prewarm_index(
    s3: &Arc<InMemory>,
    name: &str,
    device_blocks: u64,
    blocks: &[u64],
    pic: &Arc<PackIndexCache>,
) {
    let content_store = ContentStore::new(Arc::clone(s3) as Arc<dyn ObjectStore>, "test");
    let dir = TempDir::new().unwrap();
    let device_size = device_blocks * BLOCK as u64;
    let (data, _etag) = content_store.get_manifest(name).await.unwrap().expect("manifest");
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::deserialize(&data).unwrap()));
    let cfg = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: BLOCK,
        wal_sync: false,
    };
    let cache = WriteCache::open_fresh_active(cfg).expect("open fresh reader");
    let clean: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(256 * 1024 * 1024));
    let metrics = ExportMetrics::new();
    for &b in blocks {
        cache
            .read(b * BLOCK as u64, BLOCK, clean.as_ref(), pic, &vm, &content_store, &metrics)
            .await
            .unwrap();
    }
    drop(dir);
}

/// Boot-set scatter characterization — the headline diagnostic. Maps the boot
/// blocks onto the actual pack layout in `name`'s manifest and reports how many
/// distinct chunks/packs the set spans. The readahead baseline pays ~one RTT per
/// distinct pack, so `distinct_packs` largely PREDICTS its GET count (and thus
/// where a reorder helps: many packs → big win; one pack → no win).
async fn scatter_stats(s3: &Arc<InMemory>, name: &str, blocks: &[u64]) -> (usize, usize) {
    let content_store = ContentStore::new(Arc::clone(s3) as Arc<dyn ObjectStore>, "test");
    let (data, _etag) = content_store.get_manifest(name).await.unwrap().expect("manifest");
    let vm = VolumeManifest::deserialize(&data).unwrap();
    let mut chunks = std::collections::HashSet::new();
    let mut packs = std::collections::HashSet::new();
    for &b in blocks {
        let ci = vm.chunk_idx_for_block(b);
        chunks.insert(ci);
        if let Some(pids) = vm.chunk_pack_ids(ci) {
            for p in pids {
                packs.insert(*p);
            }
        }
    }
    (chunks.len(), packs.len())
}

/// Read the real decompressed content of `blocks` from a real blessed image
/// (cold, through the read path). Used to build a faithful REORDERED image whose
/// boot-set arm is measured (not modeled), with the real bytes/compression.
async fn read_blocks_content(
    store: Arc<dyn ObjectStore>,
    base: &str,
    name: &str,
    manifest_key: &str,
    device_size: u64,
    block_size: u32,
    blocks: &[u64],
    pic: &Arc<PackIndexCache>,
) -> Vec<Vec<u8>> {
    let content_store = ContentStore::new(store, base);
    let (data, _e) = content_store.get_manifest(manifest_key).await.unwrap().expect("manifest");
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::deserialize(&data).unwrap()));
    let dir = TempDir::new().unwrap();
    let cache = WriteCache::open_fresh_active(WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: block_size as usize,
        wal_sync: false,
    })
    .unwrap();
    let clean: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(512 * 1024 * 1024));
    let metrics = ExportMetrics::new();
    let mut out = Vec::with_capacity(blocks.len());
    for &b in blocks {
        let bytes = cache
            .read(b * block_size as u64, block_size as usize, clean.as_ref(), pic, &vm, &content_store, &metrics)
            .await
            .unwrap();
        out.push(bytes.to_vec());
    }
    drop(dir);
    out
}

/// Build an InMemory image whose blocks 0..N hold the given real contents
/// (the REORDERED boot set as one contiguous front run). Flushed with real
/// compression so the boot-set arm's bytes are realistic.
async fn build_image_from_contents(s3: Arc<InMemory>, name: &str, contents: &[Vec<u8>]) {
    let dir = TempDir::new().unwrap();
    let device_blocks = contents.len().max(1) as u64;
    let device_size = device_blocks * BLOCK as u64;
    let cfg = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: BLOCK,
        wal_sync: false,
    };
    let cache = WriteCache::open_fresh_active(cfg).expect("open fresh cache");
    let content_store = ContentStore::new(s3 as Arc<dyn ObjectStore>, "test");
    let pic_dir = TempDir::new().unwrap();
    let pic = Arc::new(PackIndexCache::open(pic_dir.path()).await.unwrap());
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::new(device_size, BLOCK as u32)));
    for (i, c) in contents.iter().enumerate() {
        let mut buf = c.clone();
        buf.resize(BLOCK, 0);
        cache.write(i as u64 * BLOCK as u64, &buf).unwrap();
    }
    for _ in 0..100 {
        let stats = cache.flush_to_s3(&content_store, &pic, &vm).await.unwrap();
        if stats.blocks_claimed == 0 {
            break;
        }
    }
    drop(dir);
    drop(pic_dir);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    DemandOnly,
    Readahead,
    BootSet,
    Warm,
}

impl Arm {
    fn label(self) -> &'static str {
        match self {
            Arm::DemandOnly => "demand-only",
            Arm::Readahead => "readahead(32M)",
            Arm::BootSet => "boot-set",
            Arm::Warm => "warm",
        }
    }
}

struct TrialResult {
    /// Wall-clock of the measured boot replay (prefetch warm is untimed).
    wall: Duration,
    /// GETs/bytes issued by the device-open prefetch (boot-set / warm arms).
    prefetch_gets: u64,
    prefetch_bytes: u64,
    /// GETs/bytes issued on-demand DURING the replay (cache misses).
    demand_gets: u64,
    demand_bytes: u64,
}

impl TrialResult {
    /// Total fetch cost = prefetch + on-demand.
    fn total_gets(&self) -> u64 {
        self.prefetch_gets + self.demand_gets
    }
    fn total_bytes(&self) -> u64 {
        self.prefetch_bytes + self.demand_bytes
    }
}

/// Run one cold trial of `arm` against the image `name` already in `s3`,
/// replaying `trace` (physical block indices, in access order). A fresh local
/// cache + clean cache is created per trial; `pic` is shared (models the
/// index-warm-on-open baseline — every arm sees the same warm-index condition,
/// so the comparison isolates the DATA fetch cost).
#[allow(clippy::too_many_arguments)]
async fn run_trial(
    s3: &Arc<InMemory>,
    name: &str,
    device_blocks: u64,
    trace: &[u64],
    boot_set: &[u64],
    arm: Arm,
    pic: &Arc<PackIndexCache>,
    rtt: Duration,
    throughput: u64,
) -> TrialResult {
    let latency = Arc::new(LatencyStore::new(Arc::clone(s3) as Arc<dyn ObjectStore>, rtt, throughput));
    let content_store = ContentStore::new(Arc::clone(&latency) as Arc<dyn ObjectStore>, "test");

    // Fresh cold reader: empty local cache, manifest from S3.
    let dir = TempDir::new().unwrap();
    let device_size = device_blocks * BLOCK as u64;
    let (data, _etag) = content_store
        .get_manifest(name)
        .await
        .unwrap()
        .expect("manifest in S3");
    let vm = Arc::new(parking_lot::RwLock::new(
        VolumeManifest::deserialize(&data).unwrap(),
    ));
    let cfg = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: BLOCK,
        wal_sync: false,
    };
    let cache = WriteCache::open_fresh_active(cfg).expect("open fresh reader");
    let clean: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(256 * 1024 * 1024));
    let metrics = ExportMetrics::new();

    // Configure the readahead window per arm.
    cache.set_readahead_window_bytes(match arm {
        Arm::DemandOnly => 0,
        // Big enough to cover any single pack window for the contiguous prefix.
        Arm::Readahead | Arm::BootSet | Arm::Warm => 32 * 1024 * 1024,
    });

    let read_block = |b: u64| {
        let cache = &cache;
        let clean = clean.as_ref();
        let vm = &vm;
        let cs = &content_store;
        let metrics = &metrics;
        let pic = pic;
        async move {
            cache
                .read(b * BLOCK as u64, BLOCK, clean, pic, vm, cs, metrics)
                .await
                .unwrap();
        }
    };

    // Pre-warm phase. The boot-set's device-open `[0, prefetch_len)` warm is ON
    // the boot critical path (the guest needs those bytes to boot), so it is
    // TIMED and its duration counts toward boot wall-clock. The Warm arm is the
    // artificial lower bound — its pre-load is untimed and excluded (it models
    // "everything already cached"). Both arms' prefetch GETs/bytes are recorded
    // as the policy's prefetch cost.
    let prefetch_is_on_critical_path = matches!(arm, Arm::BootSet);
    latency.reset();
    latency.set_timed(prefetch_is_on_critical_path);
    let warm_start = Instant::now();
    match arm {
        Arm::BootSet => {
            // Contiguous boot run → the 32 MiB window coalesces it into ~1 GET.
            for &b in boot_set {
                read_block(b).await;
            }
        }
        Arm::Warm => {
            for &b in trace {
                read_block(b).await;
            }
        }
        _ => {}
    }
    let warm_dur = warm_start.elapsed();
    latency.set_timed(true);
    let prefetch_gets = latency.gets();
    let prefetch_bytes = latency.bytes();

    // Measured replay — on-demand misses (0 for BootSet/Warm once warmed).
    latency.reset();
    let replay_start = Instant::now();
    for &b in trace {
        read_block(b).await;
    }
    let replay_dur = replay_start.elapsed();
    let demand_gets = latency.gets();
    let demand_bytes = latency.bytes();

    // Boot wall-clock = critical-path prefetch + on-demand replay.
    let wall = if prefetch_is_on_critical_path {
        warm_dur + replay_dur
    } else {
        replay_dur
    };

    drop(dir);
    TrialResult {
        wall,
        prefetch_gets,
        prefetch_bytes,
        demand_gets,
        demand_bytes,
    }
}

// ============================================================================
// Real-trace replay — drives the ACTUAL read path against a real blessed image
// in the configured object store, using a boot trace captured by `boot_profile`.
// ============================================================================

/// One arm of a real-image trial: replay `trace` (device block indices, from a
/// `.bootset` file) through `WriteCache::read` against `store` (wrapped in the
/// latency model), with the readahead `window`. Returns GETs / bytes / wall for
/// the on-demand replay. `pic` is shared and pre-warmed by the caller so we
/// isolate DATA fetch cost (index-warm-on-open is a baseline mechanism).
#[allow(clippy::too_many_arguments)]
async fn run_trial_real(
    store: Arc<dyn ObjectStore>,
    base: &str,
    name: &str,
    manifest_key: &str,
    device_size: u64,
    block_size: u32,
    trace: &[u64],
    window: u32,
    pic: &Arc<PackIndexCache>,
    rtt: Duration,
    throughput: u64,
) -> TrialResult {
    let latency = Arc::new(LatencyStore::new(Arc::clone(&store), rtt, throughput));
    let content_store = ContentStore::new(Arc::clone(&latency) as Arc<dyn ObjectStore>, base);
    let (data, _etag) = content_store
        .get_manifest(manifest_key)
        .await
        .unwrap()
        .expect("manifest");
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::deserialize(&data).unwrap()));
    let dir = TempDir::new().unwrap();
    let cfg = WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: block_size as usize,
        wal_sync: false,
    };
    let cache = WriteCache::open_fresh_active(cfg).expect("open fresh reader");
    let clean: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(512 * 1024 * 1024));
    let metrics = ExportMetrics::new();
    cache.set_readahead_window_bytes(window);

    latency.reset();
    let start = Instant::now();
    for &b in trace {
        cache
            .read(b * block_size as u64, block_size as usize, clean.as_ref(), pic, &vm, &content_store, &metrics)
            .await
            .unwrap();
    }
    let wall = start.elapsed();
    drop(dir);
    TrialResult {
        wall,
        prefetch_gets: 0,
        prefetch_bytes: 0,
        demand_gets: latency.gets(),
        demand_bytes: latency.bytes(),
    }
}

/// Runtime PARALLEL warm of the boot block list — the ext4/raw/layered delivery
/// (these can't reorder). Issues all boot-block fetches concurrently with a fixed
/// `concurrency` cap (models a real S3 client), `window` controls precise (0) vs
/// coalesced (32 MiB) per-block fetch. Returns GETs/bytes and the WALL — which,
/// unlike the optimistic 1-RTT model, includes the real concurrency waves.
#[allow(clippy::too_many_arguments)]
async fn run_warm_parallel(
    store: Arc<dyn ObjectStore>,
    base: &str,
    name: &str,
    manifest_key: &str,
    device_size: u64,
    block_size: u32,
    trace: &[u64],
    window: u32,
    concurrency: usize,
    pic: &Arc<PackIndexCache>,
    rtt: Duration,
    throughput: u64,
) -> TrialResult {
    let latency = Arc::new(LatencyStore::new(Arc::clone(&store), rtt, throughput));
    let content_store = ContentStore::new(Arc::clone(&latency) as Arc<dyn ObjectStore>, base);
    let (data, _e) = content_store.get_manifest(manifest_key).await.unwrap().expect("manifest");
    let vm = Arc::new(parking_lot::RwLock::new(VolumeManifest::deserialize(&data).unwrap()));
    let dir = TempDir::new().unwrap();
    let cache = WriteCache::open_fresh_active(WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size,
        block_size: block_size as usize,
        wal_sync: false,
    })
    .unwrap();
    let clean: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(512 * 1024 * 1024));
    let metrics = ExportMetrics::new();
    cache.set_readahead_window_bytes(window);

    latency.reset();
    let start = Instant::now();
    futures::stream::iter(trace.iter().copied())
        .map(|b| {
            let cache = &cache;
            let clean = clean.as_ref();
            let vm = &vm;
            let cs = &content_store;
            let metrics = &metrics;
            async move {
                cache
                    .read(b * block_size as u64, block_size as usize, clean, pic, vm, cs, metrics)
                    .await
                    .unwrap();
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<()>>()
        .await;
    let wall = start.elapsed();
    drop(dir);
    TrialResult { wall, prefetch_gets: 0, prefetch_bytes: 0, demand_gets: latency.gets(), demand_bytes: latency.bytes() }
}

/// Real-image boot-set study. Replays `boot_profile`-captured traces against the
/// real blessed images to measure the ACTUAL readahead GET count (the decisive
/// number). Opt in:
///   GLIDEFS_REAL_CONFIG=/tmp/prof-cfg.toml GLIDEFS_REAL_PREFIX=prof \
///   GLIDEFS_REAL_IMAGES="python_slim:/tmp/python_slim.bootset,node_slim:/tmp/node_slim.bootset" \
///   cargo test --features test-utils --test integration boot_replay::real_trace_study \
///     -- --ignored --nocapture
#[tokio::test]
#[ignore = "needs a configured object store + captured traces"]
async fn real_trace_study() {
    use glidefs::config::Settings;
    use glidefs::parse_object_store::parse_url_opts;

    let Ok(cfg_path) = std::env::var("GLIDEFS_REAL_CONFIG") else {
        eprintln!("set GLIDEFS_REAL_CONFIG / GLIDEFS_REAL_PREFIX / GLIDEFS_REAL_IMAGES");
        return;
    };
    let prefix = std::env::var("GLIDEFS_REAL_PREFIX").unwrap_or_else(|_| "prof".into());
    let images = std::env::var("GLIDEFS_REAL_IMAGES").unwrap_or_default();

    let settings = Settings::from_file(std::path::Path::new(&cfg_path)).unwrap();
    let (store, path_from_url) = parse_url_opts(
        &settings.storage.url.parse().unwrap(),
        settings.cloud_provider_env_vars().into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )
    .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let base = format!("{}/exports/{}", path_from_url, prefix);
    let throughput = 100 * 1024 * 1024;

    let rtt_ms = std::env::var("GLIDEFS_REAL_RTT_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(40u64);
    let rtt = Duration::from_millis(rtt_ms);
    let conc: usize = std::env::var("GLIDEFS_REAL_CONCURRENCY").ok().and_then(|s| s.parse().ok()).unwrap_or(32);
    // Serial cost (reactive boot reads / readahead): one RTT per GET in sequence.
    let serial_ms = |gets: u64, bytes: u64| gets as f64 * rtt_ms as f64 + (bytes as f64 / throughput as f64) * 1000.0;
    // Parallel cost (a proactive warm with a concurrency cap): GETs run in
    // ceil(gets/conc) waves; bytes still stream at throughput.
    let par_ms = |gets: u64, bytes: u64| (gets as f64 / conc as f64).ceil() * rtt_ms as f64 + (bytes as f64 / throughput as f64) * 1000.0;
    let win = 32u64 * 1024 * 1024;

    println!("\n=== PER-TYPE mechanism study (base={base}, RTT={rtt_ms}ms, {} MiB/s, warm concurrency={conc}) ===", throughput / (1024 * 1024));
    println!("  Candidate mechanisms (all on the real image+trace; GETs/bytes EXACT, time = cost model):");
    println!("    readahead   = 32MiB pack-window, reactive/serial            (today's baseline, any type)");
    println!("    warm-precise= parallel warm of EXACT boot blocks            (ext4/raw/layered delivery)");
    println!("    warm-window = parallel warm, 32MiB-coalesced per pack       (ext4/raw alt — over-fetches)");
    println!("    reorder     = build-time contiguous prefix, 1 GET           (EROFS delivery)");
    println!(
        "{:<16} {:>5} {:>6} | {:>6} {:>7} | {:>5} {:>7} {:>7} | {:>5} {:>7} {:>7} | {:>5} {:>7} {:>7}",
        "image", "blks", "MiB", "rd_GET", "rd_ms", "wpGET", "wp_MiB", "wp_ms", "wwGET", "ww_MiB", "ww_ms", "reGET", "re_MiB", "re_ms"
    );
    for spec in images.split(',').filter(|s| !s.is_empty()) {
        let (name, tracef) = spec.split_once(':').expect("name:tracefile");
        let trace: Vec<u64> = std::fs::read_to_string(tracef)
            .unwrap()
            .lines()
            .filter_map(|l| l.trim().parse().ok())
            .collect();

        let cs = ContentStore::new(Arc::clone(&store), &base);
        let (data, _e) = cs.get_manifest(&format!("bases/{name}")).await.unwrap().expect("manifest");
        let vm = VolumeManifest::deserialize(&data).unwrap();
        let (device_size, block_size) = (vm.size, vm.block_size);
        let boot_mib = trace.len() as f64 * block_size as f64 / (1024.0 * 1024.0);

        let mkey = format!("bases/{name}");
        let pic_dir = TempDir::new().unwrap();
        let pic = Arc::new(PackIndexCache::open(pic_dir.path()).await.unwrap());
        let _ = run_trial_real(Arc::clone(&store), &base, name, &mkey, device_size, block_size, &trace, 0, &pic, Duration::ZERO, 0).await;

        // Baseline: reactive readahead (serial during boot).
        let reada = run_trial_real(Arc::clone(&store), &base, name, &mkey, device_size, block_size, &trace, win as u32, &pic, rtt, throughput).await;
        // ext4/raw delivery A: parallel warm of EXACT blocks (window=0 → precise).
        let wp = run_warm_parallel(Arc::clone(&store), &base, name, &mkey, device_size, block_size, &trace, 0, conc, &pic, rtt, throughput).await;
        // ext4/raw delivery B: parallel warm, 32MiB-coalesced (window) per pack.
        let ww = run_warm_parallel(Arc::clone(&store), &base, name, &mkey, device_size, block_size, &trace, win as u32, conc, &pic, rtt, throughput).await;

        // EROFS delivery: measured contiguous reorder (1 GET).
        let contents = read_blocks_content(Arc::clone(&store), &base, name, &mkey, device_size, block_size, &trace, &pic).await;
        let s3_re = Arc::new(InMemory::new());
        build_image_from_contents(Arc::clone(&s3_re), "reord", &contents).await;
        let pic_re_dir = TempDir::new().unwrap();
        let pic_re = Arc::new(PackIndexCache::open(pic_re_dir.path()).await.unwrap());
        let front: Vec<u64> = (0..contents.len() as u64).collect();
        prewarm_index(&s3_re, "reord", contents.len() as u64, &front, &pic_re).await;
        let bs = run_trial(&s3_re, "reord", contents.len() as u64, &front, &[], Arm::Readahead, &pic_re, rtt, throughput).await;

        let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
        println!(
            "{:<16} {:>5} {:>6.1} | {:>6} {:>7.0} | {:>5} {:>7.1} {:>7.0} | {:>5} {:>7.1} {:>7.0} | {:>5} {:>7.1} {:>7.0}",
            name, trace.len(), boot_mib,
            reada.demand_gets, serial_ms(reada.demand_gets, reada.demand_bytes),
            wp.demand_gets, mib(wp.demand_bytes), par_ms(wp.demand_gets, wp.demand_bytes),
            ww.demand_gets, mib(ww.demand_bytes), par_ms(ww.demand_gets, ww.demand_bytes),
            bs.demand_gets, mib(bs.demand_bytes), serial_ms(bs.demand_gets, bs.demand_bytes),
        );
    }
}

/// LAYERED-image study: replay each layer's captured boot trace against THAT
/// layer's own ext4 manifest (layered images are N shared, immutable layer
/// blobs — reorder is impossible, so the candidates are readahead vs a
/// cross-layer parallel precise warm). Sums across layers.
///   GLIDEFS_LAYERED_CONFIG=/tmp/prof-cfg.toml GLIDEFS_LAYERED_NAME=python_layered \
///   GLIDEFS_LAYERED_TRACE=/tmp/python_layered.bootset \
///   cargo test --features test-utils --test integration boot_replay::layered_study \
///     -- --ignored --nocapture
#[tokio::test]
#[ignore = "needs a blessed layered image + captured per-layer traces"]
async fn layered_study() {
    use glidefs::config::Settings;
    use glidefs::oci::layer_store::{get_image_descriptor, layer_base_path};
    use glidefs::parse_object_store::parse_url_opts;

    let Ok(cfg_path) = std::env::var("GLIDEFS_LAYERED_CONFIG") else {
        eprintln!("set GLIDEFS_LAYERED_CONFIG / GLIDEFS_LAYERED_NAME / GLIDEFS_LAYERED_TRACE");
        return;
    };
    let name = std::env::var("GLIDEFS_LAYERED_NAME").unwrap();
    let trace_prefix = std::env::var("GLIDEFS_LAYERED_TRACE").unwrap();
    let rtt_ms = std::env::var("GLIDEFS_REAL_RTT_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(40u64);
    let conc: usize = std::env::var("GLIDEFS_REAL_CONCURRENCY").ok().and_then(|s| s.parse().ok()).unwrap_or(32);
    let rtt = Duration::from_millis(rtt_ms);
    let throughput = 100u64 * 1024 * 1024;

    let settings = Settings::from_file(std::path::Path::new(&cfg_path)).unwrap();
    let (store, path_from_url) = parse_url_opts(
        &settings.storage.url.parse().unwrap(),
        settings.cloud_provider_env_vars().into_iter(),
        Some(settings.storage.connect_timeout()),
        Some(settings.storage.request_timeout()),
    )
    .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let db_path = path_from_url.to_string();
    let desc = get_image_descriptor(&store, &db_path, &name).await.unwrap().expect("image descriptor");

    println!("\n=== LAYERED study {name}: {} layers (RTT={rtt_ms}ms, {} MiB/s, conc={conc}) ===", desc.layers.len(), throughput / (1024 * 1024));
    let serial_ms = |g: u64, b: u64| g as f64 * rtt_ms as f64 + (b as f64 / throughput as f64) * 1000.0;

    let (mut tot_rd_g, mut tot_rd_b, mut tot_wp_g, mut tot_wp_b) = (0u64, 0u64, 0u64, 0u64);
    let mut tot_blocks = 0usize;
    for (i, digest) in desc.layers.iter().enumerate() {
        let tf = format!("{trace_prefix}.L{i}");
        let trace: Vec<u64> = match std::fs::read_to_string(&tf) {
            Ok(s) => s.lines().filter_map(|l| l.trim().parse().ok()).collect(),
            Err(_) => continue, // layer faulted nothing
        };
        if trace.is_empty() {
            continue;
        }
        let base = layer_base_path(&db_path, digest);
        let cs = ContentStore::new(Arc::clone(&store), &base);
        let (data, _e) = cs.get_manifest("layer").await.unwrap().expect("layer manifest");
        let vm = VolumeManifest::deserialize(&data).unwrap();
        let (device_size, block_size) = (vm.size, vm.block_size);

        let pic_dir = TempDir::new().unwrap();
        let pic = Arc::new(PackIndexCache::open(pic_dir.path()).await.unwrap());
        let dev = format!("lyr{i}");
        let _ = run_trial_real(Arc::clone(&store), &base, &dev, "layer", device_size, block_size, &trace, 0, &pic, Duration::ZERO, 0).await;
        let reada = run_trial_real(Arc::clone(&store), &base, &dev, "layer", device_size, block_size, &trace, 32 * 1024 * 1024, &pic, rtt, throughput).await;
        let wp = run_warm_parallel(Arc::clone(&store), &base, &dev, "layer", device_size, block_size, &trace, 0, conc, &pic, rtt, throughput).await;
        println!(
            "  L{i} {}: {} blks | readahead {} GET/{:.1} MiB | warm-precise {} GET/{:.1} MiB",
            &digest[..19.min(digest.len())], trace.len(),
            reada.demand_gets, reada.demand_bytes as f64 / (1024.0 * 1024.0),
            wp.demand_gets, wp.demand_bytes as f64 / (1024.0 * 1024.0),
        );
        tot_rd_g += reada.demand_gets; tot_rd_b += reada.demand_bytes;
        tot_wp_g += wp.demand_gets; tot_wp_b += wp.demand_bytes;
        tot_blocks += trace.len();
    }
    // readahead is reactive/serial across the whole boot; the precise warm runs
    // ONE cross-layer parallel pass (ceil(total_gets/conc) waves).
    let rd_ms = serial_ms(tot_rd_g, tot_rd_b);
    let wp_ms = (tot_wp_g as f64 / conc as f64).ceil() * rtt_ms as f64 + (tot_wp_b as f64 / throughput as f64) * 1000.0;
    println!("\n  TOTAL {tot_blocks} blocks across {} layers:", desc.layers.len());
    println!("    readahead (baseline):       {tot_rd_g} GETs, {:.1} MiB → {rd_ms:.0} ms", tot_rd_b as f64 / (1024.0 * 1024.0));
    println!("    warm-precise (cross-layer): {tot_wp_g} GETs, {:.1} MiB → {wp_ms:.0} ms  ({:.1}× over readahead)", tot_wp_b as f64 / (1024.0 * 1024.0), rd_ms / wp_ms.max(1.0));
    println!("    reorder: N/A — layers are shared & immutable, cannot reorder.");
}

// ============================================================================
// Statistics
// ============================================================================

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = xs.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 {
        xs[n / 2]
    } else {
        (xs[n / 2 - 1] + xs[n / 2]) / 2.0
    }
}

fn percentile(xs: &mut [f64], p: f64) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if xs.is_empty() {
        return f64::NAN;
    }
    let rank = (p / 100.0 * (xs.len() - 1) as f64).round() as usize;
    xs[rank.min(xs.len() - 1)]
}

/// Mann–Whitney U test (two-sided) with normal approximation + tie correction.
/// Returns (U, z, p_value). Treats `a` (treatment) vs `b` (baseline).
fn mann_whitney_u(a: &[f64], b: &[f64]) -> (f64, f64, f64) {
    let n1 = a.len();
    let n2 = b.len();
    if n1 == 0 || n2 == 0 {
        return (f64::NAN, f64::NAN, f64::NAN);
    }
    // Pool and rank (average ranks for ties).
    let mut pooled: Vec<(f64, u8)> = Vec::with_capacity(n1 + n2);
    for &x in a {
        pooled.push((x, 0));
    }
    for &x in b {
        pooled.push((x, 1));
    }
    pooled.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap());
    let n = pooled.len();
    let mut ranks = vec![0.0f64; n];
    let mut tie_term = 0.0f64;
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && pooled[j].0 == pooled[i].0 {
            j += 1;
        }
        let t = (j - i) as f64;
        let avg_rank = (i as f64 + 1.0 + j as f64) / 2.0; // average of ranks i+1..=j
        for k in i..j {
            ranks[k] = avg_rank;
        }
        tie_term += t * t * t - t;
        i = j;
    }
    let r1: f64 = pooled
        .iter()
        .zip(ranks.iter())
        .filter(|((_, g), _)| *g == 0)
        .map(|(_, r)| *r)
        .sum();
    let n1f = n1 as f64;
    let n2f = n2 as f64;
    let u1 = r1 - n1f * (n1f + 1.0) / 2.0;
    let u = u1.min(n1f * n2f - u1);
    let mu = n1f * n2f / 2.0;
    let nf = n as f64;
    let sigma = ((n1f * n2f / 12.0) * ((nf + 1.0) - tie_term / (nf * (nf - 1.0)))).sqrt();
    if sigma == 0.0 {
        return (u, 0.0, 1.0);
    }
    let z = (u - mu) / sigma;
    let p = 2.0 * normal_sf(z.abs());
    (u, z, p)
}

/// Upper tail of the standard normal (1 - Φ(z)) via erfc.
fn normal_sf(z: f64) -> f64 {
    0.5 * erfc(z / std::f64::consts::SQRT_2)
}

/// Abramowitz & Stegun 7.1.26 approximation of erfc.
fn erfc(x: f64) -> f64 {
    let z = x.abs();
    let t = 1.0 / (1.0 + 0.5 * z);
    let tau = t
        * (-z * z - 1.26551223
            + t * (1.00002368
                + t * (0.37409196
                    + t * (0.09678418
                        + t * (-0.18628806
                            + t * (0.27886807
                                + t * (-1.13520398
                                    + t * (1.48851587
                                        + t * (-0.82215223 + t * 0.17087277)))))))))
        .exp();
    if x >= 0.0 {
        tau
    } else {
        2.0 - tau
    }
}

/// Hodges–Lehmann estimator: median of all pairwise differences a_i - b_j.
/// Robust point estimate of the median shift (treatment − baseline).
fn hodges_lehmann(a: &[f64], b: &[f64]) -> f64 {
    let mut diffs = Vec::with_capacity(a.len() * b.len());
    for &x in a {
        for &y in b {
            diffs.push(x - y);
        }
    }
    median(&mut diffs)
}

// ============================================================================
// Tests
// ============================================================================

/// Fast smoke test: a few trials, asserts the harness invariants hold.
///
/// Heavy-scatter regime: a 4 GiB device (chunk = 128 MiB = 1024 blocks) with 32
/// boot blocks each in a DIFFERENT chunk (stride = one chunk). The readahead
/// window can't help — each pack holds only one boot block — so it pays one RTT
/// per block, just like demand-only. The reorder collapses all 32 into one
/// contiguous pack that the prefix-warm pulls in ~1 GET. This is the regime
/// where an explicit boot set wins decisively.
#[tokio::test]
async fn boot_replay_smoke() {
    let device_blocks = 32 * 1024; // 4 GiB device = 32 chunks
    let spec = ImageSpec {
        device_blocks,
        boot_count: 32,
        scatter_stride: 1024, // one chunk apart → each boot block its own pack
    };

    let s3 = Arc::new(InMemory::new());
    let scattered: Vec<u64> = (0..spec.boot_count).map(|i| spec.scattered_block(i)).collect();
    let reordered: Vec<u64> = (0..spec.boot_count).map(|i| spec.reordered_block(i)).collect();
    build_image(Arc::clone(&s3), "scattered", device_blocks, &scattered).await;
    build_image(Arc::clone(&s3), "reordered", device_blocks, &reordered).await;

    let pic_dir = TempDir::new().unwrap();
    let pic = Arc::new(PackIndexCache::open(pic_dir.path()).await.unwrap());
    prewarm_index(&s3, "scattered", device_blocks, &scattered, &pic).await;
    prewarm_index(&s3, "reordered", device_blocks, &reordered, &pic).await;

    let rtt = Duration::from_millis(5);
    let throughput = 100 * 1024 * 1024; // 100 MiB/s

    let demand = run_trial(&s3, "scattered", device_blocks, &spec.scattered_trace(), &[], Arm::DemandOnly, &pic, rtt, throughput).await;
    let reada = run_trial(&s3, "scattered", device_blocks, &spec.scattered_trace(), &[], Arm::Readahead, &pic, rtt, throughput).await;
    let bootset = run_trial(&s3, "reordered", device_blocks, &spec.reordered_trace(), &reordered, Arm::BootSet, &pic, rtt, throughput).await;
    let warm = run_trial(&s3, "scattered", device_blocks, &spec.scattered_trace(), &[], Arm::Warm, &pic, rtt, throughput).await;

    for (name, r) in [("demand", &demand), ("readahead", &reada), ("bootset", &bootset), ("warm", &warm)] {
        eprintln!(
            "smoke {:<10} prefetch={}g/{}B demand={}g/{}B total={}g wall={:?}",
            name, r.prefetch_gets, r.prefetch_bytes, r.demand_gets, r.demand_bytes, r.total_gets(), r.wall
        );
    }

    // Invariants that must hold regardless of tuning:
    // 1. Warm replay issues no on-demand GETs (everything pre-cached).
    assert_eq!(warm.demand_gets, 0, "warm replay must hit cache for every block");
    // 2. The reordered boot set's TOTAL fetch (prefetch + demand) is far cheaper
    //    than the scattered demand-only replay — the whole point of the reorder.
    assert!(
        bootset.total_gets() < demand.total_gets(),
        "boot-set total ({}) should be < scattered demand-only ({})",
        bootset.total_gets(),
        demand.total_gets()
    );
    // 3. On a set scattered one-per-pack, readahead can't beat demand-only on GET
    //    count (the window finds nothing extra to coalesce).
    assert!(
        demand.demand_gets >= reada.demand_gets,
        "demand-only ({}) should not beat readahead ({}) on GET count",
        demand.demand_gets,
        reada.demand_gets
    );
    // 4. Boot-set boots faster than the readahead baseline in this regime.
    assert!(
        bootset.wall < reada.wall,
        "boot-set boot ({:?}) should beat readahead ({:?}) under heavy scatter",
        bootset.wall,
        reada.wall
    );
}

/// Statistical study (RTT × scatter sweep). Ignored by default; opt in with
/// `GLIDEFS_BOOT_REPLAY_FULL=1 ... -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "long statistical study; run explicitly"]
async fn full_study() {
    if std::env::var("GLIDEFS_BOOT_REPLAY_FULL").as_deref() != Ok("1") {
        eprintln!("set GLIDEFS_BOOT_REPLAY_FULL=1 to run the full study");
        return;
    }
    const TRIALS: usize = 25;
    const BOOT: u64 = 32;
    // 32 chunks (4 GiB sparse). strides give 1 / few / 32 packs.
    let device_blocks = 32 * 1024;
    let throughput = 100 * 1024 * 1024;

    println!("\n=== Boot-set cold-replay study (n={TRIALS}/arm) ===");
    // stride 1 = fully clustered (1 pack); 8 = a handful of packs; 1024 = one
    // boot block per chunk (32 packs, worst case for readahead).
    for scatter_stride in [1u64, 8, 1024] {
        let spec = ImageSpec { device_blocks, boot_count: BOOT, scatter_stride };
        let s3 = Arc::new(InMemory::new());
        let scattered: Vec<u64> = (0..spec.boot_count).map(|i| spec.scattered_block(i)).collect();
        let reordered: Vec<u64> = (0..spec.boot_count).map(|i| spec.reordered_block(i)).collect();
        build_image(Arc::clone(&s3), "scattered", device_blocks, &scattered).await;
        build_image(Arc::clone(&s3), "reordered", device_blocks, &reordered).await;
        let pic_dir = TempDir::new().unwrap();
        let pic = Arc::new(PackIndexCache::open(pic_dir.path()).await.unwrap());
        prewarm_index(&s3, "scattered", device_blocks, &scattered, &pic).await;
        prewarm_index(&s3, "reordered", device_blocks, &reordered, &pic).await;

        let (sc_chunks, sc_packs) = scatter_stats(&s3, "scattered", &scattered).await;
        let (ro_chunks, ro_packs) = scatter_stats(&s3, "reordered", &reordered).await;
        println!(
            "\n#### scatter_stride={scatter_stride}: scattered set spans {sc_chunks} chunks / {sc_packs} packs;  reordered spans {ro_chunks} chunks / {ro_packs} packs (predicts readahead GET count)"
        );

        for rtt_ms in [5u64, 20, 40] {
            let rtt = Duration::from_millis(rtt_ms);
            let mut walls: std::collections::HashMap<&str, Vec<f64>> = std::collections::HashMap::new();
            let mut gets: std::collections::HashMap<&str, Vec<f64>> = std::collections::HashMap::new();
            for _ in 0..TRIALS {
                for arm in [Arm::DemandOnly, Arm::Readahead, Arm::BootSet, Arm::Warm] {
                    let (name, trace, set): (&str, Vec<u64>, Vec<u64>) = match arm {
                        Arm::BootSet => ("reordered", spec.reordered_trace(), reordered.clone()),
                        _ => ("scattered", spec.scattered_trace(), vec![]),
                    };
                    let r = run_trial(&s3, name, device_blocks, &trace, &set, arm, &pic, rtt, throughput).await;
                    walls.entry(arm.label()).or_default().push(r.wall.as_secs_f64() * 1000.0);
                    gets.entry(arm.label()).or_default().push(r.total_gets() as f64);
                }
            }
            println!("\n-- scatter_stride={scatter_stride} rtt={rtt_ms}ms --");
            for arm in [Arm::DemandOnly, Arm::Readahead, Arm::BootSet, Arm::Warm] {
                let w = walls.get_mut(arm.label()).unwrap();
                let g = gets.get(arm.label()).unwrap();
                let p50 = median(&mut w.clone());
                let p95 = percentile(&mut w.clone(), 95.0);
                let gmed = median(&mut g.clone());
                println!("  {:<16} boot p50={:7.1}ms p95={:7.1}ms  gets(med)={:5.0}", arm.label(), p50, p95, gmed);
            }
            // Headline test: boot-set vs readahead on wall-clock.
            let bs = walls.get(Arm::BootSet.label()).unwrap().clone();
            let ra = walls.get(Arm::Readahead.label()).unwrap().clone();
            let (u, z, p) = mann_whitney_u(&bs, &ra);
            let hl = hodges_lehmann(&bs, &ra);
            println!(
                "  Mann-Whitney boot-set vs readahead: U={u:.0} z={z:.2} p={p:.4}  Hodges-Lehmann Δmedian={hl:+.1}ms",
            );
        }
    }
}
