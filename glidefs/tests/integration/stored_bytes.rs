//! Stored-bytes accounting: the manifest's per-chunk live-block counts must
//! equal the number of non-zero blocks actually stored (newest pack-index entry
//! per (chunk, chunk_offset) with comp_length > 0), through overwrites, zeroing,
//! forks, compaction and flush/compaction races.
//!
//! The oracle recomputes the truth from the pack indexes in the object store; it
//! shares no code with the count the flush maintains.
//!
//! Run with: `cargo test --features test-utils --test integration stored_bytes`

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tempfile::TempDir;

use glidefs::block::cache::BlockCache;
use glidefs::block::content_store::ContentStore;
use glidefs::block::live_count::{backfill_live_counts, stored_bytes_exact};
use glidefs::block::pack_index_cache::PackIndexCache;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::block::write_cache::WriteCache;
use glidefs::block::write_cache::compact::compact_if_needed;
use glidefs::block::state::Active;

use super::create_test_cache;

const BS: usize = 128 * 1024;
const BPC: usize = 1024; // blocks per 128 MiB chunk

type Vm = Arc<parking_lot::RwLock<VolumeManifest>>;

/// Truth from the object store: per chunk, merge the pack indexes oldest ->
/// newest and count entries with comp_length > 0.
async fn oracle_live_blocks(cs: &ContentStore, vm: &Vm) -> u64 {
    let chunks: Vec<(u32, Vec<u64>)> = vm
        .read()
        .chunks
        .iter()
        .map(|(&i, e)| (i, e.packs.clone()))
        .collect();
    let mut live = 0u64;
    for (chunk_idx, packs) in chunks {
        let mut newest: HashMap<u32, u32> = HashMap::new();
        for pid in packs {
            for e in cs.get_pack_index(chunk_idx, pid).await.unwrap() {
                newest.insert(e.chunk_offset, e.comp_length);
            }
        }
        live += newest.values().filter(|&&c| c > 0).count() as u64;
    }
    live
}

async fn assert_stored(cs: &ContentStore, vm: &Vm, expect_blocks: u64, ctx: &str) {
    let oracle = oracle_live_blocks(cs, vm).await;
    let got = vm.read().stored_bytes();
    assert_eq!(oracle, expect_blocks, "{ctx}: oracle disagrees with the model");
    assert_eq!(got, Some(oracle * BS as u64), "{ctx}: stored_bytes != oracle x block_size");
}

fn block(seed: u8) -> Vec<u8> {
    vec![seed.max(1); BS]
}

fn write(cache: &WriteCache<Active>, block_idx: usize, data: &[u8]) {
    cache.write((block_idx * BS) as u64, data).unwrap();
}

async fn flush(cache: &WriteCache<Active>, cs: &ContentStore, pic: &Arc<PackIndexCache>, vm: &Vm) {
    cache.flush_to_s3(cs, pic, vm).await.unwrap();
}

#[tokio::test]
async fn overwrite_does_not_change_count() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = create_test_cache(&dir, "sb-overwrite", s3).await;

    for i in 0..5 {
        write(&cache, i, &block(i as u8 + 1));
    }
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 5, "after first flush").await;

    // Overwrite two live blocks with different data: still 5 live.
    write(&cache, 1, &block(100));
    write(&cache, 3, &block(101));
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 5, "after overwrite").await;
}

#[tokio::test]
async fn zeroing_and_unzeroing() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = create_test_cache(&dir, "sb-zero", s3).await;

    for i in 0..4 {
        write(&cache, i, &block(i as u8 + 1));
    }
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 4, "initial").await;

    // data -> zero: a tombstone, no longer stored.
    write(&cache, 2, &vec![0u8; BS]);
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 3, "data -> zero").await;

    // zero over a block that was never written: nothing stored, nothing changes.
    write(&cache, 50, &vec![0u8; BS]);
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 3, "zero over unwritten").await;

    // zero over a tombstone: unchanged.
    write(&cache, 2, &vec![0u8; BS]);
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 3, "zero over tombstone").await;

    // zero -> data: stored again.
    write(&cache, 2, &block(77));
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 4, "zero -> data").await;

    // Zero everything: nothing stored.
    for i in 0..4 {
        write(&cache, i, &vec![0u8; BS]);
    }
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 0, "all zeroed").await;
}

#[tokio::test]
async fn fork_counts_inherited_chunks_and_diverges_independently() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base_dir = TempDir::new().unwrap();
    let (base, cs, pic, base_vm, _cc, _m) =
        create_test_cache(&base_dir, "sb-base", Arc::clone(&s3)).await;

    // Base: 3 blocks in chunk 0, 2 in chunk 1.
    for i in 0..3 {
        write(&base, i, &block(i as u8 + 1));
    }
    for i in 0..2 {
        write(&base, BPC + i, &block(i as u8 + 10));
    }
    flush(&base, &cs, &pic, &base_vm).await;
    assert_stored(&cs, &base_vm, 5, "base").await;

    // Fork = clone of the base manifest (as the router does). Inherited chunks
    // count in full: the fork's bill includes the base image.
    let fork_dir = TempDir::new().unwrap();
    let (fork, fork_cs, fork_pic, fork_vm, _fcc, _fm) =
        create_test_cache(&fork_dir, "sb-fork", Arc::clone(&s3)).await;
    *fork_vm.write() = base_vm.read().clone();
    assert_stored(&fork_cs, &fork_vm, 5, "fork, nothing written").await;

    // Fork overwrites one inherited block, zeroes another, adds a new one.
    write(&fork, 0, &block(200));
    write(&fork, 1, &vec![0u8; BS]);
    write(&fork, 5, &block(201));
    flush(&fork, &fork_cs, &fork_pic, &fork_vm).await;
    assert_stored(&fork_cs, &fork_vm, 5, "fork after divergence (5 - 1 zeroed + 1 new)").await;

    // The base is untouched.
    assert_stored(&cs, &base_vm, 5, "base unaffected by fork").await;
}

#[tokio::test]
async fn compaction_preserves_count_and_tombstones() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, cc, _m) = create_test_cache(&dir, "sb-compact", Arc::clone(&s3)).await;

    // 20 flushes -> >16 packs in chunk 0, mixing overwrites and zeroing.
    for round in 1..=20u8 {
        write(&cache, (round % 6) as usize, &block(round));
        if round % 5 == 0 {
            write(&cache, ((round + 1) % 6) as usize, &vec![0u8; BS]);
        }
        flush(&cache, &cs, &pic, &vm).await;
    }
    let before = oracle_live_blocks(&cs, &vm).await;
    assert_eq!(vm.read().stored_bytes(), Some(before * BS as u64), "before compaction");
    assert!(vm.read().chunk_pack_ids(0).unwrap().len() > 16);

    let compact_cc: Arc<dyn BlockCache> = cc.clone();
    let results = compact_if_needed(16, 0.5, 0, &HashMap::new(), &cs, &pic, &vm, &compact_cc)
        .await
        .unwrap();
    assert!(!results.is_empty(), "compaction should have run");
    assert_eq!(vm.read().chunk_pack_ids(0).unwrap().len(), 1);

    assert_stored(&cs, &vm, before, "after compaction (content unchanged)").await;
}

/// Flush appends a pack while compaction replaces the pack list it started
/// from. Compaction preserves content, so the flush's absolute count stays right
/// in every interleaving.
#[tokio::test]
async fn flush_racing_compaction_keeps_count_exact() {
    for yields in 0..4 {
        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let (cache, cs, pic, vm, cc, _m) =
            create_test_cache(&dir, &format!("sb-race-{yields}"), Arc::clone(&s3)).await;

        for i in 1..=17u8 {
            write(&cache, (i % 4) as usize, &block(i));
            flush(&cache, &cs, &pic, &vm).await;
        }
        // Concurrent writes: zero one live block, add two new ones, overwrite one.
        write(&cache, 0, &vec![0u8; BS]);
        write(&cache, 7, &block(0x70));
        write(&cache, 8, &block(0x71));
        write(&cache, 1, &block(0x72));

        let compact_cc: Arc<dyn BlockCache> = cc.clone();
        let no_idle = HashMap::new();
        let (compacted, flushed) = tokio::join!(
            compact_if_needed(16, 0.5, 0, &no_idle, &cs, &pic, &vm, &compact_cc),
            async {
                for _ in 0..yields {
                    tokio::task::yield_now().await;
                }
                cache.flush_to_s3(&cs, &pic, &vm).await
            }
        );
        let _ = compacted; // may abort on a CAS conflict; the count must hold regardless
        flushed.unwrap();

        let oracle = oracle_live_blocks(&cs, &vm).await;
        assert_eq!(
            vm.read().stored_bytes(),
            Some(oracle * BS as u64),
            "yields={yields}: count drifted from the pack indexes"
        );
    }
}

/// Counts survive the manifest round trip through the object store.
#[tokio::test]
async fn counts_persist_through_manifest_sync() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = create_test_cache(&dir, "sb-persist", Arc::clone(&s3)).await;
    for i in 0..7 {
        write(&cache, i, &block(i as u8 + 1));
    }
    write(&cache, BPC + 3, &block(9));
    flush(&cache, &cs, &pic, &vm).await;
    cache.sync_manifest(&cs, &vm).await.unwrap();

    let (data, _) = cs.get_manifest("sb-persist").await.unwrap().unwrap();
    let reloaded = VolumeManifest::deserialize(&data).unwrap();
    assert_eq!(reloaded.stored_bytes(), Some(8 * BS as u64));
    assert!(reloaded.chunks.values().all(|e| e.live_blocks.is_some()));
}

/// A manifest written before counts existed loads with unknown counts (billed
/// as zero, never over), and the background backfill recovers them exactly
/// from the pack indexes.
#[tokio::test]
async fn backfill_recovers_unknown_counts() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = create_test_cache(&dir, "sb-backfill", Arc::clone(&s3)).await;
    for i in 0..6 {
        write(&cache, i, &block(i as u8 + 1));
    }
    write(&cache, 2, &vec![0u8; BS]);
    write(&cache, BPC, &block(50));
    flush(&cache, &cs, &pic, &vm).await;
    write(&cache, 4, &block(60));
    write(&cache, 3, &vec![0u8; BS]);
    flush(&cache, &cs, &pic, &vm).await;
    let truth = oracle_live_blocks(&cs, &vm).await;
    assert_eq!(truth, 5 + 1 - 1);

    // Simulate a pre-counts manifest.
    for e in vm.write().chunks.values_mut() {
        e.live_blocks = None;
    }
    assert_eq!(vm.read().stored_bytes(), None, "unknown counts give no figure, not a smaller one");
    assert_eq!(vm.read().unknown_live_chunks().len(), 2);

    // Cold caches: the backfill must read indexes itself.
    backfill_live_counts(&vm, &cs, &pic).await;
    assert_eq!(vm.read().stored_bytes(), Some(truth * BS as u64));
}

/// Randomised op sequences against a model of which blocks hold non-zero data.
/// Ops: write data / write zero / flush / flush + compact. After every flush the
/// manifest count, the object-store oracle and the model must all agree.
#[tokio::test]
async fn random_ops_match_model_and_oracle() {
    for seed in 1..=6u64 {
        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = TempDir::new().unwrap();
        let (cache, cs, pic, vm, cc, _m) =
            create_test_cache(&dir, &format!("sb-rand-{seed}"), Arc::clone(&s3)).await;
        let compact_cc: Arc<dyn BlockCache> = cc.clone();

        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        // Blocks in both chunks, so chunk counts add up.
        let slots: Vec<usize> = (0..6).chain(BPC..BPC + 6).collect();
        let mut live: BTreeSet<usize> = BTreeSet::new(); // flushed model
        let mut pending: HashMap<usize, bool> = HashMap::new(); // unflushed: block -> non-zero

        for step in 0..60 {
            match next() % 6 {
                0..=2 => {
                    let b = slots[(next() % slots.len() as u64) as usize];
                    write(&cache, b, &block((next() % 250) as u8 + 1));
                    pending.insert(b, true);
                }
                3 => {
                    let b = slots[(next() % slots.len() as u64) as usize];
                    write(&cache, b, &vec![0u8; BS]);
                    pending.insert(b, false);
                }
                op => {
                    flush(&cache, &cs, &pic, &vm).await;
                    for (b, nz) in pending.drain() {
                        if nz { live.insert(b); } else { live.remove(&b); }
                    }
                    if op == 5 {
                        let _ = compact_if_needed(2, 0.5, 0, &HashMap::new(), &cs, &pic, &vm, &compact_cc).await;
                    }
                    assert_stored(&cs, &vm, live.len() as u64, &format!("seed {seed} step {step}")).await;
                }
            }
        }
    }
}

async fn fresh_index_cache() -> (Arc<PackIndexCache>, TempDir) {
    let dir = TempDir::new().unwrap();
    let pic = Arc::new(PackIndexCache::open(dir.path()).await.unwrap());
    (pic, dir)
}

fn pack_key(chunk: u32, pack: u64) -> object_store::path::Path {
    object_store::path::Path::from(format!("test/chunks/{chunk:04}/{pack:016x}.pack"))
}

/// A flush whose pack indexes are not in the cache fetches them: the count
/// stays exact (it is never dropped to unknown, which would bill nothing).
#[tokio::test]
async fn flush_with_a_cold_index_cache_keeps_an_exact_count() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = create_test_cache(&dir, "sb-cold", Arc::clone(&s3)).await;
    for i in 0..4 {
        write(&cache, i, &block(i as u8 + 1));
    }
    flush(&cache, &cs, &pic, &vm).await;
    write(&cache, 1, &vec![0u8; BS]);
    flush(&cache, &cs, &pic, &vm).await;
    assert_stored(&cs, &vm, 3, "before").await;

    let (cold, _keep) = fresh_index_cache().await;
    write(&cache, 7, &block(70));
    write(&cache, 2, &block(71));
    flush(&cache, &cs, &cold, &vm).await;
    assert_stored(&cs, &vm, 4, "flush through an empty index cache").await;
}

/// If an index cannot be loaded the flush fails, as any flush failing on the
/// object store does, and the known count is untouched.
#[tokio::test]
async fn flush_fails_instead_of_dropping_the_count_when_an_index_is_unavailable() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = create_test_cache(&dir, "sb-missing", Arc::clone(&s3)).await;
    for i in 0..3 {
        write(&cache, i, &block(i as u8 + 1));
    }
    flush(&cache, &cs, &pic, &vm).await;
    let before = vm.read().chunks[&0].clone();
    assert_eq!(before.live_blocks, Some(3));

    s3.delete(&pack_key(0, before.packs[0])).await.unwrap();
    let (cold, _keep) = fresh_index_cache().await;
    write(&cache, 5, &block(50));
    assert!(cache.flush_to_s3(&cs, &cold, &vm).await.is_err(), "the flush must fail, not guess");
    assert_eq!(vm.read().chunks[&0], before, "pack list and known count untouched");
}

/// The figure the API gives is exact while chunks are unknown (computed from
/// the indexes on demand), and absent, never smaller, when it cannot be.
#[tokio::test]
async fn stored_bytes_is_exact_or_absent_never_partial() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = create_test_cache(&dir, "sb-exact", Arc::clone(&s3)).await;
    for i in 0..3 {
        write(&cache, i, &block(i as u8 + 1));
    }
    write(&cache, BPC + 1, &block(9));
    write(&cache, BPC + 2, &block(10));
    flush(&cache, &cs, &pic, &vm).await;
    let truth = oracle_live_blocks(&cs, &vm).await;
    assert_eq!(truth, 5);

    for e in vm.write().chunks.values_mut() {
        e.live_blocks = None;
    }
    assert_eq!(vm.read().stored_bytes(), None);

    // Only chunk 1 unknown: the known chunk 0 must not be returned as a partial sum.
    let c0 = vm.read().chunks[&0].packs.clone();
    vm.write().chunks.get_mut(&0).unwrap().live_blocks = Some(3);
    let (cold, _keep) = fresh_index_cache().await;
    assert_eq!(
        stored_bytes_exact(&vm, &cs, &cold).await,
        Some(truth * BS as u64),
        "unknown chunks are computed on demand from their indexes"
    );

    // Unknown again, one index unreachable: no figure, and nothing smaller.
    for e in vm.write().chunks.values_mut() {
        e.live_blocks = None;
    }
    s3.delete(&pack_key(0, c0[0])).await.unwrap();
    let (cold2, _keep2) = fresh_index_cache().await;
    assert_eq!(stored_bytes_exact(&vm, &cs, &cold2).await, None);
    assert_eq!(vm.read().stored_bytes(), None);
}

/// Object store that counts concurrent GETs of pack objects (with a delay so
/// overlap is visible) and records the high-water mark.
#[derive(Debug)]
struct GateStore {
    inner: InMemory,
    inflight: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Display for GateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GateStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for GateStore {
    async fn put_opts(&self, l: &object_store::path::Path, p: object_store::PutPayload, o: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &object_store::path::Path, o: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &object_store::path::Path, o: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self.inflight.fetch_add(1, SeqCst) + 1;
        self.peak.fetch_max(now, SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        let r = self.inner.get_opts(l, o).await;
        self.inflight.fetch_sub(1, SeqCst);
        r
    }
    async fn delete(&self, l: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.delete(l).await
    }
    fn list(&self, p: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy(&self, f: &object_store::path::Path, t: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.copy(f, t).await
    }
    async fn copy_if_not_exists(&self, f: &object_store::path::Path, t: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(f, t).await
    }
}

/// Many exports with unknown counts, computed at once, never put more than
/// MAX_CONCURRENT_CHUNKS index fetches in flight process-wide.
#[tokio::test]
async fn backfill_across_many_exports_is_globally_bounded() {
    use glidefs::block::live_count::MAX_CONCURRENT_CHUNKS;
    let gate = Arc::new(GateStore {
        inner: InMemory::new(),
        inflight: Default::default(),
        peak: Default::default(),
    });
    let s3: Arc<dyn ObjectStore> = gate.clone();
    let dir = TempDir::new().unwrap();
    let (cache, cs, pic, vm, _cc, _m) = create_test_cache(&dir, "sb-gate", s3).await;
    // 2 chunks only fit the 256 MiB test device, so reuse one manifest as 12 "exports".
    for i in 0..4 {
        write(&cache, i, &block(i as u8 + 1));
        write(&cache, BPC + i, &block(i as u8 + 9));
        flush(&cache, &cs, &pic, &vm).await;
    }
    let truth = oracle_live_blocks(&cs, &vm).await;
    let exports: Vec<Vm> = (0..12)
        .map(|_| {
            let mut m = vm.read().clone();
            for e in m.chunks.values_mut() {
                e.live_blocks = None;
            }
            Arc::new(parking_lot::RwLock::new(m))
        })
        .collect();
    let (cold, _keep) = fresh_index_cache().await;
    gate.peak.store(0, std::sync::atomic::Ordering::SeqCst);

    let results = futures::future::join_all(exports.iter().map(|m| stored_bytes_exact(m, &cs, &cold))).await;
    assert!(results.iter().all(|r| *r == Some(truth * BS as u64)));
    let peak = gate.peak.load(std::sync::atomic::Ordering::SeqCst);
    assert!(peak >= 2, "the test must actually overlap fetches (peak {peak})");
    assert!(peak <= MAX_CONCURRENT_CHUNKS, "peak {peak} index fetches exceeds the global cap {MAX_CONCURRENT_CHUNKS}");
}
