//! GC cost estimation benchmark.
//!
//! Measures exact S3 API call counts (LIST, GET, DELETE) and wall-clock time
//! for GC reconciliation at various fleet scales, using an in-process InMemory
//! object store (zero network latency).
//!
//! Run with:
//!   cargo bench --features test-utils --bench gc

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use glidefs::block::content_store::ContentStore;
use glidefs::block::volume_manifest::VolumeManifest;
use glidefs::cli::gc::{new_gc_state_for_test, reconcile_prefix_for_test};

// ---------------------------------------------------------------------------
// CountingStore: InMemory wrapper that tracks LIST / GET / DELETE call counts.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ApiCounts {
    lists: AtomicU64,
    gets: AtomicU64,
    deletes: AtomicU64,
}

impl ApiCounts {
    fn reset(&self) {
        self.lists.store(0, Ordering::Relaxed);
        self.gets.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.lists.load(Ordering::Relaxed),
            self.gets.load(Ordering::Relaxed),
            self.deletes.load(Ordering::Relaxed),
        )
    }
}

struct CountingStore {
    inner: InMemory,
    api: ApiCounts,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            api: ApiCounts::default(),
        }
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore")
    }
}

impl fmt::Debug for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        self.api.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjectPath) -> OsResult<()> {
        self.api.deletes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.api.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
        self.api.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// ---------------------------------------------------------------------------
// Scenario definition and results
// ---------------------------------------------------------------------------

struct Scenario {
    name: &'static str,
    /// Number of exports (independent prefixes, GC'd with parallelism=16).
    exports: usize,
    /// Live packs per export (referenced by the live manifest).
    live_packs_per_export: usize,
    /// Orphan packs per export (in S3 but not in any manifest or snapshot).
    dead_packs_per_export: usize,
    /// Snapshot manifests per export (each references all live packs).
    snapshots_per_export: usize,
}

struct Result {
    elapsed_ms: f64,
    lists: u64,
    gets: u64,
    deletes: u64,
}

impl Result {
    /// AWS S3 Standard cost estimate per GC run.
    /// LIST: $0.005/1K, GET: $0.0004/1K, DELETE: free.
    fn cost_usd(&self) -> f64 {
        (self.lists as f64 / 1_000.0) * 0.005
            + (self.gets as f64 / 1_000.0) * 0.0004
    }
}

// ---------------------------------------------------------------------------
// Seed + run
// ---------------------------------------------------------------------------

const DEVICE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
const BLOCK_SIZE: u32 = 128 * 1024;           // 128 KB

async fn run_scenario(scenario: &Scenario) -> Result {
    let store = Arc::new(CountingStore::new());

    // ---- Seed phase (not counted) ----
    for exp_idx in 0..scenario.exports {
        let export_name = format!("vm{exp_idx}");
        let prefix = format!("bench/exports/{export_name}");
        let cs = ContentStore::new(Arc::clone(&store) as _, &prefix);

        let mut vm = VolumeManifest::new(DEVICE_SIZE, BLOCK_SIZE);
        let chunk_idx = 0u32;

        // Live packs — referenced by manifest.
        for i in 0..scenario.live_packs_per_export {
            let pack_id = ((exp_idx as u64) << 24) | (i as u64);
            cs.put_chunk_pack(chunk_idx, pack_id, vec![42u8; 64])
                .await
                .unwrap();
            vm.append_pack(chunk_idx, pack_id);
        }
        cs.put_manifest(&export_name, vm.serialize().unwrap(), None)
            .await
            .unwrap();

        // Snapshots — each references all live packs.
        for snap_seq in 0..scenario.snapshots_per_export {
            cs.put_snapshot(&export_name, snap_seq as u64, vm.serialize().unwrap())
                .await
                .unwrap();
        }

        // Orphan packs — not referenced by any manifest or snapshot.
        for i in 0..scenario.dead_packs_per_export {
            let pack_id = 0xDEAD_0000_0000_0000u64 | ((exp_idx as u64) << 16) | (i as u64);
            cs.put_chunk_pack(chunk_idx, pack_id, vec![0u8; 64])
                .await
                .unwrap();
        }
    }

    // ---- GC phase (counted) ----
    store.api.reset();
    let t0 = Instant::now();

    let reports: Vec<_> = futures::stream::iter(0..scenario.exports)
        .map(|exp_idx| {
            let store = Arc::clone(&store);
            async move {
                let export_name = format!("vm{exp_idx}");
                let prefix = format!("bench/exports/{export_name}");
                let cs = ContentStore::new(Arc::clone(&store) as _, &prefix);
                let mut state = new_gc_state_for_test();
                // Zero grace period: newly-discovered dead packs are immediately eligible.
                reconcile_prefix_for_test(
                    &cs,
                    &mut state,
                    std::time::Duration::ZERO,
                    100_000,
                    false,
                )
                .await
                .unwrap()
            }
        })
        .buffer_unordered(16)
        .collect()
        .await;

    let elapsed_ms = t0.elapsed().as_secs_f64() * 1_000.0;
    let (lists, gets, deletes) = store.api.snapshot();
    let deleted: usize = reports.iter().map(|r| r.deleted_count()).sum();
    assert_eq!(
        deleted,
        scenario.exports * scenario.dead_packs_per_export,
        "scenario '{}': expected {} deletions, got {}",
        scenario.name,
        scenario.exports * scenario.dead_packs_per_export,
        deleted,
    );

    Result { elapsed_ms, lists, gets, deletes }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let scenarios = [
        Scenario {
            name: "tiny",
            exports: 10,
            live_packs_per_export: 10,
            dead_packs_per_export: 2,
            snapshots_per_export: 2,
        },
        Scenario {
            name: "small",
            exports: 50,
            live_packs_per_export: 25,
            dead_packs_per_export: 5,
            snapshots_per_export: 3,
        },
        Scenario {
            name: "medium",
            exports: 200,
            live_packs_per_export: 50,
            dead_packs_per_export: 10,
            snapshots_per_export: 5,
        },
        Scenario {
            name: "large",
            exports: 500,
            live_packs_per_export: 100,
            dead_packs_per_export: 20,
            snapshots_per_export: 10,
        },
    ];

    // Run all scenarios and collect results.
    let mut results: Vec<(&Scenario, Result)> = Vec::new();
    for s in &scenarios {
        let r = run_scenario(s).await;
        results.push((s, r));
    }

    // ---- Summary table ----
    println!();
    println!("GC Cost Estimate  [InMemory S3, no network, parallelism=16]");
    let sep = "─".repeat(102);
    println!("{sep}");
    println!(
        "{:<8}  {:>7}  {:>9}  {:>8}  {:>9}  {:>7}  {:>7}  {:>8}  {:>10}  {:>11}",
        "Scenario", "Exports", "Live/exp", "Dead/exp", "Snaps/exp",
        "LIST", "GET", "DELETE", "Time (ms)", "Cost (USD)"
    );
    println!("{sep}");
    for (s, r) in &results {
        println!(
            "{:<8}  {:>7}  {:>9}  {:>8}  {:>9}  {:>7}  {:>7}  {:>8}  {:>10.1}  {:>11}",
            s.name,
            s.exports,
            s.live_packs_per_export,
            s.dead_packs_per_export,
            s.snapshots_per_export,
            r.lists,
            r.gets,
            r.deletes,
            r.elapsed_ms,
            format!("${:.6}", r.cost_usd()),
        );
    }
    println!("{sep}");

    // ---- Per-export call breakdown ----
    println!();
    println!("Per-export call formula:");
    println!("  LIST   = 3  (one each: manifests/ + chunks/ + snapshots/)");
    println!("  GET    = N_manifests + N_snapshots  (all snapshots checked when orphans survive pass 2)");
    println!("  DELETE = N_dead_packs");
    println!();

    // Verify by running a single-export scenario.
    let probe = Scenario {
        name: "1-export probe",
        exports: 1,
        live_packs_per_export: 50,
        dead_packs_per_export: 10,
        snapshots_per_export: 5,
    };
    let pr = run_scenario(&probe).await;
    println!(
        "Single-export probe (50 live, 10 dead, 5 snaps): LIST={} GET={} DELETE={}",
        pr.lists, pr.gets, pr.deletes,
    );
    println!();

    // ---- Scale projection (extrapolated from per-export formula) ----
    // Per-export rates from probe (1 manifest, 5 snapshots, 10 dead packs):
    //   LIST=3, GET=6, DELETE=10
    // These scale linearly with export count.
    // Real S3 notes:
    //   - discover_s3_prefixes paginates via list_with_offset, jumping past each
    //     export subtree, so it scales past the 1000-entries-per-page limit.
    //   - delete budget is a single shared AtomicUsize(max_deletes) claimed across
    //     all prefixes, so it caps total deletes globally (never collapses to 0).
    //   - outer loop parallelism is buffer_unordered(16), so wall time ≈
    //     ceil(N_exports/16) * per_prefix_latency_ms.
    struct Projection {
        exports: u64,
        manifests_per: u64,
        snapshots_per: u64,
        dead_per: u64,
    }
    let projections = [
        Projection { exports:      1_000, manifests_per: 1, snapshots_per: 5, dead_per: 10 },
        Projection { exports:     10_000, manifests_per: 1, snapshots_per: 5, dead_per: 10 },
        Projection { exports:    100_000, manifests_per: 1, snapshots_per: 5, dead_per: 10 },
        Projection { exports:  1_000_000, manifests_per: 1, snapshots_per: 5, dead_per: 10 },
    ];

    // Per-prefix latency: 3 serial phases (manifests list+fetch, chunks list, snapshots list+fetch).
    // Each phase is ≤16-wide parallel GETs. With 35ms RTT per batch:
    //   phase 1: ceil(manifests/16) × 35ms
    //   phase 2: 0ms   (listing only, no GETs)
    //   phase 3: ceil(snapshots/16) × 35ms
    // Plus 3 LIST calls × ~10ms each = 30ms fixed.
    let per_prefix_latency_ms = |m: u64, s: u64| -> f64 {
        let list_overhead = 30.0_f64;
        let get_batches = ((m as f64 / 16.0).ceil() + (s as f64 / 16.0).ceil()) as f64;
        list_overhead + get_batches * 35.0
    };

    println!("Scale projection  [extrapolated, 1 manifest + 5 snaps + 10 dead per export]");
    let sep2 = "─".repeat(102);
    println!("{sep2}");
    println!(
        "{:<12}  {:>10}  {:>10}  {:>10}  {:>12}  {:>12}  {:>16}",
        "Exports", "LIST", "GET", "DELETE", "Cost (USD)", "Time@par=16", "Time@par=256"
    );
    println!("{sep2}");
    for p in &projections {
        let lists  = 3 * p.exports + (p.exports as f64 / 1000.0).ceil() as u64; // +discovery pages
        let gets   = (p.manifests_per + p.snapshots_per) * p.exports;
        let dels   = p.dead_per * p.exports;
        let cost   = (lists as f64 / 1_000.0) * 0.005 + (gets as f64 / 1_000.0) * 0.0004;
        let pp_ms  = per_prefix_latency_ms(p.manifests_per, p.snapshots_per);
        let t16_s  = (p.exports as f64 / 16.0).ceil()  * pp_ms / 1_000.0;
        let t256_s = (p.exports as f64 / 256.0).ceil() * pp_ms / 1_000.0;
        let fmt_time = |secs: f64| -> String {
            if secs < 60.0       { format!("{:.0}s",       secs) }
            else if secs < 3600.0 { format!("{:.1}min",    secs / 60.0) }
            else                  { format!("{:.1}hr",     secs / 3600.0) }
        };
        println!(
            "{:<12}  {:>10}  {:>10}  {:>10}  {:>12}  {:>12}  {:>16}",
            format!("{}", p.exports),
            lists, gets, dels,
            format!("${:.2}", cost),
            fmt_time(t16_s),
            fmt_time(t256_s),
        );
    }
    println!("{sep2}");
    println!();
    println!("Pricing: LIST $0.005/1K, GET $0.0004/1K, DELETE free (AWS S3 Standard).");
}
