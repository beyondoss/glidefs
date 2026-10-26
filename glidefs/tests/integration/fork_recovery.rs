//! Fork lineage must survive a crash.
//!
//! Incident (c5n.metal, ~400 VM disks): glidefs was OOM-killed and the node
//! rebooted. Every fork export (VM disks cloned from a base) came back with
//! `manifest=None`: the fork's volume manifest (a copy of its base's) lived
//! only in memory, so the restart recovered an EMPTY manifest and every read
//! of base-resident data failed (`I/O error, dev ublkbN, sector 512 READ`).
//!
//! These tests drive the real `ExportRouter` through that exact sequence:
//! fork a base, write, "crash" (copy the cache dir as it stands on disk, with
//! no clean shutdown), restart a fresh router on the copy + same object store,
//! and run the server's boot-recovery sequence.
//!
//! Run with: `cargo test --features test-utils --test integration fork_recovery`

use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tempfile::TempDir;

use glidefs::block::cache::{BlockCache, SimpleBlockCache};
use glidefs::block::router::{DEFAULT_MANIFEST_CACHE_BYTES, ExportRouter, RouterConfig, RouterError};
use glidefs::config::ExportConfig;

const BS: usize = 128 * 1024;

async fn router(cache_dir: &Path, s3: Arc<dyn ObjectStore>) -> ExportRouter {
    let clean_cache: Arc<dyn BlockCache> = Arc::new(SimpleBlockCache::new(64 * 1024 * 1024));
    ExportRouter::new(RouterConfig {
        object_store: s3,
        db_path: "test".to_string(),
        cache_dir: cache_dir.to_path_buf(),
        block_size: BS,
        clean_cache,
        pack_index_cache: None,
        wal_sync: false,
        max_s3_uploads: 0,
        max_s3_downloads: 0,
        // Manual flush: nothing reaches S3 behind the test's back, so the
        // "crash" below happens with the fork's writes still only in the WAL.
        default_flush_threshold: 0,
        ublk_nr_queues: 1,
        nbd_dead_conn_timeout: 0,
        max_exports: 100,
        manifest_cache_bytes: DEFAULT_MANIFEST_CACHE_BYTES,
        profile: None,
    })
    .await
    .expect("router")
}

fn cfg(name: &str, pool: Option<&str>, source: Option<&str>) -> ExportConfig {
    ExportConfig {
        name: name.to_string(),
        size_gb: 0.01,
        s3_prefix: pool.map(str::to_string),
        block_size: None,
        flush_threshold: None,
        flush_mode: None,
        transport: None,
        compaction_cooldown: None,
        source: source.map(str::to_string),
    }
}

/// Power-loss copy: the cache dir exactly as the dead process left it.
fn crash_copy(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
}

/// What a node-local boot does: the device map names this node's exports,
/// then `create_export(config, false, None, None)` for each (cli/server.rs).
async fn boot_recover(r: &ExportRouter, cache_dir: &Path, names: &[&str]) -> Vec<(String, Result<(), RouterError>)> {
    let map: serde_json::Map<String, serde_json::Value> =
        names.iter().map(|n| (n.to_string(), 1.into())).collect();
    std::fs::write(cache_dir.join("ublk_devices.json"), serde_json::to_vec(&map).unwrap()).unwrap();
    let mut out = Vec::new();
    for config in r.discover_local_exports().await.unwrap() {
        let name = config.name.clone();
        out.push((name, r.create_export(config, false, None, None).await));
    }
    out
}

/// Base with a flushed block, a fork of it (published via save_export like the
/// API does), and an unflushed write on the fork. Returns the crash-copied
/// cache dir plus the shared object store.
async fn forked_then_crashed(base_data: &[u8], fork_data: &[u8]) -> (TempDir, Arc<dyn ObjectStore>) {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let r = router(dir.path(), Arc::clone(&s3)).await;

    r.create_export(cfg("base", None, None), false, None, None).await.unwrap();
    r.get_handler("base").await.unwrap().write(0, base_data, false).await.unwrap();
    r.snapshot_export("base", None).await.unwrap();

    let fork = cfg("fork1", Some("base"), Some("volume:base"));
    r.create_export(fork.clone(), false, Some("base"), None).await.unwrap();
    r.save_export(&fork).await.unwrap();
    // Unflushed write to a different block than the base's.
    r.get_handler("fork1").await.unwrap().write(BS as u64, fork_data, false).await.unwrap();

    // CRASH: no shutdown, no drain, no snapshot. Take the disk as it is.
    let crashed = TempDir::new().unwrap();
    crash_copy(dir.path(), crashed.path());
    std::mem::forget(r); // the dead process never runs destructors
    (crashed, s3)
}

/// The incident: a crashed fork must come back with its base's data AND its
/// own unflushed writes (or refuse), never "recovered" and unreadable.
#[tokio::test]
async fn fork_recovers_with_base_lineage_and_dirty_data_after_crash() {
    let base_data = vec![0xBA; BS];
    let fork_data = vec![0xF0; BS];
    let (crashed, s3) = forked_then_crashed(&base_data, &fork_data).await;

    let r = router(crashed.path(), s3).await;
    let results = boot_recover(&r, crashed.path(), &["fork1"]).await;
    let (name, res) = &results[0];
    assert_eq!(name, "fork1");
    res.as_ref().unwrap_or_else(|e| panic!("fork must recover, or be refused with a clear error: {e}"));

    let h = r.get_handler("fork1").await.expect("recovered export has a handler");
    // Base-resident block: the lineage. Pre-fix this read back as zeros
    // (manifest=None, empty block map) -- the incident's failing reads.
    let got = h.read(0, BS as u32).await.unwrap();
    assert_eq!(got.as_ref(), &base_data[..], "fork lost its base lineage across the crash");
    // The fork's own unflushed write survives via the WAL.
    let got = h.read(BS as u64, BS as u32).await.unwrap();
    assert_eq!(got.as_ref(), &fork_data[..], "fork lost its unflushed write");
}

/// If the lineage cannot be restored (fork manifest gone -- e.g. an export
/// created by a build that kept it in memory only), the export must be
/// REFUSED so instd re-forks it, never silently recovered empty.
#[tokio::test]
async fn fork_with_lost_manifest_is_refused_not_recovered_empty() {
    let (crashed, s3) = forked_then_crashed(&[0xBA; BS], &[0xF0; BS]).await;

    // Simulate the pre-fix world / S3 loss: the fork's own manifest is absent.
    use futures::TryStreamExt;
    let metas: Vec<_> = s3.list(None).try_collect().await.unwrap();
    for m in metas.iter().filter(|m| m.location.as_ref().ends_with("manifests/fork1")) {
        s3.delete(&m.location).await.unwrap();
    }

    let r = router(crashed.path(), s3).await;
    let results = boot_recover(&r, crashed.path(), &["fork1"]).await;
    let err = results[0].1.as_ref().expect_err("a fork with no manifest must not come up");
    assert!(matches!(err, RouterError::LineageLost { .. }), "unexpected error: {err}");
    assert!(r.get_handler("fork1").await.is_none(), "refused export must not serve I/O");
}

/// A blank (never-forked, never-flushed) volume legitimately has no manifest
/// yet: it must still recover, with its WAL data.
#[tokio::test]
async fn blank_volume_without_manifest_still_recovers() {
    let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let r = router(dir.path(), Arc::clone(&s3)).await;
    let blank = cfg("blank", None, None);
    r.create_export(blank.clone(), false, None, None).await.unwrap();
    r.save_export(&blank).await.unwrap();
    r.get_handler("blank").await.unwrap().write(0, &[0x42; BS], false).await.unwrap();
    let crashed = TempDir::new().unwrap();
    crash_copy(dir.path(), crashed.path());
    std::mem::forget(r);

    let r2 = router(crashed.path(), s3).await;
    let results = boot_recover(&r2, crashed.path(), &["blank"]).await;
    results[0].1.as_ref().unwrap();
    let got = r2.get_handler("blank").await.unwrap().read(0, BS as u32).await.unwrap();
    assert_eq!(got.as_ref(), &[0x42; BS][..]);
}
