//! A volume PUT that is cut off between `create_export` and `register_device` must not strand the
//! export. Found on a recovered node: instd was SIGTERMed with two volume PUTs in flight, glidefs
//! logged `connection closed before message completed`, each export stayed in the map with no block
//! device, and every later PUT answered 200 with `device: null`, so the VMs never re-forked.
//!
//! Run with: `cargo test --features test-utils --test integration attach_heal`

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as StoreResult,
};
use tempfile::TempDir;

use glidefs::block::cache::SimpleBlockCache;
use glidefs::block::router::{DEFAULT_MANIFEST_CACHE_BYTES, ExportRouter, RouterConfig};
use glidefs::config::ExportConfig;


/// An in-memory store whose `export.json` PUT takes `delay`: the step between `create_export` and the
/// end of the create, where hyper drops the handler when the client's connection closes.
#[derive(Debug)]
struct SlowIndexStore {
    inner: InMemory,
    delay: Duration,
}

impl std::fmt::Display for SlowIndexStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowIndexStore")
    }
}

#[async_trait]
impl ObjectStore for SlowIndexStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> StoreResult<PutResult> {
        if location.as_ref().ends_with("export.json") {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(l, o).await
    }
    async fn delete(&self, l: &Path) -> StoreResult<()> {
        self.inner.delete(l).await
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy(&self, f: &Path, t: &Path) -> StoreResult<()> {
        self.inner.copy(f, t).await
    }
    async fn copy_if_not_exists(&self, f: &Path, t: &Path) -> StoreResult<()> {
        self.inner.copy_if_not_exists(f, t).await
    }
}

async fn router(dir: &TempDir, s3: Arc<dyn ObjectStore>) -> Arc<ExportRouter> {
    Arc::new(
        ExportRouter::new(RouterConfig {
            object_store: s3,
            db_path: "test".to_string(),
            cache_dir: dir.path().to_path_buf(),
            block_size: 128 * 1024,
            clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
            pack_index_cache: None,
            wal_sync: false,
            max_s3_uploads: 0,
            max_s3_downloads: 0,
            default_flush_threshold: 500,
            ublk_nr_queues: 1,
            ublk_max_devices: None,
            nbd_dead_conn_timeout: 0,
            max_exports: 100,
            manifest_cache_bytes: DEFAULT_MANIFEST_CACHE_BYTES,
            profile: None,
        })
        .await
        .unwrap(),
    )
}

fn put(name: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::PUT)
        .uri(format!("/api/exports/{name}"))
        .body(Full::new(Bytes::from(r#"{"size_gb": 1}"#)))
        .unwrap()
}

/// The client goes away right after sending the PUT: the handler future is polled once, then
/// dropped, as hyper does when the connection closes. The create must still finish: the export's
/// definition reaches S3, which is the step the dropped request used to skip.
#[tokio::test]
async fn a_put_whose_client_goes_away_still_completes() {
    let s3 = Arc::new(SlowIndexStore { inner: InMemory::new(), delay: Duration::from_millis(300) });
    let dir = TempDir::new().unwrap();
    let router = router(&dir, s3.clone() as _).await;

    // Poll the PUT until it is waiting on the slow index write (the export exists in the map), then
    // drop it: the client has gone.
    let mut fut = Box::pin(glidefs::block::api::handle_request_for_test(Arc::clone(&router), put("ghost")));
    let _ = futures::poll!(fut.as_mut());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = futures::poll!(fut.as_mut());
    drop(fut);

    let key = Path::from("test/exports/ghost/export.json");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if s3.head(&key).await.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the export was left half-created: export.json never reached S3 after the client dropped the PUT"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(router.get_export_info("ghost").await.is_some(), "the export stays attached");
}

/// An export attached with no device (a process killed mid-create) is healed by the next PUT: it
/// answers with a device, or fails naming the missing device. It never answers 200 with
/// `device: null`, which is the answer that stranded the VMs.
#[tokio::test]
async fn a_put_of_an_attached_export_with_no_device_registers_one() {
    let s3 = Arc::new(InMemory::new());
    let dir = TempDir::new().unwrap();
    let router = router(&dir, s3.clone() as _).await;

    // What a create cut off after `create_export` leaves behind.
    router
        .create_export(
            ExportConfig {
                name: "half".to_string(),
                size_gb: 1.0,
                s3_prefix: None,
                block_size: None,
                flush_threshold: None,
                flush_mode: None,
                transport: None,
                compaction_cooldown: None,
                source: None,
            },
            false,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(router.get_export_info("half").await.unwrap().device.is_none(), "set-up: no device yet");

    let resp = glidefs::block::api::handle_request_for_test(Arc::clone(&router), put("half"))
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    if status == StatusCode::OK {
        assert!(body["device"].is_string(), "200 without a device is the stranding answer: {body}");
    } else {
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert!(body.to_string().contains("no block device"), "{body}");
    }
}
