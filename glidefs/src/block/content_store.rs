//! Content-addressed S3 storage for packs and manifests.
//!
//! Thin wrapper around `ObjectStore` providing typed PUT/GET for packs
//! (shared across exports) and manifests (per-export).

use crate::circuit_breaker::CircuitBreaker;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, UpdateVersion};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{debug, instrument};

use super::manifest::{manifest_s3_key, snapshot_s3_key};

#[derive(Error, Debug)]
pub enum ContentStoreError {
    #[error("S3 operation failed: {0}")]
    ObjectStore(object_store::Error),

    #[error("S3 precondition failed (ETag mismatch): {0}")]
    PreconditionFailed(object_store::Error),

    #[error("S3 circuit breaker is open (service temporarily unavailable)")]
    CircuitOpen,

    #[error("S3 concurrency semaphore closed")]
    SemaphoreClosed,
}

impl From<object_store::Error> for ContentStoreError {
    fn from(e: object_store::Error) -> Self {
        match e {
            e @ object_store::Error::Precondition { .. } => Self::PreconditionFailed(e),
            e => Self::ObjectStore(e),
        }
    }
}

/// Returns true for errors that indicate S3 connectivity failure (network,
/// timeout, 5xx). NotFound, Precondition, etc. are valid S3 responses and
/// prove the backend is reachable — everything else is assumed to be a
/// connectivity problem.
fn is_connectivity_error(e: &object_store::Error) -> bool {
    !matches!(
        e,
        object_store::Error::NotFound { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. }
            | object_store::Error::NotSupported { .. }
    )
}

pub struct ContentStore {
    object_store: Arc<dyn ObjectStore>,
    base_path: String,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    /// Global S3 upload concurrency limit shared across all exports (background flush).
    upload_semaphore: Option<Arc<Semaphore>>,
    /// Global S3 download concurrency limit shared across all exports (read path).
    download_semaphore: Option<Arc<Semaphore>>,
}

impl ContentStore {
    pub fn new(object_store: Arc<dyn ObjectStore>, base_path: &str) -> Self {
        Self {
            object_store,
            base_path: base_path.trim_end_matches('/').to_string(),
            circuit_breaker: None,
            upload_semaphore: None,
            download_semaphore: None,
        }
    }

    /// Attach a shared circuit breaker for S3 calls.
    pub fn with_circuit_breaker(mut self, cb: Arc<CircuitBreaker>) -> Self {
        self.circuit_breaker = Some(cb);
        self
    }

    /// Attach a shared semaphore to limit concurrent S3 uploads across all exports.
    pub fn with_upload_semaphore(mut self, sem: Arc<Semaphore>) -> Self {
        self.upload_semaphore = Some(sem);
        self
    }

    /// Attach a shared semaphore to limit concurrent S3 downloads across all exports.
    pub fn with_download_semaphore(mut self, sem: Arc<Semaphore>) -> Self {
        self.download_semaphore = Some(sem);
        self
    }

    /// Check circuit breaker before an S3 call.
    #[inline]
    fn check_circuit(&self) -> Result<(), ContentStoreError> {
        if let Some(cb) = &self.circuit_breaker {
            cb.allow().map_err(|_| ContentStoreError::CircuitOpen)?;
        }
        Ok(())
    }

    /// Record the outcome of an S3 call to the circuit breaker.
    #[inline]
    fn record_s3_result<T>(&self, result: &Result<T, object_store::Error>) {
        if let Some(cb) = &self.circuit_breaker {
            match result {
                Ok(_) => cb.record_success(),
                Err(e) if is_connectivity_error(e) => cb.record_failure(),
                Err(_) => cb.record_success(), // NotFound/Precondition = S3 is reachable
            }
        }
    }

    /// Record an S3 error from a streaming list operation to the circuit breaker.
    #[inline]
    fn record_s3_list_error(&self, error: &object_store::Error) {
        if let Some(cb) = &self.circuit_breaker {
            if is_connectivity_error(error) {
                cb.record_failure();
            } else {
                cb.record_success();
            }
        }
    }

    /// Upload a manifest to S3. Returns the S3 ETag if the backend provides one.
    ///
    /// When `expected_etag` is `Some`, uses a conditional PUT (`If-Match`) to
    /// prevent overwriting a manifest that was modified concurrently. Returns
    /// [`ContentStoreError::PreconditionFailed`] on ETag mismatch.
    #[instrument(skip(self, data), fields(name = %name, size = data.len()))]
    pub async fn put_manifest(
        &self,
        name: &str,
        data: Vec<u8>,
        expected_etag: Option<&str>,
    ) -> Result<Option<String>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, manifest_s3_key(name));
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let opts = match expected_etag {
            Some(etag) => PutOptions::from(PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            })),
            None => PutOptions::default(),
        };
        let result = self.object_store.put_opts(&path, payload, opts).await;
        self.record_s3_result(&result);
        let put_result = result?;
        debug!("uploaded manifest");
        Ok(put_result.e_tag)
    }

    /// Delete a manifest from S3 (idempotent).
    #[instrument(skip(self), fields(name = %name))]
    pub async fn delete_manifest(&self, name: &str) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, manifest_s3_key(name));
        let path = ObjectPath::from(key);
        let result = self.object_store.delete(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Check if a manifest exists in S3 (HEAD request, no data transfer).
    #[instrument(skip(self), fields(name = %name))]
    pub async fn head_manifest(&self, name: &str) -> Result<bool, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, manifest_s3_key(name));
        let path = ObjectPath::from(key);
        let result = self.object_store.head(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Download a manifest from S3. Returns `(data, etag)` or None if not found.
    #[allow(clippy::type_complexity)]
    #[instrument(skip(self), fields(name = %name))]
    pub async fn get_manifest(
        &self,
        name: &str,
    ) -> Result<Option<(Vec<u8>, Option<String>)>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, manifest_s3_key(name));
        let path = ObjectPath::from(key);
        let result = self.object_store.get(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(response) => {
                let etag = response.meta.e_tag.clone();
                let bytes = response.bytes().await?;
                Ok(Some((bytes.to_vec(), etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // =========================================================================
    // Snapshot operations (versioned manifests)
    // =========================================================================

    /// Upload a versioned snapshot manifest to S3.
    #[instrument(skip(self, data), fields(name = %name, sequence, size = data.len()))]
    pub async fn put_snapshot(
        &self,
        name: &str,
        sequence: u64,
        data: Vec<u8>,
    ) -> Result<Option<String>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, snapshot_s3_key(name, sequence));
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await;
        self.record_s3_result(&result);
        let put_result = result?;
        debug!(sequence, "uploaded snapshot manifest");
        Ok(put_result.e_tag)
    }

    /// Download a versioned snapshot manifest from S3. Returns None if not found.
    #[instrument(skip(self), fields(name = %name, sequence))]
    pub async fn get_snapshot(
        &self,
        name: &str,
        sequence: u64,
    ) -> Result<Option<Vec<u8>>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, snapshot_s3_key(name, sequence));
        let path = ObjectPath::from(key);
        let result = self.object_store.get(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(response) => {
                let bytes = response.bytes().await?;
                Ok(Some(bytes.to_vec()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List snapshot sequence numbers for an export, sorted ascending.
    #[instrument(skip(self), fields(name = %name))]
    pub async fn list_snapshots(&self, name: &str) -> Result<Vec<u64>, ContentStoreError> {
        self.check_circuit()?;
        let prefix_str = format!("{}/snapshots/{}/", self.base_path, name);
        let prefix = ObjectPath::from(prefix_str);
        let mut sequences = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(result) = stream.next().await {
            let meta = match result {
                Ok(m) => m,
                Err(e) => {
                    self.record_s3_list_error(&e);
                    return Err(e.into());
                }
            };
            if let Some(filename) = meta.location.filename() && let Ok(seq) = filename.parse::<u64>() {
                sequences.push(seq);
            }
        }
        if let Some(cb) = &self.circuit_breaker {
            cb.record_success();
        }
        sequences.sort_unstable();
        Ok(sequences)
    }

    /// Delete a versioned snapshot manifest from S3 (idempotent).
    #[instrument(skip(self), fields(name = %name, sequence))]
    pub async fn delete_snapshot(
        &self,
        name: &str,
        sequence: u64,
    ) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, snapshot_s3_key(name, sequence));
        let path = ObjectPath::from(key);
        let result = self.object_store.delete(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete all snapshot manifests for an export (idempotent, best-effort).
    ///
    /// Streams the S3 list directly into concurrent deletes (`buffer_unordered`)
    /// so listing and deletion overlap without materializing the full snapshot list.
    #[instrument(skip(self), fields(name = %name))]
    pub async fn delete_all_snapshots(&self, name: &str) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let prefix_str = format!("{}/snapshots/{}/", self.base_path, name);
        let prefix = ObjectPath::from(prefix_str);
        let store = Arc::clone(&self.object_store);
        let cb = self.circuit_breaker.clone();

        self.object_store
            .list(Some(&prefix))
            .map(move |result| {
                let store = Arc::clone(&store);
                let cb = cb.clone();
                async move {
                    let meta = match result {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to list snapshot during delete_all (continuing)");
                            return;
                        }
                    };
                    let seq_str = meta.location.filename().unwrap_or_default().to_string();
                    let result = store.delete(&meta.location).await;
                    if let Some(cb) = &cb {
                        match &result {
                            Ok(_) => cb.record_success(),
                            Err(e) if is_connectivity_error(e) => cb.record_failure(),
                            Err(_) => cb.record_success(),
                        }
                    }
                    if let Err(e) = &result
                        && !matches!(e, object_store::Error::NotFound { .. })
                    {
                        tracing::warn!(
                            snapshot = %seq_str, error = %e,
                            "failed to delete snapshot during cleanup (continuing)"
                        );
                    }
                }
            })
            .buffer_unordered(16)
            .for_each(|()| async {})
            .await;

        Ok(())
    }

    /// Fetch a byte range from an S3 key.
    ///
    /// Core implementation for all range-read operations. Checks the circuit
    /// breaker, acquires a download semaphore permit, spawns the S3 call onto
    /// the tokio runtime (required for non-tokio executors like ublk), and
    /// records the result to the circuit breaker.
    async fn get_range_from_key(
        &self,
        key: &str,
        offset: u32,
        comp_length: u32,
    ) -> Result<bytes::Bytes, ContentStoreError> {
        self.check_circuit()?;
        let sem = self.download_semaphore.clone();
        let store = Arc::clone(&self.object_store);
        let path = ObjectPath::from(key.to_string());
        let start = u64::from(offset);
        let end = start + u64::from(comp_length);
        let s3_result = tokio::spawn(async move {
            let _permit = match &sem {
                Some(s) => Some(
                    s.acquire()
                        .await
                        .map_err(|_| object_store::Error::Generic {
                            store: "semaphore",
                            source: "download semaphore closed".into(),
                        })?,
                ),
                None => None,
            };
            store.get_range(&path, start..end).await
        })
        .await
        .map_err(|e| {
            ContentStoreError::ObjectStore(object_store::Error::Generic {
                store: "tokio-spawn",
                source: Box::new(e),
            })
        })?;
        self.record_s3_result(&s3_result);
        Ok(s3_result?)
    }

    /// Fetch the last `suffix_len` bytes from an S3 key.
    ///
    /// Used for reading footer-indexed pack data (GLPK v3). Same semaphore +
    /// circuit breaker pattern as `get_range_from_key`.
    async fn get_suffix_from_key(
        &self,
        key: &str,
        suffix_len: u64,
    ) -> Result<bytes::Bytes, ContentStoreError> {
        self.check_circuit()?;
        let sem = self.download_semaphore.clone();
        let store = Arc::clone(&self.object_store);
        let path = ObjectPath::from(key.to_string());
        let s3_result = tokio::spawn(async move {
            let _permit = match &sem {
                Some(s) => Some(
                    s.acquire()
                        .await
                        .map_err(|_| object_store::Error::Generic {
                            store: "semaphore",
                            source: "download semaphore closed".into(),
                        })?,
                ),
                None => None,
            };
            let opts = GetOptions {
                range: Some(GetRange::Suffix(suffix_len)),
                ..Default::default()
            };
            let response = store.get_opts(&path, opts).await?;
            response.bytes().await
        })
        .await
        .map_err(|e| {
            ContentStoreError::ObjectStore(object_store::Error::Generic {
                store: "tokio-spawn",
                source: Box::new(e),
            })
        })?;
        self.record_s3_result(&s3_result);
        Ok(s3_result?)
    }

    /// List all .pack files for a given chunk index.
    #[allow(dead_code)]
    pub async fn list_chunk_packs(
        &self,
        chunk_idx: u32,
    ) -> Result<Vec<String>, ContentStoreError> {
        self.check_circuit()?;
        let prefix_str = format!("{}/chunks/{:04}/", self.base_path, chunk_idx);
        let prefix = ObjectPath::from(prefix_str);
        let mut names = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(result) = stream.next().await {
            let meta = match result {
                Ok(m) => m,
                Err(e) => {
                    self.record_s3_list_error(&e);
                    return Err(e.into());
                }
            };
            if let Some(filename) = meta.location.filename() && filename.ends_with(".pack") {
                names.push(filename.to_string());
            }
        }
        if let Some(cb) = &self.circuit_breaker {
            cb.record_success();
        }
        Ok(names)
    }

    /// Upload a base's boot SET — the bounded, precise boot working set (a device
    /// block list, [`serialize_block_list`](super::manifest::serialize_block_list)).
    /// Stored beside the base manifest as `bases/{name}.boot-set`. Consumed by the
    /// device-open precise warm for non-EROFS images (which cannot reorder).
    pub async fn put_boot_set(&self, name: &str, data: Vec<u8>) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/manifests/bases/{}.boot-set", self.base_path, name);
        let path = ObjectPath::from(key);
        let result = self.object_store.put(&path, PutPayload::from(data)).await;
        self.record_s3_result(&result);
        result?;
        debug!(name = %name, "uploaded boot set");
        Ok(())
    }

    /// Download a base's boot set. Returns `None` if absent (e.g. an EROFS base,
    /// which carries its hint as the manifest `prefetch_len` instead).
    pub async fn get_boot_set(&self, name: &str) -> Result<Option<Vec<u8>>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/manifests/bases/{}.boot-set", self.base_path, name);
        let path = ObjectPath::from(key);
        let result = self.object_store.get(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(response) => Ok(Some(response.bytes().await?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List all manifest names under `manifests/` (not just bases).
    ///
    /// Returns paths relative to `manifests/`, e.g. `"vm1"`, `"bases/ubuntu-22.04"`.
    /// Filters out the `.boot-set` sidecar (and legacy `.hot-set`).
    pub async fn list_all_manifests(&self) -> Result<Vec<String>, ContentStoreError> {
        self.check_circuit()?;
        let prefix_str = format!("{}/manifests/", self.base_path);
        let prefix = ObjectPath::from(prefix_str.clone());
        let mut names = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(result) = stream.next().await {
            let meta = match result {
                Ok(m) => m,
                Err(e) => {
                    self.record_s3_list_error(&e);
                    return Err(e.into());
                }
            };
            let path_str = meta.location.to_string();
            // Extract path relative to manifests/
            if let Some(relative) = path_str.strip_prefix(&prefix_str) {
                if relative.ends_with(".hot-set") || relative.ends_with(".boot-set") {
                    continue;
                }
                if !relative.is_empty() {
                    names.push(relative.to_string());
                }
            }
        }
        if let Some(cb) = &self.circuit_breaker {
            cb.record_success();
        }
        Ok(names)
    }

    /// Check if a chunk pack exists in S3 (HEAD request, no data transfer).
    ///
    /// Used by cross-export dedup: if a content-addressed pack already exists
    /// (uploaded by another export sharing the same prefix), skip the upload.
    #[instrument(skip(self), fields(chunk_idx, pack_id))]
    pub async fn head_chunk_pack(
        &self,
        chunk_idx: u32,
        pack_id: super::pack::PackId,
    ) -> Result<bool, ContentStoreError> {
        self.check_circuit()?;
        let key = format!(
            "{}/chunks/{:04}/{:016x}.pack",
            self.base_path, chunk_idx, pack_id
        );
        let path = ObjectPath::from(key);
        let result = self.object_store.head(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Upload a chunk pack to S3 (non-streaming, used by tests and GC).
    ///
    /// S3 key: `{base_path}/chunks/{chunk_idx:04}/{pack_id:016x}.pack`
    #[instrument(skip(self, data), fields(chunk_idx, pack_id, size = data.len()))]
    #[allow(dead_code)]
    pub async fn put_chunk_pack(
        &self,
        chunk_idx: u32,
        pack_id: super::pack::PackId,
        data: Vec<u8>,
    ) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let _permit = match &self.upload_semaphore {
            Some(sem) => Some(sem.acquire().await.map_err(|_| ContentStoreError::SemaphoreClosed)?),
            None => None,
        };
        let key = format!(
            "{}/chunks/{:04}/{:016x}.pack",
            self.base_path, chunk_idx, pack_id
        );
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await;
        self.record_s3_result(&result);
        result?;
        debug!("uploaded chunk pack");
        Ok(())
    }

    /// Multipart upload cutoff: packs smaller than this use a single PUT
    /// (1 S3 request) instead of multipart (3 requests). S3's minimum part
    /// size for multipart is 5 MB, so anything below that gains nothing
    /// from multipart and just wastes requests.
    const MULTIPART_CUTOFF: usize = 5 * 1024 * 1024;

    /// Upload a chunk pack to S3.
    ///
    /// Small packs (< 5 MB) use a single PUT with `PutMode::Create` for
    /// content-addressed dedup: if the pack already exists (same content →
    /// same pack_id → same S3 key), the PUT is skipped (returns Ok).
    ///
    /// Large packs use multipart upload for streaming.
    ///
    /// Returns the index entries for inserting into `PackIndexCache`.
    #[instrument(skip(self, blocks), fields(chunk_idx, pack_id, block_count = blocks.len()))]
    pub async fn stream_chunk_pack(
        &self,
        chunk_idx: u32,
        pack_id: super::pack::PackId,
        blocks: Vec<(super::block_map::Blake3Hash, u32, bytes::Bytes)>,
        chunk_size: u32,
    ) -> Result<Vec<super::pack::PackIndexEntry>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!(
            "{}/chunks/{:04}/{:016x}.pack",
            self.base_path, chunk_idx, pack_id
        );
        let path = ObjectPath::from(key);

        // Estimate pack size: header + compressed data + index + trailer.
        let data_bytes: usize = blocks.iter().map(|(_, _, c)| c.len()).sum();
        let estimated_size = super::pack::PACK_HEADER_SIZE
            + data_bytes
            + blocks.len() * super::pack::PACK_INDEX_ENTRY_SIZE
            + super::pack::TRAILER_SIZE;

        if estimated_size < Self::MULTIPART_CUTOFF {
            // Small pack: assemble in memory, single PUT with PutMode::Create.
            let owned_blocks: Vec<(super::block_map::Blake3Hash, u32, Vec<u8>)> = blocks
                .into_iter()
                .map(|(h, co, b)| (h, co, b.to_vec()))
                .collect();
            let (pack_bytes, entries) =
                super::pack::assemble_pack(owned_blocks, chunk_size).map_err(|e| {
                    ContentStoreError::ObjectStore(object_store::Error::Generic {
                        store: "pack-assemble",
                        source: Box::new(e),
                    })
                })?;
            let payload = PutPayload::from(pack_bytes);
            let opts = PutOptions::from(PutMode::Create);
            // A small pack is exactly one PUT request -> one global write permit.
            let _permit = match &self.upload_semaphore {
                Some(sem) => Some(
                    sem.acquire()
                        .await
                        .map_err(|_| ContentStoreError::SemaphoreClosed)?,
                ),
                None => None,
            };
            let result = self.object_store.put_opts(&path, payload, opts).await;
            match &result {
                Ok(_) => {
                    self.record_s3_result(&result);
                    debug!("uploaded chunk pack (single PUT)");
                }
                Err(object_store::Error::AlreadyExists { .. }) => {
                    // Content-addressed dedup: identical pack already in S3.
                    debug!("chunk pack already exists (dedup hit)");
                    if let Some(cb) = &self.circuit_breaker {
                        cb.record_success();
                    }
                }
                Err(_) => {
                    self.record_s3_result(&result);
                    result?;
                }
            }
            Ok(entries)
        } else {
            // Large pack: stream via multipart. Each part PUT takes one permit
            // from the shared upload semaphore (see BoundedMultipart), so total
            // in-flight PUTs across all packs/exports never exceeds the global
            // limit — the pack can't fan out into unbounded connections.
            use super::pack::{BoundedMultipart, stream_pack_to_writer};

            let upload = self
                .object_store
                .put_multipart_opts(&path, PutMultipartOptions::default())
                .await;
            self.record_s3_result(&upload);
            let upload = upload?;

            let mut writer = BoundedMultipart::new(upload, self.upload_semaphore.clone());
            let entries = stream_pack_to_writer(blocks, chunk_size, &mut writer)
                .await
                .map_err(|e| {
                    ContentStoreError::ObjectStore(object_store::Error::Generic {
                        store: "pack-stream",
                        source: Box::new(e),
                    })
                })?;

            let finish_result = writer.finish().await;
            self.record_s3_result(&finish_result);
            finish_result?;
            debug!("streamed chunk pack (multipart)");
            Ok(entries)
        }
    }

    /// Fetch a single compressed block from a chunk pack via S3 range request.
    ///
    /// S3 key: `{base_path}/chunks/{chunk_idx:04}/{pack_id:016x}.pack`
    #[instrument(skip(self), fields(chunk_idx, pack_id, offset, comp_length))]
    pub async fn get_chunk_block(
        &self,
        chunk_idx: u32,
        pack_id: super::pack::PackId,
        offset: u32,
        comp_length: u32,
    ) -> Result<bytes::Bytes, ContentStoreError> {
        let key = format!(
            "{}/chunks/{:04}/{:016x}.pack",
            self.base_path, chunk_idx, pack_id
        );
        self.get_range_from_key(&key, offset, comp_length).await
    }

    /// Fetch the GLPK v3 pack index from S3 via adaptive suffix read.
    ///
    /// Uses a two-phase strategy to minimize bandwidth:
    /// 1. Small suffix read (16 KB) — covers typical packs (≤500 blocks).
    /// 2. If the pack has more blocks than the initial suffix can hold, a
    ///    precise second read fetches exactly the remaining index bytes.
    ///
    /// For the common case (≤500 blocks, ~14 KB index), this transfers ~16 KB
    /// instead of the previous fixed 896 KB — a ~56× reduction.
    ///
    /// S3 key: `{base_path}/chunks/{chunk_idx:04}/{pack_id:016x}.pack`
    #[instrument(skip(self), fields(chunk_idx, pack_id))]
    pub async fn get_pack_index(
        &self,
        chunk_idx: u32,
        pack_id: super::pack::PackId,
    ) -> Result<Vec<super::pack::PackIndexEntry>, ContentStoreError> {
        use super::pack::{PACK_INDEX_ENTRY_SIZE, TRAILER_SIZE, parse_pack_index};

        let key = format!(
            "{}/chunks/{:04}/{:016x}.pack",
            self.base_path, chunk_idx, pack_id
        );

        // Phase 1: small suffix read sized for typical packs (≤500 blocks).
        // 500 × 28 + 8 = 14,008 bytes. Round up to 16 KB for alignment.
        const INITIAL_SUFFIX: u64 = 16 * 1024;

        let data = self.get_suffix_from_key(&key, INITIAL_SUFFIX).await?;

        match parse_pack_index(&data) {
            Ok(index) => return Ok(index.entries),
            Err(e) => {
                // Check if this is a "suffix too small" error (pack has more
                // blocks than our initial read covered). Any other parse error
                // is a real failure.
                let msg = e.to_string();
                if !msg.contains("suffix too small") {
                    return Err(ContentStoreError::ObjectStore(
                        object_store::Error::Generic {
                            store: "pack-index",
                            source: Box::new(e),
                        },
                    ));
                }
            }
        }

        // Phase 2: we know the trailer is in `data`, so extract block_count
        // from the last 8 bytes and do a precise suffix read.
        if data.len() < TRAILER_SIZE {
            return Err(ContentStoreError::ObjectStore(
                object_store::Error::Generic {
                    store: "pack-index",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "suffix too small for trailer",
                    )),
                },
            ));
        }
        let trailer_start = data.len() - TRAILER_SIZE;
        let block_count =
            u64::from(u16::from_le_bytes([data[trailer_start], data[trailer_start + 1]]));
        let precise_suffix =
            block_count * PACK_INDEX_ENTRY_SIZE as u64 + TRAILER_SIZE as u64;

        debug!(
            block_count,
            precise_suffix,
            "pack index exceeded initial suffix, fetching precise size"
        );

        let data = self.get_suffix_from_key(&key, precise_suffix).await?;

        let index = parse_pack_index(&data).map_err(|e| {
            ContentStoreError::ObjectStore(object_store::Error::Generic {
                store: "pack-index",
                source: Box::new(e),
            })
        })?;

        Ok(index.entries)
    }

    /// Delete a chunk pack from S3 (idempotent).
    pub async fn delete_chunk_pack(
        &self,
        chunk_idx: u32,
        pack_id: super::pack::PackId,
    ) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!(
            "{}/chunks/{:04}/{:016x}.pack",
            self.base_path, chunk_idx, pack_id
        );
        let path = ObjectPath::from(key);
        let result = self.object_store.delete(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Get a reference to the underlying object store.
    #[allow(dead_code)]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    /// Get the base path.
    #[allow(dead_code)]
    pub fn base_path(&self) -> &str {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn test_store(base_path: &str) -> ContentStore {
        let object_store = Arc::new(InMemory::new());
        ContentStore::new(object_store, base_path)
    }

    #[tokio::test]
    async fn test_put_get_manifest() {
        let store = test_store("test-bucket");
        let name = "vm-abc-123";
        let data = b"serialized manifest bytes".to_vec();

        store
            .put_manifest(name, data.clone(), None)
            .await
            .expect("put_manifest should succeed");

        let (got, _etag) = store
            .get_manifest(name)
            .await
            .expect("get_manifest should succeed")
            .expect("manifest should exist");

        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn test_delete_manifest_idempotent() {
        let store = test_store("test-bucket");
        let name = "vm-to-delete";
        let data = b"manifest data".to_vec();

        store
            .put_manifest(name, data, None)
            .await
            .expect("put should succeed");

        // First delete succeeds
        store
            .delete_manifest(name)
            .await
            .expect("delete should succeed");

        // Manifest is gone
        assert!(store.get_manifest(name).await.unwrap().is_none());

        // Second delete is idempotent (NotFound is Ok)
        store
            .delete_manifest(name)
            .await
            .expect("delete of missing manifest should succeed");
    }

    #[tokio::test]
    async fn test_get_missing_manifest() {
        let store = test_store("test-bucket");

        let result = store
            .get_manifest("nonexistent-vm")
            .await
            .expect("get_manifest should not error for missing manifest");

        assert!(
            result.is_none(),
            "missing manifest should return Ok(None)"
        );
    }

    #[tokio::test]
    async fn test_circuit_breaker_blocks_when_open() {
        use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, FailurePolicy};
        use std::time::Duration;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_policy: FailurePolicy::Consecutive { threshold: 1 },
            reset_timeout: Duration::from_secs(300), // long timeout so CB stays open
            half_open_permits: 1,
        }));

        let store = ContentStore::new(object_store, "test-bucket").with_circuit_breaker(Arc::clone(&cb));

        // Force the circuit breaker open by recording a failure
        cb.record_failure();

        // All operations should fail with CircuitOpen
        let err = store.get_manifest("test").await.unwrap_err();
        assert!(matches!(err, ContentStoreError::CircuitOpen), "get_manifest should fail: {err}");

        let err = store.put_manifest("test", vec![1], None).await.unwrap_err();
        assert!(matches!(err, ContentStoreError::CircuitOpen), "put_manifest should fail: {err}");

        let err = store.get_chunk_block(0, 1234u64, 0, 10).await.unwrap_err();
        assert!(matches!(err, ContentStoreError::CircuitOpen), "get_chunk_block should fail: {err}");
    }

    #[tokio::test]
    async fn test_circuit_breaker_records_success_on_ok() {
        use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState, FailurePolicy};
        use std::time::Duration;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_policy: FailurePolicy::Consecutive { threshold: 3 },
            reset_timeout: Duration::from_secs(1),
            half_open_permits: 1,
        }));

        let store = ContentStore::new(object_store, "test-bucket").with_circuit_breaker(Arc::clone(&cb));

        // Successful operation should record success
        store.put_manifest("test", vec![1, 2, 3], None).await.unwrap();
        let _ = store.get_manifest("test").await.unwrap();

        // CB should still be closed
        assert!(matches!(cb.state(), CircuitState::Closed { .. }));
    }

    #[tokio::test]
    async fn test_circuit_breaker_not_found_counts_as_success() {
        use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState, FailurePolicy};
        use std::time::Duration;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_policy: FailurePolicy::Consecutive { threshold: 2 },
            reset_timeout: Duration::from_secs(1),
            half_open_permits: 1,
        }));

        let store = ContentStore::new(object_store, "test-bucket").with_circuit_breaker(Arc::clone(&cb));

        // Getting a missing manifest returns Ok(None), not an error
        let result = store.get_manifest("nonexistent").await.unwrap();
        assert!(result.is_none());

        // CB should still be closed (NotFound is not a connectivity failure)
        assert!(matches!(cb.state(), CircuitState::Closed { .. }));
    }

    /// Round-trip through the production streaming upload path.
    ///
    /// `stream_chunk_pack` writes header + compressed blocks + footer index +
    /// trailer via `WriteMultipart`. This test verifies that `get_pack_index`
    /// can parse the footer and that `get_chunk_block` returns data that
    /// decompresses to the original block content.
    #[tokio::test]
    async fn test_stream_chunk_pack_round_trip() {
        use crate::block::block_map::{blake3_128, lz4_compress, lz4_decompress};
        use crate::block::pack::PackIndexEntry;

        let store = test_store("test-bucket");
        let pack_id: u64 =0x0123_4567_89AB_CDEF;
        let chunk_size: u32 = 128 * 1024;

        // Build 3 blocks with distinct data, out-of-order chunk offsets.
        let mut originals = Vec::new();
        let mut blocks = Vec::new();
        for (i, chunk_offset) in [5u32, 0, 3].into_iter().enumerate() {
            let data: Vec<u8> = (0..chunk_size as usize)
                .map(|j| ((i * 31 + j * 7) % 256) as u8)
                .collect();
            let hash = blake3_128(&data);
            let compressed = lz4_compress(&data);
            originals.push((chunk_offset, data));
            blocks.push((hash, chunk_offset, bytes::Bytes::from(compressed)));
        }

        // Upload via the streaming production path.
        let entries: Vec<PackIndexEntry> = store
            .stream_chunk_pack(0, pack_id, blocks, chunk_size)
            .await
            .expect("stream_chunk_pack should succeed");

        assert_eq!(entries.len(), 3);
        // Entries must be sorted by chunk_offset (stream_pack_to_writer sorts).
        assert!(entries.windows(2).all(|w| w[0].chunk_offset < w[1].chunk_offset));

        // Read back via suffix-based index parse (the S3 cold-read path).
        let index_entries = store.get_pack_index(0, pack_id).await.unwrap();
        assert_eq!(index_entries.len(), 3);

        // Verify each block decompresses to original data.
        for entry in &index_entries {
            let compressed = store
                .get_chunk_block(0, pack_id, entry.offset, entry.comp_length)
                .await
                .unwrap();
            let decompressed = lz4_decompress(&compressed)
                .expect("LZ4 decompression should succeed");
            let original = originals
                .iter()
                .find(|(co, _)| *co == entry.chunk_offset)
                .unwrap_or_else(|| panic!("no original for chunk_offset {}", entry.chunk_offset));
            assert_eq!(
                decompressed, original.1,
                "data mismatch at chunk_offset {}",
                entry.chunk_offset
            );
        }
    }

    #[tokio::test]
    async fn test_v4_chunk_pack_round_trip() {
        let store = test_store("test-bucket");
        let pack_id: u64 =0xDEADBEEF_CAFEBABE;
        let data = b"fake pack data".to_vec();

        store
            .put_chunk_pack(3, pack_id, data.clone())
            .await
            .expect("put_chunk_pack should succeed");

        // Verify via range read
        let block = store
            .get_chunk_block(3, pack_id, 0, data.len() as u32)
            .await
            .expect("get_chunk_block should succeed");

        assert_eq!(&block[..], &data[..]);
    }

    #[tokio::test]
    async fn test_manifest_round_trip() {
        let store = test_store("test-bucket");
        let data = b"binary manifest data".to_vec();

        store
            .put_manifest("test-vm", data.clone(), None)
            .await
            .expect("put_manifest should succeed");

        let (got, _etag) = store
            .get_manifest("test-vm")
            .await
            .expect("get_manifest should succeed")
            .expect("manifest should exist");

        assert_eq!(got, data);
    }

    /// Regression: suffix read window must handle packs with >1024 blocks.
    ///
    /// With block_size < 128KB, a chunk can hold more than 1024 blocks.
    /// A drain of a full chunk creates a single pack with all those blocks.
    /// The suffix read in get_pack_index must be large enough to read the
    /// entire footer index, or the pack becomes unreadable after upload —
    /// data loss on cold restart.
    ///
    /// This test creates a pack with 2048 blocks (simulating 64KB block_size
    /// with 128 MiB chunk_size) and verifies the suffix read can parse it.
    #[tokio::test]
    async fn test_suffix_read_handles_large_pack() {
        use crate::block::block_map::{blake3_128, lz4_compress, lz4_decompress};

        let store = test_store("test-bucket");
        let pack_id: u64 = 0xDEAD_BEEF_0000_2048;
        let block_size: u32 = 64 * 1024; // 64KB blocks
        let block_count: u32 = 2048; // > old MAX_BLOCKS of 1024

        // Build 2048 blocks with distinct data
        let mut originals = Vec::new();
        let mut blocks = Vec::new();
        for chunk_offset in 0..block_count {
            let data: Vec<u8> = (0..block_size as usize)
                .map(|j| ((chunk_offset as usize * 31 + j * 7) % 256) as u8)
                .collect();
            let hash = blake3_128(&data);
            let compressed = lz4_compress(&data);
            originals.push((chunk_offset, data));
            blocks.push((hash, chunk_offset, bytes::Bytes::from(compressed)));
        }

        // Upload via streaming path
        let entries = store
            .stream_chunk_pack(0, pack_id, blocks, block_size)
            .await
            .expect("stream_chunk_pack should succeed with 2048 blocks");
        assert_eq!(entries.len(), block_count as usize);

        // Read back via suffix-based index parse — this was the bug:
        // old MAX_BLOCKS=1024 would fail with "suffix too small for index"
        let index_entries = store
            .get_pack_index(0, pack_id)
            .await
            .expect("get_pack_index should handle >1024-block packs");
        assert_eq!(index_entries.len(), block_count as usize);

        // Spot-check a few blocks decompress correctly
        for &check_offset in &[0, 1023, 1024, 2047] {
            let entry = index_entries
                .iter()
                .find(|e| e.chunk_offset == check_offset)
                .unwrap_or_else(|| panic!("no entry for chunk_offset {check_offset}"));
            let compressed = store
                .get_chunk_block(0, pack_id, entry.offset, entry.comp_length)
                .await
                .unwrap();
            let decompressed =
                lz4_decompress(&compressed).expect("LZ4 decompression should succeed");
            let original = &originals[check_offset as usize].1;
            assert_eq!(
                &decompressed, original,
                "data mismatch at chunk_offset {check_offset}"
            );
        }
    }
}
