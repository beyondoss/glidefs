//! Content-addressed S3 storage for packs and manifests.
//!
//! Thin wrapper around `ObjectStore` providing typed PUT/GET for packs
//! (shared across exports) and manifests (per-export).

use crate::circuit_breaker::CircuitBreaker;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{debug, instrument};
use uuid::Uuid;

use super::manifest::{manifest_s3_key, snapshot_s3_key};
use super::pack::pack_s3_key;
use super::volume_manifest::VolumeManifest;

#[derive(Error, Debug)]
pub enum ContentStoreError {
    #[error("S3 operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("S3 circuit breaker is open (service temporarily unavailable)")]
    CircuitOpen,

    #[error("S3 concurrency semaphore closed")]
    SemaphoreClosed,
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

/// Object type within a chunk directory.
#[derive(Debug, Clone)]
pub enum ChunkObjectKind {
    /// A pack file: `{uuid}.pack`
    Pack(Uuid),
    /// A chunk meta file: `{hash}.meta`
    Meta(String),
}

/// An object discovered in a chunk directory (or legacy packs/ prefix).
#[derive(Debug, Clone)]
pub struct ChunkObject {
    /// Chunk index (4-digit zero-padded in S3). `u32::MAX` for legacy flat packs.
    pub chunk_idx: u32,
    /// Whether this is a .pack or .meta file.
    pub kind: ChunkObjectKind,
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

    /// Fetch a single compressed block from a legacy pack via S3 range request.
    ///
    /// Returns only the compressed bytes at `[offset..offset+comp_length]` —
    /// typically ~100KB vs ~3MB for the full pack.
    #[instrument(skip(self), fields(pack_id = %pack_id, offset, comp_length))]
    pub async fn get_block(
        &self,
        pack_id: Uuid,
        offset: u32,
        comp_length: u32,
    ) -> Result<bytes::Bytes, ContentStoreError> {
        let key = format!("{}/{}", self.base_path, pack_s3_key(pack_id));
        self.get_range_from_key(&key, offset, comp_length).await
    }

    /// Upload a manifest to S3. Returns the S3 ETag if the backend provides one.
    #[instrument(skip(self, data), fields(name = %name, size = data.len()))]
    pub async fn put_manifest(
        &self,
        name: &str,
        data: Vec<u8>,
    ) -> Result<Option<String>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, manifest_s3_key(name));
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await;
        self.record_s3_result(&result);
        let put_result = result?;
        debug!("uploaded manifest");
        Ok(put_result.e_tag)
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

    /// Download a manifest from S3. Returns None if not found.
    #[instrument(skip(self), fields(name = %name))]
    pub async fn get_manifest(
        &self,
        name: &str,
    ) -> Result<Option<Vec<u8>>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, manifest_s3_key(name));
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
    #[instrument(skip(self), fields(name = %name))]
    pub async fn delete_all_snapshots(&self, name: &str) -> Result<(), ContentStoreError> {
        let sequences = self.list_snapshots(name).await?;
        for seq in sequences {
            if let Err(e) = self.delete_snapshot(name, seq).await {
                tracing::warn!(
                    name = %name, sequence = seq, error = %e,
                    "failed to delete snapshot during cleanup (continuing)"
                );
            }
        }
        Ok(())
    }

    /// List all snapshot objects with their S3 last_modified timestamps.
    /// Returns (path, last_modified) for each snapshot in this prefix.
    pub async fn list_all_snapshots_with_dates(
        &self,
    ) -> Result<Vec<(ObjectPath, chrono::DateTime<chrono::Utc>)>, ContentStoreError> {
        let prefix_str = format!("{}/snapshots/", self.base_path);
        let prefix = ObjectPath::from(prefix_str);
        let mut results = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => results.push((meta.location, meta.last_modified)),
                Err(e) => {
                    self.record_s3_list_error(&e);
                    return Err(e.into());
                }
            }
        }
        if let Some(cb) = &self.circuit_breaker {
            cb.record_success();
        }
        Ok(results)
    }

    /// Delete a snapshot by its S3 path (idempotent).
    pub async fn delete_snapshot_by_path(
        &self,
        path: &ObjectPath,
    ) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let result = self.object_store.delete(path).await;
        self.record_s3_result(&result);
        match result {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// List and parse all snapshot VolumeManifests in this prefix.
    /// Corrupt or missing snapshots are warned and skipped.
    pub async fn list_snapshot_manifests(
        &self,
    ) -> Result<Vec<VolumeManifest>, ContentStoreError> {
        let prefix_str = format!("{}/snapshots/", self.base_path);
        let prefix = ObjectPath::from(prefix_str);
        let mut manifests = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));
        let mut paths = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => paths.push(meta.location),
                Err(e) => {
                    self.record_s3_list_error(&e);
                    return Err(e.into());
                }
            }
        }
        for path in paths {
            let data = match self.object_store.get(&path).await {
                Ok(response) => match response.bytes().await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(key = %path, error = %e, "failed to read snapshot manifest bytes");
                        continue;
                    }
                },
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => {
                    tracing::warn!(key = %path, error = %e, "failed to fetch snapshot manifest");
                    continue;
                }
            };
            match VolumeManifest::deserialize(&data) {
                Ok(vm) => {
                    manifests.push(vm);
                }
                Err(e) => {
                    tracing::warn!(key = %path, error = %e, "corrupt snapshot manifest, skipping");
                }
            }
        }
        if let Some(cb) = &self.circuit_breaker {
            cb.record_success();
        }
        Ok(manifests)
    }

    // =========================================================================
    // Chunk operations (v3 chunked block index)
    // =========================================================================

    /// Upload a chunk .meta file to S3.
    ///
    /// S3 key: `{base_path}/chunks/{chunk_idx:04}/{hex_hash}.meta`
    #[instrument(skip(self, data), fields(chunk_idx, size = data.len()))]
    pub async fn put_chunk_meta(
        &self,
        chunk_idx: u32,
        chunk_hash: &str,
        data: Vec<u8>,
    ) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let _permit = match &self.upload_semaphore {
            Some(sem) => Some(sem.acquire().await.map_err(|_| ContentStoreError::SemaphoreClosed)?),
            None => None,
        };
        let key = format!("{}/chunks/{:04}/{}.meta", self.base_path, chunk_idx, chunk_hash);
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await;
        self.record_s3_result(&result);
        result?;
        debug!("uploaded chunk meta");
        Ok(())
    }

    /// Download a chunk .meta file from S3. Returns None if not found.
    #[instrument(skip(self), fields(chunk_idx))]
    pub async fn get_chunk_meta(
        &self,
        chunk_idx: u32,
        chunk_hash: &str,
    ) -> Result<Option<Vec<u8>>, ContentStoreError> {
        self.check_circuit()?;
        let _permit = match &self.download_semaphore {
            Some(sem) => Some(sem.acquire().await.map_err(|_| ContentStoreError::SemaphoreClosed)?),
            None => None,
        };
        let key = format!("{}/chunks/{:04}/{}.meta", self.base_path, chunk_idx, chunk_hash);
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

    /// Upload a chunk pack to S3.
    ///
    /// S3 key: `{base_path}/chunks/{chunk_idx:04}/{pack_uuid}.pack`
    #[instrument(skip(self, data), fields(chunk_idx, pack_id = %pack_id, size = data.len()))]
    pub async fn put_chunk_pack(
        &self,
        chunk_idx: u32,
        pack_id: Uuid,
        data: Vec<u8>,
    ) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let _permit = match &self.upload_semaphore {
            Some(sem) => Some(sem.acquire().await.map_err(|_| ContentStoreError::SemaphoreClosed)?),
            None => None,
        };
        let key = format!("{}/chunks/{:04}/{}.pack", self.base_path, chunk_idx, pack_id);
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await;
        self.record_s3_result(&result);
        result?;
        debug!("uploaded chunk pack");
        Ok(())
    }

    /// Fetch a single compressed block from a chunk pack via S3 range request.
    ///
    /// S3 key: `{base_path}/chunks/{chunk_idx:04}/{pack_uuid}.pack`
    #[instrument(skip(self), fields(chunk_idx, pack_id = %pack_id, offset, comp_length))]
    pub async fn get_chunk_block(
        &self,
        chunk_idx: u32,
        pack_id: Uuid,
        offset: u32,
        comp_length: u32,
    ) -> Result<bytes::Bytes, ContentStoreError> {
        // Try chunk-scoped path first: chunks/{idx:04}/{pack_id}.pack
        let chunk_key = format!("{}/chunks/{:04}/{}.pack", self.base_path, chunk_idx, pack_id);
        match self.get_range_from_key(&chunk_key, offset, comp_length).await {
            Ok(data) => return Ok(data),
            Err(ContentStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                // Fallback: try legacy pack path (packs/{pack_id})
            }
            Err(e) => return Err(e),
        }

        // Legacy fallback: packs/{pack_id} (pre-v3 format)
        self.get_block(pack_id, offset, comp_length).await
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
        let start = offset as u64;
        let end = start + comp_length as u64;
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

    /// Upload a volume manifest (JSON) to S3.
    ///
    /// Uses the same `manifests/{name}` key as the old binary manifest.
    #[instrument(skip(self, data), fields(name = %name, size = data.len()))]
    pub async fn put_volume_manifest(
        &self,
        name: &str,
        data: Vec<u8>,
    ) -> Result<Option<String>, ContentStoreError> {
        // Reuse the existing put_manifest path (same S3 key).
        self.put_manifest(name, data).await
    }

    /// Download a volume manifest (JSON) from S3. Returns None if not found.
    #[instrument(skip(self), fields(name = %name))]
    pub async fn get_volume_manifest(
        &self,
        name: &str,
    ) -> Result<Option<Vec<u8>>, ContentStoreError> {
        // Reuse the existing get_manifest path (same S3 key).
        self.get_manifest(name).await
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

    /// List all base manifest names under `manifests/bases/`.
    pub async fn list_base_manifests(&self) -> Result<Vec<String>, ContentStoreError> {
        self.check_circuit()?;
        let prefix = ObjectPath::from(format!("{}/manifests/bases/", self.base_path));
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
            if let Some(name) = meta.location.filename() {
                names.push(name.to_string());
            }
        }
        if let Some(cb) = &self.circuit_breaker {
            cb.record_success();
        }
        Ok(names)
    }

    /// Upload a boot hot set to S3.
    pub async fn put_hot_set(&self, name: &str, data: Vec<u8>) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/manifests/bases/{}.hot-set", self.base_path, name);
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await;
        self.record_s3_result(&result);
        result?;
        debug!(name = %name, "uploaded hot set");
        Ok(())
    }

    /// Download a boot hot set from S3. Returns None if not found.
    pub async fn get_hot_set(&self, name: &str) -> Result<Option<Vec<u8>>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/manifests/bases/{}.hot-set", self.base_path, name);
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

    /// List all manifest names under `manifests/` (not just bases).
    ///
    /// Returns paths relative to `manifests/`, e.g. `"vm1"`, `"bases/ubuntu-22.04"`.
    /// Filters out `.hot-set` files.
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
                if relative.ends_with(".hot-set") {
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

    /// List ALL objects (.pack and .meta) across chunk directories, plus legacy flat packs.
    ///
    /// Single-pass listing: GC uses this to discover both known packs and known metas
    /// without a second LIST scan. Returns `ChunkObject` variants for each file type.
    /// Legacy flat packs (pre-v3) are returned as `Pack` with `chunk_idx = u32::MAX`.
    pub async fn list_all_chunk_objects(
        &self,
    ) -> Result<Vec<ChunkObject>, ContentStoreError> {
        self.check_circuit()?;
        let mut objects = Vec::new();

        // 1. List everything under chunks/
        let chunks_prefix_str = format!("{}/chunks/", self.base_path);
        let chunks_prefix = ObjectPath::from(chunks_prefix_str.clone());
        let mut stream = self.object_store.list(Some(&chunks_prefix));
        while let Some(result) = stream.next().await {
            let meta = match result {
                Ok(m) => m,
                Err(e) => {
                    self.record_s3_list_error(&e);
                    return Err(e.into());
                }
            };
            let Some(filename) = meta.location.filename() else {
                continue;
            };
            let path_str = meta.location.to_string();
            let Some(rel) = path_str.strip_prefix(&chunks_prefix_str) else {
                continue;
            };
            // rel = "{idx:04}/{filename}"
            let Some(slash_pos) = rel.find('/') else {
                continue;
            };
            let Ok(chunk_idx) = rel[..slash_pos].parse::<u32>() else {
                continue;
            };

            if let Some(uuid_str) = filename.strip_suffix(".pack")
                && let Ok(pack_id) = Uuid::parse_str(uuid_str)
            {
                objects.push(ChunkObject {
                    chunk_idx,
                    kind: ChunkObjectKind::Pack(pack_id),
                });
            } else if let Some(hash_hex) = filename.strip_suffix(".meta") {
                objects.push(ChunkObject {
                    chunk_idx,
                    kind: ChunkObjectKind::Meta(hash_hex.to_string()),
                });
            }
        }

        // 2. List legacy flat packs: packs/{uuid}
        let packs_prefix_str = format!("{}/packs/", self.base_path);
        let packs_prefix = ObjectPath::from(packs_prefix_str);
        let mut stream = self.object_store.list(Some(&packs_prefix));
        while let Some(result) = stream.next().await {
            let meta = match result {
                Ok(m) => m,
                Err(e) => {
                    self.record_s3_list_error(&e);
                    return Err(e.into());
                }
            };
            if let Some(filename) = meta.location.filename()
                && let Ok(pack_id) = Uuid::parse_str(filename)
            {
                objects.push(ChunkObject {
                    chunk_idx: u32::MAX,
                    kind: ChunkObjectKind::Pack(pack_id),
                });
            }
        }

        if let Some(cb) = &self.circuit_breaker {
            cb.record_success();
        }
        Ok(objects)
    }

    /// List ALL pack files across all chunk directories and the legacy flat `packs/` prefix.
    ///
    /// Returns a set of `(chunk_idx, pack_id)` tuples for chunk packs, plus `(u32::MAX, pack_id)`
    /// for legacy flat packs. Thin wrapper around `list_all_chunk_objects` for backward compat.
    pub async fn list_all_known_packs(
        &self,
    ) -> Result<Vec<(u32, Uuid)>, ContentStoreError> {
        let objects = self.list_all_chunk_objects().await?;
        Ok(objects
            .into_iter()
            .filter_map(|obj| match obj.kind {
                ChunkObjectKind::Pack(pack_id) => Some((obj.chunk_idx, pack_id)),
                ChunkObjectKind::Meta(_) => None,
            })
            .collect())
    }

    /// Delete a chunk .meta file from S3 by chunk_idx and chunk_hash (idempotent).
    pub async fn delete_chunk_meta(&self, chunk_idx: u32, chunk_hash: &str) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/chunks/{:04}/{}.meta", self.base_path, chunk_idx, chunk_hash);
        let path = ObjectPath::from(key);
        let result = self.object_store.delete(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete a chunk pack from S3 by chunk_idx and pack_id (idempotent).
    pub async fn delete_chunk_pack(&self, chunk_idx: u32, pack_id: Uuid) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/chunks/{:04}/{}.pack", self.base_path, chunk_idx, pack_id);
        let path = ObjectPath::from(key);
        let result = self.object_store.delete(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete a pack from S3 by pack_id (idempotent).
    pub async fn delete_pack(&self, pack_id: Uuid) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, pack_s3_key(pack_id));
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
            .put_manifest(name, data.clone())
            .await
            .expect("put_manifest should succeed");

        let got = store
            .get_manifest(name)
            .await
            .expect("get_manifest should succeed")
            .expect("manifest should exist");

        assert_eq!(got, data);
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

        let err = store.put_manifest("test", vec![1]).await.unwrap_err();
        assert!(matches!(err, ContentStoreError::CircuitOpen), "put_manifest should fail: {err}");

        let err = store.get_block(Uuid::new_v4(), 0, 10).await.unwrap_err();
        assert!(matches!(err, ContentStoreError::CircuitOpen), "get_block should fail: {err}");
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
        store.put_manifest("test", vec![1, 2, 3]).await.unwrap();
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

    #[tokio::test]
    async fn test_pack_key_sharding() {
        // Verify the pack key format: packs/XX/<uuid>
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let key1 = pack_s3_key(id1);
        let key2 = pack_s3_key(id2);

        // Both keys should start with "packs/"
        assert!(
            key1.starts_with("packs/"),
            "pack key should start with 'packs/', got: {key1}"
        );
        assert!(
            key2.starts_with("packs/"),
            "pack key should start with 'packs/', got: {key2}"
        );

        // Keys should have the format packs/XX/<uuid-string>
        let parts1: Vec<&str> = key1.splitn(3, '/').collect();
        assert_eq!(parts1.len(), 3, "pack key should have 3 path segments");
        assert_eq!(parts1[0], "packs");
        assert_eq!(
            parts1[1].len(),
            2,
            "shard prefix should be 2 hex characters"
        );

        // The shard prefix should be valid hex.
        assert!(
            parts1[1].chars().all(|c| c.is_ascii_hexdigit()),
            "shard prefix should be hex digits, got: {}",
            parts1[1]
        );

        // The UUID portion should contain the UUID string.
        assert!(
            parts1[2].contains(&id1.to_string()),
            "pack key should contain the UUID"
        );
    }

    #[tokio::test]
    async fn test_put_get_chunk_meta() {
        let store = test_store("test-bucket");
        let data = b"fake chunk meta bytes".to_vec();

        store
            .put_chunk_meta(0, "abcdef1234567890", data.clone())
            .await
            .expect("put_chunk_meta should succeed");

        let got = store
            .get_chunk_meta(0, "abcdef1234567890")
            .await
            .expect("get_chunk_meta should succeed")
            .expect("chunk meta should exist");

        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn test_get_missing_chunk_meta() {
        let store = test_store("test-bucket");
        let result = store
            .get_chunk_meta(99, "nonexistent")
            .await
            .expect("should not error for missing chunk meta");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_put_get_chunk_pack() {
        let store = test_store("test-bucket");
        let pack_id = Uuid::new_v4();
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
    async fn test_volume_manifest_round_trip() {
        let store = test_store("test-bucket");
        let data = br#"{"size":1073741824,"version":3,"chunk_size":10737418240,"block_size":131072,"chunks":{}}"#.to_vec();

        store
            .put_volume_manifest("test-vm", data.clone())
            .await
            .expect("put_volume_manifest should succeed");

        let got = store
            .get_volume_manifest("test-vm")
            .await
            .expect("get_volume_manifest should succeed")
            .expect("volume manifest should exist");

        assert_eq!(got, data);
    }
}
