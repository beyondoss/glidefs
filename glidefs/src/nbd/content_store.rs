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
use tracing::{debug, instrument};
use uuid::Uuid;

use super::manifest::manifest_s3_key;
use super::pack::pack_s3_key;
use super::pack_registry::registry_s3_key;

#[derive(Error, Debug)]
pub enum ContentStoreError {
    #[error("S3 operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("S3 circuit breaker is open (service temporarily unavailable)")]
    CircuitOpen,
}

/// Returns true for errors that indicate S3 connectivity failure (network,
/// timeout, 5xx). NotFound, Precondition, etc. are valid S3 responses and
/// prove the backend is reachable.
fn is_connectivity_error(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::Generic { .. })
}

pub struct ContentStore {
    object_store: Arc<dyn ObjectStore>,
    base_path: String,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl ContentStore {
    pub fn new(object_store: Arc<dyn ObjectStore>, base_path: &str) -> Self {
        Self {
            object_store,
            base_path: base_path.trim_end_matches('/').to_string(),
            circuit_breaker: None,
        }
    }

    /// Attach a shared circuit breaker for S3 calls.
    pub fn with_circuit_breaker(mut self, cb: Arc<CircuitBreaker>) -> Self {
        self.circuit_breaker = Some(cb);
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

    /// Upload a pack to S3.
    #[instrument(skip(self, data), fields(pack_id = %pack_id, size = data.len()))]
    pub async fn put_pack(&self, pack_id: Uuid, data: Vec<u8>) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, pack_s3_key(pack_id));
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await;
        self.record_s3_result(&result);
        result?;
        debug!("uploaded pack");
        Ok(())
    }

    /// Download a pack from S3 (buffered).
    #[allow(dead_code)]
    #[instrument(skip(self), fields(pack_id = %pack_id))]
    pub async fn get_pack(&self, pack_id: Uuid) -> Result<Vec<u8>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, pack_s3_key(pack_id));
        let path = ObjectPath::from(key);
        let result = self.object_store.get(&path).await;
        self.record_s3_result(&result);
        let response = result?;
        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }

    /// Fetch a single compressed block from a pack via S3 range request.
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
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, pack_s3_key(pack_id));
        let path = ObjectPath::from(key);
        let start = offset as u64;
        let end = start + comp_length as u64;
        let result = self.object_store.get_range(&path, start..end).await;
        self.record_s3_result(&result);
        Ok(result?)
    }

    /// Stream a pack from S3 as an async byte stream.
    ///
    /// Returns a stream of `Bytes` chunks. The caller can parse the header/index
    /// from early chunks and decompress blocks incrementally as data arrives,
    /// avoiding buffering the full ~3MB pack in memory.
    #[instrument(skip(self), fields(pack_id = %pack_id))]
    pub async fn get_pack_stream(
        &self,
        pack_id: Uuid,
    ) -> Result<
        impl futures::Stream<Item = Result<bytes::Bytes, ContentStoreError>> + Send + Unpin,
        ContentStoreError,
    > {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, pack_s3_key(pack_id));
        let path = ObjectPath::from(key);
        let result = self.object_store.get(&path).await;
        self.record_s3_result(&result);
        let response = result?;
        let stream = response.into_stream().map(|r| r.map_err(ContentStoreError::from));
        Ok(Box::pin(stream))
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

    /// List all base manifest names under `manifests/bases/`.
    pub async fn list_base_manifests(&self) -> Result<Vec<String>, ContentStoreError> {
        self.check_circuit()?;
        let prefix = ObjectPath::from(format!("{}/manifests/bases/", self.base_path));
        let mut names = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(result) = stream.next().await {
            let meta = result?;
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

    // =========================================================================
    // Pack registry operations (for GC)
    // =========================================================================

    /// Upload a pack registry to S3.
    pub async fn put_registry(
        &self,
        name: &str,
        data: Vec<u8>,
    ) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, registry_s3_key(name));
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await;
        self.record_s3_result(&result);
        result?;
        debug!(name = %name, "uploaded pack registry");
        Ok(())
    }

    /// Download a pack registry from S3. Returns None if not found.
    pub async fn get_registry(
        &self,
        name: &str,
    ) -> Result<Option<Vec<u8>>, ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, registry_s3_key(name));
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

    /// List all registry names under `pack-registries/`.
    pub async fn list_registries(&self) -> Result<Vec<String>, ContentStoreError> {
        self.check_circuit()?;
        let prefix = ObjectPath::from(format!("{}/pack-registries/", self.base_path));
        let mut names = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(result) = stream.next().await {
            let meta = result?;
            if let Some(name) = meta.location.filename() {
                names.push(name.to_string());
            }
        }
        if let Some(cb) = &self.circuit_breaker {
            cb.record_success();
        }
        Ok(names)
    }

    /// Delete a pack registry from S3 (idempotent).
    pub async fn delete_registry(&self, name: &str) -> Result<(), ContentStoreError> {
        self.check_circuit()?;
        let key = format!("{}/{}", self.base_path, registry_s3_key(name));
        let path = ObjectPath::from(key);
        let result = self.object_store.delete(&path).await;
        self.record_s3_result(&result);
        match result {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
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
            let meta = result?;
            let path_str = meta.location.to_string();
            // Extract path relative to manifests/
            if let Some(relative) = path_str.strip_prefix(&prefix_str) {
                // Skip hot-set files
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
    async fn test_put_get_pack() {
        let store = test_store("test-bucket");
        let pack_id = Uuid::new_v4();
        let data = b"compressed pack data with multiple blocks inside".to_vec();

        store
            .put_pack(pack_id, data.clone())
            .await
            .expect("put_pack should succeed");

        let got = store
            .get_pack(pack_id)
            .await
            .expect("get_pack should succeed");

        assert_eq!(got, data);
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
        let err = store.put_pack(Uuid::new_v4(), vec![1, 2, 3]).await.unwrap_err();
        assert!(matches!(err, ContentStoreError::CircuitOpen), "put_pack should fail: {err}");

        let err = store.get_pack(Uuid::new_v4()).await.unwrap_err();
        assert!(matches!(err, ContentStoreError::CircuitOpen), "get_pack should fail: {err}");

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
        let pack_id = Uuid::new_v4();
        store.put_pack(pack_id, vec![1, 2, 3]).await.unwrap();
        store.get_pack(pack_id).await.unwrap();

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
}
