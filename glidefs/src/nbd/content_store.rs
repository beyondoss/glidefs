//! Content-addressed S3 storage for packs and manifests.
//!
//! Thin wrapper around `ObjectStore` providing typed PUT/GET for packs
//! (shared across exports) and manifests (per-export).

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, instrument};
use uuid::Uuid;

use super::manifest::manifest_s3_key;
use super::pack::pack_s3_key;

#[derive(Error, Debug)]
pub enum ContentStoreError {
    #[error("S3 operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),
}

pub struct ContentStore {
    object_store: Arc<dyn ObjectStore>,
    base_path: String,
}

impl ContentStore {
    pub fn new(object_store: Arc<dyn ObjectStore>, base_path: &str) -> Self {
        Self {
            object_store,
            base_path: base_path.trim_end_matches('/').to_string(),
        }
    }

    /// Upload a pack to S3.
    #[instrument(skip(self, data), fields(pack_id = %pack_id, size = data.len()))]
    pub async fn put_pack(&self, pack_id: Uuid, data: Vec<u8>) -> Result<(), ContentStoreError> {
        let key = format!("{}/{}", self.base_path, pack_s3_key(pack_id));
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        self.object_store.put(&path, payload).await?;
        debug!("uploaded pack");
        Ok(())
    }

    /// Download a pack from S3.
    #[instrument(skip(self), fields(pack_id = %pack_id))]
    pub async fn get_pack(&self, pack_id: Uuid) -> Result<Vec<u8>, ContentStoreError> {
        let key = format!("{}/{}", self.base_path, pack_s3_key(pack_id));
        let path = ObjectPath::from(key);
        let result = self.object_store.get(&path).await?;
        let bytes = result.bytes().await.map_err(object_store::Error::from)?;
        Ok(bytes.to_vec())
    }

    /// Upload a manifest to S3. Returns the S3 ETag if the backend provides one.
    #[instrument(skip(self, data), fields(name = %name, size = data.len()))]
    pub async fn put_manifest(
        &self,
        name: &str,
        data: Vec<u8>,
    ) -> Result<Option<String>, ContentStoreError> {
        let key = format!("{}/{}", self.base_path, manifest_s3_key(name));
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(data);
        let result = self.object_store.put(&path, payload).await?;
        debug!("uploaded manifest");
        Ok(result.e_tag)
    }

    /// Download a manifest from S3. Returns None if not found.
    #[instrument(skip(self), fields(name = %name))]
    pub async fn get_manifest(
        &self,
        name: &str,
    ) -> Result<Option<Vec<u8>>, ContentStoreError> {
        let key = format!("{}/{}", self.base_path, manifest_s3_key(name));
        let path = ObjectPath::from(key);
        match self.object_store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(object_store::Error::from)?;
                Ok(Some(bytes.to_vec()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
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
