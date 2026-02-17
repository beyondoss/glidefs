use futures::StreamExt;
use object_store::{Error, ObjectStore, PutMode, PutOptions, path::Path};
use std::sync::Arc;
use uuid::Uuid;

const TEST_FILE_PREFIX: &str = ".glidefs_compatibility_test_";

pub async fn check_if_match_support(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> anyhow::Result<()> {
    // Clean up any old test files from previous runs (best effort)
    let prefix_path = Path::from(db_path).child(TEST_FILE_PREFIX);
    let mut list = object_store.list(Some(&prefix_path));
    while let Some(Ok(meta)) = list.next().await {
        let _ = object_store.delete(&meta.location).await;
    }

    let test_id = Uuid::new_v4();
    let test_path = Path::from(db_path).child(format!("{}{}", TEST_FILE_PREFIX, test_id));

    tracing::info!("Checking storage provider compatibility (conditional writes for fencing)...");

    object_store
        .put(&test_path, "initial".into())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to write test file: {e:#?}"))?;

    let result = object_store
        .put_opts(
            &test_path,
            "should_fail".into(),
            PutOptions::from(PutMode::Create),
        )
        .await;

    let _ = object_store.delete(&test_path).await;

    match result {
        Err(Error::AlreadyExists { .. }) => {
            tracing::info!("Storage provider compatibility check passed");
            Ok(())
        }
        Ok(_) => Err(anyhow::anyhow!(
            "Storage provider does not support conditional writes. \
            PutMode::Create succeeded when it should have failed. \
            This feature is required for fencing in GlideFS."
        )),
        Err(Error::NotImplemented) => Err(anyhow::anyhow!(
            "Storage provider does not support conditional writes (PutMode::Create). \
            This feature is required for fencing in GlideFS."
        )),
        Err(e) => {
            let error_str = e.to_string().to_lowercase();
            if error_str.contains("501") || error_str.contains("not implemented") {
                Err(anyhow::anyhow!(
                    "Storage provider does not support conditional writes. \
                    This feature is required for fencing in GlideFS.\n\n{e}"
                ))
            } else {
                Err(anyhow::anyhow!(
                    "Storage provider precondition check failed: {e}"
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compatible_store_passes() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let result = check_if_match_support(&store, "test-compat").await;
        assert!(result.is_ok(), "InMemory supports PutMode::Create: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_cleans_up_test_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        // Run twice — second run should clean up files from the first
        check_if_match_support(&store, "test-cleanup").await.unwrap();
        check_if_match_support(&store, "test-cleanup").await.unwrap();

        // Verify no leftover test files
        let prefix = Path::from("test-cleanup").child(TEST_FILE_PREFIX);
        let mut list = store.list(Some(&prefix));
        let mut count = 0;
        while let Some(Ok(_)) = list.next().await {
            count += 1;
        }
        assert_eq!(count, 0, "test files should be cleaned up");
    }
}
