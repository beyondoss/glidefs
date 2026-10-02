use futures::StreamExt;
use object_store::{Error, ObjectStore, PutMode, PutOptions, path::Path};
use std::sync::Arc;
use std::time::Duration;
const TEST_FILE_PREFIX: &str = ".glidefs_compatibility_test_";

/// Why the compatibility check did not pass.
#[derive(Debug)]
pub enum CheckFailure {
    /// The provider answered, and it lacks conditional writes: a misconfiguration
    /// no retry fixes.
    Unsupported(anyhow::Error),
    /// The provider couldn't be reached or didn't answer usably — the network isn't
    /// up yet, DNS, a transport error: worth waiting out.
    Unreachable(anyhow::Error),
}

/// Run the compatibility check until storage answers it. Startup runs it before
/// anything touches kernel state, so storage that is unreachable — a network not up
/// yet at boot, a blip during a restart — is waited out, with backoff from `initial`
/// doubling to `max`, rather than exiting the daemon: the unit's fail-closed restart
/// cap would otherwise leave it down until someone restarts it by hand. systemd's
/// `TimeoutStartSec` bounds the wait. A definitive answer that the provider lacks
/// conditional writes fails at once.
pub async fn wait_for_if_match_support(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    initial: Duration,
    max: Duration,
) -> anyhow::Result<()> {
    let mut delay = initial;
    loop {
        match check_once(object_store, db_path).await {
            Ok(()) => return Ok(()),
            Err(CheckFailure::Unsupported(e)) => return Err(e),
            Err(CheckFailure::Unreachable(e)) => {
                tracing::warn!(error = %e, retry_in = ?delay, "object storage unreachable at startup; waiting for it");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max);
            }
        }
    }
}

/// One compatibility check: does the provider support conditional writes
/// (`PutMode::Create`), which GlideFS's fencing requires?
async fn check_once(object_store: &Arc<dyn ObjectStore>, db_path: &str) -> Result<(), CheckFailure> {
    // Clean up any old test files from previous runs (best effort)
    let prefix_path = Path::from(db_path).child(TEST_FILE_PREFIX);
    let mut list = object_store.list(Some(&prefix_path));
    while let Some(Ok(meta)) = list.next().await {
        let _ = object_store.delete(&meta.location).await;
    }

    let test_id: u64 = rand::random();
    let test_path = Path::from(db_path).child(format!("{}{:016x}", TEST_FILE_PREFIX, test_id));

    tracing::info!("Checking storage provider compatibility (conditional writes for fencing)...");

    object_store
        .put(&test_path, "initial".into())
        .await
        .map_err(|e| CheckFailure::Unreachable(anyhow::anyhow!("Failed to write test file: {e:#?}")))?;

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
        Ok(_) => Err(CheckFailure::Unsupported(anyhow::anyhow!(
            "Storage provider does not support conditional writes. \
            PutMode::Create succeeded when it should have failed. \
            This feature is required for fencing in GlideFS."
        ))),
        Err(Error::NotImplemented) => Err(CheckFailure::Unsupported(anyhow::anyhow!(
            "Storage provider does not support conditional writes (PutMode::Create). \
            This feature is required for fencing in GlideFS."
        ))),
        Err(e) => {
            let error_str = e.to_string().to_lowercase();
            if error_str.contains("501") || error_str.contains("not implemented") {
                Err(CheckFailure::Unsupported(anyhow::anyhow!(
                    "Storage provider does not support conditional writes. \
                    This feature is required for fencing in GlideFS.\n\n{e}"
                )))
            } else {
                Err(CheckFailure::Unreachable(anyhow::anyhow!(
                    "Storage provider precondition check failed: {e}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutPayload, PutResult};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A store whose first `unreachable_puts` writes fail the way an unreachable
    /// network does, and which can lack conditional writes.
    #[derive(Debug)]
    struct Flaky {
        inner: InMemory,
        unreachable_puts: AtomicU32,
        conditional_writes: bool,
    }

    impl Flaky {
        fn store(unreachable_puts: u32, conditional_writes: bool) -> Arc<dyn ObjectStore> {
            Arc::new(Self { inner: InMemory::new(), unreachable_puts: AtomicU32::new(unreachable_puts), conditional_writes })
        }
    }

    impl std::fmt::Display for Flaky {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Flaky")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Flaky {
        async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
            let left = self.unreachable_puts.load(Ordering::SeqCst);
            if left > 0 {
                self.unreachable_puts.store(left - 1, Ordering::SeqCst);
                return Err(Error::Generic { store: "Flaky", source: "dns error: Temporary failure in name resolution".into() });
            }
            let opts = if self.conditional_writes { opts } else { PutOptions::default() };
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    const QUICK: Duration = Duration::from_millis(5);

    /// Unreachable storage at startup is waited out, not fatal: the daemon comes up
    /// once it answers (the boot that left glidefs down for 11 days).
    #[tokio::test]
    async fn waits_out_unreachable_storage() {
        let store = Flaky::store(3, true);
        assert!(
            matches!(check_once(&store, "wait").await, Err(CheckFailure::Unreachable(_))),
            "a single attempt against unreachable storage fails as unreachable"
        );
        let waited = tokio::time::timeout(Duration::from_secs(5), wait_for_if_match_support(&store, "wait", QUICK, QUICK)).await;
        assert!(matches!(waited, Ok(Ok(()))), "the wait passes once storage answers: {waited:?}");
    }

    /// A definitive answer that conditional writes are unsupported fails at once —
    /// no retry fixes a misconfiguration.
    #[tokio::test]
    async fn unsupported_storage_fails_without_waiting() {
        let store = Flaky::store(0, false);
        let result =
            tokio::time::timeout(Duration::from_secs(5), wait_for_if_match_support(&store, "unsupported", QUICK, QUICK)).await;
        assert!(
            matches!(&result, Ok(Err(e)) if e.to_string().contains("does not support conditional writes")),
            "an unsupported provider must fail at once, without waiting: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_compatible_store_passes() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let result = check_once(&store, "test-compat").await;
        assert!(result.is_ok(), "InMemory supports PutMode::Create: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_cleans_up_test_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        // Run twice — second run should clean up files from the first
        check_once(&store, "test-cleanup").await.unwrap();
        check_once(&store, "test-cleanup").await.unwrap();

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
