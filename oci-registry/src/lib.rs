mod error;
mod types;

pub use error::Error;
pub use types::*;

use bytes::Bytes;
use futures::Stream;
use oci_client::client::ClientConfig;
use oci_client::manifest::OciManifest;
use std::io;
use std::time::Duration;
use tracing::debug;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(300);

/// OCI registry client for pulling and pushing container images.
///
/// Wraps `oci-client` to provide an ergonomic API for GlideFS integration.
pub struct RegistryClient {
    client: oci_client::Client,
}

impl RegistryClient {
    /// Create a new client with default settings.
    pub fn new() -> Self {
        Self::with_config(ClientConfig {
            connect_timeout: Some(DEFAULT_CONNECT_TIMEOUT),
            read_timeout: Some(DEFAULT_READ_TIMEOUT),
            ..Default::default()
        })
    }

    /// Create a client with custom configuration.
    pub fn with_config(config: ClientConfig) -> Self {
        Self {
            client: oci_client::Client::new(config),
        }
    }

    /// Resolve an image reference to its manifest, config, and layer list.
    ///
    /// If the reference points to an image index (multi-arch), selects the
    /// first platform matching `linux/amd64`. Returns an error if no match.
    pub async fn resolve(
        &self,
        image: &Reference,
        auth: &Credentials,
    ) -> Result<ResolvedImage, Error> {
        let registry_auth = auth.into();

        // Pull the manifest — could be an image manifest or an image index.
        let (manifest, digest) = self.client.pull_manifest(image, &registry_auth).await?;

        let (image_manifest, digest) = match manifest {
            OciManifest::Image(m) => (m, digest),
            OciManifest::ImageIndex(index) => {
                // Find a manifest matching linux/amd64.
                let platform_entry = index
                    .manifests
                    .iter()
                    .find(|entry| {
                        entry.platform.as_ref().is_some_and(|p| {
                            p.os == Os::Linux && p.architecture == Arch::Amd64
                        })
                    })
                    .ok_or(Error::NoPlatformMatch)?;

                debug!(digest = platform_entry.digest, "resolved linux/amd64 from index");

                // Build a reference with the platform-specific digest.
                let platform_ref = Reference::with_digest(
                    image.registry().to_string(),
                    image.repository().to_string(),
                    platform_entry.digest.clone(),
                );

                let (nested, platform_digest) = self
                    .client
                    .pull_manifest(&platform_ref, &registry_auth)
                    .await?;

                match nested {
                    OciManifest::Image(m) => (m, platform_digest),
                    OciManifest::ImageIndex(_) => {
                        return Err(Error::InvalidReference(
                            "nested image index not supported".into(),
                        ));
                    }
                }
            }
        };

        // Fetch config blob.
        let config_descriptor = &image_manifest.config;
        let mut config_bytes = Vec::new();
        self.client
            .pull_blob(image, config_descriptor, &mut config_bytes)
            .await?;

        let layers = image_manifest.layers.clone();

        Ok(ResolvedImage {
            manifest: image_manifest,
            manifest_digest: digest,
            config: config_bytes,
            layers,
        })
    }

    /// Stream a layer blob from the registry.
    ///
    /// Returns an async byte stream. Layers are typically gzip-compressed tar
    /// archives — the caller is responsible for decompression.
    pub async fn pull_layer(
        &self,
        image: &Reference,
        layer: &OciDescriptor,
        auth: &Credentials,
    ) -> Result<impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static, Error> {
        let registry_auth = auth.into();
        self.client
            .auth(image, &registry_auth, oci_client::RegistryOperation::Pull)
            .await?;
        let sized_stream = self.client.pull_blob_stream(image, layer).await?;
        Ok(sized_stream)
    }

    /// Check if a blob exists in the registry (HEAD request).
    pub async fn blob_exists(
        &self,
        image: &Reference,
        digest: &str,
        auth: &Credentials,
    ) -> Result<bool, Error> {
        let registry_auth = auth.into();
        self.client
            .auth(image, &registry_auth, oci_client::RegistryOperation::Pull)
            .await?;
        Ok(self.client.blob_exists(image, digest).await?)
    }

    /// Push a blob to the registry via streaming upload.
    ///
    /// Idempotent — skips upload if the blob already exists. Returns the digest.
    /// Uses the OCI chunked upload flow (POST → PATCH → PUT) so the full blob
    /// never needs to be held in memory.
    pub async fn push_blob<S>(
        &self,
        image: &Reference,
        stream: S,
        digest: &str,
        auth: &Credentials,
    ) -> Result<String, Error>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin,
    {
        let registry_auth = auth.into();
        self.client
            .auth(image, &registry_auth, oci_client::RegistryOperation::Push)
            .await?;

        if self.client.blob_exists(image, digest).await? {
            debug!(digest, "blob already exists, skipping upload");
            return Ok(digest.to_string());
        }

        use futures::StreamExt;
        let mapped = stream.map(|r| {
            r.map_err(|e| oci_client::errors::OciDistributionError::IoError(e))
        });

        let url = self
            .client
            .push_blob_stream(image, mapped, digest)
            .await?;
        debug!(digest, url, "pushed blob");
        Ok(digest.to_string())
    }

    /// Push a manifest to the registry. Returns the manifest digest.
    pub async fn push_manifest(
        &self,
        image: &Reference,
        manifest: &OciImageManifest,
        auth: &Credentials,
    ) -> Result<String, Error> {
        let registry_auth = auth.into();
        self.client
            .auth(image, &registry_auth, oci_client::RegistryOperation::Push)
            .await?;
        let oci_manifest = OciManifest::Image(manifest.clone());
        let digest = self.client.push_manifest(image, &oci_manifest).await?;
        Ok(digest)
    }
}

impl Default for RegistryClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_anonymous() {
        let creds = Credentials::Anonymous;
        let auth: oci_client::secrets::RegistryAuth = (&creds).into();
        assert!(matches!(auth, oci_client::secrets::RegistryAuth::Anonymous));
    }

    #[test]
    fn test_credentials_basic() {
        let creds = Credentials::UsernamePassword {
            username: "user".into(),
            password: "pass".into(),
        };
        let auth: oci_client::secrets::RegistryAuth = (&creds).into();
        assert!(matches!(
            auth,
            oci_client::secrets::RegistryAuth::Basic(_, _)
        ));
    }

    #[test]
    fn test_reference_parse() {
        let r: Reference = "docker.io/library/alpine:3.19".parse().unwrap();
        assert_eq!(r.repository(), "library/alpine");
    }

    /// Pull alpine manifest from Docker Hub (anonymous, no credentials needed).
    /// Ignored by default — requires network access.
    #[tokio::test]
    #[ignore]
    async fn test_resolve_alpine() {
        let client = RegistryClient::new();
        let image: Reference = "docker.io/library/alpine:3.19".parse().unwrap();
        let resolved = client
            .resolve(&image, &Credentials::Anonymous)
            .await
            .unwrap();

        assert!(!resolved.layers.is_empty(), "alpine should have layers");
        assert!(
            !resolved.manifest_digest.is_empty(),
            "should have a digest"
        );
    }

    /// Stream the first layer of alpine and verify it starts with gzip magic.
    #[tokio::test]
    #[ignore]
    async fn test_pull_layer_stream() {
        use futures::StreamExt;

        let client = RegistryClient::new();
        let image: Reference = "docker.io/library/alpine:3.19".parse().unwrap();
        let resolved = client
            .resolve(&image, &Credentials::Anonymous)
            .await
            .unwrap();

        let layer = &resolved.layers[0];
        let mut stream = std::pin::pin!(client
            .pull_layer(&image, layer, &Credentials::Anonymous)
            .await
            .unwrap());

        let first_chunk = stream.next().await.unwrap().unwrap();
        // Gzip magic: 0x1f 0x8b
        assert!(
            first_chunk.len() >= 2 && first_chunk[0] == 0x1f && first_chunk[1] == 0x8b,
            "layer should be gzip-compressed"
        );
    }
}
