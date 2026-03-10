pub use oci_client::manifest::{OciDescriptor, OciImageIndex, OciImageManifest};
pub use oci_client::Reference;
pub use oci_client::config::{Architecture as Arch, Os};

/// Credentials for registry authentication.
#[derive(Clone, Debug)]
pub enum Credentials {
    /// No authentication (anonymous pull).
    Anonymous,
    /// Username + password (or personal access token).
    UsernamePassword { username: String, password: String },
}

impl From<&Credentials> for oci_client::secrets::RegistryAuth {
    fn from(creds: &Credentials) -> Self {
        match creds {
            Credentials::Anonymous => oci_client::secrets::RegistryAuth::Anonymous,
            Credentials::UsernamePassword { username, password } => {
                oci_client::secrets::RegistryAuth::Basic(username.clone(), password.clone())
            }
        }
    }
}

/// A resolved image: manifest + config + layer descriptors.
pub struct ResolvedImage {
    /// The image manifest.
    pub manifest: OciImageManifest,
    /// The manifest digest (sha256:...).
    pub manifest_digest: String,
    /// Parsed image configuration (JSON bytes).
    pub config: Vec<u8>,
    /// Layer descriptors in bottom-to-top order.
    pub layers: Vec<OciDescriptor>,
}
