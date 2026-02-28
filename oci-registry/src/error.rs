use oci_client::errors::OciDistributionError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("registry: {0}")]
    Registry(#[from] OciDistributionError),

    #[error("invalid reference: {0}")]
    InvalidReference(String),

    #[error("no matching platform in image index")]
    NoPlatformMatch,

    #[error("config parse: {0}")]
    ConfigParse(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
