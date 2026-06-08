pub mod block_adapter;
#[cfg(target_os = "linux")]
pub mod boot_capture;
pub mod boot_capture_served;
pub mod boot_set;
pub mod export;
pub mod ext4_store;
pub mod ingest;
pub mod layer_store;
pub mod pull;
pub mod push;

pub use block_adapter::BlockAdapter;
// Re-exports for external consumers (oci-distribution crate, docker integration tests).
#[allow(unused_imports)]
pub use export::export_tar;
#[allow(unused_imports)]
pub use ingest::{IngestOptions, ingest_tar};
#[allow(unused_imports)]
pub use pull::{PullError, pull_image};
#[allow(unused_imports)]
pub use push::{
    ExportedLayer, PushError, PushResult, export_delta_layer, export_full_layer, hex_sha256,
    push_delta_image, push_image,
};
