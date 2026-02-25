pub mod api;
pub mod block_map;
pub mod cache;
pub mod capacity_monitor;
pub mod chunk_cache;
pub mod chunk_meta;
pub mod error;
pub mod flush_scheduler;
pub mod handler;
pub mod content_store;
pub mod manifest;
pub mod pack;
pub mod readahead;
pub mod scrubber;

pub mod metrics;
pub mod protocol;
pub mod router;
pub mod server;
pub mod state;
pub mod sync;
pub mod wal;
pub mod volume_manifest;
pub mod write_cache;
pub mod write_trace;

// NBD kernel device management via netlink (Linux 4.10+)
#[cfg(target_os = "linux")]
pub mod nbd;

// ublk transport (Linux 6.0+, io_uring-based userspace block device)
#[cfg(all(target_os = "linux", feature = "ublk"))]
pub mod ublk;

// Re-export protocol types for fuzzing
#[cfg(feature = "fuzz")]
pub use protocol::{
    NBDClientFlags, NBDCommand, NBDOptionHeader, NBDRequest, NBDServerHandshake,
    NBD_IHAVEOPT, NBD_MAGIC, NBD_REQUEST_MAGIC,
};

// Re-exports for library API
#[allow(unused_imports)]
pub use metrics::{ExportMetrics, MetricsSnapshot};
#[allow(unused_imports)]
pub use router::{ExportInfo, ExportRouter, RouterError};
#[allow(unused_imports)]
pub use server::NBDServer;
#[allow(unused_imports)]
pub use state::{Active, DeviceState, Draining, Initializing, Recovering};
#[allow(unused_imports)]
pub use write_cache::{CacheError, WriteCache, WriteCacheConfig};
