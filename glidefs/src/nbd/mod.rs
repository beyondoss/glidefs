pub mod api;
pub mod block_map;
pub mod cache;
pub mod capacity_monitor;
pub mod error;
pub mod flush_scheduler;
pub mod handler;
pub mod content_store;
pub mod manifest;
pub mod pack;
pub mod pack_index;
pub mod pack_registry;
pub mod readahead;
pub mod scrubber;

pub mod metrics;
pub mod protocol;
pub mod router;
pub mod server;
pub mod state;
pub mod sync;
pub mod wal;
pub mod write_cache;
pub mod write_trace;

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
