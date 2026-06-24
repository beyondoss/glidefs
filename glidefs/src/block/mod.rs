pub mod api;
#[allow(unsafe_code)] // Lock-free AtomicPtr page table requires raw pointer ops
pub mod block_map;
pub mod cache;
#[allow(unsafe_code)] // libc::statvfs FFI
pub mod capacity_monitor;
pub mod error;
pub mod fence;
pub mod flush_scheduler;
pub mod foyer_engine;
pub mod handler;
pub mod content_store;
pub mod manifest;
pub mod pack;
pub mod pack_index_cache;
pub mod readahead;
pub mod registry;
pub mod scrubber;

pub mod metrics;
pub mod protocol;
pub mod router;
pub mod server;
pub mod state;
pub mod sync;
#[allow(unsafe_code)] // O_APPEND + libc::write/ftruncate/fsync FFI
pub mod wal;
pub mod volume_manifest;
#[allow(unsafe_code)] // SIMD zero-check, positional I/O, SyncFile
pub mod write_cache;
pub mod write_trace;

// NBD kernel device management via netlink (Linux 4.10+)
#[cfg(target_os = "linux")]
#[allow(unsafe_code)] // Netlink socket FFI
pub mod nbd;

// ublk transport (Linux 6.0+, io_uring-based userspace block device)
#[cfg(all(target_os = "linux", feature = "ublk"))]
#[allow(unsafe_code)] // io_uring + eventfd FFI, single-threaded executor
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
