//! When a detach is reported: the proven kernel `glidefs_kernels::detach_kernel`, called verbatim by `Router::{drain_export, teardown_export,
//! remove_export}`. A detach is reported only when the drain left no dirty block (`thm_a_reported_detach_has_flushed_every_dirty_block`).

pub use glidefs_kernels::detach_kernel::{detach_reported, drained, remaining_after_drain};
