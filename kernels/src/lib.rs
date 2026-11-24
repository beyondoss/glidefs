//! GlideFS's decision kernels: pure functions from inputs to decisions, with no I/O, no clock and no runtime. Depends on
//! `vstd` only (`tests/dependencies.rs` checks it), so Verus can verify it where it cannot verify `glidefs`. The proofs live
//! in each module's `verus!` block and `scripts/verify-kernels.sh` checks them.

pub mod attach_kernel;
pub mod cut_hold;
pub mod detach_kernel;
pub mod fence_kernel;
pub mod flush_kernel;
pub mod pack_liveness;
pub mod purge_gate;
pub mod seq_counter;
pub mod snapshot_owner;
