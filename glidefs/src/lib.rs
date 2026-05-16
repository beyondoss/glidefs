#![deny(unsafe_code)]

pub mod circuit_breaker;
#[allow(unsafe_code)] // Tests use env::set_var (unsafe in Rust 2024)
pub mod config;
pub mod block;
pub mod oci;
pub mod sd_notify;
pub mod task;

#[allow(unsafe_code)] // libc socket/bind/connect/sendmsg/recvmsg for SCM_RIGHTS
pub mod handoff;

#[cfg(feature = "test-utils")]
pub mod cli;
pub mod parse_object_store;
#[cfg(feature = "test-utils")]
mod storage_compatibility;
