//! State types for the write-behind NBD cache.
//!
//! Typestate markers for device lifecycle (compile-time enforcement). The
//! markers parameterize `WriteCache<S>`, gating which methods exist in each
//! state, and the lifecycle IS wired up in production: `Initializing` opens
//! the cache, `Recovering`/`Active`/`Draining` each carry `impl WriteCache<S>`
//! blocks, and `shutdown()` transitions `Active -> Draining`.
//!
//! `#![allow(dead_code)]` is set below because the markers are zero-sized and
//! used *only* as type parameters — they are never constructed as values, and
//! the sealed `DeviceState`/`Sealed` traits appear only as bounds, so rustc's
//! dead_code lint false-positives on them despite the types being load-bearing.
//! This is suppression of a known lint limitation, not unfinished code.
#![allow(dead_code)]

// ============================================================================
// Device Lifecycle (Typestate Markers)
// ============================================================================

/// Device is loading local cache and metadata.
/// No I/O operations are allowed in this state.
pub struct Initializing;

/// Device is recovering from a previous session.
/// Uploading any dirty blocks from crash/restart.
/// No I/O operations are allowed in this state.
pub struct Recovering;

/// Device is active and serving I/O.
/// This is the only state where read/write/flush are allowed.
pub struct Active;

/// Device is draining writes to S3 before shutdown.
/// No new writes are accepted.
pub struct Draining;

// Marker trait to seal the state types
mod private {
    pub trait Sealed {}
    impl Sealed for super::Initializing {}
    impl Sealed for super::Recovering {}
    impl Sealed for super::Active {}
    impl Sealed for super::Draining {}
}

/// Marker trait for all device states.
/// Sealed to prevent external implementations.
pub trait DeviceState: private::Sealed {}

impl DeviceState for Initializing {}
impl DeviceState for Recovering {}
impl DeviceState for Active {}
impl DeviceState for Draining {}
