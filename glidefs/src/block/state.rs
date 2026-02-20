//! State types for the write-behind NBD cache.
//!
//! Typestate markers for device lifecycle (compile-time enforcement).

// ============================================================================
// Device Lifecycle (Typestate Markers)
// ============================================================================

/// Device is loading local cache and metadata.
/// No I/O operations are allowed in this state.
#[allow(dead_code)]
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
#[allow(dead_code)]
pub struct Draining;

// Marker trait to seal the state types
mod private {
    #[allow(dead_code)]
    pub trait Sealed {}
    impl Sealed for super::Initializing {}
    impl Sealed for super::Recovering {}
    impl Sealed for super::Active {}
    impl Sealed for super::Draining {}
}

/// Marker trait for all device states.
/// Sealed to prevent external implementations.
#[allow(dead_code)]
pub trait DeviceState: private::Sealed {}

impl DeviceState for Initializing {}
impl DeviceState for Recovering {}
impl DeviceState for Active {}
impl DeviceState for Draining {}
