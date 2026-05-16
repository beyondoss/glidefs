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

/// Device is frozen for graceful daemon handoff. In-flight writes have
/// drained, WAL has been fsynced, file handles have been released. New
/// writes return `CacheError::Frozen`; reads still work via the
/// data file (which is reopened after takeover).
///
/// Held only transiently by the predecessor during the CUTOVER step of
/// graceful handoff. The successor process never sees this state — it
/// builds a fresh `Active` cache from disk after the predecessor drops.
#[allow(dead_code)]
pub struct Frozen;

// Marker trait to seal the state types
mod private {
    #[allow(dead_code)]
    pub trait Sealed {}
    impl Sealed for super::Initializing {}
    impl Sealed for super::Recovering {}
    impl Sealed for super::Active {}
    impl Sealed for super::Draining {}
    impl Sealed for super::Frozen {}
}

/// Marker trait for all device states.
/// Sealed to prevent external implementations.
#[allow(dead_code)]
pub trait DeviceState: private::Sealed {}

impl DeviceState for Initializing {}
impl DeviceState for Recovering {}
impl DeviceState for Active {}
impl DeviceState for Draining {}
impl DeviceState for Frozen {}
