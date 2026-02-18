//! State types for the write-behind NBD cache.
//!
//! This module defines:
//! - `BlockState`: Runtime enum for per-block sync state
//! - Typestate markers for device lifecycle (compile-time enforcement)

use std::fmt;

// ============================================================================
// Block State (Runtime Enum)
// ============================================================================

/// State of a single block in the write cache.
///
/// Transitions:
/// - Clean → Dirty (on write)
/// - Dirty → Syncing (sync worker claims it)
/// - Syncing → Clean (upload success)
/// - Syncing → Dirty (upload failure or new write during sync)
#[derive(Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum BlockState {
    /// Block matches S3 (or was never written).
    /// This is the default state for new/unwritten blocks.
    #[default]
    Clean = 0,

    /// Local SSD has newer data than S3.
    /// Block will be uploaded on next sync cycle.
    Dirty = 1,

    /// Upload to S3 is in progress.
    /// If a write occurs during sync, transitions back to Dirty.
    Syncing = 2,
}

impl BlockState {
    /// Returns true if this block needs to be synced to S3.
    #[inline]
    #[allow(dead_code)]
    pub fn needs_sync(&self) -> bool {
        matches!(self, BlockState::Dirty)
    }

    /// Returns true if this block is currently being uploaded.
    #[inline]
    #[allow(dead_code)]
    pub fn is_syncing(&self) -> bool {
        matches!(self, BlockState::Syncing)
    }

    /// Convert from u8 for deserialization.
    /// Unknown values default to Dirty (conservative - will re-sync).
    #[allow(dead_code)]
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => BlockState::Clean,
            1 => BlockState::Dirty,
            2 => BlockState::Syncing,
            _ => BlockState::Dirty, // Conservative: re-sync unknown states
        }
    }
}

impl fmt::Debug for BlockState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockState::Clean => write!(f, "Clean"),
            BlockState::Dirty => write!(f, "Dirty"),
            BlockState::Syncing => write!(f, "Syncing"),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_state_default() {
        assert_eq!(BlockState::default(), BlockState::Clean);
    }

    #[test]
    fn test_block_state_needs_sync() {
        assert!(!BlockState::Clean.needs_sync());
        assert!(BlockState::Dirty.needs_sync());
        assert!(!BlockState::Syncing.needs_sync());
    }

    #[test]
    fn test_block_state_from_u8() {
        assert_eq!(BlockState::from_u8(0), BlockState::Clean);
        assert_eq!(BlockState::from_u8(1), BlockState::Dirty);
        assert_eq!(BlockState::from_u8(2), BlockState::Syncing);
        // Unknown values default to Dirty
        assert_eq!(BlockState::from_u8(255), BlockState::Dirty);
    }

    #[test]
    fn test_block_state_repr() {
        // Verify repr(u8) is correct for serialization
        assert_eq!(BlockState::Clean as u8, 0);
        assert_eq!(BlockState::Dirty as u8, 1);
        assert_eq!(BlockState::Syncing as u8, 2);
    }
}
