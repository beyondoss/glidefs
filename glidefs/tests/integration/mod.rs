//! Integration tests for GlideFS NBD cache.
//!
//! These tests verify the key architectural claims:
//! 1. Wake from any node - read-through cache fetches from S3
//! 2. Instant snapshot - flush() is local SSD only
//! 3. Failure recovery - graceful handling of S3/network failures
//! 4. Crash recovery - Syncing blocks survive process crashes

mod crash_recovery;
mod failure_injection;
mod property_tests;
mod wake_any_node;
