//! Fault-injection hooks for handoff integration tests.
//!
//! Only compiled when `--features test-fault-injection`. Each variant
//! corresponds to a state-machine point where we want to verify recovery.
//! The `INJECT_AT` env var, set by the test harness before fork+exec,
//! names a point — when execution hits that point, the process exits(1).
//!
//! Variants:
//! - `s_crash_after_warming` — successor exits before sending READY
//! - `s_crash_after_ready` — successor exits between READY and CUTOVER
//! - `s_crash_after_cutover` — successor exits between PREDS_DEAD and ALIVE
//! - `p_crash_after_hello_ack` — predecessor exits after HELLO_ACK
//! - `p_crash_during_freeze` — predecessor exits between READY and CUTOVER
//! - `p_crash_after_cutover` — predecessor exits between CUTOVER and PREDS_DEAD
//!
//! Tests assert that every variant ends with zero I/O errors observed by
//! fio and zero data loss per the side-channel oracle.

#[cfg(feature = "test-fault-injection")]
pub fn inject(point: &str) {
    if let Ok(name) = std::env::var("GLIDEFS_INJECT_FAILURE") {
        if name == point {
            tracing::error!(
                point,
                "test-fault-injection: triggering deliberate exit at this state"
            );
            // Simulates an abrupt process crash at this state-machine point:
            // `exit(1)` runs NO destructors — no WAL fdatasync, no File flush,
            // no graceful socket teardown. That is the intent. The recovery
            // tests assert the system survives losing whatever was not already
            // durable here, so anything still buffered must NOT be flushed.
            // The sleep only gives the async tracing writer a chance to emit
            // the line above for test diagnostics; it does not flush any
            // on-disk state the oracle depends on.
            std::thread::sleep(std::time::Duration::from_millis(50));
            std::process::exit(1);
        }
    }
}

// No-op stub for production builds (feature off). Call sites in
// successor/predecessor invoke it unconditionally, but they are themselves
// target/feature-gated, so on some build configurations this stub has no
// compiled caller — allow dead_code to keep those configs warning-clean.
#[cfg(not(feature = "test-fault-injection"))]
#[inline(always)]
#[allow(dead_code)]
pub fn inject(_point: &str) {}
