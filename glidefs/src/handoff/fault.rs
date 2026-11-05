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
//!
//! [`pause`] points hold a process at a state-machine point instead of
//! killing it, so a test can act on the predecessor (create or remove an
//! export through its API) at an exact moment of the handoff:
//! - `s_pause_in_warming` — successor built its router; before HELLO
//! - `s_pause_before_ready` — successor holds the predecessor's export set;
//!   before READY

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

/// Hold here until the test releases us. Active when `GLIDEFS_PAUSE_AT`
/// names `point`; `GLIDEFS_PAUSE_DIR` is a directory shared with the test.
/// Writes `<dir>/<point>.reached`, then waits for `<dir>/<point>.go`.
/// Gives up after two minutes so a test that dies leaves no orphan.
#[cfg(feature = "test-fault-injection")]
pub async fn pause(point: &str) {
    let (Ok(at), Ok(dir)) = (std::env::var("GLIDEFS_PAUSE_AT"), std::env::var("GLIDEFS_PAUSE_DIR"))
    else {
        return;
    };
    if at != point {
        return;
    }
    let dir = std::path::PathBuf::from(dir);
    tracing::error!(point, "test-fault-injection: pausing");
    let _ = std::fs::write(dir.join(format!("{point}.reached")), b"");
    let go = dir.join(format!("{point}.go"));
    for _ in 0..1200 {
        if go.exists() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[cfg(not(feature = "test-fault-injection"))]
#[inline(always)]
#[allow(dead_code)]
pub async fn pause(_point: &str) {}

// No-op stub for production builds (feature off). Call sites in
// successor/predecessor invoke it unconditionally, but they are themselves
// target/feature-gated, so on some build configurations this stub has no
// compiled caller — allow dead_code to keep those configs warning-clean.
#[cfg(not(feature = "test-fault-injection"))]
#[inline(always)]
#[allow(dead_code)]
pub fn inject(_point: &str) {}
