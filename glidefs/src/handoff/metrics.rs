//! Process-wide handoff observability counters.
//!
//! Recorded by [`crate::handoff::predecessor`] / [`crate::handoff::successor`]
//! at each handoff. Exposed via the structured log lines already emitted
//! (search for `outcome=Succeeded`, `duration_ms=`, `recovered_count=`)
//! and via the atomic counters in this module for future Prometheus
//! exposure.
//!
//! ## Usage
//!
//! ```ignore
//! crate::handoff::metrics::record_outcome(HandoffOutcomeKind::Succeeded);
//! crate::handoff::metrics::record_stall_ms(123);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy)]
pub enum HandoffOutcomeKind {
    Succeeded,
    AbortedVersionMismatch,
    AbortedNoCommonStrategy,
    AbortedExportMismatch,
    AbortedWarmingFailed,
    AbortedFreezeFailed,
    AbortedOther,
    RevivedAfterSuccessorCrash,
    RevivedAfterTimeout,
    Failed,
}

/// `glidefs_handoff_outcome_total{result="success"}` equivalent.
pub static OUTCOME_SUCCEEDED: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_ABORTED_VERSION_MISMATCH: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_ABORTED_NO_COMMON_STRATEGY: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_ABORTED_EXPORT_MISMATCH: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_ABORTED_WARMING_FAILED: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_ABORTED_FREEZE_FAILED: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_ABORTED_OTHER: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_REVIVED_AFTER_SUCCESSOR_CRASH: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_REVIVED_AFTER_TIMEOUT: AtomicU64 = AtomicU64::new(0);
pub static OUTCOME_FAILED: AtomicU64 = AtomicU64::new(0);

/// Stall duration histogram buckets (milliseconds).
/// Each bucket counts handoffs whose stall window fell at-or-below it.
/// Cumulative semantics match Prometheus histogram conventions.
pub static STALL_LE_10MS: AtomicU64 = AtomicU64::new(0);
pub static STALL_LE_50MS: AtomicU64 = AtomicU64::new(0);
pub static STALL_LE_100MS: AtomicU64 = AtomicU64::new(0);
pub static STALL_LE_250MS: AtomicU64 = AtomicU64::new(0);
pub static STALL_LE_500MS: AtomicU64 = AtomicU64::new(0);
pub static STALL_LE_1S: AtomicU64 = AtomicU64::new(0);
pub static STALL_LE_5S: AtomicU64 = AtomicU64::new(0);
pub static STALL_LE_30S: AtomicU64 = AtomicU64::new(0);
pub static STALL_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static STALL_SUM_MS: AtomicU64 = AtomicU64::new(0);

pub fn record_outcome(kind: HandoffOutcomeKind) {
    let counter = match kind {
        HandoffOutcomeKind::Succeeded => &OUTCOME_SUCCEEDED,
        HandoffOutcomeKind::AbortedVersionMismatch => &OUTCOME_ABORTED_VERSION_MISMATCH,
        HandoffOutcomeKind::AbortedNoCommonStrategy => &OUTCOME_ABORTED_NO_COMMON_STRATEGY,
        HandoffOutcomeKind::AbortedExportMismatch => &OUTCOME_ABORTED_EXPORT_MISMATCH,
        HandoffOutcomeKind::AbortedWarmingFailed => &OUTCOME_ABORTED_WARMING_FAILED,
        HandoffOutcomeKind::AbortedFreezeFailed => &OUTCOME_ABORTED_FREEZE_FAILED,
        HandoffOutcomeKind::AbortedOther => &OUTCOME_ABORTED_OTHER,
        HandoffOutcomeKind::RevivedAfterSuccessorCrash => {
            &OUTCOME_REVIVED_AFTER_SUCCESSOR_CRASH
        }
        HandoffOutcomeKind::RevivedAfterTimeout => &OUTCOME_REVIVED_AFTER_TIMEOUT,
        HandoffOutcomeKind::Failed => &OUTCOME_FAILED,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn record_stall_ms(ms: u64) {
    STALL_TOTAL.fetch_add(1, Ordering::Relaxed);
    STALL_SUM_MS.fetch_add(ms, Ordering::Relaxed);
    if ms <= 10 {
        STALL_LE_10MS.fetch_add(1, Ordering::Relaxed);
    }
    if ms <= 50 {
        STALL_LE_50MS.fetch_add(1, Ordering::Relaxed);
    }
    if ms <= 100 {
        STALL_LE_100MS.fetch_add(1, Ordering::Relaxed);
    }
    if ms <= 250 {
        STALL_LE_250MS.fetch_add(1, Ordering::Relaxed);
    }
    if ms <= 500 {
        STALL_LE_500MS.fetch_add(1, Ordering::Relaxed);
    }
    if ms <= 1000 {
        STALL_LE_1S.fetch_add(1, Ordering::Relaxed);
    }
    if ms <= 5000 {
        STALL_LE_5S.fetch_add(1, Ordering::Relaxed);
    }
    if ms <= 30000 {
        STALL_LE_30S.fetch_add(1, Ordering::Relaxed);
    }
}

/// Render the histogram and counters in Prometheus text format. Wired
/// to the HTTP API endpoint by future work (task 5.x exposure).
#[allow(dead_code)]
pub fn render_prometheus() -> String {
    use std::fmt::Write;
    let mut out = String::new();

    writeln!(out, "# HELP glidefs_handoff_outcome_total Counter of handoff outcomes by result.").ok();
    writeln!(out, "# TYPE glidefs_handoff_outcome_total counter").ok();
    let outcomes = [
        ("success", &OUTCOME_SUCCEEDED),
        ("aborted_version_mismatch", &OUTCOME_ABORTED_VERSION_MISMATCH),
        ("aborted_no_common_strategy", &OUTCOME_ABORTED_NO_COMMON_STRATEGY),
        ("aborted_export_mismatch", &OUTCOME_ABORTED_EXPORT_MISMATCH),
        ("aborted_warming_failed", &OUTCOME_ABORTED_WARMING_FAILED),
        ("aborted_freeze_failed", &OUTCOME_ABORTED_FREEZE_FAILED),
        ("aborted_other", &OUTCOME_ABORTED_OTHER),
        ("revived_after_successor_crash", &OUTCOME_REVIVED_AFTER_SUCCESSOR_CRASH),
        ("revived_after_timeout", &OUTCOME_REVIVED_AFTER_TIMEOUT),
        ("failed", &OUTCOME_FAILED),
    ];
    for (label, counter) in outcomes {
        writeln!(
            out,
            "glidefs_handoff_outcome_total{{result=\"{}\"}} {}",
            label,
            counter.load(Ordering::Relaxed)
        )
        .ok();
    }

    writeln!(out, "# HELP glidefs_handoff_stall_duration_seconds Histogram of handoff stall durations.").ok();
    writeln!(out, "# TYPE glidefs_handoff_stall_duration_seconds histogram").ok();
    let buckets = [
        ("0.01", &STALL_LE_10MS),
        ("0.05", &STALL_LE_50MS),
        ("0.1", &STALL_LE_100MS),
        ("0.25", &STALL_LE_250MS),
        ("0.5", &STALL_LE_500MS),
        ("1", &STALL_LE_1S),
        ("5", &STALL_LE_5S),
        ("30", &STALL_LE_30S),
    ];
    for (le, counter) in buckets {
        writeln!(
            out,
            "glidefs_handoff_stall_duration_seconds_bucket{{le=\"{}\"}} {}",
            le,
            counter.load(Ordering::Relaxed)
        )
        .ok();
    }
    let total = STALL_TOTAL.load(Ordering::Relaxed);
    writeln!(out, "glidefs_handoff_stall_duration_seconds_bucket{{le=\"+Inf\"}} {total}").ok();
    let sum_ms = STALL_SUM_MS.load(Ordering::Relaxed);
    writeln!(out, "glidefs_handoff_stall_duration_seconds_sum {}", sum_ms as f64 / 1000.0).ok();
    writeln!(out, "glidefs_handoff_stall_duration_seconds_count {total}").ok();

    out
}
