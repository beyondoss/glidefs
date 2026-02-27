//! SSD capacity monitor with flush backpressure.
//!
//! One instance per node. Polls `statvfs` on the cache directory every 5 seconds
//! and takes escalating action when SSD utilization crosses thresholds:
//!
//! | Utilization | Action                                                       |
//! |-------------|--------------------------------------------------------------|
//! | < 80%       | Normal — no intervention                                     |
//! | ≥ 80%       | Warn — log + metric for alerting                             |
//! | ≥ 90%       | Escalate — pressure-flush dirtiest exports to S3             |
//! | < 80%       | Normal — pressure resolved                                   |
//!
//! Hole-punching clean blocks was considered and rejected: `fallocate(PUNCH_HOLE)`
//! races with `pwrite` at the kernel level, risking silent data corruption.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::router::ExportRouter;

/// Configuration for the capacity monitor.
pub struct CapacityConfig {
    /// SSD utilization ratio that triggers a warning log (default: 0.80).
    pub warn_threshold: f64,
    /// SSD utilization ratio that triggers flush mode escalation (default: 0.90).
    pub escalate_threshold: f64,
    /// Polling interval (default: 5 seconds).
    pub poll_interval: Duration,
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            warn_threshold: 0.80,
            escalate_threshold: 0.90,
            poll_interval: Duration::from_secs(5),
        }
    }
}

/// Current backpressure level, derived from SSD utilization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PressureLevel {
    /// < warn_threshold — no action.
    Normal = 0,
    /// ≥ warn_threshold — log warning, no override.
    Warn = 1,
    /// ≥ escalate_threshold — pressure flush triggered.
    Escalated = 2,
}

impl PressureLevel {
    fn classify(utilization: f64, config: &CapacityConfig) -> Self {
        if utilization >= config.escalate_threshold {
            PressureLevel::Escalated
        } else if utilization >= config.warn_threshold {
            PressureLevel::Warn
        } else {
            PressureLevel::Normal
        }
    }
}

/// Run the capacity monitor loop until shutdown.
///
/// Polls `statvfs` on the router's cache directory at `config.poll_interval`.
/// On SSD pressure, triggers pressure flushes on the dirtiest exports.
pub async fn capacity_monitor(
    router: Arc<ExportRouter>,
    config: CapacityConfig,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut current_level = PressureLevel::Normal;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("capacity monitor shutting down");
                break;
            }
            _ = interval.tick() => {
                let Some(utilization) = statvfs_utilization(router.cache_dir()) else {
                    // statvfs failed — preserve previous pressure level
                    // and skip this tick rather than falsely reporting Normal.
                    continue;
                };
                router.set_ssd_utilization(utilization);

                let new_level = PressureLevel::classify(utilization, &config);

                // Re-flush every poll while sustained at Escalated
                if new_level == PressureLevel::Escalated && current_level == PressureLevel::Escalated {
                    warn!(
                        utilization = format!("{:.1}%", utilization * 100.0),
                        "SSD pressure sustained: flushing dirtiest exports"
                    );
                    if tokio::time::timeout(Duration::from_secs(30), router.pressure_flush()).await.is_err() {
                        warn!("pressure flush timed out after 30s");
                    }
                } else if new_level != current_level {
                    match (current_level, new_level) {
                        // Escalating — pressure-flush dirtiest exports
                        (prev, PressureLevel::Escalated) if prev < PressureLevel::Escalated => {
                            warn!(
                                utilization = format!("{:.1}%", utilization * 100.0),
                                "SSD pressure: flushing dirtiest exports"
                            );
                            if tokio::time::timeout(Duration::from_secs(30), router.pressure_flush()).await.is_err() {
                                warn!("pressure flush timed out after 30s");
                            }
                        }
                        // Entering warn zone
                        (PressureLevel::Normal, PressureLevel::Warn) => {
                            warn!(
                                utilization = format!("{:.1}%", utilization * 100.0),
                                "SSD utilization above warn threshold"
                            );
                        }
                        // Recovering to normal
                        (prev, PressureLevel::Normal) if prev > PressureLevel::Normal => {
                            info!(
                                utilization = format!("{:.1}%", utilization * 100.0),
                                "SSD pressure resolved"
                            );
                        }
                        // De-escalating from Escalated to Warn (keep overrides,
                        // only restore when fully below warn threshold)
                        (PressureLevel::Escalated, PressureLevel::Warn) => {
                            info!(
                                utilization = format!("{:.1}%", utilization * 100.0),
                                "SSD pressure easing (still above warn threshold)"
                            );
                        }
                        _ => {}
                    }
                    current_level = new_level;
                }
            }
        }
    }
}

/// Query SSD utilization via `statvfs`. Returns `Some(ratio)` in [0.0, 1.0],
/// or `None` on error so the caller can preserve the previous reading.
///
/// Uses `f_bavail` (blocks available to unprivileged users) rather than
/// `f_bfree` to match what userspace processes actually see.
fn statvfs_utilization(path: &Path) -> Option<f64> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;

    let c_path = CString::new(path.as_os_str().as_encoded_bytes()).ok()?;

    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if ret != 0 {
        warn!("statvfs failed: {}", std::io::Error::last_os_error());
        return None;
    }

    let stat = unsafe { stat.assume_init() };
    if stat.f_blocks == 0 {
        return Some(0.0);
    }

    Some(1.0 - (stat.f_bavail as f64 / stat.f_blocks as f64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pressure_level_classify() {
        let config = CapacityConfig::default();

        assert_eq!(PressureLevel::classify(0.0, &config), PressureLevel::Normal);
        assert_eq!(PressureLevel::classify(0.5, &config), PressureLevel::Normal);
        assert_eq!(PressureLevel::classify(0.79, &config), PressureLevel::Normal);
        assert_eq!(PressureLevel::classify(0.80, &config), PressureLevel::Warn);
        assert_eq!(PressureLevel::classify(0.85, &config), PressureLevel::Warn);
        assert_eq!(PressureLevel::classify(0.89, &config), PressureLevel::Warn);
        assert_eq!(PressureLevel::classify(0.90, &config), PressureLevel::Escalated);
        assert_eq!(PressureLevel::classify(0.95, &config), PressureLevel::Escalated);
        assert_eq!(PressureLevel::classify(1.0, &config), PressureLevel::Escalated);
    }

    #[test]
    fn test_pressure_level_ordering() {
        assert!(PressureLevel::Normal < PressureLevel::Warn);
        assert!(PressureLevel::Warn < PressureLevel::Escalated);
    }

    #[test]
    fn test_statvfs_on_real_filesystem() {
        // Should return a valid ratio on any real filesystem
        let util = statvfs_utilization(Path::new("/tmp")).expect("statvfs should succeed on /tmp");
        assert!((0.0..=1.0).contains(&util), "utilization out of range: {util}");
    }

    #[test]
    fn test_statvfs_nonexistent_path() {
        // Should return None on error, not panic
        let util = statvfs_utilization(Path::new("/nonexistent/path/that/does/not/exist"));
        assert_eq!(util, None);
    }
}
