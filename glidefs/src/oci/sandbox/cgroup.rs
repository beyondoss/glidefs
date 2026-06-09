//! cgroup v2 limits + RAII teardown for the profiled run (accident protection).
//!
//! Creates a throwaway leaf cgroup, applies `memory.max` / `pids.max` / `cpu.max`,
//! and on drop atomically kills every surviving process in it (`cgroup.kill`) and
//! removes the directory. This is the backstop that guarantees a buggy entrypoint
//! cannot OOM the build node, fork-bomb it, or strand processes after the run —
//! independent of the namespace/seccomp machinery, so it holds on every backend.
//!
//! Best-effort: requires the cgroup v2 unified hierarchy with the controllers
//! delegated. If any of that is missing we log and continue *without* limits
//! rather than failing the profile — limits are protection, not correctness.

use std::path::{Path, PathBuf};

use tracing::{debug, warn};

use super::ResourceLimits;

const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const PARENT: &str = "glidefs-profile";

/// An active leaf cgroup. Holds processes for one profiling run; tears the whole
/// thing down on drop.
pub struct CgroupGuard {
    /// Absolute path to the leaf cgroup dir, or `None` if cgroup v2 was
    /// unavailable (the guard then no-ops).
    dir: Option<PathBuf>,
}

impl CgroupGuard {
    /// Create a leaf cgroup `glidefs-profile/<name>-<rand>/` and apply `limits`.
    /// `name` is sanitized; `rand` keeps concurrent profiles from colliding.
    /// Never errors — returns an inert guard if cgroup v2 isn't usable.
    pub fn create(name: &str, rand: u64, limits: &ResourceLimits) -> Self {
        if !is_cgroup_v2() {
            debug!("cgroup v2 unified hierarchy not present — profiling without resource limits");
            return Self { dir: None };
        }
        let parent = Path::new(CGROUP_ROOT).join(PARENT);
        // Best-effort: make sure the controllers we need are delegated to our
        // leaves. Harmless if already enabled or if delegation is refused.
        let _ = std::fs::create_dir_all(&parent);
        let _ = std::fs::write(parent.join("cgroup.subtree_control"), b"+memory +pids +cpu");

        let safe: String = name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        let dir = parent.join(format!("{safe}-{rand:x}"));
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!(error = %e, dir = %dir.display(), "failed to create profiling cgroup — running without limits");
            return Self { dir: None };
        }

        if let Some(max) = limits.memory_max_bytes {
            write_limit(&dir, "memory.max", &max.to_string());
        }
        if let Some(max) = limits.pids_max {
            write_limit(&dir, "pids.max", &max.to_string());
        }
        if let Some(pct) = limits.cpu_max_percent {
            // cpu.max is "<quota_us> <period_us>"; period 100000us = 100ms.
            let quota = u64::from(pct) * 1000;
            write_limit(&dir, "cpu.max", &format!("{quota} 100000"));
        }
        debug!(dir = %dir.display(), "created profiling cgroup");
        Self { dir: Some(dir) }
    }

    /// Path to write a pid into to join this cgroup, if active. The namespace
    /// backend uses this in a `echo $$ > <procs>` wrapper so the whole `unshare`
    /// subtree is captured at birth.
    pub fn procs_path(&self) -> Option<PathBuf> {
        self.dir.as_ref().map(|d| d.join("cgroup.procs"))
    }

    /// Atomically SIGKILL every process in the cgroup (kernel ≥5.14). Idempotent.
    pub fn kill_all(&self) {
        if let Some(dir) = &self.dir {
            let _ = std::fs::write(dir.join("cgroup.kill"), b"1");
        }
    }
}

impl Drop for CgroupGuard {
    fn drop(&mut self) {
        let Some(dir) = self.dir.take() else { return };
        // Kill anything still alive, then remove the (now-empty) cgroup. rmdir
        // fails with EBUSY until the last process exits, so retry briefly.
        let _ = std::fs::write(dir.join("cgroup.kill"), b"1");
        for _ in 0..50 {
            match std::fs::remove_dir(&dir) {
                Ok(()) => return,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
            }
        }
        warn!(dir = %dir.display(), "profiling cgroup did not become removable — leaked");
    }
}

fn write_limit(dir: &Path, file: &str, val: &str) {
    if let Err(e) = std::fs::write(dir.join(file), val.as_bytes()) {
        debug!(error = %e, file, val, "could not apply cgroup limit (controller not delegated?)");
    }
}

/// Is `/sys/fs/cgroup` a cgroup v2 unified hierarchy?
fn is_cgroup_v2() -> bool {
    // The unified hierarchy exposes cgroup.controllers at the root; v1 does not.
    Path::new(CGROUP_ROOT).join("cgroup.controllers").exists()
}
