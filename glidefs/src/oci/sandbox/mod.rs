//! Pluggable isolation for running an image entrypoint during boot-set profiling.
//!
//! The boot-set profiler ([`crate::oci::boot_capture_served`]) serves a blessed
//! image over a throwaway ublk device with a read-fault recorder attached, then
//! runs the image's entrypoint so the kernel faults the boot working set and the
//! recorder captures it. **How** the entrypoint is isolated is independent of
//! **how** blocks are captured — the read-tracer observes faults at the ublk
//! layer, below any namespace/VM boundary — so the run step is abstracted behind
//! the [`Sandbox`] trait and the backend is swappable.
//!
//! Two concerns, deliberately separated:
//!
//! * **Accident protection** (every backend, always): a hard wall-clock timeout
//!   that kills the *whole* process tree, cgroup v2 cpu/memory/pids limits, and
//!   leak-proof teardown — for a buggy *trusted* entrypoint that hangs or runs
//!   away. Not speculative; we have direct evidence of runaway boots.
//! * **Attack protection** (backend-dependent): a real isolation boundary for an
//!   untrusted image. [`namespaces::NamespaceSandbox`] (user+pid+mount+net+ipc+
//!   uts namespaces, `pivot_root`, fresh `/proc`+`/dev`+`/sys`, dropped caps,
//!   `no_new_privs`, a seccomp allowlist) fully contains untrusted *userspace*
//!   but shares the host kernel, so it stays **trusted-only** for the fs mount.
//!   [`firecracker::FirecrackerSandbox`] runs the mount + entrypoint in a guest
//!   kernel — the complete boundary for untrusted *images* — and is selected per
//!   operator via config.

#[cfg(target_os = "linux")]
pub mod caps_seccomp;
#[cfg(target_os = "linux")]
pub mod cgroup;
#[cfg(target_os = "linux")]
pub mod firecracker;
#[cfg(target_os = "linux")]
pub mod namespaces;

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// cgroup v2 resource caps for the profiled run (accident protection). `None`
/// fields are left unset (unlimited). Best-effort: if cgroup v2 is unavailable
/// the limits are skipped with a warning rather than failing the run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// `memory.max` in bytes — OOM-kills the run instead of the build node.
    pub memory_max_bytes: Option<u64>,
    /// `pids.max` — caps a fork bomb.
    pub pids_max: Option<u64>,
    /// `cpu.max` as a percent of one CPU (e.g. 200 = 2 cores) — caps a spinner.
    pub cpu_max_percent: Option<u32>,
}

impl ResourceLimits {
    /// Sensible defaults for a boot probe: 4 GiB RAM, 4096 pids, 400% CPU.
    pub fn profiling_defaults() -> Self {
        Self {
            memory_max_bytes: Some(4 * 1024 * 1024 * 1024),
            pids_max: Some(4096),
            cpu_max_percent: Some(400),
        }
    }
}

/// Everything the backend needs to mount the image and run the entrypoint. The
/// caller owns the ublk device + read-tracer + their teardown; the sandbox only
/// mounts `device` and runs `argv` against it under isolation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxSpec {
    /// Block device serving the blessed image (e.g. `/dev/ublkb0`). Mounted RO.
    pub device: PathBuf,
    /// Filesystem driver: `"erofs"` | `"ext4"` (future `"btrfs"`/`"f2fs"`).
    pub fs_type: String,
    /// Entrypoint argv (argv[0] is an in-image path or PATH-resolved name).
    pub argv: Vec<String>,
    /// `KEY=VALUE` environment for the entrypoint. PATH/HOME/LANG are defaulted
    /// by the backend if absent (mirrors the legacy run path).
    pub env: Vec<String>,
    /// In-image working directory (`""`/`"/"` => root).
    pub workdir: String,
    /// Hard wall-clock deadline. Servers run forever; their startup reads ARE
    /// the boot set, so the backend kills the whole tree after this.
    pub timeout: Duration,
    /// cgroup v2 caps (accident protection).
    pub limits: ResourceLimits,
    /// Absolute in-image paths to additionally read under the tracer after the
    /// entrypoint window, so the captured block set unions the static boot-set
    /// closure ([`crate::oci::boot_set::derive_boot_paths`]) by construction.
    pub static_seed: Vec<String>,
}

/// How a sandboxed run ended. The boot set is captured out-of-band by the
/// tracer, so this only reports liveness. `KilledByTimeout` is the *expected*
/// outcome for a long-running server: it booted, then we killed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The entrypoint returned on its own with this exit code.
    Exited(i32),
    /// The wall-clock deadline fired and the tree was killed (expected for servers).
    KilledByTimeout,
    /// The entrypoint died on this signal.
    KilledBySignal(i32),
}

impl RunOutcome {
    /// Whether a boot set should be trusted from this outcome. A clean exit and a
    /// timeout-kill both mean the boot ran; only a crash-before-start is suspect —
    /// but even then the tracer may have captured useful blocks, so this is
    /// advisory (the caller decides based on whether any blocks were recorded).
    pub fn booted(self) -> bool {
        !matches!(self, RunOutcome::Exited(code) if code == 127)
    }
}

/// Run an image entrypoint under isolation. Blocking — call under
/// `spawn_blocking`. MUST fully tear down (process tree, mounts, cgroup) on every
/// return path before returning.
pub trait Sandbox: Send + Sync {
    fn run(&self, spec: &SandboxSpec) -> anyhow::Result<RunOutcome>;
}

/// Which isolation backend to use. Operator-selectable (config `[profile] sandbox`
/// or the `glidefs profile --sandbox` flag).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxKind {
    /// Hardened Linux namespaces. Default. Trusted images only (shares the host
    /// kernel for the fs mount).
    #[serde(alias = "ns", alias = "namespaces")]
    Namespaces,
    /// Firecracker microVM. The complete boundary for untrusted images.
    Firecracker,
}

impl Default for SandboxKind {
    fn default() -> Self {
        SandboxKind::Namespaces
    }
}

impl std::str::FromStr for SandboxKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "ns" | "namespaces" | "namespace" => Ok(SandboxKind::Namespaces),
            "firecracker" | "fc" | "vm" => Ok(SandboxKind::Firecracker),
            other => anyhow::bail!("unknown sandbox backend '{other}' (use 'ns' or 'firecracker')"),
        }
    }
}

/// Backend selection + the parameters each backend needs. Built from the
/// `[profile]` config and/or CLI flags.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    pub kind: SandboxKind,
    /// Whether the image being profiled is trusted (first-party). The namespaces
    /// backend mounts the image with the **host** kernel, so an untrusted image
    /// must use Firecracker; selecting `ns` for an untrusted image is refused.
    pub trusted: bool,
    /// Path to the `firecracker` binary (Firecracker backend).
    pub firecracker_bin: Option<PathBuf>,
    /// Path to the guest kernel image (Firecracker backend).
    pub kernel_image: Option<PathBuf>,
    /// Path to the profiling initramfs (the `glidefs-vm-init` cpio — see
    /// `oci/sandbox/vm_init/build.sh`). Firecracker backend.
    pub initramfs: Option<PathBuf>,
    /// Guest RAM in MiB (Firecracker backend; default 512).
    pub mem_mib: Option<u32>,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            kind: SandboxKind::Namespaces,
            trusted: true,
            firecracker_bin: None,
            kernel_image: None,
            initramfs: None,
            mem_mib: None,
        }
    }
}

/// Build the selected backend, enforcing the trusted-image gate.
pub fn select_sandbox(cfg: &SandboxConfig) -> anyhow::Result<Box<dyn Sandbox>> {
    match cfg.kind {
        SandboxKind::Namespaces => {
            if !cfg.trusted {
                anyhow::bail!(
                    "the namespaces sandbox shares the host kernel and is unsafe for untrusted \
                     images — profile untrusted images with --sandbox firecracker"
                );
            }
            #[cfg(target_os = "linux")]
            {
                Ok(Box::new(namespaces::NamespaceSandbox::new()?))
            }
            #[cfg(not(target_os = "linux"))]
            {
                anyhow::bail!("the namespaces sandbox is Linux-only")
            }
        }
        SandboxKind::Firecracker => {
            #[cfg(target_os = "linux")]
            {
                Ok(Box::new(firecracker::FirecrackerSandbox::new(
                    cfg.firecracker_bin.clone(),
                    cfg.kernel_image.clone(),
                    cfg.initramfs.clone(),
                    cfg.mem_mib,
                )?))
            }
            #[cfg(not(target_os = "linux"))]
            {
                anyhow::bail!("the Firecracker sandbox is Linux-only")
            }
        }
    }
}
