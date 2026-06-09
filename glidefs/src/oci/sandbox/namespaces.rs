//! Hardened-namespaces [`Sandbox`] backend.
//!
//! Runs the image entrypoint in fresh mount + pid + net + ipc + uts namespaces,
//! `pivot_root`'d onto the image with a fresh `/proc`, a minimal `/dev`, and a
//! read-only `/sys`, with **all capabilities dropped**, `no_new_privs` set, a
//! seccomp allowlist, the network off, and a cgroup v2 cpu/memory/pids cap + hard
//! timeout (accident protection).
//!
//! ## Privileged setup, then drop everything (no user namespace)
//!
//! Profiling is a **root-required** operation (ublk + mounting a real block-device
//! fs, which is not `FS_USERNS_MOUNT`), so the sandbox does the privileged setup
//! (mount, `pivot_root`) as real root, then drops the entire capability set +
//! installs seccomp + `no_new_privs` in the child before `execve`. The workload
//! ends up as uid 0 with an **empty** capability set (so even DAC override is
//! gone — it is subject to normal file permissions) under seccomp, in a pivoted
//! root with a fresh proc and an empty net ns. A user namespace is intentionally
//! omitted: it can't mount the block device anyway, and re-exec'ing the helper
//! through a child userns breaks when the binary lives under a path owned by an
//! unmapped uid. This is the trusted-image boundary; untrusted *images* use the
//! Firecracker backend (a guest kernel owns the mount).
//!
//! The fs mount stays on the host kernel (the device is mounted by the host, then
//! bind-mounted in and pivoted onto), so this backend is **trusted-images-only**
//! ([`super::SandboxConfig`]'s `trusted` gate); untrusted *images* need the
//! Firecracker backend, where a guest kernel owns the mount.
//!
//! ## Process model
//!
//! `unshare(1)` (already a sanctioned dependency) creates the pid/mount/net/ipc/
//! uts namespaces and `--fork`s, exec'ing us again as `glidefs __sandbox_init` —
//! PID 1 of the new pid ns, a freshly-exec'd process (no fork-after-threads
//! hazard). That helper does the in-ns mount/pivot, forks the entrypoint as PID 2
//! (dropping caps + seccomp just before `execve`), reaps it under a precise inner
//! timeout, then reads the static-seed paths under the tracer (the boot-set union)
//! and exits — collapsing the namespace and auto-unmounting everything in it.

#![allow(unsafe_code)]

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::{RunOutcome, Sandbox, SandboxSpec, cgroup::CgroupGuard};

/// Per-file cap on static-seed reads, so a stray huge file can't be slurped
/// whole. The static closure is shared libraries (tens of MiB); this is slack.
const STATIC_SEED_READ_CAP: u64 = 128 * 1024 * 1024;

pub struct NamespaceSandbox {
    /// Absolute path to this binary, re-exec'd as the in-ns helper.
    self_exe: PathBuf,
}

impl NamespaceSandbox {
    pub fn new() -> Result<Self> {
        let self_exe = std::env::current_exe().context("resolve current_exe for sandbox helper")?;
        Ok(Self { self_exe })
    }
}

impl Sandbox for NamespaceSandbox {
    fn run(&self, spec: &SandboxSpec) -> Result<RunOutcome> {
        if unsafe { libc::geteuid() } != 0 {
            bail!("the namespaces sandbox needs root (host-side fs mount + unshare)");
        }
        if spec.argv.is_empty() {
            bail!("no entrypoint argv to run");
        }

        // Accident protection: cgroup limits + the rand suffix avoids collisions.
        let rand: u64 = rand::random();
        let name = spec.argv.first().map(String::as_str).unwrap_or("profile");
        let cgroup = CgroupGuard::create(name, rand, &spec.limits);

        // Host-side mount of the untrusted fs (trusted-gated). RAII-detached.
        let host_root = HostMount::mount(&spec.device, &spec.fs_type, rand)?;

        // Hand the child the host mount path (it bind-pivots onto it) + the run.
        let scratch = tempfile::Builder::new()
            .prefix("glidefs-sbx-spec-")
            .tempdir()?;
        let spec_path = scratch.path().join("init.json");
        let init = SandboxInit {
            host_root: host_root.path().to_path_buf(),
            argv: spec.argv.clone(),
            env: spec.env.clone(),
            workdir: spec.workdir.clone(),
            timeout_secs: spec.timeout.as_secs().max(1),
            static_seed: spec.static_seed.clone(),
        };
        std::fs::write(&spec_path, serde_json::to_vec(&init)?)?;

        // Build: `{ echo $$ > cgroup.procs ; } exec unshare … <exe> __sandbox_init <spec>`.
        // Joining the cgroup before exec captures the whole unshare subtree.
        let mut script = String::new();
        if let Some(procs) = cgroup.procs_path() {
            script.push_str(&format!(
                "echo $$ > {} 2>/dev/null; ",
                shell_quote(&procs.to_string_lossy())
            ));
        }
        script.push_str(&format!(
            "exec unshare --mount --pid --net --ipc --uts --fork \
             --kill-child --propagation private -- {} __sandbox_init {}",
            shell_quote(&self_exe_str(&self.self_exe)),
            shell_quote(&spec_path.to_string_lossy()),
        ));
        debug!(%script, "launching namespace sandbox");

        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg(&script)
            .spawn()
            .context("spawn unshare sandbox (need unshare + root)")?;

        // The helper enforces the precise inner timeout; this is a backstop for a
        // wedged helper. On expiry, cgroup.kill nukes the whole subtree.
        let backstop = spec.timeout + Duration::from_secs(20);
        let start = Instant::now();
        let status = loop {
            match child.try_wait()? {
                Some(st) => break st,
                None => {
                    if start.elapsed() > backstop {
                        warn!("sandbox exceeded backstop deadline — killing subtree");
                        cgroup.kill_all();
                        let _ = child.kill();
                        break child.wait()?;
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
            }
        };

        // The helper encodes the run outcome in its EXIT CODE (a file would land in
        // the read-only, pivoted-away image fs). `unshare --fork` propagates it.
        let outcome = outcome_from_status(&status);
        info!(?outcome, "sandbox run complete");

        // host_root, cgroup, scratch drop here → umount + cgroup teardown.
        Ok(outcome)
    }
}

// ===========================================================================
// Host-side device mount (RAII-detached).
// ===========================================================================

/// The untrusted image fs, mounted read-only by the **host** kernel. Detached on
/// drop, so a panic/early-return never strands the mount.
struct HostMount {
    dir: PathBuf,
    mounted: bool,
}

impl HostMount {
    fn mount(device: &Path, fs_type: &str, rand: u64) -> Result<Self> {
        let dir = std::env::temp_dir().join(format!("glidefs-sbx-root-{rand:x}"));
        std::fs::create_dir_all(&dir).context("create host mount dir")?;
        let mut m = HostMount {
            dir,
            mounted: false,
        };
        let flags = libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV;
        mount_syscall(Some(device), &m.dir, Some(fs_type), flags, None)
            .with_context(|| format!("host mount {} ({fs_type})", device.display()))?;
        m.mounted = true;
        Ok(m)
    }
    fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for HostMount {
    fn drop(&mut self) {
        if self.mounted {
            let c = CString::new(self.dir.as_os_str().as_bytes()).unwrap();
            unsafe { libc::umount2(c.as_ptr(), libc::MNT_DETACH) };
        }
        let _ = std::fs::remove_dir(&self.dir);
    }
}

// ===========================================================================
// The in-namespace helper (`glidefs __sandbox_init <spec.json>`), PID 1.
// ===========================================================================

#[derive(Serialize, Deserialize)]
struct SandboxInit {
    host_root: PathBuf,
    argv: Vec<String>,
    env: Vec<String>,
    workdir: String,
    timeout_secs: u64,
    static_seed: Vec<String>,
}

/// Helper exit-code sentinels. The outcome is encoded in the exit code (not a
/// file — a file would land in the read-only, pivoted-away image fs); `unshare
/// --fork` propagates it, and the host decodes it in [`outcome_from_status`].
const EXIT_TIMEOUT: i32 = 222;
const EXIT_SIGNAL: i32 = 223;
const EXIT_SETUP_FAIL: i32 = 221;

enum WireOutcome {
    Exited(i32),
    Timeout,
    Signal(i32),
}

/// Entry point for the re-exec'd helper. Runs as PID 1 in the new namespaces.
/// Never returns — exits with a code that encodes the run outcome.
pub fn run_sandbox_init(spec_path: &str) -> ! {
    let code = match sandbox_init_inner(spec_path) {
        Ok(WireOutcome::Exited(c)) => c.clamp(0, 199),
        Ok(WireOutcome::Timeout) => EXIT_TIMEOUT,
        Ok(WireOutcome::Signal(_)) => EXIT_SIGNAL,
        Err(e) => {
            eprintln!("[sandbox_init] {e:#}");
            EXIT_SETUP_FAIL
        }
    };
    std::process::exit(code);
}

fn sandbox_init_inner(spec_path: &str) -> Result<WireOutcome> {
    let init: SandboxInit =
        serde_json::from_slice(&std::fs::read(spec_path).context("read sandbox spec")?)?;

    setup_rootfs(&init.host_root).context("set up sandbox rootfs")?;

    // We are now pivot_root'd into the image with a fresh /proc, /dev, /sys.
    // Fork the entrypoint as PID 2; we (PID 1) reap + time it out.
    let entry_pid = unsafe { libc::fork() };
    if entry_pid < 0 {
        bail!("fork entrypoint: {}", std::io::Error::last_os_error());
    }
    if entry_pid == 0 {
        // Child → becomes the entrypoint. Anything here that fails _exit(127).
        exec_entrypoint(&init);
    }

    // PID 1: wait for the entrypoint with a precise inner timeout.
    let outcome = wait_with_timeout(entry_pid, Duration::from_secs(init.timeout_secs));

    // Kill anything else still alive in the ns (orphans the entrypoint forked),
    // then reap, so nothing lingers before we read the static seed.
    unsafe { libc::kill(-1, libc::SIGKILL) };
    reap_all();

    // Boot-set UNION: read the static closure under the tracer so the captured
    // block set covers it even if the entrypoint never touched it. PID 1 holds
    // full caps + no seccomp, so these reads are unconstrained.
    read_static_seed(&init.static_seed);

    Ok(outcome)
}

/// Mount the rootfs (bind of the host mount) + fresh proc/dev/sys, then pivot
/// onto it. All of these are userns-legal (bind/proc/sysfs/tmpfs).
fn setup_rootfs(host_root: &Path) -> Result<()> {
    // Defensive: ensure nothing propagates back out of our mount ns.
    mount_syscall(
        None::<&Path>,
        Path::new("/"),
        None,
        libc::MS_REC | libc::MS_PRIVATE,
        None,
    )
    .context("make / private")?;

    // A private tmpfs to host the new root mountpoint (so we don't litter /tmp).
    let base = std::env::temp_dir().join("glidefs-sbx-ns");
    std::fs::create_dir_all(&base).ok();
    mount_syscall(
        Some("tmpfs"),
        &base,
        Some("tmpfs"),
        libc::MS_NOSUID,
        Some("mode=0755"),
    )
    .context("mount scratch tmpfs")?;
    let new_root = base.join("root");
    std::fs::create_dir_all(&new_root).context("mkdir new_root")?;

    // Bind the host's image mount in as our new root (makes it a mount point too).
    mount_syscall(
        Some(host_root),
        &new_root,
        None,
        libc::MS_BIND | libc::MS_REC,
        None,
    )
    .context("bind image rootfs")?;

    // Fresh /proc (tied to our new pid ns), minimal /dev, read-only /sys — mounted
    // ON TOP of the (read-only) image dirs, which is allowed regardless of the
    // underlying fs being RO.
    let proc = new_root.join("proc");
    if proc.exists() {
        mount_syscall(
            Some("proc"),
            &proc,
            Some("proc"),
            libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
            None,
        )
        .context("mount /proc")?;
    }
    let sys = new_root.join("sys");
    if sys.exists() {
        let _ = mount_syscall(
            Some("sysfs"),
            &sys,
            Some("sysfs"),
            libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
            None,
        );
    }
    setup_dev(&new_root).context("set up /dev")?;

    // pivot_root onto new_root using the put_old == new_root trick (works on a
    // read-only rootfs with no writable .oldroot dir).
    chdir(&new_root)?;
    pivot_root_dot()?;
    umount_old()?;
    chdir(Path::new("/"))?;

    // Cosmetic, proves the uts ns: ignore failure.
    let host = CString::new("sandbox").unwrap();
    unsafe { libc::sethostname(host.as_ptr(), 7) };
    Ok(())
}

/// A small `/dev` (tmpfs) with just the device nodes a runtime needs, bind-mounted
/// from the host nodes (bind of a single node is userns-legal; `mknod` is not).
fn setup_dev(new_root: &Path) -> Result<()> {
    let dev = new_root.join("dev");
    if !dev.exists() {
        // Rare (distroless usually has /dev); best-effort create on the RO fs fails,
        // so just skip — the runtime can still boot without /dev in many cases.
        return Ok(());
    }
    mount_syscall(
        Some("tmpfs"),
        &dev,
        Some("tmpfs"),
        libc::MS_NOSUID,
        Some("mode=0755,size=1m"),
    )
    .context("mount /dev tmpfs")?;
    for node in ["null", "zero", "full", "random", "urandom", "tty"] {
        let src = Path::new("/dev").join(node); // host node, still reachable pre-pivot
        let dst = dev.join(node);
        if src.exists() {
            // Bind target must exist: create an empty file to mount over.
            let _ = std::fs::File::create(&dst);
            let _ = mount_syscall(Some(&src), &dst, None, libc::MS_BIND, None);
        }
    }
    Ok(())
}

/// In the forked child: enter the workdir, lock down, and exec the entrypoint.
/// Never returns on success; `_exit(127)` on any failure.
fn exec_entrypoint(init: &SandboxInit) -> ! {
    let fail = |_e: anyhow::Error| -> ! {
        unsafe { libc::_exit(127) };
    };
    if !init.workdir.is_empty() && init.workdir != "/" {
        // Best-effort: a missing workdir shouldn't abort the run.
        let _ = chdir(Path::new(&init.workdir));
    }
    if let Err(e) = super::caps_seccomp::set_no_new_privs() {
        fail(e);
    }
    if let Err(e) = super::caps_seccomp::drop_all_caps() {
        fail(e);
    }
    if let Err(e) = super::caps_seccomp::apply_seccomp() {
        fail(e);
    }

    // argv + envp as C arrays.
    let argv0 = CString::new(init.argv[0].as_bytes()).unwrap();
    let cargv: Vec<CString> = init
        .argv
        .iter()
        .map(|a| CString::new(a.as_bytes()).unwrap())
        .collect();
    let mut argv_p: Vec<*const libc::c_char> = cargv.iter().map(|c| c.as_ptr()).collect();
    argv_p.push(std::ptr::null());

    let env = build_env(&init.env);
    let cenv: Vec<CString> = env
        .iter()
        .map(|e| CString::new(e.as_bytes()).unwrap())
        .collect();
    let mut env_p: Vec<*const libc::c_char> = cenv.iter().map(|c| c.as_ptr()).collect();
    env_p.push(std::ptr::null());

    unsafe { libc::execvpe(argv0.as_ptr(), argv_p.as_ptr(), env_p.as_ptr()) };
    // execvpe only returns on failure.
    unsafe { libc::_exit(127) };
}

/// Apply PATH/HOME/LANG defaults if the image config didn't set them (mirrors the
/// legacy run path).
fn build_env(env: &[String]) -> Vec<String> {
    let mut out = env.to_vec();
    if !out.iter().any(|e| e.starts_with("PATH=")) {
        out.push("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".into());
    }
    if !out.iter().any(|e| e.starts_with("HOME=")) {
        out.push("HOME=/root".into());
    }
    if !out.iter().any(|e| e.starts_with("LANG=")) {
        out.push("LANG=C".into());
    }
    out
}

/// Poll-wait for `pid` until it exits or `timeout` elapses.
fn wait_with_timeout(pid: libc::pid_t, timeout: Duration) -> WireOutcome {
    let start = Instant::now();
    loop {
        let mut status: libc::c_int = 0;
        let r = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        if r == pid {
            if libc::WIFEXITED(status) {
                return WireOutcome::Exited(libc::WEXITSTATUS(status));
            }
            if libc::WIFSIGNALED(status) {
                return WireOutcome::Signal(libc::WTERMSIG(status));
            }
            return WireOutcome::Exited(0);
        }
        if r < 0 {
            // No such child / already reaped.
            return WireOutcome::Exited(0);
        }
        if start.elapsed() >= timeout {
            return WireOutcome::Timeout;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Reap every remaining child (after `kill(-1)`), so the ns can collapse cleanly.
fn reap_all() {
    loop {
        let mut status: libc::c_int = 0;
        let r = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if r <= 0 {
            break;
        }
    }
}

/// Read each static-seed path (bounded) so its blocks fault under the tracer.
fn read_static_seed(paths: &[String]) {
    use std::io::Read;
    let mut buf = vec![0u8; 1024 * 1024];
    for p in paths {
        let Ok(mut f) = std::fs::File::open(p) else {
            continue;
        };
        let mut total: u64 = 0;
        while total < STATIC_SEED_READ_CAP {
            match f.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => total += n as u64,
            }
        }
    }
}

// ===========================================================================
// libc helpers.
// ===========================================================================

fn mount_syscall(
    src: Option<impl AsRef<Path>>,
    target: &Path,
    fstype: Option<&str>,
    flags: libc::c_ulong,
    data: Option<&str>,
) -> Result<()> {
    let src_c = src.map(|s| CString::new(s.as_ref().as_os_str().as_bytes()).unwrap());
    let tgt_c = CString::new(target.as_os_str().as_bytes()).unwrap();
    let fst_c = fstype.map(|f| CString::new(f).unwrap());
    let data_c = data.map(|d| CString::new(d).unwrap());
    let rc = unsafe {
        libc::mount(
            src_c.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            tgt_c.as_ptr(),
            fst_c.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            flags,
            data_c
                .as_ref()
                .map_or(std::ptr::null(), |c| c.as_ptr().cast()),
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("mount({})", target.display()));
    }
    Ok(())
}

fn chdir(p: &Path) -> Result<()> {
    let c = CString::new(p.as_os_str().as_bytes()).unwrap();
    if unsafe { libc::chdir(c.as_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("chdir({})", p.display()));
    }
    Ok(())
}

/// `pivot_root(".", ".")` — both old and new root are the cwd; stacks the old
/// root over the new, to be detached next. Works on a RO rootfs.
fn pivot_root_dot() -> Result<()> {
    let dot = CString::new(".").unwrap();
    let rc = unsafe { libc::syscall(libc::SYS_pivot_root, dot.as_ptr(), dot.as_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("pivot_root(.,.)");
    }
    Ok(())
}

fn umount_old() -> Result<()> {
    let dot = CString::new(".").unwrap();
    if unsafe { libc::umount2(dot.as_ptr(), libc::MNT_DETACH) } != 0 {
        return Err(std::io::Error::last_os_error()).context("umount2 old root");
    }
    Ok(())
}

fn self_exe_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn outcome_from_status(status: &std::process::ExitStatus) -> RunOutcome {
    use std::os::unix::process::ExitStatusExt;
    if let Some(code) = status.code() {
        // Decode the helper's exit-code sentinels (see `run_sandbox_init`).
        match code {
            EXIT_TIMEOUT => RunOutcome::KilledByTimeout,
            EXIT_SIGNAL => RunOutcome::KilledBySignal(0),
            other => RunOutcome::Exited(other),
        }
    } else if let Some(sig) = status.signal() {
        // The helper itself was signalled (e.g. backstop SIGKILL).
        RunOutcome::KilledBySignal(sig)
    } else {
        RunOutcome::Exited(0)
    }
}

/// Minimal POSIX single-quote shell escaping.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}
