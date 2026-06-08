//! Runtime boot-set capture via fanotify (the `--profile` producer).
//!
//! Static derivation ([`crate::oci::boot_set`]) captures only the entrypoint's
//! ELF `DT_NEEDED` closure — which is the whole boot set for a compiled binary,
//! but ~50% of it for an interpreter that `dlopen`s extensions and reads data at
//! startup (measured: python ≈ 50% static coverage, so the reorder delivered no
//! cold-boot win). This module closes that gap the only way possible: it RUNS the
//! image once and records every file the boot actually opens.
//!
//! Mechanism (single pass, no ublk):
//!   1. extract the merged OCI rootfs (whiteout-aware) to a scratch dir,
//!   2. bind-mount it onto itself so an fanotify `FAN_MARK_MOUNT` is scoped to it,
//!   3. mark `FAN_OPEN` on that mount,
//!   4. `chroot` + run the image's entrypoint (real Cmd/Env) under a hard
//!      timeout in a fresh network namespace (no network), bind-mounting
//!      /proc /dev /sys (separate mounts → NOT captured),
//!   5. drain fanotify events → readlink `/proc/self/fd/<fd>` → strip the rootfs
//!      prefix → the in-image path the boot read.
//!
//! The captured set feeds [`WriterOption::PriorityOrder`] exactly like the static
//! list — paths not present in the actual image layers are skipped by the writer,
//! so runtime-created temp files are harmlessly ignored. Best-effort and
//! non-fatal: any failure (no root, no fanotify, entrypoint won't start) returns
//! an error and bless falls back to the static derivation.
//!
//! Requires root (chroot/mount/fanotify) and an architecture-compatible
//! entrypoint. NOT a complete sandbox — it runs untrusted image code with only
//! chroot + network isolation + a timeout; treat the builder accordingly.

#![cfg(target_os = "linux")]
// libc FFI: fanotify_init/fanotify_mark/read/close + geteuid — the fanotify API
// has no safe wrapper in our nix version. All pointers are local stack buffers.
#![allow(unsafe_code)]

use std::collections::HashSet;
use std::ffi::CString;
use std::io::{Read, Seek, SeekFrom};
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tracing::{debug, info, warn};

/// Statically derive *and* runtime-capture the boot set, returning their union as
/// absolute priority paths. The runtime capture is the real producer; the static
/// closure backstops anything the timed run happened not to open. `layers` are
/// left rewound for the caller's converter.
pub fn capture_boot_paths<R: Read + Seek>(
    config: &[u8],
    layers: &mut [R],
    scratch: &Path,
    timeout: Duration,
) -> Result<Vec<String>> {
    if unsafe { libc::geteuid() } != 0 {
        bail!("boot profiling needs root (chroot/mount/fanotify)");
    }
    let (mut argv, env, workdir) = crate::oci::boot_set::run_command(config);
    // Operators can override the boot command — the image default Cmd (e.g. a
    // bare `python3` REPL) often exercises far less than the real app's startup.
    // `GLIDEFS_PROFILE_CMD` is run via `/bin/sh -c` inside the image.
    if let Ok(cmd) = std::env::var("GLIDEFS_PROFILE_CMD") {
        if !cmd.trim().is_empty() {
            argv = vec!["/bin/sh".into(), "-c".into(), cmd];
        }
    }
    if argv.is_empty() {
        bail!("image config names no entrypoint/cmd to run");
    }

    let root = scratch.join("boot-profile-root");
    let _ = remove_any(&root);
    std::fs::create_dir_all(&root).context("create profile rootfs dir")?;

    // Extraction + run both need the layers from the start; the converter reseeks
    // afterward, but rewind defensively for it.
    let result = (|| -> Result<Vec<String>> {
        extract_merged_rootfs(layers, &root).context("extract rootfs")?;
        let root = std::fs::canonicalize(&root).context("canonicalize rootfs")?;
        run_and_trace(&root, &argv, &env, &workdir, timeout)
    })();
    let _ = remove_any(&root);
    for layer in layers.iter_mut() {
        let _ = layer.seek(SeekFrom::Start(0));
    }
    result
}

// ===========================================================================
// Step 1: extract the merged rootfs (whiteout-aware), bottom-to-top.
// ===========================================================================

fn extract_merged_rootfs<R: Read + Seek>(layers: &mut [R], root: &Path) -> Result<()> {
    for layer in layers.iter_mut() {
        layer.seek(SeekFrom::Start(0))?;
        let mut ar = tar::Archive::new(&mut *layer);
        ar.set_preserve_permissions(true);
        ar.set_overwrite(true);
        for entry in ar.entries()? {
            let mut entry = entry?;
            let Ok(p) = entry.path() else { continue };
            let norm = p.to_string_lossy().trim_start_matches("./").trim_start_matches('/').to_string();
            if norm.is_empty() {
                continue;
            }
            let (dir, file) = norm.rsplit_once('/').unwrap_or(("", norm.as_str()));

            if file == ".wh..wh..opq" {
                // Opaque: drop everything currently under `dir` (lower layers).
                let target = root.join(dir);
                if let Ok(rd) = std::fs::read_dir(&target) {
                    for ent in rd.flatten() {
                        let _ = remove_any(&ent.path());
                    }
                }
                continue;
            }
            if let Some(base) = file.strip_prefix(".wh.") {
                let target = if dir.is_empty() { root.join(base) } else { root.join(dir).join(base) };
                let _ = remove_any(&target);
                continue;
            }
            // Best-effort: device nodes / unsupported types may fail without the
            // right caps; we only need readable regular files, dirs, and links.
            let _ = entry.unpack_in(root);
        }
    }
    Ok(())
}

fn remove_any(p: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(p) {
        Ok(m) if m.is_dir() => std::fs::remove_dir_all(p),
        Ok(_) => std::fs::remove_file(p),
        Err(_) => Ok(()),
    }
}

// ===========================================================================
// Steps 2-5: bind-mount + fanotify-mark + chroot-run + drain events.
// ===========================================================================

/// Unmounts its paths (in reverse) on drop, so a mid-run failure never leaks the
/// bind mounts or strands the scratch dir as un-removable.
struct Mounts(Vec<PathBuf>);
impl Drop for Mounts {
    fn drop(&mut self) {
        for p in self.0.iter().rev() {
            let _ = Command::new("umount").arg("-l").arg(p).status();
        }
    }
}

/// Closes the fanotify fd on drop.
struct Fan(c_int);
impl Drop for Fan {
    fn drop(&mut self) {
        if self.0 >= 0 {
            unsafe { libc::close(self.0) };
        }
    }
}

fn run_and_trace(
    root: &Path,
    argv: &[String],
    env: &[String],
    workdir: &str,
    timeout: Duration,
) -> Result<Vec<String>> {
    // fanotify: notification class, non-blocking so the reader can poll.
    let fan = unsafe {
        libc::fanotify_init(
            libc::FAN_CLASS_NOTIF | libc::FAN_CLOEXEC | libc::FAN_NONBLOCK,
            libc::O_RDONLY as u32,
        )
    };
    if fan < 0 {
        return Err(std::io::Error::last_os_error()).context("fanotify_init (need root + CONFIG_FANOTIFY)");
    }
    let fan = Fan(fan);

    // Bind-mount the rootfs onto itself so it becomes its own mount → a
    // FAN_MARK_MOUNT mark is scoped to exactly this subtree.
    let mut mounts = Mounts(Vec::new());
    bind_mount(root, root).context("bind-mount rootfs self")?;
    mounts.0.push(root.to_path_buf());

    let croot = CString::new(root.as_os_str().as_bytes())?;
    let rc = unsafe {
        libc::fanotify_mark(
            fan.0,
            libc::FAN_MARK_ADD | libc::FAN_MARK_MOUNT,
            libc::FAN_OPEN,
            libc::AT_FDCWD,
            croot.as_ptr(),
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("fanotify_mark");
    }

    // /proc /dev /sys are separate mounts under root → child opens of them are
    // NOT captured (we only want image files), but the entrypoint needs them.
    for sub in ["proc", "dev", "sys"] {
        let dst = root.join(sub);
        if std::fs::create_dir_all(&dst).is_ok() && bind_mount(Path::new(&format!("/{sub}")), &dst).is_ok() {
            mounts.0.push(dst);
        }
    }

    // Launch: no network (unshare -n), hard-kill after the timeout (servers run
    // forever — their startup reads ARE the boot set), inside the chroot.
    let secs = timeout.as_secs().max(1).to_string();
    let mut cmd = Command::new("unshare");
    cmd.arg("--net").arg("timeout").arg("--signal=KILL").arg(&secs).arg("chroot").arg(root);
    if workdir.is_empty() || workdir == "/" {
        cmd.args(argv);
    } else {
        // chroot doesn't honor WorkingDir; emulate via a shell cd when set.
        let joined = argv.iter().map(|a| shell_quote(a)).collect::<Vec<_>>().join(" ");
        cmd.args(["/bin/sh", "-c", &format!("cd {} 2>/dev/null; exec {joined}", shell_quote(workdir))]);
    }
    cmd.env_clear();
    let mut have_path = false;
    for e in env {
        if let Some((k, v)) = e.split_once('=') {
            if k == "PATH" {
                have_path = true;
            }
            cmd.env(k, v);
        }
    }
    if !have_path {
        cmd.env("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");
    }
    cmd.env("HOME", "/root");
    cmd.env("LANG", "C");

    info!(?argv, timeout_s = %secs, "boot profiling: running entrypoint under fanotify");
    let mut child = cmd.spawn().context("spawn chroot workload (need unshare/timeout/chroot)")?;

    // Drain events until the workload exits (or a safety deadline), then a final
    // sweep for anything queued right before exit.
    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + timeout + Duration::from_secs(10);
    loop {
        drain_events(fan.0, root, &mut out, &mut seen);
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => {}
            Err(e) => {
                warn!(error = %e, "boot profiling: wait failed");
                break;
            }
        }
        if Instant::now() > deadline {
            warn!("boot profiling: workload exceeded deadline; killing");
            let _ = child.kill();
            let _ = child.wait();
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    drain_events(fan.0, root, &mut out, &mut seen);

    // `mounts` drops here → unmounts in reverse (children before the self-mount).
    drop(mounts);

    if out.is_empty() {
        bail!("workload opened no image files — entrypoint may have failed to start");
    }
    debug!(count = out.len(), "boot profiling: captured opens");
    Ok(out)
}

fn bind_mount(src: &Path, dst: &Path) -> Result<()> {
    let st = Command::new("mount").arg("--bind").arg(src).arg(dst).status()?;
    if !st.success() {
        bail!("mount --bind {} {} failed: {st}", src.display(), dst.display());
    }
    Ok(())
}

/// Read all currently-queued fanotify events, resolve each to its in-image path,
/// and append newly-seen ones. Each event carries an open fd we must close.
fn drain_events(fan: c_int, root: &Path, out: &mut Vec<String>, seen: &mut HashSet<String>) {
    const META: usize = std::mem::size_of::<libc::fanotify_event_metadata>();
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = unsafe { libc::read(fan, buf.as_mut_ptr().cast(), buf.len()) };
        if n <= 0 {
            break; // EAGAIN (empty) or error
        }
        let n = n as usize;
        let mut off = 0;
        while off + META <= n {
            let meta: libc::fanotify_event_metadata =
                unsafe { std::ptr::read_unaligned(buf.as_ptr().add(off).cast()) };
            let evlen = meta.event_len as usize;
            if evlen < META || off + evlen > n {
                break;
            }
            if meta.vers == libc::FANOTIFY_METADATA_VERSION && meta.fd >= 0 {
                if let Some(p) = fd_to_image_path(meta.fd, root) {
                    if seen.insert(p.clone()) {
                        out.push(p);
                    }
                }
            }
            if meta.fd >= 0 {
                unsafe { libc::close(meta.fd) };
            }
            off += evlen;
        }
    }
}

/// `readlink /proc/self/fd/<fd>` → host path → strip the rootfs prefix → the
/// absolute in-image path. `None` if the target is outside the rootfs.
fn fd_to_image_path(fd: c_int, root: &Path) -> Option<String> {
    let target = std::fs::read_link(format!("/proc/self/fd/{fd}")).ok()?;
    let rel = target.strip_prefix(root).ok()?;
    Some(format!("/{}", rel.to_string_lossy()))
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}
