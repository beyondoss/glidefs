//! Firecracker microVM [`Sandbox`] backend — the complete boundary for untrusted
//! images.
//!
//! Unlike [`super::namespaces::NamespaceSandbox`], which mounts the image with the
//! *host* kernel (so a crafted fs image can hit a host fs-driver bug), this backend
//! boots a microVM whose **guest kernel** mounts the untrusted fs: the caller's
//! ublk device is handed to the VM as a `virtio-blk` drive (`/dev/vda`), a tiny
//! purpose-built init ([`vm_init/init.c`](vm_init)) mounts it read-only, reads the
//! static-seed closure, and runs the entrypoint under a timeout — while the host
//! ublk read-tracer (backing the drive) records the faulted blocks exactly as
//! before (the tracer is below the VM boundary, so capture is unchanged).
//!
//! The run plan (fs type, env, argv, static seed, timeout) is passed to the init
//! over a second read-only drive (`/dev/vdb`) as a length-prefixed control blob.
//! The host scans the guest console (`VMINIT_*` markers) for the outcome and kills
//! the VM at the deadline; cgroup limits (accident protection) bound the
//! Firecracker process. No cross-repo dependency on `../beyond` — we shell to the
//! `firecracker` binary with a guest kernel + initramfs from `[profile]` config.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use super::cgroup::CgroupGuard;
use super::{RunOutcome, Sandbox, SandboxSpec};

/// The profiling initramfs (`vm_init/init.c` → static musl PID 1), built by
/// `build.rs` into `OUT_DIR`. Empty when the build host lacked `musl-gcc`/`cpio`
/// — then an explicit `[profile] initramfs` is required.
const EMBEDDED_INITRAMFS: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/glidefs-vm-initramfs.cpio"));

pub struct FirecrackerSandbox {
    firecracker_bin: PathBuf,
    kernel_image: PathBuf,
    initramfs: PathBuf,
    mem_mib: u32,
}

impl FirecrackerSandbox {
    /// Validate prerequisites up front (binary + kernel + initramfs + KVM) so
    /// selection fails fast with a clear message rather than mid-run.
    pub fn new(
        firecracker_bin: Option<PathBuf>,
        kernel_image: Option<PathBuf>,
        initramfs: Option<PathBuf>,
        mem_mib: Option<u32>,
    ) -> Result<Self> {
        let firecracker_bin = firecracker_bin.or_else(|| which("firecracker")).context(
            "Firecracker backend: no firecracker binary (set [profile] firecracker_bin)",
        )?;
        require(&firecracker_bin, "firecracker binary")?;
        let kernel_image = kernel_image.context(
            "Firecracker backend: no guest kernel (set [profile] kernel_image, e.g. a \
             firecracker vmlinux)",
        )?;
        require(&kernel_image, "guest kernel")?;
        // Prefer an explicit path; else use the initramfs baked in by build.rs.
        let initramfs = match initramfs {
            Some(p) => {
                require(&p, "initramfs")?;
                p
            }
            None => embedded_initramfs()?,
        };
        if !Path::new("/dev/kvm").exists() {
            anyhow::bail!("Firecracker backend requires /dev/kvm");
        }
        Ok(Self {
            firecracker_bin,
            kernel_image,
            initramfs,
            mem_mib: mem_mib.unwrap_or(512),
        })
    }
}

impl Sandbox for FirecrackerSandbox {
    fn run(&self, spec: &SandboxSpec) -> Result<RunOutcome> {
        if spec.argv.is_empty() {
            anyhow::bail!("no entrypoint argv to run");
        }
        let rand: u64 = rand::random();
        let name = spec.argv.first().map(String::as_str).unwrap_or("profile");
        // Accident protection bounds the Firecracker process (the whole VM).
        let cgroup = CgroupGuard::create(name, rand, &spec.limits);

        // Scratch: control drive + fc config + console log, RAII-removed.
        let scratch = Scratch::new(rand)?;
        let control = scratch.dir.join("control.bin");
        std::fs::write(&control, build_control(spec)).context("write control drive")?;
        let config = scratch.dir.join("fc.json");
        let console = scratch.dir.join("console.log");
        std::fs::write(
            &config,
            fc_config_json(
                &self.kernel_image,
                &self.initramfs,
                &spec.device,
                &control,
                self.mem_mib,
            ),
        )
        .context("write firecracker config")?;

        // Launch firecracker (in the cgroup) with the console captured to a file.
        let log = std::fs::File::create(&console).context("create console log")?;
        let mut cmd = if let Some(procs) = cgroup.procs_path() {
            // Join the cgroup before exec so the whole VM is captured.
            let script = format!(
                "echo $$ > {} 2>/dev/null; exec {} --no-api --config-file {}",
                shq(&procs.to_string_lossy()),
                shq(&self.firecracker_bin.to_string_lossy()),
                shq(&config.to_string_lossy()),
            );
            let mut c = std::process::Command::new("sh");
            c.arg("-c").arg(script);
            c
        } else {
            let mut c = std::process::Command::new(&self.firecracker_bin);
            c.arg("--no-api").arg("--config-file").arg(&config);
            c
        };
        cmd.stdout(log.try_clone()?).stderr(log);
        debug!(device = %spec.device.display(), mem_mib = self.mem_mib, "launching firecracker microVM");
        let mut child = cmd.spawn().context("spawn firecracker")?;

        // Host enforces the deadline (the init self-times-out too, but a wedged
        // guest needs the host backstop). On expiry, kill the VM + cgroup.
        let backstop = spec.timeout + Duration::from_secs(15);
        let start = Instant::now();
        let status = loop {
            match child.try_wait()? {
                Some(st) => break st,
                None => {
                    if start.elapsed() > backstop {
                        warn!("firecracker exceeded backstop deadline — killing VM");
                        cgroup.kill_all();
                        let _ = child.kill();
                        break child.wait()?;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        };

        let console_text = std::fs::read_to_string(&console).unwrap_or_default();
        // Debug affordance: persist the guest console (scratch is RAII-removed).
        if let Some(dst) = std::env::var_os("GLIDEFS_FC_KEEP_CONSOLE") {
            let _ = std::fs::write(dst, &console_text);
        }
        let outcome = outcome_from_console(&console_text, status.success());
        info!(?outcome, "firecracker run complete");
        if !console_text.contains("VMINIT_MOUNT_OK") {
            warn!(
                "guest never reported VMINIT_MOUNT_OK — image mount may have failed (console tail: {})",
                console_text
                    .lines()
                    .rev()
                    .take(3)
                    .collect::<Vec<_>>()
                    .join(" | ")
            );
        }
        Ok(outcome)
    }
}

/// Build the control blob for `/dev/vdb`: 4-byte LE length, then newline records
/// the init parses (`FS=`, `WORKDIR=`, `TIMEOUT=`, `ENV=`, `ARG=`, `SEED=`).
fn build_control(spec: &SandboxSpec) -> Vec<u8> {
    let mut body = String::new();
    body.push_str(&format!("FS={}\n", spec.fs_type));
    let wd = if spec.workdir.is_empty() {
        "/"
    } else {
        &spec.workdir
    };
    body.push_str(&format!("WORKDIR={wd}\n"));
    body.push_str(&format!("TIMEOUT={}\n", spec.timeout.as_secs().max(1)));
    let mut have_path = false;
    for e in &spec.env {
        if e.starts_with("PATH=") {
            have_path = true;
        }
        // Records are newline-delimited; drop any embedded newline defensively.
        body.push_str(&format!("ENV={}\n", e.replace('\n', "")));
    }
    if !have_path {
        body.push_str("ENV=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n");
    }
    for a in &spec.argv {
        body.push_str(&format!("ARG={}\n", a.replace('\n', "")));
    }
    for s in &spec.static_seed {
        body.push_str(&format!("SEED={}\n", s.replace('\n', "")));
    }
    let bytes = body.into_bytes();
    let mut out = Vec::with_capacity(bytes.len() + 4);
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&bytes);
    // Firecracker prefers a drive size that's a multiple of the 512B sector.
    while out.len() % 512 != 0 {
        out.push(0);
    }
    out
}

/// Firecracker `--no-api` config: kernel + initramfs (the VM root), the image as a
/// read-only `virtio-blk` data drive (`/dev/vda`), and the control drive
/// (`/dev/vdb`). `reboot=t` makes the init's triple-fault reset exit Firecracker.
fn fc_config_json(
    kernel: &Path,
    initramfs: &Path,
    image_dev: &Path,
    control: &Path,
    mem_mib: u32,
) -> String {
    format!(
        r#"{{
  "boot-source": {{
    "kernel_image_path": {kernel},
    "initrd_path": {initramfs},
    "boot_args": "console=ttyS0 reboot=t panic=1 pci=off i8042.noaux i8042.nomux i8042.nopnp i8042.dumbkbd quiet"
  }},
  "machine-config": {{ "vcpu_count": 1, "mem_size_mib": {mem_mib} }},
  "drives": [
    {{ "drive_id": "image",   "path_on_host": {image}, "is_root_device": false, "is_read_only": true }},
    {{ "drive_id": "control", "path_on_host": {control}, "is_root_device": false, "is_read_only": true }}
  ]
}}"#,
        kernel = json_str(&kernel.to_string_lossy()),
        initramfs = json_str(&initramfs.to_string_lossy()),
        image = json_str(&image_dev.to_string_lossy()),
        control = json_str(&control.to_string_lossy()),
        mem_mib = mem_mib,
    )
}

/// Decode the run outcome from the guest console markers + the fc exit status.
fn outcome_from_console(console: &str, fc_ok: bool) -> RunOutcome {
    if console.contains("VMINIT_TIMEOUT") {
        return RunOutcome::KilledByTimeout;
    }
    if console.contains("VMINIT_MOUNT_FAIL") || console.contains("VMINIT_CTL_FAIL") {
        // Setup failed inside the guest — nothing meaningful ran.
        return RunOutcome::Exited(127);
    }
    // The init reports the entrypoint's disposition as `VMINIT_EXIT=<code>` (>=128
    // = 128+signal). This catches a silent exec failure (127) the markers alone
    // would hide.
    if let Some(code) = console
        .lines()
        .rev()
        .find_map(|l| l.trim().strip_prefix("VMINIT_EXIT="))
        .and_then(|c| c.trim().parse::<i32>().ok())
    {
        return if code >= 128 {
            RunOutcome::KilledBySignal(code - 128)
        } else {
            RunOutcome::Exited(code)
        };
    }
    if console.contains("VMINIT_DONE") || fc_ok {
        RunOutcome::Exited(0)
    } else {
        // Host killed a wedged VM (no clean reset).
        RunOutcome::KilledByTimeout
    }
}

/// Materialize the build.rs-embedded initramfs to a stable cache path (keyed by
/// content hash) and return it. Errors if the build host had no musl toolchain.
fn embedded_initramfs() -> Result<PathBuf> {
    if EMBEDDED_INITRAMFS.is_empty() {
        anyhow::bail!(
            "Firecracker backend: no initramfs — this build had no musl-gcc/cpio, so the VM init \
             wasn't baked in. Install musl-tools + cpio and rebuild, or set [profile] initramfs."
        );
    }
    let hash = blake3::hash(EMBEDDED_INITRAMFS).to_hex();
    let path = std::env::temp_dir().join(format!("glidefs-vm-initramfs-{}.cpio", &hash[..16]));
    // Write once (content-addressed name → safe to reuse across runs).
    if std::fs::metadata(&path)
        .map(|m| m.len() as usize != EMBEDDED_INITRAMFS.len())
        .unwrap_or(true)
    {
        std::fs::write(&path, EMBEDDED_INITRAMFS).context("materialize embedded initramfs")?;
    }
    Ok(path)
}

/// Scratch dir holding the per-run control blob + config + console; removed on drop.
struct Scratch {
    dir: PathBuf,
}
impl Scratch {
    fn new(rand: u64) -> Result<Self> {
        let dir = std::env::temp_dir().join(format!("glidefs-fc-{rand:x}"));
        std::fs::create_dir_all(&dir).context("create firecracker scratch")?;
        Ok(Self { dir })
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn require(p: &Path, what: &str) -> Result<()> {
    if !p.exists() {
        anyhow::bail!("{what} not found at {}", p.display());
    }
    Ok(())
}

fn which(bin: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|d| d.join(bin))
            .find(|p| p.is_file())
    })
}

/// Minimal JSON string escaping (paths: backslash + quote).
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// POSIX single-quote shell escaping.
fn shq(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn spec() -> SandboxSpec {
        SandboxSpec {
            device: PathBuf::from("/dev/ublkb0"),
            fs_type: "erofs".into(),
            argv: vec!["/bin/sh".into(), "-c".into(), "true".into()],
            env: vec!["FOO=bar".into()],
            workdir: String::new(),
            timeout: Duration::from_secs(20),
            limits: Default::default(),
            static_seed: vec!["/lib/libc.so".into()],
        }
    }

    #[test]
    fn control_blob_is_len_prefixed_and_sector_aligned() {
        let blob = build_control(&spec());
        let len = u32::from_le_bytes(blob[..4].try_into().unwrap()) as usize;
        let body = std::str::from_utf8(&blob[4..4 + len]).unwrap();
        assert!(body.contains("FS=erofs\n"));
        assert!(body.contains("WORKDIR=/\n")); // empty workdir → "/"
        assert!(body.contains("TIMEOUT=20\n"));
        assert!(body.contains("ENV=FOO=bar\n"));
        assert!(body.contains("ENV=PATH=")); // PATH defaulted when absent
        assert!(
            body.contains("ARG=/bin/sh\n")
                && body.contains("ARG=-c\n")
                && body.contains("ARG=true\n")
        );
        assert!(body.contains("SEED=/lib/libc.so\n"));
        assert_eq!(blob.len() % 512, 0, "padded to a 512B sector");
    }

    #[test]
    fn control_blob_keeps_configured_path() {
        let mut s = spec();
        s.env = vec!["PATH=/custom".into()];
        let blob = build_control(&s);
        let body = String::from_utf8(blob[4..].to_vec()).unwrap();
        assert!(body.contains("ENV=PATH=/custom\n"));
        assert_eq!(
            body.matches("ENV=PATH=").count(),
            1,
            "no duplicate default PATH"
        );
    }

    #[test]
    fn outcome_decodes_guest_markers() {
        assert_eq!(
            outcome_from_console("VMINIT_MOUNT_OK\nVMINIT_EXIT=0\nVMINIT_DONE\n", true),
            RunOutcome::Exited(0)
        );
        assert_eq!(
            outcome_from_console("VMINIT_MOUNT_OK\nVMINIT_EXIT=127\nVMINIT_DONE\n", true),
            RunOutcome::Exited(127)
        );
        assert_eq!(
            outcome_from_console("VMINIT_MOUNT_OK\nVMINIT_EXIT=137\nVMINIT_DONE\n", true),
            RunOutcome::KilledBySignal(9)
        );
        assert_eq!(
            outcome_from_console("VMINIT_RUN\nVMINIT_TIMEOUT\nVMINIT_DONE\n", true),
            RunOutcome::KilledByTimeout
        );
        assert_eq!(
            outcome_from_console("VMINIT_MOUNT_FAIL\n", false),
            RunOutcome::Exited(127)
        );
        // No clean reset and no markers → host killed a wedged VM.
        assert_eq!(
            outcome_from_console("garbage\n", false),
            RunOutcome::KilledByTimeout
        );
    }

    #[test]
    fn fc_config_is_valid_json_with_two_drives() {
        let json = fc_config_json(
            Path::new("/k/vmlinux"),
            Path::new("/i/init.cpio"),
            Path::new("/dev/ublkb0"),
            Path::new("/t/control.bin"),
            512,
        );
        let v: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert_eq!(v["machine-config"]["mem_size_mib"], 512);
        assert_eq!(v["drives"].as_array().unwrap().len(), 2);
        assert_eq!(v["drives"][0]["path_on_host"], "/dev/ublkb0");
        assert_eq!(v["drives"][0]["is_read_only"], true);
        assert_eq!(v["boot-source"]["initrd_path"], "/i/init.cpio");
    }
}
