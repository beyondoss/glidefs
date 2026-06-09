//! Build the Firecracker boot-set-profiling initramfs from
//! `src/oci/sandbox/vm_init/init.c` into `OUT_DIR`, so `FirecrackerSandbox` can
//! `include_bytes!` it (no checked-in binary, never drifts from the source).
//!
//! Requires `musl-gcc` + `cpio` (the init is a static PID 1). When the toolchain
//! is absent we still emit an EMPTY placeholder so `include_bytes!` resolves; the
//! runtime then asks for an explicit `[profile] initramfs`. Linux-only (the
//! profiling sandbox is Linux-only); a no-op elsewhere.

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let cpio_out = out_dir.join("glidefs-vm-initramfs.cpio");
    let init_c = Path::new("src/oci/sandbox/vm_init/init.c");
    println!("cargo:rerun-if-changed=src/oci/sandbox/vm_init/init.c");
    println!("cargo:rerun-if-changed=build.rs");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let built = target_os == "linux" && build_initramfs(init_c, &out_dir, &cpio_out);
    if !built {
        std::fs::write(&cpio_out, []).unwrap();
        println!(
            "cargo:warning=glidefs: VM profiling initramfs not built (need musl-gcc + cpio); \
             `glidefs profile --sandbox firecracker` will require [profile] initramfs to be set"
        );
    }
}

fn have(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn build_initramfs(init_c: &Path, out_dir: &Path, cpio_out: &Path) -> bool {
    if !have("musl-gcc") || !have("cpio") {
        return false;
    }
    let init_bin = out_dir.join("glidefs-vm-init");
    let cc = Command::new("musl-gcc")
        .args(["-static", "-Os", "-o"])
        .arg(&init_bin)
        .arg(init_c)
        .status();
    if !matches!(cc, Ok(s) if s.success()) {
        return false;
    }
    let _ = Command::new("strip").arg(&init_bin).status();

    // Stage a dir containing just `init`, then `cpio -o -H newc` it. A single-file
    // archive is deterministic enough; the kernel execs /init from the initramfs.
    let stage = out_dir.join("initramfs-stage");
    let _ = std::fs::remove_dir_all(&stage);
    if std::fs::create_dir_all(&stage).is_err()
        || std::fs::copy(&init_bin, stage.join("init")).is_err()
    {
        return false;
    }
    let Ok(out) = std::fs::File::create(cpio_out) else {
        return false;
    };
    let status = Command::new("sh")
        .arg("-c")
        .arg("printf 'init\\n' | cpio -o -H newc 2>/dev/null")
        .current_dir(&stage)
        .stdout(out)
        .status();
    matches!(status, Ok(s) if s.success())
        && std::fs::metadata(cpio_out)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
}
