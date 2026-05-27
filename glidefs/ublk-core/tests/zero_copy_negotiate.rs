//! Verify the kernel-driven zero-copy auto-enable in `UblkCtrl::new`.
//!
//! Requires a real ublk kernel driver. Skips silently if `/dev/ublk-control`
//! is absent so cargo test on non-Linux or kernel-less CI passes.
//!
//! Run as root (ublk control device is root-only).

use std::path::Path;
use ublk_core::ctrl::{UblkCtrl, UblkCtrlBuilder};
use ublk_core::sys::{UBLK_F_AUTO_BUF_REG, UBLK_F_SUPPORT_ZERO_COPY};
use ublk_core::UblkFlags;

const ZC_BITS: u64 = (UBLK_F_AUTO_BUF_REG | UBLK_F_SUPPORT_ZERO_COPY) as u64;

fn ublk_available() -> bool {
    if !Path::new("/dev/ublk-control").exists() {
        return false;
    }
    // ublk-control is root-only (mode 0600). Without root we can't even
    // open it, so the test would fail spuriously on a CI runner. Skip
    // unless the process is uid 0.
    unsafe { libc::geteuid() == 0 }
}

/// Without the opt-in flag, auto-enable must NOT touch the device flags.
/// Regardless of kernel support, callers that still use `BufDesc::Slice`
/// must keep working unchanged.
#[test]
fn auto_enable_skipped_without_opt_in() {
    if !ublk_available() {
        eprintln!("skipping: /dev/ublk-control absent");
        return;
    }

    let ctrl = UblkCtrlBuilder::default()
        .name("zc-test-no-opt-in")
        .nr_queues(1)
        .depth(4)
        .io_buf_bytes(4096)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()
        .expect("device create");

    let dev_info = ctrl.dev_info();
    assert_eq!(
        dev_info.flags & ZC_BITS,
        0,
        "without UBLK_DEV_F_PREFER_ZERO_COPY the auto-detect must not set ZC bits (flags={:#x})",
        dev_info.flags
    );
}

/// With the opt-in flag, on a kernel that supports both bits, the auto-detect
/// must add them to dev_info.flags. On a kernel without support it must
/// silently leave the flags alone (copy-mode fallback).
#[test]
fn auto_enable_respects_kernel_support() {
    if !ublk_available() {
        eprintln!("skipping: /dev/ublk-control absent");
        return;
    }

    let ctrl = UblkCtrlBuilder::default()
        .name("zc-test-opt-in")
        .nr_queues(1)
        .depth(4)
        .io_buf_bytes(4096)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV | UblkFlags::UBLK_DEV_F_PREFER_ZERO_COPY)
        .build()
        .expect("device create");

    let features = ctrl.get_driver_features().unwrap_or(0);
    let dev_info = ctrl.dev_info();
    let kernel_supports = (features & ZC_BITS) == ZC_BITS;
    let dev_has_zc = (dev_info.flags & ZC_BITS) == ZC_BITS;

    eprintln!(
        "kernel features={:#x} supports_zc={} dev_info.flags={:#x} zc_enabled={}",
        features, kernel_supports, dev_info.flags, dev_has_zc
    );

    if kernel_supports {
        assert!(
            dev_has_zc,
            "kernel supports zero-copy but auto-enable didn't set the bits (flags={:#x})",
            dev_info.flags
        );
    } else {
        assert!(
            !dev_has_zc,
            "kernel does not support zero-copy but bits are set (flags={:#x})",
            dev_info.flags
        );
    }
}
