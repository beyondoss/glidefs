//! Compile-time layout assertions for FFI types we transmute through.
//!
//! These guard against silent ABI drift between (a) the `io_uring` crate's
//! `squeue::Entry`, which `ublk_core::override_sqe!` transmutes into a
//! field-accessible `RawSqe`, and (b) the kernel's `ublksrv_io_cmd`, which
//! `ublk_core::io.rs` transmutes into a 16-byte array to fit the io_uring
//! `UringCmd16` SQE inline-data slot.
//!
//! If any of these `assert_eq_size!` / `assert_eq_align!` lines stop
//! compiling, a dependency bumped its struct layout — investigate
//! immediately and bump the corresponding ublk-core transmute, do NOT
//! suppress the assertion.

use static_assertions::{assert_eq_size, const_assert};

// `ublk_core::override_sqe!` (vendored at glidefs/ublk-core/src/io.rs:478)
// transmutes `&mut io_uring::squeue::Entry` to `&mut ublk_core::io::RawSqe`
// so the macro can override individual SQE fields by name. The transmute
// itself is between two references; the real soundness invariant is that
// `RawSqe` fits *within* the bytes io_uring allocated for an `Entry` and
// that the field offsets match. We can't statically check field offsets
// without exposing private `RawSqe` field offsets, but we can at least
// guard that `RawSqe` is no larger than `Entry` — if that ever flips, the
// macro is writing past the end of the SQE and we have a real soundness
// regression to investigate.
const_assert!(
    std::mem::size_of::<ublk_core::io::RawSqe>()
        <= std::mem::size_of::<io_uring::squeue::Entry>()
);
const_assert!(
    std::mem::align_of::<ublk_core::io::RawSqe>()
        <= std::mem::align_of::<io_uring::squeue::Entry>()
);

// `ublk_core/src/io.rs:1481` and `:1906` transmute `sys::ublksrv_io_cmd`
// into `[u8; 16]` so the kernel's I/O command struct fits the io_uring
// `UringCmd16` SQE's 16-byte inline-data slot. The kernel ABI guarantees
// `ublksrv_io_cmd` is exactly 16 bytes; if a header bump changes that,
// the transmute reads or writes off the end of the SQE.
assert_eq_size!(ublk_core::sys::ublksrv_io_cmd, [u8; 16]);
