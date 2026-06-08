//! Verifies the `decompress_block` fix (right-sized + fallible allocation).
//!
//! A probe global allocator watches allocations that happen *inside* a real
//! `decompress_block` call on a real zstd-compressed 128 KiB block.
//!
//!   observe : assert the largest allocation is now the block's real size
//!             (131072), NOT the old 2097152 — proves the 16× over-allocation
//!             and the 2 MiB abort surface are both gone.
//!   oom     : inject ENOMEM for that allocation in a forked child and assert
//!             decompress_block returns Err and the process does NOT abort —
//!             proves the SIGABRT is now a recoverable per-read EIO.
//!
//! Run:
//!   cargo run -p glidefs --features ublk --example repro_2mib_abort
//!   cargo run -p glidefs --features ublk --example repro_2mib_abort -- oom

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use glidefs::block::block_map::{compress_block, decompress_block, COMPRESSION_RUNTIME_DEFAULT};

const BLOCK_LEN: usize = 131072; // 128 KiB — the production block size
const OLD_ABORT_SIZE: usize = 2 * 1024 * 1024; // 2097152, the pre-fix allocation

static GUARD: AtomicBool = AtomicBool::new(false);
static MAX_ALLOC: AtomicUsize = AtomicUsize::new(0);
static FAIL_AT_OR_ABOVE: AtomicUsize = AtomicUsize::new(usize::MAX);

struct Probe;

unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let sz = layout.size();
        if GUARD.load(Ordering::Relaxed) {
            MAX_ALLOC.fetch_max(sz, Ordering::Relaxed);
            if sz >= FAIL_AT_OR_ABOVE.load(Ordering::Relaxed) {
                return std::ptr::null_mut(); // would -> SIGABRT if alloc were infallible
            }
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Probe = Probe;

fn compressed_block() -> Vec<u8> {
    let block = vec![0xABu8; BLOCK_LEN];
    let c = compress_block(&block, COMPRESSION_RUNTIME_DEFAULT);
    assert_eq!(&c[..4], &[0x28, 0xB5, 0x2F, 0xFD], "expected a zstd frame");
    c
}

fn main() {
    let oom = std::env::args().nth(1).as_deref() == Some("oom");
    let compressed = compressed_block();

    if oom {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // Fail any allocation >= 64 KiB during decompress — this nulls the
            // real (right-sized) output-buffer reservation. With the fix it is
            // a fallible try_reserve, so we expect Err, not abort.
            FAIL_AT_OR_ABOVE.store(64 * 1024, Ordering::Relaxed);
            GUARD.store(true, Ordering::Relaxed);
            let r = decompress_block(&compressed);
            GUARD.store(false, Ordering::Relaxed);
            match r {
                Err(e) => {
                    eprintln!("[child] decompress_block returned Err ({e}) under ENOMEM — no abort");
                    std::process::exit(0);
                }
                Ok(_) => {
                    eprintln!("[child] decompress unexpectedly succeeded");
                    std::process::exit(3);
                }
            }
        }
        let mut status: libc::c_int = 0;
        unsafe { libc::waitpid(pid, &mut status, 0) };
        if libc::WIFSIGNALED(status) {
            eprintln!(
                "[parent] FAIL: child killed by signal {} — abort NOT fixed",
                libc::WTERMSIG(status)
            );
            std::process::exit(1);
        }
        let code = libc::WEXITSTATUS(status);
        if code == 0 {
            eprintln!("[parent] PASS: OOM on the decompress buffer -> Err (EIO), daemon survives");
            std::process::exit(0);
        }
        eprintln!("[parent] FAIL: child exited {code}");
        std::process::exit(code);
    }

    // observe
    GUARD.store(true, Ordering::Relaxed);
    let out = decompress_block(&compressed).expect("decompress ok");
    GUARD.store(false, Ordering::Relaxed);
    assert_eq!(out.len(), BLOCK_LEN);
    let max = MAX_ALLOC.load(Ordering::Relaxed);
    eprintln!("largest allocation inside decompress_block: {max} bytes (was {OLD_ABORT_SIZE})");
    assert!(
        max < OLD_ABORT_SIZE,
        "decompress still makes a {OLD_ABORT_SIZE}-byte allocation — fix not effective",
    );
    assert_eq!(max, BLOCK_LEN, "expected the buffer to be right-sized to the block");
    eprintln!("PASS: decompress_block now allocates {BLOCK_LEN} (right-sized), not {OLD_ABORT_SIZE}");
}
