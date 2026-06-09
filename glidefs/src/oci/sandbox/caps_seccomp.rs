//! Capability drop + seccomp allowlist for the sandboxed entrypoint.
//!
//! Applied in the fork-child just before `execve`, after the mount/pivot setup is
//! done (so it never blocks our own privileged syscalls). The workload then runs
//! with **no capabilities** (even as uid 0 in the user ns), `no_new_privs` set (so
//! a setuid bit in the image can't elevate), and a seccomp filter that defaults to
//! `EPERM` for anything outside the allowlist. `EPERM` (not `KILL`) is deliberate:
//! a profiler wants a workload that hits an odd syscall to *limp forward* and keep
//! faulting boot blocks, not die on the first surprise.
//!
//! libc FFI only (prctl/capset); the BPF program is built by `seccompiler`
//! (Firecracker's own, externally-audited crate) so we don't hand-roll cBPF with
//! the x32 ABI guard.

#![allow(unsafe_code)]

use anyhow::{Context, Result};

/// `prctl(PR_SET_NO_NEW_PRIVS, 1)` — must precede seccomp and prevents any setuid
/// binary in the image from gaining privileges.
pub fn set_no_new_privs() -> Result<()> {
    let rc = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("prctl(PR_SET_NO_NEW_PRIVS)");
    }
    Ok(())
}

/// Drop every capability: clear the bounding set, the ambient set, and the
/// effective/permitted/inheritable sets. After this the process is uid 0 in the
/// user ns with an empty capability set — it can read root-owned image files but
/// cannot mount, load modules, etc.
pub fn drop_all_caps() -> Result<()> {
    // Bounding set: drop caps 0..=last. EINVAL past the last valid cap is fine.
    let last = cap_last_cap();
    for cap in 0..=last {
        // PR_CAPBSET_DROP fails with EPERM only if we lack CAP_SETPCAP, which we
        // hold in the fresh user ns; ignore per-cap errors defensively.
        unsafe { libc::prctl(libc::PR_CAPBSET_DROP, cap as libc::c_ulong, 0, 0, 0) };
    }
    // Ambient set: clear all (best-effort; older kernels lack PR_CAP_AMBIENT).
    unsafe {
        libc::prctl(
            libc::PR_CAP_AMBIENT,
            libc::PR_CAP_AMBIENT_CLEAR_ALL as libc::c_ulong,
            0,
            0,
            0,
        )
    };

    // Effective/permitted/inheritable: capset with empty data (version 3).
    #[repr(C)]
    struct CapHeader {
        version: u32,
        pid: libc::c_int,
    }
    #[repr(C)]
    #[derive(Default, Clone, Copy)]
    struct CapData {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;
    let hdr = CapHeader {
        version: LINUX_CAPABILITY_VERSION_3,
        pid: 0,
    };
    let data = [CapData::default(); 2]; // two 32-bit data blocks for v3
    let rc = unsafe { libc::syscall(libc::SYS_capset, std::ptr::addr_of!(hdr), data.as_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("capset(empty)");
    }
    Ok(())
}

/// Highest valid capability number on this kernel (`/proc/sys/kernel/cap_last_cap`),
/// falling back to a conservative 40 if unreadable.
fn cap_last_cap() -> u32 {
    std::fs::read_to_string("/proc/sys/kernel/cap_last_cap")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(40)
}

/// Build and install the seccomp allowlist on the current thread. No-op on
/// architectures seccompiler doesn't target here (the run still has ns + cap-drop).
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn apply_seccomp() -> Result<()> {
    use std::collections::BTreeMap;

    use seccompiler::{SeccompAction, SeccompFilter, TargetArch};

    #[cfg(target_arch = "x86_64")]
    let arch = TargetArch::x86_64;
    #[cfg(target_arch = "aarch64")]
    let arch = TargetArch::aarch64;

    // Allowlist: each entry matches the syscall unconditionally (empty rule vec).
    // Anything not listed → mismatch_action = EPERM. We omit the container-escape
    // / kernel-attack surface (ptrace, bpf, keyctl, the mount/module/kexec/reboot
    // families, perf_event_open, io_uring, userfaultfd, setns/unshare, …) so those
    // return EPERM.
    let mut rules: BTreeMap<i64, Vec<seccompiler::SeccompRule>> = BTreeMap::new();
    for nr in ALLOWED.iter().copied() {
        rules.insert(nr, vec![]);
    }

    let filter = SeccompFilter::new(
        rules,
        SeccompAction::Errno(libc::EPERM as u32), // not in the allowlist
        SeccompAction::Allow,                     // in the allowlist
        arch,
    )
    .context("build seccomp filter")?;
    let prog: seccompiler::BpfProgram = filter.try_into().context("compile seccomp BPF")?;
    seccompiler::apply_filter(&prog).context("apply seccomp filter")?;
    Ok(())
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn apply_seccomp() -> Result<()> {
    Ok(())
}

// The libc crate exposes SYS_sendfile / SYS_fadvise64 on x86_64 but NOT on
// aarch64, even though the syscalls exist there (asm-generic numbers 71 / 223).
// Alias them per-arch so both ALLOWED lists can reference them uniformly without
// a cross-compile break. (x86_64's numbers differ — taken from the libc consts.)
#[cfg(target_arch = "x86_64")]
const NR_SENDFILE: i64 = libc::SYS_sendfile;
#[cfg(target_arch = "x86_64")]
const NR_FADVISE64: i64 = libc::SYS_fadvise64;
#[cfg(target_arch = "aarch64")]
const NR_SENDFILE: i64 = 71;
#[cfg(target_arch = "aarch64")]
const NR_FADVISE64: i64 = 223;

/// The allowlisted syscalls — the normal file-I/O / memory / thread / signal /
/// time / id / local-socket surface a libc runtime boot needs. Kept generous on
/// purpose: an over-narrow list just limps a profile (EPERM, no death); the
/// security value is in what is *absent* (the admin/exploit surface).
#[cfg(target_arch = "x86_64")]
const ALLOWED: &[i64] = &[
    // file I/O
    libc::SYS_read,
    libc::SYS_write,
    libc::SYS_open,
    libc::SYS_openat,
    libc::SYS_close,
    libc::SYS_stat,
    libc::SYS_fstat,
    libc::SYS_lstat,
    libc::SYS_newfstatat,
    libc::SYS_statx,
    libc::SYS_lseek,
    libc::SYS_pread64,
    libc::SYS_pwrite64,
    libc::SYS_readv,
    libc::SYS_writev,
    libc::SYS_preadv,
    libc::SYS_pwritev,
    libc::SYS_access,
    libc::SYS_faccessat,
    libc::SYS_getdents,
    libc::SYS_getdents64,
    libc::SYS_getcwd,
    libc::SYS_chdir,
    libc::SYS_fchdir,
    libc::SYS_readlink,
    libc::SYS_readlinkat,
    libc::SYS_dup,
    libc::SYS_dup2,
    libc::SYS_dup3,
    libc::SYS_fcntl,
    libc::SYS_flock,
    libc::SYS_fsync,
    libc::SYS_fdatasync,
    libc::SYS_ftruncate,
    libc::SYS_truncate,
    libc::SYS_pipe,
    libc::SYS_pipe2,
    libc::SYS_poll,
    libc::SYS_ppoll,
    libc::SYS_select,
    libc::SYS_pselect6,
    libc::SYS_epoll_create,
    libc::SYS_epoll_create1,
    libc::SYS_epoll_ctl,
    libc::SYS_epoll_wait,
    libc::SYS_epoll_pwait,
    libc::SYS_eventfd,
    libc::SYS_eventfd2,
    libc::SYS_inotify_init1,
    libc::SYS_inotify_add_watch,
    libc::SYS_inotify_rm_watch,
    libc::SYS_statfs,
    libc::SYS_fstatfs,
    libc::SYS_getrandom,
    libc::SYS_umask,
    libc::SYS_mkdir,
    libc::SYS_mkdirat,
    libc::SYS_rmdir,
    libc::SYS_unlink,
    libc::SYS_unlinkat,
    libc::SYS_rename,
    libc::SYS_renameat,
    libc::SYS_renameat2,
    libc::SYS_symlink,
    libc::SYS_symlinkat,
    libc::SYS_link,
    libc::SYS_linkat,
    libc::SYS_chmod,
    libc::SYS_fchmod,
    libc::SYS_fchmodat,
    libc::SYS_chown,
    libc::SYS_fchown,
    libc::SYS_lchown,
    libc::SYS_fchownat,
    libc::SYS_utimensat,
    libc::SYS_copy_file_range,
    NR_SENDFILE,
    libc::SYS_splice,
    NR_FADVISE64,
    libc::SYS_readahead,
    libc::SYS_fallocate,
    libc::SYS_memfd_create,
    // memory
    libc::SYS_mmap,
    libc::SYS_munmap,
    libc::SYS_mremap,
    libc::SYS_mprotect,
    libc::SYS_madvise,
    libc::SYS_brk,
    libc::SYS_mlock,
    libc::SYS_munlock,
    libc::SYS_mlockall,
    libc::SYS_munlockall,
    libc::SYS_msync,
    libc::SYS_mincore,
    // process / thread
    libc::SYS_clone,
    libc::SYS_clone3,
    libc::SYS_fork,
    libc::SYS_vfork,
    libc::SYS_execve,
    libc::SYS_execveat,
    libc::SYS_exit,
    libc::SYS_exit_group,
    libc::SYS_wait4,
    libc::SYS_waitid,
    libc::SYS_getpid,
    libc::SYS_getppid,
    libc::SYS_gettid,
    libc::SYS_getuid,
    libc::SYS_geteuid,
    libc::SYS_getgid,
    libc::SYS_getegid,
    libc::SYS_getgroups,
    libc::SYS_getresuid,
    libc::SYS_getresgid,
    libc::SYS_getpgrp,
    libc::SYS_getpgid,
    libc::SYS_getsid,
    libc::SYS_setsid,
    libc::SYS_setpgid,
    libc::SYS_set_tid_address,
    libc::SYS_set_robust_list,
    libc::SYS_get_robust_list,
    libc::SYS_prctl,
    libc::SYS_arch_prctl,
    libc::SYS_futex,
    libc::SYS_sched_yield,
    libc::SYS_sched_getaffinity,
    libc::SYS_sched_setaffinity,
    libc::SYS_sched_getparam,
    libc::SYS_sched_getscheduler,
    libc::SYS_sched_get_priority_max,
    libc::SYS_sched_get_priority_min,
    libc::SYS_getrusage,
    libc::SYS_times,
    libc::SYS_nanosleep,
    libc::SYS_clock_nanosleep,
    libc::SYS_rseq,
    libc::SYS_membarrier,
    // signals
    libc::SYS_rt_sigaction,
    libc::SYS_rt_sigprocmask,
    libc::SYS_rt_sigreturn,
    libc::SYS_rt_sigpending,
    libc::SYS_rt_sigtimedwait,
    libc::SYS_rt_sigqueueinfo,
    libc::SYS_rt_sigsuspend,
    libc::SYS_sigaltstack,
    libc::SYS_kill,
    libc::SYS_tkill,
    libc::SYS_tgkill,
    libc::SYS_restart_syscall,
    // time
    libc::SYS_clock_gettime,
    libc::SYS_clock_getres,
    libc::SYS_gettimeofday,
    libc::SYS_time,
    libc::SYS_timer_create,
    libc::SYS_timer_settime,
    libc::SYS_timer_gettime,
    libc::SYS_timer_delete,
    libc::SYS_timerfd_create,
    libc::SYS_timerfd_settime,
    libc::SYS_timerfd_gettime,
    // ids / limits / info
    libc::SYS_getrlimit,
    libc::SYS_setrlimit,
    libc::SYS_prlimit64,
    libc::SYS_getpriority,
    libc::SYS_setpriority,
    libc::SYS_capget,
    libc::SYS_uname,
    libc::SYS_sysinfo,
    libc::SYS_getcpu,
    libc::SYS_ioctl,
    // local sockets (net ns is empty; no external reach, but runtimes call these)
    libc::SYS_socket,
    libc::SYS_socketpair,
    libc::SYS_connect,
    libc::SYS_accept,
    libc::SYS_accept4,
    libc::SYS_bind,
    libc::SYS_listen,
    libc::SYS_getsockname,
    libc::SYS_getpeername,
    libc::SYS_sendto,
    libc::SYS_recvfrom,
    libc::SYS_sendmsg,
    libc::SYS_recvmsg,
    libc::SYS_sendmmsg,
    libc::SYS_recvmmsg,
    libc::SYS_shutdown,
    libc::SYS_getsockopt,
    libc::SYS_setsockopt,
];

#[cfg(target_arch = "aarch64")]
const ALLOWED: &[i64] = &[
    // aarch64 has no legacy open/stat/poll/dup2/fork/select variants; the rest
    // mirror x86_64. Kept as a parallel list to avoid per-arch cfg noise inline.
    libc::SYS_read,
    libc::SYS_write,
    libc::SYS_openat,
    libc::SYS_close,
    libc::SYS_fstat,
    libc::SYS_newfstatat,
    libc::SYS_statx,
    libc::SYS_lseek,
    libc::SYS_pread64,
    libc::SYS_pwrite64,
    libc::SYS_readv,
    libc::SYS_writev,
    libc::SYS_preadv,
    libc::SYS_pwritev,
    libc::SYS_faccessat,
    libc::SYS_getdents64,
    libc::SYS_getcwd,
    libc::SYS_chdir,
    libc::SYS_fchdir,
    libc::SYS_readlinkat,
    libc::SYS_dup,
    libc::SYS_dup3,
    libc::SYS_fcntl,
    libc::SYS_flock,
    libc::SYS_fsync,
    libc::SYS_fdatasync,
    libc::SYS_ftruncate,
    libc::SYS_truncate,
    libc::SYS_pipe2,
    libc::SYS_ppoll,
    libc::SYS_pselect6,
    libc::SYS_epoll_create1,
    libc::SYS_epoll_ctl,
    libc::SYS_epoll_pwait,
    libc::SYS_eventfd2,
    libc::SYS_inotify_init1,
    libc::SYS_inotify_add_watch,
    libc::SYS_inotify_rm_watch,
    libc::SYS_statfs,
    libc::SYS_fstatfs,
    libc::SYS_getrandom,
    libc::SYS_umask,
    libc::SYS_mkdirat,
    libc::SYS_unlinkat,
    libc::SYS_renameat2,
    libc::SYS_symlinkat,
    libc::SYS_linkat,
    libc::SYS_fchmod,
    libc::SYS_fchmodat,
    libc::SYS_fchown,
    libc::SYS_fchownat,
    libc::SYS_utimensat,
    libc::SYS_copy_file_range,
    NR_SENDFILE,
    libc::SYS_splice,
    NR_FADVISE64,
    libc::SYS_readahead,
    libc::SYS_fallocate,
    libc::SYS_memfd_create,
    libc::SYS_mmap,
    libc::SYS_munmap,
    libc::SYS_mremap,
    libc::SYS_mprotect,
    libc::SYS_madvise,
    libc::SYS_brk,
    libc::SYS_mlock,
    libc::SYS_munlock,
    libc::SYS_mlockall,
    libc::SYS_munlockall,
    libc::SYS_msync,
    libc::SYS_mincore,
    libc::SYS_clone,
    libc::SYS_clone3,
    libc::SYS_execve,
    libc::SYS_execveat,
    libc::SYS_exit,
    libc::SYS_exit_group,
    libc::SYS_wait4,
    libc::SYS_waitid,
    libc::SYS_getpid,
    libc::SYS_getppid,
    libc::SYS_gettid,
    libc::SYS_getuid,
    libc::SYS_geteuid,
    libc::SYS_getgid,
    libc::SYS_getegid,
    libc::SYS_getgroups,
    libc::SYS_getresuid,
    libc::SYS_getresgid,
    libc::SYS_getpgid,
    libc::SYS_getsid,
    libc::SYS_setsid,
    libc::SYS_setpgid,
    libc::SYS_set_tid_address,
    libc::SYS_set_robust_list,
    libc::SYS_get_robust_list,
    libc::SYS_prctl,
    libc::SYS_futex,
    libc::SYS_sched_yield,
    libc::SYS_sched_getaffinity,
    libc::SYS_sched_setaffinity,
    libc::SYS_sched_getparam,
    libc::SYS_sched_getscheduler,
    libc::SYS_sched_get_priority_max,
    libc::SYS_sched_get_priority_min,
    libc::SYS_getrusage,
    libc::SYS_times,
    libc::SYS_nanosleep,
    libc::SYS_clock_nanosleep,
    libc::SYS_rseq,
    libc::SYS_membarrier,
    libc::SYS_rt_sigaction,
    libc::SYS_rt_sigprocmask,
    libc::SYS_rt_sigreturn,
    libc::SYS_rt_sigpending,
    libc::SYS_rt_sigtimedwait,
    libc::SYS_rt_sigqueueinfo,
    libc::SYS_rt_sigsuspend,
    libc::SYS_sigaltstack,
    libc::SYS_kill,
    libc::SYS_tkill,
    libc::SYS_tgkill,
    libc::SYS_restart_syscall,
    libc::SYS_clock_gettime,
    libc::SYS_clock_getres,
    libc::SYS_gettimeofday,
    libc::SYS_timer_create,
    libc::SYS_timer_settime,
    libc::SYS_timer_gettime,
    libc::SYS_timer_delete,
    libc::SYS_timerfd_create,
    libc::SYS_timerfd_settime,
    libc::SYS_timerfd_gettime,
    libc::SYS_getrlimit,
    libc::SYS_setrlimit,
    libc::SYS_prlimit64,
    libc::SYS_getpriority,
    libc::SYS_setpriority,
    libc::SYS_capget,
    libc::SYS_uname,
    libc::SYS_sysinfo,
    libc::SYS_getcpu,
    libc::SYS_ioctl,
    libc::SYS_socket,
    libc::SYS_socketpair,
    libc::SYS_connect,
    libc::SYS_accept,
    libc::SYS_accept4,
    libc::SYS_bind,
    libc::SYS_listen,
    libc::SYS_getsockname,
    libc::SYS_getpeername,
    libc::SYS_sendto,
    libc::SYS_recvfrom,
    libc::SYS_sendmsg,
    libc::SYS_recvmsg,
    libc::SYS_sendmmsg,
    libc::SYS_recvmmsg,
    libc::SYS_shutdown,
    libc::SYS_getsockopt,
    libc::SYS_setsockopt,
];
