/* glidefs-vm-init — PID 1 for the Firecracker boot-set profiling microVM.
 *
 * The complete boundary for profiling an UNTRUSTED image: a guest kernel (not the
 * host) mounts the untrusted fs and runs its entrypoint, while the host ublk
 * read-tracer (backing the virtio-blk drive) records exactly which blocks the
 * boot faults. This init:
 *   1. mounts /proc, /sys, /dev (devtmpfs),
 *   2. reads a control blob off the second drive (/dev/vdb): u32-LE length then
 *      newline records (FS=, WORKDIR=, TIMEOUT=, ENV=, ARG=, SEED=),
 *   3. mounts the image drive (/dev/vda) read-only at /img,
 *   4. reads the static-seed paths (faults their blocks — the boot-set union),
 *   5. chroots into the image and runs the entrypoint under a wall-clock timeout,
 *   6. triple-fault resets so Firecracker exits cleanly.
 *
 * Static (musl) — see build.sh. Console (markers) goes to ttyS0 via fd 2, which
 * the host scans for VMINIT_* to decide the run outcome. Built into an initramfs,
 * so the image stays a pure data drive (never the VM root).
 */
#define _GNU_SOURCE
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <time.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/reboot.h>
#include <sys/wait.h>

static void say(const char *s) { write(2, s, strlen(s)); }

/* Triple-fault reset → Firecracker exits. Never returns. */
static void halt(void) {
    sync();
    reboot(RB_AUTOBOOT);
    for (;;) pause();
}

/* Read the control blob from /dev/vdb: 4-byte LE length, then that many bytes. */
static int read_control(char *buf, int max) {
    int fd = open("/dev/vdb", O_RDONLY);
    if (fd < 0) return -1;
    uint32_t len = 0;
    if (read(fd, &len, 4) != 4) { close(fd); return -1; }
    if (len > (uint32_t)(max - 1)) len = max - 1;
    int got = 0;
    while (got < (int)len) {
        int n = read(fd, buf + got, len - got);
        if (n <= 0) break;
        got += n;
    }
    buf[got] = 0;
    close(fd);
    return got;
}

int main(void) {
    mkdir("/proc", 0755); mkdir("/sys", 0755); mkdir("/dev", 0755); mkdir("/img", 0755);
    mount("proc", "/proc", "proc", 0, 0);
    mount("sysfs", "/sys", "sysfs", 0, 0);
    mount("dev", "/dev", "devtmpfs", 0, 0);

    static char ctl[16384];
    if (read_control(ctl, sizeof ctl) < 0) { say("VMINIT_CTL_FAIL\n"); halt(); }

    const char *fs = "erofs", *workdir = "/";
    int timeout = 30;
    char *argv[64]; int argc = 0;
    char *envp[64]; int envc = 0;
    char *seed[512]; int seedc = 0;
    char *p = ctl;
    while (*p) {
        char *line = p;
        char *nl = strchr(p, '\n');
        if (nl) { *nl = 0; p = nl + 1; } else { p += strlen(p); }
        if (!strncmp(line, "FS=", 3)) fs = line + 3;
        else if (!strncmp(line, "WORKDIR=", 8)) workdir = line + 8;
        else if (!strncmp(line, "TIMEOUT=", 8)) timeout = atoi(line + 8);
        else if (!strncmp(line, "ENV=", 4)) { if (envc < 63) envp[envc++] = line + 4; }
        else if (!strncmp(line, "ARG=", 4)) { if (argc < 63) argv[argc++] = line + 4; }
        else if (!strncmp(line, "SEED=", 5)) { if (seedc < 511) seed[seedc++] = line + 5; }
    }
    argv[argc] = 0;
    envp[envc] = 0;

    if (mount("/dev/vda", "/img", fs, MS_RDONLY, 0) != 0 &&
        mount("/dev/vda", "/img", "ext4", MS_RDONLY, 0) != 0 &&
        mount("/dev/vda", "/img", "erofs", MS_RDONLY, 0) != 0) {
        say("VMINIT_MOUNT_FAIL\n");
        halt();
    }
    say("VMINIT_MOUNT_OK\n");

    /* Boot-set UNION: read the static closure under the tracer. */
    for (int i = 0; i < seedc; i++) {
        char path[1024];
        snprintf(path, sizeof path, "/img%s", seed[i]);
        int fd = open(path, O_RDONLY);
        if (fd < 0) continue;
        static char b[65536];
        while (read(fd, b, sizeof b) > 0) {}
        close(fd);
    }

    if (argc == 0) { say("VMINIT_NOCMD\n"); halt(); }

    /* Give the chrooted entrypoint a /proc, /dev, /sys. */
    mount("proc", "/img/proc", "proc", 0, 0);
    mount("dev", "/img/dev", "devtmpfs", 0, 0);
    mount("sysfs", "/img/sys", "sysfs", 0, 0);

    pid_t pid = fork();
    if (pid == 0) {
        if (chroot("/img") != 0) _exit(127);
        if (chdir(workdir) != 0) { if (chdir("/") != 0) _exit(127); }
        execvpe(argv[0], argv, envp);
        _exit(127);
    }
    say("VMINIT_RUN\n");

    /* Wait for the entrypoint up to TIMEOUT (servers run forever → killed; their
     * startup reads ARE the boot set). */
    time_t start = time(0);
    int status = 0;
    int timed_out = 0;
    for (;;) {
        pid_t w = waitpid(pid, &status, WNOHANG);
        if (w == pid) break;
        if (time(0) - start >= timeout) {
            say("VMINIT_TIMEOUT\n");
            timed_out = 1;
            kill(pid, 9);
            waitpid(pid, &status, 0);
            break;
        }
        struct timespec ts = { 0, 50 * 1000 * 1000 };
        nanosleep(&ts, 0);
    }
    /* Surface the entrypoint's disposition so the host can tell a clean run from a
     * failed exec (127) or a crash — not just "the VM booted". */
    if (!timed_out) {
        char line[32];
        int code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        int n = snprintf(line, sizeof line, "VMINIT_EXIT=%d\n", code);
        write(2, line, n);
    }
    say("VMINIT_DONE\n");
    halt();
    return 0;
}
