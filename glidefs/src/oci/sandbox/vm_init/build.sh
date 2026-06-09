#!/usr/bin/env bash
# Build the Firecracker boot-set-profiling initramfs: a static musl init.c packed
# as /init in a newc cpio. Output: glidefs-vm-initramfs.cpio next to this script.
# Point `[profile] initramfs = "<path>"` at it (and `kernel_image` at a Firecracker
# guest vmlinux). Requires musl-gcc + cpio.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out="${1:-$here/glidefs-vm-initramfs.cpio}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

musl-gcc -static -Os -o "$work/init" "$here/init.c"
strip "$work/init"
( cd "$work" && find . -print0 | cpio --null -o -H newc 2>/dev/null ) > "$out"
echo "built $out ($(stat -c%s "$out") bytes; init $(stat -c%s "$work/init") bytes)"
