#!/bin/bash
# Prove every kernel crate with the pinned Verus (dev/verus/VERSION.env): `cargo verus verify` on each.
# Installs Verus and its Rust toolchain on first use under $VERUS_HOME (default ~/.cache/verus), checked by sha256.
# Extra args go to `cargo verus verify` (e.g. `--features mut_always_hash_base` to see the proof go red).
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# shellcheck source=/dev/null
. "$root/scripts/verus-version.env"
home=${VERUS_HOME:-$HOME/.cache/verus}
dir=$home/verus-$VERUS_VERSION
if [[ ! -x $dir/verus-x86-linux/cargo-verus ]]; then
  mkdir -p "$home"
  tmp=$(mktemp -d "$home/dl.XXXXXX")
  trap 'rm -rf "$tmp"' EXIT
  curl -fsSL -o "$tmp/verus.zip" \
    "https://github.com/verus-lang/verus/releases/download/release/$VERUS_VERSION/verus-$VERUS_VERSION-x86-linux.zip"
  echo "$VERUS_SHA256  $tmp/verus.zip" | sha256sum -c - >&2
  unzip -q "$tmp/verus.zip" -d "$tmp/x"
  rm -rf "$dir" && mv "$tmp/x" "$dir"
fi
rustup toolchain list | grep -q "^$VERUS_RUST_TOOLCHAIN-" || rustup toolchain install "$VERUS_RUST_TOOLCHAIN" --profile minimal
export PATH=$dir/verus-x86-linux:$PATH
# Its own target dir: the Verus build must not share artifacts with the normal one.
export CARGO_TARGET_DIR=${VERUS_TARGET_DIR:-${CARGO_TARGET_DIR:-$root/target}-verus}
for crate in "$root"/kernels; do
  echo "verify: ${crate#"$root"/}" >&2
  (cd "$crate" && cargo verus verify "$@")
done
