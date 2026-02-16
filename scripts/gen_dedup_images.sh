#!/usr/bin/env bash
#
# Generate real ext4 filesystem images for dedup block-size measurement.
#
# Creates ext4 images from real Ubuntu + Node.js containers:
#   base.raw         - Ubuntu 22.04 minimal
#   vm-0-express.raw - base + Node 20 + express
#   vm-1-express.raw - identical to vm-0 (same-workload dedup test)
#   vm-2-nextjs.raw  - base + Node 20 + next/react
#   vm-3-app.raw     - base + Node 20 + express + unique app code
#
# All ext4 creation happens inside Docker (works on macOS and Linux).
#
# Usage:
#   ./scripts/gen_dedup_images.sh [output_dir]
#   cargo run --release --bin dedup_measure -- /tmp/glidefs-dedup-test/*.raw
#
# Requirements: docker

set -euo pipefail

OUT="${1:-/tmp/glidefs-dedup-test}"
IMG_SIZE_MB=512

mkdir -p "$OUT"

echo "=== Generating dedup test images in $OUT ==="
echo "    Image size: ${IMG_SIZE_MB}MB each"
echo ""

# ---------------------------------------------------------------------------
# Build all app images first (docker layer caching makes this fast).
# ---------------------------------------------------------------------------

build_app_image() {
    local name="$1"
    local dockerfile="$2"
    echo "[$name] Building..."
    echo "$dockerfile" | docker build -q -t "glidefs-dedup-${name}" -f - . >/dev/null 2>&1
}

build_app_image "base" "
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null 2>&1
"

build_app_image "vm-0-express" "
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null 2>&1
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >/dev/null 2>&1 && \
    apt-get install -y -qq nodejs >/dev/null 2>&1
WORKDIR /app
RUN npm init -y >/dev/null 2>&1 && npm install express >/dev/null 2>&1
"

build_app_image "vm-1-express" "
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null 2>&1
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >/dev/null 2>&1 && \
    apt-get install -y -qq nodejs >/dev/null 2>&1
WORKDIR /app
RUN npm init -y >/dev/null 2>&1 && npm install express >/dev/null 2>&1
"

build_app_image "vm-2-nextjs" "
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null 2>&1
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >/dev/null 2>&1 && \
    apt-get install -y -qq nodejs >/dev/null 2>&1
WORKDIR /app
RUN npm init -y >/dev/null 2>&1 && npm install next react react-dom >/dev/null 2>&1
"

build_app_image "vm-3-app" "
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null 2>&1
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >/dev/null 2>&1 && \
    apt-get install -y -qq nodejs >/dev/null 2>&1
WORKDIR /app
RUN npm init -y >/dev/null 2>&1 && npm install express >/dev/null 2>&1
RUN mkdir -p src && for i in \$(seq 1 50); do \
      dd if=/dev/urandom bs=4096 count=4 2>/dev/null | base64 > src/module_\${i}.js; \
    done
RUN echo 'const express = require(\"express\"); const app = express(); app.listen(3000);' > index.js
"

echo ""

# ---------------------------------------------------------------------------
# Export each container and pack into an ext4 image.
# All done inside a privileged Ubuntu container for portability.
# ---------------------------------------------------------------------------

NAMES="base vm-0-express vm-1-express vm-2-nextjs vm-3-app"

# Export all containers as tarballs.
for name in $NAMES; do
    echo "[$name] Exporting container filesystem..."
    cid=$(docker create "glidefs-dedup-${name}")
    docker export "$cid" > "$OUT/${name}.tar"
    docker rm "$cid" >/dev/null
done

echo ""
echo "Packing into ext4 images (inside Docker)..."

# Build a helper image with e2fsprogs.
docker build -q -t glidefs-dedup-helper -f - . >/dev/null 2>&1 <<'HELPER_DOCKERFILE'
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq e2fsprogs tar >/dev/null 2>&1
HELPER_DOCKERFILE

# Run the helper to create all ext4 images from the tarballs.
docker run --rm --privileged \
    -v "$OUT:/work" \
    glidefs-dedup-helper bash -c "
        set -e
        for name in $NAMES; do
            echo \"[\$name] Creating ${IMG_SIZE_MB}MB ext4 image...\"
            dd if=/dev/zero of=/work/\${name}.raw bs=1M count=$IMG_SIZE_MB status=none
            mkfs.ext4 -q -F /work/\${name}.raw
            mkdir -p /mnt/img
            mount -o loop /work/\${name}.raw /mnt/img
            tar xf /work/\${name}.tar -C /mnt/img 2>/dev/null || true
            umount /mnt/img
            rm -f /work/\${name}.tar
            echo \"[\$name] Done\"
        done
    "

echo ""
echo "=== Generated images ==="
for name in $NAMES; do
    size=$(ls -lh "$OUT/${name}.raw" | awk '{print $5}')
    echo "  $OUT/${name}.raw  ($size)"
done

echo ""
echo "Run measurement:"
echo "  cargo run --release --bin dedup_measure -- $OUT/*.raw"
