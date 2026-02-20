#!/usr/bin/env bash
#
# Generate real filesystem images for dedup block-size measurement.
#
# Three test sets:
#
# SET 1: Independent builds (worst case — simulates cross-host without bless)
#   independent/base.raw         - Ubuntu 22.04 minimal
#   independent/vm-0-express.raw - base + Node 20 + express
#   independent/vm-1-express.raw - identical build (same Dockerfile)
#   independent/vm-2-nextjs.raw  - base + Node 20 + next/react
#   independent/vm-3-app.raw     - base + Node 20 + express + unique app code
#
# SET 2: Forked images on ext4 (realistic — simulates fork-then-write like GLIDEv2)
#   forked/base.raw              - blessed base: Ubuntu 22.04 + Node 20
#   forked/vm-0-express.raw      - copy of base, then npm install express
#   forked/vm-1-express.raw      - copy of base, then npm install express
#   forked/vm-2-nextjs.raw       - copy of base, then npm install next react
#   forked/vm-3-app.raw          - copy of base, then npm install express + app code
#
# SET 3: Forked images on XFS (same as SET 2 but with XFS instead of ext4)
#   forked-xfs/base.raw          - blessed base on XFS
#   forked-xfs/vm-0-express.raw  - copy of base, then npm install express
#   forked-xfs/vm-1-express.raw  - copy of base, then npm install express
#   forked-xfs/vm-2-nextjs.raw   - copy of base, then npm install next react
#   forked-xfs/vm-3-app.raw      - copy of base, then npm install express + app code
#
# Usage:
#   ./scripts/gen_dedup_images.sh [output_dir]
#   cargo run --release --bin dedup_measure -- /tmp/glidefs-dedup-test/independent/*.raw
#   cargo run --release --bin dedup_measure -- /tmp/glidefs-dedup-test/forked/*.raw
#   cargo run --release --bin dedup_measure -- /tmp/glidefs-dedup-test/forked-xfs/*.raw
#
# Requirements: docker

set -euo pipefail

OUT="${1:-/tmp/glidefs-dedup-test}"
IMG_SIZE_MB=512

mkdir -p "$OUT/independent" "$OUT/forked" "$OUT/forked-xfs" "$OUT/forked-btrfs"

echo "=== Generating dedup test images in $OUT ==="
echo "    Image size: ${IMG_SIZE_MB}MB each"
echo ""

# ---------------------------------------------------------------------------
# SET 1: Independent builds
# ---------------------------------------------------------------------------

build_app_image() {
    local name="$1"
    local dockerfile="$2"
    echo "[independent/$name] Building..."
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

INDEP_NAMES="base vm-0-express vm-1-express vm-2-nextjs vm-3-app"

for name in $INDEP_NAMES; do
    echo "[independent/$name] Exporting..."
    cid=$(docker create "glidefs-dedup-${name}")
    docker export "$cid" > "$OUT/independent/${name}.tar"
    docker rm "$cid" >/dev/null
done

# ---------------------------------------------------------------------------
# SET 2 & 3: Forked images — build base, then write into copies
# ---------------------------------------------------------------------------

echo ""
echo "[forked/base] Building blessed base (Ubuntu 22.04 + Node 20)..."
docker build -q -t glidefs-dedup-forked-base -f - . >/dev/null 2>&1 <<'EOF'
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null 2>&1
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >/dev/null 2>&1 && \
    apt-get install -y -qq nodejs >/dev/null 2>&1
EOF

echo "[forked/base] Exporting..."
cid=$(docker create glidefs-dedup-forked-base)
docker export "$cid" > "$OUT/forked/base.tar"
docker rm "$cid" >/dev/null

# Copy tarball for XFS and btrfs sets before ext4 packing deletes it
cp "$OUT/forked/base.tar" "$OUT/forked-xfs/base.tar"
cp "$OUT/forked/base.tar" "$OUT/forked-btrfs/base.tar"

# ---------------------------------------------------------------------------
# Build the helper image (supports ext4, XFS, and btrfs)
# ---------------------------------------------------------------------------

echo ""
echo "Building filesystem helper..."
docker build -q -t glidefs-dedup-helper -f - . >/dev/null 2>&1 <<'HELPER'
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq e2fsprogs xfsprogs btrfs-progs tar nodejs npm git python3-pip >/dev/null 2>&1
HELPER

# ---------------------------------------------------------------------------
# Pack independent set into ext4
# ---------------------------------------------------------------------------

echo ""
echo "=== Packing independent set into ext4 ==="

docker run --rm --privileged \
    -v "$OUT:/work" \
    glidefs-dedup-helper bash -c "
        set -e
        for name in $INDEP_NAMES; do
            echo \"[\$name] Creating ${IMG_SIZE_MB}MB ext4...\"
            dd if=/dev/zero of=/work/independent/\${name}.raw bs=1M count=$IMG_SIZE_MB status=none
            mkfs.ext4 -q -F /work/independent/\${name}.raw
            mkdir -p /mnt/img
            mount -o loop /work/independent/\${name}.raw /mnt/img
            tar xf /work/independent/\${name}.tar -C /mnt/img 2>/dev/null || true
            umount /mnt/img
            rm -f /work/independent/\${name}.tar
            echo \"[\$name] Done\"
        done
    "

# ---------------------------------------------------------------------------
# Pack forked set into ext4 — base image is created once, then COPIED,
# then each copy is mounted and written into. This simulates the fork-then-write
# pattern where all VMs start from the same ext4 state.
# ---------------------------------------------------------------------------

echo ""
echo "=== Packing forked set into ext4 (fork-then-write) ==="

docker run --rm --privileged \
    -v "$OUT:/work" \
    glidefs-dedup-helper bash -c '
        set -e

        # Create the blessed base ext4 image.
        echo "[forked/base] Creating base ext4..."
        dd if=/dev/zero of=/work/forked/base.raw bs=1M count='"$IMG_SIZE_MB"' status=none
        mkfs.ext4 -q -F /work/forked/base.raw
        mkdir -p /mnt/img
        mount -o loop /work/forked/base.raw /mnt/img
        tar xf /work/forked/base.tar -C /mnt/img 2>/dev/null || true
        umount /mnt/img
        rm -f /work/forked/base.tar
        echo "[forked/base] Done"

        # Fork: copy the base image, then mount and write into each copy.

        echo "[forked/vm-0-express] Forking + npm install express..."
        cp /work/forked/base.raw /work/forked/vm-0-express.raw
        mount -o loop /work/forked/vm-0-express.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked/vm-0-express] Done"

        echo "[forked/vm-1-express] Forking + npm install express..."
        cp /work/forked/base.raw /work/forked/vm-1-express.raw
        mount -o loop /work/forked/vm-1-express.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked/vm-1-express] Done"

        echo "[forked/vm-2-nextjs] Forking + npm install next react..."
        cp /work/forked/base.raw /work/forked/vm-2-nextjs.raw
        mount -o loop /work/forked/vm-2-nextjs.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install next react react-dom >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked/vm-2-nextjs] Done"

        echo "[forked/vm-3-app] Forking + express + app code..."
        cp /work/forked/base.raw /work/forked/vm-3-app.raw
        mount -o loop /work/forked/vm-3-app.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        mkdir -p src
        for i in $(seq 1 50); do
            dd if=/dev/urandom bs=4096 count=4 2>/dev/null | base64 > src/module_${i}.js
        done
        echo "const express = require(\"express\"); const app = express(); app.listen(3000);" > index.js
        cd / && umount /mnt/img
        echo "[forked/vm-3-app] Done"

        echo "[forked/vm-4-git-clone] Forking + git clone..."
        cp /work/forked/base.raw /work/forked/vm-4-git-clone.raw
        mount -o loop /work/forked/vm-4-git-clone.raw /mnt/img
        cd /mnt/img/root
        git clone --depth 1 https://github.com/expressjs/express.git >/dev/null 2>&1 || true
        cd / && umount /mnt/img
        echo "[forked/vm-4-git-clone] Done"

        echo "[forked/vm-5-pip-django] Forking + pip install django..."
        cp /work/forked/base.raw /work/forked/vm-5-pip-django.raw
        mount -o loop /work/forked/vm-5-pip-django.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        pip3 install django >/dev/null 2>&1 || true
        cd / && umount /mnt/img
        echo "[forked/vm-5-pip-django] Done"

        echo "[forked/vm-6-many-files] Forking + creating 10000 small files..."
        cp /work/forked/base.raw /work/forked/vm-6-many-files.raw
        mount -o loop /work/forked/vm-6-many-files.raw /mnt/img
        cd /mnt/img/root && mkdir -p project
        for i in $(seq 1 10000); do
            dir="project/d$(( i % 100 ))"
            mkdir -p "$dir"
            echo "file ${i} content $(date +%s%N)" > "${dir}/f${i}.txt"
        done
        cd / && umount /mnt/img
        echo "[forked/vm-6-many-files] Done"

        echo "[forked/vm-7-mixed-workload] Forking + npm + git + files..."
        cp /work/forked/base.raw /work/forked/vm-7-mixed-workload.raw
        mount -o loop /work/forked/vm-7-mixed-workload.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express body-parser cors >/dev/null 2>&1
        git init >/dev/null 2>&1
        git config user.email "test@test.com"
        git config user.name "test"
        mkdir -p src tests config
        for i in $(seq 1 100); do
            dd if=/dev/urandom bs=2048 count=1 2>/dev/null | base64 > src/module_${i}.ts
        done
        for i in $(seq 1 50); do
            dd if=/dev/urandom bs=1024 count=1 2>/dev/null | base64 > tests/test_${i}.ts
        done
        echo "{\"port\":3000}" > config/default.json
        git add -A >/dev/null 2>&1 && git commit -m "init" >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked/vm-7-mixed-workload] Done"
    '

# ---------------------------------------------------------------------------
# SET 3: Pack forked set into XFS — same fork-then-write pattern as SET 2
# but with XFS to measure metadata/data separation impact on dedup.
# ---------------------------------------------------------------------------

echo ""
echo "=== Packing forked set into XFS (fork-then-write) ==="

docker run --rm --privileged \
    -v "$OUT:/work" \
    glidefs-dedup-helper bash -c '
        set -e

        # Create the blessed base XFS image.
        echo "[forked-xfs/base] Creating base XFS..."
        dd if=/dev/zero of=/work/forked-xfs/base.raw bs=1M count='"$IMG_SIZE_MB"' status=none
        mkfs.xfs -q -f /work/forked-xfs/base.raw
        mkdir -p /mnt/img
        mount -o loop /work/forked-xfs/base.raw /mnt/img
        tar xf /work/forked-xfs/base.tar -C /mnt/img 2>/dev/null || true
        umount /mnt/img
        rm -f /work/forked-xfs/base.tar
        echo "[forked-xfs/base] Done"

        # Fork: copy the base image, then mount and write into each copy.

        echo "[forked-xfs/vm-0-express] Forking + npm install express..."
        cp /work/forked-xfs/base.raw /work/forked-xfs/vm-0-express.raw
        mount -o loop /work/forked-xfs/vm-0-express.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked-xfs/vm-0-express] Done"

        echo "[forked-xfs/vm-1-express] Forking + npm install express..."
        cp /work/forked-xfs/base.raw /work/forked-xfs/vm-1-express.raw
        mount -o loop /work/forked-xfs/vm-1-express.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked-xfs/vm-1-express] Done"

        echo "[forked-xfs/vm-2-nextjs] Forking + npm install next react..."
        cp /work/forked-xfs/base.raw /work/forked-xfs/vm-2-nextjs.raw
        mount -o loop /work/forked-xfs/vm-2-nextjs.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install next react react-dom >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked-xfs/vm-2-nextjs] Done"

        echo "[forked-xfs/vm-3-app] Forking + express + app code..."
        cp /work/forked-xfs/base.raw /work/forked-xfs/vm-3-app.raw
        mount -o loop /work/forked-xfs/vm-3-app.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        mkdir -p src
        for i in $(seq 1 50); do
            dd if=/dev/urandom bs=4096 count=4 2>/dev/null | base64 > src/module_${i}.js
        done
        echo "const express = require(\"express\"); const app = express(); app.listen(3000);" > index.js
        cd / && umount /mnt/img
        echo "[forked-xfs/vm-3-app] Done"
    '

# ---------------------------------------------------------------------------
# SET 4: Pack forked set into btrfs — same fork-then-write pattern as SET 2
# but with btrfs to measure metadata/data block group separation on dedup.
# ---------------------------------------------------------------------------

echo ""
echo "=== Packing forked set into btrfs (fork-then-write) ==="

docker run --rm --privileged \
    -v "$OUT:/work" \
    glidefs-dedup-helper bash -c '
        set -e

        # Create the blessed base btrfs image.
        echo "[forked-btrfs/base] Creating base btrfs..."
        dd if=/dev/zero of=/work/forked-btrfs/base.raw bs=1M count='"$IMG_SIZE_MB"' status=none
        mkfs.btrfs -q -f /work/forked-btrfs/base.raw
        mkdir -p /mnt/img
        mount -o loop /work/forked-btrfs/base.raw /mnt/img
        tar xf /work/forked-btrfs/base.tar -C /mnt/img 2>/dev/null || true
        umount /mnt/img
        rm -f /work/forked-btrfs/base.tar
        echo "[forked-btrfs/base] Done"

        # Fork: copy the base image, then mount and write into each copy.

        echo "[forked-btrfs/vm-0-express] Forking + npm install express..."
        cp /work/forked-btrfs/base.raw /work/forked-btrfs/vm-0-express.raw
        mount -o loop /work/forked-btrfs/vm-0-express.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked-btrfs/vm-0-express] Done"

        echo "[forked-btrfs/vm-1-express] Forking + npm install express..."
        cp /work/forked-btrfs/base.raw /work/forked-btrfs/vm-1-express.raw
        mount -o loop /work/forked-btrfs/vm-1-express.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked-btrfs/vm-1-express] Done"

        echo "[forked-btrfs/vm-2-nextjs] Forking + npm install next react..."
        cp /work/forked-btrfs/base.raw /work/forked-btrfs/vm-2-nextjs.raw
        mount -o loop /work/forked-btrfs/vm-2-nextjs.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install next react react-dom >/dev/null 2>&1
        cd / && umount /mnt/img
        echo "[forked-btrfs/vm-2-nextjs] Done"

        echo "[forked-btrfs/vm-3-app] Forking + express + app code..."
        cp /work/forked-btrfs/base.raw /work/forked-btrfs/vm-3-app.raw
        mount -o loop /work/forked-btrfs/vm-3-app.raw /mnt/img
        cd /mnt/img/root && mkdir -p app && cd app
        npm init -y >/dev/null 2>&1
        npm install express >/dev/null 2>&1
        mkdir -p src
        for i in $(seq 1 50); do
            dd if=/dev/urandom bs=4096 count=4 2>/dev/null | base64 > src/module_${i}.js
        done
        echo "const express = require(\"express\"); const app = express(); app.listen(3000);" > index.js
        cd / && umount /mnt/img
        echo "[forked-btrfs/vm-3-app] Done"
    '

echo ""
echo "=== Generated images ==="
echo ""
echo "Independent builds (worst case, ext4):"
for name in $INDEP_NAMES; do
    size=$(ls -lh "$OUT/independent/${name}.raw" | awk '{print $5}')
    echo "  $OUT/independent/${name}.raw  ($size)"
done
echo ""
echo "Forked from same base (ext4):"
for name in base vm-0-express vm-1-express vm-2-nextjs vm-3-app vm-4-git-clone vm-5-pip-django vm-6-many-files vm-7-mixed-workload; do
    [ -f "$OUT/forked/${name}.raw" ] && {
        size=$(ls -lh "$OUT/forked/${name}.raw" | awk '{print $5}')
        echo "  $OUT/forked/${name}.raw  ($size)"
    }
done
echo ""
echo "Forked from same base (XFS):"
for name in base vm-0-express vm-1-express vm-2-nextjs vm-3-app; do
    size=$(ls -lh "$OUT/forked-xfs/${name}.raw" | awk '{print $5}')
    echo "  $OUT/forked-xfs/${name}.raw  ($size)"
done
echo ""
echo "Forked from same base (btrfs):"
for name in base vm-0-express vm-1-express vm-2-nextjs vm-3-app; do
    size=$(ls -lh "$OUT/forked-btrfs/${name}.raw" | awk '{print $5}')
    echo "  $OUT/forked-btrfs/${name}.raw  ($size)"
done

echo ""
echo "Run measurements:"
echo "  cargo run --release --bin dedup_measure -- $OUT/independent/*.raw"
echo "  cargo run --release --bin dedup_measure -- $OUT/forked/*.raw"
echo "  cargo run --release --bin dedup_measure -- $OUT/forked-xfs/*.raw"
echo "  cargo run --release --bin dedup_measure -- $OUT/forked-btrfs/*.raw"
