#!/usr/bin/env bash
#
# Measure block churn from steady-state workloads over time.
#
# Forks the base image, runs a real workload for DURATION seconds,
# takes snapshots every INTERVAL seconds, then diffs consecutive
# snapshots to produce temporal block churn data.
#
# Workloads:
#   web-server  - Express server + sustained HTTP load
#   idle        - Running system, minimal filesystem activity
#   dev-loop    - Simulated dev: edit files, run tests, repeat
#   postgres    - pgbench TPC-B workload (mixed read/write transactions)
#   sqlite      - Continuous inserts + updates + WAL checkpoints
#
# Usage:
#   ./scripts/steady_state_churn.sh [workload] [duration_secs] [interval_secs]
#
# Example:
#   ./scripts/steady_state_churn.sh web-server 300 30
#   ./scripts/steady_state_churn.sh idle 300 30
#   ./scripts/steady_state_churn.sh dev-loop 300 30
#
# Prerequisites:
#   - Docker
#   - Base image at /tmp/glidefs-dedup-test/forked/base.raw
#   - cargo build --release --bin block_churn_measure (in glidefs/)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHURN_BIN="$PROJECT_ROOT/glidefs/target/release/block_churn_measure"
# Also check the workspace-root target dir (depends on build location)
if [ ! -x "$CHURN_BIN" ]; then
    CHURN_BIN="$PROJECT_ROOT/target/release/block_churn_measure"
fi

WORKLOAD="${1:-web-server}"
DURATION="${2:-300}"
INTERVAL="${3:-30}"
BASE="/tmp/glidefs-dedup-test/forked/base.raw"
OUT="/tmp/glidefs-steady-state/${WORKLOAD}"
IMG_SIZE_MB=512

if [ ! -x "$CHURN_BIN" ]; then
    echo "error: block_churn_measure not found at $CHURN_BIN"
    echo "Run: cd glidefs && cargo build --release --bin block_churn_measure"
    exit 1
fi

# Database workloads need more space than the 512MB base image provides.
# Create a fresh 1GB ext4 image instead of forking the base.
case "$WORKLOAD" in
    postgres|sqlite)
        FRESH_IMAGE=true
        IMG_SIZE_MB=1024
        ;;
    *)
        FRESH_IMAGE=false
        if [ ! -f "$BASE" ]; then
            echo "error: base image not found at $BASE"
            echo "Run ./scripts/gen_dedup_images.sh first"
            exit 1
        fi
        ;;
esac

mkdir -p "$OUT"

echo "=== Steady-State Block Churn Measurement ==="
echo "  Workload:  $WORKLOAD"
echo "  Duration:  ${DURATION}s"
echo "  Interval:  ${INTERVAL}s"
echo "  Snapshots: $(( DURATION / INTERVAL ))"
echo ""

# Create working image
if [ "$FRESH_IMAGE" = true ]; then
    echo "Creating fresh ${IMG_SIZE_MB}MB ext4 image..."
    dd if=/dev/zero of="$OUT/live.raw" bs=1M count="$IMG_SIZE_MB" 2>/dev/null
    docker run --rm --privileged \
        -v "$OUT:/work" \
        glidefs-dedup-helper bash -c "mkfs.ext4 -q /work/live.raw" 2>/dev/null
else
    echo "Forking base image..."
    cp "$BASE" "$OUT/live.raw"
fi

# Define workload scripts
case "$WORKLOAD" in
    web-server)
        WORKLOAD_SCRIPT='
            cd /root && mkdir -p app && cd app
            npm init -y >/dev/null 2>&1
            npm install express >/dev/null 2>&1

            # Create a server that writes logs and temp data
            cat > server.js << "SERVEREOF"
const express = require("express");
const fs = require("fs");
const app = express();
let reqCount = 0;

// Write access log to disk (like a real server)
app.use((req, res, next) => {
    reqCount++;
    const line = `${new Date().toISOString()} ${req.method} ${req.url} ${reqCount}\n`;
    fs.appendFileSync("/root/app/access.log", line);

    // Every 100 requests, write a "session" file
    if (reqCount % 100 === 0) {
        const sessionId = Math.random().toString(36).slice(2);
        fs.writeFileSync(`/tmp/sess_${sessionId}`, JSON.stringify({
            id: sessionId,
            count: reqCount,
            ts: Date.now(),
            data: "x".repeat(4096)
        }));
    }

    // Every 500 requests, rotate the log
    if (reqCount % 500 === 0) {
        try {
            fs.renameSync("/root/app/access.log", `/root/app/access.log.${Date.now()}`);
        } catch(e) {}
    }

    next();
});

app.get("/", (req, res) => res.json({ ok: true, count: reqCount }));
app.get("/heavy", (req, res) => {
    // Write a temp file (simulates report generation)
    const data = Buffer.alloc(32768, "A");
    fs.writeFileSync(`/tmp/report_${Date.now()}.tmp`, data);
    res.json({ ok: true, size: 32768 });
});

app.listen(3000, () => console.log("Server ready"));
SERVEREOF

            node server.js &
            SERVER_PID=$!
            sleep 2

            # Load generator: moderate sustained traffic
            while true; do
                for i in $(seq 1 50); do
                    curl -s http://localhost:3000/ > /dev/null &
                    if [ $(( i % 10 )) -eq 0 ]; then
                        curl -s http://localhost:3000/heavy > /dev/null &
                    fi
                done
                wait
                sleep 0.5
            done
        '
        ;;

    idle)
        WORKLOAD_SCRIPT='
            # Just sit there. Systemd services, journald, cron would normally
            # write, but in a container we simulate minimal idle activity.
            while true; do
                # Simulate journald-like log writes (small appends)
                echo "$(date -u +%Y-%m-%dT%H:%M:%S) idle tick" >> /var/log/idle.log

                # Simulate /tmp cleanup creating a small file
                echo "$$" > /tmp/healthcheck

                sleep 5
            done
        '
        ;;

    dev-loop)
        WORKLOAD_SCRIPT='
            cd /root && mkdir -p project && cd project
            npm init -y >/dev/null 2>&1
            npm install express mocha chai >/dev/null 2>&1

            mkdir -p src tests

            # Initial project setup
            for i in $(seq 1 20); do
                cat > src/module_${i}.js << MODEOF
// Module $i
const data = { id: $i, created: Date.now() };
module.exports = { getData: () => data, process: (x) => x * $i };
MODEOF
            done

            for i in $(seq 1 10); do
                cat > tests/test_${i}.js << TESTEOF
const assert = require("assert");
const mod = require("../src/module_${i}");
describe("module ${i}", () => {
    it("returns data", () => assert.ok(mod.getData()));
    it("processes", () => assert.equal(mod.process(2), 2 * ${i}));
});
TESTEOF
            done

            # Simulate edit-test-commit loop
            cycle=0
            while true; do
                cycle=$((cycle + 1))

                # Edit a random file
                target=$((RANDOM % 20 + 1))
                cat > src/module_${target}.js << EDITEOF
// Module $target - edit $cycle at $(date +%s)
const data = { id: $target, v: $cycle, ts: Date.now() };
const cache = Buffer.alloc(8192);
module.exports = { getData: () => data, process: (x) => x * $target + $cycle };
EDITEOF

                # Create a new file occasionally
                if [ $((cycle % 5)) -eq 0 ]; then
                    cat > src/feature_${cycle}.js << FEATEOF
// Feature $cycle
module.exports = { run: () => console.log("feature $cycle") };
FEATEOF
                fi

                # Run tests (reads many files, writes test output)
                npx mocha tests/ --reporter min 2>/dev/null || true

                # Simulate npm install of a new dep occasionally
                if [ $((cycle % 10)) -eq 0 ]; then
                    npm install uuid >/dev/null 2>&1 || true
                fi

                sleep 3
            done
        '
        ;;

    postgres)
        WORKLOAD_SCRIPT='
            # Postgres writes WAL, runs checkpoints, bgwriter flushes dirty pages.
            # This measures the block-level impact of a typical OLTP workload.
            # All data files go to /mnt/img/root/pgdata (the mounted ext4 image).

            PGBIN=/usr/lib/postgresql/14/bin
            PGDATA=/mnt/img/pgdata

            mkdir -p "$PGDATA" /var/run/postgresql
            chown postgres:postgres "$PGDATA" /var/run/postgresql

            su postgres -c "$PGBIN/initdb -D $PGDATA" >/dev/null 2>&1

            # Tune for aggressive checkpoints (listen_addresses defaults to localhost)
            {
                echo "shared_buffers = 16MB"
                echo "checkpoint_timeout = 30s"
                echo "max_wal_size = 32MB"
                echo "min_wal_size = 32MB"
                echo "wal_level = minimal"
                echo "max_wal_senders = 0"
                echo "fsync = on"
                echo "synchronous_commit = on"
            } >> $PGDATA/postgresql.conf

            su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/logfile start -w" 2>&1
            sleep 2
            su postgres -c "$PGBIN/createdb pgbench" 2>&1
            su postgres -c "$PGBIN/pgbench -i -s 1 pgbench" 2>&1

            # Run pgbench: mixed read/write TPC-B transactions
            # -c 4 clients, -T runs for the full duration
            su postgres -c "$PGBIN/pgbench -c 4 -T 86400 pgbench" &
            BENCH_PID=$!

            wait $BENCH_PID 2>/dev/null || true
        '
        ;;

    sqlite)
        WORKLOAD_SCRIPT='
            # SQLite WAL mode: writes go to WAL, periodic checkpoints merge to DB.
            # Measures block churn from continuous small transactions.
            # DB file goes to the mounted ext4 image.

            mkdir -p /mnt/img/sqldata && cd /mnt/img/sqldata
            cat > workload.py << '\''PYEOF'\''
import sqlite3, time, random, string, os

db_path = "/mnt/img/sqldata/bench.db"
conn = sqlite3.connect(db_path)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA wal_autocheckpoint=1000")

conn.execute("""CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL
)""")
conn.execute("""CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
)""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)")
conn.commit()

def random_string(n):
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))

cycle = 0
while True:
    cycle += 1
    try:
        # Batch insert events (simulates logging/telemetry)
        with conn:
            for _ in range(50):
                conn.execute(
                    "INSERT INTO events (ts, kind, payload) VALUES (?, ?, ?)",
                    (time.time(), random.choice(["click", "view", "api", "error"]),
                     random_string(random.randint(64, 512)))
                )

        # Update KV store (simulates session/config writes)
        with conn:
            for _ in range(10):
                key = f"session:{random.randint(1, 100)}"
                conn.execute(
                    "INSERT OR REPLACE INTO kv (key, value, updated_at) VALUES (?, ?, ?)",
                    (key, random_string(256), time.time())
                )

        # Occasional bulk delete (simulates cleanup)
        if cycle % 20 == 0:
            cutoff = time.time() - 60
            with conn:
                conn.execute("DELETE FROM events WHERE ts < ?", (cutoff,))

        # Occasional VACUUM (simulates maintenance)
        if cycle % 100 == 0:
            conn.execute("VACUUM")

        time.sleep(0.5)
    except Exception as e:
        print(f"error: {e}")
        time.sleep(1)
PYEOF

            python3 workload.py &
            PY_PID=$!
            wait $PY_PID 2>/dev/null || true
        '
        ;;

    *)
        echo "error: unknown workload '$WORKLOAD'"
        echo "Available: web-server, idle, dev-loop, postgres, sqlite"
        exit 1
        ;;
esac

# Take initial snapshot
echo "Taking initial snapshot (t=0)..."
cp "$OUT/live.raw" "$OUT/snap_000.raw"

# Some workloads (postgres, sqlite) need binaries from the container,
# not from the chroot. USE_CHROOT controls this.
case "$WORKLOAD" in
    postgres|sqlite) USE_CHROOT=false ;;
    *) USE_CHROOT=true ;;
esac

# Start the workload in a container
echo "Starting workload container..."
if [ "$USE_CHROOT" = true ]; then
    CONTAINER_ID=$(docker run -d --rm --privileged \
        -v "$OUT:/work" \
        glidefs-dedup-helper bash -c "
            set -e
            mkdir -p /mnt/img
            mount -o loop /work/live.raw /mnt/img

            # Run inside the mounted filesystem
            export HOME=/mnt/img/root
            cd /mnt/img
            chroot /mnt/img bash -c '$WORKLOAD_SCRIPT' &
            WORK_PID=\$!

            # Wait for DURATION, periodically sync
            for i in \$(seq 1 $DURATION); do
                sleep 1
                if [ \$((i % $INTERVAL)) -eq 0 ]; then
                    sync
                    echo \"SYNC_READY \$i\"
                fi
            done

            kill \$WORK_PID 2>/dev/null || true
            wait \$WORK_PID 2>/dev/null || true
            sync
            umount /mnt/img
            echo 'WORKLOAD_DONE'
        ")
else
    # Run in container directly, data files on mounted filesystem.
    # Binaries (postgres, python3, sqlite3) come from the container image.
    # Write workload script to a file to avoid nested quoting issues.
    WORKLOAD_FILE="$OUT/workload.sh"
    printf '%s\n' "$WORKLOAD_SCRIPT" > "$WORKLOAD_FILE"
    chmod +x "$WORKLOAD_FILE"
    CONTAINER_ID=$(docker run -d --rm --privileged \
        -v "$OUT:/work" \
        glidefs-dedup-helper bash -c "
            set -e
            mkdir -p /mnt/img
            mount -o loop /work/live.raw /mnt/img
            cd /tmp

            bash /work/workload.sh &
            WORK_PID=\$!

            for i in \$(seq 1 $DURATION); do
                sleep 1
                if [ \$((i % $INTERVAL)) -eq 0 ]; then
                    sync
                    echo \"SYNC_READY \$i\"
                fi
            done

            kill \$WORK_PID 2>/dev/null || true
            wait \$WORK_PID 2>/dev/null || true
            sync
            umount /mnt/img
            echo 'WORKLOAD_DONE'
        ")
fi

echo "Container: $CONTAINER_ID"
echo ""

# Monitor container logs and take snapshots at sync points
SNAP_NUM=0
echo "Monitoring for sync points..."
docker logs -f "$CONTAINER_ID" 2>&1 | while IFS= read -r line; do
    if [[ "$line" == SYNC_READY* ]]; then
        ELAPSED=$(echo "$line" | awk '{print $2}')
        SNAP_NUM=$((SNAP_NUM + 1))
        SNAP_NAME=$(printf "snap_%03d" "$SNAP_NUM")
        cp "$OUT/live.raw" "$OUT/${SNAP_NAME}.raw"
        echo "  Snapshot ${SNAP_NAME} at t=${ELAPSED}s"
    fi
    if [[ "$line" == "WORKLOAD_DONE" ]]; then
        echo ""
        echo "Workload complete."
        break
    fi
done

# Wait for container to finish
docker wait "$CONTAINER_ID" 2>/dev/null || true

# Take final snapshot
SNAP_NUM=$((SNAP_NUM + 1))
SNAP_NAME=$(printf "snap_%03d" "$SNAP_NUM")
cp "$OUT/live.raw" "$OUT/${SNAP_NAME}.raw" 2>/dev/null || true

# Analyze: diff each consecutive pair of snapshots
echo ""
echo "=== Temporal Churn Analysis ==="
echo ""

SNAPS=($(ls "$OUT"/snap_*.raw 2>/dev/null | sort))
NUM_SNAPS=${#SNAPS[@]}

if [ "$NUM_SNAPS" -lt 2 ]; then
    echo "Not enough snapshots for temporal analysis."
    echo "Running cumulative diff against base..."
    "$CHURN_BIN" \
        --base "$BASE" \
        --duration-secs "$DURATION" \
        "$OUT/live.raw"
    exit 0
fi

# For fresh images, compare against snap_000 (not the forked base)
CHURN_BASE="${SNAPS[0]}"
if [ "$FRESH_IMAGE" = false ] && [ -f "$BASE" ]; then
    CHURN_BASE="$BASE"
fi

echo "Cumulative churn (base → final):"
"$CHURN_BIN" \
    --base "$CHURN_BASE" \
    --duration-secs "$DURATION" \
    "${SNAPS[-1]}" 2>/dev/null

echo ""
echo "=== Interval Churn (${INTERVAL}s windows) ==="
echo ""
echo "┌──────────┬────────────┬──────────┬────────────┐"
echo "│ Interval │ New Blocks │ Changed  │ Unique/sec │"
echo "├──────────┼────────────┼──────────┼────────────┤"

for ((i=1; i<NUM_SNAPS; i++)); do
    PREV="${SNAPS[$((i-1))]}"
    CURR="${SNAPS[$i]}"
    T_START=$(( (i-1) * INTERVAL ))
    T_END=$(( i * INTERVAL ))

    # Run churn measure between consecutive snapshots
    OUTPUT=$("$CHURN_BIN" \
        --base "$PREV" \
        --duration-secs "$INTERVAL" \
        "$CURR" 2>/dev/null)

    NEW=$(echo "$OUTPUT" | grep "New:" | awk '{print $2}')
    CHANGED=$(echo "$OUTPUT" | grep "Changed:" | awk '{print $2}')
    RATE=$(echo "$OUTPUT" | grep "Write rate:" | awk '{print $3}')

    printf "│ %3d-%3ds │ %10s │ %8s │ %10s │\n" \
        "$T_START" "$T_END" "${NEW:-0}" "${CHANGED:-0}" "${RATE:-0}"
done

echo "└──────────┴────────────┴──────────┴────────────┘"

# Cleanup snapshots (keep live.raw for manual inspection)
echo ""
echo "Snapshots saved in $OUT/"
echo "To re-analyze: ls $OUT/snap_*.raw"
