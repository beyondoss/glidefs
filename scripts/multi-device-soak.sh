#!/usr/bin/env bash
# Multi-device density soak.
#
# Runs N parallel ublk-backed fio jobs against a single glidefs daemon
# for $DURATION seconds. Tests what the single-device `zc_glidefs_soak`
# integration test can't reach:
#
#   * worker_pool's queue→worker scheduling under N-device contention
#   * cross-device rotation gate behavior (each export has its own
#     `data_file` RwLock — should be fully independent)
#   * the ZC inline fast path under concurrent dispatch from many queues
#   * per-export memory accumulation at density
#   * pack-GC keeping up with N × rotation throughput
#
# Acceptance: all N fio jobs complete without I/O error, daemon
# survives, RSS bounded, FDs bounded.
#
# Usage:
#   DEVICES=32 DURATION=1800 ./scripts/multi-device-soak.sh
#
# Required binaries (override paths if not in /tmp):
#   GLIDEFS_BIN  — `glidefs` daemon binary (build: cargo build --release
#                  --features ublk -p glidefs --bin glidefs)
#   BENCH        — `scripts/ublk_bench.py` (in this repo)
#   CONFIG       — daemon config TOML (see scripts/multi-device-bench.toml
#                  for the bench-tuned shape: memory:// store, manual
#                  flush_mode, 4 ublk queues)
set -uo pipefail

API=${API:-http://127.0.0.1:9113}
GLIDEFS_BIN=${GLIDEFS_BIN:-/tmp/glidefs}
BENCH=${BENCH:-/tmp/ublk_bench.py}
CONFIG=${CONFIG:-/tmp/bench-glidefs.toml}
PREFIX=soak-
DEVICES=${DEVICES:-32}
DURATION=${DURATION:-1800}      # 30 minutes default
DEPTH=${DEPTH:-16}              # per-device QD — lower than the bench
                                # so N×QD fits the VM's memory budget
WORKING=${WORKING:-128m}        # per-device working area
SAMPLE_EVERY=${SAMPLE_EVERY:-60}  # RSS/FD sample interval
RESULTS_DIR=/tmp/multi-soak
mkdir -p "$RESULTS_DIR"
rm -rf "$RESULTS_DIR"/*

start_daemon() {
    sudo -n rm -rf /var/cache/glidefs-bench /run/glidefs
    sudo -n mkdir -p /var/cache/glidefs-bench /run/glidefs
    sudo -n prlimit --nofile=65536 -- env RUST_LOG=warn,glidefs=info \
        "$GLIDEFS_BIN" run -c "$CONFIG" >/tmp/glidefs-multisoak.log 2>&1 &
}

wait_ready() {
    for _ in $(seq 1 30); do
        curl -sf -m 1 "$API/health/ready" >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

stop_daemon() {
    sudo -n pkill -KILL -x glidefs 2>/dev/null
    sleep 2
}

snapshot() {
    local label=$1
    local pid
    pid=$(pgrep -x glidefs | head -1)
    [ -z "$pid" ] && { printf "%-12s daemon GONE\n" "$label"; return 1; }
    local rss_kb vsz_kb threads fds
    rss_kb=$(awk '/VmRSS/ {print $2}' /proc/$pid/status)
    vsz_kb=$(awk '/VmSize/ {print $2}' /proc/$pid/status)
    threads=$(awk '/Threads/ {print $2}' /proc/$pid/status)
    fds=$(sudo -n ls /proc/$pid/fd 2>/dev/null | wc -l)
    printf "%-12s rss=%dMB vsz=%dMB threads=%s fds=%s\n" \
        "$label" $((rss_kb/1024)) $((vsz_kb/1024)) "$threads" "$fds"
}

echo "=========================================================="
echo "  Multi-device density soak"
echo "  DEVICES=$DEVICES DURATION=${DURATION}s DEPTH=$DEPTH WORKING=$WORKING"
echo "=========================================================="
echo

start_daemon
if ! wait_ready; then
    echo "FATAL: daemon failed to become ready"
    tail -30 /tmp/glidefs-multisoak.log
    exit 1
fi
snapshot baseline

echo "--- setup $DEVICES exports ---"
t0=$(date +%s)
python3 "$BENCH" --format json --output "$RESULTS_DIR/setup.json" \
    setup --count "$DEVICES" --transport ublk --prefix "$PREFIX" >/dev/null 2>&1
echo "setup time: $(( $(date +%s) - t0 ))s"
sleep 2
snapshot after-setup

# Resolve device paths.
declare -A DEV_OF
for i in $(seq -f '%04g' 0 $((DEVICES-1))); do
    name="${PREFIX}${i}"
    devpath=$(curl -sf -m 3 "$API/api/exports/$name" 2>/dev/null \
              | python3 -c "import sys,json; print(json.load(sys.stdin).get('device',''))" 2>/dev/null)
    if [ -z "$devpath" ] || [ ! -b "$devpath" ]; then
        echo "warn: $name → '$devpath' (skipping)"
        continue
    fi
    DEV_OF[$name]=$devpath
done
echo "resolved ${#DEV_OF[@]} of $DEVICES devices"

# Launch parallel fio jobs.
echo "--- launching $DEVICES fio jobs (randrw 70/30, runtime=${DURATION}s) ---"
mkdir -p "$RESULTS_DIR/fio"
pids=()
for name in "${!DEV_OF[@]}"; do
    devpath=${DEV_OF[$name]}
    sudo -n fio --name="$name" --filename="$devpath" \
        --ioengine=io_uring --direct=1 --rw=randrw --bs=4k \
        --iodepth="$DEPTH" --numjobs=1 \
        --time_based --runtime="$DURATION" --ramp_time=5 \
        --size="$WORKING" --rwmixread=70 --norandommap \
        --output-format=json --output="$RESULTS_DIR/fio/$name.json" \
        >/dev/null 2>&1 &
    pids+=($!)
done
sleep 3
snapshot fio-running

# Periodic snapshot loop.
start_ts=$(date +%s)
sample_idx=0
while true; do
    sleep "$SAMPLE_EVERY"
    elapsed=$(($(date +%s) - start_ts))
    sample_idx=$((sample_idx + 1))
    if ! snapshot "t+${elapsed}s"; then
        echo "FATAL: daemon died mid-soak at t+${elapsed}s"
        break
    fi
    # Check if all fio jobs have finished.
    alive=0
    for p in "${pids[@]}"; do
        if kill -0 "$p" 2>/dev/null; then
            alive=$((alive + 1))
        fi
    done
    if [ "$alive" -eq 0 ]; then
        echo "all $DEVICES fio jobs completed at t+${elapsed}s"
        break
    fi
    [ "$elapsed" -ge "$DURATION" ] && break
done

# Final wait + status.
wait "${pids[@]}" 2>/dev/null
snapshot post-soak

# Aggregate.
echo
echo "--- aggregated fio results ---"
python3 - "$RESULTS_DIR/fio" <<'PYEOF'
import json, os, sys
fio_dir = sys.argv[1]
files = sorted(f for f in os.listdir(fio_dir) if f.endswith('.json'))
if not files:
    print("no fio result files")
    sys.exit(1)
total_r_iops = 0.0; total_w_iops = 0.0
total_r_bw_kib = 0.0; total_w_bw_kib = 0.0
errors = 0
err_devs = []
for f in files:
    try:
        d = json.load(open(os.path.join(fio_dir, f)))
        jobs = d.get('jobs', [])
        if not jobs:
            errors += 1; err_devs.append(f"{f}:no-jobs"); continue
        j = jobs[0]
        if j.get('error', 0) != 0:
            errors += 1; err_devs.append(f"{f}:err={j['error']}"); continue
        r = j.get('read', {}); w = j.get('write', {})
        total_r_iops += r.get('iops', 0); total_r_bw_kib += r.get('bw', 0)
        total_w_iops += w.get('iops', 0); total_w_bw_kib += w.get('bw', 0)
    except Exception as e:
        errors += 1; err_devs.append(f"{f}:parse-fail {e}")

print(f"devices completed: {len(files) - errors}/{len(files)}")
print(f"  read   iops={total_r_iops:>10.0f}  bw={total_r_bw_kib/1024:>7.1f} MiB/s")
print(f"  write  iops={total_w_iops:>10.0f}  bw={total_w_bw_kib/1024:>7.1f} MiB/s")
print(f"  total  iops={total_r_iops+total_w_iops:>10.0f}  bw={(total_r_bw_kib+total_w_bw_kib)/1024:>7.1f} MiB/s")
if errors:
    print(f"  ERRORS: {errors} devices")
    for d in err_devs[:5]:
        print(f"    {d}")
    sys.exit(2)
PYEOF
RC=$?

stop_daemon
exit $RC
