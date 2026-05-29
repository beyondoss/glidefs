#!/usr/bin/env bash
# Claim 5: does a buffered foyer cache steal page cache from a co-resident tenant?
#
# An fio "tenant" (buffered random reads over a file it wants page-cached) runs
# alone, then alongside a foyer antagonist in buffered vs O_DIRECT mode -- all
# inside a memory-capped *user* cgroup scope (no sudo), so it can't disturb the
# real tenants on this box.
#
#   buffered antagonist -> ~2 GiB page cache competes with fio's 2 GiB in a 3 GiB
#                          cap -> fio's pages evicted -> fio slows down.
#   direct   antagonist -> ~0 page cache -> fio keeps its working set -> fast.
set -uo pipefail

ROOT=/home/jared/glidefs
ANTAG=$ROOT/target/release/examples/cache_ram
WORK=$ROOT/target/interference
FIOFILE=$WORK/tenant.dat
ANTAG_DIR=$WORK/antag_cache
INNER=$WORK/_inner.sh
CAP=${CAP:-3G}
FIO_SIZE=${FIO_SIZE:-2G}
ANTAG_BLOCKS=${ANTAG_BLOCKS:-16384}   # 16384 * 128KiB = 2 GiB working set
SOAK=${SOAK:-60}

mkdir -p "$WORK"
[ -f "$FIOFILE" ] || fio --name=prep --filename="$FIOFILE" --size="$FIO_SIZE" \
  --rw=write --bs=1M --ioengine=psync --direct=1 >/dev/null 2>&1

# Inner runner executed *inside* the capped scope: args = label mode
cat > "$INNER" <<'INNER_EOF'
#!/usr/bin/env bash
set -uo pipefail
label=$1 mode=$2
ANTAG=$3 BLOCKS=$4 ADIR=$5 SOAK=$6 FIOFILE=$7 FIO_SIZE=$8 WORK=$9
if [ "$mode" != none ]; then
  "$ANTAG" "$mode" 64 "$BLOCKS" "$ADIR" "$SOAK" > "$WORK/antag.log" 2>&1 &
  ap=$!
  while ! grep -q SOAKING "$WORK/antag.log" 2>/dev/null; do
    kill -0 "$ap" 2>/dev/null || { echo "$label	ANTAGONIST DIED"; tail -2 "$WORK/antag.log"; exit 1; }
    sleep 0.5
  done
  sleep 2   # let the soak warm the page cache
fi
# Warm the tenant file into page cache *inside this cgroup*, then read without
# invalidating it (fio defaults to invalidate=1, which would drop it cold).
dd if="$FIOFILE" of=/dev/null bs=1M 2>/dev/null
fio --name=tenant --filename="$FIOFILE" --size="$FIO_SIZE" --rw=randread --bs=128k \
  --ioengine=psync --direct=0 --invalidate=0 --runtime=20 --time_based --numjobs=1 \
  --group_reporting --output-format=json 2>/dev/null \
| jq -r --arg l "$label" '.jobs[0].read |
    "\($l)\tIOPS=\(.iops|floor)\tclat_mean=\(.clat_ns.mean/1000|floor)us\tp99=\(.clat_ns.percentile."99.000000"/1000|floor)us\tBW=\(.bw/1024|floor)MiB/s"'
[ "$mode" != none ] && kill "${ap:-0}" 2>/dev/null
exit 0
INNER_EOF
chmod +x "$INNER"

scenario() { # $1=label  $2=mode
  rm -rf "$ANTAG_DIR"
  systemd-run --user --scope -q -p MemoryMax=$CAP \
    bash "$INNER" "$1" "$2" "$ANTAG" "$ANTAG_BLOCKS" "$ANTAG_DIR" "$SOAK" "$FIOFILE" "$FIO_SIZE" "$WORK"
}

echo "=== Claim 5: tenant interference (cap=$CAP, fio=$FIO_SIZE, antagonist=2GiB) ==="
scenario "baseline (fio alone)"  none
scenario "+ buffered antagonist" buffered
scenario "+ O_DIRECT antagonist" direct
rm -rf "$ANTAG_DIR" "$INNER"
