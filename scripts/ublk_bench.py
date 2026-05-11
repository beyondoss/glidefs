#!/usr/bin/env python3
"""
ublk/nbd transport regression bench.

Creates N test exports via the glidefs /api/exports PUT endpoint, runs fio
in parallel against them, aggregates IOPS / latency / bandwidth, and tears
the exports down. Used to detect regressions across milestones of the
multiplex rewrite (see plan: we-do-not-need-dazzling-dewdrop).

Subcommands:
  baseline       Run the regression suite (single + multi-device matrix).
  single         One device, one fio. Most useful for depth-curve diagnosis.
  parallel       N devices, one fio each, in parallel. Aggregate measurement.
  setup          Create N exports of a given transport.
  teardown       Delete all exports matching a name prefix.
  list           Show all exports.
  sysinfo        Snapshot of glidefs daemon: thread count, RSS, VSZ, FDs.

Output is JSON to stdout by default; --format human gives a tabular summary.
fio is required; sudo is required (block-device direct I/O).

Examples:
  ./scripts/ublk_bench.py baseline --transport ublk --output baseline.json
  ./scripts/ublk_bench.py single --transport ublk --rw randwrite --depth 64 --jobs 4
  ./scripts/ublk_bench.py parallel --transport ublk --devices 8 --rw randwrite --depth 32
  ./scripts/ublk_bench.py teardown --prefix bench-

==============================================================================
Benchmark methodology (READ ME if numbers look weird)
==============================================================================

For numbers to be apples-to-apples with milestone baselines (M1, M4d, M5, ...):

1. The DAEMON must run the binary you intend to measure. The systemd unit
   runs `/usr/local/bin/glidefs`; `cargo build` deposits to
   `target/release/glidefs`. They are NOT the same file until you install:

     cargo build --release --features ublk -p glidefs --bin glidefs
     sudo cp target/release/glidefs /usr/local/bin/glidefs       # keep .pre-X backup
     sudo systemctl reset-failed glidefs   # unit has StartLimitBurst=1
     sudo systemctl restart glidefs
     until curl -sf http://127.0.0.1:9113/health/ready; do sleep 1; done

2. Bench exports MUST be created with `flush_mode: "manual"` (this script
   does so in `create_export`). Without it:
     - the flush_scheduler turns each bench's accumulated dirty blocks into
       a multipart-PUT storm to real S3 mid-test (contends with bench writes
       for SSD AND tokio runtime AND outbound network);
     - if anything goes wrong with S3 (transient timeout, throttling), the
       per-export 30s circuit-breaker backoff leaves stuck retries that
       contaminate every subsequent bench you run that day (we lived this
       on 2026-05-11: 17 dirty exports × failed multipart PUTs = invented
       a 50% "write regression" out of thin air).
   With flush_mode=manual + `?purge=true` teardown, the bench is fully
   local: WriteCache + ext4 cache file only, zero S3 PUTs.

3. ALWAYS run `baseline` against fresh exports created by this script's
   `setup_n`. Do not run against pre-existing, long-lived exports — their
   WriteCache state is unpredictable (evicted blocks may need S3 GETs;
   leftover dirty blocks bias write latency).

4. Confirm the daemon has no other dirty exports racing for SSD/network:
     curl -s http://127.0.0.1:9113/api/exports | jq '[.[]|select(.dirty_bytes>0)]|length'
   If it's >0 and they're yours, purge them first:
     curl -X DELETE 'http://127.0.0.1:9113/api/exports/NAME?purge=true'

5. Co-tenancy load matters. M5/M4d ran against a daemon with ~0 hosted
   exports; "production density" benches will show lower per-device peak
   because the worker pool is sharing 16 io_urings across thousands of
   queues. Note exports_count in /health/ready when recording numbers.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional

API_DEFAULT = "http://127.0.0.1:9113"
PREFIX_DEFAULT = "bench-"
SIZE_GB_DEFAULT = 4
WARMUP_BYTES_DEFAULT = 64 * 1024 * 1024
RUNTIME_DEFAULT = 12
BS_DEFAULT = "4k"


# ---------------------------------------------------------------------------
# glidefs API client
# ---------------------------------------------------------------------------

def _api(method: str, url: str, body: Optional[dict] = None) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            payload = r.read()
            return r.status, json.loads(payload) if payload else None
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None


def list_exports(api: str) -> list[dict]:
    code, body = _api("GET", f"{api}/api/exports")
    if code != 200 or body is None:
        raise RuntimeError(f"GET /api/exports failed: {code} {body}")
    return body.get("exports", [])


def create_export(api: str, name: str, transport: str, size_gb: int) -> dict:
    # flush_mode=manual: the bench writes never trigger background S3 PUTs.
    # Dirty blocks accumulate in WriteCache, get discarded at purge teardown.
    # Without this, the flush_scheduler races the bench and either contends
    # for SSD/network or — if S3 is degraded — opens the circuit breaker and
    # leaves stuck dirty data behind. See router.rs flush_threshold_or().
    code, body = _api(
        "PUT",
        f"{api}/api/exports/{name}",
        {"size_gb": size_gb, "transport": transport, "flush_mode": "manual"},
    )
    if code not in (200, 201):
        raise RuntimeError(f"PUT export {name} failed: {code} {body}")
    return body


def delete_export(api: str, name: str) -> None:
    # `purge=true` skips the write-cache drain (no synchronous S3 PUTs)
    # and cleans up S3 manifest/snapshots/export-definition. Correct for
    # bench/test exports — never use against production data.
    code, _ = _api("DELETE", f"{api}/api/exports/{name}?purge=true")
    if code not in (200, 204, 404):
        raise RuntimeError(f"DELETE export {name} failed: {code}")


def teardown_prefix(api: str, prefix: str, concurrency: int = 4) -> int:
    """Delete every export whose name starts with `prefix`. Returns count."""
    targets = [e["name"] for e in list_exports(api) if e["name"].startswith(prefix)]
    if not targets:
        return 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(lambda n: delete_export(api, n), targets))
    return len(targets)


def setup_n(api: str, n: int, prefix: str, transport: str, size_gb: int) -> list[dict]:
    """Idempotent: existing matching exports are reused. Returns final list."""
    existing = {e["name"]: e for e in list_exports(api) if e["name"].startswith(prefix)}
    needed = [f"{prefix}{i:04d}" for i in range(n)]

    # Drop any extras beyond `n` so the set is exactly what we asked for.
    for name in list(existing.keys()):
        if name not in needed:
            delete_export(api, name)
            existing.pop(name)

    created: list[dict] = []
    for name in needed:
        if name in existing:
            created.append(existing[name])
        else:
            created.append(create_export(api, name, transport, size_gb))
    return created


# ---------------------------------------------------------------------------
# fio runner
# ---------------------------------------------------------------------------


@dataclass
class FioResult:
    device: str
    rw: str
    bs: str
    depth: int
    jobs: int
    iops: float
    bw_bytes_per_sec: float
    lat_mean_us: float
    lat_p99_us: float
    lat_p999_us: float
    runtime_ms: int

    @classmethod
    def from_fio_json(cls, dev: str, rw: str, bs: str, depth: int, jobs: int, payload: dict) -> "FioResult":
        op = "read" if "read" in rw else "write"
        j = payload["jobs"][0][op]
        clat = j["clat_ns"]
        return cls(
            device=dev,
            rw=rw,
            bs=bs,
            depth=depth,
            jobs=jobs,
            iops=j["iops"],
            bw_bytes_per_sec=j["bw_bytes"],
            lat_mean_us=clat["mean"] / 1e3,
            lat_p99_us=clat["percentile"]["99.000000"] / 1e3,
            lat_p999_us=clat["percentile"]["99.900000"] / 1e3,
            runtime_ms=j["runtime"],
        )


def _strip_sudo_noise(stdout: bytes) -> str:
    """sudo on this box prepends 'unable to resolve host' lines — strip until first '{'."""
    s = stdout.decode("utf-8", errors="replace")
    i = s.find("{")
    return s[i:] if i >= 0 else s


def fio_run(
    dev: str,
    *,
    rw: str,
    bs: str,
    depth: int,
    jobs: int,
    runtime: int,
    offset: int = 0,
    size_bytes: int = 64 * 1024 * 1024,
    extra_args: list[str] | None = None,
    warmup: bool = True,
) -> FioResult:
    """Run a single fio invocation. Includes a brief warmup pass by default."""
    name = f"bench_{os.path.basename(dev)}"
    if warmup:
        # Read-warm the test region once so subsequent runs hit cache.
        warm = [
            "sudo", "fio",
            "--name=warmup",
            f"--filename={dev}",
            "--direct=1",
            "--rw=read",
            f"--bs={bs}",
            f"--size={size_bytes}",
            f"--offset={offset}",
            "--numjobs=1",
            "--output-format=normal",
        ]
        subprocess.run(warm, capture_output=True, timeout=60)

    cmd = [
        "sudo", "fio",
        f"--name={name}",
        f"--filename={dev}",
        "--direct=1",
        "--ioengine=io_uring",
        f"--rw={rw}",
        f"--bs={bs}",
        f"--size={size_bytes}",
        f"--offset={offset}",
        f"--iodepth={depth}",
        f"--numjobs={jobs}",
        f"--runtime={runtime}",
        "--time_based=1",
        "--group_reporting=1",
        "--output-format=json",
    ]
    if extra_args:
        cmd.extend(extra_args)
    proc = subprocess.run(cmd, capture_output=True, timeout=runtime + 60)
    if proc.returncode != 0:
        raise RuntimeError(f"fio failed on {dev}: rc={proc.returncode} stderr={proc.stderr[:500]!r}")
    payload = json.loads(_strip_sudo_noise(proc.stdout))
    return FioResult.from_fio_json(dev, rw, bs, depth, jobs, payload)


# ---------------------------------------------------------------------------
# Scenario: parallel multi-device fio
# ---------------------------------------------------------------------------


@dataclass
class ParallelResult:
    devices: list[str]
    rw: str
    bs: str
    depth: int
    jobs_per_device: int
    runtime: int
    per_device: list[FioResult] = field(default_factory=list)

    @property
    def aggregate_iops(self) -> float:
        return sum(r.iops for r in self.per_device)

    @property
    def aggregate_bw_bytes_per_sec(self) -> float:
        return sum(r.bw_bytes_per_sec for r in self.per_device)

    @property
    def lat_mean_us_avg(self) -> float:
        if not self.per_device:
            return 0.0
        return sum(r.lat_mean_us for r in self.per_device) / len(self.per_device)


def fio_parallel(
    devs: list[str],
    *,
    rw: str,
    bs: str,
    depth: int,
    jobs_per_device: int,
    runtime: int,
    size_bytes: int = 64 * 1024 * 1024,
    extra_args: list[str] | None = None,
    warmup: bool = True,
) -> ParallelResult:
    if warmup:
        # Warm all devices in parallel.
        with ThreadPoolExecutor(max_workers=min(32, len(devs))) as ex:
            warm_cmd = lambda d: subprocess.run(
                ["sudo", "fio", "--name=warmup", f"--filename={d}", "--direct=1",
                 "--rw=read", "--bs=1M", f"--size={size_bytes}", "--offset=0",
                 "--numjobs=1", "--output-format=normal"],
                capture_output=True, timeout=60,
            )
            list(ex.map(warm_cmd, devs))

    def _run(d: str) -> FioResult:
        return fio_run(
            d, rw=rw, bs=bs, depth=depth, jobs=jobs_per_device,
            runtime=runtime, size_bytes=size_bytes, extra_args=extra_args,
            warmup=False,
        )

    with ThreadPoolExecutor(max_workers=len(devs)) as ex:
        per_device = list(ex.map(_run, devs))

    return ParallelResult(
        devices=devs, rw=rw, bs=bs, depth=depth,
        jobs_per_device=jobs_per_device, runtime=runtime, per_device=per_device,
    )


# ---------------------------------------------------------------------------
# System info (glidefs daemon snapshot)
# ---------------------------------------------------------------------------


@dataclass
class SysInfo:
    kernel: str
    pid: Optional[int]
    threads: Optional[int]
    rss_kb: Optional[int]
    vsize_kb: Optional[int]
    fd_count: Optional[int]
    ublk_thread_count: Optional[int]
    tokio_thread_count: Optional[int]
    iou_kthread_count: Optional[int]
    num_cpus: int
    num_numa_nodes: int


def _read_proc_status(pid: int) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if ":" in line:
                    k, _, v = line.partition(":")
                    out[k.strip()] = v.strip()
    except OSError:
        pass
    return out


def _kthread_count(pid: int, prefix: str) -> int:
    try:
        n = 0
        for tid in os.listdir(f"/proc/{pid}/task"):
            try:
                with open(f"/proc/{pid}/task/{tid}/comm") as f:
                    if f.read().startswith(prefix):
                        n += 1
            except OSError:
                continue
        return n
    except OSError:
        return 0


def sysinfo() -> SysInfo:
    kernel = subprocess.run(["uname", "-r"], capture_output=True, text=True).stdout.strip()
    pid_proc = subprocess.run(
        ["pgrep", "-f", "/usr/local/bin/glidefs run"], capture_output=True, text=True,
    )
    pid = int(pid_proc.stdout.split()[0]) if pid_proc.stdout.strip() else None

    threads = rss_kb = vsize_kb = fd_count = None
    ublk_n = tokio_n = iou_n = None
    if pid is not None:
        status = _read_proc_status(pid)
        threads = int(status.get("Threads", "0")) or None
        if "VmRSS" in status:
            rss_kb = int(status["VmRSS"].split()[0])
        if "VmSize" in status:
            vsize_kb = int(status["VmSize"].split()[0])
        try:
            fd_count = len(os.listdir(f"/proc/{pid}/fd"))
        except OSError:
            pass
        ublk_n = _kthread_count(pid, "ublk-")
        tokio_n = _kthread_count(pid, "tokio-")
        iou_n = _kthread_count(pid, "iou-")

    num_cpus = os.cpu_count() or 1
    num_numa_nodes = 1
    try:
        with open("/sys/devices/system/node/online") as f:
            spec = f.read().strip()
            # e.g. "0" or "0-1"
            if "-" in spec:
                lo, hi = spec.split("-")
                num_numa_nodes = int(hi) - int(lo) + 1
            else:
                num_numa_nodes = len([s for s in spec.split(",") if s])
    except OSError:
        pass

    return SysInfo(
        kernel=kernel, pid=pid, threads=threads, rss_kb=rss_kb, vsize_kb=vsize_kb,
        fd_count=fd_count, ublk_thread_count=ublk_n, tokio_thread_count=tokio_n,
        iou_kthread_count=iou_n, num_cpus=num_cpus, num_numa_nodes=num_numa_nodes,
    )


# ---------------------------------------------------------------------------
# Baseline scenario suite
# ---------------------------------------------------------------------------


def run_baseline(api: str, transport: str, prefix: str, runtime: int) -> dict:
    """The standard regression matrix. Run after every milestone."""
    sysbefore = sysinfo()

    # Need at least 8 devices for the parallel test.
    n_devices = 8
    exports = setup_n(api, n_devices, prefix, transport, SIZE_GB_DEFAULT)
    devs = [e["device"] for e in exports]
    print(f"# created {len(devs)} {transport} exports: {devs}", file=sys.stderr)

    try:
        single_dev = devs[0]
        results: dict[str, Any] = {
            "transport": transport,
            "kernel": sysbefore.kernel,
            "num_cpus": sysbefore.num_cpus,
            "sysinfo_before": asdict(sysbefore),
            "scenarios": {},
        }

        # Scenario 1: single-device depth curve, randread.
        depth_curve_read: list[FioResult] = []
        for depth in [1, 4, 16, 64, 128]:
            print(f"# single randread d={depth}", file=sys.stderr)
            r = fio_run(single_dev, rw="randread", bs=BS_DEFAULT, depth=depth, jobs=1, runtime=runtime)
            depth_curve_read.append(r)
        results["scenarios"]["single_depth_curve_randread"] = [asdict(r) for r in depth_curve_read]

        # Scenario 2: single-device depth curve, randwrite.
        depth_curve_write: list[FioResult] = []
        for depth in [1, 4, 16, 64, 128]:
            print(f"# single randwrite d={depth}", file=sys.stderr)
            r = fio_run(single_dev, rw="randwrite", bs=BS_DEFAULT, depth=depth, jobs=1, runtime=runtime)
            depth_curve_write.append(r)
        results["scenarios"]["single_depth_curve_randwrite"] = [asdict(r) for r in depth_curve_write]

        # Scenario 3: single-device multi-job at depth=32.
        multijob: list[FioResult] = []
        for jobs in [1, 2, 4, 8]:
            print(f"# single randwrite d=32 jobs={jobs}", file=sys.stderr)
            r = fio_run(single_dev, rw="randwrite", bs=BS_DEFAULT, depth=32, jobs=jobs, runtime=runtime)
            multijob.append(r)
        results["scenarios"]["single_multijob_randwrite_d32"] = [asdict(r) for r in multijob]

        # Scenario 4: FUA writes (the M1 target).
        fua: list[FioResult] = []
        for depth in [1, 16, 64]:
            print(f"# single randwrite+fsync d={depth}", file=sys.stderr)
            r = fio_run(single_dev, rw="randwrite", bs=BS_DEFAULT, depth=depth, jobs=1, runtime=runtime,
                        extra_args=["--fsync=1"])
            fua.append(r)
        results["scenarios"]["single_fua_randwrite"] = [asdict(r) for r in fua]

        # Scenario 5: 8-device parallel randwrite at depth=32.
        print(f"# parallel randwrite d=32 jobs=1 across {n_devices} devices", file=sys.stderr)
        agg = fio_parallel(devs, rw="randwrite", bs=BS_DEFAULT, depth=32, jobs_per_device=1, runtime=runtime)
        results["scenarios"]["parallel_randwrite_d32_x8"] = {
            "aggregate_iops": agg.aggregate_iops,
            "aggregate_bw_bytes_per_sec": agg.aggregate_bw_bytes_per_sec,
            "lat_mean_us_avg": agg.lat_mean_us_avg,
            "per_device": [asdict(r) for r in agg.per_device],
        }

        results["sysinfo_after"] = asdict(sysinfo())
        return results
    finally:
        n = teardown_prefix(api, prefix)
        print(f"# tore down {n} exports", file=sys.stderr)


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------


def _fmt_iops(v: float) -> str:
    if v >= 1e6:
        return f"{v/1e6:.2f}M"
    if v >= 1e3:
        return f"{v/1e3:.0f}k"
    return f"{v:.0f}"


def _fmt_bw(v: float) -> str:
    return f"{v/1e6:.0f}MB/s"


def render_human(results: dict) -> str:
    lines: list[str] = []
    lines.append(f"transport={results['transport']} kernel={results['kernel']} cpus={results['num_cpus']}")
    sb = results["sysinfo_before"]; sa = results["sysinfo_after"]
    lines.append(f"daemon: pid={sb.get('pid')} threads {sb.get('threads')} → {sa.get('threads')}, "
                 f"RSS {sb.get('rss_kb')}kB → {sa.get('rss_kb')}kB, "
                 f"VSZ {sb.get('vsize_kb')}kB → {sa.get('vsize_kb')}kB")
    lines.append("")
    for scenario_name, scenario in results["scenarios"].items():
        lines.append(f"=== {scenario_name} ===")
        if isinstance(scenario, list):
            for r in scenario:
                lines.append(f"  {r['rw']:>10s} bs={r['bs']} d={r['depth']:>3d} j={r['jobs']:>2d} "
                             f"| iops={_fmt_iops(r['iops']):>8s}  bw={_fmt_bw(r['bw_bytes_per_sec']):>10s}  "
                             f"lat_mean={r['lat_mean_us']:>7.1f}us  p99={r['lat_p99_us']:>7.1f}us")
        else:
            lines.append(f"  aggregate iops={_fmt_iops(scenario['aggregate_iops']):>8s}  "
                         f"bw={_fmt_bw(scenario['aggregate_bw_bytes_per_sec']):>10s}  "
                         f"lat_mean_avg={scenario['lat_mean_us_avg']:>7.1f}us")
            for r in scenario["per_device"]:
                lines.append(f"    {r['device']:>15s}: iops={_fmt_iops(r['iops']):>8s} "
                             f"lat_mean={r['lat_mean_us']:>7.1f}us")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _output(results: Any, fmt: str, path: Optional[str]) -> None:
    if fmt == "human" and isinstance(results, dict) and "scenarios" in results:
        text = render_human(results)
    else:
        text = json.dumps(results, indent=2, default=str)
    if path:
        Path(path).write_text(text)
        print(f"# wrote {path}", file=sys.stderr)
    else:
        print(text)


def main() -> int:
    if not shutil.which("fio"):
        print("error: fio not found in PATH", file=sys.stderr)
        return 2

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api", default=API_DEFAULT)
    p.add_argument("--format", choices=["json", "human"], default="human")
    p.add_argument("--output", "-o", default=None, help="write to file instead of stdout")

    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("baseline", help="run the regression suite")
    sp.add_argument("--transport", choices=["nbd", "ublk"], default="ublk")
    sp.add_argument("--prefix", default=PREFIX_DEFAULT)
    sp.add_argument("--runtime", type=int, default=RUNTIME_DEFAULT)

    sp = sub.add_parser("single", help="one device, one fio")
    sp.add_argument("--device", help="explicit device path (skips setup/teardown)")
    sp.add_argument("--transport", choices=["nbd", "ublk"], default="ublk")
    sp.add_argument("--prefix", default=PREFIX_DEFAULT)
    sp.add_argument("--rw", default="randread")
    sp.add_argument("--bs", default=BS_DEFAULT)
    sp.add_argument("--depth", type=int, default=64)
    sp.add_argument("--jobs", type=int, default=1)
    sp.add_argument("--runtime", type=int, default=RUNTIME_DEFAULT)
    sp.add_argument("--fsync", action="store_true", help="add --fsync=1 (FUA-style)")

    sp = sub.add_parser("parallel", help="N devices, parallel fio")
    sp.add_argument("--devices", type=int, default=8)
    sp.add_argument("--transport", choices=["nbd", "ublk"], default="ublk")
    sp.add_argument("--prefix", default=PREFIX_DEFAULT)
    sp.add_argument("--rw", default="randwrite")
    sp.add_argument("--bs", default=BS_DEFAULT)
    sp.add_argument("--depth", type=int, default=32)
    sp.add_argument("--jobs", type=int, default=1)
    sp.add_argument("--runtime", type=int, default=RUNTIME_DEFAULT)

    sp = sub.add_parser("setup", help="create N exports of given transport")
    sp.add_argument("--count", type=int, required=True)
    sp.add_argument("--transport", choices=["nbd", "ublk"], default="ublk")
    sp.add_argument("--prefix", default=PREFIX_DEFAULT)
    sp.add_argument("--size-gb", type=int, default=SIZE_GB_DEFAULT)

    sp = sub.add_parser("teardown", help="delete all exports matching prefix")
    sp.add_argument("--prefix", default=PREFIX_DEFAULT)

    sub.add_parser("list", help="list all exports")
    sub.add_parser("sysinfo", help="snapshot of glidefs daemon")

    args = p.parse_args()

    if args.cmd == "baseline":
        out = run_baseline(args.api, args.transport, args.prefix, args.runtime)
        _output(out, args.format, args.output)
        return 0

    if args.cmd == "single":
        if args.device:
            dev = args.device
            cleanup_name = None
        else:
            name = f"{args.prefix}single"
            e = create_export(args.api, name, args.transport, SIZE_GB_DEFAULT)
            dev = e["device"]
            cleanup_name = name
        try:
            extra = ["--fsync=1"] if args.fsync else None
            r = fio_run(dev, rw=args.rw, bs=args.bs, depth=args.depth, jobs=args.jobs,
                        runtime=args.runtime, extra_args=extra)
            _output(asdict(r), args.format, args.output)
        finally:
            if cleanup_name:
                delete_export(args.api, cleanup_name)
        return 0

    if args.cmd == "parallel":
        exports = setup_n(args.api, args.devices, args.prefix, args.transport, SIZE_GB_DEFAULT)
        devs = [e["device"] for e in exports]
        try:
            agg = fio_parallel(devs, rw=args.rw, bs=args.bs, depth=args.depth,
                               jobs_per_device=args.jobs, runtime=args.runtime)
            out = {
                "aggregate_iops": agg.aggregate_iops,
                "aggregate_bw_bytes_per_sec": agg.aggregate_bw_bytes_per_sec,
                "lat_mean_us_avg": agg.lat_mean_us_avg,
                "per_device": [asdict(r) for r in agg.per_device],
            }
            _output(out, args.format, args.output)
        finally:
            teardown_prefix(args.api, args.prefix)
        return 0

    if args.cmd == "setup":
        out = setup_n(args.api, args.count, args.prefix, args.transport, args.size_gb)
        _output({"created": out}, args.format, args.output)
        return 0

    if args.cmd == "teardown":
        n = teardown_prefix(args.api, args.prefix)
        _output({"deleted": n}, args.format, args.output)
        return 0

    if args.cmd == "list":
        _output({"exports": list_exports(args.api)}, args.format, args.output)
        return 0

    if args.cmd == "sysinfo":
        _output(asdict(sysinfo()), args.format, args.output)
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
