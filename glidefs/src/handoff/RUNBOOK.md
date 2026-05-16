# Handoff failure-mode runbook

For oncall responders dealing with a glidefs handoff that didn't go cleanly. Each section names the symptom you'd see in logs/metrics and what to do.

## Symptom: `handoff failed` log line

The structured log line looks like:
```
ERROR glidefs::cli::server: handoff failed error=<some error>
```

This means the predecessor's handoff coordination errored before completing. The predecessor is still alive and SERVING. **No I/O impact.** Investigate the error string, fix root cause, retry handoff.

Common errors:
- `failed to bind handoff socket: permission denied` — `/run/glidefs/` not writable. Fix unit's `RuntimeDirectory=glidefs`.
- `accept failed` — successor process died before connecting. Check successor's stderr/stdout (inherited from parent in current setup).
- `timeout waiting for successor HELLO` — successor hung in startup. Check whether the binary path is correct and executable.

## Symptom: `handoff complete outcome=Aborted`

Predecessor explicitly aborted before any destructive action. **No I/O impact.** Reason in the log.

Possible reasons (`AbortReason` variants in `handoff::protocol`):
- `VersionMismatch` — predecessor and successor binaries disagree on protocol version. Indicates mixed-version deploy. Roll back the unmatched binary.
- `NoCommonStrategy` — neither side advertises a usable cutover strategy. Currently impossible (CRH always available); would indicate a build with PIOD-only and the other side unaware.
- `WarmingFailed` — successor's WARMING (foyer open, S3 prefetch, router build) errored. Detail string says why. Common cause: bad config file or unreachable S3.
- `ExportMismatch` — successor's loaded router doesn't include exports the predecessor lists. Config drift between binaries (one was deployed with an old config).
- `FreezeFailed` — predecessor failed to fsync a WAL during freeze. Usually disk-error territory — check `dmesg` and SSD health.

## Symptom: `handoff complete outcome=RevivedFromFailedHandoff`

The successor crashed AFTER the predecessor dropped its UblkServer. Predecessor's `revive_after_failed_handoff` reattached its own QUIESCED devices and continues SERVING. **Brief (≤5s) VM-visible stall, no errors, no data loss.**

Investigation:
- Check whether the successor logged anything before exit (it shares the predecessor's stdout/stderr when spawned via fork+exec).
- If successor was killed by OOM, check `dmesg | grep -i 'killed process'`.
- If the failure is reproducible, file an issue with the successor's last log lines + handoff stage in the predecessor's log (e.g., "after PREDS_DEAD, before ALIVE").

## Symptom: predecessor exited but no successor process

This is the dangerous case — the daemon is GONE. Kernel ublk devices are QUIESCED. VMs see I/O hang.

Recovery: `systemctl restart glidefs`. The new daemon's cold-start `recover_quiesced_devices()` reattaches everything within seconds.

Root cause investigation:
- Check `journalctl -u glidefs` for the predecessor's last 100 lines. Look for `Shutdown complete` (clean exit) vs panic.
- If the spawned successor process is gone too, check `dmesg` for OOM, segfault, or kernel-killed indications.

## Symptom: `WAL is locked by another process` on cold start

Lockfile content: a PID. The PID listed is the process holding the WAL.

If the PID is alive (`ps -p $PID`):
- Another glidefs daemon is running. **Don't start a second one.** Either it's an unkilled successor from a botched handoff (kill it cleanly with SIGTERM) or it's an honest concurrent invocation (operator error).

If the PID is dead but the lockfile says alive:
- This shouldn't happen — `Wal::open` checks `/proc/<pid>` existence and reclaims stale locks automatically. If you see this, it's a bug — file an issue with the lockfile content + proof the PID is dead.

To clear manually (only do this if you're sure no daemon is running):
```bash
rm /var/cache/glidefs/exports/<name>/<name>.wal.lock
```

## Symptom: orphan ublk devices (`/dev/ublkbN` exists but no daemon)

After a particularly bad failure, you might end up with kernel ublk devices that no daemon is driving. They appear in `ls /dev/ublkb*` but `cat /proc/<glidefs_pid>/fd/...` shows no daemon has them open.

Recovery options:

1. **Best**: restart the daemon. `recover_quiesced_devices()` finds and reattaches them.
2. **Cleanup**: `sudo glidefs ublk_cleanup` (filters to `name="glidefs"` by default — safe with running daemons since it can't acquire control of devices another process holds).
3. **Manual** (last resort): `sudo glidefs ublk_cleanup --filter ""` deletes EVERY ublk device on the host regardless of name, including any in active use by another daemon. **DO NOT USE if any glidefs is running** — you will rip devices out from under it.

## Symptom: VM I/O EBUSY errors during handoff

`BlockHandler::freeze()` was wired to return EBUSY on writes during the handoff freeze window. This was intentionally **removed** in the current build because EBUSY exposes the handoff to guests. The `frozen` AtomicBool is now metadata-only (used for the `is_frozen()` check, not for gating writes).

If you see EBUSY during a handoff today, it's either from:
- A different code path (not handoff) returning EBUSY.
- An older build that still had the gate.

Check the `glidefs --version` of the running daemon.

## Symptom: post-handoff fio verify failure

The handoff itself completed OK (`handoff complete outcome=Succeeded`), but workload-level verification (fio --verify=crc32c, application-level checksums) reports mismatches.

This is **data corruption** and should be treated as critical.

Investigation steps:
1. Check both predecessor's and successor's logs for `tail-replayed WAL entries` lines around the handoff. Successor MUST have run `replay_wal_tail` for every export.
2. Confirm the successor's `freeze_in_progress` flag was true during WARMING (the `set_all_caches_freeze(true)` call in `run_server_as_successor`). If false, the successor's flush_scheduler may have rotated the data file out from under the predecessor, breaking the cross-process file-handle sharing.
3. Confirm the predecessor's `freeze_in_progress` flag was true during the freeze step (set by `freeze_all`). If not, predecessor's checkpoint may have truncated the WAL the successor needed to replay.

If all three are confirmed correct in logs and you still see corruption: file a critical bug. Include the failing fio offsets, both daemons' full logs, and the WAL contents (`hexdump -C cache/<export>.wal`).

## Symptom: handoff stalls applications for >5s

Acceptable target: sub-50ms p99 stall, 200ms p99.9. Anything past 5s is a regression.

Check `glidefs_handoff_stall_duration_seconds` histogram (when task 5.1 ships) for the breakdown. Common causes:
- Foyer cache initialization is slow on the successor (huge SSD, first open after format)
- S3 chunk prefetch is timing out
- WAL replay is enormous (millions of entries) — usually only an issue if the predecessor's flush_scheduler was deadlocked

Mitigation: increase `HandoffTimeouts` budget or investigate the slow stage.
