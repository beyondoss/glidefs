//! Admin tool: forcibly delete every ublk kernel device whose name
//! is "glidefs". For unsticking after a daemon restart-loop accumulates
//! orphans (each failed startup attempt allocates fresh `/dev/ublkbN`
//! entries; without the daemon to reattach, they stay in QUIESCED).
//!
//! Usage:
//!   sudo ublk_cleanup [--dry-run] [--filter NAME]
//!
//! Default filter is "glidefs"; pass `--filter ""` to clean ALL ublk
//! devices regardless of owner.
//!
//! Sends `UBLK_CMD_STOP_DEV` + `UBLK_CMD_DEL_DEV` per device. Safe iff
//! no userspace process is currently driving the device's queues —
//! e.g. when the daemon is dead. Calling this while a live daemon
//! exists will yank devices out from under it.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let dry_run = args.iter().any(|a| a == "--dry-run");
    let filter = args
        .iter()
        .position(|a| a == "--filter")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("glidefs")
        .to_string();

    eprintln!(
        "ublk_cleanup: filter='{}' dry_run={}",
        filter, dry_run
    );

    // Snapshot all ublk dev_ids the kernel currently exposes.
    let ids = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
    {
        let collected = std::sync::Arc::clone(&ids);
        ublk_core::ctrl::UblkCtrl::for_each_dev_id(move |dev_id| {
            collected.lock().unwrap().push(dev_id);
        });
    }
    let ids = std::sync::Arc::try_unwrap(ids).unwrap().into_inner().unwrap();

    eprintln!("found {} ublk device(s) total", ids.len());

    let mut killed = 0usize;
    let mut skipped_filter = 0usize;
    let mut errored = 0usize;

    for dev_id in ids {
        let dev_id_i32 = dev_id as i32;
        let ctrl = match ublk_core::ctrl::UblkCtrl::new_simple(dev_id_i32) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("dev_id={dev_id} new_simple failed: {e:?}");
                errored += 1;
                continue;
            }
        };

        let name = ctrl.get_name();
        if !filter.is_empty() && name != filter {
            skipped_filter += 1;
            continue;
        }

        let state = ctrl.dev_info().state;
        if dry_run {
            println!("would kill dev_id={dev_id} name='{name}' state={state}");
            killed += 1;
            continue;
        }

        // kill_dev() = STOP_DEV (transitions LIVE/QUIESCED → STOPPED).
        // del_dev() = DEL_DEV (removes the cdev entirely).
        // We need both — kill alone leaves the cdev registered in
        // /sys/class/ublk-char/ and counted by for_each_dev_id, so the
        // next daemon scan still sees it.
        let stop_result = ctrl.kill_dev();
        let del_result = ctrl.del_dev();
        match (stop_result, del_result) {
            (_, Ok(_)) => {
                killed += 1;
                if killed.is_multiple_of(100) {
                    eprintln!("deleted {killed} so far...");
                }
            }
            (stop_err, del_err) => {
                eprintln!(
                    "dev_id={dev_id} stop={stop_err:?} del={del_err:?}"
                );
                errored += 1;
            }
        }
    }

    eprintln!(
        "ublk_cleanup: killed={} skipped(filter mismatch)={} errored={}",
        killed, skipped_filter, errored
    );

    if errored > 0 { ExitCode::from(1) } else { ExitCode::SUCCESS }
}
