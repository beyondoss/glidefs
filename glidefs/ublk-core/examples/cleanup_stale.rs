// Parallel janitor: enumerate /sys/class/ublk-char/ublkc<N>, del_dev each.
// 32 worker threads to avoid serialization on devices stuck under udev probe.

use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn main() {
    let mut ids: Vec<i32> = fs::read_dir("/sys/class/ublk-char")
        .expect("ublk module loaded?")
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            name.strip_prefix("ublkc")?.parse::<i32>().ok()
        })
        .collect();
    ids.sort_unstable();
    eprintln!("found {} stale ublk devices", ids.len());

    let queue = Arc::new(Mutex::new(ids));
    let ok = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicUsize::new(0));
    let start = std::time::Instant::now();

    let mut workers = Vec::new();
    for _ in 0..32 {
        let q = Arc::clone(&queue);
        let ok = Arc::clone(&ok);
        let fail = Arc::clone(&fail);
        workers.push(std::thread::spawn(move || loop {
            let id = match q.lock().unwrap().pop() {
                Some(id) => id,
                None => return,
            };
            match ublk_core::ctrl::UblkCtrl::new_simple(id) {
                Ok(c) => match c.del_dev() {
                    Ok(_) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        fail.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(_) => {
                    fail.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Reporter
    let ok_r = Arc::clone(&ok);
    let fail_r = Arc::clone(&fail);
    let q_r = Arc::clone(&queue);
    let reporter = std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(5));
        let remaining = q_r.lock().unwrap().len();
        let o = ok_r.load(Ordering::Relaxed);
        let f = fail_r.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();
        let rate = (o + f) as f64 / elapsed.max(0.001);
        eprintln!(
            "[{:>4.0}s] ok={o} fail={f} queue={remaining} rate={rate:.1}/s",
            elapsed
        );
        if remaining == 0 {
            break;
        }
    });

    for w in workers {
        w.join().ok();
    }
    reporter.join().ok();
    eprintln!(
        "done: ok={} fail={} in {:.1}s",
        ok.load(Ordering::Relaxed),
        fail.load(Ordering::Relaxed),
        start.elapsed().as_secs_f64()
    );
}
