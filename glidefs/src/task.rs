//! Task spawn helpers.
//!
//! `spawn_named` / `spawn_blocking_named` carry a debug name through to
//! `tokio-console` when built with `tokio_unstable`.
//!
//! `spawn_supervised` / `spawn_blocking_supervised` add panic isolation:
//! the spawned future is wrapped in `catch_unwind` so a panic logs +
//! increments the process-wide panic counter without escaping the task.
//! Callers receive `Err(Panicked)` on the JoinHandle if they choose to
//! await; fire-and-forget callers still get the log + counter side
//! effects unconditionally.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::FutureExt;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

/// Process-wide panic counter, bumped by:
/// - the global panic hook in `main::install_panic_hook` (catches every panic
///   including those in `std::thread`-spawned ublk worker threads),
/// - each `spawn_supervised` / `spawn_blocking_supervised` wrapper (so we
///   still record the panic when the global hook is not installed, e.g. tests).
///
/// Both call sites increment, so a single panic that travels through the hook
/// AND a supervised wrapper will count twice. Acceptable: this counter exists
/// for "is something panicking?" alerting, not exact accounting.
static PANIC_COUNT: AtomicU64 = AtomicU64::new(0);

/// Snapshot the process-wide panic counter.
pub fn panic_count() -> u64 {
    PANIC_COUNT.load(Ordering::Relaxed)
}

/// Bump the panic counter. Called from the global panic hook and the
/// supervised spawn wrappers.
pub fn record_panic() {
    PANIC_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Returned in place of `T` when a supervised task panics.
#[derive(Debug)]
pub struct Panicked {
    pub task: &'static str,
    pub message: String,
}

impl std::fmt::Display for Panicked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "task '{}' panicked: {}", self.task, self.message)
    }
}

impl std::error::Error for Panicked {}

/// Extract a printable message from a panic payload. Tries `&'static str`,
/// then `String`, then falls back to a placeholder for non-string panics.
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

pub fn spawn_named<T, F>(_name: &str, future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    {
        tokio::task::Builder::new()
            .name(_name)
            .spawn(future)
            .expect("failed to spawn task")
    }
    #[cfg(not(tokio_unstable))]
    {
        tokio::spawn(future)
    }
}

#[allow(dead_code)]
pub fn spawn_named_on<T, F>(_name: &str, future: F, handle: &Handle) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    {
        tokio::task::Builder::new()
            .name(_name)
            .spawn_on(future, handle)
            .expect("failed to spawn task")
    }
    #[cfg(not(tokio_unstable))]
    {
        handle.spawn(future)
    }
}

pub fn spawn_blocking_named<T, F>(_name: &str, f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    {
        tokio::task::Builder::new()
            .name(_name)
            .spawn_blocking(f)
            .expect("failed to spawn blocking task")
    }
    #[cfg(not(tokio_unstable))]
    {
        tokio::task::spawn_blocking(f)
    }
}

/// Spawn an async task with panic isolation.
///
/// The future is wrapped in `AssertUnwindSafe(...).catch_unwind()`. A panic
/// inside the future is caught, logged, counted, and surfaced as
/// `Err(Panicked)` on the returned `JoinHandle`. Logging + counter side
/// effects fire even if the caller drops the handle.
///
/// `AssertUnwindSafe` is required because most futures are not `UnwindSafe`.
/// This is intentional: we treat any state captured by a panicking future as
/// poisoned and rely on the supervisor (e.g. connection teardown) to bound
/// the blast radius. Callers that mutate shared state must arrange their own
/// teardown on the `Err` arm (e.g. fire a `CancellationToken`).
pub fn spawn_supervised<T, F>(name: &'static str, future: F) -> JoinHandle<Result<T, Panicked>>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    spawn_named(name, async move {
        match AssertUnwindSafe(future).catch_unwind().await {
            Ok(value) => Ok(value),
            Err(payload) => {
                record_panic();
                let message = panic_message(payload.as_ref());
                tracing::error!(
                    task = name,
                    panic.message = %message,
                    "supervised task panicked",
                );
                Err(Panicked {
                    task: name,
                    message,
                })
            }
        }
    })
}

/// Like `spawn_supervised` but for blocking work. Catches panics inside the
/// blocking closure and reports them via `Err(Panicked)`.
#[allow(dead_code)]
pub fn spawn_blocking_supervised<T, F>(
    name: &'static str,
    f: F,
) -> JoinHandle<Result<T, Panicked>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    spawn_blocking_named(name, move || {
        match std::panic::catch_unwind(AssertUnwindSafe(f)) {
            Ok(value) => Ok(value),
            Err(payload) => {
                record_panic();
                let message = panic_message(payload.as_ref());
                tracing::error!(
                    task = name,
                    panic.message = %message,
                    "supervised blocking task panicked",
                );
                Err(Panicked {
                    task: name,
                    message,
                })
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawn_supervised_returns_value() {
        let handle = spawn_supervised("test-ok", async { 42_i32 });
        let result = handle.await.expect("join failed");
        assert_eq!(result.expect("task panicked"), 42);
    }

    #[tokio::test]
    async fn spawn_supervised_catches_panic() {
        let before = panic_count();
        let handle = spawn_supervised("test-panic", async {
            panic!("boom");
            #[allow(unreachable_code)]
            ()
        });
        let result = handle.await.expect("join should succeed even on inner panic");
        let panicked = result.expect_err("expected Panicked");
        assert_eq!(panicked.task, "test-panic");
        assert!(
            panicked.message.contains("boom"),
            "message should include the panic payload, got: {}",
            panicked.message,
        );
        assert!(panic_count() > before, "panic counter should increment");
    }

    #[tokio::test]
    async fn spawn_supervised_string_panic() {
        let handle = spawn_supervised("test-string-panic", async {
            let s = String::from("dynamic panic");
            panic!("{}", s);
            #[allow(unreachable_code)]
            ()
        });
        let result = handle.await.unwrap();
        let p = result.unwrap_err();
        assert!(p.message.contains("dynamic panic"));
    }

    #[tokio::test]
    async fn spawn_blocking_supervised_returns_value() {
        let handle = spawn_blocking_supervised("blk-ok", || 7_u32);
        assert_eq!(handle.await.unwrap().unwrap(), 7);
    }

    #[tokio::test]
    async fn spawn_blocking_supervised_catches_panic() {
        let before = panic_count();
        let handle = spawn_blocking_supervised("blk-panic", || {
            panic!("blocking boom");
        });
        let result = handle.await.expect("join failed");
        let p = result.expect_err("expected Panicked");
        assert_eq!(p.task, "blk-panic");
        assert!(p.message.contains("blocking boom"));
        assert!(panic_count() > before);
    }
}
