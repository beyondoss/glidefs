//! Conditional sync primitives for loom testing.
//!
//! When compiled with `--cfg loom`, uses loom's instrumented atomics
//! that enable exhaustive interleaving exploration. Otherwise uses
//! std atomics for production.
//!
//! Usage in write_cache.rs:
//! ```ignore
//! #[cfg(loom)]
//! use crate::block::sync::{AtomicU8, AtomicU64, Ordering};
//! #[cfg(not(loom))]
//! use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
//! ```

// Re-exports are used when loom is enabled; allow unused when it's not
#![allow(unused_imports)]

#[cfg(loom)]
pub use loom::sync::atomic::{AtomicU8, AtomicU64, Ordering};

#[cfg(not(loom))]
pub use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

#[cfg(loom)]
pub use loom::thread;

#[cfg(not(loom))]
pub use std::thread;

/// Mock queue for loom testing.
///
/// Crossbeam's SegQueue doesn't support loom, so we use a Mutex<VecDeque>
/// as a stand-in during loom tests. This is acceptable because:
/// - Queue correctness is orthogonal to state machine correctness
/// - The critical bugs we're looking for are in CAS state transitions
/// - Crossbeam is well-tested; our atomics are what need verification
#[cfg(loom)]
pub mod queue {
    use loom::sync::Mutex;
    use std::collections::VecDeque;

    #[derive(Debug)]
    pub struct MockSegQueue<T> {
        inner: Mutex<VecDeque<T>>,
    }

    impl<T> MockSegQueue<T> {
        pub fn new() -> Self {
            Self {
                inner: Mutex::new(VecDeque::new()),
            }
        }

        pub fn push(&self, value: T) {
            self.inner.lock().unwrap().push_back(value);
        }

        pub fn pop(&self) -> Option<T> {
            self.inner.lock().unwrap().pop_front()
        }

        pub fn is_empty(&self) -> bool {
            self.inner.lock().unwrap().is_empty()
        }

        pub fn len(&self) -> usize {
            self.inner.lock().unwrap().len()
        }
    }

    impl<T> Default for MockSegQueue<T> {
        fn default() -> Self {
            Self::new()
        }
    }
}
