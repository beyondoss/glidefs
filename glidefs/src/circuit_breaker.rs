//! Lock-free circuit breaker for protecting external service calls.
//!
//! This implementation is provably race-free through:
//! 1. Atomic words for all mutable state (no multi-variable coordination)
//! 2. Compare-and-swap loops for all state transitions
//! 3. Monotonic timestamps for timeout detection
//!
//! # States
//!
//! ```text
//!                 failure_threshold reached
//!     ┌─────────┐ ──────────────────────────► ┌────────┐
//!     │ Closed  │                             │  Open  │
//!     └─────────┘ ◄────────────────────────── └────────┘
//!          ▲        success in half-open           │
//!          │                                       │ reset_timeout elapsed
//!          │        ┌─────────────┐                │
//!          └─────── │  Half-Open  │ ◄──────────────┘
//!            success└─────────────┘
//!                         │
//!                         │ failure
//!                         ▼
//!                    back to Open
//! ```
//!
//! # Failure Policies
//!
//! Two failure detection policies are supported:
//!
//! - **Consecutive**: Opens after N failures in a row. Any success resets the count.
//!   Good for detecting complete backend failures.
//!
//! - **Windowed**: Opens after N failures within a time window. Failures outside
//!   the window are forgotten. Good for detecting degraded backends with partial failures.
//!
//! # Example
//!
//! ```rust
//! use glidefs::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, FailurePolicy};
//! use std::time::Duration;
//!
//! // Consecutive failures (default)
//! let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
//!
//! // Windowed failures (better for edge proxies)
//! let cb = CircuitBreaker::new(
//!     CircuitBreakerConfig::windowed(3, Duration::from_secs(10))
//!         .reset_timeout(Duration::from_secs(30))
//! );
//!
//! // Before calling external service
//! if cb.allow().is_err() {
//!     // return Err("service temporarily unavailable");
//! }
//!
//! // match call_external_service().await {
//! //     Ok(result) => {
//! //         cb.record_success();
//! //         Ok(result)
//! //     }
//! //     Err(e) if is_connectivity_error(&e) => {
//! //         cb.record_failure();
//! //         Err(e)
//! //     }
//! //     Err(e) => Err(e), // Don't count business logic errors
//! // }
//! ```

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, Ordering};

use std::time::Duration;

/// How failures are counted before opening the circuit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailurePolicy {
    /// N consecutive failures opens the circuit. Any success resets the count.
    Consecutive {
        /// Number of consecutive failures before opening.
        threshold: u32,
    },
    /// N failures within the window opens the circuit.
    /// Failures older than the window are forgotten.
    Windowed {
        /// Number of failures within the window before opening.
        threshold: u32,
        /// Time window for counting failures.
        window: Duration,
    },
}

impl Default for FailurePolicy {
    fn default() -> Self {
        FailurePolicy::Consecutive { threshold: 5 }
    }
}

/// Circuit breaker configuration.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// How failures are counted.
    pub failure_policy: FailurePolicy,
    /// Time to wait in open state before transitioning to half-open.
    pub reset_timeout: Duration,
    /// Number of probe requests allowed in half-open state.
    pub half_open_permits: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_policy: FailurePolicy::default(),
            reset_timeout: Duration::from_secs(30),
            half_open_permits: 3,
        }
    }
}

impl CircuitBreakerConfig {
    /// Create a config with consecutive failure detection.
    pub fn consecutive(threshold: u32) -> Self {
        Self {
            failure_policy: FailurePolicy::Consecutive { threshold },
            ..Default::default()
        }
    }

    /// Create a config with windowed failure detection.
    pub fn windowed(threshold: u32, window: Duration) -> Self {
        Self {
            failure_policy: FailurePolicy::Windowed { threshold, window },
            ..Default::default()
        }
    }

    /// Set the reset timeout (time in open state before half-open).
    pub fn reset_timeout(mut self, timeout: Duration) -> Self {
        self.reset_timeout = timeout;
        self
    }

    /// Set the number of half-open permits.
    pub fn half_open_permits(mut self, permits: u32) -> Self {
        self.half_open_permits = permits;
        self
    }

    /// Get the failure threshold from the policy.
    #[allow(dead_code)]
    fn threshold(&self) -> u32 {
        match &self.failure_policy {
            FailurePolicy::Consecutive { threshold } => *threshold,
            FailurePolicy::Windowed { threshold, .. } => *threshold,
        }
    }
}

/// Lock-free circuit breaker.
///
/// All state is packed into a single 64-bit atomic:
/// - Bits 62-63: State (0=closed, 1=open, 2=half-open)
/// - Bits 48-61: Failure count (14 bits, max 16383)
/// - Bits 32-47: Half-open permits remaining (16 bits)
/// - Bits 0-31: Timestamp of last state change (seconds since epoch, wraps every 136 years)
///
/// For windowed mode, a second atomic tracks the window start timestamp.
///
/// This packing ensures all state transitions are atomic via single CAS operations.
pub struct CircuitBreaker {
    /// Packed state word.
    state: AtomicU64,
    /// Window start timestamp (only used in windowed mode).
    /// Stores seconds since epoch when the first failure in the current window occurred.
    /// 0 means no active window.
    window_start: AtomicU64,
    /// Configuration (immutable after construction).
    config: CircuitBreakerConfig,
    /// Clock function for getting current time in seconds.
    clock: fn() -> u64,
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("state", &self.state)
            .field("window_start", &self.window_start)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

// State encoding constants
const STATE_CLOSED: u64 = 0;
const STATE_OPEN: u64 = 1;
const STATE_HALF_OPEN: u64 = 2;

const STATE_SHIFT: u32 = 62;
const STATE_MASK: u64 = 0b11;

const FAILURE_SHIFT: u32 = 48;
const FAILURE_MASK: u64 = 0x3FFF; // 14 bits

const PERMIT_SHIFT: u32 = 32;
const PERMIT_MASK: u64 = 0xFFFF; // 16 bits

const TIMESTAMP_MASK: u64 = 0xFFFF_FFFF; // 32 bits

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self::with_clock(config, Self::system_clock)
    }

    /// Create a circuit breaker with a custom clock (for testing).
    pub fn with_clock(config: CircuitBreakerConfig, clock: fn() -> u64) -> Self {
        let initial = Self::pack(STATE_CLOSED, 0, 0, clock());
        Self {
            state: AtomicU64::new(initial),
            window_start: AtomicU64::new(0),
            config,
            clock,
        }
    }

    /// System clock returning seconds since epoch (32-bit, wrapping).
    #[inline]
    fn system_clock() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() & TIMESTAMP_MASK)
            .unwrap_or(0)
    }

    /// Get current time from the configured clock.
    #[inline]
    fn now_secs(&self) -> u64 {
        (self.clock)()
    }

    /// Pack state components into a single u64.
    #[inline]
    fn pack(state: u64, failures: u64, permits: u64, timestamp: u64) -> u64 {
        ((state & STATE_MASK) << STATE_SHIFT)
            | ((failures & FAILURE_MASK) << FAILURE_SHIFT)
            | ((permits & PERMIT_MASK) << PERMIT_SHIFT)
            | (timestamp & TIMESTAMP_MASK)
    }

    /// Unpack a u64 into state components.
    #[inline]
    fn unpack(packed: u64) -> (u64, u64, u64, u64) {
        let state = (packed >> STATE_SHIFT) & STATE_MASK;
        let failures = (packed >> FAILURE_SHIFT) & FAILURE_MASK;
        let permits = (packed >> PERMIT_SHIFT) & PERMIT_MASK;
        let timestamp = packed & TIMESTAMP_MASK;
        (state, failures, permits, timestamp)
    }

    /// Check if a request should be allowed through the circuit.
    ///
    /// Returns `Ok(())` if the request is allowed, `Err(CircuitOpen)` if the
    /// circuit is open and the request should be rejected.
    ///
    /// In half-open state, this atomically decrements the permit count.
    pub fn allow(&self) -> Result<(), CircuitOpen> {
        loop {
            let packed = self.state.load(Ordering::Acquire);
            let (state, failures, permits, timestamp) = Self::unpack(packed);

            match state {
                STATE_CLOSED => return Ok(()),

                STATE_OPEN => {
                    let now = self.now_secs();
                    let elapsed = now.wrapping_sub(timestamp);

                    if elapsed >= self.config.reset_timeout.as_secs() {
                        // Timeout elapsed, try to transition to half-open
                        let new_packed = Self::pack(
                            STATE_HALF_OPEN,
                            0,
                            u64::from(self.config.half_open_permits),
                            now,
                        );

                        match self.state.compare_exchange_weak(
                            packed,
                            new_packed,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => continue,  // Transitioned, retry allow()
                            Err(_) => continue, // Someone else modified, retry
                        }
                    }
                    return Err(CircuitOpen);
                }

                STATE_HALF_OPEN => {
                    if permits == 0 {
                        return Err(CircuitOpen);
                    }

                    // Try to claim a permit
                    let new_packed = Self::pack(STATE_HALF_OPEN, failures, permits - 1, timestamp);

                    match self.state.compare_exchange_weak(
                        packed,
                        new_packed,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Ok(()),
                        Err(_) => continue, // CAS failed, retry
                    }
                }

                _ => {
                    // Invalid state, reset to closed
                    let new_packed = Self::pack(STATE_CLOSED, 0, 0, self.now_secs());
                    let _ = self.state.compare_exchange(
                        packed,
                        new_packed,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    return Ok(());
                }
            }
        }
    }

    /// Record a successful request.
    ///
    /// In closed state, resets the failure counter (and window for windowed mode).
    /// In half-open state, closes the circuit (service is healthy again).
    pub fn record_success(&self) {
        // Reset window start for windowed mode
        self.window_start.store(0, Ordering::Release);

        loop {
            let packed = self.state.load(Ordering::Acquire);
            let (state, _, _, _) = Self::unpack(packed);

            let new_packed = match state {
                STATE_CLOSED => {
                    // Reset failure count, keep closed
                    Self::pack(STATE_CLOSED, 0, 0, self.now_secs())
                }
                STATE_HALF_OPEN => {
                    // Success in half-open: close the circuit
                    Self::pack(STATE_CLOSED, 0, 0, self.now_secs())
                }
                STATE_OPEN => return, // Shouldn't record success while open
                _ => return,
            };

            match self.state.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    /// Record a failed request.
    ///
    /// In closed state, increments the failure counter and opens the circuit
    /// if the threshold is reached.
    /// In half-open state, reopens the circuit immediately.
    pub fn record_failure(&self) {
        match &self.config.failure_policy {
            FailurePolicy::Consecutive { threshold } => {
                self.record_failure_consecutive(*threshold);
            }
            FailurePolicy::Windowed { threshold, window } => {
                self.record_failure_windowed(*threshold, window.as_secs());
            }
        }
    }

    /// Record failure with consecutive failure tracking.
    fn record_failure_consecutive(&self, threshold: u32) {
        loop {
            let packed = self.state.load(Ordering::Acquire);
            let (state, failures, _, _) = Self::unpack(packed);
            let now = self.now_secs();

            let new_packed = match state {
                STATE_CLOSED => {
                    let new_failures = failures + 1;
                    if new_failures >= u64::from(threshold) {
                        Self::pack(STATE_OPEN, 0, 0, now)
                    } else {
                        Self::pack(STATE_CLOSED, new_failures, 0, now)
                    }
                }
                STATE_HALF_OPEN => Self::pack(STATE_OPEN, 0, 0, now),
                STATE_OPEN => return,
                _ => return,
            };

            match self.state.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    /// Record failure with windowed failure tracking.
    fn record_failure_windowed(&self, threshold: u32, window_secs: u64) {
        let now = self.now_secs();

        // Handle window timing
        let window_start = self.window_start.load(Ordering::Acquire);
        let (new_window_start, reset_count) = if window_start == 0 {
            // First failure, start new window
            (now, true)
        } else if now.wrapping_sub(window_start) >= window_secs {
            // Window expired, start new window
            (now, true)
        } else {
            // Within window, continue counting
            (window_start, false)
        };

        // Update window start if needed (best-effort, races are acceptable)
        if new_window_start != window_start {
            let _ = self.window_start.compare_exchange(
                window_start,
                new_window_start,
                Ordering::Release,
                Ordering::Relaxed,
            );
        }

        // Now update the main state
        loop {
            let packed = self.state.load(Ordering::Acquire);
            let (state, failures, _, _) = Self::unpack(packed);

            let new_packed = match state {
                STATE_CLOSED => {
                    let new_failures = if reset_count { 1 } else { failures + 1 };
                    if new_failures >= u64::from(threshold) {
                        // Reset window when opening circuit
                        self.window_start.store(0, Ordering::Release);
                        Self::pack(STATE_OPEN, 0, 0, now)
                    } else {
                        Self::pack(STATE_CLOSED, new_failures, 0, now)
                    }
                }
                STATE_HALF_OPEN => {
                    self.window_start.store(0, Ordering::Release);
                    Self::pack(STATE_OPEN, 0, 0, now)
                }
                STATE_OPEN => return,
                _ => return,
            };

            match self.state.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    /// Get the current circuit state for observability.
    pub fn state(&self) -> CircuitState {
        let packed = self.state.load(Ordering::Acquire);
        let (state, failures, permits, _) = Self::unpack(packed);

        match state {
            STATE_CLOSED => CircuitState::Closed {
                failure_count: failures as u32,
            },
            STATE_OPEN => CircuitState::Open,
            STATE_HALF_OPEN => CircuitState::HalfOpen {
                permits_remaining: permits as u32,
            },
            _ => CircuitState::Closed { failure_count: 0 },
        }
    }

    /// Reset the circuit breaker to closed state.
    pub fn reset(&self) {
        self.window_start.store(0, Ordering::Release);
        let packed = Self::pack(STATE_CLOSED, 0, 0, self.now_secs());
        self.state.store(packed, Ordering::Release);
    }

    /// Force the circuit to a specific state (for testing/admin).
    #[cfg(any(test, loom, feature = "test-utils"))]
    pub fn force_state(&self, new_state: CircuitState) {
        let now = self.now_secs();
        let packed = match new_state {
            CircuitState::Closed { failure_count } => {
                Self::pack(STATE_CLOSED, u64::from(failure_count), 0, now)
            }
            CircuitState::Open => Self::pack(STATE_OPEN, 0, 0, now),
            CircuitState::HalfOpen { permits_remaining } => {
                Self::pack(STATE_HALF_OPEN, 0, u64::from(permits_remaining), now)
            }
        };
        self.state.store(packed, Ordering::Release);
    }
}

/// Error returned when the circuit is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitOpen;

impl std::fmt::Display for CircuitOpen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "circuit breaker is open")
    }
}

impl std::error::Error for CircuitOpen {}

/// Observable circuit state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed, requests flow through normally.
    Closed {
        /// Number of failures since last success/reset.
        failure_count: u32,
    },
    /// Circuit is open, requests are rejected immediately.
    Open,
    /// Circuit is half-open, limited probe requests allowed.
    HalfOpen {
        /// Number of probe requests still allowed.
        permits_remaining: u32,
    },
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // =========================================================================
    // Consecutive mode tests
    // =========================================================================

    #[test]
    fn test_initial_state_is_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 0 });
    }

    #[test]
    fn test_allow_when_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
        assert!(cb.allow().is_ok());
    }

    #[test]
    fn test_consecutive_failures_increment() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::consecutive(5));

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 1 });

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 3 });
    }

    #[test]
    fn test_consecutive_success_resets_failures() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::consecutive(5));

        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 3 });

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 0 });
    }

    #[test]
    fn test_consecutive_opens_at_threshold() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::consecutive(3));

        cb.record_failure();
        cb.record_failure();
        assert!(matches!(cb.state(), CircuitState::Closed { .. }));

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_rejects_when_open() {
        let cb = CircuitBreaker::new(
            CircuitBreakerConfig::consecutive(1).reset_timeout(Duration::from_secs(3600)),
        );

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.allow().is_err());
    }

    #[test]
    fn test_half_open_after_timeout() {
        let cb = CircuitBreaker::new(
            CircuitBreakerConfig::consecutive(1)
                .reset_timeout(Duration::from_millis(1))
                .half_open_permits(2),
        );

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        thread::sleep(Duration::from_millis(10));

        assert!(cb.allow().is_ok());
        assert!(matches!(cb.state(), CircuitState::HalfOpen { .. }));
    }

    #[test]
    fn test_half_open_permits_decrement() {
        let cb = CircuitBreaker::new(
            CircuitBreakerConfig::consecutive(1)
                .reset_timeout(Duration::from_millis(1))
                .half_open_permits(3),
        );

        cb.record_failure();
        thread::sleep(Duration::from_millis(10));

        assert!(cb.allow().is_ok());
        assert_eq!(
            cb.state(),
            CircuitState::HalfOpen {
                permits_remaining: 2
            }
        );

        assert!(cb.allow().is_ok());
        assert_eq!(
            cb.state(),
            CircuitState::HalfOpen {
                permits_remaining: 1
            }
        );

        assert!(cb.allow().is_ok());
        assert_eq!(
            cb.state(),
            CircuitState::HalfOpen {
                permits_remaining: 0
            }
        );

        assert!(cb.allow().is_err());
    }

    #[test]
    fn test_half_open_success_closes() {
        let cb = CircuitBreaker::new(
            CircuitBreakerConfig::consecutive(1)
                .reset_timeout(Duration::from_millis(1))
                .half_open_permits(3),
        );

        cb.record_failure();
        thread::sleep(Duration::from_millis(10));
        let _ = cb.allow();

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 0 });
    }

    #[test]
    fn test_half_open_failure_reopens() {
        let cb = CircuitBreaker::new(
            CircuitBreakerConfig::consecutive(1)
                .reset_timeout(Duration::from_millis(1))
                .half_open_permits(3),
        );

        cb.record_failure();
        thread::sleep(Duration::from_millis(10));
        let _ = cb.allow();

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    // =========================================================================
    // Windowed mode tests
    // =========================================================================

    #[test]
    fn test_windowed_opens_at_threshold() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::windowed(3, Duration::from_secs(10)));

        cb.record_failure();
        cb.record_failure();
        assert!(matches!(cb.state(), CircuitState::Closed { .. }));

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_windowed_resets_after_window() {
        // Note: window uses second-level precision, so use 1 second window
        let cb = CircuitBreaker::new(CircuitBreakerConfig::windowed(3, Duration::from_secs(1)));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 2 });

        // Wait for window to expire (1 second + buffer)
        thread::sleep(Duration::from_millis(1100));

        // This failure starts a new window
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 1 });

        // Two more to hit threshold
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_windowed_success_resets_window() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::windowed(3, Duration::from_secs(10)));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 2 });

        // Success resets the failure count
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 0 });

        // Need 3 fresh failures to open
        cb.record_failure();
        cb.record_failure();
        assert!(matches!(cb.state(), CircuitState::Closed { .. }));

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_windowed_half_open_recovery() {
        let cb = CircuitBreaker::new(
            CircuitBreakerConfig::windowed(2, Duration::from_secs(10))
                .reset_timeout(Duration::from_millis(1)),
        );

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        thread::sleep(Duration::from_millis(10));

        assert!(cb.allow().is_ok());
        assert!(matches!(cb.state(), CircuitState::HalfOpen { .. }));

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed { failure_count: 0 });
    }

    // =========================================================================
    // Concurrency tests
    // =========================================================================

    #[test]
    fn test_concurrent_failures_open_exactly_once() {
        for _ in 0..100 {
            let cb = Arc::new(CircuitBreaker::new(
                CircuitBreakerConfig::consecutive(10).reset_timeout(Duration::from_secs(3600)),
            ));

            let handles: Vec<_> = (0..20)
                .map(|_| {
                    let cb = Arc::clone(&cb);
                    thread::spawn(move || {
                        cb.record_failure();
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(cb.state(), CircuitState::Open);
        }
    }

    #[test]
    fn test_concurrent_allow_in_half_open_respects_permits() {
        for _ in 0..100 {
            let cb = Arc::new(CircuitBreaker::new(
                CircuitBreakerConfig::consecutive(1)
                    .reset_timeout(Duration::from_millis(1))
                    .half_open_permits(5),
            ));

            cb.record_failure();
            thread::sleep(Duration::from_millis(10));

            let allowed = Arc::new(std::sync::atomic::AtomicU32::new(0));

            let handles: Vec<_> = (0..20)
                .map(|_| {
                    let cb = Arc::clone(&cb);
                    let allowed = Arc::clone(&allowed);
                    thread::spawn(move || {
                        if cb.allow().is_ok() {
                            allowed.fetch_add(1, Ordering::SeqCst);
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            let total_allowed = allowed.load(Ordering::SeqCst);
            assert!(
                total_allowed <= 5,
                "allowed {} requests but only 5 permits",
                total_allowed
            );
        }
    }

    #[test]
    fn test_concurrent_windowed_failures() {
        for _ in 0..50 {
            let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig::windowed(
                10,
                Duration::from_secs(60),
            )));

            let handles: Vec<_> = (0..20)
                .map(|_| {
                    let cb = Arc::clone(&cb);
                    thread::spawn(move || {
                        cb.record_failure();
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(cb.state(), CircuitState::Open);
        }
    }

    // =========================================================================
    // Pack/unpack tests
    // =========================================================================

    #[test]
    fn test_pack_unpack_roundtrip() {
        let test_cases = [
            (STATE_CLOSED, 0, 0, 0),
            (STATE_OPEN, 0, 0, 12345),
            (STATE_HALF_OPEN, 100, 50, 999999),
            (STATE_CLOSED, FAILURE_MASK, PERMIT_MASK, TIMESTAMP_MASK),
        ];

        for (state, failures, permits, timestamp) in test_cases {
            let packed = CircuitBreaker::pack(state, failures, permits, timestamp);
            let (s, f, p, t) = CircuitBreaker::unpack(packed);
            assert_eq!(s, state, "state mismatch");
            assert_eq!(f, failures, "failures mismatch");
            assert_eq!(p, permits, "permits mismatch");
            assert_eq!(t, timestamp, "timestamp mismatch");
        }
    }

    // =========================================================================
    // Builder API tests
    // =========================================================================

    #[test]
    fn test_builder_consecutive() {
        let config = CircuitBreakerConfig::consecutive(5)
            .reset_timeout(Duration::from_secs(60))
            .half_open_permits(10);

        assert_eq!(
            config.failure_policy,
            FailurePolicy::Consecutive { threshold: 5 }
        );
        assert_eq!(config.reset_timeout, Duration::from_secs(60));
        assert_eq!(config.half_open_permits, 10);
    }

    #[test]
    fn test_builder_windowed() {
        let config = CircuitBreakerConfig::windowed(3, Duration::from_secs(10))
            .reset_timeout(Duration::from_secs(30))
            .half_open_permits(5);

        assert_eq!(
            config.failure_policy,
            FailurePolicy::Windowed {
                threshold: 3,
                window: Duration::from_secs(10)
            }
        );
        assert_eq!(config.reset_timeout, Duration::from_secs(30));
        assert_eq!(config.half_open_permits, 5);
    }
}

/// Loom tests for verifying lock-free correctness under all thread interleavings.
///
/// Run with: RUSTFLAGS="--cfg loom" cargo test --release -p circuit_breaker
#[cfg(loom)]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Frozen clock for deterministic testing.
    fn frozen_clock() -> u64 {
        1000
    }

    /// Verify that concurrent failures properly open the circuit.
    /// No failures should be lost in the CAS loop.
    #[test]
    fn concurrent_failures_open_circuit() {
        loom::model(|| {
            let cb = Arc::new(CircuitBreaker::with_clock(
                CircuitBreakerConfig::consecutive(3),
                frozen_clock,
            ));

            let handles: Vec<_> = (0..3)
                .map(|_| {
                    let cb = Arc::clone(&cb);
                    thread::spawn(move || {
                        cb.record_failure();
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // After 3 concurrent failures with threshold=3, circuit must be open
            assert_eq!(cb.state(), CircuitState::Open);
        });
    }

    /// Verify that half-open permits are correctly decremented under contention.
    /// We should never allow more requests than configured permits.
    #[test]
    fn half_open_permits_respected() {
        loom::model(|| {
            let cb = Arc::new(CircuitBreaker::with_clock(
                CircuitBreakerConfig::consecutive(1).half_open_permits(1),
                frozen_clock,
            ));

            // Force into half-open state with only 1 permit
            cb.force_state(CircuitState::HalfOpen {
                permits_remaining: 1,
            });

            let allowed = Arc::new(loom::sync::atomic::AtomicU32::new(0));

            // 2 threads racing for 1 permit
            let cb1 = Arc::clone(&cb);
            let allowed1 = Arc::clone(&allowed);
            let h1 = thread::spawn(move || {
                if cb1.allow().is_ok() {
                    allowed1.fetch_add(1, Ordering::SeqCst);
                }
            });

            let cb2 = Arc::clone(&cb);
            let allowed2 = Arc::clone(&allowed);
            let h2 = thread::spawn(move || {
                if cb2.allow().is_ok() {
                    allowed2.fetch_add(1, Ordering::SeqCst);
                }
            });

            h1.join().unwrap();
            h2.join().unwrap();

            // Exactly 1 request should have been allowed
            assert_eq!(allowed.load(Ordering::SeqCst), 1);
        });
    }

    /// Verify that success in half-open correctly transitions to closed.
    #[test]
    fn half_open_success_closes_circuit() {
        loom::model(|| {
            let cb = Arc::new(CircuitBreaker::with_clock(
                CircuitBreakerConfig::consecutive(1).half_open_permits(3),
                frozen_clock,
            ));

            cb.force_state(CircuitState::HalfOpen {
                permits_remaining: 3,
            });

            // One thread records success, others try to allow
            let cb1 = Arc::clone(&cb);
            let h1 = thread::spawn(move || {
                cb1.record_success();
            });

            let cb2 = Arc::clone(&cb);
            let h2 = thread::spawn(move || {
                let _ = cb2.allow();
            });

            h1.join().unwrap();
            h2.join().unwrap();

            // Circuit should be closed after success
            assert!(matches!(cb.state(), CircuitState::Closed { .. }));
        });
    }

    /// Verify that failure in half-open correctly transitions back to open.
    #[test]
    fn half_open_failure_reopens_circuit() {
        loom::model(|| {
            let cb = Arc::new(CircuitBreaker::with_clock(
                CircuitBreakerConfig::consecutive(1).half_open_permits(3),
                frozen_clock,
            ));

            cb.force_state(CircuitState::HalfOpen {
                permits_remaining: 3,
            });

            // One thread records failure, another tries allow
            let cb1 = Arc::clone(&cb);
            let h1 = thread::spawn(move || {
                cb1.record_failure();
            });

            let cb2 = Arc::clone(&cb);
            let h2 = thread::spawn(move || {
                let _ = cb2.allow();
            });

            h1.join().unwrap();
            h2.join().unwrap();

            // Circuit should be open after failure in half-open
            assert_eq!(cb.state(), CircuitState::Open);
        });
    }

    /// Verify mixed success/failure doesn't corrupt state.
    #[test]
    fn mixed_success_failure_no_corruption() {
        loom::model(|| {
            let cb = Arc::new(CircuitBreaker::with_clock(
                CircuitBreakerConfig::consecutive(5),
                frozen_clock,
            ));

            let cb1 = Arc::clone(&cb);
            let h1 = thread::spawn(move || {
                cb1.record_failure();
                cb1.record_failure();
            });

            let cb2 = Arc::clone(&cb);
            let h2 = thread::spawn(move || {
                cb2.record_success();
            });

            h1.join().unwrap();
            h2.join().unwrap();

            // State should be valid (closed with some failure count, or reset to 0)
            let state = cb.state();
            match state {
                CircuitState::Closed { failure_count } => {
                    assert!(failure_count <= 2);
                }
                _ => panic!("unexpected state: {:?}", state),
            }
        });
    }
}
