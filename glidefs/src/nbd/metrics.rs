//! Metrics for tracking write amplification and cache effectiveness.
//!
//! These metrics help answer:
//! - How much write amplification are we seeing? (guest bytes vs S3 bytes)
//! - How well is the cache coalescing writes? (guest writes vs S3 writes)
//! - How effective is read caching? (cache hits vs misses)
//! - Where is latency coming from? (read/write/s3 timing breakdown)

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::Mutex;
use std::time::Duration;

/// Sample rate for latency histograms: record 1 in N operations.
/// At 100k IOPS, this gives ~1.5k samples/sec - plenty for percentiles.
/// Reduces mutex contention by ~98%.
const LATENCY_SAMPLE_INTERVAL: u64 = 64;

/// Metrics for a single export's I/O operations.
#[derive(Debug)]
pub struct ExportMetrics {
    /// Total bytes written by guest (actual NBD write payload)
    pub guest_bytes_written: AtomicU64,

    /// Total number of NBD write commands from guest
    pub guest_write_ops: AtomicU64,

    /// Total bytes read by guest
    pub guest_bytes_read: AtomicU64,

    /// Total number of NBD read commands from guest
    pub guest_read_ops: AtomicU64,

    /// Total bytes written to S3
    pub s3_bytes_written: AtomicU64,

    /// Total number of blocks written to S3 (within batches)
    pub s3_write_ops: AtomicU64,

    /// Total number of batches written to S3
    pub batches_written: AtomicU64,

    /// Total bytes read from S3
    pub s3_bytes_read: AtomicU64,

    /// Total number of blocks read from S3
    pub s3_read_ops: AtomicU64,

    /// Cache hits (reads served from local cache)
    pub cache_hits: AtomicU64,

    /// Cache misses (reads that required S3 fetch)
    pub cache_misses: AtomicU64,

    /// Failed S3 PUT operations (pack uploads)
    pub s3_put_errors: AtomicU64,

    /// Failed S3 GET operations (pack fetches)
    pub s3_get_errors: AtomicU64,

    /// Failed flush cycles (pack flush or manifest sync)
    pub flush_errors: AtomicU64,

    /// Counter for latency sampling (reduces histogram mutex contention)
    latency_sample_counter: AtomicU64,

    // Latency tracking for diagnosing performance issues (sampled)
    /// Read operation latencies (microseconds) - sampled 1:64
    read_latencies: Mutex<LatencyHistogram>,
    /// Write operation latencies (microseconds) - sampled 1:64
    write_latencies: Mutex<LatencyHistogram>,
    /// S3 fetch latencies (microseconds) - sampled 1:64
    s3_fetch_latencies: Mutex<LatencyHistogram>,
    /// Local file read latencies (microseconds) - sampled 1:64
    file_read_latencies: Mutex<LatencyHistogram>,
    /// Local file write latencies (microseconds) - sampled 1:64
    file_write_latencies: Mutex<LatencyHistogram>,
    /// S3 PUT latencies (microseconds) - sampled 1:64
    s3_put_latencies: Mutex<LatencyHistogram>,
}

/// Simple histogram for latency tracking.
/// Buckets: <100us, <1ms, <10ms, <100ms, <1s, >=1s
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    pub count: u64,
    pub sum_us: u64,
    pub min_us: u64,
    pub max_us: u64,
    /// Buckets: [<100us, <1ms, <10ms, <100ms, <1s, >=1s]
    pub buckets: [u64; 6],
}

impl LatencyHistogram {
    pub fn record(&mut self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.count += 1;
        self.sum_us += us;
        if self.min_us == 0 || us < self.min_us {
            self.min_us = us;
        }
        if us > self.max_us {
            self.max_us = us;
        }
        // Bucket assignment
        let bucket = if us < 100 {
            0
        } else if us < 1_000 {
            1
        } else if us < 10_000 {
            2
        } else if us < 100_000 {
            3
        } else if us < 1_000_000 {
            4
        } else {
            5
        };
        self.buckets[bucket] += 1;
    }

    pub fn avg_us(&self) -> f64 {
        if self.count > 0 {
            self.sum_us as f64 / self.count as f64
        } else {
            0.0
        }
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            count: self.count,
            avg_us: self.avg_us(),
            min_us: self.min_us,
            max_us: self.max_us,
            sum_us: self.sum_us,
            buckets: self.buckets,
            p50_bucket: self.percentile_bucket(50),
            p99_bucket: self.percentile_bucket(99),
        }
    }

    fn percentile_bucket(&self, pct: u64) -> &'static str {
        if self.count == 0 {
            return "n/a";
        }
        let target = (self.count * pct) / 100;
        let mut cumulative = 0u64;
        let bucket_names = ["<100us", "<1ms", "<10ms", "<100ms", "<1s", ">=1s"];
        for (i, &count) in self.buckets.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                return bucket_names[i];
            }
        }
        ">=1s"
    }
}

/// Snapshot of latency histogram data.
#[derive(Debug, Clone, Serialize)]
pub struct LatencySnapshot {
    pub count: u64,
    pub avg_us: f64,
    pub min_us: u64,
    pub max_us: u64,
    pub sum_us: u64,
    /// Raw bucket counts: [<100us, <1ms, <10ms, <100ms, <1s, >=1s]
    pub buckets: [u64; 6],
    pub p50_bucket: &'static str,
    pub p99_bucket: &'static str,
}

/// Prometheus `le` boundaries in seconds matching our bucket layout.
const HISTOGRAM_LE: [&str; 5] = ["0.0001", "0.001", "0.01", "0.1", "1"];

impl LatencySnapshot {
    /// Emit Prometheus histogram text lines for this latency.
    ///
    /// `metric_name` should already include the `_seconds` suffix.
    /// `label` is the pre-formatted label string like `export="vol1"`.
    pub fn write_prometheus(&self, out: &mut String, metric_name: &str, label: &str) {
        use std::fmt::Write;
        if self.count == 0 {
            return;
        }
        // Cumulative buckets
        let mut cumulative = 0u64;
        for (i, le) in HISTOGRAM_LE.iter().enumerate() {
            cumulative += self.buckets[i];
            writeln!(
                out,
                "{metric_name}_bucket{{{label},le=\"{le}\"}} {cumulative}"
            )
            .unwrap();
        }
        // +Inf bucket (total count)
        cumulative += self.buckets[5];
        writeln!(
            out,
            "{metric_name}_bucket{{{label},le=\"+Inf\"}} {cumulative}"
        )
        .unwrap();
        // Sum in seconds
        writeln!(
            out,
            "{metric_name}_sum{{{label}}} {:.6}",
            self.sum_us as f64 / 1_000_000.0
        )
        .unwrap();
        writeln!(out, "{metric_name}_count{{{label}}} {}", self.count).unwrap();
    }
}

impl Default for ExportMetrics {
    fn default() -> Self {
        Self {
            guest_bytes_written: AtomicU64::new(0),
            guest_write_ops: AtomicU64::new(0),
            guest_bytes_read: AtomicU64::new(0),
            guest_read_ops: AtomicU64::new(0),
            s3_bytes_written: AtomicU64::new(0),
            s3_write_ops: AtomicU64::new(0),
            batches_written: AtomicU64::new(0),
            s3_bytes_read: AtomicU64::new(0),
            s3_read_ops: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            s3_put_errors: AtomicU64::new(0),
            s3_get_errors: AtomicU64::new(0),
            flush_errors: AtomicU64::new(0),
            latency_sample_counter: AtomicU64::new(0),
            read_latencies: Mutex::new(LatencyHistogram::default()),
            write_latencies: Mutex::new(LatencyHistogram::default()),
            s3_fetch_latencies: Mutex::new(LatencyHistogram::default()),
            file_read_latencies: Mutex::new(LatencyHistogram::default()),
            file_write_latencies: Mutex::new(LatencyHistogram::default()),
            s3_put_latencies: Mutex::new(LatencyHistogram::default()),
        }
    }
}

impl ExportMetrics {
    /// Create new metrics with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if we should record this latency sample.
    /// Uses a shared counter to sample ~1 in 64 operations.
    #[inline]
    fn should_sample(&self) -> bool {
        // Relaxed is fine - we don't need precise sampling, just reduced contention
        self.latency_sample_counter
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(LATENCY_SAMPLE_INTERVAL)
    }

    /// Record read operation latency (sampled to reduce mutex contention).
    #[inline]
    pub fn record_read_latency(&self, duration: Duration) {
        if self.should_sample() {
            self.read_latencies.lock().record(duration);
        }
    }

    /// Record write operation latency (sampled to reduce mutex contention).
    #[inline]
    pub fn record_write_latency(&self, duration: Duration) {
        if self.should_sample() {
            self.write_latencies.lock().record(duration);
        }
    }

    /// Record S3 fetch latency (sampled to reduce mutex contention).
    #[allow(dead_code)]
    #[inline]
    pub fn record_s3_fetch_latency(&self, duration: Duration) {
        if self.should_sample() {
            self.s3_fetch_latencies.lock().record(duration);
        }
    }

    /// Record local file read latency (sampled to reduce mutex contention).
    #[allow(dead_code)]
    #[inline]
    pub fn record_file_read_latency(&self, duration: Duration) {
        if self.should_sample() {
            self.file_read_latencies.lock().record(duration);
        }
    }

    /// Record local file write latency (sampled to reduce mutex contention).
    #[inline]
    #[allow(dead_code)] // Available for future instrumentation
    pub fn record_file_write_latency(&self, duration: Duration) {
        if self.should_sample() {
            self.file_write_latencies.lock().record(duration);
        }
    }

    /// Record a guest write operation.
    #[inline]
    pub fn record_guest_write(&self, bytes: u64) {
        self.guest_bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.guest_write_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a guest read operation.
    #[inline]
    pub fn record_guest_read(&self, bytes: u64) {
        self.guest_bytes_read.fetch_add(bytes, Ordering::Relaxed);
        self.guest_read_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an S3 batch write operation.
    #[inline]
    pub fn record_batch_write(&self, bytes: u64) {
        self.s3_bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.batches_written.fetch_add(1, Ordering::Relaxed);
    }

    /// Record blocks synced within a batch.
    #[inline]
    #[allow(dead_code)] // Used in tests, part of metrics API
    pub fn record_blocks_synced(&self, count: u64) {
        self.s3_write_ops.fetch_add(count, Ordering::Relaxed);
    }

    /// Record an S3 read operation.
    #[allow(dead_code)]
    #[inline]
    pub fn record_s3_read(&self, bytes: u64) {
        self.s3_bytes_read.fetch_add(bytes, Ordering::Relaxed);
        self.s3_read_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache hit.
    #[allow(dead_code)]
    #[inline]
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    #[allow(dead_code)]
    #[inline]
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed S3 PUT operation.
    #[allow(dead_code)]
    #[inline]
    pub fn record_s3_put_error(&self) {
        self.s3_put_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed S3 GET operation.
    #[inline]
    pub fn record_s3_get_error(&self) {
        self.s3_get_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed flush cycle.
    #[inline]
    pub fn record_flush_error(&self) {
        self.flush_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record S3 PUT latency (sampled to reduce mutex contention).
    #[inline]
    pub fn record_s3_put_latency(&self, duration: Duration) {
        if self.should_sample() {
            self.s3_put_latencies.lock().record(duration);
        }
    }

    /// Get a snapshot of current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let guest_bytes_written = self.guest_bytes_written.load(Ordering::Relaxed);
        let guest_write_ops = self.guest_write_ops.load(Ordering::Relaxed);
        let s3_bytes_written = self.s3_bytes_written.load(Ordering::Relaxed);
        let s3_write_ops = self.s3_write_ops.load(Ordering::Relaxed);
        let batches_written = self.batches_written.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);

        // Calculate derived metrics
        let write_amplification = if guest_bytes_written > 0 {
            s3_bytes_written as f64 / guest_bytes_written as f64
        } else {
            0.0
        };

        // Coalesce ratio: guest writes per S3 batch write
        let coalesce_ratio = if batches_written > 0 {
            guest_write_ops as f64 / batches_written as f64
        } else {
            0.0
        };

        let cache_hit_rate = if cache_hits + cache_misses > 0 {
            cache_hits as f64 / (cache_hits + cache_misses) as f64
        } else {
            0.0
        };

        // Get latency snapshots
        let read_latency = Some(self.read_latencies.lock().snapshot());
        let write_latency = Some(self.write_latencies.lock().snapshot());
        let s3_fetch_latency = Some(self.s3_fetch_latencies.lock().snapshot());
        let file_read_latency = Some(self.file_read_latencies.lock().snapshot());
        let file_write_latency = Some(self.file_write_latencies.lock().snapshot());
        let s3_put_latency = Some(self.s3_put_latencies.lock().snapshot());

        MetricsSnapshot {
            guest_bytes_written,
            guest_write_ops,
            guest_bytes_read: self.guest_bytes_read.load(Ordering::Relaxed),
            guest_read_ops: self.guest_read_ops.load(Ordering::Relaxed),
            s3_bytes_written,
            s3_write_ops,
            batches_written,
            s3_bytes_read: self.s3_bytes_read.load(Ordering::Relaxed),
            s3_read_ops: self.s3_read_ops.load(Ordering::Relaxed),
            cache_hits,
            cache_misses,
            s3_put_errors: self.s3_put_errors.load(Ordering::Relaxed),
            s3_get_errors: self.s3_get_errors.load(Ordering::Relaxed),
            flush_errors: self.flush_errors.load(Ordering::Relaxed),
            dirty_blocks: None,
            syncing_blocks: None,
            write_amplification,
            coalesce_ratio,
            cache_hit_rate,
            read_latency,
            write_latency,
            s3_fetch_latency,
            file_read_latency,
            file_write_latency,
            s3_put_latency,
        }
    }
}

/// Point-in-time snapshot of metrics with derived calculations.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    // Raw counters
    pub guest_bytes_written: u64,
    pub guest_write_ops: u64,
    pub guest_bytes_read: u64,
    pub guest_read_ops: u64,
    pub s3_bytes_written: u64,
    pub s3_write_ops: u64,
    pub batches_written: u64,
    pub s3_bytes_read: u64,
    pub s3_read_ops: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub s3_put_errors: u64,
    pub s3_get_errors: u64,
    pub flush_errors: u64,

    // Cache state (populated by router)
    /// Number of dirty blocks waiting to be synced to S3
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dirty_blocks: Option<u64>,
    /// Number of blocks currently being synced to S3
    #[serde(skip_serializing_if = "Option::is_none")]
    pub syncing_blocks: Option<u64>,

    // Derived metrics
    /// S3 bytes written / guest bytes written
    /// - `> 1.0` = write amplification (S3 batching overhead)
    /// - `< 1.0` = unlikely without compression
    pub write_amplification: f64,

    /// Guest write ops / batches written
    /// > 1.0 = batching coalescing multiple guest writes into fewer S3 batch writes
    pub coalesce_ratio: f64,

    /// Cache hits / (hits + misses)
    pub cache_hit_rate: f64,

    // Latency breakdown
    /// Read operation latencies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_latency: Option<LatencySnapshot>,
    /// Write operation latencies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_latency: Option<LatencySnapshot>,
    /// S3 fetch latencies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_fetch_latency: Option<LatencySnapshot>,
    /// Local file read latencies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_read_latency: Option<LatencySnapshot>,
    /// Local file write latencies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_write_latency: Option<LatencySnapshot>,
    /// S3 PUT latencies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_put_latency: Option<LatencySnapshot>,
}

impl MetricsSnapshot {
    /// Add cache state to the snapshot.
    pub fn with_cache_state(mut self, dirty_blocks: u64, syncing_blocks: u64) -> Self {
        self.dirty_blocks = Some(dirty_blocks);
        self.syncing_blocks = Some(syncing_blocks);
        self
    }

    /// Format this snapshot as Prometheus metrics text for a single export.
    pub fn to_prometheus(&self, export_name: &str) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        // Helper to write a metric line
        let label = format!("export=\"{}\"", export_name);

        // Guest I/O counters
        writeln!(out, "glidefs_guest_bytes_written_total{{{label}}} {}", self.guest_bytes_written).unwrap();
        writeln!(out, "glidefs_guest_write_ops_total{{{label}}} {}", self.guest_write_ops).unwrap();
        writeln!(out, "glidefs_guest_bytes_read_total{{{label}}} {}", self.guest_bytes_read).unwrap();
        writeln!(out, "glidefs_guest_read_ops_total{{{label}}} {}", self.guest_read_ops).unwrap();

        // S3 I/O counters
        writeln!(out, "glidefs_s3_bytes_written_total{{{label}}} {}", self.s3_bytes_written).unwrap();
        writeln!(out, "glidefs_s3_write_ops_total{{{label}}} {}", self.s3_write_ops).unwrap();
        writeln!(out, "glidefs_s3_batches_written_total{{{label}}} {}", self.batches_written).unwrap();
        writeln!(out, "glidefs_s3_bytes_read_total{{{label}}} {}", self.s3_bytes_read).unwrap();
        writeln!(out, "glidefs_s3_read_ops_total{{{label}}} {}", self.s3_read_ops).unwrap();

        // Cache counters
        writeln!(out, "glidefs_cache_hits_total{{{label}}} {}", self.cache_hits).unwrap();
        writeln!(out, "glidefs_cache_misses_total{{{label}}} {}", self.cache_misses).unwrap();

        // Error counters
        writeln!(out, "glidefs_s3_put_errors_total{{{label}}} {}", self.s3_put_errors).unwrap();
        writeln!(out, "glidefs_s3_get_errors_total{{{label}}} {}", self.s3_get_errors).unwrap();
        writeln!(out, "glidefs_flush_errors_total{{{label}}} {}", self.flush_errors).unwrap();

        // Cache state (gauges)
        if let Some(dirty) = self.dirty_blocks {
            writeln!(out, "glidefs_dirty_blocks{{{label}}} {dirty}").unwrap();
        }
        if let Some(syncing) = self.syncing_blocks {
            writeln!(out, "glidefs_syncing_blocks{{{label}}} {syncing}").unwrap();
        }

        // Derived metrics (gauges)
        writeln!(out, "glidefs_write_amplification{{{label}}} {:.6}", self.write_amplification).unwrap();
        writeln!(out, "glidefs_coalesce_ratio{{{label}}} {:.6}", self.coalesce_ratio).unwrap();
        writeln!(out, "glidefs_cache_hit_rate{{{label}}} {:.6}", self.cache_hit_rate).unwrap();

        // Latency histograms (Prometheus histogram format)
        if let Some(ref lat) = self.read_latency {
            lat.write_prometheus(&mut out, "glidefs_read_latency_seconds", &label);
        }
        if let Some(ref lat) = self.write_latency {
            lat.write_prometheus(&mut out, "glidefs_write_latency_seconds", &label);
        }
        if let Some(ref lat) = self.s3_fetch_latency {
            lat.write_prometheus(&mut out, "glidefs_s3_fetch_latency_seconds", &label);
        }
        if let Some(ref lat) = self.file_read_latency {
            lat.write_prometheus(&mut out, "glidefs_file_read_latency_seconds", &label);
        }
        if let Some(ref lat) = self.file_write_latency {
            lat.write_prometheus(&mut out, "glidefs_file_write_latency_seconds", &label);
        }
        if let Some(ref lat) = self.s3_put_latency {
            lat.write_prometheus(&mut out, "glidefs_s3_put_latency_seconds", &label);
        }

        out
    }

    /// Print a human-readable latency breakdown summary.
    #[allow(dead_code)] // Useful for debugging
    pub fn print_latency_summary(&self) {
        eprintln!("\n=== Latency Breakdown ===");
        if let Some(ref lat) = self.read_latency
            && lat.count > 0 {
                eprintln!("  Read:       {:>8.0}us avg, {:>8}us max, {} ops (p50: {}, p99: {})",
                    lat.avg_us, lat.max_us, lat.count, lat.p50_bucket, lat.p99_bucket);
            }
        if let Some(ref lat) = self.write_latency
            && lat.count > 0 {
                eprintln!("  Write:      {:>8.0}us avg, {:>8}us max, {} ops (p50: {}, p99: {})",
                    lat.avg_us, lat.max_us, lat.count, lat.p50_bucket, lat.p99_bucket);
            }
        if let Some(ref lat) = self.s3_fetch_latency
            && lat.count > 0 {
                eprintln!("  S3 Fetch:   {:>8.0}us avg, {:>8}us max, {} ops (p50: {}, p99: {})",
                    lat.avg_us, lat.max_us, lat.count, lat.p50_bucket, lat.p99_bucket);
            }
        if let Some(ref lat) = self.file_read_latency
            && lat.count > 0 {
                eprintln!("  File Read:  {:>8.0}us avg, {:>8}us max, {} ops (p50: {}, p99: {})",
                    lat.avg_us, lat.max_us, lat.count, lat.p50_bucket, lat.p99_bucket);
            }
        if let Some(ref lat) = self.file_write_latency
            && lat.count > 0 {
                eprintln!("  File Write: {:>8.0}us avg, {:>8}us max, {} ops (p50: {}, p99: {})",
                    lat.avg_us, lat.max_us, lat.count, lat.p50_bucket, lat.p99_bucket);
            }
        if let Some(ref lat) = self.s3_put_latency
            && lat.count > 0 {
                eprintln!("  S3 PUT:     {:>8.0}us avg, {:>8}us max, {} ops (p50: {}, p99: {})",
                    lat.avg_us, lat.max_us, lat.count, lat.p50_bucket, lat.p99_bucket);
            }
    }
}

/// Generate Prometheus metrics header (TYPE and HELP lines).
/// Call once before iterating exports.
pub fn prometheus_header() -> &'static str {
    r#"# HELP glidefs_guest_bytes_written_total Total bytes written by guest
# TYPE glidefs_guest_bytes_written_total counter
# HELP glidefs_guest_write_ops_total Total NBD write commands from guest
# TYPE glidefs_guest_write_ops_total counter
# HELP glidefs_guest_bytes_read_total Total bytes read by guest
# TYPE glidefs_guest_bytes_read_total counter
# HELP glidefs_guest_read_ops_total Total NBD read commands from guest
# TYPE glidefs_guest_read_ops_total counter
# HELP glidefs_s3_bytes_written_total Total bytes written to S3
# TYPE glidefs_s3_bytes_written_total counter
# HELP glidefs_s3_write_ops_total Total blocks written to S3
# TYPE glidefs_s3_write_ops_total counter
# HELP glidefs_s3_batches_written_total Total S3 batch objects written
# TYPE glidefs_s3_batches_written_total counter
# HELP glidefs_s3_bytes_read_total Total bytes read from S3
# TYPE glidefs_s3_bytes_read_total counter
# HELP glidefs_s3_read_ops_total Total S3 read operations
# TYPE glidefs_s3_read_ops_total counter
# HELP glidefs_cache_hits_total Cache hits (reads served from local cache)
# TYPE glidefs_cache_hits_total counter
# HELP glidefs_cache_misses_total Cache misses (reads requiring S3 fetch)
# TYPE glidefs_cache_misses_total counter
# HELP glidefs_dirty_blocks Blocks waiting to sync to S3
# TYPE glidefs_dirty_blocks gauge
# HELP glidefs_syncing_blocks Blocks currently syncing to S3
# TYPE glidefs_syncing_blocks gauge
# HELP glidefs_write_amplification S3 bytes / guest bytes written
# TYPE glidefs_write_amplification gauge
# HELP glidefs_coalesce_ratio Guest write ops / S3 batch writes
# TYPE glidefs_coalesce_ratio gauge
# HELP glidefs_cache_hit_rate Fraction of reads served from cache
# TYPE glidefs_cache_hit_rate gauge
# HELP glidefs_s3_put_errors_total Failed S3 PUT operations
# TYPE glidefs_s3_put_errors_total counter
# HELP glidefs_s3_get_errors_total Failed S3 GET operations
# TYPE glidefs_s3_get_errors_total counter
# HELP glidefs_flush_errors_total Failed flush cycles
# TYPE glidefs_flush_errors_total counter
# HELP glidefs_read_latency_seconds NBD read operation latency
# TYPE glidefs_read_latency_seconds histogram
# HELP glidefs_write_latency_seconds NBD write operation latency
# TYPE glidefs_write_latency_seconds histogram
# HELP glidefs_s3_fetch_latency_seconds S3 block fetch latency
# TYPE glidefs_s3_fetch_latency_seconds histogram
# HELP glidefs_file_read_latency_seconds Local file read latency
# TYPE glidefs_file_read_latency_seconds histogram
# HELP glidefs_file_write_latency_seconds Local file write latency
# TYPE glidefs_file_write_latency_seconds histogram
# HELP glidefs_s3_put_latency_seconds S3 pack upload latency
# TYPE glidefs_s3_put_latency_seconds histogram
"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_snapshot() {
        let metrics = ExportMetrics::new();

        // Simulate: guest writes 10KB across 10 ops
        for _ in 0..10 {
            metrics.record_guest_write(1024);
        }

        // Simulate: batch write of 100KB containing 10 blocks
        metrics.record_batch_write(100_000);
        metrics.record_blocks_synced(10);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.guest_bytes_written, 10 * 1024);
        assert_eq!(snapshot.guest_write_ops, 10);
        assert_eq!(snapshot.s3_bytes_written, 100_000);
        assert_eq!(snapshot.batches_written, 1);
        assert_eq!(snapshot.s3_write_ops, 10);

        // Write amplification: 100KB written to S3 for 10KB guest writes
        assert!((snapshot.write_amplification - 9.765).abs() < 0.1);

        // Coalesce ratio: 10 guest writes → 1 batch write
        assert!((snapshot.coalesce_ratio - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = ExportMetrics::new();

        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        let snapshot = metrics.snapshot();
        assert!((snapshot.cache_hit_rate - 0.75).abs() < 0.01);
    }
}
