#![allow(dead_code, clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! Block-level write trace recorder.
//!
//! Records every block write/trim/zero with microsecond timestamps to a binary
//! file for offline analysis. Zero cost when disabled (Option<WriteTracer>).
//!
//! ## File Format
//!
//! ```text
//! Header (64 bytes):
//!   magic:        [u8; 8]  = b"GLIDETRC"
//!   version:      u32      = 1
//!   block_size:   u32      = 131072 (128KB)
//!   device_blocks: u64
//!   start_time_us: u64     (UNIX microseconds)
//!   export_name:  [u8; 32] (UTF-8, zero-padded)
//!
//! Entry (16 bytes, little-endian):
//!   block_index:  u32
//!   op:           u16      (0=write, 1=trim, 2=write_zeroes)
//!   span:         u16      (consecutive blocks in this op, usually 1)
//!   elapsed_us:   u64      (microseconds since start_time_us)
//! ```
//!
//! At 1000 writes/sec: 16KB/sec, ~56MB/hour, ~1.4GB/day.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tracing::warn;

const MAGIC: &[u8; 8] = b"GLIDETRC";
const VERSION: u32 = 1;

/// Operation types recorded in trace entries.
#[repr(u16)]
#[derive(Clone, Copy)]
pub enum TraceOp {
    Write = 0,
    Trim = 1,
    WriteZeroes = 2,
}

/// Records block-level I/O to a binary trace file.
///
/// Thread-safe: uses internal mutex via BufWriter. The write path is
/// already serialized by the cache's mmap write lock, so contention
/// is minimal.
pub struct WriteTracer {
    writer: parking_lot::Mutex<BufWriter<File>>,
    start: Instant,
    block_size: u32,
    warned: AtomicBool,
}

impl WriteTracer {
    /// Create a new tracer, writing to the given path.
    ///
    /// Writes the 64-byte header immediately. Returns an error if the
    /// file cannot be created.
    pub fn new(
        path: &Path,
        block_size: u32,
        device_blocks: u64,
        export_name: &str,
    ) -> std::io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(64 * 1024, file);

        // Header
        writer.write_all(MAGIC)?;
        writer.write_all(&VERSION.to_le_bytes())?;
        writer.write_all(&block_size.to_le_bytes())?;
        writer.write_all(&device_blocks.to_le_bytes())?;

        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        writer.write_all(&now_us.to_le_bytes())?;

        let mut name_buf = [0u8; 32];
        let name_bytes = export_name.as_bytes();
        let copy_len = name_bytes.len().min(32);
        name_buf[..copy_len].copy_from_slice(&name_bytes[..copy_len]);
        writer.write_all(&name_buf)?;

        writer.flush()?;

        Ok(Self {
            writer: parking_lot::Mutex::new(writer),
            start: Instant::now(),
            block_size,
            warned: AtomicBool::new(false),
        })
    }

    /// Record a block I/O operation.
    ///
    /// `offset` is byte offset, `length` is byte length. The tracer computes
    /// block indices internally. Multi-block writes produce one entry with
    /// span > 1.
    #[inline]
    pub fn record(&self, offset: u64, length: u64, op: TraceOp) {
        let block_start = (offset / u64::from(self.block_size)) as u32;
        let block_end = ((offset + length).div_ceil(u64::from(self.block_size))) as u32;
        let span = (block_end - block_start) as u16;
        let elapsed_us = self.start.elapsed().as_micros() as u64;

        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&block_start.to_le_bytes());
        buf[4..6].copy_from_slice(&(op as u16).to_le_bytes());
        buf[6..8].copy_from_slice(&span.to_le_bytes());
        buf[8..16].copy_from_slice(&elapsed_us.to_le_bytes());

        // Best-effort: drop writes on I/O error rather than failing the
        // guest write path.
        let mut w = self.writer.lock();
        if let Err(e) = w.write_all(&buf)
            && !self.warned.swap(true, Ordering::Relaxed)
        {
            warn!(error = %e, "write trace I/O failed, further errors will be silent");
        }
    }

    /// Flush buffered data to disk. Called on export teardown.
    pub fn finish(&self) {
        let mut w = self.writer.lock();
        if let Err(e) = w.flush()
            && !self.warned.swap(true, Ordering::Relaxed)
        {
            warn!(error = %e, "write trace flush failed");
        }
    }
}

/// Parsed trace header.
#[derive(Debug)]
pub struct TraceHeader {
    pub version: u32,
    pub block_size: u32,
    pub device_blocks: u64,
    pub start_time_us: u64,
    pub export_name: String,
}

/// Parsed trace entry.
#[derive(Debug, Clone, Copy)]
pub struct TraceEntry {
    pub block_index: u32,
    pub op: u16,
    pub span: u16,
    pub elapsed_us: u64,
}

/// Read a trace file header. Returns header and byte offset where entries start.
pub fn read_header(data: &[u8]) -> Option<TraceHeader> {
    if data.len() < 64 {
        return None;
    }
    if &data[0..8] != MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(data[8..12].try_into().ok()?);
    if version != VERSION {
        return None;
    }
    let block_size = u32::from_le_bytes(data[12..16].try_into().ok()?);
    let device_blocks = u64::from_le_bytes(data[16..24].try_into().ok()?);
    let start_time_us = u64::from_le_bytes(data[24..32].try_into().ok()?);

    let name_bytes = &data[32..64];
    let end = name_bytes.iter().position(|&b| b == 0).unwrap_or(32);
    let export_name = String::from_utf8_lossy(&name_bytes[..end]).to_string();

    Some(TraceHeader {
        version,
        block_size,
        device_blocks,
        start_time_us,
        export_name,
    })
}

/// Iterate trace entries from a memory-mapped or loaded file.
/// `data` should be the full file contents; entries start at offset 64.
pub fn iter_entries(data: &[u8]) -> impl Iterator<Item = TraceEntry> + '_ {
    let entry_data = if data.len() > 64 { &data[64..] } else { &[] };
    entry_data.chunks_exact(16).map(|chunk| TraceEntry {
        block_index: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
        op: u16::from_le_bytes(chunk[4..6].try_into().unwrap()),
        span: u16::from_le_bytes(chunk[6..8].try_into().unwrap()),
        elapsed_us: u64::from_le_bytes(chunk[8..16].try_into().unwrap()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.trace");

        let tracer = WriteTracer::new(&path, 131072, 1024, "test-export").unwrap();

        // Record some writes
        tracer.record(0, 131072, TraceOp::Write); // block 0, 1 block
        tracer.record(131072 * 5, 131072 * 3, TraceOp::Write); // blocks 5-7, 3 blocks
        tracer.record(131072 * 2, 131072, TraceOp::Trim); // block 2, trim
        tracer.finish();

        // Read back
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 64 + 3 * 16); // header + 3 entries

        let header = read_header(&data).unwrap();
        assert_eq!(header.version, 1);
        assert_eq!(header.block_size, 131072);
        assert_eq!(header.device_blocks, 1024);
        assert_eq!(header.export_name, "test-export");

        let entries: Vec<_> = iter_entries(&data).collect();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].block_index, 0);
        assert_eq!(entries[0].op, 0); // Write
        assert_eq!(entries[0].span, 1);

        assert_eq!(entries[1].block_index, 5);
        assert_eq!(entries[1].op, 0); // Write
        assert_eq!(entries[1].span, 3);

        assert_eq!(entries[2].block_index, 2);
        assert_eq!(entries[2].op, 1); // Trim
        assert_eq!(entries[2].span, 1);

        // Timestamps should be monotonically increasing
        assert!(entries[1].elapsed_us >= entries[0].elapsed_us);
        assert!(entries[2].elapsed_us >= entries[1].elapsed_us);
    }

    #[test]
    fn test_empty_trace() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("empty.trace");

        let tracer = WriteTracer::new(&path, 131072, 512, "empty").unwrap();
        tracer.finish();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 64); // header only

        let header = read_header(&data).unwrap();
        assert_eq!(header.export_name, "empty");
        assert_eq!(header.device_blocks, 512);

        let entries: Vec<_> = iter_entries(&data).collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_long_export_name_truncated() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("long.trace");

        let long_name = "a".repeat(64);
        let tracer = WriteTracer::new(&path, 131072, 100, &long_name).unwrap();
        tracer.finish();

        let data = std::fs::read(&path).unwrap();
        let header = read_header(&data).unwrap();
        assert_eq!(header.export_name.len(), 32);
    }
}
