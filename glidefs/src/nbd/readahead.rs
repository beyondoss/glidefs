//! Sequential read-ahead detector.
//!
//! Tracks recent chunk accesses to detect sequential patterns. When 3+
//! consecutive chunks are read, triggers prefetch of the next pack to
//! hide S3 latency for sequential workloads (boot, large file reads).

use crate::nbd::pack::DEFAULT_BLOCKS_PER_PACK;

/// Tracks recent chunk accesses to detect sequential read patterns.
pub struct SequentialDetector {
    /// Ring buffer of last 4 chunk indices
    last_chunks: [u64; 4],
    /// Number of entries in the ring buffer (0-4)
    len: u8,
    /// Write position in ring buffer
    pos: u8,
    /// The chunk index we last triggered readahead for (dedup)
    last_readahead_chunk: Option<u64>,
}

impl SequentialDetector {
    pub fn new() -> Self {
        Self {
            last_chunks: [u64::MAX; 4],
            len: 0,
            pos: 0,
            last_readahead_chunk: None,
        }
    }

    /// Record a chunk access. Returns Some(chunk_index) if sequential readahead
    /// should be triggered for that chunk (the first chunk of the next pack).
    pub fn record(&mut self, chunk_index: u64) -> Option<u64> {
        // Insert into ring buffer
        self.last_chunks[self.pos as usize] = chunk_index;
        self.pos = (self.pos + 1) % 4;
        if self.len < 4 {
            self.len += 1;
        }

        // Need at least 3 entries to detect sequential pattern
        if self.len < 3 {
            return None;
        }

        // Check if last 3 entries are consecutive (n, n+1, n+2).
        // Read directly from the ring buffer — no allocation needed.
        let a = self.last_chunks[(self.pos as usize + 4 - 3) % 4];
        let b = self.last_chunks[(self.pos as usize + 4 - 2) % 4];
        let c = self.last_chunks[(self.pos as usize + 4 - 1) % 4];
        let is_sequential = b == a + 1 && c == b + 1;
        if !is_sequential {
            self.last_readahead_chunk = None;
            return None;
        }

        // Calculate the first chunk of the next pack
        let current_pack = chunk_index / DEFAULT_BLOCKS_PER_PACK as u64;
        let next_pack_start = (current_pack + 1) * DEFAULT_BLOCKS_PER_PACK as u64;

        // Don't re-trigger for the same target
        if self.last_readahead_chunk == Some(next_pack_start) {
            return None;
        }

        self.last_readahead_chunk = Some(next_pack_start);
        Some(next_pack_start)
    }
}

impl Default for SequentialDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_detection() {
        let mut det = SequentialDetector::new();

        // Feed [0, 1, 2] — should trigger readahead
        assert_eq!(det.record(0), None);
        assert_eq!(det.record(1), None);
        let result = det.record(2);
        assert!(result.is_some(), "3 consecutive chunks should trigger readahead");

        // Feed [0, 1, 100] — non-sequential, should return None
        let mut det = SequentialDetector::new();
        assert_eq!(det.record(0), None);
        assert_eq!(det.record(1), None);
        assert_eq!(det.record(100), None);
    }

    #[test]
    fn test_no_trigger_below_threshold() {
        let mut det = SequentialDetector::new();

        // Only 2 entries — not enough to detect sequential pattern
        assert_eq!(det.record(0), None);
        assert_eq!(det.record(1), None);
    }

    #[test]
    fn test_dedup_trigger() {
        let mut det = SequentialDetector::new();

        // Feed [0, 1, 2] — triggers readahead for next pack
        assert_eq!(det.record(0), None);
        assert_eq!(det.record(1), None);
        let first_trigger = det.record(2);
        assert!(first_trigger.is_some());

        // Feed [3] — still sequential (ring buffer has [1,2,3]) but same pack target
        let dup = det.record(3);
        assert_eq!(dup, None, "should not re-trigger for the same pack");

        // Keep feeding until we cross into the next pack boundary.
        // DEFAULT_BLOCKS_PER_PACK = 500, so pack 0 is chunks 0..499, pack 1 is 500..999.
        // We need to reach a point where the next-pack calculation differs.
        let mut new_trigger = None;
        for i in 4..=DEFAULT_BLOCKS_PER_PACK as u64 + 2 {
            if let Some(t) = det.record(i) {
                new_trigger = Some(t);
                break;
            }
        }
        assert!(
            new_trigger.is_some(),
            "crossing a pack boundary should trigger a new readahead"
        );
        assert_ne!(
            new_trigger, first_trigger,
            "new trigger should be for a different pack"
        );
    }

    #[test]
    fn test_ring_buffer_wraparound() {
        let mut det = SequentialDetector::new();

        // Fill the ring buffer completely (4 entries) then keep going
        // to exercise the wraparound from pos=3 -> pos=0
        assert_eq!(det.record(10), None);
        assert_eq!(det.record(11), None);
        assert!(det.record(12).is_some()); // sequential detected, pos=3
        // Next record wraps pos to 0
        let result = det.record(13); // ring: [13, 11, 12, 12] -> last 3: [11, 12, 13]
        // Still sequential but same pack target, so dedup suppresses
        // (13/25 = pack 0, same as 12/25 = pack 0, so next_pack_start is 25 for both)
        // Already triggered for pack start 25, so None
        assert_eq!(result, None);
    }

    #[test]
    fn test_initial_state_no_false_triggers() {
        let mut det = SequentialDetector::new();

        // Record only 1 entry — ring has mostly u64::MAX, should never trigger
        assert_eq!(det.record(0), None);

        // A fresh detector with no entries should never trigger
        let det2 = SequentialDetector::new();
        assert_eq!(det2.len, 0);
    }

    #[test]
    fn test_large_chunk_indices() {
        let mut det = SequentialDetector::new();

        // Test with large chunk indices near pack boundaries
        let base = DEFAULT_BLOCKS_PER_PACK as u64 * 1000; // pack 1000
        assert_eq!(det.record(base), None);
        assert_eq!(det.record(base + 1), None);
        let result = det.record(base + 2);
        assert!(result.is_some(), "should trigger at high chunk indices");

        let target = result.unwrap();
        let expected_next_pack = ((base + 2) / DEFAULT_BLOCKS_PER_PACK as u64 + 1) * DEFAULT_BLOCKS_PER_PACK as u64;
        assert_eq!(target, expected_next_pack);
    }

    #[test]
    fn test_reset_on_non_sequential() {
        let mut det = SequentialDetector::new();

        // Feed [0, 1, 2] — triggers readahead
        assert_eq!(det.record(0), None);
        assert_eq!(det.record(1), None);
        assert!(det.record(2).is_some());

        // Break the sequence with a random access
        assert_eq!(det.record(100), None);

        // After the break, only 1 sequential pair exists in recent history.
        // Ring buffer last 3: [2, 100, 101] — NOT all consecutive.
        assert_eq!(det.record(101), None);
        // Ring buffer last 3: [100, 101, 102] — this IS sequential again,
        // so the detector correctly re-detects a new sequential run.
        // But we want to verify that with only 2 entries after a break,
        // the old sequential pattern is truly broken.

        // Start fresh: verify the break prevents triggering with only 2
        // sequential entries after the gap.
        let mut det = SequentialDetector::new();
        assert_eq!(det.record(0), None);
        assert_eq!(det.record(1), None);
        assert!(det.record(2).is_some());

        // Break with a non-sequential access
        assert_eq!(det.record(100), None);

        // Only 1 more sequential — ring has [2, 100, 101], last 3 not consecutive
        assert_eq!(det.record(101), None, "only 2 sequential after break should not trigger");
    }
}
