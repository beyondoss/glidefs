//! Sequential read-ahead detector.
//!
//! Tracks recent chunk accesses to detect sequential patterns. When 3+
//! consecutive chunks are read, triggers prefetch of the next pack to
//! hide S3 latency for sequential workloads (boot, large file reads).

use crate::nbd::pack::BLOCKS_PER_PACK;

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

        // Check if last 3 entries are consecutive (n, n+1, n+2)
        let count = self.len.min(3) as usize;
        let mut recent = Vec::with_capacity(count);
        for i in 0..count {
            let idx = (self.pos as usize + 4 - count + i) % 4;
            recent.push(self.last_chunks[idx]);
        }

        let is_sequential = recent.windows(2).all(|w| w[1] == w[0] + 1);
        if !is_sequential {
            self.last_readahead_chunk = None;
            return None;
        }

        // Calculate the first chunk of the next pack
        let current_pack = chunk_index / BLOCKS_PER_PACK as u64;
        let next_pack_start = (current_pack + 1) * BLOCKS_PER_PACK as u64;

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
        // BLOCKS_PER_PACK = 25, so pack 0 is chunks 0..24, pack 1 is 25..49.
        // We need to reach a point where the next-pack calculation differs.
        let mut new_trigger = None;
        for i in 4..=BLOCKS_PER_PACK as u64 + 2 {
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
