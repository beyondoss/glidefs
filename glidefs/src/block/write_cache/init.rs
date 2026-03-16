use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use tracing::{info, instrument, warn};

use crate::block::block_map::{
    SequenceNumber, SparseBlockState, SparseStateMap, shared_zero_block,
};
use crate::block::state::{Active, Initializing, Recovering};
use crate::block::wal::Wal;

use super::inner::{CacheInner, SyncFile};
use super::{CacheError, WriteCache, WriteCacheConfig};

impl WriteCache<Initializing> {
    /// Open or create a write cache.
    ///
    /// Returns a cache in `Recovering` state. Call `finish_recovery()` to
    /// transition to `Active` state before serving I/O.
    ///
    /// # Arguments
    /// * `config` - Cache configuration
    #[instrument(skip(config), fields(device = %config.device_name))]
    pub fn open(config: WriteCacheConfig) -> Result<WriteCache<Recovering>, CacheError> {
        config.validate()?;

        // Ensure cache directory exists
        std::fs::create_dir_all(&config.cache_dir)?;

        // Open or create data file for positional I/O (pread/pwrite)
        let data_path = config.data_path();
        let data_file = super::inner::SyncFile::open(&data_path, true, config.device_size)?;

        // Load block states (or create fresh)
        let num_blocks = config.num_blocks();
        let (state_map, mut dirty_count, persisted_max_seq, rotation_seq) =
            CacheInner::load_metadata(&config)?;
        let present_count = state_map.count_present();

        let wal_path = config.wal_path();
        let export_name = config.device_name.clone();

        // Track recovery issues for metrics
        let recovery_warning_count: u64 = 0;

        // Replay WAL entries after persisted sequence. When recovering from
        // a mid-flush crash (rotation_seq > 0), replay from the rotation point
        // to capture post-rotation writes — these blocks have authoritative data
        // in the active file (even zeros). Safe: already-DIRTY blocks from the
        // metadata are skipped (idempotent state transitions).
        let wal_min_seq = if rotation_seq > 0 && rotation_seq < persisted_max_seq {
            rotation_seq
        } else {
            persisted_max_seq
        };
        let wal_entries = match Wal::replay(&wal_path, wal_min_seq) {
            Ok(entries) => {
                if !entries.is_empty() {
                    info!(
                        entries = entries.len(),
                        min_seq = persisted_max_seq + 1,
                        "replaying WAL entries"
                    );
                }
                entries
            }
            Err(e) => {
                return Err(CacheError::Io(std::io::Error::other(
                    format!("WAL replay failed, refusing to open cache: {e}"),
                )));
            }
        };

        // Apply WAL entries: mark blocks dirty in SparseStateMap.
        // WAL stores metadata only — block data lives on the SSD cache file
        // (which was pwrite'd before the WAL entry was appended).
        let mut max_wal_seq = persisted_max_seq;

        for entry in &wal_entries {
            let block_index = entry.block_index as usize;
            max_wal_seq = max_wal_seq.max(entry.sequence);

            // Mark block as dirty in state_map
            if block_index < num_blocks {
                let old = state_map.get(block_index);
                if old != SparseBlockState::DIRTY {
                    // Ensure block is present first (allocates page if needed)
                    state_map.set_present(block_index);
                    // Transition to Dirty from whatever current state is
                    let current = state_map.get(block_index);
                    if current != SparseBlockState::DIRTY {
                        let _ = state_map.cas(block_index, current, SparseBlockState::DIRTY);
                    }
                    dirty_count += 1;
                }
            }
        }

        // Initialize sequence counter
        let initial_seq = persisted_max_seq.max(max_wal_seq);
        let sequence = SequenceNumber::new(initial_seq);

        // Rewrite the WAL with only valid entries to remove any torn tail.
        //
        // A crash can leave a partial/corrupt entry at the end of the WAL.
        // replay() stops at the first corruption, but Wal::open() appends
        // after the corruption. On the NEXT recovery, replay stops at the
        // old corruption and misses all entries written in the previous
        // session. Rewriting ensures a clean WAL for future appends.
        //
        // Uses atomic write (temp → fsync → rename) so a crash during
        // rewrite leaves the original WAL intact rather than truncated.
        {
            use crate::block::wal::serialize_entry;
            use std::io::Write as IoWrite;
            let mut buf = Vec::new();
            for entry in &wal_entries {
                serialize_entry(&mut buf, entry.block_index, entry.sequence);
            }
            let tmp_path = wal_path.with_extension("wal.tmp");
            let mut tmp_file = std::fs::File::create(&tmp_path)?;
            tmp_file.write_all(&buf)?;
            tmp_file.sync_all()?;
            drop(tmp_file);
            std::fs::rename(&tmp_path, &wal_path)?;
            // Fsync parent directory so the rename is durable across power loss.
            if let Some(parent) = wal_path.parent()
                && let Ok(dir) = std::fs::File::open(parent)
            {
                let _ = dir.sync_all();
            }
        }

        // Open WAL for new appends (clean file, no torn tail)
        let wal = match Wal::open(&wal_path) {
            Ok(w) => w,
            Err(e) => {
                warn!(error = %e, "failed to open WAL, removing and creating new");
                let _ = std::fs::remove_file(&wal_path);
                Wal::open(&wal_path).map_err(CacheError::Io)?
            }
        };

        let block_size = config.block_size;
        let (zbb, zbh) = shared_zero_block(block_size);

        // Crash recovery: if a flushing file exists, we crashed mid-flush.
        //
        // rotate_and_snapshot() CAS'd all DIRTY blocks to SYNCING before
        // renaming the active file to flushing. Post-rotation guest writes
        // CAS SYNCING→DIRTY and generate WAL entries. load_metadata()
        // preserves the SYNCING/DIRTY distinction so we can use it here:
        //
        //   SYNCING = pre-rotation data, lives in the flushing file
        //   DIRTY   = post-rotation write, lives in the active file
        //
        // When the metadata is from BEFORE the rotation (rotation_seq == 0
        // but flushing file exists), all blocks appear as DIRTY. In that
        // case, WAL entries (seq > persisted_max_seq) identify post-rotation
        // writes; other DIRTY blocks are pre-rotation and need recovery
        // from the flushing file.
        let flushing_path = config.flushing_path();
        if flushing_path.exists() {
            info!(rotation_seq, "found flushing file — recovering from interrupted flush");
            let flushing_file = SyncFile::open(&flushing_path, false, config.device_size)?;
            let block_size = config.block_size;

            // Identify blocks with post-rotation writes.
            let post_rotation_blocks: HashSet<usize> = if rotation_seq > 0 {
                // Metadata is from after rotation. DIRTY blocks in metadata
                // are post-rotation writes. WAL entries (post-checkpoint)
                // may have CAS'd additional SYNCING→DIRTY blocks.
                let mut set: HashSet<usize> = state_map
                    .iter_present()
                    .filter(|&(_, state)| state == SparseBlockState::DIRTY)
                    .map(|(idx, _)| idx)
                    .collect();
                for entry in &wal_entries {
                    let idx = entry.block_index as usize;
                    if idx < num_blocks {
                        set.insert(idx);
                    }
                }
                set
            } else {
                // Metadata is from before rotation. ALL dirty blocks in
                // metadata were pre-rotation. Only WAL entries identify
                // post-rotation writes.
                wal_entries
                    .iter()
                    .map(|e| e.block_index as usize)
                    .filter(|&idx| idx < num_blocks)
                    .collect()
            };

            let mut recovered = 0usize;
            let mut skipped = 0usize;
            for (idx, state) in state_map.iter_present() {
                let is_dirty = state == SparseBlockState::DIRTY
                    || state == SparseBlockState::SYNCING;
                if is_dirty {
                    if post_rotation_blocks.contains(&idx) {
                        // Post-rotation write: active file is authoritative,
                        // even if it contains all zeros (guest trim/write_zeroes).
                        skipped += 1;
                    } else {
                        // Pre-rotation data: recover from flushing file.
                        let offset = idx as u64 * block_size as u64;
                        let valid_bytes = std::cmp::min(
                            block_size as u64,
                            config.device_size.saturating_sub(offset),
                        ) as usize;
                        if valid_bytes > 0 {
                            let mut flush_buf = vec![0u8; valid_bytes];
                            flushing_file.read_exact_at(&mut flush_buf, offset)?;
                            data_file.write_all_at(&flush_buf, offset)?;
                            recovered += 1;
                        }
                    }
                    // Convert SYNCING → DIRTY for subsequent processing.
                    let _ = state_map.cas(idx, SparseBlockState::SYNCING, SparseBlockState::DIRTY);
                }
            }
            drop(flushing_file);
            std::fs::remove_file(&flushing_path)?;
            info!(
                recovered_blocks = recovered,
                skipped_blocks = skipped,
                post_rotation_blocks = post_rotation_blocks.len(),
                "flush recovery complete"
            );
        } else {
            // No flushing file — convert any SYNCING → DIRTY from metadata.
            for (idx, state) in state_map.iter_present().collect::<Vec<_>>() {
                if state == SparseBlockState::SYNCING {
                    let _ = state_map.cas(idx, SparseBlockState::SYNCING, SparseBlockState::DIRTY);
                }
            }
        }

        // Transition CLEAN blocks to NOT_PRESENT. A CLEAN block's pwrite may
        // not have landed before crash. Leaving it DIRTY would risk flushing
        // zeros to S3 (overwriting valid data). Making it NP forces reads to
        // go through S3, which is safe.
        for (idx, state) in state_map.iter_present().collect::<Vec<_>>() {
            if state == SparseBlockState::CLEAN {
                let _ = state_map.cas(idx, SparseBlockState::CLEAN, SparseBlockState::NOT_PRESENT);
                dirty_count = dirty_count.saturating_sub(1);
            }
        }

        // NOTE: DIRTY blocks may have ssd_active=0 after crash recovery
        // (rotation cleared ssd_active, flushing file was dropped before
        // recovery could copy data back). These blocks are left DIRTY so
        // the flush scheduler eventually processes them. The flush path's
        // compute_flush_batch handles zero blocks correctly (they become
        // zero-block tombstones via is_zero_block detection).
        //
        // Stateright model checking identified this scenario across 1.8M+
        // crash states. The zero-block tombstone mechanism ensures S3 data
        // is not silently overwritten with zeros — the tombstone preserves
        // "newest wins" semantics for forks.

        let inner = Arc::new(CacheInner {
            config,
            data_file: parking_lot::RwLock::new(data_file),
            flushing_file: parking_lot::Mutex::new(None),
            state_map,
            num_blocks,
            dirty_block_count: AtomicU64::new(dirty_count as u64),
            syncing_block_count: AtomicU64::new(0),
            sequence,
            wal,
            export_name,
            zero_block_hash: zbh,
            zero_block_bytes: zbb,
            recovery_warnings: AtomicU64::new(recovery_warning_count),

            flush_lock: tokio::sync::Mutex::new(()),
            manifest_etag: parking_lot::Mutex::new(None),
            page_crcs: dashmap::DashMap::new(),
            pages_per_block: block_size / 4096,
            flushing_active: AtomicBool::new(false),
            rotation_seq: AtomicU64::new(0),
            #[cfg(feature = "test-utils")]
            flush_sync: parking_lot::Mutex::new(None),
            #[cfg(feature = "test-utils")]
            promote_sync: parking_lot::Mutex::new(None),
            #[cfg(feature = "test-utils")]
            read_sync: parking_lot::Mutex::new(None),
        });

        info!(
            dirty_blocks = dirty_count,
            present_blocks = present_count,
            recovery_warnings = recovery_warning_count,
            "cache opened, transitioning to Recovering"
        );

        Ok(WriteCache {
            inner,
            _state: PhantomData,
        })
    }

    /// Create a fresh write cache in Active state (for forks).
    ///
    /// Unlike `open()`, this skips WAL replay and goes directly to Active.
    /// The local cache starts empty (all blocks NOT_PRESENT). Reads resolve
    /// through VolumeManifest + ChunkMetaCache for remote data.
    pub fn open_fresh_active(config: WriteCacheConfig) -> Result<WriteCache<Active>, CacheError> {
        config.validate()?;
        std::fs::create_dir_all(&config.cache_dir)?;

        let data_file =
            super::inner::SyncFile::open(&config.data_path(), true, config.device_size)?;
        let num_blocks = config.num_blocks();
        let state_map = SparseStateMap::new(num_blocks);
        let block_size = config.block_size;
        let sequence = SequenceNumber::new(0);
        let wal = Wal::open(&config.wal_path())?;
        let export_name = config.device_name.clone();
        let (zbb, zbh) = shared_zero_block(block_size);

        let inner = Arc::new(CacheInner {
            config,
            data_file: parking_lot::RwLock::new(data_file),
            flushing_file: parking_lot::Mutex::new(None),
            state_map,
            num_blocks,
            dirty_block_count: AtomicU64::new(0),
            syncing_block_count: AtomicU64::new(0),
            sequence,
            wal,
            export_name,
            zero_block_hash: zbh,
            zero_block_bytes: zbb,
            recovery_warnings: AtomicU64::new(0),

            flush_lock: tokio::sync::Mutex::new(()),
            manifest_etag: parking_lot::Mutex::new(None),
            page_crcs: dashmap::DashMap::new(),
            pages_per_block: block_size / 4096,
            flushing_active: AtomicBool::new(false),
            rotation_seq: AtomicU64::new(0),
            #[cfg(feature = "test-utils")]
            flush_sync: parking_lot::Mutex::new(None),
            #[cfg(feature = "test-utils")]
            promote_sync: parking_lot::Mutex::new(None),
            #[cfg(feature = "test-utils")]
            read_sync: parking_lot::Mutex::new(None),
        });

        info!("cache opened fresh for fork, directly Active");

        Ok(WriteCache {
            inner,
            _state: PhantomData,
        })
    }

}
