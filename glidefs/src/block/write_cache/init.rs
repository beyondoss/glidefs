#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
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
        //
        // **CRITICAL — skip in handoff successor mode**: the rename()
        // creates a new inode. The predecessor's still-open WAL fd
        // points to the OLD inode (renames don't touch open fds), so
        // predecessor's subsequent appends go to an unreachable file.
        // Successor's `replay_wal_tail` reads the NEW inode and misses
        // those appends. Manifests as "verify: bad magic header 0" in
        // fio under sequential / multi-export stress.
        // Predecessor's WAL is canonical until handoff completes; the
        // successor uses the existing inode as-is and lets the
        // predecessor's flush_scheduler / our own post-takeover
        // checkpoint handle WAL hygiene.
        let in_successor_passive_mode = std::env::var("GLIDEFS_HANDOFF_SUCCESSOR_PASSIVE")
            .map(|v| v == "1")
            .unwrap_or(false);
        if !in_successor_passive_mode {
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
            if let Some(parent) = wal_path.parent() {
                match std::fs::File::open(parent) {
                    Ok(dir) => {
                        if let Err(e) = dir.sync_all() {
                            warn!(error = %e, "dir fsync after WAL rename failed — durability weakened");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to open parent dir for WAL fsync — durability weakened");
                    }
                }
            }
        }

        // Open WAL for new appends (clean file, no torn tail).
        //
        // `Wal::open` acquires LOCK_EX|LOCK_NB on the fd. If a previous
        // daemon is still alive (handoff bug), the lock is held and we
        // get WouldBlock — propagate as Locked so the caller (successor
        // process during handoff) knows to abort cleanly. For any other
        // error (corrupt file etc.) the existing retry-with-remove path
        // is preserved.
        let wal = match Wal::open(&wal_path) {
            Ok(w) => w,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(CacheError::Locked(wal_path.clone()));
            }
            Err(e) => {
                warn!(error = %e, "failed to open WAL, removing and creating new");
                let _ = std::fs::remove_file(&wal_path);
                Wal::open(&wal_path).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        CacheError::Locked(wal_path.clone())
                    } else {
                        CacheError::Io(e)
                    }
                })?
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
        if flushing_path.exists() && in_successor_passive_mode {
            // **Handoff successor passive mode**: the flushing file
            // belongs to the still-running predecessor's in-flight
            // S3 upload. We must NOT recover from it here — recovery
            // would:
            //   1. pwrite old block data from the flushing file back
            //      to the active data file, overwriting any new
            //      writes the predecessor has applied since rotation
            //      (visible to fio as "verify: bad magic header 0"),
            //   2. CAS SYNCING→DIRTY in our state_map while the
            //      predecessor's state_map still shows SYNCING,
            //   3. remove the flushing file path while the
            //      predecessor's open fd still depends on the inode
            //      for its in-flight upload.
            //
            // Leave the flushing file alone. After cutover the
            // successor calls `recover_pending_flush_file` (in
            // `run_server_as_successor`) which re-checks the file:
            // if the predecessor finished its flush before exiting,
            // the file is gone and we have nothing to do; if not,
            // we recover normally.
            info!(
                rotation_seq,
                "found flushing file — deferring recovery until after handoff cutover (passive mode)"
            );
        } else if flushing_path.exists() {
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
            // Default codec: zstd-1, or GLIDEFS_COMPRESSION_LEVEL if set. bless
            // overrides to a high level via WriteCache::set_compression_level.
            compression_level: std::sync::atomic::AtomicI32::new(
                crate::block::block_map::env_default_compression_level(),
            ),
            readahead_window_bytes: std::sync::atomic::AtomicU32::new(
                super::inner::DEFAULT_READAHEAD_WINDOW_BYTES,
            ),
            data_file: Arc::new(parking_lot::RwLock::new(data_file)),
            flushing_file: parking_lot::Mutex::new(None),
            state_map,
            promote_claim: super::inner::PromoteClaimBitmap::new(),
            num_blocks,
            dirty_block_count: AtomicU64::new(dirty_count as u64),
            syncing_block_count: AtomicU64::new(0),
            sequence,
            wal,
            export_name,
            zero_block_hash: zbh,
            zero_block_bytes: zbb,
            recovery_warnings: AtomicU64::new(recovery_warning_count),
            // Default to `Warming` if `GLIDEFS_HANDOFF_SUCCESSOR_PASSIVE=1`
            // is set in the env. The handoff successor sets it before
            // calling `build_router_only` so every WriteCache is born
            // in passive mode (no flushing, no checkpoint truncate)
            // until takeover completes. Without this, the per-export
            // flush_scheduler — started inside `create_export` BEFORE
            // `set_all_caches_phase(Warming)` runs in `run_server_as_successor`
            // — could fire a rotation during the brief window and break
            // cross-process file-handle sharing with the predecessor.
            handoff_phase: std::sync::atomic::AtomicU8::new(
                if std::env::var("GLIDEFS_HANDOFF_SUCCESSOR_PASSIVE")
                    .map(|v| v == "1")
                    .unwrap_or(false)
                {
                    super::inner::HandoffPhase::Warming as u8
                } else {
                    super::inner::HandoffPhase::Idle as u8
                },
            ),

            flush_lock: tokio::sync::Mutex::new(()),
            manifest_etag: parking_lot::Mutex::new(None),
            current_generation: parking_lot::Mutex::new(0),
            current_lease_revision: parking_lot::Mutex::new(0),
            fenced: std::sync::atomic::AtomicBool::new(false),
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
        let wal_path = config.wal_path();
        let wal = Wal::open(&wal_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                CacheError::Locked(wal_path.clone())
            } else {
                CacheError::Io(e)
            }
        })?;
        let export_name = config.device_name.clone();
        let (zbb, zbh) = shared_zero_block(block_size);

        let inner = Arc::new(CacheInner {
            config,
            // Default codec: zstd-1, or GLIDEFS_COMPRESSION_LEVEL if set. bless
            // overrides to a high level via WriteCache::set_compression_level.
            compression_level: std::sync::atomic::AtomicI32::new(
                crate::block::block_map::env_default_compression_level(),
            ),
            readahead_window_bytes: std::sync::atomic::AtomicU32::new(
                super::inner::DEFAULT_READAHEAD_WINDOW_BYTES,
            ),
            data_file: Arc::new(parking_lot::RwLock::new(data_file)),
            flushing_file: parking_lot::Mutex::new(None),
            state_map,
            promote_claim: super::inner::PromoteClaimBitmap::new(),
            num_blocks,
            dirty_block_count: AtomicU64::new(0),
            syncing_block_count: AtomicU64::new(0),
            sequence,
            wal,
            export_name,
            zero_block_hash: zbh,
            zero_block_bytes: zbb,
            recovery_warnings: AtomicU64::new(0),
            // Default to `Warming` if `GLIDEFS_HANDOFF_SUCCESSOR_PASSIVE=1`
            // is set in the env. See the matching block in `open` for
            // the full rationale.
            handoff_phase: std::sync::atomic::AtomicU8::new(
                if std::env::var("GLIDEFS_HANDOFF_SUCCESSOR_PASSIVE")
                    .map(|v| v == "1")
                    .unwrap_or(false)
                {
                    super::inner::HandoffPhase::Warming as u8
                } else {
                    super::inner::HandoffPhase::Idle as u8
                },
            ),

            flush_lock: tokio::sync::Mutex::new(()),
            manifest_etag: parking_lot::Mutex::new(None),
            current_generation: parking_lot::Mutex::new(0),
            current_lease_revision: parking_lot::Mutex::new(0),
            fenced: std::sync::atomic::AtomicBool::new(false),
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
