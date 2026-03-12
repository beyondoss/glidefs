use bytes::Bytes;
use dashmap::DashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tracing::{error, info, instrument, warn};

use crate::block::block_map::{
    SequenceNumber, SparseBlockState, SparseCrcMap, SparseStateMap, zero_block_hash,
};
use crate::block::state::{Active, Initializing, Recovering};
use crate::block::wal::Wal;

use super::inner::CacheInner;
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
        let (state_map, mut dirty_count, persisted_max_seq) =
            CacheInner::load_metadata(&config)?;
        let present_count = state_map.count_present();

        let wal_path = config.wal_path();
        let export_name = config.device_name.clone();

        // Track recovery issues for metrics
        let mut recovery_warning_count: u64 = 0;

        // Replay WAL entries after persisted sequence
        let wal_entries = match Wal::replay(&wal_path, persisted_max_seq) {
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
                error!(error = %e, "WAL replay failed — dirty blocks since last checkpoint may be lost");
                recovery_warning_count += 1;
                vec![]
            }
        };

        // Apply WAL entries: mark blocks dirty in SparseStateMap.
        // WAL stores metadata only — block data lives on the SSD cache file
        // (which was pwrite'd before the WAL entry was appended).
        let mut max_wal_seq = persisted_max_seq;

        // Partial block bitmaps reconstructed from partial WAL entries.
        let partial_blocks: DashMap<usize, super::inner::PartialBlockState> = DashMap::new();

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

                // Reconstruct partial block bitmap from partial WAL entries
                if let Some(bitmap) = entry.partial_bitmap {
                    let entry = partial_blocks
                        .entry(block_index)
                        .or_insert_with(|| super::inner::PartialBlockState {
                            bitmap: std::sync::atomic::AtomicU32::new(0),
                            write_lock: parking_lot::Mutex::new(()),
                        });
                    entry.value().bitmap.fetch_or(bitmap, std::sync::atomic::Ordering::Release);
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
        {
            use crate::block::wal::{serialize_entry, serialize_partial_entry};
            let mut buf = Vec::new();
            for entry in &wal_entries {
                if let Some(bitmap) = entry.partial_bitmap {
                    serialize_partial_entry(&mut buf, entry.block_index, entry.sequence, bitmap);
                } else {
                    serialize_entry(&mut buf, entry.block_index, entry.sequence);
                }
            }
            std::fs::write(&wal_path, &buf)?;
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
        let zbh = zero_block_hash(block_size);
        let zbb = Bytes::from(vec![0u8; block_size]);

        let partial_count = partial_blocks.len();
        let inner = Arc::new(CacheInner {
            config,
            data_file,
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
            crc_map: SparseCrcMap::new(num_blocks),
            flush_lock: tokio::sync::Mutex::new(()),
            partial_blocks,
            manifest_etag: parking_lot::Mutex::new(None),
        });

        info!(
            dirty_blocks = dirty_count,
            present_blocks = present_count,
            partial_blocks = partial_count,
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
        let zbh = zero_block_hash(block_size);
        let zbb = Bytes::from(vec![0u8; block_size]);

        let inner = Arc::new(CacheInner {
            config,
            data_file,
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
            crc_map: SparseCrcMap::new(num_blocks),
            flush_lock: tokio::sync::Mutex::new(()),
            partial_blocks: DashMap::new(),
            manifest_etag: parking_lot::Mutex::new(None),
        });

        info!("cache opened fresh for fork, directly Active");

        Ok(WriteCache {
            inner,
            _state: PhantomData,
        })
    }

}
