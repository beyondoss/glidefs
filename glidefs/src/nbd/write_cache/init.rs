use bytes::Bytes;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use tracing::{info, instrument, warn};

use crate::nbd::block_map::{
    AtomicBlockMap, BlockMap, BlockMapEntry, BlockMapKind, ForkedBlockMap,
    SequenceNumber, SparseBlockState, SparseStateMap, blake3_128, zero_block_hash,
};
use crate::nbd::manifest::Manifest;
use crate::nbd::state::{Active, Initializing, Recovering};
use crate::nbd::wal::Wal;

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

        // Open or create data file with O_DIRECT for sync worker reads
        // This prevents sync worker from polluting/contending with the page cache
        let data_path = config.data_path();
        let data_file = super::inner::SyncFile::open(&data_path, true, config.device_size)?;

        // Load block states (or create fresh)
        let num_blocks = config.num_blocks();
        let (state_map, mut dirty_count) = CacheInner::load_metadata(&config)?;
        let present_count = state_map.count_present();

        // === v2 initialization ===
        let block_map_path = config.block_map_path();
        let wal_path = config.wal_path();
        let export_name = config.device_name.clone();
        let chunk_size = config.block_size as u32;

        // Load persisted block map (or create empty)
        let mut persisted_bm = if block_map_path.exists() {
            match BlockMap::load_from_file(&block_map_path) {
                Ok(bm) => {
                    info!(path = %block_map_path.display(), entries = bm.non_empty_count(), "loaded v2 block map");
                    bm
                }
                Err(e) => {
                    warn!(error = %e, "failed to load v2 block map, starting fresh");
                    BlockMap::new(config.device_size, chunk_size)
                }
            }
        } else {
            BlockMap::new(config.device_size, chunk_size)
        };

        let persisted_max_seq = persisted_bm.max_sequence();

        // Replay WAL entries after persisted sequence
        let wal_entries = match Wal::replay(&wal_path, persisted_max_seq) {
            Ok(entries) => {
                if !entries.is_empty() {
                    info!(entries = entries.len(), min_seq = persisted_max_seq + 1, "replaying WAL entries");
                }
                entries
            }
            Err(e) => {
                warn!(error = %e, "WAL replay failed, continuing with persisted block map");
                vec![]
            }
        };

        // Apply WAL entries to block map.
        // WAL stores metadata only — block data lives on the SSD cache file
        // (which was pwrite'd before the WAL entry was appended).
        let mut max_wal_seq = persisted_max_seq;
        let chunk_size_u64 = chunk_size as u64;

        for entry in &wal_entries {
            if entry.name != export_name {
                continue; // Skip entries for other exports (shouldn't happen with per-export WAL)
            }
            let chunk_index = entry.chunk_index as usize;

            // Re-read block data from SSD and re-hash for consistency.
            // The SSD may have been overwritten by a later write that didn't
            // make it to the WAL, so we trust the SSD state over the WAL hash.
            let chunk_offset = entry.chunk_index * chunk_size_u64;
            let valid_bytes = std::cmp::min(chunk_size_u64, config.device_size.saturating_sub(chunk_offset)) as usize;
            let mut chunk_buf = vec![0u8; chunk_size as usize];
            if valid_bytes > 0
                && let Err(e) = data_file.read_exact_at(&mut chunk_buf[..valid_bytes], chunk_offset)
            {
                warn!(chunk_index, error = %e, "WAL recovery: SSD read failed, skipping entry (block reverts to last checkpoint state)");
                continue;
            }

            let hash = blake3_128(&chunk_buf);
            persisted_bm.set(chunk_index, BlockMapEntry {
                hash,
                flags: BlockMapEntry::FLAG_DIRTY,
                sequence: entry.sequence,
            });
            max_wal_seq = max_wal_seq.max(entry.sequence);

            // Synchronize state_map with WAL replay: mark as Dirty so
            // that flush's targeted scan sees the correct dirty state.
            if chunk_index < num_blocks {
                let old = state_map.get(chunk_index);
                if old != SparseBlockState::DIRTY {
                    // Ensure block is present first (allocates page if needed)
                    let _ = state_map.set_present(chunk_index);
                    // Transition to Dirty from whatever current state is
                    let current = state_map.get(chunk_index);
                    if current != SparseBlockState::DIRTY {
                        let _ = state_map.cas(chunk_index, current, SparseBlockState::DIRTY);
                    }
                    dirty_count += 1;
                }
            }
        }

        // Build AtomicBlockMap from the merged block map
        let atomic_block_map = AtomicBlockMap::from_block_map(&persisted_bm);

        // Initialize sequence counter
        let initial_seq = persisted_max_seq.max(max_wal_seq);
        let sequence = SequenceNumber::new(initial_seq);

        // Open WAL for new appends
        let wal = match Wal::open(&wal_path) {
            Ok(w) => w,
            Err(e) => {
                warn!(error = %e, "failed to open WAL, creating new");
                Wal::open(&wal_path).map_err(CacheError::Io)?
            }
        };

        let block_size = config.block_size;
        let zbh = zero_block_hash(block_size);
        let zbb = Bytes::from(vec![0u8; block_size]);

        let inner = Arc::new(CacheInner {
            config,
            data_file,
            state_map,
            num_blocks,
            dirty_block_count: AtomicU64::new(dirty_count as u64),
            syncing_block_count: AtomicU64::new(0),
            block_map: RwLock::new(BlockMapKind::Full(atomic_block_map)),
            sequence,
            wal: Mutex::new(wal),
            export_name,
            zero_block_hash: zbh,
            zero_block_bytes: zbb,
        });

        info!(
            dirty_blocks = dirty_count,
            present_blocks = present_count,
            "cache opened, transitioning to Recovering"
        );

        Ok(WriteCache {
            inner,
            _state: PhantomData,
        })
    }

    /// Create a write cache from a manifest (for forked VMs).
    ///
    /// Unlike `open()`, this skips WAL replay and local metadata loading.
    /// All block data is in S3, so the local cache starts empty.
    /// Goes directly to Active state (nothing to recover).
    pub fn open_from_manifest(
        config: WriteCacheConfig,
        manifest: &Manifest,
        parent_block_map: Option<Arc<BlockMap>>,
    ) -> Result<WriteCache<Active>, CacheError> {
        config.validate()?;
        std::fs::create_dir_all(&config.cache_dir)?;

        let data_file = super::inner::SyncFile::open(&config.data_path(), true, config.device_size)?;

        let num_blocks = config.num_blocks();

        // Fresh state map: all NOT_PRESENT (data is in S3, not local)
        let state_map = SparseStateMap::new(num_blocks);

        // Build BlockMapKind from manifest or parent
        let block_map_kind = if let Some(parent) = parent_block_map {
            BlockMapKind::Forked(ForkedBlockMap::new(parent))
        } else {
            let mut bm = BlockMap::new(config.device_size, config.block_size as u32);
            for entry in &manifest.block_map {
                bm.set(
                    entry.chunk_index as usize,
                    BlockMapEntry {
                        hash: entry.hash,
                        flags: 0,
                        sequence: manifest.sequence,
                    },
                );
            }
            BlockMapKind::Full(AtomicBlockMap::from_block_map(&bm))
        };

        let sequence = SequenceNumber::new(manifest.sequence);
        let wal = Wal::open(&config.wal_path())?;
        let export_name = config.device_name.clone();
        let block_size = config.block_size;
        let zbh = zero_block_hash(block_size);
        let zbb = Bytes::from(vec![0u8; block_size]);

        let inner = Arc::new(CacheInner {
            config,
            data_file,
            state_map,
            num_blocks,
            dirty_block_count: AtomicU64::new(0),
            syncing_block_count: AtomicU64::new(0),
            block_map: RwLock::new(block_map_kind),
            sequence,
            wal: Mutex::new(wal),
            export_name,
            zero_block_hash: zbh,
            zero_block_bytes: zbb,
        });

        info!("cache opened from manifest, directly Active");

        Ok(WriteCache {
            inner,
            _state: PhantomData,
        })
    }
}
