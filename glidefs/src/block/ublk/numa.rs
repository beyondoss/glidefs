//! NUMA topology helpers for the ublk worker pool.
//!
//! Two abstractions:
//!
//! - [`NodeTopology`] — trait describing the NUMA layout of the host
//!   (how many nodes, which CPUs each owns). The pool uses this to
//!   partition workers across nodes and pin each worker to a CPU
//!   within its node.
//!
//! - [`numa_node_for_path`] — best-effort helper that resolves a file
//!   path to the NUMA node of its backing block device. The router
//!   uses this so each device's queues are placed on workers local
//!   to the NVMe that backs its data file.
//!
//! Both paths have graceful fallbacks. On a single-socket box
//! [`SysfsTopology`] reports one node containing every CPU; on a box
//! whose NVMe reports `numa_node = -1` (no preference — common on
//! virtualized hosts, ZFS pools, or this single-node test box)
//! [`numa_node_for_path`] returns `None`. Either case collapses to
//! "place anywhere," which is the pre-NUMA behavior.
//!
//! Probed once at startup. Topology does not change without a reboot,
//! so there's no point reloading at runtime.
//!
//! # Resolution at a glance
//!
//! ```text
//!  cache_dir/foo.cache   ── stat() ──►   st_dev = (major, minor)
//!                                              │
//!                                              ▼
//!  /sys/dev/block/<maj>:<min>  ── canonicalize / walk up ──►
//!       <sysfs block dir>/device/numa_node   ── read i32 ──►   0 / 1 / -1
//! ```

use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// CPUs owned by one NUMA node, in ascending order.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    /// Kernel-assigned node id (matches what
    /// `/sys/devices/system/node/nodeN` reports — *not* a dense index).
    pub id: usize,
    pub cpus: Vec<usize>,
}

/// Read-only view of the host's NUMA topology. The pool holds one
/// `Arc<dyn NodeTopology>` for its lifetime; implementations cache
/// the probe internally.
pub trait NodeTopology: Send + Sync {
    /// All online NUMA nodes, sorted by `id`. Always at least one
    /// entry — a single-socket box returns one node containing every
    /// online CPU.
    fn nodes(&self) -> &[NodeInfo];

    /// Position of the node with id `node_id` in [`Self::nodes`], or
    /// `None` if the kernel reports a node id we didn't probe.
    fn index_of(&self, node_id: usize) -> Option<usize> {
        self.nodes().iter().position(|n| n.id == node_id)
    }
}

/// Sysfs-backed topology probe. Reads `/sys/devices/system/node/online`
/// + each node's `cpulist` once at construction and caches the result.
pub struct SysfsTopology {
    nodes: Vec<NodeInfo>,
}

impl SysfsTopology {
    /// Probe the kernel's NUMA topology. Falls back to a single-node
    /// view containing every online CPU if any sysfs read fails —
    /// degraded but always usable (matches pre-NUMA behavior on
    /// machines without proper sysfs).
    pub fn detect() -> Self {
        match Self::detect_inner() {
            Ok(nodes) if !nodes.is_empty() => {
                tracing::debug!(
                    nodes = nodes.len(),
                    "NUMA topology probed"
                );
                Self { nodes }
            }
            Ok(_) => {
                tracing::warn!("NUMA probe returned zero nodes; using single-node fallback");
                Self { nodes: single_node_fallback() }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "NUMA probe failed; using single-node fallback"
                );
                Self { nodes: single_node_fallback() }
            }
        }
    }

    fn detect_inner() -> io::Result<Vec<NodeInfo>> {
        let online = fs::read_to_string("/sys/devices/system/node/online")?;
        let ids = parse_int_list(&online);
        let mut nodes = Vec::with_capacity(ids.len());
        for id in ids {
            let cpulist_path =
                format!("/sys/devices/system/node/node{id}/cpulist");
            let cpulist = fs::read_to_string(&cpulist_path)?;
            let cpus = parse_int_list(&cpulist);
            if !cpus.is_empty() {
                nodes.push(NodeInfo { id, cpus });
            }
        }
        Ok(nodes)
    }
}

impl NodeTopology for SysfsTopology {
    fn nodes(&self) -> &[NodeInfo] {
        &self.nodes
    }
}

fn single_node_fallback() -> Vec<NodeInfo> {
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    vec![NodeInfo { id: 0, cpus: (0..num_cpus).collect() }]
}

/// Test-only injectable topology. Workers will pin to the listed CPUs,
/// so when the test runs on a box where those CPUs don't exist the
/// real `sched_setaffinity` call may fail. That's an acceptable test
/// degradation — the partitioning logic still runs.
pub struct FakeTopology {
    nodes: Vec<NodeInfo>,
}

impl FakeTopology {
    pub fn single_node(num_cpus: usize) -> Self {
        Self {
            nodes: vec![NodeInfo { id: 0, cpus: (0..num_cpus).collect() }],
        }
    }

    pub fn two_node(cpus_node_0: usize, cpus_node_1: usize) -> Self {
        Self {
            nodes: vec![
                NodeInfo { id: 0, cpus: (0..cpus_node_0).collect() },
                NodeInfo {
                    id: 1,
                    cpus: (cpus_node_0..cpus_node_0 + cpus_node_1).collect(),
                },
            ],
        }
    }
}

impl NodeTopology for FakeTopology {
    fn nodes(&self) -> &[NodeInfo] {
        &self.nodes
    }
}

/// Resolve a filesystem path to the NUMA node of its backing block
/// device. Returns `None` if the path doesn't resolve to a block
/// device, the device reports `numa_node = -1` (no preference), or
/// any sysfs read fails.
///
/// Walks up the sysfs hierarchy because partitions (`nvme0n1p1`) and
/// whole disks (`nvme0n1`) expose `device/numa_node` at different
/// levels — the partition has its parent's `device` dir one level up.
pub fn numa_node_for_path(path: &Path) -> Option<usize> {
    let meta = fs::metadata(path).ok()?;
    let dev = meta.dev();
    let major = libc::major(dev);
    let minor = libc::minor(dev);
    let sysfs_block = PathBuf::from(format!("/sys/dev/block/{major}:{minor}"));
    let canonical = fs::canonicalize(&sysfs_block).ok()?;

    // Try the device dir at this level, then up to 3 levels of parent
    // (covers whole disk → partition → potentially nested mappings
    // like loop/dm/lvm). Once we find a `device/numa_node` file, stop.
    let mut cur: Option<&Path> = Some(canonical.as_path());
    for _ in 0..4 {
        let dir = cur?;
        let numa_file = dir.join("device").join("numa_node");
        if let Ok(s) = fs::read_to_string(&numa_file) {
            let parsed: Result<i32, _> = s.trim().parse();
            return match parsed {
                Ok(n) if n >= 0 => Some(n as usize),
                _ => None,
            };
        }
        cur = dir.parent();
    }
    None
}

/// Parse a kernel int-list like `"0-3,8,12-15"` into a sorted, deduped
/// vector. Whitespace tolerant; ignores unparseable fragments rather
/// than failing (sysfs is well-formed in practice).
fn parse_int_list(s: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in s.trim().split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((a, b)) = part.split_once('-') {
            if let (Ok(a), Ok(b)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>()) {
                for c in a..=b {
                    out.push(c);
                }
            }
        } else if let Ok(c) = part.parse::<usize>() {
            out.push(c);
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_int_list_handles_ranges_and_singletons() {
        assert_eq!(parse_int_list("0"), vec![0]);
        assert_eq!(parse_int_list("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(parse_int_list("0,2,4"), vec![0, 2, 4]);
        assert_eq!(parse_int_list("0-1,4-5"), vec![0, 1, 4, 5]);
        assert_eq!(parse_int_list(" 0-2 , 8 \n"), vec![0, 1, 2, 8]);
    }

    #[test]
    fn parse_int_list_dedups() {
        assert_eq!(parse_int_list("0,0,1-2,2-3"), vec![0, 1, 2, 3]);
    }

    #[test]
    fn parse_int_list_skips_garbage() {
        // Sysfs files don't contain garbage in practice, but make sure
        // a partial read or weird input doesn't panic.
        assert_eq!(parse_int_list("0,abc,2"), vec![0, 2]);
        assert_eq!(parse_int_list(""), Vec::<usize>::new());
    }

    #[test]
    fn sysfs_topology_returns_at_least_one_node() {
        // On every Linux machine /sys/devices/system/node/online exists
        // and reports at least node 0. On this single-socket test box
        // it returns "0"; on a 2-socket AWS metal box "0-1".
        let topo = SysfsTopology::detect();
        assert!(!topo.nodes().is_empty(), "must produce at least one node");
        for node in topo.nodes() {
            assert!(!node.cpus.is_empty(), "node {} has no CPUs", node.id);
        }
    }

    #[test]
    fn sysfs_topology_node_zero_present() {
        // Convention: node 0 is always present on Linux. If the probe
        // somehow elides it the worker pool would partition oddly.
        let topo = SysfsTopology::detect();
        assert!(
            topo.nodes().iter().any(|n| n.id == 0),
            "node 0 missing from probe"
        );
    }

    #[test]
    fn fake_topology_two_node_layout() {
        let topo = FakeTopology::two_node(2, 3);
        assert_eq!(topo.nodes().len(), 2);
        assert_eq!(topo.nodes()[0].id, 0);
        assert_eq!(topo.nodes()[0].cpus, vec![0, 1]);
        assert_eq!(topo.nodes()[1].id, 1);
        assert_eq!(topo.nodes()[1].cpus, vec![2, 3, 4]);
        assert_eq!(topo.index_of(0), Some(0));
        assert_eq!(topo.index_of(1), Some(1));
        assert_eq!(topo.index_of(2), None);
    }

    #[test]
    fn fake_topology_single_node() {
        let topo = FakeTopology::single_node(4);
        assert_eq!(topo.nodes().len(), 1);
        assert_eq!(topo.nodes()[0].cpus, vec![0, 1, 2, 3]);
    }

    #[test]
    fn numa_node_for_tmp_returns_none_or_some() {
        // On this single-socket test box `/tmp`'s NVMe reports
        // `numa_node = -1` → returns `None`. On a 2-socket box with
        // socket-attached NVMe it returns `Some(0)` or `Some(1)`.
        // Either is correct — we just want to confirm no panic and
        // a clean handling of `-1`.
        let _ = numa_node_for_path(Path::new("/tmp"));
        // Path that doesn't exist must return None, not panic.
        assert_eq!(
            numa_node_for_path(Path::new("/does/not/exist/anywhere")),
            None
        );
    }
}
