use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashSet;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use tracing::warn;

// Note: Block-level compression is intentionally NOT implemented.
// ZFS handles compression at its layer, and block-level compression would:
// 1. Interfere with ZFS's own compression
// 2. Break the fixed block size assumption (compressed blocks vary in size)
// 3. Add CPU overhead with minimal benefit since ZFS already compresses

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub cache: CacheConfig,
    pub storage: StorageConfig,
    pub servers: ServerConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gcp: Option<GcsConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(deserialize_with = "deserialize_expandable_path")]
    pub dir: PathBuf,
    pub disk_size_gb: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_size_gb: Option<f64>,
    /// SSD tier capacity for foyer clean cache in GB (default: 10GB).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssd_cache_size_gb: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub url: String,
    /// S3 connection timeout in seconds (default: 10).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub connect_timeout_secs: Option<u64>,
    /// S3 request timeout in seconds (default: 300).
    /// Set high to accommodate large pack uploads.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub request_timeout_secs: Option<u64>,
}

impl StorageConfig {
    pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
    pub const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 300;

    pub fn connect_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.connect_timeout_secs
                .unwrap_or(Self::DEFAULT_CONNECT_TIMEOUT_SECS),
        )
    }

    pub fn request_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.request_timeout_secs
                .unwrap_or(Self::DEFAULT_REQUEST_TIMEOUT_SECS),
        )
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbd: Option<NbdConfig>,

    /// ublk transport config (Linux 6.0+, io_uring-based userspace block device).
    /// Exports with `transport = "ublk"` require this section.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ublk: Option<UblkConfig>,
}

/// Configuration for the ublk transport (Linux 6.0+).
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct UblkConfig {
    /// Number of I/O queues per device (default: 1).
    /// Higher values improve parallelism on multi-core hosts.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub nr_queues: Option<u16>,
}

impl UblkConfig {
    #[allow(dead_code)]
    pub const DEFAULT_NR_QUEUES: u16 = 1;

    #[allow(dead_code)]
    pub fn nr_queues(&self) -> u16 {
        self.nr_queues.unwrap_or(Self::DEFAULT_NR_QUEUES)
    }
}

/// NBD server configuration for block device access.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NbdConfig {
    /// TCP addresses to listen on (e.g., "127.0.0.1:10809")
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub addresses: Option<HashSet<SocketAddr>>,

    /// Unix socket path for local connections
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,

    /// HTTP API address for dynamic export management (e.g., "127.0.0.1:8080")
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub api_address: Option<SocketAddr>,

    /// Block size in bytes (default: 128KB to match ZFS recordsize)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub block_size: Option<usize>,

    /// DEPRECATED: Was used by the legacy S3BlockStore batch path.
    /// Kept for backward compatibility with existing config files (deny_unknown_fields).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub blocks_per_batch: Option<u64>,

    /// Static exports loaded at startup
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exports: Vec<ExportConfig>,

    // Legacy single-device fields (for backward compatibility)
    /// Name of the NBD device (DEPRECATED: use exports array instead)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub device_name: Option<String>,

    /// Size of the block device in gigabytes (DEPRECATED: use exports array instead)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub device_size_gb: Option<f64>,

    /// Background scrubber rate: blocks verified per second (default: 1000, 0 = disabled).
    /// Periodically re-hashes cached blocks to detect silent corruption.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub scrubber_blocks_per_second: Option<u64>,

    /// Whether to fsync the WAL after each write batch (default: false).
    /// When false, the WAL uses OS buffer flush only (~20µs writes).
    /// Set true on SSDs without power-loss protection to guarantee
    /// WAL durability at the cost of ~10ms per write batch.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub wal_sync: Option<bool>,

    /// Graceful shutdown timeout in seconds (default: 30).
    /// Exports are drained to S3 within this window; if exceeded, the process
    /// exits with a warning. Set higher for large caches with many dirty blocks.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub shutdown_timeout_secs: Option<u64>,

    /// Max concurrent S3 pack uploads across all exports (default: 128, 0 = unlimited).
    /// Bounds host-level S3 upload concurrency to prevent connection exhaustion.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_s3_uploads: Option<usize>,

    /// Max concurrent S3 pack downloads across all exports (default: 512, 0 = unlimited).
    /// Bounds host-level S3 read concurrency on the NBD read path.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_s3_downloads: Option<usize>,

    /// Default blocks per S3 pack for new exports (default: 500).
    /// Higher values reduce S3 PUT costs but increase zombie storage.
    /// Per-export override available via exports config or API.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub flush_threshold: Option<usize>,

    /// Number of ublk I/O queues (default: 1). Only used with transport = "ublk".
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ublk_nr_queues: Option<u16>,

    /// NBD kernel device dead connection timeout in seconds (default: 30).
    /// Controls how long the kernel queues I/O when the socket disconnects
    /// (e.g., during a binary upgrade). Set to 0 to disable.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub nbd_dead_conn_timeout: Option<u32>,
}

/// Configuration for a single NBD export (virtual block device).
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExportConfig {
    /// Export name (used by NBD client: nbd-client -N <name>)
    pub name: String,

    /// Device size in gigabytes
    pub size_gb: f64,

    /// S3 prefix for this export's blocks (default: derived from name)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub s3_prefix: Option<String>,

    /// Block size in bytes (default: inherit from global nbd.block_size)
    /// Smaller blocks (16KB-32KB) reduce write amplification for random I/O.
    /// Larger blocks (256KB+) improve throughput for sequential I/O.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub block_size: Option<usize>,

    /// Blocks per S3 pack (default: inherit from global nbd.flush_threshold).
    /// 0 = manual flush mode (no auto-flush, drain/snapshot only).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub flush_threshold: Option<usize>,

    /// Flush mode: "auto" (default) or "manual" (drain-only, no auto S3 flush).
    /// When "manual", dirty blocks only flush on drain, snapshot, or shutdown.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub flush_mode: Option<String>,

    /// Block device transport: "nbd" (default) or "ublk" (Linux 6.0+, io_uring).
    /// ublk eliminates socket/protocol overhead for higher IOPS.
    /// Requires `[servers.ublk]` section when set to "ublk".
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub transport: Option<String>,

}

impl ExportConfig {
    /// Get the transport for this export: "nbd" (default) or "ublk".
    #[cfg_attr(not(all(target_os = "linux", feature = "ublk")), allow(dead_code))]
    pub fn transport(&self) -> &str {
        self.transport.as_deref().unwrap_or("nbd")
    }

    /// Get the S3 prefix for this export, defaulting to the export name.
    pub fn s3_prefix(&self) -> &str {
        self.s3_prefix.as_deref().unwrap_or(&self.name)
    }

    /// Get the device size in bytes (binary GiB).
    pub fn size_bytes(&self) -> u64 {
        (self.size_gb * 1024.0 * 1024.0 * 1024.0) as u64
    }

    /// Get the block size, falling back to the provided default.
    pub fn block_size_or(&self, default: usize) -> usize {
        self.block_size.unwrap_or(default)
    }

    /// Resolve flush_threshold: export override > global default > compile-time default.
    /// Returns 0 for manual flush mode (drain-only).
    pub fn flush_threshold_or(&self, global_default: usize) -> usize {
        if self.flush_mode.as_deref() == Some("manual") {
            return 0;
        }
        self.flush_threshold.unwrap_or(global_default)
    }
}

impl NbdConfig {
    pub const DEFAULT_BLOCK_SIZE: usize = 128 * 1024;
    pub const DEFAULT_DEVICE_SIZE_GB: f64 = 100.0;
    pub const DEFAULT_DEVICE_NAME: &'static str = "glidefs";

    pub fn block_size(&self) -> usize {
        self.block_size.unwrap_or(Self::DEFAULT_BLOCK_SIZE)
    }

    pub const DEFAULT_SCRUBBER_BPS: u64 = 0;

    /// Get the scrubber rate in blocks per second (default: 0 = disabled).
    /// Set to a positive value (e.g. 1000) to enable continuous background verification.
    pub fn scrubber_blocks_per_second(&self) -> u64 {
        self.scrubber_blocks_per_second
            .unwrap_or(Self::DEFAULT_SCRUBBER_BPS)
    }

    /// Whether to fsync the WAL after each write batch (default: false).
    pub fn wal_sync(&self) -> bool {
        self.wal_sync.unwrap_or(false)
    }

    pub const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

    /// Graceful shutdown timeout.
    pub fn shutdown_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.shutdown_timeout_secs
                .unwrap_or(Self::DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        )
    }

    pub const DEFAULT_MAX_S3_UPLOADS: usize = 128;

    /// Max concurrent S3 pack uploads across all exports (default: 128, 0 = unlimited).
    pub fn max_s3_uploads(&self) -> usize {
        self.max_s3_uploads.unwrap_or(Self::DEFAULT_MAX_S3_UPLOADS)
    }

    pub const DEFAULT_MAX_S3_DOWNLOADS: usize = 512;

    /// Max concurrent S3 pack downloads across all exports (default: 512, 0 = unlimited).
    pub fn max_s3_downloads(&self) -> usize {
        self.max_s3_downloads
            .unwrap_or(Self::DEFAULT_MAX_S3_DOWNLOADS)
    }

    /// Default blocks per pack for new exports.
    pub fn flush_threshold(&self) -> usize {
        self.flush_threshold
            .unwrap_or(crate::block::pack::DEFAULT_FLUSH_THRESHOLD)
    }

    /// Number of ublk I/O queues (default: 1).
    pub fn ublk_nr_queues(&self) -> u16 {
        self.ublk_nr_queues.unwrap_or(1)
    }

    /// NBD kernel device dead connection timeout in seconds (default: 30).
    pub fn nbd_dead_conn_timeout(&self) -> u32 {
        self.nbd_dead_conn_timeout.unwrap_or(30)
    }

    /// Get the list of exports, handling legacy single-device config.
    pub fn get_exports(&self) -> Vec<ExportConfig> {
        if !self.exports.is_empty() {
            return self.exports.clone();
        }

        // Legacy support: convert old device_name/device_size_gb to export
        if let (Some(name), Some(size_gb)) = (&self.device_name, self.device_size_gb) {
            return vec![ExportConfig {
                name: name.clone(),
                size_gb,
                s3_prefix: None,
                block_size: None,
                flush_threshold: None,
                flush_mode: None,
                transport: None,
            }];
        }

        // Default: single export with default values
        vec![ExportConfig {
            name: Self::DEFAULT_DEVICE_NAME.to_string(),
            size_gb: Self::DEFAULT_DEVICE_SIZE_GB,
            s3_prefix: None,
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
        }]
    }
}

fn default_nbd_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
        10809,
    ));
    set
}

fn default_api_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

#[derive(Debug, Serialize, Clone)]
pub struct AwsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AwsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AwsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct AzureConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AzureConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AzureConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct GcsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for GcsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(GcsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

fn deserialize_expandable_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(expanded.into_owned()),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_expandable_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_optional_expandable_path<'de, D>(
    deserializer: D,
) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    })
    .transpose()
}

fn deserialize_expandable_hashmap<'de, D>(
    deserializer: D,
) -> Result<std::collections::HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
    map.into_iter()
        .map(|(k, v)| match shellexpand::env(&v) {
            Ok(expanded) => Ok((k, expanded.into_owned())),
            Err(e) => Err(serde::de::Error::custom(format!(
                "Failed to expand environment variable: {}",
                e
            ))),
        })
        .collect()
}

impl Settings {
    pub fn from_file(config_path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = config_path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let settings: Settings = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        settings.validate()?;

        Ok(settings)
    }

    /// Validate configuration values at startup. Fails fast on nonsensical values.
    pub fn validate(&self) -> Result<()> {
        // Cache validation
        anyhow::ensure!(
            self.cache.disk_size_gb.is_finite() && self.cache.disk_size_gb > 0.0,
            "cache.disk_size_gb must be a finite number > 0, got {}",
            self.cache.disk_size_gb
        );
        if let Some(mem) = self.cache.memory_size_gb {
            anyhow::ensure!(
                mem.is_finite() && mem > 0.0,
                "cache.memory_size_gb must be a finite number > 0, got {}",
                mem
            );
        }
        if let Some(ssd) = self.cache.ssd_cache_size_gb {
            anyhow::ensure!(
                ssd.is_finite() && ssd > 0.0,
                "cache.ssd_cache_size_gb must be a finite number > 0, got {}",
                ssd
            );
        }

        // Storage timeout validation
        if let Some(t) = self.storage.connect_timeout_secs {
            anyhow::ensure!(t > 0, "storage.connect_timeout_secs must be > 0, got {}", t);
        }
        if let Some(t) = self.storage.request_timeout_secs {
            anyhow::ensure!(t > 0, "storage.request_timeout_secs must be > 0, got {}", t);
        }

        // NBD validation
        if let Some(nbd) = &self.servers.nbd {
            // Deprecated field warnings
            if nbd.blocks_per_batch.is_some() {
                warn!("config: 'blocks_per_batch' is deprecated and ignored");
            }
            if nbd.device_name.is_some() {
                warn!("config: 'device_name' is deprecated, use 'exports' array instead");
            }
            if nbd.device_size_gb.is_some() {
                warn!("config: 'device_size_gb' is deprecated, use 'exports' array instead");
            }

            if let Some(bs) = nbd.block_size {
                anyhow::ensure!(
                    bs.is_power_of_two() && (4096..=1_048_576).contains(&bs),
                    "block_size must be a power of 2 between 4096 and 1048576, got {}",
                    bs
                );
            }
            if let Some(t) = nbd.shutdown_timeout_secs {
                anyhow::ensure!(t > 0, "shutdown_timeout_secs must be > 0, got {}", t);
            }
            // Export validation
            let mut names = HashSet::new();
            for export in &nbd.exports {
                anyhow::ensure!(!export.name.is_empty(), "export name must not be empty");
                anyhow::ensure!(
                    export.name.len() <= 128
                        && export.name.starts_with(|c: char| c.is_ascii_alphanumeric())
                        && export
                            .name
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'),
                    "export '{}': name must be 1-128 chars, alphanumeric/hyphen/underscore/dot, starting with alphanumeric",
                    export.name
                );
                anyhow::ensure!(
                    names.insert(&export.name),
                    "duplicate export name: '{}'",
                    export.name
                );
                anyhow::ensure!(
                    export.size_gb.is_finite() && export.size_gb > 0.0,
                    "export '{}': size_gb must be a finite number > 0, got {}",
                    export.name,
                    export.size_gb
                );
                if let Some(ref fm) = export.flush_mode {
                    anyhow::ensure!(
                        fm == "auto" || fm == "manual",
                        "export '{}': flush_mode must be 'auto' or 'manual', got '{}'",
                        export.name,
                        fm
                    );
                }
                if let Some(ref t) = export.transport {
                    anyhow::ensure!(
                        t == "nbd" || t == "ublk",
                        "export '{}': transport must be 'nbd' or 'ublk', got '{}'",
                        export.name,
                        t
                    );
                    if t == "ublk" {
                        anyhow::ensure!(
                            self.servers.ublk.is_some(),
                            "export '{}': transport 'ublk' requires [servers.ublk] section",
                            export.name
                        );
                    }
                }
                if let Some(bs) = export.block_size {
                    anyhow::ensure!(
                        bs.is_power_of_two() && (4096..=1_048_576).contains(&bs),
                        "export '{}': block_size must be a power of 2 between 4096 and 1048576, got {}",
                        export.name,
                        bs
                    );
                }
            }
        }

        Ok(())
    }

    pub fn cloud_provider_env_vars(&self) -> Vec<(String, String)> {
        let mut env_vars = Vec::new();
        if let Some(aws) = &self.aws {
            for (k, v) in &aws.0 {
                env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(azure) = &self.azure {
            for (k, v) in &azure.0 {
                env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(gcp) = &self.gcp {
            for (k, v) in &gcp.0 {
                env_vars.push((format!("google_{}", k.to_lowercase()), v.clone()));
            }
        }
        env_vars
    }

    pub fn generate_default() -> Self {
        let mut aws_config = std::collections::HashMap::new();
        aws_config.insert(
            "access_key_id".to_string(),
            "${AWS_ACCESS_KEY_ID}".to_string(),
        );
        aws_config.insert(
            "secret_access_key".to_string(),
            "${AWS_SECRET_ACCESS_KEY}".to_string(),
        );

        Settings {
            cache: CacheConfig {
                dir: PathBuf::from("${HOME}/.cache/glidefs"),
                disk_size_gb: 10.0,
                memory_size_gb: Some(1.0),
                ssd_cache_size_gb: None,
            },
            storage: StorageConfig {
                url: "s3://your-bucket/glidefs-data".to_string(),
                connect_timeout_secs: None,
                request_timeout_secs: None,
            },
            servers: ServerConfig {
                nbd: Some(NbdConfig {
                    addresses: Some(default_nbd_addresses()),
                    unix_socket: Some(PathBuf::from("/tmp/glidefs.nbd.sock")),
                    api_address: Some(default_api_address()),
                    block_size: None,
                    blocks_per_batch: None,
                    exports: vec![ExportConfig {
                        name: "default".to_string(),
                        size_gb: 100.0,
                        s3_prefix: None,
                        block_size: None,
                        flush_threshold: None,
                        flush_mode: None,
                        transport: None,
                    }],
                    device_name: None,
                    device_size_gb: None,
                    flush_threshold: None,
                    scrubber_blocks_per_second: None,
                    wal_sync: None,
                    shutdown_timeout_secs: None,
                    max_s3_uploads: None,
                    max_s3_downloads: None,
                    ublk_nr_queues: None,
                    nbd_dead_conn_timeout: None,
                }),
                ublk: None,
            },
            aws: Some(AwsConfig(aws_config)),
            azure: None,
            gcp: None,
        }
    }

    pub fn write_default_config(path: impl AsRef<std::path::Path>) -> Result<()> {
        let default = Self::generate_default();
        let toml_string = toml::to_string_pretty(&default)?;

        let commented = format!(
            "# GlideFS Configuration File\n\
             # Generated by GlideFS v{}\n\
             #\n\
             # High-performance S3-backed block storage for ZFS\n\
             #\n\
             # Environment variables are supported: ${{VAR}} or $VAR\n\
             #\n\
             # NBD Server:\n\
             #   - FLUSH returns after local SSD fsync (<10ms)\n\
             #   - Background sync to S3 (continuous drain)\n\
             #   - Send SIGUSR1 to drain all dirty blocks (for VM migration)\n\
             #\n\
             \n{}",
            env!("CARGO_PKG_VERSION"),
            toml_string
        );

        fs::write(path, commented)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::NamedTempFile;

    #[test]
    fn test_env_var_expansion() {
        unsafe {
            env::set_var("GLIDEFS_TEST_BUCKET", "my-bucket");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://${GLIDEFS_TEST_BUCKET}/data"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(settings.storage.url, "s3://my-bucket/data");
    }

    #[test]
    fn test_home_env_var() {
        let home_dir = env::home_dir().expect("HOME not set");
        unsafe {
            env::set_var("GLIDEFS_TEST_HOME", home_dir.to_str().unwrap());
        }

        let config_content = r#"
[cache]
dir = "${GLIDEFS_TEST_HOME}/test-cache"
disk_size_gb = 1.0

[storage]
url = "file://${GLIDEFS_TEST_HOME}/data"

[servers]

[servers.nbd]
unix_socket = "${GLIDEFS_TEST_HOME}/glidefs.sock"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(settings.cache.dir, home_dir.join("test-cache"));
        assert_eq!(
            settings.storage.url,
            format!("file://{}/data", home_dir.display())
        );
        if let Some(nbd) = settings.servers.nbd {
            assert_eq!(nbd.unix_socket.unwrap(), home_dir.join("glidefs.sock"));
        } else {
            panic!("Expected NBD config");
        }
    }

    #[test]
    fn test_undefined_env_var_error() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://${GLIDEFS_TEST_UNDEFINED_VAR_THAT_SHOULD_NOT_EXIST}/data"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_aws_config_expansion() {
        unsafe {
            env::set_var("GLIDEFS_TEST_AWS_KEY", "aws123");
            env::set_var("GLIDEFS_TEST_AWS_SECRET", "aws_secret");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"

[servers]

[aws]
access_key_id = "${GLIDEFS_TEST_AWS_KEY}"
secret_access_key = "${GLIDEFS_TEST_AWS_SECRET}"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        let aws = settings.aws.unwrap();
        assert_eq!(aws.0.get("access_key_id").unwrap(), "aws123");
        assert_eq!(aws.0.get("secret_access_key").unwrap(), "aws_secret");
    }

    #[test]
    fn test_validation_rejects_bad_block_size() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"

[servers.nbd]
block_size = 7
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("block_size must be a power of 2"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validation_rejects_zero_disk_size() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 0.0

[storage]
url = "s3://bucket/data"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("disk_size_gb must be a finite number > 0"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validation_rejects_zero_export_size() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"

[servers.nbd]

[[servers.nbd.exports]]
name = "vol1"
size_gb = 0.0
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("size_gb must be a finite number > 0"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validation_rejects_empty_export_name() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"

[servers.nbd]

[[servers.nbd.exports]]
name = ""
size_gb = 10.0
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("export name must not be empty"), "got: {err}");
    }

    #[test]
    fn test_deny_unknown_fields() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0
bogus_field = true

[storage]
url = "s3://bucket/data"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err(), "unknown fields should be rejected");
    }

    #[test]
    fn test_missing_required_sections() {
        // Missing [storage] section entirely
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err(), "missing [storage] section should fail");
    }

    #[test]
    fn test_validation_rejects_zero_connect_timeout() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
connect_timeout_secs = 0

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("connect_timeout_secs must be > 0"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validation_rejects_duplicate_export_names() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"

[servers.nbd]

[[servers.nbd.exports]]
name = "vol1"
size_gb = 10.0

[[servers.nbd.exports]]
name = "vol1"
size_gb = 20.0
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate export name"), "got: {err}");
    }
}
