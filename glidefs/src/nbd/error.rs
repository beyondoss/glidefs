use super::write_cache::CacheError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NBDError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Device not found: {}", String::from_utf8_lossy(.0))]
    DeviceNotFound(Vec<u8>),

    #[error("Client does not support required features")]
    IncompatibleClient,

    #[error("Deku parsing error: {0}")]
    Deku(#[from] deku::DekuError),

    #[error("Cache error: {0}")]
    Cache(#[from] CacheError),
}

pub type Result<T> = std::result::Result<T, NBDError>;

/// Error type for block I/O command handlers.
///
/// Transport-agnostic: each transport maps these to its own error codes.
#[derive(Debug, Clone, Copy)]
pub enum CommandError {
    /// Invalid argument (EINVAL)
    InvalidArgument,
    /// I/O error (EIO)
    IoError,
    /// No space left (ENOSPC)
    NoSpace,
    /// Read-only device (EROFS)
    ReadOnly,
}

impl CommandError {
    /// Map to NBD wire format errno (used by NBD server).
    pub fn to_nbd_errno(self) -> u32 {
        match self {
            CommandError::InvalidArgument => super::protocol::NBD_EINVAL,
            CommandError::IoError => super::protocol::NBD_EIO,
            CommandError::NoSpace => super::protocol::NBD_ENOSPC,
            CommandError::ReadOnly => super::protocol::NBD_EROFS,
        }
    }

    /// Map to Linux errno (used by ublk server).
    #[cfg(target_os = "linux")]
    pub fn to_linux_errno(self) -> i32 {
        match self {
            CommandError::InvalidArgument => libc::EINVAL,
            CommandError::IoError => libc::EIO,
            CommandError::NoSpace => libc::ENOSPC,
            CommandError::ReadOnly => libc::EROFS,
        }
    }
}

impl From<CacheError> for CommandError {
    fn from(err: CacheError) -> Self {
        match &err {
            CacheError::Io(io_err) if io_err.kind() == std::io::ErrorKind::StorageFull => {
                tracing::error!(error = %err, "local SSD full (ENOSPC)");
                CommandError::NoSpace
            }
            _ => {
                tracing::warn!(error = %err, "cache error mapped to EIO");
                CommandError::IoError
            }
        }
    }
}

pub type CommandResult<T> = std::result::Result<T, CommandError>;
