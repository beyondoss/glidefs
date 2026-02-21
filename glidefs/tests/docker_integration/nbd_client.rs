//! Lightweight async NBD TCP client for integration tests.
//!
//! Speaks the NBD wire protocol (fixed newstyle handshake + OPT_GO + transmission)
//! directly over TCP. No kernel NBD module needed — runs on macOS and Linux.

use anyhow::{Context, Result, bail};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Protocol constants from glidefs
use glidefs::block::protocol::*;

pub struct NbdClient {
    stream: TcpStream,
    cookie: AtomicU64,
    #[allow(dead_code)]
    pub export_size: u64,
}

impl NbdClient {
    /// Connect to an NBD server, perform handshake, and negotiate the export via OPT_GO.
    pub async fn connect(addr: SocketAddr, export_name: &str) -> Result<Self> {
        let mut stream = TcpStream::connect(addr)
            .await
            .context("failed to connect to NBD server")?;

        // --- Phase 1: Read server handshake (18 bytes) ---
        let magic = stream.read_u64().await?;
        let ihaveopt = stream.read_u64().await?;
        let _flags = stream.read_u16().await?;

        if magic != NBD_MAGIC {
            bail!("bad NBD magic: {:#x}", magic);
        }
        if ihaveopt != NBD_IHAVEOPT {
            bail!("bad IHAVEOPT: {:#x}", ihaveopt);
        }

        // --- Phase 2: Send client flags (4 bytes) ---
        stream
            .write_u32(NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES)
            .await?;

        // --- Phase 3: Send OPT_GO with export name ---
        let name_bytes = export_name.as_bytes();
        let payload_len = 4 + name_bytes.len() + 2; // name_len(4) + name + info_count(2)

        // Option header
        stream.write_u64(NBD_IHAVEOPT).await?;
        stream.write_u32(NBD_OPT_GO).await?;
        stream.write_u32(payload_len as u32).await?;
        // Payload
        stream.write_u32(name_bytes.len() as u32).await?;
        stream.write_all(name_bytes).await?;
        stream.write_u16(0).await?; // zero info requests
        stream.flush().await?;

        // --- Phase 4: Read option replies until ACK ---
        let mut export_size = 0u64;
        loop {
            let reply_magic = stream.read_u64().await?;
            if reply_magic != NBD_REPLY_MAGIC {
                bail!("bad option reply magic: {:#x}", reply_magic);
            }
            let _option = stream.read_u32().await?;
            let reply_type = stream.read_u32().await?;
            let length = stream.read_u32().await?;

            if reply_type == NBD_REP_INFO && length >= 12 {
                let _info_type = stream.read_u16().await?;
                export_size = stream.read_u64().await?;
                let _trans_flags = stream.read_u16().await?;
                // Consume any remaining bytes
                if length > 12 {
                    let mut discard = vec![0u8; (length - 12) as usize];
                    stream.read_exact(&mut discard).await?;
                }
            } else if reply_type == NBD_REP_ACK {
                break;
            } else if reply_type & 0x80000000 != 0 {
                // Error reply
                if length > 0 {
                    let mut discard = vec![0u8; length as usize];
                    stream.read_exact(&mut discard).await?;
                }
                bail!("OPT_GO error: reply_type={:#x}", reply_type);
            } else {
                // Unknown info reply — consume and continue
                if length > 0 {
                    let mut discard = vec![0u8; length as usize];
                    stream.read_exact(&mut discard).await?;
                }
            }
        }

        Ok(Self {
            stream,
            cookie: AtomicU64::new(1),
            export_size,
        })
    }

    fn next_cookie(&self) -> u64 {
        self.cookie.fetch_add(1, Ordering::Relaxed)
    }

    /// Read `length` bytes from the device at `offset`.
    pub async fn read(&mut self, offset: u64, length: u32) -> Result<Vec<u8>> {
        let cookie = self.next_cookie();

        // Send request header (28 bytes)
        self.stream.write_u32(NBD_REQUEST_MAGIC).await?;
        self.stream.write_u16(0).await?; // flags
        self.stream.write_u16(0).await?; // cmd_type = Read
        self.stream.write_u64(cookie).await?;
        self.stream.write_u64(offset).await?;
        self.stream.write_u32(length).await?;
        self.stream.flush().await?;

        // Read reply header (16 bytes)
        let reply_magic = self.stream.read_u32().await?;
        if reply_magic != NBD_SIMPLE_REPLY_MAGIC {
            bail!("bad reply magic: {:#x}", reply_magic);
        }
        let error = self.stream.read_u32().await?;
        let reply_cookie = self.stream.read_u64().await?;
        assert_eq!(reply_cookie, cookie);

        // Read data payload (server always sends `length` bytes, even on error)
        let mut data = vec![0u8; length as usize];
        self.stream.read_exact(&mut data).await?;

        if error != NBD_SUCCESS {
            bail!("read error: {}", error);
        }
        Ok(data)
    }

    /// Write `data` to the device at `offset`.
    pub async fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let error = self.write_raw(offset, data).await?;
        if error != NBD_SUCCESS {
            bail!("write error: {}", error);
        }
        Ok(())
    }

    /// Write and return the raw NBD error code (0 = success).
    /// Use this instead of `write()` when you expect an error (e.g., readonly export).
    pub async fn write_raw(&mut self, offset: u64, data: &[u8]) -> Result<u32> {
        let cookie = self.next_cookie();

        // Send request header (28 bytes) + data
        self.stream.write_u32(NBD_REQUEST_MAGIC).await?;
        self.stream.write_u16(0).await?; // flags
        self.stream.write_u16(1).await?; // cmd_type = Write
        self.stream.write_u64(cookie).await?;
        self.stream.write_u64(offset).await?;
        self.stream.write_u32(data.len() as u32).await?;
        self.stream.write_all(data).await?;
        self.stream.flush().await?;

        // Read reply header (16 bytes)
        let reply_magic = self.stream.read_u32().await?;
        if reply_magic != NBD_SIMPLE_REPLY_MAGIC {
            bail!("bad reply magic: {:#x}", reply_magic);
        }
        let error = self.stream.read_u32().await?;
        let reply_cookie = self.stream.read_u64().await?;
        assert_eq!(reply_cookie, cookie);

        Ok(error)
    }

    /// Flush all pending writes to stable storage.
    pub async fn flush(&mut self) -> Result<()> {
        let cookie = self.next_cookie();

        self.stream.write_u32(NBD_REQUEST_MAGIC).await?;
        self.stream.write_u16(0).await?; // flags
        self.stream.write_u16(3).await?; // cmd_type = Flush
        self.stream.write_u64(cookie).await?;
        self.stream.write_u64(0).await?; // offset
        self.stream.write_u32(0).await?; // length
        self.stream.flush().await?;

        let reply_magic = self.stream.read_u32().await?;
        if reply_magic != NBD_SIMPLE_REPLY_MAGIC {
            bail!("bad reply magic: {:#x}", reply_magic);
        }
        let error = self.stream.read_u32().await?;
        let reply_cookie = self.stream.read_u64().await?;
        assert_eq!(reply_cookie, cookie);

        if error != NBD_SUCCESS {
            bail!("flush error: {}", error);
        }
        Ok(())
    }

    /// Send disconnect and drop the connection.
    pub async fn disconnect(mut self) -> Result<()> {
        let cookie = self.next_cookie();

        self.stream.write_u32(NBD_REQUEST_MAGIC).await?;
        self.stream.write_u16(0).await?;
        self.stream.write_u16(2).await?; // cmd_type = Disconnect
        self.stream.write_u64(cookie).await?;
        self.stream.write_u64(0).await?;
        self.stream.write_u32(0).await?;
        self.stream.flush().await?;

        // No reply expected for disconnect
        Ok(())
    }
}
