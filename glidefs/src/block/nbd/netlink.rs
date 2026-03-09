//! Generic netlink interface to the kernel NBD module (`NBD_GENL`).
//!
//! Creates and destroys `/dev/nbdN` block devices by sending commands to the
//! `nbd` generic netlink family. Uses `libc` directly — no extra crate needed.
//!
//! Requires Linux 4.10+ with `CONFIG_BLK_DEV_NBD=y` and the `nbd` kernel module
//! loaded.

use std::io;
use std::os::fd::RawFd;

/// RAII guard that closes a file descriptor on drop.
struct FdGuard(RawFd);

impl Drop for FdGuard {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

// --- Generic netlink constants ---

const NETLINK_GENERIC: libc::c_int = 16;

// Generic netlink controller
const GENL_ID_CTRL: u16 = 0x10;
const CTRL_CMD_GETFAMILY: u8 = 3;
const CTRL_ATTR_FAMILY_NAME: u16 = 2;
const CTRL_ATTR_FAMILY_ID: u16 = 1;

// Netlink message header flags
const NLM_F_REQUEST: u16 = 1;
const NLM_F_ACK: u16 = 4;

// Netlink message types
const NLMSG_ERROR: u16 = 2;

// --- NBD generic netlink constants ---

const NBD_GENL_FAMILY_NAME: &[u8] = b"nbd\0";

// NBD commands
const NBD_CMD_CONNECT: u8 = 0;
const NBD_CMD_DISCONNECT: u8 = 1;
const NBD_CMD_RECONFIGURE: u8 = 2;

// NBD attributes
const NBD_ATTR_INDEX: u16 = 1;
const NBD_ATTR_SIZE_BYTES: u16 = 2;
const NBD_ATTR_BLOCK_SIZE_BYTES: u16 = 3;
const NBD_ATTR_TIMEOUT: u16 = 4;
const NBD_ATTR_SERVER_FLAGS: u16 = 5;
const NBD_ATTR_SOCKETS: u16 = 7;
const NBD_ATTR_DEAD_CONN_TIMEOUT: u16 = 8;

// NBD socket attributes (nested)
const NBD_SOCK_FD: u16 = 0;

// Nested attribute flag
const NLA_F_NESTED: u16 = 1 << 15;

// --- Netlink message building ---

/// Aligned size for netlink attributes.
fn nla_align(len: usize) -> usize {
    (len + 3) & !3
}

/// Append a netlink attribute (NLA) to a buffer.
fn put_nla(buf: &mut Vec<u8>, attr_type: u16, payload: &[u8]) {
    let nla_len = 4 + payload.len();
    buf.extend_from_slice(&(nla_len as u16).to_ne_bytes());
    buf.extend_from_slice(&attr_type.to_ne_bytes());
    buf.extend_from_slice(payload);
    // Pad to 4-byte alignment
    let pad = nla_align(nla_len) - nla_len;
    buf.extend(std::iter::repeat_n(0u8, pad));
}

fn put_nla_u32(buf: &mut Vec<u8>, attr_type: u16, val: u32) {
    put_nla(buf, attr_type, &val.to_ne_bytes());
}

fn put_nla_u64(buf: &mut Vec<u8>, attr_type: u16, val: u64) {
    put_nla(buf, attr_type, &val.to_ne_bytes());
}

fn put_nla_string(buf: &mut Vec<u8>, attr_type: u16, s: &[u8]) {
    put_nla(buf, attr_type, s);
}

/// Build a complete netlink message with generic netlink header.
fn build_genl_msg(family_id: u16, cmd: u8, seq: u32, attrs: &[u8]) -> Vec<u8> {
    // nlmsghdr (16 bytes) + genlmsghdr (4 bytes) + attrs
    let msg_len = 16 + 4 + attrs.len();
    let mut buf = Vec::with_capacity(msg_len);

    // nlmsghdr
    buf.extend_from_slice(&(msg_len as u32).to_ne_bytes()); // nlmsg_len
    buf.extend_from_slice(&family_id.to_ne_bytes()); // nlmsg_type
    buf.extend_from_slice(&(NLM_F_REQUEST | NLM_F_ACK).to_ne_bytes()); // nlmsg_flags
    buf.extend_from_slice(&seq.to_ne_bytes()); // nlmsg_seq
    buf.extend_from_slice(&0u32.to_ne_bytes()); // nlmsg_pid

    // genlmsghdr
    buf.push(cmd); // cmd
    buf.push(1); // version
    buf.extend_from_slice(&0u16.to_ne_bytes()); // reserved

    buf.extend_from_slice(attrs);
    buf
}

/// Open a generic netlink socket.
fn open_genl_socket() -> io::Result<RawFd> {
    let fd = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
            NETLINK_GENERIC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    // Bind to kernel (pid=0, groups=0)
    let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
    addr.nl_family = libc::AF_NETLINK as u16;
    let ret = unsafe {
        libc::bind(
            fd,
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_nl>() as u32,
        )
    };
    if ret < 0 {
        let e = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(e);
    }

    Ok(fd)
}

/// Send a message on the netlink socket.
fn nl_send(fd: RawFd, msg: &[u8]) -> io::Result<()> {
    let ret = unsafe { libc::send(fd, msg.as_ptr() as *const _, msg.len(), 0) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Receive a message from the netlink socket.
fn nl_recv(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    let ret = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut _, buf.len(), 0) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ret as usize)
}

/// Parse the nlmsghdr from a received buffer.
fn parse_nlmsghdr(buf: &[u8]) -> Option<(u32, u16, u16, &[u8])> {
    if buf.len() < 16 {
        return None;
    }
    let nlmsg_len = u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let nlmsg_type = u16::from_ne_bytes([buf[4], buf[5]]);
    let nlmsg_flags = u16::from_ne_bytes([buf[6], buf[7]]);
    let payload_start = 16;
    let payload_end = std::cmp::min(nlmsg_len as usize, buf.len());
    if payload_end <= payload_start {
        return Some((nlmsg_len, nlmsg_type, nlmsg_flags, &[]));
    }
    Some((nlmsg_len, nlmsg_type, nlmsg_flags, &buf[payload_start..payload_end]))
}

/// Parse netlink attributes from a payload.
fn parse_nla(payload: &[u8]) -> Vec<(u16, &[u8])> {
    let mut attrs = Vec::new();
    let mut offset = 0;
    while offset + 4 <= payload.len() {
        let nla_len = u16::from_ne_bytes([payload[offset], payload[offset + 1]]) as usize;
        let nla_type = u16::from_ne_bytes([payload[offset + 2], payload[offset + 3]]);
        if nla_len < 4 || offset + nla_len > payload.len() {
            break;
        }
        attrs.push((nla_type, &payload[offset + 4..offset + nla_len]));
        offset += nla_align(nla_len);
    }
    attrs
}

/// Wait for an ACK (NLMSG_ERROR with error=0) or return the error.
fn wait_for_ack(fd: RawFd) -> io::Result<()> {
    let mut buf = [0u8; 4096];
    let n = nl_recv(fd, &mut buf)?;
    let (_, msg_type, _, payload) = parse_nlmsghdr(&buf[..n])
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated netlink response"))?;

    if msg_type == NLMSG_ERROR
        && payload.len() >= 4
    {
        let errno = i32::from_ne_bytes([payload[0], payload[1], payload[2], payload[3]]);
        if errno == 0 {
            return Ok(()); // ACK
        }
        return Err(io::Error::from_raw_os_error(-errno));
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected netlink response type {msg_type}"),
    ))
}

/// Resolve the generic netlink family ID for "nbd".
fn resolve_nbd_family(fd: RawFd) -> io::Result<u16> {
    let mut attrs = Vec::new();
    put_nla_string(&mut attrs, CTRL_ATTR_FAMILY_NAME, NBD_GENL_FAMILY_NAME);

    let msg = build_genl_msg(GENL_ID_CTRL, CTRL_CMD_GETFAMILY, 1, &attrs);
    nl_send(fd, &msg)?;

    let mut buf = [0u8; 4096];
    let n = nl_recv(fd, &mut buf)?;

    let (_, msg_type, _, payload) = parse_nlmsghdr(&buf[..n])
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated response"))?;

    if msg_type == NLMSG_ERROR
        && payload.len() >= 4
    {
        let errno = i32::from_ne_bytes([payload[0], payload[1], payload[2], payload[3]]);
        if errno != 0 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "nbd generic netlink family not found (errno {}). \
                     Is the nbd kernel module loaded? Try: modprobe nbd",
                    -errno
                ),
            ));
        }
    }

    // Skip genlmsghdr (4 bytes) to get to attributes
    if payload.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response too short for genlmsghdr",
        ));
    }
    let nla_payload = &payload[4..];

    for (attr_type, attr_data) in parse_nla(nla_payload) {
        if attr_type == CTRL_ATTR_FAMILY_ID && attr_data.len() >= 2 {
            let family_id = u16::from_ne_bytes([attr_data[0], attr_data[1]]);
            // Drain the pending ACK (NLM_F_ACK causes the kernel to send a
            // separate NLMSG_ERROR/errno=0 after the genl response). If it
            // was already batched into the recv above, this is a no-op.
            let mut drain = [0u8; 256];
            unsafe {
                libc::recv(
                    fd,
                    drain.as_mut_ptr() as *mut _,
                    drain.len(),
                    libc::MSG_DONTWAIT,
                );
            }
            return Ok(family_id);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "nbd family ID not found in response",
    ))
}

/// Connect an NBD device via generic netlink.
///
/// Creates a new `/dev/nbdN` device backed by the given socket fd.
/// Returns the assigned device index.
///
/// # Arguments
/// - `socket_fd`: Connected socket fd (one end of a socketpair). The kernel
///   takes ownership of this fd and will send NBD commands over it.
/// - `size_bytes`: Device size in bytes.
/// - `block_size`: Block size (e.g., 512 or 4096).
/// - `server_flags`: NBD transmission flags (NBD_FLAG_HAS_FLAGS | ...).
/// - `dead_conn_timeout`: Seconds before the kernel declares the connection dead.
///   Set > 0 to enable I/O queueing during restarts (zero-downtime upgrades).
/// - `preferred_index`: Request a specific device index (e.g., to reclaim a
///   device path after a crash). `None` = auto-assign.
pub fn connect(
    socket_fd: RawFd,
    size_bytes: u64,
    block_size: u32,
    server_flags: u64,
    dead_conn_timeout: u32,
    preferred_index: Option<i32>,
) -> io::Result<i32> {
    let fd = open_genl_socket()?;
    let _guard = FdGuard(fd);

    let family_id = resolve_nbd_family(fd)?;

    // Build socket list (nested attribute)
    let mut sock_attrs = Vec::new();
    // Each socket entry is itself nested
    let mut sock_entry = Vec::new();
    put_nla_u32(&mut sock_entry, NBD_SOCK_FD, socket_fd as u32);
    put_nla(&mut sock_attrs, NLA_F_NESTED, &sock_entry);

    // Build connect attributes
    let mut attrs = Vec::new();
    // Include NBD_ATTR_INDEX only when reclaiming a specific device path.
    // Omitting it tells the kernel to auto-assign the lowest free index.
    if let Some(index) = preferred_index {
        put_nla_u32(&mut attrs, NBD_ATTR_INDEX, index as u32);
    }
    put_nla_u64(&mut attrs, NBD_ATTR_SIZE_BYTES, size_bytes);
    put_nla_u32(&mut attrs, NBD_ATTR_BLOCK_SIZE_BYTES, block_size);
    put_nla_u64(&mut attrs, NBD_ATTR_SERVER_FLAGS, server_flags);
    put_nla(&mut attrs, NBD_ATTR_SOCKETS | NLA_F_NESTED, &sock_attrs);
    if dead_conn_timeout > 0 {
        put_nla_u64(&mut attrs, NBD_ATTR_DEAD_CONN_TIMEOUT, dead_conn_timeout as u64);
    }
    put_nla_u64(&mut attrs, NBD_ATTR_TIMEOUT, 0); // no I/O timeout

    let msg = build_genl_msg(family_id, NBD_CMD_CONNECT, 2, &attrs);
    nl_send(fd, &msg)?;

    // The kernel response depends on whether we requested a specific index:
    //
    // - Specific index: kernel sends NLMSG_ERROR ACK (errno=0 on success),
    //   and the index is what we requested.
    // - Auto-assign (no NBD_ATTR_INDEX): kernel sends a genl reply
    //   (msg_type = family_id) containing NBD_ATTR_INDEX with the assigned
    //   device number, followed by an NLMSG_ERROR ACK.
    let mut buf = [0u8; 4096];
    let n = nl_recv(fd, &mut buf)?;
    let (_, msg_type, _, payload) = parse_nlmsghdr(&buf[..n])
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated response"))?;

    // Case 1: NLMSG_ERROR — either an ACK (errno=0) or a real error.
    if msg_type == NLMSG_ERROR && payload.len() >= 4 {
        let errno = i32::from_ne_bytes([payload[0], payload[1], payload[2], payload[3]]);
        if errno == 0 {
            // ACK for a specific-index connect — return the index we asked for.
            if let Some(index) = preferred_index {
                return Ok(index);
            }
            // Auto-assign ACK without a prior genl reply is unexpected.
            // Try echoed attrs, then sysfs scan.
            if payload.len() >= 4 + 16 + 4 {
                let echoed_attrs = &payload[4 + 16 + 4..];
                for (attr_type, attr_data) in parse_nla(echoed_attrs) {
                    if attr_type == NBD_ATTR_INDEX && attr_data.len() >= 4 {
                        return Ok(u32::from_ne_bytes([
                            attr_data[0], attr_data[1], attr_data[2], attr_data[3],
                        ]) as i32);
                    }
                }
            }
            return find_newest_nbd_device();
        }
        return Err(io::Error::from_raw_os_error(-errno));
    }

    // Case 2: genl reply — parse NBD_ATTR_INDEX from the response attributes.
    if payload.len() >= 4 {
        // Skip genlmsghdr (4 bytes) to reach the attributes.
        for (attr_type, attr_data) in parse_nla(&payload[4..]) {
            if attr_type == NBD_ATTR_INDEX && attr_data.len() >= 4 {
                return Ok(u32::from_ne_bytes([
                    attr_data[0], attr_data[1], attr_data[2], attr_data[3],
                ]) as i32);
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected response type {msg_type} to NBD_CMD_CONNECT"),
    ))
}

/// Disconnect an NBD device via generic netlink.
pub fn disconnect(index: i32) -> io::Result<()> {
    let fd = open_genl_socket()?;
    let _guard = FdGuard(fd);

    let family_id = resolve_nbd_family(fd)?;

    let mut attrs = Vec::new();
    put_nla_u32(&mut attrs, NBD_ATTR_INDEX, index as u32);

    let msg = build_genl_msg(family_id, NBD_CMD_DISCONNECT, 3, &attrs);
    nl_send(fd, &msg)?;
    wait_for_ack(fd)
}

/// Reconfigure an NBD device with a new socket fd (zero-downtime restart).
///
/// The device must have been created with `dead_conn_timeout > 0`.
/// Queued I/O resumes on the new socket.
pub fn reconfigure(index: i32, socket_fd: RawFd) -> io::Result<()> {
    let fd = open_genl_socket()?;
    let _guard = FdGuard(fd);

    let family_id = resolve_nbd_family(fd)?;

    let mut sock_attrs = Vec::new();
    let mut sock_entry = Vec::new();
    put_nla_u32(&mut sock_entry, NBD_SOCK_FD, socket_fd as u32);
    put_nla(&mut sock_attrs, NLA_F_NESTED, &sock_entry);

    let mut attrs = Vec::new();
    put_nla_u32(&mut attrs, NBD_ATTR_INDEX, index as u32);
    put_nla(&mut attrs, NBD_ATTR_SOCKETS | NLA_F_NESTED, &sock_attrs);

    let msg = build_genl_msg(family_id, NBD_CMD_RECONFIGURE, 4, &attrs);
    nl_send(fd, &msg)?;
    wait_for_ack(fd)
}

/// Find the most recently created /dev/nbdN device by scanning /sys/block.
fn find_newest_nbd_device() -> io::Result<i32> {
    let mut best: Option<(i32, std::time::SystemTime)> = None;
    let entries = std::fs::read_dir("/sys/block")?;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(index_str) = name.strip_prefix("nbd")
            && let Ok(index) = index_str.parse::<i32>()
        {
            // Check if the device has a pid (is connected)
            let pid_path = entry.path().join("pid");
            if pid_path.exists()
                && let Ok(meta) = entry.metadata()
                && let Ok(modified) = meta.modified()
                && (best.is_none() || modified > best.as_ref().unwrap().1)
            {
                best = Some((index, modified));
            }
        }
    }
    best.map(|(idx, _)| idx).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "no connected nbd device found")
    })
}
