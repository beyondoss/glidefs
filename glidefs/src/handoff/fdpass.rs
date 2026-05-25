#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! SCM_RIGHTS file-descriptor passing over a SOCK_SEQPACKET socket.
//!
//! Used by the handoff protocol to pass listener fds (NBD TCP / Unix,
//! HTTP API) from the predecessor process to the successor. Reserved for
//! Phase 2 — the Phase 1 MVP rebinds listeners in the successor, which
//! costs a single TCP RST per in-flight client but is operationally
//! simpler.
//!
//! ## Wire format
//!
//! Each SCM_RIGHTS message carries an in-band datagram (the bincode-
//! serialized `HandoffMessage`) plus a cmsg with up to `MAX_FDS` file
//! descriptors. The receiver pulls fds from the ancillary buffer; the
//! kernel atomically dup's them into the receiver's fd table during
//! recvmsg.

use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::io::FromRawFd;
use tokio::net::UnixDatagram;

/// Maximum fds we ever pass in one cmsg. Linux's hard cap is 253 per
/// SCM_RIGHTS message; we stay well below to keep buffers cheap.
pub const MAX_FDS: usize = 64;

/// Send a payload plus optional fds over a SEQPACKET socket.
///
/// `socket` must be in SEQPACKET mode (set at bind/connect time).
/// Returns the number of payload bytes sent (= `payload.len()` on success
/// for a SEQPACKET socket; partial sends are not possible on SEQPACKET).
#[allow(dead_code)] // Phase 2
pub fn sendmsg_with_fds(
    socket: &UnixDatagram,
    payload: &[u8],
    fds: &[RawFd],
) -> io::Result<usize> {
    if fds.len() > MAX_FDS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("too many fds: {} > {}", fds.len(), MAX_FDS),
        ));
    }

    let sock_fd = socket.as_raw_fd();
    let iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut _,
        iov_len: payload.len(),
    };

    // CMSG buffer: enough for one SCM_RIGHTS with MAX_FDS slots.
    let cmsg_len = unsafe { libc::CMSG_LEN((std::mem::size_of::<RawFd>() * MAX_FDS) as u32) };
    let mut cmsg_buf = vec![0u8; cmsg_len as usize];

    let msghdr = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: &iov as *const _ as *mut _,
        msg_iovlen: 1,
        msg_control: if fds.is_empty() {
            std::ptr::null_mut()
        } else {
            cmsg_buf.as_mut_ptr() as *mut _
        },
        msg_controllen: if fds.is_empty() {
            0
        } else {
            unsafe { libc::CMSG_LEN(std::mem::size_of_val(fds) as u32) as _ }
        },
        msg_flags: 0,
    };

    if !fds.is_empty() {
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msghdr);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(
                std::mem::size_of_val(fds) as u32,
            ) as _;
            let data_ptr = libc::CMSG_DATA(cmsg) as *mut RawFd;
            for (i, fd) in fds.iter().enumerate() {
                data_ptr.add(i).write_unaligned(*fd);
            }
        }
    }

    let sent = unsafe { libc::sendmsg(sock_fd, &msghdr, 0) };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(sent as usize)
}

/// Receive a payload plus any passed fds. The fds are returned as owned
/// `OwnedFd` so the caller can dup or close them as needed; if the
/// caller drops the vec without using them, the kernel closes them
/// (no fd leak).
#[allow(dead_code)] // Phase 2
pub fn recvmsg_with_fds(
    socket: &UnixDatagram,
    buf: &mut [u8],
) -> io::Result<(usize, Vec<OwnedFd>)> {
    let sock_fd = socket.as_raw_fd();
    let iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut _,
        iov_len: buf.len(),
    };

    let cmsg_buflen = unsafe {
        libc::CMSG_SPACE((std::mem::size_of::<RawFd>() * MAX_FDS) as u32)
    };
    let mut cmsg_buf = vec![0u8; cmsg_buflen as usize];

    let mut msghdr = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: &iov as *const _ as *mut _,
        msg_iovlen: 1,
        msg_control: cmsg_buf.as_mut_ptr() as *mut _,
        msg_controllen: cmsg_buflen as _,
        msg_flags: 0,
    };

    let received = unsafe { libc::recvmsg(sock_fd, &mut msghdr, 0) };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }

    // Walk cmsg headers to collect fds.
    let mut fds = Vec::new();
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msghdr);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET
                && (*cmsg).cmsg_type == libc::SCM_RIGHTS
            {
                let data_ptr = libc::CMSG_DATA(cmsg) as *const RawFd;
                let payload_len = (*cmsg).cmsg_len as usize
                    - libc::CMSG_LEN(0) as usize;
                let nfds = payload_len / std::mem::size_of::<RawFd>();
                for i in 0..nfds {
                    let raw = data_ptr.add(i).read_unaligned();
                    fds.push(OwnedFd::from_raw_fd(raw));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msghdr, cmsg);
        }
    }

    Ok((received as usize, fds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;

    #[tokio::test]
    async fn fd_round_trip() {
        let (a, b) = UnixDatagram::pair().unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_fd = tmp.as_file().as_raw_fd();

        let payload = b"hello";
        let sent = sendmsg_with_fds(&a, payload, &[tmp_fd]).unwrap();
        assert_eq!(sent, payload.len());

        let mut buf = [0u8; 64];
        let (n, fds) = recvmsg_with_fds(&b, &mut buf).unwrap();
        assert_eq!(n, payload.len());
        assert_eq!(&buf[..n], payload);
        assert_eq!(fds.len(), 1);

        // Verify the received fd refers to the same kernel object — both
        // fds should produce the same st_ino on libc::fstat.
        let mut our_st: libc::stat = unsafe { std::mem::zeroed() };
        let mut their_st: libc::stat = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::fstat(tmp_fd, &mut our_st) }, 0);
        assert_eq!(unsafe { libc::fstat(fds[0].as_raw_fd(), &mut their_st) }, 0);
        assert_eq!(our_st.st_ino, their_st.st_ino);
        drop(fds);
    }
}
