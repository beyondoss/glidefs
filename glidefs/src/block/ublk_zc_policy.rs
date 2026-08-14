//! Policy for ublk no-payload ops on the zero-copy dispatch path.
//!
//! Extracted so Darwin (and any host without ublk) can still lock the
//! contract: `WRITE_ZEROES` is advertised (`max_write_zeroes_sectors` is
//! non-zero) and must run the handler. `DISCARD` is advertised as
//! unsupported (`max_discard_sectors = 0`); ACK-0 is correct for that
//! opcode only.
//!
//! Opcode numbers are the Linux `ublk_cmd.h` values. A Linux-only test
//! next to the ublk device asserts they still match `ublk_core::sys`.

/// `UBLK_IO_OP_FLUSH` (`ublk_cmd.h`).
pub const UBLK_IO_OP_FLUSH: u32 = 2;
/// `UBLK_IO_OP_DISCARD` (`ublk_cmd.h`).
pub const UBLK_IO_OP_DISCARD: u32 = 3;
/// `UBLK_IO_OP_WRITE_ZEROES` (`ublk_cmd.h`).
pub const UBLK_IO_OP_WRITE_ZEROES: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZcNoPayload {
    /// Run the handler; do not ACK until it completes.
    RunHandler,
    /// The device advertised this opcode as unsupported. ACK 0 is correct.
    AdvertisedNoop,
}

/// What the ZC dispatch must do for a no-payload ublk opcode.
pub fn zc_no_payload(op: u32) -> Option<ZcNoPayload> {
    match op {
        UBLK_IO_OP_FLUSH => Some(ZcNoPayload::RunHandler),
        UBLK_IO_OP_DISCARD => Some(ZcNoPayload::AdvertisedNoop),
        UBLK_IO_OP_WRITE_ZEROES => Some(ZcNoPayload::AdvertisedNoop),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_zeroes_must_run_the_handler() {
        assert_eq!(
            zc_no_payload(UBLK_IO_OP_WRITE_ZEROES),
            Some(ZcNoPayload::RunHandler),
            "ZC WRITE_ZEROES is advertised; ACK-0 without zeroing is silent wrong"
        );
    }

    #[test]
    fn discard_is_an_advertised_noop() {
        assert_eq!(
            zc_no_payload(UBLK_IO_OP_DISCARD),
            Some(ZcNoPayload::AdvertisedNoop)
        );
    }

    #[test]
    fn flush_must_run_the_handler() {
        assert_eq!(
            zc_no_payload(UBLK_IO_OP_FLUSH),
            Some(ZcNoPayload::RunHandler)
        );
    }
}
