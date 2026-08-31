//! Client→server framing.
//!
//! Three negotiated modes (mlat/jsonclient.py):
//!
//! - `none`  — newline-delimited JSON lines, raw.
//! - `zlib` / `zlib2` — frames of: 2-byte big-endian payload length, then the
//!   payload: output of a PERSISTENT zlib compressor fed the JSON lines and
//!   sync-flushed, with the 4-byte sync-flush trailer (`00 00 FF FF`) stripped.
//!   The compressor's dictionary state carries across frames — you cannot
//!   decompress frame N without having fed frames 1..N-1 — which is exactly
//!   why each capture pins one compression mode per client and why the replay
//!   engine replays byte streams, never re-encodes.
//!   `zlib2` differs only in batching policy (~1 s of lines per frame vs
//!   per-message); the wire format is identical.
//!
//! The reference implementation for goldens is CPython's zlib
//! (tools/gen_zlib_goldens.py) because that is literally what mlat-client and
//! the server run.

use flate2::{Compress, Compression, FlushCompress};

use crate::ProtoError;

/// Sync-flush trailer emitted by Z_SYNC_FLUSH, stripped on the wire.
const SYNC_TRAILER: [u8; 4] = [0x00, 0x00, 0xFF, 0xFF];

/// Stateful zlib frame encoder. One per connection, created after handshake.
pub struct ZlibFrameEncoder {
    z: Compress,
    buf: Vec<u8>,
}

impl Default for ZlibFrameEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ZlibFrameEncoder {
    pub fn new() -> Self {
        ZlibFrameEncoder {
            // Level 5-ish balance doesn't matter for correctness; mlat-client
            // uses default level. zlib header (not raw deflate): second arg true.
            z: Compress::new(Compression::default(), true),
            buf: Vec::with_capacity(8 * 1024),
        }
    }

    /// Encode one frame from already-newline-terminated JSON lines.
    /// Returns the on-wire bytes: 2-byte BE length + payload.
    pub fn encode_frame(&mut self, lines: &[u8]) -> Result<Vec<u8>, ProtoError> {
        self.buf.clear();
        self.buf.resize(lines.len() + 1024, 0);
        let before_in = self.z.total_in();
        let before_out = self.z.total_out();
        // Feed everything, then sync flush so the decoder can see a complete
        // flush point. Grow the buffer if the (rare) incompressible case
        // overflows it.
        let mut in_off = 0usize;
        let mut out_off = 0usize;
        loop {
            let status = self
                .z
                .compress(
                    &lines[in_off..],
                    &mut self.buf[out_off..],
                    FlushCompress::Sync,
                )
                .map_err(|e| ProtoError::Framing(format!("zlib: {e}")))?;
            in_off = (self.z.total_in() - before_in) as usize;
            out_off = (self.z.total_out() - before_out) as usize;
            match status {
                flate2::Status::Ok | flate2::Status::BufError => {
                    if in_off >= lines.len() && ends_with_sync(&self.buf[..out_off]) {
                        break;
                    }
                    // Need more output room.
                    let len = self.buf.len();
                    self.buf.resize(len + 4096, 0);
                }
                flate2::Status::StreamEnd => break,
            }
        }
        let payload = &self.buf[..out_off];
        let payload = payload
            .strip_suffix(&SYNC_TRAILER)
            .ok_or_else(|| ProtoError::Framing("sync flush trailer missing".into()))?;
        if payload.len() > u16::MAX as usize {
            return Err(ProtoError::Framing(format!(
                "frame too large: {} bytes (limit 65535); batch fewer lines",
                payload.len()
            )));
        }
        let mut out = Vec::with_capacity(2 + payload.len());
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        out.extend_from_slice(payload);
        Ok(out)
    }
}

fn ends_with_sync(b: &[u8]) -> bool {
    b.len() >= 4 && b[b.len() - 4..] == SYNC_TRAILER
}

/// Stateful decoder — the mirror image, used in tests and by the recorder to
/// make captures inspectable. Feed whole frames (with length header).
pub struct ZlibFrameDecoder {
    z: flate2::Decompress,
    out: Vec<u8>,
}

impl Default for ZlibFrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ZlibFrameDecoder {
    pub fn new() -> Self {
        ZlibFrameDecoder {
            z: flate2::Decompress::new(true),
            out: Vec::with_capacity(16 * 1024),
        }
    }

    /// Decode one frame; returns the decompressed JSON lines.
    pub fn decode_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, ProtoError> {
        if frame.len() < 2 {
            return Err(ProtoError::Framing("short frame".into()));
        }
        let want = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        let payload = &frame[2..];
        if payload.len() != want {
            return Err(ProtoError::Framing(format!(
                "length header {want} != payload {}",
                payload.len()
            )));
        }
        // Re-append the sync trailer the sender stripped, so inflate sees the
        // flush point.
        let mut with_trailer = Vec::with_capacity(payload.len() + 4);
        with_trailer.extend_from_slice(payload);
        with_trailer.extend_from_slice(&SYNC_TRAILER);

        self.out.clear();
        self.out.resize(64 * 1024, 0);
        let before_out = self.z.total_out();
        let mut in_off = 0usize;
        let mut out_off = 0usize;
        let before_in = self.z.total_in();
        loop {
            let status = self
                .z
                .decompress(
                    &with_trailer[in_off..],
                    &mut self.out[out_off..],
                    flate2::FlushDecompress::Sync,
                )
                .map_err(|e| ProtoError::Framing(format!("inflate: {e}")))?;
            in_off = (self.z.total_in() - before_in) as usize;
            out_off = (self.z.total_out() - before_out) as usize;
            match status {
                flate2::Status::Ok | flate2::Status::BufError => {
                    if in_off >= with_trailer.len() {
                        break;
                    }
                    let len = self.out.len();
                    self.out.resize(len + 64 * 1024, 0);
                }
                flate2::Status::StreamEnd => break,
            }
        }
        Ok(self.out[..out_off].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zlib_roundtrip_single_frame() {
        let mut enc = ZlibFrameEncoder::new();
        let mut dec = ZlibFrameDecoder::new();
        let lines = b"{\"heartbeat\":{}}\n";
        let frame = enc.encode_frame(lines).unwrap();
        assert!(frame.len() >= 2);
        let got = dec.decode_frame(&frame).unwrap();
        assert_eq!(got, lines);
    }

    #[test]
    fn zlib_state_carries_across_frames() {
        // The whole point of the persistent compressor: frame 2 references
        // frame 1's dictionary. Decoding frame 2 with a FRESH decoder must
        // fail or mis-decode; with the same decoder it must succeed.
        let mut enc = ZlibFrameEncoder::new();
        let l1 = b"{\"sync\":{\"et\":1,\"ot\":2,\"em\":\"8d\",\"om\":\"8d\"}}\n";
        let l2 = b"{\"sync\":{\"et\":3,\"ot\":4,\"em\":\"8d\",\"om\":\"8d\"}}\n";
        let f1 = enc.encode_frame(l1).unwrap();
        let f2 = enc.encode_frame(l2).unwrap();

        let mut dec = ZlibFrameDecoder::new();
        assert_eq!(dec.decode_frame(&f1).unwrap(), l1);
        assert_eq!(dec.decode_frame(&f2).unwrap(), l2);

        let mut fresh = ZlibFrameDecoder::new();
        let out = fresh.decode_frame(&f2);
        assert!(
            out.is_err() || out.unwrap() != l2,
            "frame 2 must not decode standalone — stream state is real"
        );
    }

    #[test]
    fn many_lines_one_frame() {
        let mut enc = ZlibFrameEncoder::new();
        let mut dec = ZlibFrameDecoder::new();
        let mut lines = Vec::new();
        for i in 0..200 {
            lines.extend_from_slice(
                format!("{{\"mlat\":{{\"t\":{i},\"m\":\"5d3c6444a0b1c2\"}}}}\n").as_bytes(),
            );
        }
        let frame = enc.encode_frame(&lines).unwrap();
        assert_eq!(dec.decode_frame(&frame).unwrap(), lines);
    }
}
