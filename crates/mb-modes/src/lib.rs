//! Mode S frame encoding. Pure functions, no I/O.
//!
//! - `crc24` (poly 0xFFF409); AP/PI application lives in the frame builders
//! - `cpr`: airborne even/odd encode + NL function + global decode (for tests
//!   and metrics — the oracle judges with its own)
//! - `alt`: AC12/AC13 altitude encoding (25 ft Q-bit paths only)
//! - `frames`: df17_airborne_position / df11 / df4 builders
//!
//! Validation: byte-exact reproduction of real captured frames (the canonical
//! 40621D pair) + round-trip against adsb_deku (dev-dep only).

pub mod alt;
pub mod cpr;
pub mod decode;
pub mod frames;

pub use alt::{ac12_decode as alt_ac12_decode, ac13_decode as alt_ac13_decode};

/// Mode S CRC-24 generator polynomial (per Annex 10 Vol IV).
pub const CRC24_POLY: u32 = 0xFFF409;

/// CRC-24 over `data`, MSB-first, as used by all Mode S downlink formats.
pub fn crc24(data: &[u8]) -> u32 {
    let mut crc: u32 = 0;
    for &byte in data {
        crc ^= (byte as u32) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if crc & 0x1_00_00_00 != 0 {
                crc ^= CRC24_POLY;
            }
        }
    }
    crc & 0xFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical example frame from the 1090 MHz literature:
    /// a complete DF17 has remainder 0 (CRC over all 14 bytes, since the last
    /// 3 bytes ARE the parity for DF17).
    #[test]
    fn crc24_canonical_df17() {
        let frame = hex::decode("8D40621D58C382D690C8AC2863A7").unwrap();
        assert_eq!(frame.len(), 14);
        assert_eq!(crc24(&frame), 0, "valid DF17 must have zero remainder");
        // And over the first 11 bytes, the CRC must equal the trailing parity.
        let parity = ((frame[11] as u32) << 16) | ((frame[12] as u32) << 8) | frame[13] as u32;
        assert_eq!(crc24(&frame[..11]), parity);
    }
}
