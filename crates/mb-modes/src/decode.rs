//! Frame decoding, the inverse of `frames`, for the mlatd server and for
//! tooling. Only the paths the bench emits: DF17 airborne position, DF11,
//! DF4 with Q-bit altitudes.

use crate::{alt_ac12_decode, alt_ac13_decode, crc24};
use mb_core::Icao;

/// A decoded DF17 airborne position message.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Df17Airborne {
    pub icao: Icao,
    pub odd: bool,
    pub cpr_lat: u32,
    pub cpr_lon: u32,
    pub alt_ft: Option<i32>,
}

/// Parse a 14-byte DF17 airborne position frame (TC 9–18). Verifies CRC.
pub fn parse_df17_airborne(f: &[u8]) -> Option<Df17Airborne> {
    if f.len() != 14 || f[0] >> 3 != 17 || crc24(f) != 0 {
        return None;
    }
    let icao = Icao(((f[1] as u32) << 16) | ((f[2] as u32) << 8) | f[3] as u32);
    let me = u64::from_be_bytes([0, f[4], f[5], f[6], f[7], f[8], f[9], f[10]]);
    let tc = ((me >> 51) & 0x1F) as u8;
    if !(9..=18).contains(&tc) {
        return None;
    }
    let alt12 = ((me >> 36) & 0xFFF) as u16;
    Some(Df17Airborne {
        icao,
        odd: (me >> 34) & 1 == 1,
        cpr_lat: ((me >> 17) & 0x1FFFF) as u32,
        cpr_lon: (me & 0x1FFFF) as u32,
        alt_ft: alt_ac12_decode(alt12),
    })
}

/// Parse a 7-byte DF11: recover the address (PI = CRC for II=0 squitters).
pub fn parse_df11(f: &[u8]) -> Option<Icao> {
    if f.len() != 7 || f[0] >> 3 != 11 {
        return None;
    }
    Some(Icao(
        ((f[1] as u32) << 16) | ((f[2] as u32) << 8) | f[3] as u32,
    ))
}

/// Parse a 7-byte DF4 altitude reply: address recovered from AP ⊕ CRC,
/// altitude from AC13 (Q-bit path).
pub fn parse_df4(f: &[u8]) -> Option<(Icao, Option<i32>)> {
    if f.len() != 7 || f[0] >> 3 != 4 {
        return None;
    }
    let ap = ((f[4] as u32) << 16) | ((f[5] as u32) << 8) | f[6] as u32;
    let icao = Icao((crc24(&f[..4]) ^ ap) & 0xFF_FFFF);
    let head = u32::from_be_bytes([f[0], f[1], f[2], f[3]]);
    let ac13 = (head & 0x1FFF) as u16;
    Some((icao, alt_ac13_decode(ac13)))
}

/// Downlink format of a raw frame, if length is plausible.
pub fn df_of(f: &[u8]) -> Option<u8> {
    match f.len() {
        7 | 14 => Some(f[0] >> 3),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames;

    #[test]
    fn df17_roundtrip() {
        let f =
            frames::df17_airborne_position(Icao(0x3C6444), 5, 11, 35000, 47.2, -1.5, true).unwrap();
        let d = parse_df17_airborne(&f).unwrap();
        assert_eq!(d.icao, Icao(0x3C6444));
        assert!(d.odd);
        assert_eq!(d.alt_ft, Some(35000));
        let (yz, xz) = crate::cpr::encode_airborne(47.2, -1.5, true);
        assert_eq!((d.cpr_lat, d.cpr_lon), (yz, xz));
    }

    #[test]
    fn df17_golden_frame() {
        let f = hex::decode("8d40621d58c382d690c8ac2863a7").unwrap();
        let d = parse_df17_airborne(&f).unwrap();
        assert_eq!(d.icao, Icao(0x40621D));
        assert!(!d.odd);
        assert_eq!((d.cpr_lat, d.cpr_lon), (93000, 51372));
        assert_eq!(d.alt_ft, Some(38000));
    }

    #[test]
    fn df4_df11_roundtrip() {
        let f = frames::df4(Icao(0x3944F1), 0, 21000).unwrap();
        assert_eq!(parse_df4(&f), Some((Icao(0x3944F1), Some(21000))));
        let f = frames::df11(Icao(0x3944F1), 5);
        assert_eq!(parse_df11(&f), Some(Icao(0x3944F1)));
    }

    #[test]
    fn corrupted_df17_rejected() {
        let mut f = hex::decode("8d40621d58c382d690c8ac2863a7").unwrap();
        f[5] ^= 0x40;
        assert!(parse_df17_airborne(&f).is_none());
    }
}
