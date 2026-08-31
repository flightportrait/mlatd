//! Downlink frame builders. Each returns the complete on-air bytes,
//! parity included — feed straight to hex encoding for the wire.

use crate::{alt, cpr, crc24};
use mb_core::Icao;

/// DF17 extended squitter, airborne position (TC 9–18), barometric altitude.
/// 14 bytes. `tc` defaults to 11 in callers (baro alt, NUC as broadcast by
/// most airliners); `odd` is the CPR format flag.
#[allow(clippy::too_many_arguments)]
pub fn df17_airborne_position(
    icao: Icao,
    ca: u8,
    tc: u8,
    alt_ft: i32,
    lat_deg: f64,
    lon_deg: f64,
    odd: bool,
) -> Option<[u8; 14]> {
    debug_assert!((9..=18).contains(&tc), "airborne position TC is 9..=18");
    let alt12 = alt::ac12(alt_ft)? as u64;
    let (yz, xz) = cpr::encode_airborne(lat_deg, lon_deg, odd);

    // ME, 56 bits: TC(5) SS(2) SAF(1) ALT12 T(1) F(1) LAT17 LON17.
    // SS (surveillance status) = 0, SAF = 0, T (UTC-synced time) = 0.
    let me: u64 = ((tc as u64) << 51)
        | (alt12 << 36)
        | ((odd as u64) << 34)
        | ((yz as u64) << 17)
        | (xz as u64);

    let mut f = [0u8; 14];
    f[0] = (17 << 3) | (ca & 0x7);
    f[1] = (icao.0 >> 16) as u8;
    f[2] = (icao.0 >> 8) as u8;
    f[3] = icao.0 as u8;
    for i in 0..7 {
        f[4 + i] = (me >> (48 - 8 * i)) as u8;
    }
    let p = crc24(&f[..11]);
    f[11] = (p >> 16) as u8;
    f[12] = (p >> 8) as u8;
    f[13] = p as u8;
    Some(f)
}

/// DF11 all-call reply / acquisition squitter, 7 bytes.
/// PI = parity XOR interrogator ID; spontaneous squitters use II=0,
/// so PI is the bare CRC and any receiver can recover the address.
pub fn df11(icao: Icao, ca: u8) -> [u8; 7] {
    let mut f = [0u8; 7];
    f[0] = (11 << 3) | (ca & 0x7);
    f[1] = (icao.0 >> 16) as u8;
    f[2] = (icao.0 >> 8) as u8;
    f[3] = icao.0 as u8;
    let p = crc24(&f[..4]);
    f[4] = (p >> 16) as u8;
    f[5] = (p >> 8) as u8;
    f[6] = p as u8;
    f
}

/// DF4 surveillance altitude reply, 7 bytes.
/// AP = parity XOR address — the receiver brute-recovers the address, which
/// is exactly what makes DF4 multilateratable without ADS-B.
/// fs: flight status (0 = airborne). Returns None if altitude unencodable.
pub fn df4(icao: Icao, fs: u8, alt_ft: i32) -> Option<[u8; 7]> {
    let ac13 = alt::ac13(alt_ft)? as u32;
    // First 32 bits: DF(5)=4, FS(3), DR(5)=0, UM(6)=0, AC(13).
    let head: u32 = (4u32 << 27) | ((fs as u32 & 0x7) << 24) | ac13;
    let mut f = [0u8; 7];
    f[0] = (head >> 24) as u8;
    f[1] = (head >> 16) as u8;
    f[2] = (head >> 8) as u8;
    f[3] = head as u8;
    let ap = crc24(&f[..4]) ^ icao.0;
    f[4] = (ap >> 16) as u8;
    f[5] = (ap >> 8) as u8;
    f[6] = ap as u8;
    Some(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden test: byte-for-byte reproduction of a real captured DF17 frame
    /// (the canonical pair from the 1090 MHz literature, icao 40621D,
    /// 38000 ft). If this passes, encoder, CPR, altitude, and CRC all agree
    /// with the real world at once.
    #[test]
    fn golden_frame_even() {
        let f = df17_airborne_position(
            Icao(0x40621D),
            5,
            11,
            38000,
            52.2572021484375,
            3.91937255859375,
            false,
        )
        .unwrap();
        assert_eq!(hex::encode(f), "8d40621d58c382d690c8ac2863a7");
    }

    #[test]
    fn golden_frame_odd() {
        // The odd frame of the same pair; its position is the odd-based
        // global decode of the pair (the aircraft had moved slightly).
        let (lat, lon) = cpr::global_decode_airborne((93000, 51372), (74158, 50194), true).unwrap();
        let f = df17_airborne_position(Icao(0x40621D), 5, 11, 38000, lat, lon, true).unwrap();
        assert_eq!(hex::encode(f), "8d40621d58c386435cc412692ad6");
    }

    #[test]
    fn df17_zero_remainder() {
        let f = df17_airborne_position(Icao(0x3C6444), 5, 11, 35000, 47.2, -1.5, false).unwrap();
        assert_eq!(crc24(&f), 0, "a valid DF17 has zero CRC remainder");
    }

    #[test]
    fn df11_address_recoverable() {
        let f = df11(Icao(0x3C6444), 5);
        // Receiver-side recovery: CRC over first 4 bytes equals PI (II=0).
        let pi = ((f[4] as u32) << 16) | ((f[5] as u32) << 8) | f[6] as u32;
        assert_eq!(crc24(&f[..4]), pi);
        assert_eq!(f[0] >> 3, 11);
    }

    #[test]
    fn df4_address_recoverable() {
        let f = df4(Icao(0x3C6444), 0, 35000).unwrap();
        // Receiver-side recovery: CRC over the data XOR AP = address.
        let ap = ((f[4] as u32) << 16) | ((f[5] as u32) << 8) | f[6] as u32;
        assert_eq!(crc24(&f[..4]) ^ ap, 0x3C6444);
        assert_eq!(f[0] >> 3, 4);
    }
}
