//! Compact Position Reporting (CPR), airborne format.
//!
//! Encoding must be exact: mlat-server globally decodes every sync pair
//! (clocktrack.pyx:432) and drops pairs whose decode fails or lands
//! outside its range gates. See docs/protocol-notes.md.
//!
//! Conventions follow the usual references (ICAO Annex 10 Vol IV; mode-s.org):
//! NZ = 15, 17-bit encoded lat/lon, even i=0 / odd i=1.

use std::f64::consts::PI;

const NZ: f64 = 15.0;
const TWO17: f64 = 131_072.0; // 2^17

/// Number of longitude zones at a given latitude (the "NL function"),
/// closed form. Returns 1..=59.
pub fn nl(lat_deg: f64) -> u32 {
    let lat = lat_deg.abs();
    // Spec special cases: NL=59 at the equator band edge, NL=1 near the poles.
    if lat < 10.47047130 {
        // Above 10.47° the formula drops below 59; below it, 59 exactly.
        return 59;
    }
    if lat > 87.0 {
        return 1;
    }
    let a = 1.0 - (PI / (2.0 * NZ)).cos();
    let b = (PI * lat / 180.0).cos().powi(2);
    let x = 1.0 - a / b;
    if !(-1.0..=1.0).contains(&x) {
        return 1;
    }
    let nl = 2.0 * PI / x.acos();
    (nl.floor() as u32).clamp(1, 59)
}

/// Positive modulo, as CPR requires (x mod m in [0, m) even for negative x).
fn modp(x: f64, m: f64) -> f64 {
    let r = x % m;
    if r < 0.0 {
        r + m
    } else {
        r
    }
}

/// Encode an airborne position into 17-bit (lat, lon) CPR values.
/// `odd` selects the format (F bit). Valid for |lat| ≤ 90, any lon.
pub fn encode_airborne(lat_deg: f64, lon_deg: f64, odd: bool) -> (u32, u32) {
    let i = if odd { 1.0 } else { 0.0 };
    let dlat = 360.0 / (4.0 * NZ - i);
    let yz = (TWO17 * modp(lat_deg, dlat) / dlat + 0.5).floor();
    let yz = (yz as u64 % (1 << 17)) as u32;

    // NL must be evaluated at the latitude the decoder will reconstruct,
    // not at the true latitude; otherwise encoder and decoder disagree in a
    // zone-boundary sliver.
    let rlat = dlat * (yz as f64 / TWO17 + (lat_deg / dlat).floor());

    let nl_here = nl(rlat);
    let nlon = if odd {
        (nl_here.saturating_sub(1)).max(1)
    } else {
        nl_here.max(1)
    };
    let dlon = 360.0 / nlon as f64;
    let xz = (TWO17 * modp(lon_deg, dlon) / dlon + 0.5).floor();
    let xz = (xz as u64 % (1 << 17)) as u32;
    (yz, xz)
}

/// Globally decode an even/odd CPR pair. `recent_odd` says which message is
/// newer (its zone wins). Returns None when the pair straddles an NL
/// boundary; mlat-server rejects that case too.
///
/// Used by the mlatd server, the scorer, and tests.
pub fn global_decode_airborne(
    even: (u32, u32),
    odd: (u32, u32),
    recent_odd: bool,
) -> Option<(f64, f64)> {
    let (yz_e, xz_e) = (even.0 as f64, even.1 as f64);
    let (yz_o, xz_o) = (odd.0 as f64, odd.1 as f64);
    let dlat_e = 360.0 / 60.0;
    let dlat_o = 360.0 / 59.0;

    // Latitude zone index.
    let j = ((59.0 * yz_e - 60.0 * yz_o) / TWO17 + 0.5).floor();
    let mut rlat_e = dlat_e * (modp(j, 60.0) + yz_e / TWO17);
    let mut rlat_o = dlat_o * (modp(j, 59.0) + yz_o / TWO17);
    if rlat_e >= 270.0 {
        rlat_e -= 360.0;
    }
    if rlat_o >= 270.0 {
        rlat_o -= 360.0;
    }
    if nl(rlat_e) != nl(rlat_o) {
        return None; // NL boundary straddle: pair unusable
    }
    let nl_v = nl(rlat_e) as f64;

    let m = ((xz_e * (nl_v - 1.0) - xz_o * nl_v) / TWO17 + 0.5).floor();
    let (lat, mut lon) = if recent_odd {
        let ni = (nl_v - 1.0).max(1.0);
        let dlon = 360.0 / ni;
        (rlat_o, dlon * (modp(m, ni) + xz_o / TWO17))
    } else {
        let ni = nl_v.max(1.0);
        let dlon = 360.0 / ni;
        (rlat_e, dlon * (modp(m, ni) + xz_e / TWO17))
    };
    if lon >= 180.0 {
        lon -= 360.0;
    }
    Some((lat, lon))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical even/odd pair from the 1090 MHz literature
    /// (icao 40621D): CPR values extracted from the real frames
    /// 8D40621D58C382D690C8AC2863A7 (even) and
    /// 8D40621D58C386435CC412692AD6 (odd).
    const EVEN: (u32, u32) = (93000, 51372);
    const ODD: (u32, u32) = (74158, 50194);

    #[test]
    fn nl_reference_values() {
        assert_eq!(nl(0.0), 59);
        assert_eq!(nl(52.2572021484375), 36);
        assert_eq!(nl(-52.2572021484375), 36);
        assert_eq!(nl(87.5), 1);
        assert_eq!(nl(10.0), 59);
        // A transition-table spot check: NL drops to 58 just above 10.47047130°.
        assert_eq!(nl(10.48), 58);
    }

    #[test]
    fn golden_global_decode() {
        let (lat, lon) = global_decode_airborne(EVEN, ODD, false).unwrap();
        assert!((lat - 52.2572021484375).abs() < 1e-9, "lat {lat}");
        assert!((lon - 3.91937255859375).abs() < 1e-9, "lon {lon}");
    }

    #[test]
    fn golden_encode_even() {
        // Encoding the exact even-decoded position must reproduce the even
        // message's CPR values bit-for-bit.
        let (yz, xz) = encode_airborne(52.2572021484375, 3.91937255859375, false);
        assert_eq!((yz, xz), EVEN);
    }

    #[test]
    fn golden_encode_odd() {
        // Same, with the odd-decoded position and odd format.
        let (lat, lon) = global_decode_airborne(EVEN, ODD, true).unwrap();
        let (yz, xz) = encode_airborne(lat, lon, true);
        assert_eq!((yz, xz), ODD);
    }

    #[test]
    fn roundtrip_grid() {
        // Encode a pair at the same position, globally decode, compare.
        // CPR quantization: ~5.1 m in lat (360/60/2^17 deg), lon scales with
        // 1/cos(lat) — allow 3x margin over the theoretical cell size.
        let mut checked = 0u32;
        for lat10 in (-840..=840).step_by(7) {
            for lon10 in (-1790..=1790).step_by(37) {
                let lat = lat10 as f64 / 10.0;
                let lon = lon10 as f64 / 10.0;
                let e = encode_airborne(lat, lon, false);
                let o = encode_airborne(lat, lon, true);
                let Some((dlat, dlon)) = global_decode_airborne(e, o, false) else {
                    // NL-boundary straddle is legitimate; must be rare.
                    continue;
                };
                let cell_lat = 360.0 / 60.0 / TWO17; // ≈ 4.6e-5 deg
                let lat_err = (dlat - lat).abs();
                assert!(lat_err < 3.0 * cell_lat, "lat {lat} err {lat_err}");
                // Angular lon cell: NL already widens zones at high latitude.
                let cell_lon = 360.0 / (nl(lat).max(1) as f64) / TWO17;
                let lon_err = (dlon - lon).abs();
                assert!(
                    lon_err < 3.0 * cell_lon,
                    "lat {lat} lon {lon} err {lon_err} (cell {cell_lon})"
                );
                checked += 1;
            }
        }
        assert!(checked > 15_000, "only {checked} grid points decoded");
    }
}
