//! Shared vocabulary for mlat-bench.
//!
//! Zero I/O by design: everything here is data and math that the simulator,
//! the capture format, and the scorer all agree on.

use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha12Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Speed of light in vacuum, m/s. MLAT lives and dies on this constant.
pub const C_MPS: f64 = 299_792_458.0;

/// WGS84 semi-major axis, meters.
pub const WGS84_A: f64 = 6_378_137.0;
/// WGS84 flattening.
pub const WGS84_F: f64 = 1.0 / 298.257_223_563;
/// WGS84 first eccentricity squared, e² = f(2−f).
pub const WGS84_E2: f64 = WGS84_F * (2.0 - WGS84_F);

/// 24-bit ICAO aircraft address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Icao(pub u32);

impl Icao {
    /// Render as the 6-hex-digit form used everywhere in the ADS-B world.
    pub fn to_hex(self) -> String {
        format!("{:06x}", self.0 & 0xFF_FFFF)
    }

    pub fn from_hex(s: &str) -> Option<Self> {
        u32::from_str_radix(s.trim(), 16)
            .ok()
            .filter(|v| *v <= 0xFF_FFFF)
            .map(Icao)
    }
}

/// Simulation time: nanoseconds since the scenario epoch T0.
///
/// All event scheduling uses this; wall-clock only appears in the replay
/// engine, which maps SimNanos onto tokio deadlines.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct SimNanos(pub u64);

impl SimNanos {
    pub fn from_secs_f64(s: f64) -> Self {
        SimNanos((s * 1e9).round() as u64)
    }
    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1e9
    }
}

/// Geodetic position on the WGS84 ellipsoid. Altitude is meters above the
/// ellipsoid. The geoid separation is ignored; mlat-server ignores it too,
/// and MLAT errors are much larger.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Geodetic {
    pub lat_deg: f64,
    pub lon_deg: f64,
    pub alt_m: f64,
}

/// Earth-centered, earth-fixed cartesian position, meters.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Ecef {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Geodetic {
    /// Geodetic → ECEF, the standard closed form.
    pub fn to_ecef(&self) -> Ecef {
        let lat = self.lat_deg.to_radians();
        let lon = self.lon_deg.to_radians();
        let (sin_lat, cos_lat) = lat.sin_cos();
        let (sin_lon, cos_lon) = lon.sin_cos();
        // Prime vertical radius of curvature.
        let n = WGS84_A / (1.0 - WGS84_E2 * sin_lat * sin_lat).sqrt();
        Ecef {
            x: (n + self.alt_m) * cos_lat * cos_lon,
            y: (n + self.alt_m) * cos_lat * sin_lon,
            z: (n * (1.0 - WGS84_E2) + self.alt_m) * sin_lat,
        }
    }

    /// Straight-line (through-the-earth-chord) distance in meters.
    /// This is the correct distance for signal propagation delay — radio
    /// travels the chord, not the great circle.
    pub fn slant_range_m(&self, other: &Geodetic) -> f64 {
        let a = self.to_ecef();
        let b = other.to_ecef();
        ((a.x - b.x).powi(2) + (a.y - b.y).powi(2) + (a.z - b.z).powi(2)).sqrt()
    }

    /// Great-circle surface distance in meters (haversine on the mean sphere).
    /// Good enough for scoring: at MLAT error magnitudes (tens of meters and
    /// up) the ellipsoidal correction is noise.
    pub fn haversine_m(&self, other: &Geodetic) -> f64 {
        const R: f64 = 6_371_000.8; // mean earth radius
        let lat1 = self.lat_deg.to_radians();
        let lat2 = other.lat_deg.to_radians();
        let dlat = lat2 - lat1;
        let dlon = (other.lon_deg - self.lon_deg).to_radians();
        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        2.0 * R * a.sqrt().asin()
    }
}

impl Ecef {
    /// ECEF → geodetic via Bowring's method. One iteration is
    /// sub-millimeter for aircraft altitudes, far below MLAT error scales.
    pub fn to_geodetic(&self) -> Geodetic {
        let b = WGS84_A * (1.0 - WGS84_F);
        let ep2 = (WGS84_A * WGS84_A - b * b) / (b * b);
        let p = (self.x * self.x + self.y * self.y).sqrt();
        let theta = (self.z * WGS84_A).atan2(p * b);
        let (sin_t, cos_t) = theta.sin_cos();
        let lat = (self.z + ep2 * b * sin_t.powi(3)).atan2(p - WGS84_E2 * WGS84_A * cos_t.powi(3));
        let lon = self.y.atan2(self.x);
        let sin_lat = lat.sin();
        let n = WGS84_A / (1.0 - WGS84_E2 * sin_lat * sin_lat).sqrt();
        let alt = if lat.cos().abs() > 1e-12 {
            p / lat.cos() - n
        } else {
            self.z.abs() - b
        };
        Geodetic {
            lat_deg: lat.to_degrees(),
            lon_deg: lon.to_degrees(),
            alt_m: alt,
        }
    }
}

/// One line of the ground-truth track log: where an aircraft truly was.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TruthPoint {
    pub t: SimNanos,
    pub icao: Icao,
    pub pos: Geodetic,
    /// Ground speed, m/s.
    pub gs_mps: f64,
    /// Vertical rate, m/s (positive = climb).
    pub vrate_mps: f64,
}

/// Domain-separated deterministic RNG.
///
/// `rng_for(seed, "traj/ac_042")` and `rng_for(seed, "clock/rx_003")` are
/// independent streams: adding an aircraft to a scenario never perturbs
/// another aircraft's trajectory or any receiver's clock. This property is
/// what makes scenario A/B tweaking meaningful, so treat the stream names as
/// stable API.
pub fn rng_for(seed: u64, stream: &str) -> ChaCha12Rng {
    let mut h = Sha256::new();
    h.update(seed.to_le_bytes());
    h.update([0x1f]); // separator so (1, "23") != (12, "3")
    h.update(stream.as_bytes());
    let digest = h.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    ChaCha12Rng::from_seed(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_chacha::rand_core::RngCore;

    #[test]
    fn ecef_roundtrip() {
        // A few spots including awkward ones: equator, high latitude, negative
        // altitude, antimeridian neighborhood.
        let cases = [
            Geodetic {
                lat_deg: 0.0,
                lon_deg: 0.0,
                alt_m: 0.0,
            },
            Geodetic {
                lat_deg: 47.21,
                lon_deg: -1.55,
                alt_m: 10_668.0,
            },
            Geodetic {
                lat_deg: -33.87,
                lon_deg: 151.21,
                alt_m: 304.8,
            },
            Geodetic {
                lat_deg: 71.0,
                lon_deg: -156.8,
                alt_m: 0.0,
            },
            Geodetic {
                lat_deg: 21.3,
                lon_deg: 179.99,
                alt_m: 12_000.0,
            },
            Geodetic {
                lat_deg: 52.3,
                lon_deg: 4.76,
                alt_m: -4.0,
            }, // Schiphol is below sea level
        ];
        for g in cases {
            let back = g.to_ecef().to_geodetic();
            assert!((back.lat_deg - g.lat_deg).abs() < 1e-9, "lat {g:?}");
            assert!((back.lon_deg - g.lon_deg).abs() < 1e-9, "lon {g:?}");
            // Bowring single-iteration is ~1 µm at cruise altitude — orders of
            // magnitude below anything MLAT can measure.
            assert!((back.alt_m - g.alt_m).abs() < 1e-4, "alt {g:?} -> {back:?}");
        }
    }

    #[test]
    fn ecef_known_point() {
        // Greenwich-ish reference: lat 51.4778, lon 0, alt 0.
        let e = Geodetic {
            lat_deg: 51.4778,
            lon_deg: 0.0,
            alt_m: 0.0,
        }
        .to_ecef();
        assert!((e.y).abs() < 1e-6);
        // Sanity envelope, not authority: x and z must land inside the ellipsoid bounds.
        assert!(e.x > 3.9e6 && e.x < 4.0e6, "x = {}", e.x);
        assert!(e.z > 4.9e6 && e.z < 5.0e6, "z = {}", e.z);
    }

    #[test]
    fn slant_vs_haversine() {
        // At 100 km the ellipsoid chord (slant) and mean-sphere arc (haversine)
        // agree to ~0.3% — the difference is the sphere approximation, which is
        // fine for scoring (MLAT errors are compared at a scale where 0.3% of
        // the error is noise). Assert the ratio, not an absolute gap.
        let a = Geodetic {
            lat_deg: 47.0,
            lon_deg: -1.0,
            alt_m: 0.0,
        };
        let b = Geodetic {
            lat_deg: 47.0,
            lon_deg: 0.316,
            alt_m: 0.0,
        }; // ~100 km east
        let slant = a.slant_range_m(&b);
        let hav = a.haversine_m(&b);
        let rel = (slant - hav).abs() / hav;
        assert!(rel < 5e-3, "slant {slant} vs hav {hav} (rel {rel})");
        assert!((hav - 100_000.0).abs() < 2_000.0, "hav {hav}");
    }

    #[test]
    fn rng_domain_separation() {
        let mut a = rng_for(42, "traj/ac_000");
        let mut b = rng_for(42, "traj/ac_001");
        let mut a2 = rng_for(42, "traj/ac_000");
        let (xa, xb, xa2) = (a.next_u64(), b.next_u64(), a2.next_u64());
        assert_ne!(xa, xb, "different streams must differ");
        assert_eq!(xa, xa2, "same (seed, stream) must reproduce");
    }

    /// Golden values: if these change, every previously generated capture's
    /// scenario_sha256 lineage is broken. Bump capture format version if you
    /// ever intentionally change the RNG construction.
    #[test]
    fn rng_stability_golden() {
        let mut r = rng_for(0, "golden");
        let got: Vec<u64> = (0..4).map(|_| r.next_u64()).collect();
        // Frozen 2026-08-31 from the first implementation.
        let want = vec![
            12149199462874078217,
            4310878388966232180,
            2001857045178855481,
            4123390729093139828,
        ];
        assert_eq!(got, want, "RNG construction changed — see comment above");
    }

    #[test]
    fn icao_hex() {
        assert_eq!(Icao(0x3C6444).to_hex(), "3c6444");
        assert_eq!(Icao::from_hex("3C6444"), Some(Icao(0x3C6444)));
        assert_eq!(Icao::from_hex("1000000"), None);
    }
}
