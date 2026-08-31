//! Track smoothing: an alpha-beta filter per aircraft over accepted fixes.
//!
//! The oracle ships Kalman-filtered output beside the raw solves, and smooth
//! tracks are what downstream consumers (maps, readsb) actually want. An
//! alpha-beta filter is the minimal honest version: position + velocity
//! state, gains scheduled by fix quality vs elapsed time. Whether it earns
//! its place is a bench question — filtered vs raw percentiles on real data
//! decide, not taste.

use mb_core::Geodetic;

pub struct TrackFilter {
    lat: f64,
    lon: f64,
    /// deg/s
    vlat: f64,
    vlon: f64,
    last_t: f64,
    n: u32,
}

impl TrackFilter {
    pub fn new(first: Geodetic, t: f64) -> Self {
        TrackFilter {
            lat: first.lat_deg,
            lon: first.lon_deg,
            vlat: 0.0,
            vlon: 0.0,
            last_t: t,
            n: 1,
        }
    }

    /// Feed an accepted fix; returns the smoothed position at its time.
    /// err_est_m schedules the gains: a tight fix pulls hard, a loose one
    /// mostly coasts the prediction.
    pub fn update(&mut self, fix: Geodetic, t: f64, err_est_m: f64) -> Geodetic {
        let dt = (t - self.last_t).clamp(0.0, 30.0);
        self.last_t = t;
        // Predict.
        let plat = self.lat + self.vlat * dt;
        let plon = self.lon + self.vlon * dt;
        // Gain: alpha in [0.15, 0.85], smaller for worse fixes; beta tied to
        // alpha (standard alpha-beta relation), softened while the track is
        // young so early velocity estimates don't whip.
        let quality = (40.0 / err_est_m.max(30.0)).clamp(0.15, 0.8);
        let alpha = if self.n < 5 {
            quality.max(0.5)
        } else {
            quality
        };
        let beta = alpha * alpha / (2.0 - alpha);
        let rlat = fix.lat_deg - plat;
        let rlon = fix.lon_deg - plon;
        self.lat = plat + alpha * rlat;
        self.lon = plon + alpha * rlon;
        if dt > 0.05 {
            self.vlat += beta * rlat / dt;
            self.vlon += beta * rlon / dt;
        }
        self.n += 1;
        Geodetic {
            lat_deg: self.lat,
            lon_deg: self.lon,
            alt_m: fix.alt_m,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converges_on_straight_track() {
        // Truth: 1e-3 deg/s northbound. Noisy fixes ±3e-4 deg. The filter
        // should track within the noise floor after warm-up.
        let mut f = TrackFilter::new(
            Geodetic {
                lat_deg: 47.0,
                lon_deg: -1.5,
                alt_m: 6000.0,
            },
            0.0,
        );
        let noise = [3e-4, -2e-4, 1e-4, -3e-4, 2e-4, 0.0, -1e-4, 3e-4];
        let mut worst_late: f64 = 0.0;
        for i in 1..40 {
            let t = i as f64;
            let true_lat = 47.0 + 1e-3 * t;
            let fix = Geodetic {
                lat_deg: true_lat + noise[i % 8],
                lon_deg: -1.5,
                alt_m: 6000.0,
            };
            let sm = f.update(fix, t, 100.0);
            if i > 15 {
                worst_late = worst_late.max((sm.lat_deg - true_lat).abs());
            }
        }
        assert!(
            worst_late < 2.5e-4,
            "smoothed error {worst_late} should beat raw noise 3e-4"
        );
    }
}
