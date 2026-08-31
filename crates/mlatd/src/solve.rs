//! TDOA position solve: Gauss-Newton on (lat, lon, t_tx) with altitude fixed
//! from the aircraft's own Mode S altitude replies. Fixing altitude turns a
//! marginal 4-receiver geometry into a well-determined solve; mlat-server
//! does the same.

use mb_core::{Ecef, Geodetic, C_MPS};

#[derive(Clone, Copy)]
pub struct Observation {
    pub rx: Ecef,
    /// Arrival time in the common (reference) timebase, seconds.
    pub t_s: f64,
    /// Expected timing error (1σ, seconds) — clock jitter + sync-model slack.
    /// Residuals are weighted by 1/err, mlat-server's scheme (solver.py).
    pub err_s: f64,
}

pub struct Solution {
    pub pos: Geodetic,
    /// RMS residual of the fit, seconds (unweighted).
    pub rms_s: f64,
    /// Covariance-derived horizontal position error estimate, meters:
    /// mlat-server's var_est = trace(cov) (mlattrack.py), horizontal block
    /// only. This value gates publication.
    pub err_est_m: f64,
    /// Kept for logging/tests; not part of the CSV contract.
    #[allow(dead_code)]
    pub iterations: u32,
    /// Solved transmit time in the common timebase (not yet consumed; the
    /// track layer will want it).
    #[allow(dead_code)]
    pub t_tx: f64,
    /// Per-observation UNWEIGHTED residuals (predicted − measured, seconds),
    /// same order as the input slice; input for per-receiver bias learning.
    pub residuals_s: Vec<f64>,
}

const MAX_ITER: u32 = 15;
/// Accept only fits whose residual is physically credible: 3 µs ≈ 900 m of
/// pseudorange scatter. Anything worse is a bad group or broken sync and is
/// dropped, not published.
pub const MAX_RMS_S: f64 = 3e-6;

/// Robust entry point: full-set solve first; when the residual is worse than
/// the clean-fit expectation and there are receivers to spare, retry leaving
/// each one out and keep the best fit. mlat-server reaches the same end via
/// timestamp clustering. The bench showed the failure this cures: 300 m
/// error bursts caused by one receiver's sync noise in the solve.
pub fn solve_robust(obs: &[Observation], alt_m: f64, init: Geodetic) -> Option<Solution> {
    let full = solve(obs, alt_m, init);
    // LOO only when the full set actually failed or fit badly. The bench
    // rejected unconditional LOO (lab p90 38→73 m): an n−1 subset fits
    // 3 parameters to 4 points, so its rms is structurally small, and an
    // rms-based preference then picks worse geometry.
    let trigger = match &full {
        Some(s) => s.rms_s > 0.5e-6,
        None => true,
    };
    if !trigger || obs.len() < 5 {
        return full;
    }
    let mut best = full;
    for skip in 0..obs.len() {
        let subset: Vec<Observation> = obs
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != skip)
            .map(|(_, o)| Observation { ..*o })
            .collect();
        if let Some(s) = solve(&subset, alt_m, init) {
            if best.as_ref().is_none_or(|b| s.rms_s < b.rms_s) {
                best = Some(s);
            }
        }
    }
    best
}

pub fn solve(obs: &[Observation], alt_m: f64, init: Geodetic) -> Option<Solution> {
    if obs.len() < 4 {
        return None;
    }
    let mut lat = init.lat_deg;
    let mut lon = init.lon_deg;
    // t_tx initial: earliest arrival minus a plausible propagation time.
    let t_min = obs.iter().map(|o| o.t_s).fold(f64::INFINITY, f64::min);
    let mut t_tx = t_min - 200e3 / C_MPS;

    let mut rms = f64::INFINITY;
    let mut iters = 0;
    for it in 0..MAX_ITER {
        iters = it + 1;
        // Unweighted RMS is the physical-credibility gate; the step uses
        // weighted residuals so precise receivers pull harder (solver.py).
        let ru = residuals(obs, lat, lon, alt_m, t_tx);
        rms = (ru.iter().map(|x| x * x).sum::<f64>() / ru.len() as f64).sqrt();
        let r = residuals_w(obs, lat, lon, alt_m, t_tx);

        // Numeric Jacobian. Step sizes: ~1 m in position, 0.1 µs in time.
        let dlat = 1e-5;
        let dlon = 1e-5 / lat.to_radians().cos().max(0.2);
        let dt = 1e-7;
        let jl = residuals_w(obs, lat + dlat, lon, alt_m, t_tx);
        let jo = residuals_w(obs, lat, lon + dlon, alt_m, t_tx);
        let jt = residuals_w(obs, lat, lon, alt_m, t_tx + dt);

        // Normal equations for the 3-parameter step (JᵀJ)Δ = −Jᵀr.
        let n = obs.len();
        let mut jtj = [[0.0f64; 3]; 3];
        let mut jtr = [0.0f64; 3];
        for i in 0..n {
            let ji = [
                (jl[i] - r[i]) / dlat,
                (jo[i] - r[i]) / dlon,
                (jt[i] - r[i]) / dt,
            ];
            for a in 0..3 {
                for b in 0..3 {
                    jtj[a][b] += ji[a] * ji[b];
                }
                jtr[a] -= ji[a] * r[i];
            }
        }
        let step = solve3(&jtj, &jtr)?;
        // Clamp: no step larger than ~1 degree / 1 ms — divergence guard.
        let (sl, so, st) = (
            step[0].clamp(-1.0, 1.0),
            step[1].clamp(-1.0, 1.0),
            step[2].clamp(-1e-3, 1e-3),
        );
        lat += sl;
        lon += so;
        t_tx += st;
        if sl.abs() < 1e-9 && so.abs() < 1e-9 && st.abs() < 1e-12 {
            break;
        }
    }
    if !lat.is_finite() || !lon.is_finite() || rms > MAX_RMS_S {
        return None;
    }

    // Error estimate from the final weighted normal matrix: cov = σ²(JᵀJ)⁻¹
    // with σ² from the weighted residuals. Lat/lon variances → meters.
    // If the matrix does not invert, the fix is suspect; mlat-server drops
    // those (mlattrack.py "this result is suspect") and this solver does too.
    let final_resid = residuals(obs, lat, lon, alt_m, t_tx);
    let r = residuals_w(obs, lat, lon, alt_m, t_tx);
    let dof = (obs.len() as f64 - 3.0).max(1.0);
    let sigma2 = r.iter().map(|x| x * x).sum::<f64>() / dof;
    let jtj = normal_matrix(obs, lat, lon, alt_m, t_tx);
    let cov = invert3(&jtj)?;
    let m_per_deg_lat = 111_320.0;
    let m_per_deg_lon = 111_320.0 * lat.to_radians().cos().max(0.05);
    let var_m2 = sigma2
        * (cov[0][0] * m_per_deg_lat * m_per_deg_lat + cov[1][1] * m_per_deg_lon * m_per_deg_lon);
    let err_est_m = var_m2.abs().sqrt();

    Some(Solution {
        pos: Geodetic {
            lat_deg: lat,
            lon_deg: lon,
            alt_m,
        },
        rms_s: rms,
        err_est_m,
        iterations: iters,
        t_tx,
        residuals_s: final_resid,
    })
}

fn normal_matrix(obs: &[Observation], lat: f64, lon: f64, alt_m: f64, t_tx: f64) -> [[f64; 3]; 3] {
    let r = residuals_w(obs, lat, lon, alt_m, t_tx);
    let dlat = 1e-5;
    let dlon = 1e-5 / lat.to_radians().cos().max(0.2);
    let dt = 1e-7;
    let jl = residuals_w(obs, lat + dlat, lon, alt_m, t_tx);
    let jo = residuals_w(obs, lat, lon + dlon, alt_m, t_tx);
    let jt = residuals_w(obs, lat, lon, alt_m, t_tx + dt);
    let mut jtj = [[0.0f64; 3]; 3];
    for i in 0..obs.len() {
        let ji = [
            (jl[i] - r[i]) / dlat,
            (jo[i] - r[i]) / dlon,
            (jt[i] - r[i]) / dt,
        ];
        for a in 0..3 {
            for b in 0..3 {
                jtj[a][b] += ji[a] * ji[b];
            }
        }
    }
    jtj
}

/// Invert a 3×3 via Cramer; None when singular (degenerate geometry).
fn invert3(a: &[[f64; 3]; 3]) -> Option<[[f64; 3]; 3]> {
    let mut out = [[0.0f64; 3]; 3];
    for k in 0..3 {
        let mut e = [0.0; 3];
        e[k] = 1.0;
        let col = solve3(a, &e)?;
        for r in 0..3 {
            out[r][k] = col[r];
        }
    }
    Some(out)
}

fn residuals_w(obs: &[Observation], lat: f64, lon: f64, alt_m: f64, t_tx: f64) -> Vec<f64> {
    residuals(obs, lat, lon, alt_m, t_tx)
        .iter()
        .zip(obs)
        .map(|(r, o)| r / o.err_s.max(1e-9))
        .collect()
}

fn residuals(obs: &[Observation], lat: f64, lon: f64, alt_m: f64, t_tx: f64) -> Vec<f64> {
    let p = Geodetic {
        lat_deg: lat,
        lon_deg: lon,
        alt_m,
    }
    .to_ecef();
    obs.iter()
        .map(|o| {
            let d =
                ((p.x - o.rx.x).powi(2) + (p.y - o.rx.y).powi(2) + (p.z - o.rx.z).powi(2)).sqrt();
            (t_tx + d / C_MPS) - o.t_s
        })
        .collect()
}

/// 3×3 linear solve, Cramer's rule (conditioning is fine at this size after
/// the parameter scaling above).
fn solve3(a: &[[f64; 3]; 3], b: &[f64; 3]) -> Option<[f64; 3]> {
    let det = a[0][0] * (a[1][1] * a[2][2] - a[1][2] * a[2][1])
        - a[0][1] * (a[1][0] * a[2][2] - a[1][2] * a[2][0])
        + a[0][2] * (a[1][0] * a[2][1] - a[1][1] * a[2][0]);
    if det.abs() < 1e-30 {
        return None;
    }
    let mut out = [0.0; 3];
    for k in 0..3 {
        let mut m = *a;
        for row in 0..3 {
            m[row][k] = b[row];
        }
        let dk = m[0][0] * (m[1][1] * m[2][2] - m[1][2] * m[2][1])
            - m[0][1] * (m[1][0] * m[2][2] - m[1][2] * m[2][0])
            + m[0][2] * (m[1][0] * m[2][1] - m[1][1] * m[2][0]);
        out[k] = dk / det;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic exactness: place an aircraft, compute exact arrival times at
    /// 5 receivers, solve, expect meters.
    #[test]
    fn recovers_position() {
        let truth = Geodetic {
            lat_deg: 47.25,
            lon_deg: -1.40,
            alt_m: 6400.0,
        };
        let te = truth.to_ecef();
        let rxs = [
            (47.2181, -1.5528, 40.0),
            (47.4802, -1.0511, 85.0),
            (46.9433, -1.1002, 60.0),
            (47.0821, -2.0107, 25.0),
            (47.5934, -1.7523, 110.0),
        ];
        let t_tx = 123.456;
        let obs: Vec<Observation> = rxs
            .iter()
            .map(|&(la, lo, al)| {
                let r = Geodetic {
                    lat_deg: la,
                    lon_deg: lo,
                    alt_m: al,
                }
                .to_ecef();
                let d = ((te.x - r.x).powi(2) + (te.y - r.y).powi(2) + (te.z - r.z).powi(2)).sqrt();
                Observation {
                    rx: r,
                    t_s: t_tx + d / C_MPS,
                    err_s: 100e-9,
                }
            })
            .collect();
        let init = Geodetic {
            lat_deg: 47.2,
            lon_deg: -1.5,
            alt_m: truth.alt_m,
        };
        let s = solve(&obs, truth.alt_m, init).expect("solves");
        let err = s.pos.haversine_m(&truth);
        assert!(err < 1.0, "err {err} m after {} iters", s.iterations);
        assert!(s.rms_s < 1e-9);
    }

    /// With 100 ns timing noise the solve should land within ~100 m and
    /// report a credible rms.
    #[test]
    fn tolerates_timing_noise() {
        let truth = Geodetic {
            lat_deg: 47.25,
            lon_deg: -1.40,
            alt_m: 6400.0,
        };
        let te = truth.to_ecef();
        let rxs = [
            (47.2181, -1.5528, 40.0),
            (47.4802, -1.0511, 85.0),
            (46.9433, -1.1002, 60.0),
            (47.0821, -2.0107, 25.0),
            (47.5934, -1.7523, 110.0),
        ];
        // Fixed pseudo-noise, ±100 ns.
        let noise = [70e-9, -90e-9, 40e-9, -20e-9, 85e-9];
        let obs: Vec<Observation> = rxs
            .iter()
            .zip(noise)
            .map(|(&(la, lo, al), dn)| {
                let r = Geodetic {
                    lat_deg: la,
                    lon_deg: lo,
                    alt_m: al,
                }
                .to_ecef();
                let d = ((te.x - r.x).powi(2) + (te.y - r.y).powi(2) + (te.z - r.z).powi(2)).sqrt();
                Observation {
                    rx: r,
                    t_s: 5.0 + d / C_MPS + dn,
                    err_s: 100e-9,
                }
            })
            .collect();
        let init = Geodetic {
            lat_deg: 47.3,
            lon_deg: -1.3,
            alt_m: truth.alt_m,
        };
        let s = solve(&obs, truth.alt_m, init).expect("solves");
        let err = s.pos.haversine_m(&truth);
        assert!(err < 200.0, "err {err} m");
    }
}
