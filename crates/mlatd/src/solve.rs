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
    /// Horizontal error the geometry and the stated timing errors allow,
    /// m: the same covariance, not rescaled by the fit's own residuals. A
    /// 4-receiver fit has one spare equation, so its rescaled estimate can
    /// land anywhere (a fix 510 m out estimated itself at 1 m); this one
    /// does not depend on that one residual.
    pub err_geom_m: f64,
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
    let full = solve(obs, alt_m, init).and_then(|s| across_the_line(obs, alt_m, s));
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

/// A fit's own residual, taken as a factor of the one the stated timing
/// errors predict, above which a solve over five or more receivers is tried
/// again from the far side of the receivers: the lobe test below. A
/// 4-receiver solve is always tried: with one spare equation it fits the
/// wrong lobe to the noise floor when the receivers are collinear enough.
const LOBE_TEST_RMS_FACTOR: f64 = 3.0;
/// Two lobes whose residuals are within this factor of each other cannot be
/// told apart by the timing; the fix is refused.
const LOBE_AMBIGUOUS_FACTOR: f64 = 3.0;

/// The mirror lobe. Receivers strung along a valley, a coast or a motorway
/// are near-collinear, and with altitude fixed a TDOA fit then has two
/// solutions, one each side of the line, that differ only through the
/// receivers' scatter off it. Gauss-Newton converges into whichever lobe
/// its start lies in: a cold start on the receivers' centroid picks at
/// random, and a warm start keeps the track in the lobe the first fix chose
/// (valley scenario: a helicopter 6 km east of four receivers was tracked
/// 12 km away on the west side for the whole run, every fix a 4-receiver
/// solve the track itself gated in). The wrong lobe fits worse by the
/// scatter the receivers do have; a fit whose residual is well above what
/// its timing errors predict is re-solved from the point mirrored across
/// the receivers' principal axis, and the better fit kept. Two lobes that
/// fit alike are an ambiguity the timing cannot resolve: no fix.
fn across_the_line(obs: &[Observation], alt_m: f64, sol: Solution) -> Option<Solution> {
    let expected_rms =
        (obs.iter().map(|o| o.err_s * o.err_s).sum::<f64>() / obs.len() as f64).sqrt();
    if obs.len() > 4 && sol.rms_s <= LOBE_TEST_RMS_FACTOR * expected_rms {
        return Some(sol);
    }
    let mirror = mirror_across_receivers(obs, &sol.pos)?;
    let Some(other) = solve(obs, alt_m, mirror) else {
        return Some(sol);
    };
    // The re-solve may just come back to the same lobe.
    if other.pos.haversine_m(&sol.pos) < 3.0 * sol.err_geom_m.max(other.err_geom_m).max(100.0) {
        return Some(sol);
    }
    let (good, bad) = if other.rms_s < sol.rms_s {
        (other, sol)
    } else {
        (sol, other)
    };
    if bad.rms_s < LOBE_AMBIGUOUS_FACTOR * good.rms_s {
        return None;
    }
    Some(good)
}

/// `p` reflected across the receivers' principal axis (the line through
/// their centroid along their largest spread), in ECEF; None when the
/// receivers have no spread.
fn mirror_across_receivers(obs: &[Observation], p: &Geodetic) -> Option<Geodetic> {
    let n = obs.len() as f64;
    let c = obs.iter().fold([0.0; 3], |a, o| {
        [a[0] + o.rx.x / n, a[1] + o.rx.y / n, a[2] + o.rx.z / n]
    });
    // Principal axis by a few power iterations on the 3×3 scatter matrix.
    let mut m = [[0.0f64; 3]; 3];
    for o in obs {
        let d = [o.rx.x - c[0], o.rx.y - c[1], o.rx.z - c[2]];
        for (i, di) in d.iter().enumerate() {
            for (j, dj) in d.iter().enumerate() {
                m[i][j] += di * dj;
            }
        }
    }
    let mut u = [1.0f64, 1.0, 1.0];
    for _ in 0..50 {
        let v = [
            m[0][0] * u[0] + m[0][1] * u[1] + m[0][2] * u[2],
            m[1][0] * u[0] + m[1][1] * u[1] + m[1][2] * u[2],
            m[2][0] * u[0] + m[2][1] * u[1] + m[2][2] * u[2],
        ];
        let norm = (v[0] * v[0] + v[1] * v[1] + v[2] * v[2]).sqrt();
        if norm < 1.0 {
            return None;
        }
        u = [v[0] / norm, v[1] / norm, v[2] / norm];
    }
    let pe = p.to_ecef();
    let d = [pe.x - c[0], pe.y - c[1], pe.z - c[2]];
    let along = d[0] * u[0] + d[1] * u[1] + d[2] * u[2];
    let mirrored = Ecef {
        x: c[0] + 2.0 * along * u[0] - d[0],
        y: c[1] + 2.0 * along * u[1] - d[1],
        z: c[2] + 2.0 * along * u[2] - d[2],
    }
    .to_geodetic();
    Some(Geodetic {
        alt_m: p.alt_m,
        ..mirrored
    })
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

    let mut iters = 0;
    for it in 0..MAX_ITER {
        iters = it + 1;
        // The step uses weighted residuals so precise receivers pull harder
        // (solver.py); the unweighted RMS after the loop is the
        // physical-credibility gate.
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
    if !lat.is_finite() || !lon.is_finite() || lat.abs() > 90.0 {
        return None;
    }
    // The loop measures the residual before each step, so a run that ends
    // on its iteration cap has never checked where its last step (up to a
    // degree) landed. The gate reads the returned position.
    let final_resid = residuals(obs, lat, lon, alt_m, t_tx);
    let rms = (final_resid.iter().map(|x| x * x).sum::<f64>() / final_resid.len() as f64).sqrt();
    if rms > MAX_RMS_S {
        return None;
    }
    // Longitude leaves the solve unwrapped (a warm start at 179.99° can step
    // past 180°); published positions stay in [-180, 180).
    let lon = (lon + 540.0).rem_euclid(360.0) - 180.0;

    // Error estimate from the final weighted normal matrix: cov = σ²(JᵀJ)⁻¹
    // with σ² from the weighted residuals. Lat/lon variances → meters.
    // If the matrix does not invert, the fix is suspect; mlat-server drops
    // those (mlattrack.py "this result is suspect") and this solver does too.
    let r = residuals_w(obs, lat, lon, alt_m, t_tx);
    let dof = (obs.len() as f64 - 3.0).max(1.0);
    let sigma2 = r.iter().map(|x| x * x).sum::<f64>() / dof;
    let jtj = normal_matrix(obs, lat, lon, alt_m, t_tx);
    let cov = invert3(&jtj)?;
    let m_per_deg_lat = 111_320.0;
    let m_per_deg_lon = 111_320.0 * lat.to_radians().cos().max(0.05);
    let geom_m2 =
        cov[0][0] * m_per_deg_lat * m_per_deg_lat + cov[1][1] * m_per_deg_lon * m_per_deg_lon;
    let err_est_m = (sigma2 * geom_m2).abs().sqrt();
    let err_geom_m = geom_m2.abs().sqrt();

    Some(Solution {
        pos: Geodetic {
            lat_deg: lat,
            lon_deg: lon,
            alt_m,
        },
        rms_s: rms,
        err_est_m,
        err_geom_m,
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

    /// Four receivers along a valley, a few hundred metres off a line, and
    /// a helicopter 6 km to one side. Observations from the truth, with
    /// 50 ns noise.
    fn valley(truth: &Geodetic, scatter_deg: f64) -> Vec<Observation> {
        let te = truth.to_ecef();
        let rxs = [
            (47.000, -1.500 - scatter_deg, 60.0),
            (47.063, -1.500 + 1.6 * scatter_deg, 90.0),
            (47.127, -1.500 - 1.6 * scatter_deg, 75.0),
            (47.190, -1.500 + 0.6 * scatter_deg, 110.0),
        ];
        let t_tx = 10.0;
        let mut seed = 7u64;
        rxs.iter()
            .map(|&(la, lo, al)| {
                let r = Geodetic {
                    lat_deg: la,
                    lon_deg: lo,
                    alt_m: al,
                }
                .to_ecef();
                let d = ((te.x - r.x).powi(2) + (te.y - r.y).powi(2) + (te.z - r.z).powi(2)).sqrt();
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let noise = ((seed >> 11) as f64 / (1u64 << 53) as f64 - 0.5) * 100e-9;
                Observation {
                    rx: r,
                    t_s: t_tx + d / C_MPS + noise,
                    err_s: 50e-9,
                }
            })
            .collect()
    }

    #[test]
    fn a_warm_start_in_the_wrong_lobe_comes_back_across_the_line() {
        let truth = Geodetic {
            lat_deg: 47.095,
            lon_deg: -1.420,
            alt_m: 457.0,
        };
        let obs = valley(&truth, 0.005);
        // The track sits in the mirror lobe, 6 km west of the line.
        let wrong = Geodetic {
            lon_deg: -1.580,
            ..truth
        };
        let alone = solve(&obs, truth.alt_m, wrong).expect("the wrong lobe fits under 3 µs");
        assert!(
            alone.pos.haversine_m(&truth) > 5_000.0,
            "plain solve stays in its lobe"
        );
        let s = solve_robust(&obs, truth.alt_m, wrong).expect("a fix");
        let err = s.pos.haversine_m(&truth);
        assert!(err < 150.0, "err {err} m");
    }

    #[test]
    fn a_cold_start_on_the_line_lands_in_the_right_lobe() {
        let truth = Geodetic {
            lat_deg: 47.095,
            lon_deg: -1.420,
            alt_m: 457.0,
        };
        let obs = valley(&truth, 0.005);
        let centroid = Geodetic {
            lat_deg: 47.095,
            lon_deg: -1.500,
            alt_m: 457.0,
        };
        let s = solve_robust(&obs, truth.alt_m, centroid).expect("a fix");
        let err = s.pos.haversine_m(&truth);
        assert!(err < 150.0, "err {err} m");
    }

    #[test]
    fn receivers_on_one_line_give_no_fix() {
        let truth = Geodetic {
            lat_deg: 47.095,
            lon_deg: -1.420,
            alt_m: 457.0,
        };
        let obs = valley(&truth, 0.0);
        let wrong = Geodetic {
            lon_deg: -1.580,
            ..truth
        };
        let s = solve(&obs, truth.alt_m, wrong).expect("fits");
        assert!(
            s.pos.haversine_m(&truth) > 10_000.0 && s.rms_s < 10e-9,
            "the wrong lobe fits to the noise floor: rms {}",
            s.rms_s
        );
        assert!(
            solve_robust(&obs, truth.alt_m, wrong).is_none(),
            "both lobes fit alike"
        );
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
