//! Pairwise receiver clock synchronization, O(1) memory per pair.
//!
//! The first version stored a deque of raw observations and refit on
//! demand; at world scale (10k receivers, ~10⁶ co-hearing pairs) that costs
//! gigabytes, and the fit cost saturated a core. This version keeps
//! exponentially-weighted centered sufficient statistics
//! (Welford-with-decay: means and central moments, so large counter values
//! do not cause catastrophic cancellation): ~100 bytes per pair, O(1)
//! update, O(1) convert.
//!
//! Outlier handling is an online gate: once the model is warm, an
//! observation too far from prediction is rejected instead of ingested, so
//! a false sync source or a multipath spike cannot move the fit. A burst of
//! consecutive rejections resets the pair; that is the correct response to
//! a genuine clock jump.

/// Forgetting factor per accepted observation ≈ sliding window of ~1/(1−λ)
/// observations (~300), matching the old deque's effective span.
const LAMBDA: f64 = 0.9967;
const MIN_WEIGHT: f64 = 8.0; // effective observations before usable
const MIN_SPAN_S: f64 = 5.0; // effective a-spread (via caa) before usable
/// Online outlier gate: reject when |residual| > max(4σ, this floor).
const GATE_FLOOR_S: f64 = 1e-6;
/// Consecutive rejections treated as a clock jump: reset the model.
const RESET_AFTER_REJECTS: u32 = 8;
/// Refuse conversions whose prediction sigma exceeds this; a poisoned
/// observation is worse than a missing one.
const MAX_PRED_SIGMA_S: f64 = 2e-6;

#[derive(Default)]
pub struct PairModel {
    w: f64,   // total (decayed) weight
    ma: f64,  // weighted mean of t_from
    mb: f64,  // weighted mean of t_to
    caa: f64, // weighted central Σ (a−ma)²
    cab: f64, // weighted central Σ (a−ma)(b−mb)
    msq: f64, // EW mean of squared fit residuals (σ² estimate)
    rejects: u32,
    n_total: u64,
}

impl PairModel {
    /// Returns false when the observation was rejected by the outlier gate.
    pub fn push(&mut self, t_from: f64, t_to: f64) -> bool {
        // Online gate once warm: a wild pair observation is refused, not
        // averaged in. Too many in a row = clock jump = reset.
        if self.usable() {
            let (pred, _) = self.predict_unchecked(t_from);
            let r = t_to - pred;
            let gate = (4.0 * self.msq.sqrt()).max(GATE_FLOOR_S);
            if r.abs() > gate {
                self.rejects += 1;
                if self.rejects >= RESET_AFTER_REJECTS {
                    *self = PairModel::default();
                }
                return false;
            }
            self.rejects = 0;
            // Track residual variance before this observation updates the fit.
            self.msq += (1.0 - LAMBDA) * (r * r - self.msq);
        }
        // Welford-with-decay update of centered sums.
        self.w = self.w * LAMBDA + 1.0;
        let da = t_from - self.ma;
        let db = t_to - self.mb;
        let k = 1.0 / self.w;
        self.ma += k * da;
        self.mb += k * db;
        // Central moments decay with the same factor; the (1−k) cross term
        // is the standard Welford correction.
        self.caa = self.caa * LAMBDA + da * (t_from - self.ma);
        self.cab = self.cab * LAMBDA + da * (t_to - self.mb);
        self.n_total += 1;
        true
    }

    fn predict_unchecked(&self, t_from: f64) -> (f64, f64) {
        let beta = if self.caa > 0.0 {
            self.cab / self.caa
        } else {
            1.0
        };
        let pred = self.mb + beta * (t_from - self.ma);
        // Prediction interval: inflates when the model is young or
        // extrapolates far from the weighted center — the two regimes where
        // km-scale errors hid behind small fit residuals.
        let da = t_from - self.ma;
        let infl = (1.0 + 1.0 / self.w + da * da / self.caa.max(1e-12)).sqrt();
        (pred, self.msq.sqrt() * infl)
    }

    /// Cheap usability probe (enough weight over enough span).
    pub fn usable(&self) -> bool {
        // caa/w ≈ variance of a; span ≈ a few σ. Require σ_a ≥ MIN_SPAN/4.
        self.w >= MIN_WEIGHT && self.caa / self.w.max(1.0) >= (MIN_SPAN_S / 4.0).powi(2)
    }

    /// Convert a from-clock reading to the to-clock; returns the value and
    /// its prediction sigma.
    pub fn convert(&mut self, t_from: f64) -> Option<(f64, f64)> {
        if !self.usable() {
            return None;
        }
        let (pred, sigma) = self.predict_unchecked(t_from);
        if sigma > MAX_PRED_SIGMA_S {
            return None;
        }
        Some((pred, sigma))
    }

    /// (observation count, estimated pairwise offset ppm) for status export
    /// (sync.json, in mlat-server's shape for existing monitoring).
    pub fn status(&self) -> (usize, f64) {
        let beta = if self.caa > 0.0 {
            self.cab / self.caa
        } else {
            1.0
        };
        (self.n_total as usize, (beta - 1.0) * 1e6)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovers_offset_and_drift() {
        // to-clock runs 10 ppm fast with 0.5 ms offset.
        let mut m = PairModel::default();
        for i in 0..60 {
            let t = i as f64;
            m.push(t, 0.0005 + t * (1.0 + 10e-6));
        }
        let (got, sigma) = m.convert(100.0).unwrap();
        let want = 0.0005 + 100.0 * (1.0 + 10e-6);
        assert!((got - want).abs() < 5e-9, "{got} vs {want}");
        assert!(sigma < 1e-6, "clean data, small sigma: {sigma}");
    }

    #[test]
    fn trims_poisoned_observations() {
        // Clean relation + 10% gross outliers once warm: the online gate
        // refuses them, conversion stays ns-accurate.
        let mut m = PairModel::default();
        for i in 0..80 {
            let t = i as f64;
            let poison = if i > 20 && i % 10 == 3 { 2e-6 } else { 0.0 };
            m.push(t, 0.0005 + t * (1.0 + 10e-6) + poison);
        }
        let (got, _) = m.convert(80.0).unwrap();
        let want = 0.0005 + 80.0 * (1.0 + 10e-6);
        assert!(
            (got - want).abs() < 100e-9,
            "residual poison: {} ns",
            (got - want).abs() * 1e9
        );
    }

    #[test]
    fn clock_jump_resets() {
        let mut m = PairModel::default();
        for i in 0..40 {
            let t = i as f64;
            m.push(t, t * (1.0 + 5e-6));
        }
        assert!(m.usable());
        // The to-clock jumps 50 ms: every new obs violates the gate, and
        // after RESET_AFTER_REJECTS the model starts fresh.
        for i in 40..(40 + RESET_AFTER_REJECTS + 1) {
            let t = i as f64;
            m.push(t, 0.050 + t * (1.0 + 5e-6));
        }
        assert!(!m.usable(), "post-jump the pair must relearn, not average");
    }

    #[test]
    fn refuses_thin_models() {
        let mut m = PairModel::default();
        m.push(0.0, 0.0);
        m.push(0.1, 0.1);
        assert!(
            m.convert(5.0).is_none(),
            "2 points over 0.1 s is not a model"
        );
    }
}
