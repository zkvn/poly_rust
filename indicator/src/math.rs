//! Pure math ports of `bot/signals.py`'s `_compute_pup` / `_compute_snr` and
//! the rv_5s realized-vol estimator. All functions are side-effect free and
//! deterministic — the parity harness (`indicator replay`) runs exactly these
//! against the Python originals on identical inputs.

/// Student-t CDF via the regularized incomplete beta function:
/// `I_x(ν/2, 1/2)` with `x = ν/(ν+t²)` — the same identity scipy's `stdtr`
/// (cephes) uses, so agreement is at f64 rounding level.
pub fn student_t_cdf(nu: f64, t: f64) -> f64 {
    if !t.is_finite() {
        return if t > 0.0 { 1.0 } else { 0.0 };
    }
    let x = nu / (nu + t * t);
    let ib = puruspe::betai(nu / 2.0, 0.5, x);
    if t >= 0.0 { 1.0 - 0.5 * ib } else { 0.5 * ib }
}

/// Standard normal CDF — `Φ(z) = (1 + erf(z/√2)) / 2`, matching Python's
/// `math.erf` path in streaming mode.
pub fn normal_cdf(z: f64) -> f64 {
    0.5 * (1.0 + puruspe::erf(z / std::f64::consts::SQRT_2))
}

/// Realized vol of one cycle from its 1-Hz price series: `step`-second
/// subsample (`prices[step-1::step]`), log returns, `sqrt(Σ r²)`.
/// `None` when the cycle has fewer than `min_ticks` samples or fewer than two
/// subsampled prices — mirrors `VolHarSignal.reset`'s validity gate.
pub fn realized_vol(prices: &[f64], min_ticks: usize, step: usize) -> Option<f64> {
    if prices.len() < min_ticks || step == 0 {
        return None;
    }
    let sub: Vec<f64> = prices
        .iter()
        .skip(step - 1)
        .step_by(step)
        .copied()
        .collect();
    if sub.len() < 2 {
        return None;
    }
    let sum_sq: f64 = sub.windows(2).map(|w| (w[1] / w[0]).ln().powi(2)).sum();
    Some(sum_sq.sqrt())
}

/// Sample std-dev (ddof=1) of in-cycle 1-Hz simple returns — the streaming-mode
/// volatility. `None` with fewer than 3 prices (< 2 returns).
fn streaming_vol(prices: &[f64]) -> Option<f64> {
    if prices.len() < 3 {
        return None;
    }
    let returns: Vec<f64> = prices.windows(2).map(|w| w[1] / w[0] - 1.0).collect();
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    Some(var.sqrt())
}

/// P(up): probability the price finishes above `cycle_open` at cycle end.
///
/// HAR mode (`sigma_full` provided): Student-t on
/// `z_τ = ln(p_now/p_open) / (σ_full · √(τ/cycle_period))`, variance-adjusted
/// by `√(ν/(ν−2))`. Streaming mode: Gaussian on
/// `z = ((p_now−p_open)/p_open) / (σ_stream · √τ)`.
///
/// Returns 0.5 in every not-ready case, exactly like Python's `_compute_pup`.
#[allow(clippy::too_many_arguments)]
pub fn compute_pup(
    prices: &[f64],
    cycle_open: f64,
    seconds_remaining: f64,
    sigma_full: Option<f64>,
    nu: f64,
    cycle_period_secs: f64,
) -> f64 {
    if seconds_remaining <= 0.0 || cycle_open <= 0.0 {
        return 0.5;
    }
    if let Some(sigma) = sigma_full
        && sigma > 0.0
    {
        let Some(&last) = prices.last() else {
            return 0.5;
        };
        if last <= 0.0 {
            return 0.5;
        }
        let numerator = (last / cycle_open).ln();
        let denom = sigma * (seconds_remaining / cycle_period_secs).sqrt();
        if denom <= 0.0 {
            return 0.5;
        }
        let z = numerator / denom;
        let adj = (nu / (nu - 2.0)).sqrt();
        return student_t_cdf(nu, z * adj);
    }
    // streaming mode
    let Some(vol) = streaming_vol(prices) else {
        return 0.5;
    };
    let last = prices[prices.len() - 1];
    let cumulative_return = (last - cycle_open) / cycle_open;
    let denom = vol * seconds_remaining.sqrt();
    if denom <= 0.0 {
        return 0.5;
    }
    normal_cdf(cumulative_return / denom)
}

/// SNR: the signed z-score (no CDF) — same numerator/denominator switch as
/// `compute_pup`. `None` when not ready; `0.0` only for genuine zero
/// displacement. Mirrors Python's `_compute_snr`.
pub fn compute_snr(
    prices: &[f64],
    cycle_open: f64,
    seconds_remaining: f64,
    sigma_full: Option<f64>,
    cycle_period_secs: f64,
) -> Option<f64> {
    if cycle_open <= 0.0 || seconds_remaining <= 0.0 {
        return None;
    }
    if let Some(sigma) = sigma_full
        && sigma > 0.0
    {
        let &last = prices.last()?;
        if last <= 0.0 {
            return None;
        }
        let numerator = (last / cycle_open).ln();
        let denom = sigma * (seconds_remaining / cycle_period_secs).sqrt();
        if denom <= 0.0 {
            return None;
        }
        return Some(numerator / denom);
    }
    // streaming mode
    let vol = streaming_vol(prices)?;
    let last = prices[prices.len() - 1];
    let cumulative_return = (last - cycle_open) / cycle_open;
    let denom = vol * seconds_remaining.sqrt();
    if denom <= 0.0 {
        return None;
    }
    Some(cumulative_return / denom)
}

/// Generalized HAR forecast state — `windows = [1, 5, 12]` with a 4-element
/// beta reproduces `VolHarSignal` exactly (`mean(rv[-1:]) == rv[-1]`); any
/// other ascending window set (e.g. `[1, 3, 6]`) is a config edit away.
/// Means use `min_periods=1` semantics: each window averages over however many
/// cycles are available, up to its size.
#[derive(Debug, Clone)]
pub struct HarState {
    windows: Vec<usize>,
    beta: Vec<f64>,
    buf: std::collections::VecDeque<f64>,
    val: Option<f64>,
}

impl HarState {
    /// `beta.len()` must be `windows.len() + 1` (intercept first) and windows
    /// strictly ascending and non-zero — validated by config loading; this
    /// constructor enforces it again defensively.
    pub fn new(windows: Vec<usize>, beta: Vec<f64>) -> Result<Self, String> {
        if windows.is_empty() || windows[0] == 0 {
            return Err("har windows must be non-empty and positive".into());
        }
        if !windows.windows(2).all(|w| w[0] < w[1]) {
            return Err(format!(
                "har windows must be strictly ascending: {windows:?}"
            ));
        }
        if beta.len() != windows.len() + 1 {
            return Err(format!(
                "har beta must have {} elements (intercept + one per window), got {}",
                windows.len() + 1,
                beta.len()
            ));
        }
        Ok(Self {
            windows,
            beta,
            buf: std::collections::VecDeque::new(),
            val: None,
        })
    }

    /// Feed one sealed cycle's realized vol (`None` = invalid cycle, buffer
    /// untouched) and recompute the forecast from whatever history exists —
    /// the exact `VolHarSignal.reset` update order.
    pub fn on_cycle(&mut self, rv: Option<f64>) {
        let cap = *self.windows.last().expect("windows non-empty");
        if let Some(rv) = rv {
            if self.buf.len() == cap {
                self.buf.pop_front();
            }
            self.buf.push_back(rv);
        }
        if !self.buf.is_empty() {
            let buf: Vec<f64> = self.buf.iter().copied().collect();
            let mut acc = self.beta[0];
            for (i, w) in self.windows.iter().enumerate() {
                let n = (*w).min(buf.len());
                let mean = buf[buf.len() - n..].iter().sum::<f64>() / n as f64;
                acc += self.beta[i + 1] * mean;
            }
            self.val = Some(acc.max(0.0));
        }
    }

    /// Current full-cycle forecast σ_full; `None` during warmup.
    pub fn value(&self) -> Option<f64> {
        self.val
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-12;

    // ── CDFs ──────────────────────────────────────────────────────────────

    #[test]
    fn student_t_cdf_matches_scipy_reference_values() {
        // scipy.special.stdtr(nu, t) — printed at f64 precision.
        for (nu, t, want) in [
            (4.2, 0.0, 0.5),
            (4.2, 1.0, 0.8143068986429945),
            (4.2, -1.0, 0.18569310135700545),
            (4.2469, 2.5, 0.9684171643343453),
            (4.4959, -3.0, 0.017210319015314373),
            (3.0, 0.5, 0.6742760175759245),
        ] {
            let got = student_t_cdf(nu, t);
            assert!(
                (got - want).abs() < 1e-10,
                "stdtr({nu},{t}): got {got}, want {want}"
            );
        }
    }

    #[test]
    fn normal_cdf_matches_reference_values() {
        for (z, want) in [
            (0.0, 0.5),
            (1.0, 0.8413447460685429),
            (-1.0, 0.15865525393145707),
            (2.5, 0.9937903346742238),
        ] {
            assert!((normal_cdf(z) - want).abs() < 1e-12, "phi({z})");
        }
    }

    // ── realized vol ──────────────────────────────────────────────────────

    #[test]
    fn realized_vol_matches_hand_computed_python_slicing() {
        // 30 prices, step 5 → python prices[4::5] = indices 4,9,14,19,24,29.
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + i as f64 * 0.1).collect();
        let sub: Vec<f64> = vec![
            prices[4], prices[9], prices[14], prices[19], prices[24], prices[29],
        ];
        let want: f64 = sub
            .windows(2)
            .map(|w| (w[1] / w[0]).ln().powi(2))
            .sum::<f64>()
            .sqrt();
        let got = realized_vol(&prices, 30, 5).expect("valid");
        assert!((got - want).abs() < EPS);
    }

    #[test]
    fn realized_vol_rejects_short_cycles() {
        let prices: Vec<f64> = (0..29).map(|i| 100.0 + i as f64).collect();
        assert!(realized_vol(&prices, 30, 5).is_none(), "under min_ticks");
        // ≥ min_ticks but fewer than 2 subsampled prices (step too large).
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + i as f64).collect();
        assert!(realized_vol(&prices, 30, 40).is_none());
    }

    // ── p_up / snr, HAR mode ──────────────────────────────────────────────

    #[test]
    fn pup_har_mode_matches_python_formula() {
        // Hand-computed: z = ln(100.2/100)/(0.001*sqrt(150/300)) ≈ 2.8256025,
        // p = stdtr(4.2, z*sqrt(4.2/2.2)) — scipy → 0.9920380300844064.
        let prices = vec![100.1, 100.2];
        let p = compute_pup(&prices, 100.0, 150.0, Some(0.001), 4.2, 300.0);
        assert!((p - 0.992_038_030_084_406_4).abs() < 1e-9, "got {p}");
    }

    #[test]
    fn pup_warmup_and_edge_cases_return_half() {
        assert_eq!(compute_pup(&[], 100.0, 150.0, Some(0.001), 4.2, 300.0), 0.5);
        assert_eq!(
            compute_pup(&[100.0], 100.0, 0.0, Some(0.001), 4.2, 300.0),
            0.5
        );
        assert_eq!(
            compute_pup(&[100.0], 0.0, 150.0, Some(0.001), 4.2, 300.0),
            0.5
        );
        // HAR warmup (sigma None) with < 3 prices → streaming not-ready → 0.5.
        assert_eq!(
            compute_pup(&[100.0, 100.1], 100.0, 150.0, None, 4.2, 300.0),
            0.5
        );
    }

    #[test]
    fn snr_har_mode_is_z_without_cdf() {
        let prices = vec![100.1, 100.2];
        let want = (100.2f64 / 100.0).ln() / (0.001 * (150.0f64 / 300.0).sqrt());
        let got = compute_snr(&prices, 100.0, 150.0, Some(0.001), 300.0).expect("ready");
        assert!((got - want).abs() < EPS);
    }

    #[test]
    fn snr_not_ready_is_none_zero_displacement_is_zero() {
        assert!(compute_snr(&[], 100.0, 150.0, Some(0.001), 300.0).is_none());
        assert!(compute_snr(&[100.0], 100.0, 0.0, Some(0.001), 300.0).is_none());
        let got = compute_snr(&[100.0], 100.0, 150.0, Some(0.001), 300.0).expect("ready");
        assert_eq!(got, 0.0, "zero displacement must be 0.0, not None");
    }

    // ── p_up / snr, streaming mode (locked against bot/signals.py vectors) ──

    #[test]
    fn pup_streaming_matches_python_reference() {
        // Python: _compute_pup([100.0,100.1,100.05,100.2], 100.0, 120.0)
        // returns Φ(cum_ret / (std(returns, ddof=1) * sqrt(120))).
        let prices = vec![100.0, 100.1, 100.05, 100.2];
        let returns: Vec<f64> = prices.windows(2).map(|w| w[1] / w[0] - 1.0).collect();
        let mean = returns.iter().sum::<f64>() / 3.0;
        let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / 2.0;
        let z = (0.2 / 100.0) / (var.sqrt() * 120.0f64.sqrt());
        let want = normal_cdf(z);
        let got = compute_pup(&prices, 100.0, 120.0, None, 4.2, 300.0);
        assert!((got - want).abs() < EPS);
    }

    #[test]
    fn snr_streaming_needs_three_prices() {
        assert!(compute_snr(&[100.0, 100.1], 100.0, 120.0, None, 300.0).is_none());
        assert!(compute_snr(&[100.0, 100.1, 100.2], 100.0, 120.0, None, 300.0).is_some());
    }

    // ── HAR state ─────────────────────────────────────────────────────────

    fn python_har_1_5_12(beta: &[f64], rvs: &[f64]) -> f64 {
        // Direct transcription of VolHarSignal.reset's forecast block.
        let buf: Vec<f64> = rvs.iter().rev().take(12).rev().copied().collect();
        let short = buf[buf.len() - 1];
        let n5 = buf.len().min(5);
        let mean5 = buf[buf.len() - n5..].iter().sum::<f64>() / n5 as f64;
        let mean12 = buf.iter().sum::<f64>() / buf.len() as f64;
        (beta[0] + beta[1] * short + beta[2] * mean5 + beta[3] * mean12).max(0.0)
    }

    #[test]
    fn har_1_5_12_matches_python_semantics_through_warmup() {
        let beta = vec![6.753e-5, 0.3809, 0.2301, 0.3215];
        let mut har = HarState::new(vec![1, 5, 12], beta.clone()).expect("valid");
        assert!(har.value().is_none(), "no forecast before first cycle");
        let rvs: Vec<f64> = (1..=15).map(|i| 0.0005 + 0.0001 * i as f64).collect();
        for (i, rv) in rvs.iter().enumerate() {
            har.on_cycle(Some(*rv));
            let want = python_har_1_5_12(&beta, &rvs[..=i]);
            let got = har.value().expect("forecast after ≥1 cycle");
            assert!(
                (got - want).abs() < EPS,
                "cycle {i}: got {got}, want {want}"
            );
        }
    }

    #[test]
    fn har_invalid_cycle_keeps_buffer_and_recomputes_nothing_new() {
        let mut har = HarState::new(vec![1, 5, 12], vec![0.0, 1.0, 0.0, 0.0]).expect("valid");
        har.on_cycle(Some(0.002));
        let before = har.value();
        har.on_cycle(None); // short cycle — Python leaves buffer alone, recomputes same value
        assert_eq!(har.value(), before);
    }

    #[test]
    fn har_generalized_windows_1_3_6() {
        // windows [1,3,6], beta len 4 — buffer caps at 6, mean windows 1/3/6.
        let beta = vec![0.0001, 0.4, 0.3, 0.2];
        let mut har = HarState::new(vec![1, 3, 6], beta.clone()).expect("valid");
        let rvs = [0.001, 0.002, 0.0015, 0.0025, 0.003, 0.001, 0.002, 0.0018];
        for rv in rvs {
            har.on_cycle(Some(rv));
        }
        let buf: Vec<f64> = rvs[rvs.len() - 6..].to_vec();
        let mean1 = buf[5];
        let mean3 = (buf[3] + buf[4] + buf[5]) / 3.0;
        let mean6 = buf.iter().sum::<f64>() / 6.0;
        let want = (beta[0] + beta[1] * mean1 + beta[2] * mean3 + beta[3] * mean6).max(0.0);
        let got = har.value().expect("forecast");
        assert!((got - want).abs() < EPS);
    }

    #[test]
    fn har_forecast_floors_at_zero() {
        let mut har = HarState::new(vec![1], vec![-1.0, 0.0]).expect("valid");
        har.on_cycle(Some(0.001));
        assert_eq!(har.value(), Some(0.0));
    }

    #[test]
    fn har_rejects_bad_shapes() {
        assert!(HarState::new(vec![], vec![0.0]).is_err());
        assert!(HarState::new(vec![0, 5], vec![0.0, 0.0, 0.0]).is_err());
        assert!(HarState::new(vec![5, 1], vec![0.0, 0.0, 0.0]).is_err());
        assert!(HarState::new(vec![1, 5, 12], vec![0.0, 0.0]).is_err());
    }
}
