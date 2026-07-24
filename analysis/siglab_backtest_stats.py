"""Statistical toolkit for judging siglab's own live paper-trading log.

Ported and adapted from ``../btc_5mins/bot/backtest_stats.py`` (2026-07-21), which built
this math for a different purpose: judging **backtest sweeps** in that repo. Applying it to
siglab's **live** trade log instead is new territory — see
``siglab/doc/plan_better_signal_2026-07-24.md`` for the full design rationale, and its
"Revision after DeepSeek review" section for why this module's shape differs from a literal
port in a few important ways:

- ``binomial_test_win_rate`` / ``benjamini_hochberg`` are **new** here, not in the source
  module. At siglab's current data volume (11-40 days as of this writing), an exact binomial
  test on trade-level win/loss counts needs far fewer independent observations to say
  something honest than a Sharpe-based test does on a daily-return series — see the module
  docstrings below for why. BH-FDR is applied globally across every combo's p-value, not just
  within one sweep grid, because the digest evaluates hundreds of `(market, strategy,
  variant_id)` combos at once (crypto assets x durations x variants + weather cities x
  variants) — deflating one grid's Sharpe (the source module's approach) doesn't control the
  false-positive rate across all of them together.
- ``pbo_cscv`` / ``deflated_sharpe_ratio`` are ported **faithfully** (same algorithm, same
  formulas — Bailey, Borwein, Lopez de Prado, Zhu 2015 for CSCV; Bailey & Lopez de Prado 2014
  for DSR) so they can be validated against the source module's own known-ground-truth
  sanity checks (see ``tests/test_siglab_backtest_stats.py``). They are used only as
  **group-level, informational** diagnostics here (one number per `(market, strategy)`
  16-or-18-variant grid, gated behind a much longer warm-up than the daily per-combo verdict
  needs) — never as a per-variant gate. Applying PBO's "is the best-of-N-trials pick real"
  question to an individual variant, or trusting a Sharpe estimated from a handful of daily
  PnL observations, both overstate precision the data doesn't have yet.
- ``rf_param_importance`` from the source module is **not ported**. Each `(market, strategy)`
  group has only 16-18 variants, and their swept-parameter values are far from independent
  (crypto reversal's grid is a plain 3x6 cross of two thresholds) — a random forest fit on
  that few, that-correlated a sample produces importance rankings that look precise but
  aren't. Not worth building until a real, larger, independent parameter sweep exists.

No I/O — pure functions, operating on trade records already read into memory (typically by
``siglab_daily_digest.py``, which owns reading the JSONL log).
"""

from __future__ import annotations

import itertools

import numpy as np
import pandas as pd
from scipy import stats

# ---------------------------------------------------------------------------
# Null-hypothesis win rate — identical formulas to the source module, since a
# TP/SL barrier or an efficiently-priced binary option means the same thing
# regardless of which repo is asking.
# ---------------------------------------------------------------------------


def null_win_rate_barrier(sl: float | None, tp: float | None) -> float | None:
    """Random-walk win rate for a symmetric TP/SL barrier: SL/(SL+TP).

    Applies to siglab's STOPLOSS/UNWIND outcomes — both `reversal`
    (`sl_pnl_rev`/`unwind_pnl_rev`) and `v_shape` (`sl_pnl`/`unwind_pnl`) are literal
    fractional-PnL TP/SL thresholds, so the mapping is exact. Returns ``None`` (not 0.0)
    when sl/tp aren't both a real positive barrier distance — the formula doesn't apply,
    not "the null win rate is zero".
    """
    if sl is None or tp is None or sl <= 0 or tp <= 0:
        return None
    return sl / (sl + tp)


def null_win_rate_market_implied(entry_prices) -> float | None:
    """Efficient-market null: expected win rate = mean price paid for the side bought.

    Applies to siglab's WIN/LOSS outcomes — the only path that produces them is crypto
    `reversal` held to real cycle-end resolution via `trader::machine::Machine`
    (`Machine::cycle_close` compares against Binance momentum; weather/v_shape never call
    it — see `record.rs`'s `MarketKind` doc comments). A binary option priced by an
    efficient book already encodes P(win) in its price.
    """
    entry_prices = np.asarray(list(entry_prices), dtype=float)
    entry_prices = entry_prices[~np.isnan(entry_prices)]
    if entry_prices.size == 0:
        return None
    return float(entry_prices.mean())


# ---------------------------------------------------------------------------
# Binomial test on trade-level win/loss counts — new here, not in the source module.
# ---------------------------------------------------------------------------


def binomial_test_win_rate(wins: int, n: int, null_p: float) -> dict:
    """Exact two-sided binomial test: is ``wins`` out of ``n`` trades distinguishable from
    a fair coin weighted at ``null_p``?

    Why this instead of a Sharpe-based test at siglab's current data volume: an exact
    binomial test's power comes from the **trade count**, not the number of calendar days —
    a combo with 60 trades over 11 days has a real test available today, where a
    Deflated-Sharpe-Ratio test on only 11 daily-PnL observations would be almost pure noise
    (see the module docstring and `doc/plan_better_signal_2026-07-24.md`'s "Revision after
    DeepSeek review"). It also makes no normality assumption about the return
    distribution, unlike Sharpe-based tests — appropriate for these thin-tailed,
    barrier-exit-dominated PnL series.

    Caveat this does NOT address (documented, not fixed, here): trades within the same day
    are not independent of each other (correlated market moves), so the effective sample
    size is somewhat smaller than raw ``n`` — treat the p-value as optimistic, not exact.

    Returns ``{"p_value", "realized_win_rate", "null_win_rate", "edge", "n"}``. Raises
    ValueError if ``n <= 0`` or ``null_p`` isn't in (0, 1) — callers should check
    applicability (e.g. via the null-rate functions above returning ``None``) before calling.
    """
    if n <= 0:
        raise ValueError("n must be positive")
    if not 0.0 < null_p < 1.0:
        raise ValueError("null_p must be in (0, 1)")
    result = stats.binomtest(wins, n, null_p, alternative="two-sided")
    realized = wins / n
    return {
        "p_value": float(result.pvalue),
        "realized_win_rate": realized,
        "null_win_rate": null_p,
        "edge": realized - null_p,
        "n": n,
    }


def benjamini_hochberg(pvalues: list[float], alpha: float = 0.05) -> dict:
    """Benjamini-Hochberg FDR correction across many simultaneous p-values.

    Applied globally across every combo's binomial-test p-value in one digest run (hundreds
    of `(market, strategy, variant_id)` combos at once) — without this, "PROMOTE-CANDIDATE
    at p<0.05" on hundreds of independent tests guarantees a stream of false positives by
    construction, regardless of how good any individual combo's edge really is. Same
    correction convention `../btc_5mins` already applies to its own significance tests
    (`CLAUDE.md`'s "Benjamini-Hochberg FDR correction" line).

    Returns ``{"q_values": array aligned to input order, "reject": bool array aligned to
    input order}``. Empty input returns empty arrays, not an error.
    """
    p = np.asarray(pvalues, dtype=float)
    m = p.size
    if m == 0:
        return {"q_values": np.array([]), "reject": np.array([], dtype=bool)}

    order = np.argsort(p)
    ranked = p[order]
    ranks = np.arange(1, m + 1)
    # Standard BH q-value: p * m / rank, enforced monotone via a reverse running minimum,
    # then clipped to [0, 1].
    raw_q = ranked * m / ranks
    q_sorted = np.minimum.accumulate(raw_q[::-1])[::-1]
    q_sorted = np.clip(q_sorted, 0.0, 1.0)

    q_values = np.empty(m, dtype=float)
    q_values[order] = q_sorted
    reject = q_values <= alpha
    return {"q_values": q_values, "reject": reject}


# ---------------------------------------------------------------------------
# Daily PnL panel — the T x N matrix pbo_cscv / deflated_sharpe_ratio consume.
# Adapted from the source module: siglab has no OUT_OF_SCOPE outcome (that's a bt2-only
# concept for cycles the backtest replay couldn't resolve), so that exclusion is dropped;
# everything else (zero-fill for a no-trade day, HKT-day grouping) is unchanged.
# ---------------------------------------------------------------------------


def daily_pnl_panel(combo_trade_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Build a ``T days x N combos`` daily-PnL matrix, one column per combo in
    ``combo_trade_dfs`` (same order), from each combo's own trades DataFrame (must have
    ``day`` — an HKT ``YYYY-MM-DD`` string — and ``pnl`` columns; typically produced by
    ``siglab_daily_digest.py`` grouping raw JSONL rows by combo and HKT calendar day of
    ``entry_ts``).

    A day with zero trades for a combo is a true, meaningful zero PnL, filled as such — not
    the same thing as "no data". Column order matches the input list's order, so a caller
    tracking ``combos[i] <-> dfs[i]`` can index straight back into this panel's columns by
    position.
    """
    daily_series = []
    for df in combo_trade_dfs:
        if df.empty:
            daily_series.append(pd.Series(dtype=float))
            continue
        daily_series.append(df.groupby("day")["pnl"].sum())

    panel = pd.concat(daily_series, axis=1, keys=range(len(combo_trade_dfs)))
    return panel.sort_index().fillna(0.0)


# ---------------------------------------------------------------------------
# Probability of Backtest Overfitting via Combinatorially Symmetric CV
#   Bailey, Borwein, Lopez de Prado, Zhu (2015), "The Probability of
#   Backtest Overfitting", Journal of Computational Finance.
#
# Ported FAITHFULLY from ../btc_5mins/bot/backtest_stats.py — same algorithm, same
# formulas — so this port can be validated against that module's own known-ground-truth
# sanity checks (see tests/test_siglab_backtest_stats.py). Used here only as a
# group-level (one 16-or-18-variant grid at a time), informational diagnostic, gated
# behind a much longer history warm-up than the per-combo binomial-test verdict needs —
# see siglab_daily_digest.py and the plan doc's "Revision after DeepSeek review".
# ---------------------------------------------------------------------------


def pbo_cscv(returns: pd.DataFrame, n_splits: int = 16, metric=None) -> dict:
    """Probability of Backtest Overfitting via CSCV.

    Parameters
    ----------
    returns : DataFrame, shape (T periods, N combos)
        Per-period (daily) PnL for every candidate combo, all on the identical time grid —
        see ``daily_pnl_panel``.
    n_splits : int, must be even. Bailey et al. use 16 (``C(16,8)=12,870`` splits); siglab
        calls this with a smaller value (see the digest script) while its history is still
        short — each split's blocks get noisier the fewer days there are, which is exactly
        why this stays gated behind a warm-up threshold rather than being trusted from day 1.
    metric : callable(pd.Series) -> float, optional. Falls back to a slower per-split
        ``DataFrame.apply`` path. ``None`` (default) uses the vectorized Sharpe-like
        mean/std metric via precomputed per-block sufficient statistics.

    Returns dict with ``pbo`` (fraction of splits where the in-sample-best combo lands
    at/below the OOS median), ``mean_logit``, and the raw per-split logits.
    """
    if n_splits % 2 != 0:
        raise ValueError("n_splits must be even")
    if metric is not None:
        return _pbo_cscv_generic(returns, n_splits, metric)
    return _pbo_cscv_fast(returns, n_splits)


def _pbo_cscv_generic(returns: pd.DataFrame, n_splits: int, metric) -> dict:
    T, N = returns.shape
    blocks = np.array_split(np.arange(T), n_splits)
    logits = []
    for train_ids in itertools.combinations(range(n_splits), n_splits // 2):
        test_ids = [b for b in range(n_splits) if b not in train_ids]
        train_idx = np.concatenate([blocks[b] for b in train_ids])
        test_idx = np.concatenate([blocks[b] for b in test_ids])

        is_perf = returns.iloc[train_idx].apply(metric, axis=0)
        oos_perf = returns.iloc[test_idx].apply(metric, axis=0)

        best_combo = is_perf.idxmax()
        rank = oos_perf.rank(method="average")[best_combo]
        omega = min(max(rank / (N + 1), 1e-6), 1 - 1e-6)
        logits.append(np.log(omega / (1 - omega)))

    logits = np.array(logits)
    return {
        "pbo": float(np.mean(logits <= 0)),
        "n_splits_evaluated": len(logits),
        "mean_logit": float(np.mean(logits)),
        "logits": logits,
    }


def _pbo_cscv_fast(returns: pd.DataFrame, n_splits: int) -> dict:
    """Vectorized default-metric path: precompute each block's sum/sumsq/n once, then
    every split's train/test Sharpe is a handful of numpy sums over the (n_splits, N)
    block-stat arrays instead of re-slicing the raw (T, N) panel per split."""
    values = returns.to_numpy(dtype=float)
    T, N = values.shape
    row_blocks = np.array_split(np.arange(T), n_splits)

    block_sum = np.stack([values[rows].sum(axis=0) for rows in row_blocks])  # (S, N)
    block_sumsq = np.stack([(values[rows] ** 2).sum(axis=0) for rows in row_blocks])  # (S, N)
    block_n = np.array([len(rows) for rows in row_blocks])  # (S,)

    def _sharpe(sum_, sumsq_, n_):
        if n_ <= 1:
            return np.zeros(N)
        mean = sum_ / n_
        var = np.maximum(sumsq_ / n_ - mean**2, 0.0) * n_ / (n_ - 1)
        std = np.sqrt(var)
        return np.divide(mean, std, out=np.zeros(N), where=std > 0)

    logits = []
    for train_ids in itertools.combinations(range(n_splits), n_splits // 2):
        test_ids = [b for b in range(n_splits) if b not in train_ids]
        train_ids_arr = np.array(train_ids)
        test_ids_arr = np.array(test_ids)

        is_perf = _sharpe(
            block_sum[train_ids_arr].sum(axis=0),
            block_sumsq[train_ids_arr].sum(axis=0),
            block_n[train_ids_arr].sum(),
        )
        oos_perf = _sharpe(
            block_sum[test_ids_arr].sum(axis=0),
            block_sumsq[test_ids_arr].sum(axis=0),
            block_n[test_ids_arr].sum(),
        )

        best_combo = int(np.argmax(is_perf))
        rank = stats.rankdata(oos_perf, method="average")[best_combo]
        omega = min(max(rank / (N + 1), 1e-6), 1 - 1e-6)
        logits.append(np.log(omega / (1 - omega)))

    logits = np.array(logits)
    return {
        "pbo": float(np.mean(logits <= 0)),
        "n_splits_evaluated": len(logits),
        "mean_logit": float(np.mean(logits)),
        "logits": logits,
    }


# ---------------------------------------------------------------------------
# Deflated Sharpe Ratio — Bailey & Lopez de Prado (2014).
# Ported FAITHFULLY, same reasoning as pbo_cscv above: group-level, informational,
# warm-up-gated, never a per-variant verdict gate.
# ---------------------------------------------------------------------------


def deflated_sharpe_ratio(
    sharpe_hat: float,
    n_trials: int,
    trial_sharpe_var: float,
    n_obs: int,
    skew: float = 0.0,
    kurtosis: float = 3.0,
) -> dict:
    """DSR: is the best-of-N-trials Sharpe still significant after deflating for how many
    trials were tried and how non-normal the returns are?

    sharpe_hat: observed per-period Sharpe of the selected/best combo.
    n_trials: N, size of the trial pool the winner was picked from (16 or 18 for siglab's
        grids).
    trial_sharpe_var: variance of Sharpe ratios across that trial pool (V).
    n_obs: T, number of return observations backing sharpe_hat (day count for siglab's
        daily-panel Sharpe — kept deliberately small-sample-honest by the warm-up gate in
        siglab_daily_digest.py, not used until enough days exist for this to mean anything).
    skew, kurtosis: skew / (non-excess, normal=3) kurtosis of the selected combo's own
        per-period returns.
    """
    euler_gamma = 0.5772156649015329
    if n_trials <= 1:
        sr0 = 0.0
    else:
        z1 = stats.norm.ppf(1 - 1.0 / n_trials)
        z2 = stats.norm.ppf(1 - 1.0 / (n_trials * np.e))
        sr0 = np.sqrt(max(trial_sharpe_var, 0.0)) * (
            (1 - euler_gamma) * z1 + euler_gamma * z2
        )
    denom = np.sqrt(
        max(1e-12, 1 - skew * sharpe_hat + ((kurtosis - 1) / 4) * sharpe_hat**2)
    )
    z = (sharpe_hat - sr0) * np.sqrt(n_obs - 1) / denom
    return {
        "dsr_zscore": float(z),
        "dsr_pvalue": float(stats.norm.cdf(z)),
        "expected_max_sharpe_null": float(sr0),
    }
