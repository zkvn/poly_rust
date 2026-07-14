# Study: does the (reversal_low, reversal_high) grid position predict PnL sign?

## Status

**Implemented and run.** Plan reviewed by DeepSeek (deepseek-v4-pro) before any code was
written — see "DeepSeek review" below; "Method (revised)" and "Success criterion" reflect
the fixes. Full study, code, and per-segment results:
`../../btc_5mins/studies/reversal_high_low/` (`PLAN.md`, `run_study.py`, `summary.md`,
`results/`).

## Findings

**No clear pattern in either crypto segment; non-crypto has too little data to judge.**

| segment | rows (clusters) | best GroupKFold ROC-AUC | BH p-value | verdict |
|---|---|---|---|---|
| 5m crypto | 6,317 (1,072) | 0.527 (logistic regression) | 0.32 (not significant) | no clear pattern |
| 15m crypto | 3,611 (884) | 0.547 (logistic regression) | 0.0015 (significant) | no clear pattern — real but too weak |
| non-crypto | 778 (371) | descriptive only (<2,000-row bar) | — | insufficient data |

15m crypto shows a statistically real effect (comfortably outside the cluster-permuted null,
p=0.0015) but it falls short of even the 0.55 "suggestive" floor, let alone the 0.60 "clear
pattern" bar — logistic regression and random forest agree on direction (lower
`reversal_high` slightly favored) but the signal is too weak to act on alone. 5m crypto shows
nothing statistically detectable (p=0.32). This is a genuine, useful null result for the
grid's own stated purpose ("see which of them look good on real data") — consistent with,
and explained by, the two 2026-07-14 timestamp incidents' finding that grid cells frequently
co-fire at the same real price because a shared gate (not the cell's own threshold) is the
actual binding constraint: if the threshold barely determines *when* a variant enters, it's
unsurprising it barely determines *whether* that entry wins. Full nuance, caveats (mixed
gate-tightness regimes across the sample window, single ~1.3-day snapshot, not yet
walk-forward validated), and per-segment detail in the study's `summary.md`.

## DeepSeek review

Full plan sent for critique before any code was written. Summary of the flaws it found, all
accepted and folded into "Method (revised)" below:

1. **Chi-square baseline is invalid on clustered data.** Treating every row as independent
   inflates the effective sample size — 18 near-identical rows from one shared-gate release
   event get counted as 18 independent pieces of evidence, making the test spuriously
   significant almost by construction. *Fix: dropped as an inferential test; kept only as a
   descriptive heatmap computed on deduplicated data (one row per `(market, entry_ts)`
   cluster).*
2. **`reversal_high - reversal_low` as a 3rd feature is exact multicollinearity** with the
   other two (it's a linear combination, not an interaction — an actual interaction would be
   `low × high`). Produces unstable/meaningless coefficients. *Fix: logistic regression uses
   only `reversal_low`, `reversal_high` — nothing derived.*
3. **The permutation-test null also ignores clustering** — shuffling `pnl_positive`
   per-row breaks within-cluster correlation, narrowing the null distribution and making
   p-values anti-conservative (false "significance"). *Fix: permute at the cluster level —
   shuffle labels for whole `(market, entry_ts)` groups, not individual rows.*
4. **`GroupKFold` grouped by exact-float `entry_ts` can still leak** if near-duplicate
   clusters are released a moment apart rather than at the literal same timestamp. *Fix:
   group by `(market, round(entry_ts))` — 1-second rounding, based on the two 2026-07-14
   incident docs' evidence that same-cluster entries share `entry_ts` to the microsecond, not
   just the same second, so this is a safely inclusive window.*
5. **Inconsistent class-imbalance handling** — `class_weight="balanced"` was only specified
   for the tree models, not logistic regression, making cross-model comparison confounded by
   differing objectives. *Fix: `class_weight="balanced"` applied to all three models
   uniformly.*
6. **Non-crypto's 778-row sample is too thin and too heterogeneous (weather + World Cup
   combined) for the full inferential pipeline** — sparse grid cells risk complete
   separation and unreliable estimates. *Fix: non-crypto gets a descriptive heatmap only, no
   permutation test or success-criterion claim; explicitly reported as "insufficient data,"
   not silently run through the same pipeline as the crypto segments.*
7. **AUC≥0.55 is too lenient** to call "a clear pattern worth acting on," especially given
   thousands of rows in the crypto segments make even a true AUC of 0.55 easy to detect as
   "significant." *Fix: raised the floor to AUC≥0.60 for a "clear pattern" claim; 0.55-0.60
   is reported as "suggestive, not actionable."*
8. **1,000 full 5-fold `GroupKFold` refits for the permutation test (5,000 total model
   fits) is impractical** and not how a CV-based permutation test is normally structured.
   *Fix: permutation test runs on one representative grouped train/test split; the 5-fold
   `GroupKFold` estimate is reported separately as the point estimate, not re-run inside the
   permutation loop.*

Also adopted: fixed `random_state` throughout; "direction/shape consistent" between logistic
regression and random forest is operationalized as sign-agreement between the logistic
coefficient and the random forest's partial-dependence slope; documented (not solved) the
implicit stationarity assumption from treating `entry_ts`-ordered data as exchangeable within
`GroupKFold` — flagged as a known limitation rather than adding a `TimeSeriesSplit`, since
DeepSeek's own assessment was that the impact is small with purely static (low, high)
features and full sequential splitting would meaningfully shrink the already-thin non-crypto
segment further.

## Background

`siglab`'s 18-variant reversal grid (`config/markets.toml`) sweeps `reversal_low_threshold`
∈ {0.2, 0.3, 0.4} × `reversal` ∈ {0.55, 0.6, 0.65, 0.7, 0.75, 0.8} — "low" is the dip level
that must be seen before a variant arms, "high" is the recovery level that fires entry. Every other
parameter (`sl_pnl_rev`, `unwind_pnl_rev`, `unwind_time_rev`, `delta_pct_rev`,
`price_high_rev`) is fixed and shared across all 18 (see `markets.toml`'s header comment for
the naming convention). The grid's own stated purpose (`markets.toml`) is "to see which of
them (if any) look good on real data" — this study asks that question directly and
quantitatively: **does a trade's (low, high) pair predict whether its PnL comes out positive
or negative**, and is any such relationship strong enough to act on (vs. noise)?

## Data

Source: `siglab`'s own paper-trade JSONL (`SiglabTradeRecord`, `/app/logs/siglab_trades.jsonl`
inside the `siglab-siglab-1` container) — **not** `btc_5mins`'s own price/backtest data, per
instruction ("use the data here"). A snapshot is exported into the study folder at
`btc_5mins/studies/reversal_high_low/data/siglab_trades.jsonl` (`docker cp` from the running
container) so the study is reproducible without a live container dependency; refreshing it is
a one-line `docker cp` documented in the study's own `PLAN.md`.

Filtered to `strategy == "reversal"` (the only strategy this grid applies to — `high_prob`
variants were removed from `markets.toml` 2026-07-13) and `variant_id` matching
`reversal_{low}_{high}` (excludes ~3 pre-grid historical rows like `reversal_btc`). Three
segments, exactly as requested:

| Segment | Filter | Row count (2026-07-13 11:59 → 2026-07-14 18:53 HKT, current snapshot) |
|---|---|---|
| 5m crypto | `market_kind=="crypto"` and `market` ends `-5m` | 6,317 |
| 15m crypto | `market_kind=="crypto"` and `market` ends `-15m` | 3,611 |
| non-crypto | `market_kind` in `{weather, worldcup}` | 778 |

(4h and hourly-et crypto markets are excluded — out of scope for the requested 3-way split;
noted so their omission isn't silently assumed innocuous.) Non-crypto's much smaller sample
is flagged as a real limitation below, not glossed over — 778 rows ÷ 18 grid cells ≈ 43
rows/cell on average, thin enough that per-cell estimates will be noisy.

**Feature construction**: `reversal_low`, `reversal_high` parsed directly from `variant_id`
(exact match to the config, not re-derived/estimated). Target: `pnl_positive = pnl > 0`
(binary). Deliberately **not** including `outcome` (STOPLOSS/TIMEOUT/UNWIND) as a feature —
outcome is mechanically entangled with PnL sign (e.g. STOPLOSS is loss by construction), so
using it would answer "does the exit type predict the exit type," not the actual question.

**Critical caveat — rows are not i.i.d.** Confirmed in
`siglab/doc/incident_same_entry_ts_2026-07-14.md` and
`incident_reversal_variant_correlated_timestamps_2026-07-14.md`: many trades within the same
(market, entry tick) cluster share an *identical* real fill price/PnL across several (low,
high) combinations simultaneously, whenever a shared gate (delta_pct, staleness) — not the
threshold itself — is the actual binding constraint that released them together. Naively
treating every row as an independent observation would let these near-duplicate clusters
dominate a model and inflate apparent "signal" that's really just autocorrelation. Mitigation
(see Method): **`GroupKFold` cross-validation grouped by `(market, entry_ts)`** so
train/test splits never separate rows from the same real market event, and a **duplicate-rate
report per segment** (what fraction of rows share an exact `(market, entry_ts)` with ≥1 other
row) so the reader can judge how much this matters before trusting any model output.

## Method (revised per DeepSeek review)

Grouping key used throughout: `cluster_key = (market, round(entry_ts))` — 1-second rounding
(see review point 4). `dedup_row` = one representative row per `cluster_key` (first by
`variant_id` sort order), used only for the descriptive heatmap, never for model
fitting/evaluation (models use every row — the point of `GroupKFold` is to keep clusters
whole across the train/test split, not to discard the within-cluster variation across
variants).

**Crypto segments (5m, 15m) — full pipeline:**

1. **Descriptive baseline**: win-rate heatmap over the 3×6 grid (`reversal_low` ×
   `reversal_high`) computed on `dedup_row`s (one vote per real market event, not one per
   variant) — no p-value claimed, purely descriptive, and directly comparable across
   segments.
2. **Logistic regression** — features `reversal_low`, `reversal_high` only (no derived
   `high-low` term — exact multicollinearity, per review point 2), `class_weight="balanced"`,
   fixed `random_state`. Coefficients + odds ratios.
3. **Shallow decision tree** (`max_depth=3`, `class_weight="balanced"`, fixed
   `random_state`) — human-readable split rules, a sanity check on whether logistic
   regression's linear assumption holds or the real boundary is a step function.
4. **Random forest** (`class_weight="balanced"`, modest `n_estimators`/`max_depth`, fixed
   `random_state`) — captures non-linear/non-monotonic low×high interaction. Reported via
   **permutation importance** (not impurity-based `feature_importances_`, which is biased
   toward continuous high-split-count features — review point/Sources) plus a partial
   dependence plot per feature.
5. **Point-estimate evaluation**: `GroupKFold` (5-fold, grouped by `cluster_key`) reporting
   accuracy, ROC-AUC, and PR-AUC (more informative than ROC-AUC under class imbalance) for
   models 2-4, plus a confusion matrix at the 0.5 threshold.
6. **Significance test** (review points 3 + 8): a *single* grouped train/test split
   (`GroupShuffleSplit`, grouped by `cluster_key`, ~75/25), fit once on real labels for the
   observed AUC, then 1,000 permutations that shuffle `pnl_positive` **at the cluster
   level** (every row sharing a `cluster_key` gets the same shuffled label, preserving
   within-cluster correlation under the null) — refit on that one split per permutation, not
   the full 5-fold CV. Reports the empirical p-value of the observed AUC against this null.
7. **Cross-model consistency** (review "minor considerations"): sign-agreement between the
   logistic regression coefficient and the random forest's partial-dependence slope, per
   feature.

**Non-crypto segment — descriptive only (review point 6):** step 1's heatmap, plus row/cluster
counts per grid cell, explicitly reported as insufficient for steps 2-7 (778 rows over 18
cells ≈ 43/cell, further thinned by weather/World Cup heterogeneity) — no model fit, no
p-value, no success-criterion claim for this segment.

## Success criterion

For the two crypto segments only (non-crypto is descriptive-only per above): a "clear
pattern" is claimed only if **(a)** the permutation-test p<0.05 (Benjamini-Hochberg corrected
across the segment's 3 models, same convention as `btc_5mins/studies/bt2.py`) on at least one
model's AUC, **and** **(b)** that AUC clears **0.60** (raised from an initial 0.55 per
review point 7 — 0.55 is too easy to hit "significantly" at these sample sizes without being
practically meaningful), **and** **(c)** logistic regression and random forest agree in sign
(step 7). Any segment/model failing this bar is reported as "no clear pattern" or
"suggestive, not actionable" (AUC 0.55-0.60, or single-model-only) — genuine, useful outcomes,
not failures of the study, matching how `btc_5mins/studies/` phrases existing null findings
(e.g. `pup_gate`'s "0/24 cells pass").

## Deliverables (btc_5mins/studies/reversal_high_low/, following that repo's study layout)

```
btc_5mins/studies/reversal_high_low/
├── PLAN.md                 (this plan, adapted to that repo's template)
├── data/
│   └── siglab_trades.jsonl (snapshot exported from poly_rust/siglab; refresh instructions in PLAN.md)
├── run_study.py             (entry point — loops the 3 segments instead of coins)
└── results/
    ├── _index.md            (one-line run log, appended each run)
    └── <segment>_<start>_<end>_<YYYYMMDD_HHMMSS>/
        ├── summary.md
        └── *.csv / *.png    (heatmaps, ROC/PR curves, permutation-importance bars)
```

## Model list — resolved

Kept exactly as originally scoped (logistic regression → decision tree → random forest, per
instruction "logistic regression to forest method") — no gradient-boosted tree added. With
only 2 features and ≤18 distinct combinations, a random forest already has ample capacity to
capture any non-linear structure; adding `HistGradientBoostingClassifier` would mostly add
another way to overfit the same tiny feature space without a proportionate interpretability
gain, and the study's goal is "is there a clear pattern," not squeezing out marginal AUC.

## Sources (web research on classification technique choice, consulted per instruction)

- Logistic regression vs. random forest for imbalanced/small classification tasks, and why
  PR-AUC often beats ROC-AUC under imbalance: <https://machinelearningmastery.com/bagging-and-random-forest-for-imbalanced-classification/>,
  <https://ieeexplore.ieee.org/document/8754250/>
- Random forest feature importance bias toward high-split-count continuous features, and why
  permutation importance is the more trustworthy measure here:
  <https://arxiv.org/pdf/2502.07153>, <https://towardsdatascience.com/explaining-feature-importance-by-example-of-a-random-forest-d9166011959e/>
