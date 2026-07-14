# Studies index

Research findings and standalone analyses. Each theme gets its own directory; read the relevant
entry below before starting a new study to avoid duplicating prior work.

| Date | Theme | Summary | Doc |
|---|---|---|---|
| 2026-07-12 | weather | Feasibility research: Polymarket weather (temperature) markets — market structure, order-book depth ($500 order ≈ 30% slippage on near-money legs), whether the current bot can support it (not without a new strategy module), forecast-model accuracy, and why the "forecast latency arbitrage" edge isn't trivially free money. No data collected yet — recommends a 2-4 week data-only phase before any strategy work. | [studies/weather/weather_poly_2026-07-12.md](weather/weather_poly_2026-07-12.md) |
| 2026-07-14 | siglab reversal_high_low | ML study (logistic regression → decision tree → random forest, DeepSeek-reviewed plan): does siglab's 18-variant `(reversal_low, reversal_high)` grid position predict PnL sign, across 5m-crypto/15m-crypto/non-crypto? No clear pattern in either crypto segment (5m: not significant; 15m: statistically real but too weak, ROC-AUC 0.547 < the study's own 0.55 floor); non-crypto too thin to model. Code/data/full results live in `../../btc_5mins/studies/reversal_high_low/` (per-repo convention), not here. | [siglab/doc/study_reversal_high_low_2026-07-14.md](../siglab/doc/study_reversal_high_low_2026-07-14.md) |
