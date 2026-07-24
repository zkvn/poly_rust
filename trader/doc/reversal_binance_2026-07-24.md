# reversal_binance re-pick — Table 2 (2026-07-24)

Source: `../btc_5mins/studies/reversal_binance`'s synthetic HAR-Student-t/Binance-only
backtest (no real CLOB data). Table 2 is the winning combo's parameters — the ones
DSR/PBO are vouching for, per-asset best-PnL combo that cleared the Deflated Sharpe
Ratio's 95% significance bar after deflating for the 2,592-combo trial count.

## Table 2 — winning combo parameters

| Asset | delta_pct_rev | reversal | reversal_low_threshold | unwind_pnl_rev | sl_pnl_rev | unwind_time_rev |
|-------|---------------|----------|-------------------------|----------------|------------|------------------|
| ETH   | 0.0003        | 0.5      | 0.3                     | 0.15           | 0.3        | 15s              |
| SOL   | 0.0004        | 0.5      | 0.3                     | 0.10           | 0.5        | 25s              |
| BNB   | 0.0003        | 0.5      | 0.4                     | 0.10           | 0.5        | 15s              |
| XRP   | 0.0003        | 0.5      | 0.4                     | 0.20           | 0.4        | 15s              |
| DOGE  | 0.0005        | 0.5      | 0.4                     | 0.10           | 0.4        | 15s              |

BTC did not clear DSR (p=0.9148) and is excluded from this table — its config
(`delta=0.0005, reversal=0.55, low=0.20, unwind_pnl=0.15, sl_pnl=0.0 disabled,
unwind_time=30.0`) is left untouched.

## What actually changes vs. the currently-deployed `strategy_20260724.toml`

ETH, BNB, XRP, DOGE in Table 2 are **identical** to what's already live (deployed
this morning, see that file's `meta.source`). The only real delta is **SOL**:

| Field | Was (untouched carry-over) | Now (Table 2) |
|-------|------------------------------|----------------|
| delta_pct_rev | 0.0005 | 0.0004 |
| reversal | 0.70 | 0.5 |
| reversal_low_threshold | 0.20 | 0.3 |
| unwind_pnl_rev | 0.15 | 0.10 |
| sl_pnl_rev | 0.0 (disabled) | 0.5 (enabled) |
| unwind_time_rev | 30.0 | 25.0 |

Per user decision: SOL's params are updated to Table 2 values, but SOL's trading
scope is left as-is (`trade_assets`/`[strategies]` unchanged) — no scope expansion,
params-only update.

## Paper balance reset

Fleet-wide paper balance reset to a fresh $50 start (`PAPER_STARTING_BALANCE_USDC`
in `trader/src/bin/live.rs` is already `50.0` in code — no code change needed). The
reset itself is operational: all 6 assets' persisted `paper_state_*.json` on Oracle
get archived immediately before restart, same procedure as the 2026-07-24 00:00
deploy (`live_logs/archive_20260724_fresh50/` on Oracle).

## Caveat (carried over from the source study)

The synthetic backtest that produced these values traded against a market literally
defined by the same HAR+Student-t probability model as the entry signal, not real
CLOB microstructure — the study's own QC toolkit found the apparent edge is
substantially mechanical rather than confirmed evidence of a genuine CLOB-independent
timing edge. Deployed anyway per explicit user request as a live paper-trade test,
not a validated live-adoption decision.
