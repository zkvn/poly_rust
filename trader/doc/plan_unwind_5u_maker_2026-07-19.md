# Plan: unwind-tuned, 5-share maker-entry reversal — paper trade on Oracle

**Status: executable plan — written 2026-07-19 for hand-off to another agent.**
The user will not be present; execute end-to-end, stopping only at the two
explicit STOP gates below. High_prob is **left out entirely** (user decision).

Background docs (in `/home/kev/apps/btc_5mins`, all pushed):
- `doc/plan_signal_lab_2026-07-19.md` — why taker price-action loses here (fees `0.07·p·(1−p)`, ~5¢ stop slippage) and the staged test discipline.
- `doc/plan_market_maker_mvp_2026-07-19.md` — maker entries: "split $1, sell the loser at X" ≡ resting maker BUY of the winner at 1−X (CLOB unified mint/merge book); poly_rust GTC machinery; `MIN_GTC_SHARES = 5.0`.
- `studies/split_and_sell/summary.md` — on the 158 real trades, maker entries halve reversal's losses conservatively (+6.70 at the perfect-fill bound) and are **structurally incompatible with high_prob**; measured adverse selection (filled −0.131 vs unfilled +0.041 actual mean).
- `studies/stop_loss_filter/recommendation_2026-07-19.md` — Scenario A: reversal price stops OFF, the 10–30s `unwind_time_rev` timeout is the stop; high_prob irrelevant here.
- `studies/unwind_safely/summary_2026-07-08.md` — walk-forward-validated per-asset reversal combos (three selection tables).

---

## Part 1 — unwind parameters (results already exist; confirmation sweep optional)

### 1.1 The parameters to run (from existing walk-forward results)

User preference: **steady returns, fewer trades, positive PnL, take-profit
0.05–0.15**. That maps to `unwind_safely`'s **best-by-win_rate** table
(final calibration window 2026-05-26→07-09, ~10k cycles/asset; 90–346
trades ≈ 2–8/day/asset at 79–88% win rate — the "fewer, steadier" pool by
construction). Stop-loss policy comes from the newer 07-19 live-trade study
(Scenario A), which supersedes the sl_pnl 0.40–0.45 values those sweeps
carried — see 1.2.

| Asset | delta_pct_rev | reversal | reversal_low_threshold | unwind_pnl_rev | unwind_time_rev | sl_pnl_rev | sl_reversal |
|---|---|---|---|---|---|---|---|
| BTC | 0.0010 | 0.55 | 0.20 | 0.15 | 26 | 0.0 | 0.0 |
| ETH | 0.0008 | 0.55 | 0.20 | 0.15 | 28 | 0.0 | 0.0 |
| SOL | 0.0010 | 0.55 | 0.20 | 0.15 ¹ | 30 | 0.0 | 0.0 |
| BNB | 0.0010 | 0.70 | 0.20 | 0.15 ¹ | 12 | 0.0 | 0.0 |
| XRP | 0.0010 | 0.80 | 0.20 | 0.15 | 28 | 0.0 | 0.0 |
| DOGE | 0.0008 | 0.60 | 0.30 | 0.15 | 30 | 0.0 | 0.0 |

¹ table value was 0.17; clamped to 0.15 per the user's stated 0.05–0.15 range.

### 1.2 Stop-loss: why 0.0 (and the one condition)

`studies/stop_loss_filter/summary_2026-07-19.md`, on the actual live trades:
every reversal price-stop tightness level hurt monotonically (live 0.20–0.30
zone was worst, −14.7 adj; disabled −5.5 adj); stop fills slip ~5¢/share
below trigger while timeout exits cost ~1¢. **Hard condition: `unwind_time_rev`
stays enabled (it IS the stop). If anyone disables/raises the timeout, set
`sl_pnl_rev = 0.80` (Scenario B) the same day.** With 5-share (~$3) sizing
the no-fill tail is not a material dollar risk.

### 1.3 Optional confirmation sweep (run only if time permits; don't block Part 2)

The by-win_rate table predates 07-09 data. To re-confirm on data through
yesterday, in `/home/kev/apps/btc_5mins`:

```bash
cd /home/kev/apps/btc_5mins && source venv/bin/activate
python scripts/sync_remote.py   # pull latest Oracle data first
tmux new-session -d -s bt2conf
tmux send-keys -t bt2conf "source venv/bin/activate && python scripts/bt2.py --assets BTC ETH SOL BNB XRP DOGE" Enter
```

Selection rule if re-picking: within `unwind_pnl_rev ∈ [0.05, 0.15]`,
`sl_pnl_rev ∈ {0, 0.8}`, entry dims near the table above, pick per asset by
win_rate among combos with ≥50 trades; a pick is only a *hypothesis* until it
also wins on the walk-forward view (`studies/README.md` "Validating a sweep
winner"). If results differ materially from 1.1, prefer 1.1 for the paper
run (2 days of paper data will arbitrate better than another in-sample read).

---

## Part 2 — paper trade on Oracle, ≥ 2 days (48h), parameters frozen

### 2.0 What exists / what's missing (verified 2026-07-19 by code inspection)

- `trader/src/bin/shadow.rs` exists but is the old A2 single-asset would-be-
  trade logger — **no telegram, no full lifecycle. Do not use it for this.**
- `live.rs` is the real thing (telegram, halt, gamma watcher, redemption,
  systemd). **There is no paper/dry-run mode in it** (`grep -rn "paper\|dry_run" trader/src/config.rs` → nothing).
- `execution.rs` defines the execution trait (`place_limit_sell`,
  `cancel_limit_sell`, FAK buys, fill callbacks, `MIN_GTC_SHARES=5.0`) —
  the clean seam: paper mode = a simulated implementation of this trait.
  **Missing for maker entries: a resting GTC limit BUY** (only limit sells
  exist today, for unwind TP).
- `live.rs` already consumes `indicator.<ASSET>` NATS snapshots into
  `IndicatorStore` (decision-neutral Phase 1, `trader/doc/feature_vol_2026-07-18.md`)
  — the p_up gate is a small Phase-2 step, not new plumbing.

### 2.1 Implement `--paper` mode in the live binary

- New CLI flag `--paper` (and/or `[meta] paper_trade = true` in the strategy
  TOML — flag wins). When set, construct `PaperExecutor` implementing the
  execution trait; **everything else identical**: telegram (prefix every
  message `[PAPER]`), trade CSV (write to `live_logs/paper_trades_*.csv`,
  NEVER `live_trades_*.csv` — analytics depend on that file meaning real
  money), halt logic, gamma watcher, config log.
- `PaperExecutor` fill rules (conservative, from `studies/split_and_sell/PLAN.md`):
  - marketable FAK buy/sell → fills immediately at the latest observed CLOB
    price for that token (log it as the fill price).
  - resting GTC limit **buy** at B → fills when observed price ≤ `B − 0.01`
    (trade-through, not touch). Resting limit **sell** at A → fills when
    price ≥ `A + 0.01`. Partial fills: not simulated (all-or-nothing).
  - cancel → always succeeds if not yet filled.
- Hard safety: `PaperExecutor` holds **no CLOB client at all** (compile-time
  impossibility of a real order, not a runtime `if`).

### 2.2 Maker entries, 5 shares (the "5u maker" part)

Per `doc/plan_market_maker_mvp_2026-07-19.md` §3 Phase 0, reversal only:
- New execution-trait method `place_limit_buy` (mirror of `place_limit_sell`).
- Entry flow change in the worker: on a reversal entry signal, instead of the
  FAK buy — rest a GTC buy on the signaled side at the **current best bid**,
  size **5 shares** (`MIN_GTC_SHARES`); config flag `maker_entry = true`.
- Cancel the resting quote on: signal invalidation (existing gate logic
  turning false), or `T − 15s` before cycle end, whichever first. **Log every
  canceled quote with its would-have-been outcome fields** (slug, side,
  quote price, cancel reason) — the filled-vs-canceled comparison is the
  adverse-selection metric and is the single most important output of this
  test.
- On (simulated) fill: existing position lifecycle unchanged (unwind TP
  0.15, timeout per table, no price stop).

### 2.3 p(up) negative-edge gate (test-enabled in paper)

Per `indicator_loss_filter` + `pup_gate` edge follow-up: veto an entry when
`p_side < entry_price` (the parameter-free X=0 veto — "never pay more than
the model probability"):
- `p_side = p_up` for UP entries, `1 − p_up` for DOWN, from the freshest
  `IndicatorStore` snapshot for the asset; entry price = the quote price
  being rested (2.2).
- Config key `pup_edge_min_rev` (absent/NaN = disabled; `0.0` = this veto —
  same convention as btc_5mins `BacktestParams`). Enable at `0.0` for the
  paper run.
- **Fail-open on missing/stale indicator** (no snapshot, or snapshot older
  than 10s, or warmup keys absent): do NOT veto, log
  `pup_gate=SKIPPED_NO_DATA`. A dead indicator daemon must not silently halt
  all trading.
- **Log every veto** with slug/side/p_side/price so the 48h evaluation can
  compute the vetoed trades' counterfactual outcomes from gamma.

### 2.4 Tests (before any deploy) — STOP GATE 1: all green locally

- Unit tests: `PaperExecutor` fill rules (marketable, GTC buy/sell through
  vs touch boundary at exactly ±0.01, cancel-after-fill race), maker-entry
  state machine (QUOTED→FILLED, QUOTED→CANCELLED at T−15s, partial-cycle
  restart), pup gate (veto, pass, fail-open on stale/missing snapshot),
  config parsing (`--paper` flag precedence, `pup_edge_min_rev` absent vs 0.0).
- `cargo test` full suite + `cargo clippy` clean.
- Local soak: run `live --paper` locally for ≥ 2 hours on BTC+ETH against
  real feeds; verify: `[PAPER]`-prefixed telegram messages arrive,
  `paper_trades_*.csv` rows appear with sane prices, zero CLOB writes
  (assert no order-endpoint HTTP calls in the log), quotes cancel at T−15s.

### 2.5 Config for the run

New `trader/config/strategy_<date>.toml` (copy latest, then):
- `[strategies]`: every asset `["reversal"]` only — **high_prob removed**.
- Per-asset entry/exit values from the Part-1 table (incl. `sl_pnl_rev=0.0`,
  `sl_reversal=0.0`).
- `maker_entry = true`, `pup_edge_min_rev = 0.0`, paper mode on.
- `[meta] source` must say: paper-trade test per this plan doc, parameters
  frozen for 48h, and name this file.

### 2.6 Deploy to Oracle — use poly_rust's own deploy script

```bash
cd /home/kev/apps/poly_rust && python scripts/deploy_oracle.py
```

- This is the **poly_rust** deploy path (cross-compile, rsync, config sync,
  `systemctl restart trader-live`) — NOT btc_5mins' `upgrade_oracle.py`
  (that manages the deprecated python bot). Never kill/tmux the trader
  directly — systemd `Restart=always` races (see 2026-07-03 incident in the
  script header).
- **Note the consequence, it is intended: real-money trading is paused for
  the 48h paper window** (the same service runs in paper mode). Given the
  live book is currently net negative, this is acceptable by design of this
  plan.
- Post-deploy verification (within 15 min): `journalctl -u trader-live` shows
  paper mode banner; a `[PAPER]` telegram heartbeat/first-quote arrives;
  indicator snapshots flowing (pup gate not permanently in
  `SKIPPED_NO_DATA`); confirm the poly-collector and indicator services are
  active.

### 2.7 Evaluation after ≥ 48h — STOP GATE 2: report to user, do not go real-money

Produce `trader/doc/report_paper_unwind_5u_maker_<date>.md` (and push):
- Per asset: quotes placed / filled / canceled (fill rate; target from doc 2:
  ≥30%), paper PnL, win/TP/timeout mix, trades/day.
- **Adverse selection**: gamma-resolved counterfactual outcome of canceled
  quotes vs filled ones (the doc-3 metric, now with real queue-realistic
  fills).
- **pup gate**: veto count, and vetoed entries' gamma outcomes (did the veto
  save money?). Also `SKIPPED_NO_DATA` count (indicator uptime).
- Compare fill-rate & per-fill edge against the split_and_sell backtest's
  conservative/optimistic bounds (−4.57 … +5.84 band equivalents).
- **Then stop.** Promotion to real money (flip paper off, keep 5 shares) is
  the user's call on reading the report — do not execute it autonomously.

## Execution order & discipline

1 (list/confirm params) → 2.1–2.3 (implement, one commit per step, docs per
poly_rust CLAUDE.md) → 2.4 (STOP GATE 1) → 2.5–2.6 (deploy) → 48h wait
(daily `trade_reconcile.py` checks that paper rows accrue) → 2.7 (STOP GATE 2).
Frozen parameters: no recalibration mid-run, per the user's instruction —
if something looks broken, fix *bugs*, never *thresholds*, and note it in
the report.
