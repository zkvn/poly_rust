# Recon Audit — why BT didn't fire, 2026-07-15

Today's daily recon (`trader/results/daily_recon/trade_recon_2026-07-14_to_2026-07-15.md`,
"Live vs BT" table) shows 4 of 5 live trades as `BT DID NOT FIRE`. Two carry reason
`live halted: manual /halt 08:10–01:59` — a real, already-high-confidence halt window
(`classify_mismatch_reason`'s own doc comment rates halt-window matches as the one label that
*is* proof, not just a pointer), not investigated further here. The other two both carry
`config changed 2026-07-15 08:58 same-window (verify params)` — the classifier's explicit
"unverified, go check" label. This audit does that verification, for both rows, by actually
re-running the Rust backtest against the config that was live at trade time instead of today's
(config::load_latest's) latest file.

**Two different root causes, not one:**

1. **08:55 BTC WIN — a real recon-tooling gap.** Replaying with the config that was actually
   live at 08:55 makes the backtest fire and match live almost exactly. `BT DID NOT FIRE` here
   is a false negative caused entirely by `trade_reconcile.py` always reconciling against
   *today's* config, not the config live was actually running under at the time — a gap already
   flagged in README `## TODO` since 2026-07-10, now confirmed with a concrete reproduction.
2. **08:59 BTC STOPLOSS (-$0.5273) — BT is right, live is wrong.** Even replaying with the
   correct historical config, the backtest still doesn't fire this cycle — correctly. Live's own
   entry was driven by a corrupted `cycle_open_binance` reference price, a *different*, already-
   diagnosed and already-fixed bug (`trader/doc/fix_live_deploy_2026-07-15.md`) that happened to
   land on this exact cycle. `BT DID NOT FIRE` here is BT behaving correctly and, incidentally,
   independently confirming that bug's real-money cost.

## 1. Setup — the two rows in question

From today's report:

| Time | Asset | Side | Entry Px | Outcome | Live PnL | Status | Reason |
|---|---|---|---|---|---|---|---|
| 2026-07-15 08:55:00 | BTC | UP | 0.9000 | WIN | +0.1041 | BT DID NOT FIRE | config changed 2026-07-15 08:58 same-window (verify params) |
| 2026-07-15 08:59:40 | BTC | DOWN | 0.7900 | STOPLOSS | -0.5273 | BT DID NOT FIRE | config changed 2026-07-15 08:58 same-window (verify params) |

Both trades sit either side of `trader-live.service`'s 08:58:46 restart (a `--trader-only`
config+binary deploy), which is exactly why `classify_mismatch_reason` flagged both the same
way: `get_config_last_change_ts` returns the git-commit time of `strategy_20260715.toml`
(≈08:58), and both cycles (`btc-updown-5m-1784076600` = 08:50–08:55, `btc-updown-5m-1784076900`
= 08:55–09:00) straddle that timestamp.

`run_backtest_reconciliation` (`trade_reconcile.py`) always calls the `backtest` binary with
`--config-dir trader/config` — a directory, not a pinned file — and `config::load_latest`
(`trader/src/config.rs:207-220`) glob-sorts `strategy_*.toml` and takes the lexicographically
*last* one, unconditionally. With `strategy_20260715.toml` sitting in that directory, **every**
backtest reconciliation run, for **any** date, always replays against today's config — never
whatever was actually live on the date being reconciled. This is the exact gap already on record
in README's `## TODO` ("Backtest reconciliation config-drift gap — flagged 2026-07-10, not
fixed (deliberately deferred)").

## 2. Row 1 (08:55 WIN) — reproduced with the correct historical config

The config that was actually live for this cycle was `strategy_20260713.toml` (superseded by
`strategy_20260715.toml` at 08:58, after this cycle had already closed at 08:55). Re-running the
backtest with a scratch config directory containing only that file:

```
$ ./target/release/backtest --asset BTC --date 2026-07-15 \
    --prices-dir backtest_prices --config-dir <scratch-dir-with-only-strategy_20260713.toml> \
    --format csv

slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts
...
btc-updown-5m-1784076600,reversal,UP,0.900000,1.000000,WIN,0.111100,1784076881.000
...
```

vs. against today's (2026-07-15) latest config, over the same window — no row for that slug at
all:

```
$ ./target/release/backtest --asset BTC --date 2026-07-15 \
    --prices-dir backtest_prices --config-dir trader/config --format csv
# (no btc-updown-5m-1784076600 row)
```

Old-config replay: **WIN, entry 0.9000, exit 1.0000, pnl +0.1111** — matching live's own
0.9000 → 1.0000 → +0.1041 almost exactly (entry/exit prices identical; the small pnl delta,
+0.1111 vs +0.1041, is ordinary backtest-vs-live fill/fee modeling slop, not a new finding).
**Verdict: config drift, not a real trading-logic discrepancy.** The relevant parameter that
actually differs between the two configs for BTC is `reversal_low_threshold`
(`strategy_20260713.toml`: BTC override `0.20`; `strategy_20260715.toml`: BTC override `0.30`,
raised as part of the 07-15 walk-forward refresh) — either value happens to still classify this
cycle's dip as a valid "saw low" in this specific case, so the WIN wasn't actually sensitive to
which config replayed it; it just needed to be replayed with *a* config that was ever live for
BTC, not necessarily this exact one. The failure mode is purely "the recon script asked the
wrong question," not a signal disagreement.

## 3. Row 2 (08:59 STOPLOSS) — BT is correctly silent; the bug is upstream, in live

Re-running with the *same* correct historical config (`strategy_20260713.toml`) for the 08:59
cycle (`btc-updown-5m-1784076900`, 08:55–09:00) — still nothing:

```
$ ./target/release/backtest --asset BTC --date 2026-07-15 --prices-dir backtest_prices \
    --config-dir <scratch-dir-with-only-strategy_20260713.toml> --format csv
# (no btc-updown-5m-1784076900 row either — under the *correct* historical config)
```

So config drift alone doesn't explain this row — under the config that was genuinely live at
the time, the backtest still, correctly, doesn't fire. That itself is the finding: **live traded
a cycle the backtest — replayed honestly — says shouldn't have traded.**

### 3.1 Reconstructing why live entered anyway

`ReversalStrategy::evaluate` (`trader/src/strategies.rs:66-74`) requires, for a DOWN entry:
`saw_low_dn.saw_low() && dn > reversal && delta_pct < 0.0`, and separately `gates.rs::check_gates`
requires `|delta_pct| >= delta_pct_rev` (gate #3, `MinDeltaPct`). Reading the recorded tick data
for this cycle directly (`backtest_prices/BTC_poly_2026-07-15.parquet` /
`BTC_binance_2026-07-15.parquet`, the same data both live and the backtest saw):

- `dn` (DOWN token price) dipped to a minimum of **0.195** at 08:58:49.8 — inside the
  `SawLowSignal` window (`[cycle_end−reversal_start_time, cycle_end−no_enter_when_time_left]`
  = 08:58:00–08:59:50 for this cycle) and below both configs' `reversal_low_threshold` for BTC
  (0.20 old, 0.30 new) — `saw_low_dn` latches under either config.
- At 08:59:28.0, `dn` jumps from 0.640 to 0.805 in under a second — comfortably above `reversal`
  (0.55, unchanged both configs) — satisfying the "recover" half of the signal. This is within
  a second of live's actual order (`live.log`: `📋 BTC Order placed | 08:59:28 | T-31s | DOWN`,
  entry token price 0.79 — matches the tick data almost exactly).
- **The true cycle-open Binance price** (first tick of `btc-updown-5m-1784076900`, 08:55:06) was
  **64830.01**. At 08:59:28, Binance was **64824.57** — `delta_pct = (64824.57 − 64830.01) /
  64830.01 = −0.0000839` (**0.0084%**). That's below *both* configs' `delta_pct_rev` for BTC
  (old override `0.0005`, new default `0.0003`) — the `MinDeltaPct` gate should have blocked
  this entry under either config, and the backtest — replaying against the true open — correctly
  never let it through.

So how did live enter? `live.log` shows this exact cycle's `CycleOpen` was fabricated by the
08:58:46 restart landing mid-cycle: `[live] new cycle BTC (reversal) slug=btc-updown-5m-1784076900
open_binance=64845.87` — **64845.87, not the true 64830.01**. This is precisely the bug diagnosed
and fixed same-day in `trader/doc/fix_live_deploy_2026-07-15.md` (`current_slot_val` resetting to
`0` on every restart, so the first tick after a restart always looks like a fresh cycle open,
stamping whatever Binance happened to be trading at restart time as the reference price) — that
doc's own root-cause section even names this exact stop-loss ("directly implicated in one costly
stop-loss that fired in that cycle... the 2026-07-15 08:55 BTC row").

Recomputing `delta_pct` with live's *fabricated* open: `(64824.57 − 64845.87) / 64845.87 =
−0.0003285` (**0.0329%**). Against the *new* config's loosened threshold (`delta_pct_rev` default
`0.0003`, no BTC override) that's just barely enough to clear gate #3 (`0.0003285 ≥ 0.0003`) —
against the *old* config's stricter `0.0005` it would still have been blocked. **Two compounding
same-day changes, neither alone sufficient:** the mid-cycle-restart bug corrupted the reference
price enough to inflate the apparent move from 0.0084% to 0.0329%, and the same-day config
refresh happened to loosen `delta_pct_rev` just enough (0.0005 → 0.0003) for that inflated,
still-tiny move to clear the gate. Live entered on what was, by the true reference price, a
statistically negligible 0.008% wobble — not a genuine reversal signal — and ate a real $0.53
stop-loss as a direct result.

### 3.2 Corroboration already in the report

The report's own `Entry Δ%` column for this row (independently computed by `trade_reconcile.py`
from the *correct* historical price series via `_cycle_open_close`/`_underlying_price_at`, not
from anything live logged) already reads **`-0.0%`** — rounds from the same ≈−0.0084% this audit
derived by hand from the raw ticks. That column was sitting in the report the whole time as a
quiet second confirmation that the true underlying move was near-zero.

## 4. Conclusions / status

- **Row 1 (WIN):** confirmed config-drift artifact of the recon script, per the already-tracked
  README TODO — no new bug. Fixing `trade_reconcile.py`/`backtest.rs` to accept a pinned
  historical config (the TODO's own proposed fix: a `--config-file` override, sourced from
  `config_log.rs`'s JSONL snapshot log) would make this row resolve to MATCH automatically.
  Not implemented here — same "deliberately deferred" scope call as the existing TODO entry;
  this audit only adds a concrete confirmation, doesn't change the fix-it decision.
- **Row 2 (STOPLOSS):** not a recon-tooling gap at all. The mid-cycle-restart bug
  (`fix_live_deploy_2026-07-15.md`) was already found and fixed today, independent of this
  audit — this section is corroborating evidence for that fix, tying it concretely to the
  `-$0.5273` stop-loss's `Entry Δ%` reading in this specific report, and to the loosened
  `delta_pct_rev` threshold as a second, compounding factor worth knowing about (not itself a
  bug — 0.0003 is a deliberately-chosen walk-forward parameter — but it did make this particular
  corrupted-reference-price entry slip through where the old 0.0005 would not have).
- No code changes made by this audit — it's a verification/diagnosis pass over an existing
  "(verify params)" flag, confirming one row as tooling noise and the other as real, already-
  fixed, real-money impact.
