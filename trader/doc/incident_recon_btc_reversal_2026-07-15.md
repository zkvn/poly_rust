# Incident — BTC reversal: two live trades show no backtest at all, 2026-07-15

**Trades in question** (`trader/results/daily_recon/trade_recon_2026-07-14_to_2026-07-15.md`,
"Live vs BT" table, both `BT DID NOT FIRE`, reason `unexplained` — the classifier's genuine
"couldn't determine" fallback, not a halt-window or config-change match):

| Time | Side | Entry Px | Outcome | Live PnL |
|---|---|---|---|---|
| 2026-07-15 16:54:46 | UP | 0.3500 | UNWIND | +0.8808 |
| 2026-07-15 17:24:52 | DOWN | 0.6172 | STOPLOSS | -0.7982 |

**Root cause, in one sentence:** the offline `backtest` replay trips its *own* internal
loss-streak halt from a stop-loss it independently simulates earlier in the day (a cycle live
never even traded) and — unlike the live process — has no way to observe the manual
`/reset_losses btc` a human sent at 16:49:44 HKT to unstick it, so it silently suppresses every
BTC entry for the rest of the day, including both trades above. Confirmed by direct
reproduction (§4): disabling halt entirely makes the backtest fire on both cycles.

## 1. Timeline (all HKT, `trader/live_logs/live.log` + `live_trades_btc_reversal.csv`)

- **08:59:40** — BTC `reversal` stop-loss (`-$0.5273`) trips the loss-streak halt
  (`halt_rev=1` since 2026-07-13). This is the incident already fully diagnosed and fixed
  today in `trader/doc/incident_unable_to_resume_2026-07-15.md` — `/resume` alone can't clear
  a loss-streak halt, and the command that can (`/reset_losses`) hadn't been wired into the
  live binary yet at this point.
- **09:03 – 16:16** — Multiple `/resume`/`/resume btc` attempts (per that incident doc), none
  of which clear the loss-streak gate. BTC stays halted the entire time.
- **16:49:44** — `🔄 Reset loss-streak halt counter for BTC.` — the operator uses the
  `/reset_losses` command (shipped and deployed earlier today, this same session) to finally
  clear the halt, ~7h50m after it engaged.
- **16:50:00–16:55:00 cycle** — BTC re-enters trading. UP reversal entry at 16:54:41 (token
  price 0.35), take-profit unwind at 16:54:46 (0.69), **+0.8808**.
- **17:20:00–17:25:00 cycle** — DOWN reversal entry at 17:24:47 (token price 0.6172, heavy
  slippage vs a 0.51 quote — see §3), stop-loss at 17:24:50 → 17:24:52, **-0.7982**. This
  stop-loss re-engages BTC's loss-streak halt (`🟡 BTC HALTED | 17:24:52 | reversal`), a
  separate, later halt window not otherwise relevant to these two trades.
- **18:21** — Daily recon report generated; both trades show `BT DID NOT FIRE`, reason
  `unexplained`.

## 2. Why `classify_mismatch_reason` says "unexplained" instead of "halted"

`trade_reconcile.py`'s halt-window reconstruction (`build_halt_windows`, `_HALT_OPEN_PATTERNS`)
only recognizes three halt-open triggers:

```python
_HALT_OPEN_PATTERNS = [
    (re.compile(r"BALANCE DRAWDOWN"), "balance drawdown >25% (session)"),
    (re.compile(r"GAMMA UNRESOLVED — HALTED"), "Gamma-unresolved halt"),
    (re.compile(r"telegram\] sent: 🛑.*[Hh]alt"), "manual /halt"),
]
_HALT_CLOSE_RE = re.compile(r"telegram\] sent: ▶️ Resumed|HALT RESET")
```

None of these match the loss-streak halt's actual Telegram lines — `🟡 <b>BTC HALTED</b>`
(auto-engage, yellow circle, not the 🛑 stop sign the manual-halt pattern requires) or
`🔄 Reset loss-streak halt counter for BTC` (the close, from today's new `/reset_losses` fix).
So `build_halt_windows` has **zero visibility into the loss-streak halt mechanism** — this
`08:59:40 → 16:49:44` window (and the `17:24:52 →` one after it) simply doesn't exist as far as
the recon report's halt-window reconstruction is concerned. With no halt window, no config
change in-window, and dense tick data (see §3 — ruling out the sparse-data label too),
`classify_mismatch_reason` falls through every category to `"unexplained"`.

This is the same *class* of gap already on record in README's "Backtest reconciliation
halt-state-drift gap" TODO (2026-07-10) — that one is about the **manual** `/halt` flag
(`entry_suppressed`) not being visible to the backtest; this is the **loss-streak** halt
(`halt_rev`/`HaltTracker`) instance of the identical problem, previously untracked because the
`/reset_losses` command (and the loss-streak halt actually mattering day-to-day) is itself new
as of today's earlier fix.

## 3. Ruling out data quality — CLOB tick + raw order-book cross-check

Per the request to actually replay CLOB/book data rather than trust the classifier's label:
reconstructed the *exact* merged tick-processing sequence `machine.rs`/`worker.rs` use
(`saw_low_up`/`saw_low_dn` latch → `ReversalStrategy::evaluate` → `gates.rs::check_gates`, in
the real merge order — binance-before-poly at equal timestamps) in Python, fed with the actual
recorded `backtest_prices/BTC_poly_2026-07-15.parquet` / `BTC_binance_2026-07-15.parquet` for
the 16:50–16:55 cycle:

```
[1784105580.000] saw_low_dn -> True (dn=0.1550, in_window=True, time_left=120.0)
[1784105656.600] saw_low_up -> True (up=0.2950, in_window=True, time_left=43.4)
[1784105681.400] *** ENTRY FIRES *** side=UP token_price=0.6650 dp=0.000514 saw_low_up=True saw_low_dn=True
```

Entry fires at **ts=1784105681.4 (16:54:41.4)**, side UP, token price **0.6650** — matching
live's real entry (`live.log`: `Order placed | 16:54:41 | T-18s | UP`, filled cost 0.35/share on
a 0.655 quote) almost to the second and the price. **The recorded tick data faithfully
reproduces the exact same entry signal live acted on** — this is not a missing-data or
stale-tick problem.

Cross-checked against the raw L2 order book (`price_feed/raw/BTC_book_2026-07-15_16.parquet`,
independent of the "poly" summary tick stream used for backtesting) for the same window:
best bid/ask gap and re-price violently tick-to-tick in the seconds around 16:54:38–16:54:52
(e.g. `16:54:41.200` bid=0.19 ask=0.71 → `16:54:41.400` bid=0.66 ask=0.67, back to `16:54:41.600`
bid=0.26 ask=0.27), with thin, flickering size at the touch (single-digit to low-hundreds units,
walls appearing/disappearing tick to tick). This is genuine, real book chaos in the final ~15-20
seconds before cycle resolution — not a data-recording artifact — and independently explains
live's own execution difficulty on both trades: the UP unwind took **49 failed take-profit retry
attempts** (`"no orders found to match with FAK order"` / balance-race errors) before filling at
16:54:46, and the DOWN stop-loss entry filled at **0.6172** against a **0.51** quote (21%
slippage) because the book that thin simply couldn't absorb the order at the quoted price.

**Conclusion of §3: the signal was real, the data is real, and the entry conditions were
genuinely satisfiable in the backtest's own tick series.** The reason no BT row exists is not
a signal-replay or data-quality problem — it's that the backtest's internal halt state never
got a chance to fire the entry check in the first place.

## 4. Confirming the halt-state root cause directly

`backtest --no-halt` (which zeroes `halt_rev`/`halt_prob`, disabling both loss-streak and
manual halt entirely for the replay) run against BTC/2026-07-15 with the correct config:

```
$ ./target/release/backtest --asset BTC --date 2026-07-15 --prices-dir backtest_prices \
    --config-file config/strategy_20260715.toml --no-halt --format csv
...
btc-updown-5m-1784075100,reversal,UP,0.545000,0.245000,STOPLOSS,-0.550500,1784075337.800
btc-updown-5m-1784105400,reversal,UP,0.665000,0.365000,STOPLOSS,-0.451100,1784105681.400
btc-updown-5m-1784107200,reversal,DOWN,0.540000,0.515000,UNWIND,-0.046300,1784107486.400
```

With halt disabled, **both** target cycles (`1784105400` = 16:50 cycle, `1784107200` = 17:20
cycle) fire — entries at 0.665/0.540, both very close to live's real entry prices (0.35 cost /
~0.655 quote and 0.6172, respectively; exact tick-path/fill-price divergence in each trade's
*outcome* is expected and separate, per the `machine.rs` vs `worker.rs` execution-path
differences already documented in `trader/doc/audit_recon_2026-07-15.md` §5 — not this
incident's subject). The default (halt-enabled) run produces **zero** rows for either cycle.

The mechanism: `08:25:00` cycle (`btc-updown-5m-1784075100`) — a cycle **live never even
traded** — is a stop-loss in the backtest's *own* independent simulation. With `halt_rev=1`,
that single simulated loss trips the backtest's internal `HaltTracker` right there, and since
`halt_reset_hour_rev=2` (2am HKT) is the *only* thing that ever clears it in an offline replay,
the backtest's simulated BTC stays "halted" for the rest of 2026-07-15 — it has no equivalent of
the human `/reset_losses btc` sent to the real live process at 16:49:44. Every later BTC cycle
that day, including both trades in this incident, gets silently suppressed by a halt that, in
the real world, had been cleared over an hour earlier.

## 5. Conclusion

Not a bug in the trading logic, the recorded price data, or the entry signal itself — all three
were independently verified correct. The gap is specifically in **`trade_reconcile.py`'s
halt-window reconstruction never having been taught about the loss-streak halt mechanism**
(`🟡 ASSET HALTED` / `🟢 ASSET HALT RESET` / `🔄 Reset loss-streak halt counter for ASSET`),
so it can neither (a) explain a `BT DID NOT FIRE` correctly when it's caused by loss-streak halt
drift, nor (b) hand the backtest binary a session-scoped halt override that would let it
actually replay past a manual `/reset_losses` the way it happened live. Flagged in README
`## TODO`; not implemented in this pass — same "diagnose first" scope as the earlier
`audit_recon_2026-07-15.md` before its fix was requested separately.
