# Trade Audit — 2026-07-03

Ad-hoc audit comparing today's Rust live trader (`poly_rust`, Oracle) against
the Python bot (`btc_5mins`, Oracle) after syncing both logs from
`ubuntu@10.8.0.1`.

## 1. The 10:40 ETH trade — high_prob, likely a failed unwind held to resolution

`trader/live_logs/live_trades_eth_high_prob.csv` has exactly one trade today:

```
slug=eth-updown-5m-1783046100  strategy=high_prob  side=UP
entry_ts=10:39:47 HKT  token_price=0.9300  exit_price=1.0000  outcome=WIN  pnl=+0.0753
```

**Strategy: `high_prob`, not `reversal`.** (`unwind_pnl` in `config.rs` is
resolved from the `unwind_pnl_rev` TOML key for *every* strategy on an asset —
the `_rev` in the name is legacy Python naming, not a reversal-only gate.
Unwind applies equally to `high_prob` positions.)

**Evidence of a failed unwind, not a clean hold-to-resolution WIN:**

- `trader/shadow_logs/shadow_ETH.log:535` — the shadow (paper) process, which
  runs the same worker logic against the same live feed but never places
  real CLOB orders (so it never experiences a real fill failure), replayed
  this exact cycle and produced:
  ```
  TradeRecord { slug: "eth-updown-5m-1783046100", strategy: "high_prob",
    side: Up, entry_ts: ...387.84, token_price: 0.91, exit_price: 0.94,
    outcome: Unwind, pnl: 0.033 }
  ```
  i.e. an early take-profit unwind at +0.03, not a hold to expiry.
- The Rust-engine backtest reproduction (`trader/doc/plan_daily_recon.md`
  §7) independently reached the same conclusion for this slug: entry 0.920,
  exit 0.950, outcome `UNWIND`, +0.0326 — vs. the live result of WIN held to
  1.0000, +0.0753.

Two independent reproductions (shadow-live and backtest) both say this
position should have exited early via unwind; the real live bot instead
shows a resolved WIN. `worker.rs`'s `on_limit_sell_placed`/`on_unwind_failed`
(`worker.rs:540-546`, `:568-574`) is built exactly for this: a GTC/marketable
sell that comes back `Failed`/`DryRun` falls back to `PriceMonitor` and the
position rides to expiry, which is consistent with what happened here (won
anyway, by luck, since the market resolved UP).

**Could not confirm directly from the live application log.** The `live`
binary's stdout (where an actual `[live] unwind sell failed, retrying…`-style
line would appear) is not persisted anywhere — it only exists in the tmux
pane scrollback, and that tmux session was recreated at **14:38:24 HKT
today** when `trader-a1` was merged to `main` (`8be569e`, 14:26) and
redeployed (binary rebuilt 14:38:03). The pre-merge session's scrollback
covering 10:40 is gone. This is a real observability gap — see
Recommendation below.

**Recommendation (repeat of `plan_daily_recon.md`'s, now with corroborating
live evidence, not just backtest):** log an explicit
`unwind_attempted`/`unwind_failed` reason code in `TradeRecord`/the live CSV,
and persist the live process's stdout to a rotating file (or systemd unit +
journald) instead of an ephemeral tmux pane, so this class of question is
answerable from logs instead of cross-referencing shadow/backtest runs after
the fact.

## 2. Today's trade count: Rust 1 vs. Python 4

| Time (HKT) | Asset | Side | Strategy | Bot | Result | pnl |
|---|---|---|---|---|---|---|
| 10:39:47 entry / 10:40 resolve | ETH | UP | high_prob | **Rust** | WIN | +0.0753 |
| 10:44:42 | ETH | DN | high_prob | Python | WIN | +0.0870 |
| 12:24:42 | ETH | UP | high_prob | Python | WIN | +0.0526 |
| 13:39:41 | ETH | DN | high_prob | Python | WIN | +0.0753 |
| 13:54:47 | ETH | DN | high_prob | Python | WIN | +0.0638 |

No overlap in cycles — every trade is a distinct 5-minute window. Two
different causes explain the gap:

### a) Rust was offline 12:35 → 14:38 (the 13:39 and 13:54 trades)

The `trader-a1` merge (`8be569e`, 14:26 HKT) was deployed to Oracle: the
`live` binary was rebuilt at **14:38:03** and the tmux session relaunched at
**14:38:24**. The last confirmed pre-redeploy state write (from state-file
mtimes synced earlier in the day) was **12:35** — so Rust had no running
process for roughly the `13:39` and `13:54` cycles. Python, running
continuously in its own tmux session throughout, simply had no competing
process to race during that window. This isn't a signal-detection gap, it's
scheduled downtime for a deploy — **not a bug**, but worth automating a
health-check/alert for if unattended redeploys become routine.

### b) Genuine signal-timing miss while both were alive (10:44 and 12:24 — and likely the mirror case, 10:40, where Rust caught a trade Python didn't)

Both bots read entry gates from the same `strategy_20260630.toml`
(`price_high` default `0.93`, `enter_when_time_left` `20`s — confirmed same
file/mtime on both Oracle and local per `plan_daily_recon.md`), so config
drift is ruled out. The likely cause is **tick granularity**:

- Python's `live_2026-07-03.log` prints (and, going by the cadence, evaluates)
  price/CLOB state on a strict **5-second** loop — e.g. for the 10:35-10:40
  ETH cycle, `UP` went `0.9650 (10:39:37) → 0.9950 (10:39:42) → 0.9900
  (10:39:47)`, blowing straight through the `price_high=0.93` ceiling
  somewhere inside a single 5-second gap, with no `BUY` line and no `SKIP_*`
  log line at all — the bot's loop never observed a tick inside the valid
  entry band.
- Rust's live trader consumes ticks off the NATS bridge
  (`price_feed` collector → `nats://127.0.0.1:4222`), which pushes every CLOB
  update rather than sampling every 5s. Rust's one trade this window entered
  at exactly `0.9300` — right at the `price_high` ceiling — strongly
  suggesting it caught a transient tick inside the valid band that Python's
  coarser sampling stepped over.

This is architectural, not a config bug: Python's decision loop is
poll-based (~5s cadence observed directly in its own log timestamps), Rust's
is event-driven off a tick stream. It cuts both ways — it explains both
"Rust caught the 10:40 trade Python missed" and, symmetrically, is the most
likely reason for the 10:44/12:24 misses in the other direction (Python
happened to land inside the band on its own sampling cadence while Rust's
worker apparently didn't fire — not independently confirmed since the
pre-14:38 Rust stdout log no longer exists; flagged as the same
observability gap as §1).

### Net for today

Python: 4 trades, all ETH high_prob WIN, total pnl +0.2787.
Rust: 1 trade, ETH high_prob WIN, pnl +0.0753.
Of Python's extra 3, **2 are explained by Rust downtime** (deploy window),
**1 (10:44) is an unexplained signal-timing miss** while both were live —
same bucket as 12:24. No reversal fills for either bot today (all
`*_reversal` CSVs are header-only on both sides) — reversal's entry bar
simply wasn't met by either implementation today, not a discrepancy.

## 3. Recommendations

1. Persist the live Rust process's stdout to a real file (or run it under
   systemd + journald like `nats-server`/`poly-collector` already are)
   instead of a bare tmux pane — today's biggest blocker to a definitive
   answer on both questions above was losing the pre-14:38 log to a session
   restart.
2. Log an explicit unwind-attempt outcome (`attempted`/`filled`/`failed`) in
   `TradeRecord` per `plan_daily_recon.md`'s existing recommendation — now
   backed by a second independent trade (this one) showing the same
   backtest/live divergence pattern as the first.
3. Consider a lightweight uptime/heartbeat alert for the Rust live process
   (e.g. via the existing Telegram bot) so deploy-window downtime is visible
   in real time rather than reconstructed after the fact from file mtimes.
