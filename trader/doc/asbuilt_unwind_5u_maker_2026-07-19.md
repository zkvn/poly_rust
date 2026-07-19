# As-built reference — 5-share maker-entry reversal paper trade

**Status: describes the code as it actually runs, 2026-07-19.** Companion to
[`plan_unwind_5u_maker_2026-07-19.md`](plan_unwind_5u_maker_2026-07-19.md) (the
pre-implementation plan — design intent, written before any of this existed) and
the source docs that plan built on, in `/home/kev/apps/btc_5mins`:
[`doc/plan_market_maker_mvp_2026-07-19.md`](../../../btc_5mins/doc/plan_market_maker_mvp_2026-07-19.md)
(why maker entries; the split-and-sell ≡ maker-buy equivalence) and
[`studies/split_and_sell/summary.md`](../../../btc_5mins/studies/split_and_sell/summary.md)
(the conservative-fill numbers this implementation's fill rule matches).

This doc exists because the plan doc doesn't enumerate the actual state
machine, CSV schemas, or log formats — it couldn't, it predates them. Read
this for "what does the running system actually do," the plan doc for "why."

Currently live on Oracle: `strategy_20260719.toml`, all 6 assets
(BTC/ETH/SOL/BNB/XRP/DOGE), reversal only, `paper_trade = true` (real-money
trading paused for the 48h window).

## 1. Entry — decision order

Runs on every `BinanceTick` and `PolyTick`, from `Worker::try_enter`
(`trader/src/worker.rs`), only while `WorkerState::Watching` and not halted:

1. **Strategy evaluation** (`ReversalStrategy::evaluate`, `trader/src/strategies.rs`):
   fires when `saw_low_{up,dn}` latched during the entry window AND the
   opposite side's price has since recovered above `reversal` AND
   `delta_pct`'s sign matches the recovering side. Latches once per cycle
   (`self.fired`) — a block below does **not** consume this latch, so a later
   tick can still fire once the blocking condition clears.
2. **Gates** (`check_gates`, `trader/src/gates.rs`), in this exact order —
   first match blocks, no entry:
   1. `spread_premium_limit` / `spread_discount_limit` (up+dn sanity band)
   2. `max_price_age_secs` (poly tick staleness, config default 2.0s)
   3. `|delta_pct| < delta_pct_rev` (minimum directional move)
   4. `token_price > max_buy_price`
   5. `token_price > price_high_rev` (reversal only)
3. **p(up) negative-edge gate** (§4 below) — reversal only, checked before
   `mark_fired()` so a veto doesn't lock the strategy out for the rest of the
   cycle. Blocks (returns only `Action::PupGateNote{Veto}`) or passes through
   (falls through, possibly appending `Action::PupGateNote{SkippedNoData}`
   after the entry actions below).
4. `mark_fired()` — strategy can't re-fire this cycle from here on regardless
   of what happens next.
5. **Order placement**, branching on `maker_entry` (reversal only):
   - **Maker** (`maker_entry = true`): `Action::PlaceLimitBuy{side, price:
     intent.token_price(), shares: MIN_GTC_SHARES (5.0), signal_ts}`. State →
     `WorkerState::EnteringMaker(MakerQuote{..})`.
   - **FAK** (`maker_entry = false`, or non-reversal strategy):
     `Action::PlaceBuy{side, price: intent.token_price(), size_usdc:
     trade_size_usdc, signal_ts}`. State → `WorkerState::Entering`.

**Known deviation from the source plan:** the maker quote's price is the
signal's own mid (`intent.token_price()` = `PolyTick.up`/`.dn`, the merged
`(bid+ask)/2`), not the literal current best bid the MVP plan calls for
(`PolyTick` doesn't carry bid/ask separately). See README `## TODO`.

## 2. Maker quote lifecycle (`WorkerState::EnteringMaker`)

While a quote rests, every `PolyTick` runs `check_maker_quote_cancel`
(`trader/src/worker.rs`) instead of the entry-evaluation path above:

1. **T−15s before cycle end** (`CANCEL_BEFORE_CYCLE_END_SECS = 15.0`, time-based,
   checked first regardless of price) → cancel, reason `CycleEndApproaching`.
2. **Signal invalidation** — either the reversal threshold no longer holds
   (`side_price <= reversal_threshold`, the same value that justified the
   quote firing) or a re-checked gate (spread/staleness/delta/price ceiling,
   same `check_gates` as entry) now blocks → cancel, reason
   `SignalInvalidated`.
3. Otherwise: no action, quote keeps resting.

A cancel (`Action::CancelEntryQuote`) carries `quoted_at` (the tick timestamp
the quote was placed at) so the driver can compute pull-to-cancel latency —
see §6.

**Fill**, live: no path exists yet (no USER-channel wiring for entry fills —
see README `## TODO`, "entry_resting_order_id... live path doesn't produce
this yet"). **Fill, paper**: `PaperExecutor::on_price` (§3) detects a
trade-through and the driver routes it as `Event::EntryQuoteFilled`, or a
marketable-on-placement quote (crossed the book immediately) arrives as
`Event::LimitBuyPlaced{status: Matched}`. Either way,
`Worker::finalize_entry_fill` builds the `Holding` position — same function
the FAK path's fill uses, so the position lifecycle from here on (§3 below)
is identical regardless of how the entry filled.

## 3. Exit — decision order

Runs on every `PolyTick` while `WorkerState::Holding`, checked in this order
(`Worker::on_poly`), first match wins:

1. **Stop-loss**: `sl_pnl > 0.0 && exit_price <= entry_price - sl_pnl`, or
   absolute `sl > 0.0 && exit_price < sl`. **Currently disabled for this run**
   (`sl_pnl_rev = sl_reversal = 0.0` in `strategy_20260719.toml`, per plan
   §1.2 — `unwind_time_rev` is the stop instead).
2. **Take-profit**: `PriceMonitor` arm only (a `GtcResting` arm's fill arrives
   via `UnwindFilled` directly, not this check) — `exit_price >= tp_price`
   (`tp_price = entry_price + unwind_pnl_rev`).
3. **Timeout**: `tick.ts - entry_ts >= unwind_time_rev` (26–30s per asset,
   table 1.1) — force-closes at market regardless of price. This is the real
   stop for this run.

On a fill >= `MIN_GTC_SHARES`, the take-profit exit itself is a resting GTC
limit sell (`choose_exit_order_kind`), same maker/taker duality as the entry
side — `place_limit_sell` → `LimitSellPlaced`.

## 4. p(up) negative-edge gate

Reversal only. `p_side = p_up` for an UP entry, `1 - p_up` for DOWN — "never
pay more than the model probability."

- **Veto**: `p_side < entry_price + pup_edge_min_rev` (config: `0.0` for this
  run — the parameter-free X=0 case). Logged to
  `paper_pup_vetoes_{asset}_{strategy}.csv` and console `[PUP-GATE] ... VETO`.
- **Fail-open** (`SkippedNoData`, does **not** veto): no ready snapshot ever
  received, or the freshest one is older than `PUP_GATE_MAX_AGE_SECS = 10.0`s
  (`pub` in `worker.rs`) — deliberately a fixed constant, independent of the
  config's `indicator_max_age_secs` (5.0s default, used only by the
  Phase-1 heartbeat's console `ind[...]` display, `bin/live.rs`'s ticker
  arm). A dead `poly-indicator.service` must never silently block trading.
  Logged to console `[PUP-GATE] ... pup_gate=SKIPPED_NO_DATA` only (no CSV
  row — this is an indicator-uptime signal, not a trade-relevant event).
  **`/status` and the Telegram order/quote/trade notifications' `fmt_indicator`
  display (§6) also use `PUP_GATE_MAX_AGE_SECS`, not `indicator_max_age_secs`**
  — using the tighter heartbeat default there originally produced a real,
  confusing contradiction: a quote whose `[PUP-GATE]` line never fired
  (meaning the gate found fresh data and passed cleanly, since only
  veto/skip are logged) showed "ind: no data" in the very same notification,
  because 5.0s had already elapsed by render time but 10.0s — the window the
  gate itself actually used — hadn't. Fixed 2026-07-19, caught by the user
  reading a real DOGE maker-quote notification.
- Snapshots arrive via `Event::IndicatorUpdate{p_up, ts}`, sent only when a
  NATS `indicator.<ASSET>` snapshot's `vals` map actually has a ready `p_up`
  key (a warmup snapshot with no `p_up` sends nothing — indistinguishable
  from "no snapshot" for this gate).

## 5. Fill simulation — `PaperExecutor` (paper mode only)

`trader/src/execution.rs`. Holds no CLOB client at all — a real order is a
compile-time impossibility, not a runtime check.

- **Marketable** (FAK-equivalent BUY/SELL): fills immediately, all-or-nothing,
  at the latest observed price for that token (falls back to the caller's
  signal price if no tick has been observed yet).
- **Resting** (GTC BUY or SELL): fills only on a **trade-through**, not touch
  — `PAPER_TRADE_THROUGH = 0.01`. A resting BUY at B fills when observed price
  `<= B - 0.01`; a resting SELL at A fills when observed price `>= A + 0.01`.
  Fill price is always the order's own quoted price (a maker fill fills at
  the maker's quote, not the crossing price). This is the split-and-sell
  study's conservative "variant A" fill rule verbatim — the variant the study
  says are "the ones to trust" (the optimistic touch-fill variant overstated
  PnL: +5.84 vs. −4.57 on the same 92 historic reversal trades).
- Partial fills are not simulated (all-or-nothing).
- A GTC order below `MIN_GTC_SHARES` is rejected with an
  `INVALID_ORDER_MIN_SIZE`-style error, mirroring the real exchange floor.

The driver feeds every observed poly tick to `PaperExecutor::on_price` for
both tokens (up/dn) *before* stepping the worker on that same tick, so a fill
and the tick that caused it are ordered deterministically (a fill always
happens before a same-tick cancel could race it).

## 6. Logging reference

### `paper_trades_{asset}_{strategy}.csv`

```
logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,
exit_attempts,exit_last_error,entry_signal_latency_ms,entry_process_latency_ms,
exit_signal_latency_ms,exit_process_latency_ms
```

One row per closed position (`Outcome`: `Win`/`Loss`/`StopLoss`/`Unwind`/`Timeout`).

### `paper_quotes_{asset}_{strategy}.csv`

```
logged_at,slug,strategy,side,quote_price,reason,quoted_at,pull_to_cancel_ms
```

One row per **canceled** maker-entry quote (`reason`:
`SignalInvalidated`/`CycleEndApproaching`). `quoted_at`/`pull_to_cancel_ms`
added 2026-07-19 (same day as the rest of this file) — closes a gap
`plan_market_maker_mvp_2026-07-19.md` §4 flagged as "worth logging from day
1" that the first pass here missed. A **filled** quote never appears here —
only cancels; fills show up in the trades CSV instead.

### `paper_pup_vetoes_{asset}_{strategy}.csv`

```
logged_at,slug,strategy,side,p_side,price
```

One row per **veto** only — `SkippedNoData` is console-only (§4).

### Console tags (`live_logs/live.log`)

| Tag | Meaning |
|---|---|
| `[ORDER] ... BUY ...` | FAK entry attempt (any strategy) |
| `[ORDER] ... MAKER ENTRY BUY ...` | Maker entry quote placement attempt |
| `[ORDER] ... LIMIT SELL ...` | Take-profit resting sell placement |
| `[ORDER] ... CLOSE ... (Timeout\|StopLoss)` | Market close at exit |
| `[ORDER] ... CANCEL ...` | Exit-side resting-sell cancel |
| `[ORDER] ... CANCEL ENTRY QUOTE ...` | Entry-side maker-quote cancel, incl. `pull_to_cancel_ms` |
| `[PAPER-FILL] ...` | A resting paper order traded through (`PaperExecutor::on_price`) |
| `[PUP-GATE] ... VETO` / `... SKIPPED_NO_DATA` | p(up) gate outcome |
| `[SL]` / `[TIMEOUT]` | First-trigger-only Telegram-alert guards (stop-loss / timeout) |
| `[TRADE] TradeRecord{...}` | Full closed-position record (mirrors the trades CSV row) |

### Telegram (`[PAPER] ` prefix on every message this run)

Startup banner, maker-quote-resting notice (`📝 Maker quote resting`), order
placed/rejected, stop-loss/timeout triggers, trade outcome (`✅`/`❌`/`⏱️`),
gamma correction/halt notices — same set the real-money path sends, just
prefixed.

## 7. Source map

| Concern | File |
|---|---|
| Entry/exit decision logic, maker-quote lifecycle, pup gate | `trader/src/worker.rs` |
| `PaperExecutor`, fill rules, `MIN_GTC_SHARES`/`PAPER_TRADE_THROUGH` | `trader/src/execution.rs` |
| Driver wiring, CSV/console logging, Telegram | `trader/src/bin/live.rs` |
| Config schema (`maker_entry`, `pup_edge_min_rev`, `paper_trade`) | `trader/src/config.rs` |
| This run's actual parameters | `trader/config/strategy_20260719.toml` |
| Indicator daemon (p_up/vol_har/snr source) | `indicator/` crate, `poly-indicator.service` on Oracle |
