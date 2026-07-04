# Retry Audit — DOGE order rejection, ~16:23 HKT 2026-07-03

Investigating the rejected DOGE BUY reported around 16:23 in `trader/live_logs/live.log`
(cycle started 16:11:22 after the `6e3ba58` redeploy, no timestamps in the log itself —
time recovered by matching `T-Ns` heartbeat offsets against the cycle's slug epoch).

## 1. What happened

`live.log:129-133`, cycle `doge-updown-5m-1783066800` (16:20:00-16:25:00), strategy
`reversal`:

```
[ORDER-RETRY] ... BUY attempt 1/4 price=0.7950 -> no orders found to match with FAK order...
[ORDER-RETRY] ... BUY attempt 2/4 price=0.8150 -> no orders found to match with FAK order...
[ORDER-RETRY] ... BUY attempt 3/4 price=0.8350 -> no orders found to match with FAK order...
[ORDER-RETRY] ... BUY attempt 4/4 price=0.8550 -> no orders found to match with FAK order...
[ORDER] DOGE BUY Up @ 0.7450 size=$1.00 -> placed=false shares=0.0000 cost=0.0000
```

Real time recovered from the `T-97s`/`T-63s` heartbeats bracketing these lines and the
cycle's 16:25:00 close: the four attempts happened between roughly **16:23:23 and
16:23:57 HKT**.

**It was retried.** 4 attempts total = `1 + order_max_retries` with
`order_max_retries = 3` from the active config
(`btc_5mins/config/strategy_20260630.toml`, loaded as the lexicographically-latest
`strategy_*.toml` per `config::load_latest`). Each retry stepped the limit price up by
`retry_slippage_step = 0.02` (hardcoded in `execution.rs`, `ExecutionConfig::default`)
on top of the base `order_slippage = 0.05`: 0.7950 → 0.8150 → 0.8350 → 0.8550. All four
got the identical CLOB error: *"no orders found to match with FAK order. FAK orders are
partially filled or killed if no match is found."*

This retry count wasn't visible in the Telegram "Order REJECTED" message before this
session — fixed alongside this audit (see `src/bin/live.rs`, `Action::PlaceBuy`
handling): the rejection notification now includes `attempts=N`, and still only a
**single** Telegram message is sent per order (after the retry loop exhausts), not one
per attempt — the per-attempt lines were always log-only (`eprintln!`, into
`live_logs/live.log`).

## 2. Was the order-slippage config too tight?

No — the opposite problem. Cross-referencing the local order-book collector
(`price_feed/raw/DOGE_book_2026-07-03_16.parquet`, `doge-updown-5m-1783066800`, `UP`
side) for the T-100s..T-60s window (16:23:20-16:24:00):

- **Best ask stayed in the ~0.41-0.66 range** the entire window (e.g. 0.44 at T-100s,
  spiking briefly to 0.65-0.66 around T-88s, settling back to ~0.58-0.59 by T-84s).
- The four FAK attempts (0.795, 0.815, 0.835, 0.855) were all priced **well above every
  observed ask** in that window — a FAK buy at 0.855 should sweep and fill against any
  resting ask at or below that price, so on paper these should have matched easily.
- Meanwhile the strategy's own signal ("up" probability from the HAR/Binance model,
  `live.log`'s heartbeat lines) moved from 0.4250 (T-97s) to 0.9250 (T-37s) to 0.9950
  (T-7s) — i.e. the *model's* implied probability repriced far faster than the
  order-book snapshot shows the *market* moving.

So `order_slippage`/`retry_slippage_step` are not the cause — the retries already
priced above the entire visible book depth and still failed to match. This is
consistent with the known behavior of these 5-minute crypto up/down markets in the
final ~100s before close: resting maker liquidity gets pulled within milliseconds as
the model-implied probability swings hard, so a FAK order can legitimately find an
empty book at the instant it lands even though a slightly-stale local snapshot still
shows resting asks. (Corroborating: the recorded book's full depth arrays
(`ask_prices`/`ask_sizes`) were observed frozen for multiple seconds at a time while
`best_bid`/`best_ask` kept ticking — the depth snapshot itself lags top-of-book, so the
"visible" liquidity above was already somewhat stale by the time it's read here, let
alone by the time the live order reached the CLOB.)

**No config change made based on this evidence** — widening slippage further wouldn't
have helped (attempts already cleared the whole visible book), and this looks like a
liquidity-timing problem intrinsic to trading the last ~100s of these cycles, not a
mispriced retry ladder.

## 3. Fix — more aggressive retry ladder, capped at `max_buy_price` (implemented)

Re-reading the retry math: `retry_slippage_step` (`execution.rs::LiveConfig`) is a
**hardcoded 0.02**, not sourced from `strategy_*.toml` at all (`bin/live.rs:682-685`
only forwards `order_slippage`/`order_max_retries` from the toml; `retry_slippage_step`
always falls through to `LiveConfig::default()`). That's why this cycle's 4 attempts
crept up by 2 cents each (0.795 → 0.855) and never got anywhere near the process-wide
`max_buy_price = 0.95` ceiling that's already enforced via `.min(max_buy_price)` in the
same function — there was another 9.5 cents of configured headroom that was never used.

Since this is a USDC-notional market order (`Amount::usdc(size_usdc)`, not a
share-denominated limit), the `price` argument is a worst-case ceiling, not the price
actually paid — `actual_cost = size_usdc / filled` is always the real weighted fill
price from whatever the book had. Raising the ceiling faster costs nothing if the book
doesn't need it; it only removes retries that were doomed to fail because the cap was
still below available liquidity.

**Implemented** (`src/execution.rs`, `retry_ladder_price` + `LiveExecutionEngine::place`):

1. Replaced the fixed `+= retry_slippage_step` ladder with linear interpolation from
   the first attempt's price up to the *existing* `max_buy_price` parameter, so the
   **final** retry always equals exactly `max_buy_price` — never higher, and no new
   config field needed since `max_buy_price` already is that per-run ceiling:
   ```rust
   fn retry_ladder_price(base_price: f64, max_buy_price: f64, order_max_retries: u32, attempt: u32) -> f64 {
       let step = if order_max_retries > 0 {
           (max_buy_price - base_price).max(0.0) / order_max_retries as f64
       } else {
           0.0
       };
       (base_price + attempt as f64 * step).min(max_buy_price)
   }
   ```
   With today's numbers (price 0.745, order_slippage 0.05, max_buy_price 0.95,
   order_max_retries 3): 0.795 → 0.847 → 0.898 → 0.95 — attempt 4 now lands exactly on
   the cap instead of stopping 9.5¢ short of it at 0.855.
2. Removed the dead `retry_slippage_step` field from `LiveConfig` (no longer used by
   anything, and it was never actually sourced from `strategy_*.toml` in the first
   place — always silently `0.02` regardless of config).
3. Decision on the front-load-vs-interpolate tradeoff (open question above): went with
   linear interpolation — keeps today's cautious first attempt (`order_slippage`)
   intact and only changes how the *remaining* retries close the gap to the ceiling,
   rather than jumping straight to `max_buy_price` on the first retry.
4. Added `execution::tests`: `retry_ladder_reaches_cap_on_last_attempt` (reproduces
   this exact 0.745/0.05/0.95/3 case), `retry_ladder_never_exceeds_max_buy_price`,
   `retry_ladder_monotonic_non_decreasing`, `retry_ladder_zero_retries_stays_at_base`,
   `retry_ladder_no_room_left_stays_at_cap`. Full suite: 118 passed, 0 failed.
5. Out of scope: `SimExecutionEngine` (backtest/tests) doesn't retry at all today —
   left as a single-attempt sim, no change needed for this fix.
