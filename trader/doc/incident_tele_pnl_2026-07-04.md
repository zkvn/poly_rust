# Incident — Telegram pnl doesn't match wallet balance change, ETH WIN 10:00:00 2026-07-04

Investigating why the alert

```
✅ ETH TRADE WIN | 10:00:00 | UP ↑ | high_prob
entry=0.8300 → exit=1.0000 | cycle: $1747.43→$1747.93 | pnl=+$0.0608 | 1W/0L
```

reported `pnl=+$0.0608` while the wallet's actual USDC balance only moved
`7.3123 → 7.3476` (+$0.0353) — a $0.0255 gap.

**Update:** the first version of this doc guessed the gap was "unredeemed
winnings" — wrong, ruled out by hand (no unredeemed balance sits on the
account). Redemption is irrelevant here and dropped from this writeup. The
actual mechanism, below, comes straight out of `live.log` plus Polymarket's
own fee/order-validation docs, not speculation.

Note in passing: `cycle: $1747.43→$1747.93` is *not* a balance figure — it's
`slot.worker.cycle_open_binance()` → `slot.last_binance`, i.e. the underlying
ETH/USD price at cycle-open vs. now (`live.rs:492-497`). Coincidence that
ETH's spot price ($1747-ish) is in the same numeric range as the account's
USDC balance ($7-ish); the two are unrelated.

## 1. What actually happened on the exchange (`live.log:13230-13234`)

```
[ORDER] ETH BUY Up @ 0.8650 size=$1.00 -> placed=true shares=1.2048 cost=0.8300 err=None
[ORDER] ETH CLOSE 1.2048 (TakeProfit) -> status=Matched sold=1.2000 usdc=1.0560 err=None
[ORDER] ETH CLOSE 0.0048 (TakeProfit) -> status=Failed sold=0.0000 usdc=0.0000 err=Some("...invalid maker amount")
[TRADE] TradeRecord { ..., token_price: 0.8300015687029648, exit_price: 1.0, outcome: Win, pnl: 0.0608, exit_attempts: 1, exit_last_error: Some("...invalid maker amount") }
```

So, in order:

1. **BUY** — $1.00 spent, filled **1.2048 shares** at 0.8300/share (`cost = size_usdc / filled` = `1.00 / 1.2048`).
2. **Take-profit fired**, tried to close all 1.2048 shares. `close_position` truncates the sell size to 2 decimals (`floor2`, `execution.rs:406`) → requested **1.2000** shares. This **matched for real**: sold 1.20 shares for $1.0560 (`worker.rs::on_unwind_filled`, partial-fill branch since `1.20 < 1.2048`), realizing `1.20 * (0.88 - 0.8300) ≈ +$0.0600` in genuine, banked cash.
3. The **residual 0.0048 shares** (`1.2048 - 1.2000`, pure `floor2` truncation dust) immediately re-triggers the same take-profit branch on the next `PolyTick`. That sell attempt fails with `"invalid maker amount"` — see §3, this one is *structurally* un-sellable, not a retry-me-later failure.
4. Cycle closes with the 0.0048-share residual still held. It's on the winning side, so `on_cycle_close` values it at `exit_price = 1.0`: `0.0048 * (1.0 - 0.8300) ≈ +$0.0008`.
5. Logged `pnl` = realized (0.0600) + residual mark-to-market (0.0008) = **0.0608**, exactly what the alert showed.

**Answering (1): is the "only 1 share" in the Polymarket UI history a display bug?** No — it's accurate. The bot really only ever sold **1.20 shares** for this position (the 0.0048 remainder was never transacted at all — the sell attempt for it was rejected before it reached the book, so nothing moved). If the UI showed "1", it's almost certainly Polymarket's own history view truncating/rounding "1.20" for display, not evidence of a missing sale — there's no third leg to find, the trade log and the exchange interaction agree there were exactly two attempts (one filled, one rejected).

## 2. Why the pnl estimate doesn't match cash — fees, not redemption

`settle_pnl`/Python's `shares * (1.0 - cost)` formula (`worker.rs:202`, `bot/worker.py:1875`) is arithmetically fine and matches between both bots — checked, they're the same formula. The bug is that **both bots compute *gross* pnl and neither accounts for Polymarket's taker fee**, which is real, non-zero, and charged on both legs of this trade:

Per Polymarket's docs (`docs.polymarket.com/polymarket-learn/trading/fees`), taker fee = `shares × feeRate × p × (1 − p)`, with `feeRate = 0.07` for the **Crypto** category (ETH updown markets are Crypto). Applied here:

- **BUY** (taker, 1.2048 shares @ p=0.83): `1.2048 × 0.07 × 0.83 × 0.17 ≈ $0.0119`
- **SELL** (taker, 1.2000 shares @ p≈0.88): `1.2000 × 0.07 × 0.88 × 0.12 ≈ $0.0089`
- **Total fees ≈ $0.0208**

Add the $0.0008 residual that was marked WIN but never actually left the exchange as cash (§1 step 4 — it's real in the sense the shares are worth $1 now, but nothing credited the wallet for it, matching the "no unredeemed balance" check: it's likely just sub-cent dust Polymarket doesn't bother crediting/tracking), and the accounted-for gap is `$0.0208 + $0.0008 = $0.0216` against the observed **$0.0255** — same order of magnitude and same sign, using only an approximate exit price (0.88, itself derived from the log's `usdc/sold`) and no visibility into whatever precision Polymarket actually uses internally. This is not a coincidence: **fees plus unsold dust account for essentially the entire gap.** Neither `execution.rs` nor `worker.rs` subtracts a fee anywhere — `cost = size_usdc / filled` and `exit_price = filled_usdc / sold` are both fee-blind by construction, so every trade's logged pnl is **systematically overstated by roughly the taker fee on both legs.** For a strategy whose whole edge is a few cents on a $1 position, a ~7%-of-notional×p(1−p) fee eats a large fraction of it — that's exactly the pattern in the daily recon numbers (many WINs logging pnl in the $0.01–$0.11 range, real cash presumably lower across the board).

The SDK already exposes the data needed to fix this properly: `execution.rs`'s BUY path already calls `self.client.fee_info(token_id)` (currently dead-ends inside the never-triggered `adjust_market_buy_amount` branch, since `user_usdc_balance` is never set — see below). That's a real per-market fee rate/exponent from the API, not a guess.

## 3. Root cause of "invalid maker amount" — a structural 2-decimal dust trap, not a transient error

Researched Polymarket's own order-validation rules (`docs.polymarket.com/resources/error-codes`, plus a community write-up on the same error, `dev.to/bluewhale-quant-lab/...`):

> A CLOB order carries two integer amounts, `makerAmount` and `takerAmount`, each equal to `price × size × 1e6` / `size × 1e6` respectively. **Both must be exact multiples of 10,000** — not just "≤2 decimal places," a much stricter, price-dependent step size (`docs.polymarket.com/resources/error-codes` separately lists a *different* error, `"Size (X) lower than the minimum: 5"`, for GTC limit orders under 5 shares — that one doesn't apply here, this is the market/FAK-order amount-quantization error).

For our failed sell: 0.0048 shares → `makerAmount = 0.0048 × 1e6 = 4,800`. **4,800 is below 10,000 outright** — there's no price at which this order could ever be valid; it's not about *which* price we choose, the raw share count itself is too small to produce a legal `makerAmount` no matter what. This is a **hard, unconditional floor**, confirmed distinct from (and worse than) the already-fixed DOGE oversell bug (`incident_doge_2026-07-03.md`, which was about rounding *up* past the true balance) — this is about `floor2()` (2-decimal truncation, `execution.rs:342`/`406`) being fundamentally incompatible with `Amount::shares`' own 2-decimal cap whenever a BUY fill has more precision than 2 decimals, which is the **common case**: `filled_shares = size_usdc / price` is essentially never a clean 2-decimal number (here: `1.00 / 0.83 = 1.2048...`). Every such fill guarantees a `< 0.01`-share remainder that **can never be sold**, regardless of retries, backoff, or which price the SDK's `calculate_price()` picks for the market order.

This means the take-profit path is *structurally* incapable of fully closing out a position whenever the BUY fill isn't a round 2-decimal number — which, per §2, is nearly always. The 1.20/0.0048 split isn't a fluke of this one trade; it's what will happen on essentially every fill from here on.

## 4. Proposed solutions

**A. Model the taker fee explicitly (fixes item 2). — Implemented (`worker.rs`).** Added `taker_fee(shares, price) = shares * 0.07 * price * (1 - price)` (Crypto-category rate) and a `HoldingData.fees` accumulator: set once from the entry BUY at `on_order_filled`, incremented by a sell's own fee at every executed exit (`on_limit_sell_placed`'s Matched branch, and the shared `finalize_or_hold_residual` used by `on_unwind_filled`/`on_stop_sell_filled`). `settle_pnl` now subtracts `h.fees`, and the `ApiResult`-flip recompute in `on_api_result` subtracts the entry fee too. Resolution/redemption itself stays fee-free (not a trade). No live network call needed — the rate is a static, documented constant, not per-market data, so this doesn't need `fee_info`/`/fee-rate` after all.

**B. Stop trying to sell dust below the maker/taker floor, and don't count it as WIN-at-$1. — Implemented (`worker.rs`).** Added `MIN_SELLABLE_SHARES = 0.01` (`shares * 1e6` must clear Polymarket's 10,000-unit `makerAmount` floor — below that, no price makes the order valid). `on_poly`'s stop-loss and take-profit triggers now skip firing `ClosePosition` at all once `h.shares < MIN_SELLABLE_SHARES` (no point placing an order guaranteed to be rejected). More importantly, `finalize_or_hold_residual` (the shared tail of `on_unwind_filled`/`on_stop_sell_filled`) now checks the *leftover* after a partial fill: if it clears the floor, it's still a genuine residual and keeps being managed as before; if it doesn't, the trade finalizes immediately using only the realized proceeds so far — the dust is excluded from `pnl` entirely rather than deferred to `on_cycle_close` and valued at `exit_price = 1.0/0.0`. Covered by `dust_residual_below_min_sellable_is_written_off_not_chased`, which reproduces this incident's exact numbers (1.2048 bought, 1.20 sold, 0.0048 dust) and asserts the trade closes out immediately at the correct net pnl instead of parking in `Holding`.

Also updated `partial_unwind_then_cycle_close_totals_both_legs_pnl` and `api_result_flips_confirming_outcome_and_recomputes_pnl`'s expected pnl values to the new fee-inclusive numbers (both previously asserted the old gross figures).

**C. Buy in rounded shares instead of rounded dollars, so no dust is ever created. — Implemented (`execution.rs`).**

Today's entry BUY is `Amount::usdc(size_usdc)` — "spend exactly $1.00" — which is why `filled_shares = size_usdc / fill_price` is essentially never a clean 2-decimal number (`1.00 / 0.83 = 1.2048...`). But `market_order()` also accepts `Amount::shares(_)` on the **buy** side (`order_builder.rs:588`, not just sells), so the entry can instead request "buy exactly N shares" for a pre-rounded `N`. Once the position starts life at a clean 2-decimal share count, there's nothing beyond 2 decimals left to strand later — B's write-off path becomes a rare fallback (only for a *partial* fill on entry itself) instead of firing on nearly every trade.

**Mechanism:** for a given `size_usdc` and the same reference price already used today (`capped_price` — the per-attempt ceiling from `aggressive_entry_price`), compute:

```
target_shares = round(size_usdc / capped_price, 2)   // nearest cent's worth of shares, not floor()
```

then submit `Amount::shares(target_shares)` at that same `capped_price` ceiling, instead of `Amount::usdc(size_usdc)`. The actual dollar cost becomes `target_shares × actual_fill_price` — close to, but not exactly, `size_usdc`.

**Why the drift stays tiny, worked through $1 / $5 / $100:** rounding `size_usdc / price` to the nearest 0.01 shares moves the share count by at most ±0.005 from the ideal, so the cost error at the *reference* price is bounded by `±0.005 × price`, i.e. **under half a cent, for any bet size** — because the error scales with `price` (always < $1), not with `size_usdc`. Concretely, at a representative spread of entry prices:

| size | price | ideal shares | target shares (rounded) | actual cost | deviation |
|---|---|---|---|---|---|
| $1   | 0.55 | 1.8182   | 1.82   | $1.0010  | +$0.0010 |
| $1   | 0.75 | 1.3333   | 1.33   | $0.9975  | −$0.0025 |
| $1   | 0.83 | 1.2048   | 1.20   | $0.9960  | −$0.0040 |
| $1   | 0.93 | 1.0753   | 1.08   | $1.0044  | +$0.0044 |
| $5   | 0.55 | 9.0909   | 9.09   | $4.9995  | −$0.0005 |
| $5   | 0.75 | 6.6667   | 6.67   | $5.0025  | +$0.0025 |
| $5   | 0.83 | 6.0241   | 6.02   | $4.9966  | −$0.0034 |
| $5   | 0.93 | 5.3763   | 5.38   | $5.0034  | +$0.0034 |
| $100 | 0.55 | 181.8182 | 181.82 | $100.0010 | +$0.0010 |
| $100 | 0.75 | 133.3333 | 133.33 | $99.9975  | −$0.0025 |
| $100 | 0.83 | 120.4819 | 120.48 | $99.9984  | −$0.0016 |
| $100 | 0.93 | 107.5269 | 107.53 | $100.0029 | +$0.0029 |

So: a $1 bet lands at $0.996–$1.004, a $5 bet at $4.997–$5.003, a $100 bet at $99.997–$100.003 — never off by more than half a cent from rounding *at any size*, matching the intuition ("never too far away from 1") but tighter than the 1-3¢ ballpark guessed — because the rounding grid is fixed at $0.01 *of shares*, not of dollars, and gets multiplied by a sub-$1 price.

**This is on top of, not instead of, ordinary price slippage** — which exists today too. Right now slippage shows up as "you got fewer/more shares than size_usdc/signal_price implied"; under this plan it shows up as "you paid a bit more/less than size_usdc" instead. Same underlying market risk, just which side absorbs it changes. The only *new* number here is the sub-half-cent rounding term above.

**Implementation** (`execution.rs::LiveExecutionEngine::place`):
1. Per attempt (mirroring how `capped_price` is already recomputed per retry), compute `target_shares = round2(size_usdc / capped_price)`; if it's `<= 0.0` (possible if `size_usdc` is tiny relative to a near-$1 price), fail fast (`"size too small for price"`) instead of submitting a zero-share order.
2. Swapped `Amount::usdc(size_usdc)` for `Amount::shares(target_shares)` in the `market_order()` builder call.
3. Fixed the fill-accounting formula alongside it: `cost = size_usdc / filled` was only correct when we actually spent `size_usdc`, which is no longer guaranteed once the *shares* side is the fixed one. Now uses `resp.making_amount` (the real USDC actually spent) — `cost = making_amount / filled_shares` — matching what `close_position` already does on the sell side with `making_amount`/`taking_amount`, and correctly handling a partial fill on entry (rare, but possible, and would otherwise reintroduce a non-2-decimal share count — B's write-off still covers that residual if it ever happens).
4. `estimated_shares` (the old fallback-on-parse-failure value) is gone — `target_shares` *is* what gets submitted now, so there's nothing separate to keep in sync; a parse failure on the response just falls back to `filled = 0.0`, which already routes through the existing `cost = price` fallback.

**Tests added** (`execution.rs`):
- `sim_rounds_shares_to_nearest_not_floor` — `1.0 / 0.93 = 1.07527...` must round to 1.08, not floor to 1.07 (`SimExecutionEngine` already modeled rounded-share buys; this pins nearest-not-floor so a regression back to truncation would fail loudly).
- `rounded_share_buy_cost_never_drifts_more_than_half_a_cent` — the doc's $1/$5/$100 × several-price table, asserting the `≤ 0.005 × price` deviation bound generally, not just for one example.
- `round2_of_a_too_small_size_collapses_to_zero` — confirms the precondition the `target_shares <= 0.0` guard depends on.

`LiveExecutionEngine::place` itself isn't unit-testable (needs a live/authenticated SDK client) — coverage is via the pure rounding math above plus `SimExecutionEngine`, which already shares the same `round2(size_usdc / capped_price)` sizing philosophy.

**D. Surface real fee-adjusted pnl in recon.** Once (A) lands, have `trade_reconcile.py`/`daily_recon` show gross vs. net (fee-adjusted) pnl side by side, so future gaps between "what Telegram said" and "what the wallet shows" are visible from the CSV alone instead of requiring a fresh log-diving investigation each time.
