# Incident — SOL UNWIND lost money despite a correct directional call, 2026-07-06

Telegram alert:

```
✅ SOL TRADE UNWIND | 05:44:33 | UP ↑ | reversal
entry=0.9000 → exit=0.8200 | cycle: $81.61→$81.67 | pnl=-$0.1073 | 0W/0L
```

## 1. This is not a bad directional call — the underlying went the *right* way

`live_trades_sol_reversal.csv` row for this trade:

```
logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,exit_attempts,exit_last_error
1783287873.39,sol-updown-5m-1783287600,reversal,UP,1783287870,0.8999999999999999,0.82,UNWIND,-0.1073,0,
```

Binance SOL moved `$81.61 → $81.67` across the cycle — up, the same direction as the `Up` position taken. This is the **only** UNWIND row across all four in the SOL CSV where `exit_price < token_price`; the other three all show a normal profitable take-profit (`exit_price > token_price`). So the loss isn't the reversal thesis failing — it's the exit fill itself landing far below both the entry price and the intended take-profit target.

## 2. Sequence, from `live.log` (lines 43823–43836, cycle `sol-updown-5m-1783287600`)

```
[live] heartbeat SOL (reversal) ... T-43s binance=81.5600 up=0.1150 dn=0.8850
[ORDER] SOL BUY Up @ 0.8950 size=$1.00 -> placed=true shares=1.1111 cost=0.9000 err=None
[close] retry 1/5: ... "not enough balance / allowance: the balance is not enough -> balance: 0, order amount: 1110000"
[close] retry 2/5: ... "no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found."
[close] retry 3/5: ... "no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found."
[ORDER] SOL CLOSE 1.1111 (TakeProfit) -> status=Matched sold=1.1100 usdc=0.9102 err=None
[TRADE] TradeRecord { ..., side: Up, token_price: 0.9, exit_price: 0.82, outcome: Unwind, pnl: -0.1073, exit_attempts: 0, ... }
[live] heartbeat SOL (reversal) ... T-13s binance=81.6300 up=0.8050 dn=0.1950
```

Entry filled at an average price of **0.90** (signal was 0.895; the entry itself swept a thin book up to 0.90 — see §5). The take-profit target (`tp_price`) armed at entry was `0.90 + unwind_pnl_rev(0.03) = 0.93` (`worker.rs:619`, `strategy_20260705.toml` `[unwind_pnl_rev] default = 0.03`, no SOL override). Instead of selling at 0.93, the position was closed **3.4 seconds later** for an average of **0.82** — 11 cents below the intended target and 8 cents below entry.

`exit_attempts=0` in the TradeRecord only means the *worker-level* retry (re-invoking `close_position()` on a later tick) never happened — it filled on the same call. But **within that single call**, `close_position()` has its own internal retry loop (`execution.rs:429`, `self.cfg.close_max_retries = 5`) that resubmits the FAK sell on certain errors. The log shows 3 failed internal attempts (unsettled balance, then no book match twice) before the 4th succeeded — meaning the sell order was live and re-submitting into the book for ~3.4 seconds while price reverted from the entry-time spike back down to 0.82.

Fee math checks out against the logged pnl: `1.1111 × (0.82 − 0.90) = -0.0889` on price alone, plus ≈$0.018 combined entry+exit fees ≈ **-0.1073**, matching exactly.

## 3. Root cause: entry has a max-price guard, exit has no min-price guard

**Entry side — capped, by two independent layers:**
- `gates.rs:76-78` — `MaxBuyPrice` gate rejects entry if `token_price > params.max_buy_price` (0.95).
- `gates.rs:79-84` — `PriceHighRev` gate (reversal-only) rejects entry if `token_price > price_high_rev` (0.90).
- `execution.rs:280-322` — the BUY is placed as a **limit** FAK (`.price(price_dec)`, `execution.rs:319`), with the limit price bounded by `aggressive_entry_price()` (`execution.rs:133-138`), so it structurally cannot fill above `max_buy_price`.

**Exit side — no equivalent floor:**
- Both take-profit ("unwind") and stop-loss exits funnel into `LiveExecutionEngine::close_position()` (`execution.rs:411-473`), called from `worker.rs:592` (StopLoss) and `worker.rs:602` (TakeProfit).
- Its order builder (`execution.rs:429-437`) is a bare `market_order()` / `OrderType::FAK` sized only in shares — **no `.price(...)` call anywhere in this function**. Compare directly to the entry builder at `execution.rs:313-322`, which does call `.price(price_dec)`.
- The only per-exit check is `h.shares >= MIN_SELLABLE_SHARES` (`worker.rs:586`, `:600`) — a dust-avoidance check, not a price guard.
- Net effect: once an unwind/stop-loss is triggered, the sell will fill at **whatever price the book gives it**, arbitrarily far below the trigger price, with nothing to reject or bound a bad fill.

Confirming this gap was never wired up, not merely undocumented: `strategy_20260705.toml:13` has

```
# FAK order slippage — covers normal 1-tick bid-ask spread without sweeping wide asks.
order_slippage = 0.05
```

`grep -rn "order_slippage" trader/src/` returns **zero matches** — this config key is parsed nowhere. A slippage/price-floor guard was evidently planned (there's even a comment describing its intended purpose) but never implemented in `close_position()` or anywhere else.

This also matches a prior, still-open observation in `trader/doc/latency_2026-07-04.md:113-115`, which flagged that "a stale price at exactly the wrong moment matters more for unwind than for entry, since unwind is racing a specific target price" — anticipating this exact failure mode as a latency-instrumentation gap, before it produced a concrete realized loss.

## 4. Why "reversal" in the alert is misleading here

The `reversal` in `✅ SOL TRADE UNWIND | ... | reversal` is `slot.worker.strategy_name` — the label for the *entry strategy* (`ReversalStrategy`, dip-and-recover entry logic), not a description of what happened on exit. "UNWIND" is this codebase's name for a take-profit close (`CloseReason::TakeProfit`, armed via `ExitArm::PriceMonitor { tp_price }`, fired at `worker.rs:599-602` once `exit_price >= tp_price`). Nothing in either label describes price direction on the way out — the alert format doesn't distinguish "hit target cleanly" from "hit target then the fill slipped," which is part of why this looked surprising at a glance.

## 5. Secondary, smaller effect: entry itself already paid up

Signal price was 0.895 (under both the 0.90 `price_high_rev` gate and the 0.95 `max_buy_price` gate), but the BUY's actual average fill was 0.90 — the entry-side FAK swept a thin book and `aggressive_entry_price()` (bounded by `max_buy_price = 0.95`, not `price_high_rev`) let it walk up to 0.90 across retries. This cost half a cent of entry-side slippage before the exit-side loss even began; not the main driver of the loss, but consistent with SOL's book being thin at this moment on both sides of the trade.

## 6. Proposed fix (not yet applied)

Give `close_position()` a bounded worst-acceptable-price, mirroring the entry side:

- Add a `min_sell_price` (or reuse/wire up the existing unused `order_slippage` config key as `tp_price - order_slippage` / `entry_cost - order_slippage` depending on exit reason) and pass it into `close_position()`.
- Submit the FAK sell as a **limit** order at that floor (`.price(...)`, same pattern as `execution.rs:319`) instead of an unbounded market order — same mechanism the entry side already uses, just mirrored for sells.
- Decide the fallback behavior when the floor can't fill: for a *take-profit* unwind, holding for a later tick (position isn't otherwise at risk of forced settlement) is safer than eating unbounded slippage; for a *stop-loss* exit, a wider or no floor is more defensible since the position needs to close regardless — these two `CloseReason`s likely want different floors, which the current shared `close_position()` signature doesn't distinguish.
- Wire up `UnwindWatcher` (`trader/src/unwind.rs`, implemented and tested per `latency_2026-07-04.md:170-178` but never called from `live.rs`) so future incidents like this one have a real-time record of book conditions at the moment of the fill, rather than reconstructing it after the fact from heartbeat snapshots 30 seconds apart.
