# Audit — BTC reversal entered at T-48s @ fill 0.93, forced flat by 20s timeout, 2026-07-16

## 1. What happened

Telegram alert:

```
⏱️ BTC TIME LIMIT triggered | 17:54:30 | T-29s | DOWN ↓ | reversal
price=0.9650 | max holding time elapsed — closing at market
```

Reading this alert alone, it looks like a DOWN position got force-closed at a price of 0.9650 —
i.e. entered cheap, ran up almost to certainty, and got timed out instead of taking profit. That's
not what happened. The `price=0.9650` field is a **stale market quote snapshotted at the
timeout-trigger instant** (`slot.last_poly_dn`, `trader/src/bin/live.rs:1012-1016`), not the
entry or exit fill price. The actual trade: bought DOWN at **0.93**, sold DOWN at **0.93**, 25.5s
later, `pnl=-0.0098` — a flat round trip that captured no edge, not a loss driven by a bad exit
price.

## 2. Timeline (`live.log` on Oracle, `trader/live_logs/live.log:182052-182097`, cycle `btc-updown-5m-1784195400`, 17:50:00-17:55:00 HKT)

```
T-115s  heartbeat  binance=64128.54  up=0.9050 dn=0.0950   (market pricing UP strongly)
T-85s   heartbeat  binance=64113.35  up=0.8850 dn=0.1150
T-55s   heartbeat  binance=64040.00  up=0.0350 dn=0.9650   <- binance drops 64113->64040, market flips hard to DOWN
                                                                (this is where the 0.9650 in the alert comes from)
[ORDER-RETRY] BUY attempt 1/6 price=0.9175 -> "no orders found to match with FAK order" [NoMatch]
[ORDER-RETRY] retrying in 10ms (NoMatch)
[ORDER] BTC BUY Down @ 0.8850 -> placed=true shares=1.0753 cost=0.9300  (n_attempts=2, process_ms=887)
[unwind] fill  side=Buy price=0.93 size=1.075257            <- entry_ts 17:54:10.772 (T-49s of the 300s cycle)
📋 BTC Order placed | 17:54:11 | T-48s | DOWN | reversal

[TIMEOUT] BTC max holding time elapsed — closing 1.0753 shares (unwind_time floor crossed)
⏱️ BTC TIME LIMIT triggered | 17:54:30 | T-29s | DOWN | reversal   price=0.9650   <- last_poly_dn, stale since T-55s tick
[close] retry 1/5: "no orders found to match with FAK order"
[unwind] fill  side=Sell price=0.93 size=1.07
📤 BTC TIME LIMIT order executed | 17:54:31 | reversal

[TRADE] token_price=0.93, exit_price=0.93, outcome=Timeout, pnl=-0.0098,
        entry_process_latency_ms=887, exit_process_latency_ms=1087
```

## 3. Why entry was so late in the cycle

`ReversalStrategy::evaluate` (`trader/src/strategies.rs:39-77`) fires DOWN only when three
things are simultaneously true: `saw_low_dn` has latched (DN dipped below
`reversal_low_threshold`=0.20 earlier in the cycle), the *current* `dn > reversal` (0.55 for BTC),
and `delta_pct < 0` (Binance price actively falling right now). All three only lined up once the
genuine Binance move (64113→64040) happened, which itself only happened at T-55s. Entries are
gated off only inside the last 10s of a cycle (`no_enter_when_time_left=10`, global) — T-49s is
comfortably inside that window by design, so nothing blocked this from firing. **The lateness is a
direct consequence of the underlying price move itself arriving late, not a bug in the entry
gating.**

## 4. Why the fill landed at 0.93, above the 0.90 `price_high_rev` gate

The gate that's supposed to keep reversal entries out of expensive, low-edge territory is
`price_high_rev` (`trader/src/gates.rs:79-84`): blocks any reversal entry where
`token_price > 0.90`. It correctly evaluated against the **signal** price at decision time
(0.8850, per the `[ORDER] BTC BUY Down @ 0.8850` log line) — which passed clean, 5 cents under the
ceiling.

But the *first* FAK buy attempt, at the volatility-widened price of 0.9175, hit `"no orders found
to match with FAK order"` — the book was thin right at the moment of the whipsaw. The retry logic,
`aggressive_entry_price()` (`trader/src/execution.rs:221-226`), walks the limit price toward
`max_buy_price` (0.95, global default) on each retry specifically to guarantee a fill in exactly
this kind of fast-moving/thin-book scenario. Attempt 2 filled at 0.93 — 3 cents above the
`price_high_rev` ceiling the entry was gated on, and 4.5 cents above the original signal price.

This isn't a code bug — the retry-escalation design is intentional and documented (guarantee a
fill rather than miss the reversal entirely) — but it does mean **`price_high_rev` only bounds the
signal price, not the realized fill price**. A retry that has to escalate past a thin patch of the
book can land the actual cost basis above the gate that nominally exists to prevent expensive
reversal entries.

## 5. Why it timed out flat instead of profiting

BTC's `unwind_time_rev` override is 20s (`trader/config/strategy_20260715.toml:104`, tighter than
the 25s default) — a hard force-close at market after 20s regardless of price. Entering at T-49s
with only a 20s runway before an unconditional exit left very little time for the reversal to
develop further before the position was closed no matter what. Combined with an entry cost basis
already at 0.93 (7 cents of headroom to certainty), there was minimal edge available even in the
best case. In this instance price didn't move further in either direction by T-29s (still 0.93 on
both sides of the trade), so the position closed flat, with `pnl=-0.0098` reflecting only fees/
rounding, not a bad decision on the exit.

## 6. Verdict

Not a bug. Three ordinary, individually-correct mechanisms compounded into a low-quality trade:
a genuinely late Binance move (drove late entry timing, by design not blocked until T-10s), thin
order-book liquidity during that same volatility spike (drove retry slippage past the intended
0.90 entry ceiling, via the documented aggressive-retry design), and BTC's tightened 20s timeout
(left no runway to recover once the entry basis was already high). The `price=0.9650` in the alert
being a stale quote rather than the fill price is what made this trade look, at a glance, like a
lost take-profit opportunity — it wasn't; both fills were 0.93 and the trade was flat before fees.

**Gap worth flagging, not fixing now:** `price_high_rev` is checked once against the pre-retry
signal price and never re-validated against the escalating retry price or the realized fill cost —
so a thin-book retry can silently land above the configured ceiling with no alert distinguishing
"clean fill at signal price" from "retried up to near `max_buy_price`". Added to README TODO.
