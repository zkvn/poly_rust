# Incident — ETH BUY rejected, "invalid amount for a marketable BUY order", 2026-07-04

Telegram alert:

```
❗ ETH Order REJECTED | 14:59:45 | T-14s | DOWN ↓ | high_prob
signal price=0.8950 | delta=-0.042% | attempts=4 | error=Status: error(400 Bad Request) making
POST call to /order with {"error":"invalid amount for a marketable BUY order ($0.9975), min
size: 1"}
```

## 1. This is deterministic, not a flake

Config in effect (`/home/kev/apps/btc_5mins/config/strategy_20260703.toml`):
`trade_size_usdc.default = 1.0`, `max_buy_price = 0.95`, `order_max_retries = 3` (→ 4 total
attempts, matching the alert's `attempts=4`).

Walking `LiveExecutionEngine::place`'s per-attempt price (`aggressive_entry_price`,
`execution.rs:132`) and share sizing (`round2(size_usdc / capped_price)`, `execution.rs:303`)
for this signal:

| attempt | `capped_price` | `round2(1.0/price)` shares | cost |
|---|---|---|---|
| 0 | 0.9225 (half-spread toward `max_buy_price`) | 1.08 | $0.9963 — rejected |
| 1 | 0.95 (`max_buy_price`, every retry after attempt 0 skips straight there) | 1.05 | $0.9975 — rejected |
| 2 | 0.95 | 1.05 | $0.9975 — rejected |
| 3 | 0.95 | 1.05 | $0.9975 — rejected |

$0.9975 is exactly the amount the alert reported. Attempts 1–3 recompute the *same* capped
price and therefore the *same* rounded share count — retrying achieves nothing once this
arithmetic coincidence occurs. This will recur on any asset whenever
`round2(size_usdc / price) * price` happens to land under $1.00, which is roughly a coin-flip
on every entry at `trade_size_usdc = 1.0`.

## 2. Root cause — a regression from the rounded-shares change

Commit `7d0f96c` ("buy in rounded shares instead of rounded dollars", plan C of
`incident_tele_pnl_2026-07-04.md` §3) switched entry BUYs from `Amount::usdc(size_usdc)`
(always spends exactly `size_usdc`, whatever share count that buys) to
`Amount::shares(round2(size_usdc / capped_price))` (buys a rounded share count, whatever that
costs) — correctly fixing the unsellable-dust bug described there. That doc's own worked
table already showed the resulting cost can land as low as **$0.9960–$0.9975** on a $1 bet
depending on price — but nobody checked that figure against Polymarket's *other*
order-validation floor: **a marketable (FAK/market) BUY order must be worth at least $1.00
notional**, distinct from the GTC-limit "min 5 shares" rule already documented in that same
doc's §3. Nothing in `place()` checks for it, so any rounding-down outcome near the floor is
submitted and rejected outright, four times, on every retry.

## 3. Proposed fix (not yet applied)

In `LiveExecutionEngine::place` (`execution.rs`), after computing
`target_shares = round2(size_usdc / capped_price)`: if `target_shares * capped_price < 1.00`,
replace it with `ceil2(1.00 / capped_price)` — the smallest 2-decimal share count whose
notional clears the $1.00 floor at that price — instead of submitting the under-floor amount.
Cases already `>= $1.00` are left untouched, so the existing "never off by more than half a
cent" drift behavior for normally-sized bets is unaffected; the bump only fires for bets
priced close to the floor.

**Scenarios** (`size_usdc = $1.00` unless noted):

| price | naive `round2` shares | naive cost | bump? | `ceil2(1.00/price)` shares | new cost |
|---|---|---|---|---|---|
| 0.95 | 1.05 | $0.9975 ❌ | yes | 1.06 | $1.0070 |
| 0.9225 | 1.08 | $0.9963 ❌ | yes | 1.09 | $1.0055 |
| 0.83 | 1.20 | $0.9960 ❌ | yes | 1.21 | $1.0043 |
| 0.75 | 1.33 | $0.9975 ❌ | yes | 1.34 | $1.0050 |
| 0.55 | 1.82 | $1.0010 ✅ | no (unchanged) | — | $1.0010 |
| 0.93 | 1.08 | $1.0044 ✅ | no (unchanged) | — | $1.0044 |
| 0.83, size=$5 | 6.02 | $4.9966 ✅ | no (unchanged) | — | $4.9966 |

The last row shows the guard is effectively a no-op for bets sized well above the $1 floor
(e.g. $5 stakes) — it only engages when `trade_size_usdc` is close enough to $1.00 that
rounding can tip the cost under it, which today means `trade_size_usdc.default = 1.0`
specifically.

**Caveats:**

1. **Boundary inclusivity is unverified.** The one data point we have is a *rejection* at
   $0.9975 — we don't know if Polymarket's floor is `>= $1.00` (inclusive) or `> $1.00`
   (exclusive). `ceil2` can land exactly on $1.00 (e.g. price = 0.50 → 2.00 shares → exactly
   $1.00), which would still fail if the floor is exclusive. The first live retry after this
   fix ships is the empirical test of that boundary; if it still rejects at exactly $1.00, the
   fix needs a small epsilon buffer (e.g. target `$1.0001`) on top of `ceil2`.
2. **Only matters near the floor.** If any asset's `trade_size_usdc` were ever configured
   below $1.00, `ceil2(1.00/price)` would force spending close to double the intended size.
   No current config does this (`trade_size_usdc.default = 1.0`), so this is a documented
   limitation, not something the fix handles.

## 4. Not yet implemented

This doc captures diagnosis + proposed fix only. `execution.rs` has not been changed —
holding for review before implementing.
