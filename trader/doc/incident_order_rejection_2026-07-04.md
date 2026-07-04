# Incident — DOGE BUY rejected, "invalid amounts ... max accuracy of 2 decimals", 2026-07-04

Telegram alert:

```
❗ DOGE Order REJECTED | 23:09:37 | T-22s | UP ↑ | reversal
signal price=0.6100 | delta=+0.270% | attempts=4 | error=Status: error(400 Bad Request) making
POST call to /order with {"error":"invalid amounts, the market buy orders maker amount supports
a max accuracy of 2 decimals, taker amount a max of 4 decimals"}
```

## 1. This is a total entry outage, not a one-off

Oracle's `live.log` (`/home/ubuntu/apps/poly_rust/trader/live_logs/live.log:20276-20280`) shows
all 4 attempts failing with the identical error at two different prices:

```
[ORDER-RETRY] ... BUY attempt 1/4 price=0.7800 -> invalid amounts, ...
[ORDER-RETRY] ... BUY attempt 2/4 price=0.9500 -> invalid amounts, ...
[ORDER-RETRY] ... BUY attempt 3/4 price=0.9500 -> invalid amounts, ...
[ORDER-RETRY] ... BUY attempt 4/4 price=0.9500 -> invalid amounts, ...
[ORDER] DOGE BUY Up @ 0.6100 size=$1.00 -> placed=false shares=0.0000 cost=0.0000 err=...
```

The live binary on Oracle was redeployed at **22:51** (`target/release/live` mtime). This DOGE
line at 23:09:37 is the **only** `[ORDER] ... BUY` line anywhere in the log since that redeploy
— i.e. this isn't an edge case that happened to hit an unlucky price, it's the *first* entry
attempt on the new binary, and it failed on every single retry. The bug is deterministic and
price-independent (see §2), so as it stands **no asset can currently open a new position** —
this blocks ETH/BTC/DOGE, both strategies, entirely, until fixed.

## 2. Root cause — Plan C's `Amount::shares` entry BUY violates a market-buy-specific precision rule

Commit `7d0f96c` ("buy in rounded shares instead of rounded dollars", Plan C of
`incident_tele_pnl_2026-07-04.md` §3, later patched by `49d7f77` for the $1 floor) switched
entry BUYs from `Amount::usdc(size_usdc)` to `Amount::shares(entry_shares_for_buy(size_usdc,
capped_price))`. That fixed the unsellable-dust bug on the *exit* leg, but broke the *entry*
leg against a Polymarket rule neither incident doc checked: **for a market/FAK BUY order, the
maker amount (the USDC leg) must itself be an exact multiple of $0.01 — at most 2 decimal
places — while the taker amount (shares) may have up to 4.** This is exactly what today's
error message states, and it's a real, previously-undocumented constraint distinct from both
the $1.00 marketable-notional floor (`incident_order_fail_2026-07-04.md`) and the GTC "min 5
shares" rule (`incident_tele_pnl_2026-07-04.md` §3).

The two BUY code paths in the vendored SDK (`polymarket_client_sdk_v2-0.6.0-canary.1/src/clob/
order_builder.rs:583-591`) differ in which side is "raw" (caller-supplied, therefore
naturally-scaled) and which is *derived* by multiplying by price:

```rust
(Side::Buy, AmountInner::Usdc(_)) => {           // pre-Plan-C path
    let shares = (raw_amount / price).trunc_with_scale(decimals + LOT_SIZE_SCALE); // 4dp derived
    (shares, raw_amount)                          // maker = raw_amount = caller's $X.XX, always ≤2dp
}
(Side::Buy, AmountInner::Shares(_)) => {          // Plan C's path — what we use today
    let usdc = (raw_amount * price).trunc_with_scale(decimals + LOT_SIZE_SCALE); // 4dp derived
    (raw_amount, usdc)                            // maker = usdc = shares × price, generically 4dp
}
```

`Amount::usdc(size_usdc)` naturally satisfies the maker-≤2dp rule because the caller always
supplies a 2-decimal dollar figure ($1.00) directly as the maker amount — the *derived* side
(shares) is allowed 4dp and nothing rejects it. Plan C's `Amount::shares(...)` inverts this:
now the maker amount is the *derived* side (`shares × price`), and multiplying a 2-decimal
share count by a 2-decimal price generically needs up to 4 decimal places to represent exactly
— which only avoids violating the maker's 2dp cap in the rare case that the last two digits of
`shares × price` happen to cancel to zero. Confirmed with tonight's actual numbers
(`entry_shares_for_buy`, `execution.rs:143`):

| attempt | `capped_price` | `entry_shares_for_buy` | maker (usdc) = shares × price | decimals | valid? |
|---|---|---|---|---|---|
| 0 | 0.78 | 1.29 (bumped past $1 floor) | **1.0062** | 4 | ✗ rejected |
| 1–3 | 0.95 | 1.06 (bumped past $1 floor) | **1.0070** | 4 | ✗ rejected |

Both violate the maker-amount 2-decimal rule regardless of retry or price — matching the log
exactly (identical error on every attempt, at two different prices). Unlike the $1-floor
incident (`incident_order_fail_2026-07-04.md`, fixed by `49d7f77`'s `ceil2` bump), **this
failure mode cannot be patched by adjusting the rounding/bump arithmetic** — any 2-decimal
share count multiplied by a 2-decimal price will only rarely land on a clean 2-decimal dollar
figure. `Amount::shares` is structurally the wrong shape for a market BUY's maker leg.

## 3. Proposed fix (not yet applied)

Revert the entry BUY (only) to `Amount::usdc(size_usdc)`, restoring the pre-Plan-C code path in
`LiveExecutionEngine::place` (`execution.rs`):

- Drop `entry_shares_for_buy`/`ceil2`/`MIN_MARKETABLE_NOTIONAL_USDC` from the BUY call and pass
  `Amount::usdc(Decimal::from_str(&format!("{size_usdc:.2}"))?)` instead — `size_usdc` is a
  fixed, already-≥$1.00 config value (`trade_size_usdc.default = 1.0`), so the $1 floor from
  `incident_order_fail_2026-07-04.md` can't recur either; that guard becomes unnecessary rather
  than needing to be preserved.
- Keep the fill-accounting fix from Plan C as-is (`cost = making_amount / taking_amount` off
  the actual response) — it's correct regardless of which `Amount` variant was submitted and
  already matches `close_position`'s pattern on the sell side.
- This reintroduces the *original* problem Plan C was trying to solve — entry fills land on a
  non-2-decimal share count (e.g. `1.00 / 0.83 = 1.2048...`), leaving a `<0.01`-share residual
  that can never itself be sold. That is expected and **already handled**: Plan B
  (`MIN_SELLABLE_SHARES` write-off, implemented in `worker.rs` per
  `incident_tele_pnl_2026-07-04.md` §4) already detects exactly this residual and finalizes the
  trade off the realized proceeds instead of chasing an unfillable sell — confirmed working in
  production before today's regression (`live.log:10949`, `13232`, `14417` all show the
  expected `Failed ... invalid maker amount` on the dust leg followed by a clean trade close,
  not a stuck position).
- Net effect: reverting Plan C trades "up to ~half a cent of untracked dust per trade" (the
  problem Plan C set out to fix, already contained by Plan B) for "the trader can place orders
  at all" (today's regression, currently 100% of entries). Given the severity, reverting is the
  right trade until/unless a way is found to request a *market* BUY with an exact-2-decimal
  USDC notional AND a clean 2-decimal share fill simultaneously — which, per §2, may not be
  possible at arbitrary prices at all, since it requires `shares × price` to itself be a
  round cent.

**Suggested immediate action:** since production has been fully blocked on entries since the
22:51 redeploy, treat this as a hotfix — revert, rebuild, redeploy to Oracle ahead of the next
recon cycle, rather than folding it into the next regular deploy.
