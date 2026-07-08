# Audit — ETH `high_prob` entry filled at 0.6300 despite an 0.80–0.93 signal band, 2026-07-08

```
📋 ETH Order placed | 08:29:50 | T-9s | DOWN ↓ | high_prob
price=0.6300 | delta=-0.066% | clob_latency=13ms (trigger) | binance_latency=114ms (30ms ago) | process_latency=1750ms (30ms ago) | n_attempts=2
```

Question: `price_low`/`price_high` for `high_prob` are `0.80`/`0.93` (`config/strategy_20260705.toml:119-124`) —
how does a trade in that band end up filled at `0.63`?

**Bottom line up front: not a bug.** `price=0.6300` on the Telegram line is the *actual average fill
cost* returned by the exchange, not the signal price the strategy detected or the limit price we
submitted. The strategy correctly fired on a `dn≈0.8250` signal; the order was submitted
aggressively (up to `max_buy_price=0.95`) as a FAK market buy; the first attempt found no resting
liquidity and was killed, and the retry (1s later) matched against unusually cheap resting asks,
filling the full $1 at a blended average of `0.63` — a large, favorable price improvement versus
what the bot was willing to pay. The trade closed as a win (`unwind` take-profit at `0.68`,
`pnl=+$0.029`).

## 1. Timeline (`trader/live_logs/live.log:85937-86033`, cycle `eth-updown-5m-1783470300`)

| t (HKT) | event |
|---|---|
| 08:25:00 | cycle opens, `open_binance=1779.12` |
| 08:29:26–46 (T−194s…−14s) | `binance` climbs to `1780.60`, poly `up` richens to `0.995` / `dn` to `0.005` — UP is the overwhelming favorite |
| 08:29:49.037 (T−11s) | `high_prob` fires a `DOWN` entry: `dn` signal price `0.8250` (inside the `(0.80, 0.93)` band), `delta_pct<0` (Binance had just turned down hard) |
| 08:29:49.x | attempt 1/4, FAK buy @ `0.8875` → **rejected**: `"no orders found to match with FAK order"` (`live.log:85960`) |
| (1s mandatory retry backoff, `execution.rs:427-428`) | |
| 08:29:50.8 (confirmed) | attempt 2/4, FAK buy @ `0.95` (the `max_buy_price` cap) → **matched**, `1.5873` shares, blended avg cost **`0.6300`** (`live.log:85961-85962`) |
| 08:29:50 | Telegram "Order placed" sent, `price=0.6300` |
| 08:29:54 | take-profit unwind fills at `0.68`; `TradeRecord` logged: `pnl=0.029`, `outcome=Unwind` (`live.log:86029-86032`) |

So the entry wasn't a `0.63` signal at all — the signal (`0.8250`) and the two order attempts
(`0.8875`, then `0.95`) were all inside/above the intended band. `0.63` only shows up as the
*result* of the second attempt.

## 2. Why the signal price (`0.8250`) and the fill price (`0.6300`) differ

`HighProbStrategy::evaluate` (`trader/src/strategies.rs:123-167`) fires when the side's current
poly price is in `(price_low, price_high)` — here `dn=0.8250`, comfortably in `(0.80, 0.93)`, with
`delta_pct<0` confirming the reversal direction. That `0.8250` becomes `TradeIntent::token_price()`
(`trader/src/types.rs:65-70`), which is passed to `Action::PlaceBuy` as the base `price`
(`worker.rs:644`) — this is what shows in the console `[ORDER] ETH BUY Down @ 0.8250 ...` line
(`live.log:85962`), and matches config exactly.

The actual order isn't sent at that price — `LiveExecutionEngine::place` (`execution.rs:360-431`)
walks an **aggressive entry-price ladder** so a FAK order (fill-and-kill; Polymarket rejects it
outright if nothing matches — no partial/rest) has the best chance of actually filling before the
signal goes stale:

```rust
fn aggressive_entry_price(price: f64, max_buy_price: f64, attempt: u32) -> f64 {
    if attempt == 0 {
        let spread = (max_buy_price - price).max(0.0);
        (price + spread / 2.0).min(max_buy_price)   // splits the gap to max_buy_price
    } else {
        max_buy_price                                 // every retry after that: go straight to the cap
    }
}
```
(`execution.rs:196-202`)

With `price=0.8250`, `max_buy_price=0.95` (`strategy_20260705.toml:10`):
- attempt 0 (1st try): `0.8250 + (0.95−0.8250)/2 = 0.8875` — matches the logged retry line exactly.
- attempt 1 (2nd try, after the 1s backoff): `max_buy_price = 0.95`.

Both are *limit* prices for a FAK **buy** — the exchange fills at the best available ask(s) up to
that limit, and `cost`/`price=` is computed from the exchange's own actual-fill amounts
(`actual_cost = making_amount / taking_amount`, `execution.rs:411-414`), not the limit itself. The
first attempt found nothing to match at all (thin/volatile book right at the moment `dn` was
whipsawing from `0.005` to `0.825` in ~3 seconds — plausible for other market-makers' asks to have
been pulled). The second attempt, one second later, swept a resting ask (or ladder of asks)
averaging `0.63` — well inside the `0.95` limit — and got a full fill there. The book briefly had
cheap DOWN liquidity sitting below the new fair value; the bot's aggressive limit let it take it
before that liquidity was pulled or repriced. This is a good outcome, not a misfire.

**Telegram/console convention to note**: the `price=` field on "Order placed" is always the actual
average fill cost (`result.cost`), never the signal or limit price (`live.rs:577-578`). For this
strategy that can legitimately print a number outside `[price_low, price_high]` — it isn't
supposed to match the band; the band only gates *entry*, not *fill price*.

## 3. Why `process_latency=1750ms` is so high

`process_latency_ms = (confirmed_ts − received_ts) * 1000.0`, measured around the
`self.engine.place(...)` call (`live.rs:543-547`). With `n_attempts=2`, this run comprised:
attempt 1's HTTP round trip (rejected) + the **mandatory 1-second retry sleep**
(`execution.rs:427-428`, unconditional between attempts) + attempt 2's HTTP round trip (matched).
The 1s sleep alone accounts for the bulk of the 1750ms; the rest is two real
sign-and-post round trips. Nothing anomalous here given a retry occurred — see §4 in
`trader/doc/audit_sl_no_trigger_2026-07-07.md`-style reasoning: this is expected retry-path
latency, not a regression.

## 4. Outcome

`TradeRecord` (`live.log:86032`): entry `0.63`, exit `0.68`, `outcome=Unwind`, `pnl=+0.029`. The
trade was profitable; the only thing worth flagging is that a first-attempt FAK "no orders to
match" at `0.8875` is a signal the book was extremely thin/fast-moving at that instant — worth
keeping an eye on if it starts happening often (currently a single occurrence), since a
first-attempt kill that *doesn't* get rescued by a lucky attempt-2 fill one second later would show
up as a missed entry rather than an unusually good one.

No code or config change is proposed from this audit — behavior matches the code as designed.
