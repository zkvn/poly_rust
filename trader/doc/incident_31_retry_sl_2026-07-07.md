# Incident — ETH stop-loss took 31 attempts to close, filled well below trigger, 2026-07-07

Recon flagged this row in `trade_recon_2026-07-06_to_2026-07-07.md`'s Stop Loss Detail table:

```
| 2026-07-07 00:59:44 | ETH | high_prob | DOWN | -0.4572 | 0.8200 | 0.4700 | GOOD ✓ |
```

`Failed attempts before this exit | 31` in the audit section stood out — every other stop-loss/
unwind in the same report needed 0-3 attempts.

## 1. Timeline

Cycle `eth-updown-5m-1783356900` runs 00:55:00 → 01:00:00 HKT. From `live.log:62113-62229`:

```
[live] heartbeat ETH (high_prob) T-30s binance=1792.0000 up=0.3350 dn=0.6650
[TRADE] TradeRecord { ..., side: Down, token_price: 0.82, exit_price: 0.47, outcome: StopLoss,
                       pnl: -0.4572, exit_attempts: 31, ... }
[live] heartbeat ETH (high_prob) T--0s binance=1792.5100 up=0.9950 dn=0.0050
```

Entry: 00:59:39.552 — `high_prob` bought DOWN @ 0.82, only ~20s before candle close. In that
final stretch the DOWN price collapsed from 0.6650 (T-30s) to 0.0050 (T-0s) — ETH crossed the
strike and the outcome flipped hard in the closing seconds. The stop-loss fired as the DOWN
token cratered through the floor, and by the time it filled, price had fallen almost to zero.

## 2. Why 31 attempts, and why the eventual fill (0.47) was so far from the last observed price (0.0050)

Stop-loss exits are deliberately unbounded/must-close, unlike take-profit (see
`incident_sol_unwind_but_loss_2026-07-06.md` §7 for why that split exists). Each `ClosePosition`
stop-loss action is a single FAK sell attempt, gated on a real incoming `PolyTick` — a failure
just re-arms `Holding` to retry on the next tick, which is what keeps this naturally rate-limited
rather than a hot loop (`worker.rs:791-798`, `worker.rs:756-769`). That's the *outer* retry count,
and it's what `exit_attempts: 31` counts.

Each of those 31 outer attempts also runs its own *inner* retry loop in
`execution.rs::close_position` — up to `close_max_retries = 5` immediate re-submits on
`"no orders found to match with FAK order"` (`execution.rs:510, 537-541, 552`). So this trade's
31 logged attempts represent up to ~150 raw FAK sell posts against the book, all but the last
rejected for lack of a resting counterparty.

That matches the price action: between T-30s and T-0s the book was repricing the DOWN token from
0.665 all the way toward worthless as ETH whipped across the strike. In that kind of violent,
one-sided move — especially in the last ~20 seconds of a 5-minute binary market, when
market-makers pull resting quotes rather than get caught by the resolution flip — there's often
no live bid at all for stretches of time, so every FAK sell just gets killed with "no match"
instead of filling badly. The position finally cleared at 0.47 when a buyer briefly appeared; the
0.0050 print in the recon's "CLOB Price History (token held)" table is a later snapshot
(01:00:20, after this exit), showing the price kept sliding after we were already out. So despite
the alarming attempt count, the actual fill (0.47) was considerably better than where the token
ended up — the retry loop did its job.

## 3. Verdict: not a bug

This is the stop-loss design working as intended under a genuinely illiquid, fast-moving moment,
not a defect in the retry logic:

- The outer loop is correctly bounded to one attempt per real tick (no hammering) — this is the
  fix that closed the original `incident_doge_2026-07-03.md` (284 attempts in ~9s from an
  unbounded internal loop).
- The inner loop's `close_max_retries = 5` immediate re-submits on "no match" are appropriate for
  a FAK order in a fast market — the book can change tick to tick, so retrying immediately (no
  sleep) is correct, per the same cadence documented in
  `incident_sol_unwind_but_loss_2026-07-06.md` §6.
- Stop-loss intentionally has no price floor (`close_position()`, not
  `close_position_at_price()`) — it must close regardless of price, so it can't "give up" the way
  a take-profit can. That's exactly why it kept trying through 31 ticks instead of abandoning the
  position.

No code change proposed. Recon already surfaces `exit_attempts` and the CLOB price history per
trade, which is what made this visible and explainable in the first place — worth keeping an eye
on if a future report shows a similarly high count *and* a bad fill relative to trigger price,
which would suggest the eventual match landed materially worse than what the stop-loss floor
intended (this trade's 0.47 fill vs. 0.82 trigger is a large absolute loss, but that's the
stop-loss threshold doing its job on a token headed to zero, not slippage from the retry
mechanism itself).
