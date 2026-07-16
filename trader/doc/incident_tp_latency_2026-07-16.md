# Incident — BTC take-profit close took 2891ms despite `n_attempts=1`, 2026-07-16

## 1. What happened

Telegram alert:

```
📤 BTC TAKE PROFIT order executed | 10:04:48 | reversal
sold=1.3600 @ 0.9900 = $1.3464 | clob_latency=7ms | process_latency=2891ms | n_attempts=1
```

`n_attempts=1` and `clob_latency=7ms` both read as "fast, clean fill" — but the position took
**2891ms** end-to-end to close, and the reported "1 attempt" is misleading: this was actually the
**56th** close attempt for this position, the first 55 of which failed. `n_attempts` only counts
retries *within a single `close_position_at_price()` call* (which by design makes exactly one
try, see §3), not the worker-level re-fires across ticks that actually happened here.

`clob_latency=7ms` is also not what it looks like — that field is the Poly **price-feed**
propagation latency (tick timestamp → Oracle receipt), not order round-trip time. It was fast
the whole time; it was never the bottleneck.

## 2. Timeline (from `live.log` on Oracle, `10.8.0.1:/home/ubuntu/apps/poly_rust/trader/live_logs/live.log:178806-178866`)

```
[unwind] fill event recv_ts=1784167484.544 ... side=Buy price=0.73 size=1.369862 matchtime=1784167484
[ORDER] BTC BUY Down @ 0.5600 size=$1.00 -> placed=true shares=1.3699 cost=0.7300 ... process_ms=466 n_attempts=1
[telegram] sent: 📋 BTC Order placed | 10:04:44 | T-15s | DOWN ↓ | reversal

[ORDER] BTC CLOSE 1.3699 (TakeProfit) -> status=Failed ... err="not enough balance / allowance:
  the balance is not enough -> balance: 0, order amount: 1360000" (process_ms=99  n_attempts=1)
[ORDER] BTC CLOSE 1.3699 (TakeProfit) -> status=Failed ... same error               (process_ms=143 n_attempts=1)
[ORDER] BTC CLOSE 1.3699 (TakeProfit) -> status=Failed ... same error               (process_ms=191 n_attempts=1)
... 52 more, same error, process_ms climbing in ~40-50ms steps ...
[ORDER] BTC CLOSE 1.3699 (TakeProfit) -> status=Failed ... same error               (process_ms=2424 n_attempts=1)
[ORDER] BTC CLOSE 1.3699 (TakeProfit) -> status=Matched sold=1.3600 usdc=1.3464     (process_ms=2891 n_attempts=1)
```

55 consecutive `[ORDER] BTC CLOSE 1.3699 (TakeProfit) -> status=Failed` lines, all with the
identical error `not enough balance / allowance: the balance is not enough -> balance: 0, order
amount: 1360000` (1360000 = exactly the 1.3699 shares just bought, in base units), then the 56th
succeeds. CSV confirms: `live_trades_btc_reversal.csv`'s row for this trade logs
`exit_attempts=54, exit_last_error="...balance: 0...", exit_signal_latency_ms=2425.77,
exit_process_latency_ms=2891.29`.

Entry BUY filled at **10:04:44** (`matchtime=1784167484`). The take-profit condition
(`exit_price >= tp_price`) was already true on the very next `PolyTick` — price gapped straight
to 0.99 with only T-15s left in the 5-minute cycle — so the close attempts started firing
essentially immediately (first attempt at `process_ms=99`) and hammered for **~2.8 seconds**
before the position finally cleared.

## 3. Root cause — two compounding issues

**(a) Balance/allowance settlement lag on the entry fill.** The BUY order response came back
`placed=true` and the fill-event WS confirmed the trade, but Polymarket's CLOB balance ledger
for the just-bought shares wasn't yet spendable — the same `"balance: 0"` race documented in
`incident_sol_unwind_but_loss_2026-07-06.md`, where a sell attempted within ~1-2s of the
matching buy can hit this before the buy has settled. That doc's §9 caveat called this exact
shape out by name:

> "if a take-profit fires within ~1-2s of entry (this incident's exact shape) and hits
> `"balance: 0"` on its one attempt, it now fails immediately and recovery depends on the next
> real `PolyTick` arriving with the price still qualifying, not a guaranteed 1-second internal
> wait."

That's exactly what happened today — this isn't a new failure mode, it's the predicted one.

**(b) No backoff on that retry path, so "wait for the next tick" became "retry as fast as ticks
arrive."** `close_position_at_price()` (`trader/src/execution.rs:867-948`, the take-profit exit
path) deliberately makes **one attempt with no internal retry loop** — a 2026-07-06 redesign
(see §2/§9 of that incident doc) that replaced `close_position()`'s old unbounded internal retry
with "wait for the next real `PolyTick`" specifically to bound the *price* a late fill could
land at. `close_position()` (used for stop-loss/timeout) *does* have special handling for this
error — it sleeps 1s before retrying on `"not enough balance"` specifically
(`plan_optimal_retry_sleep_2026-07-08.md`'s table) — but that 1s cooldown was never carried over
to the take-profit path, because the 07-06 redesign's retry cadence is "the next market tick,"
not a fixed sleep.

Normally "the next market tick" is a fine, slow-enough cadence. Here it wasn't: with price
already past `tp_price` and staying there, *every* subsequent `PolyTick` re-armed
`ClosePosition{TakeProfit}` (`worker.rs:987-1001`, `on_unwind_failed` re-arms
`ExitArm::PriceMonitor` on failure per the 07-06 design), and each attempt is a blocking
`POST /order` round trip that itself takes ~40-50ms to be rejected. Since new ticks kept
arriving as fast or faster than each rejected round trip completed, the single-threaded
per-market driver never caught up — it fell further behind the live tick stream on every
iteration. That backlog is exactly what `exit_signal_latency_ms=2425.77` measures: by attempt
56, the triggering tick was already 2.4s old by the time it got processed. The extra ~466ms
between `exit_signal_latency_ms` (2425ms) and `exit_process_latency_ms` (2891ms) is the actual
successful order's own round trip.

In short: **55 rejected sell attempts in ~2.8 seconds, one per incoming tick, with no cooldown
between them**, driven by a balance-settlement race that a fixed sleep (like `close_position()`
already has) would have absorbed in one or two tries instead of ~55.

## 4. Why this wasn't caught by `n_attempts`/`clob_latency` in the alert

- `n_attempts` is `CloseResult.attempts`, scoped to a single `close_position_at_price()` call,
  which is always `1` by design (§3) — it was never meant to count cross-tick re-fires, but reads
  very differently to a human than "55 rejected attempts before this one."
- `clob_latency` is WS feed propagation latency only (`exchange_latency_ms(last_poly_ts,
  last_poly_server_ts)`, `trader/src/bin/live.rs:1003-1005`) and has no relationship to order
  round-trip time or queueing delay — it looked fine the entire time because it was measuring
  the wrong thing.
- `process_latency` (`signal_ts` → `confirmed_ts` for the *final* successful attempt only) is the
  one number that did reflect the real delay, but nothing in the alert distinguishes "one slow
  order" from "one fast order at the end of a 55-attempt storm" — both would print an identical
  line.

## 5. Impact

Financially minor this time — PnL still landed positive (`pnl=0.3338`, filled at the intended
0.99 take-profit price, since `close_position_at_price` is bounded at `tp_price` by design). The
position was never at risk of a bad fill; the cost here was purely the ~2.8s of open-position
time and 55x the order-submission load on Polymarket's API for one trade. A less favorable price
trajectory during that 2.8s window (price dropping back below `tp_price` before attempt 56 could
land) would have left the position open past the intended exit and re-armed for a *worse* exit
condition (stop-loss/timeout) instead — that didn't happen here, but it's the realistic bad
outcome of this pattern.

## 6. Fix (implemented 2026-07-16)

`close_position_at_price()` (`trader/src/execution.rs`) now special-cases `"not enough balance"`
specifically: it retries internally at a fixed `tp_settle_sleep` cadence (100ms, `LiveConfig`
default) for up to `tp_settle_retries` extra attempts (30, i.e. a 3.0s ceiling — comfortably above
this incident's observed ~2.8s resolution time), and returns the real attempt count in
`CloseResult.attempts`. Every other failure (thin book, etc.) is untouched — still single-attempt,
still deferring to the next real `PolyTick`, exactly as the 2026-07-06 redesign intended. The
retry decision itself is factored into a pure `tp_close_retry_decision(msg, attempt, max_attempts)`
function so it's unit-testable without a live CLOB connection (`LiveExecutionEngine` talks to the
real SDK client directly and isn't mockable).

This fixes both original complaints in one change: the settlement-lag race now resolves inside a
single call at a controlled 100ms cadence instead of hammering the API once per incoming tick, and
`n_attempts` in the Telegram alert now reports how many attempts that call actually made (so a
future version of this incident would read e.g. `n_attempts=23`, not a misleading `n_attempts=1`).
Cross-*tick* re-fires (if the internal budget is ever exhausted) still aren't summed into
`n_attempts` — that's an accepted residual gap, expected to be rare now that the common case
resolves within the internal retry loop.

New tests: `tp_retry_decision_*` (execution.rs, pure classification logic) and
`auto_reset_line_*` (bin/live.rs — unrelated `/status` addition done in the same pass, see
README's "Latency & observability infrastructure" section). Full suite (207 lib + 34 `live.rs` bin
tests) green, `cargo clippy --all-targets --all-features -- -D warnings` and
`cargo fmt --all --check` both clean.
