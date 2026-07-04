# Incident — DOGE take-profit never filled, 17:33-17:35 HKT 2026-07-03

Investigating why the DOGE `reversal` position entered at 17:33:24 never exited via
take-profit despite price crossing the TP threshold with time to spare — it instead
rode to resolution (won by luck; the market happened to settle at 1.0).

## 1. The trade

`trader/live_logs/live_trades_doge_reversal.csv` (note: this file's header row is
stale — `logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl`,
missing the `exit_attempts,exit_last_error` columns the current binary actually writes
per row, since `append_csv_header_if_new` only writes a header when the file doesn't
exist yet and never migrates an existing one — cosmetic, but confusing when reading the
raw file):

```
TradeRecord {
  slug: "doge-updown-5m-1783071000", strategy: "reversal", side: Up,
  entry_ts: 1783071204.75 (17:33:24), token_price: 0.6600, exit_price: 1.0,
  outcome: Win, pnl: 0.5152,
  exit_attempts: 284,
  exit_last_error: Some("Validation: invalid: No opposing orders for
    70551202937976752410416575775214081279240973858490738301028299902227405983729
    which means there is no market price")
}
```

Entered 1.5151 shares of "Up" at cost 0.6600 (`unwind_pnl_rev = 0.03` → take-profit
target `tp_price = 0.69`). Price kept climbing (heartbeats: 0.55 → 0.70 → 0.99 through
the rest of the cycle), so the TP condition (`exit_price >= tp_price`) was satisfied
almost immediately and stayed satisfied for the rest of the cycle — plenty of
opportunity to exit. **284 close attempts were made and every single one failed.**

## 2. Root cause — a real oversell bug, not a liquidity problem

The very first close attempt, seconds after entry (`live.log:736-746`), failed with:

```
"error":"not enough balance / allowance: the balance is not enough -> balance: 1515150, order amount: 1520000"
```

Those numbers are exact and reproducible: `execution.rs::close_position` builds the
market SELL size as `round2(shares)` (`execution.rs:388`). `round2(1.5151)` rounds
**up** to `1.52` (`(1.5151*100).round()/100 = 152/100`) — but the actual held balance
is `1.515150` shares. **The close order asked to sell 1.52 shares of a position that
only holds 1.5151** — a guaranteed, permanent rejection. `1515150` / `1520000` in the
error message are exactly `1.515150`/`1.520000` in the exchange's 6-decimal fixed-point
units — a dead-on match confirming this is the cause, not a coincidence.

This is a **structural, deterministic bug**: it doesn't matter how many times you
retry, or whether the order book has liquidity — a sell request for more shares than
are actually held can never succeed. Every one of the 284 attempts against this
position was doomed from the first attempt, regardless of market conditions. This is a
sibling bug to the README's already-fixed "Stop-loss close never filled (2026-07-02)"
incident — same failure class (an order sized larger than the true holding), different
mechanism (that one was a wrong `Amount` type — USDC instead of shares; this one is the
correct type with an over-rounded size).

**Same bug exists in `place_limit_sell`** (`execution.rs:329`, `let shares =
round2(shares);`) — the resting-GTC path used for fills >= 5 shares. Untested by this
incident (this fill was only 1.5151 shares, under the GTC threshold, so it went through
`close_position` instead), but it has the identical rounding call on the identical kind
of value and needs the identical fix.

**Why the error message changed near the end** (`live.log:1097`, last attempt): once
the cycle got within its last few seconds, the order book itself emptied out ahead of
resolution — the same thin/vanishing end-of-cycle liquidity documented in
`trader/doc/audit_retry_doge_2026-07-03.md` for the BUY side. At that point the
exchange's validation apparently trips on "no market price" before it even gets to the
balance check. This is a real, separate, and expected end-of-cycle condition — but it's
not what blocked the first ~280 attempts; those failed purely on the oversell.

## 3. Compounding bug — no backoff on the take-profit retry loop

Independent of the rounding bug, `worker.rs::on_poly` (`:483-490`) re-fires a brand-new
`Action::ClosePosition { reason: TakeProfit }` on **every single `PolyTick`** for which
`exit_price >= tp_price` and the position is still in `ExitArm::PriceMonitor`. On
failure, `on_unwind_failed` (`:586-595`) just reverts state back to `Holding` with the
same `exit_arm`/`tp_price` untouched — so the very next tick immediately re-fires. There
is no cooldown and no cap consulting `exit_attempts` (it's incremented and stored, but
nothing ever reads it to decide "stop trying"). 284 attempts landed inside roughly a
9-10 second window — call it 1 attempt every ~35ms, i.e. every poly tick — which is a
retry storm regardless of whether the underlying error is fixable. `close_position`'s
own internal 5-retry/1s-backoff loop (`execution.rs:393-425`) never even engaged here,
because `"not enough balance"` **is** in its `retryable` string list but the *outer*
per-tick loop in `worker.rs` re-invokes the whole function from scratch on every tick
rather than relying on that inner loop — so the 1s backoff only applies within a single
`close_position` call, not across the repeated outer calls.

This matters even after the rounding bug above is fixed: any future *genuinely*
transient close failure (e.g. real thin liquidity, a rate-limit, a momentary API
hiccup) would still get hammered at up-to-tick-rate with no backoff, which risks
tripping exchange rate limits and burns the exit window doing nothing productive.

## 4. Fix (implemented)

**A. Fix the oversell — round SELL sizes down, never up** (`execution.rs`): added
`floor2(x) = (x * 100.0).floor() / 100.0` and replaced `round2(shares)` with
`floor2(shares)` at both SELL-size call sites — `place_limit_sell` (was `:329`) and
`close_position` (was `:388`). Confirmed against the reference implementation: the
official `py_clob_client_v2` (vendored at
`btc_5mins/venv/.../py_clob_client_v2/order_builder/builder.py:102`) quantizes market/limit
order sizes with `round_down` (`helpers.py`: `floor(x * 10**n) / 10**n`) — i.e. this
Rust codebase's `round2`-then-format approach was diverging from the very SDK it ports
from. The Rust `polymarket_client_sdk_v2::Amount::shares` doesn't quantize at all — it
only validates `scale() <= LOT_SIZE_SCALE` and errors otherwise — so the caller was
always responsible for pre-truncating, and wasn't. Leaves at most $0.005/share of
unsellable dust per position (immaterial at `$1` trade size). Tests: `floor2_never_exceeds_input`
(reproduces `floor2(1.5151) == 1.51`, plus a spread-check that `floor2(shares) <= shares`)
and `floor2_exact_two_decimals_unchanged`.

**B. Stop the retry storm — one-shot take-profit latch** (`worker.rs`): checked
`bot/worker.py` (the Python reference this ports from) and found it does **not** just
add a longer sleep — its `_on_poly_snap` zeroes `_rev_unwind_tp_price` the moment a
take-profit condition fires, *before* the sell even completes, so a failed sell is never
retried on a later tick; `_close_position` gets exactly one call (with its own bounded
5-retry/1s-backoff loop) and if that ultimately fails, the position is simply left to
resolve at cycle end. The Rust code didn't have this latch: `on_unwind_failed` reverted
`WorkerState::Unwinding` back to `Holding` with `exit_arm` unchanged
(`PriceMonitor { tp_price }`), so the next `PolyTick` (arriving ~30/s) immediately
re-armed the same trigger. Added a new `ExitArm::TakeProfitAbandoned` variant; on a
failed unwind, `on_unwind_failed` now sets `exit_arm` to that instead of leaving
`PriceMonitor` armed, so `on_poly`'s take-profit branch (which only matches
`ExitArm::PriceMonitor`) never re-fires for that position again. Stop-loss is
unaffected — its check in `on_poly` doesn't gate on `exit_arm` at all, so it stays fully
armed. Test: `failed_unwind_does_not_retrigger_close_on_next_poly_tick` (fires the
take-profit, fails it, drives three more ticks with price still above `tp_price` and
asserts no `ClosePosition{TakeProfit}` is re-emitted, then confirms stop-loss still
fires independently).

Also aligned `close_position`'s own internal retry cadence with
`bot/trading.py::_close_position` while in there: it sleeps 1s only on "not enough
balance" (genuine on-chain settlement lag), and retries "no orders found to match with
FAK order" immediately with no sleep (the book can change tick to tick, no reason to
wait). The Rust code previously slept 1s uniformly for both. Log prefix renamed
`[SL close]` → `[close]` since this path is shared by stop-loss and take-profit
(README updated to match).

**C. Fixed the CSV header + `trade_reconcile.py`:**
- `live.rs::append_csv_header_if_new` now detects a stale pre-`exit_attempts`/
  `exit_last_error` header (9 columns) on an existing file and heals it in place: rewrites
  the header to the current 11-column schema and pads any legacy 9-field data rows with
  two trailing empty fields so every row's column count matches. Runs once per
  (asset, strategy) log file at startup, before any concurrent appends begin for that
  session, so it's safe to do a full read-and-rewrite. Tests:
  `writes_header_for_new_file`, `leaves_current_header_untouched`,
  `heals_stale_header_and_pads_legacy_rows` (reproduces the exact mixed old/new-row file
  found in `live_trades_eth_high_prob.csv`).
- This actually mattered, not just cosmetically: `trade_reconcile.py` reads these files
  with `csv.DictReader`, which — given a header with fewer columns than a data row —
  silently dumps the extra fields into an unnamed `row[None]` bucket instead of erroring.
  `row.get("exit_attempts")` was therefore always `None` → `int(... or 0)` → `0` for
  every row in every stale-header file, meaning the "Failed Exit Attempts" report section
  (added alongside the `exit_attempts`/`exit_last_error` fields themselves) has been
  silently reporting zero retries for every single trade since it was introduced,
  regardless of what actually happened. Fixed the stale docstring (still showed the old
  9-column schema) and added a loud `stderr` warning in `load_and_filter` if a
  `row[None]` mismatch is ever seen again, so future schema drift surfaces immediately
  instead of silently zeroing data.

Not yet deployed to Oracle as of writing — the next restart of `trader-live.service`
will also trigger the CSV header self-heal for all four live CSV files as a side effect.

Not changed: the take-profit *trigger* logic itself (entry timing, `unwind_pnl`
threshold) — this incident's failure was purely mechanical (couldn't construct a valid
sell order, then retried it uncontrolled), not a signal-quality problem.
