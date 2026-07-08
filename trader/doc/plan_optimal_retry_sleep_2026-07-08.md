# Plan — optimal retry sleep for entry/exit order placement

Status: **§6.1 and #3 implemented, same day.** Entry retries (`execution.rs::place`) now split by
error type — `"no orders found to match"` retries after 10ms (proposed 0-50ms; 10ms chosen per
review), deterministic errors fail immediately with no retry, everything else retries after 250ms
— and `order_max_retries` raised 3 → 5. See README.md's "Latency & observability infrastructure"
section for the implementation writeup. §6.2/§6.3 (exit-side changes) were **not** touched, per
§5's finding that the genuine settlement-lag case shouldn't be shortened.

## 0. Question being answered

Following up on the 2026-07-08 rate-limit research (README.md's "Latency & observability
infrastructure" section): is a 30-100ms retry sleep safe, and how many retries are actually
needed? Short answer: **it depends entirely on which failure mode is being retried** — there are
two structurally different kinds of "failure," and only one of them cares about wall-clock time at
all.

## 1. Current design, exactly as coded today

| Path | Function | Retries | Sleep | Configurable? |
|---|---|---|---|---|
| Entry (BUY, all strategies) | `execution.rs::place` | `order_max_retries` (TOML: `3` → 4 total attempts) | flat `1s`, **every** attempt, **every** error type | retries: yes (TOML); sleep: no (hardcoded literal) |
| Exit — stop-loss/timeout (`ClosePosition`, no `limit_price`) | `execution.rs::close_position` | `close_max_retries` (hardcoded default `5`, **not** in TOML) | `0s` for `"no orders found to match"`, `1s` for `"not enough balance"`, **fails immediately (no retry at all)** for anything else | retries: no; sleep: no |
| Exit — take-profit (`ClosePosition`, `Some(limit_price)`) | `execution.rs::close_position_at_price` | **1 attempt, no internal retry loop at all** (see `incident_sol_unwind_but_loss_2026-07-06.md` — this was deliberately redesigned to wait for the next real `PolyTick` instead of retrying internally) | n/a | n/a |
| Internal GTC resting-sell follow-up (`PlaceLimitSell`, ≥5 shares) | `execution.rs::place_limit_sell` | `settle_retries` (hardcoded default `3`) | `1.5s`, only for `"balance: 0"`; anything else fails immediately, no retry | no (hardcoded) |

Two things worth flagging about this table on its own, before even getting to the sleep-duration
question:
- `close_max_retries`/`settle_retries`/`settle_sleep` are **not** wired to the strategy TOML at
  all — they're Rust-side defaults (`LiveConfig::default()`, `execution.rs:351-360`). Only
  `order_max_retries` reaches the TOML. Changing the exit-side numbers means editing Rust, not
  config, today.
- `place()` (entries) has no error-type branching whatsoever — every failure, transient or
  permanent, gets the same flat 1s sleep and the same retry treatment. `close_position` already
  learned (2026-07-04, `0ad6cd6`) that not all failures are equal; `place()` never got that
  lesson.

## 2. The two failure modes are not the same problem

**Mode A — thin/stale order book (`"no orders found to match with FAK order"`).** The book
genuinely had nothing to match at that instant. This can change on the very next tick — there is
no reason internal to Polymarket's system to wait before retrying. `close_position` already proves
this: it retries this specific error with **zero** sleep, by design, on purpose
(`execution.rs:731-733`'s own comment: *"no reason to wait — the book can change tick to tick"*).

**Mode B — settlement lag (`"not enough balance"`, specifically on a SELL immediately following a
BUY).** This is not a rate-limit or book-liquidity problem at all — it's that the CLOB confirms a
fill instantly over the API, but the actual token balance only becomes spendable once the
corresponding Polygon transaction is mined on-chain. `trader/doc/incident_sol_unwind_but_loss_2026-07-06.md`
§6 already measured and documented this directly: *"typically ~1-2s on Polygon, not a bug"*, with
a real observed instance taking **3.4 seconds across 4 attempts** (1 balance-not-settled attempt +
2 no-match attempts + 1 success) to actually clear. Retrying faster than this doesn't make the
chain confirm faster — it just spends more attempts waiting for the same real-world duration to
elapse.

**These are answered by completely different numbers.** Mode A wants ~0ms. Mode B wants ~1-2s.
Averaging them into one flat 1s (today's design) is a compromise that's too slow for A and
possibly still a little short for B.

## 3. Does the entry side (`place()`) actually have a Mode B problem?

Checked directly against `trader/live_logs/live.log` (411 occurrences of `"not enough balance"`
total):

```
$ grep "\[ORDER\].*BUY.*err=Some(" live_logs/live.log | grep -c "not enough balance"
0
```

**Zero.** Every single "not enough balance" occurrence in the log is on the exit/close side. This
makes sense structurally: Mode B is specifically "I just bought shares and immediately need to
resell them before the chain settles" — a fresh BUY isn't racing its own prior fill, it's spending
USDC that's already sitting funded in the wallet. **Entries only ever have a Mode A problem** (plus
a third, unrelated category below) — there is no genuine reason for `place()`'s retries to ever
need more than a nominal pause.

The 12 BUY-side final failures actually logged break down as:

| Error | Count | Retry-fixable? |
|---|---|---|
| `"no orders found to match with FAK order"` | 8 | Yes — Mode A, book can change tick to tick |
| `"invalid amounts, ... max accuracy of 2 decimals ..."` | 2 | **No** — deterministic, same input produces the same rejection every time |
| `"invalid amount for a marketable BUY order ($0.9975), min size: 1"` | 1 | **No** — deterministic |

**A third category exists: deterministic/structural errors that no amount of retrying or sleeping
can ever fix.** `place()` currently retries these exactly like Mode A — burning the full 1s sleep
up to 3 times (3 seconds) before giving up on something that was never going to succeed. This is
the same class of defect as the 2026-07-03 DOGE oversell incident (`incident_doge_2026-07-03.md`)
— a guaranteed-permanent rejection retried anyway. One observed instance shows the real cost:
`n_attempts=4 process_ms=4303` — 4.3 seconds spent finding out an order was never going to fill,
inside a 5-minute-cycle strategy whose `high_prob` variant only has a ~10-20s entry window to begin
with.

## 4. Real-world attempt distribution for successful entries

From the same log (`n_attempts` field, where present):

| Attempts needed | Count |
|---|---|
| 1 (no retry needed) | 25 |
| 2 (one retry) | 7 |
| 3 (two retries) | 1 |

~24% of successful entries in this sample needed at least one retry — meaning they paid at least
one full 1-second sleep for a fill that (per §3) almost certainly didn't need it, since entries
don't have a Mode B case. This is direct, real evidence that the flat 1s sleep is costing real
trades real seconds inside an already-narrow entry window, not just a theoretical concern.

## 5. Is 30-100ms actually "safe"?

**Two different senses of "safe" — split them:**

- **Rate-limit safety**: yes, trivially, for any of these paths. Per README's 2026-07-08 rate-limit
  research, Polymarket's `POST /order` allows ~500/s burst; our worst case is a handful of requests
  per cycle per asset. Nothing here is remotely close to that ceiling regardless of sleep duration.
- **Correctness/usefulness safety**: **only for Mode A.** For Mode B (`"not enough balance"`),
  30-100ms is almost certainly *too short to be useful* — real settlement takes on the order of
  1-2 seconds (§2), so a 100ms retry just means needing ~10-20x more attempts to cover the same
  real wait, and risks exhausting `close_max_retries` (currently 5) *before* genuine settlement
  completes — which would turn today's "usually resolves within a couple of retries" into "now
  fails outright more often." Shortening Mode B's sleep without also raising the retry budget
  would likely make stop-loss reliability *worse*, not faster.

So: **30-100ms is safe and beneficial for Mode A and for the deterministic-error case (where the
right answer is actually 0 retries, not a shorter sleep). It is not a safe substitute for Mode B's
~1-2s.**

## 6. Proposed changes (for review — none implemented)

1. **Entries (`place()`): split by error type, mirroring `close_position`'s existing pattern.**
   - `"no orders found to match"` → retry near-immediately (0ms, or a nominal ~20-50ms just to
     yield the async runtime rather than a true busy-spin — cosmetic choice, not a safety one).
   - Recognized deterministic errors (`"invalid amounts, ... decimals"`, `"invalid amount for a
     marketable BUY order"`, `"min size"`) → **fail immediately, 0 retries.** No sleep can ever
     help these; retrying is pure wasted time (up to 3s wasted today, per §3's `process_ms=4303`
     example).
   - Anything else (unrecognized/unexpected errors — genuine network blips, unexpected API
     responses) → keep a moderate backoff, e.g. 250-500ms rather than 1000ms. This is the one
     bucket without hard evidence either way, so it's the one place a conservative-but-shorter
     number is proposed rather than going straight to 0.
2. **Exit — stop-loss (`close_position`): leave `"no orders found to match"` exactly as-is**
   (already 0s, already correct). **Do not shorten `"not enough balance"` below its current 1s** —
   if anything, consider raising it toward `place_limit_sell`'s already-established `1.5s`
   (`settle_sleep`) for consistency, since both are the identical Mode B wait. No strong evidence
   either 1s or 1.5s is meaningfully better than the other given only one real timing data point
   exists (§2) — flagging as low-priority, not urgent.
3. **`order_max_retries`: consider raising** (e.g. 3 → 5, so 6 total attempts) now that Mode A
   retries would cost ~0ms each instead of ~1s each — more attempts become nearly free time-wise,
   which directly increases fill probability inside `high_prob`'s narrow ~10-20s entry window
   without materially increasing wall-clock cost. Not urgent on its own; worth doing alongside #1
   since the two changes compound (more attempts, each one cheap).

## 7. What would make this stronger before committing to exact numbers

This plan leans on:
- One official rate-limit page (§ README, already verified directly) — solid.
- One incident doc's qualitative claim ("~1-2s on Polygon") plus **one** concrete measured instance
  (3.4s / 4 attempts) for Mode B's real-world duration — thin. A single data point is enough to
  rule out 30-100ms, but not enough to confidently pick between, say, 1s vs 1.5s vs 2s as the
  *optimal* Mode B sleep.
- Production log grep for the Mode A/B split and the attempt-count distribution — solid, real
  data, but from whatever window `live.log` currently covers, not a controlled experiment.

**Recommended before shipping the exact numbers in §6**, not before understanding the direction:
add a log line specifically timestamping "first Mode B failure" → "eventual success/exhaustion" so
future occurrences build a real distribution instead of relying on n=1, and consider validating the
entry-side split (§6.1) in a shadow/paper-trading pass first (per `trader/src/bin/shadow.rs`, which
already exists for exactly this kind of no-capital-at-risk validation) before it ever touches the
live `ETH` config.
