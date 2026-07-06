# Incident — `clob_latency` missing from some "Order placed" Telegram messages, 2026-07-06

Telegram alert (SOL, reversal):

```
📋 SOL Order placed | 22:59:22 | T-37s | DOWN ↓ | reversal
price=0.8800 | delta=-0.062% | binance_latency=167ms | process_latency=408ms | n_attempts=1
```

No `clob_latency` field at all — not `clob_latency=n/a`, just absent from the string.

## 1. This is by design, not a bug — but the design only reports half the picture

`bin/live.rs::execute()`, `Action::PlaceBuy` arm (`live.rs:451-455`):

```rust
let (exchange_label, exchange_latency_ms) = match feed {
    Feed::Clob => ("clob_latency", slot.last_poly_server_ts.map(|s| (received_ts - s) * 1000.0)),
    Feed::Binance => ("binance_latency", slot.last_binance_server_ts.map(|s| (received_ts - s) * 1000.0)),
};
let exchange_latency_str = format!("{exchange_label}={}", fmt_latency(exchange_latency_ms));
```

Only **one** of `clob_latency`/`binance_latency` is ever computed, and the message
(`live.rs:464`) interpolates that single `exchange_latency_str` — the other feed's latency
is never touched, so it's not printed as `n/a` (that only happens when the chosen feed's
`Option` itself is `None`, per `fmt_latency`, `live.rs:252-257`); it's simply never part of
the string.

Which one gets picked is decided entirely by `feed: Feed`, an argument threaded in from the
event loop's `tokio::select!` arms, not from anything about the trade itself:

```rust
// live.rs:945-946
let actions = slot.worker.step(Event::BinanceTick(tick));
driver.process_actions(slot, actions, Feed::Binance).await;
...
// live.rs:957-958
let actions = slot.worker.step(Event::PolyTick(tick));
driver.process_actions(slot, actions, Feed::Clob).await;
```

`Worker::try_enter` (`worker.rs:556-579`) is called from **both** `on_binance` and `on_poly`
— an entry is armed once its price/delta thresholds are satisfied, and fires on whichever
tick *happens to arrive last* and completes the condition (see the doc comment at
`worker.rs:549-555`: "gating this exclusively behind BinanceTick... meant a poly price that
crossed its trigger band between Binance ticks sat unnoticed"). So `feed` in this code path
just means "which tick was the last one to arrive before the fire," not "which exchange this
trade is more relevant to." The SOL example above fired off a `BinanceTick` (delta_pct was
the last piece to resolve), so only `binance_latency` was computed — `clob_latency` was never
touched. Had the exact same trigger instead resolved off a `PolyTick` a moment later, the
message would show `clob_latency` and omit `binance_latency` instead. It's coin-flip-ish
per trade, not something wrong with SOL specifically.

## 2. Why this matters beyond cosmetics

The entry order itself is always placed **against Polymarket's CLOB** — `price` in the
message is a CLOB token price, and `gates.rs::check_gates`'s `max_price_age_secs` gate
(`gates.rs:63`) already treats *poly* tick staleness as something worth rejecting an entry
over. So `clob_latency` (how stale the last CLOB tick was at the moment the order was placed)
is arguably the more consistently relevant number for every entry, regardless of which tick
technically fired the trigger — but it's exactly the one that goes missing whenever a
Binance tick happens to be the last piece to resolve, which per `worker.rs`'s comment is a
routine, expected occurrence (not a bug in itself), especially for `reversal`'s delta_pct
gating.

Contrast with the exit side (`live.rs:530-536`): `ClosePosition` is only ever produced by
`on_poly` (`worker.rs:592`, `:602`), so the code there doesn't need a `match` at all — it
unconditionally computes `clob_latency_ms` and both exit Telegram messages
(`STOP LOSS`/`TAKE PROFIT ... executed`) always show it. The entry side is the only place
this either/or gap exists, because it's the only action that can legitimately fire from
either feed.

## 3. Proposed fixes (not yet implemented — for review)

**Option A (recommended) — compute and show both latencies on every "Order placed" message.**
Drop the `match` on `feed` entirely in the `PlaceBuy` arm; unconditionally compute both
`clob_latency_ms` (`slot.last_poly_server_ts`) and `binance_latency_ms`
(`slot.last_binance_server_ts`), same as the exit side already does for `clob_latency`.
Keep `feed` only to label which one was the trigger, e.g.:

```
price=0.8800 | delta=-0.062% | clob_latency=142ms | binance_latency=167ms (trigger) | process_latency=408ms | n_attempts=1
```

Smallest change, and it makes entry messages symmetric with exit messages (which already
show `clob_latency` unconditionally). Also extend the two new fields onto `TradeRecord`/CSV
the same way `entry_signal_latency_ms`/`entry_process_latency_ms` were added in
`incident_sol_unwind_but_loss_2026-07-06.md` §7 point 5, so `trade_reconcile.py` can surface
both without re-deriving them from logs.

**Option B — keep single-field messages, but rename to make the omission legible.** Instead
of a label that silently vanishes, always print both keys with `n/a` for the untriggered one
(reuses `fmt_latency`'s existing `None → "n/a"` path, just needs the untriggered side's
`Option` to reach the format call as `None` rather than not being computed at all). Cheaper
to reason about at a glance in Telegram, but strictly less informative than Option A since
the untriggered feed's real staleness is discarded rather than shown.

**Option C — no code change, documentation only.** Add a one-line note to
`trader/README.md`'s Telegram-notifications section explaining that entry messages show only
the triggering feed's latency by design, so on-call doesn't re-raise this as a bug each time
a `binance_latency`-only (or `clob_latency`-only) message shows up. Cheapest, but leaves the
actual diagnostic gap (CLOB staleness invisible on Binance-triggered entries) unaddressed.

Recommendation: **Option A** — the console `println!` at `live.rs:456` has the same gap and
should get the same fix for consistency (currently logs only the single `exchange_latency_str`
too).

## 4. Verification

No code changes made in this pass — this is a documentation/analysis-only doc per request.
