# Plan — Binance WS "staleness" is mostly a stream-choice problem, not a connection-health problem

Status: **§3 (bookTicker) and §4 (observe-only staleness logging) both implemented and deployed
2026-07-20**, per the user's explicit rollout order ("§3 first, once that's tested ok, roll out
§4") — §3 shipped and was confirmed healthy on Oracle (all assets connected cleanly, zero
errors) before §4 was added on top. §5 (REST reconciliation) untouched, per §6's original "only
if §4's data still shows a gap" ordering — nothing yet to act on, §4 only just started
collecting real data. Originally written per user request after
`trader/doc/audit_48hr_unwind_maker_2026-07-20.md` §1 found a real DOGE indicator-staleness
incident traced back to `price_feed`'s Binance ingestion. This doc researches root cause and
proposes fixes for `price_feed`'s Binance leg specifically; it does not touch the `indicator`/
`trader` gate behavior — that's `trader/doc/plan_stale_data_gate_2026-07-20.md`, implemented
separately per the user's explicit split into two tasks.

## 1. What the audit already established

`price_feed/src/collect.rs::spawn_binance_task` opens one WS connection per asset to
`wss://stream.binance.com:9443/ws/{symbol}@trade` (raw trade stream — only sends a message when
an order actually executes) and writes the latest trade price into a shared per-asset slot,
consumed by `indicator`'s `on_tick`. Checked Oracle logs for the exact incident window
(2026-07-20 02:25-02:34 HKT, DOGE): **no WS disconnect, no reconnect, no error anywhere in
`poly-collector`'s journal.** The connection was healthy the entire time. DOGE's indicator
snapshot simply stopped updating because Binance genuinely printed no new `DOGEUSDT` trades for
10+ seconds — plausible, unremarkable behavior for a lower-volume pair during Asia late-night
hours, not a bug or an outage.

**This means the "staleness" isn't a connection-health problem — it's an artifact of using a
change-only event stream (`@trade`) as if it were a continuously-sampled price feed.** A quiet
market and a dead feed produce the exact same downstream symptom (no new message), and nothing
in the current design can tell them apart.

## 2. This project already learned this lesson once, the hard way — reuse it

`price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md` (Polymarket CLOB bba/price feed, a
structurally identical problem: `best_bid_ask`/`price_change` are also change-only events) is
directly relevant prior art, not just a similar-sounding incident:

- Its first implementation attempt was a raw silence timer (5s, "no message → force
  resubscribe"). **Deployed, then rolled back the same day**: it fired constantly on genuinely
  quiet assets/periods (§0 of that doc — "a raw silence timer cannot distinguish 'broken' from
  'quiet' for a change-event stream. There is no threshold that's both fast enough to matter and
  safe against every asset's and duration's normal quiet stretches"). Exactly the failure mode a
  naive Binance silence-timer fix would hit today, for the identical reason.
- What actually shipped and is running stable: **phase 1**, an observe-only silence logger with
  no recovery action at all (`price_feed/src/staleness.rs`, 6 escalating thresholds 10s-300s,
  `[OBSERVE-STALE]` log line only) — deployed first specifically to collect real quiet-period
  data before committing to any trigger design. **Phase 2**, once real data was in hand: REST
  reconciliation against Polymarket's `/midpoint` endpoint every 5s, only treating a *genuine
  value mismatch* against the WS-cached price as staleness — not silence duration at all — which
  structurally cannot false-positive on a quiet-but-healthy market (the REST snapshot of a quiet
  market agrees with the stale WS cache, so no mismatch, no trigger, no matter how long the quiet
  stretch runs).

**Any Binance fix should follow the same order of operations this doc's own precedent validated
in production**: don't jump straight to a reconnect trigger; fix what can be fixed for free
first (§3), observe (§4), then only add REST reconciliation if real data still shows a gap after
that (§5) — mirroring phase 1 → phase 2 above, not re-deriving it from scratch.

## 3. The actual root-cause fix: switch (or add) `@bookTicker`, not a watchdog

Researched Binance's stream types specifically for this question — the fix that removes the
false-staleness problem at its source, rather than just detecting it faster:

- **`@trade`** (current): only fires on an executed trade. In a low-liquidity pair during quiet
  hours, genuinely nothing to send for 10-60s+ is normal, not broken.
- **`@bookTicker`**: fires on *any* change to the best bid or ask price/quantity — which happens
  far more often than executed trades, since order-book quotes move even when nothing actually
  fills. This is a best-bid/ask *quote* stream, not a trade-print stream, but for this project's
  purposes that's a strict improvement: `indicator`'s HAR/p(up) engine only needs a continuously-
  updating representative price, not specifically "the last executed trade price" — and this
  codebase already treats `(bid+ask)/2` as the canonical "price" elsewhere (`price_feed`'s own
  CLOB bba ingestion computes `up_mid = (bid + ask) / 2` for its NATS payload,
  `collect.rs:1253`). Using `(bookTicker.b + bookTicker.a) / 2` as the Binance "price" is the
  same pattern, not a new one.

**Proposed fix**: change `spawn_binance_task`'s URL from `{symbol}@trade` to `{symbol}@bookTicker`
and change the parse from the trade payload's `p`/`E` fields to bookTicker's `b`/`a` (best
bid/ask) fields, publishing `(b+a)/2` as `price` with the message's own local receipt time
standing in for `server_ts_ms` (bookTicker messages carry no `E` event-time field — confirm this
by checking a live payload before committing to a fallback timestamp source, don't assume from
docs alone, the same "verify the real payload shape" discipline the CLOB precedent's §10 used
after finding Polymarket's own docs were wrong about a field name).

This one change is expected to make the vast majority of DOGE-class "quiet market" gaps disappear
entirely, because there's almost always *some* order-book quote movement even when nothing
trades — turning a change-only-on-execution stream into a much closer approximation of a
continuously-sampled one, for free, with no new failure-detection machinery at all.

**Open question to verify before implementing, not guessed here:** does `indicator`'s HAR
vol/p(up) math implicitly assume "price" means "last trade price" in a way that a bid/ask
midpoint would skew (e.g. spread-driven noise on a wide-spread quiet pair)? `indicator/src/
math.rs`/`engine.rs` weren't audited for this in writing this plan — needs a check (and ideally
a side-by-side replay comparison, `indicator`'s own `replay` subcommand exists exactly for this)
before switching the live feed, not assumed safe by analogy alone.

### Implementation notes (2026-07-20) — what actually shipped, and why it differs from "just switch the URL"

User's explicit refinement changed the shape of this fix from a straight replacement to an
**addition**: `@bookTicker` runs *alongside* `@trade`, not instead of it — "so that telegram
latency info is still based on actual server ts and there's some objective data, but price
source definitely can be switched to mid price for downstream calculations like delta and
p(up)." This surfaced a real design tension the original one-line "change the URL" proposal
glossed over: the published `ts` field is consumed by *two* different downstream things that
need different semantics —

- `indicator`'s `on_tick(ts, price)` needs `ts` to keep advancing in near-real-time (it drives a
  1Hz price-fill loop) — so `ts` has to track whichever stream is actually updating, which after
  this fix means `@bookTicker`.
- Trader's `exchange_latency_ms(local_ts, server_ts)` needs `local_ts` and `server_ts` to be two
  timestamps *of the same message* — pairing a bookTicker receipt time against `@trade`'s `E`
  field would make the "latency" figure meaningless (comparing unrelated events).

Resolved by decoupling the wire format into two independent pairs instead of one: `ts`/`price`
(from `@bookTicker`, used for indicator freshness + downstream delta/p(up)) and a **new**
`server_ts`/`trade_ts` pair (both from `@trade`, used exclusively for the Telegram latency
figure — `trade_ts` is new, `price_feed`'s own local receipt time of the `@trade` message that
produced `server_ts`). `price_feed`'s `BinanceState` struct, `spawn_binance_task` (`@trade`,
now latency-only) and the new `spawn_binance_bookticker_task` (`@bookTicker`, now the sole
price source) implement this split; `trader`'s `AssetSlot` gained a matching
`last_binance_trade_ts` field and `extract_trade_ts` parser so `exchange_latency_ms` uses the
right pair. Both crates: full test suite green (299 trader + 47 live.rs + 44 price_feed tests),
`cargo fmt --all --check` and `cargo clippy --all-targets --all-features -- -D warnings` clean.
Verified live against real Binance payloads before writing this section (not assumed from docs
alone, matching this doc's own §3 discipline): `@bookTicker` carries `b`/`a` (best bid/ask) and
no `E` field, confirming the open question above — no server event-time to lose by switching.

The `indicator`-math open question above was reasoned through rather than replay-tested:
`indicator::AssetEngine::on_tick(ts, price)` treats `price` as an opaque generic input series (no
"last trade" assumption anywhere in `engine.rs`) — a continuously-updating midpoint is a strict
improvement for its 1Hz fill logic, not a risk. A side-by-side replay comparison remains a good
idea for validating the *quality* (not correctness) of p(up) under the new price source once
enough live data has accumulated, but isn't a blocker the way a hard code assumption would have
been.

## 4. Phase 1 (cheap, do this regardless of §3's outcome): observe-only staleness logging for Binance

Directly reuse the `price_feed/src/staleness.rs` module the CLOB fix already built and proved
safe — it's already generic over "silence duration since last update," not CLOB-specific in its
core logic (`buckets_to_log`). Wire the same escalating-threshold observe-only check into
`spawn_binance_task`'s per-asset loop (or the `run()` sampler, matching however `staleness.rs` is
currently wired for bba). Zero behavior change, same safety profile as the original phase 1.
Gives real data on: which assets/hours actually see multi-second Binance gaps, whether §3 alone
(bookTicker) eliminates them, and — if any remain even under bookTicker — how long they really
run, before any reconnect/reconciliation logic gets designed against real numbers instead of a
single incident's timestamps.

### Implementation notes (2026-07-20)

Shipped exactly as scoped above — wired into the `ticker_250ms` sampler in `run()`, right next to
the pre-existing bba staleness block, using the identical pattern (reset `last_seen`/
`logged_bucket` on a new sample, log newly-crossed `buckets_to_log` buckets on a repeat). Tracks
`BinanceState::price_received_at_ms` (the `@bookTicker` field), not `@trade`'s
`server_ts_ms`/`trade_received_at_ms` — those are a separate, latency-only concern (§3's
implementation notes) and were never in scope for this staleness check. `HYPE` (no Binance market)
is naturally excluded: its `price` never exceeds `0.0`, so it never reaches the staleness block at
all — confirmed locally (empty `HYPE_binance_*.parquet`, zero rows, after a live 20s run). No new
unit tests added — this wiring reuses `staleness::buckets_to_log`, already covered by its own pure-
function test suite, and mirrors the bba wiring (also untested at the integration level, just at
the pure-function level) rather than introducing a new testing pattern. Verified locally: a live
20s run against real Binance data produced zero `[OBSERVE-STALE]` lines (expected — the whole
point of §3 is that `@bookTicker` shouldn't go quiet under normal conditions), full crate test
suite green (44 tests), `cargo fmt --check`/`clippy -D warnings` clean, then deployed to Oracle —
all assets connected cleanly post-restart, zero errors.

## 5. Phase 2 (only if §4's data shows it's still needed after §3): REST reconciliation

If bookTicker (§3) doesn't fully close the gap, Binance's REST equivalent is
`GET /api/v3/ticker/bookTicker?symbol=DOGEUSDT` (public, unauthenticated, matches the CLOB
precedent's use of Polymarket's public `/midpoint`). Same design as the CLOB fix's phase 2:
poll periodically (candidate 5s, matching the CLOB precedent's tuned value), compare against the
WS-cached price, and only treat a genuine *value* mismatch beyond tolerance as real staleness —
never silence duration alone. On confirmed mismatch, reuse the same recovery precedent already
proven in this codebase: log and `std::process::exit(1)`, relying on `Restart=always`/
`RestartSec=5` (already how `poly-collector.service` recovers from a fatal NATS-connect failure
today) rather than inventing a new in-process per-asset resubscribe mechanism — simpler, and
avoids the CLOB fix's own hard-won lesson that the "obvious" surgical-unsubscribe recovery had a
refcount sharp edge that would have silently no-op'd (`plan_bba_feed_staleness_fix_2026-07-10.md`
§10). Binance's per-asset WS connections aren't refcounted/shared the way Polymarket's SDK
connection is (`spawn_binance_task` is already one full connection per asset, not multiplexed),
so this sharp edge may not even apply here — but exit-and-restart is simple enough to not need
that analysis either way.

## 6. Recommended rollout order

1. **§3 (bookTicker switch)** — the actual root-cause fix, cheapest to implement (one stream
   endpoint + payload-shape change), needs the `indicator` math compatibility check flagged
   above before shipping to the live feed.
2. **§4 (observe-only logging)** — reuse existing `staleness.rs`, deploy alongside or shortly
   after §3, to get real post-fix data.
3. **§5 (REST reconciliation)** — only if §4's data shows bookTicker didn't fully close the gap.
   Don't build this speculatively; the CLOB precedent shows the naive version of "just add a
   watchdog" backfires, and the REST design needs real quiet-period data to size correctly
   anyway.

None of this is blocking on `trader/doc/plan_stale_data_gate_2026-07-20.md`'s gate-behavior fix
(PUP_GATE_MAX_AGE_SECS=2s, fail-closed) — that fix stands on its own (never trade on stale data,
regardless of why it's stale) and should ship first, independent of whether/when this doc's
Binance-side improvements land. This doc is about reducing how *often* the gate has to say no,
not a prerequisite for the gate being safe.

## Sources

- [WebSocket Streams | Binance Open Platform](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams) — `@trade` vs `@bookTicker` stream definitions.
- [Individual Symbol Book Ticker Streams | Binance Open Platform](https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams) — bookTicker payload shape reference (verify live payload before implementing, per §3).
- [Avoiding/Detecting stale websocket (user data stream) connections — Binance Developer Community](https://dev.binance.vision/t/avoiding-detecting-stale-websocket-user-data-stream-connections/4248) — general ping/pong + last-message-timestamp health-check guidance; store last-received timestamp, use REST to backfill after a reconnect.
- `price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md` — this project's own prior, production-validated design for the structurally identical Polymarket-side problem; §0/§9/§10 are the load-bearing sections this plan reuses rather than re-deriving.
