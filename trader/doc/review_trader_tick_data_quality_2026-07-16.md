# Review: `trader`'s live tick-data pipeline — design and data-quality risk

Prompted by `siglab/doc/incident_signal_2026-07-16.md`'s open follow-up: one unexplained ~4.5¢
entry-price gap that couldn't be conclusively attributed to a real sub-200ms market move vs. a
possible artifact of how `trader::marketdata` assembles its poly ticks. This is a design review
of that pipeline and its neighbors, not a fix — no code is changed here.

**Scope:** `trader/src/marketdata.rs` (`spawn_poly_task`, `spawn_binance_task`, `PolySub`),
`trader/src/types.rs` (`PolyTick`, `BinanceTick`), `trader/src/signal/*.rs` (`LatestPolySignal`,
`SpreadSignal`, `DeltaPctSignal`), `trader/src/gates.rs` (`check_gates`). Consumers
(`trader::machine::Machine`, `siglab::v_shape::VShapeEngine`) are only referenced where they
reveal something about how a data-quality gap actually reaches a trading decision.

**Important scope correction (added after review, following user pushback — see below):**
`trader::marketdata::spawn_poly_task` is only used by the **direct-WS** path — `siglab`,
`backtest.rs`/`shadow.rs`, and `bin/live.rs`'s fallback mode when `--nats-url` is *not* passed.
The actual **production** live-trading path (`bin/live.rs` with `--nats-url`, which is what
`docker-compose.yml` always runs) gets its ticks from `price_feed`'s independently-built
collector via NATS instead, which has its own, separately-implemented ingestion
(`price_feed/src/collect.rs::spawn_bba_task`) — not the code reviewed in §1/§3 below. Findings
in this document apply as stated to the direct-WS path; where they also apply to
`price_feed::collect.rs` (checked afterward), that's called out explicitly.

## 1. `spawn_poly_task`'s stream merge has no atomicity or ordering guarantee

**Why there are two streams for one mid-price in the first place.** They're not two independent
data sources — they're two client-side-filtered views of the *same single* underlying WS
connection. Confirmed in the SDK source
(`polymarket_client_sdk_v2-0.6.0/src/clob/ws/client.rs`): both `subscribe_best_bid_ask`
(`client.rs:298-306`) and `subscribe_prices` (`client.rs:212-217`) call
`get_or_create_channel(ChannelType::Market)`, and `ChannelType` (`subscription.rs:67-72`) has
only two variants (`Market`, `User`) — so every market-data subscription kind (`Book`,
`LastTradePrice`, `PriceChange`, `BestBidAsk`, `TickSizeChange`) shares **one** real connection,
fanned out to each subscriber's own filtered `Stream` (exactly the `ConnectionManager`
one-broadcast-channel-per-connection pattern `siglab/README.md`'s 2026-07-13 CPU incident already
documented for a different symptom). The server emits `BestBidAsk` (a direct top-of-book push
whenever it changes) and `PriceChange` (an order-book delta stream that also happens to carry a
"best bid/ask after this change" convenience snapshot) as two distinct message types over that
one connection; `spawn_poly_task` subscribes to both, presumably to catch a top-of-book change
from whichever message type reports it, and merges them locally.

That framing matters for the risk below: the server's own delivery is a single, in-order TCP
stream, so a well-defined "true order" exists. It's the *client* that splits that single stream
into two independently-filtered receivers and then reassembles them with no ordering memory —
discarding a guarantee that existed at the source, not compensating for one that was never there.

```rust
// marketdata.rs:172-198
let bba_u = bba.filter_map(...);   // subscribe_best_bid_ask
let pc_u = pc.flat_map(...);       // subscribe_prices (price_change events)
let mut merged = futures::stream::select(Box::pin(bba_u), Box::pin(pc_u));
while let Some((bid, ask)) = merged.next().await {
    ...
    let up = (bid + ask) / 2.0;
```

Two client-side-filtered views of one shared connection are merged with `futures::stream::select`,
which yields whichever stream is ready first — it has no notion of the two streams' original
relative ordering on the wire, only local poll readiness. Two distinct risks fall out of this:

- **Cross-stream reordering.** If `best_bid_ask` and `price_changes` both eventually report the
  same underlying book change, nothing guarantees the merged stream yields them in the order the
  server actually emitted them. A later, already-superseded message from one channel can be
  processed after a newer message from the other channel just because of scheduling luck.
- **Confirmed, not speculated: the SDK provides exactly the timestamps needed to prevent this,
  and `spawn_poly_task` throws them away.** Checked
  `polymarket_client_sdk_v2-0.6.0/src/clob/ws/types/response.rs` directly:
  `BestBidAsk` carries its own `pub timestamp: i64` (Unix ms, `response.rs:194-196`), and the
  `PriceChange` wrapper around each `price_changes` batch carries `pub timestamp: i64`
  (`response.rs:108-109`, one timestamp per batch, shared by every `PriceChangeBatchEntry` in
  it — not per-entry). `spawn_poly_task` never reads either field
  (`marketdata.rs:172-191` only pulls `best_bid`/`best_ask`/`asset_id` off each message) and
  stamps every tick with local `now_secs_f64()` instead (`marketdata.rs:199-204`). The two
  streams' relative ordering could be reconstructed from these — right now it isn't.
- **Correction to an earlier hedge:** `PriceChangeBatchEntry.best_bid`/`best_ask` are documented
  in the SDK as "best bid/ask price *after this change*" (`response.rs:129-134`) — i.e. the SDK's
  own contract is that these two fields are a coherent snapshot taken together at that specific
  update, not one fresh field paired with an echoed stale one. So the *within-one-message*
  non-atomicity concern this document originally raised is weaker than stated; the real risk is
  entirely the **cross-stream** one above — mixing a `BestBidAsk` update and a
  `PriceChangeBatchEntry` update that describe two different moments, with no timestamp kept to
  tell them apart.

Either failure mode produces the same symptom: a `(bid, ask)` pair, and therefore a `mid`
price, that never corresponded to one real, coherent order-book instant. `siglab`'s 2026-07-16
BNB incident found exactly this signature — an entry price (`0.8999999999999999`) that didn't
match the archived best-bid/best-ask bracket (`0.94`/`0.95`) on either side of it in time, while
the market's exit tick minutes later matched the archive closely. Not proven to be this
mechanism (see that incident's own caveat about 200ms archive sampling), but it's the leading
code-level candidate.

**This exact pattern is not confined to the paper-trading path — and the fix is simpler than
"reconstruct ordering from scratch."** `price_feed/src/collect.rs::spawn_bba_task`
(`collect.rs:1136-1169`) — the real data source `bin/live.rs`'s production NATS path ultimately
depends on — has the byte-for-byte identical `futures::stream::select(bba_u, pc_u)` merge over
the same two subscriptions. The difference: it already reads `m.timestamp`/`p.timestamp` off
each message into a `server_ts_ms` field (`collect.rs:1144-1166`) — captured specifically to
compute its own `latency_ms` metric — but that timestamp is **never used to decide whether an
incoming value should overwrite the cached one** (`collect.rs:1181-1189` just unconditionally
replaces `state.latest_bba` with whatever the merged stream yields next, in arrival order). So
the ingredient needed to fix the ordering problem is already being captured right next to the
bug — it's a one-line guard away (`if new.server_ts_ms >= cached.server_ts_ms`), not a redesign.
See "Ideas" below.

## 2. `SpreadSignal`'s premium/discount gate is structurally inert

`check_gates` runs this first, ahead of every other check (`gates.rs:1-8`, mirroring Python's
`_common_gates` per the module doc comment — presumably intended as a genuine cross-check on
quote quality before anything else runs):

```rust
// gates.rs:55-60
let total = spread.value();
if total > params.spread_premium_limit { return Some(GateBlock::SpreadPremium); }
if total < params.spread_discount_limit { return Some(GateBlock::SpreadDiscount); }
```

`SpreadSignal::value()` is `self.up + self.dn` (`signal/latest_poly.rs:83`), fed from
`PolyTick.up`/`PolyTick.dn`. But `PolyTick.dn` is never independently observed — every producer
of a `PolyTick` in this codebase computes it synthetically as `dn: 1.0 - up`
(`marketdata.rs:203`; same pattern in every caller of `spawn_poly_task`/`fetch_meta`, which all
discard the resolved `dn_id` — `bin/shadow.rs:166`, `siglab/market.rs:236`, and even the
real-money path `bin/live.rs` never subscribes a price feed for `dn_id`, only stores it for order
placement). So `spread.value() = up + (1.0 - up) = 1.0`, always, up to float noise — for
*every* tick, in *every* run mode. With `spread_premium_limit = 1.05` /
`spread_discount_limit = 0.95` (`gates.rs:96-97`), this gate cannot fire. Ever.

If the Python predecessor's version of this gate compared two **independently subscribed**
UP/DOWN books (a real cross-check: on Polymarket, complementary outcome tokens' best bid/ask
don't always sum to exactly 1 — divergence is a genuine staleness/mispricing signal), then the
Rust port lost that property somewhere in the translation, and nothing currently in the test
suite would catch it, since a gate that can never fire also can never fail a test that exercises
the "should block" branch with real UP/DN asymmetry (the existing `gates.rs` tests construct
`SpreadSignal` the same synthetic way, so they'd pass either way — see `gates.rs:120-145`).

## 3. `PolyTick`/`BinanceTick` (direct-WS path only) carry local wall-clock time, no server-side
   provenance — corrected from an earlier, too-broad version of this finding

```rust
// marketdata.rs:199-204 (poly) — same pattern at marketdata.rs:129 (binance)
tx.send(PolyTick { ts: now_secs_f64(), up, dn: 1.0 - up })
```

`now_secs_f64()` is `SystemTime::now()` at the moment this task's loop iteration processes the
message — not any timestamp the exchange or CLOB emitted, **despite the CLOB side of the SDK
providing one** (see §1's confirmed `BestBidAsk.timestamp`/`PriceChange.timestamp` finding —
`spawn_poly_task` never reads them). Binance's raw `@trade` WS payload is the same story: the
code parses the full JSON value (`marketdata.rs:124`) but only ever reads `v["p"]` (price);
Binance's standard `@trade` schema also includes `E` (event time) and `T` (trade time) fields
that go unread and unused. This is real, and it's why the BNB incident couldn't get a definitive
answer from this pipeline alone — but it is **specific to the direct-WS path**
(`spawn_poly_task`/`PolySub`, used by `siglab`, `backtest.rs`/`shadow.rs`, and `bin/live.rs`'s
`--nats-url`-absent fallback). An earlier version of this finding implied the system as a whole
doesn't measure network latency — **that was wrong**, corrected below.

**Production live trading (`bin/live.rs` with `--nats-url`) already measures real network
latency, and has since 2026-07-06.** It doesn't go through `spawn_poly_task` at all — it
subscribes to `price.poly.*`/`price.binance.*` NATS subjects published by `price_feed`'s
collector, which independently connects to the CLOB/Binance WS and *does* capture the exchange's
own timestamp (`price_feed/src/collect.rs::spawn_bba_task`, `m.timestamp`/`p.timestamp` off the
same SDK messages §1 covers). That `server_ts` is threaded through the NATS payload
(`poly_nats_payload`, `collect.rs:88-92`), extracted in `bin/live.rs` (`extract_server_ts`,
`bin/live.rs:344`), cached per-asset (`AssetSlot.last_poly_server_ts`/`last_binance_server_ts`),
and used to compute a genuine one-hop network latency:

```rust
// bin/live.rs:552 — exchange_latency_ms
fn exchange_latency_ms(local_ts: Option<f64>, server_ts: Option<f64>) -> Option<f64> {
    // local_ts - server_ts: a real, fixed one-hop number, independent of tick age
}
```

Shown unconditionally on every entry as `clob_latency=`/`binance_latency=` on Telegram and in
`live.log`, and documented at length in root `README.md`'s "Latency & observability
infrastructure" section (2026-07-06/07 entries) — which this review should have read before
making the original claim and didn't.

**Two genuinely different metrics, easy to conflate (this review did):**
- `clob_latency`/`binance_latency` (`exchange_latency_ms`, `bin/live.rs`, NATS path only) =
  **real network latency** — local receipt time minus the exchange's own event timestamp.
- `entry_signal_latency_ms`/`entry_process_latency_ms` (`TradeRecord`, all paths) = **internal
  processing latency** — `live.rs`'s `latency_ms(from_ts, to_ts) = (to_ts - from_ts) * 1000.0`
  (`bin/live.rs:582-584`), where both endpoints are our *own* `now_secs_f64()` stamps at
  different pipeline stages (tick captured → driver receives it → fill confirmed). README's own
  2026-07-07 entry confirms this is deliberate: "`process_latency` confirmed as a pure local
  round-trip, not mixable with a server timestamp." This one genuinely doesn't need, and isn't
  meant to have, a server timestamp — it answers "how long did our own software take," not "how
  stale was the price."

**What's still actually true after this correction:** the `PolyTick`/`BinanceTick` struct types
themselves never carry `server_ts` — it's deliberately kept out of them (`README.md`'s
2026-07-06 entry: kept as a `bin/live.rs`-only side channel specifically to avoid rippling a new
field into ~80 existing tick-construction call sites across `worker.rs`/`strategies.rs`/
`machine.rs`/`backtest.rs`/`gates.rs`/tests). That means every consumer *except* `bin/live.rs`'s
NATS path — `siglab`, `backtest`, `shadow`, `Machine`/`Worker`'s own internal signals
(`LatestPolySignal.age()`, gate staleness checks) — has no access to real network latency at all,
even though `price_feed` is, in production, already computing it one process away. That's a real
gap (see Ideas below), just a narrower one than originally stated.

## 4. No outlier/rate-of-change filtering beyond `is_finite() && > 0`

```rust
// marketdata.rs:195-198
if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 { continue; }
let up = (bid + ask) / 2.0;
```

```rust
// signal/latest_poly.rs:54-64
fn on_poly(&mut self, t: PolyTick) {
    if t.up > 0.0 { self.up = t.up; }
    if t.dn > 0.0 { self.dn = t.dn; }
    if t.ts > self.ts { self.ts = t.ts; }
}
```

Any positive, finite value is accepted and immediately becomes `latest_poly`'s value —
unconditionally, no matter how large a jump from the previous reading. Since 2026-07-13's
`reversal_start_time = 999999` widening (`config/markets.toml`), `SawLowSignal`'s dip-latch
window now spans the entire cycle, so there's no "quiet period" during which a stray glitch tick
is harmless — every tick is live for entry evaluation the whole cycle. A single bad print (SDK
parse bug, a resting order at an absurd price briefly becoming best-of-book in an empty book)
flows straight into a trading decision with nothing between the wire and `try_enter` that asks
"is this plausible given the last N readings."

## 5. No staleness/discontinuity handling around reconnects

```rust
// marketdata.rs:210-214
eprintln!("poly ws closed, reconnecting…");
...
tokio::time::sleep(std::time::Duration::from_secs(2)).await;
```

On disconnect, the loop silently sleeps and resubscribes. `max_price_age_secs` (default `2.0`,
`gates.rs:98`) will catch a position sitting on a *stale* price for too long, but nothing marks
the *first* tick after a reconnect as special — it's treated identically to any mid-stream tick,
even though the gap it just crossed could span an arbitrary real price move (or, in a thin book,
an arbitrary discontinuity) that the strategy never got to observe incrementally.

## 6. Minor: no dedup between the two merged streams

The module's own doc comment already flags that `best_bid_ask` and `price_changes` "can (or do)
deliver updates for tokens beyond the one requested" and filters by `asset_id`
(`marketdata.rs:168-171`), but doesn't address the simpler case: both channels legitimately
reporting the *same* real book change. `merged` can emit duplicate/near-duplicate `(bid, ask)`
pairs per actual change, each independently timestamped by local arrival — inflates apparent
tick rate without adding information. Low severity on its own, but compounds with §1 and §3:
more ticks means more chances for a reordering artifact, and no way to tell "genuinely new
information" from "the same book state reported twice" after the fact.

## Ideas / possible solutions (not implemented — for discussion)

- **Make `SpreadSignal` real, or retire it.** Either subscribe `dn_id`'s own book (already
  resolved by `fetch_meta` and already discarded at every call site) so the premium/discount gate
  becomes a genuine independent cross-check, or — if a second subscription isn't worth the
  connection/subscription cost (the SDK's `ConnectionManager` is one broadcast channel per WS
  connection with client-side filtering per subscriber, per `siglab/README.md`'s 2026-07-13 CPU
  incident — cost scales with subscription count) — replace it with a check that uses data
  already in hand, e.g. cross-checking the `best_bid_ask`-channel-derived mid against the
  `price_changes`-channel-derived mid for the *same* token as a same-side consistency check
  instead of a UP-vs-DOWN one.
- **Guard the merge overwrite with the timestamp that's already there — smallest, most concrete
  fix in this whole review.** `price_feed::collect.rs::spawn_bba_task` already extracts
  `server_ts_ms` from every merged message (it needs it for `latency_ms` anyway) but never
  compares it before overwriting `state.latest_bba`. Changing that one write to
  `if server_ts_ms >= cached.server_ts_ms { overwrite }` (a "last-value-wins by timestamp" guard
  — the standard pattern for this exact problem in market-data systems, since exchanges guarantee
  order *within* one channel but never across two) would fix the reordering risk in the actual
  production data path with no new subscription, no new field, and no framework — the ingredient
  was already being computed one line away. `trader::marketdata::spawn_poly_task` doesn't capture
  `server_ts_ms` at all today, so it needs that added first, but the same one-comparison guard
  would apply once it does. Only if `best_bid_ask` alone turns out sufficient on its own would
  dropping `price_changes` from the merge entirely be simpler still.
- **Thread real provenance through `PolyTick`/`BinanceTick` — for the direct-WS path only.**
  `price_feed`'s NATS path already has this (`server_ts`, `latency_ms`, threaded through to
  `bin/live.rs`'s `clob_latency`/`binance_latency`, live since 2026-07-06 — see §3's correction).
  The gap is narrower than originally stated: `siglab`, `backtest`, `shadow`, and `Machine`'s own
  staleness gate (`LatestPolySignal.age()`) all run on `spawn_poly_task`'s direct-WS path, which
  has no equivalent. Adding a server-side timestamp there (the SDK already provides it — see §1)
  would give those paths the same real-network-latency visibility production live trading already
  has, and would let `gates.rs`'s staleness check measure actual exchange staleness instead of
  only local tick age.
- **Bounded outlier/rate-of-change filter.** Reject or flag a tick whose mid differs from the
  immediately preceding one by more than some clamp within a very short window — conceptually
  similar to what `price_feed/src/reconcile.rs` already does for its own REST/WS reconciliation
  (debounce before confirming a disagreement is real); `trader::marketdata` has no analogous
  concept on its live path today.
- **Post-reconnect grace window.** Flag the first tick(s) after a WS gap so a strategy can choose
  to not immediately arm off a jump that may be a real move, a stale cache, or a discontinuity —
  currently indistinguishable from a normal mid-stream tick.
- **Liquidity-aware fill pricing.** Not this pipeline specifically, but the same root issue
  surfaces at exit: force-unwind-near-cycle-end (`trader::machine`, `v_shape.rs`) fills at the
  raw mid with no depth/size-at-touch check, so paper PnL in a thin book (this incident's cycle
  had spreads as wide as 0.80 minutes earlier) may not reflect what a real order could achieve.
  Already flagged as a follow-up in `siglab/README.md`'s TODO; noted here because it's downstream
  of the same "mid-price-only, no book-depth-awareness" pattern.
- **Raw per-message logging**, at least for a rolling window, so a future incident doesn't have
  to fall back on a separately-archived, 200ms-resampled recording to establish ground truth.

## Independent review (DeepSeek, blind)

`deepseek-v4-pro`, `reasoning_effort=max`, was given the same source excerpts (`marketdata.rs`
in full, `types.rs`, `latest_poly.rs`, `delta_pct.rs`, `gates.rs::check_gates`, and the relevant
`machine.rs::on_poly`/`try_enter` excerpt) and the same framing — "review for data-quality
risks, cite file:line, rank by likelihood of explaining the phantom entry-price gap, ideas only,
no implementation" — with **no visibility into this document, its findings, or the incident
writeup's own conclusions**. Its findings, condensed:

1. **(ranked #1 suspect)** Local wall-clock arrival time (`now_secs_f64()`) used as the tick
   timestamp instead of any exchange-generated timestamp — a network/scheduling delay makes a
   tick "look fresh" while actually describing older market state, which independent
   exchange-time-based archives would disagree with.
2. **(ranked #2 suspect)** The `best_bid_ask`/`price_changes` merge (`stream::select`) has no
   ordering guarantee — a slower channel's stale update can land *after* a fresher update from
   the other channel and overwrite it, and because timestamps are arrival-based (finding #1), the
   stale value looks like the newest price.
3. Synthetic `dn = 1.0 - up` makes `SpreadSignal`'s gate permanently ≈1.0 (dead code) — same
   finding as this document's §2, plus an angle I didn't consider: **if the anomalous trade had
   been on the DOWN side**, the synthetic `dn` itself (not just the merge mechanics) could be the
   direct source of a phantom price, since it was never independently observed. (Doesn't apply to
   the actual 2026-07-16 BNB trade, which was UP-side, but stands as a general design risk.)
4. **New finding I missed:** `LatestPolySignal::reset` deliberately does *not* clear
   `up`/`dn`/`ts` across cycles/market rotations (`latest_poly.rs`'s own comment: "last known
   price informative across cycles"). DeepSeek flags this as a real risk on its own terms: if a
   trading decision fires before the first fresh tick of a *new* market/cycle arrives, the
   machine could act on the *previous* market's leftover price, which would still pass the
   staleness gate (it's recent enough) while being economically meaningless for the new market.
5. **New finding I missed:** no crossed-book check — `spawn_poly_task`'s filter
   (`!bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0`) never verifies
   `bid <= ask`. A crossed snapshot (`bid > ask`, plausible in exactly the kind of thin/violent
   book the BNB incident showed) would still produce a "mid" that was never a real resting price.
6. Also flagged: Binance-side staleness isn't gated at all (only `latest_poly.age()` is checked
   in `check_gates`, `delta_pct`/`latest_binance` have no equivalent); reconnect gaps still read
   as "fresh" for up to `max_price_age_secs` after the gap closes (converges with this document's
   §5); `SystemTime::now()` isn't guaranteed monotonic (NTP/suspend could produce a backwards
   jump, corrupting age math); duplicate emission across the two merged streams (converges with
   §6).

### Discrepancies between the two reviews

- **Ranking disagreement on the leading suspect.** DeepSeek puts local-timestamp-only provenance
  (its #1) ahead of the stream-merge/atomicity issue (its #2); this document ranks them the other
  way. Both are plausible and not mutually exclusive — but for the *specific* 2026-07-16 BNB
  incident, a pure local-latency explanation is a weaker fit than DeepSeek's framing suggests:
  the archived independent recording shows the market was **genuinely moving fast** in that exact
  window (a real jump from 0.535→0.945 at the previous tick), and the subsequent exit tick
  matched the archive closely, so the anomaly looks localized to one tick rather than a
  systematic clock-lag offset. That's circumstantial, not conclusive — worth keeping both
  hypotheses open rather than resolving the disagreement here.
- **Two findings DeepSeek made that this review missed entirely:** the cross-cycle stale-price
  carryover in `LatestPolySignal::reset` (#4 above), and the missing crossed-book (`bid > ask`)
  check (#5 above). Both are concrete, correctly cited, and worth folding into any follow-up —
  credited to the blind review.
- **One area this review covered that DeepSeek's blind pass couldn't:** the SDK's
  `ConnectionManager`-per-WS-connection subscription cost pattern (documented in
  `siglab/README.md`'s 2026-07-13 CPU incident) as context for *why* dropping one of the two
  merged streams isn't necessarily free — DeepSeek proposed the same "unify the feed" idea
  independently but had no way to know about that prior incident, since it wasn't given
  `siglab`'s history.
- **Everything else converges**: both reviews independently flagged the local-timestamp
  provenance gap, the unordered stream merge, and the dead `SpreadSignal` gate as the three
  standout issues — two independent passes over the same code landing on the same top three is a
  reasonable signal these are real, not one reviewer's pattern-matching artifact.
