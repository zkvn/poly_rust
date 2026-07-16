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

## 1. `spawn_poly_task`'s stream merge has no atomicity or ordering guarantee

```rust
// marketdata.rs:172-198
let bba_u = bba.filter_map(...);   // subscribe_best_bid_ask
let pc_u = pc.flat_map(...);       // subscribe_prices (price_change events)
let mut merged = futures::stream::select(Box::pin(bba_u), Box::pin(pc_u));
while let Some((bid, ask)) = merged.next().await {
    ...
    let up = (bid + ask) / 2.0;
```

Two independently-arriving WS subscriptions are merged with `futures::stream::select`, which
yields whichever stream is ready first — it has no notion of the two streams' original relative
ordering on the wire, only local poll readiness. Two distinct risks fall out of this:

- **Cross-stream reordering.** If `best_bid_ask` and `price_changes` both eventually report the
  same underlying book change, nothing guarantees the merged stream yields them in the order the
  server actually emitted them. A later, already-superseded message from one channel can be
  processed after a newer message from the other channel just because of scheduling luck.
- **Non-atomic `(best_bid, best_ask)` pairing within a single message.** The code takes each
  `price_changes` entry's `best_bid`/`best_ask` fields at face value
  (`marketdata.rs:183-186`), but nothing here confirms the SDK guarantees those two fields are a
  true simultaneous snapshot rather than one fresh field paired with an echoed/stale one from a
  partial update. This is opaque without reading `polymarket_client_sdk_v2`'s own source — not
  confirmed, just not ruled out.

Either failure mode produces the same symptom: a `(bid, ask)` pair, and therefore a `mid`
price, that never corresponded to one real, coherent order-book instant. `siglab`'s 2026-07-16
BNB incident found exactly this signature — an entry price (`0.8999999999999999`) that didn't
match the archived best-bid/best-ask bracket (`0.94`/`0.95`) on either side of it in time, while
the market's exit tick minutes later matched the archive closely. Not proven to be this
mechanism (see that incident's own caveat about 200ms archive sampling), but it's the leading
code-level candidate.

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

## 3. Ticks carry local wall-clock time only, no server-side provenance

```rust
// marketdata.rs:199-204 (poly) — same pattern at marketdata.rs:129 (binance)
tx.send(PolyTick { ts: now_secs_f64(), up, dn: 1.0 - up })
```

`now_secs_f64()` is `SystemTime::now()` at the moment this task's loop iteration processes the
message — not any timestamp the exchange or CLOB emitted. Compare `price_feed`'s own recorder,
which persists both `server_ts` and `latency_ms` per row (visible directly in its parquet
schema — confirmed while investigating the 2026-07-16 BNB incident). `trader::marketdata` has no
equivalent: once a tick is in `PolyTick`/`BinanceTick`, there's no way to later ask "how much
local processing/scheduling delay sat between the real event and when we saw it," which is
exactly the question the BNB incident needed answered and couldn't get from this pipeline alone
(it had to fall back on `price_feed`'s independent archive instead, which has its own coarser
200ms-sampling limitation).

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
- **Prefer one authoritative channel over merging two, or sequence the merge properly.** If
  `best_bid_ask` alone is sufficient (it's the channel purpose-built for this), consider dropping
  `price_changes` from the merge entirely rather than reconciling two streams with unclear
  relative guarantees. If both are genuinely needed, order by whatever the SDK exposes as a
  message sequence number or server timestamp rather than relying on `stream::select`'s
  incidental interleaving.
- **Thread real provenance through `PolyTick`/`BinanceTick`.** Add a server-side timestamp (if
  the SDK exposes one) and/or a monotonic local receive-sequence number, mirroring what
  `price_feed` already captures (`server_ts`, `latency_ms`) — so a future investigation doesn't
  have to reconstruct intent from a separately-archived, coarser-sampled recording.
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
