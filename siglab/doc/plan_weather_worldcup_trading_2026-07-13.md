# Plan: simulate real trades for weather and World Cup markets (not just monitor)

Status: **planning — not started, no code written.** Written for review per explicit
request. Extends `siglab`'s existing 18-variant reversal grid + 3 `high_prob` variants
(currently crypto-only) to also run against weather (51 cities, ~11 buckets each) and World
Cup (62 events, 1-60 buckets each) markets, producing real paper `TradeRecord`s instead of
the current monitoring-only price/staleness tracking.

**This reopens a deliberate design decision, not an oversight.** `siglab/src/event_monitor.rs`
explains at length why weather/World Cup are monitoring-only today: `Machine::cycle_close()`
resolves a held position by comparing `last_binance` against `cycle_open_binance` — correct
for crypto (that comparison *is* the market's real resolution), wrong here, where doing the
same thing would fabricate win/loss labels from price momentum instead of the real station
reading / match outcome. This plan doesn't relax that principle — it proposes the actual
machinery needed to resolve these markets correctly, so real simulation becomes honest
rather than fabricated.

---

## 1. Why this is a harder problem than "just point Machine at more markets"

Two structural gaps, not one:

1. **No reference feed for `delta_pct`.** `ReversalStrategy`/`HighProbStrategy` both require
   `dp > 0.0` or `dp < 0.0` (a strict inequality) as part of their own firing condition, not
   just the separate gate check. `DeltaPctSignal` only updates via `on_binance()`; if nothing
   ever calls that, `dp` stays exactly `0.0` forever, and **neither strategy can ever fire** —
   confirmed by reading `trader/src/signal/delta_pct.rs` and `trader/src/strategies.rs`
   directly, not assumed. Crypto has Binance as a natural, meaningful reference; weather and
   World Cup markets have nothing structurally equivalent.
2. **No valid resolution mechanism.** `Machine::cycle_close()` is the *only* public way to
   close out a still-open position, and it always resolves via
   `last_binance > cycle_open_binance`. There is no method on `Machine` that accepts an
   externally-known ground-truth outcome instead.

Both gaps need real solutions, not workarounds that quietly reintroduce the same fabrication
problem the monitoring-only design was built to avoid.

---

## 2. Proposed design — three new pieces

### 2a. A synthetic reference feed for `delta_pct` (entry-side only, siglab-only, no trader changes)

Feed each bucket's own mid-price into `Machine::on_binance()` as a stand-in `BinanceTick`,
turning `delta_pct` from "crypto reference price momentum" into "this bucket's own recent
price momentum." This unblocks entry firing using `Machine`'s existing public API — no
changes to `trader/` needed for this part.

**This is explicitly a heuristic, not a validated signal, and the plan should not pretend
otherwise.** The 18 reversal variants' thresholds were chosen for this *exercise* (test many
combinations against real crypto ticks), not derived from any evidence that a dip-then-recover
pattern in a weather bucket's own price predicts anything. Worse: `studies/weather/
weather_poly_2026-07-12.md`'s own research found that the only documented real edge in
weather markets is **forecast-latency arbitrage** (reacting to NWS/GFS model updates faster
than the market), not price-reversal patterns — so there's a specific, previously-documented
reason for skepticism here, not just generic caution. This plan is about finding out
empirically whether self-momentum reversal patterns show *any* edge on these markets,
starting from a prior that they probably don't — not a claim that they will.

### 2b. Real Yes/No-aware resolution polling (new, siglab-owned, no trader changes)

`trader::marketdata::fetch_gamma_resolution` only recognizes `"UP"`/`"DOWN"` outcome labels
(crypto-specific). Weather/World Cup buckets resolve to `"Yes"`/`"No"`. Need a new
siglab-owned poller — structurally the same idea (poll Gamma's `outcomePrices` until one
side reaches ≥0.99) but generalized for Yes/No, and per-bucket rather than per-market (each
bucket resolves independently and at different times within the same event — e.g. one World
Cup award-winner bucket resolves the moment that player is confirmed, while sibling buckets
in the same negRisk group may take longer).

### 2c. A way to close a position at the *real* outcome (small, additive change to `trader/machine.rs` — needs your explicit go-ahead)

Add one new public method, e.g.:

```rust
/// Resolves a currently-held position using an externally-known outcome, instead of
/// inferring it from last_binance vs cycle_open_binance. For markets with no valid
/// price-momentum resolution rule (see siglab/src/event_monitor.rs). Does not touch
/// cycle_close()'s existing behavior — purely additive.
pub fn resolve_with_outcome(&mut self, won: bool) -> Option<TradeRecord>
```

Mirrors `cycle_close()`'s body exactly, except `won` is passed in rather than computed from
`last_binance`. **Zero risk to existing crypto behavior** — `cycle_close()` is untouched,
this is a new method nothing else calls. This is the cleanest option found; alternatives
considered and rejected:

- *Duplicate the resolution/PnL logic inside siglab instead of touching `trader/`* — avoids
  the trader change, but `Machine`'s `state`/`HoldingData` fields are private, so siglab has
  no way to know whether a given `Machine` is currently holding a position, at what price, or
  on which side, without `Machine` exposing something. Reimplementing enough of `Machine`'s
  state machine in siglab to work around this defeats the point of reusing it and risks
  drifting out of sync with the real one.
- *Feed the bucket's own momentum into `cycle_close()`'s existing logic (reuse it
  unmodified)* — this is exactly the fabrication the monitoring-only design was built to
  avoid, just moved one layer down. Rejected on the same grounds as before, not reconsidered
  here.

**This is the one part of this plan that needs your explicit sign-off before any code is
written**, since it's the only piece touching `trader/`.

---

## 3. Cycle semantics: what is "a cycle" for a market with no fixed rotation period?

Crypto's `CycleContext { start_ts, end_ts, open_binance }` assumes a short, fixed,
repeating period (300s/900s/14400s/3600s). Weather and World Cup don't rotate like that:

- **Weather**: one cycle per city per day. `start_ts`/`end_ts` should come from the event's
  real Gamma `startDate`/`endDate` fields (already fetched by `event_monitor::
  fetch_event_buckets`, just not currently plumbed through) rather than a synthetic
  midnight-to-midnight boundary — this makes the 18 variants' "no time window" settings
  (`no_enter_when_time_left=0`, `reversal_start_time=999999`) correctly span the *real*
  trading window instead of an approximation of it.
- **World Cup**: most events have exactly **one** lifetime — no rotation at all. `cycle_open`
  fires once (at discovery), `cycle_close`/`resolve_with_outcome` fires once (at real
  resolution, whenever that happens — could be hours to weeks later). This is a simpler
  shape than crypto's repeating rotation, not a harder one, once §2c exists.

**`unwind_time_rev`/`unwind_time_hp` (max holding time) need reconsidering, not reusing
as-is.** The current grid's `unwind_time_rev = 30.0` (seconds) assumes a 5-minute-scale
cycle — applied unchanged to a multi-hour weather day or a multi-week World Cup market, it
would force-close almost every position moments after entry, silently defeating the
strategy. Needs either a much larger value scaled to the real cycle length, or `0.0`
(disabled) for these market classes — an open question in §6, not decided here.

---

## 4. Trade-record schema: the mutual-exclusivity problem is no longer deferrable

`siglab/src/record.rs`'s doc comment already flags this, deferred "until weather markets
produce real trade records": buckets within one weather city or World Cup event are
**mutually exclusive** (a negRisk group — exactly one resolves Yes). Once these markets
produce real `SiglabTradeRecord`s, summing PnL across an event's buckets is wrong, and
nothing currently distinguishes "these 5 trade records are 5 independent markets" from
"these 5 trade records are 5 mutually-exclusive legs of one weather day." **This plan must
add an `event_id` field to the trade record before or alongside this work**, not after —
this is exactly the moment §1 of that deferred note describes. Concretely: the weather
city name or World Cup event slug, so downstream analysis can group correctly.

---

## 5. Scale: how many `(bucket, variant)` instances is this, really?

Applying all 18 reversal variants + high_prob to every bucket:

- Weather: 51 cities × ~11 buckets × 18 reversal variants ≈ **10,100** reversal instances
  (high_prob currently isn't asset-scoped this way — would need its own decision, see §6).
- World Cup: 62 events × (1 to 60 buckets, most under 20, `world-cup-winner` alone has 60) ×
  18 ≈ conservatively **10,000-15,000** more.

Per this session's own findings (`doc/incident_ws_2026-07-13.md`), `Machine` instance count
itself was never the cost driver — each does trivial O(1) per-tick arithmetic, and the
expensive part (WS subscription fan-out) is already batched per-event/city, independent of
how many `Machine`s watch the resulting ticks. So ~20,000+ `Machine` instances is *plausibly*
fine, matching the earlier "task count is cheap" finding — but that finding was validated at
~450 instances (the crypto grid), not 20,000+, and this plan should not assume the same
conclusion holds two orders of magnitude further out without checking. **Needs a real test
before trusting it**, not an extrapolation — see §7.

A likely-necessary scope-down, to decide before implementing rather than after hitting a
problem: apply the full 18-variant grid only to buckets near the money (e.g. the 2-3 buckets
with current probability closest to 50%) rather than every bucket — the report already only
surfaces the top bucket per event for exactly this "far-out buckets aren't the interesting
ones" reason, and it caps the realistic Machine count by roughly (11→3)/(60→3) instead of
scaling with each event's full outcome count.

---

## 6. Open questions to resolve before writing code

- **`resolve_with_outcome` on `trader::machine::Machine` (§2c) — needs your explicit
  go-ahead**, since it's the one piece touching `trader/` source, even though additive/
  zero-risk to existing behavior.
- **`unwind_time_rev`/`unwind_time_hp` for non-crypto-scale cycles (§3)** — disable, or scale
  to the real cycle length? Affects whether positions can realistically hold to resolution
  at all for multi-day/week markets.
- **Full bucket coverage vs. near-the-money-only scoping (§5)** — real cost/signal-quality
  tradeoff, not a detail.
- **`high_prob` variants for weather/World Cup** — currently asset-scoped (`high_prob_btc`
  etc.), a shape that doesn't map onto "51 cities" or "62 events" the way the
  asset-agnostic reversal grid does. Needs its own small design pass (a handful of
  band/timing variants applied broadly, similar to reversal) rather than reusing
  `high_prob_btc`/`eth`/`doge` as-is.
- **Per-bucket resolution polling load (§2b)** — 20,000+ buckets each needing an eventual
  Gamma resolution check is a new, unbounded-until-resolved REST polling surface that didn't
  exist before (weather/World Cup discovery today is periodic *rediscovery*, not per-bucket
  resolution watching). Needs a poll-interval/backoff design, not naive per-bucket polling
  on a tight loop.

---

## 7. Phased rollout, if approved

1. **Phase 0 — one city, `resolve_with_outcome` added, no scope-down yet.** Prove the whole
   chain end-to-end (synthetic reference feed → entry fires → real Yes/No resolution polling
   → `resolve_with_outcome` closes correctly → `event_id` present in the output) on a single
   weather city before touching scale questions.
2. **Phase 1 — near-the-money scoping, all weather cities.** Apply §5's scope-down, measure
   real `Machine` count and resource cost at full weather scale (not extrapolated).
3. **Phase 2 — World Cup**, same pattern, likely needing per-event bucket-count-aware scoping
   given `world-cup-winner`'s 60 buckets vs. most others' handful.
4. **Not scheduled: any claim that this strategy family is "good" for these markets.** The
   point of this work is to find out, starting from the documented reason (§2a) to expect it
   probably isn't — a phase to "adopt" a resulting config would be a separate, later decision
   requiring its own evidence, not implied by finishing this plan.
