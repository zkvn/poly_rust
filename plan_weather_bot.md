# Plan: multi-market signal live-testing harness (working name `siglab`)

Status: **planning — not started, no code written.** This doc scopes a new, separate Rust crate;
`trader` (live/backtest bot) and `price_feed` (parquet recorder) are unchanged by this plan.

**Goal:** a new crate, sibling to `price_feed` and `trader`, that subscribes to live ticks across
a large, rotating set of Polymarket markets — the ~40 daily weather-temperature events
(`studies/weather/weather_poly_2026-07-12.md`) plus the existing 5m/15m crypto up/down markets —
and evaluates **many parameter variants** of `reversal`/`high_prob` (`reversal_1`, `reversal_2`,
..., `high_prob_1`, `high_prob_2`, ...) against each one concurrently. It does **not** place real
orders and does **not** record parquet/raw ticks. Its only output is a paper `TradeRecord` per
simulated fire: "if `reversal_7`'s params had been live on this market at this tick, here's what
it would have entered/exited and its PnL." The point is to cheaply answer "which of these many
backtested-good configs actually holds up on live data, across markets we've never traded before,"
without capital at risk and without the parquet-storage cost of full tick recording.

---

## 1. Why this is tractable (the load-bearing findings)

Four things found while scoping this make it smaller than "build a second trading bot":

1. **Weather markets don't need a new outcome model.** Each Polymarket weather event
   (`highest-temperature-in-hong-kong-on-july-13-2026`) is a `negRisk` group of 9-11
   **independent binary Yes/No sub-markets**, one per temperature bucket, each with its own
   `clobTokenIds` pair — confirmed live via Gamma API while researching
   `studies/weather/weather_poly_2026-07-12.md`. That's the same shape as a crypto Up/Down
   token pair. No change needed to `Side`/`TradeIntent`/`Machine`'s binary assumption — a
   weather "market" for this harness is just one more `(up_token, down_token)` pair per bucket,
   discovered differently (Gamma event → N buckets instead of slug → 1 pair) but consumed
   identically downstream.

2. **`trader` is already a library, and it already has the exact decision core this needs.**
   `trader/Cargo.toml` declares `[lib] name = "trader"` alongside its bin targets — every module
   (`machine`, `signal`, `gates`, `strategies`, `types`, `marketdata`) is a public, reusable crate
   dependency, not bin-only code. `trader::machine::Machine` (`trader/src/machine.rs`) is a
   **pure, side-effect-free, instant-fill decision core** — `Watching`/`Holding`/`Halted`, no
   CLOB writes, built precisely to drive backtests and `trader/src/bin/shadow.rs`'s live
   paper-trading. `shadow.rs` already proves this pattern end-to-end for **one asset, one
   config** (subscribes live Binance+Poly feeds, drives a `Machine` per configured strategy,
   logs would-be trades to CSV, zero order placement). This new crate is `shadow.rs` generalized
   from 1 market × 1 config to ~100 markets × many configs — a path dependency on `trader`
   (`trader = { path = "../trader" }`), not a fork of its logic.

3. **The "don't false-alarm on a quiet feed" problem is already solved once, expensively.**
   `price_feed/src/staleness.rs` documents a real incident: an earlier silence-timer watchdog
   (declare an asset "broken" after N seconds of no message) was deployed 2026-07-10 and
   immediately false-positive-stormed, because `best_bid_ask`/`price_change` are **change
   events**, not a heartbeat — long quiet stretches are normal, not broken. It was rolled back
   same day. The fix that shipped is **observe-only telemetry** (escalating silence buckets,
   logged not acted on) plus, separately, `gates.rs`'s existing **per-tick age gate**
   (`LatestPolySignal::age(now) > max_price_age_secs` blocks a trade *decision*, not the feed
   itself). Both pieces already exist and both are the right shape for this harness — see §6.
   Weather ticks are far quieter per-market than crypto ticks, so this lesson applies even more
   here, not less.

4. **The parameter grids already exist — this harness doesn't invent a sweep methodology.**
   `bt2`/`bt3` (`../btc_5mins/scripts/bt2.py`, `bt3.py`, `../btc_5mins/studies/bt2/`,
   `studies/bt3/`) already run walk-forward parameter sweeps per asset/duration and rank combos
   by PnL/win-rate (e.g. `studies/bt2/results_bt2_weekly/summary_2026-07-04.md`'s "top-5 by
   PnL" selection). This harness's config-loading job is to take N of those already-ranked
   combos and instantiate one `Machine` per combo — not to run its own optimization.

Net: the new/risky work is concentrated in **market discovery at this scale (weather event
enumeration, N-bucket sub-markets), the variant-fan-out config format, and connection/task
budgeting for ~100 rotating markets** — not in signal logic, staleness handling, or fill
simulation, all of which are reused as-is from `trader`/`price_feed`.

---

## 2. Explicit non-goals

- **No real orders.** No `execution.rs`/`balance.rs`/`unwind.rs`/`redemption.rs` equivalent. Ever,
  for this crate — if a variant looks worth trading for real, that's a `trader` config change, not
  a feature added here.
- **No parquet / raw tick recording.** `price_feed` already owns that; duplicating it here for 100
  markets would multiply Oracle's disk/CPU load for data this crate doesn't need to keep (it only
  needs derived trade outcomes, not the ticks that produced them).
- **No Telegram control** in the first version — this is a background analysis process, not
  something that needs runtime `/set`/`/halt` control. Read-only status (stdout/log) is enough to
  start; revisit only if operating it blind for weeks proves painful.
- **Not a replacement for backtesting.** bt2/bt3 stay the correctness oracle for *finding* good
  params (per `trader/plan_rust_module.md`'s existing "backtesting stays in Python" split). This
  harness answers a different question — "does a backtested-good config still look good on data
  it's never seen, live, right now" — which is a walk-forward-style OOS check, not a replacement
  for the sweep itself.

---

## 3. Market universe: two very different rotation shapes

| | Crypto (5m/15m up/down) | Weather (daily temperature) |
|---|---|---|
| Slug pattern | `{asset}-updown-{5m,15m}-{slot}` (`price_feed/src/markets.rs::make_slug`) | `highest-temperature-in-{city}-on-{month}-{day}-{year}` |
| Rotation period | 300s / 900s | ~24h (new event created ~1-2 days ahead per the Gamma data seen in the weather research doc) |
| Sub-markets per event | 1 (single Up/Down pair) | 9-11 (one Yes/No pair per temperature bucket) |
| Reference feed for `delta_pct` | Binance spot trade stream (existing) | **none** — see §5 |
| Gamma metadata cost | 1 fetch per asset per rotation (every 5-15 min) | 1 fetch per city per day |

The crypto side is a direct extension of `price_feed/src/markets.rs::fetch_meta` /
`trader/src/marketdata.rs` — same slug math, just more assets/durations running concurrently than
today's live config trades.

The weather side needs a **new discovery routine**: for each configured city, resolve the current
day's (and next day's, published ~1-2 days ahead) event slug, `GET
gamma-api.polymarket.com/events?slug=...`, and fan out over the `markets[]` array to get one
`(up_token, down_token, groupItemTitle)` triple per bucket — `groupItemTitle` (e.g. `"33°C"`) is
the human-readable bucket label to carry onto trade records. This is new code, but it's the same
Gamma-fetch shape already used for crypto, just walking an array instead of taking `markets[0]`.

**Rotation cost implication:** weather's Gamma load is trivial (dozens of cities × 1 fetch/day).
Crypto rotation is the actual REST-load driver if this scales to many assets × both 5m and 15m —
see §7 for the herd-avoidance point.

---

## 4. Strategy variant fan-out

Today's `trader/config/strategy_*.toml` encodes **one active param set per asset per strategy**
(e.g. one `reversal` threshold, one `high_prob` band). This harness needs **many named variants
per strategy family**, run concurrently against the same tick stream, e.g.:

```
reversal_1  = { reversal=0.70, reversal_low_threshold=0.20, delta_pct_rev=0.0006, sl_pnl_rev=0.40, unwind_time_rev=26.0, ... }
reversal_2  = { reversal=0.55, reversal_low_threshold=0.30, delta_pct_rev=0.0008, sl_pnl_rev=0.40, unwind_time_rev=30.0, ... }
high_prob_1 = { price_low=0.80, price_high=0.93, enter_when_time_left=20, delta_pct_hp=0.0004, ... }
...
```

**Proposed source of truth:** a small conversion step (Python, lives in `btc_5mins/scripts/` next
to `bt2.py`/`bt3.py` since that's where the sweep output already is) that reads a bt2/bt3 result
table's top-N ranked rows and emits a `variants_<asset>_<duration>_<date>.toml` in this crate's own
`config/` dir, in the shape above. Keeping this as a **generated, checked-in file** (not a live
query into bt2/bt3's internals at harness startup) matches the existing pattern of
`trader/config/strategy_*.toml` being a dated, versioned snapshot rather than a live computation —
and keeps this new crate's only cross-repo dependency a flat file, not a Python import.

Each `(market, variant)` pair becomes one `Machine` instance (`Machine::new_reversal`/
`new_high_prob`, unchanged) fed from that market's tick stream. `Machine` already takes an
`AssetParams`-shaped input, so the fan-out is "construct one `AssetParams` per variant row,"
no `Machine`/`gates.rs`/`strategies.rs` changes required.

**Open question (§11):** how many variants is "many many"? At 100 markets × even 10 variants
each that's 1,000 `Machine` instances — cheap in isolation (§7), but the *config file* itself
needs a sane cap or this becomes unmanageable to review/audit. Suggest scoping variant count per
market-class in the first version (e.g. top-5 reversal + top-5 high_prob per asset/duration) rather
than "every combo bt2 ever produced."

---

## 5. The weather signal gap — no `delta_pct` equivalent (phased, not solved here)

Both `ReversalStrategy` and `HighProbStrategy` gate on `delta_pct` — the crypto spot price's move
since cycle-open, sourced from the continuous Binance trade stream (`DeltaPctSignal`,
`price_feed/src/markets.rs`-style tick-by-tick). **Weather has no equivalent continuously-updating
reference feed.** The closest analogue — GFS/ECMWF forecast model output — updates a few times a
day (§4 of `studies/weather/weather_poly_2026-07-12.md`), not every second, and that same doc's
conclusion was: don't build a forecast-reactive strategy before confirming there's an actual
tradeable lag between a model-run publishing and the market repricing.

This plan does **not** attempt to solve that gap now. Proposed phasing:

- **Phase 1 (this crate's first version):** weather markets run through the harness with
  `delta_pct_rev`/`delta_pct_hp` gates effectively disabled (threshold = 0, i.e. any move passes)
  — meaning weather variants are really testing pure price-band/timing behavior (`high_prob`'s
  band-near-close idea, rescaled from "seconds before 5m cycle close" to "hours before daily
  resolution"), not a real reversal signal. This is intentionally a weaker/degenerate strategy
  for weather at first — the harness's job in Phase 1 is proving the plumbing (discovery,
  staleness, scale, trade-record output) works on a real 40-market weather universe, not
  producing a good weather strategy yet.
- **Phase 2 (only if warranted):** once the weather research doc's recommended 2-4 week
  data-collection phase actually shows a measurable, consistent lag between forecast-model
  publish and market repricing, add a `ForecastDeltaSignal` (polling open-meteo/NOAA on each
  model run) as the weather-side substitute for `DeltaPctSignal`. This is new signal code, but it
  slots into the existing extensible `Signal` trait (`trader/src/signal/mod.rs`) the same way
  every other signal does — no architecture change, just a new file.

Flagging this explicitly so weather results out of Phase 1 aren't over-read as "the strategy
didn't work" when what's actually being tested is a strategy deliberately missing its directional
gate.

---

## 6. Staleness & false-alarm prevention (reused, not reinvented)

Two independent layers, both already built elsewhere in this repo — reused, not designed fresh:

1. **Per-tick decision gate (already exists, applies unchanged):** `gates.rs::check_gates`'s
   `PolyStale` check (`latest_poly.age(now) > max_price_age_secs`) already means a `Machine`
   simply **will not fire** a trade intent on stale data — this is the primary false-alarm
   defense and it costs nothing new to adopt; every `Machine` this harness instantiates gets it
   automatically.
2. **Observe-only telemetry per market (port `price_feed/src/staleness.rs`'s pattern, don't touch
   its code):** for operator visibility (is market X's feed actually alive, independent of
   whether any variant happened to want to trade it right now), port the escalating-bucket,
   reset-on-fresh-message design — log when a market crosses 10s/30s/60s/120s/200s/300s of
   silence, take **no automatic action** on it. This is explicitly the lesson from the
   2026-07-10 incident (`price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md`): a raw
   silence timer cannot distinguish "broken" from "normally quiet" on a change-event stream, and
   an auto-resubscribe/recovery action triggered off silence alone previously caused a false
   positive storm worse than the outage it targeted. Weather markets will be quiet far more
   often than crypto (thin books, per §2 of the weather research doc), so this restraint matters
   more here, not less — resist the urge to add a "declare this weather market dead and
   resubscribe" watchdog in the first version.
3. **Explicitly deferred:** `price_feed/src/reconcile.rs`'s phase-2 REST ground-truth
   cross-check (poll `GET /midpoint` and compare against the WS-cached value) is the *correct*
   next step if observe-only telemetry ever shows a real missed-signal pattern — but per that
   module's own design note, its interval/thresholds should be sized from real observed silence
   data, not guessed upfront. Not building this until Phase 1's telemetry has run long enough to
   need it.

---

## 7. Scaling to ~100 rotating markets on Oracle

Three separate resources to budget, not one:

- **WS subscriptions, not WS connections.** `price_feed/src/markets.rs` already shares a single
  `ClobWsClient` across many per-asset tasks (`clob_client.clone()` — cheap handle clone, not a
  new socket) — the same pattern applies here: one (or a small, sharded handful, if
  `polymarket_client_sdk_v2` turns out to cap subscriptions-per-connection — **unverified,
  confirm empirically before assuming unlimited**, see §11) shared client subscribing ~100-200
  tokens (weather buckets roughly double the crypto per-market token count since each bucket
  needs its own subscription), not 100+ separate sockets.
- **REST/Gamma poll cadence — the real scaling risk.** Weather's Gamma load is negligible
  (§3). Crypto rotation (every 5-15 min per asset×duration) is where a naive "everyone refreshes
  metadata on its own 10s-poll timer" design (today's `price_feed/src/markets.rs` pattern, fine
  at ~7 assets) turns into a thundering herd at higher market counts — every asset's slot
  boundary lands on the same wall-clock 5-min mark, so unstaggered refreshes would all fire
  Gamma requests in the same instant. Needs simple jitter (spread refreshes across a few seconds
  around the boundary) rather than a shared fixed timer — small fix, but must not be skipped
  once market count is in the dozens, not ~7.
- **Trade-record writes — fan-in, not fan-out.** `shadow.rs` today opens/appends the CSV file
  directly per trade from a single asset's task. With ~1,000 `(market, variant)` evaluators
  potentially firing concurrently, route all `TradeRecord` writes through a single mpsc channel
  into one writer task (buffered append, matches the "single writer" pattern already implicit in
  `config_log.rs`'s append-only JSONL design) rather than N tasks opening the same file
  concurrently.

**Not a real concern:** tokio task count. Even 100 markets × 10 variants = 1,000 `Machine`
instances is cheap (each is a handful of small structs + arithmetic per tick, no I/O) — 1,000
green threads is unremarkable. CPU/memory budgeting should focus on the WS fan-in and REST poll
cadence above, not task count. Worth a quick empirical check on Oracle's actual free CPU/RAM
headroom before committing to a specific market×variant ceiling, rather than assuming either way.

---

## 8. Trade-record output

Reuse `trader::types::TradeRecord` as-is (`slug`, `strategy`, `side`, `entry_ts`, `token_price`,
`exit_price`, `outcome`, `pnl`, latency fields — already serde-ready) plus two additive fields this
harness needs that today's single-config live bot doesn't:

- `variant_id: String` — which named param set (`reversal_7`, `high_prob_3`, ...) fired, since
  many variants share `strategy`/`slug`.
  `market_kind: MarketKind` (`Crypto5m`/`Crypto15m`/`WeatherDaily`) — lets analysis slice results
  by market class without string-parsing the slug.

Output: plain CSV or JSONL append (JSONL is probably easier given the growing field list — no
header-migration problem as fields are added later), one file per day or per run, no parquet.
Matches the user's ask directly — this crate records **outcomes**, not **ticks**.

---

## 9. Proposed module layout

```
poly_rust/
  price_feed/     # existing — unchanged
  trader/         # existing — unchanged; consumed as a lib dependency
  siglab/         # NEW crate (working name — bikeshed welcome)
    Cargo.toml    # [dependencies] trader = { path = "../trader" }
    config/
      variants_*.toml       # generated from bt2/bt3 top-N (§4)
      weather_cities.toml   # city list → slug-prefix mapping
    src/
      main.rs        # CLI + supervisor: spawn one feed task per market, fan out ticks
      discovery/
        crypto.rs     # slot/slug rotation — thin wrapper over trader::marketdata
        weather.rs    # NEW: daily event → N-bucket sub-market discovery via Gamma
      fleet.rs         # NEW: owns the (market, variant) -> Machine map, routes ticks in
      staleness.rs     # ported observe-only pattern from price_feed (§6)
      tradelog.rs       # single-writer JSONL/CSV sink (§8), fed by mpsc from fleet.rs
```

Everything under `discovery/`, `fleet.rs`, `staleness.rs`, `tradelog.rs` is new. Everything else
(`Machine`, `Signal`s, `gates`, `strategies`, `AssetParams`) is `trader::` reused unmodified.

---

## 10. Phased rollout

1. **Phase 0 — prove the harness on markets already understood.** Run `siglab` against the
   existing 5m/15m crypto markets only, with the current live config's single param set (no
   variant fan-out yet). Success = trade records match what `shadow.rs` would produce for the
   same market/config, at the scale of "all assets × both durations" instead of one asset.
   Validates discovery, staleness telemetry, and the write path before adding complexity.
2. **Phase 1 — variant fan-out on crypto.** Add the many-`reversal`/many-`high_prob` config
   format (§4) against the still-familiar crypto markets. This is where the WS/REST scaling
   assumptions in §7 get real load for the first time — validate before adding weather.
3. **Phase 2 — add weather markets, degenerate strategy (§5 Phase 1).** New discovery code
   (`discovery/weather.rs`), same `Machine`/variant plumbing, `delta_pct` gate effectively off.
   Purely a plumbing/scale validation on the weather side — not expected to produce a tradeable
   weather strategy yet.
4. **Phase 3 — weather reference signal, only if the data justifies it.** Gated on the weather
   research doc's own recommended data-collection phase actually showing a lag worth trading
   (§5 Phase 2). Not scheduled; revisit after Phase 2 has run long enough to have an opinion.

---

## 11. Open questions to resolve before writing code

- **`polymarket_client_sdk_v2` subscription limits per WS connection** — unverified. If there's a
  hard cap well under ~150-200 tokens, §7's "one shared client" assumption needs sharding across a
  small fixed number of connections instead.
- **bt2/bt3 sweep output → machine-readable export.** Confirmed those scripts produce
  markdown/HTML/txt reports (`results_dir / f"{stem}.md"` etc., `bt2.py`); need to confirm whether
  the underlying ranked DataFrame is (or can cheaply be) also dumped to CSV/JSON for the §4
  conversion step, rather than scraping the markdown tables.
- **Variant count ceiling** — needs an explicit decision (§4), not left as "however many bt2
  produces."
- **Oracle headroom** — a quick CPU/RAM/bandwidth check on the box before committing to a specific
  market×variant ceiling for Phase 1/2, rather than assuming capacity.
- **Weather city list scope** — start with all ~40 from the Polymarket weather category, or a
  smaller curated set (HK + a couple of the most liquid US cities per
  `studies/weather/weather_poly_2026-07-12.md`'s liquidity numbers)? Affects Phase 2's discovery
  code surface and Gamma load, though per §3 that load is small either way.

---

# DeepSeek Review

Requested via `deepseek-v4-pro` (`reasoning_effort=max`), given the full doc above and asked to
be skeptical and concrete about §§1, 6, 7 in particular. Reproduced verbatim (most important
first, per its own ranking):

1. **Weather negRisk mutual exclusivity is ignored, leading to misleading per-event PnL.** Each
   temperature bucket is part of a negRisk group where exactly one can resolve Yes; the plan
   treats them as independent binary markets, so any downstream summation of PnL across buckets
   of the same event will overstate profitability and hide the real correlated risk. A strategy
   that buys multiple buckets may look profitable per bucket but cannot simultaneously win on
   all, making aggregate results unreliable without explicit event-level netting.

2. **Actual token subscription count is >800, not ~150-200, blowing up the WS scaling
   assumption.** 40 cities × ~10 buckets each = ~400 binary weather markets, each needing two
   token subscriptions (Yes/No), totaling ~800 token subscriptions for weather alone, plus
   crypto. This far exceeds the unverified ~150-200 cap mentioned in the doc; a single shared WS
   connection will almost certainly hit a hard limit, requiring a multi-connection sharding
   strategy that is presently not designed.

3. **Per-tick age gate will paralyse weather markets, making the harness produce zero trades for
   weather.** The `gates.rs` `PolyStale` check uses a `max_price_age_secs` threshold tuned for
   liquid crypto; weather markets can go hours without a tick, so nearly every evaluation will be
   blocked as stale. Without a separate, much larger threshold (or bypass) for weather, Phase 2
   will silently output no trades, invalidating the entire weather test.

4. **No analysis of per-tick CPU cost from duplicated signal computation across 1000+
   instances.** If each `Machine` independently re-derives signals (e.g., `delta_pct`) from the
   same raw tick, processing 400 weather markets × 10 variants = 4000 evaluations per tick can
   saturate a CPU core. The plan says "arithmetic per tick" is cheap, but it doesn't confirm
   whether signal computation is shared or whether `Machine` receives pre-computed signals;
   without this, the Oracle box may become overloaded once variant fan-out is active.

5. **No event-level grouping in trade records, risking misinterpretation of correlated
   outcomes.** The output adds `variant_id` and `market_kind` but lacks an event identifier
   (e.g., "HK-July-13") and a flag to indicate buckets belong to the same negRisk set. Any later
   per-event aggregation that simply sums bucket PnLs will be incorrect, and a researcher without
   deep Polymarket knowledge may draw wrong conclusions.

6. **Staleness telemetry scaling and silent WS-disconnection failure mode.** Porting the
   per-market silence watcher to 400 markets creates 400 timers, which may be costly and noisy;
   the plan also explicitly defers automatic recovery. If the entire WS connection drops, the
   harness will merely log all markets as silent and produce no trades, while the operator has no
   alert — defeating the "observe-only" goal of detecting real outages early.

7. **Weather's missing `DeltaPctSignal` may cause `ReversalStrategy` to panic or hang.** The plan
   disables the `delta_pct` gate by setting thresholds to 0, but does not specify how to satisfy
   `ReversalStrategy`'s dependency on a live `DeltaPctSignal`. If `Machine` initialization or
   evaluation expects a stream of such signals and finds none, it may fail at startup or panic on
   the first tick, blocking all weather variants.

8. **REST rate-limit budget for Gamma calls is not quantified, only jitter is mentioned.** Adding
   jitter prevents instantaneous spikes, but the total request rate across all rotating crypto
   markets (and the burst at weather day-rollover) is never compared against Polymarket's per-IP
   or global rate limits. A simple token-bucket rate limiter would provide hard guarantees;
   without it, a cluster of metadata refreshes could still trigger 429s and drop markets.

9. **Instant-fill assumption overstates performance on illiquid weather markets.** Weather
   buckets have wide spreads and thin depth; paper-trading at the best bid/ask with no slippage
   will make strategies look far better than achievable in live trading. The plan does not even
   include a configurable spread-crossing penalty, so the "holds up on live data" question
   remains largely unanswered for these markets.

10. **No operational health metrics or dead-letter detection.** Running 1000+ machine instances
    unattended for weeks without a periodic trade-count log or a "zero trades in the last hour"
    alert means a silent bug (e.g., a discovery failure that kills half the markets) could go
    unnoticed until someone manually inspects the output. A simple heartbeat and per-market-class
    trade counter would make the harness much safer to operate.

---

# Thoughts on DeepSeek Review

Going through each point against what's actually in the codebase (not just the doc's summary of
it) before deciding what to fold back in:

**Accept as-is, real gaps:**

- **#1 and #5 are the same finding and the most important one in the review** — the doc got the
  *execution* framing right (each bucket really is an independent binary CLOB market, so
  `Machine` needs no change) but missed the *analysis* framing: buckets in one event are mutually
  exclusive, so trade records need an `event_id` (grouping the buckets of one weather event)
  alongside `variant_id`/`market_kind`, and §8's output section needs an explicit warning that
  summing PnL across an event's buckets is wrong — only cross-event or within-a-single-bucket
  aggregation is valid. This is a real correctness trap for whoever analyzes the output later,
  not just a nice-to-have field. **Will fix.**
- **#2's arithmetic is right and the doc's number was just wrong** — I wrote "roughly double the
  crypto count" without doing the multiplication. 40 cities × ~10 buckets × 2 tokens is ~800,
  not ~150-200. This sharpens (doesn't change) §11's existing open question about connection
  limits and strengthens the case for scoping the initial weather city list down rather than all
  40 (already flagged as an open question, but this makes it a harder requirement, not a nice
  option). **Will fix the number and firm up the recommendation.**
- **#3 is a real bug-shaped gap, not just an open question.** `max_price_age_secs` in the current
  live config is 2.0s (`trader/config/strategy_20260709.toml`) — tuned for a feed that ticks
  multiple times a second. Applied unchanged to a market that can go minutes between ticks, every
  single evaluation gate-fails as stale and Phase 2 produces zero trades, silently. The doc's §5
  already flags the *signal* gap (no delta_pct-equivalent) but never mentioned the *staleness
  threshold* also needs to be market-class-aware. **Will fix** — add a per-market-class
  `max_price_age_secs` (or disable the gate for weather in Phase 1, consistent with disabling
  delta_pct, and revisit once §5 Phase 2's forecast signal exists).
- **#9 is the most embarrassing miss, because I'd already measured this.**
  `studies/weather/weather_poly_2026-07-12.md` §2 measured a live HK order book and found a $500
  order walks price from 49¢ to ~64¢ — that finding should have directly informed this doc and
  didn't. `Machine`'s instant-fill-at-mid sim is a reasonable simplification for crypto's deep
  book (that's what it was built and validated for) but silently carries a wrong assumption into
  a market where it's known to be wrong. Without a fill model that accounts for the book (even a
  crude one — e.g. simulate fill at best-ask-touch instead of mid, or use a recorded depth curve
  like the one already pulled for the research doc), Phase 2/3 weather PnL numbers are not
  trustworthy on their own. **Will fix** — add a required (not optional) fill-model item to §5's
  weather phasing, referencing the research doc's own order-book measurement.
- **#8 is a fair gap.** Jitter avoids a synchronized burst but doesn't bound total request rate.
  **Will fix** — add "confirm Gamma's actual rate limit and size a token-bucket limiter against
  it" to §7/§11, not just jitter.
- **#10 is good, cheap operational hygiene** for something meant to run unattended for weeks.
  **Will fix** — add a lightweight per-market-class trade-count heartbeat to §9's module list.

**Partially accept, real point but the review overstates it:**

- **#4** — the "4000 evaluations/tick saturates a CPU core" framing is alarmist (each evaluation
  is a handful of float comparisons — §7 already says task count isn't the bottleneck and that's
  still true), but the underlying architectural point is legitimate and worth fixing anyway: today's
  design has each `Machine` independently re-run `LatestPolySignal`/`SpreadSignal`/`DeltaPctSignal`
  from the *same* raw tick, once per variant watching that market — that's redundant, not
  expensive. Cleaner: compute each market's shared signals **once per tick** and hand every
  variant's strategy evaluation a read-only reference to that shared signal state, rather than N
  independent copies re-deriving identical numbers. Worth doing for cleanliness even though the
  CPU-saturation framing doesn't hold up. **Will fix** — note as a §7 design refinement.
- **#7** — checked `trader/src/signal/delta_pct.rs`: `DeltaPctSignal::value()` already returns
  `0.0` if `price` or `open` is `<= 0.0`, and it's only ever updated via `on_binance()` — if
  nothing ever calls that for a weather market, the signal simply stays at its zero default
  forever. `gates.rs`'s `MinDeltaPct` check (`dp.abs() < min_delta`) passes cleanly when
  `min_delta = 0.0`. So "panic or hang" doesn't match what the code actually does — this is
  defensive-by-construction, consistent with the project's no-`unwrap`-in-library-code rule. The
  real gap DeepSeek is pointing at is real, though: the doc never *says* this explicitly, so a
  future implementer has to go re-derive it from `delta_pct.rs` themselves instead of the plan
  just stating it. **Will fix the documentation gap, not the (non-existent) panic.**

**Noted, adds a genuinely new idea not in the original doc:**

- **#6's disconnection-failure-mode half is a good addition the doc missed entirely** — a WS
  connection dropping mid-run would show up as *every* subscribed market on that connection going
  silent at once, which is statistically nothing like one market's normal quiet stretch (the
  false-positive mode the 2026-07-10 incident was about). Correlated mass-silence across unrelated
  markets is a legitimate, low-false-positive signal for "the connection died," distinct from
  per-market observe-only telemetry, and doesn't reintroduce the original bug because it's
  triggered by *correlation*, not *duration alone*. **Will add** as a connection-level health
  check, separate from and in addition to (not instead of) the per-market observe-only pattern.
  The "400 per-market timers is costly" half of #6 is not a real concern at this scale (a few
  hundred `Instant` comparisons is negligible) and won't change anything in §7.

**Net:** 8 of 10 points fold into concrete doc changes (§4 numbers, §5 fill model + staleness
threshold, §7 signal-sharing + rate limiting + connection health, §8 event_id field, §9
heartbeat); #4 and #7 land as documentation/clarity fixes rather than the specific failure modes
described. Not yet applied to the body above — flagging here for review before editing §§4-9
directly, since several of these (the fill model in particular) are scope decisions worth
confirming rather than silently folding in.

---
