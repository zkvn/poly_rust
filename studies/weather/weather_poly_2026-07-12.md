# Weather markets on Polymarket — feasibility research

**Date:** 2026-07-12
**Type:** Research / feasibility (no code written, no data collected yet)
**Question:** Is there a viable, bot-tradeable edge in Polymarket's daily weather (temperature) markets, and can the current Rust bot support it?

## TL;DR

- **Markets exist and are real, but they're thin.** ~40 cities, daily temperature-bucket markets, $5k–$60k liquidity per event spread across 9-11 buckets. A $500 market order into a near-the-money bucket moves price ~15-30 cents on the thinnest legs — that's real slippage, not a rounding error. Doable, not free money.
- **The documented edge is "forecast latency arbitrage," not superior meteorology.** Professional weather traders (e.g. "Hans323", reportedly ~$1.1M on London temp contracts) win by re-pricing faster than the market when NWS/GFS/ECMWF model runs update — a handful of times a day, not continuously. This is a different game from the bot's current 5-minute crypto up/down strategy, but it rhymes: both are "react to a reference feed faster than the market."
- **The current Rust bot is architecturally the wrong shape for this, but not the wrong codebase.** The Gamma-API slug-fetch pattern and CLOB WS plumbing are reusable. The signal/cycle model (5-min slots, single reference tick stream, binary Up/Down) is not — weather markets are daily, multi-outcome (9-11 buckets in a `negRisk` group), and the "reference feed" is a forecast model run on a ~6-hour publish cadence, not a Chainlink price ticking every second.
- **Nobody's obviously gotten rich because the edge is small-per-trade, latency-sensitive, and getting arbitraged away as more quant money notices it** — plus real skepticism (even a profitable trader called his own edge "worthless" outside his own P&L; an academic questioned whether markets can synthesize domain-expert climate knowledge at all). This is closer to a competitive, thinning-out latency-arb niche than an inefficient backwater.
- **Recommended plan:** don't build a trading strategy yet. Spend 2-4 weeks just recording weather-market order books + NWS/GFS/ECMWF forecast updates for a handful of cities (start with HK, since it's already the ops home turf) to see if there's a measurable, tradeable lag between forecast-model updates and market repricing, before writing a single line of execution code.

---

## 1. What the market actually looks like

Polymarket runs a dedicated **weather category** (`polymarket.com/weather`) with 40+ cities, each getting a fresh daily event, e.g. `highest-temperature-in-hong-kong-on-july-13-2026`. Cities span Asia-Pacific (Tokyo, Seoul, Shanghai, Hong Kong, Singapore, Manila, Karachi...), Europe (London, Paris, Madrid, Istanbul...), Americas (NYC, LA, Chicago, Miami, Dallas, Toronto, Mexico City...), plus a couple of southern-hemisphere cities (Wellington, Cape Town). Kalshi runs an overlapping but US-focused set of the same style of market.

Structurally these are **not** simple binary Up/Down markets like the bot's current crypto 5-min contracts. Each daily event is a Polymarket `negRisk` group of ~9-11 sub-markets, one per temperature bucket (e.g. HK on 2026-07-13: "27°C or below", "28°C", "29°C", ... "37°C or higher"), each independently priced Yes/No, summing to ~100% probability across the group. Resolution source is the official government met-station reading (Hong Kong Observatory for HK, specific airport station + Wunderground for NYC) — unambiguous, no oracle risk in the crypto-bot sense.

Confirmed via Gamma API (`gamma-api.polymarket.com/events?slug=...`) for two concrete events:

| Event | Total liquidity | 24h volume | Open interest | # buckets |
|---|---|---|---|---|
| HK, Jul 13 2026 | $55,909 | $14,075 | $7,925 | 11 |
| NYC, Jul 13 2026 | $42,863 | $8,834 | $6,948 | 11 |

For reference, an outside estimate (PredictMarketCap) put Polymarket weather-wide volume around **$2M/day across 190 active markets in 37 cities**, with NYC, Chicago, Dallas, Seattle, Miami as the deepest US cities by market count. That's consistent with the per-event numbers above — decent aggregate volume, but split thin across many small, independent daily markets.

## 2. Order book depth — can I bet $500?

Pulled the live CLOB order book (`clob.polymarket.com/book`) for the HK Jul-13 "33°C" bucket, the near-the-money leg (~48.5¢ mid, tightest spread in the group):

**Asks (buy side), cumulative cost to fill:**
| Price | Size | Cumulative USDC spent |
|---|---|---|
| 0.49 | 11.7 | $5.80 |
| 0.50 | 45.1 | $28.30 |
| 0.53 | 148.1 | $109.40 |
| 0.56 | 140.5 | $231.20 |
| 0.58 | 50.0 | $320.60 |
| 0.64 | 122.3 | $398.80 |

To spend **$500 on the ask side you'd walk the book from 49¢ up past 64¢** — call it ~30% price impact for a $500 market order on the single most liquid, closest-to-50/50 bucket in the event. The out-of-the-money buckets (27°C-or-below, 37°C-or-higher, priced under 1¢) have even less depth in absolute USDC terms even though the headline "liquidity" numbers per bucket ($6k-$8k) look similar or larger — that liquidity sits far from the current price and isn't available at a tradeable price.

**Verdict:** $500 is tradeable but not free — you'd want to either (a) work a limit order instead of hitting market, (b) split size across several buckets/cities, or (c) size down to ~$100-200 per near-money leg to keep impact under ~10%. This is meaningfully thinner than the bot's current crypto 5-min markets, which have professional market-makers standing in every cycle.

## 3. Can the current Rust bot support this? (big picture, not a design doc)

**Reusable as-is:**
- The Gamma-API metadata-fetch pattern (`price_feed/src/markets.rs::fetch_meta`, `trader/src/marketdata.rs`-equivalent) — fetch-by-slug, pull `clobTokenIds`/`outcomes`, works identically for weather events.
- The CLOB WS subscription/order-book plumbing (`polymarket_client_sdk_v2`) — token-id-based, market-agnostic.

**Not reusable / needs new design:**
- **Cycle model.** The bot's whole `CycleContext`/signal architecture (`trader/src/signal/*.rs`, `machine.rs`, `worker.rs`) is built around a 5-minute slot that opens, samples a Binance/Chainlink reference tick, and closes — `DeltaPctSignal` explicitly resets every 300s (`price_feed/src/markets.rs:92-94`, `current_slot()`). Weather markets run on a **1-day** cycle with a **multi-hour** reference-feed cadence (forecast model runs), not a 5-minute one. This is a different temporal scale, not a parameter tweak.
- **Outcome shape.** The bot's `Side`/`Outcome`/`TradeIntent` types assume a binary Up/Down token pair per slug. Weather is an N-way (9-11 bucket) `negRisk` group — picking a side means picking *which bucket*, and the "reference value" (forecast temperature) needs to be mapped to a probability distribution over buckets, not compared against a single strike.
- **Reference feed.** Crypto strategy compares Polymarket price to a continuously-streaming Chainlink oracle price. Weather has no equivalent tick stream — the closest analogue is polling NWS/GFS/ECMWF model output (via NOAA NOMADS, open-meteo, or a paid feed) a few times a day when new runs publish, then re-deriving an implied probability distribution across the buckets from the forecast + its historical error distribution.

**Bottom line:** the bot is not "one config flag away" from trading weather, but it's also not a rewrite — the exchange-connectivity layer (Gamma fetch, CLOB WS, order placement/execution/`gates.rs`/`unwind.rs` risk plumbing) is reusable infrastructure. What's missing is a second, structurally different strategy module: a daily/multi-outcome cycle type, a forecast-feed poller, and a probability-distribution-to-bucket-price signal — realistically a new strategy alongside the existing one, not a modification of it.

## 4. Weather forecast models — are they good enough to trade on?

Short-range (1-3 day) temperature forecasts are genuinely good in 2026: **within 1-2°F/~1°C for day-1, with ECMWF holding roughly a 0.5-1.0°F lower mean-absolute-error than GFS** at short lead times, and about a one-day forecast-skill lead over GFS overall (GFS partially compensates by running 4x/day vs ECMWF's 2x, so it's fresher between ECMWF cycles). Given most weather markets resolve same-day or next-day, this is squarely in the models' comfort zone — this is not the regime where forecasting is hard (that's day 7-10+).

The practical implication: forecast accuracy itself is not the bottleneck. The question is whether **the market price already reflects the latest model run**, and if not, how long the gap persists and whether it's tradeable after slippage/fees.

## 5. Why isn't everyone already rich off this?

Several converging reasons, from the trading-strategy writeups down to the skeptics:

- **The edge is "who re-prices first," not "who forecasts best."** Multiple sources describe the same core strategy: scrape NWS/GFS/ECMWF model updates the moment they publish (models update on a fixed schedule — several times a day, not continuously), diff against the market's current implied probability, and trade the gap before the crowd does. One profitable trader ("securebet") reportedly ran ~3,000 small automated bets off NOAA data feeds; another ("Hans323") reportedly made ~$1.1M this way on London temperature contracts specifically. This is a **latency-arbitrage** strategy, structurally similar in shape to what the bot already does for crypto (react to a reference feed faster than the market) — but on a much slower clock (hours, not milliseconds) and against a thinner, more manual order book.
- **The edge is small per-trade and shrinks as more people find it.** This isn't a "forecast better than NWS" opportunity (rarely realistic for a solo project against agencies with far more data/compute) — it's a market-microstructure opportunity that competitive quant money is already working. As more automated traders watch the same model-run publish times, the latency window to be exploited compresses.
- **Real skepticism exists, including from people making money at it.** A top Polymarket weather trader (handle "Atte", ~$33k profit) reportedly described his own edge as "completely worthless" from a societal standpoint — i.e., personally profitable, but not evidence the market is doing anything meaningful, and not necessarily durable. A Boston University law professor (Madison Condon) separately questioned whether prediction markets can meaningfully synthesize complex domain expertise ("it's not like a basketball game") — relevant context for not over-trusting the market price as ground truth.
- **Regulatory overhang.** Weather/climate contracts are getting swept into the same scrutiny as election markets — Arizona reportedly filed action against Kalshi partly over this category. Not a trading-strategy risk, but a "don't build a business on this" risk worth flagging.

Net: this reads as a real, currently-active niche with documented winners, but one that rewards fast, automated, data-feed-driven execution against a thin book — not a slow-forecaster's free lunch. Feasible as a small-size systematic strategy; not "obviously exploitable and everyone's missing it."

## 6. Recommended plan if proceeding

Staged, cheapest-first, matching the "prove it before building it" instinct that's worked well for this project's other strategies:

1. **Data collection only (2-4 weeks), no trading.** Extend `price_feed`-style recording (or a standalone script — doesn't need to touch the live collector) to log, for a small city set (HK first — it's the team's home timezone/ops turf; add 1-2 high-volume US cities like NYC/Chicago for comparison):
   - Full order-book snapshots per bucket, on some interval (weather books move slowly — minutes, not seconds, so this is far cheaper to record than the crypto book).
   - Forecast-model output at each publish (GFS 4x/day, ECMWF 2x/day) via a free source (open-meteo API wraps both; NOAA NOMADS is free but rawer).
2. **Analyze for a lag, don't assume one.** Check whether market-implied bucket probabilities visibly lag forecast-model updates by a measurable, consistent window. If there's no visible lag (plausible — the strategy is publicly documented, may already be efficiently arbed), stop here; this was a cheap way to find out.
3. **Only if a lag shows up:** design the strategy module (new cycle type + signal, as scoped in §3) and paper-trade before real execution, sized to what the order book in §2 can actually absorb (likely $100-200/bucket to start, not $500).
4. **Treat $500-scale position sizing as an upper bound, not a target**, at least until real depth data (not a single snapshot) confirms it's typical rather than a thin moment.

This keeps the (per CLAUDE.md) "think first, verify before building" posture: the data-collection phase answers the actual open question (is there a lag to trade?) before any strategy or execution code gets written.

---

## Sources

- [Highest temperature in Hong Kong on July 13? — Polymarket](https://polymarket.com/event/highest-temperature-in-hong-kong-on-july-13-2026)
- [Highest temperature in NYC — Polymarket event pages](https://polymarket.com/event/highest-temperature-in-nyc-on-july-9-2026)
- [Polymarket Weather category](https://polymarket.com/weather)
- Gamma API (`gamma-api.polymarket.com/events?slug=...`) and CLOB API (`clob.polymarket.com/book?token_id=...`) — queried live, 2026-07-12
- [The Weather Betting Boom: $2M/Day on Kalshi & Polymarket Forecasts — PredictMarketCap](https://predictmarketcap.com/analysis/weather-betting-boom)
- [Weather Prediction Markets Are Booming. Can They Improve Forecasts? — Claims Journal](https://www.claimsjournal.com/news/national/2026/04/15/336839.htm)
- [Weather Prediction Markets Are Booming, but Can They Improve Forecasts? — Insurance Journal](https://www.insurancejournal.com/news/national/2026/04/15/866011.htm)
- [Kalshi Weather Betting Strategy: A Beginner's Guide — Laika Labs](https://laikalabs.ai/prediction-markets/kalshi-weather-betting-strategy)
- [GFS vs ECMWF: Which Weather Model Is More Accurate? — Celsi](https://celsi.markets/blog/gfs-vs-ecmwf-forecast-accuracy)
- [ECMWF vs GFS Accuracy 2026: EPT-2 Sets New Benchmark — Jua.ai](https://jua.ai/articles/ecmwf-vs-gfs-accuracy-2026/)
- [How Accurate Are Weather Forecasts in 2026? — Climavision](https://climavision.com/blog/how-accurate-are-weather-forecasts/)
- [Weather Prediction Markets: A Complete Guide — wethr.net](https://wethr.net/edu/weather-prediction-markets)
- [Betting On Weather — bettingonweather.com](https://www.bettingonweather.com/pages/temperature.php)
