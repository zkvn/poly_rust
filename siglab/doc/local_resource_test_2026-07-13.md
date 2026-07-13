# siglab — local Docker resource test (2026-07-13)

**Purpose:** Phase 0 checkpoint per `plan_weather_bot.md` §10/§11 — prove the harness works
end-to-end against real live markets, in Docker, standalone from `trader`/`price_feed`, and
get a first empirical CPU/memory data point to size Oracle capacity against before scaling
toward the ~100-market target.

## What ran

- `docker compose -f siglab/docker-compose.yml up --build` — siglab's own compose file, own
  Dockerfile, own image (`siglab-siglab`), own volume (`siglab_logs`). Not added to the root
  `docker-compose.yml`; `trader`/`price-feed`/`nats` were not running and were not touched.
- Config: `siglab/config/markets.toml` as committed — 12 real crypto markets (BTC, ETH, SOL,
  BNB, XRP, DOGE × {5m, 15m}), 9 variants (6 `reversal_*` + 3 `high_prob_*`), 18 total
  `(market, variant)` → `Machine` instances.
- Duration: 27.4 minutes (03:52:52–04:20:13 UTC), continuous, against live Polymarket CLOB +
  Binance feeds — real network I/O and real market data throughout, not a synthetic load test.
- Sampling: `docker stats siglab-siglab-1 --no-stream` every 10s (150 samples).

## Health

- **`RestartCount=0` for the entire run** — no crashes, no panics, no OOM kills.
- 5 real paper trades fired and were logged to `/app/logs/siglab_trades.jsonl` inside the
  container (all `high_prob` variants — no `reversal` fires happened to land in this
  particular 27-minute window, which is expected variance, not a bug: `reversal` needs a
  dip-then-recover pattern within its entry window, a rarer condition than `high_prob`'s
  simple price-band check). Trade records are well-formed JSONL with sensible PnL:

  ```json
  {"logged_at":1783915185.10,"market_kind":"crypto","variant_id":"high_prob_btc","asset":"BTC","slug":"btc-updown-15m-1783914300","cycle_start":1783914300.0,"strategy":"high_prob","side":"DOWN","entry_ts":1783915180.00,"token_price":0.885,"exit_price":0.955,"outcome":"UNWIND","pnl":0.0791}
  ```

- Staleness telemetry fired correctly and **only as observation** — e.g. repeated
  `DOGE-*m:binance silent ~11000ms (crossed 10000ms bucket)` lines (DOGE trades less
  frequently on Binance than BTC/ETH, a genuinely quiet stretch, not a broken feed) and a
  couple of `BNB-*m:poly silent 30000ms (crossed 30000ms bucket)` escalations. No
  reconnect/resubscribe storm, no correlated-silence warning fired (correctly — these were
  isolated single-market quiet stretches, not every feed going dark at once). This is the
  exact "observe, don't act" behavior the design set out to get right (see
  `siglab/src/staleness.rs`'s doc comment on the 2026-07-10 incident it's built to avoid).
- **One real bug caught before this run, fixed, and reflected in these numbers:** the first
  local (non-Docker) run showed each asset opening two independent Binance WebSocket
  connections (one per duration task) instead of one shared per-asset feed. Fixed via
  `market::spawn_binance_broadcast` (one real connection per asset, fanned out to every
  duration task trading it) before building the Docker image — see `market.rs`'s doc comment.
  The numbers below reflect the fixed version, not the wasteful one.

## CPU / memory (147 samples, first 3 dropped as container-startup warmup)

| Metric | Min | Max | Avg |
|---|---|---|---|
| CPU % (of one core; host has 16 cores, `docker stats` normalizes to one-core=100%) | 0.05% | 14.47% | 5.28% |
| Memory | 14.9 MiB | 31.3 MiB | 24.0 MiB |
| PIDs (tokio worker threads + OS threads) | 17 | 29 | 17.6 |

For 12 markets / 18 `Machine` instances against **live** feeds (not synthetic), this is a
trivial resource footprint — a small fraction of one CPU core and well under 32 MiB of RAM.

## Extrapolation to ~100 markets

Linear scaling on market count (12 → 100 is ~8.3x) gives a rough **upper-bound** estimate:

- CPU: ~44% of one core average, occasional bursts toward ~120% (a bit over one full core)
- Memory: ~200 MiB

This is deliberately conservative in one direction and optimistic in another, so treat it as
a bound, not a prediction:

- **Conservative (overestimates):** this run's 12 markets are all liquid, high-tick-rate
  crypto 5m/15m markets. Per `studies/weather/weather_poly_2026-07-12.md`, weather markets
  tick far less often (thin books, slow-moving temperature buckets) — the ~40-80 weather
  sub-markets that would make up most of the path to 100 will individually cost noticeably
  less CPU per market than the crypto markets measured here, not the same amount.
- **Optimistic (underestimates, i.e. real cost could be higher than this extrapolation):**
  this test did not exercise the two things `plan_weather_bot.md` §7/§11 flagged as the
  actual scaling risks — WS subscription count per connection (this run used 12 markets'
  worth of subscriptions, nowhere near the ~800 weather-alone estimate from the DeepSeek
  review) and Gamma REST poll cadence under a synchronized-rotation thundering herd (12
  markets rotating on 2 duration boundaries didn't stress this; 100 markets rotating
  crypto-style would). **CPU/memory is not the bottleneck for reaching 100 markets — subscription
  sharding and REST rate-limiting are, and neither was load-tested here.**

**Bottom line:** CPU/memory headroom on Oracle is very unlikely to be the constraint for
scaling this harness — even a pessimistic 10x-of-measured number (∼50% of one core, ~300 MiB)
is comfortably small. The open questions from `plan_weather_bot.md` §11 (WS subscription cap,
Gamma rate limit) remain open and are the actual next things to test before trusting a
100-market deployment, not CPU/memory.

## Isolation check

`git status` before/after this entire session shows only new files under `siglab/` — nothing
in `trader/` or `price_feed/` was read, written, or executed by this test. The Docker stack
used its own compose file, own image, own volume; `trader`/`price-feed`/`nats` were not
running during the test.

---

## Run 2 (same day): full scale — 24 crypto markets + 51 weather cities, and the CPU
## extrapolation above turned out to be wrong

**What changed:** added 4h and hourly-ET crypto markets (18→24 crypto markets) and full
weather monitoring (51 cities, ~525 total subscribed tokens once weather buckets are
counted — 49 of 51 cities had an active event; ~11 buckets/city × 2 tokens/bucket, since
`spawn_poly_task` subscribes both `best_bid_ask` and `prices` per token). Same isolated
Docker setup as Run 1. Sampled 16.4 minutes (90 samples/10s) after the initial
discovery burst settled.

**Result: sustained 200-370% CPU (2-3.7 cores), not the ~44% Run 1's linear extrapolation
predicted.** Average 221%, never dropping below double digits except in a couple of
individual 10s samples. This is not a startup artifact — it held for the full 16-minute
window, evenly spread across all ~16 tokio worker threads (confirmed via
`/proc/1/task/*/stat` — no single hot thread), and `RestartCount` stayed at 0 throughout
(not a crash-loop either).

**Root cause, traced to the SDK source
(`~/.cargo/registry/.../polymarket_client_sdk_v2-0.6.0-canary.1/src/ws/connection.rs`):**
`ConnectionManager` holds exactly **one `broadcast::channel` per WS connection** (one per
`ChannelType`, e.g. one for the whole "Market" channel). Every call to
`subscribe_best_bid_ask`/`subscribe_prices` — regardless of which token — calls
`self.connection.subscribe()` and gets back a **fresh receiver on that same shared
broadcast channel**, then filters client-side by `asset_id` inside a `try_stream! {
loop { rx.recv().await ... } }` (`subscription.rs::subscribe_market_with_options`). There
is no server-side or connection-level per-token filtering — every message that arrives on
the shared connection is broadcast to and filtered by **every** subscriber. With ~1,050
concurrent subscriptions (525 tokens × 2 subscription calls each), the cost is
**O(subscriptions × message rate)**, not O(subscriptions) as Run 1's "task count is cheap,
each Machine does trivial arithmetic" framing assumed. This is a structural property of
the SDK, not a bug in siglab's code — confirmed by reading the actual `subscribe()`/
`ConnectionManager` implementation, not inferred from behavior alone.

**Why Run 1 didn't see this:** 12-18 crypto markets → ~40-50 subscriptions is small enough
that the O(n×msgs) cost stayed under 15% of one core. The effect only becomes visible once
subscription count gets into the hundreds — exactly the regime weather's ~500 bucket
tokens push into, and exactly the ~800-subscription estimate the DeepSeek review (in
`plan_weather_bot.md`) flagged as unverified. **That review was right to flag it; Run 1's
own "CPU/memory is not the bottleneck" conclusion was wrong** — it is the bottleneck, and
this run gives the actual mechanism, not just a number.

**Practical implications:**
- **Correcting Run 1's extrapolation:** "linear in market count" does not hold once
  subscription count is high — the real cost driver is total *token subscription count*
  times *aggregate message rate on the shared channel*, not per-market instance count.
  Scaling further (toward Oracle's ~100-market target, especially with weather's high
  per-event bucket count) needs subscription count itself to be budgeted, not assumed free.
- **Not urgent to fix for this dev-box deployment** — 2.2 average cores out of 16 available
  is fine here, and the box stayed responsive throughout. This is flagged for Oracle
  sizing, not as something broken right now.
- **Mitigation options for later, not applied here:** (a) shard subscriptions across
  multiple `ClobWsClient` instances instead of one shared client, so each shard's broadcast
  fan-out only reaches its own subscribers, not all ~1,050; (b) reduce weather bucket
  coverage (e.g. only the 2-3 near-the-money buckets per city instead of all ~11, since the
  report already only surfaces the top bucket per city); (c) confirm with the SDK
  maintainers whether a lower-level per-token-filtered API exists. None implemented in this
  pass — flagging as the concrete next capacity question, superseding
  `plan_weather_bot.md` §11's "WS subscription limits — unverified" with an actual
  mechanism and a real 16-minute data point.

**Health otherwise unaffected:** real trades still fired and logged correctly
(`siglab_trades.jsonl`), weather prices for 49/51 cities showed correct live data in the
generated report, staleness telemetry and the per-class correlated-silence fix (added this
session — see `staleness.rs`) both behaved correctly with no false alarms despite the much
larger, much quieter weather feed set. The elevated CPU is a capacity/cost finding, not a
correctness or stability one.
