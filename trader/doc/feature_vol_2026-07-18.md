# feature_vol — standalone `indicator` module (HAR vol, P(up), SNR) — 2026-07-18

Plan for a new top-level `indicator/` crate, a sibling of `price_feed/` and
`trader/`, that runs independently, consumes live prices from NATS exactly like
the trader does, computes the bt4-study signal stack (HAR volatility forecast,
P(up), SNR) in native Rust, and republishes the results on NATS so the trader
(or anything else) can consume them with negligible overhead. Deployment target
is the Oracle box, so the budget is "rounding-error" CPU/memory.

## 1. Source study — what we're porting (../btc_5mins, bt4)

`studies/bt4/` ("Signal predictiveness", runs 2026-06-12 / 2026-06-15, generated
by `scripts/bt4.py`) replays the live strategy config and asks whether recorded
signals (`p_up_adj`, `snr_adj`, `pre_vol`, `vol_har5`, `edge_spread`,
`delta_pct_abs`) predict WIN/LOSS. Findings that matter here:

- The replayed config is profitable on all six assets (win 80–91%), but the
  signal stack is a **weak filter** (per-signal AUC ≈ 0.5). That motivated the
  follow-up `pup_gate` study isolating the two P(up) flavors — streaming
  Gaussian vs **HAR Student-t**, the latter being the winning config from
  `p_up_timing` (c_1_5_12, student-t, raw).
- So the indicators worth computing live are exactly the three the user asked
  for, in their HAR form: `vol_har`, `p_up`, `snr`.

Reference implementation is `../btc_5mins/bot/signals.py`
(`VolHarSignal`, `PUpSignal`, `SnrSignal`, and the module-level
`_compute_pup` / `_compute_snr`). The exact math to reproduce:

**HAR volatility forecast (per cycle, computed at each cycle boundary):**

- Collect ~1-Hz Binance prices over the cycle. At the boundary, if ≥ 30 ticks:
  5s-subsample (`prices[4::5]`), take log returns, and
  `rv_5s = sqrt(Σ r²)` — the cycle's realized vol.
- Push `rv_5s` into a rolling buffer of `max(windows)` cycles (Python: 12).
- Forecast with pre-fitted OLS betas (`min_periods=1` semantics — each mean
  uses whatever cycles exist):

  ```
  σ_full = max(0, b0 + b1·rv[-1] + b2·mean(rv[-5:]) + b3·mean(rv[-12:]))
  ```

- Betas/nu are fitted offline (`ml/features/har_beta.py`, method-of-moments nu
  clipped [3,30]); the live values already sit in the trader's strategy TOML
  (`[har_beta]` / `[har_nu]`, e.g. BTC beta =
  `[6.753e-05, 0.3809, 0.2301, 0.3215]`, nu = 4.2469).

**P(up)** (HAR mode; streaming-Gaussian mode kept as fallback):

```
z_τ    = ln(p_now / p_open) / (σ_full · √(τ/300))
P(up)  = T_cdf(z_τ · √(ν/(ν−2)), df=ν)        # Student-t, raw
```

Streaming mode: `z = ((p_now−p_open)/p_open) / (σ_stream·√τ)`, `P(up) = Φ(z)`,
where σ_stream is the ddof=1 std of in-cycle 1-Hz simple returns.

**SNR** — always the signed z (no CDF); only the denominator switches between
the two modes. `None` while not ready; `0.0` only for genuine zero displacement.

Warmup semantics to preserve: P(up) = 0.5 and SNR = null while HAR has no
forecast (cycle 1) or < 1 price; the 300s in `√(τ/300)` is **the HAR
calibration cycle length**, not a magic constant — it becomes
`cycle_period_secs` in config, with the caveat (inherited from the Python doc
comment) that betas are only calibrated for the cycle length they were fitted
on.

### Generalization — configurable HAR windows (1_5_12 → 1_3_6, …)

Python hardcodes 3 windows [1, 5, 12]. The Rust port generalizes:
`windows = [w1, w2, …, wk]` (ascending), `beta = [b0, b1, …, bk]`
(intercept + one coefficient per window), buffer capacity `max(windows)`,
each term `mean(rv[-wi:])` over available entries. `windows=[1,5,12]` with the
existing betas is bit-for-bit the Python behavior (`mean(rv[-1:]) == rv[-1]`).
Changing to `[1,3,6]` is a config edit — but betas must be re-fitted for a new
window set, so the config keeps windows and betas adjacent and the module logs
both at startup.

## 2. Architecture

```
                      ┌───────────────┐
 price_feed ──NATS──▶ │  indicator    │ ──NATS──▶ trader (and anything else)
 price.binance.BTC    │  (new crate)  │  indicator.BTC
 price.poly.BTC       └───────────────┘  {"ts":…,"slot":…,"vals":{"vol_har":…,"p_up":…,"snr":…}}
```

- **Independent process.** Own binary, own config, own restart policy. If it
  dies, the trader keeps trading exactly as today (indicators are additive,
  never load-bearing until a gate is explicitly enabled).
- **Input:** subscribes `price.binance.<ASSET>` (same subjects/payloads the
  trader's `--nats-url` path parses — `{"ts":…,"price":…,"server_ts":…}`).
  `price.poly.<ASSET>` subscription is wired but unused by the initial three
  indicators; it's there so future indicators (spread/edge, OBI-style) get
  poly ticks for free.
- **1-Hz sampling grid.** NATS binance ticks arrive at ~4 Hz (250 ms sampler).
  The Python bot consumed a 1-Hz poll of the same feed, and the HAR/streaming
  math (rv_5s subsampling, √τ scaling of per-second vol) is calibrated at 1 Hz.
  The engine keeps the latest tick and appends **one price per whole second**
  to the cycle buffer — this is what makes Rust-vs-Python parity exact and
  keeps memory bounded (≤ cycle_period samples per asset).
- **Cycle clock:** `slot = floor(now / period) · period` (same as trader's
  `current_slot`). At the boundary: seal the old cycle into `rv_5s`, update the
  HAR forecast, set `cycle_open` = last known price at/before the boundary.
- **Output:** on every accepted binance tick (throttleable via
  `emit_interval_ms`), publish one JSON message per asset:

  ```json
  {"ts": 1784812345.201, "asset": "BTC", "market": "5m", "slot": 1784812200,
   "vals": {"vol_har": 0.000812, "p_up": 0.6113, "snr": 0.4479}}
  ```

  `vals` is an open map — new indicators appear as new keys, no schema change
  on the trader side. Not-ready values are simply absent (matching Python's
  `None`), except `p_up` which emits its defined warmup value 0.5.
- **No panics, no unwrap** (library code), `thiserror`/`anyhow` split, tokio
  runtime, bounded buffers, no locks across `.await`.

### Crate layout

```
indicator/
  Cargo.toml            # bin `indicator` + lib (unit-testable core)
  config/indicator.toml # runtime config (see below)
  src/
    lib.rs
    config.rs           # TOML parsing + validation (windows/beta length, ranges)
    engine.rs           # per-asset cycle state machine: 1-Hz grid, slots, emit
    indicators/
      mod.rs            # Indicator trait + registry
      har_vol.rs        # generalized HAR (rv_5s, windows, betas)
      p_up.rs           # HAR Student-t + streaming Gaussian modes
      snr.rs
    nats_io.rs          # subscribe price.*, publish indicator.<ASSET>
    main.rs             # clap: `run` (live) | `replay` (parity harness)
```

**Indicator trait** (the extensibility contract):

```rust
pub trait Indicator: Send {
    fn name(&self) -> &'static str;
    fn on_second(&mut self, sec_price: f64, ctx: &CycleCtx);  // 1-Hz grid
    fn on_cycle_boundary(&mut self, sealed: &SealedCycle);    // rv etc.
    fn value(&self, now: f64, ctx: &CycleCtx) -> Option<f64>;
}
```

Indicators that don't need NATS prices at all (external sources — funding
rates, weather, whatever) can be separate tokio tasks that publish into the
same `indicator.<ASSET>` (or `indicator.<TOPIC>`) subject space; the trader
side is source-agnostic by design (it just reads the `vals` map).

### Config (`indicator/config/indicator.toml`)

```toml
nats_url = "nats://localhost:4222"
assets   = ["BTC"]
market   = "5m"            # cycle period source: 5m/15m/1h-et/4h
emit_interval_ms = 250      # min gap between publishes per asset (0 = every tick)

[har_vol]
enabled  = true
windows  = [1, 5, 12]       # look-back cycles — the 1_5_12 ↔ 1_3_6 knob
min_ticks = 30              # cycle must have ≥ this many 1-Hz samples for rv
subsample_secs = 5          # rv_5s estimator step
[har_vol.beta]              # len = windows.len()+1; per-asset with default
default = [6.75316682223936e-05, 0.3808532101541894, 0.23010976882783898, 0.32151716443117506]
[har_vol.nu]
default = 4.2469

[p_up]
enabled = true
mode = "har"                # "har" (Student-t) | "streaming" (Gaussian)

[snr]
enabled = true
mode = "har"
```

Validation fails loudly at startup (beta length mismatch, non-ascending
windows, unknown market label) — same posture as `MarketDuration::parse`.

## 3. Trader integration (receive side)

Kept deliberately tiny and decision-neutral for this phase:

- `live.rs`: when `--nats-url` is set and the strategy TOML has
  `[indicator] enabled = true`, also subscribe `indicator.<ASSET>` on the
  existing NATS connection. Each message updates an
  `IndicatorStore` — `HashMap<asset, IndicatorSnapshot { vals: HashMap<String,f64>, ts, slot }>`
  behind an `Arc<RwLock>` (writes are ~4 Hz/asset; reads at decision points).
- Staleness guard: `max_indicator_age_secs` (default 5) — a gate must treat an
  older snapshot as absent. Slot guard: a snapshot from a previous slot is
  absent for entry purposes.
- **Phase 1 behavior: log-only.** At each entry decision the worker logs the
  current indicator snapshot (and it lands in the live log for later recon).
  No gate consults it. This is what makes the docker A/B a pure overhead
  measurement, and mirrors how v_shape was promoted (observe first, gate later).
- **Phase 2 (future, out of scope here):** config-driven gates, e.g.
  `p_up_gate_hp = 0.65` direction-adjusted, following the pup_gate study.
  The store's name-keyed map means new indicators need zero trader code —
  a future generic gate can be `[[indicator_gate]] name="p_up" min=0.65`.

## 4. Quality check — Rust vs Python parity

Same input ⇒ (near-)same output, before any live use:

1. **Input:** one recorded day of BTC from `price_feed`'s parquet
   (the same files bt4 consumes via `--source poly_rust`), resampled to the
   1-Hz grid, dumped to CSV `(sec_ts, price)`.
2. **Python reference:** small script (`scripts/indicator_parity_ref.py`, run
   with ../btc_5mins's venv, importing `bot.signals`) drives
   `VolHarSignal([b…], nu)`, `PUpSignal(vol_har)`, `SnrSignal(vol_har)`
   cycle-by-cycle over that CSV and writes
   `(slot, sec_ts, vol_har, p_up, snr)` per second.
3. **Rust:** `indicator replay --input ticks.csv --config …` runs the exact
   production engine code path (same structs, no test-only math) and writes the
   same CSV shape.
4. **Compare:** max abs diff per column. Expectation: `vol_har`/`snr` agree to
   ~1e-12 (identical arithmetic, f64); `p_up` to ~1e-9 (scipy `stdtr` vs the
   Rust Student-t CDF — `statrs`). Tolerance gate: 1e-6 absolute; anything
   worse is a bug, not "float noise". Results table goes into the perf report.

Plus normal unit tests in the crate: HAR window generalization
(`[1,5,12]` ≡ Python semantics incl. min_periods=1 warmup, `[1,3,6]` buffer
math), warmup returns (p_up=0.5, snr absent), streaming-mode formulas locked
against hand-computed vectors ported from Python, emit throttling, config
validation failures.

## 5. Performance test — docker A/B soak

New `docker-compose.perf.yml` (prod `docker-compose.yml` untouched; Oracle box
untouched):

- `nats`, `price-feed` (BTC, as prod)
- `indicator` (BTC, this feature)
- `trader-base`: `--dry-run --nats-url …` — today's code path, no indicator
- `trader-ind`: `--dry-run --nats-url …` + indicator subscription enabled

Both traders dry-run concurrently on the same feed, so they see identical
market conditions — a fair same-window comparison rather than sequential runs.
A sampler script polls `docker stats --no-stream` every 30 s into a CSV for a
multi-hour soak (target ≥ 3 h). Report:

- `indicator` container CPU% (avg/p95) and RSS — expectation: ≪ 1% of one
  core and single-digit MB (it does O(1) float math on ~4 msg/s/asset and one
  publish; the tokio runtime baseline dominates).
- `trader-ind` minus `trader-base` CPU/mem delta — expectation: noise-level
  (one extra ~4 Hz subscription, JSON parse ~200 bytes, map insert).
- NATS msg-rate before/after (`:8222/varz`) for context.

Deliverable: `indicator/doc/perf_indicator_docker_2026-07-18.md` with the
tables, the parity results from §4, and a go/no-go note for Oracle deploy.

## 6. Order of work

1. This plan doc (pushed). ✅
2. `indicator` crate: config + engine + three indicators + NATS io + replay
   bin + unit tests; `cargo fmt --check` / `clippy -D warnings` / tests green.
3. Trader: indicator subscription + store + staleness + log-only wiring + tests.
4. Parity run on a real recorded day; fix until within tolerance.
5. Docker perf compose + multi-hour soak + stats CSV.
6. Perf report md; README updates (module list + TODO for phase-2 gating);
   push.

## 7. Explicitly out of scope / notes

- No trading-decision change: no gate consumes indicators yet (phase 2).
- Betas/nu are consumed as config, not re-fitted here; changing `windows`
  requires a re-fit (`ml/features/har_beta.py`) — config comment says so.
- Non-5m markets: engine supports any `MarketDuration` period, but HAR betas
  are 5m-calibrated; running other periods logs a warning (same posture as the
  Python `har_pup_enabled` guard).
- `pre_vol` / `edge_spread` / `delta_pct_abs` from bt4 are not ported (trader
  already has delta_pct; edge needs poly+model; both are easy later additions
  via the Indicator trait).
