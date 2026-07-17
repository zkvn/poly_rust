# feature_new_markets — 15m / hourly-ET / 4h crypto + weather markets in the trader (2026-07-17)

**Status: plan (written independently before re-reading `plan_15_2026-07-08.md`; a cross-check
section against that older plan is appended at the end after this plan was finalized).**

Goal: let the live trader trade Polymarket **15-minute**, **hourly (60-minute)**, and
**4-hour** crypto up/down markets, and (as a separately-gated phase) **weather**
temperature-bucket markets — **without changing anything about how the current 5-minute
markets trade or backtest**. Every new capability must be *additive*: a config that doesn't
mention new markets, and a CLI invocation identical to today's, must produce a process that
is behaviorally indistinguishable from the current binary. Nothing on Oracle is touched in
this task (local Docker validation only; Oracle deploy is a later, separate step).

---

## 1. Verified market-family facts (checked live against Gamma, 2026-07-17)

| Family | Slug shape | Cycle boundary | Exists? |
|---|---|---|---|
| 5m / 15m / 4h | `{asset}-updown-{5m,15m,4h}-{slot}`, slot = epoch multiple of period | epoch-aligned (300/900/14400 s) | ✅ all three verified live today (`btc-updown-15m-1784266200`, `btc-updown-4h-1784260800`) |
| slot-based "1h" | `btc-updown-{1h,60m,1hr,hourly}-{slot}` | — | ❌ all four checked live today: 0 events (same result siglab found 2026-07-13) |
| hourly-ET (the real 60-min market) | `{coin_name}-up-or-down-{month}-{day}-{year}-{h}{am\|pm}-et` (e.g. `bitcoin-up-or-down-july-17-2026-1am-et`) | US-Eastern calendar hour; ET offset is a whole hour, so boundaries coincide with UTC hour boundaries (epoch multiples of 3600) | ✅ verified live today, same `["Up","Down"]` two-outcome shape |
| weather | `highest-temperature-in-{city}-on-{month}-{day}-{year}` — one *event* per city per day containing N mutually-exclusive temperature buckets (negRisk), each bucket its own Yes/No market | daily event, no intra-day cycle | ✅ (siglab has been paper-trading 51 cities since 2026-07-13) |

Consequences:

- **15m and 4h are almost free**: `marketdata::make_slug` / `current_slot` / `fetch_meta`
  already handle them if given the right suffix+period (siglab and `bin/shadow.rs` both
  prove this in production/practice — shadow already takes `--suffix`/`--period-secs`).
- **"60 minutes" = the hourly-ET family**, needing its own slug builder (port of
  `siglab/src/rotation.rs::Rotation::HourlyEt`, already battle-tested in siglab since
  2026-07-13) plus a `{ASSET} -> coin_name` map (bitcoin/ethereum/solana/xrp/dogecoin/bnb).
  Everything downstream of slug construction (fetch_meta, Worker, gates, Gamma resolution
  polling) is identical — confirmed by siglab, which runs real `Machine`s against these.
- **Weather is a genuinely different animal** (no Binance reference feed, negRisk buckets,
  resolution by station reading, no recorded tick history, no backtest possible). It gets
  its own phase, own config file, own engine, and ships **disabled by default** (§7).

## 2. Where the current code is 5m-bound (audit)

Everything else — `worker.rs`, `machine.rs`, `gates.rs`, `signal/*`, `execution.rs`,
Gamma resolution watcher, halt trackers — is already duration-agnostic: all cycle math
derives from `CycleContext { start_ts, end_ts }`, which the drivers construct. The
5m-bound spots are exactly:

1. **`bin/live.rs`** — the real work:
   - one global `--period-secs` (default 300) and one global `current_slot_val`;
   - hardcoded `make_slug(&asset, slot_now, "5m")` at the cycle-rotation site;
   - tick routing keyed by **asset only** (`assets.iter_mut().filter(|s| s.worker.asset ==
     asset)`) — fine while every slot for an asset trades the *same* market, wrong the
     moment BTC-5m and BTC-15m coexist (they have different CLOB tokens; a poly tick from
     one must never reach a worker for the other);
   - log/state filenames `live_trades_{asset}_{strategy}.csv` /
     `live_state_{asset}_{strategy}.json` — would collide across durations;
   - balance-check scheduling keyed to the global `period_secs`.
2. **NATS feed coverage** — `price_feed` publishes `price.poly.{ASSET}` for the **5m
   market's token only** (its 15m/4h subscriptions go to parquet, not NATS), and
   `price.binance.{ASSET}` (asset-level, duration-independent). So in NATS mode, non-5m
   workers have no poly feed.
3. **`backtest.rs` (lib)** — `CYCLE_LENGTH_S = 300.0` hardcoded in `replay_cycle`.
4. **`scripts/build_backtest_prices.py`** — reads `price_feed/raw/` (5m) only;
   `raw_15_mins/` and `raw_4hr/` exist on disk with the same schema but aren't wired.
5. **`config.rs`** — no notion of durations; per-asset maps only.

## 3. Design principles (the non-negotiables)

1. **The 5m path is frozen.** With an unchanged config and unchanged CLI, the new binary
   must: build the same `AssetSlot`s, in the same order, with the same file names; make
   the same NATS subscriptions; construct the same slugs at the same instants; resolve
   the same `AssetParams`; and produce **byte-identical backtest output**. Anything that
   can't be shown to preserve this is out.
2. **Additive config only.** Every new TOML field is `#[serde(default)]` (the exact
   pattern the 2026-07-17 v_shape fields used, including the "old pinned configs still
   parse" test). An old `strategy_*.toml` — including every historical file pinned by
   `backtest --config-file` for daily recon — parses and resolves identically.
3. **No `price_feed` / `poly-collector` changes.** Non-5m poly feeds come from direct
   CLOB WS subscriptions inside the trader (exactly what siglab does at 24-market scale,
   with the 2026-07-16 stale-merge guard already present in
   `trader::marketdata::spawn_poly_task`). NATS stays 5m-only. This also honors "don't
   interrupt any current Oracle process" — the collector is never rebuilt or restarted.
4. **One process, per-slot periods** — not one process per duration. Multiple processes
   would need N Telegram bot tokens (same-token `getUpdates` polling 409s — see README's
   trader-env-file section) and N wallets-worth of balance-guard confusion. The driver
   already multiplexes N (asset, strategy) workers; extending the key to (asset,
   strategy, duration) is the natural, smallest generalization.
5. **Weather is separately gated and off by default** (§7). New real-money surface with
   no backtest data and (per `studies/weather/weather_poly_2026-07-12.md`) no researched
   edge for price-action scalping doesn't ride in on a crypto-durations change.

## 4. Config design (`config.rs` + `strategy_*.toml`)

### 4.1 Which markets trade — `[market_durations]`

```toml
# NEW, optional. Which durations each asset trades. Key absent entirely, or asset
# resolving to ["5m"], == exactly today's behavior. Valid entries:
# "5m", "15m", "1h-et", "4h".
[market_durations]
default = ["5m"]
BTC = ["5m", "15m"]        # example: BTC additionally trades 15m
```

- `#[serde(default)] pub market_durations: HashMap<String, Vec<String>>` — missing table
  ⇒ empty map ⇒ every asset resolves to `["5m"]` via a hardcoded fallback (same
  fallback style as the v_shape fields).
- An asset must still be in `trade_assets` to trade at all; `[market_durations]` only
  widens *which markets* a trading asset trades. `deploy_oracle.py`'s `TRADER_ASSETS`
  (reads `trade_assets`) needs no change.
- Unknown duration string ⇒ startup error (fail loud, not silent skip).

### 4.2 Per-duration parameter overrides — `@duration` key suffix

All current strategy params were calibrated on 300-second cycles; several are
*absolute-seconds* values (`reversal_start_time`, `unwind_time_*`,
`enter_when_time_left`, `no_enter_when_time_left`, `balance_check_offset`), so 15m/1h/4h
markets will eventually need their own values. Mechanism: inside every existing per-asset
map, keys may carry an `@{duration}` suffix:

```toml
[reversal_start_time]
default = 120            # unchanged — applies to 5m exactly as today
"default@15m" = 400      # NEW: all assets on 15m markets
"BTC@15m" = 500          # NEW: BTC on 15m specifically
```

Resolution order for (asset, duration): `"{ASSET}@{dur}"` → `"default@{dur}"` →
`"{ASSET}"` → `"default"`. Implemented as a new `StrategyToml::resolve_for_duration(asset,
dur)`; for `dur == "5m"` it **skips the duration keys entirely and delegates to the
existing lookup path**, so 5m resolution provably cannot change (and `resolve(asset)`
stays as-is — every existing call site, including `backtest.rs` and the pinned-config
recon path, compiles and behaves untouched). `[strategies]` participates in the same
scheme, so e.g. 15m can run `reversal` while 4h runs nothing yet.

No new config file, no schema fork: old files remain valid; new keys are inert to old
binaries (they'd just sit in the map unmatched, since old lookups never ask for `@` keys —
but we won't ship configs with `@` keys until the new binary is deployed anyway).

## 5. Live driver changes (`bin/live.rs` + `marketdata.rs`)

### 5.1 `marketdata.rs` — additive helpers only

- `pub enum MarketDuration { M5, M15, HourlyEt, H4 }` with `suffix()` (`"5m"`, `"15m"`,
  `"1h-et"`, `"4h"`), `period_secs()` (300/900/3600/14400), `parse()`.
- `pub fn hourly_et_slug(asset: &str, slot: u64) -> String` — port of siglab
  `rotation.rs::HourlyEt` (ET calendar hour formatting + the coin-name map). `slot` is
  the UTC hour epoch (`current_slot(3600)`); ET offsets are whole hours so this is exact.
- `pub fn slug_for(asset, duration, slot)` dispatching to `make_slug` (slot families) or
  `hourly_et_slug`. **`make_slug`/`current_slot`/`fetch_meta` untouched.**

### 5.2 `AssetSlot` becomes (asset, strategy, duration)

- New fields: `duration: MarketDuration`, per-slot `slot_val: u64` (replacing the single
  global `current_slot_val`), per-slot `first_tick_seen: bool` for the startup mid-cycle
  guard (`should_suppress_startup_cycle` logic unchanged, evaluated per slot).
- Slot construction: for each asset × its `strategies` × its `market_durations`. 5m slots
  keep today's exact file names; non-5m slots get
  `live_trades_{asset}_{strategy}_{suffix}.csv` / `live_state_{asset}_{strategy}_{suffix}.json`
  (fresh files — no migration, nothing existing renamed).
- The 1s ticker branch iterates slots and rotates any slot whose
  `current_slot(slot.period_secs)` changed — for a 5m-only config this fires for all
  slots at the same instants the old global check fired, same `CycleContext`, same slug.
  (The old code already looped all slots inside one boundary check; the loop merely moves
  the boundary check inside.)
- `Worker` gets an optional display label (`asset` stays `"BTC"`; log lines/Telegram show
  `BTC/reversal@15m` for non-5m). `HaltTracker`, Gamma watcher, unwind logic: untouched —
  they're already per-worker and slug/ctx-driven.

### 5.3 Tick routing — poly keyed per market, Binance per asset

- The poly mpsc key changes from `asset` to a *routing key*: the NATS 5m subscription
  keeps sending plain `"BTC"`; direct per-market subscriptions send `"BTC@15m"` etc.
  A slot matches poly ticks iff `key == its own routing key`, where a 5m slot's routing
  key is the bare asset (in NATS mode) — so the 5m NATS path is bit-for-bit today's.
- Binance ticks stay keyed by asset and fan out to **all** durations of that asset
  (correct: one reference price; this is exactly siglab's `spawn_binance_broadcast`
  lesson — no extra Binance connections per duration, zero new Binance load in NATS mode).
- **Non-5m poly feeds are always direct CLOB WS** (`PolySub`), even when `--nats-url` is
  set; one subscription per (asset, duration) market per cycle, shared by every strategy
  slot on that market (kept in a small `HashMap<(asset, duration), PolySub>` rotated at
  that market's boundary — deliberately *not* per-slot, avoiding the known dormant
  per-worker duplication bug in the direct-WS path, `siglab/doc/incident_ws_2026-07-13.md` §3).
- Staleness note: `max_price_age_secs = 2.0` gates entries on tick freshness. 15m/1h/4h
  books tick slower than 5m books; that's *fine* (the gate blocks entry on stale data,
  which is the desired conservative behavior) but expect fewer eligible instants — a
  per-duration `@` override exists if tuning is ever wanted.

### 5.4 Telegram / status / control

- `/status` groups per slot with a duration tag on non-5m rows. `/halt BTC`,
  `/resume BTC`, `/reset_losses BTC` apply to **all** BTC slots (all durations) —
  documented, backward-compatible; per-duration control can come later if needed.
- Balance guard cadence stays scheduled off `args.period_secs` (300) — it's account-wide
  and time-based, not market-bound; per-cycle scoped halts already key off *Confirming
  workers*, which now naturally includes non-5m workers (labels gain the duration tag).

### 5.5 `--dry-run` (paper execution) — required for safe local validation

The local docker-compose mounts the **real** wallet env; running the live binary locally
would place real orders concurrently with Oracle's trader — the exact class of the
2026-07-03 double-process incident. Since this task's validation is "run the whole
afternoon locally in Docker", live.rs gains `--dry-run` (default **off**):
`execute()` short-circuits `PlaceBuy`/`ClosePosition` into simulated immediate fills at
the signal price (logged + counted normally, Telegram messages prefixed `[DRY]`), and
skips engine connection/balance polling (no auth needed). Zero code path shared with
real execution beyond the branch itself; flag absent ⇒ today's behavior. This is also
how the whole-afternoon soak can exercise entries on all durations without money.

## 6. Backtest changes — 5m byte-identical, new durations opt-in

1. **`backtest.rs`**: replace the `CYCLE_LENGTH_S` constant *usage* with
   `cycle_period_from_slug(slug)`: parses `-updown-{suffix}-` → 300/900/14400; any other
   shape (or parse failure) → 300.0 (today's constant). Existing 5m slugs parse to 300 ⇒
   provably identical replay. (Hourly-ET and weather have no recorded ticks — explicitly
   *not* backtestable; documented, not faked.)
2. **`build_backtest_prices.py`**: new `--source {5m,15m,4h}` (default `5m` ⇒ reads
   `raw/`, writes today's exact filenames). `15m`/`4h` read `raw_15_mins/`/`raw_4hr/` and
   write `{asset}_poly_15m_{date}.parquet` etc. — new names, nothing existing overwritten.
3. **`bin/backtest.rs`**: new `--duration` flag (default `5m` ⇒ today's filenames and
   behavior; `15m`/`4h` load the new filenames). `trade_reconcile.py` calls stay
   default ⇒ untouched.
4. **Parity gate (mandatory before merge)**: build old binary from `main`, new binary
   from the branch; run both over ≥3 recent dates × BTC/SOL/DOGE with `--format csv`
   (both latest-config and a pinned `--config-file`); **diff must be empty**. Same for
   `cargo test` (the 4 known pre-existing config-drift failures noted in README TODO
   excepted, unchanged in count and identity).

## 7. Weather markets — Phase B, own config, disabled by default

Weather cannot reuse `Worker`/`Machine` resolution (their cycle-close resolves by
Binance momentum — meaningless for a station-temperature market) and has no reference
feed for `delta_pct` gates. The proven design is siglab's: per-bucket, pure price-action
engine (dip-below-low then recover-above-high entry; SL / take-profit / max-hold-timeout
exits; **never** holds to real resolution, so no Gamma resolution dependency for PnL).

Plan (all additive, all inert unless explicitly enabled):

1. `trader/config/weather.toml` (own file — the strategy TOML schema is untouched):
   city list (start with **3–5 liquid cities**, e.g. nyc / london / seoul — not siglab's
   51), one reversal parameter set (from siglab's grid, picking the variant siglab's
   reports support best at enable time), `trade_size_usdc` (default 1.0), per-bucket
   max-1-trade, daily halt counter.
2. New CLI flag `--weather-config <path>`; **absent (the default, and the default in
   every compose/systemd invocation) ⇒ no weather code runs at all.**
3. New module `trader/src/weather.rs`: date-derived slug per city (port of siglab
   `weather.rs::today_slug`), Gamma event fetch → Yes-token buckets, **one batched WS
   subscription per city event** (the siglab CPU lesson: never per-bucket subscribe
   calls), one small state machine per bucket driving real FAK orders through the same
   `LiveExecutionEngine` (and honoring `--dry-run`).
4. Own log files (`live_trades_weather_{city}.csv`), own halt, own Telegram tags.
5. **Ship it config-disabled.** siglab's own research doc says the documented weather
   edge is forecast-latency arbitrage, *not* price reversals; siglab's paper grid is the
   evidence pipeline. Enabling weather live is a deliberate, separate config+review step
   (same "configured but not traded" pattern v_shape used on 2026-07-17). Local Docker
   dry-run validation of the plumbing **is** in scope for this task.

If implementation time gets tight, Phase B's cutline is: plumbing + dry-run validated,
real-order path code-complete but never enabled — the crypto durations (Phase A) are the
deliverable that must land whole.

## 8. Regression-safety analysis (what could break 5m, and why it can't)

| Risk | Mitigation |
|---|---|
| Config parse/resolve drift for old files | all new fields `#[serde(default)]`; `resolve()` untouched; `resolve_for_duration(_, "5m")` delegates to the old path; regression tests load `strategy_20260713.toml` (pre-change) and assert identical `AssetParams` |
| 5m slot behavior drift in live.rs | 5m routing key == bare asset (NATS path byte-identical); per-slot boundary check fires at identical instants for period 300; same slug, same filenames, same startup-guard semantics — plus a startup-log diff test (old vs new binary, same config: identical slot list and subscriptions) |
| Backtest drift | suffix-derived period == 300 for every existing slug; empty-diff parity gate over ≥3 dates × 3 assets (§6.4) |
| New durations starving the 5m loop (CPU) | non-5m adds only direct CLOB WS subs (the thing siglab runs 24× of at ~44% CPU *with* 51 weather cities); expect single-digit % locally; measured in the soak (§9) |
| Double-trading with Oracle during local tests | `--dry-run` only, never real credentials with real order path locally |
| Accidental prod enablement | new markets require **both** a new binary **and** explicit `[market_durations]`/`--weather-config` config; deploying the binary alone changes nothing |
| price_feed / collector interruption | zero changes to `price_feed`; NATS topology unchanged; nothing on Oracle restarted in this task |
| Telegram `getUpdates` conflicts | single process, single bot — unchanged |
| Log/state file collisions | non-5m filenames carry the duration suffix; 5m names unchanged |

Known accepted limitations (documented, not hidden):
- Hourly-ET and weather have **no backtest** (no recorded ticks). Extending `price_feed`
  to record hourly-ET is future work, deliberately out of scope (collector must not be
  touched).
- Default strategy params on longer cycles are 5m-calibrated (entry window = last ~120s
  of the cycle, 25s max hold) — safe/conservative but unvalidated as *good*; tuning goes
  through `@duration` overrides + siglab evidence later. Initial live enablement (when it
  eventually happens on Oracle) should start with **one** asset × **one** new duration ×
  size $1.
- Daily recon (`trade_reconcile.py`) will see non-5m trades in new CSV files it doesn't
  read yet; BT-reconciliation for 15m/4h needs the §6 pieces wired into recon later —
  flagged as a README TODO, not silently skipped.

## 9. Test & validation plan (local only)

1. **Unit tests** (new): duration parse/period map; hourly-ET slug formatting across
   am/pm + DST boundary dates + all six coin names; `resolve_for_duration` fallback
   chain (`ASSET@dur` → `default@dur` → `ASSET` → `default`) and its 5m-delegation;
   old-config (`strategy_20260713.toml`) resolves identically with the new code;
   `cycle_period_from_slug` for 5m/15m/4h/garbage; routing-key matching (BTC-5m never
   receives a BTC-15m tick and vice versa); per-slot boundary detection at 300/900/3600
   alignment instants.
2. **Full suite**: `cargo test` (trader + siglab both — siglab consumes trader as a lib
   and must keep compiling), `cargo fmt --all --check`,
   `cargo clippy --all-targets --all-features -- -D warnings`.
3. **Backtest parity gate**: §6.4 empty-diff requirement.
4. **Docker soak (the afternoon run)**: dedicated compose file (root
   `docker-compose.yml` untouched) running NATS + price-feed (BTC, as today) + the new
   trader in `--dry-run` with a test config: BTC+ETH × `["5m","15m","1h-et","4h"]` +
   weather dry-run on 3 cities if Phase B lands. Watch for: clean cycle rotation on all
   four boundaries (15m boundaries every 900s, ET-hour rollover at the top of the hour,
   one 4h rollover if the window allows), 5m dry-run decisions sane vs. Oracle's live
   log for the same cycles, zero cross-duration tick leakage (assert via log grep), no
   reconnect storms.
5. **Resource monitoring**: `docker stats` sampled every 30s to CSV for the whole
   afternoon; pass criteria (Oracle is a small aarch64 box): trader container
   steady-state **< 25% of one core** and **RSS < 300 MiB with no unbounded slope**
   (plateauing steps tolerated per siglab's allocator findings; a monotonic climb is a
   blocker). Also verify the price-feed container numbers are unchanged vs. its current
   baseline (nothing we did should touch it at all).
6. **Repeat-restart test**: restart the trader container mid-cycle several times —
   startup mid-cycle suppression must engage per-duration (a restart 40s into a 5m cycle
   suppresses the 5m slot but, e.g., 40s into a fresh 4h cycle is *within* the guard
   window for the 4h slot only if genuinely fresh).

## 10. Deliverables / rollout

- Code: `config.rs`, `marketdata.rs`, `bin/live.rs`, `backtest.rs`, `bin/backtest.rs`,
  `scripts/build_backtest_prices.py`, (+ Phase B: `weather.rs`, `config/weather.toml`),
  new tests throughout.
- Docs: this plan; README "new markets" section (market families, config knobs,
  `--dry-run`, what's enabled where — i.e. *nothing* new enabled in prod yet); README
  TODO entries for the known gaps (§8 limitations).
- Config: **no change to the live `strategy_20260717.toml`** in this task. New-market
  enablement ships later as its own dated config revision after Oracle deploy + review.
- Oracle: untouched. Later deploy path (out of scope here): normal
  `./scripts/deploy_trader.sh` flow; binary-first (inert), config-second (enables),
  one asset × one duration × $1 first.

---

## 11. Cross-check against `trader/doc/plan_15_2026-07-08.md` (appended after independent draft)

Read only after §1–§10 were finalized. The old plan (15m only, 2026-07-08, never
implemented) + its DeepSeek review (2026-07-10) vs. this plan:

**Where both plans independently agree** (good confidence signal): the exact 5m-bound
call sites (`live.rs`'s hardcoded `make_slug(..., "5m")`, `backtest.rs`'s
`CYCLE_LENGTH_S`, `build_backtest_prices.py`'s `RAW_DIR`); worker/machine/signals/config
being already duration-agnostic; the NATS gap (old plan pins it precisely:
`collect.rs:1239`'s 15m bba task passes `None` for the NATS client — only the 5m task
publishes); and the recommendation to feed non-5m markets by **direct CLOB WS, not by
extending price_feed** (old plan's "Option A", this plan's §3.3).

**The fundamental design difference — and why this plan deliberately departs:** the old
plan proposed a *separate process per duration* (own config dir `config_15m/`, own
`trader-live-15m.service`, own log dir). Its own review then found the two blocking
consequences of that design: **(a)** shared-wallet `BalanceGuard` cross-contamination
(one product's drawdown halts the other — DeepSeek #1/#9, Claude-confirmed against
`balance.rs`, "should block go-live"), and **(b)** the Telegram `getUpdates` 409 race
(second process silently loses all remote control — DeepSeek #5, Claude-confirmed,
already documented in `live.rs`'s own header). This plan's single-process/multi-slot
design (§3.4) dissolves both by construction: one `BalanceGuard`, one Telegram loop, one
wallet, N market slots. The old plan's §2.4 deploy/systemd work (parallel unit, parallel
deploy flags, doubled restart-discipline surface) also disappears entirely.

**Old-plan material adopted into this plan after the cross-check:**

1. **The §1.1 slot-aliasing bug is worth restating as a design constraint**: 900/3600/
   14400 are exact multiples of 300, so a mismatched (period, suffix) pair *successfully*
   resolves a real 5m market and silently trades it on the wrong clock. DeepSeek #3's fix
   (never let period and suffix be two independently-settable values) is already this
   plan's design — `MarketDuration` binds suffix↔period in one enum (§5.1), and there is
   deliberately **no** standalone `--slug-suffix`/`--period-secs` pair for new markets —
   but the *why* now cites this failure mode explicitly.
2. **15m parameter seeding has real prior data** (missed by this plan's "defaults are
   unvalidated" hand-wave): `../btc_5mins`'s `studies/15_mins/` +
   `studies/bt2/summary_reversal_with_unwind_2026-07-04.md` found `high_prob` at 15m
   strong (~100% win rate in a ~19-day window, all 6 assets, best `enter_when_time_left`
   ≈ T-20→25 — close to the 5m default, i.e. **not** proportionally ×3 scaled) while
   `reversal` at 15m fired 0–5 times ("inconclusive"). When 15m is eventually enabled:
   seed `@15m` overrides from those studies, re-validate through the §6-parametrized Rust
   backtest against the (now much longer) `raw_15_mins` history, and prefer
   `"default@15m"` strategies = `high_prob`-style conservatism — reversal at 15m stays
   off until walk-forward evidence supports it (consistent with the newer 5m
   reversal-only precedent, per DeepSeek #10).
3. **`api_probe.rs` also hardcodes `"5m"`/`300`** (old plan §2.1) — diagnostic-only, fix
   opportunistically; noted so it isn't rediscovered.
4. **Kill-switch clarity** (DeepSeek #8): in the single-process design, `/halt {asset}`
   halts all of that asset's durations; per-duration disable = remove it from
   `[market_durations]` + restart. Documented in §5.4/README rather than left implicit.

**Old-plan items overtaken by events / deliberately not adopted:** the separate
`config_15m/` schema-fork (rejected — §4's `@duration` keys keep one schema, one file,
addressing DeepSeek #6's schema-drift concern directly); `--period-secs` on
`bin/backtest.rs` (this plan derives the period from each slug instead — strictly safer,
can't disagree with the data being replayed, and covers mixed-duration files for free);
and the old plan's §2.4 deploy changes (unneeded — same binary, same unit, config-gated).

---

## 12. Implementation, soak, and deploy record (same day, appended post-hoc)

**Implemented 2026-07-17** (commits `7b58270` code, `4a9075e` dry-run fill-pricing fix,
`73c04c4` enablement config): everything in §4–§7 as planned, plus two things the plan
missed that testing found:

1. **Binance slug re-bucketing for non-5m backtests** (`backtest.rs::
   rebucket_binance_slugs`): `raw/`'s Binance rows carry *5m* slugs; `run_backtest`
   groups both feeds by slug, so the first real 15m replay silently produced zero
   trades (every cycle "had no Binance data"). Re-keyed by each tick's own timestamp
   into the duration's slots. Caught because the loose-config validation run
   suspiciously returned "No trades." against 137 candidate dip-recover patterns.
2. **Dry-run close pricing**: `SimExecutionEngine::close_position` returns
   `filled_usdc: 0.0` (fine for unit tests, which only check state transitions) — the
   soak's first trade booked a ~flat TIMEOUT exit as pnl −1.02. Dry-run-only re-pricing
   at the held side's last mid (crypto) / triggering tick's mid (weather).

**Backtest parity gate (§6.4): PASSED, 18/18 byte-identical**, twice (before and after
the re-bucketing change) — old binary from pre-change `main` vs new, dates 07-14/15/16 ×
BTC/SOL/DOGE × {latest config, pinned `strategy_20260713.toml`}, `--format csv` diff.

**Docker soak (§9.4–9.6): PASSED** — ~4h dry-run, dedicated host-network compose
(host→bridge port publishing turned out broken on this box — docker-proxy accepts TCP
but forwards nothing; root compose untouched), BTC×{5m,15m,1h-et,4h} + ETH×{5m,15m} +
weather×{nyc,london,seoul} (33 buckets):
- 475 resource samples: trader CPU avg 2.15% / max 5.37% (Ryzen core), memory rose
  12.5→21 MiB then plateaued (allocator working set, not a leak). Zero errors/panics,
  120 cycles.
- All four duration families rotated on their own clocks; hourly-ET slugs resolved live
  four consecutive hours; first 4h cycle opened at 16:00 HKT.
- Cross-duration leakage: none — same-instant heartbeats showed four different CLOB
  prices for BTC's four slots (0.4650/0.4750/0.7450/0.7200) with one shared Binance ref.
- Full dry-run round-trips: BTC-5m, and ETH@15m entry 0.895 → TIMEOUT exit 0.92,
  pnl +0.0149, logged to `live_trades_eth_reversal_15m.csv`.
- Mid-cycle container restart: per-slot startup suppression engaged correctly
  (the 4h slot reported 11002s into its open cycle).
- Weather: discovery + batched subscription proven; no entries in the window (thin
  books — expected).
- Oracle CPU translation (measured, not assumed): Oracle is a 2-core Neoverse-N1;
  using price_feed (runs on both machines) as the yardstick, ~⅓ Ryzen-core per
  N1-core ⇒ soak's worst-case config ≈ 5–11% of one Oracle core. Post-deploy measured:
  ~0–4% with the actual enablement.

**Deploy (2026-07-17 ~20:00 HKT)**: `deploy_trader.sh` (build + config + restart;
poly-collector untouched). Between plan and deploy, a parallel same-day config update
(`2ceb4b0`) had already trade-enabled all 6 assets on 5m — so the §10 "one asset × one
duration" rollout became: keep everything as that update left it, add **ETH = ["5m",
"15m"]** only, with `"ETH@15m"` keys pinning the soak-validated morning params (the
plain ETH keys now hold that update's different 5m calibration — the @dur mechanism
doing exactly the job §4.2 designed it for). Post-deploy verified: exactly one live
process, 9 slots including `ETH@15m -> strategy=reversal`, per-slot startup
suppression, 14.6 MiB RSS. That update also recalibrated values three config-pinned
tests assert — updated same commit (the `fix_config_test_drift_2026-07-15.md` pattern).

Not enabled anywhere: weather (`--weather-config` unset), BTC/SOL/BNB/XRP/DOGE 15m,
1h-et, 4h.
