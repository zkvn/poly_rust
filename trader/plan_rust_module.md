# Plan: Migrate live trading to Rust (modular)

Status: **in progress**, branch `trader-a1`. See §12 for the up-to-date per-step
status; short version:

- **Track A:** A0-A3 done (scaffold, A1 state machine + bt1 golden, A2 shadow
  live feeds — validated live, caught/fixed a real feed-filtering bug, A3
  Telegram + config_log).
- **Track B:** B1 done (execution.rs + balance.rs), B3 done (unwind.rs +
  redemption.rs — redemption's on-chain execution step intentionally
  unimplemented pending its own design/go-ahead). B2's balance/auth check ran
  live against production ($7.84 USDC confirmed); the live order-placement
  half is **deferred until `worker.rs` exists** — the first real order should
  be a genuine strategy-driven trade, not a synthetic test (user preference).
- **Not started:** `worker.rs` (the live typestate machine — now the critical
  path item, since it unblocks both live trading and B2's real order),
  `main.rs` supervisor, `tradelog.rs`, §13 market generalization.

Goal: gradually move the **live-trading path** of `btc_5mins` from Python to
Rust, organised into clean modules — a **trade-API** module (order submission,
balance, redemption, all CLOB account writes), a **strategy** module (signals +
reading the strategy config), and a **Telegram** module (runtime control +
status), which is a first-class part of the system, not an afterthought. The
Rust bot reads the **same** `config/strategy_*.toml` as the Python bot.
**Backtesting stays in Python** and remains the correctness oracle.

Three cross-cutting requirements drive the design:
- **Telegram is critical** and must be migrated (control *and* display), not left
  straddling Python.
- **The signal layer must be extensible** — adding a new signal later (e.g. a HAR
  volatility signal feeding a future strategy) must not require touching existing
  signals or the worker wiring beyond one registration point.
- **Every module ships its own test suite** — no module is "done" without tests.
- **Expandable across markets** — the same engine must trade other durations
  (15m / 60m / 4hr / 1d crypto up/down) and arbitrary pass-in slugs, with
  auto-rotation when the market is periodic. **Market period is a parameter, never
  a hardcode** (see §13).

---

## 1. Why this is tractable (the load-bearing findings)

Two facts from studying the codebase make this smaller than it looks:

1. **The live strategies depend on only four trivial signals.**
   `ReversalStrategy` and `HighProbStrategy` (`bot/strategies.py`) consume only
   `SawLowSignal`, `LatestPolySignal`, `DeltaPctSignal`, `LatestBinanceSignal`
   (plus `EntryDirectionSignal`, whose guard is **off** by default).

   The expensive machinery — `PUpSignal`, `SnrSignal`, `VolHarSignal` (HAR
   forecast + Student-t CDF) — is **logging-only enrichment** (`p_up`, `snr`,
   `vol_har5`, `edge_spread` CSV columns). **No trade decision reads it today.**
   So the *initial* Rust strategy layer is simple arithmetic + time windows — but
   the architecture must still make HAR-class signals first-class for the future
   (see §3, extensibility).

2. **The Rust Polymarket SDK already does everything the trade API needs.**
   `polymarket_client_sdk_v2` (already a dependency of `../poly_rust`, features
   `ws,clob,rtds`) exposes authenticated market orders (FAK), limit orders
   (GTC/GTD), `cancel_order`/`cancel_orders`/`cancel_all_orders`,
   `balance_allowance`, and the CLOB/USER WebSocket channels. `../poly_rust`
   already implements slug rotation, Gamma metadata → token IDs, Binance/Poly WS
   feeds, Chainlink RTDS, and Arrow/Parquet writing. We **reuse** that.

Net: the risky, novel work is concentrated in **order execution + early exits
(stop-loss/unwind) + halt accounting + Telegram control**, not in signals or feeds.

---

## 2. Where the code lives

**Recommendation:** add a new binary crate **`trader`** to the existing
`../poly_rust` workspace (sibling of `price_feed`), reusing its feed,
market-discovery, and parquet code. The strategy TOML stays in
`btc_5mins/config/` and is read by path (the Python bot already references
`../poly_rust/.../price_feed` from `bot/config.py`, so cross-repo pathing is an
established pattern here).

Status legend below: ✅ built+tested, ◐ partial/stubbed, ✗ not started yet.

```
poly_rust/
  price_feed/          # existing — feeds, parquet, chainlink (reused)
  trader/              # NEW crate — the live bot
    src/
      main.rs          # ✗ CLI + Supervisor: per-asset tasks, event routing, shutdown
      types.rs         # ✅ shared: TradeIntent, TradeResult, ticks, CycleContext, enums

      # ── MODULE 1: strategy (signals + config) — pure, no I/O ──
      config.rs        # ✅ load strategy_*.toml (serde), per-asset getters, ASSET_CONFIG
      signal/          # ✅ signal layer — trait + one file per signal (extensible)
        mod.rs         #   ✅ Signal trait, SignalSet registry, TickBus fan-out
        saw_low.rs     #   ✅
        latest_poly.rs #   ✅
        latest_binance.rs # ✅
        delta_pct.rs   #   ✅
        enrich.rs      #   ✗ optional: p_up / snr / vol_har (HAR) — deferred per §15.6
      strategies.rs    # ✅ ReversalStrategy, HighProbStrategy → TradeIntent
      gates.rs         # ✅ cross-strategy gates: spread, delta, staleness, max_price, halt
      halt.rs          # ◐ HaltTracker exists inside backtest.rs; not yet extracted into
                       #   a standalone module worker.rs can share for live sessions

      # ── MODULE 2: trade_api (orders + balance + account) — all CLOB writes ──
      execution.rs     # ✅ ExecutionEngine trait + live CLOB impl + sim impl:
                       #   market BUY (FAK), limit SELL (GTC), market SELL close,
                       #   cancel; settle retries. sim impl drives backtest + tests
      balance.rs       # ✅ BalanceGuard (per-cycle drawdown halt) + balance fetch
      redemption.rs    # ◐ fetch/classify done+tested; on-chain redeem txn (RedeemExecutor)
                       #   intentionally unimplemented pending its own go-ahead
      unwind.rs        # ✅ USER-channel WS watcher for GTC fill notifications

      # ── MODULE 3: telegram (runtime control + status) ──
      telegram/        # ✅
        mod.rs         #   ✅ bot task, long-poll loop, auth (allowed chat ids) — built,
                       #      not wired into anything live (needs a bot token)
        commands.rs    #   ✅ command parsing → Command enum (/set /halt /params …)
        control.rs     #   ✅ Command → ControlMsg routed to workers over mpsc
        render.rs      #   ✅ status/params/delta formatting (display)

      # ── glue: read-only data + orchestration ──
      marketdata.rs    # ✅ slug/slot, Gamma meta → token IDs, Binance+Poly WS → ticks.
                       #   klines REST for cycle open/close not yet added (shadow/backtest
                       #   both use Binance-tick-based outcome, matching bt1)
      config_log.rs    # ✅ append-only JSONL snapshot (schema-identical to Python;
                       #   verified byte-for-byte against a real Oracle log line)
      worker.rs        # ✗ NEXT UP — per-(asset,strategy) typestate machine (§7/§8):
                       #   events → step → transition; orchestrates signals/gates/exec;
                       #   writes state/{asset}_{strategy}.json for crash recovery.
                       #   `machine.rs` (below) is its offline-only precursor.
      machine.rs       # ✅ backtest-only decision core (Watching/Holding/Halted; instant
                       #   fills). worker.rs wraps this with the full live state set.
      backtest.rs      # ✅ replay driver: parquet → Event stream → same machine +
                       #   sim venue; golden-PnL parity vs Python run_backtest (§11)
      tradelog.rs      # ✗ trades_*.log CSV (schema-identical to Python) + latency log
```

Requested split honoured: **`execution`/`balance`/`redemption`/`unwind` =
trade-API module**; **`config`/`signal`/`strategies`/`gates`/`halt` =
signal/config module**; **`telegram` = control module**. `worker`/`marketdata`/
`config_log`/`tradelog` are the glue that wires them together.

---

## 3. Module 1 — strategy (signals + config), built for extension

Pure, side-effect-free, unit-testable by feeding tick sequences.

### Extensible signal layer (the key design point)

The signal layer is the part most likely to grow (HAR vol, new features). It is
built around a trait so new signals drop in without editing existing ones:

```rust
pub struct CycleContext { pub start_ts: f64, pub end_ts: f64, pub open_binance: f64 }

pub trait Signal: Send {
    fn name(&self) -> &str;
    fn reset(&mut self, ctx: &CycleContext);
    fn on_binance(&mut self, _t: &BinanceTick) {}   // default no-op
    fn on_poly(&mut self, _t: &PolyTick) {}         // default no-op
}
```

- Each concrete signal lives in its **own file** under `signal/` and implements
  `Signal`, overriding only the tick stream it cares about (mirrors the Python
  `subscribe_binance` / `subscribe_poly` split).
- A **`TickBus`** fans `BinanceTick` / `PolyTick` to registered signals (port of
  `bot/tick_bus.py`). Adding a signal = new file + one registration line in the
  worker's signal-construction function.
- Strategies hold **typed handles** to the specific signals they need (e.g.
  `Rc<RefCell<SawLowSignal>>` within the single owning asset task, or an
  `Arc<Mutex<…>>` if shared across tasks). A strategy that wants a future HAR-vol
  signal just takes another typed handle — no change to unrelated code.
- **Typed accessors return `Option<f64>` for not-ready state** — honouring the
  repo's "Zero Means Zero" rule. `0.0` is a real value; warmup returns `None`.

**Future HAR-vol path is pre-wired:** `enrich.rs` will host `PUpSignal`,
`SnrSignal`, `VolHarSignal`. Initially they may be stubs that emit empty CSV
columns, but they implement the same `Signal` trait and subscribe via the same
`TickBus`, so promoting one to a *decision* input later is purely additive: add
its config fields, give the strategy a handle, branch in `evaluate`. The
Student-t CDF (`scipy.special.stdtr` analogue) is isolated to `enrich.rs` and
validated against a Python golden when implemented (e.g. `statrs::StudentsT`).

### config.rs
- Load the **latest** `config/strategy_*.toml` by glob+sort (same rule as
  `bot/config._load_strategy_toml`). Deserialize with `serde` + the `toml` crate.
- "Per-asset dict with a `default` key" → `HashMap<String, T>` + one helper
  `get(map, asset) -> map.get(asset).copied().unwrap_or(map["default"])`.
- Port the pydantic validators as explicit checks (ranges, `default` presence,
  `price_high > price_low`) returning `anyhow::Error`.
- `ASSET_CONFIG` (ws_symbol, slug_prefix, kline_symbol, chainlink_symbol) as a
  `static` table. Secrets (`POLY_PRIVATE_KEY`, proxy URL) from env, not the TOML.
- Config is held in an `Arc<RwLock<…>>` so the Telegram `/set` path can mutate it
  live (see §5) and the per-asset getters re-read each cycle.

### strategies.rs
- `TradeIntent { side, entry_type, up, dn, binance_price }`.
- `ReversalStrategy::evaluate` / `HighProbStrategy::evaluate` — direct ports
  (~15 lines each). `fired` / `mark_fired` / `reset` — one-trade-per-strategy-per-
  cycle guard. Direction-bias gate stays inside the strategy.

### gates.rs + halt.rs
- `common_gates(intent) -> GateOutcome` ports `worker._common_gates`: trade-asset
  membership, halt, manual halt, spread band, per-strategy `delta_pct` filter,
  staleness (`max_price_age_secs`), `max_buy_price`, `price_high_rev`.
- `SessionLossTracker` (per strategy) + `_check_daily_halt_reset`. This is the
  **accounting that decides money risk** — port carefully against the README
  "Stop-loss & halt accounting" contract, including `correct_result`.

### Tests (this module)
- `config.rs`: parse the real latest TOML; per-asset getter fallback to
  `default`; each validator rejects the documented bad input.
- `signal/*`: per signal, feed a synthetic tick sequence and assert the accessor.
  `SawLow` window-boundary cases (dip just inside vs just outside `[end_tl,
  start_tl]`); not-ready returns `None`.
- `strategies.rs`: each strategy emits the right `TradeIntent` under its
  conditions and `None` otherwise; the fired-guard suppresses a second intent.
- `gates.rs`: a table-driven test asserting each rejection reason
  (`HALTED`/`SKIP_PREMIUM`/`SKIP_DELTA`/`SKIP_STALE`/`SKIP_MAX_PRICE`/
  `SKIP_REV_PRICE`/`WATCH_ONLY`) and the pass case.
- `halt.rs`: loss counting reaches threshold → halted; daily reset clears at the
  configured HKT hour; `correct_result` flips a provisional WIN→LOSS and re-halts.

---

## 4. Module 2 — trade_api (orders + balance + account)

All Polymarket CLOB **writes** and account reads live here. Strategy code never
imports the SDK; it only produces `TradeIntent`s.

### execution.rs — `ExecutionEngine`
Defined as a **trait** with a real CLOB impl and a mock impl (so the worker and
tests don't need a live exchange):

```rust
#[async_trait]
pub trait ExecutionEngine: Send + Sync {
    async fn place(&self, intent: &TradeIntent, up_id: &U256, dn_id: &U256) -> TradeResult;
    async fn place_limit_sell(&self, token: &U256, shares: Decimal, price: Decimal) -> (Option<String>, SellStatus);
    async fn close_position(&self, token: &U256, shares: Decimal) -> CloseResult;
    async fn cancel_limit_sell(&self, order_id: &str) -> bool;
    async fn cancel_all(&self) -> bool;
}
```

- `place` — FAK market BUY: slippage added to midpoint, capped at
  `max_buy_price`, per-retry slippage escalation, fill = `takingAmount`,
  `cost = size/filled` (port `trading._place_order`). Dry-run when no key.
- `place_limit_sell` — GTC resting SELL for unwind TP; **retry on `balance: 0`**
  (on-chain settlement lag, README §unwind).
- `close_position` — FAK market SELL for stop-loss; retry on "no orders found" /
  "not enough balance".
- **Partial fills (DeepSeek 1.3):** `TradeResult`/`CloseResult` report
  `filled_shares` / `sold_shares` distinctly from the requested amount. `place`
  returning `filled < requested` puts the machine in `Holding{filled}`; an exit
  SELL returning `sold < shares` leaves a residual `Holding{shares − sold}` (§8).
  The **sim impl** must be able to model partial fills so the golden replay covers
  the branch.
- SDK mapping: `client.market_order()…build_sign_and_post(&signer)` /
  `client.limit_order()…`; FAK ⇄ `OrderType::Fak`, GTC ⇄ `OrderType::Gtc`.
- **Proxy:** on Oracle, CLOB writes route via the EC2 proxy (`clob_proxy_url`,
  e.g. `http://10.8.0.7:8888`) — configure reqwest with the proxy here.

### balance.rs — `BalanceGuard`
Wake at +120 s into each window; first wake sets baseline; halt all workers if
drawdown > 25%; `reset_baseline()` on `/resume`. Reuses the authenticated client.

### Account setup gotcha — `POLY_SIGNATURE_TYPE` (found 2026-07-02)

Polymarket accounts are not all the same on-chain wallet type, and getting this
wrong is **silent** — `get_balance_allowance` doesn't error on the wrong
`signature_type`, it just returns `balance: 0, allowances: 0`, which looks
identical to "account not funded." There is no error to grep for.

- `SignatureType`: `0=Eoa`, `1=Proxy` (Magic Link/email login), `2=GnosisSafe`
  (browser-wallet-connected), `3=Poly1271` (EIP-1271 smart-contract-wallet
  signatures — e.g. accounts funded via an EIP-7702 "smart EOA" transaction).
- The `btc_5mins` Python bot's account (and every account before 2026-07-02)
  is `signature_type=1`, hardcoded throughout that codebase — it has never
  exercised any other type.
- A new account added 2026-07-02 turned out to be `signature_type=3`. It took
  going on-chain directly (raw ERC20 `balanceOf` against Polymarket's actual
  collateral token, **`pUSD`** = `0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb`,
  6 decimals, on Polygon — *not* native USDC `0x3c499c54...` or bridged USDC.e
  `0x2791Bca1...`, both of which read 0 even for the funded old account) to
  prove the deposit had genuinely landed, then brute-forcing all 4
  `signature_type` values against `get_balance_allowance` until the number
  matched, to find this.
- **Fix:** `execution.rs::signature_type_from_env()` reads `POLY_SIGNATURE_TYPE`
  (0-3) from the env, defaulting to `Proxy` (1) for backward compat with every
  existing account/config. **Every new Polymarket account added to a `.env`
  must have its `signature_type` verified this way before trusting any
  balance/order result** — don't assume `Proxy` just because it's the default.

### redemption.rs, unwind.rs
- `redemption.rs` — periodic auto-redeem of resolved winning positions (port
  `bot/redemption.py`).
- `unwind.rs` — subscribe to the Polymarket **USER** WS channel; callback on GTC
  fill (port `bot/unwind_watcher.py`). The `shares < 5` price-monitor fallback
  lives in `worker.rs` (reacts to poly ticks).

### Tests (this module)
- `execution.rs`: dry-run `place` returns expected `(shares, cost)` for given
  size/price/slippage/cap; retry escalates the limit toward `max_buy_price`;
  `place_limit_sell` retries on a simulated `balance: 0` and stops on a non-zero
  balance error — all via the **mock client**, no network.
- `balance.rs`: drawdown > 25% fires `on_halt` exactly once; `reset_baseline`
  re-arms; a failed fetch skips without halting (fail-open).
- `redemption.rs`: positions are grouped/filtered correctly from a fixture
  response. `unwind.rs`: a fixture USER-WS fill message triggers the callback.

---

## 5. Module 3 — telegram (runtime control + status)

Telegram is migrated fully to Rust. It is both a **control plane** (mutates live
config/halt state) and a **display** (status, params).

- **Transport:** `teloxide` (full-featured) or a thin `reqwest` long-poll loop if
  we want minimal deps — recommend starting with the long-poll loop for
  simplicity and adding `teloxide` only if command ergonomics demand it.
- **Auth:** allow-list of chat ids from env (same secret model as today).
- **Commands → `Command` enum** (`commands.rs`): `/set <param> <val> [asset]`,
  `/halt`, `/resume`, `/reset`, `/strategies …`, `/params`, `/delta`, `/status`,
  `/trade_assets`, … — the surface in `bot/telegram_bot.py`.
- **Control channel (`control.rs`):** the Telegram task owns no worker state. It
  sends a `ControlMsg` over a `tokio::mpsc` channel to the per-asset workers
  (the Rust analogue of the Python `command_queue`). Each worker applies it via a
  `apply_control` / `set_param` method that **propagates baked-in values to live
  signal/strategy objects** (the exact set documented in `worker._set_param`:
  `enter_when_time_left`, `no_enter_when_time_left` → both strategies + both
  `SawLow.end_tl`, `price_low`/`price_high` → strategy + entry-dir signal,
  `reversal`, `reversal_low_threshold`, `halt_prob`/`halt_rev` → trackers), then
  **writes a `config.log` snapshot** (`config_log.rs`) so recon stays valid.
- **Display (`render.rs`):** reads current state. Workers publish a lightweight
  `StatusSnapshot` (W/L per strategy, current prices, halt state) into an
  `Arc<RwLock<…>>` or a status mpsc that the Telegram task formats on demand —
  replacing the Python bot's read of `config.log` + in-process state.
- **Invariant kept:** config mutations flow **worker → config_log snapshot**, and
  Telegram is the producer of commands + consumer of status — never writing
  config files directly (mirrors the Python "config persistence invariant").

### Tests (this module)
- `commands.rs`: parse each command string into the right `Command` (including
  per-asset `/set … BTC` and malformed-input rejection).
- `control.rs`: a `ControlMsg` for `set_param reversal 0.6 BTC` routed to a mock
  worker mutates config and live strategy handle and emits one snapshot.
- `render.rs`: a fixed `StatusSnapshot` formats to the expected text (golden).

---

## 6. Glue — marketdata, worker, config_log, tradelog

- **marketdata.rs:** reuse `../poly_rust/price_feed` patterns — `current_slot`,
  `make_slug`, `fetch_meta` (Gamma → UP/DN token IDs + volume), Binance WS, Poly
  CLOB WS (best_bid_ask → midpoint, price_change fallback), Chainlink RTDS. Add
  **klines REST** for cycle open/close prices (outcome determination).
- **worker.rs:** the per-asset cycle loop (`_run_cycle`): resolve cycle-open
  Binance price → build `CycleContext` → `reset` all signals/strategies → entry
  loop (heartbeat, `evaluate`, gates, `execution.place`, `mark_fired`, record
  placement) → stop-loss/unwind monitoring → outcome → write CSV row → confirm
  via API. One `tokio` task per asset; Supervisor in `main.rs` owns shutdown and
  an optional TUI. Holds the mpsc receiver for Telegram `ControlMsg`s.
- **config_log.rs:** append-only JSONL, **schema-identical to Python** so
  `snapshot_to_bt_overrides` / recon keep working.
- **tradelog.rs:** `trades_*.log` + `latency_*.log` with the **exact CSV schema**
  in the README — the integration contract with the Python analysis stack.

### Tests (glue)
- `marketdata.rs`: `current_slot`/`make_slug` math; `fetch_meta` parses a Gamma
  JSON fixture into the right UP token id + volume.
- `config_log.rs`: a written snapshot round-trips and matches the Python field
  set (golden line). `tradelog.rs`: the CSV header equals the README schema
  exactly; one row serialises with the right column order and empty-for-`None`.
- `worker.rs`: an integration test drives one synthetic cycle end-to-end with a
  mock `ExecutionEngine`, asserting an intent → placement → CSV row.

---

## 7. Adopting the `order_trade_machine` state-machine architecture

I studied `../order_trade_machine` (20k LoC IB options bot). Its core is a clean
**typestate machine** and is the strongest template available for the principle
you stated — *modular, testable, expandable, with clean unambiguous state*. We
should **copy the architecture, not the state set.**

### What it does (the reusable pattern)

- **States are an enum of structs**, each carrying only its own data:
  `TradeState = NotReady | ReadyToStart | ToEnter | TradeStarted |
  ToChangeExposure | ToExit | TradeCompleted`.
- **One method per state:** `handle(event, &mut context) -> Transition` where
  `Transition = Stay | Goto(TradeState)`. Dispatch is a single `match` in
  `TradeState::step`. Adding a state = add a variant + its `handle` + a `label`.
- **A single unified `Event` enum** drives everything:
  `Event = Risk | Price | Signal | Order | ExpirySettle`. The machine never polls;
  it only reacts to events. This is the key decoupling — *the machine is pure
  logic over (state, event) → state*, independent of where events come from.
- **Context holds injected trait-object boundaries**, kept as peers the machine
  orchestrates:
  - `OrderManager` (the "desk") owns working orders + a `Box<dyn Venue>` —
    the execution boundary (backtest sim vs live IB).
  - `TradeLedger` (the "book of record") owns positions/PnL + a
    `Box<dyn TradeRepository>` — the persistence boundary (SQLite vs stub).
  - Tests inject `StubVenue` / `StubTradeRepo` and drive the machine with a
    scripted `Vec<Event>` — no exchange, no DB. This is exactly the testability
    the plan wants.
- **States are `Serialize`/`Deserialize`** → the machine can be persisted and
  resumed after a crash (`resume_from_ib`). For us this enables clean restart
  recovery of an open position mid-cycle.

### Can it be copied? — Yes, the skeleton; no, the states

The *mechanism* (enum-of-state-structs + `handle→Transition` + `Event` enum +
injected `Venue`/`Repository` traits + serializable state) ports almost verbatim
and should be the backbone of `worker.rs`. But the *specific states* are
IB-options-specific (brackets/OCA, scaling via `ToChangeExposure`, 0DTE
`ExpirySettle`, IB resume) and don't match Polymarket up/down. Our lifecycle is
**per `(asset, strategy)`** (reversal and high-prob fire independently, so each
is its own small machine instance — mirroring its `strategy_ticker_machines`
vector):

```
Watching ─signal+gates pass→ Entering ─fill→ Holding ─cycle end→ Resolved
   │                            │ fail         │  ├─ TP hit ──→ Unwinding ─┘
   └─ halted/not-trade-asset    └─→ Watching   │  └─ SL hit ──→ StopExiting ┘
      → Halted                                 └─ (one trade per strat/cycle)
```

Concrete state set to design in A1 (machine) / I2 (early-exit states):
`NotReady | Watching | Entering | Holding | Unwinding | StopExiting | Resolved |
Halted`, with the trade-API calls (FAK buy, GTC unwind, FAK close) behind a
`Venue`-style `ExecutionEngine` trait (already proposed in §4) so the same
machine runs against a mock in tests and the real CLOB live. Our `Event` enum
(full spec in §8):
`Event = PolyTick | BinanceTick | Cycle(Open/Close) | Order(Fill/Reject) |
UnwindFill | ApiResult | Control(Telegram) | Balance`.

**There is no `ExpirySettle`** — that was order_trade_machine's stock/0DTE
option-settlement event and is irrelevant here. Its place is taken by **`ApiResult`**:
the async Polymarket market-resolution confirmation (the Python
`api_result_watcher`) that arrives *after* the cycle ends and can **flip a
Binance-estimated WIN↔LOSS** ("RESULT CORRECTED" in the live log), correcting the
halt accounting. That async-confirmation event is real and load-bearing here; a
settlement-at-intrinsic-value event is not.

This makes the worker’s logic a pure, table-testable `(state, event) → state`
function — the "clean unambiguous state" benefit — and folds the existing
stop-loss/unwind/halt special cases into explicit states instead of scattered
flags (`_rev_*`, `_normal_*`, `_manual_halt`) as in the Python `worker.py`.

### What we do *not* copy

SQLite persistence (`rusqlite`/`r2d2`) — our book of record is the CSV trade log
+ `config.log` JSONL, which the Python recon stack already consumes. IB/`ibapi`,
options/brackets/OCA, scaling, 0DTE settlement — none apply. **Nor the
`TradeRepository` trait** — per the DeepSeek review it is mild over-engineering
here: the Python recon stack already reads the CSV trade log directly, so a plain
append-only logger in `tradelog.rs` is sufficient (no DB-abstraction trait). The
one persistence we *do* add is the small restart-safe `TradeState` snapshot for
crash recovery (§8, "Persistence & crash recovery") — that is in-flight machine
state, not a book-of-record abstraction.

---

## 8. State machine specification

Grounded in the production live log (`log/live_*.log`, e.g. 2026-06-29) and the
README trade lifecycle. **One machine instance per `(asset, strategy)`** —
reversal and high_prob run independently, each with its own halt budget and its
own one-trade-per-cycle guard.

> **Revised per the DeepSeek review (bottom):** manual-halt/balance are **no-entry
> gates, not a freeze**; the unwind **GTC-vs-FAK fallback** and **partial fills**
> are explicit; `Confirming` is **scoped to held outcomes**; and **in-flight state
> is persisted** for crash recovery. These were the five top-priority fixes.

### States

| State | Meaning | Live-log signature |
|---|---|---|
| `NotReady` | market not loaded / cycle too short at load / asset not in `trade_assets` | load failures; `WATCH_ONLY` |
| `Watching` | cycle active, no position; signals updating each tick | `T- 62s \| $553.90 \| UP=.. \| DOWN=..` heartbeat; `Entry skipped — …` |
| `Halted` | **Watching with entry suppressed** (loss-limit / manual `/halt` / balance) — **no open position** | `🛑 STOP LOSS ACTIVE (2/2) — skipping.` |
| `Entering` | FAK BUY submitted, awaiting fill | `BUY DOWN reversal @ 0.835` → `✅ Order placed … status: matched` |
| `Holding{shares, exit_arm}` | filled; position open; unwind-TP + stop-loss armed | `Unwind GTC skipped (<5 min) — monitoring poly price @ 0.82`, or a resting GTC |
| `Unwinding{shares}` | take-profit crossed; SELL in flight | GTC fill / price-monitor → `UNWIND DN ↓ \| BNB` |
| `StopExiting{shares}` | stop-loss floor crossed; FAK SELL in flight | `🛑 Stop loss SELL … status: matched` |
| `Resolved` | cycle ended; outcome WIN/LOSS/STOPLOSS/UNWIND; CSV row written | `TRADE WIN/LOSS \| …` |
| `Confirming` | **held WIN/LOSS only** — async `ApiResult` may flip result + halt | `API result updated […]` / `RESULT CORRECTED` |
| `EnrichOnly` | **STOPLOSS/UNWIND only** — `ApiResult` fills the `api_result` column, never `pnl`/`result`/halt | `API result updated […]` |

`Holding` carries `shares: f64` and an **`exit_arm`** (the explicit form of the
Python unwind hybrid path, README §unwind):
- `GtcResting { order_id }` — shares ≥ 5: a resting GTC limit SELL at TP on the CLOB.
- `PriceMonitor { tp_price }` — shares < 5: watch `PolyTick`, FAK-sell on TP cross.

The stop-loss floor is always armed in `Holding` regardless of `exit_arm`.

### Manual halt & balance are NO-ENTRY GATES, not a state (DeepSeek 1.1, 1.6)

The log shows a `/halt` firing **while a reversal was filled and a price-monitor
unwind was in progress** — and the exit still proceeded (facts-pack Example A). So
**halt/balance never abort an in-flight machine.** Model them as a per-strategy
`entry_suppressed` flag, set by `Control(/halt)`, the loss-limit tracker, or a
`Balance` drawdown, and consumed **only at the `Watching → Entering` edge**:

- `Watching` + `entry_suppressed` → `Halted` (a labeled no-entry `Watching`;
  signals keep updating; **no position involved**).
- `Entering / Holding / Unwinding / StopExiting` **ignore** the flag for exit
  purposes and keep managing the position to completion.
- A FAK BUY is effectively atomic (fill-or-kill in ~1 s), so a halt arriving during
  `Entering` just lets that order resolve; the flag suppresses the **next** entry.
  No mid-FAK cancel exists — this answers DeepSeek 1.6 by the order semantics
  rather than a cancel that can't happen.
- `/resume` or the daily loss-reset clears the flag (`Halted → Watching`). The
  daily reset clears the **loss-limit** part only — never a manual `/halt`.

### Events

| Event | Source | Drives |
|---|---|---|
| `Cycle(Open)` | slot timer (`period_secs`, §13) | `*→Watching`; reset signals/strategy; halt-reset-hour check |
| `Cycle(Close)` | slot timer | `Holding/Watching→Resolved`; outcome from Binance open vs close |
| `BinanceTick` | Binance WS (1 Hz) | `delta_pct`; `p_up`/`snr` (enrich) |
| `PolyTick` | Poly CLOB WS (sub-s) | `saw_low`, `latest_poly`, `spread`; TP/SL price-monitor while `Holding` |
| `Order(Fill{filled_shares}\|Reject)` | `ExecutionEngine` (FAK BUY) | `Entering→Holding{shares}` / `Entering→Watching` |
| `UnwindFill{sold}` | USER-WS watcher or price-monitor crossing | `Unwinding→Resolved` / partial → `Holding` |
| `ApiResult` | Gamma/CLOB resolution watcher (async) | `Confirming` (flip) or `EnrichOnly` (column only) |
| `Control` | Telegram mpsc | `/halt`,`/resume` → set/clear `entry_suppressed`; `/set` live params |
| `Balance` | BalanceGuard (+120 s/cycle) | drawdown > 25% → set `entry_suppressed` (does **not** abort open positions) |

### Transition table (per `(asset, strategy)`)

```
NotReady    --Cycle(Open)+ready-->                  Watching
Watching    --signal & gates pass & !suppressed-->  Entering            (place FAK BUY; mark_fired)
Watching    --entry_suppressed (halt/manual/bal)--> Halted
Watching    --Cycle(Close)-->                       Resolved            (no trade → no row)
Halted      --Cycle(Open) & (/resume | loss-reset)->Watching
Halted      --Cycle(Close)-->                       Resolved            (no row)
Entering    --Order(Fill{shares})-->                Holding{shares,arm} (arm: see below)
Entering    --Order(Reject)-->                       Watching            (ORDER_FAILED)
   on fill: shares≥5 → place_limit_sell → GtcResting{order_id};
            shares<5 → PriceMonitor{tp_price};  stop-loss floor always armed
Holding     --PolyTick px≥TP / UnwindFill-->         Unwinding{shares}   (GTC fill, or FAK sell)
Holding     --PolyTick px≤SL floor-->                StopExiting{shares} (cancel resting GTC first, then FAK close)
Holding     --Cycle(Close)-->                        Resolved            (held WIN/LOSS; cancel resting GTC)
Unwinding   --sell fully matched-->                  Resolved            (UNWIND; pnl = proceeds − stake)
Unwinding   --sell partial (sold<shares)-->          Holding{shares−sold}(residual; DeepSeek 1.3)
Unwinding   --sell failed-->                          Holding             (reclassify; hold to maturity)
StopExiting --sell fully matched-->                  Resolved            (STOPLOSS; halt += 1 loss)
StopExiting --sell partial-->                         Holding{shares−sold}
StopExiting --sell failed (exit_fill=failed)-->      Holding             (reclassify as held WIN/LOSS)
Resolved(held WIN/LOSS)    --spawn-->                 Confirming          (api_result pending)
Resolved(STOPLOSS/UNWIND)  -->                        EnrichOnly          (api_result column only)
Confirming  --ApiResult match-->                     done
Confirming  --ApiResult mismatch-->                  done                (correct_result, halt fix, CSV rewrite, TG push)
EnrichOnly  --ApiResult-->                            done                (fill api_result column; never pnl/result/halt)
(in-flight) --Control(/halt) | Balance-->            set entry_suppressed; NO state change
```

### Partial fills (DeepSeek 1.3)

`Order(Fill)` carries `filled_shares`; `Holding.shares` is the **actual** fill and
every exit size derives from it. An exit SELL returning `sold < shares` goes back
to `Holding{shares − sold}` (residual managed, or held to maturity), **not**
`Resolved`. The sim `ExecutionEngine` must be able to model partial fills so the
golden replay exercises this branch.

### Confirming vs EnrichOnly (DeepSeek 1.5)

Only **held WIN/LOSS** rows spawn `Confirming`, where an `ApiResult` WIN↔LOSS flip
runs `correct_result` and fixes halt accounting. **STOPLOSS/UNWIND** rows go to
`EnrichOnly`: the `ApiResult` fills the `api_result` CSV column (counterfactual for
STOPLOSS, confirmatory for UNWIND) but **never** rewrites `pnl`/`result`/halt
(`enrich_only=True` in the Python). Both run concurrently with the next cycle.

### Persistence & crash recovery (DeepSeek 1.4)

Serializable states alone are not enough — the **dynamic in-flight data** must be
persisted. After **every transition**, atomically write the machine's `TradeState`
— including `shares`, UP/DN token-ids, the resting GTC `order_id`, and the
`exit_arm` mode — to a restart-safe file (`state/{asset}_{strategy}.json`). On
startup, reload it and **reconcile against the CLOB** (query open orders + token
balances) before resuming: a `Holding{GtcResting}` whose order is gone but whose
token balance is present resumes as `PriceMonitor`; a zero-balance position is
`Resolved`. Without this, a crash during `Holding` with a resting GTC loses the
order and leaves the position open on the exchange. (This is the Polymarket
analogue of `order_trade_machine.resume_from_ib`.)

### Invariants (load-bearing — README + live log + DeepSeek review)

- **One trade per strategy per cycle** — `Watching→Entering` fires once; `fired`
  set immediately on placement.
- **Halt/balance never abort an open position** — they are no-entry gates only.
- **A failed early-exit is not an exit** — a failed unwind/stop SELL returns to
  `Holding`; the cycle-end reclassifies it as a held WIN/LOSS (`exit_fill=failed`).
- **Exit sizes track actual filled shares; a partial exit leaves a managed residual.**
- **STOPLOSS pnl is final** (`proceeds − stake`); the later `ApiResult` is
  counterfactual (EnrichOnly). **UNWIND** `ApiResult` is confirmatory (EnrichOnly).
- **Halt accounting** — a successful STOPLOSS counts as a loss; a failed stop the
  API later resolves WIN is undone. The daily reset clears loss counters but not a
  manual `/halt`.
- **Shares zeroed under a lock before any sell thread** — whichever of TP/SL fires
  first owns the position (`Holding` has exactly one exit path).
- **In-flight state persisted after every transition and reconciled with the CLOB
  on restart.**

Every scattered flag in the Python `worker.py` (`_rev_*`, `_normal_*`,
`_manual_halt`, the unwind hybrid path, the failed-stop reclassification, the
async correction) becomes an explicit state, field, or transition above —
**table-testable by feeding a scripted `Vec<Event>` and asserting the state path +
emitted `ExecutionEngine` calls, with no live I/O.**

---

## 9. Architecture: channels vs a message bus (Redis/NATS)

Your question: should data and signal be *totally* separated over a channel, or
even via Redis/NATS — or is that overkill?

### Recommendation: in-process async channels. A broker is overkill here.

`order_trade_machine` already answers this by example: it separates concerns with
**plain in-process channels**, no broker —
- a price-producer thread → `mpsc` → the engine loop (data → consumer),
- the venue → an `mpsc<MyOrderEvent>` → the engine (fills/rejections back in),
- the engine → a `watch` channel → the HTMX/SSE dashboard (status out).

We adopt the same shape with `tokio` channels, which fit the async WS feeds:

```
 Binance WS  ─┐                              ┌─→ worker task (asset A) ─┐
 Poly CLOB WS ─┼─ feed tasks ─ broadcast/mpsc┼─→ worker task (asset B) ─┼→ ExecutionEngine
 Chainlink WS ─┘   (ticks)                   └─→ worker task (asset …) ─┘     (CLOB)
                                                     ▲   │
                       Telegram task ─ mpsc<Control> ┘   └─ watch<Status> → TUI / Telegram
                       venue/order events ─ mpsc<OrderEvent> ─→ worker
```

- **Data fully separated from signal/decision:** feed tasks own *only* I/O and
  emit ticks; signal+strategy+machine live in the worker task and consume ticks.
  This is the "totally separated over a channel" you asked about — and it makes
  the decision path testable by feeding a scripted tick/event stream (no live
  sockets), exactly as `order_trade_machine`'s stub tests do.
- **One process, one host (the Oracle box), 3–6 assets, 5-min cycles.** tokio
  `broadcast` (fan one feed to N assets), `mpsc` (commands/order events → worker),
  and `watch` (latest status → TUI/Telegram) cover every need with zero new infra.

### When Redis/NATS would (and wouldn't) earn their keep

A broker buys you cross-process/cross-host pub-sub, durable replay, and multiple
independent consumers. We have **none of those needs**:

| Need a broker solves | Our situation |
|---|---|
| Many processes/hosts sharing a stream | Single process; tasks share memory for free |
| Durable event replay / event sourcing | Recovery is per-position serialized state (§7) + CSV/parquet already on disk |
| Multiple independent consumers of one feed | `tokio::broadcast` fans out in-process |
| Decouple producer/consumer deploy cycles | One binary, one deploy |

Costs it would add: an external service to run/monitor on the Oracle box, a
network hop + (de)serialization on the hot path (latency matters — see the
`latency_*.log` instrumentation), and a new failure mode (broker down = bot
blind). **Net: overkill.** The only genuine cross-process boundary today is the
CLOB proxy (Oracle → EC2), and that is already a plain HTTP proxy, not a bus.

### The part that *is* worth taking regardless of transport

The decoupling that matters is the **`Event` enum + `(state, event) → state`
machine**, not the wire. Because the machine only consumes `Event`s, the
transport is swappable: today a direct `mpsc`; if the price-feed collector and
the trader were ever split into separate processes, the *same* machine would
consume the *same* events arriving over a local socket or NATS — without touching
its logic. So: **build the event model now, keep the transport in-process, and
leave the door open** rather than paying for a broker up front. If a future need
appears (e.g. the `../poly_rust/price_feed` collector becomes the sole feed and
multiple bots subscribe), revisit with NATS then — it slots in behind the same
`Event` boundary.

---

## 10. Sync vs async — why tokio at all, and keeping the machine runtime-agnostic

**The state machine does not need async.** `step(event) -> transition` is pure
synchronous logic over data — it behaves identically whether events arrive from a
sync `mpsc::recv()` or an async stream. `order_trade_machine` proves this: its
engine loop is **fully synchronous** (a `std::thread` price producer →
`std::mpsc` → a sync loop; `ibapi` is used with its `sync` feature), and `tokio`
is only an *optional* dependency for the HTMX web dashboard. The machine is
runtime-agnostic.

**So why tokio here at all? One reason: the Polymarket SDK and the WS feeds are
async-native.** `polymarket_client_sdk_v2` returns futures/streams; the Binance/
Poly WebSockets and `reqwest` are async. Something must host a runtime to talk to
them. tokio is **not** for the decision logic — it is for the I/O shell.

### Recommendation — functional core, imperative shell

- The **state machine + signals + strategies + gates are sync and
  runtime-agnostic.** They never `.await`. This is the part that must be
  deterministic and trivially testable.
- **tokio is confined to the live driver:** async feed tasks read the WS streams
  and push `Event`s into a channel; an order-executor runs the async SDK calls.
  The `ExecutionEngine` trait is **sync in signature** — the sim impl is pure-sync
  (backtest + tests), and the live impl bridges to the async SDK via a tokio
  runtime `Handle::block_on`, exactly as `order_trade_machine`'s sync engine
  drives its (sync) IB venue.
- The **backtest driver needs no runtime at all** (§11) — it replays parquet →
  events → `step` synchronously. Same machine, zero tokio.

### Trade-off

| | Sync core + async shell (recommended) | Fully async (tokio everywhere) |
|---|---|---|
| Machine testability | Pure, no runtime, deterministic | Tests must spin a runtime; async "colouring" |
| Backtest harness (§11) | Trivial, deterministic, fast | Must carry a runtime just to reuse the engine |
| Talking to the async SDK | Bridge via `block_on` in the live venue | Natural `.await` |
| Concurrency, 3–6 assets | per-asset thread, or async feed layer → sync engine | per-asset tokio task |
| Cognitive load | Two clear layers (core vs shell) | One model, but async leaks everywhere |
| Latency | `block_on` adds no real overhead; ≤1 order/asset/cycle | identical |

The decisive factor for this project is **bit-exact backtest parity** (§11): a
sync, runtime-free core replays deterministically and is the only clean way to
reproduce the Python golden exactly. Going fully async would force the validation
harness to carry a runtime and would lose that determinism for no benefit — order
throughput is trivial (a handful of FAK orders per cycle), so async concurrency
buys nothing on the decision path.

### Can the machine work with either? Yes — that's the point

Because `handle` is a pure `(state, event) -> transition` function, the same
machine runs unchanged under either model. Only two things differ between sync and
async: **how events get in** (sync `mpsc` vs async stream) and **how side effects
go out** (sync `ExecutionEngine` vs `.await`). Keeping the `ExecutionEngine` trait
sync makes the machine identical in backtest (sim venue), tests (mock venue), and
live (CLOB venue) — the live venue is the only place async is bridged.

One real constraint to respect: `Handle::block_on` panics if called from *inside*
a tokio worker thread. So the sync engine must run on its **own thread** (fed by a
channel the async feed layer writes to) — the `order_trade_machine` shape — rather
than inside a tokio task. Concurrency across assets is then either (a) one engine
thread per asset, or (b) a single engine loop over all assets (matches
`order_trade_machine`). Both keep the engine tokio-free; pick at A2 (open
decision #2).

---

## 11. The Rust backtest replay engine — first build, and the genuine test

*Clarification vs §16:* the **Python** backtest stays the reference oracle and
keeps its numba/cuda sweep infra. What we build in Rust is a **replay harness**
whose only job is to prove the Rust live engine reproduces the Python results —
not to replace the Python sweeps.

This is the **first thing built after scaffolding**, because it is simultaneously
both things you asked for:

1. **Reproduction of known results** — parity with the Python `run_backtest`
   golden *is* the correctness proof.
2. **A genuine integration test of the engine + state machine** — the full
   signal → strategy → gate → **state-machine** pipeline, exercised entirely
   offline and deterministically, before any live wiring exists.

It falls out almost for free because the machine is event-driven and sync (§7,
§10): a backtest is just *the same machine fed a different event source*.

- Replay recorded `prices/{ASSET}_{binance,poly}_*.parquet` (+ book parquet) in
  timestamp order → emit `Event::BinanceTick` / `Event::PolyTick` /
  `Event::Cycle(Open/Close)` exactly as the live feeds would. The poly/book files
  are read by the market's **suffix** (§13) so a 15-min golden reads 15-min data;
  binance is shared across durations.
- Drive the per-`(asset,strategy)` machine with the **sim `ExecutionEngine`**
  (fills at the recorded book/price; unwind/stop resolved against the replayed
  ticks) — the venue-swap pattern from `order_trade_machine` (sim venue in
  backtest, CLOB venue live).
- Accumulate the trade log + PnL and **assert it matches the Python `scripts/bt1.py`
  result** for the same asset + window + config (see "How the golden is built" below).

### How the golden is built (use `bt1` as the reference)

`scripts/bt1.py` is the per-strategy backtest the team already trusts; for one
asset/window/config it emits a per-trade table and a footer —

```
Total trades : 12  (wins=10, losses=0, stoplosses=2, win_rate=83.3%)
Total PnL    : $2.2054
  reversal  : 11 trades  wins=9  stoplosses=2  PnL=$2.1359
  high_prob :  1 trades  wins=1  stoplosses=0  PnL=$0.0695
```

Procedure:
1. Pick a fixed `(asset, date-window, config)` — start with **BTC, one full HKT
   day, the live `config/strategy_*.toml`** (reversal-only is the simplest path).
2. Run `bt1` (i.e. `bot.backtest.run_backtest`) → capture its **per-trade rows**
   (`cycle_start, side, outcome, pnl`) and **Total PnL / win counts**.
3. Run the Rust replay over the **same parquet files** + config.
4. **If the results are similar** — same trade sequence (slug/side/outcome) and
   Total PnL within a tiny float epsilon — **lock the `bt1` numbers as the golden**
   (per-asset/window constants in the Rust test, exactly like the Python numba
   parity tests' `_GOLDEN_*_PNL`).
5. Thereafter any drift in the state machine or signals breaks the golden.

> **Verify tick ordering first (DeepSeek risk).** The golden only holds if the Rust
> replay feeds ticks in the **exact same temporal order** as the Python tick-bus —
> any difference in how binance and poly ticks are merged/interleaved shifts the
> cycle-open price, `delta_pct`, `saw_low` latch, and entry windows, and silently
> breaks parity. So a Phase-A1 sub-task is to **reproduce the Python merge/ordering
> rule** (`bot/backtest.py` `load_price_data` + tick replay) bit-for-bit, and
> assert the *first divergent event*, not just the final PnL, when a golden fails —
> that makes ordering bugs debuggable rather than a mystery PnL delta.

> **Note — discard the `order_trade_machine` golden.** Its `random_walk`
> regression is locked to `TOTAL PNL -343.20`, but that machine trades **IB
> stocks/options** — the number is entirely irrelevant here. We borrow only the
> *discipline* (one locked PnL guards the whole pipeline), not the value. Our
> oracle is `bt1`.

Expand the golden set incrementally: add a window that exercises **STOPLOSS** and
one that exercises **UNWIND** (both visible in the `bt1` table above) so the
`StopExiting` / `Unwinding` states are covered, plus a high_prob window once that
strategy is enabled for the chosen asset.

What this buys the migration:

- The machine + signals are validated against a known-good oracle **before any
  live wiring, any orders, any tokio** — pure offline determinism.
- Every later change (a new state, a new signal, the HAR-vol promotion) re-runs
  the replay and must keep the golden — the same safety net the Python side uses.
- The live driver (A2 onward) then only has to prove that *live feeds deliver the
  same events the replay already validated*, shrinking live risk to I/O wiring,
  not decision logic.

Scope: this harness replays one config over one window for **parity/regression**,
not the large parameter sweeps — those stay in Python's numba/cuda engines (§16).
A fast Rust sweep engine is a separate, later question, out of scope here.

---

## 12. Phased migration — two parallel tracks

The work splits into **two tracks that proceed independently** and only meet at
integration. This is deliberate: the decision engine (Track A) is pure offline
logic validated against `bt1`, while the trade-API module (Track B) is pure
exchange I/O validated against the live CLOB. They share **only** the
`ExecutionEngine` trait + the `Event`/`TradeIntent` types, so they can be built by
different sessions/people at the same time and tested in total isolation.

```
Track A (decision engine, offline)          Track B (trade API, live I/O)
A0 ✅ Scaffold + config load          ║  (depends only on the ExecutionEngine trait + types)
A1 ✅ State machine + bt1 golden      ║  B1 ✅ execution.rs live CLOB impl + balance
A2 ✅ Shadow live feeds (sim venue)   ║  B2 ◐  API live test (balance/auth done; order deferred)
A3 ✅ Telegram control + config_log   ║  B3 ✅ unwind/redemption (redeem txn itself deferred)
        └──────────────┬─────────────╜──────────────┘
                       ▼
        I1 Integrate: A's machine drives B's live ExecutionEngine — one asset live (held only)
        I2 Early exits live (Unwinding/StopExiting) ·  I3 Full cutover
        (I1-I3 blocked on worker.rs, not yet started)
```

Each step is independently shippable and reversible; the Python bot keeps running
until cutover. **Every step lands with its module's tests.**

### Track A — decision engine (offline, no exchange)

| Step | Scope | Risk gate | Status |
|---|---|---|---|
| **A0 Scaffold** | `trader` crate; `config.rs` loads live TOML; `types.rs`; CI (`clippy -D warnings`, `test`, `fmt --check`). | Rust prints the same parsed params as Python for all assets. | ✅ Done |
| **A1 ★ State machine + bt1 golden** (§8, §11) | `signal/` + `strategies` + `gates` + the §7/§8 typestate machine + **sim `ExecutionEngine`** (with partial-fill support), driven by parquet→`Event` replay. **No live I/O, no tokio.** Must implement the DeepSeek-revised §8: no-entry-gate halt, explicit GTC/FAK arming, partial-fill residual, `Confirming`/`EnrichOnly` split, and `TradeState` crash-recovery persistence. | (1) Reproduces `scripts/bt1.py` per-trade rows + Total PnL for the chosen asset/window — lock as golden, after verifying tick ordering (§11). (2) Unit tests assert each revised edge case from a scripted `Vec<Event>`: `/halt` mid-`Holding` does **not** abort the exit; <5-share fill → `PriceMonitor`; partial exit → residual `Holding`; STOPLOSS/UNWIND → `EnrichOnly` (pnl untouched); restart reloads `Holding` and reconciles. **This is the genuine test that the engine + state machine work.** | ✅ Done — bt1 golden locked (BTC 2026-06-20, 1 trade, UNWIND, pnl=0.0355), 92 tests pass. `machine.rs`'s state set is the *backtest* simplification (Watching/Holding/Halted only, matching bt1's instant-fill assumption) — the full Entering/Unwinding/StopExiting/Confirming/EnrichOnly set + crash persistence is `worker.rs`'s job for live trading, not yet built. |
| **A2 Shadow live feeds** | Point the validated machine at live WS feeds via the async shell (§9/§10); **sim venue, no real orders**. Log would-be `TradeIntent`s beside Python. | Over ≥3 days, live-fed intents match Python placements within tick jitter — live feeds emit the same events the A1 golden validated. | ✅ Built + running (`marketdata.rs`, `bin/shadow.rs`). Found and fixed a real bug: unfiltered `price_change` batch entries from other tokens corrupted prices and caused instant false stop-losses. Fixed version ran 1h15m+ clean (zero false trades) on live BTC/ETH/DOGE feeds. Multi-day comparison run still ongoing/optional. |
| **A3 Telegram + config_log** | `telegram/` + `config_log` + control channel driving a shadow machine (control + status only). | `/set`/`/halt`/`/status` mutate live state, emit correct `config.log` snapshots; Python Telegram retire-able. | ✅ Done — `config_log.rs` schema verified byte-for-byte against a real Oracle `log/config.log` line. `telegram/{commands,control,render,mod}.rs` built with full command parser + tests; `TelegramBot::run_loop` makes real API calls but is not wired into anything live yet. Live smoke-tested 2026-07-02 via new standalone `bin/telegram_probe.rs` against a fresh bot (`oracle_rust_bot`): `send` confirmed delivered (`ok:true`), `poll_once`/`getUpdates` confirmed working (no errors, no 409 conflict once switched off the old bot token that a still-running poller — likely the Oracle Python bot — held). `TELEGRAM_BOT_TOKEN` in `trader/.env` updated to the new bot; still needs wiring into `worker.rs`/`bin/live.rs` and a go-ahead before controlling a live run. |

### Track B — trade-API module (live, totally independent)

Built against **only** the `ExecutionEngine` trait — no signals, no state machine,
no strategy config. Its own test binary places/cancels real orders directly.

| Step | Scope | Risk gate | Status |
|---|---|---|---|
| **B1 ★ execution + balance** | `execution.rs` live CLOB impl (FAK BUY, GTC SELL, FAK close, cancel) + `balance.rs`, behind the trait. Dry-run + a standalone `bin/api_probe`. | Auth + balance fetch succeed; a signed FAK order is accepted by CLOB validation (post + immediate cancel). | ✅ Done — `SimExecutionEngine` + `LiveExecutionEngine<S: Signer>` both implement the trait; `bin/api_probe.rs` built. |
| **B2 API live test** | Drive `api_probe` against the **real CLOB** with $1 orders on a live up/down market: place → confirm fill shape (`takingAmount`/`makingAmount`/`status:matched`) → cancel/close. Verify settle-lag retries (`balance: 0`). | Round-trips a real order and a real cancel; fill/parse/retry paths exercised end-to-end against production. | ◐ Partial — `api_probe balance` ran live 2026-07-01 (auth OK, $7.84 USDC, 3 allowances; also fixed a `/1e6` base-units bug in the balance display). Re-run 2026-07-02 against a new account (fresh `POLY_PRIVATE_KEY`/`FUND_ADDRESS` in `trader/.env`): auth OK, but balance read **0.0000 USDC** despite the UI showing $9.50 and on-chain confirmation (direct `balanceOf` call against Polymarket's `pUSD` collateral token, `0xc011a7e1...`, and the deposit tx) that the funder address genuinely held 9.5 pUSD. Root cause: **wrong `signature_type`**. `execution.rs`/`api_probe.rs` hardcoded `SignatureType::Proxy` (1 = Magic Link accounts), which is what every prior account (incl. the `btc_5mins` Python bot's) used — but this new account is a `POLY_1271` (3 = EIP-1271 smart-contract-wallet signature) account, confirmed by brute-forcing all 4 signature types against `get_balance_allowance` (only `3` returned the correct $9.50 + max allowances) and by the funding tx being an EIP-7702 "smart EOA" transaction. Fixed: `signature_type` is now a parameter to `LiveExecutionEngine::connect` and `api_probe`'s balance/roundtrip paths, read from a new `POLY_SIGNATURE_TYPE` env var (defaults to `Proxy` for backward compat; set to `3` in `trader/.env` for this account). Re-verified after the fix: `api_probe balance` now correctly reads $9.50. The order-placement half is **intentionally deferred**: the user wants the first real order to be a genuine `reversal_unwind` strategy trade fired by the live worker, not a synthetic roundtrip — so this step unblocks once `worker.rs` exists, not on a standalone go-ahead. |
| **B3 unwind + redemption** | `unwind.rs` USER-WS fill watcher; `redemption.rs`. | A live $1 GTC unwind fills and the watcher fires; a resolved position auto-redeems. | ✅ Watcher done + tested (dispatch logic). `redemption.rs`'s read/classify half is done + tested; the on-chain redeem transaction itself (`RedeemExecutor`) is an intentionally unimplemented trait boundary — bigger blast radius than a CLOB order, needs its own design pass. |

> **No paper trading exists on Polymarket.** There is no sandbox; the Amoy
> testnet (chain_id 80002) uses different contract + USDC addresses and does **not**
> host the 5-min up/down markets, so it cannot validate this strategy's API path.
> The community-standard — and our — validation is **tiny real orders ($1) on a
> live market, immediately cancelled/closed**. Track B's live test (B2) therefore
> runs against production with minimal notional. (Sources at end.)

### Integration (tracks converge)

| Step | Scope | Risk gate |
|---|---|---|
| **I1 One asset live** | A's machine drives B's **live** `ExecutionEngine` for the smallest-notional asset (e.g. DOGE); Python runs the rest. Held WIN/LOSS only. | A day of Rust DOGE trades reconciles vs the Python backtest. |
| **I2 Early exits live** | Enable `Unwinding`/`StopExiting` (GTC + USER-WS + price-monitor fallback + settle retries). Highest-risk. | Live stop/unwind fills match the README contract; `STOPLOSS`/`UNWIND` rows + halt accounting correct. |
| **I3 Full cutover** | All trade-assets in Rust; balance guard; retire the Python live path. | A full week reconciles; Python kept for backtest/recon only. |

A1 and B1 are the two starred first steps and can begin **simultaneously**.
Enrichment signals (`p_up`/`snr`/`vol_har`) can be promoted any time; the trait +
`TickBus` make it additive and the A1 golden guards every promotion.

---

## 13. Generalizing across markets & durations (expandability)

Design goal: the same engine trades 5m / 15m / 60m / 4hr / 1d crypto up/down
markets, and — by passing a slug — arbitrary Polymarket markets, auto-rotating
when the market is periodic. The good news: most of the design is **already
period-agnostic**; generalization is one parameter (the market period) plus
per-duration config.

### What is already general (no change)

- The **state machine** (§8) reasons in `time_left`, ticks, and events — it has no
  wall-clock 5-min assumption.
- **Signals** consume ticks and work in `time_left` terms (`saw_low` window,
  `delta_pct`) — no hardcoded 300 (except the deferred HAR vol; below).
- **Outcome** = Binance open vs close via klines — works at any period.

### What gets parameterized — the one knob: market period

Introduce a `MarketSpec` (the single source of truth for "what am I trading"):

```rust
struct MarketSpec { id: String, asset: Option<String>, kind: MarketKind, cfg: StrategyKey }

enum MarketKind {
    Periodic { period_secs: u64, slug_suffix: String }, // {asset}-updown-{suffix}-{slot}
    OneShot  { slug: String },                          // pass-in slug; no rotation
}
```

- **Slug + rotation** (`marketdata.rs`): generalize `make_slug` to
  `{asset}-updown-{suffix}-{slot}` with `slot = (now / period_secs) * period_secs`
  — `../poly_rust/price_feed/collect.rs` already has the `make_slug(asset, slot,
  suffix)` form, so this is a lift, not new code. A **suffix↔period table** is the
  single place a new duration is added (confirm each suffix against Gamma):
  `5m→300, 15m→900, 60m→3600, 4hr→14400, 1d→86400`.
- **Scheduler**: replace the hardcoded `CycleScheduler.WINDOW = 300` with
  `period_secs` from the `MarketSpec`. After this, **300 lives in exactly one
  place — the suffix table — and nowhere else.**
- **Config keying**: strategy windows are in seconds, so they are tuned **per
  duration** (a 15-min `reversal_start_time` ≠ a 5-min's). Key the TOML by
  `(asset, period)`, e.g. a `[BTC.15m]` block falling back to a per-duration
  `default`. The Python backtest **already supports this**:
  `scripts/bt1.py --cycle-length-s 900` + `BacktestParams.cycle_length_s` (§11) —
  so each duration earns its **own bt1 golden** by the same procedure.
- **HAR vol** stays deferred and is **per-period when promoted** — its betas are
  fitted for a fixed horizon (the `300` hardcoded in `VolHarSignal`); a 15-min
  market needs its own fit. Enrichment-only, so never a blocker.

### Price/book recording follows the same `MarketSpec` (consistency)

The recorder must be keyed the same way, or the backtest replay (§11) for a 15-min
golden would read 5-min data. Split the feeds by *what they depend on*:

- **Binance + Chainlink are asset-level** (period-independent) — one recording per
  asset, **shared** across all durations: `{ASSET}_binance_{date}.parquet`,
  `{ASSET}_chainlink_{date}.parquet`. **No change** — a 15-min market reuses the
  same spot feed as the 5-min one.
- **Polymarket token prices + order book are market-level** (token IDs differ per
  slug/period) — so they **must be keyed by the market suffix** or durations
  collide in one file. Adopt one path convention and use it everywhere:
  - filename suffix — `{ASSET}_poly_{suffix}_{date}.parquet`,
    `{ASSET}_book_{suffix}_{date}.parquet`, **or**
  - per-period subdir — `prices/{suffix}/{ASSET}_poly_{date}.parquet`. This matches
    the **existing `../poly_rust/price_feed` precedent**, which already separates
    intervals into `raw_1hr/` and `raw_4hr/` directories.

  Recommendation: the filename-suffix form (one `prices/` dir, greppable, and the
  5-min files stay valid if `5m` is the default suffix). Whichever is picked, it is
  the **single convention shared by three readers/writers**: the live recorder, the
  backtest replay reader, and `bt1 --prices-dir` — so a duration's golden reads
  exactly what that duration's recorder wrote.
- The recorder takes `slug_suffix`/`period_secs` straight from the `MarketSpec`
  (same single source of truth as rotation), so **adding a duration adds its
  recording stream automatically — no recorder code change.** HKT-midnight date
  rollover is period-independent and unchanged.
- Recording itself stays in the reused `../poly_rust/price_feed` collector (the
  `PriceRecorder`/`book_recorder` equivalents); the generalization is purely in its
  **output path** + the `MarketSpec` it is handed — it does not move into `trader`.

### Pass-in an arbitrary slug

- `trader --market btc-updown-15m` (periodic, by asset+suffix) **or**
  `trader --slug <full-slug-or-polymarket-url>` for any market.
- **Parse:** a slug matching `{asset}-updown-{suffix}` with a *known* suffix →
  `Periodic` (auto-rotation on). Anything else → `OneShot`: resolve token IDs via
  Gamma once, subscribe to the feed, run one machine instance to resolution, no
  rotation. (A URL is just a slug with the host stripped.)
- **"Trade on price pattern":** the same `signal → strategy → gate → machine`
  pipeline applies; an arbitrary market only needs a strategy + params assigned to
  it (start with reversal/high_prob; add new ones via the `Signal`/`Strategy`
  traits). A market with **no** strategy assigned runs in **watch/record mode**
  only — the `poly_rust` feed path already does data capture + rotation, so
  "just get data" is free.

### Module impact (small, localized)

| Change | File |
|---|---|
| `MarketSpec` / `MarketKind` + suffix↔period table | `config.rs`, `marketdata.rs` |
| generic `make_slug(asset, suffix, slot)` + rotation by `period_secs` | `marketdata.rs` |
| `CycleScheduler` takes `period_secs` (drop hardcoded 300) | `marketdata.rs` / `worker.rs` |
| per-`(asset, period)` strategy blocks | `config.rs` + TOML |
| CLI `--market` / `--slug` | `main.rs` |
| one-shot (non-rotating) machine lifecycle | `worker.rs` |
| market-level recording keyed by suffix (poly + book); asset-level (binance/cl) shared | `../poly_rust/price_feed` recorder output paths |

### Why this stays clean

`period_secs` makes duration a **test parameter**: the same machine + signal test
suite runs at 300 / 900 / 3600 s by swapping one field, and each duration locks
its own bt1 golden (§11). Nothing in the decision core forks per duration — the
`OneShot` vs `Periodic` split is the only branch, and it lives at the edge
(rotation), not in the machine. This is the modular/testable/expandable principle
applied to the market dimension.

Recommended sequencing: ship 5m first (Track A/B above), then add 15m purely as a
new `MarketSpec` + a `[*.15m]` config block + its bt1 golden — **no engine
changes** if the abstraction is in from A1.

---

## 14. Conventions (per `poly_rust/CLAUDE.md`)

- **Sync core / async shell (§10):** the state machine + signals + strategies run
  **sync** on a dedicated engine thread (per asset, or one shared loop); `tokio`
  is confined to the live feed/exec shell. Signals are owned by the engine thread
  → no `Mutex`; only genuinely cross-thread state (config, balance, halt, status)
  uses `Arc<RwLock/Mutex>`. Never hold a `std::Mutex` guard across `.await` in the
  shell, and never call `Handle::block_on` from a tokio worker thread.
- **Errors:** `anyhow` + `.context()` at the binary top; `thiserror` for
  structured execution errors. **No `unwrap`/`expect`/`panic` in library code** —
  an order path must never panic.
- **Decimals:** SDK `Decimal`/`rust_decimal`; match Python rounding
  (`round(size/base_price, 2)`, cost rounding) so fills/CSV are recon-comparable.
- **Not-ready = `None`**, never `0.0` (repo "Zero Means Zero" rule).
- **Lints/format:** `cargo clippy --all-targets --all-features -D warnings`,
  `cargo fmt`. **No `unsafe`.**
- **Tests live with their module** (`#[cfg(test)] mod tests` at file bottom);
  cross-module integration tests in `trader/tests/`. CI runs the full suite.

---

## 15. Open decisions (need your call at review)

1. **Crate location** — `trader` crate inside `../poly_rust` (recommended, reuses
   feeds) vs a `rust/` dir inside `btc_5mins`.
2. **Concurrency model (§10)** — one **sync engine thread per asset** (latency
   isolation; a blocking order call stalls only that asset) vs a **single sync
   engine loop over all assets** (matches `order_trade_machine`, simplest). Both
   are fed by the async feed layer over channels and keep the engine tokio-free.
   Recommendation: per-asset thread. Decide at A2.
3. **Telegram transport** — `teloxide` vs a thin `reqwest` long-poll loop.
   Recommendation: long-poll first (fewer deps, simpler), escalate only if needed.
4. **`/set` semantics across restart** — runtime `/set` mutates the in-memory
   `Arc<RwLock<Config>>` and writes a `config.log` snapshot, but does **not**
   rewrite the TOML (same as today). Confirm that's the intended contract (TOML is
   edited by hand / calibration scripts; `config.log` is the runtime source of
   truth that recon reads).
5. **State-machine granularity** — model the per-`(asset,strategy)` worker as the
   full §7/§8 typestate machine from A1 (recommended — clean states, table-
   testable, crash-resumable), or start with a simpler imperative loop and refactor
   to states at I2 when early exits add complexity? Recommendation: typestate
   from the start; it is the cheapest insurance against the flag-soup the Python
   `worker.py` accumulated (`_rev_*`/`_normal_*`/`_manual_halt`).
6. **Enrichment signals now or later** — port `p_up`/`snr`/`vol_har` to Rust in an
   early phase (needs Student-t CDF validated vs `scipy.stdtr`) or leave the CSV
   columns empty until a strategy needs them. Recommendation: stub in `enrich.rs`,
   leave empty, promote when a HAR-driven strategy is actually designed.
7. **Deploy** — `upgrade_oracle.py` does tmux start/stop of `python -m bot`; the
   Rust bot ships a release binary, so the start/stop/pull steps need a Rust-aware
   variant. Adapt at I1.
8. **Per-duration config layout (§13)** — nested `[asset.duration]` blocks in one
   TOML, vs separate `strategy_<dur>_*.toml` files per duration. Recommendation:
   nested blocks with a per-duration `default` fallback (one file, one loader);
   revisit if durations diverge enough to want separate files. Bake the
   `MarketSpec`/`period_secs` abstraction in from **A1** so adding a duration is
   config-only, no engine change.

---

## 16. What stays in Python

- All backtesting (`bot/backtest*.py`, numba/cuda sweeps) — the correctness oracle.
- Studies, recon, weekly reports (`scripts/*recon*.py`, `weekly_*.py`).
- ML feature fitting (HAR betas, Student-t dof) — Rust only *consumes* the fitted
  coefficients from the TOML.

---

## 17. Build and deploy

**Docker's role here is cross-compilation** — `cross` (below) runs the actual
build for Oracle inside a Docker container, on the dev machine, so Oracle's
CPU is never used for compilation and no aarch64 toolchain has to be installed
locally. That's the primary, load-bearing use. There is a *second*, separate
Docker image (`trader/Dockerfile`) that is **not** for cross-compiling or
deploying — it's an optional, same-arch (x86-64) local test image, useful only
for pre-deploy verification (balance/Telegram/`/status`/market feeds against
production) before touching Oracle at all. Don't conflate the two.

### Oracle deploy — cross-compile locally with `cross`, never build on Oracle

Oracle (`10.8.0.1`) is aarch64; the dev machine is x86-64. Same pattern
`price_feed` already uses (see repo-root `README.md` → "Build and deploy"):

```bash
cd trader
cargo install cross   # one-time
cross build --release --bin live --target aarch64-unknown-linux-gnu
rsync -avz target/aarch64-unknown-linux-gnu/release/live \
  ubuntu@10.8.0.1:/home/ubuntu/apps/poly_rust/trader/target/release/
```

`cross` runs the build in a Docker container (`ghcr.io/cross-rs/aarch64-unknown-linux-gnu`)
**on the dev machine** — no system linker install needed, and Oracle's own CPU
is never used for compilation. **Do not run `cargo build` on Oracle directly**
— same reasoning as `price_feed`: it saturates the box for minutes and risks
interfering with whatever's already running there (e.g. the Python bot).

Oracle also needs its own `trader/.env` kept in sync (`rsync -avz trader/.env
ubuntu@10.8.0.1:/home/ubuntu/apps/poly_rust/trader/.env`) *plus*
`CLOB_PROXY_URL=http://10.8.0.7:8888` appended to it — that's what makes order
placement actually work from Oracle (see geoblock note below). Then run it
detached:

```bash
ssh ubuntu@10.8.0.1 "cd /home/ubuntu/apps/poly_rust/trader && nohup ./target/release/live \
  --asset DOGE --strategy reversal --size-usdc 1.0 --max-trades 1 \
  --config-dir /home/ubuntu/apps/btc_5mins/config \
  --env-file /home/ubuntu/apps/poly_rust/trader/.env \
  --log live_logs/live_trades_doge.csv --state-file live_logs/live_state_doge.json \
  > live_logs/live_doge_oracle.log 2>&1 & disown"
```

✅ Done — deployed and running live on Oracle 2026-07-02 (DOGE/reversal/$1,
max_trades=1, new account, `signature_type=Poly1271`), confirmed routing CLOB
writes via the EC2 proxy (`[live] routing CLOB writes via proxy:
http://10.8.0.7:8888` in the log) instead of hitting the geoblock.

### Local test image (`trader/Dockerfile`) — same arch as the dev host, optional

```bash
cd trader
docker build -t trader-live:local .
docker run -d --name trader-live-test \
  -v "$(pwd)/.env:/app/.env:ro" \
  -v /home/kev/apps/btc_5mins/config:/app/config:ro \
  -v "$(pwd)/live_logs:/app/logs" \
  trader-live:local \
  --asset DOGE --size-usdc 1.0 --max-trades 1 \
  --env-file /app/.env --config-dir /app/config \
  --log /app/logs/live_trades_doge_docker.csv --state-file /app/logs/live_state_doge_docker.json
docker logs -f trader-live-test
```

Builds natively for the dev host's arch (x86-64), so it's fast, but it does
**not** produce a binary that runs on Oracle (aarch64) — order placement from
this container still hits the same geoblock as running natively on the dev
host (expected; see below). Only use this for testing balance/Telegram/
market-data paths, never as a deploy artifact, and **stop it before deploying
to Oracle** — both would share the same account/balance with no coordination
between them, risking a double order on the same signal.

### Geoblock — order placement needs the EC2 proxy, on *both* Oracle and the dev machine

Found 2026-07-02: Polymarket's CLOB geoblocks `POST /order` (403 Forbidden,
`"Trading restricted in your region"`) from **both** Oracle (HK) and this dev
machine's normal internet egress — not just Oracle as originally assumed. GET
requests (balance, market data) are unaffected everywhere. The existing fix
(`CLOB_PROXY_URL` → EC2's `gost-proxy` at `10.8.0.7:8888`, read by
`execution.rs`/`bin/live.rs`/`bin/api_probe.rs` and turned into `HTTPS_PROXY`)
only works from hosts that can actually reach `10.8.0.7:8888` — currently just
Oracle, which is a peer on EC2's `wg1` WireGuard subnet. **The dev machine is
not a `wg1` peer**, so setting `CLOB_PROXY_URL` locally does nothing without
first adding it as a peer (a WireGuard config change — do not make this change
without the user's explicit go-ahead, see `feedback_vpn_network` memory).
Until/unless that's set up, real order placement only works when the binary
actually runs **on Oracle**, with `CLOB_PROXY_URL=http://10.8.0.7:8888` set in
its env file.

---

## Sources (paper-trading / API validation, §12 Track B)

- Polymarket CLOB developer docs (no sandbox; mainnet-only CLOB):
  <https://docs.polymarket.com/developers/CLOB/introduction>
- `py-clob-client` (Amoy testnet `chain_id=80002` exists for contracts only;
  different ctf_exchange + USDC addresses, no up/down markets):
  <https://github.com/Polymarket/py-clob-client>
- Polymarket API guide 2026 (tiny-real-order validation is the standard workaround):
  <https://polymarkets.co.il/en/guide/api-guide/>

---

## DeepSeek Feedback (deepseek-v4-pro via aider, 2026-06-30)

> Independent design review requested via `aider --model openai/deepseek-v4-pro`.
> DeepSeek was given this plan, the Python live-bot core (`strategies.py`,
> `signals.py`, `trading.py`, `config.py`), and a facts pack of **real production
> log excerpts + the trade CSV + the `order_trade_machine` state-machine shape**,
> and asked to focus on the **state machine** and the **modular/testable** design.
> Reproduced verbatim (only terminal line-wrapping reflowed). **This review has now
> been incorporated** into the plan above — primarily §8 (the five state-machine
> fixes: no-entry-gate halt, GTC/FAK arming, partial fills, `Confirming`/`EnrichOnly`
> split, crash-recovery persistence), plus §4 (partial fills), §7 (drop
> `TradeRepository`), §11 (tick-ordering verification), and the A1 acceptance gate.
> It is preserved here verbatim as the source review.

### Summary

The plan is thorough and grounded in real production logs. The core insight — a
synchronous, runtime-free state machine driven by a scripted `Event` stream — is
the right way to achieve deterministic backtest parity and easy unit testing. The
module split (trade-API, strategy, telegram) and the sync-core/async-shell
architecture are sound.

However, the state machine and some production behaviours contain critical gaps
that would cause early-exit mishandling and loss of live fidelity. Those gaps are
detailed below; addressing them is prerequisite to the A1 golden.

### 1. State machine vs. production logs — critical gaps

**1.1 Manual halt must NOT abort an in-flight trade.** The plan's transition
`any --Control(/halt) | Balance --> Halted` is too broad. The real-world log
(facts-pack Example A) shows a manual halt activated *while a reversal entry is
already filled and a price-monitor unwind is in progress*. The halt did not stop
the stop-loss / unwind sale — the FAK exit proceeded, retried, and eventually
filled. In the plan, a `Control(/halt)` arriving while in `Holding` or `Entering`
would move the machine to `Halted`, which — as drawn — means the machine stops
reacting to exit events and the position would be orphaned. **A manual halt is a
no-new-entry gate, not a "freeze everything" state.** The machine must keep
processing exit-related events (PolyTick for price-monitor, UnwindFill, stop-loss
crosses → FAK close) even when the manual-halt flag is set; only the
`Watching → Entering` transition must be suppressed. *Fix:* separate the manual
halt into a per-strategy flag that gates the entry decision without changing the
state. *Risk:* without this, any Telegram `/halt` during a live trade leaves the
position unmanaged — a clear loss scenario.

**1.2 GTC vs. FAK unwind fallback must be explicit.** When the entry fills, the
bot either places a GTC limit sell (shares ≥ 5) or sets a price-monitor that fires
a FAK market sell when the TP is crossed (shares < 5). The plan's
`Entering --Order(Fill)--> Holding (arm unwind TP + stop-loss)` collapses these
two paths into an unspecified "arm". The state machine must know the filled share
count, immediately call `ExecutionEngine::place_limit_sell` if shares ≥ 5,
otherwise register a price-monitor that will later trigger `close_position`.
Otherwise the golden replay will never produce the `Unwind GTC skipped … monitoring
poly price` path seen in Example A.

**1.3 Partial fills.** `ExecutionEngine::place` may return `filled < requested`.
The machine treats any fill as `Holding` but does not address how much is open.
Exit quantities (unwind size, stop-loss size) must reflect actual filled shares; a
sell that only partially fills could leave a residual position — the machine needs
to return to `Holding` with a reduced share count, not treat the sell as fully
resolved.

**1.4 Crash-recovery / mid-cycle restart.** The plan mentions serialisable states
"for clean restart recovery", but the only persistence described is the CSV trade
log and JSONL config snapshot. Neither records the dynamic state of an in-flight
machine (token-ids, current shares, limit-order ID, whether a GTC or price monitor
is active). A concrete recovery mechanism (e.g. write the `TradeState` to a
restart-safe file after every transition) is needed; otherwise a crash during
`Holding` with a resting GTC will lose the order and leave the position open on the
exchange.

**1.5 Confirming machine scope.** The plan spawns a `Confirming` side-machine for
every `Resolved`. But the invariants say STOPLOSS pnl is final (ApiResult is
counterfactual) and UNWIND ApiResult is confirmatory — so `Confirming` should be a
no-op for early-exit rows and only spawn for held WIN/LOSS rows where a WIN↔LOSS
flip affects halt accounting. The current `Resolved --spawn watcher--> Confirming`
is unconditional and overly broad.

**1.6 Entry-edge timing (manual halt during `Entering`).** If a `Control(/halt)`
arrives after `place` is called but before `Order(Fill)`, the machine should cancel
the pending order (if possible) and cleanly fall back to `Watching` (or `Halted`
with order cancelled). The plan does not cover this case.

### 2. Modular design — what works and where tests will be hard

**Strengths:** the `ExecutionEngine` trait + mock impl lets the pure-sync core be
driven in tests without any CLOB connection — the right boundary. The
`Signal` trait + `TickBus` make adding a signal one file + one registration line;
genuinely extensible. Telegram as a first-class control plane is correct.

**Testing hardness:** the worker integration test (synthetic cycle → transitions)
needs careful fixture construction — scripted events must respect the exact timing
of `BinanceTick`/`PolyTick` and the per-cycle gate order; doable but heavy. The
GTC/unwind hybrid path requires the test to simulate the exchange's `balance: 0`
retry behaviour (the mock can do it), but verifying the full mock-engine ↔ state
machine interaction will be the most complex integration test.

**Over-/under-engineered:** the `TradeRepository` trait backed by CSV/JSONL is mild
over-engineering — the Python side already reads the CSV trade log directly; a
simple append-only logger in the worker is sufficient. The tokio `broadcast`
separation is appropriate; Redis/NATS would be overkill.

### 3. Backtest-first approach and market generalisation

The bt1 golden is the strongest thing in the plan; building the replay harness
before any live wiring and locking the PnL per trade sequence is exactly the
guarantee that the decision core is correct. *Risk:* bt1's golden must match the
state machine's exact tick sourcing — the Rust replay must use exactly the same
parquet files as `bt1.py` and feed them in the same temporal order to produce
identical openings, delta-pct, saw-low, and entry windows. Any shift in
timestamp alignment (e.g. different binance/poly merge logic) will break the
golden, so the replay should reuse the exact same timestamp-ordering logic as the
Python tick-bus (the plan asserts it will — this must be verified).

**Market generalisation (§13):** the `MarketSpec` / `period_secs` abstraction is
clean and correctly separates Binance (asset-level) from Polymarket (market-level)
data; per-duration `[asset.duration]` config keying is consistent with the existing
`bot/config.py`; deferring the hard-coded-300 HAR vol to enrichment-only is
pragmatic. *Note:* the 5-m golden must still use `period_secs = 300` and the
back-compatible TOML has no period field (implicit 300) — fine mechanically, worth
noting.

### 4. Top-priority actions before A1

1. Redesign manual-halt handling — make it a no-entry gate that does not terminate
   active trades.
2. Expand the `Order(Fill) → Holding` transition to explicitly handle the
   GTC-vs-FAK unwind decision based on filled shares.
3. Add a partial-fill handling branch to the exit-sell paths.
4. Specify crash-recovery persistence (a small JSON file recording the current
   `TradeState` and in-flight order details after each state change).
5. Constrain `Confirming` to held-to-maturity outcomes.

These fixes are local to the state machine design and do not change the module
architecture. Once incorporated, the A1 golden will protect the full edge-case
space seen in production.
