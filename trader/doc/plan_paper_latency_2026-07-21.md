# Simulated paper-trade order latency (~800ms) + deferred fill pricing

**Status: implemented, locally verified against Oracle's real NATS/indicator feed via SSH
tunnel (no production state touched), deploying.**

## Problem

Paper-trade order logs report a `process_latency_ms` of ~1ms for every entry/exit — e.g. the
ETH trade in `trader/doc/incident_eth_trade_2026-07-21.md`:

```
[PAPER] 📋 ETH Order placed | 15:43:58 | T-61s | DOWN ↓ | reversal
signal=0.8750 → executed=0.8750 (slippage +0.0000) | ... | process_latency=1ms | n_attempts=1
```

Per user: this should measure real time from "trigger signal received" to "order confirmed
executed" — 1ms is nowhere close to a real CLOB round trip.

## Confirmed: not a measurement bug

`process_latency_ms = latency_ms(signal_ts, confirmed_ts)`, where `confirmed_ts` is captured
immediately after `await`-ing `PaperExecutor::place()`/`close_position()`. `PaperExecutor`'s fill
logic is pure, synchronous, in-process arithmetic (look up a cached price, compute shares/cost) —
no real I/O, no network hop. The measurement is *accurate*: it genuinely only took ~1ms, because
paper mode does no real work. The gap is a **modeling** gap (paper mode never simulated realistic
latency), not a bug in how latency is computed.

## Fix: non-blocking deferred fill

Two ways to make a fill "actually take ~800ms": block the calling task with
`sleep(800ms).await` inline, or defer it. The driver runs one shared `tokio::select!` loop across
all 6 assets — an inline blocking sleep inside `execute()` would stall that *entire* loop for
800ms on every single fill, delaying every other asset's ticks too. Per explicit user choice
(offered both options with this trade-off spelled out): **non-blocking deferred fill**, mirroring
the existing `spawn_resolution_watcher` pattern (a background task + channel handoff back into
the main loop) already used for Gamma resolution.

### Mechanism

- `PAPER_SIMULATED_LATENCY_SECS = 0.8` (`bin/live.rs`).
- `Action::PlaceBuy`/`Action::ClosePosition`, **paper mode only**: instead of calling
  `engine.place()`/`close_position()` inline, `execute()` spawns a background task that:
  1. Sleeps until `signal_ts + 0.8s` has genuinely elapsed (real wall-clock time).
  2. *Then* calls the real (still-synchronous) `PaperExecutor` method — since real time has
     actually passed, the price it observes honestly reflects "the price ~800ms after the
     signal," not a fabricated lookup.
  3. Sends the result back over a new channel (`paper_fill_tx`/`paper_fill_rx`) as a
     `PaperFillMsg::Entry`/`::Close`.
- `execute()` returns `None` immediately for the paper-mode branch — the calling `select!` arm
  is never blocked.
- A new `select!` arm drains `paper_fill_rx`, looks the slot back up by `(market, strategy)`, and
  finishes processing using **fresh** slot state at whatever moment the deferred message actually
  arrives (not stale state captured at signal time) — via two extracted methods,
  `finish_entry_order`/`finish_close_order`, containing the exact same latency/notification/event
  logic the real/dry-run inline path always had (shared, not duplicated).
- Real/dry-run: entirely unchanged — same inline, immediate path as before, just now routed
  through the same extracted `finish_*` methods instead of inline code.
- Stop-loss/timeout "triggered" alerts (`🛑`/`⏱️`, first-trigger-only) still fire **immediately**
  at signal time, unchanged — only the "order executed" confirmation and the resulting
  `Event` are deferred. This is arguably more realistic anyway (mirrors "condition met, submitting
  order..." then, later, "order executed").
- Scope: entry BUY and every exit close (stop-loss, take-profit, timeout). Resting-order-ack
  placement (`PlaceLimitSell`/`PlaceLimitBuy` — "the exchange acked my resting order") is
  untouched — that's a real ack, not a fill, and already resolves quickly for real exchanges too.

### Why this is safe against races

The state machine already has to tolerate a real order's fill confirmation arriving at an
unpredictable, possibly-late time relative to other events (`Worker`'s state guards — e.g.
`on_order_filled` only acts `if matches!(self.state, WorkerState::Entering)`,
`on_unwind_filled`/equivalents only act on `Unwinding`/`Holding{GtcResting}`) — a deferred paper
fill arriving after the position already resolved some other way (e.g. cycle-close happened
first) is a no-op by the exact same pre-existing guards, not a new failure mode this change
introduces.

One accepted edge case: an entry/close signal firing within the last ~800ms of a cycle could have
its deferred confirmation arrive after `CycleClose` already ran. The worker safely no-ops it (see
above) — the simulated fill itself still executed inside `PaperExecutor` (so `observed_price`
bookkeeping stays consistent) but the trade/position bookkeeping simply doesn't see it, matching
how an unpredictably-late real order confirmation is already handled today. Not treated as a new
risk class worth guarding against separately.

## Verification

No prior test in this codebase constructs a full `Driver` (existing coverage targets pure helpers
extracted from it, e.g. `resolve_trade_size_usdc`, `paper_balance`,
`should_suppress_startup_cycle`) — the deferred-fill mechanism is significant enough to warrant
new infrastructure rather than relying on integration-only verification:

- Two new deterministic tests, `paper_deferred_fill_tests::{paper_entry_defers_and_resolves_after_simulated_latency,
  paper_close_defers_and_resolves_after_simulated_latency}` — build a real `Driver` +
  `PaperExecutor` + channels, call `execute()` with a synthetic `Action::PlaceBuy`/
  `Action::ClosePosition` under `#[tokio::test(start_paused = true)]`, and assert: (1) `execute()`
  returns `None` immediately (never blocks the caller), (2) the channel has **nothing** before
  `tokio::time::advance(850ms)`, (3) the fill arrives with correct content (market/strategy/price)
  once virtual time passes the simulated latency. Runs in ~0.01s each (virtual time, not a real
  wait) — proves the ordering/timing contract deterministically rather than hoping a real signal
  fires during a soak test. (Note: `now_secs_f64()` wraps real `SystemTime`, not
  `tokio::time::Instant`, so it doesn't advance under `pause()`/`advance()` — the tests don't
  assert an exact ~800ms `confirmed_ts - signal_ts` gap for that reason; that figure is correct by
  construction in a real run, where both reads are genuine wall-clock time.)
- `cargo build`/`clippy --all-targets --all-features -- -D warnings`/`fmt --all --check`: clean.
- Full suite: 306 lib + 5 `backtest` + 58 `live` (2 new) — all green.
- **Local integration run against Oracle's real NATS/indicator feed**: SSH-tunneled
  `127.0.0.1:14222 → oracle:127.0.0.1:4222` and ran the newly-built local binary with
  `--nats-url nats://127.0.0.1:14222 --paper`, logging to a scratch directory only (never
  touching Oracle's production `live_logs/`, never placing a real order — paper mode has no CLOB
  client regardless). ~15 minutes, all 6 assets: clean startup, real indicator snapshots flowing
  throughout, zero panics/errors. No reversal signal happened to fire in that window (matches the
  established "these are genuinely infrequent" pattern from prior soak tests this same day) — the
  deterministic tests above are what actually prove the deferred-fill mechanics; this run's value
  is confirming the surrounding driver loop (NATS ingestion, indicator store, heartbeat, cycle
  rotation) stays healthy for an extended real-feed run with the new channel/spawn plumbing wired
  in, which it did.

## Deploy

`./scripts/deploy_trader.sh` (trader-only, binary-only change — no config edit needed for this
piece; bundled with the same-day `sl_reversal` config change in one restart).
