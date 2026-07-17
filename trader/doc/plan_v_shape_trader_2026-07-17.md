# Plan: first-class `v_shape` strategy in the trader module

Status: **proposed → implemented same day** (this doc is written before the code; the
Verification section is filled in after).

## Goal

Promote the V-shape strategy family (`v_{high1}_{low}_{high2}_{sl}_{unwind}` — e.g.
`v_0.7_0.3_0.7_0.3_0.15`) from a siglab-only self-contained engine
(`siglab/src/v_shape.rs`) to a **first-class trader strategy alongside `reversal` and
`high_prob`**: its own config fields in `strategy_*.toml`, its own `Machine` constructor
(backtest/shadow/siglab path), its own `Worker` constructor (live path), its own halt
counters, selected per-asset via the existing `[strategies]` table.

This is a different thing from what siglab already has: siglab's `VShapeEngine` is a
minimal standalone simulator (fixed grid, no gates, no halt, fixed $1 size, no live-order
path). This plan gives the *trader* the same entry idea wired through its real
architecture — signals + strategy evaluate + gates + Machine/Worker exit chain — so it can
be backtested with `bin/backtest`, shadow-tested with `bin/shadow`, and (later, not now)
traded live by `bin/live`.

## Entry condition (same as siglab's `v_shape.rs`)

Per side (up, dn = 1-up), a two-stage latch: price must reach `>= v_high1`, then later
`<= v_low` (only counted after high1 was seen), then when price recovers to `>= v_high2`
the entry fires on that side. Both sides tracked independently. Latches reset every cycle
(crypto markets have real cycles — this is the `cycle_open`-driven reset siglab's crypto
path already does).

Philosophy carried over from siglab: **no Binance direction requirement** — the entry is
pure CLOB price action. `delta_pct_v` exists as a config field but defaults to `0.0`
(disabled); the gate layer's `dp.abs() < min_delta` check passes trivially at 0.0, and
`VShapeStrategy::evaluate` itself never looks at delta. Safety gates that are about
execution quality, not direction — spread sanity, poly staleness, `max_buy_price` — still
apply unchanged, since a real (future) live order should never skip those.

## Config — own set, `_v` suffix, matching the `_rev`/`_hp` convention

New `StrategyToml` per-asset maps, all `#[serde(default)]` so **every existing
`strategy_*.toml` still parses** (backtest `--config-file` pins old configs; they must not
break). `resolve()` falls back to hardcoded defaults when a map is empty:

| field | default | note |
|---|---|---|
| `v_high1` | 0.70 | the canonical `v_0.7_0.3_0.7` triple |
| `v_low` | 0.30 | |
| `v_high2` | 0.70 | |
| `delta_pct_v` | 0.0 | disabled — no directional confirmation, per siglab philosophy |
| `sl_v_shape` | 0.0 | absolute SL, disabled (mirrors `sl_reversal`'s shape) |
| `sl_pnl_v` | 0.30 | siglab grid value |
| `unwind_pnl_v` | 0.15 | mid-grid; siglab's per-variant results are still too early/mixed to call a winner (checked 2026-07-17 report) |
| `unwind_time_v` | 25.0 | same as siglab's fixed `UNWIND_TIME_SECS` |
| `halt_v` | 1 | consecutive-loss halt, same default as current `halt_rev` |
| `halt_reset_hour_v` | 2 | HKT, same as `halt_reset_hour_rev` |

New `AssetParams` scalar fields for each of the above. **This is the one breaking-ish
change**: `AssetParams` is constructed literally in 6 places (`config.rs::resolve`,
`siglab/src/config.rs::to_asset_params`, and 4 test fixtures in `machine.rs`,
`worker.rs`, `backtest.rs`, `bin/live.rs` ×2) — all get the new fields with the defaults
above. siglab compiles against trader as a path dependency, so its config.rs is updated in
the same commit (no behavior change there — siglab's own `VShapeEngine` grid is untouched
and unrelated).

A new tracked config `trader/config/strategy_20260717.toml` (copy of `_20260716` + the new
`[v_*]` sections with the defaults above) documents the fields. **`[strategies]` stays
`default = ["reversal"]`** — v_shape is configured-but-not-traded, exactly the status
`high_prob` already has today. Even if this config reaches Oracle via a routine `git pull`
+ restart, nothing starts trading v_shape. Local testing uses a scratch config with
`v_shape` added to `[strategies]`, never the tracked one.

## Code changes

1. **`types.rs`** — `EntryType::VShape` (`as_str() == "v_shape"`). Grep confirms
   `EntryType` is only matched in `gates.rs` (`== Reversal` comparisons — still correct)
   and formatted via `as_str()`, so the new variant is additive.
2. **`signal/v_shape.rs`** (new) — `VShapeSignal`, the two-stage latch
   (`seen_high` → `seen_low_after_high`), `new_up`/`new_dn` like `SawLowSignal`, `Signal`
   impl (`reset` clears both stages, `on_poly` advances them). No time window (siglab's
   V-latch is active the whole cycle) — the only timing constraint is
   `no_enter_when_time_left`, enforced in the strategy, matching `ReversalStrategy`.
3. **`strategies.rs`** — `VShapeStrategy { v_high2, no_enter_when_time_left, fired,
   cycle_end_ts }` with `evaluate(now, v_up, v_dn, latest_poly, latest_binance)`
   returning a `TradeIntent { entry_type: EntryType::VShape, .. }` when a side's latch is
   complete and its current price `>= v_high2`; `reset`/`mark_fired` like the other two.
4. **`machine.rs`** — `StrategyKind::VShape` + `Machine::new_v_shape(p)` (sl:
   `sl_v_shape`, sl_pnl: `sl_pnl_v`, unwind_pnl: `unwind_pnl_v`, unwind_time:
   `unwind_time_v`); two new owned signals `v_up`/`v_dn` (updated in `on_poly`, reset in
   `cycle_open`, same always-updated pattern as `saw_low_*`); match arms in
   `cycle_open`/`try_enter`/`mark_fired`. Exit chain (SL → TP → cycle-end force-unwind →
   timeout) reused untouched.
5. **`worker.rs`** — same additions mirrored (`Worker::new_v_shape` with
   `halt_v`/`halt_reset_hour_v`), keeping worker/machine in lockstep as their module docs
   require.
6. **`gates.rs`** — `GateParams.delta_pct_v`; `check_gates` picks the per-entry-type
   min-delta via a match over all three entry types (0.0 ⇒ passes).
7. **Binaries** — `bin/live.rs` `"v_shape" => Worker::new_v_shape(...)` (+ the
   halt-display/reset sites that branch on `strategy_name == "high_prob"` get a v_shape
   arm); `bin/shadow.rs` and `backtest.rs` get `"v_shape" => Machine::new_v_shape(...)`
   and a third `HaltTracker` for v_shape next to `halt_rev`/`halt_hp`.
8. **`config_log.rs`** — `ConfigSnapshot` gains the `v_*` maps (parity with the
   rev/hp fields it already snapshots).

## What this does NOT do

- **No live trading of v_shape** — not added to any tracked config's `[strategies]`.
  Enabling it for real is a deliberate future config change, not part of this task.
- **No siglab behavior change** — its standalone `VShapeEngine` grid keeps running as-is;
  only its `to_asset_params` constructor gains the new fields mechanically.
- **No Oracle deploy.**
- **No claim the default parameters are good** — siglab is still collecting per-variant
  evidence; today's report shows small samples with mixed PnL across markets.

## Testing

- **New unit tests**: `VShapeSignal` latch semantics (low-before-high must not count;
  reset clears; both sides independent); `VShapeStrategy` evaluate (fires only with
  complete latch + price ≥ high2 + outside no-enter window; `fired` suppresses);
  `Machine::new_v_shape` end-to-end (enter → TP / SL / timeout / cycle-end force-unwind /
  cycle_close resolution; no entry without high1 first — mirroring siglab's
  `v_shape.rs` test list); `Worker::new_v_shape` entry + halt wiring; config tests (old
  configs still parse with defaults; new toml overrides resolve; `strategy_20260717.toml`
  loads).
- **Regression**: full `cargo test` in `trader/` (all existing reversal/high_prob
  machine/worker/gates/backtest/live tests) + `siglab/` suite. `cargo fmt --all --check`,
  `cargo clippy --all-targets --all-features -- -D warnings` in both.
- **Backtest byte-for-byte regression**: run `bin/backtest` pinned to
  `strategy_20260716.toml` on existing `backtest_prices/` data before and after the
  change — outputs must be identical (proves reversal/high_prob decision paths are
  untouched by the added fields/variants).
- **Local live-tick test**: `bin/shadow` with a scratch config dir whose latest
  strategy toml adds `v_shape` to `[strategies]` (BTC, 5m, a few cycles) — verify v_shape
  paper trades log with `strategy=v_shape` and sane entry/exit prices, alongside a
  reversal machine behaving normally in the same run.

## Verification (filled in post-implementation)

- _pending_
