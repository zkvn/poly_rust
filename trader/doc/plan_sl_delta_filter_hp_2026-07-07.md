# Plan — port `sl_delta_filter_hp` (high_prob stop-loss momentum gate) into poly_rust

## 1. Motivation

`../btc_5mins/studies/stop_loss_filter/summary_2026-07-06_walk_forward.md` walk-forward
validated four candidate stop-loss tweaks (7 weekly folds, 6 calibrate→OOS steps × 6 assets =
36 asset-week trials each). Only one survived:

| Parameter | OOS win rate vs. disabled | Mean OOS PnL (picked / disabled / recommended) | Verdict |
|---|---|---|---|
| **`sl_delta_filter_hp`** | **80.6%** (29/36) | 7.11 / 4.80 / **7.32** | **Holds up — strong, robust** |
| `delta_sl_hp` | 38.9% (14/36) | 5.03 / 4.80 / 5.18 | Weak, inconsistent |
| `delta_sl_rev` | 5.6% (2/36) | 0.93 / 0.89 / 0.89 | No real edge |
| `sl_delta_filter_rev` | 0% (0/36) | 0.89 / 0.89 / 0.89 | Never won calibration |

`results_walk_forward.md`'s per-asset detail also shows `sl_delta_filter_hp`'s calibration
picked `0.0` in 34 of 36 steps (only BTC's step 6 and ETH's step 4 picked "disabled" instead;
never a positive threshold) — this isn't "some value beats disabled sometimes," it's "the
strictest setting (`X = 0.0`) consistently beats disabled," across every asset, most weeks.
That consistency (vs. `delta_sl_hp`'s inconsistent week-to-week edge) is why this is the one
worth wiring into live trading, per that summary's own §"Implications for live trading" point
1 and point 3's explicit call-out that porting it requires "porting the gating logic into
whichever worker owns live high_prob stop-losses now" — that's this repo (`poly_rust`; the
Python bot's own live trading moved here, per `../btc_5mins/CLAUDE.md`/README cross-refs).

**Scope: `sl_delta_filter_hp` (high_prob) only.** `sl_delta_filter_rev` never validated OOS
(0% win rate, picked "disabled" 100% of the time in calibration) and isn't part of this plan.

## 2. Filter semantics (ported from `../btc_5mins/studies/stop_loss_filter/PLAN.md` §"Filter
semantics", restricted to high_prob)

New parameter `sl_delta_filter_hp: Option<f64>` (per-asset, `"default"`-keyed like every other
knob in `strategy_*.toml`), applies to **both** existing high_prob stop-loss mechanisms
(`sl_pnl_hp` and absolute `sl_high_prob`) — not to `unwind_pnl_hp` (take-profit untouched).

- **`None`** (disabled, the shipped default) — stop-loss fires exactly as today, no behavior
  change.
- **`Some(x)`, `x ≥ 0`** — a stop-loss may only fire if, on the *same* tick, both hold:
  1. the existing SL price/PnL condition is met, **and**
  2. momentum has actually turned against the position:
     - **UP position**: `delta_pct ≤ x` (price has fallen back to no more than `x` above
       cycle-open; can go negative).
     - **DOWN position**: `delta_pct ≥ -x` (price has risen back to no more than `x` below
       cycle-open; can go positive).

  `x = 0.0` is a real, meaningful setting — the validated one — not a synonym for disabled;
  this is exactly why the field is `Option<f64>` rather than "0.0 = off" like `sl_pnl_hp` is.
- Re-evaluated on every tick, no memory of prior suppressions — a pure per-tick AND of two
  independent conditions, matching `check_gates`' own style (a stateless predicate, no
  latching).
- Recommended live value once shipped: **`0.0`** (the value that validated), not a positive
  threshold — set explicitly in the plan below, not left to a future sweep, since this repo
  has no sweep infrastructure of its own (see §6).

## 3. Where this lives in poly_rust (two engines need it, same as btc_5mins' njit/cuda pair)

This repo has the same "two engines must agree" shape btc_5mins has for njit/cuda, just
Rust-native instead of GPU-native:

- **`machine.rs::Machine`** — the backtest/replay engine (`instant fills, three states`,
  per `worker.rs`'s own module comment), driven by `backtest.rs::run_backtest` over recorded
  Parquet history. High_prob SL checks live in `Machine::on_poly` (`machine.rs:210-222`,
  PnL-based then absolute, in that order).
- **`worker.rs::Worker`** — the live typestate engine, driven by `bin/live.rs`. High_prob SL
  checks live in `Worker::on_poly` (`worker.rs:592-598`), currently a single combined
  `sl_hit` boolean (both PnL and absolute ORed together, unlike `machine.rs`'s two separate
  `if` blocks — see §4.2 for how the gate applies to both).

Both must change together, in the same commit, with the same semantics — this repo's own
`backtest.rs:422`'s "golden parity test" (Rust backtest vs. Python bt1 on BTC 2026-06-20 must
match exactly) is the existing guardrail that a divergence between engines would eventually
trip; shipping the filter disabled (`None`) by default keeps that golden test byte-identical
until someone deliberately turns it on.

## 4. Implementation

### 4.1 `config.rs`

- `StrategyToml`: add `pub sl_delta_filter_hp: HashMap<String, Option<f64>>` (mirrors
  `sl_pnl_hp`'s per-asset-dict shape). Needs `#[serde(default)]` so existing
  `strategy_*.toml` files without this key still parse (empty map).
- `AssetParams`: add `pub sl_delta_filter_hp: Option<f64>` under the "High-prob" section
  (`config.rs:82-88`).
- `resolve()`: `sl_delta_filter_hp: get_asset(&self.sl_delta_filter_hp, asset).flatten()
  .or(None)` — **not** `req(...)`, since unlike every other high_prob field this one is
  allowed to be entirely absent (defaults to disabled). Concretely:
  `self.sl_delta_filter_hp.get(asset).or_else(|| self.sl_delta_filter_hp.get("default"))
  .copied().flatten()` (falls back to `None` if the key/asset/default is missing at any
  level, rather than erroring like `req` would).
- New test in `config.rs`'s `mod tests`: assert `sl_delta_filter_hp` resolves to `None` for
  every asset against the *current* `strategy_*.toml` (nothing wires it on by default) —
  guards against an accidental default flip going unnoticed.

### 4.2 `strategy_<date>.toml`

Add a new dated TOML (`config/strategy_2026-07-0X.toml`, per this repo's existing "new dated
file per config change" convention — see `strategy_20260705.toml` precedent) with:

```toml
# Stop-loss momentum filter (high_prob only) — validated via walk-forward,
# see trader/doc/plan_sl_delta_filter_hp_2026-07-07.md. 0.0 suppresses a
# high_prob stop-loss unless delta_pct has actually turned against the
# position, not just the price/PnL threshold alone.
[sl_delta_filter_hp]
default = 0.0
```

No per-asset overrides initially — ship the single validated value (`0.0`) as the default for
every asset, matching how the walk-forward study picked `0.0` in 34/36 steps across all six
assets, not an asset-specific tuned value.

### 4.3 `machine.rs::Machine` (backtest engine)

- Add `sl_delta_filter_hp: Option<f64>` field to `Machine` (alongside `sl`/`sl_pnl`/
  `unwind_pnl`, `machine.rs` struct def near line 54); threaded from `AssetParams` in
  `Machine::new_high_prob`'s constructor only — `Machine::new_reversal` passes `None`
  unconditionally (out of scope per §1).
- `on_poly` (`machine.rs:210-222`): before firing either SL branch, compute
  `let momentum_ok = match self.sl_delta_filter_hp { None => true, Some(x) => match h.side {
  Side::Up => self.delta_pct.value() <= x, Side::Down => self.delta_pct.value() >= -x } };`
  and gate both the PnL-SL `if` (line 211) and the absolute-SL `if` (line 218) with
  `&& momentum_ok` appended to their existing conditions. Take-profit block (line 225)
  untouched.

### 4.4 `worker.rs::Worker` (live engine)

- Same new field, threaded the same way (`Worker::new_high_prob` only; `common()`'s shared
  constructor gets a new parameter, `new_reversal` passes `None`).
- `on_poly` (`worker.rs:592-598`): the existing combined
  `let sl_hit = (self.sl_pnl > 0.0 && exit_price <= h.token_price - self.sl_pnl) || (self.sl
  > 0.0 && exit_price < self.sl);` needs the momentum check applied to **both** disjuncts
  (not just ANDed onto the whole `sl_hit`, since that would be equivalent here — but keep it
  structurally next to each condition for readability/parity with `machine.rs`'s two separate
  `if`s). Compute `momentum_ok` identically to §4.3, then:
  `let sl_hit = ((self.sl_pnl > 0.0 && exit_price <= h.token_price - self.sl_pnl) ||
  (self.sl > 0.0 && exit_price < self.sl)) && momentum_ok;`
  (equivalent to gating each disjunct separately, since `momentum_ok` doesn't depend on which
  branch tripped — simpler than `machine.rs`'s two-`if` shape here because `worker.rs`
  already ORs them into one boolean before branching on `MIN_SELLABLE_SHARES`).

### 4.5 `backtest.rs`

- `run_backtest`'s `AssetParams`-consuming setup needs no change beyond what §4.1/4.3 already
  thread through — `Machine::new_high_prob` already takes the full `AssetParams`. Double-check
  the "golden parity" fixture (`backtest.rs:422`) still passes bit-exact with the new field
  defaulting to `None` on the fixture's config (it should, since `momentum_ok` short-circuits
  to `true` when `None`).

## 5. Tests

- `machine.rs` / `worker.rs` `mod tests`: add cases mirroring
  `../btc_5mins/tests/test_bt3_numba_parity.py`'s `sl_delta_filter=[None, 0.0, X]` pattern,
  adapted to this repo's existing test style (see `worker.rs`'s `enter_down_position` helper
  precedent):
  - `sl_delta_filter_hp = None` — byte-identical to current behavior (existing SL tests must
    still pass unmodified).
  - `sl_delta_filter_hp = Some(0.0)`, UP position, SL price condition true but
    `delta_pct > 0.0` (still trending favorably) → SL suppressed, position stays `Holding`.
  - Same setup, `delta_pct` later drops to `≤ 0.0` while the SL price condition still holds →
    SL fires then.
  - DOWN-side symmetric case.
  - Confirm `machine.rs` and `worker.rs` agree tick-for-tick on a shared scripted sequence
    (this repo doesn't have Python-style automated cross-engine parity tests today — this
    would be the first; worth adding given §3's two-engines-must-agree requirement, but can
    also be a manual side-by-side check if a shared-fixture harness doesn't already exist).
- `config.rs`: the resolves-to-`None`-by-default test from §4.1.
- Full suite: `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --all --check` clean, per this repo's standing CI bar.

## 6. What this plan deliberately does not do

- **No live Telegram `/set` command** for `sl_delta_filter_hp` — `grep`ing
  `trader/src/telegram/` for the existing stop-loss knobs (`sl_pnl_hp`, `sl_high_prob`) turns
  up no hits; unlike the Python bot's `SETTABLE_PARAMS`, this Rust trader has no live
  parameter-hot-reload mechanism at all today, only TOML-at-startup. Adding one is a
  separate, larger change and out of scope here.
- **No independent poly_rust sweep** — this repo has no sweep/grid-search infrastructure of
  its own (that lives in `../btc_5mins/scripts/bt2.py`/`bt3.py`); this plan trusts the
  already-completed 6-week walk-forward validation rather than re-deriving it from poly_rust's
  own (much shorter) recorded history. Per the walk-forward summary's own caveat, 6 weeks is
  "not independent fresh data" from the in-sample studies — worth another look after this has
  run live for a while, per §7.
- **`sl_delta_filter_rev`** — explicitly out of scope (§1); did not survive walk-forward.

## 7. Rollout

1. Implement §4.1–§4.5 together (config + both engines in one commit — a partial rollout
   would leave `machine.rs`/`worker.rs` disagreeing, same risk this repo's golden-parity test
   exists to catch).
2. Add tests (§5); confirm `cargo test`/`clippy`/`fmt --check` clean, golden parity fixture
   unchanged with the new field defaulting to `None`.
3. Ship the new dated TOML (§4.2) with `sl_delta_filter_hp.default = 0.0` — this *is* a live
   default change (unlike btc_5mins' own PLAN.md, which shipped the code with the live default
   left at `None` pending a sweep); justified here because the walk-forward validation already
   exists and consistently picked `0.0`, not a hypothesis still awaiting validation.
4. Monitor live high_prob stop-loss trades for a few weeks post-deploy (recon script / CSV,
   comparing `exit_attempts`/`outcome=StopLoss` frequency and PnL before vs. after) — the
   walk-forward's own caveat about a 6-week OOS window not being fully independent data means
   this repo's live results are the first truly out-of-sample confirmation.
