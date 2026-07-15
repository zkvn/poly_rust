# Recon Audit — why BT didn't fire, 2026-07-15

**Update (same day, after implementing the fix):** §5 below implements the pinned-historical-
config fix this audit originally only recommended, regenerates the report, and reports the
actual (not hoped-for) result: the false `BT DID NOT FIRE` is gone, but the row does **not**
resolve to a clean `MATCH` — it resolves to an accurately-explained `OUTCOME DIFF`, caused by a
second, separate, already-documented, intentional backtest-only rule. Read §5 for the honest
final state; §§1-4 are the original same-day investigation, left as written (its evidence and
row-2 conclusion are unaffected — only row 1's exact numbers, which came from a `backtest_prices`
snapshot that got rebuilt later the same day, are superseded).

Today's daily recon (`trader/results/daily_recon/trade_recon_2026-07-14_to_2026-07-15.md`,
"Live vs BT" table) shows 4 of 5 live trades as `BT DID NOT FIRE`. Two carry reason
`live halted: manual /halt 08:10–01:59` — a real, already-high-confidence halt window
(`classify_mismatch_reason`'s own doc comment rates halt-window matches as the one label that
*is* proof, not just a pointer), not investigated further here (§5 finds one of these two rows
was actually mis-attributed — see there). The other two both carry `config changed 2026-07-15
08:58 same-window (verify params)` — the classifier's explicit "unverified, go check" label. This
audit does that verification, for both rows, by actually re-running the Rust backtest against the
config that was live at trade time instead of today's (config::load_latest's) latest file.

**Two different root causes, not one:**

1. **08:55 BTC WIN — a real recon-tooling gap.** Replaying with the config that was actually
   live at 08:55 makes the backtest fire, with an entry that matches live almost exactly. `BT DID
   NOT FIRE` here is a false negative caused entirely by `trade_reconcile.py` always reconciling
   against *today's* config, not the config live was actually running under at the time — a gap
   already flagged in README `## TODO` since 2026-07-10, now confirmed with a concrete
   reproduction (and, per §5, now fixed — though the row still isn't a clean `MATCH`, for an
   unrelated reason).
2. **08:59 BTC STOPLOSS (-$0.5273) — BT is right, live is wrong.** Even replaying with the
   correct historical config, the backtest still doesn't fire this cycle — correctly. Live's own
   entry was driven by a corrupted `cycle_open_binance` reference price, a *different*, already-
   diagnosed and already-fixed bug (`trader/doc/fix_live_deploy_2026-07-15.md`) that happened to
   land on this exact cycle. `BT DID NOT FIRE` here is BT behaving correctly and, incidentally,
   independently confirming that bug's real-money cost.

## 1. Setup — the two rows in question

From today's report:

| Time | Asset | Side | Entry Px | Outcome | Live PnL | Status | Reason |
|---|---|---|---|---|---|---|---|
| 2026-07-15 08:55:00 | BTC | UP | 0.9000 | WIN | +0.1041 | BT DID NOT FIRE | config changed 2026-07-15 08:58 same-window (verify params) |
| 2026-07-15 08:59:40 | BTC | DOWN | 0.7900 | STOPLOSS | -0.5273 | BT DID NOT FIRE | config changed 2026-07-15 08:58 same-window (verify params) |

Both trades sit either side of `trader-live.service`'s 08:58:46 restart (a `--trader-only`
config+binary deploy), which is exactly why `classify_mismatch_reason` flagged both the same
way: `get_config_last_change_ts` returns the git-commit time of `strategy_20260715.toml`
(≈08:58), and both cycles (`btc-updown-5m-1784076600` = 08:50–08:55, `btc-updown-5m-1784076900`
= 08:55–09:00) straddle that timestamp.

`run_backtest_reconciliation` (`trade_reconcile.py`) always calls the `backtest` binary with
`--config-dir trader/config` — a directory, not a pinned file — and `config::load_latest`
(`trader/src/config.rs:207-220`) glob-sorts `strategy_*.toml` and takes the lexicographically
*last* one, unconditionally. With `strategy_20260715.toml` sitting in that directory, **every**
backtest reconciliation run, for **any** date, always replays against today's config — never
whatever was actually live on the date being reconciled. This is the exact gap already on record
in README's `## TODO` ("Backtest reconciliation config-drift gap — flagged 2026-07-10, not
fixed (deliberately deferred)").

## 2. Row 1 (08:55 WIN) — reproduced with the correct historical config

The config that was actually live for this cycle was `strategy_20260713.toml` (superseded by
`strategy_20260715.toml` at 08:58, after this cycle had already closed at 08:55). Re-running the
backtest with a scratch config directory containing only that file:

```
$ ./target/release/backtest --asset BTC --date 2026-07-15 \
    --prices-dir backtest_prices --config-dir <scratch-dir-with-only-strategy_20260713.toml> \
    --format csv

slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts
...
btc-updown-5m-1784076600,reversal,UP,0.900000,1.000000,WIN,0.111100,1784076881.000
...
```

vs. against today's (2026-07-15) latest config, over the same window — no row for that slug at
all:

```
$ ./target/release/backtest --asset BTC --date 2026-07-15 \
    --prices-dir backtest_prices --config-dir trader/config --format csv
# (no btc-updown-5m-1784076600 row)
```

Old-config replay: **entry 0.9000** — matching live's own entry price exactly. (The exit/outcome
this specific replay produced changed between when this was first checked and when §5's
regenerated report ran, because `backtest_prices/BTC_poly_2026-07-15.parquet` got rebuilt in
between by a later `sync_price_feed_from_oracle` in this same session — confirmed byte-for-byte
reproducible against the *current* file, twice, so it's not flaky; see §5 for the authoritative
exit/outcome and why it differs from live's.) **Verdict on the entry side: config drift, not a
real trading-logic discrepancy.** The relevant parameter that actually differs between the two
configs for BTC is `reversal_low_threshold` (`strategy_20260713.toml`: BTC override `0.20`;
`strategy_20260715.toml`: BTC override `0.30`, raised as part of the 07-15 walk-forward refresh)
— either value happens to still classify this cycle's dip as a valid "saw low" in this specific
case, so entry firing wasn't actually sensitive to which config replayed it; it just needed to be
replayed with *a* config that was ever live for BTC, not necessarily this exact one. The `BT DID
NOT FIRE` failure mode was purely "the recon script asked the wrong question," not a signal
disagreement — confirmed by the entry price match, independent of whatever the exit turns out to
be (§5).

## 3. Row 2 (08:59 STOPLOSS) — BT is correctly silent; the bug is upstream, in live

Re-running with the *same* correct historical config (`strategy_20260713.toml`) for the 08:59
cycle (`btc-updown-5m-1784076900`, 08:55–09:00) — still nothing:

```
$ ./target/release/backtest --asset BTC --date 2026-07-15 --prices-dir backtest_prices \
    --config-dir <scratch-dir-with-only-strategy_20260713.toml> --format csv
# (no btc-updown-5m-1784076900 row either — under the *correct* historical config)
```

So config drift alone doesn't explain this row — under the config that was genuinely live at
the time, the backtest still, correctly, doesn't fire. That itself is the finding: **live traded
a cycle the backtest — replayed honestly — says shouldn't have traded.**

### 3.1 Reconstructing why live entered anyway

`ReversalStrategy::evaluate` (`trader/src/strategies.rs:66-74`) requires, for a DOWN entry:
`saw_low_dn.saw_low() && dn > reversal && delta_pct < 0.0`, and separately `gates.rs::check_gates`
requires `|delta_pct| >= delta_pct_rev` (gate #3, `MinDeltaPct`). Reading the recorded tick data
for this cycle directly (`backtest_prices/BTC_poly_2026-07-15.parquet` /
`BTC_binance_2026-07-15.parquet`, the same data both live and the backtest saw):

- `dn` (DOWN token price) dipped to a minimum of **0.195** at 08:58:49.8 — inside the
  `SawLowSignal` window (`[cycle_end−reversal_start_time, cycle_end−no_enter_when_time_left]`
  = 08:58:00–08:59:50 for this cycle) and below both configs' `reversal_low_threshold` for BTC
  (0.20 old, 0.30 new) — `saw_low_dn` latches under either config.
- At 08:59:28.0, `dn` jumps from 0.640 to 0.805 in under a second — comfortably above `reversal`
  (0.55, unchanged both configs) — satisfying the "recover" half of the signal. This is within
  a second of live's actual order (`live.log`: `📋 BTC Order placed | 08:59:28 | T-31s | DOWN`,
  entry token price 0.79 — matches the tick data almost exactly).
- **The true cycle-open Binance price** (first tick of `btc-updown-5m-1784076900`, 08:55:06) was
  **64830.01**. At 08:59:28, Binance was **64824.57** — `delta_pct = (64824.57 − 64830.01) /
  64830.01 = −0.0000839` (**0.0084%**). That's below *both* configs' `delta_pct_rev` for BTC
  (old override `0.0005`, new default `0.0003`) — the `MinDeltaPct` gate should have blocked
  this entry under either config, and the backtest — replaying against the true open — correctly
  never let it through.

So how did live enter? `live.log` shows this exact cycle's `CycleOpen` was fabricated by the
08:58:46 restart landing mid-cycle: `[live] new cycle BTC (reversal) slug=btc-updown-5m-1784076900
open_binance=64845.87` — **64845.87, not the true 64830.01**. This is precisely the bug diagnosed
and fixed same-day in `trader/doc/fix_live_deploy_2026-07-15.md` (`current_slot_val` resetting to
`0` on every restart, so the first tick after a restart always looks like a fresh cycle open,
stamping whatever Binance happened to be trading at restart time as the reference price) — that
doc's own root-cause section even names this exact stop-loss ("directly implicated in one costly
stop-loss that fired in that cycle... the 2026-07-15 08:55 BTC row").

Recomputing `delta_pct` with live's *fabricated* open: `(64824.57 − 64845.87) / 64845.87 =
−0.0003285` (**0.0329%**). Against the *new* config's loosened threshold (`delta_pct_rev` default
`0.0003`, no BTC override) that's just barely enough to clear gate #3 (`0.0003285 ≥ 0.0003`) —
against the *old* config's stricter `0.0005` it would still have been blocked. **Two compounding
same-day changes, neither alone sufficient:** the mid-cycle-restart bug corrupted the reference
price enough to inflate the apparent move from 0.0084% to 0.0329%, and the same-day config
refresh happened to loosen `delta_pct_rev` just enough (0.0005 → 0.0003) for that inflated,
still-tiny move to clear the gate. Live entered on what was, by the true reference price, a
statistically negligible 0.008% wobble — not a genuine reversal signal — and ate a real $0.53
stop-loss as a direct result.

### 3.2 Corroboration already in the report

The report's own `Entry Δ%` column for this row (independently computed by `trade_reconcile.py`
from the *correct* historical price series via `_cycle_open_close`/`_underlying_price_at`, not
from anything live logged) already reads **`-0.0%`** — rounds from the same ≈−0.0084% this audit
derived by hand from the raw ticks. That column was sitting in the report the whole time as a
quiet second confirmation that the true underlying move was near-zero.

## 4. Conclusions / status (original same-day pass, before §5's fix)

- **Row 1 (WIN):** confirmed config-drift artifact of the recon script, per the already-tracked
  README TODO — no new bug. Fixing `trade_reconcile.py`/`backtest.rs` to accept a pinned
  historical config would make the false `BT DID NOT FIRE` go away. Whether it resolves all the
  way to a clean `MATCH` was, at this point in the investigation, still an open question — see
  §5, where it's implemented and checked.
- **Row 2 (STOPLOSS):** not a recon-tooling gap at all. The mid-cycle-restart bug
  (`fix_live_deploy_2026-07-15.md`) was already found and fixed today, independent of this
  audit — this section is corroborating evidence for that fix, tying it concretely to the
  `-$0.5273` stop-loss's `Entry Δ%` reading in this specific report, and to the loosened
  `delta_pct_rev` threshold as a second, compounding factor worth knowing about (not itself a
  bug — 0.0003 is a deliberately-chosen walk-forward parameter — but it did make this particular
  corrupted-reference-price entry slip through where the old 0.0005 would not have).

## 5. Implementing the fix, and what actually happened

Per the user's follow-up ask, implemented the README TODO's proposed fix rather than leaving it
recommended-but-undone:

- **`config::load_file`** (`trader/src/config.rs`) — loads one exact `strategy_*.toml` by path,
  bypassing `load_latest`'s directory-glob "newest file wins" selection. `load_latest` itself
  refactored to call it.
- **`backtest --config-file <path>`** (`trader/src/bin/backtest.rs`) — new flag, mutually
  exclusive with `--config-dir`; unset behavior (today's `load_latest`) is unchanged.
- **`trade_reconcile.py::build_config_timeline`** — reconstructs which `strategy_*.toml`
  `config::load_latest` would have resolved to at any past timestamp, from each config file's
  git *first-commit* time (`_file_first_commit_ts`, `--diff-filter=A`) — not the date embedded in
  its own filename, which can lag the real commit (`strategy_20260715.toml` was committed
  ≈08:58 HKT, not midnight). Config files are never deleted in this repo, so "the latest file as
  of past time T" is fully reconstructable this way.
- **`run_backtest_reconciliation`** now runs the `backtest` binary once per distinct config era
  the window touches (`_resolve_config_files_for_window`), and keeps each cycle's row only from
  the run whose config was actually active at that cycle's own timestamp
  (`config_file_at(timeline, cycle_ts)`) — not "whichever run happened to produce a row for that
  slug," since two eras can each legitimately fire (or not) for the same slug. Falls back to the
  pre-fix behavior (single latest-file replay, no per-cycle filtering) if git history can't be
  reconstructed for any file, so this degrades the same way every other optional enrichment in
  this module already does rather than breaking the report.
- 15 new tests (`trader/src/config.rs` x2, `trader/scripts/test_trade_reconcile.py` x13) covering
  `load_file`, the timeline reconstruction (single file, mid-window change, file added after the
  window, git-unavailable fallback), `config_file_at`'s segment lookup, and — the actual point of
  the feature — a window spanning a real config change resolving to exactly one row per cycle,
  each with the outcome from the config genuinely active at that moment, not one row per config
  run. `cargo test`/`cargo clippy`/`cargo fmt` and the Python suite (98 tests) all clean; the
  same 4 pre-existing, unrelated `config`/`config_log` test failures persist (config-drift against
  today's real `strategy_20260715.toml`, tracked in README `## TODO` since 2026-07-09).

### Regenerated report — the honest result

Re-ran `trade_reconcile.py --today` after the fix. The false `BT DID NOT FIRE` is gone — but the
row resolves to `OUTCOME DIFF`, not `MATCH`:

| Time | Side | Entry Px | Live Outcome | Live PnL | BT Outcome | BT PnL | Status |
|---|---|---|---|---|---|---|---|
| 2026-07-15 08:55:00 | UP | 0.9000 | WIN | +0.1041 | UNWIND | +0.0278 | OUTCOME DIFF (live=WIN bt=UNWIND) |

**Entry price matches exactly (0.9000)** — the config-pinning fix worked precisely as intended;
this is no longer a blind "didn't fire," it's a specific, explainable outcome disagreement. That
disagreement is **not a bug**: `machine.rs` (the `backtest`/`siglab` replay engine — a different,
simpler implementation from `worker.rs`, the live driver) force-closes any still-held position,
labeled `Unwind` at whatever price is showing, once fewer than `FORCE_UNWIND_BEFORE_CYCLE_END_SECS`
(10.0s) remain before cycle close — *regardless of whether a real take-profit/stop-loss/timeout
condition fired*:

```rust
// trader/src/machine.rs
/// Seconds before cycle-end at which a still-open position is force-closed, labeled
/// `Outcome::Unwind` regardless of whether the take-profit price was actually reached.
/// Added 2026-07-14 so a position entered late in a cycle can no longer ride to a natural
/// WIN/LOSS cycle-close... `siglab`/`backtest.rs` path only — `worker.rs` (the live
/// driver) is untouched; see `siglab/doc/incident_same_entry_ts_2026-07-14.md`.
const FORCE_UNWIND_BEFORE_CYCLE_END_SECS: f64 = 10.0;
```

This trade entered at **T-19s** (19 seconds before cycle close) — inside the replay engine's
10-second force-close window a few seconds later, so `machine.rs` closes it early at whatever
price it saw then (0.9250 → +0.0278), while `worker.rs` (live) has no such rule and legitimately
held the extra ~9 seconds to the real market resolution, landing at 1.0000 (a genuine WIN,
+0.1041). This is a **known, already-documented, deliberately-scoped** asymmetry — added
yesterday specifically to fix a different problem (`siglab`'s same-entry-timestamp report
artifact) and its own doc comment explicitly says live is untouched. It will produce this exact
`OUTCOME DIFF` shape for *any* live trade that enters in a cycle's final ~10-20 seconds and holds
to natural resolution — a residual, expected source of Live-vs-BT disagreement, separate from
(and now no longer confused with) config drift.

**Side finding:** the 2026-07-14 23:04:36 STOPLOSS row and the 2026-07-14 20:39:55 UNWIND row
also changed on regeneration. The UNWIND row now shows a clean `MATCH` (it had previously been
mis-attributed to `live halted: manual /halt 08:10–01:59` — a coincidental overlap with an
unrelated halt window, not the real cause, which was also config drift). The STOPLOSS row flipped
from `BT DID NOT FIRE` to `OUTCOME DIFF (live=STOPLOSS bt=UNWIND)`, very likely the same
`FORCE_UNWIND_BEFORE_CYCLE_END_SECS` mechanic (entered at T-39s, held into the final-seconds
window before a real stop-loss would have fired) — not independently re-verified tick-by-tick
here, flagged for whoever looks at this report next rather than left silently unexplained.

**BT vs Live** ("cycles live missed") also changed: 17 cycles now (was 10), would-be PnL +1.2251
USDC (was +1.9785) — the corrected, per-era-config numbers; the old figures were computed against
a mix of right and wrong configs depending on which side of 08:58 each cycle fell.

### Bottom line

The config-drift gap is fixed and verified against the exact case that motivated it. It does
**not** make every historical Live-vs-BT row agree — it makes the comparison ask the right
question, which sometimes surfaces a *real*, previously-hidden, already-understood limitation
(`machine.rs` vs `worker.rs` near-cycle-close behavior) instead of masking it behind "config
changed, ignore this row." That's the correct outcome for a diagnostic tool, even though it isn't
the clean "match" a first guess might have hoped for.
