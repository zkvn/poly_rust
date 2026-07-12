# Incident — BT vs Live discrepancies have no explained reason in the recon report, 2026-07-12

## Problem statement

`trade_reconcile.py`'s "Live vs BT"/"BT vs Live" tables report *that* live and the backtest replay
disagree (`MATCH` / `OUTCOME DIFF` / `SIDE DIFF` / `BT DID NOT FIRE` / cycles in the "missed"
table), but never *why*. For the two most recent reports
(`trade_recon_2026-07-10_to_2026-07-11.md`, `trade_recon_2026-07-11_to_2026-07-12.md`) that's 9
"BT DID NOT FIRE" rows, 1 "OUTCOME DIFF", and 7 "cycles live missed" rows with zero explanation
attached. This doc root-causes as many of those as the available evidence supports, and proposes
how the script itself should surface the reason instead of a bare status string.

## Method

`live.log` heartbeat lines carry `slug=<slug> T-<N>s`; since the 5-min cycle boundaries are shared
across all assets, any nearby line's real wall-clock time can be recovered as
`slug_cycle_ts(slug) + 300 - T` (same technique already used in
`trader/doc/audit_retry_doge_2026-07-03.md`). A one-off script walked `live.log` linearly, tracked
the most recently-seen heartbeat's reconstructed timestamp, and attached that timestamp to every
halt/resume/gamma-timeout/balance-drawdown log line it passed next (±1 log line in every case
checked, i.e. accurate to well under the ~30s heartbeat interval).

## Root cause 1 (confirmed, dominant): backtest never models live's balance-drawdown halt

`live.log` reconstruction for the 2026-07-10 20:00 → 2026-07-12 20:00 HKT window:

```
~2026-07-11 14:51:57 HKT :: [live] BALANCE DRAWDOWN >25% from session baseline — halting new entries on all assets.
~2026-07-12 07:52:27 HKT :: [telegram] sent: ▶️ Resumed all assets (BTC, ETH, DOGE).
```

Live was suppressed on **every asset** for ~17 hours. Cross-checking the `trade_recon_2026-07-11_to_2026-07-12.md`
"BT vs Live (cycles live missed)" table — 6 rows total — against that window:

| Cycle (HKT) | Inside 14:51:57 → 07:52:27 halt window? |
|---|---|
| 2026-07-11 21:20:00 | ✅ yes |
| 2026-07-11 23:45:00 | ✅ yes |
| 2026-07-11 23:50:00 | ✅ yes |
| 2026-07-12 00:00:00 | ✅ yes |
| 2026-07-12 00:25:00 | ✅ yes |
| 2026-07-12 09:55:00 | ❌ no (after 07:52:27 resume) |

**5 of 6** missed cycles fall exactly inside the halt window. Live wasn't beaten by the backtest's
strategy logic here — live was never allowed to enter at all, by design (a real >25% session
balance drawdown, a legitimate safety trip), and the backtest has **zero way to know that**:
`run_backtest_reconciliation` never passes `--no-halt` to the `backtest` binary, so the replay
*does* run with halt logic active (`src/backtest.rs`'s `HaltTracker`, `run_backtest` lines
492-493) — but that tracker only counts **loss-streak halts** (`HaltTracker::record_trade`,
`is_loss_for_halt`). There is no balance/drawdown concept anywhere in `backtest.rs` — the whole
mechanism (`worker.rs`'s `BalanceGuard`/`GammaBalanceTracker`, `src/balance.rs`) is live-only and
structurally cannot be replayed by the current backtest engine. Every backtest run also starts
`HaltTracker::new(...)` fresh (`losses: 0, last_session: None`) per `(asset, date)` — it has no
memory of anything live actually did, loss-streak or otherwise, prior to that call.

The 6th row (2026-07-12 09:55:00, DOGE reversal UP) is **not** explained by this halt — see
"Unexplained residual" below.

## Root cause 2 (confirmed, structural): `machine.rs` never implements `unwind_time`/`Outcome::Timeout`

The one `OUTCOME DIFF` row across both reports:

```
| 2026-07-10 22:49:59 | DOGE | reversal | UP | ... | TIMEOUT | +0.0481 | UNWIND | +0.2256 | ... |
```

```
$ grep -n "unwind_time\|Outcome::Timeout" trader/src/machine.rs
381:            unwind_time_rev: 0.0,
388:            unwind_time_hp: 0.0,
```

`machine.rs` (the backtest replay engine) never references `Outcome::Timeout` at all, and its
`unwind_time_*` fields only appear in test fixtures, always set to `0.0` (disabled). Only
`worker.rs` (the live driver) implements the timeout-exit path (`worker.rs:973`,
`tick.ts - h.entry_ts >= self.unwind_time`). `backtest.rs`'s own `format_table` already has a
comment acknowledging this: *"machine.rs (this replay engine) doesn't implement unwind_time — only
worker.rs (the live driver) does — so this never fires today."* Confirmed still true. **Any** live
trade that resolves via `TIMEOUT` is guaranteed to show as `OUTCOME DIFF` (or `BT DID NOT FIRE`, if
the backtest's own un-timed-out replay of that cycle didn't independently trigger a trade) — this
is not a bug to "find," it's a known, permanent feature gap until `unwind_time` is ported into
`machine.rs`.

## Root cause 3 (checked, ruled out for this window): config-file content drift

`config::load_latest` always picks the lexicographically-last `strategy_*.toml`; for the whole
2026-07-10→07-12 window that's always been `strategy_20260709.toml` — but that **file's content**
changed mid-window:

```
$ git log -p --follow -- trader/config/strategy_20260709.toml
commit 4c3d096  Sat Jul 11 12:52:33 2026 +0800
  feat(trader): extend Gamma poll window to 10min, scope balance-decrease halt to asset+strategy
```

This is genuine config drift — the backtest replays *every* cycle in the window (including ones
before 07-11 12:52) against the post-edit file. Checked the diff itself, though: it only touches
`gamma_poll_interval_secs`/`gamma_poll_deadline_secs` (Gamma polling cadence) and balance-halt
scoping — **not** any entry-decision parameter (`delta_pct_hp`, `enter_when_time_left`,
`halt_rev`/`halt_prob`, etc.). So for *this specific window's* entry/side mismatches, config drift
is ruled out as a cause — but it remains a live structural risk for any future window where an
entry-affecting parameter changes mid-window, and is already tracked as its own item in this
README's TODO ("Backtest reconciliation config-drift gap — flagged 2026-07-10"). A related, even
less tractable version of the same problem: the `backtest` binary itself is always built from
`HEAD` (rebuilt today for this doc), so replay logic itself — not just config — can silently drift
from whatever code was actually live at trade time. Not solved here; noted as a residual
limitation of any config-snapshot fix.

## Contributing factor (new finding, not fully attributed): price_feed tick sparsity

While verifying the Entry Δ%/Cycle Δ% fix (`trader/doc/incident_delta_pct_2026-07-12.md`), found
that local (and Oracle-confirmed) tick coverage for both `poly` and `binance` data collapsed from
~93% of minutes on 2026-07-10 to **~14-15%** on 2026-07-11/07-12, ongoing as of this write-up. A
tick-sparse cycle gives the backtest's tick-by-tick replay far fewer chances to observe the exact
condition that triggers an entry, which can independently cause `BT DID NOT FIRE` (or a different
entry price/time than live actually saw) with no halt or config explanation needed. Not
individually attributed to specific rows here — would need per-cycle tick-count cross-referencing,
not done in this pass — but flagged as a live, ongoing confound for *any* BT reconciliation over
this date range until the collector issue (README TODO) is fixed.

## Unexplained residual

`trade_recon_2026-07-10_to_2026-07-11.md`'s 8 ETH/DOGE `BT DID NOT FIRE` rows and the
2026-07-12 09:55:00 DOGE row in the second report are **not** explained by any of the three root
causes above (outside the halt window, config unchanged in the relevant parameters, outcome isn't
TIMEOUT). Plausible remaining explanations, none confirmed: (a) tick sparsity per "Contributing
factor" above, (b) genuine non-deterministic timing differences between live's real-time tick
arrival and the recorded/rebuilt price series (a tick live acted on within its entry window might
land a few hundred ms outside the window once replayed from recorded data), or (c) an
as-yet-unidentified backtest/live logic divergence. Left open rather than guessed at.

## Proposed solution

**Immediate, script-level (recommended first step):** `trade_reconcile.py` already has the halt
timeline available in principle — `parse_gamma_timeout_events` already parses one *kind* of
halt/continue event from `live.log` for the Gamma Cross-Check section, and this investigation's
one-off script proves the same log carries manual `/halt`/`/resume` and balance-drawdown events
too, all recoverable to real timestamps via the heartbeat-interpolation technique above. Extend
that into a `classify_mismatch_reason(row, halt_windows, config_change_events)` helper called from
`build_live_vs_bt`/`build_bt_vs_live`, producing a `reason` field the tables render as a new
column, in priority order:

1. `outcome == "TIMEOUT"` (live side) → **`"backtest doesn't model TIMEOUT/unwind_time (structural)"`**
2. cycle_ts falls inside a reconstructed halt window → **`"live halted: <halt source> <HH:MM>–<HH:MM>"`**,
   where `<halt source>` is one of `manual /halt`, `balance drawdown >25%`, `balance-decrease
   (asset+strategy)`, or `Gamma-unresolved halt`, distinguished by which log pattern matched
   (all four already appear in `live.log` with distinct, greppable text)
3. cycle_ts falls inside a config-file-content-change window (`git log` on the resolved
   `strategy_*.toml`, or better, a `config_log.rs` snapshot boundary) → **`"config changed mid-window (<param names diffed>)"`**
4. cycle_ts falls inside a known local price-data gap (reuse this doc's coverage-check logic,
   or simpler: flag if the cycle's own tick count from `load_underlying_price_series`/the poly
   series is below some minimum, e.g. <10 ticks) → **`"sparse/missing tick data for this cycle"`**
5. none of the above → **`"unexplained — needs manual review"`**, printed as-is rather than
   dressed up as one of the above. Honesty here matters more than looking complete — the residual
   rows above are the reason category 5 exists at all.

This directly answers the ask: a manual `/halt` or an automatic balance-drawdown halt would show
up as e.g. `"live halted: balance drawdown >25% 14:51–07:52"` right in the table, not as a bare
`BT DID NOT FIRE`.

**Longer-term, architectural (bigger, not proposed for immediate implementation):**

- Port `unwind_time`/`Outcome::Timeout` into `machine.rs` so the backtest can actually reproduce
  live's timeout-exit behavior instead of guaranteed-diverging on every such trade.
- Either model balance-drawdown halts inside `backtest.rs`'s replay (would require faithfully
  replaying `BalanceGuard`'s checkpoint logic against reconstructed balance history — heavy), or
  more pragmatically, treat category-2 rows above as **excluded from the mismatch tally entirely**
  (not a discrepancy at all, since live was correctly following a real safety mechanism) rather
  than trying to make the backtest agree with a decision it structurally can't model.
  Recommended: the exclusion approach — it's simpler, doesn't duplicate live-only logic into the
  replay engine, and matches what the balance-drawdown halt is actually *for*.
- Pin the exact historical config snapshot via `config_log.rs`'s existing JSONL log instead of
  `config::load_latest`, per the original 2026-07-10 config-drift TODO — add a `--config-file
  <path>` override to `backtest.rs`, as that TODO entry already outlines.
- Fix the underlying `price_feed` collector gap (separate README TODO item) so category 4 becomes
  rare rather than a large, currently-live confound.

## Status

Investigation and proposal only, per the request that motivated this doc — no code changes made
here. `trade_reconcile.py`'s Entry Δ%/Cycle Δ% fix (a different, already-implemented change) is
documented separately in `trader/doc/incident_delta_pct_2026-07-12.md`.
