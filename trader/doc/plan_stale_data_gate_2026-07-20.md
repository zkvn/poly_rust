# Plan — never trade on stale data, Telegram output fixes, and a fresh 24h paper window

Status: **plan, approved by user feedback on `trader/doc/audit_48hr_unwind_maker_2026-07-20.md`,
implementing now.** Covers: (1) inverting the p(up) gate's fail-open behavior to fail-closed on
stale/missing data, (2) five Telegram/`/status` output fixes from the audit (items 2-6, user
approved with amendments), (3) discarding the current 48h paper run in favor of a fresh 24h one
with parameters re-picked from the delta∈[0.0003,0.0008] band, favoring win rate. Binance-side
feed-quality improvements are a **separate, not-yet-implemented** proposal:
`price_feed/doc/plan_binance_ws_quality_2026-07-20.md`.

## 1. The gate: fail-closed on stale/missing indicator data

### 1.1 What changes

`worker.rs`'s p(up) negative-edge gate (`try_enter`, `trader/src/worker.rs:1195-1230`) currently
has two outcomes: `Veto` (fresh data, bad edge — blocks the entry) and `SkippedNoData` (missing
or stale data — **does not** block the entry, "fails open"). Per user directive: **stale
information is not acceptable at all; not trading is always fine, trading on stale data is not.**
`SkippedNoData` becomes `StaleBlocked` and now blocks the entry exactly like `Veto` does — the
`return vec![Action::PupGateNote{outcome: Veto, ...}]` early-return shape, not appended after an
entry that still proceeds.

This is a deliberate reversal of `plan_unwind_5u_maker_2026-07-19.md` §2.3's original premise ("a
dead indicator daemon must never silently block trading") — that premise optimized for uptime
over correctness. The user's call is the opposite trade-off, and it's now the codified project
principle (`CLAUDE.md` "Trading principles", `README.md`).

### 1.2 Freshness threshold: 10.0s → 2.0s, and unified — no separate display setting

`PUP_GATE_MAX_AGE_SECS` (`trader/src/worker.rs:86`) changes from `10.0` to `2.0`. Per user: *"I
don't want to have separate setting for display, it should be consistent with trading config to
make it simple."* Today there's actually still one lingering split even after the 2026-07-19
unification: `bin/live.rs:2566`'s plain-console heartbeat `ind[...]` display uses
`toml.indicator_max_age_secs` (a separate 5.0s-default config field), not
`PUP_GATE_MAX_AGE_SECS` — every Telegram-facing display already uses the gate's own constant
(2026-07-19 fix, `asbuilt_unwind_5u_maker_2026-07-19.md` §4), but this one console-only path was
missed. Closing it for real:
- `StrategyToml.indicator_max_age_secs` field, its `#[serde(default)]` fn, and the doc comment
  contrasting it with `PUP_GATE_MAX_AGE_SECS` are **deleted** (`trader/src/config.rs`).
- `bin/live.rs:2566`'s heartbeat call switches to `PUP_GATE_MAX_AGE_SECS` directly.
- Net result: **one constant**, `PUP_GATE_MAX_AGE_SECS = 2.0`, used by the gate and every display
  (Telegram + console heartbeat + `/status`) with no exceptions.

### 1.3 Telegram warning on a stale-blocked entry, debounced

Today `SkippedNoData` is console-only (`[PUP-GATE] ... pup_gate=SKIPPED_NO_DATA`). Since it now
means "we didn't trade," per the user this needs a Telegram warning so a degraded/dead indicator
is visible, not silently invisible. But `try_enter` re-evaluates every tick while the underlying
strategy signal stays latched (confirmed from the audit's own log evidence: 157+ consecutive
identical `[PUP-GATE] DOGE VETO ...` lines from one stale snapshot) — an unthrottled per-tick
Telegram alert would spam badly. Add `pup_stale_notified: bool` to `AssetSlot`
(`trader/src/bin/live.rs`), same first-trigger-only pattern as `sl_notified`/`timeout_notified`:
reset to `false` at cycle-open (`bin/live.rs:2616-2618`), set `true` on the first `StaleBlocked`
note per cycle, gating the Telegram send (console `[PUP-GATE]` line still logs every check,
unthrottled, for debugging).

Message: `⚠️ <b>{asset}</b> ENTRY BLOCKED — stale indicator | {dt} | T-{time_left}s | {arrow} |
{strategy}\nwould-be price={price:.4} | p_up last seen {age or "never"}`. Distinct icon (⚠️) and
wording ("BLOCKED") from the existing bad-edge `VETO` console tag, so the two failure modes read
differently even though both now prevent the trade.

**Expected outcome once `price_feed/doc/plan_binance_ws_quality_2026-07-20.md` lands**: "hopefully
after [the] binance ws issue is fixed there won't be many warnings" (user). Until then, a real
increase in blocked-entry frequency for low-liquidity assets (DOGE especially) is expected and
correct, not a regression — that's the trade-off being made deliberately.

## 2. Telegram/`/status` output fixes (audit items 2-6)

All in `trader/src/bin/live.rs` unless noted.

### Item 2 — entry/exit resting notifications, merged

- Rename `"📝 {asset} Maker quote resting"` → `"📝 {asset} ENTRY quote resting"` (explicit label;
  today it's unambiguous only because it's the only resting-order Telegram message that exists).
- New: when the exit take-profit GTC sell is placed, send `"🎯 {asset} ENTRY filled → EXIT quote
  resting | {dt} | T-{time_left}s | {arrow} | {strategy}\n{shares:.2}sh @ {entry_price:.4} → exit
  target {price:.4}"`. `Action::PlaceLimitSell` (`trader/src/worker.rs:331-334`) gains `side:
  Side, entry_price: f64` — both already in scope at its one production call site
  (`finalize_entry_fill`, `worker.rs:1462-1514`), so no new state-threading needed.
- **Merge, not two messages**: confirmed from `README.md`'s own "Order flow per trade" section —
  the exit order is placed in the *same synchronous action batch* as the entry-fill confirmation,
  always, by construction (`finalize_entry_fill` builds `[PlaceLimitSell, Persist]` directly).
  Scoped to the **maker-entry path only** for this change (`via_maker_entry: bool`, also added to
  `PlaceLimitSell`, `true` when called from the `EnteringMaker` state transition) — that path has
  no existing fill-time notification, so the new merged message cleanly replaces nothing with one
  thing. The FAK path already sends "📋 Order placed" at fill time with different diagnostic
  content (latency breakdown); merging that one too would need restructuring its notify timing to
  learn the follow-up exit-order outcome first, and the FAK path isn't part of the active
  configuration (100% maker-entry reversal) — left as console-only for FAK, same as today, a
  deliberate scope-narrowing noted here rather than silently dropped.

### Item 3 — "cycle:" relabeled, duration added, full exit-type breakdown

Trade-close message (`bin/live.rs:1701-1707`):
- `cycle: $X→$Y` → `cycle open→exit: $X→$Y` (still `cycle_open_binance()` → `last_binance`, just
  named for what it actually is — the underlying's move from cycle-open to this trade's exit,
  not the trade's own entry/exit lifespan). Synchronous single-threaded execution means
  `last_binance`/the indicator-store read at print time already *is* "as of exit" to the
  precision that matters here (the close message prints immediately after the close event, same
  tick) — no new capture/plumbing needed to make that true, just the honest label.
- Add `dur={:.0}s`, computed `now_secs_f64() - rec.entry_ts` at message-print time (no exact
  `exit_ts` field exists on `TradeRecord` today; this is a faithful approximation given the
  message fires immediately after the close, not worth a new field for the precision gained).
- `{}W/{}L` → `{}W/{}L/{}SL/{}UW/{}TO`, reusing `slot.stoplosses`/`slot.unwinds`/`slot.timeouts`
  (already tracked, already rendered in this exact order by `/status`'s per-asset and `Session:`
  lines — `bin/live.rs:1176`, `:1205`) instead of the current 2-of-5 subset that reads as "no
  trades yet" on every `UNWIND` close (which counts toward neither W nor L).

### Item 4 — one-sided edge display

`fmt_indicator` (`bin/live.rs:814-839`) gains `side: Option<Side>`. `Some(side)` (both Telegram
call sites already know the traded side — `PlaceLimitBuy`'s `*side`, `LogTrade`'s `rec.side`):
show only that side's edge, no `UP`/`DN` tag (redundant with the `↑`/`↓` arrow already in the
same message) — `ind: p_up=0.0304 (edge-0.2196) vol=1.16e-3`. `None` (console heartbeat, not tied
to one trade, `bin/live.rs:2565`): unchanged, both edges shown.

### Item 5 — boot banner size + timestamp

Replace the single fleet-wide `size=$X.XX` (`args.size_usdc`, wrong under maker-entry — same bug
`/status` already fixed 2026-07-19, README flagged the banner as the deferred half) with a
per-slot summary reusing `/status`'s own `size_str` logic (`bin/live.rs:1142-1146`):
`"{asset}:{strategy} ({size_str})"` joined per slot, replacing the single shared
`asset_strategy_summary` + trailing `size=`/`max_trades=`. Add `| {dt}` (`hkt_now()`, same
pattern as every other notification in this file).

### Item 6 — `/status` `start=120s` → unambiguous

Confirmed via `SawLevelSignal`'s own doc comment (`trader/src/signal/saw_low.rs:1-8`):
`reversal_start_time` is seconds-*remaining* when the entry window opens, not seconds elapsed.
`start={s:.0}s` → `entry_window=T-{start}s..T-{no_enter}s`, reusing the `T-{time_left}s`
convention already used everywhere else in this file. Also add a doc comment on
`reversal_start_time`'s two field declarations (`trader/src/config.rs:69,172`) pointing at
`saw_low.rs`'s existing correct explanation, so the direction doesn't have to be re-traced through
three files next time.

## 3. New 24h paper window — parameters re-picked from the delta∈[0.0003, 0.0008] band

### 3.1 Why

Per user: not many trades have fired in the current 48h window (`delta_pct_rev` was already
loosened mid-run for SOL/DOGE as a live test of exactly this constraint —
`README.md`'s 2026-07-19 "delta_pct_rev loosened" entry). Rather than continue an under-firing
48h window, discard it and relaunch for 24h with every asset's `delta_pct_rev` deliberately
bounded to `[0.0003, 0.0008]` (loose enough to fire meaningfully more often than the current
table's `0.0008-0.0010` values), re-selecting the rest of each asset's reversal parameters
(`reversal`, `reversal_low_threshold`, `unwind_pnl_rev`, `unwind_time_rev`) to **favor win rate**
within that bounded band, using the same source study the current config was built from
(`../btc_5mins/studies/unwind_safely/full_history_sweep.py`, calibration window
2026-05-26→07-09). Per user: *"since it's paper trader, I wouldn't worry too much about losing
money, I care more about the consistency of results"* — win-rate selection (not PnL/ppt) directly
targets that: more, more-consistent trades over 24h, not necessarily higher-PnL rarer ones.

### 3.2 Method

The existing `results/full_history_*_20260708_153401.{md,json}` artifacts only persist each
asset's **top-10** rows per metric — dominated by `delta_pct_rev=0.0010`, outside the requested
band, so they can't be filtered down to it. Re-ran the *exact same* sweep function the study
already uses (`_sweep_reversal`, unmodified, same `REV_DELTA_PCTS = [0.0003, 0.0004, 0.0005,
0.0006, 0.0008, 0.001]` grid — the requested band is a strict subset already covered by the
existing sweep dimensions, nothing new to compute), capturing the **full** per-asset result grid
(15,444 combos/asset) instead of just its top-10, for all 6 assets. For each asset: filter to
`delta_pct_rev <= 0.0008` and `trades >= 20` (the study's own existing "qualified" bar,
`MIN_TRADES` in `full_history_sweep.py`), then take the single highest-`win_rate` row.
`sl_pnl_rev`/`sl_reversal` from that row are **not** used — the stop-loss policy stays the
already-decided `sl_pnl_rev = sl_reversal = 0.0` (unwind_time_rev is the stop), per
`plan_unwind_5u_maker_2026-07-19.md` §1.2 Scenario A, unrelated to this re-pick.

### 3.3 Selected parameters

*(Filled in once the background sweep finishes — see the companion commit that adds
`trader/config/strategy_20260720_24h.toml` with each asset's picked row and its `trades`/
`win_rate` cited in the file's own `meta.source`, same convention as `strategy_20260719.toml`.)*

### 3.4 What happens to the current run's data

Same archival precedent as the 2026-07-19 mid-run restart (`live_logs/
archive_paper_run_20260719_mid_pricing/`): current `paper_trades_*`/`paper_quotes_*`/
`paper_pup_vetoes_*`/`live_state_*`/`control_log.jsonl` on Oracle move to
`live_logs/archive_paper_run_20260720_48h_discarded/` before the new config deploys. Fresh CSVs
start at restart. The §2.7-style evaluation for this new window reads only the new files, notes
in the eventual report that the archived 48h data is a separate, non-comparable cohort (different
config, mid-window binary change).

## 4. Test plan

- `cargo test` (full `trader` suite) — existing `PupGateOutcome`/`fmt_indicator` unit tests
  updated for the renamed variant/new signature; new tests: gate blocks (not just notes) on
  `StaleBlocked`, `pup_stale_notified` debounce (fires once per cycle, resets on cycle-open),
  `fmt_indicator`'s one-sided-edge rendering, `PlaceLimitSell`'s new fields flow through
  `finalize_entry_fill` correctly for both maker and FAK-with-GTC-exit paths.
- `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --all --check`.
- Read through the actual rendered strings for at least one of each changed message (unit-test
  assertions on exact format, not just "compiles") before deploying.

## 5. Rollout order

1. This doc + `CLAUDE.md`/`README.md` principle updates — pushed first (done).
2. Implement §1 (gate) + §2 (display) in `trader/`, full local test pass.
3. Generate `strategy_20260720_24h.toml` from the sweep results (§3), fill in §3.3 above.
4. Deploy: `scripts/deploy_oracle.py --trader-only` (binary + config together — the gate/display
   changes and the new config are landing in the same restart, not staged separately, since the
   old config paired with the new binary is a fine transient state but there's no reason to pay
   for two restarts).
5. Archive the current run's data (§3.4) as part of the same deploy step, before or immediately
   after restart — confirm `trader-live.service`/`poly-indicator.service` both restart clean, 0
   errors, fresh CSVs accruing rows, `/status` renders the new `entry_window=`/breakdown fields
   correctly on the live process.
6. `price_feed/doc/plan_binance_ws_quality_2026-07-20.md` stays a separate, not-yet-implemented
   proposal — not blocking this rollout (§6 of that doc).
