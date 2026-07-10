# Feature: Backtest Reconciliation in Daily Recon (BT vs Live / Live vs BT)

Goal: extend `trader/scripts/trade_reconcile.py`'s existing daily recon report
with a second, independent cross-check — not "did worker.rs's own outcome
correction match Gamma" (that's the existing `## Gamma Cross-Check` section),
but "does the live trader's behavior match what the Rust backtest engine says
*should* have happened over the same price data." Two tables, both directions:

- **Live vs BT** — for every live trade in the window, what did the backtest
  do at that same cycle+side? (catches live trades the backtest disagrees
  with, or live trades where the backtest didn't fire at all)
- **BT vs Live** — for every cycle the backtest fired a trade, did live also
  trade it? (catches cycles live **missed entirely** — no order sent when
  the engine says one should have fired)

This is a **new section in the existing `trade_recon_<period>.md` report**,
not a new file. Window/cadence are unchanged (8pm→8pm HKT, same
`_resolve_window()`), so **the cron job (`run_daily_recon.bash`, `10 */2 * * *`)
does not need to change** — it already calls `trade_reconcile.py --today`,
which will just do more work per invocation.

## 1. What already exists (reused, not rebuilt)

| Piece | Location | Role here |
|---|---|---|
| Live trade loader + window filter | `trade_reconcile.py::load_and_filter` | already gives us the exact live trade set for the window |
| Price data assembly | `trader/scripts/build_backtest_prices.py` | builds `{asset}_poly_{date}.parquet` + `{asset}_binance_{date}.parquet` into `trader/backtest_prices/` (gitignored) from `price_feed/raw/` shards, with unsealed-file recovery already handled |
| Backtest engine | `trader/src/bin/backtest.rs` (`cargo run --bin backtest -- --asset X --date Y`) | replays one asset/one HKT calendar date, prints trades to stdout; golden-tested against the Python engine (`trader/src/backtest.rs`'s `btc_20260620_golden` test) |
| Reference design | `btc_5mins/scripts/trade_recon_backtest.py` (`_build_actual_vs_bt` / `_build_bt_vs_actual`) | already implements exactly this two-direction reconciliation for the Python bot — port the *logic shape*, not the code (different schema/engine) |

## 2. Gaps to close (the actual implementation work)

1. **`backtest.rs` has no machine-readable output.** It only prints an
   aligned text table to stdout (`main.rs` lines ~53-98) — fine for manual
   runs, unsafe to regex-parse from Python. Add a `--format csv` flag
   (default stays `table`, so existing manual usage / the golden test's CLI
   contract don't change) that prints
   `slug,strategy,side,token_price,exit_price,outcome,pnl` as CSV to stdout.
   Small, additive change to `trader/src/bin/backtest.rs` only — no changes
   to `run_backtest`/`Machine` itself.
2. **Window spans two HKT calendar dates; `backtest.rs` takes one date.**
   Run it twice per asset (`window_start.date()`, `window_end.date()`),
   concat, then filter to `[window_start_ts, window_end_ts)` using each
   trade's **slug-embedded cycle timestamp** (`slug.split('-')[-1]`) — same
   trick `btc_5mins`'s `_build_bt_vs_actual` already uses, and consistent
   with how this project's own recon already treats the slug as the source
   of truth for cycle time (`plan_daily_recon.md` §2). `TradeRecord` doesn't
   need an `entry_ts` column added for this.
3. **Which assets to backtest?** Not just the assets live happened to trade
   — use `cfg.trade_assets` from the **latest `trader/config/strategy_*.toml`**
   (currently `["BTC", "ETH", "DOGE"]`) for every run. Scoping to
   live-traded assets only (as `btc_5mins` does) would make it structurally
   impossible to detect "live didn't trade this asset in the window at all,
   but the backtest says it should have" — which is exactly the failure
   mode this feature exists to catch. Costs 2 backtest runs (2 dates) per
   configured asset per cron invocation, not per live trade.
4. **Config-drift caveat (flagging, not solving here):** `config::load_latest`
   always loads whichever `strategy_*.toml` is lexicographically last in the
   directory **right now** — not "whichever config was live during the
   window." Since this recon only ever looks at the current/recent window
   (never re-backtests old historical dates), this only matters if the
   strategy config changes **mid-window**, in which case the backtest for
   the whole window uses the *new* config even for cycles that traded under
   the *old* one. `btc_5mins` solves this on the Python side via
   `config_log.read_latest_snapshot(asset, ts)` +
   `snapshot_to_bt_overrides` reading the JSONL config-change log
   (`trader/src/config_log.rs` already writes schema-compatible snapshots,
   confirmed present). Closing this gap would mean either (a) a Rust
   `--config-file <path>` override so the script can point at the exact
   historical snapshot, or (b) accept the small inaccuracy for now. **Not
   blocking — most windows won't see a config change mid-window — but
   should land in the README TODO once this section ships**, since a silent
   mismatch here would look like a real bug in the report.
5. **Prebuild the binary.** `cargo run` re-checks/recompiles on every
   invocation; called ~24 times per cron run (2 dates × up to ~3-6 assets,
   more as `trade_assets` grows) that overhead adds up run after run.
   `trade_reconcile.py` should shell out to a **prebuilt**
   `target/release/backtest` binary (built once via
   `cargo build --release --bin backtest`, same as any other release
   artifact in this repo) rather than `cargo run --release --bin backtest`.

## 3. New pipeline steps (inside `trade_reconcile.py`, after existing live-row
loading/Gamma cross-check)

1. Determine `bt_dates = sorted({window_start.date(), window_end.date()})`
   (1 or 2 HKT calendar dates) and `assets = cfg.trade_assets` (read from
   `trader/config/strategy_*.toml` the same way `backtest.rs` does — reuse
   `load_latest`'s file-selection logic, or just glob+sort in Python since
   the script already has no Rust dependency).
2. For each date in `bt_dates`: call
   `build_backtest_prices.py --asset <comma-list> --date <date> --out-dir trader/backtest_prices`
   (subprocess, same venv). Skip an asset/date pair gracefully (log + continue)
   if source files are missing rather than failing the whole recon run — a
   quiet day for one asset shouldn't blank the report for the rest.
3. For each `(asset, date)`: run
   `target/release/backtest --asset <asset> --date <date> --prices-dir trader/backtest_prices --config-dir trader/config --format csv`,
   parse stdout as CSV into rows tagged with `asset`.
4. Concat all rows, derive `cycle_ts` from each row's slug, filter to
   `[window_start.timestamp(), window_end.timestamp())` → `bt_in_window`.
5. **Live vs BT table** — for each live trade row (the same `annotated` list
   already built for the Gamma section, so the code runs once, not twice):
   - look up `bt_in_window` by `(slug, side)` → **MATCH** (outcome equal) or
     **OUTCOME DIFF** (outcome differs, e.g. live `WIN` vs bt `UNWIND` — the
     exact class of gap already documented for the one 2026-07-03 ETH trade
     in `plan_daily_recon.md` §7);
   - else look up by `(slug, opposite side)` → **SIDE DIFF**;
   - else → **BT DID NOT FIRE** (backtest saw the cycle but its own filters/
     halt logic skipped it);
   - if the asset/date pair had no price data at all → **NO PRICE DATA**.
6. **BT vs Live table** — for each `bt_in_window` row, check if **any** live
   trade exists for that `(slug, side)` (both sides checked — a live trade on
   the *opposite* side already shows up as SIDE DIFF in the other table, so
   only rows with **no live trade on either side** land here) → **LIVE
   MISSED**. Report count, would-be PnL (`sum(bt_pnl)` for missed rows,
   labelled "would-be gain/loss" like `btc_5mins` already phrases it), and a
   flag if `n_missed > 0` in the section's summary line — this is the
   headline number the user asked for ("any trade missed in live").
7. Render both tables (reuse the existing markdown-table helper pattern in
   `write_markdown_summary`) under a new `## Backtest Reconciliation`
   section, appended **after** the existing `## Gamma Cross-Check` section
   (last in the file) — keeps every earlier section's position stable across
   historical reports, so old reports/links aren't reshuffled by this change.
8. Section always renders, even on a 0-live-trade day (the current
   `write_markdown_summary` early-returns a stub before this point when
   `perf_stats` is empty — **that early return needs to move to after** the
   BT vs Live check, since a "0 live trades" day is exactly the case where a
   missed-trade report is most valuable, not the case to skip it in).

## 4. Report format (sketch)

```
## Backtest Reconciliation

> Live vs BT: 2 matched, 1 outcome-diff, 0 side-diff, 0 bt-not-fired | BT vs Live: 1 missed (would-be PnL +0.31)

### Live vs BT

| Time | Asset | Strategy | Side | Live Outcome | Live PnL | BT Outcome | BT PnL | Diff PnL | Status |
|---|---|---|---|---|---|---|---|---|---|
| ... |

### BT vs Live (cycles live missed)

| Cycle (HKT) | Asset | Strategy | Side | BT Outcome | BT PnL | Status |
|---|---|---|---|---|---|---|
| ... |
```

## 5. Non-goals

- Not replacing the existing Gamma cross-check — that's checking
  worker.rs's own WIN/LOSS correction against Polymarket's own resolution;
  this is checking live behavior against the backtest engine's replay.
  Different failure classes, both worth keeping.
- Not fixing the config-drift gap (§2.4) in this pass — flagged for README
  TODO once this ships.
- Not adding a `--config-file` override to `backtest.rs` in this pass unless
  the config-drift gap turns out to bite in practice.

## 6. Execution checklist (not yet started)

- [ ] Add `--format csv` to `trader/src/bin/backtest.rs` (additive, default
      unchanged) — code change, needs confirmation before editing per
      project convention.
- [ ] `cargo build --release --bin backtest` and confirm CSV output shape.
- [ ] Add the pipeline steps above to `trader/scripts/trade_reconcile.py`
      (new functions: asset/date resolution, price-build subprocess calls,
      backtest subprocess calls + CSV parse, Live-vs-BT / BT-vs-Live table
      builders, markdown rendering) — code change, needs confirmation.
- [ ] Move the no-trade early return in `write_markdown_summary` to after
      the BT-vs-Live check.
- [ ] Run manually for today's window, sanity-check both tables against the
      known 2026-07-03 ETH UNWIND/WIN mismatch case as a regression check.
- [ ] Add config-drift caveat (§2.4) to README `## TODO`.
- [ ] No cron change needed — confirm one live `run_daily_recon.bash` run
      end-to-end after the script change lands.
