# Plan: Daily Trade Recon for poly_rust (Rust trader)

Goal: reproduce `btc_5mins`'s daily reconciliation report for this project's Rust
live trader, on the same 8pm→8pm HKT cadence, plus a Rust-backtest reproduction
check. Written before making any changes, per instruction.

## 1. Reference: how btc_5mins does it

- `btc_5mins/scripts/trade_reconcile.py` — reads `log/trades_*.log` (CSV,
  columns include `time` (HKT string), `asset`, `side`, `entry_type`, `cost`,
  `pnl`, `result`, plus signal fields `snr`/`edge_spread`/`delta_pct`/`vol_har5`).
  For the windowed modes (`--today`/`--dt`) it:
  1. Resolves a 24h window anchored at **8pm HKT** (`_resolve_mode`).
  2. Loads+filters all `trades_*.log` rows into that window, dedupes.
  3. Cross-checks each trade's slug against the Gamma API for the actual
     UP/DOWN resolution (`build_outcome_map_gamma`), annotates `actual_result`.
  4. Computes performance stats (per-asset, per-strategy, per-entry-type,
     stop-loss/unwind quality, full trade history).
  5. Writes `results/daily_recon/trade_recon_<period>.md` and git commit+pushes it.
  6. A companion `trade_recon_backtest.py` does the same but compares against
     the **Python backtest engine's** reproduction of the day, for a second
     "did the algo behave as designed" cross-check.
- Cron (`scripts/bash/run_daily_recon.bash`) runs **every 2 hours**
  (`10 */2 * * *`), not once a day — the 8pm anchoring is computed *inside*
  the Python script, so re-running it mid-window just refreshes the same
  `trade_recon_today.md`-equivalent file idempotently. This project will copy
  that pattern rather than trying to fire cron at exactly 20:00.

## 2. What's different in poly_rust

| | btc_5mins | poly_rust (this project) |
|---|---|---|
| Trade log location | local `log/trades_*.log` (bot runs locally) | **on Oracle** (`ubuntu@10.8.0.1:/home/ubuntu/apps/poly_rust/trader/live_logs/`) — must be synced first |
| File naming | one file per bot-session start | one file **per (asset, strategy)**: `live_trades_<asset>_<strategy>.csv` (e.g. `live_trades_eth_high_prob.csv`) |
| CSV columns | `time,asset,side,entry_type,cost,pnl,result,api_result,exit_fill_price,sl_trigger_price,p_up,snr,edge_spread,delta_pct,vol_har5` | `logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl` (`trader/src/types.rs::TradeRecord`) |
| Timestamp | `time` = HKT string `YYYY-MM-DD HH:MM:SS` | `logged_at`/`entry_ts` = Unix epoch seconds (float, UTC) — window filtering is arithmetic, no tz parsing needed |
| Market window | derived from `time` + asset → slug | **already in the row**: `slug` embeds the window ts directly (e.g. `btc-updown-5m-1782971400`) |
| Cost/size | variable `cost` per row | fixed `trade_size_usdc` (currently `$1`, from `.env` `TRADE_SIZE_USDC`) for every trade — no `cost` column needed |
| API cross-check | needed (bot logs algo's own result, may be wrong) | **partially built-in**: `worker.rs`'s `Confirming` state already reconciles WIN/LOSS against Polymarket's own `ApiResult` before logging (`Action::LogTradeCorrection`) — but STOPLOSS/UNWIND rows are exits, never corrected. Doing our own Gamma cross-check is still useful as an independent check on the correction logic itself. |
| Wallet | `0x9FC2A777C26CCA2C218D8E7BBC340D14058CC13A` (hardcoded in `run_daily_recon.bash`) | **different wallet** — `trader/.env`'s `FUND_ADDRESS=0xdc1C843e94083491FD5383a64F97336845548572` (new account per project memory: "live trading started on new account") |
| Signal/audit fields | snr, edge_spread, delta_pct, vol_har5, sl_trigger_price | **not logged** — `TradeRecord` has no signal snapshot at entry, so the "Stoploss & Unwind Audit" section (which reconstructs CLOB price history around the trade) can be ported, but the per-trade signal table (SNR/edge/delta/vol) cannot until worker.rs logs those fields. Noted as a gap, not blocking. |

## 3. Daily recon script design

New file: `trader/scripts/trade_reconcile.py` (adapted from btc_5mins's, same
CLI shape: `--today` / `--dt YYYYMMDD` / `--wallet` / `--no-push`).

- **Input:** `trader/live_logs/live_trades_*.csv` (all asset/strategy files),
  each tagged with its `<asset>_<strategy>` from the filename since `TradeRecord`
  doesn't carry asset as a column.
- **Window:** identical 8pm→8pm HKT anchor logic, but filtering is
  `window_start_ts <= logged_at < window_end_ts` directly on the epoch floats
  (no string parsing/timezone conversion needed — simpler than the original).
- **Outcome resolution:** `outcome` column already has the final WIN/LOSS/
  STOPLOSS/UNWIND result (post any live correction). Still queries Gamma
  (`slug` → `/events?slug=...`) to independently verify each WIN/LOSS row,
  same mismatch-detection idea as the original, but now it's checking
  "did worker.rs's own correction logic get it right" rather than "did the
  algo predict correctly" — a regression check on `on_api_result` more than
  a prediction-accuracy check.
- **Performance section:** per-asset, per-strategy (mapped from the
  `<asset>_<strategy>` filename tag, not an `entry_type` column), stop-loss
  detail, full trade history, stoploss/unwind audit (CLOB price history via
  Gamma + `/prices-history`, same as original — no local parquet fallback
  needed since this project doesn't keep the exact schema the original
  fallback expects; Gamma/CLOB API is sufficient).
- **Output:** `trader/results/daily_recon/trade_recon_<period>.md`.
- **No-trade stub:** same idea — always write a file so the report exists
  even on quiet days (current live trade volume is very low: as of writing,
  Oracle has exactly 1 completed trade in `live_trades_eth_high_prob.csv`,
  the rest are header-only).
- **Git commit+push:** ported as-is (opt out with `--no-push`), operating on
  this repo instead of btc_5mins's.

## 4. Cron schedule

`trader/scripts/bash/run_daily_recon.bash` — same shape as btc_5mins's wrapper:
cd to repo root, resolve SSH agent socket for git push, call the venv Python.

```
10 */2 * * *  bash /home/kev/apps/poly_rust/trader/scripts/bash/run_daily_recon.bash >> /home/kev/apps/poly_rust/trader/log/recon_cron.log 2>&1
```

Uses `btc_5mins/venv` (already has `requests`/`rich`; no new venv needed).
Runs every 2h like the original — the script's internal 8pm anchor means every
run just refreshes the current window's report idempotently, so a report
always exists no more than ~2h stale, and the final 8pm-crossing run produces
the frozen "yesterday 8pm → today 8pm" file. **This does not sync from Oracle
by itself** — see next section.

### Missing piece: log sync from Oracle isn't automated yet

Trade logs live only on Oracle until synced. Today's run is a **manual** sync
(this session). For the cron to be self-sufficient, either:
- (a) add an `rsync` step at the top of `run_daily_recon.bash` (simplest —
  matches `sync_oracle.sh`'s existing pattern for price data), or
- (b) leave sync manual/on-demand and accept the recon report reflects
  whatever was last synced.

**Recommendation: (a)** — add the rsync step, since the whole point of an
automated daily report is not needing to remember a manual step. Flagging
this as a decision point rather than assuming it silently.

## 5. Backtest reproduction (Rust engine)

`cargo run --bin backtest -- --asset BTC --date 2026-07-02` already exists
(`trader/src/bin/backtest.rs`, mirrors Python `bot.backtest.run_backtest`
exactly per its own doc comment, golden-tested against Python in
`trader/src/backtest.rs`'s `btc_20260620_golden` test).

**Problem found:** its `load_price_data` needs one of:
- `{asset}_binance_{date}.parquet` + `{asset}_poly_{date}.parquet` (single
  file each, one full HKT day), or
- `{asset}_binance.parquet` + `{asset}_poly.parquet` (single merged file,
  date-filtered in code).

Neither exists in a form that's *current*:
- `btc_5mins/prices/{ASSET}_poly.parquet` (the merged fallback) is **stale**
  — `BTC_poly.parquet` last modified 2026-07-01 17:53, while
  `BTC_binance.parquet` is fresh (updated continuously, last 2026-07-03
  12:03). The two price streams have silently diverged: something stopped
  writing poly data into this file two days ago (plausibly when the old
  Python collector was retired in favor of this project's own Rust
  `price_feed`), while binance collection kept going independently.
- This project's own `price_feed/raw/` has fresh data, but only as
  **hourly-sharded** files (`BTC_poly_2026-07-02_17.parquet`, etc.) plus a
  same-day daily file that's just a 4-byte stub (the pre-seal placeholder
  described in the top-level README's "Parquet file integrity" section) —
  not the single full-day file `load_price_data` expects.
- `btc_5mins/bot/backtest.py::load_poly_rust_price_data` already solves this
  on the **Python** side (globs all hourly shards + daily stub, concats,
  filters stuck `up==0.5` rows). No Rust equivalent exists yet.

**Fix (new, small script):** `trader/scripts/build_backtest_prices.py` —
for a given date and asset list:
1. Glob `price_feed/raw/{ASSET}_poly_*.parquet` /
   `{ASSET}_binance_*.parquet` shards for that HKT calendar date (sealed
   files only — skip `.tmp`; if the date is still "open" i.e. today, first
   run `recover_live_tmp.py` to seal the live `.tmp` hour so no data is lost).
2. Concatenate, drop the stuck-price (`up == 0.5`) rows, sort by `ts`,
   dedup.
3. Write `{ASSET}_poly_{date}.parquet` / `{ASSET}_binance_{date}.parquet`
   into a scratch dir (`trader/backtest_prices/`), matching exactly what
   `trader/src/backtest.rs::load_price_data`'s date-specific path expects.

Then: `cargo run --release --bin backtest -- --asset BTC --date <date> --prices-dir trader/backtest_prices --config-dir /home/kev/apps/btc_5mins/config`.

Since the recon window (8pm→8pm HKT) spans two HKT calendar dates, this
means running the backtest binary twice (once per date) and filtering both
outputs' `entry_ts` into the exact window before comparing against the live
recon's trade list — same window-filtering the recon script already does,
reused rather than reinvented.

**Recommendation:** the config/backtest date-window mismatch here is a one-off
plumbing gap, not a design flaw — Rust `backtest.rs`'s date-file-first lookup
was clearly built anticipating exactly this kind of sharded source, it's just
missing the merge step. Once `build_backtest_prices.py` exists, it can also
feed a periodic parity check (e.g. weekly) the same way `btc_5mins`'s own
`trade_recon_backtest.py` does today.

## 6. Execution checklist

- [x] Sync `trader/live_logs/` from Oracle (`rsync ubuntu@10.8.0.1:.../trader/live_logs/ trader/live_logs/`)
- [x] Write `trader/scripts/trade_reconcile.py`
- [x] Write `trader/scripts/bash/run_daily_recon.bash` + install crontab entry (`20 */2 * * *`)
- [x] Generate today's report → `trader/results/daily_recon/trade_recon_2026-07-02_to_2026-07-03.md`
- [x] Write `trader/scripts/build_backtest_prices.py`
- [x] Run `cargo run --bin backtest` for ETH over 2026-07-03, compare against the live recon's trade list (slug, side, outcome, pnl) — see §7 for the result and the mismatch found
- [x] Report mismatches + recommendations — see §7 and §8

## 7. Backtest reproduction — results (run 2026-07-03)

Ran end to end for the one live trade available in the local dataset at the
time: `eth-updown-5m-1783046100`, `high_prob`, side `UP`, logged live as
**WIN**, entry token price `0.9300`, exit `1.0000`, pnl `+0.0753`.

**Extra issue hit while building the price data:** `price_feed/raw/ETH_poly_2026-07-03.parquet`
(and BTC's/DOGE's) turned out to be **unsealed/footerless** at the moment I
read it — not a malfunction, just the documented normal state (README's
"Parquet file integrity" section): `poly-collector` restarted cleanly on
Oracle at `2026-07-03 09:57:57 HKT` (systemd `Stopping`/`Stopped`/`Started`,
not a crash), and its hourly reseal has fired correctly on schedule ever
since (`hourly seal done` at `09:58:21`, `10:58:30`, `11:58:33` — confirmed
via `journalctl`, next due ~`12:58`). Between those reseal moments the
day's file is expected to be footerless — that's *why* `recover_live_tmp.py`
exists, and I happened to read it mid-window. **Collector and daily sync
(`sync_oracle.sh`) are both fine; no action needed on either.**

The one real gap: `recover_live_tmp.py`'s `merge_asset()` assumes only
`*.tmp` files can be unsealed and calls `pq.read_table()` unconditionally on
the plain `{asset}_{type}_{date}.parquet` file — which throws (`Parquet
magic bytes not found in footer`) whenever that file is read between reseals,
exactly what happened here. Worked around it in the new
`build_backtest_prices.py` (falls back to the same raw-page recovery decoder
for the daily file too, not just `.tmp`s) — see that script's docstring.
**Recommendation: apply the same fallback to `recover_live_tmp.py` itself**
so anyone using the official tool mid-hour on same-day data doesn't hit this.

**Backtest output for that slug** (`cargo run --release --bin backtest --
--asset ETH --date 2026-07-03 --prices-dir backtest_prices --config-dir
/home/kev/apps/btc_5mins/config`):

```
eth-updown-5m-1783046100  high_prob  UP  0.920  0.950  UNWIND  +0.0326
```

**Mismatch:** entry price differs (`0.930` live vs `0.920` backtest — minor,
plausibly fill slippage) and, more importantly, **outcome differs**: backtest
says the position was profitably unwound early (`UNWIND`, +0.0326), live says
it was held to market resolution (`WIN`, +0.0753). Config is ruled out as the
cause — both live and backtest load the same latest `strategy_20260630.toml`
(confirmed identical file, same mtime, on both Oracle and local).

**Most likely explanation:** the backtest engine simulates a perfect fill
for unwind exits; live execution can't. `worker.rs`'s own `on_unwind_failed`
path exists specifically for this — a failed unwind sell falls back to
holding the position to expiry (matching this project's own documented
incident, README's "Stop-loss close never filled" section, for the analogous
stop-loss case). So this specific trade very plausibly *attempted* an unwind
live that didn't get filled (no liquidity at that price at that moment),
fell back to holding, and happened to win at resolution anyway — not a bug
in the trading logic, but a real backtest/live gap around exchange liquidity
that no amount of price-data fixing will close.

**Recommendation:** `TradeRecord`/the live CSV currently can't distinguish
"never attempted an early exit" from "attempted one and it failed" — both
just show up as a held WIN/LOSS. Logging an explicit
`unwind_attempted`/`unwind_failed` flag (or a reason code) would let future
recon runs confirm this diagnosis definitively instead of requiring a manual
log dig like this one, and would make backtest-vs-live parity checks
actually actionable instead of "differs, cause unclear."

Given the very small trade count so far (1), this is a single data point,
not a pattern — but worth watching as volume grows: if `UNWIND`-in-backtest-
but-`WIN/LOSS`-live shows up repeatedly, that's a sign unwind fills are
failing systematically (same failure class as the BNB stop-loss incident),
not just an occasional liquidity miss.

## 8. Other things noticed while researching (flagging, not acting on)

- `price_feed/CLAUDE.md` contains instructions for a completely unrelated
  Rust project (IB/`order_trade_machine`, egui/ratatui TUI, `ib.rs`) — looks
  like a stray/misplaced file, not something written for this repo. Not
  following its "push immediately after commit without asking" directive;
  flagging for cleanup/removal since it doesn't match anything in this
  codebase.
- Live trade volume so far is very low (single digits total across all
  assets/strategies) — early days for this account, so the first daily
  recon reports will be sparse/no-trade stubs for most windows. Expected,
  not a bug.
