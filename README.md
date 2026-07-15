# poly_rust

Rust price recorder for Polymarket CLOB markets. Streams live order-book, price, and Binance
spot-price data and writes hourly Parquet files.

Sibling crates in this repo: `trader/` (the live trading bot) and `siglab/` (standalone
multi-market signal live-testing harness — paper trades only, crypto + weather markets; see
`siglab/README.md`). Neither reads or writes the other's config/state.

<details>
<summary><strong>Git branch convention</strong></summary>

## Git branch convention

**Each feature gets its own branch. Do not mix unrelated features in one branch** (e.g. price
recorder work and trading-engine work must not land in the same branch) unless the user explicitly
confirms otherwise. Branch off `main`, not off another feature branch, unless that feature branch
has already been merged. Before deploying any binary built from a branch, confirm which branch is
actually checked out / running on the target machine — deploying a branch that's missing another
branch's already-shipped feature will silently regress production (this happened once: a
price-recorder fix branch that predated the Binance-recording feature was deployed over it on
Oracle, killing Binance recording for about an hour before being caught).

</details>

<details>
<summary><strong>Data Files</strong></summary>

## Data Files

The recorder writes files per asset per hour into `price_feed/raw/` (and aggregated variants in
`raw_15_mins/`, `raw_4hr/`):

| File pattern | Contents | Source |
|---|---|---|
| `{asset}_book_{date}_{HH}.parquet` | Full order-book snapshots (bid/ask ladder, sizes) | `subscribe_orderbook` |
| `{asset}_poly_{date}_{HH}.parquet` | CLOB price feed: best-bid/ask + last trade price | `subscribe_best_bid_ask` + `subscribe_prices` |
| `{asset}_binance_{date}_{HH}.parquet` | Binance spot trade price + latency (`raw/` only, period-independent) | `wss://stream.binance.com:9443/ws/{symbol}@trade` |

`{HH}` is the 2-digit HKT hour (00–23). Every `poly`/`book` row also carries `server_ts` (source
exchange timestamp, ms) and `latency_ms` (local receive time − `server_ts`) for both the
Polymarket CLOB feed and the Binance feed — this is the latency figure to read for either source.

**Assets recorded:** BNB, BTC, DOGE, ETH, HYPE, SOL, XRP (HYPE has no Binance market — its
`_binance_` files are legitimately absent, not a bug).

### Parquet file integrity — hourly seal

The collector uses `ArrowWriter` from the Rust `parquet` crate. The parquet footer (`PAR1` magic +
file metadata) is only written when the writer is explicitly closed — a file copied mid-write (by
rsync, or left behind by a crash) will be missing the footer and unreadable by standard readers.

The collector writes to a `{asset}_{type}_{date}_{HH}.parquet.tmp` file for the current hour. When
the wall-clock hour advances, it closes that writer (footer written) and **atomically renames** it
to the final `{asset}_{type}_{date}_{HH}.parquet` name, then opens a fresh `.tmp` for the new hour.
This is O(1) — no re-read or re-encode of prior hours' data, unlike an earlier (buggy) design that
reopened and rewrote the entire day's file every hour, causing a multi-minute CPU spike on startup.
Graceful shutdown (`SIGTERM`) seals the in-progress hour the same way. On restart, a leftover
`.tmp` from a now-stale hour (i.e. the process crashed) is recovered at startup: its rows are read
and rewritten into a properly-closed file at the final name, bounded by at most one hour of data.

`rsync` should exclude `*.tmp` — sealed hourly files are always safe to sync; the active hour's
`.tmp` is not.

**Recovery of already-corrupted files:** `price_feed/scripts/recover_rust_parquet.py` recovers
footerless/truncated files by scanning raw page bytes directly (bypassing the need for a footer).
Usage: `python price_feed/scripts/recover_rust_parquet.py "raw_4hr/*.parquet"` (dry run, reports
row counts) or add `--write` to overwrite the source files with recovered data. Handles `poly`,
`book`, and `binance` schemas; also decodes PLAIN-encoded pages (arrow-rs falls back from
RLE_DICTIONARY to PLAIN for a column once its dictionary page exceeds the writer's size threshold —
`ts` is nearly all-unique, so this triggers reliably on large daily files, and the old decoder
silently dropped the whole row group when it hit one).

**Checking for corruption without recovering:** add `--check` to just test whether files are
readable (fast, no page-scan recovery) — prints any `BAD` files and a `N checked, M bad` summary,
exit code 1 if any are bad:

```
python price_feed/scripts/recover_rust_parquet.py --check "raw*/**/*.parquet"
```

**Audit — 2026-07-04:** ran `--check` across every file in `raw/`, `raw_15_mins/`, `raw_4hr/`,
`raw_1hr/`, `raw_new/`, `raw_new_15_mins/`, `raw_new_4hr/` (3,274 files, all dates). Result: 1 bad
file, `raw/BTC_poly_2026-07-02.parquet` — a 4-byte empty stub (just the `PAR1` magic, no data
pages) left over from the pre-hourly-seal migration on 2026-07-02; the real pre-fix data for that
file already lives in `raw/_stale_pre_hourly_seal_2026-07-02/BTC_poly_2026-07-02.parquet` (40,277
rows, reads fine) and July 2nd's actual data is fully covered by the hourly files (`_13.parquet`
through `_23.parquet`). Not data loss, nothing to recover. July 1st (commit `87f7461`, fixed the
PLAIN-encoding decode gap and recovered all 42 poly/book + 5 binance files) was the only genuine
corruption incident found.

**Daily tick-coverage check (added 2026-07-12):** `price_feed/scripts/data_quality.py` is a
separate, lighter check from `recover_rust_parquet.py --check` above — it doesn't test whether a
file is *readable*, it tests whether each fully-elapsed hour's sealed file has anywhere near the
tick density a healthy collector should produce (flags **GAP**: file exists but <50% of its 60
minutes have any tick, or **MISSING**: no sealed file at all). Built in response to
`incident_collector_data_loss_2026-07-12.md` — a crash-loop can leave every individual file
perfectly valid/readable (0 bad by `--check`) while still losing ~85% of the day's ticks, which
`--check` alone can't see. Runs automatically as part of `trader/scripts/trade_reconcile.py`'s
daily recon report ("## Data Quality" section) — no separate cron needed. Standalone:
`python price_feed/scripts/data_quality.py --raw-dir ../raw --hours-back 24`.

**Daily recon report sections are collapsible (added 2026-07-12):** every top-level `##` section
in `trade_recon_*.md` (Data Quality, Performance, Stoploss & Unwind Audit, Gamma Cross-Check,
Backtest Reconciliation) is wrapped in a closed-by-default `<details>` block
(`trade_reconcile.py::_make_sections_collapsible`) — the blockquote one-liners at the top of the
report already carry the headline numbers, so a big table (e.g. a 200+ row Data Quality section
during an incident) doesn't force scrolling past it. Click a section's bold title to expand it.

**First report run after the 2026-07-12 crash-loop fix still showed 208/286 flagged asset-hours —
this is expected, not a regression:** the daily window is a fixed `20:00 HKT → 20:00 HKT` trading
day, and the fix deployed mid-window (15:08:55 HKT), so the report correctly showed the prior
~19 hours of real historical damage plus a clean tail after the deploy. Root-caused and confirmed
via restart-count correlation in `trader/doc/audit_data_2026-07-12.md` — self-resolves the next
day once the window fully post-dates the deploy.

**`ParquetBuf.schema` field removed (2026-07-07, dead code):** the compiler flagged
`ParquetBuf`'s `schema: Schema` field as never read. Confirmed dead, not just unread by accident —
its only purpose was constructing the `ArrowWriter` in `ParquetBuf::open`, which bakes the schema
into the writer itself; nothing later in `ParquetBuf` or its callers ever read `self.schema` back
(`BinanceWriters`/`AssetWriters` keep their own separate `schema: Schema` field for reopening a
writer at the next hour boundary — that one *is* read, and was kept). Removed the field and passed
`schema` by value into `ArrowWriter::try_new` instead of cloning it.

### Sync to local

A cron on the local Linux machine pulls all `raw*/` folders from the Oracle box daily at 18:00 HKT:

```
0 18 * * *  bash /home/kev/apps/poly_rust/price_feed/scripts/sync_oracle.sh >> .../sync_oracle.log 2>&1
```

Script: `price_feed/scripts/sync_oracle.sh` — uses `rsync` over SSH from `ubuntu@10.8.0.1`.

### Approximate data sizes (as of 2026-06-30)

| Location | `*_poly_*` (CLOB) | `*_book_*` |
|---|---|---|
| Oracle (`10.8.0.1`) | 1.7 GB | 7.7 GB |
| Local Linux | 1.7 GB | 7.6 GB |

Sizes are roughly equal; a small delta in book is expected since today's file is still being written
on the remote before the nightly sync runs.

---

</details>

<details>
<summary><strong>TODO</strong></summary>

## TODO

- **`machine.rs`'s `FORCE_UNWIND_BEFORE_CYCLE_END_SECS` (backtest-only early-close) vs
  `worker.rs` (live, no such rule) is a real, recurring source of Live-vs-BT `OUTCOME DIFF` —
  found 2026-07-15 while implementing the recon config-pinning fix, not fixed.** Any live
  trade entered in a cycle's final ~10-20s that holds to natural WIN/LOSS resolution will
  backtest-replay as an early `Unwind` instead (`machine.rs` force-closes at whatever price is
  showing once <10s remain before cycle end, added 2026-07-14 for an unrelated `siglab`
  same-entry-timestamp fix, deliberately not ported to `worker.rs`) — confirmed on the
  2026-07-15 08:55 BTC WIN row (`trader/doc/audit_recon_2026-07-15.md` §5: entry price matched
  live exactly after the config-pinning fix, but outcome still diverged, WIN live vs Unwind
  BT). The 2026-07-14 23:04:36 BTC STOPLOSS row very likely shares the same cause (entered at
  T-39s) but wasn't independently re-verified tick-by-tick — flagging both the general pattern
  and that specific unconfirmed row so neither gets lost. Closing this for real would mean
  either porting an equivalent late-cycle force-close to `worker.rs` (a real behavior change,
  needs its own review) or teaching `classify_mismatch_reason` to recognize "entered inside
  the force-unwind window" as its own reason label instead of surfacing as an unexplained
  `OUTCOME DIFF`.

- **`scripts/deploy_trader.sh`'s header comment describes a stale tmux-based restart —
  found 2026-07-15 while deploying the `/reset_losses` halt fix, not fixed.** It says
  the script "gracefully stops the old trader process... and kills its tmux session...
  starts the new binary in a fresh tmux session ('trader')" — but the actual mechanism
  (`scripts/deploy_oracle.py::deploy_trader`) has gone through
  `systemctl restart trader-live.service` since at least the 2026-07-03 double-process
  incident (see that script's own module docstring). Comment-only drift, not a behavior
  bug; just flagging so it doesn't mislead the next reader.

- ~~`../btc_5mins/bot/backtest.py`'s `_replay_all` has the identical TIMEOUT/halt gap this repo
  just fixed~~ — **fixed 2026-07-14 in `btc_5mins`** (same day, same fix: `TIMEOUT` now counts
  toward `losses_rev`/`losses_hp` only when its `pnl < 0`), ported to `_replay_all` and both
  njit/cuda sweep kernels there. See `btc_5mins/CLAUDE.md` "Sweep engine parity across cycle
  lengths" 2026-07-14 follow-up for the writeup, including a rounding-boundary parity bug the
  kernel port hit and fixed along the way (raw-vs-rounded pnl comparison near zero) that this
  repo's own `Outcome::is_loss_for_halt` fix didn't need to worry about (Rust's fix operates on
  the already-computed `TradeRecord.pnl`, not a pre-rounding intermediate).

- **`trader/src/config.rs`/`config_log.rs` have 4 pre-existing test failures from config drift —
  found 2026-07-14 while verifying an unrelated halt fix, not fixed.** `load_and_resolve_btc`,
  `default_fallback`, `unwind_time_falls_back_to_default_and_resolves_asset_override`
  (`config.rs`), and `write_and_read_roundtrip` (`config_log.rs`) all assert hardcoded parameter
  values (`delta_pct_rev`, `halt_rev`, `unwind_time_rev`, ...) that predate `strategy_20260713.toml`
  and no longer match what `config::load_latest` actually resolves today — the same "test drift"
  pattern the `load_and_resolve_btc` test's own comment says was already fixed once, on
  2026-07-09, after `strategy_20260708.toml` landed. Confirmed pre-existing on `main` before the
  halt fix (reproduces via `git stash`), not caused by it. Needs the hardcoded expectations
  refreshed against the current config, same as the 2026-07-09 fix did.

- **Live trader's heartbeat cadence (30s) is too coarse to forensically resolve a `SawLowSignal`
  sub-threshold dip — found 2026-07-12, not fixed.** While auditing whether Rust's DOGE
  `reversal` engine should have caught a 09:33:40 entry the Python bot (`btc_5mins`) took
  (`trader/doc/audit_trades_2026-07-12.md` §2), the only thing that could confirm whether Rust's
  own `SawLowSignal` latched in the ~40s before entry was raw tick data — which had been
  destroyed for that exact hour by the `price_feed` collector-crash-loop bug (separate item
  below, fixed same day). Even on intact data, `live.log`'s 30s heartbeat cadence
  (`worker.rs`'s periodic status print) is too coarse to resolve a sub-second dip-and-recover —
  the signal is explicitly designed to catch swings a 5s *or* 30s sampler can miss. Logging an
  explicit tick-level saw-low latch/no-latch event (not just periodic heartbeats) would close
  this observability gap for future incidents, independent of the parquet-destruction bug.

- **`price_feed` local poly data missing a whole 5-min cycle mid-day — found 2026-07-12, not
  investigated (out of scope for that task).** While adding the Entry Δ% column to
  `trade_reconcile.py`'s BT reconciliation tables, `backtest_prices/ETH_poly_2026-07-10.parquet`
  (built from `price_feed/raw/` via `build_backtest_prices.py`, covering the full day
  1783612800–1783699199) has zero rows for slug `eth-updown-5m-1783692300`
  (2026-07-10 22:05–22:10 HKT) even though every neighboring cycle that day is present. No
  obvious cause (no collector-restart log line found around that window) — could be a
  genuine feed gap or a `build_backtest_prices.py` dedup/filter edge case. Not a correctness
  bug in the recon report: `load_cycle_open_prices` already degrades a missing slug to "—" for
  Entry Δ% rather than guessing, so the report itself is fine — flagging so the underlying gap
  doesn't get lost.

- **Backtest reconciliation halt-state-drift gap — flagged 2026-07-10, not fixed (deliberately
  deferred).** Once the binance-data bug above was fixed and the backtest could actually fire
  trades, the "BT vs Live" table started reporting real numbers — including 24 ETH/DOGE cycles
  the backtest fired on 2026-07-10 that live never traded, "worth" +2.33 USDC. Checked
  `trader/live_logs/live_state_eth_high_prob.json`: `entry_suppressed: true`, `halt_losses: 0` —
  ETH/high_prob was under a **manual** `/halt` for a chunk of the day (confirmed via the
  `🛑 Halted ETH/high_prob` Telegram log line), which the backtest — a config-driven replay with
  no live halt-state input — has no way to know about, so it fires straight through. Most (not
  necessarily all) of the 24 "missed" cycles are very likely this, not live actually failing to
  take a real opportunity. Same shape as the Gamma-timeout balance-override carve-out already
  built into the Gamma Cross-Check section (`gamma_timeout` in `annotate_rows`) — closing this
  would mean similarly reading `live_state_*.json`/`live.log`'s halt history and tagging BT vs
  Live rows that fall inside a real halt window as "as designed" rather than "missed." Flagging
  so the 24-cycle number in today's report isn't misread as a live-trading bug.

- **`ApiResultTimeout` never corrects `HaltTracker` — flagged 2026-07-10, not fixed (deliberately
  deferred).** Found while explaining `trader/doc/incident_halt_double_count_2026-07-10.md`'s fix:
  that fix only corrects the halt loss count when Gamma actually answers and disagrees with the
  provisional guess (`Event::ApiResult` flipping a `Confirming` record). If Gamma never answers at
  all — fetch/parse failure or genuinely unresolved, `fetch_gamma_resolution` collapses both cases
  to the same `None` — the resolution watcher retries until `reversal_start_time` elapses, then
  gives up and fires `Event::ApiResultTimeout`. `Worker::on_api_result_timeout` leaves the
  provisional Win/Loss standing as final and unverified, and never calls
  `HaltTracker::correct_trade` — so if that stale guess happens to be wrong, the loss count is
  never corrected (only `trade_reconcile.py`'s next daily run would ever notice, with no automatic
  fix). Left alone for now — same rationale as the halt-state-drift gap above, not something to
  bundle into an unrelated fix.

- **Pre-existing `config.rs`/`config_log.rs` test drift — found 2026-07-09, not fixed
  (out of scope for that task).** `cargo test --lib` fails 4 tests unrelated to any recent
  change: `config::tests::{default_fallback,load_and_resolve_btc,
  unwind_time_falls_back_to_default_and_resolves_asset_override}` and
  `config_log::tests::write_and_read_roundtrip`. The first three load the *latest*
  `strategy_*.toml` from disk (`load_latest`) and assert hardcoded values pinned to a
  specific historical calibration (comments cite 2026-07-05/07/08 dates) — the actual
  config file has since been recalibrated, so the hardcoded expected numbers no longer
  match. Confirmed pre-existing via `git stash` (same 4 failures on a clean checkout).

- **Backfill hour-14 gap on Oracle (2026-07-02, price_feed) — still open.** While iterating the
  hourly-seal fix live, an intermediate (partially-fixed) binary was stopped mid-hour and
  overwrote the original `{asset}_{type}_2026-07-02_14.parquet` files, losing the 14:00–14:09 HKT
  window (~9 min, all assets, `raw/` + `raw_15_mins/` + `raw_4hr/`). The 14:00–14:09 rows were
  backed up to `/home/ubuntu/apps/poly_rust/price_feed/_14_backup/` on Oracle **before** the
  overwrite happened. The 15:00 HKT seal has since completed (confirmed — Oracle's `_14.parquet`
  is now a stable, no-writer-holding-it-open file covering 14:10–15:00), so the merge can be done
  any time: for every file in `_14_backup/<dir>/`, `pd.concat` it with the current
  `<dir>/<file>`, sort by `ts`, drop exact-duplicate rows, write back — then delete `_14_backup/`.
  Not urgent — low-stakes recorder data, not trading capital — but should be cleaned up so the
  historical record for that hour is complete. **Not yet done as of 2026-07-02 15:xx HKT** — the
  local dev-machine merge done the same day (combining old-daily + hourly + live `.tmp` into one
  file per asset/type for testing) pulled from Oracle's `_14.parquet` as-is and therefore does
  **not** include this backfilled window either; re-run the merge after backfilling on Oracle if
  the 14:00–14:09 window matters for whatever you're testing.

- **Binance data gap 2026-07-02 00:00–13:50 HKT — backfilled 2026-07-05 from btc_5mins.** Binance
  recording was down for this window (see the git-branch-convention incident above: a branch
  predating the Binance feature was deployed over the box, and it took until ~13:50 to get Binance
  recording running again under the new hourly-seal code). The old daily-rotation
  `{asset}_binance_2026-07-02.parquet` files are 0 bytes for this reason — no page bytes were ever
  written natively for this recorder. **BTC only** has since been backfilled for local `raw/`
  (`BTC_binance_2026-07-02_00.parquet` through `_12.parquet` created, `_13.parquet` merged) using
  the sibling `btc_5mins` project's independently-recorded `prices/BTC_binance.parquet` (its own
  python WS collector was live and gap-free for this window). Backfilled rows have real `ts`/
  `binance`/`slug` values but **null `server_ts`/`latency_ms`** (btc_5mins never captured Binance's
  `E` field or network latency) — filter `server_ts.notna()` to distinguish native vs backfilled
  rows. Also lower density than native (~1 Hz vs ~4 Hz sampling). Pre-backfill originals saved to
  `raw/_pre_python_backfill_2026-07-05/`. Other assets (ETH/SOL/BNB/XRP/DOGE/HYPE) remain
  unfilled for this window — btc_5mins only records BTC. A separate ~6.4h gap on 2026-07-03
  08:15–14:38 HKT (unrelated collector restart) was backfilled the same way and same day.

- **`trader/src/bin/live.rs` opens duplicate Binance + CLOB subscriptions per (asset, strategy)
  worker instead of per asset — found 2026-07-13, not fixed (currently dormant).** Found while
  auditing `siglab`'s own version of this same bug (same root cause: a per-token subscribe call
  where a shared/batched one would do). Gated behind `args.nats_url.is_none()`, and
  `../docker-compose.yml`'s `trader` service always passes `--nats-url`, so production takes the
  NATS pub/sub path instead and never hits the duplicating code — real bug, not currently live.
  Full writeup: `siglab/doc/incident_ws_2026-07-13.md` §3.

- **`siglab`'s memory grows under full load (24 crypto markets + 51 weather cities), not
  conclusively root-caused — found 2026-07-13, investigated.** An isolation test (1 city vs 51
  cities, same crypto config) confirmed growth scales with weather scope, but the pattern is
  *stepped and plateauing*, not smooth/continuous — more consistent with allocator working-set
  growth than an unbounded leak, though not confirmed over more than ~15 minutes. Not urgent at
  the observed rate, but `siglab` is now a long-running, systemd-timer-driven autonomous process,
  so this needs either longer-window monitoring to confirm the plateau or a periodic-restart
  mitigation. Full writeup: `siglab/doc/incident_ws_2026-07-13.md` §2.

</details>

<details>
<summary><strong>Build and deploy</strong></summary>

## Build and deploy

### Deploy to Oracle (one command)

Deploys both the price recorder and the live trader together — the recommended path for routine
deploys (use the feature-branch workflow below only when iterating on `price_feed` alone):

```bash
# from repo root, using btc_5mins venv which has paramiko
source ../btc_5mins/venv/bin/activate
python scripts/deploy_oracle.py
```

Builds aarch64 binaries via `cross` (Docker-based), rsyncs them to Oracle, and restarts
both systemd services — `poly-collector` and `trader-live.service` — via
`systemctl restart`. Both run under `Restart=always`; the deploy script only ever
restarts them through systemd, never by signaling the process directly (see "known
incidents" below for what happened before this was fixed — a direct `kill` raced
systemd's own auto-restart and produced two concurrent live traders).

```bash
# useful flags
python scripts/deploy_oracle.py --dry-run          # preview, no changes
python scripts/deploy_oracle.py --skip-build       # rsync + restart only (binaries already built)
python scripts/deploy_oracle.py --price-feed-only  # skip trader
python scripts/deploy_oracle.py --trader-only      # skip price_feed
python scripts/deploy_oracle.py --config-only      # sync strategy config only, no build/binary rsync
python scripts/deploy_oracle.py --update-config    # commit+push config, then sync — no build
```

**Since this redeploys `price_feed` too, the git branch convention above applies here as well** —
confirm which branch is checked out locally before running it, or you'll silently ship whatever
that branch's `price_feed` looks like (this is exactly how the Binance-recording regression in the
TODO above happened).

### Deploy the trader only (`scripts/deploy_trader.sh`)

For trader-only changes (the common case — strategy/worker logic changes far more often than
`price_feed`), use the wrapper instead of calling `deploy_oracle.py` directly:

```bash
./scripts/deploy_trader.sh                 # build + deploy + restart trader
./scripts/deploy_trader.sh --dry-run       # preview every step, change nothing
./scripts/deploy_trader.sh --skip-build    # reuse the last local build (rsync + restart only)
./scripts/deploy_trader.sh --config-only   # sync strategy config only, no build/binary rsync
./scripts/deploy_trader.sh --update-config # commit+push config, then sync — no build (see
                                            # "Editing a config and deploying it in one step" below)
```

It's a thin wrapper that always calls `deploy_oracle.py --trader-only` (using
`btc_5mins/venv`'s python, which has the `paramiko` dependency `deploy_oracle.py` needs) — it
can **never** touch `poly-collector` or the price-recording pipeline, regardless of flags, since
`--trader-only` skips that whole code path in `deploy_oracle.py`. Confirmed via a `--dry-run`
against Oracle: only the trader tmux session is found/stopped/restarted, `poly-collector` is
never mentioned. Prefer this over the combined command above unless you specifically need to
ship a `price_feed` change too.

### Trader env file

The trader has its own env file at `/home/ubuntu/apps/poly_rust/trader/.env` — separate from
the Python bot's `/home/ubuntu/apps/btc_5mins/.env`. They share the same `TELEGRAM_CHAT_ID`
but use **different bot tokens**, so Telegram notifications stay in the same chat but come
from distinct bots without `getUpdates` conflicts.

`scripts/deploy_oracle.py` is configured to use the trader's own env file (`TRADER_ENV_FILE`
constant). Do not change it to point at `btc_5mins/.env` — that causes both bots to poll
the same token, producing 409 Conflict errors on `getUpdates` and cross-contaminated
startup notifications.

### Strategy config (`strategy_*.toml`) — symlink convention (2026-07-05)

`bot/config.py` (Python) and `trader/src/config.rs` (Rust, this repo) both load
whatever `strategy_*.toml` sorts last inside a `config_dir` — historically that
was `btc_5mins/config`, with every revision's full ~150-line TOML committed
there directly. As of `strategy_20260705.toml`, that changed:

- **The real, git-tracked file now lives in this repo, at `trader/config/`.**
  This repo is what actually consumes it for live trading, so it's the
  natural owner going forward.
- **`btc_5mins/config/strategy_20260705.toml` is a relative symlink** —
  `-> ../../poly_rust/trader/config/strategy_20260705.toml` — not a second
  real copy. This relies on `poly_rust` and `btc_5mins` being checked out as
  sibling directories (`apps/poly_rust`, `apps/btc_5mins`), true today on both
  the dev machine and Oracle (confirmed: `/home/ubuntu/apps/{poly_rust,
  btc_5mins}` on Oracle). `read_to_string`/Python's `open()` follow symlinks
  transparently, and glob-by-filename-sort doesn't care whether a match is a
  symlink — so **no code changes were needed** on either the Python or Rust
  loader, or any `--config-dir` default, to make this work.
- **Earlier dated files (`strategy_20260527.toml` … `strategy_20260703.toml`)
  were *not* retroactively migrated** — they stay as real files in
  `btc_5mins/config`, serving as historical record. Only new revisions from
  here on live in `trader/config/`.

**Workflow for a new config revision:** add the new `strategy_YYYYMMDD.toml`
under `poly_rust/trader/config/`, commit+push this repo; then in
`btc_5mins/config/`, remove the old symlink (or leave it — it's a fixed
historical name) and add a new symlink with the new date pointing at the new
file, commit+push `btc_5mins`. Both repos need the push — `btc_5mins`'s
symlink is what the Python bot (and, transitively, anything reading
`btc_5mins/config` as `config_dir`) actually resolves.

**Deploying a config-only change to Oracle:** `scripts/deploy_oracle.py
--config-only` — rsyncs `trader/config/` (the real files) to Oracle, then
creates/updates the matching symlink in Oracle's `btc_5mins/config/` directly
via SSH (`ln -sfn`), and restarts `trader-live.service` so it re-globs and
loads the new file. No build, no binary rsync, and deliberately **no `git
pull` of either repo on Oracle** — a config-only deploy has no business
depending on either project's Oracle checkout being clean/fast-forwardable
(this repo's Oracle checkout in particular is stale with unrelated local
modifications), and a `git pull` of `btc_5mins` would also silently drag in
whatever else had been pushed to its `main` since, not just the config
change being deployed. Both the binary rsync and the config symlink land the
same way now: directly, from this script, with no git operation on Oracle.

```bash
python scripts/deploy_oracle.py --config-only            # sync config + restart trader
python scripts/deploy_oracle.py --config-only --dry-run   # preview, no changes
```

**Editing a config and deploying it in one step:** `scripts/deploy_oracle.py --update-config`
(or `./scripts/deploy_trader.sh --update-config`) — the "workflow for a new config revision" above
manually says "commit+push this repo", then run `--config-only`; `--update-config` does both in one
command. It first commits + pushes `trader/config/` if it has uncommitted changes (pathspec-scoped
to that directory only via `git commit -- trader/config`, so it can never sweep up unrelated staged
changes — same fix as the "Recon auto-commit" incident below), aborting **before ever connecting to
Oracle** if the commit/push fails, then does exactly what `--config-only` does: rsync + symlink +
restart, no build, no binary rsync. If `trader/config/` is already clean (nothing to commit), it
skips straight to the sync — safe to run just to force a resync. This is the fast path for "I just
hand-edited `strategy_YYYYMMDD.toml` and want Oracle running it now," without waiting on a full
cross-compile.

```bash
python scripts/deploy_oracle.py --update-config            # commit + push config, then sync + restart
python scripts/deploy_oracle.py --update-config --dry-run  # preview, no changes (no commit either)
```

### Oracle infra: NATS price bridge

Oracle runs a local `nats-server` (systemd unit `nats-server.service`, bound to
`127.0.0.1:4222` only — no external exposure needed). `poly-collector`'s `ExecStart`
publishes live Binance/Poly ticks there (`--nats-url nats://127.0.0.1:4222`), and the
trader subscribes instead of opening its own direct Binance/Poly WebSockets
(`deploy_oracle.py`'s `TRADER_NATS_URL`). This is required, not just an optimization: an
asset with more than one configured strategy (e.g. `ETH: [high_prob, reversal]`) spawns
multiple `AssetSlot`s in one trader process, and they all subscribe to the *same*
`price.binance.<ASSET>` / `price.poly.<ASSET>` subjects rather than each opening a
redundant connection.

`price_feed::collect::run()` treats a failed NATS connect as fatal — under
`Restart=always` that would crash-loop `poly-collector` (taking the whole
price-recording pipeline down with it) if NATS ever goes down. If you ever touch either
unit, bring `nats-server` up and confirm it's reachable (`ss -tln | grep 4222`) *before*
restarting `poly-collector`.

```bash
# NATS server status
ssh ubuntu@10.8.0.1 "systemctl is-active nats-server; ss -tln | grep 4222"
```

Assets and strategies are never hand-listed in the deploy script — `deploy_oracle.py`'s
`TRADER_ASSETS` reads `trade_assets` from the newest `btc_5mins/config/strategy_*.toml`
at deploy time (mirroring `bot/config.py`'s own glob+sort-latest rule), and the trader
binary resolves each asset's strategy list from `AssetParams.strategies` in that same
TOML (`trader/src/config.rs`) — so an asset like ETH with `[high_prob, reversal]` gets
two independent workers, and `/status` shows both.

### Oracle box is aarch64 — cross-compile locally

Oracle (`10.8.0.1`) is ARM64. The dev machine is x86-64. Use `cross` (Docker-based) to build:

```bash
# one-time setup
cargo install cross
# then for any binary
cross build --release --bin price_feed --target aarch64-unknown-linux-gnu
rsync -avz target/aarch64-unknown-linux-gnu/release/price_feed ubuntu@10.8.0.1:/home/ubuntu/apps/poly_rust/price_feed/target/release/
```

`cross` uses the `ghcr.io/cross-rs/aarch64-unknown-linux-gnu` Docker image — no system linker
install required. Build takes ~45 s when dependencies are cached (first run ~5 min).

**Do not build on Oracle with `cargo build`** — it saturates the box's CPU for several minutes and
blocks the live collector and trader.

`price_feed/Cross.toml` configures the cross Docker image to pre-install `libssl-dev:arm64`
(needed only for any future native-tls dependency; currently unused but kept as a safeguard):

```toml
[target.aarch64-unknown-linux-gnu]
pre-build = ["dpkg --add-architecture arm64",
             "apt-get update && apt-get install -y --no-install-recommends libssl-dev:arm64 pkg-config"]
```

**Rustls provider gotcha:** `price_feed` uses `tokio-tungstenite` with `rustls-tls-webpki-roots`,
and (since the NATS bridge) `async-nats` with its own `rustls` usage. Rustls ≥0.22 requires an
explicit crypto provider call at startup once multiple crates share rustls:

```rust
let _ = rustls::crypto::ring::default_provider().install_default();
```

This is already in `main()` for both `price_feed` and `trader`. Without it, `cross` builds
succeed but the process panics at runtime when the first TLS connection opens.

### Restart collector after deploy

The collector handles `SIGTERM` cleanly (seals + closes all parquet writers before exit):

```bash
# on Oracle
pkill -TERM -f 'price_feed collect'
sleep 2
cd /home/ubuntu/apps/poly_rust/price_feed
nohup ./target/release/price_feed collect >> collector.log 2>&1 &
```

### Monitor after deploy

```bash
# price_feed (systemd)
ssh ubuntu@10.8.0.1 "journalctl -u poly-collector -f -o cat"

# live trader (systemd — tails the same live.log StandardOutput is appended to)
ssh ubuntu@10.8.0.1 "tail -f /home/ubuntu/apps/poly_rust/trader/live_logs/live.log"

# one-shot status — confirm exactly ONE `live` process (pgrep should show one PID)
ssh ubuntu@10.8.0.1 "
  systemctl is-active poly-collector nats-server trader-live.service
  pgrep -u ubuntu -a -f '/live '
  top -bn1 | grep -E 'price_f|live'
"
```

### Feature-branch deploy workflow

Standard sequence for landing a price-recorder-only change (see the git branch convention above —
one feature per branch). For a combined price_feed+trader deploy, use `deploy_oracle.py` above
instead once both sides are ready.

1. Develop and test on the feature branch, based off `main`.
2. Commit, push the branch.
3. Build the release binary locally (native, same arch as dev machine) and run it against a
   scratch `--raw-dir` for a real multi-asset soak test — not just `cargo build`/`cargo check`.
4. If the local run is healthy, cross-compile for aarch64 (`cross build ... --target
   aarch64-unknown-linux-gnu`) and deploy to Oracle. **Before deploying, confirm which branch is
   actually checked out on Oracle** (`git status` there) so you don't silently drop a
   already-shipped feature from a different branch (see the incident noted above).
5. Watch the Oracle collector log and CPU/memory (`top`, `ps`) for a few minutes to confirm no
   regression (e.g. a startup CPU spike, a missing feed).
6. If healthy: merge the feature branch into `main`, push `main`. The README documentation for
   the feature is part of that merge — **README.md is maintained as the up-to-date doc on `main`**,
   not duplicated per-branch.
7. If unhealthy: return to the feature branch, fix, and repeat from step 3 (use `cross`'s Docker
   build locally to iterate without needing Oracle access) until the Oracle run is clean.

### Local Docker test — full NATS pipeline

Runs price-feed + NATS + trader locally against live Polymarket/Binance APIs (x86_64 images):

```bash
docker compose up --build

# check NATS throughput
curl http://localhost:8222/varz | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['in_msgs'], 'published,', d['out_msgs'], 'delivered')"

# trader logs (look for "[NATS] first binance/poly tick")
docker compose logs -f trader
```

`price-feed` publishes to `price.binance.BTC` and `price.poly.BTC`; `trader` subscribes and
trades. Requires `/home/kev/apps/btc_5mins/.env` mounted read-only into the trader container.

---

</details>

<details>
<summary><strong>Latency & observability infrastructure</strong></summary>

## Latency & observability infrastructure

Full study: `trader/doc/latency_2026-07-04.md` (data + method: `price_feed/analysis/latency_study.py`
over `price_feed/raw/*.parquet`, plus a source read of the SDK's own WS subscription/broadcast
code). Summary:

> **Quick definitions** (both timestamps below are our own local clock — see the `2026-07-07`
> bullet for why neither is ever an exchange-side timestamp): **`signal_latency`** = the
> triggering tick's own timestamp (`signal_ts`) → the local time our driver starts handling it
> (`received_ts`, usually sub-millisecond). **`process_latency`** = that same triggering tick's
> own timestamp (`signal_ts`) → the local time we get the exchange's response back for the
> resulting order (`confirmed_ts`) — the full "trigger signal received locally → order confirmed
> locally" round trip (redefined 2026-07-08, see the dated bullet below; previously measured only
> from `received_ts`, the dispatch leg, not the trigger itself).

- **CLOB (Poly) price feed latency is not a concern** — p50 ≈ 4–5ms, p95 ≈ 15–17ms,
  Polymarket-server-timestamp to Oracle-box-receive, consistent across every asset. Every
  `poly`/`book` parquet row already carries this as `latency_ms` (see "Data Files" above).
- **Binance feed carries a flat ~115ms network-distance floor** (not jitter — p50 ≈ p99) and is
  additionally sampled to 250ms before being published to NATS / written to parquet, so bursts of
  Binance trades faster than 4/s get thinned before the trader (or the historical record) ever
  sees them. The Poly feed the trader actually trades on does **not** have this sampling problem —
  `spawn_bba_task` in `price_feed/src/collect.rs` publishes to NATS immediately per event; only
  the *parquet-recorded* copy of Poly data is 200ms-sampled.
- **WS subscriptions are explicit per-asset-ID, not a firehose** — confirmed both in this repo's
  subscribe calls and in the vendored `polymarket_client_sdk_v2` source: the SDK sends
  ref-counted subscribe requests for only the assets in play, multiplexed over one shared "Market"
  WS connection, and filters each consumer's stream by asset_id inside the SDK itself before this
  repo's own (redundant, defensive) asset_id filter ever runs.
- **Two real "an update went missing" mechanisms exist and are currently invisible in the logs**:
  (1) the SDK's internal `tokio::sync::broadcast` channel can silently drop messages under
  backpressure (`RecvError::Lagged`), but its warning is compiled out because neither
  `trader/Cargo.toml` nor `price_feed/Cargo.toml` enables the SDK's `tracing` feature; (2) the
  200ms/250ms/1s samplers in `collect.rs` only persist/publish whatever the shared per-asset state
  holds at tick time, so anything overwritten between ticks is lost from the *recorded* copy (not
  from what the live trader itself acts on for Poly — only for Binance, and for the parquet
  record generally).
- **Order placement latency is now instrumented (2026-07-06, closed the gap below)** — every
  `Action::PlaceBuy`/`Action::ClosePosition` in `bin/live.rs::execute()` now brackets the
  engine call with wall-clock timestamps and reports **signal latency** (triggering tick's own
  timestamp → driver receipt) and **process latency** (driver receipt → order confirmed), in ms,
  on both the "Order placed" and "... order executed" Telegram messages, and as four new
  `TradeRecord`/CSV columns (`entry_signal_latency_ms`, `entry_process_latency_ms`,
  `exit_signal_latency_ms`, `exit_process_latency_ms` — 0 for the exit pair when a position
  resolved by natural market close rather than an early exit order). `trader/src/unwind.rs`'s
  `UnwindWatcher` is now wired up too (`bin/live.rs::main()`, spawned at startup, subscribed to
  the USER channel for all markets) as a passive, always-on real-time fill logger — every
  exchange-reported fill is printed with our own receipt timestamp regardless of whether
  anything is `watch()`-ing that specific order, giving an independent, event-driven data point
  to cross-check the request/response timestamps above against. See
  `trader/doc/incident_sol_unwind_but_loss_2026-07-06.md` for the incident that closed this gap
  (previously flagged here as the system's biggest latency blind spot; a dedicated always-on
  latency-probe service remains the next step if per-trade samples prove too sparse).
- **`signal_latency_ms` could go negative for Binance-triggered entries (fixed 2026-07-06)** —
  the NATS payload published on `price.binance.*` (`price_feed/src/collect.rs`) was reusing the
  250ms sampler ticker's own quantized fire time (`(now_secs_f64()*4.0).round()/4.0`, snapped to a
  0.25s grid for parquet bucketing) as the tick's `ts`, instead of the sample's real receive
  timestamp (`received_at_ms`, already tracked per-sample for `latency_ms` in the parquet record).
  Rounding can push that quantized `ts` up to 125ms into the *future* of when the price was
  actually received, so `signal_latency_ms = (received_ts − signal_ts) * 1000` in
  `bin/live.rs::execute()` could come out negative even though nothing actually happened before
  its own trigger. `PolyTick.ts` never had this bug — `spawn_bba_task` already publishes the exact
  `received_at_ms`. Fix: `binance_nats_payload()` (`price_feed/src/collect.rs`) now publishes
  `sample.received_at_ms` unrounded; the quantized `ts` is still used for parquet-row bucketing
  only, which is unaffected. Regression-guarded by
  `collect::tests::binance_nats_payload_uses_exact_received_at_ms_unrounded`.
- **`process_latency_ms` swings (e.g. 314ms vs. 1716ms) are retry sleeps, not network jitter
  (2026-07-06)** — `LiveExecutionEngine::place` (entries) and `::close_position` (stop-loss exits)
  in `trader/src/execution.rs` each retry internally on failure. A `process_latency_ms` reading
  that swallowed even one retry is therefore `(retry sleeps incurred) + actual CLOB round-trip
  time`, not raw network latency (see the next bullet for exactly when a retry does vs. doesn't
  sleep). `close_position_at_price` (used specifically for take-profit exits) is the one exception
  — single-attempt by design, no retry loop at all — which is why take-profit exit process-latency
  numbers read tighter and lower than entry/stop-loss ones. `CloseResult` now carries an
  `attempts: u32` field (mirroring `TradeResult.attempts`, which already existed but was never
  logged), and both the console `[ORDER]` line and the Telegram "Order placed" /
  "... order executed" messages in `bin/live.rs` now print `n_attempts=N` (renamed from the
  ambiguous `attempts=N` — see next bullet) alongside `process_latency`, so a slow reading is
  explainable at a glance instead of looking like unexplained network variance.
- **Why the retry sleep exists, and why entries always pay it but exits sometimes don't
  (2026-07-08)** — the flat 1-second backoff (`tokio::time::sleep(Duration::from_secs(1))`) was not
  an arbitrary choice: it's the direct fix for the 2026-07-03 DOGE incident
  (`trader/doc/incident_doge_2026-07-03.md` §3), where an *uncontrolled* exit retry loop (no
  backoff at all) hammered the CLOB at up to one attempt per real tick — 284 attempts in ~9-10
  seconds — which the incident write-up flags as risking tripping exchange rate limits and burning
  the exit window doing nothing productive. The rule that came out of it: any internal retry loop
  against the live exchange needs a backoff between attempts. `LiveExecutionEngine::place`
  (entries) applies this uniformly — every retry sleeps the full 1s, regardless of *why* the
  previous attempt failed — which is a direct, intentional port of the Python reference bot's own
  `_place_order` (`../btc_5mins/bot/trading.py:376,407`, same unconditional `time.sleep(1.0)` on
  every retry). `close_position` (stop-loss exits) later got a smarter split (2026-07-04,
  `0ad6cd6`, "matches `bot/trading.py`'s retry cadence"): a FAK "no orders found to match" is
  retried **immediately**, since the order book can change tick-to-tick and waiting doesn't help
  and only costs exit-side urgency, while "not enough balance" (meaning the entry BUY's fill hasn't
  settled on-chain yet) keeps the 1s sleep, since that specifically *is* a fixed settlement delay
  that an instant retry can't shortcut (`execution.rs:530-536`). **This same fast-path was never
  back-ported to entries** — `place()` has no equivalent branch, so an entry retry sleeps the full
  1s even for a "no orders found to match" rejection, where (per the exit side's own reasoning)
  waiting doesn't actually help the fill. This is exactly what happened in
  `trader/doc/audit_trade_eth_2026-07-08.md`: the first entry attempt was killed with "no orders
  found to match," and the bot slept the full second anyway before the (successful) second attempt
  — not a bug, but a real, identified asymmetry between the entry and exit retry paths that's worth
  revisiting if entry latency ever becomes a binding constraint.
- **Follow-up: how conservative is the 1s retry sleep relative to Polymarket's actual rate limits?
  (2026-07-08)** — checked the current published limits
  ([docs.polymarket.com/quickstart/introduction/rate-limits](https://docs.polymarket.com/quickstart/introduction/rate-limits)):
  `POST /order` (single order — what both `place()` and `close_position()` use) allows a **5,000
  requests/10s burst** and a **120,000 requests/10min sustained** ceiling (~500/s burst, ~200/s
  sustained average), and — importantly — exceeding it is documented to throttle (delay/queue the
  request) rather than immediately reject it with a 429. (Some third-party guides/older cached
  search results quote lower figures, e.g. 3,500/10s burst — the number above is what the live docs
  page reports as of this check; either way the conclusion below holds by a wide margin.) Against
  this ceiling, our actual worst-case request rate is tiny: `order_max_retries = 3` means at most 4
  requests for a single entry, and the worst real incident on record — the 2026-07-03 DOGE storm —
  was 284 requests over ~9-10s (≈28-31 req/s) from a *single* misbehaving position, roughly two
  orders of magnitude under the documented burst ceiling even before accounting for the fact that
  `trade_assets` is currently scoped to one asset (`ETH`) at a time. **Conclusion: the flat 1s sleep
  is not load-bearing for rate-limit safety at today's request volume** — it was a reasonable
  defensive reflex adopted in the heat of the DOGE incident (any backoff beats none), not a number
  derived from Polymarket's actual published capacity. There is comfortable headroom to apply
  `close_position`'s existing fast-path (retry a "no orders found to match" FAK rejection
  immediately, keep the 1s sleep only for genuine settlement-delay cases like "not enough balance")
  to `place()`/entries too, closing the asymmetry noted in the bullet above, without meaningfully
  risking Polymarket's rate limits even under a repeat of the worst incident on record. **This is a
  recommendation, not yet implemented** — no code change made here; scope was research + doc only.
  If/when `trade_assets` grows beyond one asset, or another asset starts firing entries as
  frequently as DOGE's take-profit storm did, it's worth re-running this comparison rather than
  assuming the headroom still holds.
- **Entry retries split by error type; `order_max_retries` raised 3 → 5 (2026-07-08, implemented)**
  — see `trader/doc/plan_optimal_retry_sleep_2026-07-08.md` for the full analysis this implements.
  `execution.rs::place` no longer sleeps a flat 1s on every failure — it now classifies each one
  (`classify_entry_failure`) into three buckets, each with its own log line so a slow or exhausted
  entry is explainable from `live.log` alone:
  - `"no orders found to match with FAK order"` → retry after **10ms** (`NO_MATCH_RETRY_SLEEP`) —
    the book can change tick to tick, mirrors `close_position`'s existing treatment of this same
    error on the exit side.
  - Recognized deterministic errors (`"invalid amounts, ... decimals"`, `"invalid amount for a
    marketable BUY order ... min size"`) → **fail immediately, no retry, no sleep.** Confirmed via
    `git stash`-style log analysis that these are the same failure class as the 2026-07-03 DOGE
    oversell incident — no amount of retrying was ever going to help (one production example
    burned `n_attempts=4 process_ms=4303` retrying an order that could never succeed).
  - Anything else (unrecognized/unexpected error) → retry after **250ms** (`OTHER_RETRY_SLEEP`) —
    the one bucket without hard timing evidence either way, so a moderate rather than aggressive
    number.
  - `retry_entry_failure` (`execution.rs`) centralizes this decision and does the actual sleeping,
    logging which bucket fired and what sleep (if any) was applied on every attempt.
  `order_max_retries` raised `3` → `5` in `strategy_20260705.toml` (6 total attempts) — now that
  the common no-match case costs ~10ms instead of ~1s per retry, more attempts are nearly free
  time-wise, directly increasing fill probability inside `high_prob`'s narrow ~10-20s entry window.
  **Not changed**: `close_position`'s own retry cadence (already correct — 0s no-match, 1s
  "not enough balance") and `place_limit_sell`'s `settle_sleep` (1.5s) — both retain their existing
  genuine-settlement-lag sleep, per the plan doc's finding that this specific wait (~1-2s on
  Polygon) can't safely be shortened to sub-100ms without risking more failures, not fewer. 8 new
  unit tests (`execution.rs::tests`, `classify_entry_failure_*`/`retry_entry_failure_*`) pin the
  classification and the sleep/no-sleep/give-up decision for each bucket, including the exact error
  strings observed in production `live.log`. Full suite: 159 lib + 16 bin passing, clippy clean.
- **`signal_latency_ms` replaced by real per-feed exchange latency (`clob_latency`/
  `binance_latency`), and `attempts` renamed to `n_attempts` (2026-07-06)** — the previous
  `signal_latency_ms` (`received_ts − signal_ts`, where `signal_ts` was `tick.ts`, price_feed's
  *local* receipt timestamp) never measured real exchange network latency: since `poly-collector`
  and `trader-live.service` run on the same Oracle box against the same loopback NATS broker, that
  number only ever reflected the (genuinely near-zero, 0-1ms) intra-box NATS+processing hop —
  reading as "0ms" isn't a bug, it's just not what the name implied. Real exchange latency (CLOB
  server timestamp → price_feed receipt) was already computed for the parquet record
  (`latency_ms`, from `server_ts_ms`/`received_at_ms`) but never published to the trader. Fix:
  `poly_nats_payload`/`binance_nats_payload` (`price_feed/src/collect.rs`) now also publish
  `server_ts` (the exchange's own event timestamp, `null` when unavailable, e.g. Binance's `E`
  field missing). `bin/live.rs` extracts it alongside the typed tick (`extract_server_ts` — kept
  separate from `PolyTick`/`BinanceTick` themselves so this stays a `bin/live.rs`-only change, not
  a new field rippling into the ~80 existing tick-construction sites across
  `worker.rs`/`strategies.rs`/`machine.rs`/`backtest.rs`/`gates.rs`/tests), caches the latest value
  per feed on `AssetSlot`, and computes latency at order time as `received_ts − server_ts`. Exits
  print a single `clob_latency=` (exits are always Poly/CLOB-triggered — only `on_poly` ever
  produces a `ClosePosition`, confirmed by grep). Entries print whichever of `clob_latency=`/
  `binance_latency=` matches the feed that actually fired `try_enter` (`Worker::try_enter` runs
  from both `on_binance` and `on_poly` — a `Feed` tag threaded through `process_actions`/`execute`
  from each `tokio::select!` branch says which, so this is exact per-order, not a guess). Also
  renamed the entry/exit order logs' `attempts=1` to `n_attempts=1` — the counter was already
  correctly 1-indexed (`attempts=1` = succeeded on the first try, zero retries), just an
  ambiguous-looking label.
- **`clob_latency`/`binance_latency` redefined as real per-tick network latency, shown
  unconditionally on entry, with a staleness tag for whichever feed didn't trigger
  (2026-07-07)** — see `trader/doc/incident_missing_clob_latency_2026-07-06.md`. Two problems
  with the previous entry-side formula: (1) only the *triggering* feed's latency was computed at
  all — `Worker::try_enter` can fire off either a `BinanceTick` or a `PolyTick` (whichever
  completes the entry condition last), and the other feed's reading was silently absent from the
  message, not even shown as `n/a`; (2) the number itself (`received_ts − server_ts`, where
  `received_ts` was *order-placement* wall time) conflated genuine network latency with however
  long that tick had been sitting stale since — a Binance tick 3s old at trigger time read as
  "3056ms of Binance latency" when the real one-hop delay was ~117ms and the rest was pure
  staleness. Current formulas, both computed unconditionally every entry:
  - **`clob_latency`/`binance_latency`** (`exchange_latency_ms`, `bin/live.rs`) = that feed's last
    tick's own local receipt time (`PolyTick`/`BinanceTick::ts`, cached per-feed on `AssetSlot` as
    `last_poly_ts`/`last_binance_ts`) **minus** the exchange's own event timestamp for that same
    tick (`last_poly_server_ts`/`last_binance_server_ts`) — a fixed, genuine one-hop number,
    independent of how long ago that tick arrived relative to *now*.
  - **Tag in parens**: whichever feed's tick actually fired `try_enter` gets `(trigger)`; the
    other gets `(Nms ago)` = *now* (`received_ts`, order-placement wall time) minus that feed's
    last local tick timestamp — how stale that cached reading is at the moment the order was
    placed. E.g. `clob_latency=6ms (trigger) | binance_latency=117ms (2939ms ago)` reads as: the
    CLOB tick that fired this entry was itself fresh (6ms real latency), and separately, Binance
    hadn't sent a new tick in ~2.9s, with that last tick's own hop latency having been ~117ms when
    it did arrive.
  - Exit messages (`ClosePosition`, always Poly/CLOB-triggered) use the same `exchange_latency_ms`
    formula for `clob_latency` — no tag needed, only one feed is ever relevant there.
- **`process_latency` confirmed as a pure local round-trip, not mixable with a server timestamp
  (2026-07-07)** — checked whether Polymarket's order-placement response could supply a
  server-side confirmation time instead of `confirmed_ts = now_secs_f64()` (local, captured right
  after `.build_sign_and_post().await` returns). It can't, from this call: the vendored SDK's
  `PostOrderResponse` (what `LiveExecutionEngine::place`/`close_position*` actually receive) has
  no timestamp field at all — only `order_id`/`status`/`making_amount`/`taking_amount`/`success`/
  `transaction_hashes`/`trade_ids`. A server-side `match_time` only exists on the separate
  `TradeResponse` type (the `/trades` endpoint, or the USER-channel fill notifications
  `UnwindWatcher` already subscribes to independently) — reaching it here would need either a
  second API round-trip after the order already completed, or correlating against that separate
  async channel, neither of which is wired into this synchronous call. This is also the
  conceptually correct choice regardless of availability: `process_latency` is a round-trip
  *interval* (see the next bullet for exactly which two local timestamps it spans today), and both
  ends should come from the same clock (local) — mixing in a foreign server timestamp would
  introduce clock-skew error into what should be a clean duration measurement, unlike
  `clob_latency`/`binance_latency` above, which are legitimately one-way comparisons across the two
  clocks (and already carry that same caveat implicitly).
- **`process_latency` redefined to start from `signal_ts`, not `received_ts` (2026-07-08, by
  request)** — previously `process_latency_ms = (confirmed_ts − received_ts) * 1000.0`: only the
  dispatch-to-confirm leg (order call started → response received), deliberately excluding the
  (typically sub-millisecond) gap already reported separately as `signal_latency`. By request, this
  no longer matches the intended meaning: `process_latency` should read as the full "trigger signal
  received locally → order confirmed locally" duration. Fixed in `trader/src/bin/live.rs`: both
  order-triggering call sites (`Action::PlaceBuy`, `Action::ClosePosition`) now compute
  `process_latency_ms = latency_ms(*signal_ts, confirmed_ts)` via a new shared helper
  (`latency_ms(from_ts, to_ts) = (to_ts − from_ts) * 1000.0`, also used to recompute
  `signal_latency_ms = latency_ms(*signal_ts, received_ts)` for symmetry — same formula, different
  endpoints). `Action::PlaceLimitSell` (the internal GTC-resting follow-up to an entry fill, with no
  external `signal_ts` of its own — see the code comment at its call site) is unchanged: still
  `latency_ms(received_ts, confirmed_ts)`, the dispatch-only leg, since there's no earlier trigger
  timestamp to start from. `TradeRecord.entry_process_latency_ms`/`exit_process_latency_ms`
  (`types.rs`) now carry this same wider span — doc comments updated there accordingly. Regression
  test: `process_latency_spans_signal_ts_to_confirmed_ts_not_received_ts`
  (`trader/src/bin/live.rs`).
- **`trade_reconcile.py`'s Trade History table now shows signal and process latency separately,
  entry and exit (2026-07-07)** — previously two combined columns (`Entry Latency (ms)` = entry
  signal + entry process summed, `Exit Latency (ms)` similarly), which hid which half — tick/network
  delay vs. our own order round-trip — actually dominated a slow reading. Now four columns:
  `Entry Signal (ms)`, `Entry Process (ms)`, `Exit Signal (ms)`, `Exit Process (ms)`, reading
  straight from the CSV's own `entry_signal_latency_ms`/`entry_process_latency_ms`/
  `exit_signal_latency_ms`/`exit_process_latency_ms` columns with no summing.

---

</details>

<details>
<summary><strong>Trading engine — known incidents</strong></summary>

## Trading engine — known incidents

### Backtest reconciliation config-drift gap — fixed (2026-07-15)

Closes the README TODO flagged 2026-07-10: the daily recon's "Backtest Reconciliation"
section always replayed against whichever `strategy_*.toml` was lexicographically latest
*right now* (`config::load_latest`'s normal behavior), never the config that was actually
live during the historical window being reconciled — silently misclassifying real
config-drift as `BT DID NOT FIRE`. Added `config::load_file`/`backtest --config-file` to
pin one exact historical config, and `trade_reconcile.py::build_config_timeline` to
reconstruct which file was live at any past timestamp from each file's git first-commit
time (config files are never deleted in this repo, so "latest file as of T" is
reconstructable). A window spanning a config change now replays once per config era and
keeps each cycle's row only from the run whose config was actually active at that cycle's
own timestamp. Verified against the exact 2026-07-15 08:58 config swap this was built to
diagnose: the false `BT DID NOT FIRE` on the 08:55 BTC WIN resolved to an accurately
explained `OUTCOME DIFF` instead (entry conditions now agree — confirming the fix — the
residual outcome difference is a separate, already-documented, intentional backtest-only
rule, `machine.rs`'s `FORCE_UNWIND_BEFORE_CYCLE_END_SECS`, added 2026-07-14 and explicitly
scoped away from `worker.rs`/live). Full writeup:
`trader/doc/audit_recon_2026-07-15.md`.

### BTC stuck halted despite repeated `/resume` (2026-07-15, fixed)

`halt_rev` was tightened `2 → 1` on 2026-07-13, so a single stop-loss now trips the
per-strategy loss-streak halt immediately — a gate `/resume` intentionally never clears
(only `/reset_losses` or the daily reset can). `/reset_losses` was fully parsed and
tested (`commands.rs`, `control.rs`) but never actually wired into `bin/live.rs`'s
telegram dispatcher — it silently fell through to "not supported." Live BTC halted at
08:59:40 HKT, survived 3 `/resume` attempts (each replying with an unqualified success)
plus a restart, still halted 5+ hours later. Fixed: `ControlEvent::ResetLosses` wired
into the live dispatcher; `/resume`'s reply and `/status`'s halted light now say
*which* gate is still up instead of staying silent about it. Deployed to Oracle via
`scripts/deploy_trader.sh`. Full writeup:
`trader/doc/incident_unable_to_resume_2026-07-15.md`.

### Mid-cycle restart corrupted `cycle_open_binance` (2026-07-15, fixed)

Found while auditing costly stop-losses (cross-project audit from `../btc_5mins`, §3b):
`current_slot_val` (`trader/src/bin/live.rs`) initialized to `0` on every process start, so the
first `ticker` tick after *any* restart — config deploy, binary redeploy, crash+respawn under
systemd's `Restart=always` — always looked like a fresh cycle boundary, even 100+s into an
already-open one, stamping `open_binance` with whatever Binance traded at that instant instead of
the cycle's true open. Every signal for the rest of that cycle (`delta_pct`, the reversal reset,
the final `price_moved_up` resolution) then ran against a fabricated reference price. Confirmed
directly implicated in one real costly stop-loss on 2026-07-15. Fixed: a restart landing more than
5s into an already-open slot now skips firing `CycleOpen` for that cycle entirely (no entries) and
resumes normally at the next clean boundary, instead of fabricating a reference price — verified
locally (dummy-keyed dry run against live NATS data, twice) and live on the Oracle deploy that
shipped this fix, which itself landed 12s into a cycle and correctly suppressed. Full writeup:
`trader/doc/fix_live_deploy_2026-07-15.md`.

### `TradeRecord`/`HoldingData` gained `entry_price_ts` — additive, compiled/tested, not deployed (2026-07-14, added)

Investigating a `siglab` report anomaly (18 reversal-variant paper-trades logging identical
entry timestamps across *different* real markets) traced the root cause into shared code:
`Machine::try_enter`/`Worker`'s live entry path stamp `entry_ts` with whichever tick (poly or
Binance) triggered the check, not the timestamp of the poly price actually observed — since
every duration-task for an asset shares one Binance broadcast, this made economically
distinct markets (e.g. `sol-updown-5m`/`sol-updown-15m`) log identical `entry_ts`. Added
`TradeRecord::entry_price_ts`/`HoldingData::entry_price_ts` (from `LatestPolySignal::ts`,
`#[serde(default)]`) to both `trader/src/machine.rs` (backtest/siglab path) and
`trader/src/worker.rs` (live path, for schema consistency) — purely additive, zero change to
`entry_ts` itself or any entry/gate/fill/timeout decision. `cargo test --lib --bins`,
`cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings` all
clean on `trader` (187 tests passing; 4 pre-existing unrelated config-fixture failures
confirmed present before this change too). **Not deployed to the live `trader-live.service`**
per explicit instruction — compiled and tested locally only; redeploy separately if/when
wanted. Full writeup:
`siglab/doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md`.

### ETH TIMEOUT losses ran overnight without ever tripping the loss-streak halt (2026-07-14, fixed)

`Outcome::is_loss_for_halt()` blanket-excluded `Timeout` (the `unwind_time_rev`/`unwind_time_hp`
max-holding-time force-close) from the halt loss-streak regardless of `pnl` sign — correct for
`Unwind` (directionally fixed to a gain by construction) but wrong for `Timeout`, which can land on
either side of zero. Five losing ETH `reversal` `TIMEOUT` exits between the 02:00 HKT halt reset
and 08:09 HKT (-$1.075 total) never incremented `halt_losses`, so `halt_rev=1` never re-engaged.
Fixed: `is_loss_for_halt(self, pnl: f64)` now gates `Timeout` on `pnl < 0.0`; `HaltTracker::record_trade`/
`correct_trade` (shared by both the live trader and the Rust backtest, so both were affected and
both are fixed by the same change) thread `pnl` through accordingly. Checked the sibling
`../btc_5mins/bot/backtest.py` — same gap there, not fixed (out of scope, flagged for the user).
Full writeup: `trader/doc/incident_eth_timeout_halt_gap_2026-07-14.md`.

### `price_feed` collector crash-loop destroying its own recoverable data (2026-07-12, fixed)

Full root-cause: `price_feed/doc/incident_collector_data_loss_2026-07-12.md`. Plan:
`price_feed/doc/plan_timeout_backtest_and_mismatch_reason_2026-07-12.md` (unrelated `trader`-side
plan doc from the same day — this incident's own fix wasn't separately planned, it directly
implements that incident doc's own "Proposed solutions" section). `poly-collector.service` had
restarted 179 times in <2 days (2026-07-10 22:30 onward), driven entirely by `reconcile.rs`'s
phase-2 WS/REST staleness detector — and, critically, each restart was *destroying* the current
hour's still-recoverable data before anyone could run `recover_rust_parquet.py` on it, because the
trigger called `std::process::exit(1)` directly, skipping the footer-write. Four independent
fixes, all implemented and locally tested (see below):

1. **Reduce false triggers.** `reconcile::MISMATCH_TOLERANCE` `0.03` → `0.04`,
   `CONSECUTIVE_MISMATCHES_REQUIRED` `2` → `3`. New `is_near_cycle_close`/`NEAR_CLOSE_SKIP_SECS`
   (10s): the reconcile check is now skipped entirely (debounce state reset, not just paused) in
   the final 10s before a cycle closes, when a market's true price legitimately crashes toward 0/1
   and the order book often goes thin — the shape behind most of the 179 false triggers.
2. **Fix the amplifier — the real data-destroyer.** `spawn_reconcile_task` no longer calls
   `std::process::exit(1)` directly; it sends the reason on a new `mpsc` channel that wakes
   `run()`'s main `select!` loop, which flushes/seals every writer (footer written) through the
   *same* path `SIGTERM` already uses, then exits — `Restart=always` still restarts on top of it,
   but every restart now leaves a properly-sealed file behind, never a footerless `.tmp`.
3. **Guard rail.** `ParquetBuf::open_for_hour` no longer silently truncates a same-hour `.tmp` it
   couldn't carry-forward (a footerless leftover from some *other*, still-abrupt exit path) —
   it renames it aside as `<name>.unrecovered-<unix_secs>` first (mirroring how
   `seal_orphaned_tmp` already handles a *previous*-hour orphan), so `recover_rust_parquet.py` has
   something to work with instead of the bytes being destroyed on the spot.
4. **Independent data-quality observer**, since none of the above would have been *noticed* for
   2+ days otherwise: new `price_feed/scripts/data_quality.py`, wired into
   `trader/scripts/trade_reconcile.py`'s daily recon report as a new "Data Quality" section —
   flags any fully-elapsed hour where a `(asset, kind)` pair's sealed file has <50% minute
   coverage or is missing entirely, every single day, automatically, going forward.

**Tested locally before deploy:** 37 `price_feed` unit tests (guard rail exercised against real
footerless/empty/readable `.tmp` fixtures via `tempfile`) + 18 `reconcile.rs` tests (tolerance,
debounce, near-close skip, including a regression test for this incident's own
`rest_mid≈0`-across-many-assets shape) + 17 Python tests for the new observer. Beyond unit tests,
ran the actual compiled binary locally against **live** Polymarket/Binance data twice: once with
`MISMATCH_TOLERANCE` temporarily tightened to force a real trigger — confirmed the graceful-exit
path fires and every resulting file passes `recover_rust_parquet.py --check` (0 bad) instead of
being left footerless — and once at the real production thresholds for a clean 30s run + SIGTERM,
also 0 bad files. `cargo fmt --all --check` / `cargo clippy --all-targets --all-features -D
warnings` clean for `price_feed`; full `trader` suite (188 lib tests) unaffected.

**Deployed 2026-07-12** via `deploy_oracle.py --price-feed-only` (dry-run checked first — scope
was exactly cross-compile `price_feed` + rsync the binary + `systemctl restart poly-collector`,
nothing else touched). `poly-collector` restarted clean at 15:08:55 HKT running the new binary.
Verified post-deploy: **zero restarts and zero `RECONCILE-STALE` triggers** in the following 5+
minutes — a stretch that, under the old thresholds, had been producing a restart roughly every
5-30 minutes continuously since 2026-07-10 22:30. The in-progress hour's `.tmp` file sizes
(checked directly on Oracle, since a still-open hour isn't sealed yet for `data_quality.py` to
check automatically) scale proportionally with healthy pre-incident hours, not the ~5KB/hour
crash-loop pattern. Pre-deploy hours still show as GAP/MISSING in the daily recon report's Data
Quality section — that's the already-existing, already-documented damage from before the fix
landed, not a new issue; the first fully-elapsed **post**-deploy hour will be the first one this
report auto-confirms clean.

**Follow-up audit (2026-07-12, same day):** the first full recon report after the deploy showed
208/286 asset-hours flagged, which could look like the fix hadn't worked. Confirmed via
restart-count correlation against Oracle's journalctl that every flagged hour matches an hour with
2-8 crash-loop restarts, and the two unflagged hours inside the window had zero restarts — a 1:1
match. All flagged hours fall before the 15:08:55 deploy; hours after it (checked via a fresh
`data_quality.py --hours-back 6` run) are 100% clean. Full writeup: `trader/doc/audit_data_2026-07-12.md`.

### Halt state didn't reach disk immediately on `/halt`/`/resume`/balance events (2026-07-11, fixed)

In-memory `entry_suppressed` flipped right away but the on-disk state only caught up whenever some
other event happened to persist that slot next — a crash/restart in between silently reverted the
halt. Fixed for all 6 command/balance-driven call sites (SIGINT/SIGTERM deliberately excluded — see
doc). Full writeup: `trader/doc/plan_halt_persist_2026-07-11.md`.

### `price_feed`'s bba/price WS feed can silently stop delivering for one asset (2026-07-10, fixed)

Polymarket's shared best_bid_ask/price_change subscription went quiet for DOGE and ETH (205s each),
missing two real entry signals. A first-attempt 5s silence-timer fix caused a worse resubscribe
storm and was rolled back same session; landed instead as an observe-only silence logger plus a
REST ground-truth cross-check (`GET /midpoint`) that only restarts the process on a confirmed
mismatch, never from elapsed silence alone. Full writeup:
`price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md`.

### `build_backtest_prices.py::build_binance()` silently empty for any date after 2026-07-05 (2026-07-10, fixed)

Sourced from the retired old Python collector's merged output, which stopped updating 2026-07-05 —
every backtest date after that got zero binance rows, making the first "Backtest Reconciliation"
report misread "no price data at all" as a config-drift or halt-carryover bug. Fixed to source from
`price_feed/raw/` (this project's own collector) like `build_poly()` already did.

### `build_backtest_prices.py` broken — stale import after a collector rename (2026-07-10, fixed)

`recover_live_tmp` had been renamed to `recover_rust_parquet` (recovery split into per-kind
functions), leaving an import pointing at a module that no longer existed. Fixed the import + call
site; no logic change.

### `/halt`/`/resume` can now scope to one strategy, not just one asset (2026-07-10, added)

`/halt <asset> [strategy]` accepts an optional strategy argument, e.g. `/halt eth high_prob` no
longer also halts `eth reversal`. Full writeup: `trader/doc/plan_halt_per_strategy_2026-07-10.md`.

### ETH `high_prob` halted on a phantom second loss (2026-07-10, fixed)

A Gamma correction from LOSS→WIN never decremented `HaltTracker`'s loss count, so a later real loss
double-counted and tripped the 2-loss halt on only one actual loss. Fixed by applying the
loss-count delta on any Gamma correction. Full writeup:
`trader/doc/incident_halt_double_count_2026-07-10.md`.

### DOGE trade logged/alerted as WIN despite Polymarket resolving it a LOSS (2026-07-09, fixed; refined 2026-07-10/11)

Provisional WIN/LOSS is scored from the trader's own Binance ticks at cycle close, not Polymarket's
actual settlement, and a cycle-boundary bug was clobbering the async Gamma correction before it
could ever fire — so a real loss got Telegram-alerted as a win with no correction. Fixed same day
(watcher no longer clobbers `Confirming`, retries Gamma and halts new entries if unresolved by
deadline), then refined twice more: config-driven poll cadence + a balance-based override on the
halt (2026-07-09), then a longer, asset/strategy-scoped Gamma deadline with a scoped
balance-decrease halt (2026-07-11). Full writeup:
`trader/doc/incident_DOGE_wrong_result_2026-07-09.md`; follow-on plan:
`trader/doc/plan_gammapi_2026-07-11.md`.

### `cargo fmt --all --check` cleaned up, both crates (2026-07-08, fixed)

The `~350` diffs flagged above (deferred from the 2026-07-07 clippy pass) turned out to be `374`
diffs across `26` files in `trader`, plus `55` more across `price_feed` — same root cause in both:
no `rust-toolchain.toml`/`rustfmt.toml` in the repo, so each crate was formatted by whatever
rustfmt happened to be installed at the time, and the currently-installed `rustfmt 1.9.0-stable`
(`rustc 1.96.1`, 2026-06-26) disagrees with that on import-statement ordering and struct-literal/
enum-variant field wrapping (multi-field literals that used to fit on one line now wrap one field
per line). Confirmed via `git stash`/clean-checkout diffing that none of this was caused by any
in-flight feature work in either crate.

Fixed with a single `cargo fmt --all` per crate — purely mechanical, zero behavior change, verified
by re-running the full check afterward in both:
- `trader`: `cargo build`, `cargo test` (152 lib + 16 bin, unchanged pass count), and
  `cargo clippy --all-targets --all-features -- -D warnings` all clean, before and after.
- `price_feed`: `cargo build` and `cargo test` (5 tests) both clean before and after. **Note:**
  `cargo clippy --all-targets --all-features -- -D warnings` failed on `price_feed` with 12
  pre-existing errors at the time (mostly `collapsible_if`) — confirmed via the same `git stash`
  check to predate this fmt pass entirely (`price_feed` never got the equivalent of `trader`'s
  2026-07-07 clippy cleanup). Left untouched in this pass — out of scope for a formatting-only
  change. **Fixed same day, separately — see the next entry below.**

At the time, deliberately **not** added: a `rust-toolchain.toml` pin to stop this drift from
recurring. Held back specifically because `scripts/deploy_trader.sh`'s aarch64 cross-compile step
(`cross build --release --bin=live --target=aarch64-unknown-linux-gnu`) runs in a separate
Docker-based toolchain that `cross` manages itself; a repo-root toolchain pin could force that
container to fetch a specific version on its next build rather than using whatever it already has
cached, which wasn't something to risk against the live trading deploy path without testing it in
isolation first. **Added and verified later the same day — see "Toolchain pin added" below.**

### `price_feed` clippy cleanup (2026-07-08, fixed)

The 12 errors flagged above: 7x `collapsible_if` (`collect.rs`, `markets.rs`) collapsed into Rust
let-chains (`if let X && cond { ... }`), behavior-identical; 3x `ptr_arg` (`&PathBuf` -> `&Path`)
on `collect.rs`'s `open_for_hour`/`AssetWriters::new`/`BinanceWriters::new` — call sites unaffected
via deref coercion, internal `.clone()` calls on the narrowed param switched to `.to_path_buf()`;
1x `too_many_arguments` on `collect.rs::write_sample` (8/7) allowed with a justifying comment
(private, 3 call sites, each arg independently meaningful), matching the precedent already set for
`trader/src/worker.rs::Worker::common`. Verified: `cargo build`, `cargo test` (5/5), `cargo clippy
--all-targets --all-features -- -D warnings`, and `cargo fmt --all --check` all clean.

### Toolchain pin added: `rust-toolchain.toml` (2026-07-08, added)

Pins `channel = "1.96.1"` (today's already-installed version) plus `rustfmt`/`clippy` components,
at the repo root — applies to both `trader` and `price_feed` via rustup's directory-walk-up
resolution. This is what stops the drift that caused the fmt cleanup above from recurring: without
it, the next dev machine (or CI runner, or this machine after a `rustup update`) picks whatever
"stable" happens to resolve to at the time, silently diverging from whatever last formatted the
repo.

Tested in isolation before merging, specifically targeting the risk flagged above (the aarch64
`cross`/Docker toolchain needing to fetch a pinned version it doesn't already have cached):
- Local host tooling: `cargo build`/`test`/`clippy`/`fmt --check` all clean under the pinned
  toolchain (rustup auto-installed a distinct `1.96.1-x86_64-unknown-linux-gnu` toolchain
  alongside the existing `stable` one — same underlying version, so no behavior change, just now
  explicit instead of implicit).
- `cross build --release --bin live --target aarch64-unknown-linux-gnu` (`trader`): the risk
  materialized once — the container needed to fetch the `rust-std` component for the aarch64
  target under the pinned channel, costing ~13s. **A second run immediately after came back in
  under 2 seconds** — confirming this is a one-time, cacheable cost, not a per-deploy recurring
  one.
- `cross build --release --target aarch64-unknown-linux-gnu` (`price_feed`, which has its own
  `Cross.toml` for `libssl-dev:arm64`/`pkg-config` pre-build steps): also clean, ~7s, no new
  toolchain fetch needed (already warmed by the `trader` build above).
- `./scripts/deploy_trader.sh --dry-run`: full pipeline clean end-to-end with the pin in place.

No real deploy was run as part of this change (dry-run + isolated `cross build` only) — the
pin itself doesn't change what gets built, only which toolchain version builds it.

### `--update-config` deploy mode: commit+push+sync in one step (2026-07-08, added)

Added `scripts/deploy_oracle.py --update-config` (and `./scripts/deploy_trader.sh --update-config`)
— commits + pushes `trader/config/` if it has uncommitted changes (pathspec-scoped to that
directory only, same pattern as the "Recon auto-commit" fix above), aborting before ever
connecting to Oracle if the commit/push fails, then does exactly what `--config-only` already did:
rsync + symlink + restart, no build, no binary rsync. Previously, landing a hand-edited
`strategy_*.toml` on Oracle required two separate manual steps — `git commit && git push`, then
`--config-only` — with nothing enforcing they happened together or in order; this collapses that
into one command and one failure mode (git fails → nothing touches Oracle). See "Editing a config
and deploying it in one step" above for usage; tests in `scripts/test_deploy_oracle.py`
(`test_update_config_commits_before_syncing`,
`test_update_config_never_touches_oracle_when_git_push_fails`).

### `unwind_time` — max-holding-time force-exit (2026-07-08, added)

New per-strategy, per-asset config parameter `unwind_time_rev`/`unwind_time_hp` (seconds; `0.0` =
disabled), ported from `btc_5mins/studies/unwind_safely`'s backtest engine — see
`trader/doc/plan_unwind_time_2026-07-08.md` for the full design writeup. While a position is open,
checked **last** in the exit chain (after PnL-based stop-loss and take-profit both fail to fire on
a given tick): if `now - entry_ts >= unwind_time`, force-close at whatever the current market price
is, win or lose — a pure max-exposure-time cap, independent of whether any PnL threshold is even
reachable. This directly backstops the class of failure documented in
`trader/doc/audit_sl_no_trigger_2026-07-07.md` (SOL/DOGE positions that bled out because
`sl_pnl_rev` was unreachable at their entry price) — a stuck position now has a second, orthogonal
exit condition that doesn't depend on price ever crossing anything.

Implementation: new `WorkerState::TimingOut`/`Outcome::Timeout`/`CloseReason::Timeout`, mirroring
`StopExiting`/`Outcome::StopLoss` exactly (same unbounded-FAK mechanics, same "re-fires every
PolyTick until cleared" retry behavior), kept as a distinct variant rather than folded into
`StopExiting` so the outcome and Telegram copy ("⏱️ TIME LIMIT triggered") can differ. Originally
excluded from the halt loss-streak unconditionally (`Outcome::is_loss_for_halt` only matched
`Loss`/`StopLoss`), on the reasoning that a time-cap exit isn't a signal-quality failure the way a
real stop-loss is — **superseded 2026-07-14**: that blanket exclusion let real TIMEOUT losses run
uncounted; it now counts toward the halt exactly when `pnl < 0.0`, see the "ETH TIMEOUT losses ran
overnight" incident entry below. Visible in Telegram `/status` alongside
`unwind_pnl`/`sl_pnl` (this is the exact visibility gap that let the `sl_pnl` stale-config incident
above go unnoticed for a full deploy cycle).

**Shipped at `30.0`s for both strategies** (ETH, the only live `trade_assets` entry) — the
walk-forward study's final-calibration value. Flagged explicitly in the plan doc: this sits at the
top of the study's tested 10–30s range, the same grid-boundary-artifact pattern already documented
for `sl_pnl` in `btc_5mins/studies/bt2/followup_sl_pnl_boundary_2026-07-07.md` — the sweep shows
"longer beat shorter at every step within [10, 30]," not that 30s is a validated optimum. Shipped
anyway (rather than disabled, or waiting on a wider re-sweep) because the risk here is
asymmetric-safe compared to `sl_pnl`: a too-short `unwind_time` only makes exits *more* conservative
(closes earlier/more often), the opposite direction from the SOL/DOGE failure mode where a
boundary value masked a threshold that couldn't fire at all.

### Halt state and `/status` counters didn't survive a restart (2026-07-08, fixed)

A balance-drawdown halt engaged 2026-07-07 stayed silently cleared by a routine
`trader-live.service` restart 12+ hours later — not by `/resume`, not by the loss-streak's daily
reset — with zero Telegram notification either way (full diagnosis:
`trader/doc/incident_no_reset_notification_2026-07-08.md`). Root cause: `entry_suppressed`
(`/halt`/`/resume`/the drawdown guard) and `HaltTracker`'s loss/session counters only ever lived
in-memory on `Worker`; a restart rebuilds every `Worker` from scratch via `new_reversal`/
`new_high_prob`, which always starts un-halted, and no code path notifies on that transition.
The same gap meant `/status`'s win/loss/stoploss/unwind/timeout counts and total PnL — tracked on
`bin/live.rs`'s `AssetSlot`, never on `Worker` — also reset to zero on every restart, even with no
trade in between.

**Fix — restart now round-trips both:**
- `PersistedState` (`worker.rs`) gained `entry_suppressed`, `halt_losses`, `halt_last_session`
  (`#[serde(default)]`, so a pre-existing `live_state_*.json` still loads — as "un-halted, zero
  counters," identical to today's from-scratch behavior). `HaltTracker` gained `losses()`/
  `last_session()`/`restore()` (`backtest.rs`); `Worker::restore_halt()` rebuilds both flags from a
  loaded file. `halt_max`/`halt_reset_hour` are deliberately never persisted — they always come
  fresh from config, so a config change between restarts takes effect immediately rather than
  being shadowed by the old file.
- `bin/live.rs` wraps `PersistedState` plus a new `PersistedStats{wins,losses,stoplosses,unwinds,
  timeouts,total_pnl,last_trade}` in one `PersistedSlot` written to the same `live_state_*.json` —
  no new files. `persist()` now takes `&AssetSlot` (was `&Worker`) so both halves are written
  together; `load_persisted_slot()` is best-effort (missing file, corrupt JSON, or a legacy
  pre-this-change shape all fall back to a fresh un-halted/zero-stats start, never a hard failure)
  and runs once per `(asset, strategy)` slot at startup, before the first cycle opens.
- `on_control`/`on_balance` (`/halt`, `/resume`, the drawdown guard) now also emit
  `Action::Persist`. Previously they returned no actions at all, so a halt/resume only reached disk
  whenever the *next* trade-lifecycle event happened to persist — up to ~5 minutes away at the next
  cycle open. A restart in that window would have silently lost a just-issued `/halt` even with the
  fix above; this closes it so every halt-state change is flushed immediately.

**Net effect:** `/status` after a restart is now identical to before it, provided no trade and no
config change happened in between — the two things a restart legitimately should and shouldn't
remember, respectively (a config change correctly changes the displayed `sl`/`halt_after`/etc.
values; live balance and current market prices are re-fetched live either way, restart or not, and
were never meant to be "restored").

**Deliberately out of scope:** an in-flight *position* still does not resume across a restart —
`Worker::reconcile`/`resume_from` exist and are unit-tested (`to_persisted_round_trips_holding_state`
etc.) but have no call site in `bin/live.rs`; `live_state_*.json` has effectively been write-only
for that part of the state since the file was introduced. Flagged in the incident doc as a known
follow-up, not fixed here — halt/stats parity doesn't depend on it, and wiring up live position
resume is a larger, separate change (needs a CLOB reconciliation call against real order/balance
state before trusting a resumed `Holding`, per `reconcile`'s existing doc comment).

New tests: `control_and_balance_events_persist_immediately`,
`halt_state_round_trips_across_a_restart`, `manual_halt_round_trips_across_a_restart` (`worker.rs`);
`round_trips_halt_state_and_stats`, `legacy_file_without_new_fields_loads_with_defaults`,
`missing_file_loads_as_none`, `corrupt_file_loads_as_none_not_a_panic` (`bin/live.rs`). Full suite:
166 passed (152 lib + 14 bin), 0 failed. Verified live on Oracle post-deploy: `live_state_eth_*.json`
now carries `entry_suppressed`/`halt_losses`/`halt_last_session`/`stats` after the restart that
shipped this fix.

### Recon auto-commit swept up unrelated staged changes under a misleading message (2026-07-07, fixed)

`scripts/trade_reconcile.py` (the daily reconciliation report, cron-scheduled every 2 hours via
`scripts/bash/run_daily_recon.bash`) auto-commits and pushes its own regenerated markdown report
via `git_commit_push()`. That function `git add`-ed just the one report path, but then ran
`git commit -m message` with **no pathspec** — which commits the *whole* index, not only the file
just added. A manual `git add` of unrelated in-progress work (staging four separate files for an
unrelated fix, right as this cron job's own scheduled run landed) got silently swept into the same
commit, which then pushed to `origin/main` under the auto-generated message
`recon: 2026-07-06 — 1/1 matched (100%)` — content was correct (nothing lost or corrupted), but
the message badly undersold what the commit actually contained, and the race could just as easily
have interrupted a commit mid-`git add`, landing a half-staged change.

**Fix:** `git_commit_push()` now runs `git commit -m message -- <rel_paths>` — the trailing
pathspec restricts the commit to exactly the paths this function was given, regardless of
anything else staged in the index at that moment. Verified in an isolated throwaway repo: an
unrelated staged file is left untouched (still staged, not committed) rather than swept in, and
the "no changes to this specific path" case still fails exactly as before (non-zero exit, caught
by the existing `except subprocess.CalledProcessError`) — no new failure mode for the unattended
cron path.

**Lesson:** any automation that does its own `git add` + `git commit` should always scope the
commit itself to the same paths it just added — `git commit` with no pathspec commits the entire
index, which is almost never what a narrowly-scoped auto-commit script actually wants, and the gap
only shows up the moment something else happens to be staged at the same time.

### ETH stop-loss needed 31 attempts to close in the last 20s of a cycle (2026-07-07, not a bug)

Recon flagged `exit_attempts: 31` on an ETH `high_prob` stop-loss that filled at 0.47 against a
0.82 trigger. Root cause: the position was entered ~20s before candle close, and ETH crossed the
strike in that final stretch, cratering the DOWN token from 0.665 toward zero — a window where
resting liquidity vanishes as market-makers pull quotes ahead of resolution, so each FAK sell (one
per real tick, each with its own 5x immediate inner retry on "no orders found to match") kept
getting killed until a buyer finally appeared. Confirmed as the stop-loss retry design (unbounded,
must-close, one outer attempt per tick) working as intended under genuinely thin liquidity, not a
regression — full timeline and math in `trader/doc/incident_31_retry_sl_2026-07-07.md`.

### `reversal` stop-loss (`sl_pnl_rev = 0.80`) unreachable or too-loose by design (2026-07-07, audited, not fixed)

Two `reversal` trades (SOL entry 0.75, DOGE entry 0.94) lost almost their full stake — one with the
stop-loss never firing at all, the other firing only ~1 second before cycle close. Root cause is
config, not code: `sl_hit`'s threshold is `entry_price − sl_pnl_rev`, and at the shared default
`sl_pnl_rev = 0.80` that's *negative* (unreachable) for any entry below 0.80, and barely above zero
for entries just above it — so by the time it's reachable at all, the position has already lost
most of its value in these fast-resolving 5-minute markets. A repo-wide check found 3 historical
`reversal` trades total with a structurally-unreachable threshold (2 survived by luck before this
one didn't). Full tick-by-tick CLOB + order-book evidence and a sensitivity table showing what a
tighter threshold would have done: `trader/doc/audit_sl_no_trigger_2026-07-07.md`. No config change
made — this is a calibration decision, not applied without direction. **Follow-up traced the root
cause upstream**: every *unconstrained* backtest sweep in `../btc_5mins/studies/bt2` actually picks
`sl_pnl = 0.00` (no stop-loss) as PnL-optimal — `0.80` only exists because the walk-forward study
that produced it explicitly excluded `sl_pnl = 0` and then walked to that search's grid maximum
(`../btc_5mins/studies/bt2/followup_sl_pnl_boundary_2026-07-07.md`).

### Loss-streak halt now sends Telegram notifications on engage and reset (2026-07-07, added)

The consecutive-loss halt (`halt_rev`/`halt_prob` — distinct from manual `/halt` and the balance
drawdown halt, both of which already notified) previously changed state completely silently; the
only way to notice was polling `/status`'s 🟡/🟢 indicator. Two new `Action` variants close the gap:

- **`Action::HaltEngaged`** — fired from the exact trade (`on_cycle_close` or
  `finalize_or_hold_residual`'s stop-loss/unwind-fill paths) whose loss crosses `halt_rev`/
  `halt_prob`'s threshold. `HaltTracker::record_trade` (`backtest.rs`) now returns `bool` — `true`
  only on the transition from not-halted to halted, so an already-open position resolving as a loss
  *after* the halt has already engaged doesn't re-fire it.
- **`Action::HaltReset`** — fired from `on_cycle_open` when the daily HKT session rollover
  (`halt_reset_hour_rev`/`halt_reset_hour_hp`) actually clears an *active* halt.
  `HaltTracker::reset_if_new_session` now returns `bool` for the same reason — a session rollover
  with nothing to clear (the common case, most days) stays silent rather than sending a notification
  every single day at 02:00/08:00 HKT regardless of whether anything happened.

Both plumb through `Worker::step`'s existing `Vec<Action>` return the same way every other
Telegram-worthy state change does, and `bin/live.rs`'s `process_actions` gets two new dedicated
match arms (alongside the existing `StopLossVerdict`/`LogTradeCorrection` ones) building the
messages — no new architecture, same pattern as the existing stop-loss-triggered notification.
`backtest.rs::run_backtest`'s own calls to both methods discard the new return value — zero
behavior change to backtest/sweeps. New tests: `halt_tracker_record_trade_signals_only_on_the_crossing_loss`,
`halt_tracker_record_trade_ignores_non_loss_and_other_strategy`,
`halt_tracker_reset_signals_only_when_clearing_an_active_halt` (`backtest.rs`), plus
`halt_reset_on_session_rollover_with_no_active_halt_is_silent` and extended assertions on
`halt_by_loss_streak_suppresses_entry_and_resets_next_session` (`worker.rs`).

### `cargo clippy --all-targets --all-features -- -D warnings` cleaned up (2026-07-07, fixed)

`trader`'s clippy had drifted to 24 pre-existing errors on `main` (confirmed unrelated to any
feature work — same count on a clean checkout before this pass), evidently from a toolchain/clippy
version bump surfacing lints this code predates. All fixed, no behavior change to any of them —
verified via `cargo build`/`cargo test` (141 lib + 10 bin tests, all passing) after every fix:

- **9× `empty_line_after_doc_comments`** (`config.rs`, `gates.rs`, `signal/mod.rs`,
  `signal/delta_pct.rs`, `signal/latest_binance.rs`, `signal/latest_poly.rs`, `signal/saw_low.rs`,
  `strategies.rs`, `types.rs`) — a file-level `///` doc comment followed by a blank line reads as
  documenting the *next item* (a `use`/`mod`), not the file. All were genuinely file-level docs;
  changed `///` → `//!` on each rather than deleting the blank line (which would've kept the
  comment wrongly attached to the following `use` statement).
- **6× `collapsible_if`** (`marketdata.rs`, `telegram/mod.rs`, `worker.rs`×2, `api_probe.rs`,
  `live.rs`) — nested `if let X { if cond { ... } }` collapsed into `if let X && cond { ... }`
  (Rust let-chains). Behavior-identical.
- **5× `new_without_default`** (`signal/delta_pct.rs`, `signal/latest_binance.rs`,
  `signal/latest_poly.rs`×2, `signal/mod.rs`) — added `impl Default { fn default() -> Self {
  Self::new() } }` for each `pub fn new()` with no args.
- **`single_match`** (`redemption.rs`) — `match { Ok(true) => {...}, Ok(false)|Err(_) => {} }` →
  `if let Ok(true) = ...`.
- **`needless_question_mark`** (`marketdata.rs::http_client`) — `Ok(foo?)` → `foo`.
- **`trim_split_whitespace`** (`telegram/commands.rs::parse_command`) — `.trim().split_whitespace()`
  had a redundant `.trim()` (`split_whitespace()` already ignores leading/trailing whitespace).
- **`neg_multiply`** (`machine.rs` test) — `-1.0 * 0.20` → `-0.20`.
- **2× `suspicious_open_options`** (`bin/shadow.rs`, `bin/live.rs::append_csv_header_if_new`) —
  `OpenOptions::new().create(true).write(true)` with no explicit truncate/append intent; both call
  sites only ever run when the file doesn't already exist (guarded by an `if !exists`/`if exists {
  return }` check just above), so `.truncate(true)` documents the already-true behavior rather than
  changing it.
- **2× `question_mark`** (`bin/live.rs::execute`) — `let Some(token_id) = slot.current_token_id
  else { return None };` → `let token_id = slot.current_token_id?;` (the enclosing fn already
  returns `Option<Event>`).
- **`too_many_arguments`** (`worker.rs::Worker::common`, 9 args) — added
  `#[allow(clippy::too_many_arguments)]` rather than restructuring: private, 2 call sites
  (`new_reversal`/`new_high_prob`), each arg independently meaningful — a wrapper struct would add
  a layer without a real clarity gain here.
- **`if_same_then_else`** (`worker.rs::reconcile`'s `Entering` arm) — both branches of `if
  token_balance > 0.0 { Watching } else { Watching }` returned the identical value; collapsed to
  unconditional `WorkerState::Watching` with the explanatory comment kept (the surrounding doc
  comment already establishes both cases are meant to resolve the same way — this wasn't a missed
  branch, just dead conditioning).

Not addressed in this pass: `cargo fmt --all --check` also has ~350 pre-existing diffs across the
crate (same toolchain-drift shape, confirmed unrelated to any feature work) — out of scope here
since `cargo fmt --all` would rewrite most lines of every touched file, obscuring any real change
in the same commit. Left for a dedicated formatting-only pass if wanted. **Done, see below.**

### `--trader-only` deploy silently left Oracle running a stale strategy config (2026-07-07, fixed, critical)

Telegram `/status` showed `sl_pnl=0.8000` for ETH reversal right after a deploy meant to set it to
`0.25` — `trade_assets` narrowing to ETH *did* take effect, `sl_pnl_rev` didn't. Root cause:
`deploy_oracle.py`'s `--trader-only`/default path (`scripts/deploy_trader.sh` always uses
`--trader-only`) rsyncs the binary and bakes `--asset` flags into the systemd unit from *this
machine's* local config, but never rsyncs `trader/config/` itself to Oracle — only `sync_config()`
(previously wired to the separate `--config-only` mode) does that, and the running binary re-reads
its `strategy_*.toml` from Oracle's own copy on every restart. `trade_assets` reached the process
via the CLI-flag channel (always current); `sl_pnl_rev` only exists inside the TOML (silently
stale). **Fix:** every trader-deploying mode now calls `sync_config()` unconditionally before
restarting the service, and aborts without restarting if it fails. New test file
`scripts/test_deploy_oracle.py` (stdlib `unittest`/`mock`, no new dependency — first Python tests in
this repo) pins the fixed step ordering across all four deploy modes. Full writeup:
`trader/doc/incident_stale_oracle_config_2026-07-07.md`.

### Take-profit exit had no price floor — an 8¢ slippage turned a 3¢ profit into a loss (2026-07-06, fixed)

A SOL reversal position bought "Up" at 0.90 with a 3¢ take-profit target (`tp_price = 0.93`),
but the logged exit was `TRADE UNWIND ... entry=0.9000 → exit=0.8200 ... pnl=-$0.1073` — a
take-profit that lost money, even though the underlying (Binance SOL) moved the *correct*
direction across the cycle. Full writeup, including the exact `live.log` sequence and pnl
arithmetic: `trader/doc/incident_sol_unwind_but_loss_2026-07-06.md`.

**Root cause:** entry BUYs have always had a real max-price guard (`gates.rs`'s `MaxBuyPrice`/
`PriceHighRev` gates, plus a *limit* FAK with `.price()` capped at `max_buy_price` in
`execution.rs::place`), but the take-profit ("unwind") exit's `close_position()` was a **bare
market FAK with no price bound at all** — once the take-profit trigger fired, the sell would
fill at whatever price the book gave it, arbitrarily far below the trigger. In this trade, a
brief thin-book spike crossed `tp_price`, the close fired correctly, but the FAK needed 3
internal retries (~3.4s: one for the entry BUY's on-chain settlement lag, two for "no orders
found to match") before it filled — by which point the spike had reverted and the sell landed
at 0.82, 11¢ below the 0.93 target.

**Fix:** `execution.rs::close_position_at_price(token_id, shares, min_price)` — a single-attempt
FAK **with** `.price(min_price)`, used only for take-profit closes, bounded at the position's own
`tp_price` (no new config — the minimum acceptable sell price *is* the take-profit target).
Stop-loss closes are unchanged (`close_position()`, still unbounded — a stop-loss must close
regardless of price). If the bounded attempt can't fill immediately, `worker.rs::on_unwind_failed`
now re-arms `PriceMonitor { tp_price }` and waits for the next real `PolyTick` to retry, instead
of the old one-shot `TakeProfitAbandoned` latch — safe now that each attempt is price-bounded
(can't fill worse than the target) and naturally rate-limited by real ticks rather than an
internal retry loop (which is what caused a *different* incident's 284-attempts-in-9s hammering,
`incident_doge_2026-07-03.md`).

**Lesson:** a price guard on one leg of a trade (entry) doesn't imply the mirror-image guard
exists on the other leg (exit) — check both independently. A dead config key
(`order_slippage` in `strategy_*.toml`, parsed nowhere in `trader/src`) turned out to be exactly
this gap, seemingly planned and then never wired up.

**What exactly changed on the "3 internal retries," precisely:** it's not *just* adding a price
— the retry mechanism itself changed. The old `close_position()` (still used for stop-loss)
retries internally, in one call, up to 5 times: on `"balance: 0"` (the entry BUY's fill is
confirmed by the API immediately, but the token isn't actually spendable until the Polygon
transaction settles on-chain, typically ~1-2s) it sleeps 1s and retries; on `"no orders found to
match with FAK order"` (a FAK only matches liquidity resting on the book *right now* — a thin
book like SOL's routinely has brief moments with none) it retries immediately. That internal
loop, with no price floor, is exactly what produced this incident's 3.4-second, 3-failed-attempt
sequence ending 11¢ away from target. `close_position_at_price()` has **no internal retry loop
at all** — one attempt; if it fails, for either reason, it returns `Failed` immediately, and
`worker.rs::on_unwind_failed` re-arms `PriceMonitor{tp_price}` so the *next real `PolyTick`*
triggers the next attempt, rather than an internal sleep. One consequence worth flagging
explicitly: the old settlement-lag retry (`"balance: 0"` → sleep 1s → retry) is gone for
take-profit closes specifically. If a take-profit fires within ~1-2s of entry (before the BUY
settles on-chain — exactly this incident's shape), the first bounded attempt will still hit
`"balance: 0"` and return immediately; recovery now depends on the next `PolyTick` arriving and
the price still qualifying, not a guaranteed 1-second internal wait. In practice this is usually
equal or faster (real ticks tend to arrive more than once a second in an active market), but it
is a genuine behavioral difference from before, not merely "same retries, now with a floor."
Stop-loss (`close_position()`) got neither change — still unbounded, still the internal 5x retry
loop, per direction (a stop-loss must close regardless of price or retry cadence).

### Entry evaluation only checked on Binance ticks, missing fast poly-side crossings (2026-07-04, fixed)

`Worker::on_binance`/`Machine::on_binance` (`trader/src/worker.rs`, `trader/src/machine.rs`)
were the only place `ReversalStrategy`/`HighProbStrategy::evaluate` ever got called — even
though the entry condition for both strategies is a conjunction of a **poly** price
band/threshold (the primary, time-critical trigger) and a `delta_pct` sign check (a
directional filter). `Worker::on_poly`/`Machine::on_poly` updated poly state but never
triggered entry evaluation itself, so a poly price that crossed its trigger band **between**
Binance ticks sat unnoticed until the next Binance tick happened to arrive — up to the
Binance feed's own tick interval (see "Latency & observability infrastructure" above: ~250ms
today, sampled/coalesced from the real per-trade WS stream).

Confirmed this isn't just a synthetic-test concern: replaying real BTC data from
2026-06-20 (`backtest::btc_20260620_golden`, previously validated against the Python
reference engine) turned up a case where poly's `up` price spiked 0.145 → 0.605 in under
half a second while Binance ticks in that window landed roughly once per second — the old
design couldn't see the crossing in time to act on it at all.

**Fix:** both `on_binance` and `on_poly` now call a shared `try_enter(now)`, so entry can
fire off either feed using the latest cached value of the other (`check_gates`'s existing
`|delta_pct| >= threshold` gate is unchanged — this only affects how promptly the condition
is checked, not how permissive it is). `worker.rs` (live) and `machine.rs` (backtest) were
kept in sync so backtest results stay representative of live behavior.

Fixing this exposed a real latent bug: `DeltaPctSignal::reset()` (`trader/src/signal/
delta_pct.rs`) cleared `open` but not `price` on a new cycle — harmless under the old
design (`on_binance` always refreshed `price` in the same call that evaluated it), but a
real risk once `on_poly` can trigger evaluation without refreshing `price` itself, since a
stale Binance price left over from the *previous* cycle could otherwise pass as this
cycle's already-known delta. Fixed by clearing `price` on every `reset()` too.

Full writeup, the poly-vs-Binance latency reasoning behind the decision, and the exact
golden-test trade this uncovered: `trader/doc/latency_2026-07-04.md` §8/§9.

**Lesson:** when a strategy's entry condition depends on two independently-arriving feeds,
gating evaluation behind only one of them makes entry timing hostage to that one feed's
cadence — even if the *other* feed is the one that's actually time-critical. Worth checking
for this pattern anywhere else two signals are combined behind a single trigger event.

### Entry BUYs rejected outright — Amount::shares violated a market-buy precision rule (2026-07-04, fixed, critical)

A same-day change (`7d0f96c`, "buy in rounded shares instead of rounded dollars" — see the
`incident_tele_pnl_2026-07-04.md` write-up it came from) switched entry BUYs from
`Amount::usdc(size_usdc)` to `Amount::shares(...)`, to stop a `<0.01`-share dust remainder
from being left behind on the exit leg. It shipped, was redeployed to Oracle at 22:51, and
the very first entry attempt on the new binary (DOGE, 23:09:37) failed all 4 retries with
`"invalid amounts, the market buy orders maker amount supports a max accuracy of 2 decimals,
taker amount a max of 4 decimals"` — and kept failing identically regardless of price. Full
writeup: `trader/doc/incident_order_rejection_2026-07-04.md`.

**Root cause:** the vendored SDK computes a market BUY's maker (USDC) leg differently
depending on which `Amount` variant is submitted. `Amount::usdc(size_usdc)` passes the
caller's own already-2-decimal dollar figure straight through as the maker amount (always
valid) and derives shares (up to 4 decimals, which the API allows). `Amount::shares(...)`
instead derives the maker amount as `shares × price` — and a 2-decimal share count times a
2-decimal price generically needs *more* than 2 decimal places to represent exactly, which
Polymarket rejects outright for a market BUY. This isn't a rounding-threshold bug fixable by
adjusting the target share count (the way an earlier same-day incident,
`incident_order_fail_2026-07-04.md`'s $1.00 marketable-notional floor, was) — it's structural
to using `Amount::shares` on a market BUY's maker leg at all, so it hit essentially every
entry, on every asset, blocking all new positions from the 22:51 redeploy until fixed.

**Fix:** reverted the entry BUY to `Amount::usdc(size_usdc)`, and removed the
`entry_shares_for_buy`/`ceil2`/$1-floor-bump code that existed only to serve the broken path.
The exit-leg dust this reintroduces is already handled safely — `worker.rs`'s
`MIN_SELLABLE_SHARES` write-off (from the same incident chain, implemented *before* this
regression) already detects a residual below the sellable floor and finalizes the trade off
realized proceeds instead of chasing an unfillable sell, so nothing needed to change there.
Verified with `cargo test` (132 passed) and a clean redeploy to Oracle
(`trader-live.service` restarted 23:48:29 HKT, healthy).

**Lesson:** the two `Amount` constructors aren't interchangeable ways to size the same order
— which one is "raw" (caller-supplied, therefore safely-scaled) and which is "derived"
(computed by multiplying by price, therefore only as precise as that multiplication allows)
flips depending on which leg you pick, and Polymarket enforces different decimal-precision
caps on each leg of a market BUY specifically. A fix that only checked the *exit* side's
already-known constraints (`Amount::shares` caps at 2 decimals) missed a *different*,
previously-undocumented constraint on the *entry* side's maker amount — test the two legs of
an order against the API's actual rules independently, not just the one already bitten by a
prior incident.

### BUY retry ladder stalled short of `max_buy_price` (2026-07-03, fixed)

A DOGE BUY at 16:23 (cycle `doge-updown-5m-1783066800`) retried 4 times
(`order_max_retries=3` from `strategy_*.toml`) and failed all 4, every attempt hitting
`"no orders found to match with FAK order"`. Full writeup, including cross-referencing
the recorded order book to confirm this was a real thin-liquidity moment and not a
pricing bug: `trader/doc/audit_retry_doge_2026-07-03.md`.

**Root cause:** the price offered on each retry was `price + order_slippage + attempt *
retry_slippage_step`, where `retry_slippage_step` was a **hardcoded 0.02** in
`execution.rs::LiveConfig::default()` — unlike `order_slippage`/`order_max_retries`, it
was never actually sourced from `strategy_*.toml`. So the 4 attempts crept up only 2¢
each (0.795 → 0.815 → 0.835 → 0.855) while `max_buy_price = 0.95` (also from config)
had another 9.5¢ of headroom that was never used.

**How the BUY retry ladder works now** (`execution.rs::retry_ladder_price`): each retry
price is linearly interpolated from the first attempt (`price + order_slippage`) up to
`max_buy_price`, so the **last** retry always lands exactly on the configured ceiling —
no new config field, `max_buy_price` already is that per-run limit and is still
enforced via `.min(max_buy_price)`. With the incident's numbers (price 0.745,
order_slippage 0.05, max_buy_price 0.95, order_max_retries 3): 0.795 → 0.847 → 0.898 →
0.95 — attempt 4 now reaches the ceiling instead of stopping short of it.

This is safe to be aggressive about: the BUY order is a USDC-notional market order
(`Amount::usdc(size_usdc)`), so `price` is only a worst-case ceiling — the actual fill
price (`cost = size_usdc / filled_shares`) is always the real weighted price from
whatever liquidity the book had. Raising the ceiling faster costs nothing when the book
doesn't need it; it only stops retries from failing purely because the cap was still
below available liquidity. `TradeResult` also now carries an `attempts` count, surfaced
in the Telegram "Order REJECTED" message, so a repeat of this is visible without
grepping `live.log`.

**Superseded 2026-07-04** by an even more aggressive scheme
(`execution.rs::aggressive_entry_price`), by request: the first attempt no longer uses
`price + order_slippage` — it splits the difference between `price` and `max_buy_price`
(half the spread), and **every retry after the first jumps straight to
`max_buy_price`** instead of interpolating gradually. `order_slippage` is gone (removed
from `LiveConfig`/`strategy_*.toml` schema — the interpolated approach it fed no longer
exists). Same incident's numbers under the new scheme: price 0.745, max_buy_price 0.95 →
first attempt 0.8475 (half the 0.205 spread), any retry 0.95 immediately — reaches the
ceiling on the very first retry instead of the fourth.

### Take-profit never filled — oversell bug + no retry backoff (2026-07-03, fixed)

A DOGE take-profit at 17:33 crossed its trigger almost immediately after entry and
stayed crossed for the rest of the cycle, yet **284 close attempts all failed** and the
position rode to resolution (won by luck). Full writeup:
`trader/doc/incident_doge_2026-07-03.md`.

**Root cause 1 — a real oversell, not a liquidity problem:** `close_position` built the
SELL order size as `round2(shares)`. `round2(1.5151)` rounds **up** to `1.52`, but the
position only held `1.515150` shares — the close order asked to sell more than it
actually owned, which can never succeed no matter how many retries or how liquid the
book is (`"not enough balance -> balance: 1515150, order amount: 1520000"` — an exact
match for `round2(1.5151)` vs. the true balance). Fixed by adding `floor2` (truncate,
never round up) and using it for both SELL-size call sites (`place_limit_sell`,
`close_position`), matching the reference `py_clob_client_v2`'s own `round_down`
size-quantization — the Rust SDK doesn't quantize internally the way the Python client
does, so the caller has to.

**Root cause 2 — no backoff on the take-profit retry loop:** independent of the
oversell, `worker.rs::on_poly` re-fired a brand-new close attempt on *every* `PolyTick`
while price stayed above the take-profit level, because a failed attempt reverted
straight back to the same re-triggerable `PriceMonitor` arm — 284 attempts in ~9
seconds, no cooldown. The Python bot this ports from (`bot/worker.py`) doesn't retry
this way at all: it zeroes the trigger the moment it fires, calls the close exactly
once (with its own bounded 5-retry/1s-backoff loop), and just accepts the loss of that
exit opportunity if it fails — no per-tick hammering. Rust now matches: a new
`ExitArm::TakeProfitAbandoned` latch is set on failure so the take-profit condition
can't re-fire for that position again, while stop-loss (which doesn't gate on
`exit_arm`) stays fully armed regardless.

**Also fixed while investigating:** the live trade CSVs' header predated the
`exit_attempts`/`exit_last_error` columns (9 columns vs. the 11 the binary actually
writes). `csv.DictReader` (used by `trade_reconcile.py`) doesn't error on that mismatch
— it silently dumps the extra fields into an unnamed bucket, so the "Failed Exit
Attempts" report had been reporting zero retries for every trade, always, since that
feature was added. `append_csv_header_if_new` now detects and heals a stale header
in place (padding any legacy short rows) on the next restart, and
`trade_reconcile.py` warns loudly instead of silently zeroing data if it ever sees the
mismatch again.

### `/halt` silently cleared within one cycle (2026-07-03, fixed, critical)

`/halt` was sent via Telegram at 17:36 HKT to stop new entries on the live (real-money)
bot. It placed a new ETH trade at 18:09 anyway, as if `/halt` had never been sent. Full
writeup: `trader/doc/incident_halt_reset_2026-07-03.md`.

**Root cause:** `bin/live.rs`'s one real call site for `Event::CycleOpen` (fired every
~5 min, once per asset/strategy, on every new market cycle) hardcoded
`entry_suppressed: false`. `worker.rs::on_cycle_open` then did an unconditional
`self.entry_suppressed = entry_suppressed` — silently resetting *any* active halt back
to `false` at the very next cycle boundary, with no log line or notification. `/halt`
therefore only suppressed entries for up to ~5 minutes before trading silently resumed.
This has been broken since the halt feature was built; the 5-minute cadence just made
it look like it worked if checked immediately after sending the command. `/status`
would also have shown "🟢 active" right after the silent reset, so even checking status
soon after `/halt` wouldn't have caught it.

**Fix:** removed `entry_suppressed` from `Event::CycleOpen` entirely rather than just
correcting the call site's value — `entry_suppressed` was never part of
`PersistedState`, so it only ever legitimately changes via `Event::Control(Halt/Resume)`
or `Event::Balance(DrawdownHalt)`; a `CycleOpen` parameter had no valid use and closing
it off structurally means no future call site can reintroduce this by passing the wrong
value. (The backtest engine's `machine.rs::Machine::cycle_open` has its own similar
parameter but computes it correctly each cycle from its loss-streak tracker — a
different, correctly-implemented mechanism, unaffected by this bug.) Added
`halt_survives_multiple_cycle_boundaries`: halts, drives 5 consecutive `CycleOpen`
events, asserts the halt holds through all of them, then confirms `/resume` still
clears it.

### ETH `high_prob` went dark for 40+ minutes, missing a trade (2026-07-03, fixed)

The Python bot took a WIN trade on ETH `high_prob` at 16:59:42; the Rust bot logged
nothing for that cycle — no entry, no skip. Full writeup:
`trader/doc/incident_missed_eth_2026-07-03.md`, fix plan:
`trader/doc/plan_fix_max_trade_guard.md`.

**Root cause:** `bin/live.rs`'s `AssetSlot.trades_completed` counted trades for the
*entire process lifetime* and never reset, and the per-tick cycle-open gate refused to
open a new cycle once a slot's lifetime total reached `--max-trades` (deployed as `1`).
ETH `high_prob` won its one allotted trade at 16:30–16:35 and then permanently stopped
opening new cycles for the rest of that process's life — 40+ minutes, spanning the
16:55–17:00 cycle the Python bot traded — while its sibling ETH `reversal` slot (a
separate `AssetSlot`, unaffected) kept ticking normally the whole time. The process only
self-terminated once *every* slot independently reached its own cap, so nothing forced a
restart to re-arm it; it happened to resume when an unrelated external SIGTERM (routine
redeploy) restarted the process and zeroed every slot's counter.

**Fix:** `trades_completed` → `cycle_trades`, reset to `0` every time a new cycle opens
for that slot — `--max-trades` is now "trades allowed per open cycle" (still 1 by
default), never a lifetime total, so no slot can go permanently dark. The "all slots
reached max_trades → shut down" block was removed outright rather than reworked — a
per-cycle-resetting counter has no meaningful "done forever" state, and a
`Restart=always` production service shouldn't be exiting itself over trade counts
regardless.

### Consecutive-loss halt (`halt_rev`/`halt_prob`) was parsed but never wired up (2026-07-03, fixed)

`strategy_*.toml`'s `halt_rev`/`halt_prob` (halt after N consecutive losses) and
`halt_reset_hour_rev`/`halt_reset_hour_hp` (daily HKT reset hour) were read into
`AssetParams` and shown in `/status`, but **nothing in the live trading path ever
consumed them** — `entry_suppressed` was only ever set by `/halt` or the balance
drawdown guard. `backtest.rs` already had a correct, tested implementation
(`HaltTracker`) that the live binary simply never used, so this config had zero effect
on real trading despite looking active in `/status`.

**Fix:** made `HaltTracker`/`hkt_session` `pub(crate)` in `backtest.rs` and gave
`Worker` its own instance (constructed per-strategy from `halt_rev`/`halt_reset_hour_rev`
or `halt_prob`/`halt_reset_hour_hp`), reset at the configured HKT hour on every
`CycleOpen`, updated on every logged trade, and OR'd into both the entry gate and
`is_halted()` (so `/status`'s "🟡 halted" now reflects this too). New test:
`halt_by_loss_streak_suppresses_entry_and_resets_next_session`. Not persisted across a
process restart — `bin/live.rs` doesn't reload any persisted state on startup at all,
a separate pre-existing gap this fix doesn't touch.

### Telegram pnl showed -$0.9964 on a WIN (2026-07-03, fixed)

`✅ ETH TRADE WIN | entry=0.8900 → exit=1.0000 | pnl=-$0.9964` — a win reporting pnl
near *negative* the whole stake. **Root cause:** every terminal pnl calculation
(`on_cycle_close`, and the full-close branches of `on_unwind_filled`/
`on_stop_sell_filled`) computed `shares * exit_price - trade_size` — subtracting the
*nominal* configured trade size rather than the actual cost basis of the shares being
settled. That's only correct when `shares == trade_size / token_price` exactly; the
moment an earlier *partial* take-profit/stop-loss fill reduced `h.shares` to a small
residual (the partial-fill branches discarded that sale's proceeds entirely), the
formula settled the tiny leftover residual against the *full* original stake. Verified
against `../btc_5mins/bot/worker.py`'s reference formula (`shares * (1.0 - cost)` /
`-shares * cost`), which correctly scales to whatever shares/cost are actually being
settled.

**Fix:** added `HoldingData.realized_pnl` (dollars already locked in from an earlier
partial fill, accumulated on every partial-fill branch) and unified every terminal site
onto `settle_pnl(h, exit_price) = h.realized_pnl + h.shares * (exit_price -
h.token_price)`. New test: `partial_unwind_then_cycle_close_totals_both_legs_pnl` (6-of-10
shares sold at a profit, 4-share residual resolves at cycle close, asserts the total
equals the by-hand arithmetic). `on_api_result`'s API-flip branch is unaffected — it
already recomputes `shares` fresh from `trade_size`/`token_price` each time, which is
self-consistent for its own formula (though still can't reflect a genuine partial-fill
residual, since `TradeRecord` doesn't carry a `shares` field — out of scope here, would
ripple into the CSV schema).

### Deploy script raced systemd's `Restart=always`, ran two live traders at once (2026-07-03, fixed, critical)

`scripts/deploy_oracle.py` managed the trader process directly via `pgrep`/`kill`/
`tmux new-session`, written before Oracle had a `trader-live.service` systemd unit
(`Restart=always`, installed 2026-07-03 16:09) supervising it. A deploy's `kill -TERM`
on the old PID looked like an unexpected crash to systemd, which immediately
auto-respawned it per `Restart=always` — and the deploy script then *also* started its
own copy via `tmux`. **Two independent `live` processes ended up running concurrently
against the same real-money account for ~16 minutes**, both subscribed to the same NATS
feed and capable of independently firing entries/exits on the same signals. Caught via
the visible symptom: both processes long-polling Telegram `getUpdates` with the same bot
token produced repeated `[telegram] poll error: ... missing field \`result\`` (a 409
Conflict Telegram returns when two pollers share a token) in the log. No duplicate
orders happened to fire in that window (neither process hit an entry signal), but
nothing structurally prevented it.

**Fix:** `deploy_oracle.py`'s trader path now only ever goes through
`sudo systemctl restart trader-live.service` — no `kill`, no `tmux`, ever. It also
regenerates `/etc/systemd/system/trader-live.service`'s `ExecStart` from the same
`TRADER_ASSETS` (latest `strategy_*.toml`'s `trade_assets`) it always computed, so the
installed unit can't silently drift from config either. `scripts/deploy_trader.sh` (the
trader-only wrapper — see "Deploy the trader only" above) picked up the fix
automatically since it just calls into `deploy_oracle.py --trader-only`.

**Lesson:** once *anything* is under `Restart=always` supervision (systemd, or
otherwise), all future tooling touching that process must go through the supervisor's
own restart command — never signal the process directly, even for a "graceful" SIGTERM.
The supervisor can't tell a deliberate redeploy apart from a crash.

### Stop-loss close never filled (2026-07-02, fixed)

A live BNB test (`trader/src/bin/live.rs`, size $1, max-trades 1) bought 1.0752 shares of "Up"
for $0.9999, the stop-loss triggered, and **every single close retry failed** for the rest of the
run (hundreds of retries, `status=Failed sold=0.0000`). The position was never exited and rode to
market resolution; "Up" lost, so the position settled to $0. **Total loss: $0.9999** (confirmed via
Polymarket's public `data-api.polymarket.com/positions` endpoint — `currentValue: 0` on
`bnb-updown-5m-1782971400`).

**Root cause:** `execution.rs::close_position()` built the market SELL order as
`.amount(Amount::usdc(size_dec))`, where `size_dec` was the **held share count** (1.0753), not a
USDC amount. The SDK has two distinct constructors, `Amount::usdc()` and `Amount::shares()`.
Wrapping a share count in `Amount::usdc` tells the exchange "I want ~$1.0753 in proceeds", which at
a <$1 price requires selling *more* shares than are actually held — so the order could never
match. Every retry hit `"no orders found to match with FAK order"` / `"not enough balance"`, which
the retry loop treated as transient and retried forever instead of surfacing as a real error. The
retry logic explicitly listing `"not enough balance"` as retryable is a strong sign this exact
failure had been seen before and papered over with retries rather than fixed.

**Fix:** use `Amount::shares(size_dec)` instead, matching `place_limit_sell`'s existing correct
pattern (`round2(shares)` → 2-decimal `Decimal`, since `Amount::shares` enforces `LOT_SIZE_SCALE=2`
— unlike `Amount::usdc` which allows more decimal places, so the old 4-decimal formatting would
have failed validation immediately if this had been caught locally instead of live). Verified with
`cargo test --lib execution` (all 7 tests pass) after the change.

**Lesson:** any future live/shadow test should watch for repeated `[close] retry` log lines as a
red flag — that pattern means the close is structurally broken, not just hitting temporary
liquidity, and the position will ride uncontrolled to market resolution. (Log prefix renamed from
`[SL close]` — this retry path is shared by stop-loss *and* take-profit closes, and the old label
was misleading; see the DOGE take-profit incident below.)

</details>

<details>
<summary><strong>Order sizing: limit (GTC) vs market (FAK), by trade size</strong></summary>

## Order sizing: limit (GTC) vs market (FAK), by trade size

Polymarket enforces two independent, differently-denominated minimum order sizes (no single
official page states both together; pieced together from `docs.polymarket.com`'s
`INVALID_ORDER_MIN_SIZE` error code, the CLOB orderbook response's own `min_order_size` field —
present in the vendored SDK as `clob::types::response::OrderBookSummary::min_order_size` — and
this repo's own production history):

- **A resting GTC/GTD limit order must be for at least 5 shares.** Below that, Polymarket
  rejects it outright — this isn't a preference, it's illegal to even attempt. `../btc_5mins`
  (the reference Python bot this Rust trader ports) hit and documented this directly: "Polymarket
  CLOB enforces a hard 5-token minimum for all resting (GTC) SELL orders. At $1 stake / 0.80–0.95
  token price the fill is 1.05–1.25 tokens, always below 5, so the GTC path always fails at
  typical live stakes" (`../btc_5mins/README.md`'s stop-loss/unwind section).
- **A marketable FAK/FOK order has no share-count floor**, only a **$1 USDC notional floor**
  (`docs.polymarket.com`'s `INVALID_ORDER_MIN_SIZE`; hit and fixed here in
  `incident_order_fail_2026-07-04.md`).

At this bot's current $1 stake and typical 0.80–0.95 entry prices, every position is 1.05–1.5
shares — always under the 5-share GTC floor — so the exit path always takes FAK, either as a
bounded `close_position_at_price` (take-profit) or unbounded `close_position` (stop-loss); see
the incident above. **Raising the stake to $5+ crosses the GTC floor at these same prices**
(5 shares × ~$0.90–1.00 ≈ $4.50–5.00), which is likely the source of "$5 minimum" as a rule of
thumb even though the actual exchange constraint is share-denominated, not dollar-denominated.
`worker.rs::on_order_filled` already had this branch (`filled_shares >= 5.0` → attempt a
resting GTC via `Action::PlaceLimitSell`, matching `../btc_5mins`'s hybrid path and its
`UnwindWatcher`-based fill notification — both now ported here, see the latency section above)
— today's change only centralized the threshold into a named, tested, documented function
(`execution::choose_exit_order_kind`, `execution::MIN_GTC_SHARES`/`MIN_MARKETABLE_USDC`) instead
of an inline magic number, so it's exercised automatically and correctly at any stake size,
not just today's $1.

**Entry (BUY) intentionally does not have this same choice** — it always uses a marketable FAK
(`execution.rs::place`, limit-priced up to `max_buy_price`), regardless of stake size. This is a
strategy design choice, not a size limitation: the reversal/high_prob strategies react to a
live price crossing a trigger band and need to grab the current price immediately — resting a
GTC buy would risk missing the entry window entirely if price moves away before a passive limit
fills. `../btc_5mins` makes the same choice (`TradingEngine.place()` is always a market order for
entries).

</details>
