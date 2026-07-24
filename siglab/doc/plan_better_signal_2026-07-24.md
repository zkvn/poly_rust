# Overhauling siglab: daily backtest-QC filtering + a 9am signal digest

**Status: Phase 0 done, git-push behavior fixed; Phases 1-3 (the digest itself) still
plan-only.** Date: 2026-07-24, updated same day.

**Done today:**
- Phase 0 — World Cup removed (`worldcup.rs`, its config, all wiring/report sections/README
  mentions); `MarketKind::Worldcup` kept historical-only so old JSONL/report data still
  deserializes. `cargo build`/`fmt`/`clippy -D warnings`/`test` all clean (64 tests), plus a
  manual `--regenerate-reports-only` run over a synthetic old-format (worldcup-tagged) trade
  log confirmed it still regenerates correctly.
- **Interim reports stop pushing to git, immediately** — not deferred to Phase 3 as originally
  scoped below. `scripts/push_report.sh`'s glob narrowed from `doc/report/*/*.md` to
  `doc/report/*/digest_*.md` (+ `candidate_ledger.csv` once it exists); verified live via
  `systemctl --user start siglab-report-push.service` — correctly logs "no digest files exist
  yet — nothing to push" instead of committing the hourly files. `summary_{date}.md`/
  `trades_{date}_{HH}.md` are still written locally every 15 min (useful for debugging) but no
  longer touch git; repo-root `.gitignore` covers new dated folders going forward. This reuses
  the *existing*, already-SSH-agent-debugged `siglab-report-push.timer` rather than standing up
  a new one — simpler than §4.8's original draft below, which proposed a second timer just for
  pushing.
- Repo-root `README.md` TODO gained two entries: the live Docker container still needs a
  rebuild/redeploy to pick up the Phase 0 binary change (not restarted automatically — same
  "don't restart a live process without explicit go-ahead" precedent as `gamma_recorder`), and
  the digest generator itself (Phases 1-3 below) is still unbuilt.

**Not done yet**: the actual QC pipeline (§4.2-4.6) and the digest generator that produces
`digest_{date}.md` — that's still real, unstarted work. §4.7 below now also specs a
markets-monitored table + brief 24h stats, added per explicit request when this plan was
approved.

## 0. Summary

Three changes, in order:

1. **Remove World Cup entirely.** The tournament is over; `worldcup.rs`, its config,
   and its wiring are dead weight in every report and every future stats run.
2. **Close the loop siglab has never had**: apply the PBO/DSR/null-win-rate toolkit
   `../btc_5mins` built for its own backtest sweeps (`doc/best_practice_backtest_2026-07-21.md`)
   to siglab's own **live** paper-trading log, once a day, to turn "18 reversal variants
   ran today" into "these 2 variants are statistically distinguishable from noise; the
   rest aren't yet." A small dated ledger tracks verdicts over time so one lucky day
   can't get a variant promoted.
3. **Replace the primary human-facing artifact.** Today a human has to open a
   15-minute-cadence report with 18+16 variant tables × 6 assets × 4 durations × 51
   cities to find anything. A new report — `digest_{date}.md`, generated once, at
   9am HKT — leads with a plain-English bottom line and a short recommendations
   table; the granular tables move to a collapsed section at the bottom. The
   existing hourly reports keep existing as raw backing data, they just stop being
   the thing anyone reads first.

## 1. Relationship to the two docs this follows on from

This is the doc you were looking for last time and hadn't written yet — the "apply
PBO to siglab" follow-through.

- **`../btc_5mins/doc/plan_signal_lab_2026-07-19.md`** — the strategic S0→S5 signal
  pipeline (S0 registry → S1 signal test → ... → S5 live canary), and where
  PBO/DSR/CPCV were first scoped for either repo. It already names `poly_rust` at
  S4 (shadow/paper mode, judged by a Wald SPRT). siglab is, functionally, already
  running something close to S1/S2 continuously — many parameter variants live-tested
  against real ticks — it just never closes the loop with a verdict.
- **`../btc_5mins/doc/best_practice_backtest_2026-07-21.md`** — the concrete toolkit
  (`bot/backtest_stats.py`: `null_win_rate_barrier`/`null_win_rate_market_implied`,
  `daily_pnl_panel`, `pbo_cscv`, `deflated_sharpe_ratio`, `rf_param_importance`),
  verified against real bt2 sweep output but scoped to **backtest** sweeps in that
  repo (bt1-4, `studies/0dte`) — still Phase 0 only there, nothing wired in.

This plan is the third leg: **apply the same math to siglab's live paper-trading
data**, not a backtest sweep. The core insight both docs already established still
holds — every day siglab runs is one more row in a `T days × N variants` PnL panel,
exactly the input `pbo_cscv`/`deflated_sharpe_ratio` need, except the trades are real
CLOB/Binance ticks instead of a replayed backtest.

## 2. Current state — what exists, what's missing

siglab (`src/main.rs` + `rotation.rs`/`market.rs`/`bucket_reversal.rs`/`v_shape.rs`)
already does the hard part: it live-tests an 18-variant `reversal` grid and a
16-variant `v_shape` grid against real Polymarket/Binance ticks, across 6 crypto
assets (4 durations each) and 51 weather cities (soon-to-be-former 62 World Cup
events too), continuously, unattended, since 2026-07-13. Every trade is logged to
`SiglabTradeRecord` JSONL (`record.rs`) with `market`/`strategy`/`variant_id`/
`outcome`/`pnl` — everything `daily_pnl_panel` needs is already there, just never
aggregated across days.

What's missing:

- **No statistical verdict, ever.** `report.rs` renders activity (trade tables,
  win rate vs. an implicit and mostly-wrong 50%, market state, staleness, CPU) —
  never "is this variant's win rate better than its own SL/TP-implied null,"
  never "is the best-of-18 variant today distinguishable from the best-of-18-noise-
  columns case," never "which of the 2 swept dimensions actually drives PnL."
- **No cross-day memory.** Every write recomputes fresh from the trade log
  (`report.rs`'s own convention), which is correct for activity reporting but means
  nothing currently asks "has this variant looked good for 3 days running, or is
  today a fluke."
- **The report is the wrong shape to read daily.** `doc/report/2026-07-24/summary_2026-07-24.md`
  today is ~370+ lines before a single trade table — full 18-row reversal grid, full
  16-row v_shape grid, 51-row weather city list, 62-row (soon 0-row) World Cup event
  list — repeated at the top of every hour's file too. There's no "read this first"
  section; a human has to scan every table to find what changed.
- **World Cup is dead weight now.** The tournament is over — `worldcup.rs`,
  `config/worldcup_events.toml`, and every report/config-table code path that renders
  62 event slugs are testing and reporting on nothing.

## 3. Phase 0 — remove World Cup (mechanical, do first)

Delete outright:
- `src/worldcup.rs`
- `config/worldcup_events.toml`

Remove wiring (all in `src/main.rs`): the `mod worldcup;` declaration, the
`--worldcup-config`/`--worldcup-refresh-secs` CLI args, `config::load_worldcup`,
the per-event `tokio::spawn(worldcup::run_event_supervisor_for(...))` loop, and
every `worldcup_cfg.events`/`worldcup_events` argument threaded into report calls.

Remove rendering (all in `src/report.rs`): the World Cup event-count line, the
`kind == "worldcup"` staleness-snapshot filter, `render_config_section`'s World Cup
events table and its `worldcup_events: &[String]` parameter, and every call site
that threads a `worldcup_events` argument through `render_summary_body`/
`regenerate_from_trade_log`/`write_hourly_report`/etc.

**One thing NOT to delete: `MarketKind::Worldcup` itself, kept as historical-only.**
`regenerate_from_trade_log`/`regenerate_summaries_from_trade_log` re-read the *entire*
JSONL history to rebuild reports, including every World-Cup trade already logged
before this change — deleting the enum variant would break deserialization of that
real historical data (no `#[serde(other)]` fallback today, and adding one would
silently coerce future genuine World Cup config typos to Crypto instead of failing
loudly). Keep the variant, doc-comment it `// historical only — no new World Cup
trades are produced as of 2026-07-24`, and let old report days stay exactly as they
already are (not retroactively rewritten).

Also touch: `docker-compose.yml`'s `command:` list (drop the `--worldcup-config` pair),
`README.md` (Quickstart command, "What it does" bullet, Config files section, Layout
tree — all currently say "weather and World Cup").

Verify: `cargo build` / `cargo fmt --all --check` / `cargo clippy --all-targets
--all-features -- -D warnings` / `cargo test` all clean, plus a local
`--regenerate-reports-only` run over existing history to confirm old World-Cup-bearing
days still render without error.

## 4. Phase 1 — the daily statistical QC pass

### 4.1 What's a "combo" here

Same shape as a bt2 sweep grid, mapped onto siglab's existing fields: for a given
`(market, strategy)` — e.g. `("BTC-5m", "reversal")` or `("weather:hong-kong",
"v_shape")` — the swept dimension is the variant grid itself (18 `reversal_id`s or
16 `v_...` ids, per `record.rs`'s existing `variant_id` field). A **combo** =
`(market, strategy, variant_id)`. Panel rows = trading day (HKT calendar day, same
fold unit `best_practice_backtest_2026-07-21.md` already uses); panel columns = one
combo; cell = that day's summed PnL for that combo (a day with zero trades is a
true zero, not missing — `daily_pnl_panel`'s existing convention).

Crypto and weather get judged **separately** — weather's `bucket_reversal`/`v_shape`
engines never touch `Machine::cycle_close`, so they never produce a real WIN/LOSS,
only STOPLOSS/UNWIND/TIMEOUT (`record.rs`'s own doc comment, `MarketKind::Weather`).
That changes which null-hypothesis applies (below) and mixing the two market classes
into one PBO panel would compare apples to oranges anyway.

### 4.2 Toolkit: ported, not imported

siglab's README states its design principle plainly: "fully standalone from `../trader`
and `../price_feed`." The same discipline should hold for `../btc_5mins`, a *different
repo* with its own venv — a runtime cross-repo Python import is fragile (path
assumptions, dependency drift) and not what "standalone" means here.

Instead: port the four functions siglab actually needs
(`null_win_rate_barrier`, `null_win_rate_market_implied`, `daily_pnl_panel`,
`pbo_cscv`, `deflated_sharpe_ratio`; `rf_param_importance` too, it's cheap and
directly answers "does `reversal_low_threshold` or `reversal` drive PnL more") into
a new file, **`poly_rust/analysis/siglab_backtest_stats.py`** — matching this repo's
own existing convention (`CLAUDE.md`'s Studies section: "Analysis scripts (Python,
standalone) live in `analysis/`"). Credit the source (`../btc_5mins/bot/backtest_stats.py`,
Bailey/Borwein/López de Prado/Zhu 2015 for CSCV, Bailey & López de Prado 2014 for DSR)
in the module docstring. The only real adaptation needed is `daily_pnl_panel`'s input
shape — the original consumes `run_sweep`'s per-combo trades DataFrames; siglab's
version groups directly from `SiglabTradeRecord` JSONL rows by `(market, strategy,
variant_id)` and HKT day instead.

### 4.3 Which null applies to which outcome

Directly reusing `best_practice_backtest_2026-07-21.md` §2's finding, mapped onto
siglab's own outcome vocabulary (`trader::types::Outcome`, `record.rs`):

- **STOPLOSS/UNWIND** (both market classes, both strategies): barrier null,
  `null_win_rate = sl_pnl / (sl_pnl + unwind_pnl)` — exact for `reversal`
  (`sl_pnl_rev`/`unwind_pnl_rev`) and `v_shape` (`sl_pnl`/`unwind_pnl`) alike, both
  literal fractional-PnL TP/SL thresholds.
- **WIN/LOSS** (crypto `reversal` only — the only path that ever calls
  `Machine::cycle_close`): market-implied null, `null_win_rate = mean(entry price
  of the side bought)`.
- **TIMEOUT**: no clean null exists (same gap `best_practice_backtest_2026-07-21.md`
  flags for its own repo) — report the TIMEOUT share of outcomes as a caveat line,
  don't force a null onto it.

### 4.4 Warm-up: PBO needs history siglab doesn't have yet

CSCV's canonical `S=16` blocks need 16 trading days minimum. siglab started
2026-07-13 — as of today (2026-07-24) that's 11 days, short of a full run. Start at
**`S=8`** (needs only 8 days, `C(8,4)=70` splits, still statistically sane per the
btc_5mins POC's own sensitivity check) and raise to `S=16` once ~20 days of history
exists. Below `S=8`'s minimum, the digest's PBO/DSR section renders "insufficient
history — descriptive only, N=<k> days" instead of a number — showing a
confidently-wrong PBO computed on 3 days of data would be worse than admitting it
isn't ready yet.

### 4.5 Verdict rubric

Per combo, per day, compute: trade count, realized win rate, applicable null win
rate, edge (realized − null), DSR z/p (deflating for the 18-or-16-variant trial
count within that combo's `(market, strategy)` group), PBO (once warm-up clears).
Three-tier verdict:

| Verdict | Bar |
|---|---|
| **PROMOTE-CANDIDATE** | ≥20 trades in the trailing window, DSR p < 0.05, PBO < 0.5 (once computable), edge > 0 |
| **WATCH** | some positive signal but fails one bar (thin sample, borderline DSR, or PBO not yet computable) |
| **REJECT** | DSR fails outright, or PBO ≥ 0.5 (worse than a coin flip out-of-sample) |

`< 0.5` for PBO, not a stricter bound — the btc_5mins POC's own finding (§4:
"a single draw ... is itself noisy, ranging ~0.2-0.9") means a one-day PBO reading
is not trustworthy in isolation, which is exactly why §4.6's ledger requires a
*streak*, not a single day's verdict, before a recommendation gets any real weight.

**Persist every day's verdicts** to `siglab/doc/report/candidate_ledger.csv`
(columns: `date, market, strategy, variant_id, trades, win_rate, null_win_rate, edge,
dsr_z, dsr_p, pbo, verdict`), appended (not rewritten) daily. This is what makes the
system resistant to a single lucky day: the digest's recommendations section should
surface a combo prominently only once it's logged **PROMOTE-CANDIDATE on 3+
consecutive days**, tracked by a simple streak-count over the ledger's own history —
no separate state needed, it's derivable from the CSV each run.

### 4.6 Where this runs, and the one infra gap

New script, `poly_rust/analysis/siglab_daily_digest.py` — reads
`siglab_trades.jsonl` directly, no Rust changes needed for the stats layer itself
(keeps `analysis/`'s existing "Python, standalone, opt-in" shape, and keeps siglab's
Rust surface focused on live paper-trading rather than growing a `pandas`/`scipy`/
`scikit-learn` dependency it doesn't otherwise need).

**Infra gap**: the trade log lives in `siglab_logs`, a Docker **named volume**, not
bind-mounted to a host path — only `doc/report/` is (`docker-compose.yml`). The
`reversal_high_low` study in `../btc_5mins` already worked around this once with a
manual `docker cp siglab-siglab-1:/app/logs/siglab_trades.jsonl ...`; that's fine for
a one-off study, not for something that has to run unattended every morning. Fix:
bind-mount `./logs` straight into the git working tree the same way `doc/report`
already is, replacing the named volume (nothing else depends on `siglab_logs` being
Docker-managed specifically) — the digest script then reads
`siglab/logs/siglab_trades.jsonl` directly off disk, no container exec required.
One-line `docker-compose.yml` change, zero risk to the write path (still append-only
from the container's side), and it makes `docker cp` unnecessary for any future
ad-hoc analysis too. `.gitignore` needs `siglab/logs/` added — this is operational
data, not meant for git, same reasoning the named volume originally had.

### 4.7 The 9am digest itself

New file per day: `siglab/doc/report/{date}/digest_{date}.md`. Bottom-up, exactly as
asked, four sections:

1. **Bottom line** — 2-4 plain-English sentences: how many combos are at
   PROMOTE-CANDIDATE today (and their streak length), any that flipped to REJECT,
   one-line total PnL/win-rate context. This is the only thing that has to be read
   to know "did anything change."
2. **Recommendations** — one small table, PROMOTE-CANDIDATE and WATCH combos only
   (REJECT and insufficient-sample combos are noise here, not the point) — market,
   strategy, variant, streak days, edge, DSR p, PBO. Realistically well under 10
   rows even at full grid size (18+16 variants × 6 assets × 51 cities is thousands
   of combos, but almost all of them will be REJECT/insufficient-sample on any given
   day — that's the filtering doing its job).
3. **Markets monitored** — added per explicit request when this plan was approved,
   2026-07-24: one small table, one row per market actually rotating that day (every
   crypto `(asset, duration)` pair from `config/markets.toml`/`hourly_market`, every
   active weather city-bucket), with brief trailing-24h stats per row — trade count,
   win rate, total PnL, last-tick age (staleness). This is descriptive coverage/health,
   not a verdict (that's section 2's job) — it answers "is everything actually still
   running and ticking," which the verdict tables alone don't show for a market that's
   gone quiet with zero trades. Sits between the recommendations and the collapsed
   detail — short enough to stay visible (under 60 rows: ~18 crypto markets + weather
   cities that had bucket activity, not the full static 51-city list), long enough that
   it earns its own `<details>` if it grows past a screenful.
4. **Supporting detail**, collapsed `<details>` sections (mirrors `report.rs`'s
   existing collapsible convention) — full per-group DSR/PBO numbers, RF
   permutation-importance ranking (which of `reversal_low_threshold`/`reversal`, or
   `high2`/`sl_pnl`/`unwind_pnl` for v_shape, actually drives PnL), sample-size and
   warm-up caveats, TIMEOUT-share note.

The existing `summary_{date}.md`/`trades_{date}_{HH}.md` files keep being written
exactly as today — they're the raw activity/audit trail the digest is computed
*from*, not replaced by it, and (as of 2026-07-24, done — see the status block up top)
they're local-only now, not pushed. The digest is a new, additional artifact; nothing
about the 15-minute in-container report-writing cadence changes in this phase.

### 4.8 Scheduling

Two separate concerns, not one:

- **Generating** the digest needs a new systemd `--user` timer,
  `siglab-daily-digest.timer`:

  ```ini
  [Timer]
  OnCalendar=*-*-* 08:45:00
  Persistent=true
  ```

  08:45 HKT gives a 15-minute buffer before 9am for the run (PBO's `C(S, S/2)` split
  enumeration is the slow part; trivial at `S=8`, worth re-timing once raised to
  `S=16`). This still needs building — not done today.

- **Pushing** the digest once it exists needs no new timer — **already done today**
  (see the status block up top): `push_report.sh`'s glob was narrowed to
  `digest_*.md`/`candidate_ledger.csv` and verified live via a manual
  `systemctl --user start siglab-report-push.service` run. The existing
  `siglab-report-push.timer` (every 15 min, already carrying `install_timer.sh`'s
  `SSH_AUTH_SOCK`-injection fix) picks up a new digest within 15 minutes of it being
  written — no second SSH-agent gotcha to debug again.

## 5. What this plan is not proposing

- **Not auto-promoting anything into `trader`'s live config.** Verdicts are a
  recommendation surfaced to a human each morning, same "no new mandatory gate"
  principle `best_practice_backtest_2026-07-21.md` already committed to for its own
  repo. A PROMOTE-CANDIDATE streak is a strong hint to go look, not a trigger.
- **Not changing the 18/16-variant grid sizes.** Whether the grids should be
  narrower or wider is a separate design question from "how do we judge what's
  already being tested."
- **Not touching the 15-minute hourly report's cadence or content.** It keeps being
  the raw backing data the digest is computed from. Worth revisiting once the digest
  has been running for a while and it's clear how much of the hourly detail anyone
  still actually reads — not decided here.
- **Not adding purge/embargo logic.** Same judgment call `best_practice_backtest_2026-07-21.md`
  §3 made for `reversal_hourly`: siglab's swept dimensions (`reversal_low_threshold`/
  `reversal`, v_shape's `high2`/`sl_pnl`/`unwind_pnl`) are all per-cycle price-threshold/
  PnL-fraction mechanics, no rolling-window/cross-cycle state — plain contiguous HKT-day
  blocks are safe for CSCV here too. Re-check this if a rolling-window param (e.g. an
  HAR-derived gate) is ever added to siglab's grid.

## 6. Phased implementation

- **Phase 0 — done, 2026-07-24.** World Cup removed (`worldcup.rs`, config, wiring,
  report sections, README), `MarketKind::Worldcup` kept historical-only, all checks
  clean. Live container not yet redeployed (see repo-root README TODO).
- **Phase 0.5 — done, 2026-07-24** (pulled forward from Phase 3, see status block up
  top): interim reports stopped pushing to git; `push_report.sh` + `.gitignore`
  updated; verified live.
- **Phase 1** (not started): `analysis/siglab_backtest_stats.py` — ported toolkit +
  unit tests against a small synthetic JSONL fixture (mirrors `backtest_stats_poc.py`'s
  own sanity-check-against-known-ground-truth approach).
- **Phase 2** (not started): `analysis/siglab_daily_digest.py` — reads the trade log,
  builds per-`(market, strategy)` panels, computes verdicts, appends
  `candidate_ledger.csv`, writes `digest_{date}.md` (bottom line, recommendations,
  markets-monitored table, collapsed detail — §4.7).
- **Phase 3** (not started): `docker-compose.yml` bind-mount fix for the trade-log
  volume, `siglab-daily-digest.timer` (generation only — pushing is already wired,
  §4.8) + install-script update, first real unattended 9am run, spot-check the output
  by hand before trusting it unattended.
- **Phase 4** (later, once ≥20 days of history exist): raise CSCV `S` from 8→16;
  revisit whether the hourly report's cadence/detail should shrink now that the
  digest exists.

Phases 1-4 are not started and won't begin without confirmation — this doc is the
review artifact, same convention `best_practice_backtest_2026-07-21.md` used.

## Sources

- Bailey, Borwein, López de Prado, Zhu, *The Probability of Backtest Overfitting* —
  [SSRN 2326253](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253)
- Bailey & López de Prado, *The Deflated Sharpe Ratio* (2014)
- `../btc_5mins/doc/plan_signal_lab_2026-07-19.md` — S0-S5 pipeline, first scoping of this literature
- `../btc_5mins/doc/best_practice_backtest_2026-07-21.md` — the concrete toolkit this plan ports
- `../btc_5mins/bot/backtest_stats.py` — source implementation being ported/adapted
- `siglab/README.md` — current architecture, config schema, report layout, deployment
