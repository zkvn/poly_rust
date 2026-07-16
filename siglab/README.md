# siglab — multi-market signal live-testing harness

Live-tests many `reversal`/`high_prob`/`v_shape` strategy parameter variants against real
Polymarket ticks across a large, rotating set of markets — crypto (5m/15m/4h/hourly-ET, all
durations across BTC/ETH/SOL/BNB/XRP/DOGE), weather (51 cities' daily temperature-bucket
events), and
FIFA World Cup markets (62 events: outright winner, award winners, player props) — without
placing real orders and without recording raw tick data. Paper trade outcomes are logged to
JSONL; a Markdown report — written every `--report-interval-secs` (900s/15 min in
production, see `docker-compose.yml`) — summarizes signal activity, market state, and
resource usage.

**Fully standalone from `../trader` and `../price_feed`.** Own config, own Dockerfile, own
`docker-compose.yml`, own systemd units, own `.gitignore`. It depends on `../trader` as a
Cargo *library* (reuses `Machine`/`gates`/`signal`/`marketdata` — the crypto trading-decision
core), but never reads or writes anything under `../trader/config`, `../trader/live_logs`, or
`../price_feed`. See `plan_weather_bot.md` (repo root) for the original design rationale.

## What it does and doesn't do

- **Crypto markets**: fully tradeable — real `trader::machine::Machine` instances per
  `(market, variant)` pair (real Binance reference feed, real entry/exit/PnL simulation) for
  the `reversal`/`high_prob` grids, plus a self-contained `v_shape::VShapeEngine` per
  `(market, variant)` pair for the 8-variant V-shape grid (pure CLOB price action, no
  Binance/gates — see `src/v_shape.rs`'s doc comment).
- **Weather and World Cup markets**: **monitoring only** — track live prices and feed
  staleness per outcome bucket, but do **not** run them through `Machine`.
  `Machine::cycle_close()` resolves via Binance-price-momentum, which would fabricate
  win/loss labels against these markets' real (station-reading / match-outcome) resolution.
  See `src/event_monitor.rs`'s doc comment. (A plan to actually simulate trades for these too
  is under review — see `doc/plan_weather_worldcup_trading_2026-07-13.md`.)
- No real orders, ever. No parquet/raw tick recording — `price_feed` already owns that.

<details>
<summary><strong>Quickstart (local, no Docker)</strong></summary>

## Quickstart (local, no Docker)

```bash
cargo build --release
cargo run --release -- \
  --config config/markets.toml \
  --weather-config config/weather_cities.toml \
  --worldcup-config config/worldcup_events.toml \
  --log siglab_trades.jsonl \
  --report-dir reports \
  --report-interval-secs 3600
```

`cargo test` / `cargo fmt --all --check` / `cargo clippy --all-targets --all-features -- -D
warnings` — all clean, run before any change.

</details>

<details>
<summary><strong>Docker (the real deployment)</strong></summary>

## Docker (the real deployment)

```bash
docker compose -f siglab/docker-compose.yml up --build -d   # from repo root
docker compose -f siglab/docker-compose.yml logs -f
docker compose -f siglab/docker-compose.yml down             # stop
```

Own image (`siglab-siglab`), own named volume for trade logs (`siglab_logs`), and a bind
mount of `siglab/doc/report/` into the container so the hourly report lands directly in the
git working tree. `network_mode: host` for CLOB/Gamma/Binance access via the box's existing
VPN routes (same reasoning as `../trader`'s compose entry) — siglab never touches VPN config.

</details>

<details>
<summary><strong>Config files (own schema, not trader's)</strong></summary>

## Config files (own schema, not trader's)

- `config/markets.toml` — crypto markets (`[[market]]` for 5m/15m/4h, `[[hourly_market]]` for
  the ET-calendar-hour markets) + strategy `[[variant]]`s. Deliberately its own minimal
  schema, not `trader::config::StrategyToml` — see `src/config.rs`'s doc comment for why.
- `config/weather_cities.toml` — the city list for weather monitoring.
- `config/worldcup_events.toml` — the FIFA World Cup event slug list (static, not
  date-rotating like weather).

Editing any of these has zero effect on `../trader`'s live config, and vice versa.

</details>

<details>
<summary><strong>Autonomous report + push</strong></summary>

## Autonomous report + push

The container writes one folder per real HKT day, `doc/report/{YYYY-MM-DD}/` (added
2026-07-15, replacing the AM/PM-split flat files — those had already grown unwieldy again
after the 2026-07-14 split): a `summary_{date}.md` with the strategy config table, a
whole-day PnL rollup (recomputed fresh from the trade log on every write), and an index
linking every hour's own file; and one `trades_{date}_{HH}.md` per real HKT hour, holding
that hour's merged trade tables (market/strategy PnL summary plus one collapsible table per
market, regenerated fresh on every write rather than split per report-writer run) followed
by each run that landed within it (production writes every 15 min — `--report-interval-secs
900`, see `docker-compose.yml`), newest run first, carrying crypto+weather+worldcup market
state, staleness health, and CPU/memory for that run's own window. Run boundaries within an
hour's file are tracked with a plain HTML comment marker (`<!-- siglab-run -->` — see
`src/report.rs`'s module doc comment). Pre-2026-07-15 reports stay in their old flat
`signal_report_*.md` form (not retroactively migrated); `--regenerate-reports-only`
(optionally scoped with `--regenerate-since YYYY-MM-DD`) backfills a date range into the new
per-day-folder layout from the trade log's ground truth — see `src/report.rs`'s
`regenerate_from_trade_log` doc comment.

A **separate host-side** systemd `--user` timer — not the container itself, which never
gets git/SSH credentials — commits and pushes those files every 15 minutes:

```bash
bash siglab/scripts/install_timer.sh   # one-time setup: installs + enables the timer
systemctl --user status siglab-report-push.timer
journalctl --user -u siglab-report-push.service   # check recent runs
```

`install_timer.sh` also enables user lingering (`loginctl enable-linger`) so the timer keeps
firing even when you're logged out. Report writing (in-container, every 15 min) and report
pushing (host-side, every 15 min, `OnCalendar=*-*-* *:00/15:00`) are independent — the push
script only acts if a report file actually exists and has unstaged changes; see
`scripts/push_report.sh`.

**If pushes silently stop working, see "SSH agent subtleties" below before anything else** —
that's the failure mode this has already hit once.

</details>

<details>
<summary><strong>SSH agent subtleties (systemd --user services do NOT get your shell's agent)</strong></summary>

## SSH agent subtleties

This bit `push_report.sh` for real on 2026-07-13 (see Incidents below) and is exactly the
kind of thing that's obvious once you know it and baffling until then, so it gets its own
section rather than staying buried in an incident writeup.

**The gotcha:** a `systemctl --user` service does not inherit the `SSH_AUTH_SOCK` (or most
other environment variables) from whatever interactive shell you happened to run
`systemctl --user start`/`enable` from. It gets the systemd **user manager's own default
environment** instead — on this box, that's `/run/user/<uid>/gcr/ssh` (GNOME Keyring's SSH
agent proxy), which is a *different agent* from the one your interactive terminal uses, and
it either doesn't have the relevant key loaded or refuses to use it non-interactively
(`agent refused operation`). `git push` then fails with `Permission denied (publickey)` —
exit 128 — even though `git push` works fine when you run it by hand two seconds later in
your normal terminal.

**Why this is sneaky:** the failure is silent unless someone checks `journalctl --user -u
<service>`. A script's `git commit` step still succeeds (no network/auth involved), so a
`git log` inside the repo looks fine — the divergence only shows up as "local has commits
origin doesn't," which nothing surfaces proactively.

**Diagnosing it:**

```bash
# From the same kind of context the failing service runs in:
env -i HOME="$HOME" PATH="$PATH" bash -c 'git ls-remote origin'
# If this fails with "Permission denied (publickey)" but the same command works fine in
# your normal interactive shell, this is the bug.

echo $SSH_AUTH_SOCK                       # your shell's agent
systemctl --user show-environment | grep SSH_AUTH_SOCK   # systemd's default — usually different
```

**The fix used here:** `install_timer.sh` reads `$SSH_AUTH_SOCK` from the shell it's run
from and bakes that exact path into the installed unit (`Environment=SSH_AUTH_SOCK=...`),
substituting a `__SSH_AUTH_SOCK__` placeholder in the git-tracked `.service` template. The
template is never committed with a real path — it would be both wrong for anyone else and
stale the moment the login session that produced it ends.

**The tradeoff, accepted deliberately (not the only option):** the socket this points at is
tied to the *current login session* — confirmed by checking the backing `ssh-agent`/
`gcr-ssh-agent` processes' start times, which matched this session's login time, not boot
time. It will stop working after a reboot or logout, at which point **re-run
`install_timer.sh` from a shell where `ssh -T git@github.com` already succeeds** to pick up
the new socket. The more robust alternative — a dedicated deploy key with no passphrase,
independent of any interactive session — was offered and explicitly not chosen (tradeoff:
an unencrypted private key on disk, vs. this session-fragility). Revisit if the re-run
dance becomes annoying enough.

**If this repo's automation ever runs on Oracle or another remote box:** the same class of
issue applies there independently — see the TODO below.

</details>

<details>
<summary><strong>Incidents (descending by when found/occurred)</strong></summary>

## Incidents

Full writeups in `doc/incident_ws_2026-07-13.md` unless noted.

### 2026-07-16 — `reversal` and `v_shape` sharing entry/exit timestamps on BNB-5m — **investigated, not a bug**
12 `reversal` variants and 8 `v_shape` variants (two separate engines — `v_shape` never touches
`trader::machine::Machine` or Binance) logged the identical millisecond `entry_ts`, and a subset
shared the identical exit timestamp and pnl too. Confirmed not a regression of the 07-14 fix
below (that bug was cross-*duration* correlation via a shared Binance tick; `entry_price_ts ==
entry_ts` here proves every entry fired off the market's own live poly tick). Root cause: two
ordinary, independent mechanisms compound — `market.rs` feeds one poly tick to every `Machine`
*and* every `VShapeEngine` in the same loop iteration, and both strategy families implement a
structurally similar "dipped, then recovered" entry condition, so one real sharp BNB move (cross-
checked against `price_feed`'s independently-recorded parquet — genuine, not a data artifact)
completed both at once; separately, `trader::machine` and `v_shape.rs` each force-close any open
position within 10s of cycle end at the market's current price, so positions from either strategy
still open at that boundary exit together. Not caught by the 07-14 fix because `v_shape.rs`
didn't exist until the day after that investigation. Full writeup: `doc/incident_signal_2026-07-16.md`.

### 2026-07-14 — Same-market variants sharing entry_ts flagged as suspicious — **investigated, not a bug**
Two specific trades (BTC-15m 12:22:35, ETH-5m 11:47:18 — several `reversal_0.4_*` variants
firing together) checked against `price_feed`'s independently-recorded CLOB/Binance data.
Confirmed genuine: for BTC, the shared `delta_pct_rev=0.0008` momentum gate (identical across
all 18 variants) cleared at the exact recorded entry instant, ~20ms of real Binance-tick
latency, while price had already cleared every threshold 6-7 minutes earlier — releasing every
already-qualified variant at once, at the real market price. `entry_price_ts` (this session's
other fix) confirms both fired off a live poly tick, not a stale cached price. Full writeup:
`doc/incident_same_entry_ts_2026-07-14.md`.

### 2026-07-14 — Reversal variants logged correlated entry timestamps across different markets — **fixed**
`Machine::try_enter` stamped `entry_ts` with whichever tick (poly or Binance) triggered the
check, not the timestamp of the poly price actually observed. Since every duration-task for
an asset shares one Binance broadcast, this made economically distinct markets (e.g.
`sol-updown-5m` and `sol-updown-15m`) log identical `entry_ts` to the microsecond — 1,151
trades (~15.5% of crypto reversal trades) affected. Fixed by adding an additive
`entry_price_ts` field (from `LatestPolySignal::ts`), zero change to any trading decision.
Also investigated (per explicit request) whether TIMEOUT-dominance meant SL/TP were broken —
confirmed not a bug (30s `unwind_time_rev` is far shorter than any cycle, so timeout
mathematically precedes cycle-close in effectively every trade; SL/TP do fire correctly when
price actually moves enough). Report gained exit-time/holding-duration columns, a strategy
config table, and millisecond-precision timestamps.
Full writeup: `doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md`.

All entries below are the same calendar day (2026-07-13) — this module was built and put
into production in one session.

### 21:16 HKT — Hourly report push silently failing on SSH auth — **fixed**
systemd `--user` services don't inherit the interactive shell's SSH agent (see "SSH agent
subtleties" above). Found when asked to check why no report commits had landed recently;
`journalctl` showed every real firing failing with exit 128. Fixed by having
`install_timer.sh` inject the working `SSH_AUTH_SOCK` into the installed unit. Verified by
manually triggering the systemd service and watching it push for real (commit `7439045`).

### 15:29 HKT — `push_report.sh` fatal-errored on empty report directory — **fixed**
`git add` with a glob pathspec matching zero files is a **fatal** git error, not a silent
no-op — hit on the timer's first two real firings, since a freshly (re)started container
has no report yet (siglab waits a full interval after startup before writing its first
one, and this session restarted the container several times while iterating). Fixed by
checking for zero matches with `shopt -s nullglob` before calling `git add`.

### 14:56 HKT — Weather WS subscriptions caused sustained 200-370% CPU — **fixed, ~5x reduction**
Root cause traced into `polymarket_client_sdk_v2`'s source: `ConnectionManager` holds one
`broadcast::channel` per WS connection, and every `subscribe_*()` call gets its own receiver
on that *same* channel, filtering client-side — with ~1,050 subscriptions (one call per
weather bucket token), cost was O(subscriptions × message rate). Confirmed against
Polymarket's own WS docs that the API is designed for one connection subscribed to many
`assets_ids` at once (which `price_feed` already does correctly). Fixed by batching one
subscribe call per city (~102 subscriptions instead of ~1,050): CPU avg 221%→44%, max
369%→83%, verified over two live 15-minute Docker runs.

### 14:56 HKT (same investigation) — Memory grows under full load — **found, open, not resolved**
The same post-fix run that confirmed the CPU fix showed memory climbing ~50→434 MiB over 15
minutes. A follow-up 1-city-vs-51-city local A/B test confirmed growth correlates with
weather scope, but the pattern is *stepped and plateauing* (long flat stretches between
jumps), not smooth/continuous — more consistent with allocator working-set growth settling
toward a steady size than a true unbounded leak, though not confirmed past ~15 minutes.
Not urgent at the observed rate; worth periodic `docker stats` checks given this now runs
unattended for days at a time.

### 14:56 HKT (same investigation) — `trader/src/bin/live.rs` has the same subscription-duplication pattern — **found, dormant, not fixed**
`live.rs` opens one Binance + one CLOB subscription per **(asset, strategy) worker**, not
per asset — an asset running two strategies (e.g. ETH: `reversal` + `high_prob`) would
duplicate both. Gated behind `args.nats_url.is_none()`, and `../docker-compose.yml`'s
`trader` service always passes `--nats-url`, so production takes the NATS pub/sub path
instead and never executes the duplicating code — real bug, not currently live. Out of
scope to fix here without explicit go-ahead to modify `trader/`.

### ~14:00 HKT, pre-first-commit (local testing) — Correlated-silence false alarm across market classes — **fixed**
An early version of the staleness tracker computed one "fraction of feeds gone quiet"
ratio across *all* tracked markets. Mixing ~50 fast-ticking crypto feeds with ~300+
naturally-quiet weather feeds meant weather's normal quiet stretches alone pushed the
combined ratio over threshold, firing a false "connection dead" warning. Fixed by scoping
the correlated-silence check per market class (`staleness.rs`), so each class is judged
against its own baseline cadence.

### ~13:00 HKT, pre-first-commit (local testing) — Duplicate per-duration Binance connections — **fixed**
The first local (non-Docker) run showed each asset opening a separate Binance WebSocket
connection per duration it trades (e.g. BTC-5m and BTC-15m each independently subscribing
to the same Binance trade stream). Fixed via `market::spawn_binance_broadcast` — one real
connection per asset, fanned out via `tokio::sync::broadcast` to every duration task
trading it — before the first Docker deployment, so the resource-test numbers in
`doc/local_resource_test_2026-07-13.md` reflect the fixed version throughout.

</details>

<details>
<summary><strong>TODO</strong></summary>

## TODO

- **When/if siglab is ever deployed to Oracle (or any box besides this dev machine), the
  autonomous git-push setup needs redoing for that machine — the current fix is
  host-specific.** The `SSH_AUTH_SOCK`-injection approach (see "SSH agent subtleties" above)
  bakes in a path tied to *this* machine's login session; it means nothing on a different
  host. Whoever deploys there needs to either repeat the same diagnosis (check
  `$SSH_AUTH_SOCK` in an interactive shell that can push, vs. what `systemctl --user
  show-environment` gives you there) or use that as the trigger to switch to the more robust
  deploy-key approach instead, especially if Oracle has no persistent interactive login
  session to piggyback on in the first place (likely, for a headless server).
- **Weather/World Cup markets are monitoring-only; a plan to actually simulate trades for
  them (reusing the 18-variant reversal grid + high_prob) is written and pending review** —
  see `doc/plan_weather_worldcup_trading_2026-07-13.md`. Not started.
- Memory growth under full load — see Incidents above — not root-caused, being watched
  rather than fixed for now.
- **`trader::marketdata::spawn_poly_task` merges two independently-arriving WS streams
  (`best_bid_ask` + `price_changes`) into one `(bid, ask)` pair with no verified atomicity
  guarantee** — found 2026-07-16 investigating the `reversal`/`v_shape` correlated-timestamp
  report (see Incidents above), where a single entry tick's price didn't match
  `price_feed`'s independently-archived book/poly data by ~4.5¢ and couldn't be conclusively
  attributed given that archive's 200ms sampling. Needs either an audit of
  `polymarket_client_sdk_v2`'s `PriceChangeBatchEntry` guarantees or raw per-message (not
  resampled) logging to close out. Full writeup: `doc/incident_signal_2026-07-16.md`.
- Force-unwind-near-cycle-end (`trader::machine` and `v_shape.rs`, both at a 10s-before-cycle-end
  threshold) fills at the raw mid-price with no spread/liquidity check — noticed 2026-07-16 in a
  cycle that had spreads as wide as 0.80 a few minutes earlier, so paper PnL near cycle-end in
  thin books may be more optimistic than a real fill could achieve. Not investigated further; see
  `doc/incident_signal_2026-07-16.md`'s Follow-ups.

</details>

<details>
<summary><strong>Layout</strong></summary>

## Layout

```
siglab/
  Cargo.toml / Cargo.lock   # path-depends on ../trader (source only)
  Dockerfile                # context must be repo root — see comment inside
  docker-compose.yml        # standalone, not part of ../docker-compose.yml
  config/                   # markets.toml, weather_cities.toml, worldcup_events.toml
  scripts/                  # push_report.sh, install_timer.sh, regenerate_reports.py (one-off
                             #   migration of old flat reports to the nested hour/run format)
  systemd/                  # siglab-report-push.{service,timer}
  src/
    main.rs                 # CLI + task orchestration
    config.rs                # standalone TOML schema
    market.rs / rotation.rs  # crypto market rotation + Machine + v_shape wiring
    bucket_reversal.rs        # self-contained reversal engine for weather/World Cup buckets
    v_shape.rs                # self-contained V-shape engine for crypto markets
    event_monitor.rs          # shared discovery/monitoring core (monitoring-only events)
    weather.rs / worldcup.rs  # thin wrappers over event_monitor for each event source
    staleness.rs              # observe-only staleness telemetry (per-class correlated check)
    snapshot.rs / report.rs / cgroup.rs   # shared state, per-day MD report, resource sampling
    record.rs                 # paper trade-record output type
  doc/
    local_resource_test_2026-07-13.md         # Docker resource baseline + fix history
    incident_ws_2026-07-13.md                  # full incident writeups (summarized above)
    plan_weather_worldcup_trading_2026-07-13.md  # pending-review plan (not started)
    report/                                    # {date}/summary_{date}.md + trades_{date}_{HH}.md (git-tracked)
```

</details>
