# siglab — multi-market signal live-testing harness

Live-tests many `reversal`/`high_prob` strategy parameter variants against real Polymarket
ticks across a large, rotating set of markets — crypto (5m/15m/4h/hourly-ET, all durations
across BTC/ETH/SOL/BNB/XRP/DOGE), weather (51 cities' daily temperature-bucket events), and
FIFA World Cup markets (62 events: outright winner, award winners, player props) — without
placing real orders and without recording raw tick data. Paper trade outcomes are logged to
JSONL; an hourly Markdown report summarizes signal activity, market state, and resource usage.

**Fully standalone from `../trader` and `../price_feed`.** Own config, own Dockerfile, own
`docker-compose.yml`, own systemd units, own `.gitignore`. It depends on `../trader` as a
Cargo *library* (reuses `Machine`/`gates`/`signal`/`marketdata` — the crypto trading-decision
core), but never reads or writes anything under `../trader/config`, `../trader/live_logs`, or
`../price_feed`. See `plan_weather_bot.md` (repo root) for the original design rationale.

## What it does and doesn't do

- **Crypto markets**: fully tradeable — real `trader::machine::Machine` instances per
  `(market, variant)` pair, real Binance reference feed, real entry/exit/PnL simulation.
- **Weather and World Cup markets**: **monitoring only** — track live prices and feed
  staleness per outcome bucket, but do **not** run them through `Machine`.
  `Machine::cycle_close()` resolves via Binance-price-momentum, which would fabricate
  win/loss labels against these markets' real (station-reading / match-outcome) resolution.
  See `src/event_monitor.rs`'s doc comment.
- No real orders, ever. No parquet/raw tick recording — `price_feed` already owns that.

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

## Config files (own schema, not trader's)

- `config/markets.toml` — crypto markets (`[[market]]` for 5m/15m/4h, `[[hourly_market]]` for
  the ET-calendar-hour markets) + strategy `[[variant]]`s. Deliberately its own minimal
  schema, not `trader::config::StrategyToml` — see `src/config.rs`'s doc comment for why.
- `config/weather_cities.toml` — the city list for weather monitoring.
- `config/worldcup_events.toml` — the FIFA World Cup event slug list (static, not
  date-rotating like weather).

Editing any of these has zero effect on `../trader`'s live config, and vice versa.

## Autonomous hourly report + push

The container writes `doc/report/signal_report_{YYYY-MM-DD}.md` (HKT date, new file per
day) every hour, with newest-hour-first collapsible `<details>` sections: crypto trades,
crypto+weather market state, staleness health, CPU/memory for the past hour.

A **separate host-side** systemd `--user` timer — not the container itself, which never
gets git/SSH credentials — commits and pushes that file hourly:

```bash
bash siglab/scripts/install_timer.sh   # one-time setup: installs + enables the timer
systemctl --user status siglab-report-push.timer
journalctl --user -u siglab-report-push.service   # check recent runs
```

`install_timer.sh` also enables user lingering (`loginctl enable-linger`) so the timer keeps
firing even when you're logged out. Report writing (in-container, hourly on the hour) and
report pushing (host-side, hourly at :05) are independent — the push script only acts if a
report file actually exists and has unstaged changes; see `scripts/push_report.sh`.

## Known issues (see `doc/` for full writeups)

- **Memory growth under full load, not yet root-caused** — see
  `doc/incident_ws_2026-07-13.md`. Not urgent at the observed rate, but this is now a
  long-running unattended process, so worth watching.
- **CPU/subscription-batching bug — found and fixed 2026-07-13** — see the same doc. Weather
  subscriptions are now batched per-city (not per-bucket), matching Polymarket's documented
  WS best practice and `price_feed`'s existing pattern.
- **Hourly push silently failing on SSH auth — found and fixed 2026-07-13** — systemd
  `--user` services don't inherit the interactive shell's SSH agent socket. Fixed by
  `install_timer.sh` injecting the current `SSH_AUTH_SOCK` at install time. That socket is
  tied to the current login session, not stable across reboot/re-login — **re-run
  `install_timer.sh` if hourly pushes silently stop again.** See the same doc, §5.

## Layout

```
siglab/
  Cargo.toml / Cargo.lock   # path-depends on ../trader (source only)
  Dockerfile                # context must be repo root — see comment inside
  docker-compose.yml        # standalone, not part of ../docker-compose.yml
  config/                   # markets.toml, weather_cities.toml, worldcup_events.toml
  scripts/                  # push_report.sh, install_timer.sh
  systemd/                  # siglab-report-push.{service,timer}
  src/
    main.rs                 # CLI + task orchestration
    config.rs                # standalone TOML schema
    market.rs / rotation.rs  # crypto market rotation + Machine wiring
    event_monitor.rs          # shared discovery/monitoring core (monitoring-only events)
    weather.rs / worldcup.rs  # thin wrappers over event_monitor for each event source
    staleness.rs              # observe-only staleness telemetry (per-class correlated check)
    snapshot.rs / report.rs / cgroup.rs   # shared state, hourly MD report, resource sampling
    record.rs                 # paper trade-record output type
  doc/
    local_resource_test_2026-07-13.md   # Docker resource baseline + fix history
    incident_ws_2026-07-13.md            # WS subscription bug + memory investigation
    report/                              # hourly signal_report_*.md (git-tracked)
```
