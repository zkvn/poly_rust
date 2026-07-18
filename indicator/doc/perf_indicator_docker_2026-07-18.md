# indicator module — docker perf A/B + quality verification (2026-07-18)

Results for the standalone `indicator` crate's local validation, per
`trader/doc/feature_vol_2026-07-18.md` §4–§5. Everything below ran on the
dev box via `docker-compose.perf.yml` (project `indperf`), all services on
host networking (compose custom-bridge egress is firewalled here — see the
compose file header).

## Verdict

**Go for Oracle deploy.** The indicator process is effectively free
(≈0.02% of one core, ≈2–6 MiB RSS), the trader-side consumption cost is
unmeasurable (delta indistinguishable from zero across 4.4 h), and both the
offline replay and the *live* NATS output match the Python reference at
machine precision. No restarts, no errors, no drift over the soak.

## Setup

- **Stack:** `nats` + `price-feed` (BTC collector) + `indicator` (BTC, 5m,
  HAR windows [1,5,12], emit throttle 250 ms) + two concurrent `--dry-run`
  traders on the same feed: `trader-base` (prod config, indicator off) and
  `trader-ind` (identical strategy params + `indicator_enabled = true`).
  Concurrent A/B ⇒ identical market conditions for both traders.
- **Window:** 4.38 h (2026-07-18, 07:41 → 12:04 HKT); `docker stats` sampled
  every 30 s; first 10 min discarded as startup noise → 474 steady-state
  samples per container.
- **Feed volume:** `price.binance.BTC` ≈ 4.0 msg/s in; `indicator.BTC`
  ≈ 1.2 msg/s out (throttle + same-second dedup).
- Neither trader fired a dry-run entry during the window (same market, same
  config — expected symmetry), so the A/B compares the full tick-processing
  load minus order placement.

## Resource usage (steady state, 474 × 30 s samples)

| container | CPU avg | CPU p95 | CPU max | mem avg | mem max |
|---|---|---|---|---|---|
| **indicator** | **0.023%** | 0.030% | 0.04% | **2.2 MiB** | 6.2 MiB |
| trader-base | 0.390% | 0.620% | 1.13% | 6.9 MiB | 13.5 MiB |
| trader-ind | 0.384% | 0.613% | 1.33% | 5.3 MiB | 8.8 MiB |
| price-feed | 2.208% | 3.660% | 6.27% | 43.4 MiB | 54.0 MiB |
| nats | 0.985% | 1.520% | 2.52% | 14.5 MiB | 18.8 MiB |

**Trader A/B delta (ind − base): −0.006 pp CPU, −1.6 MiB** — i.e. zero;
the sign is sampling noise. Consuming the ~1.2 msg/s indicator subject (JSON
parse + map insert per message) does not register against the traders'
baseline tick load. Stability: **0 restarts** on all five containers; the
only "error" log line in either trader is the expected dry-run notice about
the intentionally absent env file; 1,036 heartbeats in `trader-ind` carried
`ind[...]` values.

## Quality — Rust vs Python bt4 reference

Two independent checks, both against `../btc_5mins/bot/signals.py`
(`VolHarSignal` / `PUpSignal` / `SnrSignal`, the bt4 signal stack):

**1. Offline replay parity** (`indicator replay` vs
`scripts/indicator_parity_ref.py`, identical 1-Hz input built from recorded
BTC 2026-07-17 parquet — 86,100 rows, 286 cycles):

| signal | max abs diff | ready rows | gate |
|---|---|---|---|
| vol_har | 4.3e-19 | 85,800 | 1e-6 |
| p_up | 9.3e-13 | 86,100 | 1e-6 |
| snr | 2.1e-14 | 85,514 | 1e-6 |

**2. Live-path verification** — during the soak, `indicator.BTC` and
`price.binance.BTC` were tapped; every published message was independently
recomputed from the raw ticks (grid reconstruction + scipy `stdtr`):

- **12,760 / 12,760 messages match**: snr bit-exact (max |Δ| = 0.0),
  p_up max |Δ| = 9.3e-13 (scipy-vs-`puruspe` last-bit rounding).
- 120 messages skipped from the check by construction (tap started mid-cycle:
  warmup / first-partial-cycle / boundary-second rows).

One real bug was found *by* the parity harness before any live use: the
textbook Student-t CDF form `1 − ½·I_{ν/(ν+t²)}(ν/2, ½)` is catastrophically
ill-conditioned near z = 0 (t = 1e-6 returned 0.995 instead of 0.5000004);
fixed with the symmetric `½ ± ½·I_{t²/(ν+t²)}(½, ν/2)` form (commit
b20e4fa). Worth remembering for any future CDF port.

Observation, not a bug: live `p_up` and the poly token price disagreed hard
in some cycles (model DOWN ≈ 0.03 while UP token traded ≈ 0.775). The
recomputation above proves the model output is correct — this is the market
disagreeing with the pure-diffusion model, consistent with bt4's "weak
filter" AUC finding and exactly why phase 1 is log-only.

## Repro

```bash
docker compose -f docker-compose.perf.yml -p indperf up -d --build
bash scripts/perf/sample_docker_stats.sh stats.csv 30      # sampler
# ... hours later ...
docker compose -f docker-compose.perf.yml -p indperf down
```

## Follow-ups

- Phase 2: config-driven gates consuming the snapshot store
  (`[[indicator_gate]]`-style, pup_gate-study thresholds) — deliberately not
  in this change.
- Oracle deploy: cross-compile `indicator` for aarch64 like the trader
  (`cross build --release --target aarch64-unknown-linux-gnu`), systemd unit
  next to price_feed's; enable `indicator_enabled` on the Oracle trader only
  after the gate work has a reason to read it.
- Changing HAR `windows` requires re-fitting betas
  (`../btc_5mins/ml/features/har_beta.py`) — the config validates shapes,
  not calibration.
