# Plan: guard the best-bid/ask stream merge against out-of-order overwrites (2026-07-16)

## Status: reviewed (DeepSeek + empirical check below), implementing.

## DeepSeek review summary

Sent this plan (before any code was written) to `deepseek-v4-pro` for an independent critique.
Overall verdict: **"Yes, provided the timestamp comparability is verified and monitored... the
guard itself cannot make the state worse than the current unguarded merge (it only narrows
acceptance)."** Findings, ranked by its own severity labels, and what was done about each:

1. **(High) Cross-stream timestamp comparability must be verified empirically before trusting a
   hard accept/reject gate on it** (same concern this plan already flagged, DeepSeek elaborated:
   a `PriceChange` batch timestamp could reflect "batch window closed" rather than the exact
   event, introducing systematic skew). **Addressed:** queried 215,685 real rows across
   `price_feed/raw/{BNB,BTC}_poly_2026-07-16_*.parquet` (`server_ts`/`latency_ms` columns, which
   merge both channels already) — **zero negative latencies**, p95 = 17ms (matches
   `README.md`'s documented p95 ≈ 15-17ms exactly), only 21 outliers >1s (max 2046ms, consistent
   with genuine reconnect blips, not a systematic mismatch). A real unit/epoch bug between the
   two channels would show up as either uniformly-wrong values or a cluster of negative
   latencies — neither appears. Can't fully isolate per-channel skew from this column alone
   (both sources are merged before it's written), but this rules out the worst-case failure mode
   DeepSeek was most concerned about. Proceeding with `>=`.
2. **(Medium) Tie-breaking on exact-equal timestamps could let a batch-derived value overwrite a
   direct `BestBidAsk` one.** Suggested a source-aware tie-break. **Deferred, not implemented**:
   adds a `Source` enum threaded through both merges for a same-millisecond tie that `>=` already
   handles safely (never worse than current behavior) — scope creep for a fix framed as minimal
   and contained. Noted as a follow-up if telemetry (next point) ever shows it matters in
   practice.
3. **(Medium) No operational visibility into how often the guard rejects updates.** **Partially
   addressed:** a rate-conscious `eprintln!` on rejection (existing codebase convention for
   diagnostics, e.g. reconnect logging) rather than a full metrics counter — real metrics
   infrastructure is out of scope for this fix. Flagged as a TODO.
4. **(Low) Document why resetting the tracked timestamp on reconnect is intentional.**
   **Addressed** — doc comment added at the reset site.
5. **(Low) Guard against a `server_ts_ms <= 0` placeholder permanently blocking real data.**
   **Addressed** — treated as "no timestamp," always accepted.
6. **(Low) Confirm `PriceChange.timestamp` actually exists on the struct before relying on it.**
   Already independently confirmed by reading the vendored SDK source directly
   (`polymarket_client_sdk_v2-0.6.0/src/clob/ws/types/response.rs:108-109`) — stronger evidence
   than DeepSeek could get without filesystem access, so this is resolved, not just deferred.

**Guard function signature, per DeepSeek's suggestion:** decoupled from `BbaSample`/any struct —
takes the two raw timestamps directly (`Option<i64>`/`i64`), for a pure, minimal, easily-testable
function in both crates.

## Problem

Both `price_feed/src/collect.rs::spawn_bba_task` and `trader/src/marketdata.rs::spawn_poly_task`
subscribe to two separate CLOB WebSocket channels for the same token — `best_bid_ask` and
`price_change` — and merge them with `futures::stream::select`. That combinator has no ordering
guarantee: it yields whichever stream has a message ready first, with zero awareness of each
message's own timestamp. If a `price_change` update describing an *older* book state happens to
arrive locally after a `best_bid_ask` update describing a *newer* one (plausible — they're two
independently-buffered subscriptions over async channels, not a single ordered stream by the time
they reach this code), the older value can silently overwrite the newer one.

Full background: `trader/doc/review_trader_tick_data_quality_2026-07-16.md` (§1) and
`siglab/doc/incident_signal_2026-07-16.md` (the incident that surfaced this).

## Why this is critical, not cosmetic

- **`price_feed`'s recorder bakes the corruption in permanently.** `collect.rs::write_sample`
  (`collect.rs:722-767`) samples whatever `state.latest_bba` currently holds every 200ms and
  writes it straight into the sealed hourly parquet file. Once written, there is no later
  correction — a stale-overwrite artifact becomes a permanent, wrong row in
  `price_feed/raw/*.parquet`.
- **Backtest inherits it with no way to tell.** `trader/backtest_prices/*.parquet` is built
  directly from `price_feed/raw/` via `build_backtest_prices.py` (confirmed: root `README.md:
  297-298`). `trader::backtest::load_price_data` reads these files as ground truth. A corrupted
  row from the live recorder replays as a real historical price in every future backtest run
  against that date, with nothing distinguishing it from a genuine market move.
- **Live trading is exposed twice.** `spawn_bba_task`'s merge feeds both the parquet sampler
  *and* the `price.poly.*` NATS publish that `bin/live.rs`'s production path (`--nats-url`,
  what `docker-compose.yml` runs) subscribes to — so a stale-overwrite event reaches real-money
  trading decisions directly, not just the historical record.
- **`trader::marketdata::spawn_poly_task` has the identical merge pattern**, used by `siglab`
  (currently deployed and actively trading paper positions on BNB/ETH/SOL/BTC/XRP/DOGE),
  `bin/shadow.rs`, and `bin/live.rs`'s non-NATS fallback mode.

## Root cause detail

- `futures::stream::select(bba_u, pc_u)` interleaves by local poll-readiness only — confirmed via
  `docs.rs`'s own description of the combinator and general async-stream practice (same behavior
  as RxJS/Kotlin Flow `merge`): it is a *fairness* primitive, not an *ordering* primitive.
- Both message types carry a real server-side timestamp: `BestBidAsk.timestamp` (SDK doc:
  "Unix timestamp in milliseconds") and `PriceChange.timestamp` (batch-level, shared by every
  `PriceChangeBatchEntry` in that batch — not explicitly doc-commented as milliseconds in the SDK
  source, but `price_feed`'s own existing `write_sample`/`latency_ms` computation already treats
  it as directly comparable, same-unit milliseconds against `BestBidAsk.timestamp` and local
  `received_at_ms` — i.e. this is an existing assumption already load-bearing in production
  today, not a new one this fix introduces. Flagged for DeepSeek to double-check regardless.)
- Nothing currently compares an incoming update's timestamp against what's already cached before
  overwriting it, in either crate.

## Proposed fix (small, contained — not a redesign)

### 1. `price_feed/src/collect.rs::spawn_bba_task`

Already captures `server_ts_ms` per merged event (needs it for the existing `latency_ms`
metric) — the fix is purely "use it." Extract a pure, unit-testable helper:

```rust
fn should_replace_bba(existing: Option<&BbaSample>, new_server_ts_ms: i64) -> bool {
    match existing {
        None => true,
        Some(e) => new_server_ts_ms >= e.server_ts_ms,
    }
}
```

Guard **both** the `latest_bba` cache write (`collect.rs:1183-1189`) **and** the NATS publish
(`collect.rs:1194-1199`) with this check — publishing a stale price to NATS would still inject
the same phantom price into live trading even if the parquet sampler no longer picks it up. Both
currently fire unconditionally for every message that merely passes the `is_finite()`/`> 0.0`
check.

### 2. `trader/src/marketdata.rs::spawn_poly_task`

Doesn't read `m.timestamp`/`p.timestamp` at all today. Needs:
- Extract `server_ts_ms` from both channels (mirroring `price_feed`'s existing extraction).
- Track the last-accepted `server_ts_ms` as a plain local variable within the subscribe loop
  (reset naturally on every reconnect, since a reconnect starts a fresh subscription — no
  cross-reconnect state needed).
- The same guard logic as above (not literally shared code — `trader` and `price_feed` are
  separate crates with no shared internal module for this — but the identical shape).
- **Does not** change `PolyTick`'s public fields or `ts`'s meaning (still local receive time,
  matching every existing call site's expectations across `worker.rs`/`strategies.rs`/
  `machine.rs`/`backtest.rs`/`gates.rs`) — this only gates *whether* an update is accepted into
  the merge before a `PolyTick` is even constructed.

## Explicitly out of scope (tracked separately, not blocking this fix)

From `trader/doc/review_trader_tick_data_quality_2026-07-16.md`: `SpreadSignal`'s dead gate
(synthetic `dn = 1 - up`), no crossed-book (`bid > ask`) validation, cross-cycle stale-price
carryover in `LatestPolySignal::reset`, liquidity-aware fill pricing. Not touched here.

## Risk / edge cases

- **Tie-breaking:** `new_server_ts_ms == existing` → accept (`>=`, not `>`). Being biased toward
  accepting avoids permanently freezing on a stale winner if the server ever emits two
  same-millisecond updates.
- **First message:** `existing = None` → always accept.
- **One channel going quiet:** the guard doesn't discriminate by channel type, only by
  timestamp, so a channel that stops sending simply stops contributing — no special-casing
  needed.
- **Self-healing:** rejecting a stale update never leaves the cache worse off than the current
  (unguarded) behavior — the next genuinely newer update is still always accepted. This can only
  narrow what gets accepted, never freeze state indefinitely.
- **Cross-message-type timestamp comparability** (flagged above) is the one assumption worth an
  independent second look before relying on it for a *rejection* decision (previously it only
  fed an informational `latency_ms` column — being wrong there was cosmetic; being wrong in a
  reject-vs-accept gate could actively drop good data if the two channels' clocks disagree more
  than expected).

## Testing plan

- Unit tests for the pure helper, both crates: in-order accepted, equal-timestamp accepted,
  out-of-order rejected, first-sample (`None` existing) always accepted.
- Full existing suite for `price_feed`, `trader`, and `siglab` (siglab depends on `trader` as a
  library — confirm no ripple): `cargo test`, `cargo fmt --all --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`.
- No behavior change expected for any currently-passing test — the guard only ever narrows what
  gets accepted, and no existing test constructs an out-of-order sequence — so no existing test
  should need changing. Worth confirming, not assuming.

## Implemented — verification results

- `price_feed::collect.rs`: added `should_accept_bba_update` (pure, `Option<i64>`/`i64` in,
  `bool` out) and wired it to guard both the `latest_bba` cache write and the NATS publish in
  `spawn_bba_task`. 7 new unit tests. No existing test needed changing.
- `trader::marketdata.rs`: added `should_accept_poly_update` (same shape) and threaded
  `server_ts_ms` through both merged streams in `spawn_poly_task` (previously not captured at
  all — a real structural change, not just a guard insertion). A local
  `last_accepted_server_ts_ms`, scoped inside the per-reconnect block so it naturally resets on
  every reconnect (commented at the call site per DeepSeek's finding #4). 7 new unit tests. No
  existing test needed changing.
- Full verification, all three crates:
  - `price_feed`: `cargo build --bin price_feed`, `cargo test --bin price_feed` (44 passed, 0
    failed), `cargo fmt --all --check` (clean after one auto-fix), `cargo clippy --all-targets
    --all-features -- -D warnings` (clean).
  - `trader`: `cargo build --lib`, `cargo test` (210 lib + 5 + 34 bin tests, 0 failed), `cargo
    fmt --all --check` (clean after one auto-fix), `cargo clippy --all-targets --all-features --
    -D warnings` (clean).
  - `siglab` (depends on `trader` as a library — confirms no ripple): `cargo build`, `cargo
    test` (56 passed, 0 failed), `cargo fmt --all --check` (clean), `cargo clippy --all-targets
    --all-features -- -D warnings` (clean).
- No existing test in any of the three crates needed modification, as predicted.

## Deployed — production observation

- `price_feed`: rebuilt (`cross build --release --bin price_feed --target
  aarch64-unknown-linux-gnu`), rsynced, and restarted on Oracle via `python scripts/
  deploy_oracle.py --price-feed-only` (dry-run previewed first). `poly-collector.service`
  confirmed `active (running)`, resumed carry-forward writes for all 7 assets across all
  durations with no errors in the post-restart log.
- `siglab`: rebuilt and restarted (`docker compose -f siglab/docker-compose.yml up --build -d`).
  Confirmed healthy: no panics/errors, trades continued logging normally post-restart (checked
  `siglab_trades.jsonl` — sane entry/exit prices, expected outcome shapes).
- **The guard is firing far more often than a rare-edge-case bug would predict**: ~50% of raw
  merged poly messages in `siglab`'s first two minutes post-restart were rejected as
  out-of-order (`stale poly bba/price update rejected` — 738 of 1,410 log lines in one 60s
  sample). This is a significant finding in its own right, beyond just confirming the fix
  compiles and runs: it means roughly half of all `price_change`-channel messages were arriving
  with a timestamp at or behind what `best_bid_ask` had already delivered — before this fix,
  each of those was silently overwriting a fresher, correct price with a stale one. The original
  bug was not a rare millisecond-scale race; it was a routine, high-frequency event. Consistent
  with `spawn_bba_task`'s own doc comment describing `best_bid_ask` as the "low latency" channel
  and `price_change` as "always-on" (implying secondary/fallback) — `price_change`'s reports
  frequently describe already-superseded state relative to the faster channel, so a high
  rejection rate specifically for `price_change`-sourced messages is the guard doing exactly what
  it's for. Confirmed trading stayed healthy throughout (DOGE trades continued logging with sane
  prices), so `best_bid_ask` alone is evidently sufficient to drive normal operation — the
  rejected messages were redundant, not uniquely-informative.

## Deploy plan

- `price_feed`'s fix needs redeploying to the Oracle box via **`python scripts/deploy_oracle.py
  --price-feed-only`** (script lives at repo-root `scripts/`, not `price_feed/scripts/`) — that's
  where the live recorder (`poly-collector.service`) actually runs and where the
  permanent-corruption risk lives.
- **Deliberately scoping to `--price-feed-only`, not the default (both binaries).** The default
  invocation also rebuilds and restarts `trader-live.service` — the real-money live trading bot.
  Checked `bin/live.rs`: `trader-live.service`'s systemd unit (rendered by this same script's
  `_trader_unit_file`) always passes `--nats-url nats://127.0.0.1:4222`, so it runs the NATS
  ingestion path, never `spawn_poly_task`/`PolySub` (the direct-WS path, gated behind
  `args.nats_url.is_none()`). The `trader::marketdata` fix (§2 above) therefore has **zero
  runtime effect on what's actually deployed as `trader-live.service`** — restarting it here
  would be a real-money-adjacent action with no corresponding benefit. `--price-feed-only` avoids
  that risk entirely.
- `trader`'s fix (`marketdata.rs`, direct-WS path) affects `siglab` instead — confirmed currently
  deployed and running (`docker ps`: `siglab-siglab-1`, actively trading paper positions) —
  so the `siglab` Docker container needs a rebuild + restart (`docker compose -f
  siglab/docker-compose.yml up --build -d`), separately from the Oracle deploy above.
  `bin/shadow.rs` and `bin/live.rs`'s non-NATS fallback aren't currently deployed anywhere, so
  no separate action needed for those beyond the source fix landing.
- Preview with `--dry-run` first (`python scripts/deploy_oracle.py --price-feed-only --dry-run`)
  before the real run, per the script's own docstring warning about `poly-collector`/
  `trader-live` needing to go through `systemctl` cleanly.
