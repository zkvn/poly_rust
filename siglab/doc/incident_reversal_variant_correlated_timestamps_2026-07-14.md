# Incident: reversal variants logging correlated entry timestamps across different markets (2026-07-14)

## Symptom

Reported from `doc/report/signal_report_2026-07-14.md`: many `reversal_{low}_{high}` variants
firing at the exact same timestamp with the exact same outcome/pnl, e.g.:

```
2026-07-14 08:15:56  reversal_0.2_0.55  DOWN  TIMEOUT  0.1006
2026-07-14 08:15:56  reversal_0.2_0.6   DOWN  TIMEOUT  0.1006
2026-07-14 08:15:56  reversal_0.2_0.65  DOWN  TIMEOUT  0.1006
2026-07-14 08:15:56  reversal_0.2_0.7   DOWN  TIMEOUT  0.1006
2026-07-14 08:15:56  reversal_0.2_0.75  DOWN  TIMEOUT  0.1006
```

repeated across multiple markets, prompting the question of whether this is real (a fast
price move legitimately satisfying several thresholds at once) or a tracking bug.

## Investigation

Pulled the raw `siglab_trades.jsonl` out of the running container (`docker cp` from
`siglab-siglab-1:/app/logs/siglab_trades.jsonl`) and analyzed it directly, since the rendered
Markdown report truncates timestamps to whole seconds (see "Contributing cosmetic bug"
below) and can't distinguish "genuinely simultaneous" from "merely rounds to the same
second."

**Within one market:** grouping by `(slug, entry_ts)` showed 93% of crypto reversal trades
share their exact `entry_ts` with at least one other trade for the *same* market instance.
This part is expected and already anticipated in `report.rs`'s own doc comment ("18 reversal
variants often firing together on the same dip") — all 18 variants for one market consume
the identical tick stream in the same loop iteration (`market.rs`'s `for (variant_id, m) in
machines.iter_mut() { m.on_poly(tick) }`), so when one tick satisfies several thresholds at
once, they legitimately tie. Not a bug.

**Across different durations of the same asset — the real bug:** 67 distinct `entry_ts`
values (1,151 trades, ~15.5% of all crypto reversal trades) were shared across *different*
slugs for the same asset, e.g. `sol-updown-15m-1783941300` and `sol-updown-5m-1783941300`
both logged `entry_ts: 1783941489.4619706` to the exact microsecond — despite recording
different prices (`token_price` 0.665 vs 0.865), proving these are genuinely different real
order books, not a duplicate-record bug. As a control: **zero** such collisions occur across
*different assets* (BTC never collides with ETH). That control rules out coincidence and
points directly at the one thing 5m/15m/4h/hourly-et tasks for the same asset actually share:
one Binance broadcast channel (`market::spawn_binance_broadcast`, one real connection per
asset fanned out via `tokio::sync::broadcast` to every duration task trading it).

## Root cause

`trader::machine::Machine::try_enter` stamps `HoldingData.entry_ts` with `now`, the
timestamp of whichever tick (poly *or* Binance) caused `try_enter` to run — entry evaluation
fires on both feeds by design (poly is the primary trigger; a Binance tick can also complete
an already-armed entry using the latest *cached* poly price and delta_pct — see
`machine.rs`'s module doc comment and `trader/doc/latency_2026-07-04.md` §8). Since every
duration-task for an asset receives the *identical* broadcast Binance tick, any entry that
fires via the Binance-triggered path records `entry_ts` = that shared tick's timestamp,
regardless of which market it belongs to or how stale its own cached poly reading actually
was. The recorded "entry time" was never the real observation time of the price that
satisfied the condition — it was just whichever feed happened to complete the check.

This also, secondarily, explains part of the same-market clustering: two variants of the
same market can fire on different real poly ticks but still record identical `entry_ts` if
both instead complete via the same later Binance tick using their (shared, since they watch
the same market) cached poly price.

## TIMEOUT-dominance — investigated, not a bug

Separately checked whether `TIMEOUT` outcomes dominating (6,370 of 7,427 crypto reversal
trades, ~86%) indicates stop-loss/take-profit are broken, given a 30s `unwind_time_rev` seems
short enough that *something* should trip first. Confirmed via the raw log this is not a
bug:

- `STOPLOSS` (250) and `UNWIND`/take-profit (789) both fire in real numbers (~14% combined) —
  they are not silently broken.
- No `TIMEOUT` trade's price delta ever reaches `sl_pnl_rev`(-0.30)/`unwind_pnl_rev`(0.15) —
  max observed delta on a TIMEOUT row was 0.1499996 (unwind_pnl_rev is checked first, so this
  is exactly the expected boundary, not a coincidence).
- Of the 5,704 TIMEOUT trades in markets with a known period, **100%** entered with more than
  30s left in their cycle — i.e. the 30s cap was mathematically guaranteed to fire before the
  market's own natural cycle-close in every one of them.
- The only 18 `WIN`/`LOSS` (natural cycle-close) outcomes all entered with ~26.5s left in a
  300s cycle — inside the 30s window, exactly where cycle-close can beat the timeout.

Given `unwind_time_rev = 30.0` is far shorter than any configured market's `period_secs`
(300s/900s/14400s/3600s) and entries can now fire at any point in the cycle (per the
2026-07-13 `reversal_start_time = 999999` widening — see `config/markets.toml`'s header
comment), almost every position mathematically times out before its cycle naturally closes,
and price simply doesn't move `0.15`/`0.30` within 30 seconds often enough to trip SL/TP
first. Working as designed, not a defect.

## Contributing cosmetic bug: whole-second timestamp truncation

`report.rs`'s `entry_datetime_hkt` (`Utc.timestamp_opt(entry_ts as i64, 0)`) discarded the
fractional-second part before formatting, so two trades a few hundred milliseconds apart
could render as the identical string in the Markdown report even when the underlying JSONL
`entry_ts` values differ. This didn't cause the bug above (confirmed via the raw JSONL, which
showed exact float equality, not just same-second rounding) but made it harder to tell real
ties from rendering artifacts by eye. Fixed alongside (see below).

## Fix

**`trader/src/types.rs` / `machine.rs` / `worker.rs`** (additive only, `#[serde(default)]`,
zero change to any entry/gate/fill/timeout decision):

- Added `TradeRecord::entry_price_ts` and `HoldingData::entry_price_ts`, populated from
  `LatestPolySignal::ts` — the actual timestamp of the poly-price observation that satisfied
  entry, independent of which feed's tick triggered the `try_enter` check.
- `entry_ts` itself is untouched, so `unwind_time`/timeout countdown math and every existing
  live-bot behavior is unaffected.
- New regression test
  `machine::tests::entry_price_ts_reflects_stale_cached_poly_tick_not_triggering_binance_tick`
  reproduces the exact mechanism: dip+recovery observed on one poly tick, entry only actually
  fires later on a Binance tick that finally caches `delta_pct` — asserts `entry_ts` (the
  Binance tick) and `entry_price_ts` (the earlier poly tick) differ.
- `worker.rs` (the live driver) got the same additive field for schema consistency, wired
  from its own `latest_poly.ts` at the `on_order_filled` `HoldingData` construction site —
  **not deployed**; see root `README.md`'s incident entry for why (compiled and tested
  locally only, per explicit instruction).

**`siglab/src/record.rs`**: `SiglabTradeRecord.entry_price_ts` threads the new field through
from both `TradeRecord` (crypto) and `bucket_reversal.rs`'s engine (which has no separate
triggering-tick-vs-observation-tick distinction, so it's set equal to `entry_ts` there,
honestly).

**`siglab/src/report.rs`**:
- Per-market trade tables gained `exit (HKT)` and `holding (s)` columns (`logged_at` as the
  exit-time proxy — it's stamped immediately after the trade record is produced in the same
  synchronous handler as the exit decision, so it's accurate without needing a further
  `trader/src` change to carry a real `exit_ts` through).
- `datetime_hkt` (renamed from `entry_datetime_hkt`) now renders to millisecond precision
  instead of truncating to whole seconds.
- New "Strategy config" section at the top of every report (markets/durations table +
  full variant-grid table), regenerated fresh on every write via a
  `<!-- siglab-config-start/end -->`-delimited block that gets replaced wholesale rather than
  left stale or duplicated — verified by
  `report::tests::config_section_is_replaced_not_duplicated_across_writes`.

## Verification

- `trader`: `cargo test --lib --bins` — 187 passed (4 pre-existing, unrelated failures in
  `config`/`config_log` tests traced to a stale config fixture; confirmed present on `main`
  before this change via `git stash`). `cargo fmt --all --check` and
  `cargo clippy --all-targets --all-features -- -D warnings` both clean.
- `siglab`: `cargo test` — 37 passed, 0 failed. `cargo fmt --all --check` and
  `cargo clippy --all-targets --all-features -- -D warnings` both clean.
- Rebuilt and restarted the `siglab` Docker container (not `trader` — see root README).

## Follow-ups

- `trader/src/worker.rs`'s live path was updated for schema consistency but not deployed —
  see root `README.md`'s "Trading engine — known incidents" entry for this change.
