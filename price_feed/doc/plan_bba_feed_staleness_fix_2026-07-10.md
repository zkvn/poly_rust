# Plan — fix silent per-asset staleness in the live Polymarket price feed (bba/book)

Status: **draft, for review** — no code changed yet.

## 1. Background — what happened

On 2026-07-10 the python sibling bot (`btc_5mins`, running in `paper_trade=True` mode on
Oracle) fired two Telegram "Order placed" alerts that never showed up in poly_rust's live
trader (`trader-live.service`, real money):

- **DOGE reversal**, 18:23:35 HKT, UP @ 0.9150 (T-84s, cycle `doge-updown-5m-1783678800`)
- **ETH high_prob**, 18:39:41 HKT, DOWN @ 0.9500 (T-18s, cycle `eth-updown-5m-1783679700`)

Investigation (SSH to Oracle, read-only; full trace in this session's transcript, not yet
written up as a separate incident doc — happy to add one if useful) ruled out gating logic,
`max_trades`, and halt state, and instead found the real cause in the raw feed data itself.
Pulling the collector's own in-progress `.tmp` parquet for hour 18 (via
`price_feed/scripts/recover_rust_parquet.py`, since the hour wasn't sealed yet) and diffing
against `journalctl -u poly-collector` showed:

| Asset | Frozen at | Gap start (HKT) | Gap end (HKT) | Duration |
|---|---|---|---|---|
| DOGE | up=0.4300 dn=0.5700 | 18:21:25.8 | 18:24:51.2 | 205.4s |
| ETH  | up=0.1750 dn=0.8250 | 18:36:38.8 | 18:40:03.4 | 204.6s |

BTC's feed in the **same collector process, same hour** had zero gaps over 1.2s. Both gaps
happened mid-cycle (not at a 5-min slot rollover boundary), lasted almost exactly the same
~205s, and **no `"bba/price stream closed, reconnecting…"` line was ever logged** — confirmed
via `journalctl -u poly-collector --since ... | grep -iE 'closed|reconnect|fail|error|retry'`
returning nothing in either window. So the existing reconnect-on-close logic never had a
chance to fire: the WS stream stayed technically open, it just silently stopped delivering
messages for that one asset's token while continuing to deliver for the others on the same
connection.

**Impact:** the Rust trader has no independent price source — `live` only subscribes to NATS,
which is fed exclusively by `poly-collector`. During each freeze the trader's `latest_poly`
signal for that asset was pinned at the last real value, so `try_enter` (worker.rs) correctly
saw no reversal/high_prob pattern in the (stale) data it had — this was not a trading-logic
bug. Python's bot has its own independent, redundant (WS1+WS2) direct connection to
Polymarket's CLOB, so it saw the real move and fired its (paper) entry.

## 2. Root cause

`price_feed/src/collect.rs::spawn_bba_task` (and, identically shaped,
`spawn_book_task`/`spawn_trade_task`) subscribes to **one shared, multiplexed WebSocket
stream covering every tracked asset's token IDs in a single `subscribe_best_bid_ask(ids)` /
`subscribe_prices(ids)` call**. The merged stream is consumed by one loop that dispatches
each message to the right `AssetState` slot by `asset_id`. There is:

- No per-asset last-seen timestamp check — `AssetState.latest_bba.received_at_ms` is recorded
  on every message, but nothing ever reads it to ask "has this asset gone quiet?"
- No staleness-driven resubscribe — the only recovery path is the `while let Some(...) =
  s.next().await` loop ending (stream close) or the initial `subscribe_*` call returning
  `Err`. A subscription that the server (or SDK) silently stops servicing for one token,
  without closing the connection, matches neither case.
- A single shared connection for all assets, so even the *unaffected* assets' subscriptions
  get torn down and rebuilt together on every 5-min cycle rollover (`slot_rx.changed()`
  aborts and respawns the whole task) — this coupling isn't the cause of the observed
  incidents (both gaps started mid-cycle, not at rollover) but it's an unnecessary shared
  blast radius worth removing at the same time.

We do not have a confirmed explanation for *why* Polymarket's server (or the
`polymarket_client_sdk_v2` WS client) stops delivering for one `asset_id` within a multi-id
subscription while continuing for others — there's no error surfaced anywhere in the stack to
diagnose further from our side. The fix below is a **detect-and-recover safety net**, not a
guarantee that the underlying transient server/SDK hiccup itself can never occur again — see
§6 Risks for why I think this is the right bar rather than overpromising.

## 3. Design goals

1. A stalled per-asset subscription is detected and force-recovered within single-digit
   seconds, not minutes — cutting the ~205s blind window down by >20x.
2. A stall on one asset can never again silently piggyback on/starve another asset's healthy
   subscription.
3. The recovery logic itself is unit-testable without a live WebSocket (matches this
   codebase's existing "sync core, async shell" pattern — worker.rs's `step()`/`try_enter()`
   are pure and tested the same way).
4. When a stall happens, it's visible same-day, not just discoverable by manually diffing raw
   parquet after a user notices a missing trade — both in the collector's own log and in the
   recon report.
5. No regression to normal operation: no resubscribe storms, no added latency on the healthy
   path, no change to the parquet schema.

## 4. Proposed fix

### 4.1 Split the shared multi-asset subscription into one subscription per asset

Change `spawn_bba_task` (and `spawn_book_task`, `spawn_trade_task` for consistency — same bug
class, lower trading impact but same fix) to spawn **one independent task per asset**, each
opening its own `subscribe_best_bid_ask(vec![this_asset_up_id])` /
`subscribe_prices(vec![this_asset_up_id])` pair, instead of one task multiplexing every
asset's IDs over a single connection.

- Removes the shared blast radius: a cycle rollover for one asset's slot no longer tears down
  every other asset's healthy connection.
- Isolates a per-token server-side/SDK hiccup to the one asset it actually affects — it can no
  longer ride along silently on a connection that's otherwise working fine for its neighbors.
- Connection count goes from 1 → N (N=6 today: BTC/ETH/SOL/BNB/XRP/DOGE) for the bba+price
  stream, and similarly for the book stream. Python's `btc_5mins` bot already runs multiple
  independent WS connections (including deliberate WS1+WS2 redundancy per
  `shared_data_process.py`) without issue, so this is not a new class of load on
  Polymarket's infra from this side.

### 4.2 Per-asset staleness watchdog with forced resubscribe

Add a small, pure, unit-tested core:

```rust
/// Pure decision core — no I/O, no tokio. Mirrors worker.rs's sync-core/async-shell split.
struct StalenessWatchdog {
    threshold_ms: i64,
    cooldown_ms: i64,
    last_forced_resubscribe_ms: Option<i64>,
}

enum WatchdogAction {
    Ok,
    ForceResubscribe,
    OkCoolingDown, // stale, but a resubscribe already fired recently — wait
}

impl StalenessWatchdog {
    fn check(&mut self, now_ms: i64, last_received_ms: i64) -> WatchdogAction { ... }
}
```

Wire one `StalenessWatchdog` per asset into the (now per-asset, from §4.1) bba task's own
loop via a `tokio::time::interval` tick alongside the message-receive `select!` arm — no
separate task needed, avoids a second lock-and-poll cycle. On `ForceResubscribe`: abort the
current per-asset stream task, log
`"[STALE] {asset} bba feed silent for {elapsed}ms — forcing resubscribe"`, and loop back to
`subscribe_best_bid_ask`/`subscribe_prices` immediately (skip the existing 2s
`tokio::time::sleep` for this path specifically — that sleep exists for the close/Err path to
avoid hot-looping against a genuinely-down endpoint, but a staleness-triggered resubscribe
should be prompt).

Threshold: **5s**. Normal cadence during an active 5-min cycle is a message roughly every
200–250ms (matches the collector's own `ticker_200ms`/`ticker_250ms` sample rate and what we
saw in the healthy portions of today's parquet data) — 5s is generous enough to avoid false
positives on a genuinely quiet market microsecond, while cutting a 205s blind window to at
worst ~5s (>40x improvement). Cooldown: 3s, to prevent a resubscribe storm if the freshly
reopened subscription takes a moment to deliver its first message.

**Regression check using today's real incident data:** replaying the actual captured
timestamps (DOGE: last real message at `1783678885.8`; ETH: last real message at
`1783679798.8`) through `StalenessWatchdog::check` on a simulated 200ms poll cadence confirms
it fires `ForceResubscribe` at ~5.0–5.2s after the last real message in both cases — i.e.
comfortably inside the incident window, long before the 205s the incidents actually ran.
This becomes an actual test (§5), not just an illustrative claim.

### 4.3 Testability seam for the WS client

`ClobWsClient` (from the external `polymarket_client_sdk_v2` crate) is a concrete struct, not
a trait — there's no existing seam to inject a fake stream that "goes silent but doesn't
close" for an integration-style test of the *whole* reconnect loop. Two options:

- **(a)** Introduce a minimal local trait, e.g. `trait BbaSource { fn subscribe_best_bid_ask
  (&self, ids: Vec<U256>) -> Result<impl Stream<...>>; ... }`, implemented for the real
  `ClobWsClient` and for a test double that can be told to "emit N messages then go silent
  without closing." This is the only way to get real coverage of "silent-but-open stream ->
  watchdog fires -> resubscribe happens" as an automated test rather than a manual/staging
  check.
- **(b)** Skip the trait, keep `StalenessWatchdog` unit-tested in isolation (§5), and treat
  the full wiring (`watchdog.check()` → `task.abort()` → resubscribe) as covered by
  staging/production monitoring only, not CI.

**Recommendation: (a).** It's a small trait (2-3 methods, mechanical to implement for the
real client) and it's the difference between "we're confident this works" and "we're
confident the isolated math works." Flagging this as a real design choice for review rather
than deciding it silently, since it does add a bit of indirection to `collect.rs`.

### 4.4 Visibility — feed_gaps log + recon report

Every time the watchdog fires `ForceResubscribe`, append one line to
`price_feed/log/feed_gaps.log`: `{timestamp},{asset},{feed},{gap_start_ms},{gap_end_ms},
{duration_ms}`. Plain CSV-ish text, append-only, matches the project's existing
plain-log conventions (`live.log`, `recon_cron.log`).

Extend `trader/scripts/trade_reconcile.py`'s daily markdown report with a new section, "Feed
Gaps," reading this log for the report's window and surfacing it the same way "Gamma Timeout"
or "Failed Exit Attempts" already are — so a stale-feed incident shows up automatically in
the report a human already reads every 2 hours, instead of requiring someone to notice a
Telegram mismatch and manually pull raw parquet like this session did. Zero gaps in a window
renders as "None — feed was continuous. 🎯" matching the existing report's tone for clean
sections.

### 4.5 Trader-side defense in depth (secondary layer, phase 2)

Independent of fixing the collector: `live`'s own `latest_poly.age(now)` already knows when
an asset's data is stale (it's what feeds the existing `PolyStale` gate in `gates.rs`). Add a
rate-limited Telegram alert — "⚠️ DOGE price feed stale for {N}s, entries suppressed" — fired
when age exceeds a threshold (e.g. 5s) for an actively-traded asset. This is deliberately
**not** relied on as the primary fix (the collector-side watchdog in §4.2 should make this
rare to never fire), but it protects against a *different* class of gap this incident didn't
happen to hit — NATS itself dropping messages, or Oracle's `nats-server` hiccuping between
collector and trader — which §4.1/4.2 don't cover since they only harden the
collector-to-Polymarket leg. Proposing this as phase 2 so the critical collector fix isn't
gated on trader-side changes; happy to pull it into phase 1 if you'd rather have both at once.

## 5. Test plan

**Unit tests (pure, no tokio/WS — `#[test]`, matches existing `collect.rs` test style):**

1. `watchdog_ok_when_recently_received` — `last_received_ms` within threshold → `Ok`.
2. `watchdog_fires_at_threshold_boundary` — exactly `threshold_ms` elapsed → fires (test both
   `threshold_ms - 1` → `Ok` and `threshold_ms` / `threshold_ms + 1` → `ForceResubscribe`, to
   pin the boundary explicitly rather than leave it implicit).
3. `watchdog_respects_cooldown_after_firing` — two staleness checks inside `cooldown_ms` of
   each other → only the first returns `ForceResubscribe`, the second returns
   `OkCoolingDown`.
4. `watchdog_fires_again_after_cooldown_expires` — third check past `cooldown_ms` → fires
   again if still stale.
5. `watchdog_never_received_treated_as_stale_from_task_start` — `last_received_ms = 0`/`None`
   (asset never got a single message since the subscription began) does not special-case into
   "fresh" — must eventually fire so a subscription that's dead-on-arrival isn't invisible
   forever.
6. **Golden-incident regression** — `watchdog_would_have_caught_todays_doge_gap` and
   `..._eth_gap`: feed the two real `(last_received_ms, simulated now)` sequences from §4.2
   through `check()` on a 200ms poll cadence and assert `ForceResubscribe` fires within
   5.5s of the real last-message timestamp for each. Ties the test suite directly back to the
   incident that motivated it, so it can't silently regress.

**Integration test (needs §4.3(a), the `BbaSource` trait):**

7. `reconnects_when_stream_goes_silent_without_closing` — fake source emits a few messages
   then simply stops (stream stays pending/open) → assert a resubscribe call happens within
   the threshold and a *second* fake stream's messages start flowing afterward.
8. `reconnects_when_stream_closes_cleanly` — existing behavior, regression-guard it explicitly
   now that the loop structure is changing.
9. `retries_with_backoff_when_resubscribe_itself_fails` — fake source's `subscribe_*` returns
   `Err` repeatedly → assert the existing 2s-sleep retry loop still applies on this path (not
   the no-sleep staleness-triggered path) and the task never panics/hot-loops CPU.
10. `one_assets_silence_does_not_affect_another_asset_task` — with per-asset tasks (§4.1),
    starve one fake asset stream and confirm a sibling asset's task keeps delivering messages
    throughout, unaffected — direct regression test for the actual bug ("BTC was fine while
    DOGE/ETH weren't" from today's incident).

**Manual/staging verification (can't reasonably be a CI test):**

- Deploy to Oracle via `scripts/deploy_oracle.py --price-feed-only --dry-run` first, review,
  then real deploy.
- Confirm `poly-collector` restarts clean under `systemctl status`, NATS still flows
  (`ss -tln | grep 4222` per README, plus confirm `trader-live.service` keeps receiving ticks
  — check `live.log` heartbeats resume normally for all assets).
- Watch `price_feed/log/feed_gaps.log` and collector journal for a full trading day
  post-deploy; expect either an empty gaps log or, if a gap does occur, confirm it's now
  measured in single-digit seconds instead of minutes.
- Let the extended recon report (§4.4) run through its normal 2-hourly cron cycle and confirm
  the new "Feed Gaps" section renders correctly on both a clean run and (if we're unlucky
  enough to catch one) an actual gap.

## 6. Rollout order

1. `StalenessWatchdog` (pure struct) + unit tests (§5 items 1-6) — no behavior change yet,
   just the tested decision core.
2. `BbaSource` trait (§4.3) + integration tests (§5 items 7-10) against the fake — still no
   production behavior change.
3. Wire `spawn_bba_task` to per-asset tasks (§4.1) + watchdog (§4.2) for real, using the now
   real-implemented `BbaSource`. Apply the same split to `spawn_book_task`/`spawn_trade_task`
   for consistency (lower urgency — book/trade aren't what the trader's entry signal depends
   on, but same bug class).
4. `feed_gaps.log` + recon report section (§4.4).
5. Deploy to Oracle, verify per the manual checklist above.
6. Phase 2 (separate follow-up, not blocking this fix): trader-side stale-feed Telegram alert
   (§4.5).

## 7. Risks / open questions for your review

- **This hardens detection+recovery; it does not explain or eliminate the underlying
  Polymarket/SDK-side cause.** I don't have visibility into why the server or
  `polymarket_client_sdk_v2` silently stops servicing one `asset_id` — there's no error
  anywhere in the stack. If it happens again post-fix, it should now self-heal in ~5s instead
  of ~205s and get logged/reported either way, but I can't promise the *trigger* itself can
  never occur — only that we'll now always catch and recover from it fast. Wanted to say this
  plainly rather than title the doc "make sure it will never ever happen again" and then
  quietly under-deliver on the literal claim.
- **Connection count**: N=6 independent bba+price connections (12 if book gets the same
  split too) instead of 1-2 shared ones. I don't expect Polymarket to rate-limit this (python
  bot already runs multiple independent connections), but flagging the change in shape since
  it's a real infra difference, not just a code refactor.
- **Threshold tuning (5s stale / 3s cooldown)**: chosen from today's observed healthy cadence
  (~200-250ms) with comfortable margin, but I have no data on quieter market conditions
  (overnight low-volume, e.g.) where a genuine 3-5s gap in `price_change` events might be
  normal rather than a stall. Worth watching the false-positive rate in `feed_gaps.log` for a
  few days post-deploy and retuning if it's noisy.
- **Scope**: I've treated `spawn_book_task`/`spawn_trade_task` as "same fix, lower priority"
  since `book`'s top-of-book is already known-unreliable for these markets (see the existing
  comment above `spawn_bba_task` — "book channel... only ever reports the outermost ticks,
  pinning its midpoint at 0.5") and isn't what `try_enter` actually keys off. Confirm you're
  OK with bba getting the fix first and book/trade following as a fast-follow rather than in
  the same change, if you'd rather keep the initial diff smaller.

---

**Nothing in this plan has been implemented yet — this is for your review before I touch any
`.rs` file.** Let me know which parts to proceed with (all of §4, or start with 4.1+4.2 and
hold 4.4/4.5), and any pushback on the threshold values or the trait-seam approach in §4.3.
