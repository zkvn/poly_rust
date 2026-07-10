# Plan — fix silent per-asset staleness in the live Polymarket price feed (bba/book)

Status: **phase 1 (observe-only) implemented and deployed to Oracle, 2026-07-10 ~20:55 HKT.**
Phase 2 (the actual recovery action) is **not implemented** — see §8 below for why, and don't
build it from §4's original design as written; §4 is kept for the record but was disproven in
production. Read §8 first if you're picking this back up.

## 0. 2026-07-10 update: the first implementation caused a production incident

§4 below (as originally written) was implemented, tested locally (18 passing tests, clippy/
fmt clean), and deployed to Oracle via `scripts/deploy_oracle.py --price-feed-only`. Two
problems surfaced, one caught before the second deploy attempt, one after:

1. **A real bug, caught in production, fixed same session**: `spawn_bba_task` is called with
   an empty `assets: Vec<String>` for the 15m/4hr feeds (they don't publish to NATS, so no
   asset names are needed — see their call sites in `run()`), but the per-asset task table
   was sized off `assets.len()` while indexed by the token-slot loop (sized off the tracked-
   asset count, `n`). Panicked immediately on every restart: `index out of bounds: the len is
   0 but the index is 0`. Fixed by sizing the task table off the token-slot count instead —
   see `run_bba_task_loop`'s doc comment and the regression test
   `spawns_correctly_when_assets_list_is_shorter_than_token_slots` in the (now superseded,
   see below) implementation.
2. **A design flaw, caught in production, not fully fixable by patching**: after the panic
   fix, redeployed, and the watchdog immediately started firing `[STALE] ... forcing
   per-asset resubscribe` roughly every 5 seconds for nearly every asset across all three
   durations — BTC, ETH, DOGE, SOL, BNB, XRP, HYPE, continuously, not just during a real
   outage. This is **worse** than the original bug: a continuous resubscribe storm hammering
   the shared connection, instead of one rare 205s gap.

   Root cause: `best_bid_ask`/`price_change` are *change* events, not a periodic heartbeat —
   Polymarket only sends a message when the price actually moves. The plan's original 5s
   threshold was calibrated only from watching BTC/ETH/DOGE during an actively-moving window
   in the historical incident data (§1's table below). In practice, plenty of legitimate quiet
   stretches — untraded assets (SOL/BNB/XRP/HYPE), the 15m/4hr durations, and (per the user,
   confirmed by this incident) the quiet minute or two right after a cycle opens before price
   action typically picks up — go well past 5s with genuinely nothing to send. **A raw silence
   timer cannot distinguish "broken" from "quiet" for a change-event stream.** There is no
   threshold that's both fast enough to matter and safe against every asset's and duration's
   normal quiet stretches.

   **Rolled back same session**: `git stash`'d the change (stash entry `wip: bba staleness
   watchdog - false-positive storm, rolling back to redesign` — kept locally, not pushed, as
   an audit trail of what was tried and disproven), rebuilt and redeployed the prior known-
   good binary, confirmed `poly-collector` active with zero errors/panics and the trader's
   `live.log` showing normal heartbeats again.

See §8 for the redesign this led to (implemented, phase 1 only) and §9 for research on the
correct fix (phase 2, deferred).

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
§7 Risks for why I think this is the right bar rather than overpromising.

### 2.1 SDK internals — researched before designing the fix

Before proposing "one WebSocket connection per asset" (an earlier draft of this plan), I
checked whether that's actually the right shape, given general WS best practice strongly
favors one multiplexed connection over many (lower handshake/rate-limit overhead, matches how
Polymarket's own protocol is designed — a single `{"assets_ids": [...], "type": "market"}`
subscribe message takes an array). Rather than guess, I read the vendored SDK source directly
(`~/.cargo/registry/.../polymarket_client_sdk_v2-0.6.0-canary.1/src/clob/ws/`, `src/ws/`) —
this crate is what `price_feed` actually depends on (`Cargo.toml`: `polymarket_client_sdk_v2
= "0.6.0-canary.1"`), so this is ground truth for what's available, not general docs.

**Findings:**

- `Client::get_or_create_channel(ChannelType::Market)` (`clob/ws/client.rs`) lazily opens
  **one WebSocket connection per channel type**, and every `subscribe_*` call — orderbook,
  prices, best_bid_ask, tick_size_change, midpoints — shares one underlying
  `subscribed_assets: DashMap<U256, usize>` refcount table (`clob/ws/subscription.rs`,
  `SubscriptionManager`). Two different callers subscribing to the same asset just increment
  a refcount; "only send subscription request for new assets" is a direct code comment there.
  **The SDK already multiplexes every asset over one connection by design** — splitting into
  N raw connections would fight this, not complement it, for no real benefit.
- It also already exposes the exact primitive we need: `Client::unsubscribe_orderbook(&[id])`
  (which `unsubscribe_prices`/`unsubscribe_tick_size_change`/`unsubscribe_midpoints` all alias
  — they share the same refcount pool) decrements that asset's count and, **only once it hits
  zero, sends a real "unsubscribe" wire message for just that asset** over the live
  connection. Calling `subscribe_best_bid_ask`/`subscribe_prices` again afterward is treated
  as "new" again and sends a fresh genuine "subscribe" message — again scoped to just that
  asset, same connection, no reconnect. **This means a single stale asset can be recovered in
  place, without touching the shared connection or any other asset's subscription, using the
  SDK's own existing API** — no new connections, no new protocol messages we'd have to hand-
  roll.
- **Sharp edge to test explicitly**: refcounting is call-for-call, and `spawn_bba_task`
  currently calls *both* `subscribe_best_bid_ask(ids)` and `subscribe_prices(ids)` for the
  same asset list — that's **2 increments per asset**. A forced refresh must call
  `unsubscribe_orderbook` twice for the stale asset to actually zero its refcount and trigger
  a real wire unsubscribe; call it once and the SDK silently keeps the asset "subscribed"
  internally with no message sent — our fix would appear to run (logged, watchdog fired) but
  silently do nothing. This is exactly the kind of failure mode this whole effort exists to
  eliminate, so it gets its own dedicated test (§5, item 7).
- **Confirms the app-level watchdog is necessary, not redundant with SDK behavior**: the SDK
  has its own connection-level heartbeat (`ws/config.rs`: 5s ping interval, 15s pong timeout)
  and reconnection handler (`subscription.rs::start_reconnection_handler`) that calls
  `resubscribe_all()` — but only on a genuine `ConnectionState` transition (transport
  disconnect/reconnect). Both of today's incidents kept the transport completely healthy
  (BTC, sharing the same connection, never blipped) — this is a subscription-level staleness
  that a transport-level heartbeat structurally cannot see. Nothing in the SDK would have
  caught this on its own; the fix has to live in `price_feed`.

**Best-practice research (web):** general guidance for WS APIs with many symbols agrees:
multiplex on one connection, don't open one socket per symbol — matches what the SDK already
does internally, and matches this plan's revised §4.1 below. Polymarket's own docs confirm
the market channel is designed for array-based multi-asset subscription and describe the
5s/10s ping-pong contract (consistent with what's in the SDK's `ws/config.rs`, modulo a small
docs/impl mismatch — SDK default is 15s pong timeout vs. docs stating 10s; not something this
plan needs to change, just noting the discrepancy). Sources at the bottom of this doc.

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

### 4.1 Keep the one shared connection; recover one asset at a time on it

**Revised from an earlier draft of this plan**, which proposed splitting into N independent
WebSocket connections (one per asset). Per §2.1, that fights the SDK's own design (it already
multiplexes every asset over one connection) and general WS best practice, for no benefit the
SDK doesn't already give us for free. The corrected design:

Keep `spawn_bba_task` subscribing every asset over the **one shared connection**, exactly as
today — but restructure it from one monolithic merged-stream loop into **one lightweight
per-asset sub-task**, each holding its own filtered `subscribe_best_bid_ask(vec![this_id])` +
`subscribe_prices(vec![this_id])` pair. Since these calls share the SDK's underlying
connection and refcount table (§2.1), this costs **zero additional WebSocket connections** —
it's still exactly one socket to Polymarket for the whole bba+price feed. What it buys us:
each asset's stream is now an independent Rust-level handle that can be individually torn
down and rebuilt without touching its neighbors.

On a staleness detection (§4.2) for one asset:
1. Call `client.unsubscribe_orderbook(&[stale_id])` **twice** (matching the 2 subscribe calls
   made for that asset — see §2.1's refcount sharp edge) to zero its refcount and trigger a
   real per-asset "unsubscribe" wire message.
2. Drop that asset's old filtered `Stream` handles.
3. Call `subscribe_best_bid_ask(vec![stale_id])` + `subscribe_prices(vec![stale_id])` again —
   a fresh "subscribe" wire message for just that asset, same connection.

No connection is closed or reopened; BTC/ETH/SOL/BNB/XRP's subscriptions are never touched by
DOGE's recovery cycle. This also removes the *unrelated* shared-blast-radius issue the
original draft correctly flagged (today, a cycle rollover for any one asset's slot currently
aborts and rebuilds the whole merged task for every asset) — under the per-asset-sub-task
structure, one asset's slot-driven resubscribe at cycle rollover no longer touches the others
either, as a side effect of the same restructuring, not a separate change.

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

Wire one `StalenessWatchdog` per asset into that asset's own per-asset sub-task (from §4.1)
via a `tokio::time::interval` tick alongside the message-receive `select!` arm — no separate
task needed, avoids a second lock-and-poll cycle. On `ForceResubscribe`: log
`"[STALE] {asset} bba feed silent for {elapsed}ms — forcing per-asset resubscribe"`, run the
`unsubscribe_orderbook` ×2 → drop old streams → `subscribe_best_bid_ask`+`subscribe_prices`
sequence from §4.1 for that asset only, and continue — no connection abort, no
`tokio::time::sleep` needed on this path (that sleep is for the close/Err retry path, which
guards against hot-looping a genuinely-down endpoint; a staleness-triggered per-asset
resubscribe is a normal, expected operation and should be prompt).

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

`Client`/`ClobWsClient` (from the external `polymarket_client_sdk_v2` crate) is a concrete
struct, not a trait — there's no existing seam to inject a fake stream that "goes silent but
doesn't close," or to assert exactly how many times `unsubscribe_orderbook` was called, for an
integration-style test of the *whole* reconnect loop. Two options:

- **(a)** Introduce a minimal local trait covering exactly the calls `spawn_bba_task` makes —
  `subscribe_best_bid_ask`, `subscribe_prices`, `unsubscribe_orderbook` — implemented for the
  real `Client<Unauthenticated>` and for a test double that can (i) emit N messages then go
  silent without closing, and (ii) record every `subscribe_*`/`unsubscribe_orderbook` call so
  a test can assert the exact refcount-parity sequence from §4.1 (2 unsubscribes before the
  resubscribe, not 1). This is the only way to get real coverage of both "silent-but-open
  stream → watchdog fires → resubscribe happens" *and* the refcount sharp edge as automated
  tests rather than manual/staging checks.
- **(b)** Skip the trait, keep `StalenessWatchdog` unit-tested in isolation (§5), and treat
  the full wiring (detect → unsubscribe ×2 → resubscribe) as covered by staging/production
  monitoring only, not CI.

**Recommendation: (a).** It's a small trait (3 methods, mechanical to implement for the real
client) and it's the difference between "we're confident this works" and "we're confident the
isolated math works" — and given §2.1's refcount sharp edge is exactly the kind of thing that
looks correct in review but silently no-ops at runtime, I'd rather have a test pinning the
call count than trust careful reading alone. Flagging this as a real design choice for review
rather than deciding it silently, since it does add a bit of indirection to `collect.rs`.

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

**Integration test (needs §4.3(a), the local subscription-source trait):**

7. `forced_refresh_unsubscribes_exactly_twice_before_resubscribing` — direct regression test
   for §2.1's refcount sharp edge: assert the recovery sequence calls
   `unsubscribe_orderbook(&[id])` exactly **twice** (once for the `subscribe_best_bid_ask`
   increment, once for `subscribe_prices`) *before* issuing the fresh subscribe calls for that
   asset — and, using the fake's own refcount bookkeeping, assert a real "unsubscribe" record
   was produced (i.e. the fake's simulated refcount actually reached zero) rather than merely
   asserting the call count blindly.
8. `reconnects_when_stream_goes_silent_without_closing` — fake source emits a few messages
   then simply stops (stream stays pending/open) → assert the full unsubscribe-then-resubscribe
   sequence fires within the threshold and a *fresh* fake stream's messages start flowing
   afterward.
9. `reconnects_when_stream_closes_cleanly` — existing behavior, regression-guard it explicitly
   now that the loop structure is changing.
10. `retries_with_backoff_when_resubscribe_itself_fails` — fake source's `subscribe_*` returns
    `Err` repeatedly → assert the existing 2s-sleep retry loop still applies on this path (not
    the no-sleep staleness-triggered path) and the task never panics/hot-loops CPU.
11. `one_assets_silence_does_not_affect_another_asset_task` — with per-asset sub-tasks (§4.1),
    starve one fake asset stream and confirm a sibling asset's task keeps delivering messages
    *and its subscription is never touched* (assert zero `unsubscribe_orderbook` calls for the
    healthy asset) throughout — direct regression test for the actual bug ("BTC was fine while
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
2. Local subscription-source trait (§4.3) + integration tests (§5 items 7-11) against the
   fake, including the refcount-parity test (item 7) — still no production behavior change.
3. Restructure `spawn_bba_task` into per-asset sub-tasks sharing the one connection (§4.1) +
   wire in the watchdog (§4.2) for real, using the now real-implemented trait impl. Apply the
   same restructuring to `spawn_book_task`/`spawn_trade_task` for consistency (lower urgency —
   book/trade aren't what the trader's entry signal depends on, but same bug class, and same
   "one shared connection, many refcounted asset subscriptions" SDK model applies).
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
- **Refcount parity is the sharpest edge in this design** (§2.1, §4.3 item 7): the recovery
  path only works if we unsubscribe exactly as many times as we subscribed for that asset. If
  `spawn_bba_task`'s subscribe calls ever change shape (e.g. a third `subscribe_*` call gets
  added for the same asset list later) without updating the matching unsubscribe count, the
  forced-refresh path silently degrades to a no-op — no panic, no error, just a "recovered"
  log line that didn't actually recover anything. Item 7's test asserts the fake's simulated
  refcount reaches zero, specifically to catch this, but it's worth flagging as an ongoing
  maintenance hazard, not just a one-time implementation risk.
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

**§§1–7 above are the original plan, kept for the record. They were disproven in production
(§0) — §4's "5s threshold, force resubscribe" design is not what's deployed. Read on.**

## 8. What's actually implemented now: phase 1, observe-only

`price_feed/src/staleness.rs` is now a small, pure module (`buckets_to_log`, 6 escalating
silence thresholds from 10s to 300s) wired into `collect.rs::run()`'s existing `ticker_200ms`
sampler loop for the 5m feed only (`state_5m` — matches this plan's existing scoping choice to
handle the trading-critical duration first). For each asset, every 200ms it checks whether
`AssetState.latest_bba.received_at_ms` has advanced since the last check; if not, and enough
silence has accumulated to cross a new threshold, it logs
`[OBSERVE-STALE] {asset} bba feed silent for >={bucket}ms (actual {silent_ms}ms) — logging
only, no action taken` to the collector's journal. **It takes no recovery action of any
kind** — no unsubscribe, no resubscribe, nothing touches the live subscription. This is
deliberately as safe a change as could be made: read-only against already-existing shared
state, additive to an existing loop, zero behavior change to anything else. Deployed and
verified stable (§0).

Purpose: collect real per-asset, per-market-phase silence-gap data — including the "quiet
right after cycle open, busier as it progresses toward resolution" pattern the user pointed
out, which the failed phase-1-that-wasn't (§0) inadvertently proved is real and underestimated
— so phase 2's actual trigger (§9) can be sized and validated against real production data
instead of another guess from a handful of historical incident timestamps.

**Next step, not yet started**: after this has run long enough to accumulate a meaningful
sample of `[OBSERVE-STALE]` lines across assets/durations/market-phases, review them
(`journalctl -u poly-collector | grep OBSERVE-STALE`) to see the real distribution of quiet-
period lengths per asset before deciding phase 2's reconciliation interval/tolerance.

## 9. Phase 2 (deferred): REST reconciliation, not a silence timer

Researched (§0's actual problem — telling "broken" apart from "quiet" — needed a real answer,
not a retuned constant) rather than guessed. The industry-standard pattern for this exact
class of problem (a WS feed that's only reliable for *changes*, where silence is ambiguous) is
**REST snapshot reconciliation**, not a refined silence timer:

- Track quote age continuously per symbol; move a symbol from a "live" state toward
  "degraded"/"stale" as its age grows — but **the actual stale determination comes from
  cross-checking against a fresh REST poll's ground truth, not from age/silence alone.** A
  price is only usable when the application also knows its source and age, so both are kept
  attached to every quote, and a stale/live transition is a state machine driven by that
  cross-check, not a bare timeout. ([insightbig.com](https://www.insightbig.com/post/real-time-market-data-fails-quietly-here-s-how-to-make-it-recoverable))
- Concretely for crypto/exchange feeds: bootstrap from a REST snapshot, maintain via WS, and
  when a symbol's WS updates slow down, request a fresh REST snapshot and compare — only a
  genuine mismatch (not just elapsed time) confirms real staleness; a symbol that's
  legitimately quiet will have a REST snapshot that agrees with the still-cached WS value, so
  it produces no false alarm no matter how long the silence runs.

  This is exactly the property §0's incident showed matters: a market that's quiet because
  nothing is happening has a *stable* true price the whole time — REST and cached-WS would
  still agree — so this approach structurally cannot false-positive on a quiet market the way
  a silence timer did.

Applied to this project: Polymarket's CLOB REST API has `GET /midpoint?token_id=...` →
`{"mid_price": "0.45"}` — no auth required for reads
([docs.polymarket.com](https://docs.polymarket.com/api-reference/data/get-midpoint-price)).
Proposed phase 2 design (not implemented):

1. Every N seconds (candidate: 20–30s, informed by §8's observed data before committing),
   poll `/midpoint` for each 5m asset's up-token via `reqwest` (already a dependency, already
   used for Gamma calls elsewhere in this file).
2. Compare the REST midpoint against the WS-cached `(best_bid + best_ask) / 2`. Only flag
   staleness if they diverge beyond a tolerance (candidate: a few cents) — not from silence
   duration at all. A quiet-but-healthy asset's REST midpoint will match the stale cache
   almost exactly, producing no false trigger regardless of how long it's been quiet.
3. On a genuine mismatch, run the same per-asset unsubscribe(×2)+resubscribe recovery §4
   designed (that part of the original design — the *recovery mechanism* itself, as opposed
   to the *trigger* — was never the problem; only the timer-based trigger was).
4. Rate limit: 6 assets polled every 20–30s is ~0.2–0.3 req/s to a public, unauthenticated
   REST endpoint — far below anything Polymarket would plausibly rate-limit, especially
   compared to the WS message volume already flowing.

This is real, non-trivial new implementation work (a REST poller, comparison logic, wiring
the existing recovery code from §4 behind a materially different trigger) — proposing it here
for review, not building it yet, per your "observe-only first" call. Happy to scope and build
this once §8's data gives us real numbers to validate the design against, or sooner if you'd
rather not wait.

## Sources

Earlier research (validated §4.1's "one shared connection" design, still correct — only the
silence-timer trigger in §4.2 was wrong, not the connection architecture):

- [Polymarket CLOB WebSocket — Overview](https://docs.polymarket.com/developers/CLOB/websocket/wss-overview) — market channel subscribe payload shape (`assets_ids` array), ping/pong contract.
- [Polymarket `agent-skills` — websocket.md](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)
- [`Polymarket/rs-clob-client-v2` — GitHub](https://github.com/Polymarket/rs-clob-client-v2) — upstream repo for the `polymarket_client_sdk_v2` crate this project depends on (source read directly from the vendored `~/.cargo/registry` copy, version `0.6.0-canary.1`, for §2.1's findings — not from this repo page, which doesn't render the same detail as the actual source).
- [`GoPolymarket/polymarket-go-sdk` ws package docs](https://pkg.go.dev/github.com/GoPolymarket/polymarket-go-sdk/pkg/clob/ws) — confirms the subscribe/unsubscribe-by-asset pattern exists as a general convention across Polymarket's own SDKs, not just this Rust crate.
- Hacker News discussion on WS multiplexing vs. per-stream connections (general best-practice framing, not Polymarket-specific): [news.ycombinator.com/item?id=39755924](https://news.ycombinator.com/item?id=39755924)

Research for §9 (the phase-2 redesign, after §0's production incident):

- [Real-Time Market Data Fails Quietly. Here's How to Make It Recoverable](https://www.insightbig.com/post/real-time-market-data-fails-quietly-here-s-how-to-make-it-recoverable) — source/age/state metadata pattern for telling stale data from fresh data, cited in §9.
- [Get midpoint price — Polymarket Documentation](https://docs.polymarket.com/api-reference/data/get-midpoint-price) — the `GET /midpoint` REST endpoint §9's design is built on.
- General REST-reconciliation pattern for stale WS symbols (sequence validation, age tracking, snapshot verification, explicit state transitions) — synthesized from multiple crypto-exchange API design discussions found via web search 2026-07-10, no single canonical source.
