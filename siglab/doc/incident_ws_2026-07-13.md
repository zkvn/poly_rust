# Incident: WebSocket subscription CPU bug, memory investigation, and related findings (2026-07-13)

**Status:** WS/CPU bug — root-caused and fixed, verified. Memory growth — investigated,
plausible explanation found, **not conclusively resolved**. `push_report.sh` bug — found
and fixed. `trader/live.rs` duplicate-subscription pattern — found, real but dormant,
**not fixed** (out of scope to touch `trader/` without explicit go-ahead).

This doc consolidates findings from the same investigation session; see
`siglab/doc/local_resource_test_2026-07-13.md` for the original Docker resource-test data
these findings were pulled from, and `plan_weather_bot.md` for the harness's overall design.

---

## 1. WS subscription CPU bug — root cause, fix, verification

### Symptom

A live 16-minute Docker run at full scale (24 crypto markets + 51 weather cities, ~525
subscribed tokens) showed sustained 200-370% CPU (2-3.7 cores) — not the ~44%-of-one-core a
smaller (12-18 market) run's linear extrapolation had predicted. Evenly spread across all
~16 tokio worker threads (checked via `/proc/1/task/*/stat` — no single hot thread), not a
crash-loop (`RestartCount` stayed 0 throughout).

### Root cause

Traced into `polymarket_client_sdk_v2`'s source
(`~/.cargo/registry/.../polymarket_client_sdk_v2-0.6.0-canary.1/src/ws/connection.rs` and
`src/clob/ws/subscription.rs`, not just inferred from behavior):

- `ConnectionManager` holds **exactly one `broadcast::channel` per WS connection** (one per
  `ChannelType`, e.g. one shared channel for the entire "Market" channel).
- Every call to `subscribe_best_bid_ask()`/`subscribe_prices()` — regardless of which
  token(s) — calls `self.connection.subscribe()` and gets back a **fresh receiver on that
  same shared broadcast channel**, then filters client-side by `asset_id` inside a
  `try_stream! { loop { rx.recv().await ... } }`.
- There is no server-side or connection-level per-subscriber filtering. Every message that
  arrives on the shared connection is broadcast to and re-filtered by **every** subscriber.

siglab's original `weather.rs` called `subscribe_best_bid_ask`/`subscribe_prices` once **per
bucket token** — ~525 tokens × 2 calls ≈ 1,050 subscriptions, i.e. 1,050 independent
receivers all filtering the same broadcast stream. Cost is **O(subscriptions × message
rate)**, not O(subscriptions) as originally assumed.

### Confirmed against official docs, not just the SDK's behavior

Checked `docs.polymarket.com/developers/CLOB/websocket/wss-overview` (2026-07-13): the
market channel is explicitly designed for **one connection subscribed to many `assets_ids`
at once** (`{"assets_ids": [...], "type": "market", ...}`), modifiable without reconnecting
via `"operation": "subscribe"`/`"unsubscribe"` messages. This was a bug in how siglab called
the SDK, not a limitation of the Polymarket API itself.

`price_feed/src/collect.rs` already does this correctly — see §3 below.

### Fix

Rewrote `siglab/src/weather.rs` to batch-subscribe **once per city** (all of that city's
~11 bucket tokens in a single `subscribe_best_bid_ask` call and a single `subscribe_prices`
call), demultiplexing the merged stream locally by `asset_id` — the same pattern as
`price_feed`'s `spawn_bba_task`. ~51 cities × 2 calls ≈ 102 subscriptions instead of ~1,050.

Crypto markets (24 markets × 2 calls ≈ 48 subscriptions) were left as one-call-per-market —
small enough relative to weather's original 1,050 that batching them wasn't worth the added
complexity of merging independent rotation schedules (5m/15m/4h/hourly-ET all rotate on
different clocks).

### Verification

Two live 15-16 minute Docker runs, same full scale (24 crypto + 51 weather cities), before
and after the fix:

| Metric | Before (per-bucket subscribe) | After (per-city batched) |
|---|---|---|
| CPU avg | 221% | **44%** |
| CPU max | 369% | **83%** |

~5x reduction. Real trades continued firing correctly throughout both runs
(`siglab_trades.jsonl`), staleness telemetry behaved correctly, no restarts.

---

## 2. Memory growth — investigated, plausible explanation, not conclusively resolved

### What was observed

The same post-fix 15-minute Docker run that confirmed the CPU fix also showed memory
growing from 50.9 MiB to 434.2 MiB over the 15 minutes — every ~2-minute checkpoint higher
than the last, no observed decrease. An earlier (pre-CPU-fix) 16-minute run showed a similar
climb (79→335 MiB) before one anomalous drop back to 23 MiB (more consistent with the
allocator releasing a large freed block back to the OS than a crash — `RestartCount` stayed
0 in both runs).

### Investigation: isolating weather's contribution

Ran two local (non-Docker, release-build) A/B comparisons, sampling `VmRSS` from
`/proc/<pid>/status` every 20s for ~6 minutes each, same crypto config (24 markets) in both,
varying only the weather city count:

**1 city (hong-kong only):**
```
20s: 31.1 MiB  ...  200s: 38.8 MiB  ...  360s: 38.9 MiB
net growth: 7.8 MiB over 340s — growth stops around 200s, flat for the last 160s.
```

**51 cities (full config):**
```
20s: 83.5 MiB  ...  160s: 140.2 MiB (+56.7)  ...  260s: 168.6 MiB (+28.4)  ...  360s: 168.6 MiB
net growth: 85.1 MiB over 340s — two step-jumps, then flat for the last 100s.
```

### What this shows

1. **Weather city count clearly drives the growth.** Starting RSS is 2.7x higher with 51
   cities (83.5 vs 31.1 MiB — more subscription/connection state), and net growth over the
   same window is ~11x higher (85.1 vs 7.8 MiB). This is not proportional-only-to-uptime
   background growth; it scales with weather monitoring scope.
2. **The growth pattern is stepped and plateauing, not smooth/continuous.** Both runs show
   growth concentrated in a few jumps followed by flat stretches — the 1-city run went
   completely flat for the last ~47% of its window; the 51-city run went completely flat for
   the last ~29%. A genuine per-message leak (one that never stops) would show continuous
   growth with no flat stretches. This pattern instead looks like **allocator-driven working-set
   growth toward some steady-state size** (e.g., a buffer doubling in capacity a few times
   as message volume ramps up, then staying at that capacity) — a known characteristic of
   how allocators like glibc's ptmalloc handle bursty allocation (freed memory isn't always
   returned to the OS immediately, so `RSS`/cgroup `memory.current` overstate live/reachable
   memory).
3. **This is not conclusively resolved.** 6 minutes (local) and 15 minutes (Docker) may
   simply not be long enough to see the *true* plateau at full 24-crypto + 51-weather scale —
   the 51-city local run's two jumps (at 160s and 260s) suggest a longer run could show a
   third jump further out, not necessarily a final plateau at 168 MiB. The honest conclusion
   is: **growth is real, correlates with weather scope, and looks bounded/decelerating rather
   than unbounded — but "looks bounded" over 6-15 minutes is not the same as "confirmed
   bounded" over hours or days.**

### Recommendation

- **Not urgent to block on right now** — even the fastest-observed sustained rate (Run 3's
  ~25 MiB/min, before the stepped-plateau pattern was understood) would take hours to reach
  a size worth worrying about on a normal dev box.
- **Does matter for the current deployment**, since siglab is now a systemd-timer-driven,
  intentionally long-running unattended process. Two honest options, neither applied yet:
  1. **Watch it** — check `docker stats siglab-siglab-1` periodically over the next several
     hours/days to see whether it actually plateaus (consistent with the stepped-growth
     theory) or keeps climbing (would mean the theory is wrong and this is a real leak).
  2. **Add a pragmatic mitigation regardless of root cause** — a scheduled container restart
     (e.g. daily, via the existing `restart: unless-stopped` policy plus an external
     `docker compose restart` cron/timer) bounds worst-case memory without needing to fully
     understand the mechanism first. Not implemented in this pass — a judgment call on
     whether to add proactive-restart infrastructure before or after confirming the actual
     growth curve.
- **If it needs deeper investigation later:** heap-profile the container with `dhat` (a
  Rust-native heap profiler, cheap to add as a dev-dependency) or `heaptrack`, or try
  swapping to `jemalloc` (known for returning freed memory to the OS more eagerly than
  glibc's default allocator) as a quick experiment to see if it changes the growth curve —
  neither attempted in this pass given time already spent and the "not urgent" assessment.

---

## 3. Cross-crate audit: same bugs in `price_feed` or `trader`?

Read-only investigation — nothing in `price_feed/` or `trader/` was modified.

### `price_feed` — clean, has neither bug

- **Subscription batching:** `price_feed/src/collect.rs`'s `spawn_book_task`/`spawn_bba_task`
  already batch every currently-relevant token into one `subscribe_orderbook`/
  `subscribe_best_bid_ask`/`subscribe_prices` call per slot-rotation cycle, demuxed locally
  via a `map: Vec<(U256, usize, bool)>` lookup — exactly the pattern siglab's `weather.rs`
  now follows after the fix in §1, not the per-token pattern it had before the fix.
- **Duplicate Binance connections:** `spawn_binance_task` is called once per *asset*, with
  an explicit code comment confirming this is deliberate: "Binance feed (asset-level,
  period-independent — one WS + one writer per asset, shared across durations)."

### `trader/src/bin/live.rs` — has the same duplication pattern, but dormant in production

**What the bug is:** `live.rs` builds one "worker" per `(asset, strategy)` pair — so an asset
running two strategies (e.g. ETH currently runs both `reversal` and `high_prob`, per
`trader/config/strategy_20260709.toml`'s `[strategies]` table) gets two separate worker
objects. The code that opens each worker's Binance and CLOB WebSocket subscriptions lives
inside the per-worker loop (`for asset in &args.asset { for strategy in &params.strategies {
... spawn_binance_task(...) ...; ... PolySub::start(...) ... } }`), not a per-asset loop —
so each of ETH's two workers would independently open its own subscription to the *same*
ETH data, instead of sharing one.

**Impact if triggered:** for any asset running 2+ strategies, duplicate WebSocket
subscriptions to identical data — wasted connections, and (per §1's finding) the same class
of client-side broadcast-and-filter CPU overhead siglab just fixed, just at a much smaller
scale (a couple of duplicate subscriptions per multi-strategy asset, not hundreds).

**Why it doesn't affect production today:** both subscription call sites are gated behind
`if args.nats_url.is_none()`. The alternative to direct WS subscription is reading ticks
from NATS — a message bus `price_feed` publishes into once per tick. `../docker-compose.yml`'s
`trader` service always passes `--nats-url nats://localhost:4222`, so `args.nats_url` is
always `Some(...)` in production, meaning `args.nats_url.is_none()` is always `false`, and
the duplicating code path physically cannot execute under the current deployment. In the
NATS path, price_feed publishes each tick once and every worker (however many share an
asset) subscribes to the same NATS subject — NATS's own pub/sub fan-out handles this
efficiently and doesn't have the duplication problem at all.

**Not fixed** — real bug, but dormant given current deployment config, and modifying
`trader/` source is out of scope for this session without explicit go-ahead. Flagged in
`README.md`'s `## TODO` for whoever next touches `live.rs`'s non-NATS fallback path.

---

## 4. Bonus finding: `push_report.sh` fatal-errored on its first two real runs

Found while checking whether the autonomous hourly report+push was actually working (it
wasn't, yet — separate from everything above). `journalctl --user -u
siglab-report-push.service` showed both real timer firings (14:05 and 15:05 HKT) failing
with exit code 128.

**Root cause:** `git add siglab/doc/report/signal_report_*.md` is a **fatal git error**
(not a silent no-op) when the glob pathspec matches zero files — which was the case both
times, since no report had been written yet. The script's existing `git diff --cached
--quiet` check only handled "files matched but nothing changed," not "nothing matched at
all," and `set -euo pipefail` meant the whole script died before reaching that check.

**Why this kept happening:** siglab writes its first hourly report **one full interval
after container start** (deliberately, to avoid writing an empty report before discovery
completes — see `main.rs`'s `report_task` comment). Every container restart during this
session's testing (there were several, for rebuilding with fixes) reset that one-hour clock,
so the timer fired against an empty report directory more than once.

**Fix:** check for zero-match explicitly with `shopt -s nullglob` before calling `git add`,
exit 0 early if nothing exists yet. Verified against both the empty-directory case and a
real generated report — the full commit+push cycle completed successfully
(`siglab: hourly signal report update (2026-07-13T07:39Z)`, pushed as commit `2694dd0`).

---

## 5. Second bonus finding (same day, later): the hourly push was silently failing on auth

User noticed no report commits had landed in a while and asked to check. `journalctl --user
-u siglab-report-push.service` showed the timer firing correctly every hour, but real
firings (once §4's empty-directory bug was fixed and a real report existed) were failing
with exit 128:

```
sign_and_send_pubkey: signing failed for ED25519 "/home/kev/.ssh/id_ed25519" from agent: agent refused operation
git@github.com: Permission denied (publickey).
```

**Root cause:** systemd `--user` services do not inherit the interactive shell's
`SSH_AUTH_SOCK`. They get the systemd user manager's own default —
`/run/user/<uid>/gcr/ssh` (GNOME Keyring's SSH agent proxy) on this box — which either
doesn't have the git-push key loaded or refuses to use it non-interactively. Reproduced
directly: running `git ls-remote` in a shell with `SSH_AUTH_SOCK` unset (simulating the
systemd environment) hit the identical `Permission denied (publickey)` error.

**Why this wasn't obviously broken in git history before now:** the script's `git commit`
step succeeds regardless (that's local, no network/auth needed) — only `git push` failed.
The orphaned local commit didn't stay orphaned because Claude's own manual `git push` calls
later in the session (for unrelated feature work) swept it up as a side effect, since `push`
sends the whole branch history, not just the newest commit. That safety net disappears
whenever Claude isn't actively working in the repo, which is exactly when the autonomous
push is supposed to matter.

**Fix (user's explicit choice, after being offered a more robust but more involved
alternative — a dedicated no-passphrase deploy key):** point the systemd service at the
same `SSH_AUTH_SOCK` the interactive shell already uses, injected by
`install_timer.sh` at install time (the repo's committed `.service` file keeps a
`__SSH_AUTH_SOCK__` placeholder, never a real path, since the actual socket is
session-specific — baking a real path into a git-tracked file would be both wrong for
anyone else and stale the moment this login session ends).

**Known limitation, accepted deliberately:** this socket is tied to the current login
session (confirmed: the backing `ssh-agent`/`gcr-ssh-agent` processes started at this GDM
session's login, not at boot). It will not survive a reboot or logout — lingering
(`loginctl enable-linger`, already enabled) keeps the systemd *user manager* alive across
logout, but not this specific interactive agent. If hourly pushes silently stop again after
a reboot/re-login, re-run `siglab/scripts/install_timer.sh` from a shell where `ssh -T
git@github.com` already works, to pick up the new socket.

**Verified end-to-end:** re-ran `install_timer.sh`, then manually triggered the service
(`systemctl --user start siglab-report-push.service`) rather than waiting for the next
scheduled firing — it authenticated and pushed for real
(`siglab: hourly signal report update (2026-07-13T13:16Z)`, commit `7439045`), entirely
through the systemd path, no manual `git push` involved.

---

## Summary of what shipped this session

| Finding | Status | Where |
|---|---|---|
| Weather subscription CPU (O(n²)-ish fan-out) | **Fixed, verified (~5x CPU reduction)** | `siglab/src/weather.rs` |
| Memory growth under full load | Investigated, plausible (allocator retention) explanation, **not conclusively resolved** | this doc §2 |
| `price_feed` subscription/connection patterns | Audited — clean, no bug | this doc §3 |
| `trader/live.rs` duplicate subscriptions | Found, real, **dormant in production**, not fixed | this doc §3, `README.md` TODO |
| `push_report.sh` fatal error on empty report dir | **Fixed, verified end-to-end** | `siglab/scripts/push_report.sh` |
| Hourly push failing on SSH auth (systemd `SSH_AUTH_SOCK` mismatch) | **Fixed, verified end-to-end** — known to need re-running `install_timer.sh` after reboot/re-login | this doc §5, `siglab/scripts/install_timer.sh` |
