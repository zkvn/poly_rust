# Investigation — the daily "Data quality digest" Telegram message

Not an incident in the "something broke" sense — a requested look at what the automated daily
digest is actually reporting, whether the numbers are concerning, and how they connect to
`trader/doc/incident_eth_trade_2026-07-21.md` #2.

## The digest that prompted this

```
⚠️ Data quality digest (last 24h)

CLOB (bba)
⚠️ 1263 event(s)
  BNB: 126 event(s), worst ≥60s
  BTC: 157 event(s), worst ≥120s
  DOGE: 142 event(s), worst ≥60s
  ETH: 192 event(s), worst ≥60s
  HYPE: 305 event(s), worst ≥120s
  SOL: 174 event(s), worst ≥60s
  XRP: 167 event(s), worst ≥120s

Binance (bookTicker)
⚠️ 25 event(s)
  BNB: 1 event(s), worst ≥10s
  DOGE: 23 event(s), worst ≥10s
  XRP: 1 event(s), worst ≥10s

logging only — no recovery action taken
```

Sent by `price_feed/scripts/data_quality_digest.sh` (`data-quality-digest.timer`, daily) —
counts `[OBSERVE-STALE]` lines from `poly-collector`'s journal over the last 24h, bucketed per
asset into escalating silence thresholds (10s/30s/60s/120s/200s/300s, `price_feed/src/
staleness.rs::OBSERVE_BUCKETS_MS`).

## What "OBSERVE-STALE" actually measures — and what it doesn't

**This is a silence-duration counter, not a correctness check.** `best_bid_ask`/`price_change`
are *change* events on Polymarket's WS — the exchange only sends a message when a price actually
moves, not a periodic heartbeat. A market with no trading interest for two minutes is
indistinguishable, from silence alone, between "genuinely quiet, price is still correct" and
"the feed broke." `staleness.rs`'s own doc comment is explicit about why this module took no
action at all: a stricter version of this exact idea (`StalenessWatchdog`, forcing a
resubscribe on 5s of silence) was deployed 2026-07-10 and immediately false-positive-stormed —
rolled back the same day (`price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md` §0). What's
live today is deliberately just telemetry: log the bucket crossings, take no action, so the real
fix could be sized from real data instead of another guess.

**The actual correctness check is a separate, already-deployed mechanism**: `price_feed/src/
reconcile.rs`. Every 5s per 5m asset, it polls Polymarket's REST `GET /midpoint` (ground truth)
and compares against the WS-cached mid. Only a **confirmed** mismatch (3 consecutive polls, not
one — filters out a single noisy read) beyond a 0.03 tolerance is treated as real staleness, and
the recovery is `std::process::exit(1)` (relying on `poly-collector.service`'s
`Restart=always`/`RestartSec=5`, the same already-established pattern the codebase uses
elsewhere for "fatal, let systemd restart cleanly" — see §10 of the 2026-07-10 plan doc for why
a surgical per-asset resubscribe was rejected: the SDK refcounts one asset's subscription across
4 separate registrations, and unsubscribing the wrong count is a silent no-op recovery). This
mechanism *does* take action, and it's judged against real ground truth, not silence duration —
so it structurally can't false-positive on a market that's merely quiet (REST and the stale
cache would still agree).

**So the digest's 1263/25 numbers are a ceiling, not a defect count.** They're everything that
went quiet for a while, most of which is expected to be ordinary market quietness. The question
worth actually answering is: how much of that silence was also *wrong*, per the mechanism that
checks?

## Checked Oracle's logs directly for the same window

```
RECONCILE-STALE count, last 24h: 8 log lines (2 lines per confirmed episode — the trigger and
                                   "flushing writers" follow-up) → 4 distinct episodes
poly-collector NRestarts:         6 (systemctl show; slightly wider window than the 24h grep)
OBSERVE-STALE raw count:          1275 (close to the digest's 1263 — small window-edge diff)
```

The 4 confirmed episodes in the last 24h, with real mismatch magnitudes:

| Time (approx) | Asset | REST mid | Cached mid | Diff |
|---|---|---|---|---|
| 07-21 12:10:12 | BNB | 0.5000 | 0.9750 | **0.475** |
| 07-21 21:29:47 | BNB | 0.9550 | 0.7200 | **0.235** |
| 07-22 00:19:48 | HYPE | 0.0050 | 0.0500 | 0.045 |
| ~07-22 | SOL | 0.0050 | 0.0550 | 0.050 |

(A slightly wider, non-24h-bounded pull also showed one more BNB episode, `rest_mid=0.03
cached_mid=0.11, diff=0.08` — outside the exact 24h window used for the digest comparison above,
included here only to show BNB is the asset seeing this most often recently, 3 of the last 5
episodes.)

These are **not** borderline, tolerance-boundary mismatches — every one is many multiples of the
0.03 tolerance (up to 47.5¢ on a 0–1 price). These are genuine "the cached WS value was
seriously wrong" events, correctly caught and auto-recovered via a clean process restart.

Worth flagging one near-miss in time: the 21:29:47 BNB episode landed **~10 minutes before**
`trader/doc/plan_aggressive_taker_entry_2026-07-21.md`'s XRP `TIMEOUT` trade at 21:39:22
(the evaluation doc's own "wall-clock timeout fired correctly, within ~1.1s of `unwind_time_rev`"
example). `poly-collector` is one process for every asset — a `RECONCILE-STALE`-triggered exit
briefly interrupts *every* asset's NATS publish stream during the restart, not just the
triggering asset's. Not claiming that specific restart caused that specific trade's book to go
quiet (didn't cross-reference tick-level data to confirm it), but it's exactly the kind of
event — process-wide, no NATS ticks for a few seconds — that the wall-clock re-check fix exists
to be safe against regardless of cause.

## Reading the two numbers together

Out of 1263 silence *episodes* logged (most of them almost certainly ordinary quiet-market
periods — untraded assets, the quiet minute after a cycle opens, long-duration markets with
naturally sparse updates), only **4 (0.3%)** were confirmed as genuinely wrong data by the
mechanism built to tell the difference, each one caught and cleanly recovered within its 5s poll
cadence + a few seconds of restart time. This is the phase-1/phase-2 design
(`plan_bba_feed_staleness_fix_2026-07-10.md`) working as intended: phase 1 (`OBSERVE-STALE`)
measures the *appearance* of the problem at true scale, deliberately without judgment; phase 2
(`RECONCILE-STALE`) is the actual judgment, and it's small and real, not silence-shaped noise.

**HYPE is worth a specific note**: it's the worst offender on pure silence count (305 events,
worst ≥120s) but isn't one of `trade_assets` (BTC/ETH/SOL/BNB/XRP/DOGE) — it has no Binance
market at all (`collect.rs`'s own comment: "An asset with no Binance market at all (HYPE) never
has price > 0.0"), tracked for recording purposes only. Its silence numbers don't carry trading
risk directly, but it did produce one of the four confirmed genuine mismatches this window —
consistent with it generally being the thinnest/least liquid of the tracked markets.

## Connection to `incident_eth_trade_2026-07-21.md` #2

This is the same underlying phenomenon that incident doc diagnosed from a single trade: the CLOB
order book going quiet meant the *old*, purely tick-driven `unwind_time_rev` timeout had no
event to evaluate itself against and silently never fired. That incident inferred "the book goes
quiet" from one trade's heartbeat log; this digest **quantifies** it directly — CLOB silence
crossing at least 60s happened 126–305 times per asset in 24h, and 120s+ for BTC/HYPE/XRP
specifically. That's a routine, frequent occurrence across every tracked asset, not a rare
one-off — strong after-the-fact validation that the wall-clock re-check fix
(`Worker::is_holding()` + the driver's per-second synthetic-tick loop, deployed 2026-07-21) was
addressing a real, common gap, not a hypothetical edge case.

One additional factor this digest surfaces that the incident doc didn't have in view: each of
poly-collector's ~4-6 reconciliation-triggered restarts per day is itself a genuine data gap for
the *trader* process too (NATS goes quiet for the restart + reconnect duration). The wall-clock
timeout fix force-closes using `slot.last_poly_up`/`last_poly_dn` — the last price observed
*before* the gap, whatever its age — which is correct and necessary (there's nothing fresher to
use), but worth being aware that a timeout-forced close landing during or just after one of these
restarts could execute against a price that's stale by more than the "ordinary quiet market"
case. Not a new bug, and not different in kind from the general "force-close uses the last known
price" design already accepted for the timeout fix — noted here as context, not a proposed
change.

## Not proposing any change

`price_feed`'s staleness/reconciliation system is working as designed — phase 1 telemetry
measuring true scale, phase 2 reconciliation catching and auto-recovering genuine mismatches at
a small, real rate. This doc is investigative/connective, not a bug report: no `price_feed` or
`trader` code changes are proposed as a result. If the reconciliation-restart rate (currently
~4-6/day) climbs meaningfully, or if `[RECONCILE-STALE]` mismatches start showing up during an
actual open trader position (not checked for in this pass — would need to cross-reference
`paper_trades_*.csv` entry/exit timestamps against `RECONCILE-STALE` timestamps directly), that
would be worth a dedicated follow-up.
