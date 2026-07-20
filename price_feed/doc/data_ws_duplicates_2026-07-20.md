# Research note — one-WS-per-market vs. combined streams, and whether duplicate connections help

Status: **research only, not implemented.** Written per user request, alongside the
`@bookTicker` rollout (`plan_binance_ws_quality_2026-07-20.md` §3, implemented same day).
Two questions: (1) is "one WS connection per market" the right design for Binance, or should
multiple markets share one connection, and (2) would running duplicate/redundant connections
to the *same* stream and taking whichever delivers first/latest help close data gaps — for
both Binance and the Polymarket CLOB feed.

## 1. Current design

`price_feed/src/collect.rs` opens one raw `wss://stream.binance.com:9443/ws/{symbol}@X`
connection **per asset per stream type**. As of the `@bookTicker` rollout this is now 2
connections/asset (`@trade` + `@bookTicker`) — 12 total for the 6 live assets, up from 6.

The Polymarket CLOB side is different: `spawn_book_task` already subscribes to **all** UP/DOWN
token IDs across every asset on a **single** `subscribe_orderbook(ids)` call (one WS, many
markets multiplexed) — it was never one-per-market. `spawn_bba_task` similarly multiplexes all
assets onto its `best_bid_ask`/`price_change` subscriptions.

So the "one WS per market" pattern this research question is actually about only applies to the
Binance leg, not CLOB.

## 2. Does Binance support one WS carrying multiple markets? Yes — combined streams

Binance's documented **combined stream** endpoint (`wss://stream.binance.com:9443/stream?streams=
btcusdt@trade/btcusdt@bookTicker/ethusdt@trade/...`) carries any mix of symbols/stream types over
one connection, wrapping each message as `{"stream":"<name>","data":<rawPayload>}`. Verified limits
from Binance's own docs (fetched directly, not just search-summarized):

- **1024 streams per connection** (plenty of headroom for this project's 6-12 assets × 2 streams).
- **300 new-connection attempts per 5 minutes per IP** — a *reconnect-rate* cap, not a concurrent-
  connection cap. No documented hard limit on how many connections can be held open simultaneously.
- **24h max connection lifetime** — expect a forced disconnect at the 24h mark regardless of stream
  count; the existing reconnect-with-backoff loop already handles this.
- **5 incoming *control* messages/sec** (ping/pong/subscribe/unsubscribe) — does not apply to
  inbound trade/bookTicker data volume.
- Binance's own stated best practice: *"Consolidate WebSocket connections where possible to reduce
  resource consumption. Use a single connection to manage multiple streams effectively."*

**Verified locally (Test A below): the combined-stream endpoint works exactly as documented** —
4 requested streams (2 assets × `@trade`+`@bookTicker`) all arrived correctly-labeled on one
connection within 8 seconds.

### Verdict on question 1

There's no hard limit forcing today's 12-connections-for-6-assets design, but it's not the
Binance-recommended pattern either, and 12 independent reconnect loops is more moving parts than
necessary. **Worth consolidating, but not into a single connection** — one connection carrying
every asset means one dropped/degraded connection makes *every* asset go stale simultaneously,
which cuts directly against this project's `[[Trading principles]]` (never trade on stale
data) by turning a single-asset network hiccup into an all-assets blackout. A middle ground (e.g.
2-3 combined connections, each carrying a subset of assets) would cut connection count from 12 to
2-3 while keeping the blast radius bounded — but this is a **connection-hygiene improvement, not a
fix for anything currently broken** (the DOGE incident that started this whole investigation was
root-caused to a genuine quiet-market gap, not a connection failure — Oracle's journalctl showed
zero disconnects/reconnects/errors during that window). Not recommended as urgent work; flagged as
a possible future cleanup, not scheduled here.

## 3. Would duplicate/redundant connections to the same stream help?

General low-latency market-data literature confirms this is a real technique (multi-homing
connections across networks/servers, taking whichever arrives first, is how some professional
feeds claim "virtually zero gaps" at scale) — but it targets a specific failure mode:
**connection-level loss** (a silently stalled TCP connection, a bad network path, a dropped packet
that the exchange *did* send). It does **not** help with a stream that has genuinely nothing to
report — two connections to the same `@trade` stream during a real quiet period both see the exact
same silence, because both are subscribed to the same exchange-side event source. That's precisely
why `@bookTicker` (a denser event source), not connection duplication, was this session's actual
fix for the DOGE staleness incident.

### Test B — empirical check, two live connections to the identical stream

Ran two independent WS clients against `wss://stream.binance.com:9443/ws/btcusdt@bookTicker`
simultaneously for 20 seconds, logging each message's Binance `updateId` (`u`) and local receive
timestamp on each connection, then compared:

```
messages on A: 1996, messages on B: 1996
updateIds seen on both: 1996
updateIds only on A: 0
updateIds only on B: 0
arrival skew (B vs A), same updateId: mean=1.11ms  median=0.21ms  min=-40.22ms  max=119.30ms  stdev=7.23ms
B arrived first: 1602/1996 (80%)   A arrived first: 394/1996 (20%)
```

**Zero gap-filling benefit observed**: every single update that arrived on one connection also
arrived on the other — no connection ever caught something the other missed, over ~2000 messages
in a real 20s window. The only effect was a marginal latency shave (median ~0.2ms, occasionally up
to ~100ms on stragglers) — consistently in favor of whichever connection happened to establish its
TCP path first, not a structural advantage of either. (This test used a healthy network path for
both connections from the same box; it can't demonstrate the failover benefit a *genuinely* faulty
connection would show — that would need real packet loss/partition injection, out of scope for a
quick local check.)

### Verdict on question 2

**Not recommended for this project, for either Binance or CLOB.** Reasons:

1. **It doesn't address the failure mode this project has actually observed.** The one real
   incident on record (DOGE, 2026-07-20 audit) was a quiet-market gap, not a dropped connection —
   duplicate connections to `@trade` would have shown the identical silence on both. `@bookTicker`
   (already shipped) is the correct fix for that failure mode; duplication is not a substitute or
   a complement to it.
2. **No evidence of upside under normal conditions** (Test B) — 100% message overlap, sub-
   millisecond median latency difference, for double the connections/bandwidth/CPU per asset.
3. **Binance's own ping/pong protocol already guards against the specific "silent dead connection"
   failure mode** duplication would help with: a 20s server ping requiring a pong within 60s, or
   the connection is torn down and the existing reconnect-with-backoff loop kicks in — meaning a
   truly black-holed connection is caught and recovered within roughly a minute even today, without
   needing a second connection running in parallel the whole time.
4. **CLOB already has an architecturally similar (but cheaper) form of this pattern** —
   `spawn_bba_task` merges `best_bid_ask` and `price_change`, two genuinely *different* event
   sources (not duplicate subscriptions to the same one), taking whichever delivers a fresher
   quote (`plan_bba_merge_ordering_fix_2026-07-16.md` already hardened the merge-ordering safety
   of exactly this pattern). That's the useful version of "redundancy" — different sources that can
   each independently have data when the other doesn't — not two subscriptions to the identical
   channel, which (per Test B) rise and fall together.

If a genuine connection-reliability problem shows up later (repeated silent stalls not caught by
ping/pong, evidenced by real Oracle logs — not hypothesized here), duplicate connections would be
the correct next thing to reach for. Nothing in the current incident history supports doing it
now.

## 4. Test scripts

Both tests run against live Binance endpoints (Test B: `wss://stream.binance.com:9443/ws/
btcusdt@bookTicker`, Test A: the combined-stream endpoint) via a standalone Python script (not
part of the `price_feed` binary — pure local verification, not shipped):

```python
# Test A: wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@bookTicker/dogeusdt@trade/dogeusdt@bookTicker
# — confirms one connection delivers all 4 correctly-labeled streams within 8s.
# Test B: two independent connections to wss://stream.binance.com:9443/ws/btcusdt@bookTicker,
# 20s, compare updateId (u) sets + arrival-timestamp skew between the two connections.
```

Full script: run ad hoc from `/tmp` scratch during this session (not committed — pure research,
reproducible from the snippet above with any WS client).

## Sources

- [WebSocket Streams | Binance Open Platform](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [binance/binance-spot-api-docs — web-socket-streams.md](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md) — verbatim source for the connection-limit/streams-per-connection/ping-pong figures quoted in §2.
- [What Are Binance WebSocket Limits? | Binance Academy](https://academy.binance.com/en/articles/what-are-binance-websocket-limits)
- [Binance API Multiple Websocket Connections Implemention — Binance Developer Community](https://dev.binance.vision/t/binance-api-multiple-websocket-connections-implemention/19280)
- `price_feed/doc/plan_binance_ws_quality_2026-07-20.md` — the `@bookTicker` fix this research is adjacent to.
- `price_feed/doc/plan_bba_merge_ordering_fix_2026-07-16.md` — the CLOB-side "merge two different channels, take latest" precedent referenced in §3.
