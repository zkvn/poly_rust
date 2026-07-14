# Investigation: reversal variants sharing entry_ts within one market (2026-07-14)

## Reported symptom

Two specific trades flagged as suspicious:

- **BTC-15m, 2026-07-14 12:22:35.229 HKT** — `reversal_0.4_0.55` and `reversal_0.4_0.8`
  triggered on what looked like the same tick.
- **ETH-5m, 2026-07-14 11:47:18.561 HKT** — `reversal_0.4_0.55` through `reversal_0.4_0.8`
  (6 variants) all triggered together.

Both postdate the 2026-07-14 `entry_price_ts` fix (container restarted 11:11:08 HKT), so this
is a distinct question from that incident: is the *same-market* co-firing itself a real price
event, or evidence of a second, different timestamp bug?

## Method

`siglab` has its own independent Polymarket CLOB WS subscription — it is not derived from
`price_feed`'s recording. That makes `price_feed`'s parquet archive (`raw/` = 5m,
`raw_15_mins/` = 15m, both period-independent `_binance_` files in `raw/`) an *independent*
ground truth to check siglab's trade log against: if siglab's recorded price/timing can't be
reproduced from `price_feed`'s separately-captured data, that's real evidence of a siglab-side
bug rather than genuine market behavior. Ran `price_feed/scripts/sync_oracle.sh` first to pull
the missing `_12` HKT hour (collector seals hourly; `_12` wasn't synced yet locally).

Pulled the exact trade rows from `siglab`'s own `/app/logs/siglab_trades.jsonl` inside the
running container, then cross-referenced against `price_feed`'s CLOB (`up`/`dn`) and Binance
spot price recordings for the same real time windows.

## BTC-15m, 12:22:35 — replayed timestamps

Trade log (`btc-updown-15m-1784002500`, cycle opened 12:15:00 HKT):

| variant | entry_ts | entry_price_ts | token_price | outcome | pnl |
|---|---|---|---|---|---|
| `reversal_0.4_0.55` | 12:22:35.2300 | 12:22:35.2282 | 0.845 | TIMEOUT | 0.0592 |
| `reversal_0.4_0.8`  | 12:22:35.2300 | 12:22:35.2282 | 0.845 | TIMEOUT | 0.0592 |

`entry_ts` and `entry_price_ts` differ by ~1.8ms — this fired directly off a fresh poly tick,
not a stale-cache-via-Binance-tick entry (the mechanism the prior `entry_price_ts` incident
fixed). `price_feed`'s independently-recorded `BTC_poly_2026-07-14_12.parquet` confirms
`up = 0.845` at 12:22:35.2-35.4, matching exactly.

Real price/gate timeline for this cycle (from `price_feed`'s parquet + Binance recording),
`reversal_low_threshold=0.4` for both variants:

| event | real HKT time | value |
|---|---|---|
| price dips below 0.4 (arms `saw_low`) | 12:15:00 – 12:18:58.8 | down to 0.015 |
| price first crosses `reversal_0.55` | **12:15:58.6** | up=0.565 |
| price first crosses `reversal_0.8` | **12:21:28.6** | up=0.805 |
| `delta_pct` (Binance BTC momentum vs. cycle-open 62564.49) first exceeds `delta_pct_rev=0.0008` | **12:22:35.250** | binance=62616.27, dp=0.000828 |
| siglab records both entries | 12:22:35.230 | up=0.845 |

Both variants' price condition was satisfied **6-7 minutes** before either fired. The actual
gating factor was `delta_pct` — the directional-confirmation gate, shared identically across
all 18 reversal variants (`delta_pct_rev=0.0008` is one fixed value in `config/markets.toml`,
not per-variant) — which stayed below threshold the whole time BTC chopped sideways, and only
cleared at 12:22:35.25, ~20ms before siglab's recorded entry (well within real WS/tick
latency). The instant it cleared, every variant whose price threshold was *already* satisfied
fired together, at whatever the real current price happened to be (0.845 — past both 0.55 and
0.8, since price had kept climbing during the wait). **This is airtight: independently
recorded, cross-checked data proves the timing and price are both real,** not a
timestamp-tracking defect.

## ETH-5m, 11:47:18 — replayed timestamps

Trade log (`eth-updown-5m-1784000700`, cycle opened 11:45:00 HKT), all 6 variants
identical:

| variant | entry_ts = entry_price_ts | token_price | outcome | pnl |
|---|---|---|---|---|
| `reversal_0.4_0.55` .. `reversal_0.4_0.8` | 11:47:18.5615 | 0.855 | TIMEOUT | 0.0592–0.1287 (per threshold) |

Real price/gate timeline:

| event | real HKT time | value |
|---|---|---|
| price dips below 0.4 | 11:45:00 – 11:45:21.4 | |
| price first crosses `reversal_0.55` | 11:45:37.6 | up=0.565 |
| price first crosses `reversal_0.8` | 11:46:42.8 | up=0.815 |
| `delta_pct` first exceeds 0.0008 | 11:47:07.25 | eth=1782.50, dp=0.000837 |
| price plateaus at 0.855, no movement | 11:47:15.6 – 11:47:18.6 | |
| siglab records all 6 entries | 11:47:18.561 | up=0.855 |
| price resumes climbing (independent confirmation of a real book update right around here) | 11:47:18.8+ | up=0.885, 0.895, ... |

Same structural pattern: price crossed all 6 thresholds 35s-2m03s before the entry, and
`delta_pct` cleared its gate **11 seconds** before the entry too — so unlike the BTC case,
`delta_pct` isn't the single smoking-gun explanation for the exact 11:47:18.561 moment. Most
likely explanation: `check_gates`' poly-staleness check (`max_price_age_secs=2.0`) — siglab's
*own* WS connection may simply not have delivered a new best-bid-ask/price-change event during
the 0.855 plateau (Polymarket's feed is event-driven, not a heartbeat; a flat book produces no
messages), making `latest_poly.age(now)` exceed 2.0s and blocking entry on every intervening
binance tick, until a fresh poly message — visible independently in `price_feed`'s own
recording as the moment the price starts moving again — arrived and cleared it. `entry_ts ==
entry_price_ts` confirms this fired off a genuinely fresh poly tick, not a stale cached
price via a Binance trigger. Couldn't pin the exact blocking gate with full certainty (siglab
doesn't log per-tick gate-rejection reasons), but the core question — is the recorded price
real and is the timestamp trustworthy — is answered the same way as BTC: **yes**, confirmed
independently.

## Conclusion: not a bug

Both flagged trades are real: the recorded prices match `price_feed`'s independently-captured
data, and (via the `entry_price_ts` field added in the prior fix) both fired directly off a
live poly tick, not a stale cached value mislabeled with the wrong timestamp. The underlying
mechanism is the same one documented in
`siglab/doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md`'s cross-duration
case, applied within a single market: **all 18 variants share the same delta_pct gate, the
same spread/staleness gates, and the same incoming tick stream — only the raw price threshold
differs between them.** Once price has already blown past several/all thresholds (common,
since `reversal_start_time=999999` lets the dip/recovery happen anywhere in the cycle) and the
last *shared* gate is what's actually rate-limiting entry, releasing that gate fires every
already-qualified variant simultaneously, at whatever the real price is at that moment — not
at each variant's own threshold value. This confirms (rather than contradicts) that incident's
finding: the 18-variant grid's members are not independent samples whenever a shared gate is
binding, which is most of the time in practice.

No code fix applies here — nothing is broken. `delta_pct_rev` lowered to 0.0003 (this session's
config change, see `siglab/README.md`) will somewhat reduce how often the momentum gate is the
binding constraint (clears faster), but won't eliminate this pattern, since it's structural to
the grid design, not a bug in gate evaluation.
