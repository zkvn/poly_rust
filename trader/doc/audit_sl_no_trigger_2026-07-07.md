# Audit — stop-loss never fired (SOL) / fired with 1 second to spare (DOGE), 2026-07-07

Two `reversal` trades flagged for review:

```
📋 SOL Order placed | 10:38:56 | T-63s | DOWN ↓ | reversal
price=0.7500 | delta=-0.061% | ...
❌ SOL TRADE LOSS | 10:40:00 | DOWN ↓ | reversal
entry=0.7500 → exit=0.0000 | pnl=-$1.0175 | 0W/1L

📋 DOGE Order placed | 11:23:46 | T-73s | DOWN ↓ | reversal
price=0.9400 | delta=-0.106% | ...
🛑 DOGE STOP LOSS triggered | 11:24:59 | T-0s | DOWN ↓ | reversal
❌ DOGE TRADE STOPLOSS | 11:24:59 | DOWN ↓ | reversal | entry=0.9400 → exit=0.0100 | pnl=-$0.9907
```

Both trades lost almost the full stake despite a stop-loss being configured. The question: did the
stop-loss logic itself fail, or was it never going to help here? Verified against tick-level CLOB
mid-price data (`{ASSET}_poly_*.parquet`) and full order-book depth (`{ASSET}_book_*.parquet`),
resynced from the Oracle box for this audit — the local copy was stale at `..._07-06_17`, missing
this whole day (`price_feed/scripts/sync_oracle.sh`, a plain read-only rsync pull, no config
changed).

**Bottom line up front: this is not a stop-loss bug. Both trades behaved exactly as
`worker.rs`/`execution.rs` are written — the actual defect is the strategy config
(`sl_pnl_rev = 0.80`, `config/strategy_20260705.toml:92-93`), which is either mathematically
unreachable or reachable only after the position has already lost almost everything, depending on
the entry price.**

## 1. SOL DOWN @ 10:38:56 — stop-loss never fired at all

`live_trades_sol_reversal.csv:12`:
```
1783392000.99,sol-updown-5m-1783391700,reversal,DOWN,1783391935.409,0.75,0,LOSS,-1.0175,0,,...
```
`exit_attempts: 0` — no exit of any kind (unwind or stop-loss) was ever attempted. The position
was held, unmanaged, all the way to natural cycle resolution.

**sl_pnl-based stop-loss condition** (`worker.rs:593-594`):
```rust
let sl_hit = (self.sl_pnl > 0.0 && exit_price <= h.token_price - self.sl_pnl)
    || (self.sl > 0.0 && exit_price < self.sl);
```
`sl_pnl_rev` has no SOL override, so it's the default `0.80` (`strategy_20260705.toml:92-93`).
Entry price was `0.75`. Threshold: `0.75 − 0.80 = −0.05`. A Polymarket token price can never go
below `0.00` — **this threshold could never be crossed by any possible price**, for the entire
life of the position. `sl_reversal` (the absolute-price floor) is `0` for every asset
(`strategy_20260705.toml:86-87`), i.e. disabled, so there was no second mechanism to catch this
either. `sl_hit` was structurally `false` from the moment of entry.

**What actually happened to the price** (tick data, `SOL_poly_2026-07-07_10.parquet`, filtered to
`slug=sol-updown-5m-1783391700`):

| t (s after entry) | up | dn |
|---|---|---|
| −4.8 | 0.2700 | 0.7300 |
| +0.2 (entry fill) | 0.2350 | 0.7650 |
| +5.2 | 0.4450 | 0.5550 |
| +10.2 | 0.6650 | 0.3350 |
| +15.2 | 0.8950 | 0.1050 |
| +30.2 | 0.9250 | 0.0750 |
| +42.2 (min) | — | **0.0050** |
| +60.2 → cycle close | 0.9950 | 0.0050 |

The DOWN token collapsed from 0.7650 to 0.1050 in the first **15 seconds** after entry, and to
0.0050 by +42s — over a minute before cycle close. A working stop-loss with *any* reachable
threshold above 0 would have fired within the first 5-15 seconds. Order-book depth at resolution
(`SOL_book_2026-07-07_10.parquet`, `side=DN`) confirms the token was simply worthless by then —
`best_bid=0.0000`, `best_ask=0.0100`, no resting bids at all — consistent with a token that lost
outright, not a liquidity failure that stopped an exit from filling. No exit was ever attempted, so
this book state is just the aftermath, not a cause.

**Root cause: `sl_pnl_rev = 0.80 > entry_price = 0.75`.** Whenever a reversal entry price is below
`sl_pnl_rev`, the stop-loss is silently disabled for that entire trade — it isn't a rare edge case,
it's guaranteed by the arithmetic (see §3).

## 2. DOGE DOWN @ 11:23:46 — stop-loss fired, but only ~1 second before cycle close

`live_trades_doge_reversal.csv:13`:
```
1783394699.86,doge-updown-5m-1783394400,reversal,DOWN,1783394624.735,0.94,0.01,STOPLOSS,-0.9907,0,,...
```
This one *did* trigger (`exit_attempts: 0` because it filled on the very first attempt — see
`live.log:72237-72241`, `[SL] DOGE stop-loss triggered`, matched immediately at 0.01). So the
mechanism itself worked correctly here. The question is why it protected almost nothing.

`sl_pnl_rev = 0.80` (no DOGE override), entry `0.94` → threshold `0.94 − 0.80 = 0.14`.

**Price path** (`DOGE_poly_2026-07-07_11.parquet`, `slug=doge-updown-5m-1783394400`):

| t (s after entry) | up | dn |
|---|---|---|
| +0.1 (entry fill) | 0.1000 | 0.9000 |
| +20.1 | 0.1450 | 0.8550 |
| +35.1 | 0.4050 | 0.5950 |
| +45.1 | 0.1400 | 0.8600 |
| +60.1 | 0.1600 | 0.8400 |
| +70.1 | 0.5050 | 0.4950 |
| **+74.3** | — | **0.0300** ← first tick ≤ 0.14 threshold |
| +75.7 (min) | — | 0.0050 |

For the first ~70 seconds (of a ~75s remaining window at entry — cycle ends at `T+300s` from
`cycle_start=1783394400`, entry was at `T+224.7s`), DOGE chopped between 0.86 and 0.59 DOWN —
nowhere near the 0.14 threshold, behaving like a normal, survivable position. Then, in the last
**~5 seconds** of the 5-minute cycle, price gapped straight through: `dn` went from 0.4950 (+70.1s)
to 0.0300 (+74.3s) in a single ~4-second window — the threshold and the near-total-loss level were
crossed almost simultaneously. `worker.rs`'s stop-loss fired on the very next `PolyTick` after the
threshold was crossed (as designed — see `incident_31_retry_sl_2026-07-07.md` for the general
retry/gating mechanism) and filled cleanly at 0.01 (`DOGE_book_2026-07-07_11.parquet` shows a
resting bid of size 121.86 @ 0.01 right at that moment — plenty of liquidity, our 1.06-share sell
matched instantly, `n_attempts=1`). **The stop-loss did its job the instant it could; the problem
is that "the instant it could" was also "the instant the token was already nearly worthless."**

This is the same underlying failure mode as the SOL trade, just less severe: `0.80` is such a large
allowed drawdown that for a `reversal` entry in the 0.85-0.95 range (typical for this strategy —
see `strategy_20260705.toml:76-84`'s `reversal` threshold column), the threshold sits down at
0.05-0.15, which in a binary market that tends to resolve via a fast terminal flip rather than a
gradual slide (both these trades exhibit that same shape — DOGE's final 4s, SOL's first 15s) is
functionally indistinguishable from "no stop-loss at all."

## 3. Root cause, generalized: `sl_pnl_rev` is a fixed absolute-price drop, not scaled to entry price or market dynamics

`sl_hit`'s reachability condition (from `worker.rs:593`) is simply:

```
reachable  ⟺  entry_price − sl_pnl_rev > 0  ⟺  entry_price > sl_pnl_rev
```

At the shared default `sl_pnl_rev = 0.80`, **any reversal entry below 0.80 has an unreachable
stop-loss**, and any entry only modestly above 0.80 has a threshold so close to zero that it only
fires after the position has already lost ~85-95% of its value — which, empirically, is usually
*after* the market's terminal move rather than during it, because these 5-minute binaries tend to
resolve via a fast last-seconds flip (both trades above; also the unrelated
`incident_31_retry_sl_2026-07-07.md` ETH case). `sl_pnl_hp` (high_prob's equivalent, `0.25`,
`strategy_20260705.toml:127-128`) is far tighter and doesn't share this problem at high_prob's
typical 0.80-0.93 entry range — this asymmetry between the two strategies' stop-loss tightness
looks unintentional rather than a deliberate choice specific to reversal.

**This isn't a one-off — checked every historical `reversal` trade's entry price against its
threshold** (`sl_pnl_rev` default 0.80, BTC override 0.50):

| Asset | Slug | Entry | Threshold | Outcome |
|---|---|---|---|---|
| DOGE | `doge-updown-5m-1783071000` | 0.6600 | **−0.1400 (unreachable)** | WIN (survived by luck) |
| SOL | `sol-updown-5m-1783254600` | 0.6800 | **−0.1200 (unreachable)** | UNWIND (survived by luck) |
| SOL | `sol-updown-5m-1783391700` | 0.7500 | **−0.0500 (unreachable)** | **LOSS — this audit** |

Three trades in the history to date had a structurally-unreachable stop-loss; two happened to
resolve in the trade's favor before it mattered, and the third (this one) didn't. This is a latent
gap that had already fired twice silently before producing a visible loss.

**Sensitivity check — where a tighter `sl_pnl_rev` would have caught each of this audit's two
trades**, using the same tick data:

| `sl_pnl` | SOL threshold | SOL first cross | DOGE threshold | DOGE first cross |
|---|---|---|---|---|
| 0.80 (current) | −0.05 | never | 0.14 | +74.3s (dn=0.03, ~1s before close) |
| 0.50 | 0.25 | +12.6s (dn=0.25) | 0.44 | +70.3s (dn=0.23) |
| 0.30 | 0.45 | +5.8s (dn=0.40) | 0.64 | +35.1s (dn=0.60) |
| 0.20 | 0.55 | +5.4s (dn=0.52) | 0.74 | +34.3s (dn=0.72) |
| 0.10 | 0.65 | +3.0s (dn=0.63) | 0.84 | +34.3s (dn=0.72) |

Even a moderately tighter `sl_pnl_rev` (e.g. 0.30-0.50) would have exited both trades many seconds
earlier, at a meaningfully better price, and would still be reachable from any entry price down to
~0.30-0.50 — well below the practical entry range these strategies actually use.

## 4. Not fixed here

Per the working-style rule in this repo's `CLAUDE.md` (confirm before any file change), this audit
doc only documents the finding — no change was made to `strategy_20260705.toml`. Whether the right
fix is a lower flat `sl_pnl_rev`, a per-asset override (matching `sl_pnl_rev`'s existing BTC
pattern), or switching to an entry-price-relative fraction (so the threshold can never exceed the
entry price by construction) is a strategy-calibration call, not a code-correctness one — worth a
decision before touching the config.

**Follow-up:** traced *why* the backtest calibration landed on `sl_pnl_rev = 0.80` in the first
place — turns out every unconstrained bt2 sweep in `../../btc_5mins` (both 2026-06-26 and
2026-07-06, two weeks apart) actually picks `sl_pnl = 0.00` (no stop-loss at all) as PnL-optimal;
`0.80` only survives because the walk-forward study that produced it specifically excluded
`sl_pnl = 0` and then walked to that search's grid maximum. Full writeup:
`../../btc_5mins/studies/bt2/followup_sl_pnl_boundary_2026-07-07.md`.

**Follow-up 2 (2026-07-08):** implemented the actual mitigation this section deferred — a
max-holding-time force-exit (`unwind_time_rev`/`unwind_time_hp`) that closes a stuck position at
market after a fixed number of seconds, *independent* of whether `sl_pnl` can ever mathematically
trigger. This is a structurally different fix from re-tuning `sl_pnl_rev` itself (which this
section left as a calibration call): it bounds worst-case exposure time regardless of what the
PnL-based threshold is set to, so a future unreachable-threshold config mistake like this one would
still be capped. See `trader/doc/plan_unwind_time_2026-07-08.md` and README's "`unwind_time` —
max-holding-time force-exit" entry.

## 5. Data note

`price_feed/raw/` on this machine only had data through `2026-07-06_17` before this audit — the
sync from the Oracle box hadn't run since then. Ran `price_feed/scripts/sync_oracle.sh` (plain
`rsync -avz --exclude=*.tmp` pull from `ubuntu@10.8.0.1`, read-only, no state changed) to bring in
`2026-07-07` hours 00-12 before this analysis; both trades' `_10`/`_11` poly and book files are now
present locally.
