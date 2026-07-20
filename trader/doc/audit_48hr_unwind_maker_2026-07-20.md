# Audit — 48h unwind-maker paper run, 6 issues from live Telegram output (2026-07-20)

Source: 6 issues raised against the `[PAPER]`-prefixed Telegram stream from the
`plan_unwind_5u_maker_2026-07-19.md` 48h paper run (restarted 2026-07-19 21:58 HKT, real-bid
maker-entry fix — see README's "Maker-entry quotes rested at the signal mid-price" entry). Each
section below: symptom → code root cause (with live evidence pulled from Oracle where relevant) →
proposed fix. **Nothing in this doc has been implemented — audit + proposal only, for review.**

Findings are ranked by how much they affect *trading correctness* first, *readability* second:

1. §1 (indicator "no data") is the one with real trading-safety consequences: a genuine DOGE feed
   gap let a bad-edge entry through the p(up) veto, not just an ugly log line.
2. §2–§6 are output-clarity issues — no bad trades resulted, but the Telegram stream is
   ambiguous or actively wrong in ways that erode trust in what it's reporting.

---

## 1. Indicator "no data" — not a display bug, a genuine DOGE feed gap that let a bad-edge trade through

### Symptom

```
[PAPER] 📝 DOGE Maker quote resting | 02:33:55 | T-64s | UP ↑ | reversal
BUY 5.00sh @ 0.8700 | ind: no data
```

### What "no data" actually means today

`fmt_indicator` (`trader/src/bin/live.rs:814-839`) collapses three very different situations into
one string:

```rust
let Some(snap) = store.fresh(asset, now, max_age_secs) else {
    return "ind: no data".to_string();
};
```

`IndicatorStore::fresh` (`trader/src/indicator_store.rs:64-68`) returns `None` for:
- an asset that has **never** received a single `indicator.<ASSET>` NATS message, *or*
- an asset whose **last received** snapshot is older than `max_age_secs` (currently
  `PUP_GATE_MAX_AGE_SECS = 10.0s`, `trader/src/worker.rs:86` — the same fixed constant is passed
  to every display call site, not just the trading gate; see `asbuilt_unwind_5u_maker_2026-07-19.md`
  §4 for why the display was unified onto the gate's own window on 2026-07-19).

Both produce the identical `"ind: no data"` string. There is no way, from the Telegram message
alone, to tell "this asset never got indicator coverage" (a config bug) apart from "the feed just
went quiet for a few seconds" (routine) apart from "the indicator daemon is dead" (a real outage
that should page someone).

### Root cause, confirmed against Oracle logs

Pulled `poly-indicator.service` + `trader-live.service` logs from Oracle for this exact incident
(`ssh ubuntu@10.8.0.1`, `trader/live_logs/live.log`):

**The indicator daemon was not down.** `poly-indicator.service` has been running continuously
since 2026-07-19 14:57:22 HKT with 0 restarts and no publish-failure/error lines anywhere near
02:33:55. So this isn't the "dead `poly-indicator.service`" case the fail-open design was built
to protect against.

**What actually happened**, reconstructed from `live.log` (lines ~497900-498059):

```
[PUP-GATE] DOGE VETO side=Up p_side=0.7539 price=0.8600   ]
[PUP-GATE] DOGE VETO side=Up p_side=0.7539 price=0.8600   ]  same p_side=0.7539 repeated
...                                                         ]  157+ times in a row — DOGE's
[PUP-GATE] DOGE VETO side=Up p_side=0.7539 price=0.8700   ]  indicator snapshot never updated
[PUP-GATE] DOGE VETO side=Up p_side=0.7539 price=0.8700   ]  during this whole span
[ORDER] DOGE MAKER ENTRY BUY 5.00 @ 0.8700 (Up) -> status=Live order_id=Some("paper-4")
[telegram] sent: [PAPER] 📝 DOGE Maker quote resting | 02:33:55 | T-64s | UP ↑ | reversal
[PUP-GATE] DOGE pup_gate=SKIPPED_NO_DATA side=Up price=0.8700   <- the one check that mattered
```

The gate had been *correctly vetoing* this exact entry for well over 10 seconds straight: DOGE's
own model probability (`p_side = 0.7539`) was below the quoted price (`0.8700`) — a bad-edge
entry the p(up) gate exists specifically to block. Then, in the instant the order was actually
placed, the same DOGE indicator snapshot crossed the fixed 10s staleness window it had been
sitting inside the whole time, `PupGateOutcome` flipped from `Veto` to `SkippedNoData` (fail-open,
by design — `worker.rs:1204`), and the entry that had just been correctly blocked for 10+ seconds
went through on the very next check.

The DOGE heartbeat lines around this window (`[live] heartbeat DOGE ... T-64s binance=0.0720
up=0.8750 dn=0.1250`) confirm the `ind[...]` suffix — present on every other asset's heartbeat —
was **missing for DOGE**, i.e. this wasn't a one-tick coincidence, DOGE's indicator was genuinely
stale for the whole window.

**Why DOGE specifically:** `indicator`'s Binance ingestion is the raw `@trade` stream
(`price_feed/src/collect.rs:976`, one WS per asset, `wss://stream.binance.com:9443/ws/{symbol}@trade`)
— it only ticks on an actual executed trade, no synthetic keepalive. Checked `poly-collector`
Binance logs for this window on Oracle: no reconnects, no errors — the WS connection itself was
healthy. DOGE-USDT is materially lower-frequency than BTC/ETH on raw trade prints, especially
during Asia late-night hours (02:33 HKT); a 10-60s gap between individual trades is normal market
behavior for this pair, not a feed fault. `indicator`'s `on_tick` (`indicator/src/engine.rs:148`)
only emits on a tick — no trades in, no snapshot out, so the last known `p_up` simply ages past
`PUP_GATE_MAX_AGE_SECS` and both the display and the trading gate treat "asset had no trade prints
for a bit" identically to "the whole pipeline is dead."

Grepped the full ~15h log for `pup_gate=SKIPPED_NO_DATA`: **2 occurrences total, both DOGE**
(`live.log:493195`, `live.log:498059`). The earlier one (2026-07-19 20:59:32) shows the same
pattern — a sustained multi-heartbeat DOGE indicator gap, quote placed on stale data, cancelled
before fill. Not a one-off: this is a recurring, DOGE-specific, liquidity-driven gap, currently
handled by the *safety* gate design (fail-open is correct — a truly dead daemon must not block all
trading) but with no distinction from *this* case, where the last reading was fresh and correct
right up until the millisecond it was needed.

### Proposed fixes (pick one or more — none implemented)

1. **Make the display show the last-known reading with its age, not a blank "no data".**
   `fmt_indicator` already has the snapshot's `ts` before it decides to reject it on staleness —
   change the miss case to something like `"ind: stale (p_up=0.7539, 14s old)"` when a snapshot
   exists but failed the freshness check, reserving a bare `"ind: no data"` for the case where
   the asset has *never* been seen at all. Directly answers "I need to see the indicator as of
   entry and exit time" — right now a stale-but-recent reading is thrown away entirely instead of
   shown with a caveat.
2. **Separate the trading-safety threshold from the display/diagnostic threshold.** Keep
   `PUP_GATE_MAX_AGE_SECS = 10.0s` fixed for the gate (correct — short window, fail-open, favors
   not blocking trading over a stale read). For display, show the reading regardless of age (with
   the age annotated) so a human reviewing Telegram/`/status` can always see *what the model last
   thought*, even when the gate itself has already discounted it.
3. **Add a proactive stale-indicator alert**, the same pattern the loss-streak halt already uses
   (README "Loss-streak halt now sends Telegram notifications on engage and reset" — previously
   silent, only visible by polling `/status`, fixed 2026-07-07). Right now a DOGE indicator gap is
   only visible if a trade happens to fire *during* the gap and someone reads the Telegram message
   closely, or if someone happens to run `/status` at that exact moment. A transition-edge alert
   ("DOGE indicator stale Xs, last p_up=Y" on going stale, "DOGE indicator recovered" on
   recovery) would surface this class of gap the same way the halt engage/reset now does, without
   spamming (fire only on state transitions, per the halt precedent).
4. **Consider whether the gate should reuse the last reading a short grace period past
   `PUP_GATE_MAX_AGE_SECS` instead of immediately failing open**, specifically for the "had fresh
   data seconds ago, market didn't move" case (distinguishable from "daemon has been dead for
   minutes") — this is a real behavior change to trading logic, not just observability, so it
   needs its own review/backtest before touching it; flagging here as the thing that actually
   would have blocked the DOGE trade above, not just made it easier to see after the fact.
5. **Give DOGE (and any other lower-liquidity asset) a synthetic keepalive tick in `indicator`**
   so `on_tick`'s snapshot timestamp advances even without a fresh Binance trade print (feed the
   last known price forward on a timer, the same "poll semantics" the engine already documents for
   1-Hz gap-filling inside a cycle — `indicator/src/engine.rs:1-10`). This would make "stale" mean
   "the pipeline is actually broken" again, since a quiet market would no longer look identical to
   a dead feed.

None of these are mutually exclusive; (1)+(3) are pure observability and lowest-risk; (4)/(5)
change actual trading/engine behavior and warrant their own follow-up plan doc if pursued.

---

## 2. "Maker quote resting" doesn't say entry or exit

### Symptom

```
[PAPER] 📝 SOL Maker quote resting | 05:43:46 | T-73s | DOWN ↓ | reversal
BUY 5.00sh @ 0.6000 | ind: ...
```

### Root cause

This notification only ever fires from `Action::PlaceLimitBuy`'s handler
(`trader/src/bin/live.rs:1338-1376`) — it is **always an entry**. There's a second, symmetric kind
of resting order in this same strategy — the take-profit exit sell, `Action::PlaceLimitSell`
(`trader/src/bin/live.rs:1312-1337`, also GTC-resting per `asbuilt_unwind_5u_maker_2026-07-19.md`
§5 "Resting (GTC BUY or SELL)") — but it currently sends **no Telegram notification at all**, only
a console `[ORDER] ... LIMIT SELL ...` line. So today "Maker quote resting" is unambiguous by
construction (it's the only resting-order Telegram message that exists), but nothing in the text
says so, and a reader has to already know that invariant. The `BUY` in the body is the only clue.

### Proposed fix

- Rename the message to say what it is explicitly, e.g. `📝 SOL ENTRY quote resting` (mirrors the
  `entry=`/`exit=` labels already used in the trade-close message, §3 below).
- Separately: decide whether the exit-side resting sell should get its own symmetric
  `📤 SOL EXIT quote resting` notification for parity, or whether exits are deliberately silent
  until they fill/cancel. Not fixing this asymmetry silently either way — flagging it as a design
  decision, since right now it's an accident of "only `PlaceLimitBuy`'s handler happens to call
  `self.notify`", not a considered choice.

---

## 3. "Cycle" is confusing terminology, and the W/L breakdown is incomplete

### Symptom

```
[PAPER] ✅ SOL TRADE UNWIND | 05:43:47 | DOWN ↓ | reversal
entry=0.6000 → exit=0.7500 | cycle: $75.87→$75.79 | delta=-0.105% | pnl=+$0.6004 | 0W/0L | ind: ...
```

### Root cause

`cycle: $X→$Y` (`trader/src/bin/live.rs:1701-1707`) is `slot.worker.cycle_open_binance()` →
`slot.last_binance` — the **underlying Binance price** at cycle-open vs. now, i.e. how far the
5-minute market's underlying has moved so far this cycle. It has nothing to do with the trade's
own entry→exit lifespan; the trade itself (the thing the user reasonably assumed "cycle" referred
to, given `entry=`/`exit=` sit right next to it) isn't measured in this message at all — there's
no duration field.

Separately, `0W/0L` (same line) is only `slot.wins`/`slot.losses` — it excludes
`slot.stoplosses`/`slot.unwinds`/`slot.timeouts`, all of which are already tracked per-slot
(`AssetSlot`, `trader/src/bin/live.rs:907-912`) and already rendered in full elsewhere:
`/status`'s per-asset line already does `{}W/{}L/{}SL/{}UW/{}TO` (`trader/src/bin/live.rs:1176`)
and its `Session:` summary line does the same (`trader/src/bin/live.rs:1205`). This trade-close
message is the one place that shows a truncated 2-of-5 subset of the same counters, on the exact
message where a UNWIND outcome (which counts toward neither W nor L —
`Outcome::is_loss_for_halt`, `trader/src/types.rs:125-131`) makes `0W/0L` read as "no trades
resolved yet this session," which is actively misleading right after a trade just closed.

### Proposed fix

- Relabel `cycle: $X→$Y` to something unambiguous about what it is, e.g. `mkt: $X→$Y` or
  `binance: $X→$Y`, freeing up "cycle"/duration wording for the trade's own lifespan.
- Add a trade duration field, e.g. `dur=21s`, computed from `rec.entry_ts` to the close time (the
  message is emitted immediately after `Action::LogTrade`, so `now_secs_f64() - rec.entry_ts` at
  that point is a faithful approximation of entry→exit wall time; an exact `exit_ts` isn't
  currently on `TradeRecord`, `trader/src/types.rs:135-187` — could be added as a follow-up if the
  approximation isn't tight enough).
- Expand `{}W/{}L` to the full `{}W/{}L/{}SL/{}UW/{}TO` breakdown, reusing the exact ordering
  `/status` already uses (`trader/src/bin/live.rs:1176`, `:1205`) so the two surfaces stay
  consistent instead of introducing a third format.

---

## 4. `edge UP±X/DN±X` is redundant — show only the traded side

### Symptom

```
ind: p_up=0.0304 (edge UP-0.2196/DN+0.2196) vol=1.16e-3
```

### What "edge" means (confirmed against `fmt_indicator`, `trader/src/bin/live.rs:814-839`)

```rust
p_up - up_price,          // UP edge
(1.0 - p_up) - dn_price,  // DN edge
```

`edge = p_side − price_side`: the model's own probability estimate for that side, minus what the
CLOB is currently pricing that side at. **Positive = the model thinks this side is worth more than
the market is charging for it** (a favorable read); negative = the model thinks the market is
overpricing that side. This is exactly the quantity `worker.rs`'s p(up) veto gates on
(`asbuilt_unwind_5u_maker_2026-07-19.md` §4: `p_side < entry_price + pup_edge_min_rev` triggers a
veto).

### Why the two numbers look like mirror images

Because `up_price + dn_price ≈ 1` (minus the book spread), `DN edge = (1−p_up) − dn_price ≈
(1−p_up) − (1−up_price) = up_price − p_up = −(UP edge)`, up to that small spread residual. The
two numbers really are (almost exactly) opposite signs of the same underlying signal — showing
both is close to pure duplication, exactly as flagged.

### Proposed fix

`fmt_indicator` already knows the traded/quoted side at both Telegram call sites — the maker-quote
message (`trader/src/bin/live.rs:1373`, has `*side`) and the trade-close message
(`trader/src/bin/live.rs:1701`, has `rec.side`). Add a `side: Option<Side>` parameter:
`Some(side)` → print only that side's edge without the `UP`/`DN` tag (it's implied by the `↑`/`↓`
arrow already in the same message), e.g. `ind: p_up=0.0304 (edge-0.2196) vol=1.16e-3`;
`None` (kept for the console heartbeat's `fmt_indicator` call, `trader/src/bin/live.rs:2565`,
which isn't tied to one trade) → keep showing both, since there's no "the" side to prefer there.

---

## 5. Boot-banner `size=$1.00` is wrong, and has no timestamp

### Symptom

```
[PAPER] 🟢 live driver started: BTC:reversal, ETH:reversal, SOL:reversal, BNB:reversal, XRP:reversal, DOGE:reversal (size=$1.00, max_trades=1)
```

### Root cause — already found and fixed once, but only half the fix landed

This is the **same bug class README already documents as fixed** ("Telegram `/status` showed
`trade_size_usdc` ($1.00) as the bet size for maker-entry reversal slots, which was actively
wrong", 2026-07-19): under `maker_entry = true`, every reversal entry is a fixed 5-share GTC quote
— `trade_size_usdc` plays no role at all. `/status`'s per-slot line was fixed to show
`size=5.00sh (maker)` in that case (`trader/src/bin/live.rs:1142-1146`). **The README entry
explicitly calls out that the boot banner has "the same cosmetic issue... but wasn't fixed —
narrower blast radius... left for a future pass."** That pass is this one. Confirmed still present
at `trader/src/bin/live.rs:2393-2403`: `args.size_usdc` (the CLI flag) unconditionally, for the
whole fleet in one line, even though every currently-configured slot is maker-entry reversal.

Also: the banner has no timestamp at all (every other Telegram message in this run —
maker-quote-resting, trade-close, stop-loss/timeout — includes an `hkt_now()` `dt` field), so
there's no way to tell from the message alone when the driver actually (re)started without cross-
referencing the log.

### Proposed fix

- Same fix as `/status`, applied per-slot instead of once globally: the banner currently
  summarizes the whole fleet in one string (`asset_strategy_summary`,
  `trader/src/bin/live.rs:2393-2397`) with one shared size — that's already slightly wrong in
  spirit for a fleet that could mix maker and non-maker slots (not the case today, since
  `strategy_20260719.toml` is maker-entry reversal for all six, but the format shouldn't assume
  that stays true). Cleanest fix: build the summary per-slot as `"{asset}:{strategy}
  ({size_str})"` using the same `size_str` logic `/status` already has
  (`trader/src/bin/live.rs:1142-1146`), dropping the single fleet-wide `size=$X.XX` entirely.
- Add `| {dt}` (or similar) using the same `hkt_now().format("%H:%M:%S")` pattern every other
  notification in this file already uses.

---

## 6. `/status`'s `start=120s` — confirmed: 120 seconds *remaining*, not 120 seconds elapsed

### Symptom

```
sl=0.0000  delta_gate=0.00030  low=0.2000  high=0.5500  halt_after=3L  unwind_pnl=0.1500  sl_pnl=0.0000  unwind_time=180.0s  start=120s  size=5.00sh (maker)
```

### Root cause, confirmed by tracing the value through

`start` here is `slot.params.reversal_start_time` (`trader/src/bin/live.rs:1129`), fed into
`SawLowSignal::new_up`/`new_dn` as `start_time_left` (`trader/src/worker.rs:619-627` →
`trader/src/signal/saw_low.rs:24-46`). The signal's own doc comment is explicit and unambiguous:

```rust
//! Window is in time_left (seconds until cycle end):
//!   - opens at start_time_left (e.g. 120s remaining for BTC reversal_start_time)
//!   - closes at end_time_left (e.g. 10s = no_enter_when_time_left)
```

and the gating check (`saw_low.rs:67-68`) is `time_left <= start_time_left` — the entry-watching
window **opens once only 120 seconds remain in the cycle** (for a 300s/5m cycle, that's 180s
*into* the cycle, not 120s in) and **closes at `no_enter_when_time_left`** (10s remaining, per
`strategy_20260719.toml:23`). So: confirmed, it's "120s left," the reading the user suspected —
`/status`'s bare `start=120s` (`trader/src/bin/live.rs:1132-1135`) says neither "left" nor "in,"
and reads naturally as elapsed time to anyone not already holding the `SawLowSignal` internals in
their head.

This isn't only a `/status` wording gap — it's undocumented at the source too (`reversal_start_time`
has no doc comment at its `AssetParams`/`StrategyToml` field declarations,
`trader/src/config.rs:69,172`), and it's been mis-described in this repo's own prior audit doc:
`trader/doc/audit_trades_2026-07-12.md:45` says "`reversal_start_time` (120s in)" — the exact
"elapsed" misreading this issue flags, already in a committed doc.

### Proposed fix

- `/status`: change `start={s:.0}s` to something that can't be misread the other way, e.g.
  `entry_window=T-{start}s..T-{no_enter}s` — reusing the `T-{time_left}s` convention already used
  everywhere else in this same file (maker-quote-resting, stop-loss, timeout messages), so it
  reads consistently with the rest of the Telegram/status vocabulary instead of introducing a
  second, ambiguous time convention.
- Add a doc comment on `reversal_start_time`'s field declarations
  (`trader/src/config.rs:69,172`) pointing at `SawLowSignal`'s existing (correct, clear) doc
  comment, so the next reader doesn't have to trace three files to confirm the direction.
- Fix the "120s in" phrasing in `audit_trades_2026-07-12.md:45` if that doc is ever revisited (not
  urgent — historical doc, not live code — but noting it since it's the same misreading, already
  in writing).

---

## Summary table

| # | Issue | Severity | Fix complexity |
|---|---|---|---|
| 1 | Indicator "no data" — real DOGE feed gap let a bad-edge trade slip the p(up) veto | **High** (trading correctness) | Display: low. Alerting: low-medium. Gate-behavior change / synthetic keepalive: medium, needs its own review |
| 2 | "Maker quote resting" doesn't say entry/exit | Low (clarity only — currently unambiguous by construction, but not self-documenting) | Trivial |
| 3 | "cycle:" mislabeled + W/L missing SL/UW/TO | Low-medium (misleading `0W/0L` right after a trade closes) | Low |
| 4 | Duplicate UP/DN edge display | Low (pure noise) | Low |
| 5 | Boot banner `size=$1.00` wrong + no timestamp | Low (cosmetic, already flagged in README as deferred) | Trivial |
| 6 | `/status` `start=120s` ambiguous (confirmed: seconds *remaining*) | Low-medium (config/ops confusion risk, already caused one doc misreading) | Trivial |
