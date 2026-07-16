# Incident — BNB PNL on `/status` showed $-1.1579 with no visible trade to explain it (2026-07-16)

## Symptom

`/status` at 17:05:10 HKT showed:

```
BNB: 0W/0L/1SL/0UW  $-1.1579
    reversal   $-1.1579
    last: 14:09:02 UP STOPLOSS pnl=-0.7366
```

`0W/0L/1SL/0UW` and a single visible trade (`pnl=-0.7366`) implies the total should be `-0.7366`,
not `-1.1579` — a $0.4213 gap with nothing on the line to account for it. Reported as "PNL summary
for BNB is wrong."

## Root cause: the per-asset line omits the timeout count, but `total_pnl` always included timeout pnl

`slot.total_pnl` is not "today's" or "this session's" pnl in the everyday sense — it's a running
total carried in the persisted state file (`live_state_bnb_reversal.json`, restored on every
restart per `trader/src/bin/live.rs:1495-1505`'s "Restore halt state + /status counters from
before the last restart" comment) that accumulates over **every trade ever logged for that
(asset, strategy)**, going back to whenever the state file was first created — not reset daily,
not reset per restart. For BNB reversal that's four trades spanning 11 days
(`trader/live_logs/live_trades_bnb_reversal.csv`):

| Time | Outcome | PnL |
|---|---|---|
| 2026-07-05 22:04:27 | UNWIND | +0.0921 |
| 2026-07-13 15:34:11 | TIMEOUT | -0.1697 |
| 2026-07-15 14:59:43 | TIMEOUT | -0.2516 |
| 2026-07-16 14:09:02 | STOPLOSS | -0.7366 |

`live_state_bnb_reversal.json` confirms the live in-memory counters directly:

```json
"stats": {
  "wins": 0, "losses": 0, "stoplosses": 1, "unwinds": 0, "timeouts": 2,
  "total_pnl": -1.1579000000000002,
  "last_trade": "14:09:02 UP STOPLOSS pnl=-0.7366"
}
```

`-0.1697 + -0.2516 + -0.7366 = -1.1579` exactly — the 2026-07-05 UNWIND (+0.0921, a prior
config/window) had already rolled off by the time this state file was created, but **two TIMEOUT
trades are baked into the total** (`timeouts: 2`), correctly tracked in `slot.timeouts` all along —
`Action::LogTrade`'s handler (`live.rs:1152-1159`) increments `slot.timeouts` and
`slot.total_pnl` together, in the same atomic block, for every outcome type including `Timeout`.
The bug was never in the math.

The bug was in `render_status`'s per-asset line (`live.rs:808-811`, before this fix):

```rust
pnl_lines.push(format!(
    "  {name}: {}W/{}L/{}SL/{}UW  {sign}${:.4}",
    slot.wins, slot.losses, slot.stoplosses, slot.unwinds, slot.total_pnl
));
```

— four counters shown (W/L/SL/UW), but no `TO` (timeout). The aggregate `Session:` line one row up
*does* show a `TO` field (`{tw}W/{tl}L/{ts}SL/{tu}UW/{tt}TO`, `live.rs:833`) — only the per-asset
breakdown was missing it. Any asset with a nonzero `timeouts` count will show a total that looks
unexplained by the visible W/L/SL/UW breakdown; BNB's case was easy to notice because 2 of its 3
non-UNWIND trades were TIMEOUTs and the visible single STOPLOSS trade was a small fraction of the
total. `last_trade` compounds the confusion since it only shows the *most recent* trade, not a
breakdown — an operator has no way to see from `/status` alone that two older TIMEOUT trades are
part of the total.

## Fix

Added the missing `{}TO` field to the per-asset line, matching the `Session:` line's format
(`live.rs:808-811`):

```rust
pnl_lines.push(format!(
    "  {name}: {}W/{}L/{}SL/{}UW/{}TO  {sign}${:.4}",
    slot.wins, slot.losses, slot.stoplosses, slot.unwinds, slot.timeouts, slot.total_pnl
));
```

Renders as `BNB: 0W/0L/1SL/0UW/2TO  $-1.1579` — now self-explanatory. Purely a display fix; no
change to `total_pnl`, `stoplosses`, or any other accounting logic, since none of it was wrong.

## Explicitly not a bug, but worth knowing

- **These counters are not "session" stats in the intuitive sense** — they never reset (no
  `/reset_pnl` command exists; only `/reset_losses` for the separate loss-streak halt counter) and
  persist across restarts by design. The `Session:` label at the top of `/status` is a bit
  optimistic — for an asset whose state file has existed a while, it's closer to "all-time since
  this state file was created." Not touched by this fix (out of scope — a labeling/product
  question, not a correctness bug), but noted here in case it's ever raised again.

## Verification

- `cargo build`, `cargo test` (249 tests across all `trader` binaries, 0 failures), `cargo clippy
  --all-targets --all-features -- -D warnings` (clean), `cargo fmt --all --check` (clean).
- Manually rendered the new format string against BNB's actual persisted numbers
  (`0W/0L/1SL/0UW/2TO  $-1.1579`) — matches the state file exactly.
- **Not yet deployed** — `trader-live.service` on Oracle is running live capital; redeploying it is
  a separate, explicit step (`scripts/deploy_trader.sh` per the main README), not bundled into this
  fix without asking first.
