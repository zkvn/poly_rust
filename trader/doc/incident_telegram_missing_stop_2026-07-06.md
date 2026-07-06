# Incident — ETH STOPLOSS follow-up verdict Telegram message never received, 2026-07-06

Telegram alert (received):

```
❌ ETH TRADE STOPLOSS | 22:04:54 | DOWN ↓ | reversal
entry=0.8400 → exit=0.0200 | cycle: $1744.89→$1745.50 | pnl=-$0.9886 | 0W/0L
```

Expected follow-up (never received): the `StopLossVerdict` message (🟢/🔴 `... STOP GOOD/COSTLY`), sent by `spawn_resolution_watcher` once Gamma resolves the market (`worker.rs:822-840`, `live.rs:649-658`).

## 1. The stop-loss itself is genuine

`live_trades_eth_reversal.csv`:

```
1783346694.6241856,eth-updown-5m-1783346400,reversal,DOWN,1783346653.741,0.84,0.020000000000000004,STOPLOSS,-0.9886,0,,46.96321487426758,466.3710594177246,244.42672729492188,704.7982215881348
```

`exit_attempts=0`, `exit_last_error` empty — the sell filled cleanly on the first try. The daily recon (`log/recon_cron.log:982`) independently queried Gamma at 22:20:06 and confirmed `eth-updown-5m-1783346400: UP` — the DOWN position really was on the wrong side of the move. Not a pricing/execution bug.

## 2. Investigation into the missing verdict message

- `live.rs:619-623` spawns a resolution watcher unconditionally after **every** `LogTrade` (win/loss/unwind/stoploss alike); only a `StopLoss` record's watcher result produces a `StopLossVerdict` (`worker.rs:822-840`).
- `Driver::notify` (pre-fix) only logged on failure — success was silent, so no positive log evidence could ever confirm a send.
- `live.log`, checked both via the rsync'd copy and directly on the Oracle box (`ssh ubuntu@10.8.0.1`), shows `API pending (attempt N/20)` for attempts 1–10 (~22:05:24–22:09:54) then **nothing further** for this slug — no attempt 11, no "gave up waiting" line. That pattern only occurs when the watcher gets `Some(went_up)` and returns without printing (`live.rs:131-136`).
- Ruled out a crash/restart: `trader-live.service` (PID 98311) has been running continuously since 2026-07-06 14:43:29 with no restart around the trade; `live.log` continues seamlessly through subsequent cycles.
- `grep -c "telegram] send error"` on the full log → **0**.
- Conclusion: every log signal is consistent with the watcher resolving (~22:10:24) and `notify()` successfully sending — but "consistent with success" and "silent because of a bug" looked identical in the old code. There is no way, from the pre-fix logs, to actually distinguish "sent successfully but not delivered/seen on the Telegram side" from "silently failed to send."

## 3. Root cause (best current assessment)

Unresolved with certainty. The actual defect this incident exposed is an **observability gap**, not a proven code bug: `notify()` had no success log, so a genuine send and a silent failure were indistinguishable after the fact. Given the user did not receive the message and the logs show no failure, the two live hypotheses are:
- the message was sent and lost/missed on the Telegram delivery or client side, or
- some send-time failure mode exists that isn't surfaced as an `Err` from `bot.send()` (e.g. a Telegram API quirk).

This doc does not resolve which one occurred — the fix below is what makes the next occurrence provable.

## 4. Fix implemented

`Driver::notify` (`trader/src/bin/live.rs`) now logs on success too:

```rust
async fn notify(&self, text: &str) {
    if let Some(bot) = &self.telegram {
        match bot.send(text).await {
            Ok(_) => println!("[telegram] sent: {}", text.lines().next().unwrap_or("")),
            Err(e) => eprintln!("[telegram] send error: {e:#}"),
        }
    }
}
```

## 5. TODO

- [ ] Next stop-loss: confirm `[telegram] sent: ...` appears in `live.log` within ~30s of the resolution watcher's last "API pending" line, and cross-check against actual Telegram receipt.
- [ ] If success is now logged but the message is still not received, this points at the Telegram side (bot token, chat_id, rate limiting, `getMe`/`getChat` health) rather than this codebase — the missing piece this incident lacked.
- [ ] Consider logging the resolution watcher's own outcome explicitly (`resolved: went_up=…` vs `gave up after 20 attempts`) instead of only inferring it from the absence of further "API pending" lines — silence was the only signal available here, which is what made this hard to diagnose.
- [ ] Optional: a periodic Telegram health self-ping if silent delivery issues recur.
