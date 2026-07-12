# Incident — `poly-collector` crash-looping and destroying its own recoverable data, ongoing since 2026-07-10 22:30

**Status as of this write-up: still active.** `poly-collector.service` on Oracle is alive right now
but has restarted 179 times since 2026-07-10 22:30 and will keep doing so until fixed. No code
changed as part of this investigation, per the request that motivated it — this is root-cause +
proposed solutions only.

## Problem statement

Local/Oracle `price_feed/raw/` tick coverage (both `poly` and `binance`) collapsed from ~93% of
minutes on 2026-07-10 to **~14-15%** on 2026-07-11 and 2026-07-12, for every asset checked
(ETH/DOGE/BTC) — first surfaced in `trader/doc/incident_delta_pct_2026-07-12.md` and flagged
urgent in this repo's README TODO. This doc investigates why, prompted by the hypothesis that it
might simply be a case of not having run `price_feed/scripts/recover_rust_parquet.py`.

## Investigation

**Ruled out: local/Oracle sync lag.** `rsync -avzc --dry-run --itemize-changes` (checksum mode,
not just mtime/size) against Oracle's own `raw/` directory for the affected files shows zero
differences — the local copy is byte-identical to Oracle's. Whatever's wrong is on Oracle itself,
not a stale local cache.

**Ruled out (partially — see below): "just run the recovery script."** `recover_rust_parquet.py
--check` against the affected sealed hourly files reports `0 bad` — the files that exist are
structurally valid, sealed, readable parquet. There's no footerless/corrupted *sealed* file lying
around waiting to be recovered. But as the root cause below shows, this framing turns out to be
closer to right than a first pass suggests — just one layer removed: it's not that a recovery step
was skipped by a human, it's that the **collector itself needs to do the equivalent of what that
script does, and doesn't** (see Root cause 2).

**Found: `poly-collector.service` is crash-looping.** `systemctl status` shows the process has only
been up for a few minutes at any given check; `journalctl -u poly-collector` shows a restart
counter climbing continuously — 179 and counting as of this write-up, roughly every 5-30 minutes.

```
Active: active (running) since Sun 2026-07-12 13:45:02 HKT; 10min ago
```

## Root cause 1 — trigger: the phase-2 staleness reconciler fires far more than its design target

Every single restart in the crash-loop is preceded by the exact same log line, and the count
matches exactly:

```
$ journalctl -u poly-collector | grep -c 'RECONCILE-STALE'
179
$ journalctl -u poly-collector | grep 'restart counter' | tail -1
... restart counter is at 179.
```

This is `price_feed/src/reconcile.rs` — the **phase 2** fix from
`price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md` (README's "bba/price WS feed can
silently stop delivering" entry), deployed **2026-07-10 22:30:02** (confirmed: that's the exact
timestamp of the very first `[RECONCILE-STALE]` line in the whole log). It polls Polymarket's REST
`/midpoint` every 5s and, if it disagrees with the WS-cached price by >0.03 for 2 consecutive
polls, logs and calls `std::process::exit(1)` — relying on `Restart=always` to recover. This is
**working as designed** in the narrow sense that it correctly detects a real disagreement between
REST and WS — but it's firing roughly 90×/day, not the "rare, multi-hour" event (the original
205s DOGE/ETH incident) it was built for.

**Why it fires so often:** a large fraction of triggers show `rest_mid=0.0050` — a near-zero
value — paired with a WS-cached price nowhere near zero, recurring across many *different*,
unrelated assets (HYPE, BNB, DOGE, SOL, BTC, ETH) at essentially random times:

```
[RECONCILE-STALE] HYPE rest_mid=0.0050 cached_mid=0.0650 diff=0.0600 — ...
[RECONCILE-STALE] BNB  rest_mid=0.0050 cached_mid=0.4000 diff=0.3950 — ...
[RECONCILE-STALE] DOGE rest_mid=0.0050 cached_mid=0.1100 diff=0.1050 — ...
```

This pattern (same near-zero value, many assets, no obvious clustering) is far more consistent
with **genuine, benign near-resolution price behavior** — one side of a 5-min market's true price
naturally crashes toward 0 in its final moments as the outcome becomes near-certain, and the order
book often goes thin/quiet right then too (few market makers still quoting) — than with a
systemically broken WS feed. With 7 assets each cycling every 5 minutes, the chance that *some*
asset is in this state at any given 5s poll is high. Not fully proven asset-by-asset here (would
need per-trigger orderbook forensics), but the trigger *rate* alone (every 5-30 min, sustained for
2+ days) is clear evidence the detector's sensitivity is miscalibrated for how often this
legitimately-quiet condition occurs, independent of whether every individual trigger was "wrong."

## Root cause 2 — amplifier: every restart destroys its own recoverable data before recovery can run

This is the part that actually explains the ~85% *data* loss (as opposed to just restart *count*)
and is the direct answer to "did you just not run the recovery script."

`spawn_reconcile_task` calls `std::process::exit(1)` immediately on confirmed staleness
(`collect.rs:1213`) — an abrupt exit that skips Rust's normal drop/cleanup path entirely. Unlike a
graceful `SIGTERM` (which calls `finish()` → `ArrowWriter::close()`, writing the parquet footer),
`std::process::exit` never runs that path, so the current hour's `.tmp` file is left **footerless**.

On restart, `BinanceWriters::new()` (and the poly/book writers) call `ParquetBuf::open_for_hour()`,
which is *supposed* to carry forward exactly this kind of leftover:

```rust
// collect.rs:306-311
// Opens a fresh writer at `tmp_path` for the current hour, carrying forward rows
// from whichever of `tmp_path` (a crash left it mid-write) or `final_path` ... exists.
fn open_for_hour(tmp_path: PathBuf, final_path: &Path, schema: Schema) -> Result<Self> {
    let carry = carry_source.as_ref().and_then(|p| Self::try_carry(p, &schema));
    let mut buf = Self::open(tmp_path, schema)?;   // <-- truncates tmp_path unconditionally
    ...
}
```

`try_carry` (`collect.rs:337`) reads via `ParquetRecordBatchReaderBuilder::try_new` — a **standard**
parquet reader that requires a valid footer to locate anything at all. A footerless `.tmp` left by
`std::process::exit` fails this read (`.ok()?` → `None`), so carry silently recovers nothing. The
very next line, `Self::open(tmp_path, ...)`, opens the **same path** with `.truncate(true)`
(`collect.rs:293`) — physically destroying whatever raw bytes were sitting in that footerless
`.tmp`, **before** `recover_rust_parquet.py`'s raw-page decoder (which exists specifically to read
footerless files exactly like this one) ever gets a chance to run on it. This all happens
automatically, in milliseconds, on every single restart — faster than any human could intervene.

**This is confirmed, not inferred**, by exact timestamp correlation. Hour 09's sealed
`ETH_binance_2026-07-12_09.parquet` (89KB, "reasonable" size) spans exactly:

```
2026-07-12 09:50:13.750000 -> 2026-07-12 10:00:00
```

The last restart inside that hour was at **09:50:12** — one second before the file's first row.
Every minute of data from 09:00:00 to 09:50:12 (three separate `RECONCILE-STALE` restarts happened
in that span: 09:00:06, 09:15:02, 09:50:07) is gone — not corrupted, not lying around
unrecovered, **overwritten**. Same for hour 10's tiny (5KB) file: spans exactly
`10:59:36.something -> 10:59:59.75`, one restart's width (10:59:28 → ~10:59:33) after the hour's
4th crash that hour. **Every hourly file's actual start time exactly matches its hour's last
restart, not its hour's true start** — proof the carry-forward is failing every time, not just
sometimes.

**Notably asymmetric with existing, safer code in the same file:** `seal_orphaned_tmp`
(`collect.rs:410`, handles a `.tmp` left over from a *previous*, now-stale hour, at startup only)
uses the same `try_carry`, but on failure it explicitly **leaves the orphaned `.tmp` in place**
("`no recoverable rows, leaving orphaned tmp in place`") rather than destroying it — a human or a
future automated pass could still run the raw-page recovery script on it afterward.
`open_for_hour`'s same-hour path has no equivalent safety net; failure there is immediately
followed by a truncating open of the identical path.

## Additional impact: this also interrupts the live trader, not just recorded data

`poly-collector` is the sole publisher of live Binance/Poly ticks to NATS
(`--nats-url nats://127.0.0.1:4222`), which `trader-live.service` subscribes to instead of opening
its own WebSockets (README's "Oracle infra: NATS price bridge"). Every one of these 179 restarts —
not just the 5-30s of `Restart=always` downtime but also the WS/NATS resubscription warm-up after
each one — is a live gap in the price feed the actual trading engine sees, not just a gap in the
historical record. This raises the severity beyond "backtest reconciliation fidelity."

## Is the historical data recoverable now?

**Mostly no, going backward.** Every restart's truncating `open()` has already overwritten the
prior instance's `.tmp` bytes for that hour by the time this investigation ran — there's no
special backup of the pre-truncation bytes. The only genuinely recoverable window would be if a
crash happens *again* before the next restart's `open()` call truncates it — a race that isn't
practically catchable by hand. Nothing was done here to try (no code/service changes, per the
scope of this investigation) — flagging as effectively-lost rather than attempting a manual
grab, since by the time this doc is read, further restarts will already have occurred.

## Proposed solutions (not implemented — investigation/proposal only)

1. **Reduce false-trigger rate (cheap, immediate).** Widen `reconcile.rs::MISMATCH_TOLERANCE`
   (currently `0.03`) and/or raise `CONSECUTIVE_MISMATCHES_REQUIRED` (currently `2`), or better,
   make the check aware of time-to-cycle-close and relax/skip it in a market's final ~10-30s when
   thin-orderbook near-resolution divergence is expected and benign. Doesn't fix the amplifier
   (root cause 2) but directly reduces how often it's triggered.

2. **Fix the amplifier — the actual data-loss mechanism (the real fix).** Two independent options,
   not mutually exclusive:
   - **Make the reconcile-triggered exit graceful.** Instead of `std::process::exit(1)` directly
     in `spawn_reconcile_task`, signal the same shutdown path `SIGTERM` already uses (which calls
     `finish()`/writes the footer) — e.g. a shared `AtomicBool`/channel the main loop checks, or
     send `SIGTERM` to itself (`libc::raise` or similar) instead of exiting directly. This alone
     would make every restart's `.tmp` file properly sealed and readable, closing the data-loss
     gap even with the crash-loop rate unchanged.
   - **Give `try_carry` the same raw-page-recovery fallback `recover_rust_parquet.py` already has**
     for exactly this shape of file (footerless, truncated by an abrupt exit) — port that Python
     decoder's approach into Rust (or call it as a subprocess, though a native fallback is cleaner
     for a hot restart path) as a second attempt inside `try_carry` when the standard reader fails.
     This closes the gap even if some future code path still exits abruptly.

3. **Guard rail: don't destroy what you can't read.** Make `open_for_hour` mirror
   `seal_orphaned_tmp`'s safer behavior — if `try_carry` returns nothing for the *current* hour's
   `.tmp` and the file is non-trivially sized, rename it aside (e.g. `.tmp.unrecovered-<ts>`)
   instead of truncating it in place, so a later manual (or automated) recovery pass has something
   to work with. Cheap, defense-in-depth, doesn't require solving 2a/2b first.

4. **Observability — this ran silently for 2+ days.** Nothing paged anyone; this was found only by
   manually SSHing in during an unrelated investigation. Add a restart-count or
   `RECONCILE-STALE`-rate alert (Telegram, matching the trader's existing alerting conventions) so
   a crash-loop like this surfaces within minutes, not by accident days later.

Recommended order: **3 first** (smallest, safest, immediately stops future data destruction even
before anything else lands), then **2a or 2b** (the real fix), then **1** (reduces how often the
whole mechanism fires at all, independent of whether it's now safe), then **4** (so the next one
doesn't take 2 days to notice).
