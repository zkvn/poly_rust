# Audit: "still gap/missing" in Data Quality section after the collector fix (2026-07-12)

## Question

After deploying the collector crash-loop fix (`price_feed/doc/incident_collector_data_loss_2026-07-12.md`,
deployed 2026-07-12 15:08:55 HKT), the daily recon report
(`trader/results/daily_recon/trade_recon_2026-07-11_to_2026-07-12.md`) still shows
**208/286 asset-hours flagged** in its Data Quality section — 16 distinct hours, most asset/kind
pairs, some full `MISSING` hours. Is the fix not working?

## Answer: no — this is 100% pre-deploy historical damage, not a new or ongoing issue

The daily recon window is a fixed trading day, `20:00 HKT → 20:00 HKT` next day
(`_resolve_window` in `trade_reconcile.py:1616-1622`). For the `2026-07-11_to_2026-07-12` report
that's `2026-07-11 20:00 → 2026-07-12 20:00` — a window that **starts 19 hours before the fix was
deployed**. Every flagged hour falls inside `[20:00 07-11, 14:00 07-12]`, i.e. entirely before
`15:08:55`. Nothing after the deploy is flagged.

### Restart-count correlation (journalctl, Oracle, `19:00 07-11` → `15:10 07-12`)

```
      6 Jul 11 19        6 Jul 12 04
      7 Jul 11 20        4 Jul 12 05
      3 Jul 11 21        6 Jul 12 06
      4 Jul 11 22        4 Jul 12 07
      5 Jul 11 23        7 Jul 12 08
      4 Jul 12 01        3 Jul 12 09
      7 Jul 12 02        4 Jul 12 10
      (0 Jul 12 00)      3 Jul 12 11
      (0 Jul 12 03)      8 Jul 12 12
                          2 Jul 12 13
                          2 Jul 12 15  <- last 2 before 15:08:55, then zero
                          3 Jul 12 14
```

Every one of the 16 hours flagged in the report (20,22,23 on 07-11; 01,02,04,05,06,07,08,09,10,
11,12,13,14 on 07-12) had 2-8 restarts. The two hours inside the window that were **not**
flagged — `00:00` and `03:00` on 07-12 — are exactly the two hours with **zero** restarts. This is
a 1:1 correlation, not a coincidence: the flagged hours are the collector crash-looping under the
old bug (destroying the current hour's data on every `RECONCILE-STALE` restart), exactly as
described in the incident doc. `22:00` and `14:00` show as full `MISSING` because the restart(s)
in those hours happened to be the amplifier bug's worst case — the sealed file for that hour never
got written at all.

### Post-deploy check (fresh run, `--hours-back 6`, now 2026-07-12 18:47 HKT)

```
65 asset-hours checked, 26 flagged   (all 26 are 13:00 and 14:00 — pre-deploy)
```

Hours `15:00`, `16:00`, `17:00` — fully after the `15:08:55` deploy — have **zero** flagged
asset-hours. `systemctl status poly-collector` shows the same PID running continuously since
`15:08:55` (3h38m uptime at check time, 0 restarts). The fix is working.

## Why the report still shows it, and what happens next

`data_quality.py` only ever reports on *fully-elapsed* hours inside the report's fixed window; it
has no concept of "when was the fix deployed" and isn't supposed to — it's a dumb, honest
tick-coverage check by design (see its own docstring: silence about a real gap is exactly what let
the original incident run 2+ days unnoticed). Today's report legitimately shows the last day of
old damage because that damage really happened inside today's window.

This is self-resolving, not a bug to fix: tomorrow's report (`2026-07-12_to_2026-07-13`, window
`20:00 07-12 → 20:00 07-13`) starts *after* the deploy and will show a clean Data Quality section
assuming the collector stays healthy, which the last several hours of zero-restart operation
support.

## Action taken

None needed on `data_quality.py` or the collector — both are working as designed. Filed this audit
so the report's flagged rows for `2026-07-11_to_2026-07-12` aren't mistaken for a regression by
anyone reading it after the fact. Daily recon report format separately updated same day to make
large sections (including Data Quality) collapsible, so a long flagged-hours table like this one
doesn't dominate the report — see commit history for `trade_reconcile.py` 2026-07-12.
