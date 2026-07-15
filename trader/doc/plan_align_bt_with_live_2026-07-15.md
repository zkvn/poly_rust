# Plan — align backtest reconciliation with live's real halt state

**Status: implemented, this doc (2026-07-15).** Written to record the design decisions before
they're lost in a commit message, per this repo's usual practice (`plan_*.md` docs land
alongside the feature, not months later).

## Problem

`trader/doc/incident_recon_btc_reversal_2026-07-15.md` found that the daily recon's "Live vs
BT" table showed zero backtest row for two real BTC trades. Root cause: the `backtest` binary's
own from-scratch `HaltTracker` trips on a stop-loss it independently simulates earlier in the
day (a cycle live never even traded) and has no way to know a human sent `/reset_losses btc` to
unstick live's *real* halt mid-day — so it silently suppresses every later entry that day,
producing a blank row with reason `unexplained` instead of an honest comparison. This is the
same *class* of gap as the pre-existing README TODO "Backtest reconciliation halt-state-drift
gap" (2026-07-10, about the *manual* `/halt` flag) — just for the loss-streak halt mechanism,
newly load-bearing now that `/reset_losses` actually works (shipped the same day, in
`trader/doc/incident_unable_to_resume_2026-07-15.md`).

The fix has two halves: **know what actually happened** (a reliable historical record of every
halt-state change, precisely scoped to which asset+strategy), and **use it** (stop trusting the
backtest's own naive simulation for reconciliation, and either explain or exclude rows the real
timeline accounts for).

## Design

### 1. `control_log.jsonl` — append-only, structured, timestamped

`bin/live.rs::log_control_event(slot, event)` appends one JSON line to a single
`{log_dir}/control_log.jsonl` (shared across every asset/strategy, not per-slot) whenever an
event can change `is_halted()`'s value — both user commands and automatic engine events:

```json
{"ts": 1784105384.0, "asset": "BTC", "strategy": "reversal", "event": "reset_losses",
 "entry_suppressed": false, "halt_losses": 0, "is_halted": false}
```

| `event` | Trigger | Call site |
|---|---|---|
| `halt` / `resume` / `reset_losses` | `/halt`, `/resume`, `/reset_losses` | `apply_control` |
| `drawdown_halt` | balance drawdown >25% | `apply_balance_halt` |
| `halt_engaged` | loss-streak (`halt_rev`/`halt_prob`) trips | `Action::HaltEngaged` |
| `halt_reset` | daily session rollover clears an active halt | `Action::HaltReset` |
| `halt_cleared_by_correction` | a Gamma correction pulls the streak back below threshold | `Action::HaltClearedByCorrection` |
| `gamma_halt_engaged` | Gamma never resolved in time | `Action::GammaHaltEngaged` |

This is exhaustive: every one of `Worker::is_halted()`'s possible causes (§8's `entry_suppressed`
— manual/drawdown/Gamma-unresolved — and the loss-streak `HaltTracker`) has exactly one place
that flips it, and every one of those places now logs. A write failure here is best-effort —
never interrupts live trading, same posture as `persist`.

**Why not reuse `config_log.rs`'s existing JSONL snapshot log?** That module already writes a
schema-compatible format but is never actually called from any Rust binary (`live.rs`,
`backtest.rs`) — confirmed dead code, existing only for its own unit tests and Python-bot
(`btc_5mins`) format parity. It also snapshots the *whole resolved config*, not halt-state
transitions specifically — reusing it would mean writing a large, mostly-redundant record on
every trade instead of a small one only when something relevant actually changes. A separate,
purpose-built log is simpler and matches what the daily recon actually needs to answer: "was
(asset, strategy) halted at time T, and why."

### 2. `trade_reconcile.py` — reconstruct the real timeline, use it two ways

- `parse_control_log` / `build_control_log_halt_windows` / `control_log_halt_window_at` — read
  the JSONL, turn it into `{(asset, strategy): [(start_ts, end_ts, reason), ...]}`, look up
  "was this halted at ts." Exact — no regex guessing, since every entry already carries its own
  `asset`/`strategy`/`is_halted`. Handles the daily reset "for free": `halt_reset`'s `is_halted:
  false` entry closes the window at exactly the real rollover moment the Rust side already
  computed (`reset_if_new_session`), no separate Python-side logic needed.
- **Reason labeling** (`classify_mismatch_reason`): checks the control-log windows *first*
  (precise), falls back to the pre-existing regex-over-`live.log`-text `build_halt_windows`
  (asset-*blind*, and never recognized the loss-streak halt's own Telegram messages at all) for
  anything the JSONL log doesn't cover — mainly history from before this feature shipped. Both
  are informational only.
- **Row exclusion** (`build_bt_vs_live`, "cycles the backtest fired but live did not trade"):
  a cycle the control log confirms was genuinely halted is dropped entirely, not counted as a
  missed opportunity — closes the 2026-07-10 TODO's own proposed fix ("tagging BT vs Live rows
  that fall inside a real halt window as 'as designed' rather than 'missed'"). **Only** the
  control-log source is trusted for this — unlike labeling, a false-positive *exclusion* here
  would silently hide a real signal for a different asset, which is worse than an unexplained
  row; the asset-blind legacy regex isn't precise enough to risk that.
- `run_backtest_reconciliation` now always runs `backtest --no-halt` — the whole point is that
  the binary's own internal `HaltTracker` is no longer trusted for reconciliation; halt truth
  comes entirely from the control log via the two mechanisms above.

### 3. Bootstrap gap (accepted, not solved)

The control log only exists from the moment this feature shipped — it can't retroactively know
about today's earlier 08:59:40→16:49:44 halt window before that. Same "optional enrichment,
degrades gracefully" posture as every other best-effort feature in this file (config-change
detection, Gamma-timeout parsing, halt-window regex itself): the legacy `build_halt_windows`
regex path stays in place specifically to keep covering that gap for reason-labeling (not
exclusion, per above), and going forward every halt transition is captured with full precision.

## Verification

- Rust: 4 new tests (`log_control_event` writes valid JSON with the right fields, is genuinely
  append-only, `apply_control` logs the correct event name per `ControlEvent` variant,
  `apply_balance_halt` logs `drawdown_halt`) plus the full existing suite.
- Python: ~20 new tests covering `parse_control_log` (parsing, missing file, malformed lines),
  `build_control_log_halt_windows` (open/close pairing, still-open-at-window-end, per-key
  isolation across assets/strategies, multiple halt/clear cycles), `control_log_halt_window_at`,
  `classify_mismatch_reason`'s new priority/scoping behavior, `build_bt_vs_live`'s exclusion
  (and that it's asset-scoped, and a no-op without control-log data), `run_rust_backtest`'s
  `--no-halt` flag, and `run_backtest_reconciliation`'s always-no-halt + control-log-wiring
  behavior — plus the full existing suite (98 tests → 120).
- Full local pipeline: `cargo test`/`clippy`/`fmt` clean; `docker compose build trader` clean;
  `deploy_trader.sh` to Oracle; today's recon regenerated post-deploy to confirm the two BTC
  trades from the motivating incident (16:54:46, 17:24:52) actually resolve differently now that
  a live `control_log.jsonl` exists to reconcile against.
