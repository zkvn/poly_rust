# Paper mode had no balance sample — the 25% "total pnl stop loss" was silently inert

**Status: found, fixed, tested — same day, follow-up to the taker-entry switch.**

## What was missing

`BalanceGuard` (`trader/src/balance.rs`) is the account-level circuit breaker: once a session
baseline balance is sampled, a later sample showing more than 25% drawdown from that baseline
halts new entries on every asset ("total pnl stop loss," as distinct from each strategy's own
per-trade `sl_pnl_rev`). It's fed once per cycle (`args.balance_check_offset_secs` into each
cycle window) from `LiveExecutionEngine::fetch_balance()` — a real CLOB collateral-balance API
call.

Paper mode (`PaperExecutor`) has no CLOB client at all, by design (`asbuilt_unwind_5u_maker_2026-07-19.md`
§5: "compile-time impossibility of a real order, not a runtime check"). `Driver::live_engine` is
`None` under `--paper`, so the periodic check's `bal` sample was always `None` — `BalanceGuard`
and `GammaBalanceTracker` (the sibling "balance decreased vs last cycle" scoped halt) both treat
`None` as "unknown, don't halt" (fail-open, matching the "a failed fetch skips the check"
design). Net effect: **the account-level drawdown halt has never once fired during any paper
run**, including the current aggressive-taker-entry window — a losing session had no backstop
beyond each individual trade's own stop-loss/timeout.

## Fix

- `PAPER_STARTING_BALANCE_USDC: f64 = 50.0` — a fixed synthetic starting balance for paper mode.
- `paper_balance(starting_balance, pnls) -> f64` — starting balance plus the sum of every
  `AssetSlot`'s running `total_pnl` (the same aggregate `render_status`'s `/status` PNL section
  already computes — `slot.total_pnl` already accumulates every closed trade's realized pnl,
  including Gamma corrections, via `Action::LogTrade`/`Action::LogTradeCorrection`, so no new
  state needed).
- The periodic balance-check arm now samples `paper_balance(...)` instead of `None` when
  `mode == RunMode::Paper`, feeding **both** `BalanceGuard` (25% drawdown) and
  `GammaBalanceTracker` (scoped decrease-halt) — both were equally inert before, both get a real
  sample now.
- `/status`'s balance line shows `$X.XXXX (paper, $50.00 start)` for paper mode instead of the
  previous `n/a (dry-run)` (which was also technically wrong for paper — that label was only
  ever accurate for `--dry-run`).
- Scope: **paper mode only**. `--dry-run` (`SimExecutionEngine`) is left exactly as it was
  (`n/a (dry-run)`, drawdown check inert) — not part of this request, not touched.

## Tests

`paper_balance_tests` (`trader/src/bin/live.rs`):
- `no_trades_yet_is_exactly_the_starting_balance` — zero slots, zero pnl → `$50.00` exactly.
- `sums_pnl_across_every_slot` — mixed win/loss slots sum correctly.
- `losses_can_drop_the_balance_below_starting` — a losing session can go negative;
  `paper_balance` doesn't clamp (correct — `BalanceGuard`'s drawdown ratio handles it).
- `drives_balance_guard_drawdown_halt_end_to_end` — feeds `paper_balance` output straight into a
  real `BalanceGuard`: a 20% loss doesn't halt, a 26% loss does. This is the actual "total pnl
  stop loss" behavior end to end, not just the arithmetic in isolation.

Full suite: 303 lib + 5 `backtest` + 56 `live` (4 new) — all green. `cargo fmt --all --check` /
`cargo clippy --all-targets --all-features -- -D warnings` clean. Local `live --paper` smoke run
(no credentials, no Telegram): clean startup and shutdown, no panics.

## Deploy

`./scripts/deploy_trader.sh` (trader-only, binary-only change — no config edit needed).
