# Plan — relax BTC/SOL `delta_pct_rev`, start a fresh 24h observation window

Status: **executable, implementing now.** Follow-on to
`plan_aggressive_taker_entry_2026-07-21.md` §6 (evaluation of the first
~20h under `strategy_20260721_taker.toml`) and
`trader/doc/recon_taker_entry_24h_2026-07-22.md` (full 12-trade recon of the
~24.5h window since that deploy).

## 1. Problem

Across the entire ~24.5h window since the 2026-07-21 12:56 HKT taker-entry
deploy, **BTC and SOL fired zero trades** while every other asset (BNB,
DOGE, ETH, XRP) traded at least twice. `delta_pct_rev` is the minimum
underlying-price move (Binance, cycle-open to current) required before a
reversal entry candidate is even considered — BTC and SOL were both at
`0.0008`, the tightest value in the 24h re-pick's `[0.0003, 0.0008]` band;
BNB was already relaxed to `0.0005` in the same config and traded 5 times.
Per the user: relax BTC and SOL to `0.0005` to match BNB and observe another
24h window, rather than continuing to watch two assets sit idle.

This is a parameter change only — no mechanism change (unlike
`plan_aggressive_taker_entry_2026-07-21.md`, which fixed a structural
fill-rate problem). `delta_pct_rev` for BTC/SOL was already inside the
24h re-pick's tested range, just at the tight end of it.

## 2. Change

`trader/config/strategy_20260721_taker.toml`, `[delta_pct_rev]`:

```
BTC  = 0.0008 -> 0.0005
SOL  = 0.0008 -> 0.0005
```

Every other reversal per-asset parameter (`reversal`, `reversal_low_threshold`,
`unwind_pnl_rev`, `unwind_time_rev`, `sl_reversal`, `sl_pnl_rev`) is
unchanged for every asset, and ETH/XRP/DOGE's own `delta_pct_rev` values are
unchanged. `meta.source` gets a same-day update note (this repo's existing
convention, matching the 2026-07-21 `sl_reversal` update in the same file).

`trader/src/config.rs::load_and_resolve_btc` and
`trader/src/config_log.rs::write_and_read_roundtrip` both asserted the old
BTC `delta_pct_rev == 0.0008` value directly against the live config file —
updated to `0.0005` alongside this change (compile/test-driven, not a
behavior change to the code itself).

## 3. Test plan

- `cargo test` (full `trader` suite) — the two BTC-specific config
  assertions above are the only ones this change touches; everything else
  (execution, worker, backtest golden tests) is parameter-blind to this
  file's contents beyond what those two tests already check.
- `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`.
- No local soak needed — this is a pure threshold relax on an already-live,
  already-tested entry mechanism (taker entry, 800ms latency sim, tp_price
  cap, wall-clock timeout re-check are all unchanged and already verified
  against real trades in the prior window).

## 4. Rollout

1. This doc — pushed first (alongside `recon_taker_entry_24h_2026-07-22.md`).
2. Config change + test updates (§2), full local test pass (§3).
3. Deploy: `./scripts/deploy_trader.sh` — trader-only, no `price_feed` change.
4. Post-deploy verification (within 15 min): clean restart
   (`journalctl -u trader-live`), config log shows the new BTC/SOL
   `delta_pct_rev` on the next `startup`/cycle-open snapshot.
5. Let it run; a follow-up evaluation is out of scope for this plan and will
   be written once another meaningful window has accrued — same shape as
   `plan_aggressive_taker_entry_2026-07-21.md` §6 and
   `recon_taker_entry_24h_2026-07-22.md`. Specifically watch: did BTC/SOL
   fire at all this time, and if so did the wider entry gate stay net
   profitable, or did the tighter delta let in worse-quality entries.
