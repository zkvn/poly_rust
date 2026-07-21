# Incident — Telegram boot banner / `/status` showed size=$1.00 instead of $5.00

**Status: found, fixed, tested — same day as the report.**

## Symptom

After deploying `strategy_20260721_taker.toml` (`trade_size_usdc = 5.0`,
`trader/doc/plan_aggressive_taker_entry_2026-07-21.md`), both the `🟢 live driver started`
Telegram banner and the `/status` per-asset lines showed `size=$1.00`, not `$5.00`.

## Root cause

`trader/src/bin/live.rs`'s per-slot config resolution:

```rust
let mut params = toml.resolve_for_duration(asset, &dur_label)?;
params.trade_size_usdc = args.size_usdc;
```

`args.size_usdc` is the `--size-usdc` CLI flag, which was declared `f64` with
`#[arg(long, default_value_t = 1.0)]`. This line **unconditionally overwrites** every slot's
config-resolved `trade_size_usdc` with the CLI value — and since no deploy path
(`scripts/deploy_oracle.py`, `scripts/deploy_trader.sh`, the systemd unit) has ever passed
`--size-usdc`, `args.size_usdc` was always exactly `1.0`. Every run since this line was added
has silently discarded whatever `[trade_size_usdc]` the strategy TOML actually specified.

Both display sites (`/status`'s `size_str`, the boot banner's `asset_strategy_summary`) already
read the *correct* field, `slot.params.trade_size_usdc` / `s.params.trade_size_usdc` — they were
fixed for the maker-entry display bug on 2026-07-19 (README's matching entry). The bug is
entirely upstream of both: the field they read had already been clobbered before the `Worker`/
`AssetSlot` were even constructed.

**Not a display-only bug.** `Worker::new_reversal(asset, &params)` is built from this same
corrupted `params`, so `Worker::common()`'s `trade_size: p.trade_size_usdc` — the value that
actually goes into `Action::PlaceBuy { size_usdc, .. }` — was wrong too. Real order sizing was
affected, not just what Telegram showed.

**Introduced 2026-07-17** (`git blame` → commit `7b58270`, "additive 15m/1h-et/4h crypto
durations + gated weather module" — unrelated to sizing, the override was folded in
incidentally). **Dormant until today** for two independent reasons that both broke at once:

1. Every paper run since 2026-07-19 used `maker_entry = true`, where entries are a fixed
   `MIN_GTC_SHARES` (5-share) GTC quote — `trade_size_usdc` plays no role in sizing that path at
   all (same reason the display bug this file's sibling incident describes went unnoticed there
   too).
2. Every config's `[trade_size_usdc]` happened to already default to `1.0` — so even on the
   rare path where it mattered, the override was a silent no-op that matched the config anyway.

`plan_aggressive_taker_entry_2026-07-21.md` §2.2 raised `trade_size_usdc` to `5.0` specifically
*because* the new taker-entry path needs it to reliably clear `MIN_GTC_SHARES=5` for the
take-profit exit — the exact value this bug was silently discarding. Left unfixed, every taker
entry since the earlier deploy today would have sized at $1 (≈1.2–1.8 shares at typical
reversal entry prices of 0.55–0.85), well under the GTC floor — the take-profit exit would
never actually rest as a GTC sell, quietly defeating the sizing fix the new config was deployed
for. **Any paper trades logged between the aggressive-taker-entry deploy (2026-07-21 ~12:56
HKT) and this fix's redeploy sized at $1, not $5** — worth excluding from any size-sensitive
read of `paper_trades_*.csv` for that window.

## Fix

- `Args::size_usdc`: `f64` (default `1.0`) → `Option<f64>` (no default) — same "explicit-opt-in
  override" shape `nats_url`/`weather_config` already use in this struct. `None` (every
  production invocation) leaves each slot's own config value untouched.
- Extracted the override into a small pure function, `resolve_trade_size_usdc(config_value,
  cli_override) -> f64` (`cli_override.unwrap_or(config_value)`), so the exact regression this
  incident describes has a direct unit-test target instead of living inline in `main()`.
- The startup console diagnostic (`[live] assets=... size_usdc=...`) now prints `config default
  (per-slot trade_size_usdc)` when no override is given, instead of a number that looked
  authoritative but never was.

## Tests

- `resolve_trade_size_tests::no_cli_override_keeps_config_value` — `None` leaves `5.0`/`1.0`
  config values alone (the exact regression case: a `1.0`-shaped default must not leak in when
  the config asks for something else).
- `resolve_trade_size_tests::explicit_cli_override_wins` — an explicit `--size-usdc` still wins,
  preserving the flag's original ad-hoc-testing purpose.
- Full suite: 303 lib + 5 `backtest` + 52 `live` (both new tests) — all green.
  `cargo fmt --all --check` / `cargo clippy --all-targets --all-features -- -D warnings` clean.
- Local verification: `live --paper` startup console line reads `size_usdc=config default
  (per-slot trade_size_usdc)` with no `--size-usdc` flag passed (previously printed `$1.00`
  unconditionally).

## Deploy

`./scripts/deploy_trader.sh` (trader-only, same as the aggressive-taker-entry deploy earlier
today) — no config change needed, this is a binary-only fix.
