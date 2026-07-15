# Fix — config fixture-drift tests refreshed for strategy_20260715.toml, 2026-07-15

**Source:** flagged in this repo's own `README.md` TODO on 2026-07-14 ("`trader/src/config.rs`/
`config_log.rs` have 4 pre-existing test failures from config drift"), fixed while deploying the
same-day `strategy_20260715.toml` update that switched to `btc_5mins`
`studies/reversal_hourly/summary.md`'s "By win_rate" table and changed `trade_assets` to
BTC/BNB/SOL.

## Root cause

`config::tests::load_and_resolve_btc`, `config::tests::default_fallback`,
`config::tests::unwind_time_falls_back_to_default_and_resolves_asset_override`, and
`config_log::tests::write_and_read_roundtrip` all call `load_latest`, which reads whichever
`strategy_*.toml` is lexicographically newest right now — not a fixture file the tests own.
Each of these 4 tests hardcodes expected resolved values (`delta_pct_rev`, `halt_rev`,
`unwind_time_rev`, `reversal`, `trade_assets` membership) that were correct for whatever config
was live when the test was last updated, and silently go stale the next time a live config
change lands, with no compile-time signal that it happened. This is the same drift class the
`load_and_resolve_btc` test's own comment already documents being fixed once before, on
2026-07-09. Confirmed pre-existing on `main` (reproduces via `git stash` before this fix's config
edit), not introduced by this deploy — but this deploy's `trade_assets` and per-asset param
changes tipped 4 more assertions over the edge:

- `delta_pct_rev`, `unwind_pnl_rev`, `sl_pnl_rev`, `unwind_time_rev`, `halt_rev` — hardcoded
  numbers no longer matched the new `strategy_20260715.toml` values.
- Two tests (`default_fallback`, `unwind_time_falls_back_to_default_and_resolves_asset_override`)
  specifically exist to demonstrate the "no per-asset override, falls back to `default`" code
  path using BTC as the example asset — but this deploy gave BTC its own explicit
  `delta_pct_rev`/`unwind_time_rev` overrides, so BTC no longer exercises that path at all.
- `write_and_read_roundtrip` asserted `trade_assets` contains `ETH` — no longer true now that
  `trade_assets` is BTC/BNB/SOL.

## Fix

Refreshed the 4 tests' hardcoded values against the new config (verified via
`toml::from_str` + manual resolution, then `cargo test`). For the two fallback-path tests,
switched the demonstrating asset from BTC to SOL — SOL has no `delta_pct_rev`/`unwind_time_rev`
override in the new config, so it now genuinely exercises the default-fallback path the tests
are meant to cover, instead of coincidentally reading a real override's value. `cargo fmt --all
--check` and `cargo clippy --all-targets --all-features -- -D warnings` both clean; full
`cargo test` green (199 passed, 0 failed). Committed separately from the config change itself
(`test(config): update fixture values for strategy_20260715.toml drift`), since
`deploy_oracle.py --update-config`'s git commit is deliberately scoped to `trader/config/` only
and would not have picked up a `.rs` change.

This is a recurring maintenance cost, not a one-off — any future live config change that alters
which asset has which override, or changes `trade_assets` membership, can retrigger the same
drift. No structural fix attempted here (e.g. deriving expectations from the config file itself
instead of hardcoding); flagging that as a possible follow-up, not doing it now.
