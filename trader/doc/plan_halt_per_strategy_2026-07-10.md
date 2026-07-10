# Plan — scope `/halt` and `/resume` to a single strategy per asset

## 1. What this adds

Today `/halt <asset>` and `/resume <asset>` (`trader/src/telegram/commands.rs`) only take an
asset. The live dispatcher (`trader/src/bin/live.rs:1600-1626`, pre-change) matches every
`AssetSlot` for that asset and halts/resumes all of its strategies together — e.g. `/halt eth`
halts both ETH `high_prob` and ETH `reversal`.

This adds an optional third token: `/halt <asset> [strategy]` / `/resume <asset> [strategy]`.
`/halt eth high_prob` halts only the ETH high_prob slot and leaves ETH reversal running.
`/halt eth` (no strategy) keeps the existing all-strategies-for-this-asset behavior.

## 2. Why this was straightforward

`Worker` (`trader/src/worker.rs:381`) is already one instance per `(asset, strategy)` pair, gated
by a single `entry_suppressed` flag checked at the sole trade-entry choke point,
`Worker::try_enter()` (`worker.rs:837`). Halt state is already persisted per-`(asset, strategy)`
JSON file (`live_state_<asset>_<strategy>.json`, `worker.rs:302-324`). So no changes were needed to
the worker, entry gating, persistence, or `/status` rendering (which already prints one line per
`(asset, strategy)` slot). The entire change is in the Telegram command-parsing/dispatch layer.

## 3. Implementation

- `trader/src/telegram/commands.rs` — added `strategy: Option<String>` to `Command::Halt` /
  `Command::Resume`. Factored a `valid_strategies()` helper (`{"high_prob", "reversal"}`, shared
  with `/strategies` parsing) and a `parse_halt_resume()` helper shared between `/halt` and
  `/resume` parsing. An unrecognized strategy name (3rd token) returns `Command::Invalid`.
- `trader/src/telegram/control.rs` — mirrored the field on `ControlMsg::Halt`/`Resume` and the
  `ControlTarget::halt`/`resume` trait signatures, for consistency with the "intended" control-plane
  architecture (this path is not yet wired into the live binary — see module doc comment in
  `telegram/mod.rs` — but it has its own test suite via `MockWorker`).
- `trader/src/bin/live.rs` — the real dispatch site (`Command::Halt`/`Resume` arms inside the main
  `tokio::select!` loop). Added a strategy-equality filter (`s.worker.strategy_name.eq_ignore_ascii_case(st)`)
  alongside the existing asset filter, only applied when a strategy was given. Reply text now says
  `ETH/high_prob` when scoped, `ETH` when not.
- `trader/src/telegram/mod.rs` — the pure `dispatch()` function's canned reply text (used by the
  dormant `control.rs` path) updated the same way via a small `halt_target_label()` helper.
- `trader/src/telegram/render.rs` — `HELP_TEXT` documents the new optional `[strategy]` arg.

A global halt/resume (`/halt` with no asset) cannot carry a strategy — there is no way to supply a
3rd token without a 2nd — so that path is unaffected.

## 4. Tests

Extended existing `#[cfg(test)]` blocks:

- `commands.rs`: `parses_halt_resume_scoped_to_strategy` (asset+strategy, case-insensitive strategy
  name), `halt_rejects_unknown_strategy`.
- `control.rs`: `strategy_scoped_halt_leaves_other_strategy_running` — asserts halting
  `ETH:high_prob` via `MockWorker` does not affect an `ETH:reversal` key, and resume clears only the
  scoped key.
- `telegram/mod.rs`: `dispatch_halt_scoped_to_strategy_produces_control_and_reply`.

All existing `Command::Halt`/`Resume` / `ControlMsg::Halt`/`Resume` construction sites updated for
the new field.

## 5. Local testing before deploy

- `cargo test` — 176 lib tests + 16 `live` bin tests, all passing.
- `cargo clippy --all-targets --all-features -- -D warnings` — clean.
- `cargo fmt --all --check` — clean.

## 6. Deploy

`scripts/deploy_trader.sh` — cross-compiles `live` for aarch64, rsyncs the binary + config to
Oracle, `systemctl restart trader-live.service`.

## 7. Documentation

README `/halt` and `/resume` usage lines updated to show the optional `[strategy]` argument.
