# Incident — `--trader-only` deploy silently left Oracle running a stale strategy config, 2026-07-07

Telegram `/status` after a deploy that was supposed to set `sl_pnl_rev = 0.25` for all assets:

```
📊 STATUS  (23:37:32 HKT)
TRADE ASSETS
  🟢 active  ETH  strategy=high_prob
    sl=0.4900  ... unwind_pnl=0.0500  sl_pnl=0.2500  size=$1.00
  🟢 active  ETH  strategy=reversal
    sl=0.0000  ... unwind_pnl=0.0300  sl_pnl=0.8000  size=$1.00
```

`sl_pnl_hp` (high_prob) correctly shows `0.2500`. `sl_pnl_rev` (reversal) shows `0.8000` — the
*original* default from before any of this session's changes, not `0.25` (the value just set) and
not even `0.50` (an intermediate value from earlier the same day). `trade_assets` narrowing to ETH
did take effect (only ETH is listed at all). A prior deploy (`./scripts/deploy_trader.sh`, which
always runs `deploy_oracle.py --trader-only`) had already been run and reported success.

## Root cause

`deploy_oracle.py`'s `main()` had two independent ways to update Oracle, and only one of them
touched the actual strategy config file:

- **`deploy_trader()`** (used by the default full deploy and `--trader-only`): rsyncs the `live`
  binary, then regenerates `trader-live.service`'s systemd unit — critically, the unit's
  `--asset ETH` flag is computed **locally**, from *this machine's* `trader/config/strategy_*.toml`
  (`TRADER_ASSETS = _latest_trade_assets(LOCAL_TRADER_CFG_DIR)`, evaluated at module import time),
  and baked directly into the `ExecStart` line. This step never touches
  `/home/ubuntu/apps/poly_rust/trader/config/*.toml` on Oracle itself.
- **`sync_config()`** (only ever called from the `--config-only` fast path): rsyncs this repo's
  `trader/config/` to Oracle and updates the `btc_5mins/config/` symlink to point at it. This is
  the *only* thing that updates the file the running binary actually reads.

The Rust binary doesn't read config from CLI flags or from anything baked in at deploy time — it
calls `load_latest(config_dir)` fresh on every process startup, globbing
`btc_5mins/config/strategy_*.toml` (a symlink to Oracle's own `poly_rust/trader/config/` copy) and
parsing whatever's there. `--trader-only` restarts that process, but the file it re-reads on
restart was never updated by that code path — Oracle just kept serving whichever `strategy_*.toml`
content was last landed there by an earlier, separate `--config-only` run (or a manual copy).

**Why `trade_assets` looked right while `sl_pnl_rev` didn't:** these two settings reach the running
process through *completely different channels*. `trade_assets` becomes a `--asset` CLI flag,
computed and embedded into the systemd unit file at deploy-script-run time on the local machine —
so it always reflects whatever config was on disk *here*, regardless of what Oracle's own file
says. `sl_pnl_rev` has no CLI equivalent; it only exists inside the TOML the binary parses for
itself, on Oracle, at its own startup. A `--trader-only` deploy updates the first channel and
silently leaves the second exactly where it was.

This is the same class of gap called out in `README.md`'s "Strategy config" section (`--config-only`
exists precisely because "this repo's Oracle checkout in particular is stale with unrelated local
modifications") — but the docs implied `--config-only` was something you'd remember to run
*alongside* a code deploy when both changed together. In this session, three separate
`git commit && git push` + `deploy` cycles happened in a row (halt notifications, clippy cleanup,
then the config change), and the config change's own deploy step
(`./scripts/deploy_trader.sh` → `--trader-only`) was — correctly, per the tool's actual contract —
never expected to need a separate `--config-only` call to also take effect. The tooling's contract
was the bug: a deploy mode whose job is "restart the trader with today's config" that doesn't
actually deliver today's config is not a safe default.

## Fix

`deploy_oracle.py::main()`'s trader-deploying branch (`if do_trader:`, covers both the default full
deploy and `--trader-only`) now calls `sync_config()` unconditionally, before `deploy_trader()`,
and aborts (does not restart the trader) if the config sync fails:

```python
if do_trader:
    print("\n[config] syncing strategy config...")
    if not sync_config(client, args.dry_run):
        print("  config sync failed.")
        ok = False
    else:
        print("\n[trader] deploying...")
        if not deploy_trader(client, args.dry_run):
            print("  trader deploy failed.")
            ok = False
```

`--config-only` is unchanged (it already called `sync_config`); it remains available as a
lighter-weight path for a config-only change that skips the binary rsync/cross-compile, not as a
*required extra step* for config to take effect — every trader-deploying mode now does the same
sync, every time. `sync_config()` itself is cheap and idempotent (an `rsync` of a small directory
plus one `ln -sfn`), so making it unconditional has no meaningful cost.

`scripts/deploy_trader.sh`'s header comment updated to note the config sync explicitly, pointing at
this doc.

## Tests

No Python test infrastructure existed in this repo before this fix (`deploy_oracle.py` is a
standalone operational script — confirmed via `find` for `pytest.ini`/`conftest.py`/`test_*.py`,
none present). Added `scripts/test_deploy_oracle.py` (stdlib `unittest` + `unittest.mock` only, no
new dependency), which imports `deploy_oracle.py` by path and mocks every network/subprocess-touching
function (`connect_oracle`, `build`, `deploy_price_feed`, `deploy_trader`, `sync_config`) so it runs
in milliseconds with zero real SSH/Docker activity:

- `test_default_full_deploy_syncs_config_before_deploying_trader`
- `test_trader_only_syncs_config_before_deploying` — the exact regression case
- `test_price_feed_only_never_touches_trader_or_config` — confirms the fix didn't overreach into
  the price-feed-only path
- `test_config_only_syncs_config_and_skips_binary_rsync` — confirms `--config-only`'s existing
  contract (`deploy_trader(..., skip_binary=True)`) is unchanged
- `test_trader_deploy_is_skipped_when_config_sync_fails` — a failed config sync must not proceed to
  restart the trader against a half-synced/unknown config state

Run: `/home/kev/apps/btc_5mins/venv/bin/python3 scripts/test_deploy_oracle.py -v` (needs the
paramiko/tomllib venv `deploy_oracle.py` itself depends on — plain `python3` fails to even import
the module). All 5 pass.

## Verification after redeploy

Re-ran `./scripts/deploy_trader.sh` after this fix. Deploy output now shows an explicit
`[config] syncing strategy config...` step before `[trader] deploying...`, confirming the fixed
code path actually executed (previously absent from the deploy log entirely). Telegram `/status`
after the restart should show `sl_pnl=0.2500` for both `ETH reversal` and `ETH high_prob`.

## Lesson

A deploy script with multiple "modes" that each update a different subset of what the running
process depends on is a standing invitation for exactly this drift — the safe default is for every
mode that (re)starts the process to bring *all* of its dependencies current, not just the ones that
mode was originally designed to change. `--config-only`/`--trader-only`/`--price-feed-only` as
*narrower, faster* options for when you know only one thing changed is fine; silently leaving a
whole category of config out of a "full" or "trader" deploy is not.
