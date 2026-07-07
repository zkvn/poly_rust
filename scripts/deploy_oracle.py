#!/usr/bin/env python3
"""Deploy Rust binaries (price_feed + live) to Oracle.

Build:  cross-compiles aarch64 binaries via `cross` (Docker-based).
Deploy:
  1. cross build price_feed and live for aarch64-unknown-linux-gnu.
  2. rsync price_feed  → Oracle price_feed/target/release/
  3. rsync live        → Oracle trader/target/release/
  4. Restart poly-collector systemd service (price_feed).
  5. Sync strategy config (trader/config/ + btc_5mins/config symlink) to
     Oracle — always, whenever the trader is being (re)deployed, not just on
     an explicit --config-only run. The running binary re-reads its
     strategy_*.toml from Oracle's own copy on every startup; skipping this
     step left a --trader-only deploy silently running against a stale
     config indefinitely (see
     trader/doc/incident_stale_oracle_config_2026-07-07.md).
  6. Restart trader-live systemd service (trader) — `systemctl restart`,
     which stops the old process and starts the new one atomically under
     systemd's own supervision.

Both the price recorder and the trader run as systemd services with
`Restart=always` (units on Oracle: poly-collector.service,
trader-live.service). This script MUST go through `systemctl` for both —
never `kill`/`tmux` the process directly. Doing that races with systemd's own
Restart=always: it sees the killed process as an unexpected exit and
auto-respawns a *second* copy on its own, while this script starts a *third*.
That's exactly what happened on 2026-07-03 (see README's "known incidents"):
two independent trader processes ended up running concurrently against the
same real-money account for ~16 minutes before it was caught by hand.

Usage:
    python scripts/deploy_oracle.py
    python scripts/deploy_oracle.py --dry-run
    python scripts/deploy_oracle.py --skip-build        # rsync + restart only
    python scripts/deploy_oracle.py --price-feed-only   # skip trader steps
    python scripts/deploy_oracle.py --trader-only       # skip price-feed steps
    python scripts/deploy_oracle.py --config-only       # sync strategy config + restart trader only

--config-only is for a config-only change (new/edited strategy_*.toml, no code
change) when you don't also want to redeploy the binary: rsyncs this repo's
trader/config/ (the real, git-tracked copy — see README "Strategy config") to
Oracle, creates/updates the matching symlink in Oracle's btc_5mins/config/
directly (no git pull of btc_5mins — see sync_config), then restarts
trader-live.service so it re-globs and loads the new file. No build, no binary
rsync. Every other mode that deploys the trader (default, --trader-only) does
this same config sync too, unconditionally, before restarting — --config-only
is just the "config changed, binary didn't" fast path, not a separate
requirement you have to remember to run.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import tomllib
from pathlib import Path

import paramiko

# ── Local repo root (two levels up from this script) ──────────────────────────
REPO_ROOT         = Path(__file__).resolve().parent.parent
TARGET            = "aarch64-unknown-linux-gnu"
PRICE_FEED_BIN    = REPO_ROOT / "price_feed" / "target" / TARGET / "release" / "price_feed"
TRADER_BIN        = REPO_ROOT / "trader"     / "target" / TARGET / "release" / "live"

# ── Oracle connection ──────────────────────────────────────────────────────────
ORACLE_HOST = "10.8.0.1"
ORACLE_USER = "ubuntu"
ORACLE_BASE = "/home/ubuntu/apps/poly_rust"


def _latest_trade_assets(config_dir: Path) -> list[str]:
    """Read `trade_assets` from the newest strategy_*.toml in config_dir.

    Same glob+sort-latest rule as bot/config.py::_load_strategy_toml and
    trader/src/config.rs::load_latest, so the deploy script always matches
    whatever assets the Python bot is actually configured to trade — no more
    hand-maintained asset list that can silently drift from the shared config.
    """
    candidates = sorted(config_dir.glob("strategy_*.toml"))
    if not candidates:
        raise FileNotFoundError(f"no strategy_*.toml found in {config_dir}")
    with open(candidates[-1], "rb") as f:
        data = tomllib.load(f)
    return [a.strip().upper() for a in data["trade_assets"] if a.strip()]


# ── Strategy config (real copy — see README "Strategy config") ────────────────
# The real, git-tracked strategy_*.toml files live in this repo's trader/config/;
# btc_5mins/config holds only a symlink pointing at whichever is current (kept
# there via a normal commit for local dev, but Oracle's copy is delivered by
# this script directly — see sync_config). Reading TRADER_ASSETS from this
# repo's own directory, not through btc_5mins, means this script has no
# dependency on the sibling repo even being checked out.
LOCAL_TRADER_CFG_DIR = REPO_ROOT / "trader" / "config"
BTC5MINS_CFG_REMOTE  = "/home/ubuntu/apps/btc_5mins/config"

# ── Oracle trader startup command ─────────────────────────────────────────────
TRADER_ASSETS   = _latest_trade_assets(LOCAL_TRADER_CFG_DIR)
TRADER_ENV_FILE = "/home/ubuntu/apps/poly_rust/trader/.env"
TRADER_CFG_DIR  = "/home/ubuntu/apps/btc_5mins/config"
TRADER_LOG_DIR  = f"{ORACLE_BASE}/trader/live_logs"
TRADER_SERVICE  = "trader-live.service"
TRADER_UNIT_PATH = "/etc/systemd/system/trader-live.service"
# price_feed publishes ticks here (poly-collector.service) and the trader
# subscribes instead of opening its own duplicate Binance/Poly WebSockets —
# required now that an asset can own more than one strategy worker.
TRADER_NATS_URL = "nats://127.0.0.1:4222"


def _trader_unit_file(asset_flags: str, nats_flag: str) -> str:
    """Renders trader-live.service's content from the same TRADER_ASSETS this
    script always keeps in sync with the latest strategy_*.toml — so the
    installed unit's ExecStart can never silently drift from config the way a
    hand-edited unit file could."""
    exec_start = (
        f"{ORACLE_BASE}/trader/target/release/live \\\n"
        f"  {asset_flags} \\\n"
        f"  --env-file {TRADER_ENV_FILE} \\\n"
        f"  --config-dir {TRADER_CFG_DIR} \\\n"
        f"  --log-dir {TRADER_LOG_DIR} \\\n"
        f"  {nats_flag}"
    )
    return f"""[Unit]
Description=poly_rust live trader
After=network-online.target nats-server.service
Wants=network-online.target

[Service]
Type=simple
User={ORACLE_USER}
WorkingDirectory={ORACLE_BASE}/trader
ExecStart={exec_start}
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30
StandardOutput=append:{TRADER_LOG_DIR}/live.log
StandardError=inherit

[Install]
WantedBy=multi-user.target
"""


# ── helpers ───────────────────────────────────────────────────────────────────

def run_local(cmd: list[str], cwd: Path | None = None, timeout: int = 600) -> bool:
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd or REPO_ROOT, timeout=timeout)
    if result.returncode != 0:
        print(f"  command failed (exit {result.returncode})")
    return result.returncode == 0


def connect_oracle() -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ORACLE_HOST, username=ORACLE_USER, allow_agent=True, look_for_keys=True)
    return client


def ssh(client: paramiko.SSHClient, cmd: str, timeout: int = 30) -> tuple[int, str, str]:
    _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    rc = stdout.channel.recv_exit_status()
    return rc, stdout.read().decode(), stderr.read().decode()


def rsync(local: Path, remote_path: str, dry_run: bool) -> bool:
    cmd = ["rsync", "-avz", "--progress"]
    if dry_run:
        cmd.append("--dry-run")
    cmd += [str(local), f"{ORACLE_USER}@{ORACLE_HOST}:{remote_path}"]
    return run_local(cmd)


# ── steps ─────────────────────────────────────────────────────────────────────

def build(bins: list[str], dry_run: bool) -> bool:
    """Cross-compile aarch64 binaries via `cross` (Docker-based toolchain)."""
    for b in bins:
        crate_dir = REPO_ROOT / ("price_feed" if b == "price_feed" else "trader")
        print(f"\n  cross build --release --bin {b} --target {TARGET}")
        if dry_run:
            continue
        if not run_local(
            ["cross", "build", "--release", f"--bin={b}", f"--target={TARGET}"],
            cwd=crate_dir,
            timeout=900,
        ):
            return False
        bin_path = PRICE_FEED_BIN if b == "price_feed" else TRADER_BIN
        print(f"  Built: {bin_path} ({bin_path.stat().st_size // 1024 // 1024} MiB)")
    return True


def sync_config(client: paramiko.SSHClient, dry_run: bool) -> bool:
    """Land the current strategy config on Oracle without touching any binary
    — and without depending on the sibling btc_5mins repo's git state at all.

    1. rsync this repo's trader/config/ (the real, git-tracked copy) to Oracle.
    2. `ln -sfn` a symlink at Oracle's btc_5mins/config/<latest>.toml pointing
       back at the file just rsynced — the same relative symlink already
       committed in btc_5mins for local dev, just created directly here
       instead of via `git pull`.

    Earlier this did a `git pull` of btc_5mins on Oracle instead of step 2.
    Dropped that: it made a config-only deploy depend on btc_5mins's Oracle
    checkout staying clean/fast-forwardable (the same class of problem this
    project's own Oracle checkout already has — see README "Build and
    deploy"), and it silently pulled in whatever else had been pushed to
    btc_5mins's main since, not just the config change being deployed. A
    symlink write is one idempotent command with neither failure mode."""
    print(f"\n  rsyncing {LOCAL_TRADER_CFG_DIR} → Oracle...")
    if not rsync(LOCAL_TRADER_CFG_DIR, f"{ORACLE_BASE}/trader/", dry_run):
        return False

    candidates = sorted(LOCAL_TRADER_CFG_DIR.glob("strategy_*.toml"))
    if not candidates:
        print(f"  no strategy_*.toml found in {LOCAL_TRADER_CFG_DIR}")
        return False
    latest_name = candidates[-1].name
    link_path = f"{BTC5MINS_CFG_REMOTE}/{latest_name}"
    link_target = f"../../poly_rust/trader/config/{latest_name}"

    print(f"  symlinking {link_path} -> {link_target} on Oracle...")
    if dry_run:
        print(f"  [dry-run] ln -sfn {link_target} {link_path}")
        return True
    rc, out, err = ssh(client, f"ln -sfn {link_target} {link_path}", timeout=10)
    if rc != 0:
        print(f"  symlink update failed:\n{out}{err}")
        return False
    return True


def deploy_price_feed(client: paramiko.SSHClient, dry_run: bool) -> bool:
    bin_path = PRICE_FEED_BIN
    if not bin_path.exists():
        print(f"  Binary not found: {bin_path}")
        return False
    print(f"\n  rsyncing {bin_path} → Oracle...")
    remote_dir = f"{ORACLE_BASE}/price_feed/target/release/"
    if not rsync(bin_path, remote_dir, dry_run):
        return False

    print("  Restarting poly-collector (systemd)...")
    if dry_run:
        print("  [dry-run] sudo systemctl restart poly-collector")
        return True
    rc, out, err = ssh(client, "sudo systemctl restart poly-collector", timeout=15)
    if rc != 0:
        print(f"  systemctl restart failed:\n{out}{err}")
        return False
    time.sleep(2)
    rc2, out2, _ = ssh(client, "systemctl is-active poly-collector")
    status = out2.strip()
    print(f"  poly-collector: {status}")
    return status == "active"


def deploy_trader(client: paramiko.SSHClient, dry_run: bool, skip_binary: bool = False) -> bool:
    """rsync the binary (unless skip_binary — used by --config-only, which has
    nothing new to rsync here), keep trader-live.service's unit file in sync
    with the current TRADER_ASSETS, then `systemctl restart` it. Always goes
    through systemd — never `kill`/`tmux` the process directly (see module
    docstring for why: that raced with systemd's own Restart=always and
    produced two concurrent live-trading processes on 2026-07-03)."""
    if not skip_binary:
        bin_path = TRADER_BIN
        if not bin_path.exists():
            print(f"  Binary not found: {bin_path}")
            return False

        print(f"\n  rsyncing {bin_path} → Oracle...")
        remote_dir = f"{ORACLE_BASE}/trader/target/release/"
        if not rsync(bin_path, remote_dir, dry_run):
            return False

    asset_flags = " ".join(f"--asset {a}" for a in TRADER_ASSETS)
    nats_flag = f"--nats-url {TRADER_NATS_URL}" if TRADER_NATS_URL else ""
    unit_content = _trader_unit_file(asset_flags, nats_flag)

    ssh(client, f"mkdir -p {TRADER_LOG_DIR}", timeout=5)

    print(f"  Checking {TRADER_UNIT_PATH} matches current config ({', '.join(TRADER_ASSETS)})...")
    rc, current, _ = ssh(client, f"cat {TRADER_UNIT_PATH} 2>/dev/null", timeout=5)
    unit_changed = current.strip() != unit_content.strip()

    if unit_changed:
        print("  Unit file differs from current strategy_*.toml's trade_assets — updating.")
        if dry_run:
            print(f"  [dry-run] write {TRADER_UNIT_PATH} + sudo systemctl daemon-reload")
        else:
            sftp = client.open_sftp()
            with sftp.file("/tmp/trader-live.service.new", "w") as f:
                f.write(unit_content)
            sftp.close()
            rc, out, err = ssh(client, f"sudo cp /tmp/trader-live.service.new {TRADER_UNIT_PATH} && sudo systemctl daemon-reload", timeout=15)
            if rc != 0:
                print(f"  unit file update failed:\n{out}{err}")
                return False
    else:
        print("  Unit file already matches — no changes.")

    print(f"  Restarting {TRADER_SERVICE} (systemd)...")
    if dry_run:
        print(f"  [dry-run] sudo systemctl restart {TRADER_SERVICE}")
        return True
    rc, out, err = ssh(client, f"sudo systemctl restart {TRADER_SERVICE}", timeout=20)
    if rc != 0:
        print(f"  systemctl restart failed:\n{out}{err}")
        return False

    time.sleep(3)
    rc2, out2, _ = ssh(client, f"systemctl is-active {TRADER_SERVICE}")
    status = out2.strip()
    rc3, pid_out, _ = ssh(client, f"systemctl show {TRADER_SERVICE} -p MainPID --value")
    print(f"  {TRADER_SERVICE}: {status} (PID {pid_out.strip()})")
    return status == "active"


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Deploy Rust binaries to Oracle.")
    ap.add_argument("--dry-run",         action="store_true")
    ap.add_argument("--skip-build",      action="store_true", help="Skip cross-compile, use existing binaries")
    ap.add_argument("--price-feed-only", action="store_true", help="Deploy price_feed only")
    ap.add_argument("--trader-only",     action="store_true", help="Deploy trader only")
    ap.add_argument("--config-only",     action="store_true",
                     help="Sync strategy config (rsync trader/config/ + git pull btc_5mins) and "
                          "restart trader-live.service only — no build, no binary rsync")
    args = ap.parse_args()

    if args.dry_run:
        print("[DRY RUN — no changes will be made]\n")

    # ── config-only fast path ────────────────────────────────────────────────
    if args.config_only:
        print(f"\nConnecting to {ORACLE_USER}@{ORACLE_HOST} ...")
        client = connect_oracle()
        print("\n[config] syncing strategy config...")
        ok = sync_config(client, args.dry_run)
        if ok:
            print("\n[trader] restarting to pick up new config...")
            ok = deploy_trader(client, args.dry_run, skip_binary=True)
        else:
            print("  config sync failed.")
        client.close()
        print("\nDone." if ok else "\nDone (with errors).")
        sys.exit(0 if ok else 1)

    do_price_feed = not args.trader_only
    do_trader     = not args.price_feed_only

    # ── 1. build ──────────────────────────────────────────────────────────────
    if not args.skip_build:
        bins = (["price_feed"] if do_price_feed else []) + (["live"] if do_trader else [])
        print(f"\n[build] cross-compiling for {TARGET}: {bins}")
        if not build(bins, args.dry_run):
            sys.exit(1)
    else:
        print(f"\n[build] --skip-build: using existing binaries ({PRICE_FEED_BIN.parent})")

    # ── connect ───────────────────────────────────────────────────────────────
    print(f"\nConnecting to {ORACLE_USER}@{ORACLE_HOST} ...")
    client = connect_oracle()

    ok = True

    # ── 2. price_feed ─────────────────────────────────────────────────────────
    if do_price_feed:
        print("\n[price-feed] deploying...")
        if not deploy_price_feed(client, args.dry_run):
            print("  price-feed deploy failed.")
            ok = False

    # ── 3. trader ─────────────────────────────────────────────────────────────
    if do_trader:
        # Always sync config before (re)starting the trader — not just on an
        # explicit --config-only run. deploy_trader() below only rsyncs the
        # binary and regenerates the systemd unit's --asset flags (computed
        # from *this machine's* trader/config/); the running binary re-reads
        # its actual strategy_*.toml from Oracle's own copy on every startup
        # via load_latest(), which this step is what actually keeps current.
        # Skipping it left Oracle silently serving a stale config after a
        # --trader-only deploy — see
        # trader/doc/incident_stale_oracle_config_2026-07-07.md.
        print("\n[config] syncing strategy config...")
        if not sync_config(client, args.dry_run):
            print("  config sync failed.")
            ok = False
        else:
            print("\n[trader] deploying...")
            if not deploy_trader(client, args.dry_run):
                print("  trader deploy failed.")
                ok = False

    client.close()
    print("\nDone." if ok else "\nDone (with errors).")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(0)
