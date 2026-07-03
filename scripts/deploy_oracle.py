#!/usr/bin/env python3
"""Deploy Rust binaries (price_feed + live) to Oracle.

Build:  cross-compiles aarch64 binaries via `cross` (Docker-based).
Deploy:
  1. cross build price_feed and live for aarch64-unknown-linux-gnu.
  2. rsync price_feed  → Oracle price_feed/target/release/
  3. rsync live        → Oracle trader/target/release/
  4. Restart poly-collector systemd service (price_feed).
  5. Gracefully stop old trader process: SIGTERM → wait 10 s → SIGKILL.
  6. Start fresh trader in tmux session 'trader'.

Usage:
    python scripts/deploy_oracle.py
    python scripts/deploy_oracle.py --dry-run
    python scripts/deploy_oracle.py --skip-build        # rsync + restart only
    python scripts/deploy_oracle.py --price-feed-only   # skip trader steps
    python scripts/deploy_oracle.py --trader-only       # skip price-feed steps
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import paramiko

# ── Oracle connection ──────────────────────────────────────────────────────────
ORACLE_HOST = "10.8.0.1"
ORACLE_USER = "ubuntu"
ORACLE_BASE = "/home/ubuntu/apps/poly_rust"

# ── Oracle trader startup command ─────────────────────────────────────────────
TRADER_ASSETS   = ["BTC"]
TRADER_ENV_FILE = "/home/ubuntu/apps/poly_rust/trader/.env"
TRADER_CFG_DIR  = "/home/ubuntu/apps/btc_5mins/config"
TRADER_LOG_DIR  = f"{ORACLE_BASE}/trader/live_logs"
TRADER_TMUX     = "trader"
# Set to e.g. "nats://localhost:4222" if NATS is running on Oracle
TRADER_NATS_URL = None

# ── Local repo root (two levels up from this script) ──────────────────────────
REPO_ROOT         = Path(__file__).resolve().parent.parent
TARGET            = "aarch64-unknown-linux-gnu"
PRICE_FEED_BIN    = REPO_ROOT / "price_feed" / "target" / TARGET / "release" / "price_feed"
TRADER_BIN        = REPO_ROOT / "trader"     / "target" / TARGET / "release" / "live"
GRACEFUL_TIMEOUT = 10  # seconds to wait after SIGTERM before SIGKILL


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


def find_trader_pid(client: paramiko.SSHClient) -> str | None:
    rc, out, _ = ssh(client, "pgrep -u \"$(whoami)\" -f 'live ' 2>/dev/null", timeout=5)
    pid = out.strip().splitlines()[0].strip() if out.strip() else None
    return pid if pid else None


def stop_trader(client: paramiko.SSHClient, dry_run: bool) -> bool:
    pid = find_trader_pid(client)
    if not pid:
        print("  Trader not running — nothing to stop.")
        return True

    print(f"  Trader PID {pid} found.")
    if dry_run:
        print(f"  [dry-run] kill -TERM {pid}, wait {GRACEFUL_TIMEOUT}s, SIGKILL if still alive")
        return True

    # kill tmux session first so it doesn't restart the process
    ssh(client, f"tmux kill-session -t {TRADER_TMUX} 2>/dev/null", timeout=5)

    print(f"  Sending SIGTERM to {pid}...")
    ssh(client, f"kill -TERM {pid} 2>/dev/null", timeout=5)

    for i in range(GRACEFUL_TIMEOUT):
        time.sleep(1)
        rc, _, _ = ssh(client, f"kill -0 {pid} 2>/dev/null", timeout=5)
        if rc != 0:
            print(f"  Exited cleanly after {i + 1}s.")
            return True

    print(f"  Still alive after {GRACEFUL_TIMEOUT}s — sending SIGKILL.")
    ssh(client, f"kill -KILL {pid} 2>/dev/null", timeout=5)
    time.sleep(1)
    return True


def start_trader(client: paramiko.SSHClient, dry_run: bool) -> bool:
    bin_path = TRADER_BIN
    if not bin_path.exists():
        print(f"  Binary not found: {bin_path}")
        return False

    print(f"\n  rsyncing {bin_path} → Oracle...")
    remote_dir = f"{ORACLE_BASE}/trader/target/release/"
    if not rsync(bin_path, remote_dir, dry_run):
        return False

    asset_flags = " ".join(f"--asset {a}" for a in TRADER_ASSETS)
    nats_flag   = f"--nats-url {TRADER_NATS_URL}" if TRADER_NATS_URL else ""
    live_bin    = f"{ORACLE_BASE}/trader/target/release/live"
    cmd_inner   = (
        f"{live_bin} {asset_flags} "
        f"--env-file {TRADER_ENV_FILE} "
        f"--config-dir {TRADER_CFG_DIR} "
        f"--log-dir {TRADER_LOG_DIR}"
    )
    if nats_flag:
        cmd_inner += f" {nats_flag}"

    # ensure log dir exists
    ssh(client, f"mkdir -p {TRADER_LOG_DIR}", timeout=5)

    tmux_cmd = f"tmux new-session -d -s {TRADER_TMUX} \"{cmd_inner}\""

    print(f"  Starting trader in tmux session '{TRADER_TMUX}'...")
    print(f"    {cmd_inner}")
    if dry_run:
        print(f"  [dry-run] {tmux_cmd}")
        return True

    rc, out, err = ssh(client, tmux_cmd, timeout=10)
    if rc != 0:
        print(f"  tmux start failed (exit {rc}):\n{out}{err}")
        return False

    time.sleep(3)
    pid = find_trader_pid(client)
    if not pid:
        print("  WARNING: no trader process found after 3s — check tmux logs:")
        print(f"    ssh {ORACLE_USER}@{ORACLE_HOST} tmux attach -t {TRADER_TMUX}")
        return False

    print(f"  Trader running (PID {pid}).")
    return True


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Deploy Rust binaries to Oracle.")
    ap.add_argument("--dry-run",         action="store_true")
    ap.add_argument("--skip-build",      action="store_true", help="Skip cross-compile, use existing binaries")
    ap.add_argument("--price-feed-only", action="store_true", help="Deploy price_feed only")
    ap.add_argument("--trader-only",     action="store_true", help="Deploy trader only")
    args = ap.parse_args()

    do_price_feed = not args.trader_only
    do_trader     = not args.price_feed_only

    if args.dry_run:
        print("[DRY RUN — no changes will be made]\n")

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
        print("\n[trader] stopping old process...")
        stop_trader(client, args.dry_run)

        print("\n[trader] deploying and starting...")
        if not start_trader(client, args.dry_run):
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
