#!/usr/bin/env python3
"""
Paper-Trading Launcher
======================

Starts both halves of the paper-trading system and keeps them together:

  1. paper_trade.py         — opens the book, marks it to market every 30s
  2. trader_exit_monitor.py — polls the copied traders, signals when to exit

The engine is started first; the monitor only has work to do once
paper_portfolio.json exists, so the launcher waits for that file before
starting it. Ctrl+C shuts both down cleanly.

Usage:
    python run_paper_trading.py
    python run_paper_trading.py --reset    # discard any existing portfolio first

Author : Antigravity AI
Version: 1.0.0
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
PYTHON = sys.executable  # use the same interpreter that launched this script

PORTFOLIO_FILE = SCRIPTS_DIR / "output" / "paper_portfolio.json"

PORTFOLIO_WAIT_SECONDS = 180  # generous — seeding fetches a live price per trade
MONITOR_SHUTDOWN_GRACE = 10


def _portfolio_mtime() -> float | None:
    try:
        return PORTFOLIO_FILE.stat().st_mtime
    except OSError:
        return None


def _wait_for_portfolio(engine: subprocess.Popen, timeout: int, baseline: float | None) -> bool:
    """
    Block until the engine has written a *fresh* portfolio snapshot.

    Comparing against the mtime captured before launch matters: a portfolio from
    an earlier session is usually still on disk, and starting the monitor against
    that would have it report on a book the engine is about to replace.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = _portfolio_mtime()
        if current is not None and (baseline is None or current > baseline):
            return True
        if engine.poll() is not None:
            return False  # engine died before writing anything
        time.sleep(1)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the paper-trading system")
    parser.add_argument("--reset", action="store_true",
                        help="discard any existing portfolio and start fresh")
    args = parser.parse_args()

    sep = "═" * 80
    thin = "─" * 80

    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    print(sep)
    print("  POLYMARKET PAPER TRADING — Starting")
    print(sep)
    print()
    print("  ▶  Engine   (paper_trade.py)          — marks the book every 30s")
    print("  ▶  Monitor  (trader_exit_monitor.py)  — follows the traders out")
    print(thin)
    print()

    engine_cmd = [PYTHON, str(SCRIPTS_DIR / "paper_trade.py")]
    if args.reset:
        engine_cmd.append("--reset")

    baseline_mtime = _portfolio_mtime()
    engine = subprocess.Popen(engine_cmd, cwd=str(SCRIPTS_DIR))

    print("  ⏳  Waiting for the engine to open its positions…")
    print()
    monitor = None
    if _wait_for_portfolio(engine, PORTFOLIO_WAIT_SECONDS, baseline_mtime):
        print()
        print(thin)
        print("  ✅  Portfolio created — starting the exit monitor")
        print(thin)
        print()
        monitor = subprocess.Popen(
            [PYTHON, str(SCRIPTS_DIR / "trader_exit_monitor.py")],
            cwd=str(SCRIPTS_DIR),
        )
    elif engine.poll() is not None:
        print(f"  ⚠  Engine exited with code {engine.returncode} before opening any positions.")
        print("     Check output/paper_trade.log — the pipeline may need to run first:")
        print("       python run_all.py")
        sys.exit(engine.returncode or 1)
    else:
        print(f"  ⚠  No portfolio after {PORTFOLIO_WAIT_SECONDS}s — running without the exit monitor.")

    # ── Run until the engine stops or the user interrupts ──
    try:
        engine.wait()
    except KeyboardInterrupt:
        print()
        print("  ⏹  Interrupt received — shutting both processes down…")
        # Ctrl+C already reached the children via the console process group;
        # give them a moment to persist state before forcing the issue.
        try:
            engine.wait(timeout=MONITOR_SHUTDOWN_GRACE)
        except subprocess.TimeoutExpired:
            engine.terminate()

    if monitor is not None and monitor.poll() is None:
        try:
            monitor.wait(timeout=MONITOR_SHUTDOWN_GRACE)
        except subprocess.TimeoutExpired:
            monitor.terminate()
            try:
                monitor.wait(timeout=5)
            except subprocess.TimeoutExpired:
                monitor.kill()

    # ── Summary ──
    print()
    print(sep)
    print("  PAPER TRADING STOPPED")
    print()
    print("  Output files:")
    output = SCRIPTS_DIR / "output"
    for name in ["paper_portfolio.json", "paper_equity_history.jsonl",
                 "trader_exit_signals.json", "paper_trade.log"]:
        path = output / name
        if path.exists():
            size_kb = path.stat().st_size / 1024
            print(f"    ✓  {name}  ({size_kb:,.1f} KB)")
        else:
            print(f"    ✗  {name}  (not found)")
    print()
    print(sep)


if __name__ == "__main__":
    main()
