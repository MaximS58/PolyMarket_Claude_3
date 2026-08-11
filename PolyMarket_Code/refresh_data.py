#!/usr/bin/env python3
"""
Trading Data Refresh
====================

Regenerates the inputs the paper trader needs, in dependency order:

  1. Scraper   (polymarket_top_traders_scraper.py) — leaderboard + open positions
  2. Overlaps  (find_overlaps.py)                  — shared positions across traders
  3. Trades    (trade_decision.py)                 — the recommendations we trade

Deliberately skips rank_traders.py: it is the slowest stage (~160s) and nothing
in the paper-trading path reads latest_rankings.json. Use run_all.py when you
actually want the rankings.

Why refresh at all: the recommendations are dominated by same-day sports markets
that settle within hours, so a file from yesterday is mostly already-resolved
trades the engine will refuse to open.

Usage:
    python refresh_data.py

Exit code is non-zero if any stage fails, so a launcher can stop before trading
on stale data.

Author : Antigravity AI
Version: 1.0.0
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPTS_DIR / "output"
PYTHON = sys.executable  # use the same interpreter that launched this script

STAGES = [
    ("Scraper",  "polymarket_top_traders_scraper.py", "leaderboard + open positions"),
    ("Overlaps", "find_overlaps.py",                  "shared positions across traders"),
    ("Trades",   "trade_decision.py",                 "trade recommendations"),
]

RECOMMENDED_TRADES_FILE = OUTPUT_DIR / "recommended_trades.json"


def _summarize() -> None:
    """Report what the refresh actually produced."""
    if not RECOMMENDED_TRADES_FILE.exists():
        print("  ⚠  recommended_trades.json was not produced.")
        return

    try:
        data = json.loads(RECOMMENDED_TRADES_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  ⚠  Could not read recommended_trades.json: {exc}")
        return

    trades = data.get("trades", [])
    high = [t for t in trades if t.get("tier") == "High"]
    notional = sum(float(t.get("recommended_trade_amount_usd", 0)) for t in high)

    print(f"  Recommendations : {len(trades)}  ({len(high)} High-tier)")
    print(f"  High notional   : ${notional:,.2f}")
    print(f"  Generated       : {data.get('metadata', {}).get('timestamp', '?')}")


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    sep = "═" * 80
    thin = "─" * 80
    started = time.monotonic()

    print(sep)
    print("  REFRESHING TRADING DATA")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(sep)
    print()
    print("  Expect roughly 4-5 minutes. Skipping rank_traders.py — the paper")
    print("  trader does not use latest_rankings.json.")
    print()

    for i, (label, script, purpose) in enumerate(STAGES, 1):
        print(thin)
        print(f"  [{i}/{len(STAGES)}]  {label}  —  {purpose}")
        print(thin)
        print()

        stage_started = time.monotonic()
        proc = subprocess.Popen([PYTHON, str(SCRIPTS_DIR / script)], cwd=str(SCRIPTS_DIR))
        exit_code = proc.wait()
        stage_elapsed = time.monotonic() - stage_started

        if exit_code != 0:
            print()
            print(sep)
            print(f"  ✗  {label} failed with exit code {exit_code} after {stage_elapsed:.0f}s.")
            print("     Refusing to continue — trading on stale data would open")
            print("     positions in markets that have already resolved.")
            print(sep)
            sys.exit(exit_code)

        print()
        print(f"  ✓  {label} finished in {stage_elapsed:.0f}s")
        print()

    print(sep)
    print(f"  REFRESH COMPLETE in {time.monotonic() - started:.0f}s")
    print()
    _summarize()
    print()
    print(sep)


if __name__ == "__main__":
    main()
