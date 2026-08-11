#!/usr/bin/env python3
"""
Polymarket Pipeline Runner
===========================

Single entry point that orchestrates all three scripts:

  1. Scraper  (polymarket_top_traders_scraper.py) — fetches leaderboard + open positions
  2. Ranker   (rank_traders.py)                  — fetches leaderboard + trade history → scores
  3. Overlaps (find_overlaps.py)                 — finds shared positions across traders
  4. Trades   (trade_decision.py)                — recommends trades based on overlaps and traders

Execution flow:
  • Scraper and Ranker start simultaneously (both fetch the leaderboard independently)
  • Overlaps starts as soon as the Scraper finishes (needs its output)
  • Ranker continues in the background until done

Usage:
    python run_all.py

Author : Antigravity AI
Version: 1.0.0
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
PYTHON = sys.executable  # use the same interpreter that launched this script


def main() -> None:
    sep = "═" * 80
    thin = "─" * 80

    print(sep)
    print("  POLYMARKET PIPELINE — Starting all scripts")
    print(sep)
    print()

    # ── 1. Launch Scraper + Ranker in parallel ──
    print(f"  ▶  Starting Scraper  (polymarket_top_traders_scraper.py)")
    print(f"  ▶  Starting Ranker   (rank_traders.py)")
    print(thin)
    print()

    scraper_proc = subprocess.Popen(
        [PYTHON, str(SCRIPTS_DIR / "polymarket_top_traders_scraper.py")],
        cwd=str(SCRIPTS_DIR),
    )

    ranker_proc = subprocess.Popen(
        [PYTHON, str(SCRIPTS_DIR / "rank_traders.py")],
        cwd=str(SCRIPTS_DIR),
    )

    # ── 2. Wait for Scraper to finish ──
    print("  ⏳  Waiting for Scraper to finish…")
    print()
    scraper_exit = scraper_proc.wait()
    if scraper_exit != 0:
        print(f"  ⚠  Scraper exited with code {scraper_exit}")
    else:
        print()
        print(thin)
        print("  ✅  Scraper finished!")
        print(thin)
        print()

    # ── 3. Run Overlaps (needs scraper output) ──
    print("  ▶  Starting Overlaps (find_overlaps.py)")
    print()
    overlaps_proc = subprocess.Popen(
        [PYTHON, str(SCRIPTS_DIR / "find_overlaps.py")],
        cwd=str(SCRIPTS_DIR),
    )
    overlaps_exit = overlaps_proc.wait()
    if overlaps_exit != 0:
        print(f"  ⚠  Overlaps exited with code {overlaps_exit}")
    else:
        print()
        print(thin)
        print("  ✅  Overlaps finished!")
        print(thin)
        print()

    # ── 4. Wait for Ranker to finish (may still be running) ──
    if ranker_proc.poll() is None:
        print("  ⏳  Waiting for Ranker to finish…")
        print()
    ranker_exit = ranker_proc.wait()
    if ranker_exit != 0:
        print(f"  ⚠  Ranker exited with code {ranker_exit}")
    else:
        print()
        print(thin)
        print("  ✅  Ranker finished!")
        print(thin)
        print()

    # ── 5. Run Trade Decision (needs overlaps and scraper output) ──
    print("  ▶  Starting Trade Decision (trade_decision.py)")
    print()
    trades_proc = subprocess.Popen(
        [PYTHON, str(SCRIPTS_DIR / "trade_decision.py")],
        cwd=str(SCRIPTS_DIR),
    )
    trades_exit = trades_proc.wait()
    if trades_exit != 0:
        print(f"  ⚠  Trade Decision exited with code {trades_exit}")
    else:
        print()
        print(thin)
        print("  ✅  Trade Decision finished!")
        print(thin)
        print()

    # ── Summary ──
    print()
    print(sep)
    print("  PIPELINE COMPLETE")
    print()
    print("  Output files:")
    output = SCRIPTS_DIR / "output"
    for name in ["latest_top_traders.json", "latest_overlaps.json", "latest_rankings.json", "recommended_trades.json"]:
        path = output / name
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"    ✓  {name}  ({size_mb:.1f} MB)")
        else:
            print(f"    ✗  {name}  (not found)")
    print()
    print(sep)


if __name__ == "__main__":
    main()
