#!/usr/bin/env python3
"""
Trade Decision Engine
=====================

Analyzes overlapping positions from top Polymarket traders, filters out
"one-hit wonder" accounts, filters by time-to-resolution and price,
and calculates a dynamic position size based on a simulated portfolio.

Outputs a JSON file of recommended trades.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
TOP_TRADERS_FILE = OUTPUT_DIR / "latest_top_traders.json"
OVERLAPS_FILE = OUTPUT_DIR / "latest_overlaps.json"
RECOMMENDED_TRADES_FILE = OUTPUT_DIR / "recommended_trades.json"

PORTFOLIO_SIZE = 10000.0  # Simulated USD

# Parameters
MIN_VALID_TRADERS = 2
MIN_PRICE = 0.05
MAX_PRICE = 0.85
MIN_DAYS_TO_EXPIRY = 1
MAX_DAYS_TO_EXPIRY = 45

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("trade_decision")

def _parse_end_date(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def fetch_end_date(slug: str) -> datetime | None:
    """
    Fetch a market's end date from the Gamma events endpoint.

    Fallback only — find_overlaps.py now supplies end_date in bulk. This path
    also misses markets the events endpoint doesn't index by slug (spreads,
    "Will X win on <date>?"), which is why the supplied value is preferred.
    """
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
        resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}", timeout=10, verify=False, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and "endDate" in data[0]:
                return _parse_end_date(data[0]["endDate"])
    except Exception as e:
        logger.warning(f"Failed to fetch end date for {slug}: {e}")
    return None

def main():
    if not TOP_TRADERS_FILE.exists() or not OVERLAPS_FILE.exists():
        logger.error("Required JSON files not found in output directory.")
        sys.exit(1)

    with open(TOP_TRADERS_FILE, encoding="utf-8") as f:
        traders_data = json.load(f)

    with open(OVERLAPS_FILE, encoding="utf-8") as f:
        overlaps_data = json.load(f)

    # Build lookup for one-hit wonders
    trader_lookup = {}
    for t in traders_data.get("traders", []):
        trader_lookup[t["proxy_wallet"]] = {
            "open_position_count": t.get("open_position_count", 0),
            "rank": t.get("rank", 999),
        }

    # Keep track of the best trade per condition_id to avoid opposing recommendations
    recommended_trades = {}
    now = datetime.now(timezone.utc)

    overlaps = overlaps_data.get("overlaps", [])
    logger.info(f"Processing {len(overlaps)} overlapping markets...")

    skipped_state = 0
    for overlap in overlaps:
        title = overlap.get("market_title")
        slug = overlap.get("slug")
        price = overlap.get("cur_price")

        # 0. State filter — never enter a game already under way or settled.
        #    find_overlaps.py normally removes these; this also covers an older
        #    overlaps file produced before that filter existed.
        state = overlap.get("market_state")
        if state is not None and state != "upcoming":
            skipped_state += 1
            continue

        # 1. Price Filter
        if price < MIN_PRICE or price > MAX_PRICE:
            continue

        # 2. Filter Traders (One-hit wonders)
        valid_traders = []
        for trader in overlap.get("traders", []):
            wallet = trader.get("proxy_wallet")
            info = trader_lookup.get(wallet, {})
            pos_count = info.get("open_position_count", 0)
            
            # Weed out 1-trade accounts
            if pos_count > 1:
                # Store trader with their current rank
                valid_traders.append({
                    "username": trader.get("username"),
                    "rank": info.get("rank", trader.get("rank", 999)),
                    "size": trader.get("size", 0)
                })

        # Check if we still have enough consensus
        if len(valid_traders) < MIN_VALID_TRADERS:
            continue

        # 3. Time Filter
        # Prefer the end date find_overlaps.py already fetched in bulk; only
        # fall back to a per-market call when it isn't there.
        end_date = _parse_end_date(overlap.get("end_date"))
        if end_date is None:
            end_date = fetch_end_date(slug)
            time.sleep(0.1)  # gentle on the gamma API for the fallback path

        days_to_expiry = None
        if end_date:
            days_to_expiry = (end_date - now).days
            if days_to_expiry < MIN_DAYS_TO_EXPIRY or days_to_expiry > MAX_DAYS_TO_EXPIRY:
                continue
        else:
            # If we can't determine the end date, skip it to be safe
            continue

        # 4. Conviction Score (0-100+)
        # Base score on number of valid top traders.
        # Plus bonus for higher ranked traders (100 - rank).
        score = 0
        for vt in valid_traders:
            rank = vt["rank"]
            # Max 50 points per trader, scaled by rank
            trader_score = max(0, 50 - (rank * 0.5))
            score += trader_score

        # Normalize score somewhat (let's say 100 is a very strong signal)
        conviction_score = min(100, round(score))

        # 5. Position Sizing
        if conviction_score >= 80:
            alloc_pct = 0.05 # 5%
            tier = "High"
        elif conviction_score >= 50:
            alloc_pct = 0.025 # 2.5%
            tier = "Medium"
        else:
            alloc_pct = 0.01 # 1%
            tier = "Low"

        trade_amount = PORTFOLIO_SIZE * alloc_pct

        trade_obj = {
            "market_title": title,
            "slug": slug,
            "condition_id": overlap.get("condition_id"),
            "outcome_to_buy": overlap.get("outcome"),
            "current_price": price,
            "days_to_expiry": days_to_expiry,
            "valid_top_traders_count": len(valid_traders),
            "conviction_score": conviction_score,
            "tier": tier,
            "recommended_allocation_pct": alloc_pct * 100,
            "recommended_trade_amount_usd": round(trade_amount, 2),
        }

        cond_id = overlap.get("condition_id")
        if cond_id in recommended_trades:
            existing_score = recommended_trades[cond_id]["conviction_score"]
            if conviction_score > existing_score:
                recommended_trades[cond_id] = trade_obj
            elif conviction_score == existing_score:
                if len(valid_traders) > recommended_trades[cond_id]["valid_top_traders_count"]:
                    recommended_trades[cond_id] = trade_obj
        else:
            recommended_trades[cond_id] = trade_obj

    # Sort by conviction score
    recommended_trades = list(recommended_trades.values())
    recommended_trades.sort(key=lambda x: x["conviction_score"], reverse=True)

    output = {
        "metadata": {
            "timestamp": now.isoformat(),
            "simulated_portfolio_usd": PORTFOLIO_SIZE,
            "total_recommended_trades": len(recommended_trades),
        },
        "trades": recommended_trades
    }

    with open(RECOMMENDED_TRADES_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    logger.info(f"Generated {len(recommended_trades)} trade recommendations.")
    if skipped_state:
        logger.info(f"Skipped {skipped_state} overlaps whose market was already "
                    f"under way or settled.")
    logger.info(f"Results saved to {RECOMMENDED_TRADES_FILE}")

    # Print summary
    print("-" * 80)
    print("RECOMMENDED TRADES (Simulated)")
    print("-" * 80)
    for i, t in enumerate(recommended_trades[:10], 1):
        print(f"#{i} BUY {t['outcome_to_buy']} @ {t['current_price'] * 100:.1f}%")
        print(f"   Market: {t['market_title']}")
        print(f"   Score: {t['conviction_score']} ({t['tier']} Conviction) | Alloc: ${t['recommended_trade_amount_usd']}")
        print(f"   Top Traders: {t['valid_top_traders_count']} | Resolves in: {t['days_to_expiry']} days")
        print()

if __name__ == "__main__":
    main()
