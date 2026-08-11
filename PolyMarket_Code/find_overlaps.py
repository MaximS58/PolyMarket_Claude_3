#!/usr/bin/env python3
"""
Polymarket Trade Overlap Analyzer
==================================

Reads the output JSON from polymarket_top_traders_scraper.py and finds
markets where multiple top traders hold positions. Groups by market and
shows which traders are on the same bet.

Only markets that have not started yet are reported. A game already under way
is priced on information we don't have, and one already settled cannot be
entered at all — copying a trader into either is not a real signal.

Each surviving group carries its market end date, so trade_decision.py can read
it locally instead of making one API call per overlap.

Usage:
    python find_overlaps.py                          # uses latest_top_traders.json
    python find_overlaps.py output/top_traders_*.json # uses a specific file
    python find_overlaps.py --no-filter              # keep in-progress/resolved too

Author : Antigravity AI
Version: 2.0.0
"""

from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

# ─────────────────────────────────── config ───────────────────────────────────

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_BATCH = 20      # condition ids per request
GAMMA_RPS = 8
MAX_RETRIES = 3
BACKOFF_BASE = 2.0

# Market states we still consider tradeable.
TRADEABLE_STATES = {"upcoming"}

# ──────────────────────────── market state lookup ─────────────────────────────


def _build_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    retry = Retry(total=3, backoff_factor=1,
                  status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        ),
    })
    return s


SESSION = _build_session()
_last_request = 0.0


def _gamma_get(params: dict) -> Any:
    """Rate-limited Gamma GET with a couple of retries."""
    global _last_request
    interval = 1.0 / GAMMA_RPS
    gap = time.monotonic() - _last_request
    if gap < interval:
        time.sleep(interval - gap)
    _last_request = time.monotonic()

    backoff = BACKOFF_BASE
    for _ in range(MAX_RETRIES):
        try:
            r = SESSION.get(GAMMA_MARKETS_URL, params=params, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(backoff)
                backoff *= 2
                continue
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            time.sleep(backoff)
            backoff *= 2
    return []


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def classify_market(market: dict, now: datetime) -> str:
    """upcoming | in_progress | resolved."""
    if market.get("closed"):
        return "resolved"
    start = _parse_dt(market.get("gameStartTime") or market.get("startDate"))
    if start is None:
        # No scheduled start — an open-ended market like "Will X happen by Y?".
        # There is no mid-game to be caught in, so it stays enterable.
        return "upcoming"
    return "in_progress" if start <= now else "upcoming"


def fetch_market_states(condition_ids: list[str]) -> dict[str, dict]:
    """
    Look up state + end date for many markets in batches.

    Gamma excludes closed markets by default, so anything missing from the first
    pass gets a second, explicit closed=true lookup. That keeps "resolved" and
    "could not be reached" from being silently conflated.
    """
    unique = sorted(set(condition_ids))
    now = datetime.now(timezone.utc)
    states: dict[str, dict] = {}

    def absorb(markets: Any) -> None:
        if not isinstance(markets, list):
            return
        for m in markets:
            cid = m.get("conditionId")
            if not cid:
                continue
            states[cid] = {
                "state": classify_market(m, now),
                "end_date": m.get("endDate"),
                "game_start_time": m.get("gameStartTime") or m.get("startDate"),
                "question": m.get("question", ""),
            }

    total_batches = (len(unique) + GAMMA_BATCH - 1) // GAMMA_BATCH
    print(f"Checking market state for {len(unique)} markets "
          f"({total_batches} batched requests)…")

    for i in range(0, len(unique), GAMMA_BATCH):
        chunk = unique[i:i + GAMMA_BATCH]
        absorb(_gamma_get({"condition_ids": chunk}))

    missing = [c for c in unique if c not in states]
    if missing:
        for i in range(0, len(missing), GAMMA_BATCH):
            chunk = missing[i:i + GAMMA_BATCH]
            absorb(_gamma_get({"condition_ids": chunk, "closed": "true"}))

    for cid in unique:
        states.setdefault(cid, {"state": "unavailable", "end_date": None,
                                "game_start_time": None, "question": ""})
    return states


def load_data(filepath: Path) -> dict:
    """Load the trader data JSON."""
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def find_overlaps(data: dict) -> list[dict]:
    """
    Find markets where 2+ traders hold positions.

    Groups by (condition_id, outcome) so traders betting the same direction
    on the same market are grouped together.

    Returns a list of overlap groups sorted by number of traders (desc).
    """
    # Key: (condition_id, outcome) → list of {trader info + position info}
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for trader in data["traders"]:
        for pos in trader["positions"]:
            key = (pos["condition_id"], pos["outcome"])
            buckets[key].append({
                "username": trader["display_name"],
                "proxy_wallet": trader["proxy_wallet"],
                "rank": trader["rank"],
                "size": pos["size"],
                "avg_price": pos["avg_price"],
                "current_value": pos["current_value"],
                "cash_pnl": pos["cash_pnl"],
                "cur_price": pos["cur_price"],
                "title": pos["title"],
                "slug": pos["slug"],
                "condition_id": pos["condition_id"],
                "outcome": pos["outcome"],
            })

    # Filter to groups with 2+ traders
    overlaps = []
    for (cid, outcome), entries in buckets.items():
        if len(entries) < 2:
            continue

        sample = entries[0]
        total_size = sum(e["size"] for e in entries)
        total_value = sum(e["current_value"] for e in entries)
        total_pnl = sum(e["cash_pnl"] for e in entries)

        # Sort traders within group by rank
        entries.sort(key=lambda e: e["rank"])

        overlaps.append({
            "market_title": sample["title"],
            "condition_id": cid,
            "outcome": outcome,
            "cur_price": sample["cur_price"],
            "slug": sample["slug"],
            "trader_count": len(entries),
            "total_size": round(total_size, 2),
            "total_value": round(total_value, 2),
            "total_pnl": round(total_pnl, 2),
            "traders": entries,
        })

    # Sort by number of traders sharing the position (desc), then by total size
    overlaps.sort(key=lambda g: (g["trader_count"], g["total_size"]), reverse=True)
    return overlaps


def print_overlaps(overlaps: list[dict], metadata: dict) -> None:
    """Pretty-print the overlap results."""
    sep = "═" * 110

    print(sep)
    print(f"  TRADE OVERLAP REPORT")
    print(f"  Source: {metadata.get('leaderboard_category', '?')} leaderboard, "
          f"{metadata.get('leaderboard_time_period', '?')} period, "
          f"top {metadata.get('top_n_traders', '?')} traders")
    print(f"  Timestamp: {metadata.get('timestamp', '?')}")
    print(f"  Overlapping markets found: {len(overlaps)}")
    if metadata.get("state_filter_applied"):
        counts = metadata.get("state_counts", {})
        print(f"  Filter: upcoming markets only "
              f"(dropped {counts.get('in_progress', 0)} in-progress, "
              f"{counts.get('resolved', 0)} resolved)")
    print(sep)

    for i, group in enumerate(overlaps, 1):
        price_pct = f"{group['cur_price'] * 100:.1f}%" if group["cur_price"] else "N/A"
        starts = group.get("game_start_time")

        print()
        print(f"  #{i}  {group['market_title']}")
        print(f"       Outcome: {group['outcome']}   Current Price: {price_pct}"
              + (f"   Starts: {str(starts)[:16]}" if starts else ""))
        print(f"       Market ID: {group['condition_id']}")
        print(f"       Traders on this bet: {group['trader_count']}   "
              f"Combined size: {group['total_size']:,.2f}   "
              f"Combined PnL: ${group['total_pnl']:+,.2f}")
        print()

        for t in group["traders"]:
            print(f"       ├─ #{t['rank']}  {t['username']}")
            print(f"       │    Size: {t['size']:,.2f}   "
                  f"Avg Price: {t['avg_price']:.4f}   "
                  f"Value: ${t['current_value']:,.2f}   "
                  f"PnL: ${t['cash_pnl']:+,.2f}")

        print()

    print(sep)
    print(f"  END OF OVERLAP REPORT — {len(overlaps)} markets with shared positions")
    print(sep)


def save_overlaps(overlaps: list[dict], metadata: dict) -> Path:
    """Save overlap results to a JSON file."""
    output = {
        "metadata": {
            **metadata,
            "overlap_count": len(overlaps),
        },
        "overlaps": overlaps,
    }

    filepath = OUTPUT_DIR / "latest_overlaps.json"
    filepath.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    return filepath


def apply_state_filter(overlaps: list[dict]) -> tuple[list[dict], dict[str, int]]:
    """
    Annotate every group with its market state and keep only the tradeable ones.

    Returns (kept, counts_by_state).
    """
    states = fetch_market_states([g["condition_id"] for g in overlaps])

    counts: dict[str, int] = defaultdict(int)
    kept: list[dict] = []
    for group in overlaps:
        info = states.get(group["condition_id"], {"state": "unavailable"})
        group["market_state"] = info["state"]
        group["end_date"] = info.get("end_date")
        group["game_start_time"] = info.get("game_start_time")

        counts[info["state"]] += 1
        if info["state"] in TRADEABLE_STATES:
            kept.append(group)

    return kept, dict(counts)


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    apply_filter = "--no-filter" not in sys.argv

    input_path = Path(args[0]) if args else OUTPUT_DIR / "latest_top_traders.json"

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        print("Run polymarket_top_traders_scraper.py first to generate trader data.")
        sys.exit(1)

    print(f"Loading data from: {input_path}")
    data = load_data(input_path)

    metadata = data.get("metadata", {})
    traders = data.get("traders", [])
    total_positions = sum(len(t["positions"]) for t in traders)
    print(f"Loaded {len(traders)} traders with {total_positions} total open positions.")

    overlaps = find_overlaps(data)
    print(f"Found {len(overlaps)} markets with overlapping positions.\n")

    counts: dict[str, int] = {}
    if apply_filter and overlaps:
        before = len(overlaps)
        overlaps, counts = apply_state_filter(overlaps)
        print()
        print(f"  Market state of those {before} groups:")
        for state in ("upcoming", "in_progress", "resolved", "unavailable"):
            if counts.get(state):
                print(f"    {state:<12} {counts[state]:>5}")
        print(f"\n  Keeping {len(overlaps)} upcoming — dropped "
              f"{before - len(overlaps)} already under way or settled.\n")
    elif not apply_filter:
        print("  --no-filter: keeping in-progress and resolved markets too.\n")

    metadata = {**metadata, "state_counts": counts,
                "state_filter_applied": apply_filter,
                "tradeable_states": sorted(TRADEABLE_STATES)}

    print_overlaps(overlaps, metadata)

    filepath = save_overlaps(overlaps, metadata)
    print(f"\nResults saved to: {filepath}")


if __name__ == "__main__":
    main()
