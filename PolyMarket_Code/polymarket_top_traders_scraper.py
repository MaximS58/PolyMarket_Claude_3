#!/usr/bin/env python3
"""
Polymarket Top Weekly Traders Scraper
=====================================

Fetches the top-10 traders from the Polymarket weekly leaderboard
(all categories), then retrieves each trader's open positions on
active (unresolved) markets.

Output per trader:
  - username / display name
  - proxy wallet (profile ID)
  - PnL and volume (for the week)
  - list of open market condition IDs
  - count of open positions

APIs used (public, no auth):
  - Data API: https://data-api.polymarket.com

Author : Antigravity AI
Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────── config ───────────────────────────────────

DATA_API_BASE = "https://data-api.polymarket.com"
LEADERBOARD_ENDPOINT = f"{DATA_API_BASE}/v1/leaderboard"
POSITIONS_ENDPOINT = f"{DATA_API_BASE}/positions"

TOP_N_TRADERS = 100
LEADERBOARD_PAGE_SIZE = 50  # API max per page

# Leaderboard filters
LEADERBOARD_CATEGORY = "OVERALL"   # all market categories
LEADERBOARD_TIME_PERIOD = "WEEK"   # last 7 days

POSITIONS_PAGE_SIZE = 500
POSITION_SIZE_THRESHOLD = 0.5  # min token count to include
MAX_POSITIONS_OFFSET = 5000    # cap pagination to avoid huge wallets stalling

# Rate-limit settings
POSITIONS_REQUESTS_PER_SECOND = 8
LEADERBOARD_REQUESTS_PER_SECOND = 5
BACKOFF_BASE_SECONDS = 2.0
BACKOFF_MAX_SECONDS = 60.0
MAX_RETRIES_PER_REQUEST = 5

# Output
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────── logging ──────────────────────────────────────

LOG_FORMAT = "%(asctime)s │ %(levelname)-8s │ %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("polymarket_scraper")

# ────────────────────────────── data models ───────────────────────────────────


@dataclass
class OpenPosition:
    """A single open position on an active market."""
    condition_id: str
    asset: str
    outcome: str       # YES / NO
    size: float
    avg_price: float
    current_value: float
    cash_pnl: float
    cur_price: float
    title: str
    slug: str


@dataclass
class TraderProfile:
    """A trader from the leaderboard with their open positions."""
    rank: int
    proxy_wallet: str
    username: str
    pnl: float
    volume: float
    profile_image: str = ""
    x_username: str = ""
    open_positions: list[OpenPosition] = field(default_factory=list)

    @property
    def display_name(self) -> str:
        if self.username:
            return self.username
        return f"{self.proxy_wallet[:6]}…{self.proxy_wallet[-4:]}"

    @property
    def open_position_count(self) -> int:
        return len(self.open_positions)

    @property
    def open_market_ids(self) -> list[str]:
        """Unique condition IDs for active open markets."""
        seen = set()
        ids = []
        for p in self.open_positions:
            if p.condition_id not in seen:
                seen.add(p.condition_id)
                ids.append(p.condition_id)
        return ids


# ──────────────────────────── HTTP session ────────────────────────────────────


def _build_session() -> requests.Session:
    session = requests.Session()
    session.verify = False
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"})
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "PolymarketTopTradersScraper/2.0",
    })
    return session


SESSION = _build_session()

# ─────────────────────────── rate-limit helpers ───────────────────────────────


class RateLimiter:
    """Simple token-bucket rate limiter."""

    def __init__(self, requests_per_second: float):
        self._min_interval = 1.0 / requests_per_second
        self._last_request_time = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()


_positions_limiter = RateLimiter(POSITIONS_REQUESTS_PER_SECOND)
_leaderboard_limiter = RateLimiter(LEADERBOARD_REQUESTS_PER_SECOND)


def _api_get(
    url: str,
    params: dict | None = None,
    limiter: RateLimiter | None = None,
    label: str = "request",
) -> Any:
    """
    GET with rate-limiting, exponential back-off on 429/5xx, and logging.
    Returns parsed JSON on success; raises on unrecoverable failure.
    """
    if limiter:
        limiter.wait()

    backoff = BACKOFF_BASE_SECONDS
    for attempt in range(1, MAX_RETRIES_PER_REQUEST + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", backoff))
                logger.warning(
                    "Rate-limited on %s (attempt %d/%d). Waiting %.1fs…",
                    label, attempt, MAX_RETRIES_PER_REQUEST, retry_after,
                )
                time.sleep(retry_after)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue

            if resp.status_code >= 500:
                logger.warning(
                    "Server error %d on %s (attempt %d/%d). Waiting %.1fs…",
                    resp.status_code, label, attempt, MAX_RETRIES_PER_REQUEST, backoff,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.ConnectionError as exc:
            logger.warning(
                "Connection error on %s (attempt %d/%d): %s. Retrying in %.1fs…",
                label, attempt, MAX_RETRIES_PER_REQUEST, exc, backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
        except requests.exceptions.Timeout:
            logger.warning(
                "Timeout on %s (attempt %d/%d). Retrying in %.1fs…",
                label, attempt, MAX_RETRIES_PER_REQUEST, backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)

    raise RuntimeError(f"Failed {label} after {MAX_RETRIES_PER_REQUEST} attempts")


# ──────────────────────────── core functions ──────────────────────────────────


def fetch_leaderboard(top_n: int = TOP_N_TRADERS) -> list[TraderProfile]:
    """
    Fetch the top-N traders from the weekly all-categories leaderboard.
    """
    traders: list[TraderProfile] = []
    offset = 0

    while len(traders) < top_n:
        page_size = min(LEADERBOARD_PAGE_SIZE, top_n - len(traders))
        params = {
            "category": LEADERBOARD_CATEGORY,
            "timePeriod": LEADERBOARD_TIME_PERIOD,
            "orderBy": "PNL",
            "limit": page_size,
            "offset": offset,
        }
        logger.info(
            "Fetching leaderboard (category=%s, period=%s, offset=%d, limit=%d)…",
            LEADERBOARD_CATEGORY, LEADERBOARD_TIME_PERIOD, offset, page_size,
        )
        data = _api_get(
            LEADERBOARD_ENDPOINT,
            params=params,
            limiter=_leaderboard_limiter,
            label=f"leaderboard offset={offset}",
        )

        if not data:
            logger.warning("Leaderboard returned empty at offset %d", offset)
            break

        for entry in data:
            traders.append(TraderProfile(
                rank=int(entry.get("rank", len(traders) + 1)),
                proxy_wallet=entry.get("proxyWallet", ""),
                username=entry.get("userName", ""),
                pnl=float(entry.get("pnl", 0)),
                volume=float(entry.get("vol", 0)),
                profile_image=entry.get("profileImage", ""),
                x_username=entry.get("xUsername", ""),
            ))

        if len(data) < page_size:
            break

        offset += page_size

    logger.info("Fetched %d traders from the leaderboard.", len(traders))
    return traders[:top_n]


def fetch_open_positions(wallet: str) -> list[OpenPosition]:
    """
    Fetch open positions for a wallet, keeping only those on active
    (unresolved) markets — i.e. positions where curPrice > 0.
    """
    positions: list[OpenPosition] = []
    offset = 0

    while offset <= MAX_POSITIONS_OFFSET:
        params = {
            "user": wallet,
            "sizeThreshold": POSITION_SIZE_THRESHOLD,
            "limit": POSITIONS_PAGE_SIZE,
            "offset": offset,
            "sortBy": "CURRENT",
            "sortDirection": "DESC",
        }
        try:
            data = _api_get(
                POSITIONS_ENDPOINT,
                params=params,
                limiter=_positions_limiter,
                label=f"positions for {wallet[:10]}… offset={offset}",
            )
        except requests.exceptions.HTTPError as exc:
            # API returns 400 when offset exceeds its limit — stop paginating
            if exc.response is not None and exc.response.status_code == 400:
                logger.warning(
                    "HTTP 400 at offset %d for %s… — stopping pagination.",
                    offset, wallet[:10],
                )
                break
            raise

        if not data:
            break

        for p in data:
            size = float(p.get("size", 0))
            cur_price = float(p.get("curPrice", 0))
            is_redeemable = p.get("redeemable", False)

            # Skip closed positions (size <= 0) and resolved markets
            if size <= 0 or is_redeemable or cur_price <= 0 or cur_price >= 1:
                continue

            positions.append(OpenPosition(
                condition_id=p.get("conditionId", ""),
                asset=p.get("asset", ""),
                outcome=p.get("outcome", ""),
                size=size,
                avg_price=float(p.get("avgPrice", 0)),
                current_value=float(p.get("currentValue", 0)),
                cash_pnl=float(p.get("cashPnl", 0)),
                cur_price=cur_price,
                title=p.get("title", ""),
                slug=p.get("slug", ""),
            ))

        if len(data) < POSITIONS_PAGE_SIZE:
            break

        offset += POSITIONS_PAGE_SIZE

    return positions


# ────────────────────────────── reporting ──────────────────────────────────────


def print_summary(traders: list[TraderProfile]) -> None:
    """Print per-trader summary with all open positions listed."""
    sep = "═" * 110
    logger.info(sep)
    logger.info(
        "TOP %d WEEKLY TRADERS — %s category, %s period",
        len(traders), LEADERBOARD_CATEGORY, LEADERBOARD_TIME_PERIOD,
    )
    logger.info(sep)

    for t in traders:
        logger.info("")
        logger.info("  #%-3d  %s", t.rank, t.display_name)
        logger.info("        Profile ID: %s", t.proxy_wallet)
        logger.info("")

        if not t.open_positions:
            logger.info("        (no open positions)")
        else:
            for pos in t.open_positions:
                logger.info(
                    "        ├─ %s",
                    pos.title or "(untitled)",
                )
                logger.info(
                    "        │    Outcome: %-5s   Size: %s   Avg Price: %.4f   PnL: $%s",
                    pos.outcome,
                    f"{pos.size:,.2f}",
                    pos.avg_price,
                    f"{pos.cash_pnl:+,.2f}",
                )
                logger.info(
                    "        │    Market ID: %s",
                    pos.condition_id,
                )

    logger.info("")
    logger.info(sep)


def save_results(traders: list[TraderProfile], run_timestamp: str) -> Path:
    """Save results to a JSON file."""
    output = {
        "metadata": {
            "timestamp": run_timestamp,
            "leaderboard_category": LEADERBOARD_CATEGORY,
            "leaderboard_time_period": LEADERBOARD_TIME_PERIOD,
            "top_n_traders": len(traders),
        },
        "traders": [
            {
                "rank": t.rank,
                "proxy_wallet": t.proxy_wallet,
                "username": t.username,
                "display_name": t.display_name,
                "pnl": t.pnl,
                "volume": t.volume,
                "x_username": t.x_username,
                "open_position_count": t.open_position_count,
                "open_market_ids": t.open_market_ids,
                "positions": [
                    {
                        "condition_id": p.condition_id,
                        "asset": p.asset,
                        "outcome": p.outcome,
                        "size": round(p.size, 4),
                        "avg_price": round(p.avg_price, 4),
                        "current_value": round(p.current_value, 2),
                        "cash_pnl": round(p.cash_pnl, 2),
                        "cur_price": round(p.cur_price, 4),
                        "title": p.title,
                        "slug": p.slug,
                    }
                    for p in t.open_positions
                ],
            }
            for t in traders
        ],
    }

    filepath = OUTPUT_DIR / f"top_traders_{run_timestamp.replace(':', '-')}.json"
    filepath.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    latest = OUTPUT_DIR / "latest_top_traders.json"
    latest.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    return filepath


# ───────────────────────────── main pipeline ──────────────────────────────────


def run() -> None:
    """Execute one scrape cycle."""
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("=" * 110)
    logger.info("Polymarket Top Traders Scraper — started at %s", run_ts)
    logger.info("=" * 110)

    t0 = time.monotonic()

    # 1) Fetch leaderboard
    logger.info("STEP 1/3 — Fetching top %d weekly traders…", TOP_N_TRADERS)
    traders = fetch_leaderboard(TOP_N_TRADERS)
    if not traders:
        logger.error("No traders returned. Aborting.")
        return

    # 2) Fetch open positions for each trader
    logger.info("STEP 2/3 — Fetching open positions for %d traders…", len(traders))
    for i, trader in enumerate(traders, 1):
        if not trader.proxy_wallet:
            logger.warning("Trader rank %d has no wallet — skipping.", trader.rank)
            continue
        try:
            trader.open_positions = fetch_open_positions(trader.proxy_wallet)
        except Exception:
            logger.exception(
                "Failed to fetch positions for %s (rank %d) — skipping.",
                trader.display_name, trader.rank,
            )
        logger.info(
            "  [%d/%d] %s — %d open positions",
            i, len(traders), trader.display_name, trader.open_position_count,
        )

    # 3) Report & save
    logger.info("STEP 3/3 — Reporting…")
    print_summary(traders)

    filepath = save_results(traders, run_ts)
    elapsed = time.monotonic() - t0
    logger.info("Done in %.1fs. Results saved to %s", elapsed, filepath)


if __name__ == "__main__":
    run()
